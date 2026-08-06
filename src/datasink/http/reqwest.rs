use std::{
    collections::HashMap,
    error::Error,
    sync::{Arc, Weak},
    time::Duration,
};

use async_trait::async_trait;
use tracing::Instrument;
use tracing_opentelemetry::OpenTelemetrySpanExt;

use crate::{
    operators::SinkStreamWithResult,
    runtime::{
        common::{Consumer, MessageContext, Payload, RuntimeStream},
        config::{HttpDataConnectorConfig, HttpEndpointConfig},
        datasink::SinkStreamContext,
        environment::{RuntimeResult, metrics::Labels},
        telemetry::HttpClientMetrics,
    },
};

pub const IMPLEMENTATION: &str = "rust/axum";

pub type HandlerError = Box<dyn Error + Send + Sync>;
pub type HandlerResult = Result<(), HandlerError>;

fn labels(values: &[(&str, &str)]) -> Labels {
    values
        .iter()
        .map(|(name, value)| ((*name).to_owned(), (*value).to_owned()))
        .collect()
}

#[derive(Clone, Debug, Default)]
pub struct Request {
    pub method: String,
    pub url: String,
    pub body: Vec<u8>,
    pub headers: HashMap<String, String>,
    pub timeout: Option<Duration>,
}

#[derive(Default)]
pub struct Requester {
    request: Option<Request>,
}

impl Requester {
    pub fn new_request(
        &mut self,
        method: impl Into<String>,
        url: impl Into<String>,
        body: impl Into<Vec<u8>>,
    ) -> &mut Request {
        self.request.insert(Request {
            method: method.into(),
            url: url.into(),
            body: body.into(),
            headers: HashMap::new(),
            timeout: None,
        })
    }

    fn take_request(&mut self) -> Result<Request, HandlerError> {
        self.request
            .take()
            .ok_or_else(|| "HTTP sink handler did not create a request".into())
    }
}

#[derive(Clone, Debug)]
pub struct Response {
    pub status: u16,
    pub body: Vec<u8>,
    pub headers: HashMap<String, String>,
}

impl Response {
    pub fn is_error(&self) -> bool {
        self.status >= 400
    }
}

#[async_trait]
pub trait Client: Send + Sync {
    async fn perform(
        &self,
        context: MessageContext,
        request: Request,
    ) -> Result<Response, HandlerError>;
}

#[derive(Clone, Default)]
pub struct ReqwestClient {
    client: reqwest::Client,
}

#[async_trait]
impl Client for ReqwestClient {
    async fn perform(
        &self,
        context: MessageContext,
        request: Request,
    ) -> Result<Response, HandlerError> {
        let method = reqwest::Method::from_bytes(request.method.as_bytes())?;
        let mut builder = self.client.request(method, request.url).body(request.body);
        for (name, value) in request.headers {
            builder = builder.header(name, value);
        }
        let timeout = match (request.timeout, context.remaining()) {
            (Some(request), Some(context)) => Some(request.min(context)),
            (Some(request), None) => Some(request),
            (None, Some(context)) => Some(context),
            (None, None) => None,
        };
        if let Some(timeout) = timeout {
            builder = builder.timeout(timeout);
        }
        let response = tokio::select! {
            response = builder.send() => response?,
            _ = context.cancelled() => {
                return Err("HTTP request context cancelled".into());
            }
        };
        let status = response.status().as_u16();
        let headers = response
            .headers()
            .iter()
            .filter_map(|(name, value)| {
                value
                    .to_str()
                    .ok()
                    .map(|value| (name.as_str().to_owned(), value.to_owned()))
            })
            .collect();
        let body = response.bytes().await?.to_vec();
        Ok(Response {
            status,
            body,
            headers,
        })
    }
}

pub struct StreamContext<T, R, E>
where
    T: Send + Sync + 'static,
    R: Send + Sync + 'static,
    E: Send + Sync + 'static,
{
    base: SinkStreamContext<T, R, E>,
    endpoint_config: HttpEndpointConfig,
    data_connector_config: HttpDataConnectorConfig,
}

impl<T, R, E> Clone for StreamContext<T, R, E>
where
    T: Send + Sync + 'static,
    R: Send + Sync + 'static,
    E: Send + Sync + 'static,
{
    fn clone(&self) -> Self {
        Self {
            base: self.base.clone(),
            endpoint_config: self.endpoint_config.clone(),
            data_connector_config: self.data_connector_config.clone(),
        }
    }
}

impl<T, R, E> StreamContext<T, R, E>
where
    T: Send + Sync + 'static,
    R: Send + Sync + 'static,
    E: Send + Sync + 'static,
{
    pub fn endpoint_config(&self) -> &HttpEndpointConfig {
        &self.endpoint_config
    }

    pub fn data_connector_config(&self) -> &HttpDataConnectorConfig {
        &self.data_connector_config
    }

    pub async fn collect(&self, context: MessageContext, value: R) {
        self.base.collect(context, value).await;
    }

    pub async fn error_collect(&self, context: MessageContext, value: E) {
        self.base.error_collect(context, value).await;
    }
}

#[async_trait]
pub trait EndpointHandler<HandlerState, T, R, E>: Send + Sync
where
    HandlerState: Send + 'static,
    T: Send + Sync + 'static,
    R: Send + Sync + 'static,
    E: Send + Sync + 'static,
{
    async fn begin_request(
        &self,
        context: MessageContext,
        stream: StreamContext<T, R, E>,
    ) -> Result<(MessageContext, HandlerState), HandlerError>;

    async fn consume_message(
        &self,
        context: MessageContext,
        stream: StreamContext<T, R, E>,
        handler_state: &mut HandlerState,
        value: Payload<T>,
        requester: &mut Requester,
    ) -> HandlerResult;

    async fn handle_response(
        &self,
        context: MessageContext,
        stream: StreamContext<T, R, E>,
        handler_state: &mut HandlerState,
        response: Response,
    ) -> HandlerResult;

    async fn end_request(
        &self,
        context: MessageContext,
        stream: StreamContext<T, R, E>,
        result: &HandlerResult,
        handler_state: HandlerState,
    );
}

pub struct EndpointConsumer<HandlerState, T, R, E, H>
where
    HandlerState: Send + 'static,
    T: Send + Sync + 'static,
    R: Send + Sync + 'static,
    E: Send + Sync + 'static,
    H: EndpointHandler<HandlerState, T, R, E> + 'static,
{
    stream: Weak<SinkStreamWithResult<T, R, E>>,
    stream_context: StreamContext<T, R, E>,
    client: Arc<dyn Client>,
    handler: H,
    messages_total: crate::runtime::environment::metrics::Int64Counter,
    request_errors: crate::runtime::environment::metrics::Int64Counter,
    begin_request_failed: crate::runtime::environment::metrics::Int64Counter,
    active_requests: crate::runtime::environment::metrics::Int64Gauge,
    request_duration: crate::runtime::environment::metrics::Float64Histogram,
    http_client_metrics: HttpClientMetrics,
    _state: std::marker::PhantomData<fn(HandlerState)>,
}

pub fn make_endpoint_consumer<HandlerState, T, R, E, H>(
    stream: &Arc<SinkStreamWithResult<T, R, E>>,
    endpoint_config: HttpEndpointConfig,
    data_connector_config: HttpDataConnectorConfig,
    client: Arc<dyn Client>,
    handler: H,
) -> RuntimeResult<Arc<EndpointConsumer<HandlerState, T, R, E, H>>>
where
    HandlerState: Send + 'static,
    T: Send + Sync + 'static,
    R: Send + Sync + 'static,
    E: Send + Sync + 'static,
    H: EndpointHandler<HandlerState, T, R, E> + 'static,
{
    let method = match endpoint_config.http_method_type {
        crate::api::HTTPMethodType::GET => "GET",
        crate::api::HTTPMethodType::POST => "POST",
        crate::api::HTTPMethodType::Undefined => "",
    };
    let metric_url = format!(
        "{}{}{}",
        data_connector_config.address.trim_end_matches('/'),
        if endpoint_config.path.starts_with('/') {
            ""
        } else {
            "/"
        },
        endpoint_config.path
    );
    let http_client_metrics =
        HttpClientMetrics::new(stream.environment().metrics().clone(), method, &metric_url);
    let scope = stream.environment().metrics().scope(
        "datasink_endpoint",
        labels(&[
            ("connector", data_connector_config.name.as_str()),
            ("endpoint", endpoint_config.name.as_str()),
            ("protocol", "http"),
        ]),
    );
    let consumer = Arc::new(EndpointConsumer {
        stream: Arc::downgrade(stream),
        stream_context: StreamContext {
            base: SinkStreamContext::new(Arc::downgrade(stream)),
            endpoint_config,
            data_connector_config,
        },
        client,
        handler,
        messages_total: scope.counter(
            "messages_total",
            "Total number of successfully processed messages in data sink endpoint",
            Labels::new(),
        )?,
        request_errors: scope.counter(
            "events_total",
            "Total number of events in data sink endpoint",
            labels(&[("event", "request_error")]),
        )?,
        begin_request_failed: scope.counter(
            "events_total",
            "Total number of events in data sink endpoint",
            labels(&[("event", "begin_request_failed")]),
        )?,
        active_requests: scope.gauge(
            "active_requests",
            "Number of active requests in data sink endpoint",
            Labels::new(),
        )?,
        request_duration: scope.histogram(
            "request_duration_seconds",
            "Request duration in seconds for data sink endpoint",
            Labels::new(),
            None,
        )?,
        http_client_metrics,
        _state: std::marker::PhantomData,
    });
    stream.set_sink_consumer(consumer.clone())?;
    Ok(consumer)
}

#[async_trait]
impl<HandlerState, T, R, E, H> Consumer<T> for EndpointConsumer<HandlerState, T, R, E, H>
where
    HandlerState: Send + 'static,
    T: Send + Sync + 'static,
    R: Send + Sync + 'static,
    E: Send + Sync + 'static,
    H: EndpointHandler<HandlerState, T, R, E> + 'static,
{
    async fn consume(&self, context: MessageContext, value: Payload<T>) {
        let Some(stream) = self.stream.upgrade() else {
            return;
        };
        let span = if context.sampling_enabled() {
            let span = tracing::info_span!(
                "http.output",
                stream = stream.name(),
                endpoint = self.stream_context.endpoint_config.name,
                error = tracing::field::Empty,
                otel.status_code = tracing::field::Empty,
                otel.status_message = tracing::field::Empty,
            );
            let _ = span.set_parent(context.open_telemetry_context().clone());
            span
        } else {
            tracing::Span::none()
        };
        let context = context.with_open_telemetry_context(span.context());
        let (handler_context, mut handler_state) = match self
            .handler
            .begin_request(context, self.stream_context.clone())
            .instrument(span.clone())
            .await
        {
            Ok(result) => result,
            Err(error) => {
                self.begin_request_failed.inc();
                crate::runtime::telemetry::record_span_error(&span, &error);
                span.in_scope(|| {
                    tracing::error!(
                        event.name = "begin_request.error",
                        error = %error,
                        "begin_request failed"
                    );
                });
                return;
            }
        };
        span.in_scope(|| tracing::info!(event.name = "begin_request"));

        self.active_requests.inc();
        let started_at = std::time::Instant::now();
        let mut requester = Requester::default();
        let mut result = self
            .handler
            .consume_message(
                handler_context.clone(),
                self.stream_context.clone(),
                &mut handler_state,
                value,
                &mut requester,
            )
            .instrument(span.clone())
            .await;
        if let Err(error) = &result {
            crate::runtime::telemetry::record_span_error(&span, error);
        }
        span.in_scope(|| match &result {
            Ok(()) => tracing::info!(event.name = "consume_message"),
            Err(error) => tracing::error!(
                event.name = "consume_message.error",
                error = %error,
                "HTTP sink handler failed"
            ),
        });

        let request = if result.is_ok() {
            requester.take_request().map(|mut request| {
                for (name, value) in handler_context.transport_metadata() {
                    request.headers.insert(name, value);
                }
                request
            })
        } else {
            Err("HTTP request was not built because ConsumeMessage failed".into())
        };
        match request {
            Ok(request) => {
                let observation = self.http_client_metrics.start(request.body.len());
                result = match self
                    .client
                    .perform(handler_context.clone(), request)
                    .instrument(span.clone())
                    .await
                {
                    Ok(response) => {
                        observation.finish(Some(response.status), Some(response.body.len()), false);
                        span.in_scope(|| {
                            tracing::info!(event.name = "http_call", status_code = response.status);
                        });
                        let handled = self
                            .handler
                            .handle_response(
                                handler_context.clone(),
                                self.stream_context.clone(),
                                &mut handler_state,
                                response,
                            )
                            .instrument(span.clone())
                            .await;
                        if let Err(error) = &handled {
                            crate::runtime::telemetry::record_span_error(&span, error);
                        }
                        span.in_scope(|| match &handled {
                            Ok(()) => tracing::info!(event.name = "handle_response"),
                            Err(error) => tracing::error!(
                                event.name = "handle_response.error",
                                error = %error,
                                "HTTP response handler failed"
                            ),
                        });
                        handled
                    }
                    Err(error) => {
                        crate::runtime::telemetry::record_span_error(&span, &error);
                        observation.finish(None, None, true);
                        span.in_scope(|| {
                            tracing::error!(
                                event.name = "http_call.error",
                                error = %error,
                                "HTTP client call failed"
                            );
                        });
                        Err(error)
                    }
                };
            }
            Err(error) if result.is_ok() => {
                crate::runtime::telemetry::record_span_error(&span, &error);
                span.in_scope(|| {
                    tracing::error!(
                        event.name = "no_request.error",
                        error = %error,
                        "HTTP sink handler did not build a request"
                    );
                });
                result = Err(error);
            }
            Err(_) => {
                // ConsumeMessage already supplied the error passed to EndRequest.
            }
        }

        self.handler
            .end_request(
                handler_context,
                self.stream_context.clone(),
                &result,
                handler_state,
            )
            .instrument(span.clone())
            .await;
        self.active_requests.dec();
        self.request_duration
            .observe(started_at.elapsed().as_secs_f64());
        if result.is_ok() {
            self.messages_total.inc();
        } else {
            self.request_errors.inc();
            span.in_scope(|| {
                tracing::error!("HTTP sink request failed");
            });
        }
    }
}
