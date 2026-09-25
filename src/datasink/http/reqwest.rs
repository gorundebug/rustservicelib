use std::{
    collections::HashMap,
    error::Error,
    fmt, io,
    pin::Pin,
    sync::{Arc, Mutex, Weak},
    task::{Context, Poll, Waker},
    time::Duration,
};

use async_trait::async_trait;
use tokio::io::{AsyncRead, AsyncReadExt, ReadBuf};
use tokio_util::io::StreamReader;
use tracing_opentelemetry::OpenTelemetrySpanExt;

use crate::{
    operators::SinkStreamWithResult,
    runtime::{
        common::{Consumer, MessageContext, Payload, RuntimeStream, new_stream_id},
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

struct ResponseBodyState {
    reader: Option<Pin<Box<dyn AsyncRead + Send>>>,
    closed: bool,
    waker: Option<Waker>,
}

/// A single-owner streaming response body. Reading is optional. The endpoint
/// closes it after EndRequest, including bodies retained by user handlers.
pub struct ResponseBody {
    state: Arc<Mutex<ResponseBodyState>>,
    content_length: Option<usize>,
}

impl fmt::Debug for ResponseBody {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ResponseBody")
            .field("content_length", &self.content_length)
            .finish_non_exhaustive()
    }
}

fn close_response_body(state: &Mutex<ResponseBodyState>) {
    let (reader, waker) = {
        let mut state = state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        state.closed = true;
        (state.reader.take(), state.waker.take())
    };
    drop(reader);
    if let Some(waker) = waker {
        waker.wake();
    }
}

struct ResponseBodyGuard(Arc<Mutex<ResponseBodyState>>);

impl Drop for ResponseBodyGuard {
    fn drop(&mut self) {
        close_response_body(&self.0);
    }
}

impl ResponseBody {
    pub fn new(reader: impl AsyncRead + Send + 'static) -> Self {
        Self {
            state: Arc::new(Mutex::new(ResponseBodyState {
                reader: Some(Box::pin(reader)),
                closed: false,
                waker: None,
            })),
            content_length: None,
        }
    }

    /// Transport-provided total length, not a reason to read or buffer the body.
    pub fn content_length(&self) -> Option<usize> {
        self.content_length
    }

    /// Explicitly collect the remaining body, analogous to Go's io.ReadAll.
    pub async fn bytes(&mut self) -> io::Result<Vec<u8>> {
        let mut bytes = Vec::new();
        self.read_to_end(&mut bytes).await?;
        Ok(bytes)
    }

    /// Release the underlying response without draining unread bytes.
    pub fn close(&mut self) {
        close_response_body(&self.state);
    }

    fn close_on_drop(&self) -> ResponseBodyGuard {
        ResponseBodyGuard(Arc::clone(&self.state))
    }
}

impl From<Vec<u8>> for ResponseBody {
    fn from(bytes: Vec<u8>) -> Self {
        let size = bytes.len();
        let mut body = Self::new(io::Cursor::new(bytes));
        body.content_length = Some(size);
        body
    }
}

impl AsyncRead for ResponseBody {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        if buffer.remaining() == 0 {
            return Poll::Ready(Ok(()));
        }
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if state.closed {
            return Poll::Ready(Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "HTTP response body is closed",
            )));
        }
        let before = buffer.filled().len();
        let outcome = match state.reader.as_mut() {
            Some(reader) => reader.as_mut().poll_read(cx, buffer),
            None => return Poll::Ready(Ok(())),
        };
        let retired = match &outcome {
            Poll::Ready(result) => {
                state.waker = None;
                if result.is_err() || buffer.filled().len() == before {
                    state.reader.take()
                } else {
                    None
                }
            }
            Poll::Pending => {
                state.waker = Some(cx.waker().clone());
                None
            }
        };
        drop(state);
        drop(retired);
        outcome
    }
}

#[derive(Debug)]
pub struct Response {
    pub status: u16,
    pub body: ResponseBody,
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
        let content_length = response
            .content_length()
            .and_then(|size| usize::try_from(size).ok());
        let chunks = futures::stream::try_unfold(
            (response, context),
            |(mut response, context)| async move {
                let chunk = tokio::select! {
                    biased;
                    _ = context.cancelled() => return Err(io::Error::other("HTTP request context cancelled")),
                    chunk = response.chunk() => chunk.map_err(io::Error::other)?,
                };
                Ok(chunk.map(|chunk| (chunk, (response, context))))
            },
        );
        let mut body = ResponseBody::new(StreamReader::new(Box::pin(chunks)));
        body.content_length = content_length;
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
        }
    }
}

impl<T, R, E> StreamContext<T, R, E>
where
    T: Send + Sync + 'static,
    R: Send + Sync + 'static,
    E: Send + Sync + 'static,
{
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

// Forward the existing boxed future without allocating an async wrapper.
impl<HandlerState, T, R, E, H> EndpointHandler<HandlerState, T, R, E> for Arc<H>
where
    HandlerState: Send + 'static,
    T: Send + Sync + 'static,
    R: Send + Sync + 'static,
    E: Send + Sync + 'static,
    H: EndpointHandler<HandlerState, T, R, E> + ?Sized,
{
    fn begin_request<'owner, 'future>(
        &'owner self,
        context: MessageContext,
        stream: StreamContext<T, R, E>,
    ) -> futures::future::BoxFuture<'future, Result<(MessageContext, HandlerState), HandlerError>>
    where
        'owner: 'future,
        Self: 'future,
    {
        self.as_ref().begin_request(context, stream)
    }

    fn consume_message<'owner, 'handler_state, 'requester, 'future>(
        &'owner self,
        context: MessageContext,
        stream: StreamContext<T, R, E>,
        handler_state: &'handler_state mut HandlerState,
        value: Payload<T>,
        requester: &'requester mut Requester,
    ) -> futures::future::BoxFuture<'future, HandlerResult>
    where
        'owner: 'future,
        'handler_state: 'future,
        'requester: 'future,
        Self: 'future,
    {
        self.as_ref()
            .consume_message(context, stream, handler_state, value, requester)
    }

    fn handle_response<'owner, 'handler_state, 'future>(
        &'owner self,
        context: MessageContext,
        stream: StreamContext<T, R, E>,
        handler_state: &'handler_state mut HandlerState,
        response: Response,
    ) -> futures::future::BoxFuture<'future, HandlerResult>
    where
        'owner: 'future,
        'handler_state: 'future,
        Self: 'future,
    {
        self.as_ref()
            .handle_response(context, stream, handler_state, response)
    }

    fn end_request<'owner, 'result, 'future>(
        &'owner self,
        context: MessageContext,
        stream: StreamContext<T, R, E>,
        result: &'result HandlerResult,
        handler_state: HandlerState,
    ) -> futures::future::BoxFuture<'future, ()>
    where
        'owner: 'future,
        'result: 'future,
        Self: 'future,
    {
        self.as_ref()
            .end_request(context, stream, result, handler_state)
    }
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
    endpoint_name: String,
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
        crate::api::HTTPMethodType::PUT => "PUT",
        crate::api::HTTPMethodType::PATCH => "PATCH",
        crate::api::HTTPMethodType::DELETE => "DELETE",
        crate::api::HTTPMethodType::HEAD => "HEAD",
        crate::api::HTTPMethodType::OPTIONS => "OPTIONS",
        crate::api::HTTPMethodType::TRACE => "TRACE",
        crate::api::HTTPMethodType::CONNECT => "CONNECT",
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
        ]),
    );
    let consumer = Arc::new(EndpointConsumer {
        stream: Arc::downgrade(stream),
        stream_context: StreamContext {
            base: SinkStreamContext::new(Arc::downgrade(stream)),
        },
        client,
        handler,
        endpoint_name: endpoint_config.name,
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
        let span = if stream.environment().tracing_enabled() && context.sampling_enabled() {
            let (stream_name, pipeline_name, component_name) = stream.tracing_labels();
            let span = tracing::info_span!(
                "http.output",
                stream = stream_name,
                pipeline = pipeline_name,
                component = component_name,
                endpoint = self.endpoint_name.as_str(),
                error = tracing::field::Empty,
                otel.status_code = tracing::field::Empty,
                otel.status_message = tracing::field::Empty,
            );
            if !span.is_disabled() {
                let _ = span.set_parent(context.open_telemetry_context().clone());
                Some(span)
            } else {
                None
            }
        } else {
            None
        };
        let context = if let Some(span) = span.as_ref() {
            context.with_span_context(span)
        } else {
            context
        };
        let (handler_context, mut handler_state) = match crate::runtime::common::instrument_if_present!(
            self.handler
                .begin_request(context, self.stream_context.clone()),
            span,
        ) {
            Ok(result) => result,
            Err(error) => {
                if self.request_duration.is_enabled() {
                    self.begin_request_failed.inc();
                }
                crate::runtime::telemetry::record_error_if_present!(span.as_ref(), &error);
                crate::runtime::common::event_if_present!(span.as_ref(), || {
                    tracing::event!(
                        name: "begin_request.error",
                        tracing::Level::ERROR,
                        error = %error,
                        "begin_request failed"
                    );
                });
                return;
            }
        };
        let request_context = handler_context.clone().with_stream_id(new_stream_id());
        crate::runtime::common::event_if_present!(
            span.as_ref(),
            || tracing::event!(name: "begin_request", tracing::Level::INFO, {})
        );

        let started_at = if self.request_duration.is_enabled() {
            self.active_requests.inc();
            Some(std::time::Instant::now())
        } else {
            None
        };
        let mut requester = Requester::default();
        let mut result = crate::runtime::common::instrument_if_present!(
            self.handler.consume_message(
                handler_context.clone(),
                self.stream_context.clone(),
                &mut handler_state,
                value,
                &mut requester,
            ),
            span,
        );
        if let Err(error) = &result {
            crate::runtime::telemetry::record_error_if_present!(span.as_ref(), error);
        }
        crate::runtime::common::event_if_present!(span.as_ref(), || match &result {
            Ok(()) => tracing::event!(name: "consume_message", tracing::Level::INFO, {}),
            Err(error) => tracing::event!(
                name: "consume_message.error",
                tracing::Level::ERROR,
                error = %error,
                "HTTP sink handler failed"
            ),
        });

        let request = if result.is_ok() {
            requester.take_request().map(|mut request| {
                request_context.extend_transport_metadata_with_tracing(
                    &mut request.headers,
                    stream.environment().tracing_enabled(),
                );
                request
            })
        } else {
            Err("HTTP request was not built because ConsumeMessage failed".into())
        };
        // Like Go's deferred Body.Close, keep this guard through EndRequest.
        // It also closes a body that the handler moved into external state.
        let mut _response_body_guard = None;
        match request {
            Ok(request) => {
                let observation = self
                    .http_client_metrics
                    .enabled()
                    .then(|| self.http_client_metrics.start(request.body.len()));
                result = match crate::runtime::common::instrument_if_present!(
                    self.client.perform(request_context, request),
                    span,
                ) {
                    Ok(response) => {
                        _response_body_guard = Some(response.body.close_on_drop());
                        if let Some(observation) = observation {
                            observation.finish(
                                Some(response.status),
                                response.body.content_length(),
                                false,
                            );
                        }
                        crate::runtime::common::event_if_present!(span.as_ref(), || {
                            tracing::event!(name: "http_call", tracing::Level::INFO, status_code = response.status);
                        });
                        let handled = crate::runtime::common::instrument_if_present!(
                            self.handler.handle_response(
                                handler_context.clone(),
                                self.stream_context.clone(),
                                &mut handler_state,
                                response,
                            ),
                            span,
                        );
                        if let Err(error) = &handled {
                            crate::runtime::telemetry::record_error_if_present!(
                                span.as_ref(),
                                error
                            );
                        }
                        crate::runtime::common::event_if_present!(
                            span.as_ref(),
                            || match &handled {
                                Ok(()) => {
                                    tracing::event!(name: "handle_response", tracing::Level::INFO, {})
                                }
                                Err(error) => tracing::event!(
                                    name: "handle_response.error",
                                    tracing::Level::ERROR,
                                    error = %error,
                                    "HTTP response handler failed"
                                ),
                            }
                        );
                        handled
                    }
                    Err(error) => {
                        crate::runtime::telemetry::record_error_if_present!(span.as_ref(), &error);
                        if let Some(observation) = observation {
                            observation.finish(None, None, true);
                        }
                        crate::runtime::common::event_if_present!(span.as_ref(), || {
                            tracing::event!(
                                name: "http_call.error",
                                tracing::Level::ERROR,
                                error = %error,
                                "HTTP client call failed"
                            );
                        });
                        Err(error)
                    }
                };
            }
            Err(error) if result.is_ok() => {
                crate::runtime::telemetry::record_error_if_present!(span.as_ref(), &error);
                crate::runtime::common::event_if_present!(span.as_ref(), || {
                    tracing::event!(
                        name: "no_request.error",
                        tracing::Level::ERROR,
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

        crate::runtime::common::instrument_if_present!(
            self.handler.end_request(
                handler_context,
                self.stream_context.clone(),
                &result,
                handler_state,
            ),
            span,
        );
        if let Some(started_at) = started_at {
            self.active_requests.dec();
            self.request_duration
                .observe(started_at.elapsed().as_secs_f64());
            if result.is_ok() {
                self.messages_total.inc();
            } else {
                self.request_errors.inc();
            }
        }
        if result.is_err() {
            crate::runtime::common::event_if_present!(span.as_ref(), || {
                tracing::error!("HTTP sink request failed");
            });
        }
    }
}
