use std::{
    collections::HashMap,
    error::Error,
    future::Future,
    pin::Pin,
    sync::{Arc, Mutex},
};

use async_trait::async_trait;
use axum::{
    Router,
    body::{Body, to_bytes},
    extract::Request,
    http::{HeaderMap, Method, StatusCode},
    response::Response,
    routing::any,
};
use tokio::sync::{Mutex as AsyncMutex, RwLock};
use tokio::{net::TcpListener, task::JoinHandle};
use tokio_util::sync::CancellationToken;
use tracing::Instrument;
use tracing_opentelemetry::OpenTelemetrySpanExt;

use crate::{
    operators::InputStream,
    runtime::{
        common::{Consumer, MessageContext, Payload, RuntimeEndpointConsumer, new_stream_id},
        config::{HttpDataConnectorConfig, HttpEndpointConfig},
        datasource::{DataSource, PendingRequests, StreamContext},
        environment::{Lifecycle, RuntimeError, RuntimeResult, metrics::Labels},
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

pub struct AxumDataSource {
    config: HttpDataConnectorConfig,
    router: Mutex<Router>,
    started: Mutex<bool>,
    shutdown: Mutex<CancellationToken>,
    server_task: AsyncMutex<Option<JoinHandle<Result<(), String>>>>,
}

impl AxumDataSource {
    pub fn new(config: HttpDataConnectorConfig) -> Arc<Self> {
        Arc::new(Self {
            config,
            router: Mutex::new(Router::new()),
            started: Mutex::new(false),
            shutdown: Mutex::new(CancellationToken::new()),
            server_task: AsyncMutex::new(None),
        })
    }

    pub fn router(&self) -> Router {
        self.router
            .lock()
            .expect("HTTP datasource router lock poisoned")
            .clone()
    }

    pub fn add_endpoint<HandlerState, ReqT, ResR, T, R, E, H>(
        self: &Arc<Self>,
        input_stream: InputStream<T, R, E>,
        endpoint_config: HttpEndpointConfig,
        handler: H,
    ) -> RuntimeResult<Arc<EndpointConsumer<HandlerState, ReqT, ResR, T, R, E, H>>>
    where
        HandlerState: Send + 'static,
        ReqT: Send + Sync + 'static,
        ResR: Send + Sync + 'static,
        T: Send + Sync + 'static,
        R: Send + Sync + 'static,
        E: Send + Sync + 'static,
        H: EndpointHandler<HandlerState, ReqT, ResR, T, R, E> + 'static,
    {
        if endpoint_config.id_data_connector != self.config.id {
            return Err(RuntimeError::InvalidConfiguration(format!(
                "HTTP endpoint {:?} references connector {}, expected {}",
                endpoint_config.name, endpoint_config.id_data_connector, self.config.id
            )));
        }
        if *self
            .started
            .lock()
            .expect("HTTP datasource lifecycle lock poisoned")
        {
            return Err(RuntimeError::ResourceAlreadyStarted(
                self.config.name.clone(),
            ));
        }
        let endpoint = make_endpoint_consumer(
            input_stream,
            endpoint_config.clone(),
            &self.config.name,
            handler,
        )?;
        let mut router = self
            .router
            .lock()
            .expect("HTTP datasource router lock poisoned");
        *router = std::mem::take(&mut *router).merge(endpoint.router(&endpoint_config));
        Ok(endpoint)
    }

    fn listen_address(&self) -> String {
        if self.config.address.is_empty() {
            format!("{}:{}", self.config.host, self.config.port)
        } else {
            self.config.address.clone()
        }
    }
}

impl DataSource for AxumDataSource {
    fn id(&self) -> i32 {
        self.config.id
    }

    fn name(&self) -> &str {
        &self.config.name
    }
}

#[async_trait]
impl Lifecycle for AxumDataSource {
    async fn start(&self, _context: MessageContext) -> RuntimeResult<()> {
        {
            let mut started = self
                .started
                .lock()
                .expect("HTTP datasource lifecycle lock poisoned");
            if *started {
                return Err(RuntimeError::ResourceAlreadyStarted(
                    self.config.name.clone(),
                ));
            }
            *started = true;
        }
        if !self.config.use_dedicated_listener {
            return Ok(());
        }

        let listener = match TcpListener::bind(self.listen_address()).await {
            Ok(listener) => listener,
            Err(error) => {
                *self
                    .started
                    .lock()
                    .expect("HTTP datasource lifecycle lock poisoned") = false;
                return Err(RuntimeError::Transport(error.to_string()));
            }
        };
        let shutdown = CancellationToken::new();
        *self
            .shutdown
            .lock()
            .expect("HTTP datasource shutdown lock poisoned") = shutdown.clone();
        let router = self.router();
        *self.server_task.lock().await = Some(tokio::spawn(async move {
            axum::serve(listener, router)
                .with_graceful_shutdown(shutdown.cancelled_owned())
                .await
                .map_err(|error| error.to_string())
        }));
        Ok(())
    }

    async fn stop(&self, _context: MessageContext) -> RuntimeResult<()> {
        self.shutdown
            .lock()
            .expect("HTTP datasource shutdown lock poisoned")
            .cancel();
        if let Some(task) = self.server_task.lock().await.take() {
            task.await
                .map_err(|error| RuntimeError::Transport(error.to_string()))?
                .map_err(RuntimeError::Transport)?;
        }
        *self
            .started
            .lock()
            .expect("HTTP datasource lifecycle lock poisoned") = false;
        Ok(())
    }
}

#[derive(Clone)]
pub struct HandlerData {
    pub method: Method,
    pub uri: axum::http::Uri,
    pub headers: HeaderMap,
    pub body: Arc<Vec<u8>>,
    response: Arc<Mutex<ResponseData>>,
}

struct ResponseData {
    status: StatusCode,
    headers: HeaderMap,
    body: Vec<u8>,
}

impl HandlerData {
    pub fn set_status(&self, status: StatusCode) {
        self.response
            .lock()
            .expect("HTTP response lock poisoned")
            .status = status;
    }

    pub fn set_header(&self, name: axum::http::header::HeaderName, value: axum::http::HeaderValue) {
        self.response
            .lock()
            .expect("HTTP response lock poisoned")
            .headers
            .insert(name, value);
    }

    pub fn set_response_body(&self, body: impl Into<Vec<u8>>) {
        self.response
            .lock()
            .expect("HTTP response lock poisoned")
            .body = body.into();
    }

    fn into_response(self) -> Response<Body> {
        let mut response = self.response.lock().expect("HTTP response lock poisoned");
        let mut result = Response::new(Body::from(std::mem::take(&mut response.body)));
        *result.status_mut() = response.status;
        *result.headers_mut() = response.headers.clone();
        result
    }
}

type ResultCallbackFn<HandlerState, T, R, E> = dyn Fn(
        MessageContext,
        StreamContext<T, R, E>,
        Arc<AsyncMutex<HandlerState>>,
        Payload<R>,
        HandlerData,
    ) -> Pin<Box<dyn Future<Output = bool> + Send>>
    + Send
    + Sync;

pub struct ResultCallback<HandlerState, ReqT, ResR, T, R, E>
where
    HandlerState: Send + 'static,
    ReqT: Send + Sync + 'static,
    ResR: Send + Sync + 'static,
    T: Send + Sync + 'static,
    R: Send + Sync + 'static,
    E: Send + Sync + 'static,
{
    callback: Arc<ResultCallbackFn<HandlerState, T, R, E>>,
    _types: std::marker::PhantomData<fn(ReqT, ResR)>,
}

impl<HandlerState, ReqT, ResR, T, R, E> ResultCallback<HandlerState, ReqT, ResR, T, R, E>
where
    HandlerState: Send + 'static,
    ReqT: Send + Sync + 'static,
    ResR: Send + Sync + 'static,
    T: Send + Sync + 'static,
    R: Send + Sync + 'static,
    E: Send + Sync + 'static,
{
    pub fn new<F>(callback: F) -> Self
    where
        F: Fn(
                MessageContext,
                StreamContext<T, R, E>,
                Arc<AsyncMutex<HandlerState>>,
                Payload<R>,
                HandlerData,
            ) -> Pin<Box<dyn Future<Output = bool> + Send>>
            + Send
            + Sync
            + 'static,
    {
        Self {
            callback: Arc::new(callback),
            _types: std::marker::PhantomData,
        }
    }

    async fn call(
        &self,
        context: MessageContext,
        stream: StreamContext<T, R, E>,
        state: Arc<AsyncMutex<HandlerState>>,
        value: Payload<R>,
        data: HandlerData,
    ) -> bool {
        (self.callback)(context, stream, state, value, data).await
    }
}

impl<HandlerState, ReqT, ResR, T, R, E> Clone for ResultCallback<HandlerState, ReqT, ResR, T, R, E>
where
    HandlerState: Send + 'static,
    ReqT: Send + Sync + 'static,
    ResR: Send + Sync + 'static,
    T: Send + Sync + 'static,
    R: Send + Sync + 'static,
    E: Send + Sync + 'static,
{
    fn clone(&self) -> Self {
        Self {
            callback: Arc::clone(&self.callback),
            _types: std::marker::PhantomData,
        }
    }
}

pub struct ResultContext<HandlerState, ReqT, ResR, T, R, E>
where
    HandlerState: Send + 'static,
    ReqT: Send + Sync + 'static,
    ResR: Send + Sync + 'static,
    T: Send + Sync + 'static,
    R: Send + Sync + 'static,
    E: Send + Sync + 'static,
{
    callbacks: Mutex<HashMap<String, ResultCallback<HandlerState, ReqT, ResR, T, R, E>>>,
    done: tokio_util::sync::CancellationToken,
    span: tracing::Span,
    _types: std::marker::PhantomData<fn(ReqT, ResR)>,
}

impl<HandlerState, ReqT, ResR, T, R, E> ResultContext<HandlerState, ReqT, ResR, T, R, E>
where
    HandlerState: Send + 'static,
    ReqT: Send + Sync + 'static,
    ResR: Send + Sync + 'static,
    T: Send + Sync + 'static,
    R: Send + Sync + 'static,
    E: Send + Sync + 'static,
{
    fn new(span: tracing::Span) -> Self {
        Self {
            callbacks: Mutex::new(HashMap::new()),
            done: tokio_util::sync::CancellationToken::new(),
            span,
            _types: std::marker::PhantomData,
        }
    }

    pub fn set_result_callback(
        &self,
        message_id: impl Into<String>,
        callback: ResultCallback<HandlerState, ReqT, ResR, T, R, E>,
    ) {
        self.callbacks
            .lock()
            .expect("HTTP result callbacks lock poisoned")
            .insert(message_id.into(), callback);
    }

    pub fn done(&self) {
        self.span
            .in_scope(|| tracing::info!(event.name = "done_called"));
        self.done.cancel();
    }
}

#[async_trait]
pub trait EndpointHandler<HandlerState, ReqT, ResR, T, R, E>: Send + Sync
where
    HandlerState: Send + 'static,
    ReqT: Send + Sync + 'static,
    ResR: Send + Sync + 'static,
    T: Send + Sync + 'static,
    R: Send + Sync + 'static,
    E: Send + Sync + 'static,
{
    async fn begin_request(
        &self,
        context: MessageContext,
        stream: StreamContext<T, R, E>,
        data: HandlerData,
    ) -> Result<(MessageContext, HandlerState), HandlerError>;

    async fn consume_message(
        &self,
        context: MessageContext,
        stream: StreamContext<T, R, E>,
        handler_state: Arc<AsyncMutex<HandlerState>>,
        data: HandlerData,
        result_context: Arc<ResultContext<HandlerState, ReqT, ResR, T, R, E>>,
    ) -> HandlerResult;

    async fn get_message_id(
        &self,
        context: &MessageContext,
        stream: &StreamContext<T, R, E>,
        handler_state: Arc<AsyncMutex<HandlerState>>,
        value: &R,
    ) -> String;

    async fn end_request(
        &self,
        context: MessageContext,
        stream: StreamContext<T, R, E>,
        result: &HandlerResult,
        handler_state: Arc<AsyncMutex<HandlerState>>,
        data: HandlerData,
    );
}

struct PendingRequest<HandlerState, ReqT, ResR, T, R, E>
where
    HandlerState: Send + 'static,
    ReqT: Send + Sync + 'static,
    ResR: Send + Sync + 'static,
    T: Send + Sync + 'static,
    R: Send + Sync + 'static,
    E: Send + Sync + 'static,
{
    state: Arc<AsyncMutex<HandlerState>>,
    data: HandlerData,
    result_context: Arc<ResultContext<HandlerState, ReqT, ResR, T, R, E>>,
    lifetime: RwLock<()>,
    span: tracing::Span,
}

struct RequestCancellationGuard(MessageContext);

impl Drop for RequestCancellationGuard {
    fn drop(&mut self) {
        self.0.cancel();
    }
}

pub struct EndpointConsumer<HandlerState, ReqT, ResR, T, R, E, H>
where
    HandlerState: Send + 'static,
    ReqT: Send + Sync + 'static,
    ResR: Send + Sync + 'static,
    T: Send + Sync + 'static,
    R: Send + Sync + 'static,
    E: Send + Sync + 'static,
    H: EndpointHandler<HandlerState, ReqT, ResR, T, R, E> + 'static,
{
    input_stream: InputStream<T, R, E>,
    stream_context: StreamContext<T, R, E>,
    handler: H,
    pending: RwLock<HashMap<String, Arc<PendingRequest<HandlerState, ReqT, ResR, T, R, E>>>>,
    messages_total: crate::runtime::environment::metrics::Int64Counter,
    request_errors: crate::runtime::environment::metrics::Int64Counter,
    begin_request_failed: crate::runtime::environment::metrics::Int64Counter,
    missing_stream_id: crate::runtime::environment::metrics::Int64Counter,
    late_result: crate::runtime::environment::metrics::Int64Counter,
    unknown_message_id: crate::runtime::environment::metrics::Int64Counter,
    duplicate_message_id: crate::runtime::environment::metrics::Int64Counter,
    invalid_http_method: crate::runtime::environment::metrics::Int64Counter,
    active_requests: crate::runtime::environment::metrics::Int64Gauge,
    pending_requests: PendingRequests,
    request_duration: crate::runtime::environment::metrics::Float64Histogram,
    endpoint_name: String,
}

pub fn make_endpoint_consumer<HandlerState, ReqT, ResR, T, R, E, H>(
    input_stream: InputStream<T, R, E>,
    endpoint_config: HttpEndpointConfig,
    connector_name: &str,
    handler: H,
) -> RuntimeResult<Arc<EndpointConsumer<HandlerState, ReqT, ResR, T, R, E, H>>>
where
    HandlerState: Send + 'static,
    ReqT: Send + Sync + 'static,
    ResR: Send + Sync + 'static,
    T: Send + Sync + 'static,
    R: Send + Sync + 'static,
    E: Send + Sync + 'static,
    H: EndpointHandler<HandlerState, ReqT, ResR, T, R, E> + 'static,
{
    if endpoint_config.path.is_empty() {
        return Err(RuntimeError::InvalidConfiguration(
            "HTTP datasource endpoint path is empty".to_owned(),
        ));
    }
    let scope = input_stream.stream().environment().metrics().scope(
        "datasource_endpoint",
        labels(&[
            ("connector", connector_name),
            ("endpoint", endpoint_config.name.as_str()),
            ("protocol", "http"),
        ]),
    );
    let consumer = Arc::new(EndpointConsumer {
        stream_context: StreamContext::new(input_stream.clone()),
        input_stream: input_stream.clone(),
        handler,
        pending: RwLock::new(HashMap::new()),
        messages_total: scope.counter(
            "messages_total",
            "Total number of successfully processed messages in data source endpoint",
            Labels::new(),
        )?,
        request_errors: scope.counter(
            "events_total",
            "Total number of events in data source endpoint",
            labels(&[("event", "request_error")]),
        )?,
        begin_request_failed: scope.counter(
            "events_total",
            "Total number of events in data source endpoint",
            labels(&[("event", "begin_request_failed")]),
        )?,
        missing_stream_id: scope.counter(
            "events_total",
            "Total number of events in data source endpoint",
            labels(&[("event", "missing_stream_id")]),
        )?,
        late_result: scope.counter(
            "events_total",
            "Total number of events in data source endpoint",
            labels(&[("event", "late_result")]),
        )?,
        unknown_message_id: scope.counter(
            "events_total",
            "Total number of events in data source endpoint",
            labels(&[("event", "unknown_message_id")]),
        )?,
        duplicate_message_id: scope.counter(
            "events_total",
            "Total number of events in data source endpoint",
            labels(&[("event", "duplicate_message_id")]),
        )?,
        invalid_http_method: scope.counter(
            "events_total",
            "Total number of events in data source endpoint",
            labels(&[("event", "invalid_http_method")]),
        )?,
        active_requests: scope.gauge(
            "active_requests",
            "Number of active requests in data source endpoint",
            Labels::new(),
        )?,
        pending_requests: PendingRequests::new(&scope)?,
        request_duration: scope.histogram(
            "request_duration_seconds",
            "Request duration in seconds for data source endpoint",
            Labels::new(),
            None,
        )?,
        endpoint_name: endpoint_config.name.clone(),
    });
    if input_stream.result_stream().is_some() {
        input_stream.set_result_consumer(Arc::new(ResultConsumer {
            endpoint_consumer: Arc::downgrade(&consumer),
        }));
    }
    input_stream
        .stream()
        .environment()
        .register_endpoint_consumer(consumer.clone())?;
    Ok(consumer)
}

impl<HandlerState, ReqT, ResR, T, R, E, H> RuntimeEndpointConsumer
    for EndpointConsumer<HandlerState, ReqT, ResR, T, R, E, H>
where
    HandlerState: Send + 'static,
    ReqT: Send + Sync + 'static,
    ResR: Send + Sync + 'static,
    T: Send + Sync + 'static,
    R: Send + Sync + 'static,
    E: Send + Sync + 'static,
    H: EndpointHandler<HandlerState, ReqT, ResR, T, R, E> + 'static,
{
    fn id(&self) -> i32 {
        self.input_stream.endpoint_id()
    }

    fn function_implementation(&self) -> &'static str {
        std::any::type_name::<H>()
    }
}

impl<HandlerState, ReqT, ResR, T, R, E, H> EndpointConsumer<HandlerState, ReqT, ResR, T, R, E, H>
where
    HandlerState: Send + 'static,
    ReqT: Send + Sync + 'static,
    ResR: Send + Sync + 'static,
    T: Send + Sync + 'static,
    R: Send + Sync + 'static,
    E: Send + Sync + 'static,
    H: EndpointHandler<HandlerState, ReqT, ResR, T, R, E> + 'static,
{
    pub fn router(self: &Arc<Self>, config: &HttpEndpointConfig) -> Router {
        let consumer = Arc::clone(self);
        let expected_method = match config.http_method_type {
            crate::api::HTTPMethodType::GET => Method::GET,
            crate::api::HTTPMethodType::POST => Method::POST,
            crate::api::HTTPMethodType::Undefined => return Router::new(),
        };
        Router::new().route(
            &config.path,
            any(move |request| {
                let consumer = Arc::clone(&consumer);
                let expected_method = expected_method.clone();
                async move { consumer.serve_http(request, expected_method).await }
            }),
        )
    }

    async fn serve_http(&self, request: Request, expected_method: Method) -> Response<Body> {
        if request.method() != expected_method {
            self.invalid_http_method.inc();
            return Response::builder()
                .status(StatusCode::METHOD_NOT_ALLOWED)
                .body(Body::empty())
                .expect("valid HTTP method error response");
        }
        let (parts, body) = request.into_parts();
        let body = match to_bytes(body, usize::MAX).await {
            Ok(body) => body.to_vec(),
            Err(error) => {
                return Response::builder()
                    .status(StatusCode::BAD_REQUEST)
                    .body(Body::from(error.to_string()))
                    .expect("valid HTTP error response");
            }
        };
        let response = Arc::new(Mutex::new(ResponseData {
            status: StatusCode::OK,
            headers: HeaderMap::new(),
            body: Vec::new(),
        }));
        let data = HandlerData {
            method: parts.method,
            uri: parts.uri,
            headers: parts.headers,
            body: Arc::new(body),
            response,
        };
        let metadata = data
            .headers
            .iter()
            .filter_map(|(name, value)| {
                value
                    .to_str()
                    .ok()
                    .map(|value| (name.as_str().to_owned(), value.to_owned()))
            })
            .collect();
        let context = MessageContext::new().with_metadata(metadata);
        let span = if context.sampling_enabled() {
            let span = tracing::info_span!(
                "http.input",
                stream = self.input_stream.stream().name(),
                endpoint = %self.endpoint_name,
                method = %data.method,
                path = %data.uri.path(),
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
        let begin = self
            .handler
            .begin_request(context, self.stream_context.clone(), data.clone())
            .instrument(span.clone())
            .await;
        let (context, state) = match begin {
            Ok(begin) => begin,
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
                return data.into_response();
            }
        };
        span.in_scope(|| tracing::info!(event.name = "begin_request"));
        let context = if context.stream_id().is_some() {
            context
        } else {
            context.with_stream_id(new_stream_id())
        };
        let _request_cancellation = RequestCancellationGuard(context.clone());
        self.active_requests.inc();
        let started_at = std::time::Instant::now();
        let stream_id = context.stream_id().unwrap().to_owned();
        let state = Arc::new(AsyncMutex::new(state));
        let result_context = Arc::new(ResultContext::new(span.clone()));
        let has_result = self.input_stream.result_stream().is_some();
        if has_result {
            self.pending.write().await.insert(
                stream_id.clone(),
                Arc::new(PendingRequest {
                    state: Arc::clone(&state),
                    data: data.clone(),
                    result_context: Arc::clone(&result_context),
                    lifetime: RwLock::new(()),
                    span: span.clone(),
                }),
            );
            self.pending_requests.add(&stream_id);
        }

        let mut result = self
            .handler
            .consume_message(
                context.clone(),
                self.stream_context.clone(),
                Arc::clone(&state),
                data.clone(),
                Arc::clone(&result_context),
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
                "HTTP source handler failed"
            ),
        });
        if result.is_ok() && has_result {
            tokio::select! {
                _ = result_context.done.cancelled() => {
                    span.in_scope(|| tracing::info!(event.name = "done_received"));
                }
                _ = context.cancelled() => {
                    crate::runtime::telemetry::record_span_error(
                        &span,
                        "HTTP request context cancelled",
                    );
                    span.in_scope(|| {
                        tracing::warn!(
                            event.name = "context_cancelled",
                            error = "HTTP request context cancelled",
                            "HTTP source context cancelled"
                        );
                    });
                    result = Err("HTTP request context cancelled before ResultContext::done".into());
                }
            }
        }
        let removed_pending = if has_result {
            let pending = self.pending.write().await.remove(&stream_id);
            self.pending_requests.remove(&stream_id);
            pending
        } else {
            None
        };
        let _lifetime = match &removed_pending {
            Some(pending) => Some(pending.lifetime.write().await),
            None => None,
        };
        self.handler
            .end_request(
                context,
                self.stream_context.clone(),
                &result,
                state,
                data.clone(),
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
                tracing::error!("HTTP source request failed");
            });
        }
        data.into_response()
    }

    async fn consume_result(&self, context: MessageContext, value: Payload<R>) {
        let Some(stream_id) = context.stream_id().map(str::to_owned) else {
            self.missing_stream_id.inc();
            tracing::error!("consumeResult called without streamID");
            return;
        };
        let Some(pending) = self.pending.read().await.get(&stream_id).cloned() else {
            self.late_result.inc();
            tracing::warn!(
                session_id = stream_id,
                "consumeResult: session not found in pending"
            );
            return;
        };
        let _lifetime = pending.lifetime.read().await;
        if !self
            .pending
            .read()
            .await
            .get(&stream_id)
            .is_some_and(|current| Arc::ptr_eq(current, &pending))
        {
            self.late_result.inc();
            pending
                .span
                .in_scope(|| tracing::warn!(event.name = "late_result"));
            return;
        }
        let message_id = self
            .handler
            .get_message_id(
                &context,
                &self.stream_context,
                Arc::clone(&pending.state),
                &value,
            )
            .instrument(pending.span.clone())
            .await;
        let callback = pending
            .result_context
            .callbacks
            .lock()
            .expect("HTTP result callbacks lock poisoned")
            .get(&message_id)
            .cloned();
        let Some(callback) = callback else {
            self.unknown_message_id.inc();
            pending.span.in_scope(|| {
                tracing::warn!(
                    event.name = "unknown_message_id",
                    message_id,
                    session_id = stream_id
                );
            });
            return;
        };
        if callback
            .call(
                context,
                self.stream_context.clone(),
                Arc::clone(&pending.state),
                value,
                pending.data.clone(),
            )
            .instrument(pending.span.clone())
            .await
        {
            let removed = pending
                .result_context
                .callbacks
                .lock()
                .expect("HTTP result callbacks lock poisoned")
                .remove(&message_id);
            if removed.is_none() {
                self.duplicate_message_id.inc();
                pending.span.in_scope(|| {
                    tracing::warn!(
                        event.name = "duplicate_message_id",
                        message_id,
                        session_id = stream_id
                    );
                });
            }
        }
        pending.span.in_scope(|| {
            tracing::info!(event.name = "result_consumed", message_id);
        });
    }
}

struct ResultConsumer<HandlerState, ReqT, ResR, T, R, E, H>
where
    HandlerState: Send + 'static,
    ReqT: Send + Sync + 'static,
    ResR: Send + Sync + 'static,
    T: Send + Sync + 'static,
    R: Send + Sync + 'static,
    E: Send + Sync + 'static,
    H: EndpointHandler<HandlerState, ReqT, ResR, T, R, E> + 'static,
{
    endpoint_consumer: std::sync::Weak<EndpointConsumer<HandlerState, ReqT, ResR, T, R, E, H>>,
}

#[async_trait]
impl<HandlerState, ReqT, ResR, T, R, E, H> Consumer<R>
    for ResultConsumer<HandlerState, ReqT, ResR, T, R, E, H>
where
    HandlerState: Send + 'static,
    ReqT: Send + Sync + 'static,
    ResR: Send + Sync + 'static,
    T: Send + Sync + 'static,
    R: Send + Sync + 'static,
    E: Send + Sync + 'static,
    H: EndpointHandler<HandlerState, ReqT, ResR, T, R, E> + 'static,
{
    async fn consume(&self, context: MessageContext, value: Payload<R>) {
        if let Some(endpoint_consumer) = self.endpoint_consumer.upgrade() {
            endpoint_consumer.consume_result(context, value).await;
        }
    }
}
