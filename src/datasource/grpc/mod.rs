use std::{
    collections::{BTreeMap, HashMap},
    error::Error,
    future::Future,
    pin::Pin,
    sync::{Arc, Mutex, Weak},
    time::{Duration, Instant},
};

use async_trait::async_trait;
use tokio::sync::{Mutex as AsyncMutex, RwLock};
use tokio_util::sync::CancellationToken;
use tracing::Instrument;
use tracing_opentelemetry::OpenTelemetrySpanExt;

use crate::{
    operators::InputStream,
    runtime::{
        common::{Consumer, MessageContext, Payload, RuntimeEndpointConsumer, new_stream_id},
        datasource::{PendingRequests, StreamContext as DataSourceStreamContext},
        environment::{
            RuntimeResult,
            metrics::{Float64Histogram, Int64Counter, Int64Gauge, Labels},
        },
        store::RotatingMap,
    },
};

const PENDING_ROTATION_INTERVAL: Duration = Duration::from_secs(30);

mod bidistreaming;
mod clientstreaming;
mod nostreaming;
mod serverstreaming;
mod tonic;

pub use bidistreaming::*;
pub use clientstreaming::*;
pub use nostreaming::*;
pub use serverstreaming::*;
pub use tonic::*;

pub const IMPLEMENTATION: &str = "rust/tonic";

pub type HandlerError = Box<dyn Error + Send + Sync>;
pub type HandlerResult<T = ()> = Result<T, HandlerError>;
pub type BoxFuture<T> = Pin<Box<dyn Future<Output = T> + Send>>;
pub type StreamContext<T, R, E> = DataSourceStreamContext<T, R, E>;

#[async_trait]
pub trait Sender<ResR>: Send + Sync
where
    ResR: Send + 'static,
{
    async fn send(&self, context: MessageContext, value: ResR) -> HandlerResult;
}

pub type ResultCallback<HandlerState, T, ResR, R, E> = Arc<
    dyn Fn(
            MessageContext,
            StreamContext<T, R, E>,
            Arc<AsyncMutex<HandlerState>>,
            Payload<R>,
            Arc<dyn Sender<ResR>>,
        ) -> BoxFuture<bool>
        + Send
        + Sync,
>;

pub struct ResultContext<HandlerState, T, ResR, R, E>
where
    HandlerState: Send + 'static,
    T: Send + Sync + 'static,
    ResR: Send + 'static,
    R: Send + Sync + 'static,
    E: Send + Sync + 'static,
{
    callbacks: Mutex<HashMap<String, ResultCallback<HandlerState, T, ResR, R, E>>>,
    done: CancellationToken,
    span: tracing::Span,
}

impl<HandlerState, T, ResR, R, E> ResultContext<HandlerState, T, ResR, R, E>
where
    HandlerState: Send + 'static,
    T: Send + Sync + 'static,
    ResR: Send + 'static,
    R: Send + Sync + 'static,
    E: Send + Sync + 'static,
{
    fn new(span: tracing::Span) -> Self {
        Self {
            callbacks: Mutex::new(HashMap::new()),
            done: CancellationToken::new(),
            span,
        }
    }

    pub fn set_result_callback(
        &self,
        message_id: impl Into<String>,
        callback: ResultCallback<HandlerState, T, ResR, R, E>,
    ) {
        self.callbacks
            .lock()
            .expect("gRPC source callbacks lock poisoned")
            .insert(message_id.into(), callback);
    }

    pub fn done(&self) {
        self.span
            .in_scope(|| tracing::info!(event.name = "done_called"));
        self.done.cancel();
    }

    pub async fn cancelled(&self) {
        self.done.cancelled().await;
    }
}

#[async_trait]
pub trait EndpointHandler<HandlerState, ReqT, ResR, T, R, E>: Send + Sync
where
    HandlerState: Send + 'static,
    ReqT: Send + 'static,
    ResR: Send + 'static,
    T: Send + Sync + 'static,
    R: Send + Sync + 'static,
    E: Send + Sync + 'static,
{
    async fn begin_request(
        &self,
        context: MessageContext,
        stream: StreamContext<T, R, E>,
    ) -> HandlerResult<(MessageContext, HandlerState)>;

    async fn consume_message(
        &self,
        context: MessageContext,
        stream: StreamContext<T, R, E>,
        handler_state: Arc<AsyncMutex<HandlerState>>,
        request: ReqT,
        result_context: Arc<ResultContext<HandlerState, T, ResR, R, E>>,
        sender: Arc<dyn Sender<ResR>>,
    ) -> HandlerResult<MessageContext>;

    async fn get_message_id(
        &self,
        context: &MessageContext,
        stream: &StreamContext<T, R, E>,
        handler_state: Arc<AsyncMutex<HandlerState>>,
        value: &R,
    ) -> String;

    async fn eof(
        &self,
        context: MessageContext,
        stream: StreamContext<T, R, E>,
        handler_state: Arc<AsyncMutex<HandlerState>>,
    );

    async fn end_request(
        &self,
        context: MessageContext,
        stream: StreamContext<T, R, E>,
        result: &HandlerResult,
        handler_state: Arc<AsyncMutex<HandlerState>>,
    ) -> HandlerResult;
}

pub(crate) struct Pending<HandlerState, T, ResR, R, E>
where
    HandlerState: Send + 'static,
    T: Send + Sync + 'static,
    ResR: Send + 'static,
    R: Send + Sync + 'static,
    E: Send + Sync + 'static,
{
    context: RwLock<MessageContext>,
    state: Arc<AsyncMutex<HandlerState>>,
    sender: Arc<dyn Sender<ResR>>,
    result_context: Arc<ResultContext<HandlerState, T, ResR, R, E>>,
    lifetime: RwLock<()>,
    started_at: Instant,
    span: tracing::Span,
}

struct EndpointMetrics {
    messages_total: Int64Counter,
    request_errors: Int64Counter,
    begin_request_failed: Int64Counter,
    missing_stream_id: Int64Counter,
    late_result: Int64Counter,
    unknown_message_id: Int64Counter,
    duplicate_message_id: Int64Counter,
    active_requests: Int64Gauge,
    pending_requests: PendingRequests,
    request_duration: Float64Histogram,
}

pub(crate) struct GrpcTypedEndpointConsumer<HandlerState, ReqT, ResR, T, R, E, H>
where
    HandlerState: Send + 'static,
    ReqT: Send + 'static,
    ResR: Send + 'static,
    T: Send + Sync + 'static,
    R: Send + Sync + 'static,
    E: Send + Sync + 'static,
    H: EndpointHandler<HandlerState, ReqT, ResR, T, R, E> + 'static,
{
    input_stream: InputStream<T, R, E>,
    stream_context: StreamContext<T, R, E>,
    handler: Arc<H>,
    pending: RotatingMap<String, Arc<Pending<HandlerState, T, ResR, R, E>>>,
    metrics: EndpointMetrics,
    endpoint_name: String,
    _request: std::marker::PhantomData<fn(ReqT)>,
}

impl<HandlerState, ReqT, ResR, T, R, E, H>
    GrpcTypedEndpointConsumer<HandlerState, ReqT, ResR, T, R, E, H>
where
    HandlerState: Send + 'static,
    ReqT: Send + 'static,
    ResR: Send + 'static,
    T: Send + Sync + 'static,
    R: Send + Sync + 'static,
    E: Send + Sync + 'static,
    H: EndpointHandler<HandlerState, ReqT, ResR, T, R, E> + 'static,
{
    pub(crate) fn make(
        input_stream: InputStream<T, R, E>,
        connector_name: &str,
        endpoint_name: &str,
        handler: H,
    ) -> RuntimeResult<Arc<Self>> {
        let labels = [
            ("connector".to_owned(), connector_name.to_owned()),
            ("endpoint".to_owned(), endpoint_name.to_owned()),
            ("protocol".to_owned(), "grpc".to_owned()),
        ]
        .into_iter()
        .collect::<BTreeMap<_, _>>();
        let scope = input_stream
            .stream()
            .environment()
            .metrics()
            .scope("datasource_endpoint", labels);
        let pending = RotatingMap::new(PENDING_ROTATION_INTERVAL);
        let endpoint_consumer = Arc::new(Self {
            stream_context: StreamContext::new(input_stream.clone()),
            input_stream: input_stream.clone(),
            handler: Arc::new(handler),
            pending: pending.clone(),
            metrics: EndpointMetrics {
                messages_total: scope.counter(
                    "messages_total",
                    "Total number of successfully processed messages in data source endpoint",
                    Labels::new(),
                )?,
                request_errors: scope.counter(
                    "events_total",
                    "Total number of events in data source endpoint",
                    [("event".to_owned(), "request_error".to_owned())]
                        .into_iter()
                        .collect(),
                )?,
                begin_request_failed: scope.counter(
                    "events_total",
                    "Total number of events in data source endpoint",
                    [("event".to_owned(), "begin_request_failed".to_owned())]
                        .into_iter()
                        .collect(),
                )?,
                missing_stream_id: scope.counter(
                    "events_total",
                    "Total number of events in data source endpoint",
                    [("event".to_owned(), "missing_stream_id".to_owned())]
                        .into_iter()
                        .collect(),
                )?,
                late_result: scope.counter(
                    "events_total",
                    "Total number of events in data source endpoint",
                    [("event".to_owned(), "late_result".to_owned())]
                        .into_iter()
                        .collect(),
                )?,
                unknown_message_id: scope.counter(
                    "events_total",
                    "Total number of events in data source endpoint",
                    [("event".to_owned(), "unknown_message_id".to_owned())]
                        .into_iter()
                        .collect(),
                )?,
                duplicate_message_id: scope.counter(
                    "events_total",
                    "Total number of events in data source endpoint",
                    [("event".to_owned(), "duplicate_message_id".to_owned())]
                        .into_iter()
                        .collect(),
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
            },
            endpoint_name: endpoint_name.to_owned(),
            _request: std::marker::PhantomData,
        });
        if input_stream.result_stream().is_some() {
            input_stream.set_result_consumer(Arc::new(ResultConsumer {
                endpoint_consumer: Arc::downgrade(&endpoint_consumer),
            }));
        }
        if input_stream.result_stream().is_some() {
            input_stream
                .stream()
                .environment()
                .register_storage(Arc::new(pending));
        }
        input_stream
            .stream()
            .environment()
            .register_endpoint_consumer(endpoint_consumer.clone())?;
        Ok(endpoint_consumer)
    }

    pub(crate) fn has_result(&self) -> bool {
        self.input_stream.result_stream().is_some()
    }

    pub(crate) async fn begin(
        &self,
        context: MessageContext,
        sender: Arc<dyn Sender<ResR>>,
    ) -> HandlerResult<(String, Arc<Pending<HandlerState, T, ResR, R, E>>)> {
        let span = if context.sampling_enabled() {
            let span = tracing::info_span!(
                "grpc.input",
                stream = self.input_stream.stream().name(),
                endpoint = %self.endpoint_name,
                error = tracing::field::Empty,
                otel.status_code = tracing::field::Empty,
                otel.status_message = tracing::field::Empty,
            );
            let _ = span.set_parent(context.open_telemetry_context().clone());
            span
        } else {
            tracing::Span::none()
        };
        // Enter once before extracting the OpenTelemetry context. Some
        // tracing subscribers materialize the child span lazily on enter.
        let child_context = span.in_scope(|| span.context());
        let context = context.with_open_telemetry_context(child_context);
        let (context, state) = match self
            .handler
            .begin_request(context, self.stream_context.clone())
            .instrument(span.clone())
            .await
        {
            Ok(begin) => begin,
            Err(error) => {
                self.metrics.begin_request_failed.inc();
                crate::runtime::telemetry::record_span_error(&span, &error);
                span.in_scope(|| {
                    tracing::error!(
                        event.name = "begin_request.error",
                        error = %error,
                        "begin_request failed"
                    );
                });
                return Err(error);
            }
        };
        span.in_scope(|| tracing::info!(event.name = "begin_request"));
        let context = if context.stream_id().is_some() {
            context
        } else {
            context.with_stream_id(new_stream_id())
        };
        let stream_id = context.stream_id().unwrap().to_owned();
        self.metrics.active_requests.inc();
        let pending = Arc::new(Pending {
            context: RwLock::new(context),
            state: Arc::new(AsyncMutex::new(state)),
            sender,
            result_context: Arc::new(ResultContext::new(span.clone())),
            lifetime: RwLock::new(()),
            started_at: Instant::now(),
            span,
        });
        if self.has_result() {
            if self
                .pending
                .set(stream_id.clone(), Arc::clone(&pending))
                .is_err()
            {
                let result: HandlerResult = Err("duplicate gRPC stream ID".into());
                crate::runtime::telemetry::record_span_error(
                    &pending.span,
                    "duplicate gRPC stream ID",
                );
                let _ = self
                    .handler
                    .end_request(
                        pending.context.read().await.clone(),
                        self.stream_context.clone(),
                        &result,
                        Arc::clone(&pending.state),
                    )
                    .instrument(pending.span.clone())
                    .await;
                self.metrics.active_requests.dec();
                self.metrics
                    .request_duration
                    .observe(pending.started_at.elapsed().as_secs_f64());
                self.metrics.request_errors.inc();
                return match result {
                    Err(error) => Err(error),
                    Ok(()) => unreachable!("duplicate stream ID is always an error"),
                };
            }
            self.metrics.pending_requests.add(&stream_id);
        }
        Ok((stream_id, pending))
    }

    pub(crate) async fn consume(
        &self,
        pending: &Arc<Pending<HandlerState, T, ResR, R, E>>,
        request: ReqT,
    ) -> HandlerResult {
        let context = pending.context.read().await.clone();
        let result = self
            .handler
            .consume_message(
                context,
                self.stream_context.clone(),
                Arc::clone(&pending.state),
                request,
                Arc::clone(&pending.result_context),
                Arc::clone(&pending.sender),
            )
            .instrument(pending.span.clone())
            .await;
        if let Err(error) = &result {
            crate::runtime::telemetry::record_span_error(&pending.span, error);
        }
        pending.span.in_scope(|| match &result {
            Ok(_) => tracing::info!(event.name = "consume_message"),
            Err(error) => tracing::error!(
                event.name = "consume_message.error",
                error = %error,
                "gRPC source handler failed"
            ),
        });
        let context = result?;
        *pending.context.write().await = context;
        Ok(())
    }

    pub(crate) async fn eof(&self, pending: &Arc<Pending<HandlerState, T, ResR, R, E>>) {
        self.handler
            .eof(
                pending.context.read().await.clone(),
                self.stream_context.clone(),
                Arc::clone(&pending.state),
            )
            .instrument(pending.span.clone())
            .await;
        pending.span.in_scope(|| tracing::info!(event.name = "eof"));
    }

    pub(crate) async fn wait_done(
        &self,
        pending: &Arc<Pending<HandlerState, T, ResR, R, E>>,
    ) -> HandlerResult {
        let context = pending.context.read().await.clone();
        tokio::select! {
            _ = pending.result_context.cancelled() => {
                pending.span.in_scope(|| tracing::info!(event.name = "done_received"));
                Ok(())
            },
            _ = context.cancelled() => {
                crate::runtime::telemetry::record_span_error(
                    &pending.span,
                    "gRPC request context cancelled",
                );
                pending.span.in_scope(|| tracing::warn!(
                    event.name = "context_cancelled",
                    error = "gRPC request context cancelled"
                ));
                Err("gRPC request context cancelled".into())
            }
        }
    }

    pub(crate) async fn finish(
        &self,
        stream_id: &str,
        pending: Arc<Pending<HandlerState, T, ResR, R, E>>,
        mut result: HandlerResult,
    ) -> HandlerResult {
        let removed = if self.has_result() {
            let removed = self.pending.pop(stream_id);
            self.metrics.pending_requests.remove(stream_id);
            removed
        } else {
            None
        };
        let _lifetime = match &removed {
            Some(pending) => Some(pending.lifetime.write().await),
            None => None,
        };
        let end_result = self
            .handler
            .end_request(
                pending.context.read().await.clone(),
                self.stream_context.clone(),
                &result,
                Arc::clone(&pending.state),
            )
            .instrument(pending.span.clone())
            .await;
        if end_result.is_err() {
            result = end_result;
        }
        self.metrics.active_requests.dec();
        self.metrics
            .request_duration
            .observe(pending.started_at.elapsed().as_secs_f64());
        if result.is_ok() {
            self.metrics.messages_total.inc();
        } else {
            self.metrics.request_errors.inc();
            pending.span.in_scope(|| {
                tracing::error!("gRPC source request failed");
            });
        }
        result
    }

    async fn consume_result(&self, context: MessageContext, value: Payload<R>) {
        let Some(stream_id) = context.stream_id().map(str::to_owned) else {
            self.metrics.missing_stream_id.inc();
            tracing::error!("consumeResult called without streamID");
            return;
        };
        let Some(pending) = self.pending.get(&stream_id) else {
            self.metrics.late_result.inc();
            tracing::warn!(
                session_id = stream_id,
                "consumeResult: session not found in pending"
            );
            return;
        };
        let _lifetime = pending.lifetime.read().await;
        if !self
            .pending
            .get(&stream_id)
            .is_some_and(|current| Arc::ptr_eq(&current, &pending))
        {
            self.metrics.late_result.inc();
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
            .expect("gRPC source callbacks lock poisoned")
            .get(&message_id)
            .cloned();
        let Some(callback) = callback else {
            self.metrics.unknown_message_id.inc();
            pending.span.in_scope(|| {
                tracing::warn!(
                    event.name = "unknown_message_id",
                    message_id,
                    session_id = stream_id
                )
            });
            return;
        };
        if callback(
            context,
            self.stream_context.clone(),
            Arc::clone(&pending.state),
            value,
            Arc::clone(&pending.sender),
        )
        .instrument(pending.span.clone())
        .await
        {
            let removed = pending
                .result_context
                .callbacks
                .lock()
                .expect("gRPC source callbacks lock poisoned")
                .remove(&message_id);
            if removed.is_none() {
                self.metrics.duplicate_message_id.inc();
                pending.span.in_scope(|| {
                    tracing::warn!(
                        event.name = "duplicate_message_id",
                        message_id,
                        session_id = stream_id
                    )
                });
            }
        }
        pending
            .span
            .in_scope(|| tracing::info!(event.name = "result_consumed", message_id));
    }
}

impl<HandlerState, ReqT, ResR, T, R, E, H> RuntimeEndpointConsumer
    for GrpcTypedEndpointConsumer<HandlerState, ReqT, ResR, T, R, E, H>
where
    HandlerState: Send + 'static,
    ReqT: Send + 'static,
    ResR: Send + 'static,
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

struct ResultConsumer<HandlerState, ReqT, ResR, T, R, E, H>
where
    HandlerState: Send + 'static,
    ReqT: Send + 'static,
    ResR: Send + 'static,
    T: Send + Sync + 'static,
    R: Send + Sync + 'static,
    E: Send + Sync + 'static,
    H: EndpointHandler<HandlerState, ReqT, ResR, T, R, E> + 'static,
{
    endpoint_consumer: Weak<GrpcTypedEndpointConsumer<HandlerState, ReqT, ResR, T, R, E, H>>,
}

#[async_trait]
impl<HandlerState, ReqT, ResR, T, R, E, H> Consumer<R>
    for ResultConsumer<HandlerState, ReqT, ResR, T, R, E, H>
where
    HandlerState: Send + 'static,
    ReqT: Send + 'static,
    ResR: Send + 'static,
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
