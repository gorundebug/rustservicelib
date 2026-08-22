use std::{
    collections::HashMap,
    error::Error,
    future::Future,
    pin::Pin,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};

use async_trait::async_trait;
use tokio::{
    sync::{Mutex as AsyncMutex, Notify, RwLock},
    task::JoinHandle,
};
use tokio_util::sync::CancellationToken;
use tracing_opentelemetry::OpenTelemetrySpanExt;

use crate::{
    operators::InputStream,
    runtime::{
        common::{Consumer, MessageContext, Payload, RuntimeEndpointConsumer, new_stream_id},
        datasource::{PendingRequests, StreamContext},
        environment::{
            Lifecycle, RuntimeResult,
            metrics::{Float64Histogram, Int64Counter, Int64Gauge, Labels},
        },
        store::RotatingMap,
    },
};

const PENDING_ROTATION_INTERVAL: Duration = Duration::from_secs(30);

pub type HandlerError = Box<dyn Error + Send + Sync>;
pub type HandlerResult = Result<(), HandlerError>;

#[async_trait]
pub trait DataProducer<T>: Send + Sync
where
    T: Send + Sync + 'static,
{
    async fn start(&self, context: MessageContext, consumer: Arc<dyn Consumer<T>>)
    -> HandlerResult;

    async fn stop(&self, context: MessageContext);
}

pub type ResultCallback<HandlerState, T, R, E> = Arc<
    dyn Fn(
            MessageContext,
            StreamContext<T, R, E>,
            Arc<HandlerState>,
            Payload<R>,
        ) -> Pin<Box<dyn Future<Output = bool> + Send>>
        + Send
        + Sync,
>;

pub struct ResultContext<HandlerState, T, R, E>
where
    HandlerState: Send + Sync + 'static,
    T: Send + Sync + 'static,
    R: Send + Sync + 'static,
    E: Send + Sync + 'static,
{
    callbacks: Mutex<HashMap<String, ResultCallback<HandlerState, T, R, E>>>,
    done: CancellationToken,
    span: tracing::Span,
}

impl<HandlerState, T, R, E> ResultContext<HandlerState, T, R, E>
where
    HandlerState: Send + Sync + 'static,
    T: Send + Sync + 'static,
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
        callback: ResultCallback<HandlerState, T, R, E>,
    ) {
        self.callbacks
            .lock()
            .expect("result callbacks lock poisoned")
            .insert(message_id.into(), callback);
    }

    pub fn done(&self) {
        self.span
            .in_scope(|| tracing::event!(name: "done_called", tracing::Level::INFO, {}));
        self.done.cancel();
    }
}

#[async_trait]
pub trait EndpointHandler<HandlerState, T, R, E>: Send + Sync
where
    HandlerState: Send + Sync + 'static,
    T: Send + Sync + 'static,
    R: Send + Sync + 'static,
    E: Send + Sync + 'static,
{
    fn concurrency(&self, stream: &StreamContext<T, R, E>) -> usize;

    async fn begin_request(
        &self,
        context: MessageContext,
        stream: StreamContext<T, R, E>,
    ) -> Result<(MessageContext, HandlerState), HandlerError>;

    async fn consume_message(
        &self,
        context: MessageContext,
        stream: StreamContext<T, R, E>,
        handler_state: Arc<HandlerState>,
        value: Payload<T>,
        result_context: Arc<ResultContext<HandlerState, T, R, E>>,
    ) -> HandlerResult;

    fn get_message_id(
        &self,
        context: &MessageContext,
        stream: &StreamContext<T, R, E>,
        handler_state: &HandlerState,
        value: &R,
    ) -> String;

    async fn end_request(
        &self,
        context: MessageContext,
        stream: StreamContext<T, R, E>,
        result: &HandlerResult,
        handler_state: Arc<HandlerState>,
    );
}

struct PendingRequest<HandlerState, T, R, E>
where
    HandlerState: Send + Sync + 'static,
    T: Send + Sync + 'static,
    R: Send + Sync + 'static,
    E: Send + Sync + 'static,
{
    handler_state: Arc<HandlerState>,
    result_context: Arc<ResultContext<HandlerState, T, R, E>>,
    lifetime: RwLock<()>,
    span: tracing::Span,
}

struct Concurrency {
    active: AtomicUsize,
    stopped: AtomicBool,
    changed: Notify,
}

struct ActiveRequestGuard<'a> {
    concurrency: &'a Concurrency,
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

impl Drop for ActiveRequestGuard<'_> {
    fn drop(&mut self) {
        self.concurrency.active.fetch_sub(1, Ordering::AcqRel);
        self.concurrency.changed.notify_waiters();
    }
}

pub struct CustomEndpointConsumer<HandlerState, T, R, E, H>
where
    HandlerState: Send + Sync + 'static,
    T: Send + Sync + 'static,
    R: Send + Sync + 'static,
    E: Send + Sync + 'static,
    H: EndpointHandler<HandlerState, T, R, E> + 'static,
{
    input_stream: InputStream<T, R, E>,
    stream_context: StreamContext<T, R, E>,
    handler: H,
    pending: RotatingMap<String, Arc<PendingRequest<HandlerState, T, R, E>>>,
    concurrency: Concurrency,
    metrics: EndpointMetrics,
}

impl<HandlerState, T, R, E, H> CustomEndpointConsumer<HandlerState, T, R, E, H>
where
    HandlerState: Send + Sync + 'static,
    T: Send + Sync + 'static,
    R: Send + Sync + 'static,
    E: Send + Sync + 'static,
    H: EndpointHandler<HandlerState, T, R, E> + 'static,
{
    async fn acquire_concurrency(&self) -> bool {
        loop {
            if self.concurrency.stopped.load(Ordering::Acquire) {
                return false;
            }
            let limit = self.handler.concurrency(&self.stream_context);
            let active = self.concurrency.active.load(Ordering::Acquire);
            if limit == 0 || active < limit {
                if self
                    .concurrency
                    .active
                    .compare_exchange(active, active + 1, Ordering::AcqRel, Ordering::Acquire)
                    .is_ok()
                {
                    return true;
                }
                continue;
            }
            self.concurrency.changed.notified().await;
        }
    }

    async fn endpoint_request(&self, context: MessageContext, value: Payload<T>) {
        if !self.acquire_concurrency().await {
            return;
        }
        let _active_request = ActiveRequestGuard {
            concurrency: &self.concurrency,
        };
        let span = if context.sampling_enabled() {
            let span = tracing::info_span!(
                "local.input",
                stream = self.input_stream.stream().name(),
                endpoint = self.input_stream.stream().name(),
                error = tracing::field::Empty,
                otel.status_code = tracing::field::Empty,
                otel.status_message = tracing::field::Empty,
            );
            let _ = span.set_parent(context.open_telemetry_context().clone());
            span
        } else {
            tracing::Span::none()
        };
        let context = context.with_span_context(&span);

        let begin = crate::runtime::common::instrument_if_enabled(
            self.handler
                .begin_request(context, self.stream_context.clone()),
            span.clone(),
        )
        .await;
        let (handler_context, handler_state) = match begin {
            Ok(value) => value,
            Err(error) => {
                self.metrics.begin_request_failed.inc();
                crate::runtime::telemetry::record_span_error(&span, &error);
                span.in_scope(|| {
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
        span.in_scope(|| tracing::event!(name: "begin_request", tracing::Level::INFO, {}));
        let handler_context = if handler_context.stream_id().is_some() {
            handler_context
        } else {
            handler_context.with_stream_id(new_stream_id())
        };
        let stream_id = handler_context
            .stream_id()
            .expect("stream ID was just assigned")
            .to_owned();
        let handler_state = Arc::new(handler_state);
        let result_context = Arc::new(ResultContext::new(span.clone()));
        let has_result = self.input_stream.result_stream().is_some();
        self.metrics.active_requests.inc();
        let started_at = self
            .metrics
            .request_duration
            .is_enabled()
            .then(Instant::now);
        if has_result {
            if let Err(error) = self.pending.set(
                stream_id.clone(),
                Arc::new(PendingRequest {
                    handler_state: Arc::clone(&handler_state),
                    result_context: Arc::clone(&result_context),
                    lifetime: RwLock::new(()),
                    span: span.clone(),
                }),
            ) {
                let result: HandlerResult = Err(Box::new(error));
                crate::runtime::telemetry::record_span_error(
                    &span,
                    result.as_ref().expect_err("duplicate pending request"),
                );
                crate::runtime::common::instrument_if_enabled(
                    self.handler.end_request(
                        handler_context,
                        self.stream_context.clone(),
                        &result,
                        handler_state,
                    ),
                    span,
                )
                .await;
                self.metrics.active_requests.dec();
                if let Some(started_at) = started_at {
                    self.metrics
                        .request_duration
                        .observe(started_at.elapsed().as_secs_f64());
                }
                self.metrics.request_errors.inc();
                return;
            }
            self.metrics.pending_requests.add(&stream_id);
        }

        let mut result = crate::runtime::common::instrument_if_enabled(
            self.handler.consume_message(
                handler_context.clone(),
                self.stream_context.clone(),
                Arc::clone(&handler_state),
                value,
                Arc::clone(&result_context),
            ),
            span.clone(),
        )
        .await;
        if let Err(error) = &result {
            crate::runtime::telemetry::record_span_error(&span, error);
        }
        span.in_scope(|| match &result {
            Ok(()) => tracing::event!(name: "consume_message", tracing::Level::INFO, {}),
            Err(error) => tracing::event!(
                name: "consume_message.error",
                tracing::Level::ERROR,
                error = %error,
                "custom source handler failed"
            ),
        });

        let mut result_wait_cancelled = false;
        if result.is_ok() && has_result {
            tokio::select! {
                _ = result_context.done.cancelled() => {
                    span.in_scope(|| tracing::event!(name: "done_received", tracing::Level::INFO, {}));
                }
                _ = handler_context.cancelled() => {
                    result_wait_cancelled = true;
                    result = Err(Box::new(std::io::Error::new(
                        std::io::ErrorKind::TimedOut,
                        "request context cancelled before ResultContext::done",
                    )));
                }
            }
        }
        let removed_pending = if has_result {
            let pending = self.pending.pop(&stream_id);
            self.metrics.pending_requests.remove(&stream_id);
            pending
        } else {
            None
        };
        let _lifetime = match &removed_pending {
            Some(pending) => Some(pending.lifetime.write().await),
            None => None,
        };
        if result_wait_cancelled && result_context.done.is_cancelled() {
            result = Ok(());
            span.in_scope(|| tracing::event!(name: "done_received", tracing::Level::INFO, {}));
        } else if result_wait_cancelled {
            crate::runtime::telemetry::record_span_error(&span, "custom source context cancelled");
            span.in_scope(|| {
                tracing::event!(
                    name: "context_cancelled",
                    tracing::Level::WARN,
                    error = "custom source context cancelled"
                );
            });
        }
        crate::runtime::common::instrument_if_enabled(
            self.handler.end_request(
                handler_context,
                self.stream_context.clone(),
                &result,
                handler_state,
            ),
            span.clone(),
        )
        .await;
        self.metrics.active_requests.dec();
        if let Some(started_at) = started_at {
            self.metrics
                .request_duration
                .observe(started_at.elapsed().as_secs_f64());
        }
        if result.is_ok() {
            self.metrics.messages_total.inc();
        } else {
            self.metrics.request_errors.inc();
        }
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
                .in_scope(|| tracing::event!(name: "late_result", tracing::Level::WARN, {}));
            return;
        }
        let message_id = pending.span.in_scope(|| {
            self.handler.get_message_id(
                &context,
                &self.stream_context,
                &pending.handler_state,
                &value,
            )
        });
        let callback = pending
            .result_context
            .callbacks
            .lock()
            .expect("result callbacks lock poisoned")
            .get(&message_id)
            .cloned();
        let Some(callback) = callback else {
            self.metrics.unknown_message_id.inc();
            pending.span.in_scope(|| {
                tracing::event!(
                    name: "unknown_message_id",
                    tracing::Level::WARN,
                    message_id,
                    session_id = stream_id
                )
            });
            return;
        };
        if crate::runtime::common::instrument_if_enabled(
            callback(
                context,
                self.stream_context.clone(),
                Arc::clone(&pending.handler_state),
                value,
            ),
            pending.span.clone(),
        )
        .await
        {
            let removed = pending
                .result_context
                .callbacks
                .lock()
                .expect("result callbacks lock poisoned")
                .remove(&message_id);
            if removed.is_none() {
                self.metrics.duplicate_message_id.inc();
                pending.span.in_scope(|| {
                    tracing::event!(
                        name: "duplicate_message_id",
                        tracing::Level::WARN,
                        message_id,
                        session_id = stream_id
                    )
                });
            }
        }
        pending.span.in_scope(
            || tracing::event!(name: "result_consumed", tracing::Level::INFO, message_id),
        );
    }

    pub fn stop(&self) {
        self.concurrency.stopped.store(true, Ordering::Release);
        self.concurrency.changed.notify_waiters();
    }
}

#[async_trait]
impl<HandlerState, T, R, E, H> Consumer<T> for CustomEndpointConsumer<HandlerState, T, R, E, H>
where
    HandlerState: Send + Sync + 'static,
    T: Send + Sync + 'static,
    R: Send + Sync + 'static,
    E: Send + Sync + 'static,
    H: EndpointHandler<HandlerState, T, R, E> + 'static,
{
    async fn consume(&self, context: MessageContext, value: Payload<T>) {
        self.endpoint_request(context, value).await;
    }
}

struct ResultConsumer<HandlerState, T, R, E, H>
where
    HandlerState: Send + Sync + 'static,
    T: Send + Sync + 'static,
    R: Send + Sync + 'static,
    E: Send + Sync + 'static,
    H: EndpointHandler<HandlerState, T, R, E> + 'static,
{
    endpoint_consumer: Arc<CustomEndpointConsumer<HandlerState, T, R, E, H>>,
}

#[async_trait]
impl<HandlerState, T, R, E, H> Consumer<R> for ResultConsumer<HandlerState, T, R, E, H>
where
    HandlerState: Send + Sync + 'static,
    T: Send + Sync + 'static,
    R: Send + Sync + 'static,
    E: Send + Sync + 'static,
    H: EndpointHandler<HandlerState, T, R, E> + 'static,
{
    async fn consume(&self, context: MessageContext, value: Payload<R>) {
        self.endpoint_consumer.consume_result(context, value).await;
    }
}

pub struct CustomDataSource<HandlerState, T, R, E, H, P>
where
    HandlerState: Send + Sync + 'static,
    T: Send + Sync + 'static,
    R: Send + Sync + 'static,
    E: Send + Sync + 'static,
    H: EndpointHandler<HandlerState, T, R, E> + 'static,
    P: DataProducer<T> + 'static,
{
    endpoint_consumer: Arc<CustomEndpointConsumer<HandlerState, T, R, E, H>>,
    producer: Arc<P>,
    cancellation: CancellationToken,
    producer_task: AsyncMutex<Option<JoinHandle<()>>>,
}

pub fn make_custom_endpoint_consumer<HandlerState, T, R, E, H, P>(
    input_stream: InputStream<T, R, E>,
    producer: P,
    handler: H,
) -> RuntimeResult<Arc<CustomDataSource<HandlerState, T, R, E, H, P>>>
where
    HandlerState: Send + Sync + 'static,
    T: Send + Sync + 'static,
    R: Send + Sync + 'static,
    E: Send + Sync + 'static,
    H: EndpointHandler<HandlerState, T, R, E> + 'static,
    P: DataProducer<T> + 'static,
{
    let scope = input_stream.stream().environment().metrics().scope(
        "datasource_endpoint",
        [
            ("connector".to_owned(), "custom".to_owned()),
            (
                "endpoint".to_owned(),
                input_stream.stream().name().to_owned(),
            ),
            ("protocol".to_owned(), "local".to_owned()),
        ]
        .into_iter()
        .collect(),
    );
    let pending = RotatingMap::new(PENDING_ROTATION_INTERVAL);
    let endpoint_consumer = Arc::new(CustomEndpointConsumer {
        stream_context: StreamContext::new(input_stream.clone()),
        input_stream: input_stream.clone(),
        handler,
        pending: pending.clone(),
        concurrency: Concurrency {
            active: AtomicUsize::new(0),
            stopped: AtomicBool::new(false),
            changed: Notify::new(),
        },
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
    });
    if endpoint_consumer.input_stream.result_stream().is_some() {
        endpoint_consumer
            .input_stream
            .stream()
            .environment()
            .register_storage(Arc::new(pending));
    }
    endpoint_consumer
        .input_stream
        .stream()
        .environment()
        .register_endpoint_consumer(endpoint_consumer.clone())?;
    input_stream.set_result_consumer(Arc::new(ResultConsumer {
        endpoint_consumer: Arc::clone(&endpoint_consumer),
    }));
    Ok(Arc::new(CustomDataSource {
        endpoint_consumer,
        producer: Arc::new(producer),
        cancellation: CancellationToken::new(),
        producer_task: AsyncMutex::new(None),
    }))
}

impl<HandlerState, T, R, E, H> RuntimeEndpointConsumer
    for CustomEndpointConsumer<HandlerState, T, R, E, H>
where
    HandlerState: Send + Sync + 'static,
    T: Send + Sync + 'static,
    R: Send + Sync + 'static,
    E: Send + Sync + 'static,
    H: EndpointHandler<HandlerState, T, R, E> + 'static,
{
    fn id(&self) -> i32 {
        self.input_stream.endpoint_id()
    }

    fn function_implementation(&self) -> &'static str {
        std::any::type_name::<H>()
    }
}

#[async_trait]
impl<HandlerState, T, R, E, H, P> Lifecycle for CustomDataSource<HandlerState, T, R, E, H, P>
where
    HandlerState: Send + Sync + 'static,
    T: Send + Sync + 'static,
    R: Send + Sync + 'static,
    E: Send + Sync + 'static,
    H: EndpointHandler<HandlerState, T, R, E> + 'static,
    P: DataProducer<T> + 'static,
{
    async fn start(&self, context: MessageContext) -> RuntimeResult<()> {
        let producer = Arc::clone(&self.producer);
        let consumer: Arc<dyn Consumer<T>> = self.endpoint_consumer.clone();
        let cancellation = self.cancellation.clone();
        *self.producer_task.lock().await = Some(tokio::spawn(async move {
            tokio::select! {
                _ = producer.start(context, consumer) => {}
                _ = cancellation.cancelled() => {}
            }
        }));
        Ok(())
    }

    async fn stop(&self, context: MessageContext) -> RuntimeResult<()> {
        self.endpoint_consumer.stop();
        self.cancellation.cancel();
        self.producer.stop(context).await;
        if let Some(task) = self.producer_task.lock().await.take() {
            let _ = task.await;
        }
        Ok(())
    }
}
