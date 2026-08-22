use std::{
    error::Error,
    sync::{Arc, RwLock, Weak},
    time::Instant,
};

use async_trait::async_trait;
use tracing_opentelemetry::OpenTelemetrySpanExt;

use crate::operators::SinkStream;
use crate::runtime::{
    common::{Consumer, MessageContext, Payload, RuntimeStream},
    environment::{
        RuntimeResult,
        metrics::{Float64Histogram, Int64Counter, Int64Gauge, Labels},
    },
    stream::Stream,
};

pub type HandlerError = Box<dyn Error + Send + Sync>;
pub type HandlerResult = Result<(), HandlerError>;

#[async_trait]
pub trait EndpointHandler<HandlerState, T, R>: Send + Sync
where
    HandlerState: Send + 'static,
    T: Send + Sync + 'static,
    R: Send + Sync + 'static,
{
    fn get_stream_id(&self, context: &MessageContext, value: &T) -> String;

    async fn begin_request(
        &self,
        context: MessageContext,
        stream: &dyn RuntimeStream,
    ) -> (MessageContext, HandlerState);

    async fn consume_message(
        &self,
        context: MessageContext,
        stream: &dyn RuntimeStream,
        handler_state: &mut HandlerState,
        value: Payload<T>,
        result_stream: &Stream<R>,
    ) -> HandlerResult;

    async fn end_request(
        &self,
        context: MessageContext,
        stream: &dyn RuntimeStream,
        result: &HandlerResult,
        handler_state: HandlerState,
    );
}

#[async_trait]
pub trait SinkCallback<T>: Send + Sync
where
    T: Send + Sync + 'static,
{
    async fn done(&self, context: MessageContext, value: Payload<T>, result: &HandlerResult);
}

pub struct CustomEndpointConsumer<HandlerState, T, R, H>
where
    HandlerState: Send + 'static,
    T: Send + Sync + 'static,
    R: Send + Sync + 'static,
    H: EndpointHandler<HandlerState, T, R> + 'static,
{
    stream: Weak<SinkStream<T, R>>,
    result_stream: Stream<R>,
    handler: H,
    sink_callback: RwLock<Option<Arc<dyn SinkCallback<T>>>>,
    messages_total: Int64Counter,
    request_errors: Int64Counter,
    active_requests: Int64Gauge,
    request_duration: Float64Histogram,
    _state: std::marker::PhantomData<fn(HandlerState, T)>,
}

impl<HandlerState, T, R, H> CustomEndpointConsumer<HandlerState, T, R, H>
where
    HandlerState: Send + 'static,
    T: Send + Sync + 'static,
    R: Send + Sync + 'static,
    H: EndpointHandler<HandlerState, T, R> + 'static,
{
    pub fn set_sink_callback(&self, callback: Arc<dyn SinkCallback<T>>) {
        *self
            .sink_callback
            .write()
            .expect("sink callback lock poisoned") = Some(callback);
    }
}

pub fn make_custom_endpoint_consumer<HandlerState, T, R, H>(
    stream: &Arc<SinkStream<T, R>>,
    handler: H,
) -> RuntimeResult<Arc<CustomEndpointConsumer<HandlerState, T, R, H>>>
where
    HandlerState: Send + 'static,
    T: Send + Sync + 'static,
    R: Send + Sync + 'static,
    H: EndpointHandler<HandlerState, T, R> + 'static,
{
    let scope = stream.environment().metrics().scope(
        "datasink_endpoint",
        [
            ("connector".to_owned(), "custom".to_owned()),
            ("endpoint".to_owned(), stream.name().to_owned()),
            ("protocol".to_owned(), "local".to_owned()),
        ]
        .into_iter()
        .collect(),
    );
    let consumer = Arc::new(CustomEndpointConsumer {
        stream: Arc::downgrade(stream),
        result_stream: stream.error_stream().clone(),
        handler,
        sink_callback: RwLock::new(None),
        messages_total: scope.counter(
            "messages_total",
            "Total number of successfully processed messages in data sink endpoint",
            Labels::new(),
        )?,
        request_errors: scope.counter(
            "events_total",
            "Total number of events in data sink endpoint",
            [("event".to_owned(), "request_error".to_owned())]
                .into_iter()
                .collect(),
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
        _state: std::marker::PhantomData,
    });
    stream.set_sink_consumer(consumer.clone())?;
    Ok(consumer)
}

#[async_trait]
impl<HandlerState, T, R, H> Consumer<T> for CustomEndpointConsumer<HandlerState, T, R, H>
where
    HandlerState: Send + 'static,
    T: Send + Sync + 'static,
    R: Send + Sync + 'static,
    H: EndpointHandler<HandlerState, T, R> + 'static,
{
    async fn consume(&self, context: MessageContext, value: Payload<T>) {
        let Some(stream) = self.stream.upgrade() else {
            return;
        };
        let span = if context.sampling_enabled() {
            let span = tracing::info_span!(
                "local.output",
                stream = stream.name(),
                endpoint = stream.name(),
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
        let stream_id = span.in_scope(|| self.handler.get_stream_id(&context, &value));
        let context = context.with_stream_id(stream_id);
        let (handler_context, mut handler_state) = crate::runtime::common::instrument_if_enabled(
            self.handler.begin_request(context, stream.as_ref()),
            span.clone(),
        )
        .await;
        span.in_scope(|| tracing::event!(name: "begin_request", tracing::Level::INFO, {}));
        self.active_requests.inc();
        let started_at = self.request_duration.is_enabled().then(Instant::now);
        let (consume_value, value) = value.share();
        let result = crate::runtime::common::instrument_if_enabled(
            self.handler.consume_message(
                handler_context.clone(),
                stream.as_ref(),
                &mut handler_state,
                consume_value,
                &self.result_stream,
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
                "custom sink handler failed"
            ),
        });
        crate::runtime::common::instrument_if_enabled(
            self.handler.end_request(
                handler_context.clone(),
                stream.as_ref(),
                &result,
                handler_state,
            ),
            span.clone(),
        )
        .await;
        let callback = self
            .sink_callback
            .read()
            .expect("sink callback lock poisoned")
            .clone();
        if let Some(callback) = callback {
            crate::runtime::common::instrument_if_enabled(
                callback.done(handler_context, value, &result),
                span.clone(),
            )
            .await;
        }
        self.active_requests.dec();
        if let Some(started_at) = started_at {
            self.request_duration
                .observe(started_at.elapsed().as_secs_f64());
        }
        if result.is_ok() {
            self.messages_total.inc();
        } else {
            self.request_errors.inc();
            span.in_scope(|| {
                tracing::error!("custom sink request failed");
            });
        }
    }
}
