use std::{
    collections::BTreeMap,
    error::Error,
    future::Future,
    pin::Pin,
    sync::{Arc, Mutex},
    time::Instant,
};

use async_trait::async_trait;
use tracing_opentelemetry::OpenTelemetrySpanExt;

use crate::{
    operators::SinkStreamWithResult,
    runtime::{
        common::{MessageContext, Payload, RuntimeStream},
        datasink::SinkStreamContext,
        environment::{
            RuntimeResult,
            metrics::{Float64Histogram, Int64Counter, Int64Gauge, Labels},
        },
        telemetry::{GrpcClientMetrics, GrpcClientObservation, grpc_error_status},
    },
};

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

pub type StreamContext<T, R, E> = SinkStreamContext<T, R, E>;

pub(crate) fn start_output_span(
    context: MessageContext,
    stream: &dyn RuntimeStream,
    endpoint: &str,
) -> (MessageContext, tracing::Span) {
    if !stream.environment().tracing_enabled() || !context.sampling_enabled() {
        return (context, tracing::Span::none());
    }
    let (stream_name, pipeline_name, component_name) = stream.tracing_labels();
    let span = tracing::info_span!(
        "grpc.output",
        stream = stream_name,
        pipeline = pipeline_name,
        component = component_name,
        endpoint = endpoint,
        error = tracing::field::Empty,
        otel.status_code = tracing::field::Empty,
        otel.status_message = tracing::field::Empty,
    );
    if span.is_disabled() {
        return (context, span);
    }
    let _ = span.set_parent(context.open_telemetry_context().clone());
    let child = span.context();
    (context.with_open_telemetry_context(child), span)
}

#[async_trait]
pub trait Sender<ReqT>: Send + Sync
where
    ReqT: Send + 'static,
{
    async fn send(&self, context: MessageContext, request: ReqT) -> HandlerResult;
}

#[derive(Clone, Default)]
pub struct ResultContext {
    done: tokio_util::sync::CancellationToken,
    span: Option<tracing::Span>,
}

impl ResultContext {
    pub(crate) fn with_span(span: tracing::Span) -> Self {
        Self {
            done: tokio_util::sync::CancellationToken::new(),
            span: (!span.is_disabled()).then_some(span),
        }
    }

    pub fn done(&self) {
        if let Some(span) = &self.span {
            crate::runtime::common::event_if_enabled!(
                &span,
                || tracing::event!(name: "done_called", tracing::Level::INFO, {})
            );
        }
        self.done.cancel();
    }

    pub fn is_done(&self) -> bool {
        self.done.is_cancelled()
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
        handler_state: Arc<tokio::sync::Mutex<HandlerState>>,
        value: Payload<T>,
        sender: &dyn Sender<ReqT>,
        result_context: ResultContext,
    ) -> HandlerResult;

    async fn handle_response(
        &self,
        context: MessageContext,
        stream: StreamContext<T, R, E>,
        handler_state: Arc<tokio::sync::Mutex<HandlerState>>,
        response: ResR,
    ) -> HandlerResult;

    async fn end_request(
        &self,
        context: MessageContext,
        stream: StreamContext<T, R, E>,
        result: &HandlerResult,
        handler_state: Arc<tokio::sync::Mutex<HandlerState>>,
    );
}

// Multiple endpoint consumers may share a single business handler. Request
// state and transport ownership remain local to each consumer.
#[async_trait]
impl<HandlerState, ReqT, ResR, T, R, E, H> EndpointHandler<HandlerState, ReqT, ResR, T, R, E>
    for Arc<H>
where
    HandlerState: Send + 'static,
    ReqT: Send + 'static,
    ResR: Send + 'static,
    T: Send + Sync + 'static,
    R: Send + Sync + 'static,
    E: Send + Sync + 'static,
    H: EndpointHandler<HandlerState, ReqT, ResR, T, R, E> + ?Sized,
{
    async fn begin_request(
        &self,
        context: MessageContext,
        stream: StreamContext<T, R, E>,
    ) -> HandlerResult<(MessageContext, HandlerState)> {
        self.as_ref().begin_request(context, stream).await
    }

    async fn consume_message(
        &self,
        context: MessageContext,
        stream: StreamContext<T, R, E>,
        handler_state: Arc<tokio::sync::Mutex<HandlerState>>,
        value: Payload<T>,
        sender: &dyn Sender<ReqT>,
        result_context: ResultContext,
    ) -> HandlerResult {
        self.as_ref()
            .consume_message(
                context,
                stream,
                handler_state,
                value,
                sender,
                result_context,
            )
            .await
    }

    async fn handle_response(
        &self,
        context: MessageContext,
        stream: StreamContext<T, R, E>,
        handler_state: Arc<tokio::sync::Mutex<HandlerState>>,
        response: ResR,
    ) -> HandlerResult {
        self.as_ref()
            .handle_response(context, stream, handler_state, response)
            .await
    }

    async fn end_request(
        &self,
        context: MessageContext,
        stream: StreamContext<T, R, E>,
        result: &HandlerResult,
        handler_state: Arc<tokio::sync::Mutex<HandlerState>>,
    ) {
        self.as_ref()
            .end_request(context, stream, result, handler_state)
            .await;
    }
}

pub(crate) struct RequestSender<ReqT> {
    request: Mutex<Option<ReqT>>,
}

impl<ReqT> Default for RequestSender<ReqT> {
    fn default() -> Self {
        Self {
            request: Mutex::new(None),
        }
    }
}

impl<ReqT> RequestSender<ReqT> {
    pub(crate) fn take(&self) -> HandlerResult<ReqT> {
        self.request
            .lock()
            .expect("gRPC request sender lock poisoned")
            .take()
            .ok_or_else(|| "gRPC sink handler did not send a request".into())
    }
}

#[async_trait]
impl<ReqT> Sender<ReqT> for RequestSender<ReqT>
where
    ReqT: Send + 'static,
{
    async fn send(&self, _context: MessageContext, request: ReqT) -> HandlerResult {
        *self
            .request
            .lock()
            .expect("gRPC request sender lock poisoned") = Some(request);
        Ok(())
    }
}

pub(crate) struct EndpointMetrics {
    enabled: bool,
    pub(crate) messages_total: Int64Counter,
    pub(crate) request_errors: Int64Counter,
    pub(crate) begin_request_failed: Int64Counter,
    pub(crate) active_requests: Int64Gauge,
    pub(crate) request_duration: Float64Histogram,
    grpc_client_metrics: GrpcClientMetrics,
    rpc_method: String,
}

impl EndpointMetrics {
    pub(crate) fn new<T, R, E>(stream: &Arc<SinkStreamWithResult<T, R, E>>) -> RuntimeResult<Self>
    where
        T: Send + Sync + 'static,
        R: Send + Sync + 'static,
        E: Send + Sync + 'static,
    {
        let runtime = stream.stream().environment().runtime_config();
        let endpoint = runtime
            .endpoint_by_id(stream.endpoint_id())
            .ok_or_else(|| {
                crate::runtime::environment::RuntimeError::InvalidConfiguration(format!(
                    "endpoint {} referenced by sink stream {:?} is not configured",
                    stream.endpoint_id(),
                    stream.name()
                ))
            })?;
        let connector = runtime
            .data_connector_by_id(endpoint.data_connector_id())
            .ok_or_else(|| {
                crate::runtime::environment::RuntimeError::InvalidConfiguration(format!(
                    "data connector {} referenced by endpoint {:?} is not configured",
                    endpoint.data_connector_id(),
                    endpoint.name()
                ))
            })?;
        let endpoint_name = endpoint.name().to_owned();
        let labels = [
            ("connector".to_owned(), connector.name().to_owned()),
            ("endpoint".to_owned(), endpoint_name.clone()),
            ("protocol".to_owned(), "grpc".to_owned()),
        ]
        .into_iter()
        .collect::<BTreeMap<_, _>>();
        let scope = stream
            .stream()
            .environment()
            .metrics()
            .scope("datasink_endpoint", labels);
        let enabled = !stream.stream().environment().metrics().is_noop();
        Ok(Self {
            enabled,
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
            begin_request_failed: scope.counter(
                "events_total",
                "Total number of events in data sink endpoint",
                [("event".to_owned(), "begin_request_failed".to_owned())]
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
            grpc_client_metrics: GrpcClientMetrics::new(
                stream.stream().environment().metrics().clone(),
                &endpoint_name,
            ),
            rpc_method: endpoint_name,
        })
    }

    pub(crate) fn request_start(&self) -> Option<Instant> {
        self.active_requests.inc();
        self.enabled.then(Instant::now)
    }

    pub(crate) fn request_end(&self, started_at: Option<Instant>, result: &HandlerResult) {
        self.active_requests.dec();
        if let Some(started_at) = started_at {
            self.request_duration
                .observe(started_at.elapsed().as_secs_f64());
        }
        if result.is_ok() {
            self.messages_total.inc();
        } else {
            self.request_errors.inc();
        }
    }

    pub(crate) fn grpc_client_start(&self) -> GrpcClientObservation {
        self.grpc_client_metrics.start()
    }

    pub(crate) fn grpc_client_measurement_start(&self) -> Option<Instant> {
        self.enabled.then(Instant::now)
    }

    pub(crate) fn grpc_client_end(&self, started_at: Option<Instant>, result: &HandlerResult) {
        let Some(started_at) = started_at else {
            return;
        };
        let status = match result {
            Ok(()) => "OK",
            Err(error) => grpc_error_status(error.as_ref()),
        };
        self.grpc_client_metrics
            .observe(status, started_at.elapsed().as_secs_f64());
    }

    pub(crate) fn rpc_method(&self) -> &str {
        &self.rpc_method
    }
}

#[cfg(test)]
mod result_tracing_tests {
    use super::ResultContext;
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };
    use tracing::{Event, Subscriber};
    use tracing_subscriber::{Layer, layer::Context, prelude::*};

    struct CountEvents(Arc<AtomicUsize>);

    impl<S: Subscriber> Layer<S> for CountEvents {
        fn on_event(&self, _event: &Event<'_>, _context: Context<'_, S>) {
            self.0.fetch_add(1, Ordering::Relaxed);
        }
    }

    #[test]
    fn disabled_result_span_does_not_emit_into_an_unrelated_parent() {
        let events = Arc::new(AtomicUsize::new(0));
        let subscriber = tracing_subscriber::registry().with(CountEvents(events.clone()));
        tracing::subscriber::with_default(subscriber, || {
            let parent = tracing::info_span!("unrelated.parent");
            let _entered = parent.enter();
            let result = ResultContext::with_span(tracing::Span::none());
            assert!(result.span.is_none());
            result.done();
            assert!(result.is_done());
        });
        assert_eq!(events.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn enabled_result_span_keeps_completion_and_event() {
        let events = Arc::new(AtomicUsize::new(0));
        let subscriber = tracing_subscriber::registry().with(CountEvents(events.clone()));
        tracing::subscriber::with_default(subscriber, || {
            let span = tracing::info_span!("grpc.output.test");
            assert!(!span.is_disabled());
            let result = ResultContext::with_span(span);
            assert!(result.span.is_some());
            result.done();
            assert!(result.is_done());
        });
        assert_eq!(events.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn output_callers_guard_before_helper_arguments() {
        for source in [
            include_str!("nostreaming.rs"),
            include_str!("serverstreaming.rs"),
            include_str!("clientstreaming.rs"),
            include_str!("bidistreaming.rs"),
        ] {
            let compact: String = source.chars().filter(|ch| !ch.is_whitespace()).collect();
            assert!(compact.contains(concat!(
                "ifstream.stream().environment().tracing_enabled()&&context.sampling_enabled(){",
                "start_output_span(context,stream.as_ref(),self.metrics.rpc_method())",
                "}else{(context,tracing::Span::none())}"
            )));
        }
    }
}
