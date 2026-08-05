use std::{
    sync::{
        Arc, Weak,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant},
};

use async_trait::async_trait;
use tokio::sync::{Mutex, OnceCell, RwLock};
use tracing::Instrument;

use super::{
    BoxFuture, EndpointHandler, EndpointMetrics, HandlerResult, ResultContext, Sender,
    StreamContext, start_output_span,
};
use crate::{
    operators::SinkStreamWithResult,
    runtime::{
        common::{Consumer, MessageContext, Payload, new_stream_id},
        environment::RuntimeResult,
        store::RotatingMap,
    },
};

const PENDING_ROTATION_INTERVAL: Duration = Duration::from_secs(30);

#[async_trait]
pub trait BidiStreamingCall<ReqT, ResR>: Send + Sync
where
    ReqT: Send + 'static,
    ResR: Send + 'static,
{
    async fn send(&self, request: ReqT) -> HandlerResult;
    async fn recv(&self) -> HandlerResult<Option<ResR>>;
    async fn close_send(&self) -> HandlerResult;
}

pub type BidiStreamingClientFunction<ReqT, ResR> = Arc<
    dyn Fn(MessageContext) -> BoxFuture<HandlerResult<Arc<dyn BidiStreamingCall<ReqT, ResR>>>>
        + Send
        + Sync,
>;

struct StreamingSender<ReqT, ResR>
where
    ReqT: Send + 'static,
    ResR: Send + 'static,
{
    call: Arc<dyn BidiStreamingCall<ReqT, ResR>>,
    span: tracing::Span,
}

#[async_trait]
impl<ReqT, ResR> Sender<ReqT> for StreamingSender<ReqT, ResR>
where
    ReqT: Send + 'static,
    ResR: Send + 'static,
{
    async fn send(&self, _context: MessageContext, request: ReqT) -> HandlerResult {
        let result = self.call.send(request).instrument(self.span.clone()).await;
        if let Err(error) = &result {
            crate::runtime::telemetry::record_span_error(&self.span, error);
        }
        self.span.in_scope(|| match &result {
            Ok(()) => tracing::info!(event.name = "send"),
            Err(error) => tracing::error!(event.name = "send.error", error = %error),
        });
        result
    }
}

struct Pending<HandlerState, ReqT, ResR>
where
    HandlerState: Send + 'static,
    ReqT: Send + 'static,
    ResR: Send + 'static,
{
    context: MessageContext,
    state: Arc<Mutex<HandlerState>>,
    sender: StreamingSender<ReqT, ResR>,
    result_context: ResultContext,
    lifetime: RwLock<()>,
    finished: AtomicBool,
    started_at: Instant,
    grpc_started_at: Instant,
    span: tracing::Span,
}

pub struct BidiStreamingEndpointConsumer<HandlerState, ReqT, ResR, T, R, E, H>
where
    HandlerState: Send + 'static,
    ReqT: Send + 'static,
    ResR: Send + 'static,
    T: Send + Sync + 'static,
    R: Send + Sync + 'static,
    E: Send + Sync + 'static,
    H: EndpointHandler<HandlerState, ReqT, ResR, T, R, E> + 'static,
{
    stream: Weak<SinkStreamWithResult<T, R, E>>,
    stream_context: StreamContext<T, R, E>,
    handler: Arc<H>,
    client_function: BidiStreamingClientFunction<ReqT, ResR>,
    // Each streamID maps to a OnceCell reserved immediately (before any
    // network I/O) so concurrent Consume calls for *different* streamIDs
    // never contend on a single shared lock; a Consume for the *same*
    // still-being-created streamID awaits the cell instead, and only ever
    // observes a fully, atomically constructed Pending.
    pending: RotatingMap<String, Arc<OnceCell<Arc<Pending<HandlerState, ReqT, ResR>>>>>,
    metrics: Arc<EndpointMetrics>,
}

pub fn make_grpc_bidi_streaming_endpoint_consumer<HandlerState, ReqT, ResR, T, R, E, H>(
    stream: &Arc<SinkStreamWithResult<T, R, E>>,
    connector_name: &str,
    endpoint_name: &str,
    handler: H,
    client_function: BidiStreamingClientFunction<ReqT, ResR>,
) -> RuntimeResult<Arc<BidiStreamingEndpointConsumer<HandlerState, ReqT, ResR, T, R, E, H>>>
where
    HandlerState: Send + 'static,
    ReqT: Send + 'static,
    ResR: Send + 'static,
    T: Send + Sync + 'static,
    R: Send + Sync + 'static,
    E: Send + Sync + 'static,
    H: EndpointHandler<HandlerState, ReqT, ResR, T, R, E> + 'static,
{
    let pending = RotatingMap::new(PENDING_ROTATION_INTERVAL);
    let consumer = Arc::new(BidiStreamingEndpointConsumer {
        stream: Arc::downgrade(stream),
        stream_context: StreamContext::new(Arc::downgrade(stream)),
        handler: Arc::new(handler),
        client_function,
        pending: pending.clone(),
        metrics: Arc::new(EndpointMetrics::new(stream, connector_name, endpoint_name)?),
    });
    stream
        .stream()
        .environment()
        .register_storage(Arc::new(pending));
    stream.set_sink_consumer(consumer.clone())?;
    Ok(consumer)
}

impl<HandlerState, ReqT, ResR, T, R, E, H>
    BidiStreamingEndpointConsumer<HandlerState, ReqT, ResR, T, R, E, H>
where
    HandlerState: Send + 'static,
    ReqT: Send + 'static,
    ResR: Send + 'static,
    T: Send + Sync + 'static,
    R: Send + Sync + 'static,
    E: Send + Sync + 'static,
    H: EndpointHandler<HandlerState, ReqT, ResR, T, R, E> + 'static,
{
    fn spawn_stream_tasks(
        &self,
        stream_id: String,
        cell: Arc<OnceCell<Arc<Pending<HandlerState, ReqT, ResR>>>>,
        pending: Arc<Pending<HandlerState, ReqT, ResR>>,
    ) {
        let close_pending = Arc::clone(&pending);
        tokio::spawn(async move {
            tokio::select! {
                _ = close_pending.result_context.cancelled() => {}
                _ = close_pending.context.cancelled() => {}
            }
            let result = close_pending
                .sender
                .call
                .close_send()
                .instrument(close_pending.span.clone())
                .await;
            if let Err(error) = result {
                crate::runtime::telemetry::record_span_error(&close_pending.span, &error);
                close_pending
                    .span
                    .in_scope(|| tracing::error!(event.name = "close_send.error", error = %error));
            }
        });

        let pending_map = self.pending.clone();
        let handler = Arc::clone(&self.handler);
        let stream_context = self.stream_context.clone();
        let metrics = Arc::clone(&self.metrics);
        tokio::spawn(async move {
            let mut result = Ok(());
            loop {
                match pending
                    .sender
                    .call
                    .recv()
                    .instrument(pending.span.clone())
                    .await
                {
                    Ok(Some(response)) => {
                        pending
                            .span
                            .in_scope(|| tracing::info!(event.name = "recv"));
                        result = handler
                            .handle_response(
                                pending.context.clone(),
                                stream_context.clone(),
                                Arc::clone(&pending.state),
                                response,
                            )
                            .instrument(pending.span.clone())
                            .await;
                        if result.is_err() {
                            if let Err(error) = &result {
                                crate::runtime::telemetry::record_span_error(&pending.span, error);
                            }
                            break;
                        }
                    }
                    Ok(None) => {
                        pending.span.in_scope(|| tracing::info!(event.name = "eof"));
                        break;
                    }
                    Err(error) => {
                        crate::runtime::telemetry::record_span_error(&pending.span, &error);
                        pending.span.in_scope(|| {
                            tracing::error!(
                                event.name = "recv.error",
                                error = %error
                            )
                        });
                        result = Err(error);
                        break;
                    }
                }
            }
            let _lifetime = pending.lifetime.write().await;
            if pending.finished.swap(true, Ordering::AcqRel) {
                return;
            }
            pending_map.pop_if(&stream_id, |current| Arc::ptr_eq(current, &cell));
            handler
                .end_request(
                    pending.context.clone(),
                    stream_context,
                    &result,
                    Arc::clone(&pending.state),
                )
                .instrument(pending.span.clone())
                .await;
            metrics.request_end(pending.started_at, &result);
            metrics.grpc_client_end(pending.grpc_started_at, &result);
        });
    }
}

#[async_trait]
impl<HandlerState, ReqT, ResR, T, R, E, H> Consumer<T>
    for BidiStreamingEndpointConsumer<HandlerState, ReqT, ResR, T, R, E, H>
where
    HandlerState: Send + 'static,
    ReqT: Send + 'static,
    ResR: Send + 'static,
    T: Send + Sync + 'static,
    R: Send + Sync + 'static,
    E: Send + Sync + 'static,
    H: EndpointHandler<HandlerState, ReqT, ResR, T, R, E> + 'static,
{
    async fn consume(&self, context: MessageContext, value: Payload<T>) {
        let Some(stream) = self.stream.upgrade() else {
            return;
        };
        let (context, stream_id) = match context.stream_id() {
            Some(stream_id) => (context.clone(), stream_id.to_owned()),
            None => {
                let stream_id = new_stream_id();
                (context.with_stream_id(stream_id.clone()), stream_id)
            }
        };
        let (cell, _) = self
            .pending
            .get_or_create(stream_id.clone(), || Arc::new(OnceCell::new()));

        let init_cell = Arc::clone(&cell);
        let init_stream_id = stream_id.clone();
        let pending = match cell
            .get_or_try_init(move || async move {
                let (context, span) =
                    start_output_span(context, stream.as_ref(), self.metrics.rpc_method());
                let (handler_context, state) = match self
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
                            )
                        });
                        return Err(error);
                    }
                };
                span.in_scope(|| tracing::info!(event.name = "begin_request"));
                let state = Arc::new(Mutex::new(state));
                let started_at = self.metrics.request_start();
                let grpc_started_at = Instant::now();
                let call = match (self.client_function)(handler_context.clone())
                    .instrument(span.clone())
                    .await
                {
                    Ok(call) => call,
                    Err(error) => {
                        crate::runtime::telemetry::record_span_error(&span, &error);
                        span.in_scope(|| {
                            tracing::error!(
                                event.name = "grpc_call.error",
                                error = %error,
                                "gRPC bidi stream creation failed"
                            )
                        });
                        let result = Err(error);
                        self.handler
                            .end_request(
                                handler_context,
                                self.stream_context.clone(),
                                &result,
                                state,
                            )
                            .instrument(span.clone())
                            .await;
                        self.metrics.request_end(started_at, &result);
                        self.metrics.grpc_client_end(grpc_started_at, &result);
                        return result.map(|()| unreachable!("gRPC creation error became success"));
                    }
                };
                span.in_scope(|| tracing::info!(event.name = "grpc_call"));
                let pending = Arc::new(Pending {
                    context: handler_context,
                    state,
                    sender: StreamingSender {
                        call,
                        span: span.clone(),
                    },
                    result_context: ResultContext::with_span(span.clone()),
                    lifetime: RwLock::new(()),
                    finished: AtomicBool::new(false),
                    started_at,
                    grpc_started_at,
                    span: span.clone(),
                });
                self.spawn_stream_tasks(
                    init_stream_id.clone(),
                    Arc::clone(&init_cell),
                    Arc::clone(&pending),
                );
                Ok(pending)
            })
            .await
        {
            Ok(pending) => Arc::clone(pending),
            Err(_) => {
                // Creation failed; drop the reservation so a future Consume
                // for the same streamID can retry from scratch.
                self.pending
                    .pop_if(&stream_id, |current| Arc::ptr_eq(current, &cell));
                return;
            }
        };

        let _lifetime = pending.lifetime.read().await;
        if pending.finished.load(Ordering::Acquire) {
            return;
        }
        let result = self
            .handler
            .consume_message(
                pending.context.clone(),
                self.stream_context.clone(),
                Arc::clone(&pending.state),
                value,
                &pending.sender,
                pending.result_context.clone(),
            )
            .instrument(pending.span.clone())
            .await;
        if let Err(error) = &result {
            crate::runtime::telemetry::record_span_error(&pending.span, error);
        }
        pending.span.in_scope(|| match &result {
            Ok(()) => tracing::info!(event.name = "consume_message"),
            Err(error) => tracing::error!(
                event.name = "consume_message.error",
                error = %error
            ),
        });
        if result.is_err() {
            pending.result_context.done();
        }
    }
}
