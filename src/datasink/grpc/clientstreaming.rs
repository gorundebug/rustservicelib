use std::{
    sync::{
        Arc, Weak,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant},
};

use async_trait::async_trait;
use tokio::sync::{Mutex, OnceCell, RwLock};

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
pub trait ClientStreamingCall<ReqT, ResR>: Send + Sync
where
    ReqT: Send + 'static,
    ResR: Send + 'static,
{
    async fn send(&self, request: ReqT) -> HandlerResult;
    async fn close_and_recv(&self) -> HandlerResult<ResR>;
}

pub type ClientStreamingClientFunction<ReqT, ResR> = Arc<
    dyn Fn(MessageContext) -> BoxFuture<HandlerResult<Arc<dyn ClientStreamingCall<ReqT, ResR>>>>
        + Send
        + Sync,
>;

struct StreamingSender<ReqT, ResR>
where
    ReqT: Send + 'static,
    ResR: Send + 'static,
{
    call: Arc<dyn ClientStreamingCall<ReqT, ResR>>,
    span: tracing::Span,
}

#[async_trait]
impl<ReqT, ResR> Sender<ReqT> for StreamingSender<ReqT, ResR>
where
    ReqT: Send + 'static,
    ResR: Send + 'static,
{
    async fn send(&self, _context: MessageContext, request: ReqT) -> HandlerResult {
        let result = crate::runtime::common::instrument_if_enabled(
            self.call.send(request),
            self.span.clone(),
        )
        .await;
        if let Err(error) = &result {
            crate::runtime::telemetry::record_span_error(&self.span, error);
        }
        self.span.in_scope(|| match &result {
            Ok(()) => tracing::event!(name: "send", tracing::Level::INFO, {}),
            Err(error) => {
                tracing::event!(name: "send.error", tracing::Level::ERROR, error = %error)
            }
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
    started_at: Option<Instant>,
    grpc_started_at: Option<Instant>,
    span: tracing::Span,
}

pub struct ClientStreamingEndpointConsumer<HandlerState, ReqT, ResR, T, R, E, H>
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
    client_function: ClientStreamingClientFunction<ReqT, ResR>,
    // Each streamID maps to a OnceCell reserved immediately (before any
    // network I/O) so concurrent Consume calls for *different* streamIDs
    // never contend on a single shared lock; a Consume for the *same*
    // still-being-created streamID awaits the cell instead, and only ever
    // observes a fully, atomically constructed Pending.
    pending: RotatingMap<String, Arc<OnceCell<Arc<Pending<HandlerState, ReqT, ResR>>>>>,
    metrics: Arc<EndpointMetrics>,
}

pub fn make_grpc_client_streaming_endpoint_consumer<HandlerState, ReqT, ResR, T, R, E, H>(
    stream: &Arc<SinkStreamWithResult<T, R, E>>,
    handler: H,
    client_function: ClientStreamingClientFunction<ReqT, ResR>,
) -> RuntimeResult<Arc<ClientStreamingEndpointConsumer<HandlerState, ReqT, ResR, T, R, E, H>>>
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
    let consumer = Arc::new(ClientStreamingEndpointConsumer {
        stream: Arc::downgrade(stream),
        stream_context: StreamContext::new(Arc::downgrade(stream)),
        handler: Arc::new(handler),
        client_function,
        pending: pending.clone(),
        metrics: Arc::new(EndpointMetrics::new(stream)?),
    });
    stream
        .stream()
        .environment()
        .register_storage(Arc::new(pending));
    stream.set_sink_consumer(consumer.clone())?;
    Ok(consumer)
}

impl<HandlerState, ReqT, ResR, T, R, E, H>
    ClientStreamingEndpointConsumer<HandlerState, ReqT, ResR, T, R, E, H>
where
    HandlerState: Send + 'static,
    ReqT: Send + 'static,
    ResR: Send + 'static,
    T: Send + Sync + 'static,
    R: Send + Sync + 'static,
    E: Send + Sync + 'static,
    H: EndpointHandler<HandlerState, ReqT, ResR, T, R, E> + 'static,
{
    fn spawn_finalizer(
        &self,
        stream_id: String,
        cell: Arc<OnceCell<Arc<Pending<HandlerState, ReqT, ResR>>>>,
        pending: Arc<Pending<HandlerState, ReqT, ResR>>,
    ) {
        let pending_map = self.pending.clone();
        let handler = Arc::clone(&self.handler);
        let stream_context = self.stream_context.clone();
        let metrics = Arc::clone(&self.metrics);
        tokio::spawn(async move {
            let cancelled = tokio::select! {
                _ = pending.result_context.cancelled() => false,
                _ = pending.context.cancelled() => true,
            };
            let _lifetime = pending.lifetime.write().await;
            if pending.finished.swap(true, Ordering::AcqRel) {
                return;
            }
            let removed = pending_map
                .pop_if(&stream_id, |current| Arc::ptr_eq(current, &cell))
                .is_some();
            if !removed {
                return;
            }

            let result = if cancelled {
                crate::runtime::telemetry::record_span_error(
                    &pending.span,
                    "gRPC client stream context cancelled",
                );
                pending.span.in_scope(|| {
                    tracing::event!(
                        name: "context_cancelled",
                        tracing::Level::WARN,
                        error = "gRPC client stream context cancelled"
                    )
                });
                Err("gRPC client stream context cancelled".into())
            } else {
                match crate::runtime::common::instrument_if_enabled(
                    pending.sender.call.close_and_recv(),
                    pending.span.clone(),
                )
                .await
                {
                    Ok(response) => {
                        pending.span.in_scope(
                            || tracing::event!(name: "close_and_recv", tracing::Level::INFO, {}),
                        );
                        let handled = crate::runtime::common::instrument_if_enabled(
                            handler.handle_response(
                                pending.context.clone(),
                                stream_context.clone(),
                                Arc::clone(&pending.state),
                                response,
                            ),
                            pending.span.clone(),
                        )
                        .await;
                        if let Err(error) = &handled {
                            crate::runtime::telemetry::record_span_error(&pending.span, error);
                        }
                        pending.span.in_scope(|| match &handled {
                            Ok(()) => {
                                tracing::event!(name: "handle_response", tracing::Level::INFO, {})
                            }
                            Err(error) => tracing::event!(
                                name: "handle_response.error",
                                tracing::Level::ERROR,
                                error = %error
                            ),
                        });
                        handled
                    }
                    Err(error) => {
                        crate::runtime::telemetry::record_span_error(&pending.span, &error);
                        pending.span.in_scope(|| {
                            tracing::event!(
                                name: "close_and_recv.error",
                                tracing::Level::ERROR,
                                error = %error
                            )
                        });
                        Err(error)
                    }
                }
            };
            crate::runtime::common::instrument_if_enabled(
                handler.end_request(
                    pending.context.clone(),
                    stream_context,
                    &result,
                    Arc::clone(&pending.state),
                ),
                pending.span.clone(),
            )
            .await;
            metrics.request_end(pending.started_at, &result);
            metrics.grpc_client_end(pending.grpc_started_at, &result);
        });
    }
}

#[async_trait]
impl<HandlerState, ReqT, ResR, T, R, E, H> Consumer<T>
    for ClientStreamingEndpointConsumer<HandlerState, ReqT, ResR, T, R, E, H>
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
                let (handler_context, state) = match crate::runtime::common::instrument_if_enabled(
                    self.handler
                        .begin_request(context, self.stream_context.clone()),
                    span.clone(),
                )
                .await
                {
                    Ok(begin) => begin,
                    Err(error) => {
                        self.metrics.begin_request_failed.inc();
                        crate::runtime::telemetry::record_span_error(&span, &error);
                        span.in_scope(|| {
                            tracing::event!(
                                name: "begin_request.error",
                                tracing::Level::ERROR,
                                error = %error,
                                "begin_request failed"
                            )
                        });
                        return Err(error);
                    }
                };
                let request_context = handler_context.clone().with_stream_id(new_stream_id());
                span.in_scope(|| tracing::event!(name: "begin_request", tracing::Level::INFO, {}));
                let state = Arc::new(Mutex::new(state));
                let started_at = self.metrics.request_start();
                let grpc_started_at = self.metrics.grpc_client_measurement_start();
                let call = match crate::runtime::common::instrument_if_enabled(
                    (self.client_function)(request_context),
                    span.clone(),
                )
                .await
                {
                    Ok(call) => call,
                    Err(error) => {
                        crate::runtime::telemetry::record_span_error(&span, &error);
                        span.in_scope(|| {
                            tracing::event!(
                                name: "grpc_call.error",
                                tracing::Level::ERROR,
                                error = %error,
                                "gRPC client stream creation failed"
                            )
                        });
                        let result = Err(error);
                        crate::runtime::common::instrument_if_enabled(
                            self.handler.end_request(
                                handler_context,
                                self.stream_context.clone(),
                                &result,
                                state,
                            ),
                            span.clone(),
                        )
                        .await;
                        self.metrics.request_end(started_at, &result);
                        self.metrics.grpc_client_end(grpc_started_at, &result);
                        return result.map(|()| unreachable!("gRPC creation error became success"));
                    }
                };
                span.in_scope(|| tracing::event!(name: "grpc_call", tracing::Level::INFO, {}));
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
                self.spawn_finalizer(
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
        let result = crate::runtime::common::instrument_if_enabled(
            self.handler.consume_message(
                pending.context.clone(),
                self.stream_context.clone(),
                Arc::clone(&pending.state),
                value,
                &pending.sender,
                pending.result_context.clone(),
            ),
            pending.span.clone(),
        )
        .await;
        if let Err(error) = &result {
            crate::runtime::telemetry::record_span_error(&pending.span, error);
        }
        pending.span.in_scope(|| match &result {
            Ok(()) => tracing::event!(name: "consume_message", tracing::Level::INFO, {}),
            Err(error) => tracing::event!(
                name: "consume_message.error",
                tracing::Level::ERROR,
                error = %error
            ),
        });
        if result.is_err() {
            pending.result_context.done();
        }
    }
}
