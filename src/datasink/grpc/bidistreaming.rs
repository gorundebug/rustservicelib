use std::{
    sync::{Arc, Weak},
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
    span: Option<tracing::Span>,
}

#[async_trait]
impl<ReqT, ResR> Sender<ReqT> for StreamingSender<ReqT, ResR>
where
    ReqT: Send + 'static,
    ResR: Send + 'static,
{
    async fn send(&self, _context: MessageContext, request: ReqT) -> HandlerResult {
        let result = crate::runtime::common::instrument_if_present!(
            self.call.send(request),
            self.span.clone(),
        );
        if let Err(error) = &result {
            crate::runtime::telemetry::record_error_if_present!(self.span.as_ref(), error);
        }
        crate::runtime::common::event_if_present!(self.span.as_ref(), || match &result {
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
    finished: tokio_util::sync::CancellationToken,
    started_at: Option<Instant>,
    grpc_started_at: Option<Instant>,
    span: Option<tracing::Span>,
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
    // observes the same creation outcome, including failure. Failure must not
    // restart initialization in an old cell already removed from the map.
    pending: RotatingMap<String, Arc<OnceCell<Option<Arc<Pending<HandlerState, ReqT, ResR>>>>>>,
    metrics: Arc<EndpointMetrics>,
}

pub fn make_grpc_bidi_streaming_endpoint_consumer<HandlerState, ReqT, ResR, T, R, E, H>(
    stream: &Arc<SinkStreamWithResult<T, R, E>>,
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
        cell: Arc<OnceCell<Option<Arc<Pending<HandlerState, ReqT, ResR>>>>>,
        pending: Arc<Pending<HandlerState, ReqT, ResR>>,
    ) {
        // Peer completion must release the close waiter even when the caller
        // never invokes Done and its parent context remains alive. Keep this
        // internal lifecycle signal separate from user completion/tracing.
        let (receive_finished, receive_completion) = tokio::sync::oneshot::channel::<()>();
        let close_pending = Arc::clone(&pending);
        tokio::spawn(async move {
            tokio::select! {
                _ = close_pending.result_context.cancelled() => {}
                _ = close_pending.context.cancelled() => {}
                _ = receive_completion => {}
            }
            let result = crate::runtime::common::instrument_if_present!(
                close_pending.sender.call.close_send(),
                close_pending.span.clone(),
            );
            if let Err(error) = result {
                crate::runtime::telemetry::record_error_if_present!(
                    close_pending.span.as_ref(),
                    &error
                );
                crate::runtime::common::event_if_present!(
                    close_pending.span.as_ref(),
                    || tracing::event!(name: "close_send.error", tracing::Level::ERROR, error = %error)
                );
            }
        });

        let pending_map = self.pending.clone();
        let handler = Arc::clone(&self.handler);
        let stream_context = self.stream_context.clone();
        let metrics = Arc::clone(&self.metrics);
        tokio::spawn(async move {
            let mut result = Ok(());
            loop {
                match crate::runtime::common::instrument_if_present!(
                    pending.sender.call.recv(),
                    pending.span.clone(),
                ) {
                    Ok(Some(response)) => {
                        crate::runtime::common::event_if_present!(
                            pending.span.as_ref(),
                            || tracing::event!(name: "recv", tracing::Level::INFO, {})
                        );
                        result = crate::runtime::common::instrument_if_present!(
                            handler.handle_response(
                                pending.context.clone(),
                                stream_context.clone(),
                                Arc::clone(&pending.state),
                                response,
                            ),
                            pending.span.clone(),
                        );
                        if result.is_err() {
                            if let Err(error) = &result {
                                crate::runtime::telemetry::record_error_if_present!(
                                    pending.span.as_ref(),
                                    error
                                );
                            }
                            break;
                        }
                    }
                    Ok(None) => {
                        crate::runtime::common::event_if_present!(
                            pending.span.as_ref(),
                            || tracing::event!(name: "eof", tracing::Level::INFO, {})
                        );
                        break;
                    }
                    Err(error) => {
                        crate::runtime::telemetry::record_error_if_present!(
                            pending.span.as_ref(),
                            &error
                        );
                        crate::runtime::common::event_if_present!(pending.span.as_ref(), || {
                            tracing::event!(
                                name: "recv.error",
                                tracing::Level::ERROR,
                                error = %error
                            )
                        });
                        result = Err(error);
                        break;
                    }
                }
            }
            // Receive callbacks stay sequential and may overlap sends. Only
            // finalization closes admission and waits for active send handlers.
            pending.finished.cancel();
            let lifetime = pending.lifetime.write().await;
            drop(lifetime);
            crate::runtime::common::instrument_if_present!(
                handler.end_request(
                    pending.context.clone(),
                    stream_context,
                    &result,
                    Arc::clone(&pending.state),
                ),
                pending.span.clone(),
            );
            metrics.request_end(pending.started_at, &result);
            metrics.grpc_client_end(pending.grpc_started_at, &result);
            pending_map.pop_if(&stream_id, |current| Arc::ptr_eq(current, &cell));
            let _ = receive_finished.send(());
        });
    }
}

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
            .get_or_init(move || async move {
                let (context, span) = if stream.stream().environment().tracing_enabled()
                    && context.sampling_enabled()
                {
                    let (context, span) =
                        start_output_span(context, stream.as_ref(), self.metrics.rpc_method());
                    (context, (!span.is_disabled()).then_some(span))
                } else {
                    (context, None)
                };
                let (handler_context, state) = match crate::runtime::common::instrument_if_present!(
                    self.handler
                        .begin_request(context, self.stream_context.clone()),
                    span.clone(),
                ) {
                    Ok(begin) => begin,
                    Err(error) => {
                        self.metrics.begin_request_failed();
                        crate::runtime::telemetry::record_error_if_present!(span.as_ref(), &error);
                        crate::runtime::common::event_if_present!(span.as_ref(), || {
                            tracing::event!(
                                name: "begin_request.error",
                                tracing::Level::ERROR,
                                error = %error,
                                "begin_request failed"
                            )
                        });
                        return None;
                    }
                };
                let request_context = handler_context.clone().with_stream_id(new_stream_id());
                crate::runtime::common::event_if_present!(
                    span.as_ref(),
                    || tracing::event!(name: "begin_request", tracing::Level::INFO, {})
                );
                let state = Arc::new(Mutex::new(state));
                let started_at = self.metrics.request_start();
                let grpc_started_at = self.metrics.grpc_client_measurement_start();
                let call = match crate::runtime::common::instrument_if_present!(
                    (self.client_function)(request_context),
                    span.clone(),
                ) {
                    Ok(call) => call,
                    Err(error) => {
                        crate::runtime::telemetry::record_error_if_present!(span.as_ref(), &error);
                        crate::runtime::common::event_if_present!(span.as_ref(), || {
                            tracing::event!(
                                name: "grpc_call.error",
                                tracing::Level::ERROR,
                                error = %error,
                                "gRPC bidi stream creation failed"
                            )
                        });
                        let result = Err(error);
                        crate::runtime::common::instrument_if_present!(
                            self.handler.end_request(
                                handler_context,
                                self.stream_context.clone(),
                                &result,
                                state,
                            ),
                            span.clone(),
                        );
                        self.metrics.request_end(started_at, &result);
                        self.metrics.grpc_client_end(grpc_started_at, &result);
                        return None;
                    }
                };
                crate::runtime::common::event_if_present!(
                    span.as_ref(),
                    || tracing::event!(name: "grpc_call", tracing::Level::INFO, {})
                );
                let pending = Arc::new(Pending {
                    context: handler_context,
                    state,
                    sender: StreamingSender {
                        call,
                        span: span.clone(),
                    },
                    result_context: ResultContext::with_optional_span(span.as_ref()),
                    lifetime: RwLock::new(()),
                    finished: tokio_util::sync::CancellationToken::new(),
                    started_at,
                    grpc_started_at,
                    span: span.clone(),
                });
                self.spawn_stream_tasks(
                    init_stream_id.clone(),
                    Arc::clone(&init_cell),
                    Arc::clone(&pending),
                );
                Some(pending)
            })
            .await
        {
            Some(pending) => Arc::clone(pending),
            None => {
                // Creation failed; drop the reservation so a future Consume
                // for the same streamID can retry from scratch.
                self.pending
                    .pop_if(&stream_id, |current| Arc::ptr_eq(current, &cell));
                return;
            }
        };

        let _lifetime = tokio::select! {
            biased;
            _ = pending.finished.cancelled() => {
                self.metrics.reject_closing_request(pending.span.as_ref());
                return;
            }
            lifetime = pending.lifetime.read() => lifetime,
        };
        if pending.finished.is_cancelled() {
            self.metrics.reject_closing_request(pending.span.as_ref());
            return;
        }
        let result = crate::runtime::common::instrument_if_present!(
            self.handler.consume_message(
                pending.context.clone(),
                self.stream_context.clone(),
                Arc::clone(&pending.state),
                value,
                &pending.sender,
                pending.result_context.clone(),
            ),
            pending.span.clone(),
        );
        if let Err(error) = &result {
            crate::runtime::telemetry::record_error_if_present!(pending.span.as_ref(), error);
        }
        crate::runtime::common::event_if_present!(pending.span.as_ref(), || match &result {
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
