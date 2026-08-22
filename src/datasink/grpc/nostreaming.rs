use std::sync::{Arc, Weak};

use async_trait::async_trait;

use super::{
    BoxFuture, EndpointHandler, EndpointMetrics, HandlerResult, RequestSender, ResultContext,
    StreamContext, start_output_span,
};
use crate::{
    operators::SinkStreamWithResult,
    runtime::{
        common::{Consumer, MessageContext, Payload, new_stream_id},
        environment::RuntimeResult,
    },
};

pub type NoStreamingClientFunction<ReqT, ResR> =
    Arc<dyn Fn(MessageContext, ReqT) -> BoxFuture<HandlerResult<ResR>> + Send + Sync>;

pub struct NoStreamingEndpointConsumer<HandlerState, ReqT, ResR, T, R, E, H>
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
    handler: H,
    client_function: NoStreamingClientFunction<ReqT, ResR>,
    metrics: EndpointMetrics,
    _state: std::marker::PhantomData<fn(HandlerState)>,
}

pub fn make_grpc_no_streaming_endpoint_consumer<HandlerState, ReqT, ResR, T, R, E, H>(
    stream: &Arc<SinkStreamWithResult<T, R, E>>,
    handler: H,
    client_function: NoStreamingClientFunction<ReqT, ResR>,
) -> RuntimeResult<Arc<NoStreamingEndpointConsumer<HandlerState, ReqT, ResR, T, R, E, H>>>
where
    HandlerState: Send + 'static,
    ReqT: Send + 'static,
    ResR: Send + 'static,
    T: Send + Sync + 'static,
    R: Send + Sync + 'static,
    E: Send + Sync + 'static,
    H: EndpointHandler<HandlerState, ReqT, ResR, T, R, E> + 'static,
{
    let consumer = Arc::new(NoStreamingEndpointConsumer {
        stream: Arc::downgrade(stream),
        stream_context: StreamContext::new(Arc::downgrade(stream)),
        handler,
        client_function,
        metrics: EndpointMetrics::new(stream)?,
        _state: std::marker::PhantomData,
    });
    stream.set_sink_consumer(consumer.clone())?;
    Ok(consumer)
}

#[async_trait]
impl<HandlerState, ReqT, ResR, T, R, E, H> Consumer<T>
    for NoStreamingEndpointConsumer<HandlerState, ReqT, ResR, T, R, E, H>
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
        let (context, span) =
            start_output_span(context, stream.as_ref(), self.metrics.rpc_method());
        let (context, state) = match crate::runtime::common::instrument_if_enabled(
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
                return;
            }
        };
        let request_context = context.clone().with_stream_id(new_stream_id());
        span.in_scope(|| tracing::event!(name: "begin_request", tracing::Level::INFO, {}));
        let state = Arc::new(tokio::sync::Mutex::new(state));
        let started_at = self.metrics.request_start();
        let sender = RequestSender::default();
        let mut result = crate::runtime::common::instrument_if_enabled(
            self.handler.consume_message(
                context.clone(),
                self.stream_context.clone(),
                Arc::clone(&state),
                value,
                &sender,
                ResultContext::with_span(span.clone()),
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
                "gRPC sink handler failed"
            ),
        });
        if result.is_ok() {
            result = match sender.take() {
                Ok(request) => {
                    let observation = self.metrics.grpc_client_start();
                    match crate::runtime::common::instrument_if_enabled(
                        (self.client_function)(request_context, request),
                        span.clone(),
                    )
                    .await
                    {
                        Ok(response) => {
                            observation.finish("OK");
                            span.in_scope(
                                || tracing::event!(name: "grpc_call", tracing::Level::INFO, {}),
                            );
                            let handled = crate::runtime::common::instrument_if_enabled(
                                self.handler.handle_response(
                                    context.clone(),
                                    self.stream_context.clone(),
                                    Arc::clone(&state),
                                    response,
                                ),
                                span.clone(),
                            )
                            .await;
                            if let Err(error) = &handled {
                                crate::runtime::telemetry::record_span_error(&span, error);
                            }
                            span.in_scope(|| match &handled {
                                Ok(()) => tracing::event!(name: "handle_response", tracing::Level::INFO, {}),
                                Err(error) => tracing::event!(
                                    name: "handle_response.error",
                                    tracing::Level::ERROR,
                                    error = %error,
                                    "gRPC response handler failed"
                                ),
                            });
                            handled
                        }
                        Err(error) => {
                            crate::runtime::telemetry::record_span_error(&span, &error);
                            observation.finish(crate::runtime::telemetry::grpc_error_status(
                                error.as_ref(),
                            ));
                            span.in_scope(|| {
                                tracing::event!(
                                    name: "grpc_call.error",
                                    tracing::Level::ERROR,
                                    error = %error,
                                    "gRPC client call failed"
                                )
                            });
                            Err(error)
                        }
                    }
                }
                Err(error) => Err(error),
            };
        }
        crate::runtime::common::instrument_if_enabled(
            self.handler
                .end_request(context, self.stream_context.clone(), &result, state),
            span.clone(),
        )
        .await;
        self.metrics.request_end(started_at, &result);
    }
}
