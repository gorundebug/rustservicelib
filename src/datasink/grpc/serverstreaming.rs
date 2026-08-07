use std::sync::{Arc, Weak};

use async_trait::async_trait;
use futures::{Stream, StreamExt};
use tracing::Instrument;

use super::{
    BoxFuture, EndpointHandler, EndpointMetrics, HandlerResult, RequestSender, ResultContext,
    StreamContext, start_output_span,
};
use crate::{
    operators::SinkStreamWithResult,
    runtime::{
        common::{Consumer, MessageContext, Payload},
        environment::RuntimeResult,
    },
};

pub type ResponseStream<ResR> = std::pin::Pin<Box<dyn Stream<Item = HandlerResult<ResR>> + Send>>;
pub type ServerStreamingClientFunction<ReqT, ResR> = Arc<
    dyn Fn(MessageContext, ReqT) -> BoxFuture<HandlerResult<ResponseStream<ResR>>> + Send + Sync,
>;

pub struct ServerStreamingEndpointConsumer<HandlerState, ReqT, ResR, T, R, E, H>
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
    client_function: ServerStreamingClientFunction<ReqT, ResR>,
    metrics: EndpointMetrics,
    _state: std::marker::PhantomData<fn(HandlerState)>,
}

pub fn make_grpc_server_streaming_endpoint_consumer<HandlerState, ReqT, ResR, T, R, E, H>(
    stream: &Arc<SinkStreamWithResult<T, R, E>>,
    connector_name: &str,
    endpoint_name: &str,
    handler: H,
    client_function: ServerStreamingClientFunction<ReqT, ResR>,
) -> RuntimeResult<Arc<ServerStreamingEndpointConsumer<HandlerState, ReqT, ResR, T, R, E, H>>>
where
    HandlerState: Send + 'static,
    ReqT: Send + 'static,
    ResR: Send + 'static,
    T: Send + Sync + 'static,
    R: Send + Sync + 'static,
    E: Send + Sync + 'static,
    H: EndpointHandler<HandlerState, ReqT, ResR, T, R, E> + 'static,
{
    let consumer = Arc::new(ServerStreamingEndpointConsumer {
        stream: Arc::downgrade(stream),
        stream_context: StreamContext::new(Arc::downgrade(stream)),
        handler,
        client_function,
        metrics: EndpointMetrics::new(stream, connector_name, endpoint_name)?,
        _state: std::marker::PhantomData,
    });
    stream.set_sink_consumer(consumer.clone())?;
    Ok(consumer)
}

#[async_trait]
impl<HandlerState, ReqT, ResR, T, R, E, H> Consumer<T>
    for ServerStreamingEndpointConsumer<HandlerState, ReqT, ResR, T, R, E, H>
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
                    )
                });
                return;
            }
        };
        span.in_scope(|| tracing::info!(event.name = "begin_request"));
        let state = Arc::new(tokio::sync::Mutex::new(state));
        let started_at = self.metrics.request_start();
        let sender = RequestSender::with_span(span.clone());
        let mut result = self
            .handler
            .consume_message(
                context.clone(),
                self.stream_context.clone(),
                Arc::clone(&state),
                value,
                &sender,
                ResultContext::with_span(span.clone()),
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
                "gRPC sink handler failed"
            ),
        });
        if result.is_ok() {
            result = match sender.take() {
                Ok(request) => {
                    let observation = self.metrics.grpc_client_start();
                    match (self.client_function)(context.clone(), request)
                        .instrument(span.clone())
                        .await
                    {
                        Ok(mut responses) => {
                            span.in_scope(|| tracing::info!(event.name = "grpc_call"));
                            let mut response_result = Ok(());
                            let mut messages_received = 0_u64;
                            while let Some(response) = responses.next().await {
                                response_result = match response {
                                    Ok(response) => {
                                        messages_received += 1;
                                        self.handler
                                            .handle_response(
                                                context.clone(),
                                                self.stream_context.clone(),
                                                Arc::clone(&state),
                                                response,
                                            )
                                            .instrument(span.clone())
                                            .await
                                    }
                                    Err(error) => Err(error),
                                };
                                if response_result.is_err() {
                                    if let Err(error) = &response_result {
                                        crate::runtime::telemetry::record_span_error(&span, error);
                                        span.in_scope(|| {
                                            tracing::error!(
                                                event.name = "handle_response.error",
                                                error = %error
                                            )
                                        });
                                    }
                                    break;
                                }
                            }
                            span.in_scope(|| tracing::info!(event.name = "eof", messages_received));
                            observation.finish(if response_result.is_ok() {
                                "OK"
                            } else {
                                "UNKNOWN"
                            });
                            response_result
                        }
                        Err(error) => {
                            crate::runtime::telemetry::record_span_error(&span, &error);
                            observation.finish(crate::runtime::telemetry::grpc_error_status(
                                error.as_ref(),
                            ));
                            span.in_scope(|| {
                                tracing::error!(
                                    event.name = "grpc_call.error",
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
        self.handler
            .end_request(context, self.stream_context.clone(), &result, state)
            .instrument(span.clone())
            .await;
        self.metrics.request_end(started_at, &result);
    }
}
