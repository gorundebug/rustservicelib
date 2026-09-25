use std::sync::Arc;

use super::{EndpointHandler, GrpcTypedEndpointConsumer, HandlerResult, Sender};
use crate::{
    operators::InputStream,
    runtime::{common::MessageContext, environment::RuntimeResult},
};

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
    endpoint_consumer: Arc<GrpcTypedEndpointConsumer<HandlerState, ReqT, ResR, T, R, E, H>>,
}

pub fn make_grpc_server_streaming_endpoint_consumer<HandlerState, ReqT, ResR, T, R, E, H>(
    input_stream: InputStream<T, R, E>,
    handler: H,
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
    Ok(Arc::new(ServerStreamingEndpointConsumer {
        endpoint_consumer: GrpcTypedEndpointConsumer::make(input_stream, handler)?,
    }))
}

impl<HandlerState, ReqT, ResR, T, R, E, H>
    ServerStreamingEndpointConsumer<HandlerState, ReqT, ResR, T, R, E, H>
where
    HandlerState: Send + 'static,
    ReqT: Send + 'static,
    ResR: Send + 'static,
    T: Send + Sync + 'static,
    R: Send + Sync + 'static,
    E: Send + Sync + 'static,
    H: EndpointHandler<HandlerState, ReqT, ResR, T, R, E> + 'static,
{
    pub async fn handle(
        &self,
        context: MessageContext,
        request: ReqT,
        sender: Arc<dyn Sender<ResR>>,
    ) -> HandlerResult {
        let (stream_id, pending, lifecycle) =
            self.endpoint_consumer.begin_owned(context, sender).await?;
        let (mut lifecycle, mut result) = self
            .endpoint_consumer
            .consume_owned(lifecycle, &pending, request)
            .await;
        if result.is_ok() {
            lifecycle = self.endpoint_consumer.eof_owned(lifecycle, &pending).await;
            if self.endpoint_consumer.has_result() {
                result = self.endpoint_consumer.wait_done(&pending).await;
            }
        }
        self.endpoint_consumer
            .finish_owned(lifecycle, stream_id, pending, result)
            .await
    }
}
