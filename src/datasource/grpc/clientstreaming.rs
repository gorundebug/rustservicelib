use std::sync::Arc;

use futures::{Stream, StreamExt};

use super::{EndpointHandler, GrpcTypedEndpointConsumer, HandlerResult};
use crate::{
    operators::InputStream,
    runtime::{common::MessageContext, environment::RuntimeResult},
};

pub struct ClientStreamingEndpointConsumer<HandlerState, ReqT, ResR, T, R, E, H>
where
    HandlerState: Send + 'static,
    ReqT: Send + 'static,
    ResR: Send + Default + 'static,
    T: Send + Sync + 'static,
    R: Send + Sync + 'static,
    E: Send + Sync + 'static,
    H: EndpointHandler<HandlerState, ReqT, ResR, T, R, E> + 'static,
{
    endpoint_consumer: Arc<GrpcTypedEndpointConsumer<HandlerState, ReqT, ResR, T, R, E, H>>,
}

pub fn make_grpc_client_streaming_endpoint_consumer<HandlerState, ReqT, ResR, T, R, E, H>(
    input_stream: InputStream<T, R, E>,
    connector_name: &str,
    endpoint_name: &str,
    handler: H,
) -> RuntimeResult<Arc<ClientStreamingEndpointConsumer<HandlerState, ReqT, ResR, T, R, E, H>>>
where
    HandlerState: Send + 'static,
    ReqT: Send + 'static,
    ResR: Send + Default + 'static,
    T: Send + Sync + 'static,
    R: Send + Sync + 'static,
    E: Send + Sync + 'static,
    H: EndpointHandler<HandlerState, ReqT, ResR, T, R, E> + 'static,
{
    Ok(Arc::new(ClientStreamingEndpointConsumer {
        endpoint_consumer: GrpcTypedEndpointConsumer::make(
            input_stream,
            connector_name,
            endpoint_name,
            handler,
        )?,
    }))
}

impl<HandlerState, ReqT, ResR, T, R, E, H>
    ClientStreamingEndpointConsumer<HandlerState, ReqT, ResR, T, R, E, H>
where
    HandlerState: Send + 'static,
    ReqT: Send + 'static,
    ResR: Send + Default + 'static,
    T: Send + Sync + 'static,
    R: Send + Sync + 'static,
    E: Send + Sync + 'static,
    H: EndpointHandler<HandlerState, ReqT, ResR, T, R, E> + 'static,
{
    pub async fn handle<S>(&self, context: MessageContext, mut requests: S) -> HandlerResult<ResR>
    where
        S: Stream<Item = HandlerResult<ReqT>> + Send + Unpin,
    {
        let sender = Arc::new(super::nostreaming::UnarySender::new());
        let (stream_id, pending) = self
            .endpoint_consumer
            .begin(context, sender.clone())
            .await?;
        let mut result = Ok(());
        while let Some(request) = requests.next().await {
            match request {
                Ok(request) => {
                    result = self.endpoint_consumer.consume(&pending, request).await;
                    if result.is_err() {
                        break;
                    }
                }
                Err(error) => {
                    result = Err(error);
                    break;
                }
            }
        }
        if result.is_ok() {
            self.endpoint_consumer.eof(&pending).await;
        }
        let response = if result.is_ok() && self.endpoint_consumer.has_result() {
            match sender.receive(pending.context.read().await.clone()).await {
                Ok(response) => Some(response),
                Err(error) => {
                    result = Err(error);
                    None
                }
            }
        } else {
            None
        };
        result = self
            .endpoint_consumer
            .finish(&stream_id, pending, result)
            .await;
        result?;
        Ok(response.or_else(|| sender.take()).unwrap_or_default())
    }
}
