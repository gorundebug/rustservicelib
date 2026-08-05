use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use tokio::sync::Notify;

use super::{EndpointHandler, GrpcTypedEndpointConsumer, HandlerResult, Sender};
use crate::{
    operators::InputStream,
    runtime::{common::MessageContext, environment::RuntimeResult},
};

pub(super) struct UnarySender<ResR> {
    value: Mutex<Option<ResR>>,
    sent: Notify,
    span: Mutex<Option<tracing::Span>>,
}

impl<ResR> UnarySender<ResR> {
    pub(super) fn new() -> Self {
        Self {
            value: Mutex::new(None),
            sent: Notify::new(),
            span: Mutex::new(None),
        }
    }

    fn set_span(&self, span: tracing::Span) {
        *self.span.lock().expect("gRPC unary span lock poisoned") = Some(span);
    }

    pub(super) fn take(&self) -> Option<ResR> {
        self.value
            .lock()
            .expect("gRPC unary sender lock poisoned")
            .take()
    }

    pub(super) async fn receive(&self, context: MessageContext) -> HandlerResult<ResR> {
        loop {
            // Register the waiter before checking the slot. Otherwise a send
            // can land between `take()` and `notified()`, and `notify_waiters`
            // does not retain a permit for a waiter that was not registered
            // yet. Under load that left a completed unary response asleep
            // until the caller's gRPC deadline.
            let notified = self.sent.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if let Some(value) = self.take() {
                return Ok(value);
            }
            tokio::select! {
                _ = &mut notified => {}
                _ = context.cancelled() => {
                    return Err("gRPC unary request context cancelled".into());
                }
            }
        }
    }
}

#[async_trait]
impl<ResR> Sender<ResR> for UnarySender<ResR>
where
    ResR: Send + 'static,
{
    async fn send(&self, _context: MessageContext, value: ResR) -> HandlerResult {
        let mut slot = self.value.lock().expect("gRPC unary sender lock poisoned");
        if slot.is_some() {
            return Err("gRPC unary result already sent".into());
        }
        *slot = Some(value);
        drop(slot);
        if let Some(span) = self
            .span
            .lock()
            .expect("gRPC unary span lock poisoned")
            .as_ref()
        {
            span.in_scope(|| tracing::info!(event.name = "send"));
        }
        self.sent.notify_one();
        Ok(())
    }
}

pub struct NoStreamingEndpointConsumer<HandlerState, ReqT, ResR, T, R, E, H>
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

pub fn make_grpc_no_streaming_endpoint_consumer<HandlerState, ReqT, ResR, T, R, E, H>(
    input_stream: InputStream<T, R, E>,
    connector_name: &str,
    endpoint_name: &str,
    handler: H,
) -> RuntimeResult<Arc<NoStreamingEndpointConsumer<HandlerState, ReqT, ResR, T, R, E, H>>>
where
    HandlerState: Send + 'static,
    ReqT: Send + 'static,
    ResR: Send + Default + 'static,
    T: Send + Sync + 'static,
    R: Send + Sync + 'static,
    E: Send + Sync + 'static,
    H: EndpointHandler<HandlerState, ReqT, ResR, T, R, E> + 'static,
{
    Ok(Arc::new(NoStreamingEndpointConsumer {
        endpoint_consumer: GrpcTypedEndpointConsumer::make(
            input_stream,
            connector_name,
            endpoint_name,
            handler,
        )?,
    }))
}

impl<HandlerState, ReqT, ResR, T, R, E, H>
    NoStreamingEndpointConsumer<HandlerState, ReqT, ResR, T, R, E, H>
where
    HandlerState: Send + 'static,
    ReqT: Send + 'static,
    ResR: Send + Default + 'static,
    T: Send + Sync + 'static,
    R: Send + Sync + 'static,
    E: Send + Sync + 'static,
    H: EndpointHandler<HandlerState, ReqT, ResR, T, R, E> + 'static,
{
    pub async fn handle(&self, context: MessageContext, request: ReqT) -> HandlerResult<ResR> {
        let sender = Arc::new(UnarySender::new());
        let (stream_id, pending) = self
            .endpoint_consumer
            .begin(context, sender.clone())
            .await?;
        sender.set_span(pending.span.clone());
        let mut result = self.endpoint_consumer.consume(&pending, request).await;
        if result.is_ok() {
            self.endpoint_consumer.eof(&pending).await;
        }

        let response = if result.is_ok() {
            if self.endpoint_consumer.has_result() {
                match sender.receive(pending.context.read().await.clone()).await {
                    Ok(value) => {
                        pending
                            .span
                            .in_scope(|| tracing::info!(event.name = "result_received"));
                        Some(value)
                    }
                    Err(error) => {
                        result = Err(error);
                        None
                    }
                }
            } else {
                None
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

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn unary_sender_does_not_lose_concurrent_notifications() {
        const ATTEMPTS: usize = 20_000;
        let mut tasks = Vec::with_capacity(ATTEMPTS);

        for value in 0..ATTEMPTS {
            let sender = Arc::new(UnarySender::new());
            let receiver = Arc::clone(&sender);
            tasks.push(tokio::spawn(async move {
                let received = tokio::time::timeout(
                    Duration::from_secs(1),
                    receiver.receive(MessageContext::new()),
                )
                .await
                .expect("unary response notification was lost")
                .unwrap();
                assert_eq!(received, value);
            }));
            sender.send(MessageContext::new(), value).await.unwrap();
        }

        for task in tasks {
            task.await.unwrap();
        }
    }
}
