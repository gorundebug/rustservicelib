use std::sync::Weak;

use crate::{
    operators::SinkStreamWithResult,
    runtime::{
        common::{MessageContext, Payload},
        environment::Lifecycle,
    },
};

pub trait DataSink: Lifecycle {
    fn id(&self) -> i32;
    fn name(&self) -> &str;
}

pub struct SinkStreamContext<T, R, E>
where
    T: Send + Sync + 'static,
    R: Send + Sync + 'static,
    E: Send + Sync + 'static,
{
    stream: Weak<SinkStreamWithResult<T, R, E>>,
}

impl<T, R, E> Clone for SinkStreamContext<T, R, E>
where
    T: Send + Sync + 'static,
    R: Send + Sync + 'static,
    E: Send + Sync + 'static,
{
    fn clone(&self) -> Self {
        Self {
            stream: self.stream.clone(),
        }
    }
}

impl<T, R, E> SinkStreamContext<T, R, E>
where
    T: Send + Sync + 'static,
    R: Send + Sync + 'static,
    E: Send + Sync + 'static,
{
    pub fn new(stream: Weak<SinkStreamWithResult<T, R, E>>) -> Self {
        Self { stream }
    }

    pub async fn collect(&self, context: MessageContext, value: R) {
        if let Some(stream) = self.stream.upgrade() {
            stream.consume_result(context, Payload::new(value)).await;
        }
    }

    pub async fn error_collect(&self, context: MessageContext, value: E) {
        if let Some(stream) = self.stream.upgrade() {
            stream
                .error_stream()
                .emit(context, Payload::new(value))
                .await;
        }
    }
}
