use super::queuedpool::QueuedPool;
use crate::runtime::{
    common::MessageContext,
    environment::{RuntimeEnvironment, RuntimeResult},
};
use std::sync::Arc;
pub type BoxTask = std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send + 'static>>;

/// FIFO pool; cancellation promotes queued work to the head.
/// Callbacks run independently up to the configured executor count.
pub struct TaskPool {
    inner: Arc<QueuedPool>,
}
impl TaskPool {
    pub fn new(
        name: impl Into<String>,
        environment: RuntimeEnvironment,
    ) -> RuntimeResult<Arc<Self>> {
        Ok(Arc::new(Self {
            inner: QueuedPool::new(name.into(), false, environment)?,
        }))
    }
    pub(crate) fn configure_metrics(&self, _environment: &RuntimeEnvironment) -> RuntimeResult<()> {
        Ok(())
    }
    pub fn name(&self) -> &str {
        self.inner.name()
    }
    pub fn start(self: &Arc<Self>) -> RuntimeResult<()> {
        self.inner.start()
    }
    pub(crate) fn reload_config(self: &Arc<Self>) {
        self.inner.reload_config();
    }
    pub async fn add_task(
        self: &Arc<Self>,
        context: MessageContext,
        task: BoxTask,
    ) -> RuntimeResult<()> {
        self.inner.add_task(context, 0, task).await
    }
    pub async fn stop(self: &Arc<Self>) {
        self.stop_with_context(MessageContext::new()).await;
    }
    pub async fn stop_with_context(self: &Arc<Self>, context: MessageContext) {
        self.inner.stop(context).await;
    }
}
