use super::BoxTask;
use super::queuedpool::QueuedPool;
use crate::runtime::{
    common::MessageContext,
    environment::{RuntimeEnvironment, RuntimeResult},
};
use std::sync::Arc;

/// Lower numeric priority executes first.
/// Callbacks run independently up to the configured executor count.
pub struct PriorityTaskPool {
    inner: Arc<QueuedPool>,
}
impl PriorityTaskPool {
    pub fn new(
        name: impl Into<String>,
        environment: RuntimeEnvironment,
    ) -> RuntimeResult<Arc<Self>> {
        Ok(Arc::new(Self {
            inner: QueuedPool::new(name.into(), true, environment)?,
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
        priority: i32,
        task: BoxTask,
    ) -> RuntimeResult<()> {
        self.inner.add_task(context, priority, task).await
    }
    pub async fn stop(self: &Arc<Self>) {
        self.stop_with_context(MessageContext::new()).await;
    }
    pub async fn stop_with_context(self: &Arc<Self>, context: MessageContext) {
        self.inner.stop(context).await;
    }
}
