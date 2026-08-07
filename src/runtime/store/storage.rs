use async_trait::async_trait;

use crate::runtime::{common::MessageContext, environment::RuntimeResult};

#[async_trait]
pub trait Storage: Send + Sync {
    async fn start(&self, context: MessageContext) -> RuntimeResult<()>;
    async fn stop(&self, context: MessageContext);
}
