use async_trait::async_trait;

use crate::runtime::environment::RuntimeResult;

/// Lifecycle of the tracing backend. Span creation remains safe without an
/// installed backend; spans then behave as no-ops.
#[async_trait]
pub trait TracingEngine: Send + Sync {
    async fn shutdown(&self) -> RuntimeResult<()>;
}

#[derive(Default)]
pub struct StdoutTracingEngine;

#[async_trait]
impl TracingEngine for StdoutTracingEngine {
    async fn shutdown(&self) -> RuntimeResult<()> {
        Ok(())
    }
}
