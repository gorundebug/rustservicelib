use async_trait::async_trait;

use crate::runtime::environment::RuntimeResult;

/// Lifecycle of the structured logging backend.
///
/// Runtime call sites use `tracing` fields directly. A logs engine installs
/// the corresponding subscriber/exporter and owns its flush on shutdown.
#[async_trait]
pub trait LogsEngine: Send + Sync {
    async fn shutdown(&self) -> RuntimeResult<()>;
}

/// The default stdout subscriber is process-global and has nothing to flush.
#[derive(Default)]
pub struct StdoutLogsEngine;

#[async_trait]
impl LogsEngine for StdoutLogsEngine {
    async fn shutdown(&self) -> RuntimeResult<()> {
        Ok(())
    }
}
