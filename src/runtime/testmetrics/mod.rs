use async_trait::async_trait;

use crate::runtime::environment::{
    RuntimeResult,
    metrics::{Metrics, MetricsEngine},
};

/// In-memory metrics engine used by runtime and generated-service tests.
///
/// The same registry also renders Prometheus text, so tests validate the exact
/// names and labels consumed by production dashboards.
#[derive(Clone, Default)]
pub struct TestMetrics {
    metrics: Metrics,
}

#[async_trait]
impl MetricsEngine for TestMetrics {
    fn metrics(&self) -> &Metrics {
        &self.metrics
    }

    async fn shutdown(&self) -> RuntimeResult<()> {
        Ok(())
    }
}

impl TestMetrics {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn metrics(&self) -> &Metrics {
        &self.metrics
    }

    pub fn prometheus(&self) -> String {
        self.metrics.render_prometheus()
    }

    pub fn contains(&self, sample: &str) -> bool {
        self.prometheus().lines().any(|line| line == sample)
    }
}
