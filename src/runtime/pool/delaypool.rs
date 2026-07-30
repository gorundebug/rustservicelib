use std::{
    future::Future,
    sync::{Arc, OnceLock},
    time::{Duration, Instant},
};

use tokio::{sync::Mutex, task::JoinSet};

use crate::runtime::{
    common::MessageContext,
    environment::{
        RuntimeEnvironment, RuntimeError, RuntimeResult,
        metrics::{Float64Histogram, Int64Counter, Int64Gauge, Labels},
    },
};

#[derive(Default)]
struct DelayPoolState {
    stopped: bool,
    tasks: JoinSet<()>,
}

/// Go-compatible delay pool.
///
/// Context cancellation expedites an accepted task; the callback itself
/// decides whether cancellation means "run now" or "skip". `DelayStream`
/// uses the latter, exactly like the Go operator.
#[derive(Default)]
pub struct DelayPool {
    state: Mutex<DelayPoolState>,
    metrics: OnceLock<DelayPoolMetrics>,
}

#[derive(Clone)]
struct DelayPoolMetrics {
    wait_queue_length: Int64Gauge,
    tasks_total: Int64Counter,
    execution_duration: Float64Histogram,
    task_cancelled: Int64Counter,
    stop_timeout: Int64Counter,
}

impl DelayPool {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    pub(crate) fn configure_metrics(&self, environment: &RuntimeEnvironment) -> RuntimeResult<()> {
        if self.metrics.get().is_some() {
            return Ok(());
        }
        let scope = environment.metrics().scope(
            "delay_pool",
            [("service".to_owned(), environment.service_name())]
                .into_iter()
                .collect(),
        );
        let metrics = DelayPoolMetrics {
            wait_queue_length: scope.gauge(
                "wait_queue_length",
                "Delay pool wait queue length",
                Labels::new(),
            )?,
            tasks_total: scope.counter(
                "tasks_total",
                "Total number of tasks executed by delay pool",
                Labels::new(),
            )?,
            execution_duration: scope.histogram(
                "task_execution_duration_seconds",
                "Task execution duration in seconds",
                Labels::new(),
                None,
            )?,
            task_cancelled: scope.counter(
                "events_total",
                "Total number of events in delay pool",
                [("event".to_owned(), "task_cancelled".to_owned())]
                    .into_iter()
                    .collect(),
            )?,
            stop_timeout: scope.counter(
                "events_total",
                "Total number of events in delay pool",
                [("event".to_owned(), "stop_timeout".to_owned())]
                    .into_iter()
                    .collect(),
            )?,
        };
        let _ = self.metrics.set(metrics);
        Ok(())
    }

    pub async fn delay<F>(
        &self,
        context: MessageContext,
        duration: Duration,
        task: F,
    ) -> RuntimeResult<()>
    where
        F: Future<Output = ()> + Send + 'static,
    {
        if context.is_cancelled() {
            return Err(RuntimeError::ContextCancelled);
        }

        let mut state = self.state.lock().await;
        if state.stopped {
            return Err(RuntimeError::ResourceStopped("delay".to_owned()));
        }

        let duration = context
            .deadline()
            .map(|deadline| deadline.saturating_duration_since(tokio::time::Instant::now()))
            .map_or(duration, |remaining| remaining.min(duration));
        let metrics = self.metrics.get().cloned();
        if let Some(metrics) = &metrics {
            metrics.wait_queue_length.inc();
        }
        state.tasks.spawn(async move {
            let mut cancelled = false;
            if !duration.is_zero() {
                tokio::select! {
                    _ = tokio::time::sleep(duration) => {}
                    _ = context.cancelled() => {
                        cancelled = true;
                    }
                }
            }
            if let Some(metrics) = &metrics {
                metrics.wait_queue_length.dec();
                if cancelled {
                    metrics.task_cancelled.inc();
                }
            }
            let started_at = Instant::now();
            super::run_task("delay", Box::pin(task)).await;
            if let Some(metrics) = &metrics {
                metrics.tasks_total.inc();
                metrics
                    .execution_duration
                    .observe(started_at.elapsed().as_secs_f64());
            }
        });
        Ok(())
    }

    pub async fn stop(&self) {
        self.stop_with_context(MessageContext::new()).await;
    }

    pub async fn stop_with_context(&self, context: MessageContext) {
        let mut tasks = {
            let mut state = self.state.lock().await;
            state.stopped = true;
            std::mem::take(&mut state.tasks)
        };
        let wait = async move { while tasks.join_next().await.is_some() {} };
        tokio::pin!(wait);
        let timed_out = tokio::select! {
            _ = &mut wait => false,
            _ = context.cancelled() => true,
        };
        if timed_out {
            if let Some(metrics) = self.metrics.get() {
                metrics.stop_timeout.inc();
            }
            tracing::warn!("delay pool stopped by timeout");
            wait.await;
        }
    }
}
