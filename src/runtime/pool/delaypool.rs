use std::{
    future::Future,
    pin::Pin,
    sync::{
        Arc, OnceLock,
        atomic::{AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};

use futures::{StreamExt, stream::FuturesUnordered};
use tokio::{
    sync::{Mutex, mpsc},
    task::JoinHandle,
};

use crate::runtime::{
    common::MessageContext,
    environment::{
        RuntimeEnvironment, RuntimeError, RuntimeResult,
        metrics::{Float64Histogram, Int64Counter, Int64Gauge, Labels},
    },
};

struct DelayPoolState {
    stopped: bool,
    sender: Option<mpsc::UnboundedSender<ScheduledDelay>>,
    worker: Option<JoinHandle<()>>,
}

type BoxDelayTask = Pin<Box<dyn Future<Output = ()> + Send + 'static>>;

struct ScheduledDelay {
    context: MessageContext,
    duration: Duration,
    task: BoxDelayTask,
    metrics: Option<DelayPoolMetrics>,
    active_tasks: Arc<AtomicUsize>,
}

/// Go-compatible delay pool.
///
/// Context cancellation expedites an accepted task; the callback itself
/// decides whether cancellation means "run now" or "skip". `DelayStream`
/// uses the latter, exactly like the Go operator.
pub struct DelayPool {
    state: Mutex<DelayPoolState>,
    metrics: OnceLock<DelayPoolMetrics>,
    active_tasks: Arc<AtomicUsize>,
}

impl Default for DelayPool {
    fn default() -> Self {
        Self {
            state: Mutex::new(DelayPoolState {
                stopped: false,
                sender: None,
                worker: None,
            }),
            metrics: OnceLock::new(),
            active_tasks: Arc::new(AtomicUsize::new(0)),
        }
    }
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

    async fn run(mut receiver: mpsc::UnboundedReceiver<ScheduledDelay>) {
        let mut delays: FuturesUnordered<BoxDelayTask> = FuturesUnordered::new();
        let mut accepting = true;
        loop {
            if !accepting {
                while delays.next().await.is_some() {}
                return;
            }
            tokio::select! {
                scheduled = receiver.recv() => match scheduled {
                    Some(scheduled) => delays.push(Box::pin(scheduled.run())),
                    None => accepting = false,
                },
                _ = delays.next(), if !delays.is_empty() => {}
            }
        }
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
        self.active_tasks.fetch_add(1, Ordering::Relaxed);
        if state.sender.is_none() {
            let (sender, receiver) = mpsc::unbounded_channel();
            state.sender = Some(sender);
            state.worker = Some(tokio::spawn(Self::run(receiver)));
        }
        let scheduled = ScheduledDelay {
            context,
            duration,
            task: Box::pin(task),
            metrics: metrics.clone(),
            active_tasks: Arc::clone(&self.active_tasks),
        };
        if state
            .sender
            .as_ref()
            .expect("delay worker sender initialized")
            .send(scheduled)
            .is_err()
        {
            if let Some(metrics) = &metrics {
                metrics.wait_queue_length.dec();
            }
            self.active_tasks.fetch_sub(1, Ordering::Relaxed);
            return Err(RuntimeError::ResourceStopped("delay".to_owned()));
        }
        Ok(())
    }

    pub async fn stop(&self) {
        self.stop_with_context(MessageContext::new()).await;
    }

    pub async fn stop_with_context(&self, context: MessageContext) {
        let worker = {
            let mut state = self.state.lock().await;
            state.stopped = true;
            state.sender.take();
            state.worker.take()
        };
        let Some(mut worker) = worker else {
            return;
        };
        let timed_out = tokio::select! {
            _ = &mut worker => false,
            _ = context.cancelled() => true,
        };
        if timed_out {
            if let Some(metrics) = self.metrics.get() {
                metrics.stop_timeout.inc();
            }
            tracing::warn!("delay pool stopped by timeout");
            let _ = worker.await;
        }
    }
}

impl ScheduledDelay {
    async fn run(self) {
        let Self {
            context,
            duration,
            task,
            metrics,
            active_tasks,
        } = self;
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
        super::run_task("delay", task).await;
        if let Some(metrics) = &metrics {
            metrics.tasks_total.inc();
            metrics
                .execution_duration
                .observe(started_at.elapsed().as_secs_f64());
        }
        active_tasks.fetch_sub(1, Ordering::Relaxed);
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use tokio::sync::oneshot;

    use super::*;

    #[tokio::test]
    async fn does_not_retain_completed_tasks_while_running() {
        let pool = DelayPool::new();
        let completed = Arc::new(AtomicUsize::new(0));
        const TASKS: usize = 1_000;

        for _ in 0..TASKS {
            let completed = Arc::clone(&completed);
            pool.delay(MessageContext::new(), Duration::ZERO, async move {
                completed.fetch_add(1, Ordering::Release);
            })
            .await
            .unwrap();
        }
        while completed.load(Ordering::Acquire) != TASKS {
            tokio::task::yield_now().await;
        }
        tokio::task::yield_now().await;

        let (release, wait) = oneshot::channel();
        pool.delay(MessageContext::new(), Duration::ZERO, async move {
            let _ = wait.await;
        })
        .await
        .unwrap();

        assert_eq!(pool.active_tasks.load(Ordering::Acquire), 1);
        let _ = release.send(());
        pool.stop().await;
    }
}
