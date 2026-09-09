use std::{
    future::Future,
    pin::Pin,
    sync::{
        Arc, OnceLock,
        atomic::{AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};

use futures::{
    FutureExt, StreamExt,
    future::{BoxFuture, Shared},
    stream::FuturesUnordered,
};
use tokio::{
    sync::{Mutex, mpsc},
    task::JoinSet,
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
    worker: Option<Shared<BoxFuture<'static, ()>>>,
}

type BoxDelayTask = Pin<Box<dyn Future<Output = ()> + Send + 'static>>;

struct ScheduledDelay {
    context: MessageContext,
    run_at: tokio::time::Instant,
    expedited_by_deadline: bool,
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
        // Only readiness futures are polled by this worker. User callbacks run
        // in independent Tokio tasks, never inline in the timer scheduler.
        let mut delays = FuturesUnordered::new();
        let mut callbacks = JoinSet::new();
        let mut accepting = true;
        while accepting || !delays.is_empty() || !callbacks.is_empty() {
            tokio::select! {
                scheduled = receiver.recv(), if accepting => match scheduled {
                    Some(scheduled) => delays.push(scheduled.wait_ready()),
                    None => accepting = false,
                },
                Some((scheduled, cancelled)) = delays.next(), if !delays.is_empty() => {
                    callbacks.spawn(scheduled.execute(cancelled));
                },
                _ = callbacks.join_next(), if !callbacks.is_empty() => {}
            }
        }
    }

    fn send_scheduled(
        sender: &mpsc::UnboundedSender<ScheduledDelay>,
        scheduled: ScheduledDelay,
    ) -> RuntimeResult<()> {
        if let Some(metrics) = &scheduled.metrics {
            metrics.wait_queue_length.inc();
        }
        scheduled.active_tasks.fetch_add(1, Ordering::Relaxed);
        if let Err(error) = sender.send(scheduled) {
            let scheduled = error.0;
            if let Some(metrics) = &scheduled.metrics {
                metrics.wait_queue_length.dec();
            }
            scheduled.active_tasks.fetch_sub(1, Ordering::Relaxed);
            return Err(RuntimeError::ResourceStopped("delay".to_owned()));
        }
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

        let now = tokio::time::Instant::now();
        let requested_at = now.checked_add(duration);
        let run_at = match (requested_at, context.deadline()) {
            (Some(requested), Some(deadline)) => requested.min(deadline),
            (Some(requested), None) => requested,
            (None, Some(deadline)) => deadline,
            (None, None) => {
                return Err(RuntimeError::InvalidConfiguration(
                    "delay duration exceeds the clock range".to_owned(),
                ));
            }
        };
        let expedited_by_deadline = requested_at.is_none_or(|requested| run_at < requested);
        let metrics = self.metrics.get().cloned();
        let scheduled = ScheduledDelay {
            context,
            run_at,
            expedited_by_deadline,
            task: Box::pin(task),
            metrics,
            active_tasks: Arc::clone(&self.active_tasks),
        };

        let mut state = self.state.lock().await;
        if state.stopped {
            return Err(RuntimeError::ResourceStopped("delay".to_owned()));
        }
        if state.sender.is_none() {
            let (sender, receiver) = mpsc::unbounded_channel();
            state.sender = Some(sender);
            let worker = tokio::spawn(Self::run(receiver));
            state.worker = Some(
                async move {
                    let _ = worker.await;
                }
                .boxed()
                .shared(),
            );
        }
        Self::send_scheduled(
            state
                .sender
                .as_ref()
                .expect("delay worker sender initialized"),
            scheduled,
        )
    }

    pub async fn stop(&self) {
        self.stop_with_context(MessageContext::new()).await;
    }

    pub async fn stop_with_context(&self, context: MessageContext) {
        let worker = {
            let mut state = self.state.lock().await;
            state.stopped = true;
            state.sender.take();
            state.worker.clone()
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
            // The deadline is diagnostic only. Accepted callbacks may retain
            // graph observers, so the worker must retire before graph teardown.
            let _ = worker.await;
        }
    }
}

impl ScheduledDelay {
    async fn wait_ready(self) -> (Self, bool) {
        // Absolute admission-time deadline: backlog in the scheduler must not
        // restart a relative delay when it eventually receives the entry.
        let cancelled = if self.run_at <= tokio::time::Instant::now() {
            self.expedited_by_deadline || self.context.is_cancelled()
        } else {
            tokio::select! {
                _ = tokio::time::sleep_until(self.run_at) => self.expedited_by_deadline,
                _ = self.context.cancelled() => true,
            }
        };
        (self, cancelled)
    }

    async fn execute(self, cancelled: bool) {
        let Self {
            task,
            metrics,
            active_tasks,
            ..
        } = self;
        if cancelled && let Some(metrics) = &metrics {
            metrics.task_cancelled.inc();
        }
        let started_at = metrics
            .as_ref()
            .is_some_and(|metrics| metrics.execution_duration.is_enabled())
            .then(Instant::now);
        super::run_task("delay", task).await;
        if let Some(metrics) = &metrics {
            metrics.wait_queue_length.dec();
            metrics.tasks_total.inc();
            if let Some(started_at) = started_at {
                metrics
                    .execution_duration
                    .observe(started_at.elapsed().as_secs_f64());
            }
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
