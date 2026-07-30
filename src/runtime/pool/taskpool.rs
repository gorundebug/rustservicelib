use std::{
    collections::VecDeque,
    future::Future,
    pin::Pin,
    sync::{
        Arc, Mutex as StdMutex, OnceLock,
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
    },
    time::Instant,
};

use tokio::{sync::Mutex, task::JoinHandle};
use tokio_util::sync::CancellationToken;

use crate::runtime::{
    common::MessageContext,
    environment::{
        RuntimeEnvironment, RuntimeError, RuntimeResult,
        metrics::{Float64Histogram, Int64Counter, Int64Gauge, Labels},
    },
};

pub type BoxTask = Pin<Box<dyn Future<Output = ()> + Send + 'static>>;

struct QueuedTask {
    sequence: u64,
    context: MessageContext,
    task: BoxTask,
    stop_cancellation_watch: CancellationToken,
}

struct TaskPoolMetrics {
    queue_length: Int64Gauge,
    executors_target: Int64Gauge,
    executors_allocated: Int64Gauge,
    executors_busy: Int64Gauge,
    tasks_total: Int64Counter,
    execution_duration: Float64Histogram,
    task_rejected: Int64Counter,
    task_cancelled: Int64Counter,
    stop_timeout: Int64Counter,
}

/// FIFO task pool. Accepted tasks are drained during stop. Cancellation of a
/// queued task expedites it to the head, matching Go's task-pool behavior.
pub struct TaskPool {
    name: String,
    queue: Mutex<VecDeque<QueuedTask>>,
    notify: tokio::sync::Notify,
    stopped: AtomicBool,
    started: AtomicBool,
    sequence: AtomicU64,
    executors_target: AtomicUsize,
    generation: AtomicU64,
    environment: RuntimeEnvironment,
    workers: StdMutex<Vec<JoinHandle<()>>>,
    metrics: OnceLock<TaskPoolMetrics>,
}

impl TaskPool {
    pub fn new(
        name: impl Into<String>,
        environment: RuntimeEnvironment,
    ) -> RuntimeResult<Arc<Self>> {
        let pool = Arc::new(Self {
            name: name.into(),
            queue: Mutex::new(VecDeque::new()),
            notify: tokio::sync::Notify::new(),
            stopped: AtomicBool::new(false),
            started: AtomicBool::new(false),
            sequence: AtomicU64::new(0),
            executors_target: AtomicUsize::new(0),
            generation: AtomicU64::new(0),
            environment,
            workers: StdMutex::new(Vec::new()),
            metrics: OnceLock::new(),
        });
        pool.configure_metrics(&pool.environment)?;
        Ok(pool)
    }

    pub(crate) fn configure_metrics(&self, environment: &RuntimeEnvironment) -> RuntimeResult<()> {
        if self.metrics.get().is_some() {
            return Ok(());
        }
        let scope = environment.metrics().scope(
            "task_pool",
            [
                ("service".to_owned(), environment.service_name()),
                ("name".to_owned(), self.name.clone()),
            ]
            .into_iter()
            .collect(),
        );
        let metrics = TaskPoolMetrics {
            queue_length: scope.gauge(
                "queue_length",
                "Task pool wait queue length",
                Labels::new(),
            )?,
            executors_target: scope.gauge(
                "executors_target",
                "Desired number of task pool executors",
                Labels::new(),
            )?,
            executors_allocated: scope.gauge(
                "executors_allocated",
                "Number of live task pool executors",
                Labels::new(),
            )?,
            executors_busy: scope.gauge(
                "executors_busy",
                "Number of task pool executors running callbacks",
                Labels::new(),
            )?,
            tasks_total: scope.counter(
                "tasks_total",
                "Total number of tasks executed by task pool",
                Labels::new(),
            )?,
            execution_duration: scope.histogram(
                "task_execution_duration_seconds",
                "Task execution duration in seconds",
                Labels::new(),
                None,
            )?,
            task_rejected: scope.counter(
                "events_total",
                "Total number of events in task pool",
                [("event".to_owned(), "task_rejected".to_owned())]
                    .into_iter()
                    .collect(),
            )?,
            task_cancelled: scope.counter(
                "events_total",
                "Total number of events in task pool",
                [("event".to_owned(), "task_cancelled".to_owned())]
                    .into_iter()
                    .collect(),
            )?,
            stop_timeout: scope.counter(
                "events_total",
                "Total number of events in task pool",
                [("event".to_owned(), "stop_timeout".to_owned())]
                    .into_iter()
                    .collect(),
            )?,
        };
        metrics
            .executors_target
            .set(self.configured_executors()? as i64);
        let _ = self.metrics.set(metrics);
        Ok(())
    }

    pub fn start(self: &Arc<Self>) -> RuntimeResult<()> {
        if self.stopped.load(Ordering::Acquire) {
            return Err(RuntimeError::ResourceStopped(self.name.clone()));
        }
        if self
            .started
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return Err(RuntimeError::ResourceAlreadyStarted(self.name.clone()));
        }
        let executors = self.configured_executors()?;
        self.executors_target.store(executors, Ordering::Release);
        self.spawn_workers(self.generation.load(Ordering::Acquire), executors);
        Ok(())
    }

    fn configured_executors(&self) -> RuntimeResult<usize> {
        self.environment
            .runtime_config()
            .pool_by_name(&self.name)
            .map(|config| config.executors_count)
            .ok_or_else(|| RuntimeError::TaskPoolNotFound(self.name.clone()))
    }

    pub(crate) fn reload_config(self: &Arc<Self>) {
        let Ok(executors) = self.configured_executors() else {
            tracing::error!(pool = self.name, "task pool config disappeared on reload");
            return;
        };
        let previous = self.executors_target.swap(executors, Ordering::AcqRel);
        if previous == executors {
            return;
        }
        if let Some(metrics) = self.metrics.get() {
            metrics.executors_target.set(executors as i64);
        }
        if self.started.load(Ordering::Acquire) && !self.stopped.load(Ordering::Acquire) {
            let generation = self.generation.fetch_add(1, Ordering::AcqRel) + 1;
            self.spawn_workers(generation, executors);
            self.notify.notify_waiters();
        }
    }

    fn spawn_workers(self: &Arc<Self>, generation: u64, executors: usize) {
        for _ in 0..executors {
            let worker = Arc::clone(self);
            self.workers
                .lock()
                .expect("task pool workers lock poisoned")
                .push(tokio::spawn(async move { worker.run(generation).await }));
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub async fn add_task(
        self: &Arc<Self>,
        context: MessageContext,
        task: BoxTask,
    ) -> RuntimeResult<()> {
        if context.is_cancelled() {
            if let Some(metrics) = self.metrics.get() {
                metrics.task_rejected.inc();
            }
            return Err(RuntimeError::ContextCancelled);
        }
        let sequence = self.sequence.fetch_add(1, Ordering::Relaxed);
        let stop_cancellation_watch = CancellationToken::new();
        let mut queue = self.queue.lock().await;
        if self.stopped.load(Ordering::Acquire) {
            if let Some(metrics) = self.metrics.get() {
                metrics.task_rejected.inc();
            }
            return Err(RuntimeError::ResourceStopped(self.name.clone()));
        }
        queue.push_back(QueuedTask {
            sequence,
            context: context.clone(),
            task,
            stop_cancellation_watch: stop_cancellation_watch.clone(),
        });
        if let Some(metrics) = self.metrics.get() {
            metrics.queue_length.set(queue.len() as i64);
        }
        drop(queue);
        self.notify.notify_one();

        let pool = Arc::downgrade(self);
        tokio::spawn(async move {
            tokio::select! {
                _ = context.cancelled() => {}
                _ = stop_cancellation_watch.cancelled() => return,
            }
            let Some(pool) = pool.upgrade() else {
                return;
            };
            let mut queue = pool.queue.lock().await;
            let Some(index) = queue.iter().position(|task| task.sequence == sequence) else {
                return;
            };
            if let Some(metrics) = pool.metrics.get() {
                metrics.task_cancelled.inc();
            }
            if index != 0 {
                let task = queue.remove(index).expect("queued task index is valid");
                queue.push_front(task);
                drop(queue);
                pool.notify.notify_one();
            }
        });
        Ok(())
    }

    pub async fn stop(self: &Arc<Self>) {
        self.stop_with_context(MessageContext::new()).await;
    }

    pub async fn stop_with_context(self: &Arc<Self>, context: MessageContext) {
        self.stopped.store(true, Ordering::Release);
        if self
            .started
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
        {
            let executors = self.executors_target.load(Ordering::Acquire);
            self.spawn_workers(self.generation.load(Ordering::Acquire), executors);
        }
        self.notify.notify_waiters();
        let workers = {
            let mut workers = self
                .workers
                .lock()
                .expect("task pool workers lock poisoned");
            std::mem::take(&mut *workers)
        };
        let wait = async move {
            for worker in workers {
                let _ = worker.await;
            }
        };
        tokio::pin!(wait);
        let timed_out = tokio::select! {
            _ = &mut wait => false,
            _ = context.cancelled() => true,
        };
        if timed_out {
            let tasks_count = self.queue.lock().await.len();
            if let Some(metrics) = self.metrics.get() {
                metrics.stop_timeout.inc();
            }
            tracing::warn!(
                pool = self.name,
                tasks_count,
                "task pool stopped by timeout"
            );
            wait.await;
        }
        if let Some(metrics) = self.metrics.get() {
            metrics.executors_allocated.set(0);
        }
    }

    async fn run(self: Arc<Self>, generation: u64) {
        if let Some(metrics) = self.metrics.get() {
            metrics.executors_allocated.inc();
        }
        struct AllocatedGuard<'a>(&'a TaskPool);
        impl Drop for AllocatedGuard<'_> {
            fn drop(&mut self) {
                if let Some(metrics) = self.0.metrics.get() {
                    metrics.executors_allocated.dec();
                }
            }
        }
        let _allocated = AllocatedGuard(&self);
        loop {
            if generation != self.generation.load(Ordering::Acquire)
                && !self.stopped.load(Ordering::Acquire)
            {
                return;
            }
            let queued = {
                let mut queue = self.queue.lock().await;
                let queued = queue.pop_front();
                if let Some(metrics) = self.metrics.get() {
                    metrics.queue_length.set(queue.len() as i64);
                }
                queued
            };
            match queued {
                Some(queued) => {
                    queued.stop_cancellation_watch.cancel();
                    let _context = queued.context;
                    let started_at = Instant::now();
                    if let Some(metrics) = self.metrics.get() {
                        metrics.executors_busy.inc();
                    }
                    super::run_task(&self.name, queued.task).await;
                    if let Some(metrics) = self.metrics.get() {
                        metrics.executors_busy.dec();
                        metrics.tasks_total.inc();
                        metrics
                            .execution_duration
                            .observe(started_at.elapsed().as_secs_f64());
                    }
                }
                None if self.stopped.load(Ordering::Acquire) => return,
                None => self.notify.notified().await,
            }
        }
    }
}
