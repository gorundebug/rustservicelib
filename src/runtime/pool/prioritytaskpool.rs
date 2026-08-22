use std::{
    cmp::Ordering,
    collections::BinaryHeap,
    sync::{
        Arc, Mutex as StdMutex, OnceLock,
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering as AtomicOrdering},
    },
    time::Instant,
};

use tokio::{sync::Notify, task::JoinHandle};

use crate::runtime::{
    common::{CancellationCallbackRegistration, MessageContext},
    environment::{
        RuntimeEnvironment, RuntimeError, RuntimeResult,
        metrics::{Float64Histogram, Int64Counter, Int64Gauge, Labels},
    },
    pool::BoxTask,
};

struct PriorityTask {
    priority: i32,
    sequence: u64,
    context: MessageContext,
    task: BoxTask,
    _cancellation_registration: CancellationCallbackRegistration,
}

impl Eq for PriorityTask {}

impl PartialEq for PriorityTask {
    fn eq(&self, other: &Self) -> bool {
        self.priority == other.priority && self.sequence == other.sequence
    }
}

impl Ord for PriorityTask {
    fn cmp(&self, other: &Self) -> Ordering {
        other
            .priority
            .cmp(&self.priority)
            .then_with(|| other.sequence.cmp(&self.sequence))
    }
}

impl PartialOrd for PriorityTask {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

struct PriorityTaskPoolMetrics {
    queue_length: Int64Gauge,
    executors_target: Int64Gauge,
    executors_allocated: Int64Gauge,
    executors_busy: Int64Gauge,
    tasks_total: Int64Counter,
    execution_duration: Float64Histogram,
    task_rejected: Int64Counter,
    task_expired: Int64Counter,
    stop_timeout: Int64Counter,
}

/// Stable priority pool: lower numeric priority runs first and equal
/// priorities preserve FIFO order.
pub struct PriorityTaskPool {
    name: String,
    queue: StdMutex<BinaryHeap<PriorityTask>>,
    notify: Notify,
    stopped: AtomicBool,
    started: AtomicBool,
    sequence: AtomicU64,
    executors_target: AtomicUsize,
    generation: AtomicU64,
    environment: RuntimeEnvironment,
    workers: StdMutex<Vec<JoinHandle<()>>>,
    metrics: OnceLock<PriorityTaskPoolMetrics>,
}

impl PriorityTaskPool {
    pub fn new(
        name: impl Into<String>,
        environment: RuntimeEnvironment,
    ) -> RuntimeResult<Arc<Self>> {
        let pool = Arc::new(Self {
            name: name.into(),
            queue: StdMutex::new(BinaryHeap::new()),
            notify: Notify::new(),
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
            "priority_task_pool",
            [
                ("service".to_owned(), environment.service_name()),
                ("name".to_owned(), self.name.clone()),
            ]
            .into_iter()
            .collect(),
        );
        let metrics = PriorityTaskPoolMetrics {
            queue_length: scope.gauge(
                "queue_length",
                "Priority task pool wait queue length",
                Labels::new(),
            )?,
            executors_target: scope.gauge(
                "executors_target",
                "Desired number of priority task pool executors",
                Labels::new(),
            )?,
            executors_allocated: scope.gauge(
                "executors_allocated",
                "Number of live priority task pool executors",
                Labels::new(),
            )?,
            executors_busy: scope.gauge(
                "executors_busy",
                "Number of priority task pool executors running callbacks",
                Labels::new(),
            )?,
            tasks_total: scope.counter(
                "tasks_total",
                "Total number of tasks executed by priority task pool",
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
                "Total number of events in priority task pool",
                [("event".to_owned(), "task_rejected".to_owned())]
                    .into_iter()
                    .collect(),
            )?,
            task_expired: scope.counter(
                "events_total",
                "Total number of events in priority task pool",
                [("event".to_owned(), "task_expired".to_owned())]
                    .into_iter()
                    .collect(),
            )?,
            stop_timeout: scope.counter(
                "events_total",
                "Total number of events in priority task pool",
                [("event".to_owned(), "stop_timeout".to_owned())]
                    .into_iter()
                    .collect(),
            )?,
        };
        let executors = self.configured_executors()?;
        self.executors_target
            .store(executors, AtomicOrdering::Release);
        metrics.executors_target.set(executors as i64);
        let _ = self.metrics.set(metrics);
        Ok(())
    }

    pub fn start(self: &Arc<Self>) -> RuntimeResult<()> {
        if self.stopped.load(AtomicOrdering::Acquire) {
            return Err(RuntimeError::ResourceStopped(self.name.clone()));
        }
        if self
            .started
            .compare_exchange(false, true, AtomicOrdering::AcqRel, AtomicOrdering::Acquire)
            .is_err()
        {
            return Err(RuntimeError::ResourceAlreadyStarted(self.name.clone()));
        }
        let executors = self.configured_executors()?;
        self.executors_target
            .store(executors, AtomicOrdering::Release);
        self.spawn_workers(self.generation.load(AtomicOrdering::Acquire), executors);
        Ok(())
    }

    fn configured_executors(&self) -> RuntimeResult<usize> {
        self.environment
            .runtime_config()
            .pool_by_name(&self.name)
            .map(|config| config.executors_count)
            .ok_or_else(|| RuntimeError::PriorityTaskPoolNotFound(self.name.clone()))
    }

    pub(crate) fn reload_config(self: &Arc<Self>) {
        let Ok(executors) = self.configured_executors() else {
            tracing::error!(
                pool = self.name,
                "priority task pool config disappeared on reload"
            );
            return;
        };
        let previous = self
            .executors_target
            .swap(executors, AtomicOrdering::AcqRel);
        if previous == executors {
            return;
        }
        if let Some(metrics) = self.metrics.get() {
            metrics.executors_target.set(executors as i64);
        }
        if self.started.load(AtomicOrdering::Acquire) && !self.stopped.load(AtomicOrdering::Acquire)
        {
            let generation = self.generation.fetch_add(1, AtomicOrdering::AcqRel) + 1;
            self.spawn_workers(generation, executors);
            self.notify.notify_waiters();
        }
    }

    fn spawn_workers(self: &Arc<Self>, generation: u64, executors: usize) {
        let mut workers = self
            .workers
            .lock()
            .expect("priority pool workers lock poisoned");
        workers.retain(|worker| !worker.is_finished());
        for _ in 0..executors {
            let worker = Arc::clone(self);
            workers.push(tokio::spawn(async move { worker.run(generation).await }));
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    fn expire_task(&self, sequence: u64) {
        let mut queue = self
            .queue
            .lock()
            .expect("priority pool queue lock poisoned");
        let mut tasks = std::mem::take(&mut *queue).into_vec();
        let Some(task) = tasks.iter_mut().find(|task| task.sequence == sequence) else {
            *queue = BinaryHeap::from(tasks);
            return;
        };
        task.priority = i32::MIN;
        if let Some(metrics) = self.metrics.get() {
            metrics.task_expired.inc();
        }
        *queue = BinaryHeap::from(tasks);
        drop(queue);
        self.notify.notify_one();
    }

    pub async fn add_task(
        self: &Arc<Self>,
        context: MessageContext,
        priority: i32,
        task: BoxTask,
    ) -> RuntimeResult<()> {
        if context.is_cancelled() {
            if let Some(metrics) = self.metrics.get() {
                metrics.task_rejected.inc();
            }
            return Err(RuntimeError::ContextCancelled);
        }
        if self.stopped.load(AtomicOrdering::Acquire) {
            if let Some(metrics) = self.metrics.get() {
                metrics.task_rejected.inc();
            }
            return Err(RuntimeError::ResourceStopped(self.name.clone()));
        }
        let sequence = self.sequence.fetch_add(1, AtomicOrdering::Relaxed);
        let pool = Arc::downgrade(self);
        let cancellation_registration = context.register_cancellation_callback(move || {
            if let Some(pool) = pool.upgrade() {
                pool.expire_task(sequence);
            }
        });
        let mut queue = self
            .queue
            .lock()
            .expect("priority pool queue lock poisoned");
        if self.stopped.load(AtomicOrdering::Acquire) {
            if let Some(metrics) = self.metrics.get() {
                metrics.task_rejected.inc();
            }
            return Err(RuntimeError::ResourceStopped(self.name.clone()));
        }
        queue.push(PriorityTask {
            priority,
            sequence,
            context: context.clone(),
            task,
            _cancellation_registration: cancellation_registration,
        });
        if let Some(metrics) = self.metrics.get() {
            metrics.queue_length.set(queue.len() as i64);
        }
        drop(queue);
        self.notify.notify_one();
        if context.is_cancelled() {
            self.expire_task(sequence);
        }
        Ok(())
    }

    pub async fn stop(self: &Arc<Self>) {
        self.stop_with_context(MessageContext::new()).await;
    }

    pub async fn stop_with_context(self: &Arc<Self>, context: MessageContext) {
        self.stopped.store(true, AtomicOrdering::Release);
        if self
            .started
            .compare_exchange(false, true, AtomicOrdering::AcqRel, AtomicOrdering::Acquire)
            .is_ok()
        {
            let executors = self.executors_target.load(AtomicOrdering::Acquire);
            self.spawn_workers(self.generation.load(AtomicOrdering::Acquire), executors);
        }
        self.notify.notify_waiters();
        let workers = {
            let mut workers = self
                .workers
                .lock()
                .expect("priority pool workers lock poisoned");
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
            let tasks_count = self
                .queue
                .lock()
                .expect("priority pool queue lock poisoned")
                .len();
            if let Some(metrics) = self.metrics.get() {
                metrics.stop_timeout.inc();
            }
            tracing::warn!(
                pool = self.name,
                tasks_count,
                "priority task pool stopped by timeout"
            );
            // The deadline reports a slow drain; workers may retain graph
            // observers and therefore still have to finish before teardown.
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
        struct AllocatedGuard<'a>(&'a PriorityTaskPool);
        impl Drop for AllocatedGuard<'_> {
            fn drop(&mut self) {
                if let Some(metrics) = self.0.metrics.get() {
                    metrics.executors_allocated.dec();
                }
            }
        }
        let _allocated = AllocatedGuard(&self);
        loop {
            if generation != self.generation.load(AtomicOrdering::Acquire)
                && !self.stopped.load(AtomicOrdering::Acquire)
            {
                return;
            }
            let task = {
                let mut queue = self
                    .queue
                    .lock()
                    .expect("priority pool queue lock poisoned");
                let task = queue.pop();
                if let Some(metrics) = self.metrics.get() {
                    metrics.queue_length.set(queue.len() as i64);
                }
                task
            };
            match task {
                Some(task) => {
                    let _context = task.context;
                    let started_at = self
                        .metrics
                        .get()
                        .is_some_and(|metrics| metrics.execution_duration.is_enabled())
                        .then(Instant::now);
                    if let Some(metrics) = self.metrics.get() {
                        metrics.executors_busy.inc();
                    }
                    super::run_task(&self.name, task.task).await;
                    if let Some(metrics) = self.metrics.get() {
                        metrics.executors_busy.dec();
                        metrics.tasks_total.inc();
                        if let Some(started_at) = started_at {
                            metrics
                                .execution_duration
                                .observe(started_at.elapsed().as_secs_f64());
                        }
                    }
                }
                None if self.stopped.load(AtomicOrdering::Acquire) => return,
                None => self.notify.notified().await,
            }
        }
    }
}
