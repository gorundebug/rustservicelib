use super::BoxTask;
use crate::runtime::{
    common::MessageContext,
    environment::{
        RuntimeEnvironment, RuntimeError, RuntimeResult,
        metrics::{Float64Histogram, Int64Counter, Int64Gauge, Labels},
    },
};
use futures::{
    FutureExt, StreamExt,
    future::{AbortHandle, Abortable, BoxFuture},
    stream::FuturesUnordered,
};
use std::{
    collections::{BTreeMap, HashMap, VecDeque},
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU8, Ordering},
    },
    time::Instant,
};
use tokio::{
    sync::{Mutex, mpsc, oneshot, watch},
    task::JoinSet,
};

#[derive(Clone)]
struct PoolMetrics {
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

impl PoolMetrics {
    fn new(name: &str, priority: bool, environment: &RuntimeEnvironment) -> RuntimeResult<Self> {
        let kind = if priority {
            "priority_task_pool"
        } else {
            "task_pool"
        };
        let scope = environment.metrics().scope(
            kind,
            [
                ("service".to_owned(), environment.service_name()),
                ("name".to_owned(), name.to_owned()),
            ]
            .into_iter()
            .collect(),
        );
        let metrics = PoolMetrics {
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
                [(
                    "event".to_owned(),
                    if priority {
                        "task_expired"
                    } else {
                        "task_cancelled"
                    }
                    .to_owned(),
                )]
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
        Ok(metrics)
    }
}

enum Command {
    Add(
        MessageContext,
        i32,
        BoxTask,
        oneshot::Sender<RuntimeResult<()>>,
    ),
    Start(usize),
    Resize(usize),
    Stop,
    Ready(u64),
}

struct Entry {
    task: BoxTask,
    cancellation: AbortHandle,
}

type Key = (i32, u64);
type Cancellation = Abortable<BoxFuture<'static, u64>>;

struct FifoEntry {
    entry: Entry,
    previous: Option<u64>,
    next: Option<u64>,
}

// FIFO has no priority tree: links are indexed by task id so cancellation can
// unlink and promote a queued task without scanning. Priority uses an ordered
// tree plus an id index for O(log n) extraction and promotion.
enum PendingQueue {
    Fifo {
        entries: HashMap<u64, FifoEntry>,
        head: Option<u64>,
        tail: Option<u64>,
    },
    Priority {
        entries: BTreeMap<Key, Entry>,
        positions: HashMap<u64, Key>,
    },
}
impl PendingQueue {
    fn new(priority: bool) -> Self {
        if priority {
            Self::Priority {
                entries: BTreeMap::new(),
                positions: HashMap::new(),
            }
        } else {
            Self::Fifo {
                entries: HashMap::new(),
                head: None,
                tail: None,
            }
        }
    }
    fn len(&self) -> usize {
        match self {
            Self::Fifo { entries, .. } => entries.len(),
            Self::Priority { entries, .. } => entries.len(),
        }
    }
    fn is_empty(&self) -> bool {
        self.len() == 0
    }
    fn push(&mut self, id: u64, priority: i32, entry: Entry) {
        match self {
            Self::Priority { entries, positions } => {
                let key = (priority, id);
                entries.insert(key, entry);
                positions.insert(id, key);
            }
            Self::Fifo {
                entries,
                head,
                tail,
            } => {
                if let Some(previous) = *tail {
                    entries.get_mut(&previous).unwrap().next = Some(id);
                } else {
                    *head = Some(id);
                }
                entries.insert(
                    id,
                    FifoEntry {
                        entry,
                        previous: *tail,
                        next: None,
                    },
                );
                *tail = Some(id);
            }
        }
    }
    fn pop(&mut self) -> Option<(u64, Entry)> {
        match self {
            Self::Priority { entries, positions } => {
                let ((_, id), entry) = entries.pop_first()?;
                positions.remove(&id);
                Some((id, entry))
            }
            Self::Fifo {
                entries,
                head,
                tail,
            } => {
                let id = (*head)?;
                let node = entries.remove(&id).unwrap();
                *head = node.next;
                if let Some(next) = node.next {
                    entries.get_mut(&next).unwrap().previous = None;
                } else {
                    *tail = None;
                }
                Some((id, node.entry))
            }
        }
    }
    fn promote(&mut self, id: u64) -> bool {
        match self {
            Self::Priority { entries, positions } => {
                let Some(key) = positions.get_mut(&id) else {
                    return false;
                };
                let entry = entries.remove(key).unwrap();
                key.0 = i32::MIN;
                entries.insert(*key, entry);
                true
            }
            Self::Fifo {
                entries,
                head,
                tail,
            } => {
                if *head == Some(id) {
                    return false;
                }
                let Some(node) = entries.get(&id) else {
                    return false;
                };
                let (previous, next) = (node.previous, node.next);
                entries.get_mut(&previous.unwrap()).unwrap().next = next;
                if let Some(next) = next {
                    entries.get_mut(&next).unwrap().previous = previous;
                } else {
                    *tail = previous;
                }
                entries.get_mut(&head.unwrap()).unwrap().previous = Some(id);
                let node = entries.get_mut(&id).unwrap();
                node.previous = None;
                node.next = *head;
                *head = Some(id);
                true
            }
        }
    }
}

// The actor owns all queue and lifecycle data. No synchronous cancellation
// callback acquires a lock, and user code only runs in executor tasks.
pub(super) struct QueuedPool {
    name: String,
    priority: bool,
    environment: RuntimeEnvironment,
    fallback_executors: usize,
    state: AtomicU8, // 0: created, 1: started, 2: stopped
    launched: AtomicBool,
    sender: mpsc::UnboundedSender<Command>,
    receiver: Mutex<Option<mpsc::UnboundedReceiver<Command>>>,
    drained: watch::Sender<bool>,
    metrics: PoolMetrics,
}

impl QueuedPool {
    pub(super) fn new(
        name: String,
        priority: bool,
        environment: RuntimeEnvironment,
    ) -> RuntimeResult<Arc<Self>> {
        let fallback_executors = environment
            .runtime_config()
            .pool_by_name(&name)
            .map(|config| config.executors_count)
            .ok_or_else(|| {
                if priority {
                    RuntimeError::PriorityTaskPoolNotFound(name.clone())
                } else {
                    RuntimeError::TaskPoolNotFound(name.clone())
                }
            })?;
        let metrics = PoolMetrics::new(&name, priority, &environment)?;
        metrics
            .executors_target
            .set(Self::resolve_executors(fallback_executors) as i64);
        let (sender, receiver) = mpsc::unbounded_channel();
        let (drained, _) = watch::channel(false);
        Ok(Arc::new(Self {
            name,
            priority,
            environment,
            fallback_executors,
            state: AtomicU8::new(0),
            launched: AtomicBool::new(false),
            sender,
            receiver: Mutex::new(Some(receiver)),
            drained,
            metrics,
        }))
    }

    fn resolve_executors(count: usize) -> usize {
        if count == 0 {
            std::thread::available_parallelism().map_or(1, usize::from)
        } else {
            count
        }
    }

    fn executors(&self) -> usize {
        Self::resolve_executors(
            self.environment
                .runtime_config()
                .pool_by_name(&self.name)
                .map_or(self.fallback_executors, |config| config.executors_count),
        )
    }

    fn launch(self: &Arc<Self>) {
        if !self.launched.swap(true, Ordering::AcqRel) {
            let pool = Arc::clone(self);
            tokio::spawn(async move {
                let receiver = pool
                    .receiver
                    .lock()
                    .await
                    .take()
                    .expect("single queue actor");
                pool.run(receiver).await;
                pool.drained.send_replace(true);
            });
        }
    }

    pub(super) fn name(&self) -> &str {
        &self.name
    }

    pub(super) fn start(self: &Arc<Self>) -> RuntimeResult<()> {
        self.state
            .compare_exchange(0, 1, Ordering::AcqRel, Ordering::Acquire)
            .map_err(|state| {
                if state == 2 {
                    RuntimeError::ResourceStopped(self.name.clone())
                } else {
                    RuntimeError::ResourceAlreadyStarted(self.name.clone())
                }
            })?;
        self.launch();
        self.sender
            .send(Command::Start(self.executors()))
            .map_err(|_| RuntimeError::ResourceStopped(self.name.clone()))
    }

    pub(super) fn reload_config(&self) {
        let _ = self.sender.send(Command::Resize(self.executors()));
    }

    pub(super) async fn add_task(
        self: &Arc<Self>,
        context: MessageContext,
        priority: i32,
        task: BoxTask,
    ) -> RuntimeResult<()> {
        if context.is_cancelled() {
            self.metrics.task_rejected.inc();
            return Err(RuntimeError::ContextCancelled);
        }
        if self.state.load(Ordering::Acquire) == 2 {
            self.metrics.task_rejected.inc();
            return Err(RuntimeError::ResourceStopped(self.name.clone()));
        }
        self.launch();
        let (tx, rx) = oneshot::channel();
        self.sender
            .send(Command::Add(context, priority, task, tx))
            .map_err(|_| RuntimeError::ResourceStopped(self.name.clone()))?;
        rx.await
            .unwrap_or_else(|_| Err(RuntimeError::ResourceStopped(self.name.clone())))
    }

    pub(super) async fn stop(self: &Arc<Self>, context: MessageContext) {
        // Every caller subscribes to the same completion, including callers
        // arriving after another stop future has been cancelled by its owner.
        let mut drained = self.drained.subscribe();
        self.launch();
        if self.state.swap(2, Ordering::AcqRel) != 2 {
            let _ = self.sender.send(Command::Stop);
        }
        let wait = async move {
            while !*drained.borrow_and_update() {
                if drained.changed().await.is_err() {
                    break;
                }
            }
        };
        tokio::pin!(wait);
        tokio::select! {
            _ = &mut wait => {},
            _ = context.cancelled() => {
                self.metrics.stop_timeout.inc();
                tracing::warn!(pool = self.name, "task pool stopped by timeout");
                wait.await;
            }
        }
    }

    async fn run(&self, mut receiver: mpsc::UnboundedReceiver<Command>) {
        let mut queue = PendingQueue::new(self.priority);
        let mut cancellations = FuturesUnordered::<Cancellation>::new();
        let mut workers = JoinSet::new();
        let mut executors = HashMap::<u64, mpsc::UnboundedSender<BoxTask>>::new();
        let mut idle = VecDeque::new();
        let mut next_worker = 0u64;
        let mut busy = 0usize;
        let mut retiring = false;
        let mut next_id = 0u64;
        let mut target = self.executors();
        let mut started = false;
        let mut stopped = false;
        let mut receiving = true;
        loop {
            // Poll cancellation before starting the next queued callback.
            tokio::select! {
                biased;
                Some(Ok(id)) = cancellations.next(), if !cancellations.is_empty() => {
                    if queue.promote(id) { self.metrics.task_cancelled.inc(); }
                }
                command = receiver.recv(), if receiving => match command {
                    Some(Command::Add(context, priority, task, reply)) => {
                        if stopped {
                            self.metrics.task_rejected.inc();
                            let _ = reply.send(Err(RuntimeError::ResourceStopped(self.name.clone())));
                        } else if context.is_cancelled() {
                            self.metrics.task_rejected.inc();
                            let _ = reply.send(Err(RuntimeError::ContextCancelled));
                        } else {
                            let id = next_id; next_id += 1;
                            let (cancel, registration) = AbortHandle::new_pair();
                            cancellations.push(Abortable::new(async move { context.cancelled().await; id }.boxed(), registration));
                            queue.push(id, priority, Entry { task, cancellation: cancel });
                            self.metrics.queue_length.set(queue.len() as i64);
                            let _ = reply.send(Ok(()));
                        }
                    }
                    Some(Command::Start(count)) if !stopped => { started = true; target = count; }
                    Some(Command::Resize(count)) if !stopped => { target = count; }
                    Some(Command::Stop) => { stopped = true; started = true; }
                    None => { receiving = false; stopped = true; started = true; }
                    Some(Command::Ready(id)) => { busy -= 1; idle.push_back(id); }
                    _ => {}
                },
                Some(_) = workers.join_next(), if !workers.is_empty() => {}
            }
            if started && !retiring {
                // Retire idle workers first; a busy worker keeps its slot
                // until its callback completes, including across await points.
                while executors.len() > target && !idle.is_empty() {
                    executors.remove(&idle.pop_front().unwrap());
                }
                while executors.len() < target {
                    let id = next_worker;
                    next_worker += 1;
                    let (tx, mut rx) = mpsc::unbounded_channel::<BoxTask>();
                    executors.insert(id, tx);
                    idle.push_back(id);
                    let name = self.name.clone();
                    let metrics = self.metrics.clone();
                    let completion = self.sender.clone();
                    workers.spawn(async move {
                        metrics.executors_allocated.inc();
                        while let Some(task) = rx.recv().await {
                            metrics.executors_busy.inc();
                            let started_at =
                                metrics.execution_duration.is_enabled().then(Instant::now);
                            super::run_task(&name, task).await;
                            metrics.executors_busy.dec();
                            metrics.tasks_total.inc();
                            if let Some(started_at) = started_at {
                                metrics
                                    .execution_duration
                                    .observe(started_at.elapsed().as_secs_f64());
                            }
                            if completion.send(Command::Ready(id)).is_err() {
                                break;
                            }
                        }
                        metrics.executors_allocated.dec();
                    });
                }
                while !idle.is_empty() && !queue.is_empty() {
                    let (_, entry) = queue.pop().unwrap();
                    entry.cancellation.abort();
                    self.metrics.queue_length.set(queue.len() as i64);
                    let worker = idle.pop_front().unwrap();
                    busy += 1;
                    executors[&worker]
                        .send(entry.task)
                        .unwrap_or_else(|_| unreachable!("live executor"));
                }
            }
            self.metrics.executors_target.set(target as i64);
            if stopped && queue.is_empty() && busy == 0 && !retiring {
                retiring = true;
                executors.clear();
                idle.clear();
            }
            if retiring && workers.is_empty() {
                break;
            }
        }
    }
}
