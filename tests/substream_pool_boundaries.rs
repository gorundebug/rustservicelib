use std::{
    sync::{Arc, atomic::{AtomicUsize, Ordering}},
    time::Duration,
};

use async_trait::async_trait;
use servicelib::{
    MessageContext, Payload, SubStream, SubStreamCollectorFunc,
    operators::MapFunction,
    runtime::{
        collector::Collector,
        common::RuntimeStream,
        config::{CallSemantics, MapStreamConfig, PoolConfig, RuntimeConfig, RuntimeStreamConfig, StreamConfig, SubStreamConfig},
        environment::{RuntimeEnvironment, RuntimeError},
        pool::{BoxTask, DelayPool, PriorityTaskPool, TaskPool},
        stream::Stream,
    },
};
use tokio::sync::{mpsc, oneshot};
use tokio_util::sync::CancellationToken;

#[derive(Clone)]
enum Scheduler {
    Fifo(Arc<TaskPool>),
    Priority(Arc<PriorityTaskPool>),
    Delay(Arc<DelayPool>),
}

impl Scheduler {
    async fn add(&self, context: MessageContext, task: BoxTask) {
        match self {
            Self::Fifo(pool) => pool.add_task(context, task).await.unwrap(),
            Self::Priority(pool) => pool.add_task(context, 0, task).await.unwrap(),
            Self::Delay(pool) => pool.delay(context, Duration::from_millis(1), task).await.unwrap(),
        }
    }

    async fn stop(&self) {
        match self {
            Self::Fifo(pool) => pool.stop().await,
            Self::Priority(pool) => pool.stop().await,
            Self::Delay(pool) => pool.stop().await,
        }
    }
}

struct Deferred(mpsc::UnboundedSender<MessageContext>);

#[async_trait]
impl MapFunction<i32, i32> for Deferred {
    async fn map(&self, context: MessageContext, _: &dyn RuntimeStream, _: &i32, _: &Collector<i32>) {
        self.0.send(context).unwrap();
    }
}

fn graph(kind: usize) -> (Scheduler, SubStream<i32, i32>, Stream<i32>, mpsc::UnboundedReceiver<MessageContext>) {
    let mut entry_config = StreamConfig::new(1, "Entry");
    entry_config.id_service = 1;
    entry_config.id_source = 2;
    let mut result_config = StreamConfig::new(2, "Result");
    result_config.id_service = 1;
    result_config.id_source = 1;
    let entry_config = SubStreamConfig::from(entry_config);
    let result_config = MapStreamConfig::from(result_config);
    let environment = RuntimeEnvironment::default();
    environment.publish_runtime_config(Arc::new(RuntimeConfig::from_parts(
        CallSemantics::FunctionCall,
        [],
        [RuntimeStreamConfig::from(entry_config.clone()), RuntimeStreamConfig::from(result_config.clone())],
        [PoolConfig { name: "worker".into(), executors_count: 1, queue_capacity: 0 }],
        [], [], [],
    ).unwrap()));
    let scheduler = match kind {
        0 => {
            let pool = TaskPool::new("worker", environment.clone()).unwrap();
            pool.start().unwrap();
            Scheduler::Fifo(pool)
        }
        1 => {
            let pool = PriorityTaskPool::new("worker", environment.clone()).unwrap();
            pool.start().unwrap();
            Scheduler::Priority(pool)
        }
        _ => Scheduler::Delay(DelayPool::new()),
    };
    let (sender, receiver) = mpsc::unbounded_channel();
    let entry = SubStream::new(&entry_config, environment.clone());
    let output = entry.stream().map(&result_config, Deferred(sender)).unwrap();
    entry.set_source(&output).unwrap();
    environment.build_runtime_streams().unwrap();
    (scheduler, entry, output, receiver)
}

#[tokio::test]
async fn substream_wait_preserves_pool_slots_but_not_executor_threads() {
    tokio::time::timeout(Duration::from_secs(10), async {
        for kind in 0..3 {
            for same_pool in [false, true] {
                let (scheduler, entry, output, mut pending) = graph(kind);
                let independent = Scheduler::Delay(DelayPool::new());
                let context = MessageContext::new().with_stream_id("parent");
                let collected = Arc::new(AtomicUsize::new(0));
                let collector = Arc::new(SubStreamCollectorFunc({
                    let collected = collected.clone();
                    move |context: MessageContext, value: Payload<i32>| {
                        assert_eq!(context.stream_id(), Some("parent"));
                        assert_eq!(*value, 42);
                        collected.fetch_add(1, Ordering::SeqCst);
                        async { true }
                    }
                }));
                let retained = Arc::downgrade(&collector);
                let (finished, completion) = oneshot::channel();
                let caller_context = context.clone();
                scheduler.add(context.clone(), Box::pin(async move {
                    let result = entry.consume(caller_context, 1, collector).await;
                    finished.send(result).unwrap();
                })).await;
                let dispatch_context = pending.recv().await.unwrap();
                let response_started = CancellationToken::new();
                let response_finished = CancellationToken::new();
                let started = response_started.clone();
                let finished = response_finished.clone();
                let destination = if same_pool { &scheduler } else { &independent };
                destination.add(context.clone(), Box::pin(async move {
                    started.cancel();
                    output.emit(dispatch_context, Payload::new(42)).await;
                    finished.cancel();
                })).await;

                let occupied_slot = same_pool && kind != 2;
                if occupied_slot {
                    assert!(tokio::time::timeout(Duration::from_millis(20), response_started.cancelled()).await.is_err(),
                        "kind {kind}: a waiting callback must retain its configured pool slot");
                    context.cancel();
                }
                let result = completion.await.unwrap();
                if occupied_slot {
                    assert!(matches!(result, Err(RuntimeError::ContextCancelled)));
                } else {
                    result.unwrap();
                }
                response_finished.cancelled().await;
                scheduler.stop().await;
                independent.stop().await;
                assert_eq!(collected.load(Ordering::SeqCst), usize::from(!occupied_slot));
                assert!(retained.upgrade().is_none());
            }
        }
    }).await.expect("pool callbacks must leave the Tokio worker available; cancellation must drain admitted work");
}
