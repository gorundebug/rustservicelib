use std::{
    any::TypeId,
    sync::{Arc, atomic::{AtomicUsize, Ordering}},
    time::Duration,
};

use async_trait::async_trait;
use servicelib::{
    MessageContext, Payload,
    operators::{DelayFunction, FilterFunction},
    runtime::{
        common::{Consumer, RuntimeStream},
        config::{CallSemantics, DelayStreamConfig, FilterStreamConfig, LinkConfig, MapStreamConfig, MergeStreamConfig, PoolConfig, RuntimeConfig, RuntimeStreamConfig, SplitStreamConfig, StreamConfig},
        environment::{RuntimeEnvironment, RuntimeResult},
        pool::{PriorityTaskPool, TaskPool},
        serde::{Serde, SerdeError, Serializer},
        stream::Stream,
    },
};
use tokio::sync::mpsc;

// Deliberately no Clone, Serialize or Deserialize implementations.
struct LargeValue {
    bytes: Box<[u8]>,
    dropped: Arc<AtomicUsize>,
}

impl Drop for LargeValue {
    fn drop(&mut self) {
        self.dropped.fetch_add(1, Ordering::SeqCst);
    }
}

static CODEC_CALLS: AtomicUsize = AtomicUsize::new(0);

struct RejectCodec;

impl Serde<LargeValue> for RejectCodec {
    fn serialize(&self, _: &LargeValue) -> Result<Vec<u8>, SerdeError> {
        CODEC_CALLS.fetch_add(1, Ordering::SeqCst);
        Err(SerdeError::new("local graph must not serialize", 0))
    }

    fn deserialize(&self, _: &[u8]) -> Result<LargeValue, SerdeError> {
        CODEC_CALLS.fetch_add(1, Ordering::SeqCst);
        Err(SerdeError::new("local graph must not deserialize", 0))
    }
}

fn provider(id: TypeId, _: &RuntimeEnvironment) -> RuntimeResult<Option<Serializer>> {
    Ok((id == TypeId::of::<LargeValue>())
        .then(|| Serializer::new::<LargeValue>(Arc::new(RejectCodec))))
}

struct Keep;

impl FilterFunction<LargeValue> for Keep {
    async fn filter(&self, context: MessageContext, _: &dyn RuntimeStream, value: &LargeValue) -> bool {
        assert_eq!(context.stream_id(), Some("parent"));
        assert_eq!(value.bytes.len(), 1024 * 1024);
        true
    }
}

struct Wait;

impl DelayFunction<LargeValue> for Wait {
    async fn duration(&self, _: MessageContext, _: &dyn RuntimeStream, _: &LargeValue) -> Duration {
        Duration::from_millis(1)
    }
}

struct Observe(mpsc::UnboundedSender<(MessageContext, Payload<LargeValue>)>);

#[async_trait]
impl Consumer<LargeValue> for Observe {
    async fn consume(&self, context: MessageContext, payload: Payload<LargeValue>) {
        self.0.send((context, payload)).unwrap_or_else(|_| panic!("observer closed"));
    }
}

fn config(id: i32, source: i32) -> StreamConfig {
    let mut config = StreamConfig::new(id, format!("Node{id}"));
    config.id_service = 1;
    config.id_source = source;
    config
}

#[tokio::test]
async fn local_links_preserve_large_payload_and_do_not_invoke_serde() {
    tokio::time::timeout(Duration::from_secs(10), async {
        for custom_serde in [false, true] {
            for mode in 0..5 {
                let root_config = MapStreamConfig::from(config(1, 0));
                let split_config = SplitStreamConfig::from(config(2, 1));
                let left_config = FilterStreamConfig::from(config(3, 2));
                let right_config = FilterStreamConfig::from(config(4, 2));
                let mut merge_stream = config(5, 0);
                merge_stream.id_sources = vec![3, 4];
                let merge_config = MergeStreamConfig::from(merge_stream);
                let delay_config = DelayStreamConfig::from(config(6, 5));
                let observer_config = MapStreamConfig::from(config(7, 6));
                let semantics = match mode {
                    0 | 1 => CallSemantics::FunctionCall,
                    2 => CallSemantics::ParallelCall,
                    3 => CallSemantics::TaskPool { pool_name: "worker".into() },
                    _ => CallSemantics::PriorityTaskPool { pool_name: "worker".into(), priority: 7 },
                };
                let environment = RuntimeEnvironment::default();
                if custom_serde {
                    environment.set_serde_provider(provider).unwrap();
                }
                environment.publish_runtime_config(Arc::new(RuntimeConfig::from_parts(
                    CallSemantics::FunctionCall, [],
                    [
                        RuntimeStreamConfig::from(root_config.clone()),
                        RuntimeStreamConfig::from(split_config.clone()),
                        RuntimeStreamConfig::from(left_config.clone()),
                        RuntimeStreamConfig::from(right_config.clone()),
                        RuntimeStreamConfig::from(merge_config.clone()),
                        RuntimeStreamConfig::from(delay_config.clone()),
                        RuntimeStreamConfig::from(observer_config),
                    ],
                    [PoolConfig { name: "worker".into(), executors_count: 1, queue_capacity: 0 }],
                    [], [],
                    [3, 4].map(|to| LinkConfig { from: 2, to, call_semantics: semantics.clone(), r#async: mode == 1 }),
                ).unwrap()));
                let fifo = if mode == 3 {
                    let pool = TaskPool::new("worker", environment.clone()).unwrap();
                    environment.register_task_pool(pool.clone()).unwrap();
                    pool.start().unwrap();
                    Some(pool)
                } else { None };
                let priority = if mode == 4 {
                    let pool = PriorityTaskPool::new("worker", environment.clone()).unwrap();
                    environment.register_priority_task_pool(pool.clone()).unwrap();
                    pool.start().unwrap();
                    Some(pool)
                } else { None };
                let root = Stream::<LargeValue>::new(&root_config.stream, environment.clone());
                let [left_link, right_link] = root.split(&split_config).unwrap();
                let left = left_link.filter(&left_config, Keep).unwrap();
                let right = right_link.filter(&right_config, Keep).unwrap();
                let merged = left.merge(&merge_config, std::slice::from_ref(&right)).unwrap();
                let delayed = merged.delay(&delay_config, Wait).unwrap();
                let (sender, mut receiver) = mpsc::unbounded_channel();
                delayed.set_consumer(Arc::new(Observe(sender)), 7);
                environment.build_runtime_streams().unwrap();
                let serde = root.get_serde();
                assert_eq!(serde.is_stub(), !custom_serde);
                for stream in [&left_link, &right_link, &left, &right, &merged, &delayed] {
                    assert!(Arc::ptr_eq(&serde, &stream.get_serde()));
                }
                let dropped = Arc::new(AtomicUsize::new(0));
                let value = LargeValue { bytes: vec![0xa5; 1024 * 1024].into_boxed_slice(), dropped: dropped.clone() };
                let allocation = value.bytes.as_ptr() as usize;
                let context = MessageContext::with_timeout(Duration::from_secs(5)).with_stream_id("parent");
                let deadline = context.deadline();
                root.emit(context, Payload::new(value)).await;
                let mut results = Vec::new();
                for _ in 0..2 {
                    let (context, payload) = receiver.recv().await.unwrap();
                    assert_eq!(context.stream_id(), Some("parent"));
                    assert_eq!(context.deadline(), deadline);
                    assert_eq!(payload.bytes.as_ptr() as usize, allocation);
                    results.push(payload);
                }
                if let Some(pool) = fifo { pool.stop().await; }
                if let Some(pool) = priority { pool.stop().await; }
                environment.delay_pool().stop().await;
                assert_eq!(CODEC_CALLS.load(Ordering::SeqCst), 0);
                assert_eq!(dropped.load(Ordering::SeqCst), 0);
                results.pop();
                assert_eq!(dropped.load(Ordering::SeqCst), 0);
                results.clear();
                assert_eq!(dropped.load(Ordering::SeqCst), 1);
            }
        }
    }).await.expect("all local calling modes must deliver without requiring a wire codec");
}
