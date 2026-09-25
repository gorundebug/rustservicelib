use std::{sync::Arc, time::Duration};

use servicelib::{
    MessageContext, Payload,
    operators::{
        flatmapiterable::FlatMapIterableStream,
        keyby::{KeyByFunction, KeyByStream},
    },
    runtime::{
        collector::Collect,
        common::{Consumer, RuntimeStream},
        config::{
            CallSemantics, FlatMapIterableStreamConfig, KeyByStreamConfig, LinkConfig,
            MapStreamConfig, PoolConfig, RuntimeConfig, RuntimeStreamConfig, StreamConfig,
        },
        datastruct::KeyValue,
        environment::RuntimeEnvironment,
        pool::{PriorityTaskPool, TaskPool},
        serde::make_stream_key_value_serde,
        stream::Stream,
    },
};
use tokio::sync::mpsc;

struct Key;

impl KeyByFunction<u32, String, u32> for Key {
    async fn key_by(
        &self,
        context: MessageContext,
        _stream: &dyn RuntimeStream,
        value: &u32,
        out: &impl Collect<KeyValue<String, u32>>,
    ) {
        out.collect(
            context,
            KeyValue {
                key: value.to_string(),
                value: *value,
            },
        )
        .await;
    }
}

struct Capture(mpsc::UnboundedSender<(MessageContext, Payload<KeyValue<String, u32>>)>);

impl Consumer<KeyValue<String, u32>> for Capture {
    async fn consume(&self, context: MessageContext, value: Payload<KeyValue<String, u32>>) {
        self.0.send((context, value)).unwrap();
    }
}

fn config(id: i32, source: i32) -> StreamConfig {
    let mut config = StreamConfig::new(id, format!("Node{id}"));
    config.id_service = 1;
    config.id_source = source;
    config
}

#[tokio::test]
async fn typed_keyby_and_iterable_keep_live_streams_context_and_serde_in_every_call_mode() {
    tokio::time::timeout(Duration::from_secs(10), async {
        for mode in 0..5 {
            let root_config = MapStreamConfig::from(config(1, 0));
            let iterable_config = FlatMapIterableStreamConfig::from(config(2, 1));
            let key_config = KeyByStreamConfig::from(config(3, 2));
            let capture_config = MapStreamConfig::from(config(4, 3));
            let semantics = match mode {
                0 | 1 => CallSemantics::FunctionCall,
                2 => CallSemantics::ParallelCall,
                3 => CallSemantics::TaskPool {
                    pool_name: "worker".into(),
                },
                _ => CallSemantics::PriorityTaskPool {
                    pool_name: "worker".into(),
                    priority: 7,
                },
            };
            let environment = RuntimeEnvironment::default();
            environment.publish_runtime_config(Arc::new(
                RuntimeConfig::from_parts(
                    CallSemantics::FunctionCall,
                    [],
                    [
                        RuntimeStreamConfig::from(root_config.clone()),
                        RuntimeStreamConfig::from(iterable_config.clone()),
                        RuntimeStreamConfig::from(key_config.clone()),
                        RuntimeStreamConfig::from(capture_config),
                    ],
                    [PoolConfig {
                        name: "worker".into(),
                        executors_count: 1,
                        queue_capacity: 0,
                    }],
                    [],
                    [],
                    (1..4).map(|from| LinkConfig {
                        from,
                        to: from + 1,
                        call_semantics: semantics.clone(),
                        r#async: mode == 1,
                    }),
                )
                .unwrap(),
            ));
            let fifo = if mode == 3 {
                let pool = TaskPool::new("worker", environment.clone()).unwrap();
                environment.register_task_pool(pool.clone()).unwrap();
                pool.start().unwrap();
                Some(pool)
            } else {
                None
            };
            let priority = if mode == 4 {
                let pool = PriorityTaskPool::new("worker", environment.clone()).unwrap();
                environment
                    .register_priority_task_pool(pool.clone())
                    .unwrap();
                pool.start().unwrap();
                Some(pool)
            } else {
                None
            };

            let root = Stream::<Vec<u32>>::new(&root_config.stream, environment.clone());
            let items = Stream::<u32>::new(&iterable_config.stream, environment.clone());
            let key_serde = make_stream_key_value_serde::<String, u32>(
                environment.make_serde::<String>(),
                environment.make_serde::<u32>(),
            );
            let keyed = Stream::derived(&key_config.stream, environment.clone(), key_serde.clone());
            let (sender, mut receiver) = mpsc::unbounded_channel();
            let output = keyed
                .try_set_typed_consumer(Arc::new(Capture(sender)), 4)
                .unwrap();
            let output = items
                .try_set_typed_consumer(Arc::new(KeyByStream::from_collector(output, Key)), 3)
                .unwrap();
            let input = root
                .try_set_typed_consumer(
                    Arc::new(FlatMapIterableStream::<Vec<u32>, u32>::from_collector(
                        output,
                    )),
                    2,
                )
                .unwrap();
            environment.build_runtime_streams().unwrap();

            assert!(Arc::ptr_eq(
                &items.get_serde(),
                &environment.make_serde::<u32>()
            ));
            assert!(Arc::ptr_eq(&keyed.get_serde(), &key_serde));
            let context =
                MessageContext::with_timeout(Duration::from_secs(5)).with_stream_id("typed-key");
            let deadline = context.deadline();
            input.collect(context.clone(), vec![1, 2]).await;
            root.emit(context.clone(), Payload::new(vec![3, 4])).await;
            items.emit(context.clone(), Payload::new(5)).await;
            keyed
                .emit(
                    context.clone(),
                    Payload::new(KeyValue {
                        key: "6".into(),
                        value: 6,
                    }),
                )
                .await;
            input.collect(context, Vec::new()).await;

            let mut values = Vec::new();
            for _ in 0..6 {
                let (context, value) = receiver.recv().await.unwrap();
                assert_eq!(context.stream_id(), Some("typed-key"));
                assert_eq!(context.deadline(), deadline);
                assert_eq!(value.key, value.value.to_string());
                values.push(value.value);
            }
            values.sort_unstable();
            assert_eq!(values, vec![1, 2, 3, 4, 5, 6]);
            if let Some(pool) = fifo {
                pool.stop().await;
            }
            if let Some(pool) = priority {
                pool.stop().await;
            }
            environment.delay_pool().stop().await;
            assert!(receiver.try_recv().is_err());
        }
    })
    .await
    .expect("typed operators must complete in all scheduling modes");
}
