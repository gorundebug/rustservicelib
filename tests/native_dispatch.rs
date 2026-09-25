use std::{collections::HashSet, sync::Arc, time::Duration};

use servicelib::{
    MessageContext, Payload,
    runtime::{
        collector::{Collect, Collector},
        common::Consumer,
        config::{
            CallSemantics, LinkConfig, MapStreamConfig, PoolConfig, RuntimeConfig,
            RuntimeStreamConfig, StreamConfig,
        },
        environment::RuntimeEnvironment,
        pool::{PriorityTaskPool, TaskPool},
        stream::Stream,
    },
};
use tokio::{sync::mpsc, time::Instant};

type Received = (String, Option<Instant>, bool, Payload<Vec<u8>>);

struct Capture(mpsc::UnboundedSender<Received>);

impl Consumer<Vec<u8>> for Capture {
    async fn consume(&self, context: MessageContext, payload: Payload<Vec<u8>>) {
        tokio::task::yield_now().await;
        self.0
            .send((
                context.stream_id().unwrap().to_owned(),
                context.deadline(),
                context.is_cancelled(),
                payload,
            ))
            .unwrap();
    }
}

struct Forward<N: Collect<Vec<u8>> + 'static>(Collector<Vec<u8>, N>);

impl<N: Collect<Vec<u8>> + 'static> Consumer<Vec<u8>> for Forward<N> {
    async fn consume(&self, context: MessageContext, payload: Payload<Vec<u8>>) {
        self.0.emit(context, payload).await;
    }
}

fn setup(
    semantics: CallSemantics,
    asynchronous: bool,
) -> (RuntimeEnvironment, [Stream<Vec<u8>>; 3]) {
    let environment = RuntimeEnvironment::default();
    let configs = std::array::from_fn::<_, 3, _>(|index| {
        let mut config = StreamConfig::new(index as i32 + 1, format!("Node{index}"));
        config.id_source = index as i32;
        config
    });
    environment.publish_runtime_config(Arc::new(
        RuntimeConfig::from_parts(
            CallSemantics::FunctionCall,
            [],
            configs
                .iter()
                .cloned()
                .map(|config| RuntimeStreamConfig::from(MapStreamConfig::from(config))),
            [PoolConfig {
                name: "worker".into(),
                executors_count: 1,
                queue_capacity: 0,
            }],
            [],
            [],
            [LinkConfig {
                from: 1,
                to: 2,
                call_semantics: semantics,
                r#async: asynchronous,
            }],
        )
        .unwrap(),
    ));
    let streams = configs.map(|config| Stream::new(&config, environment.clone()));
    (environment, streams)
}

#[tokio::test]
async fn every_stream_handle_stays_connected_to_the_same_typed_route() {
    let (environment, [source, middle, _end]) = setup(CallSemantics::FunctionCall, false);
    let saved_source = source.clone();
    let saved_middle = middle.clone();
    let (sender, mut receiver) = mpsc::unbounded_channel();
    let consumer = Arc::new(Capture(sender));
    let weak = Arc::downgrade(&consumer);
    let typed_middle = middle.try_set_typed_consumer(consumer, 3).unwrap();
    let typed_source = source
        .try_set_typed_consumer(Arc::new(Forward(typed_middle.clone())), 2)
        .unwrap();
    environment.build_runtime_streams().unwrap();

    assert!(Arc::ptr_eq(
        &middle.get_serde(),
        &typed_middle.stream().get_serde()
    ));
    assert!(!typed_source.is_async());
    typed_source
        .out(MessageContext::new().with_stream_id("typed"), vec![1])
        .await;
    saved_source
        .emit(
            MessageContext::new().with_stream_id("root"),
            Payload::new(vec![2]),
        )
        .await;
    saved_middle
        .emit(
            MessageContext::new().with_stream_id("middle"),
            Payload::new(vec![3]),
        )
        .await;
    typed_middle
        .out(
            MessageContext::new().with_stream_id("typed-middle"),
            vec![4],
        )
        .await;
    for (id, value) in [
        ("typed", 1),
        ("root", 2),
        ("middle", 3),
        ("typed-middle", 4),
    ] {
        let (actual_id, _, _, actual_value) = receiver.recv().await.unwrap();
        assert_eq!(actual_id, id);
        assert_eq!(&*actual_value, &[value]);
    }
    assert!(
        source
            .try_set_typed_consumer(Arc::new(Capture(mpsc::unbounded_channel().0)), 2)
            .is_err()
    );

    drop((
        source,
        middle,
        saved_source,
        saved_middle,
        typed_source,
        typed_middle,
    ));
    assert!(
        weak.upgrade().is_none(),
        "typed and dynamic views must not form an ownership cycle"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn typed_and_dynamic_entries_preserve_context_and_payload_across_all_schedulers() {
    for mode in 0..5 {
        let semantics = match mode {
            0 | 1 => CallSemantics::FunctionCall,
            2 => CallSemantics::ParallelCall,
            3 => CallSemantics::TaskPool {
                pool_name: "worker".into(),
            },
            _ => CallSemantics::PriorityTaskPool {
                pool_name: "worker".into(),
                priority: 3,
            },
        };
        let (environment, [source, middle, _end]) = setup(semantics, mode == 1);
        let pool = if mode == 3 {
            let pool = TaskPool::new("worker", environment.clone()).unwrap();
            environment.register_task_pool(pool.clone()).unwrap();
            pool.start().unwrap();
            Some(pool)
        } else {
            None
        };
        let priority_pool = if mode == 4 {
            let pool = PriorityTaskPool::new("worker", environment.clone()).unwrap();
            environment
                .register_priority_task_pool(pool.clone())
                .unwrap();
            pool.start().unwrap();
            Some(pool)
        } else {
            None
        };
        let (sender, mut receiver) = mpsc::unbounded_channel();
        let typed_middle = middle
            .try_set_typed_consumer(Arc::new(Capture(sender)), 3)
            .unwrap();
        let typed = source
            .try_set_typed_consumer(Arc::new(Forward(typed_middle)), 2)
            .unwrap();
        assert_eq!(typed.is_async(), mode != 0);
        environment.build_runtime_streams().unwrap();
        let bytes = Arc::new(vec![7; 1024 * 1024]);
        let deadline = Instant::now() + Duration::from_secs(10);
        let mut tasks = Vec::new();
        for index in 0..32 {
            let typed = typed.clone();
            let source = source.clone();
            let bytes = bytes.clone();
            tasks.push(tokio::spawn(async move {
                let context =
                    MessageContext::with_deadline(deadline).with_stream_id(format!("call-{index}"));
                let payload = Payload::from_arc(bytes);
                if index % 2 == 0 {
                    typed.emit(context, payload).await;
                } else {
                    source.emit(context, payload).await;
                }
            }));
        }
        for task in tasks {
            task.await.unwrap();
        }
        let mut ids = HashSet::new();
        for _ in 0..32 {
            let (id, actual_deadline, cancelled, payload) =
                tokio::time::timeout(Duration::from_secs(5), receiver.recv())
                    .await
                    .unwrap()
                    .unwrap();
            assert!(ids.insert(id));
            assert_eq!(actual_deadline, Some(deadline));
            assert!(!cancelled);
            match payload {
                Payload::Shared(value) => assert!(Arc::ptr_eq(&bytes, &value)),
                Payload::Owned(_) => panic!("shared payload was copied"),
            }
        }
        if let Some(pool) = pool {
            pool.stop().await;
        }
        if let Some(pool) = priority_pool {
            pool.stop().await;
        }
    }
}
