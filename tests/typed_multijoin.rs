use std::{collections::BTreeSet, sync::Arc, time::Duration};

use servicelib::{
    MessageContext, Payload,
    operators::{MultiJoinFunction, MultiJoinStream, downcast_join_values},
    runtime::{
        collector::Collect,
        common::{Consumer, RuntimeStream},
        config::{
            CallSemantics, MapStreamConfig, MultiJoinStreamConfig, RuntimeConfig,
            RuntimeStreamConfig, StreamConfig,
        },
        datastruct::KeyValue,
        environment::RuntimeEnvironment,
        store::JoinValues,
        stream::Stream,
    },
};
use tokio::sync::mpsc;

struct Combine {
    partial: bool,
}

impl MultiJoinFunction<u32, (u32, u32)> for Combine {
    async fn multi_join(
        &self,
        context: MessageContext,
        _: &dyn RuntimeStream,
        key: u32,
        values: JoinValues,
        out: &impl Collect<(u32, u32)>,
    ) -> bool {
        let left = downcast_join_values::<u32>(&values, 0);
        let middle = downcast_join_values::<String>(&values, 1);
        let right = downcast_join_values::<bool>(&values, 2);
        if !self.partial && (left.is_empty() || middle.is_empty() || right.is_empty()) {
            return false;
        }
        let result = left.iter().sum::<u32>()
            + middle
                .iter()
                .map(|value| value.parse::<u32>().unwrap())
                .sum::<u32>()
            + right.iter().filter(|value| **value).count() as u32;
        out.collect(context, (key, result)).await;
        !self.partial
    }
}

struct Capture(mpsc::UnboundedSender<(MessageContext, (u32, u32))>);

impl Consumer<(u32, u32)> for Capture {
    async fn consume(&self, context: MessageContext, value: Payload<(u32, u32)>) {
        self.0.send((context, *value)).unwrap();
    }
}

fn environment(ttl: Duration) -> (RuntimeEnvironment, MultiJoinStreamConfig) {
    let mut config = MultiJoinStreamConfig {
        stream: StreamConfig::new(4, "multi"),
        join_storage: servicelib::api::JoinStorageType::HashMap,
        ttl,
        renew_ttl: false,
    };
    config.stream.id_source = 1;
    let mut result = StreamConfig::new(5, "result");
    result.id_source = 4;
    let environment = RuntimeEnvironment::default();
    environment.publish_runtime_config(Arc::new(
        RuntimeConfig::from_parts(
            CallSemantics::FunctionCall,
            [],
            [
                RuntimeStreamConfig::from(MapStreamConfig::from(StreamConfig::new(1, "left"))),
                RuntimeStreamConfig::from(MapStreamConfig::from(StreamConfig::new(2, "middle"))),
                RuntimeStreamConfig::from(MapStreamConfig::from(StreamConfig::new(3, "right"))),
                RuntimeStreamConfig::from(config.clone()),
                RuntimeStreamConfig::from(MapStreamConfig::from(result)),
            ],
            [],
            [],
            [],
            [],
        )
        .unwrap(),
    ));
    (environment, config)
}

#[tokio::test]
async fn typed_multijoin_preserves_slots_across_all_arrival_orders_and_concurrent_keys() {
    tokio::time::timeout(Duration::from_secs(5), async {
        let (environment, config) = environment(Duration::ZERO);
        let left =
            Stream::<KeyValue<u32, u32>>::new(&StreamConfig::new(1, "left"), environment.clone());
        let middle = Stream::<KeyValue<u32, String>>::new(
            &StreamConfig::new(2, "middle"),
            environment.clone(),
        );
        let right =
            Stream::<KeyValue<u32, bool>>::new(&StreamConfig::new(3, "right"), environment.clone());
        let multi =
            MultiJoinStream::new(&config, environment.clone(), Combine { partial: false }).unwrap();
        let (sender, mut receiver) = mpsc::unbounded_channel();
        let collector = multi
            .stream()
            .try_set_typed_consumer(Arc::new(Capture(sender)), 5)
            .unwrap();
        let a = multi.connect_left(&left, collector.clone()).unwrap();
        let b = multi
            .add_with_collector(&middle, collector.clone())
            .unwrap();
        let c = multi.add_with_collector(&right, collector).unwrap();
        environment.build_runtime_streams().unwrap();
        let mut tasks = Vec::new();
        let orders = [
            [0, 1, 2],
            [0, 2, 1],
            [1, 0, 2],
            [1, 2, 0],
            [2, 0, 1],
            [2, 1, 0],
        ];
        for key in 0..36_u32 {
            let a = a.clone();
            let b = b.clone();
            let c = c.clone();
            tasks.push(tokio::spawn(async move {
                let context = MessageContext::new().with_stream_id(format!("multi-{key}"));
                for slot in orders[key as usize % orders.len()] {
                    match slot {
                        0 => {
                            a.collect(context.clone(), KeyValue { key, value: 10 })
                                .await
                        }
                        1 => {
                            b.collect(
                                context.clone(),
                                KeyValue {
                                    key,
                                    value: "20".to_owned(),
                                },
                            )
                            .await
                        }
                        _ => {
                            c.collect(context.clone(), KeyValue { key, value: true })
                                .await
                        }
                    }
                }
            }));
        }
        for task in tasks {
            task.await.unwrap();
        }
        let mut keys = BTreeSet::new();
        for _ in 0..36 {
            let (context, (key, value)) = receiver.try_recv().unwrap();
            assert_eq!(context.stream_id(), Some(format!("multi-{key}").as_str()));
            assert_eq!(value, 31);
            assert!(keys.insert(key));
        }
        left.emit(
            MessageContext::new(),
            Payload::new(KeyValue { key: 99, value: 1 }),
        )
        .await;
        middle
            .emit(
                MessageContext::new(),
                Payload::new(KeyValue {
                    key: 99,
                    value: "2".to_owned(),
                }),
            )
            .await;
        right
            .emit(
                MessageContext::new(),
                Payload::new(KeyValue {
                    key: 99,
                    value: false,
                }),
            )
            .await;
        assert_eq!(receiver.try_recv().unwrap().1, (99, 3));
        multi
            .stream()
            .emit(MessageContext::new(), Payload::new((100, 7)))
            .await;
        assert_eq!(receiver.try_recv().unwrap().1, (100, 7));
        environment.delay_pool().stop().await;
    })
    .await
    .unwrap();
}

#[tokio::test(start_paused = true)]
async fn multijoin_ttl_keeps_the_typed_collector_and_original_context_alive() {
    let (environment, config) = environment(Duration::from_secs(10));
    let left =
        Stream::<KeyValue<u32, u32>>::new(&StreamConfig::new(1, "left"), environment.clone());
    let multi =
        MultiJoinStream::new(&config, environment.clone(), Combine { partial: true }).unwrap();
    let (sender, mut receiver) = mpsc::unbounded_channel();
    let collector = multi
        .stream()
        .try_set_typed_consumer(Arc::new(Capture(sender)), 5)
        .unwrap();
    let input = multi.connect_left(&left, collector).unwrap();
    environment.build_runtime_streams().unwrap();
    input
        .collect(
            MessageContext::new().with_stream_id("ttl-multi"),
            KeyValue { key: 7, value: 11 },
        )
        .await;
    assert_eq!(receiver.try_recv().unwrap().1, (7, 11));
    drop(input);
    drop(left);
    drop(multi);
    tokio::time::advance(Duration::from_secs(11)).await;
    let (context, value) = tokio::time::timeout(Duration::from_secs(1), receiver.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(context.stream_id(), Some("ttl-multi"));
    assert_eq!(value, (7, 11));
    environment.delay_pool().stop().await;
}
