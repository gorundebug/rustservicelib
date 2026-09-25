use std::{collections::BTreeSet, sync::Arc, time::Duration};

use servicelib::{
    MessageContext, Payload,
    operators::join::{JoinFunction, JoinStream},
    runtime::{
        collector::Collect,
        common::{Consumer, RuntimeStream},
        config::{
            CallSemantics, JoinStreamConfig, JoinType, MapStreamConfig, RuntimeConfig,
            RuntimeStreamConfig, StreamConfig,
        },
        datastruct::KeyValue,
        environment::RuntimeEnvironment,
        stream::Stream,
    },
};
use tokio::sync::mpsc;

struct Sum {
    keep: bool,
}

impl JoinFunction<u32, u32, String, (u32, u32)> for Sum {
    async fn join(
        &self,
        context: MessageContext,
        _: &dyn RuntimeStream,
        key: u32,
        left: Vec<u32>,
        right: Vec<String>,
        out: &impl Collect<(u32, u32)>,
    ) -> bool {
        let sum = left.into_iter().sum::<u32>()
            + right
                .into_iter()
                .map(|value| value.parse::<u32>().unwrap())
                .sum::<u32>();
        out.collect(context, (key, sum)).await;
        !self.keep
    }
}

struct Capture(mpsc::UnboundedSender<(MessageContext, (u32, u32))>);

impl Consumer<(u32, u32)> for Capture {
    async fn consume(&self, context: MessageContext, value: Payload<(u32, u32)>) {
        self.0.send((context, *value)).unwrap();
    }
}

fn environment(join_type: JoinType, ttl: Duration) -> (RuntimeEnvironment, JoinStreamConfig) {
    let mut join = JoinStreamConfig {
        stream: StreamConfig::new(3, "join"),
        join_type,
        join_storage: servicelib::api::JoinStorageType::HashMap,
        ttl,
        renew_ttl: false,
    };
    join.stream.id_source = 1;
    join.join_type = join_type;
    join.ttl = ttl;
    let mut result = StreamConfig::new(4, "result");
    result.id_source = 3;
    let environment = RuntimeEnvironment::default();
    environment.publish_runtime_config(Arc::new(
        RuntimeConfig::from_parts(
            CallSemantics::FunctionCall,
            [],
            [
                RuntimeStreamConfig::from(MapStreamConfig::from(StreamConfig::new(1, "left"))),
                RuntimeStreamConfig::from(MapStreamConfig::from(StreamConfig::new(2, "right"))),
                RuntimeStreamConfig::from(join.clone()),
                RuntimeStreamConfig::from(MapStreamConfig::from(result)),
            ],
            [],
            [],
            [],
            [],
        )
        .unwrap(),
    ));
    (environment, join)
}

#[tokio::test]
async fn typed_join_shares_state_between_inputs_and_preserves_public_entries() {
    tokio::time::timeout(Duration::from_secs(5), async {
        let (environment, config) = environment(JoinType::Inner, Duration::ZERO);
        let left = Stream::new(&StreamConfig::new(1, "left"), environment.clone());
        let right = Stream::new(&StreamConfig::new(2, "right"), environment.clone());
        let output = Stream::new(&config.stream, environment.clone());
        let (sender, mut receiver) = mpsc::unbounded_channel();
        let collector = output
            .try_set_typed_consumer(Arc::new(Capture(sender)), 4)
            .unwrap();
        let join = JoinStream::from_collector(&config, collector, Sum { keep: false }).unwrap();
        let left_input = left.try_set_typed_consumer(join.clone(), 3).unwrap();
        let right_input = right
            .try_set_typed_consumer(Arc::new(join.right()), 3)
            .unwrap();
        environment.build_runtime_streams().unwrap();
        let mut tasks = Vec::new();
        for key in 0..32 {
            let left_input = left_input.clone();
            let right_input = right_input.clone();
            tasks.push(tokio::spawn(async move {
                left_input
                    .collect(
                        MessageContext::new().with_stream_id("left"),
                        KeyValue { key, value: 10 },
                    )
                    .await;
                right_input
                    .collect(
                        MessageContext::new().with_stream_id("right"),
                        KeyValue {
                            key,
                            value: "20".to_owned(),
                        },
                    )
                    .await;
            }));
        }
        for task in tasks {
            task.await.unwrap();
        }
        let mut keys = BTreeSet::new();
        for _ in 0..32 {
            let (context, (key, value)) = receiver.try_recv().unwrap();
            assert_eq!(context.stream_id(), Some("right"));
            assert_eq!(value, 30);
            assert!(keys.insert(key));
        }
        left.emit(
            MessageContext::new(),
            Payload::new(KeyValue { key: 99, value: 1 }),
        )
        .await;
        assert!(receiver.try_recv().is_err());
        right
            .emit(
                MessageContext::new().with_stream_id("public"),
                Payload::new(KeyValue {
                    key: 99,
                    value: "2".to_owned(),
                }),
            )
            .await;
        let (context, value) = receiver.try_recv().unwrap();
        assert_eq!(context.stream_id(), Some("public"));
        assert_eq!(value, (99, 3));
        output
            .emit(MessageContext::new(), Payload::new((100, 7)))
            .await;
        assert_eq!(receiver.try_recv().unwrap().1, (100, 7));
        environment.delay_pool().stop().await;
    })
    .await
    .unwrap();
}

#[tokio::test(start_paused = true)]
async fn ttl_retains_typed_output_after_operator_handles_are_dropped() {
    let (environment, config) = environment(JoinType::Left, Duration::from_secs(10));
    let left = Stream::new(&StreamConfig::new(1, "left"), environment.clone());
    let output = Stream::new(&config.stream, environment.clone());
    let (sender, mut receiver) = mpsc::unbounded_channel();
    let collector = output
        .try_set_typed_consumer(Arc::new(Capture(sender)), 4)
        .unwrap();
    let join = JoinStream::from_collector(&config, collector, Sum { keep: true }).unwrap();
    let input = left.try_set_typed_consumer(join.clone(), 3).unwrap();
    environment.build_runtime_streams().unwrap();
    input
        .collect(
            MessageContext::new().with_stream_id("ttl"),
            KeyValue { key: 7, value: 11 },
        )
        .await;
    assert_eq!(receiver.try_recv().unwrap().1, (7, 11));
    drop(input);
    drop(left);
    drop(output);
    drop(join);
    tokio::time::advance(Duration::from_secs(11)).await;
    let (context, result) = tokio::time::timeout(Duration::from_secs(1), receiver.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(context.stream_id(), Some("ttl"));
    assert_eq!(result, (7, 11));
    environment.delay_pool().stop().await;
}
