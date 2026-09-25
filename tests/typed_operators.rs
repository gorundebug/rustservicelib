use std::sync::{Arc, Mutex};

use servicelib::{
    Collect, Consumer, MessageContext, Payload, RuntimeStream, Stream,
    operators::{
        FilterFunction, FlatMapFunction, MapFunction, filter::FilterStream, flatmap::FlatMapStream,
        map::MapStream, merge::MergeStream, split::SplitStream,
    },
    runtime::{
        config::{
            CallSemantics, LinkConfig, MapStreamConfig, RuntimeConfig, RuntimeStreamConfig,
            SplitStreamConfig, StreamConfig,
        },
        environment::RuntimeEnvironment,
    },
};

struct Increment;
impl MapFunction<u64, u64> for Increment {
    async fn map(
        &self,
        context: MessageContext,
        _: &dyn RuntimeStream,
        value: &u64,
        out: &impl Collect<u64>,
    ) {
        out.collect(context, *value + 1).await;
    }
}
struct Positive;
impl FilterFunction<u64> for Positive {
    async fn filter(&self, _: MessageContext, _: &dyn RuntimeStream, value: &u64) -> bool {
        *value != 0
    }
}
struct Expand;
impl FlatMapFunction<u64, u64> for Expand {
    async fn flat_map(
        &self,
        context: MessageContext,
        _: &dyn RuntimeStream,
        value: &u64,
        out: &impl Collect<u64>,
    ) {
        out.collect(context.clone(), *value).await;
        tokio::task::yield_now().await;
        out.collect(context, *value + 1).await;
    }
}
struct Capture(Arc<Mutex<Vec<(String, u64)>>>);
impl Consumer<u64> for Capture {
    async fn consume(&self, context: MessageContext, value: Payload<u64>) {
        self.0
            .lock()
            .unwrap()
            .push((context.stream_id().unwrap().to_owned(), *value));
    }
}

#[tokio::test]
async fn real_operators_share_their_live_streams_with_typed_collectors() {
    let env = RuntimeEnvironment::default();
    let configs: [StreamConfig; 6] = std::array::from_fn(|index| {
        let mut config = StreamConfig::new(index as i32 + 1, format!("node{index}"));
        config.id_source = index as i32;
        config
    });
    env.publish_runtime_config(Arc::new(
        RuntimeConfig::from_parts(
            CallSemantics::FunctionCall,
            [],
            configs
                .iter()
                .cloned()
                .map(|config| RuntimeStreamConfig::from(MapStreamConfig::from(config))),
            [],
            [],
            [],
            [],
        )
        .unwrap(),
    ));
    let streams = configs.map(|config| Stream::new(&config, env.clone()));
    let values = Arc::new(Mutex::new(Vec::new()));
    let merge_out = streams[4]
        .try_set_typed_consumer(Arc::new(Capture(values.clone())), 6)
        .unwrap();
    let merge = Arc::new(MergeStream::from_collector(merge_out));
    let flat_out = streams[3].try_set_typed_consumer(merge, 5).unwrap();
    let flat = Arc::new(FlatMapStream::from_collector(flat_out, Expand));
    let filter_out = streams[2].try_set_typed_consumer(flat, 4).unwrap();
    let filter = Arc::new(FilterStream::from_collector(filter_out, Positive));
    let map_out = streams[1].try_set_typed_consumer(filter, 3).unwrap();
    let map = Arc::new(MapStream::from_collector(map_out, Increment));
    let root = streams[0].try_set_typed_consumer(map, 2).unwrap();
    env.build_runtime_streams().unwrap();

    root.collect(MessageContext::new().with_stream_id("typed"), 1)
        .await;
    streams[0]
        .emit(
            MessageContext::new().with_stream_id("root"),
            Payload::new(1),
        )
        .await;
    for (index, value) in [(1, 6), (2, 8), (3, 10), (4, 11)] {
        streams[index]
            .emit(
                MessageContext::new().with_stream_id(format!("node{index}")),
                Payload::new(value),
            )
            .await;
    }
    streams[1]
        .emit(
            MessageContext::new().with_stream_id("filtered"),
            Payload::new(0),
        )
        .await;
    assert_eq!(
        *values.lock().unwrap(),
        [
            ("typed".into(), 2),
            ("typed".into(), 3),
            ("root".into(), 2),
            ("root".into(), 3),
            ("node1".into(), 6),
            ("node1".into(), 7),
            ("node2".into(), 8),
            ("node2".into(), 9),
            ("node3".into(), 10),
            ("node4".into(), 11),
        ]
    );
}

struct Branch<const INDEX: usize>(Arc<Mutex<Vec<(usize, bool)>>>);
impl<const INDEX: usize> Consumer<u64> for Branch<INDEX> {
    async fn consume(&self, context: MessageContext, value: Payload<u64>) {
        assert_eq!(context.stream_id(), Some("split"));
        assert_eq!(*value, 7);
        self.0.lock().unwrap().push((INDEX, false));
        tokio::task::yield_now().await;
        self.0.lock().unwrap().push((INDEX, true));
    }
}

#[tokio::test]
async fn heterogeneous_split_preserves_order_and_sequential_completion_for_every_async_combination()
{
    for flags in 0..8 {
        let env = RuntimeEnvironment::default();
        let input = StreamConfig::new(1, "input");
        let mut split = StreamConfig::new(2, "split");
        split.id_source = 1;
        let split = SplitStreamConfig::from(split);
        let mut configurations = vec![
            RuntimeStreamConfig::from(MapStreamConfig::from(input.clone())),
            split.clone().into(),
        ];
        for index in 0..3 {
            let mut target = StreamConfig::new(index + 3, format!("target{index}"));
            target.id_source = 2;
            configurations.push(MapStreamConfig::from(target).into());
        }
        env.publish_runtime_config(Arc::new(
            RuntimeConfig::from_parts(
                CallSemantics::FunctionCall,
                [],
                configurations,
                [],
                [],
                [],
                (0..3).map(|index| LinkConfig {
                    from: 2,
                    to: index + 3,
                    call_semantics: CallSemantics::FunctionCall,
                    r#async: flags & (1 << index) != 0,
                }),
            )
            .unwrap(),
        ));
        let input = Stream::<u64>::new(&input, env.clone());
        let links = SplitStream::<u64, 3>::create_links(&split, &input);
        let events = Arc::new(Mutex::new(Vec::new()));
        let a = links[0]
            .try_set_typed_consumer(Arc::new(Branch::<0>(events.clone())), 3)
            .unwrap();
        let b = links[1]
            .try_set_typed_consumer(Arc::new(Branch::<1>(events.clone())), 4)
            .unwrap();
        let c = links[2]
            .try_set_typed_consumer(Arc::new(Branch::<2>(events.clone())), 5)
            .unwrap();
        for link in &links {
            assert!(Arc::ptr_eq(&input.get_serde(), &link.get_serde()));
        }
        let operator =
            SplitStream::<u64, 3>::from_typed(&split, &input, (a, (b, (c, ())))).unwrap();
        let root = input.try_set_typed_consumer(operator, 2).unwrap();
        env.build_runtime_streams().unwrap();
        root.collect(MessageContext::new().with_stream_id("split"), 7)
            .await;
        let mut order = [0, 1, 2];
        order.sort_by_key(|index| flags & (1 << index) == 0);
        let expected: Vec<_> = order
            .into_iter()
            .flat_map(|index| [(index, false), (index, true)])
            .collect();
        assert_eq!(*events.lock().unwrap(), expected);
        events.lock().unwrap().clear();
        input
            .emit(
                MessageContext::new().with_stream_id("split"),
                Payload::new(7),
            )
            .await;
        assert_eq!(*events.lock().unwrap(), expected);
    }
}
