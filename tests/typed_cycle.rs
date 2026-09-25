use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
    time::Duration,
};

use servicelib::{
    MessageContext, Payload,
    operators::link::LinkStream,
    runtime::{
        collector::{Collect, Collector},
        common::Consumer,
        config::{
            CallSemantics, CycleLinkStreamConfig, MapStreamConfig, RuntimeConfig,
            RuntimeStreamConfig, StreamConfig,
        },
        environment::RuntimeEnvironment,
        stream::Stream,
    },
};

type Observed = Arc<Mutex<BTreeMap<String, Vec<u32>>>>;

struct Countdown<C> {
    output: Collector<u32, C>,
    observed: Observed,
}

impl<C: Collect<u32>> Consumer<u32> for Countdown<C> {
    async fn consume(&self, context: MessageContext, value: Payload<u32>) {
        self.observed
            .lock()
            .unwrap()
            .entry(context.stream_id().unwrap().to_owned())
            .or_default()
            .push(*value);
        if *value > 0 {
            tokio::task::yield_now().await;
            self.output.collect(context, *value - 1).await;
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn typed_cycle_has_a_finite_future_and_preserves_each_invocation() {
    let mut link_config = StreamConfig::new(1, "loop");
    link_config.id_source = 2;
    let link_config = CycleLinkStreamConfig {
        stream: link_config,
    };
    let mut step_config = StreamConfig::new(2, "countdown");
    step_config.id_source = 1;
    let environment = RuntimeEnvironment::default();
    environment.publish_runtime_config(Arc::new(
        RuntimeConfig::from_parts(
            CallSemantics::FunctionCall,
            [],
            [
                RuntimeStreamConfig::from(link_config.clone()),
                RuntimeStreamConfig::from(MapStreamConfig::from(step_config.clone())),
            ],
            [],
            [],
            [],
            [],
        )
        .unwrap(),
    ));
    let link = LinkStream::make(&link_config, environment.clone());
    let source = Stream::new(&step_config, environment.clone());
    let output = link.set_source_typed(&source).unwrap();
    let observed = Arc::new(Mutex::new(BTreeMap::new()));
    let input = link
        .stream()
        .try_set_typed_consumer(
            Arc::new(Countdown {
                output,
                observed: observed.clone(),
            }),
            2,
        )
        .unwrap();
    assert!(link.set_source_typed(&source).is_err());
    assert_eq!(link.source().unwrap().id(), source.id());
    environment.build_runtime_streams().unwrap();
    let mut tasks = tokio::task::JoinSet::new();
    for index in 0..32 {
        let input = input.clone();
        tasks.spawn(async move {
            input
                .collect(
                    MessageContext::new().with_stream_id(format!("call-{index}")),
                    3,
                )
                .await;
        });
    }
    tokio::time::timeout(Duration::from_secs(5), async {
        while let Some(result) = tasks.join_next().await {
            result.unwrap();
        }
    })
    .await
    .unwrap();
    link.stream()
        .emit(
            MessageContext::new().with_stream_id("public-link"),
            Payload::new(2),
        )
        .await;
    source
        .emit(
            MessageContext::new().with_stream_id("public-source"),
            Payload::new(1),
        )
        .await;
    {
        let observed = observed.lock().unwrap();
        for index in 0..32 {
            assert_eq!(observed[&format!("call-{index}")], [3, 2, 1, 0]);
        }
        assert_eq!(observed["public-link"], [2, 1, 0]);
        assert_eq!(observed["public-source"], [1, 0]);
        assert_eq!(observed.len(), 34);
    }
    environment.delay_pool().stop().await;
}
