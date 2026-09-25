use std::{sync::Arc, time::Duration};

use servicelib::{
    MessageContext, Payload,
    operators::{InputStream, MapFunction, map::MapStream},
    runtime::{
        collector::Collect,
        common::{Consumer, RuntimeStream},
        config::{
            CallSemantics, InputStreamConfig, MapStreamConfig, RuntimeConfig, RuntimeStreamConfig,
            StreamConfig,
        },
        environment::RuntimeEnvironment,
        stream::Stream,
    },
};
use tokio::sync::{Notify, mpsc};

struct Echo;

impl MapFunction<u32, u32> for Echo {
    async fn map(
        &self,
        context: MessageContext,
        _: &dyn RuntimeStream,
        value: &u32,
        out: &impl Collect<u32>,
    ) {
        tokio::task::yield_now().await;
        out.collect(context, *value).await;
    }
}

struct Capture(mpsc::UnboundedSender<(MessageContext, u32)>);

impl Consumer<u32> for Capture {
    async fn consume(&self, context: MessageContext, value: Payload<u32>) {
        self.0.send((context, *value)).unwrap();
    }
}

fn graph() -> (
    RuntimeEnvironment,
    InputStream<u32, u32, String>,
    Stream<u32>,
) {
    let mut input_config = StreamConfig::new(1, "input");
    input_config.id_source = 2;
    let input_config = InputStreamConfig {
        stream: input_config,
        endpoint_id: 7,
    };
    let mut output_config = StreamConfig::new(2, "output");
    output_config.id_source = 1;
    let environment = RuntimeEnvironment::default();
    environment.publish_runtime_config(Arc::new(
        RuntimeConfig::from_parts(
            CallSemantics::FunctionCall,
            [],
            [
                RuntimeStreamConfig::from(input_config.clone()),
                RuntimeStreamConfig::from(MapStreamConfig::from(output_config.clone())),
            ],
            [],
            [],
            [],
            [],
        )
        .unwrap(),
    ));
    let input = InputStream::new(&input_config, environment.clone());
    let output = Stream::new(&output_config, environment.clone());
    let result = input.set_source_typed(&output).unwrap();
    input
        .stream()
        .try_set_typed_consumer(Arc::new(MapStream::from_collector(result, Echo)), 2)
        .unwrap();
    environment.build_runtime_streams().unwrap();
    (environment, input, output)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn typed_input_keeps_concurrent_results_context_and_public_result_entry() {
    let (environment, input, output) = graph();
    let (old_sender, mut old_receiver) = mpsc::unbounded_channel();
    input.set_result_consumer(Arc::new(Capture(old_sender)));
    let (sender, mut receiver) = mpsc::unbounded_channel();
    let consumer = Arc::new(Capture(sender));
    let weak = Arc::downgrade(&consumer);
    input.set_result_consumer(consumer);
    let context = MessageContext::new().with_timeout_limit(Duration::from_secs(10));
    let deadline = context.deadline();
    context.cancel();
    let mut tasks = tokio::task::JoinSet::new();
    for value in 0..64 {
        let input = input.clone();
        let context = context.clone().with_stream_id(format!("call-{value}"));
        tasks.spawn(async move { input.consume(context, value).await });
    }
    while let Some(result) = tasks.join_next().await {
        result.unwrap();
    }
    let mut values = Vec::new();
    for _ in 0..64 {
        let (context, value) = receiver.try_recv().unwrap();
        assert_eq!(context.stream_id(), Some(format!("call-{value}").as_str()));
        assert_eq!(context.deadline(), deadline);
        assert!(context.is_cancelled());
        values.push(value);
    }
    values.sort();
    assert_eq!(values, (0..64).collect::<Vec<_>>());
    output
        .emit(context.with_stream_id("public-result"), Payload::new(99))
        .await;
    assert_eq!(receiver.try_recv().unwrap().1, 99);
    assert!(receiver.try_recv().is_err());
    assert!(old_receiver.try_recv().is_err());
    drop(input);
    drop(output);
    assert!(
        weak.upgrade().is_none(),
        "result link must not retain the input graph in an Arc cycle"
    );
    environment.delay_pool().stop().await;
}

struct PendingResult {
    entered: Arc<Notify>,
    release: Arc<Notify>,
    output: mpsc::UnboundedSender<u32>,
}

impl Consumer<u32> for PendingResult {
    async fn consume(&self, _: MessageContext, value: Payload<u32>) {
        self.entered.notify_one();
        self.release.notified().await;
        self.output.send(*value).unwrap();
    }
}

#[tokio::test]
async fn typed_input_retains_pending_result_without_retaining_the_input_graph() {
    let (environment, input, output) = graph();
    let entered = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let (sender, mut receiver) = mpsc::unbounded_channel();
    let consumer = Arc::new(PendingResult {
        entered: entered.clone(),
        release: release.clone(),
        output: sender,
    });
    let weak = Arc::downgrade(&consumer);
    input.set_result_consumer(consumer);
    let task =
        tokio::spawn(async move { output.emit(MessageContext::new(), Payload::new(42)).await });
    tokio::time::timeout(Duration::from_secs(5), entered.notified())
        .await
        .unwrap();
    drop(input);
    assert!(weak.upgrade().is_some());
    release.notify_one();
    tokio::time::timeout(Duration::from_secs(5), task)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(receiver.try_recv().unwrap(), 42);
    assert!(weak.upgrade().is_none());
    environment.delay_pool().stop().await;
}
