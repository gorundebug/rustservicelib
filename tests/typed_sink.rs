use std::{sync::Arc, time::Duration};

use servicelib::{
    MessageContext, Payload,
    operators::{SinkStream, SinkStreamWithResult},
    runtime::{
        common::Consumer,
        config::{
            CallSemantics, MapStreamConfig, RuntimeConfig, RuntimeStreamConfig, SinkStreamConfig,
            StreamConfig,
        },
        environment::RuntimeEnvironment,
        stream::Stream,
    },
};
use tokio::sync::{Notify, mpsc};

struct Capture<T>(mpsc::UnboundedSender<(MessageContext, Payload<T>)>);

impl<T: Send + Sync + 'static> Consumer<T> for Capture<T> {
    async fn consume(&self, context: MessageContext, value: Payload<T>) {
        self.0.send((context, value)).unwrap();
    }
}

fn config(id: i32, source: i32) -> StreamConfig {
    let mut config = StreamConfig::new(id, format!("node-{id}"));
    config.id_source = source;
    config
}

fn sink_config(id: i32) -> SinkStreamConfig {
    SinkStreamConfig {
        stream: config(id, id - 1),
        endpoint_id: 7,
    }
}

fn environment() -> RuntimeEnvironment {
    let environment = RuntimeEnvironment::default();
    let mut streams = Vec::new();
    for base in [0, 10] {
        streams.extend([
            RuntimeStreamConfig::from(MapStreamConfig::from(config(base + 1, 0))),
            RuntimeStreamConfig::from(sink_config(base + 2)),
            RuntimeStreamConfig::from(MapStreamConfig::from(config(base + 3, base + 2))),
            RuntimeStreamConfig::from(MapStreamConfig::from(config(base + 4, -(base + 2)))),
        ]);
    }
    environment.publish_runtime_config(Arc::new(
        RuntimeConfig::from_parts(CallSemantics::FunctionCall, [], streams, [], [], [], [])
            .unwrap(),
    ));
    environment
}

#[tokio::test]
async fn typed_sink_keeps_public_input_context_and_endpoint_replacement() {
    let environment = environment();
    let source = Stream::new(&config(1, 0), environment.clone());
    let sink = SinkStream::<u32, String>::new(&sink_config(2), environment.clone()).unwrap();
    let (old_sender, mut old_receiver) = mpsc::unbounded_channel();
    sink.set_sink_consumer(Arc::new(Capture(old_sender)))
        .unwrap();
    let input = source.try_set_typed_consumer(sink.clone(), 2).unwrap();
    let (sender, mut receiver) = mpsc::unbounded_channel();
    sink.set_sink_consumer(Arc::new(Capture(sender))).unwrap();
    let (error_sender, mut errors) = mpsc::unbounded_channel();
    sink.error_stream()
        .try_set_typed_consumer(Arc::new(Capture(error_sender)), 4)
        .unwrap();
    environment.build_runtime_streams().unwrap();
    let context = MessageContext::new()
        .with_stream_id("sink-call")
        .with_timeout_limit(Duration::from_secs(10));
    let deadline = context.deadline();
    context.cancel();
    input.collect(context.clone(), 11).await;
    source.emit(context.clone(), Payload::new(12)).await;
    sink.error_stream()
        .emit(context, Payload::new("business failure".to_owned()))
        .await;
    for expected in [11, 12] {
        let (context, value) = receiver.try_recv().unwrap();
        assert_eq!(*value, expected);
        assert_eq!(context.stream_id(), Some("sink-call"));
        assert_eq!(context.deadline(), deadline);
        assert!(context.is_cancelled());
    }
    assert!(old_receiver.try_recv().is_err());
    assert!(receiver.try_recv().is_err());
    assert_eq!(&*errors.try_recv().unwrap().1, "business failure");
    assert_eq!(sink.endpoint_id(), 7);
    environment.delay_pool().stop().await;
}

#[tokio::test]
async fn typed_sinks_sharing_an_endpoint_keep_separate_results_and_errors() {
    let environment = environment();
    let source_a = Stream::new(&config(1, 0), environment.clone());
    let source_b = Stream::new(&config(11, 0), environment.clone());
    let a = SinkStreamWithResult::<u32, String, String>::new(&sink_config(2), environment.clone())
        .unwrap();
    let b = SinkStreamWithResult::<u32, String, String>::new(&sink_config(12), environment.clone())
        .unwrap();
    let (sender, mut requests) = mpsc::unbounded_channel();
    let endpoint = Arc::new(Capture(sender));
    a.set_sink_consumer(endpoint.clone()).unwrap();
    b.set_sink_consumer(endpoint).unwrap();
    let input_a = source_a.try_set_typed_consumer(a.clone(), 2).unwrap();
    let input_b = source_b.try_set_typed_consumer(b.clone(), 12).unwrap();
    let (sender_a, mut results_a) = mpsc::unbounded_channel();
    let (sender_b, mut results_b) = mpsc::unbounded_channel();
    let (error_sender, mut errors) = mpsc::unbounded_channel();
    a.stream()
        .try_set_typed_consumer(Arc::new(Capture(sender_a)), 3)
        .unwrap();
    b.stream()
        .try_set_typed_consumer(Arc::new(Capture(sender_b)), 13)
        .unwrap();
    b.error_stream()
        .try_set_typed_consumer(Arc::new(Capture(error_sender)), 14)
        .unwrap();
    environment.build_runtime_streams().unwrap();
    let context_a = MessageContext::new().with_stream_id("request-a");
    let context_b = MessageContext::new().with_stream_id("request-b");
    tokio::join!(
        input_a.collect(context_a.clone(), 1),
        input_b.collect(context_b.clone(), 2)
    );
    let mut request_values = vec![
        *requests.try_recv().unwrap().1,
        *requests.try_recv().unwrap().1,
    ];
    request_values.sort();
    assert_eq!(request_values, [1, 2]);
    a.consume_result(context_a, Payload::new("result-a".to_owned()))
        .await;
    b.consume_result(context_b.clone(), Payload::new("result-b".to_owned()))
        .await;
    b.error_stream()
        .emit(context_b, Payload::new("failure-b".to_owned()))
        .await;
    let (context, result) = results_a.try_recv().unwrap();
    assert_eq!(context.stream_id(), Some("request-a"));
    assert_eq!(&*result, "result-a");
    let (context, result) = results_b.try_recv().unwrap();
    assert_eq!(context.stream_id(), Some("request-b"));
    assert_eq!(&*result, "result-b");
    assert_eq!(&*errors.try_recv().unwrap().1, "failure-b");
    assert!(results_a.try_recv().is_err());
    assert!(results_b.try_recv().is_err());
    assert_eq!(a.endpoint_id(), b.endpoint_id());
    environment.delay_pool().stop().await;
}

struct PendingEndpoint {
    entered: Arc<Notify>,
    release: Arc<Notify>,
    output: mpsc::UnboundedSender<(MessageContext, Payload<u32>)>,
}

impl Consumer<u32> for PendingEndpoint {
    async fn consume(&self, context: MessageContext, value: Payload<u32>) {
        self.entered.notify_one();
        self.release.notified().await;
        self.output.send((context, value)).unwrap();
    }
}

#[tokio::test]
async fn typed_sink_retains_a_pending_endpoint_until_delivery_finishes() {
    let environment = environment();
    let source = Stream::new(&config(1, 0), environment.clone());
    let sink = SinkStream::<u32, String>::new(&sink_config(2), environment.clone()).unwrap();
    let entered = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let (output, mut receiver) = mpsc::unbounded_channel();
    let endpoint = Arc::new(PendingEndpoint {
        entered: entered.clone(),
        release: release.clone(),
        output,
    });
    let weak_endpoint = Arc::downgrade(&endpoint);
    sink.set_sink_consumer(endpoint).unwrap();
    let input = source.try_set_typed_consumer(sink.clone(), 2).unwrap();
    environment.build_runtime_streams().unwrap();
    let task = tokio::spawn(async move {
        input
            .collect(MessageContext::new().with_stream_id("pending"), 42)
            .await;
    });
    tokio::time::timeout(Duration::from_secs(5), entered.notified())
        .await
        .unwrap();
    drop(source);
    drop(sink);
    assert!(weak_endpoint.upgrade().is_some());
    release.notify_one();
    tokio::time::timeout(Duration::from_secs(5), task)
        .await
        .unwrap()
        .unwrap();
    let (context, value) = receiver.try_recv().unwrap();
    assert_eq!(*value, 42);
    assert_eq!(context.stream_id(), Some("pending"));
    assert!(weak_endpoint.upgrade().is_none());
    environment.delay_pool().stop().await;
}
