use std::{sync::Arc, time::Duration};

use futures::FutureExt;
use servicelib::{
    MessageContext, Payload,
    operators::case::TypedCaseStream,
    runtime::{
        common::Consumer,
        config::{
            CallSemantics, CaseStreamConfig, MapStreamConfig, RuntimeConfig, RuntimeStreamConfig,
            StreamConfig, WhenStreamConfig,
        },
        environment::RuntimeEnvironment,
        stream::Stream,
    },
};
use tokio::sync::mpsc;

struct Capture(mpsc::UnboundedSender<(MessageContext, u32)>);
struct DoubleCapture(mpsc::UnboundedSender<(MessageContext, u32)>);

impl Consumer<u32> for Capture {
    async fn consume(&self, context: MessageContext, value: Payload<u32>) {
        self.0.send((context, *value)).unwrap();
    }
}

impl Consumer<u32> for DoubleCapture {
    async fn consume(&self, context: MessageContext, value: Payload<u32>) {
        self.0.send((context, *value * 2)).unwrap();
    }
}

fn config(id: i32, source: i32) -> StreamConfig {
    let mut config = StreamConfig::new(id, format!("node-{id}"));
    config.id_source = source;
    config
}

fn environment() -> RuntimeEnvironment {
    let environment = RuntimeEnvironment::default();
    environment.publish_runtime_config(Arc::new(
        RuntimeConfig::from_parts(
            CallSemantics::FunctionCall,
            [],
            [
                RuntimeStreamConfig::from(MapStreamConfig::from(config(1, 0))),
                RuntimeStreamConfig::from(CaseStreamConfig::from(config(2, 1))),
                RuntimeStreamConfig::from(WhenStreamConfig::from(config(3, 2))),
                RuntimeStreamConfig::from(WhenStreamConfig::from(config(4, 2))),
                RuntimeStreamConfig::from(MapStreamConfig::from(config(5, 3))),
                RuntimeStreamConfig::from(MapStreamConfig::from(config(6, 4))),
            ],
            [],
            [],
            [],
            [],
        )
        .unwrap(),
    ));
    environment
}

#[tokio::test]
async fn typed_case_routes_concrete_branches_and_preserves_public_stream_entries() {
    let environment = environment();
    let root = Stream::new(&config(1, 0), environment.clone());
    let case = TypedCaseStream::create_links(&config(2, 1).into(), &root, |value: &u32| {
        (*value % 2) as usize
    });
    let even = case.when(&config(3, 2).into());
    let odd = case.when(&config(4, 2).into());
    let (sender, mut receiver) = mpsc::unbounded_channel();
    let even_output = even
        .try_set_typed_consumer(Arc::new(Capture(sender.clone())), 5)
        .unwrap();
    let odd_output = odd
        .try_set_typed_consumer(Arc::new(DoubleCapture(sender)), 6)
        .unwrap();
    assert!(
        case.from_branches((odd_output.clone(), (even_output.clone(), ())))
            .is_err()
    );
    let operator = case.from_branches((even_output, (odd_output, ()))).unwrap();
    let input = root.try_set_typed_consumer(operator, 2).unwrap();
    environment.build_runtime_streams().unwrap();
    let context = MessageContext::new()
        .with_stream_id("case-call")
        .with_timeout_limit(Duration::from_secs(10));
    let deadline = context.deadline();
    context.cancel();
    input.collect(context.clone(), 2).await;
    input.collect(context.clone(), 3).await;
    root.emit(context.clone(), Payload::new(5)).await;
    even.emit(context.clone(), Payload::new(7)).await;
    odd.emit(context, Payload::new(9)).await;
    for expected in [2, 6, 10, 7, 18] {
        let (context, value) = receiver.try_recv().unwrap();
        assert_eq!(value, expected);
        assert_eq!(context.stream_id(), Some("case-call"));
        assert_eq!(context.deadline(), deadline);
        assert!(context.is_cancelled());
    }
    assert!(receiver.try_recv().is_err());
    environment.delay_pool().stop().await;
}

#[tokio::test]
async fn typed_case_rejects_an_invalid_index_instead_of_losing_the_message() {
    let environment = environment();
    let root = Stream::new(&config(1, 0), environment.clone());
    let case = TypedCaseStream::create_links(&config(2, 1).into(), &root, |_: &u32| 2);
    let first = case.when(&config(3, 2).into());
    let second = case.when(&config(4, 2).into());
    let operator = case
        .from_branches((first.collector(), (second.collector(), ())))
        .unwrap();
    let input = root.try_set_typed_consumer(operator, 2).unwrap();
    environment.build_runtime_streams().unwrap();
    let panic = std::panic::AssertUnwindSafe(input.collect(MessageContext::new(), 7))
        .catch_unwind()
        .await;
    assert!(panic.is_err());
    environment.delay_pool().stop().await;
}
