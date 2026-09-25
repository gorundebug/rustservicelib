use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use servicelib::{
    MessageContext, Payload,
    operators::delay::{DelayFunction, DelayStream},
    runtime::{
        collector::Collect,
        common::{Consumer, RuntimeStream},
        config::{
            CallSemantics, DelayStreamConfig, MapStreamConfig, RuntimeConfig, RuntimeStreamConfig,
            StreamConfig,
        },
        environment::{RuntimeEnvironment, RuntimeError},
        stream::Stream,
    },
};
use tokio::sync::mpsc;

struct Item {
    dropped: Arc<AtomicUsize>,
}

impl Drop for Item {
    fn drop(&mut self) {
        self.dropped.fetch_add(1, Ordering::SeqCst);
    }
}

struct FixedDelay {
    duration: Duration,
    rejected: Arc<AtomicUsize>,
}

impl DelayFunction<Item> for FixedDelay {
    async fn duration(&self, _: MessageContext, _: &dyn RuntimeStream, _: &Item) -> Duration {
        self.duration
    }

    async fn delay_error(
        &self,
        _: MessageContext,
        _: &dyn RuntimeStream,
        _: &Item,
        _: RuntimeError,
        _: &impl Collect<Item>,
    ) {
        self.rejected.fetch_add(1, Ordering::SeqCst);
    }
}

struct Capture(mpsc::UnboundedSender<(MessageContext, Payload<Item>)>);

impl Consumer<Item> for Capture {
    async fn consume(&self, context: MessageContext, value: Payload<Item>) {
        self.0
            .send((context, value))
            .unwrap_or_else(|_| panic!("receiver closed"));
    }
}

fn streams() -> (RuntimeEnvironment, Stream<Item>, Stream<Item>) {
    let root = MapStreamConfig::from(StreamConfig::new(1, "root"));
    let mut delay = StreamConfig::new(2, "delay");
    delay.id_source = 1;
    let delay = DelayStreamConfig::from(delay);
    let mut capture = StreamConfig::new(3, "capture");
    capture.id_source = 2;
    let environment = RuntimeEnvironment::default();
    environment.publish_runtime_config(Arc::new(
        RuntimeConfig::from_parts(
            CallSemantics::FunctionCall,
            [],
            [
                RuntimeStreamConfig::from(root.clone()),
                RuntimeStreamConfig::from(delay.clone()),
                RuntimeStreamConfig::from(MapStreamConfig::from(capture)),
            ],
            [],
            [],
            [],
            [],
        )
        .unwrap(),
    ));
    let root = Stream::new(&root.stream, environment.clone());
    let output = Stream::derived(&delay.stream, environment.clone(), root.get_serde());
    (environment, root, output)
}

#[tokio::test]
async fn scheduled_typed_output_owns_payload_after_the_call_and_handles_are_gone() {
    tokio::time::timeout(Duration::from_secs(3), async {
        let (environment, root, output) = streams();
        let (sender, mut receiver) = mpsc::unbounded_channel();
        let collector = output
            .try_set_typed_consumer(Arc::new(Capture(sender)), 3)
            .unwrap();
        let rejected = Arc::new(AtomicUsize::new(0));
        let operator = Arc::new(DelayStream::from_collector(
            collector,
            FixedDelay {
                duration: Duration::from_millis(20),
                rejected: rejected.clone(),
            },
        ));
        let input = root.try_set_typed_consumer(operator, 2).unwrap();
        environment.build_runtime_streams().unwrap();
        let dropped = Arc::new(AtomicUsize::new(0));
        input
            .collect(
                MessageContext::new().with_stream_id("delayed"),
                Item {
                    dropped: dropped.clone(),
                },
            )
            .await;
        assert_eq!(dropped.load(Ordering::SeqCst), 0);
        drop(input);
        drop(root);
        drop(output);
        let (context, payload) = receiver.recv().await.unwrap();
        assert_eq!(context.stream_id(), Some("delayed"));
        assert_eq!(dropped.load(Ordering::SeqCst), 0);
        drop(payload);
        assert_eq!(dropped.load(Ordering::SeqCst), 1);
        assert_eq!(rejected.load(Ordering::SeqCst), 0);
        environment.delay_pool().stop().await;
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn zero_delay_keeps_all_public_entries_live_even_after_cancellation() {
    let (environment, root, output) = streams();
    let (sender, mut receiver) = mpsc::unbounded_channel();
    let collector = output
        .try_set_typed_consumer(Arc::new(Capture(sender)), 3)
        .unwrap();
    let rejected = Arc::new(AtomicUsize::new(0));
    let input = root
        .try_set_typed_consumer(
            Arc::new(DelayStream::from_collector(
                collector,
                FixedDelay {
                    duration: Duration::ZERO,
                    rejected: rejected.clone(),
                },
            )),
            2,
        )
        .unwrap();
    environment.build_runtime_streams().unwrap();
    let dropped = Arc::new(AtomicUsize::new(0));
    let context = MessageContext::new().with_stream_id("cancelled");
    context.cancel();
    input
        .collect(
            context.clone(),
            Item {
                dropped: dropped.clone(),
            },
        )
        .await;
    root.emit(
        context.clone(),
        Payload::new(Item {
            dropped: dropped.clone(),
        }),
    )
    .await;
    output
        .emit(
            context,
            Payload::new(Item {
                dropped: dropped.clone(),
            }),
        )
        .await;
    for _ in 0..3 {
        let (context, payload) = receiver.try_recv().unwrap();
        assert_eq!(context.stream_id(), Some("cancelled"));
        assert!(context.is_cancelled());
        drop(payload);
    }
    assert_eq!(dropped.load(Ordering::SeqCst), 3);
    assert_eq!(rejected.load(Ordering::SeqCst), 0);
    environment.delay_pool().stop().await;
}

#[tokio::test]
async fn positive_delay_preserves_rejection_and_accepted_cancellation() {
    tokio::time::timeout(Duration::from_secs(3), async {
        for mode in 0..3 {
            let (environment, root, output) = streams();
            let (sender, mut receiver) = mpsc::unbounded_channel();
            let collector = output
                .try_set_typed_consumer(Arc::new(Capture(sender)), 3)
                .unwrap();
            let rejected = Arc::new(AtomicUsize::new(0));
            let input = root
                .try_set_typed_consumer(
                    Arc::new(DelayStream::from_collector(
                        collector,
                        FixedDelay {
                            duration: Duration::from_secs(60),
                            rejected: rejected.clone(),
                        },
                    )),
                    2,
                )
                .unwrap();
            environment.build_runtime_streams().unwrap();
            let dropped = Arc::new(AtomicUsize::new(0));
            let context = MessageContext::new();
            if mode == 0 {
                context.cancel();
            }
            if mode == 1 {
                environment.delay_pool().stop().await;
            }
            input
                .collect(
                    context.clone(),
                    Item {
                        dropped: dropped.clone(),
                    },
                )
                .await;
            if mode == 2 {
                context.cancel();
            }
            environment.delay_pool().stop().await;
            assert!(receiver.try_recv().is_err());
            assert_eq!(rejected.load(Ordering::SeqCst), usize::from(mode != 2));
            assert_eq!(dropped.load(Ordering::SeqCst), 1);
        }
    })
    .await
    .unwrap();
}
