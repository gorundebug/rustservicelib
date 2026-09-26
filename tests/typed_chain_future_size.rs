#![recursion_limit = "512"]

use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use servicelib::{
    Collect, Consumer, MessageContext, Payload, Stream,
    runtime::{
        config::{CallSemantics, MapStreamConfig, RuntimeConfig, RuntimeStreamConfig, StreamConfig},
        environment::RuntimeEnvironment,
    },
};

struct Forward<C> {
    output: C,
    caller: Option<tokio::task::Id>,
}

impl<C: Collect<u64>> Consumer<u64> for Forward<C> {
    async fn consume(&self, context: MessageContext, payload: Payload<u64>) {
        assert_eq!(context.stream_id(), Some("long-static-chain"));
        assert_eq!(tokio::task::try_id(), self.caller);
        tokio::task::yield_now().await;
        assert_eq!(tokio::task::try_id(), self.caller);
        self.output.emit(context, payload).await;
    }
}

struct Capture {
    calls: Arc<AtomicUsize>,
    caller: Option<tokio::task::Id>,
}

impl Consumer<u64> for Capture {
    async fn consume(&self, context: MessageContext, payload: Payload<u64>) {
        assert_eq!(context.stream_id(), Some("long-static-chain"));
        assert_eq!(tokio::task::try_id(), self.caller);
        assert_eq!(*payload, 12345);
        self.calls.fetch_add(1, Ordering::Relaxed);
    }
}

#[tokio::test(flavor = "current_thread")]
async fn static_chain_depth_does_not_expand_the_callers_future_or_spawn_tasks() {
    let environment = RuntimeEnvironment::default();
    let configs: Vec<_> = (1..=34)
        .map(|id| {
            let mut config = StreamConfig::new(id, format!("node{id}"));
            config.id_service = 1;
            config.id_source = id - 1;
            config
        })
        .collect();
    environment.publish_runtime_config(Arc::new(
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
    let streams: Vec<_> = configs[..33]
        .iter()
        .map(|config| Stream::<u64>::new(config, environment.clone()))
        .collect();
    let caller = tokio::task::try_id();
    let calls = Arc::new(AtomicUsize::new(0));
    let output = streams[32]
        .try_set_typed_consumer(
            Arc::new(Capture {
                calls: calls.clone(),
                caller,
            }),
            34,
        )
        .unwrap();
    let context = MessageContext::new().with_stream_id("long-static-chain");
    let shallow_size = std::mem::size_of_val(&output.collect(context.clone(), 12345));

    // Shadowing keeps every link's concrete, nested consumer type. There is no
    // dynamic consumer or runtime loop hiding the depth from the compiler.
    macro_rules! prepend {
        ($output:ident, $index:expr) => {
            let $output = streams[$index]
                .try_set_typed_consumer(
                    Arc::new(Forward {
                        output: $output,
                        caller,
                    }),
                    ($index + 2) as i32,
                )
                .unwrap();
        };
    }
    prepend!(output, 31);
    prepend!(output, 30);
    prepend!(output, 29);
    prepend!(output, 28);
    prepend!(output, 27);
    prepend!(output, 26);
    prepend!(output, 25);
    prepend!(output, 24);
    prepend!(output, 23);
    prepend!(output, 22);
    prepend!(output, 21);
    prepend!(output, 20);
    prepend!(output, 19);
    prepend!(output, 18);
    prepend!(output, 17);
    prepend!(output, 16);
    prepend!(output, 15);
    prepend!(output, 14);
    prepend!(output, 13);
    prepend!(output, 12);
    prepend!(output, 11);
    prepend!(output, 10);
    prepend!(output, 9);
    prepend!(output, 8);
    prepend!(output, 7);
    prepend!(output, 6);
    prepend!(output, 5);
    prepend!(output, 4);
    prepend!(output, 3);
    prepend!(output, 2);
    prepend!(output, 1);
    prepend!(output, 0);

    environment.build_runtime_streams().unwrap();
    let deep_size = std::mem::size_of_val(&output.collect(context.clone(), 12345));
    assert_eq!(deep_size, shallow_size);
    output.collect(context.clone(), 12345).await;
    streams[0].emit(context, Payload::new(12345)).await;
    assert_eq!(calls.load(Ordering::Relaxed), 2);
    environment.delay_pool().stop().await;
}
