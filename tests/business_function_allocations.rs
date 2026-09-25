use std::{
    alloc::{GlobalAlloc, Layout, System},
    cell::Cell,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};

use serde::{Deserialize, Serialize};
use servicelib::{
    MessageContext, Payload,
    operators::MapFunction,
    runtime::{
        common::{Consumer, RuntimeStream},
        config::{Config, MapStreamConfig, RuntimeConfig, RuntimeStreamConfig, StreamConfig},
        environment::RuntimeEnvironment,
        stream::Stream,
    },
};

thread_local! {
    static ALLOCATIONS: Cell<Option<usize>> = const { Cell::new(None) };
}

struct CountingAllocator;

fn record_allocation() {
    let _ = ALLOCATIONS.try_with(|count| {
        if let Some(value) = count.get() {
            count.set(Some(value + 1));
        }
    });
}

// Delegate memory ownership entirely to System; count only the test thread
// while it constructs a future, excluding setup and future execution.
unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        record_allocation();
        unsafe { System.alloc(layout) }
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        record_allocation();
        unsafe { System.alloc_zeroed(layout) }
    }

    unsafe fn realloc(&self, pointer: *mut u8, layout: Layout, size: usize) -> *mut u8 {
        record_allocation();
        unsafe { System.realloc(pointer, layout, size) }
    }

    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        unsafe { System.dealloc(pointer, layout) }
    }
}

#[global_allocator]
static ALLOCATOR: CountingAllocator = CountingAllocator;

#[inline(never)]
fn measure<F>(make: impl FnOnce() -> F) -> (F, usize) {
    ALLOCATIONS.with(|count| {
        assert!(count.get().is_none());
        count.set(Some(0));
    });
    let future = make();
    std::hint::black_box(&future);
    let allocations = ALLOCATIONS.with(|count| count.replace(None).unwrap());
    (future, allocations)
}

#[derive(Clone, Serialize, Deserialize)]
struct TestConfig {
    streams: Vec<RuntimeStreamConfig>,
}

impl Config for TestConfig {
    fn apply_environment(&mut self) -> Result<(), String> {
        Ok(())
    }

    fn streams(&self) -> Vec<RuntimeStreamConfig> {
        self.streams.clone()
    }
}

struct PureMap;

impl MapFunction<u64, u64> for PureMap {
    async fn map(
        &self,
        _context: MessageContext,
        _stream: &dyn RuntimeStream,
        _value: &u64,
        _out: &impl servicelib::runtime::collector::Collect<u64>,
    ) {
    }
}

struct DynamicConsumer;

struct AllocationProbe(Arc<AtomicBool>);

impl MapFunction<u64, u64> for AllocationProbe {
    async fn map(
        &self,
        context: MessageContext,
        stream: &dyn RuntimeStream,
        value: &u64,
        out: &impl servicelib::runtime::collector::Collect<u64>,
    ) {
        let function = PureMap;
        let direct_context = context.clone();
        let (future, allocations) = measure(|| function.map(direct_context, stream, value, out));
        assert_eq!(allocations, 0);
        future.await;

        let shared = Arc::new(PureMap);
        let (future, allocations) = measure(|| shared.map(context, stream, value, out));
        assert_eq!(allocations, 0);
        future.await;
        self.0.store(true, Ordering::SeqCst);
    }
}

impl Consumer<u64> for DynamicConsumer {
    async fn consume(&self, _context: MessageContext, _payload: Payload<u64>) {}
}

#[tokio::test(flavor = "current_thread")]
async fn concrete_and_shared_business_futures_do_not_allocate() {
    let config = StreamConfig::new(1, "Allocation probe");
    let mut output_config = StreamConfig::new(2, "Measured map");
    output_config.id_source = config.id;
    let output_config = MapStreamConfig::from(output_config);
    let environment = RuntimeEnvironment::default();
    environment.publish_runtime_config(Arc::new(
        RuntimeConfig::new(&TestConfig {
            streams: vec![
                MapStreamConfig::from(config.clone()).into(),
                output_config.clone().into(),
            ],
        })
        .unwrap(),
    ));
    let stream = Stream::<u64>::new(&config, environment.clone());
    let measured = Arc::new(AtomicBool::new(false));
    let _output = stream
        .map(&output_config, AllocationProbe(measured.clone()))
        .unwrap();
    environment.build_runtime_streams().unwrap();
    stream.emit(MessageContext::new(), Payload::new(1)).await;
    assert!(measured.load(Ordering::SeqCst));

    // The public Consumer contract itself no longer introduces a box.
    let consumer = DynamicConsumer;
    let context = MessageContext::new();
    let payload = Payload::new(1);
    let (future, consumer_allocations) = measure(|| consumer.consume(context, payload));
    assert_eq!(consumer_allocations, 0);
    future.await;
}
