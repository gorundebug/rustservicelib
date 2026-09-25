use std::{
    alloc::{GlobalAlloc, Layout, System},
    cell::Cell,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

use servicelib::{
    MessageContext, Payload,
    operators::{
        flatmapiterable::FlatMapIterableStream,
        keyby::{KeyByFunction, KeyByStream},
    },
    runtime::{
        collector::Collect,
        common::{Consumer, RuntimeStream},
        config::{
            CallSemantics, FlatMapIterableStreamConfig, KeyByStreamConfig, MapStreamConfig,
            RuntimeConfig, RuntimeStreamConfig, StreamConfig,
        },
        datastruct::KeyValue,
        environment::RuntimeEnvironment,
        serde::make_stream_key_value_serde,
        stream::Stream,
    },
};

thread_local! {
    static ALLOCATIONS: Cell<Option<AllocationStats>> = const { Cell::new(None) };
}

#[derive(Clone, Copy, Debug, Default)]
struct AllocationStats {
    calls: usize,
    bytes: usize,
    largest: usize,
}

struct CountingAllocator;

fn record_allocation(bytes: usize) {
    let _ = ALLOCATIONS.try_with(|count| {
        if let Some(mut value) = count.get() {
            value.calls += 1;
            value.bytes += bytes;
            value.largest = value.largest.max(bytes);
            count.set(Some(value));
        }
    });
}

unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        record_allocation(layout.size());
        unsafe { System.alloc(layout) }
    }
    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        record_allocation(layout.size());
        unsafe { System.alloc_zeroed(layout) }
    }
    unsafe fn realloc(&self, pointer: *mut u8, layout: Layout, size: usize) -> *mut u8 {
        record_allocation(size);
        unsafe { System.realloc(pointer, layout, size) }
    }
    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        unsafe { System.dealloc(pointer, layout) }
    }
}

#[global_allocator]
static ALLOCATOR: CountingAllocator = CountingAllocator;

// There is no task spawn or suspension in the measured FunctionCall path.
// Thread-local counting excludes allocations by other tests/runtime workers.
struct Measurement;

impl Measurement {
    fn begin() -> Self {
        ALLOCATIONS.with(|count| count.set(Some(AllocationStats::default())));
        Self
    }
    fn finish(self) -> AllocationStats {
        ALLOCATIONS.with(|count| count.replace(None).unwrap())
    }
}

impl Drop for Measurement {
    fn drop(&mut self) {
        ALLOCATIONS.with(|count| count.set(None));
    }
}

struct Key;

impl KeyByFunction<u32, u32, u32> for Key {
    async fn key_by(
        &self,
        context: MessageContext,
        _: &dyn RuntimeStream,
        value: &u32,
        out: &impl Collect<KeyValue<u32, u32>>,
    ) {
        out.collect(
            context,
            KeyValue {
                key: *value,
                value: *value,
            },
        )
        .await;
    }
}

struct Capture(Arc<AtomicUsize>);

impl Consumer<KeyValue<u32, u32>> for Capture {
    async fn consume(&self, _: MessageContext, value: Payload<KeyValue<u32, u32>>) {
        self.0.fetch_add(value.value as usize, Ordering::Relaxed);
    }
}

fn config(id: i32, source: i32) -> StreamConfig {
    let mut config = StreamConfig::new(id, format!("Node{id}"));
    config.id_service = 1;
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
                RuntimeStreamConfig::from(FlatMapIterableStreamConfig::from(config(2, 1))),
                RuntimeStreamConfig::from(KeyByStreamConfig::from(config(3, 2))),
                RuntimeStreamConfig::from(MapStreamConfig::from(config(4, 3))),
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

#[tokio::test(flavor = "current_thread")]
async fn typed_chain_has_no_per_node_future_allocations() {
    const CALLS: usize = 100;
    let environment = environment();
    let root = Stream::<[u32; 2]>::new(&config(1, 0), environment.clone());
    let items = Stream::<u32>::new(&config(2, 1), environment.clone());
    let keyed = Stream::derived(
        &config(3, 2),
        environment.clone(),
        make_stream_key_value_serde::<u32, u32>(environment.make_serde(), environment.make_serde()),
    );
    let total = Arc::new(AtomicUsize::new(0));
    let output = keyed
        .try_set_typed_consumer(Arc::new(Capture(total.clone())), 4)
        .unwrap();
    let output = items
        .try_set_typed_consumer(Arc::new(KeyByStream::from_collector(output, Key)), 3)
        .unwrap();
    let input = root
        .try_set_typed_consumer(
            Arc::new(FlatMapIterableStream::<[u32; 2], u32>::from_collector(
                output,
            )),
            2,
        )
        .unwrap();
    environment.build_runtime_streams().unwrap();
    let context = MessageContext::default();
    let typed_future_size = std::mem::size_of_val(&input.collect(context.clone(), [1, 2]));
    let public_future_size =
        std::mem::size_of_val(&root.emit(context.clone(), Payload::new([1, 2])));
    input.collect(context.clone(), [1, 2]).await;
    root.emit(context.clone(), Payload::new([1, 2])).await;

    let measurement = Measurement::begin();
    for _ in 0..CALLS {
        input.collect(context.clone(), [1, 2]).await;
    }
    let typed_allocations = measurement.finish();

    let measurement = Measurement::begin();
    for _ in 0..CALLS {
        root.emit(context.clone(), Payload::new([1, 2])).await;
    }
    let public_entry_allocations = measurement.finish();

    eprintln!(
        "typed chain: future={typed_future_size} public_future={public_future_size} calls={CALLS} typed={typed_allocations:?} public={public_entry_allocations:?}"
    );
    assert_eq!(
        typed_allocations.calls, 0,
        "the typed path must not box each operator future"
    );
    assert_eq!(
        public_entry_allocations.calls, CALLS,
        "only the erased public Stream entry boxes a future"
    );
    assert_eq!(total.load(Ordering::Relaxed), (CALLS * 2 + 2) * 3);
    environment.delay_pool().stop().await;
}

#[tokio::test(flavor = "current_thread")]
async fn dynamic_control_counts_each_operator_boundary() {
    const CALLS: usize = 100;
    let environment = environment();
    let root = Stream::<[u32; 2]>::new(&config(1, 0), environment.clone());
    let items = root
        .flat_map_iterable::<u32>(&FlatMapIterableStreamConfig::from(config(2, 1)))
        .unwrap();
    let keyed = items
        .key_by(&KeyByStreamConfig::from(config(3, 2)), Key)
        .unwrap();
    let total = Arc::new(AtomicUsize::new(0));
    keyed
        .try_set_consumer(Arc::new(Capture(total.clone())), 4)
        .unwrap();
    environment.build_runtime_streams().unwrap();
    let context = MessageContext::default();
    root.emit(context.clone(), Payload::new([1, 2])).await;

    let measurement = Measurement::begin();
    for _ in 0..CALLS {
        root.emit(context.clone(), Payload::new([1, 2])).await;
    }
    let allocations = measurement.finish();

    // One iterable call, two KeyBy calls, and two terminal calls per array.
    eprintln!("dynamic control: calls={CALLS} allocations={allocations:?}");
    assert_eq!(allocations.calls, CALLS * 5);
    assert_eq!(total.load(Ordering::Relaxed), (CALLS + 1) * 3);
    environment.delay_pool().stop().await;
}
