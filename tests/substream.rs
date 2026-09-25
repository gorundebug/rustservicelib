use std::{
    sync::{
        Arc, Mutex, OnceLock,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use async_trait::async_trait;
use servicelib::{
    MessageContext, Payload, SubStream, SubStreamCollector, SubStreamCollectorFunc,
    operators::MapFunction,
    runtime::{
        collector::Collector,
        common::RuntimeStream,
        config::{
            CallSemantics, MapStreamConfig, RuntimeConfig, RuntimeStreamConfig, StreamConfig,
            SubStreamConfig,
        },
        environment::{RuntimeEnvironment, RuntimeError},
        stream::Stream,
    },
};
use tokio::sync::Notify;

fn environment() -> (RuntimeEnvironment, SubStreamConfig, MapStreamConfig) {
    let mut entry = StreamConfig::new(1, "Lookup");
    entry.id_source = 2;
    entry.id_service = 1;
    entry.value_type = Some("int32".to_owned());
    let mut output = StreamConfig::new(2, "Result");
    output.id_source = 1;
    output.id_service = 1;
    output.value_type = Some("int32".to_owned());
    let entry = SubStreamConfig::from(entry);
    let output = MapStreamConfig::from(output);
    let environment = RuntimeEnvironment::default();
    environment.publish_runtime_config(Arc::new(
        RuntimeConfig::from_parts(
            CallSemantics::FunctionCall,
            [],
            [
                RuntimeStreamConfig::from(entry.clone()),
                RuntimeStreamConfig::from(output.clone()),
            ],
            [],
            [],
            [],
            [],
        )
        .unwrap(),
    ));
    (environment, entry, output)
}

fn graph<F: MapFunction<i32, i32> + 'static>(function: F) -> (SubStream<i32, i32>, Stream<i32>) {
    let (environment, entry_config, output_config) = environment();
    let entry = SubStream::new(&entry_config, environment.clone());
    let output = entry.stream().map(&output_config, function).unwrap();
    entry.set_source(&output).unwrap();
    environment.build_runtime_streams().unwrap();
    (entry, output)
}

struct Echo;

impl MapFunction<i32, i32> for Echo {
    async fn map(
        &self,
        context: MessageContext,
        _: &dyn RuntimeStream,
        value: &i32,
        out: &Collector<i32>,
    ) {
        tokio::task::yield_now().await;
        out.collect(context, *value).await;
    }
}

#[derive(Default)]
struct Capture(Mutex<Vec<i32>>);

#[async_trait]
impl SubStreamCollector<i32> for Capture {
    async fn out(&self, context: MessageContext, value: Payload<i32>) -> bool {
        assert_eq!(context.stream_id(), Some("parent"));
        self.0.lock().unwrap().push(*value);
        true
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_calls_share_graph_not_results() {
    let (entry, _) = graph(Echo);
    let context = MessageContext::new().with_stream_id("parent");
    let mut tasks = tokio::task::JoinSet::new();
    for value in 0..100 {
        let entry = entry.clone();
        let context = context.clone();
        tasks.spawn(async move {
            let capture = Arc::new(Capture::default());
            entry
                .consume(context, value, capture.clone())
                .await
                .unwrap();
            assert_eq!(*capture.0.lock().unwrap(), vec![value]);
        });
    }
    while let Some(result) = tasks.join_next().await {
        result.unwrap();
    }
    let links = entry.stream().environment().graph_links();
    assert_eq!(links.len(), 2);
    assert!(links.iter().all(|link| link.calls.get() == 100));
}

struct Multiple;

impl MapFunction<i32, i32> for Multiple {
    async fn map(
        &self,
        context: MessageContext,
        _: &dyn RuntimeStream,
        value: &i32,
        out: &Collector<i32>,
    ) {
        for increment in 0..3 {
            out.collect(context.clone(), *value + increment).await;
        }
    }
}

#[tokio::test]
async fn collector_controls_completion_and_later_results_are_dropped() {
    let (entry, _) = graph(Multiple);
    let results = Arc::new(Mutex::new(Vec::new()));
    let collected = results.clone();
    let collector = Arc::new(SubStreamCollectorFunc(
        move |_: MessageContext, value: Payload<i32>| {
            let mut values = collected.lock().unwrap();
            values.push(*value);
            let complete = values.len() == 2;
            async move { complete }
        },
    ));
    entry
        .consume(MessageContext::new(), 40, collector)
        .await
        .unwrap();
    assert_eq!(*results.lock().unwrap(), vec![40, 41]);
}

struct Recurse(Arc<OnceLock<SubStream<i32, i32>>>);

impl MapFunction<i32, i32> for Recurse {
    async fn map(
        &self,
        context: MessageContext,
        _: &dyn RuntimeStream,
        value: &i32,
        out: &Collector<i32>,
    ) {
        let result = if *value > 0 {
            let capture = Arc::new(Capture::default());
            self.0
                .get()
                .unwrap()
                .consume(context.clone(), *value - 1, capture.clone())
                .await
                .unwrap();
            capture.0.lock().unwrap()[0] + 1
        } else {
            0
        };
        out.collect(context, result).await;
    }
}

#[tokio::test]
async fn recursive_calls_of_the_same_entry_restore_outer_results() {
    let holder = Arc::new(OnceLock::new());
    let (entry, _) = graph(Recurse(holder.clone()));
    assert!(holder.set(entry.clone()).is_ok());
    let capture = Arc::new(Capture::default());
    entry
        .consume(
            MessageContext::new().with_stream_id("parent"),
            5,
            capture.clone(),
        )
        .await
        .unwrap();
    assert_eq!(*capture.0.lock().unwrap(), vec![5]);
}

#[tokio::test]
async fn collector_can_call_the_same_substream() {
    let (entry, _) = graph(Echo);
    let nested_entry = entry.clone();
    let collector = Arc::new(SubStreamCollectorFunc(
        move |context: MessageContext, value: Payload<i32>| {
            let entry = nested_entry.clone();
            async move {
                let capture = Arc::new(Capture::default());
                entry
                    .consume(context, *value + 1, capture.clone())
                    .await
                    .unwrap();
                assert_eq!(*capture.0.lock().unwrap(), vec![43]);
                true
            }
        },
    ));
    entry
        .consume(
            MessageContext::new().with_stream_id("parent"),
            42,
            collector,
        )
        .await
        .unwrap();
}

struct Hold(Arc<Mutex<Vec<(i32, MessageContext)>>>, Arc<Notify>);

impl MapFunction<i32, i32> for Hold {
    async fn map(
        &self,
        context: MessageContext,
        _: &dyn RuntimeStream,
        value: &i32,
        _: &Collector<i32>,
    ) {
        self.0.lock().unwrap().push((*value, context));
        self.1.notify_one();
    }
}

#[tokio::test]
async fn cancellation_isolates_siblings_and_releases_retained_callbacks() {
    let contexts = Arc::new(Mutex::new(Vec::new()));
    let entered = Arc::new(Notify::new());
    let (entry, output) = graph(Hold(contexts.clone(), entered.clone()));
    let parent = MessageContext::new().with_stream_id("parent");
    let cancelled = parent.child();
    let capture1 = Arc::new(Capture::default());
    let retained = Arc::downgrade(&capture1);
    let first = tokio::spawn({
        let entry = entry.clone();
        let context = cancelled.clone();
        async move { entry.consume(context, 1, capture1).await }
    });
    entered.notified().await;
    let capture2 = Arc::new(Capture::default());
    let second = tokio::spawn({
        let entry = entry.clone();
        let capture = capture2.clone();
        async move { entry.consume(parent, 2, capture).await }
    });
    entered.notified().await;
    cancelled.cancel();
    assert!(matches!(
        first.await.unwrap(),
        Err(RuntimeError::ContextCancelled)
    ));
    assert!(retained.upgrade().is_none());
    let contexts = contexts.lock().unwrap().clone();
    for (value, context) in contexts {
        output.emit(context, Payload::new(value)).await;
    }
    second.await.unwrap().unwrap();
    assert_eq!(*capture2.0.lock().unwrap(), vec![2]);
}

#[tokio::test(start_paused = true)]
async fn deadline_finishes_call_without_results() {
    let (entry, _) = graph(Hold(Arc::default(), Arc::default()));
    let result = entry
        .consume(
            MessageContext::with_timeout(Duration::from_secs(1)),
            1,
            Arc::new(Capture::default()),
        )
        .await;
    assert!(matches!(result, Err(RuntimeError::ContextCancelled)));
}

struct BlockingCollector {
    entered: Arc<Notify>,
    release: Arc<Notify>,
    completed: Arc<AtomicUsize>,
    accept: bool,
}

#[tokio::test]
async fn completed_collector_wins_over_cancellation() {
    let (entry, _) = graph(Echo);
    let collector = Arc::new(SubStreamCollectorFunc(
        |context: MessageContext, _: Payload<i32>| async move {
            context.cancel();
            true
        },
    ));
    entry
        .consume(MessageContext::new(), 1, collector)
        .await
        .expect("a completed collector must win over cancellation, as in Go");
}

struct CancellationCleanup {
    entered: Arc<Notify>,
    completed: Arc<AtomicUsize>,
    release: Arc<Notify>,
}

impl MapFunction<i32, i32> for CancellationCleanup {
    async fn map(
        &self,
        context: MessageContext,
        _: &dyn RuntimeStream,
        _: &i32,
        _: &Collector<i32>,
    ) {
        self.entered.notify_one();
        context.cancelled().await;
        self.release.notified().await;
        self.completed.fetch_add(1, Ordering::SeqCst);
    }
}

#[tokio::test]
async fn cancellation_does_not_drop_the_direct_business_call() {
    let entered = Arc::new(Notify::new());
    let completed = Arc::new(AtomicUsize::new(0));
    let release = Arc::new(Notify::new());
    let (entry, _) = graph(CancellationCleanup {
        entered: entered.clone(),
        completed: completed.clone(),
        release: release.clone(),
    });
    let context = MessageContext::new();
    let mut task = tokio::spawn({
        let context = context.clone();
        async move {
            entry.consume(context, 1, Arc::new(Capture::default())).await
        }
    });
    entered.notified().await;
    context.cancel();
    assert!(
        tokio::time::timeout(Duration::from_millis(20), &mut task)
            .await
            .is_err(),
        "Consume returned before direct business cleanup finished"
    );
    release.notify_one();
    assert!(matches!(task.await.unwrap(), Err(RuntimeError::ContextCancelled)));
    assert_eq!(completed.load(Ordering::SeqCst), 1);
}

#[async_trait]
impl SubStreamCollector<i32> for BlockingCollector {
    async fn out(&self, _: MessageContext, _: Payload<i32>) -> bool {
        self.entered.notify_one();
        self.release.notified().await;
        self.completed.fetch_add(1, Ordering::SeqCst);
        self.accept
    }
}

#[tokio::test]
async fn cancellation_drains_an_active_direct_collector() {
    for accept in [false, true] {
    let (entry, _) = graph(Echo);
    let entered = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let completed = Arc::new(AtomicUsize::new(0));
    let context = MessageContext::new();
    let task = tokio::spawn({
        let context = context.clone();
        let collector = Arc::new(BlockingCollector {
            entered: entered.clone(),
            release: release.clone(),
            completed: completed.clone(),
            accept,
        });
        async move { entry.consume(context, 1, collector).await }
    });
    entered.notified().await;
    context.cancel();
    for _ in 0..3 {
        tokio::task::yield_now().await;
    }
    assert!(!task.is_finished());
    release.notify_one();
    let result = task.await.unwrap();
    if accept {
        result.expect("a completed collector wins over cancellation");
    } else {
        assert!(matches!(result, Err(RuntimeError::ContextCancelled)));
    }
    assert_eq!(completed.load(Ordering::SeqCst), 1);
    }
}

#[tokio::test]
async fn aborted_caller_drops_callback_even_if_body_keeps_context() {
    let entered = Arc::new(Notify::new());
    let contexts = Arc::new(Mutex::new(Vec::new()));
    let (entry, output) = graph(Hold(contexts.clone(), entered.clone()));
    let capture = Arc::new(Capture::default());
    let retained = Arc::downgrade(&capture);
    let task = tokio::spawn(async move {
        entry
            .consume(MessageContext::new().with_stream_id("parent"), 1, capture)
            .await
    });
    entered.notified().await;
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    assert!(retained.upgrade().is_none());
    let context = contexts.lock().unwrap()[0].1.clone();
    output.emit(context, Payload::new(1)).await;
}

#[tokio::test]
async fn ordinary_context_has_no_substream_recipient() {
    let (_, output) = graph(Echo);
    output.emit(MessageContext::new(), Payload::new(42)).await;
}

#[test]
fn validates_body_result_and_service_ownership() {
    let (environment, entry_config, output_config) = environment();
    let entry = SubStream::<i32, i32>::new(&entry_config, environment.clone());
    assert!(environment.build_runtime_streams().is_err());
    assert!(entry.set_source(entry.stream()).is_err());
    let output = entry.stream().map(&output_config, Echo).unwrap();
    assert!(environment.build_runtime_streams().is_err());
    entry.set_source(&output).unwrap();
    assert!(entry.set_source(&output).is_err());
    environment.build_runtime_streams().unwrap();
    assert_eq!(
        RuntimeStreamConfig::from(entry_config).transformation_type(),
        servicelib::api::TransformationType::SubStream
    );
}

#[tokio::test]
async fn sibling_collectors_overlap_on_one_worker() {
    tokio::time::timeout(Duration::from_secs(5), async {
        let (entry, _) = graph(Echo);
        let parent = MessageContext::new().with_stream_id("parent");
        let entered = Arc::new(tokio::sync::Barrier::new(3));
        let release = tokio_util::sync::CancellationToken::new();
        let mut tasks = tokio::task::JoinSet::new();
        for expected in [10, 20] {
            let entry = entry.clone();
            let parent = parent.clone();
            let entered = entered.clone();
            let release = release.clone();
            tasks.spawn(async move {
                let collector = Arc::new(SubStreamCollectorFunc(
                    move |context: MessageContext, value: Payload<i32>| {
                        let entered = entered.clone();
                        let release = release.clone();
                        async move {
                            assert_eq!(context.stream_id(), Some("parent"));
                            assert_eq!(*value, expected);
                            entered.wait().await;
                            release.cancelled().await;
                            true
                        }
                    },
                ));
                entry.consume(parent, expected, collector).await.unwrap();
            });
        }
        entered.wait().await;
        assert!(tasks.try_join_next().is_none());
        release.cancel();
        while let Some(result) = tasks.join_next().await {
            result.unwrap();
        }
    })
    .await
    .expect("independent collectors must not serialize or block the worker");
}

#[tokio::test]
async fn concurrent_results_serialize_one_collector_and_drop_late_values() {
    tokio::time::timeout(Duration::from_secs(5), async {
        let contexts = Arc::new(Mutex::new(Vec::new()));
        let entered = Arc::new(Notify::new());
        let (entry, output) = graph(Hold(contexts.clone(), entered.clone()));
        let active = Arc::new(AtomicUsize::new(0));
        let count = Arc::new(AtomicUsize::new(0));
        let collector = Arc::new(SubStreamCollectorFunc({
            let active = active.clone();
            let count = count.clone();
            move |_: MessageContext, _: Payload<i32>| {
                let active = active.clone();
                let count = count.clone();
                async move {
                    assert_eq!(active.fetch_add(1, Ordering::SeqCst), 0);
                    tokio::task::yield_now().await;
                    let complete = count.fetch_add(1, Ordering::SeqCst) + 1 == 50;
                    assert_eq!(active.fetch_sub(1, Ordering::SeqCst), 1);
                    complete
                }
            }
        }));
        let retained = Arc::downgrade(&collector);
        let call = tokio::spawn(async move {
            entry.consume(MessageContext::new(), 0, collector).await
        });
        entered.notified().await;
        let context = contexts.lock().unwrap()[0].1.clone();
        let mut deliveries = tokio::task::JoinSet::new();
        for value in 0..100 {
            let output = output.clone();
            let context = context.clone();
            deliveries.spawn(async move { output.emit(context, Payload::new(value)).await });
        }
        call.await.unwrap().unwrap();
        while let Some(result) = deliveries.join_next().await {
            result.unwrap();
        }
        output.emit(context, Payload::new(101)).await;
        assert_eq!(count.load(Ordering::SeqCst), 50);
        assert_eq!(active.load(Ordering::SeqCst), 0);
        assert!(retained.upgrade().is_none());
    })
    .await
    .expect("result delivery must remain cooperative on one worker");
}

struct NestedForkJoin(SubStream<i32, i32>);

impl MapFunction<i32, i32> for NestedForkJoin {
    async fn map(
        &self,
        context: MessageContext,
        _: &dyn RuntimeStream,
        _: &i32,
        out: &Collector<i32>,
    ) {
        let invoke = |index: i32| {
            let context = context.clone();
            async move {
                let values = Arc::new(Mutex::new(Vec::new()));
                let collector = Arc::new(SubStreamCollectorFunc({
                    let values = values.clone();
                    move |context: MessageContext, value: Payload<i32>| {
                        assert_eq!(context.stream_id(), Some("parent"));
                        let mut values = values.lock().unwrap();
                        values.push(*value);
                        let complete = values.len() == 2;
                        async move { complete }
                    }
                }));
                self.0.consume(context, index, collector).await.unwrap();
                let values = values.lock().unwrap();
                assert_eq!(*values, vec![(index + 1) * 100 + 1, (index + 1) * 100 + 2]);
                values.iter().sum::<i32>()
            }
        };
        let (first, second) = tokio::join!(invoke(0), invoke(1));
        out.collect(context, first + second).await;
    }
}

#[tokio::test]
async fn nested_fork_join_accepts_delayed_interleaved_results_on_one_worker() {
    tokio::time::timeout(Duration::from_secs(5), async {
        for order in [[0, 0, 0, 1, 1, 1], [1, 1, 1, 0, 0, 0], [0, 1, 1, 1, 0, 0]] {
            let contexts = Arc::new(Mutex::new(Vec::new()));
            let entered = Arc::new(Notify::new());
            let (inner, output) = graph(Hold(contexts.clone(), entered.clone()));
            let (outer, _) = graph(NestedForkJoin(inner));
            let capture = Arc::new(Capture::default());
            let result = capture.clone();
            let call = tokio::spawn(async move {
                outer.consume(MessageContext::new().with_stream_id("parent"), 0, result).await
            });
            while contexts.lock().unwrap().len() < 2 {
                entered.notified().await;
            }
            let mut jobs = contexts.lock().unwrap().clone();
            jobs.sort_by_key(|(index, _)| *index);
            assert!(!call.is_finished());
            let mut sequence = [0, 0];
            for index in order {
                tokio::time::sleep(Duration::from_millis(1)).await;
                sequence[index] += 1;
                output.emit(jobs[index].1.clone(), Payload::new((index as i32 + 1) * 100 + sequence[index])).await;
            }
            call.await.unwrap().unwrap();
            assert_eq!(*capture.0.lock().unwrap(), vec![606]);
        }
    })
    .await
    .expect("nested waiting must allow deferred results to run on one worker");
}
