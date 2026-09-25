use std::{sync::{Arc, atomic::{AtomicUsize, Ordering}}, time::Duration};

use async_trait::async_trait;
use servicelib::{MessageContext, Payload, operators::ProcessFunction, runtime::{
    collector::Collector,
    common::{Consumer, RuntimeStream},
    config::{CallSemantics, LinkConfig, MapStreamConfig, ProcessStreamConfig, RuntimeConfig, RuntimeStreamConfig, StreamConfig},
    environment::RuntimeEnvironment,
    stream::Stream,
}};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

struct Failure {
    item: i32,
    identity: Arc<()>,
    dropped: Arc<AtomicUsize>,
}

impl Drop for Failure {
    fn drop(&mut self) { self.dropped.fetch_add(1, Ordering::SeqCst); }
}

struct Route { identity: Arc<()>, dropped: Arc<AtomicUsize> }

#[async_trait]
impl ProcessFunction<i32, i32, Failure> for Route {
    async fn process(&self, context: MessageContext, _: &dyn RuntimeStream, value: &i32,
        out: &Collector<i32>, error: &Collector<Failure>) {
        error.collect(context.clone(), Failure {
            item: *value,
            identity: self.identity.clone(),
            dropped: self.dropped.clone(),
        }).await;
        out.collect(context, *value + 1).await;
    }
}

struct ErrorObserver {
    entered: CancellationToken,
    release: CancellationToken,
    results: mpsc::UnboundedSender<(MessageContext, Payload<Failure>)>,
}

#[async_trait]
impl Consumer<Failure> for ErrorObserver {
    async fn consume(&self, context: MessageContext, payload: Payload<Failure>) {
        self.entered.cancel();
        self.release.cancelled().await;
        self.results.send((context, payload)).unwrap_or_else(|_| panic!("error observer closed"));
    }
}

struct SuccessObserver(mpsc::UnboundedSender<(MessageContext, i32)>);

#[async_trait]
impl Consumer<i32> for SuccessObserver {
    async fn consume(&self, context: MessageContext, payload: Payload<i32>) {
        self.0.send((context, *payload)).unwrap();
    }
}

#[tokio::test]
async fn error_branch_obeys_its_link_mode_and_keeps_typed_failure() {
    tokio::time::timeout(Duration::from_secs(10), async {
        for mode in 0..3 {
            for cancel in [false, true] {
                let root_config = MapStreamConfig::from(StreamConfig::new(1, "Root"));
                let mut process_config = StreamConfig::new(2, "Process");
                process_config.id_source = 1;
                let process_config = ProcessStreamConfig::from(process_config);
                let mut success_config = StreamConfig::new(3, "Success");
                success_config.id_source = 2;
                let mut failure_config = StreamConfig::new(4, "Failure");
                failure_config.id_source = -2;
                let environment = RuntimeEnvironment::default();
                environment.publish_runtime_config(Arc::new(RuntimeConfig::from_parts(
                    CallSemantics::FunctionCall, [],
                    [RuntimeStreamConfig::from(root_config.clone()), RuntimeStreamConfig::from(process_config.clone()),
                        RuntimeStreamConfig::from(MapStreamConfig::from(success_config)),
                        RuntimeStreamConfig::from(MapStreamConfig::from(failure_config))],
                    [], [], [],
                    [LinkConfig { from: -2, to: 4, call_semantics: if mode == 2 { CallSemantics::ParallelCall } else { CallSemantics::FunctionCall }, r#async: mode == 1 }],
                ).unwrap()));
                let root = Stream::new(&root_config.stream, environment.clone());
                let identity = Arc::new(());
                let dropped = Arc::new(AtomicUsize::new(0));
                let (success, error) = root.process(&process_config, Route { identity: identity.clone(), dropped: dropped.clone() }).unwrap();
                assert_eq!(error.id(), -2);
                assert!(error.get_serde().is_stub(), "typed failures need no codec on local links");
                let entered = CancellationToken::new();
                let release = CancellationToken::new();
                let (error_tx, mut errors) = mpsc::unbounded_channel();
                let (success_tx, mut successes) = mpsc::unbounded_channel();
                error.set_consumer(Arc::new(ErrorObserver { entered: entered.clone(), release: release.clone(), results: error_tx }), 4);
                success.set_consumer(Arc::new(SuccessObserver(success_tx)), 3);
                environment.build_runtime_streams().unwrap();
                let context = MessageContext::new().with_stream_id("parent");
                let caller_context = context.clone();
                let mut call = tokio::spawn(async move { root.emit(caller_context, Payload::new(7)).await; });
                entered.cancelled().await;
                if mode == 2 {
                    (&mut call).await.unwrap();
                } else {
                    assert!(tokio::time::timeout(Duration::from_millis(10), &mut call).await.is_err());
                    assert!(successes.try_recv().is_err(), "direct error callback must finish before next output");
                }
                assert_eq!(dropped.load(Ordering::SeqCst), 0);
                if cancel { context.cancel(); }
                release.cancel();
                if mode != 2 { call.await.unwrap(); }
                let (error_context, failure) = errors.recv().await.unwrap();
                let (success_context, value) = successes.recv().await.unwrap();
                assert_eq!(error_context.stream_id(), Some("parent"));
                assert_eq!(success_context.stream_id(), Some("parent"));
                assert_eq!(error_context.is_cancelled(), cancel);
                assert_eq!(failure.item, 7);
                assert!(Arc::ptr_eq(&failure.identity, &identity));
                assert_eq!(value, 8);
                drop(failure);
                assert_eq!(dropped.load(Ordering::SeqCst), 1);
            }
        }
    }).await.expect("typed error delivery must preserve direct/parallel link semantics");
}
