use super::RuntimeEnvironment;
use crate::{
    MessageContext, Payload,
    operators::DelayFunction,
    runtime::{
        common::{Consumer, RuntimeStream},
        config::{CallSemantics, Config, LinkConfig, RuntimeConfig, RuntimeStreamConfig, StreamConfig},
        stream::Stream,
    },
};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::{sync::{Arc, Mutex}, time::Duration};
use tokio::sync::{Notify, oneshot};

#[derive(Clone, Serialize, Deserialize)]
struct Model {
    streams: Vec<RuntimeStreamConfig>,
    links: Vec<LinkConfig>,
}

impl Config for Model {
    fn apply_environment(&mut self) -> Result<(), String> { Ok(()) }
    fn streams(&self) -> Vec<RuntimeStreamConfig> { self.streams.clone() }
    fn links(&self) -> Vec<LinkConfig> { self.links.clone() }
}

fn environment(middle: RuntimeStreamConfig, links: Vec<LinkConfig>) -> RuntimeEnvironment {
    let model = Model {
        streams: vec![
            RuntimeStreamConfig::Map(StreamConfig::new(1, "Source").into()),
            middle,
            RuntimeStreamConfig::Map(StreamConfig::new(3, "First").into()),
            RuntimeStreamConfig::Map(StreamConfig::new(4, "Second").into()),
        ],
        links,
    };
    let environment = RuntimeEnvironment::new(CallSemantics::FunctionCall);
    environment.publish_runtime_config(Arc::new(RuntimeConfig::new(&model).unwrap()));
    environment
}

struct Branch {
    parallel: bool,
    caller: Option<tokio::task::Id>,
    entered: Arc<Notify>,
    release: Arc<Notify>,
    finished: Mutex<Option<oneshot::Sender<()>>>,
    events: Arc<Mutex<Vec<&'static str>>>,
}

#[async_trait]
impl Consumer<i32> for Branch {
    async fn consume(&self, _context: MessageContext, _payload: Payload<i32>) {
        if self.parallel {
            assert_ne!(tokio::task::try_id(), self.caller);
            self.events.lock().unwrap().push("parallel entered");
            self.entered.notify_one();
            self.release.notified().await;
            self.events.lock().unwrap().push("parallel finished");
            self.finished.lock().unwrap().take().unwrap().send(()).unwrap();
        } else {
            assert_eq!(tokio::task::try_id(), self.caller);
            self.events.lock().unwrap().push("direct entered");
            self.entered.notified().await;
            self.events.lock().unwrap().push("direct finished");
        }
    }
}

#[tokio::test]
async fn split_dispatches_parallel_branch_before_awaiting_direct_branch() {
    let environment = environment(
        RuntimeStreamConfig::Split(StreamConfig::new(2, "Split").into()),
        vec![
            LinkConfig { from: 2, to: 3, call_semantics: CallSemantics::FunctionCall, r#async: false },
            LinkConfig { from: 2, to: 4, call_semantics: CallSemantics::ParallelCall, r#async: false },
        ],
    );
    let source = Stream::<i32>::new(&StreamConfig::new(1, "Source"), environment.clone());
    let [direct, parallel] = source.split(&StreamConfig::new(2, "Split").into()).unwrap();
    let entered = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let events = Arc::new(Mutex::new(Vec::new()));
    let (finished, completed) = oneshot::channel();
    let caller = tokio::task::try_id();
    direct.set_consumer(Arc::new(Branch {
        parallel: false, caller, entered: entered.clone(), release: release.clone(),
        finished: Mutex::new(None), events: events.clone(),
    }), 3);
    parallel.set_consumer(Arc::new(Branch {
        parallel: true, caller, entered, release: release.clone(),
        finished: Mutex::new(Some(finished)), events: events.clone(),
    }), 4);
    environment.build_runtime_streams().unwrap();

    tokio::time::timeout(Duration::from_secs(2), source.emit(MessageContext::new(), Payload::new(42)))
        .await.expect("direct branch must not prevent parallel branch dispatch");
    {
        let events = events.lock().unwrap();
        assert_eq!(events.len(), 3);
        assert!(events.contains(&"direct entered"));
        assert!(events.contains(&"parallel entered"));
        assert!(events.contains(&"direct finished"));
        assert!(!events.contains(&"parallel finished"));
    }
    release.notify_one();
    tokio::time::timeout(Duration::from_secs(2), completed).await.unwrap().unwrap();
    environment.drain_parallel().await;
}

struct Value {
    value: u32,
    dropped: Option<oneshot::Sender<()>>,
}

impl Drop for Value {
    fn drop(&mut self) {
        if let Some(dropped) = self.dropped.take() {
            let _ = dropped.send(());
        }
    }
}

struct Delay(Duration);

impl DelayFunction<Value> for Delay {
    async fn duration(&self, _context: MessageContext, _stream: &dyn RuntimeStream, _value: &Value) -> Duration {
        self.0
    }
}

#[derive(Default)]
struct Values(Mutex<Vec<u32>>);

#[async_trait]
impl Consumer<Value> for Values {
    async fn consume(&self, _context: MessageContext, payload: Payload<Value>) {
        let value = payload.into_arc();
        self.0.lock().unwrap().push(value.value);
    }
}

#[tokio::test(start_paused = true)]
async fn cancelled_delay_releases_payload_and_cannot_emit_after_original_expiry() {
    for cancel in [false, true] {
        let environment = environment(
            RuntimeStreamConfig::Delay(StreamConfig::new(2, "Delay").into()), Vec::new(),
        );
        let source = Stream::<Value>::new(&StreamConfig::new(1, "Source"), environment.clone());
        let delayed = source.delay(&StreamConfig::new(2, "Delay").into(), Delay(Duration::from_secs(60))).unwrap();
        let values = Arc::new(Values::default());
        delayed.set_consumer(values.clone(), 3);
        environment.build_runtime_streams().unwrap();
        let context = MessageContext::new();
        let (dropped, mut drop_observed) = oneshot::channel();

        source.emit(context.clone(), Payload::new(Value { value: 42, dropped: Some(dropped) })).await;
        tokio::task::yield_now().await;
        assert!(matches!(drop_observed.try_recv(), Err(oneshot::error::TryRecvError::Empty)));
        assert!(values.0.lock().unwrap().is_empty());
        if cancel { context.cancel(); }
        tokio::time::advance(Duration::from_secs(59)).await;
        assert!(values.0.lock().unwrap().is_empty());
        tokio::time::advance(Duration::from_secs(2)).await;
        tokio::time::timeout(Duration::from_secs(1), drop_observed).await
            .expect("delay must release its payload after cancellation or expiry")
            .expect("payload destructor must run");
        assert_eq!(*values.0.lock().unwrap(), if cancel { vec![] } else { vec![42] });
        tokio::time::advance(Duration::from_secs(60)).await;
        assert_eq!(*values.0.lock().unwrap(), if cancel { vec![] } else { vec![42] });
    }
}
