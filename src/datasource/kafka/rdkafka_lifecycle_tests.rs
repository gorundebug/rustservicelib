use super::*;
use crate::{
    api::{KafkaSaslMechanism, KafkaSecurityProtocol},
    runtime::config::{InputStreamConfig, StreamConfig},
};
use tokio::sync::{Semaphore, oneshot};

struct EndpointProbe {
    id: i32,
    fail_start: bool,
    fail_stop: AtomicBool,
    stops: AtomicUsize,
    start_gate: Option<Arc<Semaphore>>,
    entered: Notify,
}

impl EndpointProbe {
    fn new(id: i32) -> Arc<Self> {
        Arc::new(Self {
            id,
            fail_start: false,
            fail_stop: AtomicBool::new(false),
            stops: AtomicUsize::new(0),
            start_gate: None,
            entered: Notify::new(),
        })
    }
}

#[async_trait]
impl Lifecycle for EndpointProbe {
    async fn start(&self, context: MessageContext) -> RuntimeResult<()> {
        assert_eq!(context.stream_id(), Some("lifecycle-owner"));
        self.entered.notify_one();
        if let Some(gate) = &self.start_gate {
            let _permit = gate.acquire().await.unwrap();
        }
        if self.fail_start {
            Err(RuntimeError::Transport("primary startup failure".into()))
        } else {
            Ok(())
        }
    }

    async fn stop(&self, context: MessageContext) -> RuntimeResult<()> {
        assert_eq!(context.stream_id(), Some("lifecycle-owner"));
        self.stops.fetch_add(1, Ordering::SeqCst);
        if self.fail_stop.load(Ordering::SeqCst) {
            Err(RuntimeError::Transport("cleanup failure".into()))
        } else {
            Ok(())
        }
    }
}

#[async_trait]
impl KafkaInputEndpoint for EndpointProbe {
    fn id(&self) -> i32 { self.id }
}

fn owner() -> MessageContext {
    MessageContext::new().with_stream_id("lifecycle-owner")
}

fn datasource(endpoints: &[Arc<EndpointProbe>]) -> Arc<RdkafkaKafkaDataSource> {
    let source = RdkafkaKafkaDataSource::new(RuntimeEnvironment::default(), 1, "probe".into());
    for endpoint in endpoints {
        source.endpoints.lock().unwrap().push(endpoint.clone());
    }
    source
}

#[tokio::test]
async fn failed_stop_retains_only_unfinished_endpoints_for_retry() {
    let first = EndpointProbe::new(1);
    let second = EndpointProbe::new(2);
    let source = datasource(&[first.clone(), second.clone()]);
    source.start(owner()).await.unwrap();
    first.fail_stop.store(true, Ordering::SeqCst);
    assert!(source.stop(owner()).await.is_err());
    assert!(source.stop(owner()).await.is_err());
    assert_eq!(first.stops.load(Ordering::SeqCst), 2);
    assert_eq!(second.stops.load(Ordering::SeqCst), 1);
    first.fail_stop.store(false, Ordering::SeqCst);
    source.stop(owner()).await.unwrap();
    source.stop(owner()).await.unwrap();
    assert_eq!(first.stops.load(Ordering::SeqCst), 3);
    assert!(source.active_endpoints.lock().unwrap().is_empty());
}

#[tokio::test]
async fn startup_rollback_retains_failing_endpoint_and_primary_error() {
    let first = EndpointProbe::new(1);
    let mut second = EndpointProbe::new(2);
    Arc::get_mut(&mut second).unwrap().fail_start = true;
    second.fail_stop.store(true, Ordering::SeqCst);
    let source = datasource(&[first.clone(), second.clone()]);
    let error = source.start(owner()).await.unwrap_err();
    assert_eq!(error.to_string(), "transport error: primary startup failure");
    assert_eq!(first.stops.load(Ordering::SeqCst), 1);
    assert_eq!(second.stops.load(Ordering::SeqCst), 1);
    assert!(matches!(source.start(owner()).await, Err(RuntimeError::ResourceStopped(_))));
    assert!(source.stop(owner()).await.is_err());
    second.fail_stop.store(false, Ordering::SeqCst);
    source.stop(owner()).await.unwrap();
    assert_eq!(first.stops.load(Ordering::SeqCst), 1);
    assert_eq!(second.stops.load(Ordering::SeqCst), 3);
}

#[tokio::test]
async fn concurrent_stop_respects_its_context_and_dropped_start_remains_owned() {
    let mut endpoint = EndpointProbe::new(1);
    Arc::get_mut(&mut endpoint).unwrap().start_gate = Some(Arc::new(Semaphore::new(0)));
    let source = datasource(&[endpoint.clone()]);
    let starting = source.start(owner());
    tokio::pin!(starting);
    tokio::select! {
        result = &mut starting => panic!("startup unexpectedly finished: {result:?}"),
        () = endpoint.entered.notified() => {}
    }
    let cancelled = owner();
    cancelled.cancel();
    assert!(matches!(source.stop(cancelled).await, Err(RuntimeError::ContextCancelled)));
    assert_eq!(endpoint.stops.load(Ordering::SeqCst), 0);
    // Drop the actual pinned startup future, not merely its Pin reference.
    drop(starting);
    // Scope-based cancellation is covered separately below; release this start.
    endpoint.start_gate.as_ref().unwrap().add_permits(1);
}

#[tokio::test]
async fn abandoned_start_can_be_cleaned_up() {
    let mut endpoint = EndpointProbe::new(1);
    Arc::get_mut(&mut endpoint).unwrap().start_gate = Some(Arc::new(Semaphore::new(0)));
    let source = datasource(&[endpoint.clone()]);
    {
        let starting = source.start(owner());
        tokio::pin!(starting);
        tokio::select! {
            result = &mut starting => panic!("startup unexpectedly finished: {result:?}"),
            () = endpoint.entered.notified() => {}
        }
    }
    source.stop(owner()).await.unwrap();
    assert_eq!(endpoint.stops.load(Ordering::SeqCst), 1);
    assert!(source.active_endpoints.lock().unwrap().is_empty());
}

struct Handler;
#[async_trait]
impl EndpointHandler<(), u32, u32, String> for Handler {
    async fn on_error(&self, context: MessageContext, stream: StreamContext<u32, u32, String>, error: HandlerError) {
        stream.error_collect(context, error.to_string()).await;
    }
    fn concurrency(&self, _: &StreamContext<u32, u32, String>) -> usize { 1 }
    async fn begin_request(&self, context: MessageContext, _: StreamContext<u32, u32, String>)
        -> Result<(MessageContext, ()), HandlerError> { Ok((context, ())) }
    async fn consume_message(&self, _: MessageContext, _: StreamContext<u32, u32, String>,
        _: Arc<AsyncMutex<()>>, _: ConsumerMessage, _: Arc<ResultContext<(), u32, u32, String>>)
        -> HandlerResult { Ok(()) }
    async fn get_message_id(&self, _: &MessageContext, _: &StreamContext<u32, u32, String>,
        _: Arc<AsyncMutex<()>>, _: &u32) -> String { String::new() }
    async fn end_request(&self, _: MessageContext, _: StreamContext<u32, u32, String>,
        _: &HandlerResult, _: Arc<AsyncMutex<()>>) {}
}

fn endpoint() -> Arc<RdkafkaKafkaEndpoint<(), u32, u32, String, Handler>> {
    let environment = RuntimeEnvironment::default();
    let input = InputStream::new(&InputStreamConfig {
        stream: StreamConfig::new(1, "input"), endpoint_id: 2,
    }, environment.clone());
    let config = KafkaEndpointConfig {
        enabled: true, id: 2, name: "topic".into(), id_data_connector: 4,
        tracing_enabled: false, create_topic: false, topic: "topic".into(),
        partitions: 1, consumer_group: "group".into(), replication_factor: 1,
    };
    let consumer = make_rdkafka_kafka_endpoint_consumer(input, config.clone(),
        KafkaDataConnectorConfig {
            id: 4, name: "connector".into(), brokers: "localhost:9092".into(),
            version: String::new(), dial_timeout: 0.0, use_partitioner: false,
            r#async: true, security_protocol: KafkaSecurityProtocol::PLAINTEXT,
            sasl_mechanism: KafkaSaslMechanism::PLAIN, username: String::new(), password: String::new(),
        }, Handler).unwrap();
    RdkafkaKafkaEndpoint::new(environment, config, consumer).unwrap()
}

#[tokio::test]
async fn interrupted_endpoint_stop_retains_task_and_does_not_cancel_parent() {
    for deadline in [false, true] {
        let endpoint = endpoint();
        let parent = owner();
        let child = parent.child();
        *endpoint.context.lock().await = Some(child.clone());
        let (release, wait) = oneshot::channel();
        let held = Arc::new(());
        let weak = Arc::downgrade(&held);
        *endpoint.task.lock().await = Some(tokio::spawn(async move {
            let _held = held;
            let _ = wait.await;
        }));
        let shutdown = if deadline {
            parent.clone().with_timeout_limit(Duration::from_millis(10))
        } else {
            let context = parent.child(); context.cancel(); context
        };
        assert!(matches!(endpoint.stop(shutdown).await, Err(RuntimeError::ContextCancelled)));
        assert!(endpoint.task.lock().await.is_some());
        assert!(weak.upgrade().is_some());
        assert!(child.is_cancelled());
        assert!(!parent.is_cancelled());
        let retry = parent.child(); retry.cancel();
        assert!(matches!(endpoint.stop(retry).await, Err(RuntimeError::ContextCancelled)));
        release.send(()).unwrap();
        endpoint.stop(parent.clone()).await.unwrap();
        assert!(endpoint.task.lock().await.is_none());
        assert!(weak.upgrade().is_none());
        assert!(!parent.is_cancelled());
    }
}

#[tokio::test]
async fn concurrent_and_abandoned_endpoint_waits_do_not_lose_task() {
    let endpoint = endpoint();
    let (release, wait) = oneshot::channel();
    *endpoint.task.lock().await = Some(tokio::spawn(async move { let _ = wait.await; }));
    {
        let stopping = endpoint.stop(owner());
        tokio::pin!(stopping);
        tokio::select! {
            result = &mut stopping => panic!("shutdown unexpectedly finished: {result:?}"),
            () = tokio::task::yield_now() => {}
        }
        let cancelled = owner(); cancelled.cancel();
        assert!(matches!(endpoint.stop(cancelled).await, Err(RuntimeError::ContextCancelled)));
    }
    assert!(endpoint.task.lock().await.is_some());
    release.send(()).unwrap();
    endpoint.stop(owner()).await.unwrap();
}

#[tokio::test]
async fn endpoint_task_panic_is_not_reported_as_success() {
    let endpoint = endpoint();
    *endpoint.task.lock().await = Some(tokio::spawn(async { panic!("handler task panic") }));
    assert!(matches!(endpoint.stop(owner()).await, Err(RuntimeError::Transport(_))));
}
