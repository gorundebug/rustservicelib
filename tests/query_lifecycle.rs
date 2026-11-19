use std::sync::{Arc, atomic::{AtomicUsize, Ordering}};
use async_trait::async_trait;
use servicelib::{dataquery::{self, BeginRequest, Collector, EndpointHandler, HandlerError, HandlerResult, Operation},
    operators::QueryStream, runtime::{
    common::{Consumer, MessageContext, Payload, RuntimeStream},
    config::{QueryStreamConfig, StreamConfig}, environment::{Lifecycle, RuntimeEnvironment}}};
use tokio::sync::Notify;

#[derive(Default)]
struct Probe {
    entered: Notify,
    finalizing: Notify,
    release: Notify,
    ends: AtomicUsize,
}
struct Handler(Arc<Probe>);
#[async_trait]
impl EndpointHandler<(), (), ()> for Handler {
    async fn begin_request(&self, context: MessageContext, _: &dyn RuntimeStream)
        -> Result<BeginRequest<()>, HandlerError> {
        Ok(BeginRequest { context, state: (), error: None })
    }
    async fn end_request(&self, context: MessageContext, _: &dyn RuntimeStream,
        state: Option<()>, result: &HandlerResult, _: &Collector<()>, _: &Collector<()>) {
        assert!(context.is_cancelled());
        assert_eq!(state, Some(()));
        assert!(result.is_err());
        self.0.finalizing.notify_one();
        self.0.release.notified().await;
        self.0.ends.fetch_add(1, Ordering::SeqCst);
    }
}
struct WaitForCancellation(Arc<Probe>);
#[async_trait]
impl Operation<(), (), (), Handler> for WaitForCancellation {
    async fn execute(&self, context: MessageContext, _: &mut (), _: Payload<()>,
        _: &Handler, _: &Collector<()>) -> HandlerResult {
        self.0.entered.notify_one();
        context.cancelled().await;
        Err(std::io::Error::new(std::io::ErrorKind::Interrupted, "cancelled").into())
    }
}

#[tokio::test]
async fn abandoned_query_finishes_once_and_shutdown_waits_without_cancelling_parent() {
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        let stream = QueryStream::<(), (), ()>::new(&QueryStreamConfig {
            stream: StreamConfig::new(1, "query"), endpoint_id: 1,
        }, RuntimeEnvironment::with_metrics(servicelib::runtime::config::CallSemantics::FunctionCall,
            servicelib::runtime::environment::metrics::Metrics::noop())).unwrap();
        let probe = Arc::new(Probe::default());
        let consumer = dataquery::make_endpoint_consumer(&stream,
            Handler(probe.clone()), WaitForCancellation(probe.clone())).unwrap();
        let parent = MessageContext::new();
        let requests = consumer.requests();
        let context = parent.clone();
        let task = tokio::spawn(async move { consumer.consume(context, Payload::new(())).await });
        probe.entered.notified().await;
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
        probe.finalizing.notified().await;
        assert!(!parent.is_cancelled());
        let stop = requests.stop(parent.clone());
        tokio::pin!(stop);
        assert!(futures::poll!(stop.as_mut()).is_pending());
        assert_eq!(probe.ends.load(Ordering::SeqCst), 0);
        probe.release.notify_one();
        stop.await.unwrap();
        assert_eq!(probe.ends.load(Ordering::SeqCst), 1);
    }).await.unwrap();
}

#[tokio::test]
async fn disabled_query_does_not_invoke_handler_and_reload_enables_it() {
    use servicelib::runtime::config::CallSemantics;
    use servicelib::runtime::config::{RuntimeConfig, RuntimeDataConnectorConfig,
        RuntimeEndpointConfig, PostgreSQLSQLxDataConnectorConfig, PostgreSQLQueryOperation,
        QueryEndpointConfig};
    struct Counter(Arc<AtomicUsize>);
    impl Consumer<()> for Counter {
        async fn consume(&self, _: MessageContext, _: Payload<()>) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }
    fn configuration(enabled: bool) -> RuntimeConfig {
        RuntimeConfig::from_parts(CallSemantics::FunctionCall, [], [], [],
            [RuntimeDataConnectorConfig::PostgreSQLSQLx(PostgreSQLSQLxDataConnectorConfig {
                id: 1, name: "database".into(), connection_string: "postgres://localhost/test".into(),
                connections_count: 1, connect_timeout_ms: 1000, operation_timeout_ms: 0,
            })],
            [RuntimeEndpointConfig::Query(QueryEndpointConfig {
                id: 2, name: "rows".into(), id_data_connector: 1, enabled,
                tracing_enabled: false, query_operation: PostgreSQLQueryOperation::Rows,
                sql_file: None,
            })], []).unwrap()
    }
    let environment = RuntimeEnvironment::default();
    environment.publish_runtime_config(Arc::new(configuration(false)));
    let stream = QueryStream::<(), (), ()>::new(&QueryStreamConfig {
        stream: StreamConfig::new(3, "query"), endpoint_id: 2,
    }, environment.clone()).unwrap();
    let calls = Arc::new(AtomicUsize::new(0));
    stream.set_query_consumer(Arc::new(Counter(Arc::clone(&calls)))).unwrap();
    stream.consume(MessageContext::new(), Payload::Owned(())).await;
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    environment.publish_runtime_config(Arc::new(configuration(true)));
    stream.consume(MessageContext::new(), Payload::Owned(())).await;
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn endpoint_metrics_include_completion_and_release_after_end() {
    use servicelib::runtime::config::{CallSemantics, RuntimeConfig, RuntimeDataConnectorConfig,
        RuntimeEndpointConfig, PostgreSQLSQLxDataConnectorConfig, PostgreSQLQueryOperation,
        QueryEndpointConfig};
    struct MetricsHandler { environment: RuntimeEnvironment, failed: bool }
    #[async_trait]
    impl EndpointHandler<(), (), ()> for MetricsHandler {
        async fn begin_request(&self, context: MessageContext, _: &dyn RuntimeStream)
            -> Result<BeginRequest<()>, HandlerError> {
            Ok(BeginRequest { context, state: (), error: None })
        }
        async fn end_request(&self, context: MessageContext, _: &dyn RuntimeStream,
            state: Option<()>, result: &HandlerResult, out: &Collector<()>, _: &Collector<()>) {
            assert_eq!(state, Some(()));
            assert_eq!(result.is_err(), self.failed);
            assert!(self.environment.metrics().render_prometheus().contains(
                "dataquery_endpoint_active_requests{connector=\"database\",endpoint=\"rows\",operation=\"Rows\"} 1"));
            out.collect(context, ()).await;
        }
    }
    struct Emit { count: usize, fail: bool }
    #[async_trait]
    impl Operation<(), (), (), MetricsHandler> for Emit {
        async fn execute(&self, context: MessageContext, _: &mut (), _: Payload<()>,
            _: &MetricsHandler, out: &Collector<()>) -> HandlerResult {
            for _ in 0..self.count { out.collect(context.clone(), ()).await; }
            if self.fail { return Err(std::io::Error::other("private query text").into()); }
            Ok(())
        }
    }
    for (count, fail, cancelled) in [(0, false, false), (3, false, false),
        (2, true, false), (0, false, true)] {
        let environment = RuntimeEnvironment::default();
        environment.publish_runtime_config(Arc::new(RuntimeConfig::from_parts(
            CallSemantics::FunctionCall, [], [], [],
            [RuntimeDataConnectorConfig::PostgreSQLSQLx(PostgreSQLSQLxDataConnectorConfig {
                id: 1, name: "database".into(), connection_string: "postgres://localhost/test".into(),
                connections_count: 1, connect_timeout_ms: 1000, operation_timeout_ms: 0,
            })],
            [RuntimeEndpointConfig::Query(QueryEndpointConfig {
                id: 2, name: "rows".into(), id_data_connector: 1, enabled: true,
                tracing_enabled: false, query_operation: PostgreSQLQueryOperation::Rows, sql_file: None,
            })], []).unwrap()));
        let stream = QueryStream::<(), (), ()>::new(&QueryStreamConfig {
            stream: StreamConfig::new(3, "query"), endpoint_id: 2,
        }, environment.clone()).unwrap();
        let endpoint = dataquery::make_endpoint_consumer(&stream,
            MetricsHandler { environment: environment.clone(), failed: fail || cancelled },
            Emit { count, fail }).unwrap();
        let context = MessageContext::new();
        if cancelled { context.cancel(); }
        endpoint.consume(context, Payload::new(())).await;
        let metrics = environment.metrics().render_prometheus();
        let labels = "{connector=\"database\",endpoint=\"rows\",operation=\"Rows\"}";
        for (name, value) in [("requests_total", 1), ("results_total", count + 1), ("active_requests", 0)] {
            assert!(metrics.contains(&format!("dataquery_endpoint_{name}{labels} {value}")), "{metrics}");
        }
        if fail || cancelled {
            let outcome = if cancelled { "cancelled" } else { "error" };
            assert!(metrics.contains(&format!("dataquery_endpoint_failures_total{{connector=\"database\",endpoint=\"rows\",operation=\"Rows\",outcome=\"{outcome}\"}} 1")));
        }
        assert!(!metrics.contains("private query text"));
    }
}

#[tokio::test]
async fn query_end_does_not_wait_for_task_pool_downstream() {
    use servicelib::runtime::{config::{CallSemantics, RuntimeConfig, RuntimeStreamConfig,
        MapStreamConfig, PoolConfig, LinkConfig}, pool::TaskPool, environment::metrics::Metrics};
    struct EmitHandler(Arc<AtomicUsize>);
    #[async_trait]
    impl EndpointHandler<(), i32, ()> for EmitHandler {
        async fn begin_request(&self, context: MessageContext, _: &dyn RuntimeStream) -> Result<BeginRequest<()>, HandlerError> {
            Ok(BeginRequest {context, state: (), error: None})
        }
        async fn end_request(&self, context: MessageContext, _: &dyn RuntimeStream,
            _: Option<()>, result: &HandlerResult, out: &Collector<i32>, _: &Collector<()>) {
            assert!(result.is_ok());
            self.0.fetch_add(1, Ordering::SeqCst);
            out.collect(context, 99).await;
        }
    }
    struct Emit;
    #[async_trait]
    impl Operation<(), (), i32, EmitHandler> for Emit {
        async fn execute(&self, context: MessageContext, _: &mut (), _: Payload<()>,
            _: &EmitHandler, out: &Collector<i32>) -> HandlerResult {
            out.collect(context, 42).await; Ok(())
        }
    }
    struct Slow {
        entered: Arc<Notify>, release: Arc<Notify>, values: Arc<std::sync::Mutex<Vec<i32>>>,
    }
    impl Consumer<i32> for Slow {
        async fn consume(&self, _: MessageContext, value: Payload<i32>) {
            if *value == 42 { self.entered.notify_one(); self.release.notified().await; }
            self.values.lock().unwrap().push(*value);
        }
    }
    tokio::time::timeout(std::time::Duration::from_secs(3), async {
        let environment = RuntimeEnvironment::with_metrics(CallSemantics::FunctionCall, Metrics::noop());
        let config = QueryStreamConfig {stream: StreamConfig::new(1, "query"), endpoint_id: 1};
        let mut output = StreamConfig::new(2, "slow"); output.id_source = 1;
        environment.publish_runtime_config(Arc::new(RuntimeConfig::from_parts(
            CallSemantics::FunctionCall, [],
            [RuntimeStreamConfig::from(config.clone()), RuntimeStreamConfig::from(MapStreamConfig::from(output))],
            [PoolConfig {name: "results".into(), executors_count: 1, queue_capacity: 0}], [], [],
            [LinkConfig {from: 1, to: 2, call_semantics: CallSemantics::TaskPool {pool_name: "results".into()}, r#async: false}],
        ).unwrap()));
        let pool = TaskPool::new("results", environment.clone()).unwrap();
        environment.register_task_pool(pool.clone()).unwrap();
        pool.start().unwrap();
        let query = QueryStream::<(), i32, ()>::new(&config, environment.clone()).unwrap();
        let entered = Arc::new(Notify::new()); let release = Arc::new(Notify::new());
        let values = Arc::new(std::sync::Mutex::new(Vec::new()));
        query.stream().try_set_consumer(Arc::new(Slow {entered: entered.clone(), release: release.clone(), values: values.clone()}), 2).unwrap();
        let ends = Arc::new(AtomicUsize::new(0));
        dataquery::make_endpoint_consumer(&query, EmitHandler(ends.clone()), Emit).unwrap();
        environment.build_runtime_streams().unwrap();
        query.consume(MessageContext::new(), Payload::new(())).await;
        entered.notified().await;
        assert_eq!(ends.load(Ordering::SeqCst), 1);
        assert!(values.lock().unwrap().is_empty());
        release.notify_one();
        pool.stop(MessageContext::new()).await;
        assert_eq!(*values.lock().unwrap(), vec![42, 99]);
    }).await.unwrap();
}

#[tokio::test]
async fn service_shutdown_joins_abandoned_query_before_closing_client() {
    use std::sync::atomic::AtomicBool;
    use servicelib::runtime::{serviceapp::ServiceApp, environment::RuntimeResult,
        config::{ServiceConfig, RuntimeConfig, CallSemantics}};
    struct Client(Arc<AtomicBool>);
    #[async_trait]
    impl Lifecycle for Client {
        async fn start(&self, _: MessageContext) -> RuntimeResult<()> { Ok(()) }
        async fn stop(&self, _: MessageContext) -> RuntimeResult<()> {
            self.0.store(true, Ordering::SeqCst); Ok(())
        }
    }
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        let service = ServiceConfig { id: 1, name: "query-finalizer".into(),
            shutdown_timeout: 3000, ..ServiceConfig::default() };
        let environment = RuntimeEnvironment::with_metrics(CallSemantics::FunctionCall,
            servicelib::runtime::environment::metrics::Metrics::noop());
        environment.publish_runtime_config(Arc::new(RuntimeConfig::from_parts(
            CallSemantics::FunctionCall, [service.clone()], [], [], [], [], []).unwrap()));
        let stream = QueryStream::<(), (), ()>::new(&QueryStreamConfig {
            stream: StreamConfig::new(1, "query"), endpoint_id: 1,
        }, environment.clone()).unwrap();
        let probe = Arc::new(Probe::default());
        let consumer = dataquery::make_endpoint_consumer(&stream,
            Handler(probe.clone()), WaitForCancellation(probe.clone())).unwrap();
        let closed = Arc::new(AtomicBool::new(false));
        let mut app = ServiceApp::new(environment, service).unwrap();
        app.add_managed_data_connector(Arc::new(Client(closed.clone()))).unwrap();
        app.add_request_drain(consumer.requests()).unwrap();
        let app = Arc::new(app);
        app.start_graph(MessageContext::new()).await.unwrap();
        let task = tokio::spawn(async move { consumer.consume(MessageContext::new(), Payload::new(())).await });
        probe.entered.notified().await;
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
        probe.finalizing.notified().await;
        let stopping_app = app.clone();
        let stopping = tokio::spawn(async move { stopping_app.stop(MessageContext::new()).await });
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        assert!(!stopping.is_finished());
        assert!(!closed.load(Ordering::SeqCst));
        assert_eq!(probe.ends.load(Ordering::SeqCst), 0);
        probe.release.notify_one();
        stopping.await.unwrap().unwrap();
        assert_eq!(probe.ends.load(Ordering::SeqCst), 1);
        assert!(closed.load(Ordering::SeqCst));
    }).await.unwrap();
}

#[tokio::test]
async fn concurrent_requests_keep_state_and_finalize_once() {
    use std::sync::Mutex;
    struct ConcurrentHandler {
        next: AtomicUsize,
        ends: Arc<Mutex<Vec<usize>>>,
    }
    #[async_trait]
    impl EndpointHandler<usize, (), ()> for ConcurrentHandler {
        async fn begin_request(&self, context: MessageContext, _: &dyn RuntimeStream)
            -> Result<BeginRequest<usize>, HandlerError> {
            Ok(BeginRequest { context, state: self.next.fetch_add(1, Ordering::SeqCst), error: None })
        }
        async fn end_request(&self, _: MessageContext, _: &dyn RuntimeStream,
            state: Option<usize>, result: &HandlerResult, _: &Collector<()>, _: &Collector<()>) {
            let state = state.unwrap();
            assert_eq!(result.is_err(), state % 2 == 1);
            self.ends.lock().unwrap().push(state);
        }
    }
    struct ConcurrentOperation(Arc<tokio::sync::Barrier>);
    #[async_trait]
    impl Operation<usize, (), (), ConcurrentHandler> for ConcurrentOperation {
        async fn execute(&self, context: MessageContext, state: &mut usize, _: Payload<()>,
            _: &ConcurrentHandler, out: &Collector<()>) -> HandlerResult {
            let original = *state;
            self.0.wait().await;
            tokio::task::yield_now().await;
            assert_eq!(*state, original);
            out.collect(context, ()).await;
            if original % 2 == 1 { return Err(std::io::Error::other("partial request failure").into()); }
            Ok(())
        }
    }
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        let stream = QueryStream::<(), (), ()>::new(&QueryStreamConfig {
            stream: StreamConfig::new(1, "query"), endpoint_id: 1,
        }, RuntimeEnvironment::with_metrics(servicelib::runtime::config::CallSemantics::FunctionCall,
            servicelib::runtime::environment::metrics::Metrics::noop())).unwrap();
        let ends = Arc::new(Mutex::new(Vec::new()));
        let consumer = dataquery::make_endpoint_consumer(&stream,
            ConcurrentHandler { next: AtomicUsize::new(0), ends: ends.clone() },
            ConcurrentOperation(Arc::new(tokio::sync::Barrier::new(8)))).unwrap();
        futures::future::join_all((0..8).map(|_| consumer.consume(MessageContext::new(), Payload::new(())))).await;
        let mut actual = ends.lock().unwrap().clone();
        actual.sort_unstable();
        assert_eq!(actual, (0..8).collect::<Vec<_>>());
    }).await.unwrap();
}
