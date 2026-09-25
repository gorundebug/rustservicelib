use std::sync::Arc;

use async_trait::async_trait;
use servicelib::{
    MessageContext, Payload, Stream, SubStream, SubStreamCollectorFunc,
    operators::MapFunction,
    runtime::{
        common::RuntimeStream,
        config::{
            CallSemantics, MapStreamConfig, RuntimeConfig, RuntimeStreamConfig, StreamConfig,
            SubStreamConfig,
        },
        environment::{RuntimeEnvironment, RuntimeResult, tracing::TracingEngine},
        testlog::TestLog,
        testmetrics::TestMetrics,
        testtracing::TestTracing,
    },
};
use tracing_subscriber::layer::SubscriberExt;

#[test]
fn test_metrics_uses_the_production_prometheus_wire_format() {
    let engine = TestMetrics::new();
    let counter = engine
        .metrics()
        .scope("service", Default::default())
        .counter("requests_total", "requests", Default::default())
        .unwrap();
    counter.inc();
    assert!(engine.contains("service_requests_total 1"));
}

#[test]
fn test_log_and_test_tracing_capture_the_same_tracing_events() {
    let logs = TestLog::default();
    let spans = TestTracing::default();
    let subscriber = tracing_subscriber::registry()
        .with(logs.clone())
        .with(spans.clone());
    tracing::subscriber::with_default(subscriber, || {
        let span = tracing::info_span!("process_order", order.id = "42");
        let _guard = span.enter();
        span.record("order.id", "43");
        tracing::event!(
            name: "order_processed",
            tracing::Level::INFO,
            status = "confirmed"
        );
    });

    let records = logs.records();
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].level, tracing::Level::INFO);
    let finished = spans.spans();
    assert_eq!(finished.len(), 1);
    assert_eq!(finished[0].name, "process_order");
    assert_eq!(finished[0].fields["order.id"], "\"43\"");
    assert_eq!(finished[0].events.len(), 1);
    assert_eq!(finished[0].events[0].name, "order_processed");
}

#[test]
fn structured_log_level_and_typed_field_contract() {
    let logs = TestLog::default();
    let subscriber = tracing_subscriber::registry().with(logs.clone());
    tracing::subscriber::with_default(subscriber, || {
        tracing::debug!("debug event");
        tracing::info!("info event");
        tracing::warn!(
            endpoint = "orders",
            attempt = 2_i64,
            ratio = 1.5_f64,
            retry = true,
            "request failed"
        );
        tracing::error!(error = "timeout", "shutdown failed");
    });

    let records = logs.records();
    assert_eq!(records.len(), 4);
    assert_eq!(records[0].level, tracing::Level::DEBUG);
    assert_eq!(records[1].level, tracing::Level::INFO);
    assert_eq!(records[2].level, tracing::Level::WARN);
    assert_eq!(records[3].level, tracing::Level::ERROR);
    assert_eq!(records[2].fields["message"], "request failed");
    assert_eq!(records[2].fields["endpoint"], "\"orders\"");
    assert_eq!(records[2].fields["attempt"], "2");
    assert_eq!(records[2].fields["ratio"], "1.5");
    assert_eq!(records[2].fields["retry"], "true");
    assert_eq!(records[3].fields["message"], "shutdown failed");
    assert_eq!(records[3].fields["error"], "\"timeout\"");
    logs.clear();
    assert!(logs.records().is_empty());
}

struct LoggingMap;

struct ReturningMap;

impl MapFunction<u32, u32> for ReturningMap {
    async fn map(
        &self,
        context: MessageContext,
        _stream: &dyn RuntimeStream,
        value: &u32,
        out: &impl servicelib::runtime::collector::Collect<u32>,
    ) {
        tracing::info!(value = *value, "substream body called");
        out.out(context, *value).await;
    }
}

struct DisabledTracing;

#[async_trait]
impl TracingEngine for DisabledTracing {
    fn enabled(&self) -> bool {
        false
    }

    async fn shutdown(&self) -> RuntimeResult<()> {
        Ok(())
    }
}

async fn check_substream_tracing(sampled: bool, enabled: bool) {
    let traces = TestTracing::default();
    let logs = TestLog::default();
    let tracing_engine: Arc<dyn TracingEngine> = if enabled {
        Arc::new(traces.clone())
    } else {
        Arc::new(DisabledTracing)
    };
    let environment = RuntimeEnvironment::with_telemetry(
        CallSemantics::FunctionCall,
        Arc::new(TestMetrics::new()),
        tracing_engine,
        Arc::new(logs.clone()),
    );
    let mut entry_config = StreamConfig::new(1, "lookup");
    entry_config.id_source = 2;
    entry_config.id_service = 1;
    entry_config.value_type = Some("uint32".to_owned());
    let mut result_config = StreamConfig::new(2, "result");
    result_config.id_source = 1;
    result_config.id_service = 1;
    result_config.value_type = Some("uint32".to_owned());
    let entry_config = SubStreamConfig::from(entry_config);
    let result_config = MapStreamConfig::from(result_config);
    environment.publish_runtime_config(Arc::new(
        RuntimeConfig::from_parts(
            CallSemantics::FunctionCall,
            [],
            [
                RuntimeStreamConfig::from(entry_config.clone()),
                RuntimeStreamConfig::from(result_config.clone()),
            ],
            [],
            [],
            [],
            [],
        )
        .unwrap(),
    ));
    let entry = SubStream::<u32, u32>::new(&entry_config, environment.clone());
    let result = entry
        .stream()
        .map::<u32, _>(&result_config, ReturningMap)
        .unwrap();
    entry.set_source(&result).unwrap();
    environment.build_runtime_streams().unwrap();
    let subscriber = tracing_subscriber::registry()
        .with(logs.clone())
        .with(traces.clone());
    let guard = tracing::subscriber::set_default(subscriber);
    let context = if sampled {
        MessageContext::new().enable_sampling()
    } else {
        MessageContext::new()
    };
    let received = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let delivered = Arc::clone(&received);
    entry
        .consume(
            context,
            42,
            Arc::new(SubStreamCollectorFunc(
                move |_: MessageContext, value: Payload<u32>| {
                    let delivered = Arc::clone(&delivered);
                    async move {
                        assert_eq!(*value, 42);
                        delivered.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        true
                    }
                },
            )),
        )
        .await
        .unwrap();
    drop(guard);
    assert_eq!(received.load(std::sync::atomic::Ordering::Relaxed), 1);
    assert_eq!(logs.records().len(), 1);
    let spans = traces.spans();
    if sampled && enabled {
        for expected in ["\"stream.substream\"", "\"stream.map\""] {
            assert!(
                spans.iter().any(|span| {
                    span.fields
                        .get("otel.name")
                        .is_some_and(|name| name == expected)
                }),
                "missing {expected} span"
            );
        }
    } else {
        assert!(spans.is_empty());
    }
}

#[tokio::test]
async fn substream_uses_standard_sampled_spans() {
    check_substream_tracing(true, true).await;
}

#[tokio::test]
async fn substream_does_not_trace_without_sampling() {
    check_substream_tracing(false, true).await;
}

#[tokio::test]
async fn substream_bypasses_disabled_tracing_backend() {
    check_substream_tracing(true, false).await;
}

impl MapFunction<u32, u32> for LoggingMap {
    async fn map(
        &self,
        _context: MessageContext,
        _stream: &dyn RuntimeStream,
        value: &u32,
        _out: &impl servicelib::runtime::collector::Collect<u32>,
    ) {
        tracing::info!(value = *value, "map function called");
    }
}

#[tokio::test]
async fn operator_future_is_executed_inside_its_tracing_span() {
    let metrics = Arc::new(TestMetrics::new());
    let traces = TestTracing::default();
    let logs = TestLog::default();
    let environment = RuntimeEnvironment::with_telemetry(
        CallSemantics::FunctionCall,
        metrics,
        Arc::new(traces.clone()),
        Arc::new(logs.clone()),
    );
    let input = Stream::new(&StreamConfig::new(1, "input"), environment);
    let _mapped = input
        .map::<u32, _>(&(StreamConfig::new(2, "mapped").into()), LoggingMap)
        .unwrap();
    let subscriber = tracing_subscriber::registry()
        .with(logs.clone())
        .with(traces.clone());

    let guard = tracing::subscriber::set_default(subscriber);
    input
        .emit(MessageContext::new().enable_sampling(), Payload::new(42))
        .await;
    drop(guard);

    assert_eq!(logs.records().len(), 1);
    let spans = traces.spans();
    let map_span = spans
        .iter()
        .find(|span| {
            span.fields
                .get("otel.name")
                .is_some_and(|name| name == "\"stream.map\"")
        })
        .expect("stream.map span");
    assert_eq!(map_span.events.len(), 1);
    assert_eq!(map_span.events[0].fields["value"], "42");
}

#[tokio::test]
async fn operator_does_not_create_a_span_without_explicit_sampling() {
    let metrics = Arc::new(TestMetrics::new());
    let traces = TestTracing::default();
    let logs = TestLog::default();
    let environment = RuntimeEnvironment::with_telemetry(
        CallSemantics::FunctionCall,
        metrics,
        Arc::new(traces.clone()),
        Arc::new(logs.clone()),
    );
    let input = Stream::new(&StreamConfig::new(1, "input"), environment);
    let _mapped = input
        .map::<u32, _>(&(StreamConfig::new(2, "mapped").into()), LoggingMap)
        .unwrap();
    let subscriber = tracing_subscriber::registry()
        .with(logs.clone())
        .with(traces.clone());

    let guard = tracing::subscriber::set_default(subscriber);
    input.emit(MessageContext::new(), Payload::new(42)).await;
    drop(guard);

    assert!(traces.spans().is_empty());
}

#[tokio::test]
async fn operator_bypasses_tracing_when_the_backend_is_disabled() {
    let metrics = Arc::new(TestMetrics::new());
    let traces = TestTracing::default();
    let logs = TestLog::default();
    let environment = RuntimeEnvironment::with_telemetry(
        CallSemantics::FunctionCall,
        metrics,
        Arc::new(DisabledTracing),
        Arc::new(logs.clone()),
    );
    let input = Stream::new(&StreamConfig::new(1, "input"), environment);
    let _mapped = input
        .map::<u32, _>(&(StreamConfig::new(2, "mapped").into()), LoggingMap)
        .unwrap();
    let subscriber = tracing_subscriber::registry()
        .with(logs.clone())
        .with(traces.clone());

    let guard = tracing::subscriber::set_default(subscriber);
    input
        .emit(MessageContext::new().enable_sampling(), Payload::new(42))
        .await;
    drop(guard);

    assert!(traces.spans().is_empty());
    assert_eq!(logs.records().len(), 1);
}
