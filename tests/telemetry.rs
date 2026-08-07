use std::sync::Arc;

use async_trait::async_trait;
use servicelib::{
    MessageContext, Payload, Stream,
    operators::MapFunction,
    runtime::{
        common::RuntimeStream,
        config::{CallSemantics, StreamConfig},
        environment::RuntimeEnvironment,
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
        tracing::info!(status = "confirmed", "order processed");
    });

    let records = logs.records();
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].level, tracing::Level::INFO);
    let finished = spans.spans();
    assert_eq!(finished.len(), 1);
    assert_eq!(finished[0].name, "process_order");
    assert_eq!(finished[0].fields["order.id"], "\"43\"");
    assert_eq!(finished[0].events.len(), 1);
}

struct LoggingMap;

#[async_trait]
impl MapFunction<u32, u32> for LoggingMap {
    async fn map(
        &self,
        _context: MessageContext,
        _stream: &dyn RuntimeStream,
        value: Payload<u32>,
        _out: &Stream<u32>,
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
