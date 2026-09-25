use servicelib::{
    MessageContext, Payload, Stream,
    operators::MapFunction,
    runtime::{
        collector::Collector,
        common::RuntimeStream,
        config::{CallSemantics, RuntimeConfig, RuntimeStreamConfig, StreamConfig},
        environment::RuntimeEnvironment,
        testlog::TestLog,
        testmetrics::TestMetrics,
        testtracing::TestTracing,
    },
};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use tracing_subscriber::layer::SubscriberExt;

struct Count(Arc<AtomicUsize>);
impl MapFunction<u32, u32> for Count {
    async fn map(
        &self,
        _context: MessageContext,
        _stream: &dyn RuntimeStream,
        _value: &u32,
        _out: &Collector<u32>,
    ) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

#[test]
fn component_config_is_optional_and_round_trips_without_instance_identity() {
    let plain = StreamConfig::new(1, "price").with_pipeline("pricing");
    assert!(
        !serde_json::to_value(&plain)
            .unwrap()
            .as_object()
            .unwrap()
            .contains_key("component")
    );
    let named = plain.with_component("Customer Pricing");
    let yaml = serde_yaml::to_string(&named).unwrap();
    assert!(!yaml.contains("component_instance"));
    let restored: StreamConfig = serde_yaml::from_str(&yaml).unwrap();
    assert_eq!(restored, named);
    assert!(!restored.properties.contains_key("component"));
}

#[tokio::test]
async fn receiving_grouping_is_used_on_existing_counter_and_operator_spans() {
    let metrics = Arc::new(TestMetrics::new());
    let traces = TestTracing::default();
    let environment = RuntimeEnvironment::with_telemetry(
        CallSemantics::FunctionCall,
        metrics.clone(),
        Arc::new(traces.clone()),
        Arc::new(TestLog::default()),
    );
    let source_config = StreamConfig::new(1, "input")
        .with_pipeline("entry")
        .with_component("Request");
    let target_config = StreamConfig::new(2, "price")
        .with_pipeline("pricing")
        .with_component("Customer Pricing");
    environment.publish_runtime_config(Arc::new(
        RuntimeConfig::from_parts(
            CallSemantics::FunctionCall,
            [],
            [
                RuntimeStreamConfig::Plain(source_config.clone()),
                RuntimeStreamConfig::Map(target_config.clone().into()),
            ],
            [],
            [],
            [],
            [],
        )
        .unwrap(),
    ));
    let calls = Arc::new(AtomicUsize::new(0));
    let source = Stream::<u32>::new(&source_config, environment);
    let _mapped = source
        .map::<u32, _>(&target_config.into(), Count(calls.clone()))
        .unwrap();
    let subscriber = tracing_subscriber::registry().with(traces.clone());
    let guard = tracing::subscriber::set_default(subscriber);
    source.emit(MessageContext::new(), Payload::new(1)).await;
    assert!(traces.spans().is_empty());
    source
        .emit(MessageContext::new().enable_sampling(), Payload::new(2))
        .await;
    drop(guard);
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    let spans = traces.spans();
    assert_eq!(spans.len(), 2, "No extra component wrapper spans");
    for span in spans {
        assert_eq!(span.fields["pipeline"].trim_matches('"'), "pricing");
        assert_eq!(
            span.fields["component"].trim_matches('"'),
            "Customer Pricing"
        );
        assert!(!span.fields.contains_key("component_instance"));
    }
    let text = metrics.prometheus();
    let sample = text
        .lines()
        .find(|line| line.starts_with("stream_messages_total{"))
        .unwrap();
    assert!(sample.contains("pipeline=\"pricing\""));
    assert!(sample.contains("component=\"Customer Pricing\""));
    assert!(sample.ends_with(" 2"));
}

#[test]
fn operator_labels_are_borrowed_from_the_stream_not_reloaded_per_span() {
    let traces = TestTracing::default();
    let environment = RuntimeEnvironment::with_telemetry(
        CallSemantics::FunctionCall,
        Arc::new(TestMetrics::new()),
        Arc::new(traces.clone()),
        Arc::new(TestLog::default()),
    );
    let config = StreamConfig::new(1, "price")
        .with_pipeline("pricing")
        .with_component("Customer Pricing");
    environment.publish_runtime_config(Arc::new(
        RuntimeConfig::from_parts(
            CallSemantics::FunctionCall,
            [],
            [RuntimeStreamConfig::Plain(config.clone())],
            [],
            [],
            [],
            [],
        )
        .unwrap(),
    ));
    let stream = Stream::<u32>::new(&config, environment.clone());
    let labels = stream.tracing_labels();
    assert_eq!(labels, ("price", "pricing", "Customer Pricing"));
    assert!(std::ptr::eq(
        labels.1.as_ptr(),
        stream.tracing_labels().1.as_ptr()
    ));
    // Removing the registry entry proves that starting spans does not depend
    // on another lookup. Definition metadata belongs to this concrete stream.
    environment.publish_runtime_config(Arc::new(
        RuntimeConfig::from_parts(CallSemantics::FunctionCall, [], [], [], [], [], []).unwrap(),
    ));
    let subscriber = tracing_subscriber::registry().with(traces.clone());
    let _guard = tracing::subscriber::set_default(subscriber);
    let (_, span) = stream.start_span(MessageContext::new().enable_sampling(), "stream.map");
    drop(span);
    assert_eq!(traces.spans().len(), 1);
    let spans = traces.spans();
    assert_eq!(spans[0].fields["pipeline"].trim_matches('"'), "pricing");
    assert_eq!(
        spans[0].fields["component"].trim_matches('"'),
        "Customer Pricing"
    );
}
