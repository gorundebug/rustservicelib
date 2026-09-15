use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use servicelib::{
    MessageContext, Payload,
    datasink::localsink::{
        EndpointHandler, HandlerResult, SinkCallback, make_custom_endpoint_consumer,
    },
    runtime::{
        common::{Consumer, RuntimeStream},
        config::{
            CallSemantics, CustomDataConnectorConfig, CustomEndpointConfig, RuntimeConfig,
            SinkStreamConfig, StreamConfig,
        },
        environment::RuntimeEnvironment,
        stream::Stream,
    },
};

#[derive(Default)]
struct Events(Mutex<Vec<String>>);

struct Handler {
    events: Arc<Events>,
}

#[async_trait]
impl EndpointHandler<String, i32, String> for Handler {
    fn get_stream_id(&self, _context: &MessageContext, value: &i32) -> String {
        format!("message-{value}")
    }

    async fn begin_request(
        &self,
        context: MessageContext,
        _stream: &dyn RuntimeStream,
    ) -> (MessageContext, String) {
        self.events.0.lock().unwrap().push("begin".to_owned());
        (context, "state".to_owned())
    }

    async fn consume_message(
        &self,
        context: MessageContext,
        _stream: &dyn RuntimeStream,
        handler_state: &mut String,
        value: Payload<i32>,
        result_stream: &Stream<String>,
    ) -> HandlerResult {
        self.events.0.lock().unwrap().push("consume".to_owned());
        assert_eq!(context.stream_id(), Some("message-42"));
        assert_eq!(handler_state, "state");
        result_stream
            .emit(context, Payload::new(format!("result-{}", *value)))
            .await;
        Ok(())
    }

    async fn end_request(
        &self,
        _context: MessageContext,
        _stream: &dyn RuntimeStream,
        result: &HandlerResult,
        handler_state: String,
    ) {
        assert!(result.is_ok());
        assert_eq!(handler_state, "state");
        self.events.0.lock().unwrap().push("end".to_owned());
    }
}

struct Done {
    events: Arc<Events>,
}

#[async_trait]
impl SinkCallback<i32> for Done {
    async fn done(&self, context: MessageContext, value: Payload<i32>, result: &HandlerResult) {
        assert_eq!(context.stream_id(), Some("message-42"));
        assert_eq!(*value, 42);
        assert!(result.is_ok());
        self.events.0.lock().unwrap().push("done".to_owned());
    }
}

struct Capture(Mutex<Vec<String>>);

#[async_trait]
impl Consumer<String> for Capture {
    async fn consume(&self, _context: MessageContext, payload: Payload<String>) {
        self.0.lock().unwrap().push((*payload).clone());
    }
}

async fn run_custom_sink(environment: RuntimeEnvironment, context: MessageContext) {
    let endpoint_config = CustomEndpointConfig {
        id: 10,
        name: "Custom endpoint".to_owned(),
        id_data_connector: 20,
        tracing_enabled: false,
    };
    environment.publish_runtime_config(Arc::new(
        RuntimeConfig::from_parts(
            CallSemantics::FunctionCall,
            [],
            [],
            [],
            [CustomDataConnectorConfig {
                id: 20,
                name: "Custom connector".to_owned(),
            }
            .into()],
            [endpoint_config.clone().into()],
            [],
        )
        .unwrap(),
    ));
    let source = Stream::new(&StreamConfig::new(1, "Output"), environment.clone());
    let sink = source
        .sink::<String>(&SinkStreamConfig {
            stream: StreamConfig::new(2, "Custom Sink")
                .with_pipeline("booking")
                .with_component("Reserve Inventory"),
            endpoint_id: 10,
        })
        .unwrap();
    let result_capture = Arc::new(Capture(Mutex::new(Vec::new())));
    sink.error_stream()
        .set_consumer(Arc::clone(&result_capture), -2);
    let events = Arc::new(Events::default());
    let endpoint = make_custom_endpoint_consumer(
        &sink,
        &endpoint_config,
        Handler {
            events: Arc::clone(&events),
        },
    )
    .unwrap();
    endpoint.set_sink_callback(Arc::new(Done {
        events: Arc::clone(&events),
    }));

    source.emit(context, Payload::new(42)).await;

    assert_eq!(
        *events.0.lock().unwrap(),
        ["begin", "consume", "end", "done"]
    );
    assert_eq!(*result_capture.0.lock().unwrap(), ["result-42"]);
    let metrics = environment.metrics().render_prometheus();
    assert!(metrics.contains(
        r#"datasink_endpoint_messages_total{connector="Custom connector",endpoint="Custom endpoint"} 1"#
    ));
    assert!(!metrics.contains(r#"protocol="local""#));
}


#[tokio::test]
async fn custom_sink_preserves_the_go_handler_lifecycle() {
    run_custom_sink(RuntimeEnvironment::default(), MessageContext::new()).await;
}

#[derive(Clone, Default)]
struct TransportTraceCapture {
    spans: Arc<Mutex<Vec<std::collections::BTreeMap<String, String>>>>,
    events: Arc<Mutex<usize>>,
}

struct StringFields(std::collections::BTreeMap<String, String>);

impl tracing::field::Visit for StringFields {
    fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
        self.0.insert(field.name().to_owned(), value.to_owned());
    }

    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        self.0.insert(field.name().to_owned(), format!("{value:?}"));
    }
}

impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for TransportTraceCapture {
    fn on_new_span(
        &self,
        attributes: &tracing::span::Attributes<'_>,
        _id: &tracing::span::Id,
        _context: tracing_subscriber::layer::Context<'_, S>,
    ) {
        if attributes.metadata().name() == "local.output" {
            let mut fields = StringFields(std::collections::BTreeMap::new());
            attributes.record(&mut fields);
            self.spans.lock().unwrap().push(fields.0);
        }
    }

    fn on_event(
        &self,
        event: &tracing::Event<'_>,
        _context: tracing_subscriber::layer::Context<'_, S>,
    ) {
        if event.metadata().target() == "servicelib::datasink::localsink::custom" {
            *self.events.lock().unwrap() += 1;
        }
    }
}

struct ConfigurableTracing(bool);

#[async_trait]
impl servicelib::runtime::environment::tracing::TracingEngine for ConfigurableTracing {
    fn enabled(&self) -> bool {
        self.0
    }

    async fn shutdown(&self) -> servicelib::runtime::environment::RuntimeResult<()> {
        Ok(())
    }
}

#[tokio::test]
async fn custom_sink_groups_sampled_spans_without_unsampled_events() {
    use tracing::instrument::WithSubscriber;
    use tracing_subscriber::prelude::*;

    for (enabled, sampled) in [(false, false), (false, true), (true, false), (true, true)] {
        let defaults = RuntimeEnvironment::default();
        let environment = RuntimeEnvironment::with_telemetry(
            CallSemantics::FunctionCall,
            defaults.metrics_engine().clone(),
            Arc::new(ConfigurableTracing(enabled)),
            defaults.logs_engine().clone(),
        );
        let capture = TransportTraceCapture::default();
        let subscriber = tracing_subscriber::registry().with(capture.clone());
        let context = if sampled {
            MessageContext::new().enable_sampling()
        } else {
            MessageContext::new()
        };
        run_custom_sink(environment, context).with_subscriber(subscriber).await;

        let spans = capture.spans.lock().unwrap();
        if enabled && sampled {
            assert_eq!(spans.len(), 1);
            for (key, value) in [
                ("stream", "Custom Sink"),
                ("endpoint", "Custom Sink"),
                ("pipeline", "booking"),
                ("component", "Reserve Inventory"),
            ] {
                assert_eq!(spans[0].get(key).map(String::as_str), Some(value));
            }
            assert_eq!(*capture.events.lock().unwrap(), 2);
        } else {
            assert!(spans.is_empty(), "disabled/unsampled transport created a span");
            assert_eq!(*capture.events.lock().unwrap(), 0, "disabled/unsampled transport emitted events");
        }
    }
}
