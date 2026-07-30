use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use servicelib::{
    MessageContext, Payload,
    datasink::localsink::{
        EndpointHandler, HandlerResult, SinkCallback, make_custom_endpoint_consumer,
    },
    runtime::{
        common::{Consumer, RuntimeStream},
        config::{SinkStreamConfig, StreamConfig},
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

#[tokio::test]
async fn custom_sink_preserves_the_go_handler_lifecycle() {
    let environment = RuntimeEnvironment::default();
    let source = Stream::new(StreamConfig::new(1, "Output"), environment.clone());
    let sink = source
        .sink::<String>(SinkStreamConfig {
            stream: StreamConfig::new(2, "Custom Sink"),
            endpoint_id: 10,
        })
        .unwrap();
    let result_capture = Arc::new(Capture(Mutex::new(Vec::new())));
    sink.error_stream()
        .set_consumer(Arc::clone(&result_capture), -2);
    let events = Arc::new(Events::default());
    let endpoint = make_custom_endpoint_consumer(
        &sink,
        Handler {
            events: Arc::clone(&events),
        },
    )
    .unwrap();
    endpoint.set_sink_callback(Arc::new(Done {
        events: Arc::clone(&events),
    }));

    source.emit(MessageContext::new(), Payload::new(42)).await;

    assert_eq!(
        *events.0.lock().unwrap(),
        ["begin", "consume", "end", "done"]
    );
    assert_eq!(*result_capture.0.lock().unwrap(), ["result-42"]);
}
