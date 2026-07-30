use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use servicelib::{
    Consumer, MessageContext, Payload, Stream,
    api::HTTPMethodType,
    datasink::http::{
        Client, EndpointHandler, HandlerError, HandlerResult, Request, Requester, Response,
        StreamContext, make_endpoint_consumer,
    },
    runtime::{
        common::RuntimeStream,
        config::{
            CallSemantics, HttpDataConnectorConfig, HttpEndpointConfig, RuntimeConfig,
            SinkStreamConfig, StreamConfig,
        },
        environment::RuntimeEnvironment,
    },
};

struct MockClient {
    requests: Arc<Mutex<Vec<Request>>>,
}

#[async_trait]
impl Client for MockClient {
    async fn perform(
        &self,
        _context: MessageContext,
        request: Request,
    ) -> Result<Response, HandlerError> {
        self.requests.lock().unwrap().push(request);
        Ok(Response {
            status: 200,
            body: b"17".to_vec(),
            headers: Default::default(),
        })
    }
}

struct Handler {
    events: Arc<Mutex<Vec<&'static str>>>,
}

#[async_trait]
impl EndpointHandler<(), u32, u32, String> for Handler {
    async fn begin_request(
        &self,
        context: MessageContext,
        _stream: StreamContext<u32, u32, String>,
    ) -> Result<(MessageContext, ()), HandlerError> {
        self.events.lock().unwrap().push("begin");
        Ok((context, ()))
    }

    async fn consume_message(
        &self,
        _context: MessageContext,
        _stream: StreamContext<u32, u32, String>,
        _handler_state: &mut (),
        value: Payload<u32>,
        requester: &mut Requester,
    ) -> HandlerResult {
        self.events.lock().unwrap().push("consume");
        requester.new_request("POST", "http://inventory/reserve", value.to_string());
        Ok(())
    }

    async fn handle_response(
        &self,
        context: MessageContext,
        stream: StreamContext<u32, u32, String>,
        _handler_state: &mut (),
        response: Response,
    ) -> HandlerResult {
        self.events.lock().unwrap().push("response");
        let value = String::from_utf8(response.body)?.parse::<u32>()?;
        stream.collect(context, value).await;
        Ok(())
    }

    async fn end_request(
        &self,
        _context: MessageContext,
        _stream: StreamContext<u32, u32, String>,
        result: &HandlerResult,
        _handler_state: (),
    ) {
        assert!(result.is_ok());
        self.events.lock().unwrap().push("end");
    }
}

struct ResultCollector(Arc<Mutex<Vec<u32>>>);

#[async_trait]
impl Consumer<u32> for ResultCollector {
    async fn consume(&self, _context: MessageContext, payload: Payload<u32>) {
        self.0.lock().unwrap().push(*payload);
    }
}

#[tokio::test]
async fn http_sink_preserves_lifecycle_correlation_and_metrics() {
    let environment = RuntimeEnvironment::default();
    environment.publish_runtime_config(Arc::new(
        RuntimeConfig::from_parts(
            CallSemantics::FunctionCall,
            [],
            [
                StreamConfig::new(1, "orders").into(),
                SinkStreamConfig {
                    stream: StreamConfig::new(2, "reserve inventory"),
                    endpoint_id: 3,
                }
                .into(),
            ],
            [],
            [],
            [],
            [],
        )
        .unwrap(),
    ));
    let source = Stream::new(StreamConfig::new(1, "orders"), environment.clone());
    let sink = source
        .sink_with_result::<u32, String>(SinkStreamConfig {
            stream: StreamConfig::new(2, "reserve inventory"),
            endpoint_id: 3,
        })
        .unwrap();
    let results = Arc::new(Mutex::new(Vec::new()));
    sink.stream()
        .try_set_consumer(Arc::new(ResultCollector(results.clone())), 4)
        .unwrap();

    let requests = Arc::new(Mutex::new(Vec::new()));
    let events = Arc::new(Mutex::new(Vec::new()));
    make_endpoint_consumer(
        &sink,
        HttpEndpointConfig {
            id: 3,
            name: "reserve".to_owned(),
            id_data_connector: 5,
            http_method_type: HTTPMethodType::POST,
            path: "/reserve".to_owned(),
        },
        HttpDataConnectorConfig {
            id: 5,
            name: "inventory".to_owned(),
            host: "inventory".to_owned(),
            port: 8080,
            address: String::new(),
            use_dedicated_listener: false,
        },
        Arc::new(MockClient {
            requests: requests.clone(),
        }),
        Handler {
            events: events.clone(),
        },
    )
    .unwrap();

    source
        .emit(
            MessageContext::new()
                .with_stream_id("order-42")
                .enable_sampling(),
            Payload::new(42),
        )
        .await;

    assert_eq!(
        &*events.lock().unwrap(),
        &["begin", "consume", "response", "end"]
    );
    assert_eq!(&*results.lock().unwrap(), &[17]);
    let requests = requests.lock().unwrap();
    assert_eq!(requests[0].headers["x-stream-id"], "order-42");
    assert_eq!(requests[0].headers["x-trace"], "1");
    assert_eq!(sink.name(), "reserve inventory");

    let metrics = environment.metrics().render_prometheus();
    assert!(metrics.contains(
        r#"datasink_endpoint_messages_total{connector="inventory",endpoint="reserve",protocol="http"} 1"#
    ));
}
