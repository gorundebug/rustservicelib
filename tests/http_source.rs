use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use axum::{
    body::{Body, to_bytes},
    http::{Request, StatusCode},
};
use servicelib::{
    Consumer, MessageContext, Payload,
    api::HTTPMethodType,
    datasource::http::{
        EndpointHandler, HandlerData, HandlerError, HandlerResult, ResultCallback, ResultContext,
        make_endpoint_consumer,
    },
    operators::InputStream,
    runtime::{
        config::{
            CallSemantics, HttpEndpointConfig, InputStreamConfig, RuntimeConfig,
            RuntimeStreamConfig, StreamConfig,
        },
        datasource::StreamContext,
        environment::RuntimeEnvironment,
        stream::Stream,
    },
};
use tokio::sync::Mutex as AsyncMutex;
use tower::ServiceExt;

struct Pipeline {
    results: Stream<u32>,
}

#[async_trait]
impl Consumer<u32> for Pipeline {
    async fn consume(&self, context: MessageContext, payload: Payload<u32>) {
        self.results.emit(context, payload).await;
    }
}

struct Handler {
    events: Arc<Mutex<Vec<&'static str>>>,
}

#[async_trait]
impl EndpointHandler<(), (), (), u32, u32, String> for Handler {
    async fn begin_request(
        &self,
        context: MessageContext,
        _stream: StreamContext<u32, u32, String>,
        _data: HandlerData,
    ) -> Result<(MessageContext, ()), HandlerError> {
        self.events.lock().unwrap().push("begin");
        Ok((context, ()))
    }

    async fn consume_message(
        &self,
        context: MessageContext,
        stream: StreamContext<u32, u32, String>,
        _handler_state: Arc<AsyncMutex<()>>,
        data: HandlerData,
        result_context: Arc<ResultContext<(), (), (), u32, u32, String>>,
    ) -> HandlerResult {
        self.events.lock().unwrap().push("consume");
        let done = Arc::clone(&result_context);
        result_context.set_result_callback(
            "42",
            ResultCallback::<(), (), (), u32, u32, String>::new(
                move |_context, _stream, _state, value, data| {
                    let done = Arc::clone(&done);
                    Box::pin(async move {
                        data.set_response_body(value.to_string());
                        done.done();
                        true
                    })
                },
            ),
        );
        let value = std::str::from_utf8(&data.body)?.parse::<u32>()?;
        stream.collect(context, value).await;
        Ok(())
    }

    async fn get_message_id(
        &self,
        _context: &MessageContext,
        _stream: &StreamContext<u32, u32, String>,
        _handler_state: Arc<AsyncMutex<()>>,
        value: &u32,
    ) -> String {
        value.to_string()
    }

    async fn end_request(
        &self,
        _context: MessageContext,
        _stream: StreamContext<u32, u32, String>,
        result: &HandlerResult,
        _handler_state: Arc<AsyncMutex<()>>,
        _data: HandlerData,
    ) {
        assert!(result.is_ok());
        self.events.lock().unwrap().push("end");
    }
}

#[tokio::test]
async fn http_source_correlates_pipeline_result_and_waits_for_done() {
    let environment = RuntimeEnvironment::default();
    environment.publish_runtime_config(Arc::new(
        RuntimeConfig::from_parts(
            CallSemantics::FunctionCall,
            [],
            [
                RuntimeStreamConfig::from(InputStreamConfig {
                    stream: StreamConfig::new(1, "process order"),
                    endpoint_id: 7,
                }),
                RuntimeStreamConfig::from(servicelib::runtime::config::MapStreamConfig::from(
                    StreamConfig::new(2, "order result"),
                )),
            ],
            [],
            [],
            [],
            [],
        )
        .unwrap(),
    ));
    let input = InputStream::new(
        &InputStreamConfig {
            stream: StreamConfig::new(1, "process order"),
            endpoint_id: 7,
        },
        environment.clone(),
    );
    let result_stream = Stream::new(&StreamConfig::new(2, "order result"), environment.clone());
    input.set_source(&result_stream).unwrap();
    input
        .stream()
        .try_set_consumer(
            Arc::new(Pipeline {
                results: result_stream,
            }),
            2,
        )
        .unwrap();

    let endpoint_config = HttpEndpointConfig {
        id: 7,
        name: "process order".to_owned(),
        id_data_connector: 8,
        http_method_type: HTTPMethodType::POST,
        path: "/orders".to_owned(),
    };
    let events = Arc::new(Mutex::new(Vec::new()));
    let endpoint = make_endpoint_consumer::<(), (), (), u32, u32, String, _>(
        input,
        endpoint_config.clone(),
        "order api",
        Handler {
            events: events.clone(),
        },
    )
    .unwrap();
    let registered = environment
        .endpoint_consumer(7)
        .expect("HTTP endpoint consumer must be owned by the runtime");
    assert_eq!(registered.id(), 7);
    assert!(registered.function_implementation().contains("Handler"));

    let response = endpoint
        .router(&endpoint_config)
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/orders")
                .header("x-stream-id", "request-42")
                .body(Body::from("42"))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        to_bytes(response.into_body(), usize::MAX).await.unwrap(),
        "42"
    );
    assert_eq!(&*events.lock().unwrap(), &["begin", "consume", "end"]);

    let metrics = environment.metrics().render_prometheus();
    assert!(metrics.contains(
        r#"datasource_endpoint_messages_total{connector="order api",endpoint="process order",protocol="http"} 1"#
    ));
    assert!(metrics.contains(
        r#"datasource_endpoint_pending_requests{connector="order api",endpoint="process order",protocol="http"} 0"#
    ));
}
