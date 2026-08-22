use std::sync::{
    Arc, Mutex,
    atomic::{AtomicUsize, Ordering},
};

use async_trait::async_trait;
use futures::stream;
use servicelib::{
    Consumer, MessageContext, Payload,
    datasource::grpc::{
        EndpointHandler, HandlerResult, ResultContext, Sender, StreamContext,
        make_grpc_bidi_streaming_endpoint_consumer, make_grpc_client_streaming_endpoint_consumer,
        make_grpc_no_streaming_endpoint_consumer, make_grpc_server_streaming_endpoint_consumer,
    },
    operators::InputStream,
    runtime::{
        config::{
            CallSemantics, GrpcDataConnectorConfig, GrpcEndpointConfig, InputStreamConfig,
            RuntimeConfig, StreamConfig,
        },
        environment::RuntimeEnvironment,
        stream::Stream,
    },
};
use tokio::sync::Mutex as AsyncMutex;

struct Pipeline {
    results: Stream<u32>,
}

#[async_trait]
impl Consumer<u32> for Pipeline {
    async fn consume(&self, context: MessageContext, payload: Payload<u32>) {
        self.results.emit(context, payload).await;
    }
}

struct State {
    sum: u32,
}

struct Handler {
    streaming_request: bool,
    eof_count: Arc<AtomicUsize>,
    end_count: Arc<AtomicUsize>,
}

#[async_trait]
impl EndpointHandler<State, u32, u32, u32, u32, String> for Handler {
    async fn begin_request(
        &self,
        context: MessageContext,
        _stream: StreamContext<u32, u32, String>,
    ) -> HandlerResult<(MessageContext, State)> {
        Ok((context, State { sum: 0 }))
    }

    async fn consume_message(
        &self,
        context: MessageContext,
        stream: StreamContext<u32, u32, String>,
        state: Arc<AsyncMutex<State>>,
        request: u32,
        result_context: Arc<ResultContext<State, u32, u32, u32, String>>,
        _sender: Arc<dyn Sender<u32>>,
    ) -> HandlerResult<MessageContext> {
        state.lock().await.sum += request;
        let done = Arc::clone(&result_context);
        let streaming_request = self.streaming_request;
        result_context.set_result_callback(
            request.to_string(),
            Arc::new(move |context, _stream, state, value, sender| {
                let done = Arc::clone(&done);
                Box::pin(async move {
                    if !streaming_request || *value == 2 {
                        let response = if streaming_request {
                            state.lock().await.sum * 10
                        } else {
                            *value * 10
                        };
                        sender.send(context, response).await.unwrap();
                    }
                    if *value == 2 {
                        done.done();
                    }
                    true
                })
            }),
        );
        stream.collect(context.clone(), request).await;
        Ok(context)
    }

    async fn get_message_id(
        &self,
        _context: &MessageContext,
        _stream: &StreamContext<u32, u32, String>,
        _state: Arc<AsyncMutex<State>>,
        value: &u32,
    ) -> String {
        value.to_string()
    }

    async fn eof(
        &self,
        _context: MessageContext,
        _stream: StreamContext<u32, u32, String>,
        _state: Arc<AsyncMutex<State>>,
    ) {
        self.eof_count.fetch_add(1, Ordering::AcqRel);
    }

    async fn end_request(
        &self,
        _context: MessageContext,
        _stream: StreamContext<u32, u32, String>,
        _result: &HandlerResult,
        _state: Arc<AsyncMutex<State>>,
    ) -> HandlerResult {
        self.end_count.fetch_add(1, Ordering::AcqRel);
        Ok(())
    }
}

fn make_input(
    id: i32,
) -> (
    InputStream<u32, u32, String>,
    Arc<AtomicUsize>,
    Arc<AtomicUsize>,
) {
    let environment = RuntimeEnvironment::default();
    let input_config = InputStreamConfig {
        stream: StreamConfig::new(id, "grpc input"),
        endpoint_id: id + 1,
    };
    environment.publish_runtime_config(Arc::new(
        RuntimeConfig::from_parts(
            CallSemantics::FunctionCall,
            [],
            [input_config.clone().into()],
            [],
            [GrpcDataConnectorConfig {
                id: id + 10,
                name: "orders".to_owned(),
                address: "http://orders".to_owned(),
                connections_count: 1,
            }
            .into()],
            [GrpcEndpointConfig {
                id: id + 1,
                name: "process".to_owned(),
                id_data_connector: id + 10,
                grpc_method_type: servicelib::api::GrpcMethodType::NoStreaming,
            }
            .into()],
            [],
        )
        .unwrap(),
    ));
    let input = InputStream::new(&input_config, environment.clone());
    let results = Stream::new(&StreamConfig::new(id + 2, "results"), environment);
    input.set_source(&results).unwrap();
    input
        .stream()
        .try_set_consumer(Arc::new(Pipeline { results }), id + 2)
        .unwrap();
    (
        input,
        Arc::new(AtomicUsize::new(0)),
        Arc::new(AtomicUsize::new(0)),
    )
}

fn handler(
    streaming_request: bool,
    eof_count: Arc<AtomicUsize>,
    end_count: Arc<AtomicUsize>,
) -> Handler {
    Handler {
        streaming_request,
        eof_count,
        end_count,
    }
}

struct CollectingSender(Arc<Mutex<Vec<u32>>>);

#[async_trait]
impl Sender<u32> for CollectingSender {
    async fn send(&self, _context: MessageContext, value: u32) -> HandlerResult {
        self.0.lock().unwrap().push(value);
        Ok(())
    }
}

#[tokio::test]
async fn grpc_unary_source_correlates_pipeline_result() {
    let (input, eof_count, end_count) = make_input(1);
    let endpoint = make_grpc_no_streaming_endpoint_consumer(
        input,
        handler(false, eof_count.clone(), end_count.clone()),
    )
    .unwrap();
    let response = endpoint
        .handle(MessageContext::new().with_stream_id("unary-1"), 1)
        .await
        .unwrap();
    assert_eq!(response, 10);
    assert_eq!(eof_count.load(Ordering::Acquire), 1);
    assert_eq!(end_count.load(Ordering::Acquire), 1);
}

#[tokio::test]
async fn grpc_server_streaming_source_waits_for_done() {
    let (input, eof_count, end_count) = make_input(10);
    let endpoint = make_grpc_server_streaming_endpoint_consumer(
        input,
        handler(false, eof_count.clone(), end_count.clone()),
    )
    .unwrap();
    let values = Arc::new(Mutex::new(Vec::new()));
    endpoint
        .handle(
            MessageContext::new(),
            2,
            Arc::new(CollectingSender(values.clone())),
        )
        .await
        .unwrap();
    assert_eq!(&*values.lock().unwrap(), &[20]);
    assert_eq!(eof_count.load(Ordering::Acquire), 1);
    assert_eq!(end_count.load(Ordering::Acquire), 1);
}

#[tokio::test]
async fn grpc_client_streaming_source_consumes_until_eof() {
    let (input, eof_count, end_count) = make_input(20);
    let endpoint = make_grpc_client_streaming_endpoint_consumer(
        input,
        handler(true, eof_count.clone(), end_count.clone()),
    )
    .unwrap();
    let response = endpoint
        .handle(MessageContext::new(), stream::iter(vec![Ok(1), Ok(2)]))
        .await
        .unwrap();
    assert_eq!(response, 30);
    assert_eq!(eof_count.load(Ordering::Acquire), 1);
    assert_eq!(end_count.load(Ordering::Acquire), 1);
}

#[tokio::test]
async fn grpc_bidi_source_streams_responses_and_finishes_once() {
    let (input, eof_count, end_count) = make_input(30);
    let endpoint = make_grpc_bidi_streaming_endpoint_consumer(
        input,
        handler(false, eof_count.clone(), end_count.clone()),
    )
    .unwrap();
    let values = Arc::new(Mutex::new(Vec::new()));
    endpoint
        .handle(
            MessageContext::new(),
            stream::iter(vec![Ok(1), Ok(2)]),
            Arc::new(CollectingSender(values.clone())),
        )
        .await
        .unwrap();
    assert_eq!(&*values.lock().unwrap(), &[10, 20]);
    assert_eq!(eof_count.load(Ordering::Acquire), 1);
    assert_eq!(end_count.load(Ordering::Acquire), 1);
}
