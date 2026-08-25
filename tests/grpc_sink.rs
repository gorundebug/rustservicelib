use std::sync::{
    Arc, Mutex,
    atomic::{AtomicUsize, Ordering},
};

use async_trait::async_trait;
use futures::stream;
use servicelib::{
    Consumer, MessageContext, Payload, Stream,
    datasink::grpc::{
        BidiStreamingCall, BidiStreamingClientFunction, ClientStreamingCall,
        ClientStreamingClientFunction, EndpointHandler, HandlerError, HandlerResult,
        NoStreamingClientFunction, ResultContext, Sender, ServerStreamingClientFunction,
        StreamContext, make_grpc_bidi_streaming_endpoint_consumer,
        make_grpc_client_streaming_endpoint_consumer, make_grpc_no_streaming_endpoint_consumer,
        make_grpc_server_streaming_endpoint_consumer,
    },
    runtime::{
        config::{
            CallSemantics, GrpcDataConnectorConfig, GrpcEndpointConfig, RuntimeConfig,
            SinkStreamConfig, StreamConfig,
        },
        environment::RuntimeEnvironment,
    },
};
use tokio::sync::Mutex as AsyncMutex;

struct Handler {
    end_count: Arc<AtomicUsize>,
}

#[async_trait]
impl EndpointHandler<(), u32, u32, u32, u32, String> for Handler {
    async fn begin_request(
        &self,
        context: MessageContext,
        _stream: StreamContext<u32, u32, String>,
    ) -> HandlerResult<(MessageContext, ())> {
        Ok((context, ()))
    }

    async fn consume_message(
        &self,
        context: MessageContext,
        _stream: StreamContext<u32, u32, String>,
        _handler_state: Arc<AsyncMutex<()>>,
        value: Payload<u32>,
        sender: &dyn Sender<u32>,
        result_context: ResultContext,
    ) -> HandlerResult {
        sender.send(context, *value).await?;
        if *value == 2 {
            result_context.done();
        }
        Ok(())
    }

    async fn handle_response(
        &self,
        context: MessageContext,
        stream: StreamContext<u32, u32, String>,
        _handler_state: Arc<AsyncMutex<()>>,
        response: u32,
    ) -> HandlerResult {
        stream.collect(context, response).await;
        Ok(())
    }

    async fn end_request(
        &self,
        _context: MessageContext,
        _stream: StreamContext<u32, u32, String>,
        _result: &HandlerResult,
        _handler_state: Arc<AsyncMutex<()>>,
    ) {
        self.end_count.fetch_add(1, Ordering::AcqRel);
    }
}

struct ResultCollector(Arc<Mutex<Vec<u32>>>);

type TestSink = Arc<servicelib::operators::SinkStreamWithResult<u32, u32, String>>;
type TestSinkFixture = (Stream<u32>, TestSink, Arc<Mutex<Vec<u32>>>);

#[async_trait]
impl Consumer<u32> for ResultCollector {
    async fn consume(&self, _context: MessageContext, payload: Payload<u32>) {
        self.0.lock().unwrap().push(*payload);
    }
}

fn make_sink(
    environment: RuntimeEnvironment,
    id: i32,
    method: servicelib::api::GrpcMethodType,
) -> TestSinkFixture {
    environment.publish_runtime_config(Arc::new(
        RuntimeConfig::from_parts(
            CallSemantics::FunctionCall,
            [],
            [
                StreamConfig::new(id, "source").into(),
                SinkStreamConfig {
                    stream: StreamConfig::new(id + 1, "grpc sink"),
                    endpoint_id: id + 2,
                }
                .into(),
            ],
            [],
            [GrpcDataConnectorConfig {
                id: 1,
                name: "inventory".to_owned(),
                address: "http://inventory".to_owned(),
                connections_count: 1,
            }
            .into()],
            [GrpcEndpointConfig {
                id: id + 2,
                name: "reserve".to_owned(),
                id_data_connector: 1,
                tracing_enabled: false,
                grpc_method_type: method,
            }
            .into()],
            [],
        )
        .unwrap(),
    ));
    let source = Stream::new(&StreamConfig::new(id, "source"), environment);
    let sink = source
        .sink_with_result::<u32, String>(&SinkStreamConfig {
            stream: StreamConfig::new(id + 1, "grpc sink"),
            endpoint_id: id + 2,
        })
        .unwrap();
    let results = Arc::new(Mutex::new(Vec::new()));
    sink.stream()
        .try_set_consumer(Arc::new(ResultCollector(results.clone())), id + 3)
        .unwrap();
    (source, sink, results)
}

#[tokio::test]
async fn grpc_unary_sink_preserves_lifecycle() {
    let environment = RuntimeEnvironment::default();
    let (source, sink, results) = make_sink(
        environment.clone(),
        1,
        servicelib::api::GrpcMethodType::NoStreaming,
    );
    let end_count = Arc::new(AtomicUsize::new(0));
    let client: NoStreamingClientFunction<u32, u32> =
        Arc::new(|_context, request| Box::pin(async move { Ok(request * 10) }));
    make_grpc_no_streaming_endpoint_consumer(
        &sink,
        Handler {
            end_count: end_count.clone(),
        },
        client,
    )
    .unwrap();

    source.emit(MessageContext::new(), Payload::new(1)).await;
    assert_eq!(&*results.lock().unwrap(), &[10]);
    assert_eq!(end_count.load(Ordering::Acquire), 1);
    assert!(
        environment
            .metrics()
            .render_prometheus()
            .contains("rpc_client_call_duration_seconds_count")
    );
}

#[tokio::test]
async fn grpc_server_streaming_sink_handles_every_response() {
    let environment = RuntimeEnvironment::default();
    let (source, sink, results) = make_sink(
        environment,
        10,
        servicelib::api::GrpcMethodType::ServerStreaming,
    );
    let end_count = Arc::new(AtomicUsize::new(0));
    let client: ServerStreamingClientFunction<u32, u32> = Arc::new(|_context, request| {
        Box::pin(async move { Ok(Box::pin(stream::iter(vec![Ok(request), Ok(request + 1)])) as _) })
    });
    make_grpc_server_streaming_endpoint_consumer(
        &sink,
        Handler {
            end_count: end_count.clone(),
        },
        client,
    )
    .unwrap();

    source.emit(MessageContext::new(), Payload::new(5)).await;
    assert_eq!(&*results.lock().unwrap(), &[5, 6]);
    assert_eq!(end_count.load(Ordering::Acquire), 1);
}

struct MockClientStreamingCall {
    sent: Mutex<Vec<u32>>,
}

#[async_trait]
impl ClientStreamingCall<u32, u32> for MockClientStreamingCall {
    async fn send(&self, request: u32) -> HandlerResult {
        self.sent.lock().unwrap().push(request);
        Ok(())
    }

    async fn close_and_recv(&self) -> HandlerResult<u32> {
        Ok(self.sent.lock().unwrap().iter().sum())
    }
}

#[tokio::test]
async fn grpc_client_streaming_sink_reuses_stream_id_until_done() {
    let environment = RuntimeEnvironment::default();
    let (source, sink, results) = make_sink(
        environment,
        20,
        servicelib::api::GrpcMethodType::ClientStreaming,
    );
    let end_count = Arc::new(AtomicUsize::new(0));
    let calls = Arc::new(AtomicUsize::new(0));
    let calls_clone = calls.clone();
    let client: ClientStreamingClientFunction<u32, u32> = Arc::new(move |_context| {
        calls_clone.fetch_add(1, Ordering::AcqRel);
        Box::pin(async {
            Ok(Arc::new(MockClientStreamingCall {
                sent: Mutex::new(Vec::new()),
            }) as Arc<dyn ClientStreamingCall<u32, u32>>)
        })
    });
    make_grpc_client_streaming_endpoint_consumer(
        &sink,
        Handler {
            end_count: end_count.clone(),
        },
        client,
    )
    .unwrap();

    let context = MessageContext::new().with_stream_id("batch-1");
    source.emit(context.clone(), Payload::new(1)).await;
    source.emit(context, Payload::new(2)).await;
    tokio::time::timeout(std::time::Duration::from_secs(1), async {
        while end_count.load(Ordering::Acquire) != 1 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();

    assert_eq!(calls.load(Ordering::Acquire), 1);
    assert_eq!(&*results.lock().unwrap(), &[3]);
}

struct MockBidiStreamingCall {
    responses_tx: Mutex<Option<tokio::sync::mpsc::UnboundedSender<u32>>>,
    responses_rx: AsyncMutex<tokio::sync::mpsc::UnboundedReceiver<u32>>,
}

impl MockBidiStreamingCall {
    fn new() -> Self {
        let (responses_tx, responses_rx) = tokio::sync::mpsc::unbounded_channel();
        Self {
            responses_tx: Mutex::new(Some(responses_tx)),
            responses_rx: AsyncMutex::new(responses_rx),
        }
    }
}

#[async_trait]
impl BidiStreamingCall<u32, u32> for MockBidiStreamingCall {
    async fn send(&self, request: u32) -> HandlerResult {
        self.responses_tx
            .lock()
            .unwrap()
            .as_ref()
            .ok_or_else(|| -> HandlerError { "send side closed".into() })?
            .send(request * 10)?;
        Ok(())
    }

    async fn recv(&self) -> HandlerResult<Option<u32>> {
        Ok(self.responses_rx.lock().await.recv().await)
    }

    async fn close_send(&self) -> HandlerResult {
        self.responses_tx.lock().unwrap().take();
        Ok(())
    }
}

#[tokio::test]
async fn grpc_bidi_streaming_sink_receives_until_done() {
    let environment = RuntimeEnvironment::default();
    let (source, sink, results) = make_sink(
        environment,
        30,
        servicelib::api::GrpcMethodType::BidirectionalStreaming,
    );
    let end_count = Arc::new(AtomicUsize::new(0));
    let calls = Arc::new(AtomicUsize::new(0));
    let calls_clone = calls.clone();
    let client: BidiStreamingClientFunction<u32, u32> = Arc::new(move |_context| {
        calls_clone.fetch_add(1, Ordering::AcqRel);
        Box::pin(async {
            Ok(Arc::new(MockBidiStreamingCall::new()) as Arc<dyn BidiStreamingCall<u32, u32>>)
        })
    });
    make_grpc_bidi_streaming_endpoint_consumer(
        &sink,
        Handler {
            end_count: end_count.clone(),
        },
        client,
    )
    .unwrap();

    let context = MessageContext::new().with_stream_id("bidi-1");
    source.emit(context.clone(), Payload::new(1)).await;
    source.emit(context, Payload::new(2)).await;
    tokio::time::timeout(std::time::Duration::from_secs(1), async {
        while end_count.load(Ordering::Acquire) != 1 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();

    assert_eq!(calls.load(Ordering::Acquire), 1);
    assert_eq!(&*results.lock().unwrap(), &[10, 20]);
}
