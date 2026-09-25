use std::{
    convert::Infallible,
    sync::{
        Arc, Mutex, Weak,
        atomic::{AtomicUsize, Ordering},
    },
    task::{Context, Poll},
    time::Duration,
};

use async_trait::async_trait;
use futures::StreamExt;
use servicelib::{
    MessageContext,
    api::GrpcMethodType,
    datasource::grpc::{
        BidiStreamingEndpointConsumer, ClientStreamingEndpointConsumer, EndpointHandler,
        HandlerResult, NoStreamingEndpointConsumer, ResultContext, Sender,
        ServerStreamingEndpointConsumer, StreamContext, make_grpc_bidi_streaming_endpoint_consumer,
        make_grpc_client_streaming_endpoint_consumer, make_grpc_no_streaming_endpoint_consumer,
        make_grpc_server_streaming_endpoint_consumer,
    },
    operators::InputStream,
    runtime::{
        common::Payload,
        config::{
            CallSemantics, GrpcDataConnectorConfig, GrpcEndpointConfig, InputStreamConfig,
            RuntimeConfig, StreamConfig,
        },
        environment::RuntimeEnvironment,
        stream::Stream,
    },
};
use tokio::sync::{Mutex as AsyncMutex, Semaphore};
use tonic::{
    Request, Response, Status,
    codec::ProstCodec,
    codegen::{Body, BoxFuture, Service, StdError, http},
};

type Results = ResultContext<bool, u32, u32, u32, String>;
type Unary = NoStreamingEndpointConsumer<bool, u32, u32, u32, u32, String, Handler>;
type Client = ClientStreamingEndpointConsumer<bool, u32, u32, u32, u32, String, Handler>;
type ServerStreaming = ServerStreamingEndpointConsumer<bool, u32, u32, u32, u32, String, Handler>;
type Bidi = BidiStreamingEndpointConsumer<bool, u32, u32, u32, u32, String, Handler>;

struct Probe {
    begins: AtomicUsize,
    consumed: AtomicUsize,
    original_ended: AtomicUsize,
    callbacks: Arc<AtomicUsize>,
    entered: Semaphore,
    release_consume: Semaphore,
    ending: Semaphore,
    release_end: Semaphore,
    context: Mutex<Option<MessageContext>>,
    result: Mutex<Option<Weak<Results>>>,
}

struct Handler(Arc<Probe>);

#[async_trait]
impl EndpointHandler<bool, u32, u32, u32, u32, String> for Handler {
    async fn begin_request(
        &self,
        context: MessageContext,
        _: StreamContext<u32, u32, String>,
    ) -> HandlerResult<(MessageContext, bool)> {
        Ok((context, self.0.begins.fetch_add(1, Ordering::SeqCst) == 0))
    }

    async fn consume_message(
        &self,
        context: MessageContext,
        _: StreamContext<u32, u32, String>,
        state: Arc<AsyncMutex<bool>>,
        request: u32,
        result: Arc<Results>,
        sender: Arc<dyn Sender<u32>>,
    ) -> HandlerResult<MessageContext> {
        let first = *state.lock().await;
        let owner = result.clone();
        let callbacks = self.0.callbacks.clone();
        result.set_result_callback(
            "reply",
            Arc::new(move |_, _, _, _, _| {
                let owner = owner.clone();
                let callbacks = callbacks.clone();
                Box::pin(async move {
                    callbacks.fetch_add(1, Ordering::SeqCst);
                    owner.done();
                    false
                })
            }),
        );
        if first {
            *self.0.context.lock().unwrap() = Some(context.clone());
            *self.0.result.lock().unwrap() = Some(Arc::downgrade(&result));
            self.0.entered.add_permits(1);
            self.0.release_consume.acquire().await.unwrap().forget();
        }
        let sent = sender.send(context.clone(), request).await;
        self.0.consumed.fetch_add(1, Ordering::SeqCst);
        sent?;
        result.done();
        Ok(context)
    }

    async fn get_message_id(
        &self,
        _: &MessageContext,
        _: &StreamContext<u32, u32, String>,
        _: Arc<AsyncMutex<bool>>,
        _: &u32,
    ) -> String {
        "reply".into()
    }

    async fn eof(
        &self,
        _: MessageContext,
        _: StreamContext<u32, u32, String>,
        _: Arc<AsyncMutex<bool>>,
    ) {
    }

    async fn end_request(
        &self,
        _: MessageContext,
        _: StreamContext<u32, u32, String>,
        _: &HandlerResult,
        state: Arc<AsyncMutex<bool>>,
    ) -> HandlerResult {
        if *state.lock().await {
            self.0.ending.add_permits(1);
            self.0.release_end.acquire().await.unwrap().forget();
            self.0.original_ended.fetch_add(1, Ordering::SeqCst);
        }
        Ok(())
    }
}

#[derive(Clone)]
enum Endpoint {
    Unary(Arc<Unary>),
    Client(Arc<Client>),
    Server(Arc<ServerStreaming>),
    Bidi(Arc<Bidi>),
}

#[derive(Clone)]
struct TestServer(Endpoint);

struct ResponseStream {
    receiver: tokio::sync::mpsc::Receiver<Result<u32, Status>>,
    context: MessageContext,
}

impl futures::Stream for ResponseStream {
    type Item = Result<u32, Status>;
    fn poll_next(self: std::pin::Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.get_mut().receiver.poll_recv(cx)
    }
}

impl Drop for ResponseStream {
    fn drop(&mut self) {
        self.context.cancel();
    }
}

struct ResponseSender(tokio::sync::mpsc::Sender<Result<u32, Status>>);

#[async_trait]
impl Sender<u32> for ResponseSender {
    async fn send(&self, context: MessageContext, value: u32) -> HandlerResult {
        tokio::select! {
            _ = context.cancelled() => Err(Box::new(Status::cancelled("RPC cancelled")) as _),
            result = self.0.send(Ok(value)) => result.map_err(|_| Box::new(Status::cancelled("response stream closed")) as _),
        }
    }
}

struct ServerMethod(Arc<ServerStreaming>);

impl tonic::server::ServerStreamingService<u32> for ServerMethod {
    type Response = u32;
    type ResponseStream = ResponseStream;
    type Future = BoxFuture<Response<ResponseStream>, Status>;

    fn call(&mut self, request: Request<u32>) -> Self::Future {
        let endpoint = self.0.clone();
        Box::pin(async move {
            let context = MessageContext::from_tonic_request_with_tracing(&request, false);
            let task_context = context.clone();
            let (sender, receiver) = tokio::sync::mpsc::channel(16);
            tokio::spawn(async move {
                if let Err(error) = endpoint
                    .handle(
                        task_context,
                        request.into_inner(),
                        Arc::new(ResponseSender(sender.clone())),
                    )
                    .await
                {
                    let _ = sender.send(Err(Status::internal(error.to_string()))).await;
                }
            });
            Ok(Response::new(ResponseStream { receiver, context }))
        })
    }
}

struct BidiMethod(Arc<Bidi>);

impl tonic::server::StreamingService<u32> for BidiMethod {
    type Response = u32;
    type ResponseStream = ResponseStream;
    type Future = BoxFuture<Response<ResponseStream>, Status>;

    fn call(&mut self, request: Request<tonic::Streaming<u32>>) -> Self::Future {
        let endpoint = self.0.clone();
        Box::pin(async move {
            let context = MessageContext::from_tonic_request_with_tracing(&request, false);
            let task_context = context.clone();
            let requests = request.into_inner().map(|result| {
                result
                    .map_err(|error| Box::new(error) as servicelib::datasource::grpc::HandlerError)
            });
            let (sender, receiver) = tokio::sync::mpsc::channel(16);
            tokio::spawn(async move {
                if let Err(error) = endpoint
                    .handle(
                        task_context,
                        requests,
                        Arc::new(ResponseSender(sender.clone())),
                    )
                    .await
                {
                    let _ = sender.send(Err(Status::internal(error.to_string()))).await;
                }
            });
            Ok(Response::new(ResponseStream { receiver, context }))
        })
    }
}

struct UnaryMethod(Arc<Unary>);

impl tonic::server::UnaryService<u32> for UnaryMethod {
    type Response = u32;
    type Future = BoxFuture<Response<u32>, Status>;

    fn call(&mut self, request: Request<u32>) -> Self::Future {
        let endpoint = self.0.clone();
        Box::pin(async move {
            let context = MessageContext::from_tonic_request_with_tracing(&request, false);
            endpoint
                .handle(context, request.into_inner())
                .await
                .map(Response::new)
                .map_err(|error| Status::internal(error.to_string()))
        })
    }
}

struct ClientMethod(Arc<Client>);

impl tonic::server::ClientStreamingService<u32> for ClientMethod {
    type Response = u32;
    type Future = BoxFuture<Response<u32>, Status>;

    fn call(&mut self, request: Request<tonic::Streaming<u32>>) -> Self::Future {
        let endpoint = self.0.clone();
        Box::pin(async move {
            let context = MessageContext::from_tonic_request_with_tracing(&request, false);
            let requests = request.into_inner().map(|result| {
                result
                    .map_err(|error| Box::new(error) as servicelib::datasource::grpc::HandlerError)
            });
            endpoint
                .handle(context, requests)
                .await
                .map(Response::new)
                .map_err(|error| Status::internal(error.to_string()))
        })
    }
}

impl tonic::server::NamedService for TestServer {
    const NAME: &'static str = "parity.Lifetime";
}

impl<B> Service<http::Request<B>> for TestServer
where
    B: Body + Send + 'static,
    B::Error: Into<StdError> + Send + 'static,
{
    type Response = http::Response<tonic::body::BoxBody>;
    type Error = Infallible;
    type Future = BoxFuture<Self::Response, Self::Error>;

    fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, request: http::Request<B>) -> Self::Future {
        let endpoint = self.0.clone();
        Box::pin(async move {
            let mut grpc = tonic::server::Grpc::new(ProstCodec::<u32, u32>::default());
            Ok(match endpoint {
                Endpoint::Unary(endpoint) => grpc.unary(UnaryMethod(endpoint), request).await,
                Endpoint::Client(endpoint) => {
                    grpc.client_streaming(ClientMethod(endpoint), request).await
                }
                Endpoint::Server(endpoint) => {
                    grpc.server_streaming(ServerMethod(endpoint), request).await
                }
                Endpoint::Bidi(endpoint) => grpc.streaming(BidiMethod(endpoint), request).await,
            })
        })
    }
}

struct ServerTask(tokio::task::JoinHandle<()>);

impl Drop for ServerTask {
    fn drop(&mut self) {
        self.0.abort();
    }
}

async fn call(
    channel: tonic::transport::Channel,
    streaming: bool,
) -> Result<Response<u32>, Box<Status>> {
    let mut client = tonic::client::Grpc::new(channel);
    client
        .ready()
        .await
        .map_err(|error| Status::unknown(error.to_string()))?;
    let path = http::uri::PathAndQuery::from_static("/parity.Lifetime/Call");
    if streaming {
        let mut request = Request::new(tokio_stream::iter([42_u32]));
        request
            .metadata_mut()
            .insert("x-stream-id", "network-rpc".parse().unwrap());
        client
            .client_streaming(request, path, ProstCodec::<u32, u32>::default())
            .await
            .map_err(Box::new)
    } else {
        let mut request = Request::new(42_u32);
        request
            .metadata_mut()
            .insert("x-stream-id", "network-rpc".parse().unwrap());
        client
            .unary(request, path, ProstCodec::<u32, u32>::default())
            .await
            .map_err(Box::new)
    }
}

async fn gate(semaphore: &Semaphore) {
    tokio::time::timeout(Duration::from_secs(5), semaphore.acquire())
        .await
        .unwrap()
        .unwrap()
        .forget();
}

async fn disconnected_rpc(streaming: bool, with_result: bool) {
    let environment = RuntimeEnvironment::default();
    let config = InputStreamConfig {
        stream: StreamConfig::new(1, "input"),
        endpoint_id: 4,
    };
    let mode = if streaming {
        GrpcMethodType::ClientStreaming
    } else {
        GrpcMethodType::NoStreaming
    };
    environment.publish_runtime_config(Arc::new(
        RuntimeConfig::from_parts(
            CallSemantics::FunctionCall,
            [],
            [config.clone().into()],
            [],
            [GrpcDataConnectorConfig {
                id: 10,
                name: "grpc".into(),
                address: "http://localhost".into(),
                connections_count: 1,
            }
            .into()],
            [GrpcEndpointConfig {
                id: 4,
                name: "request".into(),
                id_data_connector: 10,
                tracing_enabled: false,
                grpc_method_type: mode,
            }
            .into()],
            [],
        )
        .unwrap(),
    ));
    let input = InputStream::<u32, u32, String>::new(&config, environment.clone());
    let results = Stream::new(&StreamConfig::new(2, "results"), environment);
    if with_result {
        input.set_source(&results).unwrap();
    }
    let probe = Arc::new(Probe {
        begins: AtomicUsize::new(0),
        consumed: AtomicUsize::new(0),
        original_ended: AtomicUsize::new(0),
        callbacks: Arc::new(AtomicUsize::new(0)),
        entered: Semaphore::new(0),
        release_consume: Semaphore::new(0),
        ending: Semaphore::new(0),
        release_end: Semaphore::new(0),
        context: Mutex::new(None),
        result: Mutex::new(None),
    });
    let endpoint = if streaming {
        Endpoint::Client(
            make_grpc_client_streaming_endpoint_consumer(input, Handler(probe.clone())).unwrap(),
        )
    } else {
        Endpoint::Unary(
            make_grpc_no_streaming_endpoint_consumer(input, Handler(probe.clone())).unwrap(),
        )
    };
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let _server = ServerTask(tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(TestServer(endpoint))
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
            .await
            .unwrap();
    }));
    let channel = tonic::transport::Endpoint::from_shared(format!("http://{address}"))
        .unwrap()
        .connect()
        .await
        .unwrap();
    let active = tokio::spawn(call(channel.clone(), streaming));
    gate(&probe.entered).await;
    let context = probe.context.lock().unwrap().clone().unwrap();
    let result = probe.result.lock().unwrap().clone().unwrap();
    assert_eq!(context.stream_id(), Some("network-rpc"));

    active.abort();
    assert!(active.await.unwrap_err().is_cancelled());
    tokio::time::timeout(Duration::from_secs(5), context.cancelled())
        .await
        .expect("wire cancellation did not reach the admitted request");
    assert_eq!(probe.consumed.load(Ordering::SeqCst), 0);
    assert_eq!(probe.original_ended.load(Ordering::SeqCst), 0);
    tokio::time::timeout(
        Duration::from_secs(5),
        results.emit(context.clone(), Payload::new(7)),
    )
    .await
    .unwrap();
    let callbacks = usize::from(with_result);
    assert_eq!(
        probe.callbacks.load(Ordering::SeqCst),
        callbacks,
        "cancellation closed callbacks before ConsumeMessage completed"
    );

    probe.release_consume.add_permits(1);
    gate(&probe.ending).await;
    assert_eq!(
        probe.consumed.load(Ordering::SeqCst),
        1,
        "accepted handler did not complete"
    );
    tokio::time::timeout(
        Duration::from_secs(5),
        results.emit(context, Payload::new(8)),
    )
    .await
    .unwrap();
    assert_eq!(
        probe.callbacks.load(Ordering::SeqCst),
        callbacks,
        "EndRequest admitted a late result"
    );
    let duplicate = tokio::time::timeout(Duration::from_secs(5), call(channel.clone(), streaming))
        .await
        .unwrap();
    assert!(duplicate.unwrap_err().message().contains("duplicate"));
    assert_eq!(probe.original_ended.load(Ordering::SeqCst), 0);
    probe.release_end.add_permits(1);
    tokio::time::timeout(Duration::from_secs(5), async {
        while result.upgrade().is_some() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("wire-cancelled request retained its result context");
    assert_eq!(probe.original_ended.load(Ordering::SeqCst), 1);
    let response = tokio::time::timeout(Duration::from_secs(5), call(channel, streaming))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(response.into_inner(), 42);
    assert_eq!(probe.consumed.load(Ordering::SeqCst), 2);
    assert_eq!(probe.callbacks.load(Ordering::SeqCst), callbacks);
}

#[tokio::test]
async fn unary_wire_cancellation_preserves_handler_and_finalizer() {
    for with_result in [false, true] {
        disconnected_rpc(false, with_result).await;
    }
}

#[tokio::test]
async fn client_stream_wire_cancellation_preserves_handler_and_finalizer() {
    for with_result in [false, true] {
        disconnected_rpc(true, with_result).await;
    }
}

async fn open_response(
    channel: tonic::transport::Channel,
    bidi: bool,
) -> Result<Response<tonic::Streaming<u32>>, Box<Status>> {
    let mut client = tonic::client::Grpc::new(channel);
    client
        .ready()
        .await
        .map_err(|error| Status::unknown(error.to_string()))?;
    let path = http::uri::PathAndQuery::from_static("/parity.Lifetime/Call");
    if bidi {
        let mut request = Request::new(tokio_stream::iter([42_u32]));
        request
            .metadata_mut()
            .insert("x-stream-id", "network-rpc".parse().unwrap());
        client
            .streaming(request, path, ProstCodec::<u32, u32>::default())
            .await
            .map_err(Box::new)
    } else {
        let mut request = Request::new(42_u32);
        request
            .metadata_mut()
            .insert("x-stream-id", "network-rpc".parse().unwrap());
        client
            .server_streaming(request, path, ProstCodec::<u32, u32>::default())
            .await
            .map_err(Box::new)
    }
}

async fn dropped_response(bidi: bool, with_result: bool) {
    let environment = RuntimeEnvironment::default();
    let config = InputStreamConfig {
        stream: StreamConfig::new(1, "input"),
        endpoint_id: 4,
    };
    let mode = if bidi {
        GrpcMethodType::BidirectionalStreaming
    } else {
        GrpcMethodType::ServerStreaming
    };
    environment.publish_runtime_config(Arc::new(
        RuntimeConfig::from_parts(
            CallSemantics::FunctionCall,
            [],
            [config.clone().into()],
            [],
            [GrpcDataConnectorConfig {
                id: 10,
                name: "grpc".into(),
                address: "http://localhost".into(),
                connections_count: 1,
            }
            .into()],
            [GrpcEndpointConfig {
                id: 4,
                name: "request".into(),
                id_data_connector: 10,
                tracing_enabled: false,
                grpc_method_type: mode,
            }
            .into()],
            [],
        )
        .unwrap(),
    ));
    let input = InputStream::<u32, u32, String>::new(&config, environment.clone());
    let results = Stream::new(&StreamConfig::new(2, "results"), environment);
    if with_result {
        input.set_source(&results).unwrap();
    }
    let probe = Arc::new(Probe {
        begins: AtomicUsize::new(0),
        consumed: AtomicUsize::new(0),
        original_ended: AtomicUsize::new(0),
        callbacks: Arc::new(AtomicUsize::new(0)),
        entered: Semaphore::new(0),
        release_consume: Semaphore::new(0),
        ending: Semaphore::new(0),
        release_end: Semaphore::new(0),
        context: Mutex::new(None),
        result: Mutex::new(None),
    });
    let endpoint = if bidi {
        Endpoint::Bidi(
            make_grpc_bidi_streaming_endpoint_consumer(input, Handler(probe.clone())).unwrap(),
        )
    } else {
        Endpoint::Server(
            make_grpc_server_streaming_endpoint_consumer(input, Handler(probe.clone())).unwrap(),
        )
    };
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let _server = ServerTask(tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(TestServer(endpoint))
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
            .await
            .unwrap();
    }));
    let channel = tonic::transport::Endpoint::from_shared(format!("http://{address}"))
        .unwrap()
        .connect()
        .await
        .unwrap();
    let response =
        tokio::time::timeout(Duration::from_secs(5), open_response(channel.clone(), bidi))
            .await
            .unwrap()
            .unwrap();
    gate(&probe.entered).await;
    let context = probe.context.lock().unwrap().clone().unwrap();
    let weak = probe.result.lock().unwrap().clone().unwrap();
    drop(response);
    tokio::time::timeout(Duration::from_secs(5), context.cancelled())
        .await
        .expect("dropping the wire response did not cancel the server context");
    assert_eq!(probe.consumed.load(Ordering::SeqCst), 0);
    // Like Go: cancellation does not close result admission while ConsumeMessage
    // is still active. Closure begins at finalization, before EndRequest.
    tokio::time::timeout(
        Duration::from_secs(5),
        results.emit(context.clone(), Payload::new(7)),
    )
    .await
    .unwrap();
    let callbacks = usize::from(with_result);
    assert_eq!(probe.callbacks.load(Ordering::SeqCst), callbacks);
    probe.release_consume.add_permits(1);
    gate(&probe.ending).await;
    assert_eq!(probe.consumed.load(Ordering::SeqCst), 1);
    tokio::time::timeout(
        Duration::from_secs(5),
        results.emit(context, Payload::new(8)),
    )
    .await
    .unwrap();
    assert_eq!(
        probe.callbacks.load(Ordering::SeqCst),
        callbacks,
        "EndRequest admitted a late result"
    );
    let rejection = tokio::time::timeout(Duration::from_secs(5), async {
        match open_response(channel.clone(), bidi).await {
            Err(error) => error,
            Ok(response) => Box::new(response.into_inner().message().await.unwrap_err()),
        }
    })
    .await
    .unwrap();
    assert!(rejection.message().contains("duplicate"));
    assert_eq!(probe.original_ended.load(Ordering::SeqCst), 0);
    probe.release_end.add_permits(1);
    tokio::time::timeout(Duration::from_secs(5), async {
        while weak.upgrade().is_some() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("dropped response retained the original request");
    assert_eq!(probe.original_ended.load(Ordering::SeqCst), 1);
    let messages = tokio::time::timeout(Duration::from_secs(5), async {
        let mut response = open_response(channel, bidi).await.unwrap().into_inner();
        let value = response.message().await.unwrap();
        let eof = response.message().await.unwrap();
        (value, eof)
    })
    .await
    .unwrap();
    assert_eq!(messages, (Some(42), None));
    assert_eq!(probe.consumed.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn server_stream_response_drop_preserves_handler_and_finalizer() {
    for with_result in [false, true] {
        dropped_response(false, with_result).await;
    }
}

#[tokio::test]
async fn bidi_response_drop_preserves_handler_and_finalizer() {
    for with_result in [false, true] {
        dropped_response(true, with_result).await;
    }
}
