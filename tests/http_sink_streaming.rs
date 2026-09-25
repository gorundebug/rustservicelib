use async_trait::async_trait;
use servicelib::{
    Consumer, MessageContext, Payload, Stream,
    api::HTTPMethodType,
    datasink::http::{
        Client, EndpointHandler, HandlerError, HandlerResult, Request, Requester, Response,
        ResponseBody, StreamContext, make_endpoint_consumer,
    },
    runtime::{
        config::{
            CallSemantics, HttpDataConnectorConfig, HttpEndpointConfig, RuntimeConfig,
            SinkStreamConfig, StreamConfig,
        },
        environment::RuntimeEnvironment,
    },
};
use std::{
    io,
    pin::Pin,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    task::{Context, Poll},
};
use tokio::{
    io::{AsyncRead, AsyncReadExt, ReadBuf},
    sync::Semaphore,
};

struct Reader {
    reads: Arc<AtomicUsize>,
    drops: Arc<AtomicUsize>,
}
impl AsyncRead for Reader {
    fn poll_read(
        self: Pin<&mut Self>,
        _: &mut Context<'_>,
        _: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        self.reads.fetch_add(1, Ordering::SeqCst);
        Poll::Pending
    }
}
impl Drop for Reader {
    fn drop(&mut self) {
        self.drops.fetch_add(1, Ordering::SeqCst);
    }
}
struct TestClient {
    reads: Arc<AtomicUsize>,
    drops: Arc<AtomicUsize>,
}
#[async_trait]
impl Client for TestClient {
    async fn perform(&self, _: MessageContext, _: Request) -> Result<Response, HandlerError> {
        Ok(Response {
            status: 200,
            headers: Default::default(),
            body: ResponseBody::new(Reader {
                reads: self.reads.clone(),
                drops: self.drops.clone(),
            }),
        })
    }
}
struct Handler {
    retained: Arc<Mutex<Option<Response>>>,
    entered: Arc<Semaphore>,
    release: Arc<Semaphore>,
    drops: Arc<AtomicUsize>,
    ended: Arc<AtomicUsize>,
    fail: bool,
}
#[async_trait]
impl EndpointHandler<(), u32, u32, String> for Handler {
    async fn begin_request(
        &self,
        ctx: MessageContext,
        _: StreamContext<u32, u32, String>,
    ) -> Result<(MessageContext, ()), HandlerError> {
        Ok((ctx, ()))
    }
    async fn consume_message(
        &self,
        _: MessageContext,
        _: StreamContext<u32, u32, String>,
        _: &mut (),
        _: Payload<u32>,
        request: &mut Requester,
    ) -> HandlerResult {
        request.new_request("GET", "http://test/response", Vec::new());
        Ok(())
    }
    async fn handle_response(
        &self,
        ctx: MessageContext,
        stream: StreamContext<u32, u32, String>,
        _: &mut (),
        response: Response,
    ) -> HandlerResult {
        assert_eq!(response.status, 200);
        *self.retained.lock().unwrap() = Some(response);
        self.entered.add_permits(1);
        self.release.acquire().await.unwrap().forget();
        if self.fail {
            return Err("intentional handler error".into());
        }
        stream.collect(ctx, 17).await;
        Ok(())
    }
    async fn end_request(
        &self,
        _: MessageContext,
        _: StreamContext<u32, u32, String>,
        result: &HandlerResult,
        _: (),
    ) {
        assert_eq!(result.is_err(), self.fail);
        assert_eq!(
            self.drops.load(Ordering::SeqCst),
            0,
            "body closed before EndRequest"
        );
        self.ended.fetch_add(1, Ordering::SeqCst);
    }
}
struct Collector(Arc<Mutex<Vec<u32>>>);
impl Consumer<u32> for Collector {
    async fn consume(&self, _: MessageContext, value: Payload<u32>) {
        self.0.lock().unwrap().push(*value);
    }
}

async fn check_retained_body(fail: bool) {
    let environment = RuntimeEnvironment::default();
    let source_config = StreamConfig::new(1, "input");
    let sink_config = SinkStreamConfig {
        stream: StreamConfig::new(2, "output"),
        endpoint_id: 3,
    };
    environment.publish_runtime_config(Arc::new(
        RuntimeConfig::from_parts(
            CallSemantics::FunctionCall,
            [],
            [source_config.clone().into(), sink_config.clone().into()],
            [],
            [],
            [],
            [],
        )
        .unwrap(),
    ));
    let source = Stream::new(&source_config, environment.clone());
    let sink = source
        .sink_with_result::<u32, String>(&sink_config)
        .unwrap();
    let results = Arc::new(Mutex::new(Vec::new()));
    sink.stream()
        .try_set_consumer(Arc::new(Collector(results.clone())), 4)
        .unwrap();
    let reads = Arc::new(AtomicUsize::new(0));
    let drops = Arc::new(AtomicUsize::new(0));
    let ended = Arc::new(AtomicUsize::new(0));
    let retained = Arc::new(Mutex::new(None));
    let entered = Arc::new(Semaphore::new(0));
    let release = Arc::new(Semaphore::new(0));
    make_endpoint_consumer(
        &sink,
        HttpEndpointConfig {
            id: 3,
            name: "response".into(),
            id_data_connector: 5,
            tracing_enabled: false,
            http_method_type: HTTPMethodType::GET,
            path: "/response".into(),
        },
        HttpDataConnectorConfig {
            id: 5,
            name: "test".into(),
            host: "test".into(),
            port: 80,
            address: "http://test".into(),
            use_dedicated_listener: false,
        },
        Arc::new(TestClient {
            reads: reads.clone(),
            drops: drops.clone(),
        }),
        Handler {
            retained: retained.clone(),
            entered: entered.clone(),
            release: release.clone(),
            drops: drops.clone(),
            ended: ended.clone(),
            fail,
        },
    )
    .unwrap();
    let emitted = tokio::spawn(async move {
        source.emit(MessageContext::new(), Payload::new(42)).await;
    });
    tokio::time::timeout(std::time::Duration::from_secs(1), entered.acquire())
        .await
        .unwrap()
        .unwrap()
        .forget();
    assert!(
        !emitted.is_finished(),
        "direct Consume returned before handler completion"
    );
    assert_eq!(
        reads.load(Ordering::SeqCst),
        0,
        "framework eagerly read the body"
    );
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    release.add_permits(1);
    tokio::time::timeout(std::time::Duration::from_secs(1), emitted)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(ended.load(Ordering::SeqCst), 1);
    assert_eq!(
        drops.load(Ordering::SeqCst),
        1,
        "retained response kept transport alive"
    );
    assert_eq!(reads.load(Ordering::SeqCst), 0);
    assert_eq!(
        &*results.lock().unwrap(),
        if fail { &[][..] } else { &[17][..] }
    );
    let mut response = retained.lock().unwrap().take().unwrap();
    assert!(response.body.read(&mut [0_u8; 1]).await.is_err());
}

#[tokio::test]
async fn headers_only_handler_waits_and_closes_retained_body_after_end() {
    check_retained_body(false).await;
}
#[tokio::test]
async fn handler_error_still_finalizes_and_closes_retained_body() {
    check_retained_body(true).await;
}

#[tokio::test]
async fn body_is_an_async_reader_and_buffering_is_explicit() {
    let mut body: ResponseBody = b"abcdef".to_vec().into();
    assert_eq!(body.content_length(), Some(6));
    let mut first = [0_u8; 2];
    body.read_exact(&mut first).await.unwrap();
    assert_eq!(&first, b"ab");
    assert_eq!(body.bytes().await.unwrap(), b"cdef");
    assert!(body.bytes().await.unwrap().is_empty());
    body.close();
    assert!(body.bytes().await.is_err());
}
