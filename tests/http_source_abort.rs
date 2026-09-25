use std::{
    sync::{Arc, Mutex, atomic::{AtomicBool, AtomicUsize, Ordering}},
    time::Duration,
};

use async_trait::async_trait;
use axum::{body::Body, http::{Request, StatusCode}};
use servicelib::{
    MessageContext,
    api::HTTPMethodType,
    datasource::http::{
        EndpointHandler, HandlerData, HandlerError, HandlerResult,
        AxumDataSource, ResultCallback, ResultContext,
    },
    operators::InputStream,
    runtime::{
        common::Payload,
        config::{CallSemantics, HttpDataConnectorConfig, HttpEndpointConfig, InputStreamConfig, RuntimeConfig, StreamConfig},
        datasource::StreamContext,
        environment::{Lifecycle, RuntimeEnvironment},
        stream::Stream,
    },
};
use tokio::sync::{Mutex as AsyncMutex, Semaphore};
use tower::ServiceExt;

struct Probe {
    entered: Semaphore,
    context: Mutex<Option<MessageContext>>,
    callbacks: AtomicUsize,
    ended: Semaphore,
    block_end: AtomicBool,
    end_entered: Semaphore,
    release_end: Semaphore,
}

struct Handler(Arc<Probe>);

#[async_trait]
impl EndpointHandler<(), (), (), u32, u32, String> for Handler {
    async fn begin_request(&self, context: MessageContext, _: StreamContext<u32, u32, String>, _: HandlerData) -> Result<(MessageContext, ()), HandlerError> {
        Ok((context, ()))
    }

    async fn consume_message(&self, context: MessageContext, _: StreamContext<u32, u32, String>, _: Arc<AsyncMutex<()>>, data: HandlerData, result: Arc<ResultContext<(), (), (), u32, u32, String>>) -> HandlerResult {
        let probe = self.0.clone();
        result.set_result_callback("reply", ResultCallback::new(move |_, _, _, _, _| {
            let probe = probe.clone();
            Box::pin(async move {
                probe.callbacks.fetch_add(1, Ordering::SeqCst);
                true
            })
        }));
        *self.0.context.lock().unwrap() = Some(context);
        self.0.entered.add_permits(1);
        if data.headers.contains_key("x-complete") {
            result.done();
        }
        Ok(())
    }

    async fn get_message_id(&self, _: &MessageContext, _: &StreamContext<u32, u32, String>, _: Arc<AsyncMutex<()>>, _: &u32) -> String {
        "reply".to_owned()
    }

    async fn end_request(&self, _: MessageContext, _: StreamContext<u32, u32, String>, result: &HandlerResult, _: Arc<AsyncMutex<()>>, data: HandlerData) {
        if self.0.block_end.load(Ordering::SeqCst) {
            self.0.end_entered.add_permits(1);
            self.0.release_end.acquire().await.unwrap().forget();
        }
        if result.is_err() {
            data.set_status(StatusCode::CONFLICT);
        }
        self.0.ended.add_permits(1);
    }
}

fn fixture() -> (Arc<AxumDataSource>, axum::Router, Stream<u32>, Arc<Probe>) {
    let environment = RuntimeEnvironment::default();
    let input_config = InputStreamConfig { stream: StreamConfig::new(1, "input"), endpoint_id: 4 };
    let endpoint_config = HttpEndpointConfig {
        id: 4, name: "request".into(), id_data_connector: 10,
        tracing_enabled: false, http_method_type: HTTPMethodType::POST, path: "/request".into(),
    };
    let connector_config = HttpDataConnectorConfig { id: 10, name: "http".into(), host: "127.0.0.1".into(), port: 9090, address: String::new(), use_dedicated_listener: false };
    environment.publish_runtime_config(Arc::new(RuntimeConfig::from_parts(
        CallSemantics::FunctionCall, [], [input_config.clone().into()], [],
        [connector_config.clone().into()],
        [endpoint_config.clone().into()], [],
    ).unwrap()));
    let source = AxumDataSource::new(environment.clone(), &connector_config);
    let input = InputStream::<u32, u32, String>::new(&input_config, environment.clone());
    let results = Stream::new(&StreamConfig::new(2, "results"), environment);
    input.set_source(&results).unwrap();
    let probe = Arc::new(Probe { entered: Semaphore::new(0), context: Mutex::new(None), callbacks: AtomicUsize::new(0), ended: Semaphore::new(0), block_end: AtomicBool::new(false), end_entered: Semaphore::new(0), release_end: Semaphore::new(0) });
    source.add_endpoint(input, endpoint_config, Handler(probe.clone())).unwrap();
    let router = source.router();
    (source, router, results, probe)
}

#[tokio::test]
async fn aborted_http_request_rejects_late_results_and_releases_its_id() {
    let (_source, router, results, probe) = fixture();
    let request = Request::builder().method("POST").uri("/request").header("x-stream-id", "active").body(Body::empty()).unwrap();
    let first = tokio::spawn(router.clone().oneshot(request));
    tokio::time::timeout(Duration::from_secs(5), probe.entered.acquire()).await.unwrap().unwrap().forget();
    let context = probe.context.lock().unwrap().clone().unwrap();

    first.abort();
    assert!(first.await.unwrap_err().is_cancelled());
    tokio::time::timeout(Duration::from_secs(5), context.cancelled()).await.unwrap();
    // Cancellation is a signal, not synchronous completion of EndRequest.
    tokio::time::timeout(Duration::from_secs(5), probe.ended.acquire()).await.unwrap().unwrap().forget();
    // Keep the endpoint alive: cleanup must not depend on destroying the service.
    tokio::time::timeout(Duration::from_secs(5), results.emit(context, Payload::new(7))).await.unwrap();
    let request = Request::builder().method("POST").uri("/request").header("x-stream-id", "active").header("x-complete", "yes").body(Body::empty()).unwrap();
    let response = tokio::time::timeout(Duration::from_secs(5), router.oneshot(request)).await.unwrap().unwrap();
    assert_eq!(
        (probe.callbacks.load(Ordering::SeqCst), response.status()),
        (0, StatusCode::OK),
        "aborted request retained its result callback or stream ID",
    );
}

#[tokio::test]
async fn disconnected_http_client_rejects_late_results_and_releases_its_id() {
    use tokio::io::AsyncWriteExt;

    let (_source, router, results, probe) = fixture();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
    let mut client = tokio::net::TcpStream::connect(address).await.unwrap();
    client.write_all(b"POST /request HTTP/1.1\r\nHost: localhost\r\nx-stream-id: active\r\nContent-Length: 0\r\nConnection: close\r\n\r\n").await.unwrap();
    tokio::time::timeout(Duration::from_secs(5), probe.entered.acquire()).await.unwrap().unwrap().forget();
    let context = probe.context.lock().unwrap().clone().unwrap();
    drop(client);

    let cancelled = tokio::time::timeout(Duration::from_secs(2), context.cancelled()).await.is_ok();
    let ended = tokio::time::timeout(Duration::from_secs(5), probe.ended.acquire()).await;
    let ended = ended.map(|permit| permit.unwrap().forget()).is_ok();
    let delivery = tokio::time::timeout(Duration::from_secs(5), results.emit(context, Payload::new(7))).await;
    let response = tokio::time::timeout(Duration::from_secs(5), reqwest::Client::new()
        .post(format!("http://{address}/request"))
        .header("x-stream-id", "active")
        .header("x-complete", "yes")
        .send()).await;
    server.abort();
    let _ = server.await;

    delivery.unwrap();
    let status = response.unwrap().unwrap().status();
    assert_eq!(
        (cancelled, ended, probe.callbacks.load(Ordering::SeqCst), status),
        (true, true, 0, StatusCode::OK),
        "client disconnect did not clean up the pending HTTP request",
    );
}

async fn cancelled_request_in_end_request() -> (Arc<AxumDataSource>, Arc<Probe>) {
    let (source, router, _results, probe) = fixture();
    probe.block_end.store(true, Ordering::SeqCst);
    let request = Request::builder().method("POST").uri("/request").header("x-stream-id", "active").body(Body::empty()).unwrap();
    let first = tokio::spawn(router.oneshot(request));
    tokio::time::timeout(Duration::from_secs(5), probe.entered.acquire()).await.unwrap().unwrap().forget();
    first.abort();
    assert!(first.await.unwrap_err().is_cancelled());
    tokio::time::timeout(Duration::from_secs(5), probe.end_entered.acquire()).await.unwrap().unwrap().forget();
    (source, probe)
}

#[tokio::test]
async fn datasource_stop_drains_disconnected_request_handlers() {
    let (source, probe) = cancelled_request_in_end_request().await;
    let mut stop = tokio::spawn(async move {
        source.stop(MessageContext::with_timeout(Duration::from_secs(5))).await
    });
    let early = tokio::time::timeout(Duration::from_millis(50), &mut stop).await;
    let returned_early = early.is_ok();
    probe.release_end.add_permits(1);
    let finished = match early {
        Ok(finished) => finished,
        Err(_) => tokio::time::timeout(Duration::from_secs(5), stop).await.unwrap(),
    };
    finished.unwrap().unwrap();
    assert!(!returned_early, "datasource stop ignored a disconnected request's EndRequest");
}

#[tokio::test]
async fn datasource_stop_deadline_does_not_drop_disconnected_request_handlers() {
    let (source, probe) = cancelled_request_in_end_request().await;
    let stopped = tokio::time::timeout(Duration::from_secs(2), source.stop(
        MessageContext::with_timeout(Duration::from_millis(50)),
    )).await;
    let ended_before_release = probe.ended.try_acquire().is_ok();
    probe.release_end.add_permits(1);
    stopped.expect("datasource ignored its shutdown deadline").unwrap();
    assert!(!ended_before_release, "blocked handler unexpectedly completed");
    tokio::time::timeout(Duration::from_secs(5), probe.ended.acquire()).await.unwrap().unwrap().forget();
}
