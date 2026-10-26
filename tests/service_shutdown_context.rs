use std::{collections::HashMap, sync::{Arc, Mutex, atomic::{AtomicUsize, Ordering}}, time::Duration};
use async_trait::async_trait;
use tokio::sync::Notify;
use servicelib::{MessageContext, runtime::{config::{CallSemantics, RuntimeConfig, ServiceConfig},
    datasink::DataSink, environment::{Lifecycle, RuntimeEnvironment, RuntimeError, RuntimeResult},
    serviceapp::ServiceApp}};

#[derive(Default)]
struct Held {
    entered: Notify,
    release: Notify,
    calls: AtomicUsize,
    context: Mutex<Option<MessageContext>>,
}
#[async_trait]
impl Lifecycle for Held {
    async fn start(&self, _: MessageContext) -> RuntimeResult<()> { Ok(()) }
    async fn stop(&self, context: MessageContext) -> RuntimeResult<()> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        *self.context.lock().unwrap() = Some(context);
        self.entered.notify_one();
        self.release.notified().await;
        Ok(())
    }
}
struct Sink(AtomicUsize);
#[async_trait]
impl Lifecycle for Sink {
    async fn start(&self, _: MessageContext) -> RuntimeResult<()> { Ok(()) }
    async fn stop(&self, _: MessageContext) -> RuntimeResult<()> {
        self.0.fetch_add(1, Ordering::SeqCst); Ok(())
    }
}
impl DataSink for Sink {
    fn id(&self) -> i32 { 1 }
    fn name(&self) -> &str { "sink" }
}

fn app(timeout: i32, http_port: u16) -> (ServiceApp, Arc<Sink>) {
    let config = ServiceConfig { id: 1, name: "context-shutdown".into(),
        http_host: "127.0.0.1".into(), http_port: http_port.into(),
        grpc_host: "127.0.0.1".into(), grpc_port: 0,
        shutdown_timeout: timeout.into(), ..ServiceConfig::default() };
    let environment = RuntimeEnvironment::default();
    environment.publish_runtime_config(Arc::new(RuntimeConfig::from_parts(
        CallSemantics::FunctionCall, [config.clone()], [], [], [], [], [],
    ).unwrap()));
    let mut app = ServiceApp::new(environment, config).unwrap();
    let sink = Arc::new(Sink(AtomicUsize::new(0)));
    app.register_data_sink(Arc::clone(&sink)).unwrap();
    (app, sink)
}

async fn finish(app: &ServiceApp) {
    tokio::time::timeout(Duration::from_secs(2), app.stop(MessageContext::new()))
        .await.unwrap().unwrap();
}

#[tokio::test]
async fn cancelled_owner_retains_job_and_its_context() {
    let (mut app, sink) = app(1000, 0);
    let held = Arc::new(Held::default());
    app.add_component(Arc::clone(&held)).unwrap();
    app.start(MessageContext::new()).await.unwrap();
    let app = Arc::new(app);
    let owner = MessageContext::new().with_metadata(HashMap::from([("owner".into(), "stop-caller".into())]));
    let waiter_app = Arc::clone(&app);
    let context = owner.clone();
    let waiter = tokio::spawn(async move { waiter_app.stop(context).await });
    held.entered.notified().await;
    owner.cancel();
    assert!(matches!(waiter.await.unwrap(), Err(RuntimeError::ContextCancelled)));
    assert!(held.context.lock().unwrap().as_ref().unwrap().is_cancelled());
    assert_eq!(held.context.lock().unwrap().as_ref().unwrap().metadata().get("owner").unwrap(), "stop-caller");
    assert_eq!(sink.0.load(Ordering::SeqCst), 0);
    held.release.notify_one();
    finish(&app).await;
    assert_eq!(held.calls.load(Ordering::SeqCst), 1);
    assert_eq!(sink.0.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn dropped_waiter_does_not_drop_physical_cleanup() {
    let (mut app, sink) = app(1000, 0);
    let held = Arc::new(Held::default());
    app.add_component(Arc::clone(&held)).unwrap();
    app.start(MessageContext::new()).await.unwrap();
    let app = Arc::new(app);
    let waiter_app = Arc::clone(&app);
    let waiter = tokio::spawn(async move { waiter_app.stop(MessageContext::new()).await });
    held.entered.notified().await;
    waiter.abort();
    assert!(waiter.await.unwrap_err().is_cancelled());
    let cancelled = MessageContext::new(); cancelled.cancel();
    assert!(matches!(app.stop(cancelled).await, Err(RuntimeError::ContextCancelled)));
    assert_eq!(sink.0.load(Ordering::SeqCst), 0);
    held.release.notify_one();
    finish(&app).await;
    assert_eq!(held.calls.load(Ordering::SeqCst), 1);
    assert_eq!(sink.0.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn concurrent_waiter_has_its_own_deadline() {
    let (mut app, sink) = app(1000, 0);
    let held = Arc::new(Held::default());
    app.add_component(Arc::clone(&held)).unwrap();
    app.start(MessageContext::new()).await.unwrap();
    let app = Arc::new(app);
    let waiter_app = Arc::clone(&app);
    let waiter = tokio::spawn(async move { waiter_app.stop(MessageContext::new()).await });
    held.entered.notified().await;
    assert!(matches!(app.stop(MessageContext::with_timeout(Duration::from_millis(10))).await,
        Err(RuntimeError::ContextCancelled)));
    assert_eq!(held.calls.load(Ordering::SeqCst), 1);
    assert_eq!(sink.0.load(Ordering::SeqCst), 0);
    held.release.notify_one();
    waiter.await.unwrap().unwrap();
    finish(&app).await;
    assert_eq!(sink.0.load(Ordering::SeqCst), 1);
}

struct Cooperative(AtomicUsize);
#[async_trait]
impl Lifecycle for Cooperative {
    async fn start(&self, _: MessageContext) -> RuntimeResult<()> { Ok(()) }
    async fn stop(&self, context: MessageContext) -> RuntimeResult<()> {
        if self.0.fetch_add(1, Ordering::SeqCst) == 0 {
            context.cancelled().await;
            Err(RuntimeError::ContextCancelled)
        } else { assert!(!context.is_cancelled()); Ok(()) }
    }
}

#[tokio::test]
async fn cooperative_incomplete_stop_retries_with_fresh_context() {
    let (mut app, sink) = app(1000, 0);
    let resource = Arc::new(Cooperative(AtomicUsize::new(0)));
    app.add_component(Arc::clone(&resource)).unwrap();
    app.start(MessageContext::new()).await.unwrap();
    assert!(matches!(app.stop(MessageContext::with_timeout(Duration::from_millis(10))).await,
        Err(RuntimeError::ContextCancelled)));
    // The first retained operation may not have observed cancellation yet. A
    // retry must first observe its result before replacing it with new work.
    for _ in 0..2 {
        match app.stop(MessageContext::new()).await {
            Ok(()) => break,
            Err(RuntimeError::ContextCancelled) => {},
            result => panic!("unexpected result: {result:?}"),
        }
    }
    finish(&app).await;
    assert_eq!(resource.0.load(Ordering::SeqCst), 2);
    assert_eq!(sink.0.load(Ordering::SeqCst), 1);
}

struct Failure(AtomicUsize);
#[async_trait]
impl Lifecycle for Failure {
    async fn start(&self, _: MessageContext) -> RuntimeResult<()> { Ok(()) }
    async fn stop(&self, _: MessageContext) -> RuntimeResult<()> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Err(RuntimeError::Transport("driver close failed".into()))
    }
}

#[tokio::test]
async fn physical_failure_stays_visible_without_tearing_down_dependencies() {
    let (mut app, sink) = app(1000, 0);
    let resource = Arc::new(Failure(AtomicUsize::new(0)));
    app.add_component(Arc::clone(&resource)).unwrap();
    app.start(MessageContext::new()).await.unwrap();
    for _ in 0..2 {
        assert!(app.stop(MessageContext::new()).await.unwrap_err().to_string().contains("driver close failed"));
    }
    assert_eq!(resource.0.load(Ordering::SeqCst), 1);
    assert_eq!(sink.0.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn real_http_handler_keeps_server_handle_and_sink_until_response_finishes() {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    drop(listener);
    let (mut app, sink) = app(1000, port);
    let entered = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let handler_entered = Arc::clone(&entered);
    let handler_release = Arc::clone(&release);
    app.add_http_router(axum::Router::new().route("/held", axum::routing::get(move || {
        let entered = Arc::clone(&handler_entered);
        let release = Arc::clone(&handler_release);
        async move { entered.notify_one(); release.notified().await; "finished" }
    }))).unwrap();
    app.start(MessageContext::new()).await.unwrap();
    let request = tokio::spawn(async move {
        reqwest::get(format!("http://127.0.0.1:{port}/held")).await.unwrap().text().await.unwrap()
    });
    tokio::time::timeout(Duration::from_secs(2), entered.notified()).await.unwrap();
    assert!(matches!(app.stop(MessageContext::with_timeout(Duration::from_millis(10))).await,
        Err(RuntimeError::ContextCancelled)));
    assert_eq!(sink.0.load(Ordering::SeqCst), 0);
    assert!(!request.is_finished());
    release.notify_one();
    assert_eq!(request.await.unwrap(), "finished");
    finish(&app).await;
    assert_eq!(sink.0.load(Ordering::SeqCst), 1);
}
