//! Regression probe for retaining service resources across interrupted shutdown.
//! cargo run --example service_shutdown_ownership_probe
use std::{sync::{Arc, atomic::{AtomicUsize, Ordering}}, time::Duration};
use async_trait::async_trait;
use tokio::sync::Notify;
use servicelib::{MessageContext, runtime::{config::{CallSemantics, RuntimeConfig, ServiceConfig},
    datasink::DataSink, environment::{Lifecycle, RuntimeEnvironment, RuntimeResult}, serviceapp::ServiceApp}};

#[derive(Default)]
struct HeldComponent { entered: Notify, release: Notify, finished: Notify, calls: AtomicUsize }
#[async_trait]
impl Lifecycle for HeldComponent {
    async fn start(&self, _: MessageContext) -> RuntimeResult<()> { Ok(()) }
    async fn stop(&self, _: MessageContext) -> RuntimeResult<()> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.entered.notify_one();
        self.release.notified().await;
        self.finished.notify_one();
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
    fn name(&self) -> &str { "dependent-sink" }
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = ServiceConfig { id: 1, name: "shutdown-probe".into(),
        http_host: "127.0.0.1".into(), http_port: 0, grpc_host: "127.0.0.1".into(), grpc_port: 0,
        shutdown_timeout: 30, ..ServiceConfig::default() };
    let environment = RuntimeEnvironment::default();
    environment.publish_runtime_config(Arc::new(RuntimeConfig::from_parts(
        CallSemantics::FunctionCall, [config.clone()], [], [], [], [], [],
    )?));
    let component = Arc::new(HeldComponent::default());
    let sink = Arc::new(Sink(AtomicUsize::new(0)));
    let mut app = ServiceApp::new(environment, config)?;
    app.add_component(Arc::clone(&component))?;
    app.register_data_sink(Arc::clone(&sink))?;
    app.start(MessageContext::new()).await?;
    let first = app.stop(MessageContext::new()).await;
    tokio::time::timeout(Duration::from_secs(1), component.entered.notified()).await?;
    let second = app.stop(MessageContext::new()).await;
    // Let any incorrectly detached cleanup phases become observable.
    tokio::time::sleep(Duration::from_millis(10)).await;
    let premature_sink_stops = sink.0.load(Ordering::SeqCst);
    println!("before release: first={first:?}, retry={second:?}, component_calls={}, sink_stops={premature_sink_stops}",
        component.calls.load(Ordering::SeqCst));
    component.release.notify_one();
    tokio::time::timeout(Duration::from_secs(1), component.finished.notified()).await?;
    let final_stop = app.stop(MessageContext::new()).await;
    assert!(first.is_err(), "unfinished service shutdown was reported as success");
    assert!(second.is_err(), "retry falsely reported a stopped service while component was held");
    assert_eq!(premature_sink_stops, 0, "dependent sink stopped before component drained");
    final_stop?;
    assert_eq!(component.calls.load(Ordering::SeqCst), 1, "physical stop was duplicated");
    assert_eq!(sink.0.load(Ordering::SeqCst), 1);
    println!("PASS: service retains physical drain and dependency order across deadlines");
    Ok(())
}
