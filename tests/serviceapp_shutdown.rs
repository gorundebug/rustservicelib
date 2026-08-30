use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant},
};

use async_trait::async_trait;
use servicelib::{
    MessageContext,
    runtime::{
        config::{CallSemantics, RuntimeConfig, ServiceConfig},
        environment::{Lifecycle, RuntimeEnvironment, RuntimeResult},
        serviceapp::ServiceApp,
    },
};

struct SlowComponent {
    stopped: Arc<AtomicBool>,
}

#[async_trait]
impl Lifecycle for SlowComponent {
    async fn start(&self, _context: MessageContext) -> RuntimeResult<()> {
        Ok(())
    }

    async fn stop(&self, _context: MessageContext) -> RuntimeResult<()> {
        tokio::time::sleep(Duration::from_millis(100)).await;
        self.stopped.store(true, Ordering::Release);
        Ok(())
    }
}

#[tokio::test]
async fn shutdown_timeout_is_one_upper_bound_for_the_complete_service() {
    let service = ServiceConfig {
        id: 1,
        name: "Shutdown Test".to_owned(),
        http_host: "127.0.0.1".to_owned(),
        http_port: 0,
        metrics_handler: String::new(),
        status_handler: String::new(),
        startup_handler: String::new(),
        readiness_handler: String::new(),
        liveness_handler: String::new(),
        shutdown_timeout: 20,
        ..ServiceConfig::default()
    };
    let environment = RuntimeEnvironment::default();
    environment.publish_runtime_config(Arc::new(
        RuntimeConfig::from_parts(
            CallSemantics::FunctionCall,
            [service.clone()],
            [],
            [],
            [],
            [],
            [],
        )
        .unwrap(),
    ));
    let stopped = Arc::new(AtomicBool::new(false));
    let mut app = ServiceApp::new(environment, service).unwrap();
    app.add_component(Arc::new(SlowComponent {
        stopped: Arc::clone(&stopped),
    }))
    .unwrap();
    app.start(MessageContext::new()).await.unwrap();

    let started = Instant::now();
    app.stop(MessageContext::new()).await.unwrap();

    assert!(
        started.elapsed() < Duration::from_millis(70),
        "shutdown restarted the timeout between lifecycle phases"
    );
    assert!(!stopped.load(Ordering::Acquire));
    tokio::time::sleep(Duration::from_millis(110)).await;
    assert!(stopped.load(Ordering::Acquire));
}
