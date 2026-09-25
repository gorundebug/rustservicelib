//! Diagnostic only: distinguish the service stop deadline from runtime teardown.
//! This reports observed timings; it does not define delayed process exit as valid.

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

struct BlockingStop {
    started: Arc<AtomicBool>,
    completed: Arc<AtomicBool>,
}

#[async_trait]
impl Lifecycle for BlockingStop {
    async fn start(&self, _context: MessageContext) -> RuntimeResult<()> {
        Ok(())
    }

    async fn stop(&self, _context: MessageContext) -> RuntimeResult<()> {
        let started = self.started.clone();
        let completed = self.completed.clone();
        tokio::task::spawn_blocking(move || {
            started.store(true, Ordering::Release);
            std::thread::sleep(Duration::from_millis(300));
            completed.store(true, Ordering::Release);
        })
        .await
        .expect("blocking shutdown worker panicked");
        Ok(())
    }
}

fn main() {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();
    let started = Arc::new(AtomicBool::new(false));
    let completed = Arc::new(AtomicBool::new(false));

    runtime.block_on(async {
        let service = ServiceConfig {
            id: 1,
            name: "Shutdown Lifetime Probe".to_owned(),
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
        let mut app = ServiceApp::new(environment, service).unwrap();
        app.add_component(Arc::new(BlockingStop {
            started: started.clone(),
            completed: completed.clone(),
        }))
        .unwrap();
        app.start(MessageContext::new()).await.unwrap();
        let stop_started = Instant::now();
        app.stop(MessageContext::new()).await.unwrap();
        println!(
            "stop_ms={} worker_started={} worker_completed={}",
            stop_started.elapsed().as_millis(),
            started.load(Ordering::Acquire),
            completed.load(Ordering::Acquire),
        );
    });

    // The generated #[tokio::main] also drops its runtime after run() returns.
    let drop_started = Instant::now();
    drop(runtime);
    println!(
        "runtime_drop_ms={} worker_completed={}",
        drop_started.elapsed().as_millis(),
        completed.load(Ordering::Acquire),
    );
}
