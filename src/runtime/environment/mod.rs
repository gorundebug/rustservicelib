use std::{
    collections::{HashMap, HashSet},
    sync::{
        Arc, RwLock,
        atomic::{AtomicI64, Ordering},
    },
};

use arc_swap::ArcSwap;
pub mod log;
pub mod metrics;
pub mod tracing;

use async_trait::async_trait;
use thiserror::Error;

use self::{
    log::{LogsEngine, StdoutLogsEngine},
    metrics::{Metrics, MetricsEngine, PrometheusMetricsEngine},
    tracing::{StdoutTracingEngine, TracingEngine},
};

/// Plain call counter for the live status-page graph view, deliberately kept
/// independent of `metrics::Int64Counter` (which is optionally backed by an
/// OTel instrument). The status page needs a value it can always read back
/// synchronously; an OTel-backed counter may not maintain one. Mirrors Go's
/// `consumeStatistics` (runtime/runtime.go), which is likewise a bare
/// `atomic.Int64` kept separate from the OTel `messagesCounter`.
#[derive(Clone, Default)]
pub struct CallStatistics(Arc<AtomicI64>);

impl CallStatistics {
    pub fn inc(&self) {
        self.0.fetch_add(1, Ordering::Relaxed);
    }

    pub fn get(&self) -> i64 {
        self.0.load(Ordering::Relaxed)
    }
}

#[derive(Clone)]
pub struct GraphLink {
    pub from: i32,
    pub to: i32,
    pub call_semantics: CallSemantics,
    pub type_name: String,
    pub calls: CallStatistics,
}
use crate::runtime::{
    common::{MessageContext, RuntimeEndpointConsumer},
    config::{CallSemantics, RuntimeConfig, RuntimeStreamConfig, ServiceConfig},
    pool::{DelayPool, PriorityTaskPool, TaskPool},
    store::Storage,
};

#[derive(Debug, Error)]
pub enum RuntimeError {
    #[error("task pool {0:?} is not registered")]
    TaskPoolNotFound(String),
    #[error("priority task pool {0:?} is not registered")]
    PriorityTaskPoolNotFound(String),
    #[error("stream {stream:?} already has a downstream consumer")]
    ConsumerAlreadySet { stream: String },
    #[error("stream {stream:?} already has a source")]
    SourceAlreadySet { stream: String },
    #[error("stream {stream:?} has no downstream consumer")]
    ConsumerNotSet { stream: String },
    #[error("runtime resource {0:?} is already registered")]
    DuplicateResource(String),
    #[error("runtime resource {0:?} is already started")]
    ResourceAlreadyStarted(String),
    #[error("runtime resource {0:?} is stopped")]
    ResourceStopped(String),
    #[error("message context is cancelled")]
    ContextCancelled,
    #[error("duplicate key")]
    DuplicateKey,
    #[error("invalid runtime configuration: {0}")]
    InvalidConfiguration(String),
    #[error("transport error: {0}")]
    Transport(String),
    #[error(transparent)]
    Metrics(#[from] metrics::MetricsError),
}

pub type RuntimeResult<T> = Result<T, RuntimeError>;

#[derive(Clone)]
pub struct RuntimeEnvironment {
    service_id: Option<i32>,
    runtime_config: Arc<ArcSwap<RuntimeConfig>>,
    graph_links: Arc<RwLock<Vec<GraphLink>>>,
    runtime_streams: Arc<RwLock<HashSet<i32>>>,
    endpoint_consumers: Arc<RwLock<HashMap<i32, Arc<dyn RuntimeEndpointConsumer>>>>,
    task_pools: Arc<RwLock<HashMap<String, Arc<TaskPool>>>>,
    priority_task_pools: Arc<RwLock<HashMap<String, Arc<PriorityTaskPool>>>>,
    storages: Arc<RwLock<Vec<Arc<dyn Storage>>>>,
    metrics: Metrics,
    metrics_engine: Arc<dyn MetricsEngine>,
    tracing_engine: Arc<dyn TracingEngine>,
    logs_engine: Arc<dyn LogsEngine>,
    delay_pool: Arc<DelayPool>,
}

impl Default for RuntimeEnvironment {
    fn default() -> Self {
        let metrics_engine = Arc::new(PrometheusMetricsEngine::new());
        Self {
            service_id: None,
            runtime_config: Arc::new(ArcSwap::from_pointee(RuntimeConfig::default())),
            graph_links: Arc::new(RwLock::new(Vec::new())),
            runtime_streams: Arc::new(RwLock::new(HashSet::new())),
            endpoint_consumers: Arc::new(RwLock::new(HashMap::new())),
            task_pools: Arc::new(RwLock::new(HashMap::new())),
            priority_task_pools: Arc::new(RwLock::new(HashMap::new())),
            storages: Arc::new(RwLock::new(Vec::new())),
            metrics: metrics_engine.metrics().clone(),
            metrics_engine,
            tracing_engine: Arc::new(StdoutTracingEngine),
            logs_engine: Arc::new(StdoutLogsEngine),
            delay_pool: Arc::new(DelayPool::default()),
        }
    }
}

impl RuntimeEnvironment {
    pub fn new(default_call_semantics: CallSemantics) -> Self {
        Self::with_metrics(default_call_semantics, Metrics::default())
    }

    pub fn with_metrics(default_call_semantics: CallSemantics, metrics: Metrics) -> Self {
        let metrics_engine = Arc::new(ProvidedMetricsEngine {
            metrics: metrics.clone(),
        });
        Self {
            metrics,
            metrics_engine,
            runtime_config: Arc::new(ArcSwap::from_pointee(
                RuntimeConfig::with_default_call_semantics(default_call_semantics),
            )),
            ..Self::default()
        }
    }

    pub fn with_telemetry(
        default_call_semantics: CallSemantics,
        metrics_engine: Arc<dyn MetricsEngine>,
        tracing_engine: Arc<dyn TracingEngine>,
        logs_engine: Arc<dyn LogsEngine>,
    ) -> Self {
        Self {
            metrics: metrics_engine.metrics().clone(),
            metrics_engine,
            tracing_engine,
            logs_engine,
            runtime_config: Arc::new(ArcSwap::from_pointee(
                RuntimeConfig::with_default_call_semantics(default_call_semantics),
            )),
            ..Self::default()
        }
    }

    pub fn for_service(&self, service_id: i32) -> Self {
        let mut environment = self.clone();
        environment.service_id = Some(service_id);
        if let Err(error) = environment.delay_pool.configure_metrics(&environment) {
            ::tracing::error!(error = %error, "failed to configure delay pool metrics");
        }
        environment
    }

    pub fn service_name(&self) -> String {
        let config = self.runtime_config();
        self.service_id
            .and_then(|id| config.service_by_id(id))
            .or_else(|| {
                let services = config.services();
                (services.len() == 1).then(|| services[0].clone())
            })
            .map(|service| service.name.clone())
            .unwrap_or_default()
    }

    pub fn service_id(&self) -> Option<i32> {
        self.service_id
    }

    pub fn service_config(&self, id: i32) -> Option<Arc<ServiceConfig>> {
        self.runtime_config().service_by_id(id)
    }

    pub fn publish_runtime_config(&self, config: Arc<RuntimeConfig>) {
        self.runtime_config.store(config);
        for pool in self.task_pools() {
            pool.reload_config();
        }
        for pool in self.priority_task_pools() {
            pool.reload_config();
        }
    }

    pub fn runtime_config(&self) -> Arc<RuntimeConfig> {
        self.runtime_config.load_full()
    }

    pub fn stream_config(&self, id: i32) -> Option<Arc<RuntimeStreamConfig>> {
        self.runtime_config().stream_by_id(id)
    }

    pub fn stream_name(&self, id: i32) -> String {
        self.stream_config(id)
            .map(|config| config.stream().name.clone())
            .unwrap_or_else(|| id.to_string())
    }

    pub fn streams(&self) -> Vec<(i32, String)> {
        let mut streams = self
            .runtime_config()
            .streams()
            .into_iter()
            .map(|config| {
                let stream = config.stream();
                (stream.id, stream.name.clone())
            })
            .collect::<Vec<_>>();
        streams.sort_unstable_by_key(|(id, _)| *id);
        streams
    }

    pub fn register_runtime_stream(&self, id: i32) {
        self.runtime_streams
            .write()
            .expect("runtime streams lock poisoned")
            .insert(id);
    }

    pub fn runtime_stream_ids(&self) -> HashSet<i32> {
        self.runtime_streams
            .read()
            .expect("runtime streams lock poisoned")
            .clone()
    }

    pub fn register_endpoint_consumer(
        &self,
        consumer: Arc<dyn RuntimeEndpointConsumer>,
    ) -> RuntimeResult<()> {
        let id = consumer.id();
        if self
            .endpoint_consumers
            .write()
            .expect("endpoint consumers lock poisoned")
            .insert(id, consumer)
            .is_some()
        {
            return Err(RuntimeError::DuplicateResource(format!(
                "endpoint consumer {id}"
            )));
        }
        Ok(())
    }

    pub fn endpoint_consumer(&self, id: i32) -> Option<Arc<dyn RuntimeEndpointConsumer>> {
        self.endpoint_consumers
            .read()
            .expect("endpoint consumers lock poisoned")
            .get(&id)
            .cloned()
    }

    pub fn endpoint_consumers(&self) -> Vec<Arc<dyn RuntimeEndpointConsumer>> {
        self.endpoint_consumers
            .read()
            .expect("endpoint consumers lock poisoned")
            .values()
            .cloned()
            .collect()
    }

    pub(crate) fn clear_endpoint_consumers(&self) {
        self.endpoint_consumers
            .write()
            .expect("endpoint consumers lock poisoned")
            .clear();
    }

    pub fn register_graph_link(
        &self,
        from: i32,
        to: i32,
        call_semantics: CallSemantics,
        type_name: String,
        calls: CallStatistics,
    ) {
        let mut links = self.graph_links.write().expect("graph links lock poisoned");
        if let Some(link) = links
            .iter_mut()
            .find(|link| link.from == from && link.to == to)
        {
            *link = GraphLink {
                from,
                to,
                call_semantics,
                type_name,
                calls,
            };
        } else {
            links.push(GraphLink {
                from,
                to,
                call_semantics,
                type_name,
                calls,
            });
        }
    }

    pub fn graph_links(&self) -> Vec<GraphLink> {
        self.graph_links
            .read()
            .expect("graph links lock poisoned")
            .clone()
    }

    pub fn call_semantics(&self, from: i32, to: i32) -> CallSemantics {
        if let Some(link) = self.runtime_config().link(from, to) {
            return link.call_semantics.clone();
        }
        self.service_id
            .and_then(|id| self.runtime_config().service_by_id(id))
            .map(|service| service.default_call_semantics.clone())
            .unwrap_or_else(|| self.runtime_config().default_call_semantics().clone())
    }

    pub fn register_task_pool(&self, pool: Arc<TaskPool>) -> RuntimeResult<()> {
        pool.configure_metrics(self)?;
        let name = pool.name().to_owned();
        let replaced = self
            .task_pools
            .write()
            .expect("task pools lock poisoned")
            .insert(name.clone(), pool);
        if replaced.is_some() {
            return Err(RuntimeError::DuplicateResource(name));
        }
        Ok(())
    }

    pub fn register_priority_task_pool(&self, pool: Arc<PriorityTaskPool>) -> RuntimeResult<()> {
        pool.configure_metrics(self)?;
        let name = pool.name().to_owned();
        let replaced = self
            .priority_task_pools
            .write()
            .expect("priority task pools lock poisoned")
            .insert(name.clone(), pool);
        if replaced.is_some() {
            return Err(RuntimeError::DuplicateResource(name));
        }
        Ok(())
    }

    pub fn task_pool(&self, name: &str) -> RuntimeResult<Arc<TaskPool>> {
        self.task_pools
            .read()
            .expect("task pools lock poisoned")
            .get(name)
            .cloned()
            .ok_or_else(|| RuntimeError::TaskPoolNotFound(name.to_owned()))
    }

    pub fn priority_task_pool(&self, name: &str) -> RuntimeResult<Arc<PriorityTaskPool>> {
        self.priority_task_pools
            .read()
            .expect("priority task pools lock poisoned")
            .get(name)
            .cloned()
            .ok_or_else(|| RuntimeError::PriorityTaskPoolNotFound(name.to_owned()))
    }

    pub fn delay_pool(&self) -> &Arc<DelayPool> {
        &self.delay_pool
    }

    pub fn task_pools(&self) -> Vec<Arc<TaskPool>> {
        self.task_pools
            .read()
            .expect("task pools lock poisoned")
            .values()
            .cloned()
            .collect()
    }

    pub fn priority_task_pools(&self) -> Vec<Arc<PriorityTaskPool>> {
        self.priority_task_pools
            .read()
            .expect("priority task pools lock poisoned")
            .values()
            .cloned()
            .collect()
    }

    pub fn register_storage(&self, storage: Arc<dyn Storage>) {
        self.storages
            .write()
            .expect("storages lock poisoned")
            .push(storage);
    }

    pub fn storages(&self) -> Vec<Arc<dyn Storage>> {
        self.storages
            .read()
            .expect("storages lock poisoned")
            .clone()
    }

    pub fn metrics(&self) -> &Metrics {
        &self.metrics
    }

    pub fn metrics_engine(&self) -> &Arc<dyn MetricsEngine> {
        &self.metrics_engine
    }

    pub fn tracing_engine(&self) -> &Arc<dyn TracingEngine> {
        &self.tracing_engine
    }

    pub fn logs_engine(&self) -> &Arc<dyn LogsEngine> {
        &self.logs_engine
    }
}

struct ProvidedMetricsEngine {
    metrics: Metrics,
}

#[async_trait]
impl MetricsEngine for ProvidedMetricsEngine {
    fn metrics(&self) -> &Metrics {
        &self.metrics
    }

    async fn shutdown(&self) -> RuntimeResult<()> {
        Ok(())
    }
}

#[async_trait]
pub trait Lifecycle: Send + Sync {
    async fn start(&self, context: MessageContext) -> RuntimeResult<()>;
    async fn stop(&self, context: MessageContext) -> RuntimeResult<()>;
}
