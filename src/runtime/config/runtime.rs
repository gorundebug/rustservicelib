use std::{collections::HashMap, sync::Arc};

use super::{
    CallSemantics, Config, LinkConfig, PoolConfig, RuntimeDataConnectorConfig,
    RuntimeEndpointConfig, RuntimeStreamConfig, ServiceConfig,
};
use crate::runtime::environment::{RuntimeError, RuntimeResult};

#[derive(Clone, Default)]
pub struct RuntimeConfig {
    default_call_semantics: CallSemantics,
    services_by_id: HashMap<i32, Arc<ServiceConfig>>,
    services_by_name: HashMap<String, Arc<ServiceConfig>>,
    streams_by_id: HashMap<i32, Arc<RuntimeStreamConfig>>,
    streams_by_name: HashMap<String, Arc<RuntimeStreamConfig>>,
    pools_by_name: HashMap<String, Arc<PoolConfig>>,
    data_connectors_by_id: HashMap<i32, Arc<RuntimeDataConnectorConfig>>,
    data_connectors_by_name: HashMap<String, Arc<RuntimeDataConnectorConfig>>,
    endpoints_by_id: HashMap<i32, Arc<RuntimeEndpointConfig>>,
    endpoints_by_name: HashMap<String, Arc<RuntimeEndpointConfig>>,
    links: HashMap<(i32, i32), Arc<LinkConfig>>,
}

impl RuntimeConfig {
    pub fn with_default_call_semantics(default_call_semantics: CallSemantics) -> Self {
        Self {
            default_call_semantics,
            ..Self::default()
        }
    }

    pub fn new<C: Config>(config: &C) -> RuntimeResult<Self> {
        Self::from_parts(
            config.default_call_semantics(),
            config.services(),
            config.streams(),
            config.pools(),
            config.data_connectors(),
            config.endpoints(),
            config.links(),
        )
    }

    pub fn from_parts(
        default_call_semantics: CallSemantics,
        services: impl IntoIterator<Item = ServiceConfig>,
        streams: impl IntoIterator<Item = RuntimeStreamConfig>,
        pools: impl IntoIterator<Item = PoolConfig>,
        data_connectors: impl IntoIterator<Item = RuntimeDataConnectorConfig>,
        endpoints: impl IntoIterator<Item = RuntimeEndpointConfig>,
        links: impl IntoIterator<Item = LinkConfig>,
    ) -> RuntimeResult<Self> {
        let mut runtime = Self::with_default_call_semantics(default_call_semantics);
        for service in services {
            if runtime.services_by_id.contains_key(&service.id) {
                return duplicate("service id", service.id);
            }
            if runtime.services_by_name.contains_key(&service.name) {
                return duplicate_name("service", &service.name);
            }
            let service = Arc::new(service);
            runtime
                .services_by_id
                .insert(service.id, Arc::clone(&service));
            runtime
                .services_by_name
                .insert(service.name.clone(), service);
        }
        for stream in streams {
            let id = stream.stream().id;
            let name = stream.stream().name.clone();
            if runtime.streams_by_id.contains_key(&id) {
                return duplicate("stream id", id);
            }
            if runtime.streams_by_name.contains_key(&name) {
                return duplicate_name("stream", &name);
            }
            let stream = Arc::new(stream);
            runtime.streams_by_id.insert(id, Arc::clone(&stream));
            runtime.streams_by_name.insert(name, stream);
        }
        for pool in pools {
            if pool.executors_count == 0 {
                return Err(RuntimeError::InvalidConfiguration(format!(
                    "pool {:?} must have at least one executor",
                    pool.name
                )));
            }
            if runtime.pools_by_name.contains_key(&pool.name) {
                return duplicate_name("pool", &pool.name);
            }
            runtime
                .pools_by_name
                .insert(pool.name.clone(), Arc::new(pool));
        }
        for connector in data_connectors {
            let id = connector.id();
            let name = connector.name().to_owned();
            if runtime.data_connectors_by_id.contains_key(&id) {
                return duplicate("data connector id", id);
            }
            if runtime.data_connectors_by_name.contains_key(&name) {
                return duplicate_name("data connector", &name);
            }
            let connector = Arc::new(connector);
            runtime
                .data_connectors_by_id
                .insert(id, Arc::clone(&connector));
            runtime.data_connectors_by_name.insert(name, connector);
        }
        for endpoint in endpoints {
            let id = endpoint.id();
            let name = endpoint.name().to_owned();
            if runtime.endpoints_by_id.contains_key(&id) {
                return duplicate("endpoint id", id);
            }
            if runtime.endpoints_by_name.contains_key(&name) {
                return duplicate_name("endpoint", &name);
            }
            if !runtime
                .data_connectors_by_id
                .contains_key(&endpoint.data_connector_id())
            {
                return Err(RuntimeError::InvalidConfiguration(format!(
                    "endpoint {name:?} references unknown data connector {}",
                    endpoint.data_connector_id()
                )));
            }
            let endpoint = Arc::new(endpoint);
            runtime.endpoints_by_id.insert(id, Arc::clone(&endpoint));
            runtime.endpoints_by_name.insert(name, endpoint);
        }
        for link in links {
            let id = (link.from, link.to);
            if runtime.links.contains_key(&id) {
                return Err(RuntimeError::InvalidConfiguration(format!(
                    "duplicate link from={} to={}",
                    link.from, link.to
                )));
            }
            runtime.links.insert(id, Arc::new(link));
        }
        let mut referenced_pools = Vec::new();
        referenced_pools.push(runtime.default_call_semantics.clone());
        referenced_pools.extend(
            runtime
                .services_by_id
                .values()
                .map(|service| service.default_call_semantics.clone()),
        );
        referenced_pools.extend(
            runtime
                .links
                .values()
                .map(|link| link.call_semantics.clone()),
        );
        for semantics in referenced_pools {
            let pool_name = match semantics {
                CallSemantics::TaskPool { pool_name }
                | CallSemantics::PriorityTaskPool { pool_name, .. } => pool_name,
                _ => continue,
            };
            runtime
                .pools_by_name
                .entry(pool_name.clone())
                .or_insert_with(|| {
                    Arc::new(PoolConfig {
                        name: pool_name,
                        executors_count: 1,
                        queue_capacity: 0,
                    })
                });
        }
        Ok(runtime)
    }

    pub fn service_by_id(&self, id: i32) -> Option<Arc<ServiceConfig>> {
        self.services_by_id.get(&id).cloned()
    }

    pub fn service_by_name(&self, name: &str) -> Option<Arc<ServiceConfig>> {
        self.services_by_name.get(name).cloned()
    }

    pub fn services(&self) -> Vec<Arc<ServiceConfig>> {
        self.services_by_id.values().cloned().collect()
    }

    pub fn stream_by_id(&self, id: i32) -> Option<Arc<RuntimeStreamConfig>> {
        self.streams_by_id.get(&id).cloned()
    }

    pub fn stream_by_name(&self, name: &str) -> Option<Arc<RuntimeStreamConfig>> {
        self.streams_by_name.get(name).cloned()
    }

    pub fn streams(&self) -> Vec<Arc<RuntimeStreamConfig>> {
        self.streams_by_id.values().cloned().collect()
    }

    pub fn pool_by_name(&self, name: &str) -> Option<Arc<PoolConfig>> {
        self.pools_by_name.get(name).cloned()
    }

    pub fn pools(&self) -> Vec<Arc<PoolConfig>> {
        self.pools_by_name.values().cloned().collect()
    }

    pub fn data_connector_by_id(&self, id: i32) -> Option<Arc<RuntimeDataConnectorConfig>> {
        self.data_connectors_by_id.get(&id).cloned()
    }

    pub fn endpoint_by_id(&self, id: i32) -> Option<Arc<RuntimeEndpointConfig>> {
        self.endpoints_by_id.get(&id).cloned()
    }

    pub fn data_connectors(&self) -> Vec<Arc<RuntimeDataConnectorConfig>> {
        self.data_connectors_by_id.values().cloned().collect()
    }

    pub fn endpoints(&self) -> Vec<Arc<RuntimeEndpointConfig>> {
        self.endpoints_by_id.values().cloned().collect()
    }

    pub fn link(&self, from: i32, to: i32) -> Option<Arc<LinkConfig>> {
        self.links.get(&(from, to)).cloned()
    }

    pub fn links(&self) -> Vec<Arc<LinkConfig>> {
        self.links.values().cloned().collect()
    }

    pub fn default_call_semantics(&self) -> &CallSemantics {
        &self.default_call_semantics
    }
}

fn duplicate<T>(kind: &str, id: i32) -> RuntimeResult<T> {
    Err(RuntimeError::InvalidConfiguration(format!(
        "duplicate {kind}: {id}"
    )))
}

fn duplicate_name<T>(kind: &str, name: &str) -> RuntimeResult<T> {
    Err(RuntimeError::InvalidConfiguration(format!(
        "duplicate {kind} name: {name}"
    )))
}
