use std::{collections::HashMap, sync::Arc};

use super::{
    CallSemantics, Config, LinkConfig, ModuleConfig, PoolConfig, RuntimeDataConnectorConfig,
    RuntimeEndpointConfig, RuntimeStreamConfig, ServiceConfig, TypeConfig,
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
    modules_by_name: HashMap<String, Arc<ModuleConfig>>,
    types_by_name: HashMap<String, Arc<TypeConfig>>,
}

impl RuntimeConfig {
    pub fn with_default_call_semantics(default_call_semantics: CallSemantics) -> Self {
        Self {
            default_call_semantics,
            ..Self::default()
        }
    }

    pub fn new<C: Config>(config: &C) -> RuntimeResult<Self> {
        let mut runtime = Self::from_parts(
            config.default_call_semantics(),
            config.services(),
            config.streams(),
            config.pools(),
            config.data_connectors(),
            config.endpoints(),
            config.links(),
        )?;
        for module in config.modules() {
            if runtime.modules_by_name.contains_key(&module.name) {
                return duplicate_name("module", &module.name);
            }
            runtime
                .modules_by_name
                .insert(module.name.clone(), Arc::new(module));
        }
        for data_type in config.types() {
            if runtime.types_by_name.contains_key(&data_type.name) {
                return duplicate_name("type", &data_type.name);
            }
            runtime
                .types_by_name
                .insert(data_type.name.clone(), Arc::new(data_type));
        }
        Ok(runtime)
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
            let Some(connector) = runtime
                .data_connectors_by_id
                .get(&endpoint.data_connector_id())
            else {
                return Err(RuntimeError::InvalidConfiguration(format!(
                    "endpoint {name:?} references unknown data connector {}",
                    endpoint.data_connector_id()
                )));
            };
            if connector.connector_type() != endpoint.connector_type() {
                return Err(RuntimeError::InvalidConfiguration(format!(
                    "endpoint {name:?} type does not match data connector {:?}",
                    connector.name()
                )));
            }
            match &endpoint {
                RuntimeEndpointConfig::Cron(config) if config.timezone != "UTC" => {
                    return Err(RuntimeError::InvalidConfiguration(format!(
                        "Cron endpoint {:?} requires timezone UTC",
                        config.name
                    )));
                }
                RuntimeEndpointConfig::Temporal(config)
                    if !config.schedule.is_empty() && config.timezone != "UTC" =>
                {
                    return Err(RuntimeError::InvalidConfiguration(format!(
                        "scheduled Temporal endpoint {:?} requires timezone UTC",
                        config.name
                    )));
                }
                _ => {}
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

    pub fn module_by_name(&self, name: &str) -> Option<Arc<ModuleConfig>> {
        self.modules_by_name.get(name).cloned()
    }

    pub fn modules(&self) -> Vec<Arc<ModuleConfig>> {
        self.modules_by_name.values().cloned().collect()
    }

    pub fn type_by_name(&self, name: &str) -> Option<Arc<TypeConfig>> {
        self.types_by_name.get(name).cloned()
    }

    pub fn types(&self) -> Vec<Arc<TypeConfig>> {
        self.types_by_name.values().cloned().collect()
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        api::{ScheduleMissedRunPolicy, ScheduleOverlapPolicy},
        runtime::config::{CronDataConnectorConfig, CronEndpointConfig},
    };

    #[test]
    fn scheduled_endpoint_rejects_non_utc_timezone() {
        let result = RuntimeConfig::from_parts(
            CallSemantics::FunctionCall,
            [],
            [],
            [],
            [RuntimeDataConnectorConfig::Cron(CronDataConnectorConfig {
                id: 1,
                name: "cron".to_owned(),
            })],
            [RuntimeEndpointConfig::Cron(CronEndpointConfig {
                id: 2,
                name: "tick".to_owned(),
                id_data_connector: 1,
                tracing_enabled: false,
                enabled: true,
                schedule: "* * * * *".to_owned(),
                timezone: "Europe/Moscow".to_owned(),
                overlap_policy: ScheduleOverlapPolicy::Skip,
                missed_run_policy: ScheduleMissedRunPolicy::Skip,
            })],
            [],
        );
        let error = result.err().expect("non-UTC timezone must fail");
        assert!(error.to_string().contains("requires timezone UTC"));
    }
}
