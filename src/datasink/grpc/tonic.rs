use std::sync::{
    Arc, RwLock,
    atomic::{AtomicU8, AtomicUsize, Ordering},
};

use async_trait::async_trait;
use tonic::transport::{Channel, Endpoint};

use crate::runtime::{
    common::MessageContext,
    config::GrpcDataConnectorConfig,
    datasink::DataSink,
    environment::{Lifecycle, RuntimeEnvironment, RuntimeError, RuntimeResult},
};
use crate::{
    operators::SinkStreamWithResult,
    runtime::{common::RuntimeStream, config::RuntimeDataConnectorConfig},
};

pub struct TonicDataSink {
    environment: RuntimeEnvironment,
    id: i32,
    name: String,
    channels: RwLock<Vec<Channel>>,
    next_channel: AtomicUsize,
    state: AtomicU8,
}

impl TonicDataSink {
    fn validate_config(config: &GrpcDataConnectorConfig) -> RuntimeResult<Endpoint> {
        if config.connections_count == 0 {
            return Err(RuntimeError::InvalidConfiguration(
                "gRPC connections_count must be at least 1".to_owned(),
            ));
        }
        Endpoint::from_shared(config.address.clone())
            .map_err(|error| RuntimeError::InvalidConfiguration(error.to_string()))
    }

    fn new(environment: RuntimeEnvironment, id: i32, name: String) -> Arc<Self> {
        Arc::new(Self {
            environment,
            id,
            name,
            channels: RwLock::new(Vec::new()),
            next_channel: AtomicUsize::new(0),
            state: AtomicU8::new(0),
        })
    }

    pub fn from_config(
        environment: RuntimeEnvironment,
        config: &GrpcDataConnectorConfig,
    ) -> RuntimeResult<Arc<Self>> {
        Self::validate_config(config)?;
        Ok(Self::new(environment, config.id, config.name.clone()))
    }

    pub fn from_stream<T, R, E>(
        stream: &Arc<SinkStreamWithResult<T, R, E>>,
    ) -> RuntimeResult<Arc<Self>>
    where
        T: Send + Sync + 'static,
        R: Send + Sync + 'static,
        E: Send + Sync + 'static,
    {
        let runtime = stream.environment().runtime_config();
        let endpoint = runtime
            .endpoint_by_id(stream.endpoint_id())
            .ok_or_else(|| {
                RuntimeError::InvalidConfiguration(format!(
                    "endpoint {} referenced by sink stream {:?} is not configured",
                    stream.endpoint_id(),
                    stream.name()
                ))
            })?;
        let connector = runtime
            .data_connector_by_id(endpoint.data_connector_id())
            .ok_or_else(|| {
                RuntimeError::InvalidConfiguration(format!(
                    "data connector {} referenced by endpoint {:?} is not configured",
                    endpoint.data_connector_id(),
                    endpoint.name()
                ))
            })?;
        match connector.as_ref() {
            RuntimeDataConnectorConfig::Grpc(config) => {
                Self::from_config(stream.environment().clone(), config)
            }
            _ => Err(RuntimeError::InvalidConfiguration(format!(
                "endpoint {:?} does not reference a gRPC data connector",
                endpoint.name()
            ))),
        }
    }

    pub async fn channel(&self) -> RuntimeResult<Channel> {
        let channels = self.channels.read().expect("gRPC channels lock poisoned");
        if channels.is_empty() {
            return Err(RuntimeError::ResourceStopped(self.name.clone()));
        }
        let index = self.next_channel.fetch_add(1, Ordering::Relaxed) % channels.len();
        Ok(channels[index].clone())
    }

    pub fn reload_address(&self, address: String) -> RuntimeResult<()> {
        let endpoint = Endpoint::from_shared(address)
            .map_err(|error| RuntimeError::InvalidConfiguration(error.to_string()))?;
        let config = self.connector_config()?;
        let channels = Self::make_channels(&endpoint, config.connections_count);
        if self.state.load(Ordering::Acquire) == 1 {
            *self.channels.write().expect("gRPC channels lock poisoned") = channels;
            self.next_channel.store(0, Ordering::Release);
        }
        Ok(())
    }

    fn make_channels(endpoint: &Endpoint, connections_count: usize) -> Vec<Channel> {
        (0..connections_count)
            .map(|_| endpoint.clone().connect_lazy())
            .collect()
    }

    fn connector_config(&self) -> RuntimeResult<GrpcDataConnectorConfig> {
        let runtime = self.environment.runtime_config();
        let connector = runtime.data_connector_by_id(self.id).ok_or_else(|| {
            RuntimeError::InvalidConfiguration(format!(
                "gRPC data connector {} is not configured",
                self.id
            ))
        })?;
        match connector.as_ref() {
            RuntimeDataConnectorConfig::Grpc(config) => Ok(config.clone()),
            _ => Err(RuntimeError::InvalidConfiguration(format!(
                "data connector {:?} is not gRPC",
                self.name
            ))),
        }
    }
}

impl DataSink for TonicDataSink {
    fn id(&self) -> i32 {
        self.id
    }

    fn name(&self) -> &str {
        &self.name
    }
}

#[async_trait]
impl Lifecycle for TonicDataSink {
    async fn start(&self, _context: MessageContext) -> RuntimeResult<()> {
        self.state
            .compare_exchange(0, 1, Ordering::AcqRel, Ordering::Acquire)
            .map_err(|state| {
                if state == 2 {
                    RuntimeError::ResourceStopped(self.name.clone())
                } else {
                    RuntimeError::ResourceAlreadyStarted(self.name.clone())
                }
            })?;
        let config = self.connector_config()?;
        let endpoint = Self::validate_config(&config)?;
        let channels = Self::make_channels(&endpoint, config.connections_count);
        *self.channels.write().expect("gRPC channels lock poisoned") = channels;
        self.next_channel.store(0, Ordering::Release);
        Ok(())
    }

    async fn stop(&self, _context: MessageContext) -> RuntimeResult<()> {
        self.state.store(2, Ordering::Release);
        self.channels
            .write()
            .expect("gRPC channels lock poisoned")
            .clear();
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config(connections_count: usize) -> GrpcDataConnectorConfig {
        GrpcDataConnectorConfig {
            id: 1,
            name: "inventory".to_owned(),
            address: "http://127.0.0.1:9202".to_owned(),
            connections_count,
        }
    }

    #[test]
    fn rejects_zero_connections() {
        assert!(TonicDataSink::validate_config(&config(0)).is_err());
    }

    #[tokio::test]
    async fn creates_every_configured_channel() {
        let config = config(3);
        let endpoint = TonicDataSink::validate_config(&config).unwrap();
        assert_eq!(
            TonicDataSink::make_channels(&endpoint, config.connections_count).len(),
            3
        );
    }
}
