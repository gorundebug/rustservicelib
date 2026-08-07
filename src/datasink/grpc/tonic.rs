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
    environment::{Lifecycle, RuntimeError, RuntimeResult},
};

pub struct TonicDataSink {
    config: GrpcDataConnectorConfig,
    endpoint: RwLock<Endpoint>,
    channels: RwLock<Vec<Channel>>,
    next_channel: AtomicUsize,
    state: AtomicU8,
}

impl TonicDataSink {
    pub fn new(config: GrpcDataConnectorConfig) -> RuntimeResult<Arc<Self>> {
        if config.connections_count == 0 {
            return Err(RuntimeError::InvalidConfiguration(
                "gRPC connections_count must be at least 1".to_owned(),
            ));
        }
        let endpoint = Endpoint::from_shared(config.address.clone())
            .map_err(|error| RuntimeError::InvalidConfiguration(error.to_string()))?;
        Ok(Arc::new(Self {
            config,
            endpoint: RwLock::new(endpoint),
            channels: RwLock::new(Vec::new()),
            next_channel: AtomicUsize::new(0),
            state: AtomicU8::new(0),
        }))
    }

    pub async fn channel(&self) -> RuntimeResult<Channel> {
        let channels = self.channels.read().expect("gRPC channels lock poisoned");
        if channels.is_empty() {
            return Err(RuntimeError::ResourceStopped(self.config.name.clone()));
        }
        let index = self.next_channel.fetch_add(1, Ordering::Relaxed) % channels.len();
        Ok(channels[index].clone())
    }

    pub fn reload_address(&self, address: String) -> RuntimeResult<()> {
        let endpoint = Endpoint::from_shared(address)
            .map_err(|error| RuntimeError::InvalidConfiguration(error.to_string()))?;
        let channels = self.make_channels(&endpoint);
        *self.endpoint.write().expect("gRPC endpoint lock poisoned") = endpoint;
        if self.state.load(Ordering::Acquire) == 1 {
            *self.channels.write().expect("gRPC channels lock poisoned") = channels;
            self.next_channel.store(0, Ordering::Release);
        }
        Ok(())
    }

    fn make_channels(&self, endpoint: &Endpoint) -> Vec<Channel> {
        (0..self.config.connections_count)
            .map(|_| endpoint.clone().connect_lazy())
            .collect()
    }
}

impl DataSink for TonicDataSink {
    fn id(&self) -> i32 {
        self.config.id
    }

    fn name(&self) -> &str {
        &self.config.name
    }
}

#[async_trait]
impl Lifecycle for TonicDataSink {
    async fn start(&self, _context: MessageContext) -> RuntimeResult<()> {
        self.state
            .compare_exchange(0, 1, Ordering::AcqRel, Ordering::Acquire)
            .map_err(|state| {
                if state == 2 {
                    RuntimeError::ResourceStopped(self.config.name.clone())
                } else {
                    RuntimeError::ResourceAlreadyStarted(self.config.name.clone())
                }
            })?;
        let channels =
            self.make_channels(&self.endpoint.read().expect("gRPC endpoint lock poisoned"));
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
        assert!(TonicDataSink::new(config(0)).is_err());
    }

    #[tokio::test]
    async fn creates_every_configured_channel() {
        let sink = TonicDataSink::new(config(3)).unwrap();
        sink.start(MessageContext::default()).await.unwrap();
        assert_eq!(
            sink.channels
                .read()
                .expect("gRPC channels lock poisoned")
                .len(),
            3
        );
    }
}
