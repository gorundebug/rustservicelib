use std::sync::{
    Arc, RwLock,
    atomic::{AtomicU8, Ordering},
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
    channel: RwLock<Option<Channel>>,
    state: AtomicU8,
}

impl TonicDataSink {
    pub fn new(config: GrpcDataConnectorConfig) -> RuntimeResult<Arc<Self>> {
        let endpoint = Endpoint::from_shared(config.address.clone())
            .map_err(|error| RuntimeError::InvalidConfiguration(error.to_string()))?;
        Ok(Arc::new(Self {
            config,
            endpoint: RwLock::new(endpoint),
            channel: RwLock::new(None),
            state: AtomicU8::new(0),
        }))
    }

    pub async fn channel(&self) -> RuntimeResult<Channel> {
        self.channel
            .read()
            .expect("gRPC channel lock poisoned")
            .clone()
            .ok_or_else(|| RuntimeError::ResourceStopped(self.config.name.clone()))
    }

    pub fn reload_address(&self, address: String) -> RuntimeResult<()> {
        let endpoint = Endpoint::from_shared(address)
            .map_err(|error| RuntimeError::InvalidConfiguration(error.to_string()))?;
        let channel = endpoint.connect_lazy();
        *self.endpoint.write().expect("gRPC endpoint lock poisoned") = endpoint;
        if self.state.load(Ordering::Acquire) == 1 {
            *self.channel.write().expect("gRPC channel lock poisoned") = Some(channel);
        }
        Ok(())
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
        let channel = self
            .endpoint
            .read()
            .expect("gRPC endpoint lock poisoned")
            .connect_lazy();
        *self.channel.write().expect("gRPC channel lock poisoned") = Some(channel);
        Ok(())
    }

    async fn stop(&self, _context: MessageContext) -> RuntimeResult<()> {
        self.state.store(2, Ordering::Release);
        self.channel
            .write()
            .expect("gRPC channel lock poisoned")
            .take();
        Ok(())
    }
}
