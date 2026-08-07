use std::sync::{Arc, Mutex};

use async_trait::async_trait;

use crate::runtime::{
    common::MessageContext,
    config::GrpcDataConnectorConfig,
    datasource::DataSource,
    environment::{Lifecycle, RuntimeError, RuntimeResult},
};

pub struct TonicDataSource {
    config: GrpcDataConnectorConfig,
    state: Mutex<u8>,
}

impl TonicDataSource {
    pub fn new(config: GrpcDataConnectorConfig) -> Arc<Self> {
        Arc::new(Self {
            config,
            state: Mutex::new(0),
        })
    }
}

impl DataSource for TonicDataSource {
    fn id(&self) -> i32 {
        self.config.id
    }

    fn name(&self) -> &str {
        &self.config.name
    }
}

#[async_trait]
impl Lifecycle for TonicDataSource {
    async fn start(&self, _context: MessageContext) -> RuntimeResult<()> {
        let mut state = self
            .state
            .lock()
            .expect("gRPC datasource state lock poisoned");
        match *state {
            0 => {
                *state = 1;
                Ok(())
            }
            1 => Err(RuntimeError::ResourceAlreadyStarted(
                self.config.name.clone(),
            )),
            _ => Err(RuntimeError::ResourceStopped(self.config.name.clone())),
        }
    }

    async fn stop(&self, _context: MessageContext) -> RuntimeResult<()> {
        *self
            .state
            .lock()
            .expect("gRPC datasource state lock poisoned") = 2;
        Ok(())
    }
}
