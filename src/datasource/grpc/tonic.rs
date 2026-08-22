use std::sync::{Arc, Mutex};

use async_trait::async_trait;

use crate::runtime::{
    common::MessageContext,
    datasource::DataSource,
    environment::{Lifecycle, RuntimeError, RuntimeResult},
};
use crate::{operators::InputStream, runtime::config::RuntimeDataConnectorConfig};

pub struct TonicDataSource {
    id: i32,
    name: String,
    state: Mutex<u8>,
}

impl TonicDataSource {
    fn new(id: i32, name: String) -> Arc<Self> {
        Arc::new(Self {
            id,
            name,
            state: Mutex::new(0),
        })
    }

    pub fn from_input<T, R, E>(input: &InputStream<T, R, E>) -> RuntimeResult<Arc<Self>>
    where
        T: Send + Sync + 'static,
        R: Send + Sync + 'static,
        E: Send + Sync + 'static,
    {
        let runtime = input.stream().environment().runtime_config();
        let endpoint = runtime.endpoint_by_id(input.endpoint_id()).ok_or_else(|| {
            RuntimeError::InvalidConfiguration(format!(
                "endpoint {} referenced by input stream {:?} is not configured",
                input.endpoint_id(),
                input.stream().name()
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
                Ok(Self::new(config.id, config.name.clone()))
            }
            _ => Err(RuntimeError::InvalidConfiguration(format!(
                "endpoint {:?} does not reference a gRPC data connector",
                endpoint.name()
            ))),
        }
    }
}

impl DataSource for TonicDataSource {
    fn id(&self) -> i32 {
        self.id
    }

    fn name(&self) -> &str {
        &self.name
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
            1 => Err(RuntimeError::ResourceAlreadyStarted(self.name.clone())),
            _ => Err(RuntimeError::ResourceStopped(self.name.clone())),
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
