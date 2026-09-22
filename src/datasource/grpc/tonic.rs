use std::sync::{Arc, Mutex};

use async_trait::async_trait;

use crate::runtime::{
    common::MessageContext,
    config::GrpcDataConnectorConfig,
    datasource::DataSource,
    environment::{Lifecycle, RuntimeEnvironment, RuntimeError, RuntimeResult},
};
use crate::{operators::InputStream, runtime::config::RuntimeDataConnectorConfig};

/// Construction-time policy for one gRPC server connector.
#[derive(Clone, Copy, Debug)]
pub struct TonicDataSourceOptions {
    /// Maximum queued response messages per streaming RPC, not a concurrency limit.
    pub stream_buffer_capacity: usize,
}

impl Default for TonicDataSourceOptions {
    fn default() -> Self {
        Self {
            stream_buffer_capacity: 16,
        }
    }
}

pub struct TonicDataSource {
    id: i32,
    name: String,
    state: Mutex<u8>,
    stream_buffer_capacity: usize,
}

impl TonicDataSource {
    fn new(id: i32, name: String, options: TonicDataSourceOptions) -> Arc<Self> {
        Arc::new(Self {
            id,
            name,
            state: Mutex::new(0),
            stream_buffer_capacity: options.stream_buffer_capacity,
        })
    }

    pub fn from_config(
        environment: RuntimeEnvironment,
        config: &GrpcDataConnectorConfig,
    ) -> RuntimeResult<Arc<Self>> {
        Self::from_config_with_options(environment, config, TonicDataSourceOptions::default())
    }

    /// Customize this server independently of outbound clients in its maker.
    pub fn from_config_with_options(
        _environment: RuntimeEnvironment,
        config: &GrpcDataConnectorConfig,
        options: TonicDataSourceOptions,
    ) -> RuntimeResult<Arc<Self>> {
        if options.stream_buffer_capacity == 0
            || options.stream_buffer_capacity > tokio::sync::Semaphore::MAX_PERMITS
        {
            return Err(RuntimeError::InvalidConfiguration(
                "gRPC server stream_buffer_capacity is outside the supported channel capacity range".to_owned(),
            ));
        }
        Ok(Self::new(config.id, config.name.clone(), options))
    }

    pub fn stream_buffer_capacity(&self) -> usize {
        self.stream_buffer_capacity
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
                Self::from_config(input.stream().environment().clone(), config)
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
