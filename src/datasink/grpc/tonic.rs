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

/// Construction-time policy for one gRPC client connector.
#[derive(Clone, Copy, Debug)]
pub struct TonicDataSinkOptions {
    /// Maximum queued request messages per streaming RPC, not a concurrency limit.
    pub stream_buffer_capacity: usize,
}

impl Default for TonicDataSinkOptions {
    fn default() -> Self {
        Self {
            stream_buffer_capacity: 16,
        }
    }
}

pub struct TonicDataSink {
    environment: RuntimeEnvironment,
    id: i32,
    name: String,
    channels: RwLock<Vec<Channel>>,
    next_channel: AtomicUsize,
    state: AtomicU8,
    stream_buffer_capacity: usize,
}

impl TonicDataSink {
    fn endpoint(address: &str) -> RuntimeResult<Endpoint> {
        let invalid = |error: String| {
            RuntimeError::InvalidConfiguration(format!("gRPC address {address:?}: {error}"))
        };
        let Some(target) = address.strip_prefix("dns:") else {
            return Endpoint::from_shared(address.to_owned())
                .map_err(|error| invalid(error.to_string()));
        };
        // gRPC resolver URIs name a host in the path, whereas Tonic expects
        // an HTTP URI. Resolution remains asynchronous in Tonic's connector.
        let target = target.strip_prefix("///").unwrap_or(target);
        if target.starts_with("//") {
            return Err(invalid("custom DNS resolver authorities are not supported".to_owned()));
        }
        let authority = target
            .parse::<tonic::codegen::http::uri::Authority>()
            .map_err(|error| invalid(error.to_string()))?;
        if authority.host().is_empty()
            || target.contains('@')
            || target.ends_with(':')
            || (authority.port().is_some() && authority.port_u16().is_none())
        {
            return Err(invalid("expected a DNS host and optional numeric port".to_owned()));
        }
        // DNS selects a resolver, not TLS. Match gRPC's default target port;
        // explicit HTTP(S) addresses above retain their existing semantics.
        let port = if authority.port().is_none() { ":443" } else { "" };
        Endpoint::from_shared(format!("http://{authority}{port}"))
            .map_err(|error| invalid(error.to_string()))
    }

    fn validate_config(config: &GrpcDataConnectorConfig) -> RuntimeResult<Endpoint> {
        if config.connections_count == 0 {
            return Err(RuntimeError::InvalidConfiguration(
                "gRPC connections_count must be at least 1".to_owned(),
            ));
        }
        Self::endpoint(&config.address)
    }

    fn new(
        environment: RuntimeEnvironment,
        id: i32,
        name: String,
        options: TonicDataSinkOptions,
    ) -> Arc<Self> {
        Arc::new(Self {
            environment,
            id,
            name,
            channels: RwLock::new(Vec::new()),
            next_channel: AtomicUsize::new(0),
            state: AtomicU8::new(0),
            stream_buffer_capacity: options.stream_buffer_capacity,
        })
    }

    pub fn from_config(
        environment: RuntimeEnvironment,
        config: &GrpcDataConnectorConfig,
    ) -> RuntimeResult<Arc<Self>> {
        Self::from_config_with_options(environment, config, TonicDataSinkOptions::default())
    }

    /// Customize this client in the service's user-owned infrastructure maker.
    pub fn from_config_with_options(
        environment: RuntimeEnvironment,
        config: &GrpcDataConnectorConfig,
        options: TonicDataSinkOptions,
    ) -> RuntimeResult<Arc<Self>> {
        if options.stream_buffer_capacity == 0
            || options.stream_buffer_capacity > tokio::sync::Semaphore::MAX_PERMITS
        {
            return Err(RuntimeError::InvalidConfiguration(
                "gRPC client stream_buffer_capacity is outside the supported channel capacity range".to_owned(),
            ));
        }
        Self::validate_config(config)?;
        Ok(Self::new(
            environment,
            config.id,
            config.name.clone(),
            options,
        ))
    }

    pub fn stream_buffer_capacity(&self) -> usize {
        self.stream_buffer_capacity
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
        let endpoint = Self::endpoint(&address)?;
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
    use crate::runtime::config::{CallSemantics, RuntimeConfig};
    use std::time::Duration;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

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

    #[test]
    fn accepts_dns_targets_without_changing_http_uris() {
        for (address, expected) in [
            ("dns:///ctproxy.invalid:443", "http://ctproxy.invalid:443/"),
            ("dns:inventory:9202", "http://inventory:9202/"),
            ("dns:///inventory", "http://inventory:443/"),
            ("dns:///[::1]:9202", "http://[::1]:9202/"),
            ("dns:///[::1]", "http://[::1]:443/"),
            ("http://inventory:9202/", "http://inventory:9202/"),
            ("https://inventory:443/", "https://inventory:443/"),
        ] {
            let mut config = config(1);
            config.address = address.to_owned();
            let endpoint = TonicDataSink::validate_config(&config).unwrap();
            assert_eq!(endpoint.uri().to_string(), expected, "{address}");
            assert_eq!(config.address, address);
        }
    }

    #[test]
    fn rejects_malformed_dns_targets() {
        for address in [
            "dns:",
            "dns:///",
            "dns://resolver/inventory:9202",
            "dns:///inventory:9202/path",
            "dns:///inventory?query",
            "dns:///inventory#fragment",
            "dns:///user@inventory:9202",
            "dns:///inventory:",
            "dns:///inventory:99999",
            "dns:///inventory:not-a-port",
            "dns:///bad host:9202",
        ] {
            let error = TonicDataSink::endpoint(address).unwrap_err().to_string();
            assert!(error.contains("gRPC address"), "{address}: {error}");
        }
    }

    #[tokio::test]
    async fn dns_targets_work_through_start_and_reload() {
        let mut config = config(3);
        config.address = "dns:///inventory.invalid:9202".to_owned();
        let environment = RuntimeEnvironment::default();
        environment.publish_runtime_config(Arc::new(
            RuntimeConfig::from_parts(
                CallSemantics::FunctionCall,
                [],
                [],
                [],
                [config.clone().into()],
                [],
                [],
            )
            .unwrap(),
        ));
        let sink = TonicDataSink::from_config(environment, &config).unwrap();
        sink.start(MessageContext::new()).await.unwrap();
        assert_eq!(sink.channels.read().unwrap().len(), 3);
        sink.reload_address("dns:///replacement.invalid:9203".to_owned())
            .unwrap();
        assert_eq!(sink.channels.read().unwrap().len(), 3);
        assert!(sink.reload_address("dns:///".to_owned()).is_err());
        assert!(sink.channel().await.is_ok());
        sink.stop(MessageContext::new()).await.unwrap();
        assert!(sink.channel().await.is_err());
    }

    #[tokio::test]
    async fn dns_target_connects_to_local_http2_listener() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let (done, finished) = tokio::sync::oneshot::channel();
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut preface = [0; 24];
            socket.read_exact(&mut preface).await.unwrap();
            assert_eq!(&preface, b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n");
            socket.write_all(&[0, 0, 0, 4, 0, 0, 0, 0, 0]).await.unwrap();
            let _ = finished.await;
        });
        let endpoint = TonicDataSink::endpoint(&format!("dns:///localhost:{port}")).unwrap();
        let channel = tokio::time::timeout(Duration::from_secs(5), endpoint.connect())
            .await
            .unwrap()
            .unwrap();
        done.send(()).unwrap();
        tokio::time::timeout(Duration::from_secs(5), server)
            .await
            .unwrap()
            .unwrap();
        drop(channel);
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
