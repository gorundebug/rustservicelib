use std::{
    collections::{HashMap, HashSet},
    error::Error,
    future::Future,
    pin::Pin,
    sync::{
        Arc, Mutex, Weak,
        atomic::{AtomicBool, AtomicI32, AtomicU8, Ordering},
    },
    time::{Duration, Instant},
};

use async_trait::async_trait;
use rdkafka::{
    ClientConfig,
    admin::{AdminClient, AdminOptions, NewTopic, TopicReplication},
    client::DefaultClientContext,
    message::{Header, OwnedHeaders},
    producer::{FutureProducer, FutureRecord, Producer},
    types::RDKafkaErrorCode,
    util::Timeout,
};
use tokio_util::task::TaskTracker;
use tracing_opentelemetry::OpenTelemetrySpanExt;

use crate::{
    operators::SinkStreamWithResult,
    runtime::{
        common::{Consumer, MessageContext, Payload, RuntimeStream},
        config::{
            KafkaDataConnectorConfig, KafkaEndpointConfig, RuntimeDataConnectorConfig,
            RuntimeEndpointConfig,
        },
        datasink::{DataSink, SinkStreamContext},
        environment::{
            Lifecycle, RuntimeEnvironment, RuntimeError, RuntimeResult, metrics::Labels,
        },
        telemetry::librdkafka_statistics::LibrdkafkaStatisticsContext,
    },
};

pub const IMPLEMENTATION: &str = "rust/rdkafka";

pub type HandlerError = Box<dyn Error + Send + Sync>;
pub type HandlerResult<T = ()> = Result<T, HandlerError>;
pub type BoxFuture<T> = Pin<Box<dyn Future<Output = T> + Send>>;
pub type StreamContext<T, R, E> = SinkStreamContext<T, R, E>;

type SendFunction = Arc<
    dyn Fn(
            String,
            Option<Vec<u8>>,
            Vec<u8>,
            Option<i32>,
            HashMap<String, String>,
        ) -> BoxFuture<HandlerResult<(i32, i64)>>
        + Send
        + Sync,
>;
type SpawnFunction = Arc<dyn Fn(BoxFuture<()>) + Send + Sync>;
type PartitionFunction = Arc<dyn Fn() -> HandlerResult<Option<i32>> + Send + Sync>;

pub struct RdkafkaKafkaDataSink {
    environment: RuntimeEnvironment,
    id: i32,
    name: String,
    producer: Mutex<Option<FutureProducer<LibrdkafkaStatisticsContext>>>,
    endpoints: Mutex<Vec<(i32, Arc<KafkaEndpointRuntimeState>)>>,
    tasks: TaskTracker,
    state: AtomicU8,
}

struct KafkaEndpointRuntimeState {
    enabled: AtomicBool,
    partitions: AtomicI32,
}

impl KafkaEndpointRuntimeState {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            enabled: AtomicBool::new(false),
            partitions: AtomicI32::new(1),
        })
    }
}

impl RdkafkaKafkaDataSink {
    fn new(environment: RuntimeEnvironment, id: i32, name: String) -> Arc<Self> {
        Arc::new(Self {
            environment,
            id,
            name,
            producer: Mutex::new(None),
            endpoints: Mutex::new(Vec::new()),
            tasks: TaskTracker::new(),
            state: AtomicU8::new(0),
        })
    }

    pub fn from_stream<T, R, E>(
        stream: &Arc<SinkStreamWithResult<T, R, E>>,
    ) -> RuntimeResult<Arc<Self>>
    where
        T: Send + Sync + 'static,
        R: Send + Sync + 'static,
        E: Send + Sync + 'static,
    {
        let (endpoint, connector) = kafka_sink_configs(stream)?;
        if endpoint.id_data_connector != connector.id {
            return Err(RuntimeError::InvalidConfiguration(format!(
                "Kafka endpoint {:?} references connector {}, expected {}",
                endpoint.name, endpoint.id_data_connector, connector.id
            )));
        }
        Ok(Self::new(
            stream.environment().clone(),
            connector.id,
            connector.name,
        ))
    }

    fn add_endpoint(
        &self,
        endpoint: &KafkaEndpointConfig,
        runtime_state: Arc<KafkaEndpointRuntimeState>,
    ) -> RuntimeResult<()> {
        if endpoint.id_data_connector != self.id {
            return Err(RuntimeError::InvalidConfiguration(format!(
                "Kafka endpoint {:?} references connector {}, expected {}",
                endpoint.name, endpoint.id_data_connector, self.id
            )));
        }
        if self.state.load(Ordering::Acquire) != 0 {
            return Err(RuntimeError::ResourceAlreadyStarted(self.name.clone()));
        }
        let mut endpoints = self
            .endpoints
            .lock()
            .expect("Kafka data sink endpoints lock poisoned");
        if self.state.load(Ordering::Acquire) != 0 {
            return Err(RuntimeError::ResourceAlreadyStarted(self.name.clone()));
        }
        if endpoints.iter().any(|(id, _)| *id == endpoint.id) {
            return Err(RuntimeError::DuplicateResource(endpoint.name.clone()));
        }
        endpoints.push((endpoint.id, runtime_state));
        Ok(())
    }

    async fn create_topics(&self, admin: &AdminClient<DefaultClientContext>) -> HandlerResult {
        let endpoint_ids = self
            .endpoints
            .lock()
            .expect("Kafka data sink endpoints lock poisoned")
            .iter()
            .map(|(id, _)| *id)
            .collect::<Vec<_>>();
        let runtime = self.environment.runtime_config();
        let endpoints = endpoint_ids
            .iter()
            .filter_map(|id| runtime.endpoint_by_id(*id))
            .filter_map(|endpoint| match endpoint.as_ref() {
                RuntimeEndpointConfig::Kafka(config) => Some(config.clone()),
                _ => None,
            })
            .collect::<Vec<_>>();
        let mut names = HashSet::new();
        let topics = endpoints
            .iter()
            .filter(|endpoint| {
                endpoint.enabled && endpoint.create_topic && names.insert(endpoint.topic.clone())
            })
            .map(|endpoint| {
                NewTopic::new(
                    &endpoint.topic,
                    endpoint.partitions.max(1),
                    TopicReplication::Fixed(endpoint.replication_factor.max(1)),
                )
            })
            .collect::<Vec<_>>();
        if topics.is_empty() {
            return Ok(());
        }
        for result in admin.create_topics(&topics, &AdminOptions::new()).await? {
            match result {
                Ok(_) | Err((_, RDKafkaErrorCode::TopicAlreadyExists)) => {}
                Err((topic, error)) => {
                    return Err(format!("create Kafka topic {topic:?} failed: {error}").into());
                }
            }
        }
        Ok(())
    }

    async fn send(
        &self,
        topic: String,
        key: Option<Vec<u8>>,
        value: Vec<u8>,
        partition: Option<i32>,
        transport_metadata: HashMap<String, String>,
    ) -> HandlerResult<(i32, i64)> {
        if self.state.load(Ordering::Acquire) != 2 {
            return Err("Kafka producer is not running".into());
        }
        let mut record = FutureRecord::<[u8], [u8]>::to(&topic).payload(&value);
        if let Some(key) = key.as_deref() {
            record = record.key(key);
        }
        if let Some(partition) = partition {
            record = record.partition(partition);
        }
        if !transport_metadata.is_empty() {
            let mut headers = OwnedHeaders::new();
            for (key, value) in &transport_metadata {
                headers = headers.insert(Header {
                    key,
                    value: Some(value.as_bytes()),
                });
            }
            record = record.headers(headers);
        }
        let producer = self
            .producer
            .lock()
            .expect("Kafka producer lock poisoned")
            .clone()
            .ok_or_else(|| -> HandlerError { "Kafka producer is not running".into() })?;
        producer
            .send(record, Timeout::Never)
            .await
            .map(|delivery| (delivery.partition, delivery.offset))
            .map_err(|(error, _)| -> HandlerError { Box::new(error) })
    }
}

impl DataSink for RdkafkaKafkaDataSink {
    fn id(&self) -> i32 {
        self.id
    }

    fn name(&self) -> &str {
        &self.name
    }
}

#[async_trait]
impl Lifecycle for RdkafkaKafkaDataSink {
    async fn start(&self, _context: MessageContext) -> RuntimeResult<()> {
        self.state
            .compare_exchange(0, 1, Ordering::AcqRel, Ordering::Acquire)
            .map_err(|state| {
                if state == 3 {
                    RuntimeError::ResourceStopped(self.name.clone())
                } else {
                    RuntimeError::ResourceAlreadyStarted(self.name.clone())
                }
            })?;
        let runtime = self.environment.runtime_config();
        let endpoints = self
            .endpoints
            .lock()
            .expect("Kafka data sink endpoints lock poisoned")
            .clone();
        let mut any_enabled = false;
        let mut enabled_topics = Vec::new();
        for (endpoint_id, state) in &endpoints {
            let Some(endpoint) = runtime.endpoint_by_id(*endpoint_id) else {
                self.state.store(0, Ordering::Release);
                return Err(RuntimeError::InvalidConfiguration(format!(
                    "Kafka endpoint {endpoint_id} is not configured"
                )));
            };
            let RuntimeEndpointConfig::Kafka(endpoint) = endpoint.as_ref() else {
                self.state.store(0, Ordering::Release);
                return Err(RuntimeError::InvalidConfiguration(format!(
                    "endpoint {endpoint_id} is not Kafka"
                )));
            };
            state.enabled.store(endpoint.enabled, Ordering::Release);
            state
                .partitions
                .store(endpoint.partitions.max(1), Ordering::Release);
            if endpoint.enabled {
                enabled_topics.push((Arc::clone(state), endpoint.topic.clone()));
            }
            any_enabled |= endpoint.enabled;
        }
        if !any_enabled {
            self.state.store(2, Ordering::Release);
            return Ok(());
        }
        let Some(connector) = runtime.data_connector_by_id(self.id) else {
            self.state.store(0, Ordering::Release);
            return Err(RuntimeError::InvalidConfiguration(format!(
                "Kafka data connector {} is not configured",
                self.id
            )));
        };
        let RuntimeDataConnectorConfig::Kafka(connector) = connector.as_ref() else {
            self.state.store(0, Ordering::Release);
            return Err(RuntimeError::InvalidConfiguration(format!(
                "data connector {:?} is not Kafka",
                self.name
            )));
        };
        if connector.brokers.trim().is_empty() {
            self.state.store(0, Ordering::Release);
            return Err(RuntimeError::InvalidConfiguration(format!(
                "no brokers specified for Kafka data connector {:?}",
                self.name
            )));
        }
        let mut client_config = ClientConfig::new();
        client_config
            .set("bootstrap.servers", &connector.brokers)
            // Go's default partitioner is uniform even when a key is present.
            // Leaving the record partition unset lets librdkafka apply the
            // same policy using its current broker metadata.
            .set("partitioner", "random");
        apply_security(&mut client_config, connector)?;
        if !connector.version.trim().is_empty() {
            client_config
                .set("broker.version.fallback", &connector.version)
                .set("api.version.request", "false");
        }
        if connector.dial_timeout > 0.0 {
            client_config.set(
                "socket.timeout.ms",
                (connector.dial_timeout.ceil() as u64).to_string(),
            );
        }
        let statistics = LibrdkafkaStatisticsContext::new(self.environment.metrics(), "producer")?;
        statistics.configure(&mut client_config);
        let admin = match client_config.create::<AdminClient<DefaultClientContext>>() {
            Ok(admin) => admin,
            Err(error) => {
                self.state.store(0, Ordering::Release);
                return Err(RuntimeError::Transport(error.to_string()));
            }
        };
        let producer = match client_config.create_with_context::<_, FutureProducer<_>>(statistics) {
            Ok(producer) => producer,
            Err(error) => {
                self.state.store(0, Ordering::Release);
                return Err(RuntimeError::Transport(error.to_string()));
            }
        };
        if let Err(error) = self.create_topics(&admin).await {
            self.state.store(0, Ordering::Release);
            return Err(RuntimeError::Transport(error.to_string()));
        }
        let metadata_producer = producer.clone();
        let metadata_timeout = Duration::from_millis(if connector.dial_timeout > 0.0 {
            connector.dial_timeout.ceil() as u64
        } else {
            10_000
        });
        let discovered = tokio::task::spawn_blocking(move || -> HandlerResult<_> {
            let metadata = metadata_producer
                .client()
                .fetch_metadata(None, Timeout::After(metadata_timeout))?;
            let counts = metadata
                .topics()
                .iter()
                .map(|topic| (topic.name(), topic.partitions().len()))
                .collect::<HashMap<_, _>>();
            enabled_topics
                .into_iter()
                .map(|(state, topic)| {
                    let count = counts.get(topic.as_str()).copied().unwrap_or_default();
                    let count = i32::try_from(count)
                        .ok()
                        .filter(|count| *count > 0)
                        .ok_or_else(|| -> HandlerError {
                            format!("Kafka topic {topic:?} has no partitions").into()
                        })?;
                    Ok((state, count))
                })
                .collect::<HandlerResult<Vec<_>>>()
        })
        .await
        .map_err(|error| RuntimeError::Transport(error.to_string()))?
        .map_err(|error| RuntimeError::Transport(error.to_string()))?;
        for (state, partitions) in discovered {
            state.partitions.store(partitions, Ordering::Release);
        }
        *self.producer.lock().expect("Kafka producer lock poisoned") = Some(producer);
        self.state.store(2, Ordering::Release);
        Ok(())
    }

    async fn stop(&self, context: MessageContext) -> RuntimeResult<()> {
        let previous = self.state.swap(3, Ordering::AcqRel);
        if previous == 3 {
            return Ok(());
        }
        self.tasks.close();
        tokio::select! {
            () = self.tasks.wait() => {}
            () = context.cancelled() => return Err(RuntimeError::ContextCancelled),
        }
        loop {
            let producer = self
                .producer
                .lock()
                .expect("Kafka producer lock poisoned")
                .clone();
            let Some(producer) = producer else {
                break Ok(());
            };
            match producer.flush(Timeout::After(std::time::Duration::from_millis(100))) {
                Ok(()) => return Ok(()),
                Err(_) if context.is_cancelled() => return Err(RuntimeError::ContextCancelled),
                Err(_) => {}
            }
        }
    }
}

fn apply_security(
    client_config: &mut ClientConfig,
    connector: &KafkaDataConnectorConfig,
) -> RuntimeResult<()> {
    let protocol = match connector.security_protocol {
        crate::api::KafkaSecurityProtocol::PLAINTEXT => "PLAINTEXT",
        crate::api::KafkaSecurityProtocol::SASLPLAINTEXT => "SASL_PLAINTEXT",
        crate::api::KafkaSecurityProtocol::SASLSSL => "SASL_SSL",
    };
    client_config.set("security.protocol", protocol);
    if protocol == "PLAINTEXT" {
        return Ok(());
    }
    if connector.username.is_empty() || connector.password.is_empty() {
        return Err(RuntimeError::InvalidConfiguration(
            "Kafka SASL username and password must both be configured".to_owned(),
        ));
    }
    let mechanism = match connector.sasl_mechanism {
        crate::api::KafkaSaslMechanism::PLAIN => "PLAIN",
        crate::api::KafkaSaslMechanism::SCRAMSHA256 => "SCRAM-SHA-256",
        crate::api::KafkaSaslMechanism::SCRAMSHA512 => "SCRAM-SHA-512",
    };
    client_config
        .set("sasl.mechanism", mechanism)
        .set("sasl.username", &connector.username)
        .set("sasl.password", &connector.password);
    Ok(())
}

pub trait Partitioner<T>: Send + Sync {
    fn partition(&self, value: &T, num_partitions: i32) -> HandlerResult<i32>;
}

pub struct SinkMessage<R, E>
where
    R: Send + Sync + 'static,
    E: Send + Sync + 'static,
{
    pub key: Option<Vec<u8>>,
    pub value: Vec<u8>,
    topic: String,
    partition: PartitionFunction,
    context: MessageContext,
    collect: Arc<dyn Fn(MessageContext, R) -> BoxFuture<()> + Send + Sync>,
    send: SendFunction,
    spawn: SpawnFunction,
    _error: std::marker::PhantomData<fn(E)>,
}

impl<R, E> SinkMessage<R, E>
where
    R: Send + Sync + 'static,
    E: Send + Sync + 'static,
{
    pub fn topic(&self) -> &str {
        &self.topic
    }

    pub fn send<F>(&self, on_delivery: F)
    where
        F: FnOnce(i32, i64, Option<HandlerError>) -> R + Send + 'static,
    {
        let partition = match (self.partition)() {
            Ok(partition) => partition,
            Err(error) => {
                let context = self.context.clone();
                let collect = Arc::clone(&self.collect);
                (self.spawn)(Box::pin(async move {
                    collect(context, on_delivery(0, 0, Some(error))).await;
                }));
                return;
            }
        };
        let topic = self.topic.clone();
        let key = self.key.clone();
        let value = self.value.clone();
        let context = self.context.clone();
        let collect = Arc::clone(&self.collect);
        let send = Arc::clone(&self.send);
        (self.spawn)(Box::pin(async move {
            let metadata = context.transport_metadata();
            let (partition, offset, error) =
                match send(topic, key, value, partition, metadata).await {
                    Ok((partition, offset)) => (partition, offset, None),
                    Err(error) => (0, 0, Some(error)),
                };
            collect(context, on_delivery(partition, offset, error)).await;
        }));
    }

    pub async fn send_sync(&self) -> HandlerResult<(i32, i64)> {
        let partition = (self.partition)()?;
        (self.send)(
            self.topic.clone(),
            self.key.clone(),
            self.value.clone(),
            partition,
            self.context.transport_metadata(),
        )
        .await
    }

    pub async fn out(&self, result: R) {
        (self.collect)(self.context.clone(), result).await;
    }

    pub async fn skip(&self, result: R) {
        self.out(result).await;
    }
}

#[async_trait]
pub trait EndpointHandler<HandlerState, T, R, E>: Send + Sync
where
    HandlerState: Send + 'static,
    T: Send + Sync + 'static,
    R: Send + Sync + 'static,
    E: Send + Sync + 'static,
{
    fn get_stream_id(&self, context: &MessageContext, value: &T) -> String;

    async fn begin_request(
        &self,
        context: MessageContext,
        stream: StreamContext<T, R, E>,
    ) -> (MessageContext, HandlerState);

    async fn consume_message(
        &self,
        context: MessageContext,
        stream: StreamContext<T, R, E>,
        handler_state: &mut HandlerState,
        value: Payload<T>,
        message: &mut SinkMessage<R, E>,
    ) -> HandlerResult;

    async fn end_request(
        &self,
        context: MessageContext,
        stream: StreamContext<T, R, E>,
        result: &HandlerResult,
        handler_state: HandlerState,
    );
}

pub struct RdkafkaKafkaEndpointConsumer<HandlerState, T, R, E, H>
where
    HandlerState: Send + 'static,
    T: Send + Sync + 'static,
    R: Send + Sync + 'static,
    E: Send + Sync + 'static,
    H: EndpointHandler<HandlerState, T, R, E> + 'static,
{
    stream: Weak<SinkStreamWithResult<T, R, E>>,
    stream_context: StreamContext<T, R, E>,
    endpoint_name: String,
    runtime_state: Arc<KafkaEndpointRuntimeState>,
    topic: String,
    handler: Arc<H>,
    partitioner: Option<Arc<dyn Partitioner<T>>>,
    send: SendFunction,
    spawn: SpawnFunction,
    messages_total: crate::runtime::environment::metrics::Int64Counter,
    request_errors: crate::runtime::environment::metrics::Int64Counter,
    active_requests: crate::runtime::environment::metrics::Int64Gauge,
    request_duration: crate::runtime::environment::metrics::Float64Histogram,
    _state: std::marker::PhantomData<fn(HandlerState)>,
}

pub fn make_endpoint_consumer<HandlerState, T, R, E, H, F, Fut>(
    stream: &Arc<SinkStreamWithResult<T, R, E>>,
    endpoint_config: KafkaEndpointConfig,
    data_connector_config: KafkaDataConnectorConfig,
    send: F,
    partitioner: Option<Arc<dyn Partitioner<T>>>,
    handler: H,
) -> RuntimeResult<Arc<RdkafkaKafkaEndpointConsumer<HandlerState, T, R, E, H>>>
where
    HandlerState: Send + 'static,
    T: Send + Sync + 'static,
    R: Send + Sync + 'static,
    E: Send + Sync + 'static,
    H: EndpointHandler<HandlerState, T, R, E> + 'static,
    F: Fn(String, Option<Vec<u8>>, Vec<u8>, Option<i32>) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = HandlerResult<(i32, i64)>> + Send + 'static,
{
    let runtime_state = KafkaEndpointRuntimeState::new();
    runtime_state
        .enabled
        .store(endpoint_config.enabled, Ordering::Release);
    runtime_state
        .partitions
        .store(endpoint_config.partitions.max(1), Ordering::Release);
    let tasks = TaskTracker::new();
    make_endpoint_consumer_with_state(
        stream,
        endpoint_config,
        data_connector_config,
        runtime_state,
        move |topic, key, value, partition, _metadata| send(topic, key, value, partition),
        Arc::new(move |future| {
            tasks.spawn(future);
        }),
        partitioner,
        Arc::new(handler),
    )
}

fn make_endpoint_consumer_with_state<HandlerState, T, R, E, H, F, Fut>(
    stream: &Arc<SinkStreamWithResult<T, R, E>>,
    endpoint_config: KafkaEndpointConfig,
    data_connector_config: KafkaDataConnectorConfig,
    runtime_state: Arc<KafkaEndpointRuntimeState>,
    send: F,
    spawn: SpawnFunction,
    partitioner: Option<Arc<dyn Partitioner<T>>>,
    handler: Arc<H>,
) -> RuntimeResult<Arc<RdkafkaKafkaEndpointConsumer<HandlerState, T, R, E, H>>>
where
    HandlerState: Send + 'static,
    T: Send + Sync + 'static,
    R: Send + Sync + 'static,
    E: Send + Sync + 'static,
    H: EndpointHandler<HandlerState, T, R, E> + 'static,
    F: Fn(String, Option<Vec<u8>>, Vec<u8>, Option<i32>, HashMap<String, String>) -> Fut
        + Send
        + Sync
        + 'static,
    Fut: Future<Output = HandlerResult<(i32, i64)>> + Send + 'static,
{
    let labels = [
        ("connector".to_owned(), data_connector_config.name),
        ("endpoint".to_owned(), endpoint_config.name.clone()),
    ]
    .into_iter()
    .collect();
    let scope = stream
        .environment()
        .metrics()
        .scope("datasink_endpoint", labels);
    let consumer = Arc::new(RdkafkaKafkaEndpointConsumer {
        stream: Arc::downgrade(stream),
        stream_context: SinkStreamContext::new(Arc::downgrade(stream)),
        endpoint_name: endpoint_config.name,
        runtime_state,
        topic: endpoint_config.topic,
        handler,
        partitioner,
        send: Arc::new(move |topic, key, value, partition, metadata| {
            Box::pin(send(topic, key, value, partition, metadata))
        }),
        spawn,
        messages_total: scope.counter(
            "messages_total",
            "Total number of successfully processed messages in data sink endpoint",
            Labels::new(),
        )?,
        request_errors: scope.counter(
            "events_total",
            "Total number of events in data sink endpoint",
            [("event".to_owned(), "request_error".to_owned())]
                .into_iter()
                .collect(),
        )?,
        active_requests: scope.gauge(
            "active_requests",
            "Number of active requests in data sink endpoint",
            Labels::new(),
        )?,
        request_duration: scope.histogram(
            "request_duration_seconds",
            "Request duration in seconds for data sink endpoint",
            Labels::new(),
            None,
        )?,
        _state: std::marker::PhantomData,
    });
    stream.set_sink_consumer(consumer.clone())?;
    Ok(consumer)
}

pub fn make_rdkafka_kafka_endpoint_consumer<HandlerState, T, R, E, H>(
    stream: &Arc<SinkStreamWithResult<T, R, E>>,
    data_sink: Arc<RdkafkaKafkaDataSink>,
    partitioner: Option<Arc<dyn Partitioner<T>>>,
    handler: H,
) -> RuntimeResult<Option<Arc<RdkafkaKafkaEndpointConsumer<HandlerState, T, R, E, H>>>>
where
    HandlerState: Send + 'static,
    T: Send + Sync + 'static,
    R: Send + Sync + 'static,
    E: Send + Sync + 'static,
    H: EndpointHandler<HandlerState, T, R, E> + 'static,
{
    let (endpoint_config, data_connector_config) = kafka_sink_configs(stream)?;
    let runtime_state = KafkaEndpointRuntimeState::new();
    data_sink.add_endpoint(&endpoint_config, Arc::clone(&runtime_state))?;
    let tasks = data_sink.tasks.clone();
    make_endpoint_consumer_with_state(
        stream,
        endpoint_config,
        data_connector_config,
        runtime_state,
        move |topic, key, value, partition, metadata| {
            let data_sink = Arc::clone(&data_sink);
            async move { data_sink.send(topic, key, value, partition, metadata).await }
        },
        Arc::new(move |future| {
            tasks.spawn(future);
        }),
        partitioner,
        Arc::new(handler),
    )
    .map(Some)
}

pub fn make_rdkafka_kafka_endpoint_consumer_with_partitioner<HandlerState, T, R, E, H>(
    stream: &Arc<SinkStreamWithResult<T, R, E>>,
    data_sink: Arc<RdkafkaKafkaDataSink>,
    handler: H,
) -> RuntimeResult<Option<Arc<RdkafkaKafkaEndpointConsumer<HandlerState, T, R, E, H>>>>
where
    HandlerState: Send + 'static,
    T: Send + Sync + 'static,
    R: Send + Sync + 'static,
    E: Send + Sync + 'static,
    H: EndpointHandler<HandlerState, T, R, E> + Partitioner<T> + 'static,
{
    let (endpoint_config, data_connector_config) = kafka_sink_configs(stream)?;
    let runtime_state = KafkaEndpointRuntimeState::new();
    data_sink.add_endpoint(&endpoint_config, Arc::clone(&runtime_state))?;
    let tasks = data_sink.tasks.clone();
    let handler = Arc::new(handler);
    let partitioner: Arc<dyn Partitioner<T>> = handler.clone();
    make_endpoint_consumer_with_state(
        stream,
        endpoint_config,
        data_connector_config,
        runtime_state,
        move |topic, key, value, partition, metadata| {
            let data_sink = Arc::clone(&data_sink);
            async move { data_sink.send(topic, key, value, partition, metadata).await }
        },
        Arc::new(move |future| {
            tasks.spawn(future);
        }),
        Some(partitioner),
        handler,
    )
    .map(Some)
}

fn kafka_sink_configs<T, R, E>(
    stream: &Arc<SinkStreamWithResult<T, R, E>>,
) -> RuntimeResult<(KafkaEndpointConfig, KafkaDataConnectorConfig)>
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
    let RuntimeEndpointConfig::Kafka(endpoint) = endpoint.as_ref() else {
        return Err(RuntimeError::InvalidConfiguration(format!(
            "endpoint {:?} referenced by sink stream {:?} is not a Kafka endpoint",
            endpoint.name(),
            stream.name()
        )));
    };
    let connector = runtime
        .data_connector_by_id(endpoint.id_data_connector)
        .ok_or_else(|| {
            RuntimeError::InvalidConfiguration(format!(
                "data connector {} referenced by endpoint {:?} is not configured",
                endpoint.id_data_connector, endpoint.name
            ))
        })?;
    let RuntimeDataConnectorConfig::Kafka(connector) = connector.as_ref() else {
        return Err(RuntimeError::InvalidConfiguration(format!(
            "endpoint {:?} does not reference a Kafka data connector",
            endpoint.name
        )));
    };
    Ok((endpoint.clone(), connector.clone()))
}

#[async_trait]
impl<HandlerState, T, R, E, H> Consumer<T>
    for RdkafkaKafkaEndpointConsumer<HandlerState, T, R, E, H>
where
    HandlerState: Send + 'static,
    T: Send + Sync + 'static,
    R: Send + Sync + 'static,
    E: Send + Sync + 'static,
    H: EndpointHandler<HandlerState, T, R, E> + 'static,
{
    async fn consume(&self, context: MessageContext, value: Payload<T>) {
        if !self.runtime_state.enabled.load(Ordering::Acquire) {
            return;
        }
        let Some(stream) = self.stream.upgrade() else {
            return;
        };
        let span = if context.sampling_enabled() {
            let span = tracing::info_span!(
                "kafka.output",
                stream = stream.name(),
                endpoint = self.endpoint_name.as_str(),
                stream_id = tracing::field::Empty,
                error = tracing::field::Empty,
                otel.status_code = tracing::field::Empty,
                otel.status_message = tracing::field::Empty,
            );
            let _ = span.set_parent(context.open_telemetry_context().clone());
            span
        } else {
            tracing::Span::none()
        };
        let stream_id = span.in_scope(|| self.handler.get_stream_id(&context, &value));
        span.record("stream_id", stream_id.as_str());
        let context = context.with_stream_id(stream_id).with_span_context(&span);
        let (context, mut handler_state) = crate::runtime::common::instrument_if_enabled(
            self.handler
                .begin_request(context, self.stream_context.clone()),
            span.clone(),
        )
        .await;
        tracing::event!(name: "begin_request", parent: &span, tracing::Level::INFO, {});

        self.active_requests.inc();
        let started_at = self.request_duration.is_enabled().then(Instant::now);
        let value = value.into_arc();
        let collect_context = self.stream_context.clone();
        let collect = Arc::new(move |context, result| {
            let stream_context = collect_context.clone();
            Box::pin(async move { stream_context.collect(context, result).await }) as BoxFuture<()>
        });
        let partitioner = self.partitioner.clone();
        let partition_value = Arc::clone(&value);
        let runtime_state = Arc::clone(&self.runtime_state);
        let partition: PartitionFunction = Arc::new(move || {
            let partitions = runtime_state.partitions.load(Ordering::Acquire).max(1);
            match partitioner.as_ref() {
                Some(partitioner) => partitioner
                    .partition(partition_value.as_ref(), partitions)
                    .map(Some),
                None => Ok(None),
            }
        });
        let mut message = SinkMessage {
            key: None,
            value: Vec::new(),
            topic: self.topic.clone(),
            partition,
            context: context.clone(),
            collect,
            send: Arc::clone(&self.send),
            spawn: Arc::clone(&self.spawn),
            _error: std::marker::PhantomData,
        };
        let result = crate::runtime::common::instrument_if_enabled(
            self.handler.consume_message(
                context.clone(),
                self.stream_context.clone(),
                &mut handler_state,
                Payload::from_arc(value),
                &mut message,
            ),
            span.clone(),
        )
        .await;
        match &result {
            Ok(()) => {
                tracing::event!(name: "consume_message", parent: &span, tracing::Level::INFO, {})
            }
            Err(error) => {
                crate::runtime::telemetry::record_span_error(&span, error);
                tracing::event!(name: "consume_message.error", parent: &span, tracing::Level::ERROR,
                    error = %error,
                    "Kafka sink handler failed"
                );
            }
        }
        crate::runtime::common::instrument_if_enabled(
            self.handler
                .end_request(context, self.stream_context.clone(), &result, handler_state),
            span.clone(),
        )
        .await;
        self.active_requests.dec();
        if let Some(started_at) = started_at {
            self.request_duration
                .observe(started_at.elapsed().as_secs_f64());
        }
        if result.is_ok() {
            self.messages_total.inc();
        } else {
            self.request_errors.inc();
        }
        drop(stream);
    }
}
