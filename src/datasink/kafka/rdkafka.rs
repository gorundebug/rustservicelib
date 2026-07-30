use std::{
    collections::HashSet,
    error::Error,
    future::Future,
    pin::Pin,
    sync::{
        Arc, Mutex, Weak,
        atomic::{AtomicU8, Ordering},
    },
    time::Instant,
};

use async_trait::async_trait;
use rdkafka::{
    ClientConfig,
    admin::{AdminClient, AdminOptions, NewTopic, TopicReplication},
    client::DefaultClientContext,
    producer::{FutureProducer, FutureRecord, Producer},
    types::RDKafkaErrorCode,
    util::Timeout,
};
use tracing::Instrument;
use tracing_opentelemetry::OpenTelemetrySpanExt;

use crate::{
    operators::SinkStreamWithResult,
    runtime::{
        common::{Consumer, MessageContext, Payload, RuntimeStream},
        config::{KafkaDataConnectorConfig, KafkaEndpointConfig},
        datasink::{DataSink, SinkStreamContext},
        environment::{Lifecycle, RuntimeError, RuntimeResult, metrics::Labels},
    },
};

pub const IMPLEMENTATION: &str = "rust/rdkafka";

pub type HandlerError = Box<dyn Error + Send + Sync>;
pub type HandlerResult<T = ()> = Result<T, HandlerError>;
pub type BoxFuture<T> = Pin<Box<dyn Future<Output = T> + Send>>;
pub type StreamContext<T, R, E> = SinkStreamContext<T, R, E>;

type SendFunction = Arc<
    dyn Fn(String, Option<Vec<u8>>, Vec<u8>, Option<i32>) -> BoxFuture<HandlerResult<(i32, i64)>>
        + Send
        + Sync,
>;

pub struct RdkafkaKafkaDataSink {
    config: KafkaDataConnectorConfig,
    admin: AdminClient<DefaultClientContext>,
    producer: FutureProducer,
    endpoints: Mutex<Vec<KafkaEndpointConfig>>,
    state: AtomicU8,
}

impl RdkafkaKafkaDataSink {
    pub fn new(config: &KafkaDataConnectorConfig) -> HandlerResult<Arc<Self>> {
        if config.brokers.trim().is_empty() {
            return Err(format!(
                "no brokers specified for Kafka data connector {:?}",
                config.name
            )
            .into());
        }
        let mut client_config = ClientConfig::new();
        client_config.set("bootstrap.servers", &config.brokers);
        if config.dial_timeout > 0.0 {
            client_config.set(
                "socket.timeout.ms",
                (config.dial_timeout.ceil() as u64).to_string(),
            );
        }
        let admin = client_config.create::<AdminClient<DefaultClientContext>>()?;
        let producer = client_config.create::<FutureProducer>()?;
        Ok(Arc::new(Self {
            config: config.clone(),
            admin,
            producer,
            endpoints: Mutex::new(Vec::new()),
            state: AtomicU8::new(0),
        }))
    }

    fn add_endpoint(&self, endpoint: KafkaEndpointConfig) -> RuntimeResult<()> {
        if endpoint.id_data_connector != self.config.id {
            return Err(RuntimeError::InvalidConfiguration(format!(
                "Kafka endpoint {:?} references connector {}, expected {}",
                endpoint.name, endpoint.id_data_connector, self.config.id
            )));
        }
        if self.state.load(Ordering::Acquire) != 0 {
            return Err(RuntimeError::ResourceAlreadyStarted(
                self.config.name.clone(),
            ));
        }
        let mut endpoints = self
            .endpoints
            .lock()
            .expect("Kafka data sink endpoints lock poisoned");
        if self.state.load(Ordering::Acquire) != 0 {
            return Err(RuntimeError::ResourceAlreadyStarted(
                self.config.name.clone(),
            ));
        }
        if endpoints.iter().any(|current| current.id == endpoint.id) {
            return Err(RuntimeError::DuplicateResource(endpoint.name));
        }
        endpoints.push(endpoint);
        Ok(())
    }

    async fn create_topics(&self) -> HandlerResult {
        let endpoints = self
            .endpoints
            .lock()
            .expect("Kafka data sink endpoints lock poisoned")
            .clone();
        let mut names = HashSet::new();
        let topics = endpoints
            .iter()
            .filter(|endpoint| endpoint.create_topic && names.insert(endpoint.topic.clone()))
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
        for result in self
            .admin
            .create_topics(&topics, &AdminOptions::new())
            .await?
        {
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
        self.producer
            .send(record, Timeout::Never)
            .await
            .map(|delivery| (delivery.partition, delivery.offset))
            .map_err(|(error, _)| -> HandlerError { Box::new(error) })
    }
}

impl DataSink for RdkafkaKafkaDataSink {
    fn id(&self) -> i32 {
        self.config.id
    }

    fn name(&self) -> &str {
        &self.config.name
    }
}

#[async_trait]
impl Lifecycle for RdkafkaKafkaDataSink {
    async fn start(&self, _context: MessageContext) -> RuntimeResult<()> {
        self.state
            .compare_exchange(0, 1, Ordering::AcqRel, Ordering::Acquire)
            .map_err(|state| {
                if state == 3 {
                    RuntimeError::ResourceStopped(self.config.name.clone())
                } else {
                    RuntimeError::ResourceAlreadyStarted(self.config.name.clone())
                }
            })?;
        if let Err(error) = self.create_topics().await {
            self.state.store(0, Ordering::Release);
            return Err(RuntimeError::Transport(error.to_string()));
        }
        self.state.store(2, Ordering::Release);
        Ok(())
    }

    async fn stop(&self, context: MessageContext) -> RuntimeResult<()> {
        let previous = self.state.swap(3, Ordering::AcqRel);
        if previous == 3 {
            return Ok(());
        }
        loop {
            match self
                .producer
                .flush(Timeout::After(std::time::Duration::from_millis(100)))
            {
                Ok(()) => return Ok(()),
                Err(_) if context.is_cancelled() => return Err(RuntimeError::ContextCancelled),
                Err(_) => {}
            }
        }
    }
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
    partition: Option<i32>,
    partition_error: Mutex<Option<HandlerError>>,
    context: MessageContext,
    collect: Arc<dyn Fn(MessageContext, R) -> BoxFuture<()> + Send + Sync>,
    send: SendFunction,
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
        if let Some(error) = self
            .partition_error
            .lock()
            .expect("Kafka partition error lock poisoned")
            .take()
        {
            let context = self.context.clone();
            let collect = Arc::clone(&self.collect);
            tokio::spawn(async move {
                collect(context, on_delivery(0, 0, Some(error))).await;
            });
            return;
        }
        let topic = self.topic.clone();
        let key = self.key.clone();
        let value = self.value.clone();
        let partition = self.partition;
        let context = self.context.clone();
        let collect = Arc::clone(&self.collect);
        let send = Arc::clone(&self.send);
        tokio::spawn(async move {
            let (partition, offset, error) = match send(topic, key, value, partition).await {
                Ok((partition, offset)) => (partition, offset, None),
                Err(error) => (0, 0, Some(error)),
            };
            collect(context, on_delivery(partition, offset, error)).await;
        });
    }

    pub async fn send_sync(&self) -> HandlerResult<(i32, i64)> {
        if let Some(error) = self
            .partition_error
            .lock()
            .expect("Kafka partition error lock poisoned")
            .take()
        {
            return Err(error);
        }
        (self.send)(
            self.topic.clone(),
            self.key.clone(),
            self.value.clone(),
            self.partition,
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
    endpoint_config: KafkaEndpointConfig,
    handler: H,
    partitioner: Option<Arc<dyn Partitioner<T>>>,
    send: SendFunction,
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
    let labels = [
        ("connector".to_owned(), data_connector_config.name),
        ("endpoint".to_owned(), endpoint_config.name.clone()),
        ("protocol".to_owned(), "kafka".to_owned()),
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
        endpoint_config,
        handler,
        partitioner,
        send: Arc::new(move |topic, key, value, partition| {
            Box::pin(send(topic, key, value, partition))
        }),
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
    endpoint_config: KafkaEndpointConfig,
    data_connector_config: KafkaDataConnectorConfig,
    data_sink: Arc<RdkafkaKafkaDataSink>,
    partitioner: Option<Arc<dyn Partitioner<T>>>,
    handler: H,
) -> RuntimeResult<Arc<RdkafkaKafkaEndpointConsumer<HandlerState, T, R, E, H>>>
where
    HandlerState: Send + 'static,
    T: Send + Sync + 'static,
    R: Send + Sync + 'static,
    E: Send + Sync + 'static,
    H: EndpointHandler<HandlerState, T, R, E> + 'static,
{
    data_sink.add_endpoint(endpoint_config.clone())?;
    make_endpoint_consumer(
        stream,
        endpoint_config,
        data_connector_config,
        move |topic, key, value, partition| {
            let data_sink = Arc::clone(&data_sink);
            async move { data_sink.send(topic, key, value, partition).await }
        },
        partitioner,
        handler,
    )
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
        let Some(stream) = self.stream.upgrade() else {
            return;
        };
        let span = if context.sampling_enabled() {
            let span = tracing::info_span!(
                "kafka.output",
                stream = stream.name(),
                endpoint = self.endpoint_config.name.as_str(),
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
        let context = context
            .with_stream_id(stream_id)
            .with_open_telemetry_context(span.context());
        let (context, mut handler_state) = self
            .handler
            .begin_request(context, self.stream_context.clone())
            .instrument(span.clone())
            .await;
        tracing::info!(parent: &span, event.name = "begin_request");

        self.active_requests.inc();
        let started_at = Instant::now();
        let collect_context = self.stream_context.clone();
        let collect = Arc::new(move |context, result| {
            let stream_context = collect_context.clone();
            Box::pin(async move { stream_context.collect(context, result).await }) as BoxFuture<()>
        });
        let (partition, partition_error) = span.in_scope(|| match self.partitioner.as_ref() {
            Some(partitioner) => {
                let partitions = self.endpoint_config.partitions.max(1);
                match partitioner.partition(&value, partitions) {
                    Ok(partition) => (Some(partition), None),
                    Err(error) => (None, Some(error)),
                }
            }
            None => (None, None),
        });
        let mut message = SinkMessage {
            key: None,
            value: Vec::new(),
            topic: self.endpoint_config.topic.clone(),
            partition,
            partition_error: Mutex::new(partition_error),
            context: context.clone(),
            collect,
            send: Arc::clone(&self.send),
            _error: std::marker::PhantomData,
        };
        let result = self
            .handler
            .consume_message(
                context.clone(),
                self.stream_context.clone(),
                &mut handler_state,
                value,
                &mut message,
            )
            .instrument(span.clone())
            .await;
        match &result {
            Ok(()) => tracing::info!(parent: &span, event.name = "consume_message"),
            Err(error) => {
                crate::runtime::telemetry::record_span_error(&span, error);
                tracing::error!(
                    parent: &span,
                    event.name = "consume_message.error",
                    error = %error,
                    "Kafka sink handler failed"
                );
            }
        }
        self.handler
            .end_request(context, self.stream_context.clone(), &result, handler_state)
            .instrument(span.clone())
            .await;
        self.active_requests.dec();
        self.request_duration
            .observe(started_at.elapsed().as_secs_f64());
        if result.is_ok() {
            self.messages_total.inc();
        } else {
            self.request_errors.inc();
        }
        drop(stream);
    }
}
