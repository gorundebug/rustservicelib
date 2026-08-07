use std::{
    collections::{BTreeMap, HashMap, HashSet},
    error::Error,
    future::Future,
    pin::Pin,
    sync::{
        Arc, Mutex, Weak,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};

use async_trait::async_trait;
use rdkafka::{
    ClientConfig, Message,
    admin::{AdminClient, AdminOptions, NewTopic, TopicReplication},
    client::DefaultClientContext,
    consumer::{CommitMode, Consumer as KafkaConsumer, StreamConsumer},
    types::RDKafkaErrorCode,
};
use tokio::{
    sync::{Mutex as AsyncMutex, Notify, RwLock, mpsc},
    task::JoinSet,
};
use tokio_util::sync::CancellationToken;
use tracing::Instrument;
use tracing_opentelemetry::OpenTelemetrySpanExt;

use crate::{
    operators::InputStream,
    runtime::{
        common::{Consumer, MessageContext, Payload, RuntimeEndpointConsumer, new_stream_id},
        config::{KafkaDataConnectorConfig, KafkaEndpointConfig},
        datasource::{DataSource, PendingRequests, StreamContext as DataSourceStreamContext},
        environment::{
            Lifecycle, RuntimeError, RuntimeResult,
            metrics::{Float64Histogram, Int64Counter, Int64Gauge, Labels},
        },
        store::RotatingMap,
    },
};

const PENDING_ROTATION_INTERVAL: Duration = Duration::from_secs(30);

pub const IMPLEMENTATION: &str = "rust/rdkafka";

pub type HandlerError = Box<dyn Error + Send + Sync>;
pub type HandlerResult<T = ()> = Result<T, HandlerError>;
pub type BoxFuture<T> = Pin<Box<dyn Future<Output = T> + Send>>;
pub type StreamContext<T, R, E> = DataSourceStreamContext<T, R, E>;

type CommitFunction = Arc<dyn Fn() -> BoxFuture<HandlerResult> + Send + Sync>;
type MarkMessageFunction = Arc<dyn Fn(String) + Send + Sync>;

#[derive(Clone)]
pub struct ConsumerMessage {
    pub key: Option<Vec<u8>>,
    pub value: Vec<u8>,
    pub topic: String,
    pub partition: i32,
    pub offset: i64,
    commit: CommitFunction,
    mark_message: MarkMessageFunction,
}

impl ConsumerMessage {
    pub fn new<F, Fut, M>(
        key: Option<Vec<u8>>,
        value: Vec<u8>,
        topic: impl Into<String>,
        partition: i32,
        offset: i64,
        commit: F,
        mark_message: M,
    ) -> Self
    where
        F: Fn() -> Fut + Send + Sync + 'static,
        Fut: Future<Output = HandlerResult> + Send + 'static,
        M: Fn(String) + Send + Sync + 'static,
    {
        Self {
            key,
            value,
            topic: topic.into(),
            partition,
            offset,
            commit: Arc::new(move || Box::pin(commit())),
            mark_message: Arc::new(mark_message),
        }
    }

    pub async fn commit(&self) -> HandlerResult {
        (self.commit)().await
    }

    pub fn mark_message(&self, metadata: impl Into<String>) {
        (self.mark_message)(metadata.into());
    }
}

pub type ResultCallback<HandlerState, T, R, E> = Arc<
    dyn Fn(
            MessageContext,
            StreamContext<T, R, E>,
            Arc<AsyncMutex<HandlerState>>,
            Payload<R>,
        ) -> BoxFuture<bool>
        + Send
        + Sync,
>;

pub struct ResultContext<HandlerState, T, R, E>
where
    HandlerState: Send + 'static,
    T: Send + Sync + 'static,
    R: Send + Sync + 'static,
    E: Send + Sync + 'static,
{
    callbacks: Mutex<HashMap<String, ResultCallback<HandlerState, T, R, E>>>,
    done: CancellationToken,
    span: tracing::Span,
}

impl<HandlerState, T, R, E> ResultContext<HandlerState, T, R, E>
where
    HandlerState: Send + 'static,
    T: Send + Sync + 'static,
    R: Send + Sync + 'static,
    E: Send + Sync + 'static,
{
    fn new(span: tracing::Span) -> Self {
        Self {
            callbacks: Mutex::new(HashMap::new()),
            done: CancellationToken::new(),
            span,
        }
    }

    pub fn set_result_callback(
        &self,
        message_id: impl Into<String>,
        callback: ResultCallback<HandlerState, T, R, E>,
    ) {
        self.callbacks
            .lock()
            .expect("Kafka result callbacks lock poisoned")
            .insert(message_id.into(), callback);
    }

    pub fn done(&self) {
        tracing::info!(parent: &self.span, event.name = "done_called");
        self.done.cancel();
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
    fn concurrency(&self, stream: &StreamContext<T, R, E>) -> usize;

    async fn begin_request(
        &self,
        context: MessageContext,
        stream: StreamContext<T, R, E>,
    ) -> Result<(MessageContext, HandlerState), HandlerError>;

    async fn consume_message(
        &self,
        context: MessageContext,
        stream: StreamContext<T, R, E>,
        handler_state: Arc<AsyncMutex<HandlerState>>,
        message: ConsumerMessage,
        result_context: Arc<ResultContext<HandlerState, T, R, E>>,
    ) -> HandlerResult;

    async fn get_message_id(
        &self,
        context: &MessageContext,
        stream: &StreamContext<T, R, E>,
        handler_state: Arc<AsyncMutex<HandlerState>>,
        value: &R,
    ) -> String;

    async fn end_request(
        &self,
        context: MessageContext,
        stream: StreamContext<T, R, E>,
        result: &HandlerResult,
        handler_state: Arc<AsyncMutex<HandlerState>>,
    );
}

struct KafkaResult<HandlerState, T, R, E>
where
    HandlerState: Send + 'static,
    T: Send + Sync + 'static,
    R: Send + Sync + 'static,
    E: Send + Sync + 'static,
{
    handler_state: Arc<AsyncMutex<HandlerState>>,
    result_context: Arc<ResultContext<HandlerState, T, R, E>>,
    lifetime: RwLock<()>,
    started_at: Instant,
    span: tracing::Span,
}

struct Concurrency {
    active: AtomicUsize,
    stopped: AtomicBool,
    changed: Notify,
}

struct ActiveRequestGuard<'a>(&'a Concurrency);

impl Drop for ActiveRequestGuard<'_> {
    fn drop(&mut self) {
        self.0.active.fetch_sub(1, Ordering::AcqRel);
        self.0.changed.notify_waiters();
    }
}

struct EndpointMetrics {
    messages_total: Int64Counter,
    request_errors: Int64Counter,
    begin_request_failed: Int64Counter,
    missing_stream_id: Int64Counter,
    late_result: Int64Counter,
    unknown_message_id: Int64Counter,
    duplicate_message_id: Int64Counter,
    active_requests: Int64Gauge,
    pending_requests: PendingRequests,
    request_duration: Float64Histogram,
}

pub struct RdkafkaKafkaTypedEndpointConsumer<HandlerState, T, R, E, H>
where
    HandlerState: Send + 'static,
    T: Send + Sync + 'static,
    R: Send + Sync + 'static,
    E: Send + Sync + 'static,
    H: EndpointHandler<HandlerState, T, R, E> + 'static,
{
    input_stream: InputStream<T, R, E>,
    stream_context: StreamContext<T, R, E>,
    endpoint_name: String,
    handler: Arc<H>,
    pending: RotatingMap<String, Arc<KafkaResult<HandlerState, T, R, E>>>,
    concurrency: Concurrency,
    metrics: EndpointMetrics,
}

#[async_trait]
trait KafkaInputEndpoint: Lifecycle {
    fn config(&self) -> &KafkaEndpointConfig;
}

pub struct RdkafkaKafkaDataSource {
    config: KafkaDataConnectorConfig,
    admin: AdminClient<DefaultClientContext>,
    endpoints: Mutex<Vec<Arc<dyn KafkaInputEndpoint>>>,
    state: Mutex<u8>,
}

impl RdkafkaKafkaDataSource {
    pub fn new(config: KafkaDataConnectorConfig) -> HandlerResult<Arc<Self>> {
        if config.brokers.trim().is_empty() {
            return Err(format!(
                "no brokers specified for Kafka data connector {:?}",
                config.name
            )
            .into());
        }
        let admin = make_client_config(&config).create::<AdminClient<DefaultClientContext>>()?;
        Ok(Arc::new(Self {
            config,
            admin,
            endpoints: Mutex::new(Vec::new()),
            state: Mutex::new(0),
        }))
    }

    pub fn add_endpoint<HandlerState, T, R, E, H>(
        self: &Arc<Self>,
        input_stream: InputStream<T, R, E>,
        endpoint_config: KafkaEndpointConfig,
        handler: H,
    ) -> RuntimeResult<Arc<RdkafkaKafkaTypedEndpointConsumer<HandlerState, T, R, E, H>>>
    where
        HandlerState: Send + 'static,
        T: Send + Sync + 'static,
        R: Send + Sync + 'static,
        E: Send + Sync + 'static,
        H: EndpointHandler<HandlerState, T, R, E> + 'static,
    {
        if endpoint_config.id_data_connector != self.config.id {
            return Err(RuntimeError::InvalidConfiguration(format!(
                "Kafka endpoint {:?} references connector {}, expected {}",
                endpoint_config.name, endpoint_config.id_data_connector, self.config.id
            )));
        }
        let state = self
            .state
            .lock()
            .expect("Kafka datasource state lock poisoned");
        if *state != 0 {
            return Err(RuntimeError::ResourceAlreadyStarted(
                self.config.name.clone(),
            ));
        }
        let mut endpoints = self
            .endpoints
            .lock()
            .expect("Kafka datasource endpoints lock poisoned");
        if endpoints
            .iter()
            .any(|current| current.config().id == endpoint_config.id)
        {
            return Err(RuntimeError::DuplicateResource(endpoint_config.name));
        }
        let endpoint_consumer = make_rdkafka_kafka_endpoint_consumer(
            input_stream,
            endpoint_config.clone(),
            self.config.clone(),
            handler,
        )?;
        let endpoint = RdkafkaKafkaEndpoint::new(
            &self.config,
            endpoint_config,
            Arc::clone(&endpoint_consumer),
        )
        .map_err(|error| RuntimeError::Transport(error.to_string()))?;
        endpoints.push(endpoint);
        drop(state);
        Ok(endpoint_consumer)
    }

    async fn create_topics(&self, endpoints: &[Arc<dyn KafkaInputEndpoint>]) -> HandlerResult {
        let mut names = HashSet::new();
        let topics = endpoints
            .iter()
            .filter(|endpoint| {
                endpoint.config().create_topic && names.insert(endpoint.config().topic.clone())
            })
            .map(|endpoint| {
                let config = endpoint.config();
                NewTopic::new(
                    &config.topic,
                    config.partitions.max(1),
                    TopicReplication::Fixed(config.replication_factor.max(1)),
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
}

impl DataSource for RdkafkaKafkaDataSource {
    fn id(&self) -> i32 {
        self.config.id
    }

    fn name(&self) -> &str {
        &self.config.name
    }
}

#[async_trait]
impl Lifecycle for RdkafkaKafkaDataSource {
    async fn start(&self, context: MessageContext) -> RuntimeResult<()> {
        {
            let mut state = self
                .state
                .lock()
                .expect("Kafka datasource state lock poisoned");
            match *state {
                0 => *state = 1,
                1 => {
                    return Err(RuntimeError::ResourceAlreadyStarted(
                        self.config.name.clone(),
                    ));
                }
                _ => return Err(RuntimeError::ResourceStopped(self.config.name.clone())),
            }
        }
        let endpoints = self
            .endpoints
            .lock()
            .expect("Kafka datasource endpoints lock poisoned")
            .clone();
        if let Err(error) = self.create_topics(&endpoints).await {
            *self
                .state
                .lock()
                .expect("Kafka datasource state lock poisoned") = 0;
            return Err(RuntimeError::Transport(error.to_string()));
        }
        let mut started: Vec<Arc<dyn KafkaInputEndpoint>> = Vec::new();
        for endpoint in &endpoints {
            if let Err(error) = endpoint.start(context.clone()).await {
                for endpoint in started.into_iter().rev() {
                    let _ = endpoint.stop(context.clone()).await;
                }
                *self
                    .state
                    .lock()
                    .expect("Kafka datasource state lock poisoned") = 0;
                return Err(error);
            }
            started.push(Arc::clone(endpoint));
        }
        Ok(())
    }

    async fn stop(&self, context: MessageContext) -> RuntimeResult<()> {
        {
            let mut state = self
                .state
                .lock()
                .expect("Kafka datasource state lock poisoned");
            if *state == 2 {
                return Ok(());
            }
            *state = 2;
        }
        let endpoints = self
            .endpoints
            .lock()
            .expect("Kafka datasource endpoints lock poisoned")
            .clone();
        let mut first_error = None;
        for endpoint in endpoints.iter().rev() {
            if let Err(error) = endpoint.stop(context.clone()).await
                && first_error.is_none()
            {
                first_error = Some(error);
            }
        }
        match first_error {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }
}

fn make_client_config(config: &KafkaDataConnectorConfig) -> ClientConfig {
    let mut client_config = ClientConfig::new();
    client_config.set("bootstrap.servers", &config.brokers);
    if config.dial_timeout > 0.0 {
        client_config.set(
            "socket.timeout.ms",
            (config.dial_timeout.ceil() as u64).to_string(),
        );
    }
    client_config
}

struct RdkafkaKafkaEndpoint<HandlerState, T, R, E, H>
where
    HandlerState: Send + 'static,
    T: Send + Sync + 'static,
    R: Send + Sync + 'static,
    E: Send + Sync + 'static,
    H: EndpointHandler<HandlerState, T, R, E> + 'static,
{
    consumer: Arc<StreamConsumer>,
    config: KafkaEndpointConfig,
    endpoint_consumer: Arc<RdkafkaKafkaTypedEndpointConsumer<HandlerState, T, R, E, H>>,
    stop: CancellationToken,
    context: AsyncMutex<Option<MessageContext>>,
    task: AsyncMutex<Option<tokio::task::JoinHandle<()>>>,
}

impl<HandlerState, T, R, E, H> RdkafkaKafkaEndpoint<HandlerState, T, R, E, H>
where
    HandlerState: Send + 'static,
    T: Send + Sync + 'static,
    R: Send + Sync + 'static,
    E: Send + Sync + 'static,
    H: EndpointHandler<HandlerState, T, R, E> + 'static,
{
    fn new(
        data_connector_config: &KafkaDataConnectorConfig,
        endpoint_config: KafkaEndpointConfig,
        endpoint_consumer: Arc<RdkafkaKafkaTypedEndpointConsumer<HandlerState, T, R, E, H>>,
    ) -> HandlerResult<Arc<Self>> {
        if endpoint_config.topic.trim().is_empty() {
            return Err(format!(
                "no topic specified for Kafka endpoint {:?}",
                endpoint_config.name
            )
            .into());
        }
        if endpoint_config.consumer_group.trim().is_empty() {
            return Err(format!(
                "consumer group is not set for Kafka endpoint {:?}",
                endpoint_config.name
            )
            .into());
        }
        let mut config = make_client_config(data_connector_config);
        config
            .set("group.id", &endpoint_config.consumer_group)
            .set("auto.offset.reset", "earliest")
            .set("enable.auto.commit", "true")
            .set("enable.auto.offset.store", "false");
        let consumer = Arc::new(config.create::<StreamConsumer>()?);
        consumer.subscribe(&[&endpoint_config.topic])?;
        Ok(Arc::new(Self {
            consumer,
            config: endpoint_config,
            endpoint_consumer,
            stop: CancellationToken::new(),
            context: AsyncMutex::new(None),
            task: AsyncMutex::new(None),
        }))
    }
}

#[async_trait]
impl<HandlerState, T, R, E, H> Lifecycle for RdkafkaKafkaEndpoint<HandlerState, T, R, E, H>
where
    HandlerState: Send + 'static,
    T: Send + Sync + 'static,
    R: Send + Sync + 'static,
    E: Send + Sync + 'static,
    H: EndpointHandler<HandlerState, T, R, E> + 'static,
{
    async fn start(&self, context: MessageContext) -> RuntimeResult<()> {
        if self.stop.is_cancelled() {
            return Err(RuntimeError::ResourceStopped(self.config.name.clone()));
        }
        let mut task = self.task.lock().await;
        if task.is_some() {
            return Err(RuntimeError::ResourceAlreadyStarted(
                self.config.name.clone(),
            ));
        }
        self.endpoint_consumer.start();
        *self.context.lock().await = Some(context.clone());
        let consumer = Arc::clone(&self.consumer);
        let endpoint_consumer = Arc::clone(&self.endpoint_consumer);
        let stop = self.stop.clone();
        *task = Some(tokio::spawn(async move {
            let mut lanes = HashMap::new();
            let mut lane_tasks = JoinSet::new();
            loop {
                let message = tokio::select! {
                    _ = stop.cancelled() => break,
                    _ = context.cancelled() => break,
                    message = consumer.recv() => match message {
                        Ok(message) => message,
                        Err(_) => continue,
                    },
                };
                let topic = message.topic().to_owned();
                let partition = message.partition();
                let offset = message.offset();
                let key = message.key().map(ToOwned::to_owned);
                let value = message.payload().unwrap_or_default().to_vec();

                let commit_consumer = Arc::clone(&consumer);
                let commit_topic = topic.clone();
                let commit = move || {
                    let consumer = Arc::clone(&commit_consumer);
                    let topic = commit_topic.clone();
                    async move {
                        let mut offsets = rdkafka::TopicPartitionList::new();
                        offsets.add_partition_offset(
                            &topic,
                            partition,
                            rdkafka::Offset::Offset(offset + 1),
                        )?;
                        consumer.commit(&offsets, CommitMode::Sync)?;
                        Ok(())
                    }
                };
                let mark_consumer = Arc::clone(&consumer);
                let mark_topic = topic.clone();
                let mark_message = move |_metadata: String| {
                    let _ = mark_consumer.store_offset(&mark_topic, partition, offset + 1);
                };
                let lane_key = (topic.clone(), partition);
                let sender = lanes.entry(lane_key).or_insert_with(|| {
                    // Sarama invokes ConsumeClaim sequentially for each partition
                    // while different claims run concurrently. A dedicated lane
                    // preserves that same ordering with rdkafka's combined stream.
                    let (sender, mut receiver) = mpsc::channel(256);
                    let endpoint_consumer = Arc::clone(&endpoint_consumer);
                    let context = context.clone();
                    lane_tasks.spawn(async move {
                        while let Some(message) = receiver.recv().await {
                            endpoint_consumer
                                .endpoint_request(context.clone(), message)
                                .await;
                        }
                    });
                    sender
                });
                let message = ConsumerMessage::new(
                    key,
                    value,
                    topic,
                    partition,
                    offset,
                    commit,
                    mark_message,
                );
                tokio::select! {
                    result = sender.send(message) => {
                        if result.is_err() {
                            break;
                        }
                    }
                    _ = stop.cancelled() => break,
                    _ = context.cancelled() => break,
                }
            }
            lanes.clear();
            while lane_tasks.join_next().await.is_some() {}
        }));
        Ok(())
    }

    async fn stop(&self, context: MessageContext) -> RuntimeResult<()> {
        self.endpoint_consumer.stop();
        self.stop.cancel();
        if let Some(lifecycle_context) = self.context.lock().await.take() {
            lifecycle_context.cancel();
        }
        if let Some(task) = self.task.lock().await.take() {
            tokio::select! {
                _ = task => {}
                _ = context.cancelled() => {
                    return Err(RuntimeError::ContextCancelled);
                }
            }
        }
        Ok(())
    }
}

#[async_trait]
impl<HandlerState, T, R, E, H> KafkaInputEndpoint for RdkafkaKafkaEndpoint<HandlerState, T, R, E, H>
where
    HandlerState: Send + 'static,
    T: Send + Sync + 'static,
    R: Send + Sync + 'static,
    E: Send + Sync + 'static,
    H: EndpointHandler<HandlerState, T, R, E> + 'static,
{
    fn config(&self) -> &KafkaEndpointConfig {
        &self.config
    }
}

impl<HandlerState, T, R, E, H> RdkafkaKafkaTypedEndpointConsumer<HandlerState, T, R, E, H>
where
    HandlerState: Send + 'static,
    T: Send + Sync + 'static,
    R: Send + Sync + 'static,
    E: Send + Sync + 'static,
    H: EndpointHandler<HandlerState, T, R, E> + 'static,
{
    async fn acquire_concurrency(&self) -> bool {
        loop {
            if self.concurrency.stopped.load(Ordering::Acquire) {
                return false;
            }
            let limit = self.handler.concurrency(&self.stream_context);
            let active = self.concurrency.active.load(Ordering::Acquire);
            if limit == 0 || active < limit {
                if self
                    .concurrency
                    .active
                    .compare_exchange(active, active + 1, Ordering::AcqRel, Ordering::Acquire)
                    .is_ok()
                {
                    return true;
                }
                continue;
            }
            self.concurrency.changed.notified().await;
        }
    }

    pub fn start(&self) {
        self.concurrency.stopped.store(false, Ordering::Release);
        self.concurrency.changed.notify_waiters();
    }

    pub fn stop(&self) {
        self.concurrency.stopped.store(true, Ordering::Release);
        self.concurrency.changed.notify_waiters();
    }

    pub async fn endpoint_request(&self, context: MessageContext, message: ConsumerMessage) {
        if !self.acquire_concurrency().await {
            return;
        }
        let _active_request = ActiveRequestGuard(&self.concurrency);

        let span = if context.sampling_enabled() {
            let span = tracing::info_span!(
                "kafka.input",
                stream = self.input_stream.stream().name(),
                endpoint = self.endpoint_name.as_str(),
                stream_id = tracing::field::Empty,
                has_result = tracing::field::Empty,
                error = tracing::field::Empty,
                otel.status_code = tracing::field::Empty,
                otel.status_message = tracing::field::Empty,
            );
            let _ = span.set_parent(context.open_telemetry_context().clone());
            span
        } else {
            tracing::Span::none()
        };
        let context = context.with_open_telemetry_context(span.context());
        let (context, handler_state) = match self
            .handler
            .begin_request(context, self.stream_context.clone())
            .instrument(span.clone())
            .await
        {
            Ok(result) => {
                tracing::info!(parent: &span, event.name = "begin_request");
                result
            }
            Err(error) => {
                crate::runtime::telemetry::record_span_error(&span, &error);
                tracing::error!(
                    parent: &span,
                    event.name = "begin_request.error",
                    error = %error,
                    "Kafka source begin request failed"
                );
                self.metrics.begin_request_failed.inc();
                return;
            }
        };
        let context = if context.stream_id().is_some() {
            context
        } else {
            context.with_stream_id(new_stream_id())
        };
        let stream_id = context.stream_id().unwrap().to_owned();
        span.record("stream_id", stream_id.as_str());
        let handler_state = Arc::new(AsyncMutex::new(handler_state));
        let has_result = self.input_stream.result_stream().is_some();
        span.record("has_result", has_result);
        let result_context = Arc::new(ResultContext::new(span.clone()));
        let kafka_result = Arc::new(KafkaResult {
            handler_state: Arc::clone(&handler_state),
            result_context: Arc::clone(&result_context),
            lifetime: RwLock::new(()),
            started_at: Instant::now(),
            span: span.clone(),
        });

        self.metrics.active_requests.inc();
        if has_result {
            if let Err(error) = self
                .pending
                .set(stream_id.clone(), Arc::clone(&kafka_result))
            {
                let result: HandlerResult = Err(Box::new(error));
                crate::runtime::telemetry::record_span_error(
                    &span,
                    result.as_ref().expect_err("duplicate pending request"),
                );
                self.handler
                    .end_request(context, self.stream_context.clone(), &result, handler_state)
                    .instrument(span)
                    .await;
                self.metrics.active_requests.dec();
                self.metrics
                    .request_duration
                    .observe(kafka_result.started_at.elapsed().as_secs_f64());
                self.metrics.request_errors.inc();
                return;
            }
            self.metrics.pending_requests.add(&stream_id);
        }

        let mut result = self
            .handler
            .consume_message(
                context.clone(),
                self.stream_context.clone(),
                Arc::clone(&handler_state),
                message,
                Arc::clone(&result_context),
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
                    "Kafka source handler failed"
                );
            }
        }

        if result.is_ok() && has_result {
            tokio::select! {
                _ = result_context.done.cancelled() => {
                    tracing::info!(parent: &span, event.name = "done_received");
                }
                _ = context.cancelled() => {
                    result = Err("Kafka message context cancelled".into());
                    crate::runtime::telemetry::record_span_error(
                        &span,
                        "Kafka message context cancelled",
                    );
                    tracing::error!(
                        parent: &span,
                        event.name = "context_cancelled",
                        error = "Kafka message context cancelled"
                    );
                }
            }
        }

        let removed = if has_result {
            self.metrics.pending_requests.remove(&stream_id);
            self.pending.pop(&stream_id)
        } else {
            None
        };
        let _lifetime = match &removed {
            Some(result) => Some(result.lifetime.write().await),
            None => None,
        };
        self.handler
            .end_request(context, self.stream_context.clone(), &result, handler_state)
            .instrument(span.clone())
            .await;
        self.metrics.active_requests.dec();
        self.metrics
            .request_duration
            .observe(kafka_result.started_at.elapsed().as_secs_f64());
        if result.is_ok() {
            self.metrics.messages_total.inc();
        } else {
            self.metrics.request_errors.inc();
        }
    }

    async fn consume_result(&self, context: MessageContext, value: Payload<R>) {
        let Some(stream_id) = context.stream_id() else {
            self.metrics.missing_stream_id.inc();
            return;
        };
        let Some(result) = self.pending.get(stream_id) else {
            self.metrics.late_result.inc();
            return;
        };
        let _lifetime = result.lifetime.read().await;
        if !self
            .pending
            .get(stream_id)
            .is_some_and(|current| Arc::ptr_eq(&current, &result))
        {
            self.metrics.late_result.inc();
            tracing::warn!(parent: &result.span, event.name = "late_result");
            return;
        }
        let message_id = self
            .handler
            .get_message_id(
                &context,
                &self.stream_context,
                Arc::clone(&result.handler_state),
                &value,
            )
            .instrument(result.span.clone())
            .await;
        let callback = result
            .result_context
            .callbacks
            .lock()
            .expect("Kafka result callbacks lock poisoned")
            .get(&message_id)
            .cloned();
        let Some(callback) = callback else {
            self.metrics.unknown_message_id.inc();
            tracing::warn!(
                parent: &result.span,
                event.name = "unknown_message_id",
                message_id
            );
            return;
        };
        if callback(
            context,
            self.stream_context.clone(),
            Arc::clone(&result.handler_state),
            value,
        )
        .instrument(result.span.clone())
        .await
        {
            let removed = result
                .result_context
                .callbacks
                .lock()
                .expect("Kafka result callbacks lock poisoned")
                .remove(&message_id);
            if removed.is_none() {
                self.metrics.duplicate_message_id.inc();
                tracing::warn!(
                    parent: &result.span,
                    event.name = "duplicate_message_id",
                    message_id
                );
            }
        }
        tracing::info!(
            parent: &result.span,
            event.name = "result_consumed",
            message_id
        );
    }
}

pub fn make_rdkafka_kafka_endpoint_consumer<HandlerState, T, R, E, H>(
    input_stream: InputStream<T, R, E>,
    endpoint_config: KafkaEndpointConfig,
    data_connector_config: KafkaDataConnectorConfig,
    handler: H,
) -> RuntimeResult<Arc<RdkafkaKafkaTypedEndpointConsumer<HandlerState, T, R, E, H>>>
where
    HandlerState: Send + 'static,
    T: Send + Sync + 'static,
    R: Send + Sync + 'static,
    E: Send + Sync + 'static,
    H: EndpointHandler<HandlerState, T, R, E> + 'static,
{
    let labels = [
        ("connector".to_owned(), data_connector_config.name),
        ("endpoint".to_owned(), endpoint_config.name.clone()),
        ("protocol".to_owned(), "kafka".to_owned()),
    ]
    .into_iter()
    .collect::<BTreeMap<_, _>>();
    let scope = input_stream
        .stream()
        .environment()
        .metrics()
        .scope("datasource_endpoint", labels);
    let pending = RotatingMap::new(PENDING_ROTATION_INTERVAL);
    let endpoint_consumer = Arc::new(RdkafkaKafkaTypedEndpointConsumer {
        stream_context: StreamContext::new(input_stream.clone()),
        input_stream: input_stream.clone(),
        endpoint_name: endpoint_config.name,
        handler: Arc::new(handler),
        pending: pending.clone(),
        concurrency: Concurrency {
            active: AtomicUsize::new(0),
            stopped: AtomicBool::new(false),
            changed: Notify::new(),
        },
        metrics: EndpointMetrics {
            messages_total: scope.counter(
                "messages_total",
                "Total number of successfully processed messages in data source endpoint",
                Labels::new(),
            )?,
            request_errors: scope.counter(
                "events_total",
                "Total number of events in data source endpoint",
                [("event".to_owned(), "request_error".to_owned())]
                    .into_iter()
                    .collect(),
            )?,
            begin_request_failed: scope.counter(
                "events_total",
                "Total number of events in data source endpoint",
                [("event".to_owned(), "begin_request_failed".to_owned())]
                    .into_iter()
                    .collect(),
            )?,
            missing_stream_id: scope.counter(
                "events_total",
                "Total number of events in data source endpoint",
                [("event".to_owned(), "missing_stream_id".to_owned())]
                    .into_iter()
                    .collect(),
            )?,
            late_result: scope.counter(
                "events_total",
                "Total number of events in data source endpoint",
                [("event".to_owned(), "late_result".to_owned())]
                    .into_iter()
                    .collect(),
            )?,
            unknown_message_id: scope.counter(
                "events_total",
                "Total number of events in data source endpoint",
                [("event".to_owned(), "unknown_message_id".to_owned())]
                    .into_iter()
                    .collect(),
            )?,
            duplicate_message_id: scope.counter(
                "events_total",
                "Total number of events in data source endpoint",
                [("event".to_owned(), "duplicate_message_id".to_owned())]
                    .into_iter()
                    .collect(),
            )?,
            active_requests: scope.gauge(
                "active_requests",
                "Number of active requests in data source endpoint",
                Labels::new(),
            )?,
            pending_requests: PendingRequests::new(&scope)?,
            request_duration: scope.histogram(
                "request_duration_seconds",
                "Request duration in seconds for data source endpoint",
                Labels::new(),
                None,
            )?,
        },
    });
    if input_stream.result_stream().is_some() {
        input_stream.set_result_consumer(Arc::new(ResultConsumer {
            endpoint_consumer: Arc::downgrade(&endpoint_consumer),
        }));
    }
    if input_stream.result_stream().is_some() {
        input_stream
            .stream()
            .environment()
            .register_storage(Arc::new(pending));
    }
    input_stream
        .stream()
        .environment()
        .register_endpoint_consumer(endpoint_consumer.clone())?;
    Ok(endpoint_consumer)
}

impl<HandlerState, T, R, E, H> RuntimeEndpointConsumer
    for RdkafkaKafkaTypedEndpointConsumer<HandlerState, T, R, E, H>
where
    HandlerState: Send + 'static,
    T: Send + Sync + 'static,
    R: Send + Sync + 'static,
    E: Send + Sync + 'static,
    H: EndpointHandler<HandlerState, T, R, E> + 'static,
{
    fn id(&self) -> i32 {
        self.input_stream.endpoint_id()
    }

    fn function_implementation(&self) -> &'static str {
        std::any::type_name::<H>()
    }
}

struct ResultConsumer<HandlerState, T, R, E, H>
where
    HandlerState: Send + 'static,
    T: Send + Sync + 'static,
    R: Send + Sync + 'static,
    E: Send + Sync + 'static,
    H: EndpointHandler<HandlerState, T, R, E> + 'static,
{
    endpoint_consumer: Weak<RdkafkaKafkaTypedEndpointConsumer<HandlerState, T, R, E, H>>,
}

#[async_trait]
impl<HandlerState, T, R, E, H> Consumer<R> for ResultConsumer<HandlerState, T, R, E, H>
where
    HandlerState: Send + 'static,
    T: Send + Sync + 'static,
    R: Send + Sync + 'static,
    E: Send + Sync + 'static,
    H: EndpointHandler<HandlerState, T, R, E> + 'static,
{
    async fn consume(&self, context: MessageContext, value: Payload<R>) {
        if let Some(endpoint_consumer) = self.endpoint_consumer.upgrade() {
            endpoint_consumer.consume_result(context, value).await;
        }
    }
}
