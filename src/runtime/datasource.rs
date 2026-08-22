use std::{
    collections::{HashMap, hash_map::RandomState},
    hash::BuildHasher,
    sync::{Arc, Mutex},
    time::Instant,
};

const PENDING_REQUEST_SHARDS: usize = 64;

struct PendingRequestTimestamps {
    shards: Box<[Mutex<HashMap<String, Instant>>]>,
    hash_builder: RandomState,
}

impl PendingRequestTimestamps {
    fn new() -> Self {
        Self {
            shards: (0..PENDING_REQUEST_SHARDS)
                .map(|_| Mutex::new(HashMap::new()))
                .collect(),
            hash_builder: RandomState::new(),
        }
    }

    fn shard(&self, stream_id: &str) -> &Mutex<HashMap<String, Instant>> {
        let hash = self.hash_builder.hash_one(stream_id);
        &self.shards[hash as usize % self.shards.len()]
    }

    fn insert(&self, stream_id: &str) {
        self.shard(stream_id)
            .lock()
            .expect("pending request timestamps shard poisoned")
            .insert(stream_id.to_owned(), Instant::now());
    }

    fn remove(&self, stream_id: &str) {
        self.shard(stream_id)
            .lock()
            .expect("pending request timestamps shard poisoned")
            .remove(stream_id);
    }

    fn oldest_age(&self) -> f64 {
        self.shards
            .iter()
            .filter_map(|shard| {
                shard
                    .lock()
                    .expect("pending request timestamps shard poisoned")
                    .values()
                    .min()
                    .copied()
            })
            .min()
            .map_or(0.0, |started| started.elapsed().as_secs_f64())
    }
}

use async_trait::async_trait;
use tokio::sync::mpsc;

use crate::{
    operators::InputStream,
    runtime::{
        common::{Consumer, MessageContext, Payload},
        environment::{
            Lifecycle, RuntimeResult,
            metrics::{Float64Histogram, Int64Counter, Int64Gauge, Labels, Metrics, MetricsScope},
        },
        stream::Stream,
    },
};

pub(crate) struct PendingRequests {
    enabled: bool,
    gauge: Int64Gauge,
    started: Arc<PendingRequestTimestamps>,
}

impl PendingRequests {
    pub(crate) fn new(scope: &MetricsScope) -> RuntimeResult<Self> {
        let gauge = scope.gauge(
            "pending_requests",
            "Number of requests awaiting a pipeline result",
            Labels::new(),
        )?;
        let started = Arc::new(PendingRequestTimestamps::new());
        let observable = Arc::clone(&started);
        scope.observable_float64_gauge(
            "pending_oldest_age_seconds",
            "Age in seconds of the oldest pending request awaiting a pipeline result",
            Labels::new(),
            Arc::new(move || observable.oldest_age()),
        )?;
        Ok(Self {
            enabled: gauge.is_enabled(),
            gauge,
            started,
        })
    }

    pub(crate) fn add(&self, stream_id: &str) {
        if !self.enabled {
            return;
        }
        self.gauge.inc();
        self.started.insert(stream_id);
    }

    pub(crate) fn remove(&self, stream_id: &str) {
        if !self.enabled {
            return;
        }
        self.gauge.dec();
        self.started.remove(stream_id);
    }
}

/// Transport-independent datasource endpoint telemetry.
///
/// OpenAPI adapters and native ServiceLib transports use the same metric
/// contract. Protocol-specific information belongs to transport metrics and
/// must not change the framework labels defined by Go.
pub struct DataSourceEndpointMetrics {
    messages_total: Int64Counter,
    request_errors: Int64Counter,
    active_requests: Int64Gauge,
    pending_requests: PendingRequests,
    request_duration: Float64Histogram,
}

impl DataSourceEndpointMetrics {
    pub fn from_input<T, R, E>(input: &InputStream<T, R, E>) -> RuntimeResult<Self>
    where
        T: Send + Sync + 'static,
        R: Send + Sync + 'static,
        E: Send + Sync + 'static,
    {
        Self::new(
            input.stream().environment().metrics(),
            &input.connector_name()?,
            &input.endpoint_name()?,
        )
    }

    pub fn new(
        metrics: &Metrics,
        connector_name: &str,
        endpoint_name: &str,
    ) -> RuntimeResult<Self> {
        let labels = [
            ("connector".to_owned(), connector_name.to_owned()),
            ("endpoint".to_owned(), endpoint_name.to_owned()),
        ]
        .into_iter()
        .collect();
        let scope = metrics.scope("datasource_endpoint", labels);
        Ok(Self {
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
        })
    }

    pub fn request_start(&self) -> Instant {
        self.active_requests.inc();
        Instant::now()
    }

    pub fn pending_add(&self, stream_id: &str) {
        self.pending_requests.add(stream_id);
    }

    pub fn pending_remove(&self, stream_id: &str) {
        self.pending_requests.remove(stream_id);
    }

    pub fn request_end(&self, started_at: Instant, success: bool) {
        self.active_requests.dec();
        self.request_duration
            .observe(started_at.elapsed().as_secs_f64());
        if success {
            self.messages_total.inc();
        } else {
            self.request_errors.inc();
        }
    }
}

pub trait DataSource: Lifecycle {
    fn id(&self) -> i32;
    fn name(&self) -> &str;
}

/// Typed source-side stream handles passed to endpoint handlers.
///
/// This is the Rust equivalent of Go's runtime.StreamContext. Transport
/// packages add only their request-specific data around it.
pub struct StreamContext<T, R, E>
where
    T: Send + Sync + 'static,
    R: Send + Sync + 'static,
    E: Send + Sync + 'static,
{
    input_stream: InputStream<T, R, E>,
}

impl<T, R, E> Clone for StreamContext<T, R, E>
where
    T: Send + Sync + 'static,
    R: Send + Sync + 'static,
    E: Send + Sync + 'static,
{
    fn clone(&self) -> Self {
        Self {
            input_stream: self.input_stream.clone(),
        }
    }
}

impl<T, R, E> StreamContext<T, R, E>
where
    T: Send + Sync + 'static,
    R: Send + Sync + 'static,
    E: Send + Sync + 'static,
{
    pub fn new(input_stream: InputStream<T, R, E>) -> Self {
        Self { input_stream }
    }

    pub fn stream(&self) -> &Stream<T> {
        self.input_stream.stream()
    }

    pub fn result_stream(&self) -> Option<Stream<R>> {
        self.input_stream.result_stream()
    }

    pub async fn collect(&self, context: MessageContext, value: T) {
        self.input_stream.consume(context, value).await;
    }

    pub async fn error_collect(&self, context: MessageContext, value: E) {
        self.input_stream
            .error_stream()
            .emit(context, Payload::new(value))
            .await;
    }
}

/// Correlates pipeline results with an endpoint request. The endpoint owns the
/// returned receiver and explicitly removes it when the request completes,
/// matching Go's pending-result lifecycle.
pub struct ResultRouter<T> {
    pending: Box<[Mutex<HashMap<String, mpsc::UnboundedSender<Payload<T>>>>]>,
    hash_builder: RandomState,
    message_id: Arc<dyn Fn(&T) -> String + Send + Sync>,
}

impl<T> ResultRouter<T> {
    pub fn new(message_id: impl Fn(&T) -> String + Send + Sync + 'static) -> Arc<Self> {
        Arc::new(Self {
            pending: (0..PENDING_REQUEST_SHARDS)
                .map(|_| Mutex::new(HashMap::new()))
                .collect(),
            hash_builder: RandomState::new(),
            message_id: Arc::new(message_id),
        })
    }

    fn shard(
        &self,
        message_id: &str,
    ) -> &Mutex<HashMap<String, mpsc::UnboundedSender<Payload<T>>>> {
        let hash = self.hash_builder.hash_one(message_id);
        &self.pending[hash as usize % self.pending.len()]
    }

    pub fn begin(&self, message_id: impl Into<String>) -> mpsc::UnboundedReceiver<Payload<T>> {
        let (sender, receiver) = mpsc::unbounded_channel();
        let message_id = message_id.into();
        let replaced = self
            .shard(&message_id)
            .lock()
            .expect("result router lock poisoned")
            .insert(message_id, sender);
        assert!(replaced.is_none(), "duplicate pending message ID");
        receiver
    }

    pub fn done(&self, message_id: &str) {
        self.shard(message_id)
            .lock()
            .expect("result router lock poisoned")
            .remove(message_id);
    }
}

#[async_trait]
impl<T> Consumer<T> for ResultRouter<T>
where
    T: Send + Sync + 'static,
{
    async fn consume(&self, _context: MessageContext, payload: Payload<T>) {
        let message_id = (self.message_id)(&payload);
        let sender = self
            .shard(&message_id)
            .lock()
            .expect("result router lock poisoned")
            .get(&message_id)
            .cloned();
        if let Some(sender) = sender {
            let _ = sender.send(payload);
        }
    }
}
