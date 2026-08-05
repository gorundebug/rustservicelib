use std::{
    collections::HashMap,
    fmt,
    ops::Deref,
    sync::{
        Arc, Mutex as StdMutex, Weak,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use async_trait::async_trait;
use opentelemetry::{
    Context as OpenTelemetryContext, global,
    propagation::{Extractor, Injector},
    trace::TraceContextExt,
};
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;
use tracing_opentelemetry::OpenTelemetrySpanExt;

pub const STREAM_ID_HEADER: &str = "x-stream-id";
pub const TRACE_SAMPLING_HEADER: &str = "x-trace";

type CancellationCallback = Arc<dyn Fn() + Send + Sync + 'static>;

struct CancellationCallbacks {
    next_id: AtomicU64,
    callbacks: StdMutex<HashMap<u64, CancellationCallback>>,
}

impl fmt::Debug for CancellationCallbacks {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CancellationCallbacks")
            .finish_non_exhaustive()
    }
}

impl CancellationCallbacks {
    fn invoke(&self, id: u64) {
        let callback = self
            .callbacks
            .lock()
            .expect("message cancellation callback lock poisoned")
            .remove(&id);
        if let Some(callback) = callback {
            callback();
        }
    }

    fn invoke_all(&self) {
        let callbacks = {
            let mut callbacks = self
                .callbacks
                .lock()
                .expect("message cancellation callback lock poisoned");
            std::mem::take(&mut *callbacks)
        };
        for callback in callbacks.into_values() {
            callback();
        }
    }
}

pub(crate) struct CancellationCallbackRegistration {
    callbacks: Weak<CancellationCallbacks>,
    id: Option<u64>,
}

impl Drop for CancellationCallbackRegistration {
    fn drop(&mut self) {
        let (Some(callbacks), Some(id)) = (self.callbacks.upgrade(), self.id) else {
            return;
        };
        callbacks
            .callbacks
            .lock()
            .expect("message cancellation callback lock poisoned")
            .remove(&id);
    }
}

struct MetadataExtractor<'a>(&'a HashMap<String, String>);

impl Extractor for MetadataExtractor<'_> {
    fn get(&self, key: &str) -> Option<&str> {
        self.0.get(key).map(String::as_str)
    }

    fn keys(&self) -> Vec<&str> {
        self.0.keys().map(String::as_str).collect()
    }
}

struct MetadataInjector<'a>(&'a mut HashMap<String, String>);

impl Injector for MetadataInjector<'_> {
    fn set(&mut self, key: &str, value: String) {
        self.0.insert(key.to_owned(), value);
    }
}

#[derive(Debug)]
pub struct Payload<T>(Arc<T>);

impl<T> Clone for Payload<T> {
    fn clone(&self) -> Self {
        Self(Arc::clone(&self.0))
    }
}

impl<T> Payload<T> {
    pub fn new(value: T) -> Self {
        Self(Arc::new(value))
    }

    pub fn from_arc(value: Arc<T>) -> Self {
        Self(value)
    }

    pub fn into_arc(self) -> Arc<T> {
        self.0
    }
}

impl<T> Deref for Payload<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[derive(Clone, Debug)]
pub struct MessageContext {
    cancellation: CancellationToken,
    cancellation_callbacks: Arc<CancellationCallbacks>,
    deadline: Option<Instant>,
    metadata: Arc<HashMap<String, String>>,
    open_telemetry: OpenTelemetryContext,
    sampling_enabled: bool,
    priority: Option<i32>,
}

impl Default for MessageContext {
    fn default() -> Self {
        Self::new()
    }
}

impl MessageContext {
    pub fn new() -> Self {
        Self {
            cancellation: CancellationToken::new(),
            cancellation_callbacks: Arc::new(CancellationCallbacks {
                next_id: AtomicU64::new(0),
                callbacks: StdMutex::new(HashMap::new()),
            }),
            deadline: None,
            metadata: Arc::new(HashMap::new()),
            open_telemetry: OpenTelemetryContext::new(),
            sampling_enabled: false,
            priority: None,
        }
    }

    pub fn with_deadline(deadline: Instant) -> Self {
        Self {
            deadline: Some(deadline),
            ..Self::new()
        }
    }

    pub fn with_timeout(timeout: Duration) -> Self {
        Self::with_deadline(Instant::now() + timeout)
    }

    pub fn with_metadata(mut self, metadata: HashMap<String, String>) -> Self {
        self.open_telemetry = global::get_text_map_propagator(|propagator| {
            propagator.extract(&MetadataExtractor(&metadata))
        });
        self.sampling_enabled = metadata
            .get(TRACE_SAMPLING_HEADER)
            .is_some_and(|value| !value.is_empty())
            || self.open_telemetry.span().span_context().is_sampled();
        self.metadata = Arc::new(metadata);
        self
    }

    pub fn with_stream_id(mut self, stream_id: impl Into<String>) -> Self {
        let mut metadata = self.metadata().clone();
        metadata.insert(STREAM_ID_HEADER.to_owned(), stream_id.into());
        self.metadata = Arc::new(metadata);
        self
    }

    pub fn stream_id(&self) -> Option<&str> {
        self.metadata.get(STREAM_ID_HEADER).map(String::as_str)
    }

    pub fn metadata(&self) -> &HashMap<String, String> {
        &self.metadata
    }

    pub fn deadline(&self) -> Option<Instant> {
        self.deadline
    }

    pub fn remaining(&self) -> Option<Duration> {
        self.deadline
            .map(|deadline| deadline.saturating_duration_since(Instant::now()))
    }

    pub fn with_open_telemetry_context(mut self, context: OpenTelemetryContext) -> Self {
        self.open_telemetry = context;
        self
    }

    pub fn open_telemetry_context(&self) -> &OpenTelemetryContext {
        &self.open_telemetry
    }

    pub fn enable_sampling(mut self) -> Self {
        self.sampling_enabled = true;
        let mut metadata = self.metadata().clone();
        metadata.insert(TRACE_SAMPLING_HEADER.to_owned(), "1".to_owned());
        self.metadata = Arc::new(metadata);
        self
    }

    pub fn sampling_enabled(&self) -> bool {
        self.sampling_enabled
    }

    /// Overrides the configured priority for the next priority-pool edge.
    /// Like Go's PriorityFromContext, this value is process-local and is not
    /// serialized across transport boundaries.
    pub fn with_priority(mut self, priority: i32) -> Self {
        self.priority = Some(priority);
        self
    }

    pub fn priority(&self) -> Option<i32> {
        self.priority
    }

    /// Returns transport metadata with the current OpenTelemetry propagation
    /// fields injected. Only explicitly supported framework metadata is
    /// transferred; arbitrary process-local context values are not serialized.
    pub fn transport_metadata(&self) -> HashMap<String, String> {
        let mut metadata = HashMap::new();
        for name in [STREAM_ID_HEADER, TRACE_SAMPLING_HEADER] {
            if let Some(value) = self.metadata.get(name) {
                metadata.insert(name.to_owned(), value.clone());
            }
        }
        global::get_text_map_propagator(|propagator| {
            propagator.inject_context(&self.open_telemetry, &mut MetadataInjector(&mut metadata));
        });
        metadata
    }

    pub fn from_tonic_request<T>(request: &tonic::Request<T>) -> Self {
        let metadata: HashMap<String, String> = request
            .metadata()
            .iter()
            .filter_map(|entry| match entry {
                tonic::metadata::KeyAndValueRef::Ascii(key, value) => value
                    .to_str()
                    .ok()
                    .map(|value| (key.as_str().to_owned(), value.to_owned())),
                tonic::metadata::KeyAndValueRef::Binary(_, _) => None,
            })
            .collect();
        let timeout = metadata.get("grpc-timeout").and_then(|value| {
            let (number, unit) = value.split_at(value.len().checked_sub(1)?);
            let number = number.parse::<u64>().ok()?;
            match unit {
                "H" => Some(Duration::from_secs(number.saturating_mul(60 * 60))),
                "M" => Some(Duration::from_secs(number.saturating_mul(60))),
                "S" => Some(Duration::from_secs(number)),
                "m" => Some(Duration::from_millis(number)),
                "u" => Some(Duration::from_micros(number)),
                "n" => Some(Duration::from_nanos(number)),
                _ => None,
            }
        });
        let context = Self::new().with_metadata(metadata);
        if let Some(timeout) = timeout {
            Self {
                deadline: Some(Instant::now() + timeout),
                ..context
            }
        } else {
            context
        }
    }

    pub fn apply_to_tonic_request<T>(&self, request: &mut tonic::Request<T>) {
        for (name, value) in self.transport_metadata() {
            let Ok(key) =
                tonic::metadata::MetadataKey::<tonic::metadata::Ascii>::from_bytes(name.as_bytes())
            else {
                continue;
            };
            let Ok(value) = tonic::metadata::MetadataValue::try_from(value.as_str()) else {
                continue;
            };
            request.metadata_mut().insert(key, value);
        }
        if let Some(remaining) = self.remaining() {
            request.set_timeout(remaining);
        }
    }

    pub fn cancel(&self) {
        self.cancellation.cancel();
        self.cancellation_callbacks.invoke_all();
    }

    pub(crate) fn register_cancellation_callback(
        &self,
        callback: impl Fn() + Send + Sync + 'static,
    ) -> CancellationCallbackRegistration {
        let callback: CancellationCallback = Arc::new(callback);
        if self.is_cancelled() {
            callback();
            return CancellationCallbackRegistration {
                callbacks: Weak::new(),
                id: None,
            };
        }
        let id = self
            .cancellation_callbacks
            .next_id
            .fetch_add(1, Ordering::Relaxed);
        {
            let mut callbacks = self
                .cancellation_callbacks
                .callbacks
                .lock()
                .expect("message cancellation callback lock poisoned");
            if self.is_cancelled() {
                drop(callbacks);
                callback();
                return CancellationCallbackRegistration {
                    callbacks: Weak::new(),
                    id: None,
                };
            }
            callbacks.insert(id, callback);
        }
        if let Some(deadline) = self.deadline {
            let callbacks = Arc::clone(&self.cancellation_callbacks);
            tokio::spawn(async move {
                tokio::time::sleep_until(deadline).await;
                callbacks.invoke(id);
            });
        }
        CancellationCallbackRegistration {
            callbacks: Arc::downgrade(&self.cancellation_callbacks),
            id: Some(id),
        }
    }

    pub fn is_cancelled(&self) -> bool {
        self.cancellation.is_cancelled()
            || self
                .deadline
                .is_some_and(|deadline| deadline <= Instant::now())
    }

    pub async fn cancelled(&self) {
        match self.deadline {
            Some(deadline) => {
                tokio::select! {
                    _ = self.cancellation.cancelled() => {}
                    _ = tokio::time::sleep_until(deadline) => {}
                }
            }
            None => self.cancellation.cancelled().await,
        }
    }
}

pub fn new_stream_id() -> String {
    static SEQUENCE: AtomicU64 = AtomicU64::new(0);
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let sequence = SEQUENCE.fetch_add(1, Ordering::Relaxed);
    format!("{timestamp:x}-{sequence:x}")
}

#[async_trait]
pub trait Consumer<T>: Send + Sync
where
    T: Send + Sync + 'static,
{
    async fn consume(&self, context: MessageContext, payload: Payload<T>);
}

pub trait RuntimeStream: Send + Sync {
    fn id(&self) -> i32;
    fn name(&self) -> String;
    fn environment(&self) -> &crate::runtime::environment::RuntimeEnvironment;

    fn start_span(
        &self,
        context: MessageContext,
        operation: &'static str,
    ) -> (MessageContext, tracing::Span) {
        if !context.sampling_enabled() {
            return (context, tracing::Span::none());
        }
        let span = tracing::info_span!(
            "stream.operation",
            otel.name = operation,
            stream = self.name(),
            error = tracing::field::Empty,
            otel.status_code = tracing::field::Empty,
            otel.status_message = tracing::field::Empty,
        );
        let _ = span.set_parent(context.open_telemetry_context().clone());
        let child = span.context();
        (context.with_open_telemetry_context(child), span)
    }
}

/// Type-erased source endpoint owned by the service runtime.
///
/// Concrete transports retain their typed API. This interface corresponds to
/// Go's `RuntimeEndpointConsumer` and lets `ServiceApp` own every configured
/// endpoint independently of the transport connector.
pub trait RuntimeEndpointConsumer: Send + Sync {
    fn id(&self) -> i32;
    fn function_implementation(&self) -> &'static str;
}
