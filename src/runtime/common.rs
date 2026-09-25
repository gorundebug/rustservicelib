use std::{
    any::Any,
    cell::UnsafeCell,
    collections::HashMap,
    future::Future,
    marker::PhantomData,
    ops::Deref,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use async_trait::async_trait;
use axum::http::HeaderMap;
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

fn traceparent_flags(value: &str) -> Option<u8> {
    let bytes = value.as_bytes();
    if bytes.len() != 55 || bytes[2] != b'-' || bytes[35] != b'-' || bytes[52] != b'-' {
        return None;
    }
    if !bytes
        .iter()
        .enumerate()
        .all(|(index, value)| matches!(index, 2 | 35 | 52) || value.is_ascii_hexdigit())
    {
        return None;
    }
    if &value[..2] == "ff"
        || value[3..35].bytes().all(|value| value == b'0')
        || value[36..52].bytes().all(|value| value == b'0')
    {
        return None;
    }
    u8::from_str_radix(&value[53..55], 16).ok()
}

fn traceparent_is_sampled(value: &str) -> bool {
    traceparent_flags(value).is_some_and(|flags| flags & 1 != 0)
}

/// Await directly when tracing is disabled. This deliberately is a macro, not
/// an `async fn`: the no-tracing branch must not add another Future/poll layer
/// to every operator and transport call on the hot path.
///
/// Links without tracing do not construct even a disabled span.
macro_rules! instrument_if_present {
    ($future:expr, $span:ident $(,)?) => {{
        let future = $future;
        match &$span {
            Some(span) if !span.is_disabled() => {
                ::tracing::Instrument::instrument(future, ::tracing::Span::clone(span)).await
            }
            _ => future.await,
        }
    }};
    ($future:expr, $span:expr $(,)?) => {{
        let future = $future;
        match $span {
            Some(span) if !span.is_disabled() => {
                ::tracing::Instrument::instrument(future, span).await
            }
            _ => future.await,
        }
    }};
}

pub(crate) use instrument_if_present;

/// Trace-only scopes must not evaluate their closure or fields for a disabled span.
macro_rules! event_if_enabled {
    ($span:expr, $event:expr $(,)?) => {{
        let span: &::tracing::Span = $span;
        if !span.is_disabled() {
            let _: () = span.in_scope($event);
        }
    }};
}

pub(crate) use event_if_enabled;

macro_rules! event_if_present {
    ($span:expr, $event:expr $(,)?) => {{
        if let Some(span) = $span {
            if !span.is_disabled() {
                let _: () = span.in_scope($event);
            }
        }
    }};
}

pub(crate) use event_if_present;

/// Business callbacks still execute when tracing is disabled, without a span scope.
macro_rules! scope_if_present {
    ($span:expr, $callback:expr $(,)?) => {{
        let callback = $callback;
        match $span {
            Some(span) if !span.is_disabled() => span.in_scope(callback),
            _ => callback(),
        }
    }};
}

pub(crate) use scope_if_present;

#[cfg(test)]
#[path = "span_fast_path_contract_tests.rs"]
mod span_fast_path_contract_tests;

/// A value wired while a service graph is constructed and read-only after
/// `ServiceApp::start`. Unlike `Mutex`, `RwLock`, or `OnceLock`, reads are a
/// plain pointer dereference with no lock or atomic operation.
///
/// The runtime constructs graphs on one thread before publishing them to
/// workers. Keeping this lifecycle rule here mirrors Go's plain downstream
/// fields while containing the required Rust interior mutability in one place.
pub(crate) struct ConstructionCell<T> {
    value: UnsafeCell<Option<T>>,
}

// SAFETY: mutation is restricted to the single-threaded graph construction
// phase. Once the graph is published, only shared immutable reads are allowed.
unsafe impl<T: Send + Sync> Sync for ConstructionCell<T> {}

impl<T> ConstructionCell<T> {
    pub(crate) const fn empty() -> Self {
        Self {
            value: UnsafeCell::new(None),
        }
    }

    pub(crate) fn get(&self) -> Option<&T> {
        // SAFETY: values are immutable after graph construction.
        unsafe { (&*self.value.get()).as_ref() }
    }

    pub(crate) fn set(&self, value: T) -> Result<(), T> {
        // SAFETY: all setters run during single-threaded graph construction.
        let slot = unsafe { &mut *self.value.get() };
        if slot.is_some() {
            return Err(value);
        }
        *slot = Some(value);
        Ok(())
    }

    pub(crate) fn replace(&self, value: T) -> Option<T> {
        // SAFETY: graph wiring, including endpoint rebinding, is completed by
        // one thread before the graph is published to runtime workers.
        unsafe { (&mut *self.value.get()).replace(value) }
    }
}

/// A field that always has a value and may only be mutated while the graph is
/// being built. Runtime reads compile to a plain field access: there is no
/// state flag, lock, atomic operation, or initialization check.
pub(crate) struct ConstructionValue<T> {
    value: UnsafeCell<T>,
}

// SAFETY: the graph builder is single-threaded and finishes all writes before
// the graph is published to runtime workers.
unsafe impl<T: Send + Sync> Sync for ConstructionValue<T> {}

impl<T> ConstructionValue<T> {
    pub(crate) const fn new(value: T) -> Self {
        Self {
            value: UnsafeCell::new(value),
        }
    }

    #[inline]
    pub(crate) fn get(&self) -> &T {
        // SAFETY: the value is immutable after graph construction.
        unsafe { &*self.value.get() }
    }

    pub(crate) fn with_mut<R>(&self, function: impl FnOnce(&mut T) -> R) -> R {
        // SAFETY: all mutations run during single-threaded graph construction.
        function(unsafe { &mut *self.value.get() })
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

struct HeaderExtractor<'a>(&'a HeaderMap);

impl Extractor for HeaderExtractor<'_> {
    fn get(&self, key: &str) -> Option<&str> {
        self.0.get(key)?.to_str().ok()
    }

    fn keys(&self) -> Vec<&str> {
        self.0.keys().map(axum::http::HeaderName::as_str).collect()
    }
}

struct MetadataInjector<'a>(&'a mut HashMap<String, String>);

impl Injector for MetadataInjector<'_> {
    fn set(&mut self, key: &str, value: String) {
        self.0.insert(key.to_owned(), value);
    }
}

#[derive(Debug)]
pub enum Payload<T> {
    Owned(T),
    Shared(Arc<T>),
}

impl<T> Payload<T> {
    pub fn new(value: T) -> Self {
        Self::Owned(value)
    }

    pub fn from_arc(value: Arc<T>) -> Self {
        Self::Shared(value)
    }

    pub fn into_arc(self) -> Arc<T> {
        match self {
            Self::Owned(value) => Arc::new(value),
            Self::Shared(value) => value,
        }
    }

    /// Produces two payload handles while allocating only when an owned value
    /// actually needs to fan out. Linear stream chains keep the value inline.
    pub fn share(self) -> (Self, Self) {
        let value = self.into_arc();
        (Self::Shared(Arc::clone(&value)), Self::Shared(value))
    }

    pub fn into_value(self) -> T
    where
        T: Clone,
    {
        match self {
            Self::Owned(value) => value,
            Self::Shared(value) => Arc::try_unwrap(value).unwrap_or_else(|value| (*value).clone()),
        }
    }
}

impl<T> Deref for Payload<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        match self {
            Self::Owned(value) => value,
            Self::Shared(value) => value,
        }
    }
}

/// The generic key keeps type erasure private to process-local context storage.
pub(crate) struct ContextKey<T> {
    identity: Arc<()>,
    _value: PhantomData<fn() -> T>,
}

impl<T> ContextKey<T> {
    pub(crate) fn new() -> Self {
        Self {
            identity: Arc::new(()),
            _value: PhantomData,
        }
    }
}

impl<T> Clone for ContextKey<T> {
    fn clone(&self) -> Self {
        Self {
            identity: Arc::clone(&self.identity),
            _value: PhantomData,
        }
    }
}

struct LocalContextValue {
    key: Arc<()>,
    value: Arc<dyn Any + Send + Sync>,
    parent: Option<Arc<LocalContextValue>>,
}

impl std::fmt::Debug for LocalContextValue {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("LocalContextValue")
            .finish_non_exhaustive()
    }
}

#[derive(Clone, Debug)]
pub struct MessageContext {
    cancellation: CancellationToken,
    deadline: Option<Instant>,
    metadata: Arc<HashMap<String, String>>,
    open_telemetry: OpenTelemetryContext,
    sampling_enabled: bool,
    priority: Option<i32>,
    local_values: Option<Arc<LocalContextValue>>,
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
            deadline: None,
            metadata: Arc::new(HashMap::new()),
            open_telemetry: OpenTelemetryContext::new(),
            sampling_enabled: false,
            priority: None,
            local_values: None,
        }
    }

    /// Creates a cancellable child for a concurrent operation group. Parent
    /// cancellation reaches the child, while cancelling the child never
    /// cancels its parent (the same direction as Go context.WithCancel).
    pub fn child(&self) -> Self {
        Self {
            cancellation: self.cancellation.child_token(),
            deadline: self.deadline,
            metadata: Arc::clone(&self.metadata),
            open_telemetry: self.open_telemetry.clone(),
            sampling_enabled: self.sampling_enabled,
            priority: self.priority,
            local_values: self.local_values.clone(),
        }
    }

    pub fn with_deadline(deadline: Instant) -> Self {
        Self {
            deadline: Some(deadline),
            ..Self::new()
        }
    }

    pub(crate) fn with_local_value<T: Send + Sync + 'static>(
        mut self,
        key: &ContextKey<T>,
        value: Arc<T>,
    ) -> Self {
        self.local_values = Some(Arc::new(LocalContextValue {
            key: Arc::clone(&key.identity),
            value,
            parent: self.local_values.take(),
        }));
        self
    }

    pub(crate) fn local_value<T: Send + Sync + 'static>(
        &self,
        key: &ContextKey<T>,
    ) -> Option<Arc<T>> {
        let mut current = self.local_values.as_deref();
        while let Some(binding) = current {
            if Arc::ptr_eq(&binding.key, &key.identity) {
                return Arc::clone(&binding.value).downcast::<T>().ok();
            }
            current = binding.parent.as_deref();
        }
        None
    }

    pub fn with_timeout(timeout: Duration) -> Self {
        Self::with_deadline(Instant::now() + timeout)
    }

    pub fn with_timeout_limit(mut self, timeout: Duration) -> Self {
        let deadline = Instant::now() + timeout;
        if self.deadline.is_none_or(|current| deadline < current) {
            self.deadline = Some(deadline);
        }
        self
    }

    pub fn with_metadata(mut self, metadata: HashMap<String, String>) -> Self {
        let explicit_sampling = metadata
            .get(TRACE_SAMPLING_HEADER)
            .is_some_and(|value| !value.is_empty());
        let sampled_parent = metadata
            .get("traceparent")
            .is_some_and(|value| traceparent_is_sampled(value));
        let valid_parent = metadata
            .get("traceparent")
            .is_some_and(|value| traceparent_flags(value).is_some());
        self.sampling_enabled = explicit_sampling || sampled_parent;
        if self.sampling_enabled || valid_parent {
            self.open_telemetry = global::get_text_map_propagator(|propagator| {
                propagator.extract(&MetadataExtractor(&metadata))
            });
        }
        self.metadata = Arc::new(metadata);
        self
    }

    /// Retain transport metadata without extracting a trace when the service
    /// has no tracing engine. Business handlers can still read the headers.
    pub(crate) fn with_metadata_untraced(mut self, metadata: HashMap<String, String>) -> Self {
        self.metadata = Arc::new(metadata);
        self
    }

    /// Builds the transport tracing state without copying every HTTP header
    /// into framework metadata. HTTP datasource endpoints still preserve all
    /// request headers in their own `MessageContext`; this constructor is for
    /// transport middleware that only needs propagation and sampling state.
    pub(crate) fn tracing_parent_from_http_headers(
        headers: &HeaderMap,
    ) -> Option<OpenTelemetryContext> {
        let explicit_sampling = headers
            .get(TRACE_SAMPLING_HEADER)
            .is_some_and(|value| !value.is_empty());
        let sampled_parent = headers
            .get("traceparent")
            .and_then(|value| value.to_str().ok())
            .is_some_and(traceparent_is_sampled);
        if !explicit_sampling && !sampled_parent {
            return None;
        }
        Some(global::get_text_map_propagator(|propagator| {
            propagator.extract(&HeaderExtractor(headers))
        }))
    }

    pub fn with_stream_id(mut self, stream_id: impl Into<String>) -> Self {
        Arc::make_mut(&mut self.metadata).insert(STREAM_ID_HEADER.to_owned(), stream_id.into());
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

    /// Attach a tracing span only on the sampled path. Calling
    /// `OpenTelemetrySpanExt::context` for `Span::none()` still traverses the
    /// tracing subscriber, so normal requests must not use it.
    pub(crate) fn with_span_context(mut self, span: &tracing::Span) -> Self {
        if self.sampling_enabled && !span.is_disabled() {
            self.open_telemetry = span.context();
        }
        self
    }

    pub fn open_telemetry_context(&self) -> &OpenTelemetryContext {
        &self.open_telemetry
    }

    pub fn enable_sampling(mut self) -> Self {
        self.sampling_enabled = true;
        Arc::make_mut(&mut self.metadata).insert(TRACE_SAMPLING_HEADER.to_owned(), "1".to_owned());
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
        self.extend_transport_metadata(&mut metadata);
        metadata
    }

    pub(crate) fn transport_metadata_with_tracing(
        &self,
        tracing_enabled: bool,
    ) -> HashMap<String, String> {
        let mut metadata = HashMap::new();
        self.extend_transport_metadata_with_tracing(&mut metadata, tracing_enabled);
        metadata
    }

    pub(crate) fn extend_transport_metadata(&self, metadata: &mut HashMap<String, String>) {
        self.extend_transport_metadata_with_tracing(metadata, true);
    }

    pub(crate) fn extend_transport_metadata_with_tracing(
        &self,
        metadata: &mut HashMap<String, String>,
        tracing_enabled: bool,
    ) {
        if let Some(value) = self.metadata.get(STREAM_ID_HEADER) {
            metadata.insert(STREAM_ID_HEADER.to_owned(), value.clone());
        }
        if !tracing_enabled {
            return;
        }
        if let Some(value) = self.metadata.get(TRACE_SAMPLING_HEADER) {
            metadata.insert(TRACE_SAMPLING_HEADER.to_owned(), value.clone());
        }
        if self.open_telemetry.span().span_context().is_valid() {
            global::get_text_map_propagator(|propagator| {
                propagator.inject_context(&self.open_telemetry, &mut MetadataInjector(metadata));
            });
        }
    }

    pub fn from_tonic_request<T>(request: &tonic::Request<T>) -> Self {
        Self::from_tonic_request_with_tracing(request, true)
    }

    pub fn from_tonic_request_with_tracing<T>(
        request: &tonic::Request<T>,
        tracing_enabled: bool,
    ) -> Self {
        let metadata: HashMap<String, String> = request
            .metadata()
            .iter()
            .filter_map(|entry| match entry {
                tonic::metadata::KeyAndValueRef::Ascii(key, value) => {
                    let name = key.as_str();
                    if !tracing_enabled
                        && (name == TRACE_SAMPLING_HEADER
                            || name == "traceparent"
                            || name == "tracestate"
                            || name == "baggage")
                    {
                        return None;
                    }
                    value
                        .to_str()
                        .ok()
                        .map(|value| (name.to_owned(), value.to_owned()))
                }
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
        let context = if tracing_enabled {
            Self::new().with_metadata(metadata)
        } else {
            Self::new().with_metadata_untraced(metadata)
        };
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
        fn insert(metadata: &mut tonic::metadata::MetadataMap, name: &str, value: &str) {
            let Ok(key) =
                tonic::metadata::MetadataKey::<tonic::metadata::Ascii>::from_bytes(name.as_bytes())
            else {
                return;
            };
            let Ok(value) = tonic::metadata::MetadataValue::try_from(value) else {
                return;
            };
            metadata.insert(key, value);
        }
        if self.open_telemetry.span().span_context().is_valid() {
            // Keep propagator overwrite/filtering semantics, including custom propagators.
            for (name, value) in self.transport_metadata() {
                insert(request.metadata_mut(), &name, &value);
            }
        } else {
            for name in [STREAM_ID_HEADER, TRACE_SAMPLING_HEADER] {
                if let Some(value) = self.metadata.get(name) {
                    insert(request.metadata_mut(), name, value);
                }
            }
        }
        if let Some(remaining) = self.remaining() {
            request.set_timeout(remaining);
        }
    }

    pub fn cancel(&self) {
        self.cancellation.cancel();
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn untraced_metadata_preserves_headers_without_enabling_sampling() {
        let metadata = HashMap::from([
            (TRACE_SAMPLING_HEADER.to_owned(), "1".to_owned()),
            ("x-business-header".to_owned(), "value".to_owned()),
        ]);
        let untraced = MessageContext::new().with_metadata_untraced(metadata.clone());
        assert_eq!(
            untraced
                .metadata()
                .get(TRACE_SAMPLING_HEADER)
                .map(String::as_str),
            Some("1")
        );
        assert_eq!(
            untraced
                .metadata()
                .get("x-business-header")
                .map(String::as_str),
            Some("value")
        );
        assert!(!untraced.sampling_enabled());

        let traced = MessageContext::new().with_metadata(metadata);
        assert!(traced.sampling_enabled());
    }

    #[tokio::test(start_paused = true)]
    async fn cancellation_wait_observes_deadline() {
        let context = MessageContext::with_timeout(Duration::from_secs(60));
        let wait = context.cancelled();
        tokio::pin!(wait);
        assert!(futures::poll!(&mut wait).is_pending());
        tokio::time::advance(Duration::from_secs(60)).await;
        wait.await;
        assert!(context.is_cancelled());
    }

    #[tokio::test]
    async fn cancellation_wait_observes_parent_cancellation() {
        let parent = MessageContext::new();
        let child = parent.child();
        let wait = child.cancelled();
        tokio::pin!(wait);
        assert!(futures::poll!(&mut wait).is_pending());
        parent.cancel();
        assert!(futures::poll!(&mut wait).is_ready());
    }

    #[test]
    fn child_cancellation_is_one_way() {
        let parent = MessageContext::new();
        let child = parent.child();
        let sibling = child.clone();

        child.cancel();

        assert!(sibling.is_cancelled());
        assert!(!parent.is_cancelled());

        let inherited = parent.child();
        parent.cancel();
        assert!(inherited.is_cancelled());
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

/// The graph's consumer contract. Concrete links await this future directly;
/// they do not allocate a box merely to call the next operator.
pub trait Consumer<T>: Send + Sync
where
    T: Send + Sync + 'static,
{
    fn consume(
        &self,
        context: MessageContext,
        payload: Payload<T>,
    ) -> impl Future<Output = ()> + Send;
}

pub(crate) trait ErasedConsumer<T>: Send + Sync
where
    T: Send + Sync + 'static,
{
    fn consume_erased(
        &self,
        context: MessageContext,
        payload: Payload<T>,
    ) -> std::pin::Pin<Box<dyn Future<Output = ()> + Send + '_>>;
}

impl<T, C> ErasedConsumer<T> for C
where
    T: Send + Sync + 'static,
    C: Consumer<T>,
{
    fn consume_erased(
        &self,
        context: MessageContext,
        payload: Payload<T>,
    ) -> std::pin::Pin<Box<dyn Future<Output = ()> + Send + '_>> {
        Box::pin(self.consume(context, payload))
    }
}

impl<T: Send + Sync + 'static> Consumer<T> for dyn ErasedConsumer<T> + '_ {
    fn consume(
        &self,
        context: MessageContext,
        payload: Payload<T>,
    ) -> impl Future<Output = ()> + Send {
        self.consume_erased(context, payload)
    }
}

/// Return true after the last result required by this invocation.
#[async_trait]
pub trait SubStreamCollector<R>: Send + Sync
where
    R: Send + Sync + 'static,
{
    async fn out(&self, context: MessageContext, payload: Payload<R>) -> bool;
}

pub struct SubStreamCollectorFunc<F>(pub F);

#[async_trait]
impl<R, F, Fut> SubStreamCollector<R> for SubStreamCollectorFunc<F>
where
    R: Send + Sync + 'static,
    F: Fn(MessageContext, Payload<R>) -> Fut + Send + Sync,
    Fut: Future<Output = bool> + Send,
{
    async fn out(&self, context: MessageContext, payload: Payload<R>) -> bool {
        (self.0)(context, payload).await
    }
}

#[async_trait]
pub trait CallableSubStream<T, R>: Send + Sync
where
    T: Send + Sync + 'static,
    R: Send + Sync + 'static,
{
    async fn consume(
        &self,
        context: MessageContext,
        value: T,
        collector: Arc<dyn SubStreamCollector<R>>,
    ) -> crate::runtime::environment::RuntimeResult<()>;
}

/// Topology-preserving consumer for a node disabled by its custom properties.
/// It deliberately performs no transport work and completes immediately.
#[derive(Clone, Copy, Debug, Default)]
pub struct NoopConsumer;

impl<T> Consumer<T> for NoopConsumer
where
    T: Send + Sync + 'static,
{
    async fn consume(&self, _context: MessageContext, _payload: Payload<T>) {}
}

pub trait RuntimeStream: Send + Sync {
    fn id(&self) -> i32;
    fn name(&self) -> String;
    fn environment(&self) -> &crate::runtime::environment::RuntimeEnvironment;

    /// Borrow immutable stream, pipeline and component labels cached at construction.
    fn tracing_labels(&self) -> (&str, &str, &str);

    fn start_span(
        &self,
        context: MessageContext,
        operation: &'static str,
    ) -> (MessageContext, Option<tracing::Span>) {
        if !self.environment().tracing_enabled() || !context.sampling_enabled() {
            return (context, None);
        }
        let (stream, pipeline, component) = self.tracing_labels();
        let span = tracing::info_span!(
            "stream.operation",
            otel.name = operation,
            stream = stream,
            pipeline = %pipeline,
            component = %component,
            error = tracing::field::Empty,
            otel.status_code = tracing::field::Empty,
            otel.status_message = tracing::field::Empty,
        );
        if span.is_disabled() {
            return (context, None);
        }
        let _ = span.set_parent(context.open_telemetry_context().clone());
        let child = span.context();
        (context.with_open_telemetry_context(child), Some(span))
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
