use std::sync::Arc;

use serde::{Serialize, de::DeserializeOwned};

use crate::runtime::{
    collector::{Collect, Collector},
    common::{ConstructionCell, Consumer, MessageContext, Payload, RuntimeStream},
    config::{RuntimeStreamConfig, StreamConfig},
    environment::{RuntimeEnvironment, RuntimeError, RuntimeResult},
    serde::{JsonSerde, Serde as ServiceSerde, StreamSerde, make_stream_serde},
};

pub struct Stream<T>
where
    T: Send + Sync + 'static,
{
    inner: Arc<StreamInner<T>>,
}

struct StreamInner<T>
where
    T: Send + Sync + 'static,
{
    id: i32,
    config_id: i32,
    environment: RuntimeEnvironment,
    name_override: Option<String>,
    downstream: ConstructionCell<Collector<T>>,
    serde: Arc<dyn StreamSerde<T>>,
}

impl<T> Clone for Stream<T>
where
    T: Send + Sync + 'static,
{
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

// Go: runtime.MakeSerde[T](env) / MakeConsumedStream[T] — root streams and the
// output type of type-changing operators always resolve a fresh serde for
// their own type, with no parent to propagate from. Rust has no per-type
// generated serde registry (unlike Go's type switch or C++'s
// DefaultSerdeFactory specializations): JsonSerde<T> works generically for
// any T that derives serde::Serialize + Deserialize, which every generated
// type does, so no stub/fallback path is needed here.
impl<T> Stream<T>
where
    T: Serialize + DeserializeOwned + Send + Sync + 'static,
{
    pub fn new(config: &StreamConfig, environment: RuntimeEnvironment) -> Self {
        let id = config.id;
        Self::with_ids(id, id, environment)
    }

    pub(crate) fn with_ids(id: i32, config_id: i32, environment: RuntimeEnvironment) -> Self {
        let serde = make_stream_serde(Arc::new(JsonSerde::<T>::new()) as Arc<dyn ServiceSerde<T>>);
        Self::with_ids_and_serde(id, config_id, environment, serde)
    }
}

impl<T> Stream<T>
where
    T: Send + Sync + 'static,
{
    // Go: stream.GetSerde() reuse — type-preserving operators (delay, filter,
    // link, merge, split) and any other case that already has a parent stream
    // propagate its existing serde instead of resolving a new one.
    pub(crate) fn with_ids_and_serde(
        id: i32,
        config_id: i32,
        environment: RuntimeEnvironment,
        serde: Arc<dyn StreamSerde<T>>,
    ) -> Self {
        environment.register_runtime_stream(id);
        Self {
            inner: Arc::new(StreamInner {
                id,
                config_id,
                environment,
                name_override: None,
                downstream: ConstructionCell::empty(),
                serde,
            }),
        }
    }

    pub(crate) fn derived(
        config: &StreamConfig,
        environment: RuntimeEnvironment,
        serde: Arc<dyn StreamSerde<T>>,
    ) -> Self {
        let id = config.id;
        Self::with_ids_and_serde(id, id, environment, serde)
    }

    pub(crate) fn derived_with_name(
        config: &StreamConfig,
        environment: RuntimeEnvironment,
        name: String,
        serde: Arc<dyn StreamSerde<T>>,
    ) -> Self {
        let id = config.id;
        environment.register_runtime_stream(id);
        Self {
            inner: Arc::new(StreamInner {
                id,
                config_id: id,
                environment,
                name_override: Some(name),
                downstream: ConstructionCell::empty(),
                serde,
            }),
        }
    }

    pub fn get_serde(&self) -> Arc<dyn StreamSerde<T>> {
        Arc::clone(&self.inner.serde)
    }

    pub fn id(&self) -> i32 {
        self.inner.id
    }

    pub fn name(&self) -> String {
        self.inner
            .name_override
            .clone()
            .unwrap_or_else(|| self.inner.environment.stream_name(self.id()))
    }

    pub fn config(&self) -> Arc<RuntimeStreamConfig> {
        self.inner
            .environment
            .stream_config(self.inner.config_id)
            .expect("registered stream configuration is missing")
    }

    pub fn environment(&self) -> &RuntimeEnvironment {
        &self.inner.environment
    }

    pub fn set_consumer<C>(&self, consumer: Arc<C>, target_id: i32)
    where
        C: Consumer<T> + 'static,
    {
        self.try_set_consumer(consumer, target_id)
            .expect("failed to connect streams");
    }

    pub fn try_set_consumer<C>(&self, consumer: Arc<C>, target_id: i32) -> RuntimeResult<()>
    where
        C: Consumer<T> + 'static,
    {
        let semantics = self.inner.environment.call_semantics(self.id(), target_id);
        let function_call_async = self
            .inner
            .environment
            .function_call_async(self.id(), target_id);
        let collector = Collector::new(
            consumer,
            semantics,
            &self.inner.environment,
            self.id(),
            target_id,
            self.name(),
            function_call_async,
        )?;
        self.inner
            .downstream
            .set(collector)
            .map_err(|_| RuntimeError::ConsumerAlreadySet {
                stream: self.name(),
            })
    }

    pub(crate) fn collector(&self) -> Option<&Collector<T>> {
        self.inner.downstream.get()
    }

    /// Starts the same per-operator span as Go's ServiceStream.StartSpan.
    ///
    /// The returned context carries the child OTEL context into downstream
    /// calls, including calls dispatched through asynchronous pool semantics.
    pub fn start_span(
        &self,
        context: MessageContext,
        operation: &'static str,
    ) -> (MessageContext, tracing::Span) {
        RuntimeStream::start_span(self, context, operation)
    }

    pub async fn emit(&self, context: MessageContext, payload: Payload<T>) {
        if let Some(collector) = self.inner.downstream.get() {
            collector.out_payload(context, payload).await;
        }
    }
}

impl<T> RuntimeStream for Stream<T>
where
    T: Send + Sync + 'static,
{
    fn id(&self) -> i32 {
        self.id()
    }

    fn name(&self) -> String {
        self.name()
    }

    fn environment(&self) -> &RuntimeEnvironment {
        self.environment()
    }
}
