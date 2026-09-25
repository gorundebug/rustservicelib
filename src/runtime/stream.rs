use std::sync::Arc;

use crate::runtime::{
    collector::{Collect, Collector, LinkCollector},
    common::{ConstructionCell, Consumer, MessageContext, Payload, RuntimeStream},
    config::{RuntimeStreamConfig, StreamConfig},
    environment::{RuntimeEnvironment, RuntimeError, RuntimeResult},
    serde::StreamSerde,
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
    environment: RuntimeEnvironment,
    name: String,
    pipeline: String,
    component: String,
    downstream: ConstructionCell<StreamDispatch<T>>,
    serde: Arc<dyn StreamSerde<T>>,
}

pub(crate) enum StreamDispatch<T: Send + Sync + 'static> {
    Dynamic(Box<LinkCollector<T>>),
    Typed {
        collector: Arc<dyn crate::runtime::common::ErasedConsumer<T>>,
        is_async: bool,
    },
}

impl<T: Send + Sync + 'static> StreamDispatch<T> {
    pub(crate) fn is_async(&self) -> bool {
        match self {
            Self::Dynamic(collector) => collector.is_async(),
            Self::Typed { is_async, .. } => *is_async,
        }
    }
}

impl<T: Send + Sync + 'static> Collect<T> for StreamDispatch<T> {
    fn out(
        &self,
        context: MessageContext,
        value: T,
    ) -> impl std::future::Future<Output = ()> + Send {
        self.out_payload(context, Payload::new(value))
    }

    async fn out_payload(&self, context: MessageContext, payload: Payload<T>) {
        match self {
            Self::Dynamic(collector) => collector.out_payload(context, payload).await,
            Self::Typed { collector, .. } => collector.consume(context, payload).await,
        }
    }
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

// Root streams and type-changing operators resolve their serializer through
// the service environment, matching Go's runtime.MakeSerde[T](env).
impl<T> Stream<T>
where
    T: Send + Sync + 'static,
{
    pub fn new(config: &StreamConfig, environment: RuntimeEnvironment) -> Self {
        Self::with_id(config.id, environment)
    }

    pub(crate) fn with_id(id: i32, environment: RuntimeEnvironment) -> Self {
        let serde = environment.make_serde::<T>();
        Self::with_id_and_serde(id, environment, serde)
    }
}

impl<T> Stream<T>
where
    T: Send + Sync + 'static,
{
    // Go: stream.GetSerde() reuse — type-preserving operators (delay, filter,
    // link, merge, split) and any other case that already has a parent stream
    // propagate its existing serde instead of resolving a new one.
    pub(crate) fn with_id_and_serde(
        id: i32,
        environment: RuntimeEnvironment,
        serde: Arc<dyn StreamSerde<T>>,
    ) -> Self {
        environment.register_runtime_stream(id);
        let name = environment.stream_name(configured_stream_id(id));
        let (pipeline, component) = environment.stream_grouping(id);
        Self {
            inner: Arc::new(StreamInner {
                id,
                environment,
                name,
                pipeline,
                component,
                downstream: ConstructionCell::empty(),
                serde,
            }),
        }
    }

    pub fn derived(
        config: &StreamConfig,
        environment: RuntimeEnvironment,
        serde: Arc<dyn StreamSerde<T>>,
    ) -> Self {
        Self::with_id_and_serde(config.id, environment, serde)
    }

    pub(crate) fn derived_with_name(
        config: &StreamConfig,
        environment: RuntimeEnvironment,
        name: String,
        serde: Arc<dyn StreamSerde<T>>,
    ) -> Self {
        let id = config.id;
        environment.register_runtime_stream(id);
        let (pipeline, component) = environment.stream_grouping(id);
        Self {
            inner: Arc::new(StreamInner {
                id,
                environment,
                name,
                pipeline,
                component,
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
        self.inner.name.clone()
    }

    pub fn config(&self) -> Arc<RuntimeStreamConfig> {
        self.inner
            .environment
            .stream_config(configured_stream_id(self.inner.id))
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
        let consumer: Arc<dyn crate::runtime::common::ErasedConsumer<T>> = consumer;
        let collector: LinkCollector<T> = LinkCollector::new(
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
            .set(StreamDispatch::Dynamic(Box::new(collector)))
            .map_err(|_| RuntimeError::ConsumerAlreadySet {
                stream: self.name(),
            })
    }

    pub fn collector(&self) -> Collector<T> {
        Collector::from_stream(self.clone())
    }

    /// Connect once during graph construction and return an allocation-free
    /// dispatch view. All existing clones keep a working dynamic entry point.
    pub fn try_set_typed_consumer<N>(
        &self,
        consumer: Arc<N>,
        target_id: i32,
    ) -> RuntimeResult<Collector<T, impl Collect<T> + Clone + use<T, N>>>
    where
        N: Consumer<T> + 'static,
    {
        if self.inner.downstream.get().is_some() {
            return Err(RuntimeError::ConsumerAlreadySet {
                stream: self.name(),
            });
        }
        let environment = &self.inner.environment;
        let collector = Arc::new(LinkCollector::new(
            consumer,
            environment.call_semantics(self.id(), target_id),
            environment,
            self.id(),
            target_id,
            self.name(),
            environment.function_call_async(self.id(), target_id),
        )?);
        let dispatch = StreamDispatch::Typed {
            collector: collector.clone(),
            is_async: collector.is_async(),
        };
        self.inner
            .downstream
            .set(dispatch)
            .map_err(|_| RuntimeError::ConsumerAlreadySet {
                stream: self.name(),
            })?;
        Ok(Collector::from_output(self.clone(), collector))
    }

    pub(crate) fn link_collector(&self) -> Option<&StreamDispatch<T>> {
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
    ) -> (MessageContext, Option<tracing::Span>) {
        RuntimeStream::start_span(self, context, operation)
    }

    pub async fn emit(&self, context: MessageContext, payload: Payload<T>) {
        if let Some(collector) = self.inner.downstream.get() {
            collector.out_payload(context, payload).await;
        }
    }
}

fn configured_stream_id(runtime_id: i32) -> i32 {
    runtime_id
        .checked_abs()
        .expect("runtime stream ID cannot be i32::MIN")
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

    fn tracing_labels(&self) -> (&str, &str, &str) {
        (
            &self.inner.name,
            &self.inner.pipeline,
            &self.inner.component,
        )
    }
}

impl<T: Send + Sync + 'static> Collect<T> for Stream<T> {
    fn out(
        &self,
        context: MessageContext,
        value: T,
    ) -> impl std::future::Future<Output = ()> + Send {
        self.emit(context, Payload::new(value))
    }
    fn out_payload(
        &self,
        context: MessageContext,
        payload: Payload<T>,
    ) -> impl std::future::Future<Output = ()> + Send {
        self.emit(context, payload)
    }
}
