use std::sync::{Arc, RwLock};

use crate::runtime::{
    collector::{Collect, Collector},
    common::{Consumer, MessageContext, Payload, RuntimeStream},
    config::RuntimeStreamConfig,
    environment::{RuntimeEnvironment, RuntimeError, RuntimeResult},
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
    downstream: RwLock<Option<Collector<T>>>,
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

impl<T> Stream<T>
where
    T: Send + Sync + 'static,
{
    pub fn new(config: impl Into<RuntimeStreamConfig>, environment: RuntimeEnvironment) -> Self {
        let config = config.into();
        let id = config.stream().id;
        Self::with_ids(id, id, environment)
    }

    pub(crate) fn with_ids(id: i32, config_id: i32, environment: RuntimeEnvironment) -> Self {
        environment.register_runtime_stream(id);
        Self {
            inner: Arc::new(StreamInner {
                id,
                config_id,
                environment,
                name_override: None,
                downstream: RwLock::new(None),
            }),
        }
    }

    pub(crate) fn with_name(
        config: impl Into<RuntimeStreamConfig>,
        environment: RuntimeEnvironment,
        name: String,
    ) -> Self {
        let config = config.into();
        let id = config.stream().id;
        environment.register_runtime_stream(id);
        Self {
            inner: Arc::new(StreamInner {
                id,
                config_id: id,
                environment,
                name_override: Some(name),
                downstream: RwLock::new(None),
            }),
        }
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
        let mut downstream = self
            .inner
            .downstream
            .write()
            .expect("stream downstream lock poisoned");
        if downstream.is_some() {
            return Err(RuntimeError::ConsumerAlreadySet {
                stream: self.name(),
            });
        }
        let semantics = self.inner.environment.call_semantics(self.id(), target_id);
        *downstream = Some(Collector::new(
            consumer,
            semantics,
            &self.inner.environment,
            self.id(),
            target_id,
            self.name(),
        )?);
        Ok(())
    }

    pub fn collector(&self) -> Option<Collector<T>> {
        self.inner
            .downstream
            .read()
            .expect("stream downstream lock poisoned")
            .clone()
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
        let collector = self.collector();
        if let Some(collector) = collector {
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
