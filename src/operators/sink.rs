use std::sync::Arc;

use async_trait::async_trait;
use tracing::Instrument;

use serde::{Serialize, de::DeserializeOwned};

use super::error::ErrorStream;
use crate::runtime::{
    common::{ConstructionCell, Consumer, MessageContext, Payload, RuntimeStream},
    config::{RuntimeStreamConfig, SinkStreamConfig},
    environment::{RuntimeEnvironment, RuntimeResult},
    stream::Stream,
};

pub struct SinkStream<T, E>
where
    T: Send + Sync + 'static,
    E: Send + Sync + 'static,
{
    id: i32,
    sink_consumer: ConstructionCell<Arc<dyn Consumer<T>>>,
    error_stream: Stream<E>,
}

impl<T, E> SinkStream<T, E>
where
    T: Send + Sync + 'static,
    // Go: SinkStream[T,E].errorConsumer — MakeErrorStream[E](id, env), fresh.
    E: Serialize + DeserializeOwned + Send + Sync + 'static,
{
    pub fn make(config: &SinkStreamConfig, source: &Stream<T>) -> RuntimeResult<Arc<Self>> {
        let error_stream = ErrorStream::new(&config.stream, source.environment().clone())
            .stream()
            .clone();
        let id = config.stream.id;
        let sink_stream = Arc::new(Self {
            id,
            sink_consumer: ConstructionCell::empty(),
            error_stream,
        });
        source.try_set_consumer(Arc::clone(&sink_stream), sink_stream.id)?;
        Ok(sink_stream)
    }
}

impl<T, E> SinkStream<T, E>
where
    T: Send + Sync + 'static,
    E: Send + Sync + 'static,
{
    pub fn endpoint_id(&self) -> i32 {
        match self.environment().stream_config(self.id()).as_deref() {
            Some(RuntimeStreamConfig::Sink(config)) => config.endpoint_id,
            _ => 0,
        }
    }

    pub fn error_stream(&self) -> &Stream<E> {
        &self.error_stream
    }

    pub fn set_sink_consumer(&self, consumer: Arc<dyn Consumer<T>>) -> RuntimeResult<()> {
        self.sink_consumer.replace(consumer);
        Ok(())
    }
}

impl<T, E> RuntimeStream for SinkStream<T, E>
where
    T: Send + Sync + 'static,
    E: Send + Sync + 'static,
{
    fn id(&self) -> i32 {
        self.id
    }

    fn name(&self) -> String {
        self.environment().stream_name(self.id)
    }

    fn environment(&self) -> &RuntimeEnvironment {
        self.error_stream.environment()
    }
}

#[async_trait]
impl<T, E> Consumer<T> for SinkStream<T, E>
where
    T: Send + Sync + 'static,
    E: Send + Sync + 'static,
{
    async fn consume(&self, context: MessageContext, payload: Payload<T>) {
        let (context, span) = RuntimeStream::start_span(self, context, "stream.sink");
        if let Some(consumer) = self.sink_consumer.get() {
            consumer.consume(context, payload).instrument(span).await;
        }
    }
}

pub struct SinkStreamWithResult<T, R, E>
where
    T: Send + Sync + 'static,
    R: Send + Sync + 'static,
    E: Send + Sync + 'static,
{
    result_stream: Stream<R>,
    sink_consumer: ConstructionCell<Arc<dyn Consumer<T>>>,
    error_stream: Stream<E>,
}

impl<T, R, E> SinkStreamWithResult<T, R, E>
where
    T: Send + Sync + 'static,
    // Go: SinkStreamWithResult[T,R,E] embeds ConsumedStream[R] via
    // MakeSerde[R](env); errorConsumer via MakeErrorStream[E](id, env). Both
    // R and E are fresh — neither is the sink's consumed input type T.
    R: Serialize + DeserializeOwned + Send + Sync + 'static,
    E: Serialize + DeserializeOwned + Send + Sync + 'static,
{
    pub fn make(config: &SinkStreamConfig, source: &Stream<T>) -> RuntimeResult<Arc<Self>> {
        let result_stream = Stream::new(&config.stream, source.environment().clone());
        let error_stream = ErrorStream::new(&config.stream, source.environment().clone())
            .stream()
            .clone();
        let sink_stream = Arc::new(Self {
            result_stream,
            sink_consumer: ConstructionCell::empty(),
            error_stream,
        });
        source.try_set_consumer(Arc::clone(&sink_stream), sink_stream.result_stream.id())?;
        Ok(sink_stream)
    }
}

impl<T, R, E> SinkStreamWithResult<T, R, E>
where
    T: Send + Sync + 'static,
    R: Send + Sync + 'static,
    E: Send + Sync + 'static,
{
    pub fn stream(&self) -> &Stream<R> {
        &self.result_stream
    }

    pub fn endpoint_id(&self) -> i32 {
        match self.result_stream.config().as_ref() {
            RuntimeStreamConfig::Sink(config) => config.endpoint_id,
            _ => 0,
        }
    }

    pub fn error_stream(&self) -> &Stream<E> {
        &self.error_stream
    }

    pub fn set_sink_consumer(&self, consumer: Arc<dyn Consumer<T>>) -> RuntimeResult<()> {
        self.sink_consumer.replace(consumer);
        Ok(())
    }

    pub async fn consume_result(&self, context: MessageContext, payload: Payload<R>) {
        self.result_stream.emit(context, payload).await;
    }
}

#[async_trait]
impl<T, R, E> Consumer<T> for SinkStreamWithResult<T, R, E>
where
    T: Send + Sync + 'static,
    R: Send + Sync + 'static,
    E: Send + Sync + 'static,
{
    async fn consume(&self, context: MessageContext, payload: Payload<T>) {
        let (context, span) = RuntimeStream::start_span(self, context, "stream.sink");
        if let Some(consumer) = self.sink_consumer.get() {
            consumer.consume(context, payload).instrument(span).await;
        }
    }
}

impl<T, R, E> RuntimeStream for SinkStreamWithResult<T, R, E>
where
    T: Send + Sync + 'static,
    R: Send + Sync + 'static,
    E: Send + Sync + 'static,
{
    fn id(&self) -> i32 {
        self.result_stream.id()
    }

    fn name(&self) -> String {
        self.result_stream.name()
    }

    fn environment(&self) -> &RuntimeEnvironment {
        self.result_stream.environment()
    }
}

impl<T> Stream<T>
where
    T: Send + Sync + 'static,
{
    pub fn sink<E>(&self, config: &SinkStreamConfig) -> RuntimeResult<Arc<SinkStream<T, E>>>
    where
        E: Serialize + DeserializeOwned + Send + Sync + 'static,
    {
        SinkStream::make(config, self)
    }

    pub fn sink_with_result<R, E>(
        &self,
        config: &SinkStreamConfig,
    ) -> RuntimeResult<Arc<SinkStreamWithResult<T, R, E>>>
    where
        R: Serialize + DeserializeOwned + Send + Sync + 'static,
        E: Serialize + DeserializeOwned + Send + Sync + 'static,
    {
        SinkStreamWithResult::make(config, self)
    }
}
