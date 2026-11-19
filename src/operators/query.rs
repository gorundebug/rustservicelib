use std::sync::Arc;

use super::error::ErrorStream;
use crate::runtime::{
    common::{ConstructionCell, Consumer, MessageContext, Payload, RuntimeStream},
    config::{QueryStreamConfig, RuntimeEndpointConfig},
    environment::{RuntimeEnvironment, RuntimeResult},
    stream::Stream,
};

pub struct QueryStream<T, R, E>
where
    T: Send + Sync + 'static,
    R: Send + Sync + 'static,
    E: Send + Sync + 'static,
{
    result_stream: Stream<R>,
    endpoint_id: i32,
    query_consumer: ConstructionCell<Arc<dyn crate::runtime::common::ErasedConsumer<T>>>,
    error_stream: Stream<E>,
}

impl<T, R, E> QueryStream<T, R, E>
where
    T: Send + Sync + 'static,
    // Go: QueryStream[T,R,E] embeds ConsumedStream[R] via
    // MakeSerde[R](env); errorConsumer via MakeErrorStream[E](id, env). Both
    // R and E are fresh — neither is the Query input type T.
    R: Send + Sync + 'static,
    E: Send + Sync + 'static,
{
    pub fn make(config: &QueryStreamConfig, source: &Stream<T>) -> RuntimeResult<Arc<Self>> {
        let query = Self::new(config, source.environment().clone())?;
        source.try_set_consumer(Arc::clone(&query), query.result_stream.id())?;
        Ok(query)
    }

    pub fn new(
        config: &QueryStreamConfig,
        environment: RuntimeEnvironment,
    ) -> RuntimeResult<Arc<Self>> {
        let result_stream = Stream::new(&config.stream, environment.clone());
        let error_stream = ErrorStream::new(&config.stream, environment)
            .stream()
            .clone();
        let query_stream = Arc::new(Self {
            result_stream,
            endpoint_id: config.endpoint_id,
            query_consumer: ConstructionCell::empty(),
            error_stream,
        });
        Ok(query_stream)
    }
}

impl<T, R, E> QueryStream<T, R, E>
where
    T: Send + Sync + 'static,
    R: Send + Sync + 'static,
    E: Send + Sync + 'static,
{
    pub fn stream(&self) -> &Stream<R> {
        &self.result_stream
    }

    pub fn endpoint_id(&self) -> i32 {
        self.endpoint_id
    }

    pub fn error_stream(&self) -> &Stream<E> {
        &self.error_stream
    }

    pub fn set_query_consumer<C: Consumer<T> + 'static>(
        &self,
        consumer: Arc<C>,
    ) -> RuntimeResult<()> {
        self.query_consumer.replace(consumer);
        Ok(())
    }

    pub async fn consume_result(&self, context: MessageContext, payload: Payload<R>) {
        self.result_stream.emit(context, payload).await;
    }
}

impl<T, R, E> Consumer<T> for QueryStream<T, R, E>
where
    T: Send + Sync + 'static,
    R: Send + Sync + 'static,
    E: Send + Sync + 'static,
{
    async fn consume(&self, context: MessageContext, payload: Payload<T>) {
        if let Some(endpoint) = self.environment().runtime_config().endpoint_by_id(self.endpoint_id) {
            if matches!(endpoint.as_ref(), RuntimeEndpointConfig::Query(config) if !config.enabled)
                || matches!(endpoint.as_ref(), RuntimeEndpointConfig::S3Query(config) if !config.enabled) {
                return;
            }
        }
        let (context, span) = RuntimeStream::start_span(self, context, "stream.query");
        if let Some(consumer) = self.query_consumer.get() {
            crate::runtime::common::instrument_if_present!(
                consumer.consume(context, payload),
                span
            );
        }
    }
}

impl<T, R, E> RuntimeStream for QueryStream<T, R, E>
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

    fn tracing_labels(&self) -> (&str, &str, &str) {
        self.result_stream.tracing_labels()
    }
}

impl<T> Stream<T>
where T: Send + Sync + 'static,
{
    pub fn query<R, E>(&self, config: &QueryStreamConfig) -> RuntimeResult<Arc<QueryStream<T, R, E>>>
    where R: Send + Sync + 'static, E: Send + Sync + 'static,
    { QueryStream::make(config, self) }
}

pub fn create<T, R, E>(config: &QueryStreamConfig, environment: RuntimeEnvironment) -> RuntimeResult<Arc<QueryStream<T, R, E>>>
where T: Send + Sync + 'static, R: Send + Sync + 'static, E: Send + Sync + 'static,
{ QueryStream::new(config, environment) }
