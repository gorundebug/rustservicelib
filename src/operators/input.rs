use std::sync::Arc;

use async_trait::async_trait;

use serde::{Serialize, de::DeserializeOwned};

use crate::runtime::{
    common::{ConstructionCell, Consumer, MessageContext, Payload},
    config::{InputStreamConfig, RuntimeStreamConfig},
    environment::{RuntimeEnvironment, RuntimeError, RuntimeResult},
    stream::Stream,
};

pub struct InputStream<T, R, E>
where
    T: Send + Sync + 'static,
    R: Send + Sync + 'static,
    E: Send + Sync + 'static,
{
    inner: Arc<InputStreamInner<T, R, E>>,
}

struct InputStreamInner<T, R, E>
where
    T: Send + Sync + 'static,
    R: Send + Sync + 'static,
    E: Send + Sync + 'static,
{
    stream: Stream<T>,
    result_source: ConstructionCell<Stream<R>>,
    result_consumer: ConstructionCell<Arc<dyn Consumer<R>>>,
    error_stream: Stream<E>,
}

impl<T, R, E> Clone for InputStream<T, R, E>
where
    T: Send + Sync + 'static,
    R: Send + Sync + 'static,
    E: Send + Sync + 'static,
{
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl<T, R, E> InputStream<T, R, E>
where
    // Go: MakeInputStream[T,R,E] always resolves a fresh serde for T (no
    // parent stream exists for a root); MakeErrorStream[E] does the same for
    // the error branch. Neither has anything to propagate from.
    T: Serialize + DeserializeOwned + Send + Sync + 'static,
    R: Send + Sync + 'static,
    E: Serialize + DeserializeOwned + Send + Sync + 'static,
{
    pub fn new(config: &InputStreamConfig, environment: RuntimeEnvironment) -> Self {
        let error_stream = Stream::with_id(-config.stream.id, environment.clone());
        Self {
            inner: Arc::new(InputStreamInner {
                stream: Stream::new(&config.stream, environment),
                result_source: ConstructionCell::empty(),
                result_consumer: ConstructionCell::empty(),
                error_stream,
            }),
        }
    }
}

impl<T, R, E> InputStream<T, R, E>
where
    T: Send + Sync + 'static,
    R: Send + Sync + 'static,
    E: Send + Sync + 'static,
{
    pub fn stream(&self) -> &Stream<T> {
        &self.inner.stream
    }

    pub fn endpoint_id(&self) -> i32 {
        self.inner
            .stream
            .environment()
            .stream_config(self.inner.stream.id())
            .and_then(|config| match config.as_ref() {
                RuntimeStreamConfig::Input(config) => Some(config.endpoint_id),
                _ => None,
            })
            .unwrap_or_default()
    }

    pub fn endpoint_name(&self) -> RuntimeResult<String> {
        let runtime = self.stream().environment().runtime_config();
        runtime
            .endpoint_by_id(self.endpoint_id())
            .map(|endpoint| endpoint.name().to_owned())
            .ok_or_else(|| {
                RuntimeError::InvalidConfiguration(format!(
                    "endpoint {} referenced by input stream {:?} is not configured",
                    self.endpoint_id(),
                    self.stream().name()
                ))
            })
    }

    pub fn connector_name(&self) -> RuntimeResult<String> {
        let runtime = self.stream().environment().runtime_config();
        let endpoint = runtime.endpoint_by_id(self.endpoint_id()).ok_or_else(|| {
            RuntimeError::InvalidConfiguration(format!(
                "endpoint {} referenced by input stream {:?} is not configured",
                self.endpoint_id(),
                self.stream().name()
            ))
        })?;
        runtime
            .data_connector_by_id(endpoint.data_connector_id())
            .map(|connector| connector.name().to_owned())
            .ok_or_else(|| {
                RuntimeError::InvalidConfiguration(format!(
                    "data connector {} referenced by endpoint {:?} is not configured",
                    endpoint.data_connector_id(),
                    endpoint.name()
                ))
            })
    }

    pub fn error_stream(&self) -> &Stream<E> {
        &self.inner.error_stream
    }

    pub fn result_stream(&self) -> Option<Stream<R>> {
        self.inner.result_source.get().cloned()
    }

    pub fn set_result_consumer(&self, consumer: Arc<dyn Consumer<R>>) {
        // Go's SetResultConsumer is a build-time assignment. A generated
        // ResultRouter may be replaced by the concrete transport endpoint
        // while the graph is wired.
        self.inner.result_consumer.replace(consumer);
    }

    pub fn set_source(&self, source: &Stream<R>) -> RuntimeResult<()> {
        source.try_set_consumer(
            Arc::new(ResultLink {
                input_stream: self.clone(),
            }),
            self.stream().id(),
        )?;
        self.inner.result_source.set(source.clone()).map_err(|_| {
            crate::runtime::environment::RuntimeError::SourceAlreadySet {
                stream: self.inner.stream.name(),
            }
        })
    }

    pub async fn consume(&self, context: MessageContext, value: T) {
        self.consume_payload(context, Payload::new(value)).await;
    }

    pub async fn consume_payload(&self, context: MessageContext, payload: Payload<T>) {
        let (context, span) = self.inner.stream.start_span(context, "stream.input");
        crate::runtime::common::instrument_if_enabled(
            self.inner.stream.emit(context, payload),
            span,
        )
        .await;
    }

    async fn consume_result(&self, context: MessageContext, payload: Payload<R>) {
        if let Some(consumer) = self.inner.result_consumer.get() {
            consumer.consume(context, payload).await;
        }
    }
}

struct ResultLink<T, R, E>
where
    T: Send + Sync + 'static,
    R: Send + Sync + 'static,
    E: Send + Sync + 'static,
{
    input_stream: InputStream<T, R, E>,
}

#[async_trait]
impl<T, R, E> Consumer<R> for ResultLink<T, R, E>
where
    T: Send + Sync + 'static,
    R: Send + Sync + 'static,
    E: Send + Sync + 'static,
{
    async fn consume(&self, context: MessageContext, payload: Payload<R>) {
        self.input_stream.consume_result(context, payload).await;
    }
}
