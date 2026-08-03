use std::sync::Arc;

use async_trait::async_trait;
use tracing::Instrument;

use serde::{Serialize, de::DeserializeOwned};

use crate::runtime::{
    common::{Consumer, MessageContext, Payload, RuntimeStream},
    config::MapStreamConfig,
    environment::RuntimeResult,
    stream::Stream,
};

#[async_trait]
pub trait MapFunction<T, R>: Send + Sync
where
    T: Send + Sync + 'static,
    R: Send + Sync + 'static,
{
    async fn map(
        &self,
        context: MessageContext,
        stream: &dyn RuntimeStream,
        value: Payload<T>,
        out: &Stream<R>,
    );
}

pub struct MapStream<T, R, F>
where
    T: Send + Sync + 'static,
    R: Send + Sync + 'static,
    F: MapFunction<T, R>,
{
    output: Stream<R>,
    function: F,
    _input: std::marker::PhantomData<fn(T)>,
}

impl<T, R, F> MapStream<T, R, F>
where
    T: Send + Sync + 'static,
    // Go: runtime.MakeSerde[R](env) — fresh, R is a new type at this point.
    R: Serialize + DeserializeOwned + Send + Sync + 'static,
    F: MapFunction<T, R> + 'static,
{
    pub fn make(
        config: MapStreamConfig,
        source: &Stream<T>,
        function: F,
    ) -> RuntimeResult<Stream<R>> {
        let output = Stream::new(config, source.environment().clone());
        let operator = Arc::new(Self {
            output: output.clone(),
            function,
            _input: std::marker::PhantomData,
        });
        source.try_set_consumer(operator, output.id())?;
        Ok(output)
    }
}

impl<T> Stream<T>
where
    T: Send + Sync + 'static,
{
    pub fn map<R, F>(
        &self,
        config: impl Into<MapStreamConfig>,
        function: F,
    ) -> RuntimeResult<Stream<R>>
    where
        R: Serialize + DeserializeOwned + Send + Sync + 'static,
        F: MapFunction<T, R> + 'static,
    {
        MapStream::make(config.into(), self, function)
    }
}

#[async_trait]
impl<T, R, F> Consumer<T> for MapStream<T, R, F>
where
    T: Send + Sync + 'static,
    R: Send + Sync + 'static,
    F: MapFunction<T, R> + 'static,
{
    async fn consume(&self, context: MessageContext, payload: Payload<T>) {
        let (context, span) = self.output.start_span(context, "stream.map");
        self.function
            .map(context, &self.output, payload, &self.output)
            .instrument(span)
            .await;
    }
}
