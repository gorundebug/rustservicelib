use std::sync::Arc;

use async_trait::async_trait;

use serde::{Serialize, de::DeserializeOwned};

use super::error::ErrorStream;
use crate::runtime::{
    collector::Collector,
    common::{Consumer, MessageContext, Payload, RuntimeStream},
    config::ProcessStreamConfig,
    environment::RuntimeResult,
    stream::Stream,
};

#[async_trait]
pub trait ProcessFunction<T, R, E>: Send + Sync
where
    T: Send + Sync + 'static,
    R: Send + Sync + 'static,
    E: Send + Sync + 'static,
{
    async fn process(
        &self,
        context: MessageContext,
        stream: &dyn RuntimeStream,
        value: &T,
        out: &Collector<R>,
        error: &Collector<E>,
    );
}

pub struct ProcessStream<T, R, E, F>
where
    T: Send + Sync + 'static,
    R: Send + Sync + 'static,
    E: Send + Sync + 'static,
    F: ProcessFunction<T, R, E>,
{
    output: Stream<R>,
    error: Stream<E>,
    function: F,
    _input: std::marker::PhantomData<fn(T)>,
}

impl<T, R, E, F> ProcessStream<T, R, E, F>
where
    T: Send + Sync + 'static,
    // Go-aligned: output serde (R) and error serde (E) are both freshly
    // resolved (Go: runtime.MakeSerde[R](env) / MakeErrorStream[E](id, env)) —
    // both are new types at this point in the graph, not the input type T.
    R: Serialize + DeserializeOwned + Send + Sync + 'static,
    E: Serialize + DeserializeOwned + Send + Sync + 'static,
    F: ProcessFunction<T, R, E> + 'static,
{
    pub fn make(
        config: &ProcessStreamConfig,
        source: &Stream<T>,
        function: F,
    ) -> RuntimeResult<(Stream<R>, Stream<E>)> {
        let output = Stream::new(&config.stream, source.environment().clone());
        let error = ErrorStream::new(&config.stream, source.environment().clone())
            .stream()
            .clone();
        source.try_set_consumer(
            Arc::new(Self {
                output: output.clone(),
                error: error.clone(),
                function,
                _input: std::marker::PhantomData,
            }),
            output.id(),
        )?;
        Ok((output, error))
    }
}

impl<T> Stream<T>
where
    T: Send + Sync + 'static,
{
    pub fn process<R, E, F>(
        &self,
        config: &ProcessStreamConfig,
        function: F,
    ) -> RuntimeResult<(Stream<R>, Stream<E>)>
    where
        R: Serialize + DeserializeOwned + Send + Sync + 'static,
        E: Serialize + DeserializeOwned + Send + Sync + 'static,
        F: ProcessFunction<T, R, E> + 'static,
    {
        ProcessStream::make(config, self, function)
    }
}

#[async_trait]
impl<T, R, E, F> Consumer<T> for ProcessStream<T, R, E, F>
where
    T: Send + Sync + 'static,
    R: Send + Sync + 'static,
    E: Send + Sync + 'static,
    F: ProcessFunction<T, R, E> + 'static,
{
    async fn consume(&self, context: MessageContext, payload: Payload<T>) {
        let (context, span) = self.output.start_span(context, "stream.process");
        let out = self.output.collector();
        let error = self.error.collector();
        crate::runtime::common::instrument_if_enabled(
            self.function
                .process(context, &self.output, &payload, &out, &error),
            span,
        )
        .await;
    }
}
