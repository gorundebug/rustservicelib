use std::sync::Arc;

use async_trait::async_trait;
use tracing::Instrument;

use super::error::ErrorStream;
use crate::runtime::{
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
        value: Payload<T>,
        out: &Stream<R>,
        error: &Stream<E>,
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
    R: Send + Sync + 'static,
    E: Send + Sync + 'static,
    F: ProcessFunction<T, R, E> + 'static,
{
    pub fn make(
        config: ProcessStreamConfig,
        source: &Stream<T>,
        function: F,
    ) -> RuntimeResult<(Stream<R>, Stream<E>)> {
        let base = config.stream.clone();
        let output = Stream::new(config, source.environment().clone());
        let error = ErrorStream::new(&base, source.environment().clone())
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
        config: impl Into<ProcessStreamConfig>,
        function: F,
    ) -> RuntimeResult<(Stream<R>, Stream<E>)>
    where
        R: Send + Sync + 'static,
        E: Send + Sync + 'static,
        F: ProcessFunction<T, R, E> + 'static,
    {
        ProcessStream::make(config.into(), self, function)
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
        self.function
            .process(context, &self.output, payload, &self.output, &self.error)
            .instrument(span)
            .await;
    }
}
