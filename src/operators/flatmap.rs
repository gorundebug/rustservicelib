use std::sync::Arc;

use async_trait::async_trait;
use tracing::Instrument;

use crate::runtime::{
    common::{Consumer, MessageContext, Payload, RuntimeStream},
    config::FlatMapStreamConfig,
    environment::RuntimeResult,
    stream::Stream,
};

#[async_trait]
pub trait FlatMapFunction<T, R>: Send + Sync
where
    T: Send + Sync + 'static,
    R: Send + Sync + 'static,
{
    async fn flat_map(
        &self,
        context: MessageContext,
        stream: &dyn RuntimeStream,
        value: Payload<T>,
        out: &Stream<R>,
    );
}

pub struct FlatMapStream<T, R, F>
where
    T: Send + Sync + 'static,
    R: Send + Sync + 'static,
    F: FlatMapFunction<T, R>,
{
    output: Stream<R>,
    function: F,
    _input: std::marker::PhantomData<fn(T)>,
}

impl<T, R, F> FlatMapStream<T, R, F>
where
    T: Send + Sync + 'static,
    R: Send + Sync + 'static,
    F: FlatMapFunction<T, R> + 'static,
{
    pub fn make(
        config: FlatMapStreamConfig,
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
    pub fn flat_map<R, F>(
        &self,
        config: impl Into<FlatMapStreamConfig>,
        function: F,
    ) -> RuntimeResult<Stream<R>>
    where
        R: Send + Sync + 'static,
        F: FlatMapFunction<T, R> + 'static,
    {
        FlatMapStream::make(config.into(), self, function)
    }
}

#[async_trait]
impl<T, R, F> Consumer<T> for FlatMapStream<T, R, F>
where
    T: Send + Sync + 'static,
    R: Send + Sync + 'static,
    F: FlatMapFunction<T, R> + 'static,
{
    async fn consume(&self, context: MessageContext, payload: Payload<T>) {
        let (context, span) = self.output.start_span(context, "stream.flatmap");
        self.function
            .flat_map(context, &self.output, payload, &self.output)
            .instrument(span)
            .await;
    }
}
