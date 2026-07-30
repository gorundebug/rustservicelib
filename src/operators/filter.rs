use std::sync::Arc;

use async_trait::async_trait;
use tracing::Instrument;

use crate::runtime::{
    common::{Consumer, MessageContext, Payload, RuntimeStream},
    config::FilterStreamConfig,
    environment::RuntimeResult,
    stream::Stream,
};

#[async_trait]
pub trait FilterFunction<T>: Send + Sync
where
    T: Send + Sync + 'static,
{
    async fn filter(
        &self,
        context: MessageContext,
        stream: &dyn RuntimeStream,
        value: Payload<T>,
    ) -> bool;
}

pub struct FilterStream<T, F>
where
    T: Send + Sync + 'static,
    F: FilterFunction<T>,
{
    output: Stream<T>,
    function: F,
}

impl<T, F> FilterStream<T, F>
where
    T: Send + Sync + 'static,
    F: FilterFunction<T> + 'static,
{
    pub fn make(
        config: FilterStreamConfig,
        source: &Stream<T>,
        function: F,
    ) -> RuntimeResult<Stream<T>> {
        let output = Stream::new(config, source.environment().clone());
        let operator = Arc::new(Self {
            output: output.clone(),
            function,
        });
        source.try_set_consumer(operator, output.id())?;
        Ok(output)
    }
}

impl<T> Stream<T>
where
    T: Send + Sync + 'static,
{
    pub fn filter<F>(
        &self,
        config: impl Into<FilterStreamConfig>,
        function: F,
    ) -> RuntimeResult<Stream<T>>
    where
        F: FilterFunction<T> + 'static,
    {
        FilterStream::make(config.into(), self, function)
    }
}

#[async_trait]
impl<T, F> Consumer<T> for FilterStream<T, F>
where
    T: Send + Sync + 'static,
    F: FilterFunction<T> + 'static,
{
    async fn consume(&self, context: MessageContext, payload: Payload<T>) {
        let (context, span) = self.output.start_span(context, "stream.filter");
        async {
            if self
                .function
                .filter(context.clone(), &self.output, payload.clone())
                .await
            {
                self.output.emit(context, payload).await;
            }
        }
        .instrument(span)
        .await;
    }
}
