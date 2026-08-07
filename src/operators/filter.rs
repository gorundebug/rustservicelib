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
        config: &FilterStreamConfig,
        source: &Stream<T>,
        function: F,
    ) -> RuntimeResult<Stream<T>> {
        // Go: stream.GetSerde() — type-preserving, reuse the source's serde.
        let output = Stream::derived(
            &config.stream,
            source.environment().clone(),
            source.get_serde(),
        );
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
    pub fn filter<F>(&self, config: &FilterStreamConfig, function: F) -> RuntimeResult<Stream<T>>
    where
        F: FilterFunction<T> + 'static,
    {
        FilterStream::make(config, self, function)
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
            let (function_payload, payload) = payload.share();
            if self
                .function
                .filter(context.clone(), &self.output, function_payload)
                .await
            {
                self.output.emit(context, payload).await;
            }
        }
        .instrument(span)
        .await;
    }
}
