use std::sync::Arc;

use async_trait::async_trait;

use crate::runtime::{
    common::{Consumer, MessageContext, Payload, RuntimeStream},
    config::FilterStreamConfig,
    environment::RuntimeResult,
    stream::Stream,
};

pub trait FilterFunction<T>: Send + Sync
where
    T: Send + Sync + 'static,
{
    fn filter(&self, context: MessageContext, stream: &dyn RuntimeStream, value: &T) -> impl std::future::Future<Output = bool> + Send;
}

// Sharing a business function does not share operator configuration or state.
// Forward the concrete future without boxing or adding an async wrapper.
impl<T, F> FilterFunction<T> for Arc<F>
where
    T: Send + Sync + 'static,
    F: FilterFunction<T> + ?Sized,
{
    fn filter(&self, context: MessageContext, stream: &dyn RuntimeStream, value: &T) -> impl std::future::Future<Output = bool> + Send {
        self.as_ref().filter(context, stream, value)
    }
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
        crate::runtime::common::instrument_if_present!(
            async {
                if self
                    .function
                    .filter(context.clone(), &self.output, &payload)
                    .await
                {
                    self.output.emit(context, payload).await;
                }
            },
            span,
        );
    }
}
