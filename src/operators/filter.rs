use std::sync::Arc;

use crate::runtime::{
    collector::{Collect, Collector},
    common::{Consumer, MessageContext, Payload, RuntimeStream},
    config::FilterStreamConfig,
    environment::RuntimeResult,
    stream::Stream,
};

/// Create the output handle, inheriting the source's serde.
/// Consumer connections are installed separately.
pub fn create<T>(config: &FilterStreamConfig, source: &Stream<T>) -> Stream<T>
where
    T: Send + Sync + 'static,
{
    Stream::derived(
        &config.stream,
        source.environment().clone(),
        source.get_serde(),
    )
}

pub trait FilterFunction<T>: Send + Sync
where
    T: Send + Sync + 'static,
{
    fn filter(
        &self,
        context: MessageContext,
        stream: &dyn RuntimeStream,
        value: &T,
    ) -> impl std::future::Future<Output = bool> + Send;
}

// Sharing a business function does not share operator configuration or state.
// Forward the concrete future without boxing or adding an async wrapper.
impl<T, F> FilterFunction<T> for Arc<F>
where
    T: Send + Sync + 'static,
    F: FilterFunction<T> + ?Sized,
{
    fn filter(
        &self,
        context: MessageContext,
        stream: &dyn RuntimeStream,
        value: &T,
    ) -> impl std::future::Future<Output = bool> + Send {
        self.as_ref().filter(context, stream, value)
    }
}

pub struct FilterStream<T, F, C = Collector<T>>
where
    T: Send + Sync + 'static,
    F: FilterFunction<T>,
{
    output: Stream<T>,
    collector: C,
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
        let output = create(config, source);
        let operator = Arc::new(Self {
            collector: output.collector(),
            output: output.clone(),
            function,
        });
        source.try_set_consumer(operator, output.id())?;
        Ok(output)
    }

    pub fn from_collector<C>(
        output: Collector<T, C>,
        function: F,
    ) -> FilterStream<T, F, Collector<T, C>>
    where
        C: Collect<T>,
    {
        FilterStream {
            output: output.stream().clone(),
            collector: output,
            function,
        }
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

impl<T, F, C> Consumer<T> for FilterStream<T, F, C>
where
    T: Send + Sync + 'static,
    F: FilterFunction<T> + 'static,
    C: Collect<T>,
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
                    self.collector.out_payload(context, payload).await;
                }
            },
            span,
        );
    }
}
