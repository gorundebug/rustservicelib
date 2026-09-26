use std::sync::Arc;

use crate::runtime::{
    collector::{Collect, Collector},
    common::{Consumer, MessageContext, Payload, RuntimeStream},
    config::MapStreamConfig,
    environment::RuntimeResult,
    stream::Stream,
};

/// Create the output handle without connecting a consumer.
/// Both fluent and statically wired graphs resolve the output type's serde here.
pub fn create<R>(
    config: &MapStreamConfig,
    environment: crate::runtime::environment::RuntimeEnvironment,
) -> Stream<R>
where
    R: Send + Sync + 'static,
{
    Stream::new(&config.stream, environment)
}

pub trait MapFunction<T, R>: Send + Sync
where
    T: Send + Sync + 'static,
    R: Send + Sync + 'static,
{
    fn map(
        &self,
        context: MessageContext,
        stream: &dyn RuntimeStream,
        value: &T,
        out: &impl Collect<R>,
    ) -> impl std::future::Future<Output = ()> + Send;
}

// Sharing a business function does not share operator configuration or state.
// Forward the concrete future without boxing or adding an async wrapper.
impl<T, R, F> MapFunction<T, R> for Arc<F>
where
    T: Send + Sync + 'static,
    R: Send + Sync + 'static,
    F: MapFunction<T, R> + ?Sized,
{
    fn map(
        &self,
        context: MessageContext,
        stream: &dyn RuntimeStream,
        value: &T,
        out: &impl Collect<R>,
    ) -> impl std::future::Future<Output = ()> + Send {
        self.as_ref().map(context, stream, value, out)
    }
}

pub struct MapStream<T, R, F, C = Collector<R>>
where
    T: Send + Sync + 'static,
    R: Send + Sync + 'static,
    F: MapFunction<T, R>,
{
    output: Stream<R>,
    collector: C,
    function: F,
    _input: std::marker::PhantomData<fn(T)>,
}

impl<T, R, F> MapStream<T, R, F>
where
    T: Send + Sync + 'static,
    // Go: runtime.MakeSerde[R](env) — fresh, R is a new type at this point.
    R: Send + Sync + 'static,
    F: MapFunction<T, R> + 'static,
{
    pub fn make(
        config: &MapStreamConfig,
        source: &Stream<T>,
        function: F,
    ) -> RuntimeResult<Stream<R>> {
        let output = create(config, source.environment().clone());
        let operator = Arc::new(Self {
            collector: output.collector(),
            output: output.clone(),
            function,
            _input: std::marker::PhantomData,
        });
        source.try_set_consumer(operator, output.id())?;
        Ok(output)
    }

    pub fn from_collector<C>(
        output: Collector<R, C>,
        function: F,
    ) -> MapStream<T, R, F, Collector<R, C>>
    where
        C: Collect<R>,
    {
        MapStream {
            output: output.stream().clone(),
            collector: output,
            function,
            _input: std::marker::PhantomData,
        }
    }
}

impl<T> Stream<T>
where
    T: Send + Sync + 'static,
{
    pub fn map<R, F>(&self, config: &MapStreamConfig, function: F) -> RuntimeResult<Stream<R>>
    where
        R: Send + Sync + 'static,
        F: MapFunction<T, R> + 'static,
    {
        MapStream::make(config, self, function)
    }
}

impl<T, R, F, C> Consumer<T> for MapStream<T, R, F, C>
where
    T: Send + Sync + 'static,
    R: Send + Sync + 'static,
    F: MapFunction<T, R> + 'static,
    C: Collect<R>,
{
    async fn consume(&self, context: MessageContext, payload: Payload<T>) {
        let (context, span) = self.output.start_span(context, "stream.map");
        crate::runtime::common::instrument_if_present!(
            self.function
                .map(context, &self.output, &payload, &self.collector),
            span,
        );
    }
}
