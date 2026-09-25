use std::sync::Arc;

use async_trait::async_trait;

use crate::runtime::{
    collector::Collector,
    common::{Consumer, MessageContext, Payload, RuntimeStream},
    config::MapStreamConfig,
    environment::RuntimeResult,
    stream::Stream,
};

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
        out: &Collector<R>,
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
        out: &Collector<R>,
    ) -> impl std::future::Future<Output = ()> + Send {
        self.as_ref().map(context, stream, value, out)
    }
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
    R: Send + Sync + 'static,
    F: MapFunction<T, R> + 'static,
{
    pub fn make(
        config: &MapStreamConfig,
        source: &Stream<T>,
        function: F,
    ) -> RuntimeResult<Stream<R>> {
        let output = Stream::new(&config.stream, source.environment().clone());
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
    pub fn map<R, F>(&self, config: &MapStreamConfig, function: F) -> RuntimeResult<Stream<R>>
    where
        R: Send + Sync + 'static,
        F: MapFunction<T, R> + 'static,
    {
        MapStream::make(config, self, function)
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
        let out = self.output.collector();
        crate::runtime::common::instrument_if_present!(
            self.function.map(context, &self.output, &payload, &out),
            span,
        );
    }
}
