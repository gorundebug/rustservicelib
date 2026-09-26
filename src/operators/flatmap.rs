use std::sync::Arc;

use crate::runtime::{
    collector::{Collect, Collector},
    common::{Consumer, MessageContext, Payload, RuntimeStream},
    config::FlatMapStreamConfig,
    environment::RuntimeResult,
    stream::Stream,
};

/// Create the output handle without connecting a consumer.
/// Both fluent and statically wired graphs resolve the output type's serde here.
pub fn create<R>(
    config: &FlatMapStreamConfig,
    environment: crate::runtime::environment::RuntimeEnvironment,
) -> Stream<R>
where
    R: Send + Sync + 'static,
{
    Stream::new(&config.stream, environment)
}

pub trait FlatMapFunction<T, R>: Send + Sync
where
    T: Send + Sync + 'static,
    R: Send + Sync + 'static,
{
    fn flat_map(
        &self,
        context: MessageContext,
        stream: &dyn RuntimeStream,
        value: &T,
        out: &impl Collect<R>,
    ) -> impl std::future::Future<Output = ()> + Send;
}

// Sharing a business function does not share operator configuration or state.
// Forward the concrete future without boxing or adding an async wrapper.
impl<T, R, F> FlatMapFunction<T, R> for Arc<F>
where
    T: Send + Sync + 'static,
    R: Send + Sync + 'static,
    F: FlatMapFunction<T, R> + ?Sized,
{
    fn flat_map(
        &self,
        context: MessageContext,
        stream: &dyn RuntimeStream,
        value: &T,
        out: &impl Collect<R>,
    ) -> impl std::future::Future<Output = ()> + Send {
        self.as_ref().flat_map(context, stream, value, out)
    }
}

pub struct FlatMapStream<T, R, F, C = Collector<R>>
where
    T: Send + Sync + 'static,
    R: Send + Sync + 'static,
    F: FlatMapFunction<T, R>,
{
    output: Stream<R>,
    collector: C,
    function: F,
    _input: std::marker::PhantomData<fn(T)>,
}

impl<T, R, F> FlatMapStream<T, R, F>
where
    T: Send + Sync + 'static,
    // Go: runtime.MakeSerde[R](env) — fresh, R is a new type at this point.
    R: Send + Sync + 'static,
    F: FlatMapFunction<T, R> + 'static,
{
    pub fn make(
        config: &FlatMapStreamConfig,
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
    ) -> FlatMapStream<T, R, F, Collector<R, C>>
    where
        C: Collect<R>,
    {
        FlatMapStream {
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
    pub fn flat_map<R, F>(
        &self,
        config: &FlatMapStreamConfig,
        function: F,
    ) -> RuntimeResult<Stream<R>>
    where
        R: Send + Sync + 'static,
        F: FlatMapFunction<T, R> + 'static,
    {
        FlatMapStream::make(config, self, function)
    }
}

impl<T, R, F, C> Consumer<T> for FlatMapStream<T, R, F, C>
where
    T: Send + Sync + 'static,
    R: Send + Sync + 'static,
    F: FlatMapFunction<T, R> + 'static,
    C: Collect<R>,
{
    async fn consume(&self, context: MessageContext, payload: Payload<T>) {
        let (context, span) = self.output.start_span(context, "stream.flatmap");
        crate::runtime::common::instrument_if_present!(
            self.function
                .flat_map(context, &self.output, &payload, &self.collector),
            span,
        );
    }
}
