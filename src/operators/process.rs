use std::sync::Arc;

use super::error::ErrorStream;
use crate::runtime::{
    collector::{Collect, Collector},
    common::{Consumer, MessageContext, Payload, RuntimeStream},
    config::ProcessStreamConfig,
    environment::RuntimeResult,
    stream::Stream,
};

pub trait ProcessFunction<T, R, E>: Send + Sync
where
    T: Send + Sync + 'static,
    R: Send + Sync + 'static,
    E: Send + Sync + 'static,
{
    fn process(
        &self,
        context: MessageContext,
        stream: &dyn RuntimeStream,
        value: &T,
        out: &impl Collect<R>,
        error: &impl Collect<E>,
    ) -> impl std::future::Future<Output = ()> + Send;
}

// Sharing a business function does not share operator configuration or state.
// Forward the concrete future without boxing or adding an async wrapper.
impl<T, R, E, F> ProcessFunction<T, R, E> for Arc<F>
where
    T: Send + Sync + 'static,
    R: Send + Sync + 'static,
    E: Send + Sync + 'static,
    F: ProcessFunction<T, R, E> + ?Sized,
{
    fn process(
        &self,
        context: MessageContext,
        stream: &dyn RuntimeStream,
        value: &T,
        out: &impl Collect<R>,
        error: &impl Collect<E>,
    ) -> impl std::future::Future<Output = ()> + Send {
        self.as_ref().process(context, stream, value, out, error)
    }
}

pub struct ProcessStream<T, R, E, F, C = Stream<R>, D = Stream<E>>
where
    T: Send + Sync + 'static,
    R: Send + Sync + 'static,
    E: Send + Sync + 'static,
    F: ProcessFunction<T, R, E>,
    C: Collect<R>,
    D: Collect<E>,
{
    output: Stream<R>,
    collector: Collector<R, C>,
    error: Collector<E, D>,
    function: F,
    _input: std::marker::PhantomData<fn(T)>,
}

impl<T, R, E, F> ProcessStream<T, R, E, F>
where
    T: Send + Sync + 'static,
    // Go-aligned: output serde (R) and error serde (E) are both freshly
    // resolved (Go: runtime.MakeSerde[R](env) / MakeErrorStream[E](id, env)) —
    // both are new types at this point in the graph, not the input type T.
    R: Send + Sync + 'static,
    E: Send + Sync + 'static,
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
            Arc::new(Self::from_collectors(
                output.collector(),
                error.collector(),
                function,
            )),
            output.id(),
        )?;
        Ok((output, error))
    }

    pub fn from_collectors<C, D>(
        collector: Collector<R, C>,
        error: Collector<E, D>,
        function: F,
    ) -> ProcessStream<T, R, E, F, C, D>
    where
        C: Collect<R>,
        D: Collect<E>,
    {
        ProcessStream {
            output: collector.stream().clone(),
            collector,
            error,
            function,
            _input: std::marker::PhantomData,
        }
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
        R: Send + Sync + 'static,
        E: Send + Sync + 'static,
        F: ProcessFunction<T, R, E> + 'static,
    {
        ProcessStream::make(config, self, function)
    }
}

impl<T, R, E, F, C, D> Consumer<T> for ProcessStream<T, R, E, F, C, D>
where
    T: Send + Sync + 'static,
    R: Send + Sync + 'static,
    E: Send + Sync + 'static,
    F: ProcessFunction<T, R, E> + 'static,
    C: Collect<R>,
    D: Collect<E>,
{
    async fn consume(&self, context: MessageContext, payload: Payload<T>) {
        let (context, span) = self.output.start_span(context, "stream.process");
        crate::runtime::common::instrument_if_present!(
            self.function.process(
                context,
                &self.output,
                &payload,
                &self.collector,
                &self.error
            ),
            span,
        );
    }
}
