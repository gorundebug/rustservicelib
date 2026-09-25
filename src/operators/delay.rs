use std::{sync::Arc, time::Duration};

use crate::runtime::{
    collector::{Collect, Collector},
    common::{Consumer, MessageContext, Payload, RuntimeStream},
    config::DelayStreamConfig,
    environment::{RuntimeError, RuntimeResult},
    stream::Stream,
};

pub trait DelayFunction<T>: Send + Sync
where
    T: Send + Sync + 'static,
{
    fn duration(
        &self,
        context: MessageContext,
        stream: &dyn RuntimeStream,
        value: &T,
    ) -> impl std::future::Future<Output = Duration> + Send;

    fn delay_error(
        &self,
        _context: MessageContext,
        _stream: &dyn RuntimeStream,
        _value: &T,
        _error: RuntimeError,
        _out: &impl Collect<T>,
    ) -> impl std::future::Future<Output = ()> + Send {
        async move {
            let _inputs = (_context, _stream, _value, _error, _out);
        }
    }
}

// Sharing a business function does not share operator configuration or state.
// Forward the concrete future without boxing or adding an async wrapper.
impl<T, F> DelayFunction<T> for Arc<F>
where
    T: Send + Sync + 'static,
    F: DelayFunction<T> + ?Sized,
{
    fn duration(
        &self,
        context: MessageContext,
        stream: &dyn RuntimeStream,
        value: &T,
    ) -> impl std::future::Future<Output = Duration> + Send {
        self.as_ref().duration(context, stream, value)
    }

    fn delay_error(
        &self,
        _context: MessageContext,
        _stream: &dyn RuntimeStream,
        _value: &T,
        _error: RuntimeError,
        _out: &impl Collect<T>,
    ) -> impl std::future::Future<Output = ()> + Send {
        self.as_ref()
            .delay_error(_context, _stream, _value, _error, _out)
    }
}

pub struct DelayStream<T, F, C = Stream<T>>
where
    T: Send + Sync + 'static,
    F: DelayFunction<T>,
    C: Collect<T>,
{
    output: Stream<T>,
    collector: Collector<T, C>,
    function: F,
}

impl<T, F> DelayStream<T, F>
where
    T: Send + Sync + 'static,
    F: DelayFunction<T> + 'static,
{
    pub fn make(
        config: &DelayStreamConfig,
        source: &Stream<T>,
        function: F,
    ) -> RuntimeResult<Stream<T>> {
        // Go: stream.GetSerde() — type-preserving, reuse the source's serde.
        let output = Stream::derived(
            &config.stream,
            source.environment().clone(),
            source.get_serde(),
        );
        let operator = Arc::new(Self::from_collector(output.collector(), function));
        source.try_set_consumer(operator, output.id())?;
        Ok(output)
    }

    pub fn from_collector<C>(collector: Collector<T, C>, function: F) -> DelayStream<T, F, C>
    where
        C: Collect<T>,
    {
        DelayStream {
            output: collector.stream().clone(),
            collector,
            function,
        }
    }
}

impl<T> Stream<T>
where
    T: Send + Sync + 'static,
{
    pub fn delay<F>(&self, config: &DelayStreamConfig, function: F) -> RuntimeResult<Stream<T>>
    where
        F: DelayFunction<T> + 'static,
    {
        DelayStream::make(config, self, function)
    }
}

impl<T, F, C> Consumer<T> for DelayStream<T, F, C>
where
    T: Send + Sync + 'static,
    F: DelayFunction<T> + 'static,
    C: Collect<T> + Clone + 'static,
{
    async fn consume(&self, context: MessageContext, payload: Payload<T>) {
        let (context, span) = self.output.start_span(context, "stream.delay");
        let event_span = span;
        crate::runtime::common::instrument_if_present!(
            async {
                let duration = self
                    .function
                    .duration(context.clone(), &self.output, &payload)
                    .await;
                if duration.is_zero() {
                    // Go deliberately emits a non-positive delay even for an already
                    // cancelled context.
                    self.collector.emit(context, payload).await;
                    return;
                }
                let output = self.collector.clone();
                let delayed_context = context.clone();
                let error_context = context.clone();
                let (error_payload, payload) = payload.share();
                let delayed_span = event_span.clone();
                let trace_delayed_event = delayed_span
                    .as_ref()
                    .is_some_and(|span| !span.is_disabled());
                let scheduled = self
                    .output
                    .environment()
                    .delay_pool()
                    .delay(context, duration, async move {
                        crate::runtime::common::instrument_if_present!(
                            async {
                                if delayed_context.is_cancelled() {
                                    if trace_delayed_event {
                                        tracing::event!(
                                            name: "delay.skipped",
                                            tracing::Level::WARN,
                                            reason = "context canceled"
                                        );
                                    }
                                    return;
                                }
                                output.emit(delayed_context, payload).await;
                            },
                            delayed_span,
                        );
                    })
                    .await;
                if let Err(error) = scheduled {
                    if let Some(event_span) = event_span.as_ref() {
                        crate::runtime::common::event_if_enabled!(event_span, || tracing::event!(
                            name: "delay.skipped",
                            parent: event_span,
                            tracing::Level::WARN,
                            error = %error,
                            reason = "delay_pool_rejected",
                            "delay skipped"
                        ));
                    }
                    self.function
                        .delay_error(
                            error_context,
                            &self.output,
                            &error_payload,
                            error,
                            &self.collector,
                        )
                        .await;
                }
            },
            event_span,
        );
    }
}
