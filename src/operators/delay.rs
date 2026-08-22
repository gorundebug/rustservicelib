use std::{sync::Arc, time::Duration};

use async_trait::async_trait;

use crate::runtime::{
    collector::Collector,
    common::{Consumer, MessageContext, Payload, RuntimeStream},
    config::DelayStreamConfig,
    environment::{RuntimeError, RuntimeResult},
    stream::Stream,
};

#[async_trait]
pub trait DelayFunction<T>: Send + Sync
where
    T: Send + Sync + 'static,
{
    async fn duration(
        &self,
        context: MessageContext,
        stream: &dyn RuntimeStream,
        value: &T,
    ) -> Duration;

    async fn delay_error(
        &self,
        _context: MessageContext,
        _stream: &dyn RuntimeStream,
        _value: &T,
        _error: RuntimeError,
        _out: &Collector<T>,
    ) {
    }
}

pub struct DelayStream<T, F>
where
    T: Send + Sync + 'static,
    F: DelayFunction<T>,
{
    output: Stream<T>,
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
    pub fn delay<F>(&self, config: &DelayStreamConfig, function: F) -> RuntimeResult<Stream<T>>
    where
        F: DelayFunction<T> + 'static,
    {
        DelayStream::make(config, self, function)
    }
}

#[async_trait]
impl<T, F> Consumer<T> for DelayStream<T, F>
where
    T: Send + Sync + 'static,
    F: DelayFunction<T> + 'static,
{
    async fn consume(&self, context: MessageContext, payload: Payload<T>) {
        let (context, span) = self.output.start_span(context, "stream.delay");
        let event_span = span.clone();
        crate::runtime::common::instrument_if_enabled(
            async {
                let duration = self
                    .function
                    .duration(context.clone(), &self.output, &payload)
                    .await;
                if duration.is_zero() {
                    // Go deliberately emits a non-positive delay even for an already
                    // cancelled context.
                    self.output.emit(context, payload).await;
                    return;
                }
                let output = self.output.clone();
                let delayed_context = context.clone();
                let error_context = context.clone();
                let (error_payload, payload) = payload.share();
                let delayed_span = event_span.clone();
                let scheduled = self
                    .output
                    .environment()
                    .delay_pool()
                    .delay(context, duration, async move {
                        crate::runtime::common::instrument_if_enabled(
                            async {
                                if delayed_context.is_cancelled() {
                                    tracing::event!(
                                        name: "delay.skipped",
                                        tracing::Level::WARN,
                                        reason = "context canceled"
                                    );
                                    return;
                                }
                                output.emit(delayed_context, payload).await;
                            },
                            delayed_span,
                        )
                        .await;
                    })
                    .await;
                if let Err(error) = scheduled {
                    tracing::event!(
                        name: "delay.skipped",
                        parent: &event_span,
                        tracing::Level::WARN,
                        error = %error,
                        reason = "delay_pool_rejected",
                        "delay skipped"
                    );
                    let out = self.output.collector();
                    self.function
                        .delay_error(error_context, &self.output, &error_payload, error, &out)
                        .await;
                }
            },
            span,
        )
        .await;
    }
}
