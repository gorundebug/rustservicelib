use std::{sync::Arc, time::Duration};

use async_trait::async_trait;
use tracing::Instrument;

use crate::runtime::{
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
        value: Payload<T>,
    ) -> Duration;

    async fn delay_error(
        &self,
        _context: MessageContext,
        _stream: &dyn RuntimeStream,
        _value: Payload<T>,
        _error: RuntimeError,
        _out: &Stream<T>,
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
        async {
            let (duration_payload, payload) = payload.share();
            let duration = self
                .function
                .duration(context.clone(), &self.output, duration_payload)
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
            let scheduled = self
                .output
                .environment()
                .delay_pool()
                .delay(context, duration, async move {
                    if !delayed_context.is_cancelled() {
                        output.emit(delayed_context, payload).await;
                    }
                })
                .await;
            if let Err(error) = scheduled {
                tracing::warn!(
                    event.name = "delay.skipped",
                    error = %error,
                    reason = "delay_pool_rejected",
                    "delay skipped"
                );
                self.function
                    .delay_error(
                        error_context,
                        &self.output,
                        error_payload,
                        error,
                        &self.output,
                    )
                    .await;
            }
        }
        .instrument(span)
        .await;
    }
}
