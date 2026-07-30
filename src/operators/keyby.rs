use std::sync::Arc;

use async_trait::async_trait;
use tracing::Instrument;

use crate::runtime::{
    common::{Consumer, MessageContext, Payload, RuntimeStream},
    config::KeyByStreamConfig,
    datastruct::KeyValue,
    environment::RuntimeResult,
    stream::Stream,
};

#[async_trait]
pub trait KeyByFunction<T, K, V>: Send + Sync
where
    T: Send + Sync + 'static,
    K: Send + Sync + 'static,
    V: Send + Sync + 'static,
{
    async fn key_by(
        &self,
        context: MessageContext,
        stream: &dyn RuntimeStream,
        value: Payload<T>,
        out: &Stream<KeyValue<K, V>>,
    );
}

pub struct KeyByStream<T, K, V, F>
where
    T: Send + Sync + 'static,
    K: Send + Sync + 'static,
    V: Send + Sync + 'static,
    F: KeyByFunction<T, K, V>,
{
    output: Stream<KeyValue<K, V>>,
    function: F,
    _input: std::marker::PhantomData<fn(T)>,
}

impl<T, K, V, F> KeyByStream<T, K, V, F>
where
    T: Send + Sync + 'static,
    K: Send + Sync + 'static,
    V: Send + Sync + 'static,
    F: KeyByFunction<T, K, V> + 'static,
{
    pub fn make(
        config: KeyByStreamConfig,
        source: &Stream<T>,
        function: F,
    ) -> RuntimeResult<Stream<KeyValue<K, V>>> {
        let output = Stream::new(config, source.environment().clone());
        source.try_set_consumer(
            Arc::new(Self {
                output: output.clone(),
                function,
                _input: std::marker::PhantomData,
            }),
            output.id(),
        )?;
        Ok(output)
    }
}

impl<T> Stream<T>
where
    T: Send + Sync + 'static,
{
    pub fn key_by<K, V, F>(
        &self,
        config: impl Into<KeyByStreamConfig>,
        function: F,
    ) -> RuntimeResult<Stream<KeyValue<K, V>>>
    where
        K: Send + Sync + 'static,
        V: Send + Sync + 'static,
        F: KeyByFunction<T, K, V> + 'static,
    {
        KeyByStream::make(config.into(), self, function)
    }
}

#[async_trait]
impl<T, K, V, F> Consumer<T> for KeyByStream<T, K, V, F>
where
    T: Send + Sync + 'static,
    K: Send + Sync + 'static,
    V: Send + Sync + 'static,
    F: KeyByFunction<T, K, V> + 'static,
{
    async fn consume(&self, context: MessageContext, payload: Payload<T>) {
        let (context, span) = self.output.start_span(context, "stream.keyby");
        self.function
            .key_by(context, &self.output, payload, &self.output)
            .instrument(span)
            .await;
    }
}
