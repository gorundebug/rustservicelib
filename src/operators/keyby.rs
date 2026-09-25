use std::sync::Arc;

use async_trait::async_trait;

use crate::runtime::{
    collector::Collector,
    common::{Consumer, MessageContext, Payload, RuntimeStream},
    config::KeyByStreamConfig,
    datastruct::KeyValue,
    environment::RuntimeResult,
    serde::make_stream_key_value_serde,
    stream::Stream,
};

pub trait KeyByFunction<T, K, V>: Send + Sync
where
    T: Send + Sync + 'static,
    K: Send + Sync + 'static,
    V: Send + Sync + 'static,
{
    fn key_by(
        &self,
        context: MessageContext,
        stream: &dyn RuntimeStream,
        value: &T,
        out: &Collector<KeyValue<K, V>>,
    ) -> impl std::future::Future<Output = ()> + Send;
}

// Sharing a business function does not share operator configuration or state.
// Forward the concrete future without boxing or adding an async wrapper.
impl<T, K, V, F> KeyByFunction<T, K, V> for Arc<F>
where
    T: Send + Sync + 'static,
    K: Send + Sync + 'static,
    V: Send + Sync + 'static,
    F: KeyByFunction<T, K, V> + ?Sized,
{
    fn key_by(
        &self,
        context: MessageContext,
        stream: &dyn RuntimeStream,
        value: &T,
        out: &Collector<KeyValue<K, V>>,
    ) -> impl std::future::Future<Output = ()> + Send {
        self.as_ref().key_by(context, stream, value, out)
    }
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
    // Go: runtime.MakeKeyValueSerde[K, V](env) — fresh, resolving K and V
    // independently rather than a single generic serde over KeyValue<K, V>,
    // matching Go's key/value-split serialization.
    K: Send + Sync + 'static,
    V: Send + Sync + 'static,
    F: KeyByFunction<T, K, V> + 'static,
{
    pub fn make(
        config: &KeyByStreamConfig,
        source: &Stream<T>,
        function: F,
    ) -> RuntimeResult<Stream<KeyValue<K, V>>> {
        let serde = make_stream_key_value_serde::<K, V>(
            source.environment().make_serde::<K>(),
            source.environment().make_serde::<V>(),
        );
        let output = Stream::derived(&config.stream, source.environment().clone(), serde);
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
        config: &KeyByStreamConfig,
        function: F,
    ) -> RuntimeResult<Stream<KeyValue<K, V>>>
    where
        K: Send + Sync + 'static,
        V: Send + Sync + 'static,
        F: KeyByFunction<T, K, V> + 'static,
    {
        KeyByStream::make(config, self, function)
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
        let out = self.output.collector();
        crate::runtime::common::instrument_if_present!(
            self.function.key_by(context, &self.output, &payload, &out),
            span,
        );
    }
}
