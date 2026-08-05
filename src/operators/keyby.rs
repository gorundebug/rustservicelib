use std::sync::Arc;

use async_trait::async_trait;
use serde::{Serialize, de::DeserializeOwned};
use tracing::Instrument;

use crate::runtime::{
    common::{Consumer, MessageContext, Payload, RuntimeStream},
    config::KeyByStreamConfig,
    datastruct::KeyValue,
    environment::RuntimeResult,
    serde::{JsonSerde, Serde as ServiceSerde, make_stream_key_value_serde},
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
    // Go: runtime.MakeKeyValueSerde[K, V](env) — fresh, resolving K and V
    // independently rather than a single generic serde over KeyValue<K, V>,
    // matching Go's key/value-split serialization.
    K: Serialize + DeserializeOwned + Send + Sync + 'static,
    V: Serialize + DeserializeOwned + Send + Sync + 'static,
    F: KeyByFunction<T, K, V> + 'static,
{
    pub fn make(
        config: &KeyByStreamConfig,
        source: &Stream<T>,
        function: F,
    ) -> RuntimeResult<Stream<KeyValue<K, V>>> {
        let serde = make_stream_key_value_serde::<K, V>(
            Arc::new(JsonSerde::<K>::new()) as Arc<dyn ServiceSerde<K>>,
            Arc::new(JsonSerde::<V>::new()) as Arc<dyn ServiceSerde<V>>,
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
        K: Serialize + DeserializeOwned + Send + Sync + 'static,
        V: Serialize + DeserializeOwned + Send + Sync + 'static,
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
        self.function
            .key_by(context, &self.output, payload, &self.output)
            .instrument(span)
            .await;
    }
}
