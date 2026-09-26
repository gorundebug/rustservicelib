use std::sync::Arc;

use crate::runtime::{
    collector::{Collect, Collector},
    common::{Consumer, MessageContext, Payload, RuntimeStream},
    config::KeyByStreamConfig,
    datastruct::KeyValue,
    environment::RuntimeResult,
    serde::make_stream_key_value_serde,
    stream::Stream,
};

/// Create the output handle with independently resolved key and value serdes.
/// Consumer connections are installed separately.
pub fn create<K, V>(
    config: &KeyByStreamConfig,
    environment: crate::runtime::environment::RuntimeEnvironment,
) -> Stream<KeyValue<K, V>>
where
    K: Send + Sync + 'static,
    V: Send + Sync + 'static,
{
    let serde = make_stream_key_value_serde::<K, V>(
        environment.make_serde::<K>(),
        environment.make_serde::<V>(),
    );
    Stream::derived(&config.stream, environment, serde)
}

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
        out: &impl Collect<KeyValue<K, V>>,
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
        out: &impl Collect<KeyValue<K, V>>,
    ) -> impl std::future::Future<Output = ()> + Send {
        self.as_ref().key_by(context, stream, value, out)
    }
}

pub struct KeyByStream<T, K, V, F, C = Collector<KeyValue<K, V>>>
where
    T: Send + Sync + 'static,
    K: Send + Sync + 'static,
    V: Send + Sync + 'static,
    F: KeyByFunction<T, K, V>,
    C: Collect<KeyValue<K, V>>,
{
    output: Stream<KeyValue<K, V>>,
    collector: C,
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
        let output = create(config, source.environment().clone());
        source.try_set_consumer(
            Arc::new(Self::from_collector(output.collector(), function)),
            output.id(),
        )?;
        Ok(output)
    }

    pub fn from_collector<C: Collect<KeyValue<K, V>>>(
        collector: Collector<KeyValue<K, V>, C>,
        function: F,
    ) -> KeyByStream<T, K, V, F, Collector<KeyValue<K, V>, C>> {
        KeyByStream {
            output: collector.stream().clone(),
            collector,
            function,
            _input: std::marker::PhantomData,
        }
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

impl<T, K, V, F, C> Consumer<T> for KeyByStream<T, K, V, F, C>
where
    T: Send + Sync + 'static,
    K: Send + Sync + 'static,
    V: Send + Sync + 'static,
    F: KeyByFunction<T, K, V> + 'static,
    C: Collect<KeyValue<K, V>>,
{
    async fn consume(&self, context: MessageContext, payload: Payload<T>) {
        let (context, span) = self.output.start_span(context, "stream.keyby");
        crate::runtime::common::instrument_if_present!(
            self.function
                .key_by(context, &self.output, &payload, &self.collector),
            span,
        );
    }
}
