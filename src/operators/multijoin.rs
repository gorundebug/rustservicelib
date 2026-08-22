use std::{
    hash::Hash,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

use async_trait::async_trait;
use futures::FutureExt;

use serde::{Serialize, de::DeserializeOwned};

use crate::runtime::{
    collector::Collector,
    common::{Consumer, MessageContext, Payload, RuntimeStream},
    config::MultiJoinStreamConfig,
    datastruct::KeyValue,
    environment::RuntimeResult,
    store::{DynValue, HashMapJoinStorage, JoinCallback, JoinStorage, JoinValues},
    stream::Stream,
};

#[async_trait]
pub trait MultiJoinFunction<K, O>: Send + Sync
where
    K: Send + Sync + 'static,
    O: Send + Sync + 'static,
{
    async fn multi_join(
        &self,
        context: MessageContext,
        stream: &dyn RuntimeStream,
        key: K,
        values: JoinValues,
        out: &Collector<O>,
    ) -> bool;
}

pub fn downcast_join_values<T>(values: &JoinValues, index: usize) -> Vec<T>
where
    T: Clone + Send + Sync + 'static,
{
    values
        .get(index)
        .into_iter()
        .flatten()
        .map(|value| {
            (*Arc::clone(value)
                .downcast::<T>()
                .expect("multi-join value type does not match the registered input"))
            .clone()
        })
        .collect()
}

pub struct MultiJoinStream<K, O, F>
where
    K: Clone + Eq + Hash + Send + Sync + 'static,
    O: Send + Sync + 'static,
    F: MultiJoinFunction<K, O> + 'static,
{
    output: Stream<O>,
    store: Arc<dyn JoinStorage<K>>,
    callback: JoinCallback<K>,
    next_index: AtomicUsize,
    _function: std::marker::PhantomData<fn(F)>,
}

impl<K, O, F> MultiJoinStream<K, O, F>
where
    K: Clone + Eq + Hash + Send + Sync + 'static,
    O: Send + Sync + 'static,
    F: MultiJoinFunction<K, O> + 'static,
{
    fn make_callback(output: Stream<O>, function: Arc<F>) -> JoinCallback<K> {
        Arc::new(move |context, key, values| {
            let output = output.clone();
            let function = Arc::clone(&function);
            async move {
                if !values.first().is_some_and(|left| !left.is_empty()) {
                    return false;
                }
                let out = output.collector();
                function
                    .multi_join(context, &output, key, values, &out)
                    .await
            }
            .boxed()
        })
    }

    async fn consume(&self, context: MessageContext, key: K, index: usize, value: DynValue) {
        let (context, span) = self.output.start_span(context, "stream.join");
        crate::runtime::common::instrument_if_enabled(
            self.store
                .join_value(context, key, index, value, Arc::clone(&self.callback)),
            span,
        )
        .await;
    }
}

impl<K, O, F> MultiJoinStream<K, O, F>
where
    K: Clone + Eq + Hash + Send + Sync + 'static,
    // Go: runtime.MakeSerde[R](env) — fresh, O is the multi-join's output type.
    O: Serialize + DeserializeOwned + Send + Sync + 'static,
    F: MultiJoinFunction<K, O> + 'static,
{
    pub fn make<V>(
        config: &MultiJoinStreamConfig,
        left: &Stream<KeyValue<K, V>>,
        function: F,
    ) -> RuntimeResult<Arc<Self>>
    where
        V: Clone + Send + Sync + 'static,
    {
        let stream_id = config.stream.id;
        let stream_name = config.stream.name.clone();
        let hashmap_storage = Arc::new(HashMapJoinStorage::from_stream(
            left.environment().clone(),
            stream_id,
        ));
        hashmap_storage.configure_metrics(left.environment(), &stream_name)?;
        left.environment().register_storage(hashmap_storage.clone());
        let store: Arc<dyn JoinStorage<K>> = hashmap_storage;
        let output = Stream::new(&config.stream, left.environment().clone());
        let function = Arc::new(function);
        let callback = Self::make_callback(output.clone(), Arc::clone(&function));
        let multi_join_stream = Arc::new(Self {
            output,
            store,
            callback,
            next_index: AtomicUsize::new(1),
            _function: std::marker::PhantomData,
        });
        left.try_set_consumer(
            Arc::new(MultiJoinLinkStream {
                multi_join_stream: Arc::clone(&multi_join_stream),
                index: 0,
                _value: std::marker::PhantomData,
            }),
            multi_join_stream.output.id(),
        )?;
        Ok(multi_join_stream)
    }
}

impl<K, O, F> MultiJoinStream<K, O, F>
where
    K: Clone + Eq + Hash + Send + Sync + 'static,
    O: Send + Sync + 'static,
    F: MultiJoinFunction<K, O> + 'static,
{
    pub fn stream(&self) -> &Stream<O> {
        &self.output
    }

    pub fn add<V>(self: &Arc<Self>, source: &Stream<KeyValue<K, V>>) -> RuntimeResult<()>
    where
        V: Clone + Send + Sync + 'static,
    {
        let index = self.next_index.fetch_add(1, Ordering::Relaxed);
        source.try_set_consumer(
            Arc::new(MultiJoinLinkStream {
                multi_join_stream: Arc::clone(self),
                index,
                _value: std::marker::PhantomData,
            }),
            self.output.id(),
        )
    }
}

pub struct MultiJoinLinkStream<K, V, O, F>
where
    K: Clone + Eq + Hash + Send + Sync + 'static,
    V: Clone + Send + Sync + 'static,
    O: Send + Sync + 'static,
    F: MultiJoinFunction<K, O> + 'static,
{
    multi_join_stream: Arc<MultiJoinStream<K, O, F>>,
    index: usize,
    _value: std::marker::PhantomData<fn(V)>,
}

#[async_trait]
impl<K, V, O, F> Consumer<KeyValue<K, V>> for MultiJoinLinkStream<K, V, O, F>
where
    K: Clone + Eq + Hash + Send + Sync + 'static,
    V: Clone + Send + Sync + 'static,
    O: Send + Sync + 'static,
    F: MultiJoinFunction<K, O> + 'static,
{
    async fn consume(&self, context: MessageContext, payload: Payload<KeyValue<K, V>>) {
        let KeyValue { key, value } = payload.into_value();
        self.multi_join_stream
            .consume(context, key, self.index, Arc::new(value))
            .await;
    }
}

impl<K, V> Stream<KeyValue<K, V>>
where
    K: Clone + Eq + Hash + Send + Sync + 'static,
    V: Clone + Send + Sync + 'static,
{
    pub fn multi_join<O, F>(
        &self,
        config: &MultiJoinStreamConfig,
        function: F,
    ) -> RuntimeResult<Arc<MultiJoinStream<K, O, F>>>
    where
        O: Serialize + DeserializeOwned + Send + Sync + 'static,
        F: MultiJoinFunction<K, O> + 'static,
    {
        MultiJoinStream::make(config, self, function)
    }
}
