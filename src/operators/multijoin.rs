use std::{
    hash::Hash,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

use async_trait::async_trait;
use futures::FutureExt;
use tracing::Instrument;

use serde::{Serialize, de::DeserializeOwned};

use crate::runtime::{
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
        out: &Stream<O>,
    ) -> bool;
}

pub fn downcast_join_values<T>(values: &JoinValues, index: usize) -> Vec<Payload<T>>
where
    T: Send + Sync + 'static,
{
    values
        .get(index)
        .into_iter()
        .flatten()
        .map(|value| {
            Payload::from_arc(
                Arc::clone(value)
                    .downcast::<T>()
                    .expect("multi-join value type does not match the registered input"),
            )
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
    function: Arc<F>,
    next_index: AtomicUsize,
}

impl<K, O, F> MultiJoinStream<K, O, F>
where
    K: Clone + Eq + Hash + Send + Sync + 'static,
    O: Send + Sync + 'static,
    F: MultiJoinFunction<K, O> + 'static,
{
    fn callback(&self) -> JoinCallback<K> {
        let output = self.output.clone();
        let function = Arc::clone(&self.function);
        Arc::new(move |context, key, values| {
            let output = output.clone();
            let function = Arc::clone(&function);
            async move {
                if !values.first().is_some_and(|left| !left.is_empty()) {
                    return false;
                }
                function
                    .multi_join(context, &output, key, values, &output)
                    .await
            }
            .boxed()
        })
    }

    async fn consume(&self, context: MessageContext, key: K, index: usize, value: DynValue) {
        let (context, span) = self.output.start_span(context, "stream.join");
        self.store
            .join_value(context, key, index, value, self.callback())
            .instrument(span)
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
        config: MultiJoinStreamConfig,
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
        let multi_join_stream = Arc::new(Self {
            output: Stream::new(config, left.environment().clone()),
            store,
            function: Arc::new(function),
            next_index: AtomicUsize::new(1),
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
        self.multi_join_stream
            .consume(
                context,
                payload.key.clone(),
                self.index,
                Arc::new(payload.value.clone()),
            )
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
        config: MultiJoinStreamConfig,
        function: F,
    ) -> RuntimeResult<Arc<MultiJoinStream<K, O, F>>>
    where
        O: Serialize + DeserializeOwned + Send + Sync + 'static,
        F: MultiJoinFunction<K, O> + 'static,
    {
        MultiJoinStream::make(config, self, function)
    }
}
