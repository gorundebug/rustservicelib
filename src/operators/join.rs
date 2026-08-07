use std::{hash::Hash, sync::Arc};

use async_trait::async_trait;
use futures::FutureExt;
use tracing::Instrument;

use serde::{Serialize, de::DeserializeOwned};

use crate::runtime::{
    common::{Consumer, MessageContext, Payload, RuntimeStream},
    config::{JoinStreamConfig, JoinType},
    datastruct::KeyValue,
    environment::RuntimeResult,
    store::{DynValue, HashMapJoinStorage, JoinCallback, JoinStorage, JoinValues},
    stream::Stream,
};

#[async_trait]
pub trait JoinFunction<K, L, R, O>: Send + Sync
where
    K: Send + Sync + 'static,
    L: Clone + Send + Sync + 'static,
    R: Clone + Send + Sync + 'static,
    O: Send + Sync + 'static,
{
    async fn join(
        &self,
        context: MessageContext,
        stream: &dyn RuntimeStream,
        key: K,
        left: Vec<Payload<L>>,
        right: Vec<Payload<R>>,
        out: &Stream<O>,
    ) -> bool;
}

pub struct JoinStream<K, L, R, O, F>
where
    K: Clone + Eq + Hash + Send + Sync + 'static,
    L: Clone + Send + Sync + 'static,
    R: Clone + Send + Sync + 'static,
    O: Send + Sync + 'static,
    F: JoinFunction<K, L, R, O> + 'static,
{
    output: Stream<O>,
    store: Arc<dyn JoinStorage<K>>,
    callback: JoinCallback<K>,
    _types: std::marker::PhantomData<fn(L, R, F)>,
}

impl<K, L, R, O, F> JoinStream<K, L, R, O, F>
where
    K: Clone + Eq + Hash + Send + Sync + 'static,
    L: Clone + Send + Sync + 'static,
    R: Clone + Send + Sync + 'static,
    O: Send + Sync + 'static,
    F: JoinFunction<K, L, R, O> + 'static,
{
    fn make_callback(output: Stream<O>, function: Arc<F>) -> JoinCallback<K> {
        Arc::new(move |context, key, values| {
            let output = output.clone();
            let function = Arc::clone(&function);
            async move {
                let join_type = match output.config().as_ref() {
                    crate::runtime::config::RuntimeStreamConfig::Join(config) => config.join_type,
                    _ => JoinType::Undefined,
                };
                let can_call = match join_type {
                    JoinType::Inner => {
                        values.first().is_some_and(|values| !values.is_empty())
                            && values.get(1).is_some_and(|values| !values.is_empty())
                    }
                    JoinType::Left => values.first().is_some_and(|values| !values.is_empty()),
                    JoinType::Right => values.get(1).is_some_and(|values| !values.is_empty()),
                    JoinType::Outer => true,
                    JoinType::Undefined => false,
                };
                if !can_call {
                    return false;
                }
                let left = downcast_values::<L>(&values, 0);
                let right = downcast_values::<R>(&values, 1);
                function
                    .join(context, &output, key, left, right, &output)
                    .await
            }
            .boxed()
        })
    }

    async fn consume_value(&self, context: MessageContext, key: K, index: usize, value: DynValue) {
        let (context, span) = self.output.start_span(context, "stream.join");
        self.store
            .join_value(context, key, index, value, Arc::clone(&self.callback))
            .instrument(span)
            .await;
    }
}

impl<K, L, R, O, F> JoinStream<K, L, R, O, F>
where
    K: Clone + Eq + Hash + Send + Sync + 'static,
    L: Clone + Send + Sync + 'static,
    R: Clone + Send + Sync + 'static,
    // Go: runtime.MakeSerde[R](env) — fresh, O is the join's output type,
    // distinct from either input side.
    O: Serialize + DeserializeOwned + Send + Sync + 'static,
    F: JoinFunction<K, L, R, O> + 'static,
{
    pub fn make(
        config: &JoinStreamConfig,
        left: &Stream<KeyValue<K, L>>,
        right: &Stream<KeyValue<K, R>>,
        function: F,
    ) -> RuntimeResult<Stream<O>> {
        let stream_id = config.stream.id;
        let stream_name = config.stream.name.clone();
        let output = Stream::new(&config.stream, left.environment().clone());
        let hashmap_storage = Arc::new(HashMapJoinStorage::from_stream(
            left.environment().clone(),
            stream_id,
        ));
        hashmap_storage.configure_metrics(left.environment(), &stream_name)?;
        left.environment().register_storage(hashmap_storage.clone());
        let store: Arc<dyn JoinStorage<K>> = hashmap_storage;
        let function = Arc::new(function);
        let callback = Self::make_callback(output.clone(), Arc::clone(&function));
        let join_stream = Arc::new(Self {
            output: output.clone(),
            store,
            callback,
            _types: std::marker::PhantomData,
        });
        left.try_set_consumer(Arc::clone(&join_stream), output.id())?;
        right.try_set_consumer(Arc::new(JoinLink { join_stream }), output.id())?;
        Ok(output)
    }
}

fn downcast_values<T>(values: &JoinValues, index: usize) -> Vec<Payload<T>>
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
                    .expect("join storage value type does not match its input stream"),
            )
        })
        .collect()
}

pub struct JoinLink<K, L, R, O, F>
where
    K: Clone + Eq + Hash + Send + Sync + 'static,
    L: Clone + Send + Sync + 'static,
    R: Clone + Send + Sync + 'static,
    O: Send + Sync + 'static,
    F: JoinFunction<K, L, R, O> + 'static,
{
    join_stream: Arc<JoinStream<K, L, R, O, F>>,
}

#[async_trait]
impl<K, L, R, O, F> Consumer<KeyValue<K, L>> for JoinStream<K, L, R, O, F>
where
    K: Clone + Eq + Hash + Send + Sync + 'static,
    L: Clone + Send + Sync + 'static,
    R: Clone + Send + Sync + 'static,
    O: Send + Sync + 'static,
    F: JoinFunction<K, L, R, O> + 'static,
{
    async fn consume(&self, context: MessageContext, payload: Payload<KeyValue<K, L>>) {
        let KeyValue { key, value } = payload.into_value();
        let value: DynValue = Arc::new(value);
        self.consume_value(context, key, 0, value).await;
    }
}

#[async_trait]
impl<K, L, R, O, F> Consumer<KeyValue<K, R>> for JoinLink<K, L, R, O, F>
where
    K: Clone + Eq + Hash + Send + Sync + 'static,
    L: Clone + Send + Sync + 'static,
    R: Clone + Send + Sync + 'static,
    O: Send + Sync + 'static,
    F: JoinFunction<K, L, R, O> + 'static,
{
    async fn consume(&self, context: MessageContext, payload: Payload<KeyValue<K, R>>) {
        let KeyValue { key, value } = payload.into_value();
        let value: DynValue = Arc::new(value);
        self.join_stream.consume_value(context, key, 1, value).await;
    }
}

impl<K, L> Stream<KeyValue<K, L>>
where
    K: Clone + Eq + Hash + Send + Sync + 'static,
    L: Clone + Send + Sync + 'static,
{
    pub fn join<R, O, F>(
        &self,
        config: &JoinStreamConfig,
        right: &Stream<KeyValue<K, R>>,
        function: F,
    ) -> RuntimeResult<Stream<O>>
    where
        R: Clone + Send + Sync + 'static,
        O: Serialize + DeserializeOwned + Send + Sync + 'static,
        F: JoinFunction<K, L, R, O> + 'static,
    {
        JoinStream::make(config, self, right, function)
    }
}
