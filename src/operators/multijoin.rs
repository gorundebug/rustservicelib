use std::{
    hash::Hash,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

use futures::FutureExt;

use crate::runtime::{
    collector::{Collect, Collector},
    common::{Consumer, MessageContext, Payload, RuntimeStream},
    config::MultiJoinStreamConfig,
    datastruct::KeyValue,
    environment::{RuntimeEnvironment, RuntimeResult},
    store::{DynValue, HashMapJoinStorage, JoinCallback, JoinValues},
    stream::Stream,
};

pub trait MultiJoinFunction<K, O>: Send + Sync
where
    K: Send + Sync + 'static,
    O: Send + Sync + 'static,
{
    fn multi_join(
        &self,
        context: MessageContext,
        stream: &dyn RuntimeStream,
        key: K,
        values: JoinValues,
        out: &impl Collect<O>,
    ) -> impl std::future::Future<Output = bool> + Send;
}

// Sharing a business function does not share operator configuration or state.
// Forward the concrete future without boxing or adding an async wrapper.
impl<K, O, F> MultiJoinFunction<K, O> for Arc<F>
where
    K: Send + Sync + 'static,
    O: Send + Sync + 'static,
    F: MultiJoinFunction<K, O> + ?Sized,
{
    fn multi_join(
        &self,
        context: MessageContext,
        stream: &dyn RuntimeStream,
        key: K,
        values: JoinValues,
        out: &impl Collect<O>,
    ) -> impl std::future::Future<Output = bool> + Send {
        self.as_ref().multi_join(context, stream, key, values, out)
    }
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
    store: Arc<HashMapJoinStorage<K>>,
    function: Arc<F>,
    next_index: AtomicUsize,
}

impl<K, O, F> MultiJoinStream<K, O, F>
where
    K: Clone + Eq + Hash + Send + Sync + 'static,
    O: Send + Sync + 'static,
    F: MultiJoinFunction<K, O> + 'static,
{
    async fn call_function(
        output: &Stream<O>,
        function: &F,
        out: &impl Collect<O>,
        context: MessageContext,
        key: K,
        values: JoinValues,
    ) -> bool {
        if !values.first().is_some_and(|left| !left.is_empty()) {
            return false;
        }
        function.multi_join(context, output, key, values, out).await
    }

    fn make_callback<C>(
        output: Stream<O>,
        collector: Collector<O, C>,
        function: Arc<F>,
    ) -> JoinCallback<K>
    where
        C: Collect<O> + Clone + 'static,
    {
        Arc::new(move |context, key, values| {
            let output = output.clone();
            let collector = collector.clone();
            let function = Arc::clone(&function);
            async move {
                Self::call_function(&output, function.as_ref(), &collector, context, key, values)
                    .await
            }
            .boxed()
        })
    }

    async fn consume_value(
        &self,
        context: MessageContext,
        key: K,
        index: usize,
        value: DynValue,
        callback: JoinCallback<K>,
        out: &impl Collect<O>,
    ) {
        let (context, span) = self.output.start_span(context, "stream.join");
        crate::runtime::common::instrument_if_present!(
            self.store.join_value_with(
                context,
                key,
                index,
                value,
                callback,
                |context, key, values| Self::call_function(
                    &self.output,
                    self.function.as_ref(),
                    out,
                    context,
                    key,
                    values
                )
            ),
            span,
        );
    }
}

impl<K, O, F> MultiJoinStream<K, O, F>
where
    K: Clone + Eq + Hash + Send + Sync + 'static,
    // Go: runtime.MakeSerde[R](env) — fresh, O is the multi-join's output type.
    O: Send + Sync + 'static,
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
        let multi_join_stream = Self::new(config, left.environment().clone(), function)?;
        multi_join_stream.connect_left(left, multi_join_stream.output.collector())?;
        Ok(multi_join_stream)
    }

    pub fn new(
        config: &MultiJoinStreamConfig,
        environment: RuntimeEnvironment,
        function: F,
    ) -> RuntimeResult<Arc<Self>> {
        let stream_id = config.stream.id;
        let stream_name = config.stream.name.clone();
        let hashmap_storage = Arc::new(HashMapJoinStorage::from_stream(
            environment.clone(),
            stream_id,
        ));
        hashmap_storage.configure_metrics(&environment, &stream_name)?;
        environment.register_storage(hashmap_storage.clone());
        let output = Stream::new(&config.stream, environment);
        let function = Arc::new(function);
        Ok(Arc::new(Self {
            output,
            store: hashmap_storage,
            function,
            next_index: AtomicUsize::new(1),
        }))
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
        self.add_with_collector(source, self.output.collector())
            .map(|_| ())
    }

    pub fn connect_left<V, C>(
        self: &Arc<Self>,
        source: &Stream<KeyValue<K, V>>,
        collector: Collector<O, C>,
    ) -> RuntimeResult<
        Collector<KeyValue<K, V>, impl Collect<KeyValue<K, V>> + Clone + use<K, V, O, F, C>>,
    >
    where
        V: Clone + Send + Sync + 'static,
        C: Collect<O> + Clone + 'static,
    {
        source.try_set_typed_consumer(self.make_link::<V, C>(0, collector), self.output.id())
    }

    pub fn add_with_collector<V, C>(
        self: &Arc<Self>,
        source: &Stream<KeyValue<K, V>>,
        collector: Collector<O, C>,
    ) -> RuntimeResult<
        Collector<KeyValue<K, V>, impl Collect<KeyValue<K, V>> + Clone + use<K, V, O, F, C>>,
    >
    where
        V: Clone + Send + Sync + 'static,
        C: Collect<O> + Clone + 'static,
    {
        let index = self.next_index.fetch_add(1, Ordering::Relaxed);
        source.try_set_typed_consumer(self.make_link::<V, C>(index, collector), self.output.id())
    }

    fn make_link<V, C>(
        self: &Arc<Self>,
        index: usize,
        collector: Collector<O, C>,
    ) -> Arc<MultiJoinLinkStream<K, V, O, F, C>>
    where
        V: Clone + Send + Sync + 'static,
        C: Collect<O> + Clone + 'static,
    {
        let callback = Self::make_callback(
            self.output.clone(),
            collector.clone(),
            Arc::clone(&self.function),
        );
        Arc::new(MultiJoinLinkStream {
            multi_join_stream: Arc::clone(self),
            index,
            collector,
            callback,
            _value: std::marker::PhantomData,
        })
    }
}

pub struct MultiJoinLinkStream<K, V, O, F, C = Stream<O>>
where
    K: Clone + Eq + Hash + Send + Sync + 'static,
    V: Clone + Send + Sync + 'static,
    O: Send + Sync + 'static,
    F: MultiJoinFunction<K, O> + 'static,
    C: Collect<O> + Clone + 'static,
{
    multi_join_stream: Arc<MultiJoinStream<K, O, F>>,
    index: usize,
    collector: Collector<O, C>,
    callback: JoinCallback<K>,
    _value: std::marker::PhantomData<fn(V)>,
}

impl<K, V, O, F, C> Consumer<KeyValue<K, V>> for MultiJoinLinkStream<K, V, O, F, C>
where
    K: Clone + Eq + Hash + Send + Sync + 'static,
    V: Clone + Send + Sync + 'static,
    O: Send + Sync + 'static,
    F: MultiJoinFunction<K, O> + 'static,
    C: Collect<O> + Clone + 'static,
{
    async fn consume(&self, context: MessageContext, payload: Payload<KeyValue<K, V>>) {
        let KeyValue { key, value } = payload.into_value();
        self.multi_join_stream
            .consume_value(
                context,
                key,
                self.index,
                Arc::new(value),
                Arc::clone(&self.callback),
                &self.collector,
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
        config: &MultiJoinStreamConfig,
        function: F,
    ) -> RuntimeResult<Arc<MultiJoinStream<K, O, F>>>
    where
        O: Send + Sync + 'static,
        F: MultiJoinFunction<K, O> + 'static,
    {
        MultiJoinStream::make(config, self, function)
    }
}
