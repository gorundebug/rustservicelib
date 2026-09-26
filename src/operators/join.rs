use std::{hash::Hash, sync::Arc};

use futures::FutureExt;

use crate::runtime::{
    collector::{Collect, Collector},
    common::{Consumer, MessageContext, Payload, RuntimeStream},
    config::{JoinStreamConfig, JoinType},
    datastruct::KeyValue,
    environment::RuntimeResult,
    store::{DynValue, HashMapJoinStorage, JoinCallback, JoinValues},
    stream::Stream,
};

/// Create the output handle without connecting a consumer.
/// Both fluent and statically wired graphs resolve the output type's serde here.
pub fn create<R>(
    config: &JoinStreamConfig,
    environment: crate::runtime::environment::RuntimeEnvironment,
) -> Stream<R>
where
    R: Send + Sync + 'static,
{
    Stream::new(&config.stream, environment)
}

pub trait JoinFunction<K, L, R, O>: Send + Sync
where
    K: Send + Sync + 'static,
    L: Clone + Send + Sync + 'static,
    R: Clone + Send + Sync + 'static,
    O: Send + Sync + 'static,
{
    fn join(
        &self,
        context: MessageContext,
        stream: &dyn RuntimeStream,
        key: K,
        left: Vec<L>,
        right: Vec<R>,
        out: &impl Collect<O>,
    ) -> impl std::future::Future<Output = bool> + Send;
}

// Sharing a business function does not share operator configuration or state.
// Forward the concrete future without boxing or adding an async wrapper.
impl<K, L, R, O, F> JoinFunction<K, L, R, O> for Arc<F>
where
    K: Send + Sync + 'static,
    L: Clone + Send + Sync + 'static,
    R: Clone + Send + Sync + 'static,
    O: Send + Sync + 'static,
    F: JoinFunction<K, L, R, O> + ?Sized,
{
    fn join(
        &self,
        context: MessageContext,
        stream: &dyn RuntimeStream,
        key: K,
        left: Vec<L>,
        right: Vec<R>,
        out: &impl Collect<O>,
    ) -> impl std::future::Future<Output = bool> + Send {
        self.as_ref().join(context, stream, key, left, right, out)
    }
}

pub struct JoinStream<K, L, R, O, F, C = Stream<O>>
where
    K: Clone + Eq + Hash + Send + Sync + 'static,
    L: Clone + Send + Sync + 'static,
    R: Clone + Send + Sync + 'static,
    O: Send + Sync + 'static,
    F: JoinFunction<K, L, R, O> + 'static,
    C: Collect<O> + Clone + 'static,
{
    output: Stream<O>,
    collector: Collector<O, C>,
    function: Arc<F>,
    store: Arc<HashMapJoinStorage<K>>,
    callback: JoinCallback<K>,
    _types: std::marker::PhantomData<fn(L, R, F)>,
}

impl<K, L, R, O, F, C> JoinStream<K, L, R, O, F, C>
where
    K: Clone + Eq + Hash + Send + Sync + 'static,
    L: Clone + Send + Sync + 'static,
    R: Clone + Send + Sync + 'static,
    O: Send + Sync + 'static,
    F: JoinFunction<K, L, R, O> + 'static,
    C: Collect<O> + Clone + 'static,
{
    async fn call_function(
        output: &Stream<O>,
        function: &F,
        out: &Collector<O, C>,
        context: MessageContext,
        key: K,
        values: JoinValues,
    ) -> bool {
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
        function.join(context, output, key, left, right, out).await
    }

    fn make_callback(
        output: Stream<O>,
        collector: Collector<O, C>,
        function: Arc<F>,
    ) -> JoinCallback<K> {
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

    async fn consume_value(&self, context: MessageContext, key: K, index: usize, value: DynValue) {
        let (context, span) = self.output.start_span(context, "stream.join");
        crate::runtime::common::instrument_if_present!(
            self.store.join_value_with(
                context,
                key,
                index,
                value,
                Arc::clone(&self.callback),
                |context, key, values| Self::call_function(
                    &self.output,
                    self.function.as_ref(),
                    &self.collector,
                    context,
                    key,
                    values
                )
            ),
            span,
        );
    }

    pub fn right(self: &Arc<Self>) -> JoinLink<K, L, R, O, F, C> {
        JoinLink {
            join_stream: Arc::clone(self),
        }
    }
}

impl<K, L, R, O, F> JoinStream<K, L, R, O, F>
where
    K: Clone + Eq + Hash + Send + Sync + 'static,
    L: Clone + Send + Sync + 'static,
    R: Clone + Send + Sync + 'static,
    // Go: runtime.MakeSerde[R](env) — fresh, O is the join's output type,
    // distinct from either input side.
    O: Send + Sync + 'static,
    F: JoinFunction<K, L, R, O> + 'static,
{
    pub fn make(
        config: &JoinStreamConfig,
        left: &Stream<KeyValue<K, L>>,
        right: &Stream<KeyValue<K, R>>,
        function: F,
    ) -> RuntimeResult<Stream<O>> {
        let output = create(config, left.environment().clone());
        let join_stream = Self::from_collector(config, output.collector(), function)?;
        left.try_set_consumer(Arc::clone(&join_stream), output.id())?;
        right.try_set_consumer(Arc::new(join_stream.right()), output.id())?;
        Ok(output)
    }

    pub fn from_collector<C>(
        config: &JoinStreamConfig,
        collector: Collector<O, C>,
        function: F,
    ) -> RuntimeResult<Arc<JoinStream<K, L, R, O, F, C>>>
    where
        C: Collect<O> + Clone + 'static,
    {
        let output = collector.stream().clone();
        let environment = output.environment();
        let hashmap_storage = Arc::new(HashMapJoinStorage::from_stream(
            environment.clone(),
            config.stream.id,
        ));
        hashmap_storage.configure_metrics(environment, &config.stream.name)?;
        environment.register_storage(hashmap_storage.clone());
        let function = Arc::new(function);
        let callback = JoinStream::<K, L, R, O, F, C>::make_callback(
            output.clone(),
            collector.clone(),
            Arc::clone(&function),
        );
        Ok(Arc::new(JoinStream {
            output,
            collector,
            function,
            store: hashmap_storage,
            callback,
            _types: std::marker::PhantomData,
        }))
    }
}

fn downcast_values<T>(values: &JoinValues, index: usize) -> Vec<T>
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
                .expect("join storage value type does not match its input stream"))
            .clone()
        })
        .collect()
}

pub struct JoinLink<K, L, R, O, F, C = Stream<O>>
where
    K: Clone + Eq + Hash + Send + Sync + 'static,
    L: Clone + Send + Sync + 'static,
    R: Clone + Send + Sync + 'static,
    O: Send + Sync + 'static,
    F: JoinFunction<K, L, R, O> + 'static,
    C: Collect<O> + Clone + 'static,
{
    join_stream: Arc<JoinStream<K, L, R, O, F, C>>,
}

impl<K, L, R, O, F, C> Consumer<KeyValue<K, L>> for JoinStream<K, L, R, O, F, C>
where
    K: Clone + Eq + Hash + Send + Sync + 'static,
    L: Clone + Send + Sync + 'static,
    R: Clone + Send + Sync + 'static,
    O: Send + Sync + 'static,
    F: JoinFunction<K, L, R, O> + 'static,
    C: Collect<O> + Clone + 'static,
{
    async fn consume(&self, context: MessageContext, payload: Payload<KeyValue<K, L>>) {
        let KeyValue { key, value } = payload.into_value();
        let value: DynValue = Arc::new(value);
        self.consume_value(context, key, 0, value).await;
    }
}

impl<K, L, R, O, F, C> Consumer<KeyValue<K, R>> for JoinLink<K, L, R, O, F, C>
where
    K: Clone + Eq + Hash + Send + Sync + 'static,
    L: Clone + Send + Sync + 'static,
    R: Clone + Send + Sync + 'static,
    O: Send + Sync + 'static,
    F: JoinFunction<K, L, R, O> + 'static,
    C: Collect<O> + Clone + 'static,
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
        O: Send + Sync + 'static,
        F: JoinFunction<K, L, R, O> + 'static,
    {
        JoinStream::make(config, self, right, function)
    }
}
