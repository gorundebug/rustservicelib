use std::sync::Arc;

use async_trait::async_trait;
use serde::{Serialize, de::DeserializeOwned};

use crate::runtime::{
    common::{ConstructionValue, Consumer, MessageContext, Payload, RuntimeStream},
    config::{CaseStreamConfig, WhenStreamConfig},
    environment::RuntimeResult,
    stream::Stream,
};

/// Selects the `WhenStream` index for a value.
///
/// This is the Rust equivalent of Go's `BuildSwitchFunction`. Rust does not
/// route by an erased runtime type by default, so generated code supplies an
/// exhaustive selector for the source enum or model.
pub trait BuildSwitchFunction<T>: Send + Sync {
    fn select(&self, value: &T) -> usize;
}

impl<T, F> BuildSwitchFunction<T> for F
where
    F: Fn(&T) -> usize + Send + Sync,
{
    fn select(&self, value: &T) -> usize {
        self(value)
    }
}

#[async_trait]
pub trait When<T>: Send + Sync
where
    T: Send + Sync + 'static,
{
    fn stream(&self) -> &dyn RuntimeStream;
    async fn consume_case(&self, context: MessageContext, value: Payload<T>);
}

pub struct WhenStream<T, R, F>
where
    T: Send + Sync + 'static,
    R: Send + Sync + 'static,
    F: Fn(&T) -> R + Send + Sync + 'static,
{
    output: Stream<R>,
    map: F,
    _input: std::marker::PhantomData<fn(T)>,
}

#[async_trait]
impl<T, R, F> When<T> for WhenStream<T, R, F>
where
    T: Send + Sync + 'static,
    R: Send + Sync + 'static,
    F: Fn(&T) -> R + Send + Sync + 'static,
{
    fn stream(&self) -> &dyn RuntimeStream {
        &self.output
    }

    async fn consume_case(&self, context: MessageContext, value: Payload<T>) {
        self.output
            .emit(context, Payload::new((self.map)(&value)))
            .await;
    }
}

pub struct CaseStream<T, F>
where
    T: Send + Sync + 'static,
    F: BuildSwitchFunction<T> + 'static,
{
    selector: F,
    when_streams: ConstructionValue<Vec<Arc<dyn When<T>>>>,
}

impl<T, F> CaseStream<T, F>
where
    T: Send + Sync + 'static,
    F: BuildSwitchFunction<T> + 'static,
{
    pub fn make(
        config: &CaseStreamConfig,
        source: &Stream<T>,
        selector: F,
    ) -> RuntimeResult<Arc<Self>> {
        let id = config.stream.id;
        let case_stream = Arc::new(Self {
            selector,
            when_streams: ConstructionValue::new(Vec::new()),
        });
        source.try_set_consumer(Arc::clone(&case_stream), id)?;
        Ok(case_stream)
    }
}

/// A case node needs the source environment before its first branch exists.
/// Keep it in a small wrapper rather than making branch registration depend on
/// global runtime state.
pub struct TypedCaseStream<T, F>
where
    T: Send + Sync + 'static,
    F: BuildSwitchFunction<T> + 'static,
{
    inner: Arc<CaseStream<T, F>>,
    environment: crate::runtime::environment::RuntimeEnvironment,
}

impl<T, F> TypedCaseStream<T, F>
where
    T: Send + Sync + 'static,
    F: BuildSwitchFunction<T> + 'static,
{
    pub fn when<R, M>(&self, config: &WhenStreamConfig, map: M) -> Stream<R>
    where
        // Go: runtime.MakeSerde[R](env) — fresh; each branch's map(&T) -> R
        // introduces a new type distinct from T (and from other branches).
        R: Serialize + DeserializeOwned + Send + Sync + 'static,
        M: Fn(&T) -> R + Send + Sync + 'static,
    {
        let output = Stream::new(&config.stream, self.environment.clone());
        self.inner.when_streams.with_mut(|branches| {
            branches.push(Arc::new(WhenStream {
                output: output.clone(),
                map,
                _input: std::marker::PhantomData,
            }));
        });
        output
    }

    pub fn len(&self) -> usize {
        self.inner.when_streams.get().len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl<T> Stream<T>
where
    T: Send + Sync + 'static,
{
    pub fn case<F>(
        &self,
        config: &CaseStreamConfig,
        selector: F,
    ) -> RuntimeResult<TypedCaseStream<T, F>>
    where
        F: BuildSwitchFunction<T> + 'static,
    {
        let environment = self.environment().clone();
        Ok(TypedCaseStream {
            inner: CaseStream::make(config, self, selector)?,
            environment,
        })
    }
}

#[async_trait]
impl<T, F> Consumer<T> for CaseStream<T, F>
where
    T: Send + Sync + 'static,
    F: BuildSwitchFunction<T> + 'static,
{
    async fn consume(&self, context: MessageContext, payload: Payload<T>) {
        let index = self.selector.select(&payload);
        let branches = self.when_streams.get();
        let branch = branches.get(index).unwrap_or_else(|| {
            panic!(
                "case selector returned branch {index}, but only {} branches exist",
                branches.len()
            )
        });
        let (context, span) = branch.stream().start_span(context, "stream.case");
        crate::runtime::common::instrument_if_enabled(branch.consume_case(context, payload), span)
            .await;
    }
}
