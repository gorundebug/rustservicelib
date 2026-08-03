use std::sync::{Arc, RwLock};

use async_trait::async_trait;
use serde::{Serialize, de::DeserializeOwned};
use tracing::Instrument;

use crate::runtime::{
    common::{Consumer, MessageContext, Payload, RuntimeStream},
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
    when_streams: RwLock<Vec<Arc<dyn When<T>>>>,
}

impl<T, F> CaseStream<T, F>
where
    T: Send + Sync + 'static,
    F: BuildSwitchFunction<T> + 'static,
{
    pub fn make(
        config: CaseStreamConfig,
        source: &Stream<T>,
        selector: F,
    ) -> RuntimeResult<Arc<Self>> {
        let id = config.stream.id;
        let case_stream = Arc::new(Self {
            selector,
            when_streams: RwLock::new(Vec::new()),
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
    pub fn when<R, M>(&self, config: impl Into<WhenStreamConfig>, map: M) -> Stream<R>
    where
        // Go: runtime.MakeSerde[R](env) — fresh; each branch's map(&T) -> R
        // introduces a new type distinct from T (and from other branches).
        R: Serialize + DeserializeOwned + Send + Sync + 'static,
        M: Fn(&T) -> R + Send + Sync + 'static,
    {
        let output = Stream::new(config.into(), self.environment.clone());
        self.inner
            .when_streams
            .write()
            .expect("case branches lock poisoned")
            .push(Arc::new(WhenStream {
                output: output.clone(),
                map,
                _input: std::marker::PhantomData,
            }));
        output
    }

    pub fn len(&self) -> usize {
        self.inner
            .when_streams
            .read()
            .expect("case branches lock poisoned")
            .len()
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
        config: impl Into<CaseStreamConfig>,
        selector: F,
    ) -> RuntimeResult<TypedCaseStream<T, F>>
    where
        F: BuildSwitchFunction<T> + 'static,
    {
        let environment = self.environment().clone();
        Ok(TypedCaseStream {
            inner: CaseStream::make(config.into(), self, selector)?,
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
        let (branch, branch_count) = {
            let when_streams = self
                .when_streams
                .read()
                .expect("case when streams lock poisoned");
            (when_streams.get(index).cloned(), when_streams.len())
        };
        let branch = branch.unwrap_or_else(|| {
            panic!("case selector returned branch {index}, but only {branch_count} branches exist")
        });
        let (context, span) = branch.stream().start_span(context, "stream.case");
        branch.consume_case(context, payload).instrument(span).await;
    }
}
