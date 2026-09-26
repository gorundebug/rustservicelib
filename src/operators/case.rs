use std::sync::Arc;

use async_trait::async_trait;

use crate::runtime::{
    collector::{Collect, Collector, short_type_name},
    common::{ConstructionValue, Consumer, MessageContext, Payload, RuntimeStream},
    config::{CaseStreamConfig, WhenStreamConfig},
    environment::{CallStatistics, RuntimeError, RuntimeResult},
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

pub struct WhenStream<T>
where
    T: Send + Sync + 'static,
{
    output: Stream<T>,
}

#[async_trait]
impl<T> When<T> for WhenStream<T>
where
    T: Send + Sync + 'static,
{
    fn stream(&self) -> &dyn RuntimeStream {
        &self.output
    }

    async fn consume_case(&self, context: MessageContext, value: Payload<T>) {
        self.output.emit(context, value).await;
    }
}

/// Branch collections keep concrete collector types on statically connected
/// graphs. The construction-time vector remains the compatible dynamic path.
pub trait CaseBranches<T>: Send + Sync
where
    T: Send + Sync + 'static,
{
    fn len(&self) -> usize;
    fn is_empty(&self) -> bool {
        self.len() == 0
    }
    fn stream(&self, index: usize) -> Option<&dyn RuntimeStream>;
    fn consume_case(
        &self,
        index: usize,
        context: MessageContext,
        value: Payload<T>,
    ) -> impl std::future::Future<Output = ()> + Send;
}

impl<T> CaseBranches<T> for Vec<Arc<dyn When<T>>>
where
    T: Send + Sync + 'static,
{
    fn len(&self) -> usize {
        Vec::len(self)
    }
    fn stream(&self, index: usize) -> Option<&dyn RuntimeStream> {
        self.get(index).map(|branch| branch.stream())
    }
    async fn consume_case(&self, index: usize, context: MessageContext, value: Payload<T>) {
        self[index].consume_case(context, value).await;
    }
}

impl<T> CaseBranches<T> for ()
where
    T: Send + Sync + 'static,
{
    fn len(&self) -> usize {
        0
    }
    fn stream(&self, _: usize) -> Option<&dyn RuntimeStream> {
        None
    }
    async fn consume_case(&self, _: usize, _: MessageContext, _: Payload<T>) {
        unreachable!("CaseStream validates the selected branch before dispatch")
    }
}

impl<T, C, Rest> CaseBranches<T> for (Collector<T, C>, Rest)
where
    T: Send + Sync + 'static,
    C: Collect<T>,
    Rest: CaseBranches<T>,
{
    fn len(&self) -> usize {
        1 + self.1.len()
    }
    fn stream(&self, index: usize) -> Option<&dyn RuntimeStream> {
        if index == 0 {
            Some(self.0.stream())
        } else {
            self.1.stream(index - 1)
        }
    }
    async fn consume_case(&self, index: usize, context: MessageContext, value: Payload<T>) {
        if index == 0 {
            self.0.emit(context, value).await;
        } else {
            self.1.consume_case(index - 1, context, value).await;
        }
    }
}

pub struct CaseStream<T, F, B = Vec<Arc<dyn When<T>>>>
where
    T: Send + Sync + 'static,
    F: BuildSwitchFunction<T> + 'static,
    B: CaseBranches<T>,
{
    selector: Arc<F>,
    when_streams: ConstructionValue<B>,
    _input: std::marker::PhantomData<fn(T)>,
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
        let case_stream = Self::create(config, source, selector);
        source.try_set_consumer(Arc::clone(&case_stream), id)?;
        Ok(case_stream)
    }

    fn create(config: &CaseStreamConfig, source: &Stream<T>, selector: F) -> Arc<Self> {
        let id = config.stream.id;
        source.environment().register_runtime_stream(id);
        Arc::new(Self {
            selector: Arc::new(selector),
            when_streams: ConstructionValue::new(Vec::new()),
            _input: std::marker::PhantomData,
        })
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
    id: i32,
}

impl<T, F> TypedCaseStream<T, F>
where
    T: Send + Sync + 'static,
    F: BuildSwitchFunction<T> + 'static,
{
    pub fn create_links(config: &CaseStreamConfig, source: &Stream<T>, selector: F) -> Self {
        Self {
            inner: CaseStream::create(config, source, selector),
            environment: source.environment().clone(),
            id: config.stream.id,
        }
    }

    pub fn from_branches<B>(&self, branches: B) -> RuntimeResult<Arc<CaseStream<T, F, B>>>
    where
        B: CaseBranches<T> + 'static,
    {
        let expected = self.inner.when_streams.get();
        if expected.len() != branches.len()
            || expected.iter().enumerate().any(|(index, branch)| {
                branches.stream(index).map(|stream| stream.id()) != Some(branch.stream().id())
            })
        {
            return Err(RuntimeError::InvalidConfiguration(format!(
                "case {} typed branches must match the registered When streams in order",
                self.id
            )));
        }
        Ok(Arc::new(CaseStream {
            selector: Arc::clone(&self.inner.selector),
            when_streams: ConstructionValue::new(branches),
            _input: std::marker::PhantomData,
        }))
    }

    pub fn when(&self, config: &WhenStreamConfig) -> Stream<T> {
        let output = Stream::new(&config.stream, self.environment.clone());
        self.environment.register_graph_link(
            self.id,
            config.stream.id,
            self.environment.call_semantics(self.id, config.stream.id),
            short_type_name::<T>(),
            CallStatistics::default(),
        );
        self.inner.when_streams.with_mut(|branches| {
            branches.push(Arc::new(WhenStream {
                output: output.clone(),
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
            id: config.stream.id,
        })
    }
}

impl<T, F, B> Consumer<T> for CaseStream<T, F, B>
where
    T: Send + Sync + 'static,
    F: BuildSwitchFunction<T> + 'static,
    B: CaseBranches<T> + 'static,
{
    async fn consume(&self, context: MessageContext, payload: Payload<T>) {
        let index = self.selector.select(&payload);
        let branches = self.when_streams.get();
        let branch = branches.stream(index).unwrap_or_else(|| {
            panic!(
                "case selector returned branch {index}, but only {} branches exist",
                branches.len()
            )
        });
        let (context, span) = branch.start_span(context, "stream.case");
        crate::runtime::common::instrument_if_present!(
            branches.consume_case(index, context, payload),
            span
        );
    }
}

/// Create the case handle before registering its branches and typed consumers.
pub fn create<T, F>(
    config: &CaseStreamConfig,
    source: &Stream<T>,
    selector: F,
) -> TypedCaseStream<T, F>
where
    T: Send + Sync + 'static,
    F: BuildSwitchFunction<T> + 'static,
{
    TypedCaseStream::create_links(config, source, selector)
}

/// Register a branch using the same semantics as the fluent case API.
pub fn when<T, F>(config: &WhenStreamConfig, source: &TypedCaseStream<T, F>) -> Stream<T>
where
    T: Send + Sync + 'static,
    F: BuildSwitchFunction<T> + 'static,
{
    source.when(config)
}
