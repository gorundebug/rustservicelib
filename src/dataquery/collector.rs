use crate::runtime::{common::{MessageContext, Payload}, stream::Stream,
    environment::metrics::Int64Counter};

/// Query-local collector. Delegates to the ordinary graph collector, without
/// adding instrumentation to non-Query streams or changing delivery semantics.
pub struct Collector<T: Send + Sync + 'static> {
    inner: crate::runtime::collector::Collector<T>,
    results: Option<Int64Counter>,
}
impl<T: Send + Sync + 'static> Collector<T> {
    pub(crate) fn new(stream: Stream<T>, results: Option<Int64Counter>) -> Self {
        Self { inner: crate::runtime::collector::Collector::from_stream(stream), results }
    }
    pub fn stream(&self) -> &Stream<T> { self.inner.stream() }
    pub fn is_async(&self) -> bool { self.inner.is_async() }
    pub async fn collect(&self, context: MessageContext, value: T) {
        if let Some(counter) = &self.results { counter.inc(); }
        self.inner.collect(context, value).await;
    }
    pub async fn emit(&self, context: MessageContext, value: Payload<T>) {
        if let Some(counter) = &self.results { counter.inc(); }
        self.inner.emit(context, value).await;
    }
}
