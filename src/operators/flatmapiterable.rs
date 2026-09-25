use std::sync::Arc;

use crate::runtime::{
    collector::{Collect, Collector},
    common::{Consumer, MessageContext, Payload},
    config::FlatMapIterableStreamConfig,
    environment::RuntimeResult,
    stream::Stream,
};

pub trait StreamIterable<R>: Clone + Send + Sync {
    type Items: IntoIterator<Item = R>;

    fn into_stream_items(self) -> Self::Items;
}

impl<R> StreamIterable<R> for Vec<R>
where
    R: Clone + Send + Sync,
{
    type Items = std::vec::IntoIter<R>;

    fn into_stream_items(self) -> Self::Items {
        self.into_iter()
    }
}

impl<R, const N: usize> StreamIterable<R> for [R; N]
where
    R: Clone + Send + Sync,
{
    type Items = std::array::IntoIter<R, N>;

    fn into_stream_items(self) -> Self::Items {
        self.into_iter()
    }
}

impl StreamIterable<char> for String {
    type Items = std::vec::IntoIter<char>;

    fn into_stream_items(self) -> Self::Items {
        self.chars().collect::<Vec<_>>().into_iter()
    }
}

impl StreamIterable<u8> for String {
    type Items = std::vec::IntoIter<u8>;

    fn into_stream_items(self) -> Self::Items {
        self.into_bytes().into_iter()
    }
}

pub struct FlatMapIterableStream<T, R, C = Collector<R>>
where
    T: StreamIterable<R> + Send + Sync + 'static,
    R: Send + Sync + 'static,
    C: Collect<R>,
{
    output: Stream<R>,
    collector: C,
    _input: std::marker::PhantomData<fn(T)>,
}

impl<T, R> FlatMapIterableStream<T, R>
where
    T: StreamIterable<R> + Send + Sync + 'static,
    // Go: runtime.MakeSerde[R](env) — fresh, R (the element type) is a new
    // type at this point, distinct from the iterable T.
    R: Send + Sync + 'static,
    <T::Items as IntoIterator>::IntoIter: Send,
{
    pub fn make(
        config: &FlatMapIterableStreamConfig,
        source: &Stream<T>,
    ) -> RuntimeResult<Stream<R>> {
        let output = Stream::new(&config.stream, source.environment().clone());
        source.try_set_consumer(
            Arc::new(Self::from_collector(output.collector())),
            output.id(),
        )?;
        Ok(output)
    }

    pub fn from_collector<C: Collect<R>>(
        collector: Collector<R, C>,
    ) -> FlatMapIterableStream<T, R, Collector<R, C>> {
        FlatMapIterableStream {
            output: collector.stream().clone(),
            collector,
            _input: std::marker::PhantomData,
        }
    }
}

impl<T> Stream<T>
where
    T: Send + Sync + 'static,
{
    pub fn flat_map_iterable<R>(
        &self,
        config: &FlatMapIterableStreamConfig,
    ) -> RuntimeResult<Stream<R>>
    where
        T: StreamIterable<R>,
        R: Send + Sync + 'static,
        <T::Items as IntoIterator>::IntoIter: Send,
    {
        FlatMapIterableStream::make(config, self)
    }
}

impl<T, R, C> Consumer<T> for FlatMapIterableStream<T, R, C>
where
    T: StreamIterable<R> + Send + Sync + 'static,
    R: Send + Sync + 'static,
    C: Collect<R>,
    <T::Items as IntoIterator>::IntoIter: Send,
{
    async fn consume(&self, context: MessageContext, value: Payload<T>) {
        let (context, span) = self.output.start_span(context, "stream.flatmap_iterable");
        crate::runtime::common::instrument_if_present!(
            async {
                let mut items = value
                    .into_value()
                    .into_stream_items()
                    .into_iter()
                    .peekable();
                let mut context = Some(context);
                while let Some(item) = items.next() {
                    let item_context = if items.peek().is_none() {
                        context.take().expect("flat-map context is available")
                    } else {
                        context
                            .as_ref()
                            .expect("flat-map context is available")
                            .clone()
                    };
                    self.collector.emit(item_context, Payload::new(item)).await;
                }
            },
            span,
        );
    }
}
