use std::sync::Arc;

use async_trait::async_trait;
use tracing::Instrument;

use serde::{Serialize, de::DeserializeOwned};

use crate::runtime::{
    common::{Consumer, MessageContext, Payload},
    config::FlatMapIterableStreamConfig,
    environment::RuntimeResult,
    stream::Stream,
};

pub trait StreamIterable<R>: Send + Sync {
    fn stream_items(&self) -> Vec<R>;
}

impl<R> StreamIterable<R> for Vec<R>
where
    R: Clone + Send + Sync,
{
    fn stream_items(&self) -> Vec<R> {
        self.clone()
    }
}

impl<R, const N: usize> StreamIterable<R> for [R; N]
where
    R: Clone + Send + Sync,
{
    fn stream_items(&self) -> Vec<R> {
        self.to_vec()
    }
}

impl StreamIterable<char> for String {
    fn stream_items(&self) -> Vec<char> {
        self.chars().collect()
    }
}

impl StreamIterable<u8> for String {
    fn stream_items(&self) -> Vec<u8> {
        self.bytes().collect()
    }
}

pub struct FlatMapIterableStream<T, R>
where
    T: StreamIterable<R> + Send + Sync + 'static,
    R: Send + Sync + 'static,
{
    output: Stream<R>,
    _input: std::marker::PhantomData<fn(T)>,
}

impl<T, R> FlatMapIterableStream<T, R>
where
    T: StreamIterable<R> + Send + Sync + 'static,
    // Go: runtime.MakeSerde[R](env) — fresh, R (the element type) is a new
    // type at this point, distinct from the iterable T.
    R: Serialize + DeserializeOwned + Send + Sync + 'static,
{
    pub fn make(
        config: &FlatMapIterableStreamConfig,
        source: &Stream<T>,
    ) -> RuntimeResult<Stream<R>> {
        let output = Stream::new(&config.stream, source.environment().clone());
        source.try_set_consumer(
            Arc::new(Self {
                output: output.clone(),
                _input: std::marker::PhantomData,
            }),
            output.id(),
        )?;
        Ok(output)
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
        R: Serialize + DeserializeOwned + Send + Sync + 'static,
    {
        FlatMapIterableStream::make(config, self)
    }
}

#[async_trait]
impl<T, R> Consumer<T> for FlatMapIterableStream<T, R>
where
    T: StreamIterable<R> + Send + Sync + 'static,
    R: Send + Sync + 'static,
{
    async fn consume(&self, context: MessageContext, value: Payload<T>) {
        let (context, span) = self.output.start_span(context, "stream.flatmap_iterable");
        async {
            for item in value.stream_items() {
                self.output.emit(context.clone(), Payload::new(item)).await;
            }
        }
        .instrument(span)
        .await;
    }
}
