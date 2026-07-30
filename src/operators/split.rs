use std::sync::Arc;

use async_trait::async_trait;
use tracing::Instrument;

use crate::runtime::{
    common::{Consumer, MessageContext, Payload},
    config::SplitStreamConfig,
    environment::RuntimeResult,
    stream::Stream,
};

pub struct SplitStream<T, const N: usize>
where
    T: Send + Sync + 'static,
{
    stream: Stream<T>,
    links: [Stream<T>; N],
}

impl<T, const N: usize> SplitStream<T, N>
where
    T: Send + Sync + 'static,
{
    pub fn make(config: SplitStreamConfig, source: &Stream<T>) -> RuntimeResult<[Stream<T>; N]> {
        let stream = Stream::new(config.clone(), source.environment().clone());
        let links = std::array::from_fn(|index| {
            Stream::with_name(
                config.clone(),
                source.environment().clone(),
                format!("{}SplitLink{index}", config.stream.name),
            )
        });
        let operator = Arc::new(Self {
            stream,
            links: links.clone(),
        });
        source.try_set_consumer(operator, config.stream.id)?;
        Ok(links)
    }
}

impl<T> Stream<T>
where
    T: Send + Sync + 'static,
{
    pub fn split<const N: usize>(
        &self,
        config: impl Into<SplitStreamConfig>,
    ) -> RuntimeResult<[Stream<T>; N]> {
        SplitStream::make(config.into(), self)
    }
}

#[async_trait]
impl<T, const N: usize> Consumer<T> for SplitStream<T, N>
where
    T: Send + Sync + 'static,
{
    async fn consume(&self, context: MessageContext, payload: Payload<T>) {
        let (context, span) = self.stream.start_span(context, "stream.split");
        async {
            // Go's Build orders asynchronous branches before direct branches.
            let mut links: Vec<_> = self.links.iter().collect();
            links.sort_by_key(|link| {
                !link
                    .collector()
                    .is_some_and(|collector| collector.is_async())
            });
            for link in links {
                link.emit(context.clone(), payload.clone()).await;
            }
        }
        .instrument(span)
        .await;
    }
}
