use std::sync::Arc;

use async_trait::async_trait;
use tracing::Instrument;

use crate::runtime::{
    common::{Consumer, MessageContext, Payload},
    config::MergeStreamConfig,
    environment::RuntimeResult,
    stream::Stream,
};

pub struct MergeStream<T>
where
    T: Send + Sync + 'static,
{
    output: Stream<T>,
}

impl<T> MergeStream<T>
where
    T: Send + Sync + 'static,
{
    pub fn make(config: MergeStreamConfig, sources: &[Stream<T>]) -> RuntimeResult<Stream<T>> {
        assert!(!sources.is_empty(), "merge needs at least one source");
        // Go: stream.GetSerde() — type-preserving, reuse the first source's serde.
        let output = Stream::derived(
            config,
            sources[0].environment().clone(),
            sources[0].get_serde(),
        );
        let operator = Arc::new(Self {
            output: output.clone(),
        });
        for source in sources {
            source.try_set_consumer(Arc::clone(&operator), output.id())?;
        }
        Ok(output)
    }
}

impl<T> Stream<T>
where
    T: Send + Sync + 'static,
{
    pub fn merge(
        &self,
        config: impl Into<MergeStreamConfig>,
        streams: &[Stream<T>],
    ) -> RuntimeResult<Stream<T>> {
        let mut sources = Vec::with_capacity(streams.len() + 1);
        sources.push(self.clone());
        sources.extend_from_slice(streams);
        MergeStream::make(config.into(), &sources)
    }
}

#[async_trait]
impl<T> Consumer<T> for MergeStream<T>
where
    T: Send + Sync + 'static,
{
    async fn consume(&self, context: MessageContext, payload: Payload<T>) {
        let (context, span) = self.output.start_span(context, "stream.merge");
        self.output.emit(context, payload).instrument(span).await;
    }
}
