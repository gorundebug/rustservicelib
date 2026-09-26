use std::sync::Arc;

use crate::runtime::{
    collector::{Collect, Collector},
    common::{Consumer, MessageContext, Payload},
    config::MergeStreamConfig,
    environment::RuntimeResult,
    stream::Stream,
};

/// Create the output handle, inheriting the first source's serde.
/// Consumer connections are installed separately.
pub fn create<T>(config: &MergeStreamConfig, source: &Stream<T>) -> Stream<T>
where
    T: Send + Sync + 'static,
{
    Stream::derived(
        &config.stream,
        source.environment().clone(),
        source.get_serde(),
    )
}

pub struct MergeStream<T, C = Collector<T>>
where
    T: Send + Sync + 'static,
{
    output: Stream<T>,
    collector: C,
}

impl<T> MergeStream<T>
where
    T: Send + Sync + 'static,
{
    pub fn make(config: &MergeStreamConfig, sources: &[Stream<T>]) -> RuntimeResult<Stream<T>> {
        assert!(!sources.is_empty(), "merge needs at least one source");
        let output = create(config, &sources[0]);
        let operator = Arc::new(Self {
            collector: output.collector(),
            output: output.clone(),
        });
        for source in sources {
            source.try_set_consumer(Arc::clone(&operator), output.id())?;
        }
        Ok(output)
    }

    pub fn from_collector<C>(output: Collector<T, C>) -> MergeStream<T, Collector<T, C>>
    where
        C: Collect<T>,
    {
        MergeStream {
            output: output.stream().clone(),
            collector: output,
        }
    }
}

impl<T> Stream<T>
where
    T: Send + Sync + 'static,
{
    pub fn merge(
        &self,
        config: &MergeStreamConfig,
        streams: &[Stream<T>],
    ) -> RuntimeResult<Stream<T>> {
        let mut sources = Vec::with_capacity(streams.len() + 1);
        sources.push(self.clone());
        sources.extend_from_slice(streams);
        MergeStream::make(config, &sources)
    }
}

impl<T, C> Consumer<T> for MergeStream<T, C>
where
    T: Send + Sync + 'static,
    C: Collect<T>,
{
    async fn consume(&self, context: MessageContext, payload: Payload<T>) {
        let (context, span) = self.output.start_span(context, "stream.merge");
        crate::runtime::common::instrument_if_present!(
            self.collector.out_payload(context, payload),
            span
        );
    }
}
