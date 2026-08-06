use std::sync::Arc;

use async_trait::async_trait;
use tracing::Instrument;

use crate::runtime::{
    common::{ConstructionValue, Consumer, MessageContext, Payload},
    config::SplitStreamConfig,
    environment::{RuntimeBuildable, RuntimeError, RuntimeResult},
    stream::Stream,
};

pub struct SplitStream<T, const N: usize>
where
    T: Send + Sync + 'static,
{
    stream: Stream<T>,
    links: [Stream<T>; N],
    dispatch_order: ConstructionValue<[usize; N]>,
}

impl<T, const N: usize> SplitStream<T, N>
where
    T: Send + Sync + 'static,
{
    pub fn make(config: &SplitStreamConfig, source: &Stream<T>) -> RuntimeResult<[Stream<T>; N]> {
        // Go: stream.GetSerde() — type-preserving, reuse the source's serde
        // for both the internal collector stream and each branch link.
        let serde = source.get_serde();
        let stream = Stream::derived(&config.stream, source.environment().clone(), serde.clone());
        let links = std::array::from_fn(|index| {
            Stream::derived_with_name(
                &config.stream,
                source.environment().clone(),
                format!("{}SplitLink{index}", config.stream.name),
                serde.clone(),
            )
        });
        let operator = Arc::new(Self {
            stream,
            links: links.clone(),
            dispatch_order: ConstructionValue::new(std::array::from_fn(|index| index)),
        });
        let buildable: Arc<dyn RuntimeBuildable> = operator.clone();
        source
            .environment()
            .register_runtime_buildable(Arc::downgrade(&buildable));
        source.try_set_consumer(operator, config.stream.id)?;
        Ok(links)
    }
}

impl<T, const N: usize> RuntimeBuildable for SplitStream<T, N>
where
    T: Send + Sync + 'static,
{
    fn build(&self) -> RuntimeResult<()> {
        let mut order = std::array::from_fn(|index| index);
        for link in &self.links {
            if link.collector().is_none() {
                return Err(RuntimeError::ConsumerNotSet {
                    stream: link.name(),
                });
            }
        }
        order.sort_by_key(|index| {
            !self.links[*index]
                .collector()
                .expect("split link was validated")
                .is_async()
        });
        self.dispatch_order
            .with_mut(|dispatch_order| *dispatch_order = order);
        Ok(())
    }
}

impl<T> Stream<T>
where
    T: Send + Sync + 'static,
{
    pub fn split<const N: usize>(
        &self,
        config: &SplitStreamConfig,
    ) -> RuntimeResult<[Stream<T>; N]> {
        SplitStream::make(config, self)
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
            let order = self.dispatch_order.get();
            let mut context = Some(context);
            let mut payload = Some(payload);
            for (position, index) in order.iter().enumerate() {
                let link = &self.links[*index];
                let last = position + 1 == N;
                let branch_context = if last {
                    context.take().expect("split context is available")
                } else {
                    context
                        .as_ref()
                        .expect("split context is available")
                        .clone()
                };
                let branch_payload = if last {
                    payload.take().expect("split payload is available")
                } else {
                    let (branch, remaining) =
                        payload.take().expect("split payload is available").share();
                    payload = Some(remaining);
                    branch
                };
                link.emit(branch_context, branch_payload).await;
            }
        }
        .instrument(span)
        .await;
    }
}
