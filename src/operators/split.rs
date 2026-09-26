use std::sync::Arc;

use crate::runtime::{
    collector::{Collect, Collector},
    common::{ConstructionValue, Consumer, MessageContext, Payload},
    config::SplitStreamConfig,
    environment::{RuntimeBuildable, RuntimeError, RuntimeResult},
    stream::Stream,
};

/// Branch storage may be the ordinary dynamic array or a heterogeneous typed
/// list. Dispatch order and payload sharing are implemented once by SplitStream.
pub trait SplitOutputs<T>: Send + Sync
where
    T: Send + Sync + 'static,
{
    fn len(&self) -> usize;
    fn is_empty(&self) -> bool {
        self.len() == 0
    }
    fn stream(&self, index: usize) -> &Stream<T>;
    fn emit_at(
        &self,
        index: usize,
        context: MessageContext,
        payload: Payload<T>,
    ) -> impl std::future::Future<Output = ()> + Send;
}

impl<T: Send + Sync + 'static, const N: usize> SplitOutputs<T> for [Stream<T>; N] {
    fn len(&self) -> usize {
        N
    }
    fn stream(&self, index: usize) -> &Stream<T> {
        &self[index]
    }
    async fn emit_at(&self, index: usize, context: MessageContext, payload: Payload<T>) {
        self[index].emit(context, payload).await;
    }
}

impl<T: Send + Sync + 'static> SplitOutputs<T> for () {
    fn len(&self) -> usize {
        0
    }
    fn stream(&self, _index: usize) -> &Stream<T> {
        panic!("split branch index out of bounds")
    }
    async fn emit_at(&self, _index: usize, _context: MessageContext, _payload: Payload<T>) {
        panic!("split branch index out of bounds");
    }
}

impl<T, C, Rest> SplitOutputs<T> for (Collector<T, C>, Rest)
where
    T: Send + Sync + 'static,
    C: Collect<T>,
    Rest: SplitOutputs<T>,
{
    fn len(&self) -> usize {
        1 + self.1.len()
    }
    fn stream(&self, index: usize) -> &Stream<T> {
        if index == 0 {
            self.0.stream()
        } else {
            self.1.stream(index - 1)
        }
    }
    async fn emit_at(&self, index: usize, context: MessageContext, payload: Payload<T>) {
        if index == 0 {
            self.0.emit(context, payload).await;
        } else {
            self.1.emit_at(index - 1, context, payload).await;
        }
    }
}

pub struct SplitStream<T, const N: usize, B = [Stream<T>; N]>
where
    T: Send + Sync + 'static,
{
    stream: Stream<T>,
    links: B,
    dispatch_order: ConstructionValue<[usize; N]>,
}

impl<T, const N: usize> SplitStream<T, N>
where
    T: Send + Sync + 'static,
{
    pub fn make(config: &SplitStreamConfig, source: &Stream<T>) -> RuntimeResult<[Stream<T>; N]> {
        // Go: stream.GetSerde() — type-preserving, reuse the source's serde
        // for both the internal collector stream and each branch link.
        let links = Self::create_links(config, source);
        let operator = Self::from_typed(config, source, links.clone())?;
        source.try_set_consumer(operator, config.stream.id)?;
        Ok(links)
    }

    /// Create branch streams before wiring a typed graph in reverse order.
    /// These remain the actual writable Stream handles of the graph.
    pub fn create_links(config: &SplitStreamConfig, source: &Stream<T>) -> [Stream<T>; N] {
        let serde = source.get_serde();
        std::array::from_fn(|index| {
            Stream::derived_with_name(
                &config.stream,
                source.environment().clone(),
                format!("{}SplitLink{index}", config.stream.name),
                serde.clone(),
            )
        })
    }

    pub fn from_typed<B>(
        config: &SplitStreamConfig,
        source: &Stream<T>,
        links: B,
    ) -> RuntimeResult<Arc<SplitStream<T, N, B>>>
    where
        B: SplitOutputs<T> + 'static,
    {
        if links.len() != N {
            return Err(RuntimeError::InvalidConfiguration(format!(
                "split {} requires {N} branches, received {}",
                config.stream.name,
                links.len()
            )));
        }
        let operator = Arc::new(SplitStream {
            stream: Stream::derived(
                &config.stream,
                source.environment().clone(),
                source.get_serde(),
            ),
            links,
            dispatch_order: ConstructionValue::new(std::array::from_fn(|index| index)),
        });
        let buildable: Arc<dyn RuntimeBuildable> = operator.clone();
        source
            .environment()
            .register_runtime_buildable(Arc::downgrade(&buildable));
        Ok(operator)
    }
}

impl<T, const N: usize, B> RuntimeBuildable for SplitStream<T, N, B>
where
    T: Send + Sync + 'static,
    B: SplitOutputs<T> + 'static,
{
    fn build(&self) -> RuntimeResult<()> {
        let mut order = std::array::from_fn(|index| index);
        for index in 0..N {
            let link = self.links.stream(index);
            if link.link_collector().is_none() {
                return Err(RuntimeError::ConsumerNotSet {
                    stream: link.name(),
                });
            }
        }
        order.sort_by_key(|index| {
            !self
                .links
                .stream(*index)
                .link_collector()
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

impl<T, const N: usize, B> Consumer<T> for SplitStream<T, N, B>
where
    T: Send + Sync + 'static,
    B: SplitOutputs<T> + 'static,
{
    async fn consume(&self, context: MessageContext, payload: Payload<T>) {
        let (context, span) = self.stream.start_span(context, "stream.split");
        crate::runtime::common::instrument_if_present!(
            async {
                let order = self.dispatch_order.get();
                let mut context = Some(context);
                let mut payload = Some(payload);
                for (position, index) in order.iter().enumerate() {
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
                    self.links
                        .emit_at(*index, branch_context, branch_payload)
                        .await;
                }
            },
            span,
        );
    }
}

/// Create the branch handles; typed consumers are connected separately.
pub fn create<T, const N: usize>(config: &SplitStreamConfig, source: &Stream<T>) -> [Stream<T>; N]
where
    T: Send + Sync + 'static,
{
    SplitStream::<T, N>::create_links(config, source)
}
