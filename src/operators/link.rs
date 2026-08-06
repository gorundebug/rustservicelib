use std::sync::Arc;

use async_trait::async_trait;
use serde::{Serialize, de::DeserializeOwned};
use tracing::Instrument;

use crate::runtime::{
    common::{ConstructionCell, Consumer, MessageContext, Payload},
    config::CycleLinkStreamConfig,
    environment::{RuntimeEnvironment, RuntimeError, RuntimeResult},
    stream::Stream,
};

/// A stream link whose source may be connected after the rest of the graph.
///
/// This is the Rust equivalent of Go's `LinkStream`. Delayed source binding is
/// required for cyclic graphs: generated code creates the link first, builds
/// the downstream chain, and calls `set_source` only after the cycle endpoint
/// exists.
///
/// Go-aligned: like a root stream, this has no parent to propagate a serde
/// from at construction time, so it resolves one fresh.
pub struct LinkStream<T>
where
    T: Send + Sync + 'static,
{
    stream: Stream<T>,
    source: ConstructionCell<Stream<T>>,
}

impl<T> LinkStream<T>
where
    T: Serialize + DeserializeOwned + Send + Sync + 'static,
{
    pub fn make(config: &CycleLinkStreamConfig, environment: RuntimeEnvironment) -> Arc<Self> {
        Arc::new(Self {
            stream: Stream::new(&config.stream, environment),
            source: ConstructionCell::empty(),
        })
    }

    pub fn stream(&self) -> &Stream<T> {
        &self.stream
    }

    pub fn set_source(self: &Arc<Self>, source: &Stream<T>) -> RuntimeResult<()> {
        if self.source.get().is_some() {
            return Err(RuntimeError::SourceAlreadySet {
                stream: self.stream.name(),
            });
        }
        source.try_set_consumer(Arc::clone(self), self.stream.id())?;
        self.source
            .set(source.clone())
            .map_err(|_| RuntimeError::SourceAlreadySet {
                stream: self.stream.name(),
            })
    }

    pub fn source(&self) -> Option<Stream<T>> {
        self.source.get().cloned()
    }
}

#[async_trait]
impl<T> Consumer<T> for LinkStream<T>
where
    T: Send + Sync + 'static,
{
    async fn consume(&self, context: MessageContext, payload: Payload<T>) {
        let (context, span) = self.stream.start_span(context, "stream.link");
        self.stream.emit(context, payload).instrument(span).await;
    }
}
