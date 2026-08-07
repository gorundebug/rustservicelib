use serde::{Serialize, de::DeserializeOwned};

use crate::runtime::{
    common::{MessageContext, Payload},
    config::StreamConfig,
    environment::RuntimeEnvironment,
    stream::Stream,
};

/// Virtual error output owned by a `ProcessStream`.
///
/// The negative ID follows Go's `ErrorStream`: it prevents the error output
/// from colliding with the process node while still allowing link semantics to
/// be configured independently.
///
/// Go: MakeErrorStream[E](id, env) — always resolves a fresh serde for E; this
/// is a root with no parent to propagate from.
#[derive(Clone)]
pub struct ErrorStream<E>
where
    E: Send + Sync + 'static,
{
    stream: Stream<E>,
}

impl<E> ErrorStream<E>
where
    E: Serialize + DeserializeOwned + Send + Sync + 'static,
{
    pub fn new(owner: &StreamConfig, environment: RuntimeEnvironment) -> Self {
        Self {
            stream: Stream::with_ids(-owner.id, owner.id, environment),
        }
    }
}

impl<E> ErrorStream<E>
where
    E: Send + Sync + 'static,
{
    pub fn stream(&self) -> &Stream<E> {
        &self.stream
    }

    pub async fn emit(&self, context: MessageContext, payload: Payload<E>) {
        self.stream.emit(context, payload).await;
    }
}
