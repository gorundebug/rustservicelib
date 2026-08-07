use std::{any::Any, future::Future, pin::Pin, sync::Arc};

use async_trait::async_trait;

use super::Storage;
use crate::runtime::common::MessageContext;

pub type DynValue = Arc<dyn Any + Send + Sync>;
pub type JoinValues = Vec<Vec<DynValue>>;
pub type JoinCallback<K> = Arc<
    dyn Fn(MessageContext, K, JoinValues) -> Pin<Box<dyn Future<Output = bool> + Send + 'static>>
        + Send
        + Sync,
>;

#[async_trait]
pub trait JoinStorage<K>: Storage
where
    K: Send + Sync + 'static,
{
    async fn join_value(
        &self,
        context: MessageContext,
        key: K,
        index: usize,
        value: DynValue,
        callback: JoinCallback<K>,
    ) -> bool;

    async fn len(&self) -> usize;

    async fn is_empty(&self) -> bool {
        self.len().await == 0
    }
}
