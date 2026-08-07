use std::{
    collections::HashMap,
    hash::Hash,
    sync::{
        Arc, OnceLock, Weak,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use async_trait::async_trait;
use tokio::{
    sync::Mutex,
    time::{Instant, sleep},
};

use super::{DynValue, JoinCallback, JoinStorage, JoinValues, Storage};
use crate::runtime::{
    common::MessageContext,
    environment::{
        RuntimeEnvironment, RuntimeError, RuntimeResult,
        metrics::{Int64Counter, Int64Gauge, Labels},
    },
};

struct Item<K> {
    values: JoinValues,
    processed: bool,
    generation: u64,
    context: MessageContext,
    callback: JoinCallback<K>,
}

struct HashMapJoinStorageInner<K> {
    items: Mutex<HashMap<K, Arc<Mutex<Item<K>>>>>,
    config: Arc<dyn Fn() -> (Duration, bool) + Send + Sync>,
    stopped: AtomicBool,
    started: AtomicBool,
    metrics: OnceLock<HashMapJoinStorageMetrics>,
}

struct HashMapJoinStorageMetrics {
    count: Int64Gauge,
    evictions_total: Int64Counter,
}

/// Per-key serialized state used by Join and MultiJoin.
///
/// The behavior follows Go's `HashMapJoinStorage`: indexed value lists,
/// callback serialization per key, callback-driven removal, TTL callback, and
/// context-deadline replacement of configured TTL.
pub struct HashMapJoinStorage<K> {
    inner: Arc<HashMapJoinStorageInner<K>>,
}

impl<K> Clone for HashMapJoinStorage<K> {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl<K> HashMapJoinStorage<K>
where
    K: Clone + Eq + Hash + Send + Sync + 'static,
{
    pub fn new(ttl: Duration, renew_ttl: bool) -> Self {
        Self::with_config(move || (ttl, renew_ttl))
    }

    pub fn from_stream(environment: RuntimeEnvironment, stream_id: i32) -> Self {
        Self::with_config(move || {
            let Some(config) = environment.stream_config(stream_id) else {
                return (Duration::ZERO, false);
            };
            match config.as_ref() {
                crate::runtime::config::RuntimeStreamConfig::Join(config) => {
                    (config.ttl, config.renew_ttl)
                }
                crate::runtime::config::RuntimeStreamConfig::MultiJoin(config) => {
                    (config.ttl, config.renew_ttl)
                }
                _ => (Duration::ZERO, false),
            }
        })
    }

    fn with_config(config: impl Fn() -> (Duration, bool) + Send + Sync + 'static) -> Self {
        Self {
            inner: Arc::new(HashMapJoinStorageInner {
                items: Mutex::new(HashMap::new()),
                config: Arc::new(config),
                stopped: AtomicBool::new(false),
                started: AtomicBool::new(false),
                metrics: OnceLock::new(),
            }),
        }
    }

    pub fn configure_metrics(
        &self,
        environment: &RuntimeEnvironment,
        name: &str,
    ) -> RuntimeResult<()> {
        if self.inner.metrics.get().is_some() {
            return Ok(());
        }
        let scope = environment.metrics().scope(
            "hashmap_join_storage",
            [
                ("service".to_owned(), environment.service_name()),
                ("name".to_owned(), name.to_owned()),
            ]
            .into_iter()
            .collect(),
        );
        let _ = self.inner.metrics.set(HashMapJoinStorageMetrics {
            count: scope.gauge(
                "count",
                "Elements count stored in a join storage",
                Labels::new(),
            )?,
            evictions_total: scope.counter(
                "evictions_total",
                "Total number of items evicted from join storage by TTL",
                Labels::new(),
            )?,
        });
        Ok(())
    }

    fn arm_expiry(
        store: Weak<HashMapJoinStorageInner<K>>,
        key: K,
        item: Arc<Mutex<Item<K>>>,
        generation: u64,
        ttl: Duration,
        uses_context_deadline: bool,
    ) {
        tokio::spawn(async move {
            let context = { item.lock().await.context.clone() };
            if uses_context_deadline {
                context.cancelled().await;
            } else {
                sleep(ttl).await;
            }

            let Some(store) = store.upgrade() else {
                return;
            };
            if store.stopped.load(Ordering::Acquire) {
                return;
            }

            let (callback, callback_context, values) = {
                let mut item_guard = item.lock().await;
                if item_guard.processed || item_guard.generation != generation {
                    return;
                }
                item_guard.processed = true;
                (
                    Arc::clone(&item_guard.callback),
                    item_guard.context.clone(),
                    item_guard.values.clone(),
                )
            };
            callback(callback_context, key.clone(), values).await;

            let mut items = store.items.lock().await;
            if items
                .get(&key)
                .is_some_and(|stored| Arc::ptr_eq(stored, &item))
            {
                items.remove(&key);
                if let Some(metrics) = store.metrics.get() {
                    metrics.count.dec();
                    metrics.evictions_total.inc();
                }
            }
        });
    }

    async fn remove_if_same(&self, key: &K, item: &Arc<Mutex<Item<K>>>) {
        let mut items = self.inner.items.lock().await;
        if items
            .get(key)
            .is_some_and(|stored| Arc::ptr_eq(stored, item))
        {
            items.remove(key);
            if let Some(metrics) = self.inner.metrics.get() {
                metrics.count.dec();
            }
        }
    }
}

#[async_trait]
impl<K> JoinStorage<K> for HashMapJoinStorage<K>
where
    K: Clone + Eq + Hash + Send + Sync + 'static,
{
    async fn join_value(
        &self,
        context: MessageContext,
        key: K,
        index: usize,
        value: DynValue,
        callback: JoinCallback<K>,
    ) -> bool {
        if self.inner.stopped.load(Ordering::Acquire) {
            return false;
        }

        let (item, created) = {
            let mut items = self.inner.items.lock().await;
            match items.get(&key) {
                Some(item) => (Arc::clone(item), false),
                None => {
                    let item = Arc::new(Mutex::new(Item {
                        values: Vec::new(),
                        processed: false,
                        generation: 0,
                        context: context.clone(),
                        callback: Arc::clone(&callback),
                    }));
                    items.insert(key.clone(), Arc::clone(&item));
                    if let Some(metrics) = self.inner.metrics.get() {
                        metrics.count.inc();
                    }
                    (item, true)
                }
            }
        };

        let (configured_ttl, renew_ttl) = (self.inner.config)();
        let effective_ttl = context
            .deadline()
            .map(|deadline| deadline.saturating_duration_since(Instant::now()))
            .unwrap_or(configured_ttl);
        let uses_context_deadline = context.deadline().is_some();

        let (processed, generation, should_arm) = {
            let mut item_guard = item.lock().await;
            if item_guard.processed {
                return false;
            }
            if item_guard.values.len() <= index {
                item_guard.values.resize_with(index + 1, Vec::new);
            }
            item_guard.values[index].push(value);
            let values = item_guard.values.clone();
            item_guard.processed = callback(context.clone(), key.clone(), values).await;
            if created || renew_ttl {
                item_guard.generation = item_guard.generation.wrapping_add(1);
                item_guard.context = context.clone();
                item_guard.callback = Arc::clone(&callback);
            }
            (
                item_guard.processed,
                item_guard.generation,
                effective_ttl > Duration::ZERO && (created || renew_ttl),
            )
        };

        if processed {
            self.remove_if_same(&key, &item).await;
        } else if should_arm {
            Self::arm_expiry(
                Arc::downgrade(&self.inner),
                key,
                item,
                generation,
                effective_ttl,
                uses_context_deadline,
            );
        }
        true
    }

    async fn len(&self) -> usize {
        self.inner.items.lock().await.len()
    }
}

#[async_trait]
impl<K> Storage for HashMapJoinStorage<K>
where
    K: Clone + Eq + Hash + Send + Sync + 'static,
{
    async fn start(&self, _context: MessageContext) -> RuntimeResult<()> {
        if self.inner.stopped.load(Ordering::Acquire) {
            return Err(RuntimeError::ResourceStopped(
                "hashmap join storage".to_owned(),
            ));
        }
        if self.inner.started.swap(true, Ordering::AcqRel) {
            return Err(RuntimeError::ResourceAlreadyStarted(
                "hashmap join storage".to_owned(),
            ));
        }
        Ok(())
    }

    async fn stop(&self, _context: MessageContext) {
        self.inner.stopped.store(true, Ordering::Release);
        self.inner.items.lock().await.clear();
        if let Some(metrics) = self.inner.metrics.get() {
            metrics.count.set(0);
        }
    }
}
