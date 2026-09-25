use std::{
    collections::HashMap,
    hash::Hash,
    sync::{
        Arc, OnceLock,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use async_trait::async_trait;
use tokio::{
    sync::Mutex,
    time::{Instant, sleep_until},
};
use tokio_util::sync::CancellationToken;

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
    expiry: Option<CancellationToken>,
}

struct Entry<K> {
    state: Mutex<Item<K>>,
    // Admission must inspect expiry without waiting for a business callback
    // holding state. An expired generation can finish independently of its
    // replacement, as in Go's map lookup before taking Item.lock.
    deadline: std::sync::Mutex<Option<Instant>>,
}

impl<K> Entry<K> {
    fn deadline(&self) -> Option<Instant> {
        *self.deadline.lock().expect("join deadline lock poisoned")
    }

    fn set_deadline(&self, deadline: Instant) {
        *self.deadline.lock().expect("join deadline lock poisoned") = Some(deadline);
    }

    fn expired(&self) -> bool {
        self.deadline()
            .is_some_and(|deadline| deadline <= Instant::now())
    }
}

struct HashMapJoinStorageInner<K> {
    items: Mutex<HashMap<K, Arc<Entry<K>>>>,
    config: Arc<dyn Fn() -> (Duration, bool) + Send + Sync>,
    stopped: AtomicBool,
    started: AtomicBool,
    metrics: OnceLock<Option<HashMapJoinStorageMetrics>>,
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
        if environment.metrics().is_noop() {
            let _ = self.inner.metrics.set(None);
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
        let _ = self.inner.metrics.set(Some(HashMapJoinStorageMetrics {
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
        }));
        Ok(())
    }

    fn arm_expiry(
        store: Arc<HashMapJoinStorageInner<K>>,
        key: K,
        item: Arc<Entry<K>>,
        generation: u64,
        deadline: Instant,
        context: MessageContext,
        cancellation: CancellationToken,
    ) {
        tokio::spawn(async move {
            let mut deadline = deadline;
            let (callback, callback_context, values) = loop {
                // An accepted timer is not replaced on renewal. At its next
                // wake it observes an extended logical deadline, like Go's
                // AfterFunc. A shorter/zero TTL does not move that wake early.
                tokio::select! {
                    biased;
                    _ = cancellation.cancelled() => return,
                    _ = async {
                        if context.deadline().is_some() {
                            context.cancelled().await;
                        } else {
                            sleep_until(deadline).await;
                        }
                    } => {},
                }

                let mut item_guard = item.state.lock().await;
                if item_guard.processed || item_guard.generation != generation {
                    return;
                }
                if context.deadline().is_none()
                    && let Some(renewed) = item.deadline()
                    && renewed > Instant::now()
                {
                    deadline = renewed;
                    continue;
                }
                item_guard.processed = true;
                item_guard.expiry = None;
                break (
                    Arc::clone(&item_guard.callback),
                    item_guard.context.clone(),
                    item_guard.values.clone(),
                );
            };
            callback(callback_context, key.clone(), values).await;

            let mut items = store.items.lock().await;
            if items
                .get(&key)
                .is_some_and(|stored| Arc::ptr_eq(stored, &item))
            {
                items.remove(&key);
                if let Some(metrics) = store.metrics.get().and_then(Option::as_ref) {
                    metrics.count.dec();
                    metrics.evictions_total.inc();
                }
            }
        });
    }

    async fn remove_if_same(&self, key: &K, item: &Arc<Entry<K>>) {
        let mut items = self.inner.items.lock().await;
        if items
            .get(key)
            .is_some_and(|stored| Arc::ptr_eq(stored, item))
        {
            items.remove(key);
            if let Some(metrics) = self.inner.metrics.get().and_then(Option::as_ref) {
                metrics.count.dec();
            }
        }
    }
}

impl<K> HashMapJoinStorage<K>
where
    K: Clone + Eq + Hash + Send + Sync + 'static,
{
    pub(crate) async fn join_value_with<C, Fut>(
        &self,
        context: MessageContext,
        key: K,
        index: usize,
        value: DynValue,
        callback: JoinCallback<K>,
        invoke: C,
    ) -> bool
    where
        C: Fn(MessageContext, K, JoinValues) -> Fut + Send,
        Fut: std::future::Future<Output = bool> + Send,
    {
        // Snapshot invocation settings once, before possible contention, as
        // Go's JoinValue does. Reloads affect subsequent invocations.
        let (configured_ttl, renew_ttl) = (self.inner.config)();
        let effective_ttl = context
            .deadline()
            .map(|deadline| deadline.saturating_duration_since(Instant::now()))
            .unwrap_or(configured_ttl);
        loop {
            let item = {
                let mut items = self.inner.items.lock().await;
                match items.get(&key) {
                    Some(item) if !item.expired() => Arc::clone(item),
                    _ => {
                        let item = Arc::new(Entry {
                            state: Mutex::new(Item {
                                values: Vec::new(),
                                processed: false,
                                generation: 0,
                                context: context.clone(),
                                callback: Arc::clone(&callback),
                                expiry: None,
                            }),
                            deadline: std::sync::Mutex::new(
                                (effective_ttl > Duration::ZERO)
                                    .then(|| Instant::now() + effective_ttl),
                            ),
                        });
                        let is_new_key = items.insert(key.clone(), Arc::clone(&item)).is_none();
                        if is_new_key
                            && let Some(metrics) = self.inner.metrics.get().and_then(Option::as_ref)
                        {
                            metrics.count.inc();
                        }
                        item
                    }
                }
            };

            let processed = {
                let mut item_guard = item.state.lock().await;
                if item_guard.processed || item.expired() {
                    // The previous callback completed while this value waited
                    // for the key. Retire only that item, then retry with the
                    // still-unconsumed value, as Go's JoinValue does.
                    drop(item_guard);
                    self.remove_if_same(&key, &item).await;
                    continue;
                }
                let first_value = item_guard.values.is_empty();
                if item_guard.values.len() <= index {
                    item_guard.values.resize_with(index + 1, Vec::new);
                }
                item_guard.values[index].push(value);
                if first_value {
                    item_guard.generation = item_guard.generation.wrapping_add(1);
                    item_guard.context = context.clone();
                    item_guard.callback = Arc::clone(&callback);
                    if effective_ttl > Duration::ZERO {
                        let cancellation = CancellationToken::new();
                        item_guard.expiry = Some(cancellation.clone());
                        // Start the accepted group's clock before user code.
                        // Expiry still acquires the item lock before delivery.
                        Self::arm_expiry(
                            Arc::clone(&self.inner),
                            key.clone(),
                            Arc::clone(&item),
                            item_guard.generation,
                            item.deadline().expect("positive TTL has a deadline"),
                            context.clone(),
                            cancellation,
                        );
                    }
                }
                let values = item_guard.values.clone();
                item_guard.processed = invoke(context.clone(), key.clone(), values).await;
                if item_guard.processed
                    && let Some(expiry) = item_guard.expiry.take()
                {
                    expiry.cancel();
                }
                if !item_guard.processed && renew_ttl {
                    // This updates admission and any already accepted timer;
                    // it neither creates a missing timer nor replaces its
                    // original callback/context ownership.
                    item.set_deadline(Instant::now() + effective_ttl);
                }
                item_guard.processed
            };

            if processed {
                self.remove_if_same(&key, &item).await;
            }
            return true;
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
        self.join_value_with(
            context,
            key,
            index,
            value,
            Arc::clone(&callback),
            move |context, key, values| callback(context, key, values),
        )
        .await
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
        // Go stops background map maintenance, not JoinValue or the timers
        // belonging to accepted groups. Keep their values and metrics intact;
        // completion/expiration remains responsible for removing each item.
        self.inner.stopped.store(true, Ordering::Release);
    }
}

#[cfg(test)]
mod dynamic_ttl_contract {
    use super::*;
    use futures::FutureExt;
    use std::sync::atomic::AtomicU64;
    use tokio::{sync::mpsc, time::timeout};

    async fn check(initial: u64, next: u64, expect_expiry: bool, minimum: u64) {
        let ttl = Arc::new(AtomicU64::new(initial));
        let renew = Arc::new(AtomicBool::new(initial > 0));
        let store = HashMapJoinStorage::<u32>::with_config({
            let ttl = ttl.clone();
            let renew = renew.clone();
            move || {
                (
                    Duration::from_secs(ttl.load(Ordering::SeqCst)),
                    renew.load(Ordering::SeqCst),
                )
            }
        });
        let (sender, mut receiver) = mpsc::unbounded_channel();
        let callback: JoinCallback<u32> = Arc::new(move |_, _, _| {
            let sender = sender.clone();
            async move {
                sender.send(Instant::now()).unwrap();
                false
            }
            .boxed()
        });
        let started = Instant::now();
        store
            .join_value(
                MessageContext::new(),
                7,
                0,
                Arc::new(10_u32),
                callback.clone(),
            )
            .await;
        receiver.recv().await.unwrap();
        ttl.store(next, Ordering::SeqCst);
        renew.store(true, Ordering::SeqCst);
        store
            .join_value(MessageContext::new(), 7, 1, Arc::new(20_u32), callback)
            .await;
        receiver.recv().await.unwrap();
        let expired = timeout(
            Duration::from_secs(if expect_expiry { 30 } else { 3 }),
            receiver.recv(),
        )
        .await;
        let complete: JoinCallback<u32> = Arc::new(|_, _, _| async { true }.boxed());
        store
            .join_value(MessageContext::new(), 7, 0, Arc::new(30_u32), complete)
            .await;
        store.stop(MessageContext::new()).await;
        if expect_expiry {
            let expired = expired
                .expect("renewal lost the accepted expiry callback")
                .unwrap();
            assert!(
                expired >= started + Duration::from_secs(minimum),
                "callback moved ahead of accepted timer"
            );
        } else {
            assert!(
                expired.is_err(),
                "renewal installed a timer absent at initial admission"
            );
        }
    }

    #[tokio::test(start_paused = true)]
    async fn extending_ttl_postpones_accepted_expiry() {
        check(10, 20, true, 20).await;
    }

    #[tokio::test(start_paused = true)]
    async fn shortening_ttl_keeps_original_timer_boundary() {
        check(10, 2, true, 10).await;
    }

    #[tokio::test(start_paused = true)]
    async fn zero_ttl_does_not_cancel_already_accepted_expiry() {
        check(10, 0, true, 10).await;
    }

    #[tokio::test(start_paused = true)]
    async fn positive_ttl_does_not_create_missing_initial_timer() {
        check(0, 2, false, 0).await;
    }

    #[tokio::test(start_paused = true)]
    async fn expired_generation_does_not_block_or_contaminate_new_admission() {
        let store = HashMapJoinStorage::<u32>::new(Duration::from_secs(3600), false);
        let entered = Arc::new(tokio::sync::Semaphore::new(0));
        let release = Arc::new(tokio::sync::Semaphore::new(0));
        let calls = Arc::new(AtomicU64::new(0));
        let callback: JoinCallback<u32> = Arc::new({
            let entered = entered.clone();
            let release = release.clone();
            move |_, _, _| {
                let entered = entered.clone();
                let release = release.clone();
                let calls = calls.clone();
                async move {
                    if calls.fetch_add(1, Ordering::SeqCst) == 0 {
                        entered.add_permits(1);
                        release.acquire().await.unwrap().forget();
                    }
                    false
                }
                .boxed()
            }
        });
        let first = tokio::spawn({
            let store = store.clone();
            async move {
                store
                    .join_value(
                        MessageContext::new().with_timeout_limit(Duration::from_secs(10)),
                        7,
                        0,
                        Arc::new(10_u32),
                        callback,
                    )
                    .await
            }
        });
        entered.acquire().await.unwrap().forget();
        tokio::time::advance(Duration::from_secs(20)).await;
        let (sender, mut receiver) = mpsc::unbounded_channel();
        let second = tokio::spawn({
            let store = store.clone();
            async move {
                let callback: JoinCallback<u32> = Arc::new(move |_, _, values| {
                    let sender = sender.clone();
                    async move {
                        sender
                            .send(
                                values
                                    .iter()
                                    .map(|slot| {
                                        slot.iter()
                                            .map(|value| *value.downcast_ref::<u32>().unwrap())
                                            .collect::<Vec<_>>()
                                    })
                                    .collect::<Vec<_>>(),
                            )
                            .unwrap();
                        true
                    }
                    .boxed()
                });
                store
                    .join_value(MessageContext::new(), 7, 0, Arc::new(20_u32), callback)
                    .await
            }
        });
        let fresh = timeout(Duration::from_secs(1), receiver.recv()).await;
        release.add_permits(1);
        first.await.unwrap();
        second.await.unwrap();
        store.stop(MessageContext::new()).await;
        assert_eq!(
            fresh
                .expect("expired callback blocked a new generation")
                .unwrap(),
            vec![vec![20]]
        );
    }

    async fn stale_callback(completes: bool, replacement_completes: bool) {
        let store = HashMapJoinStorage::<u32>::new(Duration::from_secs(3600), true);
        let entered = Arc::new(tokio::sync::Semaphore::new(0));
        let release = Arc::new(tokio::sync::Semaphore::new(0));
        let calls = Arc::new(AtomicU64::new(0));
        let callback: JoinCallback<u32> = Arc::new({
            let entered = entered.clone();
            let release = release.clone();
            move |_, _, _| {
                let entered = entered.clone();
                let release = release.clone();
                let calls = calls.clone();
                async move {
                    if calls.fetch_add(1, Ordering::SeqCst) == 0 {
                        entered.add_permits(1);
                        release.acquire().await.unwrap().forget();
                    }
                    completes
                }
                .boxed()
            }
        });
        let first = tokio::spawn({
            let store = store.clone();
            async move {
                store
                    .join_value(
                        MessageContext::new().with_timeout_limit(Duration::from_secs(10)),
                        7,
                        0,
                        Arc::new(10_u32),
                        callback,
                    )
                    .await
            }
        });
        entered.acquire().await.unwrap().forget();
        tokio::time::advance(Duration::from_secs(20)).await;
        let keep: JoinCallback<u32> = Arc::new(move |_, _, values| {
            async move {
                assert_eq!(values.len(), 1);
                assert_eq!(values[0].len(), 1);
                assert_eq!(*values[0][0].downcast_ref::<u32>().unwrap(), 20);
                replacement_completes
            }
            .boxed()
        });
        store
            .join_value(MessageContext::new(), 7, 0, Arc::new(20_u32), keep)
            .await;
        release.add_permits(1);
        first.await.unwrap();
        let complete: JoinCallback<u32> = Arc::new(move |_, _, values| {
            async move {
                let values: Vec<Vec<u32>> = values
                    .iter()
                    .map(|slot| {
                        slot.iter()
                            .map(|value| *value.downcast_ref::<u32>().unwrap())
                            .collect()
                    })
                    .collect();
                let expected = if replacement_completes {
                    vec![vec![], vec![30]]
                } else {
                    vec![vec![20], vec![30]]
                };
                assert_eq!(
                    values, expected,
                    "stale callback changed the current generation"
                );
                true
            }
            .boxed()
        });
        store
            .join_value(MessageContext::new(), 7, 1, Arc::new(30_u32), complete)
            .await;
        store.stop(MessageContext::new()).await;
    }

    #[tokio::test(start_paused = true)]
    async fn stale_completion_preserves_new_generation() {
        stale_callback(true, false).await;
    }

    #[tokio::test(start_paused = true)]
    async fn stale_renewal_does_not_resurrect_old_generation() {
        stale_callback(false, false).await;
    }

    #[tokio::test(start_paused = true)]
    async fn stale_renewal_cannot_restore_an_already_replaced_generation() {
        stale_callback(false, true).await;
    }

    #[tokio::test(start_paused = true)]
    async fn stale_completion_after_replacement_completion_leaves_no_values() {
        stale_callback(true, true).await;
    }
}
