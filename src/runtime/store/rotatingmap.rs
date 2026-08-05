use std::{
    borrow::Borrow,
    collections::{HashMap, hash_map::RandomState},
    hash::{BuildHasher, Hash},
    sync::{
        Arc, Mutex, Weak,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use async_trait::async_trait;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use super::Storage;
use crate::runtime::{
    common::MessageContext,
    environment::{RuntimeError, RuntimeResult},
};

const ROTATING_MAP_SHRINK_FACTOR: usize = 4;

struct Maps<K, V> {
    current: HashMap<K, V>,
    previous: HashMap<K, V>,
    high_water_mark: usize,
}

struct RotatingMapInner<K, V> {
    shards: Box<[Mutex<Maps<K, V>>]>,
    hash_builder: RandomState,
    interval: Duration,
    started: AtomicBool,
    stopped: AtomicBool,
    cancellation: CancellationToken,
    rotation_task: tokio::sync::Mutex<Option<JoinHandle<()>>>,
}

// Stream IDs are uniformly distributed, so a fixed power-of-two shard count keeps
// the hot pending-request path off a single process-wide mutex without allocating
// any HashMap buckets until a shard is actually used.
const ROTATING_MAP_SHARDS: usize = 64;

/// Hash map with periodic bucket-capacity reclamation after transient growth.
/// Rotation never expires entries.
pub struct RotatingMap<K, V> {
    inner: Arc<RotatingMapInner<K, V>>,
}

impl<K, V> Clone for RotatingMap<K, V> {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl<K, V> RotatingMap<K, V>
where
    K: Eq + Hash,
{
    pub fn new(interval: Duration) -> Self {
        assert!(!interval.is_zero(), "rotation interval must be positive");
        Self {
            inner: Arc::new(RotatingMapInner {
                shards: (0..ROTATING_MAP_SHARDS)
                    .map(|_| {
                        Mutex::new(Maps {
                            current: HashMap::new(),
                            previous: HashMap::new(),
                            high_water_mark: 0,
                        })
                    })
                    .collect(),
                hash_builder: RandomState::new(),
                interval,
                started: AtomicBool::new(false),
                stopped: AtomicBool::new(false),
                cancellation: CancellationToken::new(),
                rotation_task: tokio::sync::Mutex::new(None),
            }),
        }
    }

    fn shard<Q>(&self, key: &Q) -> &Mutex<Maps<K, V>>
    where
        Q: Hash + ?Sized,
    {
        let hash = self.inner.hash_builder.hash_one(key);
        &self.inner.shards[hash as usize % self.inner.shards.len()]
    }

    pub fn set(&self, key: K, value: V) -> RuntimeResult<()> {
        let mut maps = self.shard(&key).lock().expect("rotating map lock poisoned");
        if maps.current.contains_key(&key) || maps.previous.contains_key(&key) {
            return Err(RuntimeError::DuplicateKey);
        }
        maps.current.insert(key, value);
        Ok(())
    }

    pub fn get_or_create<F>(&self, key: K, factory: F) -> (V, bool)
    where
        V: Clone,
        F: FnOnce() -> V,
    {
        let mut maps = self.shard(&key).lock().expect("rotating map lock poisoned");
        if let Some(value) = maps.current.get(&key).or_else(|| maps.previous.get(&key)) {
            return (value.clone(), true);
        }
        let value = factory();
        maps.current.insert(key, value.clone());
        (value, false)
    }

    pub fn get<Q>(&self, key: &Q) -> Option<V>
    where
        K: Borrow<Q>,
        Q: Eq + Hash + ?Sized,
        V: Clone,
    {
        let maps = self.shard(key).lock().expect("rotating map lock poisoned");
        maps.current
            .get(key)
            .or_else(|| maps.previous.get(key))
            .cloned()
    }

    pub fn pop<Q>(&self, key: &Q) -> Option<V>
    where
        K: Borrow<Q>,
        Q: Eq + Hash + ?Sized,
    {
        let mut maps = self.shard(key).lock().expect("rotating map lock poisoned");
        maps.current
            .remove(key)
            .or_else(|| maps.previous.remove(key))
    }

    pub fn pop_if<Q, F>(&self, key: &Q, predicate: F) -> Option<V>
    where
        K: Borrow<Q>,
        Q: Eq + Hash + ?Sized,
        F: FnOnce(&V) -> bool,
    {
        let mut maps = self.shard(key).lock().expect("rotating map lock poisoned");
        if let Some(value) = maps.current.get(key) {
            if predicate(value) {
                return maps.current.remove(key);
            }
            return None;
        }
        if let Some(value) = maps.previous.get(key)
            && predicate(value)
        {
            return maps.previous.remove(key);
        }
        None
    }

    pub fn rotate(&self) {
        rotate(&self.inner);
    }
}

fn rotate<K, V>(inner: &RotatingMapInner<K, V>)
where
    K: Eq + Hash,
{
    for shard in &inner.shards {
        rotate_shard(shard);
    }
}

fn rotate_shard<K, V>(shard: &Mutex<Maps<K, V>>)
where
    K: Eq + Hash,
{
    let mut maps = shard.lock().expect("rotating map lock poisoned");
    let total = maps.current.len() + maps.previous.len();
    let should_rotate = maps.high_water_mark == 0
        || total.saturating_mul(ROTATING_MAP_SHRINK_FACTOR) < maps.high_water_mark;
    maps.high_water_mark = maps.high_water_mark.max(total);
    if !should_rotate {
        return;
    }

    let mut fresh = HashMap::with_capacity(total);
    for (key, value) in maps.current.drain() {
        fresh.insert(key, value);
    }
    for (key, value) in maps.previous.drain() {
        fresh.entry(key).or_insert(value);
    }
    maps.previous = fresh;
    maps.high_water_mark = total;
}

#[async_trait]
impl<K, V> Storage for RotatingMap<K, V>
where
    K: Eq + Hash + Send + 'static,
    V: Send + 'static,
{
    async fn start(&self, _context: MessageContext) -> RuntimeResult<()> {
        if self.inner.stopped.load(Ordering::Acquire) {
            return Err(RuntimeError::ResourceStopped("rotating map".to_owned()));
        }
        if self.inner.started.swap(true, Ordering::AcqRel) {
            return Err(RuntimeError::ResourceAlreadyStarted(
                "rotating map".to_owned(),
            ));
        }

        let inner = Arc::downgrade(&self.inner);
        let cancellation = self.inner.cancellation.clone();
        let interval = self.inner.interval;
        *self.inner.rotation_task.lock().await = Some(tokio::spawn(async move {
            rotation_loop(inner, cancellation, interval).await;
        }));
        Ok(())
    }

    async fn stop(&self, _context: MessageContext) {
        self.inner.stopped.store(true, Ordering::Release);
        self.inner.cancellation.cancel();
        if let Some(task) = self.inner.rotation_task.lock().await.take() {
            let _ = task.await;
        }
    }
}

async fn rotation_loop<K, V>(
    inner: Weak<RotatingMapInner<K, V>>,
    cancellation: CancellationToken,
    interval: Duration,
) where
    K: Eq + Hash + Send + 'static,
    V: Send + 'static,
{
    loop {
        tokio::select! {
            _ = tokio::time::sleep(interval) => {}
            _ = cancellation.cancelled() => return,
        }
        let Some(inner) = inner.upgrade() else {
            return;
        };
        rotate(&inner);
    }
}
