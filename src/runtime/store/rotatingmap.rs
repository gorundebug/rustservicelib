use std::{
    collections::HashMap,
    hash::Hash,
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
    maps: Mutex<Maps<K, V>>,
    interval: Duration,
    started: AtomicBool,
    stopped: AtomicBool,
    cancellation: CancellationToken,
    rotation_task: tokio::sync::Mutex<Option<JoinHandle<()>>>,
}

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
                maps: Mutex::new(Maps {
                    current: HashMap::new(),
                    previous: HashMap::new(),
                    high_water_mark: 0,
                }),
                interval,
                started: AtomicBool::new(false),
                stopped: AtomicBool::new(false),
                cancellation: CancellationToken::new(),
                rotation_task: tokio::sync::Mutex::new(None),
            }),
        }
    }

    pub fn set(&self, key: K, value: V) -> RuntimeResult<()> {
        let mut maps = self.inner.maps.lock().expect("rotating map lock poisoned");
        if maps.current.contains_key(&key) || maps.previous.contains_key(&key) {
            return Err(RuntimeError::DuplicateKey);
        }
        maps.current.insert(key, value);
        Ok(())
    }

    pub fn get(&self, key: &K) -> Option<V>
    where
        V: Clone,
    {
        let maps = self.inner.maps.lock().expect("rotating map lock poisoned");
        maps.current
            .get(key)
            .or_else(|| maps.previous.get(key))
            .cloned()
    }

    pub fn pop(&self, key: &K) -> Option<V> {
        let mut maps = self.inner.maps.lock().expect("rotating map lock poisoned");
        maps.current
            .remove(key)
            .or_else(|| maps.previous.remove(key))
    }

    pub fn rotate(&self) {
        rotate(&self.inner);
    }
}

fn rotate<K, V>(inner: &RotatingMapInner<K, V>)
where
    K: Eq + Hash,
{
    let mut maps = inner.maps.lock().expect("rotating map lock poisoned");
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
