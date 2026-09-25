use std::{sync::{Arc, atomic::{AtomicUsize, Ordering}}, time::Duration};

use futures::FutureExt;
use servicelib::{MessageContext, runtime::store::{HashMapJoinStorage, JoinCallback, JoinStorage, Storage}};
use tokio::sync::Semaphore;

async fn accepted_expiry(stop_before_cancel: bool, drop_owner: bool) {
    let store = HashMapJoinStorage::<u32>::new(Duration::from_secs(3600), false);
    let context = MessageContext::new().with_timeout_limit(Duration::from_secs(3600));
    let calls = Arc::new(AtomicUsize::new(0));
    let completed = Arc::new(Semaphore::new(0));
    let value = Arc::new(42_u32);
    let weak = Arc::downgrade(&value);
    let callback: JoinCallback<u32> = {
        let calls = calls.clone();
        let completed = completed.clone();
        Arc::new(move |context, _, values| {
            let calls = calls.clone();
            let completed = completed.clone();
            async move {
                assert_eq!(*values[0][0].downcast_ref::<u32>().unwrap(), 42);
                if calls.fetch_add(1, Ordering::SeqCst) > 0 {
                    assert!(context.is_cancelled());
                    completed.add_permits(1);
                }
                false
            }.boxed()
        })
    };
    assert!(store.join_value(context.clone(), 7, 0, value, callback).await);
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    if stop_before_cancel { store.stop(MessageContext::new()).await; }
    assert_eq!(store.len().await, 1, "stop removed an accepted group");
    let retained_owner = if drop_owner { drop(store); None } else { Some(store) };
    context.cancel();
    tokio::time::timeout(Duration::from_secs(2), completed.acquire()).await
        .expect("storage stop suppressed an already accepted expiry callback").unwrap().forget();
    // Use real clock time here: a yield loop must not prevent a paused-clock timeout.
    tokio::time::timeout(Duration::from_secs(2), async {
        while weak.upgrade().is_some() { tokio::task::yield_now().await; }
    }).await.expect("completed expiry retained accepted input");
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    if let Some(store) = retained_owner { assert_eq!(store.len().await, 0); }
}

#[tokio::test]
async fn accepted_context_expiry_completes() { accepted_expiry(false, false).await; }

#[tokio::test]
async fn accepted_context_expiry_survives_storage_stop() { accepted_expiry(true, false).await; }

#[tokio::test]
async fn accepted_context_expiry_owns_storage_until_completion() {
    accepted_expiry(false, true).await;
    accepted_expiry(true, true).await;
}

#[tokio::test]
async fn stopped_storage_preserves_join_completion_and_reuse() {
    for ttl in [Duration::ZERO, Duration::from_secs(3600)] {
        let store = HashMapJoinStorage::<u32>::new(ttl, false);
        let calls = Arc::new(AtomicUsize::new(0));
        let callback: JoinCallback<u32> = {
            let calls = calls.clone();
            Arc::new(move |_, _, values| {
                let calls = calls.clone();
                async move {
                    calls.fetch_add(1, Ordering::SeqCst);
                    assert_eq!(*values[0][0].downcast_ref::<u32>().unwrap(), 10);
                    if values.len() == 1 { return false; }
                    assert_eq!(*values[1][0].downcast_ref::<u32>().unwrap(), 20);
                    true
                }.boxed()
            })
        };
        assert!(store.join_value(MessageContext::new(), 7, 0, Arc::new(10_u32), callback.clone()).await);
        store.stop(MessageContext::new()).await;
        store.stop(MessageContext::new()).await;
        assert_eq!(store.len().await, 1);
        assert!(store.start(MessageContext::new()).await.is_err(), "stop still forbids restart");
        assert!(store.join_value(MessageContext::new(), 7, 1, Arc::new(20_u32), callback.clone()).await);
        assert_eq!(store.len().await, 0);
        // Go also permits new groups after Stop, including reusing a completed key.
        for key in [7, 8] {
            assert!(store.join_value(MessageContext::new(), key, 0, Arc::new(10_u32), callback.clone()).await);
            assert!(store.join_value(MessageContext::new(), key, 1, Arc::new(20_u32), callback.clone()).await);
            assert_eq!(store.len().await, 0);
        }
        assert_eq!(calls.load(Ordering::SeqCst), 6);
    }
}

#[tokio::test]
async fn configured_ttl_survives_stop_and_external_owner_drop() {
    let store = HashMapJoinStorage::<u32>::new(Duration::from_millis(100), false);
    let completed = Arc::new(Semaphore::new(0));
    let calls = Arc::new(AtomicUsize::new(0));
    let value = Arc::new(42_u32);
    let weak = Arc::downgrade(&value);
    let callback: JoinCallback<u32> = {
        let completed = completed.clone();
        let calls = calls.clone();
        Arc::new(move |_, _, values| {
            let completed = completed.clone();
            let calls = calls.clone();
            async move {
                assert_eq!(*values[0][0].downcast_ref::<u32>().unwrap(), 42);
                if calls.fetch_add(1, Ordering::SeqCst) > 0 { completed.add_permits(1); }
                false
            }.boxed()
        })
    };
    assert!(store.join_value(MessageContext::new(), 7, 0, value, callback).await);
    store.stop(MessageContext::new()).await;
    drop(store);
    tokio::time::timeout(Duration::from_secs(2), completed.acquire()).await
        .expect("configured TTL callback lost after stop/drop").unwrap().forget();
    tokio::time::timeout(Duration::from_secs(2), async {
        while weak.upgrade().is_some() { tokio::task::yield_now().await; }
    }).await.expect("completed TTL retained accepted input");
    assert_eq!(calls.load(Ordering::SeqCst), 2);
}
