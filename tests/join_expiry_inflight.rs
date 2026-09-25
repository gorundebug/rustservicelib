use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use futures::FutureExt;
use servicelib::{
    MessageContext,
    runtime::store::{HashMapJoinStorage, JoinCallback, JoinStorage, Storage},
};
use tokio::sync::Semaphore;

async fn check_expiry_callback_lifetime(stop_during_callback: bool) {
    let store = HashMapJoinStorage::<u32>::new(Duration::from_secs(1), false);
    let calls = Arc::new(AtomicUsize::new(0));
    let entered = Arc::new(Semaphore::new(0));
    let release = Arc::new(Semaphore::new(0));
    let finished = Arc::new(Semaphore::new(0));
    let old_value = Arc::new(10_u32);
    let old_lifetime = Arc::downgrade(&old_value);
    let callback: JoinCallback<u32> = {
        let calls = calls.clone();
        let entered = entered.clone();
        let release = release.clone();
        let finished = finished.clone();
        Arc::new(move |_context, _key, values| {
            let calls = calls.clone();
            let entered = entered.clone();
            let release = release.clone();
            let finished = finished.clone();
            async move {
                assert_eq!(*values[0][0].downcast_ref::<u32>().unwrap(), 10);
                match calls.fetch_add(1, Ordering::SeqCst) {
                    0 => false,
                    1 => {
                        entered.add_permits(1);
                        release.acquire().await.unwrap().forget();
                        assert_eq!(*values[0][0].downcast_ref::<u32>().unwrap(), 10);
                        finished.add_permits(1);
                        false
                    }
                    count => panic!("unexpected old-generation callback {count}"),
                }
            }
            .boxed()
        })
    };
    assert!(
        store
            .join_value(MessageContext::new(), 7, 0, old_value, callback)
            .await
    );
    // Paused Tokio time advances to the actual expiry; the callback then waits
    // on a gate while the parent test exercises stop or generation replacement.
    tokio::time::timeout(Duration::from_secs(3), entered.acquire())
        .await
        .unwrap()
        .unwrap()
        .forget();

    if stop_during_callback {
        store.stop(MessageContext::new()).await;
        assert_eq!(store.len().await, 1, "active expiry owns removal, not stop");
    } else {
        let replacement: JoinCallback<u32> = Arc::new(|_context, _key, values| {
            async move {
                assert_eq!(values[0].len(), 1);
                assert_eq!(*values[0][0].downcast_ref::<u32>().unwrap(), 20);
                false
            }
            .boxed()
        });
        assert!(
            store
                .join_value(MessageContext::new(), 7, 0, Arc::new(20_u32), replacement)
                .await
        );
        assert_eq!(store.len().await, 1);
    }
    assert!(
        old_lifetime.upgrade().is_some(),
        "active callback lost its input"
    );
    assert!(finished.try_acquire().is_err(), "callback escaped its gate");
    release.add_permits(1);
    tokio::time::timeout(Duration::from_secs(1), finished.acquire())
        .await
        .expect("active callback was aborted")
        .unwrap()
        .forget();
    tokio::time::timeout(Duration::from_secs(1), async {
        while old_lifetime.upgrade().is_some() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("completed expiry task retained its old input");
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    if stop_during_callback {
        assert_eq!(
            store.len().await,
            0,
            "finished expiry must remove its group"
        );
    }

    if !stop_during_callback {
        assert_eq!(store.len().await, 1, "old expiry deleted the replacement");
        let complete: JoinCallback<u32> = Arc::new(|_context, _key, values| {
            async move {
                assert_eq!(*values[0][0].downcast_ref::<u32>().unwrap(), 20);
                assert_eq!(*values[1][0].downcast_ref::<u32>().unwrap(), 30);
                true
            }
            .boxed()
        });
        assert!(
            store
                .join_value(MessageContext::new(), 7, 1, Arc::new(30_u32), complete)
                .await
        );
        assert_eq!(store.len().await, 0);
    }
}

#[tokio::test(start_paused = true)]
async fn active_expiry_finishes_without_erasing_a_replacement() {
    check_expiry_callback_lifetime(false).await;
}

#[tokio::test(start_paused = true)]
async fn stopping_storage_does_not_abort_an_active_expiry_callback() {
    check_expiry_callback_lifetime(true).await;
}
