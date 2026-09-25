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
use tokio::{
    sync::{Semaphore, mpsc},
    time::{Instant, advance, timeout},
};

#[tokio::test(start_paused = true)]
async fn ttl_includes_time_spent_in_the_initial_callback() {
    let ttl = Duration::from_secs(10);
    let store = HashMapJoinStorage::<u32>::new(ttl, false);
    let calls = Arc::new(AtomicUsize::new(0));
    let entered = Arc::new(Semaphore::new(0));
    let release = Arc::new(Semaphore::new(0));
    let expired = Arc::new(Semaphore::new(0));
    let callback: JoinCallback<u32> = {
        let calls = calls.clone();
        let entered = entered.clone();
        let release = release.clone();
        let expired = expired.clone();
        Arc::new(move |_, _, values| {
            let calls = calls.clone();
            let entered = entered.clone();
            let release = release.clone();
            let expired = expired.clone();
            async move {
                assert_eq!(*values[0][0].downcast_ref::<u32>().unwrap(), 42);
                match calls.fetch_add(1, Ordering::SeqCst) {
                    0 => {
                        entered.add_permits(1);
                        release.acquire().await.unwrap().forget();
                    }
                    1 => expired.add_permits(1),
                    n => panic!("unexpected callback {n}"),
                }
                false
            }
            .boxed()
        })
    };
    let input = {
        let store = store.clone();
        tokio::spawn(async move {
            store
                .join_value(MessageContext::new(), 7, 0, Arc::new(42_u32), callback)
                .await
        })
    };
    entered.acquire().await.unwrap().forget();
    advance(ttl * 2).await;
    assert_eq!(
        calls.load(Ordering::SeqCst),
        1,
        "expiry must serialize with the active callback"
    );
    release.add_permits(1);
    assert!(input.await.unwrap());
    // TTL has already elapsed: waiting another full TTL would extend the
    // business timeout by the duration of the first callback.
    timeout(Duration::from_secs(1), expired.acquire())
        .await
        .expect("TTL was restarted after the initial callback")
        .unwrap()
        .forget();
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    store.stop(MessageContext::new()).await;
}

#[tokio::test(start_paused = true)]
async fn renewed_ttl_does_not_expire_at_the_original_deadline() {
    let ttl = Duration::from_secs(10);
    let store = HashMapJoinStorage::<u32>::new(ttl, true);
    let calls = Arc::new(AtomicUsize::new(0));
    let (expired, mut receiver) = mpsc::unbounded_channel();
    let callback: JoinCallback<u32> = {
        let calls = calls.clone();
        Arc::new(move |_, _, values| {
            let calls = calls.clone();
            let expired = expired.clone();
            async move {
                let call = calls.fetch_add(1, Ordering::SeqCst);
                if call == 2 {
                    assert_eq!(*values[0][0].downcast_ref::<u32>().unwrap(), 10);
                    assert_eq!(*values[1][0].downcast_ref::<u32>().unwrap(), 20);
                    expired.send(Instant::now()).unwrap();
                }
                false
            }
            .boxed()
        })
    };
    assert!(
        store
            .join_value(
                MessageContext::new(),
                7,
                0,
                Arc::new(10_u32),
                callback.clone()
            )
            .await
    );
    advance(Duration::from_secs(4)).await;
    let renewed_at = Instant::now();
    assert!(
        store
            .join_value(MessageContext::new(), 7, 1, Arc::new(20_u32), callback)
            .await
    );
    let expired_at = timeout(ttl * 2, receiver.recv()).await.unwrap().unwrap();
    assert!(
        expired_at >= renewed_at + ttl,
        "original timer bypassed renewed TTL"
    );
    assert_eq!(calls.load(Ordering::SeqCst), 3);
    store.stop(MessageContext::new()).await;
}
