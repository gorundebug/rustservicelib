use std::{sync::{Arc, atomic::{AtomicUsize, Ordering}}, time::Duration};

use futures::FutureExt;
use servicelib::{MessageContext, runtime::store::{HashMapJoinStorage, JoinCallback, JoinStorage, Storage}};
use tokio::{sync::mpsc, time::{Instant, advance, timeout}};

#[tokio::test(start_paused = true)]
async fn renewal_preserves_the_absolute_context_deadline() {
    let store = HashMapJoinStorage::<u32>::new(Duration::from_secs(3600), true);
    let start = Instant::now();
    let context = MessageContext::new().with_timeout_limit(Duration::from_secs(10));
    let calls = Arc::new(AtomicUsize::new(0));
    let (sender, mut receiver) = mpsc::unbounded_channel();
    let callback: JoinCallback<u32> = {
        let calls = calls.clone();
        Arc::new(move |context, _, values| {
            let calls = calls.clone();
            let sender = sender.clone();
            async move {
                if calls.fetch_add(1, Ordering::SeqCst) == 2 {
                    assert!(context.is_cancelled());
                    assert_eq!(*values[0][0].downcast_ref::<u32>().unwrap(), 10);
                    assert_eq!(*values[1][0].downcast_ref::<u32>().unwrap(), 20);
                    sender.send(Instant::now()).unwrap();
                }
                false
            }.boxed()
        })
    };
    assert!(store.join_value(context.clone(), 7, 0, Arc::new(10_u32), callback.clone()).await);
    advance(Duration::from_secs(4)).await;
    assert!(store.join_value(context, 7, 1, Arc::new(20_u32), callback).await);
    let expired = timeout(Duration::from_secs(7), receiver.recv()).await
        .expect("renewal extended the context deadline").unwrap();
    assert_eq!(expired, start + Duration::from_secs(10));
    store.stop(MessageContext::new()).await;
}

#[tokio::test]
async fn cancelling_a_context_after_renewal_still_delivers_expiry() {
    let store = HashMapJoinStorage::<u32>::new(Duration::from_secs(3600), true);
    let context = MessageContext::new().with_timeout_limit(Duration::from_secs(3600));
    let calls = Arc::new(AtomicUsize::new(0));
    let (sender, mut receiver) = mpsc::unbounded_channel();
    let callback: JoinCallback<u32> = {
        let calls = calls.clone();
        Arc::new(move |context, _, _| {
            let calls = calls.clone();
            let sender = sender.clone();
            async move {
                if calls.fetch_add(1, Ordering::SeqCst) == 2 {
                    assert!(context.is_cancelled());
                    sender.send(()).unwrap();
                }
                false
            }.boxed()
        })
    };
    for index in 0..2 {
        assert!(store.join_value(context.clone(), 7, index, Arc::new(42_u32), callback.clone()).await);
    }
    context.cancel();
    timeout(Duration::from_secs(1), receiver.recv()).await.unwrap().unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 3);
    store.stop(MessageContext::new()).await;
}
