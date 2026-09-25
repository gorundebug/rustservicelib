use std::{sync::{Arc, Mutex}, time::Duration};

use futures::{FutureExt, poll};
use servicelib::{MessageContext, runtime::store::{HashMapJoinStorage, JoinCallback, JoinStorage}};
use tokio::sync::Semaphore;

fn callback(
    entered: Arc<Semaphore>,
    release: Arc<Semaphore>,
    observed: Arc<Mutex<Vec<u32>>>,
) -> JoinCallback<u32> {
    Arc::new(move |_context, _key, values| {
        let entered = entered.clone();
        let release = release.clone();
        let observed = observed.clone();
        async move {
            assert_eq!(values[0].len(), 1);
            let value = *values[0][0].downcast_ref::<u32>().unwrap();
            if value == 1 {
                entered.add_permits(1);
                release.acquire().await.unwrap().forget();
            }
            observed.lock().unwrap().push(value);
            true
        }.boxed()
    })
}

#[tokio::test]
async fn waiting_value_survives_previous_join_completion() {
    let store = Arc::new(HashMapJoinStorage::<u32>::new(Duration::ZERO, false));
    let entered = Arc::new(Semaphore::new(0));
    let release = Arc::new(Semaphore::new(0));
    let observed = Arc::new(Mutex::new(Vec::new()));
    let handler = callback(entered.clone(), release.clone(), observed.clone());

    let first_store = store.clone();
    let first_handler = handler.clone();
    let first = tokio::spawn(async move {
        first_store.join_value(MessageContext::new(), 7, 0, Arc::new(1_u32), first_handler).await
    });
    entered.acquire().await.unwrap().forget();

    // Polling to Pending proves this call has reached the occupied key lock;
    // no sleeps or scheduler timing assumptions are needed.
    let waiting = store.join_value(MessageContext::new(), 7, 0, Arc::new(2_u32), handler);
    tokio::pin!(waiting);
    assert!(poll!(&mut waiting).is_pending());
    release.add_permits(1);
    assert!(first.await.unwrap());
    let accepted = tokio::time::timeout(Duration::from_secs(2), &mut waiting).await.unwrap();
    assert!(accepted, "waiting value was discarded after the previous item completed");
    assert_eq!(*observed.lock().unwrap(), vec![1, 2]);
    assert_eq!(store.len().await, 0);
}

#[tokio::test]
async fn independent_join_key_progresses_while_callback_waits() {
    let store = Arc::new(HashMapJoinStorage::<u32>::new(Duration::ZERO, false));
    let entered = Arc::new(Semaphore::new(0));
    let release = Arc::new(Semaphore::new(0));
    let observed = Arc::new(Mutex::new(Vec::new()));
    let handler = callback(entered.clone(), release.clone(), observed.clone());

    let first_store = store.clone();
    let first_handler = handler.clone();
    let first = tokio::spawn(async move {
        first_store.join_value(MessageContext::new(), 7, 0, Arc::new(1_u32), first_handler).await
    });
    entered.acquire().await.unwrap().forget();
    let second = tokio::time::timeout(Duration::from_secs(1),
        store.join_value(MessageContext::new(), 8, 0, Arc::new(2_u32), handler)).await;
    release.add_permits(1);
    assert!(first.await.unwrap());
    assert!(second.expect("unrelated key blocked behind the first callback"));
    assert_eq!(*observed.lock().unwrap(), vec![2, 1]);
    assert_eq!(store.len().await, 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_completed_items_do_not_remove_replacements_or_lose_values() {
    for round in 0..20 {
        let store = Arc::new(HashMapJoinStorage::<u32>::new(Duration::ZERO, false));
        let observed = Arc::new(Mutex::new(Vec::new()));
        let handler: JoinCallback<u32> = {
            let observed = observed.clone();
            Arc::new(move |_context, _key, values| {
                let observed = observed.clone();
                async move {
                    assert_eq!(values[0].len(), 1);
                    let value = *values[0][0].downcast_ref::<u32>().unwrap();
                    tokio::task::yield_now().await;
                    observed.lock().unwrap().push(value);
                    true
                }.boxed()
            })
        };
        let barrier = Arc::new(tokio::sync::Barrier::new(64));
        let mut calls = Vec::new();
        for value in 0..64_u32 {
            let store = store.clone();
            let handler = handler.clone();
            let barrier = barrier.clone();
            calls.push(tokio::spawn(async move {
                barrier.wait().await;
                store.join_value(MessageContext::new(), 7, 0, Arc::new(value), handler).await
            }));
        }
        let results = tokio::time::timeout(Duration::from_secs(5),
            futures::future::join_all(calls)).await.expect("same-key contenders did not finish");
        for result in results {
            assert!(result.unwrap(), "round {round}: contender lost its value");
        }
        let mut values = observed.lock().unwrap().clone();
        values.sort_unstable();
        assert_eq!(values, (0..64).collect::<Vec<_>>(), "round {round}");
        assert_eq!(store.len().await, 0, "round {round}: completed entry remained");
    }
}
