use std::{sync::Arc, time::Duration};

use futures::FutureExt;
use servicelib::{
    MessageContext,
    runtime::store::{HashMapJoinStorage, JoinCallback, JoinStorage},
};

#[tokio::test]
async fn completed_join_releases_values_and_callback_before_ttl() {
    let store = HashMapJoinStorage::<u32>::new(Duration::from_secs(3600), false);
    let payload = Arc::new("large request retained by the first input".repeat(1024));
    let payload_lifetime = Arc::downgrade(&payload);
    let callback_state = Arc::new(String::from("callback-owned request state"));
    let callback_lifetime = Arc::downgrade(&callback_state);
    let pending: JoinCallback<u32> = Arc::new(move |_context, _key, _values| {
        let state = callback_state.clone();
        async move {
            assert!(!state.is_empty());
            false
        }
        .boxed()
    });
    assert!(
        store
            .join_value(MessageContext::new(), 7, 0, payload, pending)
            .await
    );
    assert_eq!(store.len().await, 1);

    let complete: JoinCallback<u32> = Arc::new(|_context, _key, values| {
        async move {
            assert_eq!(values[0].len(), 1);
            assert_eq!(values[1].len(), 1);
            true
        }
        .boxed()
    });
    assert!(
        store
            .join_value(MessageContext::new(), 7, 1, Arc::new(42_u32), complete)
            .await
    );
    assert_eq!(
        store.len().await,
        0,
        "the completed key must already be absent"
    );

    // Cancellation cleanup may run on the next scheduler turn, but must not
    // retain completed request state until the one-hour TTL expires.
    let released = tokio::time::timeout(Duration::from_secs(2), async {
        while payload_lifetime.upgrade().is_some() || callback_lifetime.upgrade().is_some() {
            tokio::task::yield_now().await;
        }
    })
    .await;
    assert!(
        released.is_ok(),
        "completed Join still retains values or callback through its TTL task"
    );
}
