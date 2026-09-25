use std::{sync::Arc, time::Duration};

use futures::FutureExt;
use servicelib::{
    MessageContext,
    runtime::store::{HashMapJoinStorage, JoinCallback, JoinStorage, Storage},
};
use tokio::{sync::mpsc, time::timeout};

type Observation = (&'static str, String, Vec<Vec<u32>>);

fn callback(label: &'static str, sender: mpsc::UnboundedSender<Observation>) -> JoinCallback<u32> {
    Arc::new(move |context, _, values| {
        let sender = sender.clone();
        async move {
            sender
                .send((
                    label,
                    context.stream_id().unwrap().to_owned(),
                    values
                        .iter()
                        .map(|slot| {
                            slot.iter()
                                .map(|value| *value.downcast_ref::<u32>().unwrap())
                                .collect()
                        })
                        .collect(),
                ))
                .unwrap();
            false
        }
        .boxed()
    })
}

async fn check_renewal_identity(context_deadline: bool) {
    let store = HashMapJoinStorage::<u32>::new(Duration::from_secs(10), true);
    let first = MessageContext::new().with_stream_id("first-context");
    let second = MessageContext::new().with_stream_id("second-context");
    let (first, second) = if context_deadline {
        (
            first.with_timeout_limit(Duration::from_secs(3600)),
            second.with_timeout_limit(Duration::from_secs(3600)),
        )
    } else {
        (first, second)
    };
    let (sender, mut receiver) = mpsc::unbounded_channel();
    store
        .join_value(
            first.clone(),
            7,
            0,
            Arc::new(10_u32),
            callback("first", sender.clone()),
        )
        .await;
    store
        .join_value(
            second.clone(),
            7,
            1,
            Arc::new(20_u32),
            callback("second", sender),
        )
        .await;
    assert_eq!(receiver.recv().await.unwrap().0, "first");
    assert_eq!(receiver.recv().await.unwrap().0, "second");
    if context_deadline {
        first.cancel();
    }
    let expired = timeout(
        Duration::from_secs(if context_deadline { 1 } else { 11 }),
        receiver.recv(),
    )
    .await;
    // Cancel the still-pending entry before asserting on the old behavior.
    let complete: JoinCallback<u32> = Arc::new(|_, _, _| async { true }.boxed());
    store
        .join_value(second, 7, 2, Arc::new(30_u32), complete)
        .await;
    store.stop(MessageContext::new()).await;
    let (label, context_id, values) = expired
        .expect("renewal detached expiry from the first context")
        .unwrap();
    assert_eq!(
        label, "first",
        "renewal replaced the accepted group's expiry callback"
    );
    assert_eq!(context_id, "first-context");
    assert_eq!(values, vec![vec![10], vec![20]]);
}

#[tokio::test(start_paused = true)]
async fn renewal_keeps_first_expiry_callback_and_context() {
    check_renewal_identity(false).await;
}

#[tokio::test(start_paused = true)]
async fn renewal_keeps_first_context_cancellation() {
    check_renewal_identity(true).await;
}
