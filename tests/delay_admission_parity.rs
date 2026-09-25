use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use std::time::Duration;

use servicelib::{MessageContext, runtime::pool::DelayPool};
use tokio::sync::Barrier;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn delay_admission_cancel_stop_race_preserves_exactly_accepted_callbacks() {
    for round in 0..200 {
        let pool = DelayPool::new();
        let context = MessageContext::new();
        let barrier = Arc::new(Barrier::new(3));
        let executions = Arc::new(AtomicUsize::new(0));
        let payload = Arc::new(round);
        let weak_payload = Arc::downgrade(&payload);

        let admit_pool = pool.clone();
        let admit_context = context.clone();
        let admit_barrier = barrier.clone();
        let callback_executions = executions.clone();
        let admission = tokio::spawn(async move {
            admit_barrier.wait().await;
            admit_pool
                .delay(admit_context, Duration::from_secs(3600), async move {
                    assert_eq!(*payload, round);
                    callback_executions.fetch_add(1, Ordering::SeqCst);
                })
                .await
                .is_ok()
        });

        let cancel_barrier = barrier.clone();
        let cancellation = tokio::spawn(async move {
            cancel_barrier.wait().await;
            context.cancel();
        });

        let stop_pool = pool.clone();
        let stop = tokio::spawn(async move {
            barrier.wait().await;
            stop_pool.stop().await;
        });

        let accepted = tokio::time::timeout(Duration::from_secs(5), async {
            let accepted = admission.await.unwrap();
            cancellation.await.unwrap();
            stop.await.unwrap();
            accepted
        })
        .await
        .expect("admission/cancellation/stop must finish without the one-hour timer");

        assert_eq!(
            executions.load(Ordering::SeqCst),
            usize::from(accepted),
            "round {round}: callback execution must match admission"
        );
        assert!(
            weak_payload.upgrade().is_none(),
            "round {round}: retained payload"
        );
        assert!(
            pool.delay(MessageContext::new(), Duration::ZERO, async {})
                .await
                .is_err(),
            "closed pool must not reopen"
        );
    }
}
