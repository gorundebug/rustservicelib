use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use std::time::Duration;

use tokio::sync::Barrier;

use servicelib::{MessageContext, runtime::pool::DelayPool};

#[tokio::test]
async fn suspended_callback_does_not_block_other_delays_and_stop_drains_both() {
    tokio::time::timeout(Duration::from_secs(5), async {
        let pool = DelayPool::new();
        let (entered, entry) = tokio::sync::oneshot::channel();
        let (release, resume) = tokio::sync::oneshot::channel();
        let finished = Arc::new(AtomicUsize::new(0));
        let first_finished = finished.clone();
        pool.delay(MessageContext::new(), Duration::ZERO, async move {
            entered.send(()).unwrap();
            resume.await.unwrap();
            first_finished.fetch_add(1, Ordering::SeqCst);
        })
        .await
        .unwrap();
        entry.await.unwrap();
        let (second, second_finished) = tokio::sync::oneshot::channel();
        pool.delay(MessageContext::new(), Duration::ZERO, async move {
            second.send(()).unwrap();
        })
        .await
        .unwrap();
        second_finished.await.unwrap();

        let mut stop = Box::pin(pool.stop());
        assert!(futures::poll!(stop.as_mut()).is_pending());
        assert!(
            pool.delay(MessageContext::new(), Duration::ZERO, async {})
                .await
                .is_err()
        );
        assert_eq!(finished.load(Ordering::SeqCst), 0);
        release.send(()).unwrap();
        stop.await;
        assert_eq!(finished.load(Ordering::SeqCst), 1);
    })
    .await
    .unwrap();
}

#[tokio::test(start_paused = true)]
async fn accepted_cancelled_delay_still_executes_callback_once() {
    let pool = DelayPool::new();
    let context = MessageContext::new().with_stream_id("accepted-delay");
    let callback_context = context.clone();
    let (finished, completion) = tokio::sync::oneshot::channel();
    pool.delay(context.clone(), Duration::from_secs(3600), async move {
        assert_eq!(callback_context.stream_id(), Some("accepted-delay"));
        assert!(callback_context.is_cancelled());
        finished.send(()).unwrap();
    })
    .await
    .unwrap();
    context.cancel();
    tokio::time::timeout(Duration::from_secs(1), completion)
        .await
        .unwrap()
        .unwrap();
    pool.stop().await;
}

#[tokio::test(start_paused = true)]
async fn admission_deadline_expedites_callback_without_restarting_delay() {
    let pool = DelayPool::new();
    let started = tokio::time::Instant::now();
    let context = MessageContext::with_deadline(started + Duration::from_secs(2));
    let (finished, completion) = tokio::sync::oneshot::channel();
    pool.delay(context, Duration::from_secs(3600), async move {
        finished.send(tokio::time::Instant::now()).unwrap();
    })
    .await
    .unwrap();
    tokio::time::advance(Duration::from_secs(2)).await;
    let completed_at = tokio::time::timeout(Duration::from_secs(1), completion)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(completed_at.duration_since(started), Duration::from_secs(2));
    pool.stop().await;
}

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
