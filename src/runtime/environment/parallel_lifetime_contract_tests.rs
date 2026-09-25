use super::RuntimeEnvironment;
use crate::runtime::config::CallSemantics;
use std::{sync::Arc, time::Duration};

#[tokio::test]
async fn completed_parallel_callbacks_release_captures_before_shutdown() {
    let environment = RuntimeEnvironment::new(CallSemantics::ParallelCall);
    let mut captures = Vec::new();

    for batch in 1..=2 {
        for _ in 0..256 {
            let payload = Arc::new(vec![42_u8; 4096]);
            captures.push(Arc::downgrade(&payload));
            environment.spawn_parallel(async move {
                tokio::task::yield_now().await;
                assert_eq!(payload[0], 42);
            });
        }

        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let finished = environment
                    .parallel_tasks
                    .lock()
                    .expect("task registry")
                    .iter()
                    .all(tokio::task::JoinHandle::is_finished);
                if finished {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("accepted parallel callbacks must finish");

        let retained_handles = environment
            .parallel_tasks
            .lock()
            .expect("task registry")
            .len();
        let retained_captures = captures
            .iter()
            .filter(|capture| capture.upgrade().is_some())
            .count();
        // Observe registry growth without making the current storage strategy
        // the required contract. The semantic assertion concerns owned data.
        eprintln!(
            "parallel lifetime: completed={}, handles={retained_handles}, payloads={retained_captures}",
            batch * 256
        );
        assert_eq!(retained_captures, 0, "completed callbacks retain payloads");
    }

    environment.drain_parallel().await;
    assert!(
        environment
            .parallel_tasks
            .lock()
            .expect("task registry")
            .is_empty(),
        "shutdown drain must release the registry"
    );
}
