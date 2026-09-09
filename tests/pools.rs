use std::{sync::Arc, time::Duration};

use serde::{Deserialize, Serialize};
use servicelib::{
    MessageContext,
    runtime::{
        config::{Config, PoolConfig, RuntimeConfig, ServiceConfig},
        environment::{RuntimeEnvironment, RuntimeError},
        pool::{DelayPool, PriorityTaskPool, TaskPool},
    },
};
use tokio::sync::{Notify, mpsc};

#[derive(Clone, Serialize, Deserialize)]
struct PoolTestConfig {
    #[serde(skip)]
    pools: Vec<PoolConfig>,
}

impl Config for PoolTestConfig {
    fn apply_environment(&mut self) -> Result<(), String> {
        Ok(())
    }

    fn pools(&self) -> Vec<PoolConfig> {
        self.pools.clone()
    }

    fn services(&self) -> Vec<ServiceConfig> {
        vec![ServiceConfig {
            id: 1,
            name: "orders".to_owned(),
            ..ServiceConfig::default()
        }]
    }
}

fn pool_environment(pools: &[(&str, usize)]) -> RuntimeEnvironment {
    let environment = RuntimeEnvironment::default();
    let config = PoolTestConfig {
        pools: pools
            .iter()
            .map(|(name, executors_count)| PoolConfig {
                name: (*name).to_owned(),
                executors_count: *executors_count,
                queue_capacity: 0,
            })
            .collect(),
    };
    environment.publish_runtime_config(Arc::new(RuntimeConfig::new(&config).unwrap()));
    environment.for_service(1)
}

#[tokio::test]
async fn fifo_pool_expedites_a_cancelled_queued_task() {
    let pool = TaskPool::new("fifo", pool_environment(&[("fifo", 1)])).unwrap();
    pool.start().unwrap();
    let (sender, mut receiver) = mpsc::unbounded_channel();
    let release = Arc::new(Notify::new());
    let first_release = Arc::clone(&release);
    let first_sender = sender.clone();
    pool.add_task(
        MessageContext::new(),
        Box::pin(async move {
            first_sender.send(1).unwrap();
            first_release.notified().await;
        }),
    )
    .await
    .unwrap();

    assert_eq!(receiver.recv().await, Some(1));

    let mut queued = Vec::new();
    for value in [2, 3] {
        let sender = sender.clone();
        let context = MessageContext::new();
        if value == 3 {
            queued.push(context.clone());
        }
        pool.add_task(
            context,
            Box::pin(async move {
                sender.send(value).unwrap();
            }),
        )
        .await
        .unwrap();
    }
    queued[0].cancel();
    tokio::task::yield_now().await;
    release.notify_one();
    assert_eq!(receiver.recv().await, Some(3));
    assert_eq!(receiver.recv().await, Some(2));
    pool.stop().await;
}

#[tokio::test]
async fn priority_pool_is_stable_and_cancelled_task_becomes_first() {
    let pool = PriorityTaskPool::new("priority", pool_environment(&[("priority", 1)])).unwrap();
    pool.start().unwrap();
    let (sender, mut receiver) = mpsc::unbounded_channel();
    let release = Arc::new(Notify::new());
    let first_release = Arc::clone(&release);
    let first_sender = sender.clone();
    pool.add_task(
        MessageContext::new(),
        0,
        Box::pin(async move {
            first_sender.send(1).unwrap();
            first_release.notified().await;
        }),
    )
    .await
    .unwrap();
    assert_eq!(receiver.recv().await, Some(1));

    for value in [2, 3] {
        let sender = sender.clone();
        pool.add_task(
            MessageContext::new(),
            10,
            Box::pin(async move {
                sender.send(value).unwrap();
            }),
        )
        .await
        .unwrap();
    }
    let cancelled = MessageContext::new();
    let cancelled_sender = sender.clone();
    pool.add_task(
        cancelled.clone(),
        100,
        Box::pin(async move {
            cancelled_sender.send(4).unwrap();
        }),
    )
    .await
    .unwrap();
    cancelled.cancel();
    tokio::time::sleep(Duration::from_millis(1)).await;
    release.notify_one();

    assert_eq!(receiver.recv().await, Some(4));
    assert_eq!(receiver.recv().await, Some(2));
    assert_eq!(receiver.recv().await, Some(3));
    pool.stop().await;
}

#[tokio::test]
async fn fifo_pool_monitors_cancellation_for_a_large_queue() {
    let pool = TaskPool::new("fifo-large", pool_environment(&[("fifo-large", 1)])).unwrap();
    pool.start().unwrap();
    let (sender, mut receiver) = mpsc::unbounded_channel();
    let release = Arc::new(Notify::new());
    let first_release = Arc::clone(&release);
    pool.add_task(
        MessageContext::new(),
        Box::pin(async move {
            first_release.notified().await;
        }),
    )
    .await
    .unwrap();

    let mut cancelled = None;
    for value in 2..=257 {
        let sender = sender.clone();
        let context = MessageContext::new();
        if value == 257 {
            cancelled = Some(context.clone());
        }
        pool.add_task(
            context,
            Box::pin(async move {
                sender.send(value).unwrap();
            }),
        )
        .await
        .unwrap();
    }

    cancelled.unwrap().cancel();
    tokio::task::yield_now().await;
    release.notify_one();
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(1), receiver.recv())
            .await
            .unwrap(),
        Some(257)
    );
    assert_eq!(receiver.recv().await, Some(2));
    pool.stop().await;
}

#[tokio::test]
async fn priority_pool_monitors_cancellation_for_a_large_queue() {
    let pool = PriorityTaskPool::new("priority-large", pool_environment(&[("priority-large", 1)]))
        .unwrap();
    pool.start().unwrap();
    let (sender, mut receiver) = mpsc::unbounded_channel();
    let release = Arc::new(Notify::new());
    let first_release = Arc::clone(&release);
    pool.add_task(
        MessageContext::new(),
        0,
        Box::pin(async move {
            first_release.notified().await;
        }),
    )
    .await
    .unwrap();

    let mut cancelled = None;
    for value in 2..=257 {
        let sender = sender.clone();
        let context = MessageContext::new();
        if value == 257 {
            cancelled = Some(context.clone());
        }
        pool.add_task(
            context,
            10,
            Box::pin(async move {
                sender.send(value).unwrap();
            }),
        )
        .await
        .unwrap();
    }

    cancelled.unwrap().cancel();
    tokio::task::yield_now().await;
    release.notify_one();
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(1), receiver.recv())
            .await
            .unwrap(),
        Some(257)
    );
    assert_eq!(receiver.recv().await, Some(2));
    pool.stop().await;
}

#[tokio::test]
async fn task_pool_queues_before_start_and_rejects_after_stop() {
    let pool = TaskPool::new("lifecycle", pool_environment(&[("lifecycle", 1)])).unwrap();
    let (sender, mut receiver) = mpsc::unbounded_channel();
    pool.add_task(
        MessageContext::new(),
        Box::pin(async move {
            sender.send(1).unwrap();
        }),
    )
    .await
    .unwrap();

    assert!(receiver.try_recv().is_err());
    pool.start().unwrap();
    assert_eq!(receiver.recv().await, Some(1));
    assert!(matches!(
        pool.start(),
        Err(RuntimeError::ResourceAlreadyStarted(_))
    ));
    pool.stop().await;
    assert!(matches!(
        pool.add_task(MessageContext::new(), Box::pin(async {}))
            .await,
        Err(RuntimeError::ResourceStopped(_))
    ));
}

#[tokio::test]
async fn task_pools_drain_queued_tasks_when_stopped_before_start() {
    let environment = pool_environment(&[("fifo", 1), ("priority", 1)]);
    let fifo = TaskPool::new("fifo", environment.clone()).unwrap();
    let priority = PriorityTaskPool::new("priority", environment).unwrap();
    let (sender, mut receiver) = mpsc::unbounded_channel();

    let fifo_sender = sender.clone();
    fifo.add_task(
        MessageContext::new(),
        Box::pin(async move {
            fifo_sender.send("fifo").unwrap();
        }),
    )
    .await
    .unwrap();
    priority
        .add_task(
            MessageContext::new(),
            0,
            Box::pin(async move {
                sender.send("priority").unwrap();
            }),
        )
        .await
        .unwrap();

    fifo.stop().await;
    priority.stop().await;

    let mut completed = vec![
        receiver.recv().await.unwrap(),
        receiver.recv().await.unwrap(),
    ];
    completed.sort_unstable();
    assert_eq!(completed, ["fifo", "priority"]);
}

#[tokio::test]
async fn task_pool_resizes_from_the_published_runtime_config() {
    let environment = pool_environment(&[("resize", 1)]);
    let pool = TaskPool::new("resize", environment.clone()).unwrap();
    environment.register_task_pool(pool.clone()).unwrap();
    pool.start().unwrap();

    let release = Arc::new(Notify::new());
    let first_release = Arc::clone(&release);
    let (started, mut starts) = mpsc::unbounded_channel();
    let first_started = started.clone();
    pool.add_task(
        MessageContext::new(),
        Box::pin(async move {
            first_started.send(1).unwrap();
            first_release.notified().await;
        }),
    )
    .await
    .unwrap();
    assert_eq!(starts.recv().await, Some(1));
    pool.add_task(
        MessageContext::new(),
        Box::pin(async move {
            started.send(2).unwrap();
        }),
    )
    .await
    .unwrap();
    assert!(starts.try_recv().is_err());

    let resized = PoolTestConfig {
        pools: vec![PoolConfig {
            name: "resize".to_owned(),
            executors_count: 2,
            queue_capacity: 0,
        }],
    };
    environment.publish_runtime_config(Arc::new(RuntimeConfig::new(&resized).unwrap()));
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(1), starts.recv())
            .await
            .unwrap(),
        Some(2)
    );
    release.notify_one();
    pool.stop().await;
}

#[tokio::test]
async fn delay_pool_expedites_on_cancel_and_rejects_after_stop() {
    let pool = DelayPool::new();
    let context = MessageContext::new();
    let (sender, mut receiver) = mpsc::unbounded_channel();
    pool.delay(context.clone(), Duration::from_secs(60), async move {
        sender.send(1).unwrap();
    })
    .await
    .unwrap();
    context.cancel();
    assert_eq!(receiver.recv().await, Some(1));
    pool.stop().await;
    assert!(matches!(
        pool.delay(MessageContext::new(), Duration::ZERO, async {})
            .await,
        Err(RuntimeError::ResourceStopped(_))
    ));
}

#[tokio::test]
async fn delay_pool_stop_timeout_reports_but_still_drains_accepted_task() {
    let pool = DelayPool::new();
    let release = Arc::new(Notify::new());
    let task_release = Arc::clone(&release);
    let (started_tx, mut started_rx) = mpsc::unbounded_channel();
    let (completed_tx, mut completed_rx) = mpsc::unbounded_channel();
    pool.delay(MessageContext::new(), Duration::ZERO, async move {
        started_tx.send(()).unwrap();
        task_release.notified().await;
        completed_tx.send(()).unwrap();
    })
    .await
    .unwrap();
    started_rx.recv().await.unwrap();

    let stop_context = MessageContext::new();
    stop_context.cancel();
    let stop_pool = Arc::clone(&pool);
    let stop = tokio::spawn(async move {
        stop_pool.stop_with_context(stop_context).await;
    });
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(!stop.is_finished());
    assert!(completed_rx.try_recv().is_err());

    release.notify_one();
    tokio::time::timeout(Duration::from_secs(1), completed_rx.recv())
        .await
        .unwrap()
        .unwrap();
    tokio::time::timeout(Duration::from_secs(1), stop)
        .await
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn delay_pool_wait_queue_length_includes_executing_callback() {
    let environment = pool_environment(&[]);
    let pool = environment.delay_pool();
    let release = Arc::new(Notify::new());
    let task_release = Arc::clone(&release);
    let (started_tx, mut started_rx) = mpsc::unbounded_channel();
    pool.delay(MessageContext::new(), Duration::ZERO, async move {
        started_tx.send(()).unwrap();
        task_release.notified().await;
    })
    .await
    .unwrap();
    started_rx.recv().await.unwrap();

    assert!(
        environment
            .metrics()
            .render_prometheus()
            .contains(r#"delay_pool_wait_queue_length{service="orders"} 1"#)
    );

    release.notify_one();
    pool.stop().await;
    assert!(
        environment
            .metrics()
            .render_prometheus()
            .contains(r#"delay_pool_wait_queue_length{service="orders"} 0"#)
    );
}

#[tokio::test]
async fn registered_pools_publish_the_go_metric_contract() {
    let environment = pool_environment(&[("default", 1), ("priority", 1)]);
    let task_pool = TaskPool::new("default", environment.clone()).unwrap();
    let priority_pool = PriorityTaskPool::new("priority", environment.clone()).unwrap();
    environment.register_task_pool(task_pool.clone()).unwrap();
    environment
        .register_priority_task_pool(priority_pool.clone())
        .unwrap();
    task_pool.start().unwrap();
    priority_pool.start().unwrap();

    task_pool
        .add_task(MessageContext::new(), Box::pin(async {}))
        .await
        .unwrap();
    priority_pool
        .add_task(MessageContext::new(), 10, Box::pin(async {}))
        .await
        .unwrap();
    environment
        .delay_pool()
        .delay(MessageContext::new(), Duration::ZERO, async {})
        .await
        .unwrap();
    task_pool.stop().await;
    priority_pool.stop().await;
    environment.delay_pool().stop().await;

    let metrics = environment.metrics().render_prometheus();
    for name in [
        "task_pool_queue_length",
        "task_pool_executors_target",
        "task_pool_executors_allocated",
        "task_pool_executors_busy",
        "task_pool_tasks_total",
        "task_pool_task_execution_duration_seconds",
        "priority_task_pool_queue_length",
        "priority_task_pool_tasks_total",
        "delay_pool_wait_queue_length",
        "delay_pool_tasks_total",
    ] {
        assert!(metrics.contains(name), "missing metric {name}");
    }
}

#[tokio::test]
async fn task_panic_is_logged_without_permanently_losing_an_executor() {
    let pool = TaskPool::new("panic-safe", pool_environment(&[("panic-safe", 1)])).unwrap();
    pool.start().unwrap();
    pool.add_task(
        MessageContext::new(),
        Box::pin(async {
            panic!("expected task panic");
        }),
    )
    .await
    .unwrap();

    let (sender, mut receiver) = mpsc::unbounded_channel();
    pool.add_task(
        MessageContext::new(),
        Box::pin(async move {
            sender.send(()).unwrap();
        }),
    )
    .await
    .unwrap();
    assert_eq!(receiver.recv().await, Some(()));
    pool.stop().await;
}

#[tokio::test]
async fn stop_timeout_is_observable_but_pool_still_drains_safely() {
    let environment = pool_environment(&[("slow", 1)]);
    let pool = TaskPool::new("slow", environment.clone()).unwrap();
    environment.register_task_pool(pool.clone()).unwrap();
    pool.start().unwrap();
    let release = Arc::new(Notify::new());
    let task_release = Arc::clone(&release);
    let (started_tx, mut started_rx) = mpsc::unbounded_channel();
    let (completed_tx, mut completed_rx) = mpsc::unbounded_channel();
    pool.add_task(
        MessageContext::new(),
        Box::pin(async move {
            started_tx.send(()).unwrap();
            task_release.notified().await;
            completed_tx.send(()).unwrap();
        }),
    )
    .await
    .unwrap();
    started_rx.recv().await.unwrap();

    let context = MessageContext::new();
    context.cancel();
    let stop_pool = Arc::clone(&pool);
    let stop = tokio::spawn(async move {
        stop_pool.stop_with_context(context).await;
    });
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(!stop.is_finished());
    assert!(completed_rx.try_recv().is_err());

    assert!(environment.metrics().render_prometheus().contains(
        r#"task_pool_events_total{event="stop_timeout",name="slow",service="orders"} 1"#
    ));

    release.notify_one();
    tokio::time::timeout(Duration::from_secs(1), completed_rx.recv())
        .await
        .unwrap()
        .unwrap();
    tokio::time::timeout(Duration::from_secs(1), stop)
        .await
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn priority_pool_stop_timeout_reports_but_still_drains_worker() {
    let pool = PriorityTaskPool::new("slow", pool_environment(&[("slow", 1)])).unwrap();
    pool.start().unwrap();
    let release = Arc::new(Notify::new());
    let task_release = Arc::clone(&release);
    let (started_tx, mut started_rx) = mpsc::unbounded_channel();
    let (completed_tx, mut completed_rx) = mpsc::unbounded_channel();
    pool.add_task(
        MessageContext::new(),
        0,
        Box::pin(async move {
            started_tx.send(()).unwrap();
            task_release.notified().await;
            completed_tx.send(()).unwrap();
        }),
    )
    .await
    .unwrap();
    started_rx.recv().await.unwrap();

    let stop_context = MessageContext::new();
    stop_context.cancel();
    let stop_pool = Arc::clone(&pool);
    let stop = tokio::spawn(async move {
        stop_pool.stop_with_context(stop_context).await;
    });
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(!stop.is_finished());
    assert!(completed_rx.try_recv().is_err());

    release.notify_one();
    tokio::time::timeout(Duration::from_secs(1), completed_rx.recv())
        .await
        .unwrap()
        .unwrap();
    tokio::time::timeout(Duration::from_secs(1), stop)
        .await
        .unwrap()
        .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn delay_callbacks_have_independent_tasks_and_do_not_serialize() {
    let pool = DelayPool::new();
    let (started_tx, started_rx) = tokio::sync::oneshot::channel();
    let (release_tx, release_rx) = tokio::sync::oneshot::channel();
    pool.delay(
        MessageContext::new(),
        Duration::from_millis(1),
        async move {
            let _ = started_tx.send(tokio::task::id());
            let _ = release_rx.await;
        },
    )
    .await
    .unwrap();
    let first_id = started_rx.await.unwrap();
    let (second_tx, second_rx) = tokio::sync::oneshot::channel();
    pool.delay(
        MessageContext::new(),
        Duration::from_millis(1),
        async move {
            let _ = second_tx.send(tokio::task::id());
        },
    )
    .await
    .unwrap();
    let second = tokio::time::timeout(Duration::from_secs(2), second_rx).await;
    let _ = release_tx.send(());
    pool.stop().await;
    assert_ne!(first_id, second.unwrap().unwrap());
}

#[tokio::test(start_paused = true)]
async fn delay_deadline_is_measured_from_admission_not_scheduler_poll() {
    let pool = DelayPool::new();
    let (tx, rx) = tokio::sync::oneshot::channel();
    pool.delay(MessageContext::new(), Duration::from_secs(1), async move {
        let _ = tx.send(());
    })
    .await
    .unwrap();
    tokio::time::advance(Duration::from_secs(2)).await;
    tokio::time::timeout(Duration::from_millis(1), rx)
        .await
        .unwrap()
        .unwrap();
    pool.stop().await;
}

#[tokio::test]
async fn delay_concurrent_stops_both_wait_for_running_callback() {
    let pool = DelayPool::new();
    let (started_tx, started_rx) = tokio::sync::oneshot::channel();
    let (release_tx, release_rx) = tokio::sync::oneshot::channel();
    pool.delay(MessageContext::new(), Duration::ZERO, async move {
        let _ = started_tx.send(());
        let _ = release_rx.await;
    })
    .await
    .unwrap();
    started_rx.await.unwrap();
    let first_pool = Arc::clone(&pool);
    let first = tokio::spawn(async move { first_pool.stop().await });
    let second_pool = Arc::clone(&pool);
    let second = tokio::spawn(async move { second_pool.stop().await });
    tokio::task::yield_now().await;
    assert!(!first.is_finished());
    assert!(!second.is_finished());
    let _ = release_tx.send(());
    first.await.unwrap();
    second.await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn delay_cancelled_entries_execute_once_and_release_payloads() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    let pool = DelayPool::new();
    let context = MessageContext::new();
    let payload = Arc::new(42);
    let weak = Arc::downgrade(&payload);
    let executions = Arc::new(AtomicUsize::new(0));
    for _ in 0..1000 {
        let payload = Arc::clone(&payload);
        let executions = Arc::clone(&executions);
        pool.delay(context.clone(), Duration::from_secs(3600), async move {
            assert_eq!(*payload, 42);
            executions.fetch_add(1, Ordering::Relaxed);
        })
        .await
        .unwrap();
    }
    drop(payload);
    context.cancel();
    tokio::time::timeout(Duration::from_secs(5), pool.stop())
        .await
        .unwrap();
    assert_eq!(executions.load(Ordering::Relaxed), 1000);
    assert!(weak.upgrade().is_none());
}

#[tokio::test]
async fn fifo_concurrent_stop_must_drain() {
    let pool = TaskPool::new("review", pool_environment(&[("review", 1)])).unwrap();
    pool.start().unwrap();
    let (started_tx, started_rx) = tokio::sync::oneshot::channel();
    let (release_tx, release_rx) = tokio::sync::oneshot::channel();
    pool.add_task(
        MessageContext::new(),
        Box::pin(async move {
            let _ = started_tx.send(());
            let _ = release_rx.await;
        }),
    )
    .await
    .unwrap();
    started_rx.await.unwrap();
    let first_pool = Arc::clone(&pool);
    let first = tokio::spawn(async move { first_pool.stop().await });
    tokio::task::yield_now().await;
    let second_returned = tokio::time::timeout(Duration::from_millis(20), pool.stop())
        .await
        .is_ok();
    let _ = release_tx.send(());
    first.await.unwrap();
    assert!(
        !second_returned,
        "second stop returned while accepted callback was still blocked"
    );
}

#[tokio::test]
async fn priority_concurrent_stop_must_drain() {
    let pool = PriorityTaskPool::new("review", pool_environment(&[("review", 1)])).unwrap();
    pool.start().unwrap();
    let (started_tx, started_rx) = tokio::sync::oneshot::channel();
    let (release_tx, release_rx) = tokio::sync::oneshot::channel();
    pool.add_task(
        MessageContext::new(),
        0,
        Box::pin(async move {
            let _ = started_tx.send(());
            let _ = release_rx.await;
        }),
    )
    .await
    .unwrap();
    started_rx.await.unwrap();
    let first_pool = Arc::clone(&pool);
    let first = tokio::spawn(async move { first_pool.stop().await });
    tokio::task::yield_now().await;
    let second_returned = tokio::time::timeout(Duration::from_millis(20), pool.stop())
        .await
        .is_ok();
    let _ = release_tx.send(());
    first.await.unwrap();
    assert!(
        !second_returned,
        "second stop returned while accepted callback was still blocked"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fifo_uses_configured_workers_and_zero_means_cpu_count() {
    for configured in [2, 0] {
        let expected = if configured == 0 {
            std::thread::available_parallelism().map_or(1, usize::from)
        } else {
            configured
        };
        let pool = TaskPool::new("workers", pool_environment(&[("workers", configured)])).unwrap();
        let release = Arc::new(tokio::sync::Semaphore::new(0));
        let (tx, mut rx) = mpsc::unbounded_channel();
        // Before start the work is accepted, but it must not execute yet.
        for _ in 0..expected + 1 {
            let tx = tx.clone();
            let release = Arc::clone(&release);
            pool.add_task(
                MessageContext::new(),
                Box::pin(async move {
                    tx.send(tokio::task::id()).unwrap();
                    release.acquire().await.unwrap().forget();
                }),
            )
            .await
            .unwrap();
        }
        assert!(rx.try_recv().is_err());
        pool.start().unwrap();
        let mut ids = std::collections::HashSet::new();
        for _ in 0..expected {
            ids.insert(
                tokio::time::timeout(Duration::from_secs(2), rx.recv())
                    .await
                    .unwrap()
                    .unwrap(),
            );
        }
        assert!(
            tokio::time::timeout(Duration::from_millis(10), rx.recv())
                .await
                .is_err()
        );
        release.add_permits(expected + 1);
        let last = tokio::time::timeout(Duration::from_secs(2), rx.recv())
            .await
            .unwrap()
            .unwrap();
        assert!(
            ids.contains(&last),
            "callbacks must reuse the configured workers"
        );
        assert_eq!(ids.len(), expected);
        pool.stop().await;
    }
}

#[tokio::test]
async fn fifo_aborted_stop_does_not_lose_drain() {
    let pool = TaskPool::new("drain", pool_environment(&[("drain", 1)])).unwrap();
    pool.start().unwrap();
    let (started_tx, started_rx) = tokio::sync::oneshot::channel();
    let (release_tx, release_rx) = tokio::sync::oneshot::channel();
    pool.add_task(
        MessageContext::new(),
        Box::pin(async move {
            let _ = started_tx.send(());
            let _ = release_rx.await;
        }),
    )
    .await
    .unwrap();
    started_rx.await.unwrap();
    let first_pool = Arc::clone(&pool);
    let first = tokio::spawn(async move { first_pool.stop().await });
    tokio::task::yield_now().await;
    first.abort();
    let _ = first.await;
    assert!(
        tokio::time::timeout(Duration::from_millis(10), pool.stop())
            .await
            .is_err()
    );
    let _ = release_tx.send(());
    tokio::time::timeout(Duration::from_secs(2), pool.stop())
        .await
        .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fifo_start_admission_and_stop_race_preserves_accepted_work() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    for _ in 0..50 {
        let pool = TaskPool::new("race", pool_environment(&[("race", 2)])).unwrap();
        let completed = Arc::new(AtomicUsize::new(0));
        let other = Arc::clone(&pool);
        let start = tokio::spawn(async move {
            let _ = other.start();
        });
        let other = Arc::clone(&pool);
        let stop = tokio::spawn(async move {
            other.stop().await;
        });
        let done = Arc::clone(&completed);
        let accepted = pool
            .add_task(
                MessageContext::new(),
                Box::pin(async move {
                    done.fetch_add(1, Ordering::Relaxed);
                }),
            )
            .await
            .is_ok();
        start.await.unwrap();
        tokio::time::timeout(Duration::from_secs(2), stop)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(completed.load(Ordering::Relaxed), usize::from(accepted));
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn priority_uses_configured_workers_and_zero_means_cpu_count() {
    for configured in [2, 0] {
        let expected = if configured == 0 {
            std::thread::available_parallelism().map_or(1, usize::from)
        } else {
            configured
        };
        let pool =
            PriorityTaskPool::new("workers", pool_environment(&[("workers", configured)])).unwrap();
        let release = Arc::new(tokio::sync::Semaphore::new(0));
        let (tx, mut rx) = mpsc::unbounded_channel();
        // Before start the work is accepted, but it must not execute yet.
        for _ in 0..expected + 1 {
            let tx = tx.clone();
            let release = Arc::clone(&release);
            pool.add_task(
                MessageContext::new(),
                0,
                Box::pin(async move {
                    tx.send(tokio::task::id()).unwrap();
                    release.acquire().await.unwrap().forget();
                }),
            )
            .await
            .unwrap();
        }
        assert!(rx.try_recv().is_err());
        pool.start().unwrap();
        let mut ids = std::collections::HashSet::new();
        for _ in 0..expected {
            ids.insert(
                tokio::time::timeout(Duration::from_secs(2), rx.recv())
                    .await
                    .unwrap()
                    .unwrap(),
            );
        }
        assert!(
            tokio::time::timeout(Duration::from_millis(10), rx.recv())
                .await
                .is_err()
        );
        release.add_permits(expected + 1);
        let last = tokio::time::timeout(Duration::from_secs(2), rx.recv())
            .await
            .unwrap()
            .unwrap();
        assert!(
            ids.contains(&last),
            "callbacks must reuse the configured workers"
        );
        assert_eq!(ids.len(), expected);
        pool.stop().await;
    }
}

#[tokio::test]
async fn priority_aborted_stop_does_not_lose_drain() {
    let pool = PriorityTaskPool::new("drain", pool_environment(&[("drain", 1)])).unwrap();
    pool.start().unwrap();
    let (started_tx, started_rx) = tokio::sync::oneshot::channel();
    let (release_tx, release_rx) = tokio::sync::oneshot::channel();
    pool.add_task(
        MessageContext::new(),
        0,
        Box::pin(async move {
            let _ = started_tx.send(());
            let _ = release_rx.await;
        }),
    )
    .await
    .unwrap();
    started_rx.await.unwrap();
    let first_pool = Arc::clone(&pool);
    let first = tokio::spawn(async move { first_pool.stop().await });
    tokio::task::yield_now().await;
    first.abort();
    let _ = first.await;
    assert!(
        tokio::time::timeout(Duration::from_millis(10), pool.stop())
            .await
            .is_err()
    );
    let _ = release_tx.send(());
    tokio::time::timeout(Duration::from_secs(2), pool.stop())
        .await
        .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn priority_start_admission_and_stop_race_preserves_accepted_work() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    for _ in 0..50 {
        let pool = PriorityTaskPool::new("race", pool_environment(&[("race", 2)])).unwrap();
        let completed = Arc::new(AtomicUsize::new(0));
        let other = Arc::clone(&pool);
        let start = tokio::spawn(async move {
            let _ = other.start();
        });
        let other = Arc::clone(&pool);
        let stop = tokio::spawn(async move {
            other.stop().await;
        });
        let done = Arc::clone(&completed);
        let accepted = pool
            .add_task(
                MessageContext::new(),
                0,
                Box::pin(async move {
                    done.fetch_add(1, Ordering::Relaxed);
                }),
            )
            .await
            .is_ok();
        start.await.unwrap();
        tokio::time::timeout(Duration::from_secs(2), stop)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(completed.load(Ordering::Relaxed), usize::from(accepted));
    }
}

#[tokio::test]
async fn fifo_downsize_waits_for_busy_workers_without_starting_extra_callbacks() {
    let environment = pool_environment(&[("shrink", 2)]);
    let pool = TaskPool::new("shrink", environment.clone()).unwrap();
    environment.register_task_pool(pool.clone()).unwrap();
    pool.start().unwrap();
    let (started, mut starts) = mpsc::unbounded_channel();
    let mut releases = Vec::new();
    for id in 0..3 {
        let started = started.clone();
        let (release, wait) = tokio::sync::oneshot::channel();
        releases.push(release);
        pool.add_task(
            MessageContext::new(),
            Box::pin(async move {
                started.send(id).unwrap();
                let _ = wait.await;
            }),
        )
        .await
        .unwrap();
    }
    let first = starts.recv().await.unwrap();
    let second = starts.recv().await.unwrap();
    assert_ne!(first, second);
    let config = PoolTestConfig {
        pools: vec![PoolConfig {
            name: "shrink".to_owned(),
            executors_count: 1,
            queue_capacity: 0,
        }],
    };
    environment.publish_runtime_config(Arc::new(RuntimeConfig::new(&config).unwrap()));
    let _ = releases.remove(0).send(());
    let extra_started = tokio::time::timeout(Duration::from_millis(10), starts.recv())
        .await
        .is_ok();
    for release in releases {
        let _ = release.send(());
    }
    pool.stop().await;
    assert!(!extra_started);
}

#[tokio::test]
async fn priority_downsize_waits_for_busy_workers_without_starting_extra_callbacks() {
    let environment = pool_environment(&[("shrink", 2)]);
    let pool = PriorityTaskPool::new("shrink", environment.clone()).unwrap();
    environment
        .register_priority_task_pool(pool.clone())
        .unwrap();
    pool.start().unwrap();
    let (started, mut starts) = mpsc::unbounded_channel();
    let mut releases = Vec::new();
    for id in 0..3 {
        let started = started.clone();
        let (release, wait) = tokio::sync::oneshot::channel();
        releases.push(release);
        pool.add_task(
            MessageContext::new(),
            0,
            Box::pin(async move {
                started.send(id).unwrap();
                let _ = wait.await;
            }),
        )
        .await
        .unwrap();
    }
    let first = starts.recv().await.unwrap();
    let second = starts.recv().await.unwrap();
    assert_ne!(first, second);
    let config = PoolTestConfig {
        pools: vec![PoolConfig {
            name: "shrink".to_owned(),
            executors_count: 1,
            queue_capacity: 0,
        }],
    };
    environment.publish_runtime_config(Arc::new(RuntimeConfig::new(&config).unwrap()));
    let _ = releases.remove(0).send(());
    let extra_started = tokio::time::timeout(Duration::from_millis(10), starts.recv())
        .await
        .is_ok();
    for release in releases {
        let _ = release.send(());
    }
    pool.stop().await;
    assert!(!extra_started);
}
