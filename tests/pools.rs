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
    pool.add_task(
        MessageContext::new(),
        Box::pin(async {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }),
    )
    .await
    .unwrap();

    let context = MessageContext::new();
    context.cancel();
    pool.stop_with_context(context).await;

    assert!(environment.metrics().render_prometheus().contains(
        r#"task_pool_events_total{event="stop_timeout",name="slow",service="orders"} 1"#
    ));
}
