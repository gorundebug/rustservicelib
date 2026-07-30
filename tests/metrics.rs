use std::{
    collections::BTreeMap,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

use servicelib::runtime::environment::metrics::{Labels, Metrics, MetricsError};

fn labels(values: &[(&str, &str)]) -> Labels {
    values
        .iter()
        .map(|(name, value)| ((*name).to_owned(), (*value).to_owned()))
        .collect::<BTreeMap<_, _>>()
}

#[test]
fn observable_gauge_is_evaluated_on_every_collection() {
    let metrics = Metrics::default();
    let value = Arc::new(AtomicU64::new(5));
    let observed = Arc::clone(&value);
    metrics
        .scope("datasource_endpoint", Labels::new())
        .observable_float64_gauge(
            "pending_oldest_age_seconds",
            "Age of oldest pending request",
            Labels::new(),
            Arc::new(move || observed.load(Ordering::Relaxed) as f64),
        )
        .unwrap();

    assert!(
        metrics
            .render_prometheus()
            .contains("datasource_endpoint_pending_oldest_age_seconds 5")
    );
    value.store(9, Ordering::Relaxed);
    assert!(
        metrics
            .render_prometheus()
            .contains("datasource_endpoint_pending_oldest_age_seconds 9")
    );
}

#[test]
fn metrics_scopes_share_handles_and_render_prometheus() {
    let metrics = Metrics::default();
    let scope = metrics.scope("task_pool", labels(&[("service", "orders")]));
    let counter = scope
        .counter(
            "tasks_total",
            "Total number of tasks executed by task pool",
            labels(&[("name", "default")]),
        )
        .unwrap();
    let same_counter = scope
        .counter(
            "tasks_total",
            "Total number of tasks executed by task pool",
            labels(&[("name", "default")]),
        )
        .unwrap();
    counter.inc();
    same_counter.add(2);

    let gauge = scope
        .gauge("executors_busy", "Busy executors", Labels::new())
        .unwrap();
    gauge.set(4);
    gauge.dec();

    let histogram = scope
        .histogram(
            "task_execution_duration_seconds",
            "Task execution duration",
            Labels::new(),
            Some(vec![0.1, 1.0]),
        )
        .unwrap();
    histogram.observe(0.5);
    histogram.observe(2.0);

    let rendered = metrics.render_prometheus();
    assert!(rendered.contains(r#"task_pool_tasks_total{name="default",service="orders"} 3"#));
    assert!(rendered.contains(r#"task_pool_executors_busy{service="orders"} 3"#));
    assert!(rendered.contains(
        r#"task_pool_task_execution_duration_seconds_bucket{le="1",service="orders"} 1"#
    ));
    assert!(
        rendered.contains(r#"task_pool_task_execution_duration_seconds_count{service="orders"} 2"#)
    );
}

#[test]
fn prometheus_metric_family_header_is_rendered_once_for_all_label_series() {
    let metrics = Metrics::default();
    let scope = metrics.scope("task_pool", labels(&[("name", "default")]));
    scope
        .counter(
            "events_total",
            "Total number of events in task pool",
            labels(&[("event", "task_rejected")]),
        )
        .unwrap()
        .inc();
    scope
        .counter(
            "events_total",
            "Total number of events in task pool",
            labels(&[("event", "stop_timeout")]),
        )
        .unwrap()
        .inc();

    let rendered = metrics.render_prometheus();
    assert_eq!(
        rendered
            .lines()
            .filter(
                |line| line == &"# HELP task_pool_events_total Total number of events in task pool"
            )
            .count(),
        1
    );
    assert_eq!(
        rendered
            .lines()
            .filter(|line| line == &"# TYPE task_pool_events_total counter")
            .count(),
        1
    );
    assert!(rendered.contains(r#"event="task_rejected""#));
    assert!(rendered.contains(r#"event="stop_timeout""#));
}

#[test]
fn metric_family_rejects_conflicting_type_or_description_across_labels() {
    let metrics = Metrics::default();
    let scope = metrics.scope("worker", Labels::new());
    scope
        .counter(
            "events_total",
            "Worker events",
            labels(&[("event", "accepted")]),
        )
        .unwrap();

    assert!(matches!(
        scope.gauge(
            "events_total",
            "Worker events",
            labels(&[("event", "active")])
        ),
        Err(MetricsError::TypeConflict(_))
    ));
    assert!(matches!(
        scope.counter(
            "events_total",
            "A conflicting description",
            labels(&[("event", "rejected")])
        ),
        Err(MetricsError::DescriptionConflict(_))
    ));
}
