use std::{
    collections::{BTreeMap, BTreeSet},
    fmt::Write,
    sync::{
        Arc, Mutex, RwLock,
        atomic::{AtomicI64, AtomicU64, Ordering},
    },
    time::Duration,
};

use async_trait::async_trait;
use opentelemetry::{
    KeyValue,
    metrics::{Counter, Histogram, Meter, ObservableGauge, UpDownCounter},
};
use thiserror::Error;
use tokio::{sync::Mutex as AsyncMutex, task::JoinHandle};
use tokio_metrics::RuntimeMonitor;
use tokio_util::sync::CancellationToken;

use crate::runtime::environment::RuntimeResult;

pub type Labels = BTreeMap<String, String>;

#[async_trait]
pub trait MetricsEngine: Send + Sync {
    fn metrics(&self) -> &Metrics;

    async fn shutdown(&self) -> RuntimeResult<()>;
}

/// Default production engine: an in-process Prometheus registry exposed by
/// `ServiceApp` through the configured metrics handler.
#[derive(Clone, Default)]
pub struct PrometheusMetricsEngine {
    metrics: Metrics,
}

/// Metrics engine for benchmarks and deployments that need the complete
/// instrumentation call surface without retaining or exporting observations.
#[derive(Clone)]
pub struct NoopMetricsEngine {
    metrics: Metrics,
}

impl Default for NoopMetricsEngine {
    fn default() -> Self {
        Self::new()
    }
}

impl NoopMetricsEngine {
    pub fn new() -> Self {
        Self {
            metrics: Metrics::noop(),
        }
    }
}

#[async_trait]
impl MetricsEngine for NoopMetricsEngine {
    fn metrics(&self) -> &Metrics {
        &self.metrics
    }
    async fn shutdown(&self) -> RuntimeResult<()> {
        Ok(())
    }
}

impl PrometheusMetricsEngine {
    pub fn new() -> Self {
        Self::default()
    }
}

#[async_trait]
impl MetricsEngine for PrometheusMetricsEngine {
    fn metrics(&self) -> &Metrics {
        &self.metrics
    }

    async fn shutdown(&self) -> RuntimeResult<()> {
        Ok(())
    }
}

#[derive(Debug, Error)]
pub enum MetricsError {
    #[error("metric {0:?} is already registered with another type")]
    TypeConflict(String),
    #[error("metric {0:?} is already registered with another description")]
    DescriptionConflict(String),
}

#[derive(Clone, Default)]
pub struct Metrics {
    registry: Arc<RwLock<BTreeMap<MetricKey, Metric>>>,
    meter: Option<Meter>,
    noop: bool,
}

#[derive(Clone)]
pub struct MetricsScope {
    metrics: Metrics,
    namespace: String,
    labels: Labels,
}

/// Lifecycle-owned sampler for the stable metrics exposed by the official
/// `tokio-metrics` RuntimeMonitor. Sampling happens once per second and never
/// touches the message hot path. Metrics behind Tokio's `tokio_unstable` cfg
/// are intentionally excluded from the public telemetry contract.
#[derive(Default)]
pub struct TokioRuntimeMetrics {
    snapshot: Arc<TokioRuntimeSnapshot>,
    registered: Mutex<bool>,
    task: AsyncMutex<Option<(CancellationToken, JoinHandle<()>)>>,
}

#[derive(Default)]
struct TokioRuntimeSnapshot {
    workers_count: AtomicU64,
    live_tasks_count: AtomicU64,
    total_park_count: AtomicU64,
    max_park_count: AtomicU64,
    min_park_count: AtomicU64,
    total_busy_duration_seconds: AtomicU64,
    max_busy_duration_seconds: AtomicU64,
    min_busy_duration_seconds: AtomicU64,
    global_queue_depth: AtomicU64,
    busy_ratio: AtomicU64,
}

impl TokioRuntimeSnapshot {
    fn update(&self, value: &tokio_metrics::RuntimeMetrics) {
        self.workers_count
            .store(value.workers_count as u64, Ordering::Relaxed);
        self.live_tasks_count
            .store(value.live_tasks_count as u64, Ordering::Relaxed);
        self.total_park_count
            .store(value.total_park_count, Ordering::Relaxed);
        self.max_park_count
            .store(value.max_park_count, Ordering::Relaxed);
        self.min_park_count
            .store(value.min_park_count, Ordering::Relaxed);
        self.total_busy_duration_seconds.store(
            value.total_busy_duration.as_secs_f64().to_bits(),
            Ordering::Relaxed,
        );
        self.max_busy_duration_seconds.store(
            value.max_busy_duration.as_secs_f64().to_bits(),
            Ordering::Relaxed,
        );
        self.min_busy_duration_seconds.store(
            value.min_busy_duration.as_secs_f64().to_bits(),
            Ordering::Relaxed,
        );
        self.global_queue_depth
            .store(value.global_queue_depth as u64, Ordering::Relaxed);
        self.busy_ratio
            .store(value.busy_ratio().to_bits(), Ordering::Relaxed);
    }
}

impl TokioRuntimeMetrics {
    pub async fn start(&self, metrics: &Metrics) -> RuntimeResult<()> {
        if metrics.is_noop() {
            return Ok(());
        }
        let mut task = self.task.lock().await;
        if task.is_some() {
            return Ok(());
        }
        {
            let mut registered = self
                .registered
                .lock()
                .expect("Tokio runtime metric registration lock poisoned");
            if !*registered {
                register_tokio_runtime_metrics(metrics, &self.snapshot)?;
                *registered = true;
            }
        }

        let cancellation = CancellationToken::new();
        let stop = cancellation.clone();
        let snapshot = Arc::clone(&self.snapshot);
        let handle = tokio::runtime::Handle::current();
        let monitor = RuntimeMonitor::new(&handle);
        let join = tokio::spawn(async move {
            let mut intervals = monitor.intervals();
            if let Some(value) = intervals.next() {
                snapshot.update(&value);
            }
            let mut ticker = tokio::time::interval(Duration::from_secs(1));
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            ticker.tick().await;
            loop {
                tokio::select! {
                    () = stop.cancelled() => break,
                    _ = ticker.tick() => {
                        if let Some(value) = intervals.next() {
                            snapshot.update(&value);
                        }
                    }
                }
            }
        });
        *task = Some((cancellation, join));
        Ok(())
    }

    pub async fn stop(&self) -> RuntimeResult<()> {
        let Some((cancellation, task)) = self.task.lock().await.take() else {
            return Ok(());
        };
        cancellation.cancel();
        task.await.map_err(|error| {
            crate::runtime::environment::RuntimeError::Transport(error.to_string())
        })
    }
}

fn register_tokio_runtime_metrics(
    metrics: &Metrics,
    snapshot: &Arc<TokioRuntimeSnapshot>,
) -> Result<(), MetricsError> {
    let scope = metrics.scope("", Labels::new());
    let gauges: [(&str, &str, fn(&TokioRuntimeSnapshot) -> f64); 10] = [
        (
            "tokio_workers_count",
            "Number of Tokio runtime worker threads",
            |s| s.workers_count.load(Ordering::Relaxed) as f64,
        ),
        (
            "tokio_live_tasks_count",
            "Number of live Tokio tasks",
            |s| s.live_tasks_count.load(Ordering::Relaxed) as f64,
        ),
        (
            "tokio_total_park_count",
            "Worker parks observed in the latest Tokio sampling interval",
            |s| s.total_park_count.load(Ordering::Relaxed) as f64,
        ),
        (
            "tokio_max_park_count",
            "Maximum worker parks in the latest Tokio sampling interval",
            |s| s.max_park_count.load(Ordering::Relaxed) as f64,
        ),
        (
            "tokio_min_park_count",
            "Minimum worker parks in the latest Tokio sampling interval",
            |s| s.min_park_count.load(Ordering::Relaxed) as f64,
        ),
        (
            "tokio_total_busy_duration_seconds",
            "Total worker busy duration in the latest Tokio sampling interval",
            |s| f64::from_bits(s.total_busy_duration_seconds.load(Ordering::Relaxed)),
        ),
        (
            "tokio_max_busy_duration_seconds",
            "Maximum worker busy duration in the latest Tokio sampling interval",
            |s| f64::from_bits(s.max_busy_duration_seconds.load(Ordering::Relaxed)),
        ),
        (
            "tokio_min_busy_duration_seconds",
            "Minimum worker busy duration in the latest Tokio sampling interval",
            |s| f64::from_bits(s.min_busy_duration_seconds.load(Ordering::Relaxed)),
        ),
        (
            "tokio_global_queue_depth",
            "Tasks currently scheduled in the Tokio global queue",
            |s| s.global_queue_depth.load(Ordering::Relaxed) as f64,
        ),
        (
            "tokio_busy_ratio",
            "Ratio of worker busy duration to elapsed sampling time",
            |s| f64::from_bits(s.busy_ratio.load(Ordering::Relaxed)),
        ),
    ];
    for (name, help, observe) in gauges {
        let snapshot = Arc::clone(snapshot);
        scope.observable_float64_gauge(
            name,
            help,
            Labels::new(),
            Arc::new(move || observe(&snapshot)),
        )?;
    }
    Ok(())
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct MetricKey {
    name: String,
    labels: Labels,
}

#[derive(Clone)]
enum Metric {
    Counter {
        help: String,
        value: Arc<AtomicI64>,
        otel: Option<Counter<u64>>,
        attributes: Arc<Vec<KeyValue>>,
    },
    Gauge {
        help: String,
        value: Arc<AtomicI64>,
        otel: Option<UpDownCounter<i64>>,
        attributes: Arc<Vec<KeyValue>>,
    },
    Histogram {
        help: String,
        value: Arc<Mutex<HistogramValue>>,
        otel: Option<Histogram<f64>>,
        attributes: Arc<Vec<KeyValue>>,
    },
    ObservableGauge {
        help: String,
        observe: Arc<dyn Fn() -> f64 + Send + Sync>,
        _otel: Option<ObservableGauge<f64>>,
    },
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum MetricKind {
    Counter,
    Gauge,
    Histogram,
    ObservableGauge,
}

struct HistogramValue {
    bounds: Vec<f64>,
    buckets: Vec<u64>,
    count: u64,
    sum: f64,
}

#[derive(Clone)]
pub struct Int64Counter {
    value: Arc<AtomicI64>,
    otel: Option<Counter<u64>>,
    attributes: Arc<Vec<KeyValue>>,
    enabled: bool,
}

impl Int64Counter {
    pub fn inc(&self) {
        self.add(1);
    }

    pub fn add(&self, value: i64) {
        if !self.enabled {
            return;
        }
        debug_assert!(value >= 0, "counter cannot decrease");
        if let Some(counter) = &self.otel {
            counter.add(value as u64, &self.attributes);
        } else {
            self.value.fetch_add(value, Ordering::Relaxed);
        }
    }

    pub fn get(&self) -> i64 {
        if !self.enabled {
            return 0;
        }
        self.value.load(Ordering::Relaxed)
    }
}

#[derive(Clone)]
pub struct Int64Gauge {
    value: Arc<AtomicI64>,
    otel: Option<UpDownCounter<i64>>,
    attributes: Arc<Vec<KeyValue>>,
    enabled: bool,
}

impl Int64Gauge {
    pub fn is_enabled(&self) -> bool {
        self.enabled
    }

    pub fn inc(&self) {
        self.add(1);
    }

    pub fn dec(&self) {
        self.add(-1);
    }

    // Unlike Counter/Histogram, add() cannot skip the local atomic when OTel is
    // present: set() below computes its OTel delta from `self.value`, and some
    // callers mix add()/inc()/dec() with set() on the same gauge (e.g.
    // taskpool's executors_allocated). Skipping the write here would desync
    // `self.value` from the gauge's real value and corrupt the next set()'s
    // delta. So this one metric type keeps writing both unconditionally.
    pub fn add(&self, value: i64) {
        if !self.enabled {
            return;
        }
        self.value.fetch_add(value, Ordering::Relaxed);
        if let Some(gauge) = &self.otel {
            gauge.add(value, &self.attributes);
        }
    }

    pub fn set(&self, value: i64) {
        if !self.enabled {
            return;
        }
        let previous = self.value.swap(value, Ordering::Relaxed);
        if let Some(gauge) = &self.otel {
            gauge.add(value - previous, &self.attributes);
        }
    }

    pub fn get(&self) -> i64 {
        if !self.enabled {
            return 0;
        }
        self.value.load(Ordering::Relaxed)
    }
}

#[derive(Clone)]
pub struct Float64Histogram {
    value: Arc<Mutex<HistogramValue>>,
    otel: Option<Histogram<f64>>,
    attributes: Arc<Vec<KeyValue>>,
    enabled: bool,
}

impl Float64Histogram {
    pub fn is_enabled(&self) -> bool {
        self.enabled
    }

    pub fn observe(&self, value: f64) {
        if !self.enabled {
            return;
        }
        let mut histogram = self.value.lock().expect("histogram lock poisoned");
        histogram.count += 1;
        histogram.sum += value;
        let index = histogram.bounds.partition_point(|bound| *bound < value);
        if let Some(bucket) = histogram.buckets.get_mut(index) {
            *bucket += 1;
        }
        if let Some(otel) = &self.otel {
            otel.record(value, &self.attributes);
        }
    }

    pub fn count(&self) -> u64 {
        if !self.enabled {
            return 0;
        }
        self.value.lock().expect("histogram lock poisoned").count
    }

    pub fn sum(&self) -> f64 {
        if !self.enabled {
            return 0.0;
        }
        self.value.lock().expect("histogram lock poisoned").sum
    }
}

impl Metrics {
    pub fn noop() -> Self {
        Self {
            noop: true,
            ..Self::default()
        }
    }

    pub fn with_meter(meter: Meter) -> Self {
        Self {
            registry: Arc::default(),
            meter: Some(meter),
            noop: false,
        }
    }

    /// True when metrics are being pushed through an OTel meter. Counter/Gauge/
    /// Histogram recording then skips the local registry entirely (see their
    /// `add`/`observe` methods), so `render_prometheus()` would only report
    /// stale zeros in that mode -- callers should not expose it as a scrape
    /// endpoint when this returns true.
    pub fn has_otel(&self) -> bool {
        self.meter.is_some()
    }

    /// True when every metric operation is configured as a no-op. Runtime
    /// middleware uses this to bypass measurement work itself (route lookup,
    /// clocks and body observers), not merely the final instrument update.
    pub fn is_noop(&self) -> bool {
        self.noop
    }

    pub fn scope(&self, namespace: impl Into<String>, labels: Labels) -> MetricsScope {
        MetricsScope {
            metrics: self.clone(),
            namespace: namespace.into(),
            labels,
        }
    }

    pub fn render_prometheus(&self) -> String {
        if self.noop {
            return String::new();
        }
        let registry = self
            .registry
            .read()
            .expect("metrics registry lock poisoned");
        let mut output = String::new();
        let mut rendered_families = BTreeSet::new();
        for (key, metric) in registry.iter() {
            let render_header = rendered_families.insert(key.name.clone());
            match metric {
                Metric::Counter { help, value, .. } => {
                    if render_header {
                        write_header(&mut output, &key.name, help, "counter");
                    }
                    write_sample(
                        &mut output,
                        &key.name,
                        &key.labels,
                        &value.load(Ordering::Relaxed).to_string(),
                    );
                }
                Metric::Gauge { help, value, .. } => {
                    if render_header {
                        write_header(&mut output, &key.name, help, "gauge");
                    }
                    write_sample(
                        &mut output,
                        &key.name,
                        &key.labels,
                        &value.load(Ordering::Relaxed).to_string(),
                    );
                }
                Metric::Histogram { help, value, .. } => {
                    if render_header {
                        write_header(&mut output, &key.name, help, "histogram");
                    }
                    let histogram = value.lock().expect("histogram lock poisoned");
                    let mut cumulative = 0_u64;
                    for (index, count) in histogram.buckets.iter().enumerate() {
                        cumulative += count;
                        let mut labels = key.labels.clone();
                        let bound = histogram
                            .bounds
                            .get(index)
                            .map_or_else(|| "+Inf".to_owned(), ToString::to_string);
                        labels.insert("le".to_owned(), bound);
                        write_sample(
                            &mut output,
                            &format!("{}_bucket", key.name),
                            &labels,
                            &cumulative.to_string(),
                        );
                    }
                    write_sample(
                        &mut output,
                        &format!("{}_sum", key.name),
                        &key.labels,
                        &histogram.sum.to_string(),
                    );
                    write_sample(
                        &mut output,
                        &format!("{}_count", key.name),
                        &key.labels,
                        &histogram.count.to_string(),
                    );
                }
                Metric::ObservableGauge { help, observe, .. } => {
                    if render_header {
                        write_header(&mut output, &key.name, help, "gauge");
                    }
                    write_sample(&mut output, &key.name, &key.labels, &observe().to_string());
                }
            }
        }
        output
    }
}

impl MetricsScope {
    pub fn scope(&self, namespace: impl AsRef<str>, labels: Labels) -> Self {
        let namespace = join_name(&self.namespace, namespace.as_ref());
        let mut merged_labels = self.labels.clone();
        merged_labels.extend(labels);
        Self {
            metrics: self.metrics.clone(),
            namespace,
            labels: merged_labels,
        }
    }

    pub fn counter(
        &self,
        name: &str,
        help: &str,
        labels: Labels,
    ) -> Result<Int64Counter, MetricsError> {
        if self.metrics.noop {
            return Ok(Int64Counter {
                value: Arc::new(AtomicI64::new(0)),
                otel: None,
                attributes: Arc::new(Vec::new()),
                enabled: false,
            });
        }
        let key = self.key(name, labels);
        let mut registry = self
            .metrics
            .registry
            .write()
            .expect("metrics registry lock poisoned");
        validate_metric_family(&registry, &key.name, MetricKind::Counter, help)?;
        match registry.get(&key) {
            Some(Metric::Counter {
                value,
                otel,
                attributes,
                ..
            }) => Ok(Int64Counter {
                value: Arc::clone(value),
                otel: otel.clone(),
                attributes: Arc::clone(attributes),
                enabled: true,
            }),
            Some(_) => Err(MetricsError::TypeConflict(key.name)),
            None => {
                let value = Arc::new(AtomicI64::new(0));
                let attributes = Arc::new(metric_attributes(&key.labels));
                let otel = self.metrics.meter.as_ref().map(|meter| {
                    meter
                        .u64_counter(key.name.clone())
                        .with_description(help.to_owned())
                        .build()
                });
                registry.insert(
                    key,
                    Metric::Counter {
                        help: help.to_owned(),
                        value: Arc::clone(&value),
                        otel: otel.clone(),
                        attributes: Arc::clone(&attributes),
                    },
                );
                Ok(Int64Counter {
                    value,
                    otel,
                    attributes,
                    enabled: true,
                })
            }
        }
    }

    pub fn gauge(
        &self,
        name: &str,
        help: &str,
        labels: Labels,
    ) -> Result<Int64Gauge, MetricsError> {
        if self.metrics.noop {
            return Ok(Int64Gauge {
                value: Arc::new(AtomicI64::new(0)),
                otel: None,
                attributes: Arc::new(Vec::new()),
                enabled: false,
            });
        }
        let key = self.key(name, labels);
        let mut registry = self
            .metrics
            .registry
            .write()
            .expect("metrics registry lock poisoned");
        validate_metric_family(&registry, &key.name, MetricKind::Gauge, help)?;
        match registry.get(&key) {
            Some(Metric::Gauge {
                value,
                otel,
                attributes,
                ..
            }) => Ok(Int64Gauge {
                value: Arc::clone(value),
                otel: otel.clone(),
                attributes: Arc::clone(attributes),
                enabled: true,
            }),
            Some(_) => Err(MetricsError::TypeConflict(key.name)),
            None => {
                let value = Arc::new(AtomicI64::new(0));
                let attributes = Arc::new(metric_attributes(&key.labels));
                let otel = self.metrics.meter.as_ref().map(|meter| {
                    meter
                        .i64_up_down_counter(key.name.clone())
                        .with_description(help.to_owned())
                        .build()
                });
                registry.insert(
                    key,
                    Metric::Gauge {
                        help: help.to_owned(),
                        value: Arc::clone(&value),
                        otel: otel.clone(),
                        attributes: Arc::clone(&attributes),
                    },
                );
                Ok(Int64Gauge {
                    value,
                    otel,
                    attributes,
                    enabled: true,
                })
            }
        }
    }

    pub fn histogram(
        &self,
        name: &str,
        help: &str,
        labels: Labels,
        bounds: Option<Vec<f64>>,
    ) -> Result<Float64Histogram, MetricsError> {
        if self.metrics.noop {
            return Ok(Float64Histogram {
                value: Arc::new(Mutex::new(HistogramValue {
                    bounds: Vec::new(),
                    buckets: Vec::new(),
                    count: 0,
                    sum: 0.0,
                })),
                otel: None,
                attributes: Arc::new(Vec::new()),
                enabled: false,
            });
        }
        let key = self.key(name, labels);
        let mut registry = self
            .metrics
            .registry
            .write()
            .expect("metrics registry lock poisoned");
        validate_metric_family(&registry, &key.name, MetricKind::Histogram, help)?;
        match registry.get(&key) {
            Some(Metric::Histogram {
                value,
                otel,
                attributes,
                ..
            }) => Ok(Float64Histogram {
                value: Arc::clone(value),
                otel: otel.clone(),
                attributes: Arc::clone(attributes),
                enabled: true,
            }),
            Some(_) => Err(MetricsError::TypeConflict(key.name)),
            None => {
                let bounds = bounds.unwrap_or_else(default_duration_bounds);
                let bucket_count = bounds.len() + 1;
                let otel = self.metrics.meter.as_ref().map(|meter| {
                    meter
                        .f64_histogram(key.name.clone())
                        .with_description(help.to_owned())
                        .with_boundaries(bounds.clone())
                        .build()
                });
                let value = Arc::new(Mutex::new(HistogramValue {
                    bounds,
                    buckets: vec![0; bucket_count],
                    count: 0,
                    sum: 0.0,
                }));
                let attributes = Arc::new(metric_attributes(&key.labels));
                registry.insert(
                    key,
                    Metric::Histogram {
                        help: help.to_owned(),
                        value: Arc::clone(&value),
                        otel: otel.clone(),
                        attributes: Arc::clone(&attributes),
                    },
                );
                Ok(Float64Histogram {
                    value,
                    otel,
                    attributes,
                    enabled: true,
                })
            }
        }
    }

    pub fn observable_float64_gauge(
        &self,
        name: &str,
        help: &str,
        labels: Labels,
        observe: Arc<dyn Fn() -> f64 + Send + Sync>,
    ) -> Result<(), MetricsError> {
        if self.metrics.noop {
            return Ok(());
        }
        let key = self.key(name, labels);
        let mut registry = self
            .metrics
            .registry
            .write()
            .expect("metrics registry lock poisoned");
        validate_metric_family(&registry, &key.name, MetricKind::ObservableGauge, help)?;
        match registry.get(&key) {
            Some(Metric::ObservableGauge { .. }) => Ok(()),
            Some(_) => Err(MetricsError::TypeConflict(key.name)),
            None => {
                let attributes = metric_attributes(&key.labels);
                let otel = self.metrics.meter.as_ref().map(|meter| {
                    let observe = Arc::clone(&observe);
                    meter
                        .f64_observable_gauge(key.name.clone())
                        .with_description(help.to_owned())
                        .with_callback(move |observer| {
                            observer.observe(observe(), &attributes);
                        })
                        .build()
                });
                registry.insert(
                    key,
                    Metric::ObservableGauge {
                        help: help.to_owned(),
                        observe,
                        _otel: otel,
                    },
                );
                Ok(())
            }
        }
    }

    fn key(&self, name: &str, labels: Labels) -> MetricKey {
        let mut merged_labels = self.labels.clone();
        merged_labels.extend(labels);
        MetricKey {
            name: join_name(&self.namespace, name),
            labels: merged_labels,
        }
    }
}

fn validate_metric_family(
    registry: &BTreeMap<MetricKey, Metric>,
    name: &str,
    expected_kind: MetricKind,
    expected_help: &str,
) -> Result<(), MetricsError> {
    for (key, metric) in registry {
        if key.name != name {
            continue;
        }
        let (kind, help) = match metric {
            Metric::Counter { help, .. } => (MetricKind::Counter, help),
            Metric::Gauge { help, .. } => (MetricKind::Gauge, help),
            Metric::Histogram { help, .. } => (MetricKind::Histogram, help),
            Metric::ObservableGauge { help, .. } => (MetricKind::ObservableGauge, help),
        };
        if kind != expected_kind {
            return Err(MetricsError::TypeConflict(name.to_owned()));
        }
        if help != expected_help {
            return Err(MetricsError::DescriptionConflict(name.to_owned()));
        }
        break;
    }
    Ok(())
}

fn metric_attributes(labels: &Labels) -> Vec<KeyValue> {
    labels
        .iter()
        .map(|(key, value)| KeyValue::new(key.clone(), value.clone()))
        .collect()
}

fn default_duration_bounds() -> Vec<f64> {
    vec![
        0.000_01, 0.000_05, 0.000_1, 0.000_5, 0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0, 10.0,
    ]
}

fn join_name(namespace: &str, name: &str) -> String {
    if namespace.is_empty() {
        name.to_owned()
    } else if name.is_empty() {
        namespace.to_owned()
    } else {
        format!("{namespace}_{name}")
    }
}

fn write_header(output: &mut String, name: &str, help: &str, metric_type: &str) {
    let help = help.replace('\\', r"\\").replace('\n', r"\n");
    let _ = writeln!(output, "# HELP {name} {help}");
    let _ = writeln!(output, "# TYPE {name} {metric_type}");
}

fn write_sample(output: &mut String, name: &str, labels: &Labels, value: &str) {
    let _ = write!(output, "{name}");
    if !labels.is_empty() {
        output.push('{');
        for (index, (name, value)) in labels.iter().enumerate() {
            if index != 0 {
                output.push(',');
            }
            let value = value
                .replace('\\', r"\\")
                .replace('"', r#"\""#)
                .replace('\n', r"\n");
            let _ = write!(output, r#"{name}="{value}""#);
        }
        output.push('}');
    }
    let _ = writeln!(output, " {value}");
}
