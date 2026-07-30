use std::{
    collections::{BTreeMap, BTreeSet},
    fmt::Write,
    sync::{
        Arc, Mutex, RwLock,
        atomic::{AtomicI64, Ordering},
    },
};

use async_trait::async_trait;
use opentelemetry::{
    KeyValue,
    metrics::{Counter, Histogram, Meter, ObservableGauge, UpDownCounter},
};
use thiserror::Error;

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
}

#[derive(Clone)]
pub struct MetricsScope {
    metrics: Metrics,
    namespace: String,
    labels: Labels,
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
}

impl Int64Counter {
    pub fn inc(&self) {
        self.add(1);
    }

    pub fn add(&self, value: i64) {
        debug_assert!(value >= 0, "counter cannot decrease");
        self.value.fetch_add(value, Ordering::Relaxed);
        if let Some(counter) = &self.otel {
            counter.add(value as u64, &self.attributes);
        }
    }

    pub fn get(&self) -> i64 {
        self.value.load(Ordering::Relaxed)
    }
}

#[derive(Clone)]
pub struct Int64Gauge {
    value: Arc<AtomicI64>,
    otel: Option<UpDownCounter<i64>>,
    attributes: Arc<Vec<KeyValue>>,
}

impl Int64Gauge {
    pub fn inc(&self) {
        self.add(1);
    }

    pub fn dec(&self) {
        self.add(-1);
    }

    pub fn add(&self, value: i64) {
        self.value.fetch_add(value, Ordering::Relaxed);
        if let Some(gauge) = &self.otel {
            gauge.add(value, &self.attributes);
        }
    }

    pub fn set(&self, value: i64) {
        let previous = self.value.swap(value, Ordering::Relaxed);
        if let Some(gauge) = &self.otel {
            gauge.add(value - previous, &self.attributes);
        }
    }

    pub fn get(&self) -> i64 {
        self.value.load(Ordering::Relaxed)
    }
}

#[derive(Clone)]
pub struct Float64Histogram {
    value: Arc<Mutex<HistogramValue>>,
    otel: Option<Histogram<f64>>,
    attributes: Arc<Vec<KeyValue>>,
}

impl Float64Histogram {
    pub fn observe(&self, value: f64) {
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
        self.value.lock().expect("histogram lock poisoned").count
    }

    pub fn sum(&self) -> f64 {
        self.value.lock().expect("histogram lock poisoned").sum
    }
}

impl Metrics {
    pub fn with_meter(meter: Meter) -> Self {
        Self {
            registry: Arc::default(),
            meter: Some(meter),
        }
    }

    pub fn scope(&self, namespace: impl Into<String>, labels: Labels) -> MetricsScope {
        MetricsScope {
            metrics: self.clone(),
            namespace: namespace.into(),
            labels,
        }
    }

    pub fn render_prometheus(&self) -> String {
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
