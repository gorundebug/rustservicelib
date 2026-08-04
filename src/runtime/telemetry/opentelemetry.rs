use std::{sync::Arc, time::Duration};

use async_trait::async_trait;
use opentelemetry::{
    global, metrics::MeterProvider as _, propagation::TextMapCompositePropagator,
    trace::TracerProvider as _,
};
use opentelemetry_appender_tracing::layer::OpenTelemetryTracingBridge;
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::{
    Resource,
    logs::SdkLoggerProvider,
    metrics::SdkMeterProvider,
    propagation::{BaggagePropagator, TraceContextPropagator},
    trace::SdkTracerProvider,
};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

use crate::runtime::environment::{
    RuntimeError, RuntimeResult,
    log::LogsEngine,
    metrics::{Metrics, MetricsEngine},
    tracing::TracingEngine,
};

#[derive(Clone, Debug)]
pub struct Config {
    pub service_name: String,
    pub endpoint: String,
    pub timeout: Duration,
}

#[async_trait]
impl MetricsEngine for OpenTelemetry {
    fn metrics(&self) -> &Metrics {
        &self.metrics
    }

    async fn shutdown(&self) -> RuntimeResult<()> {
        self.meter_provider
            .shutdown()
            .map_err(|error| RuntimeError::Transport(error.to_string()))
    }
}

#[async_trait]
impl TracingEngine for OpenTelemetry {
    async fn shutdown(&self) -> RuntimeResult<()> {
        self.tracer_provider
            .shutdown()
            .map_err(|error| RuntimeError::Transport(error.to_string()))
    }
}

#[async_trait]
impl LogsEngine for OpenTelemetry {
    async fn shutdown(&self) -> RuntimeResult<()> {
        self.logger_provider
            .shutdown()
            .map_err(|error| RuntimeError::Transport(error.to_string()))
    }
}

impl Config {
    pub fn from_environment(service_name: impl Into<String>) -> Self {
        Self {
            service_name: service_name.into(),
            endpoint: std::env::var("OTEL_EXPORTER_OTLP_ENDPOINT")
                .unwrap_or_else(|_| "http://localhost:4318".to_owned()),
            timeout: Duration::from_secs(10),
        }
    }
}

/// Production OpenTelemetry engine for all three signals.
///
/// Metrics are recorded into the OTLP meter only -- Counter and Histogram
/// recording skip the local Prometheus registry entirely while an OTel meter
/// is attached (see `metrics::Metrics::has_otel`), so there is a single source
/// of truth per mode instead of double bookkeeping. Gauge keeps writing its
/// local value unconditionally even with OTel attached, because it needs that
/// value to compute the delta OTel's UpDownCounter API expects, and some
/// gauges mix `add`/`inc`/`dec` with `set` on the same instance. Logs and
/// spans use the same `tracing` events, so instrumentation in operators and
/// transports cannot diverge between stdout and OTLP.
pub struct OpenTelemetry {
    metrics: Metrics,
    tracer_provider: SdkTracerProvider,
    meter_provider: SdkMeterProvider,
    logger_provider: SdkLoggerProvider,
}

impl OpenTelemetry {
    pub fn install(config: Config) -> RuntimeResult<Arc<Self>> {
        let resource = Resource::builder()
            .with_service_name(config.service_name.clone())
            .build();
        let endpoint = config.endpoint.trim_end_matches('/');

        let span_exporter = opentelemetry_otlp::SpanExporter::builder()
            .with_http()
            .with_endpoint(format!("{endpoint}/v1/traces"))
            .with_timeout(config.timeout)
            .build()
            .map_err(|error| RuntimeError::Transport(error.to_string()))?;
        let tracer_provider = SdkTracerProvider::builder()
            .with_resource(resource.clone())
            .with_batch_exporter(span_exporter)
            .build();

        let metric_exporter = opentelemetry_otlp::MetricExporter::builder()
            .with_http()
            .with_endpoint(format!("{endpoint}/v1/metrics"))
            .with_timeout(config.timeout)
            .build()
            .map_err(|error| RuntimeError::Transport(error.to_string()))?;
        let meter_provider = SdkMeterProvider::builder()
            .with_resource(resource.clone())
            .with_periodic_exporter(metric_exporter)
            .build();

        let log_exporter = opentelemetry_otlp::LogExporter::builder()
            .with_http()
            .with_endpoint(format!("{endpoint}/v1/logs"))
            .with_timeout(config.timeout)
            .build()
            .map_err(|error| RuntimeError::Transport(error.to_string()))?;
        let logger_provider = SdkLoggerProvider::builder()
            .with_resource(resource)
            .with_batch_exporter(log_exporter)
            .build();

        global::set_text_map_propagator(TextMapCompositePropagator::new(vec![
            Box::new(TraceContextPropagator::new()),
            Box::new(BaggagePropagator::new()),
        ]));
        let tracer = tracer_provider.tracer(config.service_name.clone());
        let trace_layer = tracing_opentelemetry::layer().with_tracer(tracer);
        let log_layer = OpenTelemetryTracingBridge::new(&logger_provider);
        tracing_subscriber::registry()
            .with(tracing_subscriber::EnvFilter::from_default_env())
            .with(tracing_subscriber::fmt::layer())
            .with(trace_layer)
            .with(log_layer)
            .try_init()
            .map_err(|error| RuntimeError::InvalidConfiguration(error.to_string()))?;

        global::set_tracer_provider(tracer_provider.clone());
        global::set_meter_provider(meter_provider.clone());
        let metrics = Metrics::with_meter(meter_provider.meter("servicelib"));
        Ok(Arc::new(Self {
            metrics,
            tracer_provider,
            meter_provider,
            logger_provider,
        }))
    }

    pub fn metrics(&self) -> &Metrics {
        &self.metrics
    }

    pub fn shutdown(&self) -> RuntimeResult<()> {
        self.logger_provider
            .shutdown()
            .map_err(|error| RuntimeError::Transport(error.to_string()))?;
        self.meter_provider
            .shutdown()
            .map_err(|error| RuntimeError::Transport(error.to_string()))?;
        self.tracer_provider
            .shutdown()
            .map_err(|error| RuntimeError::Transport(error.to_string()))
    }
}
