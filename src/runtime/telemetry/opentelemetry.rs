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
use tracing_subscriber::{
    filter::filter_fn, layer::SubscriberExt, util::SubscriberInitExt, Layer,
};

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
    pub metrics_enabled: bool,
    pub tracing_enabled: bool,
    pub logs_enabled: bool,
}

#[async_trait]
impl MetricsEngine for OpenTelemetry {
    fn metrics(&self) -> &Metrics {
        &self.metrics
    }

    async fn shutdown(&self) -> RuntimeResult<()> {
        if let Some(provider) = &self.meter_provider {
            provider
                .shutdown()
                .map_err(|error| RuntimeError::Transport(error.to_string()))?;
        }
        Ok(())
    }
}

#[async_trait]
impl TracingEngine for OpenTelemetry {
    async fn shutdown(&self) -> RuntimeResult<()> {
        if let Some(provider) = &self.tracer_provider {
            provider
                .shutdown()
                .map_err(|error| RuntimeError::Transport(error.to_string()))?;
        }
        Ok(())
    }
}

#[async_trait]
impl LogsEngine for OpenTelemetry {
    async fn shutdown(&self) -> RuntimeResult<()> {
        if let Some(provider) = &self.logger_provider {
            provider
                .shutdown()
                .map_err(|error| RuntimeError::Transport(error.to_string()))?;
        }
        Ok(())
    }
}

impl Config {
    pub fn from_environment(service_name: impl Into<String>) -> Self {
        Self {
            service_name: service_name.into(),
            endpoint: std::env::var("OTEL_EXPORTER_OTLP_ENDPOINT")
                .unwrap_or_else(|_| "http://localhost:4318".to_owned()),
            timeout: Duration::from_secs(10),
            metrics_enabled: !environment_flag_enabled("SERVICELIB_NOOP_METRICS"),
            tracing_enabled: !environment_flag_enabled("SERVICELIB_NOOP_TRACING"),
            logs_enabled: !environment_flag_enabled("SERVICELIB_NOOP_LOGS"),
        }
    }
}

pub fn environment_flag_enabled(name: &str) -> bool {
    std::env::var(name).is_ok_and(|value| flag_value_enabled(&value))
}

fn flag_value_enabled(value: &str) -> bool {
    matches!(
        value.trim().to_ascii_lowercase().as_str(),
        "1" | "true" | "yes" | "on"
    )
}

#[cfg(test)]
mod tests {
    use super::flag_value_enabled;

    #[test]
    fn boolean_environment_values_are_strict() {
        for value in ["1", "true", "TRUE", " yes ", "On"] {
            assert!(flag_value_enabled(value), "{value:?}");
        }
        for value in ["", "0", "false", "no", "off", "anything"] {
            assert!(!flag_value_enabled(value), "{value:?}");
        }
    }
}

pub fn install_stdout(logs_enabled: bool, tracing_enabled: bool) -> RuntimeResult<()> {
    if !logs_enabled && !tracing_enabled {
        return Ok(());
    }
    let filter = filter_fn(move |metadata| {
        (logs_enabled && metadata.is_event()) || (tracing_enabled && metadata.is_span())
    });
    tracing_subscriber::registry()
        .with(tracing_subscriber::fmt::layer().with_filter(filter))
        .try_init()
        .map_err(|error| RuntimeError::InvalidConfiguration(error.to_string()))
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
    tracer_provider: Option<SdkTracerProvider>,
    meter_provider: Option<SdkMeterProvider>,
    logger_provider: Option<SdkLoggerProvider>,
}

impl OpenTelemetry {
    pub fn install(config: Config) -> RuntimeResult<Arc<Self>> {
        let resource = Resource::builder()
            .with_service_name(config.service_name.clone())
            .build();
        let endpoint = config.endpoint.trim_end_matches('/');

        let tracer_provider = if config.tracing_enabled {
            let exporter = opentelemetry_otlp::SpanExporter::builder()
                .with_http()
                .with_endpoint(format!("{endpoint}/v1/traces"))
                .with_timeout(config.timeout)
                .build()
                .map_err(|error| RuntimeError::Transport(error.to_string()))?;
            Some(
                SdkTracerProvider::builder()
                    .with_resource(resource.clone())
                    .with_batch_exporter(exporter)
                    .build(),
            )
        } else {
            None
        };

        let meter_provider = if config.metrics_enabled {
            let exporter = opentelemetry_otlp::MetricExporter::builder()
                .with_http()
                .with_endpoint(format!("{endpoint}/v1/metrics"))
                .with_timeout(config.timeout)
                .build()
                .map_err(|error| RuntimeError::Transport(error.to_string()))?;
            Some(
                SdkMeterProvider::builder()
                    .with_resource(resource.clone())
                    .with_periodic_exporter(exporter)
                    .build(),
            )
        } else {
            None
        };

        let logger_provider = if config.logs_enabled {
            let exporter = opentelemetry_otlp::LogExporter::builder()
                .with_http()
                .with_endpoint(format!("{endpoint}/v1/logs"))
                .with_timeout(config.timeout)
                .build()
                .map_err(|error| RuntimeError::Transport(error.to_string()))?;
            Some(
                SdkLoggerProvider::builder()
                    .with_resource(resource)
                    .with_batch_exporter(exporter)
                    .build(),
            )
        } else {
            None
        };

        global::set_text_map_propagator(TextMapCompositePropagator::new(vec![
            Box::new(TraceContextPropagator::new()),
            Box::new(BaggagePropagator::new()),
        ]));
        if config.tracing_enabled || config.logs_enabled {
            let trace_layer = tracer_provider.as_ref().map(|provider| {
                tracing_opentelemetry::layer()
                    .with_tracer(provider.tracer(config.service_name.clone()))
            });
            let log_layer = logger_provider
                .as_ref()
                .map(OpenTelemetryTracingBridge::new);
            tracing_subscriber::registry()
                .with(tracing_subscriber::EnvFilter::from_default_env())
                .with(config.logs_enabled.then(tracing_subscriber::fmt::layer))
                .with(trace_layer)
                .with(log_layer)
                .try_init()
                .map_err(|error| RuntimeError::InvalidConfiguration(error.to_string()))?;
        }

        if let Some(provider) = &tracer_provider {
            global::set_tracer_provider(provider.clone());
        }
        let metrics = if let Some(provider) = &meter_provider {
            global::set_meter_provider(provider.clone());
            Metrics::with_meter(provider.meter("servicelib"))
        } else {
            Metrics::noop()
        };
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
        if let Some(provider) = &self.logger_provider {
            provider
                .shutdown()
                .map_err(|error| RuntimeError::Transport(error.to_string()))?;
        }
        if let Some(provider) = &self.meter_provider {
            provider
                .shutdown()
                .map_err(|error| RuntimeError::Transport(error.to_string()))?;
        }
        if let Some(provider) = &self.tracer_provider {
            provider
                .shutdown()
                .map_err(|error| RuntimeError::Transport(error.to_string()))?;
        }
        Ok(())
    }
}
