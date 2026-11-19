use std::{sync::Arc, time::Instant};
use crate::runtime::{config::RuntimeEndpointConfig, environment::{RuntimeEnvironment,
    RuntimeError, RuntimeResult, metrics::{Labels, Int64Counter, Int64Gauge, Float64Histogram}}};
use super::HandlerResult;

pub(crate) struct EndpointMetrics {
    requests: Int64Counter,
    pub results: Int64Counter,
    errors: Int64Counter,
    cancellations: Int64Counter,
    active: Int64Gauge,
    duration: Float64Histogram,
}
impl EndpointMetrics {
    pub fn bind(environment: &RuntimeEnvironment, endpoint_id: i32) -> RuntimeResult<Option<Arc<Self>>> {
        let metrics = environment.metrics();
        if metrics.is_noop() { return Ok(None); }
        let config = environment.runtime_config();
        let endpoint = config.endpoint_by_id(endpoint_id).ok_or_else(||
            RuntimeError::InvalidConfiguration("Query metrics require an endpoint".into()))?;
        let operation = match endpoint.as_ref() {
            RuntimeEndpointConfig::Query(config) => format!("{:?}", config.query_operation),
            RuntimeEndpointConfig::S3Query(config) => format!("{:?}", config.query_operation),
            _ => return Err(RuntimeError::InvalidConfiguration("Query metrics require a Query endpoint".into())),
        };
        let connector = config.data_connector_by_id(endpoint.data_connector_id()).ok_or_else(||
            RuntimeError::InvalidConfiguration("Query metrics require a connector".into()))?;
        let scope = metrics.scope("dataquery_endpoint", Labels::from([
            ("connector".into(), connector.name().into()),
            ("endpoint".into(), endpoint.name().into()), ("operation".into(), operation)]));
        Ok(Some(Arc::new(Self {
            requests: scope.counter("requests_total", "Admitted Query endpoint requests", Labels::new())?,
            results: scope.counter("results_total", "Query result collector emissions, including application completion", Labels::new())?,
            errors: scope.counter("failures_total", "Failed Query endpoint requests", Labels::from([("outcome".into(), "error".into())]))?,
            cancellations: scope.counter("failures_total", "Failed Query endpoint requests", Labels::from([("outcome".into(), "cancelled".into())]))?,
            active: scope.gauge("active_requests", "Active Query endpoint requests through EndRequest", Labels::new())?,
            duration: scope.histogram("request_duration_seconds", "Query request duration through EndRequest in seconds", Labels::new(), None)?,
        })))
    }
    pub fn outcome(&self, result: &HandlerResult, cancelled: bool) {
        if let Err(error) = result {
            let cancelled = cancelled || error.downcast_ref::<std::io::Error>()
                .is_some_and(|error| error.kind() == std::io::ErrorKind::Interrupted);
            if cancelled { self.cancellations.inc(); } else { self.errors.inc(); }
        }
    }
}

pub(crate) struct Request {
    metrics: Option<Arc<EndpointMetrics>>,
    started: Option<Instant>,
}
impl Request {
    pub fn begin(metrics: Option<Arc<EndpointMetrics>>) -> Self {
        let started = metrics.as_ref().map(|metrics| {
            metrics.requests.inc(); metrics.active.inc(); Instant::now()
        });
        Self { metrics, started }
    }
}
impl Drop for Request {
    fn drop(&mut self) {
        if let (Some(metrics), Some(started)) = (&self.metrics, self.started) {
            metrics.active.dec();
            metrics.duration.observe(started.elapsed().as_secs_f64());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime::config::{CallSemantics, RuntimeConfig, RuntimeDataConnectorConfig,
        PostgreSQLSQLxDataConnectorConfig, PostgreSQLQueryOperation, QueryEndpointConfig};

    #[test]
    fn query_metrics_track_concurrent_scopes_and_cancelled_outcomes() {
        let environment = RuntimeEnvironment::default();
        environment.publish_runtime_config(Arc::new(RuntimeConfig::from_parts(
            CallSemantics::FunctionCall, [], [], [],
            [RuntimeDataConnectorConfig::PostgreSQLSQLx(PostgreSQLSQLxDataConnectorConfig {
                id: 1, name: "database".into(), connection_string: "postgres://localhost/test".into(),
                connections_count: 1, connect_timeout_ms: 1000, operation_timeout_ms: 0,
            })],
            [RuntimeEndpointConfig::Query(QueryEndpointConfig {
                id: 2, name: "rows".into(), id_data_connector: 1, enabled: true,
                tracing_enabled: false, query_operation: PostgreSQLQueryOperation::Rows, sql_file: None,
            })], []).unwrap()));
        let metrics = EndpointMetrics::bind(&environment, 2).unwrap().unwrap();
        let first = Request::begin(Some(metrics.clone()));
        let second = Request::begin(Some(metrics.clone()));
        let labels = "{connector=\"database\",endpoint=\"rows\",operation=\"Rows\"}";
        let active = || environment.metrics().render_prometheus();
        assert!(active().contains(&format!("dataquery_endpoint_active_requests{labels} 2")));
        metrics.results.inc();
        metrics.results.inc();
        metrics.outcome(&Err(std::io::Error::new(std::io::ErrorKind::Interrupted, "cancelled").into()), false);
        metrics.outcome(&Err(std::io::Error::other("private SQL/key must not become a label").into()), false);
        drop(first);
        assert!(active().contains(&format!("dataquery_endpoint_active_requests{labels} 1")));
        drop(second);
        assert!(active().contains(&format!("dataquery_endpoint_active_requests{labels} 0")));
        assert!(active().contains(&format!("dataquery_endpoint_requests_total{labels} 2")));
        let rendered = active();
        assert!(rendered.contains(&format!("dataquery_endpoint_results_total{labels} 2")));
        for outcome in ["error", "cancelled"] {
            assert!(rendered.contains(&format!("dataquery_endpoint_failures_total{{connector=\"database\",endpoint=\"rows\",operation=\"Rows\",outcome=\"{outcome}\"}} 1")));
        }
        assert!(!rendered.contains("private SQL/key"));
    }
}
