use std::time::Instant;
use crate::runtime::environment::metrics::{Float64Histogram, Int64Counter, Int64Gauge, Labels, Metrics, MetricsError};

pub(super) struct RequestMetrics {
    enabled: bool,
    messages: Int64Counter,
    errors: Int64Counter,
    active: Int64Gauge,
    duration: Float64Histogram,
}

impl RequestMetrics {
    pub(super) fn new(metrics: &Metrics, protocol: &str, connector: &str, endpoint: &str) -> Result<Self, MetricsError> {
        let scope = metrics.scope("datasink_endpoint", Labels::from([
            ("protocol".into(), protocol.into()), ("connector".into(), connector.into()),
            ("endpoint".into(), endpoint.into()),
        ]));
        Ok(Self {
            enabled: !metrics.is_noop(),
            messages: scope.counter("messages_total", "Total number of successfully processed messages in data sink endpoint", Labels::new())?,
            errors: scope.counter("events_total", "Total number of events in data sink endpoint", Labels::from([("event".into(), "request_error".into())]))?,
            active: scope.gauge("active_requests", "Number of active requests in data sink endpoint", Labels::new())?,
            duration: scope.histogram("request_duration_seconds", "Request duration in seconds for data sink endpoint", Labels::new(), None)?,
        })
    }

    pub(super) fn start(&self) -> Request<'_> {
        let started = if self.enabled { self.active.inc(); Some(Instant::now()) } else { None };
        Request { metrics: self, started, success: false }
    }
}

/// Dropping a cancelled handler must not leave an active-request gauge elevated.
/// A handler's success says nothing about a subsequently completed async publish.
pub(super) struct Request<'a> {
    metrics: &'a RequestMetrics,
    started: Option<Instant>,
    success: bool,
}
impl Request<'_> {
    pub(super) fn finish(mut self, success: bool) { self.success = success; }
}
impl Drop for Request<'_> {
    fn drop(&mut self) {
        if let Some(started) = self.started {
            self.metrics.active.dec();
            self.metrics.duration.observe(started.elapsed().as_secs_f64());
            if self.success { self.metrics.messages.inc(); } else { self.metrics.errors.inc(); }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn request_metrics_account_success_error_and_dropped_future() {
        let metrics = Metrics::default();
        let endpoint = RequestMetrics::new(&metrics, "nats", "events", "publish").unwrap();
        endpoint.start().finish(true);
        endpoint.start().finish(false);
        drop(endpoint.start());
        assert_eq!(endpoint.messages.get(), 1);
        assert_eq!(endpoint.errors.get(), 2);
        assert!(metrics.render_prometheus().contains("datasink_endpoint_active_requests{connector=\"events\",endpoint=\"publish\",protocol=\"nats\"} 0"));
        assert!(!metrics.render_prometheus().contains("broker_endpoint"));
    }
    #[test]
    fn request_metrics_noop_has_no_series_or_clock() {
        let metrics = Metrics::noop();
        let endpoint = RequestMetrics::new(&metrics, "nats", "events", "publish").unwrap();
        let request = endpoint.start();
        assert!(request.started.is_none());
        drop(request);
        assert!(metrics.render_prometheus().is_empty());
    }
}
