use super::ConsumerMessage;
use crate::runtime::environment::metrics::{Int64Counter, Labels, Metrics, MetricsError};

/// Endpoint-bound native observations, independent of graph completion/settlement.
pub(super) struct SourceMetrics {
    received: Option<Int64Counter>,
    redelivered: Option<Int64Counter>,
}

impl SourceMetrics {
    pub(super) fn new(metrics: &Metrics, jetstream: bool, connector: &str, endpoint: &str) -> Result<Self, MetricsError> {
        if metrics.is_noop() {
            return Ok(Self { received: None, redelivered: None });
        }
        let scope = metrics.scope("broker_endpoint", Labels::from([
            ("protocol".into(), if jetstream { "jetstream" } else { "nats" }.into()),
            ("connector".into(), connector.into()),
            ("endpoint".into(), endpoint.into()),
        ]));
        let counter = |name: &str| scope.counter("source_observations_total",
            "Observed native broker source records",
            Labels::from([("observation".into(), name.into())]));
        Ok(Self {
            received: Some(counter("received")?),
            redelivered: if jetstream { Some(counter("redelivered")?) } else { None },
        })
    }

    pub(super) fn observe(&self, message: &ConsumerMessage) {
        self.record(|| message.jetstream.as_ref().and_then(|m| m.info().ok().map(|info| info.delivered)));
    }

    fn record(&self, delivered: impl FnOnce() -> Option<i64>) {
        let Some(received) = &self.received else { return };
        received.inc();
        if let Some(redelivered) = &self.redelivered {
            if delivered().is_some_and(|count| count > 1) { redelivered.inc(); }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime::environment::metrics::{MetricsEngine, PrometheusMetricsEngine};

    #[test]
    fn native_source_observations_export_exact_family_and_counts() {
        let engine = PrometheusMetricsEngine::new();
        let metrics = engine.metrics();
        let core = SourceMetrics::new(metrics, false, "broker", "input").unwrap();
        core.record(|| panic!("Core must not parse JetStream metadata"));
        let js = SourceMetrics::new(metrics, true, "broker", "input").unwrap();
        for delivered in [None, Some(0), Some(1), Some(2), Some(12)] { js.record(|| delivered); }
        let text = metrics.render_prometheus();
        let samples: Vec<_> = text.lines().filter(|line| !line.starts_with('#') && !line.is_empty()).collect();
        assert_eq!(samples.len(), 3, "{text}");
        for (protocol, observation, count) in [("nats", "received", 1), ("jetstream", "received", 5), ("jetstream", "redelivered", 2)] {
            assert!(samples.contains(&format!("broker_endpoint_source_observations_total{{connector=\"broker\",endpoint=\"input\",observation=\"{observation}\",protocol=\"{protocol}\"}} {count}").as_str()), "{text}");
        }
    }

    #[test]
    fn noop_registers_nothing_and_does_not_parse_metadata() {
        let metrics = Metrics::noop();
        for js in [false, true] {
            let observer = SourceMetrics::new(&metrics, js, "broker", "input").unwrap();
            assert!(observer.received.is_none() && observer.redelivered.is_none());
            observer.record(|| panic!("noop must not parse metadata"));
        }
        assert!(metrics.render_prometheus().is_empty());
    }
}
