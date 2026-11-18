use super::{Delivery, HandlerResult};
use crate::runtime::environment::metrics::{Int64Counter, Labels, Metrics, MetricsError};

/// Bound once per endpoint. No message values become metric labels.
pub(super) struct PublicationMetrics {
    outcomes: Vec<(&'static str, Int64Counter)>,
}

impl PublicationMetrics {
    pub(super) fn new(metrics: &Metrics, protocol: &str, connector: &str, endpoint: &str) -> Result<Self, MetricsError> {
        if metrics.is_noop() { return Ok(Self { outcomes: vec![] }); }
        let scope = metrics.scope("broker_endpoint", Labels::from([
            ("protocol".into(), protocol.into()),
            ("connector".into(), connector.into()),
            ("endpoint".into(), endpoint.into()),
        ]));
        let names: &[&str] = if protocol == "jetstream" { &["acknowledged", "error"] }
            else { &["client-accepted", "flushed", "error"] };
        let outcomes = names.iter().map(|&name| Ok((name, scope.counter(
            "publication_outcomes_total", "Observed broker publication outcomes",
            Labels::from([("outcome".into(), name.into())]),
        )?))).collect::<Result<_, MetricsError>>()?;
        Ok(Self { outcomes })
    }

    fn record(&self, outcome: &str) {
        if let Some((_, counter)) = self.outcomes.iter().find(|(name, _)| *name == outcome) { counter.inc(); }
    }

    pub(super) fn error(&self) { self.record("error"); }

    pub(super) fn observe(&self, result: &HandlerResult<Delivery>) {
        self.record(match result {
            Err(_) => "error",
            Ok(d) if d.pub_ack.is_some() => "acknowledged",
            Ok(d) if d.flushed => "flushed",
            Ok(_) => "client-accepted",
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn publication_metrics_keep_outcomes_distinct() {
        let metrics = Metrics::default();
        let core = PublicationMetrics::new(&metrics, "nats", "broker", "events").unwrap();
        core.observe(&Ok(Delivery { flushed: false, pub_ack: None }));
        core.observe(&Ok(Delivery { flushed: true, pub_ack: None }));
        core.observe(&Err("failure".into()));
        let js = PublicationMetrics::new(&metrics, "jetstream", "broker", "events").unwrap();
        let ack = serde_json::from_str(r#"{"stream":"events","seq":1}"#).unwrap();
        js.observe(&Ok(Delivery { flushed: false, pub_ack: Some(ack) }));
        let output = metrics.render_prometheus();
        for (protocol, outcome) in [("nats", "client-accepted"), ("nats", "flushed"), ("nats", "error"), ("jetstream", "acknowledged")] {
            assert!(output.contains(&format!("broker_endpoint_publication_outcomes_total{{connector=\"broker\",endpoint=\"events\",outcome=\"{outcome}\",protocol=\"{protocol}\"}} 1")), "{output}");
        }
        assert!(!output.contains("failure"));
    }

    #[test]
    fn publication_metrics_noop_registers_nothing() {
        let metrics = Metrics::noop();
        let publication = PublicationMetrics::new(&metrics, "nats", "broker", "events").unwrap();
        publication.error();
        assert!(publication.outcomes.is_empty());
        assert!(metrics.render_prometheus().is_empty());
    }
}
