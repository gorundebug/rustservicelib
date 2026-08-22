use rdkafka::{ClientContext, consumer::ConsumerContext, statistics::Statistics};

use crate::runtime::environment::{
    RuntimeResult,
    metrics::{Int64Gauge, Labels, Metrics},
};

#[derive(Clone)]
pub(crate) struct LibrdkafkaStatisticsContext {
    enabled: bool,
    brokers: Int64Gauge,
    brokers_up: Int64Gauge,
    reply_queue_messages: Int64Gauge,
    messages_queued: Int64Gauge,
    message_bytes_queued: Int64Gauge,
    requests_sent: Int64Gauge,
    responses_received: Int64Gauge,
    bytes_sent: Int64Gauge,
    bytes_received: Int64Gauge,
    messages_sent: Int64Gauge,
    messages_received: Int64Gauge,
    consumer_lag: Int64Gauge,
}

impl LibrdkafkaStatisticsContext {
    pub(crate) fn new(metrics: &Metrics, role: &str) -> RuntimeResult<Self> {
        let scope = metrics.scope(
            "kafka_client",
            [("role".to_owned(), role.to_owned())].into_iter().collect(),
        );
        Ok(Self {
            enabled: !metrics.is_noop(),
            brokers: scope.gauge(
                "brokers",
                "Brokers known to this librdkafka client",
                Labels::new(),
            )?,
            brokers_up: scope.gauge("brokers_up", "Brokers currently connected", Labels::new())?,
            reply_queue_messages: scope.gauge(
                "reply_queue_messages",
                "Operations waiting in the librdkafka reply queue",
                Labels::new(),
            )?,
            messages_queued: scope.gauge(
                "messages_queued",
                "Messages currently queued in librdkafka",
                Labels::new(),
            )?,
            message_bytes_queued: scope.gauge(
                "message_bytes_queued",
                "Message bytes currently queued in librdkafka",
                Labels::new(),
            )?,
            requests_sent: scope.gauge(
                "requests_sent",
                "Requests sent since this librdkafka client was created",
                Labels::new(),
            )?,
            responses_received: scope.gauge(
                "responses_received",
                "Responses received since this librdkafka client was created",
                Labels::new(),
            )?,
            bytes_sent: scope.gauge(
                "bytes_sent",
                "Bytes sent since this librdkafka client was created",
                Labels::new(),
            )?,
            bytes_received: scope.gauge(
                "bytes_received",
                "Bytes received since this librdkafka client was created",
                Labels::new(),
            )?,
            messages_sent: scope.gauge(
                "messages_sent",
                "Messages sent since this librdkafka client was created",
                Labels::new(),
            )?,
            messages_received: scope.gauge(
                "messages_received",
                "Messages received since this librdkafka client was created",
                Labels::new(),
            )?,
            consumer_lag: scope.gauge(
                "consumer_lag",
                "Sum of non-negative lag for assigned partitions",
                Labels::new(),
            )?,
        })
    }

    pub(crate) fn configure(&self, config: &mut rdkafka::ClientConfig) {
        if self.enabled {
            config.set("statistics.interval.ms", "1000");
        }
    }

    fn update(&self, statistics: Statistics) {
        self.brokers.set(saturating_i64(statistics.brokers.len()));
        self.brokers_up.set(saturating_i64(
            statistics
                .brokers
                .values()
                .filter(|broker| broker.state == "UP")
                .count(),
        ));
        self.reply_queue_messages.set(statistics.replyq);
        self.messages_queued.set(saturating_i64(statistics.msg_cnt));
        self.message_bytes_queued
            .set(saturating_i64(statistics.msg_size));
        self.requests_sent.set(statistics.tx);
        self.responses_received.set(statistics.rx);
        self.bytes_sent.set(statistics.tx_bytes);
        self.bytes_received.set(statistics.rx_bytes);
        self.messages_sent.set(statistics.txmsgs);
        self.messages_received.set(statistics.rxmsgs);
        let lag = statistics
            .topics
            .values()
            .flat_map(|topic| topic.partitions.values())
            .filter_map(|partition| (partition.consumer_lag >= 0).then_some(partition.consumer_lag))
            .fold(0_i64, i64::saturating_add);
        self.consumer_lag.set(lag);
    }
}

impl ClientContext for LibrdkafkaStatisticsContext {
    fn stats(&self, statistics: Statistics) {
        self.update(statistics);
    }
}

impl ConsumerContext for LibrdkafkaStatisticsContext {}

fn saturating_i64<T>(value: T) -> i64
where
    T: TryInto<i64>,
{
    value.try_into().unwrap_or(i64::MAX)
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use rdkafka::statistics::{Broker, Partition, Statistics, Topic};

    use super::*;

    #[test]
    fn exports_the_typed_librdkafka_snapshot() {
        let metrics = Metrics::default();
        let context = LibrdkafkaStatisticsContext::new(&metrics, "consumer").unwrap();
        context.stats(Statistics {
            replyq: 2,
            msg_cnt: 3,
            msg_size: 128,
            tx: 11,
            rx: 12,
            tx_bytes: 1024,
            rx_bytes: 2048,
            txmsgs: 4,
            rxmsgs: 5,
            brokers: HashMap::from([
                (
                    "one".to_owned(),
                    Broker {
                        state: "UP".to_owned(),
                        ..Broker::default()
                    },
                ),
                (
                    "two".to_owned(),
                    Broker {
                        state: "DOWN".to_owned(),
                        ..Broker::default()
                    },
                ),
            ]),
            topics: HashMap::from([(
                "orders".to_owned(),
                Topic {
                    partitions: HashMap::from([
                        (
                            0,
                            Partition {
                                consumer_lag: 7,
                                ..Partition::default()
                            },
                        ),
                        (
                            1,
                            Partition {
                                consumer_lag: -1,
                                ..Partition::default()
                            },
                        ),
                        (
                            2,
                            Partition {
                                consumer_lag: 9,
                                ..Partition::default()
                            },
                        ),
                    ]),
                    ..Topic::default()
                },
            )]),
            ..Statistics::default()
        });
        let output = metrics.render_prometheus();
        assert!(output.contains("kafka_client_brokers{role=\"consumer\"} 2"));
        assert!(output.contains("kafka_client_brokers_up{role=\"consumer\"} 1"));
        assert!(output.contains("kafka_client_messages_queued{role=\"consumer\"} 3"));
        assert!(output.contains("kafka_client_bytes_sent{role=\"consumer\"} 1024"));
        assert!(output.contains("kafka_client_consumer_lag{role=\"consumer\"} 16"));
    }

    #[test]
    fn noop_metrics_do_not_enable_librdkafka_statistics() {
        let context = LibrdkafkaStatisticsContext::new(&Metrics::noop(), "producer").unwrap();
        let mut config = rdkafka::ClientConfig::new();
        context.configure(&mut config);
        assert_eq!(config.get("statistics.interval.ms"), None);
    }
}
