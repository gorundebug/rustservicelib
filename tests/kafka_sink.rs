use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use servicelib::{
    Consumer, MessageContext, Payload, Stream,
    api::{KafkaSaslMechanism, KafkaSecurityProtocol},
    datasink::kafka::{
        EndpointHandler, HandlerResult, Partitioner, RdkafkaKafkaDataSink, SinkMessage,
        StreamContext, make_endpoint_consumer,
        make_rdkafka_kafka_endpoint_consumer_with_partitioner,
    },
    runtime::{
        config::{KafkaDataConnectorConfig, KafkaEndpointConfig, SinkStreamConfig, StreamConfig},
        datasink::DataSink,
        environment::{Lifecycle, RuntimeEnvironment},
    },
};
use tokio::sync::Notify;

struct FixedPartitioner;

impl Partitioner<u32> for FixedPartitioner {
    fn partition(&self, _value: &u32, num_partitions: i32) -> HandlerResult<i32> {
        assert_eq!(num_partitions, 4);
        Ok(3)
    }
}

#[tokio::test]
async fn disabled_kafka_sink_owns_endpoints_without_contacting_broker() {
    let environment = RuntimeEnvironment::default();
    let sink_config = SinkStreamConfig {
        stream: StreamConfig::new(2, "publish orders"),
        endpoint_id: 3,
    };
    let connector_config = KafkaDataConnectorConfig {
        id: 5,
        name: "kafka".to_owned(),
        brokers: "localhost:9092".to_owned(),
        version: String::new(),
        dial_timeout: 0.0,
        use_partitioner: true,
        r#async: true,
        security_protocol: KafkaSecurityProtocol::PLAINTEXT,
        sasl_mechanism: KafkaSaslMechanism::PLAIN,
        username: String::new(),
        password: String::new(),
    };
    let endpoint_config = KafkaEndpointConfig {
        enabled: false,
        id: 3,
        name: "orders topic".to_owned(),
        id_data_connector: 5,
        create_topic: false,
        topic: "orders".to_owned(),
        partitions: 4,
        consumer_group: String::new(),
        replication_factor: 1,
    };
    environment.publish_runtime_config(Arc::new(
        servicelib::runtime::config::RuntimeConfig::from_parts(
            servicelib::runtime::config::CallSemantics::FunctionCall,
            [],
            [
                StreamConfig::new(1, "orders").into(),
                sink_config.clone().into(),
            ],
            [],
            [connector_config.clone().into()],
            [endpoint_config.into()],
            [],
        )
        .unwrap(),
    ));
    let source = Stream::new(&StreamConfig::new(1, "orders"), environment);
    let sink_stream = source
        .sink_with_result::<u32, String>(&sink_config)
        .unwrap();
    let data_sink = RdkafkaKafkaDataSink::from_stream(&sink_stream).unwrap();
    make_rdkafka_kafka_endpoint_consumer_with_partitioner(
        &sink_stream,
        Arc::clone(&data_sink),
        Handler {
            events: Arc::new(Mutex::new(Vec::new())),
        },
    )
    .unwrap();

    assert_eq!(data_sink.id(), 5);
    assert_eq!(data_sink.name(), "kafka");
    data_sink.start(MessageContext::new()).await.unwrap();
    data_sink.stop(MessageContext::new()).await.unwrap();
}

struct Handler {
    events: Arc<Mutex<Vec<&'static str>>>,
}

impl Partitioner<u32> for Handler {
    fn partition(&self, _value: &u32, num_partitions: i32) -> HandlerResult<i32> {
        assert_eq!(num_partitions, 4);
        Ok(3)
    }
}

#[async_trait]
impl EndpointHandler<(), u32, u32, String> for Handler {
    fn get_stream_id(&self, _context: &MessageContext, value: &u32) -> String {
        format!("order-{value}")
    }

    async fn begin_request(
        &self,
        context: MessageContext,
        _stream: StreamContext<u32, u32, String>,
    ) -> (MessageContext, ()) {
        self.events.lock().unwrap().push("begin");
        (context, ())
    }

    async fn consume_message(
        &self,
        _context: MessageContext,
        _stream: StreamContext<u32, u32, String>,
        _handler_state: &mut (),
        value: Payload<u32>,
        message: &mut SinkMessage<u32, String>,
    ) -> HandlerResult {
        self.events.lock().unwrap().push("consume");
        message.key = Some(value.to_be_bytes().to_vec());
        message.value = value.to_string().into_bytes();
        message.send(|partition, offset, error| {
            assert!(error.is_none());
            partition as u32 + offset as u32
        });
        Ok(())
    }

    async fn end_request(
        &self,
        _context: MessageContext,
        _stream: StreamContext<u32, u32, String>,
        result: &HandlerResult,
        _handler_state: (),
    ) {
        assert!(result.is_ok());
        self.events.lock().unwrap().push("end");
    }
}

struct ResultCollector {
    values: Arc<Mutex<Vec<u32>>>,
    ready: Arc<Notify>,
}

#[async_trait]
impl Consumer<u32> for ResultCollector {
    async fn consume(&self, _context: MessageContext, value: Payload<u32>) {
        self.values.lock().unwrap().push(*value);
        self.ready.notify_one();
    }
}

#[tokio::test]
async fn kafka_sink_preserves_lifecycle_partition_and_delivery_result() {
    let environment = RuntimeEnvironment::default();
    let source = Stream::new(&StreamConfig::new(1, "orders"), environment.clone());
    let sink = source
        .sink_with_result::<u32, String>(&SinkStreamConfig {
            stream: StreamConfig::new(2, "publish orders"),
            endpoint_id: 3,
        })
        .unwrap();
    let values = Arc::new(Mutex::new(Vec::new()));
    let ready = Arc::new(Notify::new());
    sink.stream()
        .try_set_consumer(
            Arc::new(ResultCollector {
                values: Arc::clone(&values),
                ready: Arc::clone(&ready),
            }),
            4,
        )
        .unwrap();

    let sent = Arc::new(Mutex::new(Vec::new()));
    let sent_copy = Arc::clone(&sent);
    let events = Arc::new(Mutex::new(Vec::new()));
    make_endpoint_consumer(
        &sink,
        KafkaEndpointConfig {
            enabled: true,
            id: 3,
            name: "orders topic".to_owned(),
            id_data_connector: 5,
            create_topic: false,
            topic: "orders".to_owned(),
            partitions: 4,
            consumer_group: String::new(),
            replication_factor: 1,
        },
        KafkaDataConnectorConfig {
            id: 5,
            name: "kafka".to_owned(),
            brokers: "localhost:9092".to_owned(),
            version: String::new(),
            dial_timeout: 0.0,
            use_partitioner: true,
            r#async: true,
            security_protocol: KafkaSecurityProtocol::PLAINTEXT,
            sasl_mechanism: KafkaSaslMechanism::PLAIN,
            username: String::new(),
            password: String::new(),
        },
        move |topic, key, value, partition| {
            let sent = Arc::clone(&sent_copy);
            async move {
                sent.lock().unwrap().push((topic, key, value, partition));
                Ok((partition.unwrap(), 11))
            }
        },
        Some(Arc::new(FixedPartitioner)),
        Handler {
            events: Arc::clone(&events),
        },
    )
    .unwrap();

    source
        .emit(MessageContext::new(), Payload::new(7_u32))
        .await;
    ready.notified().await;

    assert_eq!(&*events.lock().unwrap(), &["begin", "consume", "end"]);
    assert_eq!(&*values.lock().unwrap(), &[14]);
    let sent = sent.lock().unwrap();
    assert_eq!(sent[0].0, "orders");
    assert_eq!(sent[0].3, Some(3));
    assert_eq!(sent[0].1.as_deref(), Some(7_u32.to_be_bytes().as_slice()));
    assert_eq!(sent[0].2, b"7");

    let metrics = environment.metrics().render_prometheus();
    assert!(metrics.contains(
        r#"datasink_endpoint_messages_total{connector="kafka",endpoint="orders topic"} 1"#
    ));
}
