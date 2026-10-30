use std::sync::{Arc, atomic::{AtomicUsize, Ordering}};
use std::time::Duration;

use async_trait::async_trait;
use rdkafka::{ClientConfig, mocking::MockCluster, producer::{FutureProducer, FutureRecord}};
use tokio::sync::{Mutex, Notify, mpsc};
use servicelib::{
    MessageContext,
    api::{KafkaSaslMechanism, KafkaSecurityProtocol},
    datasource::kafka::{ConsumerMessage, EndpointHandler, HandlerError, HandlerResult,
        RdkafkaKafkaDataSource, ResultContext, StreamContext},
    operators::InputStream,
    runtime::{
        config::{CallSemantics, InputStreamConfig, KafkaDataConnectorConfig,
            KafkaEndpointConfig, RuntimeConfig, StreamConfig},
        environment::{Lifecycle, RuntimeEnvironment},
    },
};

#[derive(Default)]
struct Calls {
    begin: AtomicUsize,
    consume: AtomicUsize,
    end: AtomicUsize,
    completed: Notify,
}

struct ErrorProbe {
    calls: Arc<Calls>,
    errors: mpsc::UnboundedSender<(String, String, bool)>,
}

#[async_trait]
impl EndpointHandler<(), u32, u32, String> for ErrorProbe {
    fn concurrency(&self, _: &StreamContext<u32, u32, String>) -> usize { 1 }

    async fn on_error(&self, context: MessageContext, _: StreamContext<u32, u32, String>, error: HandlerError) {
        let _ = self.errors.send((
            context.stream_id().unwrap_or_default().to_owned(),
            error.to_string(), context.is_cancelled(),
        ));
    }

    async fn begin_request(&self, context: MessageContext, _: StreamContext<u32, u32, String>)
        -> Result<(MessageContext, ()), HandlerError> {
        self.calls.begin.fetch_add(1, Ordering::SeqCst);
        Ok((context, ()))
    }

    async fn consume_message(&self, _: MessageContext, _: StreamContext<u32, u32, String>,
        _: Arc<Mutex<()>>, message: ConsumerMessage, _: Arc<ResultContext<(), u32, u32, String>>)
        -> HandlerResult {
        assert_eq!(message.value, b"recovered");
        message.commit().await?;
        self.calls.consume.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    async fn get_message_id(&self, _: &MessageContext, _: &StreamContext<u32, u32, String>,
        _: Arc<Mutex<()>>, _: &u32) -> String {
        panic!("transport errors must not create a result request")
    }

    async fn end_request(&self, _: MessageContext, _: StreamContext<u32, u32, String>,
        result: &HandlerResult, _: Arc<Mutex<()>>) {
        assert!(result.is_ok(), "message processing failed: {result:?}");
        self.calls.end.fetch_add(1, Ordering::SeqCst);
        self.calls.completed.notify_one();
    }
}

// Exercise the public datasource lifecycle and real librdkafka polling, not a
// direct call to the handler. The broker is an isolated protocol mock.
#[tokio::test]
async fn kafka_transport_error_preserves_owner_and_recovers_without_fake_message() {
    let cluster = MockCluster::new(1).unwrap();
    cluster.create_topic("onerror", 1, 1).unwrap();
    let brokers = cluster.bootstrap_servers();
    cluster.broker_down(-1).unwrap();
    let environment = RuntimeEnvironment::default();
    let input_config = InputStreamConfig {
        stream: StreamConfig::new(1, "onerror input"), endpoint_id: 2,
    };
    let connector = KafkaDataConnectorConfig {
        id: 4, name: "onerror kafka".into(), brokers: brokers.clone(),
        version: String::new(), dial_timeout: 1000.0, use_partitioner: false,
        r#async: true, security_protocol: KafkaSecurityProtocol::PLAINTEXT,
        sasl_mechanism: KafkaSaslMechanism::PLAIN,
        username: String::new(), password: String::new(),
    };
    let endpoint = KafkaEndpointConfig {
        enabled: true, id: 2, name: "onerror topic".into(), id_data_connector: 4,
        tracing_enabled: false, create_topic: false, topic: "onerror".into(),
        partitions: 1, consumer_group: "onerror group".into(), replication_factor: 1,
    };
    environment.publish_runtime_config(Arc::new(RuntimeConfig::from_parts(
        CallSemantics::FunctionCall, [], [input_config.clone().into()], [],
        [connector.into()], [endpoint.into()], [],
    ).unwrap()));
    let input = InputStream::<u32, u32, String>::new(&input_config, environment);
    let datasource = RdkafkaKafkaDataSource::from_input(&input).unwrap();
    let calls = Arc::new(Calls::default());
    let (errors, mut received) = mpsc::unbounded_channel();
    datasource.add_endpoint(input, ErrorProbe { calls: calls.clone(), errors }).unwrap();
    let owner = MessageContext::new().with_stream_id("transport-owner");
    datasource.start(owner.clone()).await.unwrap();

    // Always stop the endpoint before propagating a timeout to the test runner.
    let outcome = tokio::time::timeout(Duration::from_secs(20), async {
        let (stream_id, error, cancelled) = received.recv().await.unwrap();
        assert_eq!(stream_id, "transport-owner");
        assert!(!error.is_empty());
        assert!(!cancelled);
        assert_eq!(calls.begin.load(Ordering::SeqCst), 0);
        assert_eq!(calls.consume.load(Ordering::SeqCst), 0);
        assert_eq!(calls.end.load(Ordering::SeqCst), 0);

        cluster.broker_up(-1).unwrap();
        let producer: FutureProducer = ClientConfig::new()
            .set("bootstrap.servers", &brokers)
            .set("message.timeout.ms", "5000")
            .create().unwrap();
        producer.send(FutureRecord::to("onerror").key("one").payload("recovered"),
            Duration::from_secs(5)).await.unwrap();
        calls.completed.notified().await;
    }).await;
    datasource.stop(owner.clone().with_timeout_limit(Duration::from_secs(5))).await.unwrap();
    outcome.expect("transport error or recovery did not reach the SDK handler");
    assert!(!owner.is_cancelled());
    assert_eq!(calls.begin.load(Ordering::SeqCst), 1);
    assert_eq!(calls.consume.load(Ordering::SeqCst), 1);
    assert_eq!(calls.end.load(Ordering::SeqCst), 1);
}
