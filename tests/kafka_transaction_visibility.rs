//! Native acceptance, deliberately ignored without an explicitly supplied broker.
//! Run with SA_TEST_KAFKA_BROKERS and --test kafka_transaction_visibility -- --ignored.

use std::{sync::{Arc, Mutex}, time::{Duration, SystemTime, UNIX_EPOCH}};

use async_trait::async_trait;
use rdkafka::{ClientConfig, producer::{FutureProducer, FutureRecord, Producer}};
use tokio::sync::{mpsc, Mutex as AsyncMutex};

use servicelib::{
    MessageContext,
    api::{KafkaSaslMechanism, KafkaSecurityProtocol},
    datasource::kafka::{ConsumerMessage, EndpointHandler, HandlerError, HandlerResult,
        RdkafkaKafkaDataSource, ResultContext, StreamContext},
    operators::{InputStream, MapFunction},
    runtime::{common::RuntimeStream, collector::Collect,
        config::{CallSemantics, InputStreamConfig, KafkaDataConnectorConfig,
            KafkaEndpointConfig, RuntimeConfig, StreamConfig},
        environment::{Lifecycle, RuntimeEnvironment}},
};

struct Observe(mpsc::UnboundedSender<String>);

impl MapFunction<String, String> for Observe {
    async fn map(&self, _context: MessageContext, _stream: &dyn RuntimeStream,
        value: &String, _out: &impl Collect<String>) {
        self.0.send(value.clone()).unwrap();
    }
}

struct Handler(Arc<Mutex<Vec<String>>>);

#[async_trait]
impl EndpointHandler<(), String, String, String> for Handler {
    fn concurrency(&self, _stream: &StreamContext<String, String, String>) -> usize { 2 }

    async fn on_error(&self, _context: MessageContext,
        _stream: StreamContext<String, String, String>, error: HandlerError) {
        self.0.lock().unwrap().push(error.to_string());
    }

    async fn begin_request(&self, context: MessageContext,
        _stream: StreamContext<String, String, String>) -> Result<(MessageContext, ()), HandlerError> {
        Ok((context, ()))
    }

    async fn consume_message(&self, context: MessageContext,
        stream: StreamContext<String, String, String>, _state: Arc<AsyncMutex<()>>,
        message: ConsumerMessage, _result: Arc<ResultContext<(), String, String, String>>) -> HandlerResult {
        stream.collect(context, String::from_utf8(message.value.clone())?).await;
        message.mark_message("observed");
        Ok(())
    }

    async fn get_message_id(&self, _context: &MessageContext,
        _stream: &StreamContext<String, String, String>, _state: Arc<AsyncMutex<()>>,
        value: &String) -> String { value.clone() }

    async fn end_request(&self, _context: MessageContext,
        _stream: StreamContext<String, String, String>, result: &HandlerResult,
        _state: Arc<AsyncMutex<()>>) {
        if let Err(error) = result { self.0.lock().unwrap().push(error.to_string()); }
    }
}

async fn send(producer: &FutureProducer, topic: &str, value: &str, partition: i32) {
    producer.send(FutureRecord::to(topic).key("visibility").payload(value).partition(partition),
        Duration::from_secs(10)).await.unwrap();
}

async fn expect(values: &mut mpsc::UnboundedReceiver<String>, expected: &str) {
    assert_eq!(tokio::time::timeout(Duration::from_secs(10), values.recv()).await.unwrap().unwrap(), expected);
}

#[tokio::test]
#[ignore = "requires disposable Kafka via SA_TEST_KAFKA_BROKERS"]
async fn native_endpoint_transaction_visibility() {
    let brokers = std::env::var("SA_TEST_KAFKA_BROKERS").expect("explicit native broker required");
    let topic = format!("sa-rust-visibility-{}-{}", std::process::id(),
        SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos());
    let environment = RuntimeEnvironment::default();
    let input_config = InputStreamConfig { stream: StreamConfig::new(1, "input"), endpoint_id: 2 };
    let connector = KafkaDataConnectorConfig {
        id: 4, name: "kafka".into(), brokers: brokers.clone(), version: String::new(), dial_timeout: 10_000.0,
        use_partitioner: false, r#async: true, security_protocol: KafkaSecurityProtocol::PLAINTEXT,
        sasl_mechanism: KafkaSaslMechanism::PLAIN, username: String::new(), password: String::new(),
    };
    let endpoint = KafkaEndpointConfig {
        enabled: true, id: 2, name: "events".into(), id_data_connector: 4, tracing_enabled: false,
        create_topic: true, topic: topic.clone(), partitions: 2, consumer_group: topic.clone(), replication_factor: 1,
    };
    environment.publish_runtime_config(Arc::new(RuntimeConfig::from_parts(
        CallSemantics::FunctionCall, [], [input_config.clone().into()], [],
        [connector.into()], [endpoint.into()], []).unwrap()));
    let input = InputStream::<String, String, String>::new(&input_config, environment.clone());
    let (sender, mut values) = mpsc::unbounded_channel();
    let _observation = input.stream().map(&(StreamConfig::new(3, "observed").into()), Observe(sender)).unwrap();
    let errors = Arc::new(Mutex::new(Vec::new()));
    let datasource = RdkafkaKafkaDataSource::from_input(&input).unwrap();
    datasource.add_endpoint(input, Handler(errors.clone())).unwrap();
    let context = MessageContext::new();
    datasource.start(context.clone()).await.unwrap();
    let ordinary: FutureProducer = ClientConfig::new().set("bootstrap.servers", &brokers)
        .set("message.timeout.ms", "10000").create().unwrap();
    let transactional: FutureProducer = ClientConfig::new().set("bootstrap.servers", &brokers)
        .set("message.timeout.ms", "10000").set("transactional.id", &topic).create().unwrap();
    transactional.init_transactions(Duration::from_secs(10)).unwrap();

    let checked = tokio::time::timeout(Duration::from_secs(60), async {
        send(&ordinary, &topic, "baseline", 0).await;
        expect(&mut values, "baseline").await;
        transactional.begin_transaction().unwrap();
        send(&transactional, &topic, "aborted", 0).await;
        transactional.abort_transaction(Duration::from_secs(10)).unwrap();
        send(&ordinary, &topic, "after-abort", 0).await;
        expect(&mut values, "after-abort").await;

        transactional.begin_transaction().unwrap();
        send(&transactional, &topic, "open-then-committed", 0).await;
        send(&ordinary, &topic, "behind-open", 0).await;
        send(&ordinary, &topic, "other-partition", 1).await;
        expect(&mut values, "other-partition").await;
        assert!(tokio::time::timeout(Duration::from_millis(500), values.recv()).await.is_err());
        transactional.commit_transaction(Duration::from_secs(10)).unwrap();
        expect(&mut values, "open-then-committed").await;
        expect(&mut values, "behind-open").await;

        transactional.begin_transaction().unwrap();
        send(&transactional, &topic, "aborted-again", 0).await;
        transactional.abort_transaction(Duration::from_secs(10)).unwrap();
        transactional.begin_transaction().unwrap();
        send(&transactional, &topic, "reused-producer-commit", 0).await;
        transactional.commit_transaction(Duration::from_secs(10)).unwrap();
        send(&ordinary, &topic, "final-barrier", 0).await;
        expect(&mut values, "reused-producer-commit").await;
        expect(&mut values, "final-barrier").await;
        assert!(values.try_recv().is_err());
        let failures = errors.lock().unwrap().clone();
        assert!(failures.is_empty(), "{failures:?}");
    }).await;
    datasource.stop(context).await.unwrap();
    checked.unwrap();
}
