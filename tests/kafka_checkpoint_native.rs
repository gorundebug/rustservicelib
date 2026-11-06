//! Public endpoint recovery against an explicitly supplied disposable broker.
use std::{collections::{BTreeMap, BTreeSet}, sync::{Arc, Mutex}, time::{Duration, SystemTime, UNIX_EPOCH}};
use async_trait::async_trait;
use rdkafka::{ClientConfig, Offset, TopicPartitionList,
    admin::{AdminClient, AdminOptions, NewTopic, TopicReplication},
    client::DefaultClientContext,
    consumer::{BaseConsumer, Consumer, CommitMode},
    producer::{FutureProducer, FutureRecord}};
use tokio::sync::{mpsc, Mutex as AsyncMutex};
use servicelib::{MessageContext,
    api::{KafkaSaslMechanism, KafkaSecurityProtocol},
    datasource::kafka::{CheckpointOffsets, ConsumerMessage, EndpointHandler, HandlerError,
        HandlerResult, RdkafkaKafkaDataSource, ResultContext, StreamContext},
    operators::InputStream,
    runtime::{config::{CallSemantics, InputStreamConfig, KafkaDataConnectorConfig,
        KafkaEndpointConfig, RuntimeConfig, StreamConfig},
        environment::{Lifecycle, RuntimeEnvironment}}};

struct Handler {
    offsets: Arc<Mutex<CheckpointOffsets>>,
    messages: mpsc::UnboundedSender<ConsumerMessage>,
    errors: mpsc::UnboundedSender<String>,
}
#[async_trait]
impl EndpointHandler<(), String, String, String> for Handler {
    fn restore_checkpoint(&self, _: &MessageContext, _: &StreamContext<String, String, String>)
        -> HandlerResult<Option<CheckpointOffsets>> { Ok(Some(self.offsets.lock().unwrap().clone())) }
    fn concurrency(&self, _: &StreamContext<String, String, String>) -> usize { 2 }
    async fn begin_request(&self, context: MessageContext, _: StreamContext<String, String, String>)
        -> Result<(MessageContext, ()), HandlerError> { Ok((context, ())) }
    async fn consume_message(&self, context: MessageContext, stream: StreamContext<String, String, String>,
        _: Arc<AsyncMutex<()>>, message: ConsumerMessage, _: Arc<ResultContext<(), String, String, String>>)
        -> HandlerResult {
        stream.collect(context, String::from_utf8(message.value.clone())?).await;
        self.messages.send(message).unwrap();
        Ok(())
    }
    async fn get_message_id(&self, _: &MessageContext, _: &StreamContext<String, String, String>,
        _: Arc<AsyncMutex<()>>, value: &String) -> String { value.clone() }
    async fn on_error(&self, _: MessageContext, _: StreamContext<String, String, String>, error: HandlerError) {
        self.errors.send(error.to_string()).unwrap();
    }
    async fn end_request(&self, _: MessageContext, _: StreamContext<String, String, String>,
        result: &HandlerResult, _: Arc<AsyncMutex<()>>) {
        if let Err(error) = result { self.errors.send(error.to_string()).unwrap(); }
    }
}

struct Endpoint {
    datasource: Arc<RdkafkaKafkaDataSource>,
    offsets: Arc<Mutex<CheckpointOffsets>>,
    messages: mpsc::UnboundedReceiver<ConsumerMessage>,
    errors: mpsc::UnboundedReceiver<String>,
}
impl Endpoint {
    async fn start(brokers: &str, topic: &str, offsets: CheckpointOffsets) -> Self {
        let environment = RuntimeEnvironment::default();
        let input_config = InputStreamConfig { stream: StreamConfig::new(1, "input"), endpoint_id: 2 };
        let connector = KafkaDataConnectorConfig {
            id: 4, name: "kafka".into(), brokers: brokers.into(), version: String::new(), dial_timeout: 10_000.0,
            use_partitioner: false, r#async: true, security_protocol: KafkaSecurityProtocol::PLAINTEXT,
            sasl_mechanism: KafkaSaslMechanism::PLAIN, username: String::new(), password: String::new(),
        };
        let endpoint = KafkaEndpointConfig {
            enabled: true, id: 2, name: "events".into(), id_data_connector: 4, tracing_enabled: false,
            create_topic: false, topic: topic.into(), partitions: 2, consumer_group: topic.into(), replication_factor: 1,
        };
        environment.publish_runtime_config(Arc::new(RuntimeConfig::from_parts(
            CallSemantics::FunctionCall, [], [input_config.clone().into()], [],
            [connector.into()], [endpoint.into()], []).unwrap()));
        let input = InputStream::<String, String, String>::new(&input_config, environment);
        let datasource = RdkafkaKafkaDataSource::from_input(&input).unwrap();
        let offsets = Arc::new(Mutex::new(offsets));
        let (messages, receiver) = mpsc::unbounded_channel();
        let (errors, error_receiver) = mpsc::unbounded_channel();
        datasource.add_endpoint(input, Handler { offsets: offsets.clone(), messages, errors }).unwrap();
        datasource.start(MessageContext::new()).await.unwrap();
        Self { datasource, offsets, messages: receiver, errors: error_receiver }
    }
    async fn next(&mut self) -> ConsumerMessage {
        tokio::time::timeout(Duration::from_secs(25), self.messages.recv()).await.unwrap().unwrap()
    }
    async fn stop(&self) {
        tokio::time::timeout(Duration::from_secs(15), self.datasource.stop(MessageContext::new())).await.unwrap().unwrap();
    }
}

struct Fixture {
    brokers: String,
    topic: String,
    admin: AdminClient<DefaultClientContext>,
    group: BaseConsumer,
}
impl Fixture {
    async fn new() -> Self {
        let brokers = std::env::var("SA_TEST_KAFKA_BROKERS").expect("explicit disposable broker required");
        let topic = format!("sa-rust-checkpoint-{}-{}", std::process::id(), SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos());
        let admin: AdminClient<_> = ClientConfig::new().set("bootstrap.servers", &brokers).create().unwrap();
        for result in admin.create_topics([&NewTopic::new(&topic, 2, TopicReplication::Fixed(1))], &AdminOptions::new()).await.unwrap() {
            result.unwrap();
        }
        let producer: FutureProducer = ClientConfig::new().set("bootstrap.servers", &brokers)
            .set("message.timeout.ms", "10000").create().unwrap();
        for partition in 0..2 {
            for offset in 0..5 {
                producer.send(FutureRecord::to(&topic).key("checkpoint").payload(&offset.to_string()).partition(partition), Duration::from_secs(10)).await.unwrap();
            }
        }
        let group: BaseConsumer = ClientConfig::new().set("bootstrap.servers", &brokers)
            .set("group.id", &topic).set("enable.auto.commit", "false").create().unwrap();
        let mut positions = TopicPartitionList::new();
        for partition in 0..2 { positions.add_partition_offset(&topic, partition, Offset::Offset(2)).unwrap(); }
        group.commit(&positions, CommitMode::Sync).unwrap();
        Self { brokers, topic, admin, group }
    }
    fn committed(&self) -> Vec<Offset> {
        let mut list = TopicPartitionList::new();
        for partition in 0..2 { list.add_partition(&self.topic, partition); }
        let result = self.group.committed_offsets(list, Duration::from_secs(10)).unwrap();
        (0..2).map(|p| result.find_partition(&self.topic, p).unwrap().offset()).collect()
    }
    async fn close(&self) {
        for result in self.admin.delete_topics(&[&self.topic], &AdminOptions::new()).await.unwrap() { result.unwrap(); }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires disposable Kafka via SA_TEST_KAFKA_BROKERS"]
async fn native_checkpoint_overrides_group_rebalances_and_rejects_old_ack() {
    let fixture = Fixture::new().await;
    let mut first = Endpoint::start(&fixture.brokers, &fixture.topic, BTreeMap::from([(0, 1), (1, 3)])).await;
    let mut messages = Vec::new();
    for _ in 0..6 { messages.push(first.next().await); }
    assert_eq!(messages.iter().map(|m| (m.partition, m.offset)).collect::<BTreeSet<_>>(),
        BTreeSet::from([(0,1),(0,2),(0,3),(0,4),(1,3),(1,4)]));
    for message in &messages { if message.offset == 4 { message.mark_message("accepted"); message.commit().await.unwrap(); } }
    assert_eq!(fixture.committed(), vec![Offset::Offset(5); 2]);
    *first.offsets.lock().unwrap() = BTreeMap::from([(0,4),(1,4)]);
    let mut second = Endpoint::start(&fixture.brokers, &fixture.topic, BTreeMap::from([(0,4),(1,4)])).await;
    let (one, two) = tokio::join!(first.next(), second.next());
    assert_eq!((one.offset, two.offset), (4,4));
    assert_ne!(one.partition, two.partition);
    for current in [&one, &two] { current.mark_message("accepted"); current.commit().await.unwrap(); }
    let old = messages.iter().find(|m| m.partition == two.partition && m.offset < 4).unwrap();
    old.mark_message("stale");
    assert!(old.commit().await.unwrap_err().to_string().contains("revoked"));
    assert_eq!(fixture.committed(), vec![Offset::Offset(5); 2]);
    assert!(first.errors.try_recv().is_err() && second.errors.try_recv().is_err());
    second.stop().await;
    first.stop().await;
    fixture.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires disposable Kafka via SA_TEST_KAFKA_BROKERS"]
async fn native_invalid_checkpoint_neither_delivers_nor_commits() {
    for offsets in [BTreeMap::new(), BTreeMap::from([(0,6),(1,6)])] {
        let fixture = Fixture::new().await;
        let mut endpoint = Endpoint::start(&fixture.brokers, &fixture.topic, offsets).await;
        let error = tokio::time::timeout(Duration::from_secs(15), endpoint.errors.recv()).await.unwrap().unwrap();
        assert!(error.contains("checkpoint"), "{error}");
        assert!(tokio::time::timeout(Duration::from_millis(300), endpoint.messages.recv()).await.is_err());
        assert_eq!(fixture.committed(), vec![Offset::Offset(2); 2]);
        endpoint.stop().await;
        fixture.close().await;
    }
}
