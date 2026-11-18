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

struct RestoreProbe {
    ticks: std::sync::atomic::AtomicUsize,
    calls: std::sync::atomic::AtomicUsize,
    progressed: std::sync::atomic::AtomicUsize,
    entered: std::sync::atomic::AtomicBool,
    cancel_wait: bool,
}
impl RestoreProbe {
    fn new(cancel_wait: bool) -> Arc<Self> { Arc::new(Self {
        ticks: 0.into(), calls: 0.into(), progressed: 0.into(), entered: false.into(), cancel_wait,
    }) }
}
struct Handler {
    failure: Option<bool>,
    ordinary: bool,
    offsets: Arc<Mutex<CheckpointOffsets>>,
    probe: Option<Arc<RestoreProbe>>,
    messages: mpsc::UnboundedSender<ConsumerMessage>,
    errors: mpsc::UnboundedSender<String>,
}
#[async_trait]
impl EndpointHandler<(), String, String, String> for Handler {
    fn restore_checkpoint(&self, context: &MessageContext, _: &StreamContext<String, String, String>)
        -> HandlerResult<Option<CheckpointOffsets>> {
        if let Some(probe)=&self.probe {
            if probe.calls.fetch_add(1,std::sync::atomic::Ordering::SeqCst)==1 {
                probe.entered.store(true,std::sync::atomic::Ordering::SeqCst);
                let before=probe.ticks.load(std::sync::atomic::Ordering::SeqCst);
                if probe.cancel_wait {
                    while !context.is_cancelled() { std::thread::sleep(Duration::from_millis(5)); }
                } else { std::thread::sleep(Duration::from_millis(200)); }
                probe.progressed.store(probe.ticks.load(std::sync::atomic::Ordering::SeqCst)-before,std::sync::atomic::Ordering::SeqCst);
            }
        }
        if self.ordinary { Ok(None) } else { Ok(Some(self.offsets.lock().unwrap().clone())) }
    }
    fn concurrency(&self, _: &StreamContext<String, String, String>) -> usize { 2 }
    async fn begin_request(&self, context: MessageContext, _: StreamContext<String, String, String>)
        -> Result<(MessageContext, ()), HandlerError> { Ok((context.child(), ())) }
    async fn consume_message(&self, context: MessageContext, stream: StreamContext<String, String, String>,
        _: Arc<AsyncMutex<()>>, message: ConsumerMessage, result: Arc<ResultContext<(), String, String, String>>)
        -> HandlerResult {
        if let Some(asynchronous) = self.failure {
            self.messages.send(message.clone()).unwrap();
            if message.partition == 0 {
                if asynchronous {
                    tokio::spawn(async move { tokio::task::yield_now().await; context.cancel(); });
                    return Ok(());
                }
                return Err("intentional processing failure".into());
            }
            message.commit().await?;
            result.done();
            return Ok(());
        }
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
        Self::start_probe(brokers,topic,offsets,None).await
    }
    async fn start_probe(brokers: &str, topic: &str, offsets: CheckpointOffsets, probe: Option<Arc<RestoreProbe>>) -> Self {
        Self::start_options(brokers, topic, offsets, probe, None, false).await
    }
    async fn start_options(brokers: &str, topic: &str, offsets: CheckpointOffsets, probe: Option<Arc<RestoreProbe>>, failure: Option<bool>, ordinary: bool) -> Self {
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
        let result_stream = input.stream().map(&(StreamConfig::new(3, "result").into()), Identity).unwrap();
        if failure.is_some() { input.set_source(&result_stream).unwrap(); }
        let datasource = RdkafkaKafkaDataSource::from_input(&input).unwrap();
        let offsets = Arc::new(Mutex::new(offsets));
        let (messages, receiver) = mpsc::unbounded_channel();
        let (errors, error_receiver) = mpsc::unbounded_channel();
        datasource.add_endpoint(input, Handler { offsets: offsets.clone(), probe, messages, errors, failure, ordinary }).unwrap();
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

struct ReceiptHold(std::path::PathBuf);
impl Drop for ReceiptHold { fn drop(&mut self) { let _=std::fs::remove_file(self.0.join("hold-commit")); } }

// The official broker is reached through the test-owned OffsetCommit receipt
// proxy. No request is fabricated and a dropped wait never means withdrawal.
#[tokio::test(flavor = "current_thread")]
#[ignore = "requires tests/kafka_commit_gate.py isolated official broker"]
async fn native_commit_receipt_loss_keeps_worker_live_then_rebalances_safely() {
    let directory = std::path::PathBuf::from(std::env::var("SA_KAFKA_FAULT_DIR").expect("fault supervisor required"));
    let fixture = Fixture::new().await;
    let mut first = Endpoint::start(&fixture.brokers, &fixture.topic, BTreeMap::from([(0,4),(1,4)])).await;
    let old = first.next().await;
    let _other = first.next().await;
    let ticks = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let counter = ticks.clone();
    let ticker = tokio::spawn(async move {
        loop { counter.fetch_add(1,std::sync::atomic::Ordering::SeqCst);tokio::time::sleep(Duration::from_millis(5)).await; }
    });
    let _ = std::fs::remove_file(directory.join("commit-seen"));
    let _hold=ReceiptHold(directory.clone());
    std::fs::write(directory.join("hold-commit"), b"hold").unwrap();
    let pending_message = old.clone();
    let mut pending = tokio::spawn(async move { pending_message.commit().await });
    tokio::time::timeout(Duration::from_secs(5), async {
        while !directory.join("commit-seen").exists() {tokio::time::sleep(Duration::from_millis(5)).await;}
    }).await.unwrap();
    let before=ticks.load(std::sync::atomic::Ordering::SeqCst);
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(ticks.load(std::sync::atomic::Ordering::SeqCst)>=before+5,"native commit blocked Tokio worker");
    assert!(!pending.is_finished(),"commit falsely completed without its own native receipt");
    // Native librdkafka may defer reassignment until its outstanding commit
    // finishes. Release the response before requiring group convergence; do not
    // promise that SDK cancellation withdraws an admitted broker request.
    std::fs::remove_file(directory.join("hold-commit")).unwrap();
    tokio::time::timeout(Duration::from_secs(2),&mut pending).await.unwrap().unwrap().unwrap();
    let mut second = Endpoint::start(&fixture.brokers,&fixture.topic,BTreeMap::from([(0,4),(1,4)])).await;
    let (one,two)=tokio::join!(first.next(),second.next());
    assert_ne!(one.partition,two.partition);
    assert!(old.commit().await.unwrap_err().to_string().contains("revoked"));
    old.mark_message("must-not-store-after-revoke");
    // New generation commits require their own receipts.
    one.commit().await.unwrap();two.commit().await.unwrap();
    assert_eq!(fixture.committed(),vec![Offset::Offset(5);2]);
    ticker.abort();
    second.stop().await;first.stop().await;fixture.close().await;
}

#[tokio::test(flavor = "current_thread")]
#[ignore = "requires tests/kafka_commit_gate.py isolated official broker"]
async fn native_abandoned_commit_wait_does_not_hold_assignment_lock() {
    let directory=std::path::PathBuf::from(std::env::var("SA_KAFKA_FAULT_DIR").expect("fault supervisor required"));
    let fixture=Fixture::new().await;
    let mut endpoint=Endpoint::start(&fixture.brokers,&fixture.topic,BTreeMap::from([(0,4),(1,4)])).await;
    let message=endpoint.next().await;let other=endpoint.next().await;
    let _=std::fs::remove_file(directory.join("commit-seen"));
    let _hold=ReceiptHold(directory.clone());
    std::fs::write(directory.join("hold-commit"),b"hold").unwrap();
    assert!(tokio::time::timeout(Duration::from_millis(150),message.commit()).await.is_err());
    assert!(directory.join("commit-seen").exists());
    // This synchronous public method used to contend with a mutex held through
    // the broker round trip. On a current-thread runtime that stalled timers.
    let began=std::time::Instant::now();other.mark_message("accepted");
    assert!(began.elapsed()<Duration::from_millis(100));
    std::fs::remove_file(directory.join("hold-commit")).unwrap();
    other.commit().await.unwrap();
    endpoint.stop().await;fixture.close().await;
}

#[tokio::test(flavor = "current_thread")]
#[ignore = "requires tests/kafka_commit_gate.py isolated official broker"]
async fn native_checkpoint_callback_keeps_tokio_worker_responsive() {
    let fixture=Fixture::new().await;
    let probe=RestoreProbe::new(false);
    let counter=probe.clone();
    let ticker=tokio::spawn(async move {loop {counter.ticks.fetch_add(1,std::sync::atomic::Ordering::SeqCst);tokio::time::sleep(Duration::from_millis(5)).await;}});
    let mut endpoint=Endpoint::start_probe(&fixture.brokers,&fixture.topic,BTreeMap::from([(0,4),(1,4)]),Some(probe.clone())).await;
    let first=endpoint.next().await;
    assert!(probe.entered.load(std::sync::atomic::Ordering::SeqCst));
    assert!(probe.progressed.load(std::sync::atomic::Ordering::SeqCst)>=10,"synchronous assignment provider ran on Tokio worker");
    first.commit().await.unwrap();
    endpoint.stop().await;ticker.abort();fixture.close().await;
}

#[tokio::test(flavor = "current_thread")]
#[ignore = "requires tests/kafka_commit_gate.py isolated official broker"]
async fn native_stop_cancels_checkpoint_restore_without_blocking_worker() {
    let fixture=Fixture::new().await;
    let probe=RestoreProbe::new(true);
    let counter=probe.clone();
    let ticker=tokio::spawn(async move {loop {counter.ticks.fetch_add(1,std::sync::atomic::Ordering::SeqCst);tokio::time::sleep(Duration::from_millis(5)).await;}});
    let mut endpoint=Endpoint::start_probe(&fixture.brokers,&fixture.topic,BTreeMap::from([(0,4),(1,4)]),Some(probe.clone())).await;
    tokio::time::timeout(Duration::from_secs(15),async {
        while !probe.entered.load(std::sync::atomic::Ordering::SeqCst) {tokio::time::sleep(Duration::from_millis(5)).await;}
    }).await.unwrap();
    tokio::time::sleep(Duration::from_millis(75)).await;
    let bounded=endpoint.datasource.stop(MessageContext::with_timeout(Duration::from_millis(100)));
    let _=tokio::time::timeout(Duration::from_millis(500),bounded).await.expect("Stop blocked Tokio worker");
    endpoint.stop().await;
    assert!(probe.progressed.load(std::sync::atomic::Ordering::SeqCst)>=5);
    assert!(endpoint.messages.try_recv().is_err());
    assert_eq!(fixture.committed(),vec![Offset::Offset(2);2]);
    ticker.abort();fixture.close().await;
}


struct Identity;
impl servicelib::operators::MapFunction<String, String> for Identity {
    async fn map(&self, context: MessageContext, _: &dyn servicelib::runtime::common::RuntimeStream, value: &String, out: &impl servicelib::runtime::collector::Collect<String>) {
        out.emit(context, servicelib::Payload::new(value.clone())).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires explicit disposable Kafka broker"]
async fn native_failed_partition_cannot_be_overtaken_and_other_partition_commits() {
    for ordinary in [false, true] {
        for asynchronous in [false, true] {
            let fixture = Fixture::new().await;
            let mut endpoint = Endpoint::start_options(&fixture.brokers, &fixture.topic,
                [(0, 2), (1, 2)].into(), None, Some(asynchronous), ordinary).await;
            let mut seen = Vec::new();
            let mut retained = None;
            for _ in 0..4 {
                let message = endpoint.next().await;
                seen.push((message.partition, message.offset));
                if message.partition == 0 { retained = Some(message); }
            }
            seen.sort();
            assert_eq!(seen, vec![(0, 2), (1, 2), (1, 3), (1, 4)], "ordinary={ordinary} asynchronous={asynchronous}");
            let error = tokio::time::timeout(Duration::from_secs(5), endpoint.errors.recv()).await.unwrap().unwrap();
            assert!(error.contains(if asynchronous { "cancelled" } else { "intentional" }), "{error}");
            assert!(tokio::time::timeout(Duration::from_millis(300), endpoint.messages.recv()).await.is_err(), "failed lane advanced");
            let retained = retained.unwrap();
            retained.mark_message("must-not-cover-failure");
            assert!(retained.commit().await.is_err(), "failed generation accepted retained commit");
            endpoint.stop().await;
            assert_eq!(fixture.committed(), vec![Offset::Offset(2), Offset::Offset(5)]);
            assert!(endpoint.errors.try_recv().is_err(), "duplicate finalization or transport failure");
            fixture.close().await;
        }
    }
}
