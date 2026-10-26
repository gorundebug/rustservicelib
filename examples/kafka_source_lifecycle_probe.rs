//! Explicit live-broker acceptance probe (not run by the ordinary unit suite).
//! KAFKA_BROKERS=broker:9092 cargo run --example kafka_source_lifecycle_probe
use std::{collections::HashMap, error::Error, sync::{Arc, Mutex, atomic::{AtomicUsize, Ordering}}, time::{Duration, SystemTime, UNIX_EPOCH}};

use async_trait::async_trait;
use rdkafka::{ClientConfig, Offset, TopicPartitionList, consumer::{BaseConsumer, Consumer}, producer::{FutureProducer, FutureRecord}};
use tokio::sync::{Mutex as AsyncMutex, Notify};
use servicelib::{
    MessageContext,
    api::{KafkaSaslMechanism, KafkaSecurityProtocol},
    datasource::kafka::{ConsumerMessage, EndpointHandler, HandlerError, HandlerResult,
        RdkafkaKafkaDataSource, ResultContext, StreamContext},
    operators::InputStream,
    runtime::{config::{CallSemantics, InputStreamConfig, KafkaDataConnectorConfig,
        KafkaEndpointConfig, RuntimeConfig, StreamConfig}, environment::{Lifecycle, RuntimeEnvironment, RuntimeError}},
};

type ProbeResult<T = ()> = Result<T, Box<dyn Error + Send + Sync>>;

#[derive(Default)]
struct Observations {
    entered: Notify,
    cancelled: Notify,
    release: Notify,
    context: Mutex<Option<MessageContext>>,
    ended: AtomicUsize,
}

struct Handler(Arc<Observations>);

#[async_trait]
impl EndpointHandler<(), u32, u32, String> for Handler {
    fn concurrency(&self, _: &StreamContext<u32, u32, String>) -> usize { 1 }
    async fn begin_request(&self, context: MessageContext, _: StreamContext<u32, u32, String>)
        -> Result<(MessageContext, ()), HandlerError> {
        assert_eq!(context.metadata().get("owner").map(String::as_str), Some("live-kafka-owner"));
        assert!(context.remaining().is_some_and(|left| left > Duration::ZERO && left <= Duration::from_secs(60)));
        *self.0.context.lock().unwrap() = Some(context.clone());
        Ok((context, ()))
    }
    async fn consume_message(&self, context: MessageContext, _: StreamContext<u32, u32, String>,
        _: Arc<AsyncMutex<()>>, message: ConsumerMessage, _: Arc<ResultContext<(), u32, u32, String>>)
        -> HandlerResult {
        assert_eq!(message.value, b"caller-owned-context");
        self.0.entered.notify_one();
        context.cancelled().await;
        self.0.cancelled.notify_one();
        // A real accepted callback may need time to finish even after cancellation.
        self.0.release.notified().await;
        message.commit().await?;
        Ok(())
    }
    async fn get_message_id(&self, _: &MessageContext, _: &StreamContext<u32, u32, String>,
        _: Arc<AsyncMutex<()>>, _: &u32) -> String { String::new() }
    async fn end_request(&self, _: MessageContext, _: StreamContext<u32, u32, String>,
        result: &HandlerResult, _: Arc<AsyncMutex<()>>) {
        assert!(result.is_ok(), "handler failed: {result:?}");
        self.0.ended.fetch_add(1, Ordering::SeqCst);
    }
}

async fn probe(brokers: &str, suffix: &str, mode: &str) -> ProbeResult {
    let topic = format!("ctx-lifecycle-{suffix}-{mode}");
    let group = format!("ctx-lifecycle-group-{suffix}-{mode}");
    let environment = RuntimeEnvironment::default();
    let input_config = InputStreamConfig { stream: StreamConfig::new(1, "input"), endpoint_id: 2 };
    let endpoint_config = KafkaEndpointConfig {
        enabled: true, id: 2, name: topic.clone(), id_data_connector: 4,
        tracing_enabled: false, create_topic: true, topic: topic.clone(),
        partitions: 1, consumer_group: group.clone(), replication_factor: 1,
    };
    let connector_config = KafkaDataConnectorConfig {
        id: 4, name: "live-kafka".into(), brokers: brokers.into(), version: String::new(),
        dial_timeout: 5000.0, use_partitioner: false, r#async: true,
        security_protocol: KafkaSecurityProtocol::PLAINTEXT, sasl_mechanism: KafkaSaslMechanism::PLAIN,
        username: String::new(), password: String::new(),
    };
    environment.publish_runtime_config(Arc::new(RuntimeConfig::from_parts(
        CallSemantics::FunctionCall, [], [input_config.clone().into()], [],
        [connector_config.into()], [endpoint_config.into()], [],
    )?));
    let input = InputStream::<u32, u32, String>::new(&input_config, environment);
    let source = RdkafkaKafkaDataSource::from_input(&input)?;
    let observed = Arc::new(Observations::default());
    let _endpoint = source.add_endpoint(input, Handler(Arc::clone(&observed)))?;
    let owner = MessageContext::with_timeout(Duration::from_secs(60))
        .with_metadata(HashMap::from([("owner".into(), "live-kafka-owner".into())]));
    tokio::time::timeout(Duration::from_secs(30), source.start(owner.clone())).await??;
    let producer: FutureProducer = ClientConfig::new().set("bootstrap.servers", brokers)
        .set("message.timeout.ms", "10000").create()?;
    producer.send(FutureRecord::to(&topic).partition(0).payload("caller-owned-context").key("key"),
        Duration::from_secs(5)).await.map_err(|(error, _)| error)?;
    tokio::time::timeout(Duration::from_secs(15), observed.entered.notified()).await?;

    if mode == "caller" {
        assert!(tokio::time::timeout(Duration::from_millis(25), source.stop(MessageContext::new())).await.is_err());
    } else {
        if mode == "parent" { owner.cancel(); }
        let stop_context = MessageContext::with_timeout(Duration::from_millis(25));
        assert!(matches!(source.stop(stop_context).await, Err(RuntimeError::ContextCancelled)));
    }
    tokio::time::timeout(Duration::from_secs(1), observed.cancelled.notified()).await?;
    assert_eq!(owner.is_cancelled(), mode == "parent");
    assert!(observed.context.lock().unwrap().as_ref().unwrap().is_cancelled());
    assert_eq!(observed.ended.load(Ordering::SeqCst), 0);
    let cancelled = MessageContext::new(); cancelled.cancel();
    assert!(matches!(source.stop(cancelled).await, Err(RuntimeError::ContextCancelled)));
    observed.release.notify_one();
    tokio::time::timeout(Duration::from_secs(15), source.stop(MessageContext::new())).await??;
    source.stop(MessageContext::new()).await?;
    assert_eq!(observed.ended.load(Ordering::SeqCst), 1);
    let verifier: BaseConsumer = ClientConfig::new().set("bootstrap.servers", brokers)
        .set("group.id", &group).set("enable.auto.commit", "false").create()?;
    let mut partitions = TopicPartitionList::new();
    partitions.add_partition(&topic, 0);
    let committed = verifier.committed_offsets(partitions, Duration::from_secs(5))?;
    assert_eq!(committed.find_partition(&topic, 0).unwrap().offset(), Offset::Offset(1));
    println!("PASS {mode}: real topic/start/receive -> inherited context -> retained callback on interrupted stop -> retry -> one finalizer -> committed offset 1");
    Ok(())
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> ProbeResult {
    let brokers = std::env::var("KAFKA_BROKERS")?;
    let suffix = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos().to_string();
    for mode in ["deadline", "parent", "caller"] {
        probe(&brokers, &suffix, mode).await?;
    }
    Ok(())
}
