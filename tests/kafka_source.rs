use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, Ordering},
};

use async_trait::async_trait;
use servicelib::{
    MessageContext, Payload,
    api::{KafkaSaslMechanism, KafkaSecurityProtocol},
    datasource::kafka::{
        ConsumerMessage, EndpointHandler, HandlerError, HandlerResult, RdkafkaKafkaDataSource,
        ResultCallback, ResultContext, StreamContext, make_rdkafka_kafka_endpoint_consumer,
    },
    operators::{InputStream, MapFunction},
    runtime::{
        collector::Collector,
        common::RuntimeStream,
        config::{
            CallSemantics, InputStreamConfig, KafkaDataConnectorConfig, KafkaEndpointConfig,
            RuntimeConfig, StreamConfig,
        },
        datasource::DataSource,
        environment::{Lifecycle, RuntimeEnvironment},
    },
};
use tokio::sync::{Mutex as AsyncMutex, oneshot};

struct Double;

#[async_trait]
impl MapFunction<u32, u32> for Double {
    async fn map(
        &self,
        context: MessageContext,
        _stream: &dyn RuntimeStream,
        value: &u32,
        out: &Collector<u32>,
    ) {
        out.emit(context, Payload::new(*value * 2)).await;
    }
}

#[tokio::test]
async fn disabled_kafka_datasource_keeps_endpoint_without_starting_transport() {
    let environment = RuntimeEnvironment::default();
    let connector_config = KafkaDataConnectorConfig {
        id: 4,
        name: "kafka".to_owned(),
        brokers: "localhost:9092".to_owned(),
        version: String::new(),
        dial_timeout: 0.0,
        use_partitioner: false,
        r#async: true,
        security_protocol: KafkaSecurityProtocol::PLAINTEXT,
        sasl_mechanism: KafkaSaslMechanism::PLAIN,
        username: String::new(),
        password: String::new(),
    };
    let input_config = InputStreamConfig {
        stream: StreamConfig::new(1, "orders input"),
        endpoint_id: 2,
    };
    let endpoint_config = KafkaEndpointConfig {
        enabled: false,
        id: 2,
        name: "orders topic".to_owned(),
        id_data_connector: 4,
        tracing_enabled: false,
        create_topic: false,
        topic: "orders".to_owned(),
        partitions: 1,
        consumer_group: "orders".to_owned(),
        replication_factor: 1,
    };
    environment.publish_runtime_config(Arc::new(
        RuntimeConfig::from_parts(
            CallSemantics::FunctionCall,
            [],
            [input_config.clone().into()],
            [],
            [connector_config.into()],
            [endpoint_config.into()],
            [],
        )
        .unwrap(),
    ));
    let input = InputStream::<u32, u32, String>::new(&input_config, environment.clone());
    let datasource = RdkafkaKafkaDataSource::from_input(&input).unwrap();
    let (finished, _wait_finished) = oneshot::channel();
    datasource
        .add_endpoint(
            input,
            Handler {
                finished: Mutex::new(Some(finished)),
            },
        )
        .unwrap();

    assert_eq!(datasource.id(), 4);
    assert_eq!(datasource.name(), "kafka");
    datasource.start(MessageContext::new()).await.unwrap();
    datasource.stop(MessageContext::new()).await.unwrap();
}

struct Handler {
    finished: Mutex<Option<oneshot::Sender<()>>>,
}

#[async_trait]
impl EndpointHandler<(), u32, u32, String> for Handler {
    fn concurrency(&self, _stream: &StreamContext<u32, u32, String>) -> usize {
        1
    }

    async fn begin_request(
        &self,
        context: MessageContext,
        _stream: StreamContext<u32, u32, String>,
    ) -> Result<(MessageContext, ()), HandlerError> {
        Ok((context, ()))
    }

    async fn consume_message(
        &self,
        context: MessageContext,
        stream: StreamContext<u32, u32, String>,
        _handler_state: Arc<AsyncMutex<()>>,
        message: ConsumerMessage,
        result_context: Arc<ResultContext<(), u32, u32, String>>,
    ) -> HandlerResult {
        message.mark_message("accepted");
        let done = Arc::clone(&result_context);
        let callback: ResultCallback<(), u32, u32, String> =
            Arc::new(move |_context, _stream, _state, value| {
                let done = Arc::clone(&done);
                let message = message.clone();
                Box::pin(async move {
                    assert_eq!(*value, 42);
                    message.commit().await.unwrap();
                    done.done();
                    true
                })
            });
        result_context.set_result_callback("42", callback);
        stream.collect(context, 21).await;
        Ok(())
    }

    async fn get_message_id(
        &self,
        _context: &MessageContext,
        _stream: &StreamContext<u32, u32, String>,
        _handler_state: Arc<AsyncMutex<()>>,
        value: &u32,
    ) -> String {
        value.to_string()
    }

    async fn end_request(
        &self,
        _context: MessageContext,
        _stream: StreamContext<u32, u32, String>,
        result: &HandlerResult,
        _handler_state: Arc<AsyncMutex<()>>,
    ) {
        assert!(result.is_ok());
        if let Some(finished) = self.finished.lock().unwrap().take() {
            let _ = finished.send(());
        }
    }
}

#[tokio::test]
async fn kafka_source_correlates_result_then_commits_and_ends() {
    let environment = RuntimeEnvironment::default();
    let input = InputStream::<u32, u32, String>::new(
        &InputStreamConfig {
            stream: StreamConfig::new(1, "orders input"),
            endpoint_id: 2,
        },
        environment.clone(),
    );
    let doubled = input
        .stream()
        .map(&(StreamConfig::new(3, "double").into()), Double)
        .unwrap();
    input.set_source(&doubled).unwrap();

    let (finished, wait_finished) = oneshot::channel();
    let endpoint = make_rdkafka_kafka_endpoint_consumer(
        input,
        KafkaEndpointConfig {
            enabled: true,
            id: 2,
            name: "orders topic".to_owned(),
            id_data_connector: 4,
            tracing_enabled: false,
            create_topic: false,
            topic: "orders".to_owned(),
            partitions: 1,
            consumer_group: "orders".to_owned(),
            replication_factor: 1,
        },
        KafkaDataConnectorConfig {
            id: 4,
            name: "kafka".to_owned(),
            brokers: "localhost:9092".to_owned(),
            version: String::new(),
            dial_timeout: 0.0,
            use_partitioner: false,
            r#async: true,
            security_protocol: KafkaSecurityProtocol::PLAINTEXT,
            sasl_mechanism: KafkaSaslMechanism::PLAIN,
            username: String::new(),
            password: String::new(),
        },
        Handler {
            finished: Mutex::new(Some(finished)),
        },
    )
    .unwrap();

    let committed = Arc::new(AtomicBool::new(false));
    let marked = Arc::new(Mutex::new(Vec::new()));
    let committed_copy = Arc::clone(&committed);
    let marked_copy = Arc::clone(&marked);
    endpoint
        .endpoint_request(
            MessageContext::new(),
            ConsumerMessage::new(
                None,
                b"21".to_vec(),
                "orders",
                0,
                10,
                move || {
                    let committed = Arc::clone(&committed_copy);
                    async move {
                        committed.store(true, Ordering::Release);
                        Ok(())
                    }
                },
                move |metadata| marked_copy.lock().unwrap().push(metadata),
            ),
        )
        .await;
    wait_finished.await.unwrap();

    assert!(committed.load(Ordering::Acquire));
    assert_eq!(&*marked.lock().unwrap(), &["accepted"]);
    let metrics = environment.metrics().render_prometheus();
    assert!(metrics.contains(
        r#"datasource_endpoint_messages_total{connector="kafka",endpoint="orders topic"} 1"#
    ));
}
