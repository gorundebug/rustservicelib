use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, Ordering},
};

use async_trait::async_trait;
use servicelib::{
    MessageContext, Payload,
    datasource::kafka::{
        ConsumerMessage, EndpointHandler, HandlerError, HandlerResult, RdkafkaKafkaDataSource,
        ResultCallback, ResultContext, StreamContext, make_rdkafka_kafka_endpoint_consumer,
    },
    operators::{InputStream, MapFunction},
    runtime::{
        common::RuntimeStream,
        config::{InputStreamConfig, KafkaDataConnectorConfig, KafkaEndpointConfig, StreamConfig},
        datasource::DataSource,
        environment::RuntimeEnvironment,
        stream::Stream,
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
        value: Payload<u32>,
        out: &Stream<u32>,
    ) {
        out.emit(context, Payload::new(*value * 2)).await;
    }
}

#[tokio::test]
async fn kafka_datasource_owns_typed_endpoints_like_go_connector() {
    let environment = RuntimeEnvironment::default();
    let connector_config = KafkaDataConnectorConfig {
        id: 4,
        name: "kafka".to_owned(),
        brokers: "localhost:9092".to_owned(),
        version: String::new(),
        dial_timeout: 0.0,
        use_partitioner: false,
        r#async: true,
    };
    let datasource = RdkafkaKafkaDataSource::new(connector_config).unwrap();
    let input = InputStream::<u32, u32, String>::new(
        &InputStreamConfig {
            stream: StreamConfig::new(1, "orders input"),
            endpoint_id: 2,
        },
        environment,
    );
    let (finished, _wait_finished) = oneshot::channel();
    datasource
        .add_endpoint(
            input,
            KafkaEndpointConfig {
                id: 2,
                name: "orders topic".to_owned(),
                id_data_connector: 4,
                create_topic: false,
                topic: "orders".to_owned(),
                partitions: 1,
                consumer_group: "orders".to_owned(),
                replication_factor: 1,
            },
            Handler {
                finished: Mutex::new(Some(finished)),
            },
        )
        .unwrap();

    assert_eq!(datasource.id(), 4);
    assert_eq!(datasource.name(), "kafka");
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
            id: 2,
            name: "orders topic".to_owned(),
            id_data_connector: 4,
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
        r#"datasource_endpoint_messages_total{connector="kafka",endpoint="orders topic",protocol="kafka"} 1"#
    ));
}
