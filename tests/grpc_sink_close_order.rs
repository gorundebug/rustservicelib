use async_trait::async_trait;
use servicelib::{
    MessageContext, Payload, Stream,
    api::GrpcMethodType,
    datasink::grpc::{
        ClientStreamingCall, ClientStreamingClientFunction, EndpointHandler, HandlerResult,
        ResultContext, Sender, StreamContext, make_grpc_client_streaming_endpoint_consumer,
    },
    runtime::{
        config::{
            CallSemantics, GrpcDataConnectorConfig, GrpcEndpointConfig, RuntimeConfig,
            SinkStreamConfig, StreamConfig,
        },
        environment::RuntimeEnvironment,
    },
};
use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::sync::{Mutex, Semaphore};

struct Gates {
    after_done: Semaphore,
    release_handler: Semaphore,
    close_started: Semaphore,
    ended: Semaphore,
    responses: AtomicUsize,
}

struct Call(Arc<Gates>);
#[async_trait]
impl ClientStreamingCall<u32, u32> for Call {
    async fn send(&self, _: u32) -> HandlerResult {
        Ok(())
    }
    async fn close_and_recv(&self) -> HandlerResult<u32> {
        self.0.close_started.add_permits(1);
        Ok(10)
    }
}

struct Handler(Arc<Gates>);
#[async_trait]
impl EndpointHandler<(), u32, u32, u32, u32, String> for Handler {
    async fn begin_request(
        &self,
        context: MessageContext,
        _: StreamContext<u32, u32, String>,
    ) -> HandlerResult<(MessageContext, ())> {
        Ok((context, ()))
    }

    async fn consume_message(
        &self,
        context: MessageContext,
        _: StreamContext<u32, u32, String>,
        _: Arc<Mutex<()>>,
        value: Payload<u32>,
        sender: &dyn Sender<u32>,
        result: ResultContext,
    ) -> HandlerResult {
        sender.send(context, *value).await?;
        result.done();
        self.0.after_done.add_permits(1);
        self.0.release_handler.acquire().await.unwrap().forget();
        Ok(())
    }

    async fn handle_response(
        &self,
        _: MessageContext,
        _: StreamContext<u32, u32, String>,
        _: Arc<Mutex<()>>,
        _: u32,
    ) -> HandlerResult {
        self.0.responses.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    async fn end_request(
        &self,
        _: MessageContext,
        _: StreamContext<u32, u32, String>,
        result: &HandlerResult,
        _: Arc<Mutex<()>>,
    ) {
        assert!(result.is_ok());
        self.0.ended.add_permits(1);
    }
}

#[tokio::test]
async fn close_starts_before_active_handler_returns_but_response_waits() {
    let gates = Arc::new(Gates {
        after_done: Semaphore::new(0),
        release_handler: Semaphore::new(0),
        close_started: Semaphore::new(0),
        ended: Semaphore::new(0),
        responses: AtomicUsize::new(0),
    });
    let environment = RuntimeEnvironment::default();
    let sink_config = SinkStreamConfig {
        stream: StreamConfig::new(2, "sink"),
        endpoint_id: 3,
    };
    environment.publish_runtime_config(Arc::new(
        RuntimeConfig::from_parts(
            CallSemantics::FunctionCall,
            [],
            [
                StreamConfig::new(1, "source").into(),
                sink_config.clone().into(),
            ],
            [],
            [GrpcDataConnectorConfig {
                id: 4,
                name: "remote".to_owned(),
                address: "http://remote".to_owned(),
                connections_count: 1,
            }
            .into()],
            [GrpcEndpointConfig {
                id: 3,
                name: "request".to_owned(),
                id_data_connector: 4,
                tracing_enabled: false,
                grpc_method_type: GrpcMethodType::ClientStreaming,
            }
            .into()],
            [],
        )
        .unwrap(),
    ));
    let source = Stream::new(&StreamConfig::new(1, "source"), environment);
    let sink = source
        .sink_with_result::<u32, String>(&sink_config)
        .unwrap();
    let client: ClientStreamingClientFunction<u32, u32> = Arc::new({
        let gates = gates.clone();
        move |_| {
            let call = Arc::new(Call(gates.clone())) as Arc<dyn ClientStreamingCall<u32, u32>>;
            Box::pin(async move { Ok(call) })
        }
    });
    make_grpc_client_streaming_endpoint_consumer(&sink, Handler(gates.clone()), client).unwrap();
    let call = tokio::spawn(async move {
        source
            .emit(
                MessageContext::new().with_stream_id("close-order"),
                Payload::new(1_u32),
            )
            .await;
    });
    tokio::time::timeout(Duration::from_secs(2), gates.after_done.acquire())
        .await
        .unwrap()
        .unwrap()
        .forget();
    let started = tokio::time::timeout(Duration::from_secs(2), gates.close_started.acquire()).await;
    let closed_before_release = match started {
        Ok(permit) => {
            permit.unwrap().forget();
            true
        }
        Err(_) => false,
    };
    let responses_before_release = gates.responses.load(Ordering::SeqCst);
    let end_before_release = gates.ended.available_permits();
    // Always release admitted work, including when checking the known failure.
    gates.release_handler.add_permits(1);
    tokio::time::timeout(Duration::from_secs(2), call)
        .await
        .unwrap()
        .unwrap();
    tokio::time::timeout(Duration::from_secs(2), gates.ended.acquire())
        .await
        .unwrap()
        .unwrap()
        .forget();
    assert!(
        closed_before_release,
        "CloseAndRecv is delayed by active ConsumeMessage"
    );
    assert_eq!(
        responses_before_release, 0,
        "HandleResponse must wait for ConsumeMessage"
    );
    assert_eq!(end_before_release, 0);
    assert_eq!(gates.responses.load(Ordering::SeqCst), 1);
}
