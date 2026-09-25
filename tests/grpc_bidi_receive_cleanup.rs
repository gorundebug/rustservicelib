use std::sync::{Arc, Mutex, Weak};
use std::time::Duration;

use async_trait::async_trait;
use servicelib::{
    MessageContext, Payload,
    api::GrpcMethodType,
    datasink::grpc::{
        BidiStreamingCall, BidiStreamingClientFunction, EndpointHandler, HandlerResult,
        ResultContext, Sender, StreamContext, make_grpc_bidi_streaming_endpoint_consumer,
    },
    operators::SinkStreamWithResult,
    runtime::{
        common::Consumer,
        config::{
            CallSemantics, GrpcDataConnectorConfig, GrpcEndpointConfig, InputStreamConfig,
            RuntimeConfig, SinkStreamConfig, StreamConfig,
        },
        environment::RuntimeEnvironment,
        stream::Stream,
    },
};
use tokio_util::sync::CancellationToken;

#[derive(Default)]
struct Probe {
    message_entered: CancellationToken,
    message_release: CancellationToken,
    block_message: bool,
    user_done: bool,
    receive: CancellationToken,
    end_entered: CancellationToken,
    end_release: CancellationToken,
    closed: CancellationToken,
    dropped: CancellationToken,
    call: Mutex<Option<Weak<Call>>>,
    end_error: Mutex<Option<String>>,
}

struct Call {
    probe: Arc<Probe>,
    mode: u8,
}

impl Drop for Call {
    fn drop(&mut self) {
        self.probe.dropped.cancel();
    }
}

#[async_trait]
impl BidiStreamingCall<u32, u32> for Call {
    async fn send(&self, _value: u32) -> HandlerResult {
        Ok(())
    }
    async fn recv(&self) -> HandlerResult<Option<u32>> {
        self.probe.receive.cancelled().await;
        match self.mode {
            1 => Err("peer disconnected".into()),
            2 => Ok(Some(7)),
            _ => Ok(None),
        }
    }
    async fn close_send(&self) -> HandlerResult {
        self.probe.closed.cancel();
        Ok(())
    }
}

struct Handler(Arc<Probe>);

#[async_trait]
impl EndpointHandler<(), u32, u32, u32, u32, String> for Handler {
    async fn begin_request(
        &self,
        ctx: MessageContext,
        _stream: StreamContext<u32, u32, String>,
    ) -> HandlerResult<(MessageContext, ())> {
        Ok((ctx, ()))
    }
    async fn consume_message(
        &self,
        ctx: MessageContext,
        _stream: StreamContext<u32, u32, String>,
        _state: Arc<tokio::sync::Mutex<()>>,
        _value: Payload<u32>,
        sender: &dyn Sender<u32>,
        result: ResultContext,
    ) -> HandlerResult {
        sender.send(ctx, 1).await?;
        if self.0.user_done {
            result.done();
        }
        self.0.message_entered.cancel();
        if self.0.block_message {
            self.0.message_release.cancelled().await;
        }
        Ok(())
    }
    async fn handle_response(
        &self,
        _ctx: MessageContext,
        _stream: StreamContext<u32, u32, String>,
        _state: Arc<tokio::sync::Mutex<()>>,
        _response: u32,
    ) -> HandlerResult {
        Err("response rejected".into())
    }
    async fn end_request(
        &self,
        _ctx: MessageContext,
        _stream: StreamContext<u32, u32, String>,
        result: &HandlerResult,
        _state: Arc<tokio::sync::Mutex<()>>,
    ) {
        *self.0.end_error.lock().unwrap() = result.as_ref().err().map(ToString::to_string);
        self.0.end_entered.cancel();
        self.0.end_release.cancelled().await;
    }
}

async fn check_cleanup(mode: u8) {
    let probe = Arc::new(Probe {
        block_message: mode >= 3,
        user_done: mode == 4,
        ..Probe::default()
    });
    let input = InputStreamConfig {
        stream: StreamConfig::new(1, "input"),
        endpoint_id: 4,
    };
    let sink_config = SinkStreamConfig {
        stream: StreamConfig::new(2, "sink"),
        endpoint_id: 4,
    };
    let connector = GrpcDataConnectorConfig {
        id: 10,
        name: "grpc".to_owned(),
        address: "127.0.0.1:50051".to_owned(),
        connections_count: 1,
    };
    let endpoint = GrpcEndpointConfig {
        id: 4,
        name: "bidi".to_owned(),
        id_data_connector: 10,
        tracing_enabled: false,
        grpc_method_type: GrpcMethodType::BidirectionalStreaming,
    };
    let env = RuntimeEnvironment::default();
    env.publish_runtime_config(Arc::new(
        RuntimeConfig::from_parts(
            CallSemantics::FunctionCall,
            [],
            [input.clone().into(), sink_config.clone().into()],
            [],
            [connector.into()],
            [endpoint.into()],
            [],
        )
        .unwrap(),
    ));
    let source = Stream::<u32>::new(&input.stream, env);
    let sink = SinkStreamWithResult::<u32, u32, String>::make(&sink_config, &source).unwrap();
    let factory_probe = probe.clone();
    let factory: BidiStreamingClientFunction<u32, u32> = Arc::new(move |_ctx| {
        let probe = factory_probe.clone();
        Box::pin(async move {
            let call = Arc::new(Call {
                probe: probe.clone(),
                mode,
            });
            *probe.call.lock().unwrap() = Some(Arc::downgrade(&call));
            Ok(call as Arc<dyn BidiStreamingCall<u32, u32>>)
        })
    });
    let consumer =
        make_grpc_bidi_streaming_endpoint_consumer(&sink, Handler(probe.clone()), factory).unwrap();
    let context = MessageContext::new().with_stream_id("peer-finished");
    let consume = tokio::spawn({
        let consumer = consumer.clone();
        let context = context.clone();
        async move { consumer.consume(context, Payload::new(1)).await }
    });
    tokio::time::timeout(Duration::from_secs(2), probe.message_entered.cancelled())
        .await
        .unwrap();
    if probe.user_done {
        tokio::time::timeout(Duration::from_secs(2), probe.closed.cancelled())
            .await
            .unwrap();
    }
    probe.receive.cancel();
    let end_overtook_message = if probe.block_message {
        tokio::time::timeout(Duration::from_millis(25), probe.end_entered.cancelled())
            .await
            .is_ok()
    } else {
        false
    };
    probe.message_release.cancel();
    tokio::time::timeout(Duration::from_secs(2), consume)
        .await
        .unwrap()
        .unwrap();
    tokio::time::timeout(Duration::from_secs(2), probe.end_entered.cancelled())
        .await
        .unwrap();
    if !probe.user_done {
        assert!(
            !probe.closed.is_cancelled(),
            "cleanup must not precede active EndRequest"
        );
    }
    assert!(!probe.dropped.is_cancelled());
    probe.end_release.cancel();

    let released = tokio::time::timeout(Duration::from_secs(1), probe.dropped.cancelled())
        .await
        .is_ok();
    // Clean up even on the old implementation, before making assertions.
    assert!(
        !context.is_cancelled(),
        "internal completion cancelled caller context"
    );
    context.cancel();
    tokio::time::timeout(Duration::from_secs(2), probe.dropped.cancelled())
        .await
        .unwrap();
    assert!(probe.closed.is_cancelled());
    assert!(
        probe
            .call
            .lock()
            .unwrap()
            .as_ref()
            .unwrap()
            .upgrade()
            .is_none()
    );
    let expected = match mode {
        1 => Some("peer disconnected"),
        2 => Some("response rejected"),
        _ => None,
    };
    assert_eq!(probe.end_error.lock().unwrap().as_deref(), expected);
    assert!(
        !end_overtook_message,
        "EndRequest overtook active ConsumeMessage"
    );
    assert!(
        released,
        "completed bidi RPC retained its transport until parent cancellation"
    );
}

#[tokio::test]
async fn peer_eof_releases_bidi_transport_without_done() {
    check_cleanup(0).await;
}

#[tokio::test]
async fn receive_error_releases_bidi_transport_without_done() {
    check_cleanup(1).await;
}

#[tokio::test]
async fn handler_error_releases_bidi_transport_without_done() {
    check_cleanup(2).await;
}

#[tokio::test]
async fn peer_eof_waits_for_active_message_before_finalization() {
    check_cleanup(3).await;
}

#[tokio::test]
async fn user_done_closes_send_before_active_message_returns() {
    check_cleanup(4).await;
}
