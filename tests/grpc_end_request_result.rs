use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use futures::stream;
use servicelib::{
    MessageContext,
    api::GrpcMethodType,
    datasource::grpc::{
        EndpointHandler, HandlerResult, ResultContext, Sender, StreamContext,
        make_grpc_bidi_streaming_endpoint_consumer, make_grpc_client_streaming_endpoint_consumer,
        make_grpc_no_streaming_endpoint_consumer, make_grpc_server_streaming_endpoint_consumer,
    },
    operators::InputStream,
    runtime::{
        config::{
            CallSemantics, GrpcDataConnectorConfig, GrpcEndpointConfig, InputStreamConfig,
            RuntimeConfig, StreamConfig,
        },
        environment::RuntimeEnvironment,
        stream::Stream,
    },
};
use tokio::sync::Mutex as AsyncMutex;

struct Handler {
    recover: bool,
    observed: Arc<Mutex<Vec<String>>>,
}

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
        _: MessageContext,
        _: StreamContext<u32, u32, String>,
        _: Arc<AsyncMutex<()>>,
        _: u32,
        _: Arc<ResultContext<(), u32, u32, u32, String>>,
        _: Arc<dyn Sender<u32>>,
    ) -> HandlerResult<MessageContext> {
        Err("original consume error".into())
    }

    async fn get_message_id(
        &self,
        _: &MessageContext,
        _: &StreamContext<u32, u32, String>,
        _: Arc<AsyncMutex<()>>,
        _: &u32,
    ) -> String {
        "reply".into()
    }

    async fn eof(
        &self,
        _: MessageContext,
        _: StreamContext<u32, u32, String>,
        _: Arc<AsyncMutex<()>>,
    ) {
    }

    async fn end_request(
        &self,
        _: MessageContext,
        _: StreamContext<u32, u32, String>,
        result: &HandlerResult,
        _: Arc<AsyncMutex<()>>,
    ) -> HandlerResult {
        self.observed
            .lock()
            .unwrap()
            .push(result.as_ref().unwrap_err().to_string());
        if self.recover {
            Ok(())
        } else {
            Err("replacement end error".into())
        }
    }
}

struct Discard;
#[async_trait]
impl Sender<u32> for Discard {
    async fn send(&self, _: MessageContext, _: u32) -> HandlerResult {
        Ok(())
    }
}

async fn check(mode: GrpcMethodType, has_result: bool, recover: bool) {
    let environment = RuntimeEnvironment::default();
    let config = InputStreamConfig {
        stream: StreamConfig::new(1, "input"),
        endpoint_id: 4,
    };
    environment.publish_runtime_config(Arc::new(
        RuntimeConfig::from_parts(
            CallSemantics::FunctionCall,
            [],
            [config.clone().into()],
            [],
            [GrpcDataConnectorConfig {
                id: 10,
                name: "grpc".into(),
                address: "http://localhost".into(),
                connections_count: 1,
            }
            .into()],
            [GrpcEndpointConfig {
                id: 4,
                name: "request".into(),
                id_data_connector: 10,
                tracing_enabled: false,
                grpc_method_type: mode,
            }
            .into()],
            [],
        )
        .unwrap(),
    ));
    let input = InputStream::<u32, u32, String>::new(&config, environment.clone());
    if has_result {
        input
            .set_source(&Stream::<u32>::new(
                &StreamConfig::new(2, "result"),
                environment,
            ))
            .unwrap();
    }
    let observed = Arc::new(Mutex::new(Vec::new()));
    let handler = Handler {
        recover,
        observed: observed.clone(),
    };
    let context = MessageContext::new().with_stream_id("recover");
    let outcomes = match mode {
        GrpcMethodType::NoStreaming => {
            let endpoint = make_grpc_no_streaming_endpoint_consumer(input, handler).unwrap();
            vec![
                endpoint.handle(context.clone(), 1).await.map(|_| ()),
                endpoint.handle(context, 1).await.map(|_| ()),
            ]
        }
        GrpcMethodType::ClientStreaming => {
            let endpoint = make_grpc_client_streaming_endpoint_consumer(input, handler).unwrap();
            vec![
                endpoint
                    .handle(context.clone(), stream::iter([Ok(1)]))
                    .await
                    .map(|_| ()),
                endpoint
                    .handle(context, stream::iter([Ok(1)]))
                    .await
                    .map(|_| ()),
            ]
        }
        GrpcMethodType::ServerStreaming => {
            let endpoint = make_grpc_server_streaming_endpoint_consumer(input, handler).unwrap();
            vec![
                endpoint.handle(context.clone(), 1, Arc::new(Discard)).await,
                endpoint.handle(context, 1, Arc::new(Discard)).await,
            ]
        }
        GrpcMethodType::BidirectionalStreaming => {
            let endpoint = make_grpc_bidi_streaming_endpoint_consumer(input, handler).unwrap();
            vec![
                endpoint
                    .handle(context.clone(), stream::iter([Ok(1)]), Arc::new(Discard))
                    .await,
                endpoint
                    .handle(context, stream::iter([Ok(1)]), Arc::new(Discard))
                    .await,
            ]
        }
        GrpcMethodType::Undefined => unreachable!(),
    };
    assert_eq!(*observed.lock().unwrap(), vec!["original consume error"; 2]);
    for outcome in outcomes {
        if recover {
            assert!(
                outcome.is_ok(),
                "EndRequest recovered the error, but Consume returned {outcome:?}"
            );
        } else {
            assert_eq!(outcome.unwrap_err().to_string(), "replacement end error");
        }
    }
}

#[tokio::test]
async fn end_request_can_recover_the_original_error() {
    for mode in [
        GrpcMethodType::NoStreaming,
        GrpcMethodType::ClientStreaming,
        GrpcMethodType::ServerStreaming,
        GrpcMethodType::BidirectionalStreaming,
    ] {
        for has_result in [false, true] {
            check(mode, has_result, true).await;
        }
    }
}

#[tokio::test]
async fn end_request_can_replace_the_original_error() {
    for mode in [
        GrpcMethodType::NoStreaming,
        GrpcMethodType::ClientStreaming,
        GrpcMethodType::ServerStreaming,
        GrpcMethodType::BidirectionalStreaming,
    ] {
        for has_result in [false, true] {
            check(mode, has_result, false).await;
        }
    }
}
