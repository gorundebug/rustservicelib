use std::{
    future::Future,
    pin::Pin,
    sync::{
        Arc, Mutex, Weak,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

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
        common::Payload,
        config::{
            CallSemantics, GrpcDataConnectorConfig, GrpcEndpointConfig, InputStreamConfig,
            RuntimeConfig, StreamConfig,
        },
        environment::RuntimeEnvironment,
        stream::Stream,
    },
};
use tokio::sync::{Mutex as AsyncMutex, Semaphore};

type Invoke = Arc<
    dyn Fn(MessageContext) -> Pin<Box<dyn Future<Output = HandlerResult> + Send>> + Send + Sync,
>;
type Results = ResultContext<bool, u32, u32, u32, String>;

struct Probe {
    begins: AtomicUsize,
    callbacks: AtomicUsize,
    ends: AtomicUsize,
    consumed: Semaphore,
    callback_entered: Semaphore,
    release_callback: Semaphore,
    end_entered: Semaphore,
    release_end: Semaphore,
    context: Mutex<Option<MessageContext>>,
    result: Mutex<Option<Weak<Results>>>,
}

struct Handler(Arc<Probe>);

#[async_trait]
impl EndpointHandler<bool, u32, u32, u32, u32, String> for Handler {
    async fn begin_request(
        &self,
        context: MessageContext,
        _: StreamContext<u32, u32, String>,
    ) -> HandlerResult<(MessageContext, bool)> {
        Ok((context, self.0.begins.fetch_add(1, Ordering::SeqCst) == 0))
    }

    async fn consume_message(
        &self,
        context: MessageContext,
        _: StreamContext<u32, u32, String>,
        state: Arc<AsyncMutex<bool>>,
        request: u32,
        result: Arc<Results>,
        sender: Arc<dyn Sender<u32>>,
    ) -> HandlerResult<MessageContext> {
        if *state.lock().await {
            *self.0.context.lock().unwrap() = Some(context.clone());
            *self.0.result.lock().unwrap() = Some(Arc::downgrade(&result));
            let probe = self.0.clone();
            let result_owner = result.clone();
            result.set_result_callback(
                "reply",
                Arc::new(move |context, _, _, _, sender| {
                    let probe = probe.clone();
                    let result_owner = result_owner.clone();
                    Box::pin(async move {
                        probe.callbacks.fetch_add(1, Ordering::SeqCst);
                        sender.send(context, 7).await.unwrap();
                        result_owner.done();
                        probe.callback_entered.add_permits(1);
                        probe.release_callback.acquire().await.unwrap().forget();
                        false
                    })
                }),
            );
            self.0.consumed.add_permits(1);
        } else {
            sender.send(context.clone(), request).await?;
            result.done();
        }
        Ok(context)
    }

    async fn get_message_id(
        &self,
        _: &MessageContext,
        _: &StreamContext<u32, u32, String>,
        _: Arc<AsyncMutex<bool>>,
        _: &u32,
    ) -> String {
        "reply".into()
    }

    async fn eof(
        &self,
        _: MessageContext,
        _: StreamContext<u32, u32, String>,
        _: Arc<AsyncMutex<bool>>,
    ) {
    }

    async fn end_request(
        &self,
        _: MessageContext,
        _: StreamContext<u32, u32, String>,
        _: &HandlerResult,
        state: Arc<AsyncMutex<bool>>,
    ) -> HandlerResult {
        if *state.lock().await {
            self.0.end_entered.add_permits(1);
            self.0.release_end.acquire().await.unwrap().forget();
            self.0.ends.fetch_add(1, Ordering::SeqCst);
        }
        Ok(())
    }
}

struct Discard;
#[async_trait]
impl Sender<u32> for Discard {
    async fn send(&self, _: MessageContext, _: u32) -> HandlerResult {
        Ok(())
    }
}

fn fixture(mode: GrpcMethodType) -> (Invoke, Stream<u32>, Arc<Probe>) {
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
    let results = Stream::new(&StreamConfig::new(2, "results"), environment);
    input.set_source(&results).unwrap();
    let probe = Arc::new(Probe {
        begins: AtomicUsize::new(0),
        callbacks: AtomicUsize::new(0),
        ends: AtomicUsize::new(0),
        consumed: Semaphore::new(0),
        callback_entered: Semaphore::new(0),
        release_callback: Semaphore::new(0),
        end_entered: Semaphore::new(0),
        release_end: Semaphore::new(0),
        context: Mutex::new(None),
        result: Mutex::new(None),
    });
    let invoke: Invoke = match mode {
        GrpcMethodType::NoStreaming => {
            let endpoint =
                make_grpc_no_streaming_endpoint_consumer(input, Handler(probe.clone())).unwrap();
            Arc::new(move |context| {
                let endpoint = endpoint.clone();
                Box::pin(async move { endpoint.handle(context, 1).await.map(|_| ()) })
            })
        }
        GrpcMethodType::ClientStreaming => {
            let endpoint =
                make_grpc_client_streaming_endpoint_consumer(input, Handler(probe.clone()))
                    .unwrap();
            Arc::new(move |context| {
                let endpoint = endpoint.clone();
                Box::pin(async move {
                    endpoint
                        .handle(context, stream::iter([Ok(1)]))
                        .await
                        .map(|_| ())
                })
            })
        }
        GrpcMethodType::ServerStreaming => {
            let endpoint =
                make_grpc_server_streaming_endpoint_consumer(input, Handler(probe.clone()))
                    .unwrap();
            Arc::new(move |context| {
                let endpoint = endpoint.clone();
                Box::pin(async move { endpoint.handle(context, 1, Arc::new(Discard)).await })
            })
        }
        GrpcMethodType::BidirectionalStreaming => {
            let endpoint =
                make_grpc_bidi_streaming_endpoint_consumer(input, Handler(probe.clone())).unwrap();
            Arc::new(move |context| {
                let endpoint = endpoint.clone();
                Box::pin(async move {
                    endpoint
                        .handle(context, stream::iter([Ok(1)]), Arc::new(Discard))
                        .await
                })
            })
        }
        GrpcMethodType::Undefined => unreachable!(),
    };
    (invoke, results, probe)
}

async fn gate(semaphore: &Semaphore) {
    tokio::time::timeout(Duration::from_secs(5), semaphore.acquire())
        .await
        .unwrap()
        .unwrap()
        .forget();
}

async fn check(mode: GrpcMethodType, active_callback: bool) {
    let (invoke, results, probe) = fixture(mode);
    let task = tokio::spawn(invoke(MessageContext::new().with_stream_id("drain")));
    gate(&probe.consumed).await;
    let context = probe.context.lock().unwrap().clone().unwrap();
    let weak = probe.result.lock().unwrap().clone().unwrap();
    let callback = if active_callback {
        let results = results.clone();
        let context = context.clone();
        let callback = tokio::spawn(async move { results.emit(context, Payload::new(7)).await });
        gate(&probe.callback_entered).await;
        Some(callback)
    } else {
        None
    };

    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    tokio::time::timeout(Duration::from_secs(5), context.cancelled())
        .await
        .unwrap();
    if active_callback {
        assert!(
            tokio::time::timeout(Duration::from_millis(20), probe.end_entered.acquire())
                .await
                .is_err(),
            "EndRequest raced an active callback"
        );
        assert!(
            weak.upgrade().is_some(),
            "active callback lost its request state"
        );
    }
    let duplicate = tokio::time::timeout(
        Duration::from_secs(5),
        invoke(MessageContext::new().with_stream_id("drain")),
    )
    .await
    .unwrap();
    assert!(duplicate.unwrap_err().to_string().contains("duplicate"));
    if let Some(callback) = callback {
        probe.release_callback.add_permits(1);
        tokio::time::timeout(Duration::from_secs(5), callback)
            .await
            .unwrap()
            .unwrap();
    }
    gate(&probe.end_entered).await;
    tokio::time::timeout(
        Duration::from_secs(5),
        results.emit(context, Payload::new(8)),
    )
    .await
    .unwrap();
    assert_eq!(
        probe.callbacks.load(Ordering::SeqCst),
        usize::from(active_callback)
    );
    probe.release_end.add_permits(1);
    tokio::time::timeout(Duration::from_secs(5), async {
        while weak.upgrade().is_some() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("finished callback/request cycle was retained");
    assert_eq!(probe.ends.load(Ordering::SeqCst), 1);
    tokio::time::timeout(
        Duration::from_secs(5),
        invoke(MessageContext::new().with_stream_id("drain")),
    )
    .await
    .unwrap()
    .unwrap();
}

#[tokio::test]
async fn cancelled_request_drains_accepted_callback_before_end_request() {
    for mode in [
        GrpcMethodType::NoStreaming,
        GrpcMethodType::ClientStreaming,
        GrpcMethodType::ServerStreaming,
        GrpcMethodType::BidirectionalStreaming,
    ] {
        check(mode, true).await;
    }
}

#[tokio::test]
async fn abandoned_response_wait_finalizes_without_a_result() {
    for mode in [
        GrpcMethodType::NoStreaming,
        GrpcMethodType::ClientStreaming,
        GrpcMethodType::ServerStreaming,
        GrpcMethodType::BidirectionalStreaming,
    ] {
        check(mode, false).await;
    }
}
