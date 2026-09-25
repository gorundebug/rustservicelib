use std::{
    future::Future,
    pin::Pin,
    sync::{
        Arc, Mutex, Weak,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    task::Poll,
    time::Duration,
};

use async_trait::async_trait;
use futures::{StreamExt, stream};
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
use tokio::sync::{Mutex as AsyncMutex, Semaphore};

type Invoke = Arc<
    dyn Fn(MessageContext) -> Pin<Box<dyn Future<Output = HandlerResult> + Send>> + Send + Sync,
>;

#[derive(Clone, Copy, Debug)]
enum Phase {
    Begin,
    Eof,
    Read,
    TransportRead,
    Result,
    Normal,
}

struct State {
    first: bool,
    _lifetime: Arc<()>,
}

struct Probe {
    phase: Phase,
    begins: AtomicUsize,
    consumes: AtomicUsize,
    eofs: AtomicUsize,
    ends: AtomicUsize,
    entered: Semaphore,
    release: Semaphore,
    context: Mutex<Option<MessageContext>>,
    lifetime: Mutex<Option<Weak<()>>>,
    tasks: Mutex<Vec<Option<tokio::task::Id>>>,
}

impl Probe {
    fn record_task(&self) {
        self.tasks.lock().unwrap().push(tokio::task::try_id());
    }

    async fn hold(&self) {
        self.entered.add_permits(1);
        self.release.acquire().await.unwrap().forget();
    }
}

struct Handler(Arc<Probe>);

#[async_trait]
impl EndpointHandler<State, u32, u32, u32, u32, String> for Handler {
    async fn begin_request(
        &self,
        context: MessageContext,
        _: StreamContext<u32, u32, String>,
    ) -> HandlerResult<(MessageContext, State)> {
        self.0.record_task();
        let first = self.0.begins.fetch_add(1, Ordering::SeqCst) == 0;
        let lifetime = Arc::new(());
        if first {
            *self.0.context.lock().unwrap() = Some(context.clone());
            *self.0.lifetime.lock().unwrap() = Some(Arc::downgrade(&lifetime));
            if matches!(self.0.phase, Phase::Begin) {
                self.0.hold().await;
            }
        }
        Ok((
            context,
            State {
                first,
                _lifetime: lifetime,
            },
        ))
    }

    async fn consume_message(
        &self,
        context: MessageContext,
        _: StreamContext<u32, u32, String>,
        state: Arc<AsyncMutex<State>>,
        request: u32,
        result: Arc<ResultContext<State, u32, u32, u32, String>>,
        sender: Arc<dyn Sender<u32>>,
    ) -> HandlerResult<MessageContext> {
        self.0.record_task();
        let first = state.lock().await.first;
        if first {
            self.0.consumes.fetch_add(1, Ordering::SeqCst);
        }
        if first && matches!(self.0.phase, Phase::Result) {
            // Neither response nor Done: cancellation must wake the runtime's
            // result wait, rather than relying on task abortion to clean up.
            self.0.entered.add_permits(1);
            return Ok(context);
        }
        sender.send(context.clone(), request).await?;
        result.done();
        Ok(context)
    }

    async fn get_message_id(
        &self,
        _: &MessageContext,
        _: &StreamContext<u32, u32, String>,
        _: Arc<AsyncMutex<State>>,
        _: &u32,
    ) -> String {
        "reply".into()
    }

    async fn eof(
        &self,
        _: MessageContext,
        _: StreamContext<u32, u32, String>,
        state: Arc<AsyncMutex<State>>,
    ) {
        self.0.record_task();
        if state.lock().await.first {
            if matches!(self.0.phase, Phase::Eof) {
                self.0.hold().await;
            }
            self.0.eofs.fetch_add(1, Ordering::SeqCst);
        }
    }

    async fn end_request(
        &self,
        _: MessageContext,
        _: StreamContext<u32, u32, String>,
        _: &HandlerResult,
        state: Arc<AsyncMutex<State>>,
    ) -> HandlerResult {
        self.0.record_task();
        if state.lock().await.first {
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

fn fixture(mode: GrpcMethodType, with_result: bool, phase: Phase) -> (Invoke, Arc<Probe>) {
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
    if with_result {
        input
            .set_source(&Stream::<u32>::new(
                &StreamConfig::new(2, "result"),
                environment,
            ))
            .unwrap();
    }
    let probe = Arc::new(Probe {
        phase,
        begins: AtomicUsize::new(0),
        consumes: AtomicUsize::new(0),
        eofs: AtomicUsize::new(0),
        ends: AtomicUsize::new(0),
        entered: Semaphore::new(0),
        release: Semaphore::new(0),
        context: Mutex::new(None),
        lifetime: Mutex::new(None),
        tasks: Mutex::new(Vec::new()),
    });
    let first_read = Arc::new(AtomicBool::new(matches!(
        phase,
        Phase::Read | Phase::TransportRead
    )));
    let tail_probe = probe.clone();
    let tail = move |context: MessageContext| {
        let mut held = first_read.swap(false, Ordering::SeqCst);
        let probe = tail_probe.clone();
        let mut entered = false;
        let mut cancelled = Box::pin(async move { context.cancelled().await });
        stream::poll_fn(move |cx| {
            if held {
                if !entered {
                    probe.entered.add_permits(1);
                    entered = true;
                }
                if matches!(phase, Phase::TransportRead) && cancelled.as_mut().poll(cx).is_ready() {
                    held = false;
                    return Poll::Ready(Some(Err("transport context cancelled".into())));
                }
                Poll::Pending
            } else {
                Poll::Ready(None::<HandlerResult<u32>>)
            }
        })
    };
    let invoke: Invoke = match mode {
        GrpcMethodType::NoStreaming => {
            let endpoint =
                make_grpc_no_streaming_endpoint_consumer(input, Handler(probe.clone())).unwrap();
            Arc::new(move |context| {
                let endpoint = endpoint.clone();
                Box::pin(async move { endpoint.handle(context, 1).await.map(|_| ()) })
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
        GrpcMethodType::ClientStreaming => {
            let endpoint =
                make_grpc_client_streaming_endpoint_consumer(input, Handler(probe.clone()))
                    .unwrap();
            Arc::new(move |context| {
                let endpoint = endpoint.clone();
                let tail = tail(context.clone());
                Box::pin(async move {
                    // The input borrows a local array: handle must not require a static stream.
                    let requests = [1_u32];
                    endpoint
                        .handle(
                            context,
                            stream::iter(requests.iter().copied().map(Ok)).chain(tail),
                        )
                        .await
                        .map(|_| ())
                })
            })
        }
        GrpcMethodType::BidirectionalStreaming => {
            let endpoint =
                make_grpc_bidi_streaming_endpoint_consumer(input, Handler(probe.clone())).unwrap();
            Arc::new(move |context| {
                let endpoint = endpoint.clone();
                let tail = tail(context.clone());
                Box::pin(async move {
                    let requests = [1_u32];
                    endpoint
                        .handle(
                            context,
                            stream::iter(requests.iter().copied().map(Ok)).chain(tail),
                            Arc::new(Discard),
                        )
                        .await
                })
            })
        }
        GrpcMethodType::Undefined => unreachable!(),
    };
    (invoke, probe)
}

async fn abandoned_phase(mode: GrpcMethodType, with_result: bool, phase: Phase) {
    let (invoke, probe) = fixture(mode, with_result, phase);
    let parent = MessageContext::new().with_stream_id("phase-cancel");
    let task = tokio::spawn(invoke(parent.clone()));
    tokio::time::timeout(Duration::from_secs(5), probe.entered.acquire())
        .await
        .unwrap()
        .unwrap()
        .forget();
    let context = probe.context.lock().unwrap().clone().unwrap();
    let weak = probe.lifetime.lock().unwrap().clone().unwrap();
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    tokio::time::timeout(Duration::from_secs(5), context.cancelled())
        .await
        .unwrap();
    assert!(!parent.is_cancelled());
    if !matches!(phase, Phase::Read) {
        assert!(weak.upgrade().is_some(), "active handler state was dropped");
        assert_eq!(
            probe.ends.load(Ordering::SeqCst),
            0,
            "EndRequest ran before the active phase ended"
        );
        probe.release.add_permits(1);
    }
    tokio::time::timeout(Duration::from_secs(5), async {
        while weak.upgrade().is_some() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("phase cancellation retained handler state");
    assert_eq!(probe.ends.load(Ordering::SeqCst), 1);
    assert_eq!(
        probe.consumes.load(Ordering::SeqCst),
        usize::from(!matches!(phase, Phase::Begin))
    );
    assert_eq!(
        probe.eofs.load(Ordering::SeqCst),
        usize::from(matches!(phase, Phase::Eof))
    );
    tokio::time::timeout(Duration::from_secs(5), invoke(parent))
        .await
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn abandoned_begin_and_eof_complete_before_finalization() {
    for mode in [
        GrpcMethodType::NoStreaming,
        GrpcMethodType::ClientStreaming,
        GrpcMethodType::ServerStreaming,
        GrpcMethodType::BidirectionalStreaming,
    ] {
        for with_result in [false, true] {
            for phase in [Phase::Begin, Phase::Eof] {
                abandoned_phase(mode, with_result, phase).await;
            }
        }
    }
}

#[tokio::test]
async fn abandoned_inbound_wait_finalizes_without_waiting_for_another_message() {
    for mode in [
        GrpcMethodType::ClientStreaming,
        GrpcMethodType::BidirectionalStreaming,
    ] {
        for with_result in [false, true] {
            abandoned_phase(mode, with_result, Phase::Read).await;
        }
    }
}

#[tokio::test]
async fn ordinary_source_handlers_stay_on_the_callers_task() {
    for mode in [
        GrpcMethodType::NoStreaming,
        GrpcMethodType::ClientStreaming,
        GrpcMethodType::ServerStreaming,
        GrpcMethodType::BidirectionalStreaming,
    ] {
        let (invoke, probe) = fixture(mode, true, Phase::Normal);
        let id = tokio::spawn(async move {
            let id = tokio::task::id();
            invoke(MessageContext::new()).await.unwrap();
            id
        })
        .await
        .unwrap();
        assert_eq!(*probe.tasks.lock().unwrap(), vec![Some(id); 4]);
    }
}

#[tokio::test]
async fn context_cancellation_alone_finalizes_each_source_phase() {
    for mode in [
        GrpcMethodType::NoStreaming,
        GrpcMethodType::ClientStreaming,
        GrpcMethodType::ServerStreaming,
        GrpcMethodType::BidirectionalStreaming,
    ] {
        for with_result in [false, true] {
            for phase in [Phase::Begin, Phase::Eof, Phase::Result] {
                if matches!(phase, Phase::Result) && !with_result {
                    continue;
                }
                let (invoke, probe) = fixture(mode, with_result, phase);
                let parent = MessageContext::new().with_stream_id("context-only-cancel");
                let task = tokio::spawn(invoke(parent.clone()));
                tokio::time::timeout(Duration::from_secs(5), probe.entered.acquire())
                    .await
                    .unwrap()
                    .unwrap()
                    .forget();
                let context = probe.context.lock().unwrap().clone().unwrap();
                let weak = probe.lifetime.lock().unwrap().clone().unwrap();
                parent.cancel();
                tokio::time::timeout(Duration::from_secs(5), context.cancelled())
                    .await
                    .unwrap();
                if matches!(phase, Phase::Begin | Phase::Eof) {
                    assert!(
                        weak.upgrade().is_some(),
                        "cancellation dropped an active handler"
                    );
                    assert_eq!(probe.ends.load(Ordering::SeqCst), 0);
                    assert!(
                        !task.is_finished(),
                        "cancellation overtook an active handler"
                    );
                    probe.release.add_permits(1);
                }
                // EndRequest is allowed to replace the final error. Here we
                // test wakeup, lifetime and admission, not error replacement.
                let _result = tokio::time::timeout(Duration::from_secs(5), task)
                    .await.unwrap_or_else(|error| panic!(
                        "context cancellation did not finish source request: mode={mode:?}, with_result={with_result}, phase={phase:?}: {error}"))
                    .expect("source task panicked");
                assert_eq!(probe.ends.load(Ordering::SeqCst), 1);
                assert!(
                    weak.upgrade().is_none(),
                    "completed request retained handler state"
                );
                tokio::time::timeout(
                    Duration::from_secs(5),
                    invoke(MessageContext::new().with_stream_id("context-only-cancel")),
                )
                .await
                .unwrap()
                .unwrap();
            }
        }
    }
}

#[tokio::test]
async fn sources_without_result_stream_do_not_wait_for_done() {
    for mode in [
        GrpcMethodType::NoStreaming,
        GrpcMethodType::ClientStreaming,
        GrpcMethodType::ServerStreaming,
        GrpcMethodType::BidirectionalStreaming,
    ] {
        let (invoke, probe) = fixture(mode, false, Phase::Result);
        tokio::time::timeout(Duration::from_secs(5), invoke(MessageContext::new()))
            .await
            .expect("source without result stream waited for Done")
            .unwrap();
        assert_eq!(probe.ends.load(Ordering::SeqCst), 1);
        assert!(
            probe
                .lifetime
                .lock()
                .unwrap()
                .as_ref()
                .unwrap()
                .upgrade()
                .is_none()
        );
    }
}

#[tokio::test]
async fn cancelled_inbound_transport_finalizes_streaming_sources() {
    for mode in [
        GrpcMethodType::ClientStreaming,
        GrpcMethodType::BidirectionalStreaming,
    ] {
        for with_result in [false, true] {
            let (invoke, probe) = fixture(mode, with_result, Phase::TransportRead);
            let parent = MessageContext::new().with_stream_id("cancelled-transport");
            let task = tokio::spawn(invoke(parent.clone()));
            tokio::time::timeout(Duration::from_secs(5), probe.entered.acquire())
                .await
                .unwrap()
                .unwrap()
                .forget();
            let weak = probe.lifetime.lock().unwrap().clone().unwrap();
            parent.cancel();
            // No task.abort(): the transport reports cancellation through its
            // normal error channel, just as a cancelled Go gRPC Recv does.
            let _result = tokio::time::timeout(Duration::from_secs(5), task)
                .await.unwrap_or_else(|error| panic!(
                    "cancelled transport did not finalize: mode={mode:?}, with_result={with_result}: {error}"))
                .expect("source task panicked");
            assert_eq!(probe.ends.load(Ordering::SeqCst), 1);
            assert_eq!(probe.consumes.load(Ordering::SeqCst), 1);
            assert_eq!(
                probe.eofs.load(Ordering::SeqCst),
                0,
                "transport error is not EOF"
            );
            assert!(
                weak.upgrade().is_none(),
                "cancelled transport retained handler state"
            );
            tokio::time::timeout(
                Duration::from_secs(5),
                invoke(MessageContext::new().with_stream_id("cancelled-transport")),
            )
            .await
            .unwrap()
            .unwrap();
        }
    }
}
