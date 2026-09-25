use std::{future::Future, pin::Pin, sync::{Arc, atomic::{AtomicBool, AtomicUsize, Ordering}}, time::Duration};

use async_trait::async_trait;
use futures::{future::join_all, stream};
use servicelib::{
    api::GrpcMethodType,
    MessageContext,
    datasource::grpc::{
        EndpointHandler, HandlerResult, ResultContext, Sender, StreamContext,
        make_grpc_no_streaming_endpoint_consumer,
        make_grpc_client_streaming_endpoint_consumer,
        make_grpc_server_streaming_endpoint_consumer,
        make_grpc_bidi_streaming_endpoint_consumer,
    },
    operators::input::InputStream,
    runtime::{
        common::Payload,
        config::{CallSemantics, GrpcDataConnectorConfig, GrpcEndpointConfig, InputStreamConfig, RuntimeConfig, StreamConfig},
        environment::RuntimeEnvironment,
        stream::Stream,
    },
};
use tokio::sync::{Mutex, Semaphore};

type Invoke = Arc<dyn Fn(MessageContext) -> Pin<Box<dyn Future<Output = HandlerResult> + Send>> + Send + Sync>;
type TestResultContext = ResultContext<bool, u32, u32, u32, String>;

#[derive(Clone, Copy)]
enum Phase { Consume, End }

struct Handler {
    phase: Phase,
    begins: AtomicUsize,
    consumes: AtomicUsize,
    consumed: AtomicUsize,
    ended: AtomicUsize,
    entered: Semaphore,
    release: Semaphore,
    results: Option<Stream<u32>>,
    callbacks: Arc<AtomicUsize>,
    capture_result: AtomicBool,
    result_context: std::sync::Mutex<Option<std::sync::Weak<TestResultContext>>>,
}

impl Handler {
    async fn hold(&self) {
        self.entered.add_permits(1);
        self.release.acquire().await.unwrap().forget();
    }
}

struct SharedHandler(Arc<Handler>);

impl std::ops::Deref for SharedHandler {
    type Target = Handler;

    fn deref(&self) -> &Self::Target {
        self.0.as_ref()
    }
}

#[async_trait]
impl EndpointHandler<bool, u32, u32, u32, u32, String> for SharedHandler {
    async fn begin_request(&self, context: MessageContext, _: StreamContext<u32, u32, String>) -> HandlerResult<(MessageContext, bool)> {
        Ok((context, self.begins.fetch_add(1, Ordering::SeqCst) == 0))
    }

    async fn consume_message(&self, context: MessageContext, _: StreamContext<u32, u32, String>, state: Arc<Mutex<bool>>, request: u32, result: Arc<ResultContext<bool, u32, u32, u32, String>>, sender: Arc<dyn Sender<u32>>) -> HandlerResult<MessageContext> {
        self.consumes.fetch_add(1, Ordering::SeqCst);
        let first = *state.lock().await;
        *self.result_context.lock().unwrap() = Some(Arc::downgrade(&result));
        let callback_owner = self.capture_result.load(Ordering::SeqCst).then(|| result.clone());
        let callbacks = self.callbacks.clone();
        result.set_result_callback("reply", Arc::new(move |_, _, _, _, _| {
            let callbacks = callbacks.clone();
            let callback_owner = callback_owner.clone();
            Box::pin(async move {
                if let Some(owner) = callback_owner {
                    owner.done();
                }
                callbacks.fetch_add(1, Ordering::SeqCst);
                false
            })
        }));
        if let Some(results) = &self.results {
            results.emit(context.clone(), Payload::new(7)).await;
        }
        if first && matches!(self.phase, Phase::Consume) { self.hold().await; }
        sender.send(context.clone(), request).await?;
        result.done();
        self.consumed.fetch_add(1, Ordering::SeqCst);
        Ok(context)
    }

    async fn get_message_id(&self, _: &MessageContext, _: &StreamContext<u32, u32, String>, _: Arc<Mutex<bool>>, _: &u32) -> String {
        "reply".to_owned()
    }

    async fn eof(&self, _: MessageContext, _: StreamContext<u32, u32, String>, _: Arc<Mutex<bool>>) {}

    async fn end_request(&self, context: MessageContext, _: StreamContext<u32, u32, String>, _: &HandlerResult, state: Arc<Mutex<bool>>) -> HandlerResult {
        let first = *state.lock().await;
        // Finalization must reject late results without waiting on its own
        // lifetime writer lock or invoking the still-registered callback.
        if first && let Some(results) = &self.results {
            results.emit(context, Payload::new(7)).await;
        }
        if first && matches!(self.phase, Phase::End) { self.hold().await; }
        self.ended.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

struct DiscardSender;

#[async_trait]
impl Sender<u32> for DiscardSender {
    async fn send(&self, _: MessageContext, _: u32) -> HandlerResult { Ok(()) }
}

fn fixture(mode: GrpcMethodType, with_result: bool, phase: Phase) -> (Invoke, Arc<Handler>) {
    let environment = RuntimeEnvironment::default();
    let config = InputStreamConfig { stream: StreamConfig::new(1, "input"), endpoint_id: 4 };
    environment.publish_runtime_config(Arc::new(RuntimeConfig::from_parts(
        CallSemantics::FunctionCall, [], [config.clone().into()], [],
        [GrpcDataConnectorConfig { id: 10, name: "grpc".into(), address: "http://localhost".into(), connections_count: 1 }.into()],
        [GrpcEndpointConfig { id: 4, name: "request".into(), id_data_connector: 10, tracing_enabled: false, grpc_method_type: mode }.into()], [],
    ).unwrap()));
    let input = InputStream::<u32, u32, String>::new(&config, environment.clone());
    let results = if with_result {
        let results = Stream::new(&StreamConfig::new(2, "results"), environment);
        input.set_source(&results).unwrap();
        Some(results)
    } else { None };
    let handler = Arc::new(Handler { phase, begins: AtomicUsize::new(0), consumes: AtomicUsize::new(0), consumed: AtomicUsize::new(0), ended: AtomicUsize::new(0), entered: Semaphore::new(0), release: Semaphore::new(0), results, callbacks: Arc::new(AtomicUsize::new(0)), capture_result: AtomicBool::new(false), result_context: std::sync::Mutex::new(None) });
    let invoke: Invoke = match mode {
        GrpcMethodType::NoStreaming => {
            let endpoint = make_grpc_no_streaming_endpoint_consumer(input, SharedHandler(handler.clone())).unwrap();
            Arc::new(move |context| { let endpoint = endpoint.clone(); Box::pin(async move { endpoint.handle(context, 1).await.map(|_| ()) }) })
        }
        GrpcMethodType::ClientStreaming => {
            let endpoint = make_grpc_client_streaming_endpoint_consumer(input, SharedHandler(handler.clone())).unwrap();
            Arc::new(move |context| { let endpoint = endpoint.clone(); Box::pin(async move { endpoint.handle(context, stream::iter([Ok(1)])).await.map(|_| ()) }) })
        }
        GrpcMethodType::ServerStreaming => {
            let endpoint = make_grpc_server_streaming_endpoint_consumer(input, SharedHandler(handler.clone())).unwrap();
            Arc::new(move |context| { let endpoint = endpoint.clone(); Box::pin(async move { endpoint.handle(context, 1, Arc::new(DiscardSender)).await }) })
        }
        GrpcMethodType::BidirectionalStreaming => {
            let endpoint = make_grpc_bidi_streaming_endpoint_consumer(input, SharedHandler(handler.clone())).unwrap();
            Arc::new(move |context| { let endpoint = endpoint.clone(); Box::pin(async move { endpoint.handle(context, stream::iter([Ok(1)]), Arc::new(DiscardSender)).await }) })
        }
        GrpcMethodType::Undefined => unreachable!(),
    };
    (invoke, handler)
}

async fn check(mode: GrpcMethodType, with_result: bool, phase: Phase) {
    let (invoke, handler) = fixture(mode, with_result, phase);
    let context = MessageContext::new().with_stream_id("active");
    let first_invoke = invoke.clone();
    let first_context = context.clone();
    let first = tokio::spawn(async move { first_invoke(first_context).await });
    tokio::time::timeout(Duration::from_secs(5), handler.entered.acquire()).await.unwrap().unwrap().forget();

    // A different session must not be serialized behind the held handler.
    let other = tokio::time::timeout(Duration::from_secs(5), invoke(MessageContext::new().with_stream_id("other"))).await;
    let duplicates = tokio::time::timeout(Duration::from_secs(5), join_all((0..8).map(|_| invoke(context.clone())))).await;
    let consumed_before_release = handler.consumes.load(Ordering::SeqCst);
    handler.release.add_permits(1);
    tokio::time::timeout(Duration::from_secs(5), first).await.unwrap().unwrap().unwrap();
    other.unwrap().unwrap();
    for result in duplicates.unwrap() {
        let error = result.expect_err("an independent RPC reused an active stream ID");
        assert!(error.to_string().contains("duplicate"), "{error}");
    }
    assert_eq!(consumed_before_release, 2, "duplicate requests reached business handlers");
    assert_eq!(handler.callbacks.load(Ordering::SeqCst), if with_result { 2 } else { 0 }, "late results reached a closing request");
    // A completed invocation must not reserve the ID forever.
    tokio::time::timeout(Duration::from_secs(5), invoke(context)).await.unwrap().unwrap();
    assert_eq!(handler.consumes.load(Ordering::SeqCst), 3);
    assert_eq!(handler.callbacks.load(Ordering::SeqCst), if with_result { 3 } else { 0 });
}

macro_rules! reservation_test {
    ($name:ident, $mode:ident, $result:expr, $phase:ident) => {
        #[tokio::test]
        async fn $name() { check(GrpcMethodType::$mode, $result, Phase::$phase).await; }
    };
}

reservation_test!(unary_result_consume, NoStreaming, true, Consume);
reservation_test!(unary_result_end, NoStreaming, true, End);
reservation_test!(unary_no_result_consume, NoStreaming, false, Consume);
reservation_test!(unary_no_result_end, NoStreaming, false, End);
reservation_test!(client_result_consume, ClientStreaming, true, Consume);
reservation_test!(client_result_end, ClientStreaming, true, End);
reservation_test!(client_no_result_consume, ClientStreaming, false, Consume);
reservation_test!(client_no_result_end, ClientStreaming, false, End);
reservation_test!(server_result_consume, ServerStreaming, true, Consume);
reservation_test!(server_result_end, ServerStreaming, true, End);
reservation_test!(server_no_result_consume, ServerStreaming, false, Consume);
reservation_test!(server_no_result_end, ServerStreaming, false, End);
reservation_test!(bidi_result_consume, BidirectionalStreaming, true, Consume);
reservation_test!(bidi_result_end, BidirectionalStreaming, true, End);
reservation_test!(bidi_no_result_consume, BidirectionalStreaming, false, Consume);
reservation_test!(bidi_no_result_end, BidirectionalStreaming, false, End);

async fn check_callback_release(mode: GrpcMethodType, with_result: bool) {
    let (invoke, handler) = fixture(mode, with_result, Phase::Consume);
    handler.capture_result.store(true, Ordering::SeqCst);
    handler.release.add_permits(1);
    tokio::time::timeout(Duration::from_secs(5), invoke(MessageContext::new().with_stream_id("callback-owner"))).await.unwrap().unwrap();
    let result_context = handler.result_context.lock().unwrap().clone().unwrap();
    assert!(result_context.upgrade().is_none(), "completed request retained a callback capturing its own ResultContext");
}

macro_rules! callback_release_test {
    ($name:ident, $mode:ident, $result:expr) => {
        #[tokio::test]
        async fn $name() { check_callback_release(GrpcMethodType::$mode, $result).await; }
    };
}

callback_release_test!(unary_releases_callback_cycle, NoStreaming, true);
callback_release_test!(unary_no_result_releases_callback_cycle, NoStreaming, false);
callback_release_test!(client_releases_callback_cycle, ClientStreaming, true);
callback_release_test!(client_no_result_releases_callback_cycle, ClientStreaming, false);
callback_release_test!(server_releases_callback_cycle, ServerStreaming, true);
callback_release_test!(server_no_result_releases_callback_cycle, ServerStreaming, false);
callback_release_test!(bidi_releases_callback_cycle, BidirectionalStreaming, true);
callback_release_test!(bidi_no_result_releases_callback_cycle, BidirectionalStreaming, false);

async fn check_late_callback_registration(mode: GrpcMethodType, with_result: bool) {
    let (invoke, handler) = fixture(mode, with_result, Phase::Consume);
    let first = tokio::spawn(async move {
        invoke(MessageContext::new().with_stream_id("late-registration")).await
    });
    tokio::time::timeout(Duration::from_secs(5), handler.entered.acquire()).await.unwrap().unwrap().forget();
    let weak = handler.result_context.lock().unwrap().clone().unwrap();
    let result = weak.upgrade().unwrap();
    handler.release.add_permits(1);
    tokio::time::timeout(Duration::from_secs(5), first).await.unwrap().unwrap().unwrap();

    let captured = result.clone();
    result.set_result_callback("late", Arc::new(move |_, _, _, _, _| {
        let captured = captured.clone();
        Box::pin(async move {
            captured.done();
            false
        })
    }));
    drop(result);
    assert!(weak.upgrade().is_none(), "late registration revived a finished request's callback cycle");
}

macro_rules! late_callback_registration_test {
    ($name:ident, $mode:ident, $result:expr) => {
        #[tokio::test]
        async fn $name() { check_late_callback_registration(GrpcMethodType::$mode, $result).await; }
    };
}

late_callback_registration_test!(unary_ignores_late_registration, NoStreaming, true);
late_callback_registration_test!(unary_no_result_ignores_late_registration, NoStreaming, false);
late_callback_registration_test!(client_ignores_late_registration, ClientStreaming, true);
late_callback_registration_test!(client_no_result_ignores_late_registration, ClientStreaming, false);
late_callback_registration_test!(server_ignores_late_registration, ServerStreaming, true);
late_callback_registration_test!(server_no_result_ignores_late_registration, ServerStreaming, false);
late_callback_registration_test!(bidi_ignores_late_registration, BidirectionalStreaming, true);
late_callback_registration_test!(bidi_no_result_ignores_late_registration, BidirectionalStreaming, false);
async fn check_abandoned_source_request(mode: GrpcMethodType, with_result: bool) {
    for phase in [Phase::Consume, Phase::End] {
    let (invoke, handler) = fixture(mode, with_result, phase);
    let parent = MessageContext::new().with_stream_id("abandoned");
    let active = tokio::spawn(invoke(parent.clone()));
    tokio::time::timeout(Duration::from_secs(2), handler.entered.acquire())
        .await.unwrap().unwrap().forget();
    let result_lifetime = handler.result_context.lock().unwrap()
        .as_ref().expect("admitted request has a result context").clone();
    active.abort();
    assert!(active.await.unwrap_err().is_cancelled());
    assert!(!parent.is_cancelled(), "request cancellation escaped into its caller");
    let duplicate = tokio::time::timeout(Duration::from_secs(2), invoke(parent.clone())).await.unwrap();
    assert!(duplicate.unwrap_err().to_string().contains("duplicate"));
    assert_eq!(handler.consumes.load(Ordering::SeqCst), 1);
    // If the implementation retains admitted work, let it finish before retrying.
    handler.release.add_permits(1);
    tokio::time::timeout(Duration::from_secs(2), async {
        while result_lifetime.upgrade().is_some() {
            tokio::task::yield_now().await;
        }
    }).await.expect("abandoned request did not retire after its handler was released");
    assert_eq!(handler.consumed.load(Ordering::SeqCst), 1, "admitted handler was dropped");
    assert_eq!(handler.ended.load(Ordering::SeqCst), 2, "original and duplicate must each finalize once");
    let retry = tokio::time::timeout(Duration::from_secs(2),
        invoke(MessageContext::new().with_stream_id("abandoned"))).await.unwrap();
    assert!(retry.is_ok(), "abandoned handler kept its stream ID reserved: {retry:?}");
    }
}

macro_rules! abandoned_source_case {
    ($name:ident, $mode:ident, $with_result:expr) => {
        #[tokio::test]
        async fn $name() {
            check_abandoned_source_request(GrpcMethodType::$mode, $with_result).await;
        }
    };
}

abandoned_source_case!(abandoned_unary_releases_id, NoStreaming, true);
abandoned_source_case!(abandoned_unary_no_result_releases_id, NoStreaming, false);
abandoned_source_case!(abandoned_client_releases_id, ClientStreaming, true);
abandoned_source_case!(abandoned_client_no_result_releases_id, ClientStreaming, false);
abandoned_source_case!(abandoned_server_releases_id, ServerStreaming, true);
abandoned_source_case!(abandoned_server_no_result_releases_id, ServerStreaming, false);
abandoned_source_case!(abandoned_bidi_releases_id, BidirectionalStreaming, true);
abandoned_source_case!(abandoned_bidi_no_result_releases_id, BidirectionalStreaming, false);
