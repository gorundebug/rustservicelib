use std::{
    sync::{Arc, Mutex},
    time::Duration,
};

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use servicelib::{
    MessageContext, Payload,
    operators::{
        DelayFunction, JoinFunction, KeyByFunction, LinkStream, MapFunction, MultiJoinFunction,
        ProcessFunction, downcast_join_values,
    },
    runtime::{
        common::{Consumer, RuntimeStream},
        config::{
            CallSemantics, Config, InputStreamConfig, JoinStreamConfig, JoinType, LinkConfig,
            MapStreamConfig, MultiJoinStreamConfig, RuntimeConfig, RuntimeStreamConfig,
            StreamConfig,
        },
        datastruct::KeyValue,
        environment::RuntimeEnvironment,
        store::JoinValues,
        stream::Stream,
    },
};

#[derive(Clone, Serialize, Deserialize)]
struct OperatorTestConfig {
    streams: Vec<RuntimeStreamConfig>,
    links: Vec<LinkConfig>,
}

impl Config for OperatorTestConfig {
    fn apply_environment(&mut self) -> Result<(), String> {
        Ok(())
    }

    fn streams(&self) -> Vec<RuntimeStreamConfig> {
        self.streams.clone()
    }

    fn links(&self) -> Vec<LinkConfig> {
        self.links.clone()
    }
}

fn test_environment(
    overrides: Vec<RuntimeStreamConfig>,
    links: Vec<LinkConfig>,
) -> RuntimeEnvironment {
    let overridden = overrides
        .iter()
        .map(|config| config.stream().id)
        .collect::<std::collections::HashSet<_>>();
    let mut streams = (1..=20)
        .filter(|id| !overridden.contains(id))
        .map(|id| {
            RuntimeStreamConfig::Map(MapStreamConfig::from(StreamConfig::new(
                id,
                format!("Stream {id}"),
            )))
        })
        .collect::<Vec<_>>();
    streams.extend(overrides);
    let config = OperatorTestConfig { streams, links };
    let environment = RuntimeEnvironment::default();
    environment.publish_runtime_config(Arc::new(
        RuntimeConfig::new(&config).expect("valid operator test runtime config"),
    ));
    environment
}

struct Capture<T>(Mutex<Vec<(MessageContext, Arc<T>)>>);

impl<T> Default for Capture<T> {
    fn default() -> Self {
        Self(Mutex::new(Vec::new()))
    }
}

#[async_trait]
impl<T> Consumer<T> for Capture<T>
where
    T: Send + Sync + 'static,
{
    async fn consume(&self, context: MessageContext, payload: Payload<T>) {
        self.0
            .lock()
            .expect("capture lock poisoned")
            .push((context, payload.into_arc()));
    }
}

struct Double;

#[async_trait]
impl MapFunction<i32, i32> for Double {
    async fn map(
        &self,
        context: MessageContext,
        _stream: &dyn RuntimeStream,
        value: Payload<i32>,
        out: &Stream<i32>,
    ) {
        out.emit(context, Payload::new(*value * 2)).await;
    }
}

struct ToKeyValue;

#[async_trait]
impl KeyByFunction<i32, String, i32> for ToKeyValue {
    async fn key_by(
        &self,
        context: MessageContext,
        _stream: &dyn RuntimeStream,
        value: Payload<i32>,
        out: &Stream<KeyValue<String, i32>>,
    ) {
        out.emit(
            context,
            Payload::new(KeyValue {
                key: format!("key-{}", *value),
                value: *value,
            }),
        )
        .await;
    }
}

struct EvenOrError;

#[async_trait]
impl ProcessFunction<i32, i32, String> for EvenOrError {
    async fn process(
        &self,
        context: MessageContext,
        _stream: &dyn RuntimeStream,
        value: Payload<i32>,
        out: &Stream<i32>,
        error: &Stream<String>,
    ) {
        if *value % 2 == 0 {
            out.emit(context, value).await;
        } else {
            error
                .emit(context, Payload::new(format!("odd: {}", *value)))
                .await;
        }
    }
}

#[tokio::test]
async fn input_and_map_use_parent_stream_api() {
    let environment = test_environment(Vec::new(), Vec::new());
    let input = servicelib::operators::InputStream::<i32, (), ()>::new(
        &InputStreamConfig {
            stream: StreamConfig::new(1, "Input"),
            endpoint_id: 10,
        },
        environment,
    );
    let mapped = input
        .stream()
        .map(&(StreamConfig::new(2, "Double").into()), Double)
        .unwrap();
    let capture = Arc::new(Capture::default());
    mapped.set_consumer(Arc::clone(&capture), 3);

    input.consume(MessageContext::new(), 21).await;

    let values = capture.0.lock().unwrap();
    assert_eq!(*values[0].1, 42);
}

#[test]
fn stream_resolves_config_from_each_published_runtime_snapshot() {
    let environment = test_environment(Vec::new(), Vec::new());
    let stream = Stream::<i32>::new(
        &StreamConfig::new(1, "constructor value is not retained"),
        environment.clone(),
    );
    assert_eq!(stream.name(), "Stream 1");

    let replacement = OperatorTestConfig {
        streams: vec![RuntimeStreamConfig::Map(MapStreamConfig::from(
            StreamConfig::new(1, "Reloaded Stream"),
        ))],
        links: Vec::new(),
    };
    environment.publish_runtime_config(Arc::new(
        RuntimeConfig::new(&replacement).expect("valid replacement snapshot"),
    ));

    assert_eq!(stream.name(), "Reloaded Stream");
}

#[tokio::test]
async fn key_by_and_process_are_available_from_the_parent_stream() {
    let environment = test_environment(Vec::new(), Vec::new());
    let key_source = Stream::new(&StreamConfig::new(1, "Key Input"), environment.clone());
    let keyed = key_source
        .key_by(&(StreamConfig::new(2, "Key By").into()), ToKeyValue)
        .unwrap();
    let keyed_capture = Arc::new(Capture::default());
    keyed.set_consumer(Arc::clone(&keyed_capture), 3);
    key_source
        .emit(MessageContext::new(), Payload::new(7))
        .await;
    {
        let keyed_values = keyed_capture.0.lock().unwrap();
        assert_eq!(keyed_values[0].1.key, "key-7");
        assert_eq!(keyed_values[0].1.value, 7);
    }

    let process_source = Stream::new(&StreamConfig::new(4, "Process Input"), environment);
    let (values, errors) = process_source
        .process(&(StreamConfig::new(3, "Process").into()), EvenOrError)
        .unwrap();
    let values_capture = Arc::new(Capture::default());
    let errors_capture = Arc::new(Capture::default());
    values.set_consumer(Arc::clone(&values_capture), 5);
    errors.set_consumer(Arc::clone(&errors_capture), -5);
    process_source
        .emit(MessageContext::new(), Payload::new(2))
        .await;
    process_source
        .emit(MessageContext::new(), Payload::new(3))
        .await;

    assert_eq!(*values_capture.0.lock().unwrap()[0].1, 2);
    assert_eq!(&*errors_capture.0.lock().unwrap()[0].1, "odd: 3");
}

#[tokio::test]
async fn input_result_source_routes_pipeline_results_back_to_endpoint() {
    let environment = test_environment(Vec::new(), Vec::new());
    let input = servicelib::operators::InputStream::<i32, i32, String>::new(
        &InputStreamConfig {
            stream: StreamConfig::new(1, "Input"),
            endpoint_id: 10,
        },
        environment,
    );
    let mapped = input
        .stream()
        .map(&(StreamConfig::new(2, "Double").into()), Double)
        .unwrap();
    input.set_source(&mapped).unwrap();
    let generated_router = Arc::new(Capture::default());
    input.set_result_consumer(generated_router.clone());
    let capture = Arc::new(Capture::default());
    input.set_result_consumer(capture.clone());

    input.consume(MessageContext::new(), 21).await;

    assert_eq!(*capture.0.lock().unwrap()[0].1, 42);
    assert!(generated_router.0.lock().unwrap().is_empty());
    assert_eq!(input.result_stream().unwrap().id(), 2);
    assert_eq!(input.error_stream().id(), -1);
}

struct CountUntilThree;

#[async_trait]
impl MapFunction<i32, i32> for CountUntilThree {
    async fn map(
        &self,
        context: MessageContext,
        _stream: &dyn RuntimeStream,
        value: Payload<i32>,
        out: &Stream<i32>,
    ) {
        if *value < 3 {
            out.emit(context, Payload::new(*value + 1)).await;
        }
    }
}

#[tokio::test]
async fn link_stream_can_bind_its_source_after_graph_construction() {
    let environment = test_environment(Vec::new(), Vec::new());
    let input = Stream::new(&StreamConfig::new(1, "Input"), environment.clone());
    let link = LinkStream::make(&(StreamConfig::new(2, "Cycle Link").into()), environment);
    let counted = link
        .stream()
        .map(
            &(StreamConfig::new(3, "Count Until Three").into()),
            CountUntilThree,
        )
        .unwrap();
    let capture = Arc::new(Capture::default());
    counted.set_consumer(Arc::clone(&capture), 4);
    link.set_source(&input).unwrap();

    input.emit(MessageContext::new(), Payload::new(1)).await;

    assert_eq!(*capture.0.lock().unwrap()[0].1, 2);
    assert_eq!(link.source().unwrap().id(), input.id());
}

#[tokio::test]
async fn flat_map_iterable_needs_no_user_function_like_go() {
    let environment = test_environment(Vec::new(), Vec::new());
    let source = Stream::new(&StreamConfig::new(1, "Input"), environment);
    let items = source
        .flat_map_iterable::<i32>(&(StreamConfig::new(2, "Items").into()))
        .unwrap();
    let capture = Arc::new(Capture::default());
    items.set_consumer(Arc::clone(&capture), 3);

    source
        .emit(MessageContext::new(), Payload::new(vec![1, 2, 3]))
        .await;

    let captured = capture.0.lock().unwrap();
    assert_eq!(
        captured
            .iter()
            .map(|(_, value)| **value)
            .collect::<Vec<_>>(),
        vec![1, 2, 3]
    );
}

#[tokio::test]
async fn split_shares_payload_and_dispatches_async_branch_first() {
    let environment = test_environment(
        Vec::new(),
        vec![
            LinkConfig {
                from: 2,
                to: 3,
                call_semantics: CallSemantics::ParallelCall,
                r#async: false,
            },
            LinkConfig {
                from: 2,
                to: 4,
                call_semantics: CallSemantics::FunctionCall,
                r#async: false,
            },
        ],
    );
    let source = Stream::new(&StreamConfig::new(1, "Input"), environment);
    let [async_branch, direct_branch] = source
        .split(&(StreamConfig::new(2, "Split").into()))
        .unwrap();
    let async_capture = Arc::new(Capture::default());
    let direct_capture = Arc::new(Capture::default());
    async_branch.set_consumer(Arc::clone(&async_capture), 3);
    direct_branch.set_consumer(Arc::clone(&direct_capture), 4);
    let (expected_payload, payload) = Payload::new(String::from("message")).share();
    let expected = expected_payload.into_arc();

    source.environment().build_runtime_streams().unwrap();
    source.emit(MessageContext::new(), payload).await;
    tokio::task::yield_now().await;

    let async_value = async_capture.0.lock().unwrap()[0].1.clone();
    let direct_value = direct_capture.0.lock().unwrap()[0].1.clone();
    assert!(Arc::ptr_eq(&expected, &async_value));
    assert!(Arc::ptr_eq(&expected, &direct_value));
}

struct FixedDelay(Duration);

#[async_trait]
impl DelayFunction<i32> for FixedDelay {
    async fn duration(
        &self,
        _context: MessageContext,
        _stream: &dyn RuntimeStream,
        _value: Payload<i32>,
    ) -> Duration {
        self.0
    }
}

#[tokio::test]
async fn positive_delay_does_not_emit_after_context_cancellation() {
    let environment = test_environment(Vec::new(), Vec::new());
    let source = Stream::new(&StreamConfig::new(1, "Input"), environment);
    let delayed = source
        .delay(
            &(StreamConfig::new(2, "Delay").into()),
            FixedDelay(Duration::from_secs(60)),
        )
        .unwrap();
    let capture = Arc::new(Capture::default());
    delayed.set_consumer(Arc::clone(&capture), 3);
    let context = MessageContext::new();

    source.emit(context.clone(), Payload::new(1)).await;
    context.cancel();
    tokio::time::sleep(Duration::from_millis(10)).await;

    assert!(capture.0.lock().unwrap().is_empty());
}

#[tokio::test]
async fn zero_delay_emits_even_if_context_is_already_cancelled() {
    let environment = test_environment(Vec::new(), Vec::new());
    let source = Stream::new(&StreamConfig::new(1, "Input"), environment);
    let delayed = source
        .delay(
            &(StreamConfig::new(2, "Delay").into()),
            FixedDelay(Duration::ZERO),
        )
        .unwrap();
    let capture = Arc::new(Capture::default());
    delayed.set_consumer(Arc::clone(&capture), 3);
    let context = MessageContext::new();
    context.cancel();

    source.emit(context, Payload::new(1)).await;

    assert_eq!(capture.0.lock().unwrap().len(), 1);
}

#[derive(Serialize, Deserialize)]
enum Event {
    Number(i32),
    Text(String),
}

#[tokio::test]
async fn case_routes_to_the_selected_typed_branch() {
    let environment = test_environment(Vec::new(), Vec::new());
    let source = Stream::new(&StreamConfig::new(1, "Input"), environment);
    let cases = source
        .case(
            &(StreamConfig::new(2, "Case").into()),
            |event: &Event| match event {
                Event::Number(_) => 0,
                Event::Text(_) => 1,
            },
        )
        .unwrap();
    let numbers = cases.when(
        &(StreamConfig::new(3, "Number").into()),
        |event| match event {
            Event::Number(value) => *value,
            Event::Text(_) => unreachable!(),
        },
    );
    let texts = cases.when(
        &(StreamConfig::new(4, "Text").into()),
        |event| match event {
            Event::Text(value) => value.clone(),
            Event::Number(_) => unreachable!(),
        },
    );
    let number_capture = Arc::new(Capture::default());
    let text_capture = Arc::new(Capture::default());
    numbers.set_consumer(Arc::clone(&number_capture), 5);
    texts.set_consumer(Arc::clone(&text_capture), 6);
    source.environment().build_runtime_streams().unwrap();

    source
        .emit(MessageContext::new(), Payload::new(Event::Number(42)))
        .await;
    source
        .emit(
            MessageContext::new(),
            Payload::new(Event::Text("hello".to_owned())),
        )
        .await;

    assert_eq!(*number_capture.0.lock().unwrap()[0].1, 42);
    assert_eq!(&*text_capture.0.lock().unwrap()[0].1, "hello");
}

struct SumJoin;

#[async_trait]
impl JoinFunction<String, i32, i32, i32> for SumJoin {
    async fn join(
        &self,
        context: MessageContext,
        _stream: &dyn RuntimeStream,
        _key: String,
        left: Vec<Payload<i32>>,
        right: Vec<Payload<i32>>,
        out: &Stream<i32>,
    ) -> bool {
        out.emit(
            context,
            Payload::new(
                left.iter().map(|value| **value).sum::<i32>()
                    + right.iter().map(|value| **value).sum::<i32>(),
            ),
        )
        .await;
        true
    }
}

#[tokio::test]
async fn inner_join_waits_for_both_sides_and_removes_processed_key() {
    let join_config = JoinStreamConfig {
        join_storage: servicelib::api::JoinStorageType::HashMap,
        stream: StreamConfig::new(3, "Join"),
        join_type: JoinType::Inner,
        ttl: Duration::from_secs(1),
        renew_ttl: false,
    };
    let environment = test_environment(vec![join_config.clone().into()], Vec::new());
    let left = Stream::new(&StreamConfig::new(1, "Left"), environment.clone());
    let right = Stream::new(&StreamConfig::new(2, "Right"), environment);
    let joined = left.join(&join_config, &right, SumJoin).unwrap();
    let capture = Arc::new(Capture::default());
    joined.set_consumer(Arc::clone(&capture), 4);
    let context = MessageContext::new();

    left.emit(
        context.clone(),
        Payload::new(KeyValue {
            key: "order".to_owned(),
            value: 10,
        }),
    )
    .await;
    assert!(capture.0.lock().unwrap().is_empty());

    right
        .emit(
            context,
            Payload::new(KeyValue {
                key: "order".to_owned(),
                value: 7,
            }),
        )
        .await;
    assert_eq!(*capture.0.lock().unwrap()[0].1, 17);
}

struct EmitOnExpiry {
    calls: Mutex<usize>,
}

#[async_trait]
impl JoinFunction<String, i32, i32, usize> for EmitOnExpiry {
    async fn join(
        &self,
        context: MessageContext,
        _stream: &dyn RuntimeStream,
        _key: String,
        _left: Vec<Payload<i32>>,
        _right: Vec<Payload<i32>>,
        out: &Stream<usize>,
    ) -> bool {
        let call = {
            let mut calls = self.calls.lock().unwrap();
            *calls += 1;
            *calls
        };
        if call == 2 {
            out.emit(context, Payload::new(call)).await;
            true
        } else {
            false
        }
    }
}

#[tokio::test]
async fn join_invokes_the_same_callback_when_ttl_expires() {
    let join_config = JoinStreamConfig {
        join_storage: servicelib::api::JoinStorageType::HashMap,
        stream: StreamConfig::new(3, "Join"),
        join_type: JoinType::Outer,
        ttl: Duration::from_millis(10),
        renew_ttl: false,
    };
    let environment = test_environment(vec![join_config.clone().into()], Vec::new());
    let left = Stream::new(&StreamConfig::new(1, "Left"), environment.clone());
    let right = Stream::new(&StreamConfig::new(2, "Right"), environment);
    let joined = left
        .join(
            &join_config,
            &right,
            EmitOnExpiry {
                calls: Mutex::new(0),
            },
        )
        .unwrap();
    let capture = Arc::new(Capture::default());
    joined.set_consumer(Arc::clone(&capture), 4);

    left.emit(
        MessageContext::new(),
        Payload::new(KeyValue {
            key: "order".to_owned(),
            value: 10,
        }),
    )
    .await;
    tokio::time::sleep(Duration::from_millis(30)).await;

    assert_eq!(*capture.0.lock().unwrap()[0].1, 2);
}

struct ThreeWayJoin;

#[async_trait]
impl MultiJoinFunction<String, String> for ThreeWayJoin {
    async fn multi_join(
        &self,
        context: MessageContext,
        _stream: &dyn RuntimeStream,
        key: String,
        values: JoinValues,
        out: &Stream<String>,
    ) -> bool {
        let left = downcast_join_values::<i32>(&values, 0);
        let names = downcast_join_values::<String>(&values, 1);
        let enabled = downcast_join_values::<bool>(&values, 2);
        if names.is_empty() || enabled.is_empty() {
            return false;
        }
        out.emit(
            context,
            Payload::new(format!("{key}:{}:{}:{}", *left[0], *names[0], *enabled[0])),
        )
        .await;
        true
    }
}

#[tokio::test]
async fn multi_join_accepts_heterogeneous_typed_inputs() {
    let join_config = MultiJoinStreamConfig {
        join_storage: servicelib::api::JoinStorageType::HashMap,
        stream: StreamConfig::new(4, "MultiJoin"),
        ttl: Duration::from_secs(1),
        renew_ttl: false,
    };
    let environment = test_environment(vec![join_config.clone().into()], Vec::new());
    let left = Stream::new(&StreamConfig::new(1, "Left"), environment.clone());
    let names = Stream::new(&StreamConfig::new(2, "Names"), environment.clone());
    let flags = Stream::new(&StreamConfig::new(3, "Flags"), environment);
    let joined = left.multi_join(&join_config, ThreeWayJoin).unwrap();
    joined.add(&names).unwrap();
    joined.add(&flags).unwrap();
    let capture = Arc::new(Capture::default());
    joined.stream().set_consumer(Arc::clone(&capture), 5);
    let context = MessageContext::new();

    left.emit(
        context.clone(),
        Payload::new(KeyValue {
            key: "key".to_owned(),
            value: 10,
        }),
    )
    .await;
    names
        .emit(
            context.clone(),
            Payload::new(KeyValue {
                key: "key".to_owned(),
                value: "name".to_owned(),
            }),
        )
        .await;
    flags
        .emit(
            context,
            Payload::new(KeyValue {
                key: "key".to_owned(),
                value: true,
            }),
        )
        .await;

    assert_eq!(&*capture.0.lock().unwrap()[0].1, "key:10:name:true");
}
