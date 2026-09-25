use std::{
    collections::HashMap,
    io,
    pin::Pin,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    task::{Context, Poll},
    time::Duration,
};

use async_trait::async_trait;
use servicelib::{
    MessageContext, Payload, Stream,
    api::HTTPMethodType,
    datasink::http::{
        Client, EndpointHandler, HandlerError, HandlerResult, Request, Requester, Response,
        ResponseBody, StreamContext, make_endpoint_consumer,
    },
    runtime::{
        config::{
            CallSemantics, HttpDataConnectorConfig, HttpEndpointConfig, RuntimeConfig,
            SinkStreamConfig, StreamConfig,
        },
        environment::RuntimeEnvironment,
    },
};
use tokio::{
    io::{AsyncRead, ReadBuf},
    sync::Semaphore,
    task::JoinHandle,
};

#[derive(Clone, Copy)]
enum Scenario {
    Retain,
    RetainAndFail,
    ReadFailure,
}

struct Observations {
    polled: Semaphore,
    end_entered: Semaphore,
    end_release: Semaphore,
    reader_drops: AtomicUsize,
    end_count: AtomicUsize,
    error: Mutex<Option<String>>,
    reader_task: Mutex<Option<JoinHandle<io::Result<Vec<u8>>>>>,
}

impl Observations {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            polled: Semaphore::new(0),
            end_entered: Semaphore::new(0),
            end_release: Semaphore::new(0),
            reader_drops: AtomicUsize::new(0),
            end_count: AtomicUsize::new(0),
            error: Mutex::new(None),
            reader_task: Mutex::new(None),
        })
    }
}

struct Reader {
    observations: Arc<Observations>,
    fail: bool,
}

impl AsyncRead for Reader {
    fn poll_read(
        self: Pin<&mut Self>,
        _: &mut Context<'_>,
        _: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        self.observations.polled.add_permits(1);
        if self.fail {
            Poll::Ready(Err(io::Error::new(
                io::ErrorKind::ConnectionReset,
                "response body reset",
            )))
        } else {
            Poll::Pending
        }
    }
}

impl Drop for Reader {
    fn drop(&mut self) {
        self.observations
            .reader_drops
            .fetch_add(1, Ordering::SeqCst);
    }
}

struct TestClient {
    observations: Arc<Observations>,
    scenario: Scenario,
}

#[async_trait]
impl Client for TestClient {
    async fn perform(&self, _: MessageContext, _: Request) -> Result<Response, HandlerError> {
        Ok(Response {
            status: 200,
            headers: HashMap::new(),
            body: ResponseBody::new(Reader {
                observations: self.observations.clone(),
                fail: matches!(self.scenario, Scenario::ReadFailure),
            }),
        })
    }
}

struct Handler {
    observations: Arc<Observations>,
    scenario: Scenario,
}

#[async_trait]
impl EndpointHandler<(), u32, u32, String> for Handler {
    async fn begin_request(
        &self,
        context: MessageContext,
        _: StreamContext<u32, u32, String>,
    ) -> Result<(MessageContext, ()), HandlerError> {
        Ok((context, ()))
    }

    async fn consume_message(
        &self,
        _: MessageContext,
        _: StreamContext<u32, u32, String>,
        _: &mut (),
        _: Payload<u32>,
        requester: &mut Requester,
    ) -> HandlerResult {
        requester.new_request("GET", "http://fixture/body", Vec::new());
        Ok(())
    }

    async fn handle_response(
        &self,
        _: MessageContext,
        _: StreamContext<u32, u32, String>,
        _: &mut (),
        mut response: Response,
    ) -> HandlerResult {
        if matches!(self.scenario, Scenario::ReadFailure) {
            response.body.bytes().await?;
            panic!("read failure must propagate");
        }
        let task = tokio::spawn(async move { response.body.bytes().await });
        *self.observations.reader_task.lock().unwrap() = Some(task);
        self.observations.polled.acquire().await.unwrap().forget();
        if matches!(self.scenario, Scenario::RetainAndFail) {
            Err("handler rejected response".into())
        } else {
            Ok(())
        }
    }

    async fn end_request(
        &self,
        _: MessageContext,
        _: StreamContext<u32, u32, String>,
        result: &HandlerResult,
        _: (),
    ) {
        *self.observations.error.lock().unwrap() = result.as_ref().err().map(ToString::to_string);
        self.observations.end_count.fetch_add(1, Ordering::SeqCst);
        self.observations.end_entered.add_permits(1);
        self.observations
            .end_release
            .acquire()
            .await
            .unwrap()
            .forget();
    }
}

async fn check(scenario: Scenario) {
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
            [],
            [],
            [],
        )
        .unwrap(),
    ));
    let source = Stream::new(&StreamConfig::new(1, "source"), environment);
    let sink = source
        .sink_with_result::<u32, String>(&sink_config)
        .unwrap();
    let observations = Observations::new();
    make_endpoint_consumer(
        &sink,
        HttpEndpointConfig {
            id: 3,
            name: "body".to_owned(),
            id_data_connector: 4,
            tracing_enabled: false,
            http_method_type: HTTPMethodType::GET,
            path: "/body".to_owned(),
        },
        HttpDataConnectorConfig {
            id: 4,
            name: "fixture".to_owned(),
            host: "fixture".to_owned(),
            port: 80,
            address: "http://fixture".to_owned(),
            use_dedicated_listener: false,
        },
        Arc::new(TestClient {
            observations: observations.clone(),
            scenario,
        }),
        Arc::new(Handler {
            observations: observations.clone(),
            scenario,
        }),
    )
    .unwrap();

    let call = tokio::spawn(async move {
        source
            .emit(MessageContext::new(), Payload::new(7_u32))
            .await
    });
    tokio::time::timeout(Duration::from_secs(2), observations.end_entered.acquire())
        .await
        .expect("EndRequest must be reached")
        .unwrap()
        .forget();
    assert!(!call.is_finished(), "Consume must still await EndRequest");
    let expected_error = match scenario {
        Scenario::Retain => None,
        Scenario::RetainAndFail => Some("handler rejected response"),
        Scenario::ReadFailure => Some("response body reset"),
    };
    assert_eq!(
        observations.error.lock().unwrap().as_deref(),
        expected_error
    );
    let expected_drops = usize::from(matches!(scenario, Scenario::ReadFailure));
    assert_eq!(
        observations.reader_drops.load(Ordering::SeqCst),
        expected_drops
    );
    if !matches!(scenario, Scenario::ReadFailure) {
        assert!(
            !observations
                .reader_task
                .lock()
                .unwrap()
                .as_ref()
                .unwrap()
                .is_finished()
        );
    }

    observations.end_release.add_permits(1);
    tokio::time::timeout(Duration::from_secs(2), call)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(observations.end_count.load(Ordering::SeqCst), 1);
    assert_eq!(observations.reader_drops.load(Ordering::SeqCst), 1);
    let reader_task = observations.reader_task.lock().unwrap().take();
    if let Some(reader_task) = reader_task {
        let error = tokio::time::timeout(Duration::from_secs(2), reader_task)
            .await
            .expect("endpoint closure must wake pending body read")
            .unwrap()
            .unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::BrokenPipe);
    }
}

#[tokio::test]
async fn successful_endpoint_closes_and_wakes_retained_pending_body() {
    check(Scenario::Retain).await;
}

#[tokio::test]
async fn failed_handler_closes_and_wakes_retained_pending_body() {
    check(Scenario::RetainAndFail).await;
}

#[tokio::test]
async fn body_read_failure_reaches_end_request_before_consume_returns() {
    check(Scenario::ReadFailure).await;
}
