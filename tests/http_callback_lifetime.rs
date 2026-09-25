use std::sync::{Arc, Mutex, Weak};

use async_trait::async_trait;
use axum::{body::Body, http::{Request, StatusCode}};
use servicelib::{
    MessageContext, Payload,
    api::HTTPMethodType,
    datasource::http::{AxumDataSource, EndpointHandler, HandlerData, HandlerError, HandlerResult, ResultCallback, ResultContext},
    operators::input::InputStream,
    runtime::{
        config::{CallSemantics, HttpDataConnectorConfig, HttpEndpointConfig, InputStreamConfig, RuntimeConfig, StreamConfig},
        datasource::StreamContext,
        environment::RuntimeEnvironment,
        stream::Stream,
    },
};
use tokio::sync::Mutex as AsyncMutex;
use tower::ServiceExt;

type HttpResult = ResultContext<(), (), (), u32, u32, String>;

#[derive(Default)]
struct Probe {
    weak: Mutex<Option<Weak<HttpResult>>>,
    retained: Mutex<Option<Arc<HttpResult>>>,
    keep_external_reference: bool,
    fail: bool,
    with_result: bool,
    expire: bool,
}

struct Handler(Arc<Probe>);

fn callback_capturing_result(result: Arc<HttpResult>) -> ResultCallback<(), (), (), u32, u32, String> {
    ResultCallback::new(move |_context, _stream, _state: Arc<AsyncMutex<()>>, _value: Payload<u32>, _data| {
        let result = result.clone();
        Box::pin(async move {
            result.done();
            false
        })
    })
}

#[async_trait]
impl EndpointHandler<(), (), (), u32, u32, String> for Handler {
    async fn begin_request(&self, context: MessageContext, _stream: StreamContext<u32, u32, String>,
        _data: HandlerData) -> Result<(MessageContext, ()), HandlerError> {
        let context = if self.0.expire {
            context.with_timeout_limit(std::time::Duration::from_millis(20))
        } else {
            context
        };
        Ok((context, ()))
    }

    async fn consume_message(&self, _context: MessageContext, _stream: StreamContext<u32, u32, String>,
        _state: Arc<AsyncMutex<()>>, _data: HandlerData, result: Arc<HttpResult>) -> HandlerResult {
        *self.0.weak.lock().unwrap() = Some(Arc::downgrade(&result));
        if self.0.keep_external_reference {
            *self.0.retained.lock().unwrap() = Some(result.clone());
        } else {
            result.set_result_callback("reply", callback_capturing_result(result.clone()));
        }
        if !self.0.expire {
            result.done();
        }
        if self.0.fail { Err("intentional consume failure".into()) } else { Ok(()) }
    }

    async fn get_message_id(&self, _context: &MessageContext, _stream: &StreamContext<u32, u32, String>,
        _state: Arc<AsyncMutex<()>>, _value: &u32) -> String {
        "reply".to_owned()
    }

    async fn end_request(&self, _context: MessageContext, _stream: StreamContext<u32, u32, String>,
        result: &HandlerResult, _state: Arc<AsyncMutex<()>>, data: HandlerData) {
        data.set_status(if result.is_err() { StatusCode::INTERNAL_SERVER_ERROR } else { StatusCode::OK });
    }
}

async fn complete_request(probe: Arc<Probe>) {
    let input_config = InputStreamConfig { stream: StreamConfig::new(1, "request"), endpoint_id: 4 };
    let connector = HttpDataConnectorConfig {
        id: 10, name: "http".to_owned(), host: "127.0.0.1".to_owned(), port: 9090,
        address: String::new(), use_dedicated_listener: false,
    };
    let endpoint = HttpEndpointConfig {
        id: 4, name: "request".to_owned(), id_data_connector: 10,
        tracing_enabled: false, http_method_type: HTTPMethodType::POST, path: "/request".to_owned(),
    };
    let environment = RuntimeEnvironment::default();
    environment.publish_runtime_config(Arc::new(RuntimeConfig::from_parts(
        CallSemantics::FunctionCall, [], [input_config.clone().into()], [],
        [connector.clone().into()], [endpoint.clone().into()], [],
    ).unwrap()));
    let input = InputStream::<u32, u32, String>::new(&input_config, environment.clone());
    if probe.with_result {
        let result_stream = Stream::<u32>::new(&StreamConfig::new(2, "result"), environment.clone());
        input.set_source(&result_stream).unwrap();
    }
    let source = AxumDataSource::new(environment, &connector);
    source.add_endpoint(input, endpoint, Handler(probe.clone())).unwrap();
    let response = source.router().oneshot(Request::post("/request").body(Body::empty()).unwrap())
        .await.unwrap();
    let expected = if probe.fail || probe.expire { StatusCode::INTERNAL_SERVER_ERROR } else { StatusCode::OK };
    assert_eq!(response.status(), expected);
}

#[tokio::test]
async fn completed_http_request_releases_callback_self_capture() {
    for with_result in [false, true] {
        let probe = Arc::new(Probe { with_result, ..Probe::default() });
        complete_request(probe.clone()).await;
        assert!(probe.weak.lock().unwrap().as_ref().unwrap().upgrade().is_none(),
            "completed HTTP callback still owns its ResultContext; with_result={with_result}");
    }
}

#[tokio::test]
async fn failed_http_request_releases_callback_self_capture() {
    for with_result in [false, true] {
        let probe = Arc::new(Probe { fail: true, with_result, ..Probe::default() });
        complete_request(probe.clone()).await;
        assert!(probe.weak.lock().unwrap().as_ref().unwrap().upgrade().is_none(),
            "failed HTTP callback still owns its ResultContext; with_result={with_result}");
    }
}

#[tokio::test]
async fn completed_http_request_cannot_retain_new_callbacks() {
    for with_result in [false, true] {
        let probe = Arc::new(Probe { keep_external_reference: true, with_result, ..Probe::default() });
        complete_request(probe.clone()).await;
        let result = probe.retained.lock().unwrap().take().unwrap();
        result.set_result_callback("late", callback_capturing_result(result.clone()));
        drop(result);
        assert!(probe.weak.lock().unwrap().as_ref().unwrap().upgrade().is_none(),
            "late HTTP callback recreated a completed-context cycle; with_result={with_result}");
    }
}

#[tokio::test]
async fn timed_out_http_request_releases_callback_self_capture() {
    let probe = Arc::new(Probe { with_result: true, expire: true, ..Probe::default() });
    complete_request(probe.clone()).await;
    assert!(probe.weak.lock().unwrap().as_ref().unwrap().upgrade().is_none(),
        "timed-out HTTP callback still owns its ResultContext");
}
