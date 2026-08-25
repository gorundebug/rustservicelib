use std::sync::Arc;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use servicelib::{
    MessageContext, Payload,
    operators::InputStream,
    runtime::{
        common::Consumer,
        config::{
            Config, HttpDataConnectorConfig, HttpEndpointConfig, InputStreamConfig,
            MapStreamConfig, RuntimeConfig, RuntimeDataConnectorConfig, RuntimeEndpointConfig,
            RuntimeStreamConfig, ServiceConfig, StreamConfig,
        },
        environment::RuntimeEnvironment,
        serviceapp::ServiceApp,
        statusweb::{graph_yaml, network_data, status_html, vis_css, vis_js},
        stream::Stream,
    },
};

#[derive(Clone, Serialize, Deserialize)]
struct StatusConfig;

impl Config for StatusConfig {
    fn apply_environment(&mut self) -> Result<(), String> {
        Ok(())
    }

    fn services(&self) -> Vec<ServiceConfig> {
        vec![ServiceConfig {
            id: 7,
            name: "Status Service".to_owned(),
            color: "#D2E5FF".to_owned(),
            ..ServiceConfig::default()
        }]
    }

    fn streams(&self) -> Vec<RuntimeStreamConfig> {
        vec![
            InputStreamConfig {
                stream: StreamConfig::new(1, "Input").with_graph(
                    7,
                    0,
                    [],
                    Some("i32"),
                    None::<String>,
                    10.0,
                    20.0,
                ),
                endpoint_id: 10,
            }
            .into(),
            MapStreamConfig::from(StreamConfig::new(2, "Value").with_graph(
                7,
                1,
                [],
                Some("i32"),
                None::<String>,
                30.0,
                40.0,
            ))
            .into(),
            MapStreamConfig::from(StreamConfig::new(3, "Error Handler").with_graph(
                7,
                -1,
                [],
                Some("String"),
                None::<String>,
                50.0,
                60.0,
            ))
            .into(),
            servicelib::runtime::config::FlatMapStreamConfig::from(
                StreamConfig::new(4, "Flat Map").with_graph(
                    7,
                    2,
                    [],
                    Some("i32"),
                    None::<String>,
                    70.0,
                    80.0,
                ),
            )
            .into(),
        ]
    }

    fn data_connectors(&self) -> Vec<RuntimeDataConnectorConfig> {
        vec![
            HttpDataConnectorConfig {
                id: 5,
                name: "HTTP".to_owned(),
                host: "127.0.0.1".to_owned(),
                port: 8080,
                address: String::new(),
                use_dedicated_listener: false,
            }
            .into(),
        ]
    }

    fn endpoints(&self) -> Vec<RuntimeEndpointConfig> {
        vec![
            HttpEndpointConfig {
                id: 10,
                name: "Input".to_owned(),
                id_data_connector: 5,
                tracing_enabled: false,
                http_method_type: servicelib::api::HTTPMethodType::POST,
                path: "/input".to_owned(),
            }
            .into(),
        ]
    }
}

#[tokio::test]
async fn service_app_serves_the_complete_status_surface() {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    drop(listener);
    let service = ServiceConfig {
        id: 9,
        name: "HTTP Status".to_owned(),
        http_host: "127.0.0.1".to_owned(),
        http_port: port,
        status_handler: "status".to_owned(),
        metrics_handler: "metrics".to_owned(),
        ..ServiceConfig::default()
    };
    let environment = RuntimeEnvironment::default();
    environment.publish_runtime_config(Arc::new(
        RuntimeConfig::from_parts(
            servicelib::runtime::config::CallSemantics::FunctionCall,
            [service.clone()],
            [],
            [],
            [],
            [],
            [],
        )
        .unwrap(),
    ));
    let app = ServiceApp::new(environment, service).unwrap();
    app.start(MessageContext::new()).await.unwrap();
    let client = reqwest::Client::new();
    for (path, content_type) in [
        ("/status", "text/html"),
        ("/status/data", "application/json"),
        ("/status/graph", "text/yaml"),
        ("/status/vis.min.js", "application/javascript"),
        ("/status/vis.min.css", "text/css"),
        ("/metrics", "text/plain"),
    ] {
        let response = client
            .get(format!("http://127.0.0.1:{port}{path}"))
            .send()
            .await
            .unwrap();
        assert!(response.status().is_success(), "{path}");
        assert!(
            response
                .headers()
                .get(reqwest::header::CONTENT_TYPE)
                .unwrap()
                .to_str()
                .unwrap()
                .starts_with(content_type),
            "{path}"
        );
    }
    app.stop(MessageContext::new()).await.unwrap();
}

struct Ignore;

#[async_trait]
impl<T> Consumer<T> for Ignore
where
    T: Send + Sync + 'static,
{
    async fn consume(&self, _context: MessageContext, _payload: Payload<T>) {}
}

#[tokio::test]
async fn status_graph_matches_the_go_runtime_contract() {
    let environment = RuntimeEnvironment::default();
    environment.publish_runtime_config(Arc::new(RuntimeConfig::new(&StatusConfig).unwrap()));
    let environment = environment.for_service(7);
    let input = InputStream::<i32, (), String>::new(
        &InputStreamConfig {
            stream: StreamConfig::new(1, "constructor config"),
            endpoint_id: 10,
        },
        environment.clone(),
    );
    let _value = Stream::<i32>::new(&StreamConfig::new(2, "value"), environment.clone());
    let _error = Stream::<String>::new(&StreamConfig::new(3, "error"), environment.clone());
    let _flat_map = Stream::<i32>::new(&StreamConfig::new(4, "flat map"), environment.clone());
    input.stream().set_consumer(Arc::new(Ignore), 2);
    input.error_stream().set_consumer(Arc::new(Ignore), 3);
    input.consume(MessageContext::new(), 1).await;

    let data = network_data(&environment);
    assert!(data.nodes.iter().any(|node| {
        node.id == 1
            && node.label == "Input(INPUT)\n[Status Service]"
            && node.x == 10.0
            && node.image.unselected.starts_with("data:image/svg+xml")
            && node.image.unselected.contains("rx=%2230%22")
    }));
    assert!(data.nodes.iter().any(|node| node.id == -1));
    assert!(data.nodes.iter().any(|node| node.id == 4
        && node.label.contains("(FLATMAP)")
        && node.image.unselected.contains("rx=%2210%22")));
    assert!(
        data.edges
            .iter()
            .any(|edge| { edge.from == 1 && edge.to == 2 && edge.label == "i32\ncalls: 1" })
    );
    assert!(
        data.edges
            .iter()
            .any(|edge| edge.from == 1 && edge.to == -1 && edge.color.color == "#FF3030")
    );
    assert!(graph_yaml(&environment).unwrap().contains("Status Service"));
    assert!(status_html().contains("/vis.min.js"));
    assert!(vis_js().len() > 600_000);
    assert!(vis_css().contains(".vis-network"));
}
