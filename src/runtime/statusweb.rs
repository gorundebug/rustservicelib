use std::collections::HashSet;

use serde::Serialize;

use crate::{
    api::{DataConnectorType, TransformationType},
    runtime::{
        config::{RuntimeConfig, RuntimeStreamConfig},
        environment::RuntimeEnvironment,
    },
};

const STATUS_HTML: &str = include_str!("web/status.html");
const VIS_JS: &str = include_str!("web/vis.min.js");
const VIS_CSS: &str = include_str!("web/vis.min.css");

const MDI_DATABASE_ARROW_RIGHT: &str = "M4 7C4 4.79 7.58 3 12 3S20 4.79 20 7 16.42 11 12 11 4 9.21 4 7M19.72 13.05C19.9 12.71 20 12.36 20 12V9C20 11.21 16.42 13 12 13S4 11.21 4 9V12C4 14.21 7.58 16 12 16C12.65 16 13.28 15.96 13.88 15.89C14.93 14.16 16.83 13 19 13C19.24 13 19.5 13 19.72 13.05M13.1 17.96C12.74 18 12.37 18 12 18C7.58 18 4 16.21 4 14V17C4 19.21 7.58 21 12 21C12.46 21 12.9 21 13.33 20.94C13.12 20.33 13 19.68 13 19C13 18.64 13.04 18.3 13.1 17.96M23 19L20 16V18H16V20H20V22L23 19Z";
const MDI_ARROW_LEFT_RIGHT: &str = "M6.45,17.45L1,12L6.45,6.55L7.86,7.96L4.83,11H19.17L16.14,7.96L17.55,6.55L23,12L17.55,17.45L16.14,16.04L19.17,13H4.83L7.86,16.04L6.45,17.45Z";
const MDI_FILTER: &str = "M14,12V19.88C14.04,20.18 13.94,20.5 13.71,20.71C13.32,21.1 12.69,21.1 12.3,20.71L10.29,18.7C10.06,18.47 9.96,18.16 10,17.87V12H9.97L4.21,4.62C3.87,4.19 3.95,3.56 4.38,3.22C4.57,3.08 4.78,3 5,3V3H19V3C19.22,3 19.43,3.08 19.62,3.22C20.05,3.56 20.13,4.19 19.79,4.62L14.03,12H14Z";
const MDI_CALL_MERGE: &str =
    "M17,20.41L18.41,19L15,15.59L13.59,17M7.5,8H11V13.59L5.59,19L7,20.41L13,14.41V8H16.5L12,3.5";
const MDI_FUNCTION: &str = "M15.6,5.29C14.5,5.19 13.53,6 13.43,7.11L13.18,10H16V12H13L12.56,17.07C12.37,19.27 10.43,20.9 8.23,20.7C6.92,20.59 5.82,19.86 5.17,18.83L6.67,17.33C6.91,18.07 7.57,18.64 8.4,18.71C9.5,18.81 10.47,18 10.57,16.89L11,12H8V10H11.17L11.44,6.93C11.63,4.73 13.57,3.1 15.77,3.3C17.08,3.41 18.18,4.14 18.83,5.17L17.33,6.67C17.09,5.93 16.43,5.36 15.6,5.29Z";
const MDI_TRANSIT: &str = "M18,11H14.82C14.4,9.84 13.3,9 12,9C10.7,9 9.6,9.84 9.18,11H6C5.67,11 4,10.9 4,9V8C4,6.17 5.54,6 6,6H16.18C16.6,7.16 17.7,8 19,8A3,3 0 0,0 22,5A3,3 0 0,0 19,2C17.7,2 16.6,2.84 16.18,4H6C4.39,4 2,5.06 2,8V9C2,11.94 4.39,13 6,13H9.18C9.6,14.16 10.7,15 12,15C13.3,15 14.4,14.16 14.82,13H18C18.33,13 20,13.1 20,15V16C20,17.83 18.46,18 18,18H7.82C7.4,16.84 6.3,16 5,16A3,3 0 0,0 2,19A3,3 0 0,0 5,22C6.3,22 7.4,21.16 7.82,20H18C19.61,20 22,18.93 22,16V15C22,12.07 19.61,11 18,11M19,4A1,1 0 0,1 20,5A1,1 0 0,1 19,6A1,1 0 0,1 18,5A1,1 0 0,1 19,4M5,20A1,1 0 0,1 4,19A1,1 0 0,1 5,18A1,1 0 0,1 6,19A1,1 0 0,1 5,20Z";
const MDI_KEY: &str = "M7 14C5.9 14 5 13.1 5 12S5.9 10 7 10 9 10.9 9 12 8.1 14 7 14M12.6 10C11.8 7.7 9.6 6 7 6C3.7 6 1 8.7 1 12S3.7 18 7 18C9.6 18 11.8 16.3 12.6 14H16V18H20V14H23V10H12.6Z";
const MDI_MERGE: &str = "M8 17L12 13H15.2C15.6 14.2 16.7 15 18 15C19.7 15 21 13.7 21 12S19.7 9 18 9C16.7 9 15.6 9.8 15.2 11H12L8 7V3H3V8H6L10.2 12L6 16H3V21H8V17Z";
const MDI_SPLIT: &str = "M14,4L16.29,6.29L13.41,9.17L14.83,10.59L17.71,7.71L20,10V4M10,4H4V10L6.29,7.71L11,12.41V20H13V11.59L7.71,6.29";
const MDI_BRANCH: &str = "M6,2A3,3 0 0,1 9,5C9,6.28 8.19,7.38 7.06,7.81C7.15,8.27 7.39,8.83 8,9.63C9,10.92 11,12.83 12,14.17C13,12.83 15,10.92 16,9.63C16.61,8.83 16.85,8.27 16.94,7.81C15.81,7.38 15,6.28 15,5A3,3 0 0,1 18,2A3,3 0 0,1 21,5C21,6.32 20.14,7.45 18.95,7.85C18.87,8.37 18.64,9 18,9.83C17,11.17 15,13.08 14,14.38C13.39,15.17 13.15,15.73 13.06,16.19C14.19,16.62 15,17.72 15,19A3,3 0 0,1 12,22A3,3 0 0,1 9,19C9,17.72 9.81,16.62 10.94,16.19C10.85,15.73 10.61,15.17 10,14.38C9,13.08 7,11.17 6,9.83C5.36,9 5.13,8.37 5.05,7.85C3.86,7.45 3,6.32 3,5A3,3 0 0,1 6,2M6,4A1,1 0 0,0 5,5A1,1 0 0,0 6,6A1,1 0 0,0 7,5A1,1 0 0,0 6,4M18,4A1,1 0 0,0 17,5A1,1 0 0,0 18,6A1,1 0 0,0 19,5A1,1 0 0,0 18,4M12,18A1,1 0 0,0 11,19A1,1 0 0,0 12,20A1,1 0 0,0 13,19A1,1 0 0,0 12,18Z";
const MDI_DATABASE_ARROW_LEFT: &str = "M4 7C4 4.79 7.58 3 12 3S20 4.79 20 7 16.42 11 12 11 4 9.21 4 7M19.72 13.05C19.9 12.71 20 12.36 20 12V9C20 11.21 16.42 13 12 13S4 11.21 4 9V12C4 14.21 7.58 16 12 16C12.65 16 13.28 15.96 13.88 15.89C14.93 14.16 16.83 13 19 13C19.24 13 19.5 13 19.72 13.05M13.1 17.96C12.74 18 12.37 18 12 18C7.58 18 4 16.21 4 14V17C4 19.21 7.58 21 12 21C12.46 21 12.9 21 13.33 20.94C13.12 20.33 13 19.68 13 19C13 18.64 13.04 18.3 13.1 17.96M18 18V16L15 19L18 22V20H22V18H18Z";
const MDI_SYNC: &str = "M12,18A6,6 0 0,1 6,12C6,11 6.25,10.03 6.7,9.2L5.24,7.74C4.46,8.97 4,10.43 4,12A8,8 0 0,0 12,20V23L16,19L12,15M12,4V1L8,5L12,9V6A6,6 0 0,1 18,12C18,13 17.75,13.97 17.3,14.8L18.76,16.26C19.54,15.03 20,13.57 20,12A8,8 0 0,0 12,4Z";
const MDI_ALERT: &str = "M13,13H11V7H13M13,17H11V15H13M12,2A10,10 0 0,0 2,12A10,10 0 0,0 12,22A10,10 0 0,0 22,12A10,10 0 0,0 12,2Z";
const MDI_TIMER: &str = "M19.03 7.39L20.45 5.97C20 5.46 19.55 5 19.04 4.56L17.62 6C16.07 4.74 14.12 4 12 4C7.03 4 3 8.03 3 13S7.03 22 12 22C17 22 21 17.97 21 13C21 10.88 20.26 8.93 19.03 7.39M13 14H11V7H13V14M15 1H9V3H15V1Z";
const MDI_WHEN: &str = "M13,14C9.64,14 8.54,15.35 8.18,16.24C9.25,16.7 10,17.76 10,19A3,3 0 0,1 7,22A3,3 0 0,1 4,19C4,17.69 4.83,16.58 6,16.17V7.83C4.83,7.42 4,6.31 4,5A3,3 0 0,1 7,2A3,3 0 0,1 10,5C10,6.31 9.17,7.42 8,7.83V13.12C8.88,12.47 10.16,12 12,12C14.67,12 15.56,10.66 15.85,9.77C14.77,9.32 14,8.25 14,7A3,3 0 0,1 17,4A3,3 0 0,1 20,7C20,8.34 19.12,9.5 17.91,9.86C17.65,11.29 16.68,14 13,14M7,18A1,1 0 0,0 6,19A1,1 0 0,0 7,20A1,1 0 0,0 8,19A1,1 0 0,0 7,18M7,4A1,1 0 0,0 6,5A1,1 0 0,0 7,6A1,1 0 0,0 8,5A1,1 0 0,0 7,4M17,6A1,1 0 0,0 16,7A1,1 0 0,0 17,8A1,1 0 0,0 18,7A1,1 0 0,0 17,6Z";
const MDI_API: &str = "M7 7H5A2 2 0 0 0 3 9V17H5V13H7V17H9V9A2 2 0 0 0 7 7M7 11H5V9H7M14 7H10V17H12V13H14A2 2 0 0 0 16 11V9A2 2 0 0 0 14 7M14 11H12V9H14M20 9V15H21V17H17V15H18V9H17V7H21V9Z";
const MDI_CALL_MADE: &str = "M9,5V7H15.59L4,18.59L5.41,20L17,8.41V15H19V5";

const EDGE_COLOR: &str = "#0050FF";
const ERROR_COLOR: &str = "#FF3030";

#[derive(Clone, Debug, Serialize)]
pub struct NetworkData {
    pub nodes: Vec<Node>,
    pub edges: Vec<Edge>,
}

#[derive(Clone, Debug, Serialize)]
pub struct Node {
    pub id: i32,
    pub label: String,
    pub shape: &'static str,
    pub image: NodeImage,
    pub size: i32,
    pub color: NodeColor,
    pub opacity: f32,
    pub x: f64,
    pub y: f64,
}

#[derive(Clone, Debug, Serialize)]
pub struct NodeImage {
    pub unselected: String,
    pub selected: String,
}

#[derive(Clone, Debug, Serialize)]
pub struct NodeColor {
    pub border: &'static str,
    pub highlight: Highlight,
}

#[derive(Clone, Debug, Serialize)]
pub struct Highlight {
    pub border: &'static str,
}

#[derive(Clone, Debug, Serialize)]
pub struct Edge {
    pub from: i32,
    pub to: i32,
    pub arrows: &'static str,
    pub length: i32,
    pub label: String,
    pub color: EdgeColor,
}

#[derive(Clone, Debug, Serialize)]
pub struct EdgeColor {
    pub opacity: f32,
    pub color: &'static str,
}

pub fn status_html() -> &'static str {
    STATUS_HTML
}

pub fn vis_js() -> &'static str {
    VIS_JS
}

pub fn vis_css() -> &'static str {
    VIS_CSS
}

pub fn network_data(environment: &RuntimeEnvironment) -> NetworkData {
    let runtime = environment.runtime_config();
    let service_id = environment.service_id().unwrap_or_default();
    let registered_streams = environment.runtime_stream_ids();
    let mut nodes = runtime
        .streams()
        .into_iter()
        .filter(|config| {
            registered_streams.contains(&config.stream().id)
                && (config.stream().id_service == 0 || config.stream().id_service == service_id)
        })
        .map(|config| make_node(&runtime, service_id, &config))
        .collect::<Vec<_>>();
    let links = environment.graph_links();
    let mut edges = Vec::with_capacity(links.len() * 2);
    let mut error_nodes = HashSet::new();
    for link in links {
        if link.from < 0 {
            let producer_id = -link.from;
            if error_nodes.insert(link.from) {
                if let Some(producer) = runtime.stream_by_id(producer_id) {
                    nodes.push(make_error_node(&runtime, service_id, link.from, &producer));
                }
                edges.push(make_edge(
                    producer_id,
                    link.from,
                    &link.type_name,
                    0,
                    ERROR_COLOR,
                    None,
                ));
            }
        }
        let join_side = runtime.stream_by_id(link.to).and_then(|target| {
            matches!(
                target.transformation_type(),
                TransformationType::Join | TransformationType::MultiJoin
            )
            .then(|| {
                if target.stream().id_source == link.from {
                    "L"
                } else {
                    "R"
                }
            })
        });
        edges.push(make_edge(
            link.from,
            link.to,
            &link.type_name,
            link.calls.get(),
            EDGE_COLOR,
            join_side,
        ));
    }
    nodes.sort_by_key(|node| node.id);
    NetworkData { nodes, edges }
}

pub fn graph_yaml(environment: &RuntimeEnvironment) -> Result<String, serde_yaml::Error> {
    let runtime = environment.runtime_config();
    #[derive(Serialize)]
    struct Graph {
        services: Vec<crate::runtime::config::ServiceConfig>,
        streams: Vec<RuntimeStreamConfig>,
        links: Vec<crate::runtime::config::LinkConfig>,
        data_connectors: Vec<crate::runtime::config::RuntimeDataConnectorConfig>,
        endpoints: Vec<crate::runtime::config::RuntimeEndpointConfig>,
        pools: Vec<crate::runtime::config::PoolConfig>,
    }
    serde_yaml::to_string(&Graph {
        services: runtime
            .services()
            .iter()
            .map(|value| (**value).clone())
            .collect(),
        streams: runtime
            .streams()
            .iter()
            .map(|value| (**value).clone())
            .collect(),
        links: runtime
            .links()
            .iter()
            .map(|value| (**value).clone())
            .collect(),
        data_connectors: runtime
            .data_connectors()
            .iter()
            .map(|value| (**value).clone())
            .collect(),
        endpoints: runtime
            .endpoints()
            .iter()
            .map(|value| (**value).clone())
            .collect(),
        pools: runtime
            .pools()
            .iter()
            .map(|value| (**value).clone())
            .collect(),
    })
}

fn make_node(runtime: &RuntimeConfig, service_id: i32, config: &RuntimeStreamConfig) -> Node {
    let stream = config.stream();
    let service = runtime
        .service_by_id(if stream.id_service == 0 {
            service_id
        } else {
            stream.id_service
        })
        .or_else(|| runtime.service_by_id(service_id));
    let service_name = service
        .as_ref()
        .map_or_else(String::new, |service| service.name.clone());
    let background = service
        .as_ref()
        .map_or("#0050FF", |service| service.color.as_str());
    let icon = status_icon(runtime, config);
    Node {
        id: stream.id,
        label: format!(
            "{}({})\n[{}]",
            stream.name,
            transformation_name(config.transformation_type()),
            service_name
        ),
        shape: "image",
        image: node_image(icon, background),
        size: 30,
        color: transparent_color(),
        opacity: 1.0,
        x: stream.x_pos,
        y: stream.y_pos,
    }
}

fn make_error_node(
    runtime: &RuntimeConfig,
    service_id: i32,
    virtual_id: i32,
    producer: &RuntimeStreamConfig,
) -> Node {
    let mut node = make_node(runtime, service_id, producer);
    node.id = virtual_id;
    node.label = format!(
        "{} Error(ERROR)\n[{}]",
        producer.stream().name,
        runtime
            .service_by_id(service_id)
            .map_or_else(String::new, |service| service.name.clone())
    );
    let background = runtime
        .service_by_id(service_id)
        .map_or_else(|| "#0050FF".to_owned(), |service| service.color.clone());
    node.image = node_image(MDI_ALERT, &background);
    node.x = 0.0;
    node.y = 0.0;
    node
}

fn make_edge(
    from: i32,
    to: i32,
    type_name: &str,
    calls: i64,
    color: &'static str,
    join_side: Option<&str>,
) -> Edge {
    let mut label = type_name.to_owned();
    if calls > 0 {
        label.push_str(&format!("\ncalls: {calls}"));
    }
    if let Some(side) = join_side {
        label.push_str(&format!(" ({side})"));
    }
    Edge {
        from,
        to,
        arrows: "to",
        length: 200,
        label,
        color: EdgeColor {
            opacity: 1.0,
            color,
        },
    }
}

fn status_icon(runtime: &RuntimeConfig, config: &RuntimeStreamConfig) -> &'static str {
    if let Some(endpoint_id) = config.endpoint_id()
        && let Some(endpoint) = runtime.endpoint_by_id(endpoint_id)
        && let Some(connector) = runtime.data_connector_by_id(endpoint.data_connector_id())
        && matches!(
            connector.connector_type(),
            DataConnectorType::HTTP | DataConnectorType::GRPC
        )
    {
        return if config.transformation_type() == TransformationType::Sink {
            MDI_CALL_MADE
        } else {
            MDI_API
        };
    }
    match config.transformation_type() {
        TransformationType::Input => MDI_DATABASE_ARROW_RIGHT,
        TransformationType::Map => MDI_ARROW_LEFT_RIGHT,
        TransformationType::Filter => MDI_FILTER,
        TransformationType::Join | TransformationType::MultiJoin => MDI_CALL_MERGE,
        TransformationType::Process => MDI_FUNCTION,
        TransformationType::FlatMap | TransformationType::FlatMapIterable => MDI_TRANSIT,
        TransformationType::KeyBy => MDI_KEY,
        TransformationType::Merge => MDI_MERGE,
        TransformationType::Split => MDI_SPLIT,
        TransformationType::Case => MDI_BRANCH,
        TransformationType::Sink => MDI_DATABASE_ARROW_LEFT,
        TransformationType::CycleLink => MDI_SYNC,
        TransformationType::Error => MDI_ALERT,
        TransformationType::Delay => MDI_TIMER,
        TransformationType::When => MDI_WHEN,
        TransformationType::Undefined => MDI_FUNCTION,
    }
}

fn transformation_name(value: TransformationType) -> &'static str {
    match value {
        TransformationType::Undefined => "UNDEFINED",
        TransformationType::Input => "INPUT",
        TransformationType::Map => "MAP",
        TransformationType::Filter => "FILTER",
        TransformationType::Join => "JOIN",
        TransformationType::MultiJoin => "MULTIJOIN",
        TransformationType::Process => "PROCESS",
        TransformationType::FlatMap => "FLATMAP",
        TransformationType::FlatMapIterable => "FLATMAPITERABLE",
        TransformationType::KeyBy => "KEYBY",
        TransformationType::Merge => "MERGE",
        TransformationType::Split => "SPLIT",
        TransformationType::Case => "CASE",
        TransformationType::Sink => "SINK",
        TransformationType::CycleLink => "CYCLELINK",
        TransformationType::Error => "ERROR",
        TransformationType::Delay => "DELAY",
        TransformationType::When => "WHEN",
    }
}

fn node_image(icon: &str, background: &str) -> NodeImage {
    NodeImage {
        unselected: svg_data_uri(&format!(
            r#"<svg xmlns="http://www.w3.org/2000/svg" width="60" height="60"><rect width="60" height="60" rx="10" fill="{background}"/><svg x="10" y="10" width="40" height="40" viewBox="0 0 24 24"><path d="{icon}" fill="white"/></svg></svg>"#
        )),
        selected: svg_data_uri(&format!(
            r##"<svg xmlns="http://www.w3.org/2000/svg" width="60" height="60"><rect width="60" height="60" rx="10" fill="{background}"/><svg x="10" y="10" width="40" height="40" viewBox="0 0 24 24"><path d="{icon}" fill="white"/></svg><rect x="2" y="2" width="56" height="56" rx="9" fill="none" stroke="#00FF80" stroke-width="4"/></svg>"##
        )),
    }
}

fn svg_data_uri(svg: &str) -> String {
    let mut encoded = String::with_capacity(svg.len() + 32);
    encoded.push_str("data:image/svg+xml;charset=utf-8,");
    for character in svg.chars() {
        match character {
            ' ' => encoded.push_str("%20"),
            '<' => encoded.push_str("%3C"),
            '>' => encoded.push_str("%3E"),
            '#' => encoded.push_str("%23"),
            '"' => encoded.push_str("%22"),
            '{' => encoded.push_str("%7B"),
            '}' => encoded.push_str("%7D"),
            _ => encoded.push(character),
        }
    }
    encoded
}

fn transparent_color() -> NodeColor {
    NodeColor {
        border: "transparent",
        highlight: Highlight {
            border: "transparent",
        },
    }
}
