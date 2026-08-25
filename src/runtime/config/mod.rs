use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::api;
pub use crate::api::JoinType;

mod loader;
mod runtime;

pub use loader::{Config, ConfigLoader};
pub use runtime::RuntimeConfig;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ServiceConfig {
    pub id: i32,
    pub name: String,
    pub programming_language: api::ProgrammingLanguage,
    pub module_path: String,
    pub default_call_semantics: CallSemantics,
    pub http_host: String,
    pub http_port: u16,
    pub metrics_handler: String,
    pub status_handler: String,
    pub startup_handler: String,
    pub readiness_handler: String,
    pub liveness_handler: String,
    pub kubernetes_workload_type: api::KubernetesWorkloadType,
    pub grpc_host: String,
    pub grpc_port: u16,
    pub default_grpc_timeout: i64,
    pub color: String,
    pub log_level: api::LogLevel,
    pub environment: String,
    pub shutdown_timeout: i64,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PoolConfig {
    pub name: String,
    pub executors_count: usize,
    #[serde(default)]
    pub queue_capacity: usize,
}

impl Default for ServiceConfig {
    fn default() -> Self {
        Self {
            id: 0,
            name: String::new(),
            programming_language: api::ProgrammingLanguage::Rust,
            module_path: String::new(),
            default_call_semantics: CallSemantics::FunctionCall,
            http_host: "0.0.0.0".to_owned(),
            http_port: 8080,
            metrics_handler: "metrics".to_owned(),
            status_handler: "status".to_owned(),
            startup_handler: "health/startup".to_owned(),
            readiness_handler: "health/ready".to_owned(),
            liveness_handler: "health/live".to_owned(),
            kubernetes_workload_type: api::KubernetesWorkloadType::Deployment,
            grpc_host: "0.0.0.0".to_owned(),
            grpc_port: 9090,
            default_grpc_timeout: 5_000,
            color: String::new(),
            log_level: api::LogLevel::Undefined,
            environment: String::new(),
            shutdown_timeout: 30_000,
        }
    }
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ModuleConfig {
    pub name: String,
    pub path: String,
    #[serde(default, flatten)]
    pub properties: HashMap<String, serde_json::Value>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TypeConfig {
    pub name: String,
    #[serde(rename = "type")]
    pub data_type: api::DataType,
    #[serde(default)]
    pub type_definition: String,
    #[serde(default)]
    pub type_import: String,
    #[serde(default)]
    pub value_type: String,
    #[serde(default)]
    pub key_type: String,
    #[serde(default)]
    pub package: String,
    #[serde(default)]
    pub module: String,
    pub definition_format: api::TypeDefinitionFormat,
    #[serde(default)]
    pub public_type: bool,
    #[serde(default)]
    pub transfer_by_value: bool,
    #[serde(default)]
    pub use_alias: bool,
    #[serde(default, flatten)]
    pub properties: HashMap<String, serde_json::Value>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct HttpDataConnectorConfig {
    pub id: i32,
    pub name: String,
    pub host: String,
    pub port: u16,
    pub address: String,
    pub use_dedicated_listener: bool,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct HttpEndpointConfig {
    pub id: i32,
    pub name: String,
    pub id_data_connector: i32,
    #[serde(default)]
    pub tracing_enabled: bool,
    pub http_method_type: api::HTTPMethodType,
    pub path: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct GrpcDataConnectorConfig {
    pub id: i32,
    pub name: String,
    pub address: String,
    #[serde(default = "default_connections_count")]
    pub connections_count: usize,
}

fn default_connections_count() -> usize {
    1
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct GrpcEndpointConfig {
    pub id: i32,
    pub name: String,
    pub id_data_connector: i32,
    #[serde(default)]
    pub tracing_enabled: bool,
    pub grpc_method_type: api::GrpcMethodType,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct KafkaDataConnectorConfig {
    pub id: i32,
    pub name: String,
    pub brokers: String,
    pub version: String,
    pub dial_timeout: f32,
    pub use_partitioner: bool,
    pub r#async: bool,
    #[serde(default = "default_kafka_security_protocol")]
    pub security_protocol: api::KafkaSecurityProtocol,
    #[serde(default = "default_kafka_sasl_mechanism")]
    pub sasl_mechanism: api::KafkaSaslMechanism,
    #[serde(default)]
    pub username: String,
    #[serde(default)]
    pub password: String,
}

fn default_kafka_security_protocol() -> api::KafkaSecurityProtocol {
    api::KafkaSecurityProtocol::PLAINTEXT
}

fn default_kafka_sasl_mechanism() -> api::KafkaSaslMechanism {
    api::KafkaSaslMechanism::PLAIN
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct KafkaEndpointConfig {
    pub id: i32,
    pub name: String,
    pub id_data_connector: i32,
    #[serde(default)]
    pub tracing_enabled: bool,
    #[serde(default)]
    pub enabled: bool,
    pub create_topic: bool,
    pub topic: String,
    pub partitions: i32,
    pub consumer_group: String,
    pub replication_factor: i32,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CronDataConnectorConfig {
    pub id: i32,
    pub name: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TemporalDataConnectorConfig {
    pub id: i32,
    pub name: String,
    pub address: String,
    pub namespace: String,
    #[serde(default)]
    pub identity: String,
    #[serde(default)]
    pub max_concurrent_activities: usize,
    #[serde(default)]
    pub max_concurrent_workflows: usize,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CronEndpointConfig {
    pub id: i32,
    pub name: String,
    pub id_data_connector: i32,
    #[serde(default)]
    pub tracing_enabled: bool,
    #[serde(default)]
    pub enabled: bool,
    pub schedule: String,
    #[serde(default = "default_schedule_timezone")]
    pub timezone: String,
    #[serde(default = "default_overlap_policy")]
    pub overlap_policy: api::ScheduleOverlapPolicy,
    #[serde(default = "default_missed_run_policy")]
    pub missed_run_policy: api::ScheduleMissedRunPolicy,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TemporalEndpointConfig {
    pub id: i32,
    pub name: String,
    pub id_data_connector: i32,
    #[serde(default)]
    pub tracing_enabled: bool,
    #[serde(default)]
    pub enabled: bool,
    pub task_queue: String,
    #[serde(default)]
    pub schedule: String,
    #[serde(default)]
    pub schedule_id: String,
    #[serde(default = "default_schedule_timezone")]
    pub timezone: String,
    #[serde(default = "default_overlap_policy")]
    pub overlap_policy: api::ScheduleOverlapPolicy,
    #[serde(default = "default_missed_run_policy")]
    pub missed_run_policy: api::ScheduleMissedRunPolicy,
    #[serde(default)]
    pub workflow_execution_timeout: i64,
    pub activity_start_to_close_timeout: i64,
    #[serde(default)]
    pub activity_heartbeat_timeout: i64,
    pub maximum_attempts: i32,
}

fn default_schedule_timezone() -> String {
    "UTC".to_owned()
}

fn default_overlap_policy() -> api::ScheduleOverlapPolicy {
    api::ScheduleOverlapPolicy::Skip
}

fn default_missed_run_policy() -> api::ScheduleMissedRunPolicy {
    api::ScheduleMissedRunPolicy::Skip
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct CustomDataConnectorConfig {
    pub id: i32,
    pub name: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct CustomEndpointConfig {
    pub id: i32,
    pub name: String,
    pub id_data_connector: i32,
    #[serde(default)]
    pub tracing_enabled: bool,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum RuntimeDataConnectorConfig {
    Http(HttpDataConnectorConfig),
    Grpc(GrpcDataConnectorConfig),
    Kafka(KafkaDataConnectorConfig),
    Cron(CronDataConnectorConfig),
    Temporal(TemporalDataConnectorConfig),
    Custom(CustomDataConnectorConfig),
}

impl RuntimeDataConnectorConfig {
    pub fn id(&self) -> i32 {
        match self {
            Self::Http(config) => config.id,
            Self::Grpc(config) => config.id,
            Self::Kafka(config) => config.id,
            Self::Cron(config) => config.id,
            Self::Temporal(config) => config.id,
            Self::Custom(config) => config.id,
        }
    }

    pub fn name(&self) -> &str {
        match self {
            Self::Http(config) => &config.name,
            Self::Grpc(config) => &config.name,
            Self::Kafka(config) => &config.name,
            Self::Cron(config) => &config.name,
            Self::Temporal(config) => &config.name,
            Self::Custom(config) => &config.name,
        }
    }

    pub fn connector_type(&self) -> api::DataConnectorType {
        match self {
            Self::Http(_) => api::DataConnectorType::HTTP,
            Self::Grpc(_) => api::DataConnectorType::GRPC,
            Self::Kafka(_) => api::DataConnectorType::Kafka,
            Self::Cron(_) => api::DataConnectorType::Cron,
            Self::Temporal(_) => api::DataConnectorType::Temporal,
            Self::Custom(_) => api::DataConnectorType::Custom,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum RuntimeEndpointConfig {
    Http(HttpEndpointConfig),
    Grpc(GrpcEndpointConfig),
    Kafka(KafkaEndpointConfig),
    Cron(CronEndpointConfig),
    Temporal(TemporalEndpointConfig),
    Custom(CustomEndpointConfig),
}

impl RuntimeEndpointConfig {
    pub fn id(&self) -> i32 {
        match self {
            Self::Http(config) => config.id,
            Self::Grpc(config) => config.id,
            Self::Kafka(config) => config.id,
            Self::Cron(config) => config.id,
            Self::Temporal(config) => config.id,
            Self::Custom(config) => config.id,
        }
    }

    pub fn name(&self) -> &str {
        match self {
            Self::Http(config) => &config.name,
            Self::Grpc(config) => &config.name,
            Self::Kafka(config) => &config.name,
            Self::Cron(config) => &config.name,
            Self::Temporal(config) => &config.name,
            Self::Custom(config) => &config.name,
        }
    }

    pub fn data_connector_id(&self) -> i32 {
        match self {
            Self::Http(config) => config.id_data_connector,
            Self::Grpc(config) => config.id_data_connector,
            Self::Kafka(config) => config.id_data_connector,
            Self::Cron(config) => config.id_data_connector,
            Self::Temporal(config) => config.id_data_connector,
            Self::Custom(config) => config.id_data_connector,
        }
    }

    pub fn connector_type(&self) -> api::DataConnectorType {
        match self {
            Self::Http(_) => api::DataConnectorType::HTTP,
            Self::Grpc(_) => api::DataConnectorType::GRPC,
            Self::Kafka(_) => api::DataConnectorType::Kafka,
            Self::Cron(_) => api::DataConnectorType::Cron,
            Self::Temporal(_) => api::DataConnectorType::Temporal,
            Self::Custom(_) => api::DataConnectorType::Custom,
        }
    }

    pub fn tracing_enabled(&self) -> bool {
        match self {
            Self::Http(config) => config.tracing_enabled,
            Self::Grpc(config) => config.tracing_enabled,
            Self::Kafka(config) => config.tracing_enabled,
            Self::Cron(config) => config.tracing_enabled,
            Self::Temporal(config) => config.tracing_enabled,
            Self::Custom(config) => config.tracing_enabled,
        }
    }
}

impl From<HttpDataConnectorConfig> for RuntimeDataConnectorConfig {
    fn from(config: HttpDataConnectorConfig) -> Self {
        Self::Http(config)
    }
}

impl From<GrpcDataConnectorConfig> for RuntimeDataConnectorConfig {
    fn from(config: GrpcDataConnectorConfig) -> Self {
        Self::Grpc(config)
    }
}

impl From<KafkaDataConnectorConfig> for RuntimeDataConnectorConfig {
    fn from(config: KafkaDataConnectorConfig) -> Self {
        Self::Kafka(config)
    }
}

impl From<CronDataConnectorConfig> for RuntimeDataConnectorConfig {
    fn from(config: CronDataConnectorConfig) -> Self {
        Self::Cron(config)
    }
}

impl From<TemporalDataConnectorConfig> for RuntimeDataConnectorConfig {
    fn from(config: TemporalDataConnectorConfig) -> Self {
        Self::Temporal(config)
    }
}

impl From<CustomDataConnectorConfig> for RuntimeDataConnectorConfig {
    fn from(config: CustomDataConnectorConfig) -> Self {
        Self::Custom(config)
    }
}

impl From<HttpEndpointConfig> for RuntimeEndpointConfig {
    fn from(config: HttpEndpointConfig) -> Self {
        Self::Http(config)
    }
}

impl From<GrpcEndpointConfig> for RuntimeEndpointConfig {
    fn from(config: GrpcEndpointConfig) -> Self {
        Self::Grpc(config)
    }
}

impl From<KafkaEndpointConfig> for RuntimeEndpointConfig {
    fn from(config: KafkaEndpointConfig) -> Self {
        Self::Kafka(config)
    }
}

impl From<CronEndpointConfig> for RuntimeEndpointConfig {
    fn from(config: CronEndpointConfig) -> Self {
        Self::Cron(config)
    }
}

impl From<TemporalEndpointConfig> for RuntimeEndpointConfig {
    fn from(config: TemporalEndpointConfig) -> Self {
        Self::Temporal(config)
    }
}

impl From<CustomEndpointConfig> for RuntimeEndpointConfig {
    fn from(config: CustomEndpointConfig) -> Self {
        Self::Custom(config)
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub enum CallSemantics {
    #[default]
    FunctionCall,
    ParallelCall,
    TaskPool {
        pool_name: String,
    },
    PriorityTaskPool {
        pool_name: String,
        priority: i32,
    },
    DurableCall {
        id_data_connector: i32,
    },
}

impl CallSemantics {
    pub fn is_async(&self) -> bool {
        !matches!(self, Self::FunctionCall)
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct LinkConfig {
    pub from: i32,
    pub to: i32,
    pub call_semantics: CallSemantics,
    #[serde(default)]
    pub r#async: bool,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct StreamConfig {
    pub id: i32,
    pub name: String,
    #[serde(default)]
    pub pipeline: String,
    #[serde(default)]
    pub id_source: i32,
    #[serde(default)]
    pub id_sources: Vec<i32>,
    #[serde(default)]
    pub id_service: i32,
    #[serde(default)]
    pub value_type: Option<String>,
    #[serde(default)]
    pub key_type: Option<String>,
    #[serde(default)]
    pub x_pos: f64,
    #[serde(default)]
    pub y_pos: f64,
    #[serde(default, flatten)]
    pub properties: HashMap<String, serde_json::Value>,
}

impl StreamConfig {
    pub fn new(id: i32, name: impl Into<String>) -> Self {
        Self {
            id,
            name: name.into(),
            pipeline: String::new(),
            id_source: 0,
            id_sources: Vec::new(),
            id_service: 0,
            value_type: None,
            key_type: None,
            x_pos: 0.0,
            y_pos: 0.0,
            properties: HashMap::new(),
        }
    }

    pub fn property(&self, name: &str) -> Option<&serde_json::Value> {
        self.properties.get(name)
    }

    pub fn property_bool(&self, name: &str) -> Option<bool> {
        self.property(name).and_then(serde_json::Value::as_bool)
    }

    #[allow(
        clippy::too_many_arguments,
        reason = "arguments map one-to-one to the generated graph configuration"
    )]
    pub fn with_graph(
        mut self,
        id_service: i32,
        id_source: i32,
        id_sources: impl IntoIterator<Item = i32>,
        value_type: Option<impl Into<String>>,
        key_type: Option<impl Into<String>>,
        x_pos: f64,
        y_pos: f64,
    ) -> Self {
        self.id_service = id_service;
        self.id_source = id_source;
        self.id_sources = id_sources.into_iter().collect();
        self.value_type = value_type.map(Into::into);
        self.key_type = key_type.map(Into::into);
        self.x_pos = x_pos;
        self.y_pos = y_pos;
        self
    }

    pub fn with_pipeline(mut self, pipeline: impl Into<String>) -> Self {
        self.pipeline = pipeline.into();
        self
    }
}

macro_rules! stream_config {
    ($name:ident) => {
        #[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
        pub struct $name {
            #[serde(flatten)]
            pub stream: StreamConfig,
        }

        impl From<$name> for StreamConfig {
            fn from(config: $name) -> Self {
                config.stream
            }
        }

        impl From<StreamConfig> for $name {
            fn from(stream: StreamConfig) -> Self {
                Self { stream }
            }
        }
    };
}

stream_config!(MapStreamConfig);
stream_config!(FilterStreamConfig);
stream_config!(FlatMapStreamConfig);
stream_config!(FlatMapIterableStreamConfig);
stream_config!(KeyByStreamConfig);
stream_config!(MergeStreamConfig);
stream_config!(SplitStreamConfig);
stream_config!(CaseStreamConfig);
stream_config!(WhenStreamConfig);
stream_config!(CycleLinkStreamConfig);

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct InputStreamConfig {
    pub stream: StreamConfig,
    pub endpoint_id: i32,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ProcessStreamConfig {
    #[serde(flatten)]
    pub stream: StreamConfig,
    pub pattern: api::ProcessPattern,
}

impl From<StreamConfig> for ProcessStreamConfig {
    fn from(stream: StreamConfig) -> Self {
        Self {
            stream,
            pattern: api::ProcessPattern::Undefined,
        }
    }
}

impl From<ProcessStreamConfig> for StreamConfig {
    fn from(config: ProcessStreamConfig) -> Self {
        config.stream
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct DelayStreamConfig {
    #[serde(flatten)]
    pub stream: StreamConfig,
    pub duration: std::time::Duration,
}

impl From<StreamConfig> for DelayStreamConfig {
    fn from(stream: StreamConfig) -> Self {
        Self {
            stream,
            duration: std::time::Duration::ZERO,
        }
    }
}

impl From<DelayStreamConfig> for StreamConfig {
    fn from(config: DelayStreamConfig) -> Self {
        config.stream
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct SinkStreamConfig {
    #[serde(flatten)]
    pub stream: StreamConfig,
    pub endpoint_id: i32,
}

impl From<SinkStreamConfig> for StreamConfig {
    fn from(config: SinkStreamConfig) -> Self {
        config.stream
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct JoinStreamConfig {
    pub stream: StreamConfig,
    pub join_type: JoinType,
    pub join_storage: api::JoinStorageType,
    pub ttl: std::time::Duration,
    pub renew_ttl: bool,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct MultiJoinStreamConfig {
    pub stream: StreamConfig,
    pub join_storage: api::JoinStorageType,
    pub ttl: std::time::Duration,
    pub renew_ttl: bool,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum RuntimeStreamConfig {
    Plain(StreamConfig),
    Error(StreamConfig),
    Map(MapStreamConfig),
    Filter(FilterStreamConfig),
    FlatMap(FlatMapStreamConfig),
    FlatMapIterable(FlatMapIterableStreamConfig),
    KeyBy(KeyByStreamConfig),
    Merge(MergeStreamConfig),
    Split(SplitStreamConfig),
    Case(CaseStreamConfig),
    When(WhenStreamConfig),
    CycleLink(CycleLinkStreamConfig),
    Input(InputStreamConfig),
    Process(ProcessStreamConfig),
    Delay(DelayStreamConfig),
    Sink(SinkStreamConfig),
    Join(JoinStreamConfig),
    MultiJoin(MultiJoinStreamConfig),
}

impl RuntimeStreamConfig {
    pub fn stream(&self) -> &StreamConfig {
        match self {
            Self::Plain(config) | Self::Error(config) => config,
            Self::Map(config) => &config.stream,
            Self::Filter(config) => &config.stream,
            Self::FlatMap(config) => &config.stream,
            Self::FlatMapIterable(config) => &config.stream,
            Self::KeyBy(config) => &config.stream,
            Self::Merge(config) => &config.stream,
            Self::Split(config) => &config.stream,
            Self::Case(config) => &config.stream,
            Self::When(config) => &config.stream,
            Self::CycleLink(config) => &config.stream,
            Self::Input(config) => &config.stream,
            Self::Process(config) => &config.stream,
            Self::Delay(config) => &config.stream,
            Self::Sink(config) => &config.stream,
            Self::Join(config) => &config.stream,
            Self::MultiJoin(config) => &config.stream,
        }
    }

    pub fn delay(&self) -> Option<&DelayStreamConfig> {
        match self {
            Self::Delay(config) => Some(config),
            _ => None,
        }
    }

    pub fn transformation_type(&self) -> api::TransformationType {
        match self {
            Self::Plain(_) => api::TransformationType::Undefined,
            Self::Error(_) => api::TransformationType::Error,
            Self::Map(_) => api::TransformationType::Map,
            Self::Filter(_) => api::TransformationType::Filter,
            Self::FlatMap(_) => api::TransformationType::FlatMap,
            Self::FlatMapIterable(_) => api::TransformationType::FlatMapIterable,
            Self::KeyBy(_) => api::TransformationType::KeyBy,
            Self::Merge(_) => api::TransformationType::Merge,
            Self::Split(_) => api::TransformationType::Split,
            Self::Case(_) => api::TransformationType::Case,
            Self::When(_) => api::TransformationType::When,
            Self::CycleLink(_) => api::TransformationType::CycleLink,
            Self::Input(_) => api::TransformationType::Input,
            Self::Process(_) => api::TransformationType::Process,
            Self::Delay(_) => api::TransformationType::Delay,
            Self::Sink(_) => api::TransformationType::Sink,
            Self::Join(_) => api::TransformationType::Join,
            Self::MultiJoin(_) => api::TransformationType::MultiJoin,
        }
    }

    pub fn endpoint_id(&self) -> Option<i32> {
        match self {
            Self::Input(config) => Some(config.endpoint_id),
            Self::Sink(config) => Some(config.endpoint_id),
            _ => None,
        }
    }
}

macro_rules! runtime_stream_from {
    ($variant:ident, $type:ty) => {
        impl From<$type> for RuntimeStreamConfig {
            fn from(config: $type) -> Self {
                Self::$variant(config)
            }
        }
    };
}

runtime_stream_from!(Map, MapStreamConfig);
runtime_stream_from!(Filter, FilterStreamConfig);
runtime_stream_from!(FlatMap, FlatMapStreamConfig);
runtime_stream_from!(FlatMapIterable, FlatMapIterableStreamConfig);
runtime_stream_from!(KeyBy, KeyByStreamConfig);
runtime_stream_from!(Merge, MergeStreamConfig);
runtime_stream_from!(Split, SplitStreamConfig);
runtime_stream_from!(Case, CaseStreamConfig);
runtime_stream_from!(When, WhenStreamConfig);
runtime_stream_from!(CycleLink, CycleLinkStreamConfig);
runtime_stream_from!(Input, InputStreamConfig);
runtime_stream_from!(Process, ProcessStreamConfig);
runtime_stream_from!(Delay, DelayStreamConfig);
runtime_stream_from!(Sink, SinkStreamConfig);
runtime_stream_from!(Join, JoinStreamConfig);
runtime_stream_from!(MultiJoin, MultiJoinStreamConfig);

impl From<StreamConfig> for RuntimeStreamConfig {
    fn from(config: StreamConfig) -> Self {
        Self::Plain(config)
    }
}
