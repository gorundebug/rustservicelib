use std::{collections::HashMap, time::Duration};
use serde::{Deserialize, Serialize};
use crate::{api, runtime::environment::{RuntimeError, RuntimeResult}};

pub const MAX_TIMEOUT_MS: u64 = 9_223_372_036_854;
fn nats_kind() -> api::DataConnectorType { api::DataConnectorType::NATS }

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct NatsDataConnectorConfig {
    pub id: i32,
    pub name: String,
    #[serde(rename = "type")]
    pub kind: api::DataConnectorType,
    pub servers: Vec<String>,
    pub connect_timeout_ms: u64,
    pub operation_timeout_ms: u64,
    pub username: String,
    pub password: String,
    pub token: String,
    pub credentials_file: String,
    pub tls_enabled: bool,
    pub tls_server_name: String,
    pub tls_ca_file: String,
    pub tls_cert_file: String,
    pub tls_key_file: String,
    #[serde(flatten)]
    pub properties: HashMap<String, serde_json::Value>,
}
impl Default for NatsDataConnectorConfig {
    fn default() -> Self { Self { id: 0, name: String::new(), kind: nats_kind(), servers: vec![], connect_timeout_ms: 0, operation_timeout_ms: 0,
        username: String::new(), password: String::new(), token: String::new(), credentials_file: String::new(), tls_enabled: false,
        tls_server_name: String::new(), tls_ca_file: String::new(), tls_cert_file: String::new(), tls_key_file: String::new(), properties: HashMap::new() } }
}
impl NatsDataConnectorConfig {
    pub fn jetstream(&self) -> bool { self.kind == api::DataConnectorType::JetStream }
    pub fn connect_timeout(&self) -> Duration { Duration::from_millis(if self.connect_timeout_ms == 0 { 2000 } else { self.connect_timeout_ms }) }
    pub fn operation_timeout(&self) -> Duration { Duration::from_millis(if self.operation_timeout_ms == 0 { 5000 } else { self.operation_timeout_ms }) }
    pub fn validate(&self) -> RuntimeResult<()> {
        let fail = |message: &str| RuntimeError::InvalidConfiguration(message.into());
        if !matches!(self.kind, api::DataConnectorType::NATS | api::DataConnectorType::JetStream) { return Err(fail("unsupported NATS connector type")); }
        if self.servers.is_empty() { return Err(fail("NATS servers must be explicit")); }
        let (mut tls, mut plain) = (false, false);
        for server in &self.servers {
            // Validate before URL normalization; never echo credential-bearing URLs.
            if server.chars().any(|c| c.is_whitespace() || c.is_control()) || server.contains(['?', '#']) ||
                !(server.starts_with("nats://") || server.starts_with("tls://")) { return Err(fail("invalid NATS server URI")); }
            let authority = server.split_once("://").unwrap().1;
            if authority.contains(['/', '@']) || authority.ends_with(':') { return Err(fail("NATS server URI must not contain credentials, path or empty port")); }
            let url = reqwest::Url::parse(server).map_err(|_| fail("invalid NATS server URI"))?;
            if url.host_str().is_none_or(str::is_empty) || url.port() == Some(0) { return Err(fail("invalid NATS host or port")); }
            tls |= url.scheme() == "tls"; plain |= url.scheme() == "nats";
        }
        if tls && plain && !self.tls_enabled { return Err(fail("mixed NATS server URLs require tlsEnabled")); }
        if !tls && !self.tls_enabled && [&self.tls_ca_file, &self.tls_cert_file, &self.tls_key_file, &self.tls_server_name].iter().any(|v| !v.is_empty()) { return Err(fail("NATS TLS settings require tlsEnabled or tls://")); }
        if self.connect_timeout_ms > MAX_TIMEOUT_MS || self.operation_timeout_ms > MAX_TIMEOUT_MS { return Err(fail("NATS timeout exceeds 9223372036854 milliseconds")); }
        if self.username.is_empty() != self.password.is_empty() { return Err(fail("NATS username and password must be paired")); }
        if usize::from(!self.username.is_empty()) + usize::from(!self.token.is_empty()) + usize::from(!self.credentials_file.is_empty()) > 1 { return Err(fail("NATS authentication modes are exclusive")); }
        if self.tls_cert_file.is_empty() != self.tls_key_file.is_empty() { return Err(fail("NATS TLS certificate and key must be paired")); }
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct NatsEndpointConfig {
    pub id: i32, pub name: String, pub id_data_connector: i32,
    #[serde(rename = "type")]
    pub kind: api::DataConnectorType,
    pub enabled: bool, pub tracing_enabled: bool,
    pub subject: String, pub queue_group: String, pub stream_name: String, pub durable_name: String,
    pub create_stream: bool, pub create_consumer: bool,
}
impl Default for NatsEndpointConfig {
    fn default() -> Self { Self { id:0, name:String::new(), id_data_connector:0, kind:nats_kind(), enabled:false, tracing_enabled:false,
        subject:String::new(), queue_group:String::new(), stream_name:String::new(), durable_name:String::new(), create_stream:false, create_consumer:false } }
}
pub fn valid_subject(subject: &str, wildcards: bool) -> bool {
    if subject.is_empty() || subject.chars().any(|c| c.is_whitespace() || c.is_control()) { return false; }
    let parts: Vec<_> = subject.split('.').collect();
    parts.iter().enumerate().all(|(i, p)| !p.is_empty() && ((!p.contains(['*','>'])) || (wildcards && (*p == "*" || (*p == ">" && i + 1 == parts.len())))))
}
fn valid_name(name: &str) -> bool { !name.is_empty() && !name.contains(['.', '*', '>', '/', '\\']) && !name.chars().any(|c| c.is_whitespace() || c.is_control()) }
impl NatsEndpointConfig {
    pub fn validate(&self, connector: &NatsDataConnectorConfig, source: bool) -> RuntimeResult<()> {
        let fail = |message: &str| RuntimeError::InvalidConfiguration(message.into());
        connector.validate()?;
        if self.id_data_connector != connector.id || self.kind != connector.kind { return Err(fail("NATS endpoint connector identity/type mismatch")); }
        if !valid_subject(&self.subject, source) { return Err(fail("invalid NATS subject")); }
        if !source && (!self.queue_group.is_empty() || !self.durable_name.is_empty() || self.create_consumer) { return Err(fail("source-only NATS sink settings")); }
        if connector.jetstream() {
            if !valid_name(&self.stream_name) || (source && !valid_name(&self.durable_name)) || !self.queue_group.is_empty() { return Err(fail("JetStream requires native stream/durable names and no queue group")); }
        } else if !self.stream_name.is_empty() || !self.durable_name.is_empty() || self.create_stream || self.create_consumer { return Err(fail("Core NATS cannot configure JetStream resources")); }
        if !self.queue_group.is_empty() && !valid_subject(&self.queue_group, false) { return Err(fail("invalid NATS queue group")); }
        Ok(())
    }
}
