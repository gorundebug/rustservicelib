use serde::{Deserialize, Serialize};

/// Connection strings are accepted on input but never exported in snapshots.
#[derive(Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PostgreSQLSQLxDataConnectorConfig {
    pub id: i32,
    pub name: String,
    #[serde(skip_serializing)]
    pub connection_string: String,
    pub connections_count: usize,
    pub connect_timeout_ms: u64,
    #[serde(default)]
    pub operation_timeout_ms: u64,
}

impl std::fmt::Debug for PostgreSQLSQLxDataConnectorConfig {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.debug_struct("PostgreSQLSQLxDataConnectorConfig")
            .field("id", &self.id).field("name", &self.name)
            .field("connections_count", &self.connections_count)
            .field("connect_timeout_ms", &self.connect_timeout_ms)
            .field("operation_timeout_ms", &self.operation_timeout_ms)
            .finish_non_exhaustive()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum PostgreSQLQueryOperation {
    Rows,
    Execute,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct QueryEndpointConfig {
    pub id: i32,
    pub name: String,
    pub id_data_connector: i32,
    pub enabled: bool,
    #[serde(default)]
    pub tracing_enabled: bool,
    pub query_operation: PostgreSQLQueryOperation,
    #[serde(default)]
    pub sql_file: Option<String>,
}

impl PostgreSQLSQLxDataConnectorConfig {
    pub fn validate(&self) -> crate::runtime::environment::RuntimeResult<()> {
        use std::str::FromStr;
        use crate::runtime::environment::RuntimeError;
        if self.connections_count == 0 || self.connections_count > u32::MAX as usize {
            return Err(RuntimeError::InvalidConfiguration(
                "PostgreSQL pool size must be positive and fit u32".into()));
        }
        if self.connect_timeout_ms > i32::MAX as u64 || self.operation_timeout_ms > i32::MAX as u64 {
            return Err(RuntimeError::InvalidConfiguration(
                "PostgreSQL timeouts must be in 0..2147483647 milliseconds".into()));
        }
        sqlx::postgres::PgConnectOptions::from_str(&self.connection_string)
            .map_err(|_| RuntimeError::InvalidConfiguration("invalid PostgreSQL connection string".into()))?;
        Ok(())
    }
}

/// S3 credentials remain with the native provider chain. Endpoint addresses are
/// runtime-only and omitted from public graph snapshots and Debug output.
#[derive(Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct S3AWSDataConnectorConfig {
    pub id: i32,
    pub name: String,
    pub region: String,
    #[serde(default, skip_serializing)]
    pub address: Option<String>,
    #[serde(default)]
    pub force_path_style: bool,
    pub connect_timeout_ms: u64,
    pub operation_timeout_ms: u64,
}
impl std::fmt::Debug for S3AWSDataConnectorConfig {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.debug_struct("S3AWSDataConnectorConfig")
            .field("id", &self.id).field("name", &self.name)
            .field("region", &self.region).field("force_path_style", &self.force_path_style)
            .field("connect_timeout_ms", &self.connect_timeout_ms)
            .field("operation_timeout_ms", &self.operation_timeout_ms)
            .finish_non_exhaustive()
    }
}
impl S3AWSDataConnectorConfig {
    pub fn validate(&self) -> crate::runtime::environment::RuntimeResult<()> {
        use crate::runtime::environment::RuntimeError;
        if self.region.trim().is_empty() || self.connect_timeout_ms == 0 || self.operation_timeout_ms == 0 {
            return Err(RuntimeError::InvalidConfiguration("S3 region and positive timeouts are required".into()));
        }
        if let Some(address) = &self.address {
            let url = reqwest::Url::parse(address)
                .map_err(|_| RuntimeError::InvalidConfiguration("invalid S3 endpoint address".into()))?;
            if !matches!(url.scheme(), "http" | "https") || url.host_str().is_none()
                || !url.username().is_empty() || url.password().is_some()
                || url.query().is_some() || url.fragment().is_some() {
                return Err(RuntimeError::InvalidConfiguration("S3 endpoint must be HTTP(S) without credentials, query or fragment".into()));
            }
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum S3QueryOperation { GetObject, HeadObject, ListObjects, PutObject, DeleteObject }

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct S3QueryEndpointConfig {
    pub id: i32,
    pub name: String,
    pub id_data_connector: i32,
    pub enabled: bool,
    #[serde(default)]
    pub tracing_enabled: bool,
    pub query_operation: S3QueryOperation,
    pub bucket: String,
}
impl S3QueryEndpointConfig {
    pub fn validate(&self) -> crate::runtime::environment::RuntimeResult<()> {
        if self.bucket.trim().is_empty() {
            return Err(crate::runtime::environment::RuntimeError::InvalidConfiguration("S3 Query bucket is required".into()));
        }
        Ok(())
    }
}

#[cfg(test)]
mod postgres_tests {
    use super::*;

    #[test]
    fn zero_timeouts_are_valid_but_values_outside_model_range_are_rejected() {
        let mut config = PostgreSQLSQLxDataConnectorConfig {
            id: 1, name: "database".into(), connection_string: "postgres://localhost/test".into(),
            connections_count: 1, connect_timeout_ms: 0, operation_timeout_ms: 0,
        };
        assert!(config.validate().is_ok());
        config.operation_timeout_ms = i32::MAX as u64 + 1;
        assert!(config.validate().is_err());
        config.operation_timeout_ms = 0;
        config.connect_timeout_ms = i32::MAX as u64 + 1;
        assert!(config.validate().is_err());
    }
}

#[cfg(test)]
mod s3_tests {
    use super::*;
    use crate::runtime::config::{RuntimeDataConnectorConfig, RuntimeEndpointConfig};

    fn connector() -> S3AWSDataConnectorConfig {
        S3AWSDataConnectorConfig {
            id: 7, name: "objects".into(), region: "us-east-1".into(),
            address: Some("http://localhost:9000".into()), force_path_style: true,
            connect_timeout_ms: 1000, operation_timeout_ms: 5000,
        }
    }

    #[test]
    fn runtime_settings_validate_and_endpoint_address_is_not_exported() {
        let config = connector();
        config.validate().unwrap();
        let json = serde_json::to_string(&config).unwrap();
        assert!(!json.contains("localhost"));
        assert!(!json.contains("address"));
        assert!(!format!("{config:?}").contains("localhost"));
        let wrapped: RuntimeDataConnectorConfig = config.into();
        assert_eq!(wrapped.id(), 7);
        assert_eq!(wrapped.connector_type(), crate::api::DataConnectorType::S3AWS);
    }

    #[test]
    fn malformed_or_credential_bearing_addresses_are_rejected_without_echoing_them() {
        for address in ["bad endpoint", "ftp://localhost", "https://user:secret@localhost",
            "https://localhost?token=secret", "https://localhost#secret"] {
            let mut config = connector();
            config.address = Some(address.into());
            let error = config.validate().unwrap_err().to_string();
            assert!(!error.contains(address));
            assert!(!error.contains("secret"));
        }
        let mut config = connector();
        config.connect_timeout_ms = 0;
        assert!(config.validate().is_err());
        config = connector();
        config.operation_timeout_ms = 0;
        assert!(config.validate().is_err());
        config = connector();
        config.region = " ".into();
        assert!(config.validate().is_err());
    }

    #[test]
    fn all_s3_operations_use_s3_endpoint_family() {
        for operation in [S3QueryOperation::GetObject, S3QueryOperation::HeadObject,
            S3QueryOperation::ListObjects, S3QueryOperation::PutObject,
            S3QueryOperation::DeleteObject] {
            let mut config = S3QueryEndpointConfig {
                id: 8, name: "query".into(), id_data_connector: 7, enabled: true,
                tracing_enabled: false, query_operation: operation, bucket: "objects".into(),
            };
            config.validate().unwrap();
            let endpoint: RuntimeEndpointConfig = config.clone().into();
            assert_eq!(endpoint.id(), 8);
            assert_eq!(endpoint.data_connector_id(), 7);
            assert_eq!(endpoint.connector_type(), crate::api::DataConnectorType::S3AWS);
            let encoded = serde_json::to_string(&endpoint).unwrap();
            let decoded: RuntimeEndpointConfig = serde_json::from_str(&encoded).unwrap();
            assert_eq!(endpoint, decoded);
            config.bucket.clear();
            assert!(config.validate().is_err());
        }
        assert!(serde_json::from_str::<S3QueryOperation>("\"Rows\"").is_err());
    }
}
