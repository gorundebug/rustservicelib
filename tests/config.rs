use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use serde::{Deserialize, Serialize};
use servicelib::{
    api::{DataType, TemporalExecutionType, TypeDefinitionFormat},
    runtime::{
        common::MessageContext,
        config::{
            Config, ConfigLoader, ModuleConfig, RuntimeConfig, TemporalEndpointConfig, TypeConfig,
        },
        environment::{Lifecycle, metrics::Metrics},
    },
};

#[test]
fn temporal_endpoint_requires_explicit_execution_type() {
    let endpoint: TemporalEndpointConfig = serde_yaml::from_str(
        r#"
id: 1
name: submitted
idDataConnector: 2
taskQueue: automation
temporalExecutionType: Activity
activityStartToCloseTimeout: 30000
maximumAttempts: 3
"#,
    )
    .unwrap();
    assert_eq!(
        endpoint.temporal_execution_type,
        TemporalExecutionType::Activity
    );

    let missing = serde_yaml::from_str::<TemporalEndpointConfig>(
        r#"
id: 1
name: submitted
idDataConnector: 2
taskQueue: automation
activityStartToCloseTimeout: 30000
maximumAttempts: 3
"#,
    );
    assert!(missing.is_err());
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
struct TestConfig {
    service: Service,
    feature: Feature,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
struct Service {
    port: u16,
    name: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
struct Feature {
    enabled: bool,
    limit: u32,
}

impl Config for TestConfig {
    fn apply_environment(&mut self) -> Result<(), String> {
        Ok(())
    }

    fn validate(&self) -> Result<(), String> {
        if self.feature.limit == 0 {
            return Err("feature.limit must be positive".to_owned());
        }
        Ok(())
    }
}

fn defaults() -> TestConfig {
    TestConfig {
        service: Service {
            port: 8080,
            name: "default".to_owned(),
        },
        feature: Feature {
            enabled: false,
            limit: 10,
        },
    }
}

#[test]
fn base_and_override_are_merged_over_generated_defaults() {
    let directory = tempfile::tempdir().unwrap();
    let base = directory.path().join("config.yaml");
    let overrides = directory.path().join("overrides.yaml");
    std::fs::write(
        &base,
        "service:\n  name: orders\nfeature:\n  enabled: true\n",
    )
    .unwrap();
    std::fs::write(&overrides, "service:\n  port: 9091\n").unwrap();

    let loader = ConfigLoader::load(
        Some(base),
        Some(overrides),
        defaults(),
        &Metrics::default(),
        "orders",
    )
    .unwrap();

    assert_eq!(
        loader.current().as_ref(),
        &TestConfig {
            service: Service {
                port: 9091,
                name: "orders".to_owned(),
            },
            feature: Feature {
                enabled: true,
                limit: 10,
            },
        }
    );
}

#[test]
fn failed_reload_keeps_last_valid_snapshot_and_counts_error() {
    let directory = tempfile::tempdir().unwrap();
    let base = directory.path().join("config.yaml");
    let overrides = directory.path().join("overrides.yaml");
    std::fs::write(
        &base,
        "service:\n  name: orders\nfeature:\n  enabled: true\n",
    )
    .unwrap();
    std::fs::write(&overrides, "service:\n  port: 9091\n").unwrap();
    let metrics = Metrics::default();
    let loader = ConfigLoader::load(
        Some(base),
        Some(overrides.clone()),
        defaults(),
        &metrics,
        "orders",
    )
    .unwrap();
    let called = Arc::new(AtomicUsize::new(0));
    let called_by_handler = Arc::clone(&called);
    loader.set_reload_handler(move |_, _| {
        called_by_handler.fetch_add(1, Ordering::Relaxed);
        Ok(())
    });

    std::fs::write(&overrides, "feature:\n  limit: 0\n").unwrap();
    assert!(loader.reload().is_err());
    assert_eq!(loader.current().service.port, 9091);
    assert_eq!(called.load(Ordering::Relaxed), 0);
    assert!(
        metrics
            .render_prometheus()
            .contains("service_config_reloads_total{event=\"error\",service=\"orders\"} 1")
    );

    std::fs::write(
        &overrides,
        "service:\n  port: 9191\nfeature:\n  limit: 20\n",
    )
    .unwrap();
    loader.reload().unwrap();
    assert_eq!(loader.current().service.port, 9191);
    assert_eq!(called.load(Ordering::Relaxed), 1);
    assert!(
        metrics
            .render_prometheus()
            .contains("service_config_reloads_total{event=\"success\",service=\"orders\"} 1")
    );
}

#[tokio::test]
async fn override_watcher_reloads_after_atomic_file_replacement() {
    let directory = tempfile::tempdir().unwrap();
    let base = directory.path().join("config.yaml");
    let overrides = directory.path().join("overrides.yaml");
    std::fs::write(&base, "service:\n  name: orders\n").unwrap();
    std::fs::write(&overrides, "service:\n  port: 9091\n").unwrap();
    let loader = ConfigLoader::load(
        Some(base),
        Some(overrides.clone()),
        defaults(),
        &Metrics::default(),
        "orders",
    )
    .unwrap();
    loader.start(MessageContext::new()).await.unwrap();

    let replacement = directory.path().join("overrides.new.yaml");
    std::fs::write(&replacement, "service:\n  port: 9191\n").unwrap();
    std::fs::rename(replacement, &overrides).unwrap();

    tokio::time::timeout(std::time::Duration::from_secs(3), async {
        while loader.current().service.port != 9191 {
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("configuration watcher did not publish the replacement");
    loader.stop(MessageContext::new()).await.unwrap();
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
struct MetadataConfig;

impl Config for MetadataConfig {
    fn apply_environment(&mut self) -> Result<(), String> {
        Ok(())
    }

    fn modules(&self) -> Vec<ModuleConfig> {
        vec![ModuleConfig {
            name: "shared".to_owned(),
            path: "example/shared".to_owned(),
            properties: Default::default(),
        }]
    }

    fn types(&self) -> Vec<TypeConfig> {
        vec![TypeConfig {
            name: "Message".to_owned(),
            data_type: DataType::Struct,
            type_definition: "Message".to_owned(),
            type_import: "crate::message".to_owned(),
            value_type: String::new(),
            key_type: String::new(),
            package: String::new(),
            module: "shared".to_owned(),
            definition_format: TypeDefinitionFormat::Native,
            public_type: true,
            transfer_by_value: false,
            use_alias: false,
            properties: Default::default(),
        }]
    }
}

#[test]
fn runtime_indexes_generated_modules_and_types() {
    let runtime = RuntimeConfig::new(&MetadataConfig).unwrap();

    assert_eq!(
        runtime.module_by_name("shared").unwrap().path,
        "example/shared"
    );
    assert_eq!(runtime.type_by_name("Message").unwrap().module, "shared");
}
