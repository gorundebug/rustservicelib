use std::{
    path::{Path, PathBuf},
    sync::{Arc, Mutex, RwLock},
};

use arc_swap::ArcSwap;
use async_trait::async_trait;
use notify::{Event, EventKind, RecommendedWatcher, RecursiveMode, Watcher};
use serde::{Serialize, de::DeserializeOwned};
use tokio::{sync::Mutex as AsyncMutex, task::JoinHandle};
use tokio_util::sync::CancellationToken;

use super::{
    LinkConfig, PoolConfig, RuntimeConfig, RuntimeDataConnectorConfig, RuntimeEndpointConfig,
    RuntimeStreamConfig, ServiceConfig,
};
use crate::runtime::{
    common::MessageContext,
    environment::{
        Lifecycle, RuntimeError, RuntimeResult,
        metrics::{Int64Counter, Labels, Metrics},
    },
};

/// Application configuration consumed by [`ConfigLoader`].
///
/// Generated configurations implement this trait. Environment variables are
/// applied after the base and override YAML files have been merged, matching
/// the Go servicelib configuration precedence.
pub trait Config: Clone + Serialize + DeserializeOwned + Send + Sync + 'static {
    fn apply_environment(&mut self) -> Result<(), String>;

    fn validate(&self) -> Result<(), String> {
        Ok(())
    }

    fn services(&self) -> Vec<ServiceConfig> {
        Vec::new()
    }

    fn streams(&self) -> Vec<RuntimeStreamConfig> {
        Vec::new()
    }

    fn pools(&self) -> Vec<PoolConfig> {
        Vec::new()
    }

    fn data_connectors(&self) -> Vec<RuntimeDataConnectorConfig> {
        Vec::new()
    }

    fn endpoints(&self) -> Vec<RuntimeEndpointConfig> {
        Vec::new()
    }

    fn links(&self) -> Vec<LinkConfig> {
        Vec::new()
    }

    fn default_call_semantics(&self) -> super::CallSemantics {
        super::CallSemantics::default()
    }
}

type ReloadHandler<C> =
    dyn Fn(Arc<C>, Arc<RuntimeConfig>) -> Result<(), String> + Send + Sync + 'static;

struct ConfigLoaderInner<C: Config> {
    base_path: Option<PathBuf>,
    override_path: Option<PathBuf>,
    defaults: C,
    current: ArcSwap<C>,
    runtime_config: ArcSwap<RuntimeConfig>,
    reload_handler: RwLock<Option<Arc<ReloadHandler<C>>>>,
    watcher: Mutex<Option<RecommendedWatcher>>,
    watched_target: Mutex<Option<PathBuf>>,
    cancellation: Mutex<CancellationToken>,
    task: AsyncMutex<Option<JoinHandle<()>>>,
    reload_success: Option<Int64Counter>,
    reload_error: Option<Int64Counter>,
}

/// Loads base YAML plus an optional override YAML and watches the override.
///
/// Reload is transactional: parsing, merging, environment application,
/// validation, and the reload hook must all succeed before the new snapshot is
/// published. A failed attempt leaves the previous snapshot active.
#[derive(Clone)]
pub struct ConfigLoader<C: Config> {
    inner: Arc<ConfigLoaderInner<C>>,
}

impl<C: Config> ConfigLoader<C> {
    pub fn load(
        base_path: Option<impl Into<PathBuf>>,
        override_path: Option<impl Into<PathBuf>>,
        defaults: C,
        metrics: &Metrics,
        service_name: impl Into<String>,
    ) -> RuntimeResult<Self> {
        let base_path = base_path.map(Into::into);
        let override_path = override_path.map(Into::into);
        let initial = load_snapshot(
            base_path.as_deref(),
            override_path.as_deref(),
            defaults.clone(),
        )?;
        let runtime_config = RuntimeConfig::new(&initial)?;
        let scope = metrics.scope(
            "service",
            [("service".to_owned(), service_name.into())]
                .into_iter()
                .collect(),
        );
        let reload_success = scope
            .counter(
                "config_reloads_total",
                "Total number of config reload attempts",
                [("event".to_owned(), "success".to_owned())]
                    .into_iter()
                    .collect::<Labels>(),
            )
            .ok();
        let reload_error = scope
            .counter(
                "config_reloads_total",
                "Total number of config reload attempts",
                [("event".to_owned(), "error".to_owned())]
                    .into_iter()
                    .collect::<Labels>(),
            )
            .ok();

        Ok(Self {
            inner: Arc::new(ConfigLoaderInner {
                base_path,
                override_path,
                defaults,
                current: ArcSwap::from_pointee(initial),
                runtime_config: ArcSwap::from_pointee(runtime_config),
                reload_handler: RwLock::new(None),
                watcher: Mutex::new(None),
                watched_target: Mutex::new(None),
                cancellation: Mutex::new(CancellationToken::new()),
                task: AsyncMutex::new(None),
                reload_success,
                reload_error,
            }),
        })
    }

    pub fn current(&self) -> Arc<C> {
        self.inner.current.load_full()
    }

    pub fn runtime_config(&self) -> Arc<RuntimeConfig> {
        self.inner.runtime_config.load_full()
    }

    pub fn set_reload_handler<F>(&self, handler: F)
    where
        F: Fn(Arc<C>, Arc<RuntimeConfig>) -> Result<(), String> + Send + Sync + 'static,
    {
        *self
            .inner
            .reload_handler
            .write()
            .expect("configuration reload handler lock poisoned") = Some(Arc::new(handler));
    }

    pub fn reload(&self) -> RuntimeResult<Arc<C>> {
        let result = self.reload_candidate();
        match result {
            Ok(candidate) => {
                if let Some(counter) = &self.inner.reload_success {
                    counter.inc();
                }
                tracing::info!("service configuration reloaded");
                Ok(candidate)
            }
            Err(error) => {
                if let Some(counter) = &self.inner.reload_error {
                    counter.inc();
                }
                Err(error)
            }
        }
    }

    fn reload_candidate(&self) -> RuntimeResult<Arc<C>> {
        let candidate = Arc::new(load_snapshot(
            self.inner.base_path.as_deref(),
            self.inner.override_path.as_deref(),
            self.inner.defaults.clone(),
        )?);
        let runtime_config = Arc::new(RuntimeConfig::new(candidate.as_ref())?);

        if let Some(handler) = self
            .inner
            .reload_handler
            .read()
            .expect("configuration reload handler lock poisoned")
            .clone()
        {
            handler(candidate.clone(), runtime_config.clone())
                .map_err(RuntimeError::InvalidConfiguration)?;
        }

        self.inner.current.store(candidate.clone());
        self.inner.runtime_config.store(runtime_config);
        Ok(candidate)
    }

    fn reload_from_watch(&self) {
        if let Err(error) = self.reload() {
            tracing::error!(error = %error, "service configuration reload failed");
        }
    }

    fn is_relevant_event(&self, event: &Event) -> bool {
        if !matches!(
            event.kind,
            EventKind::Create(_) | EventKind::Modify(_) | EventKind::Remove(_)
        ) {
            return false;
        }
        let Some(configured) = self.inner.override_path.as_deref() else {
            return false;
        };
        let configured = clean_path(configured);
        let current_target = std::fs::canonicalize(&configured).ok();
        let mut watched_target = self
            .inner
            .watched_target
            .lock()
            .expect("watched configuration target lock poisoned");
        let target_changed = current_target != *watched_target;
        if target_changed {
            *watched_target = current_target.clone();
        }
        target_changed
            || event.paths.iter().any(|path| {
                let path = clean_path(path);
                path == configured
                    || current_target
                        .as_ref()
                        .is_some_and(|target| path == *target)
            })
    }
}

#[async_trait]
impl<C: Config> Lifecycle for ConfigLoader<C> {
    async fn start(&self, _context: MessageContext) -> RuntimeResult<()> {
        let Some(override_path) = self.inner.override_path.clone() else {
            return Ok(());
        };
        let directory = override_path
            .parent()
            .filter(|path| !path.as_os_str().is_empty())
            .unwrap_or_else(|| Path::new("."))
            .to_path_buf();
        let (sender, mut receiver) = tokio::sync::mpsc::unbounded_channel();
        let mut watcher = notify::recommended_watcher(move |event| {
            let _ = sender.send(event);
        })
        .map_err(config_error)?;
        watcher
            .watch(&directory, RecursiveMode::NonRecursive)
            .map_err(config_error)?;
        *self
            .inner
            .watched_target
            .lock()
            .expect("watched configuration target lock poisoned") =
            std::fs::canonicalize(&override_path).ok();

        let cancellation = CancellationToken::new();
        *self
            .inner
            .cancellation
            .lock()
            .expect("configuration cancellation lock poisoned") = cancellation.clone();
        let loader = self.clone();
        *self.inner.task.lock().await = Some(tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = cancellation.cancelled() => return,
                    event = receiver.recv() => {
                        let Some(event) = event else {
                            return;
                        };
                        match event {
                            Ok(event) if loader.is_relevant_event(&event) => {
                                loader.reload_from_watch();
                            }
                            Ok(_) => {}
                            Err(error) => {
                                tracing::error!(error = %error, "configuration watcher error");
                            }
                        }
                    }
                }
            }
        }));
        *self
            .inner
            .watcher
            .lock()
            .expect("configuration watcher lock poisoned") = Some(watcher);
        Ok(())
    }

    async fn stop(&self, _context: MessageContext) -> RuntimeResult<()> {
        self.inner
            .cancellation
            .lock()
            .expect("configuration cancellation lock poisoned")
            .cancel();
        self.inner
            .watcher
            .lock()
            .expect("configuration watcher lock poisoned")
            .take();
        if let Some(task) = self.inner.task.lock().await.take() {
            let _ = task.await;
        }
        Ok(())
    }
}

fn load_snapshot<C: Config>(
    base_path: Option<&Path>,
    override_path: Option<&Path>,
    defaults: C,
) -> RuntimeResult<C> {
    let mut value = serde_yaml::to_value(defaults).map_err(config_error)?;
    if let Some(base_path) = base_path {
        merge_yaml(&mut value, read_yaml(base_path)?);
    }
    if let Some(override_path) = override_path {
        merge_yaml(&mut value, read_yaml(override_path)?);
    }
    let mut defaults: C = serde_yaml::from_value(value).map_err(config_error)?;
    defaults
        .apply_environment()
        .map_err(RuntimeError::InvalidConfiguration)?;
    defaults
        .validate()
        .map_err(RuntimeError::InvalidConfiguration)?;
    Ok(defaults)
}

fn read_yaml(path: &Path) -> RuntimeResult<serde_yaml::Value> {
    let data = std::fs::read(path).map_err(config_error)?;
    serde_yaml::from_slice(&data).map_err(config_error)
}

fn merge_yaml(base: &mut serde_yaml::Value, override_value: serde_yaml::Value) {
    match (base, override_value) {
        (serde_yaml::Value::Mapping(base), serde_yaml::Value::Mapping(override_map)) => {
            for (key, value) in override_map {
                match base.get_mut(&key) {
                    Some(base_value) => merge_yaml(base_value, value),
                    None => {
                        base.insert(key, value);
                    }
                }
            }
        }
        (base, override_value) => *base = override_value,
    }
}

fn clean_path(path: &Path) -> PathBuf {
    path.components().collect()
}

fn config_error(error: impl std::fmt::Display) -> RuntimeError {
    RuntimeError::InvalidConfiguration(error.to_string())
}
