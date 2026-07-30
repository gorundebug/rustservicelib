use std::{
    collections::HashMap,
    convert::Infallible,
    sync::{Arc, Mutex},
};

use axum::{
    Router,
    http::{HeaderValue, StatusCode, header},
    middleware,
    response::IntoResponse,
    routing::get,
};
use futures::future::join_all;
use tokio::{net::TcpListener, sync::Mutex as AsyncMutex, task::JoinHandle};
use tokio_stream::wrappers::TcpListenerStream;
use tokio_util::sync::CancellationToken;
use tonic::{
    body::BoxBody,
    codegen::{Service, http},
    server::NamedService,
    service::Routes,
    transport::Server as TonicServer,
};

use crate::runtime::{
    common::MessageContext,
    config::{CallSemantics, ServiceConfig},
    datasink::DataSink,
    datasource::DataSource,
    environment::metrics::{Labels, Metrics},
    environment::{Lifecycle, RuntimeEnvironment, RuntimeError, RuntimeResult},
    telemetry::{GrpcServerMetricsLayer, HttpServerMetrics, observe_http_server_request},
};

async fn stop_with_connector_telemetry<T: ?Sized + Lifecycle>(
    resource: Arc<T>,
    name: String,
    kind: &'static str,
    context: MessageContext,
    metrics: Metrics,
) -> RuntimeResult<()> {
    let stop = resource.stop(context.clone());
    tokio::pin!(stop);
    let timed_out = tokio::select! {
        result = &mut stop => return result,
        _ = context.cancelled() => true,
    };
    if timed_out {
        if let Ok(counter) = metrics
            .scope(
                kind,
                [("connector".to_owned(), name.clone())]
                    .into_iter()
                    .collect(),
            )
            .counter(
                "events_total",
                "Total number of events in data connector",
                [("event".to_owned(), "stop_timeout".to_owned())]
                    .into_iter()
                    .collect::<Labels>(),
            )
        {
            counter.inc();
        }
        tracing::warn!(connector = name, kind, "data connector stopped by timeout");
        stop.await?;
    }
    Ok(())
}

fn register_configured_pools(environment: &RuntimeEnvironment) -> RuntimeResult<()> {
    let runtime = environment.runtime_config();
    let service_default = environment
        .service_id()
        .and_then(|id| runtime.service_by_id(id))
        .map(|service| service.default_call_semantics.clone());
    let mut semantics = runtime
        .links()
        .into_iter()
        .map(|link| link.call_semantics.clone())
        .collect::<Vec<_>>();
    semantics.push(service_default.unwrap_or_else(|| runtime.default_call_semantics().clone()));
    for semantics in semantics {
        match semantics {
            CallSemantics::TaskPool { pool_name } => {
                if environment.task_pool(&pool_name).is_err() {
                    let pool = crate::runtime::pool::TaskPool::new(pool_name, environment.clone())?;
                    environment.register_task_pool(pool)?;
                }
            }
            CallSemantics::PriorityTaskPool { pool_name, .. } => {
                if environment.priority_task_pool(&pool_name).is_err() {
                    let pool = crate::runtime::pool::PriorityTaskPool::new(
                        pool_name,
                        environment.clone(),
                    )?;
                    environment.register_priority_task_pool(pool)?;
                }
            }
            CallSemantics::FunctionCall | CallSemantics::ParallelCall => {}
        }
    }
    Ok(())
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum State {
    Created,
    Starting,
    Running,
    Stopping,
    Stopped,
}

pub struct ServiceApp {
    service_id: i32,
    environment: RuntimeEnvironment,
    data_sources: HashMap<i32, Arc<dyn DataSource>>,
    data_sinks: HashMap<i32, Arc<dyn DataSink>>,
    components: Vec<Arc<dyn Lifecycle>>,
    http_router: Router,
    http_shutdown: Mutex<CancellationToken>,
    http_task: AsyncMutex<Option<JoinHandle<Result<(), String>>>>,
    grpc_routes: Option<Routes>,
    grpc_shutdown: Mutex<CancellationToken>,
    grpc_task: AsyncMutex<Option<JoinHandle<Result<(), String>>>>,
    state: AsyncMutex<State>,
}

impl ServiceApp {
    pub fn new(environment: RuntimeEnvironment, config: ServiceConfig) -> RuntimeResult<Self> {
        let environment = environment.for_service(config.id);
        register_configured_pools(&environment)?;
        if let Ok(info) = environment
            .metrics()
            .scope(
                "service",
                [
                    ("service".to_owned(), config.name.clone()),
                    ("environment".to_owned(), config.environment.clone()),
                ]
                .into_iter()
                .collect(),
            )
            .gauge(
                "info",
                "Service information (value is always 1)",
                Default::default(),
            )
        {
            info.set(1);
        }
        Ok(Self {
            service_id: config.id,
            environment,
            data_sources: HashMap::new(),
            data_sinks: HashMap::new(),
            components: Vec::new(),
            http_router: Router::new(),
            http_shutdown: Mutex::new(CancellationToken::new()),
            http_task: AsyncMutex::new(None),
            grpc_routes: None,
            grpc_shutdown: Mutex::new(CancellationToken::new()),
            grpc_task: AsyncMutex::new(None),
            state: AsyncMutex::new(State::Created),
        })
    }

    pub fn config(&self) -> ServiceConfig {
        self.environment
            .service_config(self.service_id)
            .map(|config| config.as_ref().clone())
            .expect("registered service configuration is missing")
    }

    pub fn validate_reload(&self, config: &ServiceConfig) -> RuntimeResult<()> {
        let current = self.config();
        if config.id != current.id || config.name != current.name {
            return Err(RuntimeError::InvalidConfiguration(
                "service id and name cannot change during reload".to_owned(),
            ));
        }
        Ok(())
    }

    pub fn environment(&self) -> &RuntimeEnvironment {
        &self.environment
    }

    fn ensure_created(&self) -> RuntimeResult<()> {
        let config = self.config();
        let state = self
            .state
            .try_lock()
            .map_err(|_| RuntimeError::ResourceAlreadyStarted(config.name.clone()))?;
        if *state != State::Created {
            return Err(if *state == State::Stopped {
                RuntimeError::ResourceStopped(config.name.clone())
            } else {
                RuntimeError::ResourceAlreadyStarted(config.name.clone())
            });
        }
        Ok(())
    }

    pub fn register_data_source<T>(&mut self, data_source: Arc<T>) -> RuntimeResult<()>
    where
        T: DataSource + 'static,
    {
        self.ensure_created()?;
        let id = data_source.id();
        let name = data_source.name().to_owned();
        if self.data_sources.insert(id, data_source).is_some() {
            return Err(RuntimeError::DuplicateResource(name));
        }
        Ok(())
    }

    pub fn data_source(&self, id: i32) -> Option<Arc<dyn DataSource>> {
        self.data_sources.get(&id).cloned()
    }

    pub fn register_data_sink<T>(&mut self, data_sink: Arc<T>) -> RuntimeResult<()>
    where
        T: DataSink + 'static,
    {
        self.ensure_created()?;
        let id = data_sink.id();
        let name = data_sink.name().to_owned();
        if self.data_sinks.insert(id, data_sink).is_some() {
            return Err(RuntimeError::DuplicateResource(name));
        }
        Ok(())
    }

    pub fn data_sink(&self, id: i32) -> Option<Arc<dyn DataSink>> {
        self.data_sinks.get(&id).cloned()
    }

    pub fn add_component<T>(&mut self, component: Arc<T>) -> RuntimeResult<()>
    where
        T: Lifecycle + 'static,
    {
        self.ensure_created()?;
        self.components.push(component);
        Ok(())
    }

    pub fn add_http_router(&mut self, router: Router) -> RuntimeResult<()> {
        self.ensure_created()?;
        self.http_router = std::mem::take(&mut self.http_router).merge(router);
        Ok(())
    }

    pub fn add_grpc_service<S>(&mut self, service: S) -> RuntimeResult<()>
    where
        S: Service<
                http::Request<tonic::body::BoxBody>,
                Response = http::Response<BoxBody>,
                Error = Infallible,
            > + NamedService
            + Clone
            + Send
            + Sync
            + 'static,
        S::Future: Send + 'static,
    {
        self.ensure_created()?;
        self.grpc_routes = Some(match self.grpc_routes.take() {
            Some(routes) => routes.add_service(service),
            None => Routes::new(service),
        });
        Ok(())
    }

    async fn start_grpc_server(&self) -> RuntimeResult<()> {
        let config = self.config();
        let routes = self.grpc_routes.clone();
        let Some(routes) = routes else {
            return Ok(());
        };
        let address = format!("{}:{}", config.grpc_host, config.grpc_port);
        let listener = TcpListener::bind(&address)
            .await
            .map_err(|error| RuntimeError::Transport(error.to_string()))?;
        let shutdown = CancellationToken::new();
        *self
            .grpc_shutdown
            .lock()
            .expect("service gRPC shutdown lock poisoned") = shutdown.clone();
        let routes_metrics = self.environment.metrics().clone();
        *self.grpc_task.lock().await = Some(tokio::spawn(async move {
            tracing::info!(address = %address, "gRPC service listening");
            TonicServer::builder()
                .layer(GrpcServerMetricsLayer::new(
                    // The registry is shared with the Prometheus endpoint.
                    // This keeps transport and stream metrics in one scrape.
                    // The layer itself must not own transport lifecycle.
                    routes_metrics,
                ))
                .add_routes(routes)
                .serve_with_incoming_shutdown(
                    TcpListenerStream::new(listener),
                    shutdown.cancelled_owned(),
                )
                .await
                .map_err(|error| error.to_string())
        }));
        Ok(())
    }

    async fn start_http_server(&self) -> RuntimeResult<()> {
        let config = self.config();
        let mut router = self.http_router.clone();
        if !config.status_handler.is_empty() {
            let path = format!("/{}", config.status_handler.trim_start_matches('/'));
            let environment = self.environment.clone();
            router = router.route(
                &path,
                get(|| async { axum::response::Html(crate::runtime::statusweb::status_html()) }),
            );
            let data_path = format!("{path}/data");
            router = router.route(
                &data_path,
                get(move || {
                    let environment = environment.clone();
                    async move { axum::Json(crate::runtime::statusweb::network_data(&environment)) }
                }),
            );
            let graph_environment = self.environment.clone();
            router = router.route(
                &format!("{path}/graph"),
                get(move || {
                    let environment = graph_environment.clone();
                    async move {
                        match crate::runtime::statusweb::graph_yaml(&environment) {
                            Ok(graph) => {
                                ([(header::CONTENT_TYPE, "text/yaml; charset=utf-8")], graph)
                                    .into_response()
                            }
                            Err(error) => (StatusCode::INTERNAL_SERVER_ERROR, error.to_string())
                                .into_response(),
                        }
                    }
                }),
            );
            router = router.route(
                &format!("{path}/vis.min.js"),
                get(|| async {
                    let mut response = crate::runtime::statusweb::vis_js().into_response();
                    response.headers_mut().insert(
                        header::CONTENT_TYPE,
                        HeaderValue::from_static("application/javascript"),
                    );
                    response.headers_mut().insert(
                        header::CACHE_CONTROL,
                        HeaderValue::from_static("public, max-age=31536000, immutable"),
                    );
                    response
                }),
            );
            router = router.route(
                &format!("{path}/vis.min.css"),
                get(|| async {
                    let mut response = crate::runtime::statusweb::vis_css().into_response();
                    response
                        .headers_mut()
                        .insert(header::CONTENT_TYPE, HeaderValue::from_static("text/css"));
                    response.headers_mut().insert(
                        header::CACHE_CONTROL,
                        HeaderValue::from_static("public, max-age=31536000, immutable"),
                    );
                    response
                }),
            );
        }
        if !config.metrics_handler.is_empty() {
            let path = format!("/{}", config.metrics_handler.trim_start_matches('/'));
            let metrics = self.environment.metrics().clone();
            router = router.route(
                &path,
                get(move || {
                    let metrics = metrics.clone();
                    async move { metrics.render_prometheus() }
                }),
            );
        }

        let address = format!("{}:{}", config.http_host, config.http_port);
        router = router.layer(middleware::from_fn_with_state(
            HttpServerMetrics::new(
                self.environment.metrics().clone(),
                config.http_host.clone(),
                config.http_port,
            ),
            observe_http_server_request,
        ));
        let listener = TcpListener::bind(&address)
            .await
            .map_err(|error| RuntimeError::Transport(error.to_string()))?;
        let shutdown = CancellationToken::new();
        *self
            .http_shutdown
            .lock()
            .expect("service HTTP shutdown lock poisoned") = shutdown.clone();
        *self.http_task.lock().await = Some(tokio::spawn(async move {
            tracing::info!(address = %address, "HTTP service listening");
            axum::serve(listener, router)
                .with_graceful_shutdown(shutdown.cancelled_owned())
                .await
                .map_err(|error| error.to_string())
        }));
        Ok(())
    }

    pub async fn start(&self, context: MessageContext) -> RuntimeResult<()> {
        let config = self.config();
        {
            let mut state = self.state.lock().await;
            if *state != State::Created {
                return Err(RuntimeError::ResourceAlreadyStarted(config.name.clone()));
            }
            *state = State::Starting;
        }

        let data_sources = self.data_sources.values().cloned().collect::<Vec<_>>();
        let data_sinks = self.data_sinks.values().cloned().collect::<Vec<_>>();
        let components = self.components.clone();

        let result = async {
            for resource in &data_sources {
                resource.start(context.clone()).await?;
            }
            for resource in &data_sinks {
                resource.start(context.clone()).await?;
            }
            for storage in self.environment.storages() {
                storage.start(context.clone()).await?;
            }
            for pool in self.environment.task_pools() {
                pool.start()?;
            }
            for pool in self.environment.priority_task_pools() {
                pool.start()?;
            }
            for component in &components {
                component.start(context.clone()).await?;
            }
            self.start_grpc_server().await?;
            self.start_http_server().await?;
            RuntimeResult::Ok(())
        }
        .await;

        match result {
            Ok(()) => {
                *self.state.lock().await = State::Running;
                Ok(())
            }
            Err(error) => {
                tracing::error!(error = %error, "service start failed");
                // Move through Running so the normal shutdown path can safely
                // release every resource that may already have started.
                *self.state.lock().await = State::Running;
                let _ = self.stop(MessageContext::new()).await;
                Err(error)
            }
        }
    }

    pub async fn stop(&self, context: MessageContext) -> RuntimeResult<()> {
        let config = self.config();
        tracing::info!(service = %config.name, "stopping service");
        {
            let mut state = self.state.lock().await;
            match *state {
                State::Stopped => return Ok(()),
                State::Created => {
                    *state = State::Stopped;
                    return Ok(());
                }
                State::Running => *state = State::Stopping,
                _ => {
                    return Err(RuntimeError::ResourceAlreadyStarted(config.name.clone()));
                }
            }
        }

        self.grpc_shutdown
            .lock()
            .expect("service gRPC shutdown lock poisoned")
            .cancel();
        self.http_shutdown
            .lock()
            .expect("service HTTP shutdown lock poisoned")
            .cancel();

        let data_sources = self.data_sources.values().cloned().collect::<Vec<_>>();
        let components = self.components.clone();
        let mut first_error = None;
        let mut first_phase = Vec::new();
        for resource in data_sources {
            let context = context.clone();
            let name = resource.name().to_owned();
            let metrics = self.environment.metrics().clone();
            first_phase.push(tokio::spawn(async move {
                stop_with_connector_telemetry(
                    resource,
                    name,
                    "datasource_connector",
                    context,
                    metrics,
                )
                .await
            }));
        }
        for component in components {
            let context = context.clone();
            first_phase.push(tokio::spawn(async move { component.stop(context).await }));
        }
        for pool in self.environment.task_pools() {
            let context = context.clone();
            first_phase.push(tokio::spawn(async move {
                pool.stop_with_context(context).await;
                Ok(())
            }));
        }
        for pool in self.environment.priority_task_pools() {
            let context = context.clone();
            first_phase.push(tokio::spawn(async move {
                pool.stop_with_context(context).await;
                Ok(())
            }));
        }
        let delay_pool = Arc::clone(self.environment.delay_pool());
        let delay_context = context.clone();
        first_phase.push(tokio::spawn(async move {
            delay_pool.stop_with_context(delay_context).await;
            Ok(())
        }));
        for result in join_all(first_phase).await {
            let result = match result {
                Ok(result) => result,
                Err(error) => Err(RuntimeError::Transport(error.to_string())),
            };
            if let Err(error) = result {
                tracing::warn!(error = %error, "runtime resource shutdown failed");
                if first_error.is_none() {
                    first_error = Some(error);
                }
            }
        }
        for storage in self.environment.storages() {
            storage.stop(context.clone()).await;
        }
        if let Some(task) = self.grpc_task.lock().await.take() {
            let result = task
                .await
                .map_err(|error| RuntimeError::Transport(error.to_string()))
                .and_then(|result| result.map_err(RuntimeError::Transport));
            if let Err(error) = result {
                tracing::warn!(error = %error, "gRPC server shutdown failed");
                if first_error.is_none() {
                    first_error = Some(error);
                }
            }
        }
        if let Some(task) = self.http_task.lock().await.take() {
            let result = task
                .await
                .map_err(|error| RuntimeError::Transport(error.to_string()))
                .and_then(|result| result.map_err(RuntimeError::Transport));
            if let Err(error) = result {
                tracing::warn!(error = %error, "HTTP server shutdown failed");
                if first_error.is_none() {
                    first_error = Some(error);
                }
            }
        }

        let data_sinks = self.data_sinks.values().cloned().collect::<Vec<_>>();
        for result in join_all(data_sinks.into_iter().map(|resource| {
            let context = context.clone();
            let name = resource.name().to_owned();
            let metrics = self.environment.metrics().clone();
            tokio::spawn(async move {
                stop_with_connector_telemetry(
                    resource,
                    name,
                    "datasink_connector",
                    context,
                    metrics,
                )
                .await
            })
        }))
        .await
        {
            let result = match result {
                Ok(result) => result,
                Err(error) => Err(RuntimeError::Transport(error.to_string())),
            };
            if let Err(error) = result {
                tracing::warn!(error = %error, "data sink shutdown failed");
                if first_error.is_none() {
                    first_error = Some(error);
                }
            }
        }
        // Match the Go lifecycle: stop runtime resources first, then flush
        // metrics, traces and finally logs so shutdown diagnostics remain
        // observable for as long as possible.
        if let Err(error) = self.environment.metrics_engine().shutdown().await {
            tracing::warn!(error = %error, "metrics engine shutdown");
        }
        if let Err(error) = self.environment.tracing_engine().shutdown().await {
            tracing::warn!(error = %error, "tracing engine shutdown");
        }
        // Endpoint consumers keep their typed InputStream and therefore a
        // clone of this environment. Release the runtime ownership explicitly
        // after connectors have stopped to break that Arc ownership cycle.
        self.environment.clear_endpoint_consumers();
        *self.state.lock().await = State::Stopped;
        // Emit the terminal lifecycle record before the logging provider is
        // flushed. Logging after shutdown would only reach incidental layers.
        tracing::info!(service = %config.name, "service stopped");
        if let Err(error) = self.environment.logs_engine().shutdown().await {
            tracing::warn!(error = %error, "logs engine shutdown");
        }
        match first_error {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }
}
