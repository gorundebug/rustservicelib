use std::{
    collections::HashMap,
    fmt::Display,
    future::Future,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
    time::Instant,
};

use axum::{
    body::Body,
    extract::{MatchedPath, Request, State},
    http::{HeaderMap, Method, Response},
    middleware::Next,
};
use http_body::{Body as HttpBody, Frame, SizeHint};
use tonic::{Status, body::BoxBody, codegen::Bytes};
use tower::{Layer, Service};
use tracing::Instrument;
use tracing_opentelemetry::OpenTelemetrySpanExt;

use crate::runtime::{
    common::MessageContext,
    environment::metrics::{Float64Histogram, Int64Gauge, Labels, Metrics, MetricsScope},
};

pub(crate) mod librdkafka_statistics;
pub mod opentelemetry;

use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};

#[derive(Default)]
pub struct Counter(AtomicU64);

impl Counter {
    pub fn inc(&self) {
        self.0.fetch_add(1, Ordering::Relaxed);
    }

    pub fn get(&self) -> u64 {
        self.0.load(Ordering::Relaxed)
    }
}

#[derive(Default)]
pub struct Gauge(AtomicI64);

impl Gauge {
    pub fn set(&self, value: i64) {
        self.0.store(value, Ordering::Relaxed);
    }

    pub fn get(&self) -> i64 {
        self.0.load(Ordering::Relaxed)
    }
}

pub(crate) const DURATION_BUCKETS_SECONDS: &[f64] = &[
    0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0,
];
const BODY_SIZE_BUCKETS_BYTES: &[f64] = &[
    64.0, 256.0, 1024.0, 4096.0, 16384.0, 65536.0, 262144.0, 1048576.0,
];
pub(crate) const HTTP_STATUS_CODES: &[u16] = &[
    200, 201, 202, 204, 400, 401, 403, 404, 405, 408, 409, 413, 415, 422, 429, 500, 502, 503, 504,
];

/// Record the OpenTelemetry error status as well as a human-readable error
/// field. The special `otel.status_*` fields are consumed by
/// `tracing-opentelemetry`; an error log alone does not change span status.
pub(crate) fn record_span_error(span: &tracing::Span, error: impl Display) {
    let message = error.to_string();
    span.record("error", message.as_str());
    span.record("otel.status_code", "ERROR");
    span.record("otel.status_message", message.as_str());
}

pub(crate) struct HttpClientObservation {
    metrics: HttpClientMetrics,
    started_at: Option<Instant>,
    request_body_size: usize,
}

#[derive(Clone)]
pub(crate) struct HttpClientMetrics {
    inner: Arc<HttpClientMetricsInner>,
}

struct HttpClientMetricsInner {
    enabled: bool,
    outcomes: HashMap<u16, HttpOutcomeMetrics>,
    transport_error: HttpOutcomeMetrics,
}

impl HttpClientMetrics {
    pub(crate) fn new(metrics: Metrics, method: &str, url: &str) -> Self {
        let enabled = !metrics.is_noop();
        let parsed = reqwest::Url::parse(url).ok();
        let server_address = parsed
            .as_ref()
            .and_then(reqwest::Url::host_str)
            .unwrap_or_default()
            .to_owned();
        let server_port = parsed
            .as_ref()
            .and_then(reqwest::Url::port_or_known_default)
            .map_or_else(String::new, |port| port.to_string());
        let labels = [
            ("http_request_method".to_owned(), method.to_owned()),
            ("url_full".to_owned(), url.to_owned()),
            ("server_address".to_owned(), server_address),
            ("server_port".to_owned(), server_port),
            ("network_protocol_version".to_owned(), String::new()),
        ]
        .into_iter()
        .collect::<Labels>();
        let scope = metrics.scope("http_client", Labels::new());
        let outcomes = HTTP_STATUS_CODES
            .iter()
            .copied()
            .map(|status| {
                let mut labels = labels.clone();
                labels.insert("http_response_status_code".to_owned(), status.to_string());
                labels.insert("error_type".to_owned(), String::new());
                (status, HttpOutcomeMetrics::new_with_labels(&scope, labels))
            })
            .collect();
        let mut error_labels = labels;
        error_labels.insert("http_response_status_code".to_owned(), "500".to_owned());
        error_labels.insert("error_type".to_owned(), "transport_error".to_owned());
        Self {
            inner: Arc::new(HttpClientMetricsInner {
                enabled,
                outcomes,
                transport_error: HttpOutcomeMetrics::new_with_labels(&scope, error_labels),
            }),
        }
    }

    pub(crate) fn start(&self, request_body_size: usize) -> HttpClientObservation {
        HttpClientObservation {
            metrics: self.clone(),
            started_at: self.inner.enabled.then(Instant::now),
            request_body_size,
        }
    }
}

impl HttpClientObservation {
    pub(crate) fn finish(
        self,
        status: Option<u16>,
        response_body_size: Option<usize>,
        error: bool,
    ) {
        let Some(started_at) = self.started_at else {
            return;
        };
        let metrics = if error {
            &self.metrics.inner.transport_error
        } else {
            status
                .and_then(|status| self.metrics.inner.outcomes.get(&status))
                .or_else(|| self.metrics.inner.outcomes.get(&500))
                .expect("HTTP client metrics must register status 500")
        };
        if let Some(duration) = &metrics.request_duration {
            duration.observe(started_at.elapsed().as_secs_f64());
        }
        if let Some(size) = &metrics.request_body_size {
            size.observe(self.request_body_size as f64);
        }
        if let Some(response_body_size) = response_body_size
            && let Some(size) = &metrics.response_body_size
        {
            size.observe(response_body_size as f64);
        }
    }
}

pub(crate) struct GrpcClientObservation {
    metrics: GrpcClientMetrics,
    started_at: Option<Instant>,
}

#[derive(Clone)]
pub(crate) struct GrpcClientMetrics {
    inner: Arc<GrpcClientMetricsInner>,
}

struct GrpcClientMetricsInner {
    enabled: bool,
    durations: HashMap<&'static str, Option<Float64Histogram>>,
}

impl GrpcClientMetrics {
    pub(crate) fn new(metrics: Metrics, method: &str) -> Self {
        let enabled = !metrics.is_noop();
        let scope = metrics.scope("rpc_client", Labels::new());
        let base_labels = [
            ("rpc_system_name".to_owned(), "grpc".to_owned()),
            ("rpc_method".to_owned(), method.to_owned()),
        ]
        .into_iter()
        .collect::<Labels>();
        let durations = GRPC_STATUS_NAMES
            .into_iter()
            .map(|status| {
                let mut labels = base_labels.clone();
                labels.insert("rpc_response_status_code".to_owned(), status.to_owned());
                let histogram = scope
                    .histogram(
                        "call_duration_seconds",
                        "Duration of a gRPC client call in seconds",
                        labels,
                        Some(DURATION_BUCKETS_SECONDS.to_vec()),
                    )
                    .ok();
                (status, histogram)
            })
            .collect();
        Self {
            inner: Arc::new(GrpcClientMetricsInner { enabled, durations }),
        }
    }

    pub(crate) fn start(&self) -> GrpcClientObservation {
        GrpcClientObservation {
            metrics: self.clone(),
            started_at: self.inner.enabled.then(Instant::now),
        }
    }

    pub(crate) fn observe(&self, status: &str, duration: f64) {
        let status = grpc_status_name(status);
        if let Some(Some(histogram)) = self.inner.durations.get(status) {
            histogram.observe(duration);
        }
    }
}

impl GrpcClientObservation {
    pub(crate) fn finish(self, status: &str) {
        if let Some(started_at) = self.started_at {
            self.metrics
                .observe(status, started_at.elapsed().as_secs_f64());
        }
    }
}

type HttpRouteMap = HashMap<Method, HashMap<String, Arc<HttpRouteMetrics>>>;

#[derive(Clone)]
pub(crate) struct HttpServerMetrics {
    enabled: bool,
    routes: Arc<HttpRouteMap>,
    fallback: Arc<HttpRouteMetrics>,
    host: Arc<str>,
    port: u16,
}

pub(crate) struct HttpRouteMetricSpec {
    pub(crate) method: Method,
    pub(crate) route: String,
    pub(crate) statuses: Vec<u16>,
}

struct HttpRouteMetrics {
    method: String,
    route: String,
    active_requests: Option<Int64Gauge>,
    outcomes: HashMap<u16, HttpOutcomeMetrics>,
}

struct HttpOutcomeMetrics {
    request_duration: Option<Float64Histogram>,
    request_body_size: Option<Float64Histogram>,
    response_body_size: Option<Float64Histogram>,
}

impl HttpServerMetrics {
    pub(crate) fn new(
        metrics: Metrics,
        host: String,
        port: u16,
        specs: Vec<HttpRouteMetricSpec>,
    ) -> Self {
        let enabled = !metrics.is_noop();
        let host: Arc<str> = host.into();
        let scope = metrics.scope("http_server", Labels::new());
        let base_labels = [
            ("url_scheme".to_owned(), "http".to_owned()),
            ("server_address".to_owned(), host.to_string()),
            ("server_port".to_owned(), port.to_string()),
        ]
        .into_iter()
        .collect::<Labels>();
        let mut routes = HttpRouteMap::new();
        for spec in specs {
            let metrics = Arc::new(HttpRouteMetrics::new(
                scope.clone(),
                base_labels.clone(),
                &spec.method,
                &spec.route,
                spec.statuses,
            ));
            routes
                .entry(spec.method)
                .or_default()
                .insert(spec.route, metrics);
        }
        let fallback = Arc::new(HttpRouteMetrics::new(
            scope,
            base_labels,
            &Method::GET,
            "<unmatched>",
            HTTP_STATUS_CODES.to_vec(),
        ));
        Self {
            enabled,
            routes: Arc::new(routes),
            fallback,
            host,
            port,
        }
    }

    fn route(&self, method: &Method, route: &str) -> Arc<HttpRouteMetrics> {
        self.routes
            .get(method)
            .and_then(|routes| routes.get(route))
            .map_or_else(|| Arc::clone(&self.fallback), Arc::clone)
    }
}

impl HttpRouteMetrics {
    fn new(
        scope: MetricsScope,
        mut base_labels: Labels,
        method: &Method,
        route: &str,
        statuses: Vec<u16>,
    ) -> Self {
        let method = method.as_str().to_owned();
        let route = route.to_owned();
        base_labels.insert("http_request_method".to_owned(), method.clone());
        base_labels.insert("http_route".to_owned(), route.clone());
        let active_requests = scope
            .gauge(
                "active_requests",
                "Number of active HTTP server requests",
                base_labels.clone(),
            )
            .ok();
        let outcomes = statuses
            .into_iter()
            .map(|status| {
                (
                    status,
                    HttpOutcomeMetrics::new(&scope, &base_labels, status),
                )
            })
            .collect();
        Self {
            method,
            route,
            active_requests,
            outcomes,
        }
    }

    fn outcome(&self, status: u16) -> &HttpOutcomeMetrics {
        self.outcomes
            .get(&status)
            .or_else(|| self.outcomes.get(&500))
            .expect("HTTP metric route must register status 500")
    }
}

impl HttpOutcomeMetrics {
    fn new(scope: &MetricsScope, base_labels: &Labels, status: u16) -> Self {
        let mut labels = base_labels.clone();
        labels.insert("http_response_status_code".to_owned(), status.to_string());
        labels.insert(
            "error_type".to_owned(),
            if status >= 500 {
                "http_server_error".to_owned()
            } else {
                String::new()
            },
        );
        Self::new_with_labels(scope, labels)
    }

    fn new_with_labels(scope: &MetricsScope, labels: Labels) -> Self {
        Self {
            request_duration: scope
                .histogram(
                    "request_duration_seconds",
                    "Duration of an HTTP server request in seconds",
                    labels.clone(),
                    Some(DURATION_BUCKETS_SECONDS.to_vec()),
                )
                .ok(),
            request_body_size: scope
                .histogram(
                    "request_body_size_bytes",
                    "Size of an HTTP server request body in bytes",
                    labels.clone(),
                    Some(BODY_SIZE_BUCKETS_BYTES.to_vec()),
                )
                .ok(),
            response_body_size: scope
                .histogram(
                    "response_body_size_bytes",
                    "Size of an HTTP server response body in bytes",
                    labels,
                    Some(BODY_SIZE_BUCKETS_BYTES.to_vec()),
                )
                .ok(),
        }
    }
}

fn content_length(headers: &HeaderMap) -> Option<f64> {
    headers
        .get(axum::http::header::CONTENT_LENGTH)?
        .to_str()
        .ok()?
        .parse::<f64>()
        .ok()
}

pub(crate) async fn observe_http_server_request(
    State(state): State<HttpServerMetrics>,
    request: Request,
    next: Next,
) -> Response<Body> {
    let tracing_parent = MessageContext::tracing_parent_from_http_headers(request.headers());
    if !state.enabled && tracing_parent.is_none() {
        return next.run(request).await;
    }

    let method = request.method().clone();
    let route = request
        .extensions()
        .get::<MatchedPath>()
        .map_or_else(|| request.uri().path(), MatchedPath::as_str);
    let route_metrics = state.enabled.then(|| state.route(&method, route));
    let request_body_size = state
        .enabled
        .then(|| content_length(request.headers()))
        .flatten();
    if let Some(active) = route_metrics
        .as_ref()
        .and_then(|metrics| metrics.active_requests.as_ref())
    {
        active.inc();
    }

    let started_at = state.enabled.then(Instant::now);
    let span = tracing_parent.map(|parent| {
        let span = tracing::info_span!(
            "http.server.request",
            http.request.method = %method,
            http.route = %route,
            server.address = %state.host,
            server.port = state.port,
            error = tracing::field::Empty,
            otel.status_code = tracing::field::Empty,
            otel.status_message = tracing::field::Empty,
        );
        let _ = span.set_parent(parent);
        span
    });
    let response = if let Some(span) = &span {
        next.run(request).instrument(span.clone()).await
    } else {
        next.run(request).await
    };
    if let Some(active) = route_metrics
        .as_ref()
        .and_then(|metrics| metrics.active_requests.as_ref())
    {
        active.dec();
    }

    let status = response.status();
    if status.is_server_error()
        && let Some(span) = &span
    {
        record_span_error(span, format!("HTTP status {}", status.as_u16()));
    }
    if let (Some(route_metrics), Some(started_at)) = (&route_metrics, started_at) {
        let metrics = route_metrics.outcome(status.as_u16());
        let elapsed = started_at.elapsed().as_secs_f64();
        if let Some(duration) = &metrics.request_duration {
            duration.observe(elapsed);
        }
        if let Some(size) = request_body_size
            && let Some(histogram) = &metrics.request_body_size
        {
            histogram.observe(size);
        }
        if let Some(size) = content_length(response.headers())
            && let Some(histogram) = &metrics.response_body_size
        {
            histogram.observe(size);
        }
        if let Some(span) = &span {
            span.in_scope(|| {
                tracing::info!(
                    http.request.method = %route_metrics.method,
                    http.route = %route_metrics.route,
                    http.response.status_code = status.as_u16(),
                    duration_seconds = elapsed,
                    "HTTP request completed"
                );
            });
        }
    }
    response
}

#[derive(Clone)]
pub(crate) struct GrpcServerMetricsLayer {
    metrics: GrpcServerMetrics,
}

impl GrpcServerMetricsLayer {
    pub(crate) fn new(metrics: Metrics, methods: Vec<String>) -> Self {
        Self {
            metrics: GrpcServerMetrics::new(metrics, methods),
        }
    }
}

impl<S> Layer<S> for GrpcServerMetricsLayer {
    type Service = GrpcServerMetricsService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        GrpcServerMetricsService {
            inner,
            metrics: self.metrics.clone(),
        }
    }
}

#[derive(Clone)]
pub(crate) struct GrpcServerMetricsService<S> {
    inner: S,
    metrics: GrpcServerMetrics,
}

type GrpcMethodMap = HashMap<String, Arc<GrpcMethodMetrics>>;

#[derive(Clone)]
struct GrpcServerMetrics {
    enabled: bool,
    methods: Arc<GrpcMethodMap>,
    fallback: Arc<GrpcMethodMetrics>,
}

struct GrpcMethodMetrics {
    method: String,
    durations: HashMap<&'static str, Option<Float64Histogram>>,
}

struct GrpcCallObservation {
    metrics: Option<Arc<GrpcMethodMetrics>>,
    started_at: Instant,
    span: Option<tracing::Span>,
}

const GRPC_STATUS_NAMES: [&str; 17] = [
    "OK",
    "CANCELLED",
    "UNKNOWN",
    "INVALID_ARGUMENT",
    "DEADLINE_EXCEEDED",
    "NOT_FOUND",
    "ALREADY_EXISTS",
    "PERMISSION_DENIED",
    "RESOURCE_EXHAUSTED",
    "FAILED_PRECONDITION",
    "ABORTED",
    "OUT_OF_RANGE",
    "UNIMPLEMENTED",
    "INTERNAL",
    "UNAVAILABLE",
    "DATA_LOSS",
    "UNAUTHENTICATED",
];

impl GrpcServerMetrics {
    fn new(metrics: Metrics, methods: Vec<String>) -> Self {
        let enabled = !metrics.is_noop();
        let scope = metrics.scope("rpc_server", Labels::new());
        let methods = methods
            .into_iter()
            .map(|method| {
                let metrics = Arc::new(GrpcMethodMetrics::new(&scope, &method));
                (method, metrics)
            })
            .collect();
        Self {
            enabled,
            methods: Arc::new(methods),
            fallback: Arc::new(GrpcMethodMetrics::new(&scope, "<unmatched>")),
        }
    }

    fn method(&self, method: &str) -> Arc<GrpcMethodMetrics> {
        self.methods
            .get(method)
            .map_or_else(|| Arc::clone(&self.fallback), Arc::clone)
    }
}

impl GrpcMethodMetrics {
    fn new(scope: &MetricsScope, method: &str) -> Self {
        let method = method.to_owned();
        let base_labels = [
            ("rpc_system_name".to_owned(), "grpc".to_owned()),
            ("rpc_method".to_owned(), method.clone()),
        ]
        .into_iter()
        .collect::<Labels>();
        let durations = GRPC_STATUS_NAMES
            .into_iter()
            .map(|status| {
                let mut labels = base_labels.clone();
                labels.insert("rpc_response_status_code".to_owned(), status.to_owned());
                let histogram = scope
                    .histogram(
                        "call_duration_seconds",
                        "Duration of a gRPC server call in seconds",
                        labels,
                        Some(DURATION_BUCKETS_SECONDS.to_vec()),
                    )
                    .ok();
                (status, histogram)
            })
            .collect();
        Self { method, durations }
    }

    fn observe(&self, status: &'static str, duration: f64) {
        if let Some(Some(histogram)) = self.durations.get(status) {
            histogram.observe(duration);
        }
    }
}

fn grpc_status_name(status: &str) -> &'static str {
    match status {
        "0" => "OK",
        "1" => "CANCELLED",
        "2" => "UNKNOWN",
        "3" => "INVALID_ARGUMENT",
        "4" => "DEADLINE_EXCEEDED",
        "5" => "NOT_FOUND",
        "6" => "ALREADY_EXISTS",
        "7" => "PERMISSION_DENIED",
        "8" => "RESOURCE_EXHAUSTED",
        "9" => "FAILED_PRECONDITION",
        "10" => "ABORTED",
        "11" => "OUT_OF_RANGE",
        "12" => "UNIMPLEMENTED",
        "13" => "INTERNAL",
        "14" => "UNAVAILABLE",
        "15" => "DATA_LOSS",
        "16" => "UNAUTHENTICATED",
        _ => GRPC_STATUS_NAMES
            .iter()
            .copied()
            .find(|candidate| *candidate == status)
            .unwrap_or("UNKNOWN"),
    }
}

fn tonic_code_name(code: tonic::Code) -> &'static str {
    match code {
        tonic::Code::Ok => "OK",
        tonic::Code::Cancelled => "CANCELLED",
        tonic::Code::Unknown => "UNKNOWN",
        tonic::Code::InvalidArgument => "INVALID_ARGUMENT",
        tonic::Code::DeadlineExceeded => "DEADLINE_EXCEEDED",
        tonic::Code::NotFound => "NOT_FOUND",
        tonic::Code::AlreadyExists => "ALREADY_EXISTS",
        tonic::Code::PermissionDenied => "PERMISSION_DENIED",
        tonic::Code::ResourceExhausted => "RESOURCE_EXHAUSTED",
        tonic::Code::FailedPrecondition => "FAILED_PRECONDITION",
        tonic::Code::Aborted => "ABORTED",
        tonic::Code::OutOfRange => "OUT_OF_RANGE",
        tonic::Code::Unimplemented => "UNIMPLEMENTED",
        tonic::Code::Internal => "INTERNAL",
        tonic::Code::Unavailable => "UNAVAILABLE",
        tonic::Code::DataLoss => "DATA_LOSS",
        tonic::Code::Unauthenticated => "UNAUTHENTICATED",
    }
}

pub(crate) fn grpc_error_status(error: &(dyn std::error::Error + 'static)) -> &'static str {
    error
        .downcast_ref::<tonic::Status>()
        .map_or("UNKNOWN", |status| tonic_code_name(status.code()))
}

impl GrpcCallObservation {
    fn finish(self, status: &str) {
        let status = grpc_status_name(status);
        if status != "OK"
            && let Some(span) = &self.span
        {
            record_span_error(span, format!("gRPC status {status}"));
        }
        let elapsed = self.started_at.elapsed().as_secs_f64();
        if let Some(metrics) = &self.metrics {
            metrics.observe(status, elapsed);
        }
        if let Some(span) = &self.span {
            let _guard = span.enter();
            tracing::info!(
                rpc.system.name = "grpc",
                rpc.method = self.metrics.as_ref().map_or("<unmatched>", |metrics| metrics.method.as_str()),
                rpc.response.status_code = %status,
                duration_seconds = elapsed,
                "gRPC call completed"
            );
        }
    }
}

struct ObservedGrpcBody {
    inner: BoxBody,
    observation: Option<GrpcCallObservation>,
}

impl ObservedGrpcBody {
    fn finish(&mut self, status: &str) {
        if let Some(observation) = self.observation.take() {
            observation.finish(status);
        }
    }
}

impl Drop for ObservedGrpcBody {
    fn drop(&mut self) {
        self.finish("CANCELLED");
    }
}

impl HttpBody for ObservedGrpcBody {
    type Data = Bytes;
    type Error = Status;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        let span = self
            .observation
            .as_ref()
            .and_then(|observation| observation.span.clone());
        let _guard = span.as_ref().map(tracing::Span::enter);
        match Pin::new(&mut self.inner).poll_frame(context) {
            Poll::Ready(Some(Ok(frame))) => {
                if let Some(trailers) = frame.trailers_ref() {
                    let status = trailers
                        .get("grpc-status")
                        .and_then(|value| value.to_str().ok())
                        .unwrap_or("OK");
                    self.finish(status);
                }
                Poll::Ready(Some(Ok(frame)))
            }
            Poll::Ready(Some(Err(error))) => {
                self.finish(tonic_code_name(error.code()));
                Poll::Ready(Some(Err(error)))
            }
            Poll::Ready(None) => {
                self.finish("OK");
                Poll::Ready(None)
            }
            Poll::Pending => Poll::Pending,
        }
    }

    fn is_end_stream(&self) -> bool {
        self.inner.is_end_stream()
    }

    fn size_hint(&self) -> SizeHint {
        self.inner.size_hint()
    }
}

impl<S, B> Service<axum::http::Request<B>> for GrpcServerMetricsService<S>
where
    S: Service<axum::http::Request<B>, Response = axum::http::Response<BoxBody>> + Send + 'static,
    S::Future: Send + 'static,
    S::Error: Send + 'static,
    B: Send + 'static,
{
    type Response = axum::http::Response<BoxBody>;
    type Error = S::Error;
    type Future =
        Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send + 'static>>;

    fn poll_ready(&mut self, context: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(context)
    }

    fn call(&mut self, request: axum::http::Request<B>) -> Self::Future {
        let tracing_parent = MessageContext::tracing_parent_from_http_headers(request.headers());
        if !self.metrics.enabled && tracing_parent.is_none() {
            return Box::pin(self.inner.call(request));
        }
        let metrics = self.metrics.enabled.then(|| {
            self.metrics
                .method(request.uri().path().trim_start_matches('/'))
        });
        let started_at = Instant::now();
        let future = self.inner.call(request);
        let span = tracing_parent.map(|parent| {
            let span = tracing::info_span!(
                "grpc.server.call",
                rpc.system.name = "grpc",
                rpc.method = metrics
                    .as_ref()
                    .map_or("<unmatched>", |metrics| metrics.method.as_str()),
                error = tracing::field::Empty,
                otel.status_code = tracing::field::Empty,
                otel.status_message = tracing::field::Empty,
            );
            let _ = span.set_parent(parent);
            span
        });
        Box::pin(async move {
            let response = if let Some(span) = &span {
                future.instrument(span.clone()).await
            } else {
                future.await
            };
            let response = match response {
                Ok(response) => response,
                Err(error) => {
                    GrpcCallObservation {
                        metrics,
                        started_at,
                        span,
                    }
                    .finish("UNKNOWN");
                    return Err(error);
                }
            };
            let initial_status = response
                .headers()
                .get("grpc-status")
                .and_then(|value| value.to_str().ok())
                .map(grpc_status_name);
            let (parts, body) = response.into_parts();
            let mut observed = ObservedGrpcBody {
                inner: body,
                observation: Some(GrpcCallObservation {
                    metrics,
                    started_at,
                    span,
                }),
            };
            if let Some(status) = initial_status {
                observed.finish(status);
            }
            Ok(axum::http::Response::from_parts(
                parts,
                BoxBody::new(observed),
            ))
        })
    }
}
