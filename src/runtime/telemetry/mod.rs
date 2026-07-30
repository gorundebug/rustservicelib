use std::{
    collections::BTreeMap,
    fmt::Display,
    future::Future,
    pin::Pin,
    task::{Context, Poll},
    time::Instant,
};

use axum::{
    body::Body,
    extract::{MatchedPath, Request, State},
    http::{HeaderMap, Response},
    middleware::Next,
};
use http_body::{Body as HttpBody, Frame, SizeHint};
use tonic::{Status, body::BoxBody, codegen::Bytes};
use tower::{Layer, Service};
use tracing::Instrument;
use tracing_opentelemetry::OpenTelemetrySpanExt;

use crate::runtime::{
    common::MessageContext,
    environment::metrics::{Labels, Metrics},
};

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
    metrics: Metrics,
    labels: Labels,
    started_at: Instant,
    request_body_size: usize,
}

impl HttpClientObservation {
    pub(crate) fn start(
        metrics: Metrics,
        method: &str,
        url: &str,
        request_body_size: usize,
    ) -> Self {
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
        Self {
            metrics,
            labels: [
                ("http_request_method".to_owned(), method.to_owned()),
                ("url_full".to_owned(), url.to_owned()),
                ("server_address".to_owned(), server_address),
                ("server_port".to_owned(), server_port),
                ("network_protocol_version".to_owned(), String::new()),
            ]
            .into_iter()
            .collect(),
            started_at: Instant::now(),
            request_body_size,
        }
    }

    pub(crate) fn finish(
        self,
        status: Option<u16>,
        response_body_size: Option<usize>,
        error: Option<&str>,
    ) {
        let mut labels = self.labels;
        labels.insert(
            "http_response_status_code".to_owned(),
            status.map_or_else(|| "500".to_owned(), |status| status.to_string()),
        );
        labels.insert(
            "error_type".to_owned(),
            error.unwrap_or_default().to_owned(),
        );
        let scope = self.metrics.scope("http_client", Labels::new());
        if let Ok(duration) = scope.histogram(
            "request_duration_seconds",
            "Duration of an HTTP client request in seconds",
            labels.clone(),
            Some(DURATION_BUCKETS_SECONDS.to_vec()),
        ) {
            duration.observe(self.started_at.elapsed().as_secs_f64());
        }
        if let Ok(size) = scope.histogram(
            "request_body_size_bytes",
            "Size of an HTTP client request body in bytes",
            labels.clone(),
            Some(BODY_SIZE_BUCKETS_BYTES.to_vec()),
        ) {
            size.observe(self.request_body_size as f64);
        }
        if let Some(response_body_size) = response_body_size
            && let Ok(size) = scope.histogram(
                "response_body_size_bytes",
                "Size of an HTTP client response body in bytes",
                labels,
                Some(BODY_SIZE_BUCKETS_BYTES.to_vec()),
            )
        {
            size.observe(response_body_size as f64);
        }
    }
}

pub(crate) struct GrpcClientObservation {
    metrics: Metrics,
    method: String,
    started_at: Instant,
}

impl GrpcClientObservation {
    pub(crate) fn start(metrics: Metrics, method: impl Into<String>) -> Self {
        Self {
            metrics,
            method: method.into(),
            started_at: Instant::now(),
        }
    }

    pub(crate) fn finish(self, status: &str) {
        let labels = [
            ("rpc_system_name".to_owned(), "grpc".to_owned()),
            ("rpc_method".to_owned(), self.method),
            ("rpc_response_status_code".to_owned(), status.to_owned()),
        ]
        .into_iter()
        .collect();
        if let Ok(duration) = self.metrics.scope("rpc_client", Labels::new()).histogram(
            "call_duration_seconds",
            "Duration of a gRPC client call in seconds",
            labels,
            Some(DURATION_BUCKETS_SECONDS.to_vec()),
        ) {
            duration.observe(self.started_at.elapsed().as_secs_f64());
        }
    }
}

#[derive(Clone)]
pub(crate) struct HttpServerMetrics {
    metrics: Metrics,
    host: String,
    port: u16,
}

impl HttpServerMetrics {
    pub(crate) fn new(metrics: Metrics, host: String, port: u16) -> Self {
        Self {
            metrics,
            host,
            port,
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

fn base_labels(state: &HttpServerMetrics, method: &str, route: &str) -> BTreeMap<String, String> {
    [
        ("http_request_method".to_owned(), method.to_owned()),
        ("http_route".to_owned(), route.to_owned()),
        ("url_scheme".to_owned(), "http".to_owned()),
        ("server_address".to_owned(), state.host.clone()),
        ("server_port".to_owned(), state.port.to_string()),
    ]
    .into_iter()
    .collect()
}

pub(crate) async fn observe_http_server_request(
    State(state): State<HttpServerMetrics>,
    request: Request,
    next: Next,
) -> Response<Body> {
    let method = request.method().as_str().to_owned();
    let route = request.extensions().get::<MatchedPath>().map_or_else(
        || request.uri().path().to_owned(),
        |path| path.as_str().to_owned(),
    );
    let request_body_size = content_length(request.headers());
    let incoming_context = MessageContext::new().with_metadata(
        request
            .headers()
            .iter()
            .filter_map(|(name, value)| {
                value
                    .to_str()
                    .ok()
                    .map(|value| (name.as_str().to_owned(), value.to_owned()))
            })
            .collect(),
    );
    let base_labels = base_labels(&state, &method, &route);
    let scope = state.metrics.scope("http_server", Labels::new());
    let active = scope
        .gauge(
            "active_requests",
            "Number of active HTTP server requests",
            base_labels.clone(),
        )
        .ok();
    if let Some(active) = &active {
        active.inc();
    }

    let started_at = Instant::now();
    let span = if incoming_context.sampling_enabled() {
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
        let _ = span.set_parent(incoming_context.open_telemetry_context().clone());
        span
    } else {
        tracing::Span::none()
    };
    let response = next.run(request).instrument(span.clone()).await;
    if let Some(active) = &active {
        active.dec();
    }

    let status = response.status();
    if status.is_server_error() {
        record_span_error(&span, format!("HTTP status {}", status.as_u16()));
    }
    let mut labels = base_labels;
    labels.insert(
        "http_response_status_code".to_owned(),
        status.as_u16().to_string(),
    );
    labels.insert(
        "error_type".to_owned(),
        if status.is_server_error() {
            "http_server_error".to_owned()
        } else {
            String::new()
        },
    );
    if let Ok(duration) = scope.histogram(
        "request_duration_seconds",
        "Duration of an HTTP server request in seconds",
        labels.clone(),
        Some(DURATION_BUCKETS_SECONDS.to_vec()),
    ) {
        duration.observe(started_at.elapsed().as_secs_f64());
    }
    if let Some(size) = request_body_size
        && let Ok(histogram) = scope.histogram(
            "request_body_size_bytes",
            "Size of an HTTP server request body in bytes",
            labels.clone(),
            Some(BODY_SIZE_BUCKETS_BYTES.to_vec()),
        )
    {
        histogram.observe(size);
    }
    if let Some(size) = content_length(response.headers())
        && let Ok(histogram) = scope.histogram(
            "response_body_size_bytes",
            "Size of an HTTP server response body in bytes",
            labels,
            Some(BODY_SIZE_BUCKETS_BYTES.to_vec()),
        )
    {
        histogram.observe(size);
    }
    tracing::info!(
        http.request.method = %method,
        http.route = %route,
        http.response.status_code = status.as_u16(),
        duration_seconds = started_at.elapsed().as_secs_f64(),
        "HTTP request completed"
    );
    response
}

#[derive(Clone)]
pub(crate) struct GrpcServerMetricsLayer {
    metrics: Metrics,
}

impl GrpcServerMetricsLayer {
    pub(crate) fn new(metrics: Metrics) -> Self {
        Self { metrics }
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
    metrics: Metrics,
}

struct GrpcCallObservation {
    metrics: Metrics,
    method: String,
    started_at: Instant,
    span: tracing::Span,
}

fn grpc_status_name(status: &str) -> String {
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
        other => other,
    }
    .to_owned()
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
    fn finish(self, status: impl Into<String>) {
        let status = grpc_status_name(&status.into());
        if status != "OK" {
            record_span_error(&self.span, format!("gRPC status {status}"));
        }
        let labels = [
            ("rpc_system_name".to_owned(), "grpc".to_owned()),
            ("rpc_method".to_owned(), self.method.clone()),
            ("rpc_response_status_code".to_owned(), status.clone()),
        ]
        .into_iter()
        .collect();
        if let Ok(duration) = self.metrics.scope("rpc_server", Labels::new()).histogram(
            "call_duration_seconds",
            "Duration of a gRPC server call in seconds",
            labels,
            Some(DURATION_BUCKETS_SECONDS.to_vec()),
        ) {
            duration.observe(self.started_at.elapsed().as_secs_f64());
        }
        let _guard = self.span.enter();
        tracing::info!(
            rpc.system.name = "grpc",
            rpc.method = %self.method,
            rpc.response.status_code = %status,
            duration_seconds = self.started_at.elapsed().as_secs_f64(),
            "gRPC call completed"
        );
    }
}

struct ObservedGrpcBody {
    inner: BoxBody,
    observation: Option<GrpcCallObservation>,
}

impl ObservedGrpcBody {
    fn finish(&mut self, status: impl Into<String>) {
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
            .map(|observation| observation.span.clone());
        let _guard = span.as_ref().map(tracing::Span::enter);
        match Pin::new(&mut self.inner).poll_frame(context) {
            Poll::Ready(Some(Ok(frame))) => {
                if let Some(trailers) = frame.trailers_ref() {
                    let status = trailers
                        .get("grpc-status")
                        .and_then(|value| value.to_str().ok())
                        .unwrap_or("OK")
                        .to_owned();
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
        let method = request.uri().path().trim_start_matches('/').to_owned();
        let incoming_context = MessageContext::new().with_metadata(
            request
                .headers()
                .iter()
                .filter_map(|(name, value)| {
                    value
                        .to_str()
                        .ok()
                        .map(|value| (name.as_str().to_owned(), value.to_owned()))
                })
                .collect(),
        );
        let started_at = Instant::now();
        let metrics = self.metrics.clone();
        let future = self.inner.call(request);
        let span = if incoming_context.sampling_enabled() {
            let span = tracing::info_span!(
                "grpc.server.call",
                rpc.system.name = "grpc",
                rpc.method = %method,
                error = tracing::field::Empty,
                otel.status_code = tracing::field::Empty,
                otel.status_message = tracing::field::Empty,
            );
            let _ = span.set_parent(incoming_context.open_telemetry_context().clone());
            span
        } else {
            tracing::Span::none()
        };
        Box::pin(async move {
            let response = match future.instrument(span.clone()).await {
                Ok(response) => response,
                Err(error) => {
                    GrpcCallObservation {
                        metrics,
                        method,
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
                .map(ToOwned::to_owned);
            let (parts, body) = response.into_parts();
            let mut observed = ObservedGrpcBody {
                inner: body,
                observation: Some(GrpcCallObservation {
                    metrics,
                    method,
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
