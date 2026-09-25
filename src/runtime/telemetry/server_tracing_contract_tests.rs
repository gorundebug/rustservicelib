use std::{
    convert::Infallible,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

use axum::{
    Router,
    body::Body,
    http::{Method, Request, Response},
    middleware,
    routing::get,
};
use tower::{Layer as TowerLayer, ServiceExt, service_fn};
use tracing::{
    Event, Metadata, Subscriber,
    instrument::WithSubscriber,
    span::{Attributes, Id},
};
use tracing_subscriber::{Layer, layer::Context, prelude::*};

use super::{
    GrpcCallObservation, GrpcServerMetricsLayer, HttpRouteMetricSpec, HttpServerMetrics, Metrics,
    observe_http_server_request,
};

#[derive(Clone, Default)]
struct TransportRecording {
    spans: Arc<AtomicUsize>,
    events: Arc<AtomicUsize>,
    filter_spans: bool,
}

fn is_transport_span(metadata: &Metadata<'_>) -> bool {
    matches!(metadata.name(), "http.server.request" | "grpc.server.call")
}

impl<S: Subscriber> Layer<S> for TransportRecording {
    fn enabled(&self, metadata: &Metadata<'_>, _context: Context<'_, S>) -> bool {
        !self.filter_spans || !is_transport_span(metadata)
    }

    fn on_new_span(&self, attributes: &Attributes<'_>, _id: &Id, _context: Context<'_, S>) {
        if is_transport_span(attributes.metadata()) {
            self.spans.fetch_add(1, Ordering::Relaxed);
        }
    }

    fn on_event(&self, _event: &Event<'_>, _context: Context<'_, S>) {
        self.events.fetch_add(1, Ordering::Relaxed);
    }
}

#[tokio::test]
async fn http_metrics_and_business_handler_do_not_depend_on_tracing() {
    for metrics_enabled in [false, true] {
        for tracing_enabled in [false, true] {
            for sampled in [false, true] {
                for filtered in [false, true] {
                    let recording = TransportRecording {
                        filter_spans: filtered,
                        ..Default::default()
                    };
                    let subscriber = tracing_subscriber::registry().with(recording.clone());
                    async {
                        let metrics = if metrics_enabled {
                            Metrics::default()
                        } else {
                            Metrics::noop()
                        };
                        let state = HttpServerMetrics::new(
                            metrics,
                            "localhost".to_owned(),
                            8080,
                            vec![HttpRouteMetricSpec {
                                method: Method::GET,
                                route: "/booking".to_owned(),
                                statuses: vec![200, 500],
                            }],
                            tracing_enabled,
                        );
                        let route = state.route(&Method::GET, "/booking");
                        let calls = Arc::new(AtomicUsize::new(0));
                        let handler_calls = Arc::clone(&calls);
                        let router = Router::new()
                            .route(
                                "/booking",
                                get(move || {
                                    let calls = Arc::clone(&handler_calls);
                                    async move {
                                        calls.fetch_add(1, Ordering::Relaxed);
                                        "reserved"
                                    }
                                }),
                            )
                            .layer(middleware::from_fn_with_state(
                                state,
                                observe_http_server_request,
                            ));
                        let mut request = Request::builder().uri("/booking");
                        if sampled {
                            request = request.header("x-trace", "1");
                        }
                        let response = router
                            .oneshot(request.body(Body::empty()).unwrap())
                            .await
                            .unwrap();
                        assert_eq!(response.status(), 200);
                        assert_eq!(calls.load(Ordering::Relaxed), 1);
                        let count = route
                            .outcome(200)
                            .request_duration
                            .as_ref()
                            .map_or(0, |histogram| histogram.count());
                        assert_eq!(count, u64::from(metrics_enabled));
                    }
                    .with_subscriber(subscriber)
                    .await;
                    let active = tracing_enabled && sampled && !filtered;
                    assert_eq!(recording.spans.load(Ordering::Relaxed), usize::from(active));
                    assert_eq!(
                        recording.events.load(Ordering::Relaxed),
                        usize::from(active && metrics_enabled)
                    );
                }
            }
        }
    }
}

#[tokio::test]
async fn grpc_metrics_and_business_handler_do_not_depend_on_tracing() {
    for metrics_enabled in [false, true] {
        for tracing_enabled in [false, true] {
            for sampled in [false, true] {
                for filtered in [false, true] {
                    let recording = TransportRecording {
                        filter_spans: filtered,
                        ..Default::default()
                    };
                    let subscriber = tracing_subscriber::registry().with(recording.clone());
                    async {
                        let metrics = if metrics_enabled {
                            Metrics::default()
                        } else {
                            Metrics::noop()
                        };
                        let method = "inventory.Inventory/Reserve";
                        let layer = GrpcServerMetricsLayer::new(
                            metrics,
                            vec![method.to_owned()],
                            tracing_enabled,
                        );
                        let method_metrics = layer.metrics.method(method);
                        let calls = Arc::new(AtomicUsize::new(0));
                        let handler_calls = Arc::clone(&calls);
                        let service = layer.layer(service_fn(
                            move |_request: Request<tonic::body::BoxBody>| {
                                handler_calls.fetch_add(1, Ordering::Relaxed);
                                async {
                                    Ok::<_, Infallible>(
                                        Response::builder()
                                            .header("grpc-status", "0")
                                            .body(tonic::body::empty_body())
                                            .unwrap(),
                                    )
                                }
                            },
                        ));
                        let mut request = Request::builder().uri(format!("/{method}"));
                        if sampled {
                            request = request.header("x-trace", "1");
                        }
                        let response = service
                            .oneshot(request.body(tonic::body::empty_body()).unwrap())
                            .await
                            .unwrap();
                        assert_eq!(response.status(), 200);
                        assert_eq!(calls.load(Ordering::Relaxed), 1);
                        let count = method_metrics
                            .durations
                            .get("OK")
                            .and_then(Option::as_ref)
                            .map_or(0, |histogram| histogram.count());
                        assert_eq!(count, u64::from(metrics_enabled));
                    }
                    .with_subscriber(subscriber)
                    .await;
                    let active = tracing_enabled && sampled && !filtered;
                    assert_eq!(recording.spans.load(Ordering::Relaxed), usize::from(active));
                    assert_eq!(
                        recording.events.load(Ordering::Relaxed),
                        usize::from(active)
                    );
                }
            }
        }
    }
}

#[test]
fn grpc_completion_does_not_leak_events_into_an_unrelated_parent() {
    for enabled in [false, true] {
        let recording = TransportRecording::default();
        let subscriber = tracing_subscriber::registry().with(recording.clone());
        tracing::subscriber::with_default(subscriber, || {
            let parent = tracing::info_span!("unrelated.parent");
            let _entered = parent.enter();
            let span = if enabled {
                tracing::info_span!(
                    "grpc.server.call",
                    error = tracing::field::Empty,
                    otel.status_code = tracing::field::Empty,
                    otel.status_message = tracing::field::Empty
                )
            } else {
                tracing::Span::none()
            };
            GrpcCallObservation {
                metrics: None,
                started_at: Some(std::time::Instant::now()),
                span: Some(span),
            }
            .finish("13");
        });
        assert_eq!(
            recording.events.load(Ordering::Relaxed),
            usize::from(enabled)
        );
    }
}
