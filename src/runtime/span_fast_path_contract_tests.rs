use opentelemetry::trace::{
    SpanContext, SpanId, TraceContextExt, TraceFlags, TraceId, TraceState, TracerProvider,
};
use std::{
    fmt,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};
use tracing::{Event, Subscriber};
use tracing_opentelemetry::OpenTelemetrySpanExt;
use tracing_subscriber::{Layer, layer::Context, prelude::*};

fn remote_parent() -> opentelemetry::Context {
    opentelemetry::Context::new().with_remote_span_context(SpanContext::new(
        TraceId::from(42_u128),
        SpanId::from(7_u64),
        TraceFlags::SAMPLED,
        true,
        TraceState::default(),
    ))
}

#[test]
fn disabled_span_preserves_the_existing_propagation_context_even_when_sampled() {
    for sampled in [false, true] {
        let parent = remote_parent();
        let message = super::MessageContext::new().with_open_telemetry_context(parent.clone());
        let message = if sampled {
            message.enable_sampling()
        } else {
            message
        };
        let result = message.with_span_context(&tracing::Span::none());
        assert_eq!(
            result.open_telemetry_context().span().span_context(),
            parent.span().span_context()
        );
    }
}

#[test]
fn enabled_span_replaces_the_parent_only_for_a_sampled_message() {
    let provider = opentelemetry_sdk::trace::SdkTracerProvider::builder().build();
    let subscriber = tracing_subscriber::registry()
        .with(tracing_opentelemetry::layer().with_tracer(provider.tracer("context-test")));
    tracing::subscriber::with_default(subscriber, || {
        let span = tracing::info_span!("context-test");
        let active = span.context();
        assert!(active.span().span_context().is_valid());
        for sampled in [false, true] {
            let parent = remote_parent();
            let message = super::MessageContext::new().with_open_telemetry_context(parent.clone());
            let message = if sampled {
                message.enable_sampling()
            } else {
                message
            };
            let result = message.with_span_context(&span);
            let expected = if sampled { &active } else { &parent };
            assert_eq!(
                result.open_telemetry_context().span().span_context(),
                expected.span().span_context()
            );
        }
    });
}

#[test]
fn filtered_operator_and_grpc_spans_preserve_the_propagation_context() {
    use super::RuntimeStream;
    use crate::runtime::{
        config::CallSemantics, environment::RuntimeEnvironment, testlog::TestLog,
        testmetrics::TestMetrics, testtracing::TestTracing,
    };

    struct GroupedStream(RuntimeEnvironment);
    impl RuntimeStream for GroupedStream {
        fn id(&self) -> i32 {
            1
        }
        fn name(&self) -> String {
            "Reserve".to_owned()
        }
        fn environment(&self) -> &RuntimeEnvironment {
            &self.0
        }
        fn tracing_labels(&self) -> (&str, &str, &str) {
            ("Reserve", "booking", "Inventory")
        }
    }
    let stream = GroupedStream(RuntimeEnvironment::with_telemetry(
        CallSemantics::FunctionCall,
        Arc::new(TestMetrics::new()),
        Arc::new(TestTracing::default()),
        Arc::new(TestLog::default()),
    ));
    assert!(stream.environment().tracing_enabled());
    tracing::subscriber::with_default(tracing::subscriber::NoSubscriber::default(), || {
        let parent = remote_parent();
        let context = super::MessageContext::new()
            .enable_sampling()
            .with_open_telemetry_context(parent.clone());
        let (operator_context, span) = stream.start_span(context.clone(), "stream.map");
        assert!(span.is_none());
        assert_eq!(
            operator_context
                .open_telemetry_context()
                .span()
                .span_context(),
            parent.span().span_context()
        );
        let (grpc_context, span) =
            crate::datasink::grpc::start_output_span(context, &stream, "Reserve");
        assert!(span.is_disabled());
        assert_eq!(
            grpc_context.open_telemetry_context().span().span_context(),
            parent.span().span_context()
        );
    });
}

#[test]
fn noop_tracing_environment_skips_sampled_operator_spans() {
    use super::RuntimeStream;
    use crate::runtime::{config::CallSemantics, environment::RuntimeEnvironment};

    struct Stream(RuntimeEnvironment);
    impl RuntimeStream for Stream {
        fn id(&self) -> i32 {
            1
        }
        fn name(&self) -> String {
            "Reserve".to_owned()
        }
        fn environment(&self) -> &RuntimeEnvironment {
            &self.0
        }
        fn tracing_labels(&self) -> (&str, &str, &str) {
            ("Reserve", "booking", "Inventory")
        }
    }

    let stream = Stream(RuntimeEnvironment::new(CallSemantics::FunctionCall).without_tracing());
    assert!(!stream.environment().tracing_enabled());
    let context = super::MessageContext::new().enable_sampling();
    let (_, span) = stream.start_span(context, "stream.map");
    assert!(span.is_none());
}

#[test]
fn transport_parent_cloning_is_inside_the_span_guard() {
    for source in [
        include_str!("../datasource/http/axum.rs"),
        include_str!("../datasource/localsource/custom.rs"),
        include_str!("../datasource/kafka/rdkafka.rs"),
        include_str!("../datasource/grpc/mod.rs"),
        include_str!("../datasink/http/reqwest.rs"),
        include_str!("../datasink/localsink/custom.rs"),
        include_str!("../datasink/kafka/rdkafka.rs"),
    ] {
        let compact: String = source.chars().filter(|ch| !ch.is_whitespace()).collect();
        assert!(compact.contains(concat!(
            "if!span.is_disabled(){",
            "let_=span.set_parent(context.open_telemetry_context().clone());"
        )));
    }
}

struct CountEvents(Arc<AtomicUsize>);

impl<S: Subscriber> Layer<S> for CountEvents {
    fn on_event(&self, _event: &Event<'_>, _context: Context<'_, S>) {
        self.0.fetch_add(1, Ordering::Relaxed);
    }
}

struct CountFormatting<'a>(&'a AtomicUsize);

impl fmt::Display for CountFormatting<'_> {
    fn fmt(&self, output: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fetch_add(1, Ordering::Relaxed);
        output.write_str("business failure")
    }
}

#[test]
fn trace_scopes_and_errors_are_lazy_and_do_not_leak_to_parent() {
    for enabled in [false, true] {
        let events = Arc::new(AtomicUsize::new(0));
        let subscriber = tracing_subscriber::registry().with(CountEvents(events.clone()));
        let closure_evaluations = AtomicUsize::new(0);
        let error_evaluations = AtomicUsize::new(0);
        let formatting = AtomicUsize::new(0);
        tracing::subscriber::with_default(subscriber, || {
            let parent = tracing::info_span!("unrelated.parent");
            let _entered = parent.enter();
            let span = if enabled {
                tracing::info_span!(
                    "transport",
                    error = tracing::field::Empty,
                    otel.status_code = tracing::field::Empty,
                    otel.status_message = tracing::field::Empty
                )
            } else {
                tracing::Span::none()
            };
            super::event_if_enabled!(&span, {
                closure_evaluations.fetch_add(1, Ordering::Relaxed);
                || tracing::event!(name: "request.complete", tracing::Level::INFO, {})
            });
            crate::runtime::telemetry::record_error_if_present!(Some(&span), {
                error_evaluations.fetch_add(1, Ordering::Relaxed);
                CountFormatting(&formatting)
            });
        });
        let expected = usize::from(enabled);
        assert_eq!(closure_evaluations.load(Ordering::Relaxed), expected);
        assert_eq!(error_evaluations.load(Ordering::Relaxed), expected);
        assert_eq!(formatting.load(Ordering::Relaxed), expected);
        assert_eq!(events.load(Ordering::Relaxed), expected);
    }
}

#[test]
fn business_callback_runs_once_and_returns_its_value_with_or_without_tracing() {
    let subscriber = tracing_subscriber::registry();
    tracing::subscriber::with_default(subscriber, || {
        for enabled in [false, true] {
            let calls = AtomicUsize::new(0);
            let span = if enabled {
                tracing::info_span!("business")
            } else {
                tracing::Span::none()
            };
            let result = super::scope_if_present!(Some(&span), || {
                calls.fetch_add(1, Ordering::Relaxed);
                42
            });
            assert_eq!(result, 42);
            assert_eq!(calls.load(Ordering::Relaxed), 1);
        }
    });
}

#[test]
fn transport_scopes_and_errors_use_lazy_callsite_guards() {
    for (name, source) in [
        ("HTTP source", include_str!("../datasource/http/axum.rs")),
        ("HTTP sink", include_str!("../datasink/http/reqwest.rs")),
        (
            "Custom source",
            include_str!("../datasource/localsource/custom.rs"),
        ),
        (
            "Custom sink",
            include_str!("../datasink/localsink/custom.rs"),
        ),
        (
            "Kafka source",
            include_str!("../datasource/kafka/rdkafka.rs"),
        ),
        ("Kafka sink", include_str!("../datasink/kafka/rdkafka.rs")),
        ("gRPC source", include_str!("../datasource/grpc/mod.rs")),
        ("gRPC result", include_str!("../datasink/grpc/mod.rs")),
        (
            "gRPC unary",
            include_str!("../datasink/grpc/nostreaming.rs"),
        ),
        (
            "gRPC server streaming",
            include_str!("../datasink/grpc/serverstreaming.rs"),
        ),
        (
            "gRPC client streaming",
            include_str!("../datasink/grpc/clientstreaming.rs"),
        ),
        (
            "gRPC bidi",
            include_str!("../datasink/grpc/bidistreaming.rs"),
        ),
    ] {
        let compact: String = source.chars().filter(|ch| !ch.is_whitespace()).collect();
        assert!(!compact.contains(".in_scope("), "unguarded scope in {name}");
        assert!(
            !compact.contains("telemetry::record_span_error("),
            "unguarded error in {name}"
        );
    }
}
