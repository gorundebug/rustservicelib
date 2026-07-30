use std::{collections::HashMap, sync::Mutex, time::Duration};

use opentelemetry::propagation::TextMapCompositePropagator;
use opentelemetry::{global, trace::TraceContextExt};
use opentelemetry_sdk::propagation::{BaggagePropagator, TraceContextPropagator};
use servicelib::MessageContext;

static PROPAGATOR_LOCK: Mutex<()> = Mutex::new(());

#[test]
fn message_context_propagates_stream_id_sampling_and_w3c_trace_context() {
    let _guard = PROPAGATOR_LOCK.lock().unwrap();
    global::set_text_map_propagator(TextMapCompositePropagator::new(vec![
        Box::new(TraceContextPropagator::new()),
        Box::new(BaggagePropagator::new()),
    ]));
    let context = MessageContext::new().with_metadata(HashMap::from([
        ("x-stream-id".to_owned(), "order-42".to_owned()),
        ("x-trace".to_owned(), "1".to_owned()),
        (
            "traceparent".to_owned(),
            "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01".to_owned(),
        ),
        ("baggage".to_owned(), "tenant=acme".to_owned()),
        ("authorization".to_owned(), "do-not-forward".to_owned()),
    ]));

    assert_eq!(context.stream_id(), Some("order-42"));
    assert!(context.sampling_enabled());
    assert_eq!(
        context
            .open_telemetry_context()
            .span()
            .span_context()
            .trace_id()
            .to_string(),
        "4bf92f3577b34da6a3ce929d0e0e4736"
    );

    let metadata = context.transport_metadata();
    assert_eq!(metadata["x-stream-id"], "order-42");
    assert_eq!(
        metadata["traceparent"],
        "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
    );
    assert_eq!(metadata["baggage"], "tenant=acme");
    assert!(!metadata.contains_key("authorization"));
}

#[test]
fn tracing_requires_an_explicit_marker_or_sampled_remote_parent() {
    let _guard = PROPAGATOR_LOCK.lock().unwrap();
    global::set_text_map_propagator(TraceContextPropagator::new());

    assert!(!MessageContext::new().sampling_enabled());
    assert!(
        !MessageContext::new()
            .with_metadata(HashMap::from([(
                "traceparent".to_owned(),
                "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-00".to_owned(),
            )]))
            .sampling_enabled()
    );
    assert!(
        MessageContext::new()
            .with_metadata(HashMap::from([(
                "traceparent".to_owned(),
                "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01".to_owned(),
            )]))
            .sampling_enabled()
    );
    assert!(
        MessageContext::new()
            .with_metadata(HashMap::from([(
                "x-trace".to_owned(),
                "requested".to_owned(),
            )]))
            .sampling_enabled()
    );
}

#[test]
fn tonic_round_trip_preserves_supported_context_and_deadline() {
    let _guard = PROPAGATOR_LOCK.lock().unwrap();
    global::set_text_map_propagator(TraceContextPropagator::new());
    let context = MessageContext::with_timeout(Duration::from_secs(5))
        .with_stream_id("stream-7")
        .enable_sampling();
    let mut request = tonic::Request::new(());
    context.apply_to_tonic_request(&mut request);

    let received = MessageContext::from_tonic_request(&request);
    assert_eq!(received.stream_id(), Some("stream-7"));
    assert!(received.sampling_enabled());
    assert!(
        received
            .remaining()
            .is_some_and(|ttl| ttl <= Duration::from_secs(5))
    );
}

#[test]
fn priority_is_local_context_state_and_is_not_serialized() {
    let context = MessageContext::new()
        .with_stream_id("stream-9")
        .with_priority(-50);
    assert_eq!(context.priority(), Some(-50));

    let received = MessageContext::new().with_metadata(context.transport_metadata());
    assert_eq!(received.stream_id(), Some("stream-9"));
    assert_eq!(received.priority(), None);
}

#[test]
fn assigning_stream_id_preserves_the_current_span_context() {
    let _guard = PROPAGATOR_LOCK.lock().unwrap();
    global::set_text_map_propagator(TraceContextPropagator::new());
    let remote_context = MessageContext::new().with_metadata(HashMap::from([(
        "traceparent".to_owned(),
        "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01".to_owned(),
    )]));
    let server_span_context = MessageContext::new().with_metadata(HashMap::from([(
        "traceparent".to_owned(),
        "00-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-bbbbbbbbbbbbbbbb-01".to_owned(),
    )]));

    let context = remote_context
        .with_open_telemetry_context(server_span_context.open_telemetry_context().clone())
        .with_stream_id("stream-10");

    assert_eq!(context.stream_id(), Some("stream-10"));
    assert_eq!(
        context
            .open_telemetry_context()
            .span()
            .span_context()
            .trace_id()
            .to_string(),
        "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
    );
}
