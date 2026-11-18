# Rust NATS source observations

The existing official async-nats 0.52.0 async delivery stream now records
`broker_endpoint_source_observations_total` using fixed model labels
`protocol`, `connector`, `endpoint`, `observation`.

- Core NATS: `received` only.
- JetStream: `received` and `redelivered`. The latter increments once per received
  record only when official `Message::info().delivered > 1`. It does not add the
  cumulative delivery-attempt count. Absent/invalid metadata cannot manufacture
  a redelivery observation.

Recording happens in the SDK request path after its existing payload admission
limit and before metadata/tracing processing and `BeginRequest`. Thus a received
record is not a successfully processed request. Neither Done, ACK, cancellation,
reconnect nor a public raw native-handle operation synthesizes an observation.
No public interfaces/native wrappers, client dependency, threads or scheduling
bridges changed. Counters bind once per endpoint. Noop metrics bind nothing and
skip native metadata parsing; Core also skips JetStream metadata parsing.

## Focused acceptance

Evidence: `/private/tmp/sa-rust-nats-source-observations.log`.

The two unit tests use the production `PrometheusMetricsEngine` and its actual
renderer (the same renderer served by ServiceApp). They assert the exact family,
labels, complete sample count, native-attempt interpretation, and noop fast path.
This is production exporter serialization verification, not a separate HTTP
server acceptance test.

The two existing native endpoint tests additionally assert observations precede
BeginRequest, Core receives once without redelivery series, and JetStream receives
twice with one native redelivery. Done still leaves the message unacknowledged;
explicit raw native double ACK persists across consumer restart. Core stop remains
bounded while its handler holds the request. Test-owned subjects/stream names are
unique; the JetStream resource is deleted after the test. Shared broker unchanged.

Reproduce from the workspace root with the existing image/cache:

```sh
docker run --rm --network container:cppservicelib-nats-acceptance \
  -e NATS_URL=nats://127.0.0.1:4222 \
  -v "$PWD/rustservicelib:/workspace" \
  -v rustservicelib-cargo-registry:/usr/local/cargo/registry \
  -v rust-nats-target:/workspace/target -w /workspace \
  rustservicelib-toolchain:local sh -ec '
    cargo test --offline -j2 --lib datasource::nats::source_metrics &&
    cargo test --offline -j2 --test nats_native done_does_not_ack_and_native_ack_survives_restart -- --ignored --nocapture --test-threads=1 &&
    cargo test --offline -j2 --test nats_native core_inherits_metadata_and_stop_is_bounded -- --ignored --nocapture --test-threads=1'
```
