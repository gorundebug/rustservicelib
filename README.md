# servicelib for Rust

Rust port of [gorundebug/servicelib](https://github.com/gorundebug/servicelib).
The Go implementation is the semantic reference. Rust-specific ownership and
async primitives may differ, but observable stream, cancellation, lifecycle,
and transport behavior must remain compatible.

The package structure intentionally follows the Go library:

- `api`
- `datasource`
- `datasink`
- `operators`
- `runtime`
- `transformation`

Messages keep `MessageContext` and `Payload<T>` separate. `Payload<T>` uses
`Arc<T>` so asynchronous calls and split branches do not borrow data past its
lifetime or force an eager deep copy.

## Telemetry

The default runtime exposes its Prometheus registry through the service
`metrics_handler`. For OTLP metrics, traces and structured logs, install the
shared OpenTelemetry providers and pass the three engine views to the runtime:

```rust
use servicelib::runtime::{
    config::CallSemantics,
    environment::RuntimeEnvironment,
    telemetry::opentelemetry::{Config, OpenTelemetry},
};

let telemetry = OpenTelemetry::install(Config::from_environment("orders"))?;
let environment = RuntimeEnvironment::with_telemetry(
    CallSemantics::FunctionCall,
    telemetry.clone(),
    telemetry.clone(),
    telemetry,
);
```

`ServiceApp` shuts engines down in Go-compatible order: metrics, tracing, then
logs. HTTP and gRPC propagate `x-stream-id`, `x-trace`, W3C `traceparent`,
`tracestate` and baggage. gRPC also propagates the remaining deadline; the
HTTP client applies it locally as its request timeout. Process-local context
such as priority is intentionally not serialized.

Tests can use `runtime::testmetrics`, `runtime::testlog` and
`runtime::testtracing`; they implement the same engine lifecycle contracts as
production backends.

Request tracing follows the Go contract: it is enabled only by a non-empty
`X-Trace`/`x-trace` marker or an already sampled W3C remote parent. Without
either signal ServiceLib creates no request, operator, pool, source, or sink
spans.

## Framework API generation

`servicelib/api/serviceapi.yaml` is the only canonical framework API schema.
In the development workspace, regenerate the committed strongly typed Rust API
from that schema with:

```sh
make api
```

`make api-check` verifies `src/api/serviceapi.rs` whenever the canonical Go
framework repository is available next to this checkout. A standalone checkout
contains the generated Rust API, but intentionally does not duplicate the
canonical YAML schema.

## Test in Docker

```sh
./scripts/test.sh
```
