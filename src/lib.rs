// The public API deliberately carries the full stream, handler, payload, result,
// and error types so generated services retain compile-time type safety.
#![allow(
    clippy::type_complexity,
    reason = "strongly typed generated service APIs require these generic signatures"
)]

pub mod api;
pub mod datasink;
pub mod datasource;
pub mod operators;
pub mod runtime;
pub mod transformation;

pub use runtime::collector::{Collect, Collector};
pub use runtime::common::{Consumer, MessageContext, Payload, RuntimeStream};
pub use runtime::config::{
    CallSemantics, InputStreamConfig, JoinStreamConfig, LinkConfig, MultiJoinStreamConfig,
    StreamConfig,
};
pub use runtime::schedule::{
    ScheduleBackend, ScheduleEndpointFunction, ScheduleTrigger, normalize_temporal_priority,
};
pub use runtime::stream::Stream;
