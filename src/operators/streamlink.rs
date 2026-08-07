//! Shared stream-link behavior is provided by [`crate::runtime::stream::Stream`].
//!
//! Go needs an embedded private `streamLink` forwarding object. Rust uses
//! composition and explicit `stream()` accessors, so introducing another
//! public entity here would duplicate the Go implementation detail rather
//! than its architecture.
