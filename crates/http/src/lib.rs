//! The HTTP and log plumbing the addons answer with, written once.
//!
//! Reel, Subtitles and Remux each carried a copy of the response builders, the conditional-GET rules, the CORS
//! stamping, the request-id filter, the log-line builder, the metrics bearer check and the rate-limited logger, and
//! the copies had begun to differ in ways nobody chose. Everything here is a rule an ops script, a CDN or a browser
//! reads the same way from every service: an ETag string, a header name, a log line's shape, what a log line may
//! never contain.
//!
//! One crate rather than an `http` and a `log` pair: the log half is a few hundred lines of std, every consumer
//! needs both, and the split would have bought a dependency edge between the two and nothing else.

mod auth;
mod conditional;
mod response;
mod text;

pub mod local;
pub mod log;
pub mod prom;

pub use auth::{bearer_matches, metrics_authorized};
pub use conditional::{apply_conditional, etag_of, if_none_match_matches, if_range_holds, stable_hash};
pub use local::Local;
pub use response::{
    add_timing, cors_stamp, degraded, error, full, html, json, not_found, preflight, timed, timing,
    with_total_timing, Body, StaticBody, SERVER_TIMING, X_DEN_DEGRADED, X_DEN_SERVICE_READ,
};
pub use text::{percent_decode, percent_decode_path};
