//! Port of `internal/scout/loglimit.go`. Work package 6.
//!
//! The log records changes of state, not events. A request-path line fires on every request for as long as its
//! cause lasts, so each such line goes through `log_limited`, and a condition writes at most one line a minute
//! however much traffic it sees.

use std::collections::BTreeMap;
use std::sync::Mutex;
use std::time::{Duration, Instant};

/// Go `logInterval`: a condition writes at most one line per this interval.
pub const LOG_INTERVAL: Duration = Duration::from_secs(60);

/// Go `limitedEntry`: when a condition last wrote, and how many lines it has held back since.
#[derive(Debug, Clone, Copy)]
pub(crate) struct LimitedEntry {
    pub(crate) at: Instant,
    pub(crate) suppressed: i64,
}

/// Go `limitedLog`: the last write of every condition. Tests rewind `at` to let a condition speak again.
pub(crate) static LIMITED_LOG: Mutex<BTreeMap<String, LimitedEntry>> = Mutex::new(BTreeMap::new());

/// Go `logLimited`: writes `message` unless the same condition already wrote one inside `LOG_INTERVAL`. The next
/// line of that condition to get through says how many were held back in between. `key` names the condition and
/// comes from a fixed vocabulary, never a hash or a title, or the map grows with traffic.
pub fn log_limited(key: &str, message: &str) {
    todo!("loglimit.go logLimited")
}
