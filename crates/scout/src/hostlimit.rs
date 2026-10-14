//! Port of `internal/scout/hostlimit.go`. Work package 6.
//!
//! Outbound pacing, per upstream host: a token bucket per host. A burst passes straight through; past it,
//! requests queue at `every` rather than being shed. A caller whose context ends (here, a dropped future)
//! gives its token back.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

/// Go `hostLimiter`.
pub struct HostLimiter {
    /// Go `buckets`: one bucket per host, each behind its own lock.
    pub(crate) buckets: Mutex<HashMap<String, Arc<Mutex<Bucket>>>>,
    pub(crate) every: Duration,
    pub(crate) burst: i64,
    /// Go `maxKeys`: how many hosts the limiter remembers; 0 means unbounded.
    pub(crate) max_keys: usize,
}

/// Go `bucket`: a token count, refilled by elapsed time.
pub struct Bucket {
    pub(crate) tokens: f64,
    pub(crate) last: Instant,
}

/// Go `newHostLimiter`: a non-positive `every` becomes one second and a `burst` below 1 becomes 1.
pub fn new_host_limiter(every: Duration, burst: i64) -> HostLimiter {
    todo!("hostlimit.go newHostLimiter")
}

/// Go `newBoundedHostLimiter`.
pub fn new_bounded_host_limiter(every: Duration, burst: i64, max_keys: usize) -> HostLimiter {
    todo!("hostlimit.go newBoundedHostLimiter")
}

/// Go `(*hostLimiter).wait`: blocks until `host` may be asked again. A `None` limiter or an empty host is a
/// no-op, as a nil receiver is in Go. A dropped future is the caller's deadline, and it refunds the token.
pub async fn wait(limiter: Option<&HostLimiter>, host: &str) {
    todo!("hostlimit.go hostLimiter.wait")
}

impl Bucket {
    /// Go `take`: refills by elapsed time, spends a token, and reports how long the caller must wait for it.
    /// Debt is not floored.
    pub(crate) fn take(&mut self, every: Duration, burst: i64) -> Duration {
        todo!("hostlimit.go bucket.take")
    }

    /// Go `refund`: returns a spent token, capped at the burst.
    pub(crate) fn refund(&mut self, burst: i64) {
        todo!("hostlimit.go bucket.refund")
    }
}
