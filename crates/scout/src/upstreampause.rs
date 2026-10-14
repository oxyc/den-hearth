//! Port of `internal/scout/upstreampause.go`. Work package 6.
//!
//! An upstream that says to stop is left alone by every request, not only the one it answered. The wrapper keeps
//! a 429 (or a 503 that names a Retry-After) and pauses that upstream for as long as it asked. While the pause
//! runs, a request to it is answered locally with a 429 carrying the time left. Keyed by host and the credential
//! the request carries, so one throttled account pauses that account and not every other one on the same API.
//!
//! Go's `http.RoundTripper` is the `scrape::Doer` seam here; `PausingDoer` wraps any `Doer`.

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{Duration, SystemTime};

use async_trait::async_trait;
use reqwest::header::HeaderMap;

use crate::scrape::{DoError, Doer};

/// Go `refusalPause`: the first pause when the upstream names none; doubles per refusal in a row.
pub const REFUSAL_PAUSE: Duration = Duration::from_secs(30);
/// Go `maxRefusalPause`.
pub const MAX_REFUSAL_PAUSE: Duration = Duration::from_secs(10 * 60);
/// Go `maxRetryAfter`: a Retry-After past this is read as this.
pub const MAX_RETRY_AFTER: Duration = Duration::from_secs(60 * 60);
/// Go `maxPausedUpstreams`: how many paused upstreams are remembered.
pub const MAX_PAUSED_UPSTREAMS: usize = 4096;

/// Go `upstreamPause`.
#[derive(Debug, Clone, Copy)]
pub struct UpstreamPause {
    pub until: SystemTime,
    pub refusals: i64,
}

/// Go `pausingTransport`, over any `Doer`. `now` is the clock; a test swaps it for one it can move.
pub struct PausingDoer<D: Doer> {
    next: D,
    paused: Mutex<HashMap<String, UpstreamPause>>,
    pub(crate) now: Box<dyn Fn() -> SystemTime + Send + Sync>,
}

/// Go `PauseOnRefusal`: wraps `next` so upstreams that refuse are left alone for as long as they ask.
pub fn pause_on_refusal<D: Doer>(next: D) -> PausingDoer<D> {
    todo!("upstreampause.go PauseOnRefusal")
}

impl<D: Doer> PausingDoer<D> {
    /// Go `left`: how much pause is left on `key`, or zero.
    pub(crate) fn left(&self, key: &str) -> Duration {
        todo!("upstreampause.go pausingTransport.left")
    }

    /// Go `refused`: starts or extends the pause on `key` and says how long it is.
    pub(crate) fn refused(&self, key: &str, asked: Duration, named: bool) -> Duration {
        todo!("upstreampause.go pausingTransport.refused")
    }
}

#[async_trait]
impl<D: Doer> Doer for PausingDoer<D> {
    async fn do_request(&self, req: reqwest::Request) -> Result<reqwest::Response, DoError> {
        todo!("upstreampause.go pausingTransport.RoundTrip")
    }
}

/// Go `retryAfter`: a Retry-After of seconds or an HTTP date, capped at `MAX_RETRY_AFTER`. `None` when none was
/// sent that reads.
pub fn retry_after(h: &HeaderMap, now: SystemTime) -> Option<Duration> {
    todo!("upstreampause.go retryAfter")
}

/// Go `exhaustedFor`: how long until an upstream's allowance comes back, when its answer says none is left. The
/// bool says whether it does.
pub fn exhausted_for(h: &HeaderMap, now: SystemTime) -> (Duration, bool) {
    todo!("upstreampause.go exhaustedFor")
}
