//! Port of `internal/scout/upstreampause.go`.
//!
//! An upstream that says to stop is left alone by every request, not only the one it answered.
//!
//! Nothing here remembered a 429. An indexer or a debrid that throttled one request was asked again by the next
//! — a season's worth of episode lists, every poster's availability check, every probe poll — and each one
//! counted against the limit it had just said was spent. Retry-After was never read anywhere.
//!
//! So the transport under every outbound client keeps the refusal: a 429, or a 503 that names a Retry-After,
//! pauses that upstream for as long as it asked (else 30s, doubling per refusal in a row up to 10 minutes). While
//! the pause runs, a request to it is answered here with a 429 of our own carrying the time left, so no caller
//! needs to know about the pause to honour it: each already treats a 429 as "not now". An answer ends the pause.
//!
//! Keyed by host and the credential the request carries, so one throttled debrid account pauses that account and
//! not every other one on the same API, and an indexer — which limits by address — pauses for everyone.
//!
//! The pause table, its policy and the header parsing are `den-upstream`'s, shared with Reel, Subtitles and Remux.
//! What is here is the transport seam: Go's `http.RoundTripper` is the `scrape::Doer` seam, and `PausingDoer`
//! wraps any `Doer`.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use den_upstream::{Pauses, Policy, Throttle};
use sha2::{Digest, Sha256};

use crate::loglimit::log_limited;
use crate::scrape::{DoError, Doer};

pub use den_upstream::{retry_after, MAX_RETRY_AFTER};

/// Go `refusalPause`: the first pause when the upstream names none; doubles per refusal in a row.
pub const REFUSAL_PAUSE: Duration = Duration::from_secs(30);
/// Go `maxRefusalPause`.
pub const MAX_REFUSAL_PAUSE: Duration = Duration::from_secs(10 * 60);
/// Go `maxPausedUpstreams`: how many paused upstreams are remembered. Hosts of household sources come from configs
/// anyone can build.
pub const MAX_PAUSED_UPSTREAMS: usize = 4096;

/// Scout's window: 30 s doubling to ten minutes, with up to a quarter again on top.
const POLICY: Policy = Policy {
    base: REFUSAL_PAUSE,
    cap: MAX_REFUSAL_PAUSE,
    jitter_permille: 250,
    max_keys: MAX_PAUSED_UPSTREAMS,
};

/// Go `pausingTransport`, over any `Doer`. `now` is the clock; a test swaps it for one it can move.
pub struct PausingDoer<D: Doer> {
    next: D,
    paused: Pauses<String>,
    pub(crate) now: Box<dyn Fn() -> SystemTime + Send + Sync>,
}

/// Go `PauseOnRefusal`: wraps `next` so upstreams that refuse are left alone for as long as they ask.
pub fn pause_on_refusal<D: Doer>(next: D) -> PausingDoer<D> {
    PausingDoer { next, paused: Pauses::new(POLICY), now: Box::new(SystemTime::now) }
}

impl<D: Doer> PausingDoer<D> {
    /// The pause table's clock: time since the epoch on `now`.
    fn clock(&self) -> Duration {
        (self.now)().duration_since(UNIX_EPOCH).unwrap_or(Duration::ZERO)
    }

    /// Go `left`: how much pause is left on `key`, or zero.
    pub(crate) fn left(&self, key: &str) -> Duration {
        self.paused.remaining(&key.to_owned(), self.clock()).unwrap_or(Duration::ZERO)
    }

    /// Go `refused` starts or extends the pause and says how long it is.
    #[cfg(test)]
    pub(crate) fn refused(&self, key: &str, asked: Duration, named: bool) -> Duration {
        self.paused.refuse(&key.to_owned(), named.then_some(asked), self.clock()).wait
    }
}

#[async_trait]
impl<D: Doer> Doer for PausingDoer<D> {
    async fn do_request(&self, req: reqwest::Request) -> Result<reqwest::Response, DoError> {
        let key = pause_key(&req);
        let left = self.left(&key);
        if !left.is_zero() {
            return Ok(paused_response(left));
        }
        let hostname = req.url().host_str().unwrap_or_default().to_owned();
        let observed = self.paused.begin(&key);
        let resp = self.next.do_request(req).await?;
        let status = resp.status();
        let now = (self.now)();
        if let Some(throttle) = den_upstream::throttle(status, resp.headers(), now) {
            let stated = match throttle {
                Throttle::Stated(wait) => Some(wait),
                Throttle::Unstated => None,
            };
            let pause = self.paused.refuse(&key, stated, self.clock()).wait;
            log_limited(
                "upstream-paused",
                &format!(
                    "{hostname} answered {} — leaving it alone for {}",
                    status.as_u16(),
                    go_round_secs(pause)
                ),
            );
        } else if status.as_u16() < 400 {
            // An answer that also says the allowance is spent is waited out from here, rather than found out by a 429.
            if let Some(wait) = den_upstream::exhausted_for(resp.headers(), now) {
                self.paused.hold(&key, wait, self.clock());
                log_limited(
                    "upstream-exhausted",
                    &format!(
                        "{hostname} says its allowance is spent — leaving it alone for {}",
                        go_round_secs(wait)
                    ),
                );
            } else {
                self.paused.answered(&key, observed);
            }
        }
        Ok(resp)
    }
}

/// Go `pauseKey` is the upstream a request goes to: its host, and a hash of the credential it carries, if any.
fn pause_key(req: &reqwest::Request) -> String {
    // Go keys by `url.Host`, which carries the port when the URL names one. DIVERGENCE kept: Go's is the host exactly
    // as written, and a `reqwest::Request` only holds the WHATWG-normalised URL (host lower-cased, a default port
    // such as `:443` dropped), so two spellings of one host share a pause here where Go gives them two. The text the
    // caller wrote is gone before it reaches this seam; the only effect is that the pause is the more cautious one.
    let url = req.url();
    let host = match (url.host_str(), url.port()) {
        (Some(h), Some(p)) => format!("{h}:{p}"),
        (Some(h), None) => h.to_owned(),
        (None, _) => String::new(),
    };
    let mut credential =
        req.headers().get("authorization").and_then(|v| v.to_str().ok()).unwrap_or_default().to_owned();
    if credential.is_empty() {
        for name in ["apikey", "api_key", "token", "key"] {
            if let Some((_, v)) = url.query_pairs().find(|(k, _)| k == name) {
                if !v.is_empty() {
                    credential = v.into_owned();
                    break;
                }
            }
        }
    }
    if credential.is_empty() {
        return host;
    }
    let sum: String = Sha256::digest(credential.as_bytes())[..8].iter().map(|b| format!("{b:02x}")).collect();
    format!("{host}|{sum}")
}

/// Go `pausedResponse` is the 429 a paused upstream is answered with, carrying the time left.
fn paused_response(left: Duration) -> reqwest::Response {
    let secs = left.as_nanos().div_ceil(1_000_000_000);
    let resp = http::Response::builder()
        .status(429)
        .header("Retry-After", secs.to_string())
        .header("X-Scout-Paused", "1")
        .body(String::new())
        .unwrap_or_default();
    reqwest::Response::from(resp)
}

/// Go's `Duration.Round(time.Second).String()`, for the log line: `30s`, `2m0s`, `1h0m0s`.
fn go_round_secs(d: Duration) -> String {
    let total = d.as_secs() + u64::from(d.subsec_nanos() >= 500_000_000);
    let (h, m, s) = (total / 3600, total / 60 % 60, total % 60);
    if h > 0 {
        format!("{h}h{m}m{s}s")
    } else if m > 0 {
        format!("{m}m{s}s")
    } else {
        format!("{s}s")
    }
}
