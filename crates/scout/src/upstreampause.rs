//! Port of `internal/scout/upstreampause.go`. Work package 6.
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
//! Go's `http.RoundTripper` is the `scrape::Doer` seam here; `PausingDoer` wraps any `Doer`.

use std::collections::HashMap;
use std::sync::{Mutex, MutexGuard, PoisonError};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use reqwest::header::HeaderMap;
use sha2::{Digest, Sha256};

use crate::loglimit::log_limited;
use crate::scrape::{DoError, Doer};

/// Go `refusalPause`: the first pause when the upstream names none; doubles per refusal in a row.
pub const REFUSAL_PAUSE: Duration = Duration::from_secs(30);
/// Go `maxRefusalPause`.
pub const MAX_REFUSAL_PAUSE: Duration = Duration::from_secs(10 * 60);
/// Go `maxRetryAfter`: a Retry-After past this is read as this. Debrid limits are hourly, and an absurd value from
/// a broken proxy must not close an upstream for days.
pub const MAX_RETRY_AFTER: Duration = Duration::from_secs(60 * 60);
/// Go `maxPausedUpstreams`: how many paused upstreams are remembered. Hosts of household sources come from configs
/// anyone can build.
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
    PausingDoer { next, paused: Mutex::new(HashMap::new()), now: Box::new(SystemTime::now) }
}

impl<D: Doer> PausingDoer<D> {
    /// A poisoned lock still holds consistent state: every edit under it finishes before anything can panic.
    fn lock(&self) -> MutexGuard<'_, HashMap<String, UpstreamPause>> {
        self.paused.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Go `left`: how much pause is left on `key`, or zero.
    pub(crate) fn left(&self, key: &str) -> Duration {
        let paused = self.lock();
        match paused.get(key) {
            None => Duration::ZERO,
            Some(p) => p.until.duration_since((self.now)()).unwrap_or(Duration::ZERO),
        }
    }

    /// Go `refused` starts or extends the pause and says how long it is.
    pub(crate) fn refused(&self, key: &str, asked: Duration, named: bool) -> Duration {
        let mut paused = self.lock();
        let now = (self.now)();
        if !paused.contains_key(key) {
            if paused.len() >= MAX_PAUSED_UPSTREAMS {
                prune(&mut paused, now);
            }
            paused.insert(key.to_owned(), UpstreamPause { until: UNIX_EPOCH, refusals: 0 });
        }
        let Some(p) = paused.get_mut(key) else { return asked };
        p.refusals += 1;
        let mut pause = asked;
        if !named {
            pause = (REFUSAL_PAUSE * (1u32 << (p.refusals - 1).min(10))).min(MAX_REFUSAL_PAUSE);
            let jitter = fastrand::u64(0..=(pause / 4).as_nanos() as u64);
            pause += Duration::from_nanos(jitter);
        }
        if let Some(until) = now.checked_add(pause) {
            if until > p.until {
                p.until = until;
            }
        }
        pause
    }

    /// Go `answered`: an answer ends the pause.
    fn answered(&self, key: &str) {
        self.lock().remove(key);
    }
}

/// Go `pruneLocked` drops pauses that have ended, and if none have, starts over: forgetting a pause costs one more
/// refused request, while an unbounded map costs the process.
fn prune(paused: &mut HashMap<String, UpstreamPause>, now: SystemTime) {
    paused.retain(|_, p| p.until > now);
    if paused.len() >= MAX_PAUSED_UPSTREAMS {
        paused.clear();
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
        let resp = self.next.do_request(req).await?;
        let status = resp.status().as_u16();
        let (asked, named) = match retry_after(resp.headers(), (self.now)()) {
            Some(d) => (d, true),
            None => (Duration::ZERO, false),
        };
        if status == 429 || (status == 503 && named) {
            let pause = self.refused(&key, asked, named);
            log_limited(
                "upstream-paused",
                &format!("{hostname} answered {status} — leaving it alone for {}", go_round_secs(pause)),
            );
        } else if status < 400 {
            // An answer that also says the allowance is spent is waited out from here, rather than found out by a 429.
            let (wait, spent) = exhausted_for(resp.headers(), (self.now)());
            if spent {
                let pause = self.refused(&key, wait, true);
                log_limited(
                    "upstream-exhausted",
                    &format!("{hostname} says its allowance is spent — leaving it alone for {}", go_round_secs(pause)),
                );
            } else {
                self.answered(&key);
            }
        }
        Ok(resp)
    }
}

/// Go `strconv.ParseInt(strings.TrimSpace(v), 10, 64)` accepted when non-negative.
fn number(v: &str) -> Option<i64> {
    v.trim().parse::<i64>().ok().filter(|n| *n >= 0)
}

/// Go `exhaustedFor` reports how long until an upstream's allowance comes back, when its answer says none is left: the
/// IETF draft `RateLimit: "policy";r=0;t=30`, its older `RateLimit-Remaining` / `RateLimit-Reset`, or the common
/// `X-RateLimit-Remaining` / `X-RateLimit-Reset`. A reset over a billion is a Unix time, not seconds to wait.
pub fn exhausted_for(h: &HeaderMap, now: SystemTime) -> (Duration, bool) {
    let get = |name: &str| h.get(name).and_then(|v| v.to_str().ok()).unwrap_or_default();
    let wait = |reset: i64| -> Duration {
        let d = if reset > 1_000_000_000 {
            // `Unix(reset) - now`, floored at zero below.
            (UNIX_EPOCH + Duration::from_secs(reset as u64)).duration_since(now).unwrap_or(Duration::ZERO)
        } else {
            Duration::from_secs(reset as u64)
        };
        d.min(MAX_RETRY_AFTER)
    };
    let field = get("ratelimit");
    if !field.is_empty() {
        let param = |key: &str| -> Option<i64> {
            for part in field.split(';') {
                if let Some(v) = part.trim().strip_prefix(&format!("{key}=")) {
                    return number(v);
                }
            }
            None
        };
        if let Some(0) = param("r") {
            return (wait(param("t").unwrap_or(0)), true);
        }
    }
    for (remaining, reset) in [("ratelimit-remaining", "ratelimit-reset"), ("x-ratelimit-remaining", "x-ratelimit-reset")] {
        if number(get(remaining)) == Some(0) {
            return (wait(number(get(reset)).unwrap_or(0)), true);
        }
    }
    (Duration::ZERO, false)
}

/// Go `pauseKey` is the upstream a request goes to: its host, and a hash of the credential it carries, if any.
fn pause_key(req: &reqwest::Request) -> String {
    // Go keys by `url.Host`, which carries the port when the URL names one.
    let url = req.url();
    let host = match (url.host_str(), url.port()) {
        (Some(h), Some(p)) => format!("{h}:{p}"),
        (Some(h), None) => h.to_owned(),
        (None, _) => String::new(),
    };
    let mut credential = req
        .headers()
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .unwrap_or_default()
        .to_owned();
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

/// Go `retryAfter` reads a Retry-After of seconds or an HTTP date, capped at `MAX_RETRY_AFTER`. `None` when none was
/// sent that reads.
pub fn retry_after(h: &HeaderMap, now: SystemTime) -> Option<Duration> {
    let v = h.get("retry-after").and_then(|v| v.to_str().ok()).unwrap_or_default().trim();
    if v.is_empty() {
        return None;
    }
    let d = match v.parse::<i64>() {
        Ok(secs) if secs >= 0 => Duration::from_secs(secs.min(MAX_RETRY_AFTER.as_secs() as i64) as u64),
        _ => {
            let at = httpdate::parse_http_date(v).ok()?;
            at.duration_since(now).unwrap_or(Duration::ZERO)
        }
    };
    Some(d.min(MAX_RETRY_AFTER))
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
