//! Port of `internal/scout/hostlimit.go`. Work package 6.
//!
//! Outbound pacing, per upstream host.
//!
//! Nothing bounded how fast scout asked an indexer. Retry-on-shed made that worse by design: a host under
//! load answers 502, and the answer was to ask again. Torrentio's own 502s were ultimately a cold-CDN
//! problem rather than a rate limit, but the shape was there waiting — several surfaces can each issue
//! lookups at once, and the only cap in the whole system guarded the poster grid, whose comment names the
//! risk outright ("ban risk").
//!
//! A token bucket rather than a fixed delay: a household's normal use is bursty and small — open a series,
//! glance at three episodes — and should never wait. Sustained traffic is what needs a ceiling. So a burst
//! up to `burst` passes immediately, and beyond that requests queue at `every` rather than being shed or
//! failed. Queueing is the point: a shed request became "this release does not exist" for an entire
//! evening, and a request that waits 300 ms is invisible next to an 8-second scrape budget.
//!
//! A caller whose context ends (here, a dropped future) gives its token back.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::{Duration, Instant};

/// Go `hostLimiter`.
pub struct HostLimiter {
    /// Go `buckets`: one bucket per host, each behind its own lock.
    pub(crate) buckets: Mutex<HashMap<String, Arc<Mutex<Bucket>>>>,
    pub(crate) every: Duration,
    pub(crate) burst: i64,
    /// Go `maxKeys`, when set, is how many hosts the limiter remembers. Past it every bucket is forgotten at once:
    /// a host then starts again with a full burst, which costs a little pacing and nothing else. 0 means unbounded.
    pub(crate) max_keys: usize,
}

/// Go `bucket`: a token count, refilled by elapsed time.
pub struct Bucket {
    pub(crate) tokens: f64,
    pub(crate) last: Instant,
}

/// A poisoned lock still holds consistent state: every edit under these locks finishes before anything can panic.
fn locked<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(PoisonError::into_inner)
}

/// Go `newHostLimiter`: a non-positive `every` becomes one second and a `burst` below 1 becomes 1.
pub fn new_host_limiter(every: Duration, burst: i64) -> HostLimiter {
    // A non-positive interval makes the refill +Inf and every delay zero — a limiter that looks configured
    // and silently isn't. Fail loud in the only direction that is safe here: pace something.
    let every = if every.is_zero() { Duration::from_secs(1) } else { every };
    let burst = burst.max(1);
    HostLimiter { buckets: Mutex::new(HashMap::new()), every, burst, max_keys: 0 }
}

/// Go `newBoundedHostLimiter` is a hostLimiter for hosts a caller chooses, which remembers at most `max_keys` of them.
pub fn new_bounded_host_limiter(every: Duration, burst: i64, max_keys: usize) -> HostLimiter {
    let mut l = new_host_limiter(every, burst);
    l.max_keys = max_keys;
    l
}

/// Gives a spent token back when the wait that holds it is dropped before it finishes.
struct RefundOnDrop {
    bucket: Arc<Mutex<Bucket>>,
    burst: i64,
    armed: bool,
}

impl Drop for RefundOnDrop {
    fn drop(&mut self) {
        if self.armed {
            // Give the token back. The caller abandons the request, so nothing was asked of the host — and a
            // token spent on a request never sent is debt the NEXT caller pays, at a rate comparable to the
            // scrape budget itself.
            locked(&self.bucket).refund(self.burst);
        }
    }
}

/// Go `(*hostLimiter).wait`: blocks until this host may be asked again, or the future is dropped. It never
/// returns an error of its own: a dropped future is the caller's deadline, not a refusal, and the caller already
/// handles that. A `None` limiter or an empty host is a no-op, as a nil receiver is in Go.
pub async fn wait(limiter: Option<&HostLimiter>, host: &str) {
    let Some(l) = limiter else { return };
    if host.is_empty() {
        return;
    }
    let b = {
        let mut buckets = locked(&l.buckets);
        match buckets.get(host) {
            Some(b) => Arc::clone(b),
            None => {
                if l.max_keys > 0 && buckets.len() >= l.max_keys {
                    buckets.clear();
                }
                let b = Arc::new(Mutex::new(Bucket { tokens: l.burst as f64, last: Instant::now() }));
                buckets.insert(host.to_owned(), Arc::clone(&b));
                b
            }
        }
    };

    let delay = locked(&b).take(l.every, l.burst);
    if delay.is_zero() {
        return;
    }
    // Not "skip the wait when it would exceed the caller's deadline". That was tried and is a bypass: every
    // caller carries a deadline, so a client with short ones sails past the limiter entirely — the test
    // for cancellation-minting caught it immediately, with 400 of 400 callers passing free. Queueing past
    // a deadline is a real cost, but the burst is where it gets solved, not here.
    let mut guard = RefundOnDrop { bucket: b, burst: l.burst, armed: true };
    tokio::time::sleep(delay).await;
    guard.armed = false;
}

impl HostLimiter {
    /// Go `(*hostLimiter).allow` reports whether a token was free RIGHT NOW, spending one if so, and never queues.
    ///
    /// The opposite choice from `wait`, for the opposite situation. `wait` queues because its caller is a
    /// scrape whose deadline belongs to us, and a shed scrape became "this release does not exist" — waiting
    /// 300 ms is invisible next to an eight-second budget. An INBOUND request is the other way round: the
    /// caller is anonymous, the thing being spent is one of our own tasks and connections, and parking it
    /// for ten seconds is not politeness but the cost itself. Ten thousand parked requests is the attack.
    ///
    /// Spends only a whole token, so this can never drive the bucket negative and never needs a refund — which
    /// is what keeps it clear of the token-manufacturing trap documented on `take`. An empty key is never limited.
    pub fn allow(&self, key: &str) -> bool {
        if key.is_empty() {
            return true;
        }
        let b = {
            let mut buckets = locked(&self.buckets);
            Arc::clone(
                buckets
                    .entry(key.to_owned())
                    .or_insert_with(|| Arc::new(Mutex::new(Bucket { tokens: self.burst as f64, last: Instant::now() }))),
            )
        };
        let ok = locked(&b).try_take(self.every, self.burst);
        ok
    }
}

impl Bucket {
    /// Go `tryTake`.
    pub(crate) fn try_take(&mut self, every: Duration, burst: i64) -> bool {
        let now = Instant::now();
        self.tokens += now.saturating_duration_since(self.last).as_secs_f64() / every.as_secs_f64();
        if self.tokens > burst as f64 {
            self.tokens = burst as f64;
        }
        self.last = now;
        if self.tokens < 1.0 {
            return false;
        }
        self.tokens -= 1.0;
        true
    }

    /// Go `take`: refills by elapsed time, spends a token, and reports how long the caller must wait for it. The
    /// token is spent even when the bucket goes negative, so concurrent callers queue in order rather than all
    /// waking to the same free slot.
    ///
    /// Debt is NOT floored. A floor was tried and had to come out: a clamped caller is charged nothing, but a
    /// clamped caller that then cancels still refunds — so under a cancel-heavy load the floor MANUFACTURED
    /// tokens and the limiter stopped limiting (measured: ~4000 free passes in two seconds against a budget
    /// of twelve). It was also solving a problem that is not this function's to solve. The floor existed to
    /// stop a caller queueing past its own deadline; `wait` already returns the moment the caller's future is
    /// dropped, which is the same protection with none of the accounting damage — and without collapsing every
    /// saturated caller onto one identical delay, which turned the queue into a synchronised herd.
    pub(crate) fn take(&mut self, every: Duration, burst: i64) -> Duration {
        let now = Instant::now();
        self.tokens += now.saturating_duration_since(self.last).as_secs_f64() / every.as_secs_f64();
        if self.tokens > burst as f64 {
            self.tokens = burst as f64;
        }
        self.last = now;
        self.tokens -= 1.0;
        if self.tokens >= 0.0 {
            return Duration::ZERO;
        }
        Duration::try_from_secs_f64(-self.tokens * every.as_secs_f64()).unwrap_or(Duration::MAX)
    }

    /// Go `refund` returns the token a caller spent but never used, capped at the burst so it can only ever undo a
    /// spend — never bank credit the bucket did not earn by elapsed time.
    pub(crate) fn refund(&mut self, burst: i64) {
        if self.tokens < burst as f64 {
            self.tokens += 1.0;
        }
    }
}
