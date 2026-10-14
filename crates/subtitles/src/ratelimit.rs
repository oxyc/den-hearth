//! Rate-limit pauses shared across requests.
//!
//! A service that says "slow down" is saying it about the CREDENTIAL, not about the one request that
//! happened to hear it. Remembering the refusal only where it landed — one title, one file, one batch
//! — left every other request on the same key asking a service that had already refused it, which is
//! how a rate limit turns into a longer one. A `Pauses` is that memory: one entry per hashed
//! credential, consulted before any call and cleared by the first answer. The pause itself, its
//! policy and the header parsing are `den-upstream`'s, shared with Reel, Remux and Scout; what is
//! here is Subtitles' window and the bound on calls awaiting one credential at once.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, Weak};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

pub use den_upstream::Observation;
use den_upstream::Policy;
use reqwest::header::HeaderMap;
use reqwest::StatusCode;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio::time::Instant;

/// Credentials tracked at once. An entry is a few bytes, but the keys come from install URLs, which
/// anyone can mint, so the map needs a ceiling like everything else keyed by them.
const MAX_KEYS: usize = 1024;
/// Calls allowed to be awaiting one credential at once. One translation already uses four parallel
/// batches; this keeps a second film or a burst of distinct searches from multiplying that window.
const MAX_IN_FLIGHT_PER_KEY: usize = 4;

/// The window when a service refuses without saying how long: two seconds, doubled per refusal in a
/// row up to an hour, with up to a quarter again on top so the requests it held do not all come back
/// in the same instant.
const POLICY: Policy = Policy {
    base: Duration::from_secs(2),
    cap: Duration::from_secs(60 * 60),
    jitter_permille: 250,
    max_keys: MAX_KEYS,
};

/// A hashed credential. Hashed so a key never sits in memory in a second place, and scoped so the
/// same string used for two services does not share one pause.
pub fn key(scope: &str, credential: &str) -> u64 {
    crate::httputil::stable_hash(format!("{scope}\n{credential}").as_bytes())
}

/// Is this refusal a rate limit? A 429 always; a 503 only when it names a wait, since a bare 503 is
/// the service being unwell rather than the credential being told to stop.
pub fn throttled(status: StatusCode, headers: &HeaderMap) -> bool {
    den_upstream::throttle(status, headers, SystemTime::now()).is_some()
}

pub struct Pauses {
    table: den_upstream::Pauses<u64>,
    /// The pause table's clock: time since this table was made, on tokio's clock so a paused test
    /// clock moves it.
    origin: Instant,
    admission: Mutex<HashMap<u64, Weak<Semaphore>>>,
}

impl Default for Pauses {
    fn default() -> Pauses {
        Pauses {
            table: den_upstream::Pauses::new(POLICY),
            origin: Instant::now(),
            admission: Mutex::default(),
        }
    }
}

impl Pauses {
    fn now(&self) -> Duration {
        Instant::now().saturating_duration_since(self.origin)
    }

    /// Bound calls across every title/run sharing one credential. Weak map entries keep the key space
    /// bounded: a canceled waiter drops its Arc, and the next acquire sweeps dead entries.
    pub async fn acquire(&self, key: u64) -> OwnedSemaphorePermit {
        let semaphore = {
            let mut entries = crate::lock(&self.admission);
            entries.retain(|_, semaphore| semaphore.strong_count() > 0);
            match entries.get(&key).and_then(Weak::upgrade) {
                Some(semaphore) => semaphore,
                None => {
                    let semaphore = Arc::new(Semaphore::new(MAX_IN_FLIGHT_PER_KEY));
                    entries.insert(key, Arc::downgrade(&semaphore));
                    semaphore
                }
            }
        };
        semaphore.acquire_owned().await.expect("credential admission semaphore closed")
    }

    /// Snapshot the pause revision immediately before a request leaves. Its response may clear only
    /// that revision, never a newer refusal received from a concurrent request.
    pub fn begin(&self, key: u64) -> Observation {
        self.table.begin(&key)
    }

    /// How much longer this credential must wait, or `None` if it may call now.
    pub fn remaining(&self, key: u64) -> Option<Duration> {
        self.table.remaining(&key, self.now())
    }

    /// The service refused on rate grounds. `stated` is its own `Retry-After`, if it gave one;
    /// otherwise the wait doubles per refusal in a row, with jitter so the requests it held do not
    /// all come back in the same instant. Returns the wait now in force.
    pub fn limited(&self, key: u64, stated: Option<Duration>) -> Duration {
        self.table.refuse(&key, stated, self.now()).wait
    }

    /// Hold the credential for a known span — a window the service said is used up, or a daily quota.
    /// Not capped here: a quota's reset is a fact, not a guess, and the caller bounds it.
    pub fn hold(&self, key: u64, wait: Duration) {
        self.table.hold(&key, wait, self.now());
    }

    /// The service answered. That ends the pause and the run of strikes behind it.
    pub fn answered(&self, key: u64, observed: Observation) {
        self.table.answered(&key, observed);
    }
}

/// Time left in the current UTC day — the bucket a daily quota counted by `unix_seconds / 86400`
/// resets on.
pub fn until_utc_midnight(now: SystemTime) -> Duration {
    let secs = now.duration_since(UNIX_EPOCH).map(|d| d.as_secs()).unwrap_or(0);
    Duration::from_secs(86_400 - secs % 86_400)
}

/// From `now` until a unix time; zero if it has passed.
pub fn until(at_unix: i64, now: SystemTime) -> Duration {
    let now = now.duration_since(UNIX_EPOCH).map(|d| d.as_secs() as i64).unwrap_or(0);
    Duration::from_secs(at_unix.saturating_sub(now).max(0) as u64)
}

/// An ISO 8601 UTC timestamp as OpenSubtitles writes `reset_time_utc`: `2022-01-30T06:00:47.000Z`.
pub fn iso_utc(s: &str) -> Option<i64> {
    let (date, time) = s.trim().split_once('T')?;
    let time = time.strip_suffix('Z')?.split('.').next()?;
    let mut d = date.split('-');
    let (year, month, day) = (d.next()?.parse().ok()?, d.next()?.parse().ok()?, d.next()?.parse().ok()?);
    if d.next().is_some() {
        return None;
    }
    unix_time(year, month, day, time)
}

fn unix_time(year: i64, month: u32, day: u32, hms: &str) -> Option<i64> {
    let mut t = hms.split(':');
    let (h, m, s): (u32, u32, u32) =
        (t.next()?.parse().ok()?, t.next()?.parse().ok()?, t.next()?.parse().ok()?);
    if t.next().is_some()
        || !(1..=12).contains(&month)
        || !(1..=31).contains(&day)
        || h > 23
        || m > 59
        || s > 60
    {
        return None;
    }
    Some(den_upstream::civil::days_from_civil(year, month, day) * 86_400 + i64::from(h * 3600 + m * 60 + s))
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::Ordering;

    use super::*;

    fn at(unix: u64) -> SystemTime {
        UNIX_EPOCH + Duration::from_secs(unix)
    }

    /// Subtitles' window is pinned: the policy was chosen for OpenSubtitles' and the translation providers' limits.
    #[test]
    fn the_pause_policy_is_pinned() {
        assert_eq!(POLICY.base, Duration::from_secs(2));
        assert_eq!(POLICY.cap, Duration::from_secs(60 * 60));
        assert_eq!(POLICY.jitter_permille, 250);
        assert_eq!(POLICY.max_keys, 1024);
        assert_eq!(MAX_IN_FLIGHT_PER_KEY, 4);
    }

    #[test]
    fn a_rate_limit_is_a_429_or_a_503_that_names_a_wait() {
        let mut named = HeaderMap::new();
        named.insert("retry-after", "30".parse().unwrap());
        assert!(throttled(StatusCode::TOO_MANY_REQUESTS, &HeaderMap::new()));
        assert!(throttled(StatusCode::SERVICE_UNAVAILABLE, &named));
        assert!(!throttled(StatusCode::SERVICE_UNAVAILABLE, &HeaderMap::new()));
        assert!(!throttled(StatusCode::BAD_GATEWAY, &named));
    }

    #[test]
    fn iso_reset_times_parse_as_opensubtitles_writes_them() {
        assert_eq!(iso_utc("2022-01-30T06:00:47.000Z"), Some(1_643_522_447));
        assert_eq!(iso_utc("2022-01-30T06:00:47Z"), Some(1_643_522_447));
        assert_eq!(iso_utc("1970-01-01T00:00:00Z"), Some(0));
        for junk in ["", "2022-01-30 06:00:47", "2022-01-30T06:00:47", "2022-13-01T00:00:00Z", "tomorrow"] {
            assert_eq!(iso_utc(junk), None, "{junk:?}");
        }
    }

    #[test]
    fn midnight_is_the_end_of_the_utc_day() {
        assert_eq!(until_utc_midnight(at(86_400 * 3)), Duration::from_secs(86_400));
        assert_eq!(until_utc_midnight(at(86_400 * 3 + 86_399)), Duration::from_secs(1));
    }

    #[tokio::test(start_paused = true)]
    async fn an_answer_ends_a_pause_and_its_strikes() {
        let p = Pauses::default();
        let k = key("test", "credential");
        let base = POLICY.base;
        assert_eq!(p.remaining(k), None);

        // Unstated refusals double, and a refusal from a request already in flight does not.
        let first = p.limited(k, None);
        assert!(first >= base && first <= base * 5 / 4, "{first:?}");
        assert!(p.limited(k, None) <= first, "an in-flight sibling doubled the wait");
        tokio::time::advance(first).await;
        let second = p.limited(k, None);
        assert!(second >= base * 2, "a second refusal did not back off further: {second:?}");

        // A stated wait is taken as stated, never shortening one already in force.
        p.limited(k, Some(Duration::from_secs(30)));
        assert!(p.remaining(k).unwrap() > Duration::from_secs(29));

        let observed = p.begin(k);
        p.answered(k, observed);
        assert_eq!(p.remaining(k), None, "an answer left the credential paused");
        let fresh = p.limited(k, None);
        assert!(fresh <= base * 5 / 4, "an answer did not reset the backoff: {fresh:?}");
    }

    #[test]
    fn an_older_answer_cannot_clear_a_newer_pause() {
        let p = Pauses::default();
        let k = key("test", "credential");
        let older = p.begin(k);
        p.limited(k, Some(Duration::from_secs(30)));

        p.answered(k, older);
        assert!(p.remaining(k).is_some(), "an older in-flight success erased a newer 429 pause");

        let after_pause = p.begin(k);
        p.answered(k, after_pause);
        assert_eq!(p.remaining(k), None, "a request begun after the pause could not clear it");
    }

    #[tokio::test]
    async fn admission_is_bounded_across_one_credential() {
        use std::sync::atomic::AtomicUsize;

        let pauses = Arc::new(Pauses::default());
        let running = Arc::new(AtomicUsize::new(0));
        let peak = Arc::new(AtomicUsize::new(0));
        let key = key("test", "credential");
        let mut tasks = Vec::new();
        for _ in 0..12 {
            let (pauses, running, peak) = (pauses.clone(), running.clone(), peak.clone());
            tasks.push(tokio::spawn(async move {
                let _permit = pauses.acquire(key).await;
                let active = running.fetch_add(1, Ordering::SeqCst) + 1;
                peak.fetch_max(active, Ordering::SeqCst);
                tokio::task::yield_now().await;
                running.fetch_sub(1, Ordering::SeqCst);
            }));
        }
        for task in tasks {
            task.await.unwrap();
        }
        assert_eq!(peak.load(Ordering::SeqCst), MAX_IN_FLIGHT_PER_KEY);
    }

    #[tokio::test]
    async fn a_cancelled_admission_waiter_does_not_leak_a_slot() {
        let pauses = Arc::new(Pauses::default());
        let key = key("test", "credential");
        let mut held = Vec::new();
        for _ in 0..MAX_IN_FLIGHT_PER_KEY {
            held.push(pauses.acquire(key).await);
        }
        let waiter = {
            let pauses = pauses.clone();
            tokio::spawn(async move { pauses.acquire(key).await })
        };
        tokio::task::yield_now().await;
        waiter.abort();
        let _ = waiter.await;
        drop(held.pop());

        let _permit = tokio::time::timeout(Duration::from_secs(1), pauses.acquire(key))
            .await
            .expect("a canceled waiter leaked the released credential slot");
    }
}
