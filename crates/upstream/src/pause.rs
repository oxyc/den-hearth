//! The memory of an upstream that said to stop.
//!
//! A service that answers 429 is saying it about the CREDENTIAL, not about the one request that happened to hear it.
//! Remembering the refusal only where it landed (one title, one file, one batch) leaves every other request on the
//! same key asking a service that has already refused it, which is how a rate limit turns into a longer one.
//! [`Pauses`] is that memory: one [`Pause`] per key, consulted before any call and ended by the first answer.
//!
//! The policy, decided once for every service:
//!
//!  * Only a 429, or a 503 that names a wait, starts a pause ([`crate::throttle`]); any other 5xx pauses nobody.
//!  * A refusal that lands inside a pause already running is the same incident told again by a request that left
//!    before it began. It adds no strike (a burst of N would otherwise escalate the next window to 2^N), but a longer
//!    stated wait still extends the pause.
//!  * A pause is the key's own: one throttled account does not stop another.
//!
//! Time is a [`Duration`] on a clock the caller owns, so a test moves it by hand and a service with a monotonic
//! clock (or a wall one) passes that.

use std::collections::HashMap;
use std::hash::Hash;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use crate::headers::MAX_RETRY_AFTER;

/// How a table of pauses sizes its windows.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Policy {
    /// The first window when the upstream names none; doubled per refusal in a row.
    pub base: Duration,
    /// The longest window the doubling reaches, jitter included.
    pub cap: Duration,
    /// The most jitter added to a window, in thousandths of it. It keeps a pause from ending on the same instant as
    /// the one before it, which a remote counting per window would see as the same burst arriving on schedule.
    pub jitter_permille: u64,
    /// How many keys are remembered. Keys come from install configs anyone can build, so the table needs a ceiling.
    pub max_keys: usize,
}

impl Policy {
    /// The window for the `strikes`-th refusal in a row: `base` doubling per strike up to `cap`, plus
    /// `jitter_permille` thousandths of itself, never past `cap`.
    pub fn window(&self, strikes: u32, jitter_permille: u64) -> Duration {
        let exp = self.base.saturating_mul(1u32 << strikes.saturating_sub(1).min(20)).min(self.cap);
        let jitter = (exp / 1000).saturating_mul(jitter_permille.min(1000) as u32);
        exp.saturating_add(jitter).min(self.cap)
    }
}

/// One key's pause.
#[derive(Debug, Clone, Copy)]
pub struct Pause {
    /// On the caller's clock; at or before `now` means not paused.
    pub until: Duration,
    /// Refusals in a row without an answer, for the exponential window. Reset by an answer.
    pub strikes: u32,
    /// A unique revision installed by the response that created or extended this pause. A success from an older
    /// concurrent request may not clear a newer refusal.
    pub generation: u64,
}

/// The pause revision visible when a request was dispatched ([`Pauses::begin`]).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Observation(u64);

/// What a refusal did to a pause.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Paused {
    /// What is left of the pause now.
    pub wait: Duration,
    /// Refusals in a row, this one included.
    pub strikes: u32,
    /// This refusal began the pause, rather than landing inside one already running.
    pub started: bool,
}

/// Pauses by key.
pub struct Pauses<K> {
    policy: Policy,
    inner: Mutex<HashMap<K, Pause>>,
    sequence: AtomicU64,
}

impl<K: Hash + Eq + Clone> Pauses<K> {
    pub fn new(policy: Policy) -> Pauses<K> {
        Pauses { policy, inner: Mutex::new(HashMap::new()), sequence: AtomicU64::new(0) }
    }

    pub fn policy(&self) -> &Policy {
        &self.policy
    }

    /// A poisoned lock still holds consistent state: every edit under it finishes before anything can panic.
    fn lock(&self) -> MutexGuard<'_, HashMap<K, Pause>> {
        self.inner.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn next_generation(&self) -> u64 {
        self.sequence.fetch_add(1, Ordering::Relaxed).wrapping_add(1)
    }

    /// How much longer this key must wait, or `None` if it may be asked now.
    pub fn remaining(&self, key: &K, now: Duration) -> Option<Duration> {
        let left = self.lock().get(key)?.until.checked_sub(now)?;
        (!left.is_zero()).then_some(left)
    }

    /// Snapshot the pause revision immediately before a request leaves. Its answer may end only that revision
    /// ([`answered`](Self::answered)), never a newer refusal received from a concurrent request.
    pub fn begin(&self, key: &K) -> Observation {
        Observation(self.lock().get(key).map_or(0, |p| p.generation))
    }

    /// The upstream refused this key. `stated` is its own wait, if it gave one (capped at
    /// [`MAX_RETRY_AFTER`]; zero is no wait); otherwise the window doubles per refusal in a row, with jitter so the
    /// requests it held do not all come back in the same instant. A refusal inside a pause already running adds no
    /// strike and, with no stated wait, leaves the pause as it is.
    pub fn refuse(&self, key: &K, stated: Option<Duration>, now: Duration) -> Paused {
        let stated = stated.filter(|wait| !wait.is_zero()).map(|wait| wait.min(MAX_RETRY_AFTER));
        let generation = self.next_generation();
        let mut table = self.lock();
        make_room(&mut table, key, now, self.policy.max_keys);
        let pause =
            table.entry(key.clone()).or_insert(Pause { until: Duration::ZERO, strikes: 0, generation });
        pause.generation = generation;
        let started = pause.until <= now;
        if started {
            pause.strikes = pause.strikes.saturating_add(1);
        }
        let wait = match stated {
            Some(wait) => Some(wait),
            None if started => {
                Some(self.policy.window(pause.strikes, fastrand::u64(0..=self.policy.jitter_permille)))
            }
            None => None,
        };
        if let Some(wait) = wait {
            pause.until = pause.until.max(now.saturating_add(wait));
        }
        Paused { wait: pause.until.saturating_sub(now), strikes: pause.strikes, started }
    }

    /// Hold the key for a known span: a window the upstream said is used up, or a daily quota. Not an incident of
    /// refusals, so it adds no strike; and not capped here, since a quota's reset is a fact and the caller bounds it.
    pub fn hold(&self, key: &K, wait: Duration, now: Duration) {
        let generation = self.next_generation();
        let mut table = self.lock();
        make_room(&mut table, key, now, self.policy.max_keys);
        let pause =
            table.entry(key.clone()).or_insert(Pause { until: Duration::ZERO, strikes: 0, generation });
        pause.generation = generation;
        pause.until = pause.until.max(now.saturating_add(wait));
    }

    /// The upstream answered a request begun at `observed`: that ends the pause and the run of strikes behind it,
    /// unless a refusal has landed since. Says whether a pause was ended.
    pub fn answered(&self, key: &K, observed: Observation) -> bool {
        let mut table = self.lock();
        if table.get(key).is_some_and(|pause| pause.generation == observed.0) {
            return table.remove(key).is_some();
        }
        false
    }

    /// The upstream answered, with no request to date it by (a check made before anything left): end the pause
    /// whatever its revision. Says whether there was one.
    pub fn clear(&self, key: &K) -> bool {
        self.lock().remove(key).is_some()
    }

    #[cfg(test)]
    fn len(&self) -> usize {
        self.lock().len()
    }
}

/// Keep the table under `max_keys` before inserting `key`. Lapsed pauses go first (losing one costs only its
/// strike count), and if every entry is live, the one closest to lapsing: forgetting a pause costs one more refused
/// request, while an unbounded table costs the process.
fn make_room<K: Hash + Eq + Clone>(table: &mut HashMap<K, Pause>, key: &K, now: Duration, max_keys: usize) {
    if table.len() < max_keys || table.contains_key(key) {
        return;
    }
    table.retain(|_, pause| pause.until > now);
    if table.len() >= max_keys {
        if let Some(soonest) = table.iter().min_by_key(|(_, pause)| pause.until).map(|(k, _)| k.clone()) {
            table.remove(&soonest);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const SECOND: Duration = Duration::from_secs(1);

    fn policy() -> Policy {
        Policy { base: SECOND, cap: Duration::from_secs(60), jitter_permille: 200, max_keys: 8 }
    }

    fn ms(n: u64) -> Duration {
        Duration::from_millis(n)
    }

    /// The pause doubles per episode and never passes its cap, jitter included.
    #[test]
    fn a_window_doubles_and_caps() {
        let p = policy();
        assert_eq!(p.window(1, 0), SECOND);
        assert_eq!(p.window(3, 0), 4 * SECOND);
        assert_eq!(p.window(3, 199), ms(4796));
        assert_eq!(p.window(30, 199), Duration::from_secs(60));
        assert_eq!(p.window(0, 0), SECOND, "strike zero is read as the first");
    }

    /// The pause counts one episode once: requests that left before the pause report back inside it, and must not
    /// double it once each.
    #[test]
    fn a_pause_doubles_caps_and_counts_one_episode_once() {
        let pauses = Pauses::new(policy());
        assert_eq!(pauses.remaining(&"k", ms(0)), None);
        let first = pauses.refuse(&"k", None, ms(0));
        assert!(first.started && first.strikes == 1);
        assert!((ms(1000)..=ms(1200)).contains(&first.wait), "{:?}", first.wait);

        let again = pauses.refuse(&"k", None, ms(10));
        assert_eq!(
            (again.started, again.strikes),
            (false, 1),
            "a refusal inside the pause started a new episode"
        );
        assert_eq!(again.wait, first.wait - ms(10));

        let longer = pauses.refuse(&"k", Some(ms(5000)), ms(10));
        assert_eq!(longer.wait, ms(5000), "a longer stated wait must extend the pause");
        let shorter = pauses.refuse(&"k", Some(ms(100)), ms(20));
        assert_eq!(shorter.wait, ms(4990), "a shorter one must not shorten it");

        let second = pauses.refuse(&"k", None, ms(5010));
        assert_eq!((second.started, second.strikes), (true, 2));
        assert!(
            (ms(2000)..=ms(2400)).contains(&second.wait),
            "the next episode did not double: {:?}",
            second.wait
        );

        assert!(pauses.clear(&"k"));
        assert_eq!(pauses.remaining(&"k", ms(5011)), None, "an answer did not end the pause");
        let after = pauses.refuse(&"k", None, ms(5011));
        assert!(
            (ms(1000)..=ms(1200)).contains(&after.wait),
            "an answer did not forget the strikes: {:?}",
            after.wait
        );
    }

    #[test]
    fn a_burst_of_refusals_inside_one_pause_counts_once() {
        let pauses = Pauses::new(policy());
        let first = pauses.refuse(&"k", None, ms(0)).wait;
        for _ in 0..7 {
            pauses.refuse(&"k", None, ms(1));
        }
        assert!(first <= ms(1200));
        let second = pauses.refuse(&"k", None, first + SECOND);
        assert!(
            second.wait >= 2 * SECOND && second.wait <= ms(2400),
            "a burst of 8 escalated to {:?}",
            second.wait
        );
    }

    #[test]
    fn a_zero_wait_is_no_wait_and_a_stated_day_is_an_hour() {
        let pauses = Pauses::new(policy());
        let zero = pauses.refuse(&"zero", Some(Duration::ZERO), ms(0));
        assert!(zero.wait >= SECOND, "a stated zero did not fall to the window: {:?}", zero.wait);
        let day = pauses.refuse(&"day", Some(Duration::from_secs(86_400)), ms(0));
        assert_eq!(day.wait, MAX_RETRY_AFTER);
    }

    #[test]
    fn a_hold_adds_no_strike_and_is_not_capped() {
        let pauses = Pauses::new(policy());
        pauses.hold(&"k", Duration::from_secs(7200), ms(0));
        assert_eq!(pauses.remaining(&"k", ms(0)), Some(Duration::from_secs(7200)));
        pauses.hold(&"k", Duration::from_secs(10), ms(0));
        assert_eq!(
            pauses.remaining(&"k", ms(0)),
            Some(Duration::from_secs(7200)),
            "a hold shortened a longer one"
        );
        let refused = pauses.refuse(&"fresh", None, ms(0));
        assert_eq!(refused.strikes, 1);
        pauses.hold(&"fresh", SECOND, ms(0));
        assert_eq!(
            pauses.refuse(&"fresh", None, refused.wait + SECOND).strikes,
            2,
            "a hold counted as a strike"
        );
    }

    #[test]
    fn keys_are_paused_separately() {
        let pauses = Pauses::new(policy());
        pauses.refuse(&"a", Some(Duration::from_secs(30)), ms(0));
        assert!(pauses.remaining(&"a", ms(0)).is_some());
        assert_eq!(pauses.remaining(&"b", ms(0)), None);
    }

    #[test]
    fn an_older_answer_cannot_clear_a_newer_pause() {
        let pauses = Pauses::new(policy());
        let older = pauses.begin(&"k");
        pauses.refuse(&"k", Some(Duration::from_secs(30)), ms(0));

        assert!(!pauses.answered(&"k", older));
        assert!(
            pauses.remaining(&"k", ms(0)).is_some(),
            "an older in-flight success erased a newer 429 pause"
        );

        let after_pause = pauses.begin(&"k");
        assert!(pauses.answered(&"k", after_pause));
        assert_eq!(pauses.remaining(&"k", ms(0)), None, "a request begun after the pause could not clear it");
        assert!(!pauses.answered(&"k", after_pause), "nothing left to end");
    }

    #[test]
    fn the_table_is_bounded() {
        let pauses = Pauses::new(policy());
        for i in 0..(policy().max_keys as u64 + 5) {
            pauses.hold(&i, Duration::from_secs(60 + i), ms(0));
        }
        assert_eq!(pauses.len(), policy().max_keys);
        // The newest survive; the ones closest to lapsing made room.
        assert!(pauses.remaining(&(policy().max_keys as u64 + 4), ms(0)).is_some());
        assert_eq!(pauses.remaining(&0, ms(0)), None);

        // Lapsed pauses go before live ones.
        let pauses = Pauses::new(Policy { max_keys: 2, ..policy() });
        pauses.hold(&1, SECOND, ms(0));
        pauses.hold(&2, Duration::from_secs(100), ms(0));
        pauses.hold(&3, Duration::from_secs(100), Duration::from_secs(5));
        assert_eq!(pauses.remaining(&2, Duration::from_secs(5)), Some(Duration::from_secs(95)));
        assert!(pauses.remaining(&3, Duration::from_secs(5)).is_some());
    }
}
