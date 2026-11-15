//! Runtime-wide circuit state for indexer origins.
//!
//! A failed title is not cached, so a dead origin would otherwise be asked by every cold list until every request
//! spent its scrape budget. Three consecutive timeouts/unreachable answers open that origin's circuit. Lists then
//! keep it in their coverage as `unreachable` without waiting, preserving the empty-list quorum, while one request
//! per recovery window is allowed through to close the circuit on the first answer.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use tokio::time::Instant;

pub(crate) const DOWN_AFTER: u32 = 3;
pub(crate) const RECOVERY_PROBE_EVERY: Duration = Duration::from_secs(30);
const MAX_TRACKED_ORIGINS: usize = 256;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct CircuitOpen;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct CallPermit {
    probe_at: Option<Instant>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CallOutcome {
    Answered,
    Failed,
    /// Refusals and permanent configuration errors say nothing about reachability.
    Neutral,
}

pub(crate) struct IndexerHealth {
    origins: Mutex<HashMap<String, Origin>>,
    probe_every_ms: AtomicU64,
}

struct Origin {
    failures: u32,
    down: bool,
    next_probe: Instant,
    /// When the origin went down or its most recent recovery probe was admitted. Older failures are stragglers.
    probe_at: Instant,
    /// Failures from calls that began no later than this successful call are stale and cannot undo recovery.
    last_answer_started: Option<Instant>,
    last_seen: Instant,
}

impl Default for IndexerHealth {
    fn default() -> Self {
        Self::new(RECOVERY_PROBE_EVERY)
    }
}

impl IndexerHealth {
    pub(crate) fn new(probe_every: Duration) -> Self {
        Self {
            origins: Mutex::new(HashMap::new()),
            probe_every_ms: AtomicU64::new(probe_every.as_millis().try_into().unwrap_or(u64::MAX)),
        }
    }

    fn probe_every(&self) -> Duration {
        Duration::from_millis(self.probe_every_ms.load(Ordering::Relaxed))
    }

    #[cfg(any(test, feature = "test-support"))]
    pub(crate) fn set_probe_every(&self, every: Duration) {
        self.probe_every_ms.store(every.as_millis().try_into().unwrap_or(u64::MAX), Ordering::Relaxed);
        let next = Instant::now() + every;
        for origin in self.lock().values_mut().filter(|origin| origin.down) {
            origin.next_probe = next;
        }
    }

    fn lock(&self) -> MutexGuard<'_, HashMap<String, Origin>> {
        self.origins.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Admit ordinary calls and exactly one recovery probe per window while an origin is known down.
    pub(crate) fn admit(&self, key: &str) -> Result<CallPermit, CircuitOpen> {
        let now = Instant::now();
        let mut origins = self.lock();
        let Some(origin) = origins.get_mut(key) else {
            return Ok(CallPermit { probe_at: None });
        };
        origin.last_seen = now;
        if !origin.down {
            return Ok(CallPermit { probe_at: None });
        }
        if now >= origin.next_probe {
            origin.next_probe = now + self.probe_every();
            origin.probe_at = now;
            return Ok(CallPermit { probe_at: Some(now) });
        }
        Err(CircuitOpen)
    }

    /// Recheck after local admission. Calls may have waited while other failures opened the circuit; only the
    /// recovery probe that opened the current probe window is allowed to proceed through an open circuit.
    pub(crate) fn confirm(&self, key: &str, permit: CallPermit) -> Result<(), CircuitOpen> {
        let origins = self.lock();
        let Some(origin) = origins.get(key) else { return Ok(()) };
        if !origin.down || permit.probe_at == Some(origin.probe_at) {
            Ok(())
        } else {
            Err(CircuitOpen)
        }
    }

    /// Fold in one call. An answer closes the circuit immediately; only timeout/unreachable failures open it.
    pub(crate) fn record(&self, key: &str, outcome: CallOutcome, started: Instant) {
        let now = Instant::now();
        let mut origins = self.lock();
        if !origins.contains_key(key) && origins.len() >= MAX_TRACKED_ORIGINS {
            // Evict the least recently used state. Keys are origins, never credentials, but configs may name
            // arbitrary household sources; the circuit must not turn that into unbounded process memory.
            if let Some(oldest) =
                origins.iter().min_by_key(|(_, origin)| origin.last_seen).map(|(key, _)| key.clone())
            {
                origins.remove(&oldest);
            }
        }
        let origin = origins.entry(key.to_owned()).or_insert(Origin {
            failures: 0,
            down: false,
            next_probe: now,
            probe_at: now,
            last_answer_started: None,
            last_seen: now,
        });
        origin.last_seen = now;
        match outcome {
            CallOutcome::Answered => {
                origin.failures = 0;
                origin.down = false;
                origin.last_answer_started =
                    Some(origin.last_answer_started.map_or(started, |previous| previous.max(started)));
            }
            CallOutcome::Neutral => {}
            CallOutcome::Failed => {
                if origin.last_answer_started.is_some_and(|answer| started <= answer) {
                    return;
                }
                origin.failures = origin.failures.saturating_add(1);
                if origin.failures >= DOWN_AFTER {
                    if !origin.down {
                        origin.down = true;
                        origin.probe_at = now;
                        origin.next_probe = now + self.probe_every();
                    } else if started >= origin.probe_at {
                        origin.next_probe = now + self.probe_every();
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test(start_paused = true)]
    async fn one_probe_per_window_and_one_answer_closes_the_circuit() {
        let health = IndexerHealth::new(Duration::from_secs(10));
        for _ in 0..DOWN_AFTER {
            health.record("dead.example", CallOutcome::Failed, Instant::now());
        }
        assert_eq!(health.admit("dead.example"), Err(CircuitOpen));

        tokio::time::advance(Duration::from_secs(10)).await;
        let probe = health.admit("dead.example").expect("due recovery probe was refused");
        assert!(health.confirm("dead.example", probe).is_ok(), "the admitted probe was not confirmed");
        assert_eq!(health.admit("dead.example"), Err(CircuitOpen), "more than one probe escaped");

        health.record("dead.example", CallOutcome::Answered, Instant::now());
        assert!(health.admit("dead.example").is_ok(), "an answer did not close the circuit");
    }

    #[test]
    fn refusal_is_neutral_and_origins_are_isolated() {
        let health = IndexerHealth::default();
        for _ in 0..DOWN_AFTER {
            health.record("dead.example", CallOutcome::Failed, Instant::now());
            health.record("refused.example", CallOutcome::Neutral, Instant::now());
        }
        assert_eq!(health.admit("dead.example"), Err(CircuitOpen));
        assert!(health.admit("refused.example").is_ok());
        assert!(health.admit("healthy.example").is_ok());
    }

    #[test]
    fn failures_started_before_a_newer_answer_cannot_reopen_recovery() {
        let health = IndexerHealth::default();
        let stale_started = Instant::now();
        for _ in 0..DOWN_AFTER {
            health.record("recovering.example", CallOutcome::Failed, stale_started);
        }
        assert_eq!(health.admit("recovering.example"), Err(CircuitOpen));

        let answer_started = stale_started + Duration::from_millis(1);
        health.record("recovering.example", CallOutcome::Answered, answer_started);
        for _ in 0..DOWN_AFTER {
            health.record("recovering.example", CallOutcome::Failed, stale_started);
        }

        assert!(health.admit("recovering.example").is_ok(), "stale failures reopened a recovered circuit");
    }

    #[test]
    fn a_call_queued_before_the_circuit_opened_is_refused_after_local_admission() {
        let health = IndexerHealth::default();
        let queued = health.admit("dead.example").expect("healthy origin refused an ordinary call");
        for _ in 0..DOWN_AFTER {
            health.record("dead.example", CallOutcome::Failed, Instant::now());
        }
        assert_eq!(health.confirm("dead.example", queued), Err(CircuitOpen));
    }
}
