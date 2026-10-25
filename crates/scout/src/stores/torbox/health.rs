//! How TorBox has been answering lately — ok, slow or down — derived from the calls Scout already makes.
//!
//! Every TorBox request goes through `TorBoxStore::send`, which reports its outcome here. Nothing here makes a call
//! of its own. While TorBox is down, a play or probe is answered at once with `debrid_down` instead of waiting out the
//! status budget again (`admit`), and one request every `RECOVERY_PROBE_EVERY` is let through, so the first answer
//! after an outage ends it.

use std::sync::Mutex;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use serde_json::{json, Value};
use tokio::time::Instant;

use crate::loglimit::emit;

/// An answer slower than this marks TorBox slow: half the status budget, so a read that still answers in time is
/// already flagged. A call dropped unanswered after at least this long counts as a timeout.
pub(crate) const SLOW_ANSWER: Duration = Duration::from_secs(4);
/// Consecutive failed calls (no answer in time, a transport error, a 5xx) that mark TorBox down.
pub(crate) const DOWN_AFTER: u32 = 3;
/// While down, one request is let through this often to find out whether TorBox is back.
pub(crate) const RECOVERY_PROBE_EVERY: Duration = Duration::from_secs(30);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DebridState {
    Ok,
    Slow,
    Down,
}

impl DebridState {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Ok => "ok",
            Self::Slow => "slow",
            Self::Down => "down",
        }
    }
}

/// What one TorBox call came to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CallOutcome {
    /// TorBox answered (any status below 500), after this long.
    Answered(Duration),
    /// No usable answer: `timeout`, `unreachable` or `http_5xx`.
    Failed(&'static str),
}

/// Why a request was answered without asking TorBox, and when asking again could help.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct DebridDown {
    pub(crate) reason: &'static str,
    pub(crate) retry_after: Duration,
}

pub(crate) struct DebridHealth(Mutex<Health>);

struct Health {
    /// False until the first call is reported: before that there is nothing to say.
    observed: bool,
    state: DebridState,
    /// When `state` began, in Unix milliseconds.
    since_ms: u64,
    reason: &'static str,
    failures: u32,
    /// While down, when the next request may go through to find out whether TorBox is back.
    next_probe: Instant,
    /// When TorBox went down or the last recovery probe was let through; calls older than this are stragglers.
    probe_at: Instant,
}

fn now_ms() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).map_or(0, |d| d.as_millis() as u64)
}

impl Default for DebridHealth {
    fn default() -> Self {
        Self(Mutex::new(Health {
            observed: false,
            state: DebridState::Ok,
            since_ms: now_ms(),
            reason: "",
            failures: 0,
            next_probe: Instant::now(),
            probe_at: Instant::now(),
        }))
    }
}

impl Health {
    fn enter(&mut self, state: DebridState, reason: &'static str) {
        if state != self.state {
            emit(&format!(
                "debrid health: torbox {} → {}{}",
                self.state.as_str(),
                state.as_str(),
                if reason.is_empty() { String::new() } else { format!(" ({reason})") }
            ));
            self.state = state;
            self.since_ms = now_ms();
        }
        self.reason = reason;
    }
}

impl DebridHealth {
    fn lock(&self) -> std::sync::MutexGuard<'_, Health> {
        self.0.lock().unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Folds in the outcome of one call, made at `started`. An answer ends a failure streak at once; `DOWN_AFTER`
    /// failures in a row mark TorBox down, and fewer mark it slow.
    pub(crate) fn record(&self, outcome: CallOutcome, started: Instant) {
        let mut h = self.lock();
        h.observed = true;
        match outcome {
            CallOutcome::Answered(took) => {
                h.failures = 0;
                if took >= SLOW_ANSWER {
                    h.enter(DebridState::Slow, "slow_answer");
                } else {
                    h.enter(DebridState::Ok, "");
                }
            }
            CallOutcome::Failed(reason) => {
                h.failures = h.failures.saturating_add(1);
                if h.failures >= DOWN_AFTER {
                    // The window restarts at the failure of a call made since the last recovery probe was let
                    // through: that probe can fail after longer than the window (a play spends up to its 45 s
                    // resolve budget), and without this every request after it would be a probe too. A call that
                    // started earlier is a straggler from before, and must not keep pushing recovery out.
                    let now = Instant::now();
                    if h.state != DebridState::Down {
                        h.next_probe = now + RECOVERY_PROBE_EVERY;
                        h.probe_at = now;
                    } else if started >= h.probe_at {
                        h.next_probe = now + RECOVERY_PROBE_EVERY;
                    }
                    h.enter(DebridState::Down, reason);
                } else if h.state != DebridState::Down {
                    h.enter(DebridState::Slow, reason);
                }
            }
        }
    }

    /// Whether a request should go ahead and ask TorBox. Refused only while down, and then not when a recovery probe
    /// is due: that request is let through, and the next one waits another `RECOVERY_PROBE_EVERY`.
    pub(crate) fn admit(&self) -> Result<(), DebridDown> {
        let mut h = self.lock();
        if h.state != DebridState::Down {
            return Ok(());
        }
        let now = Instant::now();
        if now >= h.next_probe {
            h.next_probe = now + RECOVERY_PROBE_EVERY;
            h.probe_at = now;
            return Ok(());
        }
        Err(DebridDown { reason: h.reason, retry_after: h.next_probe - now })
    }

    pub(crate) fn state(&self) -> DebridState {
        self.lock().state
    }

    /// The `/health` block, or `None` before any TorBox call has been made.
    pub(crate) fn report(&self) -> Option<Value> {
        let h = self.lock();
        if !h.observed {
            return None;
        }
        let mut out = json!({"provider": "torbox", "state": h.state.as_str(), "since": h.since_ms});
        if !h.reason.is_empty() {
            out["reason"] = json!(h.reason);
        }
        Some(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn says_nothing_until_a_call_is_made() {
        assert!(DebridHealth::default().report().is_none());
    }

    fn fail(health: &DebridHealth, reason: &'static str) {
        health.record(CallOutcome::Failed(reason), Instant::now());
    }

    #[test]
    fn failures_mark_slow_then_down_and_one_answer_ends_it() {
        let health = DebridHealth::default();
        fail(&health, "timeout");
        assert_eq!(health.state(), DebridState::Slow);
        assert!(health.admit().is_ok(), "slow is reported, never refused");
        for _ in 1..DOWN_AFTER {
            fail(&health, "timeout");
        }
        assert_eq!(health.state(), DebridState::Down);
        let report = health.report().expect("a report");
        assert_eq!(report["state"], "down");
        assert_eq!(report["reason"], "timeout");
        assert_eq!(report["provider"], "torbox");
        assert!(report["since"].as_u64().is_some_and(|ms| ms > 0));

        health.record(CallOutcome::Answered(Duration::from_millis(80)), Instant::now());
        assert_eq!(health.state(), DebridState::Ok);
        assert!(health.report().expect("a report").get("reason").is_none());
    }

    #[test]
    fn a_slow_answer_is_slow_not_down() {
        let health = DebridHealth::default();
        health.record(CallOutcome::Answered(SLOW_ANSWER), Instant::now());
        assert_eq!(health.report().expect("a report")["state"], "slow");
        assert_eq!(health.report().expect("a report")["reason"], "slow_answer");
    }

    #[tokio::test(start_paused = true)]
    async fn while_down_one_request_per_window_goes_through() {
        let health = DebridHealth::default();
        let before_the_outage = Instant::now();
        tokio::time::advance(Duration::from_secs(1)).await;
        for _ in 0..DOWN_AFTER {
            fail(&health, "unreachable");
        }
        let refused = health.admit().expect_err("down refuses");
        assert_eq!(refused.reason, "unreachable");
        assert!(refused.retry_after <= RECOVERY_PROBE_EVERY && !refused.retry_after.is_zero());

        // A call made before TorBox was known down, failing after the window ran out, does not push recovery out.
        tokio::time::advance(RECOVERY_PROBE_EVERY).await;
        health.record(CallOutcome::Failed("timeout"), before_the_outage);
        assert!(health.admit().is_ok(), "the recovery probe must be let through");
        assert!(health.admit().is_err(), "only one request per window");

        // The probe's own call failing after longer than the window restarts it, so the next request is refused.
        let probe_call = Instant::now();
        tokio::time::advance(RECOVERY_PROBE_EVERY + RECOVERY_PROBE_EVERY / 2).await;
        health.record(CallOutcome::Failed("timeout"), probe_call);
        assert!(health.admit().is_err(), "the window restarts at the probe's failure");
    }
}
