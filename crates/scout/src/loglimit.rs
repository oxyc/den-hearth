//! Port of `internal/scout/loglimit.go`. Work package 6.
//!
//! The log records changes of state, not events. A request-path line fires on every request for as long
//! as its cause lasts — an indexer that is down is down for every scrape, a client waiting on a download
//! polls every two seconds — so each such line goes through logLimited, and a condition writes at most
//! one line a minute however much traffic it sees. Per-request detail is what LOG_REQUESTS is for.

use std::cell::RefCell;
use std::collections::BTreeMap;
use std::sync::{Mutex, PoisonError};
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

thread_local! {
    /// The lines a `capture` on this thread is collecting; `None` outside one.
    static CAPTURED: RefCell<Option<String>> = const { RefCell::new(None) };
}

/// The crate's `log.Printf`: one line out. It goes to stderr, where the container runtime's journal stamps it. On a
/// thread inside `capture` the line is kept instead, which is how a test reads what a route logged.
pub(crate) fn emit(line: &str) {
    let kept = CAPTURED.with(|c| match c.borrow_mut().as_mut() {
        Some(buf) => {
            buf.push_str(line);
            buf.push('\n');
            true
        }
        None => false,
    });
    if !kept {
        eprintln!("{line}");
    }
}

/// Runs `f` with this thread's log lines captured, and returns them, one per line. Go's `captureLog` redirects the
/// process-wide `log` output; a thread-local sink keeps parallel tests from reading each other's lines.
#[cfg(test)]
pub(crate) fn capture(f: impl FnOnce()) -> String {
    CAPTURED.with(|c| *c.borrow_mut() = Some(String::new()));
    f();
    CAPTURED.with(|c| c.borrow_mut().take().unwrap_or_default())
}

/// Go `logLimited` writes the line unless the same condition already wrote one inside `LOG_INTERVAL`; the next
/// line of that condition to get through says how many were held back in between. There is no timer — a
/// held-back count is reported by the condition's own next line, whenever that comes, or not at all if it
/// never recurs, which is the state change the silence already told you about.
///
/// The key names the CONDITION and must come from a fixed vocabulary: a literal, an indexer id, a debrid
/// service. Never a hash, a title or anything a caller picks, or the map grows with traffic.
pub fn log_limited(key: &str, message: &str) {
    let now = Instant::now();
    let held_back = {
        // Every edit under this lock finishes before anything can panic, so a poisoned guard is still consistent.
        let mut last = LIMITED_LOG.lock().unwrap_or_else(PoisonError::into_inner);
        match last.get_mut(key) {
            Some(e) if now.duration_since(e.at) < LOG_INTERVAL => {
                e.suppressed += 1;
                return;
            }
            Some(e) => {
                let held = e.suppressed;
                *e = LimitedEntry { at: now, suppressed: 0 };
                held
            }
            None => {
                last.insert(key.to_owned(), LimitedEntry { at: now, suppressed: 0 });
                0
            }
        }
    };
    if held_back > 0 {
        emit(&format!("{message} ({held_back} more like this since the last line)"));
    } else {
        emit(message);
    }
}
