//! Port of `internal/scout/decisionlog.go`. Work package 6.
//!
//! Decision logging: every decision line shares one shape, so a line read from the journal alone explains and
//! reproduces a reported problem.
//!
//! ```text
//! event=<token> outcome=<served|skipped|refused|degraded|fallback> reason=<text> \
//!     upstream=<indexer/debrid name, when one is involved> dur_ms=<n> rid=<id> [identity…]
//! ```
//!
//! The identity fields after `rid` are gated by `LOG_IDENTITY` as one unit, built in `decision_line` so no call
//! site can forget the gate.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use crate::rank::{RawStream, GIB};
use crate::stores::short_hash;

/// Go `maxLoggedTitle`: a release name in a log line is cut here; the line must stay one line.
pub const MAX_LOGGED_TITLE: usize = 140;

/// Go `logIdentity`, which mirrors `Settings.LogIdentity` (LOG_IDENTITY, default on). Set once at startup, never from
/// a request.
static LOG_IDENTITY: AtomicBool = AtomicBool::new(true);

/// Go `SetLogIdentity`: turns the identity fields on new decision lines on or off. Called once at startup.
pub fn set_log_identity(on: bool) {
    LOG_IDENTITY.store(on, Ordering::Relaxed);
}

/// Go `decisionLine`: composes the shared shape. `identity` is whatever the caller built; this is the one place
/// that decides whether it survives into the line, so LOG_IDENTITY has exactly one choke point rather than one per
/// call site that each has to remember it.
pub fn decision_line(
    event: &str,
    outcome: &str,
    reason: &str,
    upstream: &str,
    dur: Duration,
    rid: &str,
    identity: &str,
) -> String {
    let mut line = format!("event={event} outcome={outcome} reason={reason}");
    if !upstream.is_empty() {
        line.push_str(&format!(" upstream={upstream}"));
    }
    line.push_str(&format!(" dur_ms={}", dur.as_millis()));
    if !rid.is_empty() {
        line.push_str(" rid=");
        line.push_str(rid);
    }
    if LOG_IDENTITY.load(Ordering::Relaxed) && !identity.is_empty() {
        line.push(' ');
        line.push_str(identity);
    }
    line
}

/// Go `truncateForLog`: cuts a string for a log line. Go cuts bytes; this cuts back to the nearest character boundary,
/// so the cut never splits a character.
fn truncate_for_log(s: &str, n: usize) -> String {
    if s.len() <= n {
        return s.to_owned();
    }
    let mut cut = n;
    while !s.is_char_boundary(cut) {
        cut -= 1;
    }
    format!("{}…", &s[..cut])
}

/// Go `topDropReasons`: the `n` largest drop counts as `reason:count`, highest first, ties broken by name.
/// Empty when nothing was dropped.
pub fn top_drop_reasons(counts: &HashMap<String, i64>, n: usize) -> String {
    let mut all: Vec<(&String, i64)> = counts.iter().map(|(k, v)| (k, *v)).collect();
    all.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(b.0)));
    all.truncate(n);
    all.iter().map(|(reason, count)| format!("{reason}:{count}")).collect::<Vec<_>>().join(",")
}

/// Go `formatPick`: one served release for the rank decision line: label, short hash, size, cached — the identity of
/// a specific pick, which is why it rides inside the same LOG_IDENTITY gate as the rest.
pub fn format_pick(s: &RawStream) -> String {
    let cached = if s.cached { "cached" } else { "uncached" };
    let size_gb = match s.size_bytes {
        Some(b) => format!("{:.1}", b as f64 / GIB as f64),
        None => "unknown".to_owned(),
    };
    // Go `%q`; Debug quoting is the same line-safe form for a release name.
    let title = truncate_for_log(&s.title, MAX_LOGGED_TITLE);
    format!("{title:?}(hash={},size_gb={size_gb},{cached})", short_hash(&s.info_hash))
}
