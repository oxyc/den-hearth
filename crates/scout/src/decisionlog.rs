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
use std::time::Duration;

use crate::rank::RawStream;

/// Go `maxLoggedTitle`: a release name in a log line is cut here; the line must stay one line.
pub const MAX_LOGGED_TITLE: usize = 140;

/// Go `SetLogIdentity`: turns the identity fields on new decision lines on or off. Called once at startup.
pub fn set_log_identity(on: bool) {
    todo!("decisionlog.go SetLogIdentity")
}

/// Go `decisionLine`: composes the shared shape. `identity` is whatever the caller built; this is the one place
/// that decides whether it survives into the line.
pub fn decision_line(
    event: &str,
    outcome: &str,
    reason: &str,
    upstream: &str,
    dur: Duration,
    rid: &str,
    identity: &str,
) -> String {
    todo!("decisionlog.go decisionLine")
}

/// Go `topDropReasons`: the `n` largest drop counts as `reason:count`, highest first, ties broken by name.
/// Empty when nothing was dropped.
pub fn top_drop_reasons(counts: &HashMap<String, i64>, n: usize) -> String {
    todo!("decisionlog.go topDropReasons")
}

/// Go `formatPick`: one served release for the rank decision line: label, short hash, size, cached.
pub fn format_pick(s: &RawStream) -> String {
    todo!("decisionlog.go formatPick")
}
