//! Port of `internal/scout/decisionlog.go`.
//!
//! Decision logging: every decision line shares one shape, so a line read from the journal alone explains and
//! reproduces a reported problem.
//!
//! ```text
//! event=<token> outcome=<served|skipped|refused|degraded|fallback> reason=<text> \
//!     upstream=<indexer/debrid name, when one is involved> dur_ms=<n> rid=<id> [class…] [identity…]
//! ```
//!
//! The class fields (`type=movie|series`, `kind=episode`, `top_drops=<reason:count>`, `picked_cached=yes|no`) say
//! what sort of request it was and are always written; none of them names a title or a release. The identity fields
//! (IMDb id, season/episode, release names) are off by default and written only under an explicit `LOG_IDENTITY=1`,
//! a short-lived diagnostic. They are gated as one unit, built in `decision_line` so no call site can forget the
//! gate. The info hash and the file index are never written, with or without the opt-in.

use std::collections::HashMap;
#[cfg(not(test))]
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::OnceLock;
use std::time::Duration;

use regex::Regex;

use crate::id::StreamId;
use crate::loglimit::log_limited;
use crate::rank::{RankDebug, RawStream, GIB};
use crate::scrape::ScrapeQuery;
use crate::stores::ResolveTarget;

/// Go `maxLoggedTitle`: a release name in a log line is cut here; the line must stay one line.
pub const MAX_LOGGED_TITLE: usize = 140;

/// Go `logIdentity`, which mirrors `Settings.LogIdentity` (LOG_IDENTITY, default OFF: the journal says what kind of
/// request it was, not which title or release). Set once at startup, never from a request.
#[cfg(not(test))]
static LOG_IDENTITY: AtomicBool = AtomicBool::new(false);

// A TEST build keeps the switch per thread, as the add budget and the counters are (addbudget.rs, metrics.rs). Go's
// tests run one at a time and put it back after turning it off; cargo runs ours in parallel, and a test that has it
// off would strip the identity from another test's lines. A `#[tokio::test]` keeps all its tasks on its own thread.
#[cfg(test)]
thread_local! {
    static LOG_IDENTITY: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

/// Go `SetLogIdentity`: turns the identity fields on new decision lines on or off. Called once at startup.
pub fn set_log_identity(on: bool) {
    #[cfg(not(test))]
    LOG_IDENTITY.store(on, Ordering::Relaxed);
    #[cfg(test)]
    LOG_IDENTITY.with(|c| c.set(on));
}

fn log_identity() -> bool {
    #[cfg(not(test))]
    {
        LOG_IDENTITY.load(Ordering::Relaxed)
    }
    #[cfg(test)]
    {
        LOG_IDENTITY.with(std::cell::Cell::get)
    }
}

/// Go `decisionLine`: composes the shared shape. The last argument is `(class, identity)`: `class` is the always-on
/// description of the request's kind; `identity` is whatever the caller built about the title or release, and
/// this is the one place that decides whether it survives into the line, so LOG_IDENTITY has exactly one choke
/// point rather than one per call site that each has to remember it.
pub fn decision_line(
    event: &str,
    outcome: &str,
    reason: &str,
    upstream: &str,
    dur: Duration,
    rid: &str,
    (class, identity): (&str, &str),
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
    if !class.is_empty() {
        line.push(' ');
        line.push_str(class);
    }
    if log_identity() && !identity.is_empty() {
        line.push(' ');
        line.push_str(identity);
    }
    line
}

/// Go `truncateForLog` cuts a string for a log line, the way storeErrorDetail already does for a debrid
/// error body.
///
/// Go cuts at byte `n`, which can fall inside a multi-byte character. The result is bytes, not a `String`, for
/// exactly that reason: `quote_go` then writes the stray bytes as `\xNN`, as Go's `%q` does, so the line matches.
fn truncate_for_log(s: &str, n: usize) -> Vec<u8> {
    if s.len() <= n {
        return s.as_bytes().to_vec();
    }
    let mut out = s.as_bytes()[..n].to_vec();
    out.extend_from_slice("…".as_bytes());
    out
}

/// Go `%q` (`strconv.Quote`) over bytes: double-quoted, with `"` and `\` escaped, the usual `\a\b\f\n\r\t\v`,
/// other control characters as `\xNN`, non-printable runes as `\uNNNN` / `\UNNNNNNNN`, and every byte that is not
/// valid UTF-8 as `\xNN`. Printable non-ASCII is written as is. This is what keeps a release name from writing a
/// newline into the log.
pub(crate) fn quote_go(b: &[u8]) -> String {
    static PRINT: OnceLock<Regex> = OnceLock::new();
    // strconv.IsPrint: letters, marks, numbers, punctuation and symbols, plus the ASCII space.
    let print = PRINT.get_or_init(|| Regex::new(r"^[\p{L}\p{M}\p{N}\p{P}\p{S}]$").expect("a valid pattern"));
    let mut out = String::with_capacity(b.len() + 2);
    out.push('"');
    let mut rest = b;
    while !rest.is_empty() {
        let (c, width) = match std::str::from_utf8(rest) {
            Ok(s) => (s.chars().next(), s.chars().next().map_or(0, char::len_utf8)),
            Err(e) if e.valid_up_to() > 0 => {
                // Valid up to here: take the first character of the valid prefix.
                let c = std::str::from_utf8(&rest[..e.valid_up_to()]).ok().and_then(|s| s.chars().next());
                (c, c.map_or(0, char::len_utf8))
            }
            Err(_) => (None, 1), // an invalid byte
        };
        let Some(c) = c else {
            out.push_str(&format!("\\x{:02x}", rest[0]));
            rest = &rest[1..];
            continue;
        };
        rest = &rest[width..];
        match c {
            '"' | '\\' => {
                out.push('\\');
                out.push(c);
            }
            ' ' => out.push(' '),
            c if print.is_match(c.encode_utf8(&mut [0u8; 4])) => out.push(c),
            '\u{7}' => out.push_str("\\a"),
            '\u{8}' => out.push_str("\\b"),
            '\u{c}' => out.push_str("\\f"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            '\u{b}' => out.push_str("\\v"),
            c if (c as u32) < 0x20 || c as u32 == 0x7f => out.push_str(&format!("\\x{:02x}", c as u32)),
            c if (c as u32) < 0x10000 => out.push_str(&format!("\\u{:04x}", c as u32)),
            c => out.push_str(&format!("\\U{:08x}", c as u32)),
        }
    }
    out.push('"');
    out
}

/// The request's kind as a fixed class, never the caller's own text: `movie`, `series`, or `other`.
fn kind_class(kind: &str) -> &'static str {
    match kind {
        "movie" => "movie",
        "series" => "series",
        _ => "other",
    }
}

/// The always-on suffix for a decision line about a title: `type=movie|series|other`.
pub fn title_class(kind: &str) -> String {
    format!("type={}", kind_class(kind))
}

/// The opt-in identity suffix for a decision line about a title: its IMDb id and episode coordinates. Built
/// unconditionally and gated by `decision_line`.
pub fn title_identity(imdb: &str, has_ep: bool, season: i64, episode: i64) -> String {
    if !log_identity() {
        return String::new();
    }
    let mut parts = Vec::new();
    if !imdb.is_empty() {
        parts.push(format!("imdb={imdb}"));
    }
    if has_ep {
        parts.push(format!("season={season} episode={episode}"));
    }
    parts.join(" ")
}

/// Go `scrapeClass`/`scrapeIdentity`: the `(class, identity)` suffixes for a per-indexer scrape decision line.
pub fn scrape_suffix(q: &ScrapeQuery) -> (String, String) {
    let class = if q.kind.is_empty() { String::new() } else { title_class(&q.kind) };
    (class, title_identity(&q.imdb, q.has_ep, q.season, q.episode))
}

/// The `(class, identity)` suffixes for a play/probe/cancel/download decision line. The class says only whether
/// the release is for an episode; the identity is the title coordinates and release name. The info hash and the
/// file index are in neither.
pub fn resolve_suffix(rt: &ResolveTarget) -> (String, String) {
    let class = if rt.season.is_some() || rt.episode.is_some() { "kind=episode" } else { "" };
    (class.to_owned(), resolve_identity(rt))
}

/// The opt-in identity of the release a route is acting on. Built unconditionally and gated by `decision_line`.
fn resolve_identity(rt: &ResolveTarget) -> String {
    // Off by default: skip the work (the release name is quoted char by char) rather than build and discard it.
    if !log_identity() {
        return String::new();
    }
    let mut parts = Vec::new();
    if !rt.imdb.is_empty() {
        parts.push(format!("imdb={}", rt.imdb));
    }
    if let Some(season) = rt.season {
        parts.push(format!("season={season}"));
    }
    if let Some(episode) = rt.episode {
        parts.push(format!("episode={episode}"));
    }
    if !rt.title.is_empty() {
        parts.push(format!("release={}", quote_go(&truncate_for_log(&rt.title, MAX_LOGGED_TITLE))));
    }
    parts.join(" ")
}

/// Go `playDecision` is a play/probe/cancel decision line: the shared shape plus the release it names.
pub fn play_decision(
    event: &str,
    outcome: &str,
    reason: &str,
    upstream: &str,
    dur: Duration,
    rid: &str,
    rt: &ResolveTarget,
) -> String {
    let (class, identity) = resolve_suffix(rt);
    decision_line(event, outcome, reason, upstream, dur, rid, (&class, &identity))
}

/// Go `logRankDecision` is the per-list-build decision line: how many candidates were in contention, the top drop
/// reasons and whether the pick was cached, on every build and rate-limited like any other decision line. Under
/// LOG_IDENTITY it also names the title and the chosen release with its runner-ups.
pub(crate) fn log_rank_decision(
    sid: &StreamId,
    candidates: i64,
    ranked: &[RawStream],
    dbg: &RankDebug,
    dur: Duration,
    rid: &str,
) {
    let (outcome, reason) = if ranked.is_empty() {
        ("skipped", format!("no survivors among {candidates} candidates"))
    } else {
        ("served", format!("picked from {candidates} candidates"))
    };
    log_limited(
        "rank-decision",
        &decision_line(
            "rank",
            outcome,
            &reason,
            "",
            dur,
            rid,
            (&rank_class(sid, ranked, dbg), &rank_identity(sid, ranked)),
        ),
    );
}

/// Go `rankRunnerUps`: "the chosen first release and the next 2-3 runner-ups".
const RANK_RUNNER_UPS: usize = 3;

/// The rank decision line's always-on suffix: the kind of title, the top drop reasons (the filter names, with
/// counts) and whether the chosen release was cached.
pub(crate) fn rank_class(sid: &StreamId, ranked: &[RawStream], dbg: &RankDebug) -> String {
    let mut b = title_class(&sid.kind);
    let counts: HashMap<String, i64> = dbg.dropped_by.iter().map(|(k, v)| (k.clone(), *v)).collect();
    let drops = top_drop_reasons(&counts, 3);
    if !drops.is_empty() {
        b.push_str(&format!(" top_drops={drops}"));
    }
    if let Some(top) = ranked.first() {
        b.push_str(if top.cached { " picked_cached=yes" } else { " picked_cached=no" });
    }
    b
}

/// Go `rankIdentity` is the rank decision line's identity suffix: title id and the chosen release with its
/// runner-ups, one unit gated by LOG_IDENTITY like every other identity suffix here.
pub(crate) fn rank_identity(sid: &StreamId, ranked: &[RawStream]) -> String {
    if !log_identity() {
        return String::new();
    }
    let mut b = format!("imdb={} ", sid.imdb);
    if sid.has_ep {
        b.push_str(&format!("season={} episode={} ", sid.season, sid.episode));
    }
    for (i, s) in ranked.iter().enumerate().take(RANK_RUNNER_UPS + 1) {
        let label = if i == 0 { "picked".to_owned() } else { format!("runnerup{i}") };
        b.push_str(&format!("{label}={} ", format_pick(s)));
    }
    if b.ends_with(' ') {
        b.pop();
    }
    b
}

/// Go `topDropReasons`: the `n` largest drop counts as `reason:count`, highest first, ties broken by name.
/// Empty when nothing was dropped.
pub fn top_drop_reasons(counts: &HashMap<String, i64>, n: usize) -> String {
    let mut all: Vec<(&String, i64)> = counts.iter().map(|(k, v)| (k, *v)).collect();
    all.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(b.0)));
    all.truncate(n);
    all.iter().map(|(reason, count)| format!("{reason}:{count}")).collect::<Vec<_>>().join(",")
}

/// Go `formatPick`: one served release for the rank decision line: label, size, cached — the identity of a
/// specific pick, which is why it rides inside the same LOG_IDENTITY gate as the rest. The info hash is not in it.
pub fn format_pick(s: &RawStream) -> String {
    let cached = if s.cached { "cached" } else { "uncached" };
    let size_gb = match s.size_bytes {
        Some(b) => format!("{:.1}", b as f64 / GIB as f64),
        None => "unknown".to_owned(),
    };
    let title = quote_go(&truncate_for_log(&s.title, MAX_LOGGED_TITLE));
    format!("{title}(size_gb={size_gb},{cached})")
}
