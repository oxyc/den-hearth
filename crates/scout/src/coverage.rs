//! Port of `internal/scout/coverage.go`. Work package 4.
//!
//! A stream list's account of itself, carried in the body beside `streams` as `den`.
//!
//! An empty `streams` array means two different things — "every source was asked and none has this" and "we
//! could not ask" — and a client cannot tell them apart from the array. The envelope says which, and on what
//! evidence: `answerKind` names the kind of answer, and `coverage` lists every configured catalogue source with
//! what happened when it was asked. Stremio ignores keys it does not know, so a client that reads only
//! `streams` behaves exactly as before.
//!
//! It rides in the body rather than a header because the body is what gets cached: a held list served later
//! still carries the time it was built and the coverage it was built from.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use serde_json::value::RawValue;

use crate::config::Config;
use crate::scrape::{classify_outcome, DoError, Scraper};

pub(crate) const ANSWER_ENVELOPE_VERSION: i64 = 1;

// answerKind values.
/// answerLive: every catalogue source answered and the list has releases.
pub(crate) const ANSWER_LIVE: &str = "live";
/// answerPartial: the list has releases, but at least one source did not answer, so it may be short.
pub(crate) const ANSWER_PARTIAL: &str = "partial";
/// answerEmpty: every catalogue source answered and nothing survived. The only kind that asserts a negative.
pub(crate) const ANSWER_EMPTY: &str = "empty";
/// answerUnknown: nothing to serve, and the pass cannot say why — a source did not answer, or the build is
/// degraded. Never a statement that the title has no releases.
pub(crate) const ANSWER_UNKNOWN: &str = "unknown";
/// answerStale: no source answered, so an earlier complete list is served in place of a new one.
pub(crate) const ANSWER_STALE: &str = "stale";

// Source outcomes. A source that did not answer is always one of the non-answered kinds, never absent.
pub(crate) const OUTCOME_ANSWERED: &str = "answered";
/// outcomeUnreachable: no answer — a network failure, a 5xx, or a config that could not be minted just now.
pub(crate) const OUTCOME_UNREACHABLE: &str = "unreachable";
/// outcomeRefused: the source, or scout's own transport, declined the request (a 4xx, a paused upstream, a
/// household source pointing at a non-public address).
pub(crate) const OUTCOME_REFUSED: &str = "refused";
/// outcomeTimeout: the source did not answer inside the scrape budget.
pub(crate) const OUTCOME_TIMEOUT: &str = "timeout";
/// outcomeMisconfigured: the source needs a per-install config URL and has none, so it was not asked.
pub(crate) const OUTCOME_MISCONFIGURED: &str = "skipped_misconfigured";
/// outcomeQuarantined: the config names an indexer this build has disabled, so it was not asked.
pub(crate) const OUTCOME_QUARANTINED: &str = "quarantined";

/// sourceOutcomes is every outcome, in the order /metrics renders them.
pub(crate) const SOURCE_OUTCOMES: [&str; 6] = [
    OUTCOME_ANSWERED,
    OUTCOME_UNREACHABLE,
    OUTCOME_REFUSED,
    OUTCOME_TIMEOUT,
    OUTCOME_MISCONFIGURED,
    OUTCOME_QUARANTINED,
];

/// Go `answerEnvelope`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AnswerEnvelope {
    pub v: i64,
    #[serde(rename = "answerKind")]
    pub answer_kind: String,
    /// Degraded is the X-Den-Degraded value the list was served with, "" when none.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub degraded: String,
    #[serde(rename = "generatedAt", with = "rfc3339_nanos")]
    pub generated_at: Option<SystemTime>,
    /// ExpiresAt is when the list stops being fresh. Absent on a list that is not cached at all.
    #[serde(
        rename = "expiresAt",
        default,
        skip_serializing_if = "Option::is_none",
        with = "rfc3339_nanos_present"
    )]
    pub expires_at: Option<SystemTime>,
    pub coverage: Coverage,
}

/// Go `coverage`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Coverage {
    /// Complete: every catalogue source that could be asked answered. A source that can never be asked —
    /// misconfigured or quarantined — does not hold it back, and is listed with that outcome.
    pub complete: bool,
    /// Go marshals a nil slice as `null`; `quarantined_reports` and the scrape always hand a real one.
    pub sources: Vec<SourceReport>,
}

/// Go `sourceReport`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SourceReport {
    pub id: String,
    /// Label tells a household's own sources apart: they share one id, so each is named by its host.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub label: String,
    pub outcome: String,
    /// Items is how many releases the source answered with, before dedupe.
    pub items: i64,
    #[serde(rename = "latencyMs", default, skip_serializing_if = "Option::is_none")]
    pub latency_ms: Option<i64>,
    /// ObservedAt is when the answer, or the failure, arrived. Absent for a source that was not asked, and for
    /// an answer reused from the indexer answer cache, whose age is not recorded.
    #[serde(
        rename = "observedAt",
        default,
        skip_serializing_if = "Option::is_none",
        with = "rfc3339_nanos_present"
    )]
    pub observed_at: Option<SystemTime>,
    /// Cached: the answer came from the indexer answer cache rather than a request made for this list.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub cached: bool,
}

/// decideAnswerKind is the one place a list's kind is judged. An empty list is `empty` only when every source
/// answered and nothing about the build is degraded; any gap makes it `unknown`.
pub(crate) fn decide_answer_kind(streams: i64, complete: bool, degraded: &str) -> &'static str {
    if streams == 0 && (!complete || !degraded.is_empty()) {
        ANSWER_UNKNOWN
    } else if streams == 0 {
        ANSWER_EMPTY
    } else if !complete {
        ANSWER_PARTIAL
    } else {
        ANSWER_LIVE
    }
}

/// excusedFromQuorum reports whether a source's failure to answer leaves the list complete: only when it can
/// never be asked, so waiting on it would make completeness unreachable.
pub(crate) fn excused_from_quorum(outcome: &str) -> bool {
    outcome == OUTCOME_MISCONFIGURED || outcome == OUTCOME_QUARANTINED
}

/// classifyScrape names what one scrape's error means for coverage. `None` is a scrape that answered.
///
/// Go type-switches on `unaskableScraper` and `*httpStatusError`; here the scraper's own failure says what it is
/// (`UnaskableError`, `HttpStatusError`), and `scrape::classify_outcome` reads it back, so the scraper argument
/// is not consulted.
pub(crate) fn classify_scrape(_sc: &dyn Scraper, err: Option<&DoError>) -> &'static str {
    classify_outcome(err.map(|e| &**e as &(dyn std::error::Error + 'static)))
}

/// quarantinedReports lists the indexers a config names that this build has disabled.
pub(crate) fn quarantined_reports(config: &Config) -> Vec<SourceReport> {
    config
        .quarantined
        .iter()
        .map(|id| SourceReport {
            id: id.0.clone(),
            label: String::new(),
            outcome: OUTCOME_QUARANTINED.to_owned(),
            items: 0,
            latency_ms: None,
            observed_at: None,
            cached: false,
        })
        .collect()
}

/// A list body as `stale_body` reads it: `streams` kept exactly as it was written, and the envelope.
#[derive(Deserialize)]
struct HeldIn {
    #[serde(default)]
    streams: Option<Box<RawValue>>,
    #[serde(default)]
    den: Option<AnswerEnvelope>,
}

/// The same two keys, in the order Go's struct writes them.
#[derive(Serialize)]
struct HeldOut {
    streams: Option<Box<RawValue>>,
    den: AnswerEnvelope,
}

/// staleBody relabels a list served past its freshness — inside the stale window while a rebuild runs behind
/// the reply, or in place of a failed build — as `stale`, with the degraded reason the response carries ("" when
/// none). The build time and coverage stay those of the list, which is what makes its age readable. A body
/// written before the envelope existed is returned unchanged, with `None`.
pub(crate) fn stale_body(body: &str, degraded: &str) -> Option<String> {
    let held: HeldIn = serde_json::from_str(body).ok()?;
    let mut den = held.den?;
    den.answer_kind = ANSWER_STALE.to_owned();
    den.degraded = degraded.to_owned();
    Some(crate::handler::to_json(&HeldOut { streams: held.streams, den }))
}

// --- RFC 3339 -------------------------------------------------------------------------------------------------

/// Go's zero `time.Time`, as it marshals.
const ZERO_TIME: &str = "0001-01-01T00:00:00Z";
/// The Unix time of `0001-01-01T00:00:00Z`.
const ZERO_UNIX: i64 = -62_135_596_800;

/// Days since 1970-01-01 of a civil date (proleptic Gregorian).
fn days_from_civil(y: i64, m: u32, d: u32) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = y.div_euclid(400);
    let yoe = y.rem_euclid(400);
    let mp = i64::from(if m > 2 { m - 3 } else { m + 9 });
    let doy = (153 * mp + 2) / 5 + i64::from(d) - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

/// The civil date of a day count since 1970-01-01.
fn civil_from_days(z: i64) -> (i64, u32, u32) {
    let z = z + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let m = (if mp < 10 { mp + 3 } else { mp - 9 }) as u32;
    (if m <= 2 { y + 1 } else { y }, m, d)
}

/// Unix seconds and the nanosecond part of a time, the part never negative.
fn unix_parts(t: SystemTime) -> (i64, u32) {
    match t.duration_since(UNIX_EPOCH) {
        Ok(d) => (d.as_secs() as i64, d.subsec_nanos()),
        Err(e) => {
            let d = e.duration();
            if d.subsec_nanos() == 0 {
                (-(d.as_secs() as i64), 0)
            } else {
                (-(d.as_secs() as i64) - 1, 1_000_000_000 - d.subsec_nanos())
            }
        }
    }
}

/// Go `t.UTC().Format(time.RFC3339)` (no fraction) or, with `nanos`, `time.RFC3339Nano` (the fraction written
/// only when it is not zero, trailing zeros trimmed): what `encoding/json` writes for a `time.Time`.
pub(crate) fn format_rfc3339(t: SystemTime, nanos: bool) -> String {
    let (secs, nsec) = unix_parts(t);
    let (y, mo, d) = civil_from_days(secs.div_euclid(86_400));
    let rem = secs.rem_euclid(86_400);
    let mut out = format!("{y:04}-{mo:02}-{d:02}T{:02}:{:02}:{:02}", rem / 3600, rem % 3600 / 60, rem % 60);
    if nanos && nsec != 0 {
        let frac = format!("{nsec:09}");
        out.push('.');
        out.push_str(frac.trim_end_matches('0'));
    }
    out.push('Z');
    out
}

/// Go `time.Parse(time.RFC3339, v)`: `None` for a value that is not one, `Some(None)` for Go's zero time.
pub(crate) fn parse_time(s: &str) -> Option<Option<SystemTime>> {
    parse_rfc3339(s)
}

/// Reads an RFC 3339 time (`Z` or a numeric offset, with or without a fraction). `Some(None)` is Go's zero time.
fn parse_rfc3339(s: &str) -> Option<Option<SystemTime>> {
    let b = s.as_bytes();
    let num = |from: usize, len: usize| -> Option<i64> {
        let part = s.get(from..from + len)?;
        part.bytes().all(|c| c.is_ascii_digit()).then(|| part.parse().ok()).flatten()
    };
    if b.len() < 20
        || b[4] != b'-'
        || b[7] != b'-'
        || !(b[10] == b'T' || b[10] == b't')
        || b[13] != b':'
        || b[16] != b':'
    {
        return None;
    }
    let (y, mo, d) = (num(0, 4)?, num(5, 2)?, num(8, 2)?);
    let (h, mi, sec) = (num(11, 2)?, num(14, 2)?, num(17, 2)?);
    if !(1..=12).contains(&mo) || !(1..=31).contains(&d) || h > 23 || mi > 59 || sec > 59 {
        return None;
    }
    let mut pos = 19;
    let mut nsec = 0u32;
    if b.get(pos) == Some(&b'.') {
        let start = pos + 1;
        let mut end = start;
        while end < b.len() && b[end].is_ascii_digit() {
            end += 1;
        }
        if end == start {
            return None;
        }
        let digits = &s[start..end.min(start + 9)];
        nsec = format!("{digits:0<9}").parse().ok()?;
        pos = end;
    }
    let offset = match b.get(pos)? {
        b'Z' | b'z' if pos + 1 == b.len() => 0,
        sign @ (b'+' | b'-') if pos + 6 == b.len() && b[pos + 3] == b':' => {
            let (oh, om) = (num(pos + 1, 2)?, num(pos + 4, 2)?);
            let secs = oh * 3600 + om * 60;
            if *sign == b'-' {
                -secs
            } else {
                secs
            }
        }
        _ => return None,
    };
    let unix = days_from_civil(y, mo as u32, d as u32) * 86_400 + h * 3600 + mi * 60 + sec - offset;
    if unix == ZERO_UNIX && nsec == 0 {
        return Some(None);
    }
    let t = if unix >= 0 {
        UNIX_EPOCH + Duration::new(unix as u64, nsec)
    } else {
        UNIX_EPOCH - Duration::new(unix.unsigned_abs(), 0) + Duration::new(0, nsec)
    };
    Some(Some(t))
}

/// A `time.Time` that is always written: the zero time stands in for a missing one.
mod rfc3339_nanos {
    use std::time::SystemTime;

    use serde::{Deserialize, Deserializer, Serializer};

    pub fn serialize<S: Serializer>(t: &Option<SystemTime>, s: S) -> Result<S::Ok, S::Error> {
        match t {
            Some(t) => s.serialize_str(&super::format_rfc3339(*t, true)),
            None => s.serialize_str(super::ZERO_TIME),
        }
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<Option<SystemTime>, D::Error> {
        let Some(text) = Option::<String>::deserialize(d)? else { return Ok(None) };
        super::parse_rfc3339(&text).ok_or_else(|| serde::de::Error::custom("not an RFC 3339 time"))
    }
}

/// A `time.Time` with `omitzero`: written only when set (the caller skips `None`).
mod rfc3339_nanos_present {
    use std::time::SystemTime;

    use serde::{Deserializer, Serializer};

    pub fn serialize<S: Serializer>(t: &Option<SystemTime>, s: S) -> Result<S::Ok, S::Error> {
        match t {
            Some(t) => s.serialize_str(&super::format_rfc3339(*t, true)),
            None => s.serialize_none(),
        }
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<Option<SystemTime>, D::Error> {
        super::rfc3339_nanos::deserialize(d)
    }
}
