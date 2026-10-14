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

use std::time::SystemTime;

use serde::{Deserialize, Serialize};

use crate::config::Config;
use crate::scrape::{DoError, Scraper};

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
    #[serde(rename = "generatedAt")]
    pub generated_at: Option<SystemTime>,
    /// ExpiresAt is when the list stops being fresh. Absent on a list that is not cached at all.
    #[serde(rename = "expiresAt", default, skip_serializing_if = "Option::is_none")]
    pub expires_at: Option<SystemTime>,
    pub coverage: Coverage,
}

/// Go `coverage`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Coverage {
    /// Complete: every catalogue source that could be asked answered. A source that can never be asked —
    /// misconfigured or quarantined — does not hold it back, and is listed with that outcome.
    pub complete: bool,
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
    #[serde(rename = "observedAt", default, skip_serializing_if = "Option::is_none")]
    pub observed_at: Option<SystemTime>,
    /// Cached: the answer came from the indexer answer cache rather than a request made for this list.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub cached: bool,
}

/// decideAnswerKind is the one place a list's kind is judged. An empty list is `empty` only when every source
/// answered and nothing about the build is degraded; any gap makes it `unknown`.
pub(crate) fn decide_answer_kind(streams: i64, complete: bool, degraded: &str) -> &'static str {
    todo!("coverage.go decideAnswerKind")
}

/// excusedFromQuorum reports whether a source's failure to answer leaves the list complete: only when it can
/// never be asked, so waiting on it would make completeness unreachable.
pub(crate) fn excused_from_quorum(outcome: &str) -> bool {
    todo!("coverage.go excusedFromQuorum")
}

/// classifyScrape names what one scrape's error means for coverage. `None` is a scrape that answered.
///
/// Go type-switches on `unaskableScraper` and `*httpStatusError`, both package 5's (scrape.go). Until that
/// port lands the classification is left to it, so this signature takes the scraper and the error as they
/// arrive from the scrape.
pub(crate) fn classify_scrape(sc: &dyn Scraper, err: Option<&DoError>) -> &'static str {
    todo!("coverage.go classifyScrape")
}

/// quarantinedReports lists the indexers a config names that this build has disabled.
pub(crate) fn quarantined_reports(config: &Config) -> Vec<SourceReport> {
    todo!("coverage.go quarantinedReports")
}

/// staleBody relabels a list served past its freshness — inside the stale window while a rebuild runs behind
/// the reply, or in place of a failed build — as `stale`, with the degraded reason the response carries ("" when
/// none). The build time and coverage stay those of the list, which is what makes its age readable. A body
/// written before the envelope existed is returned unchanged, with `None`.
pub(crate) fn stale_body(body: &str, degraded: &str) -> Option<String> {
    todo!("coverage.go staleBody")
}
