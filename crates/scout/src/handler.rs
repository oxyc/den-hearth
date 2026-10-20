//! The scout routes as an HTTP service, served at the service root. Off-device by design: the app never sees a
//! torrent or a debrid token.
//!
//! `Handler::serve` takes an `http::Request` and returns an `http::Response`. A client that disconnects drops the
//! `serve` future, which aborts the task serving it, and a deadline is the explicit `tokio::time::Instant` handed
//! down to whatever takes one. Work that must outlive the request (a stream-list build shared by every viewer of a
//! title, the stale rebuild, the probe fan-out, the availability check) is spawned on its own task.

mod discovery;
mod playback;
mod presentation;
use discovery::managed_candidates;
#[cfg(test)]
pub(crate) use presentation::queued_body;
pub(crate) use presentation::{write_queued, write_queued_body};

use std::borrow::Cow;
use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicI32, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::task::{Context, Poll};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use den_http::{full, Body, X_DEN_DEGRADED};
use den_seal::Rejected;
use http::header::{ALLOW, CACHE_CONTROL, CONTENT_TYPE, ETAG, HOST, RETRY_AFTER, VARY};
use http::{HeaderName, HeaderValue, Method, Request, Response, StatusCode};
use serde::{Deserialize, Serialize};
use serde_json::json;
use tokio::sync::{watch, Semaphore};
use tokio::task::JoinHandle;
use tokio::time::{timeout, timeout_at, Instant as TokioInstant};

use crate::addbudget::{global_add_budget, is_play_reserve, ADD_BUDGET_LIMIT, ADD_BUDGET_WINDOW};
use crate::attributes::stream_attributes;
use crate::availability::verdict_prefix;
use crate::client::{web_ready, ClientPlayable};
use crate::config::{Config, DebridService, SCOPE_AVAILABILITY};
use crate::coverage::{
    decide_answer_kind, quarantined_reports, stale_body, AnswerEnvelope, Coverage, ANSWER_ENVELOPE_VERSION,
};
use crate::decisionlog::{log_rank_decision, play_decision};
use crate::deps::{Deps, ReqwestDoer};
use crate::hostlimit::{new_host_limiter, HostLimiter};
use crate::id::{parse_stream_id, StreamId};
use crate::indexerconfig::Flight;
use crate::label::binge_group;
use crate::linkcheck::LinkVerdict;
use crate::linkmemo::{link_memo_key, LinkMemo, LINK_MEMO_TTL};
use crate::loglimit::{emit, log_limited};
use crate::managed_hls::{ManagedHlsError, ManagedHlsRelease, ManagedHlsRequest, ManagedHlsResult};
use crate::metrics::{cache_persistent_gauge, metrics};
use crate::play::{decode_play_token, encode_play_token, PlayTarget};
use crate::playwarm::play_pending_key;
use crate::probe_fanout::{
    browser_probe_candidates, hydrate_pack_only_sizes, hydrate_probe_facts, select_probe_jobs, ProbeJob,
    PROBE_BUDGET, PROBE_TOP_N, PROBE_TTL,
};
use crate::rank::{
    cap_seeds, rank_streams_debug, real_debrid_blocked, title_tokens, RankDebug, RankFilters, RawStream,
};
use crate::releaselog::ReleaseAnswer;
use crate::reqlog::redact_path;
use crate::scrape::{scrape_covered_until, Doer, ScrapeQuery, Scraper};
use crate::stores::pool::{has_cache_truth, status_outcome_among, StorePool};
use crate::stores::refusal::{as_unavailable, scout_side_reason};
use crate::stores::{
    join_all, CacheTruth, ProviderStatus, ResolveTarget, Store, StoreError, StoreStatus,
    StoreUnavailableError, FETCH_DOWNLOADING, FETCH_QUEUED,
};
use crate::ticket::{TicketError, TicketKeys, TICKET_ROUTE};
use crate::util::{etag_hex, fold_json_keys, key_hash, Shape, LEAF};
use crate::validate::{validate_debrid_token, ValidateResult, MAX_VALIDATE_BODY, VALIDATE_BUDGET};

/// Go `noStore`.
pub const NO_STORE: &str = "no-store";
/// Go `jsonType`.
pub const JSON_TYPE: &str = "application/json";
/// Go `defaultTimeout`.
pub const DEFAULT_TIMEOUT: Duration = Duration::from_secs(8);
/// Go `defaultListTTL`.
pub const DEFAULT_LIST_TTL: Duration = Duration::from_secs(5 * 60);
/// Headroom over the scrape timeout for the cache-check phase of a detached list build.
pub(crate) const LIST_BUILD_SLACK: Duration = Duration::from_secs(20);
/// Hard cap on a /play resolve (addMagnet→select→unrestrict across stores) so a slow debrid account
/// can't pin a task/connection indefinitely.
pub(crate) const RESOLVE_BUDGET: Duration = Duration::from_secs(45);
/// Upper bound on scraped seeds fed into the cache-check fan-out (guards outbound amplification).
pub(crate) const MAX_SEEDS: usize = 500;
/// Go `partialListTTL`: how long a list is kept when an indexer did not answer. Short enough that the missing
/// releases appear soon after that indexer recovers; long enough that a flaky upstream does not put the full
/// scrape and a debrid fan-out on every single request.
pub const PARTIAL_LIST_TTL: Duration = Duration::from_secs(60);
/// How long past its freshness a COMPLETE list may still be served while a rebuild runs behind it.
///
/// The response header has advertised stale-while-revalidate since this route existed, and the server
/// implemented none of it: the request that arrived one second after expiry paid the whole eight-second
/// scrape plus a debrid cache-check fan-out, with a perfectly good list sitting in memory.
///
/// Two minutes, not another full TTL. The window only has to cover the moment of expiry — a rebuild
/// finishes well inside it. It also bounds how wrong a list served as current may be: torrent
/// availability moves, and a list stale by an hour is not a kindness. Past this window the entry is kept
/// only as a last resort for an indexer outage (outageHoldFor), and is served then with a degraded header.
///
/// A CEILING, not the value: staleWindowFor caps it at the configured TTL as well. Left absolute, an
/// operator who set LIST_TTL_SECS=30 got a 30-second freshness followed by a two-minute stale
/// window — an entry spending 80% of its life stale, which is not what "briefly serve the old one
/// while it refreshes" means.
pub(crate) const MAX_STALE_SERVE_WINDOW: Duration = Duration::from_secs(2 * 60);
/// How long past its freshness a device may keep using a COMPLETE list on its own, as stale-if-error. Short,
/// because a device reusing its copy cannot say how old it is. A CEILING: staleIfErrorFor also keeps it
/// inside the life of the play tickets the list carries.
pub(crate) const MAX_STALE_IF_ERROR: Duration = Duration::from_secs(60 * 60);
/// What a held list's tickets must still have left when the server serves it in an outage (stale_list): the
/// viewing it starts, during which the player may ask the URL again. The server's own hold (outageHoldFor)
/// is the ticket life less this and the list's normal reach, so indexers down for a few hours still leave
/// anything opened that day playable — served marked stale, with the time it was built.
pub(crate) const OUTAGE_VIEWING_ROOM: Duration = Duration::from_secs(6 * 60 * 60);
/// Go `scrapeFailThreshold`: after this many consecutive builds where no indexer responded, /health says so.
pub const SCRAPE_FAIL_THRESHOLD: i32 = 3;

/// statusBudget bounds a status read: one upstream question, asked on the client's poll cadence, so it
/// stays far under resolveBudget and a wait answers promptly instead of hanging the poll.
///
/// Held per handler, and settable ONLY so a test can shorten it — eight seconds is not a unit-test wait, and
/// the timeout path is exactly what needs covering. There is no env knob and no Deps field; nothing
/// outside a test changes it. (Go keeps one package-level variable; tests here run in parallel, so each
/// handler carries its own.)
pub(crate) const DEFAULT_STATUS_BUDGET: Duration = Duration::from_secs(8);

/// How long a key is barred from re-booking a background rebuild after one came back degraded. Long
/// enough that an indexer outage does not put a scrape and a debrid fan-out on every request for the rest
/// of the stale window; short enough that recovery is noticed within a minute, which is the same bet
/// partialListTTL makes.
pub(crate) const REBUILD_COOLOFF: Duration = Duration::from_secs(60);

/// How many background rebuilds may be in flight across the whole process.
///
/// bookRebuild dedupes per KEY, which was the fix for one task per stale request — but it leaves one
/// per distinct stale key, and a caller picks the keys. Measured: 400 stale keys hit by a single
/// sequential caller produced 801 goroutines, each holding a finished list body. A household refreshes a
/// handful of titles at once; anything beyond that is not a viewer, and a rebuild skipped now is simply
/// rebuilt by the next request for that title, which is the same outcome as never having booked it.
pub(crate) const MAX_CONCURRENT_REBUILDS: usize = 8;

/// What /health says about that state, and what the log line marking the flip says too.
pub(crate) const INDEXERS_DOWN_DETAIL: &str =
    "No indexer has answered the last few stream-list builds, so lists are coming back empty.";

/// What /health says once a source has missed scrapeFailThreshold builds in a row while others answered.
const SOURCES_MISSING_DETAIL: &str =
    "A source has missed the last few stream-list builds, so lists may be short; /metrics names it.";

/// How long a client is asked to wait after a debrid refused: the refusal memory holds a store back about this
/// long, so asking sooner is answered the same way.
pub(crate) const STORE_REFUSAL_WAIT: Duration = Duration::from_secs(60);
/// After a store could not be asked at all: short, since that is usually one slow answer, not a limit.
pub(crate) const STORE_UNANSWERED_WAIT: Duration = Duration::from_secs(10);

/// The one bucket ?debug=1 is paced from. debugLimiter paces ?debug=1. A diagnostic is run by a person a
/// handful of times in a row, so three then one every ten seconds is invisible to that and puts a ceiling on
/// the one /stream path that has neither the list cache nor a shared singleflight in front of the debrid.
///
/// ONE bucket, under a constant key — not one per cache key, which is how this was first written and was a
/// second unbounded-memory hole in the fix for the first. hostLimiter never evicts (it was built for
/// upstream HOSTS, a small operator-controlled set), so keying it on a cache key derived from the request
/// path let an anonymous caller mint a permanent bucket per distinct blob or episode number. Worse, a
/// fresh bucket starts at full burst, so the pacing never engaged on exactly that traffic.
///
/// KNOWN AND ACCEPTED, the same trade validate.go's limiter writes down: one shared bucket means an
/// anonymous caller can hold it empty and deny the OPERATOR ?debug=1 outright, not merely make two
/// concurrent debug runs contend. Keying per caller would fix that and would reintroduce the
/// never-evicting map this exists to close, which is a worse failure than a diagnostic that says "try
/// again in a moment". Nothing about playback touches this limiter.
const DEBUG_LIMITER_KEY: &str = "debug";

/// Go `etagFor`: the quoted 64-bit ETag of a body.
pub fn etag_for(body: &str) -> String {
    format!("\"{}\"", etag_hex(body))
}

/// staleWindowFor bounds the stale window by the freshness it follows, so shortening the TTL shortens both.
pub(crate) fn stale_window_for(ttl: Duration) -> Duration {
    ttl.min(MAX_STALE_SERVE_WINDOW)
}

/// staleIfErrorFor is how long past freshness a complete list may still be used, bounded so that no use of it
/// holds a lapsed ticket. A list's tickets are minted when it is built, and the list stays fresh for one TTL
/// on the server and one more as max-age on the device; the third TTL is room to press play. Zero when the
/// tickets are too short-lived to allow any.
pub(crate) fn stale_if_error_for(list_ttl: Duration, ticket_ttl: Duration) -> Duration {
    ticket_ttl.saturating_sub(3 * list_ttl).min(MAX_STALE_IF_ERROR)
}

/// outageHoldFor is how long past freshness the server keeps a complete list to serve when no indexer answers:
/// as long as its tickets allow with outageViewingRoom to spare, and never less than a device may keep it.
pub(crate) fn outage_hold_for(list_ttl: Duration, ticket_ttl: Duration) -> Duration {
    stale_if_error_for(list_ttl, ticket_ttl)
        .max(ticket_ttl.saturating_sub(3 * list_ttl).saturating_sub(OUTAGE_VIEWING_ROOM))
}

/// Go `escalatedStatusBudget`: twice the status budget. A function, not a constant: derived once at init it did not
/// track a statusBudget a test had shortened, and the ratio silently became 107x rather than 2x.
pub(crate) fn escalated_status_budget(status_budget: Duration) -> Duration {
    2 * status_budget
}

/// escalatedStatusCtx is the ONE extra status read a /play gets when the first could not answer and the
/// alternative is queueing a torrent that may already be downloading.
///
/// Carved OUT of the resolve budget rather than added to it, and parented to it, because a sibling deadline that
/// outlives it is how an earlier version burned the whole 45 seconds, handed the resolve a dead clock, and made
/// the add impossible. It declines unless twice the budget remains, so the resolve keeps at least as long as the
/// escalation costs.
///
/// TWICE the ordinary budget, and that number has now been wrong in both directions, so it is worth
/// recording why it settled here.
///
/// It was two, then cut to one because a 16-second read on the polled /play route breaks the rule
/// TestHandlePlay_pollReadsUseTheStatusBudget states. Cutting it made the escalation useless: the retry is
/// sliced from the same budget as the read that just failed, so a store persistently slower than its slice
/// gets a bit-for-bit repeat of a read already known to time out. Measured on a listing 1.4x the budget,
/// five runs out of five: at one budget the play 302s and queues a duplicate add; at two it 202s and
/// queues nothing. A guard that cannot succeed is not a guard.
///
/// So the rule is the thing that needed amending, not the number, and the test now states the real one:
/// no status read may take the RESOLVE budget, and exactly one — this one, on the path about to spend an
/// add — may take double.
///
/// Worst-case status time on a /play is FOUR status budgets, 32 seconds — three reads, because this one
/// counts twice: the ordinary poll read (8 s), this escalation (16 s), and the post-failure read below
/// (8 s), which is a fresh budget on a detached clock and so outside the resolve clock entirely.
/// Measured end to end at 53 seconds all in, against a 60-second write timeout.
///
/// `parent` is the resolve deadline; `None` is a parent with no deadline, which declines.
pub(crate) fn escalated_status_ctx(
    parent: Option<TokioInstant>,
    now: TokioInstant,
    status_budget: Duration,
) -> Option<TokioInstant> {
    let deadline = parent?;
    let budget = escalated_status_budget(status_budget);
    // Leave at least as long as the escalation costs, so the add remains possible.
    //
    // "The resolve" is no longer all of what that protects: the preference cache check below takes a
    // statusBudget slice out of the same remainder, so with shipped constants the resolve gets 13s of
    // the 16s this reserves rather than all of it. Still enough for an add, which is what the guard is
    // for, but the arithmetic is worth stating rather than leaving a comment that reads as an exact
    // invariant and is not one.
    if deadline.saturating_duration_since(now) < 2 * budget {
        return None;
    }
    Some((now + budget).min(deadline))
}

// --- the rebuild gate ---------------------------------------------------------------------------------------

/// rebuildLease is one booking. The generation is a fencing token: the booking is a wall-clock LEASE, so a
/// rebuild can outrun its own expiry — its deadline and its budget are the same 28 seconds, and it
/// still has a marshal and a disk write to do after the deadline fires. Without fencing, that late
/// task's release deleted whatever sat at the key by then, which could be a NEWER booking (leaking a
/// concurrent rebuild slot) or a cool-off another attempt had just installed (restoring per-request
/// scrapes during an outage). A release now only acts on the lease it was given.
struct RebuildLease {
    until: Instant,
    gen: u64,
    /// outage — the last build for this key found no indexer answering. Until the lease ends, a request past the
    /// stale window is answered from the held list without scraping again (see lastResort).
    outage: bool,
}

/// Which keys already have a background rebuild booked, and the earliest a fresh one may start.
///
/// A singleflight is the wrong primitive for this and using it alone was a real hole: it collapses the
/// WORK, not the SPAWN, so every stale hit still started a task that then parked behind the leader.
/// Measured at ~3 KB apiece, and the request task returns immediately, so the flood is fire-and-forget over
/// keep-alive — roughly 60k parked rebuilds is enough to push a 230 MiB heap into continuous GC and then an
/// OOM kill, and seeding a cacheable entry to aim at needs no working token.
///
/// The same gate carries the cool-off after a DEGRADED rebuild. Nothing is cached when a build comes
/// back degraded, so without one the entry stays stale for the rest of its life and every single
/// request re-books a full scrape plus a debrid fan-out — precisely the harm the partial-list caching
/// note further down says it fixed, reappearing on the stale path.
#[derive(Default)]
pub(crate) struct RebuildGate {
    rebuilds: HashMap<String, RebuildLease>,
    gen: u64,
    /// In-flight background rebuilds, against MAX_CONCURRENT_REBUILDS. Counted rather than derived from
    /// the map's length, because that map also holds cool-off entries with no task behind them.
    live: usize,
}

impl RebuildGate {
    /// markOutage holds a key for a cool-off after a build in which no indexer answered, so the requests behind it
    /// are answered from the held list instead of each paying for a scrape that will fail the same way.
    pub(crate) fn mark_outage(&mut self, key: &str, now: Instant, until: Instant) {
        self.rebuilds.retain(|_, l| now < l.until);
        self.gen += 1;
        self.rebuilds.insert(key.to_owned(), RebuildLease { until, gen: self.gen, outage: true });
    }

    /// inOutage reports whether a recent build for this key found no indexer answering.
    pub(crate) fn in_outage(&self, key: &str, now: Instant) -> bool {
        self.rebuilds.get(key).is_some_and(|l| l.outage && now < l.until)
    }

    /// bookRebuild reserves the right to rebuild this key in the background, returning the lease's generation.
    /// ok=false means someone else already holds it, or a degraded attempt is still cooling off.
    pub(crate) fn book_rebuild(&mut self, key: &str, now: Instant, until: Instant) -> (u64, bool) {
        if self.rebuilds.get(key).is_some_and(|l| now < l.until) {
            return (0, false);
        }
        // A global ceiling as well as the per-key one. The entry stays stale and the next request for this
        // title books it, which is the same outcome as never having asked.
        if self.live >= MAX_CONCURRENT_REBUILDS {
            return (0, false);
        }
        // Swept here rather than on a timer: the map only ever holds keys with a live booking or a recent
        // cool-off, so it stays small, and pruning on the rare stale hit costs nothing worth measuring.
        self.rebuilds.retain(|_, l| now < l.until);
        self.gen += 1;
        self.rebuilds.insert(key.to_owned(), RebuildLease { until, gen: self.gen, outage: false });
        self.live += 1;
        (self.gen, true)
    }

    /// releaseRebuild ends a booking early (a healthy rebuild) or extends it into a cool-off (a degraded one).
    /// A lease that has already been superseded releases nothing — see RebuildLease.
    pub(crate) fn release_rebuild(&mut self, key: &str, gen: u64, cooloff_until: Option<Instant>) {
        // The slot goes back FIRST, and unconditionally — before the fencing check below can return early.
        // This is called exactly once per granted booking, so the count stays right even when the lease is
        // already gone: a rebuild that overruns its lease has it swept by some later booking, and decrementing
        // only on a lease we still own leaked a slot every time that happened. Eight of those and no title
        // ever refreshes in the background again.
        //
        // A cool-off is also why this cannot be derived from the map: it keeps the KEY reserved with no
        // task behind it.
        self.live = self.live.saturating_sub(1);
        if self.rebuilds.get(key).is_none_or(|l| l.gen != gen) {
            return;
        }
        match cooloff_until {
            None => {
                self.rebuilds.remove(key);
            }
            Some(until) => {
                self.rebuilds.insert(key.to_owned(), RebuildLease { until, gen, outage: false });
            }
        }
    }

    /// Whether a lease is held for the key at all, expired or not.
    #[cfg(test)]
    pub(crate) fn holds(&self, key: &str) -> bool {
        self.rebuilds.contains_key(key)
    }

    /// How many background rebuilds are in flight.
    #[cfg(test)]
    pub(crate) fn live(&self) -> usize {
        self.live
    }
}

// --- request and response -----------------------------------------------------------------------------------

/// What every route answers with.
pub(crate) type Resp = Response<Body>;

/// A request header's first value, "" when it is absent or not visible ASCII.
pub(crate) fn header<'a>(req: &'a Request<Bytes>, name: &str) -> &'a str {
    req.headers().get(name).and_then(|v| v.to_str().ok()).unwrap_or("")
}

/// Whether the query's first `key` is `value` (`?probe=1`, `?op=reannounce`).
pub(crate) fn query_is(req: &Request<Bytes>, key: &str, value: &str) -> bool {
    req.uri().query().is_some_and(|q| {
        url::form_urlencoded::parse(q.as_bytes()).find(|(k, _)| k == key).is_some_and(|(_, v)| v == value)
    })
}

/// The caller's `X-Request-Id` as a log line may carry it; "" when there is none.
pub(crate) fn rid(req: &Request<Bytes>) -> String {
    den_http::log::request_id(req.headers()).unwrap_or_default()
}

/// Sets a header whose value is built at run time. A value that is not a valid header (a link with a control
/// character in it) is left out rather than answered with a 500.
pub(crate) fn set_header(resp: &mut Resp, name: HeaderName, value: &str) {
    if let Ok(v) = HeaderValue::from_str(value) {
        resp.headers_mut().insert(name, v);
    }
}

/// A JSON body, never stored by a cache.
pub(crate) fn json_response(status: StatusCode, body: &(impl Serialize + ?Sized)) -> Resp {
    Response::builder()
        .status(status)
        .header(CONTENT_TYPE, JSON_TYPE)
        .header(CACHE_CONTROL, NO_STORE)
        .body(full(to_json(body)))
        .expect("a static response")
}

/// The refusal every route writes: `{"error": <code>}`, never stored.
pub(crate) fn error(status: StatusCode, code: &str) -> Resp {
    let mut resp = json_response(status, &json!({ "error": code }));
    resp.extensions_mut().insert(ReleaseAnswer::new(code, ""));
    resp
}

/// A refusal of the method, naming the ones the route takes.
pub(crate) fn method_not_allowed(allow: &'static str) -> Resp {
    let mut resp = error(StatusCode::METHOD_NOT_ALLOWED, "method_not_allowed");
    resp.headers_mut().insert(ALLOW, HeaderValue::from_static(allow));
    resp
}

/// The 503 an install-config request gets while CONFIG_KEY is malformed. Retry-After is a hint to clients, not a
/// promise: only an operator fixing the variable and restarting clears it.
fn config_key_invalid() -> Resp {
    let mut resp = error(StatusCode::SERVICE_UNAVAILABLE, "config_key_invalid");
    resp.headers_mut().insert(RETRY_AFTER, HeaderValue::from_static("60"));
    resp
}

/// A body that may be cached and revalidated: its ETag (`etag_for`, or `etag` when the caller already has it) is
/// what a conditional GET is answered against, in `Handler::serve`.
fn cacheable(body: impl Into<Bytes>, etag: &str, content_type: &'static str, cache_control: &str) -> Resp {
    let body = body.into();
    let etag = if etag.is_empty() {
        Cow::Owned(etag_for(std::str::from_utf8(&body).unwrap_or_default()))
    } else {
        Cow::Borrowed(etag)
    };
    let mut resp = Response::new(full(body));
    resp.headers_mut().insert(CONTENT_TYPE, HeaderValue::from_static(content_type));
    set_header(&mut resp, CACHE_CONTROL, cache_control);
    set_header(&mut resp, ETAG, &etag);
    resp
}

/// The body a cached list entry serves. A body taken as it is from the entry is shared with the cache rather than
/// copied out of it: on a warm hit, the request that pays nothing else should not pay for a copy of the list.
fn entry_body(entry: &Arc<str>, body: Cow<'_, str>) -> Bytes {
    struct Shared(Arc<str>);
    impl AsRef<[u8]> for Shared {
        fn as_ref(&self) -> &[u8] {
            self.0.as_bytes()
        }
    }
    match body {
        Cow::Borrowed(part) => {
            // `part` is a slice of `entry` (split_cached), so its offset in the entry is its address less the entry's.
            // A borrowed body from anywhere else is copied rather than sliced out of the wrong allocation.
            let base = entry.as_ptr() as usize;
            match (part.as_ptr() as usize).checked_sub(base) {
                Some(start) if start + part.len() <= entry.len() => {
                    Bytes::from_owner(Shared(Arc::clone(entry))).slice(start..start + part.len())
                }
                _ => Bytes::copy_from_slice(part.as_bytes()),
            }
        }
        Cow::Owned(relabelled) => Bytes::from(relabelled),
    }
}

/// A value as JSON text. Every type serialized here has string keys, so this cannot fail; "null" stands in if it
/// ever does.
pub(crate) fn to_json<T: Serialize + ?Sized>(v: &T) -> String {
    serde_json::to_string(v).unwrap_or_else(|_| "null".to_owned())
}

/// The 202's `progress`: a whole value without a fraction (`0`, `1`), as clients have always been sent it, and a
/// fraction as it is.
fn progress_number(f: f64) -> serde_json::Value {
    if f.fract() == 0.0 && f.abs() < 1e15 {
        return serde_json::Value::from(f as i64);
    }
    serde_json::Number::from_f64(f).map_or(serde_json::Value::Null, serde_json::Value::Number)
}

/// A duration as the log lines and the startup warning have always printed one (`10m0s`, `1.5s`, `250ms`, Go's
/// `time.Duration` form): operators grep for it, and the startup warning's test pins it.
pub(crate) fn go_duration(d: Duration) -> String {
    fn frac(v: u128, unit: u128) -> String {
        let (whole, rem) = (v / unit, v % unit);
        if rem == 0 {
            return whole.to_string();
        }
        let digits = unit.ilog10() as usize;
        let tail = format!("{rem:0digits$}");
        format!("{whole}.{}", tail.trim_end_matches('0'))
    }
    let ns = d.as_nanos();
    if ns == 0 {
        return "0s".to_owned();
    }
    if ns < 1_000 {
        return format!("{ns}ns");
    }
    if ns < 1_000_000 {
        return format!("{}µs", frac(ns, 1_000));
    }
    if ns < 1_000_000_000 {
        return format!("{}ms", frac(ns, 1_000_000));
    }
    let total_secs = ns / 1_000_000_000;
    let (h, m) = (total_secs / 3600, total_secs % 3600 / 60);
    let s = frac(ns % 60_000_000_000, 1_000_000_000);
    if h > 0 {
        format!("{h}h{m}m{s}s")
    } else if m > 0 {
        format!("{m}m{s}s")
    } else {
        format!("{s}s")
    }
}

/// Seconds since the epoch, Go's `time.Now().Unix()`.
pub(crate) fn now_unix() -> i64 {
    SystemTime::now().duration_since(UNIX_EPOCH).map_or(0, |d| d.as_secs() as i64)
}

pub(crate) fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(PoisonError::into_inner)
}

/// A compare that takes as long whatever the inputs share, as `subtle.ConstantTimeCompare`.
fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    use subtle::ConstantTimeEq;
    a.ct_eq(b).into()
}

/// A path's segments, empty ones dropped: `//<config>//stream` is `[<config>, stream]`.
pub(crate) fn split_path(p: &str) -> Vec<&str> {
    p.split('/').filter(|s| !s.is_empty()).collect()
}

fn at<'a>(parts: &[&'a str], i: usize) -> &'a str {
    parts.get(i).copied().unwrap_or("")
}

// --- the cached-entry format --------------------------------------------------------------------------------

/// A cached list is "<complete>[|<fresh-until-unix>]\x00<etag>\x00<body>". Completeness is stored with the
/// entry because the cache-hit branch has to answer the same question the build did, and could not
/// otherwise know; the freshness stamp is stored beside it because the entry now OUTLIVES its freshness
/// (see staleWindowFor) and only the entry itself knows when it stopped being current.
///
/// The stamp is a suffix on the existing first field rather than a fourth field, so that an entry written
/// by an older build — a disk tier surviving a redeploy is exactly when that happens — still parses here,
/// and so that the hot path stays two `find` calls and no allocation.
pub(crate) fn split_cached(v: &str) -> (bool, i64, &str, &str) {
    let Some(first) = v.find('\0') else {
        // No separator at all: completeness unknown, so the same conservative answer as the legacy
        // one-separator case below. Claiming completeness here while denying it three lines down would be
        // two answers to one question.
        return (false, 0, "", v);
    };
    let rest = &v[first + 1..];
    let Some(second) = rest.find('\0') else {
        // An entry written before completeness was recorded. Treat it as INCOMPLETE: a redeploy is
        // exactly when short lists are in flight, and claiming completeness for one would serve it with a
        // five-minute max-age and a day of stale-if-error — the harm this format exists to prevent. The
        // cost of being wrong the other way is one extra scrape within the minute.
        return (false, 0, &v[..first], rest);
    };
    let mut flag = &v[..first];
    // A stamp-less entry predates this format. It was written with its freshness AS its expiry, so the
    // cache holding it at all is proof it is still fresh — freshUntil 0 says exactly that.
    let mut fresh_until = 0;
    if let Some(bar) = flag.find('|') {
        fresh_until = flag[bar + 1..].parse().unwrap_or(0);
        flag = &flag[..bar];
    }
    (flag == "1", fresh_until, &rest[..second], &rest[second + 1..])
}

pub(crate) fn join_cached(complete: bool, fresh_until: i64, etag: &str, body: &str) -> String {
    let mut flag = if complete { "1".to_owned() } else { "0".to_owned() };
    if fresh_until > 0 {
        flag.push('|');
        flag.push_str(&fresh_until.to_string());
    }
    format!("{flag}\0{etag}\0{body}")
}

fn cached_list_nonempty(body: &str) -> bool {
    #[derive(Deserialize)]
    struct Answer {
        #[serde(default)]
        streams: Option<Vec<serde::de::IgnoredAny>>,
    }
    serde_json::from_str::<Answer>(body).is_ok_and(|a| a.streams.is_some_and(|s| !s.is_empty()))
}

// --- stream list shapes -------------------------------------------------------------------------------------

#[derive(Serialize)]
struct StreamOut {
    name: String,
    title: String,
    url: String,
    attributes: crate::attributes::StreamAttributes,
    #[serde(rename = "behaviorHints")]
    behavior_hints: StreamHints,
}

#[derive(Serialize)]
struct StreamHints {
    #[serde(rename = "bingeGroup")]
    binge_group: String,
    #[serde(rename = "notWebReady")]
    not_web_ready: bool,
    /// The release name, which is how a client recognises this release across resolves. A ticket URL is
    /// minted fresh on every list, so without it Den's dead-release marks and resume-to-the-same-source
    /// (both keyed by filename, then by URL) never matched a second time.
    #[serde(skip_serializing_if = "String::is_empty")]
    filename: String,
}

#[derive(Serialize)]
struct StreamsResponse {
    streams: Vec<StreamOut>,
    /// Present only for ?debug=1. Omitted when absent, so a normal response is byte-identical to what it was
    /// before this existed — which matters, because the ETag is a hash of it.
    #[serde(skip_serializing_if = "Option::is_none")]
    debug: Option<RankDebug>,
    /// The list's account of itself (coverage.go). Stremio ignores it; `streams` is unchanged by it.
    den: AnswerEnvelope,
}

fn to_stremio_stream(s: &RawStream, sid: &StreamId, play_url: &dyn Fn(&PlayTarget) -> String) -> StreamOut {
    let mut target = PlayTarget {
        info_hash: s.info_hash.clone(),
        file_idx: s.file_idx,
        season: season_ptr(sid),
        episode: episode_ptr(sid),
        // Diagnostic only (play.go) — rides in the ticket so a later /play or /p/ decision line can name
        // the release instead of keying on shortHash(InfoHash) alone.
        title: s.title.clone(),
        imdb: sid.imdb.clone(),
        ..Default::default()
    };
    if let Some(n) = s.size_bytes.filter(|n| *n > 0) {
        target.release_size = n;
    }
    let url = play_url(&target);
    let attrs = stream_attributes(s.clone());
    // Counted here rather than at the scrape: this is the one place every stream a client is actually
    // offered passes through, which is the population oxyc/den#26 estimates and nothing measures.
    metrics().release(&attrs);
    StreamOut {
        name: "Den Scout".to_owned(),
        title: s.title.clone(), // raw release name
        behavior_hints: StreamHints {
            binge_group: binge_group(&str::to_lowercase(&s.title), &s.title),
            not_web_ready: !web_ready(s, &url),
            filename: s.title.clone(),
        },
        url,
        attributes: attrs,
    }
}

fn season_ptr(sid: &StreamId) -> Option<i64> {
    sid.has_ep.then_some(sid.season)
}

fn episode_ptr(sid: &StreamId) -> Option<i64> {
    sid.has_ep.then_some(sid.episode)
}

fn stream_cache_id(sid: &StreamId) -> String {
    if sid.has_ep {
        format!("{}:{}:{}", sid.imdb, sid.season, sid.episode)
    } else {
        sid.imdb.clone()
    }
}

/// Go `rdOnly`: every configured account is Real-Debrid.
pub(crate) fn rd_only(config: &Config) -> bool {
    !config.debrid.is_empty() && config.debrid.iter().all(|d| d.service == DebridService::RealDebrid)
}

/// The header a browser's report of what it plays arrives in.
pub(crate) const PLAYABLE_HEADER: &str = "X-Den-Playable";
/// The report is about 250 bytes; anything much longer is not one.
const MAX_PLAYABLE_HEADER: usize = 2048;

/// Go `clientPlayable` reads the report off a request, and a key for the list ranked by it. None and "" when there
/// is none, or it doesn't parse: a list ranked for the TV is a worse answer for a browser, not a wrong one.
/// The key is taken from the report re-encoded, so browsers that report the same thing share one list however
/// their JSON was spelled.
pub(crate) fn client_playable(req: &Request<Bytes>) -> (Option<ClientPlayable>, String) {
    let raw = header(req, PLAYABLE_HEADER);
    if raw.is_empty() {
        return (None, String::new());
    }
    static DOLBY: Shape = Shape::Object(&[("p5", &LEAF), ("p8", &LEAF)]);
    static SHAPE: Shape = Shape::Object(&[
        ("h264", &LEAF),
        ("h264High10", &LEAF),
        ("hevcMain", &LEAF),
        ("hevcMain10", &LEAF),
        ("hevcHighTier", &LEAF),
        ("hdr", &LEAF),
        ("eac3", &LEAF),
        ("aacMultichannel", &LEAF),
        ("dolbyVision", &DOLBY),
        ("av1", &LEAF),
        ("av1Main10", &LEAF),
        ("av1Hdr", &LEAF),
        ("flac", &LEAF),
        ("aac71", &LEAF),
        ("vp9", &LEAF),
        ("vp9Profile2", &LEAF),
    ]);
    let parsed = (raw.len() <= MAX_PLAYABLE_HEADER)
        .then(|| serde_json::from_str::<serde_json::Value>(raw).ok())
        .flatten()
        .and_then(|mut v| {
            fold_json_keys(&mut v, &SHAPE);
            serde_json::from_value::<ClientPlayable>(v).ok()
        });
    let Some(p) = parsed else {
        log_limited(
            "playable-header",
            &format!("stream: an {PLAYABLE_HEADER} header that does not parse; ranking for the TV"),
        );
        return (None, String::new());
    };
    let canonical = to_json(&p);
    (Some(p), key_hash(&canonical))
}

// --- the handler --------------------------------------------------------------------------------------------

/// buildResult carries the singleflight build's body plus a degraded reason ("" when healthy).
#[derive(Clone)]
pub(crate) struct BuildResult {
    value: String,
    degraded: String,
    /// complete — every askable indexer answered. A partial list is real and worth caching, just not for
    /// as long, and not with a client freshness window longer than the server's own.
    complete: bool,
    /// The build's phases as Server-Timing entries — scrape, cache-check, and probe when it ran.
    timing: String,
}

/// Outcome of consulting the list-build singleflight. Busy is deliberately distinct from a failed build: no
/// flight was inserted, no task was spawned, and no cache entry was written, so a retry may be admitted later.
pub(crate) enum SharedBuild {
    Ready(Arc<BuildResult>),
    Busy,
    Panicked,
}

/// rankedList is a title's releases after the scrape, the debrid cache check and ranking: what a stream list
/// serves, before any probe or serialisation — and all the availability route needs.
pub(crate) struct RankedList {
    pub(crate) ranked: Vec<RawStream>,
    pub(crate) truth: CacheTruth,
    pub(crate) client: Option<ClientPlayable>,
    /// degraded — why the list cannot be trusted, "" when it can (buildResult's vocabulary).
    pub(crate) degraded: String,
    pub(crate) complete: bool,
    pub(crate) coverage: Coverage,
    pub(crate) timing: String,
}

/// Where a stream list lives: the config segment and title the request named, the origin its play URLs name, and
/// the cache key all of that gives.
#[derive(Clone, Copy)]
struct ListKey<'a> {
    config_blob: &'a str,
    sid: &'a StreamId,
    origin: &'a str,
    cache_key: &'a str,
}

/// Everything one stream-list build needs, owned, so the build can outlive the request that started it.
struct ListJob {
    config: Config,
    config_blob: String,
    sid: StreamId,
    origin: String,
    cache_key: String,
    client: Option<ClientPlayable>,
    rid: String,
}

impl ListJob {
    fn new(config: Config, key: ListKey<'_>, client: Option<ClientPlayable>, rid: String) -> ListJob {
        ListJob {
            config,
            config_blob: key.config_blob.to_owned(),
            sid: key.sid.clone(),
            origin: key.origin.to_owned(),
            cache_key: key.cache_key.to_owned(),
            client,
            rid,
        }
    }
}

/// A cached list's entry, taken apart: when it stopped being fresh, and the validator and body it carries.
#[derive(Clone, Copy)]
struct Held<'a> {
    fresh_until: i64,
    etag: &'a str,
    body: &'a str,
}

/// What differs between the ways a list is ranked: for which browser, whether the debrid is asked, and which
/// request id the decision line carries.
pub(crate) struct RankOpts<'a> {
    pub(crate) client: Option<&'a ClientPlayable>,
    pub(crate) check_cache: bool,
    pub(crate) rid: &'a str,
}

/// Go `handler`: the HTTP service the scout routes are served from. Built by `new_handler`.
pub struct Handler {
    inner: Arc<Inner>,
}

/// The state of a handler, shared with the tasks that outlive a request.
pub(crate) struct Inner {
    pub(crate) deps: Deps,
    /// Go `sf`: stream-list builds in flight, by key. A build runs on its own task, so a viewer who goes away
    /// does not take the others down with it.
    flights: Mutex<HashMap<String, watch::Receiver<Option<Arc<BuildResult>>>>>,
    /// The probe fan-out's own singleflight (Go shares `sf` and prefixes the key with "probe:").
    probe_flight: Flight<()>,
    rebuilds: Mutex<RebuildGate>,
    /// Consecutive fully-degraded builds (every indexer failed). Surfaced on /health so a scrape outage
    /// — which otherwise looks like empty stream lists — is visible to an uptime monitor.
    scrape_fails: AtomicI32,
    /// Consecutive builds a source missed (coverage incomplete), total failures included. Surfaced on /health
    /// as a state, never a count: a count that rises per build is a timeline of when the household browses,
    /// which is why the build counters live behind /metrics' token.
    partial_builds: AtomicI32,
    /// Availability checks running behind replies, by verdict key — never more than maxAvailabilityChecks,
    /// so the set holds only live checks.
    pub(crate) avail_in_flight: Mutex<HashSet<String>>,
    /// Playback links minted in the last few minutes, so /play can serve a repeat without asking the
    /// debrid again. Filled by /play and by the probe fan-out, which mints one per probed release anyway.
    pub(crate) links: LinkMemo,
    /// The play-ticket keys (ticket.go). None when no CONFIG_KEY is set, which leaves stream lists on the
    /// legacy /<config>/play URLs.
    tickets: Option<TicketKeys>,
    /// Set once a legacy play URL has been served while tickets are on, so that is logged once per process.
    legacy_play_noted: AtomicBool,
    // The three stream-list Cache-Control headers. Every input is fixed once ListTTL is known, so
    // formatting them per request was three Sprintf and four allocations on a path whose own comments
    // insist a warm hit pays nothing — measured at ~5% of the warm-hit cost, for a constant.
    list_cache: String,
    partial_list_cache: String,
    stale_list_cache: String,
    /// outageHoldFor(ListTTL, PlayTicketTTL): how long a complete list is kept for the stale_list fallback.
    outage_hold: Duration,
    status_budget_ms: AtomicU64,
    debug_limiter: Mutex<Arc<HostLimiter>>,
    validate_limiter: Mutex<Arc<HostLimiter>>,
}

/// Go `NewHandler`: builds the scout HTTP handler.
pub fn new_handler(deps: Deps) -> Handler {
    Handler { inner: Arc::new(Inner::new(deps)) }
}

/// The task serving a request, aborted when the request goes away (the caller drops the `serve` future).
struct AbortOnDrop<T>(JoinHandle<T>);

impl<T> Future for AbortOnDrop<T> {
    type Output = Result<T, tokio::task::JoinError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        Pin::new(&mut self.get_mut().0).poll(cx)
    }
}

impl<T> Drop for AbortOnDrop<T> {
    fn drop(&mut self) {
        self.0.abort();
    }
}

impl Handler {
    /// The validated full legacy config as a combined-addon's member. Credentials never leave this process: the
    /// migration endpoint immediately seals this value under Hearth's key. A scoped availability config is not an
    /// install and cannot be promoted.
    pub fn migration_member(&self, blob: &str) -> Option<serde_json::Value> {
        let config = self.inner.open_config(blob)?;
        if !config.scope.is_empty() || self.inner.deps.config_slice.is_some() {
            return None;
        }
        let opened = den_seal::open_segment(self.inner.deps.seal_keyring.as_deref(), blob).ok()?;
        let mut value: serde_json::Value = serde_json::from_slice(&opened.json).ok()?;
        let member = value.as_object_mut()?;
        member.remove("iid");
        member.remove("ep");
        Some(value)
    }

    /// Resolve one title to TorBox managed HLS without exposing a Scout route or provider identifiers.
    /// The config is opened with this handler's own combined-addon slice, keyring and revocation policy.
    pub async fn managed_hls(
        &self,
        config_blob: &str,
        request: &ManagedHlsRequest,
    ) -> Result<ManagedHlsResult, ManagedHlsError> {
        let config = self.inner.open_config(config_blob).ok_or(ManagedHlsError::Refused)?;
        if !config.scope.is_empty() {
            return Err(ManagedHlsError::Refused);
        }
        crate::runtime::scope(self.inner.deps.runtime.clone(), self.inner.managed_hls(&config, request)).await
    }

    /// Serves one request. The request body is already read (hearth bounds it); the response body is whole.
    ///
    /// A cacheable answer carries its ETag, and a matching `If-None-Match` (weak or strong) collapses it to a 304
    /// here, after the route, the way every den addon answers a conditional GET. A HEAD gets every header a GET
    /// would and no body.
    pub async fn serve<B: Into<Bytes>>(&self, req: Request<B>) -> Resp {
        let started = Instant::now();
        let req = req.map(Into::into);
        let method = req.method().clone();
        // Decided here, once: with the request log off nothing below is paid.
        let log_line = self.inner.deps.log_requests.then(|| {
            let path = redact_path(&den_http::percent_decode_path(req.uri().path()));
            (path, den_http::log::request_id(req.headers()))
        });
        let inner = self.inner.clone();
        let runtime = inner.deps.runtime.clone();
        // Convert any unforeseen panic into a clean 500 instead of a dropped connection. The request runs on its
        // own task, which is what lets a panic in it be seen here at all. The task hands the request back for the
        // conditional-GET headers.
        let served = AbortOnDrop(tokio::spawn(crate::runtime::scope(runtime, async move {
            let resp = inner.route(&req).await;
            (req, resp)
        })))
        .await;
        let mut resp = match served {
            Ok((req, resp)) => den_http::apply_conditional(&method, req.headers(), resp),
            Err(_) => den_http::apply_conditional(
                &method,
                &http::HeaderMap::new(),
                error(StatusCode::INTERNAL_SERVER_ERROR, "internal"),
            ),
        };
        add_cors(&mut resp);
        if let Some((path, rid)) = log_line {
            let ms = started.elapsed().as_millis();
            emit(&den_http::log::request_line(
                method.as_str(),
                &path,
                resp.status().as_u16(),
                ms,
                rid.as_deref(),
            ));
        }
        resp
    }
}

/// What the tests reach the handler's internals through.
#[cfg(test)]
impl Handler {
    pub(crate) fn install_test_budget(&self, budget: Arc<crate::addbudget::AddBudget>) {
        self.inner.deps.runtime.replace_add_budget(budget);
    }

    pub(crate) fn test_add_budget(&self) -> Arc<crate::addbudget::AddBudget> {
        self.inner.deps.runtime.add_budget()
    }

    /// Shortens the status budget, for a test (see `DEFAULT_STATUS_BUDGET`).
    pub(crate) fn set_status_budget(&self, d: Duration) {
        self.inner.status_budget_ms.store(d.as_millis() as u64, Ordering::Relaxed);
    }

    /// Replaces the ?debug=1 pacing, for a test.
    pub(crate) fn set_debug_limiter(&self, every: Duration, burst: i64) {
        *lock(&self.inner.debug_limiter) = Arc::new(new_host_limiter(every, burst));
    }

    /// Replaces the /validate pacing, for a test.
    pub(crate) fn set_validate_limiter(&self, limiter: HostLimiter) {
        *lock(&self.inner.validate_limiter) = Arc::new(limiter);
    }

    /// The handler's link memo, for a test to read.
    pub(crate) fn links(&self) -> &LinkMemo {
        &self.inner.links
    }

    /// Go `admitInstall`.
    pub(crate) fn admit_install(&self, config: &Config) -> bool {
        self.inner.admit_install(config)
    }

    /// Go `(*handler).verifyLink`.
    pub(crate) async fn verify_link(&self, pool: &StorePool, rt: &ResolveTarget, link: &str) -> LinkVerdict {
        self.inner.verify_link(pool, rt, link).await
    }

    /// Go `(*handler).handleProbe`, called directly with the pool a test builds, on the status budget the route gives
    /// it.
    pub(crate) async fn handle_probe(
        &self,
        config: &Config,
        pool: &StorePool,
        rt: &ResolveTarget,
    ) -> Response<Vec<u8>> {
        use http_body_util::BodyExt;
        let deadline = Inner::deadline_in(self.inner.status_budget());
        let resp = self.inner.handle_probe(config, pool, &rt.info_hash, rt, "", deadline).await;
        let (parts, body) = resp.into_parts();
        let body = body.collect().await.map(|b| b.to_bytes().to_vec()).unwrap_or_default();
        Response::from_parts(parts, body)
    }

    /// Go `hydrateProbeFacts`.
    pub(crate) fn hydrate_probe_facts(&self, streams: &mut [RawStream], sid: Option<&StreamId>) {
        self.inner.hydrate_probe_facts(streams, sid);
    }

    /// Go `hydrateKnownSizes`.
    pub(crate) fn hydrate_known_sizes(
        &self,
        config: &Config,
        streams: &mut [RawStream],
        sid: Option<&StreamId>,
    ) {
        self.inner.hydrate_known_sizes(config, streams, sid);
    }

    /// Go `probeTop`.
    pub(crate) fn probe_top(
        &self,
        config: &Config,
        streams: &mut [RawStream],
        sid: Option<&StreamId>,
        truth: &CacheTruth,
    ) -> bool {
        self.inner.hydrate_probe_facts(streams, sid);
        self.inner.probe_top_for_client(config, streams, sid, truth, None)
    }

    /// Go `probeBehind`.
    pub(crate) fn probe_behind(&self, config: &Config, jobs: Vec<ProbeJob>) {
        self.inner.probe_behind(config, jobs);
    }
}

/// The CORS headers every response carries, errors included.
///
/// Every response is readable cross-origin: browser-based Stremio clients fetch the manifest and stream lists
/// from another origin, and an error body a browser hides is an error nobody can act on. Nothing here rides on
/// ambient credentials — the config and its token are in the path — so `*` grants no access a plain curl does
/// not already have.
///
/// The debug headers readable too: a cross-origin fetch sees only the CORS-safelisted headers unless
/// Expose-Headers names more, and Resource Timing hides Server-Timing without Timing-Allow-Origin.
/// Retry-After and ETag too, or a browser client can neither wait as asked nor revalidate.
fn add_cors(resp: &mut Resp) {
    den_http::cors_stamp(
        resp.headers_mut(),
        "Server-Timing, X-Den-Degraded, Retry-After, ETag, RateLimit, RateLimit-Policy",
    );
}

/// The CORS preflight, answered on every path. Scout takes POST (/validate, /availability) and DELETE (cancel)
/// as well as reads, so it names those where den-http's read-only preflight does not.
fn preflight() -> Resp {
    let mut resp = den_http::preflight();
    resp.headers_mut().insert(
        http::header::ACCESS_CONTROL_ALLOW_METHODS,
        HeaderValue::from_static("GET, HEAD, POST, DELETE, OPTIONS"),
    );
    resp
}

/// A background task whose panic is counted and said, not allowed to take the process down. Go's
/// `recoverBackground`: work moved off the request task so the list would not wait for it left the only recover
/// in the service behind, and the probe fan-out is the sharpest case — the one code path in the addon where
/// bytes chosen by a remote server reach code that indexes into buffers.
///
/// Named for the task rather than the probe because the stale-list rebuild and the account-listing fetch use it
/// too, and the counter is named to match: booking a rebuild's panic to a probe series would quietly ruin the
/// one metric an operator would alert on for "a malformed container header crashed a parser".
pub(crate) fn spawn_background<F>(runtime: Arc<crate::runtime::ScoutRuntime>, what: &'static str, fut: F)
where
    F: Future<Output = ()> + Send + 'static,
{
    let task = tokio::spawn(crate::runtime::scope(runtime.clone(), fut));
    tokio::spawn(crate::runtime::scope(runtime, async move {
        if let Err(e) = task.await {
            if e.is_panic() {
                recover_background(what, &*e.into_panic());
            }
        }
    }));
}

/// Go `recoverBackground`, for a panic payload already caught.
pub(crate) fn recover_background(what: &str, payload: &(dyn std::any::Any + Send)) {
    metrics().background_panic.fetch_add(1, Ordering::Relaxed);
    let text = payload
        .downcast_ref::<&str>()
        .map(|s| (*s).to_owned())
        .or_else(|| payload.downcast_ref::<String>().cloned())
        .unwrap_or_else(|| "a panic".to_owned());
    // `what` is always a literal naming the job, so it is a fixed vocabulary for the limiter's key.
    log_limited(&format!("panic:{what}"), &format!("{what} panicked, abandoning it: {text}"));
}

/// Clears a rebuild booking when the task that owns it ends, however it ends.
struct RebuildRelease {
    inner: Arc<Inner>,
    key: String,
    gen: u64,
    cooloff: Option<Instant>,
}

impl Drop for RebuildRelease {
    fn drop(&mut self) {
        lock(&self.inner.rebuilds).release_rebuild(&self.key, self.gen, self.cooloff);
    }
}

/// Removes a flight from the map when its task ends, however it ends.
struct FlightDone {
    inner: Arc<Inner>,
    key: String,
}

impl Drop for FlightDone {
    fn drop(&mut self) {
        lock(&self.inner.flights).remove(&self.key);
    }
}

impl Inner {
    fn new(mut deps: Deps) -> Inner {
        if deps.scrape_timeout.is_zero() {
            deps.scrape_timeout = DEFAULT_TIMEOUT;
        }
        if deps.list_ttl.is_zero() {
            deps.list_ttl = DEFAULT_LIST_TTL;
        }
        if deps.play_ticket_ttl.is_zero() {
            deps.play_ticket_ttl = crate::ticket::DEFAULT_PLAY_TICKET_TTL;
        }
        let info = match deps.config_slice {
            Some(_) => crate::ticket::DEN_PLAY_TICKET_INFO,
            None => crate::ticket::PLAY_TICKET_INFO,
        };
        let tickets = TicketKeys::with_info(deps.seal_keyring.as_deref(), info);
        let ttl_sec = deps.list_ttl.as_secs();
        // Private, every list: its play URLs are bearer tickets good for a day, so no shared cache between the client
        // and scout (a CDN in front of den-edge, a corporate proxy) may keep a copy to hand to someone else.
        //
        // stale-if-error is bounded by the tickets' life (staleIfErrorFor): a list a device falls back to during an
        // outage must still play. It was a flat day, as long as the tickets themselves.
        let stale_if_error = stale_if_error_for(deps.list_ttl, deps.play_ticket_ttl);
        let outage_hold = outage_hold_for(deps.list_ttl, deps.play_ticket_ttl);
        let mut list_cache = format!("private, max-age={ttl_sec}, stale-while-revalidate={ttl_sec}");
        if stale_if_error.as_secs() > 0 {
            list_cache.push_str(&format!(", stale-if-error={}", stale_if_error.as_secs()));
        }
        // A short list and a stale one must never be held LONGER than a complete fresh one. Both windows are
        // therefore capped by the configured TTL, not just written as the 60s that suits the default: at
        // LIST_TTL_SECS=30 a knowingly-short list was being advertised as fresh for 60s and usable
        // for 120 while a complete one got 30 — the exact inversion three comments in this file forbid, and
        // the first attempt at this fixed it only for the stale header, one branch over from the partial one.
        let short_sec = PARTIAL_LIST_TTL.as_secs().min(ttl_sec);
        let partial_list_cache = format!("private, max-age={short_sec}, stale-while-revalidate={short_sec}");
        // No stale-if-error on a stale body: the client should come back once the rebuild this response
        // booked has landed, not hold the old list for a day on the next error.
        let stale_list_cache = format!("private, max-age={short_sec}");
        Inner {
            tickets,
            flights: Mutex::new(HashMap::new()),
            probe_flight: Flight::new(),
            rebuilds: Mutex::new(RebuildGate::default()),
            scrape_fails: AtomicI32::new(0),
            partial_builds: AtomicI32::new(0),
            avail_in_flight: Mutex::new(HashSet::new()),
            links: LinkMemo::default(),
            legacy_play_noted: AtomicBool::new(false),
            list_cache,
            partial_list_cache,
            stale_list_cache,
            outage_hold,
            status_budget_ms: AtomicU64::new(DEFAULT_STATUS_BUDGET.as_millis() as u64),
            debug_limiter: Mutex::new(Arc::new(new_host_limiter(Duration::from_secs(10), 3))),
            // Paced per upstream host, and REFUSED rather than queued when the allowance is gone. Sized for a
            // person, not a script: five presses of the button, then one every two seconds. See validate.rs.
            validate_limiter: Mutex::new(Arc::new(new_host_limiter(Duration::from_secs(2), 5))),
            deps,
        }
    }

    pub(crate) fn status_budget(&self) -> Duration {
        Duration::from_millis(self.status_budget_ms.load(Ordering::Relaxed))
    }

    /// deadlinePassed's counterpart for the explicit clock: the deadline `d` from now.
    fn deadline_in(d: Duration) -> TokioInstant {
        TokioInstant::now() + d
    }

    async fn managed_hls(
        self: &Arc<Self>,
        config: &Config,
        request: &ManagedHlsRequest,
    ) -> Result<ManagedHlsResult, ManagedHlsError> {
        let (kind, raw_id) = match (request.season, request.episode) {
            (None, None) => ("movie", format!("{}.json", request.imdb)),
            (Some(season), Some(episode)) => ("series", format!("{}:{season}:{episode}.json", request.imdb)),
            _ => return Err(ManagedHlsError::NotFound),
        };
        let sid = parse_stream_id(kind, &raw_id).ok_or(ManagedHlsError::NotFound)?;
        let resolver = self.deps.managed_hls.as_ref().ok_or(ManagedHlsError::Unavailable)?;
        let deadline = Self::deadline_in(self.deps.scrape_timeout + LIST_BUILD_SLACK);
        let list = self
            .rank_list(
                deadline,
                config,
                &sid,
                None,
                RankOpts { client: request.playable.as_ref(), check_cache: true, rid: "" },
            )
            .await;
        for candidate in managed_candidates(&list, request)? {
            let target = ResolveTarget {
                info_hash: candidate.info_hash.clone(),
                file_idx: candidate.file_idx,
                season: season_ptr(&sid),
                episode: episode_ptr(&sid),
                release_size: candidate.size_bytes.unwrap_or_default(),
                title: candidate.title.clone(),
                imdb: sid.imdb.clone(),
                ..Default::default()
            };
            match resolver.resolve(config, &target, request.subtitle).await {
                Ok(prepared) => {
                    return Ok(ManagedHlsResult {
                        url: prepared.url,
                        subtitles: prepared.subtitles,
                        selected_subtitle: prepared.selected_subtitle,
                        release: ManagedHlsRelease {
                            label: crate::label::clean_label_lower(
                                &str::to_lowercase(&candidate.title),
                                candidate,
                            ),
                            filename: candidate.title.clone(),
                            size: candidate.size_bytes.filter(|size| *size > 0),
                        },
                    });
                }
                Err(ManagedHlsError::NotFound) => continue,
                Err(error) => return Err(error),
            }
        }
        Err(ManagedHlsError::NotFound)
    }

    // --- routing ----------------------------------------------------------------------------------------

    async fn route(self: &Arc<Self>, req: &Request<Bytes>) -> Resp {
        // A CORS preflight is answered on EVERY path, ahead of the method gate below, which would otherwise
        // refuse it with a 405 and fail the preflight — /validate's JSON POST is one a browser preflights. It
        // does nothing but name what may follow, so it cannot reach /play's resolve.
        if req.method() == Method::OPTIONS {
            return preflight();
        }
        // `%XX` escapes resolved, anything malformed left as it came, `+` left alone.
        let path = den_http::percent_decode_path(req.uri().path());
        // CONFIG_KEY is set but malformed: nothing that carries an install config (a config segment, a play
        // ticket, the availability batch, a cancel) can be trusted, sealed or plaintext, so all of it is a 503
        // until an operator fixes the variable. Only /health, /metrics and /validate (a pasted token,
        // no config) keep their own handling.
        if self.deps.config_key_invalid && !matches!(path.as_str(), "/health" | "/metrics" | "/validate") {
            return config_key_invalid();
        }
        // DELETE cancels (or, with ?op=reannounce, reannounces) the release a ticket or a legacy play token
        // names — den#204. Matched ahead of the GET/HEAD-only gate below, which otherwise answers every other
        // verb with 405; cancel is the one route besides /validate and /availability that is not a read.
        if req.method() == Method::DELETE {
            return self.handle_cancel(req, &path).await;
        }
        // /validate is the one route that is not a read: it takes a pasted token and asks the service whether
        // it works, so it is a POST and is matched before the gate below rather than carved out of it.
        if path == "/validate" {
            return self.handle_validate(req).await;
        }
        // The availability batch is a POST as well — a page of ids does not belong in a URL — and it too is
        // matched ahead of the gate. It answers from cache or starts a read-only check; it never resolves.
        if path.ends_with("/availability") {
            return self.handle_availability(req, &path);
        }
        // Every route here is a read, so GET/HEAD is the entire vocabulary — and until this gate existed the
        // handler dispatched on PATH ALONE, which made that an assumption rather than a rule. `/play` is the
        // one that mattered: it resolves, and resolving an uncached release ADDS it, against an allowance of
        // fifty an hour. So any verb at all — a link unfurler's POST, a crawler's PUT — reached the one code
        // path this package spends the most effort keeping accidental callers out of (see the probe route's
        // `?probe=1`, probeTop's held-only rule, and addbudget.go).
        if req.method() != Method::GET && req.method() != Method::HEAD {
            return method_not_allowed("GET, HEAD");
        }
        match path.as_str() {
            "/health" => {
                // Stays 200 (liveness — the addon itself is up), but reports the degraded scrape
                // state so a monitor sees a total-indexer outage instead of just "empty results".
                let mut status = json!({"status": "ok", "coverage": "complete"});
                if self.deps.config_key_invalid {
                    status = json!({
                        "status": "degraded",
                        "reason": "config_key_invalid",
                        "detail": "CONFIG_KEY is set but not a valid base64 32-byte key — install configs are refused until it is fixed",
                    });
                } else if self.scrape_fails.load(Ordering::Relaxed) >= SCRAPE_FAIL_THRESHOLD {
                    status =
                        json!({"status": "degraded", "reason": "indexers", "detail": INDEXERS_DOWN_DETAIL});
                } else if self.partial_builds.load(Ordering::Relaxed) >= SCRAPE_FAIL_THRESHOLD {
                    // Which source is withheld: naming it says which indexers this install uses.
                    status["coverage"] = json!("partial");
                    status["detail"] = json!(SOURCES_MISSING_DETAIL);
                }
                // A spent add budget refuses every play with the same 503 a throttled debrid gives, and the only
                // other evidence is one log line per refusal. The tightest remaining allowance is what an operator
                // or a monitor needs; per-account detail is withheld because this route is unauthenticated.
                let (left, accounts) = global_add_budget().lowest();
                if accounts > 0 {
                    status["addBudgetRemaining"] = json!(left);
                }
                return json_response(StatusCode::OK, &status);
            }
            "/metrics" => {
                // Prometheus text format, behind a bearer token, and OFF when no token is configured.
                //
                // The first version was unauthenticated on the grounds that it carries no debrid service or
                // account labels. That answered the credential-oracle question addbudget.go raises about /health
                // and missed two the counters create on their own. scout_list_cache_total{result="miss"} rises
                // once per title opened cold, so polling this endpoint is a timeline of when the household is
                // watching and how much it browses — /health exposes nothing comparable. And per-indexer counters
                // disclose which indexers this install has configured, which IS per-install configuration: with
                // MINT_INDEXER_CONFIGS on, a nonzero mediafusion counter says the debrid token was minted
                // and sent to that host.
                //
                // Dropping the useful labels would have left an endpoint not worth scraping, so the other option
                // is taken: a token. Unset means 404 rather than an empty 200, so an operator who has not
                // configured one is not told there is something here to poke at.
                if !den_http::metrics_authorized(req.headers(), Some(&self.deps.metrics_token)) {
                    return error(StatusCode::NOT_FOUND, "not_found");
                }
                return Response::builder()
                    .header(CONTENT_TYPE, "text/plain; version=0.0.4; charset=utf-8")
                    .header(CACHE_CONTROL, NO_STORE)
                    .body(full(metrics().render(cache_persistent_gauge(&*self.deps.cache))))
                    .expect("a static response");
            }
            _ => {}
        }

        // ["<config>", "stream"|"play", ...]
        let parts = split_path(&path);
        // /p/<ticket> exists only while tickets are on; without them it stays the 404 it always was.
        if parts.len() == 2 && parts[0] == TICKET_ROUTE && self.tickets.is_some() {
            return self.handle_ticket_play(req, parts[1]).await;
        }
        let config_blob = at(&parts, 0);
        match at(&parts, 1) {
            "stream" => self.handle_stream(req, config_blob, &parts).await,
            "play" => self.handle_play(req, config_blob, &parts).await,
            "account" => self.handle_account(req, config_blob).await,
            _ => error(StatusCode::NOT_FOUND, "not_found"),
        }
    }

    // Stream-list discovery, ranking, cache rebuilds, and serialization live in handler::discovery.
    // Probe, play, and cancellation orchestration live in handler::playback.
    // --- /account ---------------------------------------------------------------------------------------

    /// handleAccount is GET /<config>/account — den#204: one quiet line of slot and allowance usage per
    /// configured debrid account, in configured order. A non-TorBox account answers with its service name and
    /// nothing else: Real-Debrid and Premiumize publish no comparable facts.
    async fn handle_account(&self, req: &Request<Bytes>, config_blob: &str) -> Resp {
        let Some(config) = self.open_config(config_blob) else {
            return error(StatusCode::BAD_REQUEST, "bad_config");
        };
        if let Some(refused) = self.refuse_scoped(req, &config) {
            return refused;
        }
        let deadline = Self::deadline_in(self.status_budget());
        let stores = (self.deps.make_stores)(&config);
        // The config permits at most one account for each of the three services. Their summaries are
        // independent, and every one is already bounded by this same absolute deadline, so ask them
        // together. `join_all` returns in input order even when a later account answers first.
        let accounts = join_all(stores.iter().map(|st| account_body(deadline, st.as_ref())).collect()).await;
        json_response(StatusCode::OK, &accounts)
    }

    // --- /validate --------------------------------------------------------------------------------------

    async fn handle_validate(&self, req: &Request<Bytes>) -> Resp {
        if req.method() != Method::POST {
            return method_not_allowed("POST");
        }
        #[derive(Deserialize, Default)]
        struct Asked {
            #[serde(default)]
            service: Option<String>,
            #[serde(default)]
            token: Option<String>,
        }
        static SHAPE: Shape = Shape::Object(&[("service", &LEAF), ("token", &LEAF)]);
        // The first JSON value in at most maxValidateBody bytes; whatever follows it is ignored.
        let body = req.body();
        let window = &body[..body.len().min(MAX_VALIDATE_BODY)];
        let decoded = serde_json::Deserializer::from_slice(window)
            .into_iter::<serde_json::Value>()
            .next()
            .and_then(Result::ok)
            .and_then(|mut v| {
                fold_json_keys(&mut v, &SHAPE);
                serde_json::from_value::<Asked>(v).ok()
            });
        let Some(body) = decoded else { return error(StatusCode::BAD_REQUEST, "bad_request") };
        let (service, token) = (body.service.unwrap_or_default(), body.token.unwrap_or_default());
        // Whitelisted the same way validateConfig does it, and the same length ceiling, so this route cannot
        // be used to send arbitrary strings to an upstream.
        let Some(svc) = DebridService::parse(&service).filter(|_| !token.is_empty() && token.len() <= 512)
        else {
            return error(StatusCode::BAD_REQUEST, "bad_request");
        };

        // NOTHING here logs the token, and no log line names this route with its arguments. The configure
        // page seals the config in the browser, so this is the one request in the system where the server is
        // handed a token in the clear — it is used once, for one upstream read, and not written anywhere.
        let res = match self.probe_doer() {
            None => ValidateResult { reason: "validation unavailable".to_owned(), ..Default::default() },
            Some(doer) => {
                let limiter = lock(&self.validate_limiter).clone();
                match timeout(VALIDATE_BUDGET, validate_debrid_token(doer.as_ref(), &limiter, svc, &token))
                    .await
                {
                    Ok(res) => res,
                    Err(_) => ValidateResult { reason: "timed out".to_owned(), ..Default::default() },
                }
            }
        };
        if res.throttled {
            let mut resp = json_response(StatusCode::TOO_MANY_REQUESTS, &res);
            resp.headers_mut().insert(RETRY_AFTER, HeaderValue::from_static("2"));
            return resp;
        }
        json_response(StatusCode::OK, &res)
    }

    // --- shared plumbing --------------------------------------------------------------------------------

    /// publicOrigin: PUBLIC_BASE_URL when set (audit #8), else forwarded headers, then the request's own scheme
    /// and Host.
    fn public_origin(&self, req: &Request<Bytes>) -> String {
        if !self.deps.public_url.is_empty() {
            return self.deps.public_url.trim_end_matches('/').to_owned();
        }
        let mut proto = first_header(req, "x-forwarded-proto");
        if proto.is_empty() {
            proto = if req.uri().scheme_str() == Some("https") { "https" } else { "http" };
        }
        let mut host = first_header(req, "x-forwarded-host");
        if host.is_empty() {
            host = req
                .headers()
                .get(HOST)
                .and_then(|v| v.to_str().ok())
                .or_else(|| req.uri().authority().map(|a| a.as_str()))
                .unwrap_or("");
        }
        format!("{proto}://{host}{}", self.deps.mount)
    }

    /// remuxAuthorized reports whether the request carries den-remux's service key (X-Den-Remux-Key), compared
    /// in constant time. Never true while REMUX_KEY is unset.
    fn remux_authorized(&self, req: &Request<Bytes>) -> bool {
        let key = &self.deps.remux_key;
        let got = header(req, "x-den-remux-key");
        !key.is_empty() && !got.is_empty() && constant_time_eq(got.as_bytes(), key.as_bytes())
    }

    /// refuseScoped answers 403 for a scoped config on a route outside its scope — every route that lists or
    /// plays streams — and says whether it did. The one exception is den-remux: with its service key, an
    /// availability-scoped config may list and play, so the browser drives playback through den-remux with the
    /// scoped URL it already holds and never gets a stream-capable config. The resulting tickets stay on the
    /// homelab (den-remux resolves them; the browser gets only its own signed session URLs).
    fn refuse_scoped(&self, req: &Request<Bytes>, config: &Config) -> Option<Resp> {
        if config.scope.is_empty() || (config.scope == SCOPE_AVAILABILITY && self.remux_authorized(req)) {
            return None;
        }
        Some(error(StatusCode::FORBIDDEN, "out_of_scope"))
    }

    /// openConfig is decodeConfig plus admitInstall: every route that takes a config segment opens it here, so a
    /// revoked install is refused exactly as an undecodable config is.
    ///
    /// A warm stream-list hit opens nothing (handleStream), so for a few minutes after a revocation the install
    /// can still read a list it had cached. Every play URL in that list is refused.
    pub(crate) fn open_config(&self, blob: &str) -> Option<Config> {
        let config = crate::config::decode_config_slice(
            self.deps.seal_keyring.as_deref(),
            blob,
            self.deps.config_slice,
        )?;
        self.admit_install(&config).then_some(config)
    }

    /// admitInstall applies REVOKED_INSTALLS and CONFIG_EPOCH to a config, whether it came from a config segment
    /// or a play ticket. The log line names the reason. A revoked id is cut to six characters, and the id of an
    /// install that is not revoked is never logged.
    pub(crate) fn admit_install(&self, config: &Config) -> bool {
        let iid = (!config.iid.is_empty()).then_some(config.iid.as_str());
        // A negative epoch is not "epoch 0" (which every CONFIG_EPOCH admits): refuse it. decode already
        // rejects one today; this keeps that true if the decoder ever stops.
        let Ok(ep) = u64::try_from(config.epoch) else {
            log_limited("install-epoch", "config refused: negative install epoch");
            return false;
        };
        // A scoped config is minted separately from the install and may carry no id. Tickets den-remux is
        // authorized to mint from it preserve that provenance; epoch and named revocation still apply.
        let verdict = if config.scope.is_empty() {
            self.deps.revocation.check(iid, ep)
        } else {
            self.deps.revocation.check_scoped(iid, ep)
        };
        match verdict {
            Ok(()) => return true,
            Err(Rejected::Revoked { iid_prefix }) => log_limited(
                "install-revoked",
                &format!("config refused: install revoked (iid {iid_prefix}…)"),
            ),
            Err(Rejected::EpochTooOld { ep, epoch }) => log_limited(
                "install-epoch",
                &format!("config refused: install epoch too old ({ep} < CONFIG_EPOCH {epoch})"),
            ),
            Err(Rejected::NoInstallId) => {
                log_limited("install-no-iid", "config refused: no install id (REQUIRE_INSTALL_ID is on)")
            }
            // `check` yields only the three above.
            Err(Rejected::Undecodable | Rejected::Plaintext) => {}
        }
        false
    }
}

fn unix_time(secs: i64) -> SystemTime {
    if secs >= 0 {
        UNIX_EPOCH + Duration::from_secs(secs as u64)
    } else {
        UNIX_EPOCH - Duration::from_secs(secs.unsigned_abs())
    }
}

/// The first of a comma-separated header's values (a proxy chain's `X-Forwarded-*`), "" when absent.
fn first_header<'a>(req: &'a Request<Bytes>, name: &str) -> &'a str {
    header(req, name).split(',').next().unwrap_or("").trim()
}

/// Marks a response as a fallback (`X-Den-Degraded`), when there is a reason.
fn mark_degraded(resp: &mut Resp, reason: &str) {
    if !reason.is_empty() {
        set_header(resp, HeaderName::from_static(X_DEN_DEGRADED), reason);
    }
}

/// Server-Timing: the phases a route named, closed by the total since `start`. A response with no phases still
/// says how long it took.
fn with_timing(resp: Resp, phases: &str, start: Instant) -> Resp {
    let total = den_http::timing("total", start.elapsed());
    den_http::add_timing(den_http::timed(resp, phases), &total)
}

/// beginPlay is what both play routes do first: refuse any verb but GET (see handlePlay for why HEAD is refused
/// too). Server-Timing is stamped on the refusal like on everything else the route answers.
/// writeAddBudgetSpent answers scout_busy with the add budget's limit and window as the IETF draft RateLimit
/// headers, beside Retry-After: nothing is left, and the budget frees in `wait`.
fn write_add_budget_spent(wait: Duration, body: &serde_json::Value) -> Resp {
    let secs = wait_secs(wait);
    let mut resp = write_unavailable(wait, body);
    let policy = format!("\"adds\";q={ADD_BUDGET_LIMIT};w={}", ADD_BUDGET_WINDOW.as_secs());
    set_header(&mut resp, HeaderName::from_static("ratelimit-policy"), &policy);
    set_header(&mut resp, HeaderName::from_static("ratelimit"), &format!("\"adds\";r=0;t={secs}"));
    resp
}

/// A wait in whole seconds, rounded up, and at least one.
fn wait_secs(wait: Duration) -> u64 {
    (wait.as_nanos().div_ceil(1_000_000_000) as u64).max(1)
}

/// writeUnavailable answers 503 with a Retry-After of at least a second, so a client that honours it waits as long as
/// the refusal behind the answer lasts instead of polling into it.
fn write_unavailable(wait: Duration, body: &serde_json::Value) -> Resp {
    let mut resp = json_response(StatusCode::SERVICE_UNAVAILABLE, body);
    resp.headers_mut().insert(RETRY_AFTER, HeaderValue::from(wait_secs(wait)));
    resp.extensions_mut().insert(ReleaseAnswer::new(
        body["error"].as_str().unwrap_or("unavailable"),
        body["service"].as_str().unwrap_or(""),
    ));
    resp
}

/// Stable retryable refusal for a cold list build that could not be admitted. `json_response` supplies
/// `no-store`, so intermediaries cannot turn transient saturation into a negative cache entry.
fn list_build_busy() -> Resp {
    write_unavailable(
        Duration::from_secs(1),
        &json!({
            "error": "scout_busy",
            "detail": "list_build_limit"
        }),
    )
}

fn scout_busy_for(pool: &StorePool, prefetch: bool) -> Duration {
    pool.scout_busy_for(prefetch)
}

fn known_file_size(pool: &StorePool, rt: &ResolveTarget) -> Option<i64> {
    pool.known_file_size(rt)
}

/// Go `torboxPlanName` maps TorBox's plan code (user/me's `plan`) to a name for the account line. No slot-count
/// table — den#204's owner decision was to show "N active" alone rather than guess a plan's slot ceiling.
pub(crate) fn torbox_plan_name(plan: i64) -> &'static str {
    match plan {
        0 => "free",
        1 => "essential",
        2 => "pro",
        3 => "standard",
        _ => "unknown",
    }
}

/// accountBody is one configured account's answer within /<config>/account's array. A TorBox read that
/// fails degrades to the bare service name — the add budget below is scout's own and always answerable,
/// but without slots/queued there is nothing else honest to report.
async fn account_body(deadline: TokioInstant, st: &dyn Store) -> serde_json::Value {
    let bare = || json!({"service": st.service().as_str()});
    let summary = match timeout_at(deadline, st.account_summary()).await {
        Ok(None) => return bare(),
        Ok(Some(Ok(sum))) => sum,
        Ok(Some(Err(err))) => {
            log_limited("account-unavailable", &format!("account {}: {err}", st.service().as_str()));
            return bare();
        }
        Err(_) => {
            log_limited(
                "account-unavailable",
                &format!("account {}: context deadline exceeded", st.service().as_str()),
            );
            return bare();
        }
    };
    let mut adds = serde_json::Map::new();
    if let Some(acct) = st.budget_account() {
        let budget = global_add_budget();
        adds.insert("left".to_owned(), json!(budget.remaining(&acct)));
        let frees_in = budget.frees_in(&acct, false);
        if !frees_in.is_zero() {
            adds.insert(
                "freesAt".to_owned(),
                json!(format_utc_rfc3339(now_unix() + frees_in.as_secs() as i64)),
            );
        }
    }
    let mut body = json!({
        "service": st.service().as_str(),
        "plan": torbox_plan_name(summary.plan),
        "slots": {"active": summary.active},
        "queued": summary.queued,
        "adds": adds,
    });
    if let Some(t) = &summary.cooldown_until {
        body["cooldownUntil"] = json!(utc_rfc3339(t));
    }
    if let Some(t) = &summary.premium_expires_at {
        body["expiresAt"] = json!(utc_rfc3339(t));
    }
    body
}

/// Go `time.Time.UTC().Format(time.RFC3339)` for a Unix second count.
fn format_utc_rfc3339(unix: i64) -> String {
    let days = unix.div_euclid(86_400);
    let rem = unix.rem_euclid(86_400);
    let (year, month, day) = den_upstream::civil::civil_from_days(days);
    format!("{year:04}-{month:02}-{day:02}T{:02}:{:02}:{:02}Z", rem / 3600, rem % 3600 / 60, rem % 60)
}

/// Re-prints an RFC 3339 time (as `Rfc3339::to_rfc3339` writes it) in UTC without a fraction, as Go's
/// `.UTC().Format(time.RFC3339)` does. The input is our own output, so a shape that does not parse is returned
/// unchanged.
fn utc_rfc3339(s: &str) -> String {
    let b = s.as_bytes();
    let num = |from: usize, len: usize| -> Option<i64> { s.get(from..from + len)?.parse().ok() };
    let parsed = (|| {
        let (year, month, day) = (num(0, 4)?, num(5, 2)?, num(8, 2)?);
        let (hour, minute, second) = (num(11, 2)?, num(14, 2)?, num(17, 2)?);
        let mut i = 19;
        if b.get(i) == Some(&b'.') {
            i += 1;
            while b.get(i).is_some_and(u8::is_ascii_digit) {
                i += 1;
            }
        }
        let offset = match b.get(i)? {
            b'Z' => 0,
            sign @ (b'+' | b'-') => {
                let secs = num(i + 1, 2)? * 3600 + num(i + 4, 2)? * 60;
                if *sign == b'-' {
                    -secs
                } else {
                    secs
                }
            }
            _ => return None,
        };
        let days = den_upstream::civil::days_from_civil(year, month as u32, day as u32);
        Some(days * 86_400 + hour * 3600 + minute * 60 + second - offset)
    })();
    parsed.map(format_utc_rfc3339).unwrap_or_else(|| s.to_owned())
}
