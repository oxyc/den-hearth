//! Port of `internal/scout/handler.go`: the runtime-agnostic core as an HTTP service (ported from
//! src/handler.ts). Off-device by design: the app never sees a torrent or a debrid token. Routes are served
//! at the service root. Work package 4.
//!
//! The Go `http.Handler` becomes `Handler::serve`, which takes an `http::Request` and returns an
//! `http::Response`. Go's request context is gone: a client that disconnects drops the `serve` future, which
//! aborts the task serving it, and a deadline is the explicit `tokio::time::Instant` handed down to whatever
//! takes one. Work that must outlive the request (a stream-list build shared by every viewer of a title, the
//! stale rebuild, the probe fan-out, the availability check) is spawned on its own task, which is what Go's
//! `context.WithoutCancel` plus a detached goroutine did.

use std::borrow::Cow;
use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicI32, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::task::{Context, Poll};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use http::{HeaderMap, HeaderName, HeaderValue, Method};
use serde::{Deserialize, Serialize};
use serde_json::json;
use tokio::sync::{watch, Semaphore};
use tokio::task::JoinHandle;
use tokio::time::{timeout, timeout_at, Instant as TokioInstant};

use crate::addbudget::{budget_account, global_add_budget, is_play_reserve, ADD_BUDGET_LIMIT, ADD_BUDGET_WINDOW};
use crate::attributes::stream_attributes;
use crate::availability::verdict_prefix;
use crate::client::{web_ready, ClientPlayable};
use crate::config::{Config, DebridService, SCOPE_AVAILABILITY};
use crate::configure::CONFIGURE_PAGE;
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
use crate::manifest::build_manifest;
use crate::metrics::{cache_persistent_gauge, metrics};
use crate::play::{decode_play_token, encode_play_token, PlayTarget};
use crate::playwarm::play_pending_key;
use crate::probe_fanout::{
    browser_probe_candidates, hydrate_pack_only_sizes, hydrate_probe_facts, select_probe_jobs, ProbeJob, PROBE_BUDGET,
    PROBE_TOP_N, PROBE_TTL,
};
use crate::rank::{
    cap_seeds, rank_streams_debug, real_debrid_blocked, title_tokens, RankDebug, RankFilters, RawStream,
};
use crate::reqlog::{redact_path, request_id};
use crate::scrape::{scrape_covered, Doer, ScrapeQuery, Scraper};
use crate::stores::pool::{has_cache_truth, status_detail_among, StorePool};
use crate::stores::refusal::{as_unavailable, scout_side_reason};
use crate::stores::{
    CacheTruth, ResolveTarget, Store, StoreError, StoreStatus, StoreUnavailableError, FETCH_DOWNLOADING, FETCH_QUEUED,
};
use crate::ticket::{TicketError, TicketKeys, TICKET_ROUTE};
use crate::util::{etag_hex, fold_json_keys, go_to_lower, key_hash, Shape, LEAF};
use crate::validate::{validate_debrid_token, ValidateResult, MAX_VALIDATE_BODY, VALIDATE_BUDGET};

/// Go `staticCache`.
pub const STATIC_CACHE: &str = "public, max-age=3600, stale-while-revalidate=600";
/// A configured manifest's URL is the install credential, so no shared cache may keep a copy keyed on it.
pub(crate) const CONFIGURED_MANIFEST_CACHE: &str = "private, max-age=3600, stale-while-revalidate=600";
/// The sealing key can rotate, so keep its freshness window short — the ETag is the primary
/// correctness mechanism (a rotated key changes the body hash and busts any stale cache).
pub(crate) const KEY_CACHE: &str = "public, max-age=300";
/// Go `noStore`.
pub const NO_STORE: &str = "no-store";
/// Go `jsonType`.
pub const JSON_TYPE: &str = "application/json";
pub(crate) const HTML_TYPE: &str = "text/html; charset=utf-8";
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
const INDEXERS_DOWN_DETAIL: &str = "No indexer has answered the last few stream-list builds, so lists are coming back empty.";

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
        if !self.rebuilds.get(key).is_some_and(|l| l.gen == gen) {
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
    pub(crate) fn holds(&self, key: &str) -> bool {
        self.rebuilds.contains_key(key)
    }

    /// How many background rebuilds are in flight.
    pub(crate) fn live(&self) -> usize {
        self.live
    }
}

// --- request and response -----------------------------------------------------------------------------------

/// One request, decoded the way Go's `*http.Request` hands it to a handler: the path percent-decoded, the query
/// as sent, the headers, and where it arrived.
pub(crate) struct Req {
    pub(crate) method: Method,
    /// Go `r.URL.Path`.
    pub(crate) path: String,
    /// Go `r.URL.RawQuery`.
    pub(crate) query: String,
    pub(crate) headers: HeaderMap,
    pub(crate) body: Vec<u8>,
    /// Go `r.TLS != nil`, read off the URI's scheme.
    pub(crate) https: bool,
    /// Go `r.Host`.
    pub(crate) host: String,
}

impl Req {
    fn new(req: http::Request<Vec<u8>>) -> Req {
        let (parts, body) = req.into_parts();
        let host = parts
            .headers
            .get(http::header::HOST)
            .and_then(|v| v.to_str().ok())
            .map(str::to_owned)
            .or_else(|| parts.uri.authority().map(|a| a.as_str().to_owned()))
            .unwrap_or_default();
        Req {
            path: percent_decode(parts.uri.path()),
            query: parts.uri.query().unwrap_or("").to_owned(),
            https: parts.uri.scheme_str() == Some("https"),
            method: parts.method,
            headers: parts.headers,
            body,
            host,
        }
    }

    /// Go `r.Header.Get`: the first value, "" when absent.
    pub(crate) fn header(&self, name: &str) -> String {
        self.headers.get(name).and_then(|v| v.to_str().ok()).unwrap_or("").to_owned()
    }

    /// Go `r.URL.Query().Get`.
    pub(crate) fn query_get(&self, key: &str) -> String {
        if self.query.is_empty() {
            return String::new();
        }
        url::form_urlencoded::parse(self.query.as_bytes())
            .find(|(k, _)| k == key)
            .map(|(_, v)| v.into_owned())
            .unwrap_or_default()
    }
}

/// Go's decoded `URL.Path`: `%XX` escapes resolved, anything malformed left as it came.
fn percent_decode(s: &str) -> String {
    if !s.contains('%') {
        return s.to_owned();
    }
    let b = s.as_bytes();
    let mut out = Vec::with_capacity(b.len());
    let mut i = 0;
    while i < b.len() {
        if b[i] == b'%' && i + 2 < b.len() && b[i + 1].is_ascii_hexdigit() && b[i + 2].is_ascii_hexdigit() {
            let hex = |c: u8| (c as char).to_digit(16).unwrap_or(0) as u8;
            out.push(hex(b[i + 1]) << 4 | hex(b[i + 2]));
            i += 3;
        } else {
            out.push(b[i]);
            i += 1;
        }
    }
    String::from_utf8_lossy(&out).into_owned()
}

/// A response being built: Go's `ResponseWriter` after its handler has run.
pub(crate) struct Out {
    pub(crate) status: u16,
    pub(crate) headers: HeaderMap,
    pub(crate) body: Vec<u8>,
}

impl Out {
    pub(crate) fn new(status: u16) -> Out {
        Out { status, headers: HeaderMap::new(), body: Vec::new() }
    }

    /// Go `Header().Set`.
    pub(crate) fn set(&mut self, name: &str, value: &str) {
        if let (Ok(n), Ok(v)) = (HeaderName::from_bytes(name.as_bytes()), HeaderValue::from_str(value)) {
            self.headers.insert(n, v);
        }
    }

    /// Go `Header().Add`.
    pub(crate) fn add(&mut self, name: &str, value: &str) {
        if let (Ok(n), Ok(v)) = (HeaderName::from_bytes(name.as_bytes()), HeaderValue::from_str(value)) {
            self.headers.append(n, v);
        }
    }

    /// Go `Header().Get`.
    pub(crate) fn get(&self, name: &str) -> String {
        self.headers.get(name).and_then(|v| v.to_str().ok()).unwrap_or("").to_owned()
    }
}

/// Go `writeJSON`: a JSON body with its status and, when given, its Cache-Control.
pub(crate) fn write_json(status: u16, body: &(impl Serialize + ?Sized), cache_control: &str) -> Out {
    let mut out = Out::new(status);
    out.set("content-type", JSON_TYPE);
    if !cache_control.is_empty() {
        out.set("cache-control", cache_control);
    }
    out.body = marshal(body).into_bytes();
    out
}

/// Go `errBody` written the way every refusal writes it.
pub(crate) fn err_out(status: u16, msg: &str) -> Out {
    write_json(status, &json!({ "error": msg }), NO_STORE)
}

/// Go `json.Marshal`: serde's output, with the characters Go's encoder escapes for HTML safety written as it
/// writes them (`<`, `>`, `&`, U+2028 and U+2029). Only strings can hold any of them, so the whole text is safe to
/// rewrite.
pub(crate) fn marshal<T: Serialize + ?Sized>(v: &T) -> String {
    let s = serde_json::to_string(v).unwrap_or_else(|_| "null".to_owned());
    if !s.contains(['<', '>', '&', '\u{2028}', '\u{2029}']) {
        return s;
    }
    let mut out = String::with_capacity(s.len() + 16);
    for c in s.chars() {
        match c {
            '<' => out.push_str("\\u003c"),
            '>' => out.push_str("\\u003e"),
            '&' => out.push_str("\\u0026"),
            '\u{2028}' => out.push_str("\\u2028"),
            '\u{2029}' => out.push_str("\\u2029"),
            c => out.push(c),
        }
    }
    out
}

/// A number as Go's encoder writes a float64: whole values without a decimal point.
fn go_num(f: f64) -> serde_json::Value {
    if f.fract() == 0.0 && f.abs() < 1e15 {
        return serde_json::Value::from(f as i64);
    }
    serde_json::Number::from_f64(f).map_or(serde_json::Value::Null, serde_json::Value::Number)
}

/// msDur formats a duration as Server-Timing's milliseconds, to a tenth — a warm hit takes well under one,
/// and "0" says nothing.
pub(crate) fn ms_dur(d: Duration) -> String {
    format!("{:.1}", d.as_micros() as f64 / 1000.0)
}

/// Go `time.Duration.String()`, for the lines that print one.
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
    if a.len() != b.len() {
        return false;
    }
    a.iter().zip(b).fold(0u8, |acc, (x, y)| acc | (x ^ y)) == 0
}

pub(crate) fn split_path(p: &str) -> Vec<String> {
    p.split('/').filter(|s| !s.is_empty()).map(str::to_owned).collect()
}

fn at(parts: &[String], i: usize) -> &str {
    parts.get(i).map_or("", String::as_str)
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
            binge_group: binge_group(&go_to_lower(&s.title), &s.title),
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
pub(crate) fn client_playable(req: &Req) -> (Option<ClientPlayable>, String) {
    let raw = req.header(PLAYABLE_HEADER);
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
        .then(|| serde_json::from_str::<serde_json::Value>(&raw).ok())
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
    let canonical = marshal(&p);
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
    // Precomputed static responses (audit #15 — no per-request rebuild/rehash).
    manifest_unconf: String,
    manifest_unconf_etag: String,
    configure_etag: String,
    // The three stream-list Cache-Control headers. Every input is fixed once ListTTL is known, so
    // formatting them per request was three Sprintf and four allocations on a path whose own comments
    // insist a warm hit pays nothing — measured at ~5% of the warm-hit cost, for a constant.
    list_cache: String,
    partial_list_cache: String,
    stale_list_cache: String,
    /// staleIfErrorFor(ListTTL, PlayTicketTTL), fixed once both are known.
    stale_if_error: Duration,
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
    /// Serves one request, as Go's `http.Handler.ServeHTTP` does. The response body is buffered.
    pub async fn serve(&self, req: http::Request<Vec<u8>>) -> http::Response<Vec<u8>> {
        let started = Instant::now();
        let head_only = req.method() == Method::HEAD;
        // Decided here, once: with the request log off nothing below is paid.
        let log_line = self.inner.deps.log_requests.then(|| {
            let rid = request_id(req.headers().get("x-request-id").and_then(|v| v.to_str().ok()).unwrap_or(""));
            (req.method().clone(), redact_path(&percent_decode(req.uri().path())), rid)
        });
        let inner = self.inner.clone();
        // Convert any unforeseen panic into a clean 500 instead of a dropped connection. The request runs on its
        // own task, which is what lets a panic in it be seen here at all.
        let served = AbortOnDrop(tokio::spawn(async move {
            let req = Req::new(req);
            inner.route(&req).await
        }))
        .await;
        let mut out = match served {
            Ok(out) => out,
            Err(_) => {
                let mut out = err_out(500, "internal");
                add_cors(&mut out);
                out
            }
        };
        if let Some((method, path, rid)) = log_line {
            let rid = if rid.is_empty() { String::new() } else { format!(" rid={rid}") };
            emit(&format!("{method} {path} {} {}ms{rid}", out.status, started.elapsed().as_millis()));
        }
        if head_only {
            out.body.clear();
        }
        let mut res = http::Response::new(out.body);
        *res.status_mut() = http::StatusCode::from_u16(out.status).unwrap_or(http::StatusCode::INTERNAL_SERVER_ERROR);
        *res.headers_mut() = out.headers;
        res
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

    /// Go `hydrateProbeFacts`.
    pub(crate) fn hydrate_probe_facts(&self, streams: &mut [RawStream], sid: Option<&StreamId>) {
        self.inner.hydrate_probe_facts(streams, sid);
    }

    /// Go `hydrateKnownSizes`.
    pub(crate) fn hydrate_known_sizes(&self, config: &Config, streams: &mut [RawStream], sid: Option<&StreamId>) {
        self.inner.hydrate_known_sizes(config, streams, sid);
    }

    /// Go `probeTop`.
    pub(crate) fn probe_top(
        &self,
        config: &Config,
        streams: &mut [RawStream],
        sid: &StreamId,
        truth: &CacheTruth,
    ) -> bool {
        self.inner.hydrate_probe_facts(streams, Some(sid));
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
fn add_cors(out: &mut Out) {
    out.set("access-control-allow-origin", "*");
    out.set(
        "access-control-expose-headers",
        "Server-Timing, X-Den-Degraded, Retry-After, ETag, RateLimit, RateLimit-Policy",
    );
    out.set("timing-allow-origin", "*");
}

/// A background task whose panic is counted and said, not allowed to take the process down. Go's
/// `recoverBackground`: work moved off the request task so the list would not wait for it left the only recover
/// in the service behind, and the probe fan-out is the sharpest case — the one code path in the addon where
/// bytes chosen by a remote server reach code that indexes into buffers.
///
/// Named for the task rather than the probe because the stale-list rebuild and the account-listing fetch use it
/// too, and the counter is named to match: booking a rebuild's panic to a probe series would quietly ruin the
/// one metric an operator would alert on for "a malformed container header crashed a parser".
pub(crate) fn spawn_background<F>(what: &'static str, fut: F)
where
    F: Future<Output = ()> + Send + 'static,
{
    let task = tokio::spawn(fut);
    tokio::spawn(async move {
        if let Err(e) = task.await {
            if e.is_panic() {
                recover_background(what, &*e.into_panic());
            }
        }
    });
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
        let tickets = TicketKeys::new(deps.seal_keyring.as_deref());
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
        let manifest_unconf = marshal(&build_manifest(None));
        let manifest_unconf_etag = etag_for(&manifest_unconf);
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
            manifest_unconf_etag,
            manifest_unconf,
            configure_etag: etag_for(CONFIGURE_PAGE),
            list_cache,
            partial_list_cache,
            stale_list_cache,
            stale_if_error,
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

    // --- routing ----------------------------------------------------------------------------------------

    async fn route(self: &Arc<Self>, req: &Req) -> Out {
        let mut out = self.route_inner(req).await;
        add_cors(&mut out);
        out
    }

    async fn route_inner(self: &Arc<Self>, req: &Req) -> Out {
        // A CORS preflight is answered on EVERY path, ahead of the method gate below, which would otherwise
        // refuse it with a 405 and fail the preflight — /validate's JSON POST is one a browser preflights. It
        // does nothing but name what may follow, so it cannot reach /play's resolve.
        if req.method == Method::OPTIONS {
            let mut out = Out::new(204);
            out.set("access-control-allow-methods", "GET, HEAD, POST, DELETE, OPTIONS");
            out.set("access-control-allow-headers", "*");
            // A day, so a browser stops re-preflighting every request.
            out.set("access-control-max-age", "86400");
            return out;
        }
        // DELETE cancels (or, with ?op=reannounce, reannounces) the release a ticket or a legacy play token
        // names — den#204. Matched ahead of the GET/HEAD-only gate below, which otherwise answers every other
        // verb with 405; cancel is the one route besides /validate and /availability that is not a read.
        if req.method == Method::DELETE {
            return self.handle_cancel(req).await;
        }
        // /validate is the one route that is not a read: it takes a pasted token and asks the service whether
        // it works, so it is a POST and is matched before the gate below rather than carved out of it.
        if req.path == "/validate" {
            return self.handle_validate(req).await;
        }
        // The availability batch is a POST as well — a page of ids does not belong in a URL — and it too is
        // matched ahead of the gate. It answers from cache or starts a read-only check; it never resolves.
        if req.path.ends_with("/availability") {
            return self.handle_availability(req);
        }
        // Every route here is a read, so GET/HEAD is the entire vocabulary — and until this gate existed the
        // handler dispatched on PATH ALONE, which made that an assumption rather than a rule. `/play` is the
        // one that mattered: it resolves, and resolving an uncached release ADDS it, against an allowance of
        // fifty an hour. So any verb at all — a link unfurler's POST, a crawler's PUT — reached the one code
        // path this package spends the most effort keeping accidental callers out of (see the probe route's
        // `?probe=1`, probeTop's held-only rule, and addbudget.go).
        if req.method != Method::GET && req.method != Method::HEAD {
            let mut out = err_out(405, "method_not_allowed");
            out.set("allow", "GET, HEAD");
            return out;
        }
        match req.path.as_str() {
            "/" | "/configure" | "/configure/" => {
                return self.conditional(req, CONFIGURE_PAGE, &self.configure_etag, HTML_TYPE, STATIC_CACHE);
            }
            "/health" => {
                // Stays 200 (liveness — the addon itself is up), but reports the degraded scrape
                // state so a monitor sees a total-indexer outage instead of just "empty results".
                let mut status = json!({"status": "ok", "coverage": "complete"});
                if self.scrape_fails.load(Ordering::Relaxed) >= SCRAPE_FAIL_THRESHOLD {
                    status = json!({"status": "degraded", "reason": "indexers", "detail": INDEXERS_DOWN_DETAIL});
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
                return write_json(200, &status, NO_STORE);
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
                if !self.metrics_authorized(req) {
                    return err_out(404, "not_found");
                }
                let mut out = Out::new(200);
                out.set("content-type", "text/plain; version=0.0.4; charset=utf-8");
                out.set("cache-control", NO_STORE);
                out.body = metrics().render(cache_persistent_gauge(&*self.deps.cache)).into_bytes();
                return out;
            }
            "/manifest.json" => {
                return self.conditional(req, &self.manifest_unconf, &self.manifest_unconf_etag, JSON_TYPE, STATIC_CACHE);
            }
            "/config-key" => {
                // The current X25519 public key (base64) so /configure (or den-app) can seal the config to it.
                // 404 when sealed configs are disabled (no key configured) — the page then keeps plaintext.
                let pub_key = self.deps.seal_keyring.as_ref().map(|k| k.current_pub_base64()).unwrap_or_default();
                if !pub_key.is_empty() {
                    // ETag over the key JSON so a rotated key isn't masked by a stale cache and If-None-Match
                    // yields 304, consistent with the other cacheable routes.
                    //
                    // The epoch rides along: /configure stamps it into a new config as `ep`, so a link built after
                    // CONFIG_EPOCH is bumped is not refused by it. The page fetches with cache: "no-cache", so
                    // a browser revalidates (a 304 while nothing changed) rather than keeping a pre-bump epoch.
                    let body = marshal(&json!({"epoch": self.deps.config_epoch, "key": pub_key}));
                    return self.conditional(req, &body, &etag_for(&body), JSON_TYPE, KEY_CACHE);
                }
                // The epoch rides the 404 too: a plaintext link built while CONFIG_EPOCH > 0 must still carry
                // it, or it would be refused the moment it was made. Matches den-subtitles and den-reel.
                return write_json(404, &json!({"epoch": self.deps.config_epoch, "error": "no_key"}), NO_STORE);
            }
            _ => {}
        }

        let parts = split_path(&req.path); // ["<config>", "stream"|"play"|"manifest.json", ...]
        // /p/<ticket> exists only while tickets are on; without them it stays the 404 it always was.
        if parts.len() == 2 && parts[0] == TICKET_ROUTE && self.tickets.is_some() {
            return self.handle_ticket_play(req, &parts[1]).await;
        }
        let config_blob = parts.first().map_or("", String::as_str);
        match at(&parts, 1) {
            "manifest.json" => {
                let Some(config) = self.open_config(config_blob) else { return err_out(400, "bad_config") };
                let body = marshal(&build_manifest(Some(&config)));
                self.conditional(req, &body, "", JSON_TYPE, CONFIGURED_MANIFEST_CACHE)
            }
            "stream" => self.handle_stream(req, config_blob, &parts).await,
            "play" => self.handle_play(req, config_blob, &parts).await,
            "account" => self.handle_account(req, config_blob).await,
            _ => err_out(404, "not_found"),
        }
    }

    // --- stream lists -----------------------------------------------------------------------------------

    async fn handle_stream(self: &Arc<Self>, req: &Req, config_blob: &str, parts: &[String]) -> Out {
        let Some(sid) = parse_stream_id(at(parts, 2), at(parts, 3)) else { return err_out(400, "bad_id") };
        let mut out = self.handle_stream_for(req, config_blob, &sid).await;
        out.add("vary", PLAYABLE_HEADER);
        out
    }

    async fn handle_stream_for(self: &Arc<Self>, req: &Req, config_blob: &str, sid: &StreamId) -> Out {
        let start = Instant::now();
        let origin = self.public_origin(req);
        // audit #7 (collision-resistant key) + #8 (origin part) + #16 (key off the raw blob, decode later).
        let mut cache_key = format!("list:{}:{}:{}", key_hash(config_blob), key_hash(&origin), stream_cache_id(sid));
        // A warm hit serves the list without opening the config, so a list den-remux built for a scoped config
        // must never be what a request without its key hits. Its own key keeps the two apart.
        if self.remux_authorized(req) {
            cache_key.push_str(":remux");
        }
        // A list ranked for a browser's report is another list: cached apart, and the response says it varies.
        let (client, client_key) = client_playable(req);
        if !client_key.is_empty() {
            cache_key.push_str(":playable:");
            cache_key.push_str(&client_key);
        }
        let list_key = ListKey { config_blob, sid, origin: &origin, cache_key: &cache_key };

        // ?debug=1 — "where did this list go", answered exactly instead of guessed from a log line.
        //
        // Bypasses the CACHE deliberately: a cached entry carries no accounting, so answering from one would
        // return a body with nothing in it, and the result is not written back either — a hand-run diagnostic
        // should pay for itself rather than displace a good entry.
        //
        // It does NOT bypass singleflight; it uses a key of its own. Sharing the normal key would hand a
        // debug request whatever an in-flight plain build produced (no accounting), while having no key at
        // all meant every concurrent debug request ran its own scrape AND its own debrid cache-check fan-out.
        //
        // And it is PACED, because collapsing concurrent requests does nothing about sequential ones — a
        // single-connection `while true; do curl …?debug=1; done` was still an unbounded stream of scrapes
        // and cache-check batches. The host limiter in scrape.go paces the INDEXERS and there is no
        // equivalent anywhere on the debrid side, so a debug loop puts unpaced repeated checkcached batches
        // on a debrid account, and a throttled debrid reads back to a viewer as "this release does not exist".
        //
        // The limiter is consulted AFTER the config is validated, and under one shared key. Both matter: an
        // undecodable blob must not be able to mint limiter state or spend the allowance, and a per-cache-key
        // bucket gave an attacker a fresh full burst for every distinct blob or episode number — so the
        // ceiling this exists to impose measured exactly zero, while the never-evicted bucket map grew for
        // free. Sharing one bucket means debugging two titles at once contends, which is the right trade for
        // something a person runs by hand.
        //
        // Not a general answer to /stream traffic with a varying config blob, which misses the list cache in
        // the same way. That is pre-existing, is bounded by the indexer limiter, and is a separate question
        // from this branch.
        if is_debug_request(req) {
            let Some(config) = self.open_config(config_blob) else { return err_out(400, "bad_config") };
            if let Some(refused) = self.refuse_scoped(req, &config) {
                return refused;
            }
            let limiter = lock(&self.debug_limiter).clone();
            if !limiter.allow(DEBUG_LIMITER_KEY) {
                let mut out = err_out(429, "debug_rate_limited");
                out.set("retry-after", "10");
                return out;
            }
            let rid = request_id(&req.header("x-request-id"));
            let key = format!("{cache_key}:debug");
            let job = ListJob::new(config, list_key, client, rid);
            let res = self.shared(key, self.clone().build_task(job, true)).await;
            let Some(res) = res else { return err_out(500, "internal") };
            let mut out = Out::new(200);
            if !res.degraded.is_empty() {
                out.set("X-Den-Degraded", &res.degraded);
            }
            out.set("server-timing", &format!("{}, total;dur={}", res.timing, ms_dur(start.elapsed())));
            let (_, _, _, body) = split_cached(&res.value);
            out.set("content-type", JSON_TYPE);
            out.set("cache-control", NO_STORE);
            out.body = body.as_bytes().to_vec();
            return out;
        }

        if let Some(hit) = self.deps.cache.get(&cache_key) {
            // The completeness rides WITH the entry. Shortening the header only on the build meant the very
            // next requester — Stremio races and cancels addon requests, so that happens routinely — was told
            // to hold a knowingly-short list for five minutes and a day on stale-if-error, which is the harm
            // the shortening exists to prevent, defeated one branch over.
            let (complete, fresh_until, etag, body) = split_cached(&hit);
            // Past the stale window a complete list is held only as a last resort: rebuild in the foreground, as on
            // a miss, and fall back to it only if no indexer answers.
            if complete && fresh_until > 0 && now_unix() >= fresh_until + stale_window_for(self.deps.list_ttl).as_secs() as i64 {
                // A useful complete list stays immediately usable throughout its ticket-safe hold. Refresh it
                // behind the response exactly like ordinary SWR; an empty answer still rebuilds in front because
                // stale emptiness is not useful and may hide a newly released title.
                if cached_list_nonempty(body) && SystemTime::now() < unix_time(fresh_until) + self.outage_hold {
                    metrics().list_cache_hit.fetch_add(1, Ordering::Relaxed);
                    metrics().list_cache_stale.fetch_add(1, Ordering::Relaxed);
                    self.rebuild_behind(req, list_key);
                    let (mut body, mut etag) = (Cow::Borrowed(body), Cow::Borrowed(etag));
                    if let Some(relabelled) = stale_body(&body, "stale_list") {
                        etag = Cow::Owned(etag_for(&relabelled));
                        body = Cow::Owned(relabelled);
                    }
                    let mut out = self.conditional(req, &body, &etag, JSON_TYPE, &self.stale_list_cache);
                    out.set("X-Den-Degraded", "stale_list");
                    out.set("server-timing", &format!("cache;desc=stale_list, total;dur={}", ms_dur(start.elapsed())));
                    return out;
                }
                metrics().list_cache_miss.fetch_add(1, Ordering::Relaxed);
                return self.last_resort(req, start, list_key, Held { fresh_until, etag, body }).await;
            }
            let mut header = self.list_cache.as_str();
            let mut served = "cache;desc=hit";
            metrics().list_cache_hit.fetch_add(1, Ordering::Relaxed);
            let (mut body, mut etag) = (Cow::Borrowed(body), Cow::Borrowed(etag));
            if !complete {
                header = &self.partial_list_cache;
            } else if fresh_until > 0 && now_unix() >= fresh_until {
                metrics().list_cache_stale.fetch_add(1, Ordering::Relaxed);
                // Stale but complete: answer now from what we have and refresh behind the reply. Only
                // COMPLETE lists get this — serving a knowingly-short one past its own expiry is the harm
                // the branch above exists to prevent, and a longer life is the last thing it should get.
                header = &self.stale_list_cache;
                served = "cache;desc=stale";
                self.rebuild_behind(req, list_key);
                if let Some(relabelled) = stale_body(&body, "") {
                    etag = Cow::Owned(etag_for(&relabelled));
                    body = Cow::Owned(relabelled);
                }
            }
            let mut out = self.conditional(req, &body, &etag, JSON_TYPE, header);
            out.set("server-timing", &format!("{served}, total;dur={}", ms_dur(start.elapsed())));
            return out;
        }

        // Miss: decode now (#16 — a warm hit never pays decode/validate).
        metrics().list_cache_miss.fetch_add(1, Ordering::Relaxed);
        let Some(config) = self.open_config(config_blob) else { return err_out(400, "bad_config") };
        // Only the miss path needs this: a warm hit is keyed on the blob, and nothing builds a list for a
        // scoped blob to hit.
        if let Some(refused) = self.refuse_scoped(req, &config) {
            return refused;
        }

        // Detach the shared build from the request: a client disconnect (Stremio routinely races and cancels
        // addon requests) must not cancel the scrape mid-flight and let an empty list get cached and served
        // to every follower. The build runs on a task of its own, which a dropped request does not abort.
        let rid = request_id(&req.header("x-request-id"));
        let job = ListJob::new(config, list_key, client, rid);
        let res = self.shared(cache_key.clone(), self.clone().build_task(job, false)).await;
        match res {
            Some(res) => self.write_built(req, start, &res),
            None => err_out(500, "internal"),
        }
    }

    /// The future of one shared build: everything it needs is owned, so it can outlive the request that started it.
    fn build_task(self: Arc<Self>, job: ListJob, debug: bool) -> impl Future<Output = BuildResult> + Send + 'static {
        async move {
            let deadline = Self::deadline_in(self.deps.scrape_timeout + LIST_BUILD_SLACK);
            self.build_stream_list(deadline, &job, debug).await
        }
    }

    /// Go `sf.Do`: concurrent callers of one key share one run of the build and its result. The build runs on a task
    /// of its own, so a caller that goes away leaves it running for the others; `None` is a build that panicked, which
    /// the caller answers as a 500, where Go's singleflight re-raises it into the request's recover.
    async fn shared<F>(self: &Arc<Self>, key: String, build: F) -> Option<Arc<BuildResult>>
    where
        F: Future<Output = BuildResult> + Send + 'static,
    {
        let mut rx = {
            let mut flights = lock(&self.flights);
            match flights.get(&key) {
                Some(rx) => rx.clone(),
                None => {
                    let (tx, rx) = watch::channel(None);
                    flights.insert(key.clone(), rx.clone());
                    let done = FlightDone { inner: self.clone(), key };
                    tokio::spawn(async move {
                        let _done = done;
                        let res = build.await;
                        let _ = tx.send(Some(Arc::new(res)));
                    });
                    rx
                }
            }
        };
        loop {
            if let Some(res) = rx.borrow().clone() {
                return Some(res);
            }
            if rx.changed().await.is_err() {
                return rx.borrow().clone();
            }
        }
    }

    /// lastResort answers a request for a complete list that is past its stale window: a foreground build, as on
    /// a miss, unless no indexer answers it — then the held list, marked X-Den-Degraded: stale_list and kept by
    /// the client for a minute. An empty list in an outage reads as "nothing to play"; the last good one plays.
    ///
    /// Only within outageHold of the list's freshness, which keeps every ticket in it alive for a viewing. After
    /// such a build the key cools off for rebuildCooloff, during which the held list is served without scraping.
    async fn last_resort(self: &Arc<Self>, req: &Req, start: Instant, key: ListKey<'_>, held: Held<'_>) -> Out {
        let ListKey { config_blob, sid, cache_key, .. } = key;
        let Held { fresh_until, etag, body } = held;
        let Some(config) = self.open_config(config_blob) else { return err_out(400, "bad_config") };
        if let Some(refused) = self.refuse_scoped(req, &config) {
            return refused;
        }
        let now = Instant::now();
        let usable = SystemTime::now() < unix_time(fresh_until) + self.outage_hold;
        let serve_held = |timing: &str| -> Out {
            let ago = SystemTime::now().duration_since(unix_time(fresh_until)).unwrap_or_default();
            log_limited(
                "stale-list",
                &format!(
                    "{} {}: no indexer answered; serving the last complete list, {} past its freshness",
                    sid.kind,
                    sid.imdb,
                    go_duration(Duration::from_secs(ago.as_secs()))
                ),
            );
            // The held body says what it was when built; served now, it is stale, and says so. A new body is a new
            // ETag, so a client revalidating the fresh list is not told its copy still stands.
            let (mut held, mut held_etag) = (Cow::Borrowed(body), Cow::Borrowed(etag));
            if let Some(relabelled) = stale_body(body, "stale_list") {
                held_etag = Cow::Owned(etag_for(&relabelled));
                held = Cow::Owned(relabelled);
            }
            let mut out = self.conditional(req, &held, &held_etag, JSON_TYPE, &self.stale_list_cache);
            out.set("X-Den-Degraded", "stale_list");
            out.set(
                "server-timing",
                &format!("{timing}cache;desc=stale_list, total;dur={}", ms_dur(start.elapsed())),
            );
            out
        };
        if usable && lock(&self.rebuilds).in_outage(cache_key, now) {
            return serve_held("");
        }
        let (client, _) = client_playable(req);
        let rid = request_id(&req.header("x-request-id"));
        let job = ListJob::new(config, key, client, rid);
        let res = self.shared(cache_key.to_owned(), self.clone().build_task(job, false)).await;
        let Some(res) = res else { return err_out(500, "internal") };
        if res.degraded == "indexers" && usable {
            lock(&self.rebuilds).mark_outage(cache_key, now, now + REBUILD_COOLOFF);
            return serve_held(&format!("{}, ", res.timing));
        }
        self.write_built(req, start, &res)
    }

    /// writeBuilt answers with a list built for this request (or for the build it joined).
    fn write_built(&self, req: &Req, start: Instant, res: &BuildResult) -> Out {
        // A follower of a shared build reports the build it was handed — which is the work its answer cost.
        let (_, _, etag, body) = split_cached(&res.value);
        // A degraded build is deliberately not cached server-side, "so the next request retries instead of
        // serving the blip for the whole TTL" — and then the same body went out with `max-age=300,
        // stale-if-error=86400`, which URLSession's shared cache honours. The guard was defeated one layer
        // down, on the only client that matters: an outage's empty list stuck on the device for five minutes,
        // and up to a day on any later error.
        let cache_header = if !res.degraded.is_empty() {
            NO_STORE
        } else if !res.complete {
            // The SAME lesson one line up, for the partial case: the server held this list for a minute and
            // then told the client to keep it for five, with stale-if-error for a day. A list knowingly
            // missing an indexer's releases must not outlive its short server-side life on the device.
            &self.partial_list_cache
        } else {
            &self.list_cache
        };
        let mut out = self.conditional(req, body, etag, JSON_TYPE, cache_header);
        // Signal a degraded build so the app can say "sources temporarily unavailable" rather than treating
        // an empty list as "no results" (a total indexer/cache-check outage otherwise looks identical).
        if !res.degraded.is_empty() {
            out.set("X-Den-Degraded", &res.degraded);
        }
        out.set("server-timing", &format!("{}, total;dur={}", res.timing, ms_dur(start.elapsed())));
        out
    }

    /// rebuildBehind refreshes an expired list alongside the stale answer, which is written by the caller the
    /// moment this returns.
    ///
    /// Deduped through the same singleflight key the foreground build uses, so a burst of viewers arriving on
    /// an expired title produces one scrape — and a foreground build already in flight absorbs this one rather
    /// than racing it.
    ///
    /// The work runs detached from the request, on a task of its own: the request is over by the time it
    /// matters. The config is decoded on this path rather than on the hit path, because a warm hit must keep
    /// paying nothing for decode/validate — this runs only on the rare stale hit.
    fn rebuild_behind(self: &Arc<Self>, req: &Req, key: ListKey<'_>) {
        let ListKey { config_blob, cache_key, .. } = key;
        let budget = self.deps.scrape_timeout + LIST_BUILD_SLACK;
        let now = Instant::now();
        // Booked BEFORE the task exists, and before the config is decoded, so a flood of stale hits costs
        // one map lookup each instead of a task each.
        let (gen, booked) = lock(&self.rebuilds).book_rebuild(cache_key, now, now + budget);
        if !booked {
            return;
        }
        // Until the task exists and owns the release, THIS function owns it — including if something
        // between here and the spawn panics. A slot lost here is lost for the life of the process, reported by
        // nothing; eight of those and no title ever refreshes in the background again.
        let mut release =
            RebuildRelease { inner: self.clone(), key: cache_key.to_owned(), gen, cooloff: None };
        let Some(config) = self.open_config(config_blob) else {
            // It decoded when the entry was built. If it no longer does, or the install has since been revoked,
            // serving stale is all we can do — and a revoked install's play URLs in it are refused anyway.
            return;
        };
        let (client, _) = client_playable(req);
        let rid = request_id(&req.header("x-request-id"));
        let this = self.clone();
        let job = ListJob::new(config, key, client, rid);
        let flight_key = cache_key.to_owned();
        spawn_background("stale list rebuild", async move {
            // The booking is released from the guard, so a panic cannot strand the key permanently.
            let build = this.clone().build_task(job, false);
            let res = this.shared(flight_key, build).await;
            // A degraded build caches nothing, so the entry is still stale and the next request would book
            // another rebuild at once. Hold the key until there is some prospect of a different answer.
            if res.is_some_and(|r| !r.degraded.is_empty()) {
                release.cooloff = Some(Instant::now() + REBUILD_COOLOFF);
            }
            drop(release);
        });
        // The guard moved into the task; nothing is left to release here.
    }

    // --- building a list --------------------------------------------------------------------------------

    /// buildStreamList scrapes → cache-checks → ranks → serializes, caches the framed entry (see joinCached).
    ///
    /// `debug` is false on every normal request. When it is set, the accounting rides along in the response body and
    /// the result is NOT cached — a debug build is a diagnostic, not an entry other viewers should be served.
    async fn build_stream_list(self: &Arc<Self>, deadline: TokioInstant, job: &ListJob, debug: bool) -> BuildResult {
        let mut dbg = debug.then(RankDebug::default);
        let opts = RankOpts { client: job.client.as_ref(), check_cache: true, rid: &job.rid };
        let list = self.rank_list(deadline, &job.config, &job.sid, dbg.as_mut(), opts).await;
        // A movie's list answers the availability route's question too, so a title someone opened needs no
        // check of its own there. Not from a degraded build, which knows nothing either way, nor a debug one, nor
        // one ranked for a browser, whose cap may have kept different releases.
        if job.sid.kind == "movie" && list.degraded.is_empty() && dbg.is_none() && job.client.is_none() {
            self.record_list_verdict(&format!("{}{}", verdict_prefix(&job.config), job.sid.imdb), &list);
        }
        self.finish_stream_list(job, dbg, list).await
    }

    /// rankList scrapes, cache-checks and ranks — for client, when a browser reported what it plays. Nothing here
    /// changes anything on the debrid: the cache check is a read. Both answers are cached on their own (each
    /// indexer's in scrapeAllCached, each held release in the stores), so ranking the same title another way asks
    /// nobody again.
    pub(crate) async fn rank_list(
        self: &Arc<Self>,
        deadline: TokioInstant,
        config: &Config,
        sid: &StreamId,
        dbg: Option<&mut RankDebug>,
        opts: RankOpts<'_>,
    ) -> RankedList {
        let RankOpts { client, check_cache, rid } = opts;
        let q = ScrapeQuery {
            kind: sid.kind.clone(),
            imdb: sid.imdb.clone(),
            season: sid.season,
            episode: sid.episode,
            has_ep: sid.has_ep,
            rid: rid.to_owned(),
        };
        let mut phase = Instant::now();
        let scrapers: Vec<Arc<dyn Scraper>> = (self.deps.make_scrapers)(config).into_iter().map(Arc::from).collect();
        let scraped = scrape_covered(
            &scrapers,
            &q,
            self.deps.scrape_timeout,
            Some(&*self.deps.cache),
            self.deps.list_ttl,
        )
        .await;
        let (mut seeds, scrape_ok, scrape_complete) = (scraped.seeds, scraped.any_ok, scraped.complete);
        let mut sources = scraped.sources;
        sources.extend(quarantined_reports(config));
        let cov = Coverage { complete: scrape_complete, sources };
        metrics().coverage(&cov.sources);
        let mut timing = format!("scrape;dur={}", ms_dur(phase.elapsed()));

        // The accounting object the rank decision line reads. When the request asked for ?debug=1 it is the SAME
        // object, so ?debug=1's accounting is unchanged; otherwise this is the only place the rank decision line
        // gets its drop reasons and picks from, and nothing is serialised to a client.
        let has_dbg = dbg.is_some();
        let mut own_dbg = RankDebug::default();
        let log_dbg: &mut RankDebug = match dbg {
            Some(d) => d,
            None => &mut own_dbg,
        };
        // Recorded HERE, before anything trims it. Taken after the two filters below instead, the count read
        // as "this is all the indexers had" while both of them had already removed releases that appear in no
        // drop tally either — on an RD-only install that is most of the list, which is exactly the confusion
        // this route exists to end.
        if has_dbg {
            log_dbg.deduped = seeds.len() as i64;
        }

        // Cap the seed set before the cache-check fan-out: a misbehaving/hostile indexer returning thousands
        // of tiny stream objects would otherwise mean hundreds of concurrent outbound debrid requests. The
        // cap is well above any real title's stream count, so it rarely bites — but when it does it now keeps
        // the most promising releases rather than the first ones the scrape happened to return.
        if seeds.len() > MAX_SEEDS {
            log_limited(
                "seed-cap",
                &format!(
                    "{} {}: {} seeds capped to {} (best-scored kept)",
                    sid.kind,
                    sid.imdb,
                    seeds.len(),
                    MAX_SEEDS
                ),
            );
            if has_dbg {
                *log_dbg.dropped_by.entry("seedCap".to_owned()).or_insert(0) += (seeds.len() - MAX_SEEDS) as i64;
            }
            seeds = cap_seeds(&seeds, MAX_SEEDS as i64);
        }

        let mut truth = CacheTruth { holders: HashMap::new(), known: HashMap::new(), complete: true };
        let mut truth_ok = true;
        if check_cache {
            let pool = StorePool { stores: (self.deps.make_stores)(config) };
            let hashes: Vec<String> = seeds.iter().map(|s| s.info_hash.clone()).collect();
            phase = Instant::now();
            (truth, truth_ok) = match timeout_at(deadline, pool.cache_check(&hashes)).await {
                Ok(answer) => answer,
                // The build's own clock ran out: every store would have failed, so nobody could be asked.
                Err(_) => (CacheTruth { holders: HashMap::new(), known: HashMap::new(), complete: false }, false),
            };
            timing.push_str(&format!(", cache-check;dur={}", ms_dur(phase.elapsed())));
            for s in seeds.iter_mut() {
                s.cached = truth.cached(&s.info_hash);
                // Whether that false means "not held" or "nobody could ask" is decided here, once, and carried —
                // rather than being re-guessed by every consumer from a header they may not have. Per HASH, not
                // per request: the checks are batched, so one failed batch leaves 100 releases unknown while the
                // rest of the list is perfectly well known. Stamping the request-wide `truthOK` on all of them
                // asserted the answer for exactly the hashes nobody had an answer for.
                s.cache_known = truth.known(&s.info_hash);
            }
        }
        // A degraded upstream (every indexer failed, or every cache-truth store's check failed) yields a
        // misleading empty/partial list; return it for this request but don't cache it, so the next request
        // retries instead of serving the blip for the whole TTL.
        // Track consecutive total-scrape failures for /health (reset on any successful scrape). The log marks
        // the moment /health changes its answer, once each way, rather than every build that failed: the
        // crossing is exactly one Add landing on the threshold, and the recovery the one Swap that finds the
        // count at or past it.
        if scrape_ok {
            if self.scrape_fails.swap(0, Ordering::Relaxed) >= SCRAPE_FAIL_THRESHOLD {
                emit("health: degraded → ok, an indexer answered again");
            }
        } else if self.scrape_fails.fetch_add(1, Ordering::Relaxed) + 1 == SCRAPE_FAIL_THRESHOLD {
            emit(&format!("health: ok → degraded (indexers): {INDEXERS_DOWN_DETAIL}"));
        }
        if scrape_complete {
            if self.partial_builds.swap(0, Ordering::Relaxed) >= SCRAPE_FAIL_THRESHOLD {
                emit("health: coverage partial → complete, every source answered again");
            }
        } else if self.partial_builds.fetch_add(1, Ordering::Relaxed) + 1 == SCRAPE_FAIL_THRESHOLD {
            emit(&format!("health: coverage complete → partial: {SOURCES_MISSING_DETAIL}"));
        }

        // TOTAL failure and PARTIAL failure are different states and drive different decisions, so they get
        // different variables. Folding them into one turned off the cached-only filter for the whole request
        // on any partial failure — which handed a viewer who asked for "only what plays now" releases the
        // store had definitively said it does not hold. The coarse gate silently won over the per-hash filter
        // that was added in the same commit to make exactly this case work.
        //
        // A cache-truth store being entirely OUT is an outage too, even when the others answer normally:
        // their replies cannot rule anything out on its behalf. Treating that as an ordinary answer let a
        // cachedOnly request return an empty list, with no degraded header, cached for the full TTL.
        let truth_out = check_cache && has_cache_truth(config) && (!truth_ok || !truth.complete());
        // audit #4: with no cache-truth store (RD-only), the cached-only filter would drop everything. Also
        // skip it when the cache-truth stores are unreachable ENTIRELY (don't drop everything on a blip). A
        // partial failure keeps the filter on — the per-hash CacheKnown check inside it drops only what is
        // known not to be held, and keeps what nobody could ask about.
        let eff_cached_only = check_cache && config.cached_only && has_cache_truth(config) && !truth_out;
        // RD-only: drop releases RD blocks by filename (they'd 404 at resolve).
        if rd_only(config) {
            let before = seeds.len();
            seeds.retain(|s| !real_debrid_blocked(&s.title));
            // The biggest dropper on an RD-only install by far — realDebridBlocked matches web-dl, webrip,
            // bdrip, hdrip and dvdrip as substrings, which is most of what indexers return. Attributed, or
            // ?debug=1 reports a list that shrank for no stated reason.
            if has_dbg && before > seeds.len() {
                *log_dbg.dropped_by.entry("realDebridBlocked".to_owned()).or_insert(0) += (before - seeds.len()) as i64;
            }
        }

        // Expected title, plus a release year for movies → drop torrents mistagged with another title's id.
        // A series' title only sinks a release naming another show (cinemeta.go says why it never drops).
        // Best-effort: a lookup failure just means no year/title filter.
        let mut expected_year: Option<i64> = None;
        let mut expected_title_tokens = HashSet::new();
        let mut show_tokens = HashSet::new();
        if let Some(meta) = &self.deps.meta {
            if let Ok((m, true)) = timeout_at(deadline, meta.lookup(&sid.kind, &sid.imdb)).await {
                if sid.kind == "series" {
                    if !m.title.is_empty() {
                        show_tokens = title_tokens(&m.title);
                    }
                } else {
                    if m.year != 0 {
                        expected_year = Some(m.year);
                    }
                    if !m.title.is_empty() {
                        expected_title_tokens = title_tokens(&m.title);
                    }
                }
            }
        }

        // Probe facts are ranking inputs, not decorations. Hydrate every locally-known fact before the
        // client compatibility cost is computed; the old order ranked first and discovered the fact only
        // afterward, so a warm probe could not affect the list it was attached to.
        self.hydrate_probe_facts(&mut seeds, Some(sid));
        // A store's own file list, cached from an earlier resolve attempt, corrects a pack-sized episode for
        // free — and does not depend on the probe fan-out's own resolve succeeding (see hydrateKnownSizes).
        self.hydrate_known_sizes(config, &mut seeds, Some(sid));
        // Whatever neither pass above corrected keeps the indexer's number only where the title itself backs
        // it as this episode's; otherwise it becomes a labelled pack rather than a wrong plain size.
        hydrate_pack_only_sizes(&mut seeds, Some(sid));
        let episode = sid.has_ep.then_some([sid.season, sid.episode]);
        let candidates = seeds.len() as i64;
        let rank_start = Instant::now();
        let ranked = rank_streams_debug(
            &seeds,
            RankFilters {
                exclude_cam: config.filters.exclude_cam,
                resolutions: config.filters.resolutions.clone(),
                prefer_resolution: config.filters.prefer_resolution.clone(),
                hdr_only: config.filters.hdr_only,
                min_seeders: config.filters.min_seeders,
                max_size_gb: config.filters.max_size_gb,
                exclude_regex: config.filters.exclude_regex.clone(),
                cached_only: eff_cached_only,
                result_cap: config.result_cap,
                expected_year,
                expected_title_tokens,
                episode,
                show_tokens,
                client: client.cloned(),
            },
            Some(&mut *log_dbg),
        );
        log_rank_decision(sid, candidates, &ranked, log_dbg, rank_start.elapsed(), rid);

        // Degraded is judged on what is actually SERVED, not on what was scraped.
        //
        // A partial cache-check failure is only a problem if it touched a release the viewer will see. Counted
        // over all 500 seeds instead, a failed batch covering releases that every filter would have dropped
        // still marked the response degraded — which means not cached, which means the next /stream re-runs
        // the whole eight-second scrape. That is a user-visible cost paid for a fact about nothing.
        let unchecked = if check_cache && has_cache_truth(config) {
            ranked.iter().filter(|s| !s.cache_known).count()
        } else {
            0
        };
        let degraded_reason = if !scrape_ok {
            "indexers"
        } else if truth_out || unchecked > 0 {
            "cache-check"
        } else {
            ""
        };
        if unchecked > 0 {
            log_limited(
                "cache-check-partial",
                &format!(
                    "{} {}: {} of {} served releases could not be cache-checked",
                    sid.kind,
                    sid.imdb,
                    unchecked,
                    ranked.len()
                ),
            );
        }

        // Where a list is lost. An empty answer has half a dozen possible authors — no indexer had it, a
        // filter removed everything, the cache-only gate dropped an uncached set — and they are
        // indistinguishable in the reply. This is the line that separates them, and the one that ended the
        // guessing about an episode torrentio was serving 50 results for.
        //
        // EMPTY only. The condition used to also fire on `len(ranked) < len(seeds)`, which is true whenever
        // the result cap trims — twenty of them, by default, on any popular title — so a line written for
        // the case worth investigating printed on very nearly every request and buried itself. "Some were
        // dropped" is now a counter on /metrics and an exact answer on ?debug=1; this stays for the one
        // outcome that is genuinely worth a line in the log.
        if ranked.is_empty() {
            let year = expected_year.map_or_else(|| "<nil>".to_owned(), |y| y.to_string());
            log_limited(
                "empty-list",
                &format!(
                    "{} {}: scraped {} → ranked {} (cachedOnly={} year={} filters: res={:?} minSeed={} maxGB={} cam={} hdr={})",
                    sid.kind,
                    sid.imdb,
                    seeds.len(),
                    ranked.len(),
                    eff_cached_only,
                    year,
                    config.filters.resolutions,
                    config.filters.min_seeders.unwrap_or(0),
                    config.filters.max_size_gb.map_or_else(|| "<nil>".to_owned(), |n| n.to_string()),
                    config.filters.exclude_cam,
                    config.filters.hdr_only
                ),
            );
        }
        RankedList {
            ranked,
            truth,
            client: client.cloned(),
            degraded: degraded_reason.to_owned(),
            complete: scrape_complete,
            coverage: cov,
            timing,
        }
    }

    /// finishStreamList probes the top of a ranked list, serialises it and caches it — the half of a build only a
    /// stream list needs.
    async fn finish_stream_list(self: &Arc<Self>, job: &ListJob, dbg: Option<RankDebug>, list: RankedList) -> BuildResult {
        let ListJob { config, config_blob, sid, origin, cache_key, .. } = job;
        let RankedList { ranked, truth, client, degraded: degraded_reason, complete: scrape_complete, coverage, mut timing } =
            list;
        let degraded = !degraded_reason.is_empty();
        // Ask the top few releases what they actually contain. After ranking, so the probe follows the order
        // the viewer will see; before serialisation, so a cached probe rides along in this same response.
        let phase = Instant::now();
        if self.probe_top_for_client(config, &ranked, sid, &truth, client.as_ref()) {
            timing.push_str(&format!(", probe;dur={}", ms_dur(phase.elapsed())));
        }

        let play_url = self.play_urls(config, config_blob, origin);
        let out: Vec<StreamOut> = ranked.iter().map(|s| to_stremio_stream(s, sid, &*play_url)).collect();
        // A list missing one indexer's releases is worth serving and worth caching — just not for as long
        // as a complete one. Refusing to cache it at all put the full scrape and a fresh debrid fan-out on
        // every single request whenever one upstream was flaky, which is a worse answer than the slightly
        // short list it was protecting against.
        let mut ttl = self.deps.list_ttl;
        if !scrape_complete {
            // Capped by the configured freshness for the same reason the header is: a knowingly-short list
            // must not outlive a complete one, which the flat 60s did whenever the TTL was set below it.
            ttl = PARTIAL_LIST_TTL.min(self.deps.list_ttl);
        }
        // The entry outlives its freshness so the hit path can answer from it while a rebuild runs, and fall back
        // to it through an indexer outage (lastResort) — but only when it is COMPLETE. A partial list expires
        // exactly when it says it does: it is already knowingly short, and the one thing it must not get is a
        // longer life.
        let now = SystemTime::now();
        let mut fresh_until = 0;
        let mut hold = ttl;
        if scrape_complete {
            fresh_until = (now + ttl).duration_since(UNIX_EPOCH).map_or(0, |d| d.as_secs() as i64);
            hold = ttl + stale_window_for(ttl).max(self.outage_hold);
        }
        let cached = !degraded && dbg.is_none();
        let whole_seconds = |t: SystemTime| UNIX_EPOCH + Duration::from_secs(t.duration_since(UNIX_EPOCH).map_or(0, |d| d.as_secs()));
        let env = AnswerEnvelope {
            v: ANSWER_ENVELOPE_VERSION,
            answer_kind: decide_answer_kind(out.len() as i64, scrape_complete, &degraded_reason).to_owned(),
            degraded: degraded_reason.clone(),
            generated_at: Some(whole_seconds(now)),
            expires_at: cached.then(|| whole_seconds(now + ttl)),
            coverage,
        };
        let body = marshal(&StreamsResponse { streams: out, debug: dbg, den: env });
        let etag = etag_for(&body);
        let value = join_cached(scrape_complete, fresh_until, &etag, &body);
        if degraded {
            metrics().build_degraded.fetch_add(1, Ordering::Relaxed);
        } else if !scrape_complete {
            metrics().build_partial.fetch_add(1, Ordering::Relaxed);
        } else {
            metrics().build_ok.fetch_add(1, Ordering::Relaxed);
        }
        // A debug build carries accounting in its body and must not become the entry every other viewer is
        // served — nor displace a good one.
        if cached {
            if !scrape_complete {
                log_limited(
                    "partial-list",
                    &format!(
                        "{} {}: an indexer did not answer; caching this list for {} only",
                        sid.kind,
                        sid.imdb,
                        go_duration(ttl)
                    ),
                );
            }
            self.deps.cache.put(cache_key, &value, hold);
        }
        BuildResult { value, degraded: degraded_reason, complete: scrape_complete, timing }
    }

    /// playURLs is how one stream list names its play URLs: a ticket per release while tickets are on, all
    /// expiring together, else the legacy config segment plus token.
    ///
    /// Only a list build calls this, and a scoped config is refused before any list is built, so the read-only
    /// config a browser holds never mints a ticket.
    fn play_urls(self: &Arc<Self>, config: &Config, config_blob: &str, origin: &str) -> Box<dyn Fn(&PlayTarget) -> String + Send> {
        let origin = origin.to_owned();
        let Some(_) = &self.tickets else {
            let blob = config_blob.to_owned();
            return Box::new(move |t| format!("{origin}/{blob}/play/{}", encode_play_token(t)));
        };
        let exp = SystemTime::now() + self.deps.play_ticket_ttl;
        let (this, config) = (self.clone(), config.clone());
        Box::new(move |t| match &this.tickets {
            Some(tickets) => format!("{origin}/{TICKET_ROUTE}/{}", tickets.mint(&config, t, exp)),
            None => String::new(),
        })
    }

    // --- probing ----------------------------------------------------------------------------------------

    /// Go `hydrateProbeFacts`: attaches the track details already known for these releases. Only the CACHE is
    /// consulted on the request path — a map lookup, no network.
    pub(crate) fn hydrate_probe_facts(&self, streams: &mut [RawStream], sid: Option<&StreamId>) {
        hydrate_probe_facts(Some(&*self.deps.cache), streams, sid);
        let hits = streams.iter().filter(|s| s.probe.is_some()).count();
        if hits > 0 {
            metrics().probe_cache_hit.fetch_add(hits as i64, Ordering::Relaxed);
        }
    }

    /// hydrateKnownSizes corrects a stream's size from what a store already knows about the file it
    /// resolves to — read off a torrent's own file list, which a store caches while picking that file for
    /// /play or a cache check, never a fresh network call of its own. It runs on every request, not just the
    /// probe's topN frontier, and does not wait on the probe fan-out's own resolve to succeed: the Fauda S1E3
    /// install's TorBox mylist kept answering "no usable answer" to that resolve, so the ranged-GET probe
    /// never warmed, while an EARLIER attempt had already gone far enough to list the pack's files and cached
    /// them. This reads that cache. The probe is still the only source of track/language facts, so it keeps
    /// running regardless of what this corrects.
    ///
    /// Movie-only guard, same reasoning as applyProbedSize: there is no pack for a movie's title size to have
    /// been confused with, so a movie is never given a lookup at all.
    pub(crate) fn hydrate_known_sizes(&self, config: &Config, streams: &mut [RawStream], sid: Option<&StreamId>) {
        let Some(sid) = sid.filter(|sid| sid.has_ep) else { return };
        let mut pool: Option<StorePool> = None;
        for s in streams.iter_mut() {
            if s.info_hash.is_empty() || s.pack_size_bytes.is_some() || s.probe.as_ref().is_some_and(|p| p.size_bytes > 0) {
                // A ranged probe measured what the playback URL actually serves, so it is more authoritative
                // than an older store file listing even when their difference was too small to name a pack.
                continue;
            }
            let pool = pool.get_or_insert_with(|| StorePool { stores: (self.deps.make_stores)(config) });
            let target = ResolveTarget {
                info_hash: s.info_hash.clone(),
                file_idx: s.file_idx,
                season: Some(sid.season),
                episode: Some(sid.episode),
                ..Default::default()
            };
            if let Some(known) = known_file_size(pool, &target) {
                crate::probe_fanout::apply_known_size(s, known);
            }
        }
    }

    /// probeTopForClient: the half of probing that decides what to probe, and starts it behind the reply. It reports
    /// whether probing is on at all, so Server-Timing names the phase only when there was one.
    pub(crate) fn probe_top_for_client(
        self: &Arc<Self>,
        config: &Config,
        streams: &[RawStream],
        sid: &StreamId,
        truth: &CacheTruth,
        client: Option<&ClientPlayable>,
    ) -> bool {
        // Opt-in: no client, no probing. Probing costs a debrid RESOLVE per release, so it must never happen
        // by accident — a caller that hasn't asked for it (a test, an embedder) gets the old behaviour
        // exactly, and the stream list is built without touching the debrid account at all.
        if self.deps.probe_client.is_none() {
            return false;
        }
        let frontier: HashSet<usize> = match client {
            Some(_) => browser_probe_candidates(streams, config, client),
            None => (0..streams.len().min(PROBE_TOP_N)).collect(),
        };
        for (i, s) in streams.iter().enumerate() {
            if !frontier.contains(&i) {
                if s.cached && s.probe.is_none() {
                    metrics().torbox_avoid("probe_frontier");
                }
                continue;
            }
            if s.probe.is_none() && s.cached && !s.info_hash.is_empty() {
                metrics().probe_cache_miss.fetch_add(1, Ordering::Relaxed);
            }
        }
        let pending = select_probe_jobs(streams, config, Some(sid), truth, client);
        if !pending.is_empty() {
            self.probe_behind(config, pending);
        }
        true
    }

    /// probeBehind warms the probe cache after the reply has gone out.
    ///
    /// Its own task, not the request's: the request is over, so inheriting it would cancel every probe
    /// immediately. Deduped through the handler's singleflight so a title several clients ask for at once is
    /// probed once, not once per request.
    pub(crate) fn probe_behind(self: &Arc<Self>, config: &Config, jobs: Vec<ProbeJob>) {
        let this = self.clone();
        let config = config.clone();
        spawn_background("probe fan-out", async move {
            let deadline = TokioInstant::now() + PROBE_BUDGET;
            let pool = Arc::new(StorePool { stores: (this.deps.make_stores)(&config) });
            let config = Arc::new(config);
            // Gentle on the debrid account: a handful of resolves, not a burst.
            let slots = Arc::new(Semaphore::new(3));
            let mut running = tokio::task::JoinSet::new();
            for job in jobs {
                let Ok(slot) = slots.clone().acquire_owned().await else { break };
                let (this, pool, config) = (this.clone(), pool.clone(), config.clone());
                running.spawn(async move {
                    let _slot = slot;
                    // Per job, and not only on the task above: each job runs on its OWN task, so a recover on the
                    // parent catches nothing that happens here.
                    let what = format!("probe {}", crate::stores::short_hash(&job.target.info_hash));
                    let run = tokio::spawn({
                        let (this, pool, config) = (this.clone(), pool.clone(), config.clone());
                        async move { this.run_probe_job(deadline, &pool, &config, job).await }
                    });
                    if let Err(e) = run.await {
                        if e.is_panic() {
                            recover_background(&what, &*e.into_panic());
                        }
                    }
                });
            }
            while running.join_next().await.is_some() {}
        });
    }

    async fn run_probe_job(&self, deadline: TokioInstant, pool: &StorePool, config: &Config, job: ProbeJob) {
        self.probe_flight
            .run(&format!("probe:{}", job.key), async {
                let resolved = match timeout_at(deadline, pool.resolve_cached_only(&job.target, &job.holders)).await {
                    Ok(r) => r,
                    Err(_) => Err(StoreError::Cancelled),
                };
                let link = match resolved {
                    Ok(link) if !link.is_empty() => link,
                    other => {
                        // Silent until now: a resolve failure here left no trace anywhere, so a release
                        // whose correction never arrived was indistinguishable from one nobody had viewed
                        // yet (den-scout#23) — this is exactly what Fauda S1E3 hit, repeatedly, with
                        // nothing in the log to say so. Key is a fixed literal, never the hash: logLimited
                        // keys a condition, not an instance.
                        let why = match other {
                            Err(e) => e.to_string(),
                            Ok(_) => "<nil>".to_owned(),
                        };
                        log_limited(
                            "probe-resolve-failed",
                            &format!(
                                "probe could not resolve {} to read its size/tracks: {why}",
                                crate::stores::short_hash(&job.target.info_hash)
                            ),
                        );
                        return;
                    }
                };
                let Some(client) = &self.deps.probe_client else { return };
                let probed = match timeout_at(deadline, crate::probe::probe_tracks(client, &link)).await {
                    Ok(r) => r,
                    Err(_) => Err("context deadline exceeded".to_owned()),
                };
                // The link was minted for this probe and is exactly what /play would mint again a moment
                // later, so it is kept rather than thrown away. Only when the read got an answer: a
                // container this cannot parse is still a file being served, but a refused or unreachable
                // link is a failure, and a failure is never remembered.
                if probed.is_ok() || probed.as_ref().is_err_and(|e| e == crate::probe::ERR_NO_TRACKS) {
                    self.links.put(&link_memo_key(config, &job.target), &link, false, SystemTime::now());
                }
                if let Ok(p) = probed {
                    self.deps.cache.put(&job.key, &marshal(&p), PROBE_TTL);
                }
            })
            .await;
    }

    // --- /play ------------------------------------------------------------------------------------------

    async fn handle_play(self: &Arc<Self>, req: &Req, config_blob: &str, parts: &[String]) -> Out {
        // GET only — HEAD is refused rather than answered.
        //
        // A HEAD here would have to either resolve (the whole point of not allowing it: an uncached release
        // is ADDED by that resolve) or be answered from the read-only path `?probe=1` uses. The second was
        // the tempting option and it is worse than useless: what a caller wants from this route is the
        // `location` of a freshly-minted link, and a HEAD that starts nothing cannot produce one — so it
        // would spend up to three upstream reads per prefetch to return a header no client can play from.
        //
        // Nothing legitimate issues it: Stremio asks for the list, and Den's AVPlayer follows the 302 with a
        // GET. What does issue HEAD is link prefetchers and unfurlers, which is precisely the traffic that
        // must not reach a debrid account.
        let start = Instant::now();
        if let Some(refused) = begin_play(req, start) {
            return refused;
        }
        let mut phases = String::new();
        let mut out = self.handle_legacy_play(req, config_blob, parts, &mut phases).await;
        stamp_play(&mut out, start, &phases);
        out
    }

    async fn handle_legacy_play(self: &Arc<Self>, req: &Req, config_blob: &str, parts: &[String], phases: &mut String) -> Out {
        // While tickets are on, this route only serves stream lists cached from before them, and a client can
        // hold one for a day on stale-if-error. LEGACY_PLAY_UNTIL is when that allowance ends.
        if self.tickets.is_some() && self.deps.legacy_play_until.is_some_and(|until| SystemTime::now() >= until) {
            return err_out(403, "legacy_play_closed");
        }
        let Some(target) = decode_play_token(at(parts, 2)) else { return err_out(400, "bad_token") };
        let Some(config) = self.open_config(config_blob) else { return err_out(400, "bad_config") };
        if let Some(refused) = self.refuse_scoped(req, &config) {
            return refused;
        }
        if self.tickets.is_some() && !self.legacy_play_noted.swap(true, Ordering::Relaxed) {
            emit(
                "serving a legacy /<config>/play URL while play tickets are on — a client still holds a \
                 stream list from before them (logged once per process)",
            );
        }
        self.resolve_play(req, &config, &target, phases).await
    }

    /// handleTicketPlay is GET /p/<ticket>. The ticket stands in for both the config segment and the token of
    /// the legacy route, and what it opens to goes through the same resolve.
    async fn handle_ticket_play(self: &Arc<Self>, req: &Req, ticket: &str) -> Out {
        let start = Instant::now();
        if let Some(refused) = begin_play(req, start) {
            return refused;
        }
        let mut phases = String::new();
        let mut out = match self.tickets.as_ref().map(|t| t.open(ticket, SystemTime::now())) {
            // 410, not 400: the ticket was good and has lapsed. What the client holds is an old stream list, not
            // a wrong one, and fetching the list again gets it fresh tickets.
            Some(Err(TicketError::Expired)) => err_out(410, "ticket_expired"),
            // A revoked install gets the answer a ticket that does not open gets.
            Some(Ok((config, target))) if self.admit_install(&config) => {
                self.resolve_play(req, &config, &target, &mut phases).await
            }
            _ => err_out(400, "bad_ticket"),
        };
        stamp_play(&mut out, start, &phases);
        out
    }

    /// resolvePlay is the play route's work once a request is admitted, whichever route admitted it.
    async fn resolve_play(self: &Arc<Self>, req: &Req, config: &Config, target: &PlayTarget, phases: &mut String) -> Out {
        let start = Instant::now();
        let pool = StorePool { stores: (self.deps.make_stores)(config) };
        let resolve_deadline = Self::deadline_in(RESOLVE_BUDGET);
        let rid = request_id(&req.header("x-request-id"));
        let rt = ResolveTarget {
            info_hash: target.info_hash.clone(),
            file_idx: target.file_idx,
            season: target.season,
            episode: target.episode,
            release_size: target.release_size,
            title: target.title.clone(),
            imdb: target.imdb.clone(),
            // ?prefetch=1 — nobody is waiting on this add, so it may not spend the adds kept for Play.
            prefetch: req.query_get("prefetch") == "1",
            ..Default::default()
        };
        let status_budget = self.status_budget();

        // ?probe=1 — answer, don't act. Asking /play for an uncached release is what QUEUES it, so a client
        // polling this URL to render a progress bar was adding the torrent again on every poll. TorBox allows
        // 60 adds an hour; a single three-minute wait spent thirty-six of them, and the account was throttled
        // out of playing anything at all. A probe reports what is true right now and starts nothing.
        if req.query_get("probe") == "1" {
            // On the STATUS budget, not the resolve one. A probe answers and starts nothing, and the client
            // polls it on a two-second cadence — so it belongs under the ceiling statusBudget's own comment
            // describes ("far under the resolve budget so a wait answers promptly instead of hanging the
            // poll"), rather than pinning a task and a connection for forty-five seconds against a slow
            // debrid.
            //
            // It is NOT free, and an earlier note here saying "up to three upstream calls, all reads" is no
            // longer true of either half. Since the readiness enquiry started asking any store that holds an
            // id — the fix for a probe that answered 404 for a release /play served — a poll against a
            // Real-Debrid holding walks that store's read chain: 2 calls when the release is ready (info,
            // unrestrict/link — it was 4 until resolveExisting stopped re-making a selection RD had already
            // made) and 3 while it is still downloading (info, selectFiles, info), at a two-second cadence,
            // with a state-changing selectFiles on each downloading poll and a fresh unrestricted link minted
            // and discarded on every ready poll. /play walks the same chain on the same cadence, so
            // this is the cost of the two routes agreeing rather than a new class of work — but it is real,
            // and it is recorded in FOLLOWUP.md rather than left as a comment claiming otherwise.
            //
            // The three SHARE this budget rather than each getting a slice, and the honest consequence is that
            // a status read slow enough to spend all eight seconds leaves the cache check a dead clock, so
            // the probe answers 503 "could not ask" for the length of that wait. That is the safe direction —
            // it never claims an absence — and slicing here is not obviously better: the pool already slices
            // per store, and stacking a second division on top is how an earlier fix left every store a budget
            // too short to answer in. Left alone deliberately; revisit if a real account is seen hitting it.
            let probe_deadline = Self::deadline_in(status_budget);
            return self.handle_probe(config, &pool, &target.info_hash, &rt, &rid, probe_deadline).await;
        }

        // A link minted in the last few minutes for this same file is served as it is: no status read, no
        // resolve, nothing upstream at all. Before this, the stream list's probe minted a link and threw it
        // away, and the play that followed a second later minted the same file again.
        //
        // `fresh=1` is the client saying the link it last got would not open. That link is dropped rather
        // than kept as a fallback — it is the only evidence there is about it, and the evidence is bad — and
        // the resolve below mints its replacement. Dropped up front, not only overwritten on success, so a
        // resolve that fails this time does not leave the dead link to be served on the next plain request.
        let memo_key = link_memo_key(config, &rt);
        if req.query_get("fresh") == "1" {
            self.links.forget(&memo_key);
        } else if let Some(hit) = self.links.get(&memo_key, SystemTime::now()) {
            // A link /play minted was checked before it was served; one the probe minted was not. So the check
            // runs once per link rather than once per play. A remembered link that fails it is dropped and a
            // fresh one minted below, not answered as dead — it may only be the one that went stale.
            let verdict = if hit.checked { LinkVerdict::Playable } else { self.verify_link(&pool, &rt, &hit.link).await };
            if verdict == LinkVerdict::Playable || verdict == LinkVerdict::Unverified {
                if verdict == LinkVerdict::Playable {
                    self.links.mark_checked(&memo_key, &hit.link);
                }
                log_limited(
                    "play-memo-hit",
                    &play_decision(
                        "play",
                        "served",
                        &format!("link minted in the last {}", go_duration(LINK_MEMO_TTL)),
                        "",
                        start.elapsed(),
                        &rid,
                        &rt,
                    ),
                );
                return write_play_redirect(&hit.link);
            }
            self.links.forget(&memo_key);
        }

        // Held already? Then try to serve it before asking whether it is downloading.
        //
        // The status read below is a mylist round trip on TorBox and an info call on Real-Debrid, and for a
        // release the account has finished it only ever answers "not downloading" — after which the resolve
        // runs anyway. Every warm play paid for that answer. HoldingServices is a cache read per store, so a
        // release no store has an id for pays nothing here and goes down the old path untouched.
        //
        // NoAdd is what makes it safe to put this ahead of the status read, and the stores enforce it rather
        // than this caller promising it: a remembered id that turns out stale is exactly when a resolve would
        // reach for createtorrent, and a NoAdd resolve refuses instead. Any failure falls through to the
        // sequence below unchanged, so what this step cannot do costs one read-only attempt and nothing more.
        //
        // Skipped while /play is answering "downloading" for this file. A download in progress is when this
        // step always fails — no link yet — and each poll would pay a link request on top of the one status
        // read it needs. playPendingTTL lets it lapse soon after the polls stop.
        //
        // On a slice of the resolve clock, and HALF the status budget rather than all of it, because what
        // follows still needs its share: the ordinary status read, then — on the path about to add — the
        // escalated read, which declines unless twice its own budget remains. With shipped constants that is
        // 45 − 4 − 8 = 33 s left against the 32 it needs; a full 8 s here would leave 29 and switch the
        // escalation off for exactly the slow accounts it exists for.
        let pending_key = play_pending_key(&memo_key);
        if self.deps.cache.get(&pending_key).is_none() {
            let holders = pool.holding_services(&rt);
            if !holders.is_empty() {
                let read_only = ResolveTarget { no_add: true, ..rt.clone() };
                let held_start = Instant::now();
                let resolved = timeout(self.warm_resolve_budget(), pool.resolve_cached_only(&read_only, &holders)).await;
                *phases = format!("held;dur={}, ", ms_dur(held_start.elapsed()));
                match resolved {
                    Ok(Ok(link)) => return self.serve_play_link(&pool, &rt, &memo_key, link).await,
                    other => {
                        let why = match other {
                            Ok(Err(e)) => e.to_string(),
                            _ => "context deadline exceeded".to_owned(),
                        };
                        log_limited(
                            "play-held-miss",
                            &play_decision(
                                "play",
                                "fallback",
                                &format!("held resolve failed ({why}), asking status"),
                                "",
                                start.elapsed(),
                                &rid,
                                &rt,
                            ),
                        );
                    }
                }
            }
        }

        // Already known to be downloading? Answer from the status alone. A waiting client polls this URL for
        // the whole fetch, and a full resolve is ~3 upstream calls (cache miss → add → link), so re-running it
        // per poll is how an account gets itself throttled — and a throttled 5xx is exactly what a client
        // cannot distinguish from a real answer.
        //
        // On the STATUS budget, like the probe route and the post-failure retry below. This one had kept the
        // 45-second resolve budget, so the read that exists to answer a poll PROMPTLY could hold the poll for
        // forty-five seconds against a slow debrid — the thing statusBudget's own comment forbids. Two of its
        // three siblings already agreed; this was the third.
        let status_deadline = Self::deadline_in(status_budget);
        let (status, ok, unknown) = pool.status_detail(&rt, Some(status_deadline)).await;
        if ok {
            return self.write_play_queued(&pending_key, &rt, &rid, start.elapsed(), &status);
        }

        // "Nobody is fetching it" and "I could not find out in time" are different answers, and only the
        // first may lead to an add.
        //
        // pool.Status reports both as ok=false, so a TIMED-OUT read fell through to the resolve below, which
        // queues the torrent — one that was very often already downloading. TorBox's Status makes two upstream
        // calls, one of them the account listing this package measures at ~13 MB on a 2,000-torrent account,
        // so an 8-second budget is genuinely reachable there. Measured against a 9-second listing: an add that
        // the old 45-second budget did not make. Opening a ten-episode season on a large account could spend
        // ten of the fifty hourly adds re-queueing torrents already in flight.
        //
        // Moving the budget back is not the fix — it was shortened deliberately, because this read answers a
        // poll on a two-second cadence and must not hold it for forty-five seconds. So the path that is ABOUT
        // TO SPEND AN ADD gets ONE more short read, on the budget below.
        //
        // Not the resolve budget, which is what the first version of this did and was worse than the bug it
        // fixed. The resolve clock starts at the top of this function, so a fresh resolveBudget for
        // the escalated read always outlives it: the read burned the whole 45 s, the clock was already dead when
        // ResolvePreferring got it, the add became impossible, and the release was NEVER queued. A permanently
        // slow listing turned into a permanent 404 with nothing downloading, which is not a trade, it is a
        // regression. Measured: 404 after ~61 s where the old code played in 8 s.
        //
        // So it is parented to the resolve clock, which makes outliving the resolve budget impossible by
        // construction, it is short, and it is skipped entirely unless enough budget remains to do the add
        // afterwards. If it still cannot tell, the resolve proceeds and may add — because a duplicate add is a
        // bounded, self-healing cost, and never starting the download is not.
        // Asked of the POOL, not inferred from this function's clock. The pool gives each store a slice of the
        // budget, so a store can time out while the pool still returns long before the caller's deadline —
        // "did my whole budget elapse" was false exactly when a store had failed to answer, which is the case
        // this branch exists for. The pool knows which store ran out of time, so it is the one that says.
        //
        // Only the stores that could not answer are re-asked. Going back through the whole pool re-ran a
        // question the other stores had already answered definitively — three store reads instead of one on a
        // three-account install, per poll, on a path that is already struggling.
        if !unknown.is_empty() {
            if let Some(slow_deadline) =
                escalated_status_ctx(Some(resolve_deadline), TokioInstant::now(), status_budget)
            {
                let (status, ok, _) = status_detail_among(&unknown, &rt, Some(slow_deadline)).await;
                if ok {
                    log_limited(
                        "play-slow-status",
                        &play_decision(
                            "play",
                            "fallback",
                            &format!("status needed longer than {} to answer", go_duration(status_budget)),
                            "",
                            start.elapsed(),
                            &rid,
                            &rt,
                        ),
                    );
                    return self.write_play_queued(&pending_key, &rt, &rid, start.elapsed(), &status);
                }
            }
        }
        drop(unknown);

        // Which services already hold it, so the resolve starts with one that can serve now rather than one
        // that would have to download it. Only worth asking with more than one account configured — with one
        // there is nothing to choose between, and it would be a wasted upstream call on every play.
        //
        // BUDGETED, like every other read on this path. Its only product is an ORDERING, so it must never be
        // able to cost the resolve it is ordering — and handed the bare resolve clock it could: a slow
        // checkcached ate what the two status reads left, ResolvePreferring then ran on an expired clock,
        // every store failed instantly, nothing was queued, and a healthy release came back 404.
        //
        // That is the regression escalatedStatusCtx exists to prevent, one line further down and not caught
        // by it, because the escalation was given a carved-out slice and this call was not. Measured with
        // shipped constants on a two-account install: 404 after 53.0s with the check holding the clock for
        // 21.0s, against 302 in 195µs when it answers at once.
        //
        // Failure here is NOT free, and an earlier note claiming it was is corrected rather than kept. Losing
        // the ordering means the pool tries configured order, so an account that would have served the
        // release from cache can be tried after one that has to fetch it — measured at one add spent and a
        // download the viewer waits on for a file another of their accounts already held.
        //
        // Capped anyway, because the two costs are not comparable: losing the ordering costs at most one add
        // on a release that still plays, while losing the RESOLVE costs a 404 on a live release AND an add
        // per configured store. The trade is a real one and it has a window — with shipped constants the
        // check is reached at t=24s with 21s left, so a checkcached answering between 8s and ~21s used to
        // order correctly and now does not. Under 8s both behave the same; over ~21s the old code produced
        // the 404 this cap exists to remove.
        //
        // The FREE half first, and unconditionally: a store that already has an id for this release can serve
        // it with no add at all, and finding that out is a cache read per store.
        //
        // It is also better evidence than the cache check. TorBox's checkcached reports what TORBOX has, not
        // what this account has; Real-Debrid contributes no cache truth whatsoever — its CacheCheck answers
        // all-false by design, so HeldBy can never name it; and Premiumize drops out whenever its own check
        // is throttled or times out. Ordering on the cache check alone therefore made a release the SECOND
        // account already held invisible, so configured order won and TorBox — always first — was asked to
        // resolve, which means createtorrent.
        //
        // Measured on a TorBox + Real-Debrid install with RD already holding the torrent: /play posted
        // createtorrent to TorBox, spent one of the fifty and left a duplicate torrent there — then the
        // probe, which consults Status first, found TorBox fetching and told a client that had been answering
        // "ready" for three polls to keep waiting for a download it never needed. With this line the same
        // play resolves from RD in four upstream calls instead of six, spends nothing, and the probe stays
        // ready throughout.
        //
        // This is the enquiry handleProbe was given for exactly this reason; it was never applied to the
        // route that pays for being wrong.
        let mut preferred = pool.holding_services(&rt);
        if config.debrid.len() > 1 {
            let check_deadline = Self::deadline_in(status_budget).min(resolve_deadline);
            if let Ok((play_truth, _)) = timeout_at(check_deadline, pool.cache_check(&[target.info_hash.clone()])).await {
                preferred.extend(play_truth.held_by(&target.info_hash));
            }
        }
        let resolve_start = Instant::now();
        let resolved = match timeout_at(resolve_deadline, pool.resolve_preferring(&rt, &preferred)).await {
            Ok(r) => r,
            // The resolve clock ran out before any store answered: named as SCOUT's, because that is whose clock
            // ran out, exactly as the pool names a store that reports the cancellation.
            Err(_) => Err(StoreError::ScoutSide(StoreUnavailableError {
                service: pool.stores.first().map_or(DebridService::TorBox, |s| s.service()),
                reason: "the resolve budget was spent before a store could answer".to_owned(),
                status: 0,
            })),
        };
        phases.push_str(&format!("resolve;dur={}, ", ms_dur(resolve_start.elapsed())));
        let err = match resolved {
            Ok(link) => return self.serve_play_link(&pool, &rt, &memo_key, link).await,
            Err(err) => err,
        };
        // Queued is not dead. An uncached release is added to the debrid by the Resolve above and then
        // takes minutes to land; answering 404 for that made the client remember a perfectly good release
        // as dead (and every uncached one below it, since they all fail the same way). 202 says "coming",
        // with whatever progress the store actually reports — the client polls this same URL and plays
        // when it turns into the usual 302.
        //
        // A FRESH clock: the resolve above may have failed by exhausting `resolveBudget` (a slow add on
        // an uncached release is exactly that case), and reusing a spent clock would turn every such
        // wait into a 404.
        let (status, ok, _) = pool.status(&rt, Some(Self::deadline_in(status_budget))).await;
        if ok {
            return self.write_play_queued(&pending_key, &rt, &rid, start.elapsed(), &status);
        }
        // An add scout already sent is not a refusal at all — the release is being fetched, by us, right
        // now. Answering 503 made the client tell the viewer their debrid was refusing AND stop trying
        // other sources, for a release scout had queued moments earlier. 202 is simply what is true.
        if matches!(err, StoreError::AddInFlight) {
            log_limited(
                "play-add-in-flight",
                &play_decision("play", "fallback", "add already in flight", "", start.elapsed(), &rid, &rt),
            );
            return self.write_play_queued(&pending_key, &rt, &rid, start.elapsed(), &StoreStatus::default());
        }
        // TorBox parked the add in its own queue (queued_id, not torrent_id — see errAddQueued). Also
        // "coming", not dead: before this decode existed, the same answer fell through to the final 404
        // below and the client blacklisted a release TorBox was about to fetch.
        if matches!(err, StoreError::AddQueued) {
            // Only TorBox hands back queued_id/errAddQueued at all (see storeCanceller's sibling comment on
            // addMagnet), so naming it here is accurate rather than a guess — and the client needs it to
            // show "Queued at TorBox" rather than a bare, serviceless "queued".
            log_limited(
                "play-add-queued",
                &play_decision(
                    "play",
                    "fallback",
                    "queued behind the account's slot limit",
                    DebridService::TorBox.as_str(),
                    start.elapsed(),
                    &rid,
                    &rt,
                ),
            );
            let queued = StoreStatus {
                state: FETCH_QUEUED.to_owned(),
                service: Some(DebridService::TorBox),
                ..StoreStatus::default()
            };
            return self.write_play_queued(&pending_key, &rt, &rid, start.elapsed(), &queued);
        }
        // A refusal SCOUT made — its hourly allowance — is not the debrid refusing, and must not be
        // reported as one. errScoutSide was added to keep these apart in the refusal memory, and then the
        // route went on saying "torbox" anyway: the app told the viewer the debrid was refusing while
        // TorBox was answering perfectly well. Still a 503, because it is still "not now, try again", but
        // named as ours.
        // A prefetch held back so a viewer pressing Play still has adds. Its own code, because the client
        // treats it differently from a spent budget: a queued download pauses until Retry-After and resumes,
        // and nothing about the release is held against it.
        if is_play_reserve(&err) {
            log_limited(
                "play-reserved",
                &play_decision(
                    "play",
                    "refused",
                    &format!("prefetch refused: {}", scout_side_reason(&err)),
                    "",
                    start.elapsed(),
                    &rid,
                    &rt,
                ),
            );
            return write_unavailable(
                scout_busy_for(&pool, true),
                &json!({"detail": scout_side_reason(&err), "error": "reserved_for_play"}),
            );
        }
        if matches!(err, StoreError::ScoutSide(_)) {
            log_limited(
                "play-scout-busy",
                &play_decision("play", "refused", &scout_side_reason(&err), "", start.elapsed(), &rid, &rt),
            );
            return write_add_budget_spent(
                scout_busy_for(&pool, false),
                &json!({"detail": scout_side_reason(&err), "error": "scout_busy"}),
            );
        }
        // A debrid that throttled or faulted is not a dead release, and answering 404 made the two
        // identical: the client fell through every remaining source collecting the same non-answer, then
        // waited indefinitely on a download nothing had started. 503 says whose problem it is.
        if let Some(unavailable) = as_unavailable(&err) {
            log_limited(
                "play-store-unavailable",
                &play_decision(
                    "play",
                    "refused",
                    &unavailable.reason,
                    unavailable.service.as_str(),
                    start.elapsed(),
                    &rid,
                    &rt,
                ),
            );
            return write_unavailable(
                STORE_REFUSAL_WAIT,
                &json!({"error": "store_unavailable", "service": unavailable.service.as_str()}),
            );
        }
        // The client reads this 404 as "still fetching" and polls for as long as the viewer will sit
        // there, so it is worth saying which of the two happened: no store could add it, and no store
        // admits to downloading it either. A wait with nothing behind it is the one case a spinner
        // cannot distinguish from a slow release.
        log_limited(
            "play-dead",
            &play_decision(
                "play",
                "skipped",
                "no store resolved it and none reports a download",
                "",
                start.elapsed(),
                &rid,
                &rt,
            ),
        );
        err_out(404, "dead_link")
    }

    /// servePlayLink answers /play with a link a store has just minted: checked, remembered, redirected.
    ///
    /// A link that fails its check is answered exactly as a release no store could resolve — 404 dead_link —
    /// because to the client it is the same thing: this source will not play, try the next one. It is not
    /// remembered, and anything remembered under the key goes too.
    ///
    /// The check runs on its own clock rather than the resolve clock. A resolve that took most of its
    /// budget would otherwise leave the check nothing, and a check with no time is a check that cannot answer;
    /// linkCheckTimeout bounds it on its own.
    ///
    /// A movie link that serves a file far smaller than the release gets one more try: see relistMovie. What that
    /// try mints is served only if it passes its own check. Otherwise the first link is served as it was before
    /// the release size was known — a collection's film, or a store with no file list to pick from, is still
    /// the file that was asked for, and a size alone is not proof enough to refuse it.
    async fn serve_play_link(&self, pool: &StorePool, rt: &ResolveTarget, memo_key: &str, mut link: String) -> Out {
        let mut verdict = self.verify_link(pool, rt, &link).await;
        if verdict == LinkVerdict::WrongFile {
            self.links.forget(memo_key);
            verdict = LinkVerdict::Unverified;
            let (relisted, v) = self.relist_movie(pool, rt).await;
            if v == LinkVerdict::Playable || v == LinkVerdict::Unverified {
                link = relisted;
                verdict = v;
            }
        }
        if verdict == LinkVerdict::Broken {
            self.links.forget(memo_key);
            return err_out(404, "dead_link");
        }
        self.links.put(memo_key, &link, verdict == LinkVerdict::Playable, SystemTime::now());
        write_play_redirect(&link)
    }

    /// relistMovie re-resolves a movie whose link served another file in the release, this time with the
    /// torrent's file list, and checks what that mints.
    ///
    /// TorBox resolves a movie without listing files, so the indexer's fileIdx — a position in the torrent — goes
    /// out raw as TorBox's file id, and on a torrent TorBox lists in another order that id names a sample or an
    /// extra: a 522 KB file for a 16 GB remux, with a 302 and no error. Listing on every movie play would cost a
    /// mylist call in front of each warm link, and nearly every movie carries fileIdx 0, so the list is fetched
    /// only here, once the check has shown the raw position was wrong. selectFileID then maps the position
    /// through the list, with its guard against a position that lands on something not feature-sized.
    ///
    /// Read-only (NoAdd) and only against the stores that hold the torrent: the link that failed came from one,
    /// and nothing about a wrong file is a reason to buy the torrent again. The list lands in TorBox's resolve
    /// entry, so later plain resolves of this hash take the fast path with it and pick the right file without
    /// listing again — and the file's size is known from it, which is what lets the check below judge a
    /// collection's film against its own listed size rather than against the whole pack's.
    ///
    /// On its own slice of the clock, not the resolve clock: this runs after a resolve that may have
    /// spent most of that clock, and a re-resolve on an expired clock fails instantly and turns a playable
    /// release into a 404. statusBudget is the same ceiling the other single-torrent reads on this route use; one
    /// mylist and one requestdl fit well inside it.
    ///
    /// Any failure is linkBroken, on which servePlayLink serves the first link as it did before this existed.
    async fn relist_movie(&self, pool: &StorePool, rt: &ResolveTarget) -> (String, LinkVerdict) {
        let start = Instant::now();
        let rid = String::new();
        let listed = ResolveTarget { no_add: true, list_files: true, ..rt.clone() };
        let resolved = match timeout(self.status_budget(), pool.resolve_cached_only(&listed, &pool.holding_services(rt))).await {
            Ok(r) => r,
            Err(_) => Err(StoreError::Cancelled),
        };
        let link = match resolved {
            Ok(link) => link,
            Err(e) => {
                log_limited(
                    "play-relist-failed",
                    &play_decision(
                        "play",
                        "fallback",
                        &format!("re-resolve with a file list failed: {e}"),
                        "",
                        start.elapsed(),
                        &rid,
                        rt,
                    ),
                );
                return (String::new(), LinkVerdict::Broken);
            }
        };
        let verdict = self.verify_link(pool, rt, &link).await;
        if verdict == LinkVerdict::Playable || verdict == LinkVerdict::Unverified {
            log_limited(
                "play-relist-served",
                &play_decision(
                    "play",
                    "served",
                    "re-resolve with a file list picked another file",
                    "",
                    start.elapsed(),
                    &rid,
                    rt,
                ),
            );
        }
        (link, verdict)
    }

    /// Go `(*handler).verifyLink`: whether a freshly minted link is the file it should be.
    async fn verify_link(&self, pool: &StorePool, rt: &ResolveTarget, link: &str) -> LinkVerdict {
        let doer = self.deps.probe_client.clone().map(ReqwestDoer);
        crate::linkcheck::verify_link(doer.as_ref().map(|d| d as &dyn Doer), known_file_size(pool, rt), rt, link).await
    }

    /// handleProbe answers "can this play yet?" without changing anything. Same vocabulary as /play so the
    /// client reads one set of statuses: 202 while downloading, 200 once the store holds it, 404 when neither
    /// is true — which here means "nothing has been queued", not "this release is dead".
    async fn handle_probe(
        &self,
        config: &Config,
        pool: &StorePool,
        info_hash: &str,
        rt: &ResolveTarget,
        rid: &str,
        deadline: TokioInstant,
    ) -> Out {
        let start = Instant::now();
        let (status, ok, unknown) = pool.status(rt, Some(deadline)).await;
        if ok {
            return write_queued(rt, rid, start.elapsed(), &status);
        }
        // "Ready" has to mean the ACCOUNT can serve it without queueing anything, which a cache check cannot
        // establish on its own: TorBox's checkcached reports what TorBox has, not what this account has. So
        // the claim is settled by a NoAdd resolve — the stores refuse rather than queue — and a cached-but-
        // not-held release correctly falls through to the "nothing queued" answer instead of promising a
        // playback that would start with a download.
        let (probe_truth, truth_ok) = match timeout_at(deadline, pool.cache_check(&[info_hash.to_owned()])).await {
            Ok(answer) => answer,
            Err(_) => (CacheTruth { holders: HashMap::new(), known: HashMap::new(), complete: false }, false),
        };
        let mut refused_us: Option<StoreUnavailableError> = None;
        // Cached OR already held. The cache check alone was the wrong question, and asking it alone left this
        // enquiry unreachable on a Real-Debrid install: RD publishes no cache API and answers all-false by
        // design, so HeldBy is always empty and ResolveCachedOnly — which refuses outright with no holders —
        // was never given anything to ask. The route then answered 404 "not queued" for a release /play
        // served a 302 for, from the same cache entry.
        //
        // HoldingServices is a cache read per store, so this costs nothing where it changes nothing.
        let mut holders = probe_truth.held_by(info_hash);
        holders.extend(pool.holding_services(rt));
        if !holders.is_empty() {
            let read_only = ResolveTarget { no_add: true, ..rt.clone() };
            let resolved = match timeout_at(deadline, pool.resolve_cached_only(&read_only, &holders)).await {
                Ok(r) => r,
                Err(_) => Err(StoreError::Cancelled),
            };
            match resolved {
                Ok(_) => {
                    log_limited("probe-ready", &play_decision("probe", "served", "ready", "", start.elapsed(), rid, rt));
                    return write_json(200, &json!({"state": "ready"}), NO_STORE);
                }
                // The probe DOES discover refusals — it just used to throw them away, inspecting only `err ==
                // nil` and then relying on the backoff cache to have been written by someone else. Any refusal
                // that reaches here unrecorded fell through to the 404 below, which this file calls a claim
                // rather than a shrug: `errNoFileList` is never recorded at all, and TorBox's warm-entry fast
                // path returns a refusal without recording it, so during a throttle the very URL the client
                // polls to draw its progress bar said "nothing is queued" while /play said the debrid was
                // refusing. Reading the error we already have needs no cache to have been written first.
                Err(err) => refused_us = as_unavailable(&err).cloned(),
            }
        }
        // An add of OURS that is out and unanswered means the release is being fetched right now, and it
        // outranks every refusal below — the same precedence /play gives errAddInFlight, because it is a
        // fact about us rather than about a service.
        //
        // The probe cannot learn it any other way: it never resolves, and all three stores answer a NoAdd
        // target before consulting the marker, so without this branch the state /play reports as 202 reached
        // this route as nothing and fell through to the 404 below — the single failure this route exists to
        // prevent, on the URL the client polls for its progress bar. Real-Debrid and Premiumize have no
        // Status to clear it, so the disagreement stood the full 90s addAttemptTTL.
        //
        // It outranks a rejected key ACROSS stores too — the order ResolvePreferring uses,
        // where `coming` is returned before `refused` so store order cannot decide the verdict.
        //
        // The per-store rule points the other way (a dead key is not a wait, and every store's Resolve puts
        // accountBackedOff above its own addInFlight), and an earlier version of this route applied that rule
        // at pool level. That made a dead key on one account outrank a live add on another: with TorBox's key
        // expired and Real-Debrid fetching, the probe answered 503 naming torbox while /play answered 202
        // downloading. AddInFlight now applies the per-store rule per store, so both hold at once.
        if pool.add_in_flight(info_hash) {
            log_limited(
                "probe-add-in-flight",
                &play_decision("probe", "fallback", "add already in flight", "", start.elapsed(), rid, rt),
            );
            return write_queued(rt, rid, start.elapsed(), &StoreStatus::default());
        }
        // No store with a usable key is fetching it, so a rejected key is now the best explanation there is.
        //
        // Account-level only, deliberately. The per-release backoff belongs below, where RecentRefusal reads
        // it, because that one is an add-path guard a read-only caller is exempt from.
        if let Some((svc, reason)) = pool.account_refusal() {
            log_limited(
                "probe-account-refused",
                &play_decision(
                    "probe",
                    "refused",
                    &format!("account refused: {reason}"),
                    svc.as_str(),
                    start.elapsed(),
                    rid,
                    rt,
                ),
            );
            return write_unavailable(STORE_REFUSAL_WAIT, &json!({"error": "store_unavailable", "service": svc.as_str()}));
        }
        // A refusal SCOUT made, above the store refusals below because it is ours rather than a service's.
        //
        // NOT identical to /play's ranking, and an earlier note here claiming it was is corrected: /play
        // takes the FIRST StoreUnavailableError in store order, scout-side or not, so with every budget
        // spent AND a per-release store refusal already on record the two routes name different causes —
        // probe scout_busy, /play store_unavailable. Both are 503 and the client retries either way, so the
        // difference is in the detail a human reads, not in behaviour. Left as it is: naming scout's own
        // ceiling is the more useful answer of the two, and reproducing /play's first-error-wins ordering
        // here would mean resolving, which this route must not do.
        //
        // The probe had no channel for it at all — recordRefusal excludes
        // errScoutSide by design, so the backoff memory this route reads stays empty and the request fell
        // through every "could not ask" guard to 404 "not queued". Measured with the hourly allowance spent:
        // /play answered 503 scout_busy while ?probe=1 answered 404, for the same release at the same
        // instant, for the rest of the rolling hour.
        if pool.every_add_refused_by_scout() {
            log_limited(
                "probe-scout-busy",
                &play_decision("probe", "refused", "scout's hourly add allowance is spent", "", start.elapsed(), rid, rt),
            );
            return write_add_budget_spent(
                scout_busy_for(pool, false),
                &json!({
                    "detail": "scout's own hourly add budget for this account is spent",
                    "error": "scout_busy",
                }),
            );
        }
        if let Some(refused) = refused_us {
            log_limited(
                "probe-refused",
                &play_decision("probe", "refused", &refused.reason, refused.service.as_str(), start.elapsed(), rid, rt),
            );
            return write_unavailable(
                STORE_REFUSAL_WAIT,
                &json!({"error": "store_unavailable", "service": refused.service.as_str()}),
            );
        }
        // A refusal the store recorded moments ago outranks "nothing queued". Without this the probe reports
        // a throttled account as an absence, which reads as a release nobody can deliver — and the client
        // condemns a perfectly good one. This still matters for a refusal recorded by an EARLIER poll, whose
        // store the probe may not reach again.
        if let Some((svc, reason)) = pool.recent_refusal(info_hash) {
            log_limited(
                "probe-refused",
                &play_decision("probe", "refused", &reason, svc.as_str(), start.elapsed(), rid, rt),
            );
            return write_unavailable(STORE_REFUSAL_WAIT, &json!({"error": "store_unavailable", "service": svc.as_str()}));
        }
        // With the cache check down we do not know whether anything is queued, and 404 "not_queued" is a
        // claim, not a shrug — the client reads it as a release nobody has. Say the store could not be asked.
        if !truth_ok && has_cache_truth(config) {
            log_limited(
                "probe-cache-check-down",
                &play_decision("probe", "degraded", "cache check unavailable", "", start.elapsed(), rid, rt),
            );
            return write_unavailable(STORE_UNANSWERED_WAIT, &json!({"error": "cache_check_unavailable"}));
        }
        // Same rule one step further out: a store that could not answer is not a store saying nothing is
        // queued. 404 here is what makes a client blacklist a release, which is the single failure this route
        // exists to prevent, so an indeterminate read gets the "ask again" answer rather than the claim.
        if unknown {
            log_limited(
                "probe-status-unknown",
                &play_decision("probe", "degraded", "a store could not answer", "", start.elapsed(), rid, rt),
            );
            return write_unavailable(STORE_UNANSWERED_WAIT, &json!({"error": "status_unavailable"}));
        }
        log_limited("probe-not-queued", &play_decision("probe", "skipped", "not queued", "", start.elapsed(), rid, rt));
        err_out(404, "not_queued")
    }

    // --- cancel -----------------------------------------------------------------------------------------

    /// handleCancel is DELETE /p/<ticket> (and, for parity, DELETE /<config>/play/<token>) — den#204. Both
    /// routes open to the same target as their GET siblings and share resolveCancel.
    async fn handle_cancel(self: &Arc<Self>, req: &Req) -> Out {
        let parts = split_path(&req.path);
        if parts.len() == 2 && parts[0] == TICKET_ROUTE && self.tickets.is_some() {
            return self.handle_ticket_cancel(req, &parts[1]).await;
        }
        if parts.len() == 3 && parts[1] == "play" {
            return self.handle_legacy_cancel(req, &parts[0], &parts[2]).await;
        }
        let mut out = err_out(405, "method_not_allowed");
        out.set("allow", "GET, HEAD");
        out
    }

    async fn handle_ticket_cancel(&self, req: &Req, ticket: &str) -> Out {
        match self.tickets.as_ref().map(|t| t.open(ticket, SystemTime::now())) {
            Some(Err(TicketError::Expired)) => err_out(410, "ticket_expired"),
            Some(Ok((config, target))) if self.admit_install(&config) => self.resolve_cancel(req, &config, &target).await,
            _ => err_out(400, "bad_ticket"),
        }
    }

    async fn handle_legacy_cancel(&self, req: &Req, config_blob: &str, token: &str) -> Out {
        if self.tickets.is_some() && self.deps.legacy_play_until.is_some_and(|until| SystemTime::now() >= until) {
            return err_out(403, "legacy_play_closed");
        }
        let Some(target) = decode_play_token(token) else { return err_out(400, "bad_token") };
        let Some(config) = self.open_config(config_blob) else { return err_out(400, "bad_config") };
        if let Some(refused) = self.refuse_scoped(req, &config) {
            return refused;
        }
        self.resolve_cancel(req, &config, &target).await
    }

    /// resolveCancel is the cancel route's work once a target and its config are admitted. It asks every
    /// configured debrid account that can cancel, in configured order, and answers with the first one that has
    /// an opinion about this hash — see storeCanceller's own comment for why only TorBox implements it.
    async fn resolve_cancel(&self, req: &Req, config: &Config, target: &PlayTarget) -> Out {
        let start = Instant::now();
        let rid = request_id(&req.header("x-request-id"));
        let rt = ResolveTarget {
            info_hash: target.info_hash.clone(),
            file_idx: target.file_idx,
            season: target.season,
            episode: target.episode,
            title: target.title.clone(),
            imdb: target.imdb.clone(),
            ..Default::default()
        };
        let reannounce = req.query_get("op") == "reannounce";
        let deadline = Self::deadline_in(self.status_budget());
        let stores = (self.deps.make_stores)(config);
        let mut saw_canceller = false;
        for st in &stores {
            let answered = match timeout_at(deadline, st.cancel(&target.info_hash, reannounce)).await {
                Ok(None) => continue,
                Ok(Some(answer)) => answer,
                Err(_) => Err(StoreError::Cancelled),
            };
            saw_canceller = true;
            let outcome = match answered {
                Ok(outcome) => outcome,
                Err(err) => {
                    if let Some(unavailable) = as_unavailable(&err) {
                        log_limited(
                            "cancel-store-unavailable",
                            &play_decision(
                                "cancel",
                                "refused",
                                &unavailable.reason,
                                unavailable.service.as_str(),
                                start.elapsed(),
                                &rid,
                                &rt,
                            ),
                        );
                        return write_unavailable(
                            STORE_REFUSAL_WAIT,
                            &json!({"error": "store_unavailable", "service": unavailable.service.as_str()}),
                        );
                    }
                    log_limited(
                        "cancel-error",
                        &play_decision("cancel", "degraded", &err.to_string(), "", start.elapsed(), &rid, &rt),
                    );
                    return write_unavailable(STORE_REFUSAL_WAIT, &json!({"error": "store_unavailable"}));
                }
            };
            if outcome.reason == "not_queued" {
                // This account has no opinion; the hash may still belong to another configured account.
                continue;
            }
            return write_cancel_outcome(&rt, &rid, start, &outcome);
        }
        if !saw_canceller {
            log_limited(
                "cancel-unsupported",
                &play_decision("cancel", "skipped", "no configured account supports it", "", start.elapsed(), &rid, &rt),
            );
            return err_out(501, "unsupported");
        }
        log_limited("cancel", &play_decision("cancel", "skipped", "nothing queued", "", start.elapsed(), &rid, &rt));
        err_out(404, "not_queued")
    }

    // --- /account ---------------------------------------------------------------------------------------

    /// handleAccount is GET /<config>/account — den#204: one quiet line of slot and allowance usage per
    /// configured debrid account, in configured order. A non-TorBox account answers with its service name and
    /// nothing else: Real-Debrid and Premiumize publish no comparable facts.
    async fn handle_account(&self, req: &Req, config_blob: &str) -> Out {
        let Some(config) = self.open_config(config_blob) else { return err_out(400, "bad_config") };
        if let Some(refused) = self.refuse_scoped(req, &config) {
            return refused;
        }
        let deadline = Self::deadline_in(self.status_budget());
        let stores = (self.deps.make_stores)(&config);
        let mut out = Vec::with_capacity(stores.len());
        for st in &stores {
            out.push(account_body(deadline, st.as_ref()).await);
        }
        write_json(200, &out, NO_STORE)
    }

    // --- /validate --------------------------------------------------------------------------------------

    async fn handle_validate(&self, req: &Req) -> Out {
        if req.method != Method::POST {
            let mut out = err_out(405, "method_not_allowed");
            out.set("allow", "POST");
            return out;
        }
        #[derive(Deserialize, Default)]
        struct Body {
            #[serde(default)]
            service: Option<String>,
            #[serde(default)]
            token: Option<String>,
        }
        static SHAPE: Shape = Shape::Object(&[("service", &LEAF), ("token", &LEAF)]);
        // The first JSON value in at most maxValidateBody bytes, as Go's decoder over a MaxBytesReader reads it.
        let window = &req.body[..req.body.len().min(MAX_VALIDATE_BODY)];
        let decoded = serde_json::Deserializer::from_slice(window)
            .into_iter::<serde_json::Value>()
            .next()
            .and_then(Result::ok)
            .and_then(|mut v| {
                fold_json_keys(&mut v, &SHAPE);
                serde_json::from_value::<Body>(v).ok()
            });
        let Some(body) = decoded else { return err_out(400, "bad_request") };
        let (service, token) = (body.service.unwrap_or_default(), body.token.unwrap_or_default());
        // Whitelisted the same way validateConfig does it, and the same length ceiling, so this route cannot
        // be used to send arbitrary strings to an upstream.
        let Some(svc) = DebridService::parse(&service).filter(|_| !token.is_empty() && token.len() <= 512) else {
            return err_out(400, "bad_request");
        };

        // NOTHING here logs the token, and no log line names this route with its arguments. The configure
        // page seals the config in the browser, so this is the one request in the system where the server is
        // handed a token in the clear — it is used once, for one upstream read, and not written anywhere.
        let res = match &self.deps.probe_client {
            None => ValidateResult { reason: "validation unavailable".to_owned(), ..Default::default() },
            Some(client) => {
                let limiter = lock(&self.validate_limiter).clone();
                let doer = ReqwestDoer(client.clone());
                match timeout(VALIDATE_BUDGET, validate_debrid_token(&doer, &limiter, svc, &token)).await {
                    Ok(res) => res,
                    Err(_) => ValidateResult { reason: "timed out".to_owned(), ..Default::default() },
                }
            }
        };
        if res.throttled {
            let mut out = write_json(429, &res, NO_STORE);
            out.set("retry-after", "2");
            return out;
        }
        write_json(200, &res, NO_STORE)
    }

    // --- shared plumbing --------------------------------------------------------------------------------

    /// metricsAuthorized reports whether this request may read /metrics: `Authorization: Bearer <token>`, the
    /// token trimmed, the rule every den addon shares. A bare token without the scheme is refused. Compared in
    /// constant time: the token is a shared secret and a timing-variable compare on a route anyone can reach
    /// is a needless gift.
    fn metrics_authorized(&self, req: &Req) -> bool {
        if self.deps.metrics_token.is_empty() {
            return false;
        }
        let header = req.header("authorization");
        let Some(given) = header.strip_prefix("Bearer ") else { return false };
        constant_time_eq(given.trim().as_bytes(), self.deps.metrics_token.as_bytes())
    }

    /// publicOrigin: PUBLIC_BASE_URL when set (audit #8), else forwarded headers / Host.
    fn public_origin(&self, req: &Req) -> String {
        if !self.deps.public_url.is_empty() {
            return self.deps.public_url.trim_end_matches('/').to_owned();
        }
        let mut proto = first_header(req, "x-forwarded-proto");
        if proto.is_empty() {
            proto = if req.https { "https" } else { "http" }.to_owned();
        }
        let mut host = first_header(req, "x-forwarded-host");
        if host.is_empty() {
            host = req.host.clone();
        }
        format!("{proto}://{host}")
    }

    /// conditional serves a cacheable GET with ETag/If-None-Match → 304. etag may be precomputed ("" → hash).
    fn conditional(&self, req: &Req, body: &str, etag: &str, content_type: &str, cache_control: &str) -> Out {
        let etag = if etag.is_empty() { Cow::Owned(etag_for(body)) } else { Cow::Borrowed(etag) };
        let inm = req.header("if-none-match");
        if !inm.is_empty() && etag_matches(&inm, &etag) {
            let mut out = Out::new(304);
            out.set("etag", &etag);
            out.set("cache-control", cache_control);
            return out;
        }
        let mut out = Out::new(200);
        out.set("content-type", content_type);
        out.set("cache-control", cache_control);
        out.set("etag", &etag);
        out.body = body.as_bytes().to_vec();
        out
    }

    /// remuxAuthorized reports whether the request carries den-remux's service key (X-Den-Remux-Key), compared
    /// in constant time. Never true while REMUX_KEY is unset.
    fn remux_authorized(&self, req: &Req) -> bool {
        let key = &self.deps.remux_key;
        let got = req.header("x-den-remux-key");
        !key.is_empty() && !got.is_empty() && constant_time_eq(got.as_bytes(), key.as_bytes())
    }

    /// refuseScoped answers 403 for a scoped config on a route outside its scope — every route that lists or
    /// plays streams — and says whether it did. The one exception is den-remux: with its service key, an
    /// availability-scoped config may list and play, so the browser drives playback through den-remux with the
    /// scoped URL it already holds and never gets a stream-capable config. The resulting tickets stay on the
    /// homelab (den-remux resolves them; the browser gets only its own signed session URLs).
    fn refuse_scoped(&self, req: &Req, config: &Config) -> Option<Out> {
        if config.scope.is_empty() || (config.scope == SCOPE_AVAILABILITY && self.remux_authorized(req)) {
            return None;
        }
        Some(err_out(403, "out_of_scope"))
    }

    /// openConfig is decodeConfig plus admitInstall: every route that takes a config segment opens it here, so a
    /// revoked install is refused exactly as an undecodable config is.
    ///
    /// A warm stream-list hit opens nothing (handleStream), so for a few minutes after a revocation the install
    /// can still read a list it had cached. Every play URL in that list is refused.
    pub(crate) fn open_config(&self, blob: &str) -> Option<Config> {
        let config = crate::config::decode_config(self.deps.seal_keyring.as_deref(), blob)?;
        self.admit_install(&config).then_some(config)
    }

    /// admitInstall applies REVOKED_INSTALLS and CONFIG_EPOCH to a config, whether it came from a config segment
    /// or a play ticket. The log line names the reason. A revoked id is cut to six characters, and the id of an
    /// install that is not revoked is never logged.
    pub(crate) fn admit_install(&self, config: &Config) -> bool {
        if !config.iid.is_empty() && self.deps.revoked_installs.contains(&config.iid) {
            log_limited(
                "install-revoked",
                &format!("config refused: install revoked (iid {}…)", config.iid.chars().take(6).collect::<String>()),
            );
            return false;
        }
        if config.epoch < self.deps.config_epoch {
            log_limited(
                "install-epoch",
                &format!(
                    "config refused: install epoch too old ({} < CONFIG_EPOCH {})",
                    config.epoch, self.deps.config_epoch
                ),
            );
            return false;
        }
        // A scoped config is minted separately from the install and may carry no id. Tickets den-remux is
        // authorized to mint from it preserve that provenance; epoch and named revocation still apply above.
        if self.deps.require_install_id && config.iid.is_empty() && config.scope.is_empty() {
            log_limited("install-no-iid", "config refused: no install id (REQUIRE_INSTALL_ID is on)");
            return false;
        }
        true
    }
}

fn unix_time(secs: i64) -> SystemTime {
    if secs >= 0 {
        UNIX_EPOCH + Duration::from_secs(secs as u64)
    } else {
        UNIX_EPOCH - Duration::from_secs(secs.unsigned_abs())
    }
}

fn first_header(req: &Req, name: &str) -> String {
    let v = req.header(name);
    v.split(',').next().unwrap_or("").trim().to_owned()
}

/// isDebugRequest checks for ?debug=1 without parsing a query string that is almost always absent.
fn is_debug_request(req: &Req) -> bool {
    !req.query.is_empty() && req.query_get("debug") == "1"
}

fn etag_matches(if_none_match: &str, etag: &str) -> bool {
    if if_none_match.trim() == "*" {
        return true;
    }
    if_none_match.split(',').any(|tag| tag.trim() == etag)
}

/// beginPlay is what both play routes do first: refuse any verb but GET (see handlePlay for why HEAD is refused
/// too). Server-Timing is stamped on whatever the route answers, by `stamp_play`.
fn begin_play(req: &Req, start: Instant) -> Option<Out> {
    if req.method == Method::GET {
        return None;
    }
    let mut out = err_out(405, "method_not_allowed");
    out.set("allow", "GET");
    stamp_play(&mut out, start, "");
    Some(out)
}

/// playTiming stamps Server-Timing on whatever /play answers: the route has a dozen exits, and setting the header
/// at each was a dozen chances to forget one.
fn stamp_play(out: &mut Out, start: Instant, phases: &str) {
    out.set("server-timing", &format!("{phases}total;dur={}", ms_dur(start.elapsed())));
}

/// writePlayRedirect is /play's success answer.
///
/// Standard bodyless 302 (audit / AetherEngine fix): explicit Content-Length:0 so the Node/Go layer
/// doesn't send a chunked empty body that strict redirect-followers read as 0 bytes and fail on.
fn write_play_redirect(link: &str) -> Out {
    let mut out = Out::new(302);
    out.set("location", link);
    out.set("cache-control", NO_STORE);
    out.set("content-length", "0");
    out
}

fn write_cancel_outcome(rt: &ResolveTarget, rid: &str, start: Instant, outcome: &crate::stores::CancelOutcome) -> Out {
    match outcome.reason.as_str() {
        "finished" => {
            log_limited("cancel", &play_decision("cancel", "refused", "finished", "", start.elapsed(), rid, rt));
            return err_out(409, "finished");
        }
        "not_ours" => {
            log_limited("cancel", &play_decision("cancel", "refused", "not_ours", "", start.elapsed(), rid, rt));
            return err_out(409, "not_ours");
        }
        _ => {}
    }
    if outcome.reannounced {
        log_limited("cancel", &play_decision("cancel", "served", "reannounced", "", start.elapsed(), rid, rt));
        return write_queued_body(&outcome.status);
    }
    log_limited("cancel", &play_decision("cancel", "served", "cancelled", "", start.elapsed(), rid, rt));
    Out::new(204)
}

/// writeQueued — the "it's coming" answer: 202 plus whatever the store actually reported. `etaSeconds` is
/// omitted rather than guessed when the service doesn't supply one.
pub(crate) fn write_queued(rt: &ResolveTarget, rid: &str, dur: Duration, status: &StoreStatus) -> Out {
    // The wait, as the viewer sees it. A percentage alone can't distinguish a slow fetch from a dead
    // swarm, so the rate is logged beside it — this is the line to read when someone says "it's stuck".
    let rate = status
        .bytes_per_second
        .map_or_else(|| "unknown".to_owned(), |b| format!("{:.1} Mbps", b as f64 * 8.0 / 1e6));
    let eta = status.eta_seconds.map_or_else(|| "none".to_owned(), |e| format!("{e}s"));
    let swarm = if status.seeds.is_some() || status.peers.is_some() {
        format!(", seeds {}, peers {}", optional_count(status.seeds), optional_count(status.peers))
    } else {
        String::new()
    };
    // A waiting client polls every two seconds for the whole fetch, so this is a sample, a line a minute.
    // Progress/rate/eta/swarm are operational, not identity, and stay outside LOG_IDENTITY's gate.
    let reason = format!("{} {:.1}% at {rate}, eta {eta}{swarm}", queued_state(status), status.progress * 100.0);
    log_limited(
        "downloading",
        &play_decision("play", "fallback", &reason, status.service.map_or("", |s| s.as_str()), dur, rid, rt),
    );
    write_queued_body(status)
}

fn optional_count(n: Option<i64>) -> String {
    n.map_or_else(|| "?".to_owned(), |n| n.to_string())
}

/// queuedState — the body's `state`: the store's mapped fetch state, or "downloading" when it named none.
fn queued_state(status: &StoreStatus) -> &str {
    if status.state.is_empty() {
        FETCH_DOWNLOADING
    } else {
        &status.state
    }
}

/// Go `writeQueuedBody`.
pub(crate) fn write_queued_body(status: &StoreStatus) -> Out {
    let mut body = serde_json::Map::new();
    body.insert("state".to_owned(), json!(queued_state(status)));
    body.insert("progress".to_owned(), go_num(status.progress));
    if let Some(service) = status.service {
        body.insert("service".to_owned(), json!(service.as_str()));
    }
    if let Some(seeds) = status.seeds {
        body.insert("seeds".to_owned(), json!(seeds));
    }
    if let Some(peers) = status.peers {
        body.insert("peers".to_owned(), json!(peers));
    }
    if let Some(eta) = status.eta_seconds {
        body.insert("etaSeconds".to_owned(), json!(eta));
    }
    if let Some(bps) = status.bytes_per_second {
        body.insert("bytesPerSecond".to_owned(), json!(bps));
    }
    write_json(202, &serde_json::Value::Object(body), NO_STORE)
}

/// writeAddBudgetSpent answers scout_busy with the add budget's limit and window as the IETF draft RateLimit
/// headers, beside Retry-After: nothing is left, and the budget frees in `wait`.
fn write_add_budget_spent(wait: Duration, body: &serde_json::Value) -> Out {
    let secs = wait_secs(wait);
    let mut out = write_unavailable(wait, body);
    out.set("ratelimit-policy", &format!("\"adds\";q={ADD_BUDGET_LIMIT};w={}", ADD_BUDGET_WINDOW.as_secs()));
    out.set("ratelimit", &format!("\"adds\";r=0;t={secs}"));
    out
}

/// A wait in whole seconds, rounded up, and at least one.
fn wait_secs(wait: Duration) -> u64 {
    (wait.as_nanos().div_ceil(1_000_000_000) as u64).max(1)
}

/// writeUnavailable answers 503 with a Retry-After of at least a second, so a client that honours it waits as long as
/// the refusal behind the answer lasts instead of polling into it.
fn write_unavailable(wait: Duration, body: &serde_json::Value) -> Out {
    let mut out = write_json(503, body, NO_STORE);
    out.set("retry-after", &wait_secs(wait).to_string());
    out
}

/// Go `pool.ScoutBusyFor(prefetch)`: how long until any of the pool's accounts may add again for this intent, for the
/// Retry-After of a scout_busy or reserved_for_play answer.
///
/// CROSS-MODULE NEED: Go asks each store for its `budgetAccount()`, which the frozen `Store` trait has no method for
/// (and `StorePool` lives in `stores/`, which this branch does not edit). Until `Store::budget_account` lands, the
/// answer is "zero", which both routes render as the one-second floor of Retry-After.
fn scout_busy_for(pool: &StorePool, prefetch: bool) -> Duration {
    let _ = (pool, prefetch, budget_account(DebridService::TorBox, ""), global_add_budget());
    Duration::ZERO
}

/// Go `pool.KnownFileSize(rt)`: the first size any store knows for this file.
///
/// CROSS-MODULE NEED: Go's `fileSizer` is an optional interface on the stores; the frozen `Store` trait has no
/// `known_file_size`, so a pool cannot ask (`RealDebridStore::known_file_size` is an inherent method, and the TorBox
/// store's is not written yet). Until it lands this answers "unknown", which is what a store with nothing cached says.
fn known_file_size(pool: &StorePool, rt: &ResolveTarget) -> Option<i64> {
    let _ = (pool, rt);
    None
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
///
/// CROSS-MODULE NEED: Go downcasts the store to `*torBoxStore` and calls `AccountSummary`; the frozen `Store` trait has
/// no such method and `stores/torbox.rs` is not this branch's. Until `Store::account_summary` lands, every account
/// answers with its service name alone, which is also what a failed TorBox read answers.
async fn account_body(_deadline: TokioInstant, st: &dyn Store) -> serde_json::Value {
    let _ = torbox_plan_name;
    json!({"service": st.service().as_str()})
}

