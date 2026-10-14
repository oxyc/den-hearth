//! What the TorBox store takes from the parts of `stores.go` that are not its own: the account identity and the
//! cache-key helpers from the shared head (lines 96-125, 529-775), the add-attempt and refusal bookkeeping that
//! `refusal.rs` ports (lines 780-1560), and the small helpers `mod.rs` holds (`readStoreError`,
//! `storeErrorText`, `redactToken`, `magnetFor`, `torboxFetchState`).
//!
//! These are written out here so this module builds and tests on its own while `refusal.rs` and `mod.rs` are
//! being ported in parallel. The stubbed functions of `refusal.rs` (`note_add_attempt`, `add_outcome_unknown`,
//! `backed_off`, `record_refusal`, the key builders) are called there, not copied. What follows is the rest,
//! with the same names and the same behaviour: once both land, point the `use` lines of `torbox.rs` at the
//! `refusal.rs` and `mod.rs` versions and delete the copies.

use std::collections::{BTreeMap, HashMap};
use std::future::Future;
use std::pin::Pin;
use std::sync::{Mutex, MutexGuard, PoisonError};
use std::task::Poll;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use serde_json::Value;
use tokio::sync::watch;

use crate::cache::{Cache, CacheClass};
use crate::config::DebridService;
use crate::scrape::DoError;
use crate::stores::refusal::{
    account_refused_key, add_outcome_unknown, record_refusal, transport_kind, unknown_outcome_key, ADD_GIVE_UP,
    UNKNOWN_OUTCOME_TTL,
};
use crate::stores::{
    DeadLinkError, StoreError, StoreUnavailableError, ERR_CHECK_FAILED, ERR_TORRENT_GONE, ERR_WOULD_ADD,
    FETCH_DOWNLOADING, FETCH_FAILED, FETCH_FETCHING, FETCH_QUEUED, FETCH_STALLED,
};
use crate::util::{fold_json_keys, go_to_lower, key_hash, Shape};

use super::TORBOX_API;

/// A poisoned lock still holds consistent state here (every edit finishes before anything can panic).
pub(super) fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(PoisonError::into_inner)
}

// --- the cache a store may not have ---

/// A cache that holds nothing, standing for Go's nil `Cache`: reading it misses and writing to it drops.
pub(super) struct NoCache;

impl Cache for NoCache {
    fn get(&self, _key: &str) -> Option<String> {
        None
    }

    fn put(&self, _key: &str, _value: &str, _ttl: Duration) {}
}

/// Go `cachePut(cache, key, "", time.Nanosecond, CacheVolatile)`: drops a key by letting it expire at once.
pub(super) fn clear(cache: &dyn Cache, key: &str) {
    cache.put_class(key, "", Duration::from_nanos(1), CacheClass::Volatile);
}

/// A marker's value when it is live. A cleared key is written as "" and lives for a nanosecond, so a read inside
/// that tick must not take it for a marker.
pub(super) fn live(cache: &dyn Cache, key: &str) -> Option<String> {
    cache.get(key).filter(|v| !v.is_empty())
}

// --- the account identity (stores.go:96-125) ---

pub(crate) const TORBOX_IDENTITY_PREFIX: &str = "torbox-account:v2:";

/// Go `torboxAccountIdentity` is the canonical identity of one TorBox account. The API is part of it: test,
/// proxy and future TorBox-compatible endpoints may legitimately reuse a token string while naming completely
/// different accounts. The v2 marker also makes every old token-only persistent fact miss safely after upgrade
/// rather than being trusted under an arbitrary endpoint.
pub(crate) fn torbox_account_identity(api: &str, token: &str) -> String {
    format!("{TORBOX_IDENTITY_PREFIX}{}:{token}", key_hash(api))
}

/// Go `canonicalTorboxIdentity`: tests and old internal call sites commonly pass a raw token to key builders.
/// Interpret that as the canonical production API, while preserving an identity already assembled by a store.
pub(crate) fn canonical_torbox_identity(identity: &str) -> String {
    if identity.starts_with(TORBOX_IDENTITY_PREFIX) {
        return identity.to_owned();
    }
    torbox_account_identity(TORBOX_API, identity)
}

/// Go `serviceAccountIdentity` normalises generic account-scoped state too. This keeps callers which pass a raw
/// TorBox token on the production API compatible while ensuring a store configured for another API never shares
/// facts with it. Other debrid services have no configurable API identity.
pub(crate) fn service_account_identity(svc: DebridService, identity: &str) -> String {
    if svc == DebridService::TorBox {
        return canonical_torbox_identity(identity);
    }
    identity.to_owned()
}

// --- "this account holds it" (stores.go:529-575, 766-775) ---

/// Go `cachedTTL`: how long "this account holds it" answers for.
///
/// Only a POSITIVE is memoised, and that asymmetry is the whole point. "Held" is stable: a release does
/// not stop being held while a viewer picks it, so the same question asked by /stream, then by ?probe=1,
/// then by /play on a two-account install — three times within seconds, about the hash the viewer just
/// chose — has one answer. "Not held" is the opposite: it becomes "held" the instant a download finishes,
/// and that transition is exactly what the probe route polls to notice. Memoising a negative would make
/// the client wait up to cachedTTL to be told its own download is ready.
pub(crate) const CACHED_TTL: Duration = Duration::from_secs(60);

pub(crate) fn cached_key(svc: DebridService, token: &str, info_hash: &str) -> String {
    format!("{}:cached:{}:{info_hash}", svc.as_str(), key_hash(&service_account_identity(svc, token)))
}

/// Go `knownCached` splits hashes into those already known to be held and those still worth asking about.
pub(crate) fn known_cached(
    cache: Option<&dyn Cache>,
    svc: DebridService,
    token: &str,
    hashes: &[String],
) -> (HashMap<String, bool>, Vec<String>) {
    let mut known = HashMap::with_capacity(hashes.len());
    let Some(cache) = cache else {
        return (known, hashes.to_vec());
    };
    let mut ask = Vec::with_capacity(hashes.len());
    for h in hashes {
        if live(cache, &cached_key(svc, token, h)).is_some() {
            known.insert(h.clone(), true);
            continue;
        }
        ask.push(h.clone());
    }
    (known, ask)
}

/// Go `rememberCached` records the positives from an answer. Negatives are deliberately not recorded.
pub(crate) fn remember_cached(
    cache: Option<&dyn Cache>,
    svc: DebridService,
    token: &str,
    result: &HashMap<String, bool>,
) {
    let Some(cache) = cache else { return };
    for (h, held) in result {
        if *held {
            cache.put_class(&cached_key(svc, token, h), "1", CACHED_TTL, CacheClass::Volatile);
        }
    }
}

/// Go `batchesFailed` reports errCheckFailed only when every batch failed (none returned usable data). A
/// partial failure is not an error — the hashes it could not answer for are simply absent from the map.
pub(crate) fn batches_failed(batch_ok: &[bool]) -> Result<(), StoreError> {
    if batch_ok.iter().any(|ok| *ok) {
        return Ok(());
    }
    Err(dead_link(ERR_CHECK_FAILED))
}

// --- add attempts and the refusal memory (stores.go:800-1560) ---

/// Go `torrentMissTTL`: remembering that the account has NO torrent for a hash, briefly.
///
/// The lookup that answers this fetches the account's ENTIRE torrent list, and only a hit was remembered.
/// So the miss — which is every poll of a release that was never queued — refetched the whole list each
/// time. A client polls /play every couple of seconds for the length of a download, so a ten-minute wait
/// was some three hundred full-account-list fetches, each one growing with the account's history.
///
/// Deliberately short: an add can land at any moment, and Status is what tells the viewer their download
/// is progressing. This only has to collapse a burst of polls, not remember anything for long. A fresh add
/// is unaffected either way, since it writes the id and the id is checked first.
pub(crate) const TORRENT_MISS_TTL: Duration = Duration::from_secs(15);

pub(crate) fn torrent_miss_key(identity: &str, info_hash: &str) -> String {
    format!("torbox:notorrent:{}:{info_hash}", key_hash(&canonical_torbox_identity(identity)))
}

/// Go `addAttemptKey`: an add we SENT but never got an answer to.
pub(crate) fn add_attempt_key(svc: DebridService, token: &str, info_hash: &str) -> String {
    format!("{}:adding:{}:{info_hash}", svc.as_str(), key_hash(&service_account_identity(svc, token)))
}

/// Go `time.Now().Unix()` as the text the markers store.
fn now_unix() -> String {
    SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_secs()).unwrap_or(0).to_string()
}

/// Go `time.Since(time.Unix(at, 0))`; None where `at` is in the future (Go's negative duration).
fn since_unix(at: i64) -> Option<Duration> {
    let then = if at >= 0 {
        UNIX_EPOCH.checked_add(Duration::from_secs(at.unsigned_abs()))?
    } else {
        UNIX_EPOCH.checked_sub(Duration::from_secs(at.unsigned_abs()))?
    };
    SystemTime::now().duration_since(then).ok()
}

/// Go `settleAddAttempt` clears the marker once the outcome IS known, whatever it was.
///
/// The marker means "we do not know", not "an add happened" — so it must not outlive learning. A refused
/// add has the refusal backoff, a successful one has its cached torrent id, and a torrent discovered in
/// the account listing is plainly no longer in flight; all three describe the state better than this
/// does. Keeping it would block the legitimate retry when the add succeeded but the follow-up did not.
pub(crate) fn settle_add_attempt(cache: &dyn Cache, svc: DebridService, token: &str, info_hash: &str) {
    clear(cache, &add_attempt_key(svc, token, info_hash));
    // An outcome, of any kind, ends the run of not knowing.
    clear(cache, &unknown_outcome_key(svc, token, info_hash));
}

/// Go `noteUnknownOutcome` stamps the FIRST such failure and never restamps — a clock that resets is not a
/// deadline, which is the mistake noteQueued was fixed for.
pub(crate) fn note_unknown_outcome(cache: &dyn Cache, svc: DebridService, token: &str, info_hash: &str) {
    let key = unknown_outcome_key(svc, token, info_hash);
    if live(cache, &key).is_some() {
        return;
    }
    cache.put_class(&key, &now_unix(), UNKNOWN_OUTCOME_TTL, CacheClass::Volatile);
}

/// Go `unknownTooLong` reports that the outcome has been unknown for longer than anyone should be asked to wait.
pub(crate) fn unknown_too_long(cache: &dyn Cache, svc: DebridService, token: &str, info_hash: &str) -> bool {
    let Some(raw) = live(cache, &unknown_outcome_key(svc, token, info_hash)) else {
        return false;
    };
    match raw.parse::<i64>() {
        Ok(at) => since_unix(at).is_some_and(|age| age > ADD_GIVE_UP),
        Err(_) => false,
    }
}

/// Go `unknownOutcome` is what an add whose answer never arrived returns: "still coming" until the deadline,
/// then a dead link so the client can fall through to another release.
///
/// `cancelled` decides whether the deadline starts at all. A viewer backing out cancels the request context,
/// and which of the two unknown-outcome branches that lands in — this one, or the transport failure above it —
/// is decided only by whether the response headers had arrived first. A deadline is for a service that went
/// quiet.
pub(crate) fn unknown_outcome(
    cache: &dyn Cache,
    svc: DebridService,
    token: &str,
    info_hash: &str,
    cancelled: bool,
) -> StoreError {
    if unknown_too_long(cache, svc, token, info_hash) {
        return dead_link(format!("{} never reported what its add did", svc.as_str()));
    }
    if !cancelled {
        note_unknown_outcome(cache, svc, token, info_hash);
    }
    StoreError::AddInFlight
}

/// Go `addInFlight` reports an add this process sent and never heard back about.
///
/// The error is `AddInFlight`, which handlePlay answers with 202 "downloading": it is scout's own bookkeeping,
/// not the service declining, so it must not be written into the refusal memory or reported to the client as
/// the debrid's doing — the confusion errScoutSide exists to end.
pub(crate) fn add_in_flight(
    cache: &dyn Cache,
    svc: DebridService,
    token: &str,
    info_hash: &str,
) -> Result<(), StoreError> {
    // The deadline is checked whether or not a marker is live, because it is what has to stop the NEXT
    // add. The marker lives ninety seconds and every attempt writes a fresh one, so an add that keeps
    // failing at the transport cycled marker → expiry → another charged add, about forty an hour, until
    // the allowance was gone and every release on the account answered 503 scout_busy. That is exactly
    // the cycle unknownOutcomeKey was written to end; it had only ever been wired to the body-read branch.
    if unknown_too_long(cache, svc, token, info_hash) {
        return Err(dead_link(format!("{} never reported what its add did", svc.as_str())));
    }
    if !add_outcome_unknown(cache, svc, token, info_hash) {
        return Ok(());
    }
    Err(StoreError::AddInFlight)
}

/// Go `addStillBelievable` mirrors addInFlight's ORDER, and the order is the point.
///
/// addInFlight — which /play's resolve consults — checks unknownTooLong FIRST and converts to a dead link past
/// addGiveUp. Reading addOutcomeUnknown on its own skipped that, and the two markers have different lifetimes
/// (the attempt marker 90s, the unknown-outcome stamp 30 minutes), so they overlap: the last add before the
/// give-up leaves an attempt marker live for up to ninety seconds past it. Measured in that window: ?probe=1
/// answered 202 downloading while /play answered 404 dead_link, for the same release at the same instant.
pub(crate) fn add_still_believable(cache: &dyn Cache, svc: DebridService, token: &str, info_hash: &str) -> bool {
    if unknown_too_long(cache, svc, token, info_hash) {
        return false;
    }
    add_outcome_unknown(cache, svc, token, info_hash)
}

/// Go `accountBackedOff`: the service rejected this ACCOUNT a moment ago, so nothing it is asked will work.
///
/// This one DOES gate a read. The per-release add backoff must not (a read cannot have caused it), but a
/// rejected key makes every request pointless, including one for a release the account holds — and without this
/// the read path recorded the refusal and then asked again on the very next poll.
pub(crate) fn account_backed_off(cache: &dyn Cache, svc: DebridService, token: &str) -> Option<String> {
    cache.get(&account_refused_key(svc, token))
}

/// Go `refusalIsAboutTheAccount`: a status the service returns about WHO is asking, not about what was asked
/// for. Anything else is remembered per release.
pub(crate) fn refusal_is_about_the_account(err: &StoreError) -> bool {
    matches!(err, StoreError::Unavailable(u) if u.status == 401 || u.status == 403)
}

/// Go `recordRefusalFor` writes a refusal on behalf of a caller that may be read-only.
///
/// recordRefusal feeds TWO memories and a NoAdd caller stands differently to each. The per-release key is an
/// add-path guard — a read-only resolve is exempt from reading it, so it must not write one either, or the
/// probe route gates the /play behind it for a minute with no upstream call. The ACCOUNT key is not:
/// `accountBackedOff` is read unconditionally at the top of Resolve, NoAdd callers included, because a rejected
/// key makes every request pointless whatever it is asking for. Guarding both together made the probe unable to
/// set the very key it then reads, so it re-asked a rejected endpoint once per poll.
pub(crate) fn record_refusal_for(
    cache: &dyn Cache,
    svc: DebridService,
    token: &str,
    info_hash: &str,
    err: &StoreError,
    no_add: bool,
) {
    if !no_add || refusal_is_about_the_account(err) {
        record_refusal(cache, svc, token, info_hash, err);
    }
}

/// Go `isCancellation`: the caller went away, rather than the store saying no. Nothing about the store or the
/// release can be concluded from it, so it must not be remembered as a refusal.
pub(crate) fn is_cancellation(err: &StoreError) -> bool {
    matches!(err, StoreError::Cancelled)
}

/// Go `errors.Is(err, errTorrentGone)`: the service answered, and said it has no such torrent. The ONLY
/// evidence that a remembered torrent id is stale.
///
/// It has to be positive evidence, because the alternative was measured: treating any failure of the held path
/// as "the torrent is gone" meant a cancelled poll erased a perfectly good id and re-bought the torrent — ten
/// polls of one already-downloaded release cost ten adds against a sixty-an-hour ceiling. A cancellation, a
/// throttle, a timeout, and a pack that lacks the requested episode say nothing at all about whether the
/// account still holds the file.
pub(crate) fn is_torrent_gone(err: &StoreError) -> bool {
    matches!(err, StoreError::DeadLink(d) if d.reason == ERR_TORRENT_GONE)
}

pub(crate) fn torrent_gone() -> StoreError {
    dead_link(ERR_TORRENT_GONE)
}

/// Go `errWouldAdd`: a NoAdd target could only be resolved by queueing the torrent, so it was not resolved.
pub(crate) fn would_add() -> StoreError {
    dead_link(ERR_WOULD_ADD)
}

/// Go `errNoFileList`: mylist gave no usable answer. Distinct from errTorrentGone, which is mylist ANSWERING
/// that the account no longer holds this id. Collapsing the two is what wedged a stale id: the refusal to guess
/// an episode fired before requestDownload, whose 404 was the only producer of errTorrentGone and so the only
/// thing that could forget the id and re-add the torrent.
pub(crate) fn no_file_list() -> StoreError {
    unavailable(DebridService::TorBox, "mylist gave no usable answer", 0)
}

pub(crate) fn dead_link(reason: impl Into<String>) -> StoreError {
    StoreError::DeadLink(DeadLinkError { reason: reason.into() })
}

pub(crate) fn unavailable(service: DebridService, reason: impl Into<String>, status: u16) -> StoreError {
    StoreError::Unavailable(StoreUnavailableError { service, reason: reason.into(), status })
}

/// A transport failure from the HTTP seam as the error a store call returns: its cause, never its URL.
pub(crate) fn transport_error(err: DoError) -> StoreError {
    match transport_kind(Some(err.as_ref())).as_str() {
        "cancelled" => StoreError::Cancelled,
        kind => StoreError::Other(kind.to_owned()),
    }
}

// --- store helpers from the shared head (stores.go:323-442) ---

/// Go `torboxFetchState` maps TorBox's `download_state` — documented as "downloading", "stalled (no seeds)",
/// "uploading (no peers)", "metaDL", "paused", "completed", "cached", "failed", "failed (processing)",
/// "expired", "missing", "incomplete" (gave up after two days of too few seeds) — onto the stable set. Matched
/// on lowercase prefixes because the documented spellings carry parenthesised qualifiers and casing varies.
/// Anything unrecognised is "downloading", which is what every wait was called before this existed.
pub(crate) fn fetch_state(raw: &str) -> &'static str {
    let s = go_to_lower(raw.trim());
    if s.starts_with("failed") || s.starts_with("error") || s.starts_with("expired") || s.contains("missing") || s.starts_with("incomplete") {
        FETCH_FAILED
    } else if s.starts_with("stalled") || s.starts_with("paused") {
        FETCH_STALLED
    } else if s.starts_with("metadl") || s.starts_with("checking") {
        FETCH_FETCHING
    } else if s.starts_with("queued") {
        FETCH_QUEUED
    } else {
        FETCH_DOWNLOADING
    }
}

pub(crate) fn magnet_for(info_hash: &str) -> String {
    format!("magnet:?xt=urn:btih:{info_hash}")
}

/// Go `redactToken` removes a credential from text that is about to be logged. The empty check is not defensive
/// tidiness: strings.ReplaceAll with an empty needle splices the replacement between every character, so an
/// unconfigured store would turn a short error into an unreadable one.
pub(crate) fn redact_token(text: &str, token: &str) -> String {
    if token.is_empty() {
        return text.to_owned();
    }
    text.replace(token, "<token>")
}

/// Go `shortHash` trims an infohash for logs: enough to correlate with the client's line, not the whole thing.
pub(crate) fn short_hash(h: &str) -> String {
    if h.len() <= 12 {
        return h.to_owned();
    }
    String::from_utf8_lossy(&h.as_bytes()[..12]).into_owned()
}

/// Go `storeRefusedUs` reports whether an HTTP status is the service declining to serve this account right now —
/// a throttle or a fault on their side — rather than a verdict about the torrent.
pub(crate) fn store_refused_us(status: u16) -> bool {
    // 401/403 belong here too: an expired or wrong API key is the service declining to serve THIS ACCOUNT,
    // which says nothing about the release. Left out, a stale token produced a plain dead link — so no
    // backoff was recorded, every poll paid another add against scout's own allowance, and the eventual
    // refusals read as scout being busy rather than as a key that needs replacing.
    status == 401 || status == 403 || status == 429 || status >= 500
}

/// Go `url.QueryEscape`: everything but the unreserved characters is escaped, and a space is a plus.
fn query_escape(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for b in s.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => out.push(char::from(b)),
            b' ' => out.push('+'),
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
}

/// Go `url.Values.Encode`: keys in sorted order, each one escaped.
pub(crate) fn encode_form(pairs: &[(&str, &str)]) -> String {
    let mut sorted: Vec<&(&str, &str)> = pairs.iter().collect();
    sorted.sort_by(|a, b| a.0.cmp(b.0));
    sorted.iter().map(|(k, v)| format!("{}={}", query_escape(k), query_escape(v))).collect::<Vec<_>>().join("&")
}

/// Go `http.NewRequestWithContext` plus the headers a store sets. The error never quotes the URL.
pub(crate) fn build_request(
    method: reqwest::Method,
    url: &str,
    headers: &[(&str, &str)],
    body: Option<String>,
) -> Result<reqwest::Request, String> {
    let parsed = reqwest::Url::parse(url).map_err(|_| "invalid request URL".to_owned())?;
    let mut req = reqwest::Request::new(method, parsed);
    for (name, value) in headers {
        let name = http::header::HeaderName::from_bytes(name.as_bytes()).map_err(|_| "invalid header name".to_owned())?;
        let value = http::header::HeaderValue::from_str(value)
            .map_err(|_| "net/http: invalid header field value".to_owned())?;
        req.headers_mut().insert(name, value);
    }
    if let Some(body) = body {
        *req.body_mut() = Some(reqwest::Body::from(body));
    }
    Ok(req)
}

/// Go `io.ReadAll(io.LimitReader(body, limit))`: the body up to `limit` bytes, the rest unread.
pub(crate) async fn read_limited(resp: &mut reqwest::Response, limit: usize) -> Result<Vec<u8>, reqwest::Error> {
    let mut out = Vec::new();
    while out.len() < limit {
        match resp.chunk().await? {
            Some(chunk) => {
                let room = limit - out.len();
                out.extend_from_slice(&chunk[..chunk.len().min(room)]);
            }
            None => break,
        }
    }
    Ok(out)
}

/// Go `readStoreError`: the service's own explanation of a failed response, as " (detail)" ready to append to a
/// message. Empty when the body cannot be read.
pub(crate) async fn read_store_error(resp: &mut reqwest::Response) -> String {
    match read_limited(resp, 2048).await {
        Ok(raw) => store_error_text(&raw),
        Err(_) => String::new(),
    }
}

/// The two fields of a store's error body.
static STORE_ERROR_SHAPE: Shape = Shape::Object(&[("error", &crate::util::LEAF), ("detail", &crate::util::LEAF)]);

/// Go's `fmt.Sprintf("%v", v)` for a decoded JSON value, which is how `storeErrorText` prints `error`.
fn go_v(v: &Value) -> String {
    match v {
        Value::Null => "<nil>".to_owned(),
        Value::Bool(b) => b.to_string(),
        Value::String(s) => s.clone(),
        Value::Number(n) => match n.as_f64() {
            // encoding/json hands `any` a float64, and %v prints it with %g: integers without a point, a
            // large or tiny magnitude in exponent form.
            Some(f) if f.fract() == 0.0 && f.abs() < 1e21 => format!("{f:.0}"),
            Some(f) if f != 0.0 && (f.abs() < 1e-4 || f.abs() >= 1e21) => {
                let s = format!("{f:e}");
                match s.split_once('e') {
                    Some((m, e)) => {
                        let (sign, digits) = match e.strip_prefix('-') {
                            Some(d) => ('-', d),
                            None => ('+', e),
                        };
                        format!("{m}e{sign}{digits:0>2}")
                    }
                    None => s,
                }
            }
            Some(f) => format!("{f}"),
            None => n.to_string(),
        },
        Value::Array(items) => format!("[{}]", items.iter().map(go_v).collect::<Vec<_>>().join(" ")),
        Value::Object(map) => {
            let mut keys: Vec<&String> = map.keys().collect();
            keys.sort();
            format!("map[{}]", keys.iter().map(|k| format!("{k}:{}", go_v(&map[*k]))).collect::<Vec<_>>().join(" "))
        }
    }
}

/// Go `storeErrorText` is `readStoreError` over bytes already in hand, for a caller that had to read the body
/// once for its own sake. TorBox answers `{"error":"ACTIVE_LIMIT","detail":"..."}`; anything unparseable falls
/// back to a clipped snippet of the raw body, because a truncated reason still beats none.
pub(crate) fn store_error_text(raw: &[u8]) -> String {
    if raw.is_empty() {
        return String::new();
    }
    let raw = &raw[..raw.len().min(2048)];
    if let Ok(mut value) = serde_json::from_slice::<Value>(raw) {
        fold_json_keys(&mut value, &STORE_ERROR_SHAPE);
        let (error, detail) = match &value {
            Value::Object(map) => (map.get("error"), map.get("detail")),
            _ => (None, None),
        };
        // Go's struct has `Detail string`: a detail of another type is a decode error, which sends the whole
        // body down the snippet path below.
        let detail = match detail {
            None | Some(Value::Null) => Some(None),
            Some(Value::String(s)) if s.is_empty() => Some(None),
            Some(Value::String(s)) => Some(Some(s.as_str())),
            Some(_) => None,
        };
        let error = error.filter(|e| !e.is_null());
        if let Some(detail) = detail {
            match (detail, error) {
                (Some(d), Some(e)) => return format!(" ({}: {d})", go_v(e)),
                (Some(d), None) => return format!(" ({d})"),
                (None, Some(e)) => return format!(" ({})", go_v(e)),
                (None, None) => {}
            }
        }
    }
    format!(" ({})", String::from_utf8_lossy(&raw[..raw.len().min(200)]).trim())
}

// --- JSON as Go's encoding/json reads it ---

/// The first JSON value in the body, the rest ignored: Go's `json.NewDecoder(r).Decode(&v)`. A value whose keys
/// are then matched to `shape`'s fields case-insensitively, as Go's decoder matches them.
pub(crate) fn first_value(raw: &[u8], shape: &Shape) -> Option<Value> {
    let mut value = serde_json::Deserializer::from_slice(raw).into_iter::<Value>().next()?.ok()?;
    fold_json_keys(&mut value, shape);
    Some(value)
}

/// A response body as the struct Go decodes it into: an object, or null, which decodes cleanly into the zero
/// value. Anything else is a decode error.
pub(crate) fn envelope(v: Value) -> Option<serde_json::Map<String, Value>> {
    match v {
        Value::Object(m) => Some(m),
        Value::Null => Some(serde_json::Map::new()),
        _ => None,
    }
}

/// A field that Go declares as a plain `int`: absent and null leave the zero value, an integer is read, and
/// anything else is a type error.
pub(crate) fn int_or_zero(v: Option<&Value>) -> Result<i64, ()> {
    match v {
        None | Some(Value::Null) => Ok(0),
        Some(Value::Number(n)) => n.as_i64().ok_or(()),
        Some(_) => Err(()),
    }
}

/// A field that Go declares as `*int`.
pub(crate) fn opt_int(v: Option<&Value>) -> Result<Option<i64>, ()> {
    match v {
        None | Some(Value::Null) => Ok(None),
        Some(Value::Number(n)) => n.as_i64().map(Some).ok_or(()),
        Some(_) => Err(()),
    }
}

/// A field that Go declares as `*bool`.
pub(crate) fn opt_bool(v: Option<&Value>) -> Result<Option<bool>, ()> {
    match v {
        None | Some(Value::Null) => Ok(None),
        Some(Value::Bool(b)) => Ok(Some(*b)),
        Some(_) => Err(()),
    }
}

/// A field that Go declares as `*float64`.
pub(crate) fn opt_float(v: Option<&Value>) -> Result<Option<f64>, ()> {
    match v {
        None | Some(Value::Null) => Ok(None),
        Some(Value::Number(n)) => n.as_f64().map(Some).ok_or(()),
        Some(_) => Err(()),
    }
}

/// A field that Go declares as `*string`.
pub(crate) fn opt_string(v: Option<&Value>) -> Result<Option<String>, ()> {
    match v {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(s)) => Ok(Some(s.clone())),
        Some(_) => Err(()),
    }
}

/// A field that Go declares as a plain `string`.
pub(crate) fn string_or_empty(v: Option<&Value>) -> Result<String, ()> {
    Ok(opt_string(v)?.unwrap_or_default())
}

// --- concurrency ---

/// A key for work that is shared between callers, scoped to the runtime the caller is on.
///
/// Shared work runs in a task on the runtime that started it, and a caller on another runtime could be left
/// waiting for a task whose runtime has since shut down. The server has one runtime, so this changes nothing
/// there; it is also what keeps the work of two independent runtimes in one process (each test has its own)
/// from being mistaken for one account's.
pub(crate) fn scoped(key: &str) -> String {
    format!("{}:{key}", tokio::runtime::Handle::current().id())
}

/// Go `singleflight.Group.DoChan`: concurrent callers of one key share one run of the work and its answer, and the
/// work is NOT tied to the caller that started it. Dropping the caller that started the work (the request went
/// away, its budget ran out) leaves the work running for everyone still waiting, which is the property the
/// account listing and the shared status reads were built to have: "the leader now runs detached from
/// cancellation".
///
/// `indexerconfig::Flight` is not that: when its leader is dropped a waiter starts the work again. That is right
/// for a mint and wrong here, where the work is a 64 MiB listing a follower is still waiting on.
///
/// The work runs in its own task, so a panic in it ends that task and nothing else: every waiter is answered
/// `None` rather than left waiting, where Go would have died (see `recoverBackground`).
pub(super) struct DetachedFlight<T>(Mutex<BTreeMap<String, watch::Receiver<Option<Option<T>>>>>);

impl<T: Clone + Send + Sync + 'static> DetachedFlight<T> {
    pub(super) const fn new() -> Self {
        DetachedFlight(Mutex::new(BTreeMap::new()))
    }

    /// Runs `make()` for the first caller of `key`; a caller that arrives while it is in flight waits for the same
    /// answer. The key is forgotten before the answer is published, so a caller arriving after it runs the work
    /// again: nothing is cached here. `None` is a run that panicked.
    pub(super) async fn run<F, Fut>(&'static self, key: &str, make: F) -> Option<T>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = T> + Send + 'static,
    {
        let key = scoped(key);
        let mut rx = {
            let mut calls = lock(&self.0);
            match calls.get(&key) {
                Some(rx) => rx.clone(),
                None => {
                    let (tx, rx) = watch::channel(None);
                    calls.insert(key.clone(), rx.clone());
                    let work = make();
                    tokio::spawn(async move {
                        let answer = tokio::spawn(work).await.ok();
                        lock(&self.0).remove(&key);
                        let _ = tx.send(Some(answer));
                    });
                    rx
                }
            }
        };
        let answered = rx.wait_for(|v| v.is_some()).await.ok()?;
        let answer = (*answered).clone().flatten();
        drop(answered);
        answer
    }
}

/// Go `errgroup.Group` with every goroutine returning nil: runs the futures together and returns their outputs in
/// order. They are polled on the caller's task, so none of them needs to be `'static`.
pub(crate) async fn join_all<F: Future>(futures: Vec<F>) -> Vec<F::Output> {
    let mut pending: Vec<Option<Pin<Box<F>>>> = futures.into_iter().map(|f| Some(Box::pin(f))).collect();
    let mut outputs: Vec<Option<F::Output>> = pending.iter().map(|_| None).collect();
    std::future::poll_fn(|cx| {
        let mut done = true;
        for (slot, out) in pending.iter_mut().zip(outputs.iter_mut()) {
            if let Some(fut) = slot {
                match fut.as_mut().poll(cx) {
                    Poll::Ready(v) => {
                        *out = Some(v);
                        *slot = None;
                    }
                    Poll::Pending => done = false,
                }
            }
        }
        if done {
            Poll::Ready(())
        } else {
            Poll::Pending
        }
    })
    .await;
    outputs.into_iter().flatten().collect()
}
