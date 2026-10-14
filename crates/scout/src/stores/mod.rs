//! Port of `internal/scout/stores.go` (5,142 lines), split across this directory because two work
//! packages share it. This file holds what is shared and FROZEN (see PORTING.md): the `Store` trait, the
//! resolve target, status and error types. The rest of the Go file lands in:
//!
//! - `refusal.rs`: lines 780-1560, cache keys, add attempts, queued/held markers, refusal backoff and
//!   the reporters (`StorePool::recent_refusal` and friends)
//! - `torbox.rs`: lines 446-3770 minus the above, the TorBox store, its listing, status and cancel
//! - `realdebrid.rs`: lines 3770-4316
//! - `premiumize.rs`: lines 4316-4722
//! - `pool.rs`: lines 4722-end, `StorePool`, `CacheTruth`, `build_stores`

pub mod pool;
pub mod premiumize;
pub mod realdebrid;
pub mod refusal;
pub mod torbox;

use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::task::Poll;
use std::time::Duration;

use async_trait::async_trait;

use crate::config::DebridService;
use crate::util::go_to_lower;

/// Go `resolveCacheTTL` (stores.go:449): how long a remembered torrent id, file list or held mark lives.
pub(crate) const RESOLVE_CACHE_TTL: Duration = Duration::from_secs(6 * 60 * 60);

/// Go `maxStoreBytes`: caps a debrid API response body.
pub const MAX_STORE_BYTES: usize = 4 << 20;
/// Go `maxListingBytes`: the one exception, the account listing is the whole account.
pub const MAX_LISTING_BYTES: usize = 64 << 20;

/// FROZEN. Go `ResolveTarget`: an infohash plus either the exact file or the series episode to pick from
/// a pack.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ResolveTarget {
    pub info_hash: String,
    pub file_idx: Option<i64>,
    pub season: Option<i64>,
    pub episode: Option<i64>,
    /// Forbids queueing the torrent: resolve it from what the account already has, or fail.
    pub no_add: bool,
    /// An add nobody is waiting on (`/play?prefetch=1`); the budget keeps its last few adds from it.
    pub prefetch: bool,
    /// The indexer's size for the release, 0 when unknown. Stores do not read it; the link check does.
    pub release_size: i64,
    /// Asks TorBox to list the torrent's files even for a movie.
    pub list_files: bool,
    /// Diagnostic only; a store never reads them.
    pub title: String,
    pub imdb: String,
}

/// FROZEN. Go `statusAnswer`: three values, because "not being fetched" and "could not find out" are
/// different facts and only the first may lead to an add.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StatusAnswer {
    /// The store answered, and it is not fetching this release. Includes a store with no status API.
    No,
    Downloading,
    Unknown,
}

/// FROZEN. Go `StoreStatus`: how far along a queued release is.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct StoreStatus {
    /// 0..=1
    pub progress: f64,
    /// None unless the store reports a real one.
    pub eta_seconds: Option<i64>,
    pub bytes_per_second: Option<i64>,
    /// One of the `FETCH_*` states; empty means the service named none, answered as "downloading".
    pub state: String,
    pub seeds: Option<i64>,
    pub peers: Option<i64>,
    /// The store that answered, set by the pool.
    pub service: Option<DebridService>,
}

/// FROZEN. The stable fetch states the 202 body carries.
pub const FETCH_QUEUED: &str = "queued";
pub const FETCH_FETCHING: &str = "fetching";
pub const FETCH_DOWNLOADING: &str = "downloading";
pub const FETCH_STALLED: &str = "stalled";
pub const FETCH_FAILED: &str = "failed";

/// FROZEN. Go `CancelOutcome`: what a cancel (or reannounce) attempt found.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct CancelOutcome {
    pub done: bool,
    pub reannounced: bool,
    /// "not_queued", "finished" or "not_ours" when nothing was done.
    pub reason: String,
    pub status: StoreStatus,
}

/// FROZEN. Go `DeadLinkError`: nothing could deliver the file, so the route answers 404.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeadLinkError {
    pub reason: String,
}

impl fmt::Display for DeadLinkError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "dead_link: {}", self.reason)
    }
}

/// The reasons of Go's sentinel `DeadLinkError` values (`errWouldAdd`, `errTorrentGone`, `errCheckFailed`),
/// which callers compare by identity in Go and by reason here.
pub const ERR_WOULD_ADD: &str = "not held by this account (add not permitted)";
pub const ERR_TORRENT_GONE: &str = "torbox no longer has this torrent";
pub const ERR_CHECK_FAILED: &str = "cache check failed";

/// FROZEN. Go `StoreUnavailableError`: the SERVICE refused us, which says nothing about the release.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoreUnavailableError {
    pub service: DebridService,
    pub reason: String,
    /// The HTTP status the service answered with, when there was one (0 when none). Carried rather than
    /// parsed back out of `reason`.
    pub status: u16,
}

impl fmt::Display for StoreUnavailableError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "store_unavailable: {} {}", self.service.as_str(), self.reason)
    }
}

/// FROZEN. What a store call can fail with: Go returns `error` and callers branch on the concrete type
/// (`*DeadLinkError`, `*StoreUnavailableError`, `errAddInFlight`, `errAddQueued`, context cancellation).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StoreError {
    DeadLink(DeadLinkError),
    Unavailable(StoreUnavailableError),
    /// Go `errScoutSide` wrapping a `StoreUnavailableError`: a refusal scout made itself (its own add
    /// allowance), not the service. `recordRefusal` never remembers it.
    ScoutSide(StoreUnavailableError),
    /// Go `errAddInFlight`: an add for this release is already out and unanswered.
    AddInFlight,
    /// Go `errAddQueued`: TorBox parked the add behind the account's slot limit.
    AddQueued,
    /// The caller's deadline or cancellation.
    Cancelled,
    /// Anything else, with its message.
    Other(String),
}

/// Go's `err.Error()` for the errors a store call returns, which is what a log line and a remembered refusal
/// reason print. Go's `fmt.Errorf("%w: %w", errAddInFlight, &StoreUnavailableError{...})` also names the
/// wrapped refusal; `AddInFlight` here carries no payload, so only its own sentence is printed.
impl fmt::Display for StoreError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            StoreError::DeadLink(d) => d.fmt(f),
            StoreError::Unavailable(u) => u.fmt(f),
            StoreError::ScoutSide(u) => write!(f, "{}: {u}", crate::stores::refusal::ERR_SCOUT_SIDE),
            StoreError::AddInFlight => f.write_str("an add for this release is already in flight"),
            StoreError::AddQueued => f.write_str("torbox queued this add behind the account's slot limit"),
            StoreError::Cancelled => f.write_str("context canceled"),
            StoreError::Other(text) => f.write_str(text),
        }
    }
}

/// FROZEN. Go `Store`: a debrid backend.
///
/// `cache_check` returns ONLY what it learned: a hash it could not check is ABSENT from the map, which
/// the pool reads as unknown. Do not pre-seed the map with false. The error is `Err` only when NOTHING
/// could be checked.
///
/// Go's optional interfaces (`statusAnswerer`, `storeCanceller`, `refusalReporter`, `addInFlightReporter`,
/// `heldTorrentReporter`, `scoutBudgetReporter`, `accountRefusalReporter`) are type assertions on the
/// store; here each is a defaulted method that a store overrides, returning `None` when it does not
/// implement it. The context argument is dropped: cancellation is dropping the future and a deadline is
/// `tokio::time::timeout` around it.
#[async_trait]
pub trait Store: Send + Sync {
    fn service(&self) -> DebridService;

    async fn cache_check(
        &self,
        hashes: &[String],
    ) -> Result<std::collections::HashMap<String, bool>, StoreError>;

    /// Infohash to a playable https link.
    async fn resolve(&self, t: &ResolveTarget) -> Result<String, StoreError>;

    /// A release the store has been ASKED for but could not deliver yet: downloading, not dead. `None`
    /// when this store knows nothing about the target.
    async fn status(&self, t: &ResolveTarget) -> Option<StoreStatus>;

    /// Go `statusAnswerer.StatusAnswer`, the three-valued half of `status`. `None` when the store has no
    /// such method, in which case the pool uses `status`.
    async fn status_answer(&self, t: &ResolveTarget) -> Option<(StoreStatus, StatusAnswer)> {
        None
    }

    /// Go `storeCanceller.Cancel`. `None` when the store cannot cancel (Real-Debrid, Premiumize).
    async fn cancel(&self, info_hash: &str, reannounce: bool) -> Option<Result<CancelOutcome, StoreError>> {
        None
    }

    /// Go `refusalReporter.RecentRefusal`: the reason this store was turned away for the hash a moment
    /// ago.
    fn recent_refusal(&self, info_hash: &str) -> Option<String> {
        None
    }

    /// Go `addInFlightReporter.AddInFlight`.
    fn add_in_flight(&self, info_hash: &str) -> Option<bool> {
        None
    }

    /// Go `heldTorrentReporter.HoldsTorrent`.
    fn holds_torrent(&self, t: &ResolveTarget) -> Option<bool> {
        None
    }

    /// Go `scoutBudgetReporter.ScoutBudgetSpent`.
    fn scout_budget_spent(&self) -> Option<bool> {
        None
    }

    /// Go `accountRefusalReporter.AccountRefused`.
    fn account_refused(&self) -> Option<String> {
        None
    }

    /// Go's `interface{ budgetAccount() string }`: the key this store's account is charged under in the add
    /// budget (`addbudget::budget_account`). `None` for a store that does not spend adds.
    fn budget_account(&self) -> Option<String> {
        None
    }

    /// Go `fileSizer.KnownFileSize` (linkcheck.go): the size of the target file from what the store already
    /// holds, cache-only. `None` when it is not known.
    fn known_file_size(&self, _t: &ResolveTarget) -> Option<i64> {
        None
    }
}

/// FROZEN. Go `CacheTruth`: what the pool actually learned about a batch of hashes. Not a bare map of
/// yes/no, because "no" and "could not find out" have opposite costs.
#[derive(Debug, Clone, Default)]
pub struct CacheTruth {
    pub(crate) holders: std::collections::HashMap<String, Vec<DebridService>>,
    pub(crate) known: std::collections::HashMap<String, bool>,
    /// Every configured cache-truth store answered for something.
    pub(crate) complete: bool,
}

impl CacheTruth {
    /// At least one store confirmed it holds this.
    pub fn cached(&self, hash: &str) -> bool {
        self.holders.get(hash).is_some_and(|h| !h.is_empty())
    }

    /// At least one cache-truth store gave an answer for this hash, either way.
    pub fn known(&self, hash: &str) -> bool {
        self.known.get(hash).copied().unwrap_or(false)
    }

    /// Every cache-truth store answered.
    pub fn complete(&self) -> bool {
        self.complete
    }

    /// The services that confirmed they hold it, in configured priority order.
    pub fn held_by(&self, hash: &str) -> Vec<DebridService> {
        self.holders.get(hash).cloned().unwrap_or_default()
    }
}

/// Go `torboxFetchState`: TorBox's `download_state` onto the stable `FETCH_*` set.
pub fn torbox_fetch_state(raw: &str) -> &'static str {
    let s = go_to_lower(raw.trim());
    if s.starts_with("failed")
        || s.starts_with("error")
        || s.starts_with("expired")
        || s.contains("missing")
        || s.starts_with("incomplete")
    {
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

/// Go `rdFetchState`: a Real-Debrid status already accepted as fetching onto the stable set.
pub fn rd_fetch_state(status: &str) -> &'static str {
    match status {
        "queued" => FETCH_QUEUED,
        "magnet_conversion" => FETCH_FETCHING,
        _ => FETCH_DOWNLOADING,
    }
}

/// Go `magnetFor`.
pub fn magnet_for(info_hash: &str) -> String {
    format!("magnet:?xt=urn:btih:{info_hash}")
}

/// Go `redactToken` removes a credential from text that is about to be logged. The empty check is not
/// defensive tidiness: strings.ReplaceAll with an empty needle splices the replacement between every
/// character, so an unconfigured store would turn a short error into an unreadable one.
pub fn redact_token(text: &str, token: &str) -> String {
    if token.is_empty() {
        return text.to_owned();
    }
    text.replace(token, "<token>")
}

/// Go `shortHash` trims an infohash for logs: enough to correlate with the client's line, not the whole thing.
pub fn short_hash(h: &str) -> String {
    if h.len() <= 12 {
        return h.to_owned();
    }
    // Go slices bytes; a hash is ASCII, and anything else is cut at the byte as Go would.
    String::from_utf8_lossy(&h.as_bytes()[..12]).into_owned()
}

/// Go `storeRefusedUs` reports whether an HTTP status is the service declining to serve this account right
/// now, a throttle or a fault on their side, rather than a verdict about the torrent.
pub(crate) fn store_refused_us(status: u16) -> bool {
    // 401/403 belong here too: an expired or wrong API key is the service declining to serve THIS ACCOUNT,
    // which says nothing about the release. Left out, a stale token produced a plain dead link, so no
    // backoff was recorded, every poll paid another add against scout's own allowance, and the eventual
    // refusals read as scout being busy rather than as a key that needs replacing.
    status == 401 || status == 403 || status == 429 || status >= 500
}

/// A `DeadLinkError` as the error a store call returns.
pub(crate) fn dead_link(reason: impl Into<String>) -> StoreError {
    StoreError::DeadLink(DeadLinkError { reason: reason.into() })
}

/// A `StoreUnavailableError` as the error a store call returns.
pub(crate) fn unavailable(service: DebridService, reason: impl Into<String>, status: u16) -> StoreError {
    StoreError::Unavailable(StoreUnavailableError { service, reason: reason.into(), status })
}

/// The text of an error and everything it wraps, which is where a body-read failure keeps its cause: a
/// `reqwest::Error` says only "error decoding response body" and carries the reason as its source.
pub(crate) fn error_chain_text(err: &(dyn std::error::Error + 'static)) -> String {
    let mut text = err.to_string();
    let mut source = err.source();
    while let Some(inner) = source {
        text.push_str(": ");
        text.push_str(&inner.to_string());
        source = inner.source();
    }
    text
}

/// Go `context.Canceled`: the caller went away. A `Doer` that is told to stop (or a test that stands in for one)
/// fails with this, and `is_cancellation_error` reads it back by type.
#[derive(Debug)]
pub(crate) struct CancelledError;

impl fmt::Display for CancelledError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("context canceled")
    }
}

impl std::error::Error for CancelledError {}

/// Go `isCancellation` (`errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)`) over a
/// chain of causes: the caller went away, or its deadline ran out, rather than the store saying no. Nothing about
/// the store or the release can be concluded from it.
///
/// Read by type first: `CancelledError`, the scrape budget's `DeadlineExceeded`, tokio's `Elapsed`, a `reqwest`
/// timeout and an `io::ErrorKind::TimedOut`. The text is the last resort, for the errors that reach a `Doer` as
/// bare messages (a mock, or a wrapper that only kept a string): Go's two sentinels render as these words, and
/// there is nothing else to go on for such an error.
pub(crate) fn is_cancellation_error(err: &(dyn std::error::Error + 'static)) -> bool {
    std::iter::successors(Some(err), |&e| e.source()).any(|e| {
        e.is::<CancelledError>()
            || e.is::<crate::scrape::DeadlineExceeded>()
            || e.is::<tokio::time::error::Elapsed>()
            || e.downcast_ref::<reqwest::Error>().is_some_and(reqwest::Error::is_timeout)
            || e.downcast_ref::<std::io::Error>().is_some_and(|io| io.kind() == std::io::ErrorKind::TimedOut)
            || {
                let text = e.to_string();
                text.contains("context canceled")
                    || text.contains("context deadline exceeded")
                    || text.contains("operation cancelled")
                    || text.contains("operation was canceled")
            }
    })
}

/// Go's `readStoreError` over a response: the service's own explanation of a failure, as " (detail)".
/// Empty when the body cannot be read.
pub(crate) async fn read_store_error(resp: &mut reqwest::Response) -> String {
    match read_limited(resp, 2048).await {
        Ok(raw) => store_error_text(&raw),
        Err(_) => String::new(),
    }
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

/// The two fields of a store's error body. A key matches case-insensitively, as Go's decoder matches it.
static STORE_ERROR_SHAPE: crate::util::Shape = crate::util::Shape::Object(&[
    ("error", &crate::util::LEAF),
    ("detail", &crate::util::LEAF),
]);

/// Go's `fmt.Sprintf("%v", v)` for a decoded JSON value, which is how `storeErrorText` prints `error`.
fn go_v(v: &serde_json::Value) -> String {
    use serde_json::Value;
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
        Value::Array(items) => {
            let parts: Vec<String> = items.iter().map(go_v).collect();
            format!("[{}]", parts.join(" "))
        }
        Value::Object(map) => {
            let mut keys: Vec<&String> = map.keys().collect();
            keys.sort();
            let parts: Vec<String> = keys.iter().map(|k| format!("{k}:{}", go_v(&map[*k]))).collect();
            format!("map[{}]", parts.join(" "))
        }
    }
}

/// Go `storeErrorText` reads the service's own explanation out of a failed body, as " (detail)" ready to
/// append to a message. TorBox answers `{"error":"ACTIVE_LIMIT","detail":"..."}`; anything unparseable
/// falls back to a clipped snippet of the raw body, because a truncated reason still beats none.
pub(crate) fn store_error_text(raw: &[u8]) -> String {
    if raw.is_empty() {
        return String::new();
    }
    let raw = &raw[..raw.len().min(2048)];
    if let Ok(mut value) = serde_json::from_slice::<serde_json::Value>(raw) {
        crate::util::fold_json_keys(&mut value, &STORE_ERROR_SHAPE);
        let (error, detail) = match &value {
            serde_json::Value::Object(map) => (map.get("error"), map.get("detail")),
            _ => (None, None),
        };
        // Go's struct has `Detail string`: a detail of another type is a decode error, which sends the
        // whole body down the snippet path below.
        let detail_text = match detail {
            None | Some(serde_json::Value::Null) => Some(None),
            Some(serde_json::Value::String(s)) if !s.is_empty() => Some(Some(s.as_str())),
            Some(serde_json::Value::String(_)) => Some(None),
            Some(_) => None,
        };
        let error = error.filter(|e| !e.is_null());
        if let Some(detail) = detail_text {
            match (detail, error) {
                (Some(d), Some(e)) => return format!(" ({}: {d})", go_v(e)),
                (Some(d), None) => return format!(" ({d})"),
                (None, Some(e)) => return format!(" ({})", go_v(e)),
                (None, None) => {}
            }
        }
    }
    let clipped = &raw[..raw.len().min(200)];
    format!(" ({})", String::from_utf8_lossy(clipped).trim())
}

/// Go `url.QueryEscape`: everything but the unreserved characters is escaped, and a space is a plus.
pub(crate) fn go_query_escape(s: &str) -> String {
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

/// Go `url.Values.Encode`: keys in sorted order, each key's values in the order they were added.
pub(crate) fn encode_form(pairs: &[(&str, &str)]) -> String {
    let mut sorted: Vec<&(&str, &str)> = pairs.iter().collect();
    sorted.sort_by(|a, b| a.0.cmp(b.0));
    sorted
        .iter()
        .map(|(k, v)| format!("{}={}", go_query_escape(k), go_query_escape(v)))
        .collect::<Vec<_>>()
        .join("&")
}

/// Go `http.NewRequestWithContext` plus the headers a store sets. The error never quotes the URL: for some
/// services it carries the account key.
pub(crate) fn build_request(
    method: reqwest::Method,
    url: &str,
    headers: &[(&str, &str)],
    body: Option<String>,
) -> Result<reqwest::Request, StoreError> {
    let parsed = reqwest::Url::parse(url).map_err(|_| StoreError::Other("invalid request URL".to_owned()))?;
    let mut req = reqwest::Request::new(method, parsed);
    for (name, value) in headers {
        let name = http::header::HeaderName::from_bytes(name.as_bytes())
            .map_err(|_| StoreError::Other("invalid header name".to_owned()))?;
        let value = http::header::HeaderValue::from_str(value)
            .map_err(|_| StoreError::Other("net/http: invalid header field value".to_owned()))?;
        req.headers_mut().insert(name, value);
    }
    if let Some(body) = body {
        *req.body_mut() = Some(reqwest::Body::from(body));
    }
    Ok(req)
}

/// Go's `json.NewDecoder(r).Decode(&v)` over bytes already in hand: the first JSON value, whatever follows
/// it ignored, with keys matched to `shape`'s fields case-insensitively. `None` where Go returns an error.
pub(crate) fn decode_first<T: serde::de::DeserializeOwned>(raw: &[u8], shape: &crate::util::Shape) -> Option<T> {
    let mut stream = serde_json::Deserializer::from_slice(raw).into_iter::<serde_json::Value>();
    let mut value = stream.next()?.ok()?;
    crate::util::fold_json_keys(&mut value, shape);
    serde_json::from_value(value).ok()
}

/// Go's `json.Unmarshal(raw, &v)`: the whole document must be valid JSON, then it is decoded the same way.
pub(crate) fn decode_all<T: serde::de::DeserializeOwned>(raw: &[u8], shape: &crate::util::Shape) -> Option<T> {
    let mut value = serde_json::from_slice::<serde_json::Value>(raw).ok()?;
    crate::util::fold_json_keys(&mut value, shape);
    serde_json::from_value(value).ok()
}

/// A JSON null (or an absent key) leaves a Go field at its zero value; serde would call it a type error.
pub(crate) fn null_default<'de, D, T>(d: D) -> Result<T, D::Error>
where
    D: serde::Deserializer<'de>,
    T: serde::Deserialize<'de> + Default,
{
    Ok(<Option<T> as serde::Deserialize>::deserialize(d)?.unwrap_or_default())
}

/// Go `errgroup.Group` with every goroutine returning nil: runs the futures together and returns their
/// outputs in order. They are polled on the caller's task, so none of them needs to be `'static`.
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
