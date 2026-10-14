//! The debrid stores. This file holds what they share: the `Store` trait, the resolve target, status and error
//! types. The rest is:
//!
//! - `refusal.rs`: cache keys, add attempts, queued/held markers, refusal backoff and the reporters
//!   (`StorePool::recent_refusal` and friends)
//! - `torbox.rs`: the TorBox store, its listing, status and cancel
//! - `realdebrid.rs`, `premiumize.rs`: the other two stores
//! - `pool.rs`: `StorePool`, `CacheTruth`, `build_stores`

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

/// Go `resolveCacheTTL` (stores.go:449): how long a remembered torrent id, file list or held mark lives.
pub(crate) const RESOLVE_CACHE_TTL: Duration = Duration::from_secs(6 * 60 * 60);

/// Go `maxStoreBytes` caps a debrid API response body — these JSON payloads are small; the limit stops a
/// hostile/misbehaving store from OOMing the container (mirrors maxScrapeBytes for indexers).
pub const MAX_STORE_BYTES: usize = 4 << 20;
/// Go `maxListingBytes` is the ONE exception: the account listing is the whole account, and 4 MiB truncates a
/// real large one (~13 MB at 2,000 torrents), which reads back as "this account holds nothing" and costs a
/// duplicate add. It can be this large safely because the listing decode streams and keeps only hash→id — see
/// fetchAccountListing. Measured at the cap rather than argued, since that is what the size rests on: a 60 MiB
/// listing allocates 2.7 MiB in total and peaks 20 MiB above baseline, against 128.9 MiB for the buffering
/// decode this replaced. Every other store read keeps the small cap.
pub const MAX_LISTING_BYTES: usize = 64 << 20;

// Debrid stores (ported from src/stores/*). Two ops: CacheCheck (which hashes are cached?) and Resolve
// (infohash → playable https link). Scout resolves server-side; the token never leaves the server.

/// Go `ResolveTarget`: an infohash + either the exact file, or the series episode to pick from a pack.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ResolveTarget {
    pub info_hash: String,
    pub file_idx: Option<i64>,
    pub season: Option<i64>,
    pub episode: Option<i64>,
    /// NoAdd forbids queueing the torrent: resolve it from what the account already has, or fail.
    ///
    /// A background caller cannot promise this by choosing carefully, because whether a Resolve adds is
    /// decided deep inside each store. The probe path DID choose carefully — it only resolves releases a
    /// cache check reported as held — and still POSTed createtorrent, because TorBox's checkcached reports
    /// what TORBOX has, not what this ACCOUNT has. "Held" was never the same as "already added". Only the
    /// store can enforce this, so the caller states the requirement and the store obeys it.
    pub no_add: bool,
    /// Prefetch marks an add nobody is waiting on (/play?prefetch=1: the binge read-ahead, a queued
    /// download). The budget keeps its last few adds from it for a viewer pressing Play — see
    /// defaultPlayReserve. Unset is a viewer waiting, which is what every older client sends.
    pub prefetch: bool,
    /// ReleaseSize is the indexer's size for the release, 0 when unknown. Stores do not read it; the link
    /// check does, for a movie whose file size no store knows.
    pub release_size: i64,
    /// ListFiles asks TorBox to list the torrent's files even for a movie, so the indexer's fileIdx — a
    /// position in the torrent — is mapped through the list rather than sent raw as TorBox's file id. /play
    /// sets it only after a movie link served a file far smaller than the release. Real-Debrid and
    /// Premiumize always pick from a list already and ignore it.
    pub list_files: bool,
    /// Title and IMDb are carried from the PlayTarget that named this resolve, diagnostic only — a store
    /// never reads them. They are what lets a play/probe/cancel decision line (decisionlog.go) name the
    /// release it is about instead of keying on shortHash(InfoHash) alone.
    pub title: String,
    pub imdb: String,
}

/// Go `statusAnswer`: three values, because "it is not being fetched" and "I could not find out" are
/// different facts and only the first may lead to an add — collapsing them into a bool is what let a
/// throttled account's 503 read as a definitive no.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StatusAnswer {
    /// statusNo — the store answered, and it is not fetching this release. Includes a store with no status
    /// API at all (Real-Debrid, Premiumize): they can never say more, so treating them as "unknown" would
    /// escalate on every request and never learn anything.
    No,
    Downloading,
    Unknown,
}

/// Go `StoreStatus` — how far along a queued release is. Progress is 0…1; ETASeconds is nil unless the
/// store reports a real one (a made-up countdown is worse than none).
#[derive(Debug, Clone, Default, PartialEq)]
pub struct StoreStatus {
    /// 0..=1
    pub progress: f64,
    /// None unless the store reports a real one.
    pub eta_seconds: Option<i64>,
    /// Bytes per second the service reports for this fetch. A percentage that moves slowly and one that has
    /// stopped look identical for minutes; the rate says which immediately, and zero says "stalled" while
    /// there is still time to pick something else.
    pub bytes_per_second: Option<i64>,
    /// One of the `FETCH_*` states, mapped from the service's own vocabulary; empty means the service named
    /// none, and is answered as "downloading" — the only value clients before it knew.
    pub state: String,
    /// The swarm as the service reports it: zero seeds is what tells a dead swarm from a slow one.
    pub seeds: Option<i64>,
    pub peers: Option<i64>,
    /// Service is the store that answered, set by the pool; the 503 body names a store the same way.
    pub service: Option<DebridService>,
}

/// The stable fetch states the 202 body carries.
pub const FETCH_QUEUED: &str = "queued";
pub const FETCH_FETCHING: &str = "fetching";
pub const FETCH_DOWNLOADING: &str = "downloading";
pub const FETCH_STALLED: &str = "stalled";
pub const FETCH_FAILED: &str = "failed";

/// Go `CancelOutcome` is what a cancel (or reannounce) attempt found. Done is true for a successful
/// delete or reannounce; otherwise Reason says why nothing was done — "not_queued" (nothing of ours to cancel),
/// "finished" (it already completed, so it is kept), or "not_ours" (the account held this before scout ever
/// asked for it). Status carries the current wait, for a reannounce's 202 answer.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct CancelOutcome {
    pub done: bool,
    pub reannounced: bool,
    /// "not_queued", "finished" or "not_ours" when nothing was done.
    pub reason: String,
    pub status: StoreStatus,
}

/// Go `DeadLinkError`: nothing could deliver the file, so the route answers 404.
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
///
/// errWouldAdd — a NoAdd target could only be resolved by queueing the torrent, so it was not resolved.
pub const ERR_WOULD_ADD: &str = "not held by this account (add not permitted)";
/// errTorrentGone — the service answered, and said it has no such torrent. The ONLY evidence that a
/// remembered torrent id is stale.
///
/// It has to be positive evidence, because the alternative was measured: treating any failure of the held path
/// as "the torrent is gone" meant a cancelled poll erased a perfectly good id and re-bought the torrent — ten
/// polls of one already-downloaded release cost ten adds against a sixty-an-hour ceiling. A cancellation, a
/// throttle, a timeout, and a pack that lacks the requested episode say nothing at all about whether the
/// account still holds the file.
pub const ERR_TORRENT_GONE: &str = "torbox no longer has this torrent";
/// errCheckFailed marks a cache check that could not reach the store at all.
pub const ERR_CHECK_FAILED: &str = "cache check failed";

/// Go `StoreUnavailableError` — the SERVICE refused us, which says nothing about the release. A rate
/// limit or a 5xx answered as 404 told the client "this release is dead" and it fell through every remaining
/// source getting the same answer, then sat on a wait for a download nobody had started. Distinct so the route
/// can answer 503 and the app can say the debrid is the problem.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoreUnavailableError {
    pub service: DebridService,
    pub reason: String,
    /// Status — the HTTP status the service answered with, when there was one (0 when none). Carried rather
    /// than parsed back out of Reason: that string embeds the service's own words, including up to 200 bytes
    /// of a non-JSON body, so matching "401"/"403" in it classified a rate-limit detail ("retry in 403 ms")
    /// and a Cloudflare Ray ID as a rejected key and silenced a healthy account. Deriving a fact from prose
    /// when the fact itself is available at the call site is the same mistake isPermanentFailure was deleted
    /// for.
    pub status: u16,
}

impl fmt::Display for StoreUnavailableError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "store_unavailable: {} {}", self.service.as_str(), self.reason)
    }
}

/// What a store call can fail with: Go returns `error` and callers branch on the concrete type
/// (`*DeadLinkError`, `*StoreUnavailableError`, `errAddInFlight`, `errAddQueued`, context cancellation).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StoreError {
    DeadLink(DeadLinkError),
    Unavailable(StoreUnavailableError),
    /// Go `errScoutSide` wrapping a `StoreUnavailableError`: a refusal scout made itself (its own add
    /// allowance), not the service. `recordRefusal` never remembers it.
    ScoutSide(StoreUnavailableError),
    /// Go `errAddInFlight` — an add for this release is already out and unanswered, so the release IS being
    /// fetched. Distinct from the hourly budget: this is per hash and per service, says nothing about the
    /// account, and its honest answer is "coming", not "refused". Reported as a 503 naming the store, the tvOS
    /// client told the viewer their debrid was refusing and stopped trying other sources — for a release scout
    /// had itself queued moments earlier.
    AddInFlight,
    /// Go `errAddQueued` — createtorrent answered with a queued_id rather than a torrent_id: TorBox accepted
    /// the add but parked it behind the account's slot limit (`queued` setting on). The release IS being
    /// fetched, eventually, same as errAddInFlight — the two are told apart only so callers can log which
    /// happened. The route maps this to the same 202 "coming" a client already knows how to wait on.
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

/// Go `Store`: a debrid backend.
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

    /// Status reports on a release the store has been ASKED for but could not deliver yet: it is
    /// downloading, not dead. Without this the two are the same 404 to the client, which then blacklists a
    /// perfectly good release.
    ///
    /// `None` when this store knows nothing about the target — which deliberately conflates "it is not being
    /// fetched" with "I could not find out". A store that CAN tell those apart implements `status_answer` as
    /// well, and the pool prefers that; the two stores with no status API at all cannot, and for them `None`
    /// really is all there is to say.
    async fn status(&self, t: &ResolveTarget) -> Option<StoreStatus>;

    /// Go `statusAnswerer.StatusAnswer`, the optional, three-valued half of `status`. `None` when the store
    /// has no such method, in which case the pool uses `status`.
    ///
    /// Only TorBox has a status API, so only TorBox can be UNCERTAIN: a throttled 503, an unreadable account
    /// listing, a body that will not decode. As a plain bool every one of those read as a definitive "nobody
    /// is fetching it", so /play queued a duplicate — more load on an account already refusing, which is the
    /// one failure mode that feeds itself. Real-Debrid and Premiumize answer false always and mean it, so
    /// leaving them on the bool keeps "unknown" meaning something a caller can act on.
    async fn status_answer(&self, _t: &ResolveTarget) -> Option<(StoreStatus, StatusAnswer)> {
        None
    }

    /// Go `storeCanceller.Cancel`: the optional half of Store that can cancel (or reannounce) a release it is
    /// fetching. Only TorBox implements it — Real-Debrid's delete exists but is unbuilt until someone uses RD
    /// (see den#204), and Premiumize has no equivalent at all — so the route answers 501 "unsupported" for
    /// either. `None` when the store cannot cancel.
    async fn cancel(&self, _info_hash: &str, _reannounce: bool) -> Option<Result<CancelOutcome, StoreError>> {
        None
    }

    /// Go `refusalReporter.RecentRefusal` — a store that can say it was recently refused for a hash, with the
    /// reason. Optional, because only a store with a cache can remember one; a store that doesn't implement it
    /// simply never reports a refusal.
    fn recent_refusal(&self, _info_hash: &str) -> Option<String> {
        None
    }

    /// Go `addInFlightReporter.AddInFlight` — a store that can say whether an add of OURS is out and
    /// unanswered for a hash.
    fn add_in_flight(&self, _info_hash: &str) -> Option<bool> {
        None
    }

    /// Go `heldTorrentReporter.HoldsTorrent` — a store that already has an id for this release, so it can be
    /// resolved without buying anything. A pure cache read in every implementation.
    fn holds_torrent(&self, _t: &ResolveTarget) -> Option<bool> {
        None
    }

    /// Go `scoutBudgetReporter.ScoutBudgetSpent` — a store that can say whether SCOUT's own hourly allowance
    /// for its account is gone, without asking the service anything.
    fn scout_budget_spent(&self) -> Option<bool> {
        None
    }

    /// Go `accountRefusalReporter.AccountRefused` — a store whose ACCOUNT the service rejected a moment ago.
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

    /// Go's `accountSummarizer` (handler.go `accountBody` asserts `*torBoxStore` and calls `AccountSummary`):
    /// the account-wide facts /<config>/account reports. `None` when the store publishes none (Real-Debrid,
    /// Premiumize); `Some(Err)` is a read that failed, which the route degrades on.
    async fn account_summary(&self) -> Option<Result<AccountSummary, StoreError>> {
        None
    }
}

/// What /<config>/account reports for an account that publishes facts: Go's `torboxAccountSummary`, with the two
/// times already printed as RFC 3339 in the zone TorBox wrote them.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AccountSummary {
    pub plan: i64,
    pub cooldown_until: Option<String>,
    pub premium_expires_at: Option<String>,
    pub active: i64,
    pub queued: i64,
}

impl From<torbox::TorboxAccountSummary> for AccountSummary {
    fn from(s: torbox::TorboxAccountSummary) -> AccountSummary {
        AccountSummary {
            plan: s.plan,
            cooldown_until: s.cooldown_until.map(|t| t.to_rfc3339()),
            premium_expires_at: s.premium_expires_at.map(|t| t.to_rfc3339()),
            active: s.active,
            queued: s.queued,
        }
    }
}

/// Go `CacheTruth`: what the pool actually learned about a batch of hashes. Not a bare map of
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

/// Go `torboxFetchState` maps TorBox's `download_state` — documented as "downloading", "stalled (no seeds)",
/// "uploading (no peers)", "metaDL", "paused", "completed", "cached", "failed", "failed (processing)",
/// "expired", "missing", "incomplete" (gave up after two days of too few seeds) — onto the stable set. Matched
/// on lowercase prefixes because the documented spellings carry parenthesised qualifiers and casing varies.
/// Anything unrecognised is "downloading", which is what every wait was called before this existed.
pub fn torbox_fetch_state(raw: &str) -> &'static str {
    let s = str::to_lowercase(raw.trim());
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

/// Go `readStoreError` pulls the service's own explanation out of a failed response, as " (detail)" ready to
/// append to a message. TorBox answers `{"error":"ACTIVE_LIMIT","detail":"..."}`; anything unparseable falls
/// back to a clipped snippet of the raw body, because a truncated reason still beats none. Empty when the body
/// cannot be read.
pub(crate) async fn read_store_error(resp: &mut reqwest::Response) -> String {
    match read_limited(resp, 2048).await {
        Ok(raw) => store_error_text(&raw),
        Err(_) => String::new(),
    }
}

/// Go `io.ReadAll(io.LimitReader(body, limit))`: the body up to `limit` bytes, the rest unread.
pub(crate) async fn read_limited(
    resp: &mut reqwest::Response,
    limit: usize,
) -> Result<Vec<u8>, reqwest::Error> {
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
static STORE_ERROR_SHAPE: crate::util::Shape =
    crate::util::Shape::Object(&[("error", &crate::util::LEAF), ("detail", &crate::util::LEAF)]);

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

/// Go `storeErrorText` is `read_store_error` over bytes already in hand, for a caller that had to read the body
/// once for its own sake — RD's add answers with the torrent id and the reason for a failure in the same place,
/// and decoding straight off the reader left nothing behind to explain a failure with. TorBox answers
/// `{"error":"ACTIVE_LIMIT","detail":"..."}`; anything unparseable falls back to a clipped snippet of the raw
/// body, because a truncated reason still beats none.
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
pub(crate) fn decode_first<T: serde::de::DeserializeOwned>(
    raw: &[u8],
    shape: &crate::util::Shape,
) -> Option<T> {
    let mut stream = serde_json::Deserializer::from_slice(raw).into_iter::<serde_json::Value>();
    let mut value = stream.next()?.ok()?;
    crate::util::fold_json_keys(&mut value, shape);
    serde_json::from_value(value).ok()
}

/// Go's `json.Unmarshal(raw, &v)`: the whole document must be valid JSON, then it is decoded the same way.
pub(crate) fn decode_all<T: serde::de::DeserializeOwned>(
    raw: &[u8],
    shape: &crate::util::Shape,
) -> Option<T> {
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
