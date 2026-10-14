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

use async_trait::async_trait;

use crate::config::DebridService;

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
        todo!("stores.go CacheTruth.Cached")
    }

    /// At least one cache-truth store gave an answer for this hash, either way.
    pub fn known(&self, hash: &str) -> bool {
        todo!("stores.go CacheTruth.Known")
    }

    /// Every cache-truth store answered.
    pub fn complete(&self) -> bool {
        todo!("stores.go CacheTruth.Complete")
    }

    /// The services that confirmed they hold it, in configured priority order.
    pub fn held_by(&self, hash: &str) -> Vec<DebridService> {
        todo!("stores.go CacheTruth.HeldBy")
    }
}

/// Go `torboxFetchState`: TorBox's `download_state` onto the stable `FETCH_*` set.
pub fn torbox_fetch_state(raw: &str) -> &'static str {
    todo!("stores.go torboxFetchState")
}

/// Go `rdFetchState`: a Real-Debrid status already accepted as fetching onto the stable set.
pub fn rd_fetch_state(status: &str) -> &'static str {
    todo!("stores.go rdFetchState")
}

/// Go `magnetFor`.
pub fn magnet_for(info_hash: &str) -> String {
    todo!("stores.go magnetFor")
}

/// Go `redactToken`.
pub fn redact_token(text: &str, token: &str) -> String {
    todo!("stores.go redactToken")
}

/// Go `shortHash`.
pub fn short_hash(h: &str) -> String {
    todo!("stores.go shortHash")
}
