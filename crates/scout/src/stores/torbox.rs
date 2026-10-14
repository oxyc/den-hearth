//! Port of the TorBox half of `internal/scout/stores.go` (lines 446-3770 less the refusal helpers in
//! `refusal.rs`): `torBoxStore`, its check-cached batcher, account listing, status, cancel and file
//! selection. Owned by the "Stores I" work package.

use std::sync::Arc;

use std::collections::HashMap;

use async_trait::async_trait;

use super::{ResolveTarget, StatusAnswer, Store, StoreError, StoreStatus};
use crate::cache::Cache;
use crate::config::DebridService;
use crate::scrape::Doer;

/// Go `torboxAPI`.
pub const TORBOX_API: &str = "https://api.torbox.app/v1/api";

/// Go `torBoxStore`.
pub struct TorBoxStore {
    pub token: String,
    pub client: Arc<dyn Doer>,
    pub cache: Option<Arc<dyn Cache>>,
    pub api: String,
}

impl TorBoxStore {
    /// Go `torBoxStore.accountIdentity`: the cache identity of this token on this API endpoint.
    pub(crate) fn account_identity(&self) -> String {
        todo!("stores.go torBoxStore.accountIdentity")
    }

    /// Go `torBoxStore.requestDownload`: asks TorBox for the playable link of one file of a torrent.
    pub(crate) async fn request_download(&self, torrent_id: i64, file_id: Option<i64>) -> Result<String, StoreError> {
        todo!("stores.go torBoxStore.requestDownload")
    }

    /// Go `torBoxStore.addMagnet`: adds the magnet, returning (torrent id, queued id). Exactly one is set.
    pub(crate) async fn add_magnet(
        &self,
        info_hash: &str,
        prefetch: bool,
        only_if_cached: bool,
    ) -> Result<(i64, i64), StoreError> {
        todo!("stores.go torBoxStore.addMagnet")
    }

    /// Go `torBoxStore.knownQueuedID`: the queue entry this store remembers for the hash, if any.
    pub(crate) fn known_queued_id(&self, info_hash: &str) -> Option<i64> {
        todo!("stores.go torBoxStore.knownQueuedID")
    }

    /// Go `torBoxStore.torrentID`: (torrent id, found, the answer is authoritative).
    pub(crate) async fn torrent_id(&self, info_hash: &str) -> (i64, bool, bool) {
        todo!("stores.go torBoxStore.torrentID")
    }

    /// Go `torBoxStore.accountListing`: the account's hash to torrent id map, and whether it is an answer.
    pub(crate) async fn account_listing(&self) -> (HashMap<String, i64>, bool) {
        todo!("stores.go torBoxStore.accountListing")
    }

    /// Go `torBoxStore.fetchAccountListing`: the listing read, its answer, and whether to remember a failure.
    pub(crate) async fn fetch_account_listing(&self) -> (HashMap<String, i64>, bool, bool) {
        todo!("stores.go torBoxStore.fetchAccountListing")
    }

    /// Go `torBoxStore.fetchCached`: one checkcached round, batched and bounded by the outbound limiter.
    pub(crate) async fn fetch_cached(&self, hashes: &[String]) -> TorboxCheckResult {
        todo!("stores.go torBoxStore.fetchCached")
    }
}

/// Go `torboxCheckResult`. Only the field the tests read is declared here; the porter adds the rest.
pub(crate) struct TorboxCheckResult {
    pub(crate) ok: bool,
}

/// Go `maxTorboxOutbound`: the most TorBox calls in flight at once.
pub(crate) const MAX_TORBOX_OUTBOUND: usize = 64;
/// Go `cacheBatch`: TorBox hashes per checkcached call.
pub(crate) const CACHE_BATCH: usize = 100;
/// Go `maxListingEntries`: the most entries an account listing may hold before it is refused.
pub(crate) const MAX_LISTING_ENTRIES: usize = 50_000;

/// Go `listingFault`: why an account listing could not be read, and whether that is worth remembering.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ListingFault {
    None,
    BadEnvelope,
    TooManyEntries,
    NoUsableEntries,
}

/// Go `decodeListing`: decodes the account listing from a reader, element by element.
pub(crate) fn decode_listing<R: std::io::Read>(r: R) -> (HashMap<String, i64>, bool, ListingFault) {
    todo!("stores.go decodeListing")
}

/// Go `truncationDetector`: reads through to `r` and records whether it ran past `limit` bytes.
pub(crate) struct TruncationDetector<R> {
    pub(crate) r: R,
    pub(crate) limit: usize,
}

impl<R: std::io::Read> std::io::Read for TruncationDetector<R> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        todo!("stores.go truncationDetector.Read")
    }
}

impl<R> TruncationDetector<R> {
    /// Go `truncationDetector.truncated`.
    pub(crate) fn truncated(&self) -> bool {
        todo!("stores.go truncationDetector.truncated")
    }
}

/// Go `wasAddedByUs`: whether scout itself added this hash under this account, so a cancel may touch it.
pub(crate) fn was_added_by_us(cache: &dyn Cache, identity: &str, info_hash: &str) -> bool {
    todo!("stores.go wasAddedByUs")
}

#[async_trait]
impl Store for TorBoxStore {
    fn service(&self) -> DebridService {
        todo!("stores.go torBoxStore.Service")
    }

    async fn cache_check(&self, hashes: &[String]) -> Result<HashMap<String, bool>, StoreError> {
        todo!("stores.go torBoxStore.CacheCheck")
    }

    async fn resolve(&self, t: &ResolveTarget) -> Result<String, StoreError> {
        todo!("stores.go torBoxStore.Resolve")
    }

    async fn status(&self, t: &ResolveTarget) -> Option<StoreStatus> {
        todo!("stores.go torBoxStore.Status")
    }

    async fn status_answer(&self, t: &ResolveTarget) -> Option<(StoreStatus, StatusAnswer)> {
        todo!("stores.go torBoxStore.StatusAnswer")
    }
}
