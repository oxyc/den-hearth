//! Port of the TorBox half of `internal/scout/stores.go` (lines 446-3770 less the refusal helpers in
//! `refusal.rs`): `torBoxStore`, its check-cached batcher, account listing, status, cancel and file
//! selection. Owned by the "Stores I" work package.

use std::sync::Arc;

use std::collections::HashMap;

use async_trait::async_trait;

use super::{ResolveTarget, Store, StoreError, StoreStatus};
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
}
