//! Port of the Real-Debrid half of `internal/scout/stores.go` (lines 3770-4316): `realDebridStore`.
//! Owned by the "Stores II" work package.

use std::sync::Arc;

use async_trait::async_trait;

use crate::cache::Cache;
use crate::config::DebridService;
use crate::scrape::Doer;
use crate::season::TorrentFile;
use crate::stores::{ResolveTarget, Store, StoreError, StoreStatus};

/// Go `realDebridAPI`.
pub const REAL_DEBRID_API: &str = "https://api.real-debrid.com/rest/1.0";

/// Go `realDebridStore`.
pub struct RealDebridStore {
    pub token: String,
    pub client: Arc<dyn Doer>,
    pub cache: Option<Arc<dyn Cache>>,
    pub api: String,
}

/// Go `rdTorrentKey` (stores.go:4015): the cache key of the Real-Debrid torrent id for a release, with the
/// season and episode selector in it.
pub(crate) fn rd_torrent_key(token: &str, info_hash: &str, t: &ResolveTarget) -> String {
    todo!("stores.go rdTorrentKey")
}

impl RealDebridStore {
    /// Go `realDebridStore.pickFileID` (stores.go:4269): the file id to unrestrict. Name match first, then
    /// the caller's position mapped to RD's own id, then the largest video; a refusal when the pack names
    /// episodes and not the one asked for.
    pub(crate) fn pick_file_id(&self, files: &[TorrentFile], t: &ResolveTarget) -> Result<Option<i64>, StoreError> {
        todo!("stores.go realDebridStore.pickFileID")
    }
}

#[async_trait]
impl Store for RealDebridStore {
    fn service(&self) -> DebridService {
        todo!("stores.go realDebridStore.Service")
    }

    async fn cache_check(
        &self,
        hashes: &[String],
    ) -> Result<std::collections::HashMap<String, bool>, StoreError> {
        todo!("stores.go realDebridStore.CacheCheck")
    }

    async fn resolve(&self, t: &ResolveTarget) -> Result<String, StoreError> {
        todo!("stores.go realDebridStore.Resolve")
    }

    async fn status(&self, t: &ResolveTarget) -> Option<StoreStatus> {
        todo!("stores.go realDebridStore.Status")
    }

    fn recent_refusal(&self, info_hash: &str) -> Option<String> {
        todo!("stores.go realDebridStore.RecentRefusal")
    }

    fn add_in_flight(&self, info_hash: &str) -> Option<bool> {
        todo!("stores.go realDebridStore.AddInFlight")
    }
}
