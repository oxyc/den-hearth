//! Port of the Premiumize half of `internal/scout/stores.go` (lines 4316-4722): `premiumizeStore`.
//! Owned by the "Stores II" work package.

use std::sync::Arc;

use async_trait::async_trait;

use crate::cache::Cache;
use crate::config::DebridService;
use crate::scrape::Doer;
use crate::stores::{ResolveTarget, Store, StoreError, StoreStatus};

/// Go `premiumizeAPI`.
pub const PREMIUMIZE_API: &str = "https://www.premiumize.me/api";

/// Go `premiumizeStore`.
pub struct PremiumizeStore {
    pub token: String,
    pub client: Arc<dyn Doer>,
    pub cache: Option<Arc<dyn Cache>>,
    pub api: String,
}

#[async_trait]
impl Store for PremiumizeStore {
    fn service(&self) -> DebridService {
        todo!("stores.go premiumizeStore.Service")
    }

    async fn cache_check(
        &self,
        hashes: &[String],
    ) -> Result<std::collections::HashMap<String, bool>, StoreError> {
        todo!("stores.go premiumizeStore.CacheCheck")
    }

    async fn resolve(&self, t: &ResolveTarget) -> Result<String, StoreError> {
        todo!("stores.go premiumizeStore.Resolve")
    }

    async fn status(&self, t: &ResolveTarget) -> Option<StoreStatus> {
        todo!("stores.go premiumizeStore.Status")
    }

    fn recent_refusal(&self, info_hash: &str) -> Option<String> {
        todo!("stores.go premiumizeStore.RecentRefusal")
    }

    fn add_in_flight(&self, info_hash: &str) -> Option<bool> {
        todo!("stores.go premiumizeStore.AddInFlight")
    }
}
