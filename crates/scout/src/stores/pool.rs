//! Port of the pool half of `internal/scout/stores.go` (lines 4722-end): `StorePool`, `buildStores`,
//! `CacheCheck`/`Resolve`/`ResolvePreferring`/`ResolveCachedOnly`/`Status` across stores. Owned by the
//! "Stores II" work package.

use crate::config::DebridService;
use crate::stores::{CacheTruth, ResolveTarget, Store, StoreError};

/// Go `StorePool`: one store per account in service-priority order (TorBox first).
pub struct StorePool {
    pub(crate) stores: Vec<Box<dyn Store>>,
}

impl StorePool {
    /// Go `StorePool.CacheCheck` (stores.go:4783): asks every store that has a cache API, and reports
    /// whether any of them answered at all.
    pub async fn cache_check(&self, hashes: &[String]) -> (CacheTruth, bool) {
        todo!("stores.go StorePool.CacheCheck")
    }

    /// Go `StorePool.Resolve` (stores.go:4865), in configured order.
    pub async fn resolve(&self, t: &ResolveTarget) -> Result<String, StoreError> {
        todo!("stores.go StorePool.Resolve")
    }

    /// Go `StorePool.ResolvePreferring` (stores.go:4879): the preferred services first, then the rest.
    pub async fn resolve_preferring(
        &self,
        t: &ResolveTarget,
        preferred: &[DebridService],
    ) -> Result<String, StoreError> {
        todo!("stores.go StorePool.ResolvePreferring")
    }

    /// Go `StorePool.ResolveCachedOnly` (stores.go:5013): only the services that hold the release.
    pub async fn resolve_cached_only(
        &self,
        t: &ResolveTarget,
        holders: &[DebridService],
    ) -> Result<String, StoreError> {
        todo!("stores.go StorePool.ResolveCachedOnly")
    }
}
