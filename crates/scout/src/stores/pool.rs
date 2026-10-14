//! Port of the pool half of `internal/scout/stores.go` (lines 4722-end): `StorePool`, `buildStores`,
//! `CacheCheck`/`Resolve`/`ResolvePreferring`/`ResolveCachedOnly`/`Status` across stores. Owned by the
//! "Stores II" work package.

use crate::stores::Store;

/// Go `StorePool`: one store per account in service-priority order (TorBox first).
pub struct StorePool {
    pub(crate) stores: Vec<Box<dyn Store>>,
}
