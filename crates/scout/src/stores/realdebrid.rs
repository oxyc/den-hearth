//! Port of the Real-Debrid half of `internal/scout/stores.go` (lines 3770-4316): `realDebridStore`.
//! Owned by the "Stores II" work package.

use std::sync::Arc;

use crate::cache::Cache;
use crate::scrape::Doer;

/// Go `realDebridAPI`.
pub const REAL_DEBRID_API: &str = "https://api.real-debrid.com/rest/1.0";

/// Go `realDebridStore`.
pub struct RealDebridStore {
    pub token: String,
    pub client: Arc<dyn Doer>,
    pub cache: Option<Arc<dyn Cache>>,
    pub api: String,
}
