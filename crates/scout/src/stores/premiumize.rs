//! Port of the Premiumize half of `internal/scout/stores.go` (lines 4316-4722): `premiumizeStore`.
//! Owned by the "Stores II" work package.

use std::sync::Arc;

use crate::cache::Cache;
use crate::scrape::Doer;

/// Go `premiumizeAPI`.
pub const PREMIUMIZE_API: &str = "https://www.premiumize.me/api";

/// Go `premiumizeStore`.
pub struct PremiumizeStore {
    pub token: String,
    pub client: Arc<dyn Doer>,
    pub cache: Option<Arc<dyn Cache>>,
    pub api: String,
}
