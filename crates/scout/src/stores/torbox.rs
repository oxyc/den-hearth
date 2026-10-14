//! Port of the TorBox half of `internal/scout/stores.go` (lines 446-3770 less the refusal helpers in
//! `refusal.rs`): `torBoxStore`, its check-cached batcher, account listing, status, cancel and file
//! selection. Owned by the "Stores I" work package.

use std::sync::Arc;

use crate::cache::Cache;
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
