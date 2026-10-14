//! Port of `internal/scout/deps.go`: `Settings` from the environment, the startup summary line, the
//! redirect policy every debrid-facing client carries, and `BuildDeps`. `Settings` is FROZEN (see
//! PORTING.md): the handler, metrics and the startup line all read it.

use std::collections::HashMap;
use std::time::{Duration, SystemTime};

use crate::config::Indexer;

/// FROZEN. Go `Settings`: configuration from env so the same binary runs anywhere. No debrid secret
/// lives here; the token rides in the per-install addon URL. Go's zero `time.Time` is `None`.
#[derive(Debug, Clone)]
pub struct Settings {
    pub port: String,
    pub scrape_timeout: Duration,
    pub list_ttl: Duration,
    pub public_url: String,
    pub indexer_urls: HashMap<Indexer, String>,
    /// Byte budget for the in-memory cache.
    pub cache_bytes: usize,
    /// Where the durable tier writes. Empty disables persistence.
    pub cache_dir: String,
    /// Metadata source for the year mistag filter (default: public Cinemeta).
    pub cinemeta_url: String,
    /// CONFIG_KEY: the current X25519 private key (base64). Empty disables sealed URLs.
    pub config_key: String,
    /// CONFIG_KEYS_PREV: comma-separated prior keys.
    pub config_keys_prev: String,
    /// Bearer token for /metrics. Empty means the route 404s.
    pub metrics_token: String,
    /// MINT_INDEXER_CONFIGS: send the debrid token to comet and mediafusion to mint their config segments.
    pub mint_indexer_configs: bool,
    pub log_requests: bool,
    pub log_identity: bool,
    /// REVOKED_INSTALLS: valid install ids.
    pub revoked_installs: Vec<String>,
    /// CONFIG_EPOCH.
    pub config_epoch: i64,
    pub require_install_id: bool,
    /// REMUX_KEY: den-remux's service key.
    pub remux_key: String,
    /// PLAY_TICKET_TTL_SECS.
    pub play_ticket_ttl: Duration,
    /// LEGACY_PLAY_UNTIL: when the `/<config>/play` route closes. `None` means never.
    pub legacy_play_until: Option<SystemTime>,
    /// PLAY_RESERVE_ADDS.
    pub play_reserve_adds: i64,
}
