//! Port of `internal/scout/deps.go`: `Settings` from the environment, the startup summary line, the
//! redirect policy every debrid-facing client carries, and `BuildDeps`. `Settings` is FROZEN (see
//! PORTING.md): the handler, metrics and the startup line all read it.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use crate::cache::Cache;
use crate::config::{Config, Indexer};
use crate::scrape::Scraper;
use crate::seal::SealKeyring;
use crate::stores::Store;

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

/// Go `StartupSummary`: the one line an operator reads to confirm what this process is running with. Nothing
/// secret goes in it: an indexer URL override can carry an encrypted config segment, so overrides are named
/// by indexer, never by address, and keys and tokens appear only as on or off.
pub fn startup_summary(s: &Settings, persistent: bool) -> String {
    todo!("deps.go StartupSummary")
}

/// Go `SettingsFromEnv`. `get` returns "" for an unset variable, as Go's `os.Getenv` does.
pub fn settings_from_env(get: impl Fn(&str) -> String) -> Settings {
    todo!("deps.go SettingsFromEnv")
}

/// Go `parsePlayReserve`. Zero is a real value (no reserve), so it is kept; anything that does not parse, or
/// that would leave a prefetch no adds at all, is said and read as the default.
pub(crate) fn parse_play_reserve(v: &str) -> i64 {
    todo!("deps.go parsePlayReserve")
}

/// Go `parseRevokedInstalls`: a comma-separated list of install ids. A malformed entry is skipped and said.
pub(crate) fn parse_revoked_installs(v: &str) -> Vec<String> {
    todo!("deps.go parseRevokedInstalls")
}

/// Go `parseConfigEpoch`. A value that does not parse is said loudly and read as 0, which refuses nothing.
pub(crate) fn parse_config_epoch(v: &str) -> i64 {
    todo!("deps.go parseConfigEpoch")
}

/// Go `parseLegacyPlayUntil`: unset (or no tickets to move to) leaves the legacy route open (`None`), and a
/// value that does not parse is said loudly and read as already passed (the Unix epoch).
pub(crate) fn parse_legacy_play_until(v: &str, tickets_on: bool) -> Option<SystemTime> {
    todo!("deps.go parseLegacyPlayUntil")
}

/// Go `RefuseRedirectReplay`: the redirect policy every client touching a debrid must carry. A redirect
/// must never replay a request that carries a body, and a credential in the query must not follow a redirect
/// off the debrid's API host. GET and HEAD keep following redirects. Returns `true` when the redirect must
/// be refused, so the caller sees the 3xx it is (Go's `http.ErrUseLastResponse`).
///
/// Takes the method and URL of the redirected request and of each earlier hop (`via`, oldest first), not a
/// `reqwest::Request`: reqwest's redirect `Attempt` exposes the URLs of the earlier hops but not their
/// methods, so the caller supplies them.
pub fn refuse_redirect_replay(
    method: &reqwest::Method,
    url: &reqwest::Url,
    via: &[(reqwest::Method, reqwest::Url)],
) -> bool {
    todo!("deps.go RefuseRedirectReplay")
}

/// Go `Deps`: injects the environment (the cache, timeouts, public origin, and the scraper and store
/// factories, mocked in tests). Go declares it in `handler.go`; it lives here because `BuildDeps` is what
/// fills it. The `Meta` field (Go `cineMeta`) is omitted until package 5 lands `cinemeta.rs`.
pub struct Deps {
    pub cache: Arc<dyn Cache>,
    /// Client for reading the head of a resolved link. Separate from the scraper client so a probe's longer
    /// body read can't tie up the timeouts tuned for indexer queries. `None` means no probe runs.
    pub probe_client: Option<reqwest::Client>,
    pub scrape_timeout: Duration,
    pub list_ttl: Duration,
    /// Fixed public origin; when empty, fall back to forwarded headers.
    pub public_url: String,
    pub make_scrapers: Arc<dyn Fn(&Config) -> Vec<Box<dyn Scraper>> + Send + Sync>,
    pub make_stores: Arc<dyn Fn(&Config) -> Vec<Box<dyn Store>> + Send + Sync>,
    /// Decrypts a sealed config path segment (docs/SEALED-CONFIG.md). None = sealed URLs disabled (legacy
    /// plaintext still works); the current key's public half is served at /config-key.
    pub seal_keyring: Option<Arc<SealKeyring>>,
    /// Gates /metrics. Empty disables the route entirely (404).
    pub metrics_token: String,
    /// One redacted line per response (reqlog.go).
    pub log_requests: bool,
    /// REVOKED_INSTALLS: install ids refused outright. CONFIG_EPOCH refuses every config minted under an older
    /// epoch. Both reach play tickets too. See admitInstall.
    pub revoked_installs: HashSet<String>,
    pub config_epoch: i64,
    /// Refuse a full config with no install id (REQUIRE_INSTALL_ID); see `Settings::require_install_id`.
    pub require_install_id: bool,
    /// den-remux's service key (REMUX_KEY); see `Settings::remux_key` and refuseScoped.
    pub remux_key: String,
    /// How long a /p/ ticket stays good; zero takes the default.
    pub play_ticket_ttl: Duration,
    /// Closes the /<config>/play route once passed, while tickets are on. None leaves it open.
    pub legacy_play_until: Option<SystemTime>,
}

/// Go `BuildDeps`: wires the core to a concrete HTTP client and cache.
pub fn build_deps(settings: &Settings, client: reqwest::Client, cache: Arc<dyn Cache>) -> Deps {
    todo!("deps.go BuildDeps")
}

/// Go `buildKeyring`: parses the sealed-config keyring from env. A malformed key disables sealed URLs (legacy
/// plaintext keeps working) and logs loudly rather than crashing the addon.
pub(crate) fn build_keyring(current: &str, prev: &str) -> Option<SealKeyring> {
    todo!("deps.go buildKeyring")
}
