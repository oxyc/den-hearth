//! Port of `internal/scout/config.go`: the per-install config carried in the URL path segment, and its
//! validation. The types here are shared by almost every other module and are FROZEN (see PORTING.md);
//! the functions are owned by the work package that owns this file.

use crate::seal::SealKeyring;

/// FROZEN. Go `DebridService` (a string type); the three services, in resolve priority order.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum DebridService {
    /// "torbox"
    TorBox,
    /// "realdebrid"
    RealDebrid,
    /// "premiumize"
    Premiumize,
}

impl DebridService {
    /// The wire spelling: "torbox", "realdebrid", "premiumize".
    pub fn as_str(&self) -> &'static str {
        todo!("config.go DebridService constants")
    }

    /// Go `isDebridService` for a single value: `None` for anything not one of the three.
    pub fn parse(s: &str) -> Option<DebridService> {
        todo!("config.go isDebridService")
    }
}

/// Go `debridServices`: the resolve/priority order (TorBox first, it has the real cache API).
pub const DEBRID_SERVICES: [DebridService; 3] =
    [DebridService::TorBox, DebridService::RealDebrid, DebridService::Premiumize];

/// FROZEN. Go `Indexer` (a string type): "torrentio", "comet", "mediafusion" or "torz".
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Indexer(pub String);

/// Go `allIndexers`: every indexer a config may name.
pub const ALL_INDEXERS: [&str; 4] = ["torrentio", "comet", "mediafusion", "torz"];

/// Go `defaultIndexers`: what an install gets when it names none.
pub const DEFAULT_INDEXERS: [&str; 2] = ["torrentio", "mediafusion"];

/// Go `configPathIndexers`: indexer -> the env var naming its per-install config URL.
pub const CONFIG_PATH_INDEXERS: [(&str, &str); 3] =
    [("comet", "COMET_URL"), ("mediafusion", "MEDIAFUSION_URL"), ("torz", "TORZ_URL")];

/// Go `disabledIndexers`: named but known-broken indexers, removed from any config, with why.
pub const DISABLED_INDEXERS: [(&str, &str); 1] = [("torz", "host no longer resolves (no DNS)")];

/// Go `validResolutions`.
pub const VALID_RESOLUTIONS: [&str; 4] = ["2160p", "1080p", "720p", "480p"];

/// Ceiling on the base64 config path segment. See `decode_config`.
pub const MAX_CONFIG_BLOB: usize = 16 << 10;
/// Ceiling on accounts in one config.
pub const MAX_DEBRID_ACCOUNTS: usize = 8;
/// Ceiling on results in one response.
pub const MAX_RESULT_CAP: i64 = 50;

/// Go `scopeAvailability`: the read-only scope a browser holds.
pub const SCOPE_AVAILABILITY: &str = "availability";

/// An install id is 16 random bytes, unpadded base64url.
pub const INSTALL_ID_BYTES: usize = 16;
pub const INSTALL_ID_LEN: usize = 22;
/// Ceiling on an epoch, so it always fits an int and a config cannot carry an absurd one.
pub const MAX_CONFIG_EPOCH: i64 = i32::MAX as i64;

/// FROZEN. Go `DebridAccount`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DebridAccount {
    pub service: DebridService,
    pub token: String,
}

/// FROZEN. Go `Filters`. Go's `*int` is `Option<i64>`; `""` for a string means unset, as in Go.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Filters {
    pub exclude_cam: bool,
    /// Empty means all.
    pub resolutions: Vec<String>,
    /// Sinks every OTHER resolution below this one instead of dropping it. "" means no preference.
    pub prefer_resolution: String,
    pub hdr_only: bool,
    /// None means no filter.
    pub min_seeders: Option<i64>,
    /// None means no filter.
    pub max_size_gb: Option<i64>,
    pub exclude_regex: String,
}

/// FROZEN. Go `Config`: a validated install config.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Config {
    pub debrid: Vec<DebridAccount>,
    pub indexers: Vec<Indexer>,
    /// The household's own stream addons (sources.go), as base URLs, scraped beside `indexers`.
    pub sources: Vec<String>,
    pub filters: Filters,
    pub cached_only: bool,
    pub result_cap: i64,
    /// "" is the full config; `SCOPE_AVAILABILITY` is the read-only one a browser holds.
    pub scope: String,
    /// The install the config was minted for, so REVOKED_INSTALLS can refuse it. "" for a config minted
    /// before installs had ids.
    pub iid: String,
    /// The CONFIG_EPOCH the config was minted under (0 when absent).
    pub epoch: i64,
    /// Indexers the config names that `DISABLED_INDEXERS` removed. Never serialised in Go (`json:"-"`).
    pub quarantined: Vec<Indexer>,
}

/// Go `validInstallID`: an install id in its one canonical spelling (strict base64url, 16 bytes).
pub fn valid_install_id(s: &str) -> bool {
    todo!("config.go validInstallID")
}

/// Go `hasCountedRepeat`: whether a user-supplied regex contains a `{n}` / `{n,}` / `{n,m}` quantifier,
/// scanned with real escape state.
pub fn has_counted_repeat(s: &str) -> bool {
    todo!("config.go hasCountedRepeat")
}

/// Go `decodeConfig`: the config path segment into a validated config, or `None` (the route answers 400).
/// Sealed with no keyring, a decrypt failure, a plaintext config while sealing is on, and a scope on a
/// plaintext config all fail closed.
pub fn decode_config(kr: Option<&SealKeyring>, blob: &str) -> Option<Config> {
    todo!("config.go decodeConfig")
}

/// Go `validateConfig`, taking the decoded JSON. Go's `rawConfig` is the untrusted wire shape; this takes
/// it as JSON text until the porter decides how to model it.
pub fn validate_config(raw_json: &[u8]) -> Option<Config> {
    todo!("config.go validateConfig")
}

/// Go `isIndexer`.
pub fn is_indexer(s: &str) -> bool {
    todo!("config.go isIndexer")
}

/// Go `dedupeIndexers`.
pub fn dedupe_indexers(indexers: Vec<Indexer>) -> Vec<Indexer> {
    todo!("config.go dedupeIndexers")
}

/// Go `clampInt`.
pub fn clamp_int(v: i64, lo: i64, hi: i64) -> i64 {
    todo!("config.go clampInt")
}

/// Go `clampPosInt`: rounds `v`; `None` if absent, non-finite or <= 0, else clamped to [1, max].
pub fn clamp_pos_int(v: Option<f64>, max: i64) -> Option<i64> {
    todo!("config.go clampPosInt")
}

/// Go `isFinite`.
pub fn is_finite(f: f64) -> bool {
    todo!("config.go isFinite")
}

/// Go `containsString`.
pub fn contains_string(xs: &[String], want: &str) -> bool {
    todo!("config.go containsString")
}
