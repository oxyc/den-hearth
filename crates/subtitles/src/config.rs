//! Runtime configuration from the environment. No user credentials live here — both the OpenSubtitles
//! key and the LLM key are BYOK and ride in the install URL (see `userconfig.rs`). What lives here
//! is addon-level infrastructure: binary paths, cache sizing, the public origin, the addon's own
//! sealing key and metrics token, and which installs it refuses.

use std::env;
use std::path::PathBuf;

pub struct Config {
    pub port: u16,
    pub cache_dir: PathBuf,
    pub cache_max_bytes: u64,
    /// Fixed public origin for building `/subtitle/…` URLs we hand back; falls back to forwarded
    /// headers when unset.
    pub public_base_url: Option<String>,
    pub alass: String,
    pub ffmpeg: String,
    /// Sealed config-in-URL (den-scout/docs/SEALED-CONFIG.md). `config_key` = current X25519 private key
    /// (base64); `config_keys_prev` = comma-separated prior keys (rotation). Empty → sealed URLs disabled.
    pub config_key: String,
    pub config_keys_prev: String,
    /// Installs refused outright (`REVOKED_INSTALLS`) and the oldest config epoch still admitted
    /// (`CONFIG_EPOCH`). See `userconfig::Revocation`.
    pub revocation: crate::userconfig::Revocation,
    /// Bearer token for `/metrics`. Empty → the route answers 404, like any unknown path.
    pub metrics_token: String,
    /// One stderr line per request (`LOG_REQUESTS`), off by default. Read once here so that, off, the
    /// request path pays a single bool check.
    pub log_requests: bool,
    /// Whether a decision-log line (`logging::Line`) may carry identity fields — title id,
    /// season/episode, file ids, language, release names (`LOG_IDENTITY`). On by default: those
    /// fields are what makes "wrong/missing subtitles" diagnosable from the journal at all, so the
    /// flag exists to turn them off for a deploy that must not keep that detail, not to make
    /// diagnosis the opt-in case. The rest of a line (event, outcome, reason, timing, rid) is
    /// unaffected either way.
    pub log_identity: bool,
    /// OpenSubtitles API root. A field so tests can point the request path at a local server instead
    /// of the live API — several of them reach `handle_translate`, which searches before it can do
    /// anything else, and without this they made a real request to api.opensubtitles.com on every
    /// `cargo test`. They passed either way, which is what made it easy to miss.
    pub os_api_base: String,
    /// Origins a Tier-2 `?resync=` target may be at (`SCOUT_ORIGINS`, comma-separated) — the den-scout
    /// origin(s) the app's stream URLs carry. Empty → Tier 2 is off. See `resync.rs` for why there is
    /// no open default.
    pub scout_origins: Vec<crate::resync::Origin>,
    /// Public scout names fetched at their LAN address (`SCOUT_ALIASES`, `resync::parse_aliases`).
    pub scout_aliases: Vec<(crate::resync::Origin, crate::resync::Origin)>,
}

#[cfg(test)]
fn env_opt(key: &str) -> Option<String> {
    match env::var(key) {
        Ok(v) if !v.trim().is_empty() => Some(v.trim().to_string()),
        _ => None,
    }
}

/// `LOG_IDENTITY`: on unless it is `0`, `false`, `off` or `no` (trimmed, any case). Every Den addon
/// reads it by this rule.
pub(crate) fn log_identity_on(v: Option<&str>) -> bool {
    !matches!(v.map(|v| v.trim().to_ascii_lowercase()).as_deref(), Some("0" | "false" | "off" | "no"))
}

impl Config {
    /// Register a trusted Scout origin served by the embedding process.
    pub fn add_scout_origin(&mut self, origin: &str) {
        for origin in crate::resync::parse_origins(origin) {
            if !self.scout_origins.contains(&origin) {
                self.scout_origins.push(origin);
            }
        }
    }

    pub fn from_env() -> Config {
        Self::from_lookup(|key| env::var(key).ok())
    }

    /// Build from an explicit lookup so an in-process host never has to rewrite global environment variables.
    pub fn from_lookup(get: impl Fn(&str) -> Option<String>) -> Config {
        let env_opt = |key: &str| match get(key) {
            Some(v) if !v.trim().is_empty() => Some(v.trim().to_string()),
            _ => None,
        };
        let cache_dir = env_opt("CACHE_DIR")
            .map(PathBuf::from)
            .unwrap_or_else(|| env::temp_dir().join("den-subtitles-cache"));
        Config {
            port: env_opt("PORT").and_then(|v| v.parse().ok()).unwrap_or(8093),
            cache_dir,
            cache_max_bytes: env_opt("CACHE_MAX_BYTES")
                .and_then(|v| v.parse().ok())
                // The disk budget. Memory is clamped to `cache::MAX_MEMORY_BYTES` so the 512 MiB
                // cgroup keeps room for three Tier-1 children (or one audio Tier-2 job and its
                // ffmpeg beside one Tier-1), the service and its buffers.
                .unwrap_or(256 * 1024 * 1024),
            public_base_url: env_opt("PUBLIC_BASE_URL"),
            alass: env_opt("ALASS_PATH").unwrap_or_else(|| "alass".to_string()),
            ffmpeg: env_opt("FFMPEG_PATH").unwrap_or_else(|| "ffmpeg".to_string()),
            config_key: env_opt("CONFIG_KEY").unwrap_or_default(),
            config_keys_prev: env_opt("CONFIG_KEYS_PREV").unwrap_or_default(),
            revocation: crate::userconfig::Revocation::from_env(
                &env_opt("REVOKED_INSTALLS").unwrap_or_default(),
                env_opt("CONFIG_EPOCH").as_deref(),
            )
            .requiring_install_id(env_opt("REQUIRE_INSTALL_ID").is_some_and(|v| v != "0")),
            metrics_token: env_opt("METRICS_TOKEN").unwrap_or_default(),
            log_requests: env_opt("LOG_REQUESTS").is_some_and(|v| v != "0"),
            // Default ON, unlike `LOG_REQUESTS` above — unset must keep diagnosing titles, not stop
            // doing it, so only an explicit off-token turns it off.
            log_identity: log_identity_on(env_opt("LOG_IDENTITY").as_deref()),
            os_api_base: crate::opensubtitles::API.to_string(),
            scout_origins: crate::resync::parse_origins(&env_opt("SCOUT_ORIGINS").unwrap_or_default()),
            scout_aliases: crate::resync::parse_aliases(&env_opt("SCOUT_ALIASES").unwrap_or_default()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn log_identity_is_off_for_the_shared_off_tokens_only() {
        assert!(log_identity_on(None));
        assert!(log_identity_on(Some("")));
        assert!(log_identity_on(Some("1")));
        for v in ["0", "false", "off", "no", "FALSE", " Off "] {
            assert!(!log_identity_on(Some(v)), "{v:?} should turn identity logging off");
        }
    }

    #[tokio::test]
    async fn startup_line_names_the_port_and_switches() {
        let state = crate::AppState::new(Config::from_env());
        let line = crate::startup_line(&state);
        assert!(line.starts_with("den-subtitles "), "{line}");
        assert!(line.contains(&format!("listening on :{}", state.cfg.port)), "{line}");
        assert!(line.contains("log_identity="), "{line}");
    }

    #[test]
    fn env_opt_trims() {
        std::env::set_var("DEN_SUBTITLES_TEST_ENV_OPT_TRIM", "  value \n");
        assert_eq!(env_opt("DEN_SUBTITLES_TEST_ENV_OPT_TRIM").as_deref(), Some("value"));
        std::env::set_var("DEN_SUBTITLES_TEST_ENV_OPT_TRIM", "   ");
        assert_eq!(env_opt("DEN_SUBTITLES_TEST_ENV_OPT_TRIM"), None);
    }

    #[test]
    fn config_can_be_built_from_an_explicit_lookup() {
        let cfg = Config::from_lookup(|key| match key {
            "PORT" => Some("9103".into()),
            "PUBLIC_BASE_URL" => Some(" https://node.example/subs ".into()),
            "LOG_REQUESTS" => Some("1".into()),
            _ => None,
        });
        assert_eq!(cfg.port, 9103);
        assert_eq!(cfg.public_base_url.as_deref(), Some("https://node.example/subs"));
        assert!(cfg.log_requests);
    }
}
