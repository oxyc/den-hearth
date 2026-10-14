//! Go: `internal/scout/deps_test.go` (128 lines).

use std::collections::HashMap;
use std::time::Duration;

use crate::config::Indexer;
use crate::deps;
use crate::handler::{DEFAULT_LIST_TTL, DEFAULT_TIMEOUT};

/// Go `cinemetaBase` (cinemeta.go:44, package 5). Copied here until `cinemeta.rs` exports it.
const CINEMETA_BASE: &str = "https://v3-cinemeta.strem.io";

fn indexer(s: &str) -> Indexer {
    Indexer(s.to_owned())
}

#[test]
fn log_identity_is_off_for_the_shared_off_tokens_only() {
    for on in ["", "  ", "1", "true", "anything"] {
        assert!(deps::bool_env_or(on, true), "{on:?} should leave identity logging on");
    }
    for off in ["0", "false", "off", "no", "FALSE", " Off ", "No"] {
        assert!(!deps::bool_env_or(off, true), "{off:?} should turn identity logging off");
    }
}

#[test]
fn settings_from_env() {
    // defaults when unset
    let s = deps::settings_from_env(|_| String::new());
    assert!(
        s.port == "8080"
            && s.scrape_timeout == DEFAULT_TIMEOUT
            && s.list_ttl == DEFAULT_LIST_TTL
            && s.cache_bytes == 48 << 20
            && s.indexer_urls.is_empty(),
        "defaults: {s:?}"
    );

    let env: HashMap<&str, &str> = HashMap::from([
        ("PORT", "9000"),
        ("SCRAPE_TIMEOUT_SECS", "5"),
        ("LIST_TTL_SECS", "60"),
        ("PUBLIC_BASE_URL", "https://scout.example"),
        ("MEDIAFUSION_URL", "https://mf.self/CONFIG/manifest.json"),
        ("MEMORY_CACHE_BYTES", "1048576"),
        ("CACHE_DIR", "/cache"),
        ("CINEMETA_URL", "https://cinemeta.self"),
        ("CONFIG_KEY", "current-key"),
        ("CONFIG_KEYS_PREV", "old-a,old-b"),
        ("METRICS_TOKEN", "metrics-secret"),
        ("MINT_INDEXER_CONFIGS", "1"),
    ]);
    let s = deps::settings_from_env(|k| env.get(k).map(|v| (*v).to_owned()).unwrap_or_default());
    assert!(
        s.port == "9000"
            && s.scrape_timeout == Duration::from_secs(5)
            && s.list_ttl == Duration::from_secs(60)
            && s.public_url == "https://scout.example"
            && s.indexer_urls.get(&indexer("mediafusion")).map(String::as_str)
                == Some("https://mf.self/CONFIG")
            && s.cache_bytes == 1 << 20,
        "from env: {s:?}"
    );
    assert!(
        s.cache_dir == "/cache"
            && s.config_key == "current-key"
            && s.config_keys_prev == "old-a,old-b"
            && s.metrics_token == "metrics-secret"
            && s.cinemeta_url == "https://cinemeta.self"
            && s.mint_indexer_configs,
        "unprefixed names from env: {s:?}"
    );

    // No variable carries the addon's name — the container is the namespace — and the SCOUT_-prefixed
    // spellings they replaced are not read at all: a stale env file must show up as an unset key, not keep
    // working by accident.
    let old: HashMap<&str, &str> = HashMap::from([
        ("SCOUT_PUBLIC_URL", "https://old.example"),
        ("SCOUT_CACHE_DIR", "/old"),
        ("SCOUT_CONFIG_KEY", "old"),
        ("SCOUT_CONFIG_KEYS_PREV", "old"),
        ("SCOUT_METRICS_TOKEN", "old"),
        ("SCOUT_SCRAPE_TIMEOUT_MS", "5000"),
        ("SCRAPE_TIMEOUT_MS", "5000"),
        ("SCOUT_LIST_TTL_SECONDS", "60"),
        ("SCOUT_CACHE_BYTES", "1048576"),
        ("SCOUT_CINEMETA_URL", "https://old.example"),
        ("SCOUT_MINT_INDEXER_CONFIGS", "true"),
        ("SCOUT_TORRENTIO_URL", "https://old.example"),
        ("SCOUT_COMET_URL", "https://old.example"),
        ("SCOUT_MEDIAFUSION_URL", "https://old.example"),
        ("SCOUT_TORZ_URL", "https://old.example"),
    ]);
    let s = deps::settings_from_env(|k| old.get(k).map(|v| (*v).to_owned()).unwrap_or_default());
    assert!(
        s.public_url.is_empty()
            && s.cache_dir != "/old"
            && s.config_key.is_empty()
            && s.config_keys_prev.is_empty()
            && s.metrics_token.is_empty()
            && s.scrape_timeout == DEFAULT_TIMEOUT
            && s.list_ttl == DEFAULT_LIST_TTL
            && s.cache_bytes == 48 << 20
            && s.cinemeta_url == CINEMETA_BASE
            && !s.mint_indexer_configs
            && s.indexer_urls.is_empty(),
        "a retired SCOUT_ name was still read: {s:?}"
    );

    // non-numeric / non-positive fall back to defaults
    let s = deps::settings_from_env(|k| match k {
        "SCRAPE_TIMEOUT_SECS" => "-1".to_owned(),
        "LIST_TTL_SECS" => "abc".to_owned(),
        _ => String::new(),
    });
    assert!(
        s.scrape_timeout == DEFAULT_TIMEOUT && s.list_ttl == DEFAULT_LIST_TTL,
        "bad values should fall back: {s:?}"
    );
}

#[tokio::test]
async fn build_deps() {
    use std::sync::Arc;

    use crate::cache::MemoryCache;
    use crate::config::{Config, DebridAccount, DebridService, Filters};
    use crate::scrape::{ScrapeQuery, UnaskableError};

    let settings = deps::settings_from_env(|_| String::new());
    let built = deps::build_deps(&settings, reqwest::Client::new(), Arc::new(MemoryCache::new(1 << 20)));
    let cfg = Config {
        indexers: vec![indexer("torrentio"), indexer("comet")],
        filters: Filters { exclude_cam: true, ..Filters::default() },
        debrid: vec![DebridAccount { service: DebridService::TorBox, token: "t".to_owned() }],
        ..Config::default()
    };
    // comet needs a per-install config URL and this environment supplies none, so it is never requested —
    // asked bare it can only 403. But it stays in the list as a source that CANNOT answer, rather than
    // disappearing from it: dropping it would take it out of the quorum too, and "an empty result is
    // authoritative only when every indexer answered" would quietly become "…every indexer we still
    // bother asking", which is how one 200-empty becomes a confident "this release does not exist".
    let sc = (built.make_scrapers)(&cfg);
    assert!(
        sc.len() == 2 && sc[0].id() == indexer("torrentio") && sc[1].id() == indexer("comet"),
        "makeScrapers: {} scrapers",
        sc.len()
    );
    let err = sc[1]
        .scrape(&ScrapeQuery::default())
        .await
        .expect_err("an unaskable indexer must fail, so it counts as a source that did not answer");
    // Go asserts `sc[1].(unaskableScraper)`; here the scraper's failure says what it is.
    let Some(un) = err.downcast_ref::<UnaskableError>() else {
        panic!("comet should be unaskable without a config URL, got {err}");
    };
    // NOT transient. The distinction decides whether the operator is told to configure something or
    // told an upstream is down, and it is read one level up to decide whether an empty list is an
    // outage. Inverting it on this, the shipped default, reported every genuinely empty title as an
    // indexer outage: X-Den-Degraded on every response, no-store so negative caching never runs, and
    // three in a row flipping /health to degraded on a service that is perfectly healthy.
    //
    // Only the CONSUMPTION of this flag was covered, never its production, so the inversion was green.
    assert!(
        !un.transient,
        "an indexer with no config URL and no minting is a misconfiguration, not an outage — marking it transient \
         suppresses the operator's log line and reports healthy empty lists as a degraded scrape"
    );
    // Given the URL, it is asked.
    let with_comet_settings = deps::settings_from_env(|k| {
        if k == "COMET_URL" {
            "https://comet.example/CONFIG".to_owned()
        } else {
            String::new()
        }
    });
    let with_comet =
        deps::build_deps(&with_comet_settings, reqwest::Client::new(), Arc::new(MemoryCache::new(1 << 20)));
    let sc = (with_comet.make_scrapers)(&cfg);
    assert!(
        sc.len() == 2 && sc[1].id() == indexer("comet"),
        "makeScrapers with comet URL: {} scrapers",
        sc.len()
    );
    let st = (built.make_stores)(&cfg);
    assert!(st.len() == 1 && st[0].service() == DebridService::TorBox, "makeStores: {} stores", st.len());
}
