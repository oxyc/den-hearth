//! Entry points for `benches/scout.rs` to the `pub(crate)` functions the hot paths live in. Compiled only with
//! `--features bench`; nothing here is reachable from a normal build.
//!
//! These are one-line forwards, so a benchmark measures the production function and not a copy of it.

use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;

use crate::cache::MemoryCache;
use crate::config::{DebridService, Indexer};
use crate::deps::Deps;
use crate::diskcache::TieredCache;
use crate::handler::{new_handler, Handler};
use crate::probe::Probe;
use crate::rank::{self, RankFilters, RawStream};
use crate::scrape::{DoError, ScrapeQuery, Scraper};
use crate::season::{self, TorrentFile};
use crate::stores::{ProviderStatus, ResolveTarget, Store, StoreError};

/// A movie request: the filters a configured install sets, a release year and the title's tokens.
pub fn rank_movie(streams: &[RawStream]) -> Vec<RawStream> {
    rank::rank_streams(
        streams,
        RankFilters {
            exclude_cam: true,
            result_cap: 100,
            expected_year: Some(2024),
            min_seeders: Some(3),
            resolutions: vec!["2160p".to_owned(), "1080p".to_owned()],
            expected_title_tokens: rank::title_tokens("Dune Part Two"),
            ..Default::default()
        },
    )
}

/// A series request for S01E05: a preferred resolution, the show's tokens and the episode.
pub fn rank_episode(streams: &[RawStream]) -> Vec<RawStream> {
    rank::rank_streams(
        streams,
        RankFilters {
            exclude_cam: true,
            result_cap: 100,
            prefer_resolution: "1080p".to_owned(),
            episode: Some([1, 5]),
            show_tokens: rank::title_tokens("Pokemon"),
            ..Default::default()
        },
    )
}

pub fn parse_stremio_streams(body: &[u8]) -> Vec<RawStream> {
    crate::scrape::parse_stremio_streams(body, "torrentio")
}

/// The number of torrents the listing named, so the decode cannot be optimised away.
pub fn decode_listing(body: &[u8]) -> (usize, bool) {
    let (ids, ok, _) = crate::stores::torbox::decode_listing(body);
    (ids.len(), ok)
}

pub fn parse_head(head: &[u8]) -> Option<Probe> {
    crate::probe::parse_head(head).ok()
}

pub fn parse_mp4(head: &[u8]) -> Probe {
    crate::probe_mp4::parse_mp4(head).0
}

pub fn parse_avi(head: &[u8]) -> Probe {
    crate::probe_avi::parse_avi(head).0
}

pub fn pick_episode_file(files: &[TorrentFile], season: i64, episode: i64) -> Option<i64> {
    season::pick_episode_file(files, season, episode).ok().flatten()
}

/// The cache a replay drives: production settings, time read from `clock` so days of traffic run in seconds.
/// The replay reaches the cache's housekeeping only through the functions below, so the same replay file
/// measures the cache before and after its policy changes.
///
/// A write-behind variant (hold a changed entry for N minutes, write it once) was measured through this file and
/// removed; see `bench/results/scout-cache/replay-24h.jsonl` (labels `policy+wb*`).
pub fn replay_cache(
    memory_bytes: usize,
    dir: &Path,
    clock: std::sync::Arc<dyn Fn() -> std::time::SystemTime + Send + Sync>,
) -> TieredCache {
    TieredCache::new_with_clock(memory_bytes, dir, clock)
}

/// What the process does once an hour (hearth's `sweep_every`). Returns entries removed.
pub fn replay_hourly(cache: &TieredCache) -> i64 {
    cache.sweep()
}

/// A tiered cache with the disk budget off, so a long write benchmark measures the write and not the sweep a
/// burst past the budget starts (the Go benchmark does the same).
pub fn tiered_cache_without_disk_budget(memory_bytes: usize, dir: &Path) -> TieredCache {
    let cache = TieredCache::new(memory_bytes, dir);
    cache.set_max_bytes(0);
    cache
}

/// An indexer that answers the same releases every time.
struct FixedScraper(Vec<RawStream>);

#[async_trait]
impl Scraper for FixedScraper {
    fn id(&self) -> Indexer {
        Indexer("torrentio".to_owned())
    }

    async fn scrape(&self, _q: &ScrapeQuery) -> Result<Vec<RawStream>, DoError> {
        Ok(self.0.clone())
    }
}

/// A TorBox account that holds everything and resolves every release to one link.
struct HeldStore;

#[async_trait]
impl Store for HeldStore {
    fn service(&self) -> DebridService {
        DebridService::TorBox
    }

    async fn cache_check(&self, hashes: &[String]) -> Result<HashMap<String, bool>, StoreError> {
        Ok(hashes.iter().map(|h| (h.clone(), true)).collect())
    }

    async fn resolve(&self, _t: &ResolveTarget) -> Result<String, StoreError> {
        Ok("https://cdn.example/file.mkv".to_owned())
    }

    async fn provider_status(&self, _t: &ResolveTarget) -> ProviderStatus {
        ProviderStatus::NotFetching
    }
}

/// The handler's hot paths, warmed: a stream list already in the list cache, its ETag, and a play ticket whose
/// link is already in the link memo. Built with sealing on, as production runs.
pub struct HandlerBench {
    handler: Handler,
    pub list: String,
    pub etag: String,
    pub ticket: String,
}

/// Builds and warms a `HandlerBench` over `streams`. Call inside a runtime.
pub async fn handler_bench(streams: Vec<RawStream>) -> HandlerBench {
    let mut private = [0u8; 32];
    for (i, b) in private.iter_mut().enumerate() {
        *b = i as u8 + 1;
    }
    let keyring = crate::seal::SealKeyring { keys: vec![den_seal::KeyPair::new(private)] };
    let plain = br#"{"debrid":[{"service":"torbox","token":"tb-bench"}],"indexers":["torrentio"]}"#;
    let mut sealed = vec![crate::seal::SEALED_VERSION];
    sealed.extend(crate::seal::seal(&keyring.keys[0].pub_key, plain).expect("seal"));
    let blob = crate::util::b64url_encode(&sealed);
    let streams = Arc::new(streams);
    let deps = Deps {
        runtime: crate::runtime::ScoutRuntime::new(),
        cache: Arc::new(MemoryCache::new(64 << 20)),
        probe_client: None,
        probe_doer: None,
        meta: None,
        scrape_timeout: Duration::from_secs(8),
        list_ttl: Duration::from_secs(3600),
        public_url: "https://scout.example".to_owned(),
        make_scrapers: Arc::new(move |_| vec![Box::new(FixedScraper(streams.to_vec())) as Box<dyn Scraper>]),
        make_stores: Arc::new(|_| vec![Box::new(HeldStore) as Box<dyn Store>]),
        managed_hls: None,
        seal_keyring: Some(Arc::new(keyring)),
        config_key_invalid: false,
        metrics_token: String::new(),
        log_requests: false,
        revocation: Default::default(),
        remux_key: String::new(),
        play_ticket_ttl: crate::ticket::DEFAULT_PLAY_TICKET_TTL,
        legacy_play_until: None,
        config_slice: None,
        mount: "",
    };
    let handler = new_handler(deps);
    let list = format!("/{blob}/stream/movie/tt0111161.json");
    let resp = request(&handler, &list, None).await;
    assert_eq!(resp.status(), 200, "the list did not build");
    let etag = resp.headers().get("etag").and_then(|v| v.to_str().ok()).unwrap_or("").to_owned();
    let body = http_body_util::BodyExt::collect(resp.into_body()).await.expect("a whole body").to_bytes();
    let body: serde_json::Value = serde_json::from_slice(&body).expect("a JSON list");
    let url = body["streams"][0]["url"].as_str().expect("a play URL").to_owned();
    let ticket = url.trim_start_matches("https://scout.example").to_owned();
    assert_eq!(request(&handler, &ticket, None).await.status(), 302, "the ticket did not play");
    HandlerBench { handler, list, etag, ticket }
}

/// One GET through the handler.
async fn request(h: &Handler, path: &str, if_none_match: Option<&str>) -> crate::handler::Resp {
    let mut req = http::Request::get(path);
    if let Some(tag) = if_none_match {
        req = req.header("if-none-match", tag);
    }
    h.serve(req.body(bytes::Bytes::new()).expect("a valid request")).await
}

/// A GET of `path` through the warmed handler, revalidating with `if_none_match`; the status it answered.
pub async fn handler_get(b: &HandlerBench, path: &str, if_none_match: Option<&str>) -> u16 {
    request(&b.handler, path, if_none_match).await.status().as_u16()
}
