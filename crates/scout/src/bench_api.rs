//! Entry points for `benches/scout.rs` to the `pub(crate)` functions the hot paths live in. Compiled only with
//! `--features bench`; nothing here is reachable from a normal build.
//!
//! These are one-line forwards, so a benchmark measures the production function and not a copy of it.

use std::path::Path;

use crate::diskcache::TieredCache;
use crate::probe::Probe;
use crate::rank::{self, RankFilters, RawStream};
use crate::season::{self, TorrentFile};

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
    TieredCache::new(memory_bytes, dir).with_clock(clock)
}

/// What the process does once an hour (hearth's `sweep_every`). Returns entries removed.
pub fn replay_hourly(cache: &TieredCache) -> i64 {
    cache.sweep()
}

/// A tiered cache with the disk budget off, so a long write benchmark measures the write and not the sweep a
/// burst past the budget starts (the Go benchmark does the same).
pub fn tiered_cache_without_disk_budget(memory_bytes: usize, dir: &Path) -> TieredCache {
    let mut cache = TieredCache::new(memory_bytes, dir);
    cache.max_bytes = 0;
    cache
}
