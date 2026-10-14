//! den-scout hot paths, with allocation counts. Run through `bench/micro.sh` (which generates the fixtures):
//!
//!     cargo bench -p den-scout --features bench
//!
//! The Go counterparts are in `bench/go/hearth_bench_test.go`, on the same fixtures.

use std::path::PathBuf;
use std::time::{Duration, SystemTime};

use den_scout::bench_api;
use den_scout::cache::{Cache, MemoryCache};
use den_scout::config::{decode_config, Config, DebridAccount, DebridService};
use den_scout::diskcache::TieredCache;
use den_scout::play::PlayTarget;
use den_scout::rank::RawStream;
use den_scout::seal::{new_seal_keypair, seal, SealKeyring, SEALED_VERSION};
use den_scout::season::TorrentFile;
use den_scout::ticket::TicketKeys;
use den_scout::util::b64url_encode;
use divan::counter::BytesCount;
use divan::{black_box, Bencher};

// divan's counting profiler by default; an `alloc-*` feature installs that allocator bare (the profiler's own
// bookkeeping would otherwise sit in every timing), so allocation counts are only reported without one.
#[cfg(not(any(feature = "alloc-system", feature = "alloc-mimalloc", feature = "alloc-jemalloc")))]
#[global_allocator]
static ALLOC: divan::AllocProfiler = divan::AllocProfiler::system();
#[cfg(feature = "alloc-system")]
#[global_allocator]
static ALLOC: std::alloc::System = std::alloc::System;
#[cfg(feature = "alloc-mimalloc")]
#[global_allocator]
static ALLOC: mimalloc::MiMalloc = mimalloc::MiMalloc;
#[cfg(feature = "alloc-jemalloc")]
#[global_allocator]
static ALLOC: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

fn main() {
    divan::main();
}

fn fixture(name: &str) -> Vec<u8> {
    let dir = std::env::var_os("BENCH_FIXTURES")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../bench/fixtures"));
    std::fs::read(dir.join(name)).unwrap_or_else(|e| panic!("{name}: {e} (run bench/gen_fixtures.py)"))
}

fn testdata(name: &str) -> Vec<u8> {
    std::fs::read(PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("testdata").join(name)).expect("testdata")
}

fn releases(n: usize) -> Vec<RawStream> {
    let raw: Vec<serde_json::Value> = serde_json::from_slice(&fixture("releases.json")).unwrap();
    raw.iter()
        .take(n)
        .map(|r| RawStream {
            info_hash: r["hash"].as_str().unwrap().to_owned(),
            title: r["title"].as_str().unwrap().to_owned(),
            size_bytes: r["size"].as_i64(),
            seeders: r["seeders"].as_i64(),
            cached: r["cached"].as_bool().unwrap(),
            cache_known: true,
            source: "torrentio".to_owned(),
            ..Default::default()
        })
        .collect()
}

// ---- rank: filter, score and sort one scrape's worth of releases ---------------------------------------------

#[divan::bench(args = [100, 500, 2000])]
fn rank_movie(bencher: Bencher, n: usize) {
    let streams = releases(n);
    assert!(bench_api::rank_movie(&streams).len() > 10, "the filters must leave a list to sort");
    bencher.bench(|| bench_api::rank_movie(black_box(&streams)));
}

#[divan::bench(args = [100, 500, 2000])]
fn rank_episode(bencher: Bencher, n: usize) {
    let streams = releases(n);
    assert!(bench_api::rank_episode(&streams).len() > 10, "the filters must leave a list to sort");
    bencher.bench(|| bench_api::rank_episode(black_box(&streams)));
}

// ---- season: episode matching (Go season_bench_test.go) ------------------------------------------------------

fn pack(n: i64, name: impl Fn(i64) -> String) -> Vec<TorrentFile> {
    (1..=n).map(|i| TorrentFile { index: i, name: name(i), size_bytes: Some(1000 + i) }).collect()
}

fn scene(i: i64) -> String {
    format!("Show.S01E{i:02}.1080p.AMZN.WEB-DL.DDP5.1.H.264-NTb.mkv")
}

fn anime(i: i64) -> String {
    format!("[Grp] Show - {i:02} [1080p][HEVC][10bit].mkv")
}

#[divan::bench]
fn pick_episode_scene24(bencher: Bencher) {
    let files = pack(24, scene);
    bencher.bench(|| bench_api::pick_episode_file(black_box(&files), 1, 13));
}

#[divan::bench]
fn pick_episode_anime24(bencher: Bencher) {
    let files = pack(24, anime);
    bencher.bench(|| bench_api::pick_episode_file(black_box(&files), 1, 13));
}

#[divan::bench]
fn pick_episode_scene200(bencher: Bencher) {
    let files = pack(200, scene);
    bencher.bench(|| bench_api::pick_episode_file(black_box(&files), 1, 199));
}

#[divan::bench]
fn pick_episode_whole_season(bencher: Bencher) {
    let files = pack(24, scene);
    bencher.bench(|| {
        for ep in 1..=24 {
            black_box(bench_api::pick_episode_file(black_box(&files), 1, ep));
        }
    });
}

// ---- scrape and store decoding -------------------------------------------------------------------------------

#[divan::bench(args = ["streams-50.json", "streams-500.json"])]
fn parse_stremio_streams(bencher: Bencher, name: &str) {
    let body = fixture(name);
    assert!(!bench_api::parse_stremio_streams(&body).is_empty());
    bencher.counter(BytesCount::new(body.len())).bench(|| bench_api::parse_stremio_streams(black_box(&body)));
}

#[divan::bench(sample_count = 10)]
fn decode_torbox_listing(bencher: Bencher) {
    let body = fixture("torbox-listing.json");
    assert!(bench_api::decode_listing(&body).1);
    bencher.counter(BytesCount::new(body.len())).bench(|| bench_api::decode_listing(black_box(&body)));
}

// ---- probe: the container parsers, on the files the Go tests use ---------------------------------------------

#[divan::bench(args = ["tracks.mkv", "hi10p.mkv"])]
fn probe_matroska(bencher: Bencher, name: &str) {
    let head = testdata(name);
    bencher.bench(|| bench_api::parse_head(black_box(&head)));
}

#[divan::bench(args = ["sample.mp4", "hevc10.mp4", "multi-audio.mp4", "moov-at-end.mp4"])]
fn probe_mp4(bencher: Bencher, name: &str) {
    let head = testdata(name);
    bencher.bench(|| bench_api::parse_mp4(black_box(&head)));
}

#[divan::bench]
fn probe_avi(bencher: Bencher) {
    let head = testdata("xvid.avi");
    bencher.bench(|| bench_api::parse_avi(black_box(&head)));
}

// ---- config and ticket: per-request crypto and parsing ------------------------------------------------------

fn keyring() -> SealKeyring {
    let mut priv_key = [0u8; 32];
    for (i, b) in priv_key.iter_mut().enumerate() {
        *b = i as u8 + 1;
    }
    SealKeyring { keys: vec![new_seal_keypair(priv_key)] }
}

#[divan::bench(args = ["plain", "sealed"])]
fn decode_config_segment(bencher: Bencher, kind: &str) {
    let plain = fixture("config.json");
    let kr = keyring();
    let segment = if kind == "plain" {
        b64url_encode(&plain)
    } else {
        let mut sealed = vec![SEALED_VERSION];
        sealed.extend(seal(&kr.keys[0].pub_key, &plain).unwrap());
        b64url_encode(&sealed)
    };
    let keys = (kind == "sealed").then_some(&kr);
    assert!(decode_config(keys, &segment).is_some(), "fixture config rejected");
    bencher.bench(|| decode_config(black_box(keys), black_box(&segment)));
}

fn ticket_inputs() -> (TicketKeys, Config, PlayTarget) {
    let keys = TicketKeys::new(Some(&keyring())).unwrap();
    let config = Config {
        debrid: vec![
            DebridAccount { service: DebridService::parse("torbox").unwrap(), token: "t".repeat(40) },
            DebridAccount { service: DebridService::parse("realdebrid").unwrap(), token: "R".repeat(52) },
        ],
        iid: "AAECAwQFBgcICQoLDA0ODw".to_owned(),
        epoch: 3,
        ..Default::default()
    };
    let target = PlayTarget {
        info_hash: "ab".repeat(20),
        file_idx: Some(3),
        season: Some(1),
        episode: Some(5),
        release_size: 4 << 30,
        title: "Show.S01E05.1080p.WEB-DL.DDP5.1.H.264-NTb".to_owned(),
        imdb: "tt0944947".to_owned(),
    };
    (keys, config, target)
}

#[divan::bench]
fn ticket_mint(bencher: Bencher) {
    let (keys, config, target) = ticket_inputs();
    let exp = SystemTime::now() + Duration::from_secs(24 * 3600);
    bencher.bench(|| keys.mint(black_box(&config), black_box(&target), exp));
}

#[divan::bench]
fn ticket_open(bencher: Bencher) {
    let (keys, config, target) = ticket_inputs();
    let ticket = keys.mint(&config, &target, SystemTime::now() + Duration::from_secs(24 * 3600));
    let now = SystemTime::now();
    bencher.bench(|| keys.open(black_box(&ticket), now).unwrap());
}

// ---- caches --------------------------------------------------------------------------------------------------

fn keys(n: usize) -> Vec<String> {
    (0..n).map(|i| format!("list:v1:{i:040x}")).collect()
}

#[divan::bench]
fn memory_put_evicting(bencher: Bencher) {
    let cache = MemoryCache::new(1 << 20);
    let keys = keys(1 << 16);
    let value = "v".repeat(1024);
    let mut i = 0usize;
    bencher.bench_local(|| {
        cache.put(&keys[i & (keys.len() - 1)], black_box(&value), Duration::from_secs(3600));
        i += 1;
    });
}

#[divan::bench]
fn memory_get_hit(bencher: Bencher) {
    let cache = MemoryCache::new(4 << 20);
    let keys = keys(512);
    let value = "v".repeat(1024);
    for k in &keys {
        cache.put(k, &value, Duration::from_secs(3600));
    }
    let mut i = 0usize;
    bencher.bench_local(|| {
        let hit = cache.get(black_box(&keys[i & 511]));
        i += 1;
        hit
    });
}

#[divan::bench]
fn memory_get_miss(bencher: Bencher) {
    let cache = MemoryCache::new(4 << 20);
    bencher.bench(|| cache.get(black_box("absent")));
}

#[divan::bench]
fn tiered_put_durable(bencher: Bencher) {
    let dir = tempfile::tempdir().unwrap();
    let cache = bench_api::tiered_cache_without_disk_budget(1 << 20, dir.path());
    let value = "v".repeat(4096);
    let mut i = 0usize;
    bencher.bench_local(|| {
        cache.put(&format!("probe:v1:{:040x}", i & 1023), black_box(&value), Duration::from_secs(3600));
        i += 1;
    });
}

#[divan::bench]
fn tiered_get_disk_hit(bencher: Bencher) {
    let dir = tempfile::tempdir().unwrap();
    let cache = TieredCache::new(1, dir.path()); // too small to hold anything: every get reads the file
    let keys: Vec<String> = (0..256).map(|i| format!("probe:v1:{i:040x}")).collect();
    let value = "v".repeat(4096);
    for k in &keys {
        cache.put(k, &value, Duration::from_secs(3600));
    }
    let mut i = 0usize;
    bencher.bench_local(|| {
        let hit = cache.get(black_box(&keys[i & 255]));
        assert!(hit.is_some());
        i += 1;
    });
}

/// Go `BenchmarkTieredCache_500DiskMisses`: the filesystem-miss cost of hydrating probe facts for a whole scrape.
#[divan::bench]
fn tiered_500_disk_misses(bencher: Bencher) {
    let dir = tempfile::tempdir().unwrap();
    let cache = TieredCache::new(1 << 20, dir.path());
    let keys: Vec<String> = (0..500).map(|i| format!("probe:v1:{i:040x}")).collect();
    bencher.bench(|| {
        for k in &keys {
            black_box(cache.get(black_box(k)));
        }
    });
}
