//! Go: `internal/scout/season_bench_test.go` (61 lines).
//!
//! Benchmarks. Each Go `Benchmark*` is a plain `#[test]` marked `#[ignore = "benchmark"]` that runs the
//! same loop; it gates nothing. Run with `cargo test -- --ignored benchmark`.

use crate::season::{pick_episode_file, TorrentFile};

/// Go `b.N`: how many times each benchmark body runs here.
const ITERATIONS: usize = 1_000;

/// Go `benchPack`. `shape` is the Go `fmt.Sprintf` format, here a closure over the file number.
fn bench_pack(n: i64, shape: impl Fn(i64) -> String) -> Vec<TorrentFile> {
    let mut out = Vec::new();
    for i in 1..=n {
        let sz = 1000 + i;
        out.push(TorrentFile { index: i, name: shape(i), size_bytes: Some(sz) });
    }
    out
}

// Episode selection is the hottest pure-CPU path in the service: it runs once per file, per pick, and
// every matching bug fixed here has added another pattern. Nothing measured it, so the cost of compiling
// those patterns per FILE rather than per PICK went unnoticed until it was 10x — see episodeNamer.

// The shape a real season pack has: every file dot-separated with audio/codec tags.
fn scene(i: i64) -> String {
    format!("Show.S01E{i:02}.1080p.AMZN.WEB-DL.DDP5.1.H.264-NTb.mkv")
}

fn anime(i: i64) -> String {
    format!("[Grp] Show - {i:02} [1080p][HEVC][10bit].mkv")
}

#[test]
#[ignore = "benchmark"]
fn pick_episode_scene24() {
    let files = bench_pack(24, scene);
    for _ in 0..ITERATIONS {
        let _ = pick_episode_file(&files, 1, 13);
    }
}

#[test]
#[ignore = "benchmark"]
fn pick_episode_anime24() {
    let files = bench_pack(24, anime);
    for _ in 0..ITERATIONS {
        let _ = pick_episode_file(&files, 1, 13);
    }
}

#[test]
#[ignore = "benchmark"]
fn pick_episode_scene200() {
    let files = bench_pack(200, scene);
    for _ in 0..ITERATIONS {
        let _ = pick_episode_file(&files, 1, 199);
    }
}

// A whole season resolved one episode at a time — what a binge actually costs.
#[test]
#[ignore = "benchmark"]
fn pick_episode_whole_season() {
    let files = bench_pack(24, scene);
    for _ in 0..ITERATIONS {
        for ep in 1..=24 {
            let _ = pick_episode_file(&files, 1, ep);
        }
    }
}
