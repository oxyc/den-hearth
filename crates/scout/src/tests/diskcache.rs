//! Go: `internal/scout/diskcache_test.go` (320 lines). Work package 6.

use std::fs::{self, File, FileTimes};
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use tempfile::TempDir;

use crate::cache::{Cache, CacheClass};
use crate::diskcache::{write_file, TieredCache, TEMP_FILE_GRACE};

/// Go `diskBytes`: the summed size of the regular files directly under `dir`.
fn disk_bytes(dir: &Path) -> i64 {
    fs::read_dir(dir)
        .expect("read_dir")
        .filter_map(|e| e.ok())
        .filter_map(|e| {
            let m = e.metadata().ok()?;
            m.is_file().then_some(m.len() as i64)
        })
        .sum()
}

fn file_count(dir: &Path) -> usize {
    fs::read_dir(dir).expect("read_dir").count()
}

// The point of the durable tier is a redeploy: the container restarts on every image push, and a probe
// costs a debrid resolve to rebuild. A fresh cache over the same directory must still know what the old
// one learned.
#[test]
fn tiered_cache_survives_restart() {
    let dir = TempDir::new().unwrap();
    let first = TieredCache::new(1 << 20, dir.path());
    first.put("tracks:abc", r#"{"audioLanguages":["swe"]}"#, Duration::from_secs(3600));

    let second = TieredCache::new(1 << 20, dir.path()); // a "restart": nothing in memory
    let got = second.get("tracks:abc");
    assert_eq!(
        got.as_deref(),
        Some(r#"{"audioLanguages":["swe"]}"#),
        "after restart got {got:?}, want the stored value"
    );
}

#[test]
fn tiered_cache_volatile_entries_never_reach_disk() {
    let dir = TempDir::new().unwrap();
    let first = TieredCache::new(1 << 20, dir.path());
    for i in 0..500 {
        first.put_class(&format!("status:{i}"), "pending", Duration::from_secs(60), CacheClass::Volatile);
    }
    assert_eq!(first.get("status:499").as_deref(), Some("pending"), "volatile entry missing from memory");
    assert_eq!(file_count(dir.path()), 0, "volatile burst wrote disk files, want zero");
    assert!(
        TieredCache::new(1 << 20, dir.path()).get("status:499").is_none(),
        "volatile coordination state survived a restart"
    );
}

// Changing a key from durable to volatile is a state transition, not a second independent cache tier.
// Once the new value expires, is evicted, or the process restarts, the superseded durable value must not
// come back from disk.
#[test]
fn tiered_cache_volatile_overwrite_retires_durable_value() {
    for name in ["expiry", "memory eviction", "restart"] {
        let dir = TempDir::new().unwrap();
        let c = TieredCache::new(64, dir.path());
        c.put("transition", "old-durable", Duration::from_secs(3600));
        c.put_class("transition", "new-volatile", Duration::from_millis(1), CacheClass::Volatile);
        let c = match name {
            "expiry" => {
                std::thread::sleep(Duration::from_millis(10));
                c
            }
            "memory eviction" => {
                for i in 0..20 {
                    c.put_class(&format!("filler{i}"), &"x".repeat(30), Duration::from_secs(3600), CacheClass::Volatile);
                }
                c
            }
            _ => TieredCache::new(64, dir.path()),
        };
        let got = c.get("transition");
        assert!(got.is_none(), "{name}: superseded durable value resurfaced: {got:?}");
    }
}

// hydrateProbeFacts may ask about every raw indexer result before ranking. Keep the real filesystem-miss
// cost visible so a negative memo is added only if those local misses become material next to the network
// scrape, rather than growing another cache from intuition alone.
#[test]
#[ignore = "benchmark: Go BenchmarkTieredCache_500DiskMisses; run with --ignored"]
fn tiered_cache_500_disk_misses() {
    let dir = TempDir::new().unwrap();
    let c = TieredCache::new(1 << 20, dir.path());
    let keys: Vec<String> = (0..500).map(|i| format!("probe:v1:{i:040x}")).collect();
    let start = std::time::Instant::now();
    for _ in 0..100 {
        for key in &keys {
            let _ = c.get(key);
        }
    }
    eprintln!("500 disk misses x100: {:?}", start.elapsed());
}

// Expiry is wall-clock on disk: a monotonic deadline means nothing to the process that reads the file.
#[test]
fn tiered_cache_expired_on_disk_is_not_served() {
    let dir = TempDir::new().unwrap();
    TieredCache::new(1 << 20, dir.path()).put("k", "v", Duration::ZERO); // already expired
    std::thread::sleep(Duration::from_millis(5));
    let got = TieredCache::new(1 << 20, dir.path()).get("k");
    assert!(got.is_none(), "served expired entry {got:?}");
}

// An unwritable directory must not take the service down with it — memory keeps serving.
#[test]
fn tiered_cache_unwritable_dir_still_serves_from_memory() {
    // A path under a FILE can never be created.
    let base = TempDir::new().unwrap();
    let file = base.path().join("not-a-dir");
    write_file(&file, "x").unwrap();
    let c = TieredCache::new(1 << 20, &file.join("cache"));
    c.put("k", "v", Duration::from_secs(3600));
    assert_eq!(c.get("k").as_deref(), Some("v"), "memory tier stopped serving");
}

// Expiry was enforced only on READ, so a key nobody asks for again was written once and kept forever —
// a stream list for a title watched once, a one-minute refusal, a fifteen-second torrent-miss. On a
// homelab box that is unbounded growth with no ceiling and nothing reporting it.
#[test]
fn tiered_cache_sweep_reclaims_what_nobody_reads_back() {
    let dir = TempDir::new().unwrap();
    // A tiny memory tier, so nothing is served from memory and the disk state is what's under test.
    let c = TieredCache::new(1, dir.path());
    for i in 0..20 {
        c.put(&format!("dead-{i}"), "v", Duration::from_millis(1));
    }
    c.put("alive", "v", Duration::from_secs(3600));
    std::thread::sleep(Duration::from_millis(10));

    assert_eq!(c.sweep(), 20, "swept entries, want the 20 expired ones");
    assert_eq!(file_count(dir.path()), 1, "files left on disk, want only the unexpired one");
    // And the survivor is still readable — a sweep that took the live entry would be worse than none.
    assert_eq!(
        TieredCache::new(1 << 20, dir.path()).get("alive").as_deref(),
        Some("v"),
        "the sweep removed a live entry"
    );
}

// A temp file only outlives its Put if the process died mid-write. Old ones are abandoned and swept;
// fresh ones may belong to a write still in flight, so they are left alone.
#[test]
fn tiered_cache_sweep_clears_abandoned_temp_files_only() {
    let dir = TempDir::new().unwrap();
    let c = TieredCache::new(1 << 20, dir.path());

    let stale = dir.path().join(".tmp-crashed");
    write_file(&stale, "half").unwrap();
    let old = SystemTime::now() - TEMP_FILE_GRACE * 2;
    File::options()
        .write(true)
        .open(&stale)
        .unwrap()
        .set_times(FileTimes::new().set_accessed(old).set_modified(old))
        .unwrap();
    let fresh = dir.path().join(".tmp-inflight");
    write_file(&fresh, "writing").unwrap();

    c.sweep();
    assert!(!stale.exists(), "an abandoned temp file survived the sweep");
    assert!(fresh.exists(), "the sweep raced a write that was still in flight");
}

// Keys carry config blobs and ids; they are neither filename-safe nor short.
#[test]
fn tiered_cache_hashes_awkward_keys() {
    let dir = TempDir::new().unwrap();
    let c = TieredCache::new(1 << 20, dir.path());
    let key = format!("list:{}/../../etc/passwd?x=1", "\0".repeat(300));
    c.put(&key, "v", Duration::from_secs(3600));
    assert_eq!(
        TieredCache::new(1 << 20, dir.path()).get(&key).as_deref(),
        Some("v"),
        "awkward key round-trip failed"
    );
}

// SweepEvery sweeps once at startup — a redeploy is the likeliest moment for a backlog of entries that
// expired while the process was not running — then on its ticker, and stops when the context ends.
#[tokio::test]
async fn tiered_cache_sweep_every_runs_and_stops() {
    let dir = TempDir::new().unwrap();
    let c = Arc::new(TieredCache::new(1, dir.path()));
    c.put("dead", "v", Duration::from_millis(1));
    tokio::time::sleep(Duration::from_millis(5)).await;

    let task = tokio::spawn({
        let c = Arc::clone(&c);
        async move { c.sweep_every(Duration::from_millis(1)).await }
    });

    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    while file_count(dir.path()) != 0 {
        assert!(tokio::time::Instant::now() < deadline, "SweepEvery never swept the expired entry");
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
    // Cancellation is dropping the future: aborting the task drops it.
    task.abort();
    let joined = tokio::time::timeout(Duration::from_secs(2), task)
        .await
        .expect("SweepEvery ignored its cancelled context");
    assert!(joined.is_err_and(|e| e.is_cancelled()), "SweepEvery was not cancelled");

    // A cache with persistence off has nothing to sweep and must return rather than tick forever.
    let off = TieredCache::new(1 << 20, Path::new(""));
    tokio::time::timeout(Duration::from_secs(1), off.sweep_every(Duration::from_secs(3600)))
        .await
        .expect("SweepEvery span a ticker for a cache with no disk tier");
}

// One failed WRITE must not stop reclamation. `disable` is set for the process lifetime, and it also
// short-circuits Get before the on-read expiry removal — so gating Sweep on it too meant a single ENOSPC
// or permissions blip turned off BOTH ways an entry is ever reclaimed, which is precisely the unbounded
// growth the sweeper exists to prevent. The directory may still be readable when it is not writable, and
// reaping is exactly what helps if the problem was space.
#[test]
fn tiered_cache_sweep_survives_a_write_failure() {
    let dir = TempDir::new().unwrap();
    let c = TieredCache::new(1, dir.path());
    for i in 0..5 {
        c.put(&format!("dead-{i}"), "v", Duration::from_millis(1));
    }
    std::thread::sleep(Duration::from_millis(10));

    c.disable("write", &"no space left on device");
    assert_eq!(
        c.sweep(),
        5,
        "swept after a write failure, want 5 — the store can no longer be reclaimed at all"
    );
}

// The volume has a ceiling, not just a schedule.
//
// Expiry is enforced on read and by an hourly sweep, and nothing re-reads a stream list — so an entry
// nobody asks for again sits on the volume until its TTL passes, however large the store has grown.
// Measured before this: 429 KiB per entry with a max-size config, one build per second past the indexer
// limiter, about 1.5 GB an hour. That mattered only once this tier had a writable volume to use.
#[test]
fn tiered_cache_sweep_enforces_a_byte_budget() {
    let dir = TempDir::new().unwrap();
    let mut c = TieredCache::new(1 << 20, dir.path());
    // Budget off while writing, so the burst-triggered sweep inside Put does not do the trimming this
    // test is here to observe. Turned on for the explicit Sweep below.
    c.max_bytes = 0;

    // Well past the budget, all long-lived so expiry reclaims nothing.
    let body = "x".repeat(8 << 10);
    for i in 0..40 {
        c.put(&format!("k{i}"), &body, Duration::from_secs(3600));
        // Distinct mtimes, so "oldest first" is a defined order rather than a coin toss.
        std::thread::sleep(Duration::from_millis(1));
    }
    let budget: i64 = 64 << 10;
    let written = disk_bytes(dir.path());
    assert!(
        written > budget,
        "the fixture only wrote {written} bytes — it cannot exercise a {budget} budget"
    );
    c.max_bytes = budget;

    c.sweep();

    let after = disk_bytes(dir.path());
    assert!(
        after <= c.max_bytes,
        "after the sweep the volume holds {after} bytes, over the {} budget",
        c.max_bytes
    );
    assert!(after != 0, "the sweep emptied the store rather than trimming it to the budget");
    // The newest entry is the one worth keeping, and it survived.
    assert!(c.get("k39").is_some(), "the most recent entry was evicted before older ones");
}

// An unexpired store under the budget is left completely alone.
#[test]
fn tiered_cache_sweep_keeps_a_store_under_budget() {
    let dir = TempDir::new().unwrap();
    let c = TieredCache::new(1 << 20, dir.path());
    for i in 0..5 {
        c.put(&format!("k{i}"), "small", Duration::from_secs(3600));
    }
    let before = disk_bytes(dir.path());
    assert_eq!(c.sweep(), 0, "swept entries from a store that is under budget and unexpired");
    assert_eq!(disk_bytes(dir.path()), before, "a store under budget lost bytes to the sweep");
}
