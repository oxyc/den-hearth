//! Scout adapter policy for the shared `den-cache` engine.
//!
//! Filesystem format, restart, generation, expiry, and eviction behavior are covered canonically in
//! `den-cache`; this module pins only Scout's class/TTL/budget choices and sweep scheduling.

use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use tempfile::TempDir;

use crate::cache::{Cache, CacheClass};
use crate::diskcache::{write_file, TieredCache};

const DAY: Duration = Duration::from_secs(24 * 60 * 60);

fn file_count(dir: &Path) -> usize {
    std::fs::read_dir(dir).expect("read_dir").count()
}

#[test]
fn scout_persists_only_durable_long_lived_facts() {
    let dir = TempDir::new().unwrap();
    let cache = TieredCache::new(1 << 20, dir.path());

    cache.put("short", "answer", Duration::from_secs(5 * 60));
    cache.put_class("coordination", "pending", DAY, CacheClass::Volatile);
    assert_eq!(cache.get("short").as_deref(), Some("answer"));
    assert_eq!(cache.get("coordination").as_deref(), Some("pending"));
    assert_eq!(file_count(dir.path()), 0, "Scout's transient classes reached disk");

    cache.put("probe", "tracks", DAY);
    assert_eq!(file_count(dir.path()), 1, "Scout's durable fact was not persisted");
}

#[test]
fn scout_accepts_an_independent_disk_budget() {
    let dir = TempDir::new().unwrap();
    let cache = TieredCache::new_with_disk_bytes(1 << 20, 3 << 20, dir.path());
    assert_eq!(cache.max_bytes(), 3 << 20);
}

#[tokio::test]
async fn scout_sweep_schedule_runs_and_cancels() {
    let dir = TempDir::new().unwrap();
    let cache = Arc::new(TieredCache::new(1, dir.path()));
    write_file(&cache.path("expired"), "1\nv").unwrap();

    let task = tokio::spawn({
        let cache = Arc::clone(&cache);
        async move { cache.sweep_every(Duration::from_millis(1)).await }
    });
    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    while file_count(dir.path()) != 0 {
        assert!(tokio::time::Instant::now() < deadline, "Scout's sweep task never ran");
        tokio::time::sleep(Duration::from_millis(1)).await;
    }

    task.abort();
    let joined = tokio::time::timeout(Duration::from_secs(2), task)
        .await
        .expect("Scout's sweep task ignored cancellation");
    assert!(joined.is_err_and(|error| error.is_cancelled()));

    let memory_only = TieredCache::new(1 << 20, Path::new(""));
    tokio::time::timeout(Duration::from_secs(1), memory_only.sweep_every(Duration::from_secs(3600)))
        .await
        .expect("memory-only Scout cache started a sweep timer");
}
