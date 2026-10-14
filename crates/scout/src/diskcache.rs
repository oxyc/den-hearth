//! Port of `internal/scout/diskcache.go`.
//!
//! The disk tier is for facts worth keeping across a restart, and it is written sparingly (SSD wear). The tier itself
//! is `den-cache`, shared with Subtitles; this is Scout's seat on it: the hashed file names, the ten-minute persistence
//! floor, and the hourly sweep.

#[cfg(test)]
use std::fmt::Display;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

pub use den_cache::{DiskCounts, PERSIST_MIN_TTL, TEMP_FILE_GRACE};
use den_cache::{Naming, Options};

use crate::cache::{Cache, CacheClass};
use crate::loglimit::emit;

/// How often expired entries are swept. The sweep reads the directory listing and nothing else once the index knows
/// the files, so hourly costs one directory scan.
pub const SWEEP_INTERVAL: Duration = Duration::from_secs(60 * 60);

/// Go `TieredCache`: a memory tier in front of a disk tier under `dir`. Only durable entries reach disk.
///
/// The container is redeployed often, and everything the memory cache held is gone with it. Memory is the hot
/// tier (bounded), disk is the durable one. A memory miss reads back lazily and repopulates.
///
/// The disk tier is written as little as possible, because the volume is an SSD:
///
/// - Only a durable entry that lives at least [`PERSIST_MIN_TTL`] is written. Shorter-lived entries (and
///   tombstones, which are puts of an empty value that expires at once) stay in memory and retire any older file.
/// - A durable put whose value the file already holds, with at least three quarters of its lifetime left, writes
///   nothing.
/// - Nothing is written to learn what is on disk: the directory is scanned once at open, and a get for a key the
///   index does not know never touches the filesystem.
///
/// Disk is BEST-EFFORT throughout. An unwritable directory disables persistence and says so once; memory keeps
/// serving, because a cache that fails closed on a permissions problem would take the service with it.
pub struct TieredCache {
    inner: den_cache::Cache<Arc<str>>,
}

impl TieredCache {
    /// Go `NewTieredCache`: `max_bytes` is the memory tier's budget, `dir` the disk tier's directory. An empty
    /// `dir` turns persistence off.
    ///
    /// Opening walks the directory once: that builds the index and reclaims what expired while the process was down
    /// (a redeploy is the likeliest moment for a backlog), so `sweep_every` does not repeat it at startup.
    pub fn new(max_bytes: usize, dir: &Path) -> TieredCache {
        TieredCache::open(max_bytes, dir, None)
    }

    /// `new` reading time from `clock` in both tiers from the first walk on, so a replay can run days of traffic in
    /// seconds.
    #[cfg(feature = "bench")]
    pub(crate) fn new_with_clock(
        max_bytes: usize,
        dir: &Path,
        clock: Arc<dyn Fn() -> std::time::SystemTime + Send + Sync>,
    ) -> TieredCache {
        TieredCache::open(max_bytes, dir, Some(clock))
    }

    fn open(max_bytes: usize, dir: &Path, clock: Option<den_cache::Clock>) -> TieredCache {
        // Files are `<expiry>\n<value>` from earlier releases and `<expiry> <len>\n<value>` from this one; both are read.
        let options = Options {
            dir: Some(dir.to_path_buf()),
            naming: Naming::Hashed,
            min_persist_ttl: PERSIST_MIN_TTL,
            accept_unframed: true,
            log: emit,
            clock,
            ..Options::new(max_bytes)
        };
        TieredCache { inner: den_cache::Cache::new(options) }
    }

    /// What the disk tier has asked of the filesystem since it was built.
    pub fn disk_counts(&self) -> DiskCounts {
        self.inner.disk_counts()
    }

    /// Go `Sweep`: removes expired entries, abandoned temp files and, past the budget, the oldest entries.
    /// Returns how many entries it removed.
    ///
    /// Deliberately NOT gated on persistence being on. A failed write turns it off, and it also short-circuits Get
    /// before the on-read expiry removal, so a single ENOSPC or permissions blip used to turn off BOTH ways an entry
    /// is ever reclaimed. The directory may still be readable when it is not writable, and reaping is exactly what
    /// helps if the problem was space.
    pub fn sweep(&self) -> i64 {
        self.inner.sweep()
    }

    /// Go `SweepEvery`: sweeps on `every` until dropped (the startup sweep is the walk `new` does). Returns at once
    /// when persistence is off. Cancellation is dropping the future.
    pub async fn sweep_every(&self, every: Duration) {
        if !self.inner.has_disk() {
            return;
        }
        let mut ticker = tokio::time::interval_at(tokio::time::Instant::now() + every, every);
        // A tick missed during a slow sweep is dropped, as Go's ticker drops it.
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            ticker.tick().await;
            let n = self.sweep();
            if n > 0 {
                emit(&format!("swept {n} expired cache entries"));
            }
        }
    }

    /// Go `disable`: turns the disk tier off after a failure, and logs why.
    #[cfg(test)]
    pub(crate) fn disable(&self, op: &str, err: &dyn Display) {
        self.inner.disable_disk(op, err);
    }

    #[cfg(test)]
    pub(crate) fn disabled(&self) -> bool {
        !self.inner.disk_enabled()
    }

    /// The disk budget the sweep trims to. 0 disables the byte budget.
    #[cfg(test)]
    pub(crate) fn max_bytes(&self) -> i64 {
        self.inner.disk_bytes()
    }

    #[cfg(any(test, feature = "bench"))]
    pub(crate) fn set_max_bytes(&self, bytes: i64) {
        self.inner.set_disk_bytes(bytes);
    }

    /// Go `path`: hashes the key. Cache keys carry config blobs and ids, which are neither filename-safe nor short.
    #[cfg(test)]
    pub(crate) fn path(&self, key: &str) -> std::path::PathBuf {
        self.inner.path_for(key).expect("a disk tier")
    }
}

impl Cache for TieredCache {
    fn get(&self, key: &str) -> Option<Arc<str>> {
        self.inner.get(key)
    }

    fn put(&self, key: &str, value: &str, ttl: Duration) {
        self.put_class(key, value, ttl, CacheClass::Durable);
    }

    fn put_class(&self, key: &str, value: &str, ttl: Duration, class: CacheClass) {
        self.inner.put(key, Arc::from(value), ttl, class);
    }

    fn persistent(&self) -> Option<bool> {
        Some(self.inner.disk_enabled())
    }
}

/// Go `writeFile`: writes `body` to `path` with mode 0644. A test seam, kept next to the cache so the
/// unwritable-directory case can be built without importing `os` into the test.
#[cfg(test)]
pub(crate) fn write_file(path: &Path, body: &str) -> std::io::Result<()> {
    std::fs::write(path, body)
}
