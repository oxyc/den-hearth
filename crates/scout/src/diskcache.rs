//! Port of `internal/scout/diskcache.go`. Work package 6.

use std::fmt::Display;
use std::path::Path;
use std::time::Duration;

use crate::cache::{Cache, CacheClass};

/// Go `tempFileGrace`: a temp file older than this is a write that died mid-way, and the sweep removes it.
pub const TEMP_FILE_GRACE: Duration = Duration::from_secs(60 * 60);

/// Go `TieredCache`: a memory tier in front of a disk tier under `dir`. Only durable entries reach disk.
pub struct TieredCache {
    /// Go `maxBytes`: the disk budget the sweep trims to. 0 disables the byte budget.
    pub(crate) max_bytes: i64,
}

impl TieredCache {
    /// Go `NewTieredCache`: `max_bytes` is the memory tier's budget, `dir` the disk tier's directory. An empty
    /// `dir` turns persistence off.
    pub fn new(max_bytes: usize, dir: &Path) -> TieredCache {
        todo!("diskcache.go NewTieredCache")
    }

    /// Go `Sweep`: removes expired entries, abandoned temp files and, past the budget, the oldest entries.
    /// Returns how many entries it removed.
    pub fn sweep(&self) -> i64 {
        todo!("diskcache.go TieredCache.Sweep")
    }

    /// Go `SweepEvery`: sweeps once at startup, then on `every`, until dropped. Returns at once when
    /// persistence is off. Cancellation is dropping the future.
    pub async fn sweep_every(&self, every: Duration) {
        todo!("diskcache.go TieredCache.SweepEvery")
    }

    /// Go `disable`: turns the disk tier off for the process lifetime after a failure, and logs why.
    pub(crate) fn disable(&self, op: &str, err: &dyn Display) {
        todo!("diskcache.go TieredCache.disable")
    }
}

impl Cache for TieredCache {
    fn get(&self, key: &str) -> Option<String> {
        todo!("diskcache.go TieredCache.Get")
    }

    fn put(&self, key: &str, value: &str, ttl: Duration) {
        todo!("diskcache.go TieredCache.Put")
    }

    fn put_class(&self, key: &str, value: &str, ttl: Duration, class: CacheClass) {
        todo!("diskcache.go TieredCache.PutClass")
    }

    fn persistent(&self) -> bool {
        todo!("diskcache.go TieredCache.Persistent")
    }
}

/// Go `writeFile`: writes `body` to `path` with mode 0644.
pub(crate) fn write_file(path: &Path, body: &str) -> std::io::Result<()> {
    todo!("diskcache.go writeFile")
}
