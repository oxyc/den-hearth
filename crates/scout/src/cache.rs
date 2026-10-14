//! The cache seam (stream lists and per-hash truth) and its in-memory TTL backend, bounded by bytes.
//!
//! The default backend is an in-memory TTL cache bounded by BYTES (audit #1: a count cap let a large
//! resultCap × many titles blow the heap and OOM the container). Thread-safe, and sharded: a hit on one shard
//! never waits for a write on another. The engine is `den-cache`, shared with Subtitles.

use std::sync::Arc;
use std::time::Duration;
#[cfg(test)]
use std::time::SystemTime;

use den_cache::Options;

/// Go `CacheClass`: facts worth surviving a deploy versus coordination state useful only to this
/// process.
pub use den_cache::Class as CacheClass;

/// Go `Cache`. Thread-safe. `get` returns `None` for a missing or expired key.
pub trait Cache: Send + Sync {
    /// The value is shared with the cache, not copied out of it: a hit costs a reference count, and a caller that
    /// needs an owned `String` makes that copy itself (measured 22 ns against Go's copying 39 ns).
    fn get(&self, key: &str) -> Option<Arc<str>>;
    fn put(&self, key: &str, value: &str, ttl: Duration);

    /// Go's `classifiedCache` optional interface (`PutClass`), folded in as a defaulted method: a cache
    /// that does not understand classes keeps the old write-through behaviour, which is what this
    /// default does. Go's `cachePut(cache, ...)` is `cache.put_class(...)`.
    fn put_class(&self, key: &str, value: &str, ttl: Duration, _class: CacheClass) {
        self.put(key, value, ttl)
    }

    /// Go's `persistenceReporter` optional interface (metrics.go): whether entries survive a restart.
    /// `None` is a backend that does not report it, which the persistence gauge shows as -1.
    fn persistent(&self) -> Option<bool> {
        None
    }
}

/// Go `MemoryCache`: in-memory TTL cache bounded by BYTES (key + value). Only the constructor and the `Cache` impl
/// are the surface.
///
/// A byte-weighted `quick_cache`: sharded locks, so concurrent hits do not queue behind one mutex (measured at
/// 8 threads: a hit 10.5 µs -> 0.24 µs, a put 13 µs -> 0.9 µs; `bench/results/scout-cache/`), and CLOCK-Pro
/// eviction, which keeps an entry that is read again over one written once. Go's strict LRU is gone with the
/// recency list it needed a global lock for.
pub struct MemoryCache {
    inner: den_cache::Cache<Arc<str>>,
}

impl MemoryCache {
    /// Go `NewMemoryCache`.
    pub fn new(max_bytes: usize) -> MemoryCache {
        MemoryCache { inner: den_cache::Cache::new(Options::new(max_bytes)) }
    }

    /// Go's `c.now`: the clock expiry reads. A test swaps it for one it can move.
    #[cfg(test)]
    pub(crate) fn set_clock(&mut self, now: Box<dyn Fn() -> SystemTime + Send + Sync>) {
        self.inner.set_clock(Arc::from(now));
    }
}

impl Cache for MemoryCache {
    fn get(&self, key: &str) -> Option<Arc<str>> {
        self.inner.get(key)
    }

    fn put(&self, key: &str, value: &str, ttl: Duration) {
        self.inner.put(key, Arc::from(value), ttl, CacheClass::Durable);
    }
}
