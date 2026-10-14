//! Port of `internal/scout/cache.go`: the cache seam (stream lists and per-hash truth) and its in-memory
//! TTL + LRU backend, bounded by bytes. `Cache` and `CacheClass` are FROZEN (see PORTING.md).

use std::time::Duration;

/// FROZEN. Go `Cache`. Thread-safe. `get` returns `None` for a missing or expired key.
pub trait Cache: Send + Sync {
    fn get(&self, key: &str) -> Option<String>;
    fn put(&self, key: &str, value: &str, ttl: Duration);

    /// Go's `classifiedCache` optional interface (`PutClass`), folded in as a defaulted method: a cache
    /// that does not understand classes keeps the old write-through behaviour, which is what this
    /// default does. Go's `cachePut(cache, ...)` is `cache.put_class(...)`.
    fn put_class(&self, key: &str, value: &str, ttl: Duration, class: CacheClass) {
        self.put(key, value, ttl)
    }

    /// Go's `persistenceReporter` optional interface (metrics.go): whether entries survive a restart.
    fn persistent(&self) -> bool {
        false
    }
}

/// FROZEN. Go `CacheClass`: facts worth surviving a deploy versus coordination state useful only to this
/// process.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CacheClass {
    Durable,
    Volatile,
}

/// Go `MemoryCache`: in-memory TTL + LRU cache bounded by BYTES (key + value). The internals are the
/// porter's; only the constructor and the `Cache` impl are the surface.
pub struct MemoryCache {
    max_bytes: usize,
}

impl MemoryCache {
    /// Go `NewMemoryCache`.
    pub fn new(max_bytes: usize) -> MemoryCache {
        todo!("cache.go NewMemoryCache")
    }
}

impl Cache for MemoryCache {
    fn get(&self, key: &str) -> Option<String> {
        todo!("cache.go MemoryCache.Get")
    }

    fn put(&self, key: &str, value: &str, ttl: Duration) {
        todo!("cache.go MemoryCache.Put")
    }
}
