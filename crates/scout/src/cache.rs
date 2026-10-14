//! Port of `internal/scout/cache.go`: the cache seam (stream lists and per-hash truth) and its in-memory
//! TTL backend, bounded by bytes. `Cache` and `CacheClass` are FROZEN (see PORTING.md).
//!
//! The default backend is an in-memory TTL cache bounded by BYTES (audit #1: a count cap let a large
//! resultCap × many titles blow the heap and OOM the container). Thread-safe, and sharded: a hit on one shard
//! never waits for a write on another.

use std::sync::Arc;
use std::time::{Duration, SystemTime};

use quick_cache::sync::Cache as Sharded;
use quick_cache::{DefaultHashBuilder, OptionsBuilder, Weighter};

/// FROZEN. Go `Cache`. Thread-safe. `get` returns `None` for a missing or expired key.
pub trait Cache: Send + Sync {
    /// The value is shared with the cache, not copied out of it: a hit costs a reference count, and a caller that
    /// needs an owned `String` makes that copy itself.
    fn get(&self, key: &str) -> Option<Arc<str>>;
    fn put(&self, key: &str, value: &str, ttl: Duration);

    /// Go's `classifiedCache` optional interface (`PutClass`), folded in as a defaulted method: a cache
    /// that does not understand classes keeps the old write-through behaviour, which is what this
    /// default does. Go's `cachePut(cache, ...)` is `cache.put_class(...)`.
    fn put_class(&self, key: &str, value: &str, ttl: Duration, class: CacheClass) {
        self.put(key, value, ttl)
    }

    /// Go's `persistenceReporter` optional interface (metrics.go): whether entries survive a restart.
    /// `None` is a backend that does not report it, which the persistence gauge shows as -1.
    fn persistent(&self) -> Option<bool> {
        None
    }
}

/// FROZEN. Go `CacheClass`: facts worth surviving a deploy versus coordination state useful only to this
/// process.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CacheClass {
    Durable,
    Volatile,
}

/// Go `MemoryCache`: in-memory TTL cache bounded by BYTES (key + value). The internals are the porter's; only
/// the constructor and the `Cache` impl are the surface.
///
/// A byte-weighted `quick_cache`: sharded locks, so concurrent hits do not queue behind one mutex (measured at
/// 8 threads: a hit 10.5 µs -> 0.24 µs, a put 13 µs -> 0.9 µs; `bench/results/scout-cache/`), and CLOCK-Pro
/// eviction, which keeps an entry that is read again over one written once. Go's strict LRU is gone with the
/// recency list it needed a global lock for.
pub struct MemoryCache {
    /// Go's `c.now`: the clock expiry reads. A test swaps it for one it can move.
    pub(crate) now: Box<dyn Fn() -> SystemTime + Send + Sync>,
    entries: Sharded<String, CacheEntry, ByteWeight>,
}

#[derive(Clone)]
struct CacheEntry {
    value: Arc<str>,
    expires: SystemTime,
}

/// Go `size`: the key + value bytes an entry counts against the budget.
#[derive(Clone)]
struct ByteWeight;

impl Weighter<String, CacheEntry> for ByteWeight {
    fn weight(&self, key: &String, entry: &CacheEntry) -> u64 {
        (key.len() + entry.value.len()) as u64
    }
}

/// The smallest budget a shard gets. An entry heavier than its shard's budget is not kept (a maximum-size stream
/// list is ~430 KiB), so shards are only added while each stays well above that.
const MIN_SHARD_BYTES: usize = 2 << 20;
const MAX_SHARDS: usize = 16;
/// What the entries are expected to average, for sizing the table and the ghost list.
const TYPICAL_ENTRY_BYTES: usize = 2048;

impl MemoryCache {
    /// Go `NewMemoryCache`.
    pub fn new(max_bytes: usize) -> MemoryCache {
        let options = OptionsBuilder::new()
            .shards((max_bytes / MIN_SHARD_BYTES).clamp(1, MAX_SHARDS))
            .estimated_items_capacity(max_bytes / TYPICAL_ENTRY_BYTES + 1)
            .weight_capacity(max_bytes as u64)
            .build()
            .expect("capacity and estimate are both set");
        let entries = Sharded::with_options(
            options,
            ByteWeight,
            DefaultHashBuilder::default(),
            quick_cache::sync::DefaultLifecycle::default(),
        );
        MemoryCache { now: Box::new(SystemTime::now), entries }
    }
}

impl Cache for MemoryCache {
    fn get(&self, key: &str) -> Option<Arc<str>> {
        let entry = self.entries.get(key)?;
        if (self.now)() > entry.expires {
            self.entries.remove(key);
            return None;
        }
        Some(entry.value)
    }

    fn put(&self, key: &str, value: &str, ttl: Duration) {
        self.put_shared(key, Arc::from(value), ttl);
    }
}

impl MemoryCache {
    /// `put` for a value the caller already holds shared, so it is stored without another copy.
    pub(crate) fn put_shared(&self, key: &str, value: Arc<str>, ttl: Duration) {
        let now = (self.now)();
        // Go's `Add` does not panic on an absurd TTL; neither may this. A TTL past what the clock can hold
        // never expires in any lifetime of this process.
        let expires =
            now.checked_add(ttl).unwrap_or_else(|| now + Duration::from_secs(100 * 365 * 24 * 60 * 60));
        self.entries.insert(key.to_owned(), CacheEntry { value, expires });
    }
}
