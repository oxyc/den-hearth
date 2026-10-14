//! Port of `internal/scout/cache.go`: the cache seam (stream lists and per-hash truth) and its in-memory
//! TTL + LRU backend, bounded by bytes. `Cache` and `CacheClass` are FROZEN (see PORTING.md).

use std::collections::{BTreeMap, HashMap};
use std::sync::{Mutex, MutexGuard, PoisonError};
use std::time::{Duration, SystemTime};

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

/// Go `MemoryCache`: in-memory TTL + LRU cache bounded by BYTES (key + value). The internals are the
/// porter's; only the constructor and the `Cache` impl are the surface.
pub struct MemoryCache {
    max_bytes: usize,
    /// Go's `c.now`: the clock expiry reads. A test swaps it for one it can move.
    pub(crate) now: Box<dyn Fn() -> SystemTime + Send + Sync>,
    state: Mutex<CacheState>,
}

/// The entries, the recency order and the byte count, behind the one lock.
#[derive(Default)]
struct CacheState {
    /// Go `items`: key -> entry.
    items: HashMap<String, CacheEntry>,
    /// Go `ll`, as a map from a recency tick to the key holding it. The smallest tick is the least recently
    /// used, so Go's `ll.Back()` is `first_key_value`.
    order: BTreeMap<u64, String>,
    next_tick: u64,
    /// Go `bytes`: the summed key + value sizes of every entry.
    bytes: usize,
}

struct CacheEntry {
    value: String,
    expires: SystemTime,
    /// Go `size`: the key + value bytes this entry counts into `bytes`.
    size: usize,
    /// This entry's position in `order`.
    tick: u64,
}

impl CacheState {
    fn new_tick(&mut self) -> u64 {
        let tick = self.next_tick;
        self.next_tick += 1;
        tick
    }

    /// Go `c.ll.MoveToFront(el)`: marks `key` as the most recently used.
    fn touch(&mut self, key: &str) {
        let tick = self.new_tick();
        if let Some(entry) = self.items.get_mut(key) {
            self.order.remove(&entry.tick);
            entry.tick = tick;
            self.order.insert(tick, key.to_owned());
        }
    }

    /// Go `remove`.
    fn remove(&mut self, key: &str) {
        if let Some(entry) = self.items.remove(key) {
            self.order.remove(&entry.tick);
            self.bytes -= entry.size;
        }
    }
}

impl MemoryCache {
    /// Go `NewMemoryCache`.
    pub fn new(max_bytes: usize) -> MemoryCache {
        MemoryCache {
            max_bytes,
            now: Box::new(SystemTime::now),
            state: Mutex::new(CacheState::default()),
        }
    }

    /// A poisoned lock still holds consistent state (every edit finishes before a panic can), so it is taken.
    fn lock(&self) -> MutexGuard<'_, CacheState> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

impl Cache for MemoryCache {
    fn get(&self, key: &str) -> Option<String> {
        let now = (self.now)();
        let mut guard = self.lock();
        let st = &mut *guard;
        let entry = st.items.get(key)?;
        if now > entry.expires {
            st.remove(key);
            return None;
        }
        let value = entry.value.clone();
        st.touch(key); // LRU touch
        Some(value)
    }

    fn put(&self, key: &str, value: &str, ttl: Duration) {
        let expires = (self.now)() + ttl;
        let size = key.len() + value.len();
        let mut guard = self.lock();
        let st = &mut *guard;
        if let Some(entry) = st.items.get_mut(key) {
            st.bytes = st.bytes + size - entry.size;
            entry.value = value.to_owned();
            entry.size = size;
            entry.expires = expires;
            st.touch(key);
        } else {
            let tick = st.new_tick();
            st.order.insert(tick, key.to_owned());
            st.items.insert(key.to_owned(), CacheEntry { value: value.to_owned(), expires, size, tick });
            st.bytes += size;
        }
        while st.bytes > self.max_bytes {
            let Some((_, oldest)) = st.order.pop_first() else { break };
            if let Some(entry) = st.items.remove(&oldest) {
                st.bytes -= entry.size;
            }
        }
    }
}
