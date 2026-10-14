//! The cache Scout and Subtitles share: a byte-weighted memory tier in front of an optional disk tier.
//!
//! Memory is the hot tier. It is a sharded `quick_cache` bounded by BYTES (key + value), so a hit on one shard never
//! waits for a write on another and a handful of large values cannot blow the heap. Disk is the durable tier, for facts
//! worth keeping across a redeploy, and it is written sparingly because the volume is an SSD (see [`disk`]).
//!
//! The value type is the caller's: anything that is cheap to clone and can show its text. Scout stores `Arc<str>`;
//! Subtitles stores a body that memoizes its ETag and cue statistics next to the text, so a hot body is hashed and
//! parsed once however many requests it serves. Because the cache hands back the stored value, those memos live as long
//! as the entry does and cost the cache nothing.

mod disk;
mod name;
#[cfg(test)]
mod tests;

use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use quick_cache::sync::Cache as Sharded;
use quick_cache::{DefaultHashBuilder, OptionsBuilder, Weighter};

use disk::{until, Disk};
pub use disk::{DiskCounts, TEMP_FILE_GRACE};
pub use name::Naming;

/// An entry that lives less than this is not a fact worth a restart: a five-minute indexer answer, a one-minute partial
/// list, a ten-minute miss, a tombstone. It is served from memory and never written, and any older file for the key is
/// retired. Nothing that matters about a restart is lost: the next request rebuilds it.
pub const PERSIST_MIN_TTL: Duration = Duration::from_secs(10 * 60);

/// A cached value. Cloning must be cheap (a reference count): the cache hands out clones and keeps one.
pub trait Value: Clone + Send + Sync + 'static {
    /// Builds the value from text read off disk or handed over by a caller that holds a `&str`.
    fn from_str(text: &str) -> Self;

    /// Builds the value from text the caller owns, so a value that stores a `String` takes it without a copy.
    fn from_string(text: String) -> Self {
        Self::from_str(&text)
    }

    fn as_str(&self) -> &str;
}

impl Value for Arc<str> {
    fn from_str(text: &str) -> Self {
        Arc::from(text)
    }

    fn from_string(text: String) -> Self {
        Arc::from(text)
    }

    fn as_str(&self) -> &str {
        self
    }
}

/// Facts worth surviving a deploy versus coordination state useful only to this process.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Class {
    /// Written to disk, if it lives long enough ([`Options::min_persist_ttl`]).
    Durable,
    /// Memory only, and it supersedes every older value for the key: a durable file for the key is retired, so it
    /// cannot come back when the volatile entry expires or is evicted, or after a restart.
    Volatile,
}

/// How a cache is built. Start from [`Options::new`] and set what differs.
pub struct Options {
    /// The memory tier's budget in bytes (key + value).
    pub mem_bytes: usize,
    /// The disk tier's directory. `None` (or an empty path) is a memory-only cache.
    pub dir: Option<PathBuf>,
    /// The disk budget a sweep trims to; 0 is unbounded. A write burst past it starts a sweep early.
    pub disk_bytes: i64,
    pub naming: Naming,
    /// A durable entry shorter-lived than this is not written (see [`PERSIST_MIN_TTL`]).
    pub min_persist_ttl: Duration,
    /// Read the older `<expiry>\n<value>` files. They carry no length, so a torn one cannot be told from a whole one;
    /// a cache that never wrote them is better off dropping them.
    pub accept_unframed: bool,
    /// Memory shards are only added while each stays at least this large. An entry heavier than its shard's budget is
    /// not kept in memory, so this is the largest value worth caching, give or take.
    pub min_shard_bytes: usize,
    /// What losing the entry for a key costs; higher is kept longer when the disk is over budget.
    pub cost: fn(&str) -> u8,
    /// Where the tier says what went wrong (an unwritable directory, a failed write, an over-budget volume).
    pub log: fn(&str),
}

impl Options {
    pub fn new(mem_bytes: usize) -> Options {
        Options {
            mem_bytes,
            dir: None,
            disk_bytes: 256 << 20,
            naming: Naming::Hashed,
            min_persist_ttl: PERSIST_MIN_TTL,
            accept_unframed: true,
            min_shard_bytes: 2 << 20,
            cost: |_| 0,
            log: |line| eprintln!("{line}"),
        }
    }
}

/// What the memory tier holds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MemStats {
    pub entries: usize,
    /// Key + value bytes, counted once however many readers hold a clone of the value.
    pub bytes: usize,
}

#[derive(Clone)]
struct Entry<V> {
    value: V,
    expires: SystemTime,
}

/// The key + value bytes an entry counts against the budget.
#[derive(Clone)]
struct ByteWeight;

impl<V: Value> Weighter<String, Entry<V>> for ByteWeight {
    fn weight(&self, key: &String, entry: &Entry<V>) -> u64 {
        (key.len() + entry.value.as_str().len()) as u64
    }
}

/// What the entries are expected to average, for sizing the table and the ghost list.
const TYPICAL_ENTRY_BYTES: usize = 2048;
const MAX_SHARDS: usize = 16;

/// What expiry reads the time from.
pub type Clock = Arc<dyn Fn() -> SystemTime + Send + Sync>;

pub struct Cache<V: Value> {
    /// The clock expiry reads in both tiers. A replay or a test swaps it for one it can move.
    clock: Clock,
    mem: Sharded<String, Entry<V>, ByteWeight>,
    disk: Option<Arc<Disk>>,
}

impl<V: Value> Cache<V> {
    pub fn new(o: Options) -> Cache<V> {
        let shards = (o.mem_bytes / o.min_shard_bytes.max(1)).clamp(1, MAX_SHARDS);
        let options = OptionsBuilder::new()
            .shards(shards)
            .estimated_items_capacity(o.mem_bytes / TYPICAL_ENTRY_BYTES + 1)
            .weight_capacity(o.mem_bytes as u64)
            .build()
            .expect("capacity and estimate are both set");
        let mem = Sharded::with_options(
            options,
            ByteWeight,
            DefaultHashBuilder::default(),
            quick_cache::sync::DefaultLifecycle::default(),
        );
        let disk =
            o.dir.as_deref().filter(|d| !d.as_os_str().is_empty()).map(|d| Arc::new(Disk::open(&o, d)));
        Cache { clock: Arc::new(SystemTime::now), mem, disk }
    }

    /// Reads time from `clock` in both tiers.
    pub fn with_clock(mut self, clock: Clock) -> Cache<V> {
        self.clock = clock;
        self
    }

    pub fn set_clock(&mut self, clock: Clock) {
        self.clock = clock;
    }

    /// Changes the disk budget (0 is unbounded).
    pub fn set_disk_bytes(&self, bytes: i64) {
        if let Some(d) = &self.disk {
            d.set_max_bytes(bytes);
        }
    }

    pub fn disk_bytes(&self) -> i64 {
        self.disk.as_ref().map_or(0, |d| d.max_bytes())
    }

    // ---- reads ------------------------------------------------------------------------------------------------

    /// The value, from memory or else from disk (which repopulates memory). `None` for a missing or expired key.
    pub fn get(&self, key: &str) -> Option<V> {
        match self.mem_entry(key) {
            Some((value, _, _)) => Some(value),
            None => self.read_back(key).map(|(value, _)| value),
        }
    }

    /// How long `key` has left, if it is present. Reads the tiers the way `get` does.
    pub fn ttl(&self, key: &str) -> Option<Duration> {
        match self.mem_entry(key) {
            Some((_, expires, now)) => Some(expires.duration_since(now).unwrap_or_default()),
            None => self.read_back(key).map(|(_, left)| left),
        }
    }

    /// Memory only: never falls through to a disk probe. For a key that was only ever written with
    /// [`Class::Volatile`], where the probe cannot hit.
    pub fn get_mem(&self, key: &str) -> Option<V> {
        self.mem_entry(key).map(|(value, _, _)| value)
    }

    /// The value and the time left on it, read back from the file and put in memory for the next hit.
    fn read_back(&self, key: &str) -> Option<(V, Duration)> {
        let disk = self.disk.as_ref()?;
        let now = (self.clock)();
        let (name, loaded) = disk.read(key, now)?;
        let text = loaded.text()?;
        // The one copy of the file's value: shared with the memory tier and the caller from here on.
        let value = V::from_str(text);
        disk.remember(&name, text);
        let left = until(loaded.expires, now);
        self.insert_mem(key, value.clone(), left, now);
        Some((value, left))
    }

    /// A live memory entry: the value, when it expires and the clock reading that judged it.
    fn mem_entry(&self, key: &str) -> Option<(V, SystemTime, SystemTime)> {
        let entry = self.mem.get(key)?;
        let now = (self.clock)();
        if now > entry.expires {
            // Conditional, under the shard lock: a `put` that landed since the read above has replaced the expired
            // entry, and removing by key alone would delete the fresh one.
            self.mem.remove_if(key, |e| now > e.expires);
            return None;
        }
        Some((entry.value, entry.expires, now))
    }

    // ---- writes -----------------------------------------------------------------------------------------------

    pub fn put(&self, key: &str, value: V, ttl: Duration, class: Class) {
        let now = (self.clock)();
        let Some(disk) = &self.disk else {
            self.insert_mem(key, value, ttl, now);
            return;
        };
        self.insert_mem(key, value.clone(), ttl, now);
        if disk.put(key, value.as_str(), ttl, class, now) {
            // A burst is what the hourly sweep cannot answer on its own, so it brings the sweep forward. It runs on
            // its own thread so the write that tripped it does not wait for the directory scan.
            let disk = Arc::clone(disk);
            std::thread::spawn(move || disk.sweep_after_burst(now));
        }
    }

    fn insert_mem(&self, key: &str, value: V, ttl: Duration, now: SystemTime) {
        // An absurd TTL must not panic: one past what the clock can hold never expires in any lifetime of this process.
        let expires =
            now.checked_add(ttl).unwrap_or_else(|| now + Duration::from_secs(100 * 365 * 24 * 60 * 60));
        self.mem.insert(key.to_owned(), Entry { value, expires });
    }

    /// Forgets a key in both tiers. For an entry proven wrong rather than merely stale: writing it back with a zero
    /// TTL would mean a disk write to say "delete this".
    pub fn remove(&self, key: &str) {
        self.mem.remove(key);
        if let Some(d) = &self.disk {
            d.remove(key);
        }
    }

    /// Forgets a key in memory only: the companion to a volatile put, for a key no file was ever written for.
    pub fn remove_mem(&self, key: &str) {
        self.mem.remove(key);
    }

    // ---- housekeeping -----------------------------------------------------------------------------------------

    /// Drops expired memory entries (they would otherwise hold their bytes against the budget until read again or
    /// pushed out) and sweeps the disk tier. Returns how many files the sweep removed.
    pub fn sweep(&self) -> i64 {
        let now = (self.clock)();
        self.mem.retain(|_, e| e.expires > now);
        self.disk.as_ref().map_or(0, |d| d.sweep(now))
    }

    pub fn mem_stats(&self) -> MemStats {
        MemStats { entries: self.mem.len(), bytes: self.mem.weight() as usize }
    }

    /// What the disk tier has asked of the filesystem since it was built.
    pub fn disk_counts(&self) -> DiskCounts {
        use std::sync::atomic::Ordering::Relaxed;
        let Some(d) = &self.disk else { return DiskCounts::default() };
        let s = &d.stats;
        DiskCounts {
            writes: s.writes.load(Relaxed),
            bytes_written: s.bytes_written.load(Relaxed),
            fsyncs: 0,
            reads: s.reads.load(Relaxed),
            bytes_read: s.bytes_read.load(Relaxed),
            unlinks: s.unlinks.load(Relaxed),
            readdirs: s.readdirs.load(Relaxed),
            stat_calls: s.stat_calls.load(Relaxed),
            skipped: s.skipped.load(Relaxed),
        }
    }

    /// Disk writes that failed since the cache was built.
    pub fn write_failures(&self) -> u64 {
        self.disk.as_ref().map_or(0, |d| d.stats.write_failures.load(std::sync::atomic::Ordering::Relaxed))
    }

    /// Whether the disk tier is configured and currently accepting writes.
    pub fn disk_enabled(&self) -> bool {
        self.disk.as_ref().is_some_and(|d| !d.disabled())
    }

    /// Whether a disk tier was asked for at all.
    pub fn has_disk(&self) -> bool {
        self.disk.is_some()
    }

    /// Turns the disk tier off until the next sweep, as a failed write does.
    pub fn disable_disk(&self, op: &str, err: &dyn std::fmt::Display) {
        if let Some(d) = &self.disk {
            d.disable(op, err);
        }
    }

    /// The file `key` is (or would be) stored in, for tests and diagnostics.
    pub fn path_for(&self, key: &str) -> Option<PathBuf> {
        self.disk.as_ref().map(|d| d.path_for(key))
    }
}
