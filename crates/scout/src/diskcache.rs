//! Port of `internal/scout/diskcache.go`.
//!
//! The disk tier is for facts worth keeping across a restart, and it is written sparingly (SSD wear): see
//! `TieredCache` for what is and is not written.

use std::collections::HashMap;
use std::fmt::Display;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, Once, PoisonError};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine;
use sha2::{Digest, Sha256};

use crate::cache::{Cache, CacheClass, MemoryCache};
use crate::loglimit::emit;

/// How often expired entries are swept, and how long a leftover temp file may linger before it counts as
/// abandoned. The sweep reads the directory listing and nothing else once the index knows the files, so hourly
/// costs one directory scan.
pub const SWEEP_INTERVAL: Duration = Duration::from_secs(60 * 60);

/// Go `tempFileGrace`: a temp file older than this is a write that died mid-way, and the sweep removes it.
pub const TEMP_FILE_GRACE: Duration = Duration::from_secs(60 * 60);

/// Go `defaultDiskBudget`. `SweepEvery` enforces this ceiling; a write burst past it triggers a sweep early.
/// Sized well above legitimate use (a household's installs write ~25 KiB per entry and land in the low tens
/// of MB) and far below filling a homelab volume.
const DEFAULT_DISK_BUDGET: i64 = 256 << 20;

/// An entry that lives less than this is not a fact worth a restart: a five-minute indexer answer, a one-minute
/// partial list, a ten-minute miss, a tombstone. It is served from memory and never written, and any older file
/// for the key is retired. Nothing that matters about a restart is lost: the next request rebuilds it.
pub const PERSIST_MIN_TTL: Duration = Duration::from_secs(10 * 60);

/// What the disk tier asked of the filesystem, counted at the call sites: the numbers behind "how much does
/// this cache write". `fsyncs` is always 0 (a write is create, write, rename; nothing is synced).
#[derive(Default)]
struct DiskStats {
    writes: AtomicU64,
    bytes_written: AtomicU64,
    fsyncs: AtomicU64,
    reads: AtomicU64,
    bytes_read: AtomicU64,
    unlinks: AtomicU64,
    readdirs: AtomicU64,
    stat_calls: AtomicU64,
    skipped: AtomicU64,
}

/// A snapshot of [`TieredCache::disk_counts`].
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct DiskCounts {
    /// Entry files created (each one a temp file plus a rename).
    pub writes: u64,
    pub bytes_written: u64,
    pub fsyncs: u64,
    /// `open` + `read` attempts, including the 32-byte expiry peek at a file the index did not know and reads
    /// that found nothing.
    pub reads: u64,
    pub bytes_read: u64,
    /// `unlink` attempts, including ones that found no file.
    pub unlinks: u64,
    /// Directory scans.
    pub readdirs: u64,
    /// `stat` calls on entry files the index did not know.
    pub stat_calls: u64,
    /// Durable puts not written because the file already holds the value with most of its life left.
    pub skipped: u64,
}

impl DiskStats {
    fn read(&self, bytes: usize) {
        self.reads.fetch_add(1, Ordering::Relaxed);
        self.bytes_read.fetch_add(bytes as u64, Ordering::Relaxed);
    }

    fn unlink(&self) {
        self.unlinks.fetch_add(1, Ordering::Relaxed);
    }
}

#[derive(Default)]
struct DiskState {
    /// Go `off`: persistence is disabled for the process lifetime.
    off: bool,
    /// Go `written`: bytes written since the last sweep.
    written: i64,
}

/// What the cache knows about one file in its directory, so it never has to open a file to learn it.
#[derive(Clone, Copy)]
struct DiskMeta {
    /// Unix seconds, from the file's first line.
    expires: i64,
    size: u64,
    /// When the file was written: the order a byte-budget eviction goes in.
    written: SystemTime,
    /// A fingerprint (hash, length) of the value, when this process wrote or read it. A file found at startup
    /// has none until it is read once.
    value: Option<(u64, usize)>,
}

/// Every entry file in the directory, by file name. Built from one directory scan when the cache opens and kept
/// current by every write and removal, so the answers to "is it there", "is it unchanged" and "has it expired" cost
/// no filesystem call.
type Index = HashMap<String, DiskMeta>;

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
    mem: MemoryCache,
    dir: PathBuf,
    /// Go `once`: the first disk failure is logged, not every one after it.
    once: Once,
    /// Shared with the thread a write burst starts, which resets `written` when its scan is done.
    state: Arc<Mutex<DiskState>>,
    /// Go `maxBytes`: the disk budget the sweep trims to. 0 disables the byte budget.
    ///
    /// The memory tier has had a byte budget since audit #1; the disk tier had none, and nothing noticed while it
    /// was disabling itself on an unwritable directory. Giving it a writable volume made that live: measured 429
    /// KiB per entry with a max-size config, one build per second past the indexer limiter, about 1.5 GB an hour,
    /// reclaimed only by an hourly sweep of entries that have EXPIRED. Nothing re-reads them, so expiry alone
    /// frees nothing early.
    ///
    /// Filling the volume is worse than it sounds: the first failed write calls disable(), which turns
    /// persistence off for the life of the process, so one flood permanently costs the 30-day probe cache this
    /// tier exists for.
    pub(crate) max_bytes: i64,
    stats: Arc<DiskStats>,
    index: Arc<Mutex<Index>>,
    hasher: quick_cache::DefaultHashBuilder,
    /// The wall clock disk expiry reads; a replay swaps it for one it can move.
    clock: Arc<dyn Fn() -> SystemTime + Send + Sync>,
}

impl TieredCache {
    /// Go `NewTieredCache`: `max_bytes` is the memory tier's budget, `dir` the disk tier's directory. An empty
    /// `dir` turns persistence off.
    pub fn new(max_bytes: usize, dir: &Path) -> TieredCache {
        let mut off = dir.as_os_str().is_empty();
        if !off {
            if let Err(e) = fs::create_dir_all(dir) {
                emit(&format!(
                    "cache dir {} not writable ({e}) — persistence disabled, memory still serving",
                    dir.display()
                ));
                off = true;
            }
        }
        let stats = Arc::new(DiskStats::default());
        // What an earlier run left behind, whatever format it wrote: entries are `<expiry>\n<value>` files, and
        // that is still the format.
        let index = if off { Index::new() } else { scan_dir(dir, &stats) };
        TieredCache {
            mem: MemoryCache::new(max_bytes),
            dir: dir.to_path_buf(),
            once: Once::new(),
            state: Arc::new(Mutex::new(DiskState { off, written: 0 })),
            max_bytes: DEFAULT_DISK_BUDGET,
            stats,
            index: Arc::new(Mutex::new(index)),
            hasher: quick_cache::DefaultHashBuilder::default(),
            clock: Arc::new(SystemTime::now),
        }
    }

    /// Reads time from `clock` in both tiers, so a replay can run days of traffic in seconds.
    #[cfg(feature = "bench")]
    pub(crate) fn with_clock(mut self, clock: Arc<dyn Fn() -> SystemTime + Send + Sync>) -> TieredCache {
        let mem_clock = Arc::clone(&clock);
        self.mem.now = Box::new(move || mem_clock());
        self.clock = clock;
        self
    }

    /// What the disk tier has asked of the filesystem since it was built.
    pub fn disk_counts(&self) -> DiskCounts {
        let s = &self.stats;
        DiskCounts {
            writes: s.writes.load(Ordering::Relaxed),
            bytes_written: s.bytes_written.load(Ordering::Relaxed),
            fsyncs: s.fsyncs.load(Ordering::Relaxed),
            reads: s.reads.load(Ordering::Relaxed),
            bytes_read: s.bytes_read.load(Ordering::Relaxed),
            unlinks: s.unlinks.load(Ordering::Relaxed),
            readdirs: s.readdirs.load(Ordering::Relaxed),
            stat_calls: s.stat_calls.load(Ordering::Relaxed),
            skipped: s.skipped.load(Ordering::Relaxed),
        }
    }

    /// Go `Sweep`: removes expired entries, abandoned temp files and, past the budget, the oldest entries.
    /// Returns how many entries it removed.
    ///
    /// Expiry was enforced only on READ, which means only for a key someone asks for again. Every key nobody
    /// re-reads was written once and kept forever, so on a homelab box that is unbounded growth with no ceiling.
    /// Best-effort like the rest of the disk tier: an unreadable directory, or a file that vanishes underneath
    /// us, is skipped rather than fatal.
    ///
    /// Deliberately NOT gated on `disabled()`. `disable` is set for the process lifetime by one failed write, and
    /// it also short-circuits Get before the on-read expiry removal, so a single ENOSPC or permissions blip used
    /// to turn off BOTH ways an entry is ever reclaimed. The directory may still be readable when it is not
    /// writable, and reaping is exactly what helps if the problem was space.
    pub fn sweep(&self) -> i64 {
        sweep_files(&self.dir, self.max_bytes, (self.clock)(), &self.stats, &self.index, || {
            reset_written(&self.state)
        })
    }

    /// Go `SweepEvery`: sweeps once at startup, then on `every`, until dropped. Returns at once when
    /// persistence is off. Cancellation is dropping the future.
    pub async fn sweep_every(&self, every: Duration) {
        if self.dir.as_os_str().is_empty() {
            return;
        }
        // Once at startup: a redeploy is the most likely moment for the store to be holding a backlog of
        // entries that expired while the process was not running.
        let n = self.sweep();
        if n > 0 {
            emit(&format!("swept {n} expired cache entries at startup"));
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

    /// Go `disable`: turns the disk tier off for the process lifetime after a failure, and logs why.
    pub(crate) fn disable(&self, op: &str, err: &dyn Display) {
        self.lock().off = true;
        self.once.call_once(|| {
            emit(&format!("cache {op} failed ({err}) — persistence disabled, memory still serving"))
        });
    }

    fn lock(&self) -> MutexGuard<'_, DiskState> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn index(&self) -> MutexGuard<'_, Index> {
        self.index.lock().unwrap_or_else(PoisonError::into_inner)
    }

    pub(crate) fn disabled(&self) -> bool {
        self.lock().off
    }

    /// Go `noteWritten`: adds to the since-last-sweep counter and reports whether it has passed the budget.
    ///
    /// Counted, not measured: stat-ing the directory per write would put a directory scan on every list build.
    fn note_written(&self, n: i64) -> bool {
        let mut st = self.lock();
        if self.max_bytes <= 0 {
            return false;
        }
        st.written += n;
        if st.written < self.max_bytes {
            return false;
        }
        st.written = 0; // one sweep per burst, not one per write past the line
        true
    }

    /// Go `path`: hashes the key. Cache keys carry config blobs and ids, which are neither filename-safe nor short.
    #[cfg(test)]
    pub(crate) fn path(&self, key: &str) -> PathBuf {
        self.dir.join(file_name(key))
    }

    /// Removes the file for `name`, if there is one. The index is exact, so a key that was never written costs no
    /// syscall. Removal is best-effort: a failure disables persistence rather than let the stale file come back,
    /// as a failed durable write does.
    fn retire(&self, name: &str) {
        if self.index().remove(name).is_none() {
            return;
        }
        self.stats.unlink();
        match fs::remove_file(self.dir.join(name)) {
            Ok(()) => {}
            Err(e) if e.kind() == io::ErrorKind::NotFound => {}
            Err(e) => self.disable("retire durable entry", &e),
        }
    }

    /// Writes one entry file and records it in the index. A failed write disables persistence.
    fn write_now(&self, name: &str, expires: i64, value: &str) {
        let body = format!("{expires}\n{value}");
        if let Err((op, e)) = write_entry(&self.dir, &self.dir.join(name), &body) {
            self.disable(op, &e);
            return;
        }
        self.stats.writes.fetch_add(1, Ordering::Relaxed);
        self.stats.bytes_written.fetch_add(body.len() as u64, Ordering::Relaxed);
        self.index().insert(
            name.to_owned(),
            DiskMeta {
                expires,
                size: body.len() as u64,
                written: (self.clock)(),
                value: Some(self.fingerprint(value)),
            },
        );
        // A burst is what the hourly sweep cannot answer on its own, so it brings the sweep forward rather
        // than waiting out the interval. The sweep runs on its own thread so the write that tripped it does
        // not wait for the directory scan.
        if self.note_written(body.len() as i64) {
            let dir = self.dir.clone();
            let max_bytes = self.max_bytes;
            let state = Arc::clone(&self.state);
            let stats = Arc::clone(&self.stats);
            let index = Arc::clone(&self.index);
            let now = (self.clock)();
            std::thread::spawn(move || {
                let n = sweep_files(&dir, max_bytes, now, &stats, &index, || reset_written(&state));
                if n > 0 {
                    emit(&format!("swept {n} cache entries after a write burst"));
                }
            });
        }
    }

    /// A cheap fingerprint of a value, to tell "the same value again" without keeping the value. Keyed per
    /// cache, and only ever compared within one.
    fn fingerprint(&self, value: &str) -> (u64, usize) {
        (self.hasher.hash_one(value), value.len())
    }

    /// Whether the file for `name` already holds `value` with most of a `ttl` still ahead of it.
    fn on_disk_is_current(&self, name: &str, value: &str, ttl: Duration, now: i64) -> bool {
        let Some(meta) = self.index().get(name).copied() else { return false };
        meta.value == Some(self.fingerprint(value))
            && u64::try_from(meta.expires - now)
                .is_ok_and(|left| u128::from(left) * 4 >= u128::from(ttl.as_secs()) * 3)
    }
}

impl Cache for TieredCache {
    fn get(&self, key: &str) -> Option<Arc<str>> {
        if let Some(v) = self.mem.get(key) {
            return Some(v);
        }
        if self.disabled() {
            return None;
        }
        let name = file_name(key);
        let now = (self.clock)();
        // The index is the directory: a key it does not know has no file, and a key it knows is expired
        // without opening it.
        let meta = self.index().get(&name).copied()?;
        if unix_secs(now) >= meta.expires {
            self.retire(&name);
            return None;
        }
        let raw = fs::read(self.dir.join(&name));
        self.stats.read(raw.as_ref().map_or(0, Vec::len));
        let raw = match raw {
            Ok(raw) => raw,
            Err(e) => {
                // The file went after the index was told about it (a concurrent retire or sweep, or something
                // outside the process). The sweep only walks files that exist, so nothing else would ever drop
                // this entry, and every get would repeat the failed read. The index holds sizes but no running
                // total, and `written` counts bytes written since the last sweep, so there is nothing else to adjust.
                if e.kind() == io::ErrorKind::NotFound {
                    self.index().remove(&name);
                }
                return None;
            }
        };
        // "<unix-expiry>\n<value>". Wall-clock on disk on purpose: a monotonic deadline means nothing to the
        // process that reads the file after a restart.
        let nl = raw.iter().position(|&b| b == b'\n').filter(|&nl| nl > 0)?;
        let expires: i64 = std::str::from_utf8(&raw[..nl]).ok()?.parse().ok()?;
        if unix_secs(now) >= expires {
            self.retire(&name);
            return None;
        }
        // The one copy of the file's value: shared with the memory tier and the caller from here on.
        let value: Arc<str> = Arc::from(std::str::from_utf8(&raw[nl + 1..]).ok()?);
        let fingerprint = self.fingerprint(&value);
        if let Some(m) = self.index().get_mut(&name) {
            m.value = Some(fingerprint);
        }
        self.mem.put_shared(key, Arc::clone(&value), until(expires, now));
        Some(value)
    }

    fn put(&self, key: &str, value: &str, ttl: Duration) {
        self.put_class(key, value, ttl, CacheClass::Durable);
    }

    fn put_class(&self, key: &str, value: &str, ttl: Duration, class: CacheClass) {
        self.mem.put(key, value, ttl);
        let name = file_name(key);
        // A volatile value supersedes every older value for this key, including one written through by
        // an earlier release. Merely leaving the durable file alone makes it reappear as soon as the
        // volatile entry expires or is evicted, and again after every restart. That is particularly bad
        // for state transitions: a Premiumize "no longer held" marker could uncover the old six-hour
        // "held" fact and authorize another read-only resolve as though the account still owned it.
        //
        // The same holds for a durable put too short-lived to be written. Removal is best-effort like the disk
        // tier itself. Do it even after persistence has disabled writes: an existing stale file can still be
        // readable, and removing it is the only operation needed here. Unlinking is also deliberately used
        // rather than an expired tombstone, so ordinary volatile coordination never creates disk entries.
        if class == CacheClass::Volatile || ttl < PERSIST_MIN_TTL {
            if !self.dir.as_os_str().is_empty() {
                self.retire(&name);
            }
            return;
        }
        if self.disabled() {
            return;
        }
        let now = unix_secs((self.clock)());
        // Skipping the rewrite keeps the file's older expiry: after a restart the entry can lapse up to a quarter
        // of `ttl` early. Refreshing only the expiry would still mean rewriting the file (`<expiry>\n<value>`),
        // and saving that write is the point of the rule. Deliberate.
        if self.on_disk_is_current(&name, value, ttl, now) {
            self.stats.skipped.fetch_add(1, Ordering::Relaxed);
            return;
        }
        self.write_now(&name, unix_secs_after((self.clock)(), ttl), value);
    }

    fn persistent(&self) -> Option<bool> {
        Some(!self.disabled())
    }
}

/// Go `resetWritten`.
fn reset_written(state: &Mutex<DiskState>) {
    state.lock().unwrap_or_else(PoisonError::into_inner).written = 0;
}

/// The entry file name for a cache key.
fn file_name(key: &str) -> String {
    let sum = Sha256::digest(key.as_bytes());
    format!("{}.ent", URL_SAFE_NO_PAD.encode(sum))
}

/// The index of a directory: one scan, one 32-byte read per entry file.
fn scan_dir(dir: &Path, stats: &DiskStats) -> Index {
    let mut index = Index::new();
    stats.readdirs.fetch_add(1, Ordering::Relaxed);
    let Ok(entries) = fs::read_dir(dir) else { return index };
    for entry in entries.flatten() {
        let name = entry.file_name().to_string_lossy().into_owned();
        if name.ends_with(".ent") && entry.file_type().is_ok_and(|t| !t.is_dir()) {
            if let Some(meta) = scan_entry(&entry.path(), stats) {
                index.insert(name, meta);
            }
        }
    }
    index
}

/// Go `entryExpired`'s read, kept: the expiry is the first line, so 32 bytes reach it. A file that cannot be
/// parsed is indexed as already expired, because Get cannot serve it either way, and the sweep then removes it.
/// `None` only for a file that is already gone.
fn scan_entry(path: &Path, stats: &DiskStats) -> Option<DiskMeta> {
    stats.stat_calls.fetch_add(1, Ordering::Relaxed);
    let mut file = File::open(path).ok()?;
    let info = file.metadata().ok()?;
    let mut head = [0u8; 32];
    // Go ignores the read error too: a head that cannot be read has no newline and counts as expired.
    let n = file.read(&mut head).unwrap_or(0);
    stats.read(n);
    let expires = head[..n]
        .iter()
        .position(|&b| b == b'\n')
        .filter(|&nl| nl > 0)
        .and_then(|nl| std::str::from_utf8(&head[..nl]).ok()?.parse::<i64>().ok())
        .unwrap_or(0);
    Some(DiskMeta { expires, size: info.len(), written: info.modified().unwrap_or(UNIX_EPOCH), value: None })
}

/// The directory half of Go `Sweep`, without the cache itself, so a write burst can run it on a thread.
/// `reset_written` is Go's `c.resetWritten()`: it runs once the directory scan is done, so what was written
/// while the scan ran is forgotten with the rest, as in Go.
///
/// One directory listing. A file the index knows is judged from the index; only a file it does not know (one
/// written by something else, or after the last scan) is opened.
fn sweep_files(
    dir: &Path,
    max_bytes: i64,
    now: SystemTime,
    stats: &DiskStats,
    index: &Mutex<Index>,
    reset_written: impl FnOnce(),
) -> i64 {
    if dir.as_os_str().is_empty() {
        return 0;
    }
    stats.readdirs.fetch_add(1, Ordering::Relaxed);
    let Ok(entries) = fs::read_dir(dir) else { return 0 };
    let now_unix = unix_secs(now);
    let lock = || index.lock().unwrap_or_else(PoisonError::into_inner);
    let mut removed = 0;
    // Survivors, so the budget pass below does not look at the directory again.
    let mut live: Vec<(String, DiskMeta)> = Vec::new();
    let mut live_bytes: i64 = 0;
    for entry in entries.flatten() {
        if entry.file_type().map_or(true, |t| t.is_dir()) {
            continue;
        }
        let path = entry.path();
        let name = entry.file_name().to_string_lossy().into_owned();
        if name.ends_with(".ent") {
            let known = lock().get(&name).copied();
            let Some(meta) = known.or_else(|| scan_entry(&path, stats)) else { continue };
            if known.is_none() {
                lock().insert(name.clone(), meta);
            }
            if meta.expires > now_unix {
                live_bytes += meta.size as i64;
                live.push((name, meta));
                continue;
            }
            // Removed under the lock, and only if the entry is still the one judged: a put that rewrote it since
            // the listing was read has a later expiry and must keep its file.
            let mut idx = lock();
            if idx.get(&name).is_some_and(|m| m.expires != meta.expires) {
                continue;
            }
            idx.remove(&name);
            stats.unlink();
            if fs::remove_file(&path).is_ok() {
                removed += 1;
            }
        } else if name.starts_with(".tmp-") {
            // A temp file outlives its Put only if the process died between create and rename. The grace period
            // keeps the sweep from racing a write that is legitimately in flight.
            let abandoned = entry
                .metadata()
                .ok()
                .and_then(|info| info.modified().ok())
                .is_some_and(|mtime| now.duration_since(mtime).is_ok_and(|age| age >= TEMP_FILE_GRACE));
            if abandoned {
                stats.unlink();
                if fs::remove_file(&path).is_ok() {
                    removed += 1;
                }
            }
        }
    }
    reset_written();

    // Expiry alone is not a ceiling; it is a schedule, and nothing re-reads a stream list, so an entry nobody
    // asks for again occupies the volume until its TTL passes however large the store has grown. Oldest first,
    // which for a write-once store is the closest thing to LRU that costs no bookkeeping.
    if max_bytes <= 0 || live_bytes <= max_bytes {
        return removed;
    }
    live.sort_by_key(|(_, m)| m.written);
    for (name, meta) in &live {
        if live_bytes <= max_bytes {
            break;
        }
        lock().remove(name);
        stats.unlink();
        if fs::remove_file(dir.join(name)).is_ok() {
            live_bytes -= meta.size as i64;
            removed += 1;
        }
    }
    emit(&format!("cache volume over its {max_bytes}-byte budget; evicted down to {live_bytes} bytes"));
    removed
}

/// The temp-file step of Go's `Put`: `body` is written to a `.tmp-` file beside `dest` and renamed over it, so a
/// half-written entry read after a crash is never served. The error names the step that failed, for `disable`.
fn write_entry(dir: &Path, dest: &Path, body: &str) -> Result<(), (&'static str, io::Error)> {
    let tmp = dir.join(temp_name());
    let mut file =
        OpenOptions::new().write(true).create_new(true).open(&tmp).map_err(|e| ("create temp", e))?;
    let written = file.write_all(body.as_bytes()).map_err(|e| ("write", e));
    drop(file);
    let result = written.and_then(|()| fs::rename(&tmp, dest).map_err(|e| ("rename", e)));
    if result.is_err() {
        // Go's deferred cleanup, for a write that failed. After a good rename there is nothing to remove.
        let _ = fs::remove_file(&tmp);
    }
    result
}

/// A temp name no other write, in this process or a previous one, can share: process id, wall-clock nanoseconds
/// and a per-process counter.
fn temp_name() -> String {
    static SEQ: AtomicU64 = AtomicU64::new(0);
    let nanos = SystemTime::now().duration_since(UNIX_EPOCH).map_or(0, |d| d.as_nanos());
    format!(".tmp-{}-{}-{}", std::process::id(), nanos, SEQ.fetch_add(1, Ordering::Relaxed))
}

fn unix_secs(t: SystemTime) -> i64 {
    t.duration_since(UNIX_EPOCH).map_or(0, |d| d.as_secs() as i64)
}

/// Go `time.Now().Add(ttl).Unix()`: whole seconds, saturating instead of overflowing on an absurd `ttl`.
fn unix_secs_after(now: SystemTime, ttl: Duration) -> i64 {
    let since_epoch = now.duration_since(UNIX_EPOCH).unwrap_or_default().saturating_add(ttl);
    i64::try_from(since_epoch.as_secs()).unwrap_or(i64::MAX)
}

/// Go `time.Until(time.Unix(expires, 0))`: the time left, to the nanosecond, not rounded to whole seconds. Go's
/// `Duration` saturates at about 292 years, which is also where an out-of-range `expires` lands here, so a
/// timestamp no clock can represent does not panic.
fn until(expires: i64, now: SystemTime) -> Duration {
    const GO_MAX_DURATION: Duration = Duration::from_nanos(i64::MAX as u64);
    let Ok(secs) = u64::try_from(expires) else { return Duration::ZERO };
    let Some(at) = UNIX_EPOCH.checked_add(Duration::from_secs(secs)) else { return GO_MAX_DURATION };
    at.duration_since(now).unwrap_or_default().min(GO_MAX_DURATION)
}

/// Go `writeFile`: writes `body` to `path` with mode 0644. A test seam, kept next to the cache so the
/// unwritable-directory case can be built without importing `os` into the test.
#[cfg(test)]
pub(crate) fn write_file(path: &Path, body: &str) -> io::Result<()> {
    fs::write(path, body)
}
