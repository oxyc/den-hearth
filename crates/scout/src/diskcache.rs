//! Port of `internal/scout/diskcache.go`. Work package 6.

use std::fmt::Display;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Mutex, MutexGuard, Once, PoisonError};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine;
use sha2::{Digest, Sha256};

use crate::cache::{Cache, CacheClass, MemoryCache};

/// Go `tempFileGrace`: a temp file older than this is a write that died mid-way, and the sweep removes it.
pub const TEMP_FILE_GRACE: Duration = Duration::from_secs(60 * 60);

/// Go `defaultDiskBudget`. `SweepEvery` enforces this ceiling; a write burst past it triggers a sweep early.
/// Sized well above legitimate use (a household's installs write ~25 KiB per entry and land in the low tens
/// of MB) and far below filling a homelab volume.
const DEFAULT_DISK_BUDGET: i64 = 256 << 20;

/// Go `TieredCache`: a memory tier in front of a disk tier under `dir`. Only durable entries reach disk.
///
/// The container is redeployed often, and everything the memory cache held is gone with it. Memory is the hot
/// tier (bounded, LRU), disk is the durable one. `Put` writes through; a memory miss reads back lazily and
/// repopulates.
///
/// Disk is BEST-EFFORT throughout. An unwritable directory disables persistence and says so once; memory keeps
/// serving, because a cache that fails closed on a permissions problem would take the service with it.
pub struct TieredCache {
    mem: MemoryCache,
    dir: PathBuf,
    /// Go `once`: the first disk failure is logged, not every one after it.
    once: Once,
    state: Mutex<DiskState>,
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
}

#[derive(Default)]
struct DiskState {
    /// Go `off`: persistence is disabled for the process lifetime.
    off: bool,
    /// Go `written`: bytes written since the last sweep.
    written: i64,
}

impl TieredCache {
    /// Go `NewTieredCache`: `max_bytes` is the memory tier's budget, `dir` the disk tier's directory. An empty
    /// `dir` turns persistence off.
    pub fn new(max_bytes: usize, dir: &Path) -> TieredCache {
        let mut off = dir.as_os_str().is_empty();
        if !off {
            if let Err(e) = fs::create_dir_all(dir) {
                eprintln!("cache dir {} not writable ({e}) — persistence disabled, memory still serving", dir.display());
                off = true;
            }
        }
        TieredCache {
            mem: MemoryCache::new(max_bytes),
            dir: dir.to_path_buf(),
            once: Once::new(),
            state: Mutex::new(DiskState { off, written: 0 }),
            max_bytes: DEFAULT_DISK_BUDGET,
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
        self.reset_written();
        sweep_files(&self.dir, self.max_bytes)
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
            eprintln!("swept {n} expired cache entries at startup");
        }
        let mut ticker = tokio::time::interval_at(tokio::time::Instant::now() + every, every);
        // A tick missed during a slow sweep is dropped, as Go's ticker drops it.
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            ticker.tick().await;
            let n = self.sweep();
            if n > 0 {
                eprintln!("swept {n} expired cache entries");
            }
        }
    }

    /// Go `disable`: turns the disk tier off for the process lifetime after a failure, and logs why.
    pub(crate) fn disable(&self, op: &str, err: &dyn Display) {
        self.lock().off = true;
        self.once.call_once(|| eprintln!("cache {op} failed ({err}) — persistence disabled, memory still serving"));
    }

    fn lock(&self) -> MutexGuard<'_, DiskState> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn disabled(&self) -> bool {
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

    fn reset_written(&self) {
        self.lock().written = 0;
    }

    /// Go `path`: hashes the key. Cache keys carry config blobs and ids, which are neither filename-safe nor short.
    fn path(&self, key: &str) -> PathBuf {
        let sum = Sha256::digest(key.as_bytes());
        self.dir.join(format!("{}.ent", URL_SAFE_NO_PAD.encode(sum)))
    }
}

impl Cache for TieredCache {
    fn get(&self, key: &str) -> Option<String> {
        if let Some(v) = self.mem.get(key) {
            return Some(v);
        }
        if self.disabled() {
            return None;
        }
        let raw = fs::read(self.path(key)).ok()?;
        // "<unix-expiry>\n<value>". Wall-clock on disk on purpose: a monotonic deadline means nothing to the
        // process that reads the file after a restart.
        let nl = raw.iter().position(|&b| b == b'\n').filter(|&nl| nl > 0)?;
        let expires: i64 = std::str::from_utf8(&raw[..nl]).ok()?.parse().ok()?;
        let now = unix_secs(SystemTime::now());
        if now >= expires {
            let _ = fs::remove_file(self.path(key));
            return None;
        }
        let value = String::from_utf8(raw[nl + 1..].to_vec()).ok()?;
        self.mem.put(key, &value, Duration::from_secs((expires - now) as u64));
        Some(value)
    }

    fn put(&self, key: &str, value: &str, ttl: Duration) {
        self.put_class(key, value, ttl, CacheClass::Durable);
    }

    fn put_class(&self, key: &str, value: &str, ttl: Duration, class: CacheClass) {
        self.mem.put(key, value, ttl);
        if class == CacheClass::Volatile {
            // A volatile value supersedes every older value for this key, including one written through by
            // an earlier release. Merely leaving the durable file alone makes it reappear as soon as the
            // volatile entry expires or is evicted, and again after every restart. That is particularly bad
            // for state transitions: a Premiumize "no longer held" marker could uncover the old six-hour
            // "held" fact and authorize another read-only resolve as though the account still owned it.
            //
            // Removal is best-effort like the disk tier itself. Do it even after persistence has disabled
            // writes: an existing stale file can still be readable, and removing it is the only operation
            // needed here. os.Remove is also deliberately used rather than an expired tombstone, so ordinary
            // volatile coordination never creates disk entries of its own.
            if !self.dir.as_os_str().is_empty() {
                match fs::remove_file(self.path(key)) {
                    Ok(()) => {}
                    Err(e) if e.kind() == io::ErrorKind::NotFound => {}
                    // Do not uncover the stale file again in this process. As with a failed durable write,
                    // memory keeps serving while persistence becomes visibly unhealthy.
                    Err(e) => self.disable("retire durable entry", &e),
                }
            }
            return;
        }
        if self.disabled() {
            return;
        }
        let expires = unix_secs(SystemTime::now() + ttl);
        let body = format!("{expires}\n{value}");
        if let Err((op, e)) = write_entry(&self.dir, &self.path(key), &body) {
            self.disable(op, &e);
            return;
        }
        // A burst is what the hourly sweep cannot answer on its own, so it brings the sweep forward rather
        // than waiting out the interval. The sweep runs on its own thread so the write that tripped it does
        // not wait for the directory scan.
        if self.note_written(body.len() as i64) {
            let dir = self.dir.clone();
            let max_bytes = self.max_bytes;
            std::thread::spawn(move || {
                let n = sweep_files(&dir, max_bytes);
                if n > 0 {
                    eprintln!("swept {n} cache entries after a write burst");
                }
            });
        }
    }

    fn persistent(&self) -> Option<bool> {
        Some(!self.disabled())
    }
}

/// The directory half of Go `Sweep`, without the cache's state, so a write burst can run it on a thread.
fn sweep_files(dir: &Path, max_bytes: i64) -> i64 {
    if dir.as_os_str().is_empty() {
        return 0;
    }
    let Ok(entries) = fs::read_dir(dir) else { return 0 };
    let now = SystemTime::now();
    let now_unix = unix_secs(now);
    let mut removed = 0;
    // Survivors, so the budget pass below does not re-stat the directory.
    struct Kept {
        path: PathBuf,
        size: i64,
        mod_time: SystemTime,
    }
    let mut live: Vec<Kept> = Vec::new();
    let mut live_bytes: i64 = 0;
    for entry in entries.flatten() {
        if entry.file_type().map_or(true, |t| t.is_dir()) {
            continue;
        }
        let path = entry.path();
        let name = entry.file_name();
        let name = name.to_string_lossy();
        if name.ends_with(".ent") {
            if !entry_expired(&path, now_unix) {
                if let Ok(info) = entry.metadata() {
                    let size = info.len() as i64;
                    live_bytes += size;
                    // An mtime is available on every platform this runs on.
                    live.push(Kept { path, size, mod_time: info.modified().unwrap_or(now) });
                }
                continue;
            }
        } else if name.starts_with(".tmp-") {
            // A temp file outlives its Put only if the process died between create and rename. The grace period
            // keeps the sweep from racing a write that is legitimately in flight.
            let abandoned = entry
                .metadata()
                .ok()
                .and_then(|info| info.modified().ok())
                .is_some_and(|mtime| now.duration_since(mtime).is_ok_and(|age| age >= TEMP_FILE_GRACE));
            if !abandoned {
                continue;
            }
        } else {
            continue; // not ours
        }
        if fs::remove_file(&path).is_ok() {
            removed += 1;
        }
    }

    // Expiry alone is not a ceiling; it is a schedule, and nothing re-reads a stream list, so an entry nobody
    // asks for again occupies the volume until its TTL passes however large the store has grown. Oldest first,
    // which for a write-once store is the closest thing to LRU that costs no bookkeeping.
    if max_bytes <= 0 || live_bytes <= max_bytes {
        return removed;
    }
    live.sort_by_key(|k| k.mod_time);
    for k in &live {
        if live_bytes <= max_bytes {
            break;
        }
        if fs::remove_file(&k.path).is_ok() {
            live_bytes -= k.size;
            removed += 1;
        }
    }
    eprintln!("cache volume over its {max_bytes}-byte budget; evicted down to {live_bytes} bytes");
    removed
}

/// Go `entryExpired`: reads just the expiry line. A file that cannot be read or parsed counts as expired, because
/// Get cannot serve it either way, so keeping it only keeps a file nothing can ever use.
fn entry_expired(path: &Path, now: i64) -> bool {
    // It may already be gone; don't claim a deletion we didn't make.
    let Ok(mut f) = File::open(path) else { return false };
    // The expiry is a unix timestamp on the first line; 32 bytes is more than enough to reach the newline.
    let mut head = [0u8; 32];
    // Go ignores the read error too: a head that cannot be read has no newline and counts as expired.
    let n = f.read(&mut head).unwrap_or(0);
    let Some(nl) = head[..n].iter().position(|&b| b == b'\n').filter(|&nl| nl > 0) else { return true };
    match std::str::from_utf8(&head[..nl]).ok().and_then(|s| s.parse::<i64>().ok()) {
        Some(expires) => now >= expires,
        None => true,
    }
}

/// The temp-file step of Go's `Put`: `body` is written to a `.tmp-` file beside `dest` and renamed over it, so a
/// half-written entry read after a crash is never served. The error names the step that failed, for `disable`.
fn write_entry(dir: &Path, dest: &Path, body: &str) -> Result<(), (&'static str, io::Error)> {
    let tmp = dir.join(temp_name());
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&tmp)
        .map_err(|e| ("create temp", e))?;
    let written = file.write_all(body.as_bytes()).map_err(|e| ("write", e));
    drop(file);
    let result = written.and_then(|()| fs::rename(&tmp, dest).map_err(|e| ("rename", e)));
    // Go's deferred cleanup: a no-op after a successful rename, and the sweep's job if this write failed.
    let _ = fs::remove_file(&tmp);
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

/// Go `writeFile`: writes `body` to `path` with mode 0644. A test seam, kept next to the cache so the
/// unwritable-directory case can be built without importing `os` into the test.
#[cfg(test)]
pub(crate) fn write_file(path: &Path, body: &str) -> io::Result<()> {
    fs::write(path, body)
}
