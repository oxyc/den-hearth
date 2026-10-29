//! The disk tier: facts worth keeping across a restart, written as sparingly as it can manage (SSD wear).
//!
//! An entry is a file `<unix-expiry> <value-bytes>\n<value>`. The length is what makes a torn file detectable: without
//! it any prefix carrying a newline parses as a whole entry. The older Scout format `<unix-expiry>\n<value>` is still
//! read (see [`Options::accept_unframed`](crate::Options)), and every write is the framed one.
//!
//! What the tier asks of the filesystem:
//!
//! - Only a durable entry that lives at least `min_persist_ttl` is written. Shorter-lived entries stay in memory and
//!   retire any older file for the key.
//! - A durable put whose value the file already holds, with at least three quarters of its lifetime left, writes
//!   nothing.
//! - Nothing is written to learn what is on disk: the directory is scanned once at open, and a get for a key the index
//!   does not know never touches the filesystem. The hourly sweep lists the directory and opens nothing it can already
//!   describe.
//!
//! Best effort throughout: a failed write disables persistence, says so once, and memory keeps serving. The next sweep
//! tries again, because reaping is what frees the space a full volume was short of.

use std::collections::HashMap;
use std::fmt::Display;
use std::fs::{self, File, OpenOptions};
use std::hash::BuildHasher;
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicI64, AtomicU64, AtomicU8, Ordering};
use std::sync::{Mutex, MutexGuard, Once, PoisonError};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use quick_cache::DefaultHashBuilder;

use crate::name::Naming;
use crate::{Class, Options};

/// How long a leftover temp file may linger before it counts as abandoned.
pub const TEMP_FILE_GRACE: Duration = Duration::from_secs(60 * 60);

/// How far ahead of the sweep's clock a temp file's mtime may read before it is undatable rather than just new.
const FUTURE_MTIME_SLACK: Duration = Duration::from_secs(2);

/// What the disk tier asked of the filesystem, counted at the call sites. `fsyncs` is always 0: a write is create,
/// write, rename, and nothing is synced.
#[derive(Default)]
pub(crate) struct DiskStats {
    pub writes: AtomicU64,
    pub bytes_written: AtomicU64,
    pub reads: AtomicU64,
    pub bytes_read: AtomicU64,
    pub unlinks: AtomicU64,
    pub readdirs: AtomicU64,
    pub stat_calls: AtomicU64,
    pub skipped: AtomicU64,
    pub write_failures: AtomicU64,
}

/// A snapshot of [`Cache::disk_counts`](crate::Cache::disk_counts).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct DiskCounts {
    /// Entry files created (each one a temp file plus a rename).
    pub writes: u64,
    pub bytes_written: u64,
    pub fsyncs: u64,
    /// `open` + `read` attempts, including the head peek at a file the index did not know and reads that found
    /// nothing.
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
struct State {
    /// Reads and writes are off until the next sweep (a retire failed, so a stale file could come back), or for good
    /// when `unusable`.
    off: bool,
    /// Writes alone are off until the next sweep (a write or create failed, e.g. ENOSPC or a read-only volume). The
    /// files already there are still sound, so they are still served.
    write_off: bool,
    /// The directory could not be created: nothing a sweep does will change that.
    unusable: bool,
    /// Bytes written since the last sweep.
    written: i64,
}

/// What the cache knows about one file in its directory, so it never has to open a file to learn it.
#[derive(Clone, Copy)]
struct Meta {
    /// Identity of this particular committed file. It changes on every rename, even when expiry and size do not.
    generation: u64,
    /// Unix seconds, from the file's first line.
    expires: i64,
    size: u64,
    /// When the file was written: the order a byte-budget eviction goes in within a cost rank.
    written: SystemTime,
    /// A fingerprint (hash, length) of the value, when this process wrote or read it. A file found at startup has
    /// none until it is read once.
    value: Option<(u64, usize)>,
    /// What losing the entry costs; higher is kept longer.
    rank: u8,
}

/// Every entry file in the directory, by file name. Built from one scan when the cache opens and kept current by every
/// write and removal, so "is it there", "is it unchanged" and "has it expired" cost no filesystem call.
type Index = HashMap<String, Meta>;

pub(crate) struct Disk {
    dir: PathBuf,
    naming: Naming,
    accept_unframed: bool,
    min_persist_ttl: Duration,
    cost: fn(&str) -> u8,
    log: fn(&str),
    /// The disk budget the sweep trims to. 0 disables it.
    max_bytes: AtomicI64,
    state: Mutex<State>,
    once: Once,
    pub(crate) stats: DiskStats,
    index: Mutex<Index>,
    hasher: DefaultHashBuilder,
    next_generation: AtomicU64,
    /// 0 idle, 1 sweeping, 2 sweeping with one coalesced follow-up requested.
    burst_sweep: AtomicU8,
}

/// A file read back: the bytes, where the value starts in them, and when it expires.
pub(crate) struct Loaded {
    raw: Vec<u8>,
    start: usize,
    pub expires: i64,
    pub generation: u64,
}

impl Loaded {
    pub(crate) fn text(&self) -> Option<&str> {
        std::str::from_utf8(&self.raw[self.start..]).ok()
    }
}

/// The first line of an entry file.
struct Header {
    expires: i64,
    /// The value's length, when the file is framed.
    len: Option<usize>,
    /// Where the value starts.
    end: usize,
}

impl Disk {
    pub(crate) fn open(o: &Options, dir: &Path, now: SystemTime) -> Disk {
        let mut unusable = false;
        if let Err(e) = fs::create_dir_all(dir) {
            (o.log)(&format!(
                "cache dir {} not writable ({e}) — persistence disabled, memory still serving",
                dir.display()
            ));
            unusable = true;
        }
        let disk = Disk {
            dir: dir.to_path_buf(),
            naming: o.naming,
            accept_unframed: o.accept_unframed,
            min_persist_ttl: o.min_persist_ttl,
            cost: o.cost,
            log: o.log,
            max_bytes: AtomicI64::new(o.disk_bytes),
            state: Mutex::new(State { off: unusable, write_off: false, unusable, written: 0 }),
            once: Once::new(),
            stats: DiskStats::default(),
            index: Mutex::new(Index::new()),
            hasher: DefaultHashBuilder::default(),
            next_generation: AtomicU64::new(1),
            burst_sweep: AtomicU8::new(0),
        };
        if !unusable {
            // One walk of the directory does both jobs: it builds the index (a file the index does not know is read
            // once) and reclaims what expired or is torn while the process was down.
            let n = disk.sweep(now);
            if n > 0 {
                (o.log)(&format!("swept {n} cache entries at startup"));
            }
        }
        disk
    }

    pub(crate) fn path_for(&self, key: &str) -> PathBuf {
        self.dir.join(self.naming.file_name(key))
    }

    pub(crate) fn set_max_bytes(&self, bytes: i64) {
        self.max_bytes.store(bytes, Ordering::Relaxed);
    }

    pub(crate) fn max_bytes(&self) -> i64 {
        self.max_bytes.load(Ordering::Relaxed)
    }

    fn lock(&self) -> MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn index(&self) -> MutexGuard<'_, Index> {
        self.index.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Whether reads are off.
    pub(crate) fn disabled(&self) -> bool {
        self.lock().off
    }

    /// Whether the tier is accepting writes.
    pub(crate) fn writes_off(&self) -> bool {
        let st = self.lock();
        st.off || st.write_off
    }

    /// Turns reads and writes off after a failure that could resurrect a stale file, and logs why, once per process.
    pub(crate) fn disable(&self, op: &str, err: &dyn Display) {
        self.lock().off = true;
        self.said(op, err);
    }

    /// Turns writes off after a failed write; what is on disk is still served.
    fn disable_writes(&self, op: &str, err: &dyn Display) {
        self.lock().write_off = true;
        self.said(op, err);
    }

    fn said(&self, op: &str, err: &dyn Display) {
        self.once.call_once(|| {
            (self.log)(&format!("cache {op} failed ({err}) — persistence disabled, memory still serving"))
        });
    }

    fn fingerprint(&self, value: &str) -> (u64, usize) {
        (self.hasher.hash_one(value), value.len())
    }

    // ---- reads ------------------------------------------------------------------------------------------------

    /// The entry for `key` if a fresh, intact file holds it. An expired, torn or unreadable file is retired.
    pub(crate) fn read(&self, key: &str, now: SystemTime) -> Option<(String, Loaded)> {
        if self.disabled() {
            return None;
        }
        let name = self.naming.file_name(key);
        let now_s = unix_secs(now);
        // The index is the directory: a key it does not know has no file, and a key it knows is expired without
        // opening it.
        let meta = self.index().get(&name).copied()?;
        if now_s >= meta.expires {
            self.retire_generation(&name, meta.generation);
            return None;
        }
        let raw = fs::read(self.dir.join(&name));
        self.stats.read(raw.as_ref().map_or(0, Vec::len));
        let raw = match raw {
            Ok(raw) => raw,
            Err(e) => {
                // The file went after the index was told about it. The sweep only walks files that exist, so nothing
                // else would ever drop this entry, and every get would repeat the failed read.
                if e.kind() == io::ErrorKind::NotFound {
                    let mut idx = self.index();
                    if idx.get(&name).is_some_and(|m| m.generation == meta.generation) {
                        idx.remove(&name);
                    }
                }
                return None;
            }
        };
        // Wall-clock on disk on purpose: a monotonic deadline means nothing to the process that reads the file after
        // a restart.
        let intact = parse_header(&raw, self.accept_unframed)
            .filter(|h| h.len.is_none_or(|len| raw.len() - h.end == len))
            .filter(|h| now_s < h.expires);
        let Some(h) = intact else {
            // Expired, torn, or in a format this cache does not read. All are unusable, and an unreadable file left in
            // place is never reclaimed by anything.
            self.retire_generation(&name, meta.generation);
            return None;
        };
        // A rename may have happened while the file was being read. Never let bytes from the superseded generation
        // repopulate memory or teach the new generation the old fingerprint.
        if self.index().get(&name).is_none_or(|m| m.generation != meta.generation) {
            return None;
        }
        Some((name, Loaded { start: h.end, expires: h.expires, generation: meta.generation, raw }))
    }

    /// Records that this process has seen `value` as the file's content, so the same put later writes nothing.
    pub(crate) fn remember(&self, name: &str, generation: u64, value: &str) {
        let fp = self.fingerprint(value);
        if let Some(m) = self.index().get_mut(name).filter(|m| m.generation == generation) {
            m.value = Some(fp);
        }
    }

    // ---- writes -----------------------------------------------------------------------------------------------

    /// Returns whether this write took the since-last-sweep total past the budget, so the caller should sweep early.
    pub(crate) fn put(&self, key: &str, value: &str, ttl: Duration, class: Class, now: SystemTime) -> bool {
        let name = self.naming.file_name(key);
        // A volatile value supersedes every older value for this key, including one written through by an earlier
        // release. Merely leaving the durable file alone makes it reappear as soon as the volatile entry expires or is
        // evicted, and again after every restart. The same holds for a durable put too short-lived to be written.
        // Removal is best-effort, and done even after persistence has disabled writes: an existing stale file can
        // still be readable, and removing it is the only operation needed.
        if class == Class::Volatile || ttl < self.min_persist_ttl {
            self.retire(&name);
            return false;
        }
        if self.writes_off() {
            return false;
        }
        // Skipping the rewrite keeps the file's older expiry: after a restart the entry can lapse up to a quarter of
        // `ttl` early. Refreshing only the expiry would still mean rewriting the file, and saving that write is the
        // point of the rule.
        if self.on_disk_is_current(&name, value, ttl, unix_secs(now)) {
            self.stats.skipped.fetch_add(1, Ordering::Relaxed);
            return false;
        }
        self.write_now(&name, key, unix_secs_after(now, ttl), value, now)
    }

    /// Whether the file for `name` already holds `value` with most of a `ttl` still ahead of it.
    fn on_disk_is_current(&self, name: &str, value: &str, ttl: Duration, now: i64) -> bool {
        let Some(meta) = self.index().get(name).copied() else { return false };
        // Most of the asked life is left, and the file does not outlive what is asked now: a shorter TTL than the file
        // holds must be written, or the old long expiry would stand.
        let lasts = u64::try_from(meta.expires - now).is_ok_and(|left| {
            u128::from(left) * 4 >= u128::from(ttl.as_secs()) * 3 && left <= ttl.as_secs() + 1
        });
        lasts && meta.value == Some(self.fingerprint(value))
    }

    fn write_now(&self, name: &str, key: &str, expires: i64, value: &str, now: SystemTime) -> bool {
        let header = format!("{expires} {}\n", value.len());
        let mut body = String::with_capacity(header.len() + value.len());
        body.push_str(&header);
        body.push_str(value);
        let tmp = match write_temp(&self.dir, &body) {
            Ok(tmp) => tmp,
            Err((op, e)) => {
                self.stats.write_failures.fetch_add(1, Ordering::Relaxed);
                self.disable_writes(op, &e);
                return false;
            }
        };
        // The rename and its index commit are one namespace transaction with retire and sweep eviction. The temp
        // write stays outside the lock, so unrelated reads and the expensive part of concurrent writes do not queue.
        let mut idx = self.index();
        if let Err(e) = fs::rename(&tmp, self.dir.join(name)) {
            drop(idx);
            let _ = fs::remove_file(&tmp);
            self.stats.write_failures.fetch_add(1, Ordering::Relaxed);
            self.disable_writes("rename", &e);
            return false;
        }
        self.stats.writes.fetch_add(1, Ordering::Relaxed);
        self.stats.bytes_written.fetch_add(body.len() as u64, Ordering::Relaxed);
        let generation = self.next_generation.fetch_add(1, Ordering::Relaxed);
        idx.insert(
            name.to_owned(),
            Meta {
                generation,
                expires,
                size: body.len() as u64,
                written: now,
                value: Some(self.fingerprint(value)),
                rank: (self.cost)(key),
            },
        );
        drop(idx);
        self.note_written(body.len())
    }

    /// Adds to the since-last-sweep counter and reports whether it has passed the budget. Counted, not measured:
    /// statting the directory per write would put a directory scan on every put.
    fn note_written(&self, n: usize) -> bool {
        let max = self.max_bytes();
        let mut st = self.lock();
        if max <= 0 {
            return false;
        }
        st.written += n as i64;
        if st.written < max {
            return false;
        }
        st.written = 0; // one sweep per burst, not one per write past the line
        true
    }

    /// Removes the file for `name`, if the index knows one: a key that was never written costs no syscall. Removal is
    /// best-effort; a failure disables persistence rather than let the stale file come back.
    pub(crate) fn retire(&self, name: &str) {
        let mut idx = self.index();
        if !idx.contains_key(name) {
            return;
        }
        self.stats.unlink();
        match fs::remove_file(self.dir.join(name)) {
            Ok(()) => {
                idx.remove(name);
            }
            Err(e) if e.kind() == io::ErrorKind::NotFound => {
                idx.remove(name);
            }
            Err(e) => {
                // Keep a tombstone in the index so the next sweep retries the unlink instead of rediscovering the
                // still-fresh stale file as live when it re-enables persistence.
                if let Some(meta) = idx.get_mut(name) {
                    meta.expires = 0;
                }
                drop(idx);
                self.disable("retire durable entry", &e);
            }
        }
    }

    fn retire_generation(&self, name: &str, generation: u64) {
        let mut idx = self.index();
        if idx.get(name).is_none_or(|m| m.generation != generation) {
            return;
        }
        self.stats.unlink();
        match fs::remove_file(self.dir.join(name)) {
            Ok(()) => {
                idx.remove(name);
            }
            Err(e) if e.kind() == io::ErrorKind::NotFound => {
                idx.remove(name);
            }
            Err(e) => {
                if let Some(meta) = idx.get_mut(name) {
                    meta.expires = 0;
                }
                drop(idx);
                self.disable("retire durable entry", &e);
            }
        }
    }

    #[cfg(test)]
    pub(crate) fn generation_for(&self, key: &str) -> Option<u64> {
        self.index().get(&self.naming.file_name(key)).map(|m| m.generation)
    }

    #[cfg(test)]
    pub(crate) fn retire_generation_for_test(&self, key: &str, generation: u64) {
        self.retire_generation(&self.naming.file_name(key), generation);
    }

    pub(crate) fn remove(&self, key: &str) {
        self.retire(&self.naming.file_name(key));
    }

    // ---- scan and sweep ---------------------------------------------------------------------------------------

    /// What one file says about itself: one head read. A file that cannot be understood is described as already
    /// expired, because a get cannot serve it either way, and the sweep then removes it. The expiry and length are the first line, so a short read reaches them; the file's size says whether the rest
    /// of it is there. `None` only for a file that is gone or cannot be opened (left alone: unreadable is not expired,
    /// and a volume whose uid shifted must not lose its whole store to a sweep).
    fn scan_entry(&self, name: &str, path: &Path) -> Option<Meta> {
        self.stats.stat_calls.fetch_add(1, Ordering::Relaxed);
        let mut file = File::open(path).ok()?;
        let info = file.metadata().ok()?;
        // Two 20-digit numbers, a space and a newline fit with room to spare.
        let mut head = [0u8; 48];
        // A head that cannot be read has no newline and counts as expired.
        let n = file.read(&mut head).unwrap_or(0);
        self.stats.read(n);
        let expires = parse_header(&head[..n], self.accept_unframed)
            // A framed file shorter or longer than its header says is torn (or two writers met): not servable.
            .filter(|h| h.len.is_none_or(|len| info.len() == (h.end + len) as u64))
            .map_or(0, |h| h.expires);
        let rank = self.naming.key_prefix(name).map_or(0, |k| (self.cost)(&k));
        Some(Meta {
            generation: self.next_generation.fetch_add(1, Ordering::Relaxed),
            expires,
            size: info.len(),
            written: info.modified().unwrap_or(UNIX_EPOCH),
            value: None,
            rank,
        })
    }

    pub(crate) fn request_burst_sweep(&self) -> bool {
        let mut state = self.burst_sweep.load(Ordering::Acquire);
        loop {
            let next = if state == 0 { 1 } else { 2 };
            match self.burst_sweep.compare_exchange_weak(state, next, Ordering::AcqRel, Ordering::Acquire) {
                Ok(previous) => return previous == 0,
                Err(current) => state = current,
            }
        }
    }

    pub(crate) fn sweep_after_burst(&self, now: SystemTime) {
        loop {
            let n = self.sweep(now);
            if n > 0 {
                (self.log)(&format!("swept {n} cache entries after a write burst"));
            }
            let requested = self.burst_sweep.swap(0, Ordering::AcqRel);
            if requested != 2
                || self.burst_sweep.compare_exchange(0, 1, Ordering::AcqRel, Ordering::Acquire).is_err()
            {
                break;
            }
        }
    }

    /// Removes expired entries, abandoned temp files and, past the budget, the entries cheapest to lose (lowest rank
    /// first, oldest first within a rank). Returns how many files it removed.
    ///
    /// Expiry was once enforced only on READ, which is only for a key someone asks for again; every key nobody
    /// re-reads was written once and kept forever. Best-effort: an unreadable directory, or a file that vanishes
    /// underneath us, is skipped rather than fatal.
    ///
    /// One directory listing. A file the index knows is judged from the index; only a file it does not know (written by
    /// something else, or after the last scan) is opened.
    pub(crate) fn sweep(&self, now: SystemTime) -> i64 {
        {
            // Reaping is what frees the space a full volume was short of, so a write that failed gets another chance.
            let mut st = self.lock();
            if !st.unusable {
                st.off = false;
                st.write_off = false;
            }
            // Claim only the bytes that existed when this sweep began. Puts during the directory walk accumulate
            // toward the next coalesced sweep instead of being erased at the end.
            st.written = 0;
        }
        self.stats.readdirs.fetch_add(1, Ordering::Relaxed);
        let Ok(entries) = fs::read_dir(&self.dir) else {
            return 0;
        };
        let now_unix = unix_secs(now);
        let mut removed = 0;
        // Survivors, so the budget pass below does not look at the directory again.
        let mut live: Vec<(String, Meta)> = Vec::new();
        let mut live_bytes: i64 = 0;
        for entry in entries.flatten() {
            if entry.file_type().map_or(true, |t| t.is_dir()) {
                continue;
            }
            let path = entry.path();
            let name = entry.file_name().to_string_lossy().into_owned();
            if self.naming.is_entry(&name) {
                let mut idx = self.index();
                let known = idx.get(&name).copied();
                let Some(meta) = known.or_else(|| self.scan_entry(&name, &path)) else { continue };
                if known.is_none() {
                    idx.insert(name.clone(), meta);
                }
                drop(idx);
                if meta.expires > now_unix {
                    live_bytes += meta.size as i64;
                    live.push((name, meta));
                    continue;
                }
                // Removed under the lock, and only if the entry is still the one judged: a put that rewrote it since
                // the listing was read has a later expiry and must keep its file.
                let mut idx = self.index();
                if idx.get(&name).is_some_and(|m| m.generation != meta.generation) {
                    continue;
                }
                self.stats.unlink();
                match fs::remove_file(&path) {
                    Ok(()) => {
                        idx.remove(&name);
                        removed += 1;
                    }
                    Err(e) if e.kind() == io::ErrorKind::NotFound => {
                        idx.remove(&name);
                    }
                    Err(_) => {}
                }
            } else if name.starts_with(".tmp-") || self.naming.is_legacy_temp(&name) {
                // A temp file outlives its put only if the process died between create and rename. The grace period
                // keeps the sweep from racing a write that is legitimately in flight.
                let Ok(info) = entry.metadata() else { continue };
                // Judged against the real time at this file, not the `now` the sweep was started with: a temp's mtime
                // is a real one, and a long sweep must not find a write that began meanwhile to be "in the future".
                match info.modified().ok().map(|m| SystemTime::now().duration_since(m)) {
                    Some(Ok(age)) if age >= TEMP_FILE_GRACE => {
                        self.stats.unlink();
                        if fs::remove_file(&path).is_ok() {
                            removed += 1;
                        }
                    }
                    // Young enough to still be in flight.
                    Some(Ok(_)) => {}
                    // Created a moment after `now` was read: another thread's write, in flight.
                    Some(Err(ahead)) if ahead.duration() < FUTURE_MTIME_SLACK => {}
                    // Undatable: the mtime reads as the future (a clock stepped back) or the filesystem does not say.
                    // Deleting on that signal would kill in-flight writes, which the grace period exists to stop, and
                    // keeping it forever is a leak. So it is budgeted as the oldest, cheapest thing there: never
                    // deleted on sight, first to go once the store is over budget.
                    _ => {
                        live_bytes += info.len() as i64;
                        let meta = Meta {
                            generation: 0,
                            expires: i64::MAX,
                            size: info.len(),
                            written: UNIX_EPOCH,
                            value: None,
                            rank: 0,
                        };
                        live.push((name, meta));
                    }
                }
            }
        }
        // Expiry alone is not a ceiling; it is a schedule. Cheapest to rebuild first, then oldest: age alone is the
        // wrong question, since a hit never touches the file and a use-every-evening entry looks as old as one read
        // once and abandoned.
        let max_bytes = self.max_bytes();
        if max_bytes <= 0 || live_bytes <= max_bytes {
            return removed;
        }
        live.sort_by_key(|(_, m)| (m.rank, m.written));
        for (name, meta) in &live {
            if live_bytes <= max_bytes {
                break;
            }
            if meta.generation == 0 {
                self.stats.unlink();
                if fs::remove_file(self.dir.join(name)).is_ok() {
                    live_bytes -= meta.size as i64;
                    removed += 1;
                }
                continue;
            }
            let mut idx = self.index();
            if idx.get(name).is_none_or(|current| current.generation != meta.generation) {
                continue;
            }
            self.stats.unlink();
            if fs::remove_file(self.dir.join(name)).is_ok() {
                idx.remove(name);
                live_bytes -= meta.size as i64;
                removed += 1;
            }
        }
        (self.log)(&format!(
            "cache volume over its {max_bytes}-byte budget; evicted down to {live_bytes} bytes"
        ));
        removed
    }
}

/// The first line of an entry: `<expiry> <len>` (framed) or `<expiry>` (the older Scout format, when accepted).
fn parse_header(raw: &[u8], accept_unframed: bool) -> Option<Header> {
    let nl = raw.iter().position(|&b| b == b'\n').filter(|&nl| nl > 0)?;
    let line = std::str::from_utf8(&raw[..nl]).ok()?;
    match line.split_once(' ') {
        Some((expires, len)) => {
            Some(Header { expires: expires.parse().ok()?, len: Some(len.parse().ok()?), end: nl + 1 })
        }
        None if accept_unframed => Some(Header { expires: line.parse().ok()?, len: None, end: nl + 1 }),
        None => None,
    }
}

/// Writes `body` to a unique temp file beside the eventual destination. Its caller serializes the rename with index
/// mutation, so a half-written entry is never served and retirement cannot unlink a newer generation.
fn write_temp(dir: &Path, body: &str) -> Result<PathBuf, (&'static str, io::Error)> {
    let tmp = dir.join(temp_name());
    let mut file =
        OpenOptions::new().write(true).create_new(true).open(&tmp).map_err(|e| ("create temp", e))?;
    let written = file.write_all(body.as_bytes()).map_err(|e| ("write", e));
    drop(file);
    let result = written.map(|()| tmp.clone());
    if result.is_err() {
        let _ = fs::remove_file(&tmp);
    }
    result
}

/// A temp name no other write, in this process or a previous one, can share: process id, wall-clock nanoseconds and a
/// per-process counter.
fn temp_name() -> String {
    static SEQ: AtomicU64 = AtomicU64::new(0);
    let nanos = SystemTime::now().duration_since(UNIX_EPOCH).map_or(0, |d| d.as_nanos());
    format!(".tmp-{}-{}-{}", std::process::id(), nanos, SEQ.fetch_add(1, Ordering::Relaxed))
}

pub(crate) fn unix_secs(t: SystemTime) -> i64 {
    t.duration_since(UNIX_EPOCH).map_or(0, |d| d.as_secs() as i64)
}

/// `now + ttl` in whole seconds, saturating instead of overflowing on an absurd `ttl`.
fn unix_secs_after(now: SystemTime, ttl: Duration) -> i64 {
    let since_epoch = now.duration_since(UNIX_EPOCH).unwrap_or_default().saturating_add(ttl);
    i64::try_from(since_epoch.as_secs()).unwrap_or(i64::MAX)
}

/// The time left until `expires`, to the nanosecond. Go's `Duration` saturates at about 292 years, which is also where
/// an out-of-range `expires` lands here, so a timestamp no clock can represent does not panic.
pub(crate) fn until(expires: i64, now: SystemTime) -> Duration {
    const MAX: Duration = Duration::from_nanos(i64::MAX as u64);
    let Ok(secs) = u64::try_from(expires) else { return Duration::ZERO };
    let Some(at) = UNIX_EPOCH.checked_add(Duration::from_secs(secs)) else { return MAX };
    at.duration_since(now).unwrap_or_default().min(MAX)
}
