//! Two-tier artifact cache: a byte-bounded in-memory tier backed by an optional disk store, so a container
//! restart/redeploy doesn't cold-start (SRTs are tiny and worth persisting). Holds the finished artifacts — search
//! results, downloaded SRTs, translated SRTs — keyed so each is fetched once. Thread-safe.
//!
//! The tiers are `den-cache`, shared with Scout: memory is the hot tier (bounded), disk is the durable tier, and a
//! memory miss lazily reads it back (repopulating memory). Disk is best-effort — a failed write or unwritable dir
//! turns persistence off and logs; memory still serves. Expiry is wall-clock in both tiers. What is Subtitles' own
//! here is the body type (it memoizes its ETag and cue stats), the key namespaces, and what each costs to lose.

use std::path::PathBuf;
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use bytes::Bytes;
use den_cache::{Class, Naming, Options, Value};

/// The three namespaces `evict_rank` prices, defined HERE because this is the module that reads
/// them back and acts on them. They were spelled as literals at both ends — built in `addon.rs`,
/// re-typed in the ranking — and nothing linked the two. An unmatched prefix falls to rank 3, which
/// is evicted LAST, so renaming a namespace would not degrade the ranking but invert it: free
/// search results would become the most protected entries in the store and translations the first
/// spent. Both sides now name the same constant, so that rename cannot compile into a silent
/// inversion.
pub const SEARCH_NS: &str = "search:";
pub const OS_NS: &str = "os:";
pub const TRANSLATE_NS: &str = "translate:";

/// Hard ceiling for the in-memory hot tier. This is coupled to the three-slot sync admission and
/// its 512 MiB full-service CI gate: a larger `CACHE_MAX_BYTES` grows only the disk tier, never the
/// resident headroom reserved for subprocesses and transient buffers.
pub const MAX_MEMORY_BYTES: usize = 64 * 1024 * 1024;

/// Memory shards are only added while each stays this large. A value heavier than its shard's budget is not kept in
/// memory, and `fetch::MAX_BODY` (12 MiB) is the heaviest body a download can produce.
const MIN_SHARD_BYTES: usize = 16 * 1024 * 1024;

/// A cached value: shared text, valid UTF-8 by construction (it is only ever built from a `String`),
/// so reading it as `&str` costs nothing and handing it to a response copies nothing.
///
/// Cloning is a refcount bump, so concurrent serves of a hot body hold one copy between them rather
/// than one each. The cue-density numbers the serve log reports are measured at most once per body,
/// on the first hit that asks, and shared by every later one; so is its ETag.
#[derive(Clone)]
pub struct Text(Arc<TextInner>);

struct TextInner {
    text: String,
    stats: OnceLock<Option<crate::srt::Stats>>,
    etag: OnceLock<String>,
}

impl Text {
    pub fn new(text: String) -> Text {
        Text(Arc::new(TextInner { text, stats: OnceLock::new(), etag: OnceLock::new() }))
    }

    /// `httputil::etag_of` over this body, hashed on the first call only.
    pub fn etag(&self) -> &str {
        self.0.etag.get_or_init(|| crate::httputil::etag_of(self.0.text.as_bytes()))
    }

    pub fn as_str(&self) -> &str {
        &self.0.text
    }

    /// The body as a response payload, sharing this allocation instead of copying it.
    pub fn to_bytes(&self) -> Bytes {
        Bytes::from_owner(self.clone())
    }

    /// `srt::stats` over this body, parsed on the first call only.
    pub fn stats(&self) -> Option<crate::srt::Stats> {
        *self.0.stats.get_or_init(|| crate::srt::stats(&crate::srt::parse(&self.0.text)))
    }
}

impl AsRef<[u8]> for Text {
    fn as_ref(&self) -> &[u8] {
        self.0.text.as_bytes()
    }
}

impl Value for Text {
    fn from_str(text: &str) -> Text {
        Text::new(text.to_owned())
    }

    fn from_string(text: String) -> Text {
        Text::new(text)
    }

    fn as_str(&self) -> &str {
        Text::as_str(self)
    }
}

pub struct Cache {
    inner: den_cache::Cache<Text>,
}

/// The cache as `/metrics` reports it.
pub struct Stats {
    pub entries: usize,
    pub bytes: usize,
    pub disk: bool,
    pub disk_write_failures: u64,
}

/// What losing the entry for `key` costs, cheapest first: what a byte-budget eviction of the disk tier weighs it by.
///
/// Age alone is the wrong question. A hit never touches the file, so the disk tier evicts in WRITE order, not use
/// order: a translation served every evening for two months looks exactly as old as one watched once and abandoned,
/// and gets deleted just as readily. And the entries are nowhere near equal in what losing them costs — a search is a
/// free round trip to rebuild, a downloaded subtitle is one metered credit, a translation is a whole film's LLM bill.
///
/// Read off the key's namespace. For an over-long key the file name carries its first 32 characters, which is all
/// this needs. A file this cache cannot name is a file it cannot serve, so it ranks lowest.
fn evict_rank(key: &str) -> u8 {
    if key.starts_with(SEARCH_NS) {
        0 // a free round trip to rebuild
    } else if key.starts_with(OS_NS) {
        1 // one metered download credit
    } else if key.starts_with(TRANSLATE_NS) {
        2 // a whole film's LLM bill
    } else {
        // Pins, allowance counters, failure markers. All tiny, so evicting them frees nothing, and each one costs
        // something to lose: a pin is a metered download, an allowance counter is the ceiling on a whole day's spend,
        // a failure marker is the backoff that keeps a known-bad source from being bought again.
        3
    }
}

impl Cache {
    pub fn stats(&self) -> Stats {
        let mem = self.inner.mem_stats();
        Stats {
            entries: mem.entries,
            bytes: mem.bytes,
            disk: self.inner.disk_enabled(),
            disk_write_failures: self.inner.write_failures(),
        }
    }

    /// `dir = Some` enables the disk tier (created if missing; falls back to memory-only on failure).
    ///
    /// `max_bytes` is the disk budget; memory is clamped to `MAX_MEMORY_BYTES`. Nothing else reclaims disk: an entry
    /// is deleted lazily by a read, which needs someone to ask for that exact key again — and search keys carry a
    /// per-file hash, so once a title is watched its entry is never re-read. So the sweep bounds it: expired first,
    /// then cheapest to lose, then oldest-written until under budget.
    pub fn new(max_bytes: usize, dir: Option<PathBuf>) -> Cache {
        Cache::open(max_bytes, dir, None)
    }

    /// `new` reading time from `clock` from the first sweep on, so a replay runs days of traffic in seconds.
    #[cfg(feature = "bench")]
    pub fn new_with_clock(max_bytes: usize, dir: Option<PathBuf>, clock: den_cache::Clock) -> Cache {
        Cache::open(max_bytes, dir, Some(clock))
    }

    fn open(max_bytes: usize, dir: Option<PathBuf>, clock: Option<den_cache::Clock>) -> Cache {
        let options = Options {
            dir,
            disk_bytes: i64::try_from(max_bytes).unwrap_or(i64::MAX),
            naming: Naming::Readable,
            // Every `put` here is a fact meant to outlive a restart, including the ten-minute failure markers that
            // keep a known-bad source from being bought again.
            min_persist_ttl: Duration::ZERO,
            // Every file this cache ever kept a length in; one without is from before that and is dropped, because
            // nothing says whether it is whole.
            accept_unframed: false,
            min_shard_bytes: MIN_SHARD_BYTES,
            cost: evict_rank,
            log: |line| eprintln!("warning: {line}"),
            ..Options::new(max_bytes.min(MAX_MEMORY_BYTES))
        };
        let mut inner = den_cache::Cache::new(options);
        if let Some(clock) = clock {
            inner.set_clock(clock);
        }
        let cache = Cache { inner };
        cache.sweep();
        cache
    }

    /// What the disk tier has asked of the filesystem since it was built.
    #[cfg(any(test, feature = "bench"))]
    pub fn disk_counts(&self) -> den_cache::DiskCounts {
        self.inner.disk_counts()
    }

    /// Bound both tiers: drops what has expired and trims the disk to its budget.
    pub fn sweep(&self) {
        self.inner.sweep();
    }

    /// An owned copy of the value. For small markers and counters; a body that is served should go
    /// through `get_shared`, which copies nothing.
    pub fn get(&self, key: &str) -> Option<String> {
        self.get_shared(key).map(|v| v.as_str().to_owned())
    }

    /// The value as a refcounted view of the cached allocation: no copy, and it stays valid after the
    /// entry is evicted or expires (the memory is then freed with the last reader).
    pub fn get_shared(&self, key: &str) -> Option<Text> {
        self.inner.get(key)
    }

    /// Written through to disk (unless an unchanged value is already there): the right trade for an artifact fetched
    /// once and served for sixty days.
    pub fn put(&self, key: String, value: String, ttl: Duration) {
        self.inner.put(&key, Text::new(value), ttl, Class::Durable);
    }

    /// How long `key` has left, if it is present — for a marker whose lifetime is the answer, like a
    /// backoff a client is told to wait out. Reads the tiers the way `get` does.
    pub fn ttl(&self, key: &str) -> Option<Duration> {
        self.inner.ttl(key)
    }

    /// Remember this in memory only.
    ///
    /// `put` writes through to disk synchronously — an `fs::write` plus a rename, on the one runtime
    /// thread that also serves every connection. That is the right trade for an artifact fetched once
    /// and served for sixty days. It is the wrong one for something written a hundred and fifty times
    /// per film, which exists only to rescue a retry a few minutes later and is redundant the moment
    /// the finished artifact is cached.
    ///
    /// The cost is real and worth naming: a restart mid-run — a deploy, an OOM, a host reboot —
    /// now re-buys the whole film, where a disk-backed entry would have rescued the batches already
    /// paid for. That is the trade. A retry by the same process, which is the common case and the
    /// one the viewer is sitting in front of, is unaffected.
    pub fn put_mem(&self, key: String, value: String, ttl: Duration) {
        self.inner.put(&key, Text::new(value), ttl, Class::Volatile);
    }

    /// Forget a key in both tiers. For an entry that has been proven wrong rather than merely stale
    /// — the alternative, writing it back with a zero TTL, means a blocking disk write to say
    /// "delete this".
    pub fn remove(&self, key: &str) {
        self.inner.remove(key);
    }

    /// Forget a memory-only key. The companion to `put_mem` that `remove` would be wrong for: that
    /// one also unlinks a file, which for a key nothing ever wrote to disk is a syscall to delete
    /// something that was never there.
    pub fn remove_mem(&self, key: &str) {
        self.inner.remove_mem(key);
    }

    /// Read from memory only. The companion to `put_mem`: going through `get` would fall through to
    /// a disk probe that, for a key only ever written by `put_mem`, cannot hit.
    pub fn get_mem(&self, key: &str) -> Option<String> {
        self.inner.get_mem(key).map(|v| v.as_str().to_owned())
    }

    #[cfg(test)]
    fn disk_path(&self, key: &str) -> PathBuf {
        self.inner.path_for(key).expect("a disk tier")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::fs::PermissionsExt;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::{SystemTime, UNIX_EPOCH};

    static SEQ: AtomicU64 = AtomicU64::new(0);

    fn tmpdir(name: &str) -> PathBuf {
        // The counter restarts per process, so include the pid: two concurrent `cargo test` runs
        // otherwise pick the same directory and delete each other's files.
        let d = std::env::temp_dir().join(format!(
            "den-subs-cache-{name}-{}-{}",
            std::process::id(),
            SEQ.fetch_add(1, Ordering::Relaxed)
        ));
        let _ = std::fs::remove_dir_all(&d);
        d
    }

    const HOUR: Duration = Duration::from_secs(3600);

    fn unix_now() -> u64 {
        SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs()
    }

    /// A cache whose disk budget is off while a test writes, so the sweep a burst past a tiny budget starts on its own
    /// thread cannot race the explicit sweep the test is here to observe. `budget` is switched on afterwards.
    fn unbudgeted(mem: usize, dir: &std::path::Path) -> Cache {
        let c = Cache::new(mem, Some(dir.to_path_buf()));
        c.inner.set_disk_bytes(0);
        c
    }

    #[test]
    fn a_value_survives_a_restart_via_the_disk_tier() {
        let dir = tmpdir("restart");
        let c = Cache::new(1 << 20, Some(dir.clone()));
        c.put("k".into(), "v".into(), HOUR);
        // A fresh Cache over the same dir is what a redeploy looks like: memory empty, disk warm.
        let c2 = Cache::new(1 << 20, Some(dir));
        assert_eq!(c2.get("k"), Some("v".into()));
    }

    #[test]
    fn an_expired_disk_entry_is_not_served() {
        let dir = tmpdir("expired");
        let c = Cache::new(1 << 20, Some(dir.clone()));
        c.put("k".into(), "v".into(), Duration::from_secs(0));
        assert_eq!(Cache::new(1 << 20, Some(dir)).get("k"), None);
    }

    /// The whole reason writes carry a length: a half-written file parses fine (any prefix with a newline splits into
    /// expiry + value), so a torn write would be served as a complete value for the full TTL.
    #[test]
    fn a_torn_write_never_becomes_a_readable_entry() {
        let dir = tmpdir("torn");
        let c = Cache::new(1 << 20, Some(dir.clone()));
        c.put("k".into(), "the whole subtitle".into(), HOUR);
        let path = c.disk_path("k");

        // Truncate the file the way ENOSPC or a kill mid-write would. It still parses — that is the hazard — so the
        // recorded length is what has to reject it.
        let raw = std::fs::read_to_string(&path).unwrap();
        std::fs::write(&path, &raw[..raw.len() - 6]).unwrap();
        assert_eq!(Cache::new(1 << 20, Some(dir.clone())).get("k"), None, "served a truncated entry");
        assert!(!path.exists(), "a torn entry should be dropped, not left to be re-read");

        // A completed put must leave exactly one file: the entry. A temp left behind would be unreadable garbage that
        // nothing ever reclaims. (The torn file was already dropped above.)
        c.put("k".into(), "v2".into(), HOUR);
        let files: Vec<_> = std::fs::read_dir(&dir).unwrap().filter_map(|e| e.ok()).collect();
        assert_eq!(
            files.len(),
            1,
            "a put left scratch behind: {:?}",
            files.iter().map(|f| f.file_name()).collect::<Vec<_>>()
        );
        assert_eq!(Cache::new(1 << 20, Some(dir)).get("k"), Some("v2".into()));
    }

    /// An entry this build cannot read is deleted, not left behind: on a persistent volume with a disk budget that is a
    /// leak only the sweep would ever reclaim.
    #[test]
    fn an_unreadable_entry_is_reclaimed() {
        let dir = tmpdir("stale");
        let c = Cache::new(1 << 20, Some(dir.clone()));
        c.put("k".into(), "v".into(), HOUR);
        let path = c.disk_path("k");
        for raw in ["999999999999\nold format, no length", "", "garbage"] {
            std::fs::write(&path, raw).unwrap();
            assert_eq!(Cache::new(1 << 20, Some(dir.clone())).get("k"), None, "served {raw:?}");
            assert!(!path.exists(), "left an unreadable entry on disk: {raw:?}");
        }
    }

    /// The names and the file format this cache has always written, spelled out independently of `den-cache`: a store
    /// an earlier release left on the volume must open and serve. Short keys are the key's base64url; a key past 180
    /// bytes is the base64url of its first 32 characters, `~`, and the FNV-1a-64 of the whole key.
    #[test]
    fn a_store_an_earlier_release_wrote_still_loads() {
        use base64::engine::general_purpose::URL_SAFE_NO_PAD as B64;
        use base64::Engine;

        let dir = tmpdir("legacy");
        std::fs::create_dir_all(&dir).unwrap();
        let old_name = |key: &str| {
            if key.len() <= 180 {
                B64.encode(key.as_bytes())
            } else {
                let head: String = key.chars().take(32).collect();
                format!(
                    "{}~{:016x}",
                    B64.encode(head.as_bytes()),
                    crate::httputil::stable_hash(key.as_bytes())
                )
            }
        };
        let long = format!("{TRANSLATE_NS}{}", "x".repeat(200));
        let far = unix_now() + 3600;
        for (key, value) in [
            ("os:123", "1\n00:00:01,000 --> 00:00:02,000\nhej\n"),
            (long.as_str(), "långt"),
            ("source:tt1:0:0", "77"),
        ] {
            // `<expiry> <value-bytes>\n<value>`, the bytes of the value and not its characters.
            std::fs::write(dir.join(old_name(key)), format!("{far} {}\n{value}", value.len())).unwrap();
        }

        let c = Cache::new(1 << 20, Some(dir.clone()));
        assert_eq!(c.get("os:123").as_deref(), Some("1\n00:00:01,000 --> 00:00:02,000\nhej\n"));
        assert_eq!(
            c.get(&long).as_deref(),
            Some("långt"),
            "a long key's hashed name is no longer the one on disk"
        );
        assert_eq!(c.get("source:tt1:0:0").as_deref(), Some("77"));
        assert!(c.ttl("source:tt1:0:0").unwrap() > Duration::from_secs(3000));
        // And the same names are what a put writes, so nothing is orphaned by an upgrade.
        assert_eq!(c.disk_path("os:123"), dir.join(old_name("os:123")));
        assert_eq!(c.disk_path(&long), dir.join(old_name(&long)));
        // Rewritten like any other put, the file keeps its name.
        c.put("source:tt1:0:0".into(), "78".into(), HOUR);
        assert_eq!(
            std::fs::read_to_string(dir.join(old_name("source:tt1:0:0")))
                .unwrap()
                .split_once('\n')
                .unwrap()
                .1,
            "78"
        );
    }

    /// The same pin written twice in a row — once when it is chosen, once after the translation it fed — is one write.
    #[test]
    fn writing_the_same_value_again_costs_no_write() {
        let dir = tmpdir("same-value");
        let c = Cache::new(1 << 20, Some(dir));
        let day = Duration::from_secs(60 * 60 * 24 * 180);
        c.put("source:tt1:0:0".into(), "77".into(), day);
        c.put("source:tt1:0:0".into(), "77".into(), day);
        let n = c.inner.disk_counts();
        assert_eq!((n.writes, n.skipped), (1, 1), "{n:?}");
        c.put("source:tt1:0:0".into(), "78".into(), day);
        assert_eq!(c.inner.disk_counts().writes, 2, "a changed value was not written");
    }

    /// The disk tier has to be bounded too. Memory's cap never touched it, and lazy expiry needs
    /// someone to re-request the exact key — which search entries, keyed by a per-file hash, never
    /// get. Expired entries go first; if that isn't enough, the oldest do.
    #[test]
    fn the_sweep_bounds_the_disk_tier() {
        let dir = tmpdir("sweep");
        // Budget fits one entry, so after the expired one is dropped the oldest must still go.
        let c = unbudgeted(60, &dir);
        c.put("expired".into(), "x".repeat(40), Duration::from_secs(0));
        c.put("old".into(), "x".repeat(40), HOUR);
        std::thread::sleep(Duration::from_millis(5));
        c.put("new".into(), "x".repeat(40), HOUR);

        let expired_path = c.disk_path("expired");
        let old_path = c.disk_path("old");
        c.inner.set_disk_bytes(60);
        c.sweep();
        // Both branches must have run: dropping the expired entry alone left the store under budget, so this test used
        // to return before the eviction loop it exists to cover.
        assert!(!old_path.exists(), "the eviction loop did not run — the oldest entry survived");
        // Assert the FILE is gone, not that `get` misses — a read expires lazily, so a `get` returning None passes
        // whether or not the sweep did anything at all.
        assert!(!expired_path.exists(), "the sweep left an expired entry on disk");
        let fresh = Cache::new(60, Some(dir.clone()));
        assert_eq!(fresh.get("new"), Some("x".repeat(40)), "the newest entry must survive");
        let bytes: u64 =
            std::fs::read_dir(&dir).unwrap().flatten().map(|e| e.metadata().unwrap().len()).sum();
        assert!(bytes <= 60, "the sweep left {bytes} bytes on disk, over the 60 budget");
    }

    /// Under pressure the sweep spends the cheap entries first. Age alone made it evict in WRITE
    /// order, so a translation served every evening looked exactly as old as one watched once, and a
    /// free-to-refetch search entry written a minute later outlived it.
    #[test]
    fn the_sweep_spends_the_cheap_entries_first() {
        let dir = tmpdir("sweep-rank");
        // Built from the namespace constants the ranking itself reads, not from literals that only look like them.
        // Spelled out at both ends, a renamed namespace fell to the `else` arm — which is evicted LAST — so the
        // ranking would have inverted rather than degraded, and this test would have gone on passing over keys that
        // no longer existed.
        let translate_key = format!("{}500:SV:openai:m", TRANSLATE_NS);
        let search_key = format!("{}tt0111161:0:0:", SEARCH_NS);
        // The expensive entry is written FIRST, so age alone would take it.
        let c = unbudgeted(120, &dir);
        c.put(translate_key.clone(), "x".repeat(40), HOUR);
        std::thread::sleep(Duration::from_millis(5));
        c.put(format!("{}777", OS_NS), "x".repeat(40), HOUR);
        c.put(search_key.clone(), "x".repeat(40), HOUR);

        let translation = c.disk_path(&translate_key);
        let search = c.disk_path(&search_key);
        c.inner.set_disk_bytes(120);
        c.sweep();

        assert!(!search.exists(), "the free-to-rebuild entry survived");
        assert!(translation.exists(), "a film's LLM bill was evicted before a search round trip");

        // A restart reads the cost back off the file name, so the order survives it.
        let c = unbudgeted(120, &dir);
        c.put(search_key.clone(), "x".repeat(40), HOUR);
        let reopened = Cache::new(120, Some(dir.clone()));
        reopened.inner.set_disk_bytes(120);
        reopened.sweep();
        assert!(!reopened.disk_path(&search_key).exists());
        assert!(reopened.disk_path(&translate_key).exists());
    }

    /// A sweep must never remove a live entry while it is still under budget — evicting eagerly
    /// would re-download subtitles the cache exists to keep.
    #[test]
    fn the_sweep_keeps_everything_that_fits() {
        let dir = tmpdir("sweep-fits");
        let c = Cache::new(1 << 20, Some(dir.clone()));
        c.put("a".into(), "aaa".into(), HOUR);
        c.put("b".into(), "bbb".into(), HOUR);
        c.sweep();
        let fresh = Cache::new(1 << 20, Some(dir));
        assert_eq!(fresh.get("a"), Some("aaa".into()));
        assert_eq!(fresh.get("b"), Some("bbb".into()));
    }

    /// Unreadable is not expired. A redeploy that shifts the volume's uid makes every file
    /// unreadable at once, and a sweep that reads that as "expired" deletes the entire store.
    #[test]
    fn the_sweep_keeps_an_entry_it_cannot_read() {
        let dir = tmpdir("sweep-unreadable");
        let c = Cache::new(1 << 20, Some(dir.clone()));
        c.put("k".into(), "v".into(), HOUR);
        let path = c.disk_path("k");
        let mut perms = std::fs::metadata(&path).unwrap().permissions();
        perms.set_mode(0o000);
        std::fs::set_permissions(&path, perms).unwrap();

        // Both a cache that knows the file and one that opens the directory afterwards.
        c.sweep();
        Cache::new(1 << 20, Some(dir.clone())).sweep();
        assert!(path.exists(), "the sweep deleted an entry it merely could not read");

        let mut perms = std::fs::metadata(&path).unwrap().permissions();
        perms.set_mode(0o644);
        std::fs::set_permissions(&path, perms).unwrap();
        assert_eq!(Cache::new(1 << 20, Some(dir)).get("k"), Some("v".into()), "and it still reads");
    }

    /// A temp left by a kill between write and rename is addressed by nothing, but parses fine —
    /// so a sweep counted it as live and could evict a real entry to make room for it.
    #[test]
    fn the_sweep_reclaims_leftover_temps() {
        let dir = tmpdir("sweep-temp");
        let c = Cache::new(1 << 20, Some(dir.clone()));
        c.put("k".into(), "v".into(), HOUR);
        let stray = c.disk_path("k").with_extension("t99");
        std::fs::write(&stray, format!("{} 1\nv", unix_now() + 3600)).unwrap();
        backdate(&stray, Duration::from_secs(2 * 3600));
        c.sweep();
        assert!(!stray.exists(), "a leftover temp survived the sweep");
        assert_eq!(Cache::new(1 << 20, Some(dir)).get("k"), Some("v".into()), "the real entry stayed");
    }

    /// A write is a temp and then a rename. A sweep that unlinks the temp on sight can delete an in-flight write, which
    /// fails the rename and makes a healthy disk report itself degraded — so a temp too young to be abandoned is left
    /// alone.
    #[test]
    fn the_sweep_leaves_an_in_flight_temp_alone() {
        let dir = tmpdir("sweep-inflight");
        let c = Cache::new(1 << 20, Some(dir.clone()));
        c.put("k".into(), "v".into(), HOUR);
        let in_flight = c.disk_path("k").with_extension("t42");
        std::fs::write(&in_flight, "half-written").unwrap();
        c.sweep();
        assert!(in_flight.exists(), "the sweep deleted a temp that was still being written");
    }

    /// An undatable temp must not pile up forever either: nothing else reclaims it, so it is counted toward the budget
    /// and evicted under pressure like anything else.
    #[test]
    fn an_undatable_temp_is_still_bounded_by_the_budget() {
        let dir = tmpdir("sweep-undatable-budget");
        let c = unbudgeted(60, &dir);
        c.put("k".into(), "x".repeat(40), HOUR);
        let stray = c.disk_path("k").with_extension("t3");
        std::fs::write(&stray, "x".repeat(80)).unwrap();
        shift_mtime(&stray, SystemTime::now() + Duration::from_secs(5));
        let entry = c.disk_path("k");
        c.inner.set_disk_bytes(60);
        c.sweep();
        let bytes: u64 =
            std::fs::read_dir(&dir).unwrap().flatten().map(|e| e.metadata().unwrap().len()).sum();
        assert!(bytes <= 60, "an undatable temp escaped the budget: {bytes} bytes left");
        // And it goes BEFORE live data: evicting a real subtitle to keep a scratch file of unknown provenance costs a
        // re-download against the viewer's quota.
        assert!(!stray.exists(), "the temp outlived the entry it displaced");
        assert!(entry.exists(), "a live entry was evicted to make room for an undatable temp");
    }

    /// A temp whose mtime reads as being in the FUTURE — an NTP step backwards, or a filesystem that does not report
    /// mtime — has no age. Treating an unknown age as "old enough" deletes the in-flight write the grace period exists
    /// to protect.
    #[test]
    fn the_sweep_leaves_a_temp_it_cannot_date() {
        let dir = tmpdir("sweep-undatable");
        let c = Cache::new(1 << 20, Some(dir.clone()));
        c.put("k".into(), "v".into(), HOUR);
        let in_flight = c.disk_path("k").with_extension("t7");
        std::fs::write(&in_flight, "half-written").unwrap();
        shift_mtime(&in_flight, SystemTime::now() + Duration::from_secs(5));
        c.sweep();
        assert!(in_flight.exists(), "the sweep deleted a temp whose age it could not determine");
    }

    /// Push a file's modification time into the past.
    fn backdate(path: &std::path::Path, by: Duration) {
        shift_mtime(path, SystemTime::now() - by);
    }

    fn shift_mtime(path: &std::path::Path, when: SystemTime) {
        let f = std::fs::File::options().write(true).open(path).unwrap();
        f.set_times(std::fs::FileTimes::new().set_modified(when)).unwrap();
    }

    #[test]
    fn memory_is_bounded_by_bytes_and_keeps_what_is_read() {
        let c = Cache::new(240, None);
        c.put("hot".into(), "0123456789".into(), HOUR);
        for i in 0..40 {
            c.put(format!("scan{i:02}"), "0123456789".into(), HOUR);
            assert!(c.get("hot").is_some(), "the entry read on every write was evicted at scan{i:02}");
        }
        assert!(c.stats().bytes <= 240, "{} bytes resident", c.stats().bytes);
    }

    #[test]
    fn a_shared_hit_is_a_view_of_the_cached_bytes_and_counts_once() {
        let c = Cache::new(1 << 20, None);
        c.put("k".into(), "0123456789".into(), HOUR);
        let a = c.get_shared("k").unwrap();
        let b = c.get_shared("k").unwrap();
        assert_eq!(a.as_str(), "0123456789");
        assert_eq!(a.as_str().as_ptr(), b.as_str().as_ptr(), "a hit copied the body");
        assert_eq!(a.to_bytes().as_ptr(), a.as_str().as_ptr(), "the response payload copied the body");
        // The memoized validator is exactly what hashing the body per response gave.
        for _ in 0..2 {
            assert_eq!(a.etag(), crate::httputil::etag_of(b"0123456789"));
        }
        assert_eq!(c.stats().bytes, "k".len() + 10, "readers must not add to the budget");
        // A reader outlives the entry's eviction and still sees the same bytes.
        c.remove("k");
        assert_eq!(a.as_str(), "0123456789");
        assert_eq!(c.stats().bytes, 0);
    }

    #[test]
    fn a_bodys_cue_stats_are_those_of_parsing_it() {
        let mut srt = String::new();
        for i in 0..40u64 {
            let (s, e) = (i * 3000, i * 3000 + 1500);
            srt.push_str(&format!(
                "{}\n{:02}:{:02}:{:02},000 --> {:02}:{:02}:{:02},500\nline {i}\n\n",
                i + 1,
                s / 3_600_000,
                s / 60_000 % 60,
                s / 1000 % 60,
                e / 3_600_000,
                e / 60_000 % 60,
                e / 1000 % 60
            ));
        }
        let want = crate::srt::stats(&crate::srt::parse(&srt)).expect("a measurable body");
        let c = Cache::new(1 << 20, None);
        c.put("k".into(), srt, HOUR);
        let text = c.get_shared("k").unwrap();
        for _ in 0..2 {
            let got = text.stats().expect("stats");
            assert_eq!((got.count, got.span_ms, got.max_gap_ms), (want.count, want.span_ms, want.max_gap_ms));
            assert_eq!(got.per_minute, want.per_minute);
        }
        // Tiny values (markers, counters) have nothing to measure.
        c.put("m".into(), "1".into(), HOUR);
        assert!(c.get_shared("m").unwrap().stats().is_none());
    }

    #[test]
    fn a_disk_hit_repopulates_memory_as_a_shared_body() {
        let dir = tmpdir("shared-disk");
        Cache::new(1 << 20, Some(dir.clone())).put("k".into(), "v1".into(), HOUR);
        let fresh = Cache::new(1 << 20, Some(dir));
        assert_eq!(fresh.get_shared("k").unwrap().as_str(), "v1");
        assert_eq!(fresh.get_mem("k"), Some("v1".into()), "the disk hit was not hoisted into memory");
    }

    /// A memory-only entry never touches the directory, and does not leave a stale durable one behind.
    #[test]
    fn put_mem_stays_off_disk() {
        let dir = tmpdir("put-mem");
        let c = Cache::new(1 << 20, Some(dir.clone()));
        c.put_mem("batch".into(), "b".into(), HOUR);
        assert_eq!(c.get_mem("batch"), Some("b".into()));
        assert_eq!(std::fs::read_dir(&dir).unwrap().count(), 0, "a memory-only entry reached disk");
    }

    #[test]
    fn a_large_budget_grows_disk_but_not_memory() {
        let dir = tmpdir("large-budget");
        let c = Cache::new(256 * 1024 * 1024, Some(dir));
        assert_eq!(c.inner.disk_bytes(), 256 * 1024 * 1024);
        let body = "x".repeat(1 << 20);
        for i in 0..80 {
            c.put_mem(format!("k{i}"), body.clone(), HOUR);
        }
        assert!(c.stats().bytes <= MAX_MEMORY_BYTES, "{} bytes resident", c.stats().bytes);
    }

    #[test]
    fn an_unwritable_dir_falls_back_to_memory_only() {
        let dir = tmpdir("file-not-dir");
        std::fs::write(&dir, "not a directory").unwrap();
        let c = Cache::new(1 << 20, Some(dir));
        c.put("k".into(), "v".into(), HOUR);
        assert_eq!(c.get("k"), Some("v".into()), "memory must still serve");
        assert!(!c.stats().disk);
    }
}
