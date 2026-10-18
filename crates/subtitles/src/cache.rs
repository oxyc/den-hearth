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
            clock,
            ..Options::new(max_bytes.min(MAX_MEMORY_BYTES))
        };
        // Opening the disk tier walks the directory once, building the index and reclaiming what expired.
        Cache { inner: den_cache::Cache::new(options) }
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
    use std::sync::atomic::{AtomicU64, Ordering};

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

    /// Keep the engine from enforcing the final disk budget until an adapter-rank fixture is complete.
    fn unbudgeted(mem: usize, dir: &std::path::Path) -> Cache {
        let c = Cache::new(mem, Some(dir.to_path_buf()));
        c.inner.set_disk_bytes(0);
        c
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
}
