//! Replays a household's day of Subtitles cache traffic against the real `Cache` on a real directory and prints
//! what the disk tier did, as one JSON line.
//!
//!     cargo bench -p den-subtitles --features bench --bench cache_replay -- [--hours 24] [--titles 400] [--label x]
//!
//! Time is simulated (`bench_api::set_clock`), so a day runs in seconds. The traffic follows how `addon.rs` calls the
//! cache (the key shapes, which calls are `put` and which `put_mem`, the TTLs); the sizes and rates are one plausible
//! household, not a measurement of one. Every rate printed is per simulated hour.

use std::alloc::{GlobalAlloc, Layout, System};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};

use den_subtitles::bench_api as subs;

/// Live and peak heap bytes: the memory the cache costs, with its index and bookkeeping.
struct Counting;
static LIVE: AtomicUsize = AtomicUsize::new(0);
static PEAK: AtomicUsize = AtomicUsize::new(0);

unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, l: Layout) -> *mut u8 {
        let p = System.alloc(l);
        if !p.is_null() {
            let now = LIVE.fetch_add(l.size(), Ordering::Relaxed) + l.size();
            PEAK.fetch_max(now, Ordering::Relaxed);
        }
        p
    }
    unsafe fn dealloc(&self, p: *mut u8, l: Layout) {
        System.dealloc(p, l);
        LIVE.fetch_sub(l.size(), Ordering::Relaxed);
    }
    unsafe fn realloc(&self, p: *mut u8, l: Layout, new: usize) -> *mut u8 {
        let q = System.realloc(p, l, new);
        if !q.is_null() {
            if new >= l.size() {
                let now = LIVE.fetch_add(new - l.size(), Ordering::Relaxed) + new - l.size();
                PEAK.fetch_max(now, Ordering::Relaxed);
            } else {
                LIVE.fetch_sub(l.size() - new, Ordering::Relaxed);
            }
        }
        q
    }
}

#[global_allocator]
static ALLOC: Counting = Counting;

const DISK_BYTES: usize = 256 << 20;
const SEARCH_TTL: u64 = 6 * 3600;
const SEARCH_LIFE: u64 = SEARCH_TTL + 7 * 24 * 3600;
const CACHE_TTL: u64 = 60 * 24 * 3600;
const SYNC_RESULT_TTL: u64 = 10 * 365 * 24 * 3600;
const RETRY_TTL: u64 = 600;
const PIN_TTL: u64 = 180 * 24 * 3600;
const STATS_TTL: u64 = 7 * 24 * 3600;
const QUOTA_TTL: u64 = 2 * 24 * 3600;
const BATCH_TTL: u64 = 24 * 3600;
const BATCHES_PER_FILM: usize = 150;
const INSTALLS: usize = 3;
const LANGS: [&str; 3] = ["SV", "EN", "FI"];

struct Rng(u64);
impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 >> 12;
        self.0 ^= self.0 << 25;
        self.0 ^= self.0 >> 27;
        self.0.wrapping_mul(0x2545F4914F6CDD1D)
    }
    fn unit(&mut self) -> f64 {
        (self.next() >> 11) as f64 / (1u64 << 53) as f64
    }
    fn below(&mut self, n: usize) -> usize {
        (self.next() % n as u64) as usize
    }
}

fn padded(prefix: &str, len: usize) -> String {
    let mut s = String::with_capacity(len.max(prefix.len()));
    s.push_str(prefix);
    let line = "1\n00:01:02,000 --> 00:01:04,500\nI told you, it was never about the money.\n\n";
    while s.len() < len {
        s.push_str(line);
    }
    s.truncate(len.max(prefix.len()));
    // The loop above grows past `len` and the buffer doubles; a decoded download is not twice its size.
    s.shrink_to_fit();
    s
}

fn dir_stats(dir: &std::path::Path) -> (u64, u64) {
    let (mut files, mut bytes) = (0, 0);
    for e in std::fs::read_dir(dir).unwrap().flatten() {
        let m = e.metadata().unwrap();
        if m.is_file() {
            files += 1;
            bytes += m.len();
        }
    }
    (files, bytes)
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let arg = |name: &str| args.iter().position(|a| a == name).and_then(|i| args.get(i + 1)).cloned();
    if args.iter().any(|a| a == "--test") {
        return; // `cargo bench -- --test`: build only
    }
    let hours: u64 = arg("--hours").and_then(|v| v.parse().ok()).unwrap_or(24);
    let titles: usize = arg("--titles").and_then(|v| v.parse().ok()).unwrap_or(400);
    let label = arg("--label").unwrap_or_else(|| "run".to_owned());

    let base = SystemTime::now();
    let offset = Arc::new(AtomicU64::new(0));
    let clock: Arc<dyn Fn() -> SystemTime + Send + Sync> = {
        let offset = Arc::clone(&offset);
        Arc::new(move || base + Duration::from_secs(offset.load(Ordering::Relaxed)))
    };
    let dir = tempfile::tempdir().unwrap();
    let heap_before = LIVE.load(Ordering::Relaxed);
    PEAK.store(heap_before, Ordering::Relaxed);
    let cache = subs::replay_cache(DISK_BYTES, dir.path(), Arc::clone(&clock));
    let mut rng = Rng(0x9E3779B97F4A7C15);

    let weights: Vec<f64> = (0..titles).map(|i| 1.0 / (i as f64 + 1.0)).collect();
    let total: f64 = weights.iter().sum();
    let pick_title = |rng: &mut Rng| {
        let mut x = rng.unit() * total;
        for (i, w) in weights.iter().enumerate() {
            x -= w;
            if x <= 0.0 {
                return i;
            }
        }
        titles - 1
    };

    let (mut requests, mut lookups, mut hits) = (0u64, 0u64, 0u64);
    let mut puts = 0u64;
    // Facts a restart should still know: the key and the second it stops being valid.
    let mut durable: HashMap<String, u64> = HashMap::new();
    let mut sweep_ns = 0u128;
    let mut sweeps = 0u64;

    macro_rules! put {
        ($now:expr, $key:expr, $val:expr, $ttl:expr) => {{
            let key: String = $key;
            durable.insert(key.clone(), $now + $ttl);
            puts += 1;
            cache.put(key, $val, Duration::from_secs($ttl));
        }};
    }

    for minute in 0..hours * 60 {
        let now = minute * 60;
        offset.store(now, Ordering::Relaxed);
        let mult: f64 = match (minute / 60) % 24 {
            0..=6 => 0.1,
            7..=16 => 1.0,
            _ => 4.0,
        };
        let n_requests = {
            let mut n = 0;
            let mut p = (-mult).exp();
            let mut s = p;
            let u = rng.unit();
            while u > s && n < 50 {
                n += 1;
                p *= mult / n as f64;
                s += p;
            }
            n
        };
        for _ in 0..n_requests {
            requests += 1;
            let title = pick_title(&mut rng);
            let lang_ix = if rng.unit() < 0.6 {
                0
            } else if rng.unit() < 0.75 {
                1
            } else {
                2
            };
            let lang = LANGS[lang_ix];
            let install = rng.below(INSTALLS);
            let episodic = title % 3 != 0;
            let (s, e) = if episodic { (1 + title % 5, 1 + rng.below(10)) } else { (0, 0) };
            let tt = format!("tt{title:07}");

            // The search: fresh for six hours, kept a week past that as the fallback.
            let search_key = format!("search:{tt}:{s}:{e}:");
            lookups += 1;
            let fresh = cache
                .get(&search_key)
                .and_then(|v| v.split_once('\n').and_then(|(f, _)| f.parse::<u64>().ok()))
                .is_some_and(|fresh_until| fresh_until > now);
            if fresh {
                hits += 1;
            } else {
                let body = padded(&format!("{}\n", now + SEARCH_TTL), 3_000 + rng.below(5_000));
                put!(now, search_key, body, SEARCH_LIFE);
            }

            // The download: one candidate usually, a different one now and then.
            let file_id =
                9_000_000 + title * 40 + lang_ix * 8 + if rng.unit() < 0.85 { 0 } else { 1 + rng.below(6) };
            let os_key = format!("os:{file_id}");
            lookups += 2;
            assert!(cache.get(&format!("syncfail:dead:{file_id}")).is_none());
            if cache.get_shared(&os_key).is_some() {
                hits += 1;
            } else {
                let body = padded("", 60_000 + rng.below(70_000));
                put!(now, os_key.clone(), body, CACHE_TTL);
                let stats_key = format!("syncfail:ep-stats:{tt}:{s}:{e}");
                lookups += 1;
                if cache.get(&stats_key).is_none() {
                    put!(now, stats_key, (1400 + rng.below(300)).to_string(), STATS_TTL);
                } else {
                    hits += 1;
                }
            }

            // A sync on some requests: the aligned body, its offset, a daily allowance counter.
            if rng.unit() < 0.12 {
                let sync_key = format!("sync:{os_key}:ref{}", rng.below(2));
                lookups += 1;
                if cache.get_shared(&sync_key).is_some() {
                    hits += 1;
                } else if rng.unit() < 0.05 {
                    put!(now, format!("syncfail:{sync_key}"), "1".to_owned(), RETRY_TTL);
                } else {
                    let quota_key = format!("quota:{install}:{}", now / 86_400);
                    lookups += 1;
                    let used: u64 = cache.get(&quota_key).and_then(|v| v.parse().ok()).unwrap_or(0);
                    put!(now, quota_key, (used + 1).to_string(), QUOTA_TTL);
                    put!(now, sync_key.clone(), padded("", 60_000 + rng.below(70_000)), SYNC_RESULT_TTL);
                    put!(now, format!("{sync_key}:offset"), "-1250".to_owned(), SYNC_RESULT_TTL);
                }
            }

            // A translation on some requests: batches held in memory, two writes of the same pin, the body.
            if rng.unit() < 0.08 {
                let tkey = format!("translate:{tt}:{s}:{e}:{lang}:openai:gpt");
                lookups += 1;
                if cache.get_shared(&tkey).is_some() {
                    hits += 1;
                } else if rng.unit() < 0.03 {
                    put!(now, format!("syncfail:translate:{tkey}"), "1".to_owned(), RETRY_TTL);
                } else {
                    let pin = format!("source:{tt}:{s}:{e}");
                    put!(now, pin.clone(), file_id.to_string(), PIN_TTL);
                    for b in 0..BATCHES_PER_FILM {
                        cache.put_mem(
                            format!("batch:{tkey}:{b}"),
                            padded("", 1_500 + rng.below(1_000)),
                            Duration::from_secs(BATCH_TTL),
                        );
                    }
                    let quota_key = format!("quota:{install}:{}", now / 86_400);
                    lookups += 1;
                    let used: u64 = cache.get(&quota_key).and_then(|v| v.parse().ok()).unwrap_or(0);
                    put!(now, quota_key, (used + 1).to_string(), QUOTA_TTL);
                    put!(now, tkey, padded("", 60_000 + rng.below(70_000)), CACHE_TTL);
                    put!(now, pin, file_id.to_string(), PIN_TTL); // refreshed after the translation: same value
                }
            }
        }
        if minute % 60 == 59 {
            let t = Instant::now();
            cache.sweep();
            sweep_ns += t.elapsed().as_nanos();
            sweeps += 1;
        }
    }
    let stats = cache.stats();
    let (files, bytes) = dir_stats(dir.path());
    let heap_end = LIVE.load(Ordering::Relaxed).saturating_sub(heap_before);
    let heap_peak = PEAK.load(Ordering::Relaxed).saturating_sub(heap_before);
    let (writes, bytes_written, skipped) = subs::disk_writes(&cache);
    drop(cache);

    // A restart: a new process over the same directory.
    let end = hours * 3600;
    offset.store(end, Ordering::Relaxed);
    let started = Instant::now();
    let restarted = subs::replay_cache(DISK_BYTES, dir.path(), Arc::clone(&clock));
    let startup_ms = started.elapsed().as_secs_f64() * 1e3;
    let (mut expected, mut restored) = (0u64, 0u64);
    for (key, valid_until) in &durable {
        if *valid_until > end {
            expected += 1;
            restored += restarted.get(key).is_some() as u64;
        }
    }

    let h = hours as f64;
    let r1 = |x: f64| (x * 10.0).round() / 10.0;
    let line = serde_json::json!({
        "suite": "subs-cache-replay", "label": label, "hours": hours, "titles": titles,
        "requests": requests, "puts": puts,
        "hit_rate": (hits as f64 / lookups as f64 * 1000.0).round() / 1000.0,
        "writes_per_hour": r1(writes as f64 / h), "bytes_written_per_hour": r1(bytes_written as f64 / h),
        "skipped_unchanged": skipped,
        "disk_files_end": files, "disk_bytes_end": bytes,
        "mem_entries_end": stats.entries, "mem_bytes_end": stats.bytes,
        "heap_bytes_end": heap_end, "heap_bytes_peak": heap_peak,
        "sweep_ms_avg": r1(sweep_ns as f64 / sweeps.max(1) as f64 / 1e6),
        "restart_startup_ms": r1(startup_ms),
        "restart_durable_expected": expected, "restart_durable_restored": restored,
    })
    .to_string();
    println!("{line}");
    if let Some(out) = arg("--out") {
        use std::io::Write;
        if let Some(parent) = std::path::Path::new(&out).parent() {
            std::fs::create_dir_all(parent).unwrap();
        }
        let mut f = std::fs::OpenOptions::new().create(true).append(true).open(out).unwrap();
        writeln!(f, "{line}").unwrap();
    }
}
