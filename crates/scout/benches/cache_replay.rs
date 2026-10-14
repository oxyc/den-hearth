//! Replays a household's day of Scout cache traffic against the real `TieredCache` on a real directory and
//! prints what the disk tier did, as one JSON line.
//!
//!     cargo bench -p den-scout --features bench --bench cache_replay -- [--hours 24] [--label baseline]
//!
//! Time is simulated (the cache reads an injected clock), so a day runs in seconds. The traffic is built from
//! how the handler and stores call the cache (see the comments on each stream below); the sizes and rates are
//! one plausible household, not a measurement of one, and every number printed is per simulated hour.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};

use den_scout::bench_api;
use den_scout::cache::{Cache, CacheClass};
use den_scout::diskcache::DiskCounts;

const MEMORY_BYTES: usize = 64 << 20;
const LIST_TTL: u64 = 300;
/// 24 h tickets minus 3 x list TTL and a viewing margin: how long a complete list is kept past freshness.
const LIST_HOLD: u64 = LIST_TTL + 6 * 3600;
const PROBE_TTL: u64 = 30 * 24 * 3600;
const RESOLVE_TTL: u64 = 6 * 3600;
const TITLES: usize = 120;
const CONFIGS: usize = 3;
const RELEASES_PER_TITLE: usize = 40;
const PROBED_PER_BUILD: usize = 8;

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
    while s.len() < len {
        s.push_str("{\"name\":\"Torrentio\\n1080p\",\"title\":\"Some.Release.2160p.WEB-DL.DDP5.1.x265-GRP\",\"url\":\"https://x/p/T\"},");
    }
    s.truncate(len.max(prefix.len()));
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
    let label = arg("--label").unwrap_or_else(|| "run".to_owned());

    let base = SystemTime::now();
    let offset = Arc::new(AtomicU64::new(0));
    let clock: Arc<dyn Fn() -> SystemTime + Send + Sync> = {
        let offset = Arc::clone(&offset);
        Arc::new(move || base + Duration::from_secs(offset.load(Ordering::Relaxed)))
    };
    let dir = tempfile::tempdir().unwrap();
    let cache = bench_api::replay_cache(MEMORY_BYTES, dir.path(), Arc::clone(&clock));
    let mut rng = Rng(0x9E3779B97F4A7C15);

    // Title popularity: Zipf, so a few titles are rebuilt every five minutes and most almost never.
    let weights: Vec<f64> = (0..TITLES).map(|i| 1.0 / (i as f64 + 1.0)).collect();
    let total: f64 = weights.iter().sum();
    let pick_title = |rng: &mut Rng| {
        let mut x = rng.unit() * total;
        for (i, w) in weights.iter().enumerate() {
            x -= w;
            if x <= 0.0 {
                return i;
            }
        }
        TITLES - 1
    };

    let (mut requests, mut builds, mut plays, mut get_hits, mut gets) = (0u64, 0u64, 0u64, 0u64, 0u64);
    // Durable facts a restart should still know: the keys and the second they stop being valid.
    let mut durable: std::collections::HashMap<String, u64> = Default::default();
    let mut release_epoch = [0u32; TITLES];

    for minute in 0..hours * 60 {
        let now = minute * 60;
        offset.store(now, Ordering::Relaxed);
        let hour_of_day = (minute / 60) % 24;
        // Quiet overnight, busy in the evening: one list request a minute at the base rate.
        let mult = match hour_of_day {
            0..=6 => 0.1,
            7..=16 => 1.0,
            _ => 4.0,
        };
        let n_requests = {
            let lambda: f64 = mult;
            let mut n = 0;
            let mut p = (-lambda).exp();
            let mut s = p;
            let u = rng.unit();
            while u > s && n < 50 {
                n += 1;
                p *= lambda / n as f64;
                s += p;
            }
            n
        };
        for _ in 0..n_requests {
            requests += 1;
            let title = pick_title(&mut rng);
            let cfg = rng.below(CONFIGS);
            let list_key = format!("list:v1:cfg{cfg}:tt{title:07}");
            gets += 1;
            let fresh = cache
                .get(&list_key)
                .and_then(|v| v.split_once('\n').and_then(|(f, _)| f.parse::<u64>().ok()))
                .is_some_and(|fresh_until| fresh_until > now);
            if fresh {
                get_hits += 1;
            } else {
                builds += 1;
                // The indexer answer (keep_answer) and cinemeta, both plain puts.
                let answer_key = format!("answer:v1:tt{title:07}:cfg{cfg}");
                cache.put(
                    &answer_key,
                    &padded("[", 30_000 + rng.below(20_000)),
                    Duration::from_secs(LIST_TTL),
                );
                let meta_key = format!("cinemeta:v1:tt{title:07}");
                gets += 1;
                if cache.get(&meta_key).is_none() {
                    if rng.unit() < 0.1 {
                        cache.put(&meta_key, "", Duration::from_secs(600)); // a miss, remembered ten minutes
                    } else {
                        cache.put(
                            &meta_key,
                            &padded("{\"meta\":", 3_000),
                            Duration::from_secs(7 * 24 * 3600),
                        );
                        durable.insert(meta_key, now + 7 * 24 * 3600);
                    }
                }
                // Probe facts for the top releases: a miss is a probe, a hit costs nothing.
                for _ in 0..PROBED_PER_BUILD {
                    let release = rng.below(RELEASES_PER_TITLE);
                    let probe_key = format!("probe:v1:tt{title:07}:{release:02}:e{}", release_epoch[title]);
                    gets += 1;
                    if cache.get(&probe_key).is_none() {
                        cache.put(
                            &probe_key,
                            &padded("{\"audio\":[\"eng\"]", 700),
                            Duration::from_secs(PROBE_TTL),
                        );
                        durable.insert(probe_key, now + PROBE_TTL);
                    }
                }
                // The finished list: a new freshness stamp in every build, so no two values are equal.
                let body = padded(&format!("{}\n", now + LIST_TTL), 18_000 + rng.below(14_000));
                cache.put(&list_key, &body, Duration::from_secs(LIST_HOLD));
            }
            // A play on some requests: the store remembers what it resolved, six hours.
            if rng.unit() < 0.08 {
                plays += 1;
                let hash = format!("{title:07}{:02}", rng.below(RELEASES_PER_TITLE));
                let id = (1000 + title).to_string();
                let resolve = padded("{\"id\":", 1_200);
                for _ in 0..2 {
                    // Both flows write the same id and entry; the second write is the repeat.
                    cache.put(&format!("tb:id:acct:{hash}"), &id, Duration::from_secs(RESOLVE_TTL));
                    cache.put(&format!("tb:resolve:acct:{hash}"), &resolve, Duration::from_secs(RESOLVE_TTL));
                }
                // clear_torbox_status: tombstones, an empty value that expires at once.
                for k in ["status", "queued", "cached"] {
                    cache.put(&format!("tb:{k}:acct:{hash}"), "", Duration::from_nanos(1));
                }
                // Ninety minutes of status polling on a play in five (every 5 s, plus summary and listing).
                if rng.unit() < 0.2 {
                    for poll in 0..(90 * 12) {
                        offset.store(now + poll * 5 / 12, Ordering::Relaxed); // stays inside this minute
                        let c = CacheClass::Volatile;
                        cache.put_class(
                            &format!("tb:status:acct:{hash}"),
                            "downloading 41%",
                            Duration::from_secs(5),
                            c,
                        );
                        if poll % 6 == 0 {
                            cache.put_class(
                                "tb:summary:acct",
                                "{\"plan\":\"pro\"}",
                                Duration::from_secs(30),
                                c,
                            );
                            cache.put_class("tb:listing:acct", "[1,2,3]", Duration::from_secs(15), c);
                        }
                        gets += 1;
                        if cache.get(&format!("tb:status:acct:{hash}")).is_some() {
                            get_hits += 1;
                        }
                    }
                    offset.store(now, Ordering::Relaxed);
                }
            }
        }
        if minute % 60 == 59 {
            bench_api::replay_hourly(&cache);
        }
        // A trickle of new releases changes what a title's top results are.
        if minute % 240 == 0 {
            release_epoch[rng.below(TITLES)] += 1;
        }
    }
    let counts = cache.disk_counts();
    let (files, bytes) = dir_stats(dir.path());
    drop(cache);

    // A restart: a new process over the same directory. How long it takes to be ready, what it reads, and how
    // much of what was learned it still knows.
    let end = hours * 60 * 60;
    offset.store(end, Ordering::Relaxed);
    let started = Instant::now();
    let restarted = bench_api::replay_cache(MEMORY_BYTES, dir.path(), Arc::clone(&clock));
    bench_api::replay_hourly(&restarted); // hearth sweeps once at startup
    let startup_ms = started.elapsed().as_secs_f64() * 1e3;
    let (mut expected, mut restored) = (0u64, 0u64);
    let after_start = restarted.disk_counts();
    for (key, valid_until) in &durable {
        if *valid_until > end {
            expected += 1;
            restored += restarted.get(key).is_some() as u64;
        }
    }
    let after_warm = restarted.disk_counts();

    let h = hours as f64;
    let per_hour = |n: u64| (n as f64 / h * 10.0).round() / 10.0;
    let DiskCounts {
        writes,
        bytes_written,
        fsyncs,
        reads,
        bytes_read,
        unlinks,
        readdirs,
        stat_calls,
        skipped,
    } = counts;
    let line = serde_json::json!({
            "suite": "scout-cache-replay", "label": label, "hours": hours,
            "requests": requests, "builds": builds, "plays": plays,
            "memory_hit_rate": (get_hits as f64 / gets as f64 * 1000.0).round() / 1000.0,
            "writes_per_hour": per_hour(writes), "bytes_written_per_hour": per_hour(bytes_written),
            "fsyncs_per_hour": per_hour(fsyncs), "reads_per_hour": per_hour(reads),
            "bytes_read_per_hour": per_hour(bytes_read), "unlinks_per_hour": per_hour(unlinks),
            "readdirs_per_hour": per_hour(readdirs), "stats_per_hour": per_hour(stat_calls),
            "skipped_unchanged": skipped,
            "disk_files_end": files, "disk_bytes_end": bytes,
            "restart_startup_ms": (startup_ms * 10.0).round() / 10.0,
            "restart_startup_reads": after_start.reads, "restart_startup_bytes_read": after_start.bytes_read,
            "restart_durable_expected": expected, "restart_durable_restored": restored,
            "restart_warm_disk_reads": after_warm.reads - after_start.reads,
    })
    .to_string();
    println!("{line}");
    if let Some(out) = arg("--out") {
        use std::io::Write;
        let mut f = std::fs::OpenOptions::new().create(true).append(true).open(out).unwrap();
        writeln!(f, "{line}").unwrap();
    }
}
