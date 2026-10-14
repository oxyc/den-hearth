//! The memory tier under concurrency: hit, evicting put and a 90/10 mix at 1, 4 and 8 threads.
//!
//!     cargo bench -p den-scout --features bench --bench cache_mem
//!
//! Each thread walks its own offset through the key set, so threads spread over the entries instead of all
//! reading one. `bench/results/scout-cache/mem-*.jsonl` holds the mutex + LRU-map implementation this replaced.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use den_scout::cache::{Cache, MemoryCache};
use divan::{black_box, Bencher};

#[global_allocator]
static ALLOC: divan::AllocProfiler = divan::AllocProfiler::system();

fn main() {
    divan::main();
}

const HIT_KEYS: usize = 512;
const THREADS: &[usize] = &[1, 4, 8];

fn keys(n: usize) -> Vec<String> {
    (0..n).map(|i| format!("list:v1:{i:040x}")).collect()
}

fn filled(cache: &MemoryCache, keys: &[String]) {
    let value = "v".repeat(1024);
    for k in keys {
        cache.put(k, &value, Duration::from_secs(3600));
    }
}

/// A counter private to each thread, started at a different offset per thread.
fn thread_slot() -> usize {
    static NEXT: AtomicU64 = AtomicU64::new(0);
    thread_local!(static SLOT: std::cell::Cell<usize> = std::cell::Cell::new(NEXT.fetch_add(97, Ordering::Relaxed) as usize));
    SLOT.with(|s| {
        s.set(s.get().wrapping_add(1));
        s.get()
    })
}

#[divan::bench(threads = THREADS, sample_size = 256)]
fn get_hit(bencher: Bencher) {
    let cache = MemoryCache::new(4 << 20);
    let keys = keys(HIT_KEYS);
    filled(&cache, &keys);
    bencher.bench(|| black_box(cache.get(black_box(&keys[thread_slot() % HIT_KEYS]))));
}

#[divan::bench(threads = THREADS, sample_size = 256)]
fn put_evicting(bencher: Bencher) {
    let cache = MemoryCache::new(1 << 20);
    let keys = keys(1 << 16);
    let value = "v".repeat(1024);
    bencher.bench(|| {
        cache.put(&keys[thread_slot() % keys.len()], black_box(&value), Duration::from_secs(3600));
    });
}

/// Nine hits to one put on a working set that fits: the shape of a handler serving lists.
#[divan::bench(threads = THREADS, sample_size = 256)]
fn mixed_90_10(bencher: Bencher) {
    let cache = MemoryCache::new(4 << 20);
    let keys = keys(HIT_KEYS);
    filled(&cache, &keys);
    let value = "v".repeat(1024);
    bencher.bench(|| {
        let i = thread_slot();
        let k = &keys[i % HIT_KEYS];
        if i % 10 == 0 {
            cache.put(k, black_box(&value), Duration::from_secs(3600));
        }
        black_box(cache.get(k))
    });
}
