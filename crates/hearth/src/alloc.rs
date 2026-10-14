//! The process's global allocator: the system one (glibc in the image) unless a feature picks another.
//! `mimalloc` and `jemalloc` exist to be measured (BENCHMARKS.md, "Allocators"); they are mutually exclusive.

#[cfg(all(feature = "mimalloc", feature = "jemalloc"))]
compile_error!("the `mimalloc` and `jemalloc` features are mutually exclusive");

#[cfg(feature = "mimalloc")]
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

#[cfg(feature = "jemalloc")]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

/// jemalloc tuned to give memory back: a background thread purges dirty and muzzy pages one second after they
/// are freed (the default is ten seconds, purged only by allocation activity), and the thread cache is capped
/// so idle worker threads do not hold freed bodies.
#[cfg(feature = "jemalloc")]
#[allow(non_upper_case_globals)]
#[export_name = "_rjem_malloc_conf"]
pub static malloc_conf: &[u8] =
    b"background_thread:true,dirty_decay_ms:1000,muzzy_decay_ms:1000,lg_tcache_max:16\0";
