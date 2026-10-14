//! The process's global allocator: jemalloc, tuned to give memory back (BENCHMARKS.md, "Allocators").

use tikv_jemalloc_ctl::{opt, raw};

#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

/// jemalloc's compiled-in configuration, read by jemalloc itself before the first allocation. A background
/// thread purges dirty and muzzy pages one second after they are freed (the default is ten seconds, purged
/// only by allocation activity). The symbol carries the `_rjem_` prefix tikv-jemalloc-sys gives every
/// jemalloc symbol; an operator's `_RJEM_MALLOC_CONF` environment variable is read after it and overrides it.
///
/// The experiment also set `lg_tcache_max:16`; jemalloc clips that to `LG_PAGE + 3` = 15 on a 4 KiB-page
/// host, which is its default (32 KiB), so on the amd64 box it changed nothing and is not carried over.
#[allow(non_upper_case_globals)]
#[export_name = "_rjem_malloc_conf"]
pub static malloc_conf: &[u8] = b"background_thread:true,dirty_decay_ms:1000,muzzy_decay_ms:1000\0";

/// One startup line: the allocator and the settings in force, as jemalloc reports them (so an override is
/// visible, and a configuration jemalloc rejected is too).
pub fn startup_line() -> String {
    // SAFETY: the types are jemalloc's documented ones for these mallctl names (`version` is a pointer to a
    // NUL-terminated string with static lifetime; the decay times are ssize_t).
    let version = unsafe { raw::read::<*const std::os::raw::c_char>(b"version\0") }
        .ok()
        .filter(|p| !p.is_null())
        .map(|p| unsafe { std::ffi::CStr::from_ptr(p) }.to_string_lossy().into_owned())
        .unwrap_or_else(|| "?".into());
    format!(
        "allocator: jemalloc {version} background_thread={} dirty_decay_ms={} muzzy_decay_ms={}",
        show(opt::background_thread::read()),
        show(unsafe { raw::read::<isize>(b"opt.dirty_decay_ms\0") }),
        show(unsafe { raw::read::<isize>(b"opt.muzzy_decay_ms\0") }),
    )
}

fn show<T: std::fmt::Display, E>(v: Result<T, E>) -> String {
    v.map_or_else(|_| "?".into(), |v| v.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The compiled-in `malloc_conf` is what jemalloc is running with. (`_RJEM_MALLOC_CONF` in the test's
    /// environment would legitimately override it, so the test stands down then.)
    #[test]
    fn compiled_in_tuning_takes_effect() {
        if std::env::var_os("_RJEM_MALLOC_CONF").is_some() {
            return;
        }
        assert!(opt::background_thread::read().unwrap());
        // SAFETY: the types are jemalloc's documented ones for these mallctl names (ssize_t).
        assert_eq!(unsafe { raw::read::<isize>(b"opt.dirty_decay_ms\0") }.unwrap(), 1000);
        assert_eq!(unsafe { raw::read::<isize>(b"opt.muzzy_decay_ms\0") }.unwrap(), 1000);
    }

    #[test]
    fn startup_line_names_jemalloc_and_its_settings() {
        let line = startup_line();
        assert!(line.starts_with("allocator: jemalloc 5."), "{line}");
        assert!(!line.contains('?'), "{line}");
    }
}
