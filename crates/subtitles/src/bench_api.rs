//! Re-exports for `benches/subtitles.rs`: the hot paths live in private modules. Compiled only with
//! `--features bench`; nothing here is reachable from a normal build, and each name is the production item.

pub use crate::cache::Cache;
pub use crate::fetch::subtitle_text;
pub use crate::httputil::{srt as srt_response, srt_shared as srt_response_shared};
pub use crate::opensubtitles::{rank, Subtitle};
pub use crate::srt::{has_a_cue, looks_incomplete, parse, serialize, serialize_vtt, stats, Cue};

/// The production cache over `dir`, reading time from `clock` so a replay runs a day of traffic in seconds.
pub fn replay_cache(
    max_bytes: usize,
    dir: &std::path::Path,
    clock: std::sync::Arc<dyn Fn() -> std::time::SystemTime + Send + Sync>,
) -> Cache {
    Cache::new_with_clock(max_bytes, Some(dir.to_path_buf()), clock)
}

/// What the disk tier was asked to write since the cache was built: (files, bytes) and how many puts it skipped
/// because the file already held the value.
pub fn disk_writes(cache: &Cache) -> (u64, u64, u64) {
    let n = cache.disk_counts();
    (n.writes, n.bytes_written, n.skipped)
}

pub fn etag_of(bytes: &[u8]) -> String {
    crate::httputil::etag_of(bytes)
}

pub fn manifest() -> serde_json::Value {
    crate::addon::manifest(None)
}
