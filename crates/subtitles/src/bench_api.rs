//! Re-exports for `benches/subtitles.rs`: the hot paths live in private modules. Compiled only with
//! `--features bench`; nothing here is reachable from a normal build, and each name is the production item.

pub use crate::cache::Cache;
pub use crate::fetch::subtitle_text;
pub use crate::httputil::{srt as srt_response, srt_shared as srt_response_shared};
pub use crate::opensubtitles::{rank, Subtitle};
pub use crate::srt::{has_a_cue, looks_incomplete, parse, serialize, serialize_vtt, stats, Cue};

pub fn etag_of(bytes: &[u8]) -> String {
    crate::httputil::etag_of(bytes)
}

pub fn manifest() -> serde_json::Value {
    crate::addon::manifest(None)
}
