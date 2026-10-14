//! Re-exports for `benches/remux.rs`: the hot paths live in private modules. Compiled only with
//! `--features bench`; nothing here is reachable from a normal build, and each name is the production item.

pub use crate::auth::{url_sig, url_sig_ok};
pub use crate::playlist::{
    bandwidth, master, media, segments, segments_for_start, subtitle_segments, Audio, Segment,
};
pub use crate::segments::longest_shared;
pub use crate::subs::{for_hls, parse_cues};

/// The probe over a file held in memory, reading only `head` up front as a session does: whether it probed.
pub async fn probe_file(file: &[u8], head: usize) -> bool {
    let source = crate::probe::Source::Mem(file);
    crate::probe::probe(&source, &file[..head.min(file.len())]).await.is_ok()
}
