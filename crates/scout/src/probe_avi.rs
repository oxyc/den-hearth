//! Port of `internal/scout/probe_avi.go`.

use crate::probe::{channel_count, channel_layout, Probe};

/// Go `maxAVIDepth`: how many LIST chunks the walk nests before it stops.
///
/// A real AVI reaches depth 3 (RIFF > hdrl > strl); anything past this is a file built to make the parser
/// recurse, not to be played.
pub(crate) const MAX_AVI_DEPTH: usize = 16;

/// Go `parseAVI`: the probe read from an AVI head, and whether the walk found a stream header at all.
///
/// AVI carries no language metadata at all — the format predates the idea — so this reads only what it
/// does know: the codec. That still matters, because the fourcc is how an XviD rip announces itself, and
/// those decode in software on an Apple TV: no hardware decoder, no native transport, no scrubber.
pub(crate) fn parse_avi(head: &[u8]) -> (Probe, bool) {
    if head.len() < 12 || &head[0..4] != b"RIFF" || &head[8..12] != b"AVI " {
        return (Probe::default(), false);
    }
    let mut p = Probe { container: "avi".into(), ..Default::default() };
    let mut last_kind = [0u8; 4];
    walk(head, 12, head.len(), 0, &mut p, &mut last_kind);
    if p.video_codec.is_empty() && p.untagged_audio == 0 {
        return (Probe::default(), false);
    }
    (p, true)
}

/// Go `parseAVI`'s inner `walk`. A nested LIST costs 12 bytes, so a crafted megabyte head buys roughly 87,000
/// stack frames of recursion — the one quantity in the three container parsers that the input controls
/// without a ceiling. It terminates and corrupts nothing, but it is stack a remote server asked for, inside
/// a 256 MiB container, on the path where its bytes are least trustworthy. A real file nests
/// RIFF > hdrl > strl and stops; the bound is loose enough that no honest muxer can reach it.
fn walk(buf: &[u8], start: usize, end: usize, depth: usize, p: &mut Probe, last_kind: &mut [u8; 4]) {
    if depth > MAX_AVI_DEPTH {
        return;
    }
    let limit = end.min(buf.len());
    let mut i = start;
    while i.checked_add(8).is_some_and(|header_end| header_end <= limit) {
        let id = [buf[i], buf[i + 1], buf[i + 2], buf[i + 3]];
        let size = u32::from_le_bytes([buf[i + 4], buf[i + 5], buf[i + 6], buf[i + 7]]) as usize;
        let body = i + 8;
        match &id {
            b"LIST" | b"RIFF" => {
                let stop = body.saturating_add(4).saturating_add(size).min(buf.len());
                walk(buf, body + 4, stop, depth + 1, p, last_kind);
            }
            // Follows the strh it belongs to; for audio it is a WAVEFORMATEX whose second field is the
            // channel count.
            // Richest wins, as everywhere else — not the first stream listed.
            b"strf" if *last_kind == *b"auds" && body + 4 <= buf.len() => {
                let n = i64::from(u16::from_le_bytes([buf[body + 2], buf[body + 3]]));
                if n > channel_count(&p.audio_channels) {
                    p.audio_channels = channel_layout(n).to_string();
                }
            }
            b"strh" if body + 8 <= buf.len() => {
                let kind = [buf[body], buf[body + 1], buf[body + 2], buf[body + 3]];
                let fourcc = String::from_utf8_lossy(&buf[body + 4..body + 8]).into_owned();
                let fourcc = fourcc.trim_end_matches(['\0', ' ']);
                match &kind {
                    b"vids" if p.video_codec.is_empty() => {
                        p.video_codec = codec_from_four_cc(fourcc).to_string();
                    }
                    // No language to read, but the track exists and the viewer should know it does.
                    b"auds" => p.untagged_audio += 1,
                    _ => {}
                }
                *last_kind = kind;
            }
            _ => {}
        }
        i = body.saturating_add(size);
        if size % 2 == 1 {
            // RIFF chunks are word-aligned
            i = i.saturating_add(1);
        }
    }
}

/// Go `codecFromFourCC`: maps the container's own label to the families Den ranks on. Unknown labels stay
/// empty rather than guessing — an unrecognised fourcc is not evidence of anything. Shared with the MP4
/// parser, which reads the same FourCCs from its sample entries.
pub(crate) fn codec_from_four_cc(cc: &str) -> &'static str {
    match str::to_lowercase(cc).as_str() {
        "xvid" | "divx" | "dx50" | "divx5" | "mp4v" | "fmp4" | "mp43" => "mpeg4",
        // dva1/dvav and dvh1/dvhe are the MP4 sample entries of Dolby Vision over AVC and HEVC. Profile 5 has
        // no other tag, so without them a DV profile 5 MP4 reported no codec at all.
        "h264" | "avc1" | "x264" | "davc" | "dva1" | "dvav" => "h264",
        "hevc" | "hvc1" | "hev1" | "x265" | "dvh1" | "dvhe" => "hevc",
        "av01" => "av1",
        "vp90" | "vp09" => "vp9",
        _ => "",
    }
}
