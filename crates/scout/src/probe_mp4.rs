//! Port of `internal/scout/probe_mp4.go`. Work package 5.

use std::borrow::Cow;

use crate::probe::{
    append_unique, bit_depth_from_config, channel_count, channel_layout, clean_lang, dovi_profile,
    hdr_from_transfer, round_rate, video_shape, Probe,
};
use crate::probe_avi::codec_from_four_cc;

/// Go `mp4Box`: one box read from a head. `body` is what the box carries after its header.
///
/// `body` borrows the head, as Go's does: a box that holds boxes is walked again at each level, and copying the
/// bodies each time would copy a one-megabyte head from untrusted bytes a dozen times over.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct Mp4Box<'a> {
    pub typ: Cow<'a, str>,
    pub body: &'a [u8],
}

/// Go `parseMP4`: the probe read from an MP4 head, and whether the head described a parseable file.
///
/// ISO base media (MP4/MOV/M4V). Its metadata lives in `moov`, which a streaming-friendly muxer puts at
/// the FRONT — but plenty of tools leave it at the end, where a one-megabyte head can't see it. That case
/// reports nothing rather than guessing, and the caller keeps whatever the release title claimed.
pub(crate) fn parse_mp4(head: &[u8]) -> (Probe, bool) {
    if head.len() < 12 || &head[4..8] != b"ftyp" {
        return (Probe::default(), false);
    }
    let Some(moov) = find_box(head, "moov") else { return (Probe::default(), false) };
    let mut p = Probe { container: "mp4".into(), ..Default::default() };
    for trak in all_boxes(moov, "trak") {
        let Some(mdia) = find_box(trak, "mdia") else { continue };
        let kind = handler_kind(mdia);
        let lang = media_language(mdia);
        let codec = sample_codec(mdia);
        if !codec.is_empty() && kind == "vide" && p.video_codec.is_empty() {
            p.video_codec = codec.to_string();
        }
        // The RICHEST audio track wins, not the first one listed — the rule probe.go states and readTrackFacts
        // already follows. First-wins reported "5.1" for a remux that lists 5.1 before its 7.1 track, and the
        // probe overrides the correctly-titled value, so the viewer is shown a worse fact than the title had.
        if kind == "soun" {
            let n = mp4_audio_channels(mdia);
            if n > channel_count(&p.audio_channels) {
                p.audio_channels = channel_layout(n).to_string();
            }
        }
        if kind == "vide" {
            mp4_video_config(mdia, &mut p);
        }
        match kind.as_str() {
            "soun" => {
                if lang.is_empty() {
                    p.untagged_audio += 1;
                } else {
                    append_unique(&mut p.audio, &lang);
                }
            }
            "sbtl" | "subt" | "text" | "clcp" => {
                if lang.is_empty() {
                    p.untagged_subtitles += 1;
                } else {
                    append_unique(&mut p.subtitles, &lang);
                }
            }
            _ => {}
        }
    }
    if p.video_codec.is_empty()
        && p.audio.is_empty()
        && p.subtitles.is_empty()
        && p.untagged_audio == 0
        && p.untagged_subtitles == 0
    {
        return (Probe::default(), false);
    }
    (p, true)
}

/// Go `handlerKind`: the four-byte handler type of an `hdlr` box, "" when absent or short.
fn handler_kind(mdia: &[u8]) -> String {
    match find_box(mdia, "hdlr") {
        Some(hdlr) if hdlr.len() >= 12 => String::from_utf8_lossy(&hdlr[8..12]).into_owned(),
        _ => String::new(),
    }
}

/// Go `mediaLanguage`: reads mdhd's packed language: three 5-bit values, each an ISO 639-2 letter offset
/// from 0x60. "und" and the all-zero placeholder both mean nobody said.
fn media_language(mdia: &[u8]) -> String {
    let Some(mdhd) = find_box(mdia, "mdhd") else { return String::new() };
    if mdhd.len() < 24 {
        return String::new();
    }
    let version = mdhd[0];
    // v0: version+flags(4) + created(4) + modified(4) + timescale(4) + duration(4)
    let mut off = 20;
    if version == 1 {
        // v1 widens created/modified/duration to 64-bit
        off = 32;
    }
    if off + 2 > mdhd.len() {
        return String::new();
    }
    let packed = u32::from(u16::from_be_bytes([mdhd[off], mdhd[off + 1]]));
    let mut out = [0u8; 3];
    for i in 0..3 {
        out[2 - i] = ((packed >> (5 * i)) & 0x1F) as u8 + 0x60;
    }
    // Every value is 0x60 + 0..=31, so ToLower changes nothing and only a–z can pass.
    if !out.iter().all(u8::is_ascii_lowercase) {
        return String::new();
    }
    clean_lang(&out)
}

/// Go `sampleCodec`: the codec family of the first sample entry in a track's `stsd`.
fn sample_codec(mdia: &[u8]) -> &'static str {
    let Some(minf) = find_box(mdia, "minf") else { return "" };
    let Some(stbl) = find_box(minf, "stbl") else { return "" };
    let Some(stsd) = find_box(stbl, "stsd") else { return "" };
    // stsd: version+flags(4), entry count(4), then boxes whose type IS the codec.
    if stsd.len() < 16 {
        return "";
    }
    codec_from_four_cc(String::from_utf8_lossy(&stsd[12..16]).trim_end_matches(['\0', ' ']))
}

/// Go `findBox`: the payload of the first child box with this type.
fn find_box<'a>(buf: &'a [u8], want: &str) -> Option<&'a [u8]> {
    boxes(buf).into_iter().find(|b| b.typ == want).map(|b| b.body)
}

/// Go `allBoxes`: the payloads of every child box with this type.
fn all_boxes<'a>(buf: &'a [u8], want: &str) -> Vec<&'a [u8]> {
    boxes(buf).into_iter().filter(|b| b.typ == want).map(|b| b.body).collect()
}

/// Go `boxes`: the top-level boxes in a buffer. A wrong size stops the walk; it never indexes past the end.
pub(crate) fn boxes(buf: &[u8]) -> Vec<Mp4Box<'_>> {
    let mut out = Vec::new();
    let mut i = 0usize;
    while i + 8 <= buf.len() {
        let size32 = u32::from_be_bytes([buf[i], buf[i + 1], buf[i + 2], buf[i + 3]]);
        let typ = String::from_utf8_lossy(&buf[i + 4..i + 8]);
        let mut body = i + 8;
        let size: u64 = match size32 {
            // 64-bit size follows the type
            1 => {
                if body + 8 > buf.len() {
                    return out;
                }
                let wide = u64::from_be_bytes([
                    buf[body],
                    buf[body + 1],
                    buf[body + 2],
                    buf[body + 3],
                    buf[body + 4],
                    buf[body + 5],
                    buf[body + 6],
                    buf[body + 7],
                ]);
                body += 8;
                wide
            }
            // runs to the end
            0 => (buf.len() - i) as u64,
            n => u64::from(n),
        };
        // A size that overflows, or runs past the buffer, ends the walk rather than wrapping into it.
        let Some(end) = usize::try_from(size).ok().and_then(|s| i.checked_add(s)) else { return out };
        if size == 0 || end > buf.len() || body > end {
            return out;
        }
        out.push(Mp4Box { typ, body: &buf[body..end] });
        i = end;
    }
    out
}

/// Go `firstSampleEntry`: the body of the first sample entry in a track's `stsd`.
fn first_sample_entry(mdia: &[u8]) -> Option<&[u8]> {
    let minf = find_box(mdia, "minf")?;
    let stbl = find_box(minf, "stbl")?;
    let stsd = find_box(stbl, "stsd")?;
    if stsd.len() < 8 {
        return None;
    }
    // skip version/flags + entry count
    boxes(&stsd[8..]).into_iter().next().map(|b| b.body)
}

/// Go `mp4AudioChannels`: reads the channel count from the audio sample entry: 6 bytes reserved, 2
/// data-reference index, 8 version/reserved, then channel count.
fn mp4_audio_channels(mdia: &[u8]) -> i64 {
    match first_sample_entry(mdia) {
        Some(entry) if entry.len() >= 18 => i64::from(u16::from_be_bytes([entry[16], entry[17]])),
        _ => 0,
    }
}

/// Go `mp4VideoConfig`: reads the configuration boxes after the video sample entry's fixed 78-byte header:
/// avcC/hvcC for the bit depth, and dvcC/dvvC/dvwC for Dolby Vision, whose presence is the signal, and
/// its profile. The first video track's depth wins, as its codec does.
fn mp4_video_config(mdia: &[u8], p: &mut Probe) {
    let Some(entry) = first_sample_entry(mdia) else { return };
    if entry.len() <= 78 {
        return;
    }
    // The first video track's shape, as its codec is.
    let first = p.width == 0 && p.video_level == 0;
    if first {
        // The visual sample entry's fixed header: 6 bytes reserved, 2 data-reference index, 16 pre-defined and
        // reserved, then width and height.
        p.width = i32::from(u16::from_be_bytes([entry[24], entry[25]]));
        p.height = i32::from(u16::from_be_bytes([entry[26], entry[27]]));
        p.frame_rate = mp4_frame_rate(mdia);
    }
    for b in boxes(&entry[78..]) {
        match &*b.typ {
            "avcC" | "hvcC" | "av1C" => {
                let codec = match &*b.typ {
                    "avcC" => "h264",
                    "hvcC" => "hevc",
                    _ => "av1",
                };
                if p.bit_depth == 0 {
                    p.bit_depth = bit_depth_from_config(codec, b.body) as i32;
                }
                if first {
                    (p.video_profile, p.video_level, p.high_tier) = video_shape(codec, b.body);
                }
            }
            "colr"
                // An nclx colour box: its type, then colour primaries, transfer characteristics and matrix, 16 bits each.
                if first && b.body.len() >= 8 && &b.body[..4] == b"nclx" => {
                    p.hdr_format =
                        hdr_from_transfer(i64::from(u16::from_be_bytes([b.body[6], b.body[7]]))).to_string();
                }
            "dvcC" | "dvvC" | "dvwC" => {
                p.dolby_vision = true;
                if p.dv_profile == 0 {
                    p.dv_profile = dovi_profile(b.body) as i32;
                }
            }
            _ => {}
        }
    }
}

/// Go `mp4FrameRate`: frames per second over the whole track: stts's sample counts over their total duration,
/// in mdhd's timescale. The first entry alone misreads a track whose opening samples are timed differently.
/// 0 when either box is missing.
fn mp4_frame_rate(mdia: &[u8]) -> f64 {
    let Some(mdhd) = find_box(mdia, "mdhd") else { return 0.0 };
    if mdhd.len() < 24 {
        return 0.0;
    }
    // v0: version+flags(4) + created(4) + modified(4); v1 widens created/modified to 64-bit
    let off = if mdhd[0] == 1 { 20 } else { 12 };
    if off + 4 > mdhd.len() {
        return 0.0;
    }
    let timescale = u32::from_be_bytes([mdhd[off], mdhd[off + 1], mdhd[off + 2], mdhd[off + 3]]);
    let Some(minf) = find_box(mdia, "minf") else { return 0.0 };
    let Some(stbl) = find_box(minf, "stbl") else { return 0.0 };
    let Some(stts) = find_box(stbl, "stts") else { return 0.0 };
    if stts.len() < 8 || timescale == 0 {
        return 0.0;
    }
    // version+flags(4), entry count(4), then sample count and sample delta, 32 bits each, per entry.
    let count = be_u32(stts, 4) as usize;
    // Go's uint64 arithmetic wraps; so does this, rather than panicking on a hostile table.
    let mut samples: u64 = 0;
    let mut ticks: u64 = 0;
    let mut at = 8usize;
    for _ in 0..count {
        if at + 8 > stts.len() {
            break;
        }
        let n = u64::from(be_u32(stts, at));
        samples = samples.wrapping_add(n);
        ticks = ticks.wrapping_add(n.wrapping_mul(u64::from(be_u32(stts, at + 4))));
        at += 8;
    }
    if ticks == 0 {
        return 0.0;
    }
    round_rate(samples as f64 * f64::from(timescale) / ticks as f64)
}

/// The big-endian u32 at `at`. Callers have checked that the four bytes are there.
fn be_u32(buf: &[u8], at: usize) -> u32 {
    u32::from_be_bytes([buf[at], buf[at + 1], buf[at + 2], buf[at + 3]])
}
