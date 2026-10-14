//! Port of `internal/scout/probe_mp4.go`.

use den_media::codec;
use den_media::mp4::{
    self, audio_channels, boxes, child, first_sample_entry, handler_type, media_header, nclx, path, u32_at,
    VISUAL_ENTRY_HEADER,
};

use crate::probe::{
    append_unique, bit_depth_from_config, channel_count, channel_layout, clean_lang, dovi_profile,
    hdr_from_transfer, round_rate, video_shape, Probe,
};
use crate::probe_avi::codec_from_four_cc;

/// Go `parseMP4`: the probe read from an MP4 head, and whether the head described a parseable file.
///
/// ISO base media (MP4/MOV/M4V). Its metadata lives in `moov`, which a streaming-friendly muxer puts at
/// the FRONT — but plenty of tools leave it at the end, where a one-megabyte head can't see it. That case
/// reports nothing rather than guessing, and the caller keeps whatever the release title claimed.
pub(crate) fn parse_mp4(head: &[u8]) -> (Probe, bool) {
    if head.len() < 12 || &head[4..8] != b"ftyp" {
        return (Probe::default(), false);
    }
    let Some(moov) = child(head, b"moov") else { return (Probe::default(), false) };
    let mut p = Probe { container: "mp4".into(), ..Default::default() };
    for (_, trak) in boxes(moov).filter(|(typ, _)| typ == b"trak") {
        let Some(mdia) = child(trak, b"mdia") else { continue };
        let kind = handler_type(mdia).unwrap_or_default();
        let lang = media_language(mdia);
        let codec = sample_codec(mdia);
        if !codec.is_empty() && &kind == b"vide" && p.video_codec.is_empty() {
            p.video_codec = codec.to_string();
        }
        // The RICHEST audio track wins, not the first one listed — the rule probe.go states and readTrackFacts
        // already follows. First-wins reported "5.1" for a remux that lists 5.1 before its 7.1 track, and the
        // probe overrides the correctly-titled value, so the viewer is shown a worse fact than the title had.
        if &kind == b"soun" {
            let n = mp4_audio_channels(mdia);
            if n > channel_count(&p.audio_channels) {
                p.audio_channels = channel_layout(n).to_string();
            }
        }
        if &kind == b"vide" {
            mp4_video_config(mdia, &mut p);
        }
        match &kind {
            b"soun" => {
                if lang.is_empty() {
                    p.untagged_audio += 1;
                } else {
                    append_unique(&mut p.audio, &lang);
                }
            }
            b"sbtl" | b"subt" | b"text" | b"clcp" => {
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

/// Go `mediaLanguage`: reads mdhd's packed language. "und" and the all-zero placeholder both mean nobody said.
fn media_language(mdia: &[u8]) -> String {
    let Some(mdhd) = child(mdia, b"mdhd").filter(|mdhd| mdhd.len() >= 24) else { return String::new() };
    match media_header(mdhd).and_then(|h| h.language) {
        Some(letters) => clean_lang(&letters),
        None => String::new(),
    }
}

/// Go `sampleCodec`: the codec family of the first sample entry in a track's `stsd`.
fn sample_codec(mdia: &[u8]) -> &'static str {
    let Some(stsd) = path(mdia, &[b"minf", b"stbl", b"stsd"]) else { return "" };
    // stsd: version+flags(4), entry count(4), then boxes whose type IS the codec.
    if stsd.len() < 16 {
        return "";
    }
    codec_from_four_cc(String::from_utf8_lossy(&stsd[12..16]).trim_end_matches(['\0', ' ']))
}

/// Go `firstSampleEntry`: the body of the first sample entry in a track's `stsd`.
fn first_entry(mdia: &[u8]) -> Option<&[u8]> {
    let stbl = path(mdia, &[b"minf", b"stbl"])?;
    first_sample_entry(stbl).map(|(_, body)| body)
}

/// Go `mp4AudioChannels`: reads the channel count from the audio sample entry.
fn mp4_audio_channels(mdia: &[u8]) -> i64 {
    first_entry(mdia).and_then(audio_channels).map_or(0, i64::from)
}

/// Go `mp4VideoConfig`: reads the configuration boxes after the video sample entry's fixed 78-byte header:
/// avcC/hvcC for the bit depth, and dvcC/dvvC/dvwC for Dolby Vision, whose presence is the signal, and
/// its profile. The first video track's depth wins, as its codec does.
fn mp4_video_config(mdia: &[u8], p: &mut Probe) {
    let Some(entry) = first_entry(mdia) else { return };
    if entry.len() <= VISUAL_ENTRY_HEADER {
        return;
    }
    // The first video track's shape, as its codec is.
    let first = p.width == 0 && p.video_level == 0;
    if first {
        // The visual sample entry's fixed header: 6 bytes reserved, 2 data-reference index, 16 pre-defined and
        // reserved, then width and height.
        p.width = i32::from(mp4::u16_at(entry, 24).unwrap_or(0));
        p.height = i32::from(mp4::u16_at(entry, 26).unwrap_or(0));
        p.frame_rate = mp4_frame_rate(mdia);
    }
    for (typ, body) in boxes(&entry[VISUAL_ENTRY_HEADER..]) {
        match &typ {
            b"avcC" | b"hvcC" | b"av1C" => {
                let family = match &typ {
                    b"avcC" => "h264",
                    b"hvcC" => "hevc",
                    _ => "av1",
                };
                if p.bit_depth == 0 {
                    p.bit_depth = bit_depth_from_config(family, body) as i32;
                }
                if first {
                    (p.video_profile, p.video_level, p.high_tier) = video_shape(family, body);
                }
            }
            // An nclx colour box: its type, then colour primaries, transfer characteristics and matrix, 16 bits each.
            b"colr" if first && body.len() >= 8 => {
                if let Some(colour) = nclx(body) {
                    p.hdr_format = hdr_from_transfer(i64::from(colour.transfer)).to_string();
                }
            }
            typ if codec::DOVI_CONFIG_TYPES.contains(typ) => {
                p.dolby_vision = true;
                if p.dv_profile == 0 {
                    p.dv_profile = dovi_profile(body) as i32;
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
    let Some(mdhd) = child(mdia, b"mdhd").filter(|mdhd| mdhd.len() >= 24) else { return 0.0 };
    let Some(header) = media_header(mdhd) else { return 0.0 };
    let Some(stts) = path(mdia, &[b"minf", b"stbl", b"stts"]) else { return 0.0 };
    if stts.len() < 8 || header.timescale == 0 {
        return 0.0;
    }
    // version+flags(4), entry count(4), then sample count and sample delta, 32 bits each, per entry.
    let count = u32_at(stts, 4).unwrap_or(0) as usize;
    // Go's uint64 arithmetic wraps; so does this, rather than panicking on a hostile table.
    let mut samples: u64 = 0;
    let mut ticks: u64 = 0;
    let mut at = 8usize;
    for _ in 0..count {
        let (Some(n), Some(delta)) = (u32_at(stts, at), u32_at(stts, at + 4)) else { break };
        let n = u64::from(n);
        samples = samples.wrapping_add(n);
        ticks = ticks.wrapping_add(n.wrapping_mul(u64::from(delta)));
        at += 8;
    }
    if ticks == 0 {
        return 0.0;
    }
    round_rate(samples as f64 * f64::from(header.timescale) / ticks as f64)
}
