//! Go: `internal/scout/probe_mp4_test.go` (368 lines). Work package 5.
//!
//! Reads the sample files in Go's `internal/scout/testdata/`; see PORTING.md for where they go.

use crate::probe::{channel_count, channel_layout};
use crate::probe_avi::codec_from_four_cc;
use crate::probe_mp4::{boxes, parse_mp4};

use super::probe_containers::testdata;

// The MP4 side of the probe is a byte parser over untrusted input: the head of a file served by a debrid
// link, which may be truncated, may be a different container wearing an .mp4 name, or may be hostile.
// Building the boxes here rather than shipping binary fixtures keeps each case's INTENT readable — the
// difference between a v0 and a v1 mdhd is one visible byte, not a hexdump.

// mp4box assembles size+type+payload. Sizes are 32-bit, which is what every box below needs.
fn mp4box(typ: &str, payload: &[u8]) -> Vec<u8> {
    let mut out = ((8 + payload.len()) as u32).to_be_bytes().to_vec();
    out.extend_from_slice(typ.as_bytes());
    out.extend_from_slice(payload);
    out
}

// Concatenates boxes or payload parts.
fn concat(parts: &[Vec<u8>]) -> Vec<u8> {
    parts.concat()
}

// hdlr — `handlerKind` reads the four bytes at payload offset 8.
fn hdlr(kind: &str) -> Vec<u8> {
    let mut p = vec![0u8; 12];
    p[8..12].copy_from_slice(kind.as_bytes());
    mp4box("hdlr", &p)
}

// mdhd v0 — the packed language sits at payload offset 20; v1 widens the timestamps and moves it to 32.
fn mdhd(lang: &str, version: u8) -> Vec<u8> {
    let off = if version == 1 { 32 } else { 20 };
    let mut p = vec![0u8; off + 4];
    p[0] = version;
    if !lang.is_empty() {
        let mut packed: u16 = 0;
        for (i, b) in lang.as_bytes().iter().take(3).enumerate() {
            packed |= u16::from(b - 0x60) << (5 * (2 - i as u32));
        }
        p[off..off + 2].copy_from_slice(&packed.to_be_bytes());
    }
    mp4box("mdhd", &p)
}

// stsd wraps sample entries behind a version/flags word and an entry count.
fn stsd(entries: &[Vec<u8>]) -> Vec<u8> {
    let mut head = vec![0u8; 8]; // version+flags, entry count
    head[4..8].copy_from_slice(&(entries.len() as u32).to_be_bytes());
    mp4box("stsd", &[head, concat(entries)].concat())
}

// An audio sample entry: 16 bytes of reserved/version fields, then the channel count.
fn audio_entry(four_cc: &str, channels: u16) -> Vec<u8> {
    let mut p = vec![0u8; 18];
    p[16..18].copy_from_slice(&channels.to_be_bytes());
    mp4box(four_cc, &p)
}

// A video sample entry. `extra` boxes live past the fixed 78-byte visual header — where dvcC/dvvC go.
fn video_entry(four_cc: &str, extra: &[Vec<u8>]) -> Vec<u8> {
    mp4box(four_cc, &[vec![0u8; 78], concat(extra)].concat())
}

fn trak(kind: &str, lang: &str, version: u8, entries: &[Vec<u8>]) -> Vec<u8> {
    let mdia = mp4box(
        "mdia",
        &concat(&[hdlr(kind), mdhd(lang, version), mp4box("minf", &mp4box("stbl", &stsd(entries)))]),
    );
    mp4box("trak", &mdia)
}

fn mp4_file(traks: &[Vec<u8>]) -> Vec<u8> {
    [mp4box("ftyp", &[0u8; 8]), mp4box("moov", &concat(traks))].concat()
}

#[test]
fn parse_mp4_reads_tracks_languages_and_codec() {
    let head = mp4_file(&[
        trak("vide", "und", 0, &[video_entry("hvc1", &[])]),
        trak("soun", "eng", 0, &[audio_entry("mp4a", 6)]),
        trak("soun", "swe", 0, &[audio_entry("mp4a", 2)]),
        trak("subt", "fin", 0, &[]),
    ]);
    let (p, ok) = parse_mp4(&head);
    assert!(ok, "a well-formed file must parse");
    assert_eq!(p.container, "mp4", "container = {:?}", p.container);
    assert_eq!(p.video_codec, "hevc", "videoCodec = {:?}, want hevc", p.video_codec);
    // mdhd stores ISO 639-2 ("eng"); the probe normalises to 639-1 so one vocabulary reaches Den,
    // whatever the container spoke.
    assert!(p.audio.len() == 2 && p.audio[0] == "en" && p.audio[1] == "sv", "audio = {:?}, want [en sv]", p.audio);
    assert!(p.subtitles.len() == 1 && p.subtitles[0] == "fi", "subtitles = {:?}, want [fi]", p.subtitles);
    // The channel count comes from the FIRST audio track's sample entry.
    assert_eq!(p.audio_channels, "5.1", "audioChannels = {:?}, want 5.1", p.audio_channels);
    // A video track tagged `und` is not a language claim, and must not appear as one.
    assert_eq!(p.untagged_audio, 0, "untaggedAudio = {}, want 0", p.untagged_audio);
}

// An untagged audio track is COUNTED, never guessed at. Den treats untagged as "probably the original
// language" downstream; inventing "eng" here would announce English audio for a Swedish show.
#[test]
fn parse_mp4_untagged_tracks_are_counted_not_named() {
    let head = mp4_file(&[
        trak("soun", "", 0, &[audio_entry("mp4a", 2)]),
        trak("soun", "und", 0, &[audio_entry("mp4a", 2)]),
        trak("sbtl", "", 0, &[]),
    ]);
    let (p, ok) = parse_mp4(&head);
    assert!(ok, "parse");
    assert!(p.audio.is_empty(), "audio = {:?}, want none named", p.audio);
    assert_eq!(p.untagged_audio, 2, "untaggedAudio = {}, want 2 (empty and 'und' both count)", p.untagged_audio);
    assert_eq!(p.untagged_subtitles, 1, "untaggedSubtitles = {}, want 1", p.untagged_subtitles);
}

// mdhd v1 widens created/modified/duration to 64-bit, moving the language field. Reading it at the v0
// offset yields three garbage bytes, which is how a file gets a language nobody wrote.
#[test]
fn parse_mp4_mdhd_version1_language_offset() {
    let (p, ok) = parse_mp4(&mp4_file(&[trak("soun", "ger", 1, &[audio_entry("mp4a", 2)])]));
    assert!(ok, "parse");
    assert!(p.audio.len() == 1 && p.audio[0] == "de", "audio = {:?}, want [de] from a v1 mdhd", p.audio);
}

#[test]
fn parse_mp4_dolby_vision_config_box() {
    let (plain, _) = parse_mp4(&mp4_file(&[trak("vide", "und", 0, &[video_entry("hvc1", &[])])]));
    assert!(!plain.dolby_vision, "no dvcC/dvvC → not Dolby Vision");
    for box_type in ["dvcC", "dvvC"] {
        let (got, ok) = parse_mp4(&mp4_file(&[trak(
            "vide",
            "und",
            0,
            &[video_entry("hvc1", &[mp4box(box_type, &[0u8; 4])])],
        )]));
        assert!(ok && got.dolby_vision, "{box_type} present → Dolby Vision (ok={ok}, dv={})", got.dolby_vision);
    }
}

// Every way a head can fail to be a parseable MP4. None may panic, and none may report a guess.
#[test]
fn parse_mp4_rejects_what_it_cannot_read() {
    let cases: Vec<(&str, Vec<u8>)> = vec![
        ("empty", vec![]),
        ("shorter than a box header", vec![0, 0, 0, 1]),
        ("not ftyp", [mp4box("junk", &[0u8; 8]), mp4box("moov", &[])].concat()),
        ("ftyp but no moov", mp4box("ftyp", &[0u8; 8])),
        ("moov with no tracks", mp4_file(&[])),
        ("a track with no mdia", [mp4box("ftyp", &[0u8; 8]), mp4box("moov", &mp4box("trak", &[]))].concat()),
        ("a track that describes nothing", mp4_file(&[trak("vide", "", 0, &[])])),
    ];
    for (name, head) in cases {
        let (p, ok) = parse_mp4(&head);
        assert!(!ok, "reported {p:?} for {name} — silence beats a guess");
    }
}

// The box walker is the part that reads sizes out of untrusted bytes. A wrong size must stop the walk,
// never index outside the buffer.
#[test]
fn boxes_size_handling() {
    // 64-bit size
    {
        let payload = [0u8; 4];
        // header(8) + widesize(8) + payload
        let mut buf = 1u32.to_be_bytes().to_vec(); // size==1 → the real size is 64-bit and follows the type
        buf.extend_from_slice(b"wide");
        buf.extend_from_slice(&((16 + payload.len()) as u64).to_be_bytes());
        buf.extend_from_slice(&payload);

        let got = boxes(&buf);
        assert!(
            got.len() == 1 && got[0].typ == "wide" && got[0].body.len() == payload.len(),
            "64-bit sized box mis-read: {got:?}"
        );
    }

    // size 0 runs to the end
    {
        let mut buf = vec![0u8; 8];
        buf[4..8].copy_from_slice(b"last");
        buf.extend_from_slice(&[1, 2, 3, 4]);
        let got = boxes(&buf);
        assert!(got.len() == 1 && got[0].body.len() == 4, "size-0 box must extend to the buffer end: {got:?}");
    }

    // truncated and impossible sizes stop the walk
    let bad: [Vec<u8>; 3] = [
        // size past the end
        vec![0, 0, 0, 0xFF, b'b', b'o', b'x', b'x', 1, 2],
        // size smaller than its own header
        vec![0, 0, 0, 4, b'b', b'o', b'x', b'x'],
        // 64-bit flagged, no room for the width
        vec![0, 0, 0, 1, b'b', b'o', b'x', b'x'],
    ];
    for (i, buf) in bad.iter().enumerate() {
        let got = boxes(buf);
        assert!(got.is_empty(), "case {i}: walked into a bad size: {got:?}");
    }
}

#[test]
fn four_cc_and_channel_mapping() {
    let codecs = [
        ("XVID", "mpeg4"),
        ("divx", "mpeg4"),
        ("DX50", "mpeg4"),
        ("mp4v", "mpeg4"),
        ("FMP4", "mpeg4"),
        ("avc1", "h264"),
        ("H264", "h264"),
        ("x264", "h264"),
        ("hvc1", "hevc"),
        ("HEV1", "hevc"),
        ("x265", "hevc"),
        // Dolby Vision's own sample entries: profile 5 MP4s carry no other codec tag.
        ("dvh1", "hevc"),
        ("dvhe", "hevc"),
        ("dva1", "h264"),
        ("dvav", "h264"),
        ("av01", "av1"),
        ("vp09", "vp9"),
        ("VP90", "vp9"),
        // Anything unrecognised must stay empty rather than becoming a wrong tag — Den's autoPickRank
        // demotes on the codec name, so a bad guess mis-ranks a release it can't decode.
        ("zzzz", ""),
        ("", ""),
    ];
    for (cc, want) in codecs {
        let got = codec_from_four_cc(cc);
        assert_eq!(got, want, "codecFromFourCC({cc:?}) = {got:?}, want {want:?}");
    }

    let layouts = [(0, ""), (1, "1.0"), (2, "2.0"), (5, "2.0"), (6, "5.1"), (7, "5.1"), (8, "7.1"), (12, "7.1")];
    for (n, want) in layouts {
        let got = channel_layout(n);
        assert_eq!(got, want, "channelLayout({n}) = {got:?}, want {want:?}");
    }
    // channelCount is the inverse, used to rank one layout against another; unknown text is 0, not a guess.
    for layout in ["7.1", "5.1", "2.0", "1.0"] {
        assert_eq!(channel_count(layout), channel_count_round_trip(layout), "channelCount({layout:?}) disagrees with its layout");
    }
    assert!(channel_count("atmos") == 0 && channel_count("") == 0, "an unrecognised layout counts as 0 channels, never a guess");
}

// The smallest channel count that produces this layout — channelLayout(channelCount(x)) must be x.
fn channel_count_round_trip(layout: &str) -> i64 {
    match layout {
        "7.1" => 8,
        "5.1" => 6,
        "2.0" => 2,
        "1.0" => 1,
        _ => 0,
    }
}

// Real files, from a real muxer. The synthetic boxes above pin the EDGE cases a muxer will never
// produce (a truncated size, a 64-bit header, a v1 mdhd); these pin that the parser agrees with what
// ffmpeg actually writes — the thing a hand-built fixture can quietly drift away from.
//
// Regenerate with (see testdata/README.md):
//
//	ffmpeg -f lavfi -i testsrc=size=64x64:rate=5:duration=1 \
//	       -f lavfi -i sine=frequency=440:duration=1 -f lavfi -i sine=frequency=880:duration=1 \
//	       -map 0:v -map 1:a -map 2:a -c:v libx264 -preset ultrafast -c:a aac -ac:a:0 6 -ac:a:1 2 \
//	       -metadata:s:a:0 language=swe -metadata:s:a:1 language=ita -movflags +faststart multi-audio.mp4
#[test]
fn parse_mp4_real_file_from_ffmpeg() {
    let (p, ok) = parse_mp4(&testdata("multi-audio.mp4"));
    assert!(ok, "a real ffmpeg mp4 must parse");
    assert_eq!(p.video_codec, "h264", "videoCodec = {:?}, want h264 (avc1)", p.video_codec);
    assert!(
        p.audio.len() == 2 && p.audio[0] == "sv" && p.audio[1] == "it",
        "audio = {:?}, want [sv it] — the languages ffmpeg was told to tag",
        p.audio
    );
    // The first audio track was muxed as 5.1; the layout must come from the file, not the release name.
    assert_eq!(p.audio_channels, "5.1", "audioChannels = {:?}, want 5.1", p.audio_channels);
    assert_eq!(p.untagged_audio, 0, "untaggedAudio = {} — both tracks carry a language", p.untagged_audio);
    assert!(!p.dolby_vision, "a plain h264 file is not Dolby Vision");
}

// The case the parser documents and declines: a muxer that leaves `moov` at the END. Production reads a
// bounded head, so the metadata is simply not there — and reporting nothing is right, because the
// alternative is describing a file from whatever bytes happened to be in reach.
#[test]
fn parse_mp4_moov_beyond_the_head_reports_nothing() {
    let full = testdata("moov-at-end.mp4");
    let (p, ok) = parse_mp4(&full);
    assert!(
        ok && p.audio.len() == 1 && p.audio[0] == "fr",
        "the whole file still parses: ok={ok} probe={p:?}"
    );
    // The first 4 KiB is all a probe of a slow source may get before its budget runs out.
    let (p, ok) = parse_mp4(&full[..4096]);
    assert!(!ok, "described a file whose moov it never saw: {p:?}");
}

// The RICHEST audio track wins, not the first one listed.
//
// probe.go states the rule and readTrackFacts follows it; MP4 and AVI did the opposite, so a remux
// listing 5.1 before 7.1 reported "5.1" — and the probe OVERRIDES the correctly-titled value, so the
// viewer was shown a worse fact than the release name already carried. Driven through the real parser:
// asserting the comparison rule alone passed against the first-wins code.
#[test]
fn parse_mp4_richest_audio_track_wins() {
    let five_then_seven = mp4_file(&[
        trak("soun", "eng", 0, &[audio_entry("mp4a", 6)]),
        trak("soun", "eng", 0, &[audio_entry("mp4a", 8)]),
    ]);
    let (p, ok) = parse_mp4(&five_then_seven);
    assert!(ok && p.audio_channels == "7.1", "5.1 listed first won over 7.1: {:?} (ok={ok})", p.audio_channels);
    // And a poorer later track must not overwrite a richer earlier one.
    let seven_then_stereo = mp4_file(&[
        trak("soun", "eng", 0, &[audio_entry("mp4a", 8)]),
        trak("soun", "eng", 0, &[audio_entry("mp4a", 2)]),
    ]);
    let (p, ok) = parse_mp4(&seven_then_stereo);
    assert!(ok && p.audio_channels == "7.1", "a stereo track overwrote 7.1: {:?} (ok={ok})", p.audio_channels);
}
