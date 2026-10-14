//! Go: `internal/scout/attributes_web_test.go` (305 lines).
//!
//! Also holds the EBML and `hvcC` builders the shape tests share (`ebml`, `hvc_c`, `avc_c`), the way the Go
//! file holds them for `shape_test.go`.

use crate::attributes::{detect_audio_codec, stream_attributes};
use crate::probe::{bit_depth_from_config, parse_matroska_tracks, Probe, TRACK_TYPE_AUDIO, TRACK_TYPE_VIDEO};
use crate::probe_mp4::parse_mp4;
use crate::rank::RawStream;
use den_media::codec::{avc_bit_depth, hevc_bit_depth};
use den_media::mkv::{
    BLOCK_ADDITION_MAPPING as ID_BLOCK_ADDITION_MAPPING,
    BLOCK_ADD_ID_EXTRA_DATA as ID_BLOCK_ADD_ID_EXTRA_DATA, BLOCK_ADD_ID_TYPE as ID_BLOCK_ADD_ID_TYPE,
    CODEC_ID as ID_CODEC_ID, CODEC_PRIVATE as ID_CODEC_PRIVATE, LANGUAGE as ID_LANGUAGE,
    SEGMENT as ID_SEGMENT, TRACKS as ID_TRACKS, TRACK_ENTRY as ID_TRACK_ENTRY, TRACK_TYPE as ID_TRACK_TYPE,
};

use super::probe_containers::testdata;
use super::probe_mp4::{mp4_file, mp4box, trak, video_entry};

// Release names as indexers actually spell them, through the whole title path: codec, audio family and bit
// depth.
#[test]
fn stream_attributes_from_realistic_titles() {
    // (title, codec, audio, depth)
    let cases = [
        ("The.Movie.2023.1080p.WEB-DL.DDP5.1.H.264-FLUX", "h264", "eac3", 0),
        ("Movie.2019.1080p.BluRay.DTS-HD.MA.5.1.x264-GRP.mkv", "h264", "dtshdma", 0),
        ("Movie.2021.2160p.UHD.BluRay.REMUX.HEVC.DV.HDR.TrueHD.Atmos.7.1-GRP.mkv", "hevc", "truehd", 10),
        ("Movie.2020.1080p.WEBRip.x264.AAC5.1-[YTS.MX].mp4", "h264", "aac", 0),
        ("Show.S01E01.1080p.WEB-DL.x265.10bit.AAC5.1-GRP.mkv", "hevc", "aac", 10),
        ("Movie.2022.1080p.WEBRip.x265.10bit.Opus.5.1-GRP.mkv", "hevc", "opus", 10),
        // Hi10P names no codec but is H.264 High 10 by definition.
        ("[Group] Anime - 01 [BD 1080p Hi10P FLAC].mkv", "h264", "flac", 10),
        ("Movie.2023.2160p.WEB-DL.DV.HEVC.AAC.mp4", "hevc", "aac", 10),
        ("Movie.2024.1080p.WEB-DL.AV1.Opus.webm", "av1", "opus", 0),
        // The title path doesn't name XviD.
        ("Movie.2005.DVDRip.XviD-MP3.avi", "", "mp3", 0),
    ];
    for (title, codec, audio, depth) in cases {
        let a = stream_attributes(RawStream { title: title.into(), ..Default::default() });
        let got = (a.codec.as_deref().unwrap_or(""), a.audio_codec.as_deref().unwrap_or(""), a.bit_depth);
        assert_eq!(got, (codec, audio, depth), "{title}: codec/audio/depth");
    }
}

// The new audio branches, and the names that must NOT trip them.
#[test]
fn detect_audio_codec_browser_codecs() {
    let cases = [
        ("movie 2020 1080p web-dl aac2.0 h264", "aac"),
        ("movie 2020 1080p web-dl he-aac x264", "aac"),
        ("movie 2020 1080p bluray x264 mp3", "mp3"),
        ("show s01e01 1080p hevc opus", "opus"),
        // DD+ is the main track; the AAC is the companion
        ("movie 2020 1080p web-dl ddp5.1 aac2.0 h.264", "eac3"),
        ("movie 2020 1080p bluray ac3 aac x264", "ac3"),
        ("mr.hollands.opus.1995.1080p.bluray.x264-grp", ""),
        ("opus.2025.2160p.web-dl.x265", ""),
        ("opus.2025.1080p.web-dl.aac.x264", "aac"),
        ("isaac.2020.1080p.web-dl.x264", ""),
        ("aachen.story.2019.1080p.x264", ""),
    ];
    for (title, want) in cases {
        let got = detect_audio_codec(title);
        assert_eq!(got, want, "detectAudioCodec({title:?}) = {got:?}, want {want:?}");
    }
}

#[test]
fn detect_bit_depth() {
    let cases = [
        ("movie 1080p x265 10bit", 10),
        ("movie 1080p x265 10-bit", 10),
        ("movie 1080p hevc 10 bit", 10),
        ("movie 1080p x265 10bits", 10),
        ("movie 1080p hi10", 10),
        ("movie 1080p main10 hevc", 10),
        ("movie 1080p bluray x264", 0),
        // "10.bit" inside an episode number and a word
        ("show s01e10.bitter.end.1080p", 0),
        ("movie 2010 bit part 1080p x264", 0),
        // HDR is 10-bit by definition
        ("movie 2160p web-dl hdr10 hevc", 10),
        ("movie 2160p web-dl dv hevc ddp5.1", 10),
    ];
    for (title, want) in cases {
        let a = stream_attributes(RawStream { title: title.into(), ..Default::default() });
        assert_eq!(a.bit_depth, want, "bitDepth({title:?}) = {}, want {want}", a.bit_depth);
    }
}

// What the file says replaces what the title guessed, in both directions.
#[test]
fn stream_attributes_follow_the_probe() {
    let cases: [(&str, &str, Probe); 6] = [
        (
            "High 10 in MP4, named plain x264",
            "Movie 1080p x264 AAC",
            Probe {
                container: "mp4".into(),
                video_codec: "h264".into(),
                bit_depth: 10,
                ..Default::default()
            },
        ),
        (
            "8-bit probe beats an HDR title's 10",
            "Movie 1080p HDR x264 AAC",
            Probe { container: "mp4".into(), video_codec: "h264".into(), bit_depth: 8, ..Default::default() },
        ),
        (
            "DV profile 5 in Matroska",
            "Movie 2160p AAC",
            Probe {
                container: "matroska".into(),
                video_codec: "hevc".into(),
                dolby_vision: true,
                dv_profile: 5,
                ..Default::default()
            },
        ),
        (
            "DV profile 8, title says only DV",
            "Movie 2160p DV AAC",
            Probe {
                container: "mp4".into(),
                video_codec: "hevc".into(),
                dolby_vision: true,
                dv_profile: 8,
                ..Default::default()
            },
        ),
        (
            "DV profile 7 in MP4",
            "Movie 2160p AAC",
            Probe {
                container: "mp4".into(),
                video_codec: "hevc".into(),
                dolby_vision: true,
                dv_profile: 7,
                ..Default::default()
            },
        ),
        (
            "VP9 in Matroska",
            "Movie 1080p WEB-DL Opus",
            Probe { container: "matroska".into(), video_codec: "vp9".into(), ..Default::default() },
        ),
    ];
    for (name, title, p) in cases {
        let a = stream_attributes(RawStream {
            title: title.into(),
            probe: Some(p.clone()),
            ..Default::default()
        });
        let codec = a.codec.as_deref().unwrap_or("");
        if !p.video_codec.is_empty() {
            assert_eq!(codec, p.video_codec, "{name}: codec = {codec:?}, the probe said {:?}", p.video_codec);
        }
        if p.bit_depth != 0 {
            assert_eq!(
                a.bit_depth, p.bit_depth as i64,
                "{name}: bitDepth = {}, the probe said {}",
                a.bit_depth, p.bit_depth
            );
        }
        if p.dv_profile != 0 {
            assert_eq!(
                a.dv_profile, p.dv_profile as i64,
                "{name}: dvProfile = {}, the probe said {}",
                a.dv_profile, p.dv_profile
            );
        }
    }
}

// Unknown depth and profile are omitted from the wire rather than sent as 0.
#[test]
fn stream_attributes_wire_shape() {
    let body = serde_json::to_value(stream_attributes(RawStream {
        title: "Movie 1080p".into(),
        ..Default::default()
    }))
    .expect("json");
    assert!(body.get("bitDepth").is_none(), "an unknown bit depth must be omitted, not sent as 0");
    assert!(body.get("dvProfile").is_none(), "an unknown DV profile must be omitted, not sent as 0");
}

#[test]
fn container_of() {
    let cases = [
        ("Movie.mp4", "mp4"),
        ("Movie.M4V", "mp4"),
        ("Movie.mkv", "matroska"),
        ("Movie.webm", "webm"),
        ("Movie.avi", "avi"),
        ("Movie 1080p", ""),
    ];
    for (title, want) in cases {
        let got = crate::attributes::container_of(RawStream { title: title.into(), ..Default::default() });
        assert_eq!(got, want, "containerOf({title:?}) = {got:?}, want {want:?}");
    }
}

// Real files from ffmpeg (testdata/README.md): the bit depth comes from the configuration record the
// muxer wrote, in MP4's hvcC and in Matroska's CodecPrivate, not from a name.
#[test]
fn parse_head_bit_depth_from_real_files() {
    // (file, container, codec, depth)
    let cases = [
        // hvcC bitDepthLumaMinus8 = 2
        ("hevc10.mp4", "mp4", "hevc", 10),
        // CodecPrivate avcC, profile_idc 110
        ("hi10p.mkv", "matroska", "h264", 10),
        // Constrained Baseline
        ("sample.mp4", "mp4", "h264", 8),
        // High 4:4:4 Predictive allows 8 or 10 bits and only the SPS says which: unknown, not a guess.
        ("multi-audio.mp4", "mp4", "h264", 0),
    ];
    for (file, container, codec, depth) in cases {
        let p = crate::probe::parse_head(&testdata(file)).expect("parse");
        assert!(
            p.container == container && p.video_codec == codec && p.bit_depth == depth,
            "{file}: got {}/{}/{}, want {container}/{codec}/{depth}",
            p.container,
            p.video_codec,
            p.bit_depth
        );
        assert!(
            !p.dolby_vision && p.dv_profile == 0,
            "no Dolby Vision in this file: dv={} profile={}",
            p.dolby_vision,
            p.dv_profile
        );
    }
}

// DOVIDecoderConfigurationRecord bytes, laid out as doviProfile documents. byte 3 = level 6 in its high
// 5 bits plus the rpu/el/bl present flags; byte 4's high nibble is the base-layer compatibility id.
const DOVI_P5: [u8; 5] = [0x01, 0x00, 0x0A, 0x35, 0x00]; // profile 5, level 6, rpu+bl, compat 0 (no fallback)
const DOVI_P7: [u8; 5] = [0x01, 0x00, 0x0E, 0x37, 0x60]; // profile 7, level 6, rpu+el+bl, compat 6 (HDR10 base)
const DOVI_P81: [u8; 5] = [0x01, 0x00, 0x10, 0x35, 0x10]; // profile 8, level 6, rpu+bl, compat 1 (HDR10 base)

// Dolby Vision can't be minted from a test source (ffmpeg needs an RPU to write one), so the record
// parser is pinned byte by byte instead.
#[test]
fn dovi_profile() {
    let cases: [(&str, &[u8], i64); 6] = [
        ("profile 5", &DOVI_P5, 5),
        ("profile 7", &DOVI_P7, 7),
        ("profile 8.1", &DOVI_P81, 8),
        // dv_level's top bit shares byte 2 with the profile and must not leak into it.
        ("level bit set", &[0x01, 0x00, 0x11, 0x05], 8),
        ("too short", &[0x01, 0x00, 0x10], 0),
        ("missing record", &[], 0),
    ];
    for (name, record, want) in cases {
        let got = crate::probe::dovi_profile(record);
        assert_eq!(got, want, "{name}: doviProfile = {got}, want {want}");
    }
}

// An hvcC record whose bitDepthLumaMinus8 byte carries the reserved high bits a muxer sets (0b11111).
pub(crate) fn hvc_c(depth: u8) -> Vec<u8> {
    let mut rec = vec![0u8; 23];
    // version 1, Main 10
    rec[0] = 1;
    rec[1] = 2;
    rec[17] = 0xF8 | (depth - 8);
    rec
}

pub(crate) fn avc_c(profile: u8) -> Vec<u8> {
    vec![1, profile, 0, 40, 0xFF, 0xE0]
}

#[test]
fn parse_mp4_video_config_boxes() {
    // DV profile 5 sample entry with hvcC and dvcC
    let (p, ok) = parse_mp4(&mp4_file(&[trak(
        "vide",
        "und",
        0,
        &[video_entry("dvh1", &[mp4box("hvcC", &hvc_c(10)), mp4box("dvcC", &DOVI_P5)])],
    )]));
    assert!(
        ok && p.video_codec == "hevc" && p.bit_depth == 10 && p.dolby_vision && p.dv_profile == 5,
        "got {p:?} (ok={ok}), want hevc, 10-bit, DV profile 5"
    );

    // High 10 avcC
    let (p, _) = parse_mp4(&mp4_file(&[trak(
        "vide",
        "und",
        0,
        &[video_entry("avc1", &[mp4box("avcC", &avc_c(110))])],
    )]));
    assert_eq!(p.bit_depth, 10, "bitDepth = {}, want 10 from profile_idc 110", p.bit_depth);

    // dvwC
    let (p, _) = parse_mp4(&mp4_file(&[trak(
        "vide",
        "und",
        0,
        // profile 20
        &[video_entry("hvc1", &[mp4box("dvwC", &[1, 0, 20 << 1, 0])])],
    )]));
    assert!(
        p.dolby_vision && p.dv_profile == 20,
        "dv={} profile={}, want the dvwC record read",
        p.dolby_vision,
        p.dv_profile
    );
}

#[test]
fn avc_and_hevc_bit_depth() {
    let cases = [(66u8, 8), (77, 8), (88, 8), (100, 8), (110, 10), (122, 0), (244, 0)];
    for (profile, want) in cases {
        let got = avc_bit_depth(&avc_c(profile));
        assert_eq!(got, want, "avcBitDepth(profile {profile}) = {got}, want {want}");
    }
    assert!(
        avc_bit_depth(&[1]) == 0 && hevc_bit_depth(&[0u8; 17]) == 0,
        "a truncated record must read as unknown"
    );
    assert!(hevc_bit_depth(&hvc_c(8)) == 8 && hevc_bit_depth(&hvc_c(10)) == 10, "hvcC bit depth mis-read");
    assert!(
        bit_depth_from_config("av1", &hvc_c(10)) == 0,
        "a codec with no parser here must read as unknown"
    );
}

// ebml assembles one element: its id's own bytes, an 8-byte size, then the body.
pub(crate) fn ebml(id: u32, body: &[Vec<u8>]) -> Vec<u8> {
    let mut out = Vec::new();
    for shift in [24u32, 16, 8, 0] {
        let b = (id >> shift) as u8;
        if b != 0 || !out.is_empty() {
            out.push(b);
        }
    }
    let b = body.concat();
    let n = b.len() as u64;
    out.push(0x01);
    out.extend_from_slice(&n.to_be_bytes()[1..]);
    out.extend_from_slice(&b);
    out
}

// A Matroska head with an HEVC video track and an English audio track (without which the parser rightly
// reports no tracks). `mapping` is the video's BlockAdditionMapping, or None.
fn mkv_with_video(private: &[u8], mapping: Option<Vec<u8>>) -> Vec<u8> {
    let mut video = vec![
        ebml(ID_TRACK_TYPE, &[vec![TRACK_TYPE_VIDEO]]),
        ebml(ID_CODEC_ID, &[b"V_MPEGH/ISO/HEVC".to_vec()]),
        ebml(ID_CODEC_PRIVATE, &[private.to_vec()]),
    ];
    if let Some(mapping) = mapping {
        video.push(mapping);
    }
    let audio = ebml(
        ID_TRACK_ENTRY,
        &[ebml(ID_TRACK_TYPE, &[vec![TRACK_TYPE_AUDIO]]), ebml(ID_LANGUAGE, &[b"eng".to_vec()])],
    );
    ebml(ID_SEGMENT, &[ebml(ID_TRACKS, &[ebml(ID_TRACK_ENTRY, &video), audio])])
}

#[test]
fn parse_matroska_dolby_vision_block_addition_mapping() {
    // (name, mapping, dv, profile)
    let cases: Vec<(&str, Option<Vec<u8>>, bool, i64)> = vec![
        (
            "dvvC with a profile 8.1 record",
            Some(ebml(
                ID_BLOCK_ADDITION_MAPPING,
                &[
                    ebml(ID_BLOCK_ADD_ID_TYPE, &[b"dvvC".to_vec()]),
                    ebml(ID_BLOCK_ADD_ID_EXTRA_DATA, &[DOVI_P81.to_vec()]),
                ],
            )),
            true,
            8,
        ),
        (
            "dvcC with a profile 7 record",
            Some(ebml(
                ID_BLOCK_ADDITION_MAPPING,
                &[
                    ebml(ID_BLOCK_ADD_ID_TYPE, &[b"dvcC".to_vec()]),
                    ebml(ID_BLOCK_ADD_ID_EXTRA_DATA, &[DOVI_P7.to_vec()]),
                ],
            )),
            true,
            7,
        ),
        // The tag alone still says Dolby Vision is there; it just can't say which profile.
        (
            "dvcC with no record",
            Some(ebml(ID_BLOCK_ADDITION_MAPPING, &[ebml(ID_BLOCK_ADD_ID_TYPE, &[b"dvcC".to_vec()])])),
            true,
            0,
        ),
        (
            "a mapping that isn't Dolby Vision",
            Some(ebml(
                ID_BLOCK_ADDITION_MAPPING,
                &[
                    ebml(ID_BLOCK_ADD_ID_TYPE, &[b"itut".to_vec()]),
                    ebml(ID_BLOCK_ADD_ID_EXTRA_DATA, &[DOVI_P81.to_vec()]),
                ],
            )),
            false,
            0,
        ),
        ("no mapping", None, false, 0),
    ];
    for (name, mapping, dv, profile) in cases {
        let p = parse_matroska_tracks(&mkv_with_video(&hvc_c(10), mapping)).expect("parse");
        assert!(
            p.video_codec == "hevc" && p.bit_depth == 10,
            "{name}: codec/depth = {}/{}, want hevc/10 from CodecPrivate",
            p.video_codec,
            p.bit_depth
        );
        assert!(
            p.dolby_vision == dv && p.dv_profile as i64 == profile,
            "{name}: dv={} profile={}, want dv={dv} profile={profile}",
            p.dolby_vision,
            p.dv_profile
        );
    }
}
