//! Go: `internal/scout/shape_test.go` (212 lines). Work package 5.

use crate::attributes::stream_attributes;
use crate::client::{NEVER_PLAYS_COST, AUDIO_CONVERTED_COST, VIDEO_CONVERTED_COST};
use crate::probe::{parse_head, parse_matroska_tracks, Probe, ID_CODEC_ID, ID_CODEC_PRIVATE, ID_COLOUR, ID_DEFAULT_DURATION,
    ID_LANGUAGE, ID_PIXEL_HEIGHT, ID_PIXEL_WIDTH, ID_SEGMENT, ID_TRACKS, ID_TRACK_ENTRY, ID_TRACK_TYPE, ID_TRANSFER_CHARACTERISTICS,
    ID_VIDEO, TRACK_TYPE_AUDIO, TRACK_TYPE_VIDEO};
use crate::probe_mp4::parse_mp4;
use crate::rank::RawStream;

use super::attributes_web::{ebml, hvc_c};
use super::client::a_browser;
use super::probe_containers::testdata;
use super::probe_mp4::{concat, hdlr, mp4_file, mp4box, stsd};

// A visual sample entry with its width and height at offsets 24 and 26, and config boxes past the 78-byte header.
fn sized_video_entry(four_cc: &str, width: u16, height: u16, extra: &[Vec<u8>]) -> Vec<u8> {
    let mut head = vec![0u8; 78];
    head[24..26].copy_from_slice(&width.to_be_bytes());
    head[26..28].copy_from_slice(&height.to_be_bytes());
    mp4box(four_cc, &[head, concat(extra)].concat())
}

// A video trak whose mdhd carries a timescale and whose stts times every sample by one delta.
fn timed_video_trak(timescale: u32, samples: u32, delta: u32, entry: &[u8]) -> Vec<u8> {
    let mut mdhd_body = vec![0u8; 24];
    mdhd_body[12..16].copy_from_slice(&timescale.to_be_bytes());
    let mut stts = vec![0u8; 16];
    stts[4..8].copy_from_slice(&1u32.to_be_bytes());
    stts[8..12].copy_from_slice(&samples.to_be_bytes());
    stts[12..16].copy_from_slice(&delta.to_be_bytes());
    let stbl = mp4box("stbl", &concat(&[stsd(&[entry.to_vec()]), mp4box("stts", &stts)]));
    let mdia = mp4box(
        "mdia",
        &concat(&[hdlr("vide"), mp4box("mdhd", &mdhd_body), mp4box("minf", &stbl)]),
    );
    mp4box("trak", &mdia)
}

fn nclx(transfer: u16) -> Vec<u8> {
    let mut body = vec![0u8; 11];
    body[..4].copy_from_slice(b"nclx");
    body[6..8].copy_from_slice(&transfer.to_be_bytes());
    mp4box("colr", &body)
}

// hvcC with a tier flag and level: byte 1 packs profile space, tier and profile; byte 12 is the level.
fn hvc_c_shape(profile: u8, high_tier: bool, level: u8) -> Vec<u8> {
    let mut rec = hvc_c(10);
    rec[1] = profile;
    if high_tier {
        rec[1] |= 0x20;
    }
    rec[12] = level;
    rec
}

// The shape fields of a probe, for comparing one against another.
fn shape_of(p: &Probe) -> Probe {
    Probe {
        video_codec: p.video_codec.clone(),
        video_profile: p.video_profile,
        video_level: p.video_level,
        high_tier: p.high_tier,
        width: p.width,
        height: p.height,
        frame_rate: p.frame_rate,
        bit_depth: p.bit_depth,
        ..Default::default()
    }
}

#[test]
fn parse_mp4_video_shape() {
    let cases: Vec<(&str, Vec<u8>, Probe, &str)> = vec![
        (
            "HEVC Main 10 high tier L5.1, PQ, 23.976 fps",
            timed_video_trak(
                24000,
                240,
                1001,
                &sized_video_entry("hvc1", 3840, 1608, &[mp4box("hvcC", &hvc_c_shape(2, true, 153)), nclx(16)]),
            ),
            Probe {
                video_codec: "hevc".into(),
                video_profile: 2,
                video_level: 153,
                high_tier: true,
                width: 3840,
                height: 1608,
                frame_rate: 23.976,
                bit_depth: 10,
                ..Default::default()
            },
            "HDR10",
        ),
        (
            "H.264 High L4.1, 25 fps",
            timed_video_trak(25, 100, 1, &sized_video_entry("avc1", 1920, 1080, &[mp4box("avcC", &[1, 100, 0, 41])])),
            Probe {
                video_codec: "h264".into(),
                video_profile: 100,
                video_level: 41,
                width: 1920,
                height: 1080,
                frame_rate: 25.0,
                bit_depth: 8,
                ..Default::default()
            },
            "",
        ),
        (
            "AV1 Main 10-bit level 5.0, HLG",
            timed_video_trak(
                60000,
                600,
                1001,
                &sized_video_entry("av01", 3840, 2160, &[mp4box("av1C", &[0x81, 12, 0x40]), nclx(18)]),
            ),
            Probe {
                video_codec: "av1".into(),
                video_profile: 0,
                video_level: 12,
                width: 3840,
                height: 2160,
                frame_rate: 59.94,
                bit_depth: 10,
                ..Default::default()
            },
            "HLG",
        ),
    ];
    for (name, trak_bytes, want, want_hdr) in cases {
        let (p, ok) = parse_mp4(&mp4_file(&[trak_bytes]));
        assert!(ok, "{name}: not parsed");
        let got = shape_of(&p);
        assert_eq!(got, shape_of(&want), "{name}: shape");
        assert_eq!(p.hdr_format, want_hdr, "{name}: hdrFormat");
    }
}

#[test]
fn parse_matroska_video_shape() {
    let video = ebml(
        ID_VIDEO,
        &[
            ebml(ID_PIXEL_WIDTH, &[vec![0x0F, 0x00]]),
            ebml(ID_PIXEL_HEIGHT, &[vec![0x06, 0x48]]),
            ebml(ID_COLOUR, &[ebml(ID_TRANSFER_CHARACTERISTICS, &[vec![16]])]),
        ],
    );
    let entry = ebml(
        ID_TRACK_ENTRY,
        &[
            ebml(ID_TRACK_TYPE as u32, &[vec![TRACK_TYPE_VIDEO]]),
            ebml(ID_CODEC_ID, &[b"V_MPEGH/ISO/HEVC".to_vec()]),
            ebml(ID_CODEC_PRIVATE, &[hvc_c_shape(2, false, 150)]),
            // 41708381 ns: 23.976 fps
            ebml(ID_DEFAULT_DURATION, &[vec![0x02, 0x7C, 0x6D, 0x5D]]),
            video,
        ],
    );
    let audio = ebml(
        ID_TRACK_ENTRY,
        &[ebml(ID_TRACK_TYPE as u32, &[vec![TRACK_TYPE_AUDIO]]), ebml(ID_LANGUAGE, &[b"eng".to_vec()])],
    );
    let p = parse_matroska_tracks(&ebml(ID_SEGMENT, &[ebml(ID_TRACKS, &[entry, audio])])).expect("parse");
    assert!(
        p.video_level == 150
            && !p.high_tier
            && p.width == 3840
            && p.height == 1608
            && p.frame_rate == 23.976
            && p.hdr_format == "HDR10",
        "got level {} high {} {}x{} {} fps {:?}",
        p.video_level,
        p.high_tier,
        p.width,
        p.height,
        p.frame_rate,
        p.hdr_format
    );
}

// ffmpeg's own output (testdata/README.md), in MP4 and Matroska.
#[test]
fn parse_head_video_shape_from_real_files() {
    // (file, width, height, fps)
    let cases: [(&str, i32, i32, f64); 3] = [("hevc10.mp4", 64, 64, 5.0), ("hi10p.mkv", 64, 64, 5.0), ("sample.mp4", 320, 240, 15.0)];
    for (file, width, height, fps) in cases {
        let p = parse_head(&testdata(file)).expect("parse");
        assert!(
            p.width == width && p.height == height && p.frame_rate == fps && p.video_level != 0,
            "{file}: got {}x{} at {} fps, level {}; want {width}x{height} at {fps} fps and a level",
            p.width,
            p.height,
            p.frame_rate,
            p.video_level
        );
    }
}

#[test]
fn guess_dv_profile() {
    let cases = [
        ("Film.2021.2160p.UHD.BluRay.REMUX.DV.HDR10.HEVC.TrueHD.7.1", 7),
        ("Film.2021.2160p.BluRay.DV.x265.10bit", 8),
        ("Film.2021.2160p.WEB-DL.DV.HDR10.HEVC.DDP5.1", 8),
        ("Film.2021.2160p.HYBRID.DV.x265", 8),
        ("Film.2021.2160p.NF.WEB-DL.DV.HEVC.DDP5.1", 5),
        ("Film.2021.2160p.DV.HEVC", 0),
        // no Dolby Vision to guess about
        ("Film.2021.2160p.WEB-DL.HDR10.HEVC", 0),
    ];
    for (title, want) in cases {
        let got = stream_attributes(RawStream { title: title.into(), ..Default::default() }).dv_profile_guess;
        assert_eq!(got, want, "guess({title:?}) = {got}, want {want}");
    }
    let probed = stream_attributes(RawStream {
        title: "Film.2021.2160p.WEB-DL.DV.HEVC".into(),
        probe: Some(Probe { video_codec: "hevc".into(), dolby_vision: true, dv_profile: 8, ..Default::default() }),
        ..Default::default()
    });
    assert!(
        probed.dv_profile_guess == 0 && probed.dv_profile == 8,
        "a probed profile must replace the guess: profile {}, guess {}",
        probed.dv_profile,
        probed.dv_profile_guess
    );
}

#[test]
fn client_playable_cost_from_the_probe() {
    let probed = |title: &str, p: Probe| {
        let s = RawStream { title: title.into(), probe: Some(p), ..Default::default() };
        let a = stream_attributes(s.clone());
        (s, a)
    };
    let browser = a_browser(|p| p.hevc_high_tier = 0);
    // (name, title, probe, want)
    let cases: Vec<(&str, &str, Probe, i64)> = vec![
        (
            "HEVC high tier to a browser without it is transcoded",
            "Film.2160p.REMUX.AAC.mkv",
            Probe {
                container: "matroska".into(),
                video_codec: "hevc".into(),
                bit_depth: 10,
                video_level: 153,
                high_tier: true,
                width: 3840,
                ..Default::default()
            },
            VIDEO_CONVERTED_COST,
        ),
        (
            "HEVC over the browser's level is transcoded",
            "Film.2160p.AAC.mkv",
            Probe {
                container: "matroska".into(),
                video_codec: "hevc".into(),
                bit_depth: 10,
                video_level: 183,
                width: 7680,
                ..Default::default()
            },
            VIDEO_CONVERTED_COST,
        ),
        (
            "H.264 over the browser's level can't play",
            "Film.1080p.AAC.mkv",
            Probe { container: "matroska".into(), video_codec: "h264".into(), bit_depth: 8, video_level: 52, width: 1920, ..Default::default() },
            NEVER_PLAYS_COST,
        ),
        (
            "H.264 within it plays as it is",
            "Film.1080p.AAC.mkv",
            Probe { container: "matroska".into(), video_codec: "h264".into(), bit_depth: 8, video_level: 41, width: 1920, ..Default::default() },
            0,
        ),
        (
            "an HDR transfer the name never gave counts as HDR",
            "Film.1080p.AAC.mkv",
            Probe {
                container: "matroska".into(),
                video_codec: "hevc".into(),
                bit_depth: 10,
                video_level: 123,
                width: 1920,
                hdr_format: "HDR10".into(),
                ..Default::default()
            },
            0,
        ),
        (
            "VP9 profile 0 to a browser that takes none can't play",
            "Film.1080p.AAC.mkv",
            Probe { container: "matroska".into(), video_codec: "vp9".into(), bit_depth: 8, width: 1920, ..Default::default() },
            NEVER_PLAYS_COST,
        ),
        (
            "FLAC to a browser without it is converted",
            "Film.1080p.x264.FLAC.mkv",
            Probe { container: "matroska".into(), video_codec: "h264".into(), bit_depth: 8, video_level: 41, width: 1920, ..Default::default() },
            AUDIO_CONVERTED_COST,
        ),
    ];
    for (name, title, probe, want) in cases {
        let (s, a) = probed(title, probe);
        let got = browser.cost(s, a);
        assert_eq!(got, want, "{name}: cost = {got}, want {want}");
    }

    let no_hdr = a_browser(|p| p.hdr = false);
    let (s, a) = probed(
        "Film.1080p.AAC.mkv",
        Probe {
            container: "matroska".into(),
            video_codec: "hevc".into(),
            bit_depth: 10,
            video_level: 123,
            width: 1920,
            hdr_format: "HDR10".into(),
            ..Default::default()
        },
    );
    let got = no_hdr.cost(s, a);
    assert_eq!(got, VIDEO_CONVERTED_COST, "HDR the probe found, to a browser without HDR: cost {got}");

    let copies = a_browser(|p| {
        p.flac = true;
        p.vp9 = true;
    });
    let (s, a) = probed(
        "Film.1080p.FLAC.mkv",
        Probe { container: "matroska".into(), video_codec: "vp9".into(), bit_depth: 8, width: 1920, ..Default::default() },
    );
    let got = copies.cost(s, a);
    assert_eq!(got, 0, "VP9 with FLAC to a browser that plays both: cost {got}");

    let guessed = RawStream { title: "Film.2160p.NF.WEB-DL.DV.HEVC.AAC.mkv".into(), ..Default::default() };
    let got = a_browser(|_| {}).cost(guessed.clone(), stream_attributes(guessed));
    assert_eq!(got, VIDEO_CONVERTED_COST, "a web DV release guessed as profile 5: cost {got}");
}
