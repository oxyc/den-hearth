//! Go: `internal/scout/probe_containers_test.go` (101 lines). Work package 5.
//!
//! Reads the sample files in Go's `internal/scout/testdata/`; see PORTING.md for where they go.

use crate::probe::parse_head;
use crate::probe_avi::{parse_avi, MAX_AVI_DEPTH};

/// Go `os.ReadFile("testdata/<name>")`, relative to the crate where Go's tests run.
pub(crate) fn testdata(name: &str) -> Vec<u8> {
    std::fs::read(std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("testdata").join(name))
        .expect("testdata fixture")
}

// AVI records no language anywhere, but it does name its codec — and that is the fact that matters most
// here: an XviD rip decodes in software on an Apple TV, which costs the viewer the scrubber and the whole
// native transport. Fixture is a real head from a release the addon offered.
#[test]
fn parse_head_avi() {
    let head = testdata("xvid.avi");
    let got = parse_head(&head).expect("parse");
    assert!(
        got.container == "avi" && got.video_codec == "mpeg4",
        "container={:?} codec={:?}, want avi/mpeg4",
        got.container,
        got.video_codec
    );
    assert!(got.audio.is_empty(), "audio = {:?}, want none — AVI carries no language metadata", got.audio);
    assert!(got.untagged_audio != 0, "an audio track exists and should be counted even though it has no language");
}

// nestedAVI builds an AVI head that nests `depth` LIST chunks and puts a single audio strh at the
// bottom. Each level costs 12 bytes, so a megabyte of head buys tens of thousands of them — the shape a
// file would take if it were built to make the parser recurse rather than to be played.
fn nested_avi(depth: usize) -> Vec<u8> {
    let mut head = b"RIFF\xff\xff\xff\xffAVI ".to_vec();
    for _ in 0..depth {
        // A size larger than the buffer, which the walker clamps to the end — so every level nests
        // rather than running out.
        head.extend_from_slice(b"LIST\xf0\xff\xff\xffjunk");
    }
    // strh: an audio stream header. Read only if the walk actually descended this far.
    head.extend_from_slice(b"strh\x08\x00\x00\x00audsxvid");
    head
}

// The walk stops at a bounded depth. Nesting is the only quantity in the three container parsers that
// the input controls with no ceiling, and these are bytes a remote server chose.
#[test]
fn parse_avi_bounds_nesting_depth() {
    // Just inside the bound: the strh is reached and counted, so the bound is not simply refusing files.
    let (shallow, ok) = parse_avi(&nested_avi(MAX_AVI_DEPTH - 1));
    assert!(
        ok && shallow.untagged_audio == 1,
        "shallow nesting: ok={ok} untaggedAudio={}, want a track found",
        shallow.untagged_audio
    );
    // Past it: the walk returns before reaching the strh, so nothing is read and nothing is claimed.
    let (deep, ok) = parse_avi(&nested_avi(MAX_AVI_DEPTH + 1));
    assert!(
        !ok && deep.untagged_audio == 0,
        "deep nesting: ok={ok} untaggedAudio={}, want the walk to have stopped",
        deep.untagged_audio
    );
    // A depth bomb the size of a real probe read returns instead of recursing tens of thousands deep.
    let (_, ok) = parse_avi(&nested_avi(80_000));
    assert!(!ok, "a depth bomb produced a probe result");
}

// A real MP4, so the box walking and mdhd's packed language are exercised against something a muxer
// actually produced.
#[test]
fn parse_head_mp4() {
    let head = testdata("sample.mp4");
    let got = parse_head(&head).expect("parse");
    assert_eq!(got.container, "mp4", "container = {:?}, want mp4", got.container);
    assert_eq!(got.video_codec, "h264", "codec = {:?}, want h264", got.video_codec);
}

// Matroska still routes correctly once the other two parsers are ahead of it.
#[test]
fn parse_head_matroska_still_wins() {
    let head = testdata("tracks.mkv");
    let got = parse_head(&head).expect("parse");
    assert!(
        got.container == "matroska" && got.audio.len() == 1 && got.audio[0] == "it",
        "got {got:?}, want matroska with [it]"
    );
}

// An unrecognised head is an honest failure, not an empty success.
#[test]
fn parse_head_unknown() {
    assert!(parse_head(b"this is not a media container").is_err(), "want an error for an unrecognised container");
}
