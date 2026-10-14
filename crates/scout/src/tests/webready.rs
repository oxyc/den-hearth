//! Go: `internal/scout/webready_test.go` (29 lines). Work package 5.
//!
//! `webReady` is defined in Go's `handler.go` (line 2190), not `client.go`. PORTING.md assigns this file to
//! `client.rs`, so the Rust stub lives there for now; the handler porter may move it.

use crate::probe::Probe;
use crate::rank::RawStream;

// notWebReady follows Stremio's definition: only an https URL to an MP4 is web-ready, and the probed
// container wins over the release name.
#[test]
fn web_ready() {
    let mkv = Probe { container: "matroska".into(), ..Default::default() };
    let mp4 = Probe { container: "mp4".into(), ..Default::default() };
    let named = |title: &str| RawStream { title: title.into(), ..Default::default() };
    let cases: [(&str, RawStream, &str, bool); 8] = [
        ("https mp4 by name", named("Movie.2020.1080p.WEB-DL.AAC.mp4"), "https://s/p/t", true),
        ("https m4v by name", named("Movie.m4v"), "https://s/p/t", true),
        ("https mkv by name", named("Movie.2020.2160p.REMUX.TrueHD.mkv"), "https://s/p/t", false),
        ("no extension", named("Movie 2020 1080p WEB-DL"), "https://s/p/t", false),
        ("http mp4", named("Movie.mp4"), "http://192.168.86.193:8080/p/t", false),
        (
            "probe says mkv despite .mp4 name",
            RawStream { probe: Some(mkv), ..named("Movie.mp4") },
            "https://s/p/t",
            false,
        ),
        (
            "probe says mp4 without an extension",
            RawStream { probe: Some(mp4), ..named("Movie 1080p") },
            "https://s/p/t",
            true,
        ),
        (
            "probe with no container falls back to the name",
            RawStream { probe: Some(Probe::default()), ..named("Movie.mp4") },
            "https://s/p/t",
            true,
        ),
    ];
    for (name, s, url, ready) in cases {
        let got = crate::client::web_ready(&s, url);
        assert_eq!(got, ready, "{name}: webReady = {got}, want {ready}");
    }
}
