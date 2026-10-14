//! Go: `internal/scout/rank_test.go` (400 lines). Work package 5.
//!
//! Also holds the helpers the other ranking tests share (`rs`, `intp`, `titles`), the way `rank_test.go`
//! is where Go's `rank_show_test.go` and `extra_test.go` get them from.

use std::collections::HashSet;

use crate::label::MIB;
use crate::rank::{self as prod, RankFilters, RawStream, GIB};

pub(crate) fn rs(title: &str, mutate: impl FnOnce(&mut RawStream)) -> RawStream {
    let mut s = RawStream {
        info_hash: "h".into(),
        title: title.into(),
        source: "torrentio".into(),
        ..Default::default()
    };
    mutate(&mut s);
    s
}

pub(crate) fn intp(n: i64) -> Option<i64> {
    Some(n)
}

pub(crate) fn titles(streams: &[RawStream]) -> Vec<String> {
    streams.iter().map(|s| s.title.clone()).collect()
}

#[test]
fn junk_class() {
    let junk = [
        ("Movie 2024 HDCAM x264", "cam"),
        ("Movie 2024 hd.cam", "cam"),
        ("Movie 2024 HD-TS", "telesync"),
        ("Movie 2024 DVDScr", "screener"),
        ("Movie 2024 WORKPRINT", "workprint"),
        ("Movie 2024 AI-upscaled", "upscaled"),
        ("Movie 2024 R5", "r5"),
        ("Movie 2024 TELECINE", "telecine"),
        ("Movie 2024 cam", "cam"),
        ("Movie 2024 ts", "telesync"),
        ("Movie 2024 scr", "screener"),
    ];
    for (title, want) in junk {
        let got = prod::junk_class(title);
        assert_eq!(got, want, "junkClass({title:?})={got:?} want {want:?}");
    }
    let not_junk = [
        "Show S01E01 WEB-DL cam.crew.release",
        "Movie 2024 BluRay REMUX ts-audio",
        "Movie.2024.2160p.WEB-DL.DV.HDR.x265",
    ];
    for title in not_junk {
        let got = prod::junk_class(title);
        assert_eq!(got, "", "junkClass({title:?})={got:?} want none");
    }
}

#[test]
fn quality_score() {
    let cached = prod::quality_score(rs("Movie 720p WEB-DL", |s| s.cached = true));
    let uncached = prod::quality_score(rs("Movie 2160p REMUX", |_| {}));
    assert!(cached > uncached, "cached ({cached}) should beat uncached ({uncached})");

    let cam = prod::quality_score(rs("Movie 2160p HDCAM", |_| {}));
    let legit = prod::quality_score(rs("Movie 480p WEB-DL", |_| {}));
    assert!(cam < legit - 50000, "junk ({cam}) should be far below legit ({legit})");

    let remux = prod::quality_score(rs("Movie 1080p REMUX", |_| {}));
    let webdl = prod::quality_score(rs("Movie 1080p WEB-DL", |_| {}));
    let hdtv = prod::quality_score(rs("Movie 1080p HDTV", |_| {}));
    assert!(!(remux <= webdl || webdl <= hdtv), "expected remux({remux}) > webdl({webdl}) > hdtv({hdtv})");

    assert!(
        prod::quality_score(rs("Movie 2160p WEB-DL AV1", |_| {}))
            < prod::quality_score(rs("Movie 2160p WEB-DL", |_| {})),
        "av1 at 4k should be penalized"
    );
    assert!(
        prod::quality_score(rs("Movie 1080p BluRay 3D HSBS", |_| {}))
            < prod::quality_score(rs("Movie 1080p BluRay", |_| {})),
        "3d should be penalized"
    );
    assert!(
        prod::quality_score(rs("Movie 2160p WEB-DL DoVi", |_| {}))
            > prod::quality_score(rs("Movie 2160p WEB-DL", |_| {})),
        "DoVi should be rewarded"
    );
    assert!(
        prod::quality_score(rs("Movie 2160p REMUX Atmos", |_| {}))
            > prod::quality_score(rs("Movie 2160p REMUX", |_| {})),
        "Atmos should be rewarded"
    );
    // Ranked by what Den DELIVERS: DD+ (stream-copy) over the same title in TrueHD/DTS (bridged to 5.1),
    // and DD+ Atmos (real Atmos) over TrueHD Atmos (Atmos lost in the bridge).
    assert!(
        prod::quality_score(rs("Movie 2160p WEB-DL DDP5.1 Atmos", |_| {}))
            > prod::quality_score(rs("Movie 2160p WEB-DL TrueHD Atmos 7.1", |_| {})),
        "DD+ Atmos (delivered) should outrank TrueHD Atmos (bridged to 5.1)"
    );
    assert!(
        prod::quality_score(rs("Movie 1080p BluRay DDP5.1", |_| {}))
            > prod::quality_score(rs("Movie 1080p BluRay DTS-HD MA 5.1", |_| {})),
        "DD+ (stream-copy) should outrank DTS-HD (bridged)"
    );
    let small = prod::quality_score(rs("Movie 4K WEB", |s| s.size_bytes = Some(GIB)));
    let large = prod::quality_score(rs("Movie 4K WEB", |s| s.size_bytes = Some(20 * GIB)));
    assert!(small < large, "a tiny 4k file should score below a large one");
}

#[test]
fn junk_class_container_extension_is_not_tele_sync() {
    for title in ["Show.S01E01.ts", "Show S01E01.m2ts", "Show.S01E01.ts\n👤 12 💾 1.2 GB"] {
        let got = prod::junk_class(title);
        assert_eq!(got, "", "junkClass({title:?})={got:?} want none");
    }
    assert_eq!(prod::junk_class("Movie.2024.TS.x264"), "telesync", "a mid-name TS tag is still TeleSync");
}

#[test]
fn quality_score_bare_dv_tag_is_dolby_vision() {
    assert!(
        prod::quality_score(rs("Movie.2024.2160p.WEB-DL.DV.x265", |_| {}))
            > prod::quality_score(rs("Movie.2024.2160p.WEB-DL.x265", |_| {})),
        "a DV-tagged release should get the Dolby Vision bonus"
    );
}

#[test]
fn quality_score_tiny_2160p_ranks_as_1080p() {
    let fake = prod::quality_score(rs("Movie 2160p WEB-DL", |s| s.size_bytes = Some(300 * MIB)));
    let real1080 = prod::quality_score(rs("Movie 1080p WEB-DL", |s| s.size_bytes = Some(300 * MIB)));
    assert_eq!(fake, real1080, "a 300 MB 2160p ({fake}) should score as 1080p ({real1080})");
    assert!(
        prod::quality_score(rs("Movie 2160p WEB-DL", |s| s.size_bytes = Some(8 * GIB))) > real1080,
        "a real-sized 2160p keeps its 4K rank"
    );
}

#[test]
fn binge_group_separates_releases() {
    let a = "Show.S01E01.1080p.WEB-DL.DDP5.1.H.264-NTb";
    let b = "Show.S01E02.1080p.WEB-DL.DDP5.1.H.264-NTb.mkv";
    let c = "Show.S01E02.1080p.WEB-DL.DDP5.1.H.264-FLUX";
    let d = "Show.S01E02.2160p.WEB-DL.DV.H.265-NTb";
    let group = |title: &str| crate::label::binge_group(&title.to_lowercase(), title);
    assert_eq!(group(a), group(b), "same release across episodes should match");
    assert!(
        group(a) != group(c) && group(a) != group(d),
        "different groups or quality must not match: {} {} {}",
        group(a),
        group(c),
        group(d)
    );
    assert_eq!(group(a), "den-scout|1080p|webdl||ntb", "bingeGroup({a:?})");
}

#[test]
fn release_group() {
    let cases = [
        ("Show.S01E01.1080p.WEB-DL-NTb.mkv", "ntb"),
        ("Show - S01E01 - Pilot", ""),
        ("Movie.2024.WEB-DL-1080p.x265", ""),
        ("Movie 2024 1080p", ""),
        ("Show 1080p WEB-DL", ""),
    ];
    for (input, want) in cases {
        let got = crate::label::release_group(input);
        assert_eq!(got, want, "releaseGroup({input:?})={got:?} want {want:?}");
    }
}

#[test]
fn detect_resolution() {
    let cases = [("x 2160p y", "2160p"), ("x 576p y", "480p"), ("x 720p y", "720p"), ("no res here", "")];
    for (input, want) in cases {
        let got = prod::detect_resolution(input);
        assert_eq!(got, want, "detectResolution({input:?})={got:?} want {want:?}");
    }
}

#[test]
fn rank_streams() {
    // CacheKnown on all three: the check ran and answered for each of them. cachedOnly drops what is
    // KNOWN not to be cached — see below for what it must not drop.
    let streams = [
        rs("A 1080p WEB-DL", |s| {
            s.cached = true;
            s.cache_known = true;
        }),
        rs("B 2160p HDCAM", |s| {
            s.cached = true;
            s.cache_known = true;
        }),
        rs("C 1080p WEB-DL", |s| s.cache_known = true),
    ];
    let ranked = prod::rank_streams(
        &streams,
        RankFilters { exclude_cam: true, cached_only: true, result_cap: 5, ..Default::default() },
    );
    assert!(
        ranked.len() == 1 && ranked[0].title == "A 1080p WEB-DL",
        "excludeCam+cachedOnly: got {:?}",
        titles(&ranked)
    );

    // A release nobody could CHECK is not a release known to be uncached. Cache checks batch, so one
    // failed batch leaves a hundred unknown while the rest of the list is perfectly well known — and
    // dropping those removed up to a hundred playable releases with nothing anywhere saying so.
    let unchecked = [
        rs("A 1080p WEB-DL", |s| {
            s.cached = true;
            s.cache_known = true;
        }),
        rs("B 1080p WEB-DL", |s| s.cache_known = true), // known: not cached
        rs("C 1080p WEB-DL", |s| s.cache_known = false), // its batch failed
    ];
    let kept_unchecked = prod::rank_streams(
        &unchecked,
        RankFilters { cached_only: true, result_cap: 5, ..Default::default() },
    );
    assert_eq!(
        kept_unchecked.len(),
        2,
        "cachedOnly dropped a release nobody could check: {:?}",
        titles(&kept_unchecked)
    );

    // hdrOnly keeps only HDR/DV.
    let hdr = prod::rank_streams(
        &[rs("A 2160p WEB HDR", |s| s.cached = true), rs("B 2160p WEB", |s| s.cached = true)],
        RankFilters {
            exclude_cam: true,
            cached_only: true,
            hdr_only: true,
            result_cap: 5,
            ..Default::default()
        },
    );
    assert!(hdr.len() == 1 && hdr[0].title == "A 2160p WEB HDR", "hdrOnly: got {:?}", titles(&hdr));

    // seeders break ties.
    let tie = prod::rank_streams(
        &[
            rs("Movie 1080p WEB-DL", |s| {
                s.cached = true;
                s.seeders = intp(5);
            }),
            rs("Movie 1080p WEB-DL", |s| {
                s.cached = true;
                s.seeders = intp(500);
            }),
        ],
        RankFilters { exclude_cam: true, cached_only: true, result_cap: 5, ..Default::default() },
    );
    assert_eq!(
        tie[0].seeders.unwrap_or(0),
        500,
        "seeders tiebreak: first has {} want 500",
        tie[0].seeders.unwrap_or(0)
    );

    // resolutions keep untagged; minSeeders/maxSizeGB/excludeRegex apply.
    let filtered = prod::rank_streams(
        &[
            rs("X 2160p WEB HDR", |s| {
                s.cached = true;
                s.seeders = intp(10);
                s.size_bytes = intp(30 * GIB);
            }),
            rs("Y 1080p WEB", |s| {
                s.cached = true;
                s.seeders = intp(10);
                s.size_bytes = intp(8 * GIB);
            }),
            rs("Z untitled release", |s| {
                s.cached = true;
                s.seeders = intp(10);
                s.size_bytes = intp(2 * GIB);
            }),
            rs("W 2160p WEB", |s| {
                s.cached = true;
                s.seeders = intp(0);
                s.size_bytes = intp(5 * GIB);
            }),
            rs("U 2160p WEB", |s| {
                s.cached = true;
                s.seeders = intp(10);
                s.size_bytes = intp(90 * GIB);
            }),
            rs("V 2160p WEB YIFY", |s| {
                s.cached = true;
                s.seeders = intp(10);
                s.size_bytes = intp(5 * GIB);
            }),
        ],
        RankFilters {
            exclude_cam: true,
            cached_only: true,
            result_cap: 20,
            resolutions: vec!["2160p".into()],
            min_seeders: intp(1),
            max_size_gb: intp(40),
            exclude_regex: "yify".into(),
            ..Default::default()
        },
    );
    let got: HashSet<&str> = filtered.iter().map(|s| s.title.as_str()).collect();
    assert!(
        filtered.len() == 2 && got.contains("X 2160p WEB HDR") && got.contains("Z untitled release"),
        "combined filters: got {:?}",
        titles(&filtered)
    );

    // MaxSizeGB is a budget on what this stream actually plays, not on the season pack it came from. A
    // probed episode (Fauda S1E3: a 68 GB pack, a 1.1 GB file) must clear a 20 GB cap on its OWN size —
    // rankStreams never reads PackSizeBytes, so a stale, unrelated pack figure sitting beside it changes
    // nothing.
    let pack = 68 * GIB;
    let episode: i64 = 1_136_580_921;
    let by_size = prod::rank_streams(
        &[rs("Fauda S01E03 1080p WEB-DL", |s| {
            s.cached = true;
            s.seeders = intp(10);
            s.size_bytes = Some(episode);
            s.pack_size_bytes = Some(pack);
        })],
        RankFilters {
            exclude_cam: true,
            cached_only: true,
            result_cap: 5,
            max_size_gb: intp(20),
            ..Default::default()
        },
    );
    assert_eq!(
        by_size.len(),
        1,
        "maxSizeGB judged a probed episode by its pack's weight: got {:?}",
        titles(&by_size)
    );

    // malformed excludeRegex is ignored, not fatal.
    let ok = prod::rank_streams(
        &[rs("A 1080p WEB-DL", |s| s.cached = true)],
        RankFilters {
            exclude_cam: true,
            cached_only: true,
            result_cap: 5,
            exclude_regex: "(".into(),
            ..Default::default()
        },
    );
    assert_eq!(ok.len(), 1, "bad excludeRegex should be ignored, got {}", ok.len());
}

#[test]
fn real_debrid_blocked() {
    let blocked = [
        "Movie.2024.WEB-DL.x265",
        "Show.WEBRip.720p",
        "X.BDRip.1080p",
        "Y.HDRip",
        "Z.DVDRip",
        "Movie.2024.BluRay.x264-GRP",
        "Show.HDTV.x264",
        "Show.HDTV.XviD",
        "Clip.WEB.x264",
        "Clip.WEB.h264",
    ];
    for tt in blocked {
        assert!(prod::real_debrid_blocked(tt), "realDebridBlocked({tt:?}) should be true");
    }
    let clean = ["Movie.2024.2160p.BluRay.REMUX.HEVC", "Movie.2024.WEB.x265", "Movie 2024 BluRay x264"];
    for tt in clean {
        assert!(!prod::real_debrid_blocked(tt), "realDebridBlocked({tt:?}) should be false");
    }
}

// The seed cap keeps the BEST releases, not the first ones the scrape happened to return.
//
// The cap bounds the debrid cache-check fan-out, which is right — but it used to trim in scrape order.
// Past the cap a 2160p REMUX sitting at the tail was dropped while cam junk at the head survived, and
// neither the viewer nor the log saw it happen.
#[test]
fn cap_seeds_keeps_the_best_not_the_first() {
    let mut streams = Vec::new();
    for i in 0..40 {
        // junk arrives first, as an indexer's own order well might
        streams.push(RawStream {
            info_hash: format!("{i:040x}"),
            title: format!("Some.Film.2024.CAM.XviD-JUNK.{i}.mkv"),
            seeders: intp(1),
            ..Default::default()
        });
    }
    let gem = RawStream {
        info_hash: "f".repeat(40),
        title: "Some.Film.2024.2160p.UHD.BluRay.REMUX.HDR.mkv".into(),
        seeders: intp(50),
        ..Default::default()
    };
    streams.push(gem.clone());

    let kept = prod::cap_seeds(&streams, 10);
    assert_eq!(kept.len(), 10, "capped to {}, want 10", kept.len());
    let found = kept.iter().any(|s| s.info_hash == gem.info_hash);
    assert!(found, "the best release was dropped and forty cam rips were kept");
}

// Under the cap nothing is touched — not reordered, not dropped. The real ranking runs afterwards and
// owns the order the viewer sees.
#[test]
fn cap_seeds_leaves_a_small_list_alone() {
    let streams = [
        RawStream { info_hash: "a".into(), title: "Some.Film.2024.CAM.mkv".into(), ..Default::default() },
        RawStream {
            info_hash: "b".into(),
            title: "Some.Film.2024.2160p.REMUX.mkv".into(),
            ..Default::default()
        },
    ];
    let kept = prod::cap_seeds(&streams, 10);
    assert!(
        kept.len() == 2 && kept[0].info_hash == "a" && kept[1].info_hash == "b",
        "a list under the cap was altered: {kept:?}"
    );
    assert_eq!(prod::cap_seeds(&streams, 0).len(), 2, "a non-positive cap must not drop anything");
}

// Equal quality falls back to seeders — between two identical-looking releases, the one more people are
// sharing is the one that will actually download.
#[test]
fn cap_seeds_breaks_ties_on_seeders() {
    let streams = [
        RawStream {
            info_hash: "low".into(),
            title: "Some.Film.2024.1080p.WEB-DL.mkv".into(),
            seeders: intp(2),
            ..Default::default()
        },
        RawStream {
            info_hash: "high".into(),
            title: "Some.Film.2024.1080p.WEB-DL.mkv".into(),
            seeders: intp(900),
            ..Default::default()
        },
    ];
    let kept = prod::cap_seeds(&streams, 1);
    assert!(kept.len() == 1 && kept[0].info_hash == "high", "tie not broken on seeders: {kept:?}");
}

// A preference RANKS, it does not filter. Every other knob in Filters is a hard drop, which is the wrong
// shape for a taste: someone who filters to 1080p to save bandwidth would still rather see a 4K remux
// than an empty list when that is all anybody is seeding.
#[test]
fn rank_streams_prefer_resolution_ranks_without_dropping() {
    let streams = [
        rs("Film.2024.2160p.BluRay.REMUX.HDR.mkv", |_| {}),
        rs("Film.2024.1080p.WEB-DL.mkv", |_| {}),
        rs("Film.2024.720p.WEB-DL.mkv", |_| {}),
    ];
    let got = prod::rank_streams(
        &streams,
        RankFilters { prefer_resolution: "1080p".into(), result_cap: 20, ..Default::default() },
    );
    assert_eq!(got.len(), 3, "a preference dropped releases: kept {} of 3", got.len());
    assert_eq!(
        prod::detect_resolution(&got[0].title),
        "1080p",
        "preferred resolution did not rank first: {:?}",
        got[0].title
    );
    // …and below it, the normal quality order is untouched: the 4K remux still beats the 720p.
    assert_eq!(
        prod::detect_resolution(&got[1].title),
        "2160p",
        "non-preferred releases lost their own ordering: {:?} then {:?}",
        got[1].title,
        got[2].title
    );
}

// The sink must clear every quality signal — otherwise "prefer 1080p" loses to a big 4K remux and means
// nothing — while staying under cachedness, which is a fact about whether it plays now rather than taste.
#[test]
fn rank_streams_preference_outweighs_quality_but_not_cached() {
    let best_4k = rs("Film.2024.2160p.BluRay.REMUX.HDR.DDP5.1.Atmos.mkv", |s| {
        s.size_bytes = intp(80 * GIB); // maximum size bonus, on top of every other quality signal
    });
    let plain_1080 = rs("Film.2024.1080p.WEB-DL.mkv", |_| {});

    let got = prod::rank_streams(
        &[best_4k.clone(), plain_1080.clone()],
        RankFilters { prefer_resolution: "1080p".into(), result_cap: 20, ..Default::default() },
    );
    assert_eq!(
        prod::detect_resolution(&got[0].title),
        "1080p",
        "the richest possible 4K outranked the preference: {:?}",
        got[0].title
    );

    // Cached still wins: waiting for a download is worse than a resolution you did not ask for.
    let mut cached_4k = best_4k.clone();
    cached_4k.cached = true;
    let got = prod::rank_streams(
        &[cached_4k, plain_1080.clone()],
        RankFilters { prefer_resolution: "1080p".into(), result_cap: 20, ..Default::default() },
    );
    assert_eq!(
        prod::detect_resolution(&got[0].title),
        "2160p",
        "an uncached preference outranked a cached release: {:?}",
        got[0].title
    );
}

// An unreadable resolution is not sunk. Every hard filter here keeps what it cannot measure — the
// whitelist keeps untagged releases, minSeeders keeps unknown counts, maxSizeGB keeps unknown sizes — and
// punishing an unknown would assert something nobody measured.
#[test]
fn rank_streams_preference_keeps_untagged_resolution_neutral() {
    let untagged = rs("Film.2024.WEB-DL.mkv", |_| {});
    let other = rs("Film.2024.720p.WEB-DL.mkv", |_| {});
    let got = prod::rank_streams(
        &[other, untagged.clone()],
        RankFilters { prefer_resolution: "1080p".into(), result_cap: 20, ..Default::default() },
    );
    assert_eq!(got.len(), 2, "kept {} of 2", got.len());
    assert_eq!(
        got[0].title, untagged.title,
        "an untagged release was sunk alongside the non-preferred ones: {:?} first",
        got[0].title
    );
}

// A preference must never lift junk. The CAM sink is orders of magnitude larger for a reason.
#[test]
fn rank_streams_preference_never_lifts_junk() {
    let cam = rs("Film.2024.1080p.HDCAM.x264.mkv", |_| {});
    let legit = rs("Film.2024.2160p.WEB-DL.mkv", |_| {});
    let got = prod::rank_streams(
        &[cam.clone(), legit],
        RankFilters { prefer_resolution: "1080p".into(), result_cap: 20, ..Default::default() },
    );
    assert_ne!(got[0].title, cam.title, "a preference lifted a CAM above a legitimate release");
}

// Not in the Go suite. The Go engines read `\d`, `\w`, `\s` and `\b` one way (RE2: ASCII only; regexp2: .NET's
// classes) and the Rust ones another by default, so these pin the spellings that make them agree.

#[test]
fn re2_patterns_read_ascii_classes() {
    // `19\d\d` is RE2's yearToken: Arabic-Indic digits are not digits to it.
    assert!(
        prod::release_years("Movie 19\u{669}\u{669} 1080p").is_empty(),
        "a non-ASCII digit was read as a year"
    );
    // RE2's `\s` is [\t\n\f\r ]: a vertical tab does not end `.ts` as a container extension, so the TS tag stands.
    assert_eq!(prod::junk_class("Show.S01E01.ts\u{b}"), "telesync", "a vertical tab was read as whitespace");
    assert_eq!(prod::junk_class("Show.S01E01.ts\n"), "", "a newline ends a container extension");
    // The same pair, through a pattern that is Unicode for its `.` and ASCII for the rest: Opus counts only once
    // the technical half of the name has begun.
    let opus = |title: &str| crate::attributes::detect_audio_codec(&title.to_lowercase());
    assert_eq!(opus("mr.hollands.opus.1995.1080p"), "", "a film called Opus is not Opus audio");
    assert_eq!(opus("show.1080p.web-dl.opus.5.1"), "opus");
    assert_eq!(opus("show.\u{661}\u{660}\u{668}\u{660}p.opus"), "", "non-ASCII digits are not a resolution");
}

#[test]
fn a_users_exclude_regex_reads_as_re2_does() {
    let kept = |pattern: &str, title: &str| {
        let out = prod::rank_streams(
            &[rs(title, |_| {})],
            RankFilters { exclude_regex: pattern.into(), result_cap: 10, ..Default::default() },
        );
        !out.is_empty()
    };
    // `\d`, `\w`, `\s`, `\b` and their negations are ASCII-only, in a class and out of one.
    assert!(kept(r"x\d", "Movie x\u{663} WEB"), r"\d matched a non-ASCII digit");
    assert!(!kept(r"x\d", "Movie x3 WEB"));
    assert!(kept(r"[\d]x", "Movie \u{663}x WEB"), r"[\d] matched a non-ASCII digit");
    assert!(kept(r"movie\wweb", "Movie\u{e9}WEB"), r"\w matched a letter outside ASCII");
    assert!(!kept(r"movie\Wweb", "Movie\u{e9}WEB"), r"\W did not match a letter outside ASCII");
    assert!(kept(r"movie\sweb", "Movie\u{a0}WEB"), r"\s matched a no-break space");
    assert!(!kept(r"movie\Sweb", "Movie\u{a0}WEB"), r"\S did not match a no-break space");
    assert!(!kept(r"\bcam\b", "Movie cam\u{e9} WEB"), r"\b took a letter outside ASCII as a word character");
    assert!(!kept(r"[^\w]cam", "Movie \u{e9}cam WEB"), r"[^\w] did not take a letter outside ASCII");
    // `.` is a whole codepoint, as in RE2.
    assert!(!kept(r"^movie .$", "Movie \u{e9}"), "`.` did not match one codepoint");
    // Syntax RE2 has and the regex crate does not.
    assert!(!kept(r"\Qa.b\E", "Movie a.b WEB"), r"\Q..\E was not read as a literal");
    assert!(kept(r"\Qa.b\E", "Movie axb WEB"), r"\Q..\E matched a wildcard");
    assert!(!kept(r"[a[]b", "Movie [b WEB"), "a `[` inside a class was read as a nested class");
    assert!(!kept(r"[[:alpha:]\d]9", "Movie a9 WEB"), "a POSIX class inside a class broke the translation");
    assert!(!kept(r"[a&&b]", "Movie & WE"), "`&&` inside a class was read as a set operation");
    // A pattern that does not compile is ignored, not an error.
    assert!(kept("(", "Movie WEB"));
}

#[test]
fn net_patterns_read_as_regexp2_does() {
    use crate::season::{bare_episode_patterns, matches_episode, names_episode};

    // .NET's `$` also matches before a final newline.
    assert!(
        matches_episode("Show - 05\n", &bare_episode_patterns(5)),
        "`$` did not match before a final newline"
    );
    assert!(!matches_episode("Show - 05\n\n", &bare_episode_patterns(5)), "`$` matched before two newlines");
    // `\b` is .NET's `\w`: a non-spacing mark (Mn) is a word character, a spacing mark (Mc) and a letter number
    // (Nl) are not, though the regex crate's `\w` takes both.
    let patterns = crate::season::episode_patterns(1, 2);
    assert!(!matches_episode("e\u{301}102", &patterns), "a non-spacing mark ended the word");
    assert!(matches_episode("\u{903}102", &patterns), "a spacing mark was a word character");
    assert!(matches_episode("\u{2167}102", &patterns), "a letter number was a word character");
    assert!(names_episode("Show.S01E02.mkv", 1, 2));
}

// regexp2 has no backtrack limit and fancy-regex stops at a million, reporting an error that reads as "no match".
// An indexer title is as long as the response it came in (2 MiB), so none of these may fall off that edge.
#[test]
fn lookaround_patterns_answer_for_a_title_of_any_length() {
    use crate::season::{episode_patterns, matches_episode, names_episode, unresolved_season_pack};

    let pad = "a ".repeat(1 << 20);
    assert_eq!(
        prod::junk_class(&format!("{pad} upscaled")),
        "upscaled",
        "a long title escaped the upscaled check"
    );
    let pad = "a ".repeat(1 << 17);
    assert!(names_episode(&format!("{pad} S01E02"), 1, 2), "a long name stopped naming its episode");
    assert!(matches_episode(&format!("{pad} S01E02"), &episode_patterns(1, 2)));
    assert!(names_episode(&format!("{pad} S01E01-06"), 1, 4), "a long name stopped holding its range");
    assert!(unresolved_season_pack(&format!("{pad} S01E01-06"), 1, 4));
    let chain = "E01".repeat(1 << 17);
    assert!(names_episode(&format!("S01{chain}"), 1, 1));
}
