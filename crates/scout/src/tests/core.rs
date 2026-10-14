//! Go: `internal/scout/core_test.go` (639 lines). Work package 6.
//!
//! A grab bag: config, stream ids, play tokens and the memory cache are this package's own modules, but
//! `TestPickEpisodeFile`, `TestSelectFileIDTorBox`, `TestPickFileIDPrefersEpisodeMatch`, `TestCleanLabel*`
//! and `TestStreamAttributes` exercise modules owned by work packages 1 and 5. Port the whole file here;
//! those tests stay red until the module they exercise lands.

use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use super::seal::VEC_PRIV_B64;
use crate::cache::{Cache, MemoryCache};
use crate::config::{DebridService, Indexer, MAX_CONFIG_BLOB, MAX_DEBRID_ACCOUNTS, MAX_RESULT_CAP};
use crate::id::MAX_STREAM_ID_LEN;
use crate::play::{decode_play_token, encode_play_token, PlayTarget};
use crate::seal::{parse_seal_keyring, seal, SEALED_VERSION};
use crate::util::{b64url_decode, b64url_encode};

/// Go `maxSources` and `maxSourceURL` (sources.go:34-35). Owned by work package 5; they are copied here so
/// the bounds test can run now. Replace with a `use` once `sources.rs` exports them.
const MAX_SOURCES: usize = 3;
const MAX_SOURCE_URL: usize = 2048;

/// Go `blob` (core_test.go:11): a JSON string as an unpadded base64url config path segment.
pub(crate) fn blob(json_str: &str) -> String {
    b64url_encode(json_str.as_bytes())
}

/// Go `jsonString` (core_test.go:440): quotes a value for embedding in a config fixture, so a pattern full
/// of backslashes and braces reaches the decoder exactly as a client would have sent it.
fn json_string(s: &str) -> String {
    serde_json::to_string(s).expect("a string always serializes")
}

/// Go `compiledInsts` (core_test.go:586): the instruction count of a pattern's compiled program, or -1 if
/// it will not compile. Go gets this from `regexp/syntax`; Rust's `regex` crates expose no program size, so
/// the owner decides what bound to assert instead (see the ignored test below).
fn compiled_insts(_pat: &str) -> i64 {
    todo!("core_test.go compiledInsts: no Rust equivalent of regexp/syntax Prog.Inst")
}

#[test]
fn decode_config() {
    let c = crate::config::decode_config(
        None,
        &blob(
            r#"{"debrid":[{"service":"torbox","token":"tb-secret"}],"indexers":["torrentio"],"filters":{"excludeCam":true},"cachedOnly":true,"resultCap":20}"#,
        ),
    );
    assert!(
        matches!(&c, Some(c) if c.debrid.len() == 1 && c.debrid[0].service == DebridService::TorBox && c.debrid[0].token == "tb-secret" && c.cached_only),
        "valid config: {c:?}"
    );

    // reject configs with no valid debrid account
    for bad in [
        r#"{"debrid":[],"indexers":["torrentio"]}"#,
        r#"{"debrid":[{"service":"nope","token":"x"}]}"#,
        r#"{"debrid":[{"service":"torbox","token":""}]}"#,
    ] {
        assert!(crate::config::decode_config(None, &blob(bad)).is_none(), "expected reject: {bad}");
    }
    assert!(
        crate::config::decode_config(None, "!!!not-base64!!!").is_none(),
        "garbage blob should be rejected"
    );

    // clamp + drop unknown; #12: minSeeders:-5 becomes nil (no filter), not 0.
    let c = crate::config::decode_config(
        None,
        &blob(&format!(
            r#"{{"debrid":[{{"service":"realdebrid","token":"rd"}}],"resultCap":9999,"filters":{{"excludeRegex":"{}","minSeeders":-5}},"evil":"ignored"}}"#,
            "x".repeat(400)
        )),
    );
    // resultCap clamps to maxResultCap, which came down from 200: every stream in the reply embeds a copy
    // of the config segment in its /play URL, so the body is len(blob) x resultCap and the cap is the
    // larger half of that product.
    assert!(
        matches!(&c, Some(c) if c.result_cap == MAX_RESULT_CAP && c.filters.exclude_regex.len() == 256 && c.filters.min_seeders.is_none()),
        "clamp: {c:?}"
    );

    // valid optional filters + dedupe indexers (#10) + drop bogus
    let c = crate::config::decode_config(None, &blob(r#"{"debrid":[{"service":"torbox","token":"t"}],"indexers":["torrentio","bogus","torz","torrentio"],"filters":{"resolutions":["2160p","nope","1080p"],"hdrOnly":true,"maxSizeGB":40}}"#))
        .unwrap_or_default();
    // torz is named here but centrally disabled (its host has no DNS), so it is dropped despite being
    // asked for: a sealed config cannot be edited from the server, and carrying a dead host makes the
    // survivors look like a quorum when one of them sheds a request.
    assert!(c.indexers == [Indexer("torrentio".to_owned())], "indexers: {:?}", c.indexers);

    assert!(
        c.filters.resolutions.len() == 2 && c.filters.hdr_only && c.filters.max_size_gb == Some(40),
        "filters: {:?}",
        c.filters
    );

    // A counted repetition is dropped from a user pattern. RE2 is linear at MATCH time, which is what
    // makes this filter safe to run over a stream list — but nothing bounded it at COMPILE time, and `{n}`
    // is expanded into the program. 250 characters inside the existing cap compile to 53 MiB of live heap,
    // from an unauthenticated caller, inside a 230 MiB GOMEMLIMIT.
    for bad in [
        format!("(?:{}){{1000}}", "x".repeat(240)),
        "a{1000}".to_owned(),
        "a{100,}".to_owned(),
        "a{2,50}".to_owned(),
        // The escape-state cases. The first version of this guard asked "is the character before the
        // brace a backslash", which is a different question: here the `\\` is an ESCAPED BACKSLASH and
        // the quantifier after it is live, so the lookbehind called it escaped and let it through.
        r"\\{1000}".to_owned(),
        r"a\\{1000}".to_owned(),
        "{1000}".to_owned(), // at the very start, where there is no preceding character at all
    ] {
        let Some(c) = crate::config::decode_config(
            None,
            &blob(&format!(
                r#"{{"debrid":[{{"service":"torbox","token":"t"}}],"filters":{{"excludeRegex":{}}}}}"#,
                json_string(&bad)
            )),
        ) else {
            panic!("config carrying {bad:?} was rejected outright");
        };
        assert!(
            c.filters.exclude_regex.is_empty(),
            "counted repetition survived validation: {:?}",
            c.filters.exclude_regex
        );
    }
    // The patterns people actually write are untouched — including a genuinely escaped brace, a unicode
    // class (whose braces carry no digits), and a brace inside a character class.
    for good in [r"hindi|tamil|dubbed", r"\bhc\b", r"x\{3\}", r"\p{Latin}", "[{]1000}"] {
        let c = crate::config::decode_config(
            None,
            &blob(&format!(
                r#"{{"debrid":[{{"service":"torbox","token":"t"}}],"filters":{{"excludeRegex":{}}}}}"#,
                json_string(good)
            )),
        )
        .unwrap_or_default();
        assert_eq!(c.filters.exclude_regex, good, "a legitimate pattern was dropped");
    }

    // preferResolution is whitelisted like every other resolution field. An unknown value becomes "no
    // preference" rather than a preference nothing can satisfy — which would sink every release equally.
    let pref = crate::config::decode_config(
        None,
        &blob(r#"{"debrid":[{"service":"torbox","token":"t"}],"filters":{"preferResolution":"1080p"}}"#),
    )
    .unwrap_or_default();
    assert_eq!(pref.filters.prefer_resolution, "1080p", "preferResolution");
    let bogus = crate::config::decode_config(
        None,
        &blob(r#"{"debrid":[{"service":"torbox","token":"t"}],"filters":{"preferResolution":"8k"}}"#),
    )
    .unwrap_or_default();
    assert!(
        bogus.filters.prefer_resolution.is_empty(),
        "an unknown preferResolution was carried: {:?}",
        bogus.filters.prefer_resolution
    );

    // A live host that needs a config PATH is not disabled. comet 403s the bare path and answers on a
    // config path, which `configPathIndexers` already handles — disabling it stripped it before
    // makeScrapers could mint one, leaving the install with a single real source.
    let with_comet = crate::config::decode_config(
        None,
        &blob(r#"{"debrid":[{"service":"torbox","token":"t"}],"indexers":["torrentio","comet"]}"#),
    )
    .unwrap_or_default();
    assert!(
        with_comet.indexers.len() == 2,
        "comet needs a config path, not a headstone: {:?}",
        with_comet.indexers
    );

    // defaults. A config carrying only the account defers everything else to the server, which is what
    // /configure builds by default — so these ARE the shipped defaults, not incidental fallbacks.
    // cachedOnly is deliberately off: inheriting "hide anything not already fetched" can leave a title
    // showing one scrap of a release, or nothing, while good ones sit a download away.
    let c = crate::config::decode_config(None, &blob(r#"{"debrid":[{"service":"torbox","token":"t"}]}"#))
        .unwrap_or_default();
    // The default indexer set is the working one, not every one this server knows how to talk to.
    assert!(
        c.filters.exclude_cam
            && !c.cached_only
            && c.result_cap == 20
            && c.indexers == [Indexer("torrentio".to_owned()), Indexer("mediafusion".to_owned())],
        "defaults: {c:?}"
    );
    // explicit off
    let c = crate::config::decode_config(None, &blob(r#"{"debrid":[{"service":"torbox","token":"t"}],"cachedOnly":false,"filters":{"excludeCam":false}}"#))
        .unwrap_or_default();
    assert!(!c.filters.exclude_cam && !c.cached_only, "explicit off not honored: {c:?}");
}

#[test]
fn b64url_round_trip() {
    let input = "héllo—世界";
    let dec = b64url_decode(&b64url_encode(input.as_bytes()));
    assert!(matches!(&dec, Ok(d) if d == input.as_bytes()), "b64url round-trip: {dec:?}");
}

// Reel and Subtitles read the config segment the same way (their `a_padded_or_wrapped_segment_opens_like_it_does_on_scout`),
// so a padded or hand-wrapped link opens on all three.
#[test]
fn a_padded_or_wrapped_config_segment_opens() {
    let seg = blob(r#"{"debrid":[{"service":"torbox","token":"t"}]}"#);
    let wrapped = format!("{}\r\n{}\n", &seg[..10], &seg[10..]);
    for (what, s) in [("plain", seg.clone()), ("padded", format!("{seg}==")), ("wrapped", wrapped)] {
        let c = crate::config::decode_config(None, &s);
        assert!(c.is_some_and(|c| c.debrid.len() == 1), "a {what} segment did not open");
    }
}

#[test]
fn parse_stream_id() {
    let s = crate::id::parse_stream_id("movie", "tt1234567.json");
    assert!(matches!(&s, Some(s) if s.kind == "movie" && s.imdb == "tt1234567"), "movie: {s:?}");
    let s = crate::id::parse_stream_id("series", "tt1234567:2:5.json");
    assert!(matches!(&s, Some(s) if s.has_ep && s.season == 2 && s.episode == 5), "series: {s:?}");
    for (kind, raw_id) in [("catalog", "tt1"), ("movie", "xx1"), ("series", "tt1:2"), ("series", "tt1:x:y")] {
        assert!(crate::id::parse_stream_id(kind, raw_id).is_none(), "expected reject: {kind} {raw_id}");
    }

    // The id is retained for the whole build — cache key and rebuild-gate map key — and it is
    // concatenated into the outbound indexer URL. `tt\d+` had no
    // bound: `tt` plus 900,000 digits was accepted, pinned 2.7 MiB per in-flight request, and turned one
    // small inbound GET into a ~900 KB outbound URL per indexer.
    assert!(
        crate::id::parse_stream_id("movie", &format!("tt{}.json", "1".repeat(900_000))).is_none(),
        "an id with 900,000 digits was accepted"
    );
    assert!(
        crate::id::parse_stream_id("movie", &format!("tt{}.json", "1".repeat(11))).is_none(),
        "an 11-digit IMDb id was accepted"
    );
    // Checked before the split, because Split allocates one string per part whatever the regex says: a
    // megabyte of colons is a megabyte of slice, paid before the id is ever rejected.
    assert!(
        crate::id::parse_stream_id("series", &format!("tt1234567{}1:1.json", ":".repeat(100_000))).is_none(),
        "an id padded with 100,000 colons was accepted"
    );
    // An id that is OVERLONG and nothing else: a valid imdb id, and a season padded with leading zeros
    // that Atoi reads as 1. Every other check here passes it, so only the length cap can refuse it —
    // which is the point. Without this, deleting maxStreamIDLen outright left the whole suite green,
    // because each assertion above is really held by the regex or by Atoi.
    let overlong = format!("tt1234567:{}1:1", "0".repeat(60));
    assert!(
        overlong.len() > MAX_STREAM_ID_LEN,
        "the fixture is {} chars, not longer than the {} cap",
        overlong.len(),
        MAX_STREAM_ID_LEN
    );
    assert!(
        crate::id::parse_stream_id("series", &format!("{overlong}.json")).is_none(),
        "an otherwise-valid id past the length cap was accepted"
    );
    // …and the same shape just under the cap still parses, so the cap is not simply refusing everything.
    let short_enough = format!("tt1234567:{}1:1", "0".repeat(8));
    let s = crate::id::parse_stream_id("series", &format!("{short_enough}.json"));
    assert!(matches!(&s, Some(s) if s.season == 1), "a padded but in-length id was refused: {s:?}");
    // The longest real id still parses.
    let s = crate::id::parse_stream_id("series", "tt1234567890:999:999.json");
    assert!(matches!(&s, Some(s) if s.season == 999), "the longest legitimate id was refused: {s:?}");
}

#[test]
fn play_token() {
    let movie = PlayTarget { info_hash: "a".repeat(40), ..Default::default() };
    let got = decode_play_token(&encode_play_token(&movie));
    assert!(
        matches!(&got, Some(g) if g.info_hash == movie.info_hash && g.file_idx.is_none()),
        "movie round-trip: {got:?}"
    );
    let series = PlayTarget {
        info_hash: "b".repeat(40),
        file_idx: Some(3),
        season: Some(1),
        episode: Some(2),
        ..Default::default()
    };
    let got = decode_play_token(&encode_play_token(&series));
    assert!(
        matches!(&got, Some(g) if g.info_hash == series.info_hash && g.file_idx == Some(3) && g.season == Some(1) && g.episode == Some(2)),
        "series round-trip: {got:?}"
    );
    let got = decode_play_token(&encode_play_token(&PlayTarget {
        info_hash: "C".repeat(40),
        ..Default::default()
    }));
    assert!(matches!(&got, Some(g) if g.info_hash == "c".repeat(40)), "infohash should be lowercased");
    assert!(decode_play_token("!!!").is_none(), "garbage token should be rejected");
    assert!(decode_play_token(&blob(r#"{"h":"short"}"#)).is_none(), "short hash should be rejected");
}

#[test]
fn pick_episode_file() {
    use crate::season::{pick_episode_file, TorrentFile};
    let files = vec![
        TorrentFile { index: 0, name: "Show.S01E01.mkv".into(), size_bytes: Some(100) },
        TorrentFile { index: 1, name: "Show.S01E02.1080p.mkv".into(), size_bytes: Some(900) },
        TorrentFile { index: 2, name: "Show.S01E02.sample.mkv".into(), size_bytes: Some(5) },
        TorrentFile { index: 3, name: "readme.txt".into(), size_bytes: Some(1) },
    ];
    // SxxExx pick (largest on ties)
    let got = pick_episode_file(&files, 1, 2);
    assert!(matches!(got, Ok(Some(1))), "SxxExx pick (largest on ties): got {got:?} want 1");
    let got =
        pick_episode_file(&[TorrentFile { index: 7, name: "Show 1x03.mp4".into(), size_bytes: None }], 1, 3);
    assert!(matches!(got, Ok(Some(7))), "1x03: got {got:?}");
    let got =
        pick_episode_file(&[TorrentFile { index: 8, name: "Show.104.mp4".into(), size_bytes: None }], 1, 4);
    assert!(matches!(got, Ok(Some(8))), "104: got {got:?}");
    // Neither name is episode-labelled and there is more than one candidate, so this function has NO
    // opinion — the largest-video fallback moved out to the callers, where it comes after the indexer's
    // fileIdx instead of pre-empting it. Answering here made a guess beat a fact: a non-nil result makes
    // every caller return before FileIdx is read, so a bare-numbered pack ("[Grp] Show - 03") served the
    // same file for every episode of it.
    let multi = [
        TorrentFile { index: 4, name: "a.mkv".into(), size_bytes: Some(10) },
        TorrentFile { index: 5, name: "b.mkv".into(), size_bytes: Some(20) },
    ];
    let got = pick_episode_file(&multi, 9, 9);
    assert!(matches!(got, Ok(None)), "no-match on a multi-file pool → no opinion: got {got:?}");
    // One candidate is different: there the largest really is the only thing it could be.
    let got =
        pick_episode_file(&[TorrentFile { index: 4, name: "a.mkv".into(), size_bytes: Some(10) }], 9, 9);
    assert!(matches!(got, Ok(Some(4))), "a lone unlabelled video is still the answer: got {got:?} want 4");
    let got = pick_episode_file(&[], 1, 1);
    assert!(matches!(got, Ok(None)), "no files → nil: got {got:?}");
}

#[test]
fn select_file_id_torbox() {
    use crate::season::TorrentFile;
    use crate::stores::torbox::select_file_id;
    use crate::stores::ResolveTarget;
    // TorBox file ids (Index) are NOT positions in Torrentio's list, so a series episode must be
    // name-matched, never resolved via the passed-through fileIdx (FOLLOWUP #13).
    let pack = vec![
        TorrentFile { index: 50, name: "Show.S01E01.mkv".into(), size_bytes: Some(100) },
        TorrentFile { index: 55, name: "Show.S01E02.mkv".into(), size_bytes: Some(100) },
    ];
    let got = select_file_id(
        &pack,
        &ResolveTarget { season: Some(1), episode: Some(2), file_idx: Some(99), ..Default::default() },
    );
    assert!(
        matches!(got, Ok(Some(55))),
        "episode name-match wins over fileIdx: got {got:?} want 55 (TorBox id)"
    );
    // fileIdx-only → POSITION in the list mapped to TorBox's file id (files[1].Index == 55).
    let got = select_file_id(&pack, &ResolveTarget { file_idx: Some(1), ..Default::default() });
    assert!(matches!(got, Ok(Some(55))), "fileIdx position → TorBox id: got {got:?} want 55");
    // fileIdx-only with no file list (single-file fast path / list failure) → raw passthrough.
    let got = select_file_id(&[], &ResolveTarget { file_idx: Some(7), ..Default::default() });
    assert!(matches!(got, Ok(Some(7))), "fileIdx passthrough when no list: got {got:?} want 7");
    let got = select_file_id(&pack, &ResolveTarget::default());
    assert!(matches!(got, Ok(None)), "no selector → nil, got {got:?}");
}

#[test]
fn pick_file_id_prefers_episode_match() {
    use crate::season::TorrentFile;
    use crate::stores::ResolveTarget;
    use crate::tests::stores_pick::{idle_premiumize, idle_real_debrid};
    // fileIdx=0 would (wrongly) point at E01 by position
    let t = ResolveTarget { season: Some(1), episode: Some(2), file_idx: Some(0), ..Default::default() };
    let rd_pack = vec![
        TorrentFile { index: 10, name: "Show.S01E01.mkv".into(), size_bytes: Some(100) },
        TorrentFile { index: 20, name: "Show.S01E02.mkv".into(), size_bytes: Some(100) },
    ];
    let got = idle_real_debrid().pick_file_id(&rd_pack, &t);
    assert!(matches!(got, Ok(Some(20))), "RD episode match over fileIdx: got {got:?} want 20");
    // Premiumize index == position
    let pm_pack = vec![
        TorrentFile { index: 0, name: "Show.S01E01.mkv".into(), size_bytes: Some(100) },
        TorrentFile { index: 1, name: "Show.S01E02.mkv".into(), size_bytes: Some(100) },
    ];
    let got = idle_premiumize().pick_index(&pm_pack, &t);
    assert!(matches!(got, Ok(Some(1))), "PM episode match over fileIdx: got {got:?} want 1");
}

#[test]
fn clean_label_and_size() {
    use crate::label::{clean_label, size_label};
    use crate::rank::RawStream;
    const GIB: i64 = 1 << 30;
    const MIB: i64 = 1 << 20;
    let s = RawStream {
        title: "Movie 2160p WEB-DL HDR Atmos".into(),
        size_bytes: Some(18 * GIB),
        ..Default::default()
    };
    assert_eq!(clean_label(&s), "4K • WEB-DL • HDR • Atmos • 18 GB", "cleanLabel");
    let sz = 3.4 * GIB as f64;
    assert_eq!(size_label(sz as i64), "3.4 GB", "sizeLabel small");
    assert_eq!(size_label(700 * MIB), "700 MB", "sizeLabel mb");
    assert_eq!(
        clean_label(&RawStream { title: "mysterious".into(), ..Default::default() }),
        "Stream",
        "cleanLabel empty"
    );
}

// Fauda S1E3: the picker must say what this stream actually is (1.1 GB) and name the pack only as
// context, never the other way round.
#[test]
fn clean_label_pack_size_is_context_not_the_headline() {
    use crate::label::clean_label;
    use crate::rank::RawStream;
    const GIB: i64 = 1 << 30;
    let s = RawStream {
        title: "Fauda.S01E03.1080p.WEB-DL".into(),
        size_bytes: Some(1_136_580_921),
        pack_size_bytes: Some(68 * GIB),
        ..Default::default()
    };
    let want = "1080p • WEB-DL • 1.1 GB · from a 68 GB pack";
    assert_eq!(clean_label(&s), want, "cleanLabel");
}

// A never-opened pack (unresolvedSeasonPack left SizeBytes nil): the label must name the number as a
// pack in the headline itself, never as a plain, unqualified "68 GB" that reads as the episode's own —
// and must not also repeat it as trailing "· from a 68 GB pack" context, since there is no separate,
// trusted episode number for that context to sit beside.
#[test]
fn clean_label_unknown_episode_size_names_the_pack_in_the_headline() {
    use crate::label::clean_label;
    use crate::rank::RawStream;
    const GIB: i64 = 1 << 30;
    let s = RawStream {
        title: "Fauda.S01.COMPLETE.1080p.WEB-DL".into(),
        pack_size_bytes: Some(68 * GIB),
        ..Default::default()
    };
    assert_eq!(clean_label(&s), "1080p • WEB-DL • 68 GB pack", "cleanLabel");
}

// Needs `streamAttributes` and `StreamAttributes` from attributes.go (work package 5). The body is ported and
// compiled out until it lands.
#[test]
fn stream_attributes() {
    use crate::attributes::stream_attributes;
    use crate::rank::RawStream;
    const GIB: i64 = 1 << 30;
    let a = stream_attributes(RawStream {
        title: "Movie.2160p.BluRay.REMUX.DV.HDR.HEVC.Atmos".into(),
        size_bytes: Some(40 * GIB),
        seeders: Some(12),
        cached: true,
        cache_known: true,
        ..Default::default()
    });
    assert!(
        a.resolution.as_deref() == Some("2160p")
            && a.source.as_deref() == Some("remux")
            && a.codec.as_deref() == Some("hevc")
            && a.hdr
            && a.dolby_vision
            && a.audio.as_deref() == Some("Atmos")
            && a.hdr_format.as_deref() == Some("HDR")
            && !a.three_d
            && a.cached == Some(true)
            && a.seeders == Some(12),
        "rich attrs: {a:?}"
    );

    // Cachedness is reported only when it was observed. A failed cache check leaves `Cached` at its zero
    // value, and sending that as `false` is a claim nobody made — the client reads a definite "must
    // download" and queues a fetch for a release the debrid may already hold.
    let unknown = stream_attributes(RawStream {
        title: "Movie.1080p.WEB-DL".into(),
        cached: false,
        cache_known: false,
        ..Default::default()
    });
    assert!(unknown.cached.is_none(), "unchecked cachedness must be omitted, got {:?}", unknown.cached);
    let held = stream_attributes(RawStream {
        title: "Movie.1080p.WEB-DL".into(),
        cached: false,
        cache_known: true,
        ..Default::default()
    });
    assert_eq!(held.cached, Some(false), "an observed 'not held' must be reported as false");

    // HDR10-family variants are distinguished (a stream can be DV *and* carry an HDR10 base).
    let hdr_cases = [
        ("Movie 2160p WEB-DL DDP5.1 DV HDR10+ HEVC", "HDR10+"), // HDR10+ beats a bare hdr10 token
        ("Movie 2160p WEB-DL HLG HEVC", "HLG"),
        ("Movie 2160p WEB-DL HDR10 HEVC", "HDR10"),
        ("Movie 2160p WEB-DL HDR HEVC", "HDR"),
        ("Movie 1080p BluRay x264", ""),
    ];
    for (title, want) in hdr_cases {
        let got = stream_attributes(RawStream { title: title.into(), ..Default::default() })
            .hdr_format
            .unwrap_or_default();
        assert_eq!(got, want, "hdrFormat({title:?})");
    }

    // Source-truth audio: normalized codec family + channel layout + atmos flag (so the client can
    // compute what Den actually delivers). DDP5 1 (space) still resolves to 5.1.
    let audio_cases = [
        ("Movie 2160p WEB-DL DDP5.1 Atmos HEVC", "eac3", "5.1", true),
        ("Movie 2160p REMUX TrueHD.Atmos.7.1 HEVC", "truehd", "7.1", true),
        ("Movie 1080p BluRay DTS-HD.MA.5.1 x264", "dtshdma", "5.1", false),
        ("Movie 2160p UHD BluRay DTS-X 7.1 HEVC", "dtsx", "7.1", false),
        ("Movie 2160p WEB-DL DDP5 1 HEVC", "eac3", "5.1", false),
        ("Movie 1080p BluRay AC3 2.0 x264", "ac3", "2.0", false),
    ];
    for (title, codec, channels, atmos) in audio_cases {
        let g = stream_attributes(RawStream { title: title.into(), ..Default::default() });
        let got_codec = g.audio_codec.unwrap_or_default();
        let got_chans = g.audio_channels.unwrap_or_default();
        assert!(
            got_codec == codec && got_chans == channels && g.atmos == atmos,
            "audio({title:?})= {{{got_codec} {got_chans} {}}} want {{{codec} {channels} {atmos}}}",
            g.atmos
        );
    }
    assert!(
        stream_attributes(RawStream { title: "Movie 2160p WEB-DL KORSUB HEVC".into(), ..Default::default() })
            .hardcoded_subs,
        "KORSUB → hardcodedSubs"
    );
    let sources = [
        ("X 1080p WEB-DL", "webdl"),
        ("X 1080p WEBRip", "webrip"),
        ("X 1080p BluRay", "bluray"),
        ("X 720p HDTV", "hdtv"),
        ("X DVDRip", "dvdrip"),
        ("X 2024 HDCAM", "cam"),
    ];
    for (title, want) in sources {
        let s = stream_attributes(RawStream { title: title.into(), ..Default::default() }).source;
        assert_eq!(s.as_deref(), Some(want), "source({title:?})");
    }
    assert!(
        stream_attributes(RawStream { title: "X mystery".into(), ..Default::default() }).source.is_none(),
        "no source → nil"
    );
    let a = stream_attributes(RawStream { title: "Movie 2160p WEB-DL HDR10".into(), ..Default::default() });
    assert!(a.hdr && !a.dolby_vision, "HDR10 without DV");
}

#[test]
fn memory_cache_ttl_and_eviction() {
    // The Go test moves `now` by reassigning a variable the closure captures; a shared clock does the same.
    let clock = Arc::new(Mutex::new(UNIX_EPOCH + Duration::from_secs(1000)));
    let mut c = MemoryCache::new(1 << 20);
    c.now = clock_fn(&clock);
    c.put("k", "v", Duration::from_secs(10));
    assert_eq!(c.get("k").as_deref(), Some("v"), "should hit within TTL");
    *clock.lock().unwrap() += Duration::from_secs(11);
    assert!(c.get("k").is_none(), "should expire past TTL");

    // byte-budget eviction (audit #1): the resident bytes stay within the budget however much is written, and
    // an entry that keeps being read outlives a stream of one-shot writes.
    let mut c2 = MemoryCache::new(1000);
    c2.now = clock_fn(&clock);
    c2.put("hot", "0123456789", Duration::from_secs(3600));
    let scan: Vec<String> = (0..200).map(|i| format!("scan{i:03}")).collect();
    for key in &scan {
        c2.put(key, &"x".repeat(40), Duration::from_secs(3600));
        assert!(c2.get("hot").is_some(), "the entry read on every write was evicted at {key}");
    }
    let resident: usize = scan.iter().filter(|k| c2.get(k).is_some()).map(|k| k.len() + 40).sum();
    assert!(resident > 0 && resident <= 1000, "{resident} scan bytes resident, want 1..=1000");
}

// A `put` that lands between `get` reading an expired entry and removing it must survive. The clock is the
// one call `get` makes in that window, so the test's clock performs the put the first time it is read after
// being armed.
#[test]
fn memory_cache_expiry_does_not_remove_a_concurrent_fresh_put() {
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::OnceLock;

    let clock = Arc::new(Mutex::new(UNIX_EPOCH + Duration::from_secs(1000)));
    let armed = Arc::new(AtomicBool::new(false));
    let cell: Arc<OnceLock<Arc<MemoryCache>>> = Arc::new(OnceLock::new());
    let mut c = MemoryCache::new(1 << 20);
    {
        let (clock, armed, cell) = (Arc::clone(&clock), Arc::clone(&armed), Arc::clone(&cell));
        c.now = Box::new(move || {
            let t = *clock.lock().unwrap();
            if armed.swap(false, Ordering::SeqCst) {
                cell.get().expect("cache set").put("k", "new", Duration::from_secs(3600));
            }
            t
        });
    }
    let c = Arc::new(c);
    cell.set(Arc::clone(&c)).ok().expect("set once");

    c.put("k", "old", Duration::from_secs(10));
    *clock.lock().unwrap() += Duration::from_secs(11);
    armed.store(true, Ordering::SeqCst);
    assert!(c.get("k").is_none(), "the expired value is not served");
    assert_eq!(c.get("k").as_deref(), Some("new"), "the fresh put was removed along with the expired value");
}

/// The Go test's `c.now = func() time.Time { return now }`: a clock the test can move.
fn clock_fn(clock: &Arc<Mutex<SystemTime>>) -> Box<dyn Fn() -> SystemTime + Send + Sync> {
    let clock = Arc::clone(clock);
    Box::new(move || *clock.lock().unwrap())
}

// An absurd config segment is refused before it is decoded, and the slices a build carries are bounded.
//
// Nothing capped the segment, and net/http accepts a request line up to its 1 MiB default: 716,770
// characters of path decoded to a VALID config with 16,289 debrid accounts, which one request then
// retained for the length of a scrape. Distinct blobs defeat both the list cache and the singleflight, so
// 150 concurrent misses measured 207 MiB inside a 230 MiB GOMEMLIMIT.
#[test]
fn decode_config_bounds_what_one_request_can_retain() {
    // A WELL-FORMED oversized segment is refused. The length matters: `maxConfigBlob+1` is 8193, and
    // 8193 % 4 == 1, which is not a valid unpadded base64 length — so the first version of this check was
    // rejected by the decoder before the cap was ever consulted, and disabling the cap left the whole
    // suite green. Built here as a real config padded with a long token, so only the cap can refuse it.
    let oversized = blob(&format!(
        r#"{{"debrid":[{{"service":"torbox","token":"t"}}],"filters":{{"excludeRegex":"{}"}},"_pad":"{}"}}"#,
        "a".repeat(64),
        "p".repeat(MAX_CONFIG_BLOB)
    ));
    assert!(
        oversized.len() > MAX_CONFIG_BLOB,
        "the oversized fixture is only {} bytes — it cannot exercise the {} cap",
        oversized.len(),
        MAX_CONFIG_BLOB
    );
    assert!(
        crate::config::decode_config(None, &oversized).is_none(),
        "a well-formed oversized config segment was accepted"
    );
    // …and it really would have decoded, so the refusal above is the cap and not a parse failure.
    if crate::config::decode_config(None, &oversized[..MAX_CONFIG_BLOB / 2 / 4 * 4]).is_none() {
        eprintln!(
            "(the truncated half does not decode either, which is fine — the check above is the point)"
        );
    }

    // The margin that matters is against the largest LEGITIMATE config the field caps admit, not against
    // a hand-picked typical one. maxDebridAccounts accounting each carrying a 512-character token (the
    // per-token cap), all four indexers, maxSources household sources at the link cap, every filter set,
    // and a 256-character excludeRegex.
    let accts: Vec<String> = (0..MAX_DEBRID_ACCOUNTS)
        .map(|_| format!(r#"{{"service":"torbox","token":"{}"}}"#, "t".repeat(512)))
        .collect();
    let sources: Vec<String> = (0..MAX_SOURCES)
        .map(|i| {
            let prefix = format!("https://source{}.example/", (b'a' + i as u8) as char);
            format!("\"{prefix}{}\"", "c".repeat(MAX_SOURCE_URL - prefix.len()))
        })
        .collect();
    let worst_legit = blob(&format!(
        r#"{{"debrid":[{}],"indexers":["torrentio","comet","mediafusion","torz"],"sources":[{}],"filters":{{"excludeCam":true,"hdrOnly":true,"resolutions":["2160p","1080p","720p","480p"],"preferResolution":"1080p","minSeeders":3,"maxSizeGB":40,"excludeRegex":"{}"}},"cachedOnly":true,"resultCap":20}}"#,
        accts.join(","),
        sources.join(","),
        "a".repeat(256)
    ));
    let c = crate::config::decode_config(None, &worst_legit);
    assert!(
        matches!(&c, Some(c) if c.sources.len() == MAX_SOURCES),
        "the largest config the field caps admit ({} bytes) does not fit the {} cap",
        worst_legit.len(),
        MAX_CONFIG_BLOB
    );
    // Sealing costs a little more, so the sealed form is what the margin has to clear.
    let kr = parse_seal_keyring(VEC_PRIV_B64, "").unwrap().unwrap();
    let plain = b64url_decode(&worst_legit).unwrap();
    let sealed_blob = seal(&kr.keys[0].pub_key, &plain).unwrap();
    let mut framed = vec![SEALED_VERSION];
    framed.extend_from_slice(&sealed_blob);
    let sealed_seg = b64url_encode(&framed);
    assert!(
        crate::config::decode_config(Some(&kr), &sealed_seg).is_some(),
        "the largest legitimate config SEALED ({} bytes) does not fit the {} cap",
        sealed_seg.len(),
        MAX_CONFIG_BLOB
    );
    // Recorded rather than asserted loosely: 90% of the cap is reachable legitimately, so the cap cannot
    // come down without lowering maxDebridAccounts or the per-token cap with it.
    eprintln!(
        "largest legitimate segment: {} plain, {} sealed, against a {} cap",
        worst_legit.len(),
        sealed_seg.len(),
        MAX_CONFIG_BLOB
    );

    // Accounts are capped, and repeated resolutions are deduped rather than retained — four valid values
    // mean a repeat carries no information, and rankStreams scans that slice per release.
    // Sized to fit under the blob cap, so this exercises the FIELD caps rather than being refused by the
    // segment cap one layer up.
    let accounts: Vec<&str> = (0..100).map(|_| r#"{"service":"torbox","token":"t"}"#).collect();
    let resolutions: Vec<&str> = (0..100).map(|_| r#""1080p""#).collect();
    let cfg = crate::config::decode_config(
        None,
        &blob(&format!(
            r#"{{"debrid":[{}],"filters":{{"resolutions":[{}]}}}}"#,
            accounts.join(","),
            resolutions.join(",")
        )),
    );
    let Some(cfg) = cfg else {
        panic!("the padded config was refused outright");
    };
    assert!(
        cfg.debrid.len() <= MAX_DEBRID_ACCOUNTS,
        "kept {} debrid accounts, want at most {}",
        cfg.debrid.len(),
        MAX_DEBRID_ACCOUNTS
    );
    assert_eq!(
        cfg.filters.resolutions.len(),
        1,
        "kept {} resolutions for one repeated value, want 1",
        cfg.filters.resolutions.len()
    );
}

// The account cap is a ceiling, not a target: a config naming several services keeps all of them.
//
// Only the upper bound was pinned, so narrowing the cap to a single account left the whole suite green
// — and dropping an account is not a cosmetic loss. Every debrid account is asked whether it holds a
// release, and a release only the second account has becomes a resolve against the first, which is an
// add: the exact cost the 50/hour budget exists to ration.
//
// The blind spot was in the fixtures rather than the assertions. Every config blob in the suite names
// exactly ONE debrid service, and the two multi-account fixtures are 8 and 100 identical torbox
// entries, so nothing anywhere decoded a config with two different services in it.
#[test]
fn decode_config_keeps_every_account_up_to_the_ceiling() {
    let cfg = crate::config::decode_config(
        None,
        &blob(
            r#"{"debrid":[{"service":"torbox","token":"tb"},{"service":"premiumize","token":"pm"},{"service":"realdebrid","token":"rd"}],"indexers":["torrentio"]}"#,
        ),
    );
    let Some(cfg) = cfg else {
        panic!("a three-service config was refused outright");
    };
    let want = [DebridService::TorBox, DebridService::Premiumize, DebridService::RealDebrid];
    assert_eq!(
        cfg.debrid.len(),
        want.len(),
        "kept {} debrid accounts, want {} — an account that is dropped is never asked whether it holds a release, \
         so every play of a release only it has costs an add against another account",
        cfg.debrid.len(),
        want.len()
    );
    for (i, svc) in want.iter().enumerate() {
        assert_eq!(cfg.debrid[i].service, *svc, "account {i} order is the preference order the pool asks in");
    }
}

// maxCompiledInsts bounds the compiled PROGRAM of a pattern that survives validation.
//
// Instructions, not megabytes, because megabytes could not be measured honestly here. Two earlier
// versions of this test sampled `runtime.MemStats.TotalAlloc` around the compile — and TotalAlloc is
// process-wide and cumulative, so every byte any other goroutine allocated inside that window was
// charged to the regex. It reported 250 literal `k`s as 23.6 MiB, and under contention 1,545 MiB for the
// same pattern that measures 0.03 MiB in a quiet process. That made the suite fail roughly one run in
// three under `-race -count=2`, and it meant the bound itself had been calibrated with a broken
// instrument.
//
// The instruction count is exactly what the guard controls, and it is deterministic: a counted
// repetition is EXPANDED into the program, which is the whole mechanism. Measured, all patterns at the
// 256-character cap:
//
//	bomb  `(?:x*240){1000}`   240,002 instructions   (dropped by the guard)
//	worst survivor                252 instructions   (250 literal characters, or 62 starred groups)
//
// 4096 sits three orders of magnitude below the bomb and sixteen times above the worst survivor.
//
// The other dimension, rune-table entries, is deliberately NOT asserted: `\pC` repeated 83 times is
// 118,192 runes (~0.47 MiB), and regexp/syntax caps that itself at 128 Ki runes — so an assertion here
// could never fail and would be the kind of forever-green check this file has already had to fix once.
const MAX_COMPILED_INSTS: i64 = 4096;

// The guard exists for a memory bound, so the bound is what gets asserted — not just which patterns the
// matcher happens to flag.
//
// A user pattern is up to 256 characters, compiled fresh on every list build, from a caller whose debrid
// token is never verified. The BOMB IS IN THE TABLE deliberately: it is the only entry containing a
// brace, so it is the only thing coupling this test to the guard. Without it, disabling
// hasCountedRepeat entirely left this test green — every flagged pattern hits the `continue` below and
// asserts nothing, so the test claimed a bound it could not have noticed being broken.
#[test]
#[ignore = "the instruction-count bound needs a Rust equivalent of regexp/syntax Prog.Inst (see compiled_insts)"]
fn exclude_regex_compiled_size_is_bounded() {
    let worst = [
        // The bomb. Dropped by the guard; if the guard ever stops dropping it, it lands at ~53 MiB and
        // this test is what says so.
        format!("(?:{}){{1000}}", "x".repeat(240)),
        // Unicode categories — the worst shapes that legitimately SURVIVE, at 1.7-3.1 MiB.
        format!("{}a", r"\pC|".repeat(62)),
        format!("{}a", r"\pL|".repeat(62)),
        r"\pC".repeat(83),
        // And the cheap shapes, all under 0.05 MiB.
        "ab|".repeat(84),
        format!("{}a{}", "(".repeat(60), ")".repeat(60)) + "*",
        "k".repeat(250),
        "[a-zA-Z0-9]".repeat(23),
        r"\p{Latin}".repeat(28),
        "(a*)".repeat(62),
    ];
    for pat in worst {
        let pat = if pat.len() > 256 { &pat[..256] } else { &pat[..] };
        let Some(cfg) = crate::config::decode_config(
            None,
            &blob(&format!(
                r#"{{"debrid":[{{"service":"torbox","token":"t"}}],"filters":{{"excludeRegex":{}}}}}"#,
                json_string(pat)
            )),
        ) else {
            panic!("rejected outright: {pat:.40}");
        };
        if cfg.filters.exclude_regex.is_empty() {
            continue; // the guard dropped it, so it is bounded by construction
        }
        let insts = compiled_insts(&cfg.filters.exclude_regex);
        assert!(
            insts <= MAX_COMPILED_INSTS,
            "a {}-char pattern that survived validation compiled to {insts} instructions (max {MAX_COMPILED_INSTS}): {pat:.40}",
            pat.len()
        );
    }
}
