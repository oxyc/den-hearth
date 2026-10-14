//! Go: `internal/scout/extra_test.go` (407 lines). Work package 5.
//!
//! A grab bag over ranking, release-title parsing, Cinemeta and the label/quality helpers.

use crate::attributes::stream_attributes;
use crate::label::clean_label;
use crate::probe::codec_from_matroska_id;
use crate::probe_avi::codec_from_four_cc;
use crate::rank::{
    detect_resolution, quality_score, rank_streams, release_years, title_overlap, title_tokens,
    year_mismatch, RankFilters, RawStream, GIB,
};
use crate::scrape::parse_stremio_streams;
use crate::season::{episode_patterns, matches_episode};

use super::rank::{intp, rs};

#[test]
fn parse_title_is_release_not_indexer_label() {
    // A stream with the indexer label as `name`, no behaviorHints, and a foreign release title: the
    // parsed title must be the release name, never the indexer label ("Torrentio").
    let body = format!(
        "{{\"streams\":[{{\"name\":\"Torrentio\",\"infoHash\":\"{}\",\"title\":\"El día de la Revelación (Spielberg's 2026) HdTScr Lat\\n👤 5 💾 1.7 GB ⚙️ x\"}}]}}",
        "a".repeat(40)
    );
    let seeds = parse_stremio_streams(body.as_bytes(), "torrentio");
    assert_eq!(seeds.len(), 1, "want 1 seed, got {}", seeds.len());
    assert!(
        !seeds[0].title.contains("Torrentio") && seeds[0].title.contains("Revelaci"),
        "title should be the release name, got {:?}",
        seeds[0].title
    );
}

#[test]
fn release_years_and_mismatch() {
    let got = release_years("Movie 1920x1080 x264 2160p");
    assert!(got.is_empty(), "resolution/codec digits parsed as years: {got:?}");
    let got = release_years("Steven Spielberg (2024) [HDTV 1080p]");
    assert!(got.len() == 1 && got[0] == 2024, "want [2024], got {got:?}");
    // A Spanish-titled 2026 release must NOT be flagged for expected 2026 (title differs, year matches).
    assert!(
        !year_mismatch("El día de la Revelación (Spielberg's 2026) HdTScr", 2026),
        "matching-year release flagged as mismatch"
    );
    assert!(year_mismatch("Steven Spielberg (2024) doc", 2026), "2024 mistag should mismatch expected 2026");
    // no-year title: can't tell, so keep
    assert!(
        !year_mismatch("Some Release WEB-DL", 2026),
        "no-year title must not be a mismatch (can't tell → keep)"
    );
    assert!(!year_mismatch("Movie (2025)", 2026), "±1 year should be tolerated");
}

#[test]
fn rank_drops_year_mistag() {
    let streams = [
        rs("Steven Spielberg el rey midas de Hollywood (2024) [HDTV 1080p]", |s| {
            s.info_hash = "a".repeat(40)
        }),
        rs("El día de la Revelación (Spielberg's 2026) HdTScr Lat", |s| s.info_hash = "b".repeat(40)),
        rs("Disclosure Day (2026) CinemaCity", |s| s.info_hash = "c".repeat(40)),
    ];
    let out = rank_streams(
        &streams,
        RankFilters { expected_year: Some(2026), result_cap: 10, ..Default::default() },
    );
    assert_eq!(out.len(), 2, "want 2 (2024 mistag dropped, both 2026 kept), got {}: {out:?}", out.len());
    for s in &out {
        assert!(!s.title.contains("2024"), "mistagged 2024 stream survived: {:?}", s.title);
    }
}

// Why the title-token filter is NOT applied to series, pinned so nobody re-enables it on the strength of
// the friendliest example.
//
// The check is safe for movies only because it judges a release solely when that release carries no
// parseable year, and a movie release almost always carries one — so it sees a small tail of nameless
// junk. A series EPISODE release carries S04E01 and no year, so the escape hatch is structurally
// unavailable and the check would judge nearly everything. These are real shows, and every one of them
// would come back as an empty list.
#[test]
fn title_tokens_would_empty_real_series() {
    // Shōgun and Pokémon were cases here until titleTokens folded diacritics; these two cannot be folded away.
    let cases = [
        ("Attack on Titan", "[Erai-raws] Shingeki no Kyojin - 01 [1080p].mkv", "anime released under romaji"),
        ("Money Heist", "La.Casa.de.Papel.S01E01.1080p.WEB-DL.mkv", "released under its original title"),
    ];
    for (show, release, why) in cases {
        let tokens = title_tokens(show);
        assert!(
            release_years(release).is_empty(),
            "{show}: the fixture carries a year, which is not the case under test"
        );
        assert!(
            !title_overlap(release, &tokens),
            "{show}: this example no longer demonstrates the problem ({why})"
        );
    }
    // The one case that does work, which is what made the first attempt at this look fine.
    assert!(
        title_overlap("The.Bear.S04E01.1080p.WEB-DL.mkv", &title_tokens("The Bear")),
        "an ASCII, English-titled show should overlap"
    );
}

/// What a `MockDoer` closure returns; the mock cannot infer the error type of a bare `Ok(..)`.
type Answer = Result<reqwest::Response, crate::scrape::DoError>;

#[tokio::test]
async fn cinemeta_meta() {
    use crate::cache::MemoryCache;
    use crate::cinemeta::cinemeta_meta;
    use crate::scrape::Doer;
    use std::sync::{Arc, Mutex};

    let client: Arc<dyn Doer> = Arc::new(crate::tests::mock::MockDoer(|_r: reqwest::Request| -> Answer {
        Ok(crate::tests::mock::resp(200, r#"{"meta":{"id":"tt1","name":"Disclosure Day","year":"2026"}}"#))
    }));
    let ok = cinemeta_meta(client, "https://cinemeta.example", Arc::new(MemoryCache::new(1 << 20)));
    let (m, found) = ok.lookup("movie", "tt15047880").await;
    assert!(found && m.year == 2026 && m.title == "Disclosure Day", "movie meta: {m:?} found={found}");

    // A series is looked up at its own path, for the title that sinks another show's releases. The handler
    // keeps it out of both mistag filters — see cinemeta.go and title_tokens_would_empty_real_series.
    let path = Arc::new(Mutex::new(String::new()));
    let seen = Arc::clone(&path);
    let series_client: Arc<dyn Doer> =
        Arc::new(crate::tests::mock::MockDoer(move |r: reqwest::Request| -> Answer {
            *seen.lock().expect("lock") = r.url().path().to_owned();
            Ok(crate::tests::mock::resp(
                200,
                r#"{"meta":{"id":"tt1","name":"The Bear","releaseInfo":"2022–2025"}}"#,
            ))
        }));
    let series =
        cinemeta_meta(series_client, "https://cinemeta.example", Arc::new(MemoryCache::new(1 << 20)));
    let (m, found) = series.lookup("series", "tt1").await;
    assert!(found && m.title == "The Bear", "series meta: {m:?} found={found}");
    assert_eq!(
        path.lock().expect("lock").as_str(),
        "/meta/series/tt1.json",
        "a series was asked for at the wrong path"
    );

    // upstream failure → found=false (list served unfiltered)
    let bad_client: Arc<dyn Doer> =
        Arc::new(crate::tests::mock::MockDoer(|_r: reqwest::Request| -> Answer {
            Ok(crate::tests::mock::resp(500, ""))
        }));
    let bad = cinemeta_meta(bad_client, "x", Arc::new(MemoryCache::new(1 << 20)));
    let (_, found) = bad.lookup("movie", "tt1").await;
    assert!(!found, "cinemeta failure should return found=false");
}

// Every movie list build asks Cinemeta, so an answer is remembered for a week and a failure for ten minutes;
// a lookup cut short by its caller's deadline is not remembered at all.
/// Go `ttlCache` records the TTL each key was last written with.
struct TtlCache {
    inner: crate::cache::MemoryCache,
    ttls: std::sync::Mutex<std::collections::HashMap<String, std::time::Duration>>,
}

impl crate::cache::Cache for TtlCache {
    fn get(&self, key: &str) -> Option<String> {
        self.inner.get(key)
    }

    fn put(&self, key: &str, value: &str, ttl: std::time::Duration) {
        self.ttls.lock().expect("lock").insert(key.to_owned(), ttl);
        self.inner.put(key, value, ttl);
    }
}

impl TtlCache {
    fn ttl_of(&self, key: &str) -> Option<std::time::Duration> {
        self.ttls.lock().expect("lock").get(key).copied()
    }
}

/// A `Doer` whose request never completes: Go's already-cancelled context, as a future that is dropped.
struct NeverAnswers;

#[async_trait::async_trait]
impl crate::scrape::Doer for NeverAnswers {
    async fn do_request(&self, _req: reqwest::Request) -> Answer {
        std::future::pending().await
    }
}

// Every movie list build asks Cinemeta, so an answer is remembered for a week and a failure for ten minutes;
// a lookup cut short by its caller's deadline is not remembered at all.
#[tokio::test]
async fn cinemeta_meta_caches_answers_and_failures() {
    use crate::cache::{Cache, MemoryCache};
    use crate::cinemeta::{cinemeta_key, cinemeta_meta, CINEMETA_HIT_TTL, CINEMETA_MISS_TTL};
    use crate::scrape::Doer;
    use std::sync::atomic::{AtomicU16, AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    let asked = Arc::new(AtomicUsize::new(0));
    let status = Arc::new(AtomicU16::new(200));
    let cache = Arc::new(TtlCache { inner: MemoryCache::new(1 << 20), ttls: Mutex::new(Default::default()) });
    let (n, s) = (Arc::clone(&asked), Arc::clone(&status));
    let client: Arc<dyn Doer> =
        Arc::new(crate::tests::mock::MockDoer(move |_r: reqwest::Request| -> Answer {
            n.fetch_add(1, Ordering::SeqCst);
            Ok(crate::tests::mock::resp(
                s.load(Ordering::SeqCst),
                r#"{"meta":{"name":"Disclosure Day","year":"2026"}}"#,
            ))
        }));
    let shared: Arc<dyn Cache> = cache.clone();
    let meta = cinemeta_meta(client, "https://cinemeta.example", Arc::clone(&shared));

    for i in 0..3 {
        let (m, found) = meta.lookup("movie", "tt100").await;
        assert!(found && m.year == 2026 && m.title == "Disclosure Day", "lookup {i}: {m:?} found={found}");
    }
    let n = asked.load(Ordering::SeqCst);
    assert_eq!(n, 1, "asked Cinemeta {n} times for one film, want 1");
    let ttl = cache.ttl_of(&cinemeta_key("https://cinemeta.example", "movie", "tt100"));
    assert_eq!(ttl, Some(CINEMETA_HIT_TTL), "an answer is kept for {ttl:?}, want {CINEMETA_HIT_TTL:?}");

    status.store(404, Ordering::SeqCst);
    for _ in 0..3 {
        let (_, found) = meta.lookup("movie", "tt200").await;
        assert!(!found, "a film Cinemeta does not have was found");
    }
    let n = asked.load(Ordering::SeqCst);
    assert_eq!(n, 2, "asked Cinemeta {n} times in all, want a failure to be remembered too");
    let ttl = cache.ttl_of(&cinemeta_key("https://cinemeta.example", "movie", "tt200"));
    assert_eq!(ttl, Some(CINEMETA_MISS_TTL), "a failure is kept for {ttl:?}, want {CINEMETA_MISS_TTL:?}");

    // Go cancels the lookup's context; here the caller gives up on a request that never answers.
    let hung = cinemeta_meta(Arc::new(NeverAnswers), "https://cinemeta.example", shared);
    let cut_short = tokio::time::timeout(Duration::from_millis(20), hung.lookup("movie", "tt300")).await;
    assert!(cut_short.is_err(), "the lookup was expected to be cut short");
    let kept = cache.ttl_of(&cinemeta_key("https://cinemeta.example", "movie", "tt300"));
    assert!(kept.is_none(), "a lookup its caller cancelled was remembered as a failure");
}

#[tokio::test]
async fn cinemeta_meta_keeps_configured_providers_apart() {
    use crate::cache::MemoryCache;
    use crate::cinemeta::{cinemeta_meta, Cinemeta};
    use crate::scrape::Doer;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    // Go: two providers (https://a.example/ and https://b.example) share one cache. Each is asked once for
    // "tt-same", each answers its own title, and A's cached answer survives B's request.
    let cache = Arc::new(MemoryCache::new(1 << 20));
    let meta = |base: &str, name: &'static str, asked: &Arc<AtomicUsize>| -> Cinemeta {
        let asked = Arc::clone(asked);
        let client: Arc<dyn Doer> =
            Arc::new(crate::tests::mock::MockDoer(move |_r: reqwest::Request| -> Answer {
                asked.fetch_add(1, Ordering::SeqCst);
                Ok(crate::tests::mock::resp(200, &format!(r#"{{"meta":{{"name":"{name}","year":"2026"}}}}"#)))
            }));
        cinemeta_meta(client, base, cache.clone())
    };
    let (asked_a, asked_b) = (Arc::new(AtomicUsize::new(0)), Arc::new(AtomicUsize::new(0)));
    let a = meta("https://a.example/", "Provider A", &asked_a);
    let b = meta("https://b.example", "Provider B", &asked_b);

    let (first, ok_a) = a.lookup("movie", "tt-same").await;
    let (second, ok_b) = b.lookup("movie", "tt-same").await;
    assert!(
        ok_a && ok_b && first.title == "Provider A" && second.title == "Provider B",
        "provider answers crossed: A={first:?}/{ok_a} B={second:?}/{ok_b}"
    );
    let (n_a, n_b) = (asked_a.load(Ordering::SeqCst), asked_b.load(Ordering::SeqCst));
    assert!(n_a == 1 && n_b == 1, "provider requests A={n_a} B={n_b}, want one each");
    let (again, _) = a.lookup("movie", "tt-same").await;
    assert!(
        again.title == "Provider A" && asked_a.load(Ordering::SeqCst) == 1,
        "provider A cache changed: {again:?}, calls={}",
        asked_a.load(Ordering::SeqCst)
    );
}

#[test]
fn rank_drops_yearless_junk() {
    let streams = [
        // no year, no overlap → drop
        rs("B-Bead.mp4", |s| s.info_hash = "a".repeat(40)),
        // no year, no overlap → drop
        rs("Random Junk File", |s| s.info_hash = "b".repeat(40)),
        // no year, shares "backrooms" → keep (foreign-lang)
        rs("Los Backrooms HDTV Lat", |s| s.info_hash = "c".repeat(40)),
        // has matching year → keep
        rs("Backrooms.2026.1080p.WEB.h265", |s| s.info_hash = "d".repeat(40)),
    ];
    let out = rank_streams(
        &streams,
        RankFilters {
            expected_year: Some(2026),
            expected_title_tokens: title_tokens("Backrooms"),
            result_cap: 10,
            ..Default::default()
        },
    );
    assert_eq!(
        out.len(),
        2,
        "want 2 (year-less junk dropped, foreign-lang + matching-year kept), got {}: {out:?}",
        out.len()
    );
    for s in &out {
        assert!(
            !s.title.contains("B-Bead") && !s.title.contains("Random Junk"),
            "year-less junk survived: {:?}",
            s.title
        );
    }
}

#[test]
fn detect_audio_codec_branches() {
    let audio = [
        ("x DTS:X", "DTS:X"),
        ("x TrueHD", "TrueHD"),
        ("x DTS-HD MA", "DTS-HD"),
        ("x FLAC", "FLAC"),
        ("x EAC3", "EAC3"),
        ("x DTS", "DTS"),
        ("x AC3", "AC3"),
        ("x Atmos", "Atmos"),
    ];
    for (title, want) in audio {
        let got = stream_attributes(RawStream { title: title.into(), ..Default::default() }).audio;
        assert!(got.as_deref() == Some(want), "audio({title:?})={got:?} want {want:?}");
    }
    // The spellings here must be the ones the container parsers produce, since withProbe overwrites
    // Codec outright — see detectCodec. TestAttributes_titleAndProbeAgreeOnCodecSpelling holds the two
    // paths together; this table only pins the title half.
    let codec =
        [("x AV1", "av1"), ("x x265", "hevc"), ("x HEVC", "hevc"), ("x x264", "h264"), ("x h264", "h264")];
    for (title, want) in codec {
        let got = stream_attributes(RawStream { title: title.into(), ..Default::default() }).codec;
        assert!(got.as_deref() == Some(want), "codec({title:?})={got:?} want {want:?}");
    }
    assert!(
        stream_attributes(RawStream { title: "x mystery".into(), ..Default::default() }).codec.is_none(),
        "no codec → nil"
    );
}

// The title path and the probe paths must spell each codec the same way.
//
// withProbe OVERWRITES Codec rather than merging, and probing is asynchronous, so any disagreement
// shows up as the value changing between a viewer's first /stream response for a release and the next.
// That was live: the title path said "avc" where all three container parsers say "h264".
//
// Asserted across the three producers at once, because a table that pins each one separately is what
// let them drift — TestWithProbe_overridesTitleGuesses seeds Codec with "h264", a value the title path
// could not then produce, so it agreed with the probe by construction and saw nothing.
#[test]
fn attributes_title_and_probe_agree_on_codec_spelling() {
    // (codec, title, matroska id, fourCC, mp4 sample entry)
    let cases = [
        ("h264", "Movie 1080p x264", "V_MPEG4/ISO/AVC", "h264", "avc1"),
        ("hevc", "Movie 2160p x265", "V_MPEGH/ISO/HEVC", "hevc", "hvc1"),
        ("av1", "Movie 2160p AV1", "V_AV1", "av01", "av01"),
    ];
    for (codec, title, matroska_id, four_cc, mp4_sample_entry) in cases {
        let got = stream_attributes(RawStream { title: title.into(), ..Default::default() }).codec;
        let Some(got) = got else {
            panic!("{codec}: title {title:?} → nil, want {codec:?}");
        };
        assert!(
            got == codec,
            "title {title:?} → {got:?}, want {codec:?} — the title path and the container parsers disagree, so the codec flips when the probe lands"
        );
        let s = codec_from_matroska_id(matroska_id);
        assert!(
            s == codec,
            "matroska {matroska_id:?} → {s:?}, but the title path says {codec:?} — one codec, two spellings, so the value flips when the probe lands"
        );
        // AVI and MP4 share codecFromFourCC but hand it different spellings — an AVI fourCC and an
        // MP4 sample-entry type. Both are asserted, because "avc1" and "h264" landing on the same
        // answer is the agreement being claimed, not an incidental one.
        let s = codec_from_four_cc(four_cc);
        assert!(s == codec, "avi fourCC {four_cc:?} → {s:?}, but the title path says {codec:?}");
        let s = codec_from_four_cc(mp4_sample_entry);
        assert!(
            s == codec,
            "mp4 sample entry {mp4_sample_entry:?} → {s:?}, but the title path says {codec:?}"
        );
    }
}

#[test]
fn quality_score_branches() {
    // Exercise the source ladder, audio ladder, and penalties (values are internal; just drive them).
    for title in [
        "Movie 1080p WEBRip",
        "Movie 1080p WEB",
        "Movie 480p DVDRip",
        "Movie 720p TVRip",
        "Movie 1080p BluRay DTS-HD MA",
        "Movie 1080p BluRay TrueHD",
        "Movie 1080p WEB EAC3",
        "Movie 1080p WEB DTS",
        "Movie 1080p WEB AC3",
        "Movie 1080p WEB FLAC",
        "Movie 1080p WEB DTS:X",
        "Movie 1440p WEB",
        "Movie 576p WEB",
        "Movie 540p WEB",
        "Movie korsub",
        "Movie HC",
    ] {
        let _ = quality_score(rs(title, |_| {}));
    }
    // a large "4k"/uhd (no explicit 2160p) with a big file is trusted as 4K.
    let big = quality_score(rs("Movie UHD WEB", |s| s.size_bytes = intp(20 * GIB)));
    let tiny = quality_score(rs("Movie UHD WEB", |s| s.size_bytes = intp(GIB)));
    assert!(big > tiny, "large uhd should outscore tiny uhd");
}

#[test]
fn label_branches() {
    let cases = [
        ("Movie 1440p BluRay", "1440p • BluRay"),
        ("Movie 480p WEB", "SD • WEB"),
        ("Movie 720p WEBRip", "720p • WEB"),
    ];
    for (title, want) in cases {
        let got = clean_label(&RawStream { title: title.into(), ..Default::default() });
        assert_eq!(got, want, "cleanLabel({title:?})={got:?} want {want:?}");
    }
    // 1440p has no bucket of its own; it must land on one of the two coarse answers rather than invent a
    // third. The previous version of this had an empty branch body, so it could not fail whatever came
    // back — it asserted nothing at all.
    let got = detect_resolution("x 1440p");
    assert!(got == "1080p" || got.is_empty(), "detectResolution(1440p) = {got:?}, want 1080p or none");
}

#[test]
fn season_absolute_numbering_no_resolution_match() {
    // concatenated form is "720"
    let p = episode_patterns(7, 20);
    assert!(
        !matches_episode("Show.720p.WEB-DL.x264.mkv", &p),
        "S7E20 must not match a 720p resolution token"
    );
    assert!(matches_episode("Show.720.mkv", &p), "S7E20 should still match a bare 720 episode token");
    assert!(
        matches_episode("Show.S07E20.1080p.mkv", &episode_patterns(7, 20)),
        "S07E20 form should still match"
    );
}

#[test]
fn rank_min_seeders_keeps_unknown() {
    let streams = [
        // seeders unknown
        rs("Movie 1080p WEB", |s| s.info_hash = "a".repeat(40)),
        // known, below threshold
        rs("Movie 720p WEB", |s| {
            s.info_hash = "b".repeat(40);
            s.seeders = intp(1);
        }),
    ];
    let out =
        rank_streams(&streams, RankFilters { min_seeders: intp(5), result_cap: 10, ..Default::default() });
    assert!(
        out.len() == 1 && out[0].info_hash == "a".repeat(40),
        "minSeeders should keep unknown-seeder streams and drop known-below-threshold: {out:?}"
    );
}

#[tokio::test]
async fn series_stream_and_forwarded_origin() {
    use std::collections::HashMap;
    use std::sync::Arc;

    use crate::config::DebridService;
    use crate::handler::new_handler;
    use crate::play::decode_play_token;
    use crate::tests::handler::{body_str, fake_scraper, get, test_deps, valid_blob, FakeStore};

    let h = new_handler(test_deps(|d| {
        d.make_scrapers = Arc::new(|_| {
            vec![fake_scraper("torrentio", || {
                Box::pin(async {
                    Ok(vec![RawStream {
                        info_hash: "a".repeat(40),
                        title: "Show 1080p WEB-DL".into(),
                        size_bytes: Some(3 * (1 << 30)),
                        file_idx: Some(4),
                        ..Default::default()
                    }])
                })
            })]
        });
        d.make_stores = Arc::new(|_| {
            vec![Box::new(FakeStore {
                svc: DebridService::TorBox,
                check: HashMap::from([("a".repeat(40), true)]),
                check_err: None,
                resolve: Arc::new(|| Ok(String::new())),
                status: None,
                status_blocks: false,
            })]
        });
    }));
    let rr = get(
        &h,
        &format!("/{}/stream/series/tt99:1:2.json", valid_blob()),
        &[("X-Forwarded-Proto", "https"), ("X-Forwarded-Host", "scout.den.example")],
    )
    .await;
    assert_eq!(rr.status(), 200, "series stream: {}", rr.status());
    let body: serde_json::Value = serde_json::from_str(&body_str(&rr)).unwrap_or(serde_json::Value::Null);
    let streams = body["streams"].as_array().cloned().unwrap_or_default();
    assert_eq!(streams.len(), 1, "want 1 series stream, got {}", streams.len());
    let first = &streams[0];
    let url = first["url"].as_str().unwrap_or_default();
    assert!(url.starts_with("https://scout.den.example/"), "forwarded origin: {url}");
    let binge = first["behaviorHints"]["bingeGroup"].as_str().unwrap_or_default();
    assert_eq!(binge, "den-scout|1080p|webdl||", "bingeGroup: {binge}");
    let tok = &url[url.rfind("/play/").expect("a play route") + "/play/".len()..];
    let pt = decode_play_token(tok);
    assert!(
        matches!(&pt, Some(pt) if pt.season == Some(1) && pt.episode == Some(2)),
        "series play token: {pt:?}"
    );
}
