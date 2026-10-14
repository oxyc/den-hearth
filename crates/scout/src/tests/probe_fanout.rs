//! Go: `internal/scout/probe_fanout_test.go` (618 lines). Work package 5.
//!
//! The pure probe-fact and size tests compile against the stubs. Those that drive `handler::probeTop`,
//! `hydrateProbeFacts` or `hydrateKnownSizes` need package 4's handler and package 1/2's stores, so their Go
//! assertions sit in `cfg(any())` items until those land.

use crate::attributes::with_probe;
use crate::attributes::StreamAttributes;
use crate::id::StreamId;
use crate::probe::Probe;
use crate::probe_fanout::{apply_probed_size, browser_probe_candidates, hydrate_pack_only_sizes, probe_cache_key, BROWSER_PROBE_FRONTIER};
use crate::client::ClientPlayable;
use crate::config::Config;
use crate::rank::RawStream;

use super::rank::intp;

fn sid_series(season: i64, episode: i64) -> StreamId {
    StreamId { kind: "series".into(), imdb: String::new(), season, episode, has_ep: true }
}

fn sid_movie() -> StreamId {
    StreamId { kind: "movie".into(), imdb: String::new(), season: 0, episode: 0, has_ep: false }
}

// A probed release's own facts replace what the title guessed; an unprobed one is left exactly as it was.
#[test]
fn with_probe_overrides_title_guesses() {
    let base = StreamAttributes {
        codec: Some("h264".into()),
        audio_channels: Some("2.0".into()),
        ..Default::default()
    };
    let got = with_probe(
        base.clone(),
        Some(&Probe {
            video_codec: "mpeg4".into(),
            audio_channels: "5.1".into(),
            audio: vec!["sv".into()],
            dolby_vision: true,
            ..Default::default()
        }),
    );
    assert!(
        got.codec.as_deref() == Some("mpeg4")
            && got.audio_channels.as_deref() == Some("5.1")
            && got.dolby_vision
            && got.hdr
            && got.probed,
        "probe did not override the title: {got:?}"
    );
    assert!(got.audio_languages.len() == 1 && got.audio_languages[0] == "sv", "languages missing: {:?}", got.audio_languages);
    let untouched = with_probe(base, None);
    assert!(
        !untouched.probed && untouched.audio_languages.is_empty() && untouched.codec.as_deref() == Some("h264"),
        "unprobed release was altered: {untouched:?}"
    );
}

// A probe that couldn't read a field must not blank what the title supplied.
#[test]
fn with_probe_empty_fields_do_not_erase() {
    let base = StreamAttributes {
        codec: Some("hevc".into()),
        audio_channels: Some("7.1".into()),
        ..Default::default()
    };
    let got = with_probe(base, Some(&Probe { container: "avi".into(), ..Default::default() }));
    assert!(
        got.codec.as_deref() == Some("hevc") && got.audio_channels.as_deref() == Some("7.1"),
        "empty probe erased title data: {got:?}"
    );
}

// The file is the identity, not the request: the same release serves every user of this instance.
#[test]
fn probe_cache_key_identifies_the_file() {
    let a = RawStream { info_hash: "h".into(), file_idx: Some(3), ..Default::default() };
    let b = RawStream { info_hash: "h".into(), file_idx: Some(3), ..Default::default() };
    assert_eq!(probe_cache_key(&a, None), probe_cache_key(&b, None), "same file produced different keys");
    let c = RawStream { info_hash: "h".into(), file_idx: Some(4), ..Default::default() };
    assert_ne!(probe_cache_key(&a, None), probe_cache_key(&c, None), "different files in one torrent share a key");
}

// A small, ordinary mismatch between the title's claim and the probe's reading (rounding, a muxer's
// padding) is not a "pack" even on a series request — only a gap wide enough that the title was plainly
// describing something else is worth surfacing.
#[test]
fn apply_probed_size_close_mismatch_is_not_called_a_pack() {
    let mut s = RawStream {
        size_bytes: intp(1_100_000_000),
        probe: Some(Probe { size_bytes: 1_080_000_000, ..Default::default() }),
        ..Default::default()
    };
    let sid = sid_series(1, 3);

    apply_probed_size(&mut s, Some(&sid));

    assert!(s.pack_size_bytes.is_none(), "PackSizeBytes = {:?}, want nil for a close, non-pack mismatch", s.pack_size_bytes);
    assert_eq!(s.size_bytes, Some(1_080_000_000), "SizeBytes = {:?}, want the probed 1.08 GB", s.size_bytes);
}

// A never-opened season pack — no probe has warmed, no store has a file list cached — must never show
// its whole total as if it were episode 3's own. The title itself says this is a pack (it names no
// episode at all), so hydratePackOnlySizes must demote SizeBytes to unknown and keep the number only as
// PackSizeBytes, rather than leave the indexer's 68 GB standing as the episode's.
#[test]
fn hydrate_pack_only_sizes_never_opened_pack_loses_its_size_as_an_episodes() {
    const PACK_HINT: i64 = 68 << 30;
    let sid = sid_series(1, 3);
    let mut streams = [RawStream {
        info_hash: "fauda-s1e3".into(),
        title: "Fauda.S01.COMPLETE.1080p.NF.WEB-DL".into(),
        size_bytes: Some(PACK_HINT),
        ..Default::default()
    }];

    hydrate_pack_only_sizes(&mut streams, Some(&sid));

    assert!(streams[0].size_bytes.is_none(), "SizeBytes = {:?}, want nil (unknown) rather than the pack's own total", streams[0].size_bytes);
    assert_eq!(
        streams[0].pack_size_bytes,
        Some(PACK_HINT),
        "PackSizeBytes = {:?}, want the title's {PACK_HINT} kept as what it actually is",
        streams[0].pack_size_bytes
    );
}

// A release whose title plainly names this exact episode is trusted exactly as it always was — this
// pass must not touch it even though nothing has probed or resolved it yet.
#[test]
fn hydrate_pack_only_sizes_a_plain_episode_file_is_left_alone() {
    let sid = sid_series(1, 3);
    let mut streams = [RawStream {
        info_hash: "fauda-s1e3-plain".into(),
        title: "Fauda.S01E03.WEB-DL.1080p.mkv".into(),
        size_bytes: Some(1_136_580_921),
        ..Default::default()
    }];

    hydrate_pack_only_sizes(&mut streams, Some(&sid));

    assert_eq!(streams[0].size_bytes, Some(1_136_580_921), "SizeBytes = {:?}, want the indexer's own untouched", streams[0].size_bytes);
    assert!(streams[0].pack_size_bytes.is_none(), "PackSizeBytes = {:?}, want nil — this title names exactly this episode", streams[0].pack_size_bytes);
}

// A movie is never subject to this demotion — there is no pack for its title to have named instead.
#[test]
fn hydrate_pack_only_sizes_movie_is_skipped() {
    let mut streams = [RawStream {
        info_hash: "movie-hash".into(),
        title: "Some.Movie.2019.COMPLETE.1080p".into(),
        size_bytes: Some(18 << 30),
        ..Default::default()
    }];

    hydrate_pack_only_sizes(&mut streams, Some(&sid_movie()));

    assert_eq!(streams[0].size_bytes, Some(18 << 30), "SizeBytes changed for a movie: {:?}", streams[0].size_bytes);
}

// The frontier is decision-directed: it carries the current winner and what could displace it, never more
// than the bound, and a compatibility-sunk 4K release that could still win is among the candidates.
#[test]
fn browser_probe_frontier_is_decision_directed_and_bounded() {
    let client = ClientPlayable { h264: 51, ..Default::default() };
    let mut streams = vec![RawStream { title: "Movie.1080p.x264".into(), ..Default::default() }];
    for i in 0..8 {
        streams.push(RawStream {
            title: "Movie.2160p.REMUX.HEVC".into(),
            seeders: intp(100 - i),
            ..Default::default()
        });
    }
    let frontier = browser_probe_candidates(&streams, &Config::default(), Some(&client));
    assert_eq!(frontier.len(), BROWSER_PROBE_FRONTIER, "frontier has {} candidates, want the bound {BROWSER_PROBE_FRONTIER}: {frontier:?}", frontier.len());
    assert!(frontier.contains(&0), "the unresolved current winner is absent from its own decision frontier");
    assert!(frontier.contains(&1), "a compatibility-sunk 4K contender that can displace the winner was not selected");
}

// Probing resolves, and resolving a release the account does not hold ADDS it, so these handler-driven
// tests need `handler::probeTop`, `Deps` and the store fakes from package 4 (handler) and packages 1/2.
#[test]
#[ignore = "needs handler::probeTop, Deps and the fakeStore fixtures from packages 1, 2 and 4"]
fn probe_top_opt_in() {
    #[cfg(any())]
    fn pending() {
        use crate::cache::MemoryCache;
        use crate::deps::Deps;
        use crate::handler::Handler;
        use std::cell::Cell;

        // Probing costs a debrid RESOLVE per release, so it must never happen by accident. A caller that hasn't
        // wired a probe client gets the old behaviour exactly — the list is built without touching the account.
        let resolves = Cell::new(0);
        let h = Handler::for_test(Deps {
            cache: std::sync::Arc::new(MemoryCache::new(1 << 20)),
            make_stores: Box::new(|_| {
                resolves.set(resolves.get() + 1);
                vec![]
            }),
            ..Default::default()
        });
        let mut streams = vec![RawStream { info_hash: "abc".into(), ..Default::default() }];
        h.probe_top(&Config::default(), &mut streams, None, &held_on_torbox(&["abc"]));
        assert!(resolves.get() == 0 && streams[0].probe.is_none(), "probed without a client: resolves={} probe={:?}", resolves.get(), streams[0].probe);
    }
    todo!("pending: probe_fanout_test.go TestProbeTop_optIn")
}

// Every already-cached fact is hydrated before ranking; this is local cache IO, not a debrid probe.
#[test]
#[ignore = "needs handler::probeTop and Deps from package 4 (handler), and MemoryCache from package 6"]
fn probe_top_caps_at_top_n() {
    #[cfg(any())]
    fn pending() {
        // Go: 20 cached streams with the same hash; every probe cache key is pre-seeded with {"audioLanguages":["sv"]},
        // so probeTop hydrates all 20 without a resolve. The test asserts that all 20 end up with a Probe.
        let mut streams: Vec<RawStream> = (0..20).map(|_| RawStream { info_hash: "hash".into(), cached: true, ..Default::default() }).collect();
        let cache = seeded_cache(&streams, r#"{"audioLanguages":["sv"]}"#);
        let h = Handler::for_test(Deps { cache, probe_client: Some(http_default_client()), make_stores: Box::new(|_| vec![]), ..Default::default() });
        h.probe_top(&Config::default(), &mut streams, None, &held_on_torbox(&["hash"]));
        let probed = streams.iter().filter(|s| s.probe.is_some()).count();
        assert_eq!(probed, streams.len(), "hydrated {probed} cached probe facts, want all {}", streams.len());
    }
    todo!("pending: probe_fanout_test.go TestProbeTop_capsAtTopN")
}

// A cached probe must not cost a resolve — that is what makes the disk tier worth having across restarts.
#[test]
#[ignore = "needs handler::probeTop and Deps from package 4 (handler), and MemoryCache from package 6"]
fn probe_top_cache_hit_skips_resolve() {
    #[cfg(any())]
    fn pending() {
        // Go: a cached probe for "cached-hash" with audio ["it"] must be applied without building a store pool.
        let s = RawStream { info_hash: "cached-hash".into(), cached: true, ..Default::default() };
        let cache = seeded_cache(std::slice::from_ref(&s), r#"{"audioLanguages":["it"],"videoCodec":"h264"}"#);
        let made = std::cell::Cell::new(0);
        let h = Handler::for_test(Deps {
            cache,
            probe_client: Some(http_default_client()),
            make_stores: Box::new(|_| {
                made.set(made.get() + 1);
                vec![]
            }),
            ..Default::default()
        });
        let mut streams = vec![s];
        h.probe_top(&Config::default(), &mut streams, None, &held_on_torbox(&["cached-hash"]));
        assert_eq!(made.get(), 0, "built a store pool despite a cache hit");
        let p = streams[0].probe.as_ref().expect("cache hit not applied");
        assert!(p.audio.len() == 1 && p.audio[0] == "it", "cache hit not applied: {p:?}");
    }
    todo!("pending: probe_fanout_test.go TestProbeTop_cacheHitSkipsResolve")
}

// Fauda S1E3: the indexer's title gave the season pack's 68 GB, and a probe of the resolved link read
// the episode's own 1.1 GB. Hydrating that cached probe fact must correct SizeBytes to the episode and
// keep the pack's number only as context.
#[test]
#[ignore = "needs handler::hydrateProbeFacts and Deps from package 4 (handler), and MemoryCache from package 6"]
fn hydrate_probe_facts_season_pack_size_corrects_to_the_episode() {
    #[cfg(any())]
    fn pending() {
        const PACK_HINT: i64 = 68 << 30; // what "💾 68 GB" in the title parsed to
        const EPISODE_SIZE: i64 = 1_136_580_921; // what den-remux actually served
        let sid = sid_series(1, 3);
        let s = RawStream { info_hash: "fauda-s1e3".into(), size_bytes: Some(PACK_HINT), ..Default::default() };
        let cache = seeded_cache(std::slice::from_ref(&s), &format!(r#"{{"sizeBytes":{EPISODE_SIZE}}}"#));
        let h = Handler::for_test(Deps { cache, ..Default::default() });
        let mut streams = vec![s];
        h.hydrate_probe_facts(&mut streams, Some(&sid));
        assert_eq!(streams[0].size_bytes, Some(EPISODE_SIZE), "SizeBytes = {:?}, want the probed episode size", streams[0].size_bytes);
        assert_eq!(streams[0].pack_size_bytes, Some(PACK_HINT), "PackSizeBytes = {:?}, want the indexer's pack size kept as context", streams[0].pack_size_bytes);
    }
    todo!("pending: probe_fanout_test.go TestHydrateProbeFacts_seasonPackSizeCorrectsToTheEpisode")
}

// Fauda S1E3, again: the Fauda install never got a corrected size because its TorBox mylist kept
// refusing the probe's own resolve. TorBox's resolve entry, cached from an EARLIER attempt, names the
// episode's own bytes in its file listing, so hydrating known sizes must correct SizeBytes from it without
// needing a fresh resolve to succeed at all.
#[test]
#[ignore = "needs handler::hydrateKnownSizes, the TorBox store and resolveKey from packages 1 and 4"]
fn hydrate_known_sizes_corrects_from_a_stores_file_list_with_no_probe() {
    #[cfg(any())]
    fn pending() {
        const PACK_HINT: i64 = 68 << 30; // what "💾 68 GB" in the title parsed to
        const EPISODE_SIZE: i64 = 1_136_580_921; // what TorBox's own file listing gives for the episode
        let cache = memory_cache();
        cache.put(
            &resolve_key("t", H),
            &format!(r#"{{"torrentId":9,"files":[{{"Index":0,"Name":"Fauda.S01E03.WEB-DL.1080p.mkv","SizeBytes":{EPISODE_SIZE}}}]}}"#),
            RESOLVE_CACHE_TTL,
        );
        let sid = sid_series(1, 3);
        let mut streams = vec![RawStream { info_hash: H.into(), size_bytes: Some(PACK_HINT), ..Default::default() }];
        let h = Handler::for_test(Deps { make_stores: Box::new(move |_| vec![torbox_store("t", cache.clone())]), ..Default::default() });
        h.hydrate_known_sizes(&Config::default(), &mut streams, Some(&sid));
        assert_eq!(streams[0].size_bytes, Some(EPISODE_SIZE), "SizeBytes = {:?}, want the store's own {EPISODE_SIZE}", streams[0].size_bytes);
        assert_eq!(streams[0].pack_size_bytes, Some(PACK_HINT), "PackSizeBytes = {:?}, want the indexer's pack size kept as context", streams[0].pack_size_bytes);
    }
    todo!("pending: probe_fanout_test.go TestHydrateKnownSizes_correctsFromAStoresFileListWithNoProbe")
}

// A ranged probe measured the file the playback URL actually serves. Store metadata is useful only
// before that measurement exists: an older or mistaken cached file-list choice must not replace the
// probe with another file's size merely because the difference was too small to be called a pack.
#[test]
#[ignore = "needs handler::hydrateProbeFacts, handler::hydrateKnownSizes and the TorBox store from packages 1 and 4"]
fn hydrate_known_sizes_does_not_replace_a_probed_size() {
    #[cfg(any())]
    fn pending() {
        const INDEXER_SIZE: i64 = 1_200_000_000;
        const PROBED_SIZE: i64 = 1_100_000_000;
        const STALE_STORE_SIZE: i64 = 10_000_000_000;
        let sid = sid_series(1, 3);
        let stream = RawStream { info_hash: H.into(), size_bytes: Some(INDEXER_SIZE), ..Default::default() };
        let cache = memory_cache();
        cache.put(&probe_cache_key(&stream, Some(&sid)), &format!(r#"{{"sizeBytes":{PROBED_SIZE}}}"#), PROBE_TTL);
        cache.put(
            &resolve_key("t", H),
            &format!(r#"{{"torrentId":9,"files":[{{"Index":0,"Name":"Show.S01E03.mkv","SizeBytes":{STALE_STORE_SIZE}}}]}}"#),
            RESOLVE_CACHE_TTL,
        );
        let h = Handler::for_test(Deps {
            cache: cache.clone(),
            make_stores: Box::new(move |_| vec![torbox_store("t", cache.clone())]),
            ..Default::default()
        });
        let mut streams = vec![stream];
        h.hydrate_probe_facts(&mut streams, Some(&sid));
        h.hydrate_known_sizes(&Config::default(), &mut streams, Some(&sid));
        assert_eq!(streams[0].probe.as_ref().map(|p| p.size_bytes), Some(PROBED_SIZE), "Probe = {:?}, want the ranged measurement", streams[0].probe);
        assert_eq!(streams[0].size_bytes, Some(PROBED_SIZE), "SizeBytes = {:?}, want the probed size to win over cached {STALE_STORE_SIZE}", streams[0].size_bytes);
    }
    todo!("pending: probe_fanout_test.go TestHydrateKnownSizes_doesNotReplaceAProbedSize")
}

// A movie never gets a KnownFileSize lookup at all — there is no pack for its title size to have been
// confused with, and a movie resolves without listing files in the first place.
#[test]
#[ignore = "needs handler::hydrateKnownSizes and Deps from package 4 (handler)"]
fn hydrate_known_sizes_movies_are_skipped() {
    #[cfg(any())]
    fn pending() {
        let resolves = std::cell::Cell::new(0);
        let mut streams = vec![RawStream { info_hash: H.into(), size_bytes: Some(18 << 30), ..Default::default() }];
        let h = Handler::for_test(Deps {
            make_stores: Box::new(|_| {
                resolves.set(resolves.get() + 1);
                vec![]
            }),
            ..Default::default()
        });
        h.hydrate_known_sizes(&Config::default(), &mut streams, Some(&sid_movie()));
        assert_eq!(resolves.get(), 0, "built a store pool for a movie, which has no pack to correct");
        assert_eq!(streams[0].size_bytes, Some(18 << 30), "SizeBytes changed for a movie");
    }
    todo!("pending: probe_fanout_test.go TestHydrateKnownSizes_moviesAreSkipped")
}

// A movie's title size and its probed size agree to the byte (or close enough to be rounding); there is
// no pack behind a single-file release, so no PackSizeBytes should appear.
#[test]
#[ignore = "needs handler::hydrateProbeFacts and MemoryCache from packages 4 and 6"]
fn hydrate_probe_facts_movie_gets_no_pack_size() {
    #[cfg(any())]
    fn pending() {
        const SIZE: i64 = 18 << 30;
        let sid = sid_movie();
        let s = RawStream { info_hash: "movie-hash".into(), size_bytes: Some(SIZE), ..Default::default() };
        let cache = seeded_cache(std::slice::from_ref(&s), &format!(r#"{{"sizeBytes":{SIZE}}}"#));
        let h = Handler::for_test(Deps { cache, ..Default::default() });
        let mut streams = vec![s];
        h.hydrate_probe_facts(&mut streams, Some(&sid));
        assert!(streams[0].pack_size_bytes.is_none(), "PackSizeBytes = {:?}, want nil for a movie", streams[0].pack_size_bytes);
        assert_eq!(streams[0].size_bytes, Some(SIZE), "SizeBytes = {:?}", streams[0].size_bytes);
    }
    todo!("pending: probe_fanout_test.go TestHydrateProbeFacts_movieGetsNoPackSize")
}

// A release the store does not hold is never probed at all.
//
// Probing resolves, and resolving a release the account does not hold ADDS it — so this read path was
// queueing up to six torrents per newly-viewed title against a sixty-an-hour ceiling.
#[test]
#[ignore = "needs handler::probeTop and the fakeStore fixture from packages 2 and 4"]
fn probe_top_uncached_is_never_probed() {
    #[cfg(any())]
    fn pending() {
        use std::time::Duration;
        // Go: an uncached release is resolved by no store and gets no Probe; the resolve channel stays empty.
        let (tx, rx) = std::sync::mpsc::channel();
        let h = Handler::for_test(Deps {
            probe_client: Some(http_default_client()),
            make_stores: Box::new(move |_| vec![fake_store_recording("torbox", tx.clone(), "")]),
            ..Default::default()
        });
        let mut streams = vec![RawStream { info_hash: "uncached-hash".into(), cached: false, ..Default::default() }];
        h.probe_top(&Config::default(), &mut streams, None, &CacheTruth::default());
        assert!(streams[0].probe.is_none(), "an uncached release was probed");
        assert!(rx.recv_timeout(Duration::from_millis(500)).is_err(), "an uncached release was resolved — that adds the torrent and spends the add quota");
    }
    todo!("pending: probe_fanout_test.go TestProbeTop_uncachedIsNeverProbed")
}

// A HELD release still probes behind the response. Probing costs a ranged read apiece, which put ~12s on
// top of the scrape and timed clients out; the list goes out first and the probe warms the cache for the
// next request.
#[test]
#[ignore = "needs handler::probeTop and the fakeStore fixture from packages 2 and 4"]
fn probe_top_cached_probes_behind_the_response() {
    #[cfg(any())]
    fn pending() {
        use std::time::Duration;
        // Go: a held release returns from probeTop with no inline Probe; the background resolve still runs.
        let (tx, rx) = std::sync::mpsc::channel();
        let h = Handler::for_test(Deps {
            probe_client: Some(http_default_client()),
            make_stores: Box::new(move |_| vec![fake_store_recording("torbox", tx.clone(), "")]),
            ..Default::default()
        });
        let mut streams = vec![RawStream { info_hash: "held-hash".into(), cached: true, ..Default::default() }];
        h.probe_top(&Config::default(), &mut streams, None, &held_on_torbox(&["held-hash"]));
        assert!(streams[0].probe.is_none(), "a held release was probed inline");
        assert!(rx.recv_timeout(Duration::from_secs(5)).is_ok(), "the background probe never ran — the cache would never warm");
    }
    todo!("pending: probe_fanout_test.go TestProbeTop_cachedProbesBehindTheResponse")
}

// Fauda S1E3: a resolve that fails behind the response left no trace anywhere. This is the line that says so.
#[test]
#[ignore = "needs handler::probeTop, the log capture helper and the StoreUnavailableError fixture from packages 2, 4 and 6"]
fn probe_behind_logs_a_failed_resolve() {
    #[cfg(any())]
    fn pending() {
        use std::time::{Duration, Instant};
        // Go: a resolve that fails with "mylist gave no usable answer" must log "could not resolve held-hash".
        // The probe-resolve-failed log key is aged first so an earlier test in the run cannot silence the line.
        let out = capture_log();
        age_log_limit("probe-resolve-failed");
        let h = Handler::for_test(Deps {
            probe_client: Some(http_default_client()),
            make_stores: Box::new(|_| vec![fake_store_failing("torbox", "mylist gave no usable answer")]),
            ..Default::default()
        });
        let mut streams = vec![RawStream { info_hash: "held-hash".into(), cached: true, ..Default::default() }];
        h.probe_top(&Config::default(), &mut streams, None, &held_on_torbox(&["held-hash"]));
        let deadline = Instant::now() + Duration::from_secs(5);
        while !out.contents().contains("could not resolve held-hash") {
            assert!(Instant::now() < deadline, "no failed-resolve line logged: {:?}", out.contents());
            std::thread::sleep(Duration::from_millis(10));
        }
    }
    todo!("pending: probe_fanout_test.go TestProbeBehind_logsAFailedResolve")
}

// A panic while probing must cost the probe, not the process.
//
// This work runs on a background task, and the service's only recover() sits on the request goroutine, so
// before this one release with a header that upset a parser took down playback for everyone.
#[test]
#[ignore = "needs handler::probeTop and a panicking transport fixture from package 4"]
fn probe_behind_panic_does_not_kill_the_process() {
    #[cfg(any())]
    fn pending() {
        use std::time::Duration;
        // Go: the probe client's transport panics on "malformed head". The background probe must still run
        // (the resolve channel fires), and a second probeTop call afterwards must still work.
        let (tx, rx) = std::sync::mpsc::channel();
        let h = Handler::for_test(Deps {
            probe_client: Some(panicking_client("malformed head")),
            make_stores: Box::new(move |_| vec![fake_store_recording("torbox", tx.clone(), "https://debrid.example/file.mkv")]),
            ..Default::default()
        });
        let mut streams = vec![RawStream { info_hash: "panic-hash".into(), cached: true, ..Default::default() }];
        h.probe_top(&Config::default(), &mut streams, None, &held_on_torbox(&["panic-hash"]));
        assert!(rx.recv_timeout(Duration::from_secs(5)).is_ok(), "the background probe never ran");
        // The handler is still usable afterwards: a poisoned singleflight would show up as this second call hanging.
        let mut other = vec![RawStream { info_hash: "other".into(), cached: true, ..Default::default() }];
        h.probe_top(&Config::default(), &mut other, None, &held_on_torbox(&["other"]));
    }
    todo!("pending: probe_fanout_test.go TestProbeBehind_panicDoesNotKillTheProcess")
}

// The same guarantee for the parent task, which builds the store pool before any job starts — a panic there
// is raised outside every job, so the per-job recover cannot see it.
#[test]
#[ignore = "needs handler::probeTop and Deps from package 4 (handler)"]
fn probe_behind_panic_building_the_store_pool() {
    #[cfg(any())]
    fn pending() {
        use std::time::Duration;
        // Go: MakeStores panics; the probe must still have been attempted (built) and the process must survive.
        let (tx, rx) = std::sync::mpsc::channel::<()>();
        let h = Handler::for_test(Deps {
            probe_client: Some(http_default_client()),
            make_stores: Box::new(move |_| {
                let _ = tx.send(());
                panic!("store construction failed")
            }),
            ..Default::default()
        });
        let mut streams = vec![RawStream { info_hash: "pool-hash".into(), cached: true, ..Default::default() }];
        h.probe_top(&Config::default(), &mut streams, None, &held_on_torbox(&["pool-hash"]));
        assert!(rx.recv_timeout(Duration::from_secs(5)).is_ok(), "the background probe never ran");
        std::thread::sleep(Duration::from_millis(50));
    }
    todo!("pending: probe_fanout_test.go TestProbeBehind_panicBuildingTheStorePool")
}

// The probe resolves ONLY against a store that holds the release — never through the pool.
//
// `Cached` is the union across accounts, so with two configured, a release only the SECOND holds still
// reads as cached. Resolving that through the pool reaches the first account in priority order, and an
// account that does not hold a torrent does not fetch it — it ADDS it.
#[test]
#[ignore = "needs handler::probeTop, CacheTruth holders and the fakeStore fixtures from packages 2 and 4"]
fn probe_top_resolves_only_against_a_holding_store() {
    #[cfg(any())]
    fn pending() {
        use std::time::{Duration, Instant};
        // Go: held on Premiumize only, configured TorBox first. The probe must be asked of Premiumize and never TorBox.
        let asked = std::sync::Arc::new(std::sync::Mutex::new(Vec::<&'static str>::new()));
        let (a1, a2) = (asked.clone(), asked.clone());
        let h = Handler::for_test(Deps {
            probe_client: Some(http_default_client()),
            make_stores: Box::new(move |_| {
                vec![
                    fake_store_asking("torbox", a1.clone(), ""),
                    fake_store_asking("premiumize", a2.clone(), ""),
                ]
            }),
            ..Default::default()
        });
        let truth = CacheTruth::for_test(&[("pm-only", &[DebridService::Premiumize])]);
        let mut streams = vec![RawStream { info_hash: "pm-only".into(), cached: true, ..Default::default() }];
        h.probe_top(&Config::default(), &mut streams, None, &truth);
        let deadline = Instant::now() + Duration::from_secs(5);
        while asked.lock().expect("lock").is_empty() {
            assert!(Instant::now() < deadline, "the background probe never ran");
            std::thread::sleep(Duration::from_millis(10));
        }
        std::thread::sleep(Duration::from_millis(100));
        let got = asked.lock().expect("lock").clone();
        assert!(!got.contains(&"torbox"), "probed through TorBox, which does not hold this — that ADDS the torrent. asked={got:?}");
    }
    todo!("pending: probe_fanout_test.go TestProbeTop_resolvesOnlyAgainstAHoldingStore")
}

// The probe marks its target NoAdd, so the store refuses rather than queueing.
//
// Choosing only held releases was not enough: TorBox's cache check reports what TORBOX has, not what this
// ACCOUNT has, so a "held" release with no warm resolve entry went straight to createtorrent.
#[test]
#[ignore = "needs handler::probeTop and the capturing store fixture from packages 2 and 4"]
fn probe_top_marks_its_target_no_add() {
    #[cfg(any())]
    fn pending() {
        use std::time::Duration;
        // Go: the store sees the probe's ResolveTarget; its NoAdd must be true.
        let (tx, rx) = std::sync::mpsc::channel();
        let h = Handler::for_test(Deps {
            probe_client: Some(http_default_client()),
            make_stores: Box::new(move |_| vec![capturing_store("torbox", tx.clone())]),
            ..Default::default()
        });
        let mut streams = vec![RawStream { info_hash: "held-hash".into(), cached: true, ..Default::default() }];
        h.probe_top(&Config::default(), &mut streams, None, &held_on_torbox(&["held-hash"]));
        match rx.recv_timeout(Duration::from_secs(5)) {
            Ok(target) => assert!(target.no_add, "the probe resolved without NoAdd — the store is free to queue the torrent"),
            Err(_) => panic!("the background probe never ran"),
        }
    }
    todo!("pending: probe_fanout_test.go TestProbeTop_marksItsTargetNoAdd")
}
