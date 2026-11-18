//! Go: `internal/scout/probe_fanout_test.go` (618 lines).
//!
//! The handler-driven tests call `Handler::probe_top`, `hydrate_probe_facts` and `hydrate_known_sizes` directly, as
//! Go's do on a `*handler`. The probe runs behind the response on a background task, so those are async tests on a
//! current-thread runtime: the task makes progress whenever the test awaits.

use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use crate::attributes::with_probe;
use crate::attributes::StreamAttributes;
use crate::cache::{Cache, MemoryCache};
use crate::client::ClientPlayable;
use crate::config::{Config, DebridService};
use crate::handler::{new_handler, Handler};
use crate::id::StreamId;
use crate::probe::Probe;
use crate::probe_fanout::{
    apply_probed_size, browser_probe_candidates, hydrate_pack_only_sizes, probe_cache_key,
    BROWSER_PROBE_FRONTIER, PROBE_TTL,
};
use crate::rank::RawStream;
use crate::stores::refusal::resolve_key;
use crate::stores::torbox::{TorBoxStore, RESOLVE_CACHE_TTL, TORBOX_API};
use crate::stores::{CacheTruth, ResolveTarget, Store, StoreError, StoreUnavailableError};

use super::handler::test_deps;
use super::mock::{resp, MockDoer};
use super::rank::intp;
use super::refusal::Scripted;
use super::reqlog::{forget_logged, LogCapture};

/// Go `H` (stores_test.go).
const H: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";

/// Go `heldOnTorBox`: the cache truth a probe needs — these hashes are confirmed present on the TorBox account, so
/// resolving them is a read rather than an add.
fn held_on_torbox(hashes: &[&str]) -> CacheTruth {
    CacheTruth {
        holders: hashes.iter().map(|h| ((*h).to_owned(), vec![DebridService::TorBox])).collect(),
        known: hashes.iter().map(|h| ((*h).to_owned(), true)).collect(),
        ..Default::default()
    }
}

/// A cache holding `value` under each stream's probe key, so a probe is answered without a resolve.
fn seeded_cache(streams: &[RawStream], value: &str) -> Arc<dyn Cache> {
    let cache = Arc::new(MemoryCache::new(1 << 20));
    for s in streams {
        cache.put(&probe_cache_key(s, None), value, PROBE_TTL);
    }
    cache
}

/// A `make_stores` over one real TorBox store that has a cache and no network.
fn torbox_stores(
    token: &str,
    cache: &Arc<dyn Cache>,
) -> Arc<dyn Fn(&Config) -> Vec<Box<dyn Store>> + Send + Sync> {
    let (token, cache) = (token.to_owned(), cache.clone());
    Arc::new(move |_| {
        vec![Box::new(TorBoxStore {
            token: token.clone(),
            cache: Some(cache.clone()),
            api: TORBOX_API.into(),
            client: Arc::new(MockDoer(|_| Ok(resp(404, "{}")))),
        }) as Box<dyn Store>]
    })
}

/// A store whose resolve is `resolve`, ignoring the target.
fn fake(
    svc: DebridService,
    resolve: impl Fn() -> Result<String, StoreError> + Send + Sync + 'static,
) -> Scripted {
    Scripted::new(svc).resolve_with(move |_| resolve())
}

/// A handler with a probe client, over the stores `make` builds.
fn probing_handler(make: impl Fn(&Config) -> Vec<Box<dyn Store>> + Send + Sync + 'static) -> Handler {
    new_handler(test_deps(move |d| {
        d.probe_client = Some(reqwest::Client::new());
        d.make_stores = Arc::new(make);
    }))
}

/// Polls `cond` until it holds, failing with `what` after five seconds.
async fn wait_for(cond: impl Fn() -> bool, what: &str) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while !cond() {
        assert!(tokio::time::Instant::now() < deadline, "{what}");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

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
    assert!(
        got.audio_languages.len() == 1 && got.audio_languages[0] == "sv",
        "languages missing: {:?}",
        got.audio_languages
    );
    let untouched = with_probe(base, None);
    assert!(
        !untouched.probed
            && untouched.audio_languages.is_empty()
            && untouched.codec.as_deref() == Some("h264"),
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
    assert_ne!(
        probe_cache_key(&a, None),
        probe_cache_key(&c, None),
        "different files in one torrent share a key"
    );
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

    assert!(
        s.pack_size_bytes.is_none(),
        "PackSizeBytes = {:?}, want nil for a close, non-pack mismatch",
        s.pack_size_bytes
    );
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

    assert!(
        streams[0].size_bytes.is_none(),
        "SizeBytes = {:?}, want nil (unknown) rather than the pack's own total",
        streams[0].size_bytes
    );
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

    assert_eq!(
        streams[0].size_bytes,
        Some(1_136_580_921),
        "SizeBytes = {:?}, want the indexer's own untouched",
        streams[0].size_bytes
    );
    assert!(
        streams[0].pack_size_bytes.is_none(),
        "PackSizeBytes = {:?}, want nil — this title names exactly this episode",
        streams[0].pack_size_bytes
    );
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

    assert_eq!(
        streams[0].size_bytes,
        Some(18 << 30),
        "SizeBytes changed for a movie: {:?}",
        streams[0].size_bytes
    );
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
    assert_eq!(
        frontier.len(),
        BROWSER_PROBE_FRONTIER,
        "frontier has {} candidates, want the bound {BROWSER_PROBE_FRONTIER}: {frontier:?}",
        frontier.len()
    );
    assert!(frontier.contains(&0), "the unresolved current winner is absent from its own decision frontier");
    assert!(
        frontier.contains(&1),
        "a compatibility-sunk 4K contender that can displace the winner was not selected"
    );
}

// Probing resolves, and resolving a release the account does not hold ADDS it, so these handler-driven
// tests need `handler::probeTop`, `Deps` and the store fakes from package 4 (handler) and packages 1/2.
#[tokio::test(start_paused = true)]
async fn probe_top_opt_in() {
    // Probing costs a debrid RESOLVE per release, so it must never happen by accident. A caller that hasn't
    // wired a probe client gets the old behaviour exactly — the list is built without touching the account.
    let resolves = Arc::new(AtomicUsize::new(0));
    let counter = resolves.clone();
    let h = new_handler(test_deps(move |d| {
        d.make_stores = Arc::new(move |_| {
            counter.fetch_add(1, Ordering::SeqCst);
            Vec::new()
        });
    }));
    let mut streams = vec![RawStream { info_hash: "abc".into(), ..Default::default() }];
    h.probe_top(&Config::default(), &mut streams, None, &held_on_torbox(&["abc"]));
    // Behind the response is where a probe would run; give it every chance to.
    tokio::time::sleep(Duration::from_millis(100)).await;
    let n = resolves.load(Ordering::SeqCst);
    assert!(
        n == 0 && streams[0].probe.is_none(),
        "probed without a client: resolves={n} probe={:?}",
        streams[0].probe
    );
}

// Every already-cached fact is hydrated before ranking; this is local cache IO, not a debrid probe.
#[tokio::test]
async fn probe_top_caps_at_top_n() {
    let mut streams: Vec<RawStream> =
        (0..20).map(|_| RawStream { info_hash: "hash".into(), cached: true, ..Default::default() }).collect();
    let cache = seeded_cache(&streams, r#"{"audioLanguages":["sv"]}"#);
    // Pre-seed every entry so no resolve is attempted, then count how many were consulted.
    let h = new_handler(test_deps(move |d| {
        d.cache = cache;
        d.probe_client = Some(reqwest::Client::new());
        d.make_stores = Arc::new(|_| Vec::new());
    }));
    h.probe_top(&Config::default(), &mut streams, None, &held_on_torbox(&["hash"]));

    let probed = streams.iter().filter(|s| s.probe.is_some()).count();
    assert_eq!(probed, streams.len(), "hydrated {probed} cached probe facts, want all {}", streams.len());
}

// A cached probe must not cost a resolve — that is what makes the disk tier worth having across restarts.
#[tokio::test]
async fn probe_top_cache_hit_skips_resolve() {
    let s = RawStream { info_hash: "cached-hash".into(), cached: true, ..Default::default() };
    let cache = seeded_cache(std::slice::from_ref(&s), r#"{"audioLanguages":["it"],"videoCodec":"h264"}"#);
    let made = Arc::new(AtomicUsize::new(0));
    let counter = made.clone();
    let h = new_handler(test_deps(move |d| {
        d.cache = cache;
        d.probe_client = Some(reqwest::Client::new());
        d.make_stores = Arc::new(move |_| {
            counter.fetch_add(1, Ordering::SeqCst);
            Vec::new()
        });
    }));
    let mut streams = vec![s];
    h.probe_top(&Config::default(), &mut streams, None, &held_on_torbox(&["cached-hash"]));

    let n = made.load(Ordering::SeqCst);
    assert_eq!(n, 0, "built a store pool despite a cache hit ({n})");
    let p = streams[0].probe.as_ref();
    assert!(matches!(p, Some(p) if p.audio.len() == 1 && p.audio[0] == "it"), "cache hit not applied: {p:?}");
}

// Fauda S1E3: the indexer's title gave the season pack's 68 GB, and a probe of the resolved link read
// the episode's own 1.1 GB. Hydrating that cached probe fact must correct SizeBytes to the episode and
// keep the pack's number only as context.
#[tokio::test]
async fn hydrate_probe_facts_season_pack_size_corrects_to_the_episode() {
    const PACK_HINT: i64 = 68 << 30; // what "💾 68 GB" in the title parsed to
    const EPISODE_SIZE: i64 = 1_136_580_921; // what den-remux actually served
    let sid = sid_series(1, 3);
    let s = RawStream { info_hash: "fauda-s1e3".into(), size_bytes: Some(PACK_HINT), ..Default::default() };
    let cache = Arc::new(MemoryCache::new(1 << 20));
    cache.put(&probe_cache_key(&s, Some(&sid)), &format!(r#"{{"sizeBytes":{EPISODE_SIZE}}}"#), PROBE_TTL);
    let h = new_handler(test_deps(move |d| d.cache = cache));
    let mut streams = vec![s];

    h.hydrate_probe_facts(&mut streams, Some(&sid));

    assert_eq!(
        streams[0].size_bytes,
        Some(EPISODE_SIZE),
        "SizeBytes = {:?}, want the probed episode size {EPISODE_SIZE}",
        streams[0].size_bytes
    );
    assert_eq!(
        streams[0].pack_size_bytes,
        Some(PACK_HINT),
        "PackSizeBytes = {:?}, want the indexer's pack size {PACK_HINT} kept as context",
        streams[0].pack_size_bytes
    );
}

// Fauda S1E3, again: the Fauda install never got a corrected size because its TorBox mylist kept
// refusing the probe's own resolve. TorBox's resolve entry, cached from an EARLIER attempt, names the
// episode's own bytes in its file listing, so hydrating known sizes must correct SizeBytes from it without
// needing a fresh resolve to succeed at all.
#[tokio::test]
async fn hydrate_known_sizes_corrects_from_a_stores_file_list_with_no_probe() {
    const PACK_HINT: i64 = 68 << 30; // what "💾 68 GB" in the title parsed to
    const EPISODE_SIZE: i64 = 1_136_580_921; // what TorBox's own file listing gives for the episode
    let cache: Arc<dyn Cache> = Arc::new(MemoryCache::new(1 << 20));
    cache.put(
        &resolve_key("t", H),
        &format!(r#"{{"torrentId":9,"files":[{{"Index":0,"Name":"Fauda.S01E03.WEB-DL.1080p.mkv","SizeBytes":{EPISODE_SIZE}}}]}}"#),
        RESOLVE_CACHE_TTL,
    );
    let sid = sid_series(1, 3);
    let mut streams =
        vec![RawStream { info_hash: H.into(), size_bytes: Some(PACK_HINT), ..Default::default() }];
    let h = new_handler(test_deps(move |d| d.make_stores = torbox_stores("t", &cache)));

    h.hydrate_known_sizes(&Config::default(), &mut streams, Some(&sid));

    assert_eq!(
        streams[0].size_bytes,
        Some(EPISODE_SIZE),
        "SizeBytes = {:?}, want the store's own {EPISODE_SIZE}",
        streams[0].size_bytes
    );
    assert_eq!(
        streams[0].pack_size_bytes,
        Some(PACK_HINT),
        "PackSizeBytes = {:?}, want the indexer's pack size {PACK_HINT} kept as context",
        streams[0].pack_size_bytes
    );
}

// A ranged probe measured the file the playback URL actually serves. Store metadata is useful only
// before that measurement exists: an older or mistaken cached file-list choice must not replace the
// probe with another file's size merely because the difference was too small to be called a pack.
#[tokio::test]
async fn hydrate_known_sizes_does_not_replace_a_probed_size() {
    const INDEXER_SIZE: i64 = 1_200_000_000;
    const PROBED_SIZE: i64 = 1_100_000_000;
    const STALE_STORE_SIZE: i64 = 10_000_000_000;
    let cache: Arc<dyn Cache> = Arc::new(MemoryCache::new(1 << 20));
    let sid = sid_series(1, 3);
    let stream = RawStream { info_hash: H.into(), size_bytes: Some(INDEXER_SIZE), ..Default::default() };
    cache.put(&probe_cache_key(&stream, Some(&sid)), &format!(r#"{{"sizeBytes":{PROBED_SIZE}}}"#), PROBE_TTL);
    cache.put(
        &resolve_key("t", H),
        &format!(r#"{{"torrentId":9,"files":[{{"Index":0,"Name":"Show.S01E03.mkv","SizeBytes":{STALE_STORE_SIZE}}}]}}"#),
        RESOLVE_CACHE_TTL,
    );
    let h = new_handler(test_deps({
        let cache = cache.clone();
        move |d| {
            d.cache = cache.clone();
            d.make_stores = torbox_stores("t", &cache);
        }
    }));
    let mut streams = vec![stream];

    h.hydrate_probe_facts(&mut streams, Some(&sid));
    h.hydrate_known_sizes(&Config::default(), &mut streams, Some(&sid));

    let probed = streams[0].probe.as_ref().map(|p| p.size_bytes);
    assert_eq!(
        probed,
        Some(PROBED_SIZE),
        "Probe size = {probed:?}, want the ranged measurement {PROBED_SIZE}"
    );
    assert_eq!(
        streams[0].size_bytes,
        Some(PROBED_SIZE),
        "SizeBytes = {:?}, want the probed size {PROBED_SIZE} to win over cached {STALE_STORE_SIZE}",
        streams[0].size_bytes
    );
}

// A movie never gets a KnownFileSize lookup at all — there is no pack for its title size to have been
// confused with, and a movie resolves without listing files in the first place.
#[tokio::test]
async fn hydrate_known_sizes_movies_are_skipped() {
    let resolves = Arc::new(AtomicUsize::new(0));
    let counter = resolves.clone();
    let mut streams =
        vec![RawStream { info_hash: H.into(), size_bytes: Some(18 << 30), ..Default::default() }];
    let h = new_handler(test_deps(move |d| {
        d.make_stores = Arc::new(move |_| {
            counter.fetch_add(1, Ordering::SeqCst);
            Vec::new()
        });
    }));

    h.hydrate_known_sizes(&Config::default(), &mut streams, Some(&sid_movie()));

    assert_eq!(
        resolves.load(Ordering::SeqCst),
        0,
        "built a store pool for a movie, which has no pack to correct"
    );
    assert_eq!(
        streams[0].size_bytes,
        Some(18 << 30),
        "SizeBytes changed for a movie: {:?}",
        streams[0].size_bytes
    );
}

// A movie's title size and its probed size agree to the byte (or close enough to be rounding); there is
// no pack behind a single-file release, so no PackSizeBytes should appear.
#[tokio::test]
async fn hydrate_probe_facts_movie_gets_no_pack_size() {
    const SIZE: i64 = 18 << 30;
    let sid = sid_movie();
    let s = RawStream { info_hash: "movie-hash".into(), size_bytes: Some(SIZE), ..Default::default() };
    let cache = Arc::new(MemoryCache::new(1 << 20));
    cache.put(&probe_cache_key(&s, Some(&sid)), &format!(r#"{{"sizeBytes":{SIZE}}}"#), PROBE_TTL);
    let h = new_handler(test_deps(move |d| d.cache = cache));
    let mut streams = vec![s];

    h.hydrate_probe_facts(&mut streams, Some(&sid));

    assert!(
        streams[0].pack_size_bytes.is_none(),
        "PackSizeBytes = {:?}, want nil for a movie",
        streams[0].pack_size_bytes
    );
    assert_eq!(streams[0].size_bytes, Some(SIZE), "SizeBytes = {:?}, want {SIZE}", streams[0].size_bytes);
}

// A release the store does not hold is never probed at all.
//
// Probing resolves, and resolving a release the account does not hold ADDS it — so this read path was
// queueing up to six torrents per newly-viewed title against a sixty-an-hour ceiling.
#[tokio::test(start_paused = true)]
async fn probe_top_uncached_is_never_probed() {
    let resolved = Arc::new(AtomicUsize::new(0));
    let counter = resolved.clone();
    let h = probing_handler(move |_| {
        vec![Box::new(fake(DebridService::TorBox, {
            let counter = counter.clone();
            move || {
                counter.fetch_add(1, Ordering::SeqCst);
                Ok(String::new())
            }
        })) as Box<dyn Store>]
    });
    let mut streams =
        vec![RawStream { info_hash: "uncached-hash".into(), cached: false, ..Default::default() }];
    h.probe_top(&Config::default(), &mut streams, None, &CacheTruth::default());

    assert!(streams[0].probe.is_none(), "an uncached release was probed: {:?}", streams[0].probe);
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert_eq!(
        resolved.load(Ordering::SeqCst),
        0,
        "an uncached release was resolved — that adds the torrent and spends the add quota"
    );
}

// A HELD release still probes behind the response. Probing costs a ranged read apiece, which put ~12s on
// top of the scrape and timed clients out; the list goes out first and the probe warms the cache for the
// next request.
#[tokio::test]
async fn probe_top_cached_probes_behind_the_response() {
    let resolved = Arc::new(AtomicUsize::new(0));
    let counter = resolved.clone();
    let h = probing_handler(move |_| {
        vec![Box::new(fake(DebridService::TorBox, {
            let counter = counter.clone();
            // no link → nothing to probe, but the attempt is what's under test
            move || {
                counter.fetch_add(1, Ordering::SeqCst);
                Ok(String::new())
            }
        })) as Box<dyn Store>]
    });
    let mut streams = vec![RawStream { info_hash: "held-hash".into(), cached: true, ..Default::default() }];
    h.probe_top(&Config::default(), &mut streams, None, &held_on_torbox(&["held-hash"]));

    // Returned WITHOUT the probe: that is the whole point — the viewer gets the list now.
    assert!(streams[0].probe.is_none(), "a held release was probed inline: {:?}", streams[0].probe);
    // …and the work still happens, just not in the client's way.
    wait_for(
        || resolved.load(Ordering::SeqCst) > 0,
        "the background probe never ran — the cache would never warm",
    )
    .await;
}

// Fauda S1E3: a resolve that fails behind the response left no trace anywhere. This is the line that says so.
#[tokio::test]
async fn probe_behind_logs_a_failed_resolve() {
    let out = LogCapture::start();
    // Several other tests in this file resolve to an empty link too (no error of their own interest), which trips
    // the same logLimited key — forgotten, so an earlier one in this run cannot silence this assertion under the
    // one-line-a-minute rule.
    forget_logged("probe-resolve-failed");
    let h = probing_handler(|_| {
        vec![Box::new(fake(DebridService::TorBox, || {
            Err(StoreError::Unavailable(StoreUnavailableError {
                service: DebridService::TorBox,
                reason: "mylist gave no usable answer".into(),
                status: 0,
            }))
        })) as Box<dyn Store>]
    });
    let mut streams = vec![RawStream { info_hash: "held-hash".into(), cached: true, ..Default::default() }];
    h.probe_top(&Config::default(), &mut streams, None, &held_on_torbox(&["held-hash"]));

    wait_for(|| out.text().contains("probe could not resolve a release"), "no failed-resolve line logged")
        .await;
    assert!(!out.text().contains("held-hash"), "the failed-resolve line names no hash");
}

// A panic while probing must cost the probe, not the process.
//
// This work runs on a background task, and the service's only recover() sits on the request goroutine, so
// before this one release with a header that upset a parser took down playback for everyone.
#[tokio::test]
async fn probe_behind_panic_does_not_kill_the_process() {
    let probed = Arc::new(AtomicUsize::new(0));
    let counter = probed.clone();
    let h = probing_handler(move |_| {
        vec![Box::new(fake(DebridService::TorBox, {
            let counter = counter.clone();
            move || {
                counter.fetch_add(1, Ordering::SeqCst);
                panic!("malformed head")
            }
        })) as Box<dyn Store>]
    });
    let mut streams = vec![RawStream { info_hash: "panic-hash".into(), cached: true, ..Default::default() }];
    h.probe_top(&Config::default(), &mut streams, None, &held_on_torbox(&["panic-hash"]));

    wait_for(|| probed.load(Ordering::SeqCst) > 0, "the background probe never ran").await;
    // The handler is still usable afterwards — a dead process would never get here, but a poisoned singleflight or
    // a half-released pool would show up as this second call hanging or panicking.
    let mut other = vec![RawStream { info_hash: "other".into(), cached: true, ..Default::default() }];
    h.probe_top(&Config::default(), &mut other, None, &held_on_torbox(&["other"]));
}

// The same guarantee for the parent task, which builds the store pool before any job starts — a panic there
// is raised outside every job, so the per-job recover cannot see it.
#[tokio::test]
async fn probe_behind_panic_building_the_store_pool() {
    let built = Arc::new(AtomicUsize::new(0));
    let counter = built.clone();
    let h = probing_handler(move |_| {
        counter.fetch_add(1, Ordering::SeqCst);
        panic!("store construction failed")
    });
    let mut streams = vec![RawStream { info_hash: "pool-hash".into(), cached: true, ..Default::default() }];
    h.probe_top(&Config::default(), &mut streams, None, &held_on_torbox(&["pool-hash"]));

    wait_for(|| built.load(Ordering::SeqCst) > 0, "the background probe never ran").await;
    // Give the panic a moment to either be recovered or take the process with it.
    tokio::time::sleep(Duration::from_millis(50)).await;
}

// The probe resolves ONLY against a store that holds the release — never through the pool.
//
// `Cached` is the union across accounts, so with two configured, a release only the SECOND holds still
// reads as cached. Resolving that through the pool reaches the first account in priority order, and an
// account that does not hold a torrent does not fetch it — it ADDS it.
#[tokio::test]
async fn probe_top_resolves_only_against_a_holding_store() {
    let asked: Arc<Mutex<Vec<DebridService>>> = Arc::new(Mutex::new(Vec::new()));
    let record = |svc: DebridService| {
        let asked = asked.clone();
        move || {
            asked.lock().expect("the lock").push(svc);
            Ok(String::new())
        }
    };
    let (tb, pm) = (record(DebridService::TorBox), record(DebridService::Premiumize));
    let h = probing_handler(move |_| {
        vec![
            Box::new(fake(DebridService::TorBox, tb.clone())) as Box<dyn Store>,
            Box::new(fake(DebridService::Premiumize, pm.clone())) as Box<dyn Store>,
        ]
    });
    // Held on Premiumize only — TorBox is configured first and must never be asked.
    let truth = CacheTruth {
        holders: HashMap::from([("pm-only".to_owned(), vec![DebridService::Premiumize])]),
        known: HashMap::from([("pm-only".to_owned(), true)]),
        ..Default::default()
    };
    let mut streams = vec![RawStream { info_hash: "pm-only".into(), cached: true, ..Default::default() }];
    h.probe_top(&Config::default(), &mut streams, None, &truth);

    wait_for(|| !asked.lock().expect("the lock").is_empty(), "the background probe never ran").await;
    tokio::time::sleep(Duration::from_millis(100)).await; // leave room for a wrong second call to happen

    let asked = asked.lock().expect("the lock").clone();
    assert!(
        !asked.contains(&DebridService::TorBox),
        "probed through TorBox, which does not hold this — that ADDS the torrent. asked={asked:?}"
    );
}

// The probe marks its target NoAdd, so the store refuses rather than queueing.
//
// Choosing only held releases was not enough: TorBox's cache check reports what TORBOX has, not what this
// ACCOUNT has, so a "held" release with no warm resolve entry went straight to createtorrent.
#[tokio::test]
async fn probe_top_marks_its_target_no_add() {
    let seen: Arc<Mutex<Vec<ResolveTarget>>> = Arc::new(Mutex::new(Vec::new()));
    let log = seen.clone();
    let h = probing_handler(move |_| {
        let log = log.clone();
        vec![Box::new(Scripted::new(DebridService::TorBox).resolve_with(move |t| {
            log.lock().expect("the lock").push(t.clone());
            Ok(String::new())
        })) as Box<dyn Store>]
    });
    let mut streams = vec![RawStream { info_hash: "held-hash".into(), cached: true, ..Default::default() }];
    h.probe_top(&Config::default(), &mut streams, None, &held_on_torbox(&["held-hash"]));

    wait_for(|| !seen.lock().expect("the lock").is_empty(), "the background probe never ran").await;
    let target = seen.lock().expect("the lock")[0].clone();
    assert!(target.no_add, "the probe resolved without NoAdd — the store is free to queue the torrent");
}

// --- MaxSizeGB and a season pack's total on an exact-episode release (#251) ------------------------------------
//
// Through /stream from cold caches: the scrape, the cache check, ranking, the probe fan-out behind the reply, the
// list cache and its refresh.

mod size_recovery {
    use super::*;

    use async_trait::async_trait;

    use crate::probe::TRACK_TYPE_AUDIO;
    use crate::runtime::ScoutRuntime;
    use crate::scrape::{DoError, Doer};
    use crate::stores::ProviderStatus;
    use crate::tests::attributes_web::ebml;
    use crate::tests::handler::{blob, fake_scraper, get, hdr};
    use den_media::mkv::{
        LANGUAGE as ID_LANGUAGE, SEGMENT as ID_SEGMENT, TRACKS as ID_TRACKS, TRACK_ENTRY as ID_TRACK_ENTRY,
        TRACK_TYPE as ID_TRACK_TYPE,
    };

    const GIB: i64 = 1 << 30;
    /// What "💾 68 GB" on Fauda S1E3's listing parsed to: the season, not the episode.
    const PACK_HINT: i64 = 68 * GIB;
    /// What den-remux actually streamed for it.
    const EPISODE: i64 = 1_136_580_921;
    const CDN: &str = "http://cdn.test";
    const EPISODE_PATH: &str = "tt99:1:3";

    const PACKED: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    const TOO_BIG: &str = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
    const SMALL: &str = "cccccccccccccccccccccccccccccccccccccccc";
    const SEASON: &str = "dddddddddddddddddddddddddddddddddddddddd";
    const UNHELD: &str = "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee";
    const RANGE: &str = "ffffffffffffffffffffffffffffffffffffffff";

    fn release(hash: &str, title: &str, size: Option<i64>) -> RawStream {
        RawStream {
            info_hash: hash.into(),
            title: title.into(),
            size_bytes: size,
            seeders: Some(10),
            file_idx: Some(2),
            ..Default::default()
        }
    }

    /// What the indexer answers for Fauda S1E3, whatever it is asked.
    fn listing() -> Vec<RawStream> {
        vec![
            // The release the budget dropped for good: an exact-episode title carrying the season's total.
            release(PACKED, "Fauda.S01E03.1080p.WEB-DL.x264", Some(PACK_HINT)),
            // Genuinely bigger than the budget, as its own file.
            release(TOO_BIG, "Fauda.S01E03.2160p.WEB-DL.x265", Some(30 * GIB)),
            release(SMALL, "Fauda.S01E03.720p.WEB.x264", Some(2 * GIB)),
            // Titles that name a pack: their number was never the episode's (hydrate_pack_only_sizes).
            release(SEASON, "Fauda.S01.1080p.WEB-DL.x264", Some(PACK_HINT)),
            release(RANGE, "Fauda.S01E01-E06.1080p.WEB-DL.x264", Some(40 * GIB)),
            // Over the budget, but no store holds it: probing it would be an add.
            release(UNHELD, "Fauda.S01E03.1080p.BluRay.x264", Some(50 * GIB)),
        ]
    }

    /// How the CDN answers the ranged probe of one release.
    #[derive(Clone, Copy)]
    enum Serve {
        /// A Matroska head, and the file's total in Content-Range.
        File(i64),
        /// A Matroska head with no total stated.
        NoTotal,
        /// The CDN fails.
        Fails,
        /// A head none of the parsers can read tracks from, with the file's total when one is given.
        Unreadable(Option<i64>),
    }

    fn mkv_head() -> Vec<u8> {
        let audio = ebml(
            ID_TRACK_ENTRY,
            &[ebml(ID_TRACK_TYPE, &[vec![TRACK_TYPE_AUDIO]]), ebml(ID_LANGUAGE, &[b"heb".to_vec()])],
        );
        ebml(ID_SEGMENT, &[ebml(ID_TRACKS, &[audio])])
    }

    /// Serves `<CDN>/<hash>.mkv` as `files` says.
    fn cdn(files: HashMap<&'static str, Serve>) -> Arc<dyn Doer> {
        let head = mkv_head();
        Arc::new(MockDoer(move |req: reqwest::Request| -> Result<reqwest::Response, DoError> {
            let hash = req.url().path().trim_start_matches('/').trim_end_matches(".mkv").to_owned();
            let serve = files.get(hash.as_str()).copied().unwrap_or(Serve::Fails);
            let mut b = http::Response::builder().header("content-type", "video/x-matroska");
            let body = match serve {
                Serve::Unreadable(_) => b"an MP4 whose moov lies past the head".to_vec(),
                _ => head.clone(),
            };
            b = match serve {
                Serve::File(total) | Serve::Unreadable(Some(total)) => {
                    b.status(206).header("content-range", format!("bytes 0-{}/{total}", body.len() - 1))
                }
                Serve::NoTotal | Serve::Unreadable(None) => b.status(206),
                Serve::Fails => return Ok(resp(500, "")),
            };
            Ok(reqwest::Response::from(b.body(body).expect("a valid response")))
        }))
    }

    /// A TorBox account holding `held`. Every resolve is recorded; one of a `gated` release waits until the gate
    /// opens.
    #[derive(Clone)]
    struct Account {
        held: Vec<&'static str>,
        gated: Vec<&'static str>,
        gate: Arc<tokio::sync::Semaphore>,
        resolves: Arc<Mutex<Vec<ResolveTarget>>>,
    }

    impl Account {
        fn new(held: &[&'static str]) -> Account {
            Account {
                held: held.to_vec(),
                gated: Vec::new(),
                gate: Arc::new(tokio::sync::Semaphore::new(0)),
                resolves: Arc::default(),
            }
        }

        fn resolved(&self, hash: &str) -> usize {
            self.resolves.lock().expect("the lock").iter().filter(|t| t.info_hash == hash).count()
        }
    }

    #[async_trait]
    impl Store for Account {
        fn service(&self) -> DebridService {
            DebridService::TorBox
        }

        async fn cache_check(&self, hashes: &[String]) -> Result<HashMap<String, bool>, StoreError> {
            Ok(hashes.iter().map(|h| (h.clone(), self.held.contains(&h.as_str()))).collect())
        }

        async fn resolve(&self, t: &ResolveTarget) -> Result<String, StoreError> {
            self.resolves.lock().expect("the lock").push(t.clone());
            if self.gated.contains(&t.info_hash.as_str()) {
                let _open = self.gate.acquire().await;
            }
            Ok(format!("{CDN}/{}.mkv", t.info_hash))
        }

        async fn provider_status(&self, _t: &ResolveTarget) -> ProviderStatus {
            ProviderStatus::NotFetching
        }
    }

    fn handler(
        account: &Account,
        files: HashMap<&'static str, Serve>,
        seeds: fn() -> Vec<RawStream>,
    ) -> (Handler, Arc<ScoutRuntime>) {
        let runtime = ScoutRuntime::new();
        let inspect = runtime.clone();
        let account = account.clone();
        let doer = cdn(files);
        let h = new_handler(test_deps(move |d| {
            d.runtime = runtime;
            d.probe_doer = Some(doer);
            d.make_scrapers = Arc::new(move |_| {
                vec![fake_scraper("torrentio", move || Box::pin(async move { Ok(seeds()) }))]
            });
            d.make_stores = Arc::new(move |_| vec![Box::new(account.clone()) as Box<dyn Store>]);
        }));
        (h, inspect)
    }

    fn config(max_size_gb: i64, result_cap: i64) -> String {
        blob(&format!(
            r#"{{"debrid":[{{"service":"torbox","token":"tb-secret"}}],"indexers":["torrentio"],"filters":{{"maxSizeGB":{max_size_gb}}},"resultCap":{result_cap}}}"#
        ))
    }

    fn path(config: &str, kind: &str, id: &str) -> String {
        format!("/{config}/stream/{kind}/{id}.json")
    }

    /// The listed releases by title, each with its attributes.
    fn listed(rr: &http::Response<Vec<u8>>) -> HashMap<String, serde_json::Value> {
        assert_eq!(rr.status(), 200, "list: {}", rr.status());
        let body: serde_json::Value = serde_json::from_slice(rr.body()).expect("a JSON list");
        body["streams"]
            .as_array()
            .expect("a streams array")
            .iter()
            .map(|s| (s["title"].as_str().unwrap_or_default().to_owned(), s["attributes"].clone()))
            .collect()
    }

    fn titles(rr: &http::Response<Vec<u8>>) -> Vec<String> {
        let mut out: Vec<String> = listed(rr).into_keys().collect();
        out.sort();
        out
    }

    fn sorted(titles: &[&str]) -> Vec<String> {
        let mut out: Vec<String> = titles.iter().map(|t| (*t).to_owned()).collect();
        out.sort();
        out
    }

    fn count(runtime: &ScoutRuntime, outcome: &str) -> i64 {
        runtime.metrics().size_recovery_count(outcome)
    }

    async fn settled(runtime: &ScoutRuntime, want: usize) {
        wait_for(
            || {
                ["fits", "oversized", "unknown", "failed"].iter().map(|o| count(runtime, o)).sum::<i64>()
                    == want as i64
            },
            "the size-recovery probes never settled",
        )
        .await;
    }

    /// Requests `path` until its list has `title`, failing after five seconds.
    async fn until_listed(h: &Handler, path: &str, title: &str) -> http::Response<Vec<u8>> {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        loop {
            let rr = get(h, path, &[]).await;
            if listed(&rr).contains_key(title) {
                return rr;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "{title} never reached the list: {:?}",
                titles(&rr)
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    const EPISODE_TITLES: [&str; 3] =
        ["Fauda.S01E03.720p.WEB.x264", "Fauda.S01.1080p.WEB-DL.x264", "Fauda.S01E01-E06.1080p.WEB-DL.x264"];

    // The Fauda S1E3 shape with a 20 GB budget. The cold list leaves the pack-sized exact-episode release out — its
    // only size is an unconfirmed number over the budget — and probes it behind the reply with the frontier, without
    // an add. Its file is 1.1 GB, so the cached list is marked stale; the viewer after that is served it once while
    // it refreshes, and from then on the list carries the release with its own size and the pack's as context. The
    // 30 GB episode file stays out, the unheld release is never resolved, and nothing is probed twice.
    #[tokio::test]
    async fn a_pack_sized_exact_episode_is_probed_and_listed_once_its_file_fits() {
        let account = Account::new(&[PACKED, TOO_BIG, SMALL, SEASON, RANGE]);
        let files = HashMap::from([
            (PACKED, Serve::File(EPISODE)),
            (TOO_BIG, Serve::File(30 * GIB)),
            (SMALL, Serve::File(2 * GIB)),
            (SEASON, Serve::File(EPISODE)),
            (RANGE, Serve::File(EPISODE)),
        ]);
        let (h, runtime) = handler(&account, files, listing);
        let list = path(&config(20, 20), "series", EPISODE_PATH);

        let cold = get(&h, &list, &[]).await;
        assert_eq!(titles(&cold), sorted(&EPISODE_TITLES), "the cold list");
        let season = &listed(&cold)["Fauda.S01.1080p.WEB-DL.x264"];
        assert!(
            season["sizeBytes"].is_null() && season["packSizeBytes"] == PACK_HINT,
            "a season-named pack keeps its total as the pack's, never as the episode's: {season}"
        );
        assert_eq!(count(&runtime, "queued"), 2, "the two held exact-episode releases over the budget");

        settled(&runtime, 2).await;
        assert_eq!((count(&runtime, "fits"), count(&runtime, "oversized")), (1, 1));

        let marked = get(&h, &list, &[]).await;
        assert!(
            hdr(&marked, "server-timing").starts_with("cache;desc=stale"),
            "the list built without the release was not marked stale: {}",
            hdr(&marked, "server-timing")
        );
        assert_eq!(titles(&marked), sorted(&EPISODE_TITLES), "a stale list is served as it was built");

        let rebuilt = until_listed(&h, &list, "Fauda.S01E03.1080p.WEB-DL.x264").await;
        let mut want = EPISODE_TITLES.to_vec();
        want.push("Fauda.S01E03.1080p.WEB-DL.x264");
        assert_eq!(titles(&rebuilt), sorted(&want), "the 30 GB and unheld releases stay out");
        let packed = &listed(&rebuilt)["Fauda.S01E03.1080p.WEB-DL.x264"];
        assert!(
            packed["sizeBytes"] == EPISODE && packed["packSizeBytes"] == PACK_HINT,
            "want the episode's own size and the pack's as context: {packed}"
        );

        let targets = account.resolves.lock().expect("the lock").clone();
        assert!(targets.iter().all(|t| t.no_add), "a probe resolved without NoAdd: {targets:?}");
        // The frontier's own probes may still be landing when the list refreshes, so only the two the budget dropped
        // are counted exactly: each was probed once, and its fact kept it from being queued again.
        for hash in [PACKED, TOO_BIG] {
            assert_eq!(
                account.resolved(hash),
                1,
                "{hash} resolved {} times, want once",
                account.resolved(hash)
            );
        }
        assert_eq!(account.resolved(UNHELD), 0, "an unheld release was resolved: that is an add");
        assert_eq!(count(&runtime, "queued"), 2, "a release with a probe fact was queued again");
    }

    // Two configs (or a TV and a browser list) queue the same file while its probe is still running: the second
    // joins the first's flight and still settles, so each marks its own list stale. A list of another title is left
    // alone, and a config built after the probe judges the probed size by its own budget, queueing nothing.
    #[tokio::test]
    async fn every_list_that_queued_a_file_is_settled_and_no_other() {
        let mut account = Account::new(&[PACKED, TOO_BIG, SMALL, SEASON, RANGE]);
        account.gated = vec![PACKED, TOO_BIG];
        let files = HashMap::from([
            (PACKED, Serve::File(EPISODE)),
            (TOO_BIG, Serve::File(30 * GIB)),
            (SMALL, Serve::File(2 * GIB)),
            (SEASON, Serve::File(EPISODE)),
            (RANGE, Serve::File(EPISODE)),
        ]);
        let (h, runtime) = handler(&account, files, listing);
        let (first, second, strict) = (config(20, 20), config(20, 10), config(1, 20));
        let first_list = path(&first, "series", EPISODE_PATH);
        let second_list = path(&second, "series", EPISODE_PATH);
        let movie = path(&first, "movie", "tt98");

        assert_eq!(titles(&get(&h, &first_list, &[]).await), sorted(&EPISODE_TITLES));
        assert_eq!(titles(&get(&h, &second_list, &[]).await), sorted(&EPISODE_TITLES));
        get(&h, &movie, &[]).await;
        wait_for(
            || account.resolved(PACKED) == 1 && account.resolved(TOO_BIG) == 1,
            "the probes never reached the store",
        )
        .await;
        // Both lists queued both; the second waits on the first's probes rather than resolving again.
        assert_eq!(count(&runtime, "queued"), 4);

        account.gate.add_permits(64);
        settled(&runtime, 4).await;
        assert_eq!((count(&runtime, "fits"), count(&runtime, "oversized")), (2, 2));
        assert_eq!(
            (account.resolved(PACKED), account.resolved(TOO_BIG)),
            (1, 1),
            "a joined probe resolved again"
        );

        for list in [&first_list, &second_list] {
            let marked = get(&h, list, &[]).await;
            assert!(
                hdr(&marked, "server-timing").starts_with("cache;desc=stale"),
                "{list} was not marked stale"
            );
            until_listed(&h, list, "Fauda.S01E03.1080p.WEB-DL.x264").await;
        }
        let other = get(&h, &movie, &[]).await;
        assert!(
            hdr(&other, "server-timing").starts_with("cache;desc=hit"),
            "another title's list was touched: {}",
            hdr(&other, "server-timing")
        );

        let strict = get(&h, &path(&strict, "series", EPISODE_PATH), &[]).await;
        assert!(
            !listed(&strict).contains_key("Fauda.S01E03.1080p.WEB-DL.x264"),
            "a 1.1 GB file is over a 1 GB budget: {:?}",
            titles(&strict)
        );
        assert_eq!(count(&runtime, "queued"), 4, "a probed release was queued again under another budget");
    }

    // A probe that cannot read the file, or reads it with no size, corrects nothing: the release stays out and the
    // cached list is left as it is. A failure leaves no fact, so a later build may ask once more; a probe that
    // answered without a size leaves one, so it is not asked again.
    #[tokio::test]
    async fn a_probe_without_a_size_leaves_the_list_alone() {
        fn seeds() -> Vec<RawStream> {
            vec![
                release(PACKED, "Fauda.S01E03.1080p.WEB-DL.x264", Some(PACK_HINT)),
                release(TOO_BIG, "Fauda.S01E03.2160p.WEB-DL.x265", Some(PACK_HINT)),
                release(SMALL, "Fauda.S01E03.720p.WEB.x264", Some(2 * GIB)),
            ]
        }
        let account = Account::new(&[PACKED, TOO_BIG, SMALL]);
        let files =
            HashMap::from([(PACKED, Serve::Fails), (TOO_BIG, Serve::NoTotal), (SMALL, Serve::File(2 * GIB))]);
        let (h, runtime) = handler(&account, files, seeds);
        let list = path(&config(20, 20), "series", EPISODE_PATH);

        assert_eq!(titles(&get(&h, &list, &[]).await), sorted(&["Fauda.S01E03.720p.WEB.x264"]));
        settled(&runtime, 2).await;
        assert_eq!((count(&runtime, "failed"), count(&runtime, "unknown")), (1, 1));

        let after = get(&h, &list, &[]).await;
        assert!(
            hdr(&after, "server-timing").starts_with("cache;desc=hit"),
            "a list was marked stale for a size nobody read: {}",
            hdr(&after, "server-timing")
        );
        assert_eq!(titles(&after), sorted(&["Fauda.S01E03.720p.WEB.x264"]));

        let rebuilt = get(&h, &format!("{list}?fresh=1"), &[]).await;
        assert_eq!(titles(&rebuilt), sorted(&["Fauda.S01E03.720p.WEB.x264"]));
        assert_eq!(count(&runtime, "queued"), 3, "want only the failed probe asked again");
    }

    /// Fails unless `attrs` is free of every field only a read of the file's tracks can give.
    fn assert_tracks_unknown(attrs: &serde_json::Value) {
        for field in [
            "probed",
            "audioLanguages",
            "subtitleLanguages",
            "untaggedAudioTracks",
            "width",
            "height",
            "frameRate",
        ] {
            assert!(attrs.get(field).is_none(), "{field} claims a read of tracks nobody read: {attrs}");
        }
    }

    // A head whose tracks cannot be read (an MP4 whose moov lies past the first MiB) still came with the CDN's total,
    // and that total is the file's size. Size recovery settles from it: the 1.1 GB file fits and is listed with its
    // own size and the pack's as context, the 30 GB one stays out. Its tracks stay unknown in the list, and the fact
    // keeps both from being resolved or queued again by a later build.
    #[tokio::test]
    async fn an_unreadable_head_with_a_stated_total_settles_size_recovery() {
        fn seeds() -> Vec<RawStream> {
            vec![
                release(PACKED, "Fauda.S01E03.1080p.WEB-DL.x264", Some(PACK_HINT)),
                release(TOO_BIG, "Fauda.S01E03.2160p.WEB-DL.x265", Some(PACK_HINT)),
                release(SMALL, "Fauda.S01E03.720p.WEB.x264", Some(2 * GIB)),
            ]
        }
        let account = Account::new(&[PACKED, TOO_BIG, SMALL]);
        let files = HashMap::from([
            (PACKED, Serve::Unreadable(Some(EPISODE))),
            (TOO_BIG, Serve::Unreadable(Some(30 * GIB))),
            (SMALL, Serve::File(2 * GIB)),
        ]);
        let (h, runtime) = handler(&account, files, seeds);
        let list = path(&config(20, 20), "series", EPISODE_PATH);

        assert_eq!(titles(&get(&h, &list, &[]).await), sorted(&["Fauda.S01E03.720p.WEB.x264"]));
        assert_eq!(count(&runtime, "queued"), 2);
        settled(&runtime, 2).await;
        assert_eq!((count(&runtime, "fits"), count(&runtime, "oversized")), (1, 1));

        let rebuilt = until_listed(&h, &list, "Fauda.S01E03.1080p.WEB-DL.x264").await;
        assert_eq!(
            titles(&rebuilt),
            sorted(&["Fauda.S01E03.1080p.WEB-DL.x264", "Fauda.S01E03.720p.WEB.x264"]),
            "the 30 GB file stays out"
        );
        let packed = &listed(&rebuilt)["Fauda.S01E03.1080p.WEB-DL.x264"];
        assert!(
            packed["sizeBytes"] == EPISODE && packed["packSizeBytes"] == PACK_HINT,
            "want the episode's own size and the pack's as context: {packed}"
        );
        let label = packed["label"].as_str().unwrap_or_default();
        assert!(label.contains("1.1 GB"), "the label does not show the file's own size: {label}");
        assert_tracks_unknown(packed);

        // The rebuilt list put the file in the frontier, and a fresh build lists it again: neither probes it.
        let fresh = get(&h, &format!("{list}?fresh=1"), &[]).await;
        assert!(listed(&fresh).contains_key("Fauda.S01E03.1080p.WEB-DL.x264"));
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(
            (account.resolved(PACKED), account.resolved(TOO_BIG)),
            (1, 1),
            "a file with a size-only fact was probed again"
        );
        assert_eq!(count(&runtime, "queued"), 2, "a release with a size-only fact was queued again");
    }

    // The ordinary frontier: a served release whose head is unreadable is probed once, not on every rebuild. Its size
    // corrects the indexer's number, naming the pack where the gap is a pack's and replacing a rounding difference
    // outright, and its tracks stay unknown.
    #[tokio::test]
    async fn a_listed_release_with_an_unreadable_head_is_probed_once_for_its_size() {
        const LISTED: i64 = 2 * GIB;
        const SERVED: i64 = 2 * GIB - 4096;
        fn seeds() -> Vec<RawStream> {
            vec![
                release(PACKED, "Fauda.S01E03.1080p.WEB-DL.x264", Some(PACK_HINT)),
                release(SMALL, "Fauda.S01E03.720p.WEB.x264", Some(LISTED)),
            ]
        }
        let account = Account::new(&[PACKED, SMALL]);
        let files = HashMap::from([
            (PACKED, Serve::Unreadable(Some(EPISODE))),
            (SMALL, Serve::Unreadable(Some(SERVED))),
        ]);
        let (h, _runtime) = handler(&account, files, seeds);
        let list = path(&config(100, 20), "series", EPISODE_PATH);

        let cold = get(&h, &list, &[]).await;
        assert_eq!(listed(&cold)["Fauda.S01E03.1080p.WEB-DL.x264"]["sizeBytes"], PACK_HINT);
        wait_for(
            || account.resolved(PACKED) == 1 && account.resolved(SMALL) == 1,
            "the frontier was never probed",
        )
        .await;
        // Past the probes' flights, so a fresh build reads their facts rather than joining or restarting a probe.
        tokio::time::sleep(Duration::from_millis(50)).await;

        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        let rebuilt = loop {
            let rr = get(&h, &format!("{list}?fresh=1"), &[]).await;
            if listed(&rr)["Fauda.S01E03.720p.WEB.x264"]["sizeBytes"] == SERVED
                && listed(&rr)["Fauda.S01E03.1080p.WEB-DL.x264"]["sizeBytes"] == EPISODE
            {
                break rr;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "the sizes were never corrected: {:?}",
                listed(&rr)
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        };
        let rebuilt = listed(&rebuilt);
        let (packed, small) =
            (&rebuilt["Fauda.S01E03.1080p.WEB-DL.x264"], &rebuilt["Fauda.S01E03.720p.WEB.x264"]);
        assert_eq!(packed["packSizeBytes"], PACK_HINT, "the gap is a pack's: {packed}");
        assert!(small.get("packSizeBytes").is_none(), "a rounding difference is not a pack: {small}");
        assert_tracks_unknown(packed);
        assert_tracks_unknown(small);

        get(&h, &format!("{list}?fresh=1"), &[]).await;
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(
            (account.resolved(PACKED), account.resolved(SMALL)),
            (1, 1),
            "a file with a size-only fact was probed again"
        );
    }

    // With no total stated, an unreadable head leaves nothing to keep, exactly as before: size recovery settles as
    // failed and the list is left alone, and a later build asks again, for the frontier and the recovery alike.
    #[tokio::test]
    async fn an_unreadable_head_without_a_total_is_asked_again() {
        fn seeds() -> Vec<RawStream> {
            vec![
                release(PACKED, "Fauda.S01E03.1080p.WEB-DL.x264", Some(PACK_HINT)),
                release(SMALL, "Fauda.S01E03.720p.WEB.x264", Some(2 * GIB)),
            ]
        }
        let account = Account::new(&[PACKED, SMALL]);
        let files = HashMap::from([(PACKED, Serve::Unreadable(None)), (SMALL, Serve::Unreadable(None))]);
        let (h, runtime) = handler(&account, files, seeds);
        let list = path(&config(20, 20), "series", EPISODE_PATH);

        assert_eq!(titles(&get(&h, &list, &[]).await), sorted(&["Fauda.S01E03.720p.WEB.x264"]));
        settled(&runtime, 1).await;
        assert_eq!(count(&runtime, "failed"), 1);
        wait_for(|| account.resolved(SMALL) == 1, "the frontier was never probed").await;
        // Past the probe's flight, so the fresh build below starts its own rather than joining this one.
        tokio::time::sleep(Duration::from_millis(50)).await;
        let after = get(&h, &list, &[]).await;
        assert!(
            hdr(&after, "server-timing").starts_with("cache;desc=hit"),
            "a list was marked stale for a size nobody read: {}",
            hdr(&after, "server-timing")
        );

        let rebuilt = get(&h, &format!("{list}?fresh=1"), &[]).await;
        assert_eq!(titles(&rebuilt), sorted(&["Fauda.S01E03.720p.WEB.x264"]));
        assert_tracks_unknown(&listed(&rebuilt)["Fauda.S01E03.720p.WEB.x264"]);
        assert_eq!(count(&runtime, "queued"), 2, "want the failed recovery probe asked again");
        wait_for(
            || account.resolved(PACKED) == 2 && account.resolved(SMALL) == 2,
            "a probe that kept nothing was not asked again",
        )
        .await;
    }

    // A movie has no pack for its size to be confused with: the budget drops it on the listed size, as before, and
    // nothing probes it. A release with no size at all is never dropped by the budget.
    #[tokio::test]
    async fn a_movie_over_the_budget_is_dropped_and_never_probed() {
        fn seeds() -> Vec<RawStream> {
            vec![
                release(PACKED, "Film.2019.2160p.REMUX", Some(68 * GIB)),
                release(SMALL, "Film.2019.1080p.WEB-DL", None),
            ]
        }
        let account = Account::new(&[PACKED, SMALL]);
        let (h, runtime) = handler(&account, HashMap::from([(SMALL, Serve::File(9 * GIB))]), seeds);

        let rr = get(&h, &path(&config(20, 20), "movie", "tt98"), &[]).await;
        assert_eq!(titles(&rr), sorted(&["Film.2019.1080p.WEB-DL"]));
        wait_for(|| account.resolved(SMALL) == 1, "the listed release was never probed").await;
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(account.resolved(PACKED), 0, "a movie over the budget was resolved");
        assert_eq!(count(&runtime, "queued"), 0);
    }
}
