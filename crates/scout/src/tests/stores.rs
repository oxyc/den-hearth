//! Go: `internal/scout/stores_test.go` (2276 lines).
//!
//! Covers all three stores. The Go tests that measured allocations with `runtime.MemStats` are not here: they
//! need a counting allocator in the test binary.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicI32, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use crate::cache::{Cache, MemoryCache};
use crate::config::{Config, DebridAccount, DebridService};
use crate::handler::{queued_body, rd_only};
use crate::scrape::{DoError, Doer};
use crate::stores::pool::{build_stores, has_cache_truth, StorePool};
use crate::stores::premiumize::{PremiumizeStore, PREMIUMIZE_API};
use crate::stores::realdebrid::{rd_torrent_key, RealDebridStore, REAL_DEBRID_API};
use crate::stores::refusal::{cached_key, known_cached, torbox_account_identity, CACHED_TTL};
use crate::stores::refusal::{resolve_key, torrent_id_key};
use crate::stores::torbox::{
    decode_listing, ListingFault, TorBoxStore, TruncationDetector, CACHE_BATCH, MAX_LISTING_ENTRIES,
    MAX_TORBOX_OUTBOUND, TORBOX_API,
};
use crate::stores::{dead_link, DeadLinkError, StoreError, StoreStatus, ERR_CHECK_FAILED};
use crate::stores::{
    rd_fetch_state, torbox_fetch_state, ProviderStatus, ResolveTarget, Store, FETCH_DOWNLOADING,
    FETCH_FAILED, FETCH_FETCHING, FETCH_QUEUED, FETCH_STALLED, MAX_LISTING_BYTES, MAX_STORE_BYTES,
};
use crate::tests::mock::{resp, MockDoer};
use crate::util::key_hash;

const H: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";

/// Go `r.URL.Query().Get(key)`: empty when absent.
fn query(req: &reqwest::Request, key: &str) -> String {
    req.url().query_pairs().find(|(k, _)| k == key).map(|(_, v)| v.into_owned()).unwrap_or_default()
}

/// Go `mockDoer{fn: ...}` behind the `Doer` seam, as the store's client.
fn mock<F>(f: F) -> Arc<dyn Doer>
where
    F: Fn(reqwest::Request) -> Result<reqwest::Response, DoError> + Send + Sync + 'static,
{
    Arc::new(MockDoer(f))
}

fn tb(token: &str, api: &str, cache: Option<Arc<dyn Cache>>, client: Arc<dyn Doer>) -> TorBoxStore {
    TorBoxStore { token: token.to_owned(), client, cache, api: api.to_owned() }
}

/// Go `m[key]` on a `map[string]int`: the zero value when absent.
fn id_of(ids: &HashMap<String, i64>, key: &str) -> i64 {
    ids.get(key).copied().unwrap_or(0)
}

/// Go `m[key]` on a `map[string]bool`: false when absent.
fn held(m: &HashMap<String, bool>, key: &str) -> bool {
    m.get(key).copied().unwrap_or(false)
}

fn target_hash(hash: &str) -> ResolveTarget {
    ResolveTarget { info_hash: hash.to_owned(), ..Default::default() }
}

fn target_file(hash: &str, file_idx: i64) -> ResolveTarget {
    ResolveTarget { info_hash: hash.to_owned(), file_idx: Some(file_idx), ..Default::default() }
}

fn target_episode(hash: &str, season: i64, episode: i64) -> ResolveTarget {
    ResolveTarget {
        info_hash: hash.to_owned(),
        season: Some(season),
        episode: Some(episode),
        ..Default::default()
    }
}

#[tokio::test]
async fn torbox_cache_check() {
    let other = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
    let d = mock(|_| Ok(resp(200, &format!(r#"{{"data":{{"{H}":{{"name":"x"}}}}}}"#))));
    let s = tb("t", TORBOX_API, None, d);
    let m = s.cache_check(&[H.to_owned(), other.to_owned()]).await;
    match &m {
        Ok(m) if held(m, H) && !held(m, other) => {}
        _ => panic!("cacheCheck: {m:?}"),
    }

    // every batch failing → errCheckFailed (lets the pool detect an outage)
    let boom = tb("t", TORBOX_API, None, mock(|_| Ok(resp(503, "down"))));
    assert!(boom.cache_check(&[H.to_owned()]).await.is_err(), "all-batch failure should return an error");
}

#[tokio::test]
async fn torbox_cache_check_batches_concurrent_demand_across_requests() {
    let calls = Arc::new(AtomicUsize::new(0));
    let counter = calls.clone();
    let client = mock(move |_| {
        counter.fetch_add(1, Ordering::SeqCst);
        Ok(resp(200, r#"{"data":{}}"#))
    });
    let cache: Arc<dyn Cache> = Arc::new(MemoryCache::new(1 << 20));
    const TOKEN: &str = "demand-batch-test";
    // All 20 checks start together, once every task is parked on the barrier.
    let start = Arc::new(tokio::sync::Barrier::new(21));
    let mut tasks = Vec::new();
    for i in 0..20 {
        let store = tb(TOKEN, TORBOX_API, Some(cache.clone()), client.clone());
        let start = start.clone();
        tasks.push(tokio::spawn(async move {
            start.wait().await;
            let hash = format!("{:040x}", i + 1);
            let answer = store.cache_check(&[hash]).await;
            match answer {
                Ok(a) if a.len() == 1 => {}
                other => panic!("answer={other:?}"),
            }
        }));
    }
    start.wait().await;
    for t in tasks {
        t.await.expect("task");
    }
    let got = calls.load(Ordering::SeqCst);
    assert_eq!(got, 1, "20 overlapping checks made {got} API calls, want 1");
}

#[tokio::test]
async fn torbox_cache_check_singleflight_includes_api_identity() {
    const TOKEN: &str = "same-token-different-api";
    let calls = Arc::new(AtomicUsize::new(0));
    let counter = calls.clone();
    let client = mock(move |_| {
        counter.fetch_add(1, Ordering::SeqCst);
        std::thread::sleep(Duration::from_millis(20));
        Ok(resp(200, r#"{"data":{}}"#))
    });
    let start = Arc::new(tokio::sync::Barrier::new(3));
    let mut tasks = Vec::new();
    for api in ["https://torbox-a.invalid/v1", "https://torbox-b.invalid/v1"] {
        let store = tb(TOKEN, api, Some(Arc::new(MemoryCache::new(1 << 20))), client.clone());
        let start = start.clone();
        tasks.push(tokio::spawn(async move {
            start.wait().await;
            if let Err(e) = store.cache_check(&[H.to_owned()]).await {
                panic!("{api}: {e:?}");
            }
        }));
    }
    start.wait().await;
    for t in tasks {
        t.await.expect("task");
    }
    let got = calls.load(Ordering::SeqCst);
    assert_eq!(got, 2, "two API identities shared one checkcached flight: {got} calls, want 2");
}

// A batch that failed leaves its hashes OUT of the map rather than marking them uncached.
//
// Checks go out in batches of 100 and up to 500 hashes are checked, so one timed-out batch used to
// assert "TorBox does not hold this" about 100 releases — and report no error, because another batch
// succeeded. With cachedOnly on, that silently drops up to 100 genuinely playable releases with no
// degraded signal; without it, playing one pays an add for a torrent the account already had.
#[tokio::test]
async fn torbox_cache_check_a_failed_batch_is_absent_not_uncached() {
    crate::runtime::scope(crate::runtime::ScoutRuntime::new(), async {
        let hashes: Vec<String> = (0..CACHE_BATCH + 2).map(|i| format!("{i:040x}")).collect();
        // The first batch answers (and holds nothing); the second — the last two hashes — fails.
        let d = mock(|req| {
            let n = query(&req, "hash").matches(',').count() + 1;
            if n == CACHE_BATCH {
                Ok(resp(200, r#"{"data":{}}"#))
            } else {
                Ok(resp(503, "down"))
            }
        });
        let s = tb("t", TORBOX_API, None, d);
        let m = match s.cache_check(&hashes).await {
            Ok(m) => m,
            Err(e) => panic!("one good batch is not a total failure: {e:?}"),
        };
        let answered = m.get(&hashes[0]).copied();
        assert_eq!(
            answered,
            Some(false),
            "a hash in the batch that came back must be present and false: value={answered:?}"
        );
        assert!(
            !m.contains_key(&hashes[CACHE_BATCH]),
            "a hash in the FAILED batch must be absent — present-and-false claims TorBox does not hold it"
        );
    })
    .await;
}

// The account's whole torrent list is fetched to find a hash, and only hits were remembered. So a miss —
// which is every poll of a release that was never queued — refetched the entire list, once per poll, for
// the length of a download.
#[tokio::test]
async fn torbox_torrent_id_remembers_a_miss() {
    let lists = Arc::new(AtomicUsize::new(0));
    let counter = lists.clone();
    let d = mock(move |req| {
        if req.url().path().contains("mylist") {
            counter.fetch_add(1, Ordering::SeqCst);
            return Ok(resp(200, r#"{"data":[]}"#));
        }
        Ok(resp(404, "{}"))
    });
    let s = tb("t", TORBOX_API, Some(Arc::new(MemoryCache::new(1 << 20))), d);
    for _ in 0..5 {
        let (_, ok, _) = s.torrent_id(H).await;
        assert!(!ok, "an empty account holds nothing");
    }
    let n = lists.load(Ordering::SeqCst);
    assert_eq!(n, 1, "fetched the whole account list {n} times for five polls of the same miss");
}

#[tokio::test]
async fn torbox_resolve_movie() {
    let reqdl = Arc::new(Mutex::new(String::new()));
    let seen = reqdl.clone();
    let d = mock(move |req| {
        let p = req.url().path().to_owned();
        if p.contains("createtorrent") {
            return Ok(resp(200, r#"{"data":{"torrent_id":7}}"#));
        }
        if p.contains("requestdl") {
            *seen.lock().unwrap() = req.url().query().unwrap_or("").to_owned();
            return Ok(resp(200, r#"{"success":true,"data":"https://cdn.torbox/x.mkv"}"#));
        }
        assert!(!p.contains("mylist"), "movie should not list files");
        Ok(resp(404, "{}"))
    });
    let s = tb("t", TORBOX_API, None, d);
    let link = s.resolve(&target_file(H, 0)).await;
    assert_eq!(link.as_deref(), Ok("https://cdn.torbox/x.mkv"), "resolve");
    let rq = reqdl.lock().unwrap().clone();
    assert!(rq.contains("file_id=0"), "requestdl missing file_id: {rq}");
}

#[tokio::test]
async fn torbox_binge_cache_and_no_poison() {
    // binge: 2 episodes on the same pack → createtorrent + mylist once.
    let creates = Arc::new(AtomicUsize::new(0));
    let lists = Arc::new(AtomicUsize::new(0));
    let dls = Arc::new(AtomicUsize::new(0));
    let (c, l, dl) = (creates.clone(), lists.clone(), dls.clone());
    let d = mock(move |req| {
        let p = req.url().path().to_owned();
        if p.contains("createtorrent") {
            c.fetch_add(1, Ordering::SeqCst);
            return Ok(resp(200, r#"{"data":{"torrent_id":9}}"#));
        }
        if p.contains("mylist") {
            l.fetch_add(1, Ordering::SeqCst);
            return Ok(resp(
                200,
                r#"{"data":{"files":[{"id":0,"name":"S01E01.mkv","size":10},{"id":1,"name":"S01E02.mkv","size":20}]}}"#,
            ));
        }
        if p.contains("requestdl") {
            dl.fetch_add(1, Ordering::SeqCst);
            return Ok(resp(
                200,
                &format!(r#"{{"success":true,"data":"https://cdn/{}"}}"#, query(&req, "file_id")),
            ));
        }
        Ok(resp(404, "{}"))
    });
    let s = tb("t", TORBOX_API, Some(Arc::new(MemoryCache::new(1 << 20))), d);
    let ep1 = s.resolve(&target_episode(H, 1, 1)).await.unwrap_or_default();
    let ep2 = s.resolve(&target_episode(H, 1, 2)).await.unwrap_or_default();
    assert!(ep1 == "https://cdn/0" && ep2 == "https://cdn/1", "binge picks: ep1={ep1:?} ep2={ep2:?}");
    let (c, l, dl) =
        (creates.load(Ordering::SeqCst), lists.load(Ordering::SeqCst), dls.load(Ordering::SeqCst));
    assert!(c == 1 && l == 1 && dl == 2, "binge cache: creates={c} lists={l} dls={dl} (want 1,1,2)");

    // A pack whose list never succeeded re-lists on the next episode instead of mis-serving from an
    // empty one.
    //
    // This half names two guards and reaches NEITHER on its own. The write guard at the bottom of
    // resolveHeldTorrent is out of reach because the list fails on the first resolve, so no good entry
    // exists to overwrite; the read guard on the warm-entry fast path is out of reach because, with the
    // write guard intact, no entry is ever written here at all and the fast path misses the cache. Only
    // the PAIR is pinned: delete either one alone and this stays green.
    //
    // Both are pinned individually elsewhere, and a relabelling of this comment that claimed it covered
    // the read guard "and NOTHING ELSE" was itself wrong — the misattribution moved rather than going
    // away. See torbox_a_failed_list_does_not_poison_a_warm_pack_entry for the write side and
    // torbox_a_pack_entry_with_no_file_list_is_not_served_to_an_episode for the read side.
    let lists2 = Arc::new(AtomicUsize::new(0));
    let l2 = lists2.clone();
    let d2 = mock(move |req| {
        let p = req.url().path().to_owned();
        if p.contains("createtorrent") {
            return Ok(resp(200, r#"{"data":{"torrent_id":9}}"#));
        }
        if p.contains("mylist") {
            l2.fetch_add(1, Ordering::SeqCst);
            return Ok(resp(500, "boom")); // empty file list
        }
        if p.contains("requestdl") {
            return Ok(resp(200, r#"{"success":true,"data":"https://cdn/x"}"#));
        }
        Ok(resp(404, "{}"))
    });
    let s2 = tb("t", TORBOX_API, Some(Arc::new(MemoryCache::new(1 << 20))), d2);
    let _ = s2.resolve(&target_episode(H, 1, 1)).await;
    let _ = s2.resolve(&target_episode(H, 1, 2)).await;
    let l2n = lists2.load(Ordering::SeqCst);
    assert_eq!(l2n, 2, "no-poison: mylist should be retried (got {l2n}, want 2)");
}

// A pack entry with no file list must not be served to an episode request.
//
// This is the READ guard on the warm-entry fast path (`!needFiles || len(e.Files) > 0`), and nothing
// reached it: deleting it left the whole package green, because every fixture that had an empty entry to
// read was also relying on the write guard to keep the entry from existing in the first place.
//
// Getting an entry with no file list past the intact write guard means writing one the way the shipped
// code deliberately does: a movie-shaped resolve needs no list, so `needFiles` is false and the entry is
// stored with `files:null` on purpose — the torrent id in it is what Status needs to report a download
// in progress. Ask for an episode of that same infohash afterwards and the fast path has an entry it
// must refuse.
//
// Without the guard the failure is silent rather than loud: selectFileID picks nothing out of an empty
// list, requestdl is sent with no file_id at all, and TorBox answers 200 with a link to the wrong file.
// No error, no retry, no log — the client just plays the wrong thing.
#[tokio::test]
async fn torbox_a_pack_entry_with_no_file_list_is_not_served_to_an_episode() {
    let creates = Arc::new(AtomicUsize::new(0));
    let lists = Arc::new(AtomicUsize::new(0));
    let (c, l) = (creates.clone(), lists.clone());
    let d = mock(move |req| {
        let p = req.url().path().to_owned();
        if p.contains("createtorrent") {
            c.fetch_add(1, Ordering::SeqCst);
            return Ok(resp(200, r#"{"data":{"torrent_id":9}}"#));
        }
        if p.contains("mylist") {
            l.fetch_add(1, Ordering::SeqCst);
            return Ok(resp(
                200,
                r#"{"data":{"files":[{"id":0,"name":"S01E01.mkv","size":10},{"id":1,"name":"S01E02.mkv","size":20}]}}"#,
            ));
        }
        if p.contains("requestdl") {
            return Ok(resp(
                200,
                &format!(r#"{{"success":true,"data":"https://cdn/{}"}}"#, query(&req, "file_id")),
            ));
        }
        Ok(resp(404, "{}"))
    });
    let s = tb("t", TORBOX_API, Some(Arc::new(MemoryCache::new(1 << 20))), d);

    // Seed the entry the way a movie does: no episode selector, so no list is fetched and files is null.
    if let Err(e) = s.resolve(&target_file(H, 0)).await {
        panic!("seeding the pack entry: {e:?}");
    }
    let n = lists.load(Ordering::SeqCst);
    assert_eq!(n, 0, "the movie path listed files ({n} times), so the entry under test is not empty and this test is not exercising the read guard");

    // Now an episode of the same pack. The entry cannot answer it.
    let link = s.resolve(&target_episode(H, 1, 2)).await;
    assert_eq!(
        link.as_deref(),
        Ok("https://cdn/1"),
        "episode resolve: an entry with no file list was served to an episode request, so requestdl went out with no file_id and the client got a silently wrong file"
    );
    let n = lists.load(Ordering::SeqCst);
    assert_eq!(n, 1, "mylist called {n} times, want 1 — the empty entry was served instead of re-listing");
    let c = creates.load(Ordering::SeqCst);
    assert_eq!(c, 1, "createtorrent called {c} times, want 1");
}

// A failed mylist must not overwrite a GOOD pack entry with files:[], poisoning the pack for six hours.
//
// The write guard this pins (`s.cache != nil && (!needFiles || len(files) > 0)`, at the bottom of
// resolveHeldTorrent) was previously named only by the no-poison half above, which cannot see it: that
// fixture fails its first list, so no good entry exists to be overwritten and its assertion is satisfied
// by the READ guard one branch up. Removing the write guard left the whole package green.
//
// Reaching the write guard while a warm entry exists is the whole difficulty, because the warm entry is
// what the fast path serves from. The way through is a link request that fails WITHOUT being a store
// outage: success:false is a DeadLinkError, and only a StoreUnavailableError returns early there, so it
// falls through to the held path — which lists, blips, and would write the empty list.
#[tokio::test]
async fn torbox_a_failed_list_does_not_poison_a_warm_pack_entry() {
    let creates = Arc::new(AtomicUsize::new(0));
    let lists = Arc::new(AtomicUsize::new(0));
    let list_ok = Arc::new(AtomicBool::new(true));
    let dl_ok = Arc::new(AtomicBool::new(true));
    let (c, l, lo, dlo) = (creates.clone(), lists.clone(), list_ok.clone(), dl_ok.clone());
    let d = mock(move |req| {
        let p = req.url().path().to_owned();
        if p.contains("createtorrent") {
            c.fetch_add(1, Ordering::SeqCst);
            return Ok(resp(200, r#"{"data":{"torrent_id":9}}"#));
        }
        if p.contains("mylist") {
            l.fetch_add(1, Ordering::SeqCst);
            if !lo.load(Ordering::SeqCst) {
                return Ok(resp(500, "boom"));
            }
            return Ok(resp(
                200,
                r#"{"data":{"files":[{"id":0,"name":"S01E01.mkv","size":10},{"id":1,"name":"S01E02.mkv","size":20},{"id":2,"name":"S01E03.mkv","size":30}]}}"#,
            ));
        }
        if p.contains("requestdl") {
            if !dlo.load(Ordering::SeqCst) {
                return Ok(resp(200, r#"{"success":false}"#)); // a dead link, NOT a store outage
            }
            return Ok(resp(
                200,
                &format!(r#"{{"success":true,"data":"https://cdn/{}"}}"#, query(&req, "file_id")),
            ));
        }
        Ok(resp(404, "{}"))
    });
    let s = tb("t", TORBOX_API, Some(Arc::new(MemoryCache::new(1 << 20))), d);

    // Warm the pack entry with a real file list.
    let link = s.resolve(&target_episode(H, 1, 1)).await;
    assert_eq!(link.as_deref(), Ok("https://cdn/0"), "warming the pack");

    // A poll whose link request fails falls through to the held path, where the list blips. The warm
    // entry must survive that.
    list_ok.store(false, Ordering::SeqCst);
    dl_ok.store(false, Ordering::SeqCst);
    assert!(
        s.resolve(&target_episode(H, 1, 2)).await.is_err(),
        "a blipped list with no usable file list should refuse rather than guess an episode"
    );

    // The proof: a later episode still resolves from the warm entry, without re-listing.
    list_ok.store(true, Ordering::SeqCst);
    dl_ok.store(true, Ordering::SeqCst);
    let link = s.resolve(&target_episode(H, 1, 3)).await;
    assert_eq!(
        link.as_deref(),
        Ok("https://cdn/2"),
        "after the blip: want the pack entry to still pick S01E03"
    );
    let n = lists.load(Ordering::SeqCst);
    assert_eq!(n, 2, "mylist called {n} times, want 2 (the warm list and the blip) — the blip overwrote the good entry with an empty file list, so the pack had to be listed again");
    let c = creates.load(Ordering::SeqCst);
    assert_eq!(c, 1, "createtorrent called {c} times, want 1");
}

#[tokio::test]
async fn torbox_dead_link() {
    let d = mock(|req| {
        if req.url().path().contains("createtorrent") {
            return Ok(resp(200, r#"{"data":{"torrent_id":7}}"#));
        }
        Ok(resp(200, r#"{"success":false}"#))
    });
    let s = tb("t", TORBOX_API, None, d);
    assert!(s.resolve(&target_file(H, 0)).await.is_err(), "expected DeadLinkError");
}

#[test]
fn torbox_fetch_state_maps_states() {
    let cases = [
        ("downloading", FETCH_DOWNLOADING),
        ("Downloading", FETCH_DOWNLOADING),
        ("stalled (no seeds)", FETCH_STALLED),
        ("Stalled (No Seeds)", FETCH_STALLED),
        ("paused", FETCH_STALLED),
        ("metaDL", FETCH_FETCHING),
        ("checkingResumeData", FETCH_FETCHING),
        ("queued", FETCH_QUEUED),
        ("failed", FETCH_FAILED),
        ("Failed (Processing)", FETCH_FAILED),
        ("error", FETCH_FAILED),
        ("expired", FETCH_FAILED),
        ("(Reported) Missing", FETCH_FAILED),
        ("incomplete", FETCH_FAILED),
        ("uploading", FETCH_DOWNLOADING),
        ("completed", FETCH_DOWNLOADING),
        ("", FETCH_DOWNLOADING),
        ("something new", FETCH_DOWNLOADING),
    ];
    for (raw, want) in cases {
        let got = torbox_fetch_state(raw);
        assert_eq!(got, want, "torboxFetchState({raw:?})");
    }
}

#[test]
fn rd_fetch_state_maps_states() {
    let cases = [
        ("queued", FETCH_QUEUED),
        ("magnet_conversion", FETCH_FETCHING),
        ("downloading", FETCH_DOWNLOADING),
        ("compressing", FETCH_DOWNLOADING),
        ("uploading", FETCH_DOWNLOADING),
    ];
    for (raw, want) in cases {
        let got = rd_fetch_state(raw);
        assert_eq!(got, want, "rdFetchState({raw:?})");
    }
}

// TorBox's documented download states collapse onto the stable set the 202 body carries. A failed download
// must never read as a stall, and anything unrecognised stays "downloading", the value old clients know.

// Gated items per test are listed in `pending` below.

// The account listing is decoded element by element, so its size bounds time and not memory — which is
// what makes the larger cap safe. A body past the cap must read as "no answer", never as "the account
// holds nothing", because the second costs a duplicate add.
#[test]
fn fetch_account_listing_streams_and_detects_truncation() {
    let mut big = String::from(r#"{"success":true,"data":["#);
    for i in 0..3000 {
        if i > 0 {
            big.push(',');
        }
        // Each entry carries a large junk field, exactly what makes a real listing big.
        big.push_str(&format!(r#"{{"id":{i},"hash":"{i:040x}","files":"{}"}}"#, "f".repeat(400)));
    }
    big.push_str("]}");

    let (ids, ok, _) = decode_listing(big.as_bytes());
    assert!(ok, "a well-formed large listing was refused");
    assert_eq!(ids.len(), 3000, "decoded entries");
    assert_eq!(id_of(&ids, &format!("{:040x}", 7)), 7, "hash→id mapping wrong");

    // Truncation is detectable, and must not look like an empty account.
    // Constructed exactly as production does: the reader is given limit+1 so that "read the cap exactly"
    // and "cut off at the cap" are distinguishable.
    let mut td = TruncationDetector { r: std::io::Read::take(big.as_bytes(), (1 << 10) + 1) };
    let (_, ok, _) = decode_listing(&mut td);
    assert!(!ok, "a truncated listing decoded as a valid answer");
    assert!(td.truncated(), "truncation went undetected — it would read as 'this account holds nothing'");

    // An envelope with no data key is not a claim about the account either.
    let (_, ok, _) = decode_listing(r#"{"success":true}"#.as_bytes());
    assert!(!ok, "a listing with no data key was treated as authoritative");
    // Nor is an explicit null. It is the one member of the bad-envelope family that was documented and
    // never pinned: initialising the map beside the null's `continue` — a one-line change someone might
    // make to "simplify" — turns it into an authoritative empty account, which writes a fifteen-second
    // miss marker and suppresses the only lookup that rediscovers a queued torrent. The duplicate-key
    // fixtures below contain `data:null` too, but decide on the repeat, so they pin nothing here.
    let (_, ok, fault) = decode_listing(r#"{"success":true,"data":null}"#.as_bytes());
    assert!(
        !ok && fault == ListingFault::BadEnvelope,
        "a null data reported ok={ok} fault={fault:?} — null is not the account saying it holds nothing"
    );
    // A REPEATED data key is unreadable, whichever order it comes in. Every rule for picking one of them
    // is unsafe in one direction — a trailing null erases a list that was read, and an empty first array
    // beats a populated second one into an authoritative "holds nothing" that costs a duplicate add —
    // and the two safe directions are opposites, so no pick is safe. All four shapes must be ok=false.
    let hash_entry = format!(r#"{{"id":7,"hash":"{H}"}}"#);
    for body in [
        format!(r#"{{"success":true,"data":null,"data":[{hash_entry}]}}"#),
        format!(r#"{{"success":true,"data":[{hash_entry}],"data":null}}"#),
        format!(r#"{{"success":true,"data":[],"data":[{hash_entry}]}}"#),
        format!(r#"{{"success":true,"data":[{hash_entry}],"data":[]}}"#),
    ] {
        let (ids, ok, _) = decode_listing(body.as_bytes());
        assert!(!ok, "a duplicate data key was treated as an answer ({ids:?}): {body}");
    }
    let (_, ok, _) = decode_listing(r#"{"success":false,"data":[]}"#.as_bytes());
    assert!(!ok, "success:false was treated as authoritative");
    // An explicitly empty account IS an answer.
    let (ids, ok, _) = decode_listing(r#"{"success":true,"data":[]}"#.as_bytes());
    assert!(ok && ids.is_empty(), "an empty account should be a real answer: ok={ok} n={}", ids.len());
}

// A listing larger than the ordinary store cap still parses, because the account listing has its own.
//
// 4 MiB truncates a real large account (~13 MB at 2,000 torrents), and a truncated listing read back as
// "this account holds nothing" — so /play queued a second copy of a torrent already downloading. The
// fixture sits between the two caps deliberately: it fails under maxStoreBytes and passes under
// maxListingBytes, which is the only way this asserts the cap and not just the decoder.
#[tokio::test]
async fn fetch_account_listing_large_account_is_not_truncated() {
    let mut body = String::from(r#"{"success":true,"data":["#);
    let mut i = 0;
    while body.len() < 6 << 20 {
        if i > 0 {
            body.push(',');
        }
        body.push_str(&format!(r#"{{"id":{i},"hash":"{i:040x}","files":"{}"}}"#, "f".repeat(2000)));
        i += 1;
    }
    body.push_str("]}");
    assert!(
        body.len() > MAX_STORE_BYTES,
        "fixture is {} bytes, not past the {} ordinary cap",
        body.len(),
        MAX_STORE_BYTES
    );
    assert!(
        body.len() < MAX_LISTING_BYTES,
        "fixture is {} bytes, past even the {} listing cap",
        body.len(),
        MAX_LISTING_BYTES
    );

    let s = tb("t", TORBOX_API, None, mock(move |_| Ok(resp(200, &body))));
    let (ids, ok, _) = s.fetch_account_listing().await;
    assert!(ok, "a large but legitimate account listing was refused — it reads back as 'holds nothing'");
    assert!(ids.len() >= 100, "decoded only {} entries from a large listing", ids.len());
}

// Joining an in-flight listing fetch respects the JOINER's budget, and the leader survives its caller.
//
// singleflight is context-blind: Do blocks a follower on the leader's WaitGroup with the follower's own
// deadline unobserved, and runs the fetch on whichever caller arrived first. Both halves were live and
// neither had a test. A follower with a 100 ms budget was measured returning after 851 ms — on a /play
// poll read capped at statusBudget so a wait answers promptly — and a follower with ten seconds left was
// measured inheriting a leader's 120 ms failure.
//
// Port note: the Rust store takes no context, so a caller's budget is `tokio::time::timeout` around the
// call, and a cancelled caller is a dropped future. The Go check that the detached fetch inherits the
// leader's DEADLINE has no counterpart here (no context carries a deadline), so it is not asserted.
#[tokio::test]
async fn account_listing_singleflight_respects_each_callers_budget() {
    // A token unique to this invocation, because listingFlight is process-wide and keyed on the token
    // alone. It matters on the FAILING paths: a failed assertion returns without waiting for the detached
    // fetch, so that fetch is still holding the flight when the next run starts — the rerun joins it,
    // takes the previous run's result, and blocks forever. A legible failure becomes a hang, exactly when
    // the message mattered.
    static SEQ: AtomicU64 = AtomicU64::new(0);
    let token = format!("shared-{}", SEQ.fetch_add(1, Ordering::SeqCst) + 1);
    let gate = Arc::new(tokio::sync::Notify::new());
    let (arrival_tx, mut arrivals) = tokio::sync::mpsc::unbounded_channel::<()>();
    let new_store = |token: String| {
        let gate = gate.clone();
        let arrival_tx = arrival_tx.clone();
        tb(
            &token,
            TORBOX_API,
            Some(Arc::new(MemoryCache::new(1 << 20))),
            Arc::new(GatedDoer { gate, arrival_tx }),
        )
    };

    // The leader: a long budget, and it goes away before the fetch completes.
    let leader = Arc::new(new_store(token.clone()));
    let leader_task = {
        let leader = leader.clone();
        tokio::spawn(async move {
            let _ = tokio::time::timeout(Duration::from_millis(31337), leader.account_listing()).await;
        })
    };
    arrivals.recv().await.expect("the fetch is in flight");

    // A follower on the same account with a SHORT budget must not wait for the leader.
    let follower = new_store(token.clone());
    let answered = tokio::time::timeout(Duration::from_millis(50), follower.account_listing()).await;
    assert!(!matches!(answered, Ok((_, true))), "a follower that ran out of time reported an answer");

    // A joiner with a long budget attaches to the SAME in-flight fetch — asserted, not assumed, by the
    // fetch count. An earlier version of this simply called accountListing after cancelling the leader,
    // which lets the caller start a NEW flight of its own and so passes whether or not the leader's fetch
    // survived. It has to be attached before the leader goes away, or it proves nothing.
    let joiner = new_store(token.clone());
    let joined = tokio::spawn(async move { joiner.account_listing().await });
    let second_fetch = tokio::time::timeout(Duration::from_millis(250), arrivals.recv()).await;
    assert!(second_fetch.is_err(), "the joiner started a second fetch instead of attaching to the one in flight, so cancelling the leader would prove nothing about whether its fetch survives");

    // Now the leader's caller hangs up. The fetch must survive it, because the joiner is waiting on it.
    // Release only once the leader's call has fully RETURNED, so anything it cancels on the way out has
    // already happened — otherwise the fetch can finish first and the assertion never gets to fire.
    leader_task.abort();
    let _ = leader_task.await;
    gate.notify_waiters();
    let (_, ok) = tokio::time::timeout(Duration::from_secs(5), joined)
        .await
        .expect("the joining caller never returned")
        .expect("joiner task");
    assert!(ok, "a caller with ten seconds left inherited the departed leader's cancellation");
}

/// Holds every fetch on `gate` and reports each arrival, so a test can see how many fetches started.
struct GatedDoer {
    gate: Arc<tokio::sync::Notify>,
    arrival_tx: tokio::sync::mpsc::UnboundedSender<()>,
}

/// Holds account listings on a counted semaphore so admission, cancellation, joining, and recovery can be
/// asserted without waiting for the production fifteen-second shared deadline.
struct ListingAdmissionDoer {
    release: Arc<tokio::sync::Semaphore>,
    arrivals: tokio::sync::mpsc::UnboundedSender<()>,
}

#[async_trait::async_trait]
impl Doer for ListingAdmissionDoer {
    async fn do_request(&self, _req: reqwest::Request) -> Result<reqwest::Response, DoError> {
        let _ = self.arrivals.send(());
        self.release.clone().acquire_owned().await.expect("listing release semaphore").forget();
        Ok(resp(200, &format!(r#"{{"success":true,"data":[{{"id":4,"hash":"{H}"}}]}}"#)))
    }
}

// A distinct account key used to be inserted and detached before it waited for the 64-call outbound semaphore.
// Thousands of cancelled unique-token requests therefore left thousands of map entries and tasks until the
// shared 15-second timeout. Admission must happen before insertion, while a same-key join remains available at
// saturation and a refused account remains retryable once admitted work completes.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn account_listing_unique_cancellation_flood_is_bounded_and_recovers() {
    const LIMIT: usize = 2;
    const FLOOD: usize = 512;
    let runtime = crate::runtime::ScoutRuntime::new_with_account_listing_limit(LIMIT);
    let release = Arc::new(tokio::sync::Semaphore::new(0));
    let (arrival_tx, mut arrivals) = tokio::sync::mpsc::unbounded_channel();
    let client: Arc<dyn Doer> = Arc::new(ListingAdmissionDoer { release: release.clone(), arrivals: arrival_tx });
    let stores: Vec<Arc<TorBoxStore>> = (0..FLOOD + LIMIT)
        .map(|i| Arc::new(tb(&format!("listing-admission-{i}"), TORBOX_API, None, client.clone())))
        .collect();
    let spawn_listing = |store: Arc<TorBoxStore>| {
        let owner = runtime.clone();
        tokio::spawn(crate::runtime::scope(owner, async move { store.account_listing().await }))
    };

    let leaders = [spawn_listing(stores[0].clone()), spawn_listing(stores[1].clone())];
    for _ in 0..LIMIT {
        tokio::time::timeout(Duration::from_secs(1), arrivals.recv())
            .await
            .expect("an admitted listing did not start")
            .expect("arrival channel closed");
    }
    assert_eq!(runtime.torbox.active_listing_flights(), LIMIT);

    // Saturation must not reject a join of an already-admitted key or start another upstream fetch.
    let joiner = spawn_listing(stores[0].clone());
    assert!(
        tokio::time::timeout(Duration::from_millis(50), arrivals.recv()).await.is_err(),
        "same-key join started a second account listing"
    );

    // Every other account is caller-controlled and unique. Let all request futures enter account_listing, then
    // cancel them as a client disconnect would. None may insert a key or park a detached task.
    let started = Arc::new(AtomicUsize::new(0));
    let mut flood = Vec::new();
    for store in stores.iter().skip(LIMIT).cloned() {
        let (owner, started) = (runtime.clone(), started.clone());
        flood.push(tokio::spawn(crate::runtime::scope(owner, async move {
            started.fetch_add(1, Ordering::SeqCst);
            store.account_listing().await
        })));
    }
    tokio::time::timeout(Duration::from_secs(1), async {
        while started.load(Ordering::SeqCst) != FLOOD {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("the cancellation flood did not start");
    for request in flood {
        request.abort();
    }
    for leader in &leaders {
        leader.abort();
    }
    tokio::task::yield_now().await;
    assert_eq!(
        runtime.torbox.active_listing_flights(),
        LIMIT,
        "unique cancelled accounts escaped pre-insertion admission"
    );

    // Creator cancellation cannot cancel admitted work: the joiner still receives the first account's answer.
    release.add_permits(LIMIT);
    let (ids, ok) = tokio::time::timeout(Duration::from_secs(1), joiner)
        .await
        .expect("same-key join did not complete")
        .expect("join task");
    assert!(ok && ids.get(H) == Some(&4), "joiner lost detached listing work");
    tokio::time::timeout(Duration::from_secs(1), async {
        while runtime.torbox.active_listing_flights() != 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("completed listings leaked flight entries or admission permits");

    // A key refused during the flood has no negative memo/account state and succeeds once a permit returns.
    release.add_permits(1);
    let (ids, ok) = spawn_listing(stores[LIMIT].clone()).await.expect("recovery task");
    assert!(ok && ids.get(H) == Some(&4), "a refused account did not recover on retry");
    assert_eq!(runtime.torbox.active_listing_flights(), 0);
}

#[async_trait::async_trait]
impl Doer for GatedDoer {
    async fn do_request(&self, _req: reqwest::Request) -> Result<reqwest::Response, DoError> {
        let _ = self.arrival_tx.send(());
        self.gate.notified().await;
        Ok(resp(200, &format!(r#"{{"success":true,"data":[{{"id":4,"hash":"{H}"}}]}}"#)))
    }
}

// ProviderStatus's doubt comes from the lookup it already did, not from a second one.
//
// The three-valued answer was first built by re-running findTorrentByHash purely to learn whether the
// listing had been readable. That call goes straight to accountListing and so steps over torrentMissKey,
// whose entire purpose is that a remembered miss costs no fetch — measured at four listing requests per
// /play against a throttled account where one was enough, on the exact path that is already refusing.
#[tokio::test]
async fn torbox_provider_status_takes_its_doubt_from_the_lookup_it_already_did() {
    crate::runtime::scope(crate::runtime::ScoutRuntime::new(), async {
        struct Case {
            name: &'static str,
            // The listing reply, which decides whether the miss is authoritative.
            listing: fn() -> reqwest::Response,
            want_answer: ProviderStatus,
            // Fetches over two polls: the first reads the listing, the second must read nothing — either
            // because the miss was remembered or because the oversize/failure path decided for itself.
            want_fetches: usize,
        }
        let cases = [
            // Read in full, hash absent: an authoritative no, remembered, so the second poll costs nothing.
            Case {
                name: "an empty account is a definitive no",
                listing: || resp(200, r#"{"success":true,"data":[]}"#),
                want_answer: ProviderStatus::NotFetching,
                want_fetches: 1,
            },
            // The list was never read, so "not in it" is not a fact about the account. The miss must NOT be
            // remembered here — that retry is the only thing that rediscovers a queued torrent — so the
            // second poll does fetch again, and neither poll may fetch twice.
            Case {
                name: "an unreadable listing is doubt, not a no",
                listing: || resp(503, "down"),
                want_answer: ProviderStatus::Unknown,
                want_fetches: 2,
            },
        ];
        for tc in cases {
            // A definitive "no" from mylist now also checks TorBox's OWN QUEUE before settling on
            // statusNo (see statusAnswerUncached) — a separate listing, answered empty here so this test
            // stays about the torrent listing's own doubt/certainty contract. An unreadable mylist (the
            // second case) must never reach that check at all: authoritative=false returns before it.
            let fetches = Arc::new(AtomicUsize::new(0));
            let queue_fetches = Arc::new(AtomicUsize::new(0));
            let (f, q) = (fetches.clone(), queue_fetches.clone());
            let listing = tc.listing;
            let s = tb(
                "t",
                TORBOX_API,
                Some(Arc::new(MemoryCache::new(1 << 20))),
                mock(move |req| {
                    let p = req.url().path().to_owned();
                    if p.contains("mylist") && query(&req, "id").is_empty() {
                        f.fetch_add(1, Ordering::SeqCst);
                        return Ok(listing());
                    }
                    if p.contains("getqueued") {
                        q.fetch_add(1, Ordering::SeqCst);
                        return Ok(resp(200, r#"{"success":true,"data":[]}"#));
                    }
                    panic!("unexpected request: {}", req.url());
                }),
            );
            for i in 0..2 {
                let answer = s.provider_status(&target_hash(H)).await;
                assert_eq!(answer, tc.want_answer, "{}: poll {i} answered {answer:?}", tc.name);
            }
            let n = fetches.load(Ordering::SeqCst);
            assert_eq!(n, tc.want_fetches, "{}: listed the account {n} times over two polls", tc.name);
            let q = queue_fetches.load(Ordering::SeqCst);
            assert!(
                q <= 1,
                "{}: checked TorBox's own queue {q} times over two polls, want at most 1",
                tc.name
            );
        }
    })
    .await;
}

// The outbound limiter bounds how many TorBox calls are in flight, and a call that waits for a slot
// must still get a real answer rather than an invented miss.
#[tokio::test]
async fn torbox_outbound_concurrency_is_bounded_without_inventing_misses() {
    let active = Arc::new(AtomicI32::new(0));
    let peak = Arc::new(AtomicI32::new(0));
    let (gate_tx, gate_rx) = tokio::sync::watch::channel(false);
    let client: Arc<dyn Doer> =
        Arc::new(CountingGatedDoer { active: active.clone(), peak: peak.clone(), gate: gate_rx });
    let store = Arc::new(tb("bounded-outbound", TORBOX_API, None, client));
    let extra = 20;
    let mut tasks = Vec::new();
    for i in 0..MAX_TORBOX_OUTBOUND + extra {
        let store = store.clone();
        tasks.push(tokio::spawn(async move {
            let hashes = [format!("{i:040x}")];
            tokio::time::timeout(Duration::from_secs(5), store.fetch_cached(&hashes))
                .await
                .map(|r| r.ok)
                .unwrap_or(false)
        }));
    }
    // Reach enough overlap to prove this burst is being held back, then verify the hard ceiling itself and
    // that every waiter eventually gets a real answer.
    let deadline = std::time::Instant::now() + Duration::from_secs(2);
    while active.load(Ordering::SeqCst) < (MAX_TORBOX_OUTBOUND / 2) as i32
        && std::time::Instant::now() < deadline
    {
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
    tokio::time::sleep(Duration::from_millis(20)).await;
    let overlapped = active.load(Ordering::SeqCst);
    if overlapped < (MAX_TORBOX_OUTBOUND / 2) as i32 {
        let _ = gate_tx.send(true);
        panic!("only {overlapped} calls overlapped; the saturation test did not reach the limiter");
    }
    if overlapped >= (MAX_TORBOX_OUTBOUND + extra) as i32 {
        let _ = gate_tx.send(true);
        panic!("all {overlapped} calls entered at once; outbound work was not backpressured");
    }
    let _ = gate_tx.send(true);
    for t in tasks {
        assert!(t.await.expect("task"), "backpressure turned a successful answer into an unknown result");
    }
    let p = peak.load(Ordering::SeqCst);
    assert!(p <= MAX_TORBOX_OUTBOUND as i32, "peak outbound concurrency = {p}, limit {MAX_TORBOX_OUTBOUND}");
}

/// Counts concurrent calls and holds each one until `gate` opens.
struct CountingGatedDoer {
    active: Arc<AtomicI32>,
    peak: Arc<AtomicI32>,
    gate: tokio::sync::watch::Receiver<bool>,
}

#[async_trait::async_trait]
impl Doer for CountingGatedDoer {
    async fn do_request(&self, _req: reqwest::Request) -> Result<reqwest::Response, DoError> {
        let now = self.active.fetch_add(1, Ordering::SeqCst) + 1;
        self.peak.fetch_max(now, Ordering::SeqCst);
        let mut gate = self.gate.clone();
        let _ = gate.wait_for(|open| *open).await;
        self.active.fetch_sub(1, Ordering::SeqCst);
        Ok(resp(200, r#"{"data":{}}"#))
    }
}

// An account whose listing is too big to read is not re-pulled on every attempt — but a TRANSIENT
// failure still is, because that retry is the only thing that rediscovers a queued torrent.
//
// Only successes were memoised, so an oversized account re-pulled the whole body every time: a /play
// makes up to three status reads and a client polls it every two seconds, which is tens of megabytes of
// egress per request to reach the same answer each time. Oversize is a property of the account, not a
// blip, so it is the one failure worth remembering.
#[tokio::test]
async fn account_listing_remembers_oversized_but_retries_transient() {
    let mut body = String::from(r#"{"success":true,"data":["#);
    let mut i = 0;
    while body.len() < MAX_LISTING_BYTES + (1 << 20) {
        if i > 0 {
            body.push(',');
        }
        body.push_str(&format!(r#"{{"id":{i},"hash":"{i:040x}","files":"{}"}}"#, "f".repeat(4000)));
        i += 1;
    }
    body.push_str("]}");

    let fetches = Arc::new(AtomicUsize::new(0));
    let f = fetches.clone();
    let oversized = tb(
        "t",
        TORBOX_API,
        Some(Arc::new(MemoryCache::new(4 << 20))),
        mock(move |_| {
            f.fetch_add(1, Ordering::SeqCst);
            Ok(resp(200, &body))
        }),
    );
    for _ in 0..3 {
        let (ids, ok) = oversized.account_listing().await;
        assert!(!ok && ids.is_empty(), "a truncated listing must not read as an answer");
    }
    let n = fetches.load(Ordering::SeqCst);
    assert_eq!(
        n, 1,
        "pulled the oversized listing {n} times for three attempts — it is not being remembered"
    );

    // The ENTRY cap takes the same road, and had to be given it separately: it trips before the body is
    // drained, so the byte counter never reaches the cap, truncated() stays false, and the oversize memo
    // was never written. That left the entry cap re-pulling the whole listing on every poll — the exact
    // egress the memo above exists to stop — and, because the read now yields "could not find out",
    // /play escalated and did it twice per request.
    let mut compact = String::from(r#"{"success":true,"data":["#);
    for i in 0..=MAX_LISTING_ENTRIES {
        if i > 0 {
            compact.push(',');
        }
        compact.push_str(&format!(r#"{{"id":{i},"hash":"{i:040x}"}}"#));
    }
    compact.push_str("]}");
    assert!(
        compact.len() < MAX_LISTING_BYTES,
        "the fixture is {} bytes, past the {MAX_LISTING_BYTES} byte cap — it would assert the wrong cap",
        compact.len()
    );
    let entry_fetches = Arc::new(AtomicUsize::new(0));
    let ef = entry_fetches.clone();
    let too_many = tb(
        "t3",
        TORBOX_API,
        Some(Arc::new(MemoryCache::new(4 << 20))),
        mock(move |_| {
            ef.fetch_add(1, Ordering::SeqCst);
            Ok(resp(200, &compact))
        }),
    );
    for _ in 0..3 {
        let (ids, ok) = too_many.account_listing().await;
        assert!(!ok && ids.is_empty(), "a listing past the entry cap must not read as an answer");
    }
    let n = entry_fetches.load(Ordering::SeqCst);
    assert_eq!(
        n, 1,
        "pulled the over-entry-count listing {n} times for three attempts — it is not being remembered"
    );

    // An UNUSABLE listing takes the same road, for the same reason: an upstream that renamed `hash` will
    // rename it again on the next poll, so re-pulling is guaranteed waste. It shipped once as
    // "not persistent", which cost 15 listing fetches over a 15-poll wait against 1 — and two per /play
    // rather than none, because an indeterminate answer also makes every poll escalate.
    let unusable_fetches = Arc::new(AtomicUsize::new(0));
    let uf = unusable_fetches.clone();
    let unusable_body = format!(r#"{{"success":true,"data":[{{"id":1,"infohash":"{}"}}]}}"#, "a".repeat(40));
    let unusable = tb(
        "t4",
        TORBOX_API,
        Some(Arc::new(MemoryCache::new(1 << 20))),
        mock(move |_| {
            uf.fetch_add(1, Ordering::SeqCst);
            Ok(resp(200, &unusable_body))
        }),
    );
    for _ in 0..3 {
        let (ids, ok) = unusable.account_listing().await;
        assert!(!ok && ids.is_empty(), "a listing with no usable entry must not read as an answer");
    }
    let n = unusable_fetches.load(Ordering::SeqCst);
    assert_eq!(n, 1, "pulled the unusable listing {n} times for three attempts — it is not being remembered, so every poll re-pulls the whole account");

    // The whole DETERMINISTIC family takes that road, not just the one that was noticed first. A renamed
    // envelope key, a `data` that is an object, and an explicit success:false are each exactly as
    // repeatable as a renamed `hash`, and each sat on the transient road costing 45 listing fetches over a
    // 15-poll wait — 539 MiB on a 12 MiB account, three times over.
    //
    // A field of the WRONG TYPE is the same class: well-formed, and identical on the next poll. One odd
    // entry among two thousand cost 45 fetches and up to 538 MiB per wait, because the decode error was read
    // as a blip — the distinction encoding/json already draws between a type error and a body that stopped
    // early.
    //
    // JSON that is not valid at all is deterministic in the same way, and arrives as a different error type
    // — so it needs its own cases or the split is only half made. No truncation reaches these three: a body
    // cut inside an object or a string reports an unexpected EOF instead.
    let a = "a".repeat(40);
    for (name, body) in [
        ("the data key renamed", format!(r#"{{"success":true,"items":[{{"id":1,"hash":"{a}"}}]}}"#)),
        ("data is an object", format!(r#"{{"success":true,"data":{{"1":"{a}"}}}}"#)),
        ("success is false", format!(r#"{{"success":false,"data":[{{"id":1,"hash":"{a}"}}]}}"#)),
        ("an entry id is a string", format!(r#"{{"success":true,"data":[{{"id":"1","hash":"{a}"}}]}}"#)),
        ("an entry hash is a number", r#"{"success":true,"data":[{"id":1,"hash":7}]}"#.to_owned()),
        ("an entry is a scalar", r#"{"success":true,"data":[42]}"#.to_owned()),
        ("success is a string", format!(r#"{{"success":"yes","data":[{{"id":1,"hash":"{a}"}}]}}"#)),
        ("a leading-zero number", r#"{"success":true,"data":[{"id":01}]}"#.to_owned()),
        ("a trailing comma", r#"{"success":true,"data":[{"id":1,}]}"#.to_owned()),
        ("an unquoted key", r#"{"success":true,"data":[{id:1}]}"#.to_owned()),
    ] {
        let fetches = Arc::new(AtomicUsize::new(0));
        let f = fetches.clone();
        let s = tb(
            &format!("envelope-{name}"),
            TORBOX_API,
            Some(Arc::new(MemoryCache::new(1 << 20))),
            mock(move |_| {
                f.fetch_add(1, Ordering::SeqCst);
                Ok(resp(200, &body))
            }),
        );
        for _ in 0..3 {
            let (ids, ok) = s.account_listing().await;
            assert!(!ok && ids.is_empty(), "{name}: must not read as an answer");
        }
        let n = fetches.load(Ordering::SeqCst);
        assert_eq!(n, 1, "{name}: pulled the listing {n} times for three attempts — a deterministic envelope fault is not being remembered, so every poll re-pulls the whole account");
    }

    // A body cut off inside a structure is transient, and each place it can happen has its own
    // completeness check to get past. The decoder ends a loop for a read error exactly as it does for a
    // closed bracket, so asking "was anything usable?" or "was the envelope good?" before reading that
    // bracket classifies a blip as permanent — and a memoised blip suppresses the one retry that
    // rediscovers a queued torrent. (The one cut that is NOT transient is a bare scalar at end of input,
    // which scans as a complete value; it is a schema change either way, so the verdict is the same.)
    //
    // The array case is the subtle one: it needs an entry that PARSED and was then filtered, so the
    // question "entries seen, none usable" is reachable before the array is known to have closed.
    for (name, body) in [
        // inside data, after an unusable entry
        ("inside data, after an unusable entry", r#"{"success":true,"data":[{"id":1,"hash":"tooshort"}"#),
        ("data closed, outer object not", r#"{"success":true,"data":[]"#),
        // Cut at the two decode sites that now split type errors from blips: the same call that must
        // memoise a wrong-typed field must still retry a body that simply stopped.
        ("mid-entry", r#"{"success":true,"data":[{"id":1,"ha"#),
        // Cut inside the VALUE, not the key. `{"suc` looks like it exercises the success decode and does
        // not: the key token above fails first and returns before the decode is ever reached, so the
        // site's retry direction went unpinned and memoising every error there passed the whole suite.
        ("mid-success", r#"{"success":tru"#),
        // Cut inside an UNKNOWN top-level field, which is the one place that reaches skipValue. Its two
        // failures share a return and must not share a verdict: past maxSkipDepth is a property of the
        // body and is memoised, while running out of brackets mid-walk is the ordinary blip.
        ("mid-walk in an unknown field", r#"{"success":true,"x":[["#),
    ] {
        let n = Arc::new(AtomicUsize::new(0));
        let c = n.clone();
        let s = tb(
            &format!("cut-{name}"),
            TORBOX_API,
            Some(Arc::new(MemoryCache::new(1 << 20))),
            mock(move |_| {
                c.fetch_add(1, Ordering::SeqCst);
                Ok(resp(200, body))
            }),
        );
        for _ in 0..3 {
            let (ids, ok) = s.account_listing().await;
            assert!(!ok && ids.is_empty(), "{name}: a truncated listing must not read as an answer");
        }
        let n = n.load(Ordering::SeqCst);
        assert_eq!(n, 3, "{name}: pulled {n} times for three attempts — a truncated body was remembered as a permanent fault, and that retry is the only thing that rediscovers a queued torrent");
    }

    // But a body CUT OFF in the OUTER object reaches the same place and is transient — the decoder reports
    // "no more" for a read error exactly as it does for a closed object, and only reading the closing token
    // tells them apart. The fixture has to be cut where the envelope ends, not inside `data`.
    //
    // `{"success":false` complete is a persistent bad envelope; the same bytes without the brace are a
    // blip. That one character is the whole difference, which is what makes this worth pinning.
    let cut_fetches = Arc::new(AtomicUsize::new(0));
    let cf = cut_fetches.clone();
    let cut = tb(
        "cut",
        TORBOX_API,
        Some(Arc::new(MemoryCache::new(1 << 20))),
        mock(move |_| {
            cf.fetch_add(1, Ordering::SeqCst);
            Ok(resp(200, r#"{"success":false"#))
        }),
    );
    for _ in 0..3 {
        let (ids, ok) = cut.account_listing().await;
        assert!(!ok && ids.is_empty(), "a truncated listing must not read as an answer");
    }
    let n = cut_fetches.load(Ordering::SeqCst);
    assert_eq!(n, 3, "a body cut off mid-stream was pulled {n} times for three attempts — it was remembered as persistent, and that retry is the only thing that rediscovers a queued torrent");

    // A transient failure must NOT be remembered: suppressing that retry is how a queued torrent stops
    // being rediscoverable, which this package has a separate test for.
    let transient_fetches = Arc::new(AtomicUsize::new(0));
    let tf = transient_fetches.clone();
    let flaky = tb(
        "t2",
        TORBOX_API,
        Some(Arc::new(MemoryCache::new(1 << 20))),
        mock(move |_| {
            tf.fetch_add(1, Ordering::SeqCst);
            Ok(resp(503, "down"))
        }),
    );
    for _ in 0..3 {
        let _ = flaky.account_listing().await;
    }
    let n = transient_fetches.load(Ordering::SeqCst);
    assert_eq!(n, 3, "a transient listing failure was memoised ({n} fetches for three attempts) — a queued torrent would stop being rediscoverable");
}

// A body of EXACTLY the cap is not truncated, which is the case the limit+1 read exists for.
//
// The detector is fed a LimitReader of limit+1 so that "read the cap exactly" and "cut off at the cap"
// produce different byte counts. With the comparison at >= instead of >, a listing that landed precisely
// on maxListingBytes parsed perfectly and was then discarded AND remembered as oversized for the listing
// TTL — every poll for the next fifteen seconds answering "no idea" about an account that had just been
// read in full. Nothing covered it: the existing truncation case is a body far past the cap, which reads
// the same under either comparison.
#[test]
fn truncation_detector_a_body_of_exactly_the_cap_is_whole() {
    const LIMIT: usize = 1 << 10;
    for (name, size, want) in
        [("one short", LIMIT - 1, false), ("exactly the cap", LIMIT, false), ("one over", LIMIT + 1, true)]
    {
        // Constructed exactly as fetchAccountListing does it.
        let body = "x".repeat(size);
        let mut td = TruncationDetector { r: std::io::Read::take(body.as_bytes(), (LIMIT + 1) as u64) };
        let n = std::io::copy(&mut td, &mut std::io::sink()).expect("read");
        assert_eq!(
            td.truncated(),
            want,
            "{name}: a {size}-byte body under a {LIMIT}-byte cap read {n} bytes and reported truncated={}, want {want}",
            td.truncated()
        );
    }
}

// Entries do not carry fields over from the entry before them.
//
// What makes each element independent is that the struct is declared INSIDE the loop, and nothing said
// so: hoisting it above the loop — the obvious way to remove one heap allocation per entry, and half the
// allocations this decode makes — compiles and passes everything, while an entry missing `hash` silently
// inherits the previous one's. That maps a hash the account holds to a DIFFERENT torrent's id, which the
// status read then follows.
#[test]
fn decode_listing_entries_do_not_inherit_from_the_entry_before() {
    let a = "a".repeat(40);
    let b = "b".repeat(40);
    let body = format!(r#"{{"success":true,"data":[{{"id":1,"hash":"{a}"}},{{"id":2}},{{"hash":"{b}"}}]}}"#);
    let (ids, ok, _) = decode_listing(body.as_bytes());
    assert!(ok, "a listing with sparse entries is still readable");
    assert_eq!(id_of(&ids, &a), 1, "the complete entry was rewritten: {}", &a[..8]);
    assert_eq!(id_of(&ids, &b), 0, "an entry with no id took the previous entry's: {}", &b[..8]);
    assert_eq!(ids.len(), 2, "expected exactly the two hashed entries, got {ids:?}");
}

// The entry's `id` is read straight to an int64 rather than through its text: the whole range is an id, and
// anything else that is a valid number (a fraction, an exponent, one past either end) is a type error.
#[test]
fn decode_listing_id_is_an_int64_or_a_type_error() {
    let hash = "a".repeat(40);
    for (id, want) in [
        ("9223372036854775807", Some(i64::MAX)),
        ("-9223372036854775808", Some(i64::MIN)),
        ("-0", Some(0)),
        ("9223372036854775808", None),
        ("-9223372036854775809", None),
        ("1.0", None),
        ("1e2", None),
        ("-1.5E+3", None),
    ] {
        let body = format!(r#"{{"success":true,"data":[{{"id":{id},"hash":"{hash}"}}]}}"#);
        let (ids, ok, fault) = decode_listing(body.as_bytes());
        match want {
            Some(want) => assert!(ok && id_of(&ids, &hash) == want, "id {id}: ok={ok} ids={ids:?}"),
            None => assert!(!ok && fault == ListingFault::BadEnvelope, "id {id}: ok={ok} fault={fault:?}"),
        }
    }
}

// Keys match case-insensitively by simple folding, after their escapes are decoded; escapes inside a field that
// is skipped, or in the middle of the hash, do not disturb the entry around them.
#[test]
fn decode_listing_keys_fold_and_unescape() {
    let hash = "ab".repeat(20);
    let upper = hash.to_uppercase();
    for key in ["id", "ID", "Id"] {
        let body = format!(r#"{{"success":true,"data":[{{"{key}":5,"hash":"{hash}"}}]}}"#);
        assert_eq!(id_of(&decode_listing(body.as_bytes()).0, &hash), 5, "key {key}");
    }
    for key in ["hash", "HASH", "HaSh", "ha\u{17f}h", "\\u0068ash", "h\\u0061sh"] {
        let body = format!(r#"{{"success":true,"data":[{{"id":6,"{key}":"{upper}"}}]}}"#);
        assert_eq!(id_of(&decode_listing(body.as_bytes()).0, &hash), 6, "key {key}");
    }
    for key in ["hash2", "hashh", "has", "ha\u{17f}", "idd", "\\u00e9d", "ha\\ud800sh"] {
        let body = format!(r#"{{"success":true,"data":[{{"id":6,"{key}":"{hash}"}}]}}"#);
        let (_, ok, fault) = decode_listing(body.as_bytes());
        assert!(!ok && fault == ListingFault::NoUsableEntries, "key {key}: ok={ok} fault={fault:?}");
    }
    // A surrogate pair, a lone surrogate, a BMP escape and the short escapes, all spelled as JSON escapes.
    let name = "a\\n\\\"b\\\" \\ud83d\\ude00 \\ud800x \\u00e9";
    let body = format!(r#"{{"success":true,"data":[{{"id":7,"name":"{name}","hash":"{hash}"}}]}}"#);
    assert_eq!(id_of(&decode_listing(body.as_bytes()).0, &hash), 7);
}

// A deeply nested unknown field is refused rather than walked: a body nested past the bound is an envelope
// fault, not a long walk.
#[test]
fn skip_value_accepts_depth_just_under_the_bound() {
    let hash = "a".repeat(40);
    for (name, depth, want_ok) in
        [("under the bound", 63, true), ("exactly at the bound", 64, true), ("past the bound", 65, false)]
    {
        // The nested field precedes `data` so the refusal, when it comes, is the depth bound rather
        // than anything downstream; a body that decodes must still yield the entry after it.
        let body = format!(
            r#"{{"success":true,"x":{}{},"data":[{{"id":1,"hash":"{hash}"}}]}}"#,
            "[".repeat(depth),
            "]".repeat(depth)
        );
        let (ids, ok, fault) = decode_listing(body.as_bytes());
        assert_eq!(ok, want_ok, "{name}: depth {depth}: ok={ok} fault={fault:?}, want ok={want_ok}");
        if !want_ok {
            assert_eq!(
                fault,
                ListingFault::BadEnvelope,
                "{name}: depth {depth}: fault={fault:?}, want BadEnvelope"
            );
            continue;
        }
        assert_eq!(
            id_of(&ids, &hash),
            1,
            "{name}: depth {depth} decoded, but the entry after the nested field was lost: {ids:?}"
        );
    }
}

// The listing map is bounded by ENTRY COUNT as well as by bytes. The fault is reported separately, because
// that is what makes the caller remember the account as oversized instead of re-pulling it every poll.
#[test]
fn decode_listing_bounds_entry_count() {
    let mut body = String::from(r#"{"success":true,"data":["#);
    for i in 0..=MAX_LISTING_ENTRIES {
        if i > 0 {
            body.push(',');
        }
        body.push_str(&format!(r#"{{"id":{i},"hash":"{i:040x}"}}"#));
    }
    body.push_str("]}");

    let (_, ok, fault) = decode_listing(body.as_bytes());
    assert!(!ok && fault == ListingFault::TooManyEntries, "a listing with more than {MAX_LISTING_ENTRIES} entries: ok={ok} fault={fault:?} (want false, TooManyEntries)");

    // And EXACTLY the cap still succeeds. Only the "one too many fails" side was pinned, so tightening the
    // comparison by one passed the whole suite — and that direction is worse than the one covered: it turns
    // a legitimate account at the ceiling into a permanent statusUnknown plus a 15 s oversized memo, on
    // every poll, for an account that is merely large.
    let mut at_cap = String::from(r#"{"success":true,"data":["#);
    for i in 0..MAX_LISTING_ENTRIES {
        if i > 0 {
            at_cap.push(',');
        }
        at_cap.push_str(&format!(r#"{{"id":{i},"hash":"{i:040x}"}}"#));
    }
    at_cap.push_str("]}");
    let (ids, ok, fault) = decode_listing(at_cap.as_bytes());
    assert!(ok && fault == ListingFault::None && ids.len() == MAX_LISTING_ENTRIES, "a listing of exactly {MAX_LISTING_ENTRIES} entries: ok={ok} fault={fault:?} n={} (want true, None, {MAX_LISTING_ENTRIES})", ids.len());
}

// The tests below are ignored until the package that owns what they reach lands. Each keeps its full Go
// body in a compiled-out item, so the port is reviewable now and only needs moving up when enabled.

/// Go `fakeStore`: a Store answering from fixed values, for the pool tests.
#[derive(Clone)]
struct FakeStore {
    svc: DebridService,
    check: HashMap<String, bool>,
    check_err: Option<StoreError>,
    resolve: Option<Result<String, StoreError>>,
    status: Option<StoreStatus>,
    /// Makes `status` outlast whatever budget it is given, the way a slow account listing does.
    status_blocks: bool,
}

impl FakeStore {
    fn new(svc: DebridService) -> FakeStore {
        FakeStore {
            svc,
            check: HashMap::new(),
            check_err: None,
            resolve: None,
            status: None,
            status_blocks: false,
        }
    }

    fn check(mut self, answers: &[(&str, bool)]) -> FakeStore {
        self.check = answers.iter().map(|(h, held)| ((*h).to_owned(), *held)).collect();
        self
    }

    fn check_err(mut self, err: StoreError) -> FakeStore {
        self.check_err = Some(err);
        self
    }

    fn resolve_with(mut self, answer: Result<String, StoreError>) -> FakeStore {
        self.resolve = Some(answer);
        self
    }

    fn status_blocks(mut self) -> FakeStore {
        self.status_blocks = true;
        self
    }
}

#[async_trait::async_trait]
impl Store for FakeStore {
    fn service(&self) -> DebridService {
        self.svc
    }

    async fn cache_check(&self, _hashes: &[String]) -> Result<HashMap<String, bool>, StoreError> {
        match &self.check_err {
            Some(err) => Err(err.clone()),
            None => Ok(self.check.clone()),
        }
    }

    async fn resolve(&self, _t: &ResolveTarget) -> Result<String, StoreError> {
        self.resolve.clone().expect("this fake store was given no resolve")
    }

    async fn provider_status(&self, _t: &ResolveTarget) -> ProviderStatus {
        if self.status_blocks {
            std::future::pending::<()>().await;
        }
        self.status.clone().into()
    }
}

/// Go `answeringStore`: implements the optional three-valued `statusAnswerer`, and DISAGREES with its own two-valued
/// `Status` (always "no") on purpose — that is the only way a test can tell which one the pool consulted.
#[derive(Clone)]
struct AnsweringStore {
    svc: DebridService,
    answer: ProviderStatus,
    asked: Arc<AtomicUsize>,
    /// Go `onAsk`: runs after the answer is recorded, so a test can make the world change mid-loop — here, the
    /// clock running past the pool's deadline, which stands in for the caller hanging up.
    on_ask: Option<Arc<dyn Fn() + Send + Sync>>,
}

impl AnsweringStore {
    fn new(svc: DebridService, answer: ProviderStatus, asked: Arc<AtomicUsize>) -> AnsweringStore {
        AnsweringStore { svc, answer, asked, on_ask: None }
    }
}

#[async_trait::async_trait]
impl Store for AnsweringStore {
    fn service(&self) -> DebridService {
        self.svc
    }

    async fn cache_check(&self, _hashes: &[String]) -> Result<HashMap<String, bool>, StoreError> {
        Ok(HashMap::new())
    }

    async fn resolve(&self, _t: &ResolveTarget) -> Result<String, StoreError> {
        panic!("not resolved in these tests")
    }

    async fn provider_status(&self, _t: &ResolveTarget) -> ProviderStatus {
        self.asked.fetch_add(1, Ordering::SeqCst);
        if let Some(on_ask) = &self.on_ask {
            on_ask();
        }
        match self.answer {
            ProviderStatus::Downloading(_) => {
                ProviderStatus::Downloading(StoreStatus { progress: 0.5, ..Default::default() })
            }
            ProviderStatus::NotFetching => ProviderStatus::NotFetching,
            ProviderStatus::Unknown => ProviderStatus::Unknown,
        }
    }
}

#[tokio::test]
async fn real_debrid() {
    // RD cacheCheck is all-false with no error, even on a store with no token.
    let m = RealDebridStore {
        token: String::new(),
        api: REAL_DEBRID_API.into(),
        cache: None,
        client: mock(|_| Ok(resp(200, ""))),
    }
    .cache_check(&[H.to_owned()])
    .await;
    assert!(m.is_ok_and(|m| !held(&m, H)), "RD cacheCheck should be all-false, no error");

    let infocalls = Arc::new(AtomicUsize::new(0));
    let ic = infocalls.clone();
    let d = mock(move |req| {
        let p = req.url().path().to_owned();
        if p.contains("addMagnet") {
            return Ok(resp(201, r#"{"id":"t1"}"#));
        }
        if p.contains("/torrents/info/") {
            let n = ic.fetch_add(1, Ordering::SeqCst) + 1;
            if n == 1 {
                return Ok(resp(200, r#"{"files":[{"id":1,"path":"/movie.mkv","bytes":999}],"links":[]}"#));
            }
            return Ok(resp(
                200,
                r#"{"files":[{"id":1,"path":"/movie.mkv","bytes":999}],"links":["https://rd/restricted"]}"#,
            ));
        }
        if p.contains("selectFiles") {
            return Ok(resp(204, ""));
        }
        if p.contains("unrestrict/link") {
            return Ok(resp(200, r#"{"download":"https://rd/dl.mkv"}"#));
        }
        Ok(resp(404, "{}"))
    });
    let link = RealDebridStore { token: "t".into(), api: REAL_DEBRID_API.into(), cache: None, client: d }
        .resolve(&target_hash(H))
        .await;
    assert_eq!(link.as_deref(), Ok("https://rd/dl.mkv"), "RD resolve");

    // blocked filename → DeadLinkError (pool falls through)
    let db = mock(|req| {
        if req.url().path().contains("addMagnet") {
            return Ok(resp(201, r#"{"id":"t1"}"#));
        }
        Ok(resp(200, r#"{"files":[{"id":1,"path":"Movie.WEB-DL.x264.mkv","bytes":999}],"links":[]}"#))
    });
    assert!(
        RealDebridStore { token: "t".into(), api: REAL_DEBRID_API.into(), cache: None, client: db }
            .resolve(&target_hash(H))
            .await
            .is_err(),
        "RD-blocked filename should error"
    );
}

#[tokio::test]
async fn premiumize() {
    let other = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
    let dc = mock(|_| Ok(resp(200, r#"{"status":"success","response":[true,false]}"#)));
    let m = PremiumizeStore { token: "t".into(), api: PREMIUMIZE_API.into(), cache: None, client: dc }
        .cache_check(&[H.to_owned(), other.to_owned()])
        .await;
    assert!(m.as_ref().is_ok_and(|m| held(m, H) && !held(m, other)), "PM cacheCheck: {m:?}");

    let dr = mock(|_| {
        Ok(resp(
            200,
            r#"{"status":"success","content":[{"path":"S01E01.mkv","link":"https://pm/1","size":10},{"path":"S01E02.mkv","link":"https://pm/2","size":20}]}"#,
        ))
    });
    let link = PremiumizeStore { token: "t".into(), api: PREMIUMIZE_API.into(), cache: None, client: dr }
        .resolve(&target_episode(H, 1, 2))
        .await;
    assert_eq!(link.as_deref(), Ok("https://pm/2"), "PM resolve");
}

// The pool asks statusAnswerer where a store has one, in preference to the bool Status.
#[tokio::test]
async fn store_pool_prefers_the_three_valued_answer() {
    // A store whose two-valued Status always says "no", and whose three-valued answer is set per case.
    for (name, answer, want_ok, want_unknown) in [
        ("unknown is carried, not flattened to no", ProviderStatus::Unknown, false, true),
        ("downloading is carried too", ProviderStatus::Downloading(StoreStatus::default()), true, false),
    ] {
        let asked = Arc::new(AtomicUsize::new(0));
        let pool = StorePool {
            stores: vec![Box::new(AnsweringStore::new(DebridService::TorBox, answer, asked.clone()))],
        };
        let outcome = pool.status_outcome(&target_hash(H), None).await;
        let (status, ok, unknown) = match outcome.result {
            ProviderStatus::Downloading(status) => (status, true, false),
            ProviderStatus::NotFetching => (StoreStatus::default(), false, false),
            ProviderStatus::Unknown => (StoreStatus::default(), false, true),
        };
        assert!(ok == want_ok && unknown == want_unknown, "{name}: pool answered ok={ok} unknown={unknown}");
        assert_eq!(
            asked.load(Ordering::SeqCst),
            1,
            "{name}: ProviderStatus called the wrong number of times — the pool used the bool fallback"
        );
        if want_ok {
            assert_eq!(status.progress, 0.5, "{name}: the status itself was dropped");
        }
    }
}

#[tokio::test]
async fn store_pool() {
    // buildStores orders TorBox first regardless of config order.
    let cfg = Config {
        debrid: vec![
            DebridAccount { service: DebridService::Premiumize, token: "p".into() },
            DebridAccount { service: DebridService::TorBox, token: "t".into() },
            DebridAccount { service: DebridService::RealDebrid, token: "r".into() },
        ],
        ..Default::default()
    };
    let stores = build_stores(&cfg, mock(|_| Ok(resp(404, ""))), None);
    assert!(
        stores.len() == 3
            && stores[0].service() == DebridService::TorBox
            && stores[2].service() == DebridService::Premiumize,
        "store order"
    );

    let other = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
    let pool = StorePool {
        stores: vec![
            Box::new(FakeStore::new(DebridService::TorBox).check(&[(H, true), (other, false)])),
            Box::new(FakeStore::new(DebridService::Premiumize).check(&[(H, false), (other, true)])),
        ],
    };
    let (m, truth_ok) = pool.cache_check(&[H.to_owned(), other.to_owned()]).await;
    assert!(m.cached(H) && m.cached(other), "pool union: {:?} / {:?}", m.held_by(H), m.held_by(other));
    assert!(truth_ok, "pool truthOK should be true when a cache-truth store succeeded");

    // all cache-truth stores erroring → truthOK false (an outage the handler must not cache)
    let down = StorePool {
        stores: vec![Box::new(
            FakeStore::new(DebridService::TorBox).check(&[(H, false)]).check_err(dead_link(ERR_CHECK_FAILED)),
        )],
    };
    let (_, ok) = down.cache_check(&[H.to_owned()]).await;
    assert!(!ok, "truthOK should be false when every cache-truth store failed");
    // RD succeeding is not cache truth → truthOK false
    let rd =
        StorePool { stores: vec![Box::new(FakeStore::new(DebridService::RealDebrid).check(&[(H, false)]))] };
    let (_, ok) = rd.cache_check(&[H.to_owned()]).await;
    assert!(!ok, "RD success is not cache truth");

    let dead = || Err(StoreError::DeadLink(DeadLinkError { reason: "x".into() }));
    let dead_only =
        StorePool { stores: vec![Box::new(FakeStore::new(DebridService::TorBox).resolve_with(dead()))] };
    assert!(dead_only.resolve(&target_hash(H)).await.is_err(), "all-fail should error");
    let fallthr = StorePool {
        stores: vec![
            Box::new(FakeStore::new(DebridService::TorBox).resolve_with(dead())),
            Box::new(FakeStore::new(DebridService::TorBox).resolve_with(Ok("https://ok".into()))),
        ],
    };
    assert_eq!(fallthr.resolve(&target_hash(H)).await.as_deref(), Ok("https://ok"), "fallthrough");
}

#[test]
fn has_cache_truth_reads_the_configured_services() {
    let rd = DebridAccount { service: DebridService::RealDebrid, token: "r".into() };
    let tb = DebridAccount { service: DebridService::TorBox, token: "t".into() };
    assert!(
        !has_cache_truth(&Config { debrid: vec![rd.clone()], ..Default::default() }),
        "RD-only should have no cache truth"
    );
    assert!(
        has_cache_truth(&Config { debrid: vec![rd, tb], ..Default::default() }),
        "torbox present → cache truth"
    );
}

#[test]
fn has_cache_truth_and_rd_only() {
    let rd = DebridAccount { service: DebridService::RealDebrid, token: "r".into() };
    let tb = DebridAccount { service: DebridService::TorBox, token: "t".into() };
    assert!(
        !has_cache_truth(&Config { debrid: vec![rd.clone()], ..Default::default() }),
        "RD-only should have no cache truth"
    );
    assert!(
        has_cache_truth(&Config { debrid: vec![rd.clone(), tb.clone()], ..Default::default() }),
        "torbox present → cache truth"
    );
    assert!(rd_only(&Config { debrid: vec![rd.clone()], ..Default::default() }), "rd-only");
    assert!(
        !rd_only(&Config { debrid: vec![rd, tb], ..Default::default() }),
        "not rd-only when torbox present"
    );
}

#[tokio::test]
async fn torbox_account_state_is_isolated_by_api_and_token() {
    // Account state is keyed by API endpoint and token: two endpoints sharing a token must not share facts.
    const TOKEN: &str = "shared-token";
    let cache: Arc<dyn Cache> = Arc::new(MemoryCache::new(1 << 20));
    let calls = Arc::new(AtomicUsize::new(0));
    let c = calls.clone();
    let client = mock(move |req| {
        c.fetch_add(1, Ordering::SeqCst);
        let id = if req.url().host_str().unwrap_or("").contains("torbox-b") { 22 } else { 11 };
        Ok(resp(200, &format!(r#"{{"success":true,"data":[{{"id":{id},"hash":"{H}"}}]}}"#)))
    });
    let a = tb(TOKEN, "https://torbox-a.invalid/v1", Some(cache.clone()), client.clone());
    let b = tb(TOKEN, "https://torbox-b.invalid/v1", Some(cache.clone()), client);

    // The whole-account listing memo and its singleflight must not hand endpoint B endpoint A's ids.
    let (ids_a, ok_a) = a.account_listing().await;
    let (ids_b, ok_b) = b.account_listing().await;
    assert!(ok_a && ok_b && id_of(&ids_a, H) == 11 && id_of(&ids_b, H) == 22, "cross-API listings");
    assert_eq!(calls.load(Ordering::SeqCst), 2, "two API accounts made listing calls, want 2");

    // Per-hash ids and resolve/file facts are account data, not facts about the token string itself.
    cache.put(&torrent_id_key(&a.account_identity(), H), "11", Duration::from_secs(3600));
    let (id, held_b) = b.known_torrent_id(H);
    assert!(!held_b, "endpoint B accepted endpoint A's torrent id {id}");
    cache.put(
        &resolve_key(&a.account_identity(), H),
        r#"{"torrentId":11,"files":[{"Index":0,"Name":"Movie.mkv","SizeBytes":123}]}"#,
        Duration::from_secs(3600),
    );
    assert!(
        b.known_file_size(&target_hash(H)).is_none(),
        "endpoint B accepted endpoint A's resolve/file fact"
    );

    // A positive cache check is scoped the same way.
    cache.put(&cached_key(DebridService::TorBox, &a.account_identity(), H), "1", CACHED_TTL);
    let (known, ask) = known_cached(&*cache, DebridService::TorBox, &b.account_identity(), &[H.to_owned()]);
    assert!(!held(&known, H) && ask.len() == 1, "endpoint B reused endpoint A's positive");

    // Migration is fail-cold: a pre-v2 token-only file is never accepted under either arbitrary API.
    cache.put(&format!("torbox:torrent:{}:{H}", key_hash(TOKEN)), "99", Duration::from_secs(3600));
    assert!(a.known_torrent_id(H).1, "endpoint A lost its canonical torrent id");
    let other_hash = "b".repeat(40);
    cache.put(&format!("torbox:torrent:{}:{other_hash}", key_hash(TOKEN)), "99", Duration::from_secs(3600));
    assert!(!a.known_torrent_id(&other_hash).1, "accepted a legacy token-only TorBox fact");
}

#[tokio::test]
async fn torbox_status() {
    crate::runtime::scope(crate::runtime::ScoutRuntime::new(), async {
        // A download TorBox reports on is a wait; an unknown or finished-but-unresolvable one is not.
        for (name, body, want_ok, want_prog) in [
            (
                "downloading",
                r#"{"success":true,"data":{"progress":0.42,"download_finished":false,"eta":300}}"#,
                true,
                0.42,
            ),
            (
                "array payload",
                r#"{"success":true,"data":[{"progress":0.1,"download_finished":false}]}"#,
                true,
                0.1,
            ),
            (
                "finished but unresolvable",
                r#"{"success":true,"data":{"progress":1,"download_finished":true}}"#,
                false,
                0.0,
            ),
            ("deleted torrent", r#"{"success":false,"data":null}"#, false, 0.0),
            ("null payload", r#"{"success":true,"data":null}"#, false, 0.0),
            ("empty object", r#"{"success":true,"data":{}}"#, false, 0.0),
        ] {
            let cache: Arc<dyn Cache> = Arc::new(MemoryCache::new(1 << 16));
            cache.put(&torrent_id_key("t", H), "7", Duration::from_secs(3600));
            let s = tb("t", TORBOX_API, Some(cache), mock(move |_| Ok(resp(200, body))));
            let got = s.provider_status(&target_hash(H)).await;
            assert_eq!(got.is_downloading(), want_ok, "{name}: ok");
            if let (true, ProviderStatus::Downloading(g)) = (want_ok, got) {
                assert_eq!(g.progress, want_prog, "{name}: progress");
            }
        }
        // No torrent id remembered, and the account doesn't hold it either → nothing to report.
        let empty = tb(
            "t",
            TORBOX_API,
            Some(Arc::new(MemoryCache::new(1 << 16))),
            mock(|_| Ok(resp(200, r#"{"data":[]}"#))),
        );
        assert!(
            !empty.provider_status(&target_hash(H)).await.is_downloading(),
            "an unknown infohash must not report a download"
        );
    })
    .await;
}

#[tokio::test]
async fn held_real_debrid_transport_failure_remains_unknown() {
    let cache: Arc<dyn Cache> = Arc::new(MemoryCache::new(1 << 16));
    let target = target_hash(H);
    cache.put(&rd_torrent_key("t", H, &target), "rd-id", Duration::from_secs(3600));
    let store = RealDebridStore {
        token: "t".into(),
        api: REAL_DEBRID_API.into(),
        cache: Some(cache),
        client: mock(|_| Err(Box::new(std::io::Error::other("transport failed")))),
    };

    assert_eq!(store.provider_status(&target).await, ProviderStatus::Unknown);
    let pool = StorePool { stores: vec![Box::new(store)] };
    let outcome = pool.status_outcome(&target, None).await;
    assert!(matches!(outcome.result, ProviderStatus::Unknown));
    assert_eq!(outcome.unresolved.len(), 1, "the provider must remain available for a targeted retry");
}

#[tokio::test]
async fn torbox_status_carries_state_and_swarm() {
    crate::runtime::scope(crate::runtime::ScoutRuntime::new(), async {
    // Status carries TorBox's state and swarm through, rather than reducing every wait to a percentage.
    for (name, body, want_state, want_seeds, want_peers) in [
        (
            "stalled, empty swarm",
            r#"{"data":{"progress":0,"download_finished":false,"eta":8640000,"download_speed":0,"download_state":"stalled (no seeds)","seeds":0,"peers":0}}"#,
            FETCH_STALLED,
            Some(0),
            Some(0),
        ),
        (
            "failed",
            r#"{"data":{"progress":0.3,"download_finished":false,"download_state":"failed"}}"#,
            FETCH_FAILED,
            None,
            None,
        ),
        (
            "metadata",
            r#"{"data":{"progress":0,"download_finished":false,"download_state":"metaDL","seeds":4}}"#,
            FETCH_FETCHING,
            Some(4),
            None,
        ),
        ("no state named", r#"{"data":{"progress":0.4,"download_finished":false}}"#, "", None, None),
    ] {
        let cache: Arc<dyn Cache> = Arc::new(MemoryCache::new(1 << 16));
        cache.put(&torrent_id_key("t", H), "7", Duration::from_secs(3600));
        let s = tb("t", TORBOX_API, Some(cache), mock(move |_| Ok(resp(200, body))));
        let got = s
            .provider_status(&target_hash(H))
            .await
            .expect_downloading("a download TorBox reports on must be a wait");
        assert_eq!(got.state, want_state, "{name}: state");
        assert!(
            same_count(got.seeds, want_seeds) && same_count(got.peers, want_peers),
            "{name}: seeds/peers"
        );
    }
    })
    .await;
}

/// Go `sameCount`: two optional counts are the same when both are absent or both are present and equal.
fn same_count(a: Option<i64>, b: Option<i64>) -> bool {
    a == b
}

#[tokio::test]
async fn torbox_status_rediscovers_lost_torrent_id() {
    crate::runtime::scope(crate::runtime::ScoutRuntime::new(), async {
        // Losing the cached torrent id is not losing the download: the id is rediscovered from the account's
        // own list, and remembered, so the next poll is a single-id lookup again.
        let list_calls = Arc::new(AtomicUsize::new(0));
        let lc = list_calls.clone();
        // The indexer's hash is upper-case, TorBox reports lower-case — so this covers the fold too.
        let target = H.to_uppercase();
        let cache: Arc<dyn Cache> = Arc::new(MemoryCache::new(1 << 16)); // deliberately empty
        let s = tb(
            "t",
            TORBOX_API,
            Some(cache.clone()),
            mock(move |req| {
                if req.url().query().unwrap_or("").contains("id=") {
                    return Ok(resp(200, r#"{"data":{"progress":0.25,"download_finished":false}}"#));
                }
                lc.fetch_add(1, Ordering::SeqCst);
                Ok(resp(200, &format!(r#"{{"data":[{{"id":7,"hash":"{H}"}}]}}"#)))
            }),
        );
        let got = s
            .provider_status(&target_hash(&target))
            .await
            .expect_downloading("a download the account is holding must be reported");
        assert_eq!(got.progress, 0.25, "progress");
        assert_eq!(
            cache.get(&torrent_id_key("t", &target)).as_deref(),
            Some("7"),
            "the rediscovered id must be remembered"
        );
        // Second call must reuse the cache rather than list the whole account again.
        assert!(s.provider_status(&target_hash(&target)).await.is_downloading(), "second status");
        assert_eq!(list_calls.load(Ordering::SeqCst), 1, "account listed more than once");
    })
    .await;
}

#[tokio::test]
async fn torbox_remembers_torrent_id_for_queued_episode() {
    // The series path: Resolve refuses to cache an entry whose file list is empty, and a just-queued torrent
    // has no files yet. The id must survive that anyway.
    let cache: Arc<dyn Cache> = Arc::new(MemoryCache::new(1 << 16));
    let s = tb(
        "t",
        TORBOX_API,
        Some(cache.clone()),
        mock(|req| {
            let p = req.url().path().to_owned();
            if p.contains("createtorrent") {
                return Ok(resp(200, r#"{"data":{"torrent_id":7}}"#));
            }
            if p.contains("mylist") {
                return Ok(resp(200, r#"{"success":true,"data":{"files":[]}}"#));
                // queued: no files yet
            }
            Ok(resp(200, r#"{"success":false}"#)) // link not ready
        }),
    );
    let target = target_episode(H, 1, 3);
    assert!(
        s.resolve(&target).await.is_err(),
        "expected the link request to fail while the torrent is still queued"
    );
    assert!(
        cache.get(&torrent_id_key("t", H)).is_some(),
        "the torrent id must be remembered so Status can report the download"
    );
}

/// Go's status test doer: every call takes 20 ms, counts itself and reports a download at half way.
struct SlowStatusDoer {
    calls: Arc<AtomicUsize>,
}

#[async_trait::async_trait]
impl Doer for SlowStatusDoer {
    async fn do_request(&self, _req: reqwest::Request) -> Result<reqwest::Response, DoError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        tokio::time::sleep(Duration::from_millis(20)).await;
        Ok(resp(200, r#"{"data":{"progress":0.5,"download_finished":false}}"#))
    }
}

#[tokio::test]
async fn torbox_provider_status_coalesces_viewers_and_reuses_snapshot() {
    // 20 concurrent viewers of one download make one status call; a warm snapshot makes no further call.
    const TOKEN: &str = "status-coordination-test";
    let cache: Arc<dyn Cache> = Arc::new(MemoryCache::new(1 << 20));
    cache.put(&torrent_id_key(TOKEN, H), "77", Duration::from_secs(3600));
    let calls = Arc::new(AtomicUsize::new(0));
    let c = calls.clone();
    let store = Arc::new(tb(TOKEN, TORBOX_API, Some(cache), Arc::new(SlowStatusDoer { calls: c })));
    let barrier = Arc::new(tokio::sync::Barrier::new(21));
    let mut tasks = Vec::new();
    for _ in 0..20 {
        let (store, barrier) = (store.clone(), barrier.clone());
        tasks.push(tokio::spawn(async move {
            barrier.wait().await;
            assert!(store.provider_status(&target_hash(H)).await.is_downloading(), "answer");
        }));
    }
    barrier.wait().await;
    for t in tasks {
        t.await.expect("task");
    }
    assert_eq!(calls.load(Ordering::SeqCst), 1, "20 concurrent viewers made status calls, want 1");
    assert!(store.provider_status(&target_hash(H)).await.is_downloading(), "snapshot answer");
    assert_eq!(calls.load(Ordering::SeqCst), 1, "warm snapshot made another call");
}

#[tokio::test]
async fn torbox_provider_status_coordination_includes_api_identity() {
    // Two API identities sharing one token must not share status coordination.
    const TOKEN: &str = "same-status-token-different-api";
    let cache: Arc<dyn Cache> = Arc::new(MemoryCache::new(1 << 20));
    let calls = Arc::new(AtomicUsize::new(0));
    let barrier = Arc::new(tokio::sync::Barrier::new(3));
    let mut tasks = Vec::new();
    for api in ["https://torbox-a.invalid/v1", "https://torbox-b.invalid/v1"] {
        cache.put(&torrent_id_key(&torbox_account_identity(api, TOKEN), H), "77", Duration::from_secs(3600));
        let store = tb(TOKEN, api, Some(cache.clone()), Arc::new(SlowStatusDoer { calls: calls.clone() }));
        let barrier = barrier.clone();
        tasks.push(tokio::spawn(async move {
            barrier.wait().await;
            assert!(store.provider_status(&target_hash(H)).await.is_downloading(), "{api} answer");
        }));
    }
    barrier.wait().await;
    for t in tasks {
        t.await.expect("task");
    }
    assert_eq!(calls.load(Ordering::SeqCst), 2, "two API identities shared status coordination");
}

#[test]
fn queued_body_carries_state_and_swarm() {
    // The 202 body names the state and the swarm when the store knew them, and omits the counts when it didn't.
    let decode = |status: StoreStatus| -> serde_json::Value {
        serde_json::from_slice(&serde_json::to_vec(&queued_body(&status)).expect("encode")).expect("decode")
    };
    let stalled = decode(StoreStatus {
        state: FETCH_STALLED.into(),
        seeds: Some(0),
        peers: Some(2),
        ..Default::default()
    });
    assert!(
        stalled["state"] == "stalled" && stalled["seeds"] == 0 && stalled["peers"] == 2,
        "stalled body: {stalled}"
    );
    let plain = decode(StoreStatus { progress: 0.5, ..Default::default() });
    assert_eq!(plain["state"], "downloading", "a store naming no state must still answer downloading");
    assert!(plain.get("seeds").is_none(), "unknown seeds must be omitted, not zero: {plain}");
}

#[tokio::test]
async fn store_pool_status_reports_when_a_store_could_not_answer() {
    // A store that could not answer is not a store saying "no", and the pool has to report the difference.
    let wedged = FakeStore::new(DebridService::TorBox).status_blocks();
    let silent = FakeStore::new(DebridService::RealDebrid); // answers at once: "not downloading"

    // One store that runs out of time → indeterminate, even though the pool returns early. The caller's
    // 80 ms budget is the deadline the pool is given.
    let pool = StorePool { stores: vec![Box::new(wedged), Box::new(silent.clone())] };
    let start = std::time::Instant::now();
    let deadline = tokio::time::Instant::now() + Duration::from_millis(80);
    let outcome = pool.status_outcome(&target_hash(H), Some(deadline)).await;
    let ok = outcome.result.is_downloading();
    let unknown = !outcome.unresolved.is_empty();
    assert!(!ok, "a wedged store reported a download");
    assert!(unknown, "a store that ran out of time was reported as a definitive 'not downloading'");
    assert!(
        start.elapsed() < Duration::from_millis(80),
        "the first store consumed the whole budget ({:?}) — the slices are not being applied",
        start.elapsed()
    );

    // Every store answering promptly → a real answer, not indeterminate.
    let clean = StorePool { stores: vec![Box::new(silent.clone()), Box::new(silent)] };
    let deadline = tokio::time::Instant::now() + Duration::from_secs(1);
    let outcome = clean.status_outcome(&target_hash(H), Some(deadline)).await;
    let ok = outcome.result.is_downloading();
    let unknown = !outcome.unresolved.is_empty();
    assert!(!ok && !unknown, "prompt answers reported ok={ok} unknown={unknown}, want false/false");
}

#[tokio::test]
async fn store_pool_status_outcome_names_only_the_stores_that_could_not_answer() {
    // The named outcome retains stores that could not answer, so the escalated retry asks only those.
    let uncertain_asks = Arc::new(AtomicUsize::new(0));
    let definite_asks = Arc::new(AtomicUsize::new(0));
    let uncertain =
        AnsweringStore::new(DebridService::TorBox, ProviderStatus::Unknown, uncertain_asks.clone());
    let definite =
        AnsweringStore::new(DebridService::RealDebrid, ProviderStatus::NotFetching, definite_asks.clone());

    let pool = StorePool {
        stores: vec![Box::new(definite.clone()), Box::new(uncertain), Box::new(definite.clone())],
    };
    let outcome = pool.status_outcome(&target_hash(H), None).await;
    assert!(!outcome.result.is_downloading(), "nothing was downloading");
    let unknown = outcome.unresolved;
    assert_eq!(unknown.len(), 1, "reported {} stores as unable to answer, want 1", unknown.len());
    assert_eq!(unknown[0].service(), DebridService::TorBox, "named the store that could not answer");

    // The retry, as handlePlay performs it: only the named store is asked again.
    let before = definite_asks.load(Ordering::SeqCst);
    let _ = crate::stores::pool::status_outcome_among(&unknown, &target_hash(H), None).await;
    assert_eq!(
        definite_asks.load(Ordering::SeqCst),
        before,
        "the retry asked a store that had already answered definitively"
    );
    assert_eq!(
        uncertain_asks.load(Ordering::SeqCst),
        2,
        "the store that could not answer was asked {} times, want 2",
        uncertain_asks.load(Ordering::SeqCst)
    );
}

#[tokio::test]
async fn store_pool_status_outcome_keeps_uncertain_stores_when_time_runs_out() {
    // Running out of time mid-loop keeps the stores that ALREADY said they could not answer, as well as the ones
    // never reached: three stores — answered, uncertain, never reached — and the caller hangs up (here: the clock
    // passes the deadline while the second store is being asked, which a blocking sleep makes certain).
    let deadline = tokio::time::Instant::now() + Duration::from_millis(40);
    let answered = AnsweringStore::new(
        DebridService::Premiumize,
        ProviderStatus::NotFetching,
        Arc::new(AtomicUsize::new(0)),
    );
    let mut uncertain =
        AnsweringStore::new(DebridService::TorBox, ProviderStatus::Unknown, Arc::new(AtomicUsize::new(0)));
    uncertain.on_ask = Some(Arc::new(|| std::thread::sleep(Duration::from_millis(60))));
    let unreached = FakeStore::new(DebridService::RealDebrid);

    let pool = StorePool { stores: vec![Box::new(answered), Box::new(uncertain), Box::new(unreached)] };
    let outcome = pool.status_outcome(&target_hash(H), Some(deadline)).await;
    assert!(!outcome.result.is_downloading(), "nothing was downloading");
    let unknown = outcome.unresolved;
    let named: Vec<DebridService> = unknown.iter().map(|s| s.service()).collect();
    assert_eq!(named, vec![DebridService::TorBox, DebridService::RealDebrid], "named as unable to answer");
}

#[tokio::test]
async fn account_listing_a_panic_in_the_fetch_is_abandoned_not_fatal() {
    // A panic inside the listing fetch is abandoned, not fatal. Its sole admission slot and map entry must both
    // be returned: otherwise an unrelated healthy account remains refused forever after one parser panic.
    let runtime = crate::runtime::ScoutRuntime::new_with_account_listing_limit(1);
    crate::runtime::scope(runtime.clone(), async move {
        let before = crate::metrics::background_panic();
        let s = tb(
            "panics",
            TORBOX_API,
            Some(Arc::new(MemoryCache::new(1 << 20))),
            mock(|_| panic!("a parser went off the end")),
        );
        let s = Arc::new(s);
        let mut tasks = Vec::new();
        for _ in 0..2 {
            let s = s.clone();
            let owner = runtime.clone();
            tasks.push(tokio::spawn(crate::runtime::scope(owner, async move {
                let (ids, ok) = tokio::time::timeout(Duration::from_secs(5), s.account_listing())
                    .await
                    .expect("a caller never returned from a panicking fetch");
                ok || !ids.is_empty()
            })));
        }
        for t in tasks {
            assert!(!t.await.expect("task"), "a panicking fetch reported an answer");
        }
        assert!(
            crate::metrics::background_panic() > before,
            "the panic was not counted — an operator has nothing to alert on"
        );
        assert_eq!(runtime.torbox.active_listing_flights(), 0, "panicking fetch leaked its flight key");

        let healthy = tb(
            "after-panic",
            TORBOX_API,
            None,
            mock(|_| Ok(resp(200, &format!(r#"{{"success":true,"data":[{{"id":9,"hash":"{H}"}}]}}"#)))),
        );
        let (ids, ok) = healthy.account_listing().await;
        assert!(ok && ids.get(H) == Some(&9), "panic leaked the only admission permit");
    })
    .await;
}

// Not in the Go suite: the parts of the three allocation-measuring decode tests that do not need a counting
// allocator — what the decoder answers, not how many bytes it allocated getting there.

#[test]
fn skip_value_refuses_runaway_nesting_functional_half() {
    use crate::stores::torbox::{decode_listing, ListingFault};

    let deep = format!(r#"{{"success":true,"x":{}"#, "[".repeat(1 << 20));
    let (_, ok, fault) = decode_listing(deep.as_bytes());
    assert!(
        !ok && fault == ListingFault::BadEnvelope,
        "runaway nesting: ok={ok} fault={fault:?}, want false and BadEnvelope"
    );
}

#[test]
fn decode_listing_does_not_retain_the_body_functional_half() {
    use crate::stores::torbox::decode_listing;

    // Two fixtures: bulk in a skipped top-level field, and bulk inside data[].
    let mut skipped = String::from(r#"{"success":true,"unknown_field":["#);
    let mut i = 0;
    while skipped.len() < 24 << 20 {
        if i > 0 {
            skipped.push(',');
        }
        skipped.push_str(&format!(r#"{{"noise":"{}"}}"#, "f".repeat(6000)));
        i += 1;
    }
    skipped.push_str(&format!(r#"],"data":[{{"id":1,"hash":"{}"}}]}}"#, "a".repeat(40)));

    let mut in_data = String::from(r#"{"success":true,"data":["#);
    let mut j = 0;
    while in_data.len() < 24 << 20 {
        if j > 0 {
            in_data.push(',');
        }
        in_data.push_str(&format!(r#"{{"id":{j},"hash":"{j:040x}","files":"{}"}}"#, "f".repeat(6000)));
        j += 1;
    }
    in_data.push_str("]}");

    for (name, body) in [
        ("bulk in a skipped top-level field", skipped.as_str()),
        ("bulk inside the data array", in_data.as_str()),
    ] {
        let (ids, ok, _) = decode_listing(body.as_bytes());
        assert!(ok, "{name}: the fixture did not decode");
        assert!(!ids.is_empty(), "{name}: no entries");
    }
}

#[test]
fn decode_listing_drops_unusable_hashes_functional_half() {
    use crate::play::{is_info_hash, MAX_INFO_HASH_LEN, MIN_INFO_HASH_LEN};
    use crate::scrape::normalize_hash;
    use crate::stores::torbox::{decode_listing, ListingFault};

    // Hashes nothing could ask about are dropped before retention; a body of them still reads as
    // "could not read this", never "the account holds nothing".
    let good = "a".repeat(40);
    let giant = format!(
        r#"{{"success":true,"data":[{{"id":1,"hash":"{}"}},{{"id":2,"hash":"{good}"}}]}}"#,
        "A".repeat(8 << 20)
    );
    let (ids, ok, _) = decode_listing(giant.as_bytes());
    assert!(ok, "one unusable entry must not make the whole listing unreadable");
    assert_eq!(ids.get(&good).copied(), Some(2), "the usable entry was lost");
    assert!(ids.keys().all(|k| k.len() <= 40), "retained a hash longer than any infohash");

    let mut junk = String::from(r#"{"success":true,"data":["#);
    for i in 0..=MAX_LISTING_ENTRIES {
        if i > 0 {
            junk.push(',');
        }
        junk.push_str(r#"{"id":1,"hash":"tooshort"}"#);
    }
    junk.push_str("]}");
    let (_, ok, fault) = decode_listing(junk.as_bytes());
    assert!(
        !ok && fault == ListingFault::TooManyEntries,
        "a listing of more than {MAX_LISTING_ENTRIES} unusable hashes: ok={ok} fault={fault:?}"
    );

    let renamed =
        format!(r#"{{"success":true,"data":[{{"id":1,"infohash":"{good}"}},{{"id":2,"infohash":"x"}}]}}"#);
    let (_, ok, fault) = decode_listing(renamed.as_bytes());
    assert!(
        !ok && fault == ListingFault::NoUsableEntries,
        "a listing whose entries all filtered out: ok={ok} fault={fault:?}"
    );
    let (ids, ok, _) = decode_listing(&br#"{"success":true,"data":[]}"#[..]);
    assert!(ok && ids.is_empty(), "a genuinely empty account must stay an ANSWER");

    // The filter keeps 32 (base32) and 40 (hex) character hashes, and nothing else.
    const { assert!(MIN_INFO_HASH_LEN == 32 && MAX_INFO_HASH_LEN == 40, "infohash bounds") };
    for n in [32usize, 40] {
        assert!(
            normalize_hash(&"b".repeat(n)).is_some(),
            "hashNorm rejects a {n}-char hash the listing keeps"
        );
        let (got, ok, _) = decode_listing(
            format!(r#"{{"success":true,"data":[{{"id":9,"hash":"{}"}}]}}"#, "b".repeat(n)).as_bytes(),
        );
        assert!(
            ok && got.get(&"b".repeat(n)).copied() == Some(9),
            "a {n}-char hash that /play accepts was dropped"
        );
    }
    for n in [31usize, 33, 39, 41, 64] {
        assert!(
            normalize_hash(&"b".repeat(n)).is_none(),
            "hashNorm accepts a {n}-char hash the listing filter drops"
        );
    }

    // The length that decides is the LOWERED one: 32 upper-case Kelvin signs lower into a valid hash.
    let kelvin = "\u{212A}".repeat(32);
    let lowered = kelvin.to_lowercase();
    assert!(is_info_hash(&lowered), "the fixture does not lower into an askable hash");
    let (got, ok, _) =
        decode_listing(format!(r#"{{"success":true,"data":[{{"id":7,"hash":"{kelvin}"}}]}}"#).as_bytes());
    assert!(
        ok && got.get(&lowered).copied() == Some(7),
        "a hash judged on its raw byte length was dropped though it lowers into range"
    );
}
