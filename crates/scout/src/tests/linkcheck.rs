//! Go: `internal/scout/linkcheck_test.go` (353 lines). Work package 6.
//!
//! The Go tests run the check against `httptest` servers. The verdict table below runs it through a mocked
//! `Doer` instead, which sees the same request and answers with the same headers. The `/play` tests drive the
//! handler and the stores, so they wait for packages 1, 2 and 4; they are ignored, and their bodies are ported
//! in full inside a compiled-out item so they only need to parse until the owner enables them.

use async_trait::async_trait;

use super::mock::MockDoer;
use crate::linkcheck::{check_link, LinkVerdict};
use crate::scrape::{DoError, Doer};

/// Builds a response with headers, the way `httptest` would have written them.
fn response(status: u16, headers: &[(&str, &str)], body: &str) -> reqwest::Response {
    let mut b = http::Response::builder().status(status);
    for (k, v) in headers {
        b = b.header(*k, *v);
    }
    reqwest::Response::from(b.body(body.to_owned()).expect("valid response"))
}

/// A `Doer` that never answers: the check's own timeout is the only thing that ends it.
struct NeverAnswers;

#[async_trait]
impl Doer for NeverAnswers {
    async fn do_request(&self, _req: reqwest::Request) -> Result<reqwest::Response, DoError> {
        std::future::pending().await
    }
}

#[tokio::test]
async fn check_link_verdicts() {
    // (name, status, content-type, content-range, want size, want verdict)
    let cases: [(&str, u16, &str, &str, i64, LinkVerdict); 12] = [
        ("206 of the listed size", 206, "video/x-matroska", "bytes 0-0/1000", 1000, LinkVerdict::Playable),
        ("200, the range ignored", 200, "application/octet-stream", "", 1000, LinkVerdict::Playable),
        ("size unknown to the debrid", 206, "video/mp4", "bytes 0-0/5555", 0, LinkVerdict::Playable),
        ("total not stated", 206, "video/mp4", "bytes 0-0/*", 1000, LinkVerdict::Playable),
        ("within the tolerance", 206, "video/mp4", "bytes 0-0/1015", 1000, LinkVerdict::Playable),
        ("a different file", 206, "video/mp4", "bytes 0-0/1100", 1000, LinkVerdict::Broken),
        ("an error page", 200, "text/html; charset=utf-8", "", 0, LinkVerdict::Broken),
        ("a JSON error body", 200, "application/json", "", 0, LinkVerdict::Broken),
        ("not found", 404, "text/plain", "", 0, LinkVerdict::Broken),
        ("forbidden", 403, "text/plain", "", 0, LinkVerdict::Broken),
        ("throttled", 429, "text/plain", "", 0, LinkVerdict::Unverified),
        ("CDN fault", 503, "text/plain", "", 0, LinkVerdict::Unverified),
    ];
    for (name, status, ctype, rng, want_size, want) in cases {
        let doer = MockDoer(move |req: reqwest::Request| {
            assert_eq!(
                req.headers().get("range").and_then(|v| v.to_str().ok()),
                Some("bytes=0-0"),
                "range"
            );
            let mut headers = vec![("content-type", ctype)];
            if !rng.is_empty() {
                headers.push(("content-range", rng));
            }
            Ok(response(status, &headers, "\0"))
        });
        let (got, reason) = check_link(&doer, "http://cdn.test/f.mkv", want_size, 0).await;
        assert_eq!(got, want, "{name}: verdict = {got:?} ({reason}), want {want:?}");
    }
}

// The check failing to get an answer says nothing about the link, so it never condemns one.
#[tokio::test(start_paused = true)]
async fn check_link_its_own_failure_does_not_condemn_the_link() {
    let refused = MockDoer(|_req: reqwest::Request| Err(DoError::from("connection refused")));
    let (got, _) = check_link(&refused, "http://cdn.test/f.mkv", 0, 0).await;
    assert_eq!(got, LinkVerdict::Unverified, "a refused connection gave verdict {got:?}, want unverified");

    // The Go test bounds this with a 50 ms context. check_link owns its own timeout (linkCheckTimeout), and
    // the paused clock lets the test run past it without waiting.
    let (got, _) = check_link(&NeverAnswers, "http://cdn.test/f.mkv", 0, 0).await;
    assert_eq!(got, LinkVerdict::Unverified, "a timed-out check gave verdict {got:?}, want unverified");
}

// Handler- and store-level helpers for the `/play` tests below. They need `handler::serve`, `Deps::for_test`,
// the stores (packages 1, 2 and 4) and a loopback CDN, so they stay compiled out until those land. Names follow
// the assumptions in the coordinator's note; package 4 reconciles them.
#[cfg(any())]
mod pending_handler {
    use super::*;
    use crate::deps::Deps;
    use crate::handler::serve;
    use crate::tests::mock::resp;
    use crate::tests::ticket::{ticket_keyring, ticket_keys};

    /// Go `sizedCDN`: a loopback server that answers the one-byte range with each path's total, or 200 with no
    /// Content-Range for any other path. Returns its base URL.
    pub(super) fn sized_cdn(totals: &[(&str, i64)]) -> String {
        todo!("sizedCDN: a loopback CDN from the package 4 harness")
    }

    /// Go `tbFile`.
    pub(super) struct TbFile {
        pub(super) id: i64,
        pub(super) name: &'static str,
        pub(super) size: i64,
    }

    /// Go `tbTorrent`: answers as TorBox holding torrent 42 with `files`. `mylist` lists them, and `requestdl`
    /// links each file id to `<cdn>/<id>.mkv`.
    pub(super) fn tb_torrent(cdn: String, files: Vec<TbFile>) -> impl Fn(&reqwest::Request) -> reqwest::Response {
        move |req: &reqwest::Request| {
            let last = req.url().path().rsplit('/').next().unwrap_or("");
            match last {
                "requestdl" => {
                    let file_id = req.url().query_pairs().find(|(k, _)| k == "file_id").map(|(_, v)| v.into_owned()).unwrap_or_default();
                    resp(200, &format!(r#"{{"success":true,"data":"{cdn}/{file_id}.mkv"}}"#))
                }
                "mylist" => {
                    let list: Vec<String> = files
                        .iter()
                        .map(|f| format!(r#"{{"id":{},"name":{:?},"size":{}}}"#, f.id, f.name, f.size))
                        .collect();
                    resp(200, &format!(r#"{{"success":true,"data":{{"id":42,"files":[{}]}}}}"#, list.join(",")))
                }
                _ => resp(404, "{}"),
            }
        }
    }

    /// Go `sameCalls`: the upstream calls, in order, equal `want`.
    pub(super) fn same_calls(got: &[String], want: &[&str]) -> bool {
        got.len() == want.len() && got.iter().zip(want).all(|(g, w)| g == w)
    }

    /// Go `heldTorBoxPlay` (defined in stores_test.go, package 1): a `serve` closure over a torrent TorBox
    /// holds for `hash`, recording each upstream call it sees.
    pub(super) fn held_torbox_play(
        hash: &str,
        upstream: impl Fn(&reqwest::Request) -> reqwest::Response + Send + Sync + 'static,
        over: impl FnOnce(&mut Deps),
    ) -> (Box<dyn Fn(&str) -> http::Response<String>>, std::sync::Arc<std::sync::Mutex<Vec<String>>>) {
        todo!("heldTorBoxPlay: stores_test.go helper, package 1 (hash {hash}, {over:?})")
    }

    /// Go `sizedTicketPlay`: a `/play` handler over a held TorBox torrent whose links the check can reach, and
    /// the ticket path for `target`.
    pub(super) fn sized_ticket_play(
        cdn: &str,
        files: Vec<TbFile>,
        target: PlayTarget,
    ) -> (Box<dyn Fn(&str) -> http::Response<String>>, std::sync::Arc<std::sync::Mutex<Vec<String>>>, String) {
        let kr = ticket_keyring();
        let (serve_fn, calls) = held_torbox_play(&target.info_hash, tb_torrent(cdn.to_owned(), files), {
            let kr = kr.clone();
            move |d: &mut Deps| {
                d.probe_client = Some(cdn.to_owned());
                d.seal_keyring = Some(kr);
            }
        });
        let ticket = ticket_keys(&kr).mint(
            &Config { debrid: vec![DebridAccount { service: DebridService::TorBox, token: "tb-secret".to_owned() }], ..Default::default() },
            &target,
            SystemTime::now() + Duration::from_secs(3600),
        );
        (serve_fn, calls, format!("/p/{ticket}"))
    }
}

// Needs `handler`, `/play` and the dead-link route (work package 4), and `tallyStore` (test helper for
// packages 1 and 2).
#[test]
#[ignore = "needs handler, /play and tallyStore (packages 4, 1, 2)"]
fn play_a_link_that_fails_its_check_is_a_dead_link() {
    // Enable when packages 4, 1 and 2 land: move this body up and delete the wrapper.
    #[cfg(any())]
    fn pending() {
        use super::pending_handler::*;
        use crate::deps::Deps;
        use crate::tests::linkmemo::TallyStore;
        let cdn = sized_cdn(&[]);
        let store = TallyStore::new(crate::config::DebridService::TorBox, move |_| Ok(format!("{cdn}/f.mkv")));
        let deps = Deps::for_test(|d| {
            d.probe_client = Some(store.cdn_client());
            d.make_stores = Box::new(move |_| vec![store.clone_box()]);
        });
        let path = format!("/{}/play/{}", valid_blob(), encode_play_token(&PlayTarget { info_hash: "a".repeat(40), file_idx: Some(0), ..Default::default() }));
        for i in 1..=2 {
            let rr = serve(&deps, get_request(&path));
            assert!(rr.status() == 404 && rr.body().contains("dead_link"), "play {i}: {} {}, want 404 dead_link", rr.status(), rr.body());
        }
        assert_eq!(store.resolves(), 2, "resolves: a rejected link must not be served from the memo");
    }
    todo!("pending: linkcheck_test.go:~99 TestPlay_aLinkThatFailsItsCheckIsADeadLink")
}

// Needs `handler`, `/play`, `linkMemo` and `tallyStore` (packages 4, 1, 2).
#[test]
#[ignore = "needs handler, /play, linkMemo and tallyStore (packages 4, 1, 2)"]
fn play_a_check_that_cannot_answer_passes_the_link_through() {
    // Enable when packages 4, 1 and 2 land: move this body up and delete the wrapper.
    #[cfg(any())]
    fn pending() {
        use super::pending_handler::*;
        use crate::deps::Deps;
        use crate::tests::linkmemo::TallyStore;
        let link = "http://127.0.0.1:1/f.mkv".to_owned(); // a port nothing listens on: the check cannot answer
        let store = TallyStore::new(crate::config::DebridService::TorBox, move |_| Ok(link.clone()));
        let deps = Deps::for_test(|d| {
            d.probe_client = Some(crate::scrape::default_client());
            d.make_stores = Box::new(move |_| vec![store.clone_box()]);
        });
        let config = crate::config::decode_config(None, &valid_blob()).unwrap_or_default();
        let target = ResolveTarget { info_hash: "a".repeat(40), file_idx: Some(0), ..Default::default() };
        let path = format!("/{}/play/{}", valid_blob(), encode_play_token(&PlayTarget { info_hash: target.info_hash.clone(), file_idx: target.file_idx, ..Default::default() }));

        let rr = serve(&deps, get_request(&path));
        assert!(rr.status() == 302 && rr.headers().get("location").map(|v| v.as_bytes()) == Some(link.as_bytes()), "play: {} want 302 to the unchecked link", rr.status());
        let hit = deps.links.get(&link_memo_key(&config, &target), SystemTime::now());
        assert!(matches!(&hit, Some(h) if !h.checked), "memo = {hit:?}, want an unchecked entry");
    }
    todo!("pending: linkcheck_test.go:~121 TestPlay_aCheckThatCannotAnswerPassesTheLinkThrough")
}

// Needs `handler`, `/play` and `linkMemo` (packages 4 and 1).
#[test]
#[ignore = "needs handler, /play and linkMemo (packages 4, 1)"]
fn play_a_checked_link_is_not_checked_again() {
    // Enable when packages 4 and 1 land: move this body up and delete the wrapper.
    #[cfg(any())]
    fn pending() {
        use super::pending_handler::*;
        use crate::deps::Deps;
        use crate::tests::linkmemo::TallyStore;
        use std::sync::atomic::{AtomicI32, Ordering};
        let checks = std::sync::Arc::new(AtomicI32::new(0));
        let cdn = sized_cdn(&[("/f.mkv", 1000)]);
        let store = TallyStore::new(crate::config::DebridService::TorBox, move |_| Ok(format!("{cdn}/f.mkv")));
        let deps = Deps::for_test(|d| {
            d.probe_client = Some(crate::scrape::default_client());
            d.make_stores = Box::new(move |_| vec![store.clone_box()]);
        });
        let path = format!("/{}/play/{}", valid_blob(), encode_play_token(&PlayTarget { info_hash: "a".repeat(40), file_idx: Some(0), ..Default::default() }));
        for i in 1..=3 {
            let rr = serve(&deps, get_request(&path));
            assert_eq!(rr.status(), 302, "play {i}");
        }
        assert_eq!(checks.load(Ordering::SeqCst), 1, "link checked {} times, want once", checks.load(Ordering::SeqCst));
    }
    todo!("pending: linkcheck_test.go:~153 TestPlay_aCheckedLinkIsNotCheckedAgain")
}

// Needs the TorBox and Real-Debrid stores' `KnownFileSize`, `rememberFileSize` and `rdTorrentKey` (packages
// 1 and 2).
#[tokio::test]
async fn known_file_size_from_what_the_stores_already_hold() {
        use std::sync::Arc;

        use crate::cache::{Cache, MemoryCache};
        use crate::stores::realdebrid::{rd_torrent_key, RealDebridStore, REAL_DEBRID_API};
        use crate::stores::refusal::resolve_key;
        use crate::stores::torbox::{TorBoxStore, RESOLVE_CACHE_TTL, TORBOX_API};
        use crate::stores::{ResolveTarget, Store};
        use crate::tests::mock::{resp, MockDoer};
        let h = "b".repeat(40);
        let cache = Arc::new(MemoryCache::new(1 << 20));
        let tb = TorBoxStore {
            token: "t".into(),
            client: Arc::new(MockDoer(|_| Ok(resp(404, "{}")))),
            cache: Some(cache.clone()),
            api: TORBOX_API.into(),
        };
        cache.put(&resolve_key("t", &h), r#"{"torrentId":9,"files":[{"Index":0,"Name":"S01E01.mkv","SizeBytes":10},{"Index":1,"Name":"S01E02.mkv","SizeBytes":20}]}"#, RESOLVE_CACHE_TTL);
        assert_eq!(tb.known_file_size(&ResolveTarget { info_hash: h.clone(), season: Some(1), episode: Some(2), ..Default::default() }), Some(20), "torbox size");
        assert_eq!(tb.known_file_size(&ResolveTarget { info_hash: "f".repeat(40), ..Default::default() }), None, "torbox claimed a size for a release it has no entry for");

        let target = ResolveTarget { info_hash: h.clone(), ..Default::default() };
        let rd_cache = Arc::new(MemoryCache::new(1 << 20));
        rd_cache.put(&rd_torrent_key("k", &h, &target), "abc", RESOLVE_CACHE_TTL);
        let rd = RealDebridStore {
            token: "k".into(),
            api: REAL_DEBRID_API.into(),
            cache: Some(rd_cache.clone()),
            client: Arc::new(MockDoer(|req| {
                let path = req.url().path();
                if path.contains("torrents/info") {
                    return Ok(resp(200, r#"{"status":"downloaded","links":["https://rd/l"],"files":[{"id":1,"path":"Movie.mkv","bytes":900,"selected":1}]}"#));
                }
                if path.contains("unrestrict") {
                    return Ok(resp(200, r#"{"download":"https://rd/final.mkv"}"#));
                }
                Ok(resp(404, "{}"))
            })),
        };
        rd.resolve(&target).await.expect("resolve");
        assert_eq!(rd.known_file_size(&target), Some(900), "realdebrid size");
}

// The release size condemns a link only where nothing better is known: a movie, whose file size no store
// has, served at under half the release.
#[test]
#[ignore = "needs handler.verify_link and StorePool (packages 4, 1)"]
fn verify_link_wrong_file_only_for_a_movie_whose_file_size_is_unknown() {
    // Enable when packages 4 and 1 land: move this body up and delete the wrapper.
    #[cfg(any())]
    fn pending() {
        use super::pending_handler::*;
        use crate::cache::{Cache, MemoryCache};
        use crate::deps::Deps;
        use crate::handler::Handler;
        use crate::linkcheck::LinkVerdict;
        use crate::stores::{pool::StorePool, torbox::TorBoxStore, ResolveTarget};
        use std::sync::Arc;
        const H: &str = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
        let cdn = sized_cdn(&[("/sample.mkv", 522)]);
        let h = Handler { deps: Deps::for_test(|d| d.probe_client = Some(crate::scrape::default_client())) };
        let unknown = StorePool::new(vec![Box::new(TorBoxStore::new("t", Arc::new(MemoryCache::new(1 << 20))))]);
        let cache = Arc::new(MemoryCache::new(1 << 20));
        cache.put(&crate::stores::resolve_key("t", H), r#"{"torrentId":9,"files":[{"Index":0,"Name":"Movie.mkv","SizeBytes":522}]}"#, crate::stores::RESOLVE_CACHE_TTL);
        let listed = StorePool::new(vec![Box::new(TorBoxStore::new("t", cache))]);
        const RELEASE: i64 = 16_000_000_000;
        let cases: Vec<(&str, &StorePool, ResolveTarget, &str, LinkVerdict)> = vec![
            ("a movie whose file size is unknown", &unknown, ResolveTarget { info_hash: H.into(), file_idx: Some(0), release_size: RELEASE, ..Default::default() }, "/sample.mkv", LinkVerdict::WrongFile),
            ("no release size", &unknown, ResolveTarget { info_hash: H.into(), file_idx: Some(0), ..Default::default() }, "/sample.mkv", LinkVerdict::Playable),
            ("an episode", &unknown, ResolveTarget { info_hash: H.into(), season: Some(1), episode: Some(2), release_size: RELEASE, ..Default::default() }, "/sample.mkv", LinkVerdict::Playable),
            ("the file size is known", &listed, ResolveTarget { info_hash: H.into(), file_idx: Some(0), release_size: RELEASE, ..Default::default() }, "/sample.mkv", LinkVerdict::Playable),
            ("at least half the release", &unknown, ResolveTarget { info_hash: H.into(), file_idx: Some(0), release_size: 1000, ..Default::default() }, "/sample.mkv", LinkVerdict::Playable),
            ("no total served", &unknown, ResolveTarget { info_hash: H.into(), file_idx: Some(0), release_size: RELEASE, ..Default::default() }, "/whole.mkv", LinkVerdict::Playable),
        ];
        for (name, pool, rt, path, want) in cases {
            let got = h.verify_link(pool, &rt, &format!("{cdn}{path}"));
            assert_eq!(got, want, "{name}: verdict {got:?}, want {want:?}");
        }
    }
    todo!("pending: linkcheck_test.go:~221 TestVerifyLink_wrongFileOnlyForAMovieWhoseFileSizeIsUnknown")
}

// The live failure: TorBox lists a remux's sample first, so the indexer's position 0 sent raw as a file id
// served 522 bytes of a 16 GB release. /play lists the torrent once, serves the feature, and the list it
// kept lets the next mint pick the feature with no list call.
#[test]
#[ignore = "needs /play, the ticket route and held_torbox_play (packages 4, 1)"]
fn play_a_movie_link_to_another_file_is_re_resolved_with_the_file_list() {
    // Enable when packages 4 and 1 land: move this body up and delete the wrapper.
    #[cfg(any())]
    fn pending() {
        use super::pending_handler::*;
        let cdn = sized_cdn(&[("/0.mkv", 522), ("/1.mkv", 16_000_000_000)]);
        let (serve_fn, calls, play) = sized_ticket_play(
            &cdn,
            vec![
                TbFile { id: 0, name: "Movie/Sample/sample.mkv", size: 522 },
                TbFile { id: 1, name: "Movie/Movie.2160p.REMUX.mkv", size: 16_000_000_000 },
            ],
            PlayTarget { info_hash: "a".repeat(40), file_idx: Some(0), release_size: 16_000_000_000, ..Default::default() },
        );
        let rr = serve_fn(&play);
        assert!(rr.status() == 302 && rr.headers()["location"] == format!("{cdn}/1.mkv"), "play: {} want 302 to the feature", rr.status());
        assert!(same_calls(&calls.lock().unwrap(), &["requestdl", "mylist", "requestdl"]), "upstream calls = {:?}", calls.lock().unwrap());

        calls.lock().unwrap().clear();
        let rr = serve_fn(&format!("{play}?fresh=1"));
        assert!(rr.status() == 302 && rr.headers()["location"] == format!("{cdn}/1.mkv"), "second play: {} want 302 to the feature", rr.status());
        assert!(same_calls(&calls.lock().unwrap(), &["requestdl"]), "second play calls = {:?} — the list should have been kept", calls.lock().unwrap());
    }
    todo!("pending: linkcheck_test.go:~260 TestPlay_aMovieLinkToAnotherFileIsReResolvedWithTheFileList")
}

// A link of the release's size is the normal case, and costs the one link request it always did.
#[test]
#[ignore = "needs /play, the ticket route and held_torbox_play (packages 4, 1)"]
fn play_a_movie_link_of_the_release_size_costs_nothing_more() {
    // Enable when packages 4 and 1 land: move this body up and delete the wrapper.
    #[cfg(any())]
    fn pending() {
        use super::pending_handler::*;
        let cdn = sized_cdn(&[("/0.mkv", 16_000_000_000)]);
        let (serve_fn, calls, play) = sized_ticket_play(
            &cdn,
            vec![],
            PlayTarget { info_hash: "a".repeat(40), file_idx: Some(0), release_size: 16_000_000_000, ..Default::default() },
        );
        let rr = serve_fn(&play);
        assert!(rr.status() == 302 && rr.headers()["location"] == format!("{cdn}/0.mkv"), "play: {}", rr.status());
        assert!(same_calls(&calls.lock().unwrap(), &["requestdl"]), "upstream calls = {:?}", calls.lock().unwrap());
    }
    todo!("pending: linkcheck_test.go:~291 TestPlay_aMovieLinkOfTheReleaseSizeCostsNothingMore")
}

// A collection's film is smaller than half the pack an indexer may report. The first check cannot tell that
// from a wrong file; the list can, and the film plays against its own listed size.
#[test]
#[ignore = "needs /play, the ticket route and held_torbox_play (packages 4, 1)"]
fn play_a_collection_film_smaller_than_half_the_release_still_plays() {
    // Enable when packages 4 and 1 land: move this body up and delete the wrapper.
    #[cfg(any())]
    fn pending() {
        use super::pending_handler::*;
        let cdn = sized_cdn(&[("/0.mkv", 9_000_000_000), ("/1.mkv", 10_000_000_000), ("/2.mkv", 11_000_000_000)]);
        let (serve_fn, calls, play) = sized_ticket_play(
            &cdn,
            vec![
                TbFile { id: 0, name: "Trilogy/Part.1.mkv", size: 9_000_000_000 },
                TbFile { id: 1, name: "Trilogy/Part.2.mkv", size: 10_000_000_000 },
                TbFile { id: 2, name: "Trilogy/Part.3.mkv", size: 11_000_000_000 },
            ],
            PlayTarget { info_hash: "a".repeat(40), file_idx: Some(0), release_size: 30_000_000_000, ..Default::default() },
        );
        let rr = serve_fn(&play);
        assert!(rr.status() == 302 && rr.headers()["location"] == format!("{cdn}/0.mkv"), "play: {} want 302 to part 1", rr.status());
        assert!(same_calls(&calls.lock().unwrap(), &["requestdl", "mylist", "requestdl"]), "upstream calls = {:?}", calls.lock().unwrap());
    }
    todo!("pending: linkcheck_test.go:~308 TestPlay_aCollectionFilmSmallerThanHalfTheReleaseStillPlays")
}

// A re-resolve that cannot list serves the first link, as /play did before the release size was known: the
// size alone does not prove the file wrong (a collection's film, a store with no list), so it is not refused.
#[test]
#[ignore = "needs /play, the ticket route and held_torbox_play (packages 4, 1)"]
fn play_a_wrong_file_whose_re_resolve_fails_serves_the_first_link() {
    // Enable when packages 4 and 1 land: move this body up and delete the wrapper.
    #[cfg(any())]
    fn pending() {
        use super::pending_handler::*;
        let cdn = sized_cdn(&[("/0.mkv", 522)]);
        let hash = "a".repeat(40);
        let listing = tb_torrent(cdn.clone(), vec![]);
        let (serve_fn, calls) = held_torbox_play(
            &hash,
            move |req: &reqwest::Request| {
                if req.url().path().ends_with("mylist") {
                    return resp(500, "boom");
                }
                listing(req)
            },
            {
                let kr = ticket_keyring();
                move |d: &mut Deps| {
                    d.probe_client = Some(cdn.clone());
                    d.seal_keyring = Some(kr.clone());
                }
            },
        );
        let play = format!(
            "/p/{}",
            ticket_keys(&ticket_keyring()).mint(
                &Config { debrid: vec![DebridAccount { service: DebridService::TorBox, token: "tb-secret".to_owned() }], ..Default::default() },
                &PlayTarget { info_hash: hash.clone(), file_idx: Some(0), release_size: 16_000_000_000, ..Default::default() },
                SystemTime::now() + Duration::from_secs(3600),
            )
        );
        let rr = serve_fn(&play);
        assert!(rr.status() == 302 && rr.headers()["location"] == format!("{cdn}/0.mkv"), "play: {} want 302 to the first link", rr.status());
        assert!(same_calls(&calls.lock().unwrap(), &["requestdl", "mylist"]), "upstream calls = {:?}", calls.lock().unwrap());
    }
    todo!("pending: linkcheck_test.go:~343 TestPlay_aWrongFileWhoseReResolveFailsServesTheFirstLink")
}
