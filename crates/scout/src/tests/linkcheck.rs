//! Go: `internal/scout/linkcheck_test.go` (353 lines). Work package 6.
//!
//! The Go tests run the check against `httptest` servers. The verdict table below runs it through a mocked
//! `Doer` instead, which sees the same request and answers with the same headers. The `/play` tests drive the
//! handler and the stores with that mock set as `Deps::probe_doer`, where Go sets `Deps.ProbeClient` to the
//! `httptest` server's client.

use std::sync::atomic::{AtomicI32, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use async_trait::async_trait;

use super::handler::{body_str, get, hdr, shared_stores, test_deps, valid_blob};
use super::linkmemo::TallyStore;
use super::mock::{resp, MockDoer};
use super::playwarm::{held_torbox_play, same_calls};
use super::ticket::{ticket_keyring, ticket_keys};
use crate::cache::{Cache, MemoryCache};
use crate::config::{decode_config, Config, DebridAccount, DebridService};
use crate::handler::new_handler;
use crate::linkcheck::{check_link, LinkVerdict};
use crate::linkmemo::link_memo_key;
use crate::play::{encode_play_token, PlayTarget};
use crate::scrape::{DoError, Doer};
use crate::stores::pool::StorePool;
use crate::stores::refusal::resolve_key;
use crate::stores::torbox::{TorBoxStore, RESOLVE_CACHE_TTL, TORBOX_API};
use crate::stores::{ResolveTarget, Store};

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
            assert_eq!(req.headers().get("range").and_then(|v| v.to_str().ok()), Some("bytes=0-0"), "range");
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

// Not in the Go suite. Go's `http.NewRequest` refuses only what `url.Parse` refuses (a control byte, a bad escape, a
// space in the host); a link it takes but cannot dial is a failure of the check, which never condemns the link.
#[tokio::test]
async fn check_link_condemns_only_what_go_cannot_parse() {
    let unreached = MockDoer(|_req: reqwest::Request| -> Result<reqwest::Response, DoError> {
        panic!("no request expected")
    });
    for (link, want) in [
        ("/relative/f.mkv", LinkVerdict::Unverified),
        ("ftp-less-host/f.mkv", LinkVerdict::Unverified),
        ("http://cdn.test:99999/f.mkv", LinkVerdict::Unverified),
        ("http://cdn.test/f\u{7f}.mkv", LinkVerdict::Broken),
        ("http://cdn test/f.mkv", LinkVerdict::Broken),
        ("http://cdn.test/%zz", LinkVerdict::Broken),
        ("://cdn.test/f.mkv", LinkVerdict::Broken),
    ] {
        let (got, reason) = check_link(&unreached, link, 0, 0).await;
        assert_eq!(got, want, "{link:?}: {got:?} ({reason})");
    }
}

/// Go's `Deps.ProbeClient = srv.Client()` against an `httptest` server: the mock the link check is asked through
/// (`Deps::probe_doer`), answering every request with `reply`.
fn probe_doer(
    reply: impl Fn(&reqwest::Request) -> Result<reqwest::Response, DoError> + Send + Sync + 'static,
) -> Arc<dyn Doer> {
    Arc::new(MockDoer(move |req: reqwest::Request| reply(&req)))
}

fn play_path() -> String {
    format!(
        "/{}/play/{}",
        valid_blob(),
        encode_play_token(&PlayTarget { info_hash: "a".repeat(40), file_idx: Some(0), ..Default::default() })
    )
}

fn tally_store(link: &str) -> Arc<TallyStore> {
    let link = link.to_owned();
    Arc::new(TallyStore {
        svc: DebridService::TorBox,
        resolve: Box::new(move |_| Ok(link.clone())),
        resolves: AtomicI32::new(0),
        statuses: AtomicI32::new(0),
        checks: AtomicI32::new(0),
    })
}

// A minted link that fails its check is a dead link to the client, and is not remembered.
#[tokio::test]
async fn play_a_link_that_fails_its_check_is_a_dead_link() {
    let doer = probe_doer(|_| Ok(response(200, &[("content-type", "text/html")], "<html>error</html>")));
    let store = tally_store("http://cdn.test/f.mkv");
    let shared: Arc<dyn Store> = store.clone();
    let h = new_handler(test_deps(move |d| {
        d.probe_doer = Some(doer);
        d.make_stores = shared_stores(shared.clone());
    }));
    let path = play_path();

    for i in 1..=2 {
        let rr = get(&h, &path, &[]).await;
        assert!(
            rr.status() == 404 && body_str(&rr).contains("dead_link"),
            "play {i}: {} {}, want 404 dead_link",
            rr.status(),
            body_str(&rr)
        );
    }
    let got = store.resolves.load(Ordering::SeqCst);
    assert_eq!(got, 2, "resolves = {got}, want 2 — a rejected link must not be served from the memo");
}

// A check that cannot answer lets the link through, and leaves it to be checked again on the next hit.
#[tokio::test]
async fn play_a_check_that_cannot_answer_passes_the_link_through() {
    // Go points the link at a port nothing listens on; the check's transport failing is what matters.
    let link = "http://127.0.0.1:1/f.mkv".to_owned();
    let doer = probe_doer(|_| Err(DoError::from("connection refused")));
    let store = tally_store(&link);
    let shared: Arc<dyn Store> = store.clone();
    let h = new_handler(test_deps(move |d| {
        d.probe_doer = Some(doer);
        d.make_stores = shared_stores(shared.clone());
    }));
    let config = decode_config(None, &valid_blob()).unwrap_or_default();
    let target = ResolveTarget { info_hash: "a".repeat(40), file_idx: Some(0), ..Default::default() };

    let rr = get(&h, &play_path(), &[]).await;
    assert!(
        rr.status() == 302 && hdr(&rr, "location") == link,
        "play: {} {:?}, want 302 to the unchecked link",
        rr.status(),
        hdr(&rr, "location")
    );
    let hit = h.links().get(&link_memo_key(&config, &target), SystemTime::now());
    assert!(matches!(&hit, Some(hit) if !hit.checked), "memo = {hit:?}, want an unchecked entry");
}

// A link that passed its check once is not checked again on a memo hit.
#[tokio::test]
async fn play_a_checked_link_is_not_checked_again() {
    let checks = Arc::new(AtomicI32::new(0));
    let counter = checks.clone();
    let doer = probe_doer(move |_| {
        counter.fetch_add(1, Ordering::SeqCst);
        Ok(response(206, &[("content-type", "video/x-matroska"), ("content-range", "bytes 0-0/1000")], "\0"))
    });
    let store = tally_store("http://cdn.test/f.mkv");
    let shared: Arc<dyn Store> = store.clone();
    let h = new_handler(test_deps(move |d| {
        d.probe_doer = Some(doer);
        d.make_stores = shared_stores(shared.clone());
    }));
    let path = play_path();

    for i in 1..=3 {
        let rr = get(&h, &path, &[]).await;
        assert_eq!(rr.status(), 302, "play {i}: {}", rr.status());
    }
    let got = checks.load(Ordering::SeqCst);
    assert_eq!(got, 1, "link checked {got} times, want once");
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
    assert_eq!(
        tb.known_file_size(&ResolveTarget {
            info_hash: h.clone(),
            season: Some(1),
            episode: Some(2),
            ..Default::default()
        }),
        Some(20),
        "torbox size"
    );
    assert_eq!(
        tb.known_file_size(&ResolveTarget { info_hash: "f".repeat(40), ..Default::default() }),
        None,
        "torbox claimed a size for a release it has no entry for"
    );

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
                return Ok(resp(
                    200,
                    r#"{"status":"downloaded","links":["https://rd/l"],"files":[{"id":1,"path":"Movie.mkv","bytes":900,"selected":1}]}"#,
                ));
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

/// Go `sizedCDN`: serves each path in `totals` as a file of that many bytes, answering the one-byte range the link
/// check asks for. Any other path answers 200 with no Content-Range. The CDN is a mock behind `Deps::probe_doer`
/// (Go starts an `httptest` server), so its base URL is a name nothing resolves.
const CDN: &str = "http://cdn.test";

fn sized_cdn(totals: &[(&'static str, i64)]) -> Arc<dyn Doer> {
    let totals: Vec<(&'static str, i64)> = totals.to_vec();
    probe_doer(move |req| match totals.iter().find(|(path, _)| *path == req.url().path()) {
        Some((_, n)) => Ok(response(
            206,
            &[("content-type", "video/x-matroska"), ("content-range", &format!("bytes 0-0/{n}"))],
            "\0",
        )),
        None => Ok(response(200, &[("content-type", "video/x-matroska")], "\0")),
    })
}

// The release size condemns a link only where nothing better is known: a movie, whose file size no store
// has, served at under half the release.
#[tokio::test]
async fn verify_link_wrong_file_only_for_a_movie_whose_file_size_is_unknown() {
    const H: &str = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
    const RELEASE: i64 = 16_000_000_000;
    let cdn = sized_cdn(&[("/sample.mkv", 522)]);
    let h = new_handler(test_deps(move |d| d.probe_doer = Some(cdn)));
    let torbox = |cache: Arc<MemoryCache>| -> Box<dyn Store> {
        Box::new(TorBoxStore {
            token: "t".to_owned(),
            cache: Some(cache),
            api: TORBOX_API.to_owned(),
            client: Arc::new(MockDoer(|_| Ok(resp(404, "{}")))),
        })
    };
    let unknown = StorePool { stores: vec![torbox(Arc::new(MemoryCache::new(1 << 20)))] };
    let cache = Arc::new(MemoryCache::new(1 << 20));
    cache.put(
        &resolve_key("t", H),
        r#"{"torrentId":9,"files":[{"Index":0,"Name":"Movie.mkv","SizeBytes":522}]}"#,
        RESOLVE_CACHE_TTL,
    );
    let listed = StorePool { stores: vec![torbox(cache)] };
    let target = |release_size: i64| ResolveTarget {
        info_hash: H.into(),
        file_idx: Some(0),
        release_size,
        ..Default::default()
    };
    let cases: Vec<(&str, &StorePool, ResolveTarget, &str, LinkVerdict)> = vec![
        (
            "a movie whose file size is unknown",
            &unknown,
            target(RELEASE),
            "/sample.mkv",
            LinkVerdict::WrongFile,
        ),
        ("no release size", &unknown, target(0), "/sample.mkv", LinkVerdict::Playable),
        (
            "an episode",
            &unknown,
            ResolveTarget {
                info_hash: H.into(),
                season: Some(1),
                episode: Some(2),
                release_size: RELEASE,
                ..Default::default()
            },
            "/sample.mkv",
            LinkVerdict::Playable,
        ),
        ("the file size is known", &listed, target(RELEASE), "/sample.mkv", LinkVerdict::Playable),
        ("at least half the release", &unknown, target(1000), "/sample.mkv", LinkVerdict::Playable),
        ("no total served", &unknown, target(RELEASE), "/whole.mkv", LinkVerdict::Playable),
    ];
    for (name, pool, rt, path, want) in cases {
        let got = h.verify_link(pool, &rt, &format!("{CDN}{path}")).await;
        assert_eq!(got, want, "{name}: verdict {got:?}, want {want:?}");
    }
}

/// Go `tbFile`.
struct TbFile {
    id: i64,
    name: &'static str,
    size: i64,
}

/// Go `tbTorrent`: answers as TorBox holding torrent 42 with `files`. `mylist` lists them, and `requestdl` links each
/// file id to `<cdn>/<id>.mkv`.
fn tb_torrent(files: Vec<TbFile>) -> impl Fn(&reqwest::Request) -> reqwest::Response + Send + Sync + 'static {
    move |req: &reqwest::Request| match req.url().path().rsplit('/').next().unwrap_or("") {
        "requestdl" => {
            let file_id = req
                .url()
                .query_pairs()
                .find(|(k, _)| k == "file_id")
                .map(|(_, v)| v.into_owned())
                .unwrap_or_default();
            resp(200, &format!(r#"{{"success":true,"data":"{CDN}/{file_id}.mkv"}}"#))
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

fn ticket_for(kr: &crate::seal::SealKeyring, target: &PlayTarget) -> String {
    format!(
        "/p/{}",
        ticket_keys(kr).mint(
            &Config {
                debrid: vec![DebridAccount { service: DebridService::TorBox, token: "tb-secret".to_owned() }],
                ..Default::default()
            },
            target,
            SystemTime::now() + Duration::from_secs(3600),
        )
    )
}

/// Go `sizedTicketPlay`: a /play handler over a held TorBox torrent whose links the check can reach, and the ticket
/// path for `target`.
fn sized_ticket_play(
    cdn: Arc<dyn Doer>,
    files: Vec<TbFile>,
    target: PlayTarget,
) -> (crate::handler::Handler, Arc<std::sync::Mutex<Vec<String>>>, String) {
    let kr = ticket_keyring();
    let (h, calls) = held_torbox_play(&target.info_hash, tb_torrent(files), {
        let seal = Arc::new(ticket_keyring());
        move |d| {
            d.probe_doer = Some(cdn);
            d.seal_keyring = Some(seal);
        }
    });
    let play = ticket_for(&kr, &target);
    (h, calls, play)
}

// The live failure: TorBox lists a remux's sample first, so the indexer's position 0 sent raw as a file id
// served 522 bytes of a 16 GB release. /play lists the torrent once, serves the feature, and the list it
// kept lets the next mint pick the feature with no list call.
#[tokio::test]
async fn play_a_movie_link_to_another_file_is_re_resolved_with_the_file_list() {
    let cdn = sized_cdn(&[("/0.mkv", 522), ("/1.mkv", 16_000_000_000)]);
    let (h, calls, play) = sized_ticket_play(
        cdn,
        vec![
            TbFile { id: 0, name: "Movie/Sample/sample.mkv", size: 522 },
            TbFile { id: 1, name: "Movie/Movie.2160p.REMUX.mkv", size: 16_000_000_000 },
        ],
        PlayTarget {
            info_hash: "a".repeat(40),
            file_idx: Some(0),
            release_size: 16_000_000_000,
            ..Default::default()
        },
    );

    let rr = get(&h, &play, &[]).await;
    assert!(
        rr.status() == 302 && hdr(&rr, "location") == format!("{CDN}/1.mkv"),
        "play: {} {:?}, want 302 to the feature",
        rr.status(),
        hdr(&rr, "location")
    );
    let got = calls.lock().unwrap().clone();
    assert!(
        same_calls(&got, &["requestdl", "mylist", "requestdl"]),
        "upstream calls = {got:?}, want [requestdl, mylist, requestdl]"
    );

    calls.lock().unwrap().clear();
    let rr = get(&h, &format!("{play}?fresh=1"), &[]).await;
    assert!(
        rr.status() == 302 && hdr(&rr, "location") == format!("{CDN}/1.mkv"),
        "second play: {} {:?}, want 302 to the feature",
        rr.status(),
        hdr(&rr, "location")
    );
    let got = calls.lock().unwrap().clone();
    assert!(
        same_calls(&got, &["requestdl"]),
        "second play calls = {got:?}, want [requestdl] — the list should have been kept"
    );
}

// A link of the release's size is the normal case, and costs the one link request it always did.
#[tokio::test]
async fn play_a_movie_link_of_the_release_size_costs_nothing_more() {
    let cdn = sized_cdn(&[("/0.mkv", 16_000_000_000)]);
    let (h, calls, play) = sized_ticket_play(
        cdn,
        vec![],
        PlayTarget {
            info_hash: "a".repeat(40),
            file_idx: Some(0),
            release_size: 16_000_000_000,
            ..Default::default()
        },
    );
    let rr = get(&h, &play, &[]).await;
    assert!(
        rr.status() == 302 && hdr(&rr, "location") == format!("{CDN}/0.mkv"),
        "play: {} {:?}",
        rr.status(),
        hdr(&rr, "location")
    );
    let got = calls.lock().unwrap().clone();
    assert!(same_calls(&got, &["requestdl"]), "upstream calls = {got:?}, want [requestdl]");
}

// A collection's film is smaller than half the pack an indexer may report. The first check cannot tell that
// from a wrong file; the list can, and the film plays against its own listed size.
#[tokio::test]
async fn play_a_collection_film_smaller_than_half_the_release_still_plays() {
    let cdn = sized_cdn(&[("/0.mkv", 9_000_000_000), ("/1.mkv", 10_000_000_000), ("/2.mkv", 11_000_000_000)]);
    let (h, calls, play) = sized_ticket_play(
        cdn,
        vec![
            TbFile { id: 0, name: "Trilogy/Part.1.mkv", size: 9_000_000_000 },
            TbFile { id: 1, name: "Trilogy/Part.2.mkv", size: 10_000_000_000 },
            TbFile { id: 2, name: "Trilogy/Part.3.mkv", size: 11_000_000_000 },
        ],
        PlayTarget {
            info_hash: "a".repeat(40),
            file_idx: Some(0),
            release_size: 30_000_000_000,
            ..Default::default()
        },
    );
    let rr = get(&h, &play, &[]).await;
    assert!(
        rr.status() == 302 && hdr(&rr, "location") == format!("{CDN}/0.mkv"),
        "play: {} {:?}, want 302 to part 1",
        rr.status(),
        hdr(&rr, "location")
    );
    let got = calls.lock().unwrap().clone();
    assert!(
        same_calls(&got, &["requestdl", "mylist", "requestdl"]),
        "upstream calls = {got:?}, want [requestdl, mylist, requestdl]"
    );
}

// A re-resolve that cannot list serves the first link, as /play did before the release size was known: the
// size alone does not prove the file wrong (a collection's film, a store with no list), so it is not refused.
#[tokio::test]
async fn play_a_wrong_file_whose_re_resolve_fails_serves_the_first_link() {
    let cdn = sized_cdn(&[("/0.mkv", 522)]);
    let hash = "a".repeat(40);
    let kr = ticket_keyring();
    let listing = tb_torrent(vec![]);
    let (h, calls) = held_torbox_play(
        &hash,
        move |req| {
            if req.url().path().rsplit('/').next() == Some("mylist") {
                return resp(500, "boom");
            }
            listing(req)
        },
        {
            let seal = Arc::new(ticket_keyring());
            move |d| {
                d.probe_doer = Some(cdn);
                d.seal_keyring = Some(seal);
            }
        },
    );
    let play = ticket_for(
        &kr,
        &PlayTarget {
            info_hash: hash,
            file_idx: Some(0),
            release_size: 16_000_000_000,
            ..Default::default()
        },
    );

    let rr = get(&h, &play, &[]).await;
    assert!(
        rr.status() == 302 && hdr(&rr, "location") == format!("{CDN}/0.mkv"),
        "play: {} {:?}, want 302 to the first link",
        rr.status(),
        hdr(&rr, "location")
    );
    let got = calls.lock().unwrap().clone();
    assert!(same_calls(&got, &["requestdl", "mylist"]), "upstream calls = {got:?}, want [requestdl, mylist]");
}
