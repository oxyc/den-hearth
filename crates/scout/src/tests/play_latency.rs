//! The first play of an episode against a TorBox that takes a realistic time per call.
//!
//! Each call to the fake answers after `LATENCY`, as TorBox's API does from the box (hundreds of milliseconds, not
//! the microseconds an in-process fake takes), so the time a play costs is the number of calls it makes one after
//! another. What each scenario asserts is the calls themselves, in order; the wall time is printed for the record
//! (`cargo test -p den-scout play_latency -- --nocapture`) and only bounded below, which a sleep guarantees.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, Instant};

use async_trait::async_trait;

use crate::cache::{Cache, MemoryCache};
use crate::config::DebridService;
use crate::handler::new_handler;
use crate::loglimit::Capture;
use crate::play::{encode_play_token, PlayTarget};
use crate::rank::RawStream;
use crate::runtime::{scope, ScoutRuntime};
use crate::scrape::{DoError, Doer};
use crate::stores::refusal::{remember_cached, torrent_id_key};
use crate::stores::torbox::{age_listing_memo, listing_memo_age, TorBoxStore, RESOLVE_CACHE_TTL, TORBOX_API};
use crate::stores::Store;
use crate::tests::handler::{fake_scraper, get, hdr, shared_stores, test_deps, valid_blob};

/// What one TorBox (or CDN) call takes. TorBox answers small reads in roughly this from the box; createtorrent and
/// a large account listing take longer, so the real gap is wider than the one measured here.
const LATENCY: Duration = Duration::from_millis(150);

const PACK: &str = "e83a1825cfc4000000000000000000000000beef";
const ID: i64 = 42;

/// A TorBox account and its CDN, answering after `LATENCY`. `held` is whether the account's listing names the pack,
/// `queued` whether TorBox's own queue does (so createtorrent answers "already queued").
struct Upstream {
    held: bool,
    queued: bool,
    calls: Mutex<Vec<String>>,
}

impl Upstream {
    fn calls(&self) -> Vec<String> {
        self.calls.lock().unwrap_or_else(PoisonError::into_inner).clone()
    }

    fn answer(&self, req: &reqwest::Request) -> (String, http::Response<String>) {
        let url = req.url();
        let path = url.path();
        let by_id = url.query_pairs().any(|(k, _)| k == "id");
        let ok = |body: String| http::Response::builder().status(200).body(body).expect("a response");
        let name = |n: &str| n.to_owned();
        if url.host_str() == Some("cdn.example") {
            let r = http::Response::builder()
                .status(206)
                .header("content-range", "bytes 0-0/1000")
                .body("x".to_owned())
                .expect("a response");
            return (name("cdn"), r);
        }
        if path.ends_with("/torrents/mylist") && by_id {
            let files = r#"[{"id":0,"name":"Show.S01E01.1080p.mkv","size":1000},{"id":1,"name":"Show.S01E02.1080p.mkv","size":1000}]"#;
            let body = format!(
                r#"{{"success":true,"data":{{"id":{ID},"hash":"{PACK}","progress":1,"download_finished":true,"download_state":"cached","files":{files}}}}}"#
            );
            return (name("mylist_id"), ok(body));
        }
        if path.ends_with("/torrents/mylist") {
            let entry = if self.held { PACK } else { "1111111111111111111111111111111111111111" };
            let other = if self.held { 7 } else { 8 };
            let body = format!(
                r#"{{"success":true,"data":[{{"id":{},"hash":"{entry}"}},{{"id":{other},"hash":"2222222222222222222222222222222222222222"}}]}}"#,
                if self.held { ID } else { 9 }
            );
            return (name("mylist_account"), ok(body));
        }
        if path.ends_with("/queued/getqueued") {
            let data = if self.queued { format!(r#"[{{"id":5,"hash":"{PACK}"}}]"#) } else { "[]".to_owned() };
            return (name("getqueued"), ok(format!(r#"{{"success":true,"data":{data}}}"#)));
        }
        if path.ends_with("/torrents/createtorrent") && self.queued {
            let body = r#"{"success":false,"error":"DIFF_ISSUE","detail":"Download already queued."}"#;
            let r = http::Response::builder().status(400).body(body.to_owned()).expect("a response");
            return (name("createtorrent"), r);
        }
        if path.ends_with("/torrents/createtorrent") {
            return (
                name("createtorrent"),
                ok(format!(r#"{{"success":true,"data":{{"torrent_id":{ID}}}}}"#)),
            );
        }
        if path.ends_with("/torrents/requestdl") {
            let fid = url.query_pairs().find(|(k, _)| k == "file_id").map(|(_, v)| v.into_owned());
            let link = format!("https://cdn.example/{}.mkv", fid.unwrap_or_default());
            return (name("requestdl"), ok(format!(r#"{{"success":true,"data":"{link}"}}"#)));
        }
        (
            format!("unexpected {path}"),
            http::Response::builder().status(404).body("{}".to_owned()).expect("404"),
        )
    }
}

struct Slow(Arc<Upstream>);

#[async_trait]
impl Doer for Slow {
    async fn do_request(&self, req: reqwest::Request) -> Result<reqwest::Response, DoError> {
        let (name, resp) = self.0.answer(&req);
        tokio::time::sleep(LATENCY).await;
        self.0.calls.lock().unwrap_or_else(PoisonError::into_inner).push(name);
        Ok(reqwest::Response::from(resp))
    }
}

fn play_path() -> String {
    format!(
        "/{}/play/{}",
        valid_blob(),
        encode_play_token(&PlayTarget {
            info_hash: PACK.to_owned(),
            file_idx: Some(1),
            season: Some(1),
            episode: Some(2),
            title: "Show.S01.1080p.WEB-DL".to_owned(),
            imdb: "tt0000001".to_owned(),
            ..Default::default()
        })
    )
}

/// How the account stands before the play.
#[derive(Clone, Copy, Debug)]
enum Before {
    /// Cached at TorBox (the stream list's check said so a moment ago), not in the account.
    CachedNotHeld,
    /// In the account, finished, but Scout no longer remembers its id (past the six-hour memo, or a restart).
    HeldForgotten,
    /// In the account and remembered.
    HeldRemembered,
    /// Reported cached, yet waiting in TorBox's own queue: a stale cache answer, or an add parked by hand.
    CachedButQueued,
}

/// What came before the play on this account.
#[derive(Clone, Copy, Debug)]
enum Lead {
    /// Nothing: the play is the first thing to ask TorBox about the account.
    Cold,
    /// The episode's stream list was built a moment ago.
    StreamList,
    /// The stream list was built minutes ago, so the listing it read is past its fifteen seconds.
    StreamListMinutesAgo,
}

struct Played {
    status: u16,
    location: String,
    calls: Vec<String>,
    took: Duration,
    log: String,
}

async fn play(before: Before) -> Played {
    play_after(before, Lead::Cold).await
}

async fn play_after(before: Before, lead: Lead) -> Played {
    let upstream = Arc::new(Upstream {
        held: matches!(before, Before::HeldForgotten | Before::HeldRemembered),
        queued: matches!(before, Before::CachedButQueued),
        calls: Mutex::new(Vec::new()),
    });
    let cache = Arc::new(MemoryCache::new(1 << 20));
    let store = Arc::new(TorBoxStore {
        token: "tb-secret".to_owned(),
        cache: Some(cache.clone()),
        api: TORBOX_API.to_owned(),
        client: Arc::new(Slow(upstream.clone())),
    });
    let identity = store.account_identity();
    // What the stream list's checkcached left behind for this release.
    remember_cached(&*cache, DebridService::TorBox, &identity, &HashMap::from([(PACK.to_owned(), true)]));
    if matches!(before, Before::HeldRemembered) {
        cache.put(&torrent_id_key(&identity, PACK), &ID.to_string(), RESOLVE_CACHE_TTL);
    }
    let runtime = ScoutRuntime::new();
    let shared: Arc<dyn Store> = store.clone();
    let (deps_cache, cdn, deps_runtime) = (cache.clone(), upstream.clone(), runtime.clone());
    let h = new_handler(test_deps(move |d| {
        d.cache = deps_cache.clone();
        d.runtime = deps_runtime.clone();
        d.make_stores = shared_stores(shared.clone());
        d.probe_doer = Some(Arc::new(Slow(cdn.clone())));
        d.make_scrapers = Arc::new(|_| {
            vec![fake_scraper("torrentio", || {
                Box::pin(async {
                    Ok(vec![RawStream {
                        info_hash: PACK.to_owned(),
                        title: "Show.S01.1080p.WEB-DL".to_owned(),
                        seeders: Some(10),
                        ..RawStream::default()
                    }])
                })
            })]
        });
    }));
    let shared_cache: Arc<dyn Cache> = cache.clone();
    if !matches!(lead, Lead::Cold) {
        let list = get(&h, &format!("/{}/stream/series/tt0000001:1:2.json", valid_blob()), &[]).await;
        assert_eq!(list.status(), 200, "the stream list");
        // The list does not wait for the listing; the play that follows a list does not either, but here it has
        // to have landed for the play to show what it saves.
        let waited = Instant::now();
        while scope(runtime.clone(), async { listing_memo_age(&shared_cache, &identity) }).await.is_none() {
            assert!(
                waited.elapsed() < Duration::from_secs(5),
                "the stream list never read the account listing"
            );
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        assert_eq!(upstream.calls(), ["mylist_account"], "what the stream list asked TorBox");
        upstream.calls.lock().unwrap_or_else(PoisonError::into_inner).clear();
    }
    if matches!(lead, Lead::StreamListMinutesAgo) {
        scope(runtime.clone(), async {
            age_listing_memo(&shared_cache, &identity, Duration::from_secs(5 * 60))
        })
        .await;
    }
    let log = Capture::start();
    let start = Instant::now();
    let resp = get(&h, &play_path(), &[]).await;
    let took = start.elapsed();
    if matches!(lead, Lead::StreamListMinutesAgo) {
        // The old listing answered the play and a fresh one was asked for behind it.
        let waited = Instant::now();
        while scope(runtime.clone(), async { listing_memo_age(&shared_cache, &identity) })
            .await
            .is_none_or(|age| age >= Duration::from_secs(60))
        {
            assert!(waited.elapsed() < Duration::from_secs(5), "the old listing was never refreshed");
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }
    Played {
        status: resp.status().as_u16(),
        location: hdr(&resp, "location"),
        calls: upstream.calls(),
        took,
        log: log.text(),
    }
}

fn report(before: Before, p: &Played) {
    eprintln!(
        "play_latency {before:?}: {} in {} ms over {} calls: {}",
        p.status,
        p.took.as_millis(),
        p.calls.len(),
        p.calls.join(" → ")
    );
}

fn assert_calls(before: Before, p: &Played, status: u16, want: &[&str]) {
    report(before, p);
    assert_eq!(p.status, status, "{before:?}: {}", p.log);
    assert_eq!(p.calls, want, "{before:?}");
    assert!(p.took >= LATENCY * want.len() as u32, "{before:?}: calls ran one after another");
}

/// An episode cached at TorBox and not in the account: the listing must still be read (an add of a torrent the
/// account already has would be marked ours, which is what the cancel route trusts), the add made, the pack's
/// files listed to pick the episode, its link minted and checked. TorBox's queue is not asked: a cached torrent
/// waits for no download slot.
#[tokio::test]
async fn cached_not_held() {
    let p = play(Before::CachedNotHeld).await;
    assert_calls(
        Before::CachedNotHeld,
        &p,
        302,
        &["mylist_account", "createtorrent", "mylist_id", "requestdl", "cdn"],
    );
}

/// The queue not asked up front is still found when it matters: the add hears "already queued", the queue is read
/// then, and the play answers "queued" rather than a dead link.
#[tokio::test]
async fn cached_but_queued() {
    let p = play(Before::CachedButQueued).await;
    assert_calls(Before::CachedButQueued, &p, 202, &["mylist_account", "createtorrent", "getqueued"]);
}

/// An episode the account holds that Scout has forgotten: the listing finds it, and the status read's answer,
/// which lists the pack's files, picks the episode without asking for the list again.
#[tokio::test]
async fn held_forgotten() {
    let p = play(Before::HeldForgotten).await;
    assert_calls(Before::HeldForgotten, &p, 302, &["mylist_account", "mylist_id", "requestdl", "cdn"]);
}

/// An episode the account holds and Scout remembers: the held path, a file list and a link.
#[tokio::test]
async fn held_remembered() {
    let p = play(Before::HeldRemembered).await;
    assert_calls(Before::HeldRemembered, &p, 302, &["mylist_id", "requestdl", "cdn"]);
}

/// The play's own calls, without the listing refresh that runs beside it and is not waited on. The play line names
/// only the calls the play waited on, so it must not name the listing.
fn waited_on(p: &Played) -> Vec<String> {
    let line = p
        .log
        .lines()
        .find(|l| l.starts_with("event=download outcome=changed reason=play:"))
        .unwrap_or_else(|| panic!("no play line: {}", p.log));
    assert!(!line.contains("torbox_mylist_account"), "the play waited on the account listing: {line}");
    let refreshes = p.calls.iter().filter(|c| *c == "mylist_account").count();
    assert_eq!(refreshes, 1, "one listing refresh behind the play: {:?}", p.calls);
    p.calls.iter().filter(|c| *c != "mylist_account").cloned().collect()
}

/// The same episode played right after its stream list: the list read the account listing while the viewer chose,
/// so the play does not wait for it. The add is still made only because the listing says the account lacks the
/// pack, and the episode's file (id 1, S01E02) is the one linked.
#[tokio::test]
async fn cached_not_held_after_the_stream_list() {
    let p = play_after(Before::CachedNotHeld, Lead::StreamList).await;
    assert_calls(Before::CachedNotHeld, &p, 302, &["createtorrent", "mylist_id", "requestdl", "cdn"]);
    assert_eq!(p.location, "https://cdn.example/1.mkv");
}

/// A held pack Scout has forgotten, played after its stream list: the listing the list read finds it, so nothing
/// is added (no duplicate torrent, no add spent, nothing marked as Scout's own), and the episode is picked from
/// the status read's file list.
#[tokio::test]
async fn held_forgotten_after_the_stream_list() {
    let p = play_after(Before::HeldForgotten, Lead::StreamList).await;
    assert_calls(Before::HeldForgotten, &p, 302, &["mylist_id", "requestdl", "cdn"]);
    assert_eq!(p.location, "https://cdn.example/1.mkv");
}

/// A queued add is still found when the listing came from the stream list.
#[tokio::test]
async fn cached_but_queued_after_the_stream_list() {
    let p = play_after(Before::CachedButQueued, Lead::StreamList).await;
    assert_calls(Before::CachedButQueued, &p, 202, &["createtorrent", "getqueued"]);
}

/// Minutes after the stream list the listing is past its fifteen seconds. It still answers the play, which does
/// not wait for a new one, and a new one is fetched behind it.
#[tokio::test]
async fn cached_not_held_minutes_after_the_stream_list() {
    let p = play_after(Before::CachedNotHeld, Lead::StreamListMinutesAgo).await;
    report(Before::CachedNotHeld, &p);
    assert_eq!(p.status, 302, "{}", p.log);
    assert_eq!(waited_on(&p), ["createtorrent", "mylist_id", "requestdl", "cdn"]);
    assert_eq!(p.location, "https://cdn.example/1.mkv");
}

/// The older listing still names a held pack, so the play adds nothing.
#[tokio::test]
async fn held_forgotten_minutes_after_the_stream_list() {
    let p = play_after(Before::HeldForgotten, Lead::StreamListMinutesAgo).await;
    report(Before::HeldForgotten, &p);
    assert_eq!(p.status, 302, "{}", p.log);
    assert_eq!(waited_on(&p), ["mylist_id", "requestdl", "cdn"]);
    assert_eq!(p.location, "https://cdn.example/1.mkv");
}

/// The play's log line names each step and each TorBox call it waited on.
#[tokio::test]
async fn the_play_line_names_its_calls() {
    let p = play(Before::CachedNotHeld).await;
    let line = p
        .log
        .lines()
        .find(|l| l.starts_with("event=download outcome=changed reason=play: none -> ready"))
        .unwrap_or_else(|| panic!("no play line: {}", p.log))
        .to_owned();
    for phase in [
        "status;dur=",
        "resolve;dur=",
        "link_check;dur=",
        "torbox_createtorrent;dur=",
        "torbox_requestdl;dur=",
    ] {
        assert!(line.contains(phase), "{phase} missing: {line}");
    }
    assert!(line.contains(" phases=\""), "{line}");
}
