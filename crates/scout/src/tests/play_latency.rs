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
use crate::scrape::{DoError, Doer};
use crate::stores::refusal::{remember_cached, torrent_id_key};
use crate::stores::torbox::{TorBoxStore, RESOLVE_CACHE_TTL, TORBOX_API};
use crate::stores::Store;
use crate::tests::handler::{get, shared_stores, test_deps, valid_blob};

/// What one TorBox (or CDN) call takes. TorBox answers small reads in roughly this from the box; createtorrent and
/// a large account listing take longer, so the real gap is wider than the one measured here.
const LATENCY: Duration = Duration::from_millis(150);

const PACK: &str = "e83a1825cfc4000000000000000000000000beef";
const ID: i64 = 42;

/// A TorBox account and its CDN, answering after `LATENCY`. `held` is whether the account's listing names the pack.
struct Upstream {
    held: bool,
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
            return (name("getqueued"), ok(r#"{"success":true,"data":[]}"#.to_owned()));
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
}

struct Played {
    status: u16,
    calls: Vec<String>,
    took: Duration,
    log: String,
}

async fn play(before: Before) -> Played {
    let upstream =
        Arc::new(Upstream { held: !matches!(before, Before::CachedNotHeld), calls: Mutex::new(Vec::new()) });
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
    let shared: Arc<dyn Store> = store.clone();
    let (deps_cache, cdn) = (cache.clone(), upstream.clone());
    let h = new_handler(test_deps(move |d| {
        d.cache = deps_cache.clone();
        d.make_stores = shared_stores(shared.clone());
        d.probe_doer = Some(Arc::new(Slow(cdn.clone())));
    }));
    let log = Capture::start();
    let start = Instant::now();
    let resp = get(&h, &play_path(), &[]).await;
    let took = start.elapsed();
    Played { status: resp.status().as_u16(), calls: upstream.calls(), took, log: log.text() }
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

fn assert_calls(before: Before, p: &Played, want: &[&str]) {
    report(before, p);
    assert_eq!(p.status, 302, "{before:?} plays: {}", p.log);
    assert_eq!(p.calls, want, "{before:?}");
    assert!(p.took >= LATENCY * want.len() as u32, "{before:?}: calls ran one after another");
}

/// An episode cached at TorBox and not in the account: the listing must still be read (an add of a torrent the
/// account already has would be marked ours, which is what the cancel route trusts), the add made, the pack's
/// files listed to pick the episode, its link minted and checked.
#[tokio::test]
async fn cached_not_held() {
    let p = play(Before::CachedNotHeld).await;
    assert_calls(
        Before::CachedNotHeld,
        &p,
        &["mylist_account", "getqueued", "createtorrent", "mylist_id", "requestdl", "cdn"],
    );
}

/// An episode the account holds that Scout has forgotten: the listing finds it, the status read sees it finished,
/// and the pack's files are listed again to pick the episode.
#[tokio::test]
async fn held_forgotten() {
    let p = play(Before::HeldForgotten).await;
    assert_calls(
        Before::HeldForgotten,
        &p,
        &["mylist_account", "mylist_id", "mylist_id", "requestdl", "cdn"],
    );
}

/// An episode the account holds and Scout remembers: the held path, a file list and a link.
#[tokio::test]
async fn held_remembered() {
    let p = play(Before::HeldRemembered).await;
    assert_calls(Before::HeldRemembered, &p, &["mylist_id", "requestdl", "cdn"]);
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
