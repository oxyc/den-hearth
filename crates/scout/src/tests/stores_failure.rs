//! Go: `internal/scout/stores_failure_test.go` (352 lines). Work package 2.
//!
//! Every way a debrid can fail to hand over a link. These matter more than the happy path: a resolve that
//! returns an error lets the pool fall through to another service, while one that panics, hangs, or
//! returns an empty string with a nil error takes the whole request down with it. The 2026-08-20 outage
//! was exactly this layer misbehaving under a service that had stopped answering.
//!
//! The TorBox tests and the ones that need `resolveCacheTTL` or `realDebridBlocked` are ignored until
//! their modules land.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;

use crate::cache::{Cache, MemoryCache};
use crate::rank::real_debrid_blocked;
use crate::scrape::{DoError, Doer};
use crate::stores::premiumize::{PremiumizeStore, PREMIUMIZE_API};
use crate::stores::realdebrid::{rd_torrent_key, RealDebridStore, REAL_DEBRID_API};
use crate::stores::{ResolveTarget, Store, StoreError, RESOLVE_CACHE_TTL};

use super::mock::resp;

/// Go `H` (stores_test.go:19).
const H: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";

type Answer = Result<reqwest::Response, DoError>;
type Route = Box<dyn Fn() -> Answer + Send + Sync>;

/// Go `routed`: dispatches by URL substring so a multi-call flow (addMagnet → info → selectFiles →
/// unrestrict) can fail at one specific step while the others behave.
struct Routed {
    routes: Vec<(&'static str, Route)>,
    fallback: Option<Route>,
}

#[async_trait]
impl Doer for Routed {
    async fn do_request(&self, req: reqwest::Request) -> Answer {
        let url = req.url().as_str();
        for (frag, f) in &self.routes {
            if url.contains(frag) {
                return f();
            }
        }
        match &self.fallback {
            Some(f) => f(),
            None => Ok(resp(200, "{}")),
        }
    }
}

fn routed(routes: Vec<(&'static str, Route)>) -> Arc<dyn Doer> {
    Arc::new(Routed { routes, fallback: None })
}

fn fallback_only(f: Route) -> Arc<dyn Doer> {
    Arc::new(Routed { routes: vec![], fallback: Some(f) })
}

fn ok(body: &'static str) -> Route {
    Box::new(move || -> Answer { Ok(resp(200, body)) })
}

fn status(code: u16) -> Route {
    Box::new(move || -> Answer { Ok(resp(code, "{}")) })
}

fn boom() -> Route {
    Box::new(|| -> Answer { Err("connection reset".into()) })
}

/// Go `wantDead`. A `Result` cannot carry a link and an error together, so Go's "returned a link
/// alongside an error" check has no Rust counterpart.
fn want_dead(what: &str, got: Result<String, StoreError>) {
    if let Ok(link) = got {
        panic!("{what}: expected an error so the pool can fall through, got link {link:?}");
    }
}

fn target(hash: &str) -> ResolveTarget {
    ResolveTarget { info_hash: hash.to_owned(), ..Default::default() }
}

/// Go `blockedName`: a name RD's matcher rejects, asserted to actually be one so the test and the block list
/// can't drift apart into a test that passes for the wrong reason.
fn blocked_name() -> &'static str {
    const NAME: &str = "Show.S01E01.WEB-DL.x264.mkv";
    assert!(real_debrid_blocked(NAME), "{NAME:?} is no longer a blocked name — pick one from rdBlockSubstrings");
    NAME
}

// Every way a debrid can fail to hand over a link: the Real-Debrid flow fails at each step in turn.
#[tokio::test]
async fn real_debrid_resolve_every_step_can_fail() {
    const ADD_OK: &str = r#"{"id":"abc"}"#;
    const INFO_OK: &str = r#"{"files":[{"id":1,"path":"Show.S01E01.mkv","bytes":900}],"links":["https://rd/l"]}"#;

    let cases: Vec<(&str, Arc<dyn Doer>)> = vec![
        ("addMagnet transport error", routed(vec![("addMagnet", boom())])),
        ("addMagnet rejected", routed(vec![("addMagnet", status(401))])),
        ("addMagnet returned no id", routed(vec![("addMagnet", ok("{}"))])),
        (
            "info http error",
            routed(vec![("addMagnet", ok(ADD_OK)), ("torrents/info", status(503))]),
        ),
        (
            "info body is not json",
            routed(vec![("addMagnet", ok(ADD_OK)), ("torrents/info", ok("<html>maintenance</html>"))]),
        ),
        (
            "torrent has no files",
            routed(vec![("addMagnet", ok(ADD_OK)), ("torrents/info", ok(r#"{"files":[]}"#))]),
        ),
        (
            "selectFiles rejected",
            routed(vec![
                ("addMagnet", ok(ADD_OK)),
                ("torrents/info", ok(INFO_OK)),
                ("selectFiles", status(403)),
            ]),
        ),
        (
            "nothing ready to unrestrict",
            routed(vec![
                ("addMagnet", ok(ADD_OK)),
                ("selectFiles", ok("{}")),
                (
                    "torrents/info",
                    ok(r#"{"files":[{"id":1,"path":"a.mkv","bytes":9}],"links":[]}"#),
                ),
            ]),
        ),
        (
            "unrestrict rejected",
            routed(vec![
                ("addMagnet", ok(ADD_OK)),
                ("torrents/info", ok(INFO_OK)),
                ("selectFiles", ok("{}")),
                ("unrestrict", status(429)),
            ]),
        ),
        (
            "unrestrict returned no download url",
            routed(vec![
                ("addMagnet", ok(ADD_OK)),
                ("torrents/info", ok(INFO_OK)),
                ("selectFiles", ok("{}")),
                ("unrestrict", ok(r#"{"download":""}"#)),
            ]),
        ),
    ];
    for (name, client) in cases {
        let s = RealDebridStore { token: "k".into(), client, cache: None, api: REAL_DEBRID_API.into() };
        want_dead(name, s.resolve(&target(H)).await);
    }
}

#[tokio::test]
async fn real_debrid_resolve_happy_path_returns_the_unrestricted_link() {
    let client = routed(vec![
        ("addMagnet", ok(r#"{"id":"abc"}"#)),
        (
            "torrents/info",
            ok(r#"{"files":[{"id":1,"path":"Show.S01E01.mkv","bytes":900}],"links":["https://rd/l"]}"#),
        ),
        ("selectFiles", ok("{}")),
        ("unrestrict", ok(r#"{"download":"https://rd/final.mkv"}"#)),
    ]);
    let s = RealDebridStore { token: "k".into(), client, cache: None, api: REAL_DEBRID_API.into() };
    match s.resolve(&target(H)).await {
        Ok(link) if link == "https://rd/final.mkv" => {}
        other => panic!("link/err: {other:?}"),
    }
}

// A held torrent RD has finished, with the requested file already selected, resolves in two calls: the
// link is in the first info, so re-POSTing the selection and re-reading info learns nothing. Anything
// short of that — not finished, or finished for a different selection — keeps the full four-call chain.
#[tokio::test]
async fn real_debrid_resolve_downloaded_and_selected_skips_select_files() {
    let cases = [
        // already bought: the held path
        ("downloaded, file selected", r#"{"status":"downloaded","links":["https://rd/l"],"files":[{"id":1,"path":"Show.S01E01.mkv","bytes":900,"selected":1}]}"#, 0, 1),
        ("downloaded, file not selected", r#"{"status":"downloaded","links":["https://rd/l"],"files":[{"id":1,"path":"Show.S01E01.mkv","bytes":900,"selected":0}]}"#, 1, 2),
        ("still waiting for a selection", r#"{"status":"waiting_files_selection","links":["https://rd/l"],"files":[{"id":1,"path":"Show.S01E01.mkv","bytes":900,"selected":1}]}"#, 1, 2),
    ];
    for (name, info, want_select, want_info) in cases {
        let calls: Arc<Mutex<HashMap<&'static str, usize>>> = Arc::default();
        let count = |name: &'static str, body: &'static str| -> Route {
            let calls = calls.clone();
            Box::new(move || -> Answer {
                *calls.lock().unwrap().entry(name).or_default() += 1;
                Ok(resp(200, body))
            })
        };
        let target = ResolveTarget { info_hash: H.into(), ..Default::default() };
        let cache = Arc::new(MemoryCache::new(1 << 20));
        cache.put(&rd_torrent_key("k", H, &target), "abc", RESOLVE_CACHE_TTL);
        let s = RealDebridStore {
            token: "k".into(),
            api: REAL_DEBRID_API.into(),
            cache: Some(cache),
            client: routed(vec![
                ("addMagnet", count("add", r#"{"id":"abc"}"#)),
                ("torrents/info", count("info", info)),
                ("selectFiles", count("select", "{}")),
                ("unrestrict", count("unrestrict", r#"{"download":"https://rd/final.mkv"}"#)),
            ]),
        };
        let link = s.resolve(&target).await;
        assert!(matches!(&link, Ok(l) if l == "https://rd/final.mkv"), "{name}: link/err: {link:?}");
        let calls = calls.lock().unwrap().clone();
        assert!(
            calls.get("select").copied().unwrap_or(0) == want_select
                && calls.get("info").copied().unwrap_or(0) == want_info
                && calls.get("unrestrict").copied().unwrap_or(0) == 1,
            "{name}: calls = {calls:?}, want select={want_select} info={want_info} unrestrict=1"
        );
        assert_eq!(calls.get("add").copied().unwrap_or(0), 0, "{name}: a held torrent was added again: {calls:?}");
    }
}

// RD refuses certain release SOURCE tags outright (web-dl, webrip, bdrip and friends). Failing fast on
// those is the point: the pool then tries another service, instead of spending an unrestrict call to
// surface a link that will not play.
#[tokio::test]
async fn real_debrid_resolve_blocked_filename_fails_fast() {
    // RD refuses certain release SOURCE tags outright, so the probe never reaches unrestrict.
    let unrestricted = Arc::new(AtomicBool::new(false));
    let u = unrestricted.clone();
    let info: &'static str = Box::leak(
        format!(r#"{{"files":[{{"id":1,"path":"{}","bytes":900}}],"links":["x"]}}"#, blocked_name()).into_boxed_str(),
    );
    let s = RealDebridStore {
        token: "k".into(),
        api: REAL_DEBRID_API.into(),
        cache: None,
        client: routed(vec![
            ("addMagnet", ok(r#"{"id":"abc"}"#)),
            ("torrents/info", ok(info)),
            (
                "unrestrict",
                Box::new(move || -> Answer {
                    u.store(true, Ordering::SeqCst);
                    Ok(resp(200, r#"{"download":"nope"}"#))
                }),
            ),
        ]),
    };
    let link = s.resolve(&target(H)).await;
    assert!(link.is_err(), "a blocked filename must not resolve");
    assert!(!unrestricted.load(Ordering::SeqCst), "it reached unrestrict — the point is to give up before spending that call");
}

#[tokio::test]
async fn premiumize_resolve_failure_modes() {
    let cases: Vec<(&str, Route)> = vec![
        ("transport error", boom()),
        ("http error", status(500)),
        ("body is not json", ok("not json at all")),
        ("status not success", ok(r#"{"status":"error","content":[{"path":"a.mkv","link":"x"}]}"#)),
        ("no content", ok(r#"{"status":"success","content":[]}"#)),
        (
            "content carries no link",
            ok(r#"{"status":"success","content":[{"path":"a.mkv","link":""}]}"#),
        ),
    ];
    for (name, f) in cases {
        let s = PremiumizeStore {
            token: "k".into(),
            client: fallback_only(f),
            cache: None,
            api: PREMIUMIZE_API.into(),
        };
        want_dead(name, s.resolve(&target(H)).await);
    }

    let s = PremiumizeStore {
        token: "k".into(),
        client: fallback_only(ok(
            r#"{"status":"success","content":[
			{"path":"Show.S01E01.mkv","link":"https://pm/1","size":10},
			{"path":"Show.S01E02.mkv","link":"https://pm/2","size":20}]}"#,
        )),
        cache: None,
        api: PREMIUMIZE_API.into(),
    };
    let t = ResolveTarget { info_hash: H.into(), season: Some(1), episode: Some(2), ..Default::default() };
    match s.resolve(&t).await {
        Ok(link) if link == "https://pm/2" => {}
        other => panic!("the named episode must win: {other:?}"),
    }
}

// TorBox's add is the step that decides whether a release is queued at all — and a failure here is what
// left `Status` with no torrent id to report during the outage.
#[tokio::test]
#[ignore = "needs stores/torbox.rs (package 1)"]
async fn torbox_resolve_add_failures() {
    #[cfg(any())]
    async fn pending() {
        let cases: Vec<(&str, Route)> = vec![
            ("transport error", boom()),
            ("rate limited", status(429)),
            ("service down", status(503)),
            ("no torrent_id in the body", ok(r#"{"data":{}}"#)),
            ("body is not json", ok("<html>502</html>")),
        ];
        for (name, f) in cases {
            let s = TorBoxStore { token: "k".into(), api: TORBOX_API.into(), cache: Some(Arc::new(MemoryCache::new(1 << 16))), client: fallback_only(f) };
            want_dead(name, s.resolve(&target(H)).await);
            // Nothing was queued, so nothing may be claimed as downloading — a false "downloading"
            // here is a wait the viewer sits through forever.
            assert!(s.status(&target(H)).await.is_none(), "claimed a download after the add failed");
        }
    }
    todo!("pending: stores_failure_test.go:221")
}

// A lost torrent id is rediscovered from the account list — but only when the account actually holds it.
#[tokio::test]
#[ignore = "needs stores/torbox.rs (package 1)"]
async fn find_torrent_by_hash_failure_modes() {
    #[cfg(any())]
    async fn pending() {
        let cases: Vec<(&str, Route)> = vec![
            ("transport error", boom()),
            ("http error", status(500)),
            ("body is not json", ok("nope")),
            ("account holds nothing", ok(r#"{"data":[]}"#)),
            ("account holds a different torrent", ok(r#"{"data":[{"id":3,"hash":"beef"}]}"#)),
        ];
        for (name, f) in cases {
            let s = TorBoxStore { token: "k".into(), api: TORBOX_API.into(), cache: Some(Arc::new(MemoryCache::new(1 << 16))), client: fallback_only(f) };
            let found = s.find_torrent_by_hash(H).await;
            assert!(found.is_none(), "{name}: claimed to find a torrent id");
        }
    }
    todo!("pending: stores_failure_test.go:248")
}

// listFiles says WHY it has no files, because the callers need different things from each answer. It was
// once best-effort — "a pack whose file list can't be read is still playable by index" — and that was
// wrong twice over: the index comes from the indexer, not the pack, so playing by it serves the wrong
// episode; and TorBox's own "no such torrent" is an answer that heals by re-adding, not a failure.
#[tokio::test]
#[ignore = "needs stores/torbox.rs (package 1)"]
async fn torbox_list_files_says_why_it_has_no_files() {
    #[cfg(any())]
    async fn pending() {
        // A 400 says nothing about the account; a 500 or a 401 does, and is classified separately in
        // TestTorBoxListFiles_classifiesTheRemainingShapes so it can raise the account-wide backoff.
        let cases: Vec<(&str, Route, Option<StoreError>)> = vec![
            ("a transport blip", boom(), Some(err_no_file_list())),
            ("a 400", status(400), Some(err_no_file_list())),
            ("an unreadable body", ok("garbage"), Some(err_no_file_list())),
            ("a 404", status(404), Some(err_torrent_gone())),
            ("the account no longer holds it", ok(r#"{"success":false,"data":null}"#), Some(err_torrent_gone())),
            ("a null payload", ok(r#"{"data":null}"#), Some(err_torrent_gone())),
            // Answered, with an entry that simply lists no files — not a failure, and not gone. The caller
            // refuses to guess an episode from it, but must not forget the id over it.
            ("an entry with no files", ok(r#"{"data":{}}"#), None),
        ];
        for (name, f, want) in cases {
            let s = TorBoxStore { token: "k".into(), api: TORBOX_API.into(), cache: None, client: fallback_only(f) };
            let (files, err) = s.list_files(1).await;
            assert!(files.is_empty(), "{name}: expected no files, got {}", files.len());
            assert_eq!(err, want, "{name}: got {err:?}, want {want:?} — the caller decides whether to re-add on this");
        }
    }
    todo!("pending: stores_failure_test.go:274")
}

// Fauda S1E3 hit errNoFileList repeatedly with nothing more specific anywhere in the log — every branch
// behind it shares one identity by design, so a transport blip, a bad shape and a genuine id mismatch
// were indistinguishable from the log alone. Each branch now writes its own line; this is what finds the
// cause the NEXT time this happens, rather than only ever rediscovering it as "no usable answer" again.
#[tokio::test]
#[ignore = "needs stores/torbox.rs (package 1) and the log helpers (package 6)"]
async fn torbox_list_files_logs_which_branch_failed() {
    #[cfg(any())]
    async fn pending() {
        // Each branch writes its own line, keyed by a name the test ages out of the log limiter first.
        let cases: Vec<(&str, &str, Route, &str)> = vec![
            ("a transport blip", "mylist-transport", boom(), "torbox mylist 1: connection reset"),
            ("a 400", "mylist-status", status(400), "torbox mylist 1: http 400"),
            ("an unreadable body", "mylist-decode", ok("garbage"), "torbox mylist 1: unreadable body"),
            ("a body with no data field", "mylist-empty", ok("{}"), "torbox mylist 1: no data field"),
            ("an answer naming another torrent", "mylist-other-id", ok(r#"{"data":{"id":2}}"#), "torbox mylist 1: answer named torrent 2"),
            ("neither an object nor an array", "mylist-shape", ok(r#"{"data":"garbage"}"#), "answer is neither an object nor an array"),
            ("an array with no matching entry", "mylist-no-match", ok(r#"{"data":[{"id":5}]}"#), "torbox mylist 1: 1 entries, none named it"),
        ];
        for (name, key, f, want) in cases {
            let out = capture_log();
            // Every one of these shares its key with the test above, which exercises the same branches for the
            // ERROR identity rather than the log line — ageLogLimit keeps that earlier coverage from silencing
            // this one under logLimited's one-line-a-minute rule.
            age_log_limit(key);
            let s = TorBoxStore { token: "k".into(), api: TORBOX_API.into(), cache: None, client: fallback_only(f) };
            let _ = s.list_files(1).await;
            assert!(out.contains(want), "{name}: log = {:?}, want it to contain {want:?}", out.contents());
        }
    }
    todo!("pending: stores_failure_test.go:309")
}
