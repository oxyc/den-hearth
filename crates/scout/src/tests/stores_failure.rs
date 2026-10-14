//! Go: `internal/scout/stores_failure_test.go` (352 lines). Work package 2.
//!
//! Every way a debrid can fail to hand over a link. These matter more than the happy path: a resolve that
//! returns an error lets the pool fall through to another service, while one that panics, hangs, or
//! returns an empty string with a nil error takes the whole request down with it. The 2026-08-20 outage
//! was exactly this layer misbehaving under a service that had stopped answering.
//!
//! The TorBox tests and the ones that need `resolveCacheTTL` or `realDebridBlocked` are ignored until
//! their modules land.

use std::sync::Arc;

use async_trait::async_trait;

use crate::scrape::{DoError, Doer};
use crate::stores::premiumize::{PremiumizeStore, PREMIUMIZE_API};
use crate::stores::realdebrid::{RealDebridStore, REAL_DEBRID_API};
use crate::stores::{ResolveTarget, Store, StoreError};

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
#[ignore = "needs resolveCacheTTL (stores/torbox.rs, package 1)"]
async fn real_debrid_resolve_downloaded_and_selected_skips_select_files() {
    todo!("port stores_failure_test.go:122 once package 1 lands")
}

// RD refuses certain release SOURCE tags outright (web-dl, webrip, bdrip and friends). Failing fast on
// those is the point: the pool then tries another service, instead of spending an unrestrict call to
// surface a link that will not play.
#[tokio::test]
#[ignore = "needs realDebridBlocked (rank.rs, package 5)"]
async fn real_debrid_resolve_blocked_filename_fails_fast() {
    todo!("port stores_failure_test.go:168 once package 5 lands")
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
    todo!("port stores_failure_test.go:221 once package 1 lands")
}

// A lost torrent id is rediscovered from the account list — but only when the account actually holds it.
#[tokio::test]
#[ignore = "needs stores/torbox.rs (package 1)"]
async fn find_torrent_by_hash_failure_modes() {
    todo!("port stores_failure_test.go:248 once package 1 lands")
}

// listFiles says WHY it has no files, because the callers need different things from each answer. It was
// once best-effort — "a pack whose file list can't be read is still playable by index" — and that was
// wrong twice over: the index comes from the indexer, not the pack, so playing by it serves the wrong
// episode; and TorBox's own "no such torrent" is an answer that heals by re-adding, not a failure.
#[tokio::test]
#[ignore = "needs stores/torbox.rs (package 1)"]
async fn torbox_list_files_says_why_it_has_no_files() {
    todo!("port stores_failure_test.go:274 once package 1 lands")
}

// Fauda S1E3 hit errNoFileList repeatedly with nothing more specific anywhere in the log — every branch
// behind it shares one identity by design, so a transport blip, a bad shape and a genuine id mismatch
// were indistinguishable from the log alone. Each branch now writes its own line; this is what finds the
// cause the NEXT time this happens, rather than only ever rediscovering it as "no usable answer" again.
#[tokio::test]
#[ignore = "needs stores/torbox.rs (package 1) and the log helpers (package 6)"]
async fn torbox_list_files_logs_which_branch_failed() {
    todo!("port stores_failure_test.go:309 once packages 1 and 6 land")
}
