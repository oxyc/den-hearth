//! Go: `internal/scout/redirect_test.go` (132 lines).
//!
//! Tests `RefuseRedirectReplay`, the redirect policy in `deps.go`. Go's `http.Client.CheckRedirect` has no
//! reqwest twin of the same shape (`reqwest::redirect::Policy::custom` is the nearest), so the port
//! decides how the policy is expressed.

use std::sync::atomic::{AtomicI32, Ordering};
use std::sync::Arc;

use reqwest::{Method, Url};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use crate::addbudget::{budget_account, global_add_budget};
use crate::cache::MemoryCache;
use crate::config::DebridService;
use crate::deps::{refuse_redirect_replay, GuardedClient};
use crate::scrape::Doer;
use crate::stores::torbox::TorBoxStore;
use crate::stores::{ResolveTarget, Store};

/// What a loopback test server answers with: status, optional `Location`, body.
type Reply = (u16, Option<String>, &'static str);

/// Serves `respond(method, path)` on 127.0.0.1 until the test ends. Stands in for Go's `httptest.NewServer`.
/// Returns the base URL.
async fn spawn_server(respond: impl Fn(&str, &str) -> Reply + Send + Sync + 'static) -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.expect("bind a loopback port");
    let addr = listener.local_addr().expect("local address");
    let respond = Arc::new(respond);
    tokio::spawn(async move {
        loop {
            let Ok((mut sock, _)) = listener.accept().await else { return };
            let respond = respond.clone();
            tokio::spawn(async move {
                let mut buf = Vec::new();
                let mut chunk = [0u8; 1024];
                while !buf.windows(4).any(|w| w == b"\r\n\r\n") {
                    match sock.read(&mut chunk).await {
                        Ok(0) | Err(_) => return,
                        Ok(n) => buf.extend_from_slice(&chunk[..n]),
                    }
                }
                let head = String::from_utf8_lossy(&buf);
                let mut parts = head.lines().next().unwrap_or("").split(' ');
                let method = parts.next().unwrap_or("").to_owned();
                let path = parts.next().unwrap_or("").to_owned();
                let (status, location, body) = respond(&method, &path);
                let mut out =
                    format!("HTTP/1.1 {status} x\r\ncontent-length: {}\r\nconnection: close\r\n", body.len());
                if let Some(loc) = location {
                    out.push_str(&format!("location: {loc}\r\n"));
                }
                out.push_str("\r\n");
                let _ = sock.write_all(out.as_bytes()).await;
                if method != "HEAD" {
                    let _ = sock.write_all(body.as_bytes()).await;
                }
                let _ = sock.shutdown().await;
            });
        }
    });
    format!("http://{addr}")
}

// An upstream redirect must not turn one charged add into several real ones.
//
// Every add is charged once through spendAdd and then sent once — but Go's default redirect policy re-sends
// the same method AND body on a 307 or 308, up to ten hops. Measured before the guard: one charged add became
// two real POSTs on a single hop and ten on a loop, so the fifty-an-hour ceiling would have permitted five
// hundred real adds against the account. Premiumize is worse than a count: its apikey rides in the directdl
// form body, and Go replays the body across hosts even though it strips the Authorization header, so a 308
// elsewhere hands the token to an unrelated server.
//
// The redirect target is a SEPARATE path, so what is asserted is the number of add requests that actually
// reach a second location — the thing that costs the account.
#[tokio::test]
async fn an_add_is_not_replayed_to_the_redirect_target() {
    crate::runtime::scope(crate::runtime::ScoutRuntime::new(), async {
        for (status, name) in [(307u16, "307 temporary"), (308, "308 permanent"), (302, "302 found")] {
            let first_hop = Arc::new(AtomicI32::new(0));
            let replayed = Arc::new(AtomicI32::new(0));
            let (fh, rp) = (first_hop.clone(), replayed.clone());
            let base = spawn_server(move |method, path| match path {
                "/torrents/createtorrent" => {
                    fh.fetch_add(1, Ordering::SeqCst);
                    (status, Some("/moved".to_owned()), "")
                }
                "/moved" => {
                    if method == "POST" {
                        rp.fetch_add(1, Ordering::SeqCst);
                    }
                    (200, None, r#"{"data":{"torrent_id":9}}"#)
                }
                _ => (200, None, r#"{"success":true,"data":[]}"#),
            })
            .await;
            let token = format!("redirect-{name}");
            let hash = "a".repeat(40);
            let before = global_add_budget().remaining(&budget_account(DebridService::TorBox, &token));
            // The client the stores really use: the shared one `build_deps_shared` is handed.
            let client = GuardedClient::new(reqwest::Client::builder).expect("a client");
            let s = TorBoxStore {
                token: token.clone(),
                cache: Some(Arc::new(MemoryCache::new(1 << 20))),
                api: base,
                client: Arc::new(client),
            };
            let _ = s.resolve(&ResolveTarget { info_hash: hash, ..ResolveTarget::default() }).await;
            assert_eq!(
                replayed.load(Ordering::SeqCst),
                0,
                "{name}: the add was re-sent to the redirect target"
            );
            assert_eq!(first_hop.load(Ordering::SeqCst), 1, "{name}: the add reached the original endpoint");
            let after = global_add_budget().remaining(&budget_account(DebridService::TorBox, &token));
            assert!(before - after <= 1, "{name}: the hourly allowance moved by {}", before - after);
        }
    })
    .await;
}

// A credential in the query string must not follow a redirect off the debrid's API host — Go strips the
// Authorization header across hosts but never the query. A CDN's own redirects are untouched.
#[test]
fn credential_query_stays_on_the_debrid_host() {
    let url = |s: &str| Url::parse(s).expect("a valid url");
    let get = Method::GET;

    let requestdl = url("https://api.torbox.app/v1/api/torrents/requestdl?token=secret&torrent_id=1");
    assert!(
        refuse_redirect_replay(
            &get,
            &url("https://elsewhere.example/x?token=secret"),
            &[(get.clone(), requestdl.clone())]
        ),
        "a redirect from the TorBox API to another host was followed — the token goes with it"
    );
    assert!(
        !refuse_redirect_replay(
            &get,
            &url("https://api.torbox.app/v1/api/other"),
            &[(get.clone(), requestdl)]
        ),
        "a same-host redirect was refused"
    );

    let cache_check = url("https://www.premiumize.me/api/cache/check?apikey=secret");
    assert!(
        refuse_redirect_replay(&get, &url("https://elsewhere.example/"), &[(get.clone(), cache_check)]),
        "a redirect from Premiumize to another host was followed — the apikey goes with it"
    );

    let playback = url("https://store-1.tb-cdn.st/dld/abc?token=link");
    assert!(
        !refuse_redirect_replay(&get, &url("https://store-2.tb-cdn.st/dld/abc"), &[(get.clone(), playback)]),
        "a playback link's CDN redirect was refused"
    );
}

// GET and HEAD must still follow redirects: reading a playback link is exactly that, and the same client does
// it.
#[tokio::test]
async fn reads_still_follow_redirects() {
    for method in [Method::GET, Method::HEAD] {
        let served = Arc::new(AtomicI32::new(0));
        let hits = served.clone();
        let base = spawn_server(move |_, path| match path {
            "/final" => {
                hits.fetch_add(1, Ordering::SeqCst);
                (200, None, "")
            }
            "/start" => (302, Some("/final".to_owned()), ""),
            _ => (404, None, ""),
        })
        .await;

        let client = GuardedClient::new(reqwest::Client::builder).expect("a client");
        let target = Url::parse(&format!("{base}/start")).expect("a url");
        let resp = client
            .do_request(reqwest::Request::new(method.clone(), target))
            .await
            .expect("the request sends");

        assert!(
            resp.status() == 200 && served.load(Ordering::SeqCst) == 1,
            "{method} did not follow the redirect (status {}, final hits {}) — a playback link is read this way, \
             and CDNs redirect",
            resp.status(),
            served.load(Ordering::SeqCst)
        );
    }
}

// Through the client every store uses, a POST that is answered 307 or 308 (to the same host, or back to itself in
// a loop) is sent exactly once and the caller is handed the 3xx, as Go's `http.ErrUseLastResponse` hands it. A
// 303 turns the POST into a GET, which is followed once and is not a replay.
#[tokio::test]
async fn a_post_redirect_is_not_replayed_by_the_shared_client() {
    for (status, location, name) in [
        (307u16, "/moved", "307 to another path"),
        (308, "/moved", "308 to another path"),
        (307, "/add", "307 loop onto itself"),
    ] {
        let posts = Arc::new(AtomicI32::new(0));
        let requests = Arc::new(AtomicI32::new(0));
        let (p, r) = (posts.clone(), requests.clone());
        let base = spawn_server(move |method, _| {
            r.fetch_add(1, Ordering::SeqCst);
            if method == "POST" {
                p.fetch_add(1, Ordering::SeqCst);
            }
            (status, Some(location.to_owned()), "")
        })
        .await;
        let client = GuardedClient::new(reqwest::Client::builder).expect("a client");
        let mut req = reqwest::Request::new(Method::POST, Url::parse(&format!("{base}/add")).expect("a url"));
        *req.body_mut() = Some(reqwest::Body::from("hash=abc"));
        let resp = client.do_request(req).await.expect("the request sends");
        assert_eq!(resp.status().as_u16(), status, "{name}: the caller must see the 3xx");
        assert_eq!(posts.load(Ordering::SeqCst), 1, "{name}: the POST was sent more than once");
        assert_eq!(requests.load(Ordering::SeqCst), 1, "{name}: something followed the redirect");
    }

    let gets = Arc::new(AtomicI32::new(0));
    let posts = Arc::new(AtomicI32::new(0));
    let (g, p) = (gets.clone(), posts.clone());
    let base = spawn_server(move |method, path| match (method, path) {
        ("POST", "/add") => {
            p.fetch_add(1, Ordering::SeqCst);
            (303, Some("/done".to_owned()), "")
        }
        ("GET", "/done") => {
            g.fetch_add(1, Ordering::SeqCst);
            (200, None, "ok")
        }
        _ => (404, None, ""),
    })
    .await;
    let client = GuardedClient::new(reqwest::Client::builder).expect("a client");
    let mut req = reqwest::Request::new(Method::POST, Url::parse(&format!("{base}/add")).expect("a url"));
    *req.body_mut() = Some(reqwest::Body::from("hash=abc"));
    let resp = client.do_request(req).await.expect("the request sends");
    assert_eq!(resp.status().as_u16(), 200, "a 303 is followed as a GET, as Go does");
    assert_eq!((posts.load(Ordering::SeqCst), gets.load(Ordering::SeqCst)), (1, 1));
}

// A GET redirect loop is cut off at ten hops with an error, as Go's client does, not followed forever.
#[tokio::test]
async fn a_get_redirect_loop_stops_after_ten_hops() {
    let hits = Arc::new(AtomicI32::new(0));
    let h = hits.clone();
    let base = spawn_server(move |_, _| {
        h.fetch_add(1, Ordering::SeqCst);
        (302, Some("/again".to_owned()), "")
    })
    .await;
    let client = GuardedClient::new(reqwest::Client::builder).expect("a client");
    let req = reqwest::Request::new(Method::GET, Url::parse(&format!("{base}/again")).expect("a url"));
    assert!(client.do_request(req).await.is_err(), "a redirect loop was not cut off");
    // Go's client stops when `len(via) >= 10`, so ten requests are made and the eleventh is refused.
    assert_eq!(hits.load(Ordering::SeqCst), 10);
}
