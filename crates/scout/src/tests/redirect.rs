//! Go: `internal/scout/redirect_test.go` (132 lines). Work package 4.
//!
//! Tests `RefuseRedirectReplay`, the redirect policy in `deps.go`. Go's `http.Client.CheckRedirect` has no
//! reqwest twin of the same shape (`reqwest::redirect::Policy::custom` is the nearest), so the port
//! decides how the policy is expressed.

use std::sync::atomic::{AtomicI32, Ordering};
use std::sync::Arc;

use reqwest::{Method, Url};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use crate::deps::refuse_redirect_replay;

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
                let mut out = format!("HTTP/1.1 {status} x\r\ncontent-length: {}\r\nconnection: close\r\n", body.len());
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
#[ignore = "needs stores::torbox::TorBoxStore and addbudget's global budget from packages 1 and 3"]
async fn an_add_is_not_replayed_to_the_redirect_target() {
    // Enable when packages 1 and 3 land: move `pending` up and delete the wrapper.
    #[cfg(any())]
    async fn pending() {
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
            let client = reqwest::Client::builder()
                .redirect(reqwest::redirect::Policy::custom(|a| {
                    let via = a.previous().iter().map(|u| (Method::POST, u.clone())).collect::<Vec<_>>();
                    if refuse_redirect_replay(&Method::POST, a.url(), &via) { a.stop() } else { a.follow() }
                }))
                .build()
                .expect("a client");
            let s = TorBoxStore { token: token.clone(), cache: Arc::new(MemoryCache::new(1 << 20)), api: base, client };
            let _ = s.resolve(&ResolveTarget { info_hash: hash, ..ResolveTarget::default() }).await;
            assert_eq!(replayed.load(Ordering::SeqCst), 0, "{name}: the add was re-sent to the redirect target");
            assert_eq!(first_hop.load(Ordering::SeqCst), 1, "{name}: the add reached the original endpoint");
            let after = global_add_budget().remaining(&budget_account(DebridService::TorBox, &token));
            assert!(before - after <= 1, "{name}: the hourly allowance moved by {}", before - after);
        }
    }
    todo!("pending: go redirect_test.go:TestRefuseRedirectReplay_anAddIsNotReplayedToTheRedirectTarget")
}

// A credential in the query string must not follow a redirect off the debrid's API host — Go strips the
// Authorization header across hosts but never the query. A CDN's own redirects are untouched.
#[test]
fn credential_query_stays_on_the_debrid_host() {
    let url = |s: &str| Url::parse(s).expect("a valid url");
    let get = Method::GET;

    let requestdl = url("https://api.torbox.app/v1/api/torrents/requestdl?token=secret&torrent_id=1");
    assert!(
        refuse_redirect_replay(&get, &url("https://elsewhere.example/x?token=secret"), &[(get.clone(), requestdl.clone())]),
        "a redirect from the TorBox API to another host was followed — the token goes with it"
    );
    assert!(
        !refuse_redirect_replay(&get, &url("https://api.torbox.app/v1/api/other"), &[(get.clone(), requestdl)]),
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

        let m = method.clone();
        let client = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::custom(move |attempt| {
                let via: Vec<(Method, Url)> = attempt.previous().iter().map(|u| (m.clone(), u.clone())).collect();
                if refuse_redirect_replay(&m, attempt.url(), &via) {
                    attempt.stop()
                } else {
                    attempt.follow()
                }
            }))
            .build()
            .expect("a client");
        let resp = client.request(method.clone(), format!("{base}/start")).send().await.expect("the request sends");

        assert!(
            resp.status() == 200 && served.load(Ordering::SeqCst) == 1,
            "{method} did not follow the redirect (status {}, final hits {}) — a playback link is read this way, \
             and CDNs redirect",
            resp.status(),
            served.load(Ordering::SeqCst)
        );
    }
}
