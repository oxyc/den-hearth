//! Scout in process for Remux and Subtitles.
//!
//! Remux lists a title's releases and resolves a play URL at scout, and Subtitles follows a resync target through
//! scout's play redirect. Both used to do it over HTTP at the address they were configured with, the LAN one after
//! `ORIGIN_ALIASES`/`SCOUT_ALIASES`. Inside hearth that address is this process, so a URL at one of hearth's own
//! Scout bases is served by calling hearth's request path — `handle`, the same admission lanes, Scout handler,
//! revocation and scope checks, request log and metrics as a request on the socket — and never by connecting to it.
//!
//! The bases are what hearth knows to be itself, nothing guessed:
//!   * Scout's own listener (`HEARTH_SCOUT_PORT`), at the loopback names, at `SCOUT_PUBLIC_BASE_URL` (the origin
//!     Scout writes into every play URL it hands out), and at the host of `HEARTH_PUBLIC_BASE_URL` when that names
//!     this process directly (`http://<host>:<PORT>`, no path), which is how the LAN address is known.
//!   * The main listener's `/scout`, at the loopback names and under `HEARTH_PUBLIC_BASE_URL`.
//!
//! Any other scout URL — someone else's scout, or a name hearth was not told is its own — is fetched over HTTP as it
//! was, and every hop after scout (the debrid's CDN) always is.

use std::sync::{Arc, Weak};

use bytes::Bytes;
use http_body_util::Full;
use hyper::{Request, StatusCode, Uri};

use crate::route::Service;
use crate::Hearth;

/// A base URL served in process, and which listener's view of hearth it is served as.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Base {
    pub url: String,
    /// Scout's own listener (paths as they come), or the main one (paths under `/scout`).
    pub own: bool,
}

const LOOPBACK: [&str; 3] = ["127.0.0.1", "localhost", "[::1]"];

/// Hearth's own Scout bases (see the module comment). `port` is hearth's main port, `scout_port` Scout's own
/// listener if it has one, `hearth_public` `HEARTH_PUBLIC_BASE_URL`, `scout_public` Scout's `PUBLIC_BASE_URL` on its
/// own listener.
pub fn scout_bases(
    port: u16,
    scout_port: Option<u16>,
    hearth_public: Option<&str>,
    scout_public: &str,
) -> Vec<Base> {
    let mut bases = Vec::new();
    let hearth_public = hearth_public.map(|b| b.trim().trim_end_matches('/')).filter(|b| !b.is_empty());
    if let Some(p) = scout_port {
        let own = |url: String| Base { url, own: true };
        bases.extend(LOOPBACK.iter().map(|host| own(format!("http://{host}:{p}"))));
        let scout_public = scout_public.trim().trim_end_matches('/');
        if !scout_public.is_empty() {
            bases.push(own(scout_public.to_string()));
        }
        // This process's own address, when `HEARTH_PUBLIC_BASE_URL` is it: plain HTTP, hearth's port, no path.
        let direct = hearth_public.and_then(|b| b.parse::<Uri>().ok()).filter(|u| {
            u.scheme_str() == Some("http") && u.port_u16() == Some(port) && matches!(u.path(), "" | "/")
        });
        if let Some(host) = direct.as_ref().and_then(Uri::host) {
            bases.push(own(format!("http://{host}:{p}")));
        }
    }
    let main = |url: String| Base { url, own: false };
    bases.extend(LOOPBACK.iter().map(|host| main(format!("http://{host}:{port}/scout"))));
    if let Some(b) = hearth_public {
        bases.push(main(format!("{b}/scout")));
    }
    // `/den` (the combined addon) is deliberately not a base. If it ever becomes one, it must be limited to Scout's
    // routes (stream, p, play, account, availability, validate): `/den` also routes to Subtitles and Reel, and the
    // Remux key a call carries must never reach either.
    let mut seen = std::collections::HashSet::new();
    bases.retain(|b| seen.insert(b.url.clone()));
    bases
}

/// Serve `bases` in process to Remux and Subtitles. Returns the bases that were taken, for the startup line.
pub fn install(hearth: &Arc<Hearth>, bases: &[Base]) -> Vec<String> {
    let mut local = den_http::Local::default();
    for base in bases {
        let weak = Arc::downgrade(hearth);
        let own = base.own;
        let serve: den_http::local::Serve = Arc::new(move |req| Box::pin(call(weak.clone(), own, req)));
        if let Err(e) = local.add(&base.url, serve) {
            eprintln!("scout in process: skipping a base: {e}");
        }
    }
    let taken = local.bases().into_iter().map(str::to_string).collect();
    let _ = hearth.remux.local_scout.set(local.clone());
    let _ = hearth.subs.local_scout.set(local);
    taken
}

/// One request through hearth's own request path, as if it had arrived on Scout's listener (`own`) or on the main
/// one under `/scout`.
async fn call(hearth: Weak<Hearth>, own: bool, req: Request<Bytes>) -> hyper::Response<crate::Body> {
    let Some(hearth) = hearth.upgrade() else {
        return crate::json(StatusCode::SERVICE_UNAVAILABLE, &serde_json::json!({"error": "shutting_down"}));
    };
    let (mut parts, body) = req.into_parts();
    if !own {
        let rooted = format!("/scout{}", parts.uri.path_and_query().map_or("/", |p| p.as_str()));
        match rooted.parse() {
            Ok(uri) => parts.uri = uri,
            Err(_) => return crate::bad_path(),
        }
    }
    let service = own.then_some(Service::Scout);
    crate::handle(hearth, service, false, Request::from_parts(parts, Full::new(body))).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering::Relaxed};
    use std::time::{Duration, Instant};

    use hyper::HeaderMap;

    /// An install id (16 bytes, base64url) that the test Scout has revoked.
    const REVOKED: &str = "AAECAwQFBgcICQoLDA0ODw";
    const REMUX_KEY: &str = "remux-secret";

    /// A port that counts the connections it accepts and answers none: claimed as Scout's own port, so any request
    /// that reached a socket there would show.
    async fn counting_port() -> (u16, Arc<AtomicUsize>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let accepted = Arc::new(AtomicUsize::new(0));
        let counter = accepted.clone();
        tokio::spawn(async move {
            while let Ok((sock, _)) = listener.accept().await {
                counter.fetch_add(1, Relaxed);
                drop(sock);
            }
        });
        (port, accepted)
    }

    /// An indexer and Cinemeta in one: every stream list is one release and every lookup finds nothing, so a request
    /// Scout admits makes no call off this machine (the test configs are Real-Debrid only, which has no cache check
    /// to ask at list time, and an uncached release is never probed).
    async fn upstream_stub() -> String {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        tokio::spawn(async move {
            while let Ok((mut sock, _)) = listener.accept().await {
                tokio::spawn(async move {
                    let mut buf = [0u8; 8192];
                    while let Ok(n) = sock.read(&mut buf).await {
                        if n == 0 {
                            break;
                        }
                        let body = match buf[..n].windows(8).any(|w| w == b"/stream/") {
                            true => RELEASE,
                            false => "{}",
                        };
                        let head = format!(
                            "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\n\r\n",
                            body.len()
                        );
                        if sock.write_all(format!("{head}{body}").as_bytes()).await.is_err() {
                            break;
                        }
                    }
                });
            }
        });
        base
    }

    /// The one release the indexer stub lists, as Torrentio would.
    const RELEASE: &str = r#"{"streams":[{"name":"Torrentio\n1080p","title":"Film.1994.2160p.BluRay.REMUX.HEVC\n👤 50 💾 58 GB","infoHash":"0123456789abcdef0123456789abcdef01234567","fileIdx":0,"behaviorHints":{"filename":"Film.1994.2160p.BluRay.REMUX.HEVC.mkv"}}]}"#;

    fn hearth_with(admission: crate::admit::Limits, upstream: &str) -> Arc<Hearth> {
        let upstream = upstream.to_string();
        let settings = || {
            let mut s = den_scout::deps::settings_from_env(|name| match name {
                "CONFIG_KEY" => den_scout::util::b64url_encode(&CONFIG_KEY),
                "REMUX_KEY" => REMUX_KEY.into(),
                "REVOKED_INSTALLS" => REVOKED.into(),
                // Absolute URLs from a fixed origin, so they do not depend on the port a request came to.
                "PUBLIC_BASE_URL" => "http://scout.test".into(),
                "TORRENTIO_URL" | "CINEMETA_URL" => upstream.clone(),
                _ => String::new(),
            });
            s.cache_dir = String::new();
            s
        };
        let den = crate::den::Den::new(None, false, Default::default(), None, "none");
        Hearth::new(
            den_reel::AppState::new(den_reel::Config::from_env()),
            den_subtitles::AppState::new(den_subtitles::Config::from_env()),
            den_remux::AppState::new(den_remux::Config::from_env()),
            crate::scout::Scout::new(settings(), settings(), &den.scout),
            den,
            crate::Limits { admission, media: crate::media::Limits::default() },
            None,
        )
    }

    /// The test Scout's `CONFIG_KEY`: a scoped config is honoured only sealed.
    const CONFIG_KEY: [u8; 32] = [7; 32];

    fn config(json: &str) -> String {
        let public = den_seal::KeyPair::new(CONFIG_KEY).pub_key;
        let mut framed = vec![den_seal::SEALED_VERSION];
        framed.extend(den_seal::seal(&public, json.as_bytes()).unwrap());
        den_scout::util::b64url_encode(&framed)
    }

    /// Scout's own answers to the requests Remux makes, for each of the admission cases: a scoped config with and
    /// without the key, a revoked install, a ticket that is not one.
    fn cases() -> Vec<(&'static str, String, Option<&'static str>)> {
        let debrid = r#""debrid":[{"service":"realdebrid","token":"t"}],"indexers":["torrentio"]"#;
        let scoped = config(&format!(r#"{{{debrid},"scope":"availability"}}"#));
        let revoked = config(&format!(r#"{{{debrid},"iid":"{REVOKED}"}}"#));
        vec![
            ("scoped without the key", format!("/{scoped}/stream/movie/tt0111161.json"), None),
            ("scoped with the key", format!("/{scoped}/stream/movie/tt0111161.json"), Some(REMUX_KEY)),
            ("scoped with a wrong key", format!("/{scoped}/stream/movie/tt0111161.json"), Some("wrong")),
            ("revoked install", format!("/{revoked}/stream/movie/tt0111161.json"), Some(REMUX_KEY)),
            ("not a ticket", "/p/not-a-ticket".to_string(), Some(REMUX_KEY)),
        ]
    }

    fn headers(key: Option<&str>) -> HeaderMap {
        let mut h = HeaderMap::new();
        if let Some(k) = key {
            h.insert("x-den-remux-key", k.parse().unwrap());
        }
        h
    }

    /// An answer: status, the headers that do not vary by the moment it was sent, body.
    type Answer = (u16, Vec<(String, String)>, Bytes);

    fn steady(headers: &HeaderMap) -> Vec<(String, String)> {
        let mut out: Vec<(String, String)> = headers
            .iter()
            .filter(|(name, _)| !matches!(name.as_str(), "date" | "server-timing"))
            .map(|(name, value)| (name.to_string(), String::from_utf8_lossy(value.as_bytes()).into_owned()))
            .collect();
        out.sort();
        out
    }

    async fn in_process(hearth: &Arc<Hearth>, url: &str, key: Option<&str>) -> Answer {
        let local = hearth.remux.local_scout.get().expect("installed");
        let resp = local.get(url, headers(key), 1 << 20).expect("served in process").await.unwrap();
        (resp.status().as_u16(), steady(resp.headers()), resp.into_body())
    }

    async fn over_http(url: &str, key: Option<&str>) -> Answer {
        let resp = reqwest::Client::new().get(url).headers(headers(key)).send().await.unwrap();
        let mut steady = steady(resp.headers());
        // Hyper's own framing of a body it already holds; an in-process answer has no wire to frame it on.
        steady.retain(|(name, _)| name != "content-length");
        (resp.status().as_u16(), steady, resp.bytes().await.unwrap())
    }

    /// An answer's JSON less what is new on every answer: each play ticket is sealed with a fresh nonce, and `den`
    /// carries the build's timestamps and whether the scrape behind it was cached. Everything else must match.
    fn comparable(body: &[u8]) -> serde_json::Value {
        let mut json: serde_json::Value = serde_json::from_slice(body).unwrap();
        if let Some(o) = json.as_object_mut() {
            o.remove("den");
        }
        for s in json["streams"].as_array_mut().into_iter().flatten() {
            let url = s["url"].as_str().unwrap_or_default();
            let ticketless =
                url.split_once("/p/").map_or(url.to_string(), |(origin, _)| format!("{origin}/p/<ticket>"));
            s["url"] = ticketless.into();
        }
        json
    }

    /// Every admission case answers the same in process as over Scout's own port — the scoped config lists only
    /// with the service key, the revoked install is refused — and the port claimed as Scout's sees no connection.
    #[tokio::test]
    async fn scout_in_process_admits_and_refuses_exactly_as_over_its_port() {
        let upstream = upstream_stub().await;
        let hearth = hearth_with(crate::admit::Limits::default(), &upstream);
        let (claimed, accepted) = counting_port().await;
        install(&hearth, &scout_bases(1, Some(claimed), None, ""));
        // The same hearth, really listening on Scout's own port elsewhere, for the HTTP side.
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let real = listener.local_addr().unwrap().port();
        let (_stop, stopped) = tokio::sync::watch::channel(false);
        tokio::spawn(crate::serve_until(
            listener,
            Some(Service::Scout),
            hearth.clone(),
            stopped,
            Duration::from_millis(200),
        ));

        let mut statuses = Vec::new();
        for (case, path, key) in cases() {
            let local_url = format!("http://127.0.0.1:{claimed}{path}");
            // Scraped once first, so both sides list from the same scrape rather than two a moment apart.
            in_process(&hearth, &local_url, key).await;
            let (status, mut headers, body) = in_process(&hearth, &local_url, key).await;
            let (http_status, http_headers, http_body) =
                over_http(&format!("http://127.0.0.1:{real}{path}"), key).await;
            headers.retain(|(name, _)| name != "content-length");
            let (body, http_body) = (comparable(&body), comparable(&http_body));
            // The ETag hashes the body, which differs only in what `comparable` sets aside.
            let no_etag =
                |h: Vec<(String, String)>| -> Vec<_> { h.into_iter().filter(|(n, _)| n != "etag").collect() };
            assert_eq!(
                (status, no_etag(headers), &body),
                (http_status, no_etag(http_headers), &http_body),
                "{case}"
            );
            statuses.push((case, status, body["streams"].as_array().map_or(0, Vec::len)));
        }
        assert_eq!(
            statuses,
            [
                ("scoped without the key", 403, 0),
                ("scoped with the key", 200, 1),
                ("scoped with a wrong key", 403, 0),
                ("revoked install", 400, 0),
                ("not a ticket", 400, 0),
            ]
        );
        assert_eq!(accepted.load(Relaxed), 0, "a request served in process reached Scout's port");
    }

    /// The in-process call is admitted like a request on the socket: with the bulk lane full it is refused in the
    /// stable shape, and Scout's control routes still answer.
    #[tokio::test]
    async fn scout_in_process_takes_the_same_admission_lanes() {
        let tiny = crate::admit::Limits {
            connections: 2,
            probation: 1,
            requests: 2,
            bulk_requests: 1,
            wait: Duration::from_millis(5),
            body_idle: Duration::from_secs(30),
        };
        let hearth = hearth_with(tiny, "http://127.0.0.1:9");
        install(&hearth, &scout_bases(8090, Some(8080), None, ""));
        let _bulk = hearth.admission.admit(crate::route::Lane::Bulk).await.unwrap();
        let (status, _, body) =
            in_process(&hearth, "http://127.0.0.1:8080/cfg/stream/movie/tt1.json", None).await;
        assert_eq!(status, 503);
        assert!(String::from_utf8_lossy(&body).contains("server_busy"), "{body:?}");
        let (status, _, _) = in_process(&hearth, "http://localhost:8090/scout/manifest.json", None).await;
        assert_eq!(status, 200, "the control lane still answers, through the main listener's /scout");
    }

    /// How long Remux's scout hop takes, in process and over Scout's own port on loopback: a keyed stream list (the
    /// first call of a session start) and a play URL (the second). `cargo test -p den-hearth --release -- --ignored
    /// --nocapture scout_hop_timing`.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[ignore = "a measurement, not a check"]
    async fn scout_hop_timing() {
        let upstream = upstream_stub().await;
        let hearth = hearth_with(crate::admit::Limits::default(), &upstream);
        let (claimed, _) = counting_port().await;
        install(&hearth, &scout_bases(1, Some(claimed), None, ""));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let real = listener.local_addr().unwrap().port();
        let (_stop, stopped) = tokio::sync::watch::channel(false);
        tokio::spawn(crate::serve_until(
            listener,
            Some(Service::Scout),
            hearth.clone(),
            stopped,
            Duration::ZERO,
        ));
        // Remux's own client: pooled, so the HTTP side pays a request on a kept-alive connection, its best case.
        let client = reqwest::Client::builder().redirect(reqwest::redirect::Policy::none()).build().unwrap();
        let cases = cases();
        for (case, path, key) in [&cases[1], &cases[4]] {
            const N: u32 = 2000;
            let http_url = format!("http://127.0.0.1:{real}{path}");
            let local_url = format!("http://127.0.0.1:{claimed}{path}");
            for _ in 0..50 {
                client.get(&http_url).headers(headers(*key)).send().await.unwrap().bytes().await.unwrap();
                in_process(&hearth, &local_url, *key).await;
            }
            let t = Instant::now();
            for _ in 0..N {
                client.get(&http_url).headers(headers(*key)).send().await.unwrap().bytes().await.unwrap();
            }
            let http = t.elapsed() / N;
            let t = Instant::now();
            for _ in 0..N {
                in_process(&hearth, &local_url, *key).await;
            }
            let local = t.elapsed() / N;
            println!("{case}: over HTTP {http:?} per call, in process {local:?} per call");
        }
    }

    fn urls(bases: &[Base]) -> Vec<(&str, bool)> {
        bases.iter().map(|b| (b.url.as_str(), b.own)).collect()
    }

    #[test]
    fn the_bases_are_what_hearth_knows_to_be_itself() {
        let bases =
            scout_bases(8090, Some(8080), Some("http://192.168.86.193:8090/"), "https://d-play.example");
        assert_eq!(
            urls(&bases),
            [
                ("http://127.0.0.1:8080", true),
                ("http://localhost:8080", true),
                ("http://[::1]:8080", true),
                ("https://d-play.example", true),
                ("http://192.168.86.193:8080", true),
                ("http://127.0.0.1:8090/scout", false),
                ("http://localhost:8090/scout", false),
                ("http://[::1]:8090/scout", false),
                ("http://192.168.86.193:8090/scout", false),
            ]
        );
    }

    #[test]
    fn a_public_base_that_is_not_this_process_names_no_lan_address() {
        // Behind a proxy (https, another port, or a path), the host is not where Scout's own port listens.
        for public in ["https://hearth.example", "http://hearth.example:9000", "http://hearth.example:8090/x"]
        {
            let bases = scout_bases(8090, Some(8080), Some(public), "");
            assert!(!bases.iter().any(|b| b.url.starts_with("http://hearth.example:8080")), "{public}");
        }
        // No own listener: only the main one's /scout.
        let bases = scout_bases(8090, None, None, "https://d-play.example");
        assert!(bases.iter().all(|b| !b.own && b.url.ends_with(":8090/scout")), "{bases:?}");
    }

    /// A base named twice, however far apart, is listed once.
    #[test]
    fn a_base_named_twice_is_listed_once() {
        // Scout's public URL is also its loopback address, and the LAN address is loopback too.
        let bases = scout_bases(8090, Some(8080), Some("http://127.0.0.1:8090"), "http://127.0.0.1:8080/");
        let mut urls: Vec<&str> = bases.iter().map(|b| b.url.as_str()).collect();
        let listed = urls.len();
        urls.sort();
        urls.dedup();
        assert_eq!(urls.len(), listed, "{bases:?}");
        assert_eq!(listed, 6, "{bases:?}");
    }
}
