//! The wire contract of every route, pinned whole: status, every response header and the body, for each route
//! and each refusal it can give, plus the request log lines. Written from the handler as it was before its HTTP
//! layer was rewritten (`golden_http.txt`), so a rewrite that changes any byte a client or proxy reads fails here.
//!
//! What cannot be fixed across runs is masked, never dropped: Server-Timing durations, the build time a list
//! carries (and so a list's ETag, which is checked against its body instead), random play tickets and the request
//! time in a log line.
//!
//! `SCOUT_GOLDEN_WRITE=1 cargo test -p den-scout golden` rewrites the file.

use std::fmt::Write as _;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use regex::Regex;

use crate::cache::{Cache, MemoryCache};
use crate::config::DebridService;
use crate::deps::Deps;
use crate::handler::{etag_for, join_cached, new_handler, now_unix, split_cached, Handler};
use crate::play::{encode_play_token, PlayTarget};
use crate::rank::RawStream;
use crate::scrape::DoError;
use crate::stores::{StoreError, StoreStatus, StoreUnavailableError};
use crate::tests::availability::sealed_config;
use crate::tests::handler::{
    blob, dead_link, fake_scraper, send, test_deps, test_seeds, valid_blob, FakeStore, RecordingCache,
};
use crate::tests::ticket::{ticket_keyring, ticket_keys};

const GOLDEN: &str = include_str!("golden_http.txt");

struct Golden {
    out: String,
    /// The ETag of the last response, for a revalidation that follows it.
    last_etag: String,
    /// Random values (a sealed config segment) and the name each is written as.
    aliases: Vec<(String, &'static str)>,
}

impl Golden {
    async fn case(&mut self, name: &str, h: &Handler, method: &str, path: &str, headers: &[(&str, &str)]) {
        self.case_with_body(name, h, method, path, headers, "").await;
    }

    async fn case_with_body(
        &mut self,
        name: &str,
        h: &Handler,
        method: &str,
        path: &str,
        headers: &[(&str, &str)],
        body: &str,
    ) {
        let mut req = http::Request::builder()
            .method(http::Method::from_bytes(method.as_bytes()).expect("a method"))
            .uri(format!("https://scout.example{path}"));
        for (k, v) in headers {
            req = req.header(*k, *v);
        }
        let resp = send(h, req.body(body.as_bytes().to_vec()).expect("a request")).await;
        let sent_etag = headers.iter().find(|(k, _)| *k == "if-none-match").map(|(_, v)| *v);
        let mut out = String::new();
        let _ = writeln!(out, "== {name}\n> {method} {}", mask_path(path));
        for (k, _) in headers {
            let _ = writeln!(out, "> {k}: …");
        }
        let _ = writeln!(out, "< {}", resp.status().as_u16());
        let text = String::from_utf8_lossy(resp.body()).into_owned();
        let timed = text.contains("\"generatedAt\"");
        let mut names: Vec<&str> = resp.headers().keys().map(|k| k.as_str()).collect();
        names.sort_unstable();
        let duration = Regex::new(r"dur=[0-9.]+").unwrap();
        for name in names {
            for value in resp.headers().get_all(name) {
                let value = value.to_str().unwrap_or("<binary>");
                let shown = match name {
                    "server-timing" => duration.replace_all(value, "dur=*").into_owned(),
                    "location" => mask_path(value),
                    "etag"
                        if resp.status() == 304
                            && sent_etag.is_some_and(|s| s.trim_start_matches("W/") == value) =>
                    {
                        "<the tag revalidated>".to_owned()
                    }
                    "etag" if timed && value == etag_for(&text) => "<the body's FNV-1a-64>".to_owned(),
                    // A HEAD has no body to check its tag against; it must be the tag the GET before it carried.
                    "etag" if method == "HEAD" && value == self.last_etag => {
                        "<the previous response's>".to_owned()
                    }
                    "etag" if timed => format!("<MISMATCH {value}>"),
                    _ => value.to_owned(),
                };
                let _ = writeln!(out, "< {name}: {shown}");
            }
        }
        if let Some(etag) = resp.headers().get("etag").and_then(|v| v.to_str().ok()) {
            self.last_etag = etag.to_owned();
        }
        let content_type = resp.headers().get("content-type").and_then(|v| v.to_str().ok()).unwrap_or("");
        let body = if content_type.starts_with("text/html") {
            format!("<html: {} bytes, FNV-1a-64 {}>", text.len(), crate::util::etag_hex(&text))
        } else if content_type.starts_with("text/plain") {
            format!(
                "<metrics: {} lines, first {:?}>",
                text.lines().count(),
                text.lines().next().unwrap_or("")
            )
        } else {
            mask_body(&text)
        };
        let _ = writeln!(out, "{body}\n");
        for (value, alias) in &self.aliases {
            out = out.replace(value.as_str(), alias);
        }
        self.out.push_str(&out);
    }
}

/// A play ticket is random, so its URL is written as the route alone.
fn mask_path(p: &str) -> String {
    Regex::new(r"/p/[A-Za-z0-9_-]{24,}").unwrap().replace_all(p, "/p/<ticket>").into_owned()
}

fn mask_body(s: &str) -> String {
    let s = mask_path(s);
    let s = Regex::new(r#""(generatedAt|expiresAt|observedAt)":"[^"]*""#)
        .unwrap()
        .replace_all(&s, "\"$1\":\"*\"");
    s.into_owned()
}

fn handler(over: impl FnOnce(&mut Deps)) -> Handler {
    new_handler(test_deps(over))
}

fn token(hash: char) -> String {
    encode_play_token(&PlayTarget {
        info_hash: hash.to_string().repeat(40),
        file_idx: Some(0),
        ..Default::default()
    })
}

fn store(over: impl FnOnce(&mut FakeStore)) -> crate::deps::StoreFactory {
    let mut s = FakeStore {
        svc: DebridService::TorBox,
        check: [("a".repeat(40), true)].into_iter().collect(),
        check_err: None,
        resolve: Arc::new(|| Ok("https://cdn.torbox/a.mkv".to_owned())),
        status: None,
        status_blocks: false,
    };
    over(&mut s);
    let s = Arc::new(s);
    Arc::new(move |_| {
        let s = s.clone();
        vec![Box::new(FakeStore {
            svc: s.svc,
            check: s.check.clone(),
            check_err: s.check_err.clone(),
            resolve: s.resolve.clone(),
            status: s.status.clone(),
            status_blocks: s.status_blocks,
        }) as Box<dyn crate::stores::Store>]
    })
}

async fn render() -> String {
    let mut g = Golden { out: String::new(), last_etag: String::new(), aliases: Vec::new() };
    let cfg = valid_blob();
    let list = format!("/{cfg}/stream/movie/tt1234567.json");

    // ---- static routes and the method gate, on a handler with no keys --------------------------------------
    let h = handler(|_| {});
    g.case("preflight on any path", &h, "OPTIONS", "/anything/at/all", &[]).await;
    g.case("standalone root removed", &h, "GET", "/", &[]).await;
    g.case("standalone configure removed", &h, "GET", "/configure", &[]).await;
    g.case("standalone manifest removed", &h, "GET", "/manifest.json", &[]).await;
    g.case("standalone config key removed", &h, "GET", "/config-key", &[]).await;
    g.case("health", &h, "GET", "/health", &[]).await;
    g.case("health HEAD", &h, "HEAD", "/health", &[]).await;
    g.case("health POST", &h, "POST", "/health", &[]).await;
    g.case("PUT on a list", &h, "PUT", &list, &[]).await;
    g.case("metrics without a token configured", &h, "GET", "/metrics", &[("authorization", "Bearer x")])
        .await;
    g.case("unknown one-segment path", &h, "GET", "/nope", &[]).await;
    g.case("unknown route under a config", &h, "GET", &format!("/{cfg}/nope/x"), &[]).await;

    // ---- stream lists ----------------------------------------------------------------------------------------
    g.case("list, bad id", &h, "GET", &format!("/{cfg}/stream/movie/nope.json"), &[]).await;
    g.case("list, bad config", &h, "GET", "/not-a-config/stream/movie/tt1234567.json", &[]).await;
    g.case("list, cold build", &h, "GET", &list, &[]).await;
    let tag = g.last_etag.clone();
    g.case("list, warm hit", &h, "GET", &list, &[]).await;
    g.case("list, revalidated", &h, "GET", &list, &[("if-none-match", &tag)]).await;
    let weak = format!("W/{tag}");
    g.case("list, revalidated with a weak tag", &h, "GET", &list, &[("if-none-match", &weak)]).await;
    g.case("list, HEAD", &h, "HEAD", &list, &[]).await;
    let playable = r#"{"h264":true,"hevcMain":false,"eac3":false}"#;
    g.case("list, ranked for a browser", &h, "GET", &list, &[("x-den-playable", playable)]).await;
    g.case("list, unreadable playable report", &h, "GET", &list, &[("x-den-playable", "{nope")]).await;
    g.case("list, episode", &h, "GET", &format!("/{cfg}/stream/series/tt1234567:1:2.json"), &[]).await;
    h.set_debug_limiter(Duration::from_secs(3600), 1);
    g.case("list, debug", &h, "GET", &format!("{list}?debug=1"), &[]).await;
    g.case("list, debug paced", &h, "GET", &format!("{list}?debug=1"), &[]).await;
    let scoped = blob(r#"{"debrid":[{"service":"torbox","token":"tb-secret"}],"scope":"availability"}"#);
    g.case("list, scoped config", &h, "GET", &format!("/{scoped}/stream/movie/tt1234567.json"), &[]).await;

    // A stale list is served at once, marked, while a rebuild runs behind it.
    let cache =
        Arc::new(RecordingCache { inner: Arc::new(MemoryCache::new(1 << 20)), last: Default::default() });
    let stale_h = handler(|d| d.cache = cache.clone());
    g.case("stale: cold build", &stale_h, "GET", &list, &[]).await;
    let key = cache.last_key();
    let held = cache.get(&key).expect("the build cached its list");
    let (complete, _, etag, body) = split_cached(&held);
    cache.put(&key, &join_cached(complete, now_unix() - 1, etag, body), Duration::from_secs(60));
    g.case("stale: served while it rebuilds", &stale_h, "GET", &list, &[]).await;

    // Release names a JSON encoder may escape.
    let odd = handler(|d| {
        d.make_scrapers = Arc::new(|_| {
            vec![fake_scraper("torrentio", || {
                Box::pin(async {
                    let mut seeds = test_seeds();
                    seeds[0].title = "Fast & Furious <Director's Cut> 2160p WEB-DL HDR".to_owned();
                    Ok::<Vec<RawStream>, DoError>(seeds)
                })
            })]
        })
    });
    g.case("list, a release name with & < > in it", &odd, "GET", &list, &[]).await;

    // Every indexer failed: the empty list is served, marked, and not cached.
    let down = handler(|d| {
        d.make_scrapers = Arc::new(|_| {
            vec![fake_scraper("torrentio", || {
                Box::pin(async { Err(Box::new(std::io::Error::other("connection refused")) as DoError) })
            })]
        })
    });
    g.case("list, every indexer down", &down, "GET", &list, &[]).await;

    // ---- /play on the legacy route -----------------------------------------------------------------------------
    let play = format!("/{cfg}/play/{}", token('a'));
    g.case("play, resolved", &h, "GET", &play, &[]).await;
    g.case("play, from the link memo", &h, "GET", &play, &[]).await;
    g.case("play, HEAD", &h, "HEAD", &play, &[]).await;
    g.case("play, POST", &h, "POST", &play, &[]).await;
    g.case("play, bad token", &h, "GET", &format!("/{cfg}/play/nope"), &[]).await;
    g.case("play, bad config", &h, "GET", &format!("/not-a-config/play/{}", token('a')), &[]).await;
    g.case("play, probe ready", &h, "GET", &format!("{play}?probe=1"), &[]).await;
    let other = format!("/{cfg}/play/{}", token('c'));
    let nothing = handler(|d| d.make_stores = store(|s| s.resolve = Arc::new(|| Err(dead_link("gone")))));
    g.case("play, probe not queued", &nothing, "GET", &format!("{other}?probe=1"), &[]).await;
    g.case("play, dead", &nothing, "GET", &other, &[]).await;
    let queued = handler(|d| {
        d.make_stores = store(|s| {
            s.status = Some(StoreStatus {
                progress: 0.25,
                service: Some(DebridService::TorBox),
                seeds: Some(4),
                eta_seconds: Some(90),
                ..Default::default()
            })
        })
    });
    g.case("play, downloading", &queued, "GET", &other, &[]).await;
    let whole = handler(|d| {
        d.make_stores = store(|s| s.status = Some(StoreStatus { progress: 0.0, ..Default::default() }))
    });
    g.case("play, downloading at zero", &whole, "GET", &other, &[]).await;
    let refused = handler(|d| {
        d.make_stores = store(|s| {
            s.resolve = Arc::new(|| {
                Err(StoreError::Unavailable(StoreUnavailableError {
                    service: DebridService::TorBox,
                    reason: "429".to_owned(),
                    status: 429,
                }))
            })
        })
    });
    g.case("play, store refused", &refused, "GET", &other, &[]).await;

    // ---- cancel, account, validate, availability -------------------------------------------------------------
    g.case("cancel, legacy route, no store can", &h, "DELETE", &play, &[]).await;
    g.case("cancel, bad token", &h, "DELETE", &format!("/{cfg}/play/nope"), &[]).await;
    g.case("DELETE elsewhere", &h, "DELETE", "/health", &[]).await;
    g.case("account", &h, "GET", &format!("/{cfg}/account"), &[]).await;
    g.case("account, bad config", &h, "GET", "/not-a-config/account", &[]).await;
    g.case("validate GET", &h, "GET", "/validate", &[]).await;
    g.case_with_body("validate, not JSON", &h, "POST", "/validate", &[], "nope").await;
    g.case_with_body(
        "validate, unknown service",
        &h,
        "POST",
        "/validate",
        &[],
        r#"{"service":"x","token":"t"}"#,
    )
    .await;
    let ok = r#"{"service":"torbox","token":"t"}"#;
    g.case_with_body("validate, no client to ask with", &h, "POST", "/validate", &[], ok).await;
    let avail = format!("/{cfg}/availability");
    g.case_with_body("availability", &h, "POST", &avail, &[], r#"{"ids":["tt1234567"]}"#).await;
    g.case("availability GET", &h, "GET", &avail, &[]).await;
    g.case_with_body("availability, bad id", &h, "POST", &avail, &[], r#"{"ids":["nope"]}"#).await;
    g.case_with_body("availability, not JSON", &h, "POST", &avail, &[], "nope").await;
    g.case_with_body("availability, bad config", &h, "POST", "/not-a-config/availability", &[], "{}").await;
    g.case_with_body("availability, too deep", &h, "POST", &format!("/{cfg}/x/availability"), &[], "{}")
        .await;

    // ---- with keys: tickets, metrics, den-remux --------------------------------------------------------------
    let kr = ticket_keyring();
    let keyed = handler(|d| {
        d.seal_keyring = Some(Arc::new(ticket_keyring()));
        d.metrics_token = "metrics-token".to_owned();
        d.remux_key = "remux-key".to_owned();
    });
    g.case("metrics, refused", &keyed, "GET", "/metrics", &[("authorization", "Bearer wrong")]).await;
    g.case("metrics, bare token", &keyed, "GET", "/metrics", &[("authorization", "metrics-token")]).await;
    g.case("metrics", &keyed, "GET", "/metrics", &[("authorization", "Bearer metrics-token")]).await;
    let sealed = sealed_config(
        &kr,
        r#"{"debrid":[{"service":"torbox","token":"tb-secret"}],"indexers":["torrentio"],"cachedOnly":true}"#,
    );
    g.aliases.push((sealed.clone(), "<sealed config>"));
    let keyed_list = format!("/{sealed}/stream/movie/tt1234567.json");
    g.case("list with tickets", &keyed, "GET", &keyed_list, &[]).await;
    g.case("plaintext config while sealing is on", &keyed, "GET", &list, &[]).await;
    let config = crate::config::decode_config(Some(&kr), &sealed).expect("the sealed config opens");
    let target = PlayTarget { info_hash: "a".repeat(40), file_idx: Some(0), ..Default::default() };
    let tickets = ticket_keys(&kr);
    let ticket = tickets.mint(&config, &target, SystemTime::now() + Duration::from_secs(3600));
    let expired = tickets.mint(&config, &target, SystemTime::now() - Duration::from_secs(1));
    g.case("ticket, resolved", &keyed, "GET", &format!("/p/{ticket}"), &[]).await;
    g.case("ticket, HEAD", &keyed, "HEAD", &format!("/p/{ticket}"), &[]).await;
    g.case("ticket, garbage", &keyed, "GET", "/p/garbage", &[]).await;
    g.case("ticket, expired", &keyed, "GET", &format!("/p/{expired}"), &[]).await;
    g.case("ticket, probe", &keyed, "GET", &format!("/p/{ticket}?probe=1"), &[]).await;
    g.case("ticket cancel, no store can", &keyed, "DELETE", &format!("/p/{ticket}"), &[]).await;
    g.case("ticket cancel, expired", &keyed, "DELETE", &format!("/p/{expired}"), &[]).await;
    g.case("ticket cancel, garbage", &keyed, "DELETE", "/p/garbage", &[]).await;
    g.case("/p/ without tickets is a config segment", &h, "GET", "/p/garbage", &[]).await;
    let scoped_sealed = sealed_config(
        &kr,
        r#"{"debrid":[{"service":"torbox","token":"tb-secret"}],"indexers":["torrentio"],"scope":"availability"}"#,
    );
    g.aliases.push((scoped_sealed.clone(), "<sealed scoped config>"));
    let scoped_list = format!("/{scoped_sealed}/stream/movie/tt1234567.json");
    g.case("scoped list, refused", &keyed, "GET", &scoped_list, &[]).await;
    g.case("scoped list, den-remux", &keyed, "GET", &scoped_list, &[("x-den-remux-key", "remux-key")]).await;
    g.case("scoped list, wrong remux key", &keyed, "GET", &scoped_list, &[("x-den-remux-key", "nope")]).await;

    // Tickets on, and the legacy route's allowance over.
    let closed = handler(|d| {
        d.seal_keyring = Some(Arc::new(ticket_keyring()));
        d.legacy_play_until = Some(SystemTime::now() - Duration::from_secs(1));
    });
    g.case("legacy play closed", &closed, "GET", &format!("/{sealed}/play/{}", token('a')), &[]).await;
    g.case("legacy cancel closed", &closed, "DELETE", &format!("/{sealed}/play/{}", token('a')), &[]).await;

    // ---- CONFIG_KEY set but malformed: fail closed (#27) -------------------------------------------------------
    let bad_key = handler(|d| d.config_key_invalid = true);
    for (name, method, path) in [
        ("bad key: health", "GET", "/health"),
        ("bad key: list", "GET", &list),
        ("bad key: ticket", "GET", "/p/garbage"),
        ("bad key: cancel", "DELETE", &play),
        ("bad key: availability", "POST", &avail),
        ("bad key: preflight", "OPTIONS", &list),
    ] {
        g.case(name, &bad_key, method, path, &[]).await;
    }
    g.case_with_body("bad key: validate", &bad_key, "POST", "/validate", &[], "nope").await;

    // ---- the request log ---------------------------------------------------------------------------------------
    let log = crate::loglimit::Capture::start();
    let logged = handler(|d| {
        d.log_requests = true;
        d.seal_keyring = Some(Arc::new(ticket_keyring()));
    });
    let rid = [("x-request-id", "abc-123")];
    let hostile = [("x-request-id", "x y=z<>")];
    for (method, path, headers) in [
        ("GET", "/health", &rid[..]),
        ("GET", "/nope", &hostile[..]),
        ("GET", &format!("{keyed_list}?debug=1&token=secret"), &rid[..]),
        ("GET", &format!("/p/{ticket}?fresh=1"), &rid[..]),
        ("DELETE", &format!("/p/{expired}"), &[][..]),
        ("GET", &format!("/{sealed}/play/{}?probe=1", token('a')), &[][..]),
        ("GET", "//p//garbage", &[][..]),
        ("GET", "/cfg/stream/movie/tt1%0AGET%20/health.json", &[][..]),
        ("OPTIONS", "/x", &[][..]),
    ] {
        let mut req = http::Request::builder()
            .method(http::Method::from_bytes(method.as_bytes()).unwrap())
            .uri(format!("https://scout.example{path}"));
        for (k, v) in headers {
            req = req.header(*k, *v);
        }
        send(&logged, req.body(Vec::new()).unwrap()).await;
    }
    let lines = log.text();
    drop(log);
    let request_lines = Regex::new(r"^[A-Z]+ /").unwrap();
    let millis = Regex::new(r" \d+ms").unwrap();
    let _ = writeln!(g.out, "== request log");
    for line in lines.lines().filter(|l| request_lines.is_match(l)) {
        let _ = writeln!(g.out, "{}", millis.replace(line, " *ms"));
    }
    g.out
}

#[tokio::test]
async fn every_route_answers_as_it_did_before_the_rewrite() {
    let got = render().await;
    if std::env::var_os("SCOUT_GOLDEN_WRITE").is_some() {
        let path = concat!(env!("CARGO_MANIFEST_DIR"), "/src/tests/golden_http.txt");
        std::fs::write(path, &got).expect("write the golden file");
        return;
    }
    if got != GOLDEN {
        let first = got.lines().zip(GOLDEN.lines()).position(|(a, b)| a != b).unwrap_or(0);
        let context =
            |s: &str| s.lines().skip(first.saturating_sub(8)).take(16).collect::<Vec<_>>().join("\n");
        panic!(
            "the wire contract changed at line {}:\n--- golden\n{}\n--- now\n{}",
            first + 1,
            context(GOLDEN),
            context(&got)
        );
    }
}

/// The whole /metrics exposition of a fresh process, every family and label in order, pinned the same way.
#[tokio::test]
async fn the_metrics_text_is_what_it_was() {
    let runtime = crate::runtime::ScoutRuntime::new();
    let got = crate::runtime::scope(runtime, async { crate::metrics::metrics().render(-1) }).await;
    let got = got.replace(crate::manifest::manifest_version(), "<version>");
    if std::env::var_os("SCOUT_GOLDEN_WRITE").is_some() {
        let path = concat!(env!("CARGO_MANIFEST_DIR"), "/src/tests/golden_metrics.txt");
        std::fs::write(path, &got).expect("write the golden file");
        return;
    }
    assert_eq!(got, include_str!("golden_metrics.txt"), "the /metrics text changed");
}
