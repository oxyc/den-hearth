//! Go: `internal/scout/handler_test.go` (2429 lines). Work package 4.
//!
//! Also home to the test doubles and request helpers the other package-4 test modules share (Go keeps them
//! in `handler_test.go`, `scrape_test.go` and `stores_test.go`): `FakeScraper` (scrape_test.go's `fakeScraper`),
//! `FakeStore` (stores_test.go's `fakeStore`), and the `blob`/`ticket` helpers that `ticket_test.go` owns.
//! Package 5 and 6 will port their own copies in their modules; these are the package 4 stand-ins.

use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicI32, Ordering};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;

use crate::cache::{Cache, MemoryCache};
use crate::config::{DebridService, Indexer};
use crate::deps::Deps;
use crate::handler::{new_handler, Handler};
use crate::rank::RawStream;
use crate::scrape::{DoError, ScrapeQuery, Scraper};
use crate::seal::{parse_seal_keyring, SealKeyring};
use crate::stores::{DeadLinkError, ResolveTarget, Store, StoreError, StoreStatus, ERR_CHECK_FAILED};
use crate::util::b64url_encode;

/// `gib` in handler_test.go.
const GIB: i64 = 1 << 30;

/// The recipient X25519 private key of seal_test.go (seed 00..1f), and its public key.
pub(crate) const VEC_PRIV_B64: &str = "AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8=";
pub(crate) const VEC_PUB_B64: &str = "j0DFrbaPJWJK5bIU6nZ6bslNgp09e14a0bpvPiE4KF8=";

/// `ticket_test.go`'s `ticketCfg`: unterminated, so a test can append iid/ep/scope before closing it.
pub(crate) const TICKET_CFG: &str =
    r#"{"debrid":[{"service":"torbox","token":"tb-secret"}],"indexers":["torrentio"],"cachedOnly":true"#;

/// `ticket_test.go`'s `testIID` and `otherIID`: `b64urlEncode(bytesSeq(0x00|0x10, installIDBytes))`.
pub(crate) fn test_iid() -> String {
    b64url_encode(&(0u8..16).collect::<Vec<u8>>())
}

pub(crate) fn other_iid() -> String {
    b64url_encode(&(0x10u8..0x20).collect::<Vec<u8>>())
}

/// `ticket_test.go`'s `ticketKeyring`.
pub(crate) fn ticket_keyring() -> SealKeyring {
    parse_seal_keyring(VEC_PRIV_B64, "").expect("the test key parses").expect("the test key is set")
}

/// `handler_test.go`'s `validBlob`: a TorBox config with one indexer, CAM filtered, cached only.
pub(crate) fn valid_blob() -> String {
    blob(r#"{"debrid":[{"service":"torbox","token":"tb-secret"}],"indexers":["torrentio"],"filters":{"excludeCam":true},"cachedOnly":true,"resultCap":20}"#)
}

/// `core_test.go`'s `blob`: a config's JSON as the base64url path segment.
pub(crate) fn blob(json: &str) -> String {
    b64url_encode(json.as_bytes())
}

/// `handler_test.go`'s `testSeeds`: three releases, two 4K/1080p and one CAM.
pub(crate) fn test_seeds() -> Vec<RawStream> {
    vec![
        RawStream {
            info_hash: "a".repeat(40),
            title: "Movie 2160p WEB-DL HDR".to_owned(),
            size_bytes: Some(18 * GIB),
            seeders: Some(100),
            file_idx: Some(0),
            ..RawStream::default()
        },
        RawStream {
            info_hash: "b".repeat(40),
            title: "Movie 1080p WEB-DL".to_owned(),
            size_bytes: Some(8 * GIB),
            seeders: Some(50),
            file_idx: Some(0),
            ..RawStream::default()
        },
        RawStream {
            info_hash: "c".repeat(40),
            title: "Movie 2160p HDCAM".to_owned(),
            size_bytes: Some(2 * GIB),
            seeders: Some(3),
            ..RawStream::default()
        },
    ]
}

/// `handler_test.go`'s `testDeps`: a handler whose one scraper answers `test_seeds` and whose one TorBox store
/// caches the first two hashes, then `over` adjusts it.
pub(crate) fn test_deps(over: impl FnOnce(&mut Deps)) -> Deps {
    let mut d = Deps {
        cache: Arc::new(MemoryCache::new(1 << 20)),
        probe_client: None,
        scrape_timeout: Duration::from_secs(1),
        list_ttl: Duration::from_secs(5 * 60),
        public_url: String::new(),
        make_scrapers: Arc::new(|_| vec![fake_scraper("torrentio", || Box::pin(async { Ok(test_seeds()) }))]),
        make_stores: Arc::new(|_| {
            vec![Box::new(FakeStore {
                svc: DebridService::TorBox,
                check: HashMap::from([
                    ("a".repeat(40), true),
                    ("b".repeat(40), true),
                    ("c".repeat(40), false),
                ]),
                check_err: None,
                resolve: Arc::new(|| Ok(format!("https://cdn.torbox/{}.mkv", "a".repeat(40)))),
                status: None,
                status_blocks: false,
            })]
        }),
        seal_keyring: None,
        metrics_token: String::new(),
        log_requests: false,
        revoked_installs: HashSet::new(),
        config_epoch: 0,
        require_install_id: false,
        remux_key: String::new(),
        play_ticket_ttl: crate::ticket::DEFAULT_PLAY_TICKET_TTL,
        legacy_play_until: None,
    };
    over(&mut d);
    d
}

/// The future a `FakeScraper`'s behaviour returns, so a test can answer late, hang, or fail.
pub(crate) type FakeFuture = Pin<Box<dyn Future<Output = Result<Vec<RawStream>, DoError>> + Send>>;

/// scrape_test.go's `fakeScraper`: an indexer whose answer is whatever its closure does when asked.
pub(crate) struct FakeScraper {
    id: &'static str,
    behave: Arc<dyn Fn() -> FakeFuture + Send + Sync>,
}

/// Builds a boxed `FakeScraper` for `make_scrapers`.
pub(crate) fn fake_scraper(id: &'static str, behave: impl Fn() -> FakeFuture + Send + Sync + 'static) -> Box<dyn Scraper> {
    Box::new(FakeScraper { id, behave: Arc::new(behave) })
}

#[async_trait]
impl Scraper for FakeScraper {
    fn id(&self) -> Indexer {
        Indexer(self.id.to_owned())
    }

    async fn scrape(&self, _q: &ScrapeQuery) -> Result<Vec<RawStream>, DoError> {
        (self.behave)().await
    }
}

/// stores_test.go's `fakeStore`: a store that answers from fixed fields.
pub(crate) struct FakeStore {
    pub svc: DebridService,
    /// The cache check's answer, when it succeeds.
    pub check: HashMap<String, bool>,
    pub check_err: Option<StoreError>,
    pub resolve: Arc<dyn Fn() -> Result<String, StoreError> + Send + Sync>,
    pub status: Option<StoreStatus>,
    /// statusBlocks makes Status outlast whatever budget it is given, the way a slow account listing does.
    pub status_blocks: bool,
}

#[async_trait]
impl Store for FakeStore {
    fn service(&self) -> DebridService {
        self.svc
    }

    async fn cache_check(&self, _hashes: &[String]) -> Result<HashMap<String, bool>, StoreError> {
        match &self.check_err {
            Some(e) => Err(e.clone()),
            None => Ok(self.check.clone()),
        }
    }

    async fn resolve(&self, _t: &ResolveTarget) -> Result<String, StoreError> {
        (self.resolve)()
    }

    async fn status(&self, _t: &ResolveTarget) -> Option<StoreStatus> {
        if self.status_blocks {
            std::future::pending::<()>().await;
        }
        self.status.clone()
    }
}

/// handler_test.go's `countingStore`: counts resolves so a test can prove a route never reached the debrid.
pub(crate) struct CountingStore {
    pub inner: Box<dyn Store>,
    pub resolved: Arc<AtomicI32>,
}

#[async_trait]
impl Store for CountingStore {
    fn service(&self) -> DebridService {
        self.inner.service()
    }

    async fn cache_check(&self, hashes: &[String]) -> Result<HashMap<String, bool>, StoreError> {
        self.inner.cache_check(hashes).await
    }

    async fn resolve(&self, t: &ResolveTarget) -> Result<String, StoreError> {
        self.resolved.fetch_add(1, Ordering::SeqCst);
        self.inner.resolve(t).await
    }

    async fn status(&self, t: &ResolveTarget) -> Option<StoreStatus> {
        self.inner.status(t).await
    }
}

/// A dead link, as a fake store's `resolve` returns it for a release nothing can deliver.
pub(crate) fn dead_link(reason: &str) -> StoreError {
    StoreError::DeadLink(DeadLinkError { reason: reason.to_owned() })
}

/// handler_test.go's `do`: a GET with the given headers.
pub(crate) async fn get(h: &Handler, path: &str, headers: &[(&str, &str)]) -> http::Response<Vec<u8>> {
    method(h, http::Method::GET, path, headers).await
}

/// handler_test.go's `postJSON`.
pub(crate) async fn post_json(h: &Handler, path: &str, body: &str) -> http::Response<Vec<u8>> {
    let req = http::Request::builder()
        .method(http::Method::POST)
        .uri(format!("https://scout.example{path}"))
        .header("content-type", "application/json")
        .body(body.as_bytes().to_vec())
        .expect("a valid request");
    h.serve(req).await
}

/// handler_test.go's `doMethod`.
pub(crate) async fn method(
    h: &Handler,
    m: http::Method,
    path: &str,
    headers: &[(&str, &str)],
) -> http::Response<Vec<u8>> {
    let mut b = http::Request::builder().method(m).uri(format!("https://scout.example{path}"));
    for (k, v) in headers {
        b = b.header(*k, *v);
    }
    h.serve(b.body(Vec::new()).expect("a valid request")).await
}

/// A response header's value, or "" when absent, as Go's `Header().Get` returns it.
pub(crate) fn hdr(r: &http::Response<Vec<u8>>, name: &str) -> String {
    r.headers().get(name).and_then(|v| v.to_str().ok()).unwrap_or("").to_owned()
}

/// A response body as text.
pub(crate) fn body_str(r: &http::Response<Vec<u8>>) -> String {
    String::from_utf8_lossy(r.body()).into_owned()
}

/// handler_test.go's `streamsLen`: how many streams a list response carries.
pub(crate) fn streams_len(r: &http::Response<Vec<u8>>) -> usize {
    let v: serde_json::Value = serde_json::from_slice(r.body()).unwrap_or(serde_json::Value::Null);
    v.get("streams").and_then(|s| s.as_array()).map_or(0, Vec::len)
}

/// The play URL's path from a stream list's first entry, from its `/<config>/play/` (or `/p/`) segment on.
pub(crate) fn first_play_path(body: &str, marker: &str) -> String {
    let v: serde_json::Value = serde_json::from_str(body).expect("a JSON stream list");
    let url = v["streams"][0]["url"].as_str().expect("a stream url").to_owned();
    url[url.find(marker).expect("the play marker")..].to_owned()
}

#[tokio::test]
async fn routes_pages_and_manifest() {
    let h = new_handler(test_deps(|_| {}));
    let r = get(&h, "/configure", &[]).await;
    let b = body_str(&r);
    assert!(
        r.status() == 200 && b.contains("Den Scout") && b.contains("DenSeal") && hdr(&r, "cache-control") == crate::handler::STATIC_CACHE,
        "configure: {} cc={:?}",
        r.status(),
        hdr(&r, "cache-control")
    );
    let r = get(&h, "/health", &[]).await;
    assert!(r.status() == 200 && hdr(&r, "cache-control") == crate::handler::NO_STORE, "health");
    let r = get(&h, "/manifest.json", &[]).await;
    assert!(r.status() == 200 && body_str(&r).contains(r#""configurationRequired":true"#), "unconfigured manifest: {}", body_str(&r));
    let r = get(&h, &format!("/{}/manifest.json", valid_blob()), &[]).await;
    assert!(r.status() == 200 && body_str(&r).contains(r#""configurationRequired":false"#), "configured manifest");
    assert_eq!(get(&h, "/@@@/manifest.json", &[]).await.status(), 400, "bad config");
    assert_eq!(get(&h, &format!("/{}/bogus", valid_blob()), &[]).await.status(), 404, "unknown resource");
}

#[tokio::test]
async fn routes_sealed_config() {
    let pub_key: [u8; 32] = crate::seal::decode_key_b64(VEC_PUB_B64).expect("the test key decodes").try_into().expect("32 bytes");
    let kr = parse_seal_keyring(VEC_PRIV_B64, "").expect("parses").expect("set");
    // Same config as valid_blob(), but sealed to the addon's key.
    let cfg_json = br#"{"debrid":[{"service":"torbox","token":"tb-secret"}],"indexers":["torrentio"],"filters":{"excludeCam":true},"cachedOnly":true,"resultCap":20}"#;
    let sealed = crate::seal::seal(&pub_key, cfg_json).expect("seals");
    let mut framed = vec![crate::seal::SEALED_VERSION];
    framed.extend_from_slice(&sealed);
    let seg = b64url_encode(&framed);
    let h = new_handler(test_deps(|d| d.seal_keyring = Some(Arc::new(kr))));

    // /config-key serves the current public key so a client can seal to it.
    let r = get(&h, "/config-key", &[]).await;
    assert!(r.status() == 200 && body_str(&r).contains(VEC_PUB_B64), "config-key: {} {}", r.status(), body_str(&r));
    // A SEALED URL resolves manifest + streams identically to the plaintext blob.
    let r = get(&h, &format!("/{seg}/manifest.json"), &[]).await;
    assert!(r.status() == 200 && body_str(&r).contains(r#""configurationRequired":false"#), "sealed manifest: {} {}", r.status(), body_str(&r));
    let r = get(&h, &format!("/{seg}/stream/movie/tt1234567.json"), &[]).await;
    assert!(r.status() == 200 && streams_len(&r) == 2, "sealed stream: {} n={}", r.status(), streams_len(&r));
    // Fail CLOSED: a sealed URL against a handler with no keyring → 400; /config-key → 404.
    let no_key = new_handler(test_deps(|_| {}));
    assert_eq!(get(&no_key, &format!("/{seg}/manifest.json"), &[]).await.status(), 400, "sealed with no keyring");
    assert_eq!(get(&no_key, "/config-key", &[]).await.status(), 404, "config-key without keyring");
    // With a keyring configured, plaintext is refused: every issued install is sealed then.
    assert_eq!(get(&h, &format!("/{}/manifest.json", valid_blob()), &[]).await.status(), 400, "plaintext with a keyring");
    assert_eq!(get(&no_key, &format!("/{}/manifest.json", valid_blob()), &[]).await.status(), 200, "plaintext with no keyring");
}

/// The configured manifest names its install as REVOKED_INSTALLS spells it; a config without an id, and the
/// unconfigured manifest, carry no such field. The ETag is over the body, so one install's validator never
/// earns another install a 304.
#[tokio::test]
async fn manifest_names_the_install() {
    let kr = ticket_keyring();
    let with_id = sealed_config(&kr, &format!(r#"{TICKET_CFG},"iid":"{}"}}"#, test_iid()));
    let other = sealed_config(&kr, &format!(r#"{TICKET_CFG},"iid":"{}"}}"#, other_iid()));
    let without = sealed_config(&kr, &format!("{TICKET_CFG}}}"));
    let h = new_handler(test_deps(|d| d.seal_keyring = Some(Arc::new(ticket_keyring()))));

    let install_id = |r: &http::Response<Vec<u8>>| -> serde_json::Value {
        let m: serde_json::Value = serde_json::from_slice(r.body()).expect("the manifest is JSON");
        m.get("denInstallId").cloned().unwrap_or(serde_json::Value::Null)
    };
    let first = get(&h, &format!("/{with_id}/manifest.json"), &[]).await;
    assert!(first.status() == 200 && install_id(&first) == test_iid(), "with an iid: {} {}", first.status(), body_str(&first));
    for (name, path) in [("no iid", format!("/{without}/manifest.json")), ("unconfigured", "/manifest.json".to_owned())] {
        let r = get(&h, &path, &[]).await;
        assert!(r.status() == 200 && !body_str(&r).contains("denInstallId"), "{name}: {} {}", r.status(), body_str(&r));
    }
    let etag = hdr(&first, "etag");
    let r = get(&h, &format!("/{other}/manifest.json"), &[("If-None-Match", &etag)]).await;
    assert!(r.status() == 200 && install_id(&r) == other_iid(), "another install with the first one's ETag: {} {}", r.status(), body_str(&r));
}

/// A configured manifest's URL is the install credential, so it is never cached where anyone else could be
/// handed it; the config-less routes stay publicly cacheable.
#[tokio::test]
async fn manifest_configured_is_private_and_the_rest_public() {
    let h = new_handler(test_deps(|_| {}));
    let cases = [
        (format!("/{}/manifest.json", valid_blob()), "private, max-age=3600, stale-while-revalidate=600"),
        ("/manifest.json".to_owned(), "public, max-age=3600, stale-while-revalidate=600"),
        ("/configure".to_owned(), "public, max-age=3600, stale-while-revalidate=600"),
    ];
    for (path, want) in cases {
        let r = get(&h, &path, &[]).await;
        assert!(r.status() == 200 && hdr(&r, "cache-control") == want, "{path}: {} cache-control {:?}, want {want:?}", r.status(), hdr(&r, "cache-control"));
        // A revalidation answers with the same header.
        let again = get(&h, &path, &[("If-None-Match", &hdr(&r, "etag"))]).await;
        assert!(hdr(&again, "cache-control") == want, "{path} 304: cache-control {:?}, want {want:?}", hdr(&again, "cache-control"));
    }
}

/// ETags are 64-bit: sixteen hex digits, quoted.
#[test]
fn etag_is64_bit() {
    for body in ["", r#"{"streams":[]}"#] {
        let tag = crate::handler::etag_for(body);
        let inner = &tag[1..tag.len().saturating_sub(1)];
        assert!(
            tag.len() == 18 && tag.starts_with('"') && tag.ends_with('"') && inner.bytes().all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase()),
            "etag for {body:?} = {tag}, want 16 lowercase hex digits in quotes"
        );
    }
    assert_ne!(crate::handler::etag_for("a"), crate::handler::etag_for("b"), "two bodies share an etag");
}

/// /configure shows the id of the link it just built, with the variable that revokes it.
#[tokio::test]
async fn configure_page_shows_the_install_id() {
    let h = new_handler(test_deps(|_| {}));
    let page = body_str(&get(&h, "/configure", &[]).await);
    for want in [
        r#"<code id="installId"></code>"#,
        "lastInstallID = config.iid;",
        r#"$("installId").textContent = lastInstallID;"#,
        "<code>REVOKED_INSTALLS</code> to revoke just this link",
    ] {
        assert!(page.contains(want), "configure page lacks {want:?}");
    }
}

#[tokio::test]
async fn routes_stream() {
    let h = new_handler(test_deps(|_| {}));
    let r = get(&h, &format!("/{}/stream/movie/tt1234567.json", valid_blob()), &[]).await;
    assert_eq!(r.status(), 200, "stream");
    assert!(
        hdr(&r, "cache-control").starts_with("private, max-age=300, stale-while-revalidate=300"),
        "stream cache-control: {:?}",
        hdr(&r, "cache-control")
    );
    // CAM dropped, cached only, 4K then 1080p.
    let body: serde_json::Value = serde_json::from_slice(r.body()).expect("a stream list");
    let streams = body["streams"].as_array().expect("streams");
    assert_eq!(streams.len(), 2, "want 2 streams");
    let first = &streams[0];
    assert_eq!(first["name"], "Den Scout");
    assert_eq!(first["title"], "Movie 2160p WEB-DL HDR");
    assert_eq!(first["behaviorHints"]["bingeGroup"], "den-scout|2160p|webdl|hdr|");
    // The release name is how Den recognises a release across resolves once play URLs are per-list tickets.
    assert_eq!(first["behaviorHints"]["filename"], "Movie 2160p WEB-DL HDR");
    let url = first["url"].as_str().expect("a play url");
    assert!(url.contains(&format!("/{}/play/", valid_blob())) && !url.contains("tb-secret"), "play url: {url}");
    let tok = &url[url.rfind("/play/").expect("a play segment") + "/play/".len()..];
    let pt = crate::play::decode_play_token(tok).expect("the play token decodes");
    assert_eq!(pt.info_hash, "a".repeat(40), "play token");
    assert_eq!(first["attributes"]["resolution"], "2160p", "attributes");
    assert_eq!(get(&h, &format!("/{}/stream/movie/nope.json", valid_blob()), &[]).await.status(), 400, "bad id");
}

/// The handler dispatched on PATH alone, so /play answered any verb — and /play resolves, which ADDS an
/// uncached release against a fifty-an-hour allowance. A link unfurler's HEAD or a crawler's POST reached
/// the one route this package works hardest to keep accidental callers out of.
#[tokio::test]
async fn routes_reject_non_read_methods() {
    let resolves = Arc::new(AtomicI32::new(0));
    let counter = resolves.clone();
    let h = new_handler(test_deps(move |d| {
        let inner = d.make_stores.clone();
        let counter = counter.clone();
        d.make_stores = Arc::new(move |c| {
            let stores = inner(c);
            vec![Box::new(CountingStore { inner: stores.into_iter().next().expect("a store"), resolved: counter.clone() })]
        });
    }));

    // Get a real play token the honest way, so the route under test is the one clients actually hit.
    let r = get(&h, &format!("/{}/stream/movie/tt1234567.json", valid_blob()), &[]).await;
    let play_path = first_play_path(&body_str(&r), &valid_blob());

    // GET is the one verb that may resolve.
    let got = get(&h, &play_path, &[]).await;
    assert_eq!(got.status(), 302, "GET /play");
    assert_eq!(resolves.load(Ordering::SeqCst), 1, "GET /play resolved");

    // HEAD is refused, not answered: what a caller wants here is a `location`, and a HEAD that starts nothing
    // cannot produce one — so answering it would spend upstream reads for a useless reply.
    for m in [http::Method::HEAD, http::Method::POST, http::Method::PUT] {
        let got = method(&h, m.clone(), &play_path, &[]).await;
        assert_eq!(got.status(), 405, "{m} /play");
        assert!(!hdr(&got, "allow").is_empty(), "{m} /play: 405 without an Allow header");
    }
    // DELETE is den#204's cancel route, not a plain read: it is handled, never 405'd — and against a store
    // that cannot cancel (the fake here has no Cancel method) it answers 501, not a resolve.
    assert_eq!(method(&h, http::Method::DELETE, &play_path, &[]).await.status(), 501, "DELETE /play");
    assert_eq!(resolves.load(Ordering::SeqCst), 1, "a non-GET verb reached the debrid");

    // Every other route is a plain read: POST is refused, HEAD is served.
    for path in [
        "/".to_owned(),
        "/configure".to_owned(),
        "/health".to_owned(),
        "/manifest.json".to_owned(),
        format!("/{}/manifest.json", valid_blob()),
        format!("/{}/stream/movie/tt1234567.json", valid_blob()),
    ] {
        assert_eq!(method(&h, http::Method::POST, &path, &[]).await.status(), 405, "POST {path}");
        assert_eq!(method(&h, http::Method::HEAD, &path, &[]).await.status(), 200, "HEAD {path}");
    }
}

/// A preflight is answered on every path — including /play, which refuses every other non-GET verb, and paths
/// that do not exist — and every real response, error or not, is readable cross-origin.
#[tokio::test]
async fn cors() {
    let h = new_handler(test_deps(|_| {}));
    for path in [
        "/".to_owned(),
        "/health".to_owned(),
        "/validate".to_owned(),
        "/metrics".to_owned(),
        "/nope".to_owned(),
        format!("/{}/stream/movie/tt1234567.json", valid_blob()),
        format!("/{}/play/anything", valid_blob()),
    ] {
        let r = method(&h, http::Method::OPTIONS, &path, &[]).await;
        assert!(
            r.status() == 204
                && r.body().is_empty()
                && hdr(&r, "access-control-allow-origin") == "*"
                && hdr(&r, "access-control-allow-methods") == "GET, HEAD, POST, DELETE, OPTIONS"
                && hdr(&r, "access-control-allow-headers") == "*"
                && hdr(&r, "access-control-max-age") == "86400",
            "OPTIONS {path}: {}",
            r.status()
        );
    }
    for (m, path, code) in [
        (http::Method::GET, "/manifest.json", 200),
        (http::Method::GET, "/health", 200),
        (http::Method::GET, "/@@@/manifest.json", 400),
        (http::Method::GET, "/nope", 404),
        (http::Method::POST, "/health", 405),
    ] {
        let r = method(&h, m.clone(), path, &[]).await;
        assert!(
            r.status().as_u16() == code && hdr(&r, "access-control-allow-origin") == "*",
            "{m} {path}: {} acao={:?}, want {code} with acao *",
            r.status(),
            hdr(&r, "access-control-allow-origin")
        );
    }
}

/// One 404 for everything that is not here, byte for byte the same across den addons — and /metrics without its
/// token is indistinguishable from a path that does not exist.
#[tokio::test]
async fn not_found_body() {
    let open = new_handler(test_deps(|_| {}));
    let gated = new_handler(test_deps(|d| d.metrics_token = "secret".to_owned()));
    let cases = [
        (&open, "/nope".to_owned()),
        (&open, "/a/b/c".to_owned()),
        (&open, format!("/{}/bogus", valid_blob())),
        (&open, "/metrics".to_owned()),
        (&gated, "/metrics".to_owned()),
    ];
    for (h, path) in cases {
        let r = get(h, &path, &[]).await;
        assert!(
            r.status() == 404
                && hdr(&r, "content-type") == crate::handler::JSON_TYPE
                && hdr(&r, "cache-control") == crate::handler::NO_STORE
                && body_str(&r) == r#"{"error":"not_found"}"#,
            "{path}: {} ct={:?} cc={:?} body={}",
            r.status(),
            hdr(&r, "content-type"),
            hdr(&r, "cache-control"),
            body_str(&r)
        );
    }
}

/// Server-Timing names the phases a response actually paid for: a built list its scrape and cache check, a
/// cached one where it came from, a play its resolve — and each its total.
#[tokio::test]
async fn server_timing() {
    let h = new_handler(test_deps(|_| {}));
    let path = format!("/{}/stream/movie/tt1234567.json", valid_blob());

    let built = get(&h, &path, &[]).await;
    let st = hdr(&built, "server-timing");
    for want in ["scrape;dur=", "cache-check;dur=", "total;dur="] {
        assert!(st.contains(want), "a built list's Server-Timing {st:?} lacks {want}");
    }
    // testDeps sets no probe client, so no probe ran and none is claimed.
    assert!(!st.contains("probe"), "a probe phase was named when none ran: {st:?}");

    let cached = get(&h, &path, &[]).await;
    assert!(hdr(&cached, "server-timing").starts_with("cache;desc=hit, total;dur="), "a cached list's Server-Timing = {:?}", hdr(&cached, "server-timing"));

    let play_path = first_play_path(&body_str(&built), &valid_blob());
    let play = get(&h, &play_path, &[]).await;
    let st = hdr(&play, "server-timing");
    assert!(
        play.status() == 302 && st.starts_with("resolve;dur=") && st.contains(", total;dur="),
        "play: {} Server-Timing {st:?}",
        play.status()
    );
}

#[tokio::test]
async fn routes_etag_and_singleflight() {
    let h = new_handler(test_deps(|_| {}));
    let first = get(&h, &format!("/{}/manifest.json", valid_blob()), &[]).await;
    let etag = hdr(&first, "etag");
    assert!(!etag.is_empty(), "no etag");
    let second = get(&h, &format!("/{}/manifest.json", valid_blob()), &[("If-None-Match", &etag)]).await;
    assert!(second.status() == 304 && second.body().is_empty(), "expected 304 empty, got {} len={}", second.status(), second.body().len());

    // singleflight: concurrent misses for the same title share one scrape.
    let count = Arc::new(AtomicI32::new(0));
    let c = count.clone();
    let h2 = new_handler(test_deps(move |d| {
        let c = c.clone();
        d.make_scrapers = Arc::new(move |_| {
            let c = c.clone();
            vec![fake_scraper("torrentio", move || {
                let c = c.clone();
                Box::pin(async move {
                    c.fetch_add(1, Ordering::SeqCst);
                    tokio::time::sleep(Duration::from_millis(25)).await;
                    Ok(test_seeds())
                })
            })]
        });
    }));
    let path = format!("/{}/stream/movie/tt777.json", valid_blob());
    let _ = tokio::join!(get(&h2, &path, &[]), get(&h2, &path, &[]));
    assert_eq!(count.load(Ordering::SeqCst), 1, "singleflight: scrape ran more than once");
}

#[tokio::test]
async fn routes_play() {
    let h = new_handler(test_deps(|_| {}));
    let stream_rr = get(&h, &format!("/{}/stream/movie/tt1.json", valid_blob()), &[]).await;
    let play_path = first_play_path(&body_str(&stream_rr), &valid_blob());

    let rr = get(&h, &play_path, &[]).await;
    assert!(
        rr.status() == 302 && hdr(&rr, "location") == format!("https://cdn.torbox/{}.mkv", "a".repeat(40)),
        "play 302: {} loc={:?}",
        rr.status(),
        hdr(&rr, "location")
    );
    assert!(
        hdr(&rr, "content-length") == "0" && hdr(&rr, "cache-control") == crate::handler::NO_STORE && hdr(&rr, "content-type").is_empty(),
        "play 302 headers: cl={:?} cc={:?} ct={:?}",
        hdr(&rr, "content-length"),
        hdr(&rr, "cache-control"),
        hdr(&rr, "content-type")
    );
    assert_eq!(get(&h, &format!("/{}/play/@@@", valid_blob()), &[]).await.status(), 400, "bad token");
    let dead = new_handler(test_deps(|d| {
        d.make_stores = Arc::new(|_| {
            vec![Box::new(FakeStore {
                svc: DebridService::TorBox,
                check: HashMap::new(),
                check_err: None,
                resolve: Arc::new(|| Err(dead_link("x"))),
                status: None,
                status_blocks: false,
            })]
        });
    }));
    assert_eq!(get(&dead, &play_path, &[]).await.status(), 404, "dead link");

    // Queued is NOT dead. An uncached release resolves only after the debrid has fetched it; answering 404 in the
    // meantime made the client blacklist the release (and every uncached one ranked below it, since they all fail
    // identically). 202 + progress lets the client wait instead.
    let queued = new_handler(test_deps(|d| {
        d.make_stores = Arc::new(|_| {
            vec![Box::new(FakeStore {
                svc: DebridService::TorBox,
                check: HashMap::new(),
                check_err: None,
                resolve: Arc::new(|| Err(dead_link("not ready"))),
                status: Some(StoreStatus { progress: 0.34, eta_seconds: Some(420), ..StoreStatus::default() }),
                status_blocks: false,
            })]
        });
    }));
    let rr = get(&queued, &play_path, &[]).await;
    assert_eq!(rr.status(), 202, "queued release should be 202");
    let queued_body: serde_json::Value = serde_json::from_slice(rr.body()).unwrap_or(serde_json::Value::Null);
    assert!(
        queued_body["state"] == "downloading" && queued_body["progress"] == 0.34 && queued_body["service"] == "torbox",
        "queued body: {}",
        body_str(&rr)
    );
    assert_eq!(queued_body["etaSeconds"], 420, "eta should be passed through only when the store reports one");

    // A known-queued release answers from the status alone. A client polls this URL for the whole fetch, so
    // re-resolving per poll (~3 upstream calls) is how the account gets itself throttled.
    let resolves = Arc::new(AtomicI32::new(0));
    let counter = resolves.clone();
    let polled = new_handler(test_deps(move |d| {
        let counter = counter.clone();
        d.make_stores = Arc::new(move |_| {
            let counter = counter.clone();
            vec![Box::new(FakeStore {
                svc: DebridService::TorBox,
                check: HashMap::new(),
                check_err: None,
                resolve: Arc::new(move || {
                    counter.fetch_add(1, Ordering::SeqCst);
                    Err(dead_link("not ready"))
                }),
                status: Some(StoreStatus { progress: 0.5, ..StoreStatus::default() }),
                status_blocks: false,
            })]
        });
    }));
    assert_eq!(get(&polled, &play_path, &[]).await.status(), 202, "poll should stay 202");
    assert_eq!(resolves.load(Ordering::SeqCst), 0, "a known-queued release re-resolved; it should answer from status alone");
}

/// `ticket_test.go`'s `sealedConfig` lives in `availability_test.go`; the handler tests call the same helper.
pub(crate) fn sealed_config(kr: &SealKeyring, cfg_json: &str) -> String {
    crate::tests::availability::sealed_config(kr, cfg_json)
}

/// Go `recordingCache`: remembers the key a list was written under, so a test can age that entry without
/// rebuilding the handler's cache-key formula.
pub(crate) struct RecordingCache {
    pub inner: Arc<dyn Cache>,
    pub last: std::sync::Mutex<String>,
}

impl Cache for RecordingCache {
    fn get(&self, key: &str) -> Option<String> {
        self.inner.get(key)
    }

    fn put(&self, key: &str, value: &str, ttl: Duration) {
        *self.last.lock().expect("the lock") = key.to_owned();
        self.inner.put(key, value, ttl)
    }
}

impl RecordingCache {
    /// Go `lastKey`.
    pub fn last_key(&self) -> String {
        self.last.lock().expect("the lock").clone()
    }
}

#[tokio::test]
async fn stream_a_cached_list_is_never_served_to_another_install() {
    let token_blob = |token: &str| {
        blob(&format!(
            r#"{{"debrid":[{{"service":"torbox","token":"{token}"}}],"indexers":["torrentio"],"filters":{{"excludeCam":true}},"cachedOnly":true,"resultCap":20}}"#
        ))
    };
    let (blob_a, blob_b) = (token_blob("tb-aaa"), token_blob("tb-bbb"));
    let h = new_handler(test_deps(|_| {}));

    // A first, so its list is the entry sitting in the cache when B asks for the same title.
    let a = get(&h, &format!("/{blob_a}/stream/movie/tt44.json"), &[]).await;
    assert!(streams_len(&a) > 0, "install A got no streams, so there is nothing cached for B to be served");
    let rr_b = get(&h, &format!("/{blob_b}/stream/movie/tt44.json"), &[]).await;
    assert!(streams_len(&rr_b) > 0, "install B got no streams");
    let body = body_str(&rr_b);
    assert!(
        !body.contains(&blob_a),
        "install B was served install A's cached list: its /play URLs carry A's config blob, which in legacy plaintext mode is A's debrid token"
    );
    assert!(body.contains(&blob_b), "install B's /play URLs do not carry its own config blob");
}

/// A client disconnect must not cancel the shared build.
///
/// Stremio routinely races and cancels addon requests, and the build is behind a singleflight, so a cancelled
/// LEADER takes its followers down with it. handleStream detaches the build with context.WithoutCancel for
/// exactly this reason, and replacing that with the request context left the whole package green.
///
/// Asserted on what the scraper SEES rather than on the resulting list.
#[tokio::test]
async fn stream_a_client_disconnect_does_not_cancel_the_shared_build() {
    let entered = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Notify::new());
    let seen_by_scraper = Arc::new(std::sync::Mutex::new(None::<bool>));
    let (e, r, s) = (entered.clone(), release.clone(), seen_by_scraper.clone());
    let h = Arc::new(new_handler(test_deps(move |d| {
        let (e, r, s) = (e.clone(), r.clone(), s.clone());
        d.make_scrapers = Arc::new(move |_| {
            let (e, r, s) = (e.clone(), r.clone(), s.clone());
            vec![
                fake_scraper("torrentio", || Box::pin(async { Ok(test_seeds()) })),
                fake_scraper("comet", move || {
                    let (e, r, s) = (e.clone(), r.clone(), s.clone());
                    Box::pin(async move {
                        e.notify_one();
                        r.notified().await; // the request is cancelled while this indexer is still working
                        *s.lock().expect("the lock") = Some(false);
                        Ok(Vec::new())
                    })
                }),
            ]
        });
    })));

    let path = format!("/{}/stream/movie/tt46.json", valid_blob());
    let serving = tokio::spawn({
        let h = h.clone();
        async move { get(&h, &path, &[]).await }
    });
    entered.notified().await;
    serving.abort(); // the client goes away mid-scrape
    release.notify_one();
    tokio::time::sleep(Duration::from_millis(50)).await;

    assert!(
        seen_by_scraper.lock().expect("the lock").is_some(),
        "the scrape did not run to its end after the client disconnected — the build is riding the request context, so a cancelled leader yields a partial list that is cached and served to every singleflight follower"
    );
}

#[tokio::test]
async fn health_degraded_on_scrape_outage() {
    let h = new_handler(test_deps(|d| {
        d.make_scrapers = Arc::new(|_| vec![fake_scraper("torrentio", || Box::pin(async { Err(Box::new(std::io::Error::other("canceled")) as DoError) }))]);
    }));
    assert!(body_str(&get(&h, "/health", &[]).await).contains(r#""ok""#), "health should start ok");
    for i in 0..crate::handler::SCRAPE_FAIL_THRESHOLD {
        // distinct titles → each builds
        get(&h, &format!("/{}/stream/movie/tt{i}.json", valid_blob()), &[]).await;
    }
    let rr = get(&h, "/health", &[]).await;
    assert_eq!(rr.status(), 200, "health must stay 200 (liveness)");
    // The shape every den addon shares: a machine-readable reason plus one sentence a person can read.
    let body: serde_json::Value = serde_json::from_slice(rr.body()).unwrap_or(serde_json::Value::Null);
    assert!(
        body["status"] == "degraded" && body["reason"] == "indexers" && body["detail"].as_str().is_some_and(|d| !d.is_empty()),
        "health should be degraded with a reason and a detail after {} scrape failures: {}",
        crate::handler::SCRAPE_FAIL_THRESHOLD,
        body_str(&rr)
    );
}

/// A source that keeps missing while another answers turns /health's coverage partial — a state, not a count,
/// and without naming the source — and one complete build turns it back.
#[tokio::test]
async fn health_coverage_partial_while_a_source_keeps_missing() {
    let comet_down = Arc::new(std::sync::atomic::AtomicBool::new(true));
    let down = comet_down.clone();
    let h = new_handler(test_deps(move |d| {
        let down = down.clone();
        d.make_scrapers = Arc::new(move |_| {
            let down = down.clone();
            vec![
                fake_scraper("torrentio", || Box::pin(async { Ok(test_seeds()) })),
                fake_scraper("comet", move || {
                    let down = down.clone();
                    Box::pin(async move {
                        if down.load(Ordering::SeqCst) {
                            Err(Box::new(std::io::Error::other("deadline")) as DoError)
                        } else {
                            Ok(Vec::new())
                        }
                    })
                }),
            ]
        });
    }));
    let health = |b: &str| serde_json::from_str::<serde_json::Value>(b).unwrap_or(serde_json::Value::Null);
    let first = get(&h, "/health", &[]).await;
    assert!(health(&body_str(&first))["coverage"] == "complete", "health should start ok and complete: {}", body_str(&first));
    for i in 0..crate::handler::SCRAPE_FAIL_THRESHOLD {
        get(&h, &format!("/{}/stream/movie/tt{i}.json", valid_blob()), &[]).await;
    }
    let raw = body_str(&get(&h, "/health", &[]).await);
    let b = health(&raw);
    assert!(b["status"] == "ok" && b["coverage"] == "partial" && b["detail"].as_str().is_some_and(|d| !d.is_empty()), "health after partial builds = {raw}");
    assert!(!raw.contains("comet") && !raw.contains("torrentio"), "/health names a source: {raw}");
    comet_down.store(false, Ordering::SeqCst);
    get(&h, &format!("/{}/stream/movie/tt9.json", valid_blob()), &[]).await;
    assert_eq!(health(&body_str(&get(&h, "/health", &[]).await))["coverage"], "complete", "one complete build should restore coverage");
}

/// When every indexer fails, the empty list must NOT be cached — a later healthy request rebuilds.
#[tokio::test]
async fn routes_degraded_scrape_not_cached() {
    let healthy = Arc::new(AtomicI32::new(0));
    let flag = healthy.clone();
    let h = new_handler(test_deps(move |d| {
        let flag = flag.clone();
        d.make_scrapers = Arc::new(move |_| {
            let flag = flag.clone();
            vec![fake_scraper("torrentio", move || {
                let flag = flag.clone();
                Box::pin(async move {
                    if flag.load(Ordering::SeqCst) == 0 {
                        Err(Box::new(std::io::Error::other("canceled")) as DoError)
                    } else {
                        Ok(test_seeds())
                    }
                })
            })]
        });
    }));
    let path = format!("/{}/stream/movie/tt42.json", valid_blob());
    let degraded = get(&h, &path, &[]).await;
    assert_eq!(streams_len(&degraded), 0, "degraded scrape should yield 0 streams");
    assert_eq!(hdr(&degraded, "x-den-degraded"), "indexers", "degraded scrape should set X-Den-Degraded: indexers");
    healthy.store(1, Ordering::SeqCst);
    assert!(streams_len(&get(&h, &path, &[]).await) > 0, "empty degraded list was cached — a later healthy request should rebuild and return streams");
}

/// TorBox present + cachedOnly, but its check errors (outage). The list must not be emptied — the cached-only
/// filter is skipped for the request (streams shown), and the degraded list isn't cached.
#[tokio::test]
async fn routes_cache_truth_outage_skips_cached_only() {
    let down = Arc::new(AtomicI32::new(1));
    let flag = down.clone();
    let h = new_handler(test_deps(move |d| {
        let flag = flag.clone();
        d.make_stores = Arc::new(move |_| {
            if flag.load(Ordering::SeqCst) == 1 {
                vec![Box::new(FakeStore {
                    svc: DebridService::TorBox,
                    check: HashMap::new(),
                    check_err: Some(dead_link(ERR_CHECK_FAILED)),
                    resolve: Arc::new(|| Err(dead_link("unused"))),
                    status: None,
                    status_blocks: false,
                })]
            } else {
                vec![Box::new(FakeStore {
                    svc: DebridService::TorBox,
                    check: HashMap::from([("a".repeat(40), true), ("b".repeat(40), true)]),
                    check_err: None,
                    resolve: Arc::new(|| Err(dead_link("unused"))),
                    status: None,
                    status_blocks: false,
                })]
            }
        });
    }));
    let path = format!("/{}/stream/movie/tt43.json", valid_blob());
    assert!(streams_len(&get(&h, &path, &[]).await) > 0, "cache-truth outage should skip cachedOnly (show streams), not drop everything");
    // not cached while degraded: once the store recovers, the request reflects real cache truth
    down.store(0, Ordering::SeqCst);
    assert_eq!(streams_len(&get(&h, &path, &[]).await), 2, "after recovery want 2 cached streams (cam dropped)");
}

/// audit #4: RD-only + cachedOnly:true would return empty; the fix skips cachedOnly so streams show.
#[tokio::test]
async fn routes_rd_only_returns_streams() {
    let rd_blob = blob(r#"{"debrid":[{"service":"realdebrid","token":"rd"}],"indexers":["torrentio"],"filters":{"excludeCam":true},"cachedOnly":true,"resultCap":20}"#);
    let h = new_handler(test_deps(|d| {
        // RD-safe titles (no web-dl/bdrip/etc. that RD blocks) so the test isolates the cachedOnly skip.
        d.make_scrapers = Arc::new(|_| {
            vec![fake_scraper("torrentio", || {
                Box::pin(async {
                    Ok(vec![RawStream {
                        info_hash: "a".repeat(40),
                        title: "Movie 2160p REMUX HDR".to_owned(),
                        size_bytes: Some(18 * GIB),
                        ..RawStream::default()
                    }])
                })
            })]
        });
        d.make_stores = Arc::new(|_| {
            vec![Box::new(FakeStore {
                svc: DebridService::RealDebrid,
                check: HashMap::new(), // all-false
                check_err: None,
                resolve: Arc::new(|| Err(dead_link("unused"))),
                status: None,
                status_blocks: false,
            })]
        });
    }));
    let rr = get(&h, &format!("/{rd_blob}/stream/movie/tt5.json"), &[]).await;
    assert!(streams_len(&rr) > 0, "RD-only should still return streams (cachedOnly skipped)");
}

/// A PARTIAL cache-check failure is degraded, and the response must say so. The check must be per hash, over
/// what is actually SERVED.
#[tokio::test]
async fn stream_partial_cache_check_is_degraded() {
    // The store answers for one hash and omits the other — what a half-failed batch looks like.
    let answered = "a".repeat(40);
    let not_held = "b".repeat(40);
    let unknown = "c".repeat(40);
    let (ans, nh, unk) = (answered.clone(), not_held.clone(), unknown.clone());
    let h = new_handler(test_deps(move |d| {
        let (ans, nh, unk) = (ans.clone(), nh.clone(), unk.clone());
        d.make_scrapers = Arc::new(move |_| {
            let (ans, nh, unk) = (ans.clone(), nh.clone(), unk.clone());
            vec![fake_scraper("torrentio", move || {
                let (ans, nh, unk) = (ans.clone(), nh.clone(), unk.clone());
                Box::pin(async move {
                    Ok(vec![
                        RawStream { info_hash: ans, title: "A 1080p WEB-DL".to_owned(), seeders: Some(10), ..RawStream::default() },
                        RawStream { info_hash: nh, title: "B 1080p WEB-DL".to_owned(), seeders: Some(10), ..RawStream::default() },
                        RawStream { info_hash: unk, title: "C 1080p WEB-DL".to_owned(), seeders: Some(10), ..RawStream::default() },
                    ])
                })
            })]
        });
        // Answered for two of three: one held, one definitively NOT held, one its batch never covered.
        // All three are needed — with only "held" and "unchecked", keeping the filter per-hash and switching
        // it off entirely serve the same two streams, so the test cannot tell them apart.
        d.make_stores = Arc::new(move |_| {
            vec![Box::new(FakeStore {
                svc: DebridService::TorBox,
                check: HashMap::from([(answered.clone(), true), (not_held.clone(), false)]),
                check_err: None,
                resolve: Arc::new(|| Err(dead_link("unused"))),
                status: None,
                status_blocks: false,
            })]
        });
    }));
    let rec = get(&h, &format!("/{}/stream/movie/tt1234567.json", valid_blob()), &[]).await;
    assert_eq!(hdr(&rec, "x-den-degraded"), "cache-check", "a release nobody could check was served as fact");
    // validBlob sets cachedOnly. The held release and the unchecked one are served; the one the store
    // definitively does not hold is dropped.
    assert_eq!(streams_len(&rec), 2, "the held one and the one nobody could check");
    assert!(!body_str(&rec).contains(&"b".repeat(40)), "a release the store said it does not hold was served to a cachedOnly request");
}

/// Degraded is judged on what is SERVED, not on what was scraped: an unchecked release that every filter would
/// drop must not degrade the response.
#[tokio::test]
async fn stream_unchecked_releases_that_are_filtered_out_do_not_degrade() {
    let held = "a".repeat(40);
    let junk = "b".repeat(40); // unchecked AND dropped by excludeCam, so it cannot affect the answer
    let (h1, j1) = (held.clone(), junk.clone());
    let h = new_handler(test_deps(move |d| {
        let (h1, j1) = (h1.clone(), j1.clone());
        d.make_scrapers = Arc::new(move |_| {
            let (h1, j1) = (h1.clone(), j1.clone());
            vec![fake_scraper("torrentio", move || {
                let (h1, j1) = (h1.clone(), j1.clone());
                Box::pin(async move {
                    Ok(vec![
                        RawStream { info_hash: h1, title: "A 1080p WEB-DL".to_owned(), seeders: Some(10), ..RawStream::default() },
                        RawStream { info_hash: j1, title: "B 2160p HDCAM".to_owned(), seeders: Some(10), ..RawStream::default() },
                    ])
                })
            })]
        });
        d.make_stores = Arc::new(move |_| {
            vec![Box::new(FakeStore {
                svc: DebridService::TorBox,
                check: HashMap::from([(held.clone(), true)]),
                check_err: None,
                resolve: Arc::new(|| Err(dead_link("unused"))),
                status: None,
                status_blocks: false,
            })]
        });
    }));
    let rec = get(&h, &format!("/{}/stream/movie/tt7654321.json", valid_blob()), &[]).await;
    assert_eq!(hdr(&rec, "x-den-degraded"), "", "an unchecked release nobody will see is not a degraded answer");
    assert_eq!(streams_len(&rec), 1, "served streams");
}

/// One cache-truth store being entirely OUT is a degraded answer, even when the other replies normally. Its
/// silence is why the survivor's "no" cannot rule anything out.
#[tokio::test]
async fn stream_one_store_entirely_down_is_degraded_and_stops_filtering() {
    let held = "a".repeat(40);
    let h1 = held.clone();
    let h = new_handler(test_deps(move |d| {
        let h1 = h1.clone();
        d.make_scrapers = Arc::new(move |_| {
            let h1 = h1.clone();
            vec![fake_scraper("torrentio", move || {
                let h1 = h1.clone();
                Box::pin(async move { Ok(vec![RawStream { info_hash: h1, title: "A 1080p WEB-DL".to_owned(), seeders: Some(10), ..RawStream::default() }]) })
            })]
        });
        d.make_stores = Arc::new(move |_| {
            vec![
                // TorBox answers "I do not hold it"; Premiumize's check is down and says nothing. The release
                // may be sitting on Premiumize, so it must not be dropped.
                Box::new(FakeStore {
                    svc: DebridService::TorBox,
                    check: HashMap::from([(held.clone(), false)]),
                    check_err: None,
                    resolve: Arc::new(|| Err(dead_link("unused"))),
                    status: None,
                    status_blocks: false,
                }),
                Box::new(FakeStore {
                    svc: DebridService::Premiumize,
                    check: HashMap::new(),
                    check_err: Some(dead_link(ERR_CHECK_FAILED)),
                    resolve: Arc::new(|| Err(dead_link("unused"))),
                    status: None,
                    status_blocks: false,
                }),
            ]
        });
    }));
    let rec = get(&h, &format!("/{}/stream/movie/tt5555555.json", valid_blob()), &[]).await;
    assert_eq!(hdr(&rec, "x-den-degraded"), "cache-check", "a store being out is not an ordinary answer");
    assert_eq!(streams_len(&rec), 1, "cachedOnly must stop filtering when a store cannot be asked");
    assert!(hdr(&rec, "cache-control").contains("no-store"), "a degraded list must not be cached");
}

/// A store being out is degraded even when every SERVED release is confirmed held by the survivor.
#[tokio::test]
async fn stream_a_down_store_is_degraded_even_when_everything_served_is_known_held() {
    let held = "a".repeat(40);
    let (h1, h2) = (held.clone(), held.clone());
    let h = new_handler(test_deps(move |d| {
        let h1 = h1.clone();
        d.make_scrapers = Arc::new(move |_| {
            let h1 = h1.clone();
            vec![fake_scraper("torrentio", move || {
                let h1 = h1.clone();
                Box::pin(async move { Ok(vec![RawStream { info_hash: h1, title: "A 1080p WEB-DL".to_owned(), seeders: Some(10), ..RawStream::default() }]) })
            })]
        });
        d.make_stores = Arc::new(move |_| {
            vec![
                Box::new(FakeStore {
                    svc: DebridService::TorBox,
                    check: HashMap::from([(h2.clone(), true)]), // a confirmed yes
                    check_err: None,
                    resolve: Arc::new(|| Err(dead_link("unused"))),
                    status: None,
                    status_blocks: false,
                }),
                Box::new(FakeStore {
                    svc: DebridService::Premiumize,
                    check: HashMap::new(),
                    check_err: Some(dead_link(ERR_CHECK_FAILED)), // entirely down
                    resolve: Arc::new(|| Err(dead_link("unused"))),
                    status: None,
                    status_blocks: false,
                }),
            ]
        });
    }));
    let rec = get(&h, &format!("/{}/stream/movie/tt6666666.json", valid_blob()), &[]).await;
    assert_eq!(streams_len(&rec), 1, "the held one");
    assert_eq!(hdr(&rec, "x-den-degraded"), "cache-check", "a store is down and nothing in the response says so");
    assert!(hdr(&rec, "cache-control").contains("no-store"), "a list built during an outage must not be cached");
}

/// A title with no releases is a COMPLETE answer, not an outage: the cache check is never asked anything when
/// there are no hashes, and it is cached like any other answer.
#[tokio::test]
async fn stream_no_releases_is_an_answer_not_an_outage() {
    let scrapes = Arc::new(AtomicI32::new(0));
    let count = scrapes.clone();
    let h = new_handler(test_deps(move |d| {
        let count = count.clone();
        d.make_scrapers = Arc::new(move |_| {
            let count = count.clone();
            vec![fake_scraper("torrentio", move || {
                let count = count.clone();
                Box::pin(async move {
                    count.fetch_add(1, Ordering::SeqCst); // asked, answered, and it has nothing
                    Ok(Vec::new())
                })
            })]
        });
        d.make_stores = Arc::new(|_| {
            vec![Box::new(FakeStore {
                svc: DebridService::TorBox,
                check: HashMap::new(),
                check_err: None,
                resolve: Arc::new(|| Err(dead_link("unused"))),
                status: None,
                status_blocks: false,
            })]
        });
    }));
    let path = format!("/{}/stream/movie/tt7777777.json", valid_blob());
    let first = get(&h, &path, &[]).await;
    assert_eq!(hdr(&first, "x-den-degraded"), "", "X-Den-Degraded for a title that simply has no releases");
    get(&h, &path, &[]).await;
    assert_eq!(scrapes.load(Ordering::SeqCst), 1, "an authoritative empty answer must be cached like any other");
}

/// A partial but non-empty list is served AND cached. Whatever came back is real. The client must not be told to
/// hold it longer than the server does.
#[tokio::test]
async fn stream_a_partial_list_is_still_served_and_cached() {
    let scrapes = Arc::new(AtomicI32::new(0));
    let count = scrapes.clone();
    let h = new_handler(test_deps(move |d| {
        d.scrape_timeout = Duration::from_millis(50);
        let count = count.clone();
        d.make_scrapers = Arc::new(move |_| {
            let count = count.clone();
            vec![
                fake_scraper("torrentio", move || {
                    let count = count.clone();
                    Box::pin(async move {
                        count.fetch_add(1, Ordering::SeqCst);
                        Ok(vec![RawStream { info_hash: "a".repeat(40), title: "A 1080p WEB-DL".to_owned(), seeders: Some(10), ..RawStream::default() }])
                    })
                }),
                fake_scraper("comet", || Box::pin(async { Err(Box::new(std::io::Error::other("canceled")) as DoError) })),
            ]
        });
        d.make_stores = Arc::new(|_| {
            vec![Box::new(FakeStore {
                svc: DebridService::TorBox,
                check: HashMap::from([("a".repeat(40), true)]),
                check_err: None,
                resolve: Arc::new(|| Err(dead_link("unused"))),
                status: None,
                status_blocks: false,
            })]
        });
    }));
    let path = format!("/{}/stream/movie/tt8888888.json", valid_blob());
    let first = get(&h, &path, &[]).await;
    assert_eq!(streams_len(&first), 1, "what the healthy indexer returned is real");
    assert_eq!(hdr(&first, "x-den-degraded"), "", "X-Den-Degraded over a list that is short, not broken");
    let short = format!("max-age={}", crate::handler::PARTIAL_LIST_TTL.as_secs());
    let cc = hdr(&first, "cache-control");
    assert!(cc.contains(&short), "cache-control = {cc:?}: the client would hold this longer than the server does");
    assert!(!cc.contains("stale-if-error"), "cache-control = {cc:?}: a short list must not survive a day of errors on the device");
    // The SECOND requester is served from cache, and must be told the same short freshness.
    let second = get(&h, &path, &[]).await;
    let hit_cc = hdr(&second, "cache-control");
    assert!(hit_cc.contains(&short) && !hit_cc.contains("stale-if-error"), "cache hit sent {hit_cc:?}");
    assert_eq!(scrapes.load(Ordering::SeqCst), 1, "a partial list must be cached, or every request pays the full scrape");
}

// ---- Ported, but blocked on package 6 (hostlimit's debugLimiter) and on the handler's build internals.

#[test]
#[ignore = "needs hostlimit::HostLimiter from package 6 (debugLimiter) and handler build internals"]
fn stream_debug_reports_where_the_list_went() {
    // Go TestStreamDebug_reportsWhereTheListWent: ?debug=1 names deduped=3, ranked=2, the CAM drop under
    // droppedBy.cam, and scores in served order.
    todo!("port with debugLimiter once hostlimit lands")
}

#[test]
#[ignore = "needs hostlimit::HostLimiter from package 6 (debugLimiter)"]
fn stream_debug_is_never_cached() {
    // Go TestStreamDebug_isNeverCached: a ?debug=1 build is never written under the list cache key, and a
    // normal response carries no debug block.
    todo!("port with debugLimiter once hostlimit lands")
}

#[test]
#[ignore = "needs hostlimit::HostLimiter from package 6 (debugLimiter)"]
fn stream_debug_accounting_reconciles() {
    // Go TestStreamDebug_accountingReconciles: deduped minus every attributed drop equals ranked, with the
    // seed cap and the RD filename filter both attributed.
    todo!("port with debugLimiter once hostlimit lands")
}

#[test]
#[ignore = "needs hostlimit::HostLimiter from package 6 (debugLimiter)"]
fn stream_debug_is_rate_limited() {
    // Go TestStreamDebug_isRateLimited: the third ?debug=1 in the window answers 429 with Retry-After, and a
    // plain request is unaffected.
    todo!("port with debugLimiter once hostlimit lands")
}

#[test]
#[ignore = "needs hostlimit::HostLimiter from package 6 (debugLimiter)"]
fn stream_debug_leaks_no_upstream_urls_or_tokens() {
    // Go TestStreamDebug_leaksNoUpstreamURLsOrTokens: the debug body names no token and no upstream address.
    todo!("port with debugLimiter once hostlimit lands")
}

#[test]
#[ignore = "needs the handler's stale-list build internals (splitCached, joinCached, staleWindowFor)"]
fn stream_list_serves_stale_while_rebuilding() {
    // Go TestStreamList_servesStaleWhileRebuilding: a complete list one second past freshness is served at
    // once with den.answerKind stale and no degraded reason, and a rebuild runs behind it.
    todo!("port once splitCached and joinCached are ported")
}

#[test]
#[ignore = "needs the handler's stale-list build internals (splitCached, joinCached)"]
fn stream_list_a_disconnect_does_not_kill_the_stale_rebuild() {
    // Go TestStreamList_aDisconnectDoesNotKillTheStaleRebuild: the rebuild behind a stale answer runs to its
    // end after the client that triggered it has gone.
    todo!("port once splitCached and joinCached are ported")
}

#[test]
#[ignore = "needs the handler's stale-list build internals (staleWindowFor, splitCached, joinCached)"]
fn stream_list_stale_window_scales_with_the_ttl() {
    // Go TestStreamList_staleWindowScalesWithTheTTL: a 30s list's stale reply is capped at max-age=30.
    todo!("port once splitCached and joinCached are ported")
}

#[test]
#[ignore = "needs the handler's stale-if-error and outage-hold arithmetic (staleIfErrorFor, outageHoldFor)"]
fn stream_list_stale_if_error_stays_inside_ticket_life() {
    // Go TestStreamList_staleIfErrorStaysInsideTicketLife: stale-if-error is min(ceiling, ticket-3*list).
    todo!("port the stale-if-error arithmetic")
}

#[test]
#[ignore = "needs the handler's outage-hold arithmetic (outageHoldFor)"]
fn stream_list_outage_hold_leaves_tickets_a_viewing() {
    // Go TestStreamList_outageHoldLeavesTicketsAViewing: the outage hold is the ticket life less the list
    // TTL and outageViewingRoom, and never less than the stale window.
    todo!("port the outage-hold arithmetic")
}

#[test]
#[ignore = "needs the handler's stale-list build internals (splitCached, joinCached, outageHoldFor)"]
fn stream_list_serves_the_last_complete_list_when_no_indexer_answers() {
    // Go TestStreamList_servesTheLastCompleteListWhenNoIndexerAnswers: stale_list during the outage, the
    // cool-off suppresses re-scrapes, and past the outage hold the answer is unknown.
    todo!("port once splitCached and joinCached are ported")
}

#[test]
#[ignore = "needs the handler's rebuild gate (bookRebuild, releaseRebuild) and its counters"]
fn stream_list_stale_rebuild_spawns_one_goroutine_per_key() {
    // Go TestStreamList_staleRebuildSpawnsOneGoroutinePerKey: 300 stale hits start one rebuild, not 300.
    todo!("port with the rebuild gate")
}

#[test]
#[ignore = "needs the handler's rebuild gate (bookRebuild, releaseRebuild)"]
fn rebuild_gate() {
    // Go TestRebuildGate: booking, the cool-off after a degraded rebuild, and its expiry.
    todo!("port with the rebuild gate")
}

#[test]
#[ignore = "needs the handler's rebuild gate (bookRebuild, releaseRebuild, maxConcurrentRebuilds)"]
fn rebuild_gate_bounds_concurrent_rebuilds() {
    // Go TestRebuildGate_boundsConcurrentRebuilds: a global ceiling on concurrent rebuilds.
    todo!("port with the rebuild gate")
}

#[test]
#[ignore = "needs the handler's rebuild gate lease sweep (rebuilds, releaseRebuild)"]
fn rebuild_gate_swept_lease_still_returns_its_slot() {
    // Go TestRebuildGate_sweptLeaseStillReturnsItsSlot: a swept lease gives its slot back.
    todo!("port with the rebuild gate")
}

#[test]
#[ignore = "needs the handler's rebuild gate fencing token (releaseRebuild)"]
fn rebuild_gate_stale_lease_releases_nothing() {
    // Go TestRebuildGate_staleLeaseReleasesNothing: a superseded lease neither deletes a live booking nor a
    // cool-off.
    todo!("port with the rebuild gate")
}

#[test]
#[ignore = "needs the handler's rebuild gate counter (rebuildsLive)"]
fn rebuild_gate_slot_accounting_balances() {
    // Go TestRebuildGate_slotAccountingBalances: every granted booking returns its slot exactly once.
    todo!("port with the rebuild gate")
}

#[tokio::test]
#[ignore = "needs the handler's stale-list build internals (splitCached, joinCached)"]
async fn stream_list_outage_does_not_scrape_per_request() {
    // Go TestStreamList_outageDoesNotScrapePerRequest: 20 stale hits during an outage produce at most 3 scrapes.
    todo!("port once splitCached and joinCached are ported")
}

#[tokio::test]
#[ignore = "needs the handler's stale-list build internals (splitCached)"]
async fn stream_list_partial_list_gets_no_stale_window() {
    // Go TestStreamList_partialListGetsNoStaleWindow: a partial list carries no freshness stamp.
    todo!("port once splitCached is ported")
}

#[test]
#[ignore = "needs the handler's cached-entry format (splitCached, joinCached)"]
fn split_cached_reads_an_older_entry_conservatively() {
    // Go TestSplitCached_readsAnOlderEntryConservatively: an entry with no completeness recorded is read as
    // incomplete, and the three-field format round-trips.
    todo!("port the cached-entry format")
}

#[test]
#[ignore = "needs the handler's cached-entry format (splitCached)"]
fn split_cached_treats_every_unknown_form_the_same_way() {
    // Go TestSplitCached_treatsEveryUnknownFormTheSameWay: a value with no separator is read the same as the
    // legacy one-separator form.
    todo!("port the cached-entry format")
}

#[tokio::test]
#[ignore = "needs package 1's torBoxStore and package 2's StorePool (the play path's resolve and status budgets)"]
async fn handle_play_poll_reads_use_the_status_budget() {
    // Go TestHandlePlay_pollReadsUseTheStatusBudget: a poll-route status read never takes the resolve budget,
    // and only the escalation may take double the status budget.
    todo!("needs the play path")
}

#[tokio::test]
#[ignore = "needs package 2's StorePool (escalation)"]
async fn handle_play_the_escalation_re_asks_only_the_store_that_could_not_answer() {
    // Go TestHandlePlay_theEscalationReAsksOnlyTheStoreThatCouldNotAnswer.
    todo!("needs StorePool")
}

#[test]
#[ignore = "needs the handler's escalated status context (escalatedStatusCtx, escalatedStatusBudget)"]
fn escalated_status_ctx_cannot_outlive_the_resolve_budget() {
    // Go TestEscalatedStatusCtx_cannotOutliveTheResolveBudget.
    todo!("port escalatedStatusCtx")
}

#[tokio::test]
#[ignore = "needs package 1's torBoxStore (the probe's refusal path)"]
async fn handle_probe_reports_a_refusal_it_discovers_itself() {
    // Go TestHandleProbe_reportsARefusalItDiscoversItself: a refusal seen by the probe is not reported as 404.
    todo!("needs the TorBox store")
}

#[tokio::test]
#[ignore = "needs the status budget as a mutable value (statusBudget) and package 2's StorePool"]
async fn play_the_preference_cache_check_is_budgeted() {
    // Go TestPlay_thePreferenceCacheCheckIsBudgeted: the preference check gets a slice of the resolve clock.
    todo!("needs the status budget and StorePool")
}

#[tokio::test]
#[ignore = "needs package 1's torBoxStore and package 3's global add budget"]
async fn resolve_a_spent_clock_does_not_buy_an_add() {
    // Go TestResolve_aSpentClockDoesNotBuyAnAdd.
    todo!("needs the TorBox store and the add budget")
}

#[tokio::test]
#[ignore = "needs package 2's StorePool (ResolvePreferring) and package 1's torBoxStore"]
async fn resolve_preferring_a_spent_clock_still_surfaces_an_in_flight_add() {
    // Go TestResolvePreferring_aSpentClockStillSurfacesAnInFlightAdd.
    todo!("needs StorePool")
}

#[tokio::test]
#[ignore = "needs the status budget as a mutable value (statusBudget)"]
async fn play_status_timeout_does_not_add() {
    // Go TestPlay_statusTimeoutDoesNotAdd: a timed-out status read leads to no resolve.
    todo!("needs the mutable status budget")
}

#[tokio::test]
#[ignore = "needs the status budget as a mutable value (statusBudget)"]
async fn play_prompt_not_downloading_still_adds() {
    // Go TestPlay_promptNotDownloadingStillAdds: a prompt "not downloading" answer still adds.
    todo!("needs the mutable status budget")
}

#[tokio::test]
#[ignore = "needs the status budget as a mutable value (statusBudget)"]
async fn play_multi_store_status_timeout_does_not_add() {
    // Go TestPlay_multiStoreStatusTimeoutDoesNotAdd: with several accounts, a timed-out store adds nothing.
    todo!("needs the mutable status budget")
}
