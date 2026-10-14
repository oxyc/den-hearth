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
use crate::handler::{new_handler, now_unix, Handler};
use crate::rank::RawStream;
use crate::scrape::{DoError, ScrapeQuery, Scraper};
use crate::seal::{parse_seal_keyring, SealKeyring};
use crate::stores::{DeadLinkError, ResolveTarget, StatusAnswer, Store, StoreError, StoreStatus, ERR_CHECK_FAILED};
use crate::tests::mock::resp;
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
        probe_doer: None,
        meta: None,
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

/// handler_test.go's `answeringStore`: a store whose three-valued status answer is fixed, counting the times it was
/// asked. Its resolve finds nothing.
#[derive(Clone)]
pub(crate) struct AnsweringStore {
    svc: DebridService,
    answer: StatusAnswer,
    pub asked: Arc<AtomicI32>,
}

impl AnsweringStore {
    pub(crate) fn new(svc: DebridService, answer: StatusAnswer) -> AnsweringStore {
        AnsweringStore { svc, answer, asked: Arc::new(AtomicI32::new(0)) }
    }
}

#[async_trait]
impl Store for AnsweringStore {
    fn service(&self) -> DebridService {
        self.svc
    }

    async fn cache_check(&self, _hashes: &[String]) -> Result<HashMap<String, bool>, StoreError> {
        Ok(HashMap::new())
    }

    async fn resolve(&self, _t: &ResolveTarget) -> Result<String, StoreError> {
        Err(dead_link("nothing here"))
    }

    async fn status(&self, _t: &ResolveTarget) -> Option<StoreStatus> {
        None
    }

    async fn status_answer(&self, _t: &ResolveTarget) -> Option<(StoreStatus, StatusAnswer)> {
        self.asked.fetch_add(1, Ordering::SeqCst);
        Some((StoreStatus::default(), self.answer))
    }
}

/// handler_test.go's `slowStatusStore` answers status by waiting out the caller's budget the first `slow_calls`
/// times, then reports a download in progress. It is the shape of a real TorBox account with a large listing: status
/// makes two upstream calls, one of them an account listing measured at ~13 MB on 2,000 torrents.
#[derive(Clone)]
pub(crate) struct SlowStatusStore {
    svc: DebridService,
    /// How many leading status calls wait out the caller's budget instead of answering.
    slow_calls: i32,
    /// When set, a call that is not slow answers "nobody is fetching it" rather than "downloading".
    never_downloading: bool,
    pub status_hits: Arc<AtomicI32>,
    pub resolves: Arc<AtomicI32>,
}

impl SlowStatusStore {
    pub(crate) fn new(svc: DebridService, slow_calls: i32, never_downloading: bool) -> SlowStatusStore {
        SlowStatusStore {
            svc,
            slow_calls,
            never_downloading,
            status_hits: Arc::new(AtomicI32::new(0)),
            resolves: Arc::new(AtomicI32::new(0)),
        }
    }
}

#[async_trait]
impl Store for SlowStatusStore {
    fn service(&self) -> DebridService {
        self.svc
    }

    async fn cache_check(&self, _hashes: &[String]) -> Result<HashMap<String, bool>, StoreError> {
        Ok(HashMap::new())
    }

    async fn resolve(&self, _t: &ResolveTarget) -> Result<String, StoreError> {
        self.resolves.fetch_add(1, Ordering::SeqCst);
        Ok("https://cdn.example/added.mkv".to_owned())
    }

    async fn status(&self, _t: &ResolveTarget) -> Option<StoreStatus> {
        if self.status_hits.fetch_add(1, Ordering::SeqCst) + 1 <= self.slow_calls {
            std::future::pending::<()>().await; // outlast the caller's budget, exactly as a slow account listing does
        }
        if self.never_downloading {
            return None;
        }
        Some(StoreStatus { progress: 0.4, ..StoreStatus::default() })
    }
}

/// handler_test.go's `countingResolveStore` counts resolves so a test can prove no store was asked to queue anything.
pub(crate) struct CountingResolveStore<S: Store> {
    inner: S,
    n: Arc<AtomicI32>,
}

impl<S: Store> CountingResolveStore<S> {
    pub(crate) fn new(inner: S, n: Arc<AtomicI32>) -> CountingResolveStore<S> {
        CountingResolveStore { inner, n }
    }
}

#[async_trait]
impl<S: Store> Store for CountingResolveStore<S> {
    fn service(&self) -> DebridService {
        self.inner.service()
    }

    async fn cache_check(&self, hashes: &[String]) -> Result<HashMap<String, bool>, StoreError> {
        self.inner.cache_check(hashes).await
    }

    async fn resolve(&self, t: &ResolveTarget) -> Result<String, StoreError> {
        self.n.fetch_add(1, Ordering::SeqCst);
        self.inner.resolve(t).await
    }

    async fn status(&self, t: &ResolveTarget) -> Option<StoreStatus> {
        self.inner.status(t).await
    }
}

/// One store shared by every `make_stores` call, as Go hands the handler the same `Store` value each time. Every
/// method, optional ones included, goes to the shared store.
#[derive(Clone)]
pub(crate) struct SharedStore(pub Arc<dyn Store>);

#[async_trait]
impl Store for SharedStore {
    fn service(&self) -> DebridService {
        self.0.service()
    }

    async fn cache_check(&self, hashes: &[String]) -> Result<HashMap<String, bool>, StoreError> {
        self.0.cache_check(hashes).await
    }

    async fn resolve(&self, t: &ResolveTarget) -> Result<String, StoreError> {
        self.0.resolve(t).await
    }

    async fn status(&self, t: &ResolveTarget) -> Option<StoreStatus> {
        self.0.status(t).await
    }

    async fn status_answer(&self, t: &ResolveTarget) -> Option<(StoreStatus, StatusAnswer)> {
        self.0.status_answer(t).await
    }

    async fn cancel(&self, info_hash: &str, reannounce: bool) -> Option<Result<crate::stores::CancelOutcome, StoreError>> {
        self.0.cancel(info_hash, reannounce).await
    }

    fn recent_refusal(&self, info_hash: &str) -> Option<String> {
        self.0.recent_refusal(info_hash)
    }

    fn add_in_flight(&self, info_hash: &str) -> Option<bool> {
        self.0.add_in_flight(info_hash)
    }

    fn holds_torrent(&self, t: &ResolveTarget) -> Option<bool> {
        self.0.holds_torrent(t)
    }

    fn scout_budget_spent(&self) -> Option<bool> {
        self.0.scout_budget_spent()
    }

    fn account_refused(&self) -> Option<String> {
        self.0.account_refused()
    }

    fn budget_account(&self) -> Option<String> {
        self.0.budget_account()
    }

    fn known_file_size(&self, t: &ResolveTarget) -> Option<i64> {
        self.0.known_file_size(t)
    }

    async fn account_summary(&self) -> Option<Result<crate::stores::AccountSummary, StoreError>> {
        self.0.account_summary().await
    }
}

/// `make_stores` returning the one shared store on every call.
pub(crate) fn shared_stores(store: Arc<dyn Store>) -> Arc<dyn Fn(&crate::config::Config) -> Vec<Box<dyn Store>> + Send + Sync> {
    Arc::new(move |_| vec![Box::new(SharedStore(store.clone())) as Box<dyn Store>])
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
    let at = url.find(marker).expect("the play marker");
    // A config blob names the segment without its slash; the path being asked for starts at that slash.
    let at = if url[..at].ends_with('/') { at - 1 } else { at };
    url[at..].to_owned()
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

/// Go `unthrottleDebug`: the ?debug=1 pacing is one bucket shared by every ?debug=1 request, so without this a test
/// that issues several would spend from the same three. Go swaps a package-level limiter; here each handler holds its
/// own, so the swap is on the handler under test.
fn unthrottle_debug(h: &Handler) {
    h.set_debug_limiter(Duration::from_millis(1), 10_000);
}

#[tokio::test]
async fn stream_debug_reports_where_the_list_went() {
    // Go TestStreamDebug_reportsWhereTheListWent: ?debug=1 names deduped=3, ranked=2, the CAM drop under
    // droppedBy.cam, and scores in served order.
    let h = new_handler(test_deps(|_| {}));
    unthrottle_debug(&h);
    let rr = get(&h, &format!("/{}/stream/movie/tt1234567.json?debug=1", valid_blob()), &[]).await;
    assert_eq!(rr.status(), 200, "debug");
    let body: serde_json::Value = serde_json::from_slice(rr.body()).expect("JSON");
    let debug = &body["debug"];
    assert!(!debug.is_null(), "?debug=1 returned no debug block");
    assert!(debug["deduped"] == 3 && debug["ranked"] == 2, "counts: {debug}");
    assert_eq!(debug["droppedBy"]["cam"], 1, "the CAM drop was not attributed");
    let kept = debug["kept"].as_array().expect("kept");
    let streams = body["streams"].as_array().expect("streams");
    assert_eq!(kept.len(), streams.len(), "kept scores for served streams");
    for (i, s) in streams.iter().enumerate() {
        assert_eq!(kept[i]["title"], s["title"], "kept[{i}] order");
    }
    assert!(kept[0]["score"].as_i64() > kept[1]["score"].as_i64(), "scores do not explain the order");
}

#[tokio::test]
async fn stream_debug_is_never_cached() {
    // Go TestStreamDebug_isNeverCached: a ?debug=1 build is never written under the list cache key, and a
    // normal response carries no debug block.
    let cache = Arc::new(RecordingCache { inner: Arc::new(MemoryCache::new(1 << 20)), last: Default::default() });
    let h = new_handler(test_deps(|d| d.cache = cache.clone()));
    unthrottle_debug(&h);
    let path = format!("/{}/stream/movie/tt1234567.json", valid_blob());
    assert_eq!(get(&h, &format!("{path}?debug=1"), &[]).await.status(), 200, "debug");
    let key = cache.last_key();
    if !key.is_empty() {
        if let Some(v) = cache.get(&key) {
            assert!(!v.contains("droppedBy"), "a debug build was cached and would be served to everyone else");
        }
    }
    let plain = get(&h, &path, &[]).await;
    assert!(!body_str(&plain).contains("\"debug\""), "a normal response carries a debug block");
}

#[tokio::test]
async fn stream_debug_accounting_reconciles() {
    // Go TestStreamDebug_accountingReconciles: deduped minus every attributed drop equals ranked, with the
    // seed cap and the RD filename filter both attributed.
    let rd_blob = blob(r#"{"debrid":[{"service":"realdebrid","token":"rd"}],"indexers":["torrentio"],"resultCap":20}"#);
    let mut seeds: Vec<RawStream> = (0..crate::handler::MAX_SEEDS + 20)
        .map(|i| RawStream { info_hash: format!("{i:040x}"), title: format!("Film.2024.1080p.WEB-DL.x264-G{i}.mkv"), ..RawStream::default() })
        .collect();
    seeds.extend((0..20).map(|i| RawStream {
        info_hash: format!("{:040x}", 100000 + i),
        title: format!("Film.2024.1080p.BluRay.REMUX-G{i}.mkv"),
        ..RawStream::default()
    }));
    let s = seeds.clone();
    let h = new_handler(test_deps(move |d| {
        let s = s.clone();
        d.make_scrapers = Arc::new(move |_| {
            let s = s.clone();
            vec![fake_scraper("torrentio", move || {
                let s = s.clone();
                Box::pin(async move { Ok(s) })
            })]
        });
        d.make_stores = Arc::new(|_| {
            vec![Box::new(FakeStore {
                svc: DebridService::RealDebrid,
                check: HashMap::new(),
                check_err: None,
                resolve: Arc::new(|| Err(dead_link("unused"))),
                status: None,
                status_blocks: false,
            })]
        });
    }));
    unthrottle_debug(&h);
    let rr = get(&h, &format!("/{rd_blob}/stream/movie/tt1234567.json?debug=1"), &[]).await;
    assert_eq!(rr.status(), 200, "debug");
    let body: serde_json::Value = serde_json::from_slice(rr.body()).expect("JSON");
    let debug = &body["debug"];
    assert_eq!(debug["deduped"], seeds.len(), "deduped = the indexer's count before trimming");
    assert_eq!(debug["droppedBy"]["seedCap"], seeds.len() - crate::handler::MAX_SEEDS, "seedCap drops");
    assert!(debug["droppedBy"]["realDebridBlocked"].as_i64().unwrap_or(0) > 0, "the RD filename filter dropped nothing");
    let dropped: i64 = debug["droppedBy"].as_object().map_or(0, |m| m.values().filter_map(|v| v.as_i64()).sum());
    assert_eq!(debug["deduped"].as_i64().unwrap_or(0) - dropped, debug["ranked"].as_i64().unwrap_or(-1), "accounting does not reconcile");
}

#[tokio::test]
async fn stream_debug_is_rate_limited() {
    // Go TestStreamDebug_isRateLimited: the third ?debug=1 in the window answers 429 with Retry-After, and a
    // plain request is unaffected.
    let scrapes = Arc::new(AtomicI32::new(0));
    let count = scrapes.clone();
    let h = new_handler(test_deps(move |d| {
        let count = count.clone();
        d.make_scrapers = Arc::new(move |_| {
            let count = count.clone();
            vec![fake_scraper("torrentio", move || {
                let count = count.clone();
                Box::pin(async move {
                    count.fetch_add(1, Ordering::SeqCst);
                    Ok(test_seeds())
                })
            })]
        });
    }));
    h.set_debug_limiter(Duration::from_secs(3600), 2);
    let path = format!("/{}/stream/movie/tt1234567.json?debug=1", valid_blob());
    for i in 0..2 {
        assert_eq!(get(&h, &path, &[]).await.status(), 200, "debug {i}");
    }
    let third = get(&h, &path, &[]).await;
    assert_eq!(third.status(), 429, "third debug");
    assert!(!hdr(&third, "retry-after").is_empty(), "429 without a Retry-After");
    assert_eq!(scrapes.load(Ordering::SeqCst), 2, "a refused debug must not reach the indexers or the debrid");
    assert_eq!(get(&h, &format!("/{}/stream/movie/tt1234567.json", valid_blob()), &[]).await.status(), 200, "normal request after debug throttling");
}

#[tokio::test]
async fn stream_debug_leaks_no_upstream_urls_or_tokens() {
    // Go TestStreamDebug_leaksNoUpstreamURLsOrTokens: the debug body names no token and no upstream address.
    let h = new_handler(test_deps(|_| {}));
    unthrottle_debug(&h);
    let body = body_str(&get(&h, &format!("/{}/stream/movie/tt1234567.json?debug=1", valid_blob()), &[]).await);
    for forbidden in ["tb-secret", "mediafusion.elfhosted", "torrentio.strem.fun", "https://comet"] {
        assert!(!body.contains(forbidden), "?debug=1 disclosed {forbidden:?}");
    }
}

#[tokio::test]
async fn stream_list_serves_stale_while_rebuilding() {
    // Go TestStreamList_servesStaleWhileRebuilding: a complete list one second past freshness is served at
    // once with den.answerKind stale and no degraded reason, and a rebuild runs behind it.
    let cache = Arc::new(RecordingCache { inner: Arc::new(MemoryCache::new(1 << 20)), last: Default::default() });
    let scraped = Arc::new(tokio::sync::Notify::new());
    let s = scraped.clone();
    let h = new_handler(test_deps(|d| {
        d.cache = cache.clone();
        d.make_scrapers = Arc::new(move |_| {
            let s = s.clone();
            vec![fake_scraper("torrentio", move || {
                let s = s.clone();
                Box::pin(async move {
                    s.notify_one();
                    Ok(test_seeds())
                })
            })]
        });
    }));
    let path = format!("/{}/stream/movie/tt1234567.json", valid_blob());
    assert_eq!(get(&h, &path, &[]).await.status(), 200, "cold build");
    scraped.notified().await;
    // Age the entry past its freshness without touching its physical expiry.
    let key = cache.last_key();
    let held = cache.get(&key).expect("the cold build cached nothing");
    let (complete, _, etag, body) = crate::handler::split_cached(&held);
    cache.put(&key, &crate::handler::join_cached(complete, now_unix() - 1, &etag, &body), Duration::from_secs(60));
    let stale = get(&h, &path, &[]).await;
    assert_eq!(stale.status(), 200, "stale hit");
    let cached: serde_json::Value = serde_json::from_str(&body).expect("cached body");
    let served: serde_json::Value = serde_json::from_slice(stale.body()).expect("served body");
    assert_eq!(served["streams"], cached["streams"], "the stale hit did not serve the cached streams");
    assert!(served["den"]["answerKind"] == "stale" && served["den"]["degraded"].is_null(), "stale hit den = {}", served["den"]);
    assert_eq!(served["den"]["generatedAt"], cached["den"]["generatedAt"], "the stale hit lost the list's build time");
    assert_ne!(hdr(&stale, "etag"), etag, "the relabelled body kept the fresh list's ETag");
    assert_eq!(hdr(&stale, "cache-control"), "private, max-age=60", "stale cache-control");
    tokio::time::timeout(Duration::from_secs(5), scraped.notified()).await.expect("no rebuild ran behind the stale answer");
}

#[tokio::test]
async fn stream_list_a_disconnect_does_not_kill_the_stale_rebuild() {
    // Go TestStreamList_aDisconnectDoesNotKillTheStaleRebuild: the rebuild behind a stale answer runs to its
    // end after the client that triggered it has gone. The rebuild waits so the cancel has happened first.
    let cache = Arc::new(RecordingCache { inner: Arc::new(MemoryCache::new(1 << 20)), last: Default::default() });
    let scraped = Arc::new(tokio::sync::Mutex::new(Vec::<bool>::new()));
    let release = Arc::new(tokio::sync::Notify::new());
    let calls = Arc::new(AtomicI32::new(0));
    let (sc, rel, cl) = (scraped.clone(), release.clone(), calls.clone());
    let h = Arc::new(new_handler(test_deps(|d| {
        d.cache = cache.clone();
        d.make_scrapers = Arc::new(move |_| {
            let (sc, rel, cl) = (sc.clone(), rel.clone(), cl.clone());
            vec![fake_scraper("torrentio", move || {
                let (sc, rel, cl) = (sc.clone(), rel.clone(), cl.clone());
                Box::pin(async move {
                    if cl.fetch_add(1, Ordering::SeqCst) > 0 {
                        rel.notified().await;
                    }
                    sc.lock().await.push(true);
                    Ok(test_seeds())
                })
            })]
        });
    })));
    let path = format!("/{}/stream/movie/tt1234567.json", valid_blob());
    assert_eq!(get(&h, &path, &[]).await.status(), 200, "cold build");
    let key = cache.last_key();
    let held = cache.get(&key).expect("the cold build cached nothing");
    let (complete, _, etag, body) = crate::handler::split_cached(&held);
    cache.put(&key, &crate::handler::join_cached(complete, now_unix() - 1, &etag, &body), Duration::from_secs(60));
    let serving = tokio::spawn({
        let h = h.clone();
        let path = path.clone();
        async move { get(&h, &path, &[]).await }
    });
    serving.abort(); // a stale hit from a client that goes away the instant it has its answer
    release.notify_one();
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(!scraped.lock().await.is_empty(), "the rebuild died with the request context: it never refreshed the entry");
}

#[tokio::test]
async fn stream_list_stale_window_scales_with_the_ttl() {
    // Go TestStreamList_staleWindowScalesWithTheTTL: the stale window is capped by the configured TTL, and the
    // stale reply never claims a longer freshness than the operator configured.
    assert_eq!(crate::handler::stale_window_for(Duration::from_secs(30)), Duration::from_secs(30), "short TTL");
    assert_eq!(crate::handler::stale_window_for(Duration::from_secs(300)), crate::handler::MAX_STALE_SERVE_WINDOW, "long TTL");
    let cache = Arc::new(RecordingCache { inner: Arc::new(MemoryCache::new(1 << 20)), last: Default::default() });
    let c = cache.clone();
    let h = new_handler(test_deps(move |d| {
        d.cache = c.clone();
        d.list_ttl = Duration::from_secs(30);
    }));
    let path = format!("/{}/stream/movie/tt1234567.json", valid_blob());
    get(&h, &path, &[]).await;
    let key = cache.last_key();
    let held = cache.get(&key).unwrap_or_default();
    let (complete, _, etag, body) = crate::handler::split_cached(&held);
    cache.put(&key, &crate::handler::join_cached(complete, now_unix() - 1, &etag, &body), Duration::from_secs(60));
    assert_eq!(hdr(&get(&h, &path, &[]).await, "cache-control"), "private, max-age=30", "stale cache-control");
}

#[tokio::test]
async fn stream_list_stale_if_error_stays_inside_ticket_life() {
    // Go TestStreamList_staleIfErrorStaysInsideTicketLife: a complete list's stale-if-error never outlives the
    // tickets in it.
    for (list, ticket, want) in [
        (Duration::from_secs(300), Duration::from_secs(24 * 3600), Duration::from_secs(3600)),
        (Duration::from_secs(300), Duration::from_secs(3600), Duration::from_secs(45 * 60)),
        (Duration::from_secs(300), Duration::from_secs(600), Duration::ZERO),
    ] {
        assert_eq!(crate::handler::stale_if_error_for(list, ticket), want, "list {list:?}, ticket {ticket:?}");
    }
    let path = format!("/{}/stream/movie/tt1234567.json", valid_blob());
    let h = new_handler(test_deps(|_| {}));
    assert_eq!(hdr(&get(&h, &path, &[]).await, "cache-control"), "private, max-age=300, stale-while-revalidate=300, stale-if-error=3600");
    let short = new_handler(test_deps(|d| d.play_ticket_ttl = Duration::from_secs(600)));
    assert_eq!(hdr(&get(&short, &path, &[]).await, "cache-control"), "private, max-age=300, stale-while-revalidate=300", "with ten-minute tickets");
}

#[test]
fn stream_list_outage_hold_leaves_tickets_a_viewing() {
    // Go TestStreamList_outageHoldLeavesTicketsAViewing: the server holds a complete list for an outage far
    // longer than a device may keep its own copy, but always leaves the list's tickets a viewing's worth of life.
    for (list, ticket, want) in [
        (Duration::from_secs(300), Duration::from_secs(24 * 3600), Duration::from_secs(24 * 3600) - Duration::from_secs(15 * 60) - crate::handler::OUTAGE_VIEWING_ROOM),
        (Duration::from_secs(300), Duration::from_secs(4 * 3600), Duration::from_secs(3600)),
        (Duration::from_secs(300), Duration::from_secs(600), Duration::ZERO),
    ] {
        assert_eq!(crate::handler::outage_hold_for(list, ticket), want, "list {list:?}, ticket {ticket:?}");
    }
}

#[tokio::test]
async fn stream_list_serves_the_last_complete_list_when_no_indexer_answers() {
    // Go TestStreamList_servesTheLastCompleteListWhenNoIndexerAnswers: when no indexer answers, a title whose
    // last complete list is past its stale window gets that list back, marked stale_list. Within the cool-off
    // it is served without scraping again. Past the outage hold the answer is the build's own (unknown).
    let down = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let scrapes = Arc::new(AtomicI32::new(0));
    let cache = Arc::new(RecordingCache { inner: Arc::new(MemoryCache::new(1 << 20)), last: Default::default() });
    let (dn, sc, c) = (down.clone(), scrapes.clone(), cache.clone());
    let h = new_handler(test_deps(move |d| {
        d.cache = c.clone();
        let (dn, sc) = (dn.clone(), sc.clone());
        d.make_scrapers = Arc::new(move |_| {
            let (dn, sc) = (dn.clone(), sc.clone());
            vec![fake_scraper("torrentio", move || {
                let (dn, sc) = (dn.clone(), sc.clone());
                Box::pin(async move {
                    sc.fetch_add(1, Ordering::SeqCst);
                    if dn.load(Ordering::SeqCst) {
                        return Err(Box::new(std::io::Error::other("indexer down")) as DoError);
                    }
                    Ok(test_seeds())
                })
            })]
        });
    }));
    let path = format!("/{}/stream/movie/tt1234567.json", valid_blob());
    assert_eq!(get(&h, &path, &[]).await.status(), 200, "cold build");
    let key = cache.last_key();
    let held = cache.get(&key).unwrap_or_default();
    let (complete, _, etag, body) = crate::handler::split_cached(&held);
    let age = |past: Duration| cache.put(&key, &crate::handler::join_cached(complete, now_unix() - past.as_secs() as i64, &etag, &body), Duration::from_secs(24 * 3600));
    age(Duration::from_secs(5 * 60));
    assert_eq!(hdr(&get(&h, &path, &[]).await, "x-den-degraded"), "stale_list", "healthy background rebuild");
    down.store(true, Ordering::SeqCst);
    age(Duration::from_secs(5 * 60));
    let before = scrapes.load(Ordering::SeqCst);
    let stale = get(&h, &path, &[]).await;
    assert_eq!(stale.status(), 200, "outage");
    assert_eq!(hdr(&stale, "x-den-degraded"), "stale_list", "X-Den-Degraded");
    assert_eq!(hdr(&stale, "cache-control"), "private, max-age=60", "stale cache-control");
    assert_eq!(scrapes.load(Ordering::SeqCst), before + 1, "scraped once for the outage answer");
    let again = get(&h, &path, &[]).await;
    assert_eq!(hdr(&again, "x-den-degraded"), "stale_list", "inside the cool-off");
    assert_eq!(scrapes.load(Ordering::SeqCst), before + 1, "a request inside the cool-off scraped again");
    age(Duration::from_secs(3 * 3600));
    assert_eq!(hdr(&get(&h, &path, &[]).await, "x-den-degraded"), "stale_list", "three hours into an outage");
    age(crate::handler::outage_hold_for(crate::handler::DEFAULT_LIST_TTL, crate::ticket::DEFAULT_PLAY_TICKET_TTL) + Duration::from_secs(120));
    let late = get(&h, &path, &[]).await;
    assert_eq!(hdr(&late, "x-den-degraded"), "indexers", "past the outage hold");
    let v: serde_json::Value = serde_json::from_slice(late.body()).unwrap_or_default();
    assert_eq!(v["den"]["answerKind"], "unknown", "past the outage hold the answer is unknown");
}

#[tokio::test]
async fn stream_list_stale_rebuild_spawns_one_goroutine_per_key() {
    // Go TestStreamList_staleRebuildSpawnsOneGoroutinePerKey: one goroutine per stale KEY, not per stale REQUEST.
    // 300 stale hits while one rebuild is in flight must start nothing else.
    let release = Arc::new(tokio::sync::Notify::new());
    let builds = Arc::new(AtomicI32::new(0));
    let cache = Arc::new(RecordingCache { inner: Arc::new(MemoryCache::new(1 << 20)), last: Default::default() });
    let (rel, bd, c) = (release.clone(), builds.clone(), cache.clone());
    let h = new_handler(test_deps(move |d| {
        d.cache = c.clone();
        let (rel, bd) = (rel.clone(), bd.clone());
        d.make_scrapers = Arc::new(move |_| {
            let (rel, bd) = (rel.clone(), bd.clone());
            vec![fake_scraper("torrentio", move || {
                let (rel, bd) = (rel.clone(), bd.clone());
                Box::pin(async move {
                    if bd.fetch_add(1, Ordering::SeqCst) > 0 {
                        rel.notified().await; // hold the rebuild open so the flood lands while it is in flight
                    }
                    Ok(test_seeds())
                })
            })]
        });
    }));
    let path = format!("/{}/stream/movie/tt1234567.json", valid_blob());
    get(&h, &path, &[]).await; // seed
    let key = cache.last_key();
    let held = cache.get(&key).unwrap_or_default();
    let (complete, _, etag, body) = crate::handler::split_cached(&held);
    cache.put(&key, &crate::handler::join_cached(complete, now_unix() - 1, &etag, &body), Duration::from_secs(60));
    // Go counts goroutines; the runtime's live tasks are the same question for this port.
    let live_tasks = || tokio::runtime::Handle::current().metrics().num_alive_tasks();
    let before = live_tasks();
    for i in 0..300 {
        assert_eq!(get(&h, &path, &[]).await.status(), 200, "stale hit {i}");
    }
    let grew = live_tasks().saturating_sub(before);
    release.notify_waiters();
    assert!(grew <= 5, "300 stale hits grew the goroutine count by {grew}, want ~1");
}

#[test]
fn rebuild_gate() {
    // Go TestRebuildGate: booking, the cool-off after a degraded rebuild, and its expiry, against injected times.
    let mut h = crate::handler::RebuildGate::default();
    let t0 = std::time::Instant::now();
    let budget = Duration::from_secs(28);
    let (gen, ok) = h.book_rebuild("k", t0, t0 + budget);
    assert!(ok, "first booking refused");
    assert!(!h.book_rebuild("k", t0 + Duration::from_secs(1), t0 + budget).1, "a second booking was granted while the first was live");
    // A healthy rebuild clears the key immediately, so the next expiry rebuilds at once.
    h.release_rebuild("k", gen, None);
    let (gen2, ok) = h.book_rebuild("k", t0 + Duration::from_secs(2), t0 + budget);
    assert!(ok, "a released key refused the next booking");
    // A DEGRADED rebuild installs a cool-off, which outlives the booking window it replaces.
    h.release_rebuild("k", gen2, Some(t0 + crate::handler::REBUILD_COOLOFF));
    assert!(!h.book_rebuild("k", t0 + budget + Duration::from_secs(1), t0 + budget).1, "the cool-off did not outlive the booking window");
    assert!(h.book_rebuild("k", t0 + crate::handler::REBUILD_COOLOFF + Duration::from_secs(1), t0 + budget).1, "the cool-off never expired");
}

#[test]
fn rebuild_gate_bounds_concurrent_rebuilds() {
    // Go TestRebuildGate_boundsConcurrentRebuilds: a global ceiling on concurrent rebuilds, in addition to the
    // per-key one. Finishing one frees exactly one slot.
    let mut h = crate::handler::RebuildGate::default();
    let t0 = std::time::Instant::now();
    let mut gens = std::collections::HashMap::new();
    for i in 0..crate::handler::MAX_CONCURRENT_REBUILDS {
        let key = format!("k{i}");
        let (gen, ok) = h.book_rebuild(&key, t0, t0 + Duration::from_secs(60));
        assert!(ok, "booking {i} refused below the ceiling");
        gens.insert(key, gen);
    }
    assert!(!h.book_rebuild("one-too-many", t0, t0 + Duration::from_secs(60)).1, "booked more than the ceiling");
    h.release_rebuild("k0", gens["k0"], None);
    assert!(h.book_rebuild("after-release", t0, t0 + Duration::from_secs(60)).1, "a freed slot was not reusable");
}

#[test]
fn rebuild_gate_swept_lease_still_returns_its_slot() {
    // Go TestRebuildGate_sweptLeaseStillReturnsItsSlot: a lease swept by a later booking still returns its slot.
    let mut h = crate::handler::RebuildGate::default();
    let t0 = std::time::Instant::now();
    let (gen, _) = h.book_rebuild("overrun", t0, t0 + Duration::from_secs(1));
    let later = t0 + Duration::from_secs(10);
    let (sweeper, _) = h.book_rebuild("sweeper", later, later + Duration::from_secs(60));
    assert!(!h.holds("overrun"), "the expired lease was not swept");
    h.release_rebuild("overrun", gen, None);
    h.release_rebuild("sweeper", sweeper, None);
    for i in 0..crate::handler::MAX_CONCURRENT_REBUILDS {
        assert!(h.book_rebuild(&format!("fresh{i}"), later, later + Duration::from_secs(60)).1, "only {i} slots came back");
    }
}

#[test]
fn rebuild_gate_stale_lease_releases_nothing() {
    // Go TestRebuildGate_staleLeaseReleasesNothing: a superseded lease neither deletes a live booking nor a
    // cool-off. The fencing token (the generation) is what tells them apart.
    let mut h = crate::handler::RebuildGate::default();
    let t0 = std::time::Instant::now();
    let (old, _) = h.book_rebuild("k", t0, t0 + Duration::from_secs(1));
    let (fresh, ok) = h.book_rebuild("k", t0 + Duration::from_secs(2), t0 + Duration::from_secs(30));
    assert!(ok && fresh != old, "second booking");
    h.release_rebuild("k", old, None);
    assert!(!h.book_rebuild("k", t0 + Duration::from_secs(3), t0 + Duration::from_secs(30)).1, "a superseded lease deleted the live booking");
    h.release_rebuild("k", fresh, Some(t0 + Duration::from_secs(3600)));
    let (late, ok) = h.book_rebuild("other", t0 + Duration::from_secs(4), t0 + Duration::from_secs(30));
    assert!(ok, "could not book a second key");
    h.release_rebuild("k", late, None); // right gen, wrong key: must not touch k's cool-off
    assert!(!h.book_rebuild("k", t0 + Duration::from_secs(5), t0 + Duration::from_secs(30)).1, "a superseded lease wiped the cool-off");
    h.release_rebuild("other", late, None);
}

#[test]
fn rebuild_gate_slot_accounting_balances() {
    // Go TestRebuildGate_slotAccountingBalances: every granted booking returns its slot exactly once, so the
    // ceiling neither seizes up nor loosens.
    let mut h = crate::handler::RebuildGate::default();
    let t0 = std::time::Instant::now();
    for round in 0..5 {
        let mut gens = Vec::new();
        for i in 0..crate::handler::MAX_CONCURRENT_REBUILDS {
            let (gen, ok) = h.book_rebuild(&format!("r{round}-k{i}"), t0, t0 + Duration::from_secs(60));
            assert!(ok, "round {round}: booking {i} refused — slots did not come back");
            gens.push(gen);
        }
        for (i, gen) in gens.into_iter().enumerate() {
            h.release_rebuild(&format!("r{round}-k{i}"), gen, None);
        }
    }
    assert_eq!(h.live(), 0, "after balanced book/release pairs the live count is not 0");
}

#[tokio::test]
async fn stream_list_outage_does_not_scrape_per_request() {
    // Go TestStreamList_outageDoesNotScrapePerRequest: 20 stale hits during an outage produce at most 3 scrapes.
    let healthy = Arc::new(std::sync::atomic::AtomicBool::new(true));
    let scrapes = Arc::new(AtomicI32::new(0));
    let cache = Arc::new(RecordingCache { inner: Arc::new(MemoryCache::new(1 << 20)), last: Default::default() });
    let (hl, sc, c) = (healthy.clone(), scrapes.clone(), cache.clone());
    let h = new_handler(test_deps(move |d| {
        d.cache = c.clone();
        let (hl, sc) = (hl.clone(), sc.clone());
        d.make_scrapers = Arc::new(move |_| {
            let (hl, sc) = (hl.clone(), sc.clone());
            vec![fake_scraper("torrentio", move || {
                let (hl, sc) = (hl.clone(), sc.clone());
                Box::pin(async move {
                    sc.fetch_add(1, Ordering::SeqCst);
                    if !hl.load(Ordering::SeqCst) {
                        return Err(Box::new(std::io::Error::other("canceled")) as DoError);
                    }
                    Ok(test_seeds())
                })
            })]
        });
    }));
    let path = format!("/{}/stream/movie/tt1234567.json", valid_blob());
    get(&h, &path, &[]).await;
    let key = cache.last_key();
    let held = cache.get(&key).unwrap_or_default();
    let (complete, _, etag, body) = crate::handler::split_cached(&held);
    healthy.store(false, Ordering::SeqCst);
    for _ in 0..20 {
        // Re-stale the entry before each request, as a real expired entry would be.
        cache.put(&key, &crate::handler::join_cached(complete, now_unix() - 1, &etag, &body), Duration::from_secs(60));
        get(&h, &path, &[]).await;
    }
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(scrapes.load(Ordering::SeqCst) <= 3, "scrapes for 20 stale hits during an outage — the cool-off is not holding");
}

#[tokio::test]
async fn stream_list_partial_list_gets_no_stale_window() {
    // Go TestStreamList_partialListGetsNoStaleWindow: a partial list must never be served stale, and carries no
    // freshness stamp.
    let cache = Arc::new(RecordingCache { inner: Arc::new(MemoryCache::new(1 << 20)), last: Default::default() });
    let c = cache.clone();
    let h = new_handler(test_deps(move |d| {
        d.cache = c.clone();
        d.make_scrapers = Arc::new(|_| {
            vec![
                fake_scraper("torrentio", || Box::pin(async { Ok(test_seeds()) })),
                fake_scraper("comet", || Box::pin(async { Err(Box::new(std::io::Error::other("canceled")) as DoError) })),
            ]
        });
    }));
    assert_eq!(get(&h, &format!("/{}/stream/movie/tt1234567.json", valid_blob()), &[]).await.status(), 200, "partial build");
    let held = cache.get(&cache.last_key()).expect("the partial build cached nothing");
    let (complete, fresh_until, _, _) = crate::handler::split_cached(&held);
    assert!(!complete, "a scrape with a failed indexer was recorded as complete");
    assert_eq!(fresh_until, 0, "a partial list carries a freshness stamp");
}

#[test]
fn split_cached_reads_an_older_entry_conservatively() {
    // Go TestSplitCached_readsAnOlderEntryConservatively: an entry with no completeness recorded is read as
    // incomplete, and the current format round-trips, a body with a separator is not truncated, and the
    // freshness stamp round-trips beside completeness.
    let (complete, _, etag, body) = crate::handler::split_cached("W/\"abc\"\x00{\"streams\":[]}");
    assert!(!complete, "an entry with no completeness recorded must not be assumed complete");
    assert_eq!((etag, body), ("W/\"abc\"", "{\"streams\":[]}"), "legacy entry mis-split");
    for want in [true, false] {
        let joined = crate::handler::join_cached(want, 0, "E", "B");
        let (got, _, e, b) = crate::handler::split_cached(&joined);
        assert!(got == want && e == "E" && b == "B", "round trip complete={want}");
    }
    let joined = crate::handler::join_cached(true, 0, "E", "a\x00b");
    let (_, _, _, weird) = crate::handler::split_cached(&joined);
    assert_eq!(weird, "a\x00b", "body with a separator was truncated");
    let joined = crate::handler::join_cached(true, 1757000000, "E", "B");
    let (c, f, e, b) = crate::handler::split_cached(&joined);
    assert!(c && f == 1757000000 && e == "E" && b == "B", "stamped round trip");
    // A three-field entry from a build that predates the stamp reports freshUntil 0: no stamp.
    let (c, f, _, _) = crate::handler::split_cached("1\x00E\x00B");
    assert!(c && f == 0, "pre-stamp entry: complete={c} freshUntil={f}, want complete with no stamp");
}

#[test]
fn split_cached_treats_every_unknown_form_the_same_way() {
    // Go TestSplitCached_treatsEveryUnknownFormTheSameWay: a value with no separator is read the same way as the
    // legacy one-separator form, and a separator-less value is served whole.
    let (bare_complete, _, _, bare_body) = crate::handler::split_cached("just-a-body");
    let (sep_complete, _, _, _) = crate::handler::split_cached("etag\x00body");
    assert_eq!(bare_complete, sep_complete, "unknown completeness answered differently with and without a separator");
    assert!(!bare_complete, "completeness that was never recorded must not be assumed");
    assert_eq!(bare_body, "just-a-body", "a separator-less value must be served whole");
}

#[tokio::test]
#[ignore = "Go's deadlineStore reads the deadline each Status call arrives with (ctx.Deadline()); a Rust store takes no deadline, so a prompt read cannot report its budget. See handle_play_a_status_read_that_cannot_answer_is_bounded_by_the_status_budget for the half that can be measured"]
async fn handle_play_poll_reads_use_the_status_budget() {
    // Kept as the Go body: it needs a store-level deadline that the Store trait does not carry.
    #[cfg(any())]
    async fn pending() {
        // Go TestHandlePlay_pollReadsUseTheStatusBudget: a poll-route status read never takes the resolve budget,
        // and only the escalation may take double the status budget.
        for route in ["", "?probe=1"] {
            for unknown in [false, true] {
                let store = DeadlineStore::new(DebridService::TorBox, unknown);
                let seen = store.status_at.clone();
                let h = new_handler(test_deps(move |d| d.make_stores = Arc::new(move |_| vec![Box::new(store.clone())])));
                let tok = crate::play::encode_play_token(&crate::play::PlayTarget { info_hash: "a".repeat(40), ..Default::default() });
                get(&h, &format!("/{}/play/{tok}{route}", valid_blob()), &[]).await;
                let reads = seen.lock().expect("the lock").clone();
                assert!(!reads.is_empty(), "the status read never happened");
                let ceiling = 2 * crate::handler::STATUS_BUDGET;
                let doubled = reads.iter().filter(|l| **l > crate::handler::STATUS_BUDGET).count();
                assert!(reads.iter().all(|l| *l <= ceiling), "a status read exceeded the 2x ceiling: {reads:?}");
                let want = usize::from(unknown && route.is_empty());
                assert_eq!(doubled, want, "reads exceeding the ordinary status budget: {reads:?}");
            }
        }
    }
    todo!("pending: handler_test.go:TestHandlePlay_pollReadsUseTheStatusBudget")
}

#[tokio::test]
async fn handle_play_the_escalation_re_asks_only_the_store_that_could_not_answer() {
    // Go TestHandlePlay_theEscalationReAsksOnlyTheStoreThatCouldNotAnswer: the escalation re-asks only the store
    // that could not answer. Three reads happen: poll, escalation, post-failure. Only the escalation is narrowed.
    let stores = vec![
        AnsweringStore::new(DebridService::RealDebrid, StatusAnswer::No),
        AnsweringStore::new(DebridService::TorBox, StatusAnswer::Unknown),
        AnsweringStore::new(DebridService::Premiumize, StatusAnswer::No),
    ];
    let handles = stores.iter().map(|s| s.asked.clone()).collect::<Vec<_>>();
    let h = new_handler(test_deps(move |d| d.make_stores = Arc::new(move |_| stores.clone().into_iter().map(|s| Box::new(s) as Box<dyn Store>).collect())));
    let tok = crate::play::encode_play_token(&crate::play::PlayTarget { info_hash: "a".repeat(40), ..Default::default() });
    get(&h, &format!("/{}/play/{tok}", valid_blob()), &[]).await;
    assert_eq!(handles[1].load(Ordering::SeqCst), 3, "the store that could not answer was read the wrong number of times");
    assert!(handles[0].load(Ordering::SeqCst) == 2 && handles[2].load(Ordering::SeqCst) == 2, "definitive stores were re-asked");
}

#[test]
fn escalated_status_ctx_cannot_outlive_the_resolve_budget() {
    // Go TestEscalatedStatusCtx_cannotOutliveTheResolveBudget: the escalated status read is bounded by its own
    // ceiling and can never outlive the resolve budget it is gating.
    //
    // The deadlines are explicit instants here, as they are in the handler: Go's contexts carry them, this port
    // hands them down. `status_budget` is the handler's (the shipped eight seconds).
    use crate::handler::{escalated_status_budget, escalated_status_ctx, DEFAULT_STATUS_BUDGET as STATUS_BUDGET};
    let now = tokio::time::Instant::now();
    // A resolve budget with plenty left: the escalation is granted, and bounded by its own ceiling.
    let parent = now + crate::handler::RESOLVE_BUDGET;
    let got = escalated_status_ctx(Some(parent), now, STATUS_BUDGET).expect("a full resolve budget refused the escalation");
    // Spelled out rather than taken from escalated_status_budget(), which is what the code under test derives the
    // deadline from — asserting a value against its own source pins nothing.
    let left = got - now;
    assert!(left <= 2 * STATUS_BUDGET, "escalated read got {left:?}, want no more than the {:?} ceiling", 2 * STATUS_BUDGET);
    assert_eq!(escalated_status_budget(STATUS_BUDGET), 2 * STATUS_BUDGET, "the escalation is not twice the status budget");
    assert!(got <= parent, "the escalated read outlives the resolve budget it is gating — the add becomes impossible");

    // Too little left to do the add afterwards: declined, so the resolve keeps what remains.
    let tight = now + escalated_status_budget(STATUS_BUDGET);
    assert!(escalated_status_ctx(Some(tight), now, STATUS_BUDGET).is_none(), "escalated with too little budget left to add afterwards");
    // A parent with no deadline at all has nothing to carve from.
    assert!(escalated_status_ctx(None, now, STATUS_BUDGET).is_none(), "escalated from a context with no deadline");
}

// A status budget the Rust stores cannot see. Go's `deadlineStore` and `recordingCheckStore` read `ctx.Deadline()` to
// learn the budget the handler built; a Rust store takes no deadline and is simply dropped when the budget is spent,
// so the two ignored tests below cannot read it. What a store CAN observe is how long it was allowed to run, and
// these two tests measure that instead: a store that outlasts any budget, on a paused clock, records how long it
// lived before the handler dropped it.

/// A store whose reads never answer, and which remembers how long each one lived before being dropped.
#[derive(Clone)]
struct LifetimeStore {
    svc: DebridService,
    /// Whether a resolve serves a link; otherwise it finds nothing, as Go's `deadlineStore` does.
    resolves: bool,
    status_lived: Arc<std::sync::Mutex<Vec<Duration>>>,
    check_lived: Arc<std::sync::Mutex<Vec<Duration>>>,
}

impl LifetimeStore {
    fn new(svc: DebridService, resolves: bool) -> LifetimeStore {
        LifetimeStore { svc, resolves, status_lived: Default::default(), check_lived: Default::default() }
    }
}

/// Records, on drop, how long the read that held it lived (tokio time, so a paused clock reads exactly).
struct Lived {
    since: tokio::time::Instant,
    into: Arc<std::sync::Mutex<Vec<Duration>>>,
}

impl Drop for Lived {
    fn drop(&mut self) {
        self.into.lock().expect("the lock").push(self.since.elapsed());
    }
}

/// Outlasts any budget the handler can hand out (the resolve budget is 45 s).
async fn outlast(into: &Arc<std::sync::Mutex<Vec<Duration>>>) {
    let _lived = Lived { since: tokio::time::Instant::now(), into: Arc::clone(into) };
    tokio::time::sleep(Duration::from_secs(100)).await;
}

#[async_trait]
impl Store for LifetimeStore {
    fn service(&self) -> DebridService {
        self.svc
    }

    async fn cache_check(&self, _hashes: &[String]) -> Result<HashMap<String, bool>, StoreError> {
        outlast(&self.check_lived).await;
        Ok(HashMap::new())
    }

    async fn resolve(&self, _t: &ResolveTarget) -> Result<String, StoreError> {
        if self.resolves {
            Ok("https://cdn.example/added.mkv".to_owned())
        } else {
            Err(dead_link("nothing here"))
        }
    }

    async fn status(&self, _t: &ResolveTarget) -> Option<StoreStatus> {
        outlast(&self.status_lived).await;
        None
    }

    async fn status_answer(&self, _t: &ResolveTarget) -> Option<(StoreStatus, StatusAnswer)> {
        outlast(&self.status_lived).await;
        Some((StoreStatus::default(), StatusAnswer::Unknown))
    }
}

// Not in the Go suite: `handle_play_poll_reads_use_the_status_budget` for the half Rust can see. A status read that
// cannot answer is the one that gets escalated; it must live no longer than the status budget, the escalated retry
// no longer than twice it, and the probe route, which never escalates, only the first.
#[tokio::test(start_paused = true)]
async fn handle_play_a_status_read_that_cannot_answer_is_bounded_by_the_status_budget() {
    let budget = crate::handler::DEFAULT_STATUS_BUDGET;
    for (route, want_reads, want_doubled) in [("", 3usize, 1usize), ("?probe=1", 1, 0)] {
        let store = LifetimeStore::new(DebridService::TorBox, false);
        let lived = store.status_lived.clone();
        let h = new_handler(test_deps(move |d| d.make_stores = Arc::new(move |_| vec![Box::new(store.clone())])));
        let tok = crate::play::encode_play_token(&crate::play::PlayTarget { info_hash: "a".repeat(40), ..Default::default() });
        get(&h, &format!("/{}/play/{tok}{route}", valid_blob()), &[]).await;

        let reads = lived.lock().expect("the lock").clone();
        assert!(!reads.is_empty(), "play{route}: the status read never happened, so this asserts nothing");
        // The rule, stated exactly: a read on the poll route never takes the RESOLVE budget, and at most one — the
        // escalation, on the path about to spend an add — may take double the status budget. The ceiling is 2x
        // spelled out, not escalated_status_budget(), which is what the handler derives the deadline from.
        let ceiling = 2 * budget;
        for (i, left) in reads.iter().enumerate() {
            assert!(
                *left <= ceiling,
                "play{route}: status read {i} lived {left:?}, want no more than {ceiling:?} — a poll read must not be able to hold the poll for the whole resolve budget"
            );
        }
        let doubled = reads.iter().filter(|l| **l > budget).count();
        assert_eq!(doubled, want_doubled, "play{route}: reads exceeding the ordinary status budget: {reads:?}");
        assert_eq!(reads.len(), want_reads, "play{route}: status reads: {reads:?}");
    }
}

// Not in the Go suite: `play_the_preference_cache_check_is_budgeted` for what Rust can see. /play's preference cache
// check gets a slice of the resolve clock, never the whole thing, and a check that cannot answer leaves the play able
// to resolve.
#[tokio::test(start_paused = true)]
async fn play_the_preference_cache_check_lives_no_longer_than_the_status_budget() {
    let store = LifetimeStore::new(DebridService::TorBox, true);
    let checks = store.check_lived.clone();
    let h = new_handler(test_deps(move |d| d.make_stores = Arc::new(move |_| vec![Box::new(store.clone())])));
    h.set_status_budget(Duration::from_millis(150));
    // TWO accounts: the preference check is gated on more than one being configured.
    let two = blob(r#"{"debrid":[{"service":"torbox","token":"tb"},{"service":"realdebrid","token":"rd"}],"indexers":["torrentio"],"resultCap":20}"#);
    let tok = crate::play::encode_play_token(&crate::play::PlayTarget { info_hash: "c".repeat(40), ..Default::default() });

    let rr = get(&h, &format!("/{two}/play/{tok}"), &[]).await;
    assert_eq!(rr.status(), 302, "play: {}, want 302", rr.status());

    let lived = checks.lock().expect("the lock").clone();
    let Some(got) = lived.first() else { panic!("no cache check was made, so this test is not exercising its budget") };
    // The status budget exactly, not "roughly": handed the resolve clock the check lives 45 s and the resolve that
    // follows runs on what is left of it.
    assert!(
        *got <= Duration::from_millis(150),
        "the preference cache check lived {got:?}, want about the 150ms status budget — handed the resolve clock it can spend all of it and leave the resolve an expired one, which queues nothing and answers 404 for a release that is alive"
    );
}

// The probe DISCOVERS refusals; it just used to throw them away, inspecting only `err == nil` and then
// relying on the backoff cache to have been written by someone else. A refusal that reaches it
// unrecorded fell through to 404 "not_queued" — a claim, not a shrug — so during a throttle the very URL
// the client polls to draw its progress bar said nothing was queued while /play said the debrid was
// refusing. Both unrecorded classes are driven here: errNoFileList, which is never recorded anywhere,
// and TorBox's warm-entry fast path.
#[tokio::test]
async fn handle_probe_reports_a_refusal_it_discovers_itself() {
    use crate::cache::{Cache, MemoryCache};
    use crate::stores::refusal::{resolve_key, torrent_id_key};
    use crate::stores::torbox::{TorBoxStore, RESOLVE_CACHE_TTL, TORBOX_API};

    let held = "a".repeat(40);
    type Reply = Box<dyn Fn(&reqwest::Request) -> reqwest::Response + Send + Sync>;
    let unusable_held = held.clone();
    let throttled_held = held.clone();
    let cases: Vec<(&str, bool, Reply)> = vec![
        (
            "mylist answers something unusable",
            false,
            Box::new(move |r| {
                if r.url().path().contains("mylist") {
                    return resp(200, r#"{"unexpected":"envelope"}"#);
                }
                resp(200, &format!(r#"{{"data":{{"{unusable_held}":{{"name":"x"}}}}}}"#))
            }),
        ),
        (
            "a throttled link request on a warm entry",
            true,
            Box::new(move |r| {
                if r.url().path().contains("requestdl") {
                    return resp(429, "{}");
                }
                if r.url().path().contains("mylist") {
                    return resp(200, r#"{"data":{"id":42,"files":[{"id":0,"name":"m.mkv","size":9}]}}"#);
                }
                resp(200, &format!(r#"{{"data":{{"{throttled_held}":{{"name":"x"}}}}}}"#))
            }),
        ),
    ];
    for (name, warm, reply) in cases {
        let cache = Arc::new(MemoryCache::new(1 << 20));
        // The account HOLDS it, so the probe's NoAdd resolve reaches the read path rather than
        // returning "this would queue" before it asks anything.
        cache.put(&torrent_id_key("tb-secret", &held), "42", RESOLVE_CACHE_TTL);
        if warm {
            cache.put(&resolve_key("tb-secret", &held), r#"{"torrentId":42,"files":[{"Index":0,"Name":"m.mkv","SizeBytes":9}]}"#, RESOLVE_CACHE_TTL);
        }
        let store: Arc<dyn Store> = Arc::new(TorBoxStore {
            token: "tb-secret".to_owned(),
            cache: Some(cache),
            api: TORBOX_API.to_owned(),
            client: Arc::new(crate::tests::mock::MockDoer(move |r: reqwest::Request| Ok(reply(&r)))),
        });
        let h = new_handler(test_deps(move |d| d.make_stores = crate::tests::handler::shared_stores(store.clone())));
        let tok = crate::play::encode_play_token(&crate::play::PlayTarget {
            info_hash: held.clone(),
            season: Some(1),
            episode: Some(2),
            ..Default::default()
        });
        let rec = get(&h, &format!("/{}/play/{tok}?probe=1", valid_blob()), &[]).await;
        assert_ne!(
            rec.status(),
            404,
            "{name}: probe answered {} {} — a refusal it saw itself was reported as an absence",
            rec.status(),
            body_str(&rec).trim()
        );
    }
}

#[tokio::test]
#[ignore = "Go's recordingCheckStore reads the deadline its CacheCheck arrives with (ctx.Deadline()); a Rust store takes no deadline. See play_the_preference_cache_check_lives_no_longer_than_the_status_budget for the same property measured as how long the check lived"]
async fn play_the_preference_cache_check_is_budgeted() {
    // Kept as the Go body: it needs a store-level deadline that the Store trait does not carry.
    #[cfg(any())]
    async fn pending() {
        // Go TestPlay_thePreferenceCacheCheckIsBudgeted: /play's preference cache check gets a slice of the resolve
        // clock, never the whole thing. Asserted on the deadline the store is handed.
        let _budget = crate::handler::set_status_budget(Duration::from_millis(150));
        let store = RecordingCheckStore::new(DebridService::TorBox);
        let budgets = store.budgets.clone();
        let h = new_handler(test_deps(move |d| d.make_stores = Arc::new(move |_| vec![Box::new(store.clone())])));
        // TWO accounts: the preference check is gated on more than one being configured.
        let two = blob(r#"{"debrid":[{"service":"torbox","token":"tb"},{"service":"realdebrid","token":"rd"}],"indexers":["torrentio"],"resultCap":20}"#);
        let tok = crate::play::encode_play_token(&crate::play::PlayTarget { info_hash: "c".repeat(40), ..Default::default() });
        assert_eq!(get(&h, &format!("/{two}/play/{tok}"), &[]).await.status(), 302, "play");
        let got = budgets.recv_timeout(Duration::from_secs(1)).expect("no cache check was made");
        assert!(got <= crate::handler::STATUS_BUDGET, "the preference cache check was given {got:?} of budget, want about the status budget");
    }
    todo!("pending: handler_test.go:TestPlay_thePreferenceCacheCheckIsBudgeted")
}

#[tokio::test]
#[ignore = "Go resolves with an already-cancelled context and asserts nothing was charged or sent; a Rust store takes no context, a spent clock is a dropped future, and a future dropped before its first poll has no result to assert on"]
async fn resolve_a_spent_clock_does_not_buy_an_add() {
    // Kept as the Go body: it needs a store-level deadline that the Store trait does not carry.
    #[cfg(any())]
    async fn pending() {
        // Go TestResolve_aSpentClockDoesNotBuyAnAdd: a resolve on a spent clock must not charge an add, and issues
        // no upstream request.
        let token = "spent-clock-resolve".to_owned();
        let hash = "d".repeat(40);
        let before = global_add_budget().remaining(&budget_account(DebridService::TorBox, &token));
        let upstream = Arc::new(AtomicI32::new(0));
        let u = upstream.clone();
        let s = TorBoxStore::with_client(&token, Arc::new(MemoryCache::new(1 << 20)), MockDoer::new(move |_| {
            u.fetch_add(1, Ordering::SeqCst);
            resp(200, r#"{"data":{}}"#)
        }));
        let ctx = CancelledCtx::new();
        assert!(s.resolve_in(&ctx, &ResolveTarget { info_hash: hash, ..Default::default() }).await.is_err(), "a resolve on a spent clock should not succeed");
        assert_eq!(global_add_budget().remaining(&budget_account(DebridService::TorBox, &token)), before, "the hourly add budget moved on a spent clock");
        assert_eq!(upstream.load(Ordering::SeqCst), 0, "upstream requests were issued on an expired context");
    }
    todo!("pending: handler_test.go:TestResolve_aSpentClockDoesNotBuyAnAdd")
}

#[tokio::test]
#[ignore = "Go calls ResolvePreferring with an already-cancelled context so that the first store fails before it charges; ResolvePreferring takes no context here and no store can be told the clock is spent, so the first store would be asked (and charged) for real"]
async fn resolve_preferring_a_spent_clock_still_surfaces_an_in_flight_add() {
    // Kept as the Go body: it needs a store-level deadline that the Store trait does not carry.
    #[cfg(any())]
    async fn pending() {
        // Go TestResolvePreferring_aSpentClockStillSurfacesAnInFlightAdd: a spent clock still reports an add scout
        // already has out, which is a pure cache read ahead of the charge, so it costs nothing.
        let token = "inflight-resolve-preferring".to_owned();
        let hash = "e".repeat(40);
        let cache = Arc::new(MemoryCache::new(1 << 20));
        note_add_attempt(&cache, DebridService::RealDebrid, &token, &hash);
        let quiet = MockDoer::new(|_| resp(200, r#"{"data":{}}"#));
        let pool = StorePool::new(vec![
            Box::new(TorBoxStore::with_client(&token, cache.clone(), quiet.clone())),
            Box::new(RealDebridStore::with_client(&token, cache.clone(), quiet)),
        ]);
        let ctx = CancelledCtx::new();
        let err = pool.resolve_preferring(&ctx, &ResolveTarget { info_hash: hash, ..Default::default() }, None).await;
        assert!(matches!(err, Err(StoreError::AddInFlight)), "a spent clock reported {err:?}, want AddInFlight");
    }
    todo!("pending: handler_test.go:TestResolvePreferring_aSpentClockStillSurfacesAnInFlightAdd")
}

#[tokio::test]
async fn play_status_timeout_does_not_add() {
    // Go TestPlay_statusTimeoutDoesNotAdd: a timed-out status read is not "nobody is fetching it", so it leads to
    // no add. The second, longer read is what answers.
    // The shipped 8s is not a unit-test wait, but not so small that scheduling jitter can spend it before the read
    // is even attempted — StorePool::status declines to ask a store whose budget is already gone.
    let store = SlowStatusStore::new(DebridService::TorBox, 1, false); // slow once, then answers
    let resolves = store.resolves.clone();
    let hits = store.status_hits.clone();
    let h = new_handler(test_deps(move |d| d.make_stores = Arc::new(move |_| vec![Box::new(store.clone())])));
    h.set_status_budget(Duration::from_millis(150));
    let tok = crate::play::encode_play_token(&crate::play::PlayTarget { info_hash: "a".repeat(40), ..Default::default() });
    assert_eq!(get(&h, &format!("/{}/play/{tok}", valid_blob()), &[]).await.status(), 202, "the release is downloading");
    assert_eq!(resolves.load(Ordering::SeqCst), 0, "a timed-out status read still queued the torrent");
    assert_eq!(hits.load(Ordering::SeqCst), 2, "status calls: the short read, then the definitive one");
}

#[tokio::test]
async fn play_prompt_not_downloading_still_adds() {
    // Go TestPlay_promptNotDownloadingStillAdds: a prompt "not downloading" answer is a real answer, and the add
    // proceeds.
    let store = SlowStatusStore::new(DebridService::TorBox, 0, true); // answers at once, and says no
    let resolves = store.resolves.clone();
    let h = new_handler(test_deps(move |d| d.make_stores = Arc::new(move |_| vec![Box::new(store.clone())])));
    h.set_status_budget(Duration::from_millis(150));
    let tok = crate::play::encode_play_token(&crate::play::PlayTarget { info_hash: "b".repeat(40), ..Default::default() });
    assert_eq!(get(&h, &format!("/{}/play/{tok}", valid_blob()), &[]).await.status(), 302, "play");
    assert_eq!(resolves.load(Ordering::SeqCst), 1, "a prompt 'not downloading' must still add");
}

#[tokio::test]
async fn play_multi_store_status_timeout_does_not_add() {
    // Go TestPlay_multiStoreStatusTimeoutDoesNotAdd: with several accounts, a store that ran out of time adds
    // nothing, and the other stores' prompt "no" does not end the pool early.
    let slow = SlowStatusStore::new(DebridService::TorBox, 1, false); // TorBox is slow once, then reports the download
    let slow_resolves = slow.resolves.clone();
    let quick_resolves = Arc::new(AtomicI32::new(0));
    let quick = |svc: DebridService| CountingResolveStore::new(SlowStatusStore::new(svc, 0, true), quick_resolves.clone());
    let stores: Vec<Box<dyn Store>> = vec![Box::new(slow), Box::new(quick(DebridService::RealDebrid)), Box::new(quick(DebridService::Premiumize))];
    let h = new_handler(test_deps(move |d| {
        let stores = std::sync::Mutex::new(Some(stores));
        d.make_stores = Arc::new(move |_| stores.lock().expect("the lock").take().unwrap_or_default());
    }));
    h.set_status_budget(Duration::from_millis(150));
    let tok = crate::play::encode_play_token(&crate::play::PlayTarget { info_hash: "a".repeat(40), ..Default::default() });
    assert_eq!(get(&h, &format!("/{}/play/{tok}", valid_blob()), &[]).await.status(), 202, "TorBox is downloading it");
    assert_eq!(slow_resolves.load(Ordering::SeqCst) + quick_resolves.load(Ordering::SeqCst), 0, "a store that ran out of time still led to a resolve");
}
