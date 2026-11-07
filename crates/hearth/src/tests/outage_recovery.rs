//! Scout recovers from an upstream outage without a restart.
//!
//! The whole node is served on a real socket (`serve_until`, admission included) and lists through `/den`, the route
//! clients use. Scout's production outbound stack is kept — the pause-on-refusal transport over the guarded client,
//! the real indexer scrapers and the real TorBox store — and pointed at local fakes: the indexers through their
//! `*_URL` overrides, TorBox by rewriting its fixed API origin under the pause layer, so pause keys are TorBox's own.
//! Each fake carries a fault switch: blackhole (the request is read and never answered, until the switch moves),
//! reset (the connection closes without an answer), or a throttle naming a `Retry-After`.
//!
//! The clocks involved are wall clocks (list freshness in Unix seconds, the cool-off and TorBox's probe window on
//! `Instant`, `Retry-After` in whole seconds), so the windows are shortened through the existing knobs
//! (`SCRAPE_TIMEOUT_SECS`, `LIST_TTL_SECS`) and the test-only `shorten_recovery_for_test`, and every wait is either a
//! wait for a wall-clock second that must pass or a bounded wait on observable state (a fake's request count, a
//! `/metrics` gauge). No fixed sleep stands in for a synchronisation.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use base64::Engine;
use den_scout::deps::{settings_from_env, shared_profile, GuardedClient};
use den_scout::scrape::{DoError, Doer};
use den_scout::stores::torbox::TORBOX_API;
use den_scout::upstreampause::pause_on_refusal;

use super::*;

const TOKEN: &str = "metrics-fixture";
/// What `shorten_recovery_for_test` puts in place of the production minute and half-minute.
const COOLOFF: Duration = Duration::from_secs(1);
const PROBE_EVERY: Duration = Duration::from_secs(1);
/// `SCRAPE_TIMEOUT_SECS` here: a blackholed indexer costs a build this long.
const SCRAPE_TIMEOUT: Duration = Duration::from_secs(1);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Fault {
    Up,
    /// Read the request and never answer it, until the switch moves: the caller's own deadline ends it.
    Blackhole,
    /// Close the connection without an answer: a fast transport failure.
    Reset,
    /// Answer with this status and `Retry-After: <secs>`.
    Throttle(u16, u64),
}

type Reply = Arc<dyn Fn(&str) -> (StatusCode, String) + Send + Sync>;

/// Counts down when dropped, so a gauge follows a request or connection however it ends.
struct Held(Arc<AtomicUsize>);

impl Held {
    fn new(gauge: &Arc<AtomicUsize>) -> Held {
        gauge.fetch_add(1, Ordering::SeqCst);
        Held(gauge.clone())
    }
}

impl Drop for Held {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}

/// One fake upstream on its own port, behind its fault switch.
struct Upstream {
    base: String,
    fault: Arc<watch::Sender<Fault>>,
    /// Every request that reached it, whatever the switch said.
    requests: Arc<AtomicUsize>,
    in_flight: Arc<AtomicUsize>,
    connections: Arc<AtomicUsize>,
}

impl Upstream {
    async fn spawn(reply: Reply) -> Upstream {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        let fault = Arc::new(watch::channel(Fault::Up).0);
        let gauge = || Arc::new(AtomicUsize::new(0));
        let (requests, in_flight, connections) = (gauge(), gauge(), gauge());
        let (f, r, i, c) = (fault.clone(), requests.clone(), in_flight.clone(), connections.clone());
        tokio::spawn(async move {
            while let Ok((stream, _)) = listener.accept().await {
                let open = Held::new(&c);
                let (fault, requests, in_flight, reply) = (f.clone(), r.clone(), i.clone(), reply.clone());
                tokio::spawn(async move {
                    let _open = open;
                    let service = service_fn(move |req: Request<Incoming>| {
                        let (fault, requests, in_flight, reply) =
                            (fault.clone(), requests.clone(), in_flight.clone(), reply.clone());
                        async move {
                            requests.fetch_add(1, Ordering::SeqCst);
                            let _busy = Held::new(&in_flight);
                            let now = *fault.borrow();
                            match now {
                                Fault::Up => {
                                    let (status, body) = (*reply)(req.uri().path());
                                    Ok(Response::builder()
                                        .status(status)
                                        .header("content-type", "application/json")
                                        .body(http_body_util::Full::new(Bytes::from(body)))
                                        .unwrap())
                                }
                                Fault::Throttle(status, secs) => Ok(Response::builder()
                                    .status(status)
                                    .header("retry-after", secs.to_string())
                                    .body(http_body_util::Full::new(Bytes::new()))
                                    .unwrap()),
                                Fault::Reset => Err(std::io::Error::other("reset")),
                                Fault::Blackhole => {
                                    let mut moved = fault.subscribe();
                                    let _ = moved.wait_for(|f| *f != Fault::Blackhole).await;
                                    Err(std::io::Error::other("blackhole lifted"))
                                }
                            }
                        }
                    });
                    let _ = hyper::server::conn::http1::Builder::new()
                        .serve_connection(TokioIo::new(stream), service)
                        .await;
                });
            }
        });
        Upstream { base, fault, requests, in_flight, connections }
    }

    fn set(&self, fault: Fault) {
        self.fault.send_replace(fault);
    }

    fn requests(&self) -> usize {
        self.requests.load(Ordering::SeqCst)
    }
}

/// A 40-character lower-case hex infohash, the same for the same seed.
fn hash(seed: &str) -> String {
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for b in seed.bytes() {
        h = (h ^ u64::from(b)).wrapping_mul(0x0100_0000_01b3);
    }
    format!("{h:016x}{:016x}{:08x}", h.rotate_left(17) ^ 0x9e37_79b9_7f4a_7c15, (h >> 7) as u32)
}

/// A Stremio indexer answering two releases of its own for every title.
fn indexer(name: &'static str) -> Reply {
    Arc::new(move |path: &str| {
        let id = path.rsplit('/').next().unwrap_or_default().trim_end_matches(".json").to_owned();
        let streams: Vec<Value> = (1..=2)
            .map(|n| {
                serde_json::json!({
                    "name": name,
                    "title": format!("Fixture {name} {n} 1080p WEB-DL\n💾 {n}.5 GB 👤 {}", 10 * n),
                    "infoHash": hash(&format!("{name}:{id}:{n}")),
                    "fileIdx": 0,
                })
            })
            .collect();
        (StatusCode::OK, serde_json::json!({ "streams": streams }).to_string())
    })
}

/// TorBox answering a cache check: it holds none of them, which is an answer for every hash.
fn torbox() -> Reply {
    Arc::new(|path: &str| {
        if path.ends_with("/torrents/checkcached") {
            (StatusCode::OK, r#"{"success":true,"data":{}}"#.to_owned())
        } else {
            (StatusCode::NOT_FOUND, r#"{"success":false}"#.to_owned())
        }
    })
}

/// Scout's production client, with TorBox's fixed origin rewritten to the fake: below the pause layer, as the real
/// network is.
struct ToFakeTorbox {
    inner: GuardedClient,
    base: String,
}

/// `Doer` is an `async_trait`; this is the method it expands to, written out so Hearth needs no macro crate of its own.
impl Doer for ToFakeTorbox {
    fn do_request<'life0, 'async_trait>(
        &'life0 self,
        mut req: reqwest::Request,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<reqwest::Response, DoError>> + Send + 'async_trait>,
    >
    where
        'life0: 'async_trait,
        Self: 'async_trait,
    {
        Box::pin(async move {
            let origin = TORBOX_API.trim_end_matches("/v1/api");
            if let Some(rest) = req.url().as_str().strip_prefix(origin) {
                *req.url_mut() = reqwest::Url::parse(&format!("{}{rest}", self.base)).unwrap();
            }
            self.inner.do_request(req).await
        })
    }
}

struct Answer {
    status: u16,
    degraded: String,
    cache_control: String,
    body: Value,
}

impl Answer {
    fn kind(&self) -> &str {
        self.body["den"]["answerKind"].as_str().unwrap_or_default()
    }

    fn streams(&self) -> usize {
        self.body["streams"].as_array().map_or(0, Vec::len)
    }
}

impl std::fmt::Debug for Answer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{} degraded={:?} kind={:?} streams={}",
            self.status,
            self.degraded,
            self.kind(),
            self.streams()
        )
    }
}

struct Node {
    base: String,
    http: reqwest::Client,
    torrentio: Upstream,
    mediafusion: Upstream,
    torbox: Upstream,
    _stop: watch::Sender<bool>,
}

impl Node {
    async fn start() -> Node {
        let torrentio = Upstream::spawn(indexer("torrentio")).await;
        let mediafusion = Upstream::spawn(indexer("mediafusion")).await;
        let torbox = Upstream::spawn(torbox()).await;
        let cinemeta = Upstream::spawn(Arc::new(|_: &str| (StatusCode::NOT_FOUND, String::new()))).await;
        let env = |key: &str| match key {
            "TORRENTIO_URL" => torrentio.base.clone(),
            "MEDIAFUSION_URL" => mediafusion.base.clone(),
            "CINEMETA_URL" => cinemeta.base.clone(),
            "SCRAPE_TIMEOUT_SECS" => SCRAPE_TIMEOUT.as_secs().to_string(),
            "LIST_TTL_SECS" => "1".into(),
            "METRICS_TOKEN" => TOKEN.into(),
            _ => String::new(),
        };
        let settings = || {
            let mut s = settings_from_env(env);
            s.cache_dir = String::new();
            s
        };
        let client = ToFakeTorbox {
            inner: GuardedClient::from_profile(&shared_profile()).unwrap(),
            base: torbox.base.clone(),
        };
        let den = den::Den::new(None, false, Default::default(), None, "none");
        let scout = crate::scout::Scout::with_client(
            settings(),
            settings(),
            &den.scout,
            Arc::new(pause_on_refusal(client)),
        );
        scout.runtime().shorten_recovery_for_test(COOLOFF, PROBE_EVERY);
        let hearth = Hearth::new(
            Some(den_reel::AppState::new(den_reel::Config::from_env())),
            Some(den_subtitles::AppState::new(den_subtitles::Config::from_env())),
            Some(den_remux::AppState::new(den_remux::Config::from_env())),
            Some(scout),
            den,
            Limits::default(),
            Options { metrics_token: Some(TOKEN.into()), ..Default::default() },
        );
        let (addr, stop) = serve(hearth).await;
        // A client that keeps no connection, so nothing it holds is mistaken for something Hearth holds.
        let http = reqwest::Client::builder()
            .pool_max_idle_per_host(0)
            .timeout(Duration::from_secs(15))
            .build()
            .unwrap();
        Node { base: format!("http://{addr}"), http, torrentio, mediafusion, torbox, _stop: stop }
    }

    fn indexers(&self, fault: Fault) {
        self.torrentio.set(fault);
        self.mediafusion.set(fault);
    }

    async fn fetch(&self, path: &str) -> (u16, reqwest::header::HeaderMap, Bytes) {
        let mut req = self.http.get(format!("{}{path}", self.base));
        if path.ends_with("/metrics") {
            req = req.header("authorization", format!("Bearer {TOKEN}"));
        }
        let resp = req.send().await.unwrap();
        let (status, headers) = (resp.status().as_u16(), resp.headers().clone());
        (status, headers, resp.bytes().await.unwrap())
    }

    /// The stream list for `imdb`, as a client asks for it.
    async fn list(&self, imdb: &str) -> Answer {
        let config = r#"{"iid":"AAECAwQFBgcICQoLDA0ODw","ep":0,"scout":{"debrid":[{"service":"torbox","token":"fixture"}]}}"#;
        let segment = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(config);
        let (status, headers, body) = self.fetch(&format!("/den/{segment}/stream/movie/{imdb}.json")).await;
        let header =
            |name: &str| headers.get(name).and_then(|v| v.to_str().ok()).unwrap_or_default().to_owned();
        Answer {
            status,
            degraded: header("x-den-degraded"),
            cache_control: header("cache-control"),
            body: serde_json::from_slice(&body).unwrap_or(Value::Null),
        }
    }

    async fn health(&self) -> Value {
        serde_json::from_slice(&self.fetch("/health").await.2).unwrap()
    }

    async fn metrics(&self, path: &str) -> String {
        String::from_utf8(self.fetch(path).await.2.to_vec()).unwrap()
    }

    async fn degraded_builds(&self) -> i64 {
        metric(&self.metrics("/scout/metrics").await, r#"scout_list_builds_total{result="degraded"}"#)
    }

    /// Every admission slot, detached build and upstream request back to rest, and the fakes' sockets bounded.
    async fn assert_idle(&self) {
        let upstreams = [&self.torrentio, &self.mediafusion, &self.torbox];
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            let scout = self.metrics("/scout/metrics").await;
            let hearth = self.metrics("/metrics").await;
            let gauges = [
                metric(&scout, "scout_list_builds_active"),
                metric(&scout, "scout_scraper_requests_active"),
                metric(&scout, "scout_late_diagnostics_active"),
                metric(&hearth, r#"den_hearth_request_admission_active{lane="bulk"}"#),
            ];
            let in_flight: Vec<usize> =
                upstreams.iter().map(|u| u.in_flight.load(Ordering::SeqCst)).collect();
            let open: Vec<usize> = upstreams.iter().map(|u| u.connections.load(Ordering::SeqCst)).collect();
            // Idle keep-alive connections stay pooled; a leak of the ones held through the outage would not.
            if gauges.iter().all(|g| *g == 0)
                && in_flight.iter().all(|n| *n == 0)
                && open.iter().all(|n| *n <= 4)
            {
                return;
            }
            assert!(
                Instant::now() < deadline,
                "Scout did not return to idle: gauges={gauges:?} in flight={in_flight:?} open={open:?}"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }
}

/// The value of one Prometheus series in `text`.
fn metric(text: &str, series: &str) -> i64 {
    text.lines()
        .find_map(|line| line.strip_prefix(series)?.strip_prefix(' ')?.trim().parse().ok())
        .unwrap_or_else(|| panic!("no {series} in\n{text}"))
}

fn unix_secs(t: SystemTime) -> u64 {
    t.duration_since(UNIX_EPOCH).unwrap().as_secs()
}

/// Waits for the wall clock to reach Unix second `secs`: list freshness is kept in whole Unix seconds.
async fn until_unix(secs: u64) {
    let at = UNIX_EPOCH + Duration::from_secs(secs);
    if let Ok(left) = at.duration_since(SystemTime::now()) {
        tokio::time::sleep(left + Duration::from_millis(20)).await;
    }
}

fn scout_health(health: &Value) -> &Value {
    &health["services"]["scout"]
}

/// The household outage: a warm complete list is held, marked, through a total indexer outage; a title nobody had
/// listed is a typed unknown and never cached; /health degrades on its schedule; and once the upstreams answer
/// again the very next requests rebuild, without a restart.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_total_indexer_outage_is_held_then_recovers_without_a_restart() {
    let node = Node::start().await;

    let warm = node.list("tt0000101").await;
    let built = SystemTime::now();
    assert_eq!(
        (warm.status, warm.degraded.as_str(), warm.kind(), warm.streams()),
        (200, "", "live", 4),
        "{warm:?}"
    );

    node.indexers(Fault::Blackhole);
    let (torrentio, mediafusion, torbox) =
        (node.torrentio.requests(), node.mediafusion.requests(), node.torbox.requests());

    // A cold title: no held list, so the answer is unknown, typed and uncacheable — never an authoritative empty —
    // and the next request asks again. /health turns at the third such build in a row, not before.
    for build in 1..=3 {
        let cold = node.list("tt0000102").await;
        assert_eq!(cold.status, 200);
        assert_eq!(
            (cold.degraded.as_str(), cold.kind(), cold.streams()),
            ("indexers", "unknown", 0),
            "{cold:?}"
        );
        assert_eq!(cold.cache_control, "no-store", "a degraded empty list must not be kept by the client");
        assert_eq!(node.torrentio.requests(), torrentio + build, "one request per build, no retry loop");
        assert_eq!(node.mediafusion.requests(), mediafusion + build, "one request per build, no retry loop");
        let health = node.health().await;
        let want = if build < 3 { "ok" } else { "degraded" };
        assert_eq!(scout_health(&health)["status"], want, "after {build} failed builds: {health}");
    }
    assert_eq!(scout_health(&node.health().await)["reason"], "indexers");
    assert_eq!(node.torbox.requests(), torbox, "nothing to cache-check, so TorBox was not asked");

    // The warm list past its stale window: served at once from what is held, marked stale_list, while one rebuild
    // runs behind it and fails.
    until_unix(unix_secs(built) + 2).await;
    let degraded_before = node.degraded_builds().await;
    let started = Instant::now();
    let held = node.list("tt0000101").await;
    assert!(
        started.elapsed() < SCRAPE_TIMEOUT,
        "the held list waited on the outage: {:?}",
        started.elapsed()
    );
    assert_eq!((held.status, held.degraded.as_str(), held.kind()), (200, "stale_list", "stale"), "{held:?}");
    assert_eq!(held.body["den"]["degraded"], "stale_list");
    assert_eq!(held.body["streams"], warm.body["streams"], "the held list is the warm one");
    assert_eq!(
        held.cache_control, "private, max-age=1",
        "a stale list is kept briefly, with no stale-if-error"
    );
    let deadline = Instant::now() + Duration::from_secs(5);
    while node.degraded_builds().await == degraded_before {
        assert!(Instant::now() < deadline, "the rebuild behind the held list never finished");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let (torrentio, mediafusion) = (node.torrentio.requests(), node.mediafusion.requests());
    // Inside the cool-off every request is answered from the held list without asking anybody.
    for _ in 0..5 {
        assert_eq!(node.list("tt0000101").await.degraded, "stale_list");
    }
    assert_eq!(
        (node.torrentio.requests(), node.mediafusion.requests()),
        (torrentio, mediafusion),
        "a request inside the cool-off reached an indexer"
    );

    // Connectivity returns. No restart: the requests that follow converge on the healthy list.
    node.indexers(Fault::Up);
    let restored = Instant::now();
    let mut answer = node.list("tt0000101").await;
    while !answer.degraded.is_empty()
        && restored.elapsed() < COOLOFF + SCRAPE_TIMEOUT + Duration::from_secs(2)
    {
        assert_eq!(answer.degraded, "stale_list", "{answer:?}");
        tokio::time::sleep(Duration::from_millis(50)).await;
        answer = node.list("tt0000101").await;
    }
    let latency = restored.elapsed();
    assert_eq!(answer.degraded, "", "still degraded {latency:?} after the indexers came back: {answer:?}");
    assert!(matches!(answer.kind(), "live" | "stale") && answer.streams() == 4, "{answer:?}");
    assert!(
        node.torrentio.requests() - torrentio <= 2 && node.mediafusion.requests() - mediafusion <= 2,
        "recovery asked the indexers more than one rebuild's worth"
    );
    eprintln!("warm list recovered {latency:?} after the indexers came back (cool-off {COOLOFF:?})");

    let started = Instant::now();
    let cold = node.list("tt0000102").await;
    assert_eq!((cold.degraded.as_str(), cold.kind(), cold.streams()), ("", "live", 4), "{cold:?}");
    eprintln!("cold title answered in {:?} after recovery", started.elapsed());
    let health = node.health().await;
    assert_eq!(scout_health(&health)["status"], "ok", "{health}");
    assert!(scout_health(&health).get("reason").is_none(), "{health}");

    node.assert_idle().await;
}

/// TorBox and the indexers fail and come back separately. While TorBox is down a list is served at once, its cache
/// state unknown, with one recovery probe per window and no request in between; the indexers coming back first gives
/// releases while TorBox is still down; TorBox coming back is noticed by the first probe after it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn torbox_and_the_indexers_recover_independently() {
    let node = Node::start().await;
    node.torbox.set(Fault::Reset);

    for build in 1..=3 {
        let list = node.list("tt0000201").await;
        assert_eq!(
            (list.status, list.degraded.as_str(), list.streams()),
            (200, "cache-check", 4),
            "{list:?}"
        );
        assert_eq!(list.cache_control, "no-store");
        assert_eq!(list.body["den"]["degraded"], "cache-check");
        assert_eq!(node.torbox.requests(), build, "one cache check per build");
    }
    let health = node.health().await;
    assert_eq!(scout_health(&health)["status"], "degraded", "{health}");
    assert_eq!(scout_health(&health)["reason"], "debrid", "{health}");
    assert_eq!(health["debrid"]["state"], "down", "{health}");

    // Down: answered without asking TorBox, at once.
    for _ in 0..2 {
        let started = Instant::now();
        let list = node.list("tt0000201").await;
        assert_eq!((list.degraded.as_str(), list.streams()), ("cache-check", 4), "{list:?}");
        assert!(started.elapsed() < Duration::from_millis(500), "a list waited on a TorBox known down");
    }
    assert_eq!(node.torbox.requests(), 3, "TorBox was asked while known down");

    // Both out: unknown, as before.
    node.indexers(Fault::Blackhole);
    let both = node.list("tt0000202").await;
    assert_eq!((both.degraded.as_str(), both.kind(), both.streams()), ("indexers", "unknown", 0), "{both:?}");

    // The indexers come back first. Releases return at once; the probe window has passed, so this build is TorBox's
    // one probe, which fails, and the next is answered without asking.
    node.indexers(Fault::Up);
    let back = node.list("tt0000203").await;
    assert_eq!((back.degraded.as_str(), back.streams()), ("cache-check", 4), "{back:?}");
    assert_eq!(node.torbox.requests(), 4, "one probe per window");
    assert_eq!(node.list("tt0000203").await.degraded, "cache-check");
    assert_eq!(node.torbox.requests(), 4, "a second probe inside the window");
    assert_eq!(
        scout_health(&node.health().await)["reason"],
        "debrid",
        "the indexers' recovery is seen alone"
    );

    node.torbox.set(Fault::Up);
    let restored = Instant::now();
    let mut answer = node.list("tt0000203").await;
    while !answer.degraded.is_empty() && restored.elapsed() < PROBE_EVERY + Duration::from_secs(3) {
        tokio::time::sleep(Duration::from_millis(50)).await;
        answer = node.list("tt0000203").await;
    }
    let latency = restored.elapsed();
    assert_eq!((answer.degraded.as_str(), answer.kind(), answer.streams()), ("", "live", 4), "{answer:?}");
    assert_eq!(node.torbox.requests(), 5, "recovery took more than the one probe");
    eprintln!("TorBox recovered {latency:?} after it came back (probe window {PROBE_EVERY:?})");
    let health = node.health().await;
    assert_eq!(scout_health(&health)["status"], "ok", "{health}");
    assert_eq!(health["debrid"]["state"], "ok", "{health}");

    node.assert_idle().await;
}

/// One indexer out: lists are partial, served and kept briefly, and /health says coverage is partial. The missing
/// releases are back as soon as the short-lived partial list expires.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_partial_answer_is_kept_briefly_and_completes_when_the_source_returns() {
    let node = Node::start().await;
    node.mediafusion.set(Fault::Blackhole);

    for (n, imdb) in ["tt0000301", "tt0000302", "tt0000303"].into_iter().enumerate() {
        let partial = node.list(imdb).await;
        assert_eq!(
            (partial.degraded.as_str(), partial.kind(), partial.streams()),
            ("", "partial", 2),
            "{partial:?}"
        );
        assert_eq!(partial.cache_control, "private, max-age=1, stale-while-revalidate=1");
        assert_eq!(node.mediafusion.requests(), n + 1, "one request per build, no retry loop");
    }
    let health = node.health().await;
    assert_eq!(scout_health(&health)["status"], "ok", "a missing source is not an outage: {health}");
    assert_eq!(scout_health(&health)["coverage"], "partial", "{health}");

    node.mediafusion.set(Fault::Up);
    let restored = Instant::now();
    let mut answer = node.list("tt0000303").await;
    while answer.kind() != "live" && restored.elapsed() < Duration::from_secs(4) {
        assert_eq!(answer.kind(), "partial", "{answer:?}");
        tokio::time::sleep(Duration::from_millis(50)).await;
        answer = node.list("tt0000303").await;
    }
    let latency = restored.elapsed();
    assert_eq!((answer.degraded.as_str(), answer.kind(), answer.streams()), ("", "live", 4), "{answer:?}");
    eprintln!("partial list completed {latency:?} after the source came back (partial TTL 1s here)");
    assert_eq!(scout_health(&node.health().await)["coverage"], "complete");

    node.assert_idle().await;
}

/// An upstream that names a `Retry-After` is not asked again until it has passed, however many requests arrive, and
/// is asked again once it has. The indexer's refusal leaves lists partial; TorBox's leaves them unchecked, without
/// calling TorBox down, since it answered.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn retry_after_is_honoured_and_then_the_upstream_is_asked_again() {
    let node = Node::start().await;
    let wait = 2;

    // The indexer: one refusal, then nothing until the stated wait has passed.
    node.torrentio.set(Fault::Throttle(503, wait));
    let refused_at = Instant::now();
    let first = node.list("tt0000401").await;
    assert_eq!((first.degraded.as_str(), first.kind(), first.streams()), ("", "partial", 2), "{first:?}");
    assert_eq!(
        node.torrentio.requests(),
        1,
        "the retry of a shed request was sent although the host paused it"
    );
    node.torrentio.set(Fault::Up);
    let mut n = 0;
    let answered = loop {
        n += 1;
        let list = node.list(&format!("tt00005{n:02}")).await;
        if node.torrentio.requests() > 1 {
            break (list, refused_at.elapsed());
        }
        assert_eq!(list.kind(), "partial", "{list:?}");
        assert!(refused_at.elapsed() < Duration::from_secs(wait + 2), "the indexer was never asked again");
        tokio::time::sleep(Duration::from_millis(100)).await;
    };
    assert!(
        answered.1 >= Duration::from_secs(wait),
        "asked again {:?} after a {wait}s Retry-After",
        answered.1
    );
    assert_eq!((answered.0.kind(), answered.0.streams()), ("live", 4), "{:?}", answered.0);
    assert_eq!(node.torrentio.requests(), 2, "{n} lists asked the paused indexer");
    eprintln!("indexer asked again {:?} after a {wait}s Retry-After", answered.1);

    // TorBox: the same, and a throttled TorBox is not a TorBox that is down.
    node.torbox.set(Fault::Throttle(429, wait));
    let before = node.torbox.requests();
    let refused_at = Instant::now();
    let first = node.list("tt0000601").await;
    assert_eq!((first.degraded.as_str(), first.streams()), ("cache-check", 4), "{first:?}");
    assert_eq!(node.torbox.requests(), before + 1);
    assert_ne!(node.health().await["debrid"]["state"], "down", "a throttle is an answer");
    node.torbox.set(Fault::Up);
    let mut n = 0;
    let answered = loop {
        n += 1;
        let list = node.list(&format!("tt00007{n:02}")).await;
        if node.torbox.requests() > before + 1 {
            break (list, refused_at.elapsed());
        }
        assert_eq!(list.degraded, "cache-check", "{list:?}");
        assert!(refused_at.elapsed() < Duration::from_secs(wait + 2), "TorBox was never asked again");
        tokio::time::sleep(Duration::from_millis(100)).await;
    };
    assert!(
        answered.1 >= Duration::from_secs(wait),
        "asked again {:?} after a {wait}s Retry-After",
        answered.1
    );
    assert_eq!((answered.0.degraded.as_str(), answered.0.streams()), ("", 4), "{:?}", answered.0);
    assert_eq!(node.torbox.requests(), before + 2, "{n} lists asked the paused TorBox");
    eprintln!("TorBox asked again {:?} after a {wait}s Retry-After", answered.1);

    node.assert_idle().await;
}
