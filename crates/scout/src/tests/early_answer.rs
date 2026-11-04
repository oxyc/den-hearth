//! A stream list answers once its fast indexers have had `LATE_GRACE` to be joined, not on the slowest one.
//!
//! On a paused clock: each fake indexer answers after a fixed latency in virtual time, so the time a scrape takes is
//! exact and the run is instant. `cargo test -p den-scout early_answer -- --nocapture` prints the before/after
//! table the profiles below produce.

use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use tokio::time::Instant;

use crate::cache::{Cache, MemoryCache};
use crate::config::DebridService;
use crate::config::Indexer;
use crate::coverage::{OUTCOME_ANSWERED, OUTCOME_LATE};
use crate::handler::new_handler;
use crate::loglimit::Capture;
use crate::rank::{RawStream, GIB};
use crate::scrape::{scrape_covered_until, DoError, ScrapeQuery, Scraper, LATE_GRACE};
use crate::stores::{ProviderStatus, ResolveTarget, Store, StoreError};
use crate::tests::handler::{fake_scraper, get, test_deps, valid_blob};

fn release(hash: char, title: &str) -> RawStream {
    RawStream {
        info_hash: hash.to_string().repeat(40),
        title: title.to_owned(),
        size_bytes: Some(8 * GIB),
        seeders: Some(50),
        file_idx: Some(0),
        ..RawStream::default()
    }
}

/// An indexer answering `streams` after `latency`.
fn indexer(id: &'static str, latency: Duration, streams: Vec<RawStream>) -> Arc<dyn Scraper> {
    Arc::from(fake_scraper(id, move || {
        let streams = streams.clone();
        Box::pin(async move {
            tokio::time::sleep(latency).await;
            Ok(streams)
        })
    }))
}

fn query() -> ScrapeQuery {
    ScrapeQuery { kind: "movie".into(), imdb: "tt1234567".into(), ..Default::default() }
}

const TIMEOUT: Duration = Duration::from_secs(8);

/// How long a scrape over indexers answering at these latencies takes, waiting for all of them or answering early.
async fn scrape_time(latencies: &[u64], grace: Option<Duration>) -> Duration {
    const IDS: [&str; 3] = ["torrentio", "mediafusion", "comet"];
    let scrapers: Vec<Arc<dyn Scraper>> = latencies
        .iter()
        .zip(IDS)
        .enumerate()
        .map(|(i, (ms, id))| {
            let hash = char::from(b'a' + i as u8);
            indexer(id, Duration::from_millis(*ms), vec![release(hash, "Movie 1080p WEB-DL")])
        })
        .collect();
    let start = Instant::now();
    let _ = scrape_covered_until(&scrapers, &query(), TIMEOUT, start + TIMEOUT, None, Duration::ZERO, grace)
        .await;
    start.elapsed()
}

/// Latency profiles for torrentio and mediafusion, in ms. The first is a list built in step; the rest are the
/// shapes of the box's slow builds (p50 0.9 s, p95 3.5 s, max 7.3 s for series; movies p95 4.1 s), a slow second
/// indexer, and one that never answers inside the scrape timeout.
const PROFILES: [[u64; 2]; 5] = [[400, 700], [500, 2900], [800, 4100], [700, 7300], [600, 9000]];

#[tokio::test(start_paused = true)]
async fn answers_after_the_grace_not_the_slowest() {
    eprintln!("early_answer: profile (ms) | waits for all | answers early");
    for [fast, slow] in PROFILES {
        let before = scrape_time(&[fast, slow], None).await;
        let after = scrape_time(&[fast, slow], Some(LATE_GRACE)).await;
        eprintln!("early_answer: {fast}/{slow} | {} ms | {} ms", before.as_millis(), after.as_millis());
        let all = Duration::from_millis(slow).min(TIMEOUT);
        assert_eq!(before, all, "{fast}/{slow}: without a grace the slowest decides");
        assert_eq!(after, all.min(Duration::from_millis(fast) + LATE_GRACE), "{fast}/{slow}");
    }
}

/// An indexer that answers nothing does not start the clock: the list still waits for one that has releases.
#[tokio::test(start_paused = true)]
async fn an_empty_answer_does_not_start_the_grace() {
    let scrapers = vec![
        indexer("torrentio", Duration::from_millis(100), Vec::new()),
        indexer("mediafusion", Duration::from_millis(3000), vec![release('a', "Movie 1080p WEB-DL")]),
    ];
    let start = Instant::now();
    let res = scrape_covered_until(
        &scrapers,
        &query(),
        TIMEOUT,
        start + TIMEOUT,
        None,
        Duration::ZERO,
        Some(LATE_GRACE),
    )
    .await;
    assert_eq!(start.elapsed(), Duration::from_millis(3000));
    assert!(res.complete && res.late.is_none());
    assert_eq!(res.seeds.len(), 1);
}

/// An indexer whose answers are kept (it has an answer key, as the real ones do), counting what it is asked.
struct Kept {
    id: &'static str,
    latency: Duration,
    streams: Vec<RawStream>,
    asked: AtomicUsize,
}

#[async_trait]
impl Scraper for Kept {
    fn id(&self) -> Indexer {
        Indexer(self.id.to_owned())
    }

    async fn scrape(&self, _q: &ScrapeQuery) -> Result<Vec<RawStream>, DoError> {
        self.asked.fetch_add(1, Ordering::SeqCst);
        tokio::time::sleep(self.latency).await;
        Ok(self.streams.clone())
    }

    fn answer_key(&self) -> Option<String> {
        Some(format!("kept-{}", self.id))
    }
}

/// What the list went out without is reported as timed out, the list as incomplete, and the late answer still
/// lands: on the channel, and in the indexer's kept answer, so the next build has it at once and is complete.
#[tokio::test(start_paused = true)]
async fn the_late_answer_is_kept_for_the_next_build() {
    let cache: Arc<dyn Cache> = Arc::new(MemoryCache::new(1 << 20));
    let kept = |id, ms, streams| {
        Arc::new(Kept { id, latency: Duration::from_millis(ms), streams, asked: AtomicUsize::new(0) })
    };
    let fast = kept("torrentio", 400, vec![release('a', "Movie 1080p WEB-DL")]);
    let slow = kept("mediafusion", 4000, vec![release('b', "Movie 2160p WEB-DL")]);
    let scrapers: Vec<Arc<dyn Scraper>> = vec![fast.clone(), slow.clone()];
    let ttl = Duration::from_secs(300);
    let start = Instant::now();
    let res = scrape_covered_until(
        &scrapers,
        &query(),
        TIMEOUT,
        start + TIMEOUT,
        Some(cache.clone()),
        ttl,
        Some(LATE_GRACE),
    )
    .await;
    assert_eq!(start.elapsed(), Duration::from_millis(1400));
    let outcomes: Vec<(&str, &str)> =
        res.sources.iter().map(|s| (s.id.as_str(), s.outcome.as_str())).collect();
    assert_eq!(outcomes, [("torrentio", OUTCOME_ANSWERED), ("mediafusion", OUTCOME_LATE)]);
    assert!(!res.complete && res.any_ok);
    let late = res.late.expect("a source was left behind");
    assert_eq!(late.ids, ["mediafusion"]);
    let answers = late.answers.await.expect("the late source finishes");
    assert_eq!(answers.len(), 1);
    assert_eq!(answers[0].streams.as_ref().map(Vec::len), Some(1));

    let again = Instant::now();
    let res = scrape_covered_until(
        &scrapers,
        &query(),
        TIMEOUT,
        again + TIMEOUT,
        Some(cache),
        ttl,
        Some(LATE_GRACE),
    )
    .await;
    assert_eq!(again.elapsed(), Duration::ZERO, "both answers were kept");
    assert!(res.complete && res.late.is_none());
    assert_eq!(res.seeds.len(), 2);
    assert_eq!((fast.asked.load(Ordering::SeqCst), slow.asked.load(Ordering::SeqCst)), (1, 1));
}

/// A stream route whose torrentio answers in 400 ms and mediafusion in 4 s.
fn slow_second_indexer() -> crate::handler::Handler {
    new_handler(test_deps(|d| {
        d.scrape_timeout = TIMEOUT;
        d.make_scrapers = Arc::new(|_| {
            vec![
                fake_scraper("torrentio", || {
                    Box::pin(async {
                        tokio::time::sleep(Duration::from_millis(400)).await;
                        Ok(vec![release('b', "Movie 1080p WEB-DL")])
                    })
                }),
                fake_scraper("mediafusion", || {
                    Box::pin(async {
                        tokio::time::sleep(Duration::from_millis(4000)).await;
                        Ok(vec![release('a', "Movie 2160p WEB-DL HDR")])
                    })
                }),
            ]
        });
    }))
}

/// A managed-playback handler whose torrentio answers in `fast` ms with a cached 1080p and mediafusion in `slow` ms
/// with a cached 4K copy.
fn managed_play(fast: u64, slow: u64) -> crate::handler::Handler {
    let resolver = Arc::new(crate::tests::handler::FixedManagedHls::default());
    new_handler(test_deps(move |d| {
        d.scrape_timeout = TIMEOUT;
        d.managed_hls = Some(resolver);
        d.make_scrapers = Arc::new(move |_| {
            vec![
                fake_scraper("torrentio", move || {
                    Box::pin(async move {
                        tokio::time::sleep(Duration::from_millis(fast)).await;
                        Ok(vec![release('b', "Movie 1080p WEB-DL")])
                    })
                }),
                fake_scraper("mediafusion", move || {
                    Box::pin(async move {
                        tokio::time::sleep(Duration::from_millis(slow)).await;
                        Ok(vec![release('a', "Movie 2160p WEB-DL HDR")])
                    })
                }),
            ]
        });
    }))
}

/// Play through Hearth's managed path, until the release it plays is chosen: the list is the wait, and a viewer's
/// Play (`early_answer`) is answered a grace after the first indexer. A named release still waits for every one.
#[tokio::test(start_paused = true)]
async fn a_viewers_play_is_answered_a_grace_after_the_first_indexer() {
    use crate::managed_hls::ManagedHlsRequest;
    eprintln!("early_answer play: profile (ms) | waits for all | answers early");
    for [fast, slow] in PROFILES {
        let mut took = Vec::new();
        for early_answer in [false, true] {
            let h = managed_play(fast, slow);
            let request = ManagedHlsRequest { imdb: "tt1234567".into(), early_answer, ..Default::default() };
            let start = Instant::now();
            let played = h.managed_hls(&valid_blob(), &request).await.expect("a release plays");
            took.push(start.elapsed());
            // Early, the 4K copy the slow indexer had is not in the list; waiting, it is, and it plays — unless the
            // indexer never answers inside the scrape timeout.
            let waited_for =
                if early_answer { fast + LATE_GRACE.as_millis() as u64 } else { TIMEOUT.as_millis() as u64 };
            let want = if slow <= waited_for { "Movie 2160p WEB-DL HDR" } else { "Movie 1080p WEB-DL" };
            assert_eq!(played.release.filename, want, "{fast}/{slow} early={early_answer}");
        }
        eprintln!(
            "early_answer play: {fast}/{slow} | {} ms | {} ms",
            took[0].as_millis(),
            took[1].as_millis()
        );
        let all = Duration::from_millis(slow).min(TIMEOUT);
        assert_eq!(took[0], all, "{fast}/{slow}");
        assert_eq!(took[1], all.min(Duration::from_millis(fast) + LATE_GRACE), "{fast}/{slow}");
    }
    let h = managed_play(400, 4000);
    let named = ManagedHlsRequest {
        imdb: "tt1234567".into(),
        early_answer: true,
        filename: Some("Movie 2160p WEB-DL HDR".into()),
        ..Default::default()
    };
    let start = Instant::now();
    assert_eq!(
        h.managed_hls(&valid_blob(), &named).await.unwrap().release.filename,
        "Movie 2160p WEB-DL HDR"
    );
    assert_eq!(start.elapsed(), Duration::from_millis(4000), "a named release waits for every indexer");
}

fn list_path() -> String {
    format!("/{}/stream/movie/tt1234567.json", valid_blob())
}

const EARLY: (&str, &str) = ("X-Den-Early-Answer", "1");

/// Without the header a list waits for every indexer: a request that judges a list complete (a download's search
/// or fallback, an availability check), and every client that has never heard of the header, is unchanged.
#[tokio::test(start_paused = true)]
async fn without_the_header_the_route_waits_for_every_indexer() {
    let h = slow_second_indexer();
    let start = Instant::now();
    let resp = get(&h, &list_path(), &[]).await;
    assert_eq!(start.elapsed(), Duration::from_millis(4000));
    let body = String::from_utf8_lossy(resp.body()).into_owned();
    assert!(body.contains("Movie 1080p WEB-DL") && body.contains("Movie 2160p"), "{body}");
    assert!(body.contains(r#""answerKind":"live""#) && !body.contains(r#""outcome":"late""#), "{body}");
}

/// An early answer is its requester's alone: not stored by any cache, and not the list a plain request that
/// follows is served — that one waits for every indexer, and gets the 4K copy the early list went without.
#[tokio::test(start_paused = true)]
async fn an_early_answer_is_never_served_to_a_request_that_waits() {
    let h = slow_second_indexer();
    let early = get(&h, &list_path(), &[EARLY]).await;
    assert_eq!(early.headers().get("cache-control").and_then(|v| v.to_str().ok()), Some("no-store"));
    let body = String::from_utf8_lossy(early.body()).into_owned();
    assert!(body.contains(r#""outcome":"late""#) && !body.contains("expiresAt"), "{body}");

    let start = Instant::now();
    let plain = get(&h, &list_path(), &[]).await;
    assert_eq!(start.elapsed(), Duration::from_millis(4000), "built again, waiting for every indexer");
    let body = String::from_utf8_lossy(plain.body()).into_owned();
    assert!(body.contains("Movie 2160p") && !body.contains(r#""outcome":"late""#), "{body}");
}

/// Through the stream route with the header: the list goes out after the grace, without the slow indexer's
/// release, which is reported `late` (not `timeout`, which clients read as a source that failed). Once that lands
/// the log says whether it would have led the list. Here it would have (a cached 4K copy), and says so.
#[tokio::test(start_paused = true)]
async fn the_route_answers_early_and_counts_what_the_late_indexer_would_have_changed() {
    let h = slow_second_indexer();
    let log = Capture::start();
    let start = Instant::now();
    let resp = get(&h, &list_path(), &[EARLY]).await;
    assert_eq!(resp.status(), 200);
    assert_eq!(start.elapsed(), Duration::from_millis(1400), "answered a grace after the first indexer");
    let body = String::from_utf8_lossy(resp.body()).into_owned();
    assert!(body.contains("Movie 1080p WEB-DL") && !body.contains("Movie 2160p"), "{body}");
    assert!(body.contains(r#""id":"mediafusion","outcome":"late""#), "{body}");
    assert!(body.contains(r#""answerKind":"partial""#), "{body}");

    let mut line = None;
    for _ in 0..200 {
        line = log.text().lines().find(|l| l.starts_with("event=scrape outcome=late")).map(str::to_owned);
        if line.is_some() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let line = line.unwrap_or_else(|| panic!("no late line: {}", log.text()));
    assert!(line.contains("upstream=mediafusion"), "{line}");
    assert!(line.contains("new=1 top_changed=true"), "{line}");
}

/// A real stream-route load source: one fast foreground release, then `count` late releases. The useful 4K
/// releases deliberately come last, after thousands of CAMs, so a first-N truncation cannot pass this test.
struct DiagnosticLoadScraper {
    id: &'static str,
    latency: Duration,
    late_count: usize,
}

#[async_trait]
impl Scraper for DiagnosticLoadScraper {
    fn id(&self) -> Indexer {
        Indexer(self.id.to_owned())
    }

    async fn scrape(&self, q: &ScrapeQuery) -> Result<Vec<RawStream>, DoError> {
        tokio::time::sleep(self.latency).await;
        if self.late_count == 0 {
            return Ok(vec![RawStream {
                info_hash: diagnostic_hash(q, 0),
                title: "Movie 1080p WEB-DL".to_owned(),
                size_bytes: Some(8 * GIB),
                seeders: Some(50),
                file_idx: Some(0),
                ..RawStream::default()
            }]);
        }
        Ok((1..=self.late_count)
            .map(|i| {
                let useful = i > self.late_count.saturating_sub(crate::handler::MAX_SEEDS);
                RawStream {
                    info_hash: diagnostic_hash(q, i),
                    title: if useful {
                        "Movie 2160p REMUX HDR".to_owned()
                    } else {
                        "Movie 2160p HDCAM".to_owned()
                    },
                    size_bytes: Some(if useful { 18 } else { 2 } * GIB),
                    seeders: Some(i as i64),
                    file_idx: Some(0),
                    ..RawStream::default()
                }
            })
            .collect())
    }
}

fn diagnostic_hash(q: &ScrapeQuery, i: usize) -> String {
    let title = q.imdb.trim_start_matches("tt").parse::<u64>().expect("numeric test imdb");
    format!("{:040x}", title * 10_000 + i as u64)
}

struct DiagnosticChecks {
    foreground_requests: AtomicUsize,
    late_requests: AtomicUsize,
    late_hashes: Mutex<Vec<Vec<String>>>,
    release: tokio::sync::Semaphore,
    block_late: bool,
}

impl Default for DiagnosticChecks {
    fn default() -> Self {
        Self {
            foreground_requests: AtomicUsize::new(0),
            late_requests: AtomicUsize::new(0),
            late_hashes: Mutex::new(Vec::new()),
            release: tokio::sync::Semaphore::new(0),
            block_late: false,
        }
    }
}

struct DiagnosticStore(Arc<DiagnosticChecks>);

#[async_trait]
impl Store for DiagnosticStore {
    fn service(&self) -> DebridService {
        DebridService::TorBox
    }

    async fn cache_check(&self, hashes: &[String]) -> Result<HashMap<String, bool>, StoreError> {
        if hashes.len() == 1 {
            self.0.foreground_requests.fetch_add(1, Ordering::SeqCst);
        } else {
            self.0.late_requests.fetch_add(1, Ordering::SeqCst);
            self.0.late_hashes.lock().expect("late hashes").push(hashes.to_vec());
            if self.0.block_late {
                self.0.release.acquire().await.expect("release semaphore").forget();
            }
        }
        Ok(hashes.iter().map(|hash| (hash.clone(), true)).collect())
    }

    async fn resolve(&self, _target: &ResolveTarget) -> Result<String, StoreError> {
        Err(StoreError::Other("diagnostic store does not resolve".to_owned()))
    }

    async fn provider_status(&self, _target: &ResolveTarget) -> ProviderStatus {
        ProviderStatus::NotFetching
    }
}

fn diagnostic_handler(
    late_count: usize,
    task_limit: usize,
    checks: Arc<DiagnosticChecks>,
) -> (crate::handler::Handler, Arc<crate::runtime::ScoutRuntime>) {
    let runtime = crate::runtime::ScoutRuntime::new_with_late_diagnostic_limit(task_limit);
    let inspect_runtime = runtime.clone();
    let h = new_handler(test_deps(move |d| {
        d.runtime = runtime;
        d.scrape_timeout = TIMEOUT;
        d.make_scrapers = Arc::new(move |_| {
            vec![
                Box::new(DiagnosticLoadScraper {
                    id: "torrentio",
                    latency: Duration::from_millis(10),
                    late_count: 0,
                }),
                Box::new(DiagnosticLoadScraper {
                    id: "mediafusion",
                    latency: Duration::from_secs(2),
                    late_count,
                }),
            ]
        });
        d.make_stores = Arc::new(move |_| vec![Box::new(DiagnosticStore(checks.clone()))]);
    }));
    (h, inspect_runtime)
}

async fn wait_for_count(counter: &AtomicUsize, want: usize) {
    for _ in 0..1000 {
        if counter.load(Ordering::SeqCst) == want {
            return;
        }
        tokio::task::yield_now().await;
    }
    panic!("counter = {}, want {want}", counter.load(Ordering::SeqCst));
}

async fn run_late_diagnostic_profile(late_count: usize) {
    let checks = Arc::new(DiagnosticChecks::default());
    let (h, runtime) = diagnostic_handler(late_count, 1, checks.clone());
    let log = Capture::start();
    let path = format!("/{}/stream/movie/tt9000001.json", valid_blob());
    let start = Instant::now();
    let response = get(&h, &path, &[EARLY]).await;
    assert_eq!(response.status(), 200);
    assert_eq!(start.elapsed(), Duration::from_millis(1010), "late diagnostics delayed the viewer");

    tokio::time::sleep(Duration::from_secs(1)).await;
    wait_for_count(&checks.late_requests, 1).await;
    for _ in 0..1000 {
        if runtime.metrics().late_diagnostic_active.load(Ordering::SeqCst) == 0 {
            break;
        }
        tokio::task::yield_now().await;
    }
    assert_eq!(runtime.metrics().late_diagnostic_active.load(Ordering::SeqCst), 0);
    assert_eq!(checks.foreground_requests.load(Ordering::SeqCst), 1);
    assert_eq!(checks.late_requests.load(Ordering::SeqCst), 1);
    let asked = checks.late_hashes.lock().expect("late hashes");
    assert_eq!(asked.len(), 1);
    assert_eq!(asked[0].len(), crate::handler::MAX_SEEDS);
    let first_useful = late_count.saturating_sub(crate::handler::MAX_SEEDS) + 1;
    let expected_query = ScrapeQuery { imdb: "tt9000001".to_owned(), ..query() };
    assert!(
        asked[0].iter().all(|hash| {
            (first_useful..=late_count).any(|i| *hash == diagnostic_hash(&expected_query, i))
        }),
        "cache check did not retain the foreground cap's best-scored tail"
    );

    let dropped = late_count + 1 - crate::handler::MAX_SEEDS;
    let metrics = runtime.metrics();
    assert_eq!(metrics.late_diagnostic_retained.load(Ordering::SeqCst), crate::handler::MAX_SEEDS as i64);
    assert_eq!(metrics.late_diagnostic_dropped.load(Ordering::SeqCst), dropped as i64);
    assert_eq!(
        metrics.late_diagnostic_cache_checked.load(Ordering::SeqCst),
        crate::handler::MAX_SEEDS as i64
    );
    let line = log
        .text()
        .lines()
        .find(|line| line.starts_with("event=scrape outcome=late"))
        .map(str::to_owned)
        .unwrap_or_else(|| panic!("no late line: {}", log.text()));
    assert!(line.contains(&format!("new={late_count}")), "{line}");
    assert!(line.contains("top_changed=true retained=500 late_retained=500"), "{line}");
    assert!(line.contains(&format!("truncated={dropped} cache_checked=500")), "{line}");
    assert!(line.contains("top_changed=true"), "{line}");
}

/// The matched low workload for the #207 resource profile. It traverses the production stream route, source
/// cutoff, late cache check and ranking; the test binary is also run directly under `/usr/bin/time -l`.
#[tokio::test(start_paused = true)]
async fn late_diagnostic_profile_500() {
    run_late_diagnostic_profile(500).await;
}

/// Five thousand late releases still make one cache-check request for the same best 500 candidates, and retain
/// no more candidates than the 500-release workload.
#[tokio::test(start_paused = true)]
async fn late_diagnostic_profile_5000() {
    run_late_diagnostic_profile(5000).await;
}

/// Distinct early answers do not share a list flight. This exercises the real task boundary and proves excess
/// diagnostics are refused without queueing, while admitted work stays bounded until its cache checks are released.
#[tokio::test(start_paused = true)]
async fn concurrent_late_diagnostics_bound_tasks_requests_and_retained_candidates() {
    const REQUESTS: usize = crate::runtime::MAX_CONCURRENT_LATE_DIAGNOSTICS + 4;
    let checks = Arc::new(DiagnosticChecks { block_late: true, ..DiagnosticChecks::default() });
    let (h, runtime) =
        diagnostic_handler(5000, crate::runtime::MAX_CONCURRENT_LATE_DIAGNOSTICS, checks.clone());
    let h = Arc::new(h);
    let start = Instant::now();
    let mut requests = Vec::new();
    for i in 0..REQUESTS {
        let h = h.clone();
        requests.push(tokio::spawn(async move {
            let path = format!("/{}/stream/movie/tt{:07}.json", valid_blob(), 9_100_000 + i);
            get(&h, &path, &[EARLY]).await
        }));
    }
    for request in requests {
        assert_eq!(request.await.expect("request task").status(), 200);
    }
    assert_eq!(start.elapsed(), Duration::from_millis(1010), "diagnostic admission delayed early responses");
    let metrics = runtime.metrics();
    assert_eq!(
        metrics.late_diagnostic_active.load(Ordering::SeqCst),
        crate::runtime::MAX_CONCURRENT_LATE_DIAGNOSTICS as i64
    );
    assert_eq!(
        metrics.late_diagnostic_refused.load(Ordering::SeqCst),
        (REQUESTS - crate::runtime::MAX_CONCURRENT_LATE_DIAGNOSTICS) as i64
    );

    tokio::time::sleep(Duration::from_secs(1)).await;
    wait_for_count(&checks.late_requests, crate::runtime::MAX_CONCURRENT_LATE_DIAGNOSTICS).await;
    let asked = checks.late_hashes.lock().expect("late hashes");
    assert_eq!(asked.len(), crate::runtime::MAX_CONCURRENT_LATE_DIAGNOSTICS);
    assert!(asked.iter().all(|hashes| hashes.len() == crate::handler::MAX_SEEDS));
    drop(asked);
    assert_eq!(
        metrics.late_diagnostic_retained.load(Ordering::SeqCst),
        (crate::runtime::MAX_CONCURRENT_LATE_DIAGNOSTICS * crate::handler::MAX_SEEDS) as i64
    );
    assert_eq!(
        metrics.late_diagnostic_cache_checked.load(Ordering::SeqCst),
        (crate::runtime::MAX_CONCURRENT_LATE_DIAGNOSTICS * crate::handler::MAX_SEEDS) as i64
    );

    checks.release.add_permits(crate::runtime::MAX_CONCURRENT_LATE_DIAGNOSTICS);
    for _ in 0..1000 {
        if metrics.late_diagnostic_active.load(Ordering::SeqCst) == 0 {
            break;
        }
        tokio::task::yield_now().await;
    }
    assert_eq!(metrics.late_diagnostic_active.load(Ordering::SeqCst), 0, "released tasks retained permits");
}
