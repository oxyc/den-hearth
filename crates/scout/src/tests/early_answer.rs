//! A stream list answers once its fast indexers have had `LATE_GRACE` to be joined, not on the slowest one.
//!
//! On a paused clock: each fake indexer answers after a fixed latency in virtual time, so the time a scrape takes is
//! exact and the run is instant. `cargo test -p den-scout early_answer -- --nocapture` prints the before/after
//! table the profiles below produce.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use tokio::time::Instant;

use crate::cache::{Cache, MemoryCache};
use crate::config::Indexer;
use crate::coverage::{OUTCOME_ANSWERED, OUTCOME_TIMEOUT};
use crate::handler::new_handler;
use crate::loglimit::Capture;
use crate::rank::{RawStream, GIB};
use crate::scrape::{scrape_covered_until, DoError, ScrapeQuery, Scraper, LATE_GRACE};
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
    assert_eq!(outcomes, [("torrentio", OUTCOME_ANSWERED), ("mediafusion", OUTCOME_TIMEOUT)]);
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

/// Through the stream route: the list goes out after the grace, without the slow indexer's release, and once that
/// lands the log says whether it would have led the list. Here it would have (a cached 4K copy), and says so.
#[tokio::test(start_paused = true)]
async fn the_route_answers_early_and_counts_what_the_late_indexer_would_have_changed() {
    let h = new_handler(test_deps(|d| {
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
    }));
    let log = Capture::start();
    let start = Instant::now();
    let resp = get(&h, &format!("/{}/stream/movie/tt1234567.json", valid_blob()), &[]).await;
    assert_eq!(resp.status(), 200);
    assert_eq!(start.elapsed(), Duration::from_millis(1400), "answered a grace after the first indexer");
    let body = String::from_utf8_lossy(resp.body()).into_owned();
    assert!(body.contains("Movie 1080p WEB-DL") && !body.contains("Movie 2160p"), "{body}");
    assert!(body.contains(r#""id":"mediafusion","outcome":"timeout""#), "{body}");

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
