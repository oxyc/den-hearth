//! `?fresh=1` on a stream list: rebuilt past the held list and every indexer's kept answer, at most once per list
//! key per interval, and stored like any other build.

use std::sync::atomic::{AtomicBool, AtomicI32, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;

use crate::cache::{Cache, MemoryCache};
use crate::config::Indexer;
use crate::diskcache::TieredCache;
use crate::handler::{new_handler, FreshGate, FRESH_INTERVAL, MAX_FRESH_KEYS};
use crate::rank::RawStream;
use crate::scrape::{kept_answer, scrape_all_cached, DoError, ScrapeQuery, Scraper};
use crate::tests::handler::{body_str, get, hdr, streams_len, test_deps, valid_blob, RecordingCache};
use crate::util::key_hash;

/// An indexer whose answer is kept (it has an answer key) and that starts listing a release once `out` is set: an
/// episode that turns up after the first list was built.
struct ReleasingScraper {
    calls: Arc<AtomicI32>,
    out: Arc<AtomicBool>,
}

#[async_trait]
impl Scraper for ReleasingScraper {
    fn id(&self) -> Indexer {
        Indexer("torrentio".into())
    }
    async fn scrape(&self, _q: &ScrapeQuery) -> Result<Vec<RawStream>, DoError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        if !self.out.load(Ordering::SeqCst) {
            return Ok(Vec::new());
        }
        Ok(vec![RawStream {
            info_hash: "a".repeat(40),
            title: "Show S02E01 1080p WEB-DL".into(),
            file_idx: Some(0),
            ..Default::default()
        }])
    }
    fn answer_key(&self) -> Option<String> {
        Some(key_hash("https://torrentio.example"))
    }
}

/// Holds an ordinary answer across a fresh one and makes the two generations visible in the returned release.
struct RacingScraper {
    ordinary_started: tokio::sync::mpsc::UnboundedSender<()>,
    ordinary_release: Arc<tokio::sync::Semaphore>,
    fail_fresh: bool,
    answer_key: &'static str,
}

/// A second source that makes the ordinary list complete and the fresh one partial.
struct PartialOnFreshScraper;

#[async_trait]
impl Scraper for PartialOnFreshScraper {
    fn id(&self) -> Indexer {
        Indexer("comet".into())
    }

    async fn scrape(&self, q: &ScrapeQuery) -> Result<Vec<RawStream>, DoError> {
        if q.fresh {
            Err("fresh secondary failure".into())
        } else {
            Ok(vec![RawStream {
                info_hash: "b".repeat(40),
                title: "Ordinary secondary 720p WEB-DL".into(),
                file_idx: Some(0),
                ..Default::default()
            }])
        }
    }
}

/// Both generations are held independently, for proving a caller leaving does not cancel fresh authority.
struct CancelledFreshScraper {
    ordinary_started: tokio::sync::mpsc::UnboundedSender<()>,
    ordinary_release: Arc<tokio::sync::Semaphore>,
    fresh_started: tokio::sync::mpsc::UnboundedSender<()>,
    fresh_release: Arc<tokio::sync::Semaphore>,
}

#[async_trait]
impl Scraper for CancelledFreshScraper {
    fn id(&self) -> Indexer {
        Indexer("torrentio".into())
    }

    async fn scrape(&self, q: &ScrapeQuery) -> Result<Vec<RawStream>, DoError> {
        let (started, release, hash, title) = if q.fresh {
            (&self.fresh_started, &self.fresh_release, "b", "Fresh generation 1080p WEB-DL")
        } else {
            (&self.ordinary_started, &self.ordinary_release, "a", "Ordinary generation 1080p WEB-DL")
        };
        let _ = started.send(());
        release.acquire().await.expect("the test never closes its release gate").forget();
        Ok(vec![RawStream {
            info_hash: hash.repeat(40),
            title: title.into(),
            file_idx: Some(0),
            ..Default::default()
        }])
    }

    fn answer_key(&self) -> Option<String> {
        Some(key_hash("https://cancelled-fresh.example"))
    }
}

#[async_trait]
impl Scraper for RacingScraper {
    fn id(&self) -> Indexer {
        Indexer("torrentio".into())
    }

    async fn scrape(&self, q: &ScrapeQuery) -> Result<Vec<RawStream>, DoError> {
        if q.fresh {
            if self.fail_fresh {
                return Err("fresh indexer failure".into());
            }
            return Ok(vec![RawStream {
                info_hash: "b".repeat(40),
                title: "Fresh generation 1080p WEB-DL".into(),
                file_idx: Some(0),
                ..Default::default()
            }]);
        }
        let _ = self.ordinary_started.send(());
        self.ordinary_release.acquire().await.expect("the test never closes its release gate").forget();
        Ok(vec![RawStream {
            info_hash: "a".repeat(40),
            title: "Ordinary generation 1080p WEB-DL".into(),
            file_idx: Some(0),
            ..Default::default()
        }])
    }

    fn answer_key(&self) -> Option<String> {
        Some(key_hash(self.answer_key))
    }
}

fn racing_handler(
    cache: Arc<dyn Cache>,
    started: tokio::sync::mpsc::UnboundedSender<()>,
    release: Arc<tokio::sync::Semaphore>,
    fail_fresh: bool,
) -> crate::handler::Handler {
    new_handler(test_deps(move |deps| {
        deps.cache = cache;
        deps.make_scrapers = Arc::new(move |_| {
            vec![Box::new(RacingScraper {
                ordinary_started: started.clone(),
                ordinary_release: Arc::clone(&release),
                fail_fresh,
                answer_key: "https://race-http.example",
            })]
        });
    }))
}

fn partial_racing_handler(
    cache: Arc<dyn Cache>,
    started: tokio::sync::mpsc::UnboundedSender<()>,
    release: Arc<tokio::sync::Semaphore>,
) -> crate::handler::Handler {
    new_handler(test_deps(move |deps| {
        deps.cache = cache;
        deps.make_scrapers = Arc::new(move |_| {
            vec![
                Box::new(RacingScraper {
                    ordinary_started: started.clone(),
                    ordinary_release: Arc::clone(&release),
                    fail_fresh: false,
                    answer_key: "https://race-partial.example",
                }),
                Box::new(PartialOnFreshScraper),
            ]
        });
    }))
}

fn bidirectional_racing_handler(
    cache: Arc<dyn Cache>,
    ordinary_started: tokio::sync::mpsc::UnboundedSender<()>,
    ordinary_release: Arc<tokio::sync::Semaphore>,
    fresh_started: tokio::sync::mpsc::UnboundedSender<()>,
    fresh_release: Arc<tokio::sync::Semaphore>,
) -> crate::handler::Handler {
    new_handler(test_deps(move |deps| {
        deps.cache = cache;
        deps.make_scrapers = Arc::new(move |_| {
            vec![Box::new(CancelledFreshScraper {
                ordinary_started: ordinary_started.clone(),
                ordinary_release: Arc::clone(&ordinary_release),
                fresh_started: fresh_started.clone(),
                fresh_release: Arc::clone(&fresh_release),
            })]
        });
    }))
}

fn handler(calls: &Arc<AtomicI32>, out: &Arc<AtomicBool>) -> crate::handler::Handler {
    let (calls, out) = (Arc::clone(calls), Arc::clone(out));
    new_handler(test_deps(move |d| {
        d.make_scrapers = Arc::new(move |_| {
            vec![Box::new(ReleasingScraper { calls: Arc::clone(&calls), out: Arc::clone(&out) })]
        });
    }))
}

fn path(episode: u32) -> String {
    format!("/{}/stream/series/tt1234567:2:{episode}.json", valid_blob())
}

// fresh=1 skips both the kept list and the indexer's kept answer, so a release that appeared since is found, and
// the rebuilt list is what the next plain request is served.
#[tokio::test]
async fn fresh_bypasses_both_caches_and_the_next_plain_request_gets_the_rebuilt_list() {
    let (calls, out) = (Arc::new(AtomicI32::new(0)), Arc::new(AtomicBool::new(false)));
    let h = handler(&calls, &out);
    let plain = path(1);

    let first = get(&h, &plain, &[]).await;
    assert_eq!((first.status().as_u16(), streams_len(&first)), (200, 0), "first list");
    out.store(true, Ordering::SeqCst);
    let held = get(&h, &plain, &[]).await;
    assert_eq!(streams_len(&held), 0, "a plain request is served the kept list");
    assert_eq!(calls.load(Ordering::SeqCst), 1, "a plain request asked an indexer");

    let fresh = get(&h, &format!("{plain}?fresh=1"), &[]).await;
    assert_eq!((fresh.status().as_u16(), streams_len(&fresh)), (200, 1), "fresh list");
    assert_eq!(calls.load(Ordering::SeqCst), 2, "fresh must ask the indexer past its kept answer");
    assert_eq!(hdr(&fresh, "x-den-fresh"), "rebuilt");
    assert!(
        hdr(&fresh, "server-timing").starts_with("fresh;desc=rebuilt, "),
        "{}",
        hdr(&fresh, "server-timing")
    );
    assert!(
        hdr(&fresh, "cache-control").starts_with("private, max-age=300, stale-while-revalidate=300"),
        "{}",
        hdr(&fresh, "cache-control")
    );
    assert!(hdr(&fresh, "access-control-expose-headers").contains("X-Den-Fresh"));

    let after = get(&h, &plain, &[]).await;
    assert_eq!(streams_len(&after), 1, "the plain request after a fresh one must get the rebuilt list");
    assert_eq!(calls.load(Ordering::SeqCst), 2, "the rebuilt list is served without asking again");
    assert_eq!(hdr(&after, "x-den-fresh"), "");
}

// A second fresh=1 inside the interval is a plain request: the kept list, no scrape, and it says so.
#[tokio::test]
async fn fresh_is_limited_per_list() {
    let (calls, out) = (Arc::new(AtomicI32::new(0)), Arc::new(AtomicBool::new(false)));
    let h = handler(&calls, &out);

    get(&h, &format!("{}?fresh=1", path(1)), &[]).await;
    out.store(true, Ordering::SeqCst);
    let limited = get(&h, &format!("{}?fresh=1", path(1)), &[]).await;
    assert_eq!(calls.load(Ordering::SeqCst), 1, "a second fresh within {FRESH_INTERVAL:?} asked the indexer");
    assert_eq!((limited.status().as_u16(), streams_len(&limited)), (200, 0));
    assert_eq!(hdr(&limited, "x-den-fresh"), "limited");
    assert!(
        hdr(&limited, "server-timing").starts_with("cache;desc=hit"),
        "{}",
        hdr(&limited, "server-timing")
    );

    // Another episode is another list, with its own allowance.
    let other = get(&h, &format!("{}?fresh=1", path(2)), &[]).await;
    assert_eq!(hdr(&other, "x-den-fresh"), "rebuilt");
}

// Without fresh=1 nothing changes, and a config that does not open is refused as before.
#[tokio::test]
async fn fresh_leaves_other_requests_alone() {
    let (calls, out) = (Arc::new(AtomicI32::new(0)), Arc::new(AtomicBool::new(false)));
    let h = handler(&calls, &out);
    let plain = path(1);

    get(&h, &plain, &[]).await;
    let again = get(&h, &plain, &[]).await;
    assert_eq!((hdr(&again, "x-den-fresh").as_str(), calls.load(Ordering::SeqCst)), ("", 1));
    let zero = get(&h, &format!("{plain}?fresh=0"), &[]).await;
    assert_eq!((hdr(&zero, "x-den-fresh").as_str(), calls.load(Ordering::SeqCst)), ("", 1), "fresh=0");
    let bad = get(&h, "/@@@/stream/series/tt1234567:2:1.json?fresh=1", &[]).await;
    assert_eq!(bad.status(), 400, "a bad config with fresh=1");
    assert_eq!(hdr(&bad, "x-den-fresh"), "");
}

#[test]
fn fresh_gate_interval_and_bound() {
    let mut gate = FreshGate::default();
    let t0 = Instant::now();
    assert!(gate.claim("k", t0));
    assert!(
        !gate.claim("k", t0 + FRESH_INTERVAL - Duration::from_secs(1)),
        "one rebuild per key per interval"
    );
    assert!(gate.claim("k", t0 + FRESH_INTERVAL), "the interval ends");

    // Full of live entries: refused. Once they age out, the sweep makes room.
    let t1 = t0 + FRESH_INTERVAL;
    for i in 0..MAX_FRESH_KEYS - 1 {
        assert!(gate.claim(&format!("full{i}"), t1));
    }
    assert!(!gate.claim("over", t1), "a full ledger of live entries admitted another key");
    assert!(gate.claim("over", t1 + 3 * FRESH_INTERVAL));
    assert_eq!(gate.len(), 1, "the sweep left stale entries");
}

// A fresh question asks past the kept answer, and keeps the new one.
#[tokio::test]
async fn fresh_scrape_asks_past_the_kept_answer_and_keeps_the_new_one() {
    let cache: Arc<dyn Cache> = Arc::new(MemoryCache::new(1 << 20));
    let (calls, out) = (Arc::new(AtomicI32::new(0)), Arc::new(AtomicBool::new(true)));
    let scrapers: Vec<Arc<dyn Scraper>> = vec![Arc::new(ReleasingScraper { calls: calls.clone(), out })];
    let mut q = ScrapeQuery { kind: "movie".into(), imdb: "tt1".into(), ..Default::default() };
    let ask = |q: ScrapeQuery| {
        let (scrapers, cache) = (scrapers.clone(), cache.clone());
        async move {
            scrape_all_cached(&scrapers, &q, Duration::from_secs(1), Some(cache), Duration::from_secs(60))
                .await
        }
    };
    ask(q.clone()).await;
    q.fresh = true;
    ask(q.clone()).await;
    q.fresh = false;
    ask(q).await;
    assert_eq!(
        calls.load(Ordering::SeqCst),
        2,
        "fresh asks past the kept answer, and the next plain one reuses it"
    );
}

// The actual provider-cache path: an ordinary upstream request starts first and finishes last. Its caller may
// use that answer, but it cannot replace the fresh answer kept for the next list.
#[tokio::test]
async fn stale_ordinary_provider_completion_cannot_replace_a_fresh_answer() {
    let cache: Arc<dyn Cache> = Arc::new(MemoryCache::new(1 << 20));
    let (started_tx, mut started_rx) = tokio::sync::mpsc::unbounded_channel();
    let release = Arc::new(tokio::sync::Semaphore::new(0));
    let scrapers: Vec<Arc<dyn Scraper>> = vec![Arc::new(RacingScraper {
        ordinary_started: started_tx,
        ordinary_release: Arc::clone(&release),
        fail_fresh: false,
        answer_key: "https://race-provider.example",
    })];
    let ordinary_q =
        ScrapeQuery { kind: "movie".into(), imdb: "tt-provider-race".into(), ..Default::default() };
    let ordinary = {
        let (scrapers, cache, q) = (scrapers.clone(), cache.clone(), ordinary_q.clone());
        tokio::spawn(async move {
            scrape_all_cached(&scrapers, &q, Duration::from_secs(2), Some(cache), Duration::from_secs(60))
                .await
        })
    };
    tokio::time::timeout(Duration::from_secs(1), started_rx.recv())
        .await
        .expect("ordinary provider request did not start")
        .expect("ordinary provider start channel closed");

    let fresh_q = ScrapeQuery { fresh: true, ..ordinary_q.clone() };
    let (fresh, ok, complete) = scrape_all_cached(
        &scrapers,
        &fresh_q,
        Duration::from_secs(2),
        Some(cache.clone()),
        Duration::from_secs(60),
    )
    .await;
    assert!(ok && complete);
    assert_eq!(fresh[0].title, "Fresh generation 1080p WEB-DL");

    release.add_permits(1);
    let (ordinary, ok, complete) = ordinary.await.expect("ordinary provider task");
    assert!(ok && complete);
    assert_eq!(ordinary[0].title, "Ordinary generation 1080p WEB-DL");

    let (after, ok, complete) = scrape_all_cached(
        &scrapers,
        &ordinary_q,
        Duration::from_secs(2),
        Some(cache),
        Duration::from_secs(60),
    )
    .await;
    assert!(ok && complete);
    assert_eq!(after[0].title, "Fresh generation 1080p WEB-DL", "stale provider answer won the cache race");
}

// Freshness is authority, not wall-clock recency: an ordinary request admitted after a fresh request may
// still represent older source truth. It cannot overwrite the fresh provider answer when it happens to finish
// later.
#[tokio::test]
async fn later_started_ordinary_provider_completion_cannot_replace_an_active_fresh_answer() {
    let cache: Arc<dyn Cache> = Arc::new(MemoryCache::new(1 << 20));
    let (ordinary_started_tx, mut ordinary_started_rx) = tokio::sync::mpsc::unbounded_channel();
    let (fresh_started_tx, mut fresh_started_rx) = tokio::sync::mpsc::unbounded_channel();
    let ordinary_release = Arc::new(tokio::sync::Semaphore::new(0));
    let fresh_release = Arc::new(tokio::sync::Semaphore::new(0));
    let scrapers: Vec<Arc<dyn Scraper>> = vec![Arc::new(CancelledFreshScraper {
        ordinary_started: ordinary_started_tx,
        ordinary_release: Arc::clone(&ordinary_release),
        fresh_started: fresh_started_tx,
        fresh_release: Arc::clone(&fresh_release),
    })];
    let ordinary_q =
        ScrapeQuery { kind: "movie".into(), imdb: "tt-fresh-first-provider".into(), ..Default::default() };
    let fresh = {
        let (scrapers, cache) = (scrapers.clone(), cache.clone());
        let q = ScrapeQuery { fresh: true, ..ordinary_q.clone() };
        tokio::spawn(async move {
            scrape_all_cached(&scrapers, &q, Duration::from_secs(2), Some(cache), Duration::from_secs(60))
                .await
        })
    };
    fresh_started_rx.recv().await.expect("fresh provider start");
    let ordinary = {
        let (scrapers, cache, q) = (scrapers.clone(), cache.clone(), ordinary_q.clone());
        tokio::spawn(async move {
            scrape_all_cached(&scrapers, &q, Duration::from_secs(2), Some(cache), Duration::from_secs(60))
                .await
        })
    };
    ordinary_started_rx.recv().await.expect("ordinary provider start");

    fresh_release.add_permits(1);
    let (fresh_answer, ok, complete) = fresh.await.expect("fresh provider task");
    assert!(ok && complete);
    assert_eq!(fresh_answer[0].title, "Fresh generation 1080p WEB-DL");
    ordinary_release.add_permits(1);
    ordinary.await.expect("ordinary provider task");

    let (after, _, _) = scrape_all_cached(
        &scrapers,
        &ordinary_q,
        Duration::from_secs(2),
        Some(cache),
        Duration::from_secs(60),
    )
    .await;
    assert_eq!(after[0].title, "Fresh generation 1080p WEB-DL");
}

// Dropping the fresh caller does not cancel the detached provider work. When that work succeeds, it has the
// same newer authority it received at admission and still fences the older ordinary completion.
#[tokio::test]
async fn cancelled_fresh_caller_still_leaves_its_success_authoritative() {
    let cache: Arc<dyn Cache> = Arc::new(MemoryCache::new(1 << 20));
    let (ordinary_started_tx, mut ordinary_started_rx) = tokio::sync::mpsc::unbounded_channel();
    let (fresh_started_tx, mut fresh_started_rx) = tokio::sync::mpsc::unbounded_channel();
    let ordinary_release = Arc::new(tokio::sync::Semaphore::new(0));
    let fresh_release = Arc::new(tokio::sync::Semaphore::new(0));
    let scrapers: Vec<Arc<dyn Scraper>> = vec![Arc::new(CancelledFreshScraper {
        ordinary_started: ordinary_started_tx,
        ordinary_release: Arc::clone(&ordinary_release),
        fresh_started: fresh_started_tx,
        fresh_release: Arc::clone(&fresh_release),
    })];
    let ordinary_q =
        ScrapeQuery { kind: "movie".into(), imdb: "tt-cancelled-fresh".into(), ..Default::default() };
    let ordinary = {
        let (scrapers, cache, q) = (scrapers.clone(), cache.clone(), ordinary_q.clone());
        tokio::spawn(async move {
            scrape_all_cached(&scrapers, &q, Duration::from_secs(2), Some(cache), Duration::from_secs(60))
                .await
        })
    };
    ordinary_started_rx.recv().await.expect("ordinary provider start");

    let leaving = {
        let (scrapers, cache) = (scrapers.clone(), cache.clone());
        let q = ScrapeQuery { fresh: true, ..ordinary_q.clone() };
        tokio::spawn(async move {
            tokio::time::timeout(
                Duration::from_millis(25),
                scrape_all_cached(
                    &scrapers,
                    &q,
                    Duration::from_secs(2),
                    Some(cache),
                    Duration::from_secs(60),
                ),
            )
            .await
        })
    };
    fresh_started_rx.recv().await.expect("fresh provider start");
    assert!(leaving.await.expect("fresh caller task").is_err(), "fresh caller did not leave");

    fresh_release.add_permits(1);
    tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            if kept_answer(Some(&*cache), &*scrapers[0], &ordinary_q)
                .is_some_and(|answer| answer[0].title == "Fresh generation 1080p WEB-DL")
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("detached fresh answer was not published");

    ordinary_release.add_permits(1);
    ordinary.await.expect("ordinary provider task");
    let (after, _, _) = scrape_all_cached(
        &scrapers,
        &ordinary_q,
        Duration::from_secs(2),
        Some(cache),
        Duration::from_secs(60),
    )
    .await;
    assert_eq!(after[0].title, "Fresh generation 1080p WEB-DL");
}

// The full HTTP path fences the ranked-list write as well as the provider answer. The fresh response is
// durable, remains the plain in-memory hit after the old request finishes, and is still the canonical entry
// after reopening the disk tier (a process restart has no old producer left to fence).
#[tokio::test]
async fn stale_ordinary_list_completion_cannot_replace_a_fresh_durable_result() {
    let dir = tempfile::TempDir::new().expect("cache directory");
    let durable = Arc::new(TieredCache::new(1 << 20, dir.path()));
    let recording = Arc::new(RecordingCache { inner: durable.clone(), last: Default::default() });
    let cache: Arc<dyn Cache> = recording.clone();
    let (started_tx, mut started_rx) = tokio::sync::mpsc::unbounded_channel();
    let release = Arc::new(tokio::sync::Semaphore::new(0));
    let handler = Arc::new(racing_handler(cache, started_tx, Arc::clone(&release), false));
    let plain = path(31);
    let ordinary = {
        let (handler, plain) = (Arc::clone(&handler), plain.clone());
        tokio::spawn(async move { get(&handler, &plain, &[]).await })
    };
    tokio::time::timeout(Duration::from_secs(1), started_rx.recv())
        .await
        .expect("ordinary list request did not reach its provider")
        .expect("ordinary list start channel closed");

    let fresh = get(&handler, &format!("{plain}?fresh=1"), &[]).await;
    assert_eq!(hdr(&fresh, "x-den-fresh"), "rebuilt");
    assert!(body_str(&fresh).contains("Fresh generation"), "fresh response: {}", body_str(&fresh));
    assert!(hdr(&fresh, "cache-control").starts_with("private, max-age=300"));

    release.add_permits(1);
    let old = ordinary.await.expect("ordinary list task");
    assert!(body_str(&old).contains("Ordinary generation"), "ordinary response: {}", body_str(&old));

    let after = get(&handler, &plain, &[]).await;
    assert!(body_str(&after).contains("Fresh generation"), "plain response regressed: {}", body_str(&after));
    assert!(hdr(&after, "server-timing").starts_with("cache;desc=hit"));
    let key = recording.last_key();
    assert!(recording.get(&key).is_some_and(|entry| entry.contains("Fresh generation")));

    drop(handler);
    drop(recording);
    drop(durable);
    let reopened = TieredCache::new(1 << 20, dir.path());
    assert!(
        reopened.get(&key).is_some_and(|entry| entry.contains("Fresh generation")),
        "the durable tier did not retain the winning fresh list"
    );
}

// The same reverse admission order reaches both real write layers through HTTP. A plain list build admitted
// while fresh work is active is lower-authority even though its generation number is later.
#[tokio::test]
async fn later_started_ordinary_list_cannot_replace_an_active_fresh_result() {
    let cache: Arc<dyn Cache> = Arc::new(MemoryCache::new(1 << 20));
    let (ordinary_started_tx, mut ordinary_started_rx) = tokio::sync::mpsc::unbounded_channel();
    let (fresh_started_tx, mut fresh_started_rx) = tokio::sync::mpsc::unbounded_channel();
    let ordinary_release = Arc::new(tokio::sync::Semaphore::new(0));
    let fresh_release = Arc::new(tokio::sync::Semaphore::new(0));
    let handler = Arc::new(bidirectional_racing_handler(
        cache,
        ordinary_started_tx,
        Arc::clone(&ordinary_release),
        fresh_started_tx,
        Arc::clone(&fresh_release),
    ));
    let plain = path(34);
    let fresh = {
        let (handler, path) = (Arc::clone(&handler), format!("{plain}?fresh=1"));
        tokio::spawn(async move { get(&handler, &path, &[]).await })
    };
    fresh_started_rx.recv().await.expect("fresh list start");
    let ordinary = {
        let (handler, plain) = (Arc::clone(&handler), plain.clone());
        tokio::spawn(async move { get(&handler, &plain, &[]).await })
    };
    ordinary_started_rx.recv().await.expect("ordinary list start");

    fresh_release.add_permits(1);
    let fresh_response = fresh.await.expect("fresh list task");
    assert!(body_str(&fresh_response).contains("Fresh generation"));
    ordinary_release.add_permits(1);
    let ordinary_response = ordinary.await.expect("ordinary list task");
    assert!(body_str(&ordinary_response).contains("Ordinary generation"));

    let after = get(&handler, &plain, &[]).await;
    assert!(body_str(&after).contains("Fresh generation"), "fresh list lost authority");
    assert!(hdr(&after, "server-timing").starts_with("cache;desc=hit"));
}

// A failed newer refresh publishes neither provider answer nor ranked list. The older successful build is
// still allowed to become the fallback, and the next plain request reads it without another upstream call.
#[tokio::test]
async fn failed_fresh_result_does_not_block_ordinary_recovery() {
    let cache: Arc<dyn Cache> = Arc::new(MemoryCache::new(1 << 20));
    let (started_tx, mut started_rx) = tokio::sync::mpsc::unbounded_channel();
    let release = Arc::new(tokio::sync::Semaphore::new(0));
    let handler = Arc::new(racing_handler(cache, started_tx, Arc::clone(&release), true));
    let plain = path(32);
    let ordinary = {
        let (handler, plain) = (Arc::clone(&handler), plain.clone());
        tokio::spawn(async move { get(&handler, &plain, &[]).await })
    };
    tokio::time::timeout(Duration::from_secs(1), started_rx.recv())
        .await
        .expect("ordinary list request did not reach its provider")
        .expect("ordinary list start channel closed");

    let failed = get(&handler, &format!("{plain}?fresh=1"), &[]).await;
    assert_eq!(hdr(&failed, "x-den-degraded"), "indexers");
    assert_eq!(hdr(&failed, "cache-control"), "no-store");

    release.add_permits(1);
    let recovered = ordinary.await.expect("ordinary list task");
    assert!(body_str(&recovered).contains("Ordinary generation"));
    let after = get(&handler, &plain, &[]).await;
    assert!(body_str(&after).contains("Ordinary generation"), "recovered list was not cached");
    assert!(hdr(&after, "server-timing").starts_with("cache;desc=hit"));
}

// A non-empty fresh result with one missing source is deliberately cacheable for the short partial TTL. It is
// still newer source truth, so an older complete build cannot replace it merely because that older work
// finished later.
#[tokio::test]
async fn partial_fresh_result_fences_an_older_complete_list() {
    let cache: Arc<dyn Cache> = Arc::new(MemoryCache::new(1 << 20));
    let (started_tx, mut started_rx) = tokio::sync::mpsc::unbounded_channel();
    let release = Arc::new(tokio::sync::Semaphore::new(0));
    let handler = Arc::new(partial_racing_handler(cache, started_tx, Arc::clone(&release)));
    let plain = path(33);
    let ordinary = {
        let (handler, plain) = (Arc::clone(&handler), plain.clone());
        tokio::spawn(async move { get(&handler, &plain, &[]).await })
    };
    started_rx.recv().await.expect("ordinary list start");

    let fresh = get(&handler, &format!("{plain}?fresh=1"), &[]).await;
    assert!(body_str(&fresh).contains("Fresh generation"));
    assert_eq!(hdr(&fresh, "x-den-degraded"), "");
    assert!(
        hdr(&fresh, "cache-control").starts_with("private, max-age=60, stale-while-revalidate=60"),
        "partial cache header: {}",
        hdr(&fresh, "cache-control")
    );

    release.add_permits(1);
    let old = ordinary.await.expect("ordinary list task");
    assert!(body_str(&old).contains("Ordinary secondary"), "ordinary response was not complete");
    let after = get(&handler, &plain, &[]).await;
    assert!(body_str(&after).contains("Fresh generation"), "partial fresh list lost authority");
    assert!(
        !body_str(&after).contains("Ordinary secondary"),
        "older complete list replaced the fresh partial"
    );
    assert!(hdr(&after, "server-timing").starts_with("cache;desc=hit"));
}
