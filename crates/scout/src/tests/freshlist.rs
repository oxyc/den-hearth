//! `?fresh=1` on a stream list: rebuilt past the held list and every indexer's kept answer, at most once per list
//! key per interval, and stored like any other build.

use std::sync::atomic::{AtomicBool, AtomicI32, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;

use crate::cache::{Cache, MemoryCache};
use crate::config::Indexer;
use crate::handler::{new_handler, FreshGate, FRESH_INTERVAL, MAX_FRESH_KEYS};
use crate::rank::RawStream;
use crate::scrape::{scrape_all_cached, DoError, ScrapeQuery, Scraper};
use crate::tests::handler::{get, hdr, streams_len, test_deps, valid_blob};
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
