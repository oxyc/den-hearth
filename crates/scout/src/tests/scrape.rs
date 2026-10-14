//! Go: `internal/scout/scrape_test.go` (267 lines). Work package 5.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;

use crate::config::Indexer;
use crate::rank::{RawStream, GIB};
use crate::scrape::{parse_seeders, parse_size, DoError, Doer, Scraper, ScrapeQuery, StremioScraper};

use super::mock::{resp, MockDoer};

const TORRENTIO_FIXTURE: &str = r#"{"streams":[
{"name":"Torrentio\n4k HDR","title":"The.Movie.2024.2160p.WEB-DL.DDP5.1.DV.HDR.HEVC-GROUP\n👤 142 💾 18.4 GB ⚙️ ThePirateBay","infoHash":"AABBCCDDEEFF00112233445566778899AABBCCDD","fileIdx":0},
{"name":"Torrentio","title":"already resolved somehow"},
{"infoHash":"bbbbccddeeff00112233445566778899aabbccdd","behaviorHints":{"filename":"Exact.Name.mkv","videoSize":2048}},
"garbage"
]}"#;

// Go `gibBytes`: the byte count of `f` GiB, rounded.
fn gib_bytes(f: f64) -> i64 {
    (f * GIB as f64 + 0.5) as i64
}

fn movie(imdb: &str) -> ScrapeQuery {
    ScrapeQuery { kind: "movie".into(), imdb: imdb.into(), ..Default::default() }
}

#[test]
fn parse_stremio_streams() {
    let seeds = crate::scrape::parse_stremio_streams(TORRENTIO_FIXTURE.as_bytes(), "torrentio");
    // url-only + garbage dropped
    assert_eq!(seeds.len(), 2, "want 2 seeds, got {}: {seeds:?}", seeds.len());
    assert!(
        seeds[0].info_hash == "aabbccddeeff00112233445566778899aabbccdd"
            && seeds[0].seeders.unwrap_or(0) == 142
            && seeds[0].file_idx.is_some(),
        "seed0: {:?}",
        seeds[0]
    );
    assert!(seeds[0].size_bytes == Some(gib_bytes(18.4)), "seed0 size: {:?}", seeds[0].size_bytes);
    assert!(
        seeds[1].title == "Exact.Name.mkv" && seeds[1].size_bytes == Some(2048),
        "seed1 (behaviorHints): {:?}",
        seeds[1]
    );
}

#[test]
fn parse_size_seeders() {
    assert_eq!(parse_size("👤 50 💾 15.2 GB"), Some(gib_bytes(15.2)), "parseSize GB");
    assert_eq!(parse_size("700 MB"), Some(700 * crate::label::MIB), "parseSize MB");
    assert_eq!(parse_size("no size"), None, "parseSize none");
    assert_eq!(parse_seeders("👤 123 ⚙️ prov"), Some(123), "parseSeeders emoji");
    assert_eq!(parse_seeders("Seeders: 7"), Some(7), "parseSeeders word");
    assert_eq!(parse_seeders("none"), None, "parseSeeders none");
}

#[tokio::test]
async fn scraper_url() {
    // (url, user-agent) of the last request the client saw
    let seen: Arc<Mutex<(String, String)>> = Arc::new(Mutex::new((String::new(), String::new())));
    let seen_in_client = Arc::clone(&seen);
    let client: Arc<dyn Doer> = Arc::new(MockDoer(move |r: reqwest::Request| {
        let ua = r.headers().get("user-agent").and_then(|v| v.to_str().ok()).unwrap_or("").to_owned();
        *seen_in_client.lock().expect("lock") = (r.url().to_string(), ua);
        Ok(resp(200, r#"{"streams":[]}"#))
    }));

    let sc = StremioScraper {
        indexer: Indexer("torrentio".into()),
        base_url: "https://torrentio.strem.fun".into(),
        client: Arc::clone(&client),
    };
    let _ = sc.scrape(&movie("tt1234567")).await;
    let (url, ua) = seen.lock().expect("lock").clone();
    assert_eq!(url, "https://torrentio.strem.fun/stream/movie/tt1234567.json", "movie url");
    // A non-default User-Agent is required — Torrentio 403s Go-http-client (regression guard).
    assert!(
        !ua.is_empty() && !ua.contains("Go-http-client"),
        "scrape must send a browser-like User-Agent, got {ua:?}"
    );

    let sc2 = StremioScraper {
        indexer: Indexer("comet".into()),
        base_url: "https://comet.example/".into(),
        client: Arc::clone(&client),
    };
    let _ = sc2
        .scrape(&ScrapeQuery { kind: "series".into(), imdb: "tt99".into(), season: 2, episode: 5, has_ep: true, ..Default::default() })
        .await;
    // Colons as Stremio clients send them, not %3A: an encoded id is a different URL to torrentio's CDN, which
    // serves only the URL everyone requests while its origin is down.
    assert_eq!(seen.lock().expect("lock").0, "https://comet.example/stream/series/tt99:2:5.json", "series url");

    // non-200 → error (fan-out treats as no result)
    let bad_client: Arc<dyn Doer> = Arc::new(MockDoer(|_r: reqwest::Request| Ok(resp(502, "{}"))));
    let bad = StremioScraper {
        indexer: Indexer("torz".into()),
        base_url: "https://torz.example".into(),
        client: bad_client,
    };
    assert!(bad.scrape(&movie("tt1")).await.is_err(), "non-200 should error");
}

// A shed request is retried, and a refusal is not.
//
// The whole retry mechanism had no coverage: retryableScrapeStatus, both backoff durations and the
// attempt counter all survived mutation, and making scrape() never retry left the package green. The
// only test touching a 502 asserts `err != nil`, which is true whether or not the retry happened.
//
// The cost of losing it is not one release. Opening a season asks for every episode at once, an indexer
// sheds part of that burst, and each shed request becomes "no source found" for an episode the same
// indexer serves 50 releases of a second later — and per the quorum an episode that lost its only
// answering indexer is a degraded, uncacheable response rather than a list.
#[tokio::test]
async fn scrape_retries_a_shed_request_but_not_a_refusal() {
    let body = format!(r#"{{"streams":[{{"name":"x","infoHash":"{}","title":"Movie 1080p"}}]}}"#, "a".repeat(40));

    // (name, status, want_calls, want_stream)
    let cases: [(&str, u16, usize, bool); 5] = [
        // Shedding: the indexer is declining to answer right now, so ask again.
        ("408 request timeout", 408, 2, true),
        ("502 bad gateway", 502, 2, true),
        // A refusal is an answer. Retrying it spends someone else's share of the 8s budget to be told
        // the same thing, and 403 is what an unconfigured indexer returns for every request it gets.
        // A rate limit is waited out by the transport's pause, not asked again inside the same budget.
        ("429 too many requests", 429, 1, false),
        ("403 forbidden", 403, 1, false),
        ("404 not found", 404, 1, false),
    ];
    for (name, status, want_calls, want_stream) in cases {
        let calls = Arc::new(AtomicUsize::new(0));
        let calls_in_client = Arc::clone(&calls);
        let body = body.clone();
        let client: Arc<dyn Doer> = Arc::new(MockDoer(move |_r: reqwest::Request| {
            let n = calls_in_client.fetch_add(1, Ordering::SeqCst) + 1;
            if n == 1 {
                Ok(resp(status, "{}"))
            } else {
                Ok(resp(200, &body))
            }
        }));
        let sc = StremioScraper { indexer: Indexer("torrentio".into()), base_url: "https://tor.example".into(), client };

        let result = sc.scrape(&movie("tt1")).await;
        let made = calls.load(Ordering::SeqCst);
        assert_eq!(made, want_calls, "{name}: made {made} request(s), want {want_calls}");
        if want_stream {
            assert!(
                matches!(&result, Ok(streams) if streams.len() == 1),
                "{name}: a shed request should be retried and then succeed: {result:?}"
            );
        } else {
            assert!(result.is_err(), "{name}: a refusal is an answer and must be reported, not retried into a success");
        }
    }
}

// Torrentio is asked on its BARE path, whatever the filters say.
//
// The options segment was free to add and expensive to keep: with torrentio's origin down, Cloudflare
// answers from `stale-if-error` only for a URL it already holds, and a private options path is warm
// essentially never. Scout collected 502s for an episode whose bare path served 50 releases to a curl on
// the same machine. Nothing is lost — rankStreams drops cam/scr itself and applies its own ordering.
#[test]
#[ignore = "needs makeScrapers (scrape.rs, not yet stubbed) and Config/Filters from config.rs"]
fn make_scrapers_torrentio_uses_bare_path() {
    #[cfg(any())]
    fn pending() {
        use crate::config::{Config, Filters, Indexer};
        use crate::scrape::make_scrapers;
        use std::collections::HashMap;

        let cfg = Config {
            indexers: vec![Indexer("torrentio".into()), Indexer("mediafusion".into())],
            filters: Filters { exclude_cam: true, ..Default::default() },
            ..Default::default()
        };
        let overrides = HashMap::from([(Indexer("mediafusion".into()), "https://mf.self/CONFIG".to_owned())]);
        let got = make_scrapers(&cfg, plain_doer(), guarded_doer(), &overrides);
        assert_eq!(got[0].base_url(), "https://torrentio.strem.fun", "torrentio base");
        assert_eq!(got[1].base_url(), "https://mf.self/CONFIG", "mediafusion override");

        // The filter setting must not change the URL at all — that coupling was the whole defect.
        let off = make_scrapers(
            &Config { indexers: vec![Indexer("torrentio".into())], filters: Filters { exclude_cam: false, ..Default::default() }, ..Default::default() },
            plain_doer(),
            guarded_doer(),
            &HashMap::new(),
        );
        assert_eq!(off[0].base_url(), "https://torrentio.strem.fun", "excludeCam off");

        // An operator override still wins, so a self-hosted torrentio can be pointed at.
        let custom = make_scrapers(
            &cfg,
            plain_doer(),
            guarded_doer(),
            &HashMap::from([(Indexer("torrentio".into()), "https://tor.self/opts".to_owned())]),
        );
        assert_eq!(custom[0].base_url(), "https://tor.self/opts", "override");
    }
    todo!("pending: scrape_test.go TestMakeScrapersTorrentioUsesBarePath")
}

// A canned answer for `FakeScraper`.
enum FakeAnswer {
    Streams(Vec<RawStream>),
    Fail,
    Hang,
}

struct FakeScraper {
    name: &'static str,
    answer: FakeAnswer,
}

#[async_trait]
impl Scraper for FakeScraper {
    fn id(&self) -> Indexer {
        Indexer(self.name.into())
    }
    async fn scrape(&self, _q: &ScrapeQuery) -> Result<Vec<RawStream>, DoError> {
        match &self.answer {
            FakeAnswer::Streams(streams) => Ok(streams.clone()),
            FakeAnswer::Fail => Err("context canceled".into()),
            // Go: <-ctx.Done(). The timeout in scrape_all drops the future, which is the Rust equivalent.
            FakeAnswer::Hang => std::future::pending().await,
        }
    }
}

#[tokio::test]
async fn scrape_all() {
    let ok = FakeScraper {
        name: "torrentio",
        answer: FakeAnswer::Streams(vec![RawStream {
            info_hash: "a".repeat(40),
            title: "first".into(),
            seeders: Some(10),
            ..Default::default()
        }]),
    };
    let boom = FakeScraper { name: "comet", answer: FakeAnswer::Fail };
    let hang = FakeScraper { name: "torz", answer: FakeAnswer::Hang };
    let scrapers: Vec<Arc<dyn Scraper>> = vec![Arc::new(ok), Arc::new(boom), Arc::new(hang)];
    let (out, any_ok, complete) = crate::scrape::scrape_all(&scrapers, &ScrapeQuery::default(), Duration::from_millis(30)).await;
    assert!(out.len() == 1 && out[0].info_hash == "a".repeat(40), "scrapeAll gather-what-responded: {out:?}");
    // A partial NON-EMPTY answer is still worth serving and worth caching — whatever came back is real.
    // Marking it non-authoritative put the full scrape and a fresh debrid fan-out on every request while
    // one indexer was flaky, which is a worse answer than the slightly short list it was protecting
    // against. What it is NOT is complete, and that is carried separately so it can be held for less time.
    assert!(any_ok, "a non-empty list is real; refusing to trust it re-scrapes on every request");
    assert!(!complete, "two of three indexers failed; the list cannot be described as complete");

    // Every indexer answering IS complete, empty or not — otherwise nothing would ever be cached in full.
    let quiet = FakeScraper { name: "mediafusion", answer: FakeAnswer::Streams(vec![]) };
    let answered_ok = FakeScraper {
        name: "torrentio",
        answer: FakeAnswer::Streams(vec![RawStream { info_hash: "a".repeat(40), title: "first".into(), seeders: Some(10), ..Default::default() }]),
    };
    let both: Vec<Arc<dyn Scraper>> = vec![Arc::new(answered_ok), Arc::new(quiet)];
    let (_, _, whole) = crate::scrape::scrape_all(&both, &ScrapeQuery::default(), Duration::from_millis(30)).await;
    assert!(whole, "every indexer answered; that is a complete list");

    // every scraper failing → anyOK false (a degraded blip, not a genuine empty)
    let only_boom: Vec<Arc<dyn Scraper>> = vec![Arc::new(FakeScraper { name: "comet", answer: FakeAnswer::Fail })];
    let (_, any, _) = crate::scrape::scrape_all(&only_boom, &ScrapeQuery::default(), Duration::from_millis(30)).await;
    assert!(!any, "anyOK should be false when every scraper failed");

    // dedupe merges facts across indexers
    let a = FakeScraper {
        name: "torrentio",
        answer: FakeAnswer::Streams(vec![RawStream { info_hash: "h".repeat(40), title: "first".into(), seeders: Some(10), ..Default::default() }]),
    };
    let b = FakeScraper {
        name: "comet",
        answer: FakeAnswer::Streams(vec![RawStream {
            info_hash: "h".repeat(40),
            title: "second".into(),
            seeders: Some(99),
            file_idx: Some(3),
            size_bytes: Some(500),
            ..Default::default()
        }]),
    };
    let pair: Vec<Arc<dyn Scraper>> = vec![Arc::new(a), Arc::new(b)];
    let (merged, _, _) = crate::scrape::scrape_all(&pair, &ScrapeQuery::default(), Duration::from_secs(1)).await;
    assert!(
        merged.len() == 1
            && merged[0].title == "first"
            && merged[0].seeders.unwrap_or(0) == 99
            && merged[0].file_idx.is_some()
            && merged[0].size_bytes.is_some(),
        "dedupe merge: {:?}",
        merged.first()
    );
}

// A scrape that spends its whole budget queueing behind the limiter must not then ask the indexer.
//
// The request cannot succeed — its context is already dead — and its failure was reported as
// "indexer unreachable", so scout's own queue was logged and counted as the upstream's outage. That is
// the difference between "we could not get to it in time" and "it is down", and only the second is a
// reason to go looking at the indexer.
#[test]
#[ignore = "needs indexerLimiter (scrape.go, package-5 static not stubbed yet) and a deadline-aware scrape"]
fn scrape_a_budget_spent_queueing_is_not_an_indexer_outage() {
    #[cfg(any())]
    async fn pending() {
        use crate::scrape::indexer_limiter;
        use std::time::Duration;

        const HOST: &str = "queued-past-budget.example";
        // Drain the burst so the next caller genuinely has to wait for a token. These are free by
        // definition — the burst exists so a household's normal use never queues.
        for _ in 0..30 {
            indexer_limiter().wait(HOST).await;
        }

        let asked = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let asked_in_client = Arc::clone(&asked);
        let client: Arc<dyn Doer> = Arc::new(MockDoer(move |_r: reqwest::Request| {
            asked_in_client.store(true, Ordering::SeqCst);
            Ok(resp(200, r#"{"streams":[]}"#))
        }));
        let sc = StremioScraper { indexer: Indexer("torrentio".into()), base_url: format!("https://{HOST}"), client };

        // Shorter than the ~1s the drained bucket now makes a caller wait.
        let result = tokio::time::timeout(Duration::from_millis(20), sc.scrape(&movie("tt0111161"))).await;

        assert!(!asked.load(Ordering::SeqCst), "asked the indexer with a context that had already expired");
        // The caller still learns nothing came back — that is what keeps an empty result non-authoritative.
        // What it must be able to tell is WHY, so a deadline is not filed against the indexer's health.
        assert!(result.is_err(), "a scrape that never happened must report an error");
    }
    todo!("pending: scrape_test.go TestScrape_aBudgetSpentQueueingIsNotAnIndexerOutage")
}
