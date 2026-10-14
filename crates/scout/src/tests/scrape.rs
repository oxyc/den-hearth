//! Go: `internal/scout/scrape_test.go` (267 lines). Work package 5.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;

use crate::config::Indexer;
use crate::rank::{RawStream, GIB};
use crate::scrape::{parse_seeders, parse_size, DoError, Doer, ScrapeQuery, Scraper, StremioScraper};

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
        .scrape(&ScrapeQuery {
            kind: "series".into(),
            imdb: "tt99".into(),
            season: 2,
            episode: 5,
            has_ep: true,
            ..Default::default()
        })
        .await;
    // Colons as Stremio clients send them, not %3A: an encoded id is a different URL to torrentio's CDN, which
    // serves only the URL everyone requests while its origin is down.
    assert_eq!(
        seen.lock().expect("lock").0,
        "https://comet.example/stream/series/tt99:2:5.json",
        "series url"
    );

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
    let body =
        format!(r#"{{"streams":[{{"name":"x","infoHash":"{}","title":"Movie 1080p"}}]}}"#, "a".repeat(40));

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
        let sc = StremioScraper {
            indexer: Indexer("torrentio".into()),
            base_url: "https://tor.example".into(),
            client,
        };

        let result = sc.scrape(&movie("tt1")).await;
        let made = calls.load(Ordering::SeqCst);
        assert_eq!(made, want_calls, "{name}: made {made} request(s), want {want_calls}");
        if want_stream {
            assert!(
                matches!(&result, Ok(streams) if streams.len() == 1),
                "{name}: a shed request should be retried and then succeed: {result:?}"
            );
        } else {
            assert!(
                result.is_err(),
                "{name}: a refusal is an answer and must be reported, not retried into a success"
            );
        }
    }
}

// Torrentio is asked on its BARE path, whatever the filters say.
//
// The options segment was free to add and expensive to keep: with torrentio's origin down, Cloudflare
// answers from `stale-if-error` only for a URL it already holds, and a private options path is warm
// essentially never. Scout collected 502s for an episode whose bare path served 50 releases to a curl on
// the same machine. Nothing is lost — rankStreams drops cam/scr itself and applies its own ordering.
//
// Go reads `baseURL` off each `*stremioScraper`; the scraper is a trait object here, so each one is asked and the
// URL it asked is the base it was built with.
#[tokio::test]
async fn make_scrapers_torrentio_uses_bare_path() {
    use crate::config::{Config, Filters};
    use crate::scrape::make_scrapers;
    use std::collections::HashMap;

    /// The URL each scraper asked, in scraper order.
    async fn asked_urls(cfg: &Config, overrides: &HashMap<Indexer, String>) -> Vec<String> {
        let seen: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
        let log = Arc::clone(&seen);
        let doer: Arc<dyn Doer> = Arc::new(MockDoer(move |r: reqwest::Request| {
            log.lock().expect("lock").push(r.url().to_string());
            Ok(resp(200, r#"{"streams":[]}"#))
        }));
        let got = make_scrapers(cfg, Arc::clone(&doer), doer, overrides);
        // An indexer with no config URL is never asked, so it leaves no entry.
        for sc in &got {
            let _ = sc.scrape(&movie("tt1")).await;
        }
        let urls = seen.lock().expect("lock").clone();
        urls
    }

    let cfg = Config {
        indexers: vec![Indexer("torrentio".into()), Indexer("mediafusion".into())],
        filters: Filters { exclude_cam: true, ..Default::default() },
        ..Default::default()
    };
    let overrides = HashMap::from([(Indexer("mediafusion".into()), "https://mf.self/CONFIG".to_owned())]);
    let got = asked_urls(&cfg, &overrides).await;
    assert_eq!(got[0], "https://torrentio.strem.fun/stream/movie/tt1.json", "torrentio base: {}", got[0]);
    assert_eq!(got[1], "https://mf.self/CONFIG/stream/movie/tt1.json", "mediafusion override: {}", got[1]);

    // The filter setting must not change the URL at all — that coupling was the whole defect.
    let off = asked_urls(
        &Config {
            indexers: vec![Indexer("torrentio".into())],
            filters: Filters { exclude_cam: false, ..Default::default() },
            ..Default::default()
        },
        &HashMap::new(),
    )
    .await;
    assert_eq!(off[0], "https://torrentio.strem.fun/stream/movie/tt1.json", "excludeCam off: {}", off[0]);

    // An operator override still wins, so a self-hosted torrentio can be pointed at.
    let custom =
        asked_urls(&cfg, &HashMap::from([(Indexer("torrentio".into()), "https://tor.self/opts".to_owned())]))
            .await;
    assert_eq!(custom[0], "https://tor.self/opts/stream/movie/tt1.json", "override: {}", custom[0]);
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
    let (out, any_ok, complete) =
        crate::scrape::scrape_all(&scrapers, &ScrapeQuery::default(), Duration::from_millis(30)).await;
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
        answer: FakeAnswer::Streams(vec![RawStream {
            info_hash: "a".repeat(40),
            title: "first".into(),
            seeders: Some(10),
            ..Default::default()
        }]),
    };
    let both: Vec<Arc<dyn Scraper>> = vec![Arc::new(answered_ok), Arc::new(quiet)];
    let (_, _, whole) =
        crate::scrape::scrape_all(&both, &ScrapeQuery::default(), Duration::from_millis(30)).await;
    assert!(whole, "every indexer answered; that is a complete list");

    // every scraper failing → anyOK false (a degraded blip, not a genuine empty)
    let only_boom: Vec<Arc<dyn Scraper>> =
        vec![Arc::new(FakeScraper { name: "comet", answer: FakeAnswer::Fail })];
    let (_, any, _) =
        crate::scrape::scrape_all(&only_boom, &ScrapeQuery::default(), Duration::from_millis(30)).await;
    assert!(!any, "anyOK should be false when every scraper failed");

    // dedupe merges facts across indexers
    let a = FakeScraper {
        name: "torrentio",
        answer: FakeAnswer::Streams(vec![RawStream {
            info_hash: "h".repeat(40),
            title: "first".into(),
            seeders: Some(10),
            ..Default::default()
        }]),
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
    let (merged, _, _) =
        crate::scrape::scrape_all(&pair, &ScrapeQuery::default(), Duration::from_secs(1)).await;
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
#[tokio::test]
async fn scrape_a_budget_spent_queueing_is_not_an_indexer_outage() {
    use crate::scrape::indexer_limiter;

    const HOST: &str = "queued-past-budget.example";
    // Drain the burst so the next caller genuinely has to wait for a token. These are free by
    // definition — the burst exists so a household's normal use never queues.
    for _ in 0..30 {
        crate::hostlimit::wait(Some(indexer_limiter()), HOST).await;
    }

    let asked = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let asked_in_client = Arc::clone(&asked);
    let client: Arc<dyn Doer> = Arc::new(MockDoer(move |_r: reqwest::Request| {
        asked_in_client.store(true, Ordering::SeqCst);
        Ok(resp(200, r#"{"streams":[]}"#))
    }));
    let sc =
        StremioScraper { indexer: Indexer("torrentio".into()), base_url: format!("https://{HOST}"), client };

    // Shorter than the ~1s the drained bucket now makes a caller wait. Go gives the scrape a 20 ms context; the
    // caller's deadline here is a timeout around the future, which drops it while it is still queued.
    let result = tokio::time::timeout(Duration::from_millis(20), sc.scrape(&movie("tt0111161"))).await;

    assert!(!asked.load(Ordering::SeqCst), "asked the indexer with a context that had already expired");
    // The caller still learns nothing came back — that is what keeps an empty result non-authoritative.
    // What it must be able to tell is WHY, so a deadline is not filed against the indexer's health.
    assert!(result.is_err(), "a scrape that never happened must report an error");
}

// Not in the Go suite: what the Rust port adds around the same behaviour. Go tells an unaskable scraper and an
// HTTP status error apart by type; here they are typed errors, and the cache is shared with the Go build.
mod beyond_the_go_suite {
    use std::collections::HashMap;
    use std::error::Error;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    use async_trait::async_trait;

    use crate::cache::{Cache, MemoryCache};
    use crate::config::{Config, DebridAccount, DebridService, Indexer};
    use crate::coverage::{
        OUTCOME_ANSWERED, OUTCOME_MISCONFIGURED, OUTCOME_REFUSED, OUTCOME_TIMEOUT, OUTCOME_UNREACHABLE,
    };
    use crate::rank::RawStream;
    use crate::scrape::{
        classify_outcome, keep_answer, kept_answer, make_scrapers, parse_stremio_streams, scrape_all_cached,
        scrape_covered, DeadlineExceeded, DoError, Doer, HttpStatusError, MintingScraper, ScrapeQuery,
        Scraper, StremioScraper, UnaskableError,
    };
    use crate::sources::ERR_NOT_PUBLIC;
    use crate::util::key_hash;

    use super::super::mock::{resp, MockDoer};

    type Answer = Result<reqwest::Response, DoError>;

    fn movie(imdb: &str) -> ScrapeQuery {
        ScrapeQuery { kind: "movie".into(), imdb: imdb.into(), ..Default::default() }
    }

    /// A doer that answers `{"streams":[]}` and records the URL of every request it gets.
    fn recording_doer() -> (Arc<dyn Doer>, Arc<Mutex<Vec<String>>>) {
        let seen = Arc::new(Mutex::new(Vec::new()));
        let log = Arc::clone(&seen);
        let doer: Arc<dyn Doer> = Arc::new(MockDoer(move |r: reqwest::Request| -> Answer {
            log.lock().expect("lock").push(r.url().to_string());
            Ok(resp(200, r#"{"streams":[]}"#))
        }));
        (doer, seen)
    }

    fn failing_doer(message: &'static str) -> Arc<dyn Doer> {
        Arc::new(MockDoer(move |_r: reqwest::Request| -> Answer { Err(message.into()) }))
    }

    fn classify(e: &(dyn Error + 'static)) -> &'static str {
        classify_outcome(Some(e))
    }

    fn text(e: &str) -> DoError {
        e.into()
    }

    #[test]
    fn classify_outcome_names_what_an_error_means_for_coverage() {
        assert_eq!(classify_outcome(None), OUTCOME_ANSWERED);
        let status = |code| HttpStatusError { name: "torrentio".into(), code };
        let cases: Vec<(&str, Box<dyn Error + Send + Sync>, &str)> = vec![
            ("deadline", Box::new(DeadlineExceeded), OUTCOME_TIMEOUT),
            ("wrapped deadline", text("wrapped: context deadline exceeded"), OUTCOME_TIMEOUT),
            (
                "queued past the budget",
                text("torrentio: queued past the scrape budget: deadline"),
                OUTCOME_TIMEOUT,
            ),
            (
                "an io timeout",
                Box::new(std::io::Error::new(std::io::ErrorKind::TimedOut, "slow")),
                OUTCOME_TIMEOUT,
            ),
            ("http 408", Box::new(status(408)), OUTCOME_TIMEOUT),
            ("http 429 (or a paused upstream)", Box::new(status(429)), OUTCOME_REFUSED),
            ("http 403", Box::new(status(403)), OUTCOME_REFUSED),
            ("http 502", Box::new(status(502)), OUTCOME_UNREACHABLE),
            ("non-public household source", Box::new(std::io::Error::other(ERR_NOT_PUBLIC)), OUTCOME_REFUSED),
            ("network failure", text("connection reset"), OUTCOME_UNREACHABLE),
            (
                "no config URL",
                Box::new(UnaskableError { indexer: "mediafusion".into(), transient: false }),
                OUTCOME_MISCONFIGURED,
            ),
            (
                "config mint failed",
                Box::new(UnaskableError { indexer: "mediafusion".into(), transient: true }),
                OUTCOME_UNREACHABLE,
            ),
        ];
        for (name, err, want) in cases {
            assert_eq!(classify(&*err), want, "{name}");
        }
    }

    // A scrape's report is the scraper's id and what its error meant; an unaskable one carries no latency or time.
    #[tokio::test]
    async fn scrape_covered_reports_each_source() {
        struct Fixed(&'static str, Option<fn() -> DoError>);
        #[async_trait]
        impl Scraper for Fixed {
            fn id(&self) -> Indexer {
                Indexer(self.0.into())
            }
            async fn scrape(&self, _q: &ScrapeQuery) -> Result<Vec<RawStream>, DoError> {
                match self.1 {
                    Some(err) => Err(err()),
                    None => Ok(vec![RawStream { info_hash: "a".repeat(40), ..Default::default() }]),
                }
            }
        }
        let scrapers: Vec<Arc<dyn Scraper>> = vec![
            Arc::new(Fixed("torrentio", None)),
            Arc::new(Fixed(
                "comet",
                Some(|| Box::new(UnaskableError { indexer: "comet".into(), transient: false })),
            )),
            Arc::new(Fixed("torz", Some(|| Box::new(HttpStatusError { name: "torz".into(), code: 403 })))),
        ];
        let res =
            scrape_covered(&scrapers, &ScrapeQuery::default(), Duration::from_secs(1), None, Duration::ZERO)
                .await;
        let outcomes: Vec<(&str, &str, i64)> =
            res.sources.iter().map(|s| (s.id.as_str(), s.outcome.as_str(), s.items)).collect();
        assert_eq!(
            outcomes,
            [
                ("torrentio", OUTCOME_ANSWERED, 1),
                ("comet", OUTCOME_MISCONFIGURED, 0),
                ("torz", OUTCOME_REFUSED, 0)
            ]
        );
        assert!(res.sources[0].observed_at.is_some(), "an answer carries the time it arrived");
        assert!(
            res.sources[1].observed_at.is_none() && res.sources[1].latency_ms.is_none(),
            "an unasked source has no time"
        );
        assert!(res.sources[2].observed_at.is_some(), "a refusal arrived at some time");
        assert!(!res.complete, "a refusal is a source that did not answer");
        assert!(res.any_ok);
    }

    // The cache is shared with the Go build across a redeploy, so an answer one wrote must read in the other: Go's
    // untagged struct writes its field names as they are, a nil slice as `null`.
    #[tokio::test]
    async fn answers_cached_by_the_go_build_are_read_and_ours_read_in_go() {
        let cache = Arc::new(MemoryCache::new(1 << 20));
        let (client, seen) = recording_doer();
        let sc = StremioScraper {
            indexer: Indexer("torrentio".into()),
            base_url: "https://go-compat.example/".into(),
            client,
        };
        let key = |id: &str| format!("answer:v1:{}:movie:{id}", key_hash("https://go-compat.example"));
        let hash = "b".repeat(40);
        cache.put(
            &key("tt1"),
            &format!(
                r#"[{{"InfoHash":"{hash}","FileIdx":null,"Title":"Movie 1080p","SizeBytes":1234,"PackSizeBytes":null,"Seeders":5,"Cached":false,"CacheKnown":false,"Source":"torrentio","Probe":null}}]"#
            ),
            Duration::from_secs(60),
        );
        cache.put(&key("tt2"), "null", Duration::from_secs(60));

        let one: Vec<Arc<dyn Scraper>> = vec![Arc::new(StremioScraper {
            indexer: sc.indexer.clone(),
            base_url: sc.base_url.clone(),
            client: Arc::clone(&sc.client),
        })];
        let (out, ok, complete) = scrape_all_cached(
            &one,
            &movie("tt1"),
            Duration::from_secs(1),
            Some(cache.clone()),
            Duration::from_secs(60),
        )
        .await;
        assert!(ok && complete && out.len() == 1, "{out:?}");
        assert!(
            out[0].info_hash == hash
                && out[0].size_bytes == Some(1234)
                && out[0].seeders == Some(5)
                && out[0].file_idx.is_none()
        );
        let (out, ok, complete) = scrape_all_cached(
            &one,
            &movie("tt2"),
            Duration::from_secs(1),
            Some(cache.clone()),
            Duration::from_secs(60),
        )
        .await;
        assert!(out.is_empty() && ok && complete, "a nil slice cached by Go is an answer, not a miss");
        assert!(seen.lock().expect("lock").is_empty(), "both were served from the cache");

        keep_answer(Some(&*cache), Duration::from_secs(60), &sc, &movie("tt3"), &out_streams(&hash));
        let raw = cache.get(&key("tt3")).expect("kept");
        assert!(
            raw.contains(r#""InfoHash":""#) && raw.contains(r#""Probe":null"#),
            "not Go's spelling: {raw}"
        );
        assert_eq!(kept_answer(Some(&*cache), &sc, &movie("tt3")).expect("read back")[0].info_hash, hash);
    }

    fn out_streams(hash: &str) -> Vec<RawStream> {
        vec![RawStream {
            info_hash: hash.into(),
            title: "T".into(),
            size_bytes: Some(7),
            ..Default::default()
        }]
    }

    // A scraper that panics inside the shared question must cost that answer, not wedge every later ask for it.
    #[tokio::test]
    async fn a_panicking_scraper_does_not_wedge_its_question() {
        struct Panics;
        #[async_trait]
        impl Scraper for Panics {
            fn id(&self) -> Indexer {
                Indexer("torrentio".into())
            }
            async fn scrape(&self, _q: &ScrapeQuery) -> Result<Vec<RawStream>, DoError> {
                panic!("a malformed answer")
            }
            fn answer_key(&self) -> Option<String> {
                Some("panics".into())
            }
        }
        let scrapers: Vec<Arc<dyn Scraper>> = vec![Arc::new(Panics)];
        for _ in 0..2 {
            let asked = tokio::time::timeout(
                Duration::from_secs(5),
                scrape_all_cached(
                    &scrapers,
                    &movie("tt-panics"),
                    Duration::from_secs(1),
                    None,
                    Duration::ZERO,
                ),
            )
            .await;
            let (out, ok, complete) =
                asked.expect("the caller was left waiting on a question nobody was asking");
            assert!(out.is_empty() && !ok && !complete);
        }
    }

    // The 502 line names the indexer, the class and the request, and never the URL: a configured source's carries
    // an encrypted config, and a transport error's carries the whole thing.
    #[test]
    fn scrape_decision_line_names_the_class_and_never_the_url() {
        let rt = tokio::runtime::Builder::new_current_thread().enable_time().build().expect("runtime");
        let bad = StremioScraper {
            indexer: Indexer("torzlog".into()),
            base_url: "https://torz.example/SECRET-PATH".into(),
            client: Arc::new(MockDoer(|_r: reqwest::Request| -> Answer { Ok(resp(502, "{}")) })),
        };
        let q = ScrapeQuery {
            kind: "movie".into(),
            imdb: "tt1234567".into(),
            rid: "req-scrape-1".into(),
            ..Default::default()
        };
        let out = crate::loglimit::capture(|| {
            let (result, retryable) = rt.block_on(bad.scrape_once("", &q));
            assert!(result.is_err() && retryable, "a 502 is a shed request");
        });
        for want in ["event=scrape", "upstream=torzlog", "rid=req-scrape-1", "reason=http 502"] {
            assert!(out.contains(want), "scrape decision line missing {want:?}:\n{out}");
        }
        assert!(
            !out.contains("SECRET-PATH") && !out.contains("torz.example"),
            "the indexer base URL leaked:\n{out}"
        );

        let unreachable = StremioScraper {
            indexer: Indexer("torzlog2".into()),
            base_url: "https://torz.example/SECRET-PATH".into(),
            client: failing_doer("dial tcp: lookup torz.example/SECRET-PATH?token=LEAK: no such host"),
        };
        let q = ScrapeQuery {
            kind: "movie".into(),
            imdb: "tt7654321".into(),
            rid: "req-scrape-2".into(),
            ..Default::default()
        };
        let out = crate::loglimit::capture(|| {
            let (result, retryable) = rt.block_on(unreachable.scrape_once("", &q));
            assert!(result.is_err() && retryable, "a transport failure is worth another attempt");
        });
        assert!(out.contains("rid=req-scrape-2") && out.contains("event=scrape"), "no decision line:\n{out}");
        assert!(
            !out.contains("SECRET-PATH") && !out.contains("LEAK"),
            "the transport error leaked into the log:\n{out}"
        );
    }

    // A household source that points at a non-public address is refused once; a retry would only spend the budget.
    #[tokio::test]
    async fn a_refused_address_is_not_retried() {
        let calls = Arc::new(AtomicUsize::new(0));
        let n = Arc::clone(&calls);
        let client: Arc<dyn Doer> = Arc::new(MockDoer(move |_r: reqwest::Request| -> Answer {
            n.fetch_add(1, Ordering::SeqCst);
            Err(Box::new(std::io::Error::other(ERR_NOT_PUBLIC)))
        }));
        let sc =
            StremioScraper { indexer: Indexer("own".into()), base_url: "https://127.0.0.1".into(), client };
        assert!(sc.scrape(&movie("tt1")).await.is_err());
        assert_eq!(calls.load(Ordering::SeqCst), 1, "a refusal was asked again");
    }

    // The body is bounded: an indexer that answers with more than MAX_SCRAPE_BYTES is read up to that and no further.
    #[tokio::test]
    async fn the_response_body_is_bounded() {
        let stream = format!(r#"{{"infoHash":"{}","title":"Movie.1080p"}},"#, "c".repeat(40));
        let mut body = String::from(r#"{"streams":["#);
        while body.len() < crate::scrape::MAX_SCRAPE_BYTES + 1000 {
            body.push_str(&stream);
        }
        body.push_str(r#"{"infoHash":"zzzz"}]}"#);
        let client: Arc<dyn Doer> =
            Arc::new(MockDoer(move |_r: reqwest::Request| -> Answer { Ok(resp(200, &body)) }));
        let sc = StremioScraper {
            indexer: Indexer("torrentio".into()),
            base_url: "https://big.example".into(),
            client,
        };
        // Cut mid-document, the JSON is unreadable, which is an empty answer rather than an unbounded read.
        let got = sc.scrape(&movie("tt1")).await.expect("an answer");
        assert!(got.is_empty(), "read past the cap: {} releases", got.len());
    }

    #[test]
    fn parse_follows_go_json_decoding() {
        let h = "a".repeat(40);
        let parse = |body: String| parse_stremio_streams(body.as_bytes(), "torrentio");
        // keys match case-insensitively, as encoding/json does
        let got = parse(format!(
            r#"{{"STREAMS":[{{"INFOHASH":"{h}","Title":"Movie 1080p","BehaviorHints":{{"VideoSize":4096}}}}]}}"#
        ));
        assert!(got.len() == 1 && got[0].size_bytes == Some(4096), "{got:?}");
        // a field of the wrong type skips that element only
        let got = parse(format!(
            r#"{{"streams":[{{"infoHash":"{h}","fileIdx":1.5}},{{"infoHash":"{h}","name":7}},{{"infoHash":"{h}"}}]}}"#
        ));
        assert_eq!(got.len(), 1, "{got:?}");
        // a negative file index is no index, and a hash with the wrong shape is no hash
        let got = parse(format!(
            r#"{{"streams":[{{"infoHash":"{h}","fileIdx":-1}},{{"infoHash":"{}"}}]}}"#,
            "g".repeat(39)
        ));
        assert!(got.len() == 1 && got[0].file_idx.is_none(), "{got:?}");
        // streams of the wrong type, a body that is not an object, and invalid JSON all read as nothing
        for body in [r#"{"streams":{}}"#, r#"[1]"#, r#"{"streams":[1"#, r#"{"streams":null}"#, ""] {
            assert!(parse(body.to_owned()).is_empty(), "{body}");
        }
        // an invalid byte inside a string is U+FFFD, as in Go, not a refusal of the whole body
        let mut raw = format!(r#"{{"streams":[{{"infoHash":"{h}","title":"Caf"#).into_bytes();
        raw.extend_from_slice(b"\xff\"}]}");
        let got = parse_stremio_streams(&raw, "torrentio");
        assert_eq!(got.len(), 1);
        assert_eq!(got[0].title, "Caf\u{fffd}");
        // a seeder word spelled with a long s is the same word to Go's case folding
        assert_eq!(crate::scrape::parse_seeders("ſeeders: 12"), Some(12));
    }

    // A playback link that does not parse as a URL names no hash, and the hash must be a whole path segment.
    #[test]
    fn a_hash_is_read_from_the_path_of_a_link_go_would_parse() {
        let h = "d".repeat(40);
        let from = |url: String| {
            let body = format!(r#"{{"streams":[{{"url":"{url}","title":"Movie 1080p"}}]}}"#);
            parse_stremio_streams(body.as_bytes(), "own").into_iter().map(|s| s.info_hash).collect::<Vec<_>>()
        };
        assert_eq!(from(format!("https://x.example/resolve/{h}/f.mkv")), vec![h.clone()]);
        assert_eq!(from(format!("/playback/{h}")), vec![h.clone()]);
        assert_eq!(from(format!("https://x.example/a%2F{h}")), vec![h.clone()]);
        assert!(from(format!("https://x.example/play?h={h}")).is_empty());
        assert!(from(format!("https://x.example/play#{h}")).is_empty());
        assert!(from(format!("https://x.example/play/{h}x")).is_empty());
        assert!(from(format!("mailto:{h}")).is_empty());
        assert!(from(format!("https://x.example/%zz/{h}")).is_empty());
    }

    fn config_with(indexers: &[&str], sources: &[&str]) -> Config {
        Config {
            indexers: indexers.iter().map(|i| Indexer((*i).into())).collect(),
            sources: sources.iter().map(|s| (*s).to_owned()).collect(),
            debrid: vec![DebridAccount { service: DebridService::TorBox, token: "scrape-test-token".into() }],
            ..Default::default()
        }
    }

    // Torrentio is asked on its BARE path, an override wins, and an indexer that needs a config segment and has none
    // stays in the list as one that cannot answer, so it still counts in the quorum.
    #[tokio::test]
    async fn make_scrapers_builds_the_list_the_quorum_counts() {
        let (plain, plain_seen) = recording_doer();
        let (guarded, guarded_seen) = recording_doer();
        let cfg = config_with(&["torrentio", "mediafusion", "comet"], &["https://mf.example/cfg"]);
        let overrides = HashMap::from([(Indexer("mediafusion".into()), "https://mf.self/CONFIG".to_owned())]);
        let got = make_scrapers(&cfg, plain, guarded, &overrides);
        let ids: Vec<String> = got.iter().map(|s| s.id().0).collect();
        assert_eq!(ids, ["own", "torrentio", "mediafusion", "comet"]);
        for sc in &got[..3] {
            sc.scrape(&movie("tt1")).await.expect("an answer");
        }
        assert_eq!(
            guarded_seen.lock().expect("lock").as_slice(),
            ["https://mf.example/cfg/stream/movie/tt1.json"]
        );
        assert_eq!(
            plain_seen.lock().expect("lock").as_slice(),
            [
                "https://torrentio.strem.fun/stream/movie/tt1.json",
                "https://mf.self/CONFIG/stream/movie/tt1.json"
            ]
        );
        // Minting is off by default, so comet has no config: it cannot be asked, and says why.
        let err = got[3].scrape(&movie("tt1")).await.expect_err("comet is unconfigured");
        let unaskable = err.downcast_ref::<UnaskableError>().expect("a typed refusal");
        assert!(!unaskable.transient, "an unconfigured indexer is not an outage");
    }

    // With minting on, comet's config is built from the account on the spot, and a mint that cannot be made leaves the
    // indexer unaskable: transient when the host failed, permanent when there is nothing to mint.
    #[tokio::test]
    async fn a_minting_scraper_mints_when_asked_and_says_why_when_it_cannot() {
        let (client, seen) = recording_doer();
        let comet = MintingScraper {
            indexer: Indexer("comet".into()),
            config: config_with(&[], &[]),
            env_var: "COMET_URL",
            client,
        };
        comet.scrape(&movie("tt1")).await.expect("an answer");
        let url = seen.lock().expect("lock")[0].clone();
        assert!(
            url.starts_with("https://comet.elfhosted.com/")
                && url.ends_with("/stream/movie/tt1.json")
                && url.len() > 80,
            "comet was not asked on a minted path: {url}"
        );
        assert!(comet.answer_key().is_some());

        let torz = MintingScraper {
            indexer: Indexer("torz".into()),
            config: config_with(&[], &[]),
            env_var: "TORZ_URL",
            client: failing_doer("never asked"),
        };
        let err = torz.scrape(&movie("tt1")).await.expect_err("torz has no minter");
        assert!(!err.downcast_ref::<UnaskableError>().expect("typed").transient);

        let mediafusion = MintingScraper {
            indexer: Indexer("mediafusion".into()),
            config: config_with(&[], &[]),
            env_var: "MEDIAFUSION_URL",
            client: failing_doer("dial tcp: i/o timeout"),
        };
        let err = mediafusion.scrape(&movie("tt1")).await.expect_err("the host is down");
        assert!(
            err.downcast_ref::<UnaskableError>().expect("typed").transient,
            "an unreachable host is an outage"
        );
    }
}
