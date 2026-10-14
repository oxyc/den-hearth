//! Go: `internal/scout/coverage_test.go` (219 lines). Work package 4.

use std::sync::Arc;
use std::time::Duration;

use crate::cache::{Cache, MemoryCache};
use crate::config::Indexer;
use crate::coverage::{
    classify_scrape as classify, decide_answer_kind as decide_kind, AnswerEnvelope, ANSWER_EMPTY,
    ANSWER_LIVE, ANSWER_PARTIAL, ANSWER_UNKNOWN, OUTCOME_ANSWERED, OUTCOME_MISCONFIGURED,
    OUTCOME_QUARANTINED, OUTCOME_REFUSED, OUTCOME_TIMEOUT, OUTCOME_UNREACHABLE,
};
use crate::handler::new_handler;
use crate::indexerconfig::UnaskableScraper;
use crate::rank::RawStream;
use crate::scrape::{
    keep_answer, scrape_covered as cover, DeadlineExceeded, DoError, HttpStatusError, ScrapeQuery, Scraper,
    StremioScraper, UnaskableError,
};
use crate::sources::ERR_NOT_PUBLIC;
use crate::tests::handler::{
    blob, body_str, fake_scraper, get, test_deps, test_seeds, valid_blob, FakeFuture,
};
use crate::tests::mock::MockDoer;

/// Go `envelopeOf`: splits a stream-list body into its `streams` array and its `den` envelope.
pub(crate) fn envelope_of(body: &str) -> (serde_json::Value, AnswerEnvelope) {
    let v: serde_json::Value = serde_json::from_str(body).expect("a JSON stream list");
    let den = v.get("den").expect("no den envelope");
    let env: AnswerEnvelope = serde_json::from_value(den.clone()).expect("a den envelope");
    (v["streams"].clone(), env)
}

/// `empty` is the one kind that asserts a negative, so it needs every source to have answered and nothing
/// degraded; anything short of that with nothing to serve is `unknown`.
#[test]
fn decide_answer_kind() {
    for (name, streams, complete, degraded, want) in [
        ("all answered, none found", 0, true, "", ANSWER_EMPTY),
        ("one source missing, none found", 0, false, "", ANSWER_UNKNOWN),
        ("every source failed", 0, false, "indexers", ANSWER_UNKNOWN),
        ("all answered, cache check out, none served", 0, true, "cache-check", ANSWER_UNKNOWN),
        ("one source missing, releases found", 3, false, "", ANSWER_PARTIAL),
        ("all answered, releases found", 3, true, "", ANSWER_LIVE),
        ("all answered, releases found, cache check out", 3, true, "cache-check", ANSWER_LIVE),
    ] {
        assert_eq!(crate::coverage::decide_answer_kind(streams, complete, degraded), want, "{name}");
    }
}

/// Go `fmt.Errorf("<prefix>: %w", context.DeadlineExceeded)`.
#[derive(Debug)]
struct WrappedDeadline(&'static str);

impl std::fmt::Display for WrappedDeadline {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {}", self.0, DeadlineExceeded)
    }
}

impl std::error::Error for WrappedDeadline {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&DeadlineExceeded)
    }
}

/// Go `TestClassifyScrape`: every scrape error maps to one outcome. Go tells an unaskable scraper apart by its type;
/// here the scraper's failure says what it is (`UnaskableError`), so the case is the scraper's error.
#[test]
fn classify_scrape() {
    let asked = || fake_scraper("torrentio", || Box::pin(async { Ok(Vec::new()) }) as FakeFuture);
    let err = |text: &str| -> Option<DoError> { Some(Box::new(std::io::Error::other(text.to_owned()))) };
    let deadline = |prefix: &'static str| -> Option<DoError> { Some(Box::new(WrappedDeadline(prefix))) };
    let http = |code: u16| -> Option<DoError> {
        Some(Box::new(HttpStatusError { name: "torrentio".to_owned(), code }))
    };
    let unaskable = |transient: bool| -> Option<DoError> {
        Some(Box::new(UnaskableError { indexer: "mediafusion".to_owned(), transient }))
    };
    let cases: Vec<(&str, Option<DoError>, &str)> = vec![
        ("answered", None, OUTCOME_ANSWERED),
        ("deadline", deadline("wrapped"), OUTCOME_TIMEOUT),
        ("queued past the budget", deadline("torrentio: queued past the scrape budget"), OUTCOME_TIMEOUT),
        ("http 408", http(408), OUTCOME_TIMEOUT),
        ("http 429 (or a paused upstream)", http(429), OUTCOME_REFUSED),
        ("http 403", http(403), OUTCOME_REFUSED),
        ("http 502", http(502), OUTCOME_UNREACHABLE),
        ("non-public household source", err(&format!("dial: {ERR_NOT_PUBLIC}")), OUTCOME_REFUSED),
        ("network failure", err("connection reset"), OUTCOME_UNREACHABLE),
        ("no config URL", unaskable(false), OUTCOME_MISCONFIGURED),
        ("config mint failed", unaskable(true), OUTCOME_UNREACHABLE),
    ];
    for (name, e, want) in cases {
        let got = classify(asked().as_ref(), e.as_ref());
        assert_eq!(got, want, "{name}: {got:?}, want {want:?}");
    }
}

/// Go `TestScrapeCovered`: every scraper appears in the coverage with what it did, and completeness follows from
/// those outcomes.
#[tokio::test]
async fn scrape_covered() {
    let found = || -> Arc<dyn Scraper> {
        Arc::from(fake_scraper("torrentio", || {
            Box::pin(async {
                Ok(vec![RawStream {
                    info_hash: "a".repeat(40),
                    title: "Movie 1080p".to_owned(),
                    ..Default::default()
                }])
            }) as FakeFuture
        }))
    };
    let none = || -> Arc<dyn Scraper> {
        Arc::from(fake_scraper("comet", || Box::pin(async { Ok(Vec::new()) }) as FakeFuture))
    };
    // Go's `<-ctx.Done()`: never answers, so only the scrape's own budget ends it.
    let hang = || -> Arc<dyn Scraper> {
        Arc::from(fake_scraper("mediafusion", || {
            Box::pin(std::future::pending::<Result<Vec<RawStream>, DoError>>()) as FakeFuture
        }))
    };
    let unconfigured = || -> Arc<dyn Scraper> {
        Arc::new(UnaskableScraper { indexer: Indexer("mediafusion".to_owned()), transient: false })
    };

    let cases: Vec<(&str, Vec<Arc<dyn Scraper>>, bool, Vec<&str>, &str)> = vec![
        (
            "all answered, none found",
            vec![none(), unconfigured()],
            true,
            vec![OUTCOME_ANSWERED, OUTCOME_MISCONFIGURED],
            ANSWER_EMPTY,
        ),
        (
            "one timed out, none found",
            vec![none(), hang()],
            false,
            vec![OUTCOME_ANSWERED, OUTCOME_TIMEOUT],
            ANSWER_UNKNOWN,
        ),
        (
            "one timed out, releases found",
            vec![found(), hang()],
            false,
            vec![OUTCOME_ANSWERED, OUTCOME_TIMEOUT],
            ANSWER_PARTIAL,
        ),
        (
            "all answered, releases found",
            vec![found(), none()],
            true,
            vec![OUTCOME_ANSWERED, OUTCOME_ANSWERED],
            ANSWER_LIVE,
        ),
    ];
    for (name, scrapers, complete, outcomes, kind) in cases {
        let res =
            cover(&scrapers, &ScrapeQuery::default(), Duration::from_millis(30), None, Duration::ZERO).await;
        assert_eq!(res.complete, complete, "{name}: complete = {}, want {complete}", res.complete);
        assert_eq!(
            res.sources.len(),
            scrapers.len(),
            "{name}: {} sources in coverage, want every one of {}",
            res.sources.len(),
            scrapers.len()
        );
        for (i, want) in outcomes.iter().enumerate() {
            let s = &res.sources[i];
            assert!(
                s.id == scrapers[i].id().0 && s.outcome == *want,
                "{name}: source {i} = {} {}, want {} {want}",
                s.id,
                s.outcome,
                scrapers[i].id().0
            );
            if *want == OUTCOME_ANSWERED {
                assert!(s.observed_at.is_some(), "{name}: source {i} answered with no time");
            }
        }
        let degraded = if res.any_ok { "" } else { "indexers" };
        let got = decide_kind(res.seeds.len() as i64, res.complete, degraded);
        assert_eq!(got, kind, "{name}: answerKind = {got:?}, want {kind:?}");
    }
}

/// Go `TestScrapeCovered_reusedAnswerIsMarked`: an answer reused from the indexer answer cache is an answer,
/// marked as reused.
#[tokio::test]
async fn scrape_covered_reused_answer_is_marked() {
    let cache: Arc<dyn Cache> = Arc::new(MemoryCache::new(1 << 20));
    let sc = StremioScraper {
        indexer: Indexer("torrentio".to_owned()),
        base_url: "https://torrentio.example".to_owned(),
        client: Arc::new(MockDoer(|_| -> Result<reqwest::Response, DoError> {
            panic!("a reused answer must not ask the indexer")
        })),
    };
    let q = ScrapeQuery { kind: "movie".to_owned(), imdb: "tt1".to_owned(), ..Default::default() };
    keep_answer(Some(&*cache), Duration::from_secs(60), &sc, &q, &test_seeds());
    let scrapers: Vec<Arc<dyn Scraper>> = vec![Arc::new(sc)];
    let res = cover(&scrapers, &q, Duration::from_secs(1), Some(cache), Duration::from_secs(60)).await;
    let s = &res.sources[0];
    assert!(
        s.outcome == OUTCOME_ANSWERED && s.cached && s.items == 3 && res.complete,
        "reused answer: {s:?}, complete {}",
        res.complete
    );
}

/// Go `TestStream_envelopeReportsCoverage`: the body carries the envelope, the header and the envelope agree, and
/// a pass with a silent source never says `empty`.
#[tokio::test]
async fn stream_envelope_reports_coverage() {
    fn answers_empty() -> Box<dyn crate::scrape::Scraper> {
        fake_scraper("torrentio", || Box::pin(async { Ok(Vec::new()) }) as FakeFuture)
    }
    fn answers_seeds() -> Box<dyn crate::scrape::Scraper> {
        fake_scraper("torrentio", || Box::pin(async { Ok(test_seeds()) }) as FakeFuture)
    }
    fn times_out() -> Box<dyn crate::scrape::Scraper> {
        fake_scraper("comet", || {
            Box::pin(std::future::pending::<Result<Vec<crate::rank::RawStream>, crate::scrape::DoError>>())
                as FakeFuture
        })
    }
    type Make = fn() -> Vec<Box<dyn crate::scrape::Scraper>>;
    let cases: [(&str, Make, &str, &str, bool, Vec<&str>, bool); 4] = [
        (
            "all answered, none found",
            || vec![answers_empty()],
            ANSWER_EMPTY,
            "",
            true,
            vec!["answered"],
            false,
        ),
        (
            "one timed out, none found",
            || vec![answers_empty(), times_out()],
            ANSWER_UNKNOWN,
            "indexers",
            false,
            vec!["answered", "timeout"],
            false,
        ),
        (
            "one timed out, releases found",
            || vec![answers_seeds(), times_out()],
            ANSWER_PARTIAL,
            "",
            false,
            vec!["answered", "timeout"],
            true,
        ),
        (
            "all answered, releases found",
            || vec![answers_seeds()],
            ANSWER_LIVE,
            "",
            true,
            vec!["answered"],
            true,
        ),
    ];
    for (name, make, kind, degraded, complete, outcomes, streams) in cases {
        let h = new_handler(test_deps(move |d| {
            d.scrape_timeout = Duration::from_millis(30);
            d.make_scrapers = std::sync::Arc::new(move |_| make());
        }));
        let rr = get(&h, &format!("/{}/stream/movie/tt1234567.json", valid_blob()), &[]).await;
        let (list, env) = envelope_of(&body_str(&rr));
        assert!(
            env.v == 1 && env.answer_kind == kind && env.coverage.complete == complete,
            "{name}: envelope = {env:?}"
        );
        let header = crate::tests::handler::hdr(&rr, "x-den-degraded");
        assert!(
            header == degraded && env.degraded == header,
            "{name}: X-Den-Degraded {header:?}, envelope {:?}",
            env.degraded
        );
        // A cached (non-degraded) list says when it expires; an uncached (degraded) one does not.
        assert!(
            env.generated_at.is_some() && degraded.is_empty() != env.expires_at.is_none(),
            "{name}: generatedAt/expiresAt"
        );
        assert_eq!(env.coverage.sources.len(), outcomes.len(), "{name}: coverage sources");
        for (i, want) in outcomes.iter().enumerate() {
            assert_eq!(env.coverage.sources[i].outcome, *want, "{name}: source {i} outcome");
        }
        // What a client that ignores `den` reads is still a plain Stremio stream list.
        assert_eq!(list.as_array().is_some_and(|a| !a.is_empty()), streams, "{name}: streams");
    }
}

/// Go `TestStream_quarantinedIndexerAppearsInCoverage`: an indexer the config names but this build has disabled is
/// listed, not dropped, and does not hold the list back from being complete.
#[tokio::test]
async fn stream_quarantined_indexer_appears_in_coverage() {
    let cfg =
        blob(r#"{"debrid":[{"service":"torbox","token":"tb-secret"}],"indexers":["torrentio","torz"]}"#);
    let h = new_handler(test_deps(|_| {}));
    let rr = get(&h, &format!("/{cfg}/stream/movie/tt1234567.json"), &[]).await;
    let (_, env) = envelope_of(&body_str(&rr));
    assert!(
        env.coverage.complete && env.answer_kind == ANSWER_LIVE,
        "envelope = {env:?}, want live and complete"
    );
    let torz = env.coverage.sources.iter().find(|s| s.id == "torz");
    assert!(
        torz.is_some_and(|s| s.outcome == OUTCOME_QUARANTINED),
        "torz in coverage: {:?}",
        env.coverage.sources
    );
}
