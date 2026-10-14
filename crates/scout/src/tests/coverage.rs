//! Go: `internal/scout/coverage_test.go` (219 lines). Work package 4.

use std::time::Duration;

use crate::coverage::{AnswerEnvelope, ANSWER_EMPTY, ANSWER_LIVE, ANSWER_PARTIAL, ANSWER_UNKNOWN, OUTCOME_QUARANTINED};
use crate::handler::new_handler;
use crate::tests::handler::{blob, body_str, fake_scraper, get, test_deps, test_seeds, valid_blob, FakeFuture};

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

/// Go `TestClassifyScrape`: every scrape error maps to one outcome. The cases that need the scraper error taxonomy
/// (`unaskableScraper`, `*httpStatusError`, `errNotPublic`) belong to package 5's scrape.go.
#[test]
#[ignore = "needs scrape::UnaskableScraper, scrape::HttpStatusError and sources::ERR_NOT_PUBLIC from package 5"]
fn classify_scrape() {
    // Enable when package 5 lands: move `pending` up and delete the wrapper.
    #[cfg(any())]
    fn pending() {
        use crate::coverage::{classify_scrape, OUTCOME_ANSWERED, OUTCOME_MISCONFIGURED, OUTCOME_REFUSED, OUTCOME_TIMEOUT, OUTCOME_UNREACHABLE};
        let asked = FakeScraper::named("torrentio");
        let cases: Vec<(&str, Box<dyn Scraper>, Option<DoError>, &str)> = vec![
            ("answered", asked.clone(), None, OUTCOME_ANSWERED),
            ("deadline", asked.clone(), Some(err("wrapped: context deadline exceeded")), OUTCOME_TIMEOUT),
            ("queued past the budget", asked.clone(), Some(err("torrentio: queued past the scrape budget: deadline")), OUTCOME_TIMEOUT),
            ("http 408", asked.clone(), Some(http_status_error("torrentio", 408)), OUTCOME_TIMEOUT),
            ("http 429 (or a paused upstream)", asked.clone(), Some(http_status_error("torrentio", 429)), OUTCOME_REFUSED),
            ("http 403", asked.clone(), Some(http_status_error("torrentio", 403)), OUTCOME_REFUSED),
            ("http 502", asked.clone(), Some(http_status_error("torrentio", 502)), OUTCOME_UNREACHABLE),
            ("non-public household source", asked.clone(), Some(err("dial: refusing a non-public address")), OUTCOME_REFUSED),
            ("network failure", asked.clone(), Some(err("connection reset")), OUTCOME_UNREACHABLE),
            ("no config URL", unaskable("mediafusion", false), Some(err("not asked")), OUTCOME_MISCONFIGURED),
            ("config mint failed", unaskable("mediafusion", true), Some(err("not asked")), OUTCOME_UNREACHABLE),
        ];
        for (name, sc, e, want) in cases {
            assert_eq!(classify_scrape(sc.as_ref(), e.as_ref()), want, "{name}");
        }
    }
    todo!("pending: coverage_test.go:TestClassifyScrape")
}

/// Go `TestScrapeCovered`: every scraper appears in the coverage with what it did, and completeness follows from
/// those outcomes. Needs `scrapeCovered` (scrape.go, package 5).
#[test]
#[ignore = "needs scrape::scrape_covered from package 5"]
fn scrape_covered() {
    // Enable when package 5 lands: move `pending` up and delete the wrapper.
    #[cfg(any())]
    async fn pending() {
        let release = test_seeds()[..1].to_vec();
        let found = FakeScraper::answering("torrentio", release);
        let none = FakeScraper::answering("comet", Vec::new());
        let hang = FakeScraper::hanging("mediafusion");
        let unconfigured = unaskable("mediafusion", false);
        let cases = [
            ("all answered, none found", vec![none.clone(), unconfigured.clone()], true, vec![OUTCOME_ANSWERED, OUTCOME_MISCONFIGURED], ANSWER_EMPTY),
            ("one timed out, none found", vec![none.clone(), hang.clone()], false, vec![OUTCOME_ANSWERED, OUTCOME_TIMEOUT], ANSWER_UNKNOWN),
            ("one timed out, releases found", vec![found.clone(), hang.clone()], false, vec![OUTCOME_ANSWERED, OUTCOME_TIMEOUT], ANSWER_PARTIAL),
            ("all answered, releases found", vec![found.clone(), none.clone()], true, vec![OUTCOME_ANSWERED, OUTCOME_ANSWERED], ANSWER_LIVE),
        ];
        for (name, scrapers, complete, outcomes, kind) in cases {
            let res = scrape_covered(&scrapers, &ScrapeQuery::default(), Duration::from_millis(30), None, Duration::ZERO).await;
            assert_eq!(res.complete, complete, "{name}: complete");
            assert_eq!(res.sources.len(), scrapers.len(), "{name}: every source in coverage");
            for (i, want) in outcomes.iter().enumerate() {
                let s = &res.sources[i];
                assert!(s.id == scrapers[i].id().0 && s.outcome == *want, "{name}: source {i} = {} {}", s.id, s.outcome);
                if *want == OUTCOME_ANSWERED {
                    assert!(s.observed_at.is_some(), "{name}: source {i} answered with no time");
                }
            }
            let degraded = if res.any_ok { "" } else { "indexers" };
            assert_eq!(decide_answer_kind(res.seeds.len() as i64, res.complete, degraded), kind, "{name}: answerKind");
        }
    }
    todo!("pending: coverage_test.go:TestScrapeCovered")
}

/// Go `TestScrapeCovered_reusedAnswerIsMarked`: an answer reused from the indexer answer cache is an answer,
/// marked as reused. Needs `keepAnswer` and `stremioScraper` (scrape.go, package 5).
#[test]
#[ignore = "needs scrape::keep_answer and scrape::StremioScraper from package 5"]
fn scrape_covered_reused_answer_is_marked() {
    // Enable when package 5 lands: move `pending` up and delete the wrapper.
    #[cfg(any())]
    async fn pending() {
        let cache = MemoryCache::new(1 << 20);
        let sc = StremioScraper::new("torrentio", "https://torrentio.example");
        keep_answer(&cache, Duration::from_secs(60), &sc, &ScrapeQuery { kind: "movie".into(), imdb: "tt1".into(), ..Default::default() }, test_seeds());
        let res = scrape_covered(&[Box::new(sc)], &ScrapeQuery { kind: "movie".into(), imdb: "tt1".into(), ..Default::default() }, Duration::from_secs(1), Some(&cache), Duration::from_secs(60)).await;
        let s = &res.sources[0];
        assert!(s.outcome == OUTCOME_ANSWERED && s.cached && s.items == 3 && res.complete, "reused answer: {s:?}, complete {}", res.complete);
    }
    todo!("pending: coverage_test.go:TestScrapeCovered_reusedAnswerIsMarked")
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
        fake_scraper("comet", || Box::pin(std::future::pending::<Result<Vec<crate::rank::RawStream>, crate::scrape::DoError>>()) as FakeFuture)
    }
    type Make = fn() -> Vec<Box<dyn crate::scrape::Scraper>>;
    let cases: [(&str, Make, &str, &str, bool, Vec<&str>, bool); 4] = [
        ("all answered, none found", || vec![answers_empty()], ANSWER_EMPTY, "", true, vec!["answered"], false),
        ("one timed out, none found", || vec![answers_empty(), times_out()], ANSWER_UNKNOWN, "indexers", false, vec!["answered", "timeout"], false),
        ("one timed out, releases found", || vec![answers_seeds(), times_out()], ANSWER_PARTIAL, "", false, vec!["answered", "timeout"], true),
        ("all answered, releases found", || vec![answers_seeds()], ANSWER_LIVE, "", true, vec!["answered"], true),
    ];
    for (name, make, kind, degraded, complete, outcomes, streams) in cases {
        let h = new_handler(test_deps(move |d| {
            d.scrape_timeout = Duration::from_millis(30);
            d.make_scrapers = std::sync::Arc::new(move |_| make());
        }));
        let rr = get(&h, &format!("/{}/stream/movie/tt1234567.json", valid_blob()), &[]).await;
        let (list, env) = envelope_of(&body_str(&rr));
        assert!(env.v == 1 && env.answer_kind == kind && env.coverage.complete == complete, "{name}: envelope = {env:?}");
        let header = crate::tests::handler::hdr(&rr, "x-den-degraded");
        assert!(header == degraded && env.degraded == header, "{name}: X-Den-Degraded {header:?}, envelope {:?}", env.degraded);
        assert!(env.generated_at.is_some() && (degraded.is_empty()) == env.expires_at.is_none(), "{name}: generatedAt/expiresAt");
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
    let cfg = blob(r#"{"debrid":[{"service":"torbox","token":"tb-secret"}],"indexers":["torrentio","torz"]}"#);
    let h = new_handler(test_deps(|_| {}));
    let rr = get(&h, &format!("/{cfg}/stream/movie/tt1234567.json"), &[]).await;
    let (_, env) = envelope_of(&body_str(&rr));
    assert!(env.coverage.complete && env.answer_kind == ANSWER_LIVE, "envelope = {env:?}, want live and complete");
    let torz = env.coverage.sources.iter().find(|s| s.id == "torz");
    assert!(torz.is_some_and(|s| s.outcome == OUTCOME_QUARANTINED), "torz in coverage: {:?}", env.coverage.sources);
}
