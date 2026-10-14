//! Go: `internal/scout/answercache_test.go` (191 lines). Work package 5.
//!
//! Every test here needs `MemoryCache` (cache.rs, package 6) and the optional `answerKey` on the scraper
//! (a FROZEN type change), so each keeps its full Go body in a `cfg(any())` item until those land.

// The scraper fixtures, compiled out with the bodies that use them.
#[cfg(any())]
mod pending_fixtures {
    use std::sync::atomic::AtomicI32;
    use std::sync::Arc;

    use async_trait::async_trait;

    use crate::config::Indexer;
    use crate::rank::RawStream;
    use crate::scrape::{DoError, Scraper, ScrapeQuery};

    // Go `askCountingScraper`: counts its asks and can be made to fail.
    pub(super) struct AskCountingScraper {
        pub(super) base: String,
        pub(super) calls: Arc<AtomicI32>,
        pub(super) fail: bool,
    }

    // Go `heldAnswerScraper`: holds its answer until released, so callers can overlap.
    pub(super) struct HeldAnswerScraper {
        pub(super) base: String,
        pub(super) calls: Arc<AtomicI32>,
        pub(super) started: tokio::sync::mpsc::UnboundedSender<()>,
        pub(super) release: Arc<tokio::sync::Notify>,
    }

    #[async_trait]
    impl Scraper for AskCountingScraper {
        fn id(&self) -> Indexer {
            Indexer("torrentio".into())
        }
        async fn scrape(&self, _q: &ScrapeQuery) -> Result<Vec<RawStream>, DoError> {
            self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            if self.fail {
                return Err("indexer down".into());
            }
            Ok(vec![RawStream { info_hash: "a".repeat(40), title: "Movie 1080p WEB-DL".into(), ..Default::default() }])
        }
    }

    #[async_trait]
    impl Scraper for HeldAnswerScraper {
        fn id(&self) -> Indexer {
            Indexer("torrentio".into())
        }
        async fn scrape(&self, _q: &ScrapeQuery) -> Result<Vec<RawStream>, DoError> {
            self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            let _ = self.started.send(());
            self.release.notified().await;
            Ok(vec![RawStream { info_hash: "b".repeat(40), title: "Shared 1080p WEB-DL".into(), ..Default::default() }])
        }
    }
}

// An indexer is asked once per title while its answer is fresh, whichever list is built from it; a failure is
// never kept, so the next build asks again.
#[test]
#[ignore = "needs MemoryCache (cache.rs, package 6) and Scraper::answer_key (FROZEN change)"]
fn scrape_all_cached_keeps_answers_not_failures() {
    #[cfg(any())]
    async fn pending() {
        use crate::tests::answercache::pending_fixtures::AskCountingScraper;
        use crate::cache::MemoryCache;
        use crate::scrape::{scrape_all_cached, ScrapeQuery};
        use std::sync::atomic::{AtomicI32, Ordering};
        use std::sync::Arc;
        use std::time::Duration;

        let cache = MemoryCache::new(1 << 20);
        let movie = ScrapeQuery { kind: "movie".into(), imdb: "tt1".into(), ..Default::default() };

        let calls = Arc::new(AtomicI32::new(0));
        let up = AskCountingScraper { base: "https://torrentio.example".into(), calls: Arc::clone(&calls), fail: false };
        for _ in 0..2 {
            let (out, ok, complete) =
                scrape_all_cached(&[Arc::new(up.clone())], &movie, Duration::from_secs(1), Some(&cache), Duration::from_secs(60)).await;
            assert!(out.len() == 1 && ok && complete, "got {} releases, ok={ok} complete={complete}", out.len());
        }
        assert_eq!(calls.load(Ordering::SeqCst), 1, "asked for one title more than once");

        let episode = ScrapeQuery { kind: "series".into(), imdb: "tt1".into(), season: 1, episode: 2, has_ep: true, ..Default::default() };
        scrape_all_cached(&[Arc::new(up.clone())], &episode, Duration::from_secs(1), Some(&cache), Duration::from_secs(60)).await;
        assert_eq!(calls.load(Ordering::SeqCst), 2, "asked after another title: want 2");

        let failures = Arc::new(AtomicI32::new(0));
        let down = AskCountingScraper { base: "https://down.example".into(), calls: Arc::clone(&failures), fail: true };
        for _ in 0..2 {
            let (_, ok, _) =
                scrape_all_cached(&[Arc::new(down.clone())], &movie, Duration::from_secs(1), Some(&cache), Duration::from_secs(60)).await;
            assert!(!ok, "a failed indexer reported an answer");
        }
        assert_eq!(failures.load(Ordering::SeqCst), 2, "a failing indexer must not have its failure kept");
    }
    todo!("pending: answercache_test.go TestScrapeAllCached_keepsAnswersNotFailures")
}

// Concurrent callers asking the same provider the same question share one request in flight.
#[test]
#[ignore = "needs MemoryCache (cache.rs, package 6) and Scraper::answer_key (FROZEN change)"]
fn scrape_all_cached_coalesces_a_provider_question_in_flight() {
    #[cfg(any())]
    async fn pending() {
        use crate::tests::answercache::pending_fixtures::HeldAnswerScraper;
        use crate::scrape::{scrape_all_cached, ScrapeQuery};
        use std::sync::atomic::{AtomicI32, Ordering};
        use std::sync::Arc;
        use std::time::Duration;

        let query = ScrapeQuery { kind: "movie".into(), imdb: "tt-flight".into(), ..Default::default() };
        let release = Arc::new(tokio::sync::Notify::new());
        let (started_tx, mut started_rx) = tokio::sync::mpsc::unbounded_channel();
        let calls = Arc::new(AtomicI32::new(0));
        let up = Arc::new(HeldAnswerScraper {
            base: "https://configured.example/account-a".into(),
            calls: Arc::clone(&calls),
            started: started_tx,
            release: Arc::clone(&release),
        });

        let mut handles = Vec::new();
        for _ in 0..6 {
            let up = Arc::clone(&up);
            let query = query.clone();
            handles.push(tokio::spawn(async move {
                scrape_all_cached(&[up], &query, Duration::from_secs(1), None, Duration::from_secs(60)).await.0
            }));
        }
        started_rx.recv().await;
        // With no settled-answer cache, only callers that have joined this in-flight question can avoid a request.
        tokio::time::sleep(Duration::from_millis(10)).await;
        release.notify_waiters();

        let mut got = Vec::new();
        for h in handles {
            let answer = h.await.expect("join");
            assert_eq!(answer.len(), 1, "shared answer has {} releases, want 1", answer.len());
            got.push(answer);
        }
        assert_eq!(calls.load(Ordering::SeqCst), 1, "same provider and title started more than one indexer request");
        got[0][0].title.clone_from(&"caller-local annotation".to_owned());
        assert_ne!(got[1][0].title, got[0][0].title, "shared callers received the same mutable RawStream slice");
    }
    todo!("pending: answercache_test.go TestScrapeAllCached_coalescesAProviderQuestionInFlight")
}

// Two configured identities of the same host are two questions, not one.
#[test]
#[ignore = "needs MemoryCache (cache.rs, package 6) and Scraper::answer_key (FROZEN change)"]
fn scrape_all_cached_keeps_provider_identities_apart() {
    #[cfg(any())]
    async fn pending() {
        use crate::tests::answercache::pending_fixtures::HeldAnswerScraper;
        use crate::cache::MemoryCache;
        use crate::scrape::{scrape_all_cached, ScrapeQuery};
        use std::sync::atomic::{AtomicI32, Ordering};
        use std::sync::Arc;
        use std::time::Duration;

        let query = ScrapeQuery { kind: "movie".into(), imdb: "tt-identities".into(), ..Default::default() };
        let release = Arc::new(tokio::sync::Notify::new());
        let (started_tx, mut started_rx) = tokio::sync::mpsc::unbounded_channel();
        let calls = Arc::new(AtomicI32::new(0));
        let one = Arc::new(HeldAnswerScraper {
            base: "https://configured.example/account-a".into(),
            calls: Arc::clone(&calls),
            started: started_tx.clone(),
            release: Arc::clone(&release),
        });
        let two = Arc::new(HeldAnswerScraper {
            base: "https://configured.example/account-b".into(),
            calls: Arc::clone(&calls),
            started: started_tx,
            release: Arc::clone(&release),
        });

        let mut handles = Vec::new();
        for up in [one, two] {
            let query = query.clone();
            handles.push(tokio::spawn(async move {
                let cache = MemoryCache::new(1 << 20);
                scrape_all_cached(&[up], &query, Duration::from_secs(1), Some(&cache), Duration::from_secs(60)).await;
            }));
        }
        started_rx.recv().await;
        started_rx.recv().await;
        assert_eq!(calls.load(Ordering::SeqCst), 2, "two configured identities must start two requests");
        release.notify_waiters();
        for h in handles {
            h.await.expect("join");
        }
    }
    todo!("pending: answercache_test.go TestScrapeAllCached_keepsProviderIdentitiesApart")
}

// A caller that leaves does not cancel the question other callers are still waiting on.
#[test]
#[ignore = "needs MemoryCache (cache.rs, package 6) and Scraper::answer_key (FROZEN change)"]
fn scrape_all_cached_one_caller_leaving_does_not_cancel_the_shared_question() {
    #[cfg(any())]
    async fn pending() {
        use crate::tests::answercache::pending_fixtures::HeldAnswerScraper;
        use crate::cache::MemoryCache;
        use crate::scrape::{scrape_all_cached, ScrapeQuery};
        use std::sync::atomic::{AtomicI32, Ordering};
        use std::sync::Arc;
        use std::time::Duration;

        let cache = Arc::new(MemoryCache::new(1 << 20));
        let query = ScrapeQuery { kind: "movie".into(), imdb: "tt-leaving".into(), ..Default::default() };
        let release = Arc::new(tokio::sync::Notify::new());
        let (started_tx, mut started_rx) = tokio::sync::mpsc::unbounded_channel();
        let calls = Arc::new(AtomicI32::new(0));
        let up = Arc::new(HeldAnswerScraper {
            base: "https://configured.example/account-a".into(),
            calls: Arc::clone(&calls),
            started: started_tx,
            release: Arc::clone(&release),
        });

        let leaving = {
            let (up, query, cache) = (Arc::clone(&up), query.clone(), Arc::clone(&cache));
            tokio::spawn(async move {
                tokio::time::timeout(
                    Duration::from_millis(50),
                    scrape_all_cached(&[up], &query, Duration::from_secs(1), Some(&*cache), Duration::from_secs(60)),
                )
                .await
                .ok()
            })
        };
        started_rx.recv().await;
        let left = leaving.await.expect("join");
        assert!(left.is_none(), "the caller that left got a result, want none");

        let stayed = {
            let (up, query, cache) = (Arc::clone(&up), query.clone(), Arc::clone(&cache));
            tokio::spawn(async move {
                scrape_all_cached(&[up], &query, Duration::from_secs(1), Some(&*cache), Duration::from_secs(60)).await.0
            })
        };
        release.notify_waiters();
        let got = stayed.await.expect("join");
        assert_eq!(got.len(), 1, "the caller that stayed got {} releases, want 1", got.len());
        assert_eq!(calls.load(Ordering::SeqCst), 1, "leaving restarted the shared provider request");
    }
    todo!("pending: answercache_test.go TestScrapeAllCached_oneCallerLeavingDoesNotCancelTheSharedQuestion")
}

// A caller that arrives already canceled starts no work at all.
#[test]
#[ignore = "needs MemoryCache-free scrape_all_cached plus Scraper::answer_key (FROZEN change)"]
fn scrape_all_cached_does_not_start_detached_work_for_a_canceled_caller() {
    #[cfg(any())]
    async fn pending() {
        use crate::tests::answercache::pending_fixtures::AskCountingScraper;
        use crate::scrape::{scrape_all_cached, ScrapeQuery};
        use std::sync::atomic::{AtomicI32, Ordering};
        use std::sync::Arc;
        use std::time::Duration;

        let calls = Arc::new(AtomicI32::new(0));
        let up = AskCountingScraper { base: "https://configured.example/canceled".into(), calls: Arc::clone(&calls), fail: false };
        let canceled = tokio::time::timeout(Duration::ZERO, std::future::pending::<()>()).await;
        assert!(canceled.is_err());
        let query = ScrapeQuery { kind: "movie".into(), imdb: "tt-canceled".into(), ..Default::default() };
        let (out, ok, complete) = scrape_all_cached(&[Arc::new(up)], &query, Duration::from_secs(1), None, Duration::from_secs(60)).await;
        assert!(out.is_empty() && !ok && !complete && calls.load(Ordering::SeqCst) == 0, "canceled scrape started work");
    }
    todo!("pending: answercache_test.go TestScrapeAllCached_doesNotStartDetachedWorkForACanceledCaller")
}
