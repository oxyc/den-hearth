//! Rust-only tests for the TorBox store, from the line-by-line review against `stores.go`.
//!
//! Go cancels with a context and the port cancels by dropping the future, so a call that is abandoned part way
//! must leave the state Go leaves when its context ends. None of this has a Go test, because Go's tests
//! cancel with a context and the code that runs after it ends is the code under test.

use std::future::pending;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;

use crate::cache::{Cache, MemoryCache};
use crate::scrape::{DoError, Doer};
use crate::stores::torbox::{TorBoxStore, TORBOX_API};
use crate::util::DetachedFlight;

/// A `Doer` that answers the requests its closure has an answer for and never answers the rest, which is how a
/// test holds a call open until its caller is dropped.
struct Stalling<F>(F)
where
    F: Fn(&reqwest::Request) -> Option<reqwest::Response> + Send + Sync;

#[async_trait]
impl<F> Doer for Stalling<F>
where
    F: Fn(&reqwest::Request) -> Option<reqwest::Response> + Send + Sync,
{
    async fn do_request(&self, req: reqwest::Request) -> Result<reqwest::Response, DoError> {
        match (self.0)(&req) {
            Some(r) => Ok(r),
            None => pending::<Result<reqwest::Response, DoError>>().await,
        }
    }
}

fn store(token: &str, cache: &Arc<dyn Cache>, client: Arc<dyn Doer>) -> TorBoxStore {
    TorBoxStore { token: token.to_owned(), api: TORBOX_API.to_owned(), cache: Some(cache.clone()), client }
}

/// The count `scout_debrid_calls_total` reports for one endpoint and outcome.
fn calls(endpoint: &str, outcome: &str) -> i64 {
    let series = format!(r#"scout_debrid_calls_total{{endpoint="{endpoint}",outcome="{outcome}"}} "#);
    crate::metrics::metrics()
        .render(-1)
        .lines()
        .find_map(|line| line.strip_prefix(series.as_str()))
        .and_then(|n| n.trim().parse().ok())
        .unwrap_or_else(|| panic!("no series {series}"))
}

static FLIGHT: DetachedFlight<u32> = DetachedFlight::new("test flight");
static LONE_FLIGHT: DetachedFlight<u32> = DetachedFlight::new("test lone flight");

// singleflight's `Result.Shared` is `c.dups > 0`, read when the answer goes out, so EVERY caller of a run that was
// handed to more than one caller sees it, the caller that started the work included. The leader used to be told
// `false`, so the avoided-call counters (`status_snapshot`, `checkcached_singleflight`) counted one saved read
// where Go counts two.
#[tokio::test]
async fn a_shared_flight_reports_shared_to_the_caller_that_started_it_too() {
    let release = Arc::new(tokio::sync::Notify::new());
    let (leader_release, joiner_release) = (release.clone(), release.clone());
    let leader = FLIGHT.run("k", move || async move {
        leader_release.notified().await;
        7
    });
    let joiner = FLIGHT.run("k", move || async move {
        joiner_release.notified().await;
        8
    });
    let (leader, joiner, ()) = tokio::join!(leader, joiner, async {
        // Both callers have been polled by the time this runs, so the second is a joiner.
        for _ in 0..8 {
            tokio::task::yield_now().await;
        }
        release.notify_one();
    });
    assert_eq!(leader, (Some(7), true), "the caller that started the work");
    assert_eq!(joiner, (Some(7), true), "the caller that joined it");

    // Alone, nothing was shared.
    let alone = LONE_FLIGHT.run("k", || async { 9 }).await;
    assert_eq!(alone, (Some(9), false), "a caller nobody joined");
}

// Go counts a TorBox call when `do` returns, and a context that ends makes `do` return an error, so a call its
// caller walked away from is an `error` in scout_debrid_calls_total. Dropped here it was not counted at all.
#[tokio::test(start_paused = true)]
async fn a_call_dropped_before_it_is_answered_is_counted_as_an_error() {
    let cache: Arc<dyn Cache> = Arc::new(MemoryCache::new(1 << 20));
    let s = store("tb-review-call-count", &cache, Arc::new(Stalling(|_| None)));
    let before = calls("user", "error");
    let dropped = tokio::time::timeout(Duration::from_secs(1), s.account_summary()).await;
    assert!(dropped.is_err(), "user/me never answers");
    assert!(calls("user", "error") > before, "the abandoned user/me call was not counted");
}
