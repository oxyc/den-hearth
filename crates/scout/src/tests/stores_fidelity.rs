//! Rust-only tests for the TorBox store, from the line-by-line review against `stores.go`.
//!
//! Go cancels with a context and the port cancels by dropping the future, so a call that is abandoned part way
//! must leave the state Go leaves when its context ends. None of this has a Go test, because Go's tests
//! cancel with a context and the code that runs after it ends is the code under test.

use std::future::pending;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;

use crate::addbudget::{budget_account, global_add_budget};
use crate::cache::{Cache, MemoryCache};
use crate::config::DebridService;
use crate::scrape::{DoError, Doer};
use crate::stores::refusal::{add_outcome_unknown, backed_off};
use crate::stores::torbox::{TorBoxStore, TORBOX_API};
use crate::stores::{ResolveTarget, Store};
use crate::tests::addbudget::{install_budget, HOUR};
use crate::tests::mock::resp;
use crate::util::DetachedFlight;

fn base(req: &reqwest::Request) -> String {
    req.url().path().rsplit('/').next().unwrap_or("").to_owned()
}

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

// resolveHeldTorrent writes the torrent id createtorrent just returned whatever listFiles said, and a listFiles
// whose context has ended says "no answer" — so a client that backs out while an episode's file list is being read
// still leaves the id behind. Dropped here, the id was lost, the next poll knew of no torrent and bought another
// one: an add per poll against the hourly allowance, the loop that write exists to stop.
#[tokio::test(start_paused = true)]
async fn a_resolve_dropped_while_listing_files_keeps_the_id_it_was_just_handed() {
    let _budget = install_budget(HOUR, 50);
    let hash = "b".repeat(40);
    let cache: Arc<dyn Cache> = Arc::new(MemoryCache::new(1 << 20));
    let s = store(
        "tb-review-id-write",
        &cache,
        Arc::new(Stalling(|req| match base(req).as_str() {
            "createtorrent" => Some(resp(200, r#"{"success":true,"data":{"torrent_id":42}}"#)),
            // The file list never comes back.
            "mylist" => None,
            _ => Some(resp(404, "{}")),
        })),
    );
    let target = ResolveTarget { info_hash: hash.clone(), season: Some(1), episode: Some(1), ..Default::default() };
    let dropped = tokio::time::timeout(Duration::from_secs(1), s.resolve(&target)).await;
    assert!(dropped.is_err(), "the resolve should still have been waiting for the file list");
    assert_eq!(s.known_torrent_id(&hash), (42, true), "the id createtorrent returned must be remembered");
}

// createtorrent answered 400 "already queued", and the lookup that finds the queue entry is abandoned. Go's lookup
// fails with its context and the add falls through to the answered-failure code: the charge goes back (TorBox
// created nothing) and Resolve files the 400 as a refusal. Dropped here, the charge stayed spent and nothing was
// filed, so a polled release kept spending the hour's allowance on a 400.
#[tokio::test(start_paused = true)]
async fn an_add_dropped_during_the_already_queued_lookup_is_refunded_and_filed() {
    let _budget = install_budget(HOUR, 50);
    let hash = "c".repeat(40);
    let cache: Arc<dyn Cache> = Arc::new(MemoryCache::new(1 << 20));
    let s = store(
        "tb-review-diff-issue-drop",
        &cache,
        Arc::new(Stalling(|req| match base(req).as_str() {
            "createtorrent" => Some(resp(400, r#"{"error":"DIFF_ISSUE","detail":"Download already queued."}"#)),
            // The queue listing never comes back.
            "getqueued" => None,
            _ => Some(resp(404, "{}")),
        })),
    );
    let account = budget_account(DebridService::TorBox, &s.account_identity());
    let before = global_add_budget().remaining(&account);
    let dropped = tokio::time::timeout(Duration::from_secs(1), s.add_magnet(&hash, false, false)).await;
    assert!(dropped.is_err(), "the add should still have been waiting for the queue listing");
    assert_eq!(global_add_budget().remaining(&account), before, "the charge for an add TorBox refused must go back");
    assert!(!add_outcome_unknown(&*cache, DebridService::TorBox, &s.account_identity(), &hash), "the answer was read");
    assert_eq!(
        backed_off(&*cache, DebridService::TorBox, &s.account_identity(), &hash).as_deref(),
        Some("torbox createtorrent http 400 (DIFF_ISSUE: Download already queued.)"),
        "the refusal Resolve files for that answer"
    );
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
