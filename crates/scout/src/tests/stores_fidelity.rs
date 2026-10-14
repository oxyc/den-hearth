//! Rust-only tests for the TorBox store, from the line-by-line review against `stores.go`.
//!
//! Go cancels with a context and the port cancels by dropping the future, so a call that is abandoned part way
//! must leave the state Go leaves when its context ends. None of this has a Go test, because Go's tests
//! cancel with a context and the code that runs after it ends is the code under test.

use std::sync::Arc;

use crate::util::DetachedFlight;

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
