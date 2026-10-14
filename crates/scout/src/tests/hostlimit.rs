//! Go: `internal/scout/hostlimit_test.go` (258 lines). Work package 6.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::hostlimit::{new_host_limiter, wait, Bucket};

// A burst passes straight through. A household glancing down a few episodes must never wait: the limiter
// exists to bound sustained traffic, not to tax normal use.
#[tokio::test]
async fn host_limiter_burst_is_free() {
    let l = new_host_limiter(Duration::from_millis(50), 4);
    let start = Instant::now();
    for _ in 0..4 {
        wait(Some(&l), "indexer.example").await;
    }
    let elapsed = start.elapsed();
    assert!(elapsed <= Duration::from_millis(20), "a burst within the allowance waited {elapsed:?}");
}

// Past the burst, requests QUEUE rather than being shed. That distinction is the whole point: a shed
// request became "this release does not exist" for an entire evening, and a request that waits is
// invisible next to an eight-second scrape budget.
#[tokio::test]
async fn host_limiter_paces_beyond_the_burst() {
    let l = new_host_limiter(Duration::from_millis(30), 2);
    let start = Instant::now();
    for _ in 0..5 {
        wait(Some(&l), "indexer.example").await;
    }
    // 2 free, then 3 paced ⇒ at least ~90ms. Generous upper bound: CI timers are not precise.
    let elapsed = start.elapsed();
    assert!(elapsed >= Duration::from_millis(60), "no pacing applied: {elapsed:?}");
    assert!(elapsed <= Duration::from_secs(1), "pacing far slower than configured: {elapsed:?}");
}

// Per host, so a slow or throttled indexer cannot delay a healthy one.
#[tokio::test]
async fn host_limiter_is_per_host() {
    let l = new_host_limiter(Duration::from_secs(1), 1);
    wait(Some(&l), "slow.example").await; // spends slow's only token
    let start = Instant::now();
    wait(Some(&l), "fast.example").await; // must not be held behind it
    let elapsed = start.elapsed();
    assert!(elapsed <= Duration::from_millis(20), "one host's budget delayed another: {elapsed:?}");
}

// A cancelled context ends the wait. The caller's deadline governs; the limiter never outlives it, or a
// queued request would go on holding a scrape budget that has already expired.
#[tokio::test]
async fn host_limiter_respects_cancellation() {
    let l = new_host_limiter(Duration::from_secs(10), 1);
    wait(Some(&l), "h").await; // spend the token
    let start = Instant::now();
    // The caller's deadline is the dropped future.
    let _ = tokio::time::timeout(Duration::from_millis(20), wait(Some(&l), "h")).await;
    let elapsed = start.elapsed();
    assert!(elapsed <= Duration::from_secs(1), "waited past the caller's deadline: {elapsed:?}");
}

// Concurrent callers queue in order rather than all waking to the same free slot — the token is spent
// even when the bucket goes negative, which is what makes the wait fair.
//
// Asserted on the SPREAD of delays, not on the wall clock. A wall-clock bound measures only the slowest
// caller, so it passed just as happily when every overflow caller was handed one identical delay — which
// is a synchronised herd, the precise opposite of a queue, and was a real shipped behaviour.
#[test]
fn host_limiter_concurrent_callers_queue() {
    let mut b = Bucket { tokens: 2.0, last: Instant::now() };
    let delays: Vec<Duration> = (0..6).map(|_| b.take(Duration::from_millis(20), 2)).collect();
    assert!(delays[0] == Duration::ZERO && delays[1] == Duration::ZERO, "the burst should pass free: {:?}", &delays[..2]);
    // Each overflow caller waits one interval longer than the one before it, so they leave in order
    // instead of arriving at the host together.
    for i in 2..delays.len() {
        let want = Duration::from_millis(20) * (i as u32 - 1);
        let d = delays[i];
        let lo = want.saturating_sub(Duration::from_millis(1));
        let hi = want + Duration::from_millis(1);
        assert!(d >= lo && d <= hi, "caller {i} waited {d:?}, want ~{want:?} (delays must fan out, not collapse)");
    }
}

// A cancel-heavy caller must not be able to defeat the limiter.
//
// This is the regression that a debt floor plus an unconditional refund produced together: a caller
// clamped by the floor was charged nothing, but cancelling still gave a token back, so every abandoned
// wait MINTED one. Measured on the shipped settings, ~4000 requests passed free in two seconds against a
// budget of twelve. Each half looked reasonable in review; only together were they a hole.
#[tokio::test(flavor = "multi_thread")]
async fn host_limiter_cancellation_cannot_mint_tokens() {
    // Repeated WAVES, not one burst: minted tokens are only visible once a later caller spends them, so
    // a single wave of cancels proves nothing and would pass against the very bug this exists to catch.
    let l = Arc::new(new_host_limiter(Duration::from_millis(50), 4));
    let free = Arc::new(AtomicUsize::new(0));
    for _ in 0..20 {
        let mut tasks = Vec::new();
        for _ in 0..20 {
            let (l, free) = (Arc::clone(&l), Arc::clone(&free));
            tasks.push(tokio::spawn(async move {
                let start = Instant::now();
                let _ = tokio::time::timeout(Duration::from_millis(1), wait(Some(&l), "h")).await;
                if start.elapsed() < Duration::from_micros(500) {
                    free.fetch_add(1, Ordering::SeqCst);
                }
            }));
        }
        for t in tasks {
            t.await.expect("caller task");
        }
    }
    // 400 callers over ~20 waves. Only the burst of 4, plus whatever elapsed time genuinely earned,
    // may pass without waiting; the bug this catches let hundreds through.
    let free = free.load(Ordering::SeqCst);
    assert!(free <= 25, "{free} of 400 callers passed free under a burst of 4: cancellation is minting tokens");
}

// The refill arithmetic itself, asserted exactly rather than through the clock.
//
// Every timing test here runs in one burst with no waiting, so deleting the refill line entirely leaves
// them all passing — a limiter that never refills is only ever SLOWER, and the bounds are one-sided.
// This drives the bucket directly instead.
#[test]
fn bucket_refills_by_elapsed_time() {
    let mut b = Bucket { tokens: 0.0, last: Instant::now() - Duration::from_millis(500) };
    // 500ms elapsed at 100ms per token = 5 tokens, so the next take is free and 4 remain.
    let delay = b.take(Duration::from_millis(100), 10);
    assert_eq!(delay, Duration::ZERO, "half a second should have refilled five tokens");
    assert!(b.tokens >= 3.9 && b.tokens <= 4.1, "refill arithmetic: {:.2} tokens, want ~4", b.tokens);

    // The cap is applied before the spend, so an idle bucket cannot bank more than one burst.
    let mut idle = Bucket { tokens: 0.0, last: Instant::now() - Duration::from_secs(3600) };
    idle.take(Duration::from_millis(1), 3);
    assert!(idle.tokens <= 2.001, "an idle bucket banked past its burst: {:.2}", idle.tokens);
}

// The configured rate, asserted as a rate. The timing tests accept a wide band; this pins the maths.
#[test]
fn bucket_delay_matches_the_configured_rate() {
    let mut b = Bucket { tokens: 0.0, last: Instant::now() };
    b.take(Duration::from_secs(1), 8); // spends to -1
    let delay = b.take(Duration::from_secs(1), 8);
    // Now at -2, so the caller waits two intervals.
    assert!(
        delay >= Duration::from_millis(1900) && delay <= Duration::from_millis(2100),
        "delay {delay:?} does not match a 1s interval at two tokens of debt"
    );
}

// Debt accumulates exactly, one token per caller. It is deliberately NOT floored: a caller that would
// queue past its own deadline is released by its context, not by forgiving the debt, because forgiven
// debt is indistinguishable from a token that was never spent and can be refunded into existence.
#[test]
fn bucket_debt_is_exact() {
    let mut b = Bucket { tokens: 0.0, last: Instant::now() };
    for _ in 0..100 {
        b.take(Duration::from_secs(3600), 4); // an hour per token, so elapsed-time refill can't blur the count
    }
    assert!(b.tokens <= -99.9 && b.tokens >= -100.1, "100 callers should owe 100 tokens, bucket holds {:.2}", b.tokens);
}

// A refund can only ever undo a spend. Without the cap it is a way to create tokens: refunds outnumber
// spends whenever a caller is released by its context, and an uncapped counter banks the difference.
#[test]
fn bucket_refund_cannot_bank_credit() {
    let mut b = Bucket { tokens: 4.0, last: Instant::now() };
    for _ in 0..10 {
        b.refund(4);
    }
    assert!(b.tokens <= 4.001, "refunds banked credit past the burst: {:.2}", b.tokens);

    // It still refunds where a token really was spent.
    let mut b = Bucket { tokens: 0.0, last: Instant::now() };
    b.take(Duration::from_secs(3600), 4);
    b.refund(4);
    assert!(b.tokens >= -0.001 && b.tokens <= 0.001, "a spend followed by its refund should net zero: {:.2}", b.tokens);
}

// The clamps in newHostLimiter, exercised. A zero interval makes the refill +Inf and every delay zero —
// a limiter that reads as configured and silently isn't — and burst 0 would pace even the first request.
#[tokio::test]
async fn new_host_limiter_clamps_nonsense_settings() {
    let l = new_host_limiter(Duration::ZERO, 0);
    assert!(l.every > Duration::ZERO, "a non-positive interval was not clamped: {:?}", l.every);
    assert!(l.burst >= 1, "burst was not clamped to at least one: {}", l.burst);
    // And the clamped limiter genuinely paces: one free, the next waits. Go's second case is a negative
    // interval (-time.Second); a Rust Duration cannot be negative, so it is the zero case again.
    let l = new_host_limiter(Duration::ZERO, -5);
    wait(Some(&l), "h").await;
    let d = l.buckets.lock().unwrap()["h"].lock().unwrap().take(l.every, l.burst);
    assert!(d > Duration::ZERO, "a clamped limiter does not pace at all");
}

// An abandoned wait returns its token. A request that was never sent must not charge the next caller —
// the debt decays at a rate comparable to the scrape budget, so it carries across requests.
#[tokio::test]
async fn host_limiter_cancellation_refunds_the_token() {
    let l = new_host_limiter(Duration::from_secs(10), 1);
    wait(Some(&l), "h").await; // spend the only token
    // The caller's deadline is the dropped future.
    let _ = tokio::time::timeout(Duration::from_millis(20), wait(Some(&l), "h")).await; // abandoned — must give the token back

    let b = Arc::clone(&l.buckets.lock().unwrap()["h"]);
    let tokens = b.lock().unwrap().tokens;
    assert!(tokens >= -0.001, "an abandoned wait kept its token: {tokens:.2}");
}

// A nil limiter and an empty host are no-ops, so a test double or an unparsed URL never blocks.
#[tokio::test]
async fn host_limiter_degrades_to_no_op() {
    wait(None, "h").await;
    wait(Some(&new_host_limiter(Duration::from_secs(3600), 0)), "").await;
}

// The burst has to cover a household's largest legitimate burst, which is opening a season: one request
// per episode, all at once.
//
// At a burst of 10 a 24-episode show queued the tail fourteen seconds out, against an eight-second
// scrape budget — so those episodes were never asked, counted as indexers that did not answer, and came
// back as a degraded empty list from a perfectly healthy indexer. The sustained rate is what protects
// the indexer; the burst only decides whether normal use waits at all.
#[test]
fn indexer_limiter_burst_covers_a_season_open() {
    use crate::scrape::indexer_limiter;
    const SEASON: i64 = 24;
    let lim = indexer_limiter();
    let mut b = Bucket { tokens: lim.burst as f64, last: Instant::now() };
    for i in 0..SEASON {
        let delay = b.take(lim.every, lim.burst);
        assert!(delay == Duration::ZERO, "episode {} of a season open waited {delay:?}; the tail is never asked within the budget", i + 1);
    }
}
