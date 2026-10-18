//! Go: `internal/scout/playreserve_test.go` (248 lines).
//!
//! Exercises `addbudget` through the handler. The budget's clock is injected (`AddBudget::now`), the process-wide
//! budget is swapped with `install_budget` (a per-thread budget in a test build, as Go's tests swap
//! `globalAddBudget`), and the reserve is the budget's `reserve` atomic.

use std::sync::atomic::{AtomicI32, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, UNIX_EPOCH};

use crate::addbudget::{
    global_add_budget, is_play_reserve, set_play_reserve as set_reserve, spend_add, AddBudget,
    ADD_BUDGET_LIMIT, DEFAULT_PLAY_RESERVE,
};
use crate::cache::MemoryCache;
use crate::config::DebridService;
use crate::deps::{parse_play_reserve as parse_reserve, settings_from_env};
use crate::handler::new_handler;
use crate::metrics::metrics;
use crate::play::{encode_play_token, PlayTarget};
use crate::stores::refusal::{backed_off, record_refusal};
use crate::stores::torbox::{TorBoxStore, TORBOX_API};
use crate::stores::{Store, StoreError};
use crate::tests::addbudget::{install_budget, is_add_endpoint, HOUR};
use crate::tests::handler::{body_str, get, hdr, shared_stores, test_deps, valid_blob};
use crate::tests::mock::{resp, MockDoer};

const H: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";

/// Go `time.Date(2026, 9, 22, h, m, s, 0, time.UTC)`: 2026-09-22T00:00:00Z is 1_790_035_200.
fn sept22(h: u64, m: u64, s: u64) -> u64 {
    1_790_035_200 + h * 3600 + m * 60 + s
}

/// A budget whose clock is `now` seconds since the epoch, which the test moves.
fn clocked_budget(limit: i64, reserve: i64, now: &Arc<AtomicU64>) -> AddBudget {
    let mut b = AddBudget::new(HOUR, limit);
    b.reserve.store(reserve, Ordering::SeqCst);
    let now = Arc::clone(now);
    b.now = Box::new(move || UNIX_EPOCH + Duration::from_secs(now.load(Ordering::SeqCst)));
    b
}

/// A prefetch stops at the reserve; a viewer waiting spends down to zero; both come back as the window drains, each at
/// its own ceiling.
#[test]
fn add_budget_prefetch_stops_at_the_reserve_and_play_spends_to_zero() {
    let now = Arc::new(AtomicU64::new(sept22(20, 0, 0)));
    let b = clocked_budget(10, 3, &now);

    // Seven prefetches, a minute apart: the last one leaves exactly the reserve.
    for i in 0..7 {
        assert!(
            b.take("acct", true),
            "prefetch {} refused with {} left, above the reserve of 3",
            i + 1,
            b.remaining("acct")
        );
        now.fetch_add(60, Ordering::SeqCst);
    }
    assert!(!b.take("acct", true), "a prefetch spent into the reserve kept for Play");
    let got = b.remaining("acct");
    assert_eq!(got, 3, "remaining = {got} after the refused prefetch, want 3 — a refusal must spend nothing");
    // The first charge (20:00) drains at 21:00, which frees one prefetch.
    let got = b.frees_in("acct", true);
    assert_eq!(
        got,
        Duration::from_secs(53 * 60),
        "prefetch freesIn = {got:?}, want 53m (until the 20:00 charge drains at 21:00)"
    );
    let got = b.frees_in("acct", false);
    assert_eq!(got, Duration::ZERO, "a viewer was told to wait ({got:?}) with the reserve still there");

    // The viewer spends the reserve, down to zero, and no further.
    for _ in 0..3 {
        assert!(b.take("acct", false), "Play refused with {} left", b.remaining("acct"));
    }
    assert!(!b.take("acct", false), "Play spent past the whole allowance");
    assert!(!b.take("acct", true), "a prefetch was served from a spent allowance");

    // 21:00:30 — the 20:00 charge has drained: one slot, which is the viewer's, not the prefetch's.
    now.store(sept22(21, 0, 30), Ordering::SeqCst);
    assert!(
        !b.take("acct", true),
        "a prefetch took the one slot that freed while the account was below the reserve"
    );
    assert!(b.take("acct", false), "Play did not recover as the window drained");

    // 21:06:30 — every prefetch charge (20:00–20:06) has drained; three viewer charges from 20:07 and one
    // from 21:00:30 remain, so six are left and three of them are the prefetch's.
    now.store(sept22(21, 6, 30), Ordering::SeqCst);
    for i in 0..3 {
        assert!(
            b.take("acct", true),
            "prefetch {} did not recover as the window drained ({} left)",
            i + 1,
            b.remaining("acct")
        );
    }
    assert!(!b.take("acct", true), "a recovered prefetch spent into the reserve");
}

/// With no flag the reserve does not exist: a viewer's ceiling is the whole allowance, exactly as before.
#[test]
fn add_budget_without_the_flag_the_reserve_changes_nothing() {
    let b = AddBudget::new(HOUR, 5);
    b.reserve.store(4, Ordering::SeqCst);
    for i in 0..5 {
        assert!(
            b.take("acct", false),
            "add {} of 5 refused — the reserve leaked into the default intent",
            i + 1
        );
    }
    assert!(!b.take("acct", false), "a sixth add against an allowance of 5");
}

/// A reserve refusal is its own error: scout-side (never filed as the store's refusal) and apart from a spent
/// budget, so the route can answer it with its own code.
#[test]
fn spend_add_prefetch_refusal_is_the_reserve_not_the_budget() {
    let _budget = install_budget(HOUR, 6);
    global_add_budget().reserve.store(5, Ordering::SeqCst);

    let before = metrics().add_refused_prefetch.load(Ordering::Relaxed);
    if let Err(err) = spend_add(DebridService::TorBox, "tok", H, true) {
        panic!("the one prefetch add above the reserve was refused: {err:?}");
    }
    let err = spend_add(DebridService::TorBox, "tok", H, true)
        .expect_err("the reserve refuses the second prefetch");
    // The reserve is a typed scout-side refusal; its display/wire reason remains stable.
    assert!(
        is_play_reserve(&err) && matches!(err, StoreError::PlayReserve(_)),
        "want errPlayReserve wrapped as scout-side, got {err:?}"
    );
    let cache = MemoryCache::new(1 << 20);
    record_refusal(&cache, DebridService::TorBox, "tok", H, &err);
    assert!(
        backed_off(&cache, DebridService::TorBox, "tok", H).is_none(),
        "the reserve was written into the store's refusal memory"
    );
    let got = metrics().add_refused_prefetch.load(Ordering::Relaxed) - before;
    assert_eq!(got, 1, "prefetch refusals counted {got}, want 1");

    let play_before = metrics().add_refused_play.load(Ordering::Relaxed);
    for _ in 0..5 {
        if let Err(err) = spend_add(DebridService::TorBox, "tok", H, false) {
            panic!("Play refused with the reserve left: {err:?}");
        }
    }
    let err = spend_add(DebridService::TorBox, "tok", H, false).expect_err("a spent budget refuses Play");
    assert!(!is_play_reserve(&err), "a spent budget must refuse Play as spent, not as the reserve: {err:?}");
    let got = metrics().add_refused_play.load(Ordering::Relaxed) - play_before;
    assert_eq!(got, 1, "play refusals counted {got}, want 1");
}

/// Through the route: with the allowance at the reserve, /play?prefetch=1 is refused with its own code and a
/// Retry-After and sends no add, while the same request without the flag still adds.
#[tokio::test]
async fn handle_play_prefetch_is_held_back_and_play_is_served() {
    let budget = install_budget(HOUR, 5);
    budget.budget().reserve.store(5, Ordering::SeqCst); // only the reserve is left: no prefetch may add, a viewer still may

    let adds = Arc::new(AtomicI32::new(0));
    let cache = Arc::new(MemoryCache::new(1 << 20));
    let counter = adds.clone();
    let store: Arc<dyn Store> = Arc::new(TorBoxStore {
        token: "tok".to_owned(),
        api: TORBOX_API.to_owned(),
        cache: Some(cache.clone()),
        client: Arc::new(MockDoer(move |req: reqwest::Request| {
            if is_add_endpoint(&req) {
                counter.fetch_add(1, Ordering::SeqCst);
            }
            Ok(resp(200, r#"{"data":[]}"#))
        })),
    });
    let deps = test_deps(move |d| {
        d.cache = cache;
        d.make_stores = shared_stores(store.clone());
    });
    deps.runtime.replace_add_budget(budget.budget());
    let h = new_handler(deps);
    let path = format!(
        "/{}/play/{}",
        valid_blob(),
        encode_play_token(&PlayTarget { info_hash: H.to_owned(), ..Default::default() })
    );

    let rec = get(&h, &format!("{path}?prefetch=1"), &[]).await;
    let body = body_str(&rec);
    assert!(
        rec.status() == 503 && body.contains(r#""reserved_for_play""#),
        "prefetch at the reserve: got {} {body}, want 503 reserved_for_play",
        rec.status()
    );
    assert!(
        !body.contains("scout_busy") && !body.contains("store_unavailable"),
        "the reserve must not read as a spent budget or a refusing store: {body}"
    );
    let secs: i64 = hdr(&rec, "retry-after").parse().unwrap_or(0);
    assert!(secs >= 1, "retry-after = {:?}, want a positive number of seconds", hdr(&rec, "retry-after"));
    let n = adds.load(Ordering::SeqCst);
    assert_eq!(n, 0, "a held-back prefetch sent {n} adds");

    let rec = get(&h, &path, &[]).await;
    let body = body_str(&rec);
    assert!(
        !body.contains("reserved_for_play") && !body.contains("scout_busy"),
        "Play was refused by the budget with the reserve left: {} {body}",
        rec.status()
    );
    let n = adds.load(Ordering::SeqCst);
    assert_eq!(n, 1, "Play sent {n} adds, want 1 — the reserve is for it");
}

/// /metrics reports the allowance as each intent sees it, and the refusals per intent.
#[test]
fn metrics_report_the_allowance_and_refusals_per_intent() {
    let _budget = install_budget(HOUR, 10);
    global_add_budget().reserve.store(3, Ordering::SeqCst);

    let out = metrics().render(-1);
    for want in [
        r#"scout_add_budget_remaining_by_intent{intent="play"} -1"#,
        r#"scout_add_budget_remaining_by_intent{intent="prefetch"} -1"#,
        r#"scout_add_budget_refusals_total{intent="play"}"#,
        r#"scout_add_budget_refusals_total{intent="prefetch"}"#,
    ] {
        assert!(out.contains(want), "before any spend, /metrics lacks {want:?}");
    }

    for _ in 0..8 {
        global_add_budget().take("acct", false);
    }
    let out = metrics().render(-1);
    for want in [
        "scout_add_budget_remaining 2\n",
        "scout_add_budget_remaining_by_intent{intent=\"play\"} 2\n",
        "scout_add_budget_remaining_by_intent{intent=\"prefetch\"} 0\n",
    ] {
        assert!(out.contains(want), "/metrics lacks {want:?}:\n{out}");
    }
}

#[test]
fn parse_play_reserve() {
    let limit = ADD_BUDGET_LIMIT.to_string();
    for (input, want) in [
        ("", DEFAULT_PLAY_RESERVE),
        ("0", 0),
        (" 8 ", 8),
        ("-1", DEFAULT_PLAY_RESERVE),
        ("five", DEFAULT_PLAY_RESERVE),
        // would leave a prefetch nothing at all
        (limit.as_str(), DEFAULT_PLAY_RESERVE),
    ] {
        let got = parse_reserve(input);
        assert_eq!(got, want, "parsePlayReserve({input:?}) = {got}, want {want}");
    }
    let s = settings_from_env(|k| if k == "PLAY_RESERVE_ADDS" { "2".to_owned() } else { String::new() });
    assert_eq!(s.play_reserve_adds, 2, "PLAY_RESERVE_ADDS=2 read as {}", s.play_reserve_adds);
}

#[test]
fn set_play_reserve() {
    let _budget = install_budget(HOUR, 10);
    set_reserve(7);
    let got = global_add_budget().play_reserve();
    assert_eq!(got, 7, "reserve = {got}, want 7");
}
