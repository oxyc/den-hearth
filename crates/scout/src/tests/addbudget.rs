//! Go: `internal/scout/addbudget_test.go` (3782 lines, 101 tests). Work package 3.
//!
//! The tests that exercise the budget alone run against `addbudget.rs` and fail at its `todo!()` stubs until it
//! lands. Most of the file drives the stores, so it also needs the store impls, the refusal memory and the
//! handler from packages 1, 2 and 4, which have not landed. Those items are stood in for by `foreign` at the
//! bottom of this file so the crate compiles; every test that reaches one is `#[ignore]`d. When an owner lands,
//! delete its stand-ins here and point the test at the real item.

use std::io;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crate::addbudget::{
    budget_account, budget_remaining, budget_take, global_add_budget, refund_add, set_global_add_budget,
    spend_add, AddBudget, RequestNotSent,
};
use crate::config::DebridService::{self, Premiumize, RealDebrid, TorBox};
use crate::stores::StoreError;

const HOUR: Duration = Duration::from_secs(60 * 60);

/// Go `H` (`stores_test.go`): the infohash most tests resolve.
const H: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";

/// Go `repeat` (`core_test.go`).
fn repeat(s: &str, n: usize) -> String {
    s.repeat(n)
}

/// Go `intp` (`rank_test.go`).
fn intp(n: i64) -> Option<i64> {
    Some(n)
}

/// Go `time.Date(2026, 9, 2, 10, 0, 0, 0, time.UTC)`.
fn t0() -> SystemTime {
    UNIX_EPOCH + Duration::from_secs(1_788_343_200)
}

/// A test clock. Go closes over a `now` variable and moves it forward; a shared cell does the same here.
#[derive(Clone)]
struct Clock(Arc<Mutex<SystemTime>>);

impl Clock {
    fn new(t: SystemTime) -> Clock {
        Clock(Arc::new(Mutex::new(t)))
    }

    fn add(&self, d: Duration) {
        let mut t = self.0.lock().unwrap();
        *t += d;
    }

    /// The `now` function to install on an `AddBudget`.
    fn now_fn(&self) -> Box<dyn Fn() -> SystemTime + Send + Sync> {
        let c = self.clone();
        Box::new(move || *c.0.lock().unwrap())
    }
}

/// Holds the process-wide budget for one test. Go swaps `globalAddBudget` and restores it with a `defer`;
/// the guard does the same on drop, including when a test panics. The lock keeps two tests from swapping it
/// at once, since Rust runs tests on parallel threads and Go's do not.
struct GlobalBudget {
    prev: Option<Arc<AddBudget>>,
    _serial: MutexGuard<'static, ()>,
}

static SERIAL: Mutex<()> = Mutex::new(());

fn install_budget(window: Duration, limit: i64) -> GlobalBudget {
    let serial = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let prev = set_global_add_budget(Arc::new(AddBudget::new(window, limit)));
    GlobalBudget { prev: Some(prev), _serial: serial }
}

impl Drop for GlobalBudget {
    fn drop(&mut self) {
        if let Some(prev) = self.prev.take() {
            set_global_add_budget(prev);
        }
    }
}

/// The allowance is spent, then refused — not spent, then apologised for.
#[test]
fn add_budget_refuses_past_the_limit() {
    let b = AddBudget::new(HOUR, 3);
    for i in 0..3 {
        assert!(b.take("acct", false), "refused add {} of an allowance of 3", i + 1);
    }
    assert!(!b.take("acct", false), "a fourth add was allowed against an allowance of 3");
    assert_eq!(b.remaining("acct"), 0, "remaining");
}

/// Per ACCOUNT. One install's spending must not refuse another's, and the token is the identity.
#[test]
fn add_budget_is_per_account() {
    let b = AddBudget::new(HOUR, 1);
    assert!(b.take("a", false) && b.take("b", false), "two accounts each have their own first add");
    assert!(!b.take("a", false), "account a's allowance was not its own");
    assert_eq!(b.remaining("b"), 0, "b remaining");
}

/// Rolling, not a fixed hourly bucket: the ceiling this mirrors is rolling, so spending the whole
/// allowance at 10:59 and again at 11:01 is exactly the burst that trips the real limit.
#[test]
fn add_budget_window_rolls_rather_than_resetting() {
    let clock = Clock::new(t0());
    let mut b = AddBudget::new(HOUR, 2);
    b.now = clock.now_fn();

    // Staggered, so the two adds age out at different times — that is what makes the window a rolling
    // one rather than a bucket that empties all at once.
    assert!(b.take("acct", false), "the first add is within the allowance");
    clock.add(Duration::from_secs(30 * 60));
    assert!(b.take("acct", false), "the second add is within the allowance");
    // 10:59 — both are still inside the hour.
    clock.add(Duration::from_secs(29 * 60));
    assert!(!b.take("acct", false), "an add while the allowance is still spent must be refused");
    // 11:01 — the 10:00 add has aged out; the 10:30 one has not, so exactly one slot is free.
    clock.add(Duration::from_secs(2 * 60));
    assert!(b.take("acct", false), "the oldest add aged out; one slot should be free");
    assert!(!b.take("acct", false), "only one add had aged out, but two slots were granted");
}

/// A refusal spends nothing, or a client that keeps asking would hold its own budget shut forever.
#[test]
fn add_budget_a_refusal_costs_nothing() {
    let clock = Clock::new(t0());
    let mut b = AddBudget::new(HOUR, 1);
    b.now = clock.now_fn();

    b.take("acct", false);
    for _ in 0..50 {
        // a poll loop, hammering while refused
        clock.add(Duration::from_secs(1));
        b.take("acct", false);
    }
    // The single spent add is ~50s old; once it ages out the next caller is served, and the 50 refusals
    // must not have pushed the window forward.
    clock.add(HOUR);
    assert!(b.take("acct", false), "refusals extended the window — a polling client would never recover");
}

/// Concurrent callers must not be able to overspend: the whole point is a hard ceiling under load, and
/// load is exactly when the paths that spend adds all fire at once.
#[test]
fn add_budget_concurrent_callers_cannot_overspend() {
    let b = Arc::new(AddBudget::new(HOUR, 10));
    let granted = Arc::new(AtomicUsize::new(0));
    let handles: Vec<_> = (0..200)
        .map(|_| {
            let b = b.clone();
            let granted = granted.clone();
            thread::spawn(move || {
                if b.take("acct", false) {
                    granted.fetch_add(1, Ordering::SeqCst);
                }
            })
        })
        .collect();
    for h in handles {
        h.join().unwrap();
    }
    assert_eq!(granted.load(Ordering::SeqCst), 10, "adds granted against an allowance of 10");
}

/// A nil budget is a no-op, so a store built without one is never silently blocked.
#[test]
fn add_budget_nil_allows_everything() {
    let b: Option<&AddBudget> = None;
    assert!(budget_take(b, "acct", false), "a nil budget must not refuse");
    assert_eq!(budget_remaining(b, "acct"), -1, "a nil budget has no remaining count to report");
}

/// The allowance covers EVERY store that can add, not just the one whose limit is documented.
///
/// It was enforced inside torBoxStore.addMagnet, so Real-Debrid and Premiumize adds were uncounted — and
/// an unbounded add loop is a bug wherever it points, published limit or not. Per service AND account, so
/// a busy Real-Debrid cannot close TorBox's budget.
#[test]
fn spend_add_is_per_service_and_account() {
    let _budget = install_budget(HOUR, 1);

    for svc in [TorBox, RealDebrid, Premiumize] {
        assert!(spend_add(svc, "tok", H, false).is_ok(), "{svc:?}: first add refused");
        assert!(spend_add(svc, "tok", H, false).is_err(), "{svc:?}: second add allowed against an allowance of 1");
        // A different account on the same service has its own allowance.
        assert!(
            spend_add(svc, "other-tok", H, false).is_ok(),
            "{svc:?}: another account's allowance was consumed"
        );
    }
}

/// A refusal names the service, so the app can say which debrid is the problem rather than "no source".
#[test]
fn spend_add_refusal_names_the_service() {
    let _budget = install_budget(HOUR, 0);

    match spend_add(RealDebrid, "tok", H, false) {
        Err(StoreError::Unavailable(u)) if u.service == DebridService::RealDebrid => {}
        other => panic!("want a realdebrid StoreUnavailableError, got {other:?}"),
    }
}

/// The window is per account and the account comes from the install's own config URL, on a route with no
/// authentication — so a map that only ever grows is not bounded by anything this process controls.
#[test]
fn add_budget_forgets_accounts_whose_window_has_drained() {
    let clock = Clock::new(SystemTime::now());
    let mut b = AddBudget::new(HOUR, 50);
    b.now = clock.now_fn();
    for i in 0..5000 {
        b.take(&format!("torbox:{i}"), false);
    }
    assert_eq!(b.spent.lock().unwrap().len(), 5000, "setup: accounts");

    // An hour later every one of those windows has drained; the next caller must not inherit them.
    clock.add(2 * HOUR);
    b.take("torbox:live", false);
    assert_eq!(b.spent.lock().unwrap().len(), 1, "map holds accounts, want just the live one");
    // And the live account still accounts correctly.
    assert_eq!(b.remaining("torbox:live"), 49, "remaining");
}

/// refundAdd must discriminate. Its whole job is telling "the request was never sent" from "the response
/// never arrived", and an earlier version refunded both — putting sixty real adds on the account while
/// reporting a full allowance. The prior test for this drove a 400 RESPONSE, so err was nil and refundAdd
/// was never reached: it passed against an unconditional refund.
#[test]
fn refund_add_only_for_a_request_that_was_never_sent() {
    let _budget = install_budget(HOUR, 10);
    let acct = budget_account(TorBox, "tok");

    global_add_budget().take(&acct, false);
    refund_add(TorBox, "tok", &io::Error::other("context canceled"));
    assert_eq!(
        global_add_budget().remaining(&acct),
        9,
        "a cancelled add was refunded — it had already reached the service"
    );
    refund_add(TorBox, "tok", &io::Error::other("context deadline exceeded"));
    assert_eq!(global_add_budget().remaining(&acct), 9, "an expired deadline was refunded — same reason");
    refund_add(TorBox, "tok", &io::Error::other("connection reset"));
    assert_eq!(
        global_add_budget().remaining(&acct),
        9,
        "a transport error was refunded; it may still have been delivered"
    );
    refund_add(TorBox, "tok", &RequestNotSent);
    assert_eq!(global_add_budget().remaining(&acct), 10, "a request that was never built must be refunded");
}
