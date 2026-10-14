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
use crate::cache::{Cache, MemoryCache};
use crate::scrape::{DoError, Doer};
use crate::stores::premiumize::{PremiumizeStore, PREMIUMIZE_API};
use crate::stores::realdebrid::{RealDebridStore, REAL_DEBRID_API};
use crate::stores::torbox::{TorBoxStore, TORBOX_API};
use crate::stores::{ResolveTarget, StoreError, StoreUnavailableError, redact_token};
use crate::tests::mock::MockDoer;

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

// ---- Store-level helpers -------------------------------------------------------------------------------------

/// Go `mockDoer`'s reply: a response, or a transport error.
type Reply = Result<reqwest::Response, DoError>;

/// Go's table entries of `func(doer, Cache) Store`: one store per service, built on a `Doer` and a cache.
#[cfg(any())]
type Build = fn(Arc<dyn Doer>, Arc<MemoryCache>) -> Box<dyn Store>;

fn doer<F>(f: F) -> Arc<dyn Doer>
where
    F: Fn(reqwest::Request) -> Reply + Send + Sync + 'static,
{
    Arc::new(MockDoer(f))
}

fn new_cache() -> Arc<MemoryCache> {
    Arc::new(MemoryCache::new(1 << 20))
}

fn tgt(info_hash: &str) -> ResolveTarget {
    ResolveTarget { info_hash: info_hash.to_string(), ..Default::default() }
}

fn tb(token: &str, client: Arc<dyn Doer>, cache: Option<Arc<dyn Cache>>) -> TorBoxStore {
    TorBoxStore { token: token.to_string(), client, cache, api: TORBOX_API.to_string() }
}

fn rd(token: &str, client: Arc<dyn Doer>, cache: Option<Arc<dyn Cache>>) -> RealDebridStore {
    RealDebridStore { token: token.to_string(), client, cache, api: REAL_DEBRID_API.to_string() }
}

fn pm(token: &str, client: Arc<dyn Doer>, cache: Option<Arc<dyn Cache>>) -> PremiumizeStore {
    PremiumizeStore { token: token.to_string(), client, cache, api: PREMIUMIZE_API.to_string() }
}

/// Go `isAddEndpoint` — the three calls that actually queue a torrent. Reads (mylist, checkcached, requestdl)
/// are not adds, and a test that counts every request cannot tell the difference.
fn is_add_endpoint(r: &reqwest::Request) -> bool {
    let p = r.url().path();
    p.contains("createtorrent") || p.contains("addMagnet") || p.contains("directdl")
}

/// The Go `errors.As(err, &unavailable)` check: the service refused, with its `StoreUnavailableError`.
fn unavailable<T>(res: &Result<T, StoreError>) -> Option<&StoreUnavailableError> {
    match res {
        Err(StoreError::Unavailable(u)) => Some(u),
        _ => None,
    }
}

/// Go `errors.Is(err, errWouldAdd)` and the other `*DeadLinkError` sentinels, which are compared by reason.
fn is_dead_reason<T>(res: &Result<T, StoreError>, reason: &str) -> bool {
    matches!(res, Err(StoreError::DeadLink(d)) if d.reason == reason)
}

/// Go `errors.As(err, &dead)` for a `*DeadLinkError`.
fn is_dead_link<T>(res: &Result<T, StoreError>) -> bool {
    matches!(res, Err(StoreError::DeadLink(_)))
}

/// Go `errServiceWentQuiet`: the cause these tests mean — the service stopped answering, not the client hanging up.
fn service_went_quiet() -> io::Error {
    io::Error::other("read tcp 10.0.0.2:443: connection reset by peer")
}

/// Go `TorBoxStore`/`RealDebridStore`/`PremiumizeStore` as a table entry, with each store's own `Build`.
#[cfg(any())]
fn builders() -> [(&'static str, Build); 3] {
    [
        ("torbox", |d, c| Box::new(tb("tok", d, Some(c)))),
        ("realdebrid", |d, c| Box::new(rd("tok", d, Some(c)))),
        ("premiumize", |d, c| Box::new(pm("tok", d, Some(c)))),
    ]
}

// ---- Store-level tests ---------------------------------------------------------------------------------------

/// Every store that can add is WIRED to the allowance, not merely able to consult it.
///
/// The budget lived inside torBoxStore.addMagnet, so Real-Debrid and Premiumize added freely past it.
/// Testing spendAdd alone would not have noticed: this drives each store's Resolve with the allowance
/// already spent and asserts no request reaches the service at all.
#[tokio::test]
#[ignore = "needs packages 1 and 2: the TorBox, Real-Debrid and Premiumize Store impls"]
async fn stores_refuse_to_add_once_the_budget_is_spent() {
    // Enable when the owning package lands: move the body up and delete this wrapper.
    #[cfg(any())]
    async fn pending() {
        for (name, build) in builders() {
            let _budget = install_budget(HOUR, 0); // nothing left this hour

            let reqs = Arc::new(AtomicUsize::new(0));
            let r = reqs.clone();
            let d = doer(move |_| {
                r.fetch_add(1, Ordering::SeqCst);
                Ok(resp(200, "{}"))
            });
            let res = build(d, new_cache()).resolve(&tgt(H)).await;
            assert!(unavailable(&res).is_some(), "{name}: want a budget refusal, got {res:?}");
            let n = reqs.load(Ordering::SeqCst);
            assert_eq!(n, 0, "{name}: made {n} requests despite a spent allowance — the store is not wired to it");
        }
    }
    todo!("needs packages 1 and 2: the TorBox, Real-Debrid and Premiumize Store impls");
}

/// A cancel storm must cost ONE add, not one per poll — and the one it costs must be counted.
///
/// Both halves matter and an earlier version got each of them wrong in turn. Refunding cancelled adds
/// looked right and was measurably false: the POST has already been written, the debrid counts it, and
/// sixty cancelled polls of one release put sixty createtorrent calls on the account while this budget
/// still reported a full allowance. The cure is not to stop counting but to stop re-sending: an add whose
/// outcome we never learned is remembered, and the next poll finds that memory instead of the add path.
#[tokio::test]
#[ignore = "needs package 1: the TorBox Store impl and its add path"]
async fn add_budget_a_cancel_storm_costs_one_add() {
    // Enable when the owning package lands: move the body up and delete this wrapper.
    #[cfg(any())]
    async fn pending() {
        let _budget = install_budget(HOUR, 50);

        let sent = Arc::new(AtomicUsize::new(0));
        let s = sent.clone();
        let d = doer(move |req| {
            if !is_add_endpoint(&req) {
                return Ok(resp(200, r#"{"data":[]}"#)); // the account holds nothing; only the ADD is abandoned
            }
            s.fetch_add(1, Ordering::SeqCst); // the request went out; the caller just never gets the answer
            Err(io::Error::other("context canceled").into())
        });
        let store = tb("tok", d, Some(new_cache()));
        for _ in 0..60 {
            let _ = store.resolve(&tgt(H)).await;
        }
        let n = sent.load(Ordering::SeqCst);
        assert_eq!(n, 1, "sent {n} createtorrent requests across sixty cancelled polls of one release");
        let left = global_add_budget().remaining(&budget_account(TorBox, "tok"));
        assert_eq!(left, 49, "remaining = {left}, want 49 — the add that WAS sent must be counted");
        // Other titles are unaffected: one stuck release must not close the hour.
        let other = spend_add(TorBox, "tok", &repeat("c", 40), false);
        assert!(other.is_ok(), "another title was refused: {other:?}");
    }
    todo!("needs package 1: the TorBox Store impl and its add path");
}

/// A torrent the account already holds is resolved from the account, not bought again.
///
/// The resolve entry is a six-hour convenience cache, not a record of what the account has — so anything
/// not played in the last six hours, which is most of a library, fell through to createtorrent and paid
/// an add for a torrent already sitting there. The account listing answers this and was already being
/// consulted by Status; it just was not consulted here.
#[tokio::test]
#[ignore = "needs package 1: the TorBox Store impl (Status and the held-torrent path)"]
async fn add_budget_a_held_torrent_is_not_bought_again() {
    // Enable when the owning package lands: move the body up and delete this wrapper.
    #[cfg(any())]
    async fn pending() {
        let _budget = install_budget(HOUR, 50);

        let adds = Arc::new(AtomicUsize::new(0));
        let a = adds.clone();
        let d = doer(move |req| {
            if is_add_endpoint(&req) {
                a.fetch_add(1, Ordering::SeqCst);
                return Ok(resp(200, r#"{"data":{"torrent_id":9}}"#));
            }
            if req.url().path().contains("mylist") {
                return Ok(resp(200, &format!(r#"{{"data":[{{"id":9,"hash":"{H}"}}]}}"#)));
            }
            Ok(resp(200, r#"{"success":true,"data":"https://cdn/x"}"#))
        });
        let store = tb("tok", d, Some(new_cache()));
        let target = ResolveTarget { file_idx: Some(0), ..tgt(H) };
        // The sequence /play actually runs: Status first — it finds the torrent in the account and remembers
        // its id — then Resolve. Resolve must use what Status learned rather than buying the torrent again.
        assert!(store.status(&target).await.is_none(), "a finished torrent is not downloading");
        let link = store.resolve(&target).await.expect("a held torrent must resolve");
        assert!(!link.is_empty(), "a held torrent must resolve to a link");
        let n = adds.load(Ordering::SeqCst);
        assert_eq!(n, 0, "spent {n} adds on a torrent the account already holds");
        let left = global_add_budget().remaining(&budget_account(TorBox, "tok"));
        assert_eq!(left, 50, "allowance dropped to {left} for a torrent that needed no add");
    }
    todo!("needs package 1: the TorBox Store impl (Status and the held-torrent path)");
}

/// What the ceiling counts is adds that may HAVE HAPPENED, and the dividing line is whether the service
/// answered. A request that reached the wire and was never answered keeps its charge — the torrent may
/// well exist, which is why refundAdd will not give it back. An answered non-2xx is the opposite: the
/// service said it created nothing, so holding the charge only erodes the allowance. All three stores
/// draw the line in the same place; TorBox used to keep both, so a repeatedly polled bad magnet ate its
/// hourly allowance where RD and Premiumize were immune.
#[tokio::test]
#[ignore = "needs package 1: the TorBox Store impl (the add path's charge)"]
async fn add_budget_the_charge_follows_whether_the_service_answered() {
    // Enable when the owning package lands: move the body up and delete this wrapper.
    #[cfg(any())]
    async fn pending() {
        let cases: [(&str, fn() -> Reply, i64); 2] = [
            ("answered, created nothing", || Ok(resp(400, r#"{"error":"NOPE"}"#)), 5),
            ("never answered", || Err(service_went_quiet().into()), 4),
        ];
        for (name, respond, want) in cases {
            let _budget = install_budget(HOUR, 5);
            let store = tb("tok", doer(move |_| respond()), Some(new_cache()));
            let _ = store.resolve(&tgt(H)).await;
            let left = global_add_budget().remaining(&budget_account(TorBox, "tok"));
            assert_eq!(left, want, "{name}: remaining = {left}, want {want}");
        }
    }
    todo!("needs package 1: the TorBox Store impl (the add path's charge)");
}

/// A NoAdd target can never queue a torrent, whatever the store would otherwise do.
///
/// The probe path already chose carefully — it only resolves releases a cache check reported as held —
/// and still POSTed createtorrent, because TorBox's checkcached says what TORBOX has, not what this
/// ACCOUNT has. A caller cannot promise this by choosing well; only the store can enforce it.
#[tokio::test]
#[ignore = "needs packages 1 and 2: the three Store impls (the NoAdd path)"]
async fn no_add_never_reaches_an_add_endpoint() {
    // Enable when the owning package lands: move the body up and delete this wrapper.
    #[cfg(any())]
    async fn pending() {
        for (name, build) in builders() {
            let adds = Arc::new(AtomicUsize::new(0));
            let a = adds.clone();
            let d = doer(move |req| {
                if is_add_endpoint(&req) {
                    a.fetch_add(1, Ordering::SeqCst);
                }
                Ok(resp(200, r#"{"data":[]}"#)) // an empty account: nothing held
            });
            let res = build(d, new_cache()).resolve(&ResolveTarget { no_add: true, ..tgt(H) }).await;
            assert!(res.is_err(), "{name}: nothing is held, so a NoAdd resolve must fail rather than queue it");
            // Reads are fine and expected — asking the account what it holds is how NoAdd is answered.
            // What must never happen is the queueing call.
            let n = adds.load(Ordering::SeqCst);
            assert_eq!(n, 0, "{name}: made {n} adds for a NoAdd target — the store queued the torrent anyway");
        }
    }
    todo!("needs packages 1 and 2: the three Store impls (the NoAdd path)");
}

/// Scout's own ceiling is not the debrid's refusal, and must not be remembered or reported as one.
#[test]
#[ignore = "needs package 2: stores/refusal.rs (recordRefusal and backedOff)"]
fn spend_add_our_ceiling_is_not_the_stores_refusal() {
    // Enable when the owning package lands: move the body up and delete this wrapper.
    #[cfg(any())]
    fn pending() {
        let _budget = install_budget(HOUR, 0);

        let cache = new_cache();
        let err = spend_add(TorBox, "tok", H, false).expect_err("nothing left this hour");
        record_refusal(&*cache, TorBox, "tok", H, &err);
        assert!(
            backed_off(&*cache, TorBox, "tok", H).is_none(),
            "scout's own budget was written into the store's refusal memory"
        );
        // It still reads as unavailable to the caller — just not as TorBox's doing.
        match &err {
            StoreError::Unavailable(u) => {
                assert!(u.reason.contains("scout"), "the reason must name scout as the refuser: {:?}", u.reason)
            }
            other => panic!("a budget refusal must still be a refusal: {other:?}"),
        }
    }
    todo!("needs package 2: stores/refusal.rs (recordRefusal and backedOff)");
}

/// /health reports the allowance without naming accounts. The route is unauthenticated, and a stable
/// service:hash(token) key would make it a confirmation oracle for a guessed token.
#[test]
#[ignore = "needs package 4: handler.rs and deps.rs (Deps, the /health route)"]
fn health_reports_the_budget_without_naming_accounts() {
    // Enable when the owning package lands: move the body up and delete this wrapper.
    #[cfg(any())]
    fn pending() {
        let _budget = install_budget(HOUR, 10);
        global_add_budget().take(&budget_account(TorBox, "secret-token"), false);

        let deps = crate::deps::Deps::for_test(new_cache());
    let req = http::Request::get("/health").body(String::new()).unwrap();
    let body = crate::handler::serve(&deps, req).into_body();
        assert!(body.contains("addBudgetRemaining"), "a spent allowance must be visible: {body}");
        assert!(
            !body.contains(&key_hash("secret-token")) && !body.contains("torbox"),
            "/health discloses which account is spending: {body}"
        );
    }
    todo!("needs package 4: handler.rs and deps.rs (Deps, the /health route)");
}

/// Scout's own in-flight marker is not the STORE refusing, and must not be recorded or reported as one.
///
/// It shipped returning a bare StoreUnavailableError{torbox,…}, which recordRefusal then wrote into the
/// per-hash refusal memory: /play and the probe answered 503 naming TorBox for a state TorBox had no part
/// in, the backoff outlived its own 90s window (60s refusal re-recorded on each poll), and the read-only
/// NoAdd path was blocked by a marker it cannot cause.
#[test]
#[ignore = "needs package 2: stores/refusal.rs (noteAddAttempt, addInFlight, recordRefusal)"]
fn add_in_flight_is_scout_side_not_the_stores() {
    // Enable when the owning package lands: move the body up and delete this wrapper.
    #[cfg(any())]
    fn pending() {
        let cache = new_cache();
        note_add_attempt(&*cache, TorBox, "tok", H);

        let err = add_in_flight(&*cache, TorBox, "tok", H).expect_err("an add in flight must be reported");
        // Its own sentinel, not the budget's: an add in flight means the release IS being fetched, so the
        // route answers 202 "coming", where a spent allowance answers 503 "not now". Sharing errScoutSide
        // made both come out as a refusal, and the client stopped trying other sources for a release scout
        // had queued itself.
        assert!(
            matches!(err, StoreError::AddInFlight),
            "an add in flight needs its own sentinel, not the budget's: {err:?}"
        );
        assert!(!is_scout_side(&err), "an add in flight is not a refusal; conflating them loses the 202");
        record_refusal(&*cache, TorBox, "tok", H, &err);
        assert!(
            backed_off(&*cache, TorBox, "tok", H).is_none(),
            "scout's in-flight marker was written into the store's refusal memory"
        );
    }
    todo!("needs package 2: stores/refusal.rs (noteAddAttempt, addInFlight, recordRefusal)");
}

/// EVERY store that can add needs the in-flight guard. Giving it to TorBox alone did not stop the cancel
/// loop — it moved it: ResolvePreferring walks on to the store without a marker, and fifty cancelled
/// polls closed THAT account's allowance instead.
#[tokio::test]
#[ignore = "needs packages 1 and 2: the three Store impls (the in-flight guard)"]
async fn add_in_flight_applies_to_every_store_that_adds() {
    // Enable when the owning package lands: move the body up and delete this wrapper.
    #[cfg(any())]
    async fn pending() {
        for (name, build) in builders() {
            let _budget = install_budget(HOUR, 50);

            let adds = Arc::new(AtomicUsize::new(0));
            let a = adds.clone();
            let d = doer(move |req| {
                if !is_add_endpoint(&req) {
                    return Ok(resp(200, r#"{"data":[]}"#));
                }
                a.fetch_add(1, Ordering::SeqCst);
                Err(io::Error::other("context canceled").into()) // sent, then abandoned by the client
            });
            let store = build(d, new_cache());
            for _ in 0..30 {
                let _ = store.resolve(&tgt(H)).await;
            }
            let n = adds.load(Ordering::SeqCst);
            assert_eq!(n, 1, "{name}: sent {n} adds across thirty cancelled polls of one release");
        }
    }
    todo!("needs packages 1 and 2: the three Store impls (the in-flight guard)");
}

/// Finding the torrent in the account settles the in-flight marker — that add plainly landed.
///
/// Without this the marker outlives the fact it stood in for: the release stays "awaiting the result"
/// for its full 90s window while the result is sitting in the account listing, and every poll in between
/// is refused for a state that has already resolved itself.
#[test]
#[ignore = "needs package 1: the TorBox Store impl (torrentID) and package 2: refusal.rs (addInFlight)"]
fn add_in_flight_discovering_the_torrent_settles_the_marker() {
    // Enable when the owning package lands: move the body up and delete this wrapper.
    #[cfg(any())]
    fn pending() {
        let cache = new_cache();
        let d = doer(|req| {
            if req.url().path().contains("mylist") {
                return Ok(resp(200, &format!(r#"{{"data":[{{"id":9,"hash":"{H}"}}]}}"#)));
            }
            Ok(resp(404, "{}"))
        });
        let store = tb("tok", d, Some(cache.clone()));

        // An add went out and was abandoned, so the marker is set.
        note_add_attempt(&*cache, TorBox, "tok", H);
        assert!(add_in_flight(&*cache, TorBox, "tok", H).is_err(), "the marker should be set");

        // The account listing then shows the torrent: the add landed, so the marker must go.
        let (_, found, _) = store.torrent_id(H);
        assert!(found, "the account holds it");
        let settled = add_in_flight(&*cache, TorBox, "tok", H);
        assert!(settled.is_ok(), "the marker outlived the discovery it stood in for: {settled:?}");
    }
    todo!("needs package 1: the TorBox Store impl (torrentID) and package 2: refusal.rs (addInFlight)");
}

/// A remembered torrent id is a claim with a six-hour life, not a fact. When the account no longer has
/// the torrent, forget it and buy it again — do not answer dead_link for six hours.
///
/// The shortcut that stops re-buying a held torrent also stopped the SELF-HEALING that used to happen
/// when a torrent was removed account-side: Resolve took the held path, failed, and re-Put the id with a
/// fresh six-hour TTL on every poll, so a client polling kept the stale entry alive indefinitely and
/// blacklisted a release that had been playing an hour earlier.
#[tokio::test]
#[ignore = "needs package 1: the TorBox Store impl (the held-torrent path)"]
async fn known_torrent_id_a_deleted_torrent_is_forgotten_and_re_bought() {
    // Enable when the owning package lands: move the body up and delete this wrapper.
    #[cfg(any())]
    async fn pending() {
        let _budget = install_budget(HOUR, 50);

        let gone = Arc::new(AtomicBool::new(true));
        let adds = Arc::new(AtomicUsize::new(0));
        let (g, a) = (gone.clone(), adds.clone());
        let d = doer(move |req| {
            if is_add_endpoint(&req) {
                a.fetch_add(1, Ordering::SeqCst);
                g.store(false, Ordering::SeqCst); // re-bought: the account has it again
                return Ok(resp(200, r#"{"data":{"torrent_id":77}}"#));
            }
            if req.url().path().contains("mylist") {
                return Ok(resp(200, r#"{"data":[]}"#));
            }
            if g.load(Ordering::SeqCst) {
                return Ok(resp(404, r#"{"success":false}"#)); // requestdl on a torrent that is not there
            }
            Ok(resp(200, r#"{"success":true,"data":"https://cdn/x"}"#))
        });
        let cache = new_cache();
        cache.put(&torrent_id_key("tok", H), "42", resolve_cache_ttl()); // a stale id from before the deletion
        let store = tb("tok", d, Some(cache.clone()));
        let target = ResolveTarget { file_idx: Some(0), ..tgt(H) };

        let link = store.resolve(&target).await.expect("a deleted torrent must be re-bought, not answered dead");
        assert!(!link.is_empty(), "a deleted torrent must be re-bought, not answered dead");
        let n = adds.load(Ordering::SeqCst);
        assert_eq!(n, 1, "made {n} adds; the stale id should have been forgotten and the torrent re-added");
        assert_ne!(
            cache.get(&torrent_id_key("tok", H)).as_deref(),
            Some("42"),
            "the stale id survived, and every later poll would keep it alive for another six hours"
        );

        // And when the re-add ALSO fails, the stale id must be gone rather than re-stamped with a fresh
        // six-hour TTL. That refresh is what made a deleted torrent unplayable indefinitely: the held path
        // re-Put the id on every poll, so a polling client kept its own poison alive.
        let stuck = new_cache();
        stuck.put(&torrent_id_key("tok", H), "42", resolve_cache_ttl());
        let dead = tb(
            "tok",
            doer(|req| {
                if req.url().path().contains("mylist") {
                    return Ok(resp(200, r#"{"data":[]}"#));
                }
                Ok(resp(404, r#"{"success":false}"#)) // nothing works: the add fails too
            }),
            Some(stuck.clone()),
        );
        let _ = dead.resolve(&target).await;
        assert_ne!(
            stuck.get(&torrent_id_key("tok", H)).as_deref(),
            Some("42"),
            "a failing resolve refreshed the stale id instead of forgetting it"
        );
    }
    todo!("needs package 1: the TorBox Store impl (the held-torrent path)");
}

/// An add going out clears the torrent-miss marker.
///
/// That marker suppresses the account listing for 15s, and the listing is the only thing that can find
/// the torrent the add just created — so leaving it kept the next poll on the "nothing is queued" path
/// instead of "it is downloading". This line was deleted once during a refactor, with its explanation,
/// and nothing failed.
#[test]
#[ignore = "needs package 2: stores/refusal.rs (torrentMissKey, noteAddAttempt)"]
fn note_add_attempt_clears_the_torrent_miss_marker() {
    // Enable when the owning package lands: move the body up and delete this wrapper.
    #[cfg(any())]
    fn pending() {
        let cache = new_cache();
        cache.put(&torrent_miss_key("tok", H), "1", torrent_miss_ttl()); // Status looked and found nothing
        note_add_attempt(&*cache, TorBox, "tok", H);
        assert!(
            cache.get(&torrent_miss_key("tok", H)).is_none(),
            "the miss marker outlived the add that disproves it; Status cannot see the new torrent"
        );
    }
    todo!("needs package 2: stores/refusal.rs (torrentMissKey, noteAddAttempt)");
}

/// Only a definitive "no such torrent" forgets a remembered id. Everything else says nothing about what
/// the account holds, and re-buying on it costs an add per poll.
///
/// Measured before this: ten polls of ONE already-downloaded release cost ten adds, because a cancelled
/// poll erased the id, the re-add re-created the torrent, the next Status found it and cleared the
/// in-flight marker meant to stop the loop, and round it went.
#[tokio::test]
#[ignore = "needs package 1: the TorBox Store impl (the held-torrent path)"]
async fn known_torrent_id_only_a_confirmed_absence_forgets_the_id() {
    // Enable when the owning package lands: move the body up and delete this wrapper.
    #[cfg(any())]
    async fn pending() {
        let cases: [(&str, fn() -> Reply); 3] = [
            ("cancelled poll", || Err(io::Error::other("context canceled").into())),
            ("throttled", || Ok(resp(429, r#"{"error":"RATE"}"#))),
            ("server fault", || Ok(resp(500, "{}"))),
        ];
        for (name, respond) in cases {
            let _budget = install_budget(HOUR, 50);

            let adds = Arc::new(AtomicUsize::new(0));
            let a = adds.clone();
            let d = doer(move |req| {
                if is_add_endpoint(&req) {
                    a.fetch_add(1, Ordering::SeqCst);
                    return Ok(resp(200, r#"{"data":{"torrent_id":77}}"#));
                }
                if req.url().path().contains("mylist") {
                    return Ok(resp(200, r#"{"data":[]}"#));
                }
                respond() // requestdl on the held torrent
            });
            let cache = new_cache();
            cache.put(&torrent_id_key("tok", H), "42", resolve_cache_ttl());
            let store = tb("tok", d, Some(cache.clone()));

            for _ in 0..10 {
                let _ = store.resolve(&ResolveTarget { file_idx: Some(0), ..tgt(H) }).await;
            }
            let n = adds.load(Ordering::SeqCst);
            assert_eq!(n, 0, "{n} adds for a held torrent after a {name} — this says nothing about what the account has");
            assert_eq!(
                cache.get(&torrent_id_key("tok", H)).as_deref(),
                Some("42"),
                "the id was forgotten on a {name}"
            );
        }
    }
    todo!("needs package 1: the TorBox Store impl (the held-torrent path)");
}

/// A pack that does not contain the requested episode is a PERMANENT fact about a torrent the account
/// has. Re-buying the pack to re-learn it spends an add and returns the identical refusal.
#[tokio::test]
#[ignore = "needs package 1: the TorBox Store impl (selectFileID and the held-torrent path)"]
async fn known_torrent_id_a_pack_mismatch_does_not_re_buy_the_pack() {
    // Enable when the owning package lands: move the body up and delete this wrapper.
    #[cfg(any())]
    async fn pending() {
        let _budget = install_budget(HOUR, 50);

        let adds = Arc::new(AtomicUsize::new(0));
        let a = adds.clone();
        let d = doer(move |req| {
            if is_add_endpoint(&req) {
                a.fetch_add(1, Ordering::SeqCst);
                return Ok(resp(200, r#"{"data":{"torrent_id":77}}"#));
            }
            // The pack holds S01E01–E02 and nothing else.
            Ok(resp(
                200,
                r#"{"data":{"files":[{"id":0,"name":"Show.S01E01.mkv","size":10},{"id":1,"name":"Show.S01E02.mkv","size":20}]}}"#,
            ))
        });
        let cache = new_cache();
        cache.put(&torrent_id_key("tok", H), "42", resolve_cache_ttl());
        let store = tb("tok", d, Some(cache));

        let err = store
            .resolve(&ResolveTarget { season: intp(1), episode: intp(9), ..tgt(H) })
            .await
            .expect_err("the pack does not hold that episode");
        assert_eq!(err, err_episode_not_in_torrent(), "want errEpisodeNotInTorrent, got {err:?}");
        let n = adds.load(Ordering::SeqCst);
        assert_eq!(n, 0, "spent {n} adds re-buying a pack to re-learn what it does not contain");
    }
    todo!("needs package 1: the TorBox Store impl (selectFileID and the held-torrent path)");
}

/// Every cache key that carries account state is scoped by the account identity.
///
/// The comments say what happens otherwise — "would let one user's cached torrent_id be used with another
/// user's token → wrong/other-account content" — and the cache is process-global, so it is reachable.
/// Stripping keyHash(token) from any of these left the whole suite green.
#[test]
#[ignore = "needs package 2: stores/refusal.rs (the cache keys)"]
fn cache_keys_are_scoped_to_the_account() {
    // Enable when the owning package lands: move the body up and delete this wrapper.
    #[cfg(any())]
    fn pending() {
        let cases: [(&str, fn(&str) -> String); 6] = [
            ("resolve", |t| resolve_key(t, H)),
            ("torrentID", |t| torrent_id_key(t, H)),
            ("torrentMiss", |t| torrent_miss_key(t, H)),
            ("refused", |t| refused_key(TorBox, t, H)),
            ("addAttempt", |t| add_attempt_key(TorBox, t, H)),
            ("budget", |t| budget_account(TorBox, t)),
        ];
        for (name, key) in cases {
            let mine = key("my-token");
            let theirs = key("their-token");
            assert_ne!(mine, theirs, "{name}: two accounts share the key {mine:?} — one user's state would be served to another");
            assert!(!mine.contains("my-token"), "{name}: the raw token is in the key: {mine:?}");
        }
    }
    todo!("needs package 2: stores/refusal.rs (the cache keys)");
}

/// The Real-Debrid and Premiumize stores, the two that have no Status endpoint of their own.
#[cfg(any())]
fn rd_pm_builders() -> [(&'static str, Build); 2] {
    [
        ("realdebrid", |d, c| Box::new(rd("tok", d, Some(c)))),
        ("premiumize", |d, c| Box::new(pm("tok", d, Some(c)))),
    ]
}

/// The request body as text (Go `io.ReadAll(r.Body)`).
fn body_of(req: &reqwest::Request) -> String {
    req.body().and_then(|b| b.as_bytes()).map(|b| String::from_utf8_lossy(b).into_owned()).unwrap_or_default()
}

/// A Unix-seconds stamp for a marker's "when" (Go `strconv.FormatInt(t.Unix(), 10)`).
fn stamp_at(t: SystemTime) -> String {
    t.duration_since(UNIX_EPOCH).unwrap().as_secs().to_string()
}

/// A transfer queued and not yet produced anything: "coming" until the give-up deadline.
/// A transfer already queued is never PAID FOR again, however long it goes on failing.

// Queued-marker and Real-Debrid / Premiumize tests (addbudget_test.go lines 733-1479 in the Go file).

/// A store with no Status endpoint must remember that it QUEUED a release, or every poll re-adds it.
///
/// TorBox does not need this — it keeps a torrent id and answers Status, so a poll during the download
/// short-circuits long before Resolve. Real-Debrid and Premiumize have neither, and a just-added torrent
/// that is not downloadable yet comes back as a dead link rather than a refusal, so nothing stopped the
/// loop. Thirty polls of one release put thirty duplicate torrents on the account and spent thirty of the
/// hourly allowance in a minute: the loop TorBox's guards closed, relocated.
#[tokio::test]
#[ignore = "needs packages 1 and 2: the Real-Debrid and Premiumize Store impls (the queued marker)"]
async fn queued_a_store_without_status_does_not_re_add_on_every_poll() {
    // Enable when the owning package lands: move the body up and delete this wrapper.
    #[cfg(any())]
    async fn pending() {
        for (name, build) in rd_pm_builders() {
            let _budget = install_budget(HOUR, 50);

            let adds = Arc::new(AtomicUsize::new(0));
            let a = adds.clone();
            let d = doer(move |req| {
                if is_add_endpoint(&req) {
                    a.fetch_add(1, Ordering::SeqCst);
                    // Accepted, but nothing playable yet — the normal state of a fresh download.
                    return Ok(resp(200, r#"{"id":"t1","status":"success","content":[]}"#));
                }
                Ok(resp(200, r#"{"files":[],"links":[]}"#))
            });
            let store = build(d, new_cache());
            for _ in 0..30 {
                let _ = store.resolve(&tgt(H)).await;
            }
            // The measure is the CHARGE, not the call. Premiumize's directdl is both the question and the
            // purchase — it is the only thing that can notice the transfer finished, so it has to keep
            // being called; what must not repeat is paying for it. Real-Debrid can separate the two, and
            // does: one addMagnet, then info against the torrent it remembers.
            let service = store.service();
            let left = global_add_budget().remaining(&budget_account(service, "tok"));
            assert_eq!(left, 49, "{name}: allowance dropped to {left}; one queued release must cost one add");
            let n = adds.load(Ordering::SeqCst);
            if service == RealDebrid {
                assert_eq!(n, 1, "{n} addMagnet calls across thirty polls — each one a duplicate torrent");
            }
        }
    }
    todo!("needs packages 1 and 2: the Real-Debrid and Premiumize Store impls (the queued marker)");
}

/// A release that RESOLVED must be playable again immediately.
///
/// The queued marker reused the in-flight key with a 20-minute TTL and nothing cleared it on success, so
/// a store with no held-torrent fast path answered 202 "downloading" for a release it had just served.
/// One infohash is a whole season pack, so playing S01E01 blocked every other episode of the show; a
/// replay, a resume, or an expired redirect hit the same wall.
#[tokio::test]
#[ignore = "needs packages 1 and 2: the Real-Debrid and Premiumize Store impls (the queued marker)"]
async fn queued_a_resolved_release_is_not_blocked_by_its_own_marker() {
    // Enable when the owning package lands: move the body up and delete this wrapper.
    #[cfg(any())]
    async fn pending() {
        let cases: [(&str, &'static str, Build); 2] = [
            ("realdebrid", "", |d, c| Box::new(rd("tok", d, Some(c)))),
            (
                "premiumize",
                r#"{"status":"success","content":[{"path":"movie.mkv","link":"https://pm/final","size":999}]}"#,
                |d, c| Box::new(pm("tok", d, Some(c))),
            ),
        ];
        for (name, body, build) in cases {
            let _budget = install_budget(HOUR, 50);

            let d = doer(move |req| {
                if !body.is_empty() {
                    return Ok(resp(200, body));
                }
                // Real-Debrid needs its full add → info → select → unrestrict sequence
                let path = req.url().path();
                if path.contains("addMagnet") {
                    return Ok(resp(201, r#"{"id":"t1"}"#));
                }
                if path.contains("/torrents/info/") {
                    return Ok(resp(
                        200,
                        r#"{"files":[{"id":1,"path":"/movie.mkv","bytes":999}],"links":["https://rd/restricted"]}"#,
                    ));
                }
                if path.contains("selectFiles") {
                    return Ok(resp(204, ""));
                }
                if path.contains("unrestrict/link") {
                    return Ok(resp(200, r#"{"download":"https://rd/dl.mkv"}"#));
                }
                Ok(resp(404, "{}"))
            });
            let store = build(d, new_cache());

            let first = store.resolve(&tgt(H)).await;
            assert!(matches!(&first, Ok(l) if !l.is_empty()), "{name}: first resolve: {first:?}");
            let second = store.resolve(&tgt(H)).await;
            assert!(
                matches!(&second, Ok(l) if !l.is_empty()),
                "{name}: a release that just played came back as {second:?} — its own marker blocked it"
            );
        }
    }
    todo!("needs packages 1 and 2: the Real-Debrid and Premiumize Store impls (the queued marker)");
}

/// A permanent refusal after the add must not be laundered into a 20-minute wait: the client has to be
/// able to give up on this release and try the next.
#[tokio::test]
#[ignore = "needs package 2: the Real-Debrid Store impl and refusal.rs (addInFlight)"]
async fn queued_a_permanent_refusal_clears_the_marker() {
    // Enable when the owning package lands: move the body up and delete this wrapper.
    #[cfg(any())]
    async fn pending() {
        let _budget = install_budget(HOUR, 50);

        let cache = new_cache();
        let d = doer(|req| {
            if is_add_endpoint(&req) {
                return Ok(resp(201, r#"{"id":"t1"}"#));
            }
            // A filename RD will not serve — no amount of waiting changes it.
            Ok(resp(200, r#"{"files":[{"id":1,"path":"/Movie.2024.WEB-DL.x265.mkv","bytes":999}],"links":[]}"#))
        });
        let store = rd("tok", d, Some(cache.clone()));
        assert!(store.resolve(&tgt(H)).await.is_err(), "a blocked filename must fail");
        assert!(
            add_in_flight(&*cache, RealDebrid, "tok", H).is_ok(),
            "a permanent refusal was left looking like a download in progress"
        );
    }
    todo!("needs package 2: the Real-Debrid Store impl and refusal.rs (addInFlight)");
}

/// Real-Debrid remembers the torrent it bought, so a release is paid for once however often it is asked
/// for — including when it can never be served.
///
/// RD's addMagnet creates a NEW torrent every call, and RD has no Status endpoint for the wait in
/// between, so the guard has to be a memory of the purchase. It must survive both a success (the next
/// episode of the same pack) and a permanent refusal (a pack that lacks the episode), which is what
/// every earlier version of this got wrong in one direction or the other.
#[tokio::test]
#[ignore = "needs package 2: the Real-Debrid Store impl (the remembered torrent per release)"]
async fn real_debrid_buys_a_release_once() {
    // Enable when the owning package lands: move the body up and delete this wrapper.
    #[cfg(any())]
    async fn pending() {
        let cases: [(&str, &'static str, &'static str); 2] = [
            (
                "a pack that lacks the episode",
                r#"{"files":[{"id":1,"path":"/Show.S01E01.mkv","bytes":9}],"links":[]}"#,
                "",
            ),
            (
                "a release that plays",
                r#"{"files":[{"id":1,"path":"/movie.mkv","bytes":9}],"links":["https://rd/r"]}"#,
                "https://rd/dl.mkv",
            ),
        ];
        for (name, info, want) in cases {
            let _budget = install_budget(HOUR, 50);

            let adds = Arc::new(AtomicUsize::new(0));
            let a = adds.clone();
            let d = doer(move |req| {
                let path = req.url().path();
                if path.contains("addMagnet") {
                    a.fetch_add(1, Ordering::SeqCst);
                    return Ok(resp(201, r#"{"id":"t1"}"#));
                }
                if path.contains("/torrents/info/") {
                    return Ok(resp(200, info));
                }
                if path.contains("selectFiles") {
                    return Ok(resp(204, ""));
                }
                if path.contains("unrestrict/link") {
                    return Ok(resp(200, r#"{"download":"https://rd/dl.mkv"}"#));
                }
                Ok(resp(404, "{}"))
            });
            let store = rd("tok", d, Some(new_cache()));
            let target = if want.is_empty() {
                ResolveTarget { season: intp(1), episode: intp(9), ..tgt(H) }
            } else {
                tgt(H)
            };
            let mut last = String::new();
            for _ in 0..10 {
                last = store.resolve(&target).await.unwrap_or_default();
            }
            let n = adds.load(Ordering::SeqCst);
            assert_eq!(n, 1, "{name}: {n} adds across ten polls — RD makes a new torrent every time");
            assert_eq!(last, want, "{name}: last resolve = {last:?}, want {want:?}");
        }
    }
    todo!("needs package 2: the Real-Debrid Store impl (the remembered torrent per release)");
}

/// Only RD's own 404 forgets the remembered torrent. An empty file list is a torrent whose metadata has
/// not resolved yet — forgetting on it re-bought the release on the very next poll.
#[tokio::test]
#[ignore = "needs package 2: the Real-Debrid Store impl and knownTorrent"]
async fn real_debrid_forgets_only_on_a_confirmed_absence() {
    // Enable when the owning package lands: move the body up and delete this wrapper.
    #[cfg(any())]
    async fn pending() {
        let _budget = install_budget(HOUR, 50);

        let adds = Arc::new(AtomicUsize::new(0));
        let gone = Arc::new(AtomicBool::new(false));
        let (a, g) = (adds.clone(), gone.clone());
        let d = doer(move |req| {
            let path = req.url().path();
            if path.contains("addMagnet") {
                a.fetch_add(1, Ordering::SeqCst);
                return Ok(resp(201, r#"{"id":"t1"}"#));
            }
            if path.contains("/torrents/info/") {
                if g.load(Ordering::SeqCst) {
                    return Ok(resp(404, "{}"));
                }
                return Ok(resp(200, r#"{"files":[],"links":[]}"#)); // queued, metadata not in yet
            }
            Ok(resp(404, "{}"))
        });
        let cache = new_cache();
        let s = rd("tok", d, Some(cache.clone()));
        for _ in 0..5 {
            let _ = s.resolve(&tgt(H)).await;
        }
        let n = adds.load(Ordering::SeqCst);
        assert_eq!(n, 1, "{n} adds while the torrent was merely not ready");
        let (_, held) = s.known_torrent(&tgt(H));
        assert!(held, "a not-ready torrent was forgotten");

        // A THROTTLE on the info call is not an absence either — it describes this attempt, not the account.
        let throttled = new_cache();
        let t_adds = Arc::new(AtomicUsize::new(0));
        let ta = t_adds.clone();
        let ts = rd(
            "tok",
            doer(move |req| {
                if req.url().path().contains("addMagnet") {
                    ta.fetch_add(1, Ordering::SeqCst);
                    return Ok(resp(201, r#"{"id":"t1"}"#));
                }
                Ok(resp(503, "{}")) // RD is faulting, and says nothing about what it holds
            }),
            Some(throttled.clone()),
        );
        for _ in 0..5 {
            let _ = ts.resolve(&tgt(H)).await;
        }
        let n = t_adds.load(Ordering::SeqCst);
        assert_eq!(n, 1, "{n} adds while RD was merely throttling its own info endpoint");
        let (_, held) = ts.known_torrent(&tgt(H));
        assert!(held, "a throttle forgot the torrent, so the next poll buys it again");

        // Now RD really does not have it: forget, and let the next attempt buy it again.
        gone.store(true, Ordering::SeqCst);
        let _ = s.resolve(&tgt(H)).await;
        let (_, held) = s.known_torrent(&tgt(H));
        assert!(!held, "a 404 must forget the id, or the release is stuck behind a ghost for six hours");
    }
    todo!("needs package 2: the Real-Debrid Store impl and knownTorrent");
}

/// Premiumize marks a release as queued ONLY when directdl had nothing to serve. directdl IS the fetch,
/// so marking every call blocked releases that would have played instantly — the second episode of a pack
/// could not be resolved for twenty minutes.
#[tokio::test]
#[ignore = "needs package 2: the Premiumize Store impl and refusal.rs (alreadyQueued)"]
async fn premiumize_marks_only_an_empty_answer() {
    // Enable when the owning package lands: move the body up and delete this wrapper.
    #[cfg(any())]
    async fn pending() {
        let _budget = install_budget(HOUR, 50);

        let cache = new_cache();
        let served = pm(
            "tok",
            doer(|_| {
                Ok(resp(
                    200,
                    r#"{"status":"success","content":[{"path":"m.mkv","link":"https://pm/l","size":9}]}"#,
                ))
            }),
            Some(cache.clone()),
        );
        let first = served.resolve(&tgt(H)).await;
        assert!(first.is_ok(), "a release premiumize can serve must resolve: {first:?}");
        assert!(
            !already_queued(&*cache, "tok", H),
            "a release that was SERVED was marked as queued; the next request would be refused"
        );
        let second = served.resolve(&tgt(H)).await;
        assert!(second.is_ok(), "the same release could not be resolved twice: {second:?}");

        // An empty answer means a transfer was queued: remember it, so the next poll does not pay again —
        // but keep asking, because directdl is the only thing that can notice it finished.
        let before = global_add_budget().remaining(&budget_account(Premiumize, "pending"));
        let calls = Arc::new(AtomicUsize::new(0));
        let done = Arc::new(AtomicBool::new(false));
        let (c, dn) = (calls.clone(), done.clone());
        let pending_cache = new_cache();
        let queued = pm(
            "pending",
            doer(move |_| {
                c.fetch_add(1, Ordering::SeqCst);
                if dn.load(Ordering::SeqCst) {
                    return Ok(resp(
                        200,
                        r#"{"status":"success","content":[{"path":"m.mkv","link":"https://pm/done","size":9}]}"#,
                    ));
                }
                Ok(resp(200, r#"{"status":"success","content":[]}"#))
            }),
            Some(pending_cache.clone()),
        );
        for _ in 0..10 {
            let res = queued.resolve(&tgt(H)).await;
            assert!(
                matches!(&res, Err(StoreError::AddInFlight)),
                "a queued transfer must read as coming, not dead: {res:?}"
            );
        }
        let spent = before - global_add_budget().remaining(&budget_account(Premiumize, "pending"));
        assert_eq!(spent, 1, "paid for the same transfer {spent} times");
        let n = calls.load(Ordering::SeqCst);
        assert!(n >= 10, "only asked {n} times: nothing else can notice the transfer finished");

        // And when it does finish, the very next poll serves it.
        done.store(true, Ordering::SeqCst);
        let link = queued.resolve(&tgt(H)).await;
        assert!(
            matches!(&link, Ok(l) if l == "https://pm/done"),
            "a completed transfer must be served: {link:?}"
        );
        assert!(!already_queued(&*pending_cache, "pending", H), "the marker outlived the transfer it described");
    }
    todo!("needs package 2: the Premiumize Store impl and refusal.rs (alreadyQueued)");
}

/// A remembered Real-Debrid torrent is per FILE, and its link is the one for the file selected.
///
/// One infohash is a whole season pack. Reusing a torrent id across episodes re-selected files on a
/// torrent RD had already started and then served links[0] regardless — S01E01's link for a request for
/// S01E02. Silently the wrong episode, which is worse than the add it saved.
#[tokio::test]
#[ignore = "needs package 2: the Real-Debrid Store impl (remembered torrent and rdLinkFor)"]
async fn real_debrid_serves_the_episode_that_was_asked_for() {
    // Enable when the owning package lands: move the body up and delete this wrapper.
    #[cfg(any())]
    async fn pending() {
        let _budget = install_budget(HOUR, 50);

        // A pack RD has already fully selected: two files, two links, in file order.
        let pack = r#"{"files":[{"id":1,"path":"/Show.S01E01.mkv","bytes":9,"selected":1},
                       {"id":2,"path":"/Show.S01E02.mkv","bytes":9,"selected":1}],
              "links":["https://rd/E01","https://rd/E02"]}"#;
        let unrestricted = Arc::new(Mutex::new(String::new()));
        let u = unrestricted.clone();
        let d = doer(move |req| {
            let path = req.url().path();
            if path.contains("addMagnet") {
                return Ok(resp(201, r#"{"id":"t1"}"#));
            }
            if path.contains("/torrents/info/") {
                return Ok(resp(200, pack));
            }
            if path.contains("selectFiles") {
                return Ok(resp(204, ""));
            }
            if path.contains("unrestrict/link") {
                *u.lock().unwrap() = body_of(&req);
                return Ok(resp(200, r#"{"download":"https://rd/dl"}"#));
            }
            Ok(resp(404, "{}"))
        });
        let s = rd("tok", d, Some(new_cache()));

        let e1 = s.resolve(&ResolveTarget { season: intp(1), episode: intp(1), ..tgt(H) }).await;
        assert!(e1.is_ok(), "E01: {e1:?}");
        let un = unrestricted.lock().unwrap().clone();
        assert!(un.contains("E01"), "E01 unrestricted {un:?}");
        let e2 = s.resolve(&ResolveTarget { season: intp(1), episode: intp(2), ..tgt(H) }).await;
        assert!(e2.is_ok(), "E02: {e2:?}");
        let un = unrestricted.lock().unwrap().clone();
        assert!(un.contains("E02"), "E02 unrestricted {un:?} — the pack's first link was served for the second episode");
    }
    todo!("needs package 2: the Real-Debrid Store impl (remembered torrent and rdLinkFor)");
}

/// rdLinkFor maps a file id onto RD's links, which describe the SELECTED files in file order.
///
/// The test's own name is the Go name, which is also the name of the function it calls, so the call is
/// written through the stand-in module's path.
#[test]
#[ignore = "needs package 2: stores/realdebrid.rs (rdLinkFor)"]
fn rd_link_for() {
    // Enable when the owning package lands: move the body up and delete this wrapper.
    #[cfg(any())]
    fn pending() {
        let info = RdInfo {
            links: vec!["L-b".into(), "L-d".into()],
            files: vec![
                RdFile { id: 1, path: "a".into(), bytes: 0, selected: 0 },
                RdFile { id: 2, path: "b".into(), bytes: 0, selected: 1 },
                RdFile { id: 3, path: "c".into(), bytes: 0, selected: 0 },
                RdFile { id: 4, path: "d".into(), bytes: 0, selected: 1 },
            ],
        };
        assert_eq!(foreign::rd_link_for(&info, 2), Some("L-b".to_string()), "first selected file → first link");
        assert_eq!(foreign::rd_link_for(&info, 4), Some("L-d".to_string()), "second selected file → second link");
        // An unselected file has no link of its own; guessing one is what served the wrong episode.
        assert!(foreign::rd_link_for(&info, 3).is_none(), "an unselected file must not be given someone else's link");
        // A leaner response with no selection flags and exactly one link has only one possible answer.
        let lean = RdInfo {
            links: vec!["only".into()],
            files: vec![RdFile { id: 7, path: "x".into(), bytes: 0, selected: 0 }],
        };
        assert_eq!(foreign::rd_link_for(&lean, 7), Some("only".to_string()), "single-link fallback");
    }
    todo!("needs package 2: stores/realdebrid.rs (rdLinkFor)");
}

/// The two keys HEAD added carry account state and must be scoped to the token like every other one.
#[test]
#[ignore = "needs packages 2: stores/realdebrid.rs and refusal.rs (the cache keys)"]
fn cache_keys_the_new_ones_are_scoped_too() {
    // Enable when the owning package lands: move the body up and delete this wrapper.
    #[cfg(any())]
    fn pending() {
        let cases: [(&str, fn(&str) -> String); 2] = [
            ("rdTorrent", |t| rd_torrent_key(t, H, &tgt(H))),
            ("pmQueued", |t| pm_queued_key(t, H)),
        ];
        for (name, key) in cases {
            let mine = key("my-token");
            let theirs = key("their-token");
            assert_ne!(mine, theirs, "{name}: two accounts share the key {mine:?}");
            assert!(
                !mine.contains("my-token") && mine.contains(&key_hash("my-token")),
                "{name}: key not derived from the account without exposing it: {mine:?}"
            );
        }
        // And the RD key separates episodes of one pack, or a season pack serves one episode for all of them.
        let e1 = rd_torrent_key("tok", H, &ResolveTarget { season: intp(1), episode: intp(1), ..tgt(H) });
        let e2 = rd_torrent_key("tok", H, &ResolveTarget { season: intp(1), episode: intp(2), ..tgt(H) });
        assert_ne!(e1, e2, "two episodes of one pack share a remembered torrent");
    }
    todo!("needs packages 2: stores/realdebrid.rs and refusal.rs (the cache keys)");
}

/// Premiumize's "coming" has a deadline, and a release it already holds costs nothing.
///
/// Two failures either side of the same line. Refreshing the marker on every poll made 202 an absorbing
/// state: a dead magnet answered "downloading 0%" forever and the client could never fall through. And
/// charging for every directdl billed an add per play of a release the account already had — directdl is
/// a purchase for what Premiumize lacks and a plain read for what it holds, and only the answer says
/// which.
#[tokio::test]
#[ignore = "needs package 2: the Premiumize Store impl and refusal.rs (pmQueuedKey, pendingGiveUp)"]
async fn premiumize_pending_expires_and_held_releases_are_free() {
    // Enable when the owning package lands: move the body up and delete this wrapper.
    #[cfg(any())]
    async fn pending() {
        let _budget = install_budget(HOUR, 50);
        let acct = budget_account(Premiumize, "tok");

        // A release Premiumize holds: served, and net zero against the allowance however often it is played.
        let held = pm(
            "tok",
            doer(|_| {
                Ok(resp(
                    200,
                    r#"{"status":"success","content":[{"path":"m.mkv","link":"https://pm/l","size":9}]}"#,
                ))
            }),
            Some(new_cache()),
        );
        for _ in 0..24 {
            // a season of a pack the account already has
            let res = held.resolve(&tgt(H)).await;
            assert!(res.is_ok(), "a held release must serve: {res:?}");
        }
        let left = global_add_budget().remaining(&acct);
        assert_eq!(left, 50, "allowance {left} after 24 plays of a release the account already holds");

        // A transfer that never produces anything: "coming" until the deadline, then dead so the client can
        // try something else.
        let pending = new_cache();
        let stuck = pm(
            "tok",
            doer(|_| Ok(resp(200, r#"{"status":"success","content":[]}"#))),
            Some(pending.clone()),
        );
        let res = stuck.resolve(&tgt(H)).await;
        assert!(matches!(&res, Err(StoreError::AddInFlight)), "a freshly queued transfer is coming: {res:?}");
        // Backdate the stamp past the give-up window — the marker records WHEN, so it can age.
        let stale = stamp_at(SystemTime::now() - pending_give_up() - Duration::from_secs(60));
        pending.put(&pm_queued_key("tok", H), &stale, queued_ttl());
        let res = stuck.resolve(&tgt(H)).await;
        assert!(
            !matches!(&res, Err(StoreError::AddInFlight)),
            "a transfer that never arrived is still reported as coming; the client cannot move on"
        );
        assert!(res.is_err(), "it produced nothing, so it cannot be a success either");
    }
    todo!("needs package 2: the Premiumize Store impl and refusal.rs (pmQueuedKey, pendingGiveUp)");
}

/// A refused key is the SERVICE declining this account, not a verdict on the release — so it backs off
/// instead of paying for another attempt every poll.
#[tokio::test]
#[ignore = "needs package 2: stores/premiumize.rs, realdebrid.rs and refusal.rs (storeRefusedUs, the backoff)"]
async fn store_refused_us_covers_an_expired_key() {
    // Enable when the owning package lands: move the body up and delete this wrapper.
    #[cfg(any())]
    async fn pending() {
        for code in [401u16, 403, 429, 500] {
            assert!(store_refused_us(code), "http {code} is the service refusing us, not a dead release");
        }
        for code in [200u16, 404] {
            assert!(!store_refused_us(code), "http {code} is an answer about the release, not a refusal");
        }

        // End to end: a stale token must cost one add and one backoff, not one per poll.
        let _budget = install_budget(HOUR, 50);
        let calls = Arc::new(AtomicUsize::new(0));
        let c = calls.clone();
        let s = pm(
            "stale",
            doer(move |_| {
                c.fetch_add(1, Ordering::SeqCst);
                Ok(resp(403, r#"{"status":"error","message":"invalid apikey"}"#))
            }),
            Some(new_cache()),
        );
        for _ in 0..10 {
            let _ = s.resolve(&tgt(H)).await;
        }
        let n = calls.load(Ordering::SeqCst);
        assert_eq!(n, 1, "asked a service with a bad key {n} times");
        let left = global_add_budget().remaining(&budget_account(Premiumize, "stale"));
        assert!(left >= 49, "a stale key drained the allowance to {left}");
    }
    todo!("needs package 2: stores/premiumize.rs, realdebrid.rs and refusal.rs (storeRefusedUs, the backoff)");
}

/// A pack Premiumize ALREADY HOLDS costs nothing, even when it cannot serve the episode asked for.
///
/// The refund sat behind the file-selection checks, so the two failures a season pack actually produces —
/// the pack does not contain the requested episode, or the entry carries no link — returned before it.
/// Every poll charged another add while queueing nothing: twenty polls, twenty adds, the hourly allowance
/// gone in under two minutes, on the branch season packs always take. The movie branch was fine, and the
/// movie branch was all that was tested.
#[tokio::test]
#[ignore = "needs package 2: stores/premiumize.rs (a held pack and the refund)"]
async fn premiumize_a_held_pack_costs_nothing_even_when_it_cannot_serve_the_episode() {
    // Enable when the owning package lands: move the body up and delete this wrapper.
    #[cfg(any())]
    async fn pending() {
        let _budget = install_budget(HOUR, 50);

        // The account holds this pack; it just does not contain S01E09.
        let s = pm(
            "tok",
            doer(|_| {
                Ok(resp(
                    200,
                    r#"{"status":"success","content":[
                    {"path":"Show.S01E01.mkv","link":"https://pm/1","size":9},
                    {"path":"Show.S01E02.mkv","link":"https://pm/2","size":9}]}"#,
                ))
            }),
            Some(new_cache()),
        );
        for _ in 0..20 {
            let res = s.resolve(&ResolveTarget { season: intp(1), episode: intp(9), ..tgt(H) }).await;
            assert!(res.is_err(), "the pack does not hold that episode; it must not resolve");
        }
        let left = global_add_budget().remaining(&budget_account(Premiumize, "tok"));
        assert_eq!(left, 50, "allowance {left} after twenty polls of a pack the account already holds");
    }
    todo!("needs package 2: stores/premiumize.rs (a held pack and the refund)");
}

/// A key the service refuses is an ACCOUNT condition, so it backs the account off — not one release at a
/// time while every other release pays for the same 403.
///
/// Keyed per infohash, sixty distinct releases made fifty calls and emptied the hourly allowance, and
/// replacing the key did not restore service because the budget stayed spent for the rest of the hour.
#[tokio::test]
#[ignore = "needs package 2: stores/premiumize.rs and refusal.rs (accountBackedOff, recordRefusal)"]
async fn store_refusal_a_rejected_key_backs_off_the_whole_account() {
    // Enable when the owning package lands: move the body up and delete this wrapper.
    #[cfg(any())]
    async fn pending() {
        let _budget = install_budget(HOUR, 50);

        let calls = Arc::new(AtomicUsize::new(0));
        let c = calls.clone();
        let cache = new_cache();
        let s = pm(
            "stale",
            doer(move |_| {
                c.fetch_add(1, Ordering::SeqCst);
                Ok(resp(403, r#"{"status":"error","message":"invalid apikey"}"#))
            }),
            Some(cache.clone()),
        );
        for i in 0..60 {
            // sixty DIFFERENT releases, as a browse would produce
            let _ = s.resolve(&tgt(&format!("{i:040x}"))).await;
        }
        let n = calls.load(Ordering::SeqCst);
        assert_eq!(n, 1, "asked a service that had rejected the key {n} times");
        let left = global_add_budget().remaining(&budget_account(Premiumize, "stale"));
        assert!(left >= 49, "a rejected key drained the allowance to {left}; replacing it would not restore service");
        // A per-release refusal must still be per-release, or one dead torrent silences the account.
        let per_release = new_cache();
        record_refusal(
            &*per_release,
            TorBox,
            "tok",
            H,
            &StoreError::Unavailable(StoreUnavailableError {
                service: TorBox,
                reason: "createtorrent http 429".into(),
                status: 429,
            }),
        );
        assert!(
            backed_off(&*per_release, TorBox, "tok", &repeat("c", 40)).is_none(),
            "a 429 about one release backed off the whole account"
        );
    }
    todo!("needs package 2: stores/premiumize.rs and refusal.rs (accountBackedOff, recordRefusal)");
}

/// The give-up deadline sits strictly inside the marker's life, or it can never be reached: the marker
/// expires first, the next poll queues the same doomed transfer, and "coming" becomes absorbing again.
#[test]
#[ignore = "needs package 2: stores/refusal.rs (pendingGiveUp, queuedTTL, pendingTooLong)"]
fn premiumize_give_up_happens_before_the_marker_expires() {
    // Enable when the owning package lands: move the body up and delete this wrapper.
    #[cfg(any())]
    fn pending() {
        assert!(
            pending_give_up() < queued_ttl(),
            "pendingGiveUp ({:?}) must be shorter than queuedTTL ({:?}), or the deadline is unreachable",
            pending_give_up(),
            queued_ttl()
        );
        let cache = new_cache();
        // Stamped a moment ago: still believable.
        cache.put(&pm_queued_key("tok", H), &stamp_at(SystemTime::now()), queued_ttl());
        assert!(!pending_too_long(&*cache, "tok", H), "a transfer queued seconds ago has not had its chance yet");
        // Stamped just inside the marker's life but past the deadline: no longer believable, and reachable.
        let old = SystemTime::now() - (queued_ttl() - Duration::from_secs(60));
        cache.put(&pm_queued_key("tok", H), &stamp_at(old), queued_ttl());
        assert!(
            pending_too_long(&*cache, "tok", H),
            "a transfer still marked but long past its deadline is being reported as coming"
        );
    }
    todo!("needs package 2: stores/refusal.rs (pendingGiveUp, queuedTTL, pendingTooLong)");
}

/// Giving up on a stuck transfer is REMEMBERED, not merely reached.
///
/// Without that, the marker expired at queuedTTL and the next poll queued the same doomed transfer for
/// another ten-minute window — and the client keeps polling a release it was told was dead, so it would.
#[tokio::test]
#[ignore = "needs package 2: the Premiumize Store impl and refusal.rs (the give-up verdict)"]
async fn premiumize_the_give_up_verdict_is_remembered() {
    // Enable when the owning package lands: move the body up and delete this wrapper.
    #[cfg(any())]
    async fn pending() {
        let _budget = install_budget(HOUR, 50);

        let cache = new_cache();
        let s = pm(
            "tok",
            doer(|_| Ok(resp(200, r#"{"status":"success","content":[]}"#))),
            Some(cache.clone()),
        );
        // Queued long ago and still producing nothing.
        let old = SystemTime::now() - (queued_ttl() - Duration::from_secs(60));
        cache.put(&pm_queued_key("tok", H), &stamp_at(old), queued_ttl());

        assert!(s.resolve(&tgt(H)).await.is_err(), "a transfer past its deadline is not a success");

        // Remembered — but in the QUEUE marker, never as a store refusal. Filing it as a refusal made every
        // following poll a 503 naming Premiumize, which tells the viewer their debrid is refusing and stops
        // the client trying other sources — for a release scout itself condemned, on a healthy store. The
        // whole point of the dead link is that the client moves on.
        assert!(
            backed_off(&*cache, Premiumize, "tok", H).is_none(),
            "the give-up was filed as a store refusal; the client can no longer fall through"
        );
        for i in 0..5 {
            let res = s.resolve(&tgt(H)).await;
            assert!(unavailable(&res).is_none(), "poll {i} blamed the store for a verdict scout reached: {res:?}");
            assert!(res.is_err(), "poll {i} resolved a transfer that produced nothing");
        }
        // And it is not re-queued: the marker keeps suppressing the charge for its full life.
        let left = global_add_budget().remaining(&budget_account(Premiumize, "tok"));
        assert_eq!(left, 50, "allowance {left} — a condemned transfer was queued again");
    }
    todo!("needs package 2: the Premiumize Store impl and refusal.rs (the give-up verdict)");
}

/// Whether a refusal is about the ACCOUNT is decided by the status code, not by matching digits in prose.
///
/// The reason string embeds the service's own words — including up to 200 bytes of a non-JSON body — so
/// substring-matching "401"/"403" classified a rate-limit detail ("retry in 403 ms") and a Cloudflare Ray
/// ID as a rejected key, and silenced a healthy account for a minute while logging that the key was bad.
/// Deriving a fact from prose when the fact is available at the call site is what isPermanentFailure was
/// deleted for.
#[test]
#[ignore = "needs package 2: stores/refusal.rs (refusalIsAboutTheAccount, recordRefusal, backedOff)"]
fn refusal_is_about_the_account_reads_the_status_not_the_prose() {
    // Enable when the owning package lands: move the body up and delete this wrapper.
    #[cfg(any())]
    fn pending() {
        let cases: [(&str, StoreUnavailableError, bool); 5] = [
            (
                "a rejected key",
                StoreUnavailableError { service: TorBox, status: 403, reason: "createtorrent http 403 (FORBIDDEN)".into() },
                true,
            ),
            (
                "an expired key",
                StoreUnavailableError { service: TorBox, status: 401, reason: "createtorrent http 401".into() },
                true,
            ),
            (
                "a rate limit whose DETAIL contains 403",
                StoreUnavailableError {
                    service: TorBox,
                    status: 429,
                    reason: "createtorrent http 429 (RATE_LIMIT: slow down, retry in 403 ms)".into(),
                },
                false,
            ),
            (
                "a 502 body carrying a Ray ID with 401 in it",
                StoreUnavailableError {
                    service: TorBox,
                    status: 502,
                    reason: "createtorrent http 502 (Cloudflare Ray ID: 8a1b401c9d2e4f01)".into(),
                },
                false,
            ),
            (
                "a queue depth that looks like a status",
                StoreUnavailableError {
                    service: TorBox,
                    status: 429,
                    reason: "createtorrent http 429 (queued behind 4031 jobs)".into(),
                },
                false,
            ),
        ];
        for (name, err, want_ac) in cases {
            let got = refusal_is_about_the_account(&StoreError::Unavailable(err));
            assert_eq!(got, want_ac, "{name}: account-level = {got}, want {want_ac}");
        }

        // End to end: a 429 whose text contains 403 must back off ONE release, not the account.
        let cache = new_cache();
        record_refusal(
            &*cache,
            TorBox,
            "tok",
            H,
            &StoreError::Unavailable(StoreUnavailableError {
                service: TorBox,
                status: 429,
                reason: "createtorrent http 429 (retry in 403 ms)".into(),
            }),
        );
        assert!(
            backed_off(&*cache, TorBox, "tok", &repeat("c", 40)).is_none(),
            "a rate limit about one release silenced the whole account"
        );
    }
    todo!("needs package 2: stores/refusal.rs (refusalIsAboutTheAccount, recordRefusal, backedOff)");
}

/// The add-path guards must not block a read.
///
/// A release the account already holds needs no add, so a backoff — which describes the cost of adding,
/// and which is now account-wide for a rejected key — must not stand in its way. TorBox has ordered it
/// this way all along; Real-Debrid and Premiumize checked the backoff first, so one 401 on an unrelated
/// release blocked every release the account held, read-only probes included.
#[tokio::test]
#[ignore = "needs packages 2: the Real-Debrid and Premiumize Store impls (the read path ordering)"]
async fn resolve_add_guards_do_not_block_a_read_path() {
    // Enable when the owning package lands: move the body up and delete this wrapper.
    #[cfg(any())]
    async fn pending() {
        let _budget = install_budget(HOUR, 50);

        let cache = new_cache();
        // A PER-RELEASE add backoff on some other release: a throttle, not a rejected key. This is the kind a
        // read cannot have caused and must not be blocked by. (An account-level 401/403 is different, and
        // does gate reads — asking again with a rejected key is pointless whatever is being asked for.)
        record_refusal(
            &*cache,
            RealDebrid,
            "tok",
            &repeat("f", 40),
            &StoreError::Unavailable(StoreUnavailableError {
                service: RealDebrid,
                status: 429,
                reason: "addmagnet http 429".into(),
            }),
        );

        let d = doer(|req| {
            let path = req.url().path();
            if path.contains("/torrents/info/") {
                return Ok(resp(200, r#"{"files":[{"id":1,"path":"/movie.mkv","bytes":9,"selected":1}],"links":["https://rd/r"]}"#));
            }
            if path.contains("selectFiles") {
                return Ok(resp(204, ""));
            }
            if path.contains("unrestrict/link") {
                return Ok(resp(200, r#"{"download":"https://rd/dl.mkv"}"#));
            }
            Ok(resp(404, "{}"))
        });
        let s = rd("tok", d, Some(cache.clone()));
        let target = tgt(H);
        s.remember_torrent(&target, "t1"); // this one is already bought

        let link = s.resolve(&target).await;
        assert!(
            matches!(&link, Ok(l) if l == "https://rd/dl.mkv"),
            "a release the account already holds was blocked by an add-path backoff: {link:?}"
        );

        // A NoAdd caller is likewise answered before the backoff, on a release that is NOT held.
        let no_add = s.resolve(&ResolveTarget { no_add: true, ..tgt(&repeat("b", 40)) }).await;
        assert!(
            matches!(&no_add, Err(StoreError::DeadLink(d)) if d.reason == ERR_WOULD_ADD),
            "a read-only resolve was blocked by an add-path guard: {no_add:?}"
        );

        // Premiumize orders it the same way. It has no held-torrent path, so NoAdd is the read here — and it
        // must be answered before an account backoff it cannot have caused.
        let pm_cache = new_cache();
        record_refusal(
            &*pm_cache,
            Premiumize,
            "tok",
            &repeat("f", 40),
            &StoreError::Unavailable(StoreUnavailableError {
                service: Premiumize,
                status: 429,
                reason: "directdl http 429".into(),
            }),
        );
        let pm_store = pm(
            "tok",
            doer(|_| {
                panic!("a NoAdd resolve must not reach the service at all");
            }),
            Some(pm_cache),
        );
        let pm_res = pm_store.resolve(&ResolveTarget { no_add: true, ..tgt(H) }).await;
        assert!(
            matches!(&pm_res, Err(StoreError::DeadLink(d)) if d.reason == ERR_WOULD_ADD),
            "premiumize blocked a read-only resolve with an add-path backoff: {pm_res:?}"
        );
    }
    todo!("needs packages 2: the Real-Debrid and Premiumize Store impls (the read path ordering)");
}

/// A transfer already queued is never PAID FOR again, however long it goes on failing.
///
/// The bound that matters is the charge, not the call: directdl is both the purchase and the only way to
/// notice the transfer finished, so it has to keep being made — an earlier version suppressed the call
/// instead and made a late-completing transfer permanently unplayable.
#[tokio::test]
#[ignore = "needs package 2: the Premiumize Store impl (the queued transfer and its charge)"]
async fn premiumize_a_queued_transfer_is_never_charged_twice() {
    // Enable when the owning package lands: move the body up and delete this wrapper.
    #[cfg(any())]
    async fn pending() {
        let _budget = install_budget(HOUR, 50);

        let calls = Arc::new(AtomicUsize::new(0));
        let c = calls.clone();
        let s = pm(
            "tok",
            doer(move |_| {
                c.fetch_add(1, Ordering::SeqCst);
                Ok(resp(200, r#"{"status":"success","content":[]}"#))
            }),
            Some(new_cache()),
        );
        for i in 0..30 {
            let res = s.resolve(&tgt(H)).await;
            assert!(res.is_err(), "poll {i} resolved a transfer that produced nothing");
        }
        let n = calls.load(Ordering::SeqCst);
        assert_eq!(n, 30, "asked {n} times; the call is what notices a transfer finishing and must keep happening");
        // One charge for the one transfer that was genuinely queued, and none for the twenty-nine repeats.
        let left = global_add_budget().remaining(&budget_account(Premiumize, "tok"));
        assert_eq!(left, 49, "allowance {left} — a queued transfer was paid for more than once");
    }
    todo!("needs package 2: the Premiumize Store impl (the queued transfer and its charge)");
}

/// A rejected key DOES gate a read: asking again with a key the service refuses is pointless whatever is
/// being asked for, and without this the read path recorded the refusal and asked again on the next poll.
#[tokio::test]
#[ignore = "needs package 2: the Real-Debrid Store impl (the account gate on the read path)"]
async fn resolve_a_rejected_key_gates_even_a_read_path() {
    // Enable when the owning package lands: move the body up and delete this wrapper.
    #[cfg(any())]
    async fn pending() {
        let cache = new_cache();
        record_refusal(
            &*cache,
            RealDebrid,
            "tok",
            &repeat("f", 40),
            &StoreError::Unavailable(StoreUnavailableError {
                service: RealDebrid,
                status: 403,
                reason: "addmagnet http 403".into(),
            }),
        );
        let s = rd(
            "tok",
            doer(|_| {
                panic!("a rejected key must stop the request before it is made");
            }),
            Some(cache),
        );
        let target = tgt(H);
        s.remember_torrent(&target, "t1"); // even a release the account holds
        assert!(s.resolve(&target).await.is_err(), "a rejected key cannot serve anything");
    }
    todo!("needs package 2: the Real-Debrid Store impl (the account gate on the read path)");
}

/// Real-Debrid's READ path classifies a refusal as a refusal.
///
/// Promoting the held-torrent path ahead of the backoff copied TorBox's ordering without the
/// classification that makes it safe: RD mapped every non-2xx to a dead link, so an expired key or a
/// throttle — on a release RD demonstrably holds — reached the app as "this release does not exist", with
/// no backoff recorded and the account backoff shadowed by the very reordering.
#[tokio::test]
#[ignore = "needs package 2: the Real-Debrid Store impl (the read path classification)"]
async fn real_debrid_the_read_path_reports_a_refusal_as_a_refusal() {
    // Enable when the owning package lands: move the body up and delete this wrapper.
    #[cfg(any())]
    async fn pending() {
        let _budget = install_budget(HOUR, 50);

        let calls = Arc::new(AtomicUsize::new(0));
        let c = calls.clone();
        let cache = new_cache();
        let s = rd(
            "tok",
            doer(move |_| {
                c.fetch_add(1, Ordering::SeqCst);
                Ok(resp(403, r#"{"error":"bad_token"}"#))
            }),
            Some(cache.clone()),
        );
        let target = tgt(H);
        s.remember_torrent(&target, "t1"); // the account holds this one

        let err = s.resolve(&target).await;
        assert!(unavailable(&err).is_some(), "a rejected key is the service refusing, not a dead release: {err:?}");
        // Remembered, so the poll loop stops — and account-wide, because a 403 is about the key.
        for _ in 0..9 {
            let _ = s.resolve(&target).await;
        }
        let n = calls.load(Ordering::SeqCst);
        assert_eq!(n, 1, "asked a service that had rejected the key {n} times");
        assert!(
            account_backed_off(&*cache, RealDebrid, "tok").is_some(),
            "a rejected key must back off the account, not one release"
        );
    }
    todo!("needs package 2: the Real-Debrid Store impl (the read path classification)");
}

/// A transfer Premiumize LATER completes must still be discoverable.
///
/// Answering a condemned transfer without asking looked like a saving and was not: directdl is the only
/// thing that can observe the transfer finishing, so short-circuiting it made a 4K remux slower than the
/// give-up window permanently unplayable until the marker expired. The call is already unbilled once the
/// transfer is queued, so suppressing it bought nothing.
#[tokio::test]
#[ignore = "needs package 2: the Premiumize Store impl (a condemned transfer is still asked about)"]
async fn premiumize_a_slow_transfer_is_still_discovered_when_it_completes() {
    // Enable when the owning package lands: move the body up and delete this wrapper.
    #[cfg(any())]
    async fn pending() {
        let _budget = install_budget(HOUR, 50);

        let done = Arc::new(AtomicBool::new(false));
        let calls = Arc::new(AtomicUsize::new(0));
        let (d, c) = (done.clone(), calls.clone());
        let cache = new_cache();
        let s = pm(
            "tok",
            doer(move |_| {
                c.fetch_add(1, Ordering::SeqCst);
                if d.load(Ordering::SeqCst) {
                    return Ok(resp(200, r#"{"status":"success","content":[{"path":"m.mkv","link":"https://pm/late","size":9}]}"#));
                }
                Ok(resp(200, r#"{"status":"success","content":[]}"#))
            }),
            Some(cache.clone()),
        );
        // Queued longer ago than the give-up window: it reads as dead, but is still being asked about.
        cache.put(
            &pm_queued_key("tok", H),
            &stamp_at(SystemTime::now() - pending_give_up() - Duration::from_secs(60)),
            queued_ttl(),
        );
        assert!(s.resolve(&tgt(H)).await.is_err(), "past its deadline it is not a success");
        let n = calls.load(Ordering::SeqCst);
        assert_eq!(n, 1, "a condemned transfer must still be asked about: {n} calls");

        // It finishes. The very next poll serves it.
        done.store(true, Ordering::SeqCst);
        let link = s.resolve(&tgt(H)).await;
        assert!(
            matches!(&link, Ok(l) if l == "https://pm/late"),
            "a transfer that completed after the deadline was never discovered: {link:?}"
        );
        // And it cost nothing extra: the transfer was already paid for.
        let left = global_add_budget().remaining(&budget_account(Premiumize, "tok"));
        assert_eq!(left, 50, "allowance {left} — an already-queued transfer was charged again");
    }
    todo!("needs package 2: the Premiumize Store impl (a condemned transfer is still asked about)");
}

/// The READ path records only a SERVICE refusal — not a torrent that is merely not ready, and not a
/// cancelled poll.
///
/// requestDownload flattens every transport failure into a DeadLinkError on purpose, to keep the token
/// out of the logs, which destroys the classification isCancellation needs. Recorded broadly, a cancelled
/// poll and a still-downloading torrent were both filed as TorBox refusing: the probe route then told the
/// viewer their debrid was refusing and stopped the client trying other sources, and a read-only probe
/// created a backoff its own contract says it cannot cause.
#[tokio::test]
#[ignore = "needs package 1: the TorBox Store impl (requestDownload's classification)"]
async fn tor_box_the_read_path_records_only_a_real_refusal() {
    // Enable when the owning package lands: move the body up and delete this wrapper.
    #[cfg(any())]
    async fn pending() {
        // (name, requestdl answer, want recorded)
        let cases: [(&str, fn() -> Reply, bool); 3] = [
            ("still downloading", || Ok(resp(200, r#"{"success":false}"#)), false),
            ("a cancelled poll", || Err(io::Error::other("context canceled").into()), false),
            ("the service refusing", || Ok(resp(429, r#"{"error":"RATE"}"#)), true),
        ];
        for (name, requestdl, want_record) in cases {
            let _budget = install_budget(HOUR, 50);
            let cache = new_cache();
            cache.put(&torrent_id_key("tok", H), "42", resolve_cache_ttl());
            let d = doer(move |req| {
                if req.url().path().contains("requestdl") {
                    return requestdl();
                }
                Ok(resp(200, r#"{"data":{"files":[{"id":0,"name":"m.mkv","size":9}]}}"#))
            });
            let s = tb("tok", d, Some(cache.clone()));
            let _ = s.resolve(&ResolveTarget { file_idx: Some(0), ..tgt(H) }).await;
            let recorded = backed_off(&*cache, TorBox, "tok", H).is_some();
            assert_eq!(recorded, want_record, "{name}: recorded={recorded} — this decides whether the app blames the debrid");
        }
    }
    todo!("needs package 1: the TorBox Store impl (requestDownload's classification)");
}

/// TorBox gets the same account-level gate as the other two: a rejected key makes every request
/// pointless, reads included.
#[tokio::test]
#[ignore = "needs package 1: the TorBox Store impl (the account gate)"]
async fn tor_box_a_rejected_key_gates_the_read_path_too() {
    // Enable when the owning package lands: move the body up and delete this wrapper.
    #[cfg(any())]
    async fn pending() {
        let cache = new_cache();
        record_refusal(
            &*cache,
            TorBox,
            "tok",
            &repeat("f", 40),
            &StoreError::Unavailable(StoreUnavailableError {
                service: TorBox,
                status: 403,
                reason: "createtorrent http 403".into(),
            }),
        );
        cache.put(&torrent_id_key("tok", H), "42", resolve_cache_ttl());
        let s = tb(
            "tok",
            doer(|_| {
                panic!("a rejected key must stop the request before it is made");
            }),
            Some(cache),
        );
        assert!(
            s.resolve(&ResolveTarget { file_idx: Some(0), ..tgt(H) }).await.is_err(),
            "a rejected key cannot serve even a held torrent"
        );
    }
    todo!("needs package 1: the TorBox Store impl (the account gate)");
}

/// Premiumize's account gate, which nothing pinned: the existing ordering test seeds a 429, which is a
/// per-release refusal and never reaches this branch.
#[tokio::test]
#[ignore = "needs package 2: the Premiumize Store impl (the account gate)"]
async fn premiumize_a_rejected_key_gates_even_a_read_path() {
    // Enable when the owning package lands: move the body up and delete this wrapper.
    #[cfg(any())]
    async fn pending() {
        let cache = new_cache();
        record_refusal(
            &*cache,
            Premiumize,
            "tok",
            &repeat("f", 40),
            &StoreError::Unavailable(StoreUnavailableError {
                service: Premiumize,
                status: 401,
                reason: "directdl http 401".into(),
            }),
        );
        let s = pm(
            "tok",
            doer(|_| {
                panic!("a rejected key must stop the request before it is made");
            }),
            Some(cache),
        );
        assert!(s.resolve(&tgt(H)).await.is_err(), "a rejected key cannot serve anything");
    }
    todo!("needs package 2: the Premiumize Store impl (the account gate)");
}

/// Premiumize answers an application-level refusal as HTTP 200 `{"status":"error",…}` — an unsupported
/// magnet, an account limit, no space. Only a SUCCESSFUL answer carrying no content means a transfer was
/// queued. Collapsing the two charged an add for a transfer that does not exist, stamped the queue marker
/// for its full twenty minutes so a later legitimate attempt was suppressed as "already queued", and made
/// /play answer 202 "downloading, 0%" for ten minutes — a spinner the client cannot fall through — for a
/// magnet Premiumize had refused outright.
#[tokio::test]
#[ignore = "needs package 2: the Premiumize Store impl (a refused magnet is not a queued transfer)"]
async fn premiumize_a_refused_magnet_is_not_a_queued_transfer() {
    // Enable when the owning package lands: move the body up and delete this wrapper.
    #[cfg(any())]
    async fn pending() {
        let _budget = install_budget(HOUR, 50);

        let cache = new_cache();
        let calls = Arc::new(AtomicUsize::new(0));
        let c = calls.clone();
        let s = pm(
            "tok",
            doer(move |_| {
                c.fetch_add(1, Ordering::SeqCst);
                Ok(resp(200, r#"{"status":"error","message":"Invalid src"}"#))
            }),
            Some(cache.clone()),
        );
        // The FIRST answer is the one the viewer acts on; the polls behind it meet the backoff.
        let first = s.resolve(&tgt(H)).await;
        assert!(!matches!(&first, Err(StoreError::AddInFlight)), "a refused magnet was reported as a transfer on its way: {first:?}");
        let dead = match &first {
            Err(StoreError::DeadLink(d)) => d.clone(),
            other => panic!("got {other:?}, want a dead link the client can move past"),
        };
        for _ in 0..29 {
            let _ = s.resolve(&tgt(H)).await;
        }
        // This is where the loop shows, and where an allowance assertion cannot: HTTP 200 `{"status":"error"}`
        // is how Premiumize reports an unsupported magnet, an account at its limit and "no space", and this
        // branch alone recorded nothing — so thirty polls made thirty real calls while the refund kept the
        // allowance at fifty the whole way.
        let n = calls.load(Ordering::SeqCst);
        assert!(n <= 1, "made {n} directdl calls over 30 polls — nothing bounds the loop");
        // Its own words, so "Invalid src" and "not enough space" stay tellable apart.
        assert!(dead.to_string().contains("Invalid src"), "the service's own explanation was discarded: {dead}");
        assert!(!already_queued(&*cache, "tok", H), "a magnet Premiumize refused was marked as queued, blocking any retry for twenty minutes");
        let left = global_add_budget().remaining(&budget_account(Premiumize, "tok"));
        assert_eq!(left, 50, "allowance {left} — refusals were charged as adds");
    }
    todo!("needs package 2: the Premiumize Store impl (a refused magnet is not a queued transfer)");
}

/// Real-Debrid's read path records ONLY a service refusal, the same narrowing TorBox's has. `info` also
/// hands back raw transport errors, a bare `info http 400`, and `bad info` for a non-JSON 200 (a
/// Cloudflare interstitial). Filing those as refusals made a TCP reset answer 503 store_unavailable
/// naming realdebrid — which stops the client trying other sources — for a release RD demonstrably holds,
/// and because this path runs on every poll and re-stamps the record, it never lapsed.
#[tokio::test]
#[ignore = "needs package 2: the Real-Debrid Store impl (the read path's classification)"]
async fn real_debrid_only_a_service_refusal_is_recorded_on_the_read_path() {
    // Enable when the owning package lands: move the body up and delete this wrapper.
    #[cfg(any())]
    async fn pending() {
        let cases: [(&str, fn() -> Reply, bool); 4] = [
            ("a transport blip", || Err("read tcp 10.0.0.2:443: connection reset by peer".into()), false),
            ("a 400", || Ok(resp(400, "{}")), false),
            ("an interstitial", || Ok(resp(200, "<html>just a moment</html>")), false),
            ("the service refusing", || Ok(resp(403, r#"{"error":"bad_token"}"#)), true),
        ];
        for (name, respond, want_record) in cases {
            let cache = new_cache();
            let s = rd("tok", doer(move |_| respond()), Some(cache.clone()));
            let target = tgt(H);
            s.remember_torrent(&target, "t1");
            assert!(s.resolve(&target).await.is_err(), "{name}: nothing here resolves");
            let recorded = backed_off(&*cache, RealDebrid, "tok", H).is_some();
            assert_eq!(recorded, want_record, "{name}: recorded={recorded} — this decides whether the app blames the debrid");
        }
    }
    todo!("needs package 2: the Real-Debrid Store impl (the read path's classification)");
}

/// The account gate has to sit ABOVE the warm resolve entry, not below it. Any pack played in the last
/// six hours takes that fast path and returns without reaching a gate placed after it — the normal state
/// during a binge — so ten polls made ten requestdl calls against a key TorBox had already rejected.
#[tokio::test]
#[ignore = "needs package 1: the TorBox Store impl (the warm resolve entry)"]
async fn tor_box_the_account_gate_covers_the_warm_fast_path() {
    // Enable when the owning package lands: move the body up and delete this wrapper.
    #[cfg(any())]
    async fn pending() {
        let cache = new_cache();
        record_refusal(
            &*cache,
            TorBox,
            "tok",
            &repeat("f", 40),
            &StoreError::Unavailable(StoreUnavailableError {
                service: TorBox,
                status: 403,
                reason: "createtorrent http 403".into(),
            }),
        );
        cache.put(
            &resolve_key("tok", H),
            r#"{"torrentId":42,"files":[{"Index":0,"Name":"Show.S01E01.mkv","SizeBytes":900}]}"#,
            resolve_cache_ttl(),
        );
        let s = tb(
            "tok",
            doer(|_| {
                panic!("a rejected key must stop the request before it is made, warm entry or not");
            }),
            Some(cache),
        );
        assert!(
            s.resolve(&ResolveTarget { file_idx: Some(0), ..tgt(H) }).await.is_err(),
            "a rejected key cannot serve even a pack played minutes ago"
        );
    }
    todo!("needs package 1: the TorBox Store impl (the warm resolve entry)");
}

/// The read path keeps the service's own words, the same rule the add path follows: an account at its
/// active-download limit and a torrent id TorBox cannot serve are both a bare 400, and only the text says
/// which. Discarding it left the read path unable to tell a genuine refusal from a dead link, so it
/// recorded nothing and re-asked on every poll.
#[tokio::test]
#[ignore = "needs package 1: the TorBox Store impl (requestDownload's error text)"]
async fn tor_box_the_read_path_keeps_the_services_own_words() {
    // Enable when the owning package lands: move the body up and delete this wrapper.
    #[cfg(any())]
    async fn pending() {
        let cache = new_cache();
        cache.put(&torrent_id_key("tok", H), "42", resolve_cache_ttl());
        let s = tb(
            "tok",
            doer(|req| {
                if req.url().path().contains("requestdl") {
                    return Ok(resp(
                        400,
                        r#"{"error":"DOWNLOAD_SERVER_ERROR","detail":"active download limit reached"}"#,
                    ));
                }
                Ok(resp(200, r#"{"data":{"files":[{"id":0,"name":"m.mkv","size":9}]}}"#))
            }),
            Some(cache),
        );
        let res = s.resolve(&ResolveTarget { file_idx: Some(0), no_add: true, ..tgt(H) }).await;
        let msg = res.as_ref().err().map(|e| e.to_string()).unwrap_or_default();
        assert!(msg.contains("active download limit reached"), "got {res:?} — the answer that says WHY was thrown away one layer down");
    }
    todo!("needs package 1: the TorBox Store impl (requestDownload's error text)");
}

/// A cached file list that names its episodes is proof about the pack, not a hint: if the requested one
/// is not among them, adding the torrent again only re-learns the same thing, on an account with an
/// hourly ceiling on adds.
#[tokio::test]
#[ignore = "needs package 1: the TorBox Store impl (selectFileID on a cached list)"]
async fn tor_box_a_proven_wrong_pack_is_not_re_added() {
    // Enable when the owning package lands: move the body up and delete this wrapper.
    #[cfg(any())]
    async fn pending() {
        let cache = new_cache();
        cache.put(
            &resolve_key("tok", H),
            r#"{"torrentId":42,"files":[{"Index":0,"Name":"Show.S01E01.mkv","SizeBytes":900},{"Index":1,"Name":"Show.S01E02.mkv","SizeBytes":950}]}"#,
            resolve_cache_ttl(),
        );
        let s = tb(
            "tok",
            doer(|req| {
                panic!("the cached list already proves this pack is wrong; {} was asked anyway", req.url().path());
            }),
            Some(cache),
        );
        let res = s.resolve(&ResolveTarget { season: intp(1), episode: intp(9), ..tgt(H) }).await;
        assert_eq!(res.expect_err("the cached list names no S01E09"), err_episode_not_in_torrent());
    }
    todo!("needs package 1: the TorBox Store impl (selectFileID on a cached list)");
}

/// Every answer Premiumize gives that is not a 2xx is one that queued nothing, so the charge goes back.
/// Keeping it was the expensive mistake: the allowance is a rolling in-memory window with no reset but a
/// restart, so one bad magnet polled every two seconds spent all fifty in about a hundred seconds — after
/// which spendAdd refused EVERY Premiumize resolve for the rest of the hour, including releases the
/// account already holds that directdl would have served instantly. A refused magnet is a nuisance; an
/// hour of 503 scout_busy on a working store is an outage.
#[tokio::test]
#[ignore = "needs package 2: the Premiumize Store impl (an answered failure is not a charge)"]
async fn premiumize_an_answered_failure_is_not_a_charge() {
    // Enable when the owning package lands: move the body up and delete this wrapper.
    #[cfg(any())]
    async fn pending() {
        for status in [400u16, 402, 404, 429, 500] {
            let _budget = install_budget(HOUR, 50);

            let cache = new_cache();
            let s = pm(
                "tok",
                doer(move |_| Ok(resp(status, r#"{"status":"error","message":"nope"}"#))),
                Some(cache.clone()),
            );
            for _ in 0..60 {
                let _ = s.resolve(&tgt(H)).await;
            }
            let left = global_add_budget().remaining(&budget_account(Premiumize, "tok"));
            assert_eq!(left, 50, "http {status}: allowance {left} — a poll loop on an answered failure spent the hour's adds");
            // The allowance still being there is only half of it: what it buys is that ANOTHER release —
            // one the account holds, which directdl serves as a read — still resolves, instead of being
            // refused by scout's own bookkeeping for the rest of the hour. A different infohash, because
            // a 429/5xx correctly backs THIS release off for a minute; the damage being pinned here is
            // the account-wide one.
            let held = pm(
                "tok",
                doer(|_| Ok(resp(200, r#"{"status":"success","content":[{"path":"m.mkv","link":"https://pm/ok"}]}"#))),
                Some(cache),
            );
            let other = held.resolve(&tgt(&repeat("b", 40))).await;
            assert!(matches!(&other, Ok(l) if l == "https://pm/ok"), "a held release answered {other:?} — scout refused what Premiumize would have served");
        }
    }
    todo!("needs package 2: the Premiumize Store impl (an answered failure is not a charge)");
}

/// A blip listing a pack's files must never fall through to the indexer's positional index. listFiles
/// answers nil for a transport error, a non-200 and an unreadable body alike, and that nil reached
/// selectFileID, where the raw fileIdx is used as a TorBox file id — serving S01E01 for a request for
/// S01E02 with a 302 and no error. The cache-read path already refuses on the same evidence.
#[tokio::test]
#[ignore = "needs package 1: the TorBox Store impl (listFiles and selectFileID)"]
async fn tor_box_a_failed_file_list_never_guesses_the_episode() {
    // Enable when the owning package lands: move the body up and delete this wrapper.
    #[cfg(any())]
    async fn pending() {
        // (name, mylist answer)
        let cases: [(&str, fn() -> Reply); 3] = [
            ("a transport blip", || Err("connection reset".into())),
            ("a 500", || Ok(resp(500, "{}"))),
            ("an unreadable body", || Ok(resp(200, "<html>"))),
        ];
        for (name, mylist) in cases {
            let cache = new_cache();
            cache.put(&torrent_id_key("tok", H), "42", resolve_cache_ttl());
            let d = doer(move |req| {
                if req.url().path().contains("mylist") {
                    return mylist();
                }
                Ok(resp(200, r#"{"success":true,"data":"https://tb/link"}"#))
            });
            let s = tb("tok", d, Some(cache.clone()));
            // The indexer says file 0; the pack's own names would have said file 1.
            let res = s
                .resolve(&ResolveTarget { season: intp(1), episode: intp(2), file_idx: intp(0), ..tgt(H) })
                .await;
            assert!(res.is_err(), "{name}: served blind — this is how the wrong episode plays: {res:?}");
            assert!(unavailable(&res).is_some(), "{name}: got {res:?}, want the store saying it could not answer");
            // Status still has to be able to report the download, so the id must survive the refusal.
            assert!(cache.get(&torrent_id_key("tok", H)).is_some(), "{name}: the torrent id was dropped, so a fetch in progress now reads as dead");
        }
    }
    todo!("needs package 1: the TorBox Store impl (listFiles and selectFileID)");
}

/// The one TorBox URL carrying the token as a query parameter must not gain a second route into the log
/// through an upstream error page that quotes the request URI back.
#[tokio::test]
#[ignore = "needs package 1: the TorBox Store impl (error text is redacted)"]
async fn tor_box_the_services_words_cannot_carry_the_token() {
    // Enable when the owning package lands: move the body up and delete this wrapper.
    #[cfg(any())]
    async fn pending() {
        let token = "supersecrettoken";
        let cache = new_cache();
        // Seeded under the store's OWN token: the id key is account-scoped, so seeding it under any other
        // name leaves the account holding nothing and the request is never made.
        cache.put(&torrent_id_key(token, H), "42", resolve_cache_ttl());
        let reached = Arc::new(AtomicBool::new(false));
        let r = reached.clone();
        let s = tb(
            token,
            doer(move |req| {
                if req.url().path().contains("requestdl") {
                    r.store(true, Ordering::SeqCst);
                    return Ok(resp(400, &format!("Bad Request on {}", req.url())));
                }
                Ok(resp(200, r#"{"data":{"files":[{"id":0,"name":"m.mkv","size":9}]}}"#))
            }),
            Some(cache),
        );
        let res = s.resolve(&ResolveTarget { file_idx: intp(0), no_add: true, ..tgt(H) }).await;
        assert!(reached.load(Ordering::SeqCst), "requestdl was never called, so this asserts nothing");
        let msg = res.as_ref().err().map(|e| e.to_string()).unwrap_or_default();
        assert!(res.is_err() && !msg.contains(token), "the token reached an error that gets logged: {res:?}");
    }
    todo!("needs package 1: the TorBox Store impl (error text is redacted)");
}

/// An empty needle would splice the replacement between every character, turning a short error from an
/// unconfigured store into an unreadable one.
#[test]
fn redact_token_an_empty_token_leaves_text_alone() {
    assert_eq!(redact_token("http 400 (nope)", ""), "http 400 (nope)");
    assert_eq!(redact_token("http 400 (tok=abc)", "abc"), "http 400 (tok=<token>)");
}

/// The series path must heal from a deleted torrent exactly as the movie path does. Refusing to guess an
/// episode fires BEFORE requestDownload, whose 404 was the only producer of errTorrentGone and so the
/// only thing that could forget the id and re-add — so a pack deleted account-side answered 503
/// store_unavailable on every poll, forever: no backoff to lapse, the six-hour id refreshed each time,
/// and the tiered cache writing it through to disk so a restart did not clear it either. The distinction
/// that fixes it is that mylist ANSWERING "no such torrent" is not the same as mylist not answering.
#[tokio::test]
#[ignore = "needs package 1: the TorBox Store impl (mylist and the re-add)"]
async fn tor_box_a_deleted_pack_is_re_bought_for_a_series_too() {
    // Enable when the owning package lands: move the body up and delete this wrapper.
    #[cfg(any())]
    async fn pending() {
        let _budget = install_budget(HOUR, 50);

        let gone = Arc::new(AtomicBool::new(true));
        let adds = Arc::new(AtomicUsize::new(0));
        let (g, a) = (gone.clone(), adds.clone());
        let d = doer(move |req| {
            if is_add_endpoint(&req) {
                a.fetch_add(1, Ordering::SeqCst);
                g.store(false, Ordering::SeqCst); // re-bought: the account has it again
                return Ok(resp(200, r#"{"data":{"torrent_id":77}}"#));
            }
            if req.url().path().contains("mylist") {
                if g.load(Ordering::SeqCst) {
                    // TorBox's own words for an id it no longer holds — a 200, not an error.
                    return Ok(resp(200, r#"{"success":false,"data":null}"#));
                }
                return Ok(resp(
                    200,
                    r#"{"data":{"files":[{"id":0,"name":"Show.S01E01.mkv","size":900},{"id":1,"name":"Show.S01E02.mkv","size":950}]}}"#,
                ));
            }
            Ok(resp(200, r#"{"success":true,"data":"https://cdn/x"}"#))
        });
        let cache = new_cache();
        cache.put(&torrent_id_key("tok", H), "42", resolve_cache_ttl()); // a stale id from before the deletion
        let s = tb("tok", d, Some(cache.clone()));

        let link = s.resolve(&ResolveTarget { season: intp(1), episode: intp(2), ..tgt(H) }).await;
        assert!(matches!(&link, Ok(l) if !l.is_empty()), "a deleted pack must be re-bought, not answered unavailable forever: {link:?}");
        let n = adds.load(Ordering::SeqCst);
        assert_eq!(n, 1, "made {n} adds; the stale id should have been forgotten and the pack re-added");
        assert_ne!(cache.get(&torrent_id_key("tok", H)).as_deref(), Some("42"), "the stale id survived, and every later poll would keep it alive for another six hours");
    }
    todo!("needs package 1: the TorBox Store impl (mylist and the re-add)");
}

/// The two answers must stay apart in the other direction too: a mylist that does not answer is NOT
/// grounds to forget the id and buy the torrent again, or a blip costs an add against the hourly ceiling
/// and Status loses the id it needs to report the download in progress.
#[tokio::test]
#[ignore = "needs package 1: the TorBox Store impl (mylist and the re-add)"]
async fn tor_box_a_blip_listing_files_does_not_re_buy_the_pack() {
    // Enable when the owning package lands: move the body up and delete this wrapper.
    #[cfg(any())]
    async fn pending() {
        let _budget = install_budget(HOUR, 50);

        let adds = Arc::new(AtomicUsize::new(0));
        let a = adds.clone();
        let d = doer(move |req| {
            if is_add_endpoint(&req) {
                a.fetch_add(1, Ordering::SeqCst);
                return Ok(resp(200, r#"{"data":{"torrent_id":77}}"#));
            }
            if req.url().path().contains("mylist") {
                return Ok(resp(500, "{}"));
            }
            Ok(resp(200, r#"{"success":true,"data":"https://cdn/x"}"#))
        });
        let cache = new_cache();
        cache.put(&torrent_id_key("tok", H), "42", resolve_cache_ttl());
        let s = tb("tok", d, Some(cache.clone()));

        for i in 0..10 {
            let link = s.resolve(&ResolveTarget { season: intp(1), episode: intp(2), ..tgt(H) }).await;
            assert!(!matches!(&link, Ok(l) if !l.is_empty()), "poll {i} served without ever seeing the pack's file names: {link:?}");
        }
        let n = adds.load(Ordering::SeqCst);
        assert_eq!(n, 0, "made {n} adds; a blip listing files says nothing about what the account holds");
        assert_eq!(cache.get(&torrent_id_key("tok", H)).as_deref(), Some("42"), "the id was dropped on a blip, so Status can no longer report the download");
    }
    todo!("needs package 1: the TorBox Store impl (mylist and the re-add)");
}

/// Premiumize's error text is not merely logged: the refused branch PERSISTS it into the refusal cache,
/// from where the probe route prints it verbatim on every later poll. The apikey rides in directdl's form
/// body, so an upstream error page quoting the request back would put it there to stay.
#[tokio::test]
#[ignore = "needs package 2: the Premiumize Store impl (the token is redacted from errors)"]
async fn premiumize_the_services_words_cannot_carry_the_token() {
    // Enable when the owning package lands: move the body up and delete this wrapper.
    #[cfg(any())]
    async fn pending() {
        let _budget = install_budget(HOUR, 50);

        let token = "pmsecrettoken";
        let reached = Arc::new(AtomicBool::new(false));
        let r = reached.clone();
        let cache = new_cache();
        let s = pm(
            token,
            doer(move |req| {
                r.store(true, Ordering::SeqCst);
                Ok(resp(500, &format!("Gateway error handling {}", body_of(&req))))
            }),
            Some(cache.clone()),
        );
        let res = s.resolve(&tgt(H)).await;
        assert!(reached.load(Ordering::SeqCst), "directdl was never called, so this asserts nothing");
        let msg = res.as_ref().err().map(|e| e.to_string()).unwrap_or_default();
        assert!(res.is_err() && !msg.contains(token), "the token reached an error that gets logged: {res:?}");
        // And it must not have been written into the refusal memory either, which outlives the request.
        if let Some(reason) = backed_off(&*cache, Premiumize, token, H) {
            assert!(!reason.contains(token), "the token was persisted into the refusal cache: {reason}");
        }
    }
    todo!("needs package 2: the Premiumize Store impl (the token is redacted from errors)");
}

/// mylist replies with arrays as well as objects — that is why listFiles carries an []entry fallback at
/// all — and `data:[]` is the array form's only way to say "I hold nothing for this id", exactly as
/// `data:null` is the object form's. Read as "no answer" it kept the stale id, re-stamped its six hours
/// on every poll and never re-added, wedging the series path at a permanent 503 while movies healed.
#[tokio::test]
#[ignore = "needs package 1: the TorBox Store impl (mylist's envelope)"]
async fn tor_box_an_empty_listing_is_the_account_answering() {
    // Enable when the owning package lands: move the body up and delete this wrapper.
    #[cfg(any())]
    async fn pending() {
        // A BARE top-level `[]` is deliberately absent: it is not the documented envelope at all, so it stays
        // "no answer" rather than being read as a claim about what the account holds.
        for payload in [r#"{"data":[]}"#, r#"{"success":false,"data":null}"#] {
            let _budget = install_budget(HOUR, 50);

            let gone = Arc::new(AtomicBool::new(true));
            let adds = Arc::new(AtomicUsize::new(0));
            let (g, a) = (gone.clone(), adds.clone());
            let d = doer(move |req| {
                if is_add_endpoint(&req) {
                    a.fetch_add(1, Ordering::SeqCst);
                    g.store(false, Ordering::SeqCst);
                    return Ok(resp(200, r#"{"data":{"torrent_id":77}}"#));
                }
                if req.url().path().contains("mylist") {
                    if g.load(Ordering::SeqCst) {
                        return Ok(resp(200, payload));
                    }
                    return Ok(resp(
                        200,
                        r#"{"data":[{"files":[{"id":0,"name":"Show.S01E01.mkv","size":900},{"id":1,"name":"Show.S01E02.mkv","size":950}]}]}"#,
                    ));
                }
                Ok(resp(200, r#"{"success":true,"data":"https://cdn/x"}"#))
            });
            let cache = new_cache();
            cache.put(&torrent_id_key("tok", H), "42", resolve_cache_ttl());
            let s = tb("tok", d, Some(cache.clone()));

            let link = s.resolve(&ResolveTarget { season: intp(1), episode: intp(2), ..tgt(H) }).await;
            assert!(matches!(&link, Ok(l) if !l.is_empty()), "{payload}: a pack the account no longer holds must be re-bought: {link:?}");
            let n = adds.load(Ordering::SeqCst);
            assert_eq!(n, 1, "{payload}: made {n} adds; the stale id should have been forgotten once and the pack re-added");
            assert_ne!(cache.get(&torrent_id_key("tok", H)).as_deref(), Some("42"), "{payload}: the stale id survived");
        }
    }
    todo!("needs package 1: the TorBox Store impl (mylist's envelope)");
}

/// A torrent createtorrent returned seconds ago, which mylist then denies, is TorBox disagreeing with
/// itself — not the account saying it lacks the torrent. Passed through as `gone` the fresh id was never
/// remembered, so the next poll found nothing known and added the same torrent again: one add per poll,
/// bounded by nothing but the hourly allowance.
#[tokio::test]
#[ignore = "needs package 1: the TorBox Store impl (the just-added contradiction)"]
async fn tor_box_a_torrent_denied_right_after_buying_it_is_not_re_bought() {
    // Enable when the owning package lands: move the body up and delete this wrapper.
    #[cfg(any())]
    async fn pending() {
        let _budget = install_budget(HOUR, 50);

        let adds = Arc::new(AtomicUsize::new(0));
        let a = adds.clone();
        let d = doer(move |req| {
            if is_add_endpoint(&req) {
                a.fetch_add(1, Ordering::SeqCst);
                return Ok(resp(200, r#"{"data":{"torrent_id":77}}"#));
            }
            if req.url().path().contains("mylist") {
                return Ok(resp(200, r#"{"success":false,"data":null}"#));
            }
            Ok(resp(200, r#"{"success":true,"data":"https://cdn/x"}"#))
        });
        let cache = new_cache();
        let s = tb("tok", d, Some(cache));

        let target = ResolveTarget { season: intp(1), episode: intp(2), ..tgt(H) };
        // The first poll's answer is the one the viewer acts on: a dead link, so the client moves to another
        // release rather than sitting on a service error for one that may be fine elsewhere.
        let first = s.resolve(&target).await;
        assert!(is_dead_link(&first), "got {first:?}, want a dead link the client can move past");
        // The polls behind it are what the backoff is for: they cost nothing, where before each bought the
        // same torrent again.
        for _ in 0..9 {
            let _ = s.resolve(&target).await;
        }
        let n = adds.load(Ordering::SeqCst);
        assert!(n <= 1, "made {n} adds; a store contradicting itself must not be paid once per poll");
    }
    todo!("needs package 1: the TorBox Store impl (the just-added contradiction)");
}

/// Real-Debrid needs the same bound TorBox's just-added path got, and for a worse loop: `gone` about a
/// torrent RD created moments ago erases the memory just written, the next poll finds nothing known and
/// adds again — and RD mints a NEW id every time, so the re-add can never converge on one torrent and
/// heal. Two minutes of one viewer on one release spent the whole hourly allowance, left fifty duplicate
/// torrents on the account, and then answered 503 scout_busy for every RD resolve for the rest of the
/// hour. On an RD-only install every /play poll reaches here, since RD has no Status to short-circuit on.
#[tokio::test]
#[ignore = "needs package 2: the Real-Debrid Store impl (the just-added contradiction)"]
async fn real_debrid_a_torrent_denied_right_after_buying_it_is_not_re_bought() {
    // Enable when the owning package lands: move the body up and delete this wrapper.
    #[cfg(any())]
    async fn pending() {
        let _budget = install_budget(HOUR, 50);

        let adds = Arc::new(AtomicUsize::new(0));
        let a = adds.clone();
        let d = doer(move |req| {
            let path = req.url().path();
            if path.contains("addMagnet") {
                let n = a.fetch_add(1, Ordering::SeqCst) + 1;
                return Ok(resp(201, &format!(r#"{{"id":"t{n}"}}"#))); // RD mints a fresh id every time
            }
            if path.contains("/torrents/info/") {
                return Ok(resp(404, "{}")); // RD denies the torrent it just created
            }
            Ok(resp(200, "{}"))
        });
        let cache = new_cache();
        let s = rd("tok", d, Some(cache));
        let target = tgt(H);

        let first = s.resolve(&target).await;
        assert!(
            matches!(&first, Err(StoreError::DeadLink(dl)) if dl.reason.contains("denied holding it")),
            "got {first:?}, want a dead link naming the contradiction"
        );
        for _ in 0..30 {
            let _ = s.resolve(&target).await;
        }
        let n = adds.load(Ordering::SeqCst);
        assert!(n <= 1, "made {n} adds and left {n} duplicate torrents on the account");
        let left = global_add_budget().remaining(&budget_account(RealDebrid, "tok"));
        assert!(left >= 49, "allowance {left} — one viewer on one release spent the hour");
    }
    todo!("needs package 2: the Real-Debrid Store impl (the just-added contradiction)");
}

/// The narrow direction of the just-added backoff, which only a comment guarded: it must fire ONLY when
/// the store denies a torrent it just created. Widened to any error it would back off every legitimately
/// queueing torrent — the ordinary state of a fetch in progress — and answer 503 for a minute on a
/// release that is simply not ready yet.
#[tokio::test]
#[ignore = "needs package 1: the TorBox Store impl (the contradiction backoff's narrow trigger)"]
async fn tor_box_the_contradiction_backoff_does_not_fire_on_an_ordinary_wait() {
    // Enable when the owning package lands: move the body up and delete this wrapper.
    #[cfg(any())]
    async fn pending() {
        let _budget = install_budget(HOUR, 50);

        let d = doer(move |req| {
            if is_add_endpoint(&req) {
                return Ok(resp(200, r#"{"data":{"torrent_id":77}}"#));
            }
            if req.url().path().contains("mylist") {
                // Queued: the entry exists, its files have not resolved yet.
                return Ok(resp(200, r#"{"data":{"files":[]}}"#));
            }
            Ok(resp(200, r#"{"success":false}"#)) // no link yet — still downloading
        });
        let cache = new_cache();
        let s = tb("tok", d, Some(cache.clone()));

        let _ = s.resolve(&ResolveTarget { season: intp(1), episode: intp(2), ..tgt(H) }).await;
        if let Some(reason) = backed_off(&*cache, TorBox, "tok", H) {
            panic!("a torrent that is merely still fetching was backed off: {reason}");
        }
        // And the id is kept, so Status can report the download rather than the release reading as dead.
        assert_eq!(cache.get(&torrent_id_key("tok", H)).as_deref(), Some("77"), "a queued fetch lost the id Status needs");
    }
    todo!("needs package 1: the TorBox Store impl (the contradiction backoff's narrow trigger)");
}

/// mylist is queried by id, so an answer carrying several entries must be matched to the one asked for.
/// Taking the first blind picks a file id out of ANOTHER torrent's list — the wrong-episode failure by a
/// route the name-match cannot catch, since the names it matches would be the other torrent's.
#[tokio::test]
#[ignore = "needs package 1: the TorBox Store impl (listFiles matches the entry by id)"]
async fn tor_box_list_files_matches_the_entry_to_the_id_asked_for() {
    // Enable when the owning package lands: move the body up and delete this wrapper.
    #[cfg(any())]
    async fn pending() {
        let body = r#"{"data":[{"id":9,"files":[{"id":0,"name":"OTHER.S09E09.mkv","size":10}]},{"id":42,"files":[{"id":7,"name":"Show.S01E02.mkv","size":900}]}]}"#;
        let s = tb("tok", doer(move |_| Ok(resp(200, body))), None);
        let files = s.list_files(42).expect("unexpected error");
        assert!(files.len() == 1 && files[0].name == "Show.S01E02.mkv", "got {files:?} — another torrent's files were used to pick ours");
        // And when no entry matches, that is not our torrent's file list either: refuse rather than guess.
        let s = tb("tok", doer(move |_| Ok(resp(200, body))), None);
        let err = s.list_files(5).expect_err("the answer describes other torrents");
        assert_eq!(err, err_no_file_list(), "got {err:?}, want errNoFileList when the answer describes other torrents");
    }
    todo!("needs package 1: the TorBox Store impl (listFiles matches the entry by id)");
}

/// Two classifications on mylist that the three-way split got wrong in opposite directions.
#[tokio::test]
#[ignore = "needs package 1: the TorBox Store impl (listFiles classification)"]
async fn tor_box_list_files_classifies_the_remaining_shapes() {
    // Enable when the owning package lands: move the body up and delete this wrapper.
    #[cfg(any())]
    async fn pending() {
        // A rejected key found HERE must be able to raise the account-wide backoff. errNoFileList carries
        // Status 0, so routing a 401 to it left the dead key undiscoverable on this endpoint.
        let cache = new_cache();
        let s = tb("tok", doer(|_| Ok(resp(401, "{}"))), Some(cache.clone()));
        let err = s.list_files(42).expect_err("a 401 must be a refusal");
        let u = match &err {
            StoreError::Unavailable(u) if u.status == 401 => u.clone(),
            other => panic!("got {other:?}, want a refusal carrying its status"),
        };
        record_refusal(&*cache, TorBox, "tok", H, &StoreError::Unavailable(u));
        assert!(account_backed_off(&*cache, TorBox, "tok").is_some(), "a key TorBox rejected on mylist never reaches the account backoff");

        // A body with no `data` key at all is not TorBox's envelope — a proxy page that happens to be valid
        // JSON, say. Silence, not a claim about the account; read as `gone` it paid an add to re-buy a
        // torrent nobody said was missing.
        let s = tb("tok", doer(|_| Ok(resp(200, "{}"))), None);
        let err = s.list_files(42).expect_err("a body without data is silence");
        assert_eq!(err, err_no_file_list(), "got {err:?}, want errNoFileList for a body that is not the envelope");
        // The two shapes that DO say it still must.
        for payload in [r#"{"success":false,"data":null}"#, r#"{"data":null}"#] {
            let s = tb("tok", doer(move |_| Ok(resp(200, payload))), None);
            let err = s.list_files(42).expect_err("the listing says the torrent is gone");
            assert!(is_dead_reason(&Err::<(), _>(err.clone()), ERR_TORRENT_GONE), "{payload}: got {err:?}, want errTorrentGone");
        }
    }
    todo!("needs package 1: the TorBox Store impl (listFiles classification)");
}

/// The same invariant on all three stores: when the service ANSWERS and creates nothing, the charge taken
/// before the request goes back. RD had neither of its siblings' bounds on this branch — TorBox records a
/// refusal here, Premiumize refunds — so a magnet RD rejects with a 400, or any 2xx scout cannot pull an
/// id out of (a proxy page served as 200), spent one add per poll. Fifty real addMagnet calls inside two
/// minutes at the client's cadence, and then 503 scout_busy on every RD resolve for the rest of the
/// rolling hour, healthy releases included.
#[tokio::test]
#[ignore = "needs package 2: the Real-Debrid Store impl (an answered failure is not a charge)"]
async fn real_debrid_an_answered_failure_is_not_a_charge() {
    // Enable when the owning package lands: move the body up and delete this wrapper.
    #[cfg(any())]
    async fn pending() {
        // (name, body, status)
        let cases: [(&str, &'static str, u16); 2] =
            [("a rejected magnet", r#"{"error":"bad magnet","error_code":11}"#, 400), ("a 2xx with no id", r#"{"ok":true}"#, 200)];
        for (name, body, status) in cases {
            let _budget = install_budget(HOUR, 50);

            let adds = Arc::new(AtomicUsize::new(0));
            let a = adds.clone();
            let d = doer(move |req| {
                if req.url().path().contains("addMagnet") {
                    a.fetch_add(1, Ordering::SeqCst);
                    return Ok(resp(status, body));
                }
                Ok(resp(200, "{}"))
            });
            let cache = new_cache();
            let s = rd("tok", d, Some(cache.clone()));
            for _ in 0..60 {
                let _ = s.resolve(&tgt(H)).await;
            }
            let left = global_add_budget().remaining(&budget_account(RealDebrid, "tok"));
            assert_eq!(left, 50, "{name}: allowance {left} — one viewer on one release spent the hour");
            // The allowance alone cannot see the loop: refunding every attempt keeps it at 50 while sixty
            // polls make sixty real calls upstream. Counting them is what notices that the refund removed
            // the only bound this branch had, which is exactly how that regression shipped.
            let n = adds.load(Ordering::SeqCst);
            assert!(n <= 1, "{name}: made {n} addMagnet calls over 60 polls — nothing bounds the loop");
            // What the allowance buys: a DIFFERENT, healthy release still resolves, instead of being
            // refused by scout's own bookkeeping for the rest of the hour.
            let healthy = rd(
                "tok",
                doer(|req| {
                    let path = req.url().path();
                    if path.contains("addMagnet") {
                        return Ok(resp(201, r#"{"id":"t9"}"#));
                    }
                    if path.contains("unrestrict") {
                        return Ok(resp(200, r#"{"download":"https://rd/ok"}"#));
                    }
                    if path.contains("/torrents/info/") {
                        return Ok(resp(200, r#"{"files":[{"id":1,"path":"m.mkv","bytes":9,"selected":1}],"links":["l1"]}"#));
                    }
                    Ok(resp(200, "{}"))
                }),
                Some(cache),
            );
            let other = healthy.resolve(&tgt(&repeat("b", 40))).await;
            assert!(other.is_ok(), "{name}: a healthy release answered {other:?} — scout refused what RD would have served");
        }
    }
    todo!("needs package 2: the Real-Debrid Store impl (an answered failure is not a charge)");
}

/// RD was also the only store discarding the service's own words on this branch, so the log read the same
/// for a bad magnet, a locked account and a Cloudflare page.
#[tokio::test]
#[ignore = "needs package 2: the Real-Debrid Store impl (the add keeps the service's words)"]
async fn real_debrid_the_add_keeps_the_services_own_words() {
    // Enable when the owning package lands: move the body up and delete this wrapper.
    #[cfg(any())]
    async fn pending() {
        let _budget = install_budget(HOUR, 50);

        let d = doer(|req| {
            if req.url().path().contains("addMagnet") {
                return Ok(resp(400, r#"{"error":"infringing_file"}"#));
            }
            Ok(resp(200, "{}"))
        });
        let s = rd("tok", d, Some(new_cache()));
        let res = s.resolve(&tgt(H)).await;
        let msg = res.as_ref().err().map(|e| e.to_string()).unwrap_or_default();
        assert!(msg.contains("infringing_file"), "got {res:?} — the answer that says WHY was discarded");
    }
    todo!("needs package 2: the Real-Debrid Store impl (the add keeps the service's words)");
}

/// A key TorBox rejects on mylist must reach the account-wide backoff. listFiles classifies it, but the
/// only caller assigned the error and never consulted it for any value but errTorrentGone, so the
/// classification died one line after it was made.
#[tokio::test]
#[ignore = "needs package 1: the TorBox Store impl (mylist's refusal reaches the account backoff)"]
async fn tor_box_a_rejected_key_on_mylist_reaches_the_account_backoff() {
    // Enable when the owning package lands: move the body up and delete this wrapper.
    #[cfg(any())]
    async fn pending() {
        let cache = new_cache();
        cache.put(&torrent_id_key("tok", H), "42", resolve_cache_ttl());
        let s = tb(
            "tok",
            doer(|req| {
                if req.url().path().contains("mylist") {
                    return Ok(resp(401, r#"{"error":"BAD_TOKEN"}"#));
                }
                Ok(resp(200, r#"{"success":true,"data":"https://cdn/x"}"#))
            }),
            Some(cache.clone()),
        );
        let res = s.resolve(&ResolveTarget { season: intp(1), episode: intp(2), ..tgt(H) }).await;
        let status = unavailable(&res).map(|u| u.status);
        assert_eq!(status, Some(401), "got {res:?}, want the refusal carrying its status");
        assert!(account_backed_off(&*cache, TorBox, "tok").is_some(), "the dead key was never recorded, so every later request asks with it again");
    }
    todo!("needs package 1: the TorBox Store impl (mylist's refusal reaches the account backoff)");
}

/// A body that could not be READ is not an answer that nothing was created. RD had already sent its
/// status line — a 201 means the torrent exists on the account — and a mid-body reset, a TLS truncation
/// or the client's context expiring after the headers arrived all land here. Folding it in with "RD
/// created nothing" refunded the charge for a torrent that does exist, and with no backoff on that branch
/// the budget was the only bound: sixty polls created sixty torrents with the allowance untouched.
#[tokio::test]
#[ignore = "needs package 2: the Real-Debrid Store impl, and a streamed body that errors mid-read (futures-util)"]
async fn real_debrid_an_unreadable_answer_is_not_proof_nothing_was_created() {
    // Enable when the owning package lands: move the body up and delete this wrapper.
    #[cfg(any())]
    async fn pending() {
        let _budget = install_budget(HOUR, 50);

        let created = Arc::new(AtomicUsize::new(0));
        let c = created.clone();
        let d = doer(move |req| {
            if req.url().path().contains("addMagnet") {
                c.fetch_add(1, Ordering::SeqCst);
                // Answered 201 — the torrent exists — then the body dies mid-flight.
                return Ok(broken_body_response(201, "unexpected EOF mid-body"));
            }
            Ok(resp(200, "{}"))
        });
        let s = rd("tok", d, Some(new_cache()));
        for _ in 0..60 {
            let _ = s.resolve(&tgt(H)).await;
        }
        let n = created.load(Ordering::SeqCst);
        assert!(n <= 1, "created {n} torrents on the account across 60 polls");
        // The charge stays: the outcome is unknown, and that is precisely when assuming nothing happened is
        // the expensive assumption.
        let left = global_add_budget().remaining(&budget_account(RealDebrid, "tok"));
        assert_eq!(left, 49, "allowance {left} — an add whose outcome is unknown was treated as one that never happened");
    }
    todo!("needs package 2: the Real-Debrid Store impl, and a streamed body that errors mid-read (futures-util)");
}

/// The rejected-key return must not drop the torrent id it was just handed: Status needs it to report a
/// download in progress, and without it the next poll knows of no torrent and buys another.
#[tokio::test]
#[ignore = "needs package 1: the TorBox Store impl (the rejected-key return keeps the id)"]
async fn tor_box_the_rejected_key_return_keeps_the_torrent_id() {
    // Enable when the owning package lands: move the body up and delete this wrapper.
    #[cfg(any())]
    async fn pending() {
        let _budget = install_budget(HOUR, 50);

        let adds = Arc::new(AtomicUsize::new(0));
        let a = adds.clone();
        let d = doer(move |req| {
            if is_add_endpoint(&req) {
                a.fetch_add(1, Ordering::SeqCst);
                return Ok(resp(200, r#"{"data":{"torrent_id":77}}"#));
            }
            if req.url().path().contains("mylist") {
                return Ok(resp(403, r#"{"error":"BAD_TOKEN"}"#));
            }
            Ok(resp(200, r#"{"success":true,"data":"https://cdn/x"}"#))
        });
        let cache = new_cache();
        let s = tb("tok", d, Some(cache.clone()));
        let _ = s.resolve(&ResolveTarget { season: intp(1), episode: intp(2), ..tgt(H) }).await;
        assert_eq!(cache.get(&torrent_id_key("tok", H)).as_deref(), Some("77"), "the id createtorrent just handed us was dropped");
        let n = adds.load(Ordering::SeqCst);
        assert_eq!(n, 1, "made {n} adds");
    }
    todo!("needs package 1: the TorBox Store impl (the rejected-key return keeps the id)");
}

/// The same bound on Premiumize's answered-failure branch: refunding the charge fixed the quota but left
/// a poll loop free to re-ask a magnet Premiumize has already rejected, once every couple of seconds for
/// as long as the viewer sits there. Free of quota is not free.
#[tokio::test]
#[ignore = "needs package 2: the Premiumize Store impl (an answered failure is asked only once)"]
async fn premiumize_an_answered_failure_is_asked_only_once() {
    // Enable when the owning package lands: move the body up and delete this wrapper.
    #[cfg(any())]
    async fn pending() {
        let _budget = install_budget(HOUR, 50);

        let calls = Arc::new(AtomicUsize::new(0));
        let c = calls.clone();
        let s = pm(
            "tok",
            doer(move |_| {
                c.fetch_add(1, Ordering::SeqCst);
                Ok(resp(400, r#"{"status":"error","message":"Invalid src"}"#))
            }),
            Some(new_cache()),
        );
        let target = tgt(H);
        // The first answer is a dead link, so the client falls through to another release rather than being
        // told the store is unavailable for one the store has a definite opinion about.
        let first = s.resolve(&target).await;
        assert!(is_dead_link(&first), "got {first:?}, want a dead link the client can move past");
        for _ in 0..59 {
            let _ = s.resolve(&target).await;
        }
        let n = calls.load(Ordering::SeqCst);
        assert!(n <= 1, "made {n} directdl calls over 60 polls — nothing bounds the loop");
        let left = global_add_budget().remaining(&budget_account(Premiumize, "tok"));
        assert_eq!(left, 50, "allowance {left} — a refused magnet was charged");
    }
    todo!("needs package 2: the Premiumize Store impl (an answered failure is asked only once)");
}

/// A cancelled poll is not the store refusing. /play runs on the client's context — a focus change is
/// enough — and the add's response BODY is read inside it, so a cancellation lands there on every store.
/// Nothing covered that: the existing guard test hands recordRefusal a context.Canceled directly, which
/// certifies the helper but not the property that matters (no production path files a cancellation as a
/// refusal). A caller that builds its own error satisfies the helper test and breaks the property, which
/// is exactly how it shipped.
#[tokio::test]
#[ignore = "needs packages 1 and 2: the three Store impls, and a body that is cancelled mid-read (futures-util)"]
async fn stores_a_cancelled_add_body_is_not_a_refusal() {
    // Enable when the owning package lands: move the body up and delete this wrapper.
    #[cfg(any())]
    async fn pending() {
        // (name, service, add path, body that the store then reads)
        let cases: [(&str, DebridService, &'static str, &'static str); 3] = [
            ("realdebrid", RealDebrid, "addMagnet", r#"{"id":"t1"}"#),
            ("torbox", TorBox, "createtorrent", r#"{"data":{"torrent_id":7}}"#),
            ("premiumize", Premiumize, "directdl", r#"{"status":"success","content":[]}"#),
        ];
        for (name, svc, path, head) in cases {
            let _budget = install_budget(HOUR, 50);

            let cache = new_cache();
            let adds = Arc::new(AtomicUsize::new(0));
            let a = adds.clone();
            let d = doer(move |req| {
                if req.url().path().contains(path) {
                    a.fetch_add(1, Ordering::SeqCst);
                    // The add was accepted; the body then dies because the viewer moved on.
                    return Ok(cancelled_body_response(200, "context canceled"));
                }
                Ok(resp(200, head))
            });
            let store = build_for(name, d, cache.clone());
            for _ in 0..30 {
                let _ = store.resolve(&tgt(H)).await;
            }
            // Three properties, because "not filed as a refusal" alone is a PROXY: both the TorBox and
            // the Premiumize bug this test was written for passed that assertion while re-sending the add
            // on every poll. What matters is that a cancelled poll cannot cost an add and cannot loop.
            if let Some(reason) = backed_off(&*cache, svc, "tok", H) {
                panic!("{name}: a cancelled poll was filed as {svc:?} refusing: {reason}");
            }
            let n = adds.load(Ordering::SeqCst);
            assert!(n <= 1, "{name}: sent the add {n} times across 30 polls — the marker that says one is in flight was cleared before the body was read");
            // The charge stays exactly once: the add was written to the wire, and refunding it is what
            // removed the only bound the loop had.
            let left = global_add_budget().remaining(&budget_account(svc, "tok"));
            assert_eq!(left, 49, "{name}: allowance {left}, want 49");
        }
    }
    todo!("needs packages 1 and 2: the three Store impls, and a body that is cancelled mid-read (futures-util)");
}

/// An add whose answer never arrived is what the in-flight marker is for. It must survive, so the next
/// poll says "downloading" — honest for an add that went out and whose result we never saw — rather than
/// buying the torrent again or blaming the store.
#[tokio::test]
#[ignore = "needs package 2: the Real-Debrid Store impl, and a body that errors mid-read (futures-util)"]
async fn real_debrid_an_unread_answer_leaves_the_add_in_flight() {
    // Enable when the owning package lands: move the body up and delete this wrapper.
    #[cfg(any())]
    async fn pending() {
        let _budget = install_budget(HOUR, 50);

        let created = Arc::new(AtomicUsize::new(0));
        let c = created.clone();
        let d = doer(move |req| {
            if req.url().path().contains("addMagnet") {
                c.fetch_add(1, Ordering::SeqCst);
                return Ok(broken_body_response(201, "unexpected EOF mid-body"));
            }
            Ok(resp(200, "{}"))
        });
        let cache = new_cache();
        let s = rd("tok", d, Some(cache.clone()));

        let first = s.resolve(&tgt(H)).await;
        assert!(matches!(&first, Err(StoreError::AddInFlight)), "got {first:?}, want the add reported as still in flight");
        for _ in 0..60 {
            let _ = s.resolve(&tgt(H)).await;
        }
        let n = created.load(Ordering::SeqCst);
        assert!(n <= 1, "created {n} torrents on the account across 61 polls");
        // The charge stays: the add was written to the wire, and assuming otherwise is the expensive guess.
        let left = global_add_budget().remaining(&budget_account(RealDebrid, "tok"));
        assert_eq!(left, 49, "allowance {left} — an add that went out was treated as one that never happened");
        // And it is not a refusal, so no store gets blamed for a body scout could not read.
        assert!(backed_off(&*cache, RealDebrid, "tok", H).is_none(), "filed as RD refusing");
    }
    todo!("needs package 2: the Real-Debrid Store impl, and a body that errors mid-read (futures-util)");
}

/// TorBox's createtorrent detail is persisted into the refusal cache, which writes through to disk — so
/// it gets the redaction the other three detail sites have. The token rides in a header here rather than
/// the URL, so a leak needs the service to echo the header back; this was the one site not following the
/// rule, which is reason enough.
#[tokio::test]
#[ignore = "needs package 1: the TorBox Store impl (the add detail is redacted)"]
async fn tor_box_the_add_detail_cannot_carry_the_token() {
    // Enable when the owning package lands: move the body up and delete this wrapper.
    #[cfg(any())]
    async fn pending() {
        let _budget = install_budget(HOUR, 50);

        let token = "torboxsecrettoken";
        let cache = new_cache();
        let s = tb(
            token,
            doer(|req| {
                if is_add_endpoint(&req) {
                    let auth = req.headers().get("authorization").and_then(|v| v.to_str().ok()).unwrap_or_default().to_string();
                    return Ok(resp(403, &format!(r#"{{"error":"rejected for {auth}"}}"#)));
                }
                Ok(resp(200, "{}"))
            }),
            Some(cache.clone()),
        );
        let res = s.resolve(&tgt(H)).await;
        let msg = res.as_ref().err().map(|e| e.to_string()).unwrap_or_default();
        assert!(res.is_err() && !msg.contains(token), "the token reached an error that gets logged: {res:?}");
        if let Some(reason) = backed_off(&*cache, TorBox, token, H) {
            assert!(!reason.contains(token), "the token was persisted into the refusal cache: {reason}");
        }
    }
    todo!("needs package 1: the TorBox Store impl (the add detail is redacted)");
}

/// The other side of moving the settle after the body read: an add that DID complete must still clear the
/// marker. Left set, every later poll answers "an add is already in flight" for a release that resolved
/// perfectly well — 202 downloading forever, with no add ever sent again to heal it.
#[tokio::test]
#[ignore = "needs packages 1 and 2: the three Store impls (the settle after a completed add)"]
async fn stores_a_completed_add_clears_the_in_flight_marker() {
    // Enable when the owning package lands: move the body up and delete this wrapper.
    #[cfg(any())]
    async fn pending() {
        for (name, svc, make) in [
            ("realdebrid", RealDebrid, 0usize),
            ("torbox", TorBox, 1),
            ("premiumize", Premiumize, 2),
        ] {
            let _budget = install_budget(HOUR, 50);

            let cache = new_cache();
            let d = doer(move |req| {
                let path = req.url().path();
                Ok(match make {
                    0 => {
                        if path.contains("addMagnet") {
                            resp(201, r#"{"id":"t1"}"#)
                        } else if path.contains("unrestrict") {
                            resp(200, r#"{"download":"https://rd/ok"}"#)
                        } else if path.contains("/torrents/info/") {
                            resp(200, r#"{"files":[{"id":1,"path":"m.mkv","bytes":9,"selected":1}],"links":["l1"]}"#)
                        } else {
                            resp(200, "{}")
                        }
                    }
                    1 => {
                        if is_add_endpoint(&req) {
                            resp(200, r#"{"data":{"torrent_id":7}}"#)
                        } else {
                            resp(200, r#"{"success":true,"data":"https://cdn/x"}"#)
                        }
                    }
                    _ => resp(200, r#"{"status":"success","content":[{"path":"m.mkv","link":"https://pm/ok"}]}"#),
                })
            });
            let store = build_for(name, d, cache.clone());
            let link = store.resolve(&tgt(H)).await;
            assert!(matches!(&link, Ok(l) if !l.is_empty()), "{name}: setup: the release did not resolve: {link:?}");
            assert!(add_in_flight(&*cache, svc, "tok", H).is_ok(), "{name}: the marker survived a completed add, so every later poll answers in flight");
            // And the release still resolves on the next poll rather than reporting a phantom add.
            let again = store.resolve(&tgt(H)).await;
            assert!(matches!(&again, Ok(l) if !l.is_empty()), "{name}: the second poll answered {again:?}");
        }
    }
    todo!("needs packages 1 and 2: the three Store impls (the settle after a completed add)");
}

/// A pack named with bare episode numbers — `[Grp] Show - 03 [1080p].mkv`, the standard anime/TV shape —
/// is not episode-LABELLED by the only evidence test that can be trusted (a bare number cannot be, since
/// "Movie.2019.1080p" is full of digit runs). It used to fall back to the largest video, and because that
/// answer is non-nil every caller returned before reading the indexer's fileIdx — a position in this very
/// torrent, and correct. So every episode of the pack resolved to the same file, with a 302 and no error.
#[test]
#[ignore = "needs packages 1 and 2: selectFileID (torbox.rs), pickFileID (realdebrid.rs) and pickIndex (premiumize.rs)"]
fn stores_a_bare_numbered_pack_uses_the_indexers_file_index() {
    // Enable when the owning package lands: move the body up and delete this wrapper.
    #[cfg(any())]
    fn pending() {
        let files = vec![
            TorrentFile { index: 0, name: "[Grp] Show - 01 [1080p].mkv".into(), size_bytes: Some(900) },
            TorrentFile { index: 1, name: "[Grp] Show - 02 [1080p].mkv".into(), size_bytes: Some(4000) }, // the largest: the old answer
            TorrentFile { index: 2, name: "[Grp] Show - 03 [1080p].mkv".into(), size_bytes: Some(950) },
        ];
        // S01E03 is at position 2, which is exactly what the indexer said.
        let target = ResolveTarget { season: intp(1), episode: intp(3), file_idx: intp(2), ..tgt(H) };

        want_pick(select_file_id(&files, &target), 2, "torbox follows the indexer rather than the biggest file");
        want_pick(RealDebridStore::default_for_test().pick_file_id(&files, &target), 2, "realdebrid follows the indexer");
        want_pick(PremiumizeStore::default_for_test().pick_index(&files, &target), 2, "premiumize follows the indexer");

        // With no fileIdx there is nothing better than the largest, and that fallback must survive — a
        // feature plus a sample is the common shape and the big one is right.
        let sampled = vec![
            TorrentFile { index: 0, name: "Show.1080p.mkv".into(), size_bytes: Some(4000) },
            TorrentFile { index: 1, name: "Sample/sample.mkv".into(), size_bytes: Some(2) },
        ];
        let blind = ResolveTarget { season: intp(1), episode: intp(3), ..tgt(H) };
        want_pick(select_file_id(&sampled, &blind), 0, "torbox still prefers the feature to the sample");
        want_pick(RealDebridStore::default_for_test().pick_file_id(&sampled, &blind), 0, "realdebrid still prefers the feature");
        want_pick(PremiumizeStore::default_for_test().pick_index(&sampled, &blind), 0, "premiumize still prefers the feature");
    }
    todo!("needs packages 1 and 2: selectFileID (torbox.rs), pickFileID (realdebrid.rs) and pickIndex (premiumize.rs)");
}

/// "The outcome is unknown" is a wait, and a wait needs a deadline. The add-attempt marker lives 90s and
/// is rewritten by every new attempt, so a release whose add answer is never readable cycled marker →
/// expiry → fresh charged add → marker, forever: the client shown 202 "downloading" throughout with no
/// path ever answering dead link, and on RD and Premiumize — which have no Status to rediscover the
/// torrent with — forty duplicate torrents an hour and a viewer with nothing to do but back out.
#[test]
#[ignore = "needs package 2: stores/refusal.rs (unknownOutcome, its clock and TTL)"]
fn stores_an_add_whose_outcome_is_never_known_gives_up() {
    // Enable when the owning package lands: move the body up and delete this wrapper.
    #[cfg(any())]
    fn pending() {
        for svc in [RealDebrid, TorBox, Premiumize] {
            let cache = new_cache();
            // The first failure starts the clock and reads as "still coming".
            let first = unknown_outcome(&*cache, svc, "tok", H, &service_went_quiet());
            assert!(matches!(first, StoreError::AddInFlight), "{svc:?}: got {first:?}, want the add reported as still in flight");
            // Re-asking must not restamp it: a clock every poll resets is not a deadline, it is the
            // absorbing state this exists to end. Wind it back to just inside the window, ask twenty more
            // times, and the stamp must still be the old one.
            let aged = stamp_at(SystemTime::now() - add_give_up() + Duration::from_secs(60));
            cache.put(&unknown_outcome_key(svc, "tok", H), &aged, unknown_outcome_ttl());
            for _ in 0..20 {
                let _ = unknown_outcome(&*cache, svc, "tok", H, &service_went_quiet());
            }
            assert_eq!(cache.get(&unknown_outcome_key(svc, "tok", H)).as_deref(), Some(aged.as_str()), "{svc:?}: the clock was restamped — twenty polls just bought another ten minutes");
            let stale = stamp_at(SystemTime::now() - add_give_up() - Duration::from_secs(60));
            cache.put(&unknown_outcome_key(svc, "tok", H), &stale, unknown_outcome_ttl());
            let dead = unknown_outcome(&*cache, svc, "tok", H, &service_went_quiet());
            assert!(matches!(dead, StoreError::DeadLink(_)), "{svc:?}: got {dead:?}, want a dead link so the client can fall through");
            // An outcome of any kind ends the run of not knowing, so the next add starts fresh.
            settle_add_attempt(&*cache, svc, "tok", H);
            let fresh = unknown_outcome(&*cache, svc, "tok", H, &service_went_quiet());
            assert!(matches!(fresh, StoreError::AddInFlight), "{svc:?}: got {fresh:?} — the give-up outlived the answer that resolved it");
        }
    }
    todo!("needs package 2: stores/refusal.rs (unknownOutcome, its clock and TTL)");
}

/// An add that went out and was never answered is scout's own uncertainty, not the debrid refusing. All
/// three stores set the in-flight marker for exactly that; two of them then recorded the transport
/// failure as a refusal as well, and since they consulted backedOff FIRST the refusal pre-empted the
/// marker: errAddInFlight became unreachable and the client was told its debrid was refusing — and, per
/// this codebase's own account of that answer, stopped trying other sources — for a release scout had an
/// add out for. Real-Debrid is the sharp case: it has no Status for handlePlay to rescue the answer with.
#[tokio::test]
#[ignore = "needs packages 1 and 2: the three Store impls (the in-flight marker outranks the backoff)"]
async fn stores_an_unanswered_add_reads_as_coming_not_as_refusing() {
    // Enable when the owning package lands: move the body up and delete this wrapper.
    #[cfg(any())]
    async fn pending() {
        // (name, svc, add path, build)
        let cases: [(&str, DebridService, &'static str); 3] = [
            ("torbox", TorBox, "createtorrent"),
            ("realdebrid", RealDebrid, "addMagnet"),
            ("premiumize", Premiumize, "directdl"),
        ];
        for (name, svc, path) in cases {
            let _budget = install_budget(HOUR, 50);

            let adds = Arc::new(AtomicUsize::new(0));
            let a = adds.clone();
            let cache = new_cache();
            let store = build_for(
                name,
                doer(move |req| {
                    if req.url().path().contains(path) {
                        a.fetch_add(1, Ordering::SeqCst);
                        return Err(io::Error::other("read tcp 10.0.0.2:443: connection reset by peer").into());
                    }
                    Ok(resp(200, "{}"))
                }),
                cache.clone(),
            );
            let target = tgt(H);
            let _ = store.resolve(&target).await;

            // The store said nothing, so nothing may be recorded against it.
            assert!(backed_off(&*cache, svc, "tok", H).is_none(), "{name}: an unanswered add was filed as refusing");
            // And the polls behind it read as "coming", which handlePlay answers 202, rather than 503.
            for i in 0..5 {
                let res = store.resolve(&target).await;
                assert!(matches!(&res, Err(StoreError::AddInFlight)), "{name}: poll {i} answered {res:?}, want the add reported as still in flight");
            }
            let n = adds.load(Ordering::SeqCst);
            assert_eq!(n, 1, "{name}: sent the add {n} times; the marker exists to stop exactly that");
        }
    }
    todo!("needs packages 1 and 2: the three Store impls (the in-flight marker outranks the backoff)");
}

/// The account gate the other two stores have. It was redundant on Premiumize until addInFlight was moved
/// above backedOff, at which point a live in-flight marker pre-empted a rejected key and answered 202
/// "downloading" where TorBox and RD answer 503. A dead key is not a wait.
#[tokio::test]
#[ignore = "needs package 2: the Premiumize Store impl (the account gate outranks the in-flight marker)"]
async fn premiumize_a_rejected_key_outranks_an_in_flight_marker() {
    // Enable when the owning package lands: move the body up and delete this wrapper.
    #[cfg(any())]
    async fn pending() {
        let cache = new_cache();
        record_refusal(
            &*cache,
            Premiumize,
            "tok",
            &repeat("f", 40),
            &StoreError::Unavailable(StoreUnavailableError {
                service: Premiumize,
                status: 401,
                reason: "directdl http 401".into(),
            }),
        );
        note_add_attempt(&*cache, Premiumize, "tok", H);
        let s = pm(
            "tok",
            doer(|_| {
                panic!("a rejected key must stop the request before it is made");
            }),
            Some(cache),
        );
        let res = s.resolve(&tgt(H)).await;
        assert!(!matches!(&res, Err(StoreError::AddInFlight)), "a dead key was reported as a transfer on its way: {res:?}");
        assert!(unavailable(&res).is_some(), "got {res:?}, want the store reported unavailable");
    }
    todo!("needs package 2: the Premiumize Store impl (the account gate outranks the in-flight marker)");
}

/// An add that keeps failing at the transport must not cycle forever. The marker lives 90 seconds and
/// every attempt writes a fresh one, so without a deadline it went marker → expiry → another charged add,
/// about forty an hour, until the allowance was gone and every release on the account answered 503
/// scout_busy. That is the cycle unknownOutcomeKey exists to end; it was wired only to the body-read path.
#[tokio::test]
#[ignore = "needs packages 1 and 2: the three Store impls (the give-up clock on the transport path)"]
async fn stores_a_transport_failed_add_gives_up_eventually() {
    // Enable when the owning package lands: move the body up and delete this wrapper.
    #[cfg(any())]
    async fn pending() {
        // (name, svc, add path)
        let cases: [(&str, DebridService, &'static str); 3] = [
            ("torbox", TorBox, "createtorrent"),
            ("realdebrid", RealDebrid, "addMagnet"),
            ("premiumize", Premiumize, "directdl"),
        ];
        for (name, svc, path) in cases {
            let _budget = install_budget(HOUR, 50);

            let adds = Arc::new(AtomicUsize::new(0));
            let a = adds.clone();
            let cache = new_cache();
            let store = build_for(
                name,
                doer(move |req| {
                    if req.url().path().contains(path) {
                        a.fetch_add(1, Ordering::SeqCst);
                        return Err(io::Error::other("dial tcp 10.0.0.2:443: connect: connection refused").into());
                    }
                    Ok(resp(200, "{}"))
                }),
                cache.clone(),
            );
            let target = tgt(H);
            let _ = store.resolve(&target).await;

            // PRODUCTION must have started the clock. Writing it here instead would test the deadline
            // against a stamp the service never makes — the proxy shape that has hidden a defect in nine
            // of the last ten rounds.
            let stamp = cache.get(&unknown_outcome_key(svc, "tok", H));
            assert!(stamp.as_deref().is_some_and(|s| !s.is_empty()), "{name}: a transport-failed add left no give-up clock, so nothing bounds the retries");
            // While it is running, the polls behind it read as "coming".
            let res = store.resolve(&target).await;
            assert!(matches!(&res, Err(StoreError::AddInFlight)), "{name}: got {res:?}, want the add reported as still in flight");
            // Wind the clock the service wrote past its deadline, and lapse the 90s marker with it. Every
            // later poll must answer a dead link the client can move past, and must not buy the torrent
            // again.
            settle_add_attempt(&*cache, svc, "tok", H);
            let stale = stamp_at(SystemTime::now() - add_give_up() - Duration::from_secs(60));
            cache.put(&unknown_outcome_key(svc, "tok", H), &stale, unknown_outcome_ttl());

            let before = adds.load(Ordering::SeqCst);
            for i in 0..20 {
                let res = store.resolve(&target).await;
                assert!(is_dead_link(&res), "{name}: poll {i} answered {res:?}, want a dead link the client can move past");
            }
            let after = adds.load(Ordering::SeqCst);
            assert_eq!(after, before, "{name}: sent {} more adds past the give-up deadline", after - before);
        }
    }
    todo!("needs packages 1 and 2: the three Store impls (the give-up clock on the transport path)");
}

/// An add of ours already out outranks any refusal, whichever store said what and in whichever order.
/// errAddInFlight wraps a StoreUnavailableError, so the first refusal used to claim the verdict and store
/// ORDER decided whether /play answered 202 "downloading" or 503 naming a store that had said nothing.
#[tokio::test]
#[ignore = "needs package 2: stores/pool.rs (StorePool.ResolvePreferring)"]
async fn resolve_preferring_an_add_in_flight_outranks_a_refusal() {
    // Enable when the owning package lands: move the body up and delete this wrapper.
    #[cfg(any())]
    async fn pending() {
        let refusing = StubStore {
            svc: TorBox,
            err: StoreError::Unavailable(StoreUnavailableError {
                service: TorBox,
                status: 429,
                reason: "createtorrent http 429 (backing off)".into(),
            }),
        };
        let fetching = StubStore {
            svc: RealDebrid,
            err: StoreError::AddInFlight,
        };
        for order in [vec![refusing.clone(), fetching.clone()], vec![fetching.clone(), refusing.clone()]] {
            let pool = StorePool { stores: order.into_iter().map(|s| Box::new(s) as Box<dyn Store>).collect() };
            let res = pool.resolve_preferring(&tgt(H), None).await;
            assert!(matches!(&res, Err(StoreError::AddInFlight)), "store order decided the verdict: got {res:?}, want the add reported in flight");
        }
    }
    todo!("needs package 2: stores/pool.rs (StorePool.ResolvePreferring)");
}

/// A viewer backing out must not condemn a release. /play runs on the client's context and a focus change
/// is enough to cancel it, so a cancelled add landed in the same branch as a service that never answered
/// — and once the give-up clock was stamped, every later resolve of that release answered a dead link for
/// the rest of unknownOutcomeTTL, with no upstream call able to clear it. RD and Premiumize have no
/// Status to rediscover the torrent with, so nothing escaped it. A deadline belongs on a service that
/// went quiet, not on a client that hung up.
#[tokio::test]
#[ignore = "needs packages 1 and 2: the three Store impls (the give-up clock is not started by a cancel)"]
async fn stores_a_cancelled_add_does_not_start_the_give_up_clock() {
    // Enable when the owning package lands: move the body up and delete this wrapper.
    #[cfg(any())]
    async fn pending() {
        // (name, svc, add path)
        let cases: [(&str, DebridService, &'static str); 3] = [
            ("torbox", TorBox, "createtorrent"),
            ("realdebrid", RealDebrid, "addMagnet"),
            ("premiumize", Premiumize, "directdl"),
        ];
        for (name, svc, path) in cases {
            let _budget = install_budget(HOUR, 50);
            let cache = new_cache();
            for cause in ["context canceled", "context deadline exceeded"] {
                let store = build_for(
                    name,
                    doer(move |req| {
                        if req.url().path().contains(path) {
                            return Err(io::Error::other(format!("Post \"https://example/add\": {cause}")).into());
                        }
                        Ok(resp(200, "{}"))
                    }),
                    cache.clone(),
                );
                let _ = store.resolve(&tgt(H)).await;
                if let Some(stamp) = cache.get(&unknown_outcome_key(svc, "tok", H)) {
                    assert!(stamp.is_empty(), "{name}: {cause} started the give-up clock: the release is condemned for {:?}", add_give_up());
                }
                settle_add_attempt(&*cache, svc, "tok", H);
            }
            // A service that genuinely went quiet still starts it — the guard must be about the CAUSE,
            // not about switching the deadline off.
            let store = build_for(
                name,
                doer(move |req| {
                    if req.url().path().contains(path) {
                        return Err(io::Error::other("read tcp 10.0.0.2:443: connection reset by peer").into());
                    }
                    Ok(resp(200, "{}"))
                }),
                cache.clone(),
            );
            let _ = store.resolve(&tgt(H)).await;
            assert!(cache.get(&unknown_outcome_key(svc, "tok", H)).is_some_and(|s| !s.is_empty()), "{name}: a service that never answered left no give-up clock");
        }
    }
    todo!("needs packages 1 and 2: the three Store impls (the give-up clock is not started by a cancel)");
}

/// errServiceWentQuiet stands for the cause these tests mean: the service stopped answering, as opposed
/// to the client hanging up. Only the former starts the give-up clock.
///
/// The body-read branch is the other half of the same fact, and which one a back-out lands in is decided
/// only by whether the response headers arrived first. Guarding the transport branch alone left the same
/// release condemned by the same viewer action.
#[tokio::test]
#[ignore = "needs packages 1 and 2: the three Store impls, and a body that is cancelled mid-read (futures-util)"]
async fn stores_a_cancelled_body_read_does_not_start_the_give_up_clock() {
    // Enable when the owning package lands: move the body up and delete this wrapper.
    #[cfg(any())]
    async fn pending() {
        // (name, svc, add path)
        let cases: [(&str, DebridService, &'static str); 3] = [
            ("torbox", TorBox, "createtorrent"),
            ("realdebrid", RealDebrid, "addMagnet"),
            ("premiumize", Premiumize, "directdl"),
        ];
        for (name, svc, path) in cases {
            let _budget = install_budget(HOUR, 50);
            let cache = new_cache();
            // The add is ACCEPTED — 201 on RD, 200 elsewhere — and the body then dies because the viewer
            // moved on. The torrent very likely exists, which is exactly why condemning it is wrong.
            let store = build_for(
                name,
                doer(move |req| {
                    if req.url().path().contains(path) {
                        return Ok(cancelled_body_response(201, "context canceled"));
                    }
                    Ok(resp(200, "{}"))
                }),
                cache.clone(),
            );
            let _ = store.resolve(&tgt(H)).await;
            if let Some(stamp) = cache.get(&unknown_outcome_key(svc, "tok", H)) {
                assert!(stamp.is_empty(), "{name}: a back-out mid-body started the give-up clock: the release is condemned for {:?}", add_give_up());
            }

            // A body that dies for the SERVICE's own reasons still starts it.
            settle_add_attempt(&*cache, svc, "tok", H);
            let store = build_for(
                name,
                doer(move |req| {
                    if req.url().path().contains(path) {
                        return Ok(broken_body_response(201, "unexpected EOF mid-body"));
                    }
                    Ok(resp(200, "{}"))
                }),
                cache.clone(),
            );
            let _ = store.resolve(&tgt(H)).await;
            assert!(cache.get(&unknown_outcome_key(svc, "tok", H)).is_some_and(|s| !s.is_empty()), "{name}: a truncated answer left no give-up clock, so nothing bounds the retries");
        }
    }
    todo!("needs packages 1 and 2: the three Store impls, and a body that is cancelled mid-read (futures-util)");
}

/// TorBox reports plenty of errors as HTTP 200 with no torrent id, and a proxy page served as 200 lands
/// there too. Keeping the charge walked the whole hourly allowance in one sitting — this branch answers a
/// dead link, which is what makes the client fall through to the next candidate, so no poll loop is even
/// needed. RD already refunded the same shape.
#[tokio::test]
#[ignore = "needs packages 1 and 2: the TorBox and Real-Debrid Store impls (an empty 200 is not an add)"]
async fn add_budget_an_empty_two_hundred_is_not_an_add() {
    // Enable when the owning package lands: move the body up and delete this wrapper.
    #[cfg(any())]
    async fn pending() {
        // (name, svc, add path, body, build)
        let cases: [(&str, DebridService, &'static str, &'static str); 2] = [
            ("torbox", TorBox, "createtorrent", r#"{"success":false}"#),
            ("realdebrid", RealDebrid, "addMagnet", r#"{"ok":true}"#),
        ];
        for (name, svc, path, body) in cases {
            let _budget = install_budget(HOUR, 50);

            let adds = Arc::new(AtomicUsize::new(0));
            // Sixty DISTINCT releases: the client falling through candidates, not a poll loop.
            for i in 0..60 {
                let a = adds.clone();
                let store = build_for(
                    name,
                    doer(move |req| {
                        if req.url().path().contains(path) {
                            a.fetch_add(1, Ordering::SeqCst);
                            return Ok(resp(200, body));
                        }
                        Ok(resp(200, "{}"))
                    }),
                    new_cache(),
                );
                let _ = store.resolve(&tgt(&format!("{:0>40x}", i))).await;
            }
            let left = global_add_budget().remaining(&budget_account(svc, "tok"));
            let n = adds.load(Ordering::SeqCst);
            assert_eq!(left, 50, "{name}: allowance {left} after {n} empty answers — one bad title walks the hour");
        }
    }
    todo!("needs packages 1 and 2: the TorBox and Real-Debrid Store impls (an empty 200 is not an add)");
}

/// Refund what the ANSWER says was not created, and nothing more. `success:false` and an unparseable body
/// are the two cases the empty-2xx branch exists for; a body that parses, claims no failure and simply
/// carries an id under a name this struct does not know may describe a torrent that WAS created, and
/// refunding there would manufacture allowance against a real add.
#[tokio::test]
#[ignore = "needs package 1: the TorBox Store impl (the refund follows the answer)"]
async fn add_budget_tor_box_refunds_only_what_the_answer_denies() {
    // Enable when the owning package lands: move the body up and delete this wrapper.
    #[cfg(any())]
    async fn pending() {
        // (name, body, remaining out of 50)
        let cases: [(&str, &'static str, i64); 5] = [
            ("success:false", r#"{"success":false}"#, 50),
            ("an html proxy page", "<html>502 Bad Gateway</html>", 50),
            ("an id under a name we do not know", r#"{"data":{"queued_id":91}}"#, 49),
            ("a string id", r#"{"data":{"torrent_id":"7"}}"#, 49),
            ("a one-element array", r#"{"data":[{"torrent_id":7}]}"#, 49),
        ];
        for (name, body, want) in cases {
            let _budget = install_budget(HOUR, 50);

            let s = tb(
                "tok",
                doer(move |req| {
                    if is_add_endpoint(&req) {
                        return Ok(resp(200, body));
                    }
                    Ok(resp(200, "{}"))
                }),
                Some(new_cache()),
            );
            let _ = s.resolve(&tgt(H)).await;
            let left = global_add_budget().remaining(&budget_account(TorBox, "tok"));
            assert_eq!(left, want, "{name}: remaining = {left}, want {want}");
        }
    }
    todo!("needs package 1: the TorBox Store impl (the refund follows the answer)");
}

/// A miss is only worth remembering when the account list was READ and the hash was not in it. A timeout
/// or a cancelled poll is not the account saying no, and remembering it suppressed the one call that can
/// rediscover a queued torrent — for fifteen seconds, on /play as well as the probe route.
#[tokio::test]
#[ignore = "needs package 1: the TorBox Store impl (Status and the miss marker)"]
async fn tor_box_a_timed_out_listing_is_not_a_miss() {
    // Enable when the owning package lands: move the body up and delete this wrapper.
    #[cfg(any())]
    async fn pending() {
        let cache = new_cache();
        let failing = tb("tok", doer(|_| Err(io::Error::other("deadline has elapsed").into())), Some(cache.clone()));
        assert!(failing.status(&tgt(H)).await.is_none(), "a timed-out listing cannot report a status");
        assert!(cache.get(&torrent_miss_key("tok", H)).is_none(), "a timeout was remembered as the account not holding the torrent");

        // The list recovers, and the very next poll must be allowed to ask again.
        let healthy = tb(
            "tok",
            doer(|req| {
                if req.url().query().unwrap_or("").contains("id=") {
                    return Ok(resp(200, r#"{"data":{"progress":0.5,"download_finished":false}}"#));
                }
                Ok(resp(200, &format!(r#"{{"data":[{{"id":77,"hash":"{H}"}}]}}"#)))
            }),
            Some(cache.clone()),
        );
        assert!(healthy.status(&tgt(H)).await.is_some(), "the torrent was rediscoverable, but the miss marker suppressed the lookup");

        // An authoritative miss — the list was read and the hash is not in it — IS still remembered.
        let other = new_cache();
        let absent = tb("tok", doer(|_| Ok(resp(200, r#"{"data":[]}"#))), Some(other.clone()));
        let _ = absent.status(&tgt(H)).await;
        assert!(other.get(&torrent_miss_key("tok", H)).is_some(), "an answered miss must still be remembered, or every poll re-fetches the whole list");
    }
    todo!("needs package 1: the TorBox Store impl (Status and the miss marker)");
}

/// mylist is asked BY id, but it can answer with the whole account list — which is why the array fallback
/// exists at all. Taking the first entry blind is the mistake listFiles was hardened against, and here it
/// was worse: a finished first entry made a live download read as "no status", which handlePlay answers
/// 404 dead_link, so the client blacklists a release that is downloading right now — the one failure
/// Status exists to prevent. A downloading first entry is quieter and no better: the viewer watches
/// another torrent's percentage, ETA and rate.
#[tokio::test]
#[ignore = "needs package 1: the TorBox Store impl (Status reports on the torrent it asked about)"]
async fn tor_box_status_reports_on_the_torrent_it_asked_about() {
    // Enable when the owning package lands: move the body up and delete this wrapper.
    #[cfg(any())]
    async fn pending() {
        const OURS: i64 = 100;
        let finished_other = r#"{"id":42,"progress":1,"download_finished":true,"eta":0,"download_speed":0}"#;
        let busy_other = r#"{"id":42,"progress":0.43,"download_finished":false,"eta":900,"download_speed":1500000}"#;
        let ours_busy = r#"{"id":100,"progress":0.02,"download_finished":false,"eta":60,"download_speed":10}"#;

        let cases = [
            ("ours behind a finished torrent", format!("{finished_other},{ours_busy}")),
            ("ours behind a busy torrent", format!("{busy_other},{ours_busy}")),
            ("ours last of several", format!("{finished_other},{busy_other},{ours_busy}")),
        ];
        for (name, entries) in cases {
            let cache = new_cache();
            cache.put(&torrent_id_key("tok", H), &OURS.to_string(), resolve_cache_ttl());
            let body = format!(r#"{{"data":[{entries}]}}"#);
            let s = tb("tok", doer(move |_| Ok(resp(200, &body))), Some(cache));
            let got = s.status(&tgt(H)).await.expect("our torrent is in the list and downloading; this must report on it");
            assert!(got.progress == 0.02 && got.bytes_per_second == Some(10), "{name}: reported {got:?} — that is another torrent's progress");
        }

        // A list that does not mention our torrent says nothing about it, and must not borrow an answer.
        let cache = new_cache();
        cache.put(&torrent_id_key("tok", H), &OURS.to_string(), resolve_cache_ttl());
        let body = format!(r#"{{"data":[{busy_other}]}}"#);
        let s = tb("tok", doer(move |_| Ok(resp(200, &body))), Some(cache));
        assert!(s.status(&tgt(H)).await.is_none(), "reported a status for a torrent the answer never mentions");

        // The ordinary single-entry answer to a by-id query need not repeat the id, and still counts.
        let cache = new_cache();
        cache.put(&torrent_id_key("tok", H), &OURS.to_string(), resolve_cache_ttl());
        let s = tb(
            "tok",
            doer(|_| Ok(resp(200, r#"{"data":{"progress":0.25,"download_finished":false,"eta":30}}"#))),
            Some(cache),
        );
        let got = s.status(&tgt(H)).await;
        assert!(matches!(&got, Some(g) if g.progress == 0.25), "an unlabelled single entry is still ours: {got:?}");

        // But an object naming a DIFFERENT torrent is not.
        let cache = new_cache();
        cache.put(&torrent_id_key("tok", H), &OURS.to_string(), resolve_cache_ttl());
        let s = tb(
            "tok",
            doer(|_| Ok(resp(200, r#"{"data":{"id":42,"progress":0.9,"download_finished":false}}"#))),
            Some(cache),
        );
        assert!(s.status(&tgt(H)).await.is_none(), "reported a status from an entry that names torrent 42");
    }
    todo!("needs package 1: the TorBox Store impl (Status reports on the torrent it asked about)");
}

/// A 200 with no `data` key is TorBox's envelope missing, not the account saying it holds nothing —
/// listFiles reads the identical body as silence. Calling it a miss wrote a marker that then suppressed
/// the only lookup able to rediscover a queued torrent.
#[tokio::test]
#[ignore = "needs package 1: the TorBox Store impl (Status's envelope handling)"]
async fn tor_box_an_envelope_without_data_is_not_a_miss() {
    // Enable when the owning package lands: move the body up and delete this wrapper.
    #[cfg(any())]
    async fn pending() {
        for body in [r#"{}"#, r#"{"success":false}"#, r#"{"success":false,"data":null}"#] {
            let cache = new_cache();
            let s = tb("tok", doer(move |_| Ok(resp(200, body))), Some(cache.clone()));
            let _ = s.status(&tgt(H)).await;
            assert!(cache.get(&torrent_miss_key("tok", H)).is_none(), "{body} was remembered as the account not holding the torrent");
        }
        // A real, readable list that lacks the hash still is one.
        let cache = new_cache();
        let s = tb("tok", doer(|_| Ok(resp(200, r#"{"success":true,"data":[]}"#))), Some(cache.clone()));
        let _ = s.status(&tgt(H)).await;
        assert!(cache.get(&torrent_miss_key("tok", H)).is_some(), "an answered miss must still be remembered");
    }
    todo!("needs package 1: the TorBox Store impl (Status's envelope handling)");
}

/// A refusal is a fact about the account, not the release — and the last calls of RD's chain classified
/// nothing. `info`, one call earlier on the same read path, has drawn that line for a long time. So the
/// same throttle answered 503 or 404 depending only on which call it landed on: a release RD is holding
/// and would serve was reported dead, the client blacklisted it and walked the rest of the list, and
/// every candidate hit the same throttled endpoint for the same answer. Nothing was recorded either,
/// since resolveExisting only remembers a *StoreUnavailableError.
#[tokio::test]
#[ignore = "needs package 2: the Real-Debrid Store impl (every call in the chain classifies a refusal)"]
async fn real_debrid_every_call_in_the_chain_classifies_a_refusal() {
    // Enable when the owning package lands: move the body up and delete this wrapper.
    #[cfg(any())]
    async fn pending() {
        let info_ok = r#"{"files":[{"id":1,"path":"Show.S01E01.mkv","bytes":900,"selected":1}],"links":["l1"]}"#;
        // (name, selectFiles status, want unavailable)
        let cases: [(&str, u16, bool); 3] = [
            ("selectFiles throttled", 429, true),
            ("selectFiles faulting", 503, true),
            ("selectFiles rejecting the file", 400, false),
        ];
        for (name, status, want) in cases {
            let s = rd(
                "tok",
                doer(move |req| {
                    let path = req.url().path();
                    if path.contains("addMagnet") {
                        return Ok(resp(200, r#"{"id":"t1"}"#));
                    }
                    if path.contains("torrents/info") {
                        return Ok(resp(200, info_ok));
                    }
                    if path.contains("selectFiles") {
                        return Ok(resp(status, "{}"));
                    }
                    Ok(resp(200, "{}"))
                }),
                Some(new_cache()),
            );
            let res = s.resolve(&tgt(H)).await;
            assert_eq!(unavailable(&res).is_some(), want, "{name}: got {res:?}");
        }

        // The last call of the chain, on a release the account demonstrably holds.
        for code in [429u16, 503] {
            let cache = new_cache();
            let s = rd(
                "tok",
                doer(move |req| {
                    let path = req.url().path();
                    if path.contains("addMagnet") {
                        return Ok(resp(200, r#"{"id":"t1"}"#));
                    }
                    if path.contains("torrents/info") {
                        return Ok(resp(200, info_ok));
                    }
                    if path.contains("selectFiles") {
                        return Ok(resp(200, "{}"));
                    }
                    if path.contains("unrestrict") {
                        return Ok(resp(code, "{}"));
                    }
                    Ok(resp(200, "{}"))
                }),
                Some(cache.clone()),
            );
            let res = s.resolve(&tgt(H)).await;
            assert!(unavailable(&res).is_some(), "unrestrict {code} answered {res:?} — a throttle is not a dead release");
            // And being classified is what gets it remembered, so the next poll costs one call rather than four.
            assert!(backed_off(&*cache, RealDebrid, "tok", H).is_some(), "unrestrict {code} left no backoff, so nothing slows the retries");
        }
    }
    todo!("needs package 2: the Real-Debrid Store impl (every call in the chain classifies a refusal)");
}

/// Premiumize's HTTP-200 `status:error` branch is the one carrying its real refusals, and it was the only
/// detail site splicing the upstream message in unredacted — into text that is logged AND persisted into
/// a refusal cache that writes through to disk, from where the probe route prints it back.
#[tokio::test]
#[ignore = "needs package 2: the Premiumize Store impl (the status:error message is redacted)"]
async fn premiumize_the_status_error_message_cannot_carry_the_token() {
    // Enable when the owning package lands: move the body up and delete this wrapper.
    #[cfg(any())]
    async fn pending() {
        let _budget = install_budget(HOUR, 50);

        let token = "pm-super-secret-apikey";
        let cache = new_cache();
        let s = pm(
            token,
            doer(|req| {
                Ok(resp(200, &format!(r#"{{"status":"error","message":"invalid src for {}"}}"#, body_of(&req))))
            }),
            Some(cache.clone()),
        );
        let res = s.resolve(&tgt(H)).await;
        let msg = res.as_ref().err().map(|e| e.to_string()).unwrap_or_default();
        assert!(res.is_err() && !msg.contains(token), "the token reached an error that gets logged: {res:?}");
        if let Some(reason) = backed_off(&*cache, Premiumize, token, H) {
            assert!(!reason.contains(token), "the token was persisted into the refusal cache: {reason}");
        }
    }
    todo!("needs package 2: the Premiumize Store impl (the status:error message is redacted)");
}

/// listFiles and Status parse the same mylist payload; they must not disagree about whose entry it is.
/// listFiles checked the id only when the array had more than one element, so a single foreign entry
/// still handed back its files.
#[tokio::test]
#[ignore = "needs package 1: the TorBox Store impl (listFiles matches the id at any length)"]
async fn tor_box_list_files_matches_the_id_at_any_length() {
    // Enable when the owning package lands: move the body up and delete this wrapper.
    #[cfg(any())]
    async fn pending() {
        let foreign = r#"{"id":42,"files":[{"id":7,"name":"Wanted.S01E01.mkv","size":10}]}"#;
        let ours = r#"{"id":100,"files":[{"id":3,"name":"Show.S01E01.mkv","size":10}]}"#;
        // (name, payload, want error)
        let cases: [(&str, String, bool); 6] = [
            ("a lone foreign entry, object form", format!(r#"{{"data":{foreign}}}"#), true),
            ("a lone foreign entry, array form", format!(r#"{{"data":[{foreign}]}}"#), true),
            ("ours, object form", format!(r#"{{"data":{ours}}}"#), false),
            ("ours, array form", format!(r#"{{"data":[{ours}]}}"#), false),
            ("ours behind a foreign entry", format!(r#"{{"data":[{foreign},{ours}]}}"#), false),
            // An entry with no id at all is still taken at its word.
            ("an unlabelled entry", r#"{"data":{"files":[{"id":3,"name":"Show.S01E01.mkv","size":10}]}}"#.to_string(), false),
        ];
        for (name, payload, want_err) in cases {
            let s = tb("tok", doer(move |_| Ok(resp(200, &payload))), None);
            let res = s.list_files(100);
            if want_err {
                assert!(res.is_err(), "{name}: accepted another torrent's file list: {res:?}");
                continue;
            }
            assert!(matches!(&res, Ok(f) if f.len() == 1 && f[0].name == "Show.S01E01.mkv"), "{name}: got {res:?}");
        }
    }
    todo!("needs package 1: the TorBox Store impl (listFiles matches the id at any length)");
}

/// An exact id wins; an unlabelled entry is only a fallback. Taking the first of either meant an id-less
/// entry ahead of ours in a multi-entry answer was chosen over the entry that actually names our torrent
/// — worse than the blind arr[0] it replaced, because it looks like a check.
#[tokio::test]
#[ignore = "needs package 1: the TorBox Store impl (listFiles prefers an exact id)"]
async fn tor_box_list_files_an_exact_id_beats_an_unlabelled_entry() {
    // Enable when the owning package lands: move the body up and delete this wrapper.
    #[cfg(any())]
    async fn pending() {
        let ours = r#"{"id":100,"files":[{"id":3,"name":"Show.S01E01.mkv","size":10}]}"#;
        let unlabelled = r#"{"files":[{"id":7,"name":"OTHER.mkv","size":10}]}"#;
        let zero_id = r#"{"id":0,"files":[{"id":7,"name":"OTHER.mkv","size":10}]}"#;
        // (name, payload, want file name)
        let cases: [(&str, String, &str); 4] = [
            ("unlabelled first", format!(r#"{{"data":[{unlabelled},{ours}]}}"#), "Show.S01E01.mkv"),
            ("id 0 first", format!(r#"{{"data":[{zero_id},{ours}]}}"#), "Show.S01E01.mkv"),
            ("ours first", format!(r#"{{"data":[{ours},{unlabelled}]}}"#), "Show.S01E01.mkv"),
            // With no exact match anywhere, an unlabelled entry is still better than nothing.
            ("only unlabelled", format!(r#"{{"data":[{unlabelled}]}}"#), "OTHER.mkv"),
        ];
        for (name, payload, want) in cases {
            let s = tb("tok", doer(move |_| Ok(resp(200, &payload))), None);
            let res = s.list_files(100);
            assert!(matches!(&res, Ok(f) if f.len() == 1 && f[0].name == want), "{name}: got {res:?} — want {want}");
        }
    }
    todo!("needs package 1: the TorBox Store impl (listFiles prefers an exact id)");
}

/// A 403 from unrestrict is RD refusing a FILE, not the account: realDebridBlocked exists because RD
/// rejects specific files, and refusalIsAboutTheAccount keys on 401/403 — so passing it through escalated
/// one unplayable file into a sixty-second account-wide outage that took unrelated releases with it.
#[tokio::test]
#[ignore = "needs package 2: the Real-Debrid Store impl (a 403 on one file is not the account)"]
async fn real_debrid_a_forbidden_link_is_not_a_forbidden_account() {
    // Enable when the owning package lands: move the body up and delete this wrapper.
    #[cfg(any())]
    async fn pending() {
        let info_ok = r#"{"files":[{"id":1,"path":"Show.S01E01.mkv","bytes":900,"selected":1}],"links":["l1"]}"#;
        // (unrestrict status, want account-wide backoff)
        let cases: [(u16, bool); 3] = [
            (403, false), // one file RD will not serve
            (401, true),  // the key itself
            (429, false), // a throttle: per-release, not account-wide
        ];
        for (code, want_account) in cases {
            let cache = new_cache();
            let s = rd(
                "tok",
                doer(move |req| {
                    let path = req.url().path();
                    if path.contains("addMagnet") {
                        return Ok(resp(200, r#"{"id":"t1"}"#));
                    }
                    if path.contains("torrents/info") {
                        return Ok(resp(200, info_ok));
                    }
                    if path.contains("selectFiles") {
                        return Ok(resp(200, "{}"));
                    }
                    if path.contains("unrestrict") {
                        return Ok(resp(code, "{}"));
                    }
                    Ok(resp(200, "{}"))
                }),
                Some(cache.clone()),
            );
            let _ = s.resolve(&tgt(H)).await;
            let got = account_backed_off(&*cache, RealDebrid, "tok").is_some();
            assert_eq!(got, want_account, "unrestrict {code}: account backoff = {got}, want {want_account}");
        }
    }
    todo!("needs package 2: the Real-Debrid Store impl (a 403 on one file is not the account)");
}

/// The warm-entry fast path returns a refusal from the same requestdl call the held path uses, and only
/// the held path remembered it. A warm entry is the normal state during a binge, so a rejected key was
/// re-asked once per poll — and the account gate a few lines above already said as much ("that branch
/// records no refusal either, so it could not even set the key it skipped") without fixing it.
#[tokio::test]
#[ignore = "needs package 1: the TorBox Store impl (the warm path remembers its refusal)"]
async fn tor_box_the_warm_path_remembers_its_refusal() {
    // Enable when the owning package lands: move the body up and delete this wrapper.
    #[cfg(any())]
    async fn pending() {
        // (name, requestdl status, want account-wide)
        let cases: [(&str, u16, bool); 2] = [("a throttle", 429, false), ("a rejected key", 401, true)];
        for (name, status, want_account) in cases {
            let calls = Arc::new(AtomicUsize::new(0));
            let c = calls.clone();
            let cache = new_cache();
            cache.put(
                &resolve_key("tok", H),
                r#"{"torrentId":42,"files":[{"Index":0,"Name":"m.mkv","SizeBytes":9}]}"#,
                resolve_cache_ttl(),
            );
            let s = tb(
                "tok",
                doer(move |req| {
                    if req.url().path().contains("requestdl") {
                        c.fetch_add(1, Ordering::SeqCst);
                        return Ok(resp(status, "{}"));
                    }
                    Ok(resp(200, "{}"))
                }),
                Some(cache.clone()),
            );
            let target = ResolveTarget { file_idx: intp(0), ..tgt(H) };
            for _ in 0..10 {
                let _ = s.resolve(&target).await;
            }
            assert!(backed_off(&*cache, TorBox, "tok", H).is_some(), "{name}: the refusal was not remembered, so every poll re-asks");
            let n = calls.load(Ordering::SeqCst);
            assert!(n <= 1, "{name}: asked {n} times across ten polls");
            let got = account_backed_off(&*cache, TorBox, "tok").is_some();
            assert_eq!(got, want_account, "{name}: account backoff = {got}, want {want_account}");
        }
    }
    todo!("needs package 1: the TorBox Store impl (the warm path remembers its refusal)");
}

/// listFiles and Status read the same mylist payload and must agree about whose entry it is. Status was
/// left on first-of-either, and it is the reader that decides "downloading" against "dead": a finished
/// stranger at the head of the array made a live download report nothing, which /play answers 404
/// dead_link — the client blacklists a release that is downloading right now.
#[tokio::test]
#[ignore = "needs package 1: the TorBox Store impl (Status prefers an exact id)"]
async fn tor_box_status_an_exact_id_beats_an_unlabelled_entry() {
    // Enable when the owning package lands: move the body up and delete this wrapper.
    #[cfg(any())]
    async fn pending() {
        const OURS: i64 = 42;
        let unlabelled_finished = r#"{"progress":1,"download_finished":true,"eta":0,"download_speed":0}"#;
        let stranger_busy = r#"{"id":9,"progress":0.11,"download_finished":false,"eta":9999,"download_speed":5}"#;
        let ours_busy = r#"{"id":42,"progress":0.5,"download_finished":false,"eta":600,"download_speed":900000}"#;
        let cases = [
            ("an unlabelled finished entry first", format!("{unlabelled_finished},{ours_busy}")),
            ("a stranger downloading first", format!("{stranger_busy},{ours_busy}")),
            ("both ahead of ours", format!("{unlabelled_finished},{stranger_busy},{ours_busy}")),
        ];
        for (name, entries) in cases {
            let cache = new_cache();
            cache.put(&torrent_id_key("tok", H), &OURS.to_string(), resolve_cache_ttl());
            let body = format!(r#"{{"data":[{entries}]}}"#);
            let s = tb("tok", doer(move |_| Ok(resp(200, &body))), Some(cache));
            let got = s.status(&tgt(H)).await.expect("our torrent is in the list and downloading; this must report on it");
            assert!(got.progress == 0.5, "{name}: reported {got:?} — that is another torrent's progress");
        }

        // With no exact match anywhere, an id-less entry is still the best available answer.
        let cache = new_cache();
        cache.put(&torrent_id_key("tok", H), &OURS.to_string(), resolve_cache_ttl());
        let s = tb(
            "tok",
            doer(|_| Ok(resp(200, r#"{"data":[{"progress":0.25,"download_finished":false}]}"#))),
            Some(cache),
        );
        let got = s.status(&tgt(H)).await;
        assert!(matches!(&got, Some(g) if g.progress == 0.25), "an unlabelled lone entry is still ours: {got:?}");
    }
    todo!("needs package 1: the TorBox Store impl (Status prefers an exact id)");
}

/// A read-only caller is exempt from the gate that READS the per-release backoff, so it must not WRITE
/// one either: the probe re-stamped a refusal it would never consult, which kept /play gated for exactly
/// as long as the probe kept polling.
#[tokio::test]
#[ignore = "needs package 1: the TorBox Store impl (a read-only resolve writes no backoff)"]
async fn tor_box_a_read_only_resolve_writes_no_backoff() {
    // Enable when the owning package lands: move the body up and delete this wrapper.
    #[cfg(any())]
    async fn pending() {
        let cache = new_cache();
        cache.put(
            &resolve_key("tok", H),
            r#"{"torrentId":42,"files":[{"Index":0,"Name":"m.mkv","SizeBytes":9}]}"#,
            resolve_cache_ttl(),
        );
        let s = tb(
            "tok",
            doer(|req| {
                if req.url().path().contains("requestdl") {
                    return Ok(resp(429, "{}"));
                }
                Ok(resp(200, "{}"))
            }),
            Some(cache.clone()),
        );
        let probe = ResolveTarget { file_idx: intp(0), no_add: true, ..tgt(H) };
        for _ in 0..5 {
            let _ = s.resolve(&probe).await;
        }
        assert!(backed_off(&*cache, TorBox, "tok", H).is_none(), "a read-only resolve wrote the backoff that gates /play");
        // And a real play still records it, so the poll loop is bounded where it matters.
        let _ = s.resolve(&ResolveTarget { file_idx: intp(0), ..tgt(H) }).await;
        assert!(backed_off(&*cache, TorBox, "tok", H).is_some(), "a queueing resolve must still remember the refusal");
    }
    todo!("needs package 1: the TorBox Store impl (a read-only resolve writes no backoff)");
}

/// recordRefusal feeds two memories and a read-only caller stands differently to each. The per-release
/// key is an add-path guard, so a probe must not write one — it would gate the /play behind it for a
/// minute with no upstream call. The ACCOUNT key is read unconditionally at the top of Resolve, NoAdd
/// included, because a rejected key makes every request pointless; guarding both together left the probe
/// unable to set the very key it then reads, so it re-asked a rejected endpoint once per poll.
#[tokio::test]
#[ignore = "needs package 1: the TorBox Store impl (recordRefusal's two memories)"]
async fn tor_box_a_read_only_resolve_writes_only_the_account_refusal() {
    // Enable when the owning package lands: move the body up and delete this wrapper.
    #[cfg(any())]
    async fn pending() {
        // Both paths that record: the warm resolve entry, and the held torrent id.
        for path in ["warm", "held"] {
            // (name, status, want per-release, want account)
            let cases: [(&str, u16, bool, bool); 2] = [
                ("a throttle", 429, false, false),
                // A rejected key writes both, because recordRefusal writes the per-release key alongside the
                // account one. That is harmless where it would not be for a throttle: the account gate blocks
                // every release on this token anyway, so the narrower key gates nothing that was not already
                // gated. What matters is that a THROTTLE — an add-path fact — writes neither.
                ("a rejected key", 401, true, true),
            ];
            for (name, status, want_per_release, want_account) in cases {
                let calls = Arc::new(AtomicUsize::new(0));
                let c = calls.clone();
                let cache = new_cache();
                if path == "warm" {
                    cache.put(
                        &resolve_key("tok", H),
                        r#"{"torrentId":42,"files":[{"Index":0,"Name":"m.mkv","SizeBytes":9}]}"#,
                        resolve_cache_ttl(),
                    );
                } else {
                    cache.put(&torrent_id_key("tok", H), "42", resolve_cache_ttl());
                }
                let s = tb(
                    "tok",
                    doer(move |req| {
                        if req.url().path().contains("requestdl") {
                            c.fetch_add(1, Ordering::SeqCst);
                            return Ok(resp(status, "{}"));
                        }
                        Ok(resp(200, r#"{"data":{"id":42,"files":[{"id":0,"name":"m.mkv","size":9}]}}"#))
                    }),
                    Some(cache.clone()),
                );
                let probe = ResolveTarget { file_idx: intp(0), no_add: true, ..tgt(H) };
                for _ in 0..5 {
                    let _ = s.resolve(&probe).await;
                }
                let per_release = backed_off(&*cache, TorBox, "tok", H).is_some();
                assert_eq!(per_release, want_per_release, "{path}/{name}: per-release backoff = {per_release} — a probe must not gate /play");
                let account = account_backed_off(&*cache, TorBox, "tok").is_some();
                assert_eq!(account, want_account, "{path}/{name}: account backoff = {account}");
                // A dead key must stop the probe re-asking; a throttle is an add-path fact and does not.
                let n = calls.load(Ordering::SeqCst);
                if want_account {
                    assert!(n <= 1, "{path}/{name}: asked a rejected endpoint {n} times across five polls");
                }
            }
        }

        // And a queueing resolve still writes the per-release key, which is where the bound is wanted.
        let cache = new_cache();
        cache.put(&torrent_id_key("tok", H), "42", resolve_cache_ttl());
        let s = tb(
            "tok",
            doer(|req| {
                if req.url().path().contains("requestdl") {
                    return Ok(resp(429, "{}"));
                }
                Ok(resp(200, r#"{"data":{"id":42,"files":[{"id":0,"name":"m.mkv","size":9}]}}"#))
            }),
            Some(cache.clone()),
        );
        let _ = s.resolve(&ResolveTarget { file_idx: intp(0), ..tgt(H) }).await;
        assert!(backed_off(&*cache, TorBox, "tok", H).is_some(), "a queueing resolve must still remember the refusal");
    }
    todo!("needs package 1: the TorBox Store impl (recordRefusal's two memories)");
}

/// The account listing is the WHOLE account, and it was pulled once per infohash because the only memo
/// was the per-hash miss marker. A poster grid probing eight releases made eight identical full-list
/// requests. The question "what does this account hold?" has one answer at a time.
#[tokio::test]
#[ignore = "needs package 1: the TorBox Store impl (the account listing is memoised once per account)"]
async fn tor_box_the_account_listing_is_fetched_once_per_account() {
    // Enable when the owning package lands: move the body up and delete this wrapper.
    #[cfg(any())]
    async fn pending() {
        // A realistic account: 300 torrents, one of which is the one being asked about.
        let mut entries: Vec<String> = (0..300)
            .map(|i| format!(r#"{{"id":{i},"hash":"{}"}}"#, repeat(&format!("{:x}", i % 16), 40)))
            .collect();
        entries.push(format!(r#"{{"id":999,"hash":"{H}"}}"#));
        let payload = format!(r#"{{"success":true,"data":[{}]}}"#, entries.join(","));

        let fetches = Arc::new(AtomicUsize::new(0));
        let bytes_out = Arc::new(AtomicUsize::new(0));
        let cache = new_cache();
        let make = |f: Arc<AtomicUsize>, b: Arc<AtomicUsize>| {
            let payload = payload.clone();
            tb(
                "tok",
                doer(move |req| {
                    if req.url().path().contains("mylist") && !req.url().query().unwrap_or("").contains("id=") {
                        f.fetch_add(1, Ordering::SeqCst);
                        b.fetch_add(payload.len(), Ordering::SeqCst);
                        return Ok(resp(200, &payload));
                    }
                    Ok(resp(200, r#"{"data":{"progress":0.5,"download_finished":false}}"#))
                }),
                Some(cache.clone()),
            )
        };

        // Eight different releases, the poster-grid shape. Only one is in the account.
        let mut hashes = vec![H.to_string()];
        for i in 0..7 {
            hashes.push(repeat(&format!("{i:x}"), 40));
        }
        for h in &hashes {
            let _ = make(fetches.clone(), bytes_out.clone()).status(&tgt(h)).await;
        }
        let n = fetches.load(Ordering::SeqCst);
        let b = bytes_out.load(Ordering::SeqCst);
        assert_eq!(n, 1, "fetched the account listing {n} times for {} releases ({b} bytes)", hashes.len());
        // And the answer is still right for both a hash it holds and one it does not.
        assert!(make(fetches.clone(), bytes_out.clone()).status(&tgt(H)).await.is_some(), "a held, downloading torrent must still be found through the memo");
        let absent = "dead".repeat(10); // 40 chars, and not one of the generated single-digit hashes
        assert!(make(fetches.clone(), bytes_out.clone()).status(&tgt(&absent)).await.is_none(), "a hash the account does not hold must not be found");
    }
    todo!("needs package 1: the TorBox Store impl (the account listing is memoised once per account)");
}

/// The same hash is asked about three times within seconds — by /stream, by ?probe=1, and by /play on a
/// two-account install. "Held" is stable, so one answer serves all three. "Not held" is not: it becomes
/// held the instant a download finishes, and noticing that transition is the entire job of the poll loop,
/// so a negative is never memoised.
#[tokio::test]
#[ignore = "needs packages 1 and 2: the TorBox and Premiumize Store impls (CacheCheck memoises held only)"]
async fn cache_check_remembers_held_but_never_not_held() {
    // Enable when the owning package lands: move the body up and delete this wrapper.
    #[cfg(any())]
    async fn pending() {
        // (name, svc, body for a held / not-held answer, build)
        let cases: [(&str, DebridService, fn(bool) -> &'static str); 2] = [
            ("torbox", TorBox, |held| if held { r#"{"data":{"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa":{"name":"x"}}}"# } else { r#"{"data":{}}"# }),
            ("premiumize", Premiumize, |held| if held { r#"{"status":"success","response":[true]}"# } else { r#"{"status":"success","response":[false]}"# }),
        ];
        for (name, svc, body_for) in cases {
            // A release the account HOLDS: asked once, answered three times.
            let calls = Arc::new(AtomicUsize::new(0));
            let c = calls.clone();
            let cache = new_cache();
            let s = build_for(name, doer(move |_| { c.fetch_add(1, Ordering::SeqCst); Ok(resp(200, body_for(true))) }), cache.clone());
            for i in 0..3 {
                let got = s.cache_check(&[H.to_string()]).await;
                assert!(matches!(&got, Ok(m) if m.get(H) == Some(&true)), "{name}: check {i}: {got:?}");
            }
            let n = calls.load(Ordering::SeqCst);
            assert_eq!(n, 1, "{name}: asked {n} times about a release the account holds");

            // A release it does NOT hold: asked every time, because the answer is what changes when the
            // download lands.
            let calls = Arc::new(AtomicUsize::new(0));
            let c = calls.clone();
            let held = Arc::new(AtomicBool::new(false));
            let h = held.clone();
            let cache = new_cache();
            let s = build_for(name, doer(move |_| { c.fetch_add(1, Ordering::SeqCst); Ok(resp(200, body_for(h.load(Ordering::SeqCst)))) }), cache.clone());
            for i in 0..3 {
                let got = s.cache_check(&[H.to_string()]).await;
                assert!(!matches!(&got, Ok(m) if m.get(H) == Some(&true)), "{name}: check {i} claimed a release that is still downloading");
            }
            let n = calls.load(Ordering::SeqCst);
            assert_eq!(n, 3, "{name}: asked {n} times about a downloading release — the poll must see it land");
            // It lands, and the very next check must notice.
            held.store(true, Ordering::SeqCst);
            let got = s.cache_check(&[H.to_string()]).await;
            assert!(matches!(&got, Ok(m) if m.get(H) == Some(&true)), "{name}: the download finished and the next check did not notice");
            let _ = svc;
        }
    }
    todo!("needs packages 1 and 2: the TorBox and Premiumize Store impls (CacheCheck memoises held only)");
}
