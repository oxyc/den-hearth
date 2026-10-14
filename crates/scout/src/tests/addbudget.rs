//! Go: `internal/scout/addbudget_test.go` (3782 lines, 101 tests). Work package 3.
//!
//! The tests that exercise the budget alone run against `addbudget.rs` and fail at its `todo!()` stubs until it
//! lands. Most of the file drives the stores, so it also needs the store impls, the refusal memory and the
//! handler from packages 1, 2 and 4, which have not landed. Those items are stood in for by `foreign` at the
//! bottom of this file so the crate compiles; every test that reaches one is `#[ignore]`d. When an owner lands,
//! delete its stand-ins here and point the test at the real item.

use std::io;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
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
use crate::stores::{
    DeadLinkError, ResolveTarget, Store, StoreError, StoreUnavailableError, ERR_TORRENT_GONE, ERR_WOULD_ADD,
};
use crate::tests::mock::{resp, MockDoer};
use crate::util::key_hash;

use self::foreign::*;

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

/// A torrent the account already holds is resolved from the account, not bought again.
///
/// The resolve entry is a six-hour convenience cache, not a record of what the account has — so anything
/// not played in the last six hours, which is most of a library, fell through to createtorrent and paid
/// an add for a torrent already sitting there. The account listing answers this and was already being
/// consulted by Status; it just was not consulted here.
#[tokio::test]
#[ignore = "needs package 1: the TorBox Store impl (Status and the held-torrent path)"]
async fn add_budget_a_held_torrent_is_not_bought_again() {
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

/// What the ceiling counts is adds that may HAVE HAPPENED, and the dividing line is whether the service
/// answered. A request that reached the wire and was never answered keeps its charge — the torrent may
/// well exist, which is why refundAdd will not give it back. An answered non-2xx is the opposite: the
/// service said it created nothing, so holding the charge only erodes the allowance. All three stores
/// draw the line in the same place; TorBox used to keep both, so a repeatedly polled bad magnet ate its
/// hourly allowance where RD and Premiumize were immune.
#[tokio::test]
#[ignore = "needs package 1: the TorBox Store impl (the add path's charge)"]
async fn add_budget_the_charge_follows_whether_the_service_answered() {
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

/// A NoAdd target can never queue a torrent, whatever the store would otherwise do.
///
/// The probe path already chose carefully — it only resolves releases a cache check reported as held —
/// and still POSTed createtorrent, because TorBox's checkcached says what TORBOX has, not what this
/// ACCOUNT has. A caller cannot promise this by choosing well; only the store can enforce it.
#[tokio::test]
#[ignore = "needs packages 1 and 2: the three Store impls (the NoAdd path)"]
async fn no_add_never_reaches_an_add_endpoint() {
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

/// Scout's own ceiling is not the debrid's refusal, and must not be remembered or reported as one.
#[test]
#[ignore = "needs package 2: stores/refusal.rs (recordRefusal and backedOff)"]
fn spend_add_our_ceiling_is_not_the_stores_refusal() {
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

/// /health reports the allowance without naming accounts. The route is unauthenticated, and a stable
/// service:hash(token) key would make it a confirmation oracle for a guessed token.
#[test]
#[ignore = "needs package 4: handler.rs and deps.rs (Deps, the /health route)"]
fn health_reports_the_budget_without_naming_accounts() {
    let _budget = install_budget(HOUR, 10);
    global_add_budget().take(&budget_account(TorBox, "secret-token"), false);

    let body = health_body(new_cache());
    assert!(body.contains("addBudgetRemaining"), "a spent allowance must be visible: {body}");
    assert!(
        !body.contains(&key_hash("secret-token")) && !body.contains("torbox"),
        "/health discloses which account is spending: {body}"
    );
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

/// EVERY store that can add needs the in-flight guard. Giving it to TorBox alone did not stop the cancel
/// loop — it moved it: ResolvePreferring walks on to the store without a marker, and fifty cancelled
/// polls closed THAT account's allowance instead.
#[tokio::test]
#[ignore = "needs packages 1 and 2: the three Store impls (the in-flight guard)"]
async fn add_in_flight_applies_to_every_store_that_adds() {
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

/// Finding the torrent in the account settles the in-flight marker — that add plainly landed.
///
/// Without this the marker outlives the fact it stood in for: the release stays "awaiting the result"
/// for its full 90s window while the result is sitting in the account listing, and every poll in between
/// is refused for a state that has already resolved itself.
#[test]
#[ignore = "needs package 1: the TorBox Store impl (torrentID) and package 2: refusal.rs (addInFlight)"]
fn add_in_flight_discovering_the_torrent_settles_the_marker() {
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

/// An add going out clears the torrent-miss marker.
///
/// That marker suppresses the account listing for 15s, and the listing is the only thing that can find
/// the torrent the add just created — so leaving it kept the next poll on the "nothing is queued" path
/// instead of "it is downloading". This line was deleted once during a refactor, with its explanation,
/// and nothing failed.
#[test]
#[ignore = "needs package 2: stores/refusal.rs (torrentMissKey, noteAddAttempt)"]
fn note_add_attempt_clears_the_torrent_miss_marker() {
    let cache = new_cache();
    cache.put(&torrent_miss_key("tok", H), "1", torrent_miss_ttl()); // Status looked and found nothing
    note_add_attempt(&*cache, TorBox, "tok", H);
    assert!(
        cache.get(&torrent_miss_key("tok", H)).is_none(),
        "the miss marker outlived the add that disproves it; Status cannot see the new torrent"
    );
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

/// A pack that does not contain the requested episode is a PERMANENT fact about a torrent the account
/// has. Re-buying the pack to re-learn it spends an add and returns the identical refusal.
#[tokio::test]
#[ignore = "needs package 1: the TorBox Store impl (selectFileID and the held-torrent path)"]
async fn known_torrent_id_a_pack_mismatch_does_not_re_buy_the_pack() {
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

/// Every cache key that carries account state is scoped by the account identity.
///
/// The comments say what happens otherwise — "would let one user's cached torrent_id be used with another
/// user's token → wrong/other-account content" — and the cache is process-global, so it is reachable.
/// Stripping keyHash(token) from any of these left the whole suite green.
#[test]
#[ignore = "needs package 2: stores/refusal.rs (the cache keys)"]
fn cache_keys_are_scoped_to_the_account() {
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

// ---- Stand-ins for items other packages own -----------------------------------------------------------------
//
// Each item below is `todo!()` and stands in for a module that has not landed (the package is named in its
// comment). When the owner lands: delete the stand-in here, import the real item from its module, and drop
// the `#[ignore]` from the tests that use it. Nothing here is a port; nothing here may be relied on.
mod foreign {
    use std::collections::HashMap;
    use std::error::Error;
    use std::sync::Arc;
    use std::time::Duration;

    use async_trait::async_trait;

    use crate::cache::Cache;
    use crate::config::DebridService;
    use crate::season::TorrentFile;
    use crate::stores::pool::StorePool;
    use crate::stores::premiumize::PremiumizeStore;
    use crate::stores::realdebrid::RealDebridStore;
    use crate::stores::torbox::TorBoxStore;
    use crate::stores::{ResolveTarget, Store, StoreError, StoreStatus};

    // Package 1 (stores/torbox.rs) and package 2 (stores/realdebrid.rs, stores/premiumize.rs): the Store impls.

    #[async_trait]
    impl Store for TorBoxStore {
        fn service(&self) -> DebridService {
            todo!("stores/torbox.rs: TorBoxStore's Store impl")
        }
        async fn cache_check(&self, hashes: &[String]) -> Result<HashMap<String, bool>, StoreError> {
            todo!("stores/torbox.rs: TorBoxStore's Store impl")
        }
        async fn resolve(&self, t: &ResolveTarget) -> Result<String, StoreError> {
            todo!("stores/torbox.rs: TorBoxStore's Store impl")
        }
        async fn status(&self, t: &ResolveTarget) -> Option<StoreStatus> {
            todo!("stores/torbox.rs: TorBoxStore's Store impl")
        }
    }

    #[async_trait]
    impl Store for RealDebridStore {
        fn service(&self) -> DebridService {
            todo!("stores/realdebrid.rs: RealDebridStore's Store impl")
        }
        async fn cache_check(&self, hashes: &[String]) -> Result<HashMap<String, bool>, StoreError> {
            todo!("stores/realdebrid.rs: RealDebridStore's Store impl")
        }
        async fn resolve(&self, t: &ResolveTarget) -> Result<String, StoreError> {
            todo!("stores/realdebrid.rs: RealDebridStore's Store impl")
        }
        async fn status(&self, t: &ResolveTarget) -> Option<StoreStatus> {
            todo!("stores/realdebrid.rs: RealDebridStore's Store impl")
        }
    }

    #[async_trait]
    impl Store for PremiumizeStore {
        fn service(&self) -> DebridService {
            todo!("stores/premiumize.rs: PremiumizeStore's Store impl")
        }
        async fn cache_check(&self, hashes: &[String]) -> Result<HashMap<String, bool>, StoreError> {
            todo!("stores/premiumize.rs: PremiumizeStore's Store impl")
        }
        async fn resolve(&self, t: &ResolveTarget) -> Result<String, StoreError> {
            todo!("stores/premiumize.rs: PremiumizeStore's Store impl")
        }
        async fn status(&self, t: &ResolveTarget) -> Option<StoreStatus> {
            todo!("stores/premiumize.rs: PremiumizeStore's Store impl")
        }
    }

    // Package 1 (stores/torbox.rs): Go `torBoxStore`'s unexported methods.

    pub(super) trait TorBoxInternals {
        /// Go `torrentID`: the account's id for the hash, whether it was found, and the error when it was not
        /// answered.
        fn torrent_id(&self, info_hash: &str) -> (i64, bool, Option<StoreError>);
        /// Go `listFiles`.
        fn list_files(&self, torrent_id: i64) -> Result<Vec<TorrentFile>, StoreError>;
    }

    impl TorBoxInternals for TorBoxStore {
        fn torrent_id(&self, info_hash: &str) -> (i64, bool, Option<StoreError>) {
            todo!("stores/torbox.rs: torBoxStore.torrentID")
        }
        fn list_files(&self, torrent_id: i64) -> Result<Vec<TorrentFile>, StoreError> {
            todo!("stores/torbox.rs: torBoxStore.listFiles")
        }
    }

    /// Go `selectFileID` (stores/torbox.rs).
    pub(super) fn select_file_id(files: &[TorrentFile], t: &ResolveTarget) -> Result<i64, StoreError> {
        todo!("stores/torbox.rs: selectFileID")
    }

    // Package 2 (stores/realdebrid.rs): Go `realDebridStore`'s unexported methods, and the RD link mapping.

    pub(super) trait RealDebridInternals {
        /// Go `knownTorrent`: the remembered torrent id for the target, and whether one is held.
        fn known_torrent(&self, t: &ResolveTarget) -> (String, bool);
        /// Go `rememberTorrent`.
        fn remember_torrent(&self, t: &ResolveTarget, id: &str);
        /// Go `pickFileID`.
        fn pick_file_id(&self, files: &[TorrentFile], t: &ResolveTarget) -> Result<i64, StoreError>;
    }

    impl RealDebridInternals for RealDebridStore {
        fn known_torrent(&self, t: &ResolveTarget) -> (String, bool) {
            todo!("stores/realdebrid.rs: realDebridStore.knownTorrent")
        }
        fn remember_torrent(&self, t: &ResolveTarget, id: &str) {
            todo!("stores/realdebrid.rs: realDebridStore.rememberTorrent")
        }
        fn pick_file_id(&self, files: &[TorrentFile], t: &ResolveTarget) -> Result<i64, StoreError> {
            todo!("stores/realdebrid.rs: realDebridStore.pickFileID")
        }
    }

    /// Go `rdInfo`: the torrent-info body Real-Debrid answers with.
    #[derive(Default)]
    pub(super) struct RdInfo {
        pub(super) links: Vec<String>,
        pub(super) files: Vec<RdFile>,
    }

    /// One entry of Go `rdInfo.Files`.
    pub(super) struct RdFile {
        pub(super) id: i64,
        pub(super) path: String,
        pub(super) bytes: i64,
        pub(super) selected: i64,
    }

    /// Go `rdLinkFor` (stores/realdebrid.rs).
    pub(super) fn rd_link_for(info: &RdInfo, file_id: i64) -> Option<String> {
        todo!("stores/realdebrid.rs: rdLinkFor")
    }

    /// Go `rdTorrentKey` (stores/realdebrid.rs).
    pub(super) fn rd_torrent_key(token: &str, info_hash: &str, t: &ResolveTarget) -> String {
        todo!("stores/realdebrid.rs: rdTorrentKey")
    }

    // Package 2 (stores/pool.rs, premiumize.rs): the pool's resolve and Premiumize's file pick.

    pub(super) trait PoolInternals {
        /// Go `StorePool.ResolvePreferring`.
        fn resolve_preferring(&self, t: &ResolveTarget, prefer: Option<&[DebridService]>) -> Result<String, StoreError>;
    }

    impl PoolInternals for StorePool {
        fn resolve_preferring(&self, t: &ResolveTarget, prefer: Option<&[DebridService]>) -> Result<String, StoreError> {
            todo!("stores/pool.rs: StorePool.ResolvePreferring")
        }
    }

    pub(super) trait PremiumizeInternals {
        /// Go `pickIndex`.
        fn pick_index(&self, files: &[TorrentFile], t: &ResolveTarget) -> Result<i64, StoreError>;
    }

    impl PremiumizeInternals for PremiumizeStore {
        fn pick_index(&self, files: &[TorrentFile], t: &ResolveTarget) -> Result<i64, StoreError> {
            todo!("stores/premiumize.rs: premiumizeStore.pickIndex")
        }
    }

    // Package 2 (stores/refusal.rs): the account-scoped keys, the add markers, the refusal memory and the TTLs.

    pub(super) fn resolve_key(token: &str, info_hash: &str) -> String {
        todo!("stores/refusal.rs: resolveKey")
    }
    pub(super) fn torrent_id_key(token: &str, info_hash: &str) -> String {
        todo!("stores/refusal.rs: torrentIDKey")
    }
    pub(super) fn torrent_miss_key(token: &str, info_hash: &str) -> String {
        todo!("stores/refusal.rs: torrentMissKey")
    }
    pub(super) fn refused_key(svc: DebridService, token: &str, info_hash: &str) -> String {
        todo!("stores/refusal.rs: refusedKey")
    }
    pub(super) fn add_attempt_key(svc: DebridService, token: &str, info_hash: &str) -> String {
        todo!("stores/refusal.rs: addAttemptKey")
    }
    pub(super) fn pm_queued_key(token: &str, info_hash: &str) -> String {
        todo!("stores/refusal.rs: pmQueuedKey")
    }
    pub(super) fn unknown_outcome_key(svc: DebridService, token: &str, info_hash: &str) -> String {
        todo!("stores/refusal.rs: unknownOutcomeKey")
    }
    pub(super) fn add_in_flight(
        cache: &dyn Cache,
        svc: DebridService,
        token: &str,
        info_hash: &str,
    ) -> Result<(), StoreError> {
        todo!("stores/refusal.rs: addInFlight")
    }
    pub(super) fn note_add_attempt(cache: &dyn Cache, svc: DebridService, token: &str, info_hash: &str) {
        todo!("stores/refusal.rs: noteAddAttempt")
    }
    pub(super) fn settle_add_attempt(cache: &dyn Cache, svc: DebridService, token: &str, info_hash: &str) {
        todo!("stores/refusal.rs: settleAddAttempt")
    }
    pub(super) fn pending_too_long(cache: &dyn Cache, token: &str, info_hash: &str) -> bool {
        todo!("stores/refusal.rs: pendingTooLong")
    }
    pub(super) fn already_queued(cache: &dyn Cache, token: &str, info_hash: &str) -> bool {
        todo!("stores/refusal.rs: alreadyQueued")
    }
    pub(super) fn unknown_outcome(
        cache: &dyn Cache,
        svc: DebridService,
        token: &str,
        info_hash: &str,
        cause: &(dyn Error + 'static),
    ) -> StoreError {
        todo!("stores/refusal.rs: unknownOutcome")
    }
    pub(super) fn record_refusal(cache: &dyn Cache, svc: DebridService, token: &str, info_hash: &str, err: &StoreError) {
        todo!("stores/refusal.rs: recordRefusal")
    }
    pub(super) fn backed_off(cache: &dyn Cache, svc: DebridService, token: &str, info_hash: &str) -> Option<String> {
        todo!("stores/refusal.rs: backedOff")
    }
    pub(super) fn account_backed_off(cache: &dyn Cache, svc: DebridService, token: &str) -> Option<String> {
        todo!("stores/refusal.rs: accountBackedOff")
    }
    pub(super) fn refusal_is_about_the_account(err: &StoreError) -> bool {
        todo!("stores/refusal.rs: refusalIsAboutTheAccount")
    }
    pub(super) fn store_refused_us(status: u16) -> bool {
        todo!("stores/refusal.rs: storeRefusedUs")
    }
    /// Go `errScoutSide`: a refusal scout made, not the store.
    pub(super) fn is_scout_side(err: &StoreError) -> bool {
        todo!("stores/refusal.rs: errScoutSide")
    }
    /// Go `errEpisodeNotInTorrent` (stores/torbox.rs).
    pub(super) fn err_episode_not_in_torrent() -> StoreError {
        todo!("stores/torbox.rs: errEpisodeNotInTorrent")
    }
    /// Go `errNoFileList` (stores/torbox.rs).
    pub(super) fn err_no_file_list() -> StoreError {
        todo!("stores/torbox.rs: errNoFileList")
    }
    pub(super) fn resolve_cache_ttl() -> Duration {
        todo!("stores.go resolveCacheTTL")
    }
    pub(super) fn torrent_miss_ttl() -> Duration {
        todo!("stores/refusal.rs: torrentMissTTL")
    }
    pub(super) fn queued_ttl() -> Duration {
        todo!("stores/refusal.rs: queuedTTL")
    }
    pub(super) fn pending_give_up() -> Duration {
        todo!("stores/refusal.rs: pendingGiveUp")
    }
    pub(super) fn add_give_up() -> Duration {
        todo!("stores/refusal.rs: addGiveUp")
    }
    pub(super) fn unknown_outcome_ttl() -> Duration {
        todo!("stores/refusal.rs: unknownOutcomeTTL")
    }

    // Package 4 (handler.rs): the /health body.

    /// The body `GET /health` answers with, for a process whose only dependency is `cache`.
    pub(super) fn health_body(cache: Arc<dyn Cache>) -> String {
        todo!("handler.rs: GET /health")
    }
}
