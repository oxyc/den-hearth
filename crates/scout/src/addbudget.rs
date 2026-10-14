//! Port of `internal/scout/addbudget.go`. Work package 3.
//!
//! A local ceiling on debrid ADDS, per account.
//!
//! TorBox allows 60 torrent-adds an hour, and exhausting it has broken playback for a whole evening. Every
//! guard in this package until now was a guard against a KNOWN way of spending them — don't probe an
//! uncached release, don't re-add while backed off, don't resolve through a store that does not hold it.
//! Each one closed a real hole, and each one was found only after it had already cost an evening.
//!
//! This is the other kind of control: it does not care WHY an add is being made. It counts them, and when
//! the hour's allowance is gone it refuses, so an unforeseen path — a new caller, a client looping on an
//! error, a bug not yet written — costs a refusal instead of the evening. The backoff in Resolve is the
//! post-hoc version of this: it reacts after TorBox has already said no, which is one incident too late.
//!
//! Sized under the real ceiling, not at it. The gap absorbs adds this process cannot see: another client
//! on the same account, a retry TorBox counted and we did not, an add whose response never arrived.

use std::collections::HashMap;
use std::sync::atomic::{AtomicI64, Ordering};
#[cfg(not(test))]
use std::sync::LazyLock;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::{Duration, SystemTime};

use crate::config::DebridService;
use crate::loglimit::log_limited;
use crate::metrics::metrics;
use crate::stores::refusal::service_account_identity;
use crate::stores::{short_hash, StoreError, StoreUnavailableError};
use crate::util::key_hash;

pub(crate) const ADD_BUDGET_WINDOW: Duration = Duration::from_secs(60 * 60);
pub(crate) const ADD_BUDGET_LIMIT: i64 = 50;
/// Go `defaultPlayReserve` is how many of the window's adds a prefetch may not spend (PLAY_RESERVE_ADDS). The
/// budget used not to care WHY an add was made, so a season download running in the background could
/// spend the whole hour and the viewer who then pressed Play was refused. A prefetch — the binge
/// read-ahead, a queued download — stops this many short of the ceiling; a viewer waiting spends down to
/// zero. Small and fixed rather than a share of the allowance: a season pass should still fetch the whole
/// season, it just must not take the last few adds a Play needs.
pub(crate) const DEFAULT_PLAY_RESERVE: i64 = 5;

/// Go `addBudget` is a rolling-window counter, per account. Rolling rather than a fixed hourly bucket because
/// the ceiling it mirrors is rolling: spending the whole allowance at 10:59 and again at 11:01 is exactly
/// the burst that trips the real limit, and a fixed bucket permits it.
pub(crate) struct AddBudget {
    /// Go `spent`: the add times still inside the window, per account, oldest first.
    pub(crate) spent: Mutex<HashMap<String, Vec<SystemTime>>>,
    pub(crate) window: Duration,
    pub(crate) limit: i64,
    /// Go `reserve` is how many adds of the window only a viewer waiting may spend; a prefetch is refused once
    /// the account's remaining allowance is at or below it.
    pub(crate) reserve: AtomicI64,
    /// Go `now`, injectable so the tests do not sleep for an hour.
    pub(crate) now: Box<dyn Fn() -> SystemTime + Send + Sync>,
}

impl AddBudget {
    /// Go `newAddBudget`.
    pub(crate) fn new(window: Duration, limit: i64) -> AddBudget {
        AddBudget {
            spent: Mutex::new(HashMap::new()),
            window,
            limit,
            reserve: AtomicI64::new(DEFAULT_PLAY_RESERVE),
            now: Box::new(SystemTime::now),
        }
    }

    /// A poisoned lock still holds consistent state: every edit under it finishes before anything can panic.
    fn lock(&self) -> MutexGuard<'_, HashMap<String, Vec<SystemTime>>> {
        self.spent.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// The start of the window as seen from `now`. A clock too close to the epoch to subtract from has nothing
    /// older than the window, so everything counts.
    fn cutoff(&self, now: SystemTime) -> SystemTime {
        now.checked_sub(self.window).unwrap_or(SystemTime::UNIX_EPOCH)
    }

    /// Go `playReserve` is the reserve in force, for /metrics.
    pub(crate) fn play_reserve(&self) -> i64 {
        self.reserve.load(Ordering::SeqCst)
    }

    /// Go `ceiling` is how many adds the window may hold for this intent: the whole limit for a viewer waiting, the
    /// limit less the reserve for a prefetch.
    fn ceiling(&self, prefetch: bool) -> i64 {
        if prefetch {
            self.limit - self.play_reserve()
        } else {
            self.limit
        }
    }

    /// Go `addBudget.take`: records an add against the account and reports whether it is allowed. A refusal spends
    /// nothing, so the window drains normally and the next caller after it expires is served. A prefetch is refused
    /// while the reserve is all that is left; a viewer waiting spends down to zero.
    pub(crate) fn take(&self, account: &str, prefetch: bool) -> bool {
        let mut spent = self.lock();
        let now = (self.now)();
        let cutoff = self.cutoff(now);
        drop_drained(&mut spent, cutoff);
        let kept = spent.entry(account.to_owned()).or_default();
        kept.retain(|t| *t > cutoff);
        if kept.len() as i64 >= self.ceiling(prefetch) {
            return false;
        }
        kept.push(now);
        true
    }

    /// Go `refund` returns the most recent charge. Only for an add that was never sent — see `refund_add`; refunding
    /// one that merely lost its response hands back an allowance the debrid has already spent.
    pub(crate) fn refund(&self, account: &str) {
        if let Some(spent) = self.lock().get_mut(account) {
            spent.pop();
        }
    }

    /// Go `addBudget.remaining` is for logging and /health — how many adds the account may still make this window.
    pub(crate) fn remaining(&self, account: &str) -> i64 {
        let spent = self.lock();
        let cutoff = self.cutoff((self.now)());
        let live = spent.get(account).map_or(0, |v| v.iter().filter(|t| **t > cutoff).count()) as i64;
        (self.limit - live).max(0)
    }

    /// Go `freesIn` is how long until the account may add again for this intent: zero while it has allowance left,
    /// else until the charge holding it at its ceiling drains. What a spent budget's (or a held reserve's)
    /// Retry-After says.
    pub(crate) fn frees_in(&self, account: &str, prefetch: bool) -> Duration {
        let spent = self.lock();
        let now = (self.now)();
        let cutoff = self.cutoff(now);
        let live: Vec<SystemTime> =
            spent.get(account).map(|v| v.iter().copied().filter(|t| *t > cutoff).collect()).unwrap_or_default();
        let ceiling = self.ceiling(prefetch);
        if ceiling <= 0 {
            return self.window; // no allowance ever frees; a window is the honest "not soon"
        }
        if (live.len() as i64) < ceiling {
            return Duration::ZERO;
        }
        // Append-only in time order: the charge that must drain for one to free is the one `ceiling` from the newest.
        (live[live.len() - ceiling as usize] + self.window).duration_since(now).unwrap_or(Duration::ZERO)
    }

    /// Go `lowest` reports the smallest remaining allowance across all accounts, and how many accounts have spent
    /// anything. For /health, which is the point: an operator needs to know the ceiling is being approached,
    /// and a monitor needs one number to alert on.
    ///
    /// Deliberately NOT keyed by account. /health is unauthenticated — every other route is protected by an
    /// unguessable config segment — and `service:hash(token)` is stable, so publishing it turns the endpoint
    /// into a confirmation oracle for a guessed token and discloses which services this install uses. The
    /// aggregate answers the operational question without answering that one.
    pub(crate) fn lowest(&self) -> (i64, i64) {
        let keys: Vec<String> = self.lock().keys().cloned().collect();
        let mut left = self.limit;
        let mut accounts = 0;
        for acct in keys {
            let r = self.remaining(&acct);
            if r < self.limit {
                accounts += 1;
                if r < left {
                    left = r;
                }
            }
        }
        (left, accounts)
    }
}

/// Go `dropDrainedLocked` forgets accounts whose window has emptied. Entries were only ever rewritten, never
/// deleted, so the map grew one key per distinct service+token ever seen and never shrank — and the token
/// comes from the install's own config URL on an unauthenticated route, so the set of keys is not bounded
/// by anything this process controls. `pruneMintedLocked` is the same idea two files over.
fn drop_drained(spent: &mut HashMap<String, Vec<SystemTime>>, cutoff: SystemTime) {
    // The vector is append-only in time order, so the last entry is the newest.
    spent.retain(|_, v| v.last().is_some_and(|newest| *newest > cutoff));
}

/// Go's nil-receiver `take`: a nil budget admits everything.
pub(crate) fn budget_take(budget: Option<&AddBudget>, account: &str, prefetch: bool) -> bool {
    budget.is_none_or(|b| b.take(account, prefetch))
}

/// Go's nil-receiver `remaining`: -1 for a nil budget, which has no count to report.
pub(crate) fn budget_remaining(budget: Option<&AddBudget>, account: &str) -> i64 {
    budget.map_or(-1, |b| b.remaining(account))
}

/// One per process, keyed by service+account, so every store built for every request shares the count.
///
/// In memory, deliberately but not costlessly: a redeploy or crash-loop resets the count while the real
/// hourly ceiling keeps counting, so a restart hands back an allowance the service has not. The gap
/// between this limit and the real 60 is what absorbs that, which is one more reason not to close it.
/// A budget held on the store would be worse still — the stores are rebuilt per request.
#[cfg(not(test))]
static GLOBAL_ADD_BUDGET: LazyLock<Mutex<Arc<AddBudget>>> =
    LazyLock::new(|| Mutex::new(Arc::new(AddBudget::new(ADD_BUDGET_WINDOW, ADD_BUDGET_LIMIT))));

// A TEST build keeps the budget per thread instead. Go's tests run one at a time and swap `globalAddBudget` around
// the ones that care, while cargo runs ours in parallel: a shared budget lets one test's limit of zero refuse
// another's add, and every test that uses the token "tok" spends from the same fifty. Each test runs on its own
// thread (and a `#[tokio::test]` keeps all its tasks there), so a thread's budget is that test's, fresh and
// unshared, and the assertions about it need no lock.
#[cfg(test)]
thread_local! {
    static GLOBAL_ADD_BUDGET: std::cell::RefCell<Arc<AddBudget>> =
        std::cell::RefCell::new(Arc::new(AddBudget::new(ADD_BUDGET_WINDOW, ADD_BUDGET_LIMIT)));
}

/// Go `globalAddBudget`: one per process, keyed by service and account.
pub(crate) fn global_add_budget() -> Arc<AddBudget> {
    #[cfg(not(test))]
    {
        Arc::clone(&GLOBAL_ADD_BUDGET.lock().unwrap_or_else(PoisonError::into_inner))
    }
    #[cfg(test)]
    {
        GLOBAL_ADD_BUDGET.with(|b| Arc::clone(&b.borrow()))
    }
}

/// Replaces the process-wide budget and returns the one it replaced. Go assigns `globalAddBudget` directly;
/// the Rust form returns the previous value so a test can put it back.
pub(crate) fn set_global_add_budget(budget: Arc<AddBudget>) -> Arc<AddBudget> {
    #[cfg(not(test))]
    {
        std::mem::replace(&mut *GLOBAL_ADD_BUDGET.lock().unwrap_or_else(PoisonError::into_inner), budget)
    }
    #[cfg(test)]
    {
        GLOBAL_ADD_BUDGET.with(|b| std::mem::replace(&mut *b.borrow_mut(), budget))
    }
}

/// Go `SetPlayReserve` sets how many adds of the hour a prefetch may not spend. Called once at startup, from
/// PLAY_RESERVE_ADDS.
pub fn set_play_reserve(n: i64) {
    global_add_budget().reserve.store(n, Ordering::SeqCst);
}

/// The reason a prefetch refused for the reserve carries. Go marks it with the `errPlayReserve` sentinel wrapped
/// beside `errScoutSide`; the frozen `StoreError` has no variant for it, so `is_play_reserve` reads it back from
/// this text.
pub(crate) const PLAY_RESERVE_REASON: &str = "the rest of scout's hourly add budget is kept for Play";

/// The reason a spent budget refuses a viewer with.
pub(crate) const BUDGET_SPENT_REASON: &str = "scout's own hourly add budget for this account is spent";

/// Go `errors.Is(err, errPlayReserve)`: a prefetch refused because what is left of the hour is kept for a viewer
/// waiting. Not a spent budget and not the service refusing: /play answers it as "reserved_for_play", and the
/// client retries after Retry-After without holding it against the release.
pub(crate) fn is_play_reserve(err: &StoreError) -> bool {
    matches!(err, StoreError::ScoutSide(u) if u.reason == PLAY_RESERVE_REASON)
}

/// Go `spendAdd` charges one add against a service's account, or refuses. Every store calls this immediately
/// before the request that queues a torrent, so there is one place the allowance is enforced rather than
/// one per store — and no way to add without passing through it.
///
/// Per service AND account: TorBox's ceiling is TorBox's. Counting them together would let a busy
/// Real-Debrid close TorBox's budget, and a service with no published limit would still be worth bounding
/// — an unbounded add loop is a bug wherever it points.
///
/// `prefetch` is the caller's intent (`ResolveTarget::prefetch`): a prefetch is refused while only the reserve
/// is left, with the play-reserve marker so the route can say so apart from a spent budget.
pub(crate) fn spend_add(
    svc: DebridService,
    token: &str,
    info_hash: &str,
    prefetch: bool,
) -> Result<(), StoreError> {
    if global_add_budget().take(&budget_account(svc, token), prefetch) {
        return Ok(());
    }
    let svc_name = svc.as_str();
    if prefetch {
        metrics().add_refused_prefetch.fetch_add(1, Ordering::Relaxed);
        log_limited(
            &format!("add-reserve:{svc_name}"),
            &format!(
                "{svc_name} add allowance is down to the reserve kept for Play, refusing prefetch {}",
                short_hash(info_hash)
            ),
        );
        return Err(StoreError::ScoutSide(StoreUnavailableError {
            service: svc,
            reason: PLAY_RESERVE_REASON.to_owned(),
            status: 0,
        }));
    }
    metrics().add_refused_play.fetch_add(1, Ordering::Relaxed);
    log_limited(
        &format!("add-budget:{svc_name}"),
        &format!("{svc_name} add budget spent for the hour, refusing {}", short_hash(info_hash)),
    );
    // Wrapped so the refusal memory can tell scout's own ceiling from the service's — see errOurBudget.
    Err(StoreError::ScoutSide(StoreUnavailableError {
        service: svc,
        reason: BUDGET_SPENT_REASON.to_owned(),
        status: 0,
    }))
}

/// Go `refundAdd` gives the charge back when the request was never sent at all.
///
/// It used to refund on cancellation, which was wrong and measurably so: a cancelled add has already been
/// written to the wire, and the debrid counts it. Sixty cancelled polls of one release put sixty
/// createtorrent calls on the account while this budget still reported a full allowance. "The response
/// never arrived" and "the request never happened" are not the same fact, and only the second is a
/// refund — the recurring mistake in this codebase is treating one as the other.
///
/// The problem the refund was reaching for is real (a cancelling client must not be able to lock itself
/// out), but the answer is to stop re-adding, not to stop counting. See addAttemptKey.
pub(crate) fn refund_add(svc: DebridService, token: &str, err: &(dyn std::error::Error + 'static)) {
    // Go `errors.Is`: the marker anywhere along the chain of causes.
    let mut cause: Option<&(dyn std::error::Error + 'static)> = Some(err);
    while let Some(e) = cause {
        if e.is::<RequestNotSent>() {
            global_add_budget().refund(&budget_account(svc, token));
            return;
        }
        cause = e.source();
    }
}

/// Go `refundUnusedAdd` gives a charge back when the request turned out not to be an add at all.
///
/// Premiumize's directdl is the only caller: it is a purchase for a release the account lacks and a plain
/// read for one it holds, and which of those happened is only knowable from the answer. Charging first
/// keeps the budget able to GATE the call; refunding here keeps a library of held releases from costing
/// an add per play.
pub(crate) fn refund_unused_add(svc: DebridService, token: &str) {
    global_add_budget().refund(&budget_account(svc, token));
}

/// Go `errRequestNotSent` marks the one case where nothing reached the service: the request could not even be
/// constructed. Everything past that point may have been received.
#[derive(Debug)]
pub(crate) struct RequestNotSent;

impl std::fmt::Display for RequestNotSent {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("request was not sent")
    }
}

impl std::error::Error for RequestNotSent {}

/// Go `budgetAccount`: the key one account's allowance is kept under.
pub(crate) fn budget_account(svc: DebridService, token: &str) -> String {
    format!("{}:{}", svc.as_str(), key_hash(&service_account_identity(svc, token)))
}
