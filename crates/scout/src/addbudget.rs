//! Port of `internal/scout/addbudget.go`.
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
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
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
/// Distinct service/account identities with live charges. Refuse unseen accounts at capacity rather than
/// evicting a live counter: eviction would reset the very quota this budget protects. Expired or fully
/// refunded entries release capacity. At the normal allowance this bounds storage to 204,800 timestamps.
pub(crate) const MAX_TRACKED_ACCOUNTS: usize = 4096;

/// Identity of one admitted provider add. A refund must name the charge it is returning: removing
/// whichever timestamp happens to be newest lets concurrent, out-of-order answers move another
/// request's rolling-window expiry and briefly spend past the real provider ceiling.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct AddCharge {
    pub(crate) id: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct SpentCharge {
    pub(crate) charge: AddCharge,
    pub(crate) at: SystemTime,
}

#[derive(Debug)]
pub(crate) enum Refused {
    Allowance,
    Capacity,
}
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
    /// Go `spent`: the identified add times still inside the window, per account, oldest first.
    pub(crate) spent: Mutex<HashMap<String, Vec<SpentCharge>>>,
    pub(crate) window: Duration,
    pub(crate) limit: i64,
    /// Go `reserve` is how many adds of the window only a viewer waiting may spend; a prefetch is refused once
    /// the account's remaining allowance is at or below it.
    pub(crate) reserve: AtomicI64,
    /// Process-unique charge identity. Exhausting this counter would require admitting 2^64 adds in
    /// one process; fail closed instead of wrapping onto a live charge if that impossible lifetime
    /// is ever reached.
    next_charge: AtomicU64,
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
            next_charge: AtomicU64::new(1),
            now: Box::new(SystemTime::now),
        }
    }

    /// A poisoned lock still holds consistent state: every edit under it finishes before anything can panic.
    fn lock(&self) -> MutexGuard<'_, HashMap<String, Vec<SpentCharge>>> {
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
    #[cfg(test)]
    pub(crate) fn take(&self, account: &str, prefetch: bool) -> bool {
        self.try_take(account, prefetch).is_ok()
    }

    pub(crate) fn try_take(&self, account: &str, prefetch: bool) -> Result<AddCharge, Refused> {
        let mut spent = self.lock();
        let now = (self.now)();
        let cutoff = self.cutoff(now);
        drop_drained(&mut spent, cutoff);
        if !spent.contains_key(account) && spent.len() >= MAX_TRACKED_ACCOUNTS {
            return Err(Refused::Capacity);
        }
        let ceiling = self.ceiling(prefetch);
        if ceiling <= 0 {
            return Err(Refused::Allowance);
        }
        let kept = spent.entry(account.to_owned()).or_default();
        kept.retain(|charge| charge.at > cutoff);
        if kept.len() as i64 >= ceiling {
            return Err(Refused::Allowance);
        }
        let id = self
            .next_charge
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |id| id.checked_add(1))
            .expect("the process admitted 2^64 provider adds");
        let charge = AddCharge { id };
        kept.push(SpentCharge { charge, at: now });
        Ok(charge)
    }

    /// Go `refund` returns this exact charge. Only for an add that was never sent — see `refund_add`; refunding
    /// one that merely lost its response hands back an allowance the debrid has already spent. Identity matters:
    /// provider calls finish out of order, so removing the newest charge can move another call's expiry.
    pub(crate) fn refund(&self, account: &str, charge: AddCharge) {
        let mut all = self.lock();
        if let Some(spent) = all.get_mut(account) {
            if let Some(at) = spent.iter().position(|spent| spent.charge == charge) {
                spent.remove(at);
            }
            if spent.is_empty() {
                all.remove(account);
            }
        }
    }

    /// Go `addBudget.remaining` is for logging and /health — how many adds the account may still make this window.
    pub(crate) fn remaining(&self, account: &str) -> i64 {
        let spent = self.lock();
        let cutoff = self.cutoff((self.now)());
        if !spent.contains_key(account)
            && spent.values().filter(|v| v.last().is_some_and(|charge| charge.at > cutoff)).count()
                >= MAX_TRACKED_ACCOUNTS
        {
            return 0;
        }
        let live =
            spent.get(account).map_or(0, |v| v.iter().filter(|charge| charge.at > cutoff).count()) as i64;
        (self.limit - live).max(0)
    }

    /// Go `freesIn` is how long until the account may add again for this intent: zero while it has allowance left,
    /// else until the charge holding it at its ceiling drains. What a spent budget's (or a held reserve's)
    /// Retry-After says.
    pub(crate) fn frees_in(&self, account: &str, prefetch: bool) -> Duration {
        let mut spent = self.lock();
        let now = (self.now)();
        let cutoff = self.cutoff(now);
        drop_drained(&mut spent, cutoff);
        if !spent.contains_key(account) && spent.len() >= MAX_TRACKED_ACCOUNTS {
            // An unseen account needs an entire entry to drain, not just one charge from it.
            return spent
                .values()
                .filter_map(|charges| charges.last().map(|charge| charge.at))
                .min()
                .and_then(|last| last.checked_add(self.window))
                .and_then(|until| until.duration_since(now).ok())
                .unwrap_or(self.window);
        }
        let live: Vec<SystemTime> = spent
            .get(account)
            .map(|v| v.iter().map(|charge| charge.at).filter(|t| *t > cutoff).collect())
            .unwrap_or_default();
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
fn drop_drained(spent: &mut HashMap<String, Vec<SpentCharge>>, cutoff: SystemTime) {
    // The vector is append-only in time order, so the last entry is the newest.
    spent.retain(|_, v| v.last().is_some_and(|newest| newest.at > cutoff));
}

/// Go's nil-receiver `take`: a nil budget admits everything.
#[cfg(test)]
pub(crate) fn budget_take(budget: Option<&AddBudget>, account: &str, prefetch: bool) -> bool {
    budget.is_none_or(|b| b.take(account, prefetch))
}

/// Go's nil-receiver `remaining`: -1 for a nil budget, which has no count to report.
#[cfg(test)]
pub(crate) fn budget_remaining(budget: Option<&AddBudget>, account: &str) -> i64 {
    budget.map_or(-1, |b| b.remaining(account))
}

/// One per `ScoutRuntime`, keyed by service+account, so every store built for every request shares the count.
///
/// In memory, deliberately but not costlessly: a redeploy or crash-loop resets the count while the real
/// hourly ceiling keeps counting, so a restart hands back an allowance the service has not. The gap
/// between this limit and the real 60 is what absorbs that, which is one more reason not to close it.
/// A budget held on the store would be worse still — the stores are rebuilt per request. Hearth injects one
/// runtime into all of its Scout handlers.
/// Go `globalAddBudget`: one per process, keyed by service and account.
pub(crate) fn global_add_budget() -> Arc<AddBudget> {
    crate::runtime::current_runtime().add_budget()
}

/// Replaces this test thread's budget and returns the one it replaced, so a test can put it back.
#[cfg(test)]
pub(crate) fn set_global_add_budget(budget: Arc<AddBudget>) -> Arc<AddBudget> {
    crate::runtime::current_runtime().replace_add_budget(budget)
}

/// Go `SetPlayReserve` sets how many adds of the hour a prefetch may not spend. Called once at startup, from
/// PLAY_RESERVE_ADDS.
pub fn set_play_reserve(n: i64) {
    global_add_budget().reserve.store(n, Ordering::SeqCst);
}

/// Stable wire/log wording for the typed `StoreError::PlayReserve` refusal.
pub(crate) const PLAY_RESERVE_REASON: &str = "the rest of scout's hourly add budget is kept for Play";

/// The reason a spent budget refuses a viewer with.
pub(crate) const BUDGET_SPENT_REASON: &str = "scout's own hourly add budget for this account is spent";

/// Go `errors.Is(err, errPlayReserve)`: a prefetch refused because what is left of the hour is kept for a viewer
/// waiting. Not a spent budget and not the service refusing: /play answers it as "reserved_for_play", and the
/// client retries after Retry-After without holding it against the release.
pub(crate) fn is_play_reserve(err: &StoreError) -> bool {
    matches!(err, StoreError::PlayReserve(_))
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
) -> Result<AddCharge, StoreError> {
    let refused = match global_add_budget().try_take(&budget_account(svc, token), prefetch) {
        Ok(charge) => return Ok(charge),
        Err(refused) => refused,
    };
    let svc_name = svc.as_str();
    if matches!(refused, Refused::Capacity) {
        let counters = metrics();
        let refused = if prefetch { &counters.add_refused_prefetch } else { &counters.add_refused_play };
        refused.fetch_add(1, Ordering::Relaxed);
        log_limited("add-budget-capacity", "scout's add budget is tracking the maximum number of accounts");
        return Err(StoreError::ScoutSide(StoreUnavailableError {
            service: svc,
            reason: "scout's add budget cannot track another account until an existing window drains"
                .to_owned(),
            status: 0,
        }));
    }
    if prefetch {
        metrics().add_refused_prefetch.fetch_add(1, Ordering::Relaxed);
        log_limited(
            &format!("add-reserve:{svc_name}"),
            &format!(
                "{svc_name} add allowance is down to the reserve kept for Play, refusing prefetch {}",
                short_hash(info_hash)
            ),
        );
        return Err(StoreError::PlayReserve(StoreUnavailableError {
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
pub(crate) fn refund_add(
    svc: DebridService,
    token: &str,
    charge: AddCharge,
    err: &(dyn std::error::Error + 'static),
) {
    // Go `errors.Is`: the marker anywhere along the chain of causes.
    let mut cause: Option<&(dyn std::error::Error + 'static)> = Some(err);
    while let Some(e) = cause {
        if e.is::<RequestNotSent>() {
            global_add_budget().refund(&budget_account(svc, token), charge);
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
pub(crate) fn refund_unused_add(svc: DebridService, token: &str, charge: AddCharge) {
    global_add_budget().refund(&budget_account(svc, token), charge);
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
