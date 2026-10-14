//! Port of the pool half of `internal/scout/stores.go` (lines 4722-end): `StorePool`, `buildStores`,
//! `CacheCheck`/`Resolve`/`ResolvePreferring`/`ResolveCachedOnly`/`Status` across stores. Owned by the
//! "Stores II" work package. The reporters (`recent_refusal`, `account_refusal`, `holding_services` and
//! friends) are in `refusal.rs`, beside the markers they read.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use tokio::time::Instant;

use crate::addbudget::global_add_budget;
use crate::cache::Cache;
use crate::config::{Config, DebridService, DEBRID_SERVICES};
use crate::loglimit::log_limited;
use crate::scrape::Doer;
use crate::stores::premiumize::{PremiumizeStore, PREMIUMIZE_API};
use crate::stores::realdebrid::{RealDebridStore, REAL_DEBRID_API};
use crate::stores::refusal::{describe, wraps_unavailable};
use crate::stores::torbox::{TorBoxStore, TORBOX_API};
use crate::stores::{
    dead_link, join_all, short_hash, CacheTruth, ResolveTarget, StatusAnswer, Store, StoreError, StoreStatus,
    StoreUnavailableError,
};

/// Go `StorePool`: one store per account in service-priority order (TorBox first).
pub struct StorePool {
    pub(crate) stores: Vec<Box<dyn Store>>,
}

/// Go `buildStores`: one store per configured account, in service-priority order (TorBox first).
pub fn build_stores(config: &Config, client: Arc<dyn Doer>, cache: Option<Arc<dyn Cache>>) -> Vec<Box<dyn Store>> {
    let mut by_service: HashMap<DebridService, &str> = HashMap::new();
    for d in &config.debrid {
        by_service.insert(d.service, d.token.as_str());
    }
    let mut stores: Vec<Box<dyn Store>> = Vec::new();
    for svc in DEBRID_SERVICES {
        let Some(token) = by_service.get(&svc) else { continue };
        let token = (*token).to_owned();
        match svc {
            DebridService::TorBox => stores.push(Box::new(TorBoxStore {
                token,
                client: client.clone(),
                cache: cache.clone(),
                api: TORBOX_API.to_owned(),
            })),
            DebridService::RealDebrid => stores.push(Box::new(RealDebridStore {
                token,
                client: client.clone(),
                cache: cache.clone(),
                api: REAL_DEBRID_API.to_owned(),
            })),
            DebridService::Premiumize => stores.push(Box::new(PremiumizeStore {
                token,
                client: client.clone(),
                cache: cache.clone(),
                api: PREMIUMIZE_API.to_owned(),
            })),
        }
    }
    stores
}

/// Go `isCacheTruthService`.
pub fn is_cache_truth_service(svc: DebridService) -> bool {
    svc == DebridService::TorBox || svc == DebridService::Premiumize
}

/// Go `hasCacheTruth` reports whether any configured store has a real cache API (TorBox/Premiumize). When
/// false (RD-only), the handler skips the cached-only filter so the list isn't always empty (audit #4).
pub fn has_cache_truth(config: &Config) -> bool {
    config.debrid.iter().any(|d| is_cache_truth_service(d.service))
}

/// Go `configHasService`.
pub fn config_has_service(config: &Config, service: DebridService) -> bool {
    config.debrid.iter().any(|account| account.service == service)
}

impl StorePool {
    /// Go `StorePool.CacheCheck` (stores.go:4783): asks every store that has a cache API, and reports
    /// whether any of them answered at all.
    ///
    /// `truthOK` is whether at least one cache-truth store (TorBox/Premiumize) answered at all; when false
    /// the handler skips the cached-only filter (rather than dropping everything) and declines to cache the
    /// degraded list.
    pub async fn cache_check(&self, hashes: &[String]) -> (CacheTruth, bool) {
        let mut truth = CacheTruth {
            holders: HashMap::with_capacity(hashes.len()),
            known: HashMap::with_capacity(hashes.len()),
            complete: false,
        };
        // Nothing to ask about is a COMPLETE answer, not an outage. Leaving `complete` false here meant a
        // title with no releases at all was reported degraded and re-scraped in full on every request,
        // forever — an outage flag raised over a question nobody asked.
        if hashes.is_empty() {
            truth.complete = true;
            return (truth, true);
        }
        // Independent per store; run concurrently. A store error can't 500 the request (audit #5) — it only
        // withholds that store's truth.
        let maps: Vec<Option<HashMap<String, bool>>> =
            // An error means the store learned NOTHING — a partial failure returns no error and simply omits
            // the hashes it could not check, so absence carries that case on its own.
            join_all(self.stores.iter().map(|st| async move { st.cache_check(hashes).await.ok() }).collect()).await;
        // Store order, so a hash several services hold lists them in configured priority.
        let mut truth_ok = false;
        let mut answers: HashMap<&str, usize> = HashMap::with_capacity(hashes.len());
        let (mut cache_truth_stores, mut answered_stores) = (0, 0);
        let empty = HashMap::new();
        for (st, m) in self.stores.iter().zip(&maps) {
            let m = m.as_ref().unwrap_or(&empty);
            if !is_cache_truth_service(st.service()) {
                for (hash, cached) in m {
                    if *cached {
                        truth.holders.entry(hash.clone()).or_default().push(st.service());
                    }
                }
                continue;
            }
            cache_truth_stores += 1;
            // A store that answered for NOTHING is out. It STAYS in the denominator below — its silence is
            // exactly why a "no" from the others cannot rule anything out — and the outage is reported
            // separately through Complete, so the handler stops filtering rather than quietly serving an
            // empty list. Dropping it from the denominator instead made the survivor's "no" authoritative: a
            // cachedOnly request came back `{"streams":[]}` with no degraded header, cached for five minutes
            // and held for a day on stale-if-error. A visible no-op is a far better failure than that.
            if m.is_empty() {
                continue;
            }
            answered_stores += 1;
            truth_ok = true;
            for (hash, cached) in m {
                // Presence in the map is the store's claim to have answered for that hash.
                *answers.entry(hash.as_str()).or_default() += 1;
                if *cached {
                    truth.holders.entry(hash.clone()).or_default().push(st.service());
                }
            }
        }
        // A "yes" is knowledge on its own: one store confirming it HOLDS a release settles the question, and
        // no other store's silence can unsettle it. A "no" needs everyone who could answer to have answered,
        // because a store that never voted may be the one holding it.
        //
        // The union got the second half wrong — a confident "nobody has this" from the only party that never
        // voted. Requiring unanimity for both got the FIRST half wrong, which was worse: it discarded the
        // only certain facts in the set, so a cached-only list stopped filtering exactly when the surviving
        // store's answers mattered most.
        for (hash, n) in answers {
            if truth.holders.get(hash).is_some_and(|h| !h.is_empty()) || n == cache_truth_stores {
                truth.known.insert(hash.to_owned(), true);
            }
        }
        truth.complete = answered_stores == cache_truth_stores;
        (truth, truth_ok)
    }

    /// Go `StorePool.KnownFileSize` (linkcheck.go:118) is the first size any store knows for this file. Stores
    /// that hold the same torrent describe the same file, so which one answers does not matter.
    pub fn known_file_size(&self, t: &ResolveTarget) -> Option<i64> {
        self.stores.iter().find_map(|st| st.known_file_size(t))
    }

    /// Go `StorePool.ScoutBusyFor` is how long until any of the pool's accounts may add again for this intent, for
    /// the Retry-After of a scout_busy or reserved_for_play answer; zero when one already may, or none reports a
    /// budget.
    pub fn scout_busy_for(&self, prefetch: bool) -> Duration {
        let mut soonest = Duration::ZERO;
        for account in self.stores.iter().filter_map(|st| st.budget_account()) {
            let wait = global_add_budget().frees_in(&account, prefetch);
            if wait.is_zero() {
                return Duration::ZERO;
            }
            if soonest.is_zero() || wait < soonest {
                soonest = wait;
            }
        }
        soonest
    }

    /// Go `StorePool.Resolve` (stores.go:4865), in configured order.
    pub async fn resolve(&self, t: &ResolveTarget) -> Result<String, StoreError> {
        self.resolve_preferring(t, &[]).await
    }

    /// Go `StorePool.ResolvePreferring` (stores.go:4879) resolves, trying the services in `preferred` before
    /// the rest.
    ///
    /// The point is not speed but WHAT ARRIVES: a release one service already holds plays now, while the same
    /// release on another has to be downloaded first — minutes to hours. CacheCheck already learns which
    /// services hold it and the union threw that away, so the fixed store order could send a viewer to
    /// download a file that was sitting cached on their other account.
    /// Every store's failure is logged. The generic "no store could resolve" that replaced them said only
    /// that something went wrong, which is indistinguishable from a dead release — and an add that never
    /// reached the debrid (an expired token, a rejected magnet, an exhausted deadline) looks from the
    /// outside exactly like a release nobody is seeding: the client waits on a download the service was
    /// never asked to start.
    pub async fn resolve_preferring(
        &self,
        t: &ResolveTarget,
        preferred: &[DebridService],
    ) -> Result<String, StoreError> {
        let (mut refused, mut coming): (Option<StoreError>, Option<StoreError>) = (None, None);
        // The caller's context ran out: a store reported the cancellation. Go reads `ctx.Err()` at the end;
        // here the context is gone from the signature, and a store saying it was cancelled is the same fact.
        let mut cancelled = false;
        // Holders are tried FIRST, which is the whole point: a service that already holds the release serves
        // it now, where another has to fetch it. Every store still gets a turn if the holders fail.
        //
        // An earlier version stopped after one non-holder, to keep a single play from queueing the same
        // torrent on every account. That bound could not be made safe: it bites only when a store FAILS,
        // which is exactly when the next store is the viewer's last chance, so it turned "the first two
        // accounts couldn't serve this" into "you cannot play this", a release that used to play. Adds are
        // bounded by `spendAdd`, which is a ceiling rather than a coin flip on which store gets to try.
        for st in self.ordered(preferred) {
            // A spent clock must not stop this loop. Skipping the remaining stores was tried and reverted:
            // the answers that matter most on a dead clock are FREE. addInFlight and both backoff gates are
            // pure cache reads sitting ahead of the charge and ahead of the wire, so asking a store costs
            // nothing and can still return errAddInFlight — which outranks every refusal below and is what
            // makes /play answer 202. Skipping the store threw that away and answered 503 instead, naming
            // the viewer's debrid as refusing for a release scout was actively fetching, which is the one
            // answer this file twice says never to give. Measured: 202 before the skip, 503 after, with the
            // second store never asked.
            //
            // What must not happen on a dead clock is the CHARGE, and that is guarded where it happens.
            let err = match st.resolve(t).await {
                Ok(link) => return Ok(link),
                Err(err) => err,
            };
            log_limited(
                &format!("resolve-failed:{}", st.service().as_str()),
                &format!("{} could not resolve {}: {}", st.service().as_str(), short_hash(&t.info_hash), describe(&err)),
            );
            cancelled |= matches!(err, StoreError::Cancelled);
            // An add of ours already out for this release outranks any refusal, whichever store said what and
            // in whichever order. It is the one answer that is about US rather than about a service: the
            // release is being fetched right now, so 202 "downloading" is simply true.
            //
            // It was being lost because errAddInFlight wraps a StoreUnavailableError, so the first refusal
            // claimed `refused` and nothing downstream could tell them apart. Store order then decided the
            // verdict: TorBox throttled with RD fetching answered 503 naming TorBox, while the same two facts
            // in the other order answered 202. The 503 tells the viewer their debrid is refusing and stops the
            // client trying other sources — for a release scout has an add out for.
            // errAddQueued ranks with errAddInFlight for the same reason: TorBox accepted this add too, it is
            // merely parked behind the account's slot limit rather than unanswered — "coming", not refused and
            // not dead. Without this a parked add fell through to the generic dead_link below the moment any
            // OTHER configured store also failed, which is exactly the regression den#204 decoding queued_id
            // was meant to remove.
            if coming.is_none() && matches!(err, StoreError::AddInFlight | StoreError::AddQueued) {
                coming = Some(err.clone());
            }
            // A service refusing US outranks a dead link as an explanation: if even one store was throttled
            // or faulting, "this release is dead" is not a conclusion the evidence supports.
            if wraps_unavailable(&err) && refused.is_none() {
                refused = Some(err);
            }
        }
        if let Some(coming) = coming {
            return Err(coming);
        }
        if let Some(refused) = refused {
            return Err(refused);
        }
        // A resolve that ran out of clock did not learn the release is dead — it learned nothing at all, and
        // possibly from no store. DeadLinkError here is the same conflation the rest of this file spent
        // several rounds removing: it is a 404, and a 404 makes the client blacklist a release nobody was
        // able to ask about.
        // Named as SCOUT's, not a service's, because that is whose clock ran out. errScoutSide is the
        // existing way to say "this refusal is ours"; spendAdd wraps the same way. A pool has no service of
        // its own to name, so the first store's stands in; the handler renders a scout-side refusal by its
        // reason, never by the service.
        if cancelled {
            let service = self.stores.first().map_or(DebridService::TorBox, |s| s.service());
            return Err(StoreError::ScoutSide(StoreUnavailableError {
                service,
                reason: "the resolve budget was spent before a store could answer".to_owned(),
                status: 0,
            }));
        }
        Err(dead_link("no store could resolve"))
    }

    /// Go `StorePool.ordered` puts the preferred services first, keeping the configured order within each
    /// group so the result stays deterministic — the same request must not resolve differently run to run.
    pub(crate) fn ordered(&self, preferred: &[DebridService]) -> Vec<&dyn Store> {
        if preferred.is_empty() {
            return self.stores.iter().map(|s| s.as_ref()).collect();
        }
        // The CALLER'S order within `preferred`, not the configured one. Those are different questions, and
        // collapsing them to a set threw away the answer to the first.
        //
        // /play ranks its holders deliberately: stores that already have an id for the release come before
        // stores a cache check merely reported as holding it, because the first can serve with no add and the
        // second still costs a createtorrent — TorBox's checkcached says what TORBOX has, not what this
        // ACCOUNT has. Re-sorting by configured priority handed the release back to whichever store happened
        // to be listed first. Measured on TorBox + Real-Debrid with RD holding the torrent and TorBox's
        // checkcached reporting it cached: /play resolved from TorBox, spent one of the fifty and left a
        // duplicate torrent there, while ?probe=1 answered 200 ready from RD for free — the two routes
        // disagreeing about which store serves, and only the paying one getting it wrong.
        //
        // Within one preferred service the configured order still decides, and every store still gets a turn:
        // this reorders, it never drops.
        let mut order: Vec<DebridService> = Vec::with_capacity(preferred.len());
        for svc in preferred {
            if !order.contains(svc) {
                order.push(*svc);
            }
        }
        let mut out: Vec<&dyn Store> = Vec::with_capacity(self.stores.len());
        for svc in &order {
            out.extend(self.stores.iter().filter(|st| st.service() == *svc).map(|s| s.as_ref()));
        }
        out.extend(self.stores.iter().filter(|st| !order.contains(&st.service())).map(|s| s.as_ref()));
        out
    }

    /// Go `StorePool.ResolveCachedOnly` (stores.go:5013) resolves against ONLY the services that are known to
    /// hold the release, and fails rather than falling through to one that would have to fetch it.
    ///
    /// This is the read-only sibling of Resolve, for callers that must never cause an add. Resolve tries
    /// every configured store in priority order, so with two accounts a release cached on the SECOND one
    /// still reaches the first — which adds it. That is invisible on a single-account install and burns the
    /// whole hourly quota on a two-account one, from a background path nobody asked for.
    pub async fn resolve_cached_only(
        &self,
        t: &ResolveTarget,
        holders: &[DebridService],
    ) -> Result<String, StoreError> {
        if holders.is_empty() {
            return Err(dead_link("no store holds this release"));
        }
        let mut refused: Option<StoreError> = None;
        for st in &self.stores {
            if !holders.contains(&st.service()) {
                continue;
            }
            match st.resolve(t).await {
                Ok(link) => return Ok(link),
                Err(err) => {
                    if wraps_unavailable(&err) && refused.is_none() {
                        refused = Some(err);
                    }
                }
            }
        }
        if let Some(refused) = refused {
            return Err(refused);
        }
        Err(dead_link("no holding store could resolve"))
    }

    /// Go `StorePool.Status` — the first store that can say a queued release is still downloading. Only
    /// meaningful right after a failed Resolve; `ok == false` means nobody is fetching it, i.e. genuinely dead.
    ///
    /// The third result is the one that matters to a caller deciding whether to ADD: it reports that at least
    /// one store could not answer, as opposed to answering "no". Callers that will RETRY want `status_detail`
    /// instead, which says which stores those were; this is the same question collapsed to a bool.
    ///
    /// Returning only (status, ok) forced handlePlay to infer doubt from its own clock, and the per-store
    /// budget slicing below made the two questions different: store 0 can burn its slice and time out while
    /// the pool still returns well before the caller's deadline, so "did the whole budget elapse" was false
    /// exactly when a store had failed to answer. The caller then queued a second copy of a torrent store 0
    /// was already fetching — the bug the escalation exists to prevent, reintroduced for every multi-account
    /// install by the fix for a different one. The pool knows; it says so rather than leaving it to be
    /// guessed.
    ///
    /// `deadline` is the caller's, which Go carries in the context: the budget the stores share.
    pub async fn status(&self, t: &ResolveTarget, deadline: Option<Instant>) -> (StoreStatus, bool, bool) {
        let (status, ok, unknown) = self.status_detail(t, deadline).await;
        (status, ok, !unknown.is_empty())
    }

    /// Go `StorePool.StatusDetail` is `status`, plus WHICH stores could not answer — so a retry asks only those.
    ///
    /// The escalated read went back through the whole pool, and a store that answered definitively has
    /// nothing new to say a moment later: measured through the handler on a three-account pool, one unknown
    /// store cost NINE store reads per /play where SEVEN suffice, and each TorBox read is up to two upstream
    /// calls. Seven rather than six because the post-failure read below still asks everyone — deliberately,
    /// since it runs after an add may have landed and any store's answer can have changed. On a two-second
    /// poll cadence for the length of a download, the difference is a wasted upstream read per extra account
    /// per poll, on the path the escalation exists for — which is by definition one where something is
    /// already struggling.
    ///
    /// Each store gets its own SLICE of the budget, rather than all of them sharing one deadline. Sharing it
    /// meant the first store could spend the lot: TorBox's status walks an account listing this package
    /// measures at ~13 MB on a large account, so with TorBox configured first, every later store was called
    /// with an already-expired context and failed instantly. A Real-Debrid account that was actively
    /// downloading the release was asked twice and answered neither time — the pool reported "nobody is
    /// fetching it", and the caller queued a second copy. The store that knew the answer was in the list the
    /// whole time. Split evenly across the stores STILL TO BE ASKED, so an early store that answers quickly
    /// hands its unused time to the rest rather than the first one taking it all.
    pub async fn status_detail(
        &self,
        t: &ResolveTarget,
        deadline: Option<Instant>,
    ) -> (StoreStatus, bool, Vec<&dyn Store>) {
        let stores: Vec<&dyn Store> = self.stores.iter().map(|s| s.as_ref()).collect();
        status_detail_among(&stores, t, deadline).await
    }
}

/// `StatusDetail` over any list of the pool's stores, which is how the retry asks only the ones that could
/// not answer: the list it was handed back.
pub(crate) async fn status_detail_among<'a>(
    stores: &[&'a dyn Store],
    t: &ResolveTarget,
    deadline: Option<Instant>,
) -> (StoreStatus, bool, Vec<&'a dyn Store>) {
    let mut unknown: Vec<&'a dyn Store> = Vec::new();
    for (i, st) in stores.iter().enumerate() {
        let mut share = None;
        if let Some(deadline) = deadline {
            let now = Instant::now();
            if now >= deadline {
                // Out of time. The stores still unasked have not answered — which is not the same as
                // answering no, so they are the ones to retry.
                //
                // Together with the stores that ALREADY said they could not answer. Returning only the
                // unasked tail dropped them, so the escalation re-asked stores that had never been asked
                // (which answer "no" promptly) while never re-asking the one store that was uncertain —
                // and then /play concluded nobody was fetching the torrent and spent an add on it. That
                // is the failure the escalation exists to prevent, reintroduced by the change that made
                // the retry cheaper.
                unknown.extend(stores[i..].iter().copied());
                return (StoreStatus::default(), false, unknown);
            }
            share = Some((deadline - now) / u32::try_from(stores.len() - i).unwrap_or(u32::MAX));
        }
        // Prefer the three-valued answer where the store can give one.
        let ask = async {
            if let Some(answered) = st.status_answer(t).await {
                answered
            } else if let Some(s) = st.status(t).await {
                (s, StatusAnswer::Downloading)
            } else {
                (StoreStatus::default(), StatusAnswer::No)
            }
        };
        let (status, answer, cut_short) = match share {
            Some(share) => match tokio::time::timeout(share, ask).await {
                Ok((status, answer)) => (status, answer, false),
                Err(_) => (StoreStatus::default(), StatusAnswer::No, true),
            },
            None => {
                let (status, answer) = ask.await;
                (status, answer, false)
            }
        };
        // Two ways a store fails to answer, and both count: it said so itself (a throttled account, an
        // unreadable listing), or it was cut short by its own slice or by the caller going away. Only
        // deadlines were counted before, so a 503 read as a definitive "not downloading".
        let late = deadline.is_some_and(|d| Instant::now() > d) && answer != StatusAnswer::Downloading;
        if answer == StatusAnswer::Unknown || (answer != StatusAnswer::Downloading && (cut_short || late)) {
            unknown.push(*st);
        }
        if answer == StatusAnswer::Downloading {
            let mut status = status;
            status.service = Some(st.service());
            return (status, true, Vec::new());
        }
    }
    (StoreStatus::default(), false, unknown)
}
