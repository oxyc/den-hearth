//! Go: `internal/scout/stores_preference_test.go` (294 lines). Work package 2.
//!
//! With more than one debrid, WHICH service resolves a release decides whether it plays now or downloads
//! first. CacheCheck already learns that and the union threw it away, so the fixed store order could send
//! a viewer to download a file sitting cached on their other account.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;

use crate::config::DebridService;
use crate::stores::pool::StorePool;
use crate::stores::{DeadLinkError, ResolveTarget, Store, StoreError, StoreStatus, StoreUnavailableError};

/// Go `H` (stores_test.go:19).
const H: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";

/// Go `resolves *[]DebridService`: every store in a test appends to the same list.
type Calls = Arc<Mutex<Vec<DebridService>>>;

fn new_calls() -> Calls {
    Arc::new(Mutex::new(Vec::new()))
}

/// Go `namedStore`: a fake store with a fixed cache answer and a fixed resolve outcome.
struct NamedStore {
    svc: DebridService,
    /// `None` means the store has no cache API, so its check errors.
    cached: Option<HashMap<String, bool>>,
    /// Empty means the release is dead on this store.
    link: String,
    /// The store faulted or throttled US, rather than the release being dead.
    refuse: bool,
    resolves: Calls,
}

/// A `NamedStore` with no cache API, no link and no refusal, for struct-update syntax.
fn named(svc: DebridService, resolves: &Calls) -> NamedStore {
    NamedStore { svc, cached: None, link: String::new(), refuse: false, resolves: resolves.clone() }
}

fn cached(entries: &[(&str, bool)]) -> Option<HashMap<String, bool>> {
    Some(entries.iter().map(|(h, v)| (h.to_string(), *v)).collect())
}

fn boxed<S: Store + 'static>(s: S) -> Box<dyn Store> {
    Box::new(s)
}

fn pool_of(stores: Vec<Box<dyn Store>>) -> StorePool {
    StorePool { stores }
}

fn target(hash: &str) -> ResolveTarget {
    ResolveTarget { info_hash: hash.to_owned(), ..Default::default() }
}

#[async_trait]
impl Store for NamedStore {
    fn service(&self) -> DebridService {
        self.svc
    }

    async fn cache_check(&self, _hashes: &[String]) -> Result<HashMap<String, bool>, StoreError> {
        match &self.cached {
            Some(m) => Ok(m.clone()),
            None => Err(StoreError::Other("no cache api".into())),
        }
    }

    async fn resolve(&self, _t: &ResolveTarget) -> Result<String, StoreError> {
        self.resolves.lock().expect("calls lock").push(self.svc);
        if self.refuse {
            return Err(StoreError::Unavailable(StoreUnavailableError {
                service: self.svc,
                reason: "throttled".into(),
                status: 0,
            }));
        }
        if self.link.is_empty() {
            return Err(StoreError::DeadLink(DeadLinkError {
                reason: format!("{} has nothing", self.svc.as_str()),
            }));
        }
        Ok(self.link.clone())
    }

    async fn status(&self, _t: &ResolveTarget) -> Option<StoreStatus> {
        None
    }
}

#[tokio::test]
async fn cache_truth_reports_each_service_holding_it() {
    let calls = new_calls();
    let pool = pool_of(vec![
        boxed(NamedStore { cached: cached(&[(H, false)]), ..named(DebridService::TorBox, &calls) }),
        boxed(NamedStore { cached: cached(&[(H, true)]), ..named(DebridService::Premiumize, &calls) }),
    ]);
    let (truth, _) = pool.cache_check(&[H.to_string()]).await;
    assert_eq!(truth.held_by(H), vec![DebridService::Premiumize], "HeldBy");
    // Cached is the union — SOMEONE holds it — while HeldBy says who. Conflating the two sent the probe
    // path to resolve a Premiumize-only release against TorBox, which adds it.
    assert!(truth.cached(H), "a release one store holds is cached");

    // A store whose cache API errored contributes nothing rather than a false "not cached".
    let broken = pool_of(vec![boxed(named(DebridService::TorBox, &calls))]);
    let (broken_truth, _) = broken.cache_check(&[H.to_string()]).await;
    assert!(
        !broken_truth.cached(H) && !broken_truth.known(H),
        "an errored cache check must not claim knowledge"
    );
}

// A hash a store could not check is UNKNOWN, not "not cached". They were the same value, and the
// difference decides whether a cached-only list silently drops a playable release, and whether pressing
// play pays an add for a torrent the account already holds.
#[tokio::test]
async fn cache_truth_separates_unknown_from_not_cached() {
    let calls = new_calls();
    // The store answered for one hash and not the other — what a partially-failed batch looks like.
    let other = "b".repeat(40);
    let pool = pool_of(vec![boxed(NamedStore {
        cached: cached(&[(H, false)]),
        ..named(DebridService::TorBox, &calls)
    })]);
    let (truth, ok) = pool.cache_check(&[H.to_string(), other.clone()]).await;
    assert!(ok, "a store that answered at all is not a total outage");
    assert!(
        truth.known(H) && !truth.cached(H),
        "H was answered for, and the answer was no: known={} cached={}",
        truth.known(H),
        truth.cached(H)
    );
    assert!(
        !truth.known(&other),
        "a hash the store never reported on must read as unknown, not as not-cached"
    );
}

// The probe path must never reach a store that would ADD the release.
#[tokio::test]
async fn resolve_cached_only_never_touches_a_non_holder() {
    let calls = new_calls();
    let pool = pool_of(vec![
        // configured first, does NOT hold it
        boxed(named(DebridService::TorBox, &calls)),
        boxed(NamedStore { link: "https://pm/x".into(), ..named(DebridService::Premiumize, &calls) }),
    ]);
    match pool.resolve_cached_only(&target(H), &[DebridService::Premiumize]).await {
        Ok(link) if link == "https://pm/x" => {}
        other => panic!("resolve from the holder: {other:?}"),
    }
    assert_eq!(
        *calls.lock().expect("calls lock"),
        vec![DebridService::Premiumize],
        "TorBox does not hold this, and asking it would add the torrent"
    );

    // With no known holder it refuses outright rather than shopping around.
    calls.lock().expect("calls lock").clear();
    assert!(pool.resolve_cached_only(&target(H), &[]).await.is_err(), "no holder should be a refusal");
    assert!(calls.lock().expect("calls lock").is_empty(), "asked a store with no holder known");

    // A holder that is configured but not in the pool leaves nothing to ask — a dead link, not a crash.
    let absent = pool_of(vec![boxed(named(DebridService::TorBox, &calls))]);
    assert!(
        absent.resolve_cached_only(&target(H), &[DebridService::Premiumize]).await.is_err(),
        "a holder that is not in the pool cannot resolve"
    );

    // A store REFUSING us (throttled, faulting) outranks "dead" as the explanation — the same rule
    // ResolvePreferring follows, because a refusal is not evidence that a release does not exist.
    let refusing = pool_of(vec![boxed(NamedStore { refuse: true, ..named(DebridService::TorBox, &calls) })]);
    let err = refusing.resolve_cached_only(&target(H), &[DebridService::TorBox]).await;
    assert!(
        matches!(err, Err(StoreError::Unavailable(_))),
        "a refusal must surface as unavailable, not as a dead link: {err:?}"
    );
}

#[tokio::test]
async fn resolve_preferring_tries_the_holder_first() {
    let calls = new_calls();
    let pool = pool_of(vec![
        // configured first, has nothing
        boxed(named(DebridService::TorBox, &calls)),
        boxed(NamedStore { link: "https://pm/x".into(), ..named(DebridService::Premiumize, &calls) }),
    ]);
    match pool.resolve_preferring(&target(H), &[DebridService::Premiumize]).await {
        Ok(link) if link == "https://pm/x" => {}
        other => panic!("link/err: {other:?}"),
    }
    assert_eq!(
        *calls.lock().expect("calls lock"),
        vec![DebridService::Premiumize],
        "the holder must be tried first, and alone when it succeeds"
    );
}

// The preference is an ordering, not a filter: if the holder fails, the others must still be tried.
#[tokio::test]
async fn resolve_preferring_falls_through_when_the_holder_fails() {
    let calls = new_calls();
    let pool = pool_of(vec![
        boxed(NamedStore { link: "https://tb/x".into(), ..named(DebridService::TorBox, &calls) }),
        // "holds" it but can't resolve
        boxed(named(DebridService::Premiumize, &calls)),
    ]);
    match pool.resolve_preferring(&target(H), &[DebridService::Premiumize]).await {
        Ok(link) if link == "https://tb/x" => {}
        other => panic!("link/err: {other:?}"),
    }
    assert_eq!(
        *calls.lock().expect("calls lock"),
        vec![DebridService::Premiumize, DebridService::TorBox],
        "want premiumize then torbox"
    );
}

// No preference must behave exactly as before — configured order, deterministically.
#[tokio::test]
async fn resolve_preferring_without_preference_keeps_configured_order() {
    let calls = new_calls();
    let pool = pool_of(vec![
        boxed(named(DebridService::TorBox, &calls)),
        boxed(NamedStore { link: "https://pm/x".into(), ..named(DebridService::Premiumize, &calls) }),
    ]);
    if let Err(e) = pool.resolve(&target(H)).await {
        panic!("{e:?}");
    }
    let got = calls.lock().expect("calls lock").clone();
    assert!(got.len() == 2 && got[0] == DebridService::TorBox, "asked {got:?}, want the configured order");
}

// A preference naming a service that isn't configured must not drop the ones that are.
#[tokio::test]
async fn resolve_preferring_ignores_unknown_services() {
    let calls = new_calls();
    let pool = pool_of(vec![boxed(NamedStore {
        link: "https://tb/x".into(),
        ..named(DebridService::TorBox, &calls)
    })]);
    match pool.resolve_preferring(&target(H), &[DebridService::Premiumize]).await {
        Ok(link) if link == "https://tb/x" => {}
        other => panic!("link/err: {other:?}"),
    }
}

// Every store gets a turn. An earlier version stopped after one non-holder had been asked, to keep a
// single play from queueing the same torrent on every account — but that bound bites only when a store
// FAILS, which is exactly when the next store is the viewer's last chance. It turned "the first two
// accounts could not serve this" into "you cannot play this", for a release that used to play. Adds are
// bounded by spendAdd instead, which is a ceiling rather than a coin flip on which store gets to try.
#[tokio::test]
async fn resolve_preferring_never_stops_short_of_a_store_that_could_serve() {
    let calls = new_calls();
    let pool = pool_of(vec![
        // holder, but cannot serve
        boxed(named(DebridService::TorBox, &calls)),
        // dead-links too
        boxed(named(DebridService::RealDebrid, &calls)),
        boxed(NamedStore { link: "https://pm/x".into(), ..named(DebridService::Premiumize, &calls) }),
    ]);
    match pool.resolve_preferring(&target(H), &[DebridService::TorBox]).await {
        Ok(link) if link == "https://pm/x" => {}
        other => panic!("the third store could serve it and must be asked: {other:?}"),
    }
    let got = calls.lock().expect("calls lock").clone();
    assert_eq!(got.len(), 3, "stores asked: {got:?} — a store that could serve the release was skipped");
}

// With NO holders known — a cache-check outage, or an RD-only install that has no cache truth to give —
// the bound must not apply. There the fallthrough is the only way to resolve at all, and refusing it
// turns "we could not find out" into "you cannot play this". The add budget bounds that case instead.
#[tokio::test]
async fn resolve_preferring_unknown_holders_still_falls_through() {
    let calls = new_calls();
    let pool = pool_of(vec![
        boxed(named(DebridService::TorBox, &calls)),
        boxed(NamedStore { link: "https://rd/x".into(), ..named(DebridService::RealDebrid, &calls) }),
    ]);
    match pool.resolve_preferring(&target(H), &[]).await {
        Ok(link) if link == "https://rd/x" => {}
        other => panic!("with nothing known, the fallthrough is the only route: {other:?}"),
    }
    let got = calls.lock().expect("calls lock").clone();
    assert_eq!(got.len(), 2, "stores asked: {got:?} — the bound applied where nothing was known");
}

// What "known" means, in the three cases that actually occur.
//
// A "yes" is knowledge on its own: one store confirming it HOLDS a release settles the question, and no
// other store's silence unsettles it. A "no" needs everyone who could answer to have answered, because a
// store that never voted may be the one holding it. And a store that answered NOTHING is out — excluded,
// so its outage does not erase what the healthy store confirmed.
//
// Both halves have been wrong in turn. The union treated a partial "no" as knowledge: a confident
// "nobody has this" from the only party that never voted. Requiring unanimity for both was worse — it
// discarded the only certain facts in the set, so a cachedOnly list stopped filtering entirely and every
// response was permanently degraded and uncacheable, with the filter on and inert.
#[tokio::test]
async fn cache_truth_what_counts_as_known() {
    let calls = new_calls();
    let other = "b".repeat(40);

    // Both stores answered for H; only Premiumize answered for `other`.
    let partial = pool_of(vec![
        boxed(NamedStore { cached: cached(&[(H, false)]), ..named(DebridService::TorBox, &calls) }),
        boxed(NamedStore {
            cached: cached(&[(H, false), (other.as_str(), false)]),
            ..named(DebridService::Premiumize, &calls)
        }),
    ]);
    let (truth, truth_ok) = partial.cache_check(&[H.to_string(), other.clone()]).await;
    assert!(truth_ok, "stores answered; this is not an outage");
    assert!(truth.known(H), "both stores said no about H — that is knowledge");
    assert!(!truth.known(&other), "TorBox never voted on `other`, so 'not cached' is not something we know");

    // A holder's yes stands alone, even where TorBox said nothing.
    let held = pool_of(vec![
        boxed(NamedStore { cached: cached(&[(H, false)]), ..named(DebridService::TorBox, &calls) }),
        boxed(NamedStore {
            cached: cached(&[(H, false), (other.as_str(), true)]),
            ..named(DebridService::Premiumize, &calls)
        }),
    ]);
    let (yes, _) = held.cache_check(&[H.to_string(), other.clone()]).await;
    assert!(
        yes.known(&other) && yes.cached(&other),
        "a store confirming it HOLDS a release settles the question by itself"
    );

    // Both answered, so nothing is missing.
    assert!(truth.complete(), "both cache-truth stores answered; the picture is complete");

    // One store entirely out. Its silence is exactly why the survivor's "no" cannot rule anything out,
    // so the hash is NOT known — and the outage is reported separately, through Complete, so the handler
    // can stop filtering rather than quietly serving an empty list. Dropping the down store from the
    // count instead made the survivor authoritative: a cachedOnly request returned {"streams":[]}, with
    // no degraded header, cached for five minutes and held for a day on stale-if-error.
    let down = pool_of(vec![
        // its check errored: no answers at all
        boxed(named(DebridService::TorBox, &calls)),
        boxed(NamedStore { cached: cached(&[(H, false)]), ..named(DebridService::Premiumize, &calls) }),
    ]);
    let (survivor, survivor_ok) = down.cache_check(&[H.to_string()]).await;
    assert!(survivor_ok, "one store still answered");
    assert!(
        !survivor.known(H),
        "the down store may be the one holding it; the survivor's no cannot settle that"
    );
    assert!(
        !survivor.complete(),
        "a store that answered nothing must make the answer incomplete, or the outage is invisible"
    );
}
