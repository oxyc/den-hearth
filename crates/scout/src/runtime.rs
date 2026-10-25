//! Mutable state shared by every Scout handler hosted in one Hearth process.

use std::future::Future;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, LazyLock, Mutex, PoisonError};

use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio::time::Instant;

use crate::addbudget::{AddBudget, ADD_BUDGET_LIMIT, ADD_BUDGET_WINDOW};
use crate::metrics::{new_metric_set, MetricSet};
use crate::stores::torbox::TorboxRuntime;

/// Maximum number of distinct stream-list builds admitted across one hosted Scout runtime.
///
/// A list build fans out to indexers and debrid cache checks, retains the decoded config and result set, and
/// deliberately survives the request that started it. Sixteen leaves room for an ordinary household burst while
/// putting a hard ceiling under hostile unique-key cancellation traffic. Duplicate keys do not consume another
/// slot: the handler checks its flight map before asking this gate.
pub(crate) const MAX_CONCURRENT_LIST_BUILDS: usize = 16;

/// Maximum number of scraper response bodies buffered across every Scout handler in this process.
///
/// Hearth admits at most 48 bulk requests and Scout admits at most 16 distinct list builds. A build normally fans
/// out to seven sources, each allowed a 2 MiB body, so bounding builds alone still permits roughly 224 MiB of raw
/// scraper responses. Twenty-four requests cap that part of the working set near 48 MiB while retaining more
/// parallelism than the list-build limit. Tokio's semaphore wakes waiters in FIFO order; the outer Hearth and list
/// admission bounds keep its waiter set finite.
pub(crate) const MAX_CONCURRENT_SCRAPER_REQUESTS: usize = 24;

/// Told of each episode a viewer starts playing through a play route: its IMDb id, season and episode.
pub type PlayObserver = Arc<dyn Fn(&str, u32, u32) + Send + Sync>;

/// Explicit owner for budgets, counters, and provider coordination.
pub struct ScoutRuntime {
    id: u64,
    add_budget: Mutex<Arc<AddBudget>>,
    metrics: Arc<MetricSet>,
    list_builds: Arc<Semaphore>,
    scraper_requests: Arc<Semaphore>,
    pub(crate) torbox: TorboxRuntime,
    play_observer: std::sync::OnceLock<PlayObserver>,
}

/// One admitted list build. Dropping it returns the slot even when the detached task panics or is aborted.
pub(crate) struct ListBuildAdmission {
    _permit: OwnedSemaphorePermit,
    metrics: Arc<MetricSet>,
}

/// One scraper request admitted process-wide. It covers the response read and decode performed by `scrape`.
pub(crate) struct ScraperRequestAdmission {
    _permit: OwnedSemaphorePermit,
    metrics: Arc<MetricSet>,
}

impl Drop for ScraperRequestAdmission {
    fn drop(&mut self) {
        self.metrics.scraper_request_active.fetch_sub(1, Ordering::Relaxed);
    }
}

impl Drop for ListBuildAdmission {
    fn drop(&mut self) {
        self.metrics.list_build_active.fetch_sub(1, Ordering::Relaxed);
    }
}

impl ScoutRuntime {
    pub fn new() -> Arc<Self> {
        Self::build_with_limits(MAX_CONCURRENT_LIST_BUILDS, MAX_CONCURRENT_SCRAPER_REQUESTS)
    }

    #[cfg(test)]
    fn with_list_build_limit(list_build_limit: usize) -> Arc<Self> {
        Self::build_with_limits(list_build_limit, MAX_CONCURRENT_SCRAPER_REQUESTS)
    }

    fn build_with_limits(list_build_limit: usize, scraper_request_limit: usize) -> Arc<Self> {
        Self::with_components(list_build_limit, scraper_request_limit, TorboxRuntime::new())
    }

    fn with_components(
        list_build_limit: usize,
        scraper_request_limit: usize,
        torbox: TorboxRuntime,
    ) -> Arc<Self> {
        Arc::new(Self {
            id: NEXT_RUNTIME_ID
                .try_update(Ordering::Relaxed, Ordering::Relaxed, |id| id.checked_add(1))
                .expect("ScoutRuntime id space exhausted"),
            add_budget: Mutex::new(Arc::new(AddBudget::new(ADD_BUDGET_WINDOW, ADD_BUDGET_LIMIT))),
            metrics: Arc::new(new_metric_set()),
            list_builds: Arc::new(Semaphore::new(list_build_limit)),
            scraper_requests: Arc::new(Semaphore::new(scraper_request_limit)),
            torbox,
            play_observer: std::sync::OnceLock::new(),
        })
    }

    /// Report every episode play this runtime's handlers serve to `observer`. Set once; a later call is ignored.
    pub fn observe_plays(&self, observer: PlayObserver) {
        let _ = self.play_observer.set(observer);
    }

    /// Leaves TorBox's health as Scout's own calls would: `down` fails enough calls in a row to call it down, else
    /// one answer arrives slowly. For a dependant's tests, which cannot make TorBox's real API fail.
    #[cfg(feature = "test-support")]
    pub fn torbox_health_for_test(&self, down: bool) {
        use crate::stores::torbox::health::{CallOutcome, DOWN_AFTER, SLOW_ANSWER};
        let health = &self.torbox.health;
        if down {
            for _ in 0..DOWN_AFTER {
                health.record(CallOutcome::Failed("timeout"), Instant::now());
            }
        } else {
            health.record(CallOutcome::Answered(SLOW_ANSWER), Instant::now());
        }
    }

    pub(crate) fn played(&self, imdb: &str, season: u32, episode: u32) {
        if let Some(observer) = self.play_observer.get() {
            observer(imdb, season, episode);
        }
    }

    /// Fair, cancellation-safe admission for one real scraper request. The caller's absolute list-build deadline
    /// bounds both this wait and the request made under the returned guard. Dropping the wait future removes it
    /// from Tokio's FIFO queue; dropping the guard returns the permit on success, error, timeout, panic, or abort.
    pub(crate) async fn admit_scraper_request_until(
        &self,
        deadline: Instant,
    ) -> Option<ScraperRequestAdmission> {
        let permit = tokio::time::timeout_at(deadline, Arc::clone(&self.scraper_requests).acquire_owned())
            .await
            .ok()?
            .expect("the scraper request semaphore is never closed");
        self.metrics.scraper_request_admitted.fetch_add(1, Ordering::Relaxed);
        self.metrics.scraper_request_active.fetch_add(1, Ordering::Relaxed);
        Some(ScraperRequestAdmission { _permit: permit, metrics: Arc::clone(&self.metrics) })
    }

    /// Nonblocking admission for a new, distinct list build. The caller must first check whether its key already
    /// has a flight to join, and must keep the returned guard in the detached task for the build's whole lifetime.
    pub(crate) fn try_admit_list_build(&self) -> Option<ListBuildAdmission> {
        match Arc::clone(&self.list_builds).try_acquire_owned() {
            Ok(permit) => {
                self.metrics.list_build_admitted.fetch_add(1, Ordering::Relaxed);
                self.metrics.list_build_active.fetch_add(1, Ordering::Relaxed);
                Some(ListBuildAdmission { _permit: permit, metrics: Arc::clone(&self.metrics) })
            }
            Err(_) => {
                self.metrics.list_build_refused.fetch_add(1, Ordering::Relaxed);
                None
            }
        }
    }

    #[cfg(test)]
    pub(crate) fn new_with_list_build_limit(list_build_limit: usize) -> Arc<Self> {
        Self::with_list_build_limit(list_build_limit)
    }

    #[cfg(test)]
    pub(crate) fn new_with_limits(list_build_limit: usize, scraper_request_limit: usize) -> Arc<Self> {
        Self::build_with_limits(list_build_limit, scraper_request_limit)
    }

    #[cfg(test)]
    pub(crate) fn new_with_account_listing_limit(account_listing_limit: usize) -> Arc<Self> {
        Self::with_components(
            MAX_CONCURRENT_LIST_BUILDS,
            MAX_CONCURRENT_SCRAPER_REQUESTS,
            TorboxRuntime::new_with_listing_limit(account_listing_limit),
        )
    }

    pub(crate) fn id(&self) -> u64 {
        self.id
    }

    pub(crate) fn add_budget(&self) -> Arc<AddBudget> {
        Arc::clone(&self.add_budget.lock().unwrap_or_else(PoisonError::into_inner))
    }

    pub(crate) fn metrics(&self) -> Arc<MetricSet> {
        Arc::clone(&self.metrics)
    }

    pub(crate) fn set_play_reserve(&self, reserve: i64) {
        self.add_budget().reserve.store(reserve, Ordering::SeqCst);
    }

    #[cfg(test)]
    pub(crate) fn replace_add_budget(&self, budget: Arc<AddBudget>) -> Arc<AddBudget> {
        std::mem::replace(&mut *self.add_budget.lock().unwrap_or_else(PoisonError::into_inner), budget)
    }
}

// A runtime id is also a namespace for process-wide flights and batchers. Do not derive it from the Arc's
// address: after the last owner drops, the allocator may reuse that address while an old detached call is still
// removing its key. Exhausting this counter is preferable to ever reusing a namespace.
static NEXT_RUNTIME_ID: AtomicU64 = AtomicU64::new(1);

static SHARED_RUNTIME: LazyLock<Arc<ScoutRuntime>> = LazyLock::new(ScoutRuntime::new);

tokio::task_local! {
    static ACTIVE_RUNTIME: Arc<ScoutRuntime>;
}

pub(crate) fn current_runtime() -> Arc<ScoutRuntime> {
    ACTIVE_RUNTIME.try_with(Arc::clone).unwrap_or_else(|_| Arc::clone(&SHARED_RUNTIME))
}

pub(crate) async fn scope<T>(runtime: Arc<ScoutRuntime>, future: impl Future<Output = T>) -> T {
    ACTIVE_RUNTIME.scope(runtime, future).await
}

/// Compatibility default for callers that have not adopted `build_deps_with_runtime` yet.
pub fn shared_runtime() -> Arc<ScoutRuntime> {
    Arc::clone(&SHARED_RUNTIME)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::util::DetachedFlight;
    use std::time::Duration;

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn detached_flights_are_namespaced_by_owner_and_preserve_the_owner() {
        let first = ScoutRuntime::new();
        let second = ScoutRuntime::new();
        let flight = Arc::new(DetachedFlight::new("runtime owner test"));
        let rendezvous = Arc::new(tokio::sync::Barrier::new(3));

        let run = |runtime: Arc<ScoutRuntime>| {
            let flight = flight.clone();
            let rendezvous = rendezvous.clone();
            tokio::spawn(scope(runtime, async move {
                flight
                    .run("same key", || async move {
                        rendezvous.wait().await;
                        current_runtime().id()
                    })
                    .await
            }))
        };
        let first_flight = run(first.clone());
        let second_flight = run(second.clone());

        tokio::time::timeout(Duration::from_secs(1), rendezvous.wait())
            .await
            .expect("both owners must start independent work for the same flight key");
        let (first_answer, second_answer) = tokio::join!(first_flight, second_flight);
        assert_eq!(first_answer.unwrap(), (Some(first.id()), false));
        assert_eq!(second_answer.unwrap(), (Some(second.id()), false));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn runtime_clones_share_a_flight_and_budget() {
        let runtime = ScoutRuntime::new();
        let runtime_clone = runtime.clone();
        assert!(Arc::ptr_eq(&runtime.add_budget(), &runtime_clone.add_budget()));
        runtime.add_budget().take("clone-account", false);
        assert_eq!(runtime.add_budget().remaining("clone-account"), ADD_BUDGET_LIMIT - 1);
        assert_eq!(runtime_clone.add_budget().remaining("clone-account"), ADD_BUDGET_LIMIT - 1);

        let flight = Arc::new(DetachedFlight::new("runtime clone test"));
        let release = Arc::new(tokio::sync::Semaphore::new(0));
        let executions = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let run = |owner: Arc<ScoutRuntime>| {
            let flight = flight.clone();
            let release = release.clone();
            let executions = executions.clone();
            tokio::spawn(scope(owner, async move {
                flight
                    .run("same key", || async move {
                        executions.fetch_add(1, Ordering::SeqCst);
                        release.acquire().await.unwrap().forget();
                        current_runtime().id()
                    })
                    .await
            }))
        };
        let first = run(runtime.clone());

        tokio::time::timeout(Duration::from_secs(1), async {
            while executions.load(Ordering::SeqCst) != 1 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("the leader must start");
        let second = run(runtime_clone);
        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                let joined = scope(runtime.clone(), async { flight.joiners("same key") }).await;
                if joined == Some(1) {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("the runtime clone must join the owner's existing flight");

        release.add_permits(1);
        let (first_answer, second_answer) = tokio::join!(first, second);
        assert_eq!(executions.load(Ordering::SeqCst), 1);
        assert_eq!(first_answer.unwrap(), (Some(runtime.id()), true));
        assert_eq!(second_answer.unwrap(), (Some(runtime.id()), true));
    }

    #[tokio::test]
    async fn admitted_detached_flight_returns_slot_after_timed_work() {
        let runtime = ScoutRuntime::new();
        let flight = DetachedFlight::new("timed admission test");
        let slots = Arc::new(Semaphore::new(1));
        let answer = scope(runtime, async {
            flight
                .run_admitted("timed", &slots, || async {
                    tokio::time::timeout(Duration::from_millis(10), std::future::pending::<()>())
                        .await
                        .is_err()
                })
                .await
        })
        .await;
        assert_eq!(answer, Some((Some(true), false)));
        assert_eq!(flight.active(), 0, "timed work leaked its flight key");
        assert_eq!(slots.available_permits(), 1, "timed work leaked its admission permit");
    }

    #[tokio::test]
    async fn scraper_admission_recovers_after_wait_timeout_and_owner_cancellation() {
        let runtime = ScoutRuntime::new_with_limits(MAX_CONCURRENT_LIST_BUILDS, 1);
        let held = runtime
            .admit_scraper_request_until(Instant::now() + Duration::from_secs(1))
            .await
            .expect("the first scraper request is admitted");

        assert!(
            runtime.admit_scraper_request_until(Instant::now() + Duration::from_millis(10)).await.is_none(),
            "a queued scraper request outlived its deadline"
        );
        assert_eq!(runtime.metrics().scraper_request_active.load(Ordering::SeqCst), 1);

        drop(held);
        let recovered = runtime
            .admit_scraper_request_until(Instant::now() + Duration::from_secs(1))
            .await
            .expect("the timed-out waiter leaked or stole the only permit");
        assert_eq!(runtime.metrics().scraper_request_active.load(Ordering::SeqCst), 1);
        drop(recovered);
        assert_eq!(runtime.metrics().scraper_request_active.load(Ordering::SeqCst), 0);

        let entered = Arc::new(tokio::sync::Barrier::new(2));
        let owner = tokio::spawn({
            let runtime = runtime.clone();
            let entered = entered.clone();
            async move {
                let _permit = runtime
                    .admit_scraper_request_until(Instant::now() + Duration::from_secs(1))
                    .await
                    .expect("the cancellable owner is admitted");
                entered.wait().await;
                std::future::pending::<()>().await;
            }
        });
        entered.wait().await;
        assert_eq!(runtime.metrics().scraper_request_active.load(Ordering::SeqCst), 1);
        owner.abort();
        assert!(owner.await.unwrap_err().is_cancelled());
        assert_eq!(
            runtime.metrics().scraper_request_active.load(Ordering::SeqCst),
            0,
            "canceling admitted work leaked its permit"
        );
        let final_request = runtime
            .admit_scraper_request_until(Instant::now() + Duration::from_secs(1))
            .await
            .expect("the canceled owner leaked the only permit");
        drop(final_request);
    }
}
