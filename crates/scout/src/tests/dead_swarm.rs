//! A torrent stalled with no seeds stops costing a TorBox call per poll once it has been dead for a while.

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crate::cache::{Cache, MemoryCache};
use crate::stores::refusal::torrent_id_key;
use crate::stores::torbox::{
    clear_torbox_status, torbox_dead_swarm_key, TorBoxStore, DEAD_SWARM_AFTER, DEAD_SWARM_RECHECK,
    RESOLVE_CACHE_TTL, TORBOX_API,
};
use crate::stores::{ProviderStatus, ResolveTarget, Store};
use crate::tests::mock::{resp, MockDoer};

const HASH: &str = "c3389aa7019b0000000000000000000000000000";

struct Fixture {
    store: Arc<TorBoxStore>,
    cache: Arc<MemoryCache>,
    clock: Arc<Mutex<SystemTime>>,
    asked: Arc<AtomicUsize>,
    seeded: Arc<AtomicBool>,
    runtime: Arc<crate::runtime::ScoutRuntime>,
}

impl Fixture {
    fn new() -> Fixture {
        let clock = Arc::new(Mutex::new(SystemTime::now()));
        let mut cache = MemoryCache::new(1 << 20);
        let now = clock.clone();
        cache.set_clock(Box::new(move || *now.lock().unwrap_or_else(PoisonError::into_inner)));
        let cache = Arc::new(cache);
        let (asked, seeded) = (Arc::new(AtomicUsize::new(0)), Arc::new(AtomicBool::new(false)));
        let (count, seeds) = (asked.clone(), seeded.clone());
        cache.put(&torrent_id_key("tb-secret", HASH), "42", RESOLVE_CACHE_TTL);
        let store = Arc::new(TorBoxStore {
            token: "tb-secret".to_owned(),
            cache: Some(cache.clone()),
            api: TORBOX_API.to_owned(),
            client: Arc::new(MockDoer(move |req| {
                assert!(req.url().path().ends_with("/torrents/mylist"), "{}", req.url());
                count.fetch_add(1, Ordering::SeqCst);
                let (state, seeds) =
                    if seeds.load(Ordering::SeqCst) { ("downloading", 2) } else { ("stalled (no seeds)", 0) };
                Ok(resp(
                    200,
                    &format!(
                        r#"{{"success":true,"data":{{"id":42,"progress":0,"download_finished":false,"download_state":"{state}","download_speed":0,"seeds":{seeds},"peers":5}}}}"#
                    ),
                ))
            })),
        });
        Fixture { store, cache, clock, asked, seeded, runtime: crate::runtime::ScoutRuntime::new() }
    }

    /// On the fixture's own runtime: status reads are shared per account and hash within one, and these tests share
    /// both.
    async fn poll(&self) -> ProviderStatus {
        let target = ResolveTarget { info_hash: HASH.to_owned(), ..Default::default() };
        crate::runtime::scope(self.runtime.clone(), self.store.provider_status(&target)).await
    }

    /// Moves the cache's clock, which is what expires a held status.
    fn advance(&self, by: Duration) {
        *self.clock.lock().unwrap_or_else(PoisonError::into_inner) += by;
    }

    /// Says the torrent was first seen dead `ago`.
    fn dead_since(&self, ago: Duration) {
        let at = SystemTime::now() - ago;
        let ms = at.duration_since(UNIX_EPOCH).map_or(0, |d| d.as_millis());
        let key = torbox_dead_swarm_key(&self.store.account_identity(), HASH);
        self.cache.put(&key, &format!("{ms}:0.0000"), DEAD_SWARM_AFTER * 2);
    }

    fn asked(&self) -> usize {
        self.asked.load(Ordering::SeqCst)
    }
}

/// Inside `DEAD_SWARM_AFTER` every poll past the 5 s snapshot asks TorBox, as before.
#[tokio::test]
async fn a_fresh_stall_is_asked_about_on_every_poll() {
    let f = Fixture::new();
    assert!(matches!(f.poll().await, ProviderStatus::Downloading(_)));
    assert_eq!(f.asked(), 1);
    f.advance(Duration::from_secs(6));
    f.poll().await;
    assert_eq!(f.asked(), 2, "a stall minutes old is still asked about");
}

/// Past it, the answer is held for `DEAD_SWARM_RECHECK`: the polls in between cost nothing upstream, and say the
/// same thing they would have.
#[tokio::test]
async fn a_long_dead_swarm_is_asked_about_once_per_recheck() {
    let f = Fixture::new();
    f.dead_since(DEAD_SWARM_AFTER + Duration::from_secs(60));
    f.poll().await;
    assert_eq!(f.asked(), 1);
    for _ in 0..20 {
        f.advance(Duration::from_secs(20));
        let ProviderStatus::Downloading(st) = f.poll().await else { panic!("still downloading") };
        assert_eq!((st.state.as_str(), st.seeds, st.peers), ("stalled", Some(0), Some(5)));
    }
    assert_eq!(f.asked(), 1, "twenty polls over 400 s, one TorBox call");
    f.advance(DEAD_SWARM_RECHECK);
    f.poll().await;
    assert_eq!(f.asked(), 2, "asked again once the recheck is due");
}

/// A swarm that comes back is heard at the recheck, and from then on is asked about at the normal cadence.
#[tokio::test]
async fn a_swarm_that_returns_starts_the_clock_over() {
    let f = Fixture::new();
    f.dead_since(DEAD_SWARM_AFTER + Duration::from_secs(60));
    f.poll().await;
    f.seeded.store(true, Ordering::SeqCst);
    f.advance(DEAD_SWARM_RECHECK + Duration::from_secs(1));
    let ProviderStatus::Downloading(st) = f.poll().await else { panic!("downloading") };
    assert_eq!(st.seeds, Some(2));
    f.advance(Duration::from_secs(6));
    f.poll().await;
    assert_eq!(f.asked(), 3, "seeded again: back to the 5 s snapshot");
}

/// Whatever clears the status snapshot (an add, a resolve, a reannounce) clears the clock with it.
#[tokio::test]
async fn clearing_the_snapshot_clears_the_clock() {
    let f = Fixture::new();
    f.dead_since(DEAD_SWARM_AFTER + Duration::from_secs(60));
    clear_torbox_status(&*f.cache, &f.store.account_identity(), HASH);
    f.poll().await;
    f.advance(Duration::from_secs(6));
    f.poll().await;
    assert_eq!(f.asked(), 2, "a new start is asked about at the normal cadence");
}
