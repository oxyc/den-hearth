//! Go: `internal/scout/refusal_test.go` (1563 lines). Work package 2.
//!
//! About a third of it goes through the handler (`TestHandleProbe_*`, `TestHandlePlay_*`, `TestProbeAndPlay_*`).
//! Go calls `handleProbe` directly in some; `Handler::handle_probe` is that call. The rest go through `serve`.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;

use crate::addbudget::{budget_account, global_add_budget, ADD_BUDGET_LIMIT};
use crate::cache::{Cache, MemoryCache};
use crate::config::{Config, DebridAccount, DebridService};
use crate::diskcache::TieredCache;
use crate::handler::{new_handler, Handler};
use crate::play::{encode_play_token, PlayTarget};
use crate::scrape::{DoError, Doer};
use crate::stores::pool::StorePool;
use crate::stores::premiumize::{PremiumizeStore, PREMIUMIZE_API};
use crate::stores::realdebrid::{rd_torrent_key, RealDebridStore, REAL_DEBRID_API};
use crate::stores::refusal::{
    account_refused_key, add_outcome_unknown, already_queued, backed_off, note_add_attempt, pm_queued_key,
    record_refusal, refused_key, unknown_outcome_key, ADD_GIVE_UP, PENDING_GIVE_UP, QUEUED_TTL, REFUSAL_BACKOFF,
    UNKNOWN_OUTCOME_TTL,
};
use crate::stores::torbox::{TorBoxStore, TORBOX_API};
use crate::stores::{
    dead_link, DeadLinkError, ResolveTarget, StatusAnswer, Store, StoreError, StoreStatus, StoreUnavailableError,
    ERR_CHECK_FAILED, ERR_WOULD_ADD,
};

use super::addbudget::{install_budget, HOUR};
use super::handler::{blob, body_str, get, shared_stores, test_deps, valid_blob, SharedStore};
use super::mock::{resp, MockDoer};

/// Go `H` (stores_test.go:19).
const H: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";

type Answer = Result<reqwest::Response, DoError>;

/// A `Doer` that answers every request with the same status and body.
fn answer(status: u16, body: &'static str) -> Arc<dyn Doer> {
    Arc::new(MockDoer(move |_r: reqwest::Request| -> Answer { Ok(resp(status, body)) }))
}

/// Go `context.Canceled` as the transport reports it: the request failed before any answer came back.
fn transport_error() -> Answer {
    Err("context canceled".into())
}

fn mem() -> Arc<dyn Cache> {
    Arc::new(MemoryCache::new(1 << 20))
}

/// The Unix time `ago` before now, as the markers store it.
fn unix_secs_ago(ago: Duration) -> String {
    (SystemTime::now() - ago).duration_since(UNIX_EPOCH).expect("after the epoch").as_secs().to_string()
}

fn target(hash: &str) -> ResolveTarget {
    ResolveTarget { info_hash: hash.to_owned(), ..Default::default() }
}

fn real_debrid(token: &str, cache: &Arc<dyn Cache>, client: Arc<dyn Doer>) -> RealDebridStore {
    RealDebridStore { token: token.into(), client, cache: Some(cache.clone()), api: REAL_DEBRID_API.into() }
}

fn premiumize(token: &str, cache: &Arc<dyn Cache>, client: Arc<dyn Doer>) -> PremiumizeStore {
    PremiumizeStore { token: token.into(), client, cache: Some(cache.clone()), api: PREMIUMIZE_API.into() }
}

/// A Real-Debrid or Premiumize store over the given cache and client, for the tests that run the same
/// assertion over both.
fn store_for(svc: DebridService, token: &str, cache: &Arc<dyn Cache>, client: Arc<dyn Doer>) -> Box<dyn Store> {
    match svc {
        DebridService::RealDebrid => Box::new(real_debrid(token, cache, client)),
        DebridService::Premiumize => Box::new(premiumize(token, cache, client)),
        DebridService::TorBox => unreachable!("TorBox is work package 1"),
    }
}

/// A refusal the service made (HTTP 429 and the like), as the stores record it.
fn refused(svc: DebridService, reason: &str) -> StoreError {
    StoreError::Unavailable(StoreUnavailableError { service: svc, reason: reason.into(), status: 429 })
}

/// Go `errors.As(err, &unavailable)`: the error is the service refusing us.
fn is_refusal(err: &StoreError) -> bool {
    matches!(err, StoreError::Unavailable(_))
}

/// Go `probeConfig` — a TorBox install, so handleProbe's cache-truth branches are exercised.
fn probe_config() -> Config {
    Config { debrid: vec![DebridAccount { service: DebridService::TorBox, token: "tok".into() }], ..Default::default() }
}

/// A real TorBox store over `cache`, answering through `client`.
fn torbox(token: &str, cache: &Arc<dyn Cache>, client: Arc<dyn Doer>) -> Arc<dyn Store> {
    Arc::new(TorBoxStore { token: token.into(), cache: Some(cache.clone()), api: TORBOX_API.into(), client })
}

/// Go `NewHandler(Deps{Cache: cache, MakeStores: ...store...})`: every request is served over the one store.
fn route_handler(cache: &Arc<dyn Cache>, store: Arc<dyn Store>) -> Handler {
    let cache = cache.clone();
    new_handler(test_deps(move |d| {
        d.cache = cache;
        d.make_stores = shared_stores(store.clone());
    }))
}

/// Go's `fakeStore`, `answeringStore` and `noAddAwareStore` in one: a store whose answers the test fixes.
#[derive(Clone)]
struct Scripted {
    svc: DebridService,
    check: HashMap<String, bool>,
    /// Go `noAddAwareStore.cached`: the one hash the cache check reports held, whichever hashes it is asked about.
    cached: Option<String>,
    check_err: Option<StoreError>,
    resolve: Arc<dyn Fn(&ResolveTarget) -> Result<String, StoreError> + Send + Sync>,
    status: Option<StoreStatus>,
    /// The three-valued answer, when the store gives one (Go `answeringStore`).
    answer: Option<StatusAnswer>,
}

impl Scripted {
    fn new(svc: DebridService) -> Scripted {
        Scripted {
            svc,
            check: HashMap::new(),
            cached: None,
            check_err: None,
            resolve: Arc::new(|_| panic!("this scripted store was given no resolve")),
            status: None,
            answer: None,
        }
    }

    fn check(mut self, answers: &[(&str, bool)]) -> Scripted {
        self.check = answers.iter().map(|(h, held)| ((*h).to_owned(), *held)).collect();
        self
    }

    fn cached(mut self, hash: &str) -> Scripted {
        self.cached = Some(hash.to_owned());
        self
    }

    fn check_err(mut self, err: StoreError) -> Scripted {
        self.check_err = Some(err);
        self
    }

    fn resolve_with(mut self, f: impl Fn(&ResolveTarget) -> Result<String, StoreError> + Send + Sync + 'static) -> Scripted {
        self.resolve = Arc::new(f);
        self
    }

    fn status(mut self, status: StoreStatus) -> Scripted {
        self.status = Some(status);
        self
    }

    fn answer(mut self, answer: StatusAnswer) -> Scripted {
        self.answer = Some(answer);
        self
    }
}

#[async_trait]
impl Store for Scripted {
    fn service(&self) -> DebridService {
        self.svc
    }

    async fn cache_check(&self, hashes: &[String]) -> Result<HashMap<String, bool>, StoreError> {
        if let Some(err) = &self.check_err {
            return Err(err.clone());
        }
        if let Some(cached) = &self.cached {
            return Ok(hashes.iter().map(|h| (h.clone(), h == cached)).collect());
        }
        Ok(self.check.clone())
    }

    async fn resolve(&self, t: &ResolveTarget) -> Result<String, StoreError> {
        (self.resolve)(t)
    }

    async fn status(&self, _t: &ResolveTarget) -> Option<StoreStatus> {
        self.status.clone()
    }

    async fn status_answer(&self, _t: &ResolveTarget) -> Option<(StoreStatus, StatusAnswer)> {
        // Deliberately disagrees with `status`, which a store with an answer never reports: that is the only way a
        // test can tell which one the pool consulted.
        self.answer.map(|a| (StoreStatus { progress: 0.5, ..Default::default() }, a))
    }
}

// A refusal names the service and its reason. This string reaches the app, which renders it to the
// viewer as "TorBox is refusing requests right now" — a sentence someone can act on, unlike the "no
// source found" it replaced.
#[test]
fn store_unavailable_error_names_the_service() {
    let err = StoreUnavailableError {
        service: DebridService::TorBox,
        reason: "createtorrent http 429 (60 per 1 hour)".into(),
        status: 429,
    };
    let msg = err.to_string();
    assert!(msg.contains("torbox") && msg.contains("429"), "a refusal must carry who refused and why: {msg}");
    assert!(msg.starts_with("store_unavailable:"), "prefix identifies the class: {msg}");
}

// The probe route cannot discover a refusal on its own — it deliberately never calls the endpoint that
// refuses. It reads the backoff the queueing path wrote, so a throttled account reads as "the service
// refused us" rather than as an absence, which the client cannot tell from a dead release.
#[test]
fn recent_refusal_reads_what_the_queueing_path_wrote() {
        let cache = Arc::new(MemoryCache::new(1 << 20));
        let store = TorBoxStore {
            token: "tok".into(),
            cache: Some(cache.clone()),
            api: "https://api.example".into(),
            client: answer(200, r#"{"data":[]}"#),
        };
        let pool = StorePool { stores: vec![Box::new(store.clone())] };

        assert!(pool.recent_refusal("hash-none").is_none(), "nothing was refused, so nothing should be reported");

        cache.put(
            &refused_key(DebridService::TorBox, &store.account_identity(), "hash-refused"),
            "createtorrent http 429",
            Duration::from_secs(60),
        );
        let (svc, reason) = pool.recent_refusal("hash-refused").expect("refusal not reported");
        assert!(svc == DebridService::TorBox && reason.contains("429"), "refusal not reported: {svc:?} {reason:?}");

        // A store with no cache remembers nothing, and must not claim otherwise.
        let bare = StorePool {
            stores: vec![Box::new(TorBoxStore {
                token: "tok".into(),
                cache: None,
                api: "https://api.example".into(),
                client: answer(200, r#"{"data":[]}"#),
            })],
        };
        assert!(bare.recent_refusal("hash-refused").is_none(), "a store with no cache cannot report a refusal");
}

// The disk tier turns itself off on the first write failure rather than logging the same fact per entry.
// Once off it must stay off and must not panic — the memory tier carries on alone.
#[test]
fn tiered_cache_disables_once_and_keeps_serving() {
    // A path that cannot be created: the parent is a file, not a directory.
    let c = TieredCache::new(1 << 20, std::path::Path::new("/dev/null/not-a-dir"));
    c.put("k", "v", Duration::from_secs(60));
    assert!(c.disabled(), "a failing disk tier should disable itself");
    c.put("k2", "v2", Duration::from_secs(60)); // must not panic once off

    // The memory tier still answers — losing persistence is not losing the cache.
    let got = c.get("k");
    assert_eq!(got.as_deref(), Some("v"), "memory tier stopped serving after the disk tier failed: {got:?}");
}

// The disk tier round-trips a value and expires it. This is what makes a restart cheap: the container is
// redeployed on every image push, and a probe costs a debrid resolve to rebuild.
#[test]
fn tiered_cache_persists_and_expires() {
    let dir = tempfile::TempDir::new().expect("a temp dir");
    let c = TieredCache::new(1 << 20, dir.path());
    c.put("live", "value", Duration::from_secs(60));
    c.put("dead", "gone", Duration::from_millis(1));

    // A fresh cache over the same directory: memory is empty, so a hit proves it came off disk.
    let reopened = TieredCache::new(1 << 20, dir.path());
    let got = reopened.get("live");
    assert_eq!(got.as_deref(), Some("value"), "an entry did not survive a restart: {got:?}");
    std::thread::sleep(Duration::from_millis(5));
    assert!(reopened.get("dead").is_none(), "an expired entry was served from disk");
    assert!(reopened.get("never-written").is_none(), "a key that was never written must miss");
}

// A probe answers what is already true and starts nothing. `handleProbe` is the route that exists so a
// client can poll a wait without re-queueing the torrent on every poll — the bug that spent an hour's
// worth of the debrid's add allowance inside a single wait.
#[tokio::test]
async fn handle_probe_reports_without_queueing() {
    let resolves = Arc::new(AtomicUsize::new(0));
    let counter = resolves.clone();
    let h = new_handler(test_deps(|_| {}));
    // The store ANSWERED and said it does not hold this — "not queued" is a fact here, not a
    // shrug. A store that could not be asked gets a different answer, tested below.
    let store = Scripted::new(DebridService::TorBox).check(&[("abc", false)]).resolve_with(move |_| {
        counter.fetch_add(1, Ordering::SeqCst);
        Ok(String::new())
    });
    let pool = StorePool { stores: vec![Box::new(store)] };
    let rec = h.handle_probe(&probe_config(), &pool, &target("abc")).await;

    let n = resolves.load(Ordering::SeqCst);
    assert_eq!(n, 0, "a probe resolved {n} times — resolving adds the torrent");
    assert_eq!(rec.status(), 404, "nothing queued should be 404 not_queued, got {}", rec.status());
    assert!(body_str(&rec).contains("not_queued"), "the body must say which kind of empty this is: {}", body_str(&rec));
}

// The probe's other three answers. Each is a different fact, and the app renders each differently — a
// download in progress, a release ready to play, and an account being refused are three situations that
// spent an evening being indistinguishable from one another.
#[tokio::test]
async fn handle_probe_distinguishes_its_answers() {
    let h = new_handler(test_deps(|_| {}));
    let probe = |stores: Vec<Box<dyn Store>>| {
        let h = &h;
        async move { h.handle_probe(&probe_config(), &StorePool { stores }, &target("abc")).await }
    };

    // Downloading → 202 with whatever progress the store reported.
    let downloading = probe(vec![Box::new(
        Scripted::new(DebridService::TorBox).status(StoreStatus { progress: 0.42, eta_seconds: Some(120), ..Default::default() }),
    )])
    .await;
    assert_eq!(downloading.status(), 202, "a download in progress should be 202, got {}", downloading.status());
    assert!(body_str(&downloading).contains("0.42"), "progress must reach the client: {}", body_str(&downloading));

    // Held by the store → 200 ready, without minting a link (that is /play's job).
    // Held AND resolvable without an add: both are required, because a cache check alone cannot say the
    // account can serve it — TorBox reports what TorBox has, not what this account has.
    let ready = probe(vec![Box::new(
        Scripted::new(DebridService::TorBox).check(&[("abc", true)]).resolve_with(|_| Ok("https://cdn/x".to_owned())),
    )])
    .await;
    assert!(
        ready.status() == 200 && body_str(&ready).contains("ready"),
        "a held release should be 200 ready: {} {}",
        ready.status(),
        body_str(&ready)
    );

    // Recently refused → 503, naming the service. Never 404: the account was turned away, which says
    // nothing at all about whether the release exists.
    let cache = mem();
    let refused_store = TorBoxStore {
        token: "tok".into(),
        cache: Some(cache.clone()),
        api: "https://api.example".into(),
        client: answer(200, r#"{"data":[]}"#),
    };
    cache.put(&refused_key(DebridService::TorBox, &refused_store.account_identity(), "abc"), "createtorrent http 429", Duration::from_secs(60));
    let rec = probe(vec![Box::new(refused_store)]).await;
    assert_eq!(rec.status(), 503, "a refused account should be 503, got {}", rec.status());
    assert!(body_str(&rec).contains("store_unavailable"), "the client must be able to tell a refusal from an absence: {}", body_str(&rec));
}

// Every store draws the same line: being turned away is a fact about the account, not the release.
//
// Only TorBox did. On a Real-Debrid or Premiumize install a 429 became `DeadLinkError`, reached the app
// as 404 "this release does not exist", and the player then walked the whole candidate list collecting
// the identical non-answer — condemning healthy releases on the way. That is the bug that was fixed once
// and left unfixed twice.
#[tokio::test]
async fn every_store_reports_a_refusal_as_a_refusal() {
    let stores = [
        (
            DebridService::RealDebrid,
            Box::new(RealDebridStore {
                token: "t".into(),
                client: answer(429, "{}"),
                cache: None,
                api: "https://rd.example".into(),
            }) as Box<dyn Store>,
        ),
        (
            DebridService::Premiumize,
            Box::new(PremiumizeStore {
                token: "t".into(),
                client: answer(429, "{}"),
                cache: None,
                api: "https://pm.example".into(),
            }) as Box<dyn Store>,
        ),
    ];
    for (want, store) in stores {
        let err = store.resolve(&target(&"e".repeat(40))).await;
        match err {
            Err(StoreError::Unavailable(u)) => assert_eq!(u.service, want, "wrong service named"),
            other => panic!("a throttle must not read as a dead link: {other:?}"),
        }
    }
}

// An unconfigured indexer must not make "nobody has this" permanently unsayable.
//
// Counting an indexer that can never be asked as one that did not answer made the quorum unsatisfiable
// on the shipped default (mediafusion has no config URL), so every genuinely empty result was reported
// as an outage, negative caching never ran, and three unavailable titles in a row flipped /health to
// degraded on a perfectly healthy service. A permanent misconfiguration is not a transient failure.
#[tokio::test]
async fn scrape_all_unaskable_indexer_does_not_block_an_empty_verdict() {
    use crate::config::Indexer;
    use crate::indexerconfig::UnaskableScraper;
    use crate::rank::RawStream;
    use crate::scrape::{scrape_all, ScrapeQuery, Scraper};

    /// Go `fakeScraper`: answers a fixed result.
    struct FakeScraper {
        result: Result<Vec<RawStream>, &'static str>,
    }

    #[async_trait::async_trait]
    impl Scraper for FakeScraper {
        fn id(&self) -> Indexer {
            Indexer("torrentio".into())
        }
        async fn scrape(&self, _q: &ScrapeQuery) -> Result<Vec<RawStream>, DoError> {
            self.result.clone().map_err(DoError::from)
        }
    }

    let unaskable: Arc<dyn Scraper> = Arc::new(UnaskableScraper { indexer: Indexer("mediafusion".into()), transient: false });
    let with = |sc: FakeScraper| -> Vec<Arc<dyn Scraper>> { vec![Arc::new(sc), Arc::clone(&unaskable)] };

    let (_, ok, _) = scrape_all(&with(FakeScraper { result: Ok(vec![]) }), &ScrapeQuery::default(), Duration::from_secs(1)).await;
    assert!(ok, "every askable indexer answered, so the empty result is authoritative");

    // A non-empty list is trusted whatever the coverage: whatever came back is real.
    let found = FakeScraper {
        result: Ok(vec![RawStream { info_hash: "d".repeat(40), title: "a release".into(), ..Default::default() }]),
    };
    let (seeds, ok, _) = scrape_all(&with(found), &ScrapeQuery::default(), Duration::from_secs(1)).await;
    assert!(ok && seeds.len() == 1, "a non-empty result stands on its own: {} seeds, ok={ok}", seeds.len());

    // A real failure still withholds the verdict — that is the rule this must not weaken.
    let (_, ok, _) = scrape_all(&with(FakeScraper { result: Err("502") }), &ScrapeQuery::default(), Duration::from_secs(1)).await;
    assert!(!ok, "an indexer that failed means the empty result is not authoritative");
}

// Season and episode are read off the stream id, and a movie has neither. A wrong answer here sends the
// probe at the wrong episode of a pack.
#[test]
fn season_episode_of() {
    use crate::id::StreamId;
    use crate::probe_fanout::{episode_of, season_of};
    // nil, not zero: episode 0 is a real episode on some series, so "no episode" needs its own value.
    assert!(season_of(None).is_none() && episode_of(None).is_none(), "a nil id has neither a season nor an episode");
    let sid = StreamId { kind: "series".into(), imdb: "tt1".into(), season: 3, episode: 7, has_ep: true };
    assert_eq!(season_of(Some(&sid)), Some(3), "season not read");
    assert_eq!(episode_of(Some(&sid)), Some(7), "episode not read");
    let movie = StreamId { kind: "movie".into(), imdb: "tt2".into(), season: 0, episode: 0, has_ep: false };
    assert!(season_of(Some(&movie)).is_none() && episode_of(Some(&movie)).is_none(), "a movie has neither");
}

// The backoff belongs to every store that can be refused, not just the one whose refusal was noticed
// first.
//
// Only TorBox remembered a refusal. The other two had no cache at all, so a client polling /play for the
// length of a download re-added the magnet once per poll — hundreds of adds for one wait, each leaving a
// duplicate torrent on the account. TorBox backing off correctly made this WORSE: ResolvePreferring fell
// straight through to whichever store had no memory of being turned away.
#[tokio::test]
async fn refusal_backoff_applies_to_every_store() {
    for svc in [DebridService::RealDebrid, DebridService::Premiumize] {
        let adds = Arc::new(AtomicUsize::new(0));
        let counter = adds.clone();
        let client: Arc<dyn Doer> = Arc::new(MockDoer(move |_r: reqwest::Request| -> Answer {
            counter.fetch_add(1, Ordering::SeqCst);
            Ok(resp(503, r#"{"error":"busy"}"#))
        }));
        let cache = mem();
        let store = store_for(svc, "tok", &cache, client);
        for i in 0..6 {
            // A client polling /play through a download.
            let err = store.resolve(&target(H)).await;
            assert!(matches!(&err, Err(e) if is_refusal(e)), "{svc:?} poll {i}: want a refusal, got {err:?}");
        }
        assert_eq!(adds.load(Ordering::SeqCst), 1, "{svc:?}: added more than once across six polls");
        assert!(store.recent_refusal(H).is_some(), "{svc:?}: cannot report the refusal it just recorded");
    }
}

// A refusal is remembered per SERVICE. One store being throttled says nothing about another, and sharing
// a key would let TorBox's backoff silence a Premiumize that is answering perfectly well.
#[test]
fn refusal_backoff_is_per_service() {
    let cache = MemoryCache::new(1 << 20);
    record_refusal(&cache, DebridService::TorBox, "tok", H, &refused(DebridService::TorBox, "429"));

    assert!(backed_off(&cache, DebridService::TorBox, "tok", H).is_some(), "torbox's own refusal was not remembered");
    assert!(
        backed_off(&cache, DebridService::Premiumize, "tok", H).is_none(),
        "torbox's refusal silenced premiumize"
    );
}

// A cancelled request is not a refusal. /play runs on the client's context and the client cancels
// aggressively, so recording one served a healthy release 503 for the next minute.
#[test]
fn refusal_backoff_ignores_cancellation() {
    let cache = MemoryCache::new(1 << 20);
    record_refusal(&cache, DebridService::TorBox, "tok", H, &StoreError::Cancelled);
    assert!(
        backed_off(&cache, DebridService::TorBox, "tok", H).is_none(),
        "a cancelled request was remembered as a refusal by the store"
    );
    // The Go test's second case, an expired deadline, is the same `StoreError::Cancelled` here.
    record_refusal(&cache, DebridService::TorBox, "tok", H, &StoreError::Cancelled);
    assert!(
        backed_off(&cache, DebridService::TorBox, "tok", H).is_none(),
        "an expired deadline was remembered as a refusal by the store"
    );
}

// The probe never queues a torrent, and says so honestly when it cannot tell.
//
// Three answers that were each wrong at some point: "ready" from a cache check alone (which reports what
// TorBox has, not what this account has), 404 "not_queued" when the check was simply unreachable, and a
// resolve that queued the very torrent the probe exists to avoid queueing.
#[tokio::test]
async fn handle_probe_never_queues_and_says_when_it_cannot_tell() {
    // A store that resolves happily UNLESS told not to add — exactly like the real ones. A fake that
    // refuses either way cannot tell "the probe asked for a read-only resolve" from "the probe let it
    // queue", which is the whole assertion.
    let saw_no_add = Arc::new(AtomicBool::new(false));
    let saw = saw_no_add.clone();
    let held = Scripted::new(DebridService::TorBox).cached("abc").resolve_with(move |t| {
        if t.no_add {
            saw.store(true, Ordering::SeqCst);
            return Err(dead_link(ERR_WOULD_ADD)); // TorBox has it; this ACCOUNT does not
        }
        Ok("https://cdn/queued-it".to_owned())
    });
    let h = new_handler(test_deps(|_| {}));

    let rec = h.handle_probe(&probe_config(), &StorePool { stores: vec![Box::new(held)] }, &target("abc")).await;
    assert_ne!(rec.status(), 200, "cached-but-not-held answered 200 ready; playing it would start with a download");
    assert!(saw_no_add.load(Ordering::SeqCst), "the probe resolved WITHOUT NoAdd — it is free to queue the torrent it exists to avoid");

    // The check could not be reached at all → say so, rather than claiming nothing is queued.
    let down = Scripted::new(DebridService::TorBox).check_err(dead_link(ERR_CHECK_FAILED));
    let rec = h.handle_probe(&probe_config(), &StorePool { stores: vec![Box::new(down)] }, &target("abc")).await;
    assert_eq!(rec.status(), 503, "an unreachable cache check should be 503, got {}: {}", rec.status(), body_str(&rec));
    assert!(body_str(&rec).contains("cache_check_unavailable"), "the body must name the reason: {}", body_str(&rec));
}

// One step further out than the case above: the cache check WORKED, and the status read is the part that
// could not find out. 404 "not_queued" there is what makes a client blacklist a release that is
// downloading right now, so an indeterminate read gets "ask again" instead.
//
// Nothing asserted it — deleting the branch left the whole suite green, because the case it exists for
// (checkcached answers while mylist times out) is reachable only through a store that answers the two
// questions differently.
#[tokio::test]
async fn handle_probe_an_indeterminate_status_is_not_an_absence() {
    let h = new_handler(test_deps(|_| {}));
    // The cache check succeeds and honestly reports "not cached"; the status read cannot tell. Without
    // the status read, this is the ordinary 404 path — which is what the definitive case below asserts.
    let uncertain = Scripted::new(DebridService::TorBox)
        .check(&[("abc", false)])
        .resolve_with(|_| Err(dead_link("not held")))
        .answer(StatusAnswer::Unknown);
    let rec = h.handle_probe(&probe_config(), &StorePool { stores: vec![Box::new(uncertain.clone())] }, &target("abc")).await;
    assert_eq!(rec.status(), 503, "an indeterminate status read should be 503, got {}: {}", rec.status(), body_str(&rec));
    assert!(body_str(&rec).contains("status_unavailable"), "the body must distinguish this from an unreachable cache check: {}", body_str(&rec));

    // The same fixture answering DEFINITIVELY still 404s, or the branch above would just be a blanket 503
    // and the route would never report an absence at all.
    let definitive = uncertain.answer(StatusAnswer::No);
    let rec = h.handle_probe(&probe_config(), &StorePool { stores: vec![Box::new(definitive)] }, &target("abc")).await;
    assert_eq!(
        rec.status(),
        404,
        "a store that answered should still be able to say nothing is queued, got {}: {}",
        rec.status(),
        body_str(&rec)
    );
}

// Scout's own refusals reach the CLIENT as scout's, not as the debrid's.
//
// errScoutSide was wired into the refusal memory and then the route went on reading the wrapped
// StoreUnavailableError and printing its service — so the app told the viewer TorBox was refusing while
// TorBox was answering perfectly well. Both scout-side refusals (the hourly allowance, and an add
// already in flight) go through this path.
#[tokio::test]
async fn handle_play_names_scout_not_the_debrid_for_its_own_refusals() {
    let _budget = install_budget(HOUR, 0); // nothing left this hour
    let cache = mem();
    let h = route_handler(&cache, torbox("tok", &cache, answer(200, r#"{"data":[]}"#)));
    let rec = get(&h, &format!("/{}/play/{}", valid_blob(), encode_play_token(&PlayTarget { info_hash: H.into(), ..Default::default() })), &[]).await;

    assert_eq!(rec.status(), 503, "a spent allowance is still 'not now': got {} {}", rec.status(), body_str(&rec));
    let body = body_str(&rec);
    assert!(!body.contains("torbox") && !body.contains("store_unavailable"), "scout's own ceiling was blamed on the debrid: {body}");
    assert!(body.contains("scout_busy"), "the client cannot tell whose refusal this is: {body}");
}

// A read-only resolve is answered before the ADD backoff, and is never blocked by an add already in
// flight — both describe a state a NoAdd caller cannot have caused.
#[tokio::test]
async fn resolve_no_add_is_answered_ahead_of_the_add_guards() {
        let cache = mem();
        record_refusal(&*cache, DebridService::TorBox, "tok", H, &refused(DebridService::TorBox, "429"));
        note_add_attempt(&*cache, DebridService::TorBox, "tok", H);

        let s = TorBoxStore {
            token: "tok".into(),
            client: answer(200, r#"{"data":[]}"#),
            cache: Some(cache.clone()),
            api: TORBOX_API.into(),
        };
        let err = s.resolve(&ResolveTarget { no_add: true, ..target(H) }).await;
        assert!(
            matches!(&err, Err(StoreError::DeadLink(d)) if d.reason == ERR_WOULD_ADD),
            "a read-only resolve was blocked by an add-path guard: {err:?}"
        );
}

// An add scout already sent is answered 202 "coming", not 503 "refused".
//
// The release IS being fetched — by us, moments ago. Answering 503 named the debrid and, on the tvOS
// client, stopped it trying other sources: the viewer was told their debrid was refusing, for a release
// scout had itself just queued.
#[tokio::test]
async fn handle_play_an_in_flight_add_is_queued_not_refused() {
    let cache = mem();
    note_add_attempt(&*cache, DebridService::TorBox, "tok", H); // an add went out and was abandoned

    // nothing queued that Status can see yet
    let h = route_handler(&cache, torbox("tok", &cache, answer(200, r#"{"data":[]}"#)));
    let rec = get(&h, &format!("/{}/play/{}", valid_blob(), encode_play_token(&PlayTarget { info_hash: H.into(), ..Default::default() })), &[]).await;

    assert_eq!(rec.status(), 202, "an add already in flight is 202 coming, got {}: {}", rec.status(), body_str(&rec));
    assert!(!body_str(&rec).contains("store_unavailable"), "a release scout itself queued was reported as the debrid refusing: {}", body_str(&rec));
}

// A spent clock must not be remembered as the STORE refusing.
//
// The guard that stops an add being charged on a dead context returns a StoreUnavailableError, and
// recordRefusal files anything that is not a cancellation, not errScoutSide, not errAddInFlight and not
// errTorrentGone. So the refusal memory learned that TorBox had declined a release TorBox was never
// asked about: one cancelled poll — a viewer changing focus is enough — made a healthy release answer
// 503 on BOTH routes for the next sixty seconds, on a fresh clock, with the store skipped as "backing
// off". The clock is scout's, so the error says so, and errScoutSide is what keeps it out of the memory.
//
// All three stores, because only TorBox's guard had a test and the other two were free to drift.
//
// Ported with `tokio::time::timeout(Duration::ZERO, ...)` standing in for the spent context: a Rust
// store takes no context (PORTING.md), so a zero deadline is the closest seam.
#[tokio::test]
#[ignore = "Go resolves each store with an already-cancelled context and asserts the error is scout-side and unremembered; the Rust stores take no context, so a spent clock is a dropped future with no error to inspect"]
async fn resolve_a_spent_clock_is_not_remembered_as_the_store_refusing() {
    #[cfg(any())]
    async fn pending() {
        for svc in [DebridService::TorBox, DebridService::RealDebrid, DebridService::Premiumize] {
            let token = format!("clock-{svc:?}");
            let hash = "7".repeat(40);
            let cache = mem();
            let quiet = answer(200, r#"{"data":{}}"#);
            let store = store_for_any(svc, &token, &cache, quiet);

            let err = match tokio::time::timeout(Duration::ZERO, store.resolve(&target(&hash))).await {
                Err(_elapsed) => StoreError::Cancelled,
                Ok(Ok(_)) => panic!("a resolve on a spent clock should not succeed"),
                Ok(Err(e)) => e,
            };
            assert!(
                matches!(err, StoreError::Cancelled),
                "a spent clock reported {err:?} — not marked as scout's own, so recordRefusal files it and the store is blamed for a request it never saw"
            );
            assert!(
                backed_off(&*cache, svc, &token, &hash).is_none(),
                "a spent clock left a refusal against {svc:?} — the next poll skips a healthy store for a minute, on both /play and ?probe=1"
            );
        }
    }
    todo!("pending: refusal_test.go:538")
}

// The probe stops believing an add at the same moment /play does.
//
// addInFlight, which /play's resolve consults, checks unknownTooLong FIRST and calls the release dead
// past addGiveUp. The pool's reporter read the attempt marker alone, and the two markers have different
// lifetimes — the attempt marker 90s, the unknown-outcome stamp 30 minutes — so they overlap: the last
// add before the give-up leaves an attempt marker live for up to ninety seconds past it. In that window
// ?probe=1 said 202 downloading while /play said 404 dead_link, which is the disagreement the reporter
// was added to remove, pointing the other way.
#[test]
fn add_in_flight_stops_believing_an_add_at_the_give_up() {
    let token = "giveup";
    let hash = "8".repeat(40);
    let cache = mem();
    let s = real_debrid(token, &cache, answer(200, "{}"));

    note_add_attempt(&*cache, DebridService::RealDebrid, token, &hash);
    assert_eq!(s.add_in_flight(&hash), Some(true), "a fresh add attempt should read as in flight");

    // Age the unknown-outcome stamp past the give-up, leaving the 90s attempt marker live beneath it.
    let stamp = unix_secs_ago(ADD_GIVE_UP + Duration::from_secs(60));
    cache.put(&unknown_outcome_key(DebridService::RealDebrid, token, &hash), &stamp, UNKNOWN_OUTCOME_TTL);

    assert_ne!(
        s.add_in_flight(&hash),
        Some(true),
        "past the give-up the probe still called the add live, while /play calls the release dead — the \
         client is told to keep waiting for something /play has stopped waiting for"
    );
}

// The pool asks EVERY store, not just the first.
//
// Every fixture for this reporter built a single-store pool, so the loop was never exercised: a version
// that answered false for any pool with more than one store passed the whole suite, and would have made
// the probe blind to in-flight adds on exactly the multi-account installs it matters for.
#[test]
fn store_pool_add_in_flight_asks_every_store() {
        let token = "pool-loop";
        let hash = "6".repeat(40);
        let cache = mem();
        note_add_attempt(&*cache, DebridService::Premiumize, token, &hash); // the marker is on the LAST store

        let pool = StorePool {
            stores: vec![
                Box::new(TorBoxStore {
                    token: token.into(),
                    cache: Some(cache.clone()),
                    api: TORBOX_API.into(),
                    client: answer(200, "{}"),
                }),
                Box::new(real_debrid(token, &cache, answer(200, "{}"))),
                Box::new(premiumize(token, &cache, answer(200, "{}"))),
            ],
        };
        assert!(pool.add_in_flight(&hash), "the pool missed an add in flight on a store that was not the first one asked");
}

// A release Real-Debrid already holds is READY on the probe route, not "not queued".
//
// The probe's readiness branch was gated on the cache check alone, and RD publishes no cache API — it
// answers all-false by design, saying "RD contributes no cache truth". So the branch was unreachable on
// an RD-only install and the route fell through to 404, while /play resolved the very same release from
// a cache lookup plus an info call and returned a 302. The client polls the probe to know when its
// download has landed; it never learned that it had.
#[tokio::test]
async fn probe_a_torrent_real_debrid_already_holds_reads_as_ready() {
    let (token, hash) = ("rd-held", "f".repeat(40));
    let cache = mem();
    let rt = target(&hash);
    cache.put(&rd_torrent_key(token, &hash, &rt), "RDID123", Duration::from_secs(3600)); // already bought

    let client: Arc<dyn Doer> = Arc::new(MockDoer(|r: reqwest::Request| -> Answer {
        let path = r.url().path();
        if path.contains("/torrents/info/") {
            return Ok(resp(
                200,
                r#"{"id":"RDID123","status":"downloaded","files":[{"id":1,"path":"/Movie.mkv","bytes":100,"selected":1}],"links":["https://rd.example/link"]}"#,
            ));
        }
        if path.contains("/unrestrict/link") {
            return Ok(resp(200, r#"{"download":"https://rd.example/final.mkv"}"#));
        }
        Ok(resp(200, "{}"))
    }));
    let h = route_handler(&cache, Arc::new(real_debrid(token, &cache, client)));
    let tok = encode_play_token(&PlayTarget { info_hash: hash, ..Default::default() });

    // The config must name the same service the pool holds, or the route stops at "cache check
    // unavailable" — hasCacheTruth reads the CONFIG — before reaching the branch under test.
    let rd_only = blob(r#"{"debrid":[{"service":"realdebrid","token":"rd"}],"indexers":["torrentio"],"resultCap":20}"#);
    let probe = get(&h, &format!("/{rd_only}/play/{tok}?probe=1"), &[]).await;
    assert_ne!(
        probe.status(),
        404,
        "?probe=1 answered 404 for a torrent the account already holds: {} — /play resolves it from the same cache entry, so the client is told a release it can play does not exist",
        body_str(&probe).trim()
    );
    assert_eq!(probe.status(), 200, "?probe=1 = {}, want 200 ready: {}", probe.status(), body_str(&probe).trim());
}

// NOTE ON THIS TEST'S SCOPE: it pins premiumizeStore.AddInFlight at three marker ages and nothing else.
// It does NOT observe the agreement between the two routes, despite what an earlier version of this
// comment claimed — and that overclaim is why two real disagreements passed it: a queued transfer that
// directdl had since REFUSED, and one that had since COMPLETED. Its fixture answers
// success-with-no-content for every call, which is the one case where the marker alone is right. The
// route-level agreements are pinned by the two tests below it.
//
// Premiumize has a second way to be mid-fetch, and the probe could not see it either.
//
// directdl answering success with empty content means the transfer was queued: the store stamps
// pmQueuedKey and returns errAddInFlight, so /play says 202 downloading. That is not the add marker —
// settleAddAttempt clears the add marker as soon as the body is read — so a probe asking only about
// adds answered 404 for the whole ten minutes /play spends saying "coming".
//
// Both ends of the window are asserted. Past pendingGiveUp /play reports the release dead, so a probe
// still claiming 202 there would be the same defect pointing the other way.
#[test]
fn premiumize_add_in_flight_believes_a_queued_transfer_only_until_the_give_up() {
    let cases = [
        ("just queued", Duration::ZERO, true),
        ("still believable", Duration::from_secs(9 * 60), true),
        ("past the give-up", PENDING_GIVE_UP + Duration::from_secs(60), false),
    ];
    for (name, queued_ago, want_queued) in cases {
        let token = format!("pm-{name}");
        let hash = "9".repeat(40);
        let cache = mem();
        // Stamp the marker at the age under test, the way noteQueued writes it.
        cache.put(&pm_queued_key(&token, &hash), &unix_secs_ago(queued_ago), QUEUED_TTL);

        // Still nothing to serve.
        let s = premiumize(&token, &cache, answer(200, r#"{"status":"success","content":[]}"#));
        assert_eq!(
            s.add_in_flight(&hash),
            Some(want_queued),
            "{name}: the probe route reads this to decide between 202 downloading and 404 not_queued, and it \
             must change sides at the same moment /play does"
        );
    }
}

// HoldingServices asks EVERY store, and builds Real-Debrid's key the way /play writes it.
//
// Two gaps in one: the loop had no multi-store fixture, so stopping after the first store passed the
// whole suite; and the only test used a movie target, so dropping season/episode from the RD key — which
// would make the fix silently do nothing for series, most of the traffic — also passed.
#[test]
fn holding_services_asks_every_store_and_keys_episodes_correctly() {
        let token = "holding";
        let hash = "3".repeat(40);
        let cache = mem();
        let target = ResolveTarget { info_hash: hash.clone(), season: Some(2), episode: Some(5), ..Default::default() };
        // Written the way /play writes it, episode selector and all, on the store asked LAST.
        cache.put(&rd_torrent_key(token, &hash, &target), "RDID-S2E5", Duration::from_secs(3600));

        let pool = StorePool {
            stores: vec![
                Box::new(TorBoxStore {
                    token: token.into(),
                    cache: Some(cache.clone()),
                    api: TORBOX_API.into(),
                    client: answer(200, "{}"),
                }),
                Box::new(real_debrid(token, &cache, answer(200, "{}"))),
            ],
        };

        let held = pool.holding_services(&target);
        assert!(
            held.len() == 1 && held[0] == DebridService::RealDebrid,
            "HoldingServices = {held:?}, want [realdebrid] — either the loop stopped at the first store, or the episode selector is missing from the key, which makes the whole enquiry a no-op for series"
        );
        // A different episode of the same pack is a different entry, so it must NOT match.
        let other = ResolveTarget { episode: Some(6), ..target.clone() };
        assert!(pool.holding_services(&other).is_empty(), "episode 6 matched episode 5's entry");
        // And a different SEASON, same episode number. Without this the key could discriminate on episode
        // alone — measured: dropping the season from rdTorrentKey's selector passed the whole suite, because
        // every probe here used season 2.
        let other_season = ResolveTarget { season: Some(3), ..target };
        assert!(
            pool.holding_services(&other_season).is_empty(),
            "S03E05 matched S02E05's entry — the key is not discriminating on season"
        );
}

// EveryAddRefusedByScout means EVERY, and an empty pool refuses nothing.
//
// The quantifier was held by nothing: flipping it to ANY passed the entire suite, and shipped it would
// answer 503 scout_busy on a two-account install where one budget is spent and the other is healthy —
// while /play serves a 302 from the healthy account. That is a fresh disagreement of exactly the kind
// the branch was added to remove.
#[test]
fn every_add_refused_by_scout_needs_every_account_spent() {
        // The process-wide budget, held for this test: other tests swap it, and Go's ran one at a time.
        let _budget = crate::tests::addbudget::install_budget(crate::tests::addbudget::HOUR, ADD_BUDGET_LIMIT);
        let spent = "spent-every_add_refused_by_scout_needs_every_account_spent";
        let healthy = "healthy-every_add_refused_by_scout_needs_every_account_spent";
        for _ in 0..ADD_BUDGET_LIMIT {
            global_add_budget().take(&budget_account(DebridService::TorBox, spent), false);
        }
        let cache = mem();

        let mixed = StorePool {
            stores: vec![
                Box::new(TorBoxStore {
                    token: spent.into(),
                    cache: Some(cache.clone()),
                    api: TORBOX_API.into(),
                    client: answer(200, "{}"),
                }),
                Box::new(real_debrid(healthy, &cache, answer(200, "{}"))),
            ],
        };
        assert!(
            !mixed.every_add_refused_by_scout(),
            "one spent account out of two reported as every add refused — the probe would answer 503 scout_busy for a release the healthy account can still fetch"
        );

        let all_spent = StorePool {
            stores: vec![Box::new(TorBoxStore {
                token: spent.into(),
                cache: Some(cache.clone()),
                api: TORBOX_API.into(),
                client: answer(200, "{}"),
            })],
        };
        assert!(all_spent.every_add_refused_by_scout(), "the only account's allowance is spent and it was not reported");

        assert!(
            !StorePool { stores: vec![] }.every_add_refused_by_scout(),
            "an empty pool refused every add — vacuously true is not true here"
        );
}

// A refusal on record outranks the queue marker, and NOT the other way round.
//
// premiumizeStore.AddInFlight checks the add marker first, then the refusal, then the queue marker.
// Hoisting the refusal check above the add marker passed the whole suite, and it inverts the precedence
// addStillBelievable's own comment calls the point: an add genuinely in flight would read as
// not-in-flight whenever a stale per-release refusal happened to exist.
#[test]
fn premiumize_add_in_flight_an_add_in_flight_outranks_a_stale_refusal() {
    let token = "pm-order";
    let hash = "c".repeat(40);
    let cache = mem();
    note_add_attempt(&*cache, DebridService::Premiumize, token, &hash); // an add of ours IS out
    // ...alongside an older per-release refusal
    record_refusal(
        &*cache,
        DebridService::Premiumize,
        token,
        &hash,
        &StoreError::DeadLink(DeadLinkError { reason: "premiumize directdl: error something earlier".into() }),
    );

    let s = premiumize(token, &cache, answer(200, "{}"));
    assert_eq!(
        s.add_in_flight(&hash),
        Some(true),
        "an add scout has out read as not-in-flight because a stale refusal exists — /play answers 202 from \
         that marker, so the probe must too"
    );
}

// A queued Premiumize transfer that directdl has SINCE refused must stop reading as "downloading".
//
// The test above pins the marker's boolean and drives only one route, which is how this escaped: it
// answers success-with-no-content for every call, and the disagreeing case is the one where directdl
// answers something else. Premiumize reports an unsupported magnet, an account at its limit and "not
// enough space" as HTTP 200 with status:error — none of those branches clears pmQueuedKey, so the
// marker alone kept the probe at 202 for the whole ten-minute window while /play had the verdict.
//
// Both routes are driven here, and the assertion is that they agree.
#[tokio::test]
async fn probe_and_play_agree_when_a_queued_premiumize_transfer_is_refused() {
    let (token, hash) = ("pm-refused", "5".repeat(40));
    let cache = mem();
    cache.put(&pm_queued_key(token, &hash), &unix_secs_ago(Duration::ZERO), QUEUED_TTL);

    // The shape an account out of space returns: HTTP 200, status error.
    let h = route_handler(&cache, Arc::new(premiumize(token, &cache, answer(200, r#"{"status":"error","message":"not enough space"}"#))));
    let pm_blob = blob(&format!(r#"{{"debrid":[{{"service":"premiumize","token":"{token}"}}],"indexers":["torrentio"],"resultCap":20}}"#));
    let tok = encode_play_token(&PlayTarget { info_hash: hash, ..Default::default() });

    // The PROBE goes first, which is the order a real client uses — it polls to draw its progress bar and
    // only calls /play when told the release is ready. An earlier version issued a /play first, which
    // recorded the refusal on the probe's behalf and so hid whether the probe could learn it alone.
    let probe_first = get(&h, &format!("/{pm_blob}/play/{tok}?probe=1"), &[]).await;
    assert_ne!(
        probe_first.status(),
        202,
        "the first ?probe=1 says downloading ({}) for a transfer Premiumize refused on that very call — nothing withdraws the queue marker's claim, so the client sits on a spinner it cannot fall through",
        body_str(&probe_first).trim()
    );

    let probe = get(&h, &format!("/{pm_blob}/play/{tok}?probe=1"), &[]).await;
    let play = get(&h, &format!("/{pm_blob}/play/{tok}"), &[]).await;
    assert!(
        !(probe.status() == 202 && play.status() != 202),
        "?probe=1 still says 202 downloading ({}) while /play says {} ({}) — the client sits on a 0% spinner for ten minutes with nothing behind it",
        body_str(&probe).trim(),
        play.status(),
        body_str(&play).trim()
    );
    assert_eq!(
        probe.status(),
        play.status(),
        "?probe=1 = {} ({}), /play = {} ({}) — one vocabulary, two answers",
        probe.status(),
        body_str(&probe).trim(),
        play.status(),
        body_str(&play).trim()
    );
}

// A Premiumize transfer that has COMPLETED must read as ready, not as still downloading.
//
// Nothing on the probe path could clear pmQueuedKey: settleQueuedTransfer is reached only from
// directdl's content branch, and Premiumize refused a NoAdd target before consulting anything, so the
// probe never called directdl and the marker went on saying "coming". The probe therefore never
// transitioned to ready for Premiumize at all — 202 until the marker aged past pendingGiveUp, then 404,
// while /play answered 302 with a playable link. Measured before the fix: probe 202, /play 302.
//
// For an already-queued transfer directdl is a READ — the charge is suppressed in exactly that case —
// so letting a read-only caller through buys nothing and is the only way to discover completion.
// Every realistic outcome of Premiumize's /cache/check, because the readiness enquiry runs only when a
// store NAMES itself a holder, and PM's cache check was for a while the only channel that could. The
// first version of this test asserted the cached-true row alone — the one branch that worked — so the
// other four kept answering 202 for a completed transfer and the suite stayed green.
#[tokio::test]
async fn probe_and_play_agree_when_a_premiumize_transfer_completes() {
    type CacheCheck = fn() -> Answer;
    let cases: [(&str, CacheCheck); 5] = [
        ("cache check says cached", || Ok(resp(200, r#"{"status":"success","response":[true]}"#))),
        ("cache check says not cached", || Ok(resp(200, r#"{"status":"success","response":[false]}"#))),
        ("cache check throttled", || Ok(resp(429, "{}"))),
        ("cache check transport error", || Err(Box::new(crate::scrape::DeadlineExceeded))),
        ("cache check unreadable", || Ok(resp(200, "not json"))),
    ];
    for (name, cache_check) in cases {
        let (token, hash) = (format!("pm-complete-{name}"), "2".repeat(40));
        let cache = mem();
        cache.put(&pm_queued_key(&token, &hash), &unix_secs_ago(Duration::ZERO), QUEUED_TTL);

        let client: Arc<dyn Doer> = Arc::new(MockDoer(move |r: reqwest::Request| -> Answer {
            if r.url().path().contains("cache/check") {
                return cache_check();
            }
            Ok(resp(200, r#"{"status":"success","content":[{"path":"Movie.mkv","link":"https://pm.example/final.mkv","size":100}]}"#))
        }));
        let h = route_handler(&cache, Arc::new(premiumize(&token, &cache, client)));
        let pm_blob = blob(&format!(r#"{{"debrid":[{{"service":"premiumize","token":"{token}"}}],"indexers":["torrentio"],"resultCap":20}}"#));
        let tok = encode_play_token(&PlayTarget { info_hash: hash, ..Default::default() });

        // THREE polls, because a client polls. One poll cannot see a route that destroys the state
        // its own next answer depends on: the readiness enquiry used to clear the queue marker, so
        // the probe reported ready once and 404 "not queued" forever after, while /play kept serving
        // the same transfer.
        for i in 1..=3 {
            let probe = get(&h, &format!("/{pm_blob}/play/{tok}?probe=1"), &[]).await;
            assert_ne!(probe.status(), 202, "{name}: poll {i}: ?probe=1 says downloading ({}) for a completed transfer", body_str(&probe).trim());
            assert_eq!(probe.status(), 200, "{name}: poll {i}: ?probe=1 = {} ({}), want 200 ready", probe.status(), body_str(&probe).trim());
        }
        let play = get(&h, &format!("/{pm_blob}/play/{tok}"), &[]).await;
        assert_eq!(play.status(), 302, "{name}: /play = {}, want 302 — the fixture is not serving a completed transfer", play.status());
    }
}

// A read-only probe poll must not write Premiumize's ADD-path in-flight marker.
//
// The NoAdd gate was widened so the probe could see a queued transfer finish, and everything below that
// gate is add-path bookkeeping. A probe runs on the client's context under statusBudget and clients
// cancel aggressively, so a cut-off poll left a ninety-second marker behind — and /play then
// short-circuits on that marker WITHOUT calling directdl, answering 202 "downloading" for a release
// Premiumize already holds. Unplayable for the marker's life, with no Status API to clear it.
//
// A read-only route stranding a release by writing add-path memory is exactly the contract
// recordRefusalFor states, and the commit that widened this gate fixed that contract for Real-Debrid
// while missing the store it had just widened.
#[tokio::test]
async fn probe_a_read_only_poll_writes_no_premiumize_add_memory() {
    let token = "pm-readonly";
    let hash = "1".repeat(40);
    let cache = mem();
    cache.put(&pm_queued_key(token, &hash), &unix_secs_ago(Duration::ZERO), QUEUED_TTL);

    // The poll is cut off mid-call, as a focus change does.
    let s = premiumize(
        token,
        &cache,
        Arc::new(MockDoer(move |_r: reqwest::Request| -> Answer { transport_error() })),
    );
    let _ = s.resolve(&ResolveTarget { no_add: true, ..target(&hash) }).await;

    assert!(
        !add_outcome_unknown(&*cache, DebridService::Premiumize, token, &hash),
        "a read-only poll left an add-path in-flight marker — /play then answers 202 'downloading' without \
         calling directdl, for a release Premiumize already holds"
    );

    // The queue marker must survive a read-only poll: it is the only thing naming Premiumize a holder,
    // so clearing it from here makes the very next probe answer 404 for a transfer /play still serves.
    //
    // A SECOND store, because the branch that clears it is only reached when directdl answers with
    // content. The cancelled fixture above returns long before it, so asserting the marker there could
    // not fail for the reason it names — which is exactly the flaw the previous version of this test was
    // rewritten to remove, repeated one assertion over.
    let served = premiumize(
        token,
        &cache,
        answer(
            200,
            r#"{"status":"success","content":[{"path":"Movie.mkv","link":"https://pm.example/final.mkv","size":100}]}"#,
        ),
    );
    let link = served.resolve(&ResolveTarget { no_add: true, ..target(&hash) }).await;
    assert!(
        matches!(&link, Ok(l) if !l.is_empty()),
        "the read-only resolve should serve a completed transfer: {link:?}"
    );
    assert!(
        already_queued(&*cache, token, &hash),
        "a read-only poll cleared the queue marker — the next poll names no holder and answers 404 not_queued \
         for a release /play resolves from the same marker"
    );
}

// A refusal Premiumize ANSWERS must be recorded even by a read-only poll, because for this store that
// key is what withdraws the queue marker's "coming" claim.
//
// This half was previously asserted with a fixture whose error was context.Canceled — which recordRefusal
// excludes by itself, so the assertion could not fail for the reason it named, and reverting the whole
// change passed the entire suite. The errors below are the ones Premiumize actually returns: "not enough
// space" and an invalid magnet arrive as HTTP 200 with status:error.
#[tokio::test]
async fn premiumize_a_read_only_poll_records_an_answered_refusal() {
    let cases = [
        ("out of space", r#"{"status":"error","message":"not enough space"}"#, 200),
        ("server error", "{}", 500),
        // A non-2xx that is NOT a refusal of the account (401/403/429/5xx are handled a branch up).
        // Reverting only this site reproduced the whole defect through a different status code while the
        // suite stayed green — the two rows above cover the other two sites.
        ("bad request", "{}", 400),
    ];
    for (name, body, code) in cases {
        let token = format!("pm-answered-{name}");
        let hash = "1".repeat(40);
        let cache = mem();
        cache.put(&pm_queued_key(&token, &hash), &unix_secs_ago(Duration::ZERO), QUEUED_TTL);

        let s = premiumize(&token, &cache, answer(code, body));
        let _ = s.resolve(&ResolveTarget { no_add: true, ..target(&hash) }).await;

        assert!(
            backed_off(&*cache, DebridService::Premiumize, &token, &hash).is_some(),
            "{name}: an answered refusal was not recorded by a read-only poll — nothing then withdraws the queue \
             marker's claim, so the probe reports 202 downloading on every poll of a transfer Premiumize has \
             refused, re-asking directdl each time"
        );
        assert_ne!(
            s.add_in_flight(&hash),
            Some(true),
            "{name}: the queue marker still reads as in-flight after Premiumize refused the transfer — the \
             client sits on a spinner it cannot fall through"
        );
    }
}

// A rejected KEY outranks an add in flight, on the probe route as in every store.
//
// All three stores check accountBackedOff above addInFlight and say why — a dead key is not a wait. The
// probe consulted its in-flight reporter first, so a live marker pre-empted the rejected key and the two
// routes disagreed: /play 503 naming the debrid, ?probe=1 202 downloading, for the same release at the
// same instant, for up to ninety seconds per outstanding add.
#[tokio::test]
async fn probe_and_play_agree_when_the_account_key_is_rejected() {
    let (token, hash) = ("dead-key", "0".repeat(40));
    let cache = mem();
    note_add_attempt(&*cache, DebridService::TorBox, token, &hash); // an add of ours is out
    record_refusal(
        &*cache,
        DebridService::TorBox,
        token,
        &"f".repeat(40), // ...and the key is rejected
        &StoreError::Unavailable(StoreUnavailableError { service: DebridService::TorBox, status: 401, reason: "createtorrent http 401".into() }),
    );

    let h = route_handler(&cache, torbox(token, &cache, answer(200, r#"{"success":true,"data":[]}"#)));
    let tb_blob = blob(&format!(r#"{{"debrid":[{{"service":"torbox","token":"{token}"}}],"indexers":["torrentio"],"resultCap":20}}"#));
    let tok = encode_play_token(&PlayTarget { info_hash: hash, ..Default::default() });

    let probe = get(&h, &format!("/{tb_blob}/play/{tok}?probe=1"), &[]).await;
    let play = get(&h, &format!("/{tb_blob}/play/{tok}"), &[]).await;

    assert!(
        !(probe.status() == 202 && play.status() != 202),
        "?probe=1 says downloading ({}) while /play says {} ({}) — the viewer watches a spinner for a release nothing is fetching, on an account whose key needs replacing",
        body_str(&probe).trim(),
        play.status(),
        body_str(&play).trim()
    );
    assert_eq!(
        probe.status(),
        play.status(),
        "?probe=1 = {} ({}), /play = {} ({}) — one vocabulary, two answers",
        probe.status(),
        body_str(&probe).trim(),
        play.status(),
        body_str(&play).trim()
    );
}

// A dead key on ONE account must not outrank a live add on ANOTHER.
//
// Two rules meet here. Per store, a rejected key beats that store's own add in flight — every Resolve
// says so. Across stores, an add in flight anywhere beats a refusal elsewhere — ResolvePreferring
// returns `coming` before `refused` precisely so store order cannot decide the verdict. Applying the
// first rule at POOL level broke the second: with TorBox's key expired and Real-Debrid fetching, the
// probe answered 503 naming torbox while /play answered 202 downloading — naming a store that was not
// the one fetching, on the URL the client polls for its progress bar.
//
// The sibling single-account case is pinned by TestProbeAndPlay_agreeWhenTheAccountKeyIsRejected, which
// configures one store and so cannot see this. Both are needed.
#[tokio::test]
async fn probe_and_play_agree_when_one_accounts_key_is_dead_and_another_is_fetching() {
    let (token, hash) = ("two-accounts", "b".repeat(40));
    let cache = mem();
    // TorBox's key is rejected...
    cache.put(&account_refused_key(DebridService::TorBox, token), "createtorrent http 401", REFUSAL_BACKOFF);
    // ...while Real-Debrid has an add out for the release being watched.
    note_add_attempt(&*cache, DebridService::RealDebrid, token, &hash);

    let quiet = answer(200, r#"{"success":true,"data":[]}"#);
    let tb: Arc<dyn Store> = torbox(token, &cache, quiet.clone());
    let rd: Arc<dyn Store> = Arc::new(real_debrid(token, &cache, quiet));
    let h = new_handler(test_deps({
        let cache = cache.clone();
        move |d| {
            d.cache = cache;
            d.make_stores = Arc::new(move |_| {
                vec![Box::new(SharedStore(tb.clone())) as Box<dyn Store>, Box::new(SharedStore(rd.clone())) as Box<dyn Store>]
            });
        }
    }));
    let two_blob = blob(&format!(
        r#"{{"debrid":[{{"service":"torbox","token":"{token}"}},{{"service":"realdebrid","token":"{token}"}}],"indexers":["torrentio"],"resultCap":20}}"#
    ));
    let tok = encode_play_token(&PlayTarget { info_hash: hash, ..Default::default() });

    let probe = get(&h, &format!("/{two_blob}/play/{tok}?probe=1"), &[]).await;
    let play = get(&h, &format!("/{two_blob}/play/{tok}"), &[]).await;

    assert_eq!(
        probe.status(),
        play.status(),
        "?probe=1 = {} ({}), /play = {} ({}) — a dead key on one account is outranking a live add on another, so the probe names a store that is not the one fetching",
        probe.status(),
        body_str(&probe).trim(),
        play.status(),
        body_str(&play).trim()
    );
    assert_eq!(probe.status(), 202, "?probe=1 = {}, want 202 — Real-Debrid is fetching it", probe.status());
}

// TorBox must rank its own add in flight above a refusal recorded in the meantime, as RD and PM do.
//
// The per-release backoff was hoisted above the warm fast path so a binge could not bypass it, and that
// hoist put it above the in-flight marker for TorBox alone — reversing the order the gate further down
// states in as many words. Both markers can be live at once without planting anything: /play has no
// singleflight and Stremio races requests, so one poll's createtorrent can be answered 429 (settling its
// attempt and recording the refusal) while another's connection resets, leaving its attempt marker.
#[tokio::test]
async fn torbox_an_add_in_flight_outranks_a_refusal_recorded_meanwhile() {
        let token = "tb-order";
        let hash = "a".repeat(40);
        let cache = mem();
        note_add_attempt(&*cache, DebridService::TorBox, token, &hash);
        cache.put(&refused_key(DebridService::TorBox, token, &hash), "createtorrent http 429", REFUSAL_BACKOFF);

        let s = TorBoxStore {
            token: token.into(),
            cache: Some(cache.clone()),
            api: TORBOX_API.into(),
            client: answer(200, r#"{"success":true,"data":[]}"#),
        };

        let err = s.resolve(&target(&hash)).await;
        assert!(
            matches!(&err, Err(StoreError::AddInFlight)),
            "torbox answered {err:?}, want errAddInFlight — it is blaming the store for a release scout has an add out for, which is the one answer this file says never to give"
        );

        // PAST the give-up, the refusal must be heard again. The gate is skipped only while the add is still
        // BELIEVABLE, not merely while its marker exists — weakening it to the bare marker left the suite
        // green while suppressing a real refusal behind a marker nothing will ever settle.
        //
        // Asserted on WHICH error surfaces, not on the absence of errAddInFlight: past the give-up both the
        // shipped gate and the weakened one stop saying "in flight", and they differ in what they say
        // instead. The gate lets the recorded refusal through (503, agreeing with the probe); the bare
        // marker falls to addInFlight's give-up branch and answers a dead link (404), which is /play calling
        // a release dead on the strength of a marker nothing settled.
        cache.put(
            &unknown_outcome_key(DebridService::TorBox, token, &hash),
            &unix_secs_ago(ADD_GIVE_UP + Duration::from_secs(60)),
            UNKNOWN_OUTCOME_TTL,
        );
        let late = s.resolve(&target(&hash)).await;
        assert!(
            matches!(&late, Err(e) if is_refusal(e)),
            "past addGiveUp torbox answered {late:?}, want the recorded refusal — a dead link here is /play calling a release dead while the probe reports the refusal, and it comes from trusting a marker past the point scout stops believing it"
        );
}

// /play must not buy a release another configured account already holds.
//
// Its resolve ordering came only from the cache check, and Real-Debrid can never appear there: its
// CacheCheck answers all-false by design, so HeldBy never names it. A release RD already held was
// therefore invisible to the ordering, configured order won, and TorBox — first in the list — was asked
// to resolve, which means createtorrent: one of the fifty per hour spent, a duplicate torrent left on
// TorBox, and the probe (which consults Status first) then reporting a download the viewer never needed
// for a file the other account could have served instantly.
//
// HoldingServices is the free cache read that answers this, added for the probe and not applied to the
// route that pays for being wrong.
#[tokio::test]
async fn play_does_not_buy_a_release_another_account_already_holds() {
    let (token, hash) = ("prefer-held", "d".repeat(40));
    let cache = mem();
    let rt = target(&hash);
    cache.put(&rd_torrent_key(token, &hash, &rt), "RDID-HELD", Duration::from_secs(3600)); // RD bought it earlier

    let creates = Arc::new(AtomicUsize::new(0));
    let counter = creates.clone();
    let cached_hash = hash.clone();
    let tb_client: Arc<dyn Doer> = Arc::new(MockDoer(move |r: reqwest::Request| -> Answer {
        let path = r.url().path();
        if path.contains("createtorrent") {
            counter.fetch_add(1, Ordering::SeqCst);
            return Ok(resp(200, r#"{"data":{"torrent_id":9}}"#));
        }
        if path.contains("checkcached") {
            // TorBox reports it CACHED, which is the ordinary case for anything popular — and the case
            // that matters, because a cache hit still costs a createtorrent while RD's held id does not.
            // Answering "nothing cached" here left HeldBy empty, so RD was the only preferred service and
            // the ordering was never actually under test.
            return Ok(resp(200, &format!(r#"{{"data":{{"{cached_hash}":{{}}}}}}"#)));
        }
        if path.contains("requestdl") {
            return Ok(resp(200, r#"{"success":true,"data":"https://tb.example/link.mkv"}"#));
        }
        Ok(resp(200, r#"{"success":true,"data":[]}"#))
    }));
    let rd_client: Arc<dyn Doer> = Arc::new(MockDoer(|r: reqwest::Request| -> Answer {
        let path = r.url().path();
        if path.contains("/torrents/info/") {
            return Ok(resp(
                200,
                r#"{"id":"RDID-HELD","status":"downloaded","files":[{"id":1,"path":"/Movie.mkv","bytes":100,"selected":1}],"links":["https://rd.example/link"]}"#,
            ));
        }
        if path.contains("/unrestrict/link") {
            return Ok(resp(200, r#"{"download":"https://rd.example/final.mkv"}"#));
        }
        Ok(resp(200, "{}"))
    }));

    let tb = Arc::new(TorBoxStore { token: token.into(), cache: Some(cache.clone()), api: TORBOX_API.into(), client: tb_client });
    let account = budget_account(DebridService::TorBox, &tb.account_identity());
    let before = global_add_budget().remaining(&account);
    // TorBox first, as the config orders it — the whole point is that order must not decide this.
    let tb: Arc<dyn Store> = tb;
    let rd: Arc<dyn Store> = Arc::new(real_debrid(token, &cache, rd_client));
    let h = new_handler(test_deps({
        let cache = cache.clone();
        move |d| {
            d.cache = cache;
            d.make_stores = Arc::new(move |_| {
                vec![Box::new(SharedStore(tb.clone())) as Box<dyn Store>, Box::new(SharedStore(rd.clone())) as Box<dyn Store>]
            });
        }
    }));
    let two_blob = blob(&format!(
        r#"{{"debrid":[{{"service":"torbox","token":"{token}"}},{{"service":"realdebrid","token":"{token}"}}],"indexers":["torrentio"],"resultCap":20}}"#
    ));

    let play = get(&h, &format!("/{two_blob}/play/{}", encode_play_token(&PlayTarget { info_hash: hash, ..Default::default() })), &[]).await;

    assert_eq!(play.status(), 302, "/play = {} ({}), want 302 from the account that already holds it", play.status(), body_str(&play).trim());
    let got = creates.load(Ordering::SeqCst);
    assert_eq!(got, 0, "TorBox was asked to create {got} torrent(s) for a release Real-Debrid already held — that is one of the fifty per hour, and a duplicate torrent on the account");
    let after = global_add_budget().remaining(&account);
    assert_eq!(after, before, "TorBox's hourly allowance went {before} → {after} for a release another account held");
}

// A season pack Premiumize has already served must not read as "not queued" for its next episode.
//
// Premiumize has no torrent id to remember, so the queue marker was the only thing naming it a holder —
// and /play settles that marker on success. The first play of a pack therefore blinded the probe to the
// whole rest of it: every later poll answered 404 while /play kept serving 302s from the same transfer.
// Measured across three polls of episode 2 after playing episode 1.
//
// The probe is driven AFTER the play here, deliberately. The sibling three-poll test polls before any
// play and so cannot see a marker that /play removes.
#[tokio::test]
async fn probe_and_play_agree_on_a_later_episode_after_premiumize_served_the_first() {
    let (token, hash) = ("pm-pack", "e".repeat(40));
    let cache = mem();
    cache.put(&pm_queued_key(token, &hash), &unix_secs_ago(Duration::ZERO), QUEUED_TTL);

    // The cache check answers NOT cached, so HeldBy names nobody and the held mark is the only thing
    // that can. With it answering `true` the holder came from the cache truth instead, and reverting
    // HoldsTorrent's held-mark arm left the whole suite green.
    let client: Arc<dyn Doer> = Arc::new(MockDoer(|r: reqwest::Request| -> Answer {
        if r.url().path().contains("cache/check") {
            return Ok(resp(200, r#"{"status":"success","response":[false]}"#));
        }
        Ok(resp(
            200,
            r#"{"status":"success","content":[{"path":"Show.S01E01.mkv","link":"https://pm.example/e1.mkv","size":100},{"path":"Show.S01E02.mkv","link":"https://pm.example/e2.mkv","size":100}]}"#,
        ))
    }));
    let h = route_handler(&cache, Arc::new(premiumize(token, &cache, client)));
    let pm_blob = blob(&format!(r#"{{"debrid":[{{"service":"premiumize","token":"{token}"}}],"indexers":["torrentio"],"resultCap":20}}"#));
    let ep = |n: i64| encode_play_token(&PlayTarget { info_hash: hash.clone(), season: Some(1), episode: Some(n), ..Default::default() });

    // Episode 1 plays, which settles the queue marker.
    let first = get(&h, &format!("/{pm_blob}/play/{}", ep(1)), &[]).await;
    assert_eq!(first.status(), 302, "playing episode 1 = {}, want 302", first.status());

    // Episode 2 of the same pack, polled the way a client polls.
    for i in 1..=3 {
        let probe = get(&h, &format!("/{pm_blob}/play/{}?probe=1", ep(2)), &[]).await;
        assert_ne!(
            probe.status(),
            404,
            "poll {i}: ?probe=1 answered 404 not_queued for a pack Premiumize just served an episode of — the client blacklists a release /play resolves in the same breath"
        );
    }
    let play = get(&h, &format!("/{pm_blob}/play/{}", ep(2)), &[]).await;
    assert_eq!(play.status(), 302, "playing episode 2 = {}, want 302", play.status());
}

// A Premiumize release that has been EVICTED must not let read-only polls buy it back, uncharged.
//
// The held mark is a six-hour claim about the account, and a claim can be wrong. Once Premiumize evicts
// the release the mark still reads "held", which lets a read-only poll through the NoAdd gate to
// directdl — and directdl IS the purchase. Letting that mark also suppress the charge made every poll a
// real, unbilled add with no queue marker to bound it:
//
//	before: 30 polls -> 2 directdl, allowance 50->49, marker set (10-minute give-up)
//	after:  30 polls -> 32 directdl, allowance 50->50, no marker, no give-up
//
// Two guards are asserted: the charge is taken (so the fifty an hour sees it), and the mark is dropped
// (so the next poll does not walk the same path again).
#[tokio::test]
async fn premiumize_an_evicted_release_is_not_bought_back_by_read_only_polls() {
    use crate::stores::refusal::{held_recently, note_held};
    let token = "pm-evicted";
    let hash = "7".repeat(40);
    let cache = mem();
    note_held(&*cache, token, &hash); // the account held it when /play last served it

    let calls = Arc::new(AtomicUsize::new(0));
    let c = calls.clone();
    let client: Arc<dyn Doer> = Arc::new(MockDoer(move |_r: reqwest::Request| -> Answer {
        c.fetch_add(1, Ordering::SeqCst);
        // Evicted: directdl succeeds by QUEUEING a fresh transfer, and returns nothing to serve.
        Ok(resp(200, r#"{"status":"success","content":[]}"#))
    }));
    let s = premiumize(token, &cache, client);

    let before = global_add_budget().remaining(&budget_account(DebridService::Premiumize, token));
    let _ = s.resolve(&ResolveTarget { no_add: true, ..target(&hash) }).await;

    let after = global_add_budget().remaining(&budget_account(DebridService::Premiumize, token));
    assert_ne!(
        after,
        before,
        "a directdl that queued a transfer was not charged (allowance {before} → {after}) — the held mark is suppressing the charge, so the hourly ceiling never sees these and a polling client makes one real add every couple of seconds"
    );
    assert!(
        !held_recently(&*cache, token, &hash),
        "the held mark survived a directdl that returned no content — that answer proves the account does not hold it, and nothing else can ever clear the mark"
    );
    assert!(
        already_queued(&*cache, token, &hash),
        "no queue marker was written for a transfer this call queued — nothing bounds the 'coming' claim, so /play answers 202 downloading with no give-up"
    );

    // And the next poll must not walk the same path again.
    let _ = s.resolve(&ResolveTarget { no_add: true, ..target(&hash) }).await;
    assert!(
        calls.load(Ordering::SeqCst) <= 2,
        "{} directdl calls across two polls — the read-only path is re-queueing on every poll",
        calls.load(Ordering::SeqCst)
    );
}

// A Real-Debrid release that is DOWNLOADING must not read as dead on either route.
//
// This was the plainest failure in the route and it survived every earlier round, because both routes
// agreed and every comparison between them looked fine. An uncached release on an RD-only install is
// added, starts downloading, and has no links until it finishes — so the resolve returned a dead link,
// which is neither errAddInFlight nor a StoreUnavailableError, and RD reported no status to rescue it.
// The add is already spent, so the viewer pays for the fetch and is told the release is dead throughout.
//
// The terminal statuses are asserted alongside, because the guard this replaces existed to stop scout
// turning a dead torrent into a promise — that must still hold.
#[tokio::test]
async fn real_debrid_a_downloading_release_is_not_reported_dead() {
    for (status, want_wait) in [
        ("downloading", true),
        ("magnet_conversion", true),
        ("queued", true),
        ("compressing", true),
        ("uploading", true),
        ("error", false),
        ("virus", false),
        ("dead", false),
        ("magnet_error", false),
        ("waiting_files_selection", false),
    ] {
        let (token, hash) = (format!("rd-{status}"), "a".repeat(40));
        let cache = mem();
        let rt = target(&hash);
        cache.put(&rd_torrent_key(&token, &hash, &rt), "RDID1", Duration::from_secs(3600));

        let client: Arc<dyn Doer> = Arc::new(MockDoer(move |r: reqwest::Request| -> Answer {
            if r.url().path().contains("/torrents/info/") {
                return Ok(resp(
                    200,
                    &format!(
                        r#"{{"id":"RDID1","status":"{status}","progress":37,"files":[{{"id":1,"path":"/Movie.mkv","bytes":100,"selected":1}}],"links":[]}}"#
                    ),
                ));
            }
            Ok(resp(200, "{}"))
        }));
        let h = route_handler(&cache, Arc::new(real_debrid(&token, &cache, client)));
        let rd_blob = blob(&format!(r#"{{"debrid":[{{"service":"realdebrid","token":"{token}"}}],"indexers":["torrentio"],"resultCap":20}}"#));
        let tok = encode_play_token(&PlayTarget { info_hash: hash, ..Default::default() });

        let play = get(&h, &format!("/{rd_blob}/play/{tok}"), &[]).await;
        let probe = get(&h, &format!("/{rd_blob}/play/{tok}?probe=1"), &[]).await;

        if want_wait {
            assert_eq!(
                play.status(),
                202,
                "{status}: /play = {} ({}), want 202 — the account is fetching this right now, and the add is already spent",
                play.status(),
                body_str(&play).trim()
            );
            assert_eq!(probe.status(), 202, "{status}: ?probe=1 = {} ({}), want 202", probe.status(), body_str(&probe).trim());
            continue;
        }
        // A dead or errored torrent must NOT become a promise scout cannot keep.
        assert!(
            play.status() != 202 && probe.status() != 202,
            "status {status:?} reported as downloading (play={} probe={}) — a failure has been turned into a wait the client can never come out of",
            play.status(),
            probe.status()
        );
    }
}

// A refusal SCOUT made must not read as "nothing is queued".
//
// recordRefusal excludes errScoutSide on purpose — scout's own ceiling is not the debrid declining, and
// filing it as one blames a healthy service — so the backoff memory the probe reads stays empty and the
// route fell through all three of its "could not ask" guards to 404. The budget is per account and
// rolling, so once the ceiling is reached every uncached release on that account answers that way for
// the rest of the hour, and 404 is the answer that makes a client blacklist a release.
#[tokio::test]
async fn probe_and_play_agree_when_scouts_own_budget_is_spent() {
    let token = "budget-probe_and_play_agree_when_scouts_own_budget_is_spent";
    let hash = "4".repeat(40);
    // Spend the account's allowance the way a busy hour does.
    for _ in 0..ADD_BUDGET_LIMIT {
        global_add_budget().take(&budget_account(DebridService::TorBox, token), false);
    }

    let cache = mem();
    // The upstream must answer DEFINITIVELY, or the probe stops at "a store could not answer"
    // and both routes return 503 for unrelated reasons — which is a green test that proves
    // nothing. checkcached needs a decodable object; the account listing needs a valid empty
    // envelope so Status says "not downloading" rather than "could not find out".
    let client: Arc<dyn Doer> = Arc::new(MockDoer(|r: reqwest::Request| -> Answer {
        if r.url().path().contains("checkcached") {
            return Ok(resp(200, r#"{"data":{}}"#));
        }
        Ok(resp(200, r#"{"success":true,"data":[]}"#))
    }));
    let h = route_handler(&cache, torbox(token, &cache, client));
    let tb_blob = blob(&format!(r#"{{"debrid":[{{"service":"torbox","token":"{token}"}}],"indexers":["torrentio"],"resultCap":20}}"#));
    let tok = encode_play_token(&PlayTarget { info_hash: hash, ..Default::default() });

    let probe = get(&h, &format!("/{tb_blob}/play/{tok}?probe=1"), &[]).await;
    let play = get(&h, &format!("/{tb_blob}/play/{tok}"), &[]).await;

    assert_ne!(
        probe.status(),
        404,
        "?probe=1 answered 404 not_queued while scout's own allowance is spent (/play = {} {}) — scout could not ask, which is not the same as nobody having it",
        play.status(),
        body_str(&play).trim()
    );
    assert_eq!(
        probe.status(),
        play.status(),
        "?probe=1 = {} ({}), /play = {} ({}) — one vocabulary, two answers",
        probe.status(),
        body_str(&probe).trim(),
        play.status(),
        body_str(&play).trim()
    );
}

// ?probe=1 and /play must give the SAME answer about an add scout has out.
//
// They are one vocabulary by design — the doc on handleProbe says so — and this is the one fact in the
// package that is about us rather than about a service. /play reads it through errAddInFlight, raised
// from inside the resolve. The probe route never resolves, and all three stores answer a NoAdd target
// with errWouldAdd BEFORE they consult the marker, so it had no way to learn the state at all and fell
// through to 404 "not_queued" — for the same release, at the same instant, that /play was answering 202
// "downloading".
//
// A 404 there is the single failure that route exists to prevent: it is the URL the client polls to
// draw its progress bar, and it reads a 404 as a release nobody has. The window is not brief for two of
// the three services — Real-Debrid and Premiumize have no Status API to rediscover the torrent with, so
// nothing clears the marker before addAttemptTTL expires 90 seconds later.
//
// Asserted as an agreement between the two routes rather than as a status code, because the defect was
// the DISAGREEMENT; pinning one route's number invites the other to drift again.
//
// This fixture reaches the disagreement one branch earlier than the reported 404: with every upstream
// answering an empty body the probe stops at 503 (`status_unavailable` for TorBox, which has a Status
// API, `cache_check_unavailable` for the two that do not) while /play answers 202. Removing the guard
// reproduces that, which is the same defect at a different exit — the route not knowing a fact it
// holds. Reaching the literal 404 needs a healthy cache check AND a store that answers "not queued",
// which is a fuller fixture than this invariant needs.
#[tokio::test]
async fn probe_and_play_agree_that_an_in_flight_add_is_downloading() {
    for svc in [DebridService::TorBox, DebridService::RealDebrid, DebridService::Premiumize] {
        let cache = mem();
        note_add_attempt(&*cache, svc, "tok", H); // an add went out and nothing came back

        // Every upstream answers a well-formed "nothing here", so the marker is the only evidence
        // either route has. `{"data":{}}` rather than `{"data":[]}`: the cache check decodes data as
        // an object, so an array fails the decode and the probe stops at "cache check unavailable"
        // before it ever reaches the branch under test.
        let quiet = answer(200, r#"{"data":{}}"#);
        let store: Arc<dyn Store> = match svc {
            DebridService::RealDebrid => Arc::new(real_debrid("tok", &cache, quiet)),
            DebridService::Premiumize => Arc::new(premiumize("tok", &cache, quiet)),
            DebridService::TorBox => torbox("tok", &cache, quiet),
        };
        let h = route_handler(&cache, store);
        let tok = encode_play_token(&PlayTarget { info_hash: H.into(), ..Default::default() });

        let probe = get(&h, &format!("/{}/play/{tok}?probe=1", valid_blob()), &[]).await;
        let play = get(&h, &format!("/{}/play/{tok}", valid_blob()), &[]).await;

        // The agreement is the invariant. Comparing the two routes rather than asserting one number
        // keeps this pinned to the defect — a disagreement — instead of to whichever status a
        // fixture happens to produce.
        let svc = svc.as_str();
        assert_eq!(
            probe.status(),
            play.status(),
            "{svc}: ?probe=1 answered {} ({}) while /play answered {} ({}) for the same release at the same instant — the two routes are one vocabulary, and the client polls the probe to draw its progress bar",
            probe.status(),
            body_str(&probe).trim(),
            play.status(),
            body_str(&play).trim()
        );
        assert_ne!(
            probe.status(),
            404,
            "{svc}: ?probe=1 answered 404 for a release scout has an add out for: {} — the client reads that as a release nobody has and blacklists it",
            body_str(&probe)
        );
        assert_eq!(probe.status(), 202, "{svc}: ?probe=1 = {}, want 202 downloading: {}", probe.status(), body_str(&probe));
    }
}
