//! Go: `internal/scout/indexerconfig_test.go` (429 lines). Work package 5.
//!
//! The Go tests share the package-level `minted` cache, so each test here takes `serial()` to keep the
//! parallel Rust test runner from interleaving them.

use std::collections::HashMap;
use std::sync::{Mutex, MutexGuard};
use std::time::{Duration, Instant};

use async_trait::async_trait;

use crate::config::{Config, DebridAccount, DebridService, Indexer};
use crate::indexerconfig::{
    indexer_base_with_config, primary_debrid, prune_minted_locked, MintedConfig, UnaskableScraper,
    MAX_MINTED_ENTRIES, MINTED, MINTED_TTL,
};
use crate::rank::RawStream;
use crate::scrape::{scrape_all, DoError, Doer, ScrapeQuery, Scraper, DEFAULT_INDEXER_URLS};

use super::mock::resp;

static SERIAL: Mutex<()> = Mutex::new(());

fn serial() -> MutexGuard<'static, ()> {
    SERIAL.lock().unwrap_or_else(|e| e.into_inner())
}

fn default_url(indexer: &str) -> &'static str {
    DEFAULT_INDEXER_URLS
        .iter()
        .find(|(name, _)| *name == indexer)
        .map(|(_, url)| *url)
        .expect("known indexer")
}

fn reset_minted() {
    *MINTED.lock().expect("lock") = Some(HashMap::new());
}

fn with_minted<R>(f: impl FnOnce(&mut HashMap<String, MintedConfig>) -> R) -> R {
    let mut guard = MINTED.lock().expect("lock");
    f(guard.get_or_insert_with(HashMap::new))
}

fn torbox(token: &str) -> DebridAccount {
    DebridAccount { service: DebridService::TorBox, token: token.into() }
}

fn cfg_with(token: &str) -> Config {
    Config { debrid: vec![torbox(token)], ..Default::default() }
}

// stubDoer answers one canned response and records what it was asked.
struct StubDoer {
    status: u16,
    state: Mutex<StubState>,
}

#[derive(Default)]
struct StubState {
    body: String,
    err: Option<String>,
    reqs: usize,
    last_body: String,
}

impl StubDoer {
    fn ok(body: &str) -> StubDoer {
        StubDoer { status: 200, state: Mutex::new(StubState { body: body.into(), ..Default::default() }) }
    }
    fn failing(err: &str) -> StubDoer {
        StubDoer { status: 0, state: Mutex::new(StubState { err: Some(err.into()), ..Default::default() }) }
    }
    fn set_body(&self, body: &str) {
        self.state.lock().expect("lock").body = body.into();
    }
    fn reqs(&self) -> usize {
        self.state.lock().expect("lock").reqs
    }
    fn last_body(&self) -> String {
        self.state.lock().expect("lock").last_body.clone()
    }
}

#[async_trait]
impl Doer for StubDoer {
    async fn do_request(&self, req: reqwest::Request) -> Result<reqwest::Response, DoError> {
        let mut st = self.state.lock().expect("lock");
        st.reqs += 1;
        if let Some(bytes) = req.body().and_then(|b| b.as_bytes()) {
            st.last_body = String::from_utf8_lossy(bytes).into_owned();
        }
        if let Some(err) = &st.err {
            return Err(err.clone().into());
        }
        Ok(resp(self.status, &st.body))
    }
}

// The comet config is built locally — it carries the debrid token, which is the whole reason this is
// opt-in, so the shape is worth pinning.
#[test]
fn mint_comet_url_carries_the_account() {
    use crate::indexerconfig::mint_comet_url;
    use base64::Engine as _;
    let got = mint_comet_url(&torbox("tb-token"));
    let prefix = format!("{}/", default_url("comet"));
    assert!(got.starts_with(&prefix), "not a comet URL: {got}");
    let raw = base64::engine::general_purpose::STANDARD
        .decode(&got[prefix.len()..])
        .expect("config segment is base64");
    let cfg: serde_json::Value = serde_json::from_slice(&raw).expect("config segment is JSON");
    assert!(
        cfg["debridService"] == "torbox" && cfg["debridApiKey"] == "tb-token",
        "account not carried: {} / {}",
        cfg["debridService"],
        cfg["debridApiKey"]
    );
}

// MediaFusion encrypts server-side, so a mint is a round trip that can fail — and a failure must not
// produce a half-formed URL that then 404s on every search.
#[tokio::test]
async fn mint_media_fusion_url() {
    let _guard = serial();
    let ok = StubDoer::ok(r#"{"encrypted_str":"ENCRYPTED","status":"success"}"#);
    let got = crate::indexerconfig::mint_media_fusion_url(&torbox("t"), Some(&ok)).await;
    assert_eq!(got, format!("{}/ENCRYPTED", default_url("mediafusion")), "minted URL");
    assert!(ok.last_body().contains(r#""token":"t""#), "the account was not sent: {}", ok.last_body());

    let refused = StubDoer::ok(r#"{"message":"invalid user data","status":"error"}"#);
    let got = crate::indexerconfig::mint_media_fusion_url(&torbox("t"), Some(&refused)).await;
    assert_eq!(got, "", "a refusal must yield no URL, got {got}");

    let transport = StubDoer::failing("dial tcp: refused");
    let got = crate::indexerconfig::mint_media_fusion_url(&torbox("t"), Some(&transport)).await;
    assert_eq!(got, "", "an unreachable host must yield no URL, got {got}");

    let got = crate::indexerconfig::mint_media_fusion_url(&torbox("t"), None).await;
    assert_eq!(got, "", "no client must yield no URL, got {got}");
}

// Minted once per account, then reused: a season opens with one request per episode, and minting on each
// would put a round trip to a third party in front of every one of them.
#[tokio::test]
async fn indexer_base_with_config_caches_per_account() {
    let _guard = serial();
    reset_minted();
    let d = StubDoer::ok(r#"{"encrypted_str":"E1","status":"success"}"#);
    let cfg = cfg_with("acct-a");

    let (first, _) = indexer_base_with_config(&Indexer("mediafusion".into()), &cfg, Some(&d)).await;
    let (second, _) = indexer_base_with_config(&Indexer("mediafusion".into()), &cfg, Some(&d)).await;
    assert!(
        !first.is_empty() && first == second,
        "expected a stable minted URL, got {first:?} then {second:?}"
    );
    assert_eq!(d.reqs(), 1, "minted more than once");

    // A different account must not reuse the first account's config — that would search one person's
    // debrid with another's token.
    let other = cfg_with("acct-b");
    d.set_body(r#"{"encrypted_str":"E2","status":"success"}"#);
    let (got, _) = indexer_base_with_config(&Indexer("mediafusion".into()), &other, Some(&d)).await;
    assert_ne!(got, first, "a second account reused the first's config: {got}");
}

// With no debrid account there is nothing to mint from, and comet is built without a round trip.
#[tokio::test]
async fn indexer_base_with_config_edges() {
    let _guard = serial();
    reset_minted();
    let d = StubDoer::ok(r#"{"encrypted_str":"E","status":"success"}"#);
    let (got, transient) =
        indexer_base_with_config(&Indexer("mediafusion".into()), &Config::default(), Some(&d)).await;
    assert!(
        got.is_empty() && !transient,
        "no account should mint nothing, and permanently so: {got:?} transient={transient}"
    );
    assert!(primary_debrid(None).is_none(), "a nil config has no account");
    let cfg = cfg_with("t");
    let (got, _) = indexer_base_with_config(&Indexer("comet".into()), &cfg, Some(&d)).await;
    assert!(!got.is_empty(), "comet mints locally and should need no round trip");
    let (got, _) = indexer_base_with_config(&Indexer("torrentio".into()), &cfg, Some(&d)).await;
    assert!(got.is_empty(), "torrentio needs no config segment, got {got}");
}

// A mint that FAILED because the indexer was unreachable is an outage, not a misconfiguration, and the
// two must stay distinguishable.
//
// They were not. A failed mint is cached for two minutes and makes the indexer "unaskable", which the
// empty-result quorum EXCUSES — so one 10s timeout on a loaded homelab promoted torrentio's empty answer
// to an authoritative "this release does not exist", cached and served with stale-if-error for a day.
// A transient failure means we do not know what that indexer would have said, which is precisely the
// state the quorum exists to detect.
#[tokio::test]
async fn indexer_base_with_config_separates_outage_from_misconfiguration() {
    let _guard = serial();
    let cfg = cfg_with("t");

    reset_minted();
    let unreachable = StubDoer::failing("dial tcp: i/o timeout");
    let (got, transient) =
        indexer_base_with_config(&Indexer("mediafusion".into()), &cfg, Some(&unreachable)).await;
    assert!(
        got.is_empty() && transient,
        "an unreachable indexer is a transient failure: {got:?} transient={transient}"
    );

    // The classification must survive the failure cache, or the two minutes it is replayed for
    // reintroduce the same confusion.
    let (_, cached) =
        indexer_base_with_config(&Indexer("mediafusion".into()), &cfg, Some(&unreachable)).await;
    assert!(cached, "the cached failure forgot that it was transient");
    assert_eq!(unreachable.reqs(), 1, "the failure was re-POSTed instead of being cached");
}

struct Answered;

#[async_trait]
impl Scraper for Answered {
    fn id(&self) -> Indexer {
        Indexer("torrentio".into())
    }
    async fn scrape(&self, _q: &ScrapeQuery) -> Result<Vec<RawStream>, DoError> {
        Ok(vec![])
    }
}

// A transient mint failure COUNTS as an indexer that did not answer, so an empty list stays
// non-authoritative. Only a permanently unconfigured one is excused from the quorum.
#[tokio::test]
async fn unaskable_scraper_transient_still_counts_in_the_quorum() {
    let budget = Duration::from_millis(50);
    let answered: std::sync::Arc<dyn Scraper> = std::sync::Arc::new(Answered);

    let unconfigured: Vec<std::sync::Arc<dyn Scraper>> = vec![
        std::sync::Arc::clone(&answered),
        std::sync::Arc::new(UnaskableScraper { indexer: Indexer("mediafusion".into()), transient: false }),
    ];
    let (_, ok, _) = scrape_all(&unconfigured, &ScrapeQuery::default(), budget).await;
    assert!(ok, "an unconfigured indexer must not make an empty result look like an outage");

    let outage: Vec<std::sync::Arc<dyn Scraper>> = vec![
        std::sync::Arc::clone(&answered),
        std::sync::Arc::new(UnaskableScraper { indexer: Indexer("mediafusion".into()), transient: true }),
    ];
    let (_, ok, _) = scrape_all(&outage, &ScrapeQuery::default(), budget).await;
    assert!(!ok, "an indexer that could not be reached must leave the empty result non-authoritative");

    // Asking NOBODY is not an answer either, and this is the case the excuse above can reach on its own.
    // Every unaskable indexer is excused from the quorum, so a list of nothing but unaskable indexers
    // excuses everyone and the empty result comes back authoritative — a confident "nobody has this",
    // cached for five minutes on the device and up to a day on any later error, from an install that
    // made no request at all.
    //
    // Reachable without editing anything: /configure offers comet and mediafusion as tick boxes, so a
    // user can untick torrentio, and with minting off (the default) and no URLs set, every scraper in
    // the list is unaskable.
    let nobody: Vec<std::sync::Arc<dyn Scraper>> = vec![
        std::sync::Arc::new(UnaskableScraper { indexer: Indexer("comet".into()), transient: false }),
        std::sync::Arc::new(UnaskableScraper { indexer: Indexer("mediafusion".into()), transient: false }),
    ];
    let (_, ok, _) = scrape_all(&nobody, &ScrapeQuery::default(), budget).await;
    assert!(
        !ok,
        "every indexer unaskable: an empty result must not be authoritative — nothing was asked, so nothing answered"
    );
}

// Off unless the operator says otherwise: this decides whether the debrid token leaves the homelab.
#[test]
#[ignore = "needs settings_from_env (deps.rs, package 4) to read MINT_INDEXER_CONFIGS"]
fn minting_is_opt_in() {
    #[cfg(any())]
    fn pending() {
        use crate::deps::settings_from_env;
        assert!(
            !settings_from_env(|_| String::new()).mint_indexer_configs,
            "minting must default to off — it sends the debrid token to a third party"
        );
        let on =
            settings_from_env(|k| if k == "MINT_INDEXER_CONFIGS" { "true".into() } else { String::new() });
        assert!(on.mint_indexer_configs, "MINT_INDEXER_CONFIGS=true should enable it");
    }
    todo!("pending: indexerconfig_test.go TestMintingIsOptIn")
}

// Expired mint entries are reclaimed on write.
//
// Expiry was only consulted on READ, so an entry nobody asked for again stayed resident forever — and
// the key is derived from a token the config supplies unverified, with comet's mint needing no network
// at all, so every distinct token minted successfully and was kept.
#[tokio::test]
async fn minted_expired_entries_are_reclaimed() {
    let _guard = serial();
    reset_minted();
    let now = Instant::now();
    with_minted(|m| {
        for i in 0..500 {
            m.insert(
                format!("comet:{i}"),
                MintedConfig { url: "u".into(), at: now - 2 * MINTED_TTL, used: now - 2 * MINTED_TTL },
            );
        }
        m.insert("comet:live".into(), MintedConfig { url: "u".into(), at: now, used: now });
    });

    let (url, _) = indexer_base_with_config(&Indexer("comet".into()), &cfg_with("fresh"), None).await;
    assert!(!url.is_empty(), "comet mints locally and should succeed");
    with_minted(|m| {
        assert!(m.len() <= 2, "{} entries retained; expired mints are never reclaimed", m.len());
        assert!(m.contains_key("comet:live"), "a live entry was pruned");
    });
}

// The minted-config cache is bounded by COUNT as well as by age.
//
// TTL pruning alone cannot bound it, and that is what made it look guarded: the key derives from a token
// the config supplies and nobody verifies, and comet's mint is local base64 with no round trip, so every
// distinct token a caller invents mints successfully and is held for twelve hours. At ~500 bytes an
// entry that is a memory bomb with a twelve-hour fuse inside a 230 MiB heap.
#[test]
fn minted_cache_is_bounded_by_count() {
    let _guard = serial();
    let saved = MINTED.lock().expect("lock").take();

    // Far more distinct, unexpired entries than the ceiling — all well inside mintedTTL so nothing expires
    // out, and all IDLE past the protect window so all of them are eviction candidates.
    let now = Instant::now();
    let idle_base = now - Duration::from_secs(3600);
    let mut m = HashMap::new();
    for i in 0..MAX_MINTED_ENTRIES * 3 {
        // Staggered so "least recently used first" is a defined order.
        let stamp = idle_base - Duration::from_secs(i as u64);
        m.insert(
            format!("comet:{i}"),
            MintedConfig { url: "https://comet.example/x".into(), at: now, used: stamp },
        );
    }
    prune_minted_locked(&mut m);
    let got = m.len();
    let newest_kept = m.contains_key("comet:0");
    let oldest_kept = m.contains_key(&format!("comet:{}", MAX_MINTED_ENTRIES * 3 - 1));
    *MINTED.lock().expect("lock") = saved;

    assert!(got < MAX_MINTED_ENTRIES, "kept {got} entries, want under the {MAX_MINTED_ENTRIES} ceiling (with room for the insert that follows)");
    assert!(got != 0, "the prune emptied the cache rather than trimming it");
    assert!(newest_kept, "the newest entry was evicted");
    assert!(!oldest_kept, "the oldest entry survived while newer ones were evicted");
}

// A cache under the ceiling with nothing expired is left completely alone.
#[test]
fn minted_cache_prune_keeps_a_healthy_cache() {
    let _guard = serial();
    let mut m = HashMap::new();
    for i in 0..5 {
        m.insert(
            format!("comet:{i}"),
            MintedConfig { url: "https://comet.example/x".into(), at: Instant::now(), used: Instant::now() },
        );
    }
    prune_minted_locked(&mut m);
    assert_eq!(m.len(), 5, "a healthy cache of 5 was pruned to {}", m.len());
}

// Eviction is least-recently-USED, not oldest-minted — and the difference is the whole point.
//
// A legitimate install's entries are the OLDEST things in the map by construction: minted once and good
// for twelve hours. Evicting by mint time therefore threw them out first and kept a flood of
// caller-invented ones, so every later stream request re-minted — for mediafusion a POST with a 3s
// timeout sitting in front of the scrape. The ceiling turned a memory bomb into a latency one.
#[test]
fn minted_cache_keeps_the_entries_actually_in_use() {
    let _guard = serial();
    let saved = MINTED.lock().expect("lock").take();

    let now = Instant::now();
    let mut m = HashMap::new();
    // The operator's own: minted an hour ago and served just now. Sorting by MINT time evicts exactly
    // these, because a legitimate install's entries are the oldest in the map by construction.
    let legit = ["comet:real", "mediafusion:real"];
    for k in legit {
        m.insert(
            k.to_owned(),
            MintedConfig {
                url: "https://real.example/x".into(),
                at: now - Duration::from_secs(3600),
                used: now,
            },
        );
    }
    // Freshly minted entries that nobody has come back for.
    for i in 0..MAX_MINTED_ENTRIES * 2 {
        let stamp = now - Duration::from_secs(i as u64 + 1);
        m.insert(
            format!("comet:flood{i}"),
            MintedConfig { url: "https://flood.example/x".into(), at: stamp, used: stamp },
        );
    }
    prune_minted_locked(&mut m);
    let survived = legit.iter().filter(|k| m.contains_key(**k)).count();
    let total = m.len();
    *MINTED.lock().expect("lock") = saved;

    assert_eq!(
        survived,
        legit.len(),
        "{survived} of {} in-use entries survived — eviction is not ordered by last use",
        legit.len()
    );
    assert!(
        total < MAX_MINTED_ENTRIES,
        "ceiling not enforced: total={total}, want under {MAX_MINTED_ENTRIES}"
    );
}

// An entry idle past the protect window is evictable again, so a genuinely disused config cannot pin a
// slot until its 12-hour TTL runs out.
#[test]
fn minted_cache_reclaims_idle_entries() {
    let _guard = serial();
    let saved = MINTED.lock().expect("lock").take();

    let now = Instant::now();
    let mut m = HashMap::new();
    for i in 0..MAX_MINTED_ENTRIES + 10 {
        let stamp = now - Duration::from_secs(i as u64);
        m.insert(
            format!("comet:idle{i}"),
            MintedConfig { url: "https://x.example/x".into(), at: now, used: stamp },
        );
    }
    prune_minted_locked(&mut m);
    let total = m.len();
    *MINTED.lock().expect("lock") = saved;

    assert!(
        total < MAX_MINTED_ENTRIES,
        "idle entries were not reclaimed: total={total}, want under {MAX_MINTED_ENTRIES}"
    );
}

// The ceiling holds through the REAL caller, not just through prune_minted_locked.
//
// It is reached from two call sites — a successful mint and a remembered failure — and one of them
// forgetting to prune is not something a direct test of prune_minted_locked can see, so the flood is driven
// through indexer_base_with_config. The rationale used to be a caller-side room check that gated the insert;
// that guard is gone, because under plain LRU the prune always makes room and the answer was always yes.
#[tokio::test]
async fn indexer_base_with_config_map_stays_bounded_under_a_flood() {
    let _guard = serial();
    // BOTH sites get their own flood. Only the success one was driven, so deleting the prune from the
    // failure site left the whole suite green while 1,024 invented tokens sat in the map — the site that
    // is easier to reach of the two, since it needs no working indexer, only an unhealthy one.
    //
    // (name, indexer, doer, want_url). comet mints locally, so a flood costs no round trips — which is why
    // it is the cheap attack. mediafusion mints over the network; while it is down every distinct token
    // remembers a failure, and a failed mint yields no URL, which is the point of that path.
    let cases = [
        ("a successful mint", "comet", StubDoer::ok(r#"{"encrypted_str":"E","status":"success"}"#), true),
        ("a remembered failure", "mediafusion", StubDoer::failing("dial tcp: i/o timeout"), false),
    ];
    for (name, indexer, doer, want_url) in cases {
        reset_minted();
        for i in 0..MAX_MINTED_ENTRIES * 4 {
            let (got, _) = indexer_base_with_config(
                &Indexer(indexer.into()),
                &cfg_with(&format!("invented-{i}")),
                Some(&doer),
            )
            .await;
            assert_eq!(!got.is_empty(), want_url, "{name}: mint {i} returned {got:?}");
        }
        let total = with_minted(|m| m.len());
        assert!(total <= MAX_MINTED_ENTRIES, "{name}: the map holds {total} entries after a flood of distinct tokens, ceiling is {MAX_MINTED_ENTRIES}");
        // And it is actually full, or the flood never reached the ceiling and the bound is untested.
        assert!(
            total >= MAX_MINTED_ENTRIES,
            "{name}: the flood left only {total} entries — it never reached the {MAX_MINTED_ENTRIES} ceiling, so this asserts nothing about eviction"
        );
    }
    reset_minted();
}

// Not in the Go suite, which relies on `singleflight.Group`. `Flight` is the port of it, and these pin what the
// callers rely on: concurrent callers share one run and its result, and nothing outlives the flight, an error
// included.
#[tokio::test]
async fn flight_shares_concurrent_calls_and_does_not_keep_an_error() {
    use crate::indexerconfig::Flight;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    let flight: Arc<Flight<Result<u32, String>>> = Arc::new(Flight::new());
    let runs = Arc::new(AtomicUsize::new(0));
    let callers: Vec<_> = (0..8)
        .map(|_| {
            let (flight, runs) = (Arc::clone(&flight), Arc::clone(&runs));
            tokio::spawn(async move {
                flight
                    .run("k", async {
                        runs.fetch_add(1, Ordering::SeqCst);
                        tokio::time::sleep(Duration::from_millis(100)).await;
                        Err::<u32, String>("boom".into())
                    })
                    .await
            })
        })
        .collect();
    for c in callers {
        assert_eq!(
            c.await.expect("caller"),
            Err("boom".to_owned()),
            "a caller did not get the shared result"
        );
    }
    assert_eq!(runs.load(Ordering::SeqCst), 1, "concurrent callers ran the call more than once");
    assert_eq!(flight.in_flight(), 0, "a finished flight was kept");

    // The error was not cached: the next caller runs the call again, and can succeed.
    let again = flight.run("k", async { Ok(7) }).await;
    assert_eq!(again, Ok(7), "a later caller was handed the earlier error");
    assert_eq!(flight.in_flight(), 0);
}

// A caller that goes away mid-flight must not strand the callers waiting on it, or leave its key behind.
#[tokio::test]
async fn flight_survives_a_caller_that_goes_away() {
    use crate::indexerconfig::Flight;
    use std::sync::Arc;

    let flight: Arc<Flight<u32>> = Arc::new(Flight::new());
    let leader = {
        let flight = Arc::clone(&flight);
        tokio::spawn(async move {
            flight
                .run("k", async {
                    tokio::time::sleep(Duration::from_secs(3600)).await;
                    1
                })
                .await
        })
    };
    tokio::time::sleep(Duration::from_millis(20)).await;
    let waiter = {
        let flight = Arc::clone(&flight);
        tokio::spawn(async move { flight.run("k", async { 2 }).await })
    };
    tokio::time::sleep(Duration::from_millis(20)).await;
    leader.abort();
    let got = tokio::time::timeout(Duration::from_secs(5), waiter)
        .await
        .expect("the waiter was stranded")
        .expect("waiter");
    assert_eq!(got, 2, "the waiter should run its own call once the leader is gone");
    assert_eq!(flight.in_flight(), 0, "a finished flight was kept");

    // A lone caller that goes away leaves nothing behind either.
    let lone = {
        let flight = Arc::clone(&flight);
        tokio::spawn(async move { flight.run("other", std::future::pending::<u32>()).await })
    };
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert_eq!(flight.in_flight(), 1);
    lone.abort();
    let _ = lone.await;
    assert_eq!(flight.in_flight(), 0, "an abandoned flight was kept");
}
