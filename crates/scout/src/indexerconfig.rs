//! Port of `internal/scout/indexerconfig.go`. Work package 5.
//!
//! Minting the per-install config segment that comet and mediafusion require.
//!
//! Both serve streams only from `/{config}/stream/...`, where the config is produced by their own
//! /configure page. Asked bare they do not fail usefully — comet answers 403 and mediafusion answers 200
//! with an empty list, a dead indexer wearing a healthy status — so scout skips an indexer it cannot
//! address, and the install was left querying exactly ONE source. Every resilience rule in this package
//! (the answered-quorum, the shed-request retry, dropping dead indexers) then reduces to "torrentio said
//! so".
//!
//! Scout is already given the debrid account on every request, so it can build those configs itself
//! rather than asking the operator to paste two URLs. **This sends the debrid token to the indexer**,
//! which is a real change in who holds it: it goes from "our homelab and the debrid" to "and the two
//! indexer hosts too". That is why it is opt-in via MINT_INDEXER_CONFIGS, and why an explicit
//! COMET_URL / MEDIAFUSION_URL always wins — an operator who has pasted a token-free config
//! keeps it.
//!
//! The indexers are asked to SEARCH, not to resolve: scout takes infohashes from them and does its own
//! debrid work, so a minted config should never cause an add against the account. That is an assumption
//! about their behaviour, not something this package can enforce, and it is why the add-quota guards in
//! probe_fanout.go and stores.go matter independently of this file.

use std::collections::{BTreeMap, HashMap};
use std::future::Future;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use base64::Engine;
use reqwest::header::{HeaderValue, CONTENT_TYPE, USER_AGENT};
use serde::Deserialize;
use tokio::sync::OnceCell;

use crate::config::{Config, DebridAccount, DebridService, Indexer};
use crate::loglimit::log_limited;
use crate::rank::RawStream;
use crate::scrape::{DoError, Doer, Scraper, ScrapeQuery, DEFAULT_INDEXER_URLS};
use crate::util::key_hash;

/// Go `mintedTTL`: how long a minted config is reused. Long enough that minting is rare, short enough that a
/// rotated token stops being replayed to a third party within the day.
pub(crate) const MINTED_TTL: Duration = Duration::from_secs(12 * 60 * 60);
/// Go `mintFailureTTL`: how long a failure to mint is remembered. Short, because a host that is down now may be
/// up in a minute and an indexer skipped is real coverage lost — but not zero, which is what made an unhealthy
/// mediafusion get one POST per stream request.
pub(crate) const MINT_FAILURE_TTL: Duration = Duration::from_secs(2 * 60);
/// Go `mintTimeout`: how long a mint may take. It runs BEFORE the scrape and inside the singleflight leader, so
/// its ceiling has to leave the scrape budget intact rather than consume it.
pub(crate) const MINT_TIMEOUT: Duration = Duration::from_secs(3);
/// Go `maxMintedEntries` caps the cache by COUNT as well as by age.
///
/// TTL pruning alone cannot bound this map, and that is the whole problem with it: the key is derived
/// from a token the config supplies and nobody verifies, comet's mint is local base64 with no round trip
/// at all, so every distinct token a caller invents mints successfully and is then held for the full
/// twelve hours. At roughly 500 bytes an entry, an unbounded map inside a 230 MiB heap is a memory bomb
/// with a twelve-hour fuse — and TTL pruning makes it look guarded.
///
/// A legitimate install has one entry per (indexer, account): four indexers and a handful of accounts,
/// so under twenty. 256 is an order of magnitude above that.
pub(crate) const MAX_MINTED_ENTRIES: usize = 256;

/// Go `mintedConfig`: one minted indexer URL, when it was minted and when it was last used.
///
/// Go also records whether a failure was transient. That is not stored here: a transient failure is a failed
/// mint for mediafusion (the only minter that reaches the network), so `is_transient_failure` derives it from
/// the indexer and the URL, as Go computed it at mint time.
#[derive(Debug, Clone)]
pub(crate) struct MintedConfig {
    pub url: String,
    /// When it was minted — the clock the TTL runs on, and deliberately NOT refreshed by use: a minted
    /// config should be re-minted every mintedTTL however busy it is.
    pub at: Instant,
    /// When it was last handed out — the clock EVICTION runs on, which is a different question.
    ///
    /// Evicting by `at` was first-in-first-out by creation, and that is exactly backwards under the attack
    /// the ceiling exists for: a legitimate install's entries are the oldest things in the map by
    /// construction (minted once, good for twelve hours), so a flood of caller-invented tokens evicted the
    /// operator's own entries and kept the attacker's.
    pub used: Instant,
}

impl MintedConfig {
    /// Go `mintedConfig.ttl`.
    fn ttl(&self) -> Duration {
        if self.url.is_empty() {
            MINT_FAILURE_TTL
        } else {
            MINTED_TTL
        }
    }
}

/// Go `minted` with `mintedMu`: the minted-config cache. `None` until first use.
pub(crate) static MINTED: Mutex<Option<HashMap<String, MintedConfig>>> = Mutex::new(None);

/// Go `mintFlight`: one round trip per (indexer, account) in flight at a time. The cache alone only collapses
/// SEQUENTIAL repeats, and a burst of episode requests is anything but sequential.
static MINT_FLIGHT: Flight<String> = Flight::new();

/// Go `singleflight.Group`: concurrent callers of one key share one run of the call and its result.
pub(crate) struct Flight<T>(Mutex<BTreeMap<String, Arc<OnceCell<T>>>>);

impl<T: Clone> Flight<T> {
    pub(crate) const fn new() -> Self {
        Flight(Mutex::new(BTreeMap::new()))
    }

    /// Runs `call` for the first caller of `key`; a caller that arrives while it is in flight waits for it and
    /// takes the same answer. The key is forgotten once the call has finished, so the next caller runs again.
    pub(crate) async fn run<F: Future<Output = T>>(&self, key: &str, call: F) -> T {
        let cell = Arc::clone(lock(&self.0).entry(key.to_owned()).or_default());
        let value = cell.get_or_init(|| call).await.clone();
        let mut calls = lock(&self.0);
        if calls.get(key).is_some_and(|c| Arc::ptr_eq(c, &cell)) {
            calls.remove(key);
        }
        value
    }
}

/// Go `pruneMintedLocked` drops expired entries, then evicts the least recently used past the ceiling.
///
/// It used to return "is there room?" for the caller to gate its insert on. That guard is gone: plain LRU
/// always makes room, so the answer was always yes, and a guard that cannot be false hides the bound rather
/// than enforcing it.
pub(crate) fn prune_minted_locked(minted: &mut HashMap<String, MintedConfig>) {
    let now = Instant::now();
    minted.retain(|_, m| now.duration_since(m.at) < m.ttl());
    if minted.len() < MAX_MINTED_ENTRIES {
        return;
    }
    // Least-recently-USED first. Sorting by mint time instead evicted the operator's own entries
    // preferentially, since a legitimate install's are the oldest in the map by construction — minted once
    // and good for twelve hours.
    //
    // A scan for the single oldest, not a sort of the whole map. Only one entry has to go — the caller is
    // about to insert exactly one — and the loop is only ever one entry past the ceiling, because both insert
    // sites prune first.
    while minted.len() >= MAX_MINTED_ENTRIES {
        let oldest = minted.iter().min_by_key(|(_, m)| m.used).map(|(k, _)| k.clone());
        match oldest {
            Some(k) => {
                minted.remove(&k);
            }
            None => break,
        }
    }
    // Once at the ceiling every insert evicts, so logging per eviction is one line per request under exactly
    // the flood this ceiling exists to absorb — unbounded log volume in place of unbounded memory, written
    // while holding the mint mutex. Once a minute is enough to notice.
    log_limited(
        "minted-config-ceiling",
        &format!("minted-config cache is at its {MAX_MINTED_ENTRIES}-entry ceiling; evicting least-recently-used"),
    );
}

/// Go `primaryDebrid`: the account a config's searches are made with, None when it has none. First configured
/// wins — the same order ResolvePreferring uses when nothing is known about who holds a release.
pub(crate) fn primary_debrid(cfg: Option<&Config>) -> Option<DebridAccount> {
    cfg?.debrid.first().cloned()
}

/// Go `defaultIndexerURLs`: the public base URL of an indexer, or "" for one that has none.
fn default_indexer_url(id: &str) -> &'static str {
    DEFAULT_INDEXER_URLS.iter().find(|(name, _)| *name == id).map_or("", |&(_, url)| url)
}

/// Go `mintCometURL`: the Comet config built locally from the account, carrying its debrid token. Comet's
/// config is base64 of a plain JSON object; no round trip to the service is needed.
pub(crate) fn mint_comet_url(account: &DebridAccount) -> String {
    let service = if account.service == DebridService::RealDebrid {
        "realdebrid"
    } else {
        account.service.as_str()
    };
    let body = serde_json::json!({
        "maxResultsPerResolution": 0,
        "maxSize": 0,
        "cachedOnly": false,
        "removeTrash": false,
        "resultFormat": ["all"],
        "debridService": service,
        "debridApiKey": account.token,
        "debridStreamProxyPassword": "",
        "languages": {"required": [], "exclude": [], "preferred": []},
        "resolutions": {},
        "options": {},
    });
    let encoded = base64::engine::general_purpose::STANDARD.encode(body.to_string());
    format!("{}/{encoded}", default_indexer_url("comet"))
}

/// Go `mintMediaFusionURL`: MediaFusion encrypts its config server-side: POST the settings, get an opaque
/// string back. "" on a refusal or an unreachable host.
pub(crate) async fn mint_media_fusion_url(account: &DebridAccount, client: Option<&dyn Doer>) -> String {
    let Some(client) = client else { return String::new() };
    let body = serde_json::json!({
        "streaming_provider": {
            "service": account.service.as_str(),
            "token": account.token,
        },
        "selected_resolutions": ["4k", "2160p", "1440p", "1080p", "720p", "480p", null],
        "enable_catalogs": false,
        "max_size": "inf",
        "torrent_sorting_priority": [],
    });
    let base = default_indexer_url("mediafusion");
    let Ok(target) = reqwest::Url::parse(&format!("{base}/encrypt-user-data")) else {
        return String::new();
    };
    let mut req = reqwest::Request::new(reqwest::Method::POST, target);
    req.headers_mut().insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
    req.headers_mut().insert(USER_AGENT, HeaderValue::from_static(SCRAPE_USER_AGENT));
    *req.body_mut() = Some(body.to_string().into());

    // Under the 8s per-indexer scrape budget this sits in front of, not over it. At 10s a slow mediafusion
    // could burn the whole budget before a single indexer had been asked anything.
    let answer = tokio::time::timeout(MINT_TIMEOUT, async {
        let resp = match client.do_request(req).await {
            Ok(resp) => resp,
            Err(err) => {
                log_limited("mint-mediafusion-unreachable", &format!("could not mint a mediafusion config: {err}"));
                return String::new();
            }
        };
        let Some(raw) = read_capped(resp, 64 << 10).await else { return String::new() };
        // Go decodes the first JSON value in the body, and ignores whatever follows it.
        let Some(Ok(out)) = serde_json::Deserializer::from_slice(&raw).into_iter::<MediaFusionMint>().next() else {
            return String::new();
        };
        // Its own words when it refuses — "invalid user data: Invalid max_size" is the difference between a
        // bad payload and an unreachable host, and guessing between them wastes an evening.
        if out.status != "success" || out.encrypted_str.is_empty() {
            log_limited(
                "mint-mediafusion-refused",
                &format!("mediafusion refused the config: {} {}", out.status, out.message.trim()),
            );
            return String::new();
        }
        format!("{base}/{}", out.encrypted_str)
    })
    .await;
    answer.unwrap_or_default()
}

/// The answer of MediaFusion's `/encrypt-user-data`.
#[derive(Deserialize, Default)]
#[serde(default)]
struct MediaFusionMint {
    encrypted_str: String,
    status: String,
    message: String,
}

/// Go `scrapeUserAgent` (scrape.go), the user agent every indexer and minting request is sent with.
pub(crate) const SCRAPE_USER_AGENT: &str =
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36";

/// Reads at most `limit` bytes of a response body. None when the body cannot be read.
pub(crate) async fn read_capped(mut resp: reqwest::Response, limit: usize) -> Option<Vec<u8>> {
    let mut buf = Vec::new();
    while buf.len() < limit {
        match resp.chunk().await {
            Ok(Some(chunk)) => {
                let room = limit - buf.len();
                buf.extend_from_slice(&chunk[..chunk.len().min(room)]);
            }
            Ok(None) => break,
            Err(_) => return None,
        }
    }
    Some(buf)
}

/// Whether a cached or fresh minting failure was transient (Go's `mintedConfig.transient`, set only for
/// mediafusion, the one minter that reaches the network).
fn is_transient_failure(indexer: &Indexer, url: &str) -> bool {
    indexer.0 == "mediafusion" && url.is_empty()
}

/// Go `indexerBaseWithConfig` returns a ready-to-use base URL for an indexer that needs a config segment, or
/// "" when one cannot be built. Cached per (indexer, account) so a burst of episode requests mints once.
/// The second result reports that a FAILURE was transient — see `is_transient_failure`. It is meaningless
/// when a URL was returned.
pub(crate) async fn indexer_base_with_config(
    indexer: &Indexer,
    cfg: &Config,
    client: Option<&dyn Doer>,
) -> (String, bool) {
    // nothing to mint from, and no retry changes that
    let Some(acct) = primary_debrid(Some(cfg)) else { return (String::new(), false) };
    let key = format!("{}:{}", indexer.0, key_hash(&format!("{}:{}", acct.service.as_str(), acct.token)));

    {
        let mut guard = lock(&MINTED);
        let map = guard.get_or_insert_with(HashMap::new);
        if let Some(m) = map.get_mut(&key) {
            if Instant::now().duration_since(m.at) < m.ttl() {
                // Touched on read, which is what makes eviction least-recently-USED rather than oldest-minted.
                // The TTL still runs on `at`, so this cannot keep a stale config alive.
                m.used = Instant::now();
                return (m.url.clone(), is_transient_failure(indexer, &m.url));
            }
        }
    }

    // Coalesced: the lock above is released before the round trip, so a burst of concurrent list builds
    // each found the cache cold and each POSTed. Twelve at once meant twelve mints of the identical
    // config, all of them ahead of the scrape on the user-visible path.
    let url = MINT_FLIGHT
        .run(&key, async {
            match indexer.0.as_str() {
                "comet" => mint_comet_url(&acct),
                "mediafusion" => mint_media_fusion_url(&acct, client).await,
                _ => String::new(),
            }
        })
        .await;

    // Remember the FAILURE too, briefly. Only successes were cached, so while the host was unhealthy every
    // single stream request re-POSTed to it — with its own 10 s timeout, ahead of the scrape — meaning the
    // worse it was, the harder scout hit it. The opposite of what a limiter is for.
    let now = Instant::now();
    {
        let mut guard = lock(&MINTED);
        let map = guard.get_or_insert_with(HashMap::new);
        prune_minted_locked(map);
        map.insert(key, MintedConfig { url: url.clone(), at: now, used: now });
    }
    if url.is_empty() {
        return (String::new(), is_transient_failure(indexer, &url));
    }
    log_limited(
        &format!("mint:{}", indexer.0),
        &format!("minted a config for the {} indexer from the {} account", indexer.0, acct.service.as_str()),
    );
    (url, false)
}

/// Go `unaskableScraper` stands in for an indexer that could not be asked at all. `transient` says whether
/// that is an outage (counts against the quorum) or a permanent lack of configuration (excused from it).
pub(crate) struct UnaskableScraper {
    pub(crate) indexer: Indexer,
    pub(crate) transient: bool,
}

#[async_trait]
impl Scraper for UnaskableScraper {
    fn id(&self) -> Indexer {
        self.indexer.clone()
    }

    async fn scrape(&self, _q: &ScrapeQuery) -> Result<Vec<RawStream>, DoError> {
        Err(format!("{}: no config URL, not asked", self.indexer.0).into())
    }
}

/// Locks a module-level cache. A poisoned lock still holds a valid map, so the cache is used as it is.
fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(PoisonError::into_inner)
}
