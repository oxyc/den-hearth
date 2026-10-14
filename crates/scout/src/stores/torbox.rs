//! Port of the TorBox half of `internal/scout/stores.go` (lines 446-3770 less the refusal helpers in
//! `refusal.rs`): `torBoxStore`, its check-cached batcher, account listing, status, cancel and file
//! selection. Owned by the "Stores I" work package.
//!
//! Context: Go threads a `context.Context` through every call. Here a call's deadline is the caller's
//! `tokio::time::timeout` and its cancellation is dropping the future. Work that Go runs detached from the
//! caller's context (the shared status read, the account listing, the check-cached batcher) runs in its own
//! task, for the same reason Go gives: another waiter may still need the answer.

mod listing;
pub(crate) mod support;

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::io::Read;
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::sync::{mpsc, oneshot, Notify, Semaphore, SemaphorePermit};

use super::{
    CancelOutcome, ResolveTarget, StatusAnswer, Store, StoreError, StoreStatus, FETCH_QUEUED, FETCH_STALLED,
    MAX_LISTING_BYTES, MAX_STORE_BYTES,
};
use crate::addbudget::{budget_account, global_add_budget, refund_add, spend_add, RequestNotSent};
use crate::cache::{Cache, CacheClass};
use crate::config::DebridService;
use crate::linkcheck::size_of;
use crate::loglimit::log_limited;
use crate::metrics::metrics;
use crate::scrape::{DoError, Doer};
use crate::season::{
    ambiguous_episode_guess, is_feature_sized, largest_episode_candidate, largest_playable,
    pick_episode_file, TorrentFile,
};
use crate::stores::refusal::{
    account_backed_off, add_in_flight, add_outcome_unknown, add_still_believable, backed_off, batches_failed,
    cache_or_none, canonical_torbox_identity, clear, is_cancellation, is_torrent_gone, known_cached, live,
    note_add_attempt, note_unknown_outcome, record_refusal, record_refusal_for, refusal_is_about_the_account,
    remember_cached, resolve_key, settle_add_attempt, torbox_account_identity, torrent_id_key,
    torrent_miss_key, transport_error, transport_kind, unknown_outcome, TORRENT_MISS_TTL,
};
use crate::util::{go_to_lower, key_hash, Shape, LEAF};

use super::{
    build_request, dead_link, encode_form, join_all, magnet_for, read_limited, read_store_error,
    redact_token, short_hash, store_error_text, store_refused_us, torbox_fetch_state as fetch_state,
    unavailable,
};
use crate::util::{scoped, DetachedFlight};
pub(crate) use listing::{decode_listing, ListingFault, TruncationDetector};
use support::{
    envelope, first_value, int_or_zero, lock, no_file_list, opt_bool, opt_float, opt_int, opt_string,
    string_or_empty, torrent_gone, would_add, GaugeGuard,
};

/// Go `torboxAPI`.
pub const TORBOX_API: &str = "https://api.torbox.app/v1/api";
/// Go `resolveCacheTTL`: how long a remembered torrent id, file list or ownership mark lives.
pub(crate) const RESOLVE_CACHE_TTL: Duration = Duration::from_secs(6 * 60 * 60);

/// Go `maxTorboxOutbound`: the most TorBox calls in flight at once.
///
/// One process-wide ceiling for every TorBox request, including the microbatch overflow path. The
/// coordinators bound how much work they retain, but their old overflow fallback called the API directly:
/// once the demand channels filled, the exact traffic spike they were meant to absorb became unbounded
/// outbound concurrency. Waiting here preserves truth (a caller that runs out of context gets "unknown",
/// never a fabricated miss) while preventing an outage from being amplified by more sockets and requests.
pub(crate) const MAX_TORBOX_OUTBOUND: usize = 64;
/// Go `cacheBatch`: TorBox hashes per checkcached call.
pub(crate) const CACHE_BATCH: usize = 100;
/// Go `maxListingEntries` bounds what the listing RETAINS, which the byte cap does not: the map holds one
/// entry per torrent, and 60 MiB of minimal entries is roughly a million of them. Fifty thousand is an
/// order of magnitude past the largest account this package has ever measured.
pub(crate) const MAX_LISTING_ENTRIES: usize = 50_000;

/// Go `torboxDemandWindow`, `maxTorboxBatchers`, `maxTorboxDemands`.
const DEMAND_WINDOW: Duration = Duration::from_millis(7);
const MAX_BATCHERS: usize = 64;
const MAX_DEMANDS: usize = 64;
/// The shared deadline of work that outlives any one caller (`context.WithTimeout(context.Background(), 15s)`).
const SHARED_DEADLINE: Duration = Duration::from_secs(15);

/// Go `torboxStatusTTL`, `torboxUnknownStatusTTL`.
///
/// Status polling is shared by every viewer waiting on the same account torrent. A five-second snapshot
/// turns the old one-request-per-two-second-poll shape (30 TorBox calls/minute/viewer) into at most twelve
/// calls/minute/target, independent of viewer count. The flight closes the cold/stale race; the bounded
/// cache owns retention, so there is no per-target worker or timer left behind after readers disappear.
const STATUS_TTL: Duration = Duration::from_secs(5);
const UNKNOWN_STATUS_TTL: Duration = Duration::from_secs(2);
/// Go `torboxStatusSlots`: how many uncached status reads may be running at once.
const STATUS_SLOTS: usize = 128;

/// Go `accountSummaryTTL` — how long /<config>/account's answer stands before asking TorBox again. Paced like
/// validate: this is a Downloads-shelf header a viewer may glance at a few times, not the poll loop
/// checkcached/mylist/status share, so a few seconds of staleness costs nothing and 30s keeps the account
/// read off repeated renders of the same screen.
const ACCOUNT_SUMMARY_TTL: Duration = Duration::from_secs(30);

/// Go `listingTTL` — how long one account listing answers for. The same fifteen seconds torrentMissKey
/// already imposes, and deliberately so: a miss is remembered for that long anyway, so memoising the
/// listing adds no staleness that was not already there. A torrent this process just added is known by id
/// directly (resolveHeldTorrent writes torrentIDKey), so it never needs the list to find itself.
const LISTING_TTL: Duration = Duration::from_secs(15);

/// Go `maxListingMemoEntries` bounds the memo by TORRENT, summed across accounts — not by account.
///
/// A per-account ceiling is the mistake byte budgets exist to correct, and cache.go's own header records
/// it: an entry count says nothing about size when the entries differ by four orders of magnitude. Each
/// account may hold up to maxListingEntries, measured at 4.0 MiB live for 50,000, so a 32-ACCOUNT ceiling
/// admits 127 MiB — 55% of GOMEMLIMIT, and held outside the 48 MiB cache budget that used to bound this
/// when it was a string in the LRU.
///
/// 100,000 torrents is about 8 MiB by that measurement. It admits two accounts at the absolute per-account
/// cap, or fifty at the largest size this package has actually measured (2,000), which is far past any
/// real install: buildStores keys accounts by SERVICE, so one config contributes exactly one TorBox
/// account here — not the eight maxDebridAccounts allows — and only a token TorBox answered a listing for
/// can reach this map at all.
///
/// Two limits of the torrent count as the bound, both measured:
///
/// A torrent count does not price the per-entry overhead, ~440 bytes of map header and key. At the extreme
/// — 100,000 accounts of one torrent each — the same ceiling admits 42 MiB rather than 8. That needs
/// 100,000 tokens TorBox answers for, so it is a clause about the shape of the bound, not a risk.
///
/// Eviction is oldest-first, which is the worst possible policy for a cyclic access pattern: past the
/// ceiling, the entry evicted is always the one asked for next, so the hit rate does not degrade — it goes
/// straight from 100% to 0%. The cliff is at ONE torrent over the ceiling, measured, not at any percentage
/// of it; a coarse sweep makes it look like a margin and there is none.
///
/// The condition is total torrents across every TorBox account the process has seen, not a few large ones:
/// the memo is keyed on (cache, API+token) and the cache is process-wide, so fifty-one ordinary users at the
/// 2,000 this package has actually measured trips it just as well as three 34,000-torrent accounts. Fifty
/// at 2,000 is exactly the ceiling and still hits 100%; the boundary is that sharp. That is all still past
/// what a self-hosted install is, which is why the policy stays simple — but the thing to watch is the
/// account COUNT, and oldest-first is the policy to replace when it moves. Random eviction degrades to
/// roughly capacity-over-working-set instead of to nothing; LRU does not, because a round-robin scan evicts
/// the next entry needed under LRU too.
///
/// What "0%" costs is not "every poll re-pulls every listing": torrentID answers most polls from
/// torrentIDKey or the 15 s torrentMissKey without reaching the listing at all, and the flight collapses
/// concurrent misses. The measured law is one pull per DISTINCT INFOHASH per TTL window when the probes are
/// SEQUENTIAL — an eight-release poster grid goes from 1 fetch to 8, and a repeat of a hash already
/// resolved still costs nothing. Concurrently it is far less: the same eight measured 2 pulls, because the
/// flight collapses whatever overlaps.
const MAX_LISTING_MEMO_ENTRIES: usize = 100_000;

/// Go `torBoxStore`.
#[derive(Clone)]
pub struct TorBoxStore {
    pub token: String,
    pub client: Arc<dyn Doer>,
    pub cache: Option<Arc<dyn Cache>>,
    pub api: String,
}

/// Go `torboxCheckResult`: one checkcached round's answer, and whether it is one at all.
#[derive(Clone, Default)]
pub(crate) struct TorboxCheckResult {
    pub(crate) cached: HashMap<String, bool>,
    pub(crate) ok: bool,
}

// --- process-wide state (Go package variables) ---

/// Go `torboxOutboundSlots`.
static OUTBOUND: Semaphore = Semaphore::const_new(MAX_TORBOX_OUTBOUND);
/// Go `torboxStatusSlots`.
static STATUS_SLOT_POOL: Semaphore = Semaphore::const_new(STATUS_SLOTS);
/// Go `torboxCheckFlights`, `torboxStatusFlights`, `listingFlight`.
///
/// listingFlight collapses concurrent fetches of one account's listing into a single round trip. The cache
/// alone only helps SEQUENTIAL callers, and the case this exists for — a poster grid probing eight releases at
/// once — is anything but sequential.
static CHECK_FLIGHTS: DetachedFlight<TorboxCheckResult> = DetachedFlight::new("torbox check-cached round");
static STATUS_FLIGHTS: DetachedFlight<CachedStatus> = DetachedFlight::new("torbox status read");
static LISTING_FLIGHTS: DetachedFlight<Option<Arc<HashMap<String, i64>>>> =
    DetachedFlight::new("account listing fetch");
/// Go `torboxBatchers`.
static BATCHERS: Mutex<BTreeMap<String, Arc<Batcher>>> = Mutex::new(BTreeMap::new());
/// Go `listingMemo` with `listingMemoMu`. See `LISTING_MEMO`'s notes on `memo_key`.
static LISTING_MEMO: Mutex<BTreeMap<(usize, String), ListingMemoEntry>> = Mutex::new(BTreeMap::new());

// --- the store ---

/// A response whose outbound slot is held until it is dropped.
///
/// A request is still consuming an upstream connection while its body is being read. Releasing at headers
/// would bound only dial/header latency and let slow or hostile bodies accumulate without a ceiling. Every
/// caller already drops its response when it is done; the slot is coupled to that lifetime (Go
/// `torboxSlotBody`).
struct Reply {
    resp: reqwest::Response,
    _slot: SemaphorePermit<'static>,
}

impl Reply {
    fn status(&self) -> u16 {
        self.resp.status().as_u16()
    }

    /// The body up to `limit` bytes.
    async fn read(&mut self, limit: usize) -> Result<Vec<u8>, reqwest::Error> {
        read_limited(&mut self.resp, limit).await
    }

    /// Go `json.NewDecoder(io.LimitReader(resp.Body, maxStoreBytes)).Decode(&body)`: the first JSON value of the
    /// body, with keys matched to `shape` case-insensitively. None where Go returns an error.
    async fn json(&mut self, shape: &Shape) -> Option<Value> {
        let raw = self.read(MAX_STORE_BYTES).await.ok()?;
        first_value(&raw, shape)
    }

    /// The service's own explanation of a failure, redacted: " (detail)".
    async fn detail(&mut self, token: &str) -> String {
        redact_token(&read_store_error(&mut self.resp).await, token)
    }
}

/// Counts one TorBox call by endpoint and outcome when it ends, however it ends.
///
/// Go counts the call beside the request (`metrics.torboxCall(endpoint, err == nil && …)` after `s.do`), and a
/// cancelled context makes `do` return an error, whether the caller was still waiting for an outbound slot or for
/// the response. A call its caller walked away from is therefore an `error` in the counters. Cancellation here is
/// dropping the future, which would otherwise leave the call uncounted.
struct CallCount {
    endpoint: &'static str,
    ok: bool,
}

impl Drop for CallCount {
    fn drop(&mut self) {
        metrics().torbox_call(self.endpoint, self.ok);
    }
}

/// What an add that createtorrent answered with a failure leaves behind when its caller goes away before the code
/// that files the failure has run: the charge handed back (`refundUnusedAdd`) and the refusal `Resolve` writes for
/// the error `addMagnet` returns. Armed across the one await between the answer and that code.
struct AbandonedAnswer<'a> {
    store: &'a TorBoxStore,
    info_hash: &'a str,
    err: StoreError,
    armed: bool,
}

impl Drop for AbandonedAnswer<'_> {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        let identity = self.store.account_identity();
        refund_unused_add(&identity);
        record_refusal(self.store.cache(), DebridService::TorBox, &identity, self.info_hash, &self.err);
    }
}

/// What `CacheCheck` remembers for a caller that goes away before every batch has come back: the held releases it
/// already knew and those the finished batches reported, which Go writes after `g.Wait()` returns on the cancelled
/// context. Armed from the first batch until they have all been joined.
struct AbandonedCacheCheck<'a> {
    store: &'a TorBoxStore,
    known: &'a HashMap<String, bool>,
    hashes: &'a [String],
    answered: &'a Mutex<Vec<Option<HashMap<String, bool>>>>,
    armed: bool,
}

impl Drop for AbandonedCacheCheck<'_> {
    fn drop(&mut self) {
        if self.armed {
            let answers = lock(self.answered).clone();
            self.store.settle_cache_check(self.known.clone(), self.hashes, &answers);
        }
    }
}

/// The torrent id `resolveHeldTorrent` remembers unconditionally, written for it when its caller goes away while the
/// pack's file list is being read: the id (with the status snapshot cleared) is what Go's flow leaves behind once the
/// cancelled read has come back as "no answer". Armed across that one await.
struct UnwrittenId<'a> {
    store: &'a TorBoxStore,
    info_hash: &'a str,
    torrent_id: i64,
    armed: bool,
}

impl Drop for UnwrittenId<'_> {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        if let Some(cache) = &self.store.cache {
            let identity = self.store.account_identity();
            cache.put(
                &torrent_id_key(&identity, self.info_hash),
                &self.torrent_id.to_string(),
                RESOLVE_CACHE_TTL,
            );
            clear_torbox_status(cache.as_ref(), &identity, self.info_hash);
        }
    }
}

impl TorBoxStore {
    /// A store's cache, or the one that holds nothing where Go has a nil `Cache`.
    fn cache(&self) -> &dyn Cache {
        cache_or_none(&self.cache)
    }

    /// Go `torBoxStore.accountIdentity`: the cache identity of this token on this API endpoint.
    pub(crate) fn account_identity(&self) -> String {
        torbox_account_identity(&self.api, &self.token)
    }

    /// Go `torBoxStore.get`.
    async fn get(&self, u: &str) -> Result<Reply, DoError> {
        let auth = format!("Bearer {}", self.token);
        let req = build_request(
            reqwest::Method::GET,
            u,
            &[("authorization", &auth), ("accept", "application/json")],
            None,
        )
        .map_err(|e| DoError::from(e.to_string()))?;
        self.send(req).await
    }

    /// Go `torBoxStore.do`: every TorBox request waits for an outbound slot first. The call is counted by endpoint
    /// and outcome (Go does it beside the request in `get`, `addMagnet` and `controlRequest`; it is the same count).
    async fn send(&self, req: reqwest::Request) -> Result<Reply, DoError> {
        let mut count = CallCount { endpoint: torbox_endpoint(req.url()), ok: false };
        let slot = OUTBOUND.acquire().await.map_err(|_| DoError::from("the outbound limiter is closed"))?;
        let resp = self.client.do_request(req).await;
        count.ok = resp.as_ref().is_ok_and(|r| r.status().is_success());
        Ok(Reply { resp: resp?, _slot: slot })
    }

    /// Go `torBoxStore.post`: a form POST, authorised by header.
    async fn post(&self, path: &str, form: &[(&str, &str)]) -> Result<Reply, DoError> {
        let auth = format!("Bearer {}", self.token);
        let req = build_request(
            reqwest::Method::POST,
            &format!("{}{path}", self.api),
            &[("authorization", &auth), ("content-type", "application/x-www-form-urlencoded")],
            Some(encode_form(form)),
        )
        .map_err(|e| DoError::from(e.to_string()))?;
        self.send(req).await
    }

    /// Go `torBoxStore.fetchCached`: one checkcached round, bounded by the outbound limiter.
    pub(crate) async fn fetch_cached(&self, hashes: &[String]) -> TorboxCheckResult {
        let u = format!(
            "{}/torrents/checkcached?hash={}&format=object&list_files=false",
            self.api,
            hashes.join(",")
        );
        let Ok(mut reply) = self.get(&u).await else {
            return TorboxCheckResult::default();
        };
        if reply.status() != 200 {
            return TorboxCheckResult::default();
        }
        static SHAPE: Shape = Shape::Object(&[("data", &LEAF)]);
        let Some(body) = reply.json(&SHAPE).await.and_then(envelope) else {
            return TorboxCheckResult::default();
        };
        // `Data map[string]json.RawMessage`: an object, or null. Anything else is a decode error.
        let data = match body.get("data") {
            None | Some(Value::Null) => None,
            Some(Value::Object(m)) => Some(m),
            Some(_) => return TorboxCheckResult::default(),
        };
        let mut answer = HashMap::with_capacity(hashes.len());
        for hash in hashes {
            let held = data.is_some_and(|d| d.contains_key(hash) || d.contains_key(&go_to_upper(hash)));
            answer.insert(hash.clone(), held);
        }
        TorboxCheckResult { cached: answer, ok: true }
    }

    /// What `CacheCheck` makes of the batches that came back. Only a batch that came back describes its hashes. One
    /// that failed leaves them ABSENT from the map, which the pool reads as "we do not know" — writing false there
    /// states that TorBox does not hold them, which is a different and costly claim: it drops a genuinely cached
    /// release from a cached-only list, or pays an add at play time for a torrent the account already had. With
    /// 500 seeds in batches of 100, one timed-out batch made that claim about 100 releases and reported no error
    /// at all. The held releases are remembered whatever came back. Returns the map and which batches answered.
    fn settle_cache_check(
        &self,
        mut result: HashMap<String, bool>,
        hashes: &[String],
        answers: &[Option<HashMap<String, bool>>],
    ) -> (HashMap<String, bool>, Vec<bool>) {
        let mut batch_ok = vec![false; answers.len()];
        for (i, (batch, answer)) in hashes.chunks(CACHE_BATCH).zip(answers).enumerate() {
            let Some(answer) = answer else { continue };
            batch_ok[i] = true;
            for h in batch {
                result.insert(h.clone(), answer.get(&go_to_lower(h)).copied().unwrap_or(false));
            }
        }
        remember_cached(self.cache(), DebridService::TorBox, &self.account_identity(), &result);
        (result, batch_ok)
    }

    /// Go `torBoxStore.checkCachedBatch`.
    pub(crate) async fn check_cached_batch(&self, hashes: &[String]) -> Option<HashMap<String, bool>> {
        let mut set: Vec<String> = hashes.iter().map(|h| go_to_lower(h)).collect();
        set.sort();
        let key = format!("{}:{}", key_hash(&self.account_identity()), key_hash(&set.join(",")));
        let store = self.clone();
        let (answer, shared) =
            CHECK_FLIGHTS.run(&key, move || async move { store.demand_check_cached(set).await }).await;
        if shared {
            metrics().torbox_avoid("checkcached_singleflight");
        }
        let answer = answer?;
        answer.ok.then_some(answer.cached)
    }

    /// Go `torBoxStore.demandCheckCached`: asks for these hashes through the account's batcher, which folds the
    /// demands of every request in a short window into one checkcached call.
    async fn demand_check_cached(&self, hashes: Vec<String>) -> TorboxCheckResult {
        let key = scoped(&key_hash(&self.account_identity()));
        let (reply, answer) = oneshot::channel();
        let mut demand = Some(Demand { hashes: hashes.clone(), reply });
        {
            let mut batchers = lock(&BATCHERS);
            let batcher = match batchers.get(&key) {
                Some(b) => Some(b.clone()),
                None if batchers.len() < MAX_BATCHERS => {
                    let b = Arc::new(Batcher {
                        store: self.clone(),
                        queue: Mutex::new(Vec::new()),
                        full: Notify::new(),
                    });
                    batchers.insert(key.clone(), b.clone());
                    tokio::spawn(b.clone().run(key));
                    Some(b)
                }
                None => None,
            };
            if let (Some(b), Some(d)) = (batcher, demand.take()) {
                // Every sender holds the registry lock while enqueueing, so the coordinator draining under
                // it closes the race with a sender that found the batcher just as its timer fired.
                if let Err(d) = b.try_push(d) {
                    demand = Some(d);
                }
            }
        }
        if demand.is_none() {
            return answer.await.unwrap_or_default();
        }
        // No batcher to join, or its queue was full: ask directly, still bounded by the outbound limiter.
        tokio::time::timeout(SHARED_DEADLINE, self.fetch_cached(&hashes)).await.unwrap_or_default()
    }

    /// Go `torBoxStore.requestDownload`: asks TorBox for the playable link of one file of a torrent.
    pub(crate) async fn request_download(
        &self,
        torrent_id: i64,
        file_id: Option<i64>,
    ) -> Result<String, StoreError> {
        let id = torrent_id.to_string();
        let file = file_id.map(|f| f.to_string());
        let mut q = vec![("token", self.token.as_str()), ("torrent_id", id.as_str())];
        if let Some(file) = &file {
            q.push(("file_id", file.as_str()));
        }
        let u = format!("{}/torrents/requestdl?{}", self.api, encode_form(&q));
        let mut reply = match self.get(&u).await {
            Ok(r) => r,
            // Never the raw error. TorBox wants the token as a QUERY PARAMETER, and a transport failure yields
            // a *url.Error whose message is `Get "<the whole URL>": …` — Go redacts userinfo passwords, not
            // query strings. Both the resolve log and the /play error line print %v of whatever comes back
            // here, so one timeout would have written the live debrid token into a rotated, shipped container
            // log. `docs/SEALED-CONFIG.md` lists "the token is never logged" as an acceptance criterion, and
            // the whole sealing design exists because the URL *is* the credential.
            Err(e) => {
                return Err(dead_link(redact_token(
                    &format!("torbox requestdl transport: {}", transport_kind(Some(e.as_ref()))),
                    &self.token,
                )))
            }
        };
        // 404 is TorBox saying it has no such torrent — the ONE answer that means a remembered id is stale.
        // Everything else says nothing about whether the account still holds it. Checked before the body is
        // read, because it is the one status whose meaning does not depend on the text.
        let status = reply.status();
        if status == 404 {
            return Err(torrent_gone());
        }
        if status != 200 {
            // TorBox explains itself in the body, and an account at its active-download limit and a torrent id
            // it cannot serve are both a bare 400. What the text buys here is a DIAGNOSIS, not a
            // classification: 400 is not in `storeRefusedUs`, so such an answer is still a dead link, still
            // records no backoff, and is still re-asked on every poll. That is deliberate on the read path —
            // a dead link means "no link yet", the ordinary state of a torrent still downloading, and
            // requestdl costs no add quota — but it means the operator has to read the log to tell the two
            // apart, and before this the log did not say.
            //
            // Redacted, because this is the ONE TorBox URL that carries the token as a query parameter and
            // an upstream error page may quote the request URI back. The transport branch above goes to
            // lengths to keep that URL out of the logs; a body spliced into the same error would be a second
            // route to the same place, and `docs/SEALED-CONFIG.md` makes "the token is never logged" an
            // acceptance criterion.
            let detail = reply.detail(&self.token).await;
            if store_refused_us(status) {
                return Err(unavailable(
                    DebridService::TorBox,
                    format!("requestdl http {status}{detail}"),
                    status,
                ));
            }
            return Err(dead_link(format!("torbox requestdl http {status}{detail}")));
        }
        // NOT errTorrentGone. `success:false` is also what a torrent that is still downloading answers, which
        // is the normal state of one that was queued moments ago — treating it as "the account does not have
        // this" forgot the id of a perfectly good fetch in progress. A 404 is the only answer that means the
        // id is stale; this one means "not yet".
        static SHAPE: Shape = Shape::Object(&[("success", &LEAF), ("data", &LEAF)]);
        if let Some(Value::Object(o)) = reply.json(&SHAPE).await {
            if let (Some(Value::Bool(true)), Some(Value::String(link))) = (o.get("success"), o.get("data")) {
                if !link.is_empty() {
                    return Ok(link.clone());
                }
            }
        }
        Err(dead_link("torbox no link"))
    }

    /// Go `torBoxStore.addMagnet` asks TorBox to fetch a hash. It returns either a torrent id (fetching now) or a
    /// queued id (parked behind the account's slot limit, TorBox's `queued` setting on) — never both — plus
    /// whichever `add_only_if_cached` asks for: true restricts the add to a release TorBox already has cached,
    /// so it cannot silently turn into an uncached download that counts against the 60-an-hour cap.
    pub(crate) async fn add_magnet(
        &self,
        info_hash: &str,
        prefetch: bool,
        only_if_cached: bool,
    ) -> Result<(i64, i64), StoreError> {
        let identity = self.account_identity();
        let cache = self.cache();
        // An add we already sent and never heard back about must not be sent again — see addAttemptKey.
        add_in_flight(cache, DebridService::TorBox, &identity, info_hash)?;
        // Go's `addWouldMissTheClock` refuses to CHARGE an add the caller's deadline can no longer carry. Here a
        // spent deadline drops this future, so there is no spent clock to find; its comment is kept for whoever
        // adds a deadline seam:
        //
        // The charge and the in-flight marker are both written before the request is built, and a dead context
        // fails at client.Do, which returns without refunding — deliberately, since a request that may have
        // reached the wire must not be re-sent. So an add attempted on a spent clock costs one of the fifty per
        // hour, sends nothing at all, and leaves a 90-second marker that has the next poll report a download
        // nobody queued. Measured on an expired context: zero upstream requests, one add spent per service.
        //
        // It guards the CHARGE rather than the store, and that distinction is the whole point. An earlier
        // version skipped the whole store from ResolvePreferring when the clock was gone, which also discarded
        // the free cache reads ahead of the charge — addInFlight and the two backoff gates — and so turned a
        // 202 "downloading" into a 503 naming the viewer's debrid as refusing, for a release scout had queued
        // moments earlier. Placed here, the free answers still surface and only the pointless purchase stops.
        // Wrapped in errScoutSide, and that wrap is load-bearing rather than cosmetic. recordRefusal excludes
        // errScoutSide by name, and without it this error was filed as a REFUSAL BY THE STORE: it wraps neither
        // a cancellation nor errScoutSide, and it returns before noteAddAttempt, so both of recordRefusal's
        // guards missed it and TorBox's add-error handler wrote a one-minute refusedKey naming TorBox for a
        // release TorBox had never been asked about. One spent clock then made a healthy release answer 503 on
        // /play AND on ?probe=1 for the next sixty seconds, on a fresh clock, with the store skipped as
        // "backing off". Measured: control 302, treatment 503 twice over.
        //
        // That is the laundering this file warns about twice — at recordRefusal itself and at the RD site that
        // once built a StoreUnavailableError here and hid the cause from isCancellation. It came back because
        // the guard was placed one line ABOVE noteAddAttempt, and it was the marker that used to route this
        // error away from recordRefusal.
        spend_add(DebridService::TorBox, &identity, info_hash, prefetch)?;
        let magnet = magnet_for(info_hash);
        let mut form = vec![("magnet", magnet.as_str()), ("seed", "3"), ("allow_zip", "false")];
        if only_if_cached {
            form.push(("add_only_if_cached", "true"));
        }
        let auth = format!("Bearer {}", self.token);
        let req = match build_request(
            reqwest::Method::POST,
            &format!("{}/torrents/createtorrent", self.api),
            &[("authorization", &auth), ("content-type", "application/x-www-form-urlencoded")],
            Some(encode_form(&form)),
        ) {
            Ok(r) => r,
            Err(e) => {
                refund_add(DebridService::TorBox, &identity, &RequestNotSent);
                return Err(e);
            }
        };
        note_add_attempt(cache, DebridService::TorBox, &identity, info_hash);
        let mut reply = match self.send(req).await {
            Ok(r) => r,
            // No response, so the outcome is genuinely unknown: the marker STAYS, and the next poll finds it
            // rather than sending the same add again.
            Err(e) => return Err(transport_error(e)),
        };
        // Read the body BEFORE settling, because the read is the last thing that can fail: a status line
        // without a body we could read is still an add whose outcome we never saw. The previous commit added
        // the errAddInFlight branch below but left this settle where it was, so TorBox returned "an add is in
        // flight" having just deleted the marker that says so — and since recordRefusal ignores
        // errAddInFlight there was no backoff either, so the next poll charged and sent createtorrent again.
        // Fifty adds, the hour's allowance gone, the client shown 202 "downloading, 0%" throughout.
        let status = reply.status();
        let raw = match reply.read(MAX_STORE_BYTES).await {
            Ok(raw) => raw,
            // Marker left set on purpose: the next poll answers 202 from it rather than buying the torrent
            // again. The charge stays too — the add was written to the wire. A viewer backing out is the cause
            // that starts no give-up clock.
            Err(e) => return Err(unknown_outcome(cache, DebridService::TorBox, &identity, info_hash, &e)),
        };
        drop(reply);
        settle_add_attempt(cache, DebridService::TorBox, &identity, info_hash);
        if !(200..300).contains(&status) {
            // TorBox explains itself in the body — an account at its active-download limit and a malformed
            // magnet are both a bare 400, and only the text says which. Discarding it left the status code as
            // the entire diagnosis, which is how a hard refusal came to look like a slow download.
            //
            // Redacted like the other three detail sites: recordRefusal does not merely log this, it PERSISTS
            // it into the refusal cache, which in production writes through to disk. TorBox's token rides in
            // a header here rather than the URL, so a leak needs the service to echo the header back — but
            // this was the one call site not following the rule, which is reason enough.
            let detail = redact_token(&store_error_text(&raw), &self.token);
            // "Download already queued" (TorBox's DIFF_ISSUE) means this exact hash already has an entry in
            // the account's OWN queue — not a refusal, and not a dead link. Previously this fell straight
            // through to the DeadLinkError below, which the client read as a release nobody could deliver:
            // the add that put it there (very likely scout's own, from before this hash had a queued-id
            // marker, or one whose marker's TTL lapsed) is undiscoverable from createtorrent's error body
            // alone, so the queue's own listing is asked to find it.
            if status == 400 && torbox_already_queued_error(&raw) {
                // The lookup runs on the caller's context in Go, so a caller that goes away during it makes the
                // lookup fail and the add fall through to the answered-failure code below. Dropping this future
                // must leave that state: the charge handed back, and the refusal Resolve files for that answer.
                let mut abandoned = AbandonedAnswer {
                    store: self,
                    info_hash,
                    err: dead_link(format!("torbox createtorrent http {status}{detail}")),
                    armed: true,
                };
                let found = self.find_queued_by_hash(info_hash).await;
                abandoned.armed = false;
                if let Ok((qid, true)) = found {
                    refund_unused_add(&identity);
                    mark_added_by_us(cache, &identity, info_hash);
                    cache.put_class(
                        &torbox_queued_key(&identity, info_hash),
                        &qid.to_string(),
                        RESOLVE_CACHE_TTL,
                        CacheClass::Volatile,
                    );
                    log_limited(
                        "torbox-already-queued",
                        &format!(
                            "torbox: {} already queued (queued_id {qid}) — reporting queued, not a dead link",
                            short_hash(info_hash)
                        ),
                    );
                    return Ok((0, qid));
                }
            }
            // TorBox answered and created nothing, so the charge goes back — the rule RD and Premiumize
            // already follow on their own answered failures. Kept here, a repeatedly polled bad magnet ate
            // this account's hourly allowance where the other two were already immune.
            refund_unused_add(&identity);
            if store_refused_us(status) {
                return Err(unavailable(
                    DebridService::TorBox,
                    format!("createtorrent http {status}{detail}"),
                    status,
                ));
            }
            return Err(dead_link(format!("torbox createtorrent http {status}{detail}")));
        }
        let body = CreateBody::read(&raw);
        let ids = match body.ids() {
            Some(ids) => ids,
            None => {
                // Answered, and created nothing — the same rule as the non-2xx branch above, and the same one RD
                // applies to `added.ID == ""`. Keeping the charge walked the whole hourly allowance in a single
                // sitting: this branch answers a dead link, which is exactly what makes the client fall through
                // to the next candidate, so one bad title spends fifty adds without any poll loop at all, after
                // which every TorBox resolve on the account is 503 scout_busy for the hour.
                //
                // But only when nothing was created is what the ANSWER says. TorBox reports errors as HTTP 200
                // with `success:false`, and a proxy page served as 200 does not parse at all — those two are the
                // cases this branch exists for. A body that parses, says nothing about success and simply
                // carries an id under a name this struct does not know (a string id, a one-element array) may
                // well describe a torrent that WAS created, and refunding there would manufacture allowance
                // against a real add. Unrecognised is not the same as failed.
                // `readable` is the wrong test for that: a string id or a one-element array is VALID JSON that
                // simply does not fit this struct, and either may describe a torrent that exists. Only a body
                // that is not JSON at all — a proxy or gateway page — proves nothing was created.
                if !body.valid || body.success == Some(false) {
                    refund_unused_add(&identity);
                }
                return Err(dead_link("torbox no torrent_id"));
            }
        };
        if let Some(torrent_id) = ids.0 {
            mark_added_by_us(cache, &identity, info_hash);
            return Ok((torrent_id, 0));
        }
        // Queued: remembered so the next poll answers "queued" instead of sending createtorrent again, and so
        // a cancel can find it via /queued/controlqueued rather than /torrents/controltorrent.
        let queued_id = ids.1.unwrap_or(0);
        mark_added_by_us(cache, &identity, info_hash);
        cache.put_class(
            &torbox_queued_key(&identity, info_hash),
            &queued_id.to_string(),
            RESOLVE_CACHE_TTL,
            CacheClass::Volatile,
        );
        Ok((0, queued_id))
    }

    /// Go `torBoxStore.findQueuedByHash` looks up one hash in the account's own queued-torrents list
    /// (getqueued?type=torrent) — the only way to learn a queue entry's id when createtorrent answers
    /// DIFF_ISSUE "already queued" rather than handing one back directly in a success body (see addMagnet), or
    /// when a release was queued before this account had a remembered queued-id marker at all (see
    /// statusAnswerUncached). found=false with Ok means the listing was read and the hash was not in it; Err
    /// means the listing itself could not be read, which the caller must not mistake for "not queued" — the
    /// same distinction findTorrentByHash draws on the torrent listing.
    async fn find_queued_by_hash(&self, info_hash: &str) -> Result<(i64, bool), StoreError> {
        let mut reply = self
            .get(&format!("{}/queued/getqueued?type=torrent&bypass_cache=true", self.api))
            .await
            .map_err(transport_error)?;
        let status = reply.status();
        if status != 200 {
            let detail = reply.detail(&self.token).await;
            return Err(unavailable(
                DebridService::TorBox,
                format!("getqueued http {status}{detail}"),
                status,
            ));
        }
        static ENTRY: Shape = Shape::Object(&[("id", &LEAF), ("hash", &LEAF)]);
        static ENTRIES: Shape = Shape::Array(&ENTRY);
        static SHAPE: Shape = Shape::Object(&[("success", &LEAF), ("data", &ENTRIES)]);
        let unreadable = || unavailable(DebridService::TorBox, "getqueued: unreadable body", 0);
        let obj = reply.json(&SHAPE).await.and_then(envelope).ok_or_else(unreadable)?;
        let success = opt_bool(obj.get("success")).map_err(|()| unreadable())?;
        let data = match obj.get("data") {
            None | Some(Value::Null) => &[][..],
            Some(Value::Array(a)) => a.as_slice(),
            Some(_) => return Err(unreadable()),
        };
        // Every element must decode, as Go's slice of structs does, before any is looked at.
        let mut entries = Vec::with_capacity(data.len());
        for e in data {
            let o = match e {
                Value::Object(o) => Some(o),
                Value::Null => None,
                _ => return Err(unreadable()),
            };
            let id = opt_int(o.and_then(|o| o.get("id"))).map_err(|()| unreadable())?;
            let hash = string_or_empty(o.and_then(|o| o.get("hash"))).map_err(|()| unreadable())?;
            entries.push((id, hash));
        }
        if success == Some(false) {
            return Ok((0, false));
        }
        let needle = go_to_lower(info_hash);
        for (id, hash) in entries {
            if let Some(id) = id {
                if go_to_lower(&hash) == needle {
                    return Ok((id, true));
                }
            }
        }
        Ok((0, false))
    }

    /// Go `torBoxStore.fetchEntry` asks TorBox for one torrent's current mylist entry by id. found=false with Ok is
    /// TorBox saying it no longer has this torrent (success:false, or the id missing from a listing-shaped
    /// answer); Err is a service refusal or an unreadable response, which the caller must not read as "gone" —
    /// the same distinction statusAnswerUncached draws on the same endpoint.
    async fn fetch_entry(&self, torrent_id: i64) -> Result<(EntryState, bool), StoreError> {
        let mut reply = self
            .get(&format!("{}/torrents/mylist?id={torrent_id}&bypass_cache=true", self.api))
            .await
            .map_err(transport_error)?;
        let status = reply.status();
        if status != 200 {
            let detail = reply.detail(&self.token).await;
            return Err(unavailable(DebridService::TorBox, format!("mylist http {status}{detail}"), status));
        }
        static SHAPE: Shape = Shape::Object(&[("success", &LEAF), ("data", &LEAF)]);
        let unreadable = || unavailable(DebridService::TorBox, "mylist: unreadable body", 0);
        let obj = reply.json(&SHAPE).await.and_then(envelope).ok_or_else(unreadable)?;
        if opt_bool(obj.get("success")).map_err(|()| unreadable())? == Some(false) {
            return Ok((EntryState::default(), false));
        }
        match pick_entry(obj.get("data"), torrent_id, EntryState::read) {
            Some(st) => Ok((st, true)),
            None => Ok((EntryState::default(), false)),
        }
    }

    /// Go `torBoxStore.controlRequest` is controltorrent/controlqueued's shared shape: a form POST answered with a
    /// bare success flag. Any non-2xx, or a 2xx carrying success:false, is a service refusal (503) — never
    /// silently swallowed, since a cancel the caller believes happened would leave a torrent running against
    /// the account's slot, and the memo cleared under it.
    async fn control_request(
        &self,
        path: &str,
        metric: &str,
        form: &[(&str, &str)],
    ) -> Result<(), StoreError> {
        let mut reply = match self.post(path, form).await {
            Ok(r) => r,
            Err(e) => {
                return Err(unavailable(
                    DebridService::TorBox,
                    redact_token(&format!("{metric}: {}", transport_kind(Some(e.as_ref()))), &self.token),
                    0,
                ))
            }
        };
        let status = reply.status();
        let Ok(raw) = reply.read(MAX_STORE_BYTES).await else {
            return Err(unavailable(DebridService::TorBox, format!("{metric}: unreadable response"), 0));
        };
        if !(200..300).contains(&status) {
            let detail = redact_token(&store_error_text(&raw), &self.token);
            return Err(unavailable(
                DebridService::TorBox,
                format!("{metric} http {status}{detail}"),
                status,
            ));
        }
        let refused = serde_json::from_slice::<Value>(&raw).ok().is_some_and(|v| {
            v.as_object()
                .and_then(|o| o.iter().find(|(k, _)| go_to_lower(k) == "success"))
                .is_some_and(|(_, s)| matches!(s, Value::Bool(false)))
        });
        if refused {
            let detail = redact_token(&store_error_text(&raw), &self.token);
            return Err(unavailable(DebridService::TorBox, format!("{metric} refused{detail}"), 0));
        }
        Ok(())
    }

    async fn control_torrent(&self, torrent_id: i64, operation: &str) -> Result<(), StoreError> {
        let id = torrent_id.to_string();
        self.control_request(
            "/torrents/controltorrent",
            "controltorrent",
            &[("torrent_id", &id), ("operation", operation)],
        )
        .await
    }

    async fn control_queued(&self, queued_id: i64, operation: &str) -> Result<(), StoreError> {
        let id = queued_id.to_string();
        self.control_request(
            "/queued/controlqueued",
            "controlqueued",
            &[("queued_id", &id), ("type", "torrent"), ("operation", operation)],
        )
        .await
    }

    /// Go `torBoxStore.AccountSummary` answers /<config>/account for a TorBox account. Deliberately NOT built from
    /// accountListing's hash→id map — that map is hardened to retain nothing but a hash and an id (see
    /// fetchAccountListing), by design, because it is read on every poll of a two-second cadence. This route is
    /// read a few times per Downloads-shelf visit, so it pays for two more TorBox reads (user/me, a plain
    /// decode of mylist) rather than widening that hot path's retention to carry a field it does not need.
    pub(crate) async fn account_summary(&self) -> Result<TorboxAccountSummary, StoreError> {
        let key = torbox_account_summary_key(&self.account_identity());
        if let Some(raw) = self.cache.as_ref().and_then(|c| c.get(&key)) {
            if let Ok(sum) = serde_json::from_str::<TorboxAccountSummary>(&raw) {
                return Ok(sum);
            }
        }
        let (plan, cooldown_until, premium_expires_at) = self.fetch_user_me().await?;
        let active = self.count_active_torrents().await?;
        let queued = self.count_queued_torrents().await?;
        let sum = TorboxAccountSummary { plan, cooldown_until, premium_expires_at, active, queued };
        if let Ok(raw) = serde_json::to_string(&sum) {
            self.cache().put_class(&key, &raw, ACCOUNT_SUMMARY_TTL, CacheClass::Volatile);
        }
        Ok(sum)
    }

    /// Go `torBoxStore.fetchUserMe` reads the account-level facts /<config>/account needs beyond what mylist and
    /// the add budget already cover: the plan code and the two account-wide deadlines.
    async fn fetch_user_me(&self) -> Result<(i64, Option<Rfc3339>, Option<Rfc3339>), StoreError> {
        let mut reply = self.get(&format!("{}/user/me", self.api)).await.map_err(transport_error)?;
        let status = reply.status();
        if status != 200 {
            let detail = reply.detail(&self.token).await;
            return Err(unavailable(DebridService::TorBox, format!("user/me http {status}{detail}"), status));
        }
        static DATA: Shape =
            Shape::Object(&[("plan", &LEAF), ("cooldown_until", &LEAF), ("premium_expires_at", &LEAF)]);
        static SHAPE: Shape = Shape::Object(&[("success", &LEAF), ("data", &DATA)]);
        let unreadable = || unavailable(DebridService::TorBox, "user/me: unreadable body", 0);
        let body = reply.json(&SHAPE).await.ok_or_else(unreadable)?;
        let data =
            body.as_object().and_then(|o| o.get("data")).and_then(Value::as_object).ok_or_else(unreadable)?;
        let plan = opt_int(data.get("plan")).map_err(|()| unreadable())?;
        let cooldown = opt_string(data.get("cooldown_until")).map_err(|()| unreadable())?;
        let expires = opt_string(data.get("premium_expires_at")).map_err(|()| unreadable())?;
        Ok((plan.unwrap_or(0), parse_torbox_time(cooldown.as_deref()), parse_torbox_time(expires.as_deref())))
    }

    /// Go `torBoxStore.countActiveTorrents` counts mylist entries that are active and not finished — see
    /// `TorboxAccountSummary`. A plain decode, capped at the listing size: see `account_summary` for why this
    /// does not reuse accountListing's hardened streaming walk.
    async fn count_active_torrents(&self) -> Result<i64, StoreError> {
        let mut reply = self
            .get(&format!("{}/torrents/mylist?bypass_cache=true", self.api))
            .await
            .map_err(transport_error)?;
        let status = reply.status();
        if status != 200 {
            let detail = reply.detail(&self.token).await;
            return Err(unavailable(DebridService::TorBox, format!("mylist http {status}{detail}"), status));
        }
        let unreadable = || unavailable(DebridService::TorBox, "mylist: unreadable body", 0);
        let Ok(raw) = reply.read(MAX_LISTING_BYTES + 1).await else {
            return Err(unreadable());
        };
        if raw.len() > MAX_LISTING_BYTES {
            return Err(unavailable(DebridService::TorBox, "mylist: account listing too large to count", 0));
        }
        static ENTRY: Shape = Shape::Object(&[("active", &LEAF), ("download_finished", &LEAF)]);
        static ENTRIES: Shape = Shape::Array(&ENTRY);
        static SHAPE: Shape = Shape::Object(&[("success", &LEAF), ("data", &ENTRIES)]);
        let mut body = serde_json::from_slice::<Value>(&raw).map_err(|_| unreadable())?;
        crate::util::fold_json_keys(&mut body, &SHAPE);
        let obj = envelope(body).ok_or_else(unreadable)?;
        let success = opt_bool(obj.get("success")).map_err(|()| unreadable())?;
        let data = match obj.get("data") {
            None | Some(Value::Null) => &[][..],
            Some(Value::Array(a)) => a.as_slice(),
            Some(_) => return Err(unreadable()),
        };
        let mut n = 0;
        let mut entries = Vec::with_capacity(data.len());
        for e in data {
            let o = match e {
                Value::Object(o) => Some(o),
                Value::Null => None,
                _ => return Err(unreadable()),
            };
            let active = opt_bool(o.and_then(|o| o.get("active"))).map_err(|()| unreadable())?;
            let finished = opt_bool(o.and_then(|o| o.get("download_finished"))).map_err(|()| unreadable())?;
            entries.push((active, finished));
        }
        if success == Some(false) {
            return Ok(0);
        }
        for (active, finished) in entries {
            if active == Some(true) && finished != Some(true) {
                n += 1;
            }
        }
        Ok(n)
    }

    /// Go `torBoxStore.countQueuedTorrents` counts the account's parked-torrent queue.
    async fn count_queued_torrents(&self) -> Result<i64, StoreError> {
        let mut reply = self
            .get(&format!("{}/queued/getqueued?type=torrent", self.api))
            .await
            .map_err(transport_error)?;
        let status = reply.status();
        if status != 200 {
            let detail = reply.detail(&self.token).await;
            return Err(unavailable(
                DebridService::TorBox,
                format!("getqueued http {status}{detail}"),
                status,
            ));
        }
        static SHAPE: Shape = Shape::Object(&[("success", &LEAF), ("data", &LEAF)]);
        let unreadable = || unavailable(DebridService::TorBox, "getqueued: unreadable body", 0);
        let obj = reply.json(&SHAPE).await.and_then(envelope).ok_or_else(unreadable)?;
        let success = opt_bool(obj.get("success")).map_err(|()| unreadable())?;
        let len = match obj.get("data") {
            None | Some(Value::Null) => 0,
            Some(Value::Array(a)) => a.len() as i64,
            Some(_) => return Err(unreadable()),
        };
        if success == Some(false) {
            return Ok(0);
        }
        Ok(len)
    }

    /// Go `torBoxStore.statusAnswerUncached`.
    async fn status_answer_uncached(&self, t: &ResolveTarget) -> (StoreStatus, StatusAnswer) {
        let identity = self.account_identity();
        // A parked add, the warm case: Resolve already recorded the queued-id marker (from a success body
        // or, now, from addMagnet's DIFF_ISSUE branch), so this is a cache read, not an upstream call. Checked
        // before torrentID below, which only ever finds a TORRENT — asking it alone answered "nothing here"
        // for a release that was waiting in TorBox's own queue, never a torrent at all yet.
        if self.known_queued_id(&t.info_hash).is_some() {
            return (queued_status(), StatusAnswer::Downloading);
        }
        let (torrent_id, ok, authoritative) = self.torrent_id(&t.info_hash).await;
        if !ok {
            // A miss here is only trustworthy when the listing was actually read. torrentID declines to
            // remember one otherwise, and the same doubt has to reach the caller — taken from the lookup that
            // already happened rather than by asking again, which was a second full account fetch.
            if !authoritative {
                return (StoreStatus::default(), StatusAnswer::Unknown);
            }
            // Not an active torrent — but mylist and TorBox's own queue are two different listings, and a
            // release parked in the queue (with no cached marker yet: a cold hash, or one whose marker's TTL
            // lapsed) answers mylist exactly like one TorBox never heard of at all. Without this, the probe
            // route read a genuinely queued download as "not queued" forever, which is what made it drop out
            // of Home's in-flight states once the fallback's earlier release gave up. Found here, the marker
            // is written so the next poll takes the cheap `knownQueuedID` path above instead.
            //
            // Gated by its own remembered miss, same as torrentMissKey above it: a release that is genuinely
            // nowhere — neither listing holds it — must not pay for the queue listing again on every poll of
            // a two-second cadence, which is exactly the cost torrentMissKey already exists to keep off this
            // path for the torrent listing.
            if live(self.cache(), &torbox_queued_miss_key(&identity, &t.info_hash)).is_some() {
                return (StoreStatus::default(), StatusAnswer::No);
            }
            if let Ok((qid, found)) = self.find_queued_by_hash(&t.info_hash).await {
                if found {
                    self.cache().put_class(
                        &torbox_queued_key(&identity, &t.info_hash),
                        &qid.to_string(),
                        RESOLVE_CACHE_TTL,
                        CacheClass::Volatile,
                    );
                    return (queued_status(), StatusAnswer::Downloading);
                }
                self.cache().put_class(
                    &torbox_queued_miss_key(&identity, &t.info_hash),
                    "1",
                    TORRENT_MISS_TTL,
                    CacheClass::Volatile,
                );
            }
            return (StoreStatus::default(), StatusAnswer::No);
        }
        let Ok(mut reply) =
            self.get(&format!("{}/torrents/mylist?id={torrent_id}&bypass_cache=true", self.api)).await
        else {
            return (StoreStatus::default(), StatusAnswer::Unknown);
        };
        if reply.status() != 200 {
            return (StoreStatus::default(), StatusAnswer::Unknown);
        }
        static SHAPE: Shape = Shape::Object(&[("success", &LEAF), ("data", &LEAF)]);
        // a body we could not read is not the account saying no
        let Some(obj) = reply.json(&SHAPE).await.and_then(envelope) else {
            return (StoreStatus::default(), StatusAnswer::Unknown);
        };
        let Ok(success) = opt_bool(obj.get("success")) else {
            return (StoreStatus::default(), StatusAnswer::Unknown);
        };
        // TorBox answers 200 with success:false / data:null for an id it no longer holds, and unmarshalling
        // null into a struct succeeds silently — so both have to be rejected explicitly.
        if success == Some(false) {
            return (StoreStatus::default(), StatusAnswer::No);
        }
        // The ID is what makes an answer about OUR torrent. mylist is asked by id and should reply with one
        // entry, but it can reply with the account list — that is why the array fallback exists at all — and
        // this took arr[0] blind, the very mistake listFiles was hardened against. It has no id field even in
        // principle, so it could not check. A finished arr[0] made a live download read as ok=false, which
        // handlePlay answers 404 dead_link: the client blacklists a release that is downloading right now,
        // which is the single failure Status exists to prevent. A downloading arr[0] is quieter and no better
        // — the viewer watches another torrent's percentage, ETA and rate.
        //
        // An EXACT id wins; an id-less entry is only a fallback — the rule listFiles uses on this very payload.
        // First-of-either let an unlabelled entry ahead of ours answer for it, and here that is the reading
        // that decides "downloading" against "dead": a finished stranger at the head of the array made a live
        // download report nothing. Two readers of one shape must not disagree about it. An entry with no id is
        // taken at its word, since the single-entry answer to a by-id query need not repeat it; one that names
        // a DIFFERENT torrent never is.
        let Some(st) = pick_entry(obj.get("data"), torrent_id, StatusEntry::read) else {
            return (StoreStatus::default(), StatusAnswer::No);
        };
        // Finished but Resolve still failed → not a wait we can promise anything about; reads as dead.
        if st.download_finished == Some(true) {
            return (StoreStatus::default(), StatusAnswer::No);
        }
        // Nothing said about the download at all (an empty object, a deleted torrent) — not a wait either.
        if st.progress.is_none() && st.download_finished.is_none() {
            return (StoreStatus::default(), StatusAnswer::No);
        }
        let mut out = StoreStatus::default();
        if let Some(p) = st.progress {
            out.progress = p;
        }
        if let Some(eta) = st.eta.filter(|eta| *eta > 0) {
            out.eta_seconds = Some(eta);
        }
        // Zero is meaningful here — it is the stall — so only a missing or negative figure is dropped.
        out.bytes_per_second = st.download_speed.filter(|s| *s >= 0);
        if let Some(state) = &st.download_state {
            out.state = fetch_state(state).to_owned();
        }
        out.seeds = st.seeds.filter(|n| *n >= 0);
        out.peers = st.peers.filter(|n| *n >= 0);
        (out, StatusAnswer::Downloading)
    }

    /// Go `torBoxStore.knownTorrentID` answers "does this account already have this torrent?" from cache alone.
    ///
    /// Cache-only on purpose. The resolve entry is a six-hour convenience cache, not a record of what the
    /// account holds, so anything not played within six hours fell through to createtorrent and paid an add for
    /// a torrent already sitting there. The torrent id answers that — and by the time Resolve runs on the /play
    /// and probe paths, Status has already looked it up and cached it, so this costs no request at all. Asking
    /// the account directly here instead would put a full account-list fetch on every cold resolve, which is
    /// the cost torrentMissKey exists to keep off the poll path.
    pub(crate) fn known_torrent_id(&self, info_hash: &str) -> (i64, bool) {
        let Some(raw) = self.cache().get(&torrent_id_key(&self.account_identity(), info_hash)) else {
            return (0, false);
        };
        match raw.parse::<i64>() {
            Ok(id) => (id, true),
            Err(_) => (0, false),
        }
    }

    /// Go `torBoxStore.forgetTorrentID` drops both remembered facts about a torrent the account turns out not to
    /// have, so the next resolve starts from nothing rather than from a stale id.
    fn forget_torrent_id(&self, info_hash: &str) {
        let Some(cache) = &self.cache else { return };
        let identity = self.account_identity();
        cache.put(&torrent_id_key(&identity, info_hash), "", Duration::from_nanos(1));
        cache.put(&resolve_key(&identity, info_hash), "", Duration::from_nanos(1));
        clear_torbox_status(cache.as_ref(), &identity, info_hash);
        clear_added_by_us(cache.as_ref(), &identity, info_hash);
    }

    /// Go `torBoxStore.knownQueuedID` answers "is this account's add for this hash parked in TorBox's queue?" from
    /// cache alone, mirroring knownTorrentID.
    pub(crate) fn known_queued_id(&self, info_hash: &str) -> Option<i64> {
        let raw = self.cache().get(&torbox_queued_key(&self.account_identity(), info_hash))?;
        raw.parse::<i64>().ok()
    }

    /// Go `torBoxStore.forgetQueuedID` drops the queued marker once the add is promoted to a real torrent id or
    /// cancelled.
    fn forget_queued_id(&self, info_hash: &str) {
        if let Some(cache) = &self.cache {
            cache.put(&torbox_queued_key(&self.account_identity(), info_hash), "", Duration::from_nanos(1));
        }
    }

    /// Go `torBoxStore.torrentID` finds the account's torrent id for an infohash: from the cache Resolve wrote,
    /// and failing that by asking TorBox for the account's list and matching on hash.
    ///
    /// The fallback exists because the cached id is the ONLY thing that made a queued release describable, and
    /// it is lost by any of a redeploy, a pruned cache, or an add that recorded nothing. Without it `Status`
    /// answers "nothing here" while the file downloads perfectly well — which a client can only read as a dead
    /// link. Rediscovering the id costs one list call, and only on the path that was previously a dead end.
    /// The third result says whether a MISS is authoritative — the listing was read and the hash was not in
    /// it — as opposed to the listing not having been read at all.
    ///
    /// Returned rather than re-derived. StatusAnswer needs the same fact, and asking findTorrentByHash a second
    /// time for it meant a second real upstream fetch whenever the listing failed fast: measured at four full
    /// account-listing requests per /play against a throttled account, where the whole point of that code is
    /// not to pile load on an account already refusing. It also stepped over the miss marker this function
    /// writes, so a remembered miss re-pulled the whole listing anyway — the one thing the marker exists to
    /// prevent.
    pub(crate) async fn torrent_id(&self, info_hash: &str) -> (i64, bool, bool) {
        let identity = self.account_identity();
        if let (id, true) = self.known_torrent_id(info_hash) {
            return (id, true, true);
        }
        if live(self.cache(), &torrent_miss_key(&identity, info_hash)).is_some() {
            return (0, false, true); // remembered, and only an authoritative miss is ever remembered
        }
        let (id, ok, authoritative) = self.find_torrent_by_hash(info_hash).await;
        if !ok {
            // Remember the miss, or every poll re-fetches the whole account list to learn the same thing —
            // but only when the list was actually READ and the hash was not in it. A timeout or a cancelled
            // poll is not the account saying no, and remembering it suppressed the one call that can
            // rediscover a queued torrent for the next fifteen seconds, on /play as well as the probe route.
            // The probe now runs on an eight-second budget, so this is easier to hit than it was.
            if authoritative {
                self.cache().put_class(
                    &torrent_miss_key(&identity, info_hash),
                    "1",
                    TORRENT_MISS_TTL,
                    CacheClass::Volatile,
                );
            }
            return (0, false, authoritative);
        }
        // Remember it, so the next poll of this wait is a single-id lookup again rather than another list.
        if let Some(cache) = &self.cache {
            cache.put(&torrent_id_key(&identity, info_hash), &id.to_string(), RESOLVE_CACHE_TTL);
        }
        // Finding the torrent settles any add we had in flight for it — that add plainly landed. Without this
        // the marker outlives the fact it stood in for, and a release stays "awaiting the result" long after
        // the result is sitting in the account listing.
        settle_add_attempt(self.cache(), DebridService::TorBox, &identity, info_hash);
        (id, true, true)
    }

    /// Go `torBoxStore.findTorrentByHash` scans the account's torrent list for an infohash. TorBox reports hashes
    /// lower-case; indexers are inconsistent about it, so both sides are folded before comparing. The third
    /// result says whether the ANSWER is authoritative — the list was read and the hash was not in it — as
    /// opposed to the list not having been read at all. Only the first justifies remembering a miss.
    pub(crate) async fn find_torrent_by_hash(&self, info_hash: &str) -> (i64, bool, bool) {
        let (ids, ok) = self.account_listing().await;
        if !ok {
            return (0, false, false);
        }
        match ids.get(&go_to_lower(info_hash)) {
            Some(id) => (*id, true, true),
            None => (0, false, true),
        }
    }

    /// Go `torBoxStore.accountListing` returns the account's hash → torrent-id map, fetching it at most once per
    /// listingTTL.
    ///
    /// The listing is the whole account and it was pulled once per INFOHASH, because the only memo was the
    /// per-hash miss marker. A poster grid probing eight releases made eight identical full-list requests —
    /// 253 KiB on a 300-torrent account, and around 13 MB on a 2,000-torrent one, close enough to the 4 MiB
    /// parse cap that answers would start being dropped rather than merely repeated. The question "what does
    /// this account hold?" has one answer at a time, so it is asked that way.
    ///
    /// The map is shared and read-only: every caller of a memoised listing holds the same one.
    pub(crate) async fn account_listing(&self) -> (Arc<HashMap<String, i64>>, bool) {
        let key = torrent_list_key(&self.account_identity());
        if let Some(ids) = cached_listing(self.cache.as_ref(), &key) {
            return (ids, true);
        }
        // A listing this account will keep failing to produce is memoised, briefly. A transient failure is not.
        //
        // The distinction is the whole point, and it is the FAULT that draws it, not the size: too big to
        // read, too many entries, no usable entry among them, or an envelope carrying no usable array at all.
        // A transient failure — a timeout, a 5xx, a body cut off mid-stream — must be retried at once,
        // because that retry is the only thing able to rediscover a queued torrent;
        // suppressing it is a bug this package has already had and has a test for. None of the four faults is
        // transient: the body was that shape a moment ago and will be again, so re-pulling it is guaranteed
        // waste. Without this an oversized account re-pulled the whole body on every attempt, and a single
        // /play makes up to three status reads while a client polls it every two seconds — tens of megabytes
        // of egress per request, sustained for the length of a wait, to reach the same answer each time.
        //
        // Still not the same thing as torrentMissKey, which the code below deliberately refuses to write here:
        // this says "do not re-pull the listing yet", not "the account does not hold it". Callers get ok=false,
        // which stays indeterminate, so nothing concludes the release is absent.
        let unanswered = || (Arc::new(HashMap::new()), false);
        if live(self.cache(), &format!("{key}:oversized")).is_some() {
            return unanswered();
        }
        // A detached flight, and the wait is the caller's own: singleflight is context-blind, and that broke
        // this both ways.
        //
        // A follower blocked on the leader's WaitGroup with its own deadline unobserved: measured at a caller
        // with a 100 ms budget returning after 851 ms. On this route that is not a latency nit — a /play poll
        // read is capped at statusBudget precisely so a wait answers promptly, and joining a flight led by the
        // 2x escalated read blew that cap to twice what the poll test asserts, on a URL a client hits every
        // two seconds. Here a caller waits for its OWN budget (its timeout drops this wait) and no longer.
        //
        // And the fetch ran on whichever caller happened to arrive first, so that caller going away killed the
        // listing every other caller was waiting on: measured at a follower with ten seconds left inheriting a
        // leader's 120 ms failure. The fetch runs in a task of its own, on a deadline of its own.
        //
        // What is left, named rather than hidden: a follower with MORE budget than the leader still inherits
        // the leader's shorter attempt, so an escalated read that joins a poll's in-flight fetch can be told
        // "could not find out" while it still had time. That answer is indeterminate, so it costs a retry
        // rather than a false "nobody is fetching it" — and the flight is per token, so it needs a second
        // concurrent request against the same API+token account in the window between the two reads.
        //
        // A panic in the fetch is abandoned rather than fatal, and every waiter is answered "could not find
        // out" (Go's `recoverBackground`). Go's own words for it: this closure is a BACKGROUND goroutine now, and
        // DoChan makes a panic in it fatal to the process: singleflight re-raises it with `go panic(e)` on a
        // fresh goroutine whenever anyone is waiting on a channel, where nothing can recover it. Under the
        // blocking Do it unwound into the caller's stack and the handler's own recover turned it into a 500.
        // Moving to DoChan quietly converted a recoverable panic into a dead container, which is the rule
        // probe_fanout.go's recoverBackground was written for; abandoning the fetch is already a first-class
        // outcome here, so a panic joins it.
        //
        // Go builds the fetch's context INSIDE the closure, so its cancel is tied to the lifetime of the fetch
        // rather than to the leader's own call: constructing it outside and deferring the cancel there undid the
        // detachment completely, since the deferred cancel fires when the leader's caller returns, which on the
        // abandonment path is exactly when the fetch still needs to be running for everyone else — measured as
        // the joiner inheriting "context canceled", the very failure this is here to prevent. Go's fetch also
        // keeps the LEADER'S DEADLINE (`context.WithDeadline`); the port has no context to read one from, so
        // the fetch runs on the same fifteen seconds as the other shared reads.
        //
        // Go also looks once more at the flight's answer when the caller's own budget runs out in the same
        // instant the answer lands, because select picks at random when both are ready and the answer is a fact
        // about the account that does not go stale because the asker ran out of clock. Measured 50.1% lost over
        // 20,000 trials, and recovered only for the case ready at the moment the select is evaluated. Here the
        // caller's budget is a timeout around this whole call, which polls the answer before the clock.
        let store = self.clone();
        let memo_key = key.clone();
        let (answer, _) =
            LISTING_FLIGHTS
                .run(&key, move || async move {
                    // A fetch past the shared deadline is no answer: empty, not ok, not a persistent fault.
                    let (ids, ok, persistent) =
                        tokio::time::timeout(SHARED_DEADLINE, store.fetch_account_listing())
                            .await
                            .unwrap_or((HashMap::new(), false, false));
                    if !ok {
                        if persistent {
                            store.cache().put_class(
                                &format!("{memo_key}:oversized"),
                                "1",
                                LISTING_TTL,
                                CacheClass::Volatile,
                            );
                        }
                        return None;
                    }
                    let ids = Arc::new(ids);
                    put_cached_listing(store.cache.as_ref(), &memo_key, ids.clone());
                    Some(ids)
                })
                .await;
        match answer.flatten() {
            Some(ids) => (ids, true),
            None => unanswered(),
        }
    }

    /// Go `torBoxStore.fetchAccountListing`. The third result says whether this failure is worth REMEMBERING: too
    /// big to read, too many entries, no usable entry among them, or a complete envelope with no usable data
    /// array. Each is a property of the listing rather than a blip, so each will fail the same way on the next
    /// poll and retrying it immediately is guaranteed waste — where retrying a timeout is the only way a queued
    /// torrent is ever rediscovered.
    pub(crate) async fn fetch_account_listing(&self) -> (HashMap<String, i64>, bool, bool) {
        let failed = |persistent: bool| (HashMap::new(), false, persistent);
        let Ok(mut reply) = self.get(&format!("{}/torrents/mylist?bypass_cache=true", self.api)).await else {
            return failed(false);
        };
        if reply.status() != 200 {
            return failed(false);
        }
        // The ACCOUNT LISTING gets its own, much larger cap, and it is the only read that does.
        //
        // 4 MiB truncates a real large account: ~13 MB at 2,000 torrents was measured. They are dropped — the
        // decode fails in milliseconds, `Status` answers "nobody is fetching it", and /play queues a second
        // copy of a torrent already downloading. A SIZE failure, not a slow one, so no timeout-based guard can
        // see it.
        //
        // Decoded ELEMENT BY ELEMENT rather than into one big value, which is what makes the larger cap safe:
        // the body is fed to the decoder a chunk at a time, so only the hash→id map (sized by the torrent
        // count, ~100 KB at 2,000, not by the body) and a few chunks are held. Measured in Go, a 60 MiB
        // listing allocates 2.7 MiB in total and peaks 20 MiB above baseline, against 128.9 MiB for the
        // buffering decode this replaced. The read is singleflighted and memoised for listingTTL, but per
        // TOKEN — two accounts do not collapse into one — so the per-call ceiling is what has to be small.
        // limit+1 so "read exactly the cap" and "cut off at the cap" stop being the same observation: a body
        // of exactly maxListingBytes parsed perfectly would otherwise be discarded AND remembered as oversized
        // for the listing TTL.
        let (tx, rx) = mpsc::channel::<Vec<u8>>(4);
        let decode = tokio::task::spawn_blocking(move || {
            let reader = ChunkReader { rx, current: Vec::new(), pos: 0 };
            let mut limited =
                TruncationDetector { r: reader.take(MAX_LISTING_BYTES as u64 + 1), limit: MAX_LISTING_BYTES };
            let (ids, ok, fault) = decode_listing(&mut limited);
            (ids, ok, fault, limited.truncated())
        });
        // Feed it until it has what it needs: a decode that has finished (or refused) drops its end, and the
        // rest of the body is not read.
        while let Ok(Some(chunk)) = reply.resp.chunk().await {
            if tx.send(chunk.to_vec()).await.is_err() {
                break;
            }
        }
        drop(tx);
        let Ok((ids, ok, fault, truncated)) = decode.await else {
            return failed(false);
        };
        if fault != ListingFault::None {
            // Every fault takes the same road as the byte cap: each is a property of this listing rather than a
            // blip, so each will fail the same way on the next poll, and each is therefore remembered. Only
            // the byte one was at first, and tripping the entry cap re-pulled the whole body every poll —
            // 14.9 MiB per /play on a two-second cadence, which is the egress this memo exists to stop.
            //
            // The unusable-entries fault was added later returning "not persistent", which put it straight
            // back on that road: measured at 15 listing fetches over a 15-poll wait against 1, and two per
            // /play rather than none, because an indeterminate answer also makes every poll escalate.
            match fault {
                ListingFault::TooManyEntries => log_limited(
                    "torbox-listing-too-many",
                    &format!(
                        "torbox account listing holds more than {MAX_LISTING_ENTRIES} torrents — treating it as no \
                         answer rather than as an empty account"
                    ),
                ),
                ListingFault::NoUsableEntries => log_limited(
                    "torbox-listing-unusable",
                    "torbox account listing had entries but no usable infohash among them — treating it as no \
                     answer rather than as an empty account; the upstream may have renamed the field",
                ),
                ListingFault::BadEnvelope => log_limited(
                    "torbox-listing-shape",
                    "torbox account listing was not a shape we can read — no usable data array, or a field of an \
                     unexpected type — treating it as no answer rather than as an empty account; the upstream \
                     schema may have changed",
                ),
                ListingFault::None => {}
            }
            return failed(true);
        }
        if truncated {
            // Silent truncation is the failure this whole comment is about: it reads back as "the account
            // holds nothing" and costs a duplicate add, with nothing anywhere saying the account simply
            // outgrew the cap. Raising the cap moved that cliff rather than removing it, so the case still
            // has to be named — and remembered, so it is not re-pulled on every poll.
            log_limited(
                "torbox-listing-truncated",
                &format!(
                    "torbox account listing exceeded {MAX_LISTING_BYTES} bytes and was truncated — treating it as no \
                     answer rather than as an empty account"
                ),
            );
            return failed(true);
        }
        (ids, ok, false)
    }

    /// Go `torBoxStore.listFiles` returns the pack's files, or says WHY it cannot. The bare nil it used to answer
    /// stood for four different facts — a transport blip, a non-200, an unreadable body, and TorBox's own "no
    /// such torrent" — and every caller had to guess between them.
    pub(crate) async fn list_files(&self, torrent_id: i64) -> Result<Vec<TorrentFile>, StoreError> {
        let mut reply = match self
            .get(&format!("{}/torrents/mylist?id={torrent_id}&bypass_cache=true", self.api))
            .await
        {
            Ok(r) => r,
            Err(e) => {
                // Logged per CAUSE, not per torrent: errNoFileList itself is one shared identity for several
                // branches below, by design (every caller treats them alike), which also means every caller's
                // own log line reads "mylist gave no usable answer" with no way to tell a transport blip from a
                // shape TorBox has never sent before. A Fauda S1E3 install hit this repeatedly with nothing
                // more specific anywhere in the log; these lines are what distinguishes the branch next time.
                log_limited(
                    "mylist-transport",
                    &format!("torbox mylist {torrent_id}: {}", transport_kind(Some(e.as_ref()))),
                );
                return Err(no_file_list());
            }
        };
        let status = reply.status();
        if status == 404 {
            return Err(torrent_gone());
        }
        // Being turned away is a fact about the ACCOUNT, and errNoFileList carries Status 0 — so a key TorBox
        // has rejected, discovered on this endpoint, could never raise the account-wide backoff that exists
        // to stop every later request asking with the same dead key.
        if store_refused_us(status) {
            return Err(unavailable(DebridService::TorBox, format!("mylist http {status}"), status));
        }
        if status != 200 {
            log_limited("mylist-status", &format!("torbox mylist {torrent_id}: http {status}"));
            return Err(no_file_list());
        }
        static SHAPE: Shape = Shape::Object(&[("success", &LEAF), ("data", &LEAF)]);
        let Some(body) = reply.json(&SHAPE).await.and_then(envelope) else {
            log_limited("mylist-decode", &format!("torbox mylist {torrent_id}: unreadable body"));
            return Err(no_file_list());
        };
        let Ok(success) = opt_bool(body.get("success")) else {
            log_limited("mylist-decode", &format!("torbox mylist {torrent_id}: unreadable body"));
            return Err(no_file_list());
        };
        // TorBox answers 200 with success:false / data:null for an id it no longer holds — the same shape
        // Status already rejects explicitly, and unmarshalling null into a struct succeeds silently. This is
        // mylist ANSWERING, so it is `gone`, not `no answer`: the caller must forget the id and re-add rather
        // than report the store unavailable forever. (On requestdl the same success:false means the opposite
        // — "no link yet" — which is why that endpoint keys the verdict on a 404 instead.)
        let data = body.get("data");
        if success == Some(false) || matches!(data, Some(Value::Null)) {
            return Err(torrent_gone());
        }
        // A body carrying no `data` key AT ALL is not the documented envelope — a proxy or CDN page that
        // happens to be valid JSON, say. That is silence, not a claim about the account, and reading it as
        // `gone` paid an add to re-buy a torrent nobody said was missing. The two shapes that really do say
        // so are handled above, so nothing is lost by being careful here.
        let Some(data) = data else {
            log_limited("mylist-empty", &format!("torbox mylist {torrent_id}: no data field"));
            return Err(no_file_list());
        };
        let data = fold_entry_keys(data.clone(), &LIST_FILES_ENTRY, &LIST_FILES_ENTRIES);
        // The object form gets the same test as the array form below. An entry naming a different torrent is
        // not an answer about ours, whichever shape it arrives in; one carrying no id is taken at its word,
        // since a single-entry answer to a by-id query need not repeat it.
        let entry = match ListEntry::read(&data) {
            Some(e) => {
                if e.id != 0 && e.id != torrent_id {
                    log_limited(
                        "mylist-other-id",
                        &format!("torbox mylist {torrent_id}: answer named torrent {}", e.id),
                    );
                    return Err(no_file_list());
                }
                e
            }
            None => {
                // Two different facts, and folding them together reproduced the wedge the errTorrentGone split
                // was written to remove. A payload that will not parse is no answer. An EMPTY ARRAY is TorBox
                // answering: `data:[]` is the array form's only way to say "I hold nothing for this id",
                // exactly as `data:null` is the object form's — and this fallback exists at all because mylist
                // does reply with arrays. Read as "no answer" it kept the stale id, re-stamped its six hours on
                // every poll, and never re-added, so the series path wedged at a permanent 503 while movies
                // still healed.
                let arr: Option<Vec<ListEntry>> = match &data {
                    Value::Array(items) => items.iter().map(ListEntry::read_or_null).collect(),
                    _ => None,
                };
                let Some(arr) = arr else {
                    log_limited(
                        "mylist-shape",
                        &format!("torbox mylist {torrent_id}: answer is neither an object nor an array"),
                    );
                    return Err(no_file_list());
                };
                if arr.is_empty() {
                    return Err(torrent_gone());
                }
                // The entry for the id we ASKED about, not whichever came first. mylist is queried by id and
                // so should answer with one, but taking arr[0] blind means a multi-entry answer picks a file
                // id out of another torrent's file list — the wrong-episode failure, by a route the name-match
                // cannot catch, since the names it matches would be the other torrent's.
                //
                // Checked at ANY length. Guarding only `len(arr) > 1` left the one-element case blind, so a
                // single foreign entry still handed back its files — and Status, which parses this same
                // payload, is stricter in exactly that spot. Two readers of one shape should not disagree
                // about it. An EXACT id wins, and an unlabelled entry is only a fallback. Taking the first of
                // either meant an id-less entry ahead of ours in a multi-entry answer was chosen over the entry
                // that actually names our torrent — worse than the blind arr[0] this replaced, since it looks
                // like a check.
                let mut m = None;
                let mut unlabelled = None;
                for (i, cand) in arr.iter().enumerate() {
                    if cand.id == torrent_id {
                        m = Some(i);
                        break;
                    }
                    if cand.id == 0 && unlabelled.is_none() {
                        unlabelled = Some(i);
                    }
                }
                let Some(i) = m.or(unlabelled) else {
                    log_limited(
                        "mylist-no-match",
                        &format!("torbox mylist {torrent_id}: {} entries, none named it", arr.len()),
                    );
                    return Err(no_file_list());
                };
                arr.into_iter().nth(i).unwrap_or_default()
            }
        };
        Ok(entry
            .files
            .into_iter()
            .map(|f| TorrentFile {
                index: f.id,
                name: if f.name.is_empty() { f.short_name } else { f.name },
                size_bytes: f.size,
            })
            .collect())
    }

    /// Go `torBoxStore.resolveHeldTorrent` turns a torrent the account HAS into a playable link: list its files if
    /// an episode must be picked, remember what was learned, and mint the download. Shared by the just-added path
    /// and by the one that discovered the torrent was already there — they differ only in how the id was
    /// obtained.
    async fn resolve_held_torrent(
        &self,
        torrent_id: i64,
        key: &str,
        need_files: bool,
        t: &ResolveTarget,
    ) -> Result<String, StoreError> {
        let identity = self.account_identity();
        let mut files: Vec<TorrentFile> = Vec::new();
        if need_files {
            // Go reads the list on the caller's context, so a caller that goes away during it gets errNoFileList
            // and the id write below still happens. Dropping this future must leave the same id behind: it is
            // the id createtorrent just handed us, and losing it buys the torrent again on the next poll.
            let mut unwritten = UnwrittenId { store: self, info_hash: &t.info_hash, torrent_id, armed: true };
            let listed = self.list_files(torrent_id).await;
            unwritten.armed = false;
            match listed {
                Ok(f) => files = f,
                Err(err) if is_torrent_gone(&err) => {
                    // Returned HERE, above the id re-stamp below, and left for the caller to act on: this is
                    // the account saying it no longer holds the torrent, so the id must be forgotten and the
                    // torrent bought again. Stamping it first would refresh a stale id's six hours on every
                    // poll.
                    return Err(err);
                }
                // A REJECTED KEY has to be remembered here, or the classification listFiles makes is thrown
                // away by the only caller that sees it: the error was assigned and then never consulted for any
                // value but errTorrentGone, so a 401 fell through to the status-less errNoFileList below and
                // the account-wide backoff never learned the key was dead. Narrow on purpose — a 5xx is this
                // attempt failing, and letting it back the release off would stop the next poll retrying a
                // list that is very likely to succeed.
                Err(err) if refusal_is_about_the_account(&err) => {
                    // The id first, for the same reason the unconditional write below exists: Status needs it
                    // to report a download in progress. Returning above that write dropped an id
                    // createtorrent had just handed us, so the next poll knew of no torrent and bought another
                    // one.
                    if let Some(cache) = &self.cache {
                        cache.put(
                            &torrent_id_key(&identity, &t.info_hash),
                            &torrent_id.to_string(),
                            RESOLVE_CACHE_TTL,
                        );
                        clear_torbox_status(cache.as_ref(), &identity, &t.info_hash);
                    }
                    record_refusal(self.cache(), DebridService::TorBox, &identity, &t.info_hash, &err);
                    return Err(err);
                }
                Err(_) => {}
            }
        }
        // audit #3: don't cache an empty file list when we needed one (avoids poisoning the pack for 6h).
        if let Some(cache) = &self.cache {
            if !need_files || !files.is_empty() {
                if let Ok(raw) = serde_json::to_string(&ResolveEntry::new(torrent_id, &files)) {
                    cache.put(key, &raw, RESOLVE_CACHE_TTL);
                }
            }
        }
        // The torrent id is remembered separately and unconditionally, because `Status` needs it in exactly
        // the case the entry above refuses to write: a just-queued torrent has no file list yet, and that is
        // what makes an episode's first /play look "dead" instead of "downloading".
        if let Some(cache) = &self.cache {
            cache.put(&torrent_id_key(&identity, &t.info_hash), &torrent_id.to_string(), RESOLVE_CACHE_TTL);
            clear_torbox_status(cache.as_ref(), &identity, &t.info_hash);
        }
        // No list, and an episode to pick out of a pack: refuse rather than guess — but only after the id
        // above is remembered, or a just-queued episode stops reporting as "downloading" and reads as dead.
        //
        // A missing list used to fall straight through to `selectFileID`, where the indexer's raw fileIdx is
        // used as a TorBox file id — the exact disagreement the name-match exists to correct. A blip on
        // mylist therefore served S01E01 for a request for S01E02, with a 302 and no error: silently the
        // wrong episode, and nothing downstream can tell. The cache-read path already refuses on the same
        // evidence (`!needFiles || len(e.Files) > 0`); this is the live path's equivalent. Naming TorBox is
        // accurate — it is the one that did not answer — so the pool moves on to a store that can, and /play
        // answers 503 or, for a torrent still fetching, the 202 `Status` produces from the id just written.
        //
        // Only "no answer" reaches here. `listFiles` returns errTorrentGone above for the account saying it no
        // longer holds the id, which heals by re-adding; answering both alike wedged every episode of a
        // deleted pack at a permanent 503, since the id was re-stamped on each poll and nothing re-added.
        if need_files && files.is_empty() {
            return Err(no_file_list());
        }
        let file_id = select_file_id(&files, t)?;
        let link = self.request_download(torrent_id, file_id).await;
        // ONLY a service refusal is remembered here. The add path deliberately records dead links too — a
        // TorBox account at its download limit answers 400, so keying on the error TYPE missed the one case
        // that mattered — but that reasoning does not carry to the read path, where a dead link means "no link
        // yet", the ordinary state of a torrent still downloading. And requestDownload flattens every
        // transport failure into a DeadLinkError on purpose, to keep the token out of the logs, which destroys
        // the classification isCancellation needs: a cancelled poll was being filed as TorBox refusing.
        // Recorded broadly, this told the viewer their debrid was refusing — and stopped the client trying
        // other sources — for a torrent that was merely not ready, and let a read-only probe create a backoff
        // its own contract says it cannot cause.
        if let Err(err @ StoreError::Unavailable(_)) = &link {
            // The same rule as the warm path in Resolve: this is reached BEFORE Resolve's NoAdd return, so a
            // probe poll landed here and wrote the add-path backoff its own contract says it cannot cause.
            record_refusal_for(self.cache(), DebridService::TorBox, &identity, &t.info_hash, err, t.no_add);
        }
        if matches!(&link, Err(e) if is_torrent_gone(e)) {
            // The id we were just handed is not one TorBox has. Undo the two writes above: leaving them
            // re-stamped a stale id with a fresh six-hour TTL on every poll, so a polling client kept its own
            // poison alive and the release stayed unplayable for as long as it kept asking.
            self.forget_torrent_id(&t.info_hash);
        }
        link
    }
}

// --- the checkcached batcher ---

/// Go `torboxCheckDemand`.
struct Demand {
    hashes: Vec<String>,
    reply: oneshot::Sender<TorboxCheckResult>,
}

/// Go `torboxCheckBatcher`: the demands of one account within a window, answered by one pass.
struct Batcher {
    store: TorBoxStore,
    queue: Mutex<Vec<Demand>>,
    /// Signalled when the queue reaches `MAX_DEMANDS`, which ends the window early.
    full: Notify,
}

impl Batcher {
    /// Enqueues a demand if there is room (Go's non-blocking channel send).
    fn try_push(&self, d: Demand) -> Result<(), Demand> {
        let mut queue = lock(&self.queue);
        if queue.len() >= MAX_DEMANDS {
            return Err(d);
        }
        queue.push(d);
        if queue.len() == MAX_DEMANDS {
            self.full.notify_one();
        }
        Ok(())
    }

    /// Go `torboxCheckBatcher.run`.
    async fn run(self: Arc<Self>, key: String) {
        // If this task ends any other way than by answering (a panic), the batcher is unpublished and its
        // demands dropped, so their callers stop waiting instead of queueing behind a coordinator that is gone.
        let _retire = Retire { key: key.clone(), batcher: self.clone() };
        let _ = tokio::time::timeout(DEMAND_WINDOW, self.full.notified()).await;
        // A sender may have found this batcher just as the timer fired. Every sender holds the registry lock
        // while enqueueing, so draining under it closes that race before the coordinator is unpublished.
        let demands = {
            let mut batchers = lock(&BATCHERS);
            if batchers.get(&key).is_some_and(|b| Arc::ptr_eq(b, &self)) {
                batchers.remove(&key);
            }
            std::mem::take(&mut *lock(&self.queue))
        };

        let union: BTreeSet<&String> = demands.iter().flat_map(|d| d.hashes.iter()).collect();
        let all: Vec<String> = union.into_iter().cloned().collect();
        // Every call the demands would have made on their own, less the ones this pass makes, was saved by folding
        // them together.
        let avoided = demands.len().saturating_sub(all.len().div_ceil(CACHE_BATCH));
        for _ in 0..avoided {
            metrics().torbox_avoid("checkcached_batch");
        }
        let deadline = tokio::time::Instant::now() + SHARED_DEADLINE;
        let mut answers: HashMap<String, bool> = HashMap::with_capacity(all.len());
        for batch in all.chunks(CACHE_BATCH) {
            let Ok(answer) = tokio::time::timeout_at(deadline, self.store.fetch_cached(batch)).await else {
                continue;
            };
            if !answer.ok {
                continue;
            }
            answers.extend(answer.cached);
        }
        for demand in demands {
            let mut answer = HashMap::with_capacity(demand.hashes.len());
            let mut ok = true;
            for hash in &demand.hashes {
                match answers.get(hash) {
                    Some(cached) => {
                        answer.insert(hash.clone(), *cached);
                    }
                    None => ok = false,
                }
            }
            let _ = demand.reply.send(TorboxCheckResult { cached: answer, ok });
        }
    }
}

/// Unpublishes a batcher and drops what it still holds when its coordinator ends.
struct Retire {
    key: String,
    batcher: Arc<Batcher>,
}

impl Drop for Retire {
    fn drop(&mut self) {
        let mut batchers = lock(&BATCHERS);
        if batchers.get(&self.key).is_some_and(|b| Arc::ptr_eq(b, &self.batcher)) {
            batchers.remove(&self.key);
        }
        lock(&self.batcher.queue).clear();
    }
}

/// Go `torboxEndpoint`: which of the fixed debrid endpoints a request is for, for the call counters. Anything
/// else is "", which the counters ignore, so a caller cannot grow the series.
fn torbox_endpoint(u: &reqwest::Url) -> &'static str {
    let path = u.path();
    if path.ends_with("/torrents/checkcached") {
        "checkcached"
    } else if path.ends_with("/torrents/mylist") && u.query_pairs().any(|(k, v)| k == "id" && !v.is_empty()) {
        "mylist_id"
    } else if path.ends_with("/torrents/mylist") {
        "mylist_account"
    } else if path.ends_with("/torrents/requestdl") {
        "requestdl"
    } else if path.ends_with("/torrents/createtorrent") {
        "createtorrent"
    } else if path.ends_with("/torrents/controltorrent") {
        "controltorrent"
    } else if path.ends_with("/queued/controlqueued") {
        "controlqueued"
    } else if path.ends_with("/queued/getqueued") {
        "getqueued"
    } else if path.ends_with("/user/me") {
        "user"
    } else {
        ""
    }
}

/// Go `strings.ToUpper` for a hash: simple per-character upper-casing.
fn go_to_upper(s: &str) -> String {
    s.chars().map(|c| c.to_uppercase().next().unwrap_or(c)).collect()
}

/// Feeds the blocking listing decode from the async body reader.
struct ChunkReader {
    rx: mpsc::Receiver<Vec<u8>>,
    current: Vec<u8>,
    pos: usize,
}

impl Read for ChunkReader {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        loop {
            if self.pos < self.current.len() {
                let n = buf.len().min(self.current.len() - self.pos);
                buf[..n].copy_from_slice(&self.current[self.pos..self.pos + n]);
                self.pos += n;
                return Ok(n);
            }
            match self.rx.blocking_recv() {
                Some(chunk) => {
                    self.current = chunk;
                    self.pos = 0;
                }
                None => return Ok(0),
            }
        }
    }
}

// --- keys ---

/// Go `torboxStatusKey`.
pub(crate) fn torbox_status_key(identity: &str, info_hash: &str) -> String {
    format!("torbox:status:{}:{}", key_hash(&canonical_torbox_identity(identity)), go_to_lower(info_hash))
}

/// Go `clearTorboxStatus`.
pub(crate) fn clear_torbox_status(cache: &dyn Cache, identity: &str, info_hash: &str) {
    clear(cache, &torbox_status_key(identity, info_hash));
}

/// Go `torboxAddedKey` records that THIS process is the one that asked TorBox to fetch a hash, as opposed to
/// discovering a hold the account already had (a torrent the owner added by hand, or one from before this scout
/// existed). Cancel reads it to refuse deleting a torrent scout never added — the owner may still want it — and
/// it is written only at the moment a fresh add (or a queued add TorBox later promotes) resolves to a real
/// torrent id, never on the "already held" fast path that merely discovers one.
pub(crate) fn torbox_added_key(identity: &str, info_hash: &str) -> String {
    format!("torbox:addedbyus:{}:{}", key_hash(&canonical_torbox_identity(identity)), go_to_lower(info_hash))
}

pub(crate) fn mark_added_by_us(cache: &dyn Cache, identity: &str, info_hash: &str) {
    cache.put_class(&torbox_added_key(identity, info_hash), "1", RESOLVE_CACHE_TTL, CacheClass::Volatile);
}

/// Go `wasAddedByUs`: whether scout itself added this hash under this account, so a cancel may touch it.
pub(crate) fn was_added_by_us(cache: &dyn Cache, identity: &str, info_hash: &str) -> bool {
    live(cache, &torbox_added_key(identity, info_hash)).is_some()
}

pub(crate) fn clear_added_by_us(cache: &dyn Cache, identity: &str, info_hash: &str) {
    clear(cache, &torbox_added_key(identity, info_hash));
}

/// Go `torboxQueuedKey` remembers a `queued_id` createtorrent handed back instead of a torrent_id: the add
/// landed in TorBox's own queue (the account was at its slot limit), not yet a fetchable torrent. Without this,
/// the next poll finds no torrent id and no "add in flight" marker (the add already settled — it succeeded) and
/// sends createtorrent again, once per poll, until the hourly allowance is gone.
pub(crate) fn torbox_queued_key(identity: &str, info_hash: &str) -> String {
    format!("torbox:queuedid:{}:{}", key_hash(&canonical_torbox_identity(identity)), go_to_lower(info_hash))
}

/// Go `torboxQueuedMissKey` remembers that findQueuedByHash read the account's queue listing and the hash was
/// not in it — the queue-listing equivalent of torrentMissKey, and gated by the same short TTL for the same
/// reason: a release that is genuinely nowhere must not pay for a fresh getqueued on every poll of a two-second
/// cadence.
pub(crate) fn torbox_queued_miss_key(identity: &str, info_hash: &str) -> String {
    format!("torbox:noqueue:{}:{}", key_hash(&canonical_torbox_identity(identity)), go_to_lower(info_hash))
}

/// Go `torrentListKey` — the account's hash → torrent-id map. Account-scoped like every other key here.
pub(crate) fn torrent_list_key(identity: &str) -> String {
    format!("torbox:list:{}", key_hash(&canonical_torbox_identity(identity)))
}

fn torbox_account_summary_key(identity: &str) -> String {
    format!("torbox:accountsummary:{}", key_hash(&canonical_torbox_identity(identity)))
}

/// Go `refundUnusedAdd` gives a charge back when the request turned out not to be an add at all.
fn refund_unused_add(identity: &str) {
    refund_add(DebridService::TorBox, identity, &RequestNotSent);
}

// --- the listing memo ---

/// Go `listingMemoEntry`.
struct ListingMemoEntry {
    ids: Arc<HashMap<String, i64>>,
    at: Instant,
    /// Keeps the cache alive while its address is the memo's key, so a freed cache's address cannot be taken by
    /// a new one and inherit its listing. Go's map key holds the interface value, which does the same.
    _cache: Arc<dyn Cache>,
}

/// Go `listingMemo` holds the PARSED listing, in process, rather than JSON in the shared cache.
///
/// The shared Cache is string-valued, so every hit re-ran json.Unmarshal and handed each caller a private
/// copy of the map: measured at 14 ms and 9.1 MB per hit on a 50,000-entry account, and 26.5 MB held
/// simultaneously across the eight concurrent probes a poster grid makes — 11% of GOMEMLIMIT to answer one
/// grid, for a listing that had already been fetched once. maxListingEntries reads as if it bounded this, and
/// it does not: the real bound was entries times concurrent callers, and nothing capped the second factor. It
/// also went through the TIERED cache, so the whole listing was written to disk every listingTTL, on a
/// container whose only writable storage is that volume.
///
/// The map is published READ-ONLY and shared by every caller. Nothing may write to it after it lands here; the
/// one consumer, findTorrentByHash, only looks entries up.
///
/// Scoped to the CACHE as well as the account, which is where this memo lived before and therefore the
/// isolation everything already assumes: two handlers built with different caches do not read each other's
/// memo, and neither do two tests. Keyed on the cache's address.
///
/// Not a claim that two such handlers share nothing: the listing flight is keyed on the API+token identity, so
/// concurrent fetches for one account still collapse into one round trip across caches. That is correct — the
/// endpoint and token together identify the account, and the answer does not depend on who asked — and it is
/// only the memo that is cache-scoped, so it is worth saying rather than implying otherwise.
///
/// Go's `memoisable` reports whether a Cache can be a map key at all. Comparable() is a STATIC property of the
/// type and is not sufficient on its own: a struct holding an interface field is comparable as a type and panics
/// as a value when that field holds a slice or a map. Since the whole point is to avoid a panic in a request
/// handler, such a value is actually tried. A POINTER never needs that. It hashes by address whatever it points
/// at, so it cannot panic, and every Cache in this repo is one — the probe therefore costs nothing on any real
/// path (measured: 1.3 ns for a pointer against 27.6 ns for the probe, next to 25.8 ns for the whole memo
/// lookup it guards). An `Arc`'s address always can be a key, so the port has no such check and no memo-less
/// cache.
fn memo_key(cache: &Arc<dyn Cache>, key: &str) -> (usize, String) {
    (Arc::as_ptr(cache) as *const () as usize, key.to_owned())
}

/// Go `cachedListing`.
fn cached_listing(cache: Option<&Arc<dyn Cache>>, key: &str) -> Option<Arc<HashMap<String, i64>>> {
    let cache = cache?;
    let mut memo = lock(&LISTING_MEMO);
    let k = memo_key(cache, key);
    let e = memo.get(&k)?;
    if e.at.elapsed() >= LISTING_TTL {
        // Dropped here rather than left for the next put. An account nobody asks about again otherwise stays
        // resident until some OTHER account is fetched, which for a single-account install is never.
        memo.remove(&k);
        return None;
    }
    Some(e.ids.clone())
}

/// Go `putCachedListing`.
fn put_cached_listing(cache: Option<&Arc<dyn Cache>>, key: &str, ids: Arc<HashMap<String, i64>>) {
    let Some(cache) = cache else { return };
    let mut memo = lock(&LISTING_MEMO);
    // The entry being REPLACED is dropped before the total is taken, or it is counted twice — once in held and
    // again as len(ids) — and a refresh of one account evicts another that fits perfectly well.
    let k = memo_key(cache, key);
    memo.remove(&k);
    let mut held = 0;
    memo.retain(|_, e| {
        if e.at.elapsed() >= LISTING_TTL {
            return false;
        }
        held += e.ids.len();
        true
    });
    // Expiry alone cannot bound a caller-keyed map — the same reasoning as pruneMintedLocked, which this
    // deliberately mirrors. Oldest first, since every entry lives the same fifteen seconds and there is no
    // separate last-used clock to prefer.
    while held + ids.len() > MAX_LISTING_MEMO_ENTRIES {
        let Some(oldest) = memo.iter().min_by_key(|(_, e)| e.at).map(|(k, _)| k.clone()) else {
            break;
        };
        if let Some(e) = memo.remove(&oldest) {
            held -= e.ids.len();
        }
    }
    memo.insert(k, ListingMemoEntry { ids, at: Instant::now(), _cache: cache.clone() });
}

// --- JSON shapes ---

/// Go `createtorrent`'s answer, read the way `json.Unmarshal` into
/// `{Success *bool; Data *struct{TorrentID, QueuedID *int}}` reads it, including how much of it survives a type
/// error: Go leaves the fields that did decode.
struct CreateBody {
    /// `json.Valid(raw)`.
    valid: bool,
    success: Option<bool>,
    /// The decode reported an error (a field of the wrong type).
    type_error: bool,
    data: Option<(Option<i64>, Option<i64>)>,
}

impl CreateBody {
    fn read(raw: &[u8]) -> CreateBody {
        static DATA: Shape = Shape::Object(&[("torrent_id", &LEAF), ("queued_id", &LEAF)]);
        static SHAPE: Shape = Shape::Object(&[("success", &LEAF), ("data", &DATA)]);
        let mut out = CreateBody { valid: false, success: None, type_error: false, data: None };
        let Ok(mut value) = serde_json::from_slice::<Value>(raw) else {
            return out;
        };
        out.valid = true;
        crate::util::fold_json_keys(&mut value, &SHAPE);
        let obj = match &value {
            Value::Object(o) => o,
            Value::Null => return out,
            _ => {
                out.type_error = true;
                return out;
            }
        };
        match obj.get("success") {
            None | Some(Value::Null) => {}
            Some(Value::Bool(b)) => out.success = Some(*b),
            Some(_) => out.type_error = true,
        }
        match obj.get("data") {
            None | Some(Value::Null) => {}
            Some(Value::Object(d)) => {
                let mut ids = (None, None);
                match opt_int(d.get("torrent_id")) {
                    Ok(id) => ids.0 = id,
                    Err(()) => out.type_error = true,
                }
                // QueuedID is set instead of TorrentID when the account is at its slot limit and TorBox's
                // `queued` setting parks the add rather than refusing it outright.
                match opt_int(d.get("queued_id")) {
                    Ok(id) => ids.1 = id,
                    Err(()) => out.type_error = true,
                }
                out.data = Some(ids);
            }
            Some(_) => out.type_error = true,
        }
        out
    }

    /// The ids, when the answer carries a usable one.
    fn ids(&self) -> Option<(Option<i64>, Option<i64>)> {
        if self.type_error {
            return None;
        }
        self.data.filter(|(t, q)| t.is_some() || q.is_some())
    }
}

/// Go `torboxAlreadyQueuedError` reports whether createtorrent's error body is TorBox's DIFF_ISSUE for a hash
/// that already has an entry in the account's queue — "Download already queued." — as opposed to any other 400
/// (a malformed magnet, an account past its active-download limit). The error CODE is checked, but is not
/// required on its own: the detail text is matched too, in case TorBox ever answers the same fact under a
/// different code, and either is enough.
fn torbox_already_queued_error(raw: &[u8]) -> bool {
    static SHAPE: Shape = Shape::Object(&[("error", &LEAF), ("detail", &LEAF)]);
    let Ok(mut value) = serde_json::from_slice::<Value>(raw) else {
        return false;
    };
    crate::util::fold_json_keys(&mut value, &SHAPE);
    let Some(obj) = value.as_object() else {
        return false;
    };
    // `Detail string`: a detail of another type is a decode error, and the whole body is then no answer.
    let detail = match obj.get("detail") {
        None | Some(Value::Null) => "",
        Some(Value::String(s)) => s.as_str(),
        Some(_) => return false,
    };
    if let Some(Value::String(code)) = obj.get("error") {
        if code.eq_ignore_ascii_case("DIFF_ISSUE") {
            return true;
        }
    }
    go_to_lower(detail).contains("already queued")
}

/// Go `torboxEntryState` is the subset of one mylist-by-id entry Cancel needs: whether TorBox has finished with
/// the torrent (kept, never cancelled) and, for a reannounce's answer, the swarm it reports now.
#[derive(Default, Clone)]
struct EntryState {
    id: Option<i64>,
    download_finished: Option<bool>,
    download_state: Option<String>,
    seeds: Option<i64>,
    peers: Option<i64>,
}

impl EntryState {
    fn read(v: &Value) -> Option<EntryState> {
        let o = v.as_object()?;
        Some(EntryState {
            id: opt_int(o.get("id")).ok()?,
            download_finished: opt_bool(o.get("download_finished")).ok()?,
            download_state: opt_string(o.get("download_state")).ok()?,
            seeds: opt_int(o.get("seeds")).ok()?,
            peers: opt_int(o.get("peers")).ok()?,
        })
    }
}

/// The entry of a by-id mylist answer that describes torrent `torrent_id`: the entry itself, or its place in a
/// listing-shaped answer. `None` is TorBox having nothing to say about it.
///
/// An EXACT id wins; an id-less entry is only a fallback — the rule listFiles uses on this very payload.
/// `describesOurs` is true for an entry with no id (the single-entry answer to a by-id query need not repeat it)
/// and false for one that names a DIFFERENT torrent.
fn pick_entry<T: HasId>(data: Option<&Value>, torrent_id: i64, read: fn(&Value) -> Option<T>) -> Option<T> {
    // `len(body.Data) == 0` is an absent key; a null decodes cleanly into the zero entry.
    let data = fold_entry_keys(data?.clone(), &ENTRY_STATE_SHAPE_ANY, &ENTRY_STATE_LIST_ANY);
    let direct = if data.is_null() { Some(T::blank()) } else { read(&data) };
    let st = match direct {
        Some(st) => st,
        None => {
            let Value::Array(items) = &data else { return None };
            let arr: Option<Vec<T>> =
                items.iter().map(|e| if e.is_null() { Some(T::blank()) } else { read(e) }).collect();
            let arr = arr?;
            let mut found = None;
            let mut fallback = None;
            for (i, e) in arr.iter().enumerate() {
                match e.id() {
                    Some(id) if id == torrent_id => {
                        found = Some(i);
                        break;
                    }
                    None if fallback.is_none() => fallback = Some(i),
                    _ => {}
                }
            }
            arr.into_iter().nth(found.or(fallback)?)?
        }
    };
    match st.id() {
        Some(id) if id != torrent_id => None,
        _ => Some(st),
    }
}

trait HasId {
    fn id(&self) -> Option<i64>;
    fn blank() -> Self;
}

impl HasId for EntryState {
    fn id(&self) -> Option<i64> {
        self.id
    }

    fn blank() -> Self {
        EntryState::default()
    }
}

/// Go's local `entryState` in `statusAnswerUncached`.
#[derive(Default, Clone)]
struct StatusEntry {
    id: Option<i64>,
    progress: Option<f64>,
    download_finished: Option<bool>,
    eta: Option<i64>,
    download_speed: Option<i64>,
    download_state: Option<String>,
    seeds: Option<i64>,
    peers: Option<i64>,
}

impl StatusEntry {
    fn read(v: &Value) -> Option<StatusEntry> {
        let o = v.as_object()?;
        Some(StatusEntry {
            id: opt_int(o.get("id")).ok()?,
            progress: opt_float(o.get("progress")).ok()?,
            download_finished: opt_bool(o.get("download_finished")).ok()?,
            eta: opt_int(o.get("eta")).ok()?,
            download_speed: opt_int(o.get("download_speed")).ok()?,
            download_state: opt_string(o.get("download_state")).ok()?,
            seeds: opt_int(o.get("seeds")).ok()?,
            peers: opt_int(o.get("peers")).ok()?,
        })
    }
}

impl HasId for StatusEntry {
    fn id(&self) -> Option<i64> {
        self.id
    }

    fn blank() -> Self {
        StatusEntry::default()
    }
}

/// The union of the keys the by-id mylist readers look at, so one fold serves both of them.
static ENTRY_STATE_SHAPE_ANY: Shape = Shape::Object(&[
    ("id", &LEAF),
    ("progress", &LEAF),
    ("download_finished", &LEAF),
    ("eta", &LEAF),
    ("download_speed", &LEAF),
    ("download_state", &LEAF),
    ("seeds", &LEAF),
    ("peers", &LEAF),
]);
static ENTRY_STATE_LIST_ANY: Shape = Shape::Array(&ENTRY_STATE_SHAPE_ANY);

/// Folds an entry's keys whichever of the two shapes mylist answered in.
fn fold_entry_keys(mut data: Value, entry: &Shape, list: &Shape) -> Value {
    let shape = if data.is_array() { list } else { entry };
    crate::util::fold_json_keys(&mut data, shape);
    data
}

static LIST_FILES_FILE: Shape =
    Shape::Object(&[("id", &LEAF), ("name", &LEAF), ("short_name", &LEAF), ("size", &LEAF)]);
static LIST_FILES_FILES: Shape = Shape::Array(&LIST_FILES_FILE);
static LIST_FILES_ENTRY: Shape = Shape::Object(&[("id", &LEAF), ("files", &LIST_FILES_FILES)]);
static LIST_FILES_ENTRIES: Shape = Shape::Array(&LIST_FILES_ENTRY);

/// Go's local `tbFile` in `listFiles`.
#[derive(Default)]
struct ListFile {
    id: i64,
    name: String,
    short_name: String,
    size: Option<i64>,
}

/// Go's local `entry` in `listFiles`.
#[derive(Default)]
struct ListEntry {
    id: i64,
    files: Vec<ListFile>,
}

impl ListEntry {
    /// `json.Unmarshal(data, &entry)` for an object (a null leaves the zero entry). None where Go reports an
    /// error: the wrong shape, or a field of the wrong type.
    fn read(v: &Value) -> Option<ListEntry> {
        let o = match v {
            Value::Object(o) => o,
            _ => return None,
        };
        let mut files = Vec::new();
        match o.get("files") {
            None | Some(Value::Null) => {}
            Some(Value::Array(items)) => {
                for f in items {
                    let f = match f {
                        Value::Object(f) => f,
                        Value::Null => {
                            files.push(ListFile::default());
                            continue;
                        }
                        _ => return None,
                    };
                    files.push(ListFile {
                        id: int_or_zero(f.get("id")).ok()?,
                        name: string_or_empty(f.get("name")).ok()?,
                        short_name: string_or_empty(f.get("short_name")).ok()?,
                        size: opt_int(f.get("size")).ok()?,
                    });
                }
            }
            Some(_) => return None,
        }
        Some(ListEntry { id: int_or_zero(o.get("id")).ok()?, files })
    }

    fn read_or_null(v: &Value) -> Option<ListEntry> {
        if v.is_null() {
            return Some(ListEntry::default());
        }
        ListEntry::read(v)
    }
}

/// Go `torboxResolveEntry`. The cache holds it as JSON in Go's spelling, so a value written by either side reads
/// back on the other.
#[derive(Serialize, Deserialize)]
struct ResolveEntry {
    #[serde(rename = "torrentId", alias = "torrentid", alias = "TorrentID")]
    torrent_id: i64,
    #[serde(default, deserialize_with = "null_default")]
    files: Vec<ResolveFile>,
}

#[derive(Serialize, Deserialize)]
struct ResolveFile {
    #[serde(rename = "Index", alias = "index")]
    index: i64,
    #[serde(rename = "Name", alias = "name")]
    name: String,
    #[serde(rename = "SizeBytes", alias = "sizeBytes", alias = "sizebytes")]
    size_bytes: Option<i64>,
}

fn null_default<'de, D, T>(d: D) -> Result<T, D::Error>
where
    D: serde::Deserializer<'de>,
    T: Deserialize<'de> + Default,
{
    Ok(Option::<T>::deserialize(d)?.unwrap_or_default())
}

impl ResolveEntry {
    fn new(torrent_id: i64, files: &[TorrentFile]) -> ResolveEntry {
        ResolveEntry {
            torrent_id,
            files: files
                .iter()
                .map(|f| ResolveFile { index: f.index, name: f.name.clone(), size_bytes: f.size_bytes })
                .collect(),
        }
    }

    fn files(self) -> Vec<TorrentFile> {
        self.files
            .into_iter()
            .map(|f| TorrentFile { index: f.index, name: f.name, size_bytes: f.size_bytes })
            .collect()
    }
}

// --- status ---

/// Go `cachedStatusAnswer`: a status read and its answer, as the shared snapshot keeps them.
#[derive(Clone, Serialize, Deserialize)]
struct CachedStatus {
    progress: f64,
    eta_seconds: Option<i64>,
    bytes_per_second: Option<i64>,
    state: String,
    seeds: Option<i64>,
    peers: Option<i64>,
    service: Option<String>,
    /// 0 no, 1 downloading, 2 unknown.
    answer: u8,
}

impl CachedStatus {
    fn new(status: StoreStatus, answer: StatusAnswer) -> CachedStatus {
        CachedStatus {
            progress: status.progress,
            eta_seconds: status.eta_seconds,
            bytes_per_second: status.bytes_per_second,
            state: status.state,
            seeds: status.seeds,
            peers: status.peers,
            service: status.service.map(|s| s.as_str().to_owned()),
            answer: match answer {
                StatusAnswer::No => 0,
                StatusAnswer::Downloading => 1,
                StatusAnswer::Unknown => 2,
            },
        }
    }

    fn parts(self) -> (StoreStatus, StatusAnswer) {
        let answer = match self.answer {
            0 => StatusAnswer::No,
            1 => StatusAnswer::Downloading,
            _ => StatusAnswer::Unknown,
        };
        let status = StoreStatus {
            progress: self.progress,
            eta_seconds: self.eta_seconds,
            bytes_per_second: self.bytes_per_second,
            state: self.state,
            seeds: self.seeds,
            peers: self.peers,
            service: self.service.as_deref().and_then(DebridService::parse),
        };
        (status, answer)
    }
}

fn queued_status() -> StoreStatus {
    StoreStatus { state: FETCH_QUEUED.to_owned(), service: Some(DebridService::TorBox), ..Default::default() }
}

// --- the account summary ---

/// Go `torboxAccountSummary` is the account-wide facts GET /<config>/account reports for TorBox.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct TorboxAccountSummary {
    pub(crate) plan: i64,
    pub(crate) cooldown_until: Option<Rfc3339>,
    pub(crate) premium_expires_at: Option<Rfc3339>,
    /// Active is mylist entries with active:true that are not finished — TorBox's own definition of a download
    /// currently holding a slot.
    pub(crate) active: i64,
    /// Queued is the length of the account's queued-torrents list (getqueued?type=torrent).
    pub(crate) queued: i64,
}

/// A time as TorBox wrote it. Go keeps a `*time.Time` with its zone and prints it back as RFC 3339, so the offset
/// is part of the value: "2027-01-02T00:00:00+02:00" does not come back as the same instant in UTC.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct Rfc3339 {
    year: i32,
    month: u8,
    day: u8,
    hour: u8,
    minute: u8,
    second: u8,
    nanos: u32,
    offset_secs: i32,
}

impl Rfc3339 {
    /// Go `time.Parse(time.RFC3339, s)`. A fraction of a second is accepted though the layout names none.
    fn parse(s: &str) -> Option<Rfc3339> {
        let b = s.as_bytes();
        let num = |from: usize, len: usize| -> Option<u32> {
            let digits = b.get(from..from + len)?;
            if !digits.iter().all(u8::is_ascii_digit) {
                return None;
            }
            std::str::from_utf8(digits).ok()?.parse().ok()
        };
        let year = num(0, 4)? as i32;
        let (month, day) = (num(5, 2)?, num(8, 2)?);
        let (hour, minute, second) = (num(11, 2)?, num(14, 2)?, num(17, 2)?);
        if *b.get(4)? != b'-'
            || *b.get(7)? != b'-'
            || *b.get(10)? != b'T'
            || *b.get(13)? != b':'
            || *b.get(16)? != b':'
        {
            return None;
        }
        let mut i = 19;
        let mut nanos = 0u32;
        if matches!(b.get(i), Some(b'.') | Some(b',')) {
            i += 1;
            let start = i;
            while b.get(i).is_some_and(u8::is_ascii_digit) {
                i += 1;
            }
            if i == start {
                return None;
            }
            for (n, d) in b[start..i].iter().take(9).enumerate() {
                nanos += u32::from(d - b'0') * 10u32.pow(8 - n as u32);
            }
        }
        let offset_secs = match b.get(i)? {
            b'Z' if i + 1 == b.len() => 0,
            sign @ (b'+' | b'-') if i + 6 == b.len() && b[i + 3] == b':' => {
                let (h, m) = (num(i + 1, 2)?, num(i + 4, 2)?);
                if h > 23 || m > 59 {
                    return None;
                }
                let secs = (h * 3600 + m * 60) as i32;
                if *sign == b'-' {
                    -secs
                } else {
                    secs
                }
            }
            _ => return None,
        };
        let leap = (year % 4 == 0 && year % 100 != 0) || year % 400 == 0;
        let days = match month {
            1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
            4 | 6 | 9 | 11 => 30,
            2 if leap => 29,
            2 => 28,
            _ => return None,
        };
        if day < 1 || day > days || hour > 23 || minute > 59 || second > 59 {
            return None;
        }
        Some(Rfc3339 {
            year,
            month: month as u8,
            day: day as u8,
            hour: hour as u8,
            minute: minute as u8,
            second: second as u8,
            nanos,
            offset_secs,
        })
    }

    /// Go's `time.Time.MarshalJSON`: RFC 3339 with the fraction trimmed of trailing zeros, and `Z` for UTC.
    pub(crate) fn to_rfc3339(self) -> String {
        let mut out = format!(
            "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}",
            self.year, self.month, self.day, self.hour, self.minute, self.second
        );
        if self.nanos != 0 {
            let frac = format!("{:09}", self.nanos);
            out.push('.');
            out.push_str(frac.trim_end_matches('0'));
        }
        if self.offset_secs == 0 {
            out.push('Z');
        } else {
            let abs = self.offset_secs.abs();
            out.push_str(&format!(
                "{}{:02}:{:02}",
                if self.offset_secs < 0 { '-' } else { '+' },
                abs / 3600,
                abs % 3600 / 60
            ));
        }
        out
    }
}

/// Go `parseTorboxTime` parses one of TorBox's nullable RFC3339 timestamps. An unparseable or absent value is
/// None rather than an error — a field TorBox adds or reformats later degrades to "unknown", not a failure of
/// the whole read.
fn parse_torbox_time(raw: Option<&str>) -> Option<Rfc3339> {
    match raw {
        None | Some("") => None,
        Some(s) => Rfc3339::parse(s),
    }
}

// --- file selection ---

/// Go `selectFileID` picks TorBox's own file id for the requested file. For a series episode it name-matches the
/// pack first (most reliable); a fileIdx is a POSITION in the torrent's file list, so it's mapped to TorBox's file
/// id via the loaded list. Without a file list (single-file fast path / list failure) the raw fileIdx is passed
/// through best-effort — TorBox ignores file_id for a single-file torrent.
/// A pack that demonstrably holds other episodes and not this one is refused rather than substituted for: the
/// indexer's fileIdx is a position in a list we can now see does not contain the episode, so trusting it would
/// only pick the wrong file by a different route.
pub(crate) fn select_file_id(files: &[TorrentFile], t: &ResolveTarget) -> Result<Option<i64>, StoreError> {
    if let (Some(season), Some(episode)) = (t.season, t.episode) {
        if let Some(id) = pick_episode_file(files, season, episode)? {
            return Ok(Some(id));
        }
    }
    if let Some(idx) = t.file_idx {
        if idx >= 0 && (idx as u64) < files.len() as u64 {
            let picked = &files[idx as usize];
            // A movie's position can land on the wrong file when TorBox lists the torrent in another order.
            // The feature is a video and the size of one, so a position naming anything else — an extra, a
            // sample, an .nfo — gives way to the largest video. A collection's films are alike in size, so
            // the position still picks between them.
            if t.season.is_none() && !is_feature_sized(files, picked) {
                if let Some(feature) = largest_playable(files) {
                    return Ok(Some(feature.index));
                }
            }
            return Ok(Some(picked.index));
        }
        return Ok(Some(idx));
    }
    // Below the indexer's position, above the last-resort largest: a number that named several files did not
    // tell them apart, but on a dual-quality pack those several ARE the episode and the better copy wins.
    // Falling straight through to "largest video anywhere" served episode 4 of an eight-file pack for a request
    // for episode 1.
    if let (Some(_), Some(episode)) = (t.season, t.episode) {
        if let Some(id) = ambiguous_episode_guess(files, episode) {
            return Ok(Some(id));
        }
    }
    // An episode was asked for, no filename named one, and the indexer gave no position either: the largest
    // video is the last thing left to go on, and for the common shape — one feature plus a sample — it is
    // right. This used to live inside pickEpisodeFile, where being non-nil made this function return before
    // FileIdx was ever read, so a guess beat a fact. Here it is what it should be: the fallback after the fact
    // rather than instead of it.
    if t.season.is_some() && t.episode.is_some() && !files.is_empty() {
        if let Some(f) = largest_episode_candidate(files) {
            return Ok(Some(f.index));
        }
    }
    Ok(None)
}

// --- the Store ---

#[async_trait]
impl Store for TorBoxStore {
    fn service(&self) -> DebridService {
        DebridService::TorBox
    }

    async fn cache_check(&self, hashes: &[String]) -> Result<HashMap<String, bool>, StoreError> {
        let identity = self.account_identity();
        let (result, hashes) = known_cached(self.cache(), DebridService::TorBox, &identity, hashes);
        if hashes.is_empty() {
            return Ok(result);
        }
        let batches = hashes.len().div_ceil(CACHE_BATCH);
        // Each batch leaves its answer here as it lands, so that a caller that goes away part way still has the
        // batches that came back remembered, as Go's `g.Wait()` and `rememberCached` do after its goroutines have
        // all returned on the cancelled context.
        let answered: Mutex<Vec<Option<HashMap<String, bool>>>> = Mutex::new(vec![None; batches]);
        let mut abandoned = AbandonedCacheCheck {
            store: self,
            known: &result,
            hashes: &hashes,
            answered: &answered,
            armed: true,
        };
        join_all(
            hashes
                .chunks(CACHE_BATCH)
                .enumerate()
                .map(|(i, batch)| {
                    let answered = &answered;
                    async move {
                        let answer = self.check_cached_batch(batch).await;
                        lock(answered)[i] = answer;
                    }
                })
                .collect(),
        )
        .await;
        abandoned.armed = false;
        drop(abandoned);
        let answers = std::mem::take(&mut *lock(&answered));
        let (result, batch_ok) = self.settle_cache_check(result, &hashes, &answers);
        batches_failed(&batch_ok)?;
        Ok(result)
    }

    async fn resolve(&self, t: &ResolveTarget) -> Result<String, StoreError> {
        // List the pack's files for any series episode (even when a fileIdx is present) so we can name-match
        // the episode — Torrentio's fileIdx and TorBox's file ids/order aren't guaranteed to agree.
        // A movie normally resolves without a list: nearly every one carries fileIdx 0, and listing on every
        // play would put a mylist call in front of each warm link. ListFiles is /play's way back when a movie's
        // raw position turned out to name another file.
        let need_files = (t.season.is_some() && t.episode.is_some()) || t.list_files;
        // Scope by the debrid token: the cached value is a TorBox torrent_id, which is account-scoped.
        // Every per-install store shares one process-global cache, so an infohash-only key would let one
        // user's cached torrent_id be used with another user's token (→ wrong/other-account content).
        let identity = self.account_identity();
        let key = resolve_key(&identity, &t.info_hash);
        let cache = self.cache();
        let svc = DebridService::TorBox;

        // A rejected key makes every request pointless, reads included — the same gate RD and Premiumize have.
        // It sits ABOVE the warm fast path, not below it: any pack played in the last six hours resolves
        // straight out of that entry and returns without ever reaching a gate placed after it, which is the
        // normal state during a binge. Ten polls meant ten requestdl calls on a key TorBox had already
        // rejected, and that branch records no refusal either, so it could not even set the key it skipped.
        if let Some(reason) = account_backed_off(cache, svc, &identity) {
            return Err(unavailable(svc, format!("{reason} (backing off)"), 0));
        }

        // And the per-release backoff, for the same reason the account one is here rather than below: this
        // path returns without ever reaching the gate further down, so recording a refusal here bought
        // nothing — the very next poll asked the throttled endpoint again. Classifying and remembering are
        // only worth anything if something then reads the memory.
        //
        // Not for a NoAdd caller, which the guards below also let past: a read-only resolve cannot have
        // caused a backoff and must not be blocked by one — the probe route depends on that, and a test
        // pins it.
        //
        // Nor when an add of OURS is out and still believable. That is the order the gate further down
        // states — "scout's own bookkeeping first, the store's verdict second… asking backedOff ahead of it
        // let any refusal recorded in the meantime answer 503 instead, naming a store that had said nothing"
        // — and hoisting this gate for the warm-path reason above quietly reversed it for TorBox alone. Real-
        // Debrid and Premiumize both order it as documented.
        //
        // Reachable without planting anything: /play has no singleflight and Stremio races requests, so two
        // concurrent polls for one hash can leave both markers live — one poll's createtorrent answered 429
        // (settles its attempt, records the refusal), the other's connection reset (its attempt marker
        // stays). Measured on 4 of 20 racing pairs: /play answered 503 blaming TorBox for a release scout had
        // an add out for, while ?probe=1 correctly said downloading.
        //
        // addStillBelievable rather than the bare marker, so past addGiveUp the refusal is heard again
        // instead of being suppressed by a marker nothing will ever settle.
        if !t.no_add && !add_still_believable(cache, svc, &identity, &t.info_hash) {
            if let Some(reason) = backed_off(cache, svc, &identity, &t.info_hash) {
                return Err(unavailable(svc, format!("{reason} (backing off)"), 0));
            }
        }

        // Fast path: a warm entry from an earlier episode of the same pack. Skip it when episode-select is
        // needed but the cached file list is empty (audit #3 — a transient blip would otherwise mis-serve).
        if let Some(raw) = self.cache.as_ref().and_then(|c| c.get(&key)) {
            if let Ok(entry) = serde_json::from_str::<ResolveEntry>(&raw) {
                if !need_files || !entry.files.is_empty() {
                    let torrent_id = entry.torrent_id;
                    // The cached list already proves this pack is the wrong one. Say so now rather than adding
                    // the torrent again below to re-learn the same thing.
                    let file_id = select_file_id(&entry.files(), t)?;
                    match self.request_download(torrent_id, file_id).await {
                        Ok(link) => return Ok(link),
                        // A throttled link request on a torrent this account ALREADY holds must not fall
                        // through to createtorrent: that turns a read into a write, on an account whose whole
                        // problem is that it has written too much.
                        //
                        // Remembered, like the identical error from the identical call on the held path. The
                        // account gate a few lines up already noted that this branch "records no refusal
                        // either, so it could not even set the key it skipped" — stated and then left. A warm
                        // entry is the normal state during a binge, so a rejected key was re-asked once per
                        // poll and the probe route, which reads the backoff, reported nothing queued.
                        Err(err @ StoreError::Unavailable(_)) => {
                            record_refusal_for(cache, svc, &identity, &t.info_hash, &err, t.no_add);
                            return Err(err);
                        }
                        Err(_) => {}
                    }
                }
            }
        }

        // Does the account ALREADY hold this? The entry above is a six-hour convenience cache, not a record of
        // what the account has — so "not played in the last six hours", which is the normal state of most of a
        // library, fell through to createtorrent and paid an add for a torrent already sitting there. The
        // lookup that answers this properly already exists and is already used by Status; it simply was never
        // consulted here.
        if let (id, true) = self.known_torrent_id(&t.info_hash) {
            match self.resolve_held_torrent(id, &key, need_files, t).await {
                Ok(link) => return Ok(link),
                Err(err) => {
                    // The remembered id is a claim with a six-hour life, not a fact — the torrent can be removed
                    // from the account at any point, by the user or by TorBox. When it will not resolve, forget
                    // it and let the add path below buy it again. Without this the id is not only kept but
                    // REFRESHED on every poll, so a deleted torrent answered dead_link for at least six hours
                    // and the client blacklisted a release that had been playing an hour earlier. Before this
                    // shortcut existed, that case healed itself by falling through to the add.
                    // ONLY a definitive "no such torrent" means the remembered id is stale. Anything else — a
                    // cancelled poll, a throttle, a timeout, a pack that does not contain the requested
                    // episode — is a fact about this attempt, not about what the account holds. Forgetting on
                    // all of them cost an add per poll: the re-add re-created the torrent, the next Status
                    // found it and cleared the in-flight marker meant to stop the loop, and round it went.
                    if !is_torrent_gone(&err) {
                        return Err(err);
                    }
                    self.forget_torrent_id(&t.info_hash);
                    log_limited(
                        "torbox-gone",
                        &format!("torbox no longer has {} — re-adding", short_hash(&t.info_hash)),
                    );
                }
            }
        }

        // A parked add from an earlier poll: TorBox had handed back a queued_id rather than a torrent_id. Find
        // out, at the cost of one read rather than another add, whether it has since been promoted to a real
        // torrent — the account listing is what Status already uses to discover one.
        if self.known_queued_id(&t.info_hash).is_some() {
            let (id, ok, _) = self.torrent_id(&t.info_hash).await;
            if ok {
                self.forget_queued_id(&t.info_hash);
                return self.resolve_held_torrent(id, &key, need_files, t).await;
            }
            if t.no_add {
                return Err(would_add());
            }
            return Err(StoreError::AddQueued);
        }

        // From here on, resolving MEANS queueing — so a caller that forbade that is answered now, before the
        // add-backoff below. That backoff describes a state a read-only caller cannot have caused and must not
        // be blocked by.
        if t.no_add {
            return Err(would_add());
        }

        // Scout's own bookkeeping first, the store's verdict second — the order Premiumize already uses. An add
        // we sent and never heard back about is a 202 "downloading", and asking backedOff ahead of it let any
        // refusal recorded in the meantime answer 503 instead, naming a store that had said nothing.
        add_in_flight(cache, svc, &identity, &t.info_hash)?;
        // A client polls /play every few seconds for the whole fetch, and every poll that got this far ran a
        // fresh createtorrent. Once TorBox starts throttling, that loop is what keeps it throttled: the add
        // fails, nothing is cached, and the next poll adds again. Back off for a minute after a refusal so a
        // wait costs one call rather than one per poll.
        if self.cache.is_some() {
            if let Some(reason) = backed_off(cache, svc, &identity, &t.info_hash) {
                return Err(unavailable(svc, format!("{reason} (backing off)"), 0));
            }
        }
        // Believed cached ALREADY, from the checkcached read the stream list or the cache check ahead of this
        // resolve already paid for (knownCached's 60s memo — see cachedTTL) — not a fresh call, a cache read.
        // If TorBox's earlier answer was stale, add_only_if_cached makes the add refuse rather than silently
        // spend the separate 60-an-hour UNCACHED allowance on a release that turned out not to be cached.
        let (believed_cached, _) =
            known_cached(self.cache(), svc, &identity, std::slice::from_ref(&t.info_hash));
        let only_if_cached = believed_cached.get(&t.info_hash).copied().unwrap_or(false);
        let (torrent_id, queued_id) = match self.add_magnet(&t.info_hash, t.prefetch, only_if_cached).await {
            // Parked, not dead — see errAddQueued. Nothing more to do with this id until a later poll's
            // knownQueuedID branch (above) finds it promoted.
            Ok((0, queued_id)) if queued_id != 0 => return Err(StoreError::AddQueued),
            Ok(ids) => ids,
            Err(err) => {
                // Every refused add backs off, not only a 429. The refusal that caused the incident was a 400 —
                // TorBox's answer for an account at its download limit — which is a `DeadLinkError`, so keying
                // the backoff on the error TYPE left the one case that mattered re-adding on every poll.
                //
                // A cancelled request is NOT a refusal. /play runs on the client's context and the client
                // cancels aggressively — a focus change is enough — so one cancelled add wrote "context
                // canceled" into the backoff and served that release 503 store_unavailable for the next
                // minute, on both /play and the probe route. An accusation TorBox never made, about a release
                // that is very likely fine.
                //
                // An add that went out and was never answered is not a refusal either — the marker addMagnet
                // deliberately leaves set says the outcome is unknown, and filing that here made backedOff,
                // which this store consults FIRST, pre-empt it on the next poll: errAddInFlight became
                // unreachable and the client was told its debrid was refusing for a release scout had an add
                // out for.
                if add_outcome_unknown(cache, svc, &identity, &t.info_hash) {
                    // Not on a cancellation. /play runs on the client's context and a focus change is enough,
                    // so a viewer backing out wrote a give-up stamp that outlived them: past addGiveUp every
                    // resolve of that release answered a dead link for the rest of unknownOutcomeTTL, with no
                    // upstream call able to clear it — RD and Premiumize have no Status to rediscover the
                    // torrent with. A deadline belongs on a service that never answered, not on a client that
                    // hung up. This is the same guard recordRefusal applies one branch over, for the same
                    // reason. Start the clock on not knowing, so this cannot cycle forever.
                    // Nor when the add itself already judged the outcome: it returns errAddInFlight from the
                    // body-read branch, which is not recognisably a cancellation out here, so stamping again
                    // undid the guard one layer down.
                    if !is_cancellation(&err) && err != StoreError::AddInFlight {
                        note_unknown_outcome(cache, svc, &identity, &t.info_hash);
                    }
                } else {
                    record_refusal(cache, svc, &identity, &t.info_hash, &err);
                }
                return Err(err);
            }
        };
        let _ = queued_id;
        let link = self.resolve_held_torrent(torrent_id, &key, need_files, t).await;
        // `gone` about an id createtorrent returned SECONDS ago is TorBox disagreeing with itself, not the
        // account saying it lacks the torrent — and the two must not lead to the same place. Passed through, it
        // left the fresh id unremembered, so the next poll found no known id and added the same torrent again:
        // one add per poll, bounded by nothing but the hourly allowance, which a two-second cadence spends in
        // under two minutes. (The held path above still re-buys on `gone`, which is where that verdict does
        // mean what it says.)
        //
        // Backing off is what actually bounds it — converting the error alone does not, because the next poll
        // finds no known id and buys the torrent again regardless of what this one answered. A store that
        // creates a torrent and then denies holding it is faulting, so naming TorBox is accurate; the answer to
        // THIS poll stays a dead link so the client can move on to another release rather than sit on a service
        // error for a release that may be fine elsewhere.
        if matches!(&link, Err(e) if is_torrent_gone(e)) {
            record_refusal(
                cache,
                svc,
                &identity,
                &t.info_hash,
                &unavailable(svc, "created this torrent and then denied holding it", 0),
            );
            return Err(dead_link("torbox created this torrent and then denied holding it"));
        }
        link
    }

    /// Go `torBoxStore.Status`, the two-valued half of the interface, for callers that only need "is it
    /// downloading".
    async fn status(&self, t: &ResolveTarget) -> Option<StoreStatus> {
        let (status, answer) = self.status_answer(t).await?;
        (answer == StatusAnswer::Downloading).then_some(status)
    }

    /// Go `torBoxStore.StatusAnswer` — progress for a torrent this account has already been asked to fetch.
    /// Resolve records the torrent id unconditionally, so a queued release (add succeeded, link not ready) is
    /// exactly the case this answers.
    ///
    /// Reports Downloading unless TorBox positively describes an unfinished download. Anything less — a
    /// `success:false` body, a `data:null` for a torrent the user deleted, a payload with neither progress nor a
    /// finished flag — must read as "no wait to promise", or a genuinely dead release would answer
    /// "downloading, 0%" forever.
    ///
    /// The answer is three-valued, not a bool: "it is downloading", "it is not", and "I could not find out" are
    /// three different facts and only the middle one may lead to an add. A throttled TorBox answering 503 used
    /// to read back as "nobody is fetching it", so /play queued a duplicate — more load on an account that was
    /// already refusing, which is the one failure mode that feeds itself.
    async fn status_answer(&self, t: &ResolveTarget) -> Option<(StoreStatus, StatusAnswer)> {
        let key = torbox_status_key(&self.account_identity(), &t.info_hash);
        if let Some(raw) = self.cache.as_ref().and_then(|c| c.get(&key)) {
            if let Ok(hit) = serde_json::from_str::<CachedStatus>(&raw) {
                metrics().torbox_status_avoided.fetch_add(1, Ordering::Relaxed);
                metrics().torbox_avoid("status_snapshot");
                return Some(hit.parts());
            }
        }
        let store = self.clone();
        let target = t.clone();
        let flight_key = key.clone();
        let _waiting = GaugeGuard::new(&metrics().torbox_status_waiters);
        let (answer, shared) = STATUS_FLIGHTS
            .run(&key, move || async move {
                // Bounded: a status read that finds the slots full answers "could not find out" at once, and
                // does not remember it.
                let Ok(_slot) = STATUS_SLOT_POOL.try_acquire() else {
                    return CachedStatus::new(StoreStatus::default(), StatusAnswer::Unknown);
                };
                let _active = GaugeGuard::new(&metrics().torbox_status_active);
                // Shared work cannot inherit one waiter's cancellation: another viewer may still need the
                // answer.
                let (status, answer) =
                    tokio::time::timeout(SHARED_DEADLINE, store.status_answer_uncached(&target))
                        .await
                        .unwrap_or((StoreStatus::default(), StatusAnswer::Unknown));
                let ttl = if answer == StatusAnswer::Unknown { UNKNOWN_STATUS_TTL } else { STATUS_TTL };
                let shared = CachedStatus::new(status, answer);
                if answer != StatusAnswer::Unknown {
                    if let (Some(cache), Ok(raw)) = (&store.cache, serde_json::to_string(&shared)) {
                        cache.put_class(&flight_key, &raw, ttl, CacheClass::Volatile);
                    }
                }
                shared
            })
            .await;
        if shared {
            metrics().torbox_status_avoided.fetch_add(1, Ordering::Relaxed);
            metrics().torbox_avoid("status_snapshot");
        }
        Some(match answer {
            Some(shared) => shared.parts(),
            // The shared read panicked: nothing is known.
            None => (StoreStatus::default(), StatusAnswer::Unknown),
        })
    }

    /// Go `torBoxStore.Cancel` drops (or reannounces) the release this account is fetching for infoHash. See
    /// `CancelOutcome` for what each answer means, and den#204 for the safety rules the route layers on top:
    /// never a torrent scout did not add, never one already finished — both checked here — and never one a
    /// sibling queue entry still needs, which only the CLIENT can know and so is not this store's job.
    async fn cancel(&self, info_hash: &str, reannounce: bool) -> Option<Result<CancelOutcome, StoreError>> {
        Some(self.cancel_release(&go_to_lower(info_hash), reannounce).await)
    }

    fn recent_refusal(&self, info_hash: &str) -> Option<String> {
        backed_off(self.cache(), DebridService::TorBox, &self.account_identity(), info_hash)
    }

    fn add_in_flight(&self, info_hash: &str) -> Option<bool> {
        Some(add_still_believable(self.cache(), DebridService::TorBox, &self.account_identity(), info_hash))
    }

    fn holds_torrent(&self, t: &ResolveTarget) -> Option<bool> {
        Some(self.known_torrent_id(&t.info_hash).1)
    }

    fn scout_budget_spent(&self) -> Option<bool> {
        Some(
            global_add_budget().remaining(&budget_account(DebridService::TorBox, &self.account_identity()))
                <= 0,
        )
    }

    fn account_refused(&self) -> Option<String> {
        account_backed_off(self.cache(), DebridService::TorBox, &self.account_identity())
    }

    fn budget_account(&self) -> Option<String> {
        Some(budget_account(DebridService::TorBox, &self.account_identity()))
    }

    /// Go `torBoxStore.KnownFileSize` (linkcheck.go:161): TorBox's size comes out of the resolve entry, which
    /// carries the pack's file list whenever an episode had to be picked from one. A movie resolves without
    /// listing files, so its size is simply not known. Cache-only: a size is only worth having if finding it
    /// costs nothing.
    fn known_file_size(&self, t: &ResolveTarget) -> Option<i64> {
        let raw = self.cache.as_ref()?.get(&resolve_key(&self.account_identity(), &t.info_hash))?;
        let entry = serde_json::from_str::<ResolveEntry>(&raw).ok()?;
        if entry.files.is_empty() {
            return None;
        }
        let files = entry.files();
        let id = select_file_id(&files, t).ok()??;
        size_of(&files, id)
    }
}

impl TorBoxStore {
    async fn cancel_release(&self, info_hash: &str, reannounce: bool) -> Result<CancelOutcome, StoreError> {
        let identity = self.account_identity();
        let cache = self.cache();

        // A parked add has no torrent id yet — TorBox's queue is a separate endpoint from the torrent one.
        if let Some(qid) = self.known_queued_id(info_hash) {
            if !was_added_by_us(cache, &identity, info_hash) {
                return Ok(CancelOutcome { reason: "not_ours".to_owned(), ..Default::default() });
            }
            if reannounce {
                // Nothing to reannounce — TorBox has not started fetching it, so there is no swarm to retry.
                // Reported as still queued rather than refused: the stall-limit/2 retry costs nothing here.
                return Ok(CancelOutcome {
                    done: true,
                    reannounced: true,
                    status: StoreStatus { state: FETCH_QUEUED.to_owned(), ..Default::default() },
                    ..Default::default()
                });
            }
            self.control_queued(qid, "delete").await?;
            self.forget_queued_id(info_hash);
            clear_added_by_us(cache, &identity, info_hash);
            return Ok(CancelOutcome { done: true, ..Default::default() });
        }

        let (torrent_id, ok, authoritative) = self.torrent_id(info_hash).await;
        if !ok {
            if !authoritative {
                return Err(unavailable(
                    DebridService::TorBox,
                    "could not confirm this account's listing",
                    0,
                ));
            }
            return Ok(CancelOutcome { reason: "not_queued".to_owned(), ..Default::default() });
        }
        // The entry is fetched before the ownership check now, not after it, because the one exception to
        // "never a torrent scout did not add" (below) needs to know the torrent's own state to decide — and
        // because "finished" must win over "not ours" the same way it wins over "ours": a completed download is
        // kept either way.
        let (entry, found) = self.fetch_entry(torrent_id).await?;
        if !found {
            // TorBox agrees it no longer has this one either; nothing to cancel, and the stale id must not
            // outlive the fact — the same rule Resolve's own `gone` handling follows.
            self.forget_torrent_id(info_hash);
            return Ok(CancelOutcome { reason: "not_queued".to_owned(), ..Default::default() });
        }
        if torbox_is_finished(entry.download_finished, entry.download_state.as_deref()) {
            return Ok(CancelOutcome { reason: "finished".to_owned(), ..Default::default() });
        }
        if !was_added_by_us(cache, &identity, info_hash) {
            // The narrow exception: a torrent with no addedByUs marker — added before ownership tracking
            // existed, or by some other path entirely — that is stalled dead (torboxFetchState's own "stalled"
            // rule: no seeds, or paused) and not finished. Left alone, this held a TorBox slot forever: the
            // client's fallback had already abandoned the release for this exact hash, and `not_ours` meant no
            // cancel ever reached TorBox, so the stalled torrent outlived every episode that tried the next
            // release instead. A healthy (not stalled) torrent with no marker is still refused — this is not a
            // general ownership override, only a drain for dead weight.
            let Some(state) = entry.download_state.as_deref().filter(|s| fetch_state(s) == FETCH_STALLED)
            else {
                return Ok(CancelOutcome { reason: "not_ours".to_owned(), ..Default::default() });
            };
            log_limited(
                "torbox-stalled-cancel",
                &format!(
                    "cancelling a stalled torrent with no addedByUs marker ({}, state {}) — abandoned by the client's fallback",
                    short_hash(info_hash),
                    quote_go(state)
                ),
            );
        }
        let status = StoreStatus {
            seeds: entry.seeds,
            peers: entry.peers,
            state: entry.download_state.as_deref().map(|s| fetch_state(s).to_owned()).unwrap_or_default(),
            ..Default::default()
        };
        if reannounce {
            self.control_torrent(torrent_id, "reannounce").await?;
            return Ok(CancelOutcome { done: true, reannounced: true, status, ..Default::default() });
        }
        self.control_torrent(torrent_id, "delete").await?;
        self.forget_torrent_id(info_hash);
        clear_added_by_us(cache, &identity, info_hash);
        Ok(CancelOutcome { done: true, ..Default::default() })
    }
}

/// Go `torboxIsFinished` reports whether a mylist entry describes a completed download — kept on a cancel,
/// never deleted, because it holds no slot and is what plays instantly. download_finished is the primary signal;
/// the state names are TorBox's own words for "nothing left to fetch", checked too because a finished torrent
/// does not always carry the flag on every reply.
fn torbox_is_finished(finished: Option<bool>, state: Option<&str>) -> bool {
    if finished == Some(true) {
        return true;
    }
    let Some(state) = state else { return false };
    matches!(go_to_lower(state.trim()).as_str(), "completed" | "cached" | "uploading")
}

/// Go's `%q` for a log line.
fn quote_go(s: &str) -> String {
    crate::decisionlog::quote_go(s.as_bytes())
}
