//! Port of the cross-store bookkeeping in `internal/scout/stores.go` (lines 780-1560): cache keys for
//! resolve and torrent ids, add attempts, queued and held markers, the unknown-outcome rules, refusal
//! backoff, and the `StorePool` reporters built on them. Owned by the "Stores II" work package; the
//! TorBox store (`torbox.rs`) calls into it.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crate::addbudget::{budget_account, global_add_budget};
use crate::cache::{Cache, CacheClass};
use crate::config::DebridService;
use crate::loglimit::log_limited;
use crate::scrape::DoError;
use crate::stores::pool::StorePool;
use crate::stores::torbox::TORBOX_API;
use crate::stores::{
    dead_link, error_chain_text, is_cancellation_error, ResolveTarget, StoreError, StoreUnavailableError,
    ERR_CHECK_FAILED, ERR_TORRENT_GONE, RESOLVE_CACHE_TTL,
};
use crate::util::key_hash;

/// Go `errScoutSide`: a refusal scout made itself (its own add allowance, an add already in flight), not
/// one the service made. `recordRefusal` must never remember it.
pub(crate) const ERR_SCOUT_SIDE: &str = "refused by scout, not by the service";

/// Go `addAttemptTTL` (stores.go:817): how long an add attempt marker stays live.
pub(crate) const ADD_ATTEMPT_TTL: Duration = Duration::from_secs(90);
/// Go `queuedTTL` (stores.go:895): how long a queued-transfer marker stays live.
pub(crate) const QUEUED_TTL: Duration = Duration::from_secs(20 * 60);
/// Go `pendingGiveUp` (stores.go:900): past this a queued Premiumize transfer is reported dead.
pub(crate) const PENDING_GIVE_UP: Duration = Duration::from_secs(10 * 60);
/// Go `addGiveUp` (stores.go:1004): past this an add with an unknown outcome is reported dead.
pub(crate) const ADD_GIVE_UP: Duration = Duration::from_secs(10 * 60);
/// Go `unknownOutcomeTTL` (stores.go:1005).
pub(crate) const UNKNOWN_OUTCOME_TTL: Duration = Duration::from_secs(30 * 60);
/// Go `refusalBackoff` (stores.go:1557): how long to stop asking after the service refuses. Long enough that a
/// poll loop can't sustain a throttle, short enough that a passing 500 doesn't cost the viewer a real wait.
pub(crate) const REFUSAL_BACKOFF: Duration = Duration::from_secs(60);
/// Go `torrentMissTTL` (stores.go:800): how long "the account has no torrent for this hash" is remembered.
pub(crate) const TORRENT_MISS_TTL: Duration = Duration::from_secs(15);
/// Go `cachedTTL` (stores.go:537): how long "this account holds it" answers for.
///
/// Only a POSITIVE is memoised, and that asymmetry is the whole point. "Held" is stable: a release does
/// not stop being held while a viewer picks it, so the same question asked by /stream, then by ?probe=1,
/// then by /play on a two-account install — three times within seconds, about the hash the viewer just
/// chose — has one answer. "Not held" is the opposite: it becomes "held" the instant a download finishes,
/// and that transition is exactly what the probe route polls to notice. Memoising a negative would make
/// the client wait up to cachedTTL to be told its own download is ready.
pub(crate) const CACHED_TTL: Duration = Duration::from_secs(60);

/// Go `torboxIdentityPrefix` (stores.go:96).
pub(crate) const TORBOX_IDENTITY_PREFIX: &str = "torbox-account:v2:";

/// A cache that holds nothing, standing for Go's nil `Cache`: every helper below treats a missing cache as
/// "no memory", which is what reading this returns and writing to it does.
pub(crate) struct NoCache;

impl Cache for NoCache {
    fn get(&self, _key: &str) -> Option<Arc<str>> {
        None
    }

    fn put(&self, _key: &str, _value: &str, _ttl: Duration) {}
}

/// A store's optional cache as the `&dyn Cache` the helpers take; a store without one reads and writes the
/// empty cache.
pub(crate) fn cache_or_none(cache: &Option<Arc<dyn Cache>>) -> &dyn Cache {
    match cache {
        Some(c) => c.as_ref(),
        None => &NoCache,
    }
}

/// Go `cachePut(cache, key, "", time.Nanosecond, CacheVolatile)`: drops a key by letting it expire at once.
pub(crate) fn clear(cache: &dyn Cache, key: &str) {
    cache.put_class(key, "", Duration::from_nanos(1), CacheClass::Volatile);
}

/// A marker's value when it is live. A cleared key is written as "" and lives for a nanosecond, so a read
/// inside that tick must not take it for a marker.
pub(crate) fn live(cache: &dyn Cache, key: &str) -> Option<Arc<str>> {
    cache.get(key).filter(|v| !v.is_empty())
}

/// Go `time.Since(time.Unix(at, 0))`, `None` when `at` is in the future (Go's negative duration).
fn since_unix(at: i64) -> Option<Duration> {
    let now = SystemTime::now();
    let then = if at >= 0 {
        UNIX_EPOCH.checked_add(Duration::from_secs(at.unsigned_abs()))?
    } else {
        UNIX_EPOCH.checked_sub(Duration::from_secs(at.unsigned_abs()))?
    };
    now.duration_since(then).ok()
}

fn now_unix() -> String {
    SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_secs()).unwrap_or(0).to_string()
}

/// Go `torboxAccountIdentity` (stores.go:102): the canonical identity of one TorBox account. The API is
/// part of it: test, proxy and future TorBox-compatible endpoints may legitimately reuse a token string
/// while naming completely different accounts. The v2 marker also makes every old token-only persistent
/// fact miss safely after upgrade rather than being trusted under an arbitrary endpoint.
pub(crate) fn torbox_account_identity(api: &str, token: &str) -> String {
    format!("{TORBOX_IDENTITY_PREFIX}{}:{token}", key_hash(api))
}

/// Go `canonicalTorboxIdentity` (stores.go:108). Tests and old internal call sites commonly pass a raw token
/// to key builders. Interpret that as the canonical production API, while preserving an identity already
/// assembled by a store.
pub(crate) fn canonical_torbox_identity(identity: &str) -> String {
    if identity.starts_with(TORBOX_IDENTITY_PREFIX) {
        return identity.to_owned();
    }
    torbox_account_identity(TORBOX_API, identity)
}

/// Go `serviceAccountIdentity` (stores.go:118) normalises generic account-scoped state too. This keeps
/// callers which pass a raw TorBox token on the production API compatible while ensuring a store configured
/// for another API never shares facts with it. Other debrid services have no configurable API identity.
pub(crate) fn service_account_identity(svc: DebridService, identity: &str) -> String {
    if svc == DebridService::TorBox {
        return canonical_torbox_identity(identity);
    }
    identity.to_owned()
}

/// Go `cachedKey` (stores.go:539).
pub(crate) fn cached_key(svc: DebridService, token: &str, info_hash: &str) -> String {
    format!("{}:cached:{}:{info_hash}", svc.as_str(), key_hash(&service_account_identity(svc, token)))
}

/// Go `knownCached` (stores.go:544) splits hashes into those already known to be held and those still worth
/// asking about.
pub(crate) fn known_cached(
    cache: &dyn Cache,
    svc: DebridService,
    token: &str,
    hashes: &[String],
) -> (HashMap<String, bool>, Vec<String>) {
    let mut known = HashMap::with_capacity(hashes.len());
    let mut ask = Vec::with_capacity(hashes.len());
    for h in hashes {
        if cache.get(&cached_key(svc, token, h)).is_some() {
            known.insert(h.clone(), true);
            continue;
        }
        ask.push(h.clone());
    }
    (known, ask)
}

/// Go `rememberCached` (stores.go:561) records the positives from an answer. Negatives are deliberately not
/// recorded.
pub(crate) fn remember_cached(
    cache: &dyn Cache,
    svc: DebridService,
    token: &str,
    result: &HashMap<String, bool>,
) {
    for (h, held) in result {
        if *held {
            cache.put_class(&cached_key(svc, token, h), "1", CACHED_TTL, CacheClass::Volatile);
        }
    }
}

/// Go `batchesFailed` (stores.go:768) reports errCheckFailed only when every batch failed (none returned
/// usable data). A partial failure is not an error: the hashes it could not answer for are simply absent
/// from the map.
pub(crate) fn batches_failed(batch_ok: &[bool]) -> Result<(), StoreError> {
    if batch_ok.iter().any(|ok| *ok) {
        return Ok(());
    }
    Err(dead_link(ERR_CHECK_FAILED))
}

/// Go `resolveKey` (stores.go:782): the cached torrent id and file list for a hash on this account. Scoped
/// by the canonical TorBox identity, because the value is an account-scoped torrent id.
pub(crate) fn resolve_key(identity: &str, info_hash: &str) -> String {
    let identity = canonical_torbox_identity(identity);
    format!("torbox:resolve:{}:{}", crate::util::key_hash(&identity), info_hash)
}

/// Go `torrentIDKey` (stores.go:786): the torrent id alone, kept apart from the resolve entry so a queued
/// torrent with no file list is still addressable by `Status`.
pub(crate) fn torrent_id_key(identity: &str, info_hash: &str) -> String {
    let identity = canonical_torbox_identity(identity);
    format!("torbox:torrent:{}:{}", crate::util::key_hash(&identity), info_hash)
}

/// Go `transportKind` (stores.go:1060): a transport failure described WITHOUT its URL, which for TorBox
/// carries the account token in its query string.
///
/// Go reads the cause off a `*url.Error`. The errors a `Doer` returns are boxed, and either a `reqwest::Error`
/// (whose `Display` embeds the URL) or a plain message of the same shape, so the URL is cut out of the text
/// and only the cause is kept.
pub(crate) fn transport_kind(err: Option<&(dyn std::error::Error + Send + Sync + 'static)>) -> String {
    let Some(err) = err else {
        return "unknown".to_owned();
    };
    // The messages of the whole chain: a reqwest error names the URL in its own text and the cause in its sources.
    let mut chain = Vec::new();
    let mut link: Option<&(dyn std::error::Error + 'static)> = Some(err);
    while let Some(e) = link {
        chain.push(e.to_string());
        link = e.source();
    }
    if err.downcast_ref::<reqwest::Error>().is_some_and(reqwest::Error::is_timeout) {
        return "timeout".to_owned();
    }
    for message in &chain {
        let lower = message.to_lowercase();
        if lower.contains("timeout") || lower.contains("timed out") || lower.contains("deadline exceeded") {
            return "timeout".to_owned();
        }
    }
    for message in &chain {
        let lower = message.to_lowercase();
        if lower.contains("cancel") {
            return "cancelled".to_owned();
        }
    }
    // The deepest link is the cause itself ("connection refused"); a chain of one is the text with its URL cut.
    let deepest = chain.last().map(String::as_str).unwrap_or_default();
    without_url(deepest)
}

/// Cuts the `for url (<url>)` clause out of a transport error's text, and the generic lead-in with it, leaving the
/// cause (Go's `urlErr.Err.Error()`).
fn without_url(message: &str) -> String {
    let Some(start) = message.find(" for url (") else {
        return message.to_owned();
    };
    let after = &message[start + " for url (".len()..];
    // The URL ends at the first ")" that closes the clause: followed by ": ", or the end of the text.
    let mut end = None;
    for (i, c) in after.char_indices() {
        if c == ')' {
            let rest = &after[i + 1..];
            if rest.is_empty() || rest.starts_with(": ") {
                end = Some(i);
                break;
            }
        }
    }
    match end {
        Some(i) => {
            let rest = after[i + 1..].trim_start_matches(": ");
            if rest.is_empty() {
                message[..start].to_owned()
            } else {
                rest.to_owned()
            }
        }
        // An unterminated clause cannot be told from the URL itself, so nothing after the lead-in is kept.
        None => message[..start].to_owned(),
    }
}

/// Go `torrentMissKey` (stores.go:802): the account has NO torrent for this hash, briefly.
///
/// The lookup that answers this fetches the account's ENTIRE torrent list, and only a hit was remembered.
/// So the miss — which is every poll of a release that was never queued — refetched the whole list each
/// time. A client polls /play every couple of seconds for the length of a download, so a ten-minute wait
/// was some three hundred full-account-list fetches, each one growing with the account's history.
///
/// Deliberately short: an add can land at any moment, and Status is what tells the viewer their download
/// is progressing. This only has to collapse a burst of polls, not remember anything for long.
pub(crate) fn torrent_miss_key(identity: &str, info_hash: &str) -> String {
    format!("torbox:notorrent:{}:{info_hash}", key_hash(&canonical_torbox_identity(identity)))
}

/// Go `addAttemptKey` (stores.go:819): an add we SENT but never got an answer to.
///
/// This is the honest state between "queued" and "not queued", and it needs its own memory because the
/// two obvious ones both lie about it. Treating it as a refusal blames the service for the client hanging
/// up; treating it as never-happened re-adds on the next poll. A client polls /play every couple of
/// seconds and cancels on a focus change, so re-adding turned ONE release into sixty createtorrent calls
/// against a sixty-an-hour ceiling — measured — while the budget reported a full allowance because each
/// one had been refunded.
pub(crate) fn add_attempt_key(svc: DebridService, token: &str, info_hash: &str) -> String {
    format!("{}:adding:{}:{info_hash}", svc.as_str(), key_hash(&service_account_identity(svc, token)))
}

/// Go `refusedKey` (stores.go:1466): the per-release refusal memory.
///
/// Marks a hash the service just declined to add for this account, so polls stop re-asking. Per service,
/// because a refusal by one says nothing about another.
pub(crate) fn refused_key(svc: DebridService, token: &str, info_hash: &str) -> String {
    format!("{}:refused:{}:{info_hash}", svc.as_str(), key_hash(&service_account_identity(svc, token)))
}

/// Go `accountRefusedKey` (stores.go:1476): the service turned this ACCOUNT away, whatever was asked for.
///
/// An expired or wrong API key is not a fact about one release, and remembering it per infohash meant a
/// stale token backed off one hash while the next release paid for the same 403 again: sixty releases,
/// fifty calls, the whole hourly allowance gone, and replacing the key did not restore service because
/// the budget stayed spent for the rest of the hour.
pub(crate) fn account_refused_key(svc: DebridService, token: &str) -> String {
    format!("{}:refused-account:{}", svc.as_str(), key_hash(&service_account_identity(svc, token)))
}

/// Go `unknownOutcomeKey` (stores.go:998): when scout FIRST failed to learn what an add did, for this
/// release on this account. The add-attempt marker cannot answer that: it lives 90 seconds and is written
/// afresh by every new attempt, so a release whose add answer is never readable cycles marker → expiry →
/// new charged add → marker, forever. The client is shown 202 "downloading" throughout and no path ever says
/// dead link, so it cannot fall through; on RD and Premiumize, which have no Status to rediscover the
/// torrent with, that is forty duplicate torrents an hour and a viewer with nothing to do but back out.
///
/// This is the deadline `pendingGiveUp` already gives Premiumize's queue marker — "'coming' is a claim
/// with a deadline… refreshing the marker made it an absorbing state" — applied to the sibling marker
/// that never got one.
pub(crate) fn unknown_outcome_key(svc: DebridService, token: &str, info_hash: &str) -> String {
    format!("{}:unknown:{}:{info_hash}", svc.as_str(), key_hash(&service_account_identity(svc, token)))
}

/// Go `pmQueuedKey` (stores.go:889): Premiumize's directdl queued a transfer and had nothing to serve yet.
///
/// Set ONLY on that outcome, never on a successful one. directdl IS the fetch: for a release the account
/// holds it returns links straight away, and only an uncached one queues. A marker set on every call
/// blocked releases that would have played instantly — one infohash is a whole season pack, so the second
/// episode of a show could not be resolved for twenty minutes. Marking only the empty answer suppresses
/// exactly the repeat that costs something, and nothing else.
pub(crate) fn pm_queued_key(token: &str, info_hash: &str) -> String {
    format!("premiumize:queued:{}:{info_hash}", key_hash(token))
}

/// Go `pmHeldKey` (stores.go:937): Premiumize served this release from the account, so a later resolve
/// needs no transfer.
///
/// Premiumize has no torrent id to remember, which is how TorBox and Real-Debrid name themselves holders.
/// Without an equivalent, the moment /play settled the queue marker the store went back to being
/// unnameable, and the probe answered 404 for a pack it had just served an episode of.
pub(crate) fn pm_held_key(token: &str, info_hash: &str) -> String {
    format!("premiumize:held:{}:{info_hash}", key_hash(token))
}

/// Go `isCancellation` (stores.go:1080): the caller went away, rather than the store saying no. Nothing
/// about the store or the release can be concluded from it, so it must not be remembered as a refusal.
pub(crate) fn is_cancellation(err: &StoreError) -> bool {
    matches!(err, StoreError::Cancelled)
}

/// A transport failure from the HTTP seam as the error a store call returns. A caller going away or its
/// deadline running out is `Cancelled`; anything else keeps its text.
pub(crate) fn store_error_from_do(err: DoError) -> StoreError {
    if is_cancellation_error(err.as_ref()) {
        return StoreError::Cancelled;
    }
    StoreError::Other(error_chain_text(err.as_ref()))
}

/// The same for TorBox, whose requests carry the account token in the URL: a transport failure is reported as its
/// cause and never its URL (Go `transportKind`). A caller going away or its deadline running out is `Cancelled`.
pub(crate) fn transport_error(err: DoError) -> StoreError {
    if is_cancellation_error(err.as_ref()) {
        return StoreError::Cancelled;
    }
    StoreError::Other(transport_kind(Some(err.as_ref())))
}

/// Go's `err.Error()` for the errors a store call returns, for a log line or a remembered reason.
pub(crate) fn describe(err: &StoreError) -> String {
    err.to_string()
}

/// Go `errTorrentGone`: the service answered, and said it has no such torrent. The ONLY evidence that a
/// remembered torrent id is stale.
pub(crate) fn is_torrent_gone(err: &StoreError) -> bool {
    matches!(err, StoreError::DeadLink(d) if d.reason == ERR_TORRENT_GONE)
}

/// Go `errors.As(err, &unavailable)`: the `StoreUnavailableError` an error is or wraps.
pub(crate) fn as_unavailable(err: &StoreError) -> Option<&StoreUnavailableError> {
    match err {
        StoreError::Unavailable(u) | StoreError::ScoutSide(u) => Some(u),
        _ => None,
    }
}

/// Go `errors.As(err, &unavailable)` as a yes/no. Go's `errAddInFlight` is always built beside a
/// `StoreUnavailableError` (`fmt.Errorf("%w: %w", errAddInFlight, &StoreUnavailableError{...})`), so `errors.As`
/// finds one in it; the frozen `AddInFlight` variant carries no payload, so that fact is stated here.
pub(crate) fn wraps_unavailable(err: &StoreError) -> bool {
    as_unavailable(err).is_some() || matches!(err, StoreError::AddInFlight)
}

/// Go `scoutBudgetSpent` (stores.go:1218).
pub(crate) fn scout_budget_spent(svc: DebridService, token: &str) -> bool {
    global_add_budget().remaining(&budget_account(svc, token)) <= 0
}

/// Go `addInFlight` (stores.go:828) reports an add this process sent and never heard back about.
///
/// The error is `AddInFlight`, which handlePlay answers with 202 "downloading": it is scout's own
/// bookkeeping, not the service declining, so it must not be written into the refusal memory or reported
/// to the client as the debrid's doing — the confusion errScoutSide exists to end.
pub(crate) fn add_in_flight(
    cache: &dyn Cache,
    svc: DebridService,
    token: &str,
    info_hash: &str,
) -> Result<(), StoreError> {
    // The deadline is checked whether or not a marker is live, because it is what has to stop the NEXT
    // add. The marker lives ninety seconds and every attempt writes a fresh one, so an add that keeps
    // failing at the transport cycled marker → expiry → another charged add, about forty an hour, until
    // the allowance was gone and every release on the account answered 503 scout_busy. That is exactly
    // the cycle unknownOutcomeKey was written to end; it had only ever been wired to the body-read branch.
    if unknown_too_long(cache, svc, token, info_hash) {
        return Err(dead_link(format!("{} never reported what its add did", svc.as_str())));
    }
    if !add_outcome_unknown(cache, svc, token, info_hash) {
        return Ok(());
    }
    Err(StoreError::AddInFlight)
}

/// Go `addOutcomeUnknown` (stores.go:853) reports that an add went out and nothing came back — the marker
/// is written before the request and cleared only once a response arrives.
///
/// It is what keeps scout's own uncertainty out of the refusal memory. A connection reset mid-add is not
/// the service refusing: TorBox's addMagnet says exactly that ("the outcome is genuinely unknown: the
/// marker STAYS") and then its caller recorded a refusal anyway, which pre-empted this marker on the next
/// poll — backedOff is consulted first there — so errAddInFlight became unreachable and the client was
/// told its debrid was refusing, for a release scout had an add out for. Premiumize was right by
/// accident: it happens to ask addInFlight before backedOff.
pub(crate) fn add_outcome_unknown(
    cache: &dyn Cache,
    svc: DebridService,
    token: &str,
    info_hash: &str,
) -> bool {
    live(cache, &add_attempt_key(svc, token, info_hash)).is_some()
}

/// Go `noteAddAttempt` (stores.go:867): records that an add request went out, whatever comes back.
///
/// EVERY store that can add needs this, not just the one where the loop was first measured. Giving it to
/// TorBox alone did not stop the loop — it moved it: a cancelling client simply spent Real-Debrid's
/// allowance instead, fifty adds in under two minutes, because ResolvePreferring walks on to the store
/// with no marker.
pub(crate) fn note_add_attempt(cache: &dyn Cache, svc: DebridService, token: &str, info_hash: &str) {
    cache.put_class(&add_attempt_key(svc, token, info_hash), "1", ADD_ATTEMPT_TTL, CacheClass::Volatile);
    // Clear the torrent-miss marker too. It suppresses the account listing for 15s, and that listing is
    // the only thing that can discover the torrent this add just created — so leaving it kept the next
    // poll on the "nothing is queued" path instead of "it is downloading". The negative cache and the add
    // loop feed each other. This line was dropped when these helpers were made shared, along with the
    // paragraph saying why, and nothing failed: hence the test that now covers it.
    if svc == DebridService::TorBox {
        clear(cache, &torrent_miss_key(token, info_hash));
    }
}

/// Go `noteQueued` (stores.go:904) stamps the moment the transfer was queued. The TIME, not a flag: a flag
/// rewritten on every poll never ages, and a claim that never ages is not a wait, it is a promise nothing
/// can withdraw.
pub(crate) fn note_queued(cache: &dyn Cache, token: &str, info_hash: &str) {
    cache.put_class(&pm_queued_key(token, info_hash), &now_unix(), QUEUED_TTL, CacheClass::Volatile);
}

/// Go `pendingTooLong` (stores.go:912): this transfer was queued long enough ago that "still coming" has
/// stopped being a credible answer, so the client should be free to try something else.
pub(crate) fn pending_too_long(cache: &dyn Cache, token: &str, info_hash: &str) -> bool {
    let Some(raw) = cache.get(&pm_queued_key(token, info_hash)) else { return false };
    match raw.parse::<i64>() {
        Ok(at) => since_unix(at).is_some_and(|age| age >= PENDING_GIVE_UP),
        Err(_) => false,
    }
}

/// Go `settleQueuedTransfer` (stores.go:926) clears the marker once Premiumize has something to serve — the
/// transfer is done, and a later request must be free to pay for it again if it is ever evicted.
pub(crate) fn settle_queued_transfer(cache: &dyn Cache, token: &str, info_hash: &str) {
    clear(cache, &pm_queued_key(token, info_hash));
}

/// Go `noteHeld` (stores.go:941): the six-hour "this account held it" mark.
pub(crate) fn note_held(cache: &dyn Cache, token: &str, info_hash: &str) {
    cache.put_class(&pm_held_key(token, info_hash), "1", RESOLVE_CACHE_TTL, CacheClass::Durable);
}

/// Go `forgetHeld` (stores.go:952) drops the claim, which directdl answering with no content has just
/// disproved.
///
/// Every remembered-holding claim in this package needs one of these. TorBox and Real-Debrid forget their
/// ids on errTorrentGone for the same reason: the claim has a life measured in hours and the account can
/// change under it at any moment.
pub(crate) fn forget_held(cache: &dyn Cache, token: &str, info_hash: &str) {
    clear(cache, &pm_held_key(token, info_hash));
}

/// Go `heldRecently` (stores.go:958).
pub(crate) fn held_recently(cache: &dyn Cache, token: &str, info_hash: &str) -> bool {
    live(cache, &pm_held_key(token, info_hash)).is_some()
}

/// Go `alreadyQueued` (stores.go:966): whether a queued-transfer marker is on record.
pub(crate) fn already_queued(cache: &dyn Cache, token: &str, info_hash: &str) -> bool {
    live(cache, &pm_queued_key(token, info_hash)).is_some()
}

/// Go `settleAddAttempt` (stores.go:980) clears the marker once the outcome IS known, whatever it was.
///
/// The marker means "we do not know", not "an add happened" — so it must not outlive learning. A refused
/// add has the refusal backoff, a successful one has its cached torrent id, and a torrent discovered in
/// the account listing is plainly no longer in flight; all three describe the state better than this
/// does. Keeping it would block the legitimate retry when the add succeeded but the follow-up did not.
pub(crate) fn settle_add_attempt(cache: &dyn Cache, svc: DebridService, token: &str, info_hash: &str) {
    clear(cache, &add_attempt_key(svc, token, info_hash));
    // An outcome, of any kind, ends the run of not knowing.
    clear(cache, &unknown_outcome_key(svc, token, info_hash));
}

/// Go `noteUnknownOutcome` (stores.go:1009) stamps the FIRST such failure and never restamps — a clock that
/// resets is not a deadline, which is the mistake noteQueued was fixed for.
pub(crate) fn note_unknown_outcome(cache: &dyn Cache, svc: DebridService, token: &str, info_hash: &str) {
    let key = unknown_outcome_key(svc, token, info_hash);
    if live(cache, &key).is_some() {
        return;
    }
    cache.put_class(&key, &now_unix(), UNKNOWN_OUTCOME_TTL, CacheClass::Volatile);
}

/// Go `unknownTooLong` (stores.go:1021) reports that the outcome has been unknown for longer than anyone
/// should be asked to wait.
pub(crate) fn unknown_too_long(cache: &dyn Cache, svc: DebridService, token: &str, info_hash: &str) -> bool {
    let Some(raw) = live(cache, &unknown_outcome_key(svc, token, info_hash)) else { return false };
    match raw.parse::<i64>() {
        Ok(at) => since_unix(at).is_some_and(|age| age > ADD_GIVE_UP),
        Err(_) => false,
    }
}

/// Go `unknownOutcome` (stores.go:1046) is what an add whose answer never arrived returns: "still coming"
/// until the deadline, then a dead link so the client can fall through to another release.
///
/// `cause` decides whether the deadline starts at all. A viewer backing out cancels the request context,
/// and which of the two unknown-outcome branches that lands in — this one, or the transport failure above
/// it — is decided only by whether the response headers had arrived first. The transport branch got the
/// isCancellation guard; this one was not even passed the error to ask with, so the same back-out still
/// condemned the release: past addGiveUp every resolve answered a dead link for the rest of
/// unknownOutcomeTTL, with no upstream call able to clear it. A deadline is for a service that went
/// quiet.
pub(crate) fn unknown_outcome(
    cache: &dyn Cache,
    svc: DebridService,
    token: &str,
    info_hash: &str,
    cause: &(dyn std::error::Error + 'static),
) -> StoreError {
    if unknown_too_long(cache, svc, token, info_hash) {
        return dead_link(format!("{} never reported what its add did", svc.as_str()));
    }
    if !is_cancellation_error(cause) {
        note_unknown_outcome(cache, svc, token, info_hash);
    }
    StoreError::AddInFlight
}

/// Go `recordRefusalFor` (stores.go:1092) writes a refusal on behalf of a caller that may be read-only.
///
/// recordRefusal feeds TWO memories and a NoAdd caller stands differently to each. The per-release key is
/// an add-path guard — a read-only resolve is exempt from reading it, so it must not write one either, or
/// the probe route gates the /play behind it for a minute with no upstream call. The ACCOUNT key is not:
/// `accountBackedOff` is read unconditionally at the top of Resolve, NoAdd callers included, because a
/// rejected key makes every request pointless whatever it is asking for. Guarding both together made the
/// probe unable to set the very key it then reads, so it re-asked a rejected endpoint once per poll.
pub(crate) fn record_refusal_for(
    cache: &dyn Cache,
    svc: DebridService,
    token: &str,
    info_hash: &str,
    err: &StoreError,
    no_add: bool,
) {
    if !no_add || refusal_is_about_the_account(err) {
        record_refusal(cache, svc, token, info_hash, err);
    }
}

/// Go `refusalReason` (stores.go:1100) renders an add failure for the backoff cache, keeping the service's
/// own words where it gave any — that string is what the probe route later reports and the log later prints.
pub(crate) fn refusal_reason(err: &StoreError) -> String {
    if let Some(unavailable) = as_unavailable(err) {
        return unavailable.reason.clone();
    }
    if let StoreError::DeadLink(dead) = err {
        return dead.reason.clone();
    }
    describe(err)
}

/// Go `backedOff` (stores.go:1481): this account was turned away a moment ago, for this hash or outright.
pub(crate) fn backed_off(
    cache: &dyn Cache,
    svc: DebridService,
    token: &str,
    info_hash: &str,
) -> Option<String> {
    // The reason is handed on as an owned String (an error path, not the hit path): the copy `get` used to make.
    if let Some(reason) = cache.get(&account_refused_key(svc, token)) {
        return Some(reason.to_string());
    }
    cache.get(&refused_key(svc, token, info_hash)).map(|reason| reason.to_string())
}

/// Go `accountBackedOff` (stores.go:1496): the service rejected this ACCOUNT a moment ago, so nothing it
/// is asked will work.
///
/// This one DOES gate a read. The per-release add backoff must not (a read cannot have caused it), but a
/// rejected key makes every request pointless, including one for a release the account holds — and
/// without this the read path recorded the refusal and then asked again on the very next poll.
pub(crate) fn account_backed_off(cache: &dyn Cache, svc: DebridService, token: &str) -> Option<String> {
    cache.get(&account_refused_key(svc, token)).map(|reason| reason.to_string())
}

/// Go `refusalIsAboutTheAccount` (stores.go:1505): a status the service returns about WHO is asking, not
/// about what was asked for. Anything else is remembered per release.
pub(crate) fn refusal_is_about_the_account(err: &StoreError) -> bool {
    as_unavailable(err).is_some_and(|u| u.status == 401 || u.status == 403)
}

/// Go `scoutSideReason` (stores.go:1521): what scout is busy with, for a client that can only show a
/// sentence. The wrapped StoreUnavailableError still carries the wording; only the blame changes.
pub(crate) fn scout_side_reason(err: &StoreError) -> String {
    match as_unavailable(err) {
        Some(unavailable) => unavailable.reason.clone(),
        None => "scout is not accepting this request right now".to_owned(),
    }
}

/// Go `recordRefusal` (stores.go:1538) remembers a refusal briefly, so a poll loop cannot sustain one.
///
/// Only TorBox had this. The other two had no cache at all, so `ResolvePreferring` fell straight through
/// to them and added once per poll for the whole length of a download — several hundred adds on Real-
/// Debrid for one wait, each leaving a duplicate torrent on the account. The backoff belongs to every
/// store that can be refused, not to the one whose refusal was noticed first.
///
/// A cancellation is not a refusal: the caller went away, and nothing about the store or the release can
/// be concluded from it.
pub(crate) fn record_refusal(
    cache: &dyn Cache,
    svc: DebridService,
    token: &str,
    info_hash: &str,
    err: &StoreError,
) {
    // errTorrentGone joins the exclusions: "this account no longer has that torrent" is a fact about the
    // TORRENT, not the service declining. Remembered as a refusal it blocked the re-buy that fact exists
    // to trigger.
    if is_cancellation(err)
        || matches!(err, StoreError::ScoutSide(_) | StoreError::AddInFlight)
        || is_torrent_gone(err)
    {
        return;
    }
    if refusal_is_about_the_account(err) {
        cache.put_class(
            &account_refused_key(svc, token),
            &refusal_reason(err),
            REFUSAL_BACKOFF,
            CacheClass::Volatile,
        );
        log_limited(
            &format!("account-refused:{}", svc.as_str()),
            &format!("{} refused the account itself ({}) — check the key", svc.as_str(), refusal_reason(err)),
        );
        return;
    }
    cache.put_class(
        &refused_key(svc, token, info_hash),
        &refusal_reason(err),
        REFUSAL_BACKOFF,
        CacheClass::Volatile,
    );
}

impl StorePool {
    /// Go `StorePool.RecentRefusal` (stores.go:1123) reports the first store that was turned away for this
    /// hash a moment ago, with its reason.
    ///
    /// The probe route needs this because it cannot discover a refusal on its own: it deliberately never
    /// calls the endpoint that refuses. Without it a throttled account reads as "nothing is queued", which
    /// the client cannot distinguish from a release nobody can deliver — and it condemns a healthy one.
    pub fn recent_refusal(&self, info_hash: &str) -> Option<(DebridService, String)> {
        self.stores.iter().find_map(|st| st.recent_refusal(info_hash).map(|reason| (st.service(), reason)))
    }

    /// Go `StorePool.AccountRefusal` (stores.go:1170) reports a rejected key, which outranks an add of ours
    /// being in flight.
    ///
    /// A DEAD KEY IS NOT A WAIT. All three stores order it that way and say so — accountBackedOff sits above
    /// addInFlight in every one of them, because a live marker pre-empting a rejected key answers 202
    /// "downloading" where the store itself answers 503. The probe route consulted its in-flight reporter
    /// first and so did exactly what that comment forbids.
    ///
    /// Account-level only. The per-release backoff must NOT be checked here: it is an add-path guard a
    /// read-only caller is exempt from, and backedOff conflates the two by consulting the account key first.
    pub fn account_refusal(&self) -> Option<(DebridService, String)> {
        self.stores.iter().find_map(|st| st.account_refused().map(|reason| (st.service(), reason)))
    }

    /// Go `StorePool.EveryAddRefusedByScout` (stores.go:1203) reports that scout's own allowance is spent on
    /// every configured account, so no store could queue anything even if asked.
    ///
    /// It exists because a refusal SCOUT makes is invisible to the probe route otherwise. recordRefusal
    /// deliberately excludes errScoutSide — scout's ceiling is not the debrid declining, and filing it as
    /// one blames a healthy service — so the backoff memory the probe reads stays empty, and the route
    /// falls through its three "could not ask" guards to 404 "not queued". Measured with the hourly
    /// allowance spent, same release, same instant: /play answered 503 scout_busy while ?probe=1 answered
    /// 404. The budget is per account and rolling, so once the ceiling is hit every uncached release on that
    /// account answers that way for the rest of the hour, and 404 is the answer that makes a client
    /// blacklist them.
    pub fn every_add_refused_by_scout(&self) -> bool {
        let mut asked = 0;
        for st in &self.stores {
            let Some(spent) = st.scout_budget_spent() else { continue };
            asked += 1;
            if !spent {
                return false;
            }
        }
        asked > 0
    }

    /// Go `StorePool.HoldingServices` (stores.go:1269) names the stores that already have an id for this
    /// release, so it can be resolved without buying anything.
    ///
    /// It exists because a cache check is NOT that question, and ResolveCachedOnly refuses outright when
    /// given no holders. Real-Debrid publishes no cache API and answers CacheCheck all-false by design, so
    /// HeldBy is always empty for it — which made the probe route's readiness enquiry unreachable on an
    /// RD-only install. Every implementation is a cache read, so asking costs nothing on the installs where
    /// it adds nothing.
    pub fn holding_services(&self, t: &ResolveTarget) -> Vec<DebridService> {
        self.stores.iter().filter(|st| st.holds_torrent(t) == Some(true)).map(|st| st.service()).collect()
    }

    /// Go `StorePool.AddInFlight` (stores.go:1330): some store with a USABLE key has an add out for this
    /// release whose outcome we never saw.
    ///
    /// It is the one fact in this package that is about US rather than about a service, and both routes
    /// must give the same answer to it. /play does via errAddInFlight raised inside the resolve; the probe
    /// route cannot ask that way, because it never resolves — NoAdd is refused before any store consults
    /// the marker — so without this it fell through to 404 "not queued" for a release /play was reporting
    /// as downloading at the same instant.
    ///
    /// Two rules meet here and they point different ways, which is what made an earlier version wrong:
    ///
    ///   - Per store, a rejected key outranks that store's own add in flight. Every store's Resolve says
    ///     so: a dead key is not a wait, and accountBackedOff sits above addInFlight in all three.
    ///   - Across stores, an add in flight ANYWHERE outranks a refusal elsewhere. ResolvePreferring
    ///     returns `coming` before `refused`, and the comment there explains why store order must not
    ///     decide it.
    ///
    /// Applying only the first rule at pool level — an account-refusal gate above this one — made a dead
    /// key on account A outrank a live add on account B. So the per-store rule is applied per store — a
    /// store whose key is rejected cannot be fetching anything, whatever its marker says — and the
    /// cross-store rule falls out of the loop.
    pub fn add_in_flight(&self, info_hash: &str) -> bool {
        for st in &self.stores {
            if st.account_refused().is_some() {
                continue;
            }
            if st.add_in_flight(info_hash) == Some(true) {
                return true;
            }
        }
        false
    }
}

/// Go `addStillBelievable` (stores.go:1356) mirrors addInFlight's ORDER, and the order is the point.
///
/// addInFlight — which /play's resolve consults — checks unknownTooLong FIRST and converts to a dead
/// link past addGiveUp. Reading addOutcomeUnknown on its own skipped that, and the two markers have
/// different lifetimes (the attempt marker 90s, the unknown-outcome stamp 30 minutes), so they overlap:
/// the last add before the give-up leaves an attempt marker live for up to ninety seconds past it.
/// Measured in that window: ?probe=1 answered 202 downloading while /play answered 404 dead_link, for
/// the same release at the same instant.
///
/// That is the defect this reporter was added to fix, pointing the other way, and it defeats what
/// addGiveUp is for — "past this the release is reported dead so the client can fall through" — on the
/// one URL the client polls to decide whether to keep waiting.
pub(crate) fn add_still_believable(
    cache: &dyn Cache,
    svc: DebridService,
    token: &str,
    info_hash: &str,
) -> bool {
    if unknown_too_long(cache, svc, token, info_hash) {
        return false;
    }
    add_outcome_unknown(cache, svc, token, info_hash)
}
