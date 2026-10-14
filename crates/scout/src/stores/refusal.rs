//! Port of the cross-store bookkeeping in `internal/scout/stores.go` (lines 780-1560): cache keys for
//! resolve and torrent ids, add attempts, queued and held markers, the unknown-outcome rules, refusal
//! backoff, and the `StorePool` reporters built on them. Owned by the "Stores II" work package; the
//! TorBox store (`torbox.rs`) calls into it.

use std::time::Duration;

use crate::cache::Cache;
use crate::config::DebridService;
use crate::stores::StoreError;

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
/// Go `refusalBackoff` (stores.go:1557): how long a refusal holds a store off.
pub(crate) const REFUSAL_BACKOFF: Duration = Duration::from_secs(60);

/// Go `resolveKey` (stores.go:782): the cached torrent id and file list for a hash on this account. Scoped
/// by the canonical TorBox identity, because the value is an account-scoped torrent id.
pub(crate) fn resolve_key(identity: &str, info_hash: &str) -> String {
    let identity = crate::stores::torbox::canonical_torbox_identity(identity);
    format!("torbox:resolve:{}:{}", crate::util::key_hash(&identity), info_hash)
}

/// Go `torrentIDKey` (stores.go:786): the torrent id alone, kept apart from the resolve entry so a queued
/// torrent with no file list is still addressable by `Status`.
pub(crate) fn torrent_id_key(identity: &str, info_hash: &str) -> String {
    let identity = crate::stores::torbox::canonical_torbox_identity(identity);
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
            if rest.is_empty() { message[..start].to_owned() } else { rest.to_owned() }
        }
        // An unterminated clause cannot be told from the URL itself, so nothing after the lead-in is kept.
        None => message[..start].to_owned(),
    }
}

/// Go `refusedKey` (stores.go:1466): the per-release refusal memory.
pub(crate) fn refused_key(svc: DebridService, token: &str, info_hash: &str) -> String {
    todo!("stores.go refusedKey")
}

/// Go `accountRefusedKey` (stores.go:1476): the per-account refusal memory.
pub(crate) fn account_refused_key(svc: DebridService, token: &str) -> String {
    todo!("stores.go accountRefusedKey")
}

/// Go `unknownOutcomeKey` (stores.go:998).
pub(crate) fn unknown_outcome_key(svc: DebridService, token: &str, info_hash: &str) -> String {
    todo!("stores.go unknownOutcomeKey")
}

/// Go `pmQueuedKey` (stores.go:889): the Premiumize queued-transfer marker.
pub(crate) fn pm_queued_key(token: &str, info_hash: &str) -> String {
    todo!("stores.go pmQueuedKey")
}

/// Go `addOutcomeUnknown` (stores.go:853).
pub(crate) fn add_outcome_unknown(cache: &dyn Cache, svc: DebridService, token: &str, info_hash: &str) -> bool {
    todo!("stores.go addOutcomeUnknown")
}

/// Go `noteAddAttempt` (stores.go:867): records an add attempt for the release.
pub(crate) fn note_add_attempt(cache: &dyn Cache, svc: DebridService, token: &str, info_hash: &str) {
    todo!("stores.go noteAddAttempt")
}

/// Go `noteHeld` (stores.go:941): the six-hour "this account held it" mark.
pub(crate) fn note_held(cache: &dyn Cache, token: &str, info_hash: &str) {
    todo!("stores.go noteHeld")
}

/// Go `heldRecently` (stores.go:958).
pub(crate) fn held_recently(cache: &dyn Cache, token: &str, info_hash: &str) -> bool {
    todo!("stores.go heldRecently")
}

/// Go `alreadyQueued` (stores.go:966): whether a queued-transfer marker is on record.
pub(crate) fn already_queued(cache: &dyn Cache, token: &str, info_hash: &str) -> bool {
    todo!("stores.go alreadyQueued")
}

/// Go `backedOff` (stores.go:1481): the refusal on record for this release, if any, with its reason.
pub(crate) fn backed_off(cache: &dyn Cache, svc: DebridService, token: &str, info_hash: &str) -> Option<String> {
    todo!("stores.go backedOff")
}

/// Go `recordRefusal` (stores.go:1538): remembers a refusal the service made. Cancellations, scout's own
/// refusals (`ERR_SCOUT_SIDE`), in-flight adds and gone torrents are not refusals and are not recorded.
pub(crate) fn record_refusal(
    cache: &dyn Cache,
    svc: DebridService,
    token: &str,
    info_hash: &str,
    err: &StoreError,
) {
    todo!("stores.go recordRefusal")
}
