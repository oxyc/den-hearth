//! Port of the Premiumize half of `internal/scout/stores.go` (lines 4316-4722): `premiumizeStore`.
//! Owned by the "Stores II" work package.

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use serde::Deserialize;

use crate::addbudget::spend_add;
use crate::cache::Cache;
use crate::config::DebridService;
use crate::scrape::Doer;
use crate::season::{
    ambiguous_episode_guess, largest_episode_candidate, largest_playable, pick_episode_file, TorrentFile,
};
use crate::stores::refusal::{
    account_backed_off, add_in_flight, add_outcome_unknown, add_still_believable, already_queued, backed_off,
    batches_failed, cache_or_none, forget_held, held_recently, is_cancellation, known_cached, note_add_attempt,
    note_held, note_queued, note_unknown_outcome, pending_too_long, record_refusal, refund_unused_add,
    remember_cached, scout_budget_spent, settle_add_attempt, settle_queued_transfer, store_error_from_do,
    unknown_outcome,
};
use crate::stores::torbox::CACHE_BATCH;
use crate::stores::{
    build_request, dead_link, decode_all, decode_first, encode_form, go_query_escape, join_all, magnet_for,
    null_default, read_limited, redact_token, store_error_text, store_refused_us, unavailable, ResolveTarget, Store,
    StoreError, StoreStatus, ERR_WOULD_ADD, MAX_STORE_BYTES,
};
use crate::util::{Shape, LEAF};

/// Go `premiumizeAPI`.
pub const PREMIUMIZE_API: &str = "https://www.premiumize.me/api";

/// Go `premiumizeStore`.
pub struct PremiumizeStore {
    pub token: String,
    pub client: Arc<dyn Doer>,
    pub cache: Option<Arc<dyn Cache>>,
    pub api: String,
}

/// The `/cache/check` answer.
#[derive(Default, Deserialize)]
struct PmCheck {
    #[serde(default, deserialize_with = "null_default")]
    status: String,
    #[serde(default, deserialize_with = "null_default")]
    response: Vec<bool>,
}

static PM_CHECK_SHAPE: Shape = Shape::Object(&[("status", &LEAF), ("response", &LEAF)]);

/// One entry of the `directdl` content list.
#[derive(Default, Deserialize)]
struct PmContent {
    #[serde(default, deserialize_with = "null_default")]
    path: String,
    #[serde(default, deserialize_with = "null_default")]
    link: String,
    #[serde(default)]
    size: Option<i64>,
}

/// The `directdl` answer.
#[derive(Default, Deserialize)]
struct PmDirect {
    #[serde(default, deserialize_with = "null_default")]
    status: String,
    #[serde(default, deserialize_with = "null_default")]
    message: String,
    #[serde(default, deserialize_with = "null_default")]
    content: Vec<PmContent>,
}

static PM_CONTENT_ITEM_SHAPE: Shape = Shape::Object(&[("path", &LEAF), ("link", &LEAF), ("size", &LEAF)]);
static PM_CONTENT_SHAPE: Shape = Shape::Array(&PM_CONTENT_ITEM_SHAPE);
static PM_DIRECT_SHAPE: Shape =
    Shape::Object(&[("status", &LEAF), ("message", &LEAF), ("content", &PM_CONTENT_SHAPE)]);

impl PremiumizeStore {
    fn cache(&self) -> &dyn Cache {
        cache_or_none(&self.cache)
    }

    /// Go `premiumizeStore.budgetAccount`.
    pub(crate) fn budget_account(&self) -> String {
        crate::addbudget::budget_account(DebridService::Premiumize, &self.token)
    }

    /// One `/cache/check` round. `None` is a batch that could not be read, which leaves its hashes unknown.
    async fn check_batch(&self, batch: &[String]) -> Option<Vec<bool>> {
        let mut query = format!("apikey={}", go_query_escape(&self.token));
        for h in batch {
            query.push_str("&items%5B%5D=");
            query.push_str(&go_query_escape(h));
        }
        let req = build_request(reqwest::Method::GET, &format!("{}/cache/check?{query}", self.api), &[], None).ok()?;
        let resp = self.client.do_request(req).await.ok()?;
        if resp.status().as_u16() != 200 {
            return None;
        }
        let raw = read_limited(resp, MAX_STORE_BYTES).await.ok()?;
        let body = decode_first::<PmCheck>(&raw, &PM_CHECK_SHAPE)?;
        if body.status != "success" {
            return None;
        }
        Some(body.response)
    }

    /// Go `premiumizeStore.pickIndex`.
    pub(crate) fn pick_index(&self, files: &[TorrentFile], t: &ResolveTarget) -> Result<Option<i64>, StoreError> {
        if files.is_empty() {
            return Ok(None);
        }
        // Series episode: name-match against the pack first — Torrentio's fileIdx and Premiumize's content
        // order aren't guaranteed to agree, so a positional index can pick the wrong episode. A pack that
        // names other episodes and not this one is refused, not approximated by the fileIdx or largest file.
        if let (Some(season), Some(episode)) = (t.season, t.episode) {
            if let Some(id) = pick_episode_file(files, season, episode)? {
                return Ok(Some(id));
            }
        }
        if let Some(idx) = t.file_idx {
            if usize::try_from(idx).is_ok_and(|i| i < files.len()) {
                return Ok(Some(idx));
            }
        }
        // Below the indexer's position, above the last-resort largest: a number that named several files did
        // not tell them apart, but on a dual-quality pack those several ARE the episode and the better copy
        // wins. Falling straight through to "largest video anywhere" served episode 4 of an eight-file pack
        // for a request for episode 1.
        if let (Some(_), Some(episode)) = (t.season, t.episode) {
            if let Some(id) = ambiguous_episode_guess(files, episode) {
                return Ok(Some(id));
            }
        }
        // Largest VIDEO — see the note on RD's tail: this fallback was unreachable for an unlabelled pack
        // until pickEpisodeFile stopped answering for one, and it picks from every file, .iso included.
        if t.season.is_some() && t.episode.is_some() {
            return Ok(largest_episode_candidate(files).map(|f| f.index));
        }
        Ok(largest_playable(files).map(|f| f.index))
    }
}

#[async_trait]
impl Store for PremiumizeStore {
    fn service(&self) -> DebridService {
        DebridService::Premiumize
    }

    async fn cache_check(&self, hashes: &[String]) -> Result<HashMap<String, bool>, StoreError> {
        let svc = DebridService::Premiumize;
        let (mut result, hashes) = known_cached(self.cache(), svc, &self.token, hashes);
        if hashes.is_empty() {
            return Ok(result);
        }
        let batches: Vec<&[String]> = hashes.chunks(CACHE_BATCH).collect();
        let answers = join_all(batches.iter().map(|batch| self.check_batch(batch)).collect()).await;
        let batch_ok: Vec<bool> = answers.iter().map(Option::is_some).collect();
        // As with TorBox: a failed batch leaves its hashes absent rather than claiming they are not cached.
        for (i, h) in hashes.iter().enumerate() {
            if let Some(response) = &answers[i / CACHE_BATCH] {
                result.insert(h.clone(), response.get(i % CACHE_BATCH).copied().unwrap_or(false));
            }
        }
        remember_cached(self.cache(), svc, &self.token, &result);
        batches_failed(&batch_ok)?;
        Ok(result)
    }

    /// Premiumize likewise: cached or not, with no in-between to report.
    async fn status(&self, _t: &ResolveTarget) -> Option<StoreStatus> {
        None
    }

    async fn resolve(&self, t: &ResolveTarget) -> Result<String, StoreError> {
        let svc = DebridService::Premiumize;
        let cache = self.cache();
        // No dedicated account gate here, unlike TorBox and Real-Debrid: Premiumize has no read path at all —
        // directdl is both the question and the purchase — so nothing here reaches the service without first
        // passing the backoff below, and `backedOff` already consults the account key. A separate check would
        // be a no-op wearing the look of a guard. (Two paths DO return above it, `NoAdd` and `addInFlight`,
        // but neither speaks to Premiumize.) TorBox and RD need theirs because their held-torrent paths do
        // talk to the service before any backoff is reached.
        //
        // NoAdd first: it costs nothing and the guards below all describe the cost of adding. TorBox orders
        // it this way for the same reason — a read-only caller cannot have caused a backoff and must not be
        // blocked by one.
        //
        // directdl queues a transfer for anything the account does not already hold, so it is an add — EXCEPT
        // for a transfer we already queued, where it is the read that discovers the transfer finished. The
        // charge below is skipped in exactly that case (`if !queued`), so letting a read-only caller through
        // here buys nothing and costs nothing.
        //
        // Refusing it unconditionally is what left the probe route unable to see a Premiumize release become
        // READY. Nothing on that path can clear pmQueuedKey — settleQueuedTransfer is reached only from the
        // content branch below, which this return made unreachable for a NoAdd caller — so once scout queued
        // a transfer the probe answered 202 "downloading" until the marker aged past pendingGiveUp and then
        // 404, never 200, while /play answered 302 with a playable link. Measured on a completed transfer:
        // probe 202, /play 302, on the one URL that exists to tell a client its download landed.
        if t.no_add && !already_queued(cache, &self.token, &t.info_hash) && !held_recently(cache, &self.token, &t.info_hash)
        {
            return Err(dead_link(ERR_WOULD_ADD));
        }
        // The account gate the other two have. It was genuinely redundant here — `backedOff` consults the
        // account key first, and nothing that returned above it spoke to Premiumize — but `addInFlight` now
        // does return above it, so a live marker pre-empted a rejected key and answered 202 "downloading"
        // where TorBox and RD answer 503. A dead key is not a wait.
        if let Some(reason) = account_backed_off(cache, svc, &self.token) {
            return Err(unavailable(svc, format!("{reason} (backing off)"), 0));
        }
        add_in_flight(cache, svc, &self.token, &t.info_hash)?;
        // The same backoff TorBox has — a poll loop must not be able to sustain a refusal here either.
        if let Some(reason) = backed_off(cache, svc, &self.token, &t.info_hash) {
            return Err(unavailable(svc, format!("{reason} (backing off)"), 0));
        }
        // A transfer we already queued. Do NOT skip the call — directdl is the only thing that can discover
        // the transfer finished, so blocking it made a release that completed in two minutes unresolvable for
        // the remaining eighteen. What the marker suppresses is the CHARGE: the transfer is already paid for,
        // so asking about it again must not spend another add.
        // ONLY the queue marker suppresses the charge. The held mark must not, whatever it costs in tidiness:
        // it is a six-hour CLAIM about the account, and a claim can be wrong, whereas a pending transfer is
        // something scout itself queued and already paid for.
        //
        // Letting the held mark in here was measured and it is the expensive failure this function already
        // records once, arriving through a different door. Once Premiumize evicts the release — or the user
        // removes it — the mark still reads "held" for the rest of its six hours, so the NoAdd gate lets a
        // read-only poll through to directdl, which IS the purchase; the charge is skipped, so the fifty an
        // hour never sees it; and success-with-no-content skips noteQueued too, so nothing bounds the claim:
        //
        //   parent commit: 30 polls -> 2 directdl, allowance 50->49, queue marker set (10-min give-up)
        //   with the mark: 30 polls -> 32 directdl, allowance 50->50, NO marker, no give-up at all
        //
        // Sixty polls is two minutes of a viewer sitting there. The comment on pmHeldKey said this outright —
        // "must NOT suppress a later charge the way a pending transfer does" — and the code did the opposite.
        let queued = already_queued(cache, &self.token, &t.info_hash);
        // Charged BEFORE the call, because the budget has to be able to gate it — directdl queues a transfer
        // for anything the account lacks, so a spent allowance must stop the request, not merely record it.
        // But directdl is a READ for anything Premiumize already holds, and charging every one of those billed
        // an add per play: twenty-four episodes of a pack the account already had spent twenty-four of the
        // hourly fifty while adding nothing. So the charge is given back below the moment the answer shows
        // nothing was queued.
        if !queued {
            // addWouldMissTheClock has no counterpart: a store takes no context, so a spent clock drops the
            // future before the charge instead of arriving at it.
            spend_add(svc, &self.token, &t.info_hash, t.prefetch)?;
        }
        let form = encode_form(&[("apikey", &self.token), ("src", &magnet_for(&t.info_hash))]);
        let req = build_request(
            reqwest::Method::POST,
            &format!("{}/transfer/directdl", self.api),
            &[("content-type", "application/x-www-form-urlencoded")],
            Some(form),
        )?;
        // Not for a read-only caller. The marker means "an add of OURS is out"; a NoAdd poll is reached here
        // only for a transfer already queued, where this call is the READ that discovers it finished, so it
        // has no add of its own to record.
        //
        // Letting it through was measured: the probe route runs on the client's context under statusBudget,
        // and clients cancel aggressively, so a cut-off poll left a ninety-second in-flight marker behind.
        // /play then short-circuits on that marker without ever calling directdl and answers 202
        // "downloading" for a release Premiumize already HOLDS — unplayable for the marker's life, with no
        // Status API to rescue it. A read-only route stranded a release by writing add-path memory, which is
        // the contract recordRefusalFor exists to state; the commit that widened the gate above fixed that
        // contract for Real-Debrid and missed the store it had just widened.
        if !t.no_add {
            note_add_attempt(cache, svc, &self.token, &t.info_hash);
        }
        let resp = match self.client.do_request(req).await.map_err(store_error_from_do) {
            Ok(resp) => resp,
            Err(err) => {
                // The third store gets the same rule. Its ordering already answered 202 from the marker, so
                // the client saw the right thing — but the refusal was still written, and a refusal is read
                // by the probe route and by every other release on this account's per-hash key.
                if add_outcome_unknown(cache, svc, &self.token, &t.info_hash) {
                    // Not on a cancellation. /play runs on the client's context and a focus change is
                    // enough, so a viewer backing out wrote a give-up stamp that outlived them: past
                    // addGiveUp every resolve of that release answered a dead link for the rest of
                    // unknownOutcomeTTL, with no upstream call able to clear it — RD and Premiumize have no
                    // Status to rediscover the torrent with. A deadline belongs on a service that never
                    // answered, not on a client that hung up. This is the same guard recordRefusal applies
                    // one branch over, for the same reason.
                    // Nor when the add itself already judged the outcome: it returns errAddInFlight from the
                    // body-read branch, which is not recognisably a cancellation out here, so stamping again
                    // undid the guard one layer down.
                    if !is_cancellation(&err) && !matches!(err, StoreError::AddInFlight) {
                        note_unknown_outcome(cache, svc, &self.token, &t.info_hash);
                    }
                } else {
                    record_refusal(cache, svc, &self.token, &t.info_hash, &err);
                }
                return Err(err);
            }
        };
        let status = resp.status().as_u16();
        // Read before settling, the same order RD and TorBox use: the read is the last thing that can fail,
        // and a status line whose body we could not read is an add whose outcome we never saw. Premiumize was
        // the store this never reached. Its `!readable` branch REFUNDED the charge and returned a bare dead
        // link, leaving no memory of the add on any of the four channels — charge given back, marker already
        // cleared, noteQueued never reached, no refusal recorded — so thirty polls of one release made thirty
        // real directdl calls with the allowance still at fifty. Nothing bounded it, the budget least of all,
        // because the refund kept handing it back. directdl IS the fetch, so each of those queued a transfer.
        let raw = match read_limited(resp, MAX_STORE_BYTES).await {
            Ok(raw) => raw,
            // Marker left set and the charge kept: the next poll answers 202 from the marker instead of
            // buying the transfer again.
            Err(read_err) => return Err(unknown_outcome(cache, svc, &self.token, &t.info_hash, &read_err)),
        };
        settle_add_attempt(cache, svc, &self.token, &t.info_hash);
        // Premiumize ANSWERED, and every answer below this line is one that queued nothing — so the charge
        // goes back on all of them, the same rule the success path follows further down. Only the transport
        // failure above keeps its charge, because there the outcome is genuinely unknown.
        //
        // Keeping it was the more expensive mistake by far. The allowance is a rolling in-memory window with
        // no reset but a restart, so a single bad magnet polled every two seconds spent all fifty in about a
        // hundred seconds — after which `spendAdd` refused EVERY Premiumize resolve for the rest of the hour,
        // including releases the account already holds that directdl would have served instantly. A refused
        // magnet is a nuisance; an hour of 503 scout_busy on a working store is an outage.
        if !(200..300).contains(&status) {
            if !queued {
                refund_unused_add(svc, &self.token);
            }
            // Redacted for the same reason TorBox's is: the apikey rides in directdl's form body, and this
            // text is not merely logged but PERSISTED into the refusal cache, from where the probe route
            // prints it verbatim. An upstream error page that quotes the request back would put it there.
            let detail = redact_token(&store_error_text(&raw), &self.token);
            // Same rule as the other two stores: being turned away says nothing about the release.
            if store_refused_us(status) {
                let refused = unavailable(svc, format!("directdl http {status}{detail}"), status);
                record_refusal(cache, svc, &self.token, &t.info_hash, &refused);
                return Err(refused);
            }
            // Backed off for the same reason RD's twin is: without it a poll loop re-asks a magnet Premiumize
            // has already rejected once every couple of seconds for as long as the viewer sits there. The
            // answer to this poll stays a dead link so the client can fall through to another release.
            let dead = dead_link(format!("premiumize directdl http {status}{detail}"));
            record_refusal(cache, svc, &self.token, &t.info_hash, &dead);
            return Err(dead);
        }
        // Premiumize accepted it. Same reason as RD: no Status endpoint, so without this the next poll
        // queues the same transfer again.
        let body = decode_all::<PmDirect>(&raw, &PM_DIRECT_SHAPE);
        // Premiumize answers an application-level refusal as HTTP 200 with `{"status":"error","message":…}` —
        // an unsupported magnet, an account limit, no space. Only ONE of the three ways this call can fail to
        // hand back content means a transfer was queued: a SUCCESSFUL answer that carries none. Collapsing
        // all three charged an add for a transfer that does not exist, stamped the queue marker for its full
        // twenty minutes (suppressing any later legitimate attempt), and answered 202 "downloading, 0%" for
        // ten minutes on a magnet Premiumize had refused outright — a spinner the client cannot fall through.
        let body = match body {
            Some(body) if body.status == "success" => body,
            body => {
                // Nothing was queued, so the charge must go back the same way the read path's does.
                if !queued {
                    refund_unused_add(svc, &self.token);
                }
                // Remembered, like every other answered failure on all three stores. This branch is the one
                // that carries Premiumize's REAL refusals — it reports an unsupported magnet, an account at
                // its limit and "no space" as HTTP 200 with `{"status":"error"}` — and it was the only one
                // recording nothing at all: charge handed back, no marker, no backoff, no queue note. Thirty
                // polls made thirty real directdl calls with the allowance still at fifty, because the refund
                // kept giving it back. Worse for an account out of space, where EVERY release answers this
                // way: twenty healthy releases condemned as dead in sixty calls, and neither /play nor
                // ?probe=1 ever naming Premiumize, because nothing was written for them to read.
                //
                // Still a dead link to the client, though. Its own words — "Invalid src" and "not enough
                // space" need different fixes and only the text says which — and this is not a refusal of
                // the ACCOUNT: a 401/403/429/5xx is what that looks like, and it was handled above.
                let dead = match body {
                    None => dead_link("premiumize directdl answered something unreadable"),
                    Some(body) => {
                        // Redacted like its sibling a few lines up, and for the reason that comment already
                        // gives: this text is not merely logged, it is PERSISTED into the refusal cache,
                        // which writes through to disk, and the probe route prints it back verbatim. The
                        // apikey rides in this very request's form body, so an upstream message quoting the
                        // request carries it — the same premise every other redaction site here is built on.
                        // This branch was the one left out, and it is the one carrying Premiumize's real
                        // refusals.
                        let msg = redact_token(body.message.trim(), &self.token);
                        let clipped = String::from_utf8_lossy(&msg.as_bytes()[..msg.len().min(200)]).into_owned();
                        dead_link(format!(
                            "premiumize directdl: {}",
                            format!("{} {clipped}", redact_token(&body.status, &self.token)).trim()
                        ))
                    }
                };
                record_refusal(cache, svc, &self.token, &t.info_hash, &dead);
                return Err(dead);
            }
        };
        if body.content.is_empty() {
            // A successful answer with nothing in it: this call really did queue a transfer, so the charge
            // stands.
            //
            // It also PROVES the account does not hold this release — a held one comes back with content —
            // so the held mark goes, and that is the only way it can go. Written with a six-hour life and no
            // clearing path, it outlived every eviction: TorBox and Real-Debrid forget a remembered id the
            // moment the service says the torrent is gone, and this had no such twin.
            forget_held(cache, &self.token, &t.info_hash);
            if !queued {
                note_queued(cache, &self.token, &t.info_hash);
            }
            // "Coming" is a claim with a deadline. Refreshing the marker on every poll made it an absorbing
            // state: a dead magnet or a transfer that errored answered 202 at 0% forever, and the client
            // cannot fall through to another release while it is being told one is on its way. Past the
            // window the answer becomes a dead link, which is what it has turned out to be.
            if pending_too_long(cache, &self.token, &t.info_hash) {
                // Remembered in the QUEUE marker, not the refusal cache. Filing it as a store refusal made
                // every following poll a 503 naming Premiumize — which tells the viewer their debrid is
                // refusing and stops the client trying other sources, for a release scout itself just
                // condemned, on a store that is answering perfectly well. The whole point of the dead link is
                // that the client moves on; a refusal is the one answer that prevents exactly that.
                return Err(dead_link("premiumize queued this transfer and never produced anything"));
            }
            return Err(StoreError::AddInFlight);
        }
        // Content came back, so the account HOLDS this release: nothing was queued and this call was a read.
        // The refund belongs HERE, before anything can return. Behind the file-selection checks below it was
        // skipped by the two failures a season pack actually produces — the pack does not contain the
        // requested episode, or the chosen entry carries no link — so every poll of such a pack charged
        // another add while queueing nothing. Twenty polls, twenty adds, the hourly allowance gone in under
        // two minutes, on the branch season packs always take.
        //
        // The transfer is likewise no longer pending, whatever we go on to make of its contents — but only a
        // caller that could have QUEUED it may say so.
        //
        // A read-only poll clearing this marker destroys the only thing that names Premiumize a holder, and
        // so destroys its own next answer: the probe reported 200 ready once, then 404 "not queued" on every
        // later poll, while /play kept serving a 302 from the same transfer. Measured across all five
        // cache-check outcomes — 200, then 404, 404. That 404 is what this route calls the single failure it
        // exists to prevent, so the read-only path leaves the marker for /play to settle. The marker's own
        // TTL bounds it either way.
        if !queued {
            refund_unused_add(svc, &self.token);
        }
        if !t.no_add {
            settle_queued_transfer(cache, &self.token, &t.info_hash);
            // ...and remember that the account HOLDS it, which is what the queue marker was standing in for.
            //
            // Clearing alone left Premiumize unable to name itself a holder at all: HoldsTorrent had only the
            // queue marker to read, so the first /play of a season pack settled it and every later probe for
            // another episode of the same infohash answered 404 "not queued" while /play kept serving 302s
            // from the same transfer. Measured across three polls of episode 2 after playing episode 1.
            //
            // Separate from the queue marker on purpose: this one says "served, no add needed", so it must
            // NOT suppress a later charge the way a pending transfer does. Same six hours as the other
            // stores' remembered ids, and for the same reason — it is a convenience claim, not a promise.
            note_held(cache, &self.token, &t.info_hash);
        }

        let files: Vec<TorrentFile> = body
            .content
            .iter()
            .enumerate()
            .map(|(i, c)| TorrentFile { index: i as i64, name: c.path.clone(), size_bytes: c.size })
            .collect();
        let idx = self.pick_index(&files, t)?;
        match idx.and_then(|i| usize::try_from(i).ok()).and_then(|i| body.content.get(i)) {
            Some(c) if !c.link.is_empty() => Ok(c.link.clone()),
            _ => Err(dead_link("premiumize no link")),
        }
    }

    fn recent_refusal(&self, info_hash: &str) -> Option<String> {
        backed_off(self.cache(), DebridService::Premiumize, &self.token, info_hash)
    }

    /// Go `premiumizeStore.AddInFlight`. Premiumize has a SECOND way to be mid-fetch, and it is the usual one.
    ///
    /// directdl answering success with no content means the transfer was queued: the store stamps
    /// pmQueuedKey and returns errAddInFlight, which /play renders as 202 downloading. That marker is not
    /// addOutcomeUnknown — settleAddAttempt clears the add marker the moment the body is read — so a probe
    /// asking only about the add saw nothing and answered 404 "not queued" for the entire ten minutes /play
    /// spends saying "downloading". Measured at 0s, 1m, 5m and 9m: disagree; at 10m and 19m: agree, because
    /// /play has itself given up by then.
    ///
    /// Bounded by pendingGiveUp rather than by the marker's own twenty-minute life, so the two routes stop
    /// agreeing at the same moment rather than swapping sides — past the give-up /play reports the release
    /// dead, and a probe still claiming 202 would be the same defect pointing the other way.
    fn add_in_flight(&self, info_hash: &str) -> Option<bool> {
        let cache = self.cache();
        let svc = DebridService::Premiumize;
        if add_still_believable(cache, svc, &self.token, info_hash) {
            return Some(true);
        }
        // NOTE FOR ANYONE TIGHTENING THIS: Premiumize's per-release refusal key is written by read-only
        // callers too, unlike Real-Debrid's, and that asymmetry is deliberate. For RD the key is purely an
        // add-path guard, so recordRefusalFor keeps a probe from writing one. Here it carries a SECOND
        // meaning — it is the only thing that withdraws the queue marker's "coming" claim, in the branch just
        // below — so suppressing the write suppressed the withdrawal: the probe answered 202 "downloading"
        // on every poll of a transfer Premiumize had refused, for the full ten-minute window, and re-asked
        // directdl once per poll for a magnet it kept refusing. Measured: 202/202/202 with four directdl
        // calls, against 503/503/503 with one when the refusal is recorded.
        //
        // The cost of recording it from a read-only poll is the one agent B measured and called bounded: the
        // next /play answers 503 with no upstream call where it would otherwise have answered 404 after one
        // — and /play writes the same record itself a poll later regardless.
        //
        // A refusal on record outranks the queue marker. directdl's answered-failure branches — an
        // unsupported magnet, an account at its limit, "not enough space", all of which Premiumize reports
        // as HTTP 200 with status:error — record a refusal and do NOT clear pmQueuedKey, so the marker
        // alone kept saying "coming" for the rest of the ten-minute window while /play, which actually made
        // the call, had the verdict and condemned the release. Measured: /play 404 then 503 on every later
        // poll, probe 202 downloading throughout, for ten minutes. An account out of space produces that
        // for every release it queues.
        //
        // Reading the refusal rather than clearing the marker on failure is deliberate: the marker also
        // suppresses the CHARGE, and clearing it would let a persistently failing account be charged once
        // per sixty-second backoff instead of once per twenty-minute marker. This changes only what the
        // probe reports, which is where the defect is.
        if backed_off(cache, svc, &self.token, info_hash).is_some() {
            return Some(false);
        }
        Some(already_queued(cache, &self.token, info_hash) && !pending_too_long(cache, &self.token, info_hash))
    }

    /// Go `premiumizeStore.HoldsTorrent`. Premiumize's equivalent of "we have an id for this" is the queue
    /// marker: directdl is the only handle on a transfer, and a marker means scout queued one and it may
    /// since have landed.
    ///
    /// Without this the READY fix one commit back only worked when Premiumize's own /cache/check answered
    /// "cached" — the sole other channel that can name PM as a holder. Every other outcome of that check
    /// (not cached, a 429, a transport error, an unreadable body) skipped the readiness enquiry entirely,
    /// left AddInFlight reading the still-set marker, and answered 202 "downloading" for a completed
    /// transfer /play was serving a 302 for. Measured across those five outcomes: one row 200, four rows
    /// 202. And the shared statusBudget makes the bad rows ordinary — the route's own comment notes a slow
    /// status read can leave the cache check a dead context.
    ///
    /// Keyed by hash alone, like the marker: directdl is per-magnet, so a pack queued for one episode
    /// answers for any episode of the same release, at no charge.
    fn holds_torrent(&self, t: &ResolveTarget) -> Option<bool> {
        Some(
            already_queued(self.cache(), &self.token, &t.info_hash)
                || held_recently(self.cache(), &self.token, &t.info_hash),
        )
    }

    fn scout_budget_spent(&self) -> Option<bool> {
        Some(scout_budget_spent(DebridService::Premiumize, &self.token))
    }

    fn account_refused(&self) -> Option<String> {
        account_backed_off(self.cache(), DebridService::Premiumize, &self.token)
    }
}
