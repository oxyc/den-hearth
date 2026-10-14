//! Port of the Real-Debrid half of `internal/scout/stores.go` (lines 3770-4316): `realDebridStore`.
//! Owned by the "Stores II" work package.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use serde::Deserialize;

use crate::addbudget::{refund_unused_add, spend_add};
use crate::cache::Cache;
use crate::config::DebridService;
use crate::linkcheck::{rd_size_key, size_of};
use crate::rank::real_debrid_blocked;
use crate::scrape::Doer;
use crate::season::{
    ambiguous_episode_guess, largest_episode_candidate, largest_playable, pick_episode_file, TorrentFile,
};
use crate::stores::refusal::{
    account_backed_off, add_in_flight, add_outcome_unknown, add_still_believable, as_unavailable, backed_off,
    cache_or_none, is_cancellation, is_torrent_gone, note_add_attempt, note_unknown_outcome, record_refusal,
    record_refusal_for, scout_budget_spent, settle_add_attempt, store_error_from_do, unknown_outcome,
};
use crate::stores::{
    build_request, dead_link, decode_first, encode_form, magnet_for, null_default, rd_fetch_state,
    read_limited, redact_token, store_error_text, store_refused_us, unavailable, ResolveTarget, Store,
    StoreError, StoreStatus, ERR_TORRENT_GONE, ERR_WOULD_ADD, MAX_STORE_BYTES, RESOLVE_CACHE_TTL,
};
use crate::util::{key_hash, Shape, LEAF};

/// Go `realDebridAPI`.
pub const REAL_DEBRID_API: &str = "https://api.real-debrid.com/rest/1.0";

/// Go `realDebridStore`.
pub struct RealDebridStore {
    pub token: String,
    pub client: Arc<dyn Doer>,
    pub cache: Option<Arc<dyn Cache>>,
    pub api: String,
}

/// Go `rdInfo`'s file entries.
#[derive(Default, Deserialize)]
pub(crate) struct RdFile {
    #[serde(default, deserialize_with = "null_default")]
    pub(crate) id: i64,
    #[serde(default, deserialize_with = "null_default")]
    pub(crate) path: String,
    #[serde(default, deserialize_with = "null_default")]
    pub(crate) bytes: i64,
    #[serde(default, deserialize_with = "null_default")]
    pub(crate) selected: i64,
}

/// Go `rdInfo`.
#[derive(Default, Deserialize)]
pub(crate) struct RdInfo {
    #[serde(default, deserialize_with = "null_default")]
    pub(crate) files: Vec<RdFile>,
    #[serde(default, deserialize_with = "null_default")]
    pub(crate) links: Vec<String>,
    // Status/Progress are what tell a download apart from a dead release. RD's own vocabulary:
    // magnet_conversion, waiting_files_selection, queued, downloading, compressing, uploading,
    // downloaded, error, virus, dead, magnet_error.
    #[serde(default, deserialize_with = "null_default")]
    pub(crate) status: String,
    #[serde(default, deserialize_with = "null_default")]
    pub(crate) progress: f64,
}

static RD_FILE_SHAPE: Shape =
    Shape::Object(&[("id", &LEAF), ("path", &LEAF), ("bytes", &LEAF), ("selected", &LEAF)]);
static RD_FILES_SHAPE: Shape = Shape::Array(&RD_FILE_SHAPE);
static RD_INFO_SHAPE: Shape =
    Shape::Object(&[("files", &RD_FILES_SHAPE), ("links", &LEAF), ("status", &LEAF), ("progress", &LEAF)]);

#[derive(Default, Deserialize)]
struct RdAdded {
    #[serde(default, deserialize_with = "null_default")]
    id: String,
}

static RD_ADDED_SHAPE: Shape = Shape::Object(&[("id", &LEAF)]);

#[derive(Default, Deserialize)]
struct RdUnrestricted {
    #[serde(default, deserialize_with = "null_default")]
    download: String,
}

static RD_UNRESTRICTED_SHAPE: Shape = Shape::Object(&[("download", &LEAF)]);

/// Go `rdFetching`: the statuses that mean the ACCOUNT is actively fetching this right now.
///
/// Listed explicitly rather than treated as "anything that is not downloaded", because the whole point is
/// never to turn a dead release into a promise: error, virus, dead and magnet_error must stay failures,
/// and waiting_files_selection is a torrent nothing has asked for yet.
fn rd_fetching(status: &str) -> bool {
    matches!(status, "magnet_conversion" | "queued" | "downloading" | "compressing" | "uploading")
}

/// Go `rdTorrentKey` (stores.go:4015): the cache key of the Real-Debrid torrent id for a release, with the
/// season and episode selector in it.
///
/// The RD torrent id this account created for an infohash. Account-scoped like every other key that
/// carries account state. Keyed by the FILE, not the torrent.
///
/// One infohash is a whole season pack, and Real-Debrid decides which file a torrent serves through
/// selectFiles — the links it then returns describe that selection. So a remembered id is only reusable
/// for the same target: reusing it across episodes re-selected on a torrent RD had already started and
/// served S01E01's link for a request for S01E02. Wrong content, silently, which is worse than the extra
/// add it was saving. A separate torrent per episode is how RD is meant to be used, and repeated polls of
/// the SAME episode — the loop that actually costs quota — still reuse one.
pub(crate) fn rd_torrent_key(token: &str, info_hash: &str, t: &ResolveTarget) -> String {
    let sel = match (t.season, t.episode, t.file_idx) {
        (Some(season), Some(episode), _) => format!(":s{season}e{episode}"),
        (_, _, Some(file_idx)) => format!(":f{file_idx}"),
        _ => String::new(),
    };
    format!("realdebrid:torrent:{}:{info_hash}{sel}", key_hash(token))
}

/// Go `rdLinkFor` picks the link belonging to a file id.
///
/// RD's `links` map one-to-one onto the SELECTED files, in file order — not onto every file, and not onto
/// the one thing we asked for. Returning links[0] regardless served S01E01's link for a request for
/// S01E02 on any torrent with more than one file selected, which is the normal state of a season pack RD
/// has already been asked about. Silently the wrong episode, which is worse than any cost it saved.
pub(crate) fn rd_link_for(info: &RdInfo, file_id: i64) -> Option<String> {
    let mut pos = 0;
    for f in &info.files {
        if f.selected == 0 {
            continue;
        }
        if f.id == file_id {
            return info.links.get(pos).cloned();
        }
        pos += 1;
    }
    // No file reports a selection — an older/leaner response shape. With exactly one link there is only
    // one thing it can be; with several, guessing is what caused the bug.
    if pos == 0 && info.links.len() == 1 {
        return Some(info.links[0].clone());
    }
    None
}

/// Go `rdFileSelected` reports whether RD says this file is part of the torrent's current selection.
fn rd_file_selected(info: &RdInfo, file_id: i64) -> bool {
    info.files.iter().find(|f| f.id == file_id).is_some_and(|f| f.selected == 1)
}

impl RealDebridStore {
    fn cache(&self) -> &dyn Cache {
        cache_or_none(&self.cache)
    }

    /// Go `realDebridStore.post`.
    async fn post(&self, path: &str, form: &[(&str, &str)]) -> Result<reqwest::Response, StoreError> {
        let req = build_request(
            reqwest::Method::POST,
            &format!("{}{}", self.api, path),
            &[
                ("authorization", &format!("Bearer {}", self.token)),
                ("content-type", "application/x-www-form-urlencoded"),
            ],
            Some(encode_form(form)),
        )?;
        self.client.do_request(req).await.map_err(store_error_from_do)
    }

    /// Go `realDebridStore.rememberTorrent`.
    pub(crate) fn remember_torrent(&self, t: &ResolveTarget, id: &str) {
        if let Some(cache) = &self.cache {
            cache.put(&rd_torrent_key(&self.token, &t.info_hash, t), id, RESOLVE_CACHE_TTL);
        }
    }

    /// Go `realDebridStore.knownTorrent`: the remembered torrent id and whether there is one.
    pub(crate) fn known_torrent(&self, t: &ResolveTarget) -> (String, bool) {
        let Some(cache) = self.cache.as_ref() else { return (String::new(), false) };
        match cache.get(&rd_torrent_key(&self.token, &t.info_hash, t)) {
            Some(id) => {
                let held = !id.is_empty();
                (id.to_string(), held)
            }
            None => (String::new(), false),
        }
    }

    /// Go `realDebridStore.forgetTorrent`.
    fn forget_torrent(&self, t: &ResolveTarget) {
        if let Some(cache) = &self.cache {
            cache.put(&rd_torrent_key(&self.token, &t.info_hash, t), "", Duration::from_nanos(1));
        }
    }

    /// Go `realDebridStore.rememberFileSize` (linkcheck.go:187) keeps the size RD's info reported, which no
    /// later call would otherwise see: the Store interface returns a link and nothing else.
    fn remember_file_size(&self, t: &ResolveTarget, files: &[TorrentFile], file_id: i64) {
        let Some(cache) = &self.cache else { return };
        if let Some(n) = size_of(files, file_id) {
            cache.put(&rd_size_key(&self.token, &t.info_hash, t), &n.to_string(), RESOLVE_CACHE_TTL);
        }
    }

    /// Go `realDebridStore.pickFileID` (stores.go:4269): the file id to unrestrict. Name match first, then
    /// the caller's position mapped to RD's own id, then the largest video; a refusal when the pack names
    /// episodes and not the one asked for.
    pub(crate) fn pick_file_id(
        &self,
        files: &[TorrentFile],
        t: &ResolveTarget,
    ) -> Result<Option<i64>, StoreError> {
        if files.is_empty() {
            return Ok(None);
        }
        // Series episode: name-match against the pack first — Torrentio's fileIdx and RD's file order aren't
        // guaranteed to agree, so a positional index can pick the wrong episode. A pack that names other
        // episodes and not this one is refused, not approximated by the fileIdx or the largest file.
        if let (Some(season), Some(episode)) = (t.season, t.episode) {
            if let Some(id) = pick_episode_file(files, season, episode)? {
                return Ok(Some(id));
            }
        }
        if let Some(idx) = t.file_idx {
            if let Some(f) = usize::try_from(idx).ok().and_then(|i| files.get(i)) {
                return Ok(Some(f.index));
            }
        }
        // An index out of range is the indexer describing a file list that is not this one — it arrives
        // unvalidated from the scrape and is never checked against what the debrid actually holds. Falling
        // back to files[0] handed back whatever sorted first, which on a release with a Sample/ directory is
        // the sample: a stream that plays, for one second. The largest is the same guess Premiumize makes and
        // the better one, and it is the answer for "no index at all" here already.
        //
        // Below the indexer's position, above the last-resort largest: a number that named several files did
        // not tell them apart, but on a dual-quality pack those several ARE the episode and the better copy
        // wins. Falling straight through to "largest video anywhere" served episode 4 of an eight-file pack
        // for a request for episode 1.
        if let (Some(_), Some(episode)) = (t.season, t.episode) {
            if let Some(id) = ambiguous_episode_guess(files, episode) {
                return Ok(Some(id));
            }
        }
        //
        // Largest VIDEO, not largest file. pickEpisodeFile used to end in this same fallback over its
        // video-only pool, so this tail was unreachable for a multi-file unlabelled pack; moving the fallback
        // out here exposed it, and a pack whose biggest entry is a .iso or a .rar then resolved to that, with
        // a 302 and no error.
        if t.season.is_some() && t.episode.is_some() {
            return Ok(largest_episode_candidate(files).map(|f| f.index));
        }
        Ok(largest_playable(files).map(|f| f.index))
    }

    /// Go `realDebridStore.info`.
    async fn info(&self, id: &str) -> Result<RdInfo, StoreError> {
        // A checked path segment: id comes from RD's addMagnet response (untrusted); a stray byte must not
        // turn into a different request.
        let mut url = reqwest::Url::parse(&format!("{}/torrents/info", self.api))
            .map_err(|_| dead_link("realdebrid bad torrent id"))?;
        url.path_segments_mut().map_err(|_| dead_link("realdebrid bad torrent id"))?.push(id);
        let req = build_request(
            reqwest::Method::GET,
            url.as_str(),
            &[("authorization", &format!("Bearer {}", self.token))],
            None,
        )
        .map_err(|_| dead_link("realdebrid bad torrent id"))?;
        let mut resp = self.client.do_request(req).await.map_err(store_error_from_do)?;
        let status = resp.status().as_u16();
        // 404 is RD saying it has no torrent under that id — the one answer that means a remembered id is
        // stale. Anything else, including an empty file list on a torrent whose metadata has not resolved
        // yet, says nothing about whether the account has it.
        if status == 404 {
            return Err(dead_link(ERR_TORRENT_GONE));
        }
        // A refusal is a fact about the ACCOUNT, not the release — the same rule the add path already
        // follows. Mapping every non-2xx to a dead link on the READ path meant an expired key or a throttle,
        // on a release RD demonstrably holds, reached the app as "this release does not exist": no backoff
        // recorded, and the account backoff shadowed because this path now runs before it.
        if store_refused_us(status) {
            return Err(unavailable(DebridService::RealDebrid, format!("info http {status}"), status));
        }
        if status != 200 {
            return Err(dead_link(format!("realdebrid info http {status}")));
        }
        let raw =
            read_limited(&mut resp, MAX_STORE_BYTES).await.map_err(|_| dead_link("realdebrid bad info"))?;
        decode_first::<RdInfo>(&raw, &RD_INFO_SHAPE).ok_or_else(|| dead_link("realdebrid bad info"))
    }

    /// Go `realDebridStore.unrestrict`.
    async fn unrestrict(&self, link: &str) -> Result<String, StoreError> {
        let mut resp = self.post("/unrestrict/link", &[("link", link)]).await?;
        let status = resp.status().as_u16();
        if !(200..300).contains(&status) {
            // See selectFiles: the last call of the chain classifies like the first — except for 403, which
            // on THIS call is RD refusing a particular file rather than the account. realDebridBlocked exists
            // because RD rejects specific files, and `refusalIsAboutTheAccount` keys on 401/403, so passing
            // it through here escalated one unplayable file into a sixty-second account-wide outage that
            // took unrelated releases down with it.
            if status != 403 && store_refused_us(status) {
                return Err(unavailable(
                    DebridService::RealDebrid,
                    format!("unrestrict http {status}"),
                    status,
                ));
            }
            return Err(dead_link(format!("realdebrid unrestrict http {status}")));
        }
        let raw = read_limited(&mut resp, MAX_STORE_BYTES)
            .await
            .map_err(|_| dead_link("realdebrid no download"))?;
        match decode_first::<RdUnrestricted>(&raw, &RD_UNRESTRICTED_SHAPE) {
            Some(body) if !body.download.is_empty() => Ok(body.download),
            _ => Err(dead_link("realdebrid no download")),
        }
    }

    /// Go `realDebridStore.unrestrictFor` is the last call of resolveExisting, shared by the full chain and
    /// the already-downloaded shortcut so both remember a refusal the same way.
    async fn unrestrict_for(&self, link: &str, t: &ResolveTarget) -> Result<String, StoreError> {
        let got = self.unrestrict(link).await;
        // Classifying a refusal only matters if it is REMEMBERED: `info`'s branch above records its own, and
        // without this the two calls after it were classified and then forgotten, so every poll walked the
        // whole chain again to reach the same throttled endpoint.
        if let Err(err) = &got {
            if as_unavailable(err).is_some() {
                record_refusal_for(
                    self.cache(),
                    DebridService::RealDebrid,
                    &self.token,
                    &t.info_hash,
                    err,
                    t.no_add,
                );
            }
        }
        got
    }

    /// Go `realDebridStore.resolveExisting` turns an RD torrent this account already has into a playable
    /// link. Shared by the just-added path and by the one that found it was already bought — they differ only
    /// in how the id came.
    async fn resolve_existing(&self, id: &str, t: &ResolveTarget) -> Result<String, StoreError> {
        let svc = DebridService::RealDebrid;
        let info = match self.info(id).await {
            Ok(info) => info,
            Err(err) => {
                // A refusal from the read path is remembered like any other, or nothing ever backs off and
                // the next poll asks again — but ONLY a refusal. `info` also hands back a raw transport
                // error, a bare `realdebrid info http 400`, and `realdebrid bad info` for a non-JSON 200 (a
                // Cloudflare interstitial). None of those is RD declining to serve this account, and filing
                // them as one made a TCP reset answer 503 store_unavailable naming realdebrid — which tells
                // the viewer their debrid is refusing and stops the client trying other sources — for a
                // release RD demonstrably holds. Because this path runs on every poll and each one re-stamps
                // the record, it never lapsed while the blip lasted. The same narrowing TorBox's read path
                // already makes.
                //
                // recordRefusalFor, not recordRefusal: this chain is now reachable by a NoAdd caller (the
                // probe route asks any store holding an id), and the per-release key is an add-path guard a
                // read-only resolve is exempt from READING — so it must not write one either, or a probe
                // poll gates the next /play behind a minute of backoff with no upstream call. TorBox already
                // honours that contract; RD's read path predates the probe ever reaching it.
                if as_unavailable(&err).is_some() {
                    record_refusal_for(self.cache(), svc, &self.token, &t.info_hash, &err, t.no_add);
                }
                // Only a definitive "no such torrent" forgets the id. An empty file list, a throttle or a
                // timeout all describe this attempt, not what the account holds — forgetting on those
                // re-bought the torrent on the very next poll, which is the loop this memory exists to end.
                if is_torrent_gone(&err) {
                    self.forget_torrent(t);
                }
                return Err(err);
            }
        };
        let files: Vec<TorrentFile> = info
            .files
            .iter()
            .map(|f| TorrentFile { index: f.id, name: f.path.clone(), size_bytes: Some(f.bytes) })
            .collect();
        let Some(file_id) = self.pick_file_id(&files, t)? else {
            return Err(dead_link("realdebrid no file"));
        };
        // RD rejects anti-piracy-matched filenames — fail fast so the pool falls through to another store.
        for f in &files {
            if f.index == file_id && real_debrid_blocked(&f.name) {
                return Err(dead_link("realdebrid blocked filename"));
            }
        }
        // For /play's link check, which can then tell a link to this file from a link to another one.
        self.remember_file_size(t, &files, file_id);

        // Already downloaded, with this very file selected: the link is in the answer we are holding, so go
        // straight to unrestrict. Every held play and every probe poll used to walk info → selectFiles → info →
        // unrestrict, re-POSTing a selection RD had already made and then re-reading the info it had just
        // sent — two of the four calls, one of them a write, spent learning nothing.
        //
        // All three conditions, not just the status. `downloaded` with the file unselected is a torrent that
        // was bought for a different file, and rdLinkFor's one-link fallback would hand back that file's link;
        // requiring Selected here keeps the fallback out of it. Anything short of all three takes the full
        // chain below, unchanged.
        if info.status == "downloaded" && rd_file_selected(&info, file_id) {
            if let Some(link) = rd_link_for(&info, file_id) {
                return self.unrestrict_for(&link, t).await;
            }
        }

        let sel =
            self.post(&format!("/torrents/selectFiles/{id}"), &[("files", &file_id.to_string())]).await?;
        let sel_status = sel.status().as_u16();
        drop(sel);
        if !(200..300).contains(&sel_status) {
            // A refusal is a fact about the ACCOUNT, not the release — the rule `info` applies one call
            // earlier on this same read path, and the one TorBox's final call applies too. Mapping every
            // non-2xx to a dead link here meant a throttle, on a release RD demonstrably holds and would
            // serve, reached the app as "this release does not exist": the client blacklists it and walks the
            // rest of the list, every candidate hitting the same throttled endpoint for the same answer.
            // Nothing was recorded either, since resolveExisting only remembers a *StoreUnavailableError, so
            // no backoff slowed the retries and the probe route had nothing to report.
            if store_refused_us(sel_status) {
                let refused = unavailable(svc, format!("selectFiles http {sel_status}"), sel_status);
                record_refusal_for(self.cache(), svc, &self.token, &t.info_hash, &refused, t.no_add);
                return Err(refused);
            }
            return Err(dead_link(format!("realdebrid selectFiles http {sel_status}")));
        }
        let ready = match self.info(id).await {
            Ok(ready) => ready,
            Err(err) => {
                // The fourth call of the chain, remembered like the other three. Left out, a throttle here
                // cost one extra walk of the whole chain before the first `info` caught it on the next poll.
                if as_unavailable(&err).is_some() {
                    record_refusal_for(self.cache(), svc, &self.token, &t.info_hash, &err, t.no_add);
                }
                return Err(err);
            }
        };
        if ready.links.is_empty() {
            return Err(dead_link("realdebrid not ready"));
        }
        let Some(link) = rd_link_for(&ready, file_id) else {
            return Err(dead_link("realdebrid has no link for the selected file"));
        };
        self.unrestrict_for(&link, t).await
    }
}

#[async_trait]
impl Store for RealDebridStore {
    fn service(&self) -> DebridService {
        DebridService::RealDebrid
    }

    /// RD has no usable cache API → all-false, and no error (this is authoritative "nothing known cached via
    /// RD", not a failure — RD contributes no cache truth by design).
    async fn cache_check(&self, hashes: &[String]) -> Result<HashMap<String, bool>, StoreError> {
        Ok(hashes.iter().map(|h| (h.clone(), false)).collect())
    }

    /// Go `realDebridStore.Status` reports a Real-Debrid download in progress, which for a long time it
    /// could not.
    ///
    /// The note that stood here said RD "exposes no queue state we can poll per infohash", and that was true
    /// of the infohash — but not of a torrent id, and this store remembers one from the moment addMagnet
    /// answers. The consequence of never reporting a wait was the plainest failure in the whole route: an
    /// uncached release on an RD-only install is ADDED, starts downloading, and has no links until it
    /// finishes, so resolveExisting returned a dead link — which is neither errAddInFlight nor a
    /// StoreUnavailableError, and nothing else could rescue it. Measured on a torrent RD reports as
    /// downloading at 37%, three polls:
    ///
    /// ```text
    /// play  404 {"error":"dead_link"}  |  probe 404 {"error":"not_queued"}   (x3)
    /// ```
    ///
    /// The add is already spent at that point, so the viewer pays for the fetch and is told the release is
    /// dead for its entire duration, on every poll. Both routes agreed, which is why no amount of
    /// probe-versus-play comparison found it.
    ///
    /// Still never invents a wait: it answers only for a torrent this account is recorded as holding, only
    /// when RD itself names a fetching status, and only while there are no links to serve. A dead, errored
    /// or virus-flagged torrent stays a failure, which is what the original note was protecting.
    async fn status(&self, t: &ResolveTarget) -> Option<StoreStatus> {
        let (id, held) = self.known_torrent(t);
        if !held {
            return None;
        }
        let info = self.info(&id).await.ok()?;
        // Links present means it is ready to serve, not waiting — that is the resolve's answer, not a status.
        if !info.links.is_empty() || !rd_fetching(&info.status) {
            return None;
        }
        Some(StoreStatus {
            progress: info.progress / 100.0,
            state: rd_fetch_state(&info.status).to_owned(),
            ..Default::default()
        })
    }

    async fn resolve(&self, t: &ResolveTarget) -> Result<String, StoreError> {
        let svc = DebridService::RealDebrid;
        let cache = self.cache();
        // A rejected key makes every request pointless, reads included — see accountBackedOff.
        if let Some(reason) = account_backed_off(cache, svc, &self.token) {
            return Err(unavailable(svc, format!("{reason} (backing off)"), 0));
        }
        // Already bought? Then resolve from it instead of buying it again — BEFORE any add-path guard, the
        // way TorBox orders it. Those guards describe the cost of adding, and this path adds nothing: with
        // the backoff first, one 401 on an unrelated release blocked every release the account already held,
        // read-only probes included, for the whole backoff window.
        let (id, held) = self.known_torrent(t);
        if held {
            return self.resolve_existing(&id, t).await;
        }
        if t.no_add {
            return Err(dead_link(ERR_WOULD_ADD)); // RD has no cache API, so resolving anything else here is an add
        }
        // The same backoff TorBox has. Without it, a client polling /play for the length of a download added
        // this magnet once per poll — hundreds of adds for one wait, each leaving a duplicate torrent on the
        // account. TorBox backing off correctly made this worse, not better: ResolvePreferring fell straight
        // through to whichever store had no memory of being refused.
        // The same in-flight guard TorBox has, and ahead of the backoff for the same reason: an add of ours
        // that was never answered must read as "coming", not as RD refusing. Giving the guard to TorBox alone
        // did not stop the cancel loop, it moved it: ResolvePreferring walks on to the store without a
        // marker, and fifty cancelled polls shut this account's allowance instead.
        add_in_flight(cache, svc, &self.token, &t.info_hash)?;
        if let Some(reason) = backed_off(cache, svc, &self.token, &t.info_hash) {
            return Err(unavailable(svc, format!("{reason} (backing off)"), 0));
        }
        // addWouldMissTheClock has no counterpart here: a store takes no context, so a spent clock drops the
        // future before it reaches the charge instead of arriving at it.
        spend_add(svc, &self.token, &t.info_hash, t.prefetch)?;
        note_add_attempt(cache, svc, &self.token, &t.info_hash);
        let mut add_resp =
            match self.post("/torrents/addMagnet", &[("magnet", &magnet_for(&t.info_hash))]).await {
                Ok(resp) => resp,
                Err(err) => {
                    // See TorBox's twin: an unanswered add is scout's uncertainty, not RD refusing. RD is the sharp
                    // case, having no Status for handlePlay to rescue the answer with.
                    if add_outcome_unknown(cache, svc, &self.token, &t.info_hash) {
                        // Not on a cancellation. /play runs on the client's context and a focus change is enough,
                        // so a viewer backing out wrote a give-up stamp that outlived them: past addGiveUp every
                        // resolve of that release answered a dead link for the rest of unknownOutcomeTTL, with no
                        // upstream call able to clear it — RD and Premiumize have no Status to rediscover the
                        // torrent with. A deadline belongs on a service that never answered, not on a client that
                        // hung up. This is the same guard recordRefusal applies one branch over, for the same
                        // reason.
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
        let add_status = add_resp.status().as_u16();
        // Read once, keep the bytes: the id and the explanation of a failure live in the same body, and
        // decoding straight off the reader left nothing for readStoreError further down.
        let raw = match read_limited(&mut add_resp, MAX_STORE_BYTES).await {
            Ok(raw) => raw,
            // A body that could not be READ is not an answer that nothing was created. RD had already sent
            // its status line — a 201 means the torrent exists on the account — and a mid-body reset, a TLS
            // truncation or the client's context expiring after the headers arrived all land here. So this
            // is precisely what the in-flight marker is for: an add that went out whose result we never saw.
            // It is settled BELOW, once there is an answer to settle on, so the next poll says "downloading"
            // — honest, and it neither adds again nor blames RD.
            //
            // It must NOT be recorded as a refusal. Doing that meant building a StoreUnavailableError here,
            // which laundered the cause: recordRefusal's isCancellation guard could no longer see it, so a
            // viewer changing focus mid-poll — /play runs on the client's context, and this read is inside
            // it — was filed as Real-Debrid refusing the account, and every /play and probe of that release
            // answered 503 store_unavailable for the next minute. RD has no Status for handlePlay to rescue
            // it with. That is the exact confusion the guard, errScoutSide and errAddInFlight all exist to
            // end.
            Err(read_err) => return Err(unknown_outcome(cache, svc, &self.token, &t.info_hash, &read_err)),
        };
        settle_add_attempt(cache, svc, &self.token, &t.info_hash);
        let added_id = serde_json::from_slice::<serde_json::Value>(&raw)
            .ok()
            .and_then(|mut v| {
                crate::util::fold_json_keys(&mut v, &RD_ADDED_SHAPE);
                serde_json::from_value::<RdAdded>(v).ok()
            })
            .map(|a| a.id)
            .unwrap_or_default();
        // A refusal is a fact about the account, not the release — the same distinction TorBox already draws.
        // Every non-2xx here used to become a dead link, so on a Real-Debrid install a 429 or a 503 reached
        // the app as "this release does not exist", and the player walked the whole candidate list collecting
        // the identical non-answer and condemning healthy releases on the way.
        //
        // Either way RD answered and created nothing, so the charge taken before the request goes back — the
        // rule Premiumize's answered-failure branch already followed and this one did not. Without it a
        // magnet RD rejects with a 400, or any 2xx scout cannot pull an id out of (a proxy page served as
        // 200), spent one add per poll: fifty real addMagnet calls inside two minutes at the client's
        // cadence, and then 503 scout_busy on EVERY Real-Debrid resolve for the rest of the rolling hour,
        // healthy releases included. On an RD-only install that is every /play, since RD has no Status for
        // the handler to short-circuit on.
        if !(200..300).contains(&add_status) || added_id.is_empty() {
            refund_unused_add(svc, &self.token);
            let detail = redact_token(&store_error_text(&raw), &self.token);
            if store_refused_us(add_status) {
                let refused = unavailable(svc, format!("addmagnet http {add_status}{detail}"), add_status);
                record_refusal(cache, svc, &self.token, &t.info_hash, &refused);
                return Err(refused);
            }
            // RD was the only store discarding the service's own words here, so the log said the same thing
            // for a bad magnet, a locked account and a Cloudflare page.
            let dead = dead_link(format!("realdebrid no torrent id (http {add_status}){detail}"));
            // Backed off as well, the way TorBox's add path does. The refund fixes the quota, but on its own
            // it left a poll loop free to re-ask forever — sixty polls, sixty real addMagnet calls — and
            // "free" is why that went unnoticed. The client's answer to THIS poll is still a dead link, so it
            // moves on to another release; only a client that keeps asking about this one meets the backoff.
            record_refusal(cache, svc, &self.token, &t.info_hash, &dead);
            return Err(dead);
        }
        // Remember the torrent RD just created, the way TorBox remembers its id. This is the fix the marker
        // kept failing to be: a "we already bought this" fact, separate from "an add is in flight", and not
        // cleared when the release resolves or is refused. Without it every poll called addMagnet again and
        // RD made a NEW torrent each time — ten polls of one unplayable pack, ten adds, ten duplicates.
        self.remember_torrent(t, &added_id);

        let link = self.resolve_existing(&added_id, t).await;
        // The same bound TorBox's just-added path has, and for a worse loop. `gone` about a torrent RD
        // created moments ago erases the memory just written, so the next poll finds nothing known and adds
        // again — and RD mints a NEW id every time, so unlike TorBox the re-add cannot converge on one
        // torrent and heal. `recordRefusal` deliberately ignores errTorrentGone, so nothing bounded it: two
        // minutes of one viewer sitting on one release spent the whole hourly allowance, left fifty duplicate
        // torrents on the account, and then answered 503 scout_busy for every RD resolve — healthy releases
        // included — for the rest of the hour. On an RD-only install every /play poll reaches here, because
        // RD has no Status for the handler to short-circuit on.
        if matches!(&link, Err(err) if is_torrent_gone(err)) {
            record_refusal(
                cache,
                svc,
                &self.token,
                &t.info_hash,
                &unavailable(svc, "created this torrent and then denied holding it", 0),
            );
            return Err(dead_link("realdebrid created this torrent and then denied holding it"));
        }
        link
    }

    fn recent_refusal(&self, info_hash: &str) -> Option<String> {
        backed_off(self.cache(), DebridService::RealDebrid, &self.token, info_hash)
    }

    fn add_in_flight(&self, info_hash: &str) -> Option<bool> {
        Some(add_still_believable(self.cache(), DebridService::RealDebrid, &self.token, info_hash))
    }

    fn holds_torrent(&self, t: &ResolveTarget) -> Option<bool> {
        Some(self.known_torrent(t).1)
    }

    fn scout_budget_spent(&self) -> Option<bool> {
        Some(scout_budget_spent(DebridService::RealDebrid, &self.token))
    }

    fn account_refused(&self) -> Option<String> {
        account_backed_off(self.cache(), DebridService::RealDebrid, &self.token)
    }

    fn budget_account(&self) -> Option<String> {
        Some(crate::addbudget::budget_account(DebridService::RealDebrid, &self.token))
    }

    /// Go `realDebridStore.KnownFileSize` (linkcheck.go:196).
    fn known_file_size(&self, t: &ResolveTarget) -> Option<i64> {
        let cache = self.cache.as_ref()?;
        let raw = cache.get(&rd_size_key(&self.token, &t.info_hash, t))?;
        raw.parse::<i64>().ok().filter(|n| *n > 0)
    }
}
