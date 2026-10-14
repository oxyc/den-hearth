//! Port of `internal/scout/probe_fanout.go`.
//!
//! The pure half: what a probe's facts change about a stream, which releases are worth probing, and the cache key a
//! probed file lives under. Go also hangs `hydrateKnownSizes`, `probeTop` and `probeBehind` on the handler (they read
//! its singleflight, link memo and store factory); those belong to handler.rs, and take these as their parts.

use std::collections::HashSet;
use std::sync::atomic::Ordering;
use std::time::Duration;

use crate::attributes::stream_attributes;
use crate::cache::Cache;
use crate::client::ClientPlayable;
use crate::config::{Config, DebridService};
use crate::id::StreamId;
use crate::metrics::metrics;
use crate::probe::Probe;
use crate::rank::{detect_resolution_lower, junk_class_of, quality_score_lower, RawStream, PREFERENCE_SINK};
use crate::stores::{CacheTruth, ResolveTarget};

/// Go `probeTopN`: how many of the ranked releases get probed. The list is already in the order the client will show,
/// so the ones a viewer actually considers are at the top — probing all twenty would spend a debrid resolve
/// apiece to describe releases nobody scrolls to.
pub(crate) const PROBE_TOP_N: usize = 6;
/// Go `browserProbeFrontier`: how many candidates a browser's probe pass may resolve.
pub(crate) const BROWSER_PROBE_FRONTIER: usize = 3;

/// Go `probeBudget`: how long the whole fan-out may take. It runs BEHIND the response now, so this bounds the
/// background work rather than the client's wait.
pub(crate) const PROBE_BUDGET: Duration = Duration::from_secs(12);

/// Go `probeTTL`: probe results never change for a given file, so they are cached hard. The tier is disk-backed,
/// which is what makes a redeploy cheap.
pub(crate) const PROBE_TTL: Duration = Duration::from_secs(720 * 60 * 60);

/// Go `packSizeRatio` is how much bigger the indexer's number has to be than the probe's before it is worth
/// naming as a pack. A movie's title size and its probed size routinely differ by a rounding byte or two;
/// a season pack's differs by the other ninety percent of the torrent.
pub(crate) const PACK_SIZE_RATIO: f64 = 1.3;

/// Go `(*handler).hydrateProbeFacts`: attaches the track details already known for these releases. Only the CACHE is
/// consulted on the request path — a map lookup, no network.
pub(crate) fn hydrate_probe_facts(
    cache: Option<&dyn Cache>,
    streams: &mut [RawStream],
    sid: Option<&StreamId>,
) {
    let Some(cache) = cache else { return };
    for s in streams.iter_mut() {
        if s.info_hash.is_empty() {
            continue;
        }
        if let Some(raw) = cache.get(&probe_cache_key(s, sid)) {
            if let Ok(probe) = serde_json::from_str::<Probe>(&raw) {
                s.probe = Some(probe);
                apply_probed_size(s, sid);
                metrics().probe_cache_hit.fetch_add(1, Ordering::Relaxed);
            }
        }
    }
}

/// Go `applyProbedSize` promotes the probe's own reading of the resolved file over whatever the indexer's title
/// or behaviorHints claimed. For an episode picked out of a season pack, that claim named the whole
/// torrent — Fauda's "💾 68 GB" was the season, not the 1.1 GB episode den-remux actually streamed — so
/// every size-based decision downstream (the label, ranking's size bonus, a MaxSizeGB budget filter) would
/// otherwise judge an episode by its pack's weight. The indexer's number survives as PackSizeBytes, purely
/// as label context, and only for an episode request whose pack is actually bigger than the file by enough
/// to matter; a movie is never given one, since there is no pack for its title size to have been
/// confused with.
pub(crate) fn apply_probed_size(s: &mut RawStream, sid: Option<&StreamId>) {
    let Some(probed) = s.probe.as_ref().map(|p| p.size_bytes).filter(|&n| n > 0) else { return };
    if sid.is_some_and(|sid| sid.has_ep)
        && s.size_bytes.is_some_and(|n| n as f64 >= probed as f64 * PACK_SIZE_RATIO)
    {
        s.pack_size_bytes = s.size_bytes;
    }
    s.size_bytes = Some(probed);
}

/// Go `applyKnownSize` is applyProbedSize's twin for a size a store's own file list already gave, rather than
/// one read off a ranged GET: the same gap-check, so the two paths agree on when a number is worth naming
/// as a pack rather than a rounding difference.
pub(crate) fn apply_known_size(s: &mut RawStream, known: i64) {
    if known <= 0 {
        return;
    }
    if s.size_bytes.is_some_and(|n| n as f64 >= known as f64 * PACK_SIZE_RATIO) {
        s.pack_size_bytes = s.size_bytes;
    }
    s.size_bytes = Some(known);
}

/// Go `hydratePackOnlySizes` runs last of the three size passes, for whatever hydrateProbeFacts and
/// hydrateKnownSizes left untouched: a release whose declared size has NOT been shown to be the episode's
/// own by either a probe or a store's file list. Most such releases are a genuine single-episode file and
/// the indexer's number for them is correct, as it always was — this only acts on the ones whose own
/// title says otherwise (unresolvedSeasonPack, season.go): a season named with no episode at all, or a
/// multi-episode range that merely spans this one. For those, the number is demonstrably the PACK's, not
/// the episode's, and showing it as a plain "68 GB" states something false. SizeBytes becomes unknown
/// (None) — which rank.go already treats as "don't know", earning no size bonus and never tripping a
/// MaxSizeGB budget — rather than wrong, and PackSizeBytes keeps the number as what it actually is.
pub(crate) fn hydrate_pack_only_sizes(streams: &mut [RawStream], sid: Option<&StreamId>) {
    let Some(sid) = sid.filter(|sid| sid.has_ep) else { return };
    for s in streams.iter_mut() {
        if s.size_bytes.is_none() || s.pack_size_bytes.is_some() {
            continue; // nothing to demote, or already named a pack by an earlier pass
        }
        if crate::season::unresolved_season_pack(&s.title, sid.season, sid.episode) {
            s.pack_size_bytes = s.size_bytes.take();
        }
    }
}

/// Go `probeJob` is a copy, deliberately: the background work must not reach into the response's slice, which
/// the caller is free to marshal and hand to the client the moment probeTop returns.
#[derive(Debug, Clone)]
pub(crate) struct ProbeJob {
    pub(crate) key: String,
    /// The services confirmed to hold this release. The probe resolves against these ONLY — anything else
    /// would fetch rather than read, which is an add against the quota.
    pub(crate) holders: Vec<DebridService>,
    pub(crate) target: ResolveTarget,
}

/// The part of Go `probeTopForClient` that decides what gets probed: the frontier, then which of its releases are
/// worth a resolve. `client` is the browser the list is ranked for, `None` for the TV.
pub(crate) fn select_probe_jobs(
    streams: &[RawStream],
    config: &Config,
    sid: Option<&StreamId>,
    truth: &CacheTruth,
    client: Option<&ClientPlayable>,
) -> Vec<ProbeJob> {
    // For a browser, probes exist to settle the compatibility decision at the head of the ranked list,
    // not to annotate everything below it. Only an unresolved release whose optimistic (zero conversion
    // cost) score can displace the current winner belongs to that decision. Three contenders bound the
    // cold fan-out at half the former six calls; cached facts for every candidate were already hydrated
    // before ranking. The TV keeps the six-release validation window because it provides no client
    // capability report from which to define a narrower frontier.
    let frontier: HashSet<usize> = match client {
        Some(_) => browser_probe_candidates(streams, config, client),
        None => (0..streams.len().min(PROBE_TOP_N)).collect(),
    };
    let mut pending = Vec::new();
    for (i, s) in streams.iter().enumerate() {
        if !frontier.contains(&i) {
            if s.cached && s.probe.is_none() {
                metrics().torbox_avoid("probe_frontier");
            }
            continue;
        }
        if s.probe.is_some() {
            continue;
        }
        // Only probe what the store ALREADY holds. Probing resolves, and resolving a release the account
        // does not hold ADDS it — so this read path was queueing up to six torrents per newly-viewed
        // title, against a sixty-an-hour ceiling. Browsing did it; opening a ten-episode season did it
        // sixty times. That is where the quota went, and the refusals it produced were then read as dead
        // releases and blamed on the releases.
        //
        // Nothing is lost: probe facts are keyed by infohash and kept for a month, so a release is probed
        // for free the first time it is listed after it becomes cached. It also self-heals during a
        // cache-check outage — everything reads as not-held, so nothing is probed, which is precisely
        // when touching the debrid is least wise.
        if !s.cached || s.info_hash.is_empty() {
            continue;
        }
        // WHICH services hold it, not just that one does. `s.Cached` is the union across accounts, so on a
        // two-account install a release only the second holds still reads as cached here — and resolving
        // it through the pool would reach the first account, which adds it. Carrying the holders lets the
        // probe resolve read-only.
        let key = probe_cache_key(s, sid);
        metrics().probe_cache_miss.fetch_add(1, Ordering::Relaxed);
        let holders = truth.held_by(&s.info_hash);
        if holders.is_empty() {
            continue;
        }
        pending.push(ProbeJob {
            key,
            holders,
            target: ResolveTarget {
                info_hash: s.info_hash.clone(),
                file_idx: s.file_idx,
                season: season_of(sid),
                episode: episode_of(sid),
                // Enforced by the store, not promised by this caller. Picking only held releases was not
                // enough: TorBox's cache check says what TORBOX has, not what this ACCOUNT has, so a
                // "held" release with no warm resolve entry still went to createtorrent.
                no_add: true,
                ..Default::default()
            },
        });
    }
    pending
}

/// Go `browserProbeCandidates`: the indices of the streams a browser's probe pass should resolve, decided by
/// what could displace the current winner, bounded by `BROWSER_PROBE_FRONTIER`.
pub(crate) fn browser_probe_candidates(
    streams: &[RawStream],
    cfg: &Config,
    client: Option<&ClientPlayable>,
) -> HashSet<usize> {
    let mut frontier = HashSet::with_capacity(BROWSER_PROBE_FRONTIER);
    if streams.is_empty() {
        return frontier;
    }
    let score = |stream: &RawStream, include_client: bool| -> i64 {
        let lower = str::to_lowercase(&stream.title);
        let mut out = quality_score_lower(&lower, stream, junk_class_of(&lower));
        let prefer = &cfg.filters.prefer_resolution;
        if !prefer.is_empty() {
            let resolution = detect_resolution_lower(&lower);
            if !resolution.is_empty() && resolution != prefer {
                out -= PREFERENCE_SINK;
            }
        }
        if include_client {
            if let Some(client) = client {
                out -= client.cost(stream.clone(), stream_attributes(stream.clone()));
            }
        }
        out
    };
    let winner = score(&streams[0], true);
    // (index, optimistic score)
    let mut candidates: Vec<(usize, i64)> = Vec::new();
    for (i, stream) in streams.iter().enumerate() {
        if stream.probe.is_some() {
            continue;
        }
        if i == 0 {
            frontier.insert(0);
            continue;
        }
        let optimistic = score(stream, false);
        if optimistic > winner {
            candidates.push((i, optimistic));
        }
    }
    candidates.sort_by_key(|c| std::cmp::Reverse(c.1)); // stable, as sort.SliceStable
    let remaining = BROWSER_PROBE_FRONTIER.saturating_sub(frontier.len());
    frontier.extend(candidates.into_iter().take(remaining).map(|(i, _)| i));
    frontier
}

/// Go `probeCacheKey` identifies the FILE, not the request: the same release serves every user of this instance
/// and every episode request that maps onto it. The key carries the info hash, the file index and the episode the file
/// was picked for.
pub(crate) fn probe_cache_key(s: &RawStream, sid: Option<&StreamId>) -> String {
    let mut key = format!("probe:v1:{}", s.info_hash);
    if let Some(idx) = s.file_idx {
        key.push_str(&format!(":{idx}"));
    }
    if let Some(sid) = sid.filter(|sid| sid.has_ep) {
        key.push_str(&format!(":{}:{}", sid.season, sid.episode));
    }
    key
}

/// Go `seasonOf`.
pub(crate) fn season_of(sid: Option<&StreamId>) -> Option<i64> {
    sid.filter(|sid| sid.has_ep).map(|sid| sid.season)
}

/// Go `episodeOf`.
pub(crate) fn episode_of(sid: Option<&StreamId>) -> Option<i64> {
    sid.filter(|sid| sid.has_ep).map(|sid| sid.episode)
}
