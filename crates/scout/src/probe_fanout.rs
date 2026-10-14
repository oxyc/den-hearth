//! Port of `internal/scout/probe_fanout.go`. Work package 5.

use std::collections::HashSet;

use crate::client::ClientPlayable;
use crate::config::Config;
use crate::id::StreamId;
use crate::rank::RawStream;

/// Go `browserProbeFrontier`: how many candidates a browser's probe pass may resolve.
pub(crate) const BROWSER_PROBE_FRONTIER: usize = 4;

/// Go `probeCacheKey`: the cache key one probed file lives under. The file is the identity, not the request,
/// so the key carries the info hash, the file index and the episode the file was picked for.
pub(crate) fn probe_cache_key(_s: &RawStream, _sid: Option<&StreamId>) -> String {
    todo!()
}

/// Go `applyProbedSize`: a probe that read the file's own size replaces the indexer's number, keeping the
/// pack's total only as context when the two are plainly different.
pub(crate) fn apply_probed_size(_s: &mut RawStream, _sid: Option<&StreamId>) {
    todo!()
}

/// Go `hydratePackOnlySizes`: a never-probed season pack's total is demoted from the episode's size to the
/// pack's (`PackSizeBytes`), since the title named a pack rather than the episode.
pub(crate) fn hydrate_pack_only_sizes(_streams: &mut [RawStream], _sid: Option<&StreamId>) {
    todo!()
}

/// Go `browserProbeCandidates`: the indices of the streams a browser's probe pass should resolve, decided by
/// what could displace the current winner, bounded by `BROWSER_PROBE_FRONTIER`.
pub(crate) fn browser_probe_candidates(
    _streams: &[RawStream],
    _cfg: &Config,
    _client: Option<&ClientPlayable>,
) -> HashSet<usize> {
    todo!()
}
