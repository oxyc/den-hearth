//! Port of `internal/scout/rank.go`: release-title parsing, filtering and ranking. `RawStream` is FROZEN
//! (see PORTING.md): scrapers produce it and the handler and probes consume it.

use std::collections::HashSet;

use crate::client::ClientPlayable;
use crate::probe::Probe;

/// FROZEN. Go `RawStream`: one release from one indexer.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct RawStream {
    pub info_hash: String,
    pub file_idx: Option<i64>,
    pub title: String,
    /// The size of what this stream actually plays. For an episode picked out of a season pack it starts
    /// as the indexer's number (the whole pack) and is corrected to the episode's own size once a probe
    /// reads it. Every size-based decision reads this field, never `pack_size_bytes`.
    pub size_bytes: Option<i64>,
    /// The season pack's own total, kept only once a probe has shown it differs. Display-only.
    pub pack_size_bytes: Option<i64>,
    pub seeders: Option<i64>,
    pub cached: bool,
    /// Whether anyone could answer the cachedness question this request. False means `cached` is a
    /// default rather than an observation.
    pub cache_known: bool,
    pub source: String,
    /// What the file itself said, when probed. `None` means "not asked", distinct from "asked and it
    /// told us nothing".
    pub probe: Option<Probe>,
}

/// Go `gib` (rank.go).
pub(crate) const GIB: i64 = 1_073_741_824;

/// Go `rankFilters`. Only the fields the ported tests set; the rest of the Go struct comes with the
/// implementation.
#[derive(Debug, Clone, Default)]
pub(crate) struct RankFilters {
    pub exclude_cam: bool,
    pub cached_only: bool,
    pub hdr_only: bool,
    pub result_cap: i64,
    pub resolutions: Vec<String>,
    pub min_seeders: Option<i64>,
    pub max_size_gb: Option<i64>,
    pub exclude_regex: String,
    pub prefer_resolution: String,
    pub show_tokens: HashSet<String>,
    pub episode: Option<[i64; 2]>,
    pub client: Option<ClientPlayable>,
}

/// Go `junkClass`: "" when the title carries no junk marker.
pub(crate) fn junk_class(_title: &str) -> &'static str {
    todo!()
}

/// Go `qualityScore`.
pub(crate) fn quality_score(_s: RawStream) -> i64 {
    todo!()
}

/// Go `detectResolution`: "" when the title names none.
pub(crate) fn detect_resolution(_title: &str) -> &'static str {
    todo!()
}

/// Go `capSeeds`: keeps the best `limit` releases, not the first ones.
pub(crate) fn cap_seeds(_streams: &[RawStream], _limit: i64) -> Vec<RawStream> {
    todo!()
}

/// Go `realDebridBlocked`.
pub(crate) fn real_debrid_blocked(_title: &str) -> bool {
    todo!()
}

/// Go `titleTokens`: a set, so duplicates collapse.
pub(crate) fn title_tokens(_s: &str) -> HashSet<String> {
    todo!()
}

/// Go `otherShow`.
pub(crate) fn other_show(_title: &str, _expected: &HashSet<String>) -> bool {
    todo!()
}

/// Go `otherEpisode`.
pub(crate) fn other_episode(_title: &str, _season: i64, _episode: i64) -> bool {
    todo!()
}

/// Go `titleOverlap`.
pub(crate) fn title_overlap(_release_title: &str, _expected: &HashSet<String>) -> bool {
    todo!()
}

/// Go `rankStreams`.
pub(crate) fn rank_streams(_streams: &[RawStream], _f: RankFilters) -> Vec<RawStream> {
    todo!()
}
