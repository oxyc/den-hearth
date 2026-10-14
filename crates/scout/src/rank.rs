//! Port of `internal/scout/rank.go`: release-title parsing, filtering and ranking. `RawStream` is FROZEN
//! (see PORTING.md): scrapers produce it and the handler and probes consume it.

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
