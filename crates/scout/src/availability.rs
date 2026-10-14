//! Port of `internal/scout/availability.go`. Work package 4.
//!
//! The availability route answers "is this title playable" for a page of IMDb ids, from the verdicts the stream
//! lists and a background checker have already written. A verdict is keyed by the account, not the install, so
//! the TV's full config and the browser's scoped config share one.

use crate::config::Config;

/// Go `maxAvailabilityIDs`: the most ids one availability request may ask about.
pub(crate) const MAX_AVAILABILITY_IDS: usize = 100;

/// Go `verdictUndetermined`: the marker a failed or partial check leaves, which holds off the next check.
pub(crate) const VERDICT_UNDETERMINED: &str = "undetermined";

/// Go `verdictPrefix`: the cache-key prefix every verdict for a config is written under. Scope, install id and
/// epoch are left out, so the scoped browser config reads the verdicts the full config wrote.
pub(crate) fn verdict_prefix(config: &Config) -> String {
    todo!("availability.go verdictPrefix")
}
