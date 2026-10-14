//! Port of `internal/scout/probe_avi.go`. Work package 5.

use crate::probe::Probe;

/// Go `maxAVIDepth`: how many LIST chunks the walk nests before it stops.
pub(crate) const MAX_AVI_DEPTH: usize = 16;

/// Go `parseAVI`: the probe read from an AVI head, and whether the walk found a stream header at all.
pub(crate) fn parse_avi(_head: &[u8]) -> (Probe, bool) {
    todo!()
}

/// Go `codecFromFourCC`: the codec name for a sample-entry or stream FourCC, "" when unrecognised. Shared
/// with the MP4 parser, which reads the same FourCCs from its sample entries.
pub(crate) fn codec_from_four_cc(_cc: &str) -> &'static str {
    todo!()
}
