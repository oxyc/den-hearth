//! Port of `internal/scout/probe_mp4.go`. Work package 5.

use crate::probe::Probe;

/// Go `mp4Box`: one box read from a head. `body` is what the box carries after its header.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct Mp4Box {
    pub typ: String,
    pub body: Vec<u8>,
}

/// Go `parseMP4`: the probe read from an MP4 head, and whether the head described a parseable file.
pub(crate) fn parse_mp4(_head: &[u8]) -> (Probe, bool) {
    todo!()
}

/// Go `boxes`: the top-level boxes in a buffer. A wrong size stops the walk; it never indexes past the end.
pub(crate) fn boxes(_buf: &[u8]) -> Vec<Mp4Box> {
    todo!()
}
