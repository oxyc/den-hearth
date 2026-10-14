//! What Scout's probe and Remux's probe both do to the head of a release: walk EBML elements and ISO-BMFF boxes,
//! and read the configuration records the video codecs keep in them.
//!
//! The two probes ask different questions of those bytes. Scout reads the head of a link to tell the viewer, and
//! rank the list, by what the file says (and tolerates a head cut short); Remux reads the whole index to cut
//! keyframes and decides what a browser plays. Both read bytes from a stranger, so the parts that are the same
//! live once, here, and are written for that: every offset is checked, nothing indexes past what it was handed,
//! nothing panics on input, and there is no `unsafe`.
//!
//! * [`ebml`] — element ids and sizes, and the children of a master element.
//! * [`mkv`] — the Matroska element ids both probes read.
//! * [`mp4`] — the boxes of a buffer, and the track boxes both probes read.
//! * [`codec`] — the `avcC`, `hvcC`, `av1C` and Dolby Vision configuration records.

#![forbid(unsafe_code)]

pub mod codec;
pub mod ebml;
pub mod mkv;
pub mod mp4;
