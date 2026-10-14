//! Port of `internal/scout/client.go`. Work package 5.

use serde::{Deserialize, Serialize};

use crate::attributes::StreamAttributes;
use crate::rank::RawStream;

/// A browser's report of what it plays, as Den Web sends it to den-remux and den-remux passes on in
/// X-Den-Playable (den-edge web/src/lib/playable.ts). With it a stream list is ranked for that browser rather
/// than for the Apple TV: releases it takes as they are first, then those den-remux has to convert, and last
/// those nothing here can make play. Without it the list is the TV's, as it always was.
///
/// Levels are the codecs' own numbers: H.264 `level_idc` (51 is 5.1), HEVC `general_level_idc` (level × 30,
/// so 150 is 5.0), AV1 `seq_level_idx` (12 is 5.0). 0 means none.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub(crate) struct ClientPlayable {
    pub h264: i64,
    pub h264_high10: i64,
    pub hevc_main: i64,
    pub hevc_main10: i64,
    pub hevc_high_tier: i64,
    pub hdr: bool,
    pub eac3: bool,
    pub aac_multichannel: bool,
    pub dolby_vision: ClientDolbyVision,
    pub av1: i64,
    pub av1_main10: i64,
    pub av1_hdr: bool,
    /// FLAC in fMP4, and VP9 profile 0 and 2: what den-remux copies rather than converts or skips for a browser that
    /// plays them. den-remux clears VP9 profile 2 for a session in Safari's own player, where 10-bit VP9 is unmeasured.
    pub flac: bool,
    pub aac71: bool,
    pub vp9: bool,
    pub vp9_profile2: bool,
}

/// The `dolbyVision` object of a `ClientPlayable`.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub(crate) struct ClientDolbyVision {
    pub p5: bool,
    pub p8: bool,
}

/// What a release costs in rank, for this browser, by what den-remux will have to do for it to play.
///
/// Converted audio costs about a source bonus: den-remux does it on the CPU as it goes, and the picture is the
/// release's own, so a 4K release whose TrueHD becomes AAC still ranks above a 1080p one. Video nobody named
/// costs the same, since den-remux's probe settles it when the file is opened.
///
/// Converted video costs more than the widest spread quality alone produces (4860, see preferenceSink), so every
/// release that plays as it is ranks above every one that has to be transcoded — one at a time on the box's GPU,
/// at most 1080p, tone-mapped — while staying under the cached bonus, so a cached conversion still beats a
/// download. A release that can't play here at all sinks below every one that can, and stays above junk.
pub(crate) const AUDIO_CONVERTED_COST: i64 = 300;
pub(crate) const VIDEO_UNNAMED_COST: i64 = 300;
pub(crate) const VIDEO_CONVERTED_COST: i64 = 6000;
pub(crate) const NEVER_PLAYS_COST: i64 = 60000;

impl ClientPlayable {
    /// Go `(*ClientPlayable).cost`: applies den-remux's own reading of scout's attributes (what it copies,
    /// what it converts, what it won't open).
    pub(crate) fn cost(&self, _s: RawStream, _a: StreamAttributes) -> i64 {
        todo!()
    }
}

/// Go `webReady` (defined in `handler.go`, line 2190; placed here per PORTING.md's assignment of
/// `webready_test.go`).
pub(crate) fn web_ready(_s: &RawStream, _url: &str) -> bool {
    todo!()
}
