//! A browser's report of what it plays (`playable`), as Den Web measures it once (den-edge `web/src/lib/playable.ts`)
//! and sends to Remux, Reel and Scout. One definition, so every service reads the same JSON the same way. Unknown
//! fields are ignored and missing ones default, so a report with more or fewer fields than this still parses.

use serde::{Deserialize, Serialize};

/// What the player decodes, as its own tests found: the highest level it takes of 8-bit H.264 and of H.264 High 10
/// (`level_idc`), 8-bit and 10-bit HEVC (`general_level_idc`, level × 30) and HEVC's High tier, and of 8-bit and
/// 10-bit AV1 at Main profile (`seq_level_idx`), 0 for none, and whether it decodes PQ HDR in HEVC and in AV1, which
/// of VP9's profiles it decodes, and which audio it plays as it is.
#[derive(Deserialize, Serialize, Clone, Copy, Debug, Default, PartialEq)]
#[serde(rename_all = "camelCase", default)]
pub struct Playable {
    pub h264: u16,
    /// 10-bit H.264 (High 10, profile 110) — almost only anime Hi10P. Safari decodes none; Chrome decodes it in
    /// software. Not transcoded when it's 0: the box's GPU has no High 10 decoder.
    pub h264_high10: u16,
    pub hevc_main: u16,
    pub hevc_main10: u16,
    /// A UHD Blu-ray remux is often High tier. Apple's decoders refuse it, whatever their tests say, so the web app
    /// reports 0 there; a player that doesn't send it gets it converted.
    pub hevc_high_tier: u16,
    pub hdr: bool,
    /// Plays E-AC-3 and AC-3 in fMP4 HLS — Safari and Apple's receivers: such a track is copied, not converted.
    pub eac3: bool,
    /// Plays 6-channel AAC-LC: a converted track of six channels or more stays 5.1 rather than coming down to stereo.
    pub aac_multichannel: bool,
    /// Which Dolby Vision the player shows as Dolby Vision rather than as its base layer. Absent is neither.
    pub dolby_vision: DolbyVisionPlay,
    /// 8-bit AV1's highest `seq_level_idx` at Main profile and tier (8 is level 4.0, 13 is 5.1). Nothing on the box
    /// converts AV1, so where this and `av1_main10` are 0 an AV1 release isn't tried at all.
    pub av1: u16,
    /// 10-bit AV1's.
    pub av1_main10: u16,
    /// Decodes 10-bit AV1 with PQ.
    pub av1_hdr: bool,
    /// Plays FLAC in fMP4 HLS: such a track is copied, not converted.
    pub flac: bool,
    /// Plays 8-channel AAC-LC: a converted track of eight channels or more stays 7.1 rather than folding down to 5.1.
    pub aac71: bool,
    /// Decodes VP9 profile 0 (8-bit) in fMP4 HLS. Nothing on the box converts VP9, so where this and `vp9_profile2` are
    /// false a VP9 release isn't tried at all.
    pub vp9: bool,
    /// Decodes VP9 profile 2 (10-bit) — never, whatever it says, for a session Safari's native player plays.
    pub vp9_profile2: bool,
}

/// `playable.dolbyVision`: profile 5 (no base layer at all; Safari shows it, Chrome can't) and profile 8.x shown as
/// Dolby Vision.
#[derive(Deserialize, Serialize, Clone, Copy, Debug, Default, PartialEq)]
#[serde(default)]
pub struct DolbyVisionPlay {
    pub p5: bool,
    pub p8: bool,
}

impl Playable {
    pub fn takes_hevc(&self) -> bool {
        self.hevc_main > 0 || self.hevc_main10 > 0
    }

    pub fn takes_av1(&self) -> bool {
        self.av1 > 0 || self.av1_main10 > 0
    }

    pub fn takes_vp9(&self) -> bool {
        self.vp9 || self.vp9_profile2
    }
}

impl std::fmt::Display for Playable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let hdr = match (self.hdr, self.eac3) {
            (true, true) => ", HDR, E-AC-3",
            (true, false) => ", HDR",
            (false, true) => ", E-AC-3",
            (false, false) => "",
        };
        let (h264, high10) = (self.h264, self.h264_high10);
        let (main, main10, high) = (self.hevc_main, self.hevc_main10, self.hevc_high_tier);
        write!(f, "H.264 L{h264}, High 10 L{high10}, HEVC L{main}, 10-bit L{main10}, High tier L{high}")?;
        let dv = match (self.dolby_vision.p5, self.dolby_vision.p8) {
            (true, true) => ", Dolby Vision 5 and 8",
            (true, false) => ", Dolby Vision 5",
            (false, true) => ", Dolby Vision 8",
            (false, false) => "",
        };
        let av1_hdr = if self.av1_hdr { ", AV1 HDR" } else { "" };
        let aac = if self.aac_multichannel { ", AAC 5.1" } else { "" };
        let more: String = [
            (self.aac71, ", AAC 7.1"),
            (self.flac, ", FLAC"),
            (self.vp9, ", VP9"),
            (self.vp9_profile2, ", VP9 profile 2"),
        ]
        .into_iter()
        .filter_map(|(on, name)| on.then_some(name))
        .collect();
        write!(f, "{hdr}, AV1 L{}, AV1 10-bit L{}{av1_hdr}{dv}{aac}{more}", self.av1, self.av1_main10)
    }
}
