//! Port of `internal/scout/client.go`. Work package 5.

use serde::{Deserialize, Serialize};

use crate::attributes::{container_of, StreamAttributes};
use crate::rank::RawStream;
use crate::util::go_to_lower;

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
    pub(crate) fn cost(&self, s: RawStream, a: StreamAttributes) -> i64 {
        if a.three_d || !remux_opens(&s) {
            return NEVER_PLAYS_COST;
        }
        let mut cost = 0;
        // Most Profile 5 has no base layer, but a malformed class has a P5 container record over a regular HDR base.
        // Keep proven P5 near the end so den-remux can inspect the HEVC VUI/RPU and either refuse genuine P5 or strip
        // the false record. Treating it as impossible here prevented that definitive probe from ever running.
        if a.dv_profile == 5 && !self.dolby_vision.p5 {
            cost += VIDEO_CONVERTED_COST * 2;
        }
        // A profile 5 the name only suggests weighs like a transcode: most likely it won't play here, but the probe
        // has the last word, so it stays in the list.
        if a.dv_profile == 0 && a.dv_profile_guess == 5 && !self.dolby_vision.p5 {
            cost += VIDEO_CONVERTED_COST;
        }
        let uhd = a.resolution.as_deref().unwrap_or("") == "2160p";
        let ten_bit = a.bit_depth >= 10;
        // beyond answers whether the video is past what the decoder takes: by the level the probe read, else by
        // the level a 3840 × 2160 picture needs at least.
        let beyond = |most: i64, uhd_level: i64| {
            if a.video_level > 0 {
                a.video_level > most
            } else {
                uhd && most < uhd_level
            }
        };
        match a.codec.as_deref().unwrap_or("") {
            "h264" => {
                // A 10 the name inferred from "HDR" is no evidence of High 10: H.264 releases are not HDR.
                let mut most = self.h264;
                if ten_bit && (a.probed || !a.hdr) {
                    most = self.h264_high10;
                }
                // Nothing on the box makes H.264 smaller.
                if most == 0 || beyond(most, 51) {
                    return NEVER_PLAYS_COST;
                }
            }
            "hevc" => {
                let mut most = self.hevc_main10;
                if !ten_bit {
                    most = most.max(self.hevc_main);
                }
                if a.high_tier {
                    most = self.hevc_high_tier;
                }
                if most == 0 || beyond(most, 150) || (a.hdr && !self.hdr) {
                    cost += VIDEO_CONVERTED_COST;
                }
            }
            "av1" => {
                let mut most = self.av1_main10;
                if !ten_bit {
                    most = most.max(self.av1);
                }
                // Nothing converts AV1, and a browser is only asked about its Main tier.
                if most == 0 || a.high_tier || beyond(most, 12) || (a.hdr && !self.av1_hdr) {
                    return NEVER_PLAYS_COST;
                }
            }
            "vp9" => {
                // Nothing converts VP9 either. Profile 2 is its 10-bit profile.
                let takes = if ten_bit { self.vp9_profile2 } else { self.vp9 };
                if !takes {
                    return NEVER_PLAYS_COST;
                }
            }
            "" => cost += VIDEO_UNNAMED_COST,
            // MPEG-4 Part 2, VC-1, MPEG-2: den-remux converts none of them.
            _ => return NEVER_PLAYS_COST,
        }
        match a.audio_codec.as_deref().unwrap_or("") {
            "aac" => {}
            "eac3" | "ac3" => {
                if !self.eac3 {
                    cost += AUDIO_CONVERTED_COST;
                }
            }
            "flac" => {
                if !self.flac {
                    cost += AUDIO_CONVERTED_COST;
                }
            }
            _ => cost += AUDIO_CONVERTED_COST,
        }
        cost
    }
}

/// Go `remuxRefusedExt`: den-remux opens Matroska and MP4 only, and refuses these by name.
const REMUX_REFUSED_EXT: [&str; 9] =
    [".avi", ".ts", ".m2ts", ".iso", ".wmv", ".mpg", ".mpeg", ".vob", ".webm"];

/// Go `remuxOpens`.
fn remux_opens(s: &RawStream) -> bool {
    if matches!(container_of(s.clone()).as_str(), "avi" | "webm") {
        return false;
    }
    let name = go_to_lower(&s.title);
    !REMUX_REFUSED_EXT.iter().any(|ext| name.ends_with(ext))
}

/// Go `webReady` (defined in `handler.go`, line 2190; placed here per PORTING.md's assignment of
/// `webready_test.go`).
pub(crate) fn web_ready(s: &RawStream, url: &str) -> bool {
    url.starts_with("https://") && container_of(s.clone()) == "mp4"
}
