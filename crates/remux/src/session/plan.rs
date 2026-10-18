//! Pure playback capability, output, and bandwidth policy.
//!
//! This module turns a probed release and viewer capabilities into immutable playback decisions.
//! Release discovery, source I/O, and HTTP response construction deliberately live elsewhere.

use super::*;

pub use den_net::Playable;

/// Remux's reading of a `Playable` against a probed file. A trait because `Playable` lives in den-net.
pub(crate) trait TakesMedia {
    fn takes(&self, info: &crate::probe::MediaInfo) -> bool;
}

impl TakesMedia for Playable {
    /// Whether the player takes this video as it is. A level the file doesn't name is taken: refusing it would
    /// refuse every release without a codec record.
    fn takes(&self, info: &crate::probe::MediaInfo) -> bool {
        let named = info.codecs.as_deref().and_then(crate::probe::profile_level);
        let fits = |max: u16| max > 0 && named.is_none_or(|(_, level, _)| level <= max);
        match info.video {
            // High 10 is its own decoder, reported at its own level; every other H.264 profile is 8-bit here.
            VideoCodec::H264 => match named {
                Some((110, _, _)) => fits(self.h264_high10),
                _ => fits(self.h264),
            },
            VideoCodec::Hevc => {
                let max = match named {
                    Some((_, _, true)) => self.hevc_high_tier,
                    Some((1, _, _)) | None => self.hevc_main.max(self.hevc_main10),
                    Some((2, _, _)) => self.hevc_main10,
                    Some(_) => 0,
                };
                fits(max) && (!info.hdr || self.hdr)
            }
            VideoCodec::Av1 => {
                // Main profile at Main tier is all a player is asked about; a Main decoder takes 8-bit and 10-bit.
                let max = match (named, info.codecs.as_deref().and_then(crate::probe::av1_bit_depth)) {
                    (Some((profile, _, high_tier)), _) if profile != 0 || high_tier => 0,
                    (_, Some(10)) => self.av1_main10,
                    (_, Some(8) | None) => self.av1.max(self.av1_main10),
                    _ => 0,
                };
                fits(max) && (!info.hdr || self.av1_hdr)
            }
            // Profile 0 is 8-bit; profile 2 is 10-bit or 12-bit, and a player is asked about 10. No level is asked.
            VideoCodec::Vp9 => match (named, info.codecs.as_deref().and_then(|c| c.split('.').nth(3))) {
                (Some((0, _, _)), _) => self.vp9,
                (Some((2, _, _)), Some("10")) => self.vp9_profile2,
                _ => false,
            },
            VideoCodec::Other(_) => false,
        }
    }
}

/// Whether a copy keeps the release's Dolby Vision: profile 5 for a player that shows profile 5, profile 8 with an
/// HDR10, SDR or HLG base layer (8.1, 8.2, 8.4) for one that shows profile 8. Profile 7's enhancement layer is
/// always dropped, and so is anything HLS has no name for.
pub(crate) fn keeps_dolby_vision(dv: crate::probe::DolbyVision, playable: Option<&Playable>) -> bool {
    let Some(p) = playable.filter(|_| dolby_vision_signal(dv).is_some()) else { return false };
    match dv.profile {
        5 => p.dolby_vision.p5,
        8 => p.dolby_vision.p8,
        _ => false,
    }
}

pub(super) fn kept_dolby_vision(
    info: &MediaInfo,
    transcoded: bool,
    playable: Option<&Playable>,
) -> Option<crate::probe::DolbyVision> {
    info.dolby_vision
        .filter(|dv| !transcoded && !info.dolby_vision_record_mismatch && keeps_dolby_vision(*dv, playable))
}

/// How kept Dolby Vision is named in the master playlist: CODECS to use in place of the base layer's (profile 5,
/// `dvh1.05.LL`), SUPPLEMENTAL-CODECS (profile 8, `dvh1.08.LL/` and the brand of what its base layer is — `db1p`
/// HDR10, `db2g` SDR, `db4h` HLG) and VIDEO-RANGE. `None` for a profile HLS does not name.
pub(crate) fn dolby_vision_signal(
    dv: crate::probe::DolbyVision,
) -> Option<(Option<String>, Option<String>, &'static str)> {
    let (brand, range) = match (dv.profile, dv.compat) {
        (5, _) => return Some((Some(format!("dvh1.05.{:02}", dv.level)), None, "PQ")),
        (8, 1) => ("db1p", "PQ"),
        (8, 2) => ("db2g", "SDR"),
        (8, 4) => ("db4h", "HLG"),
        _ => return None,
    };
    Some((None, Some(format!("dvh1.08.{:02}/{brand}", dv.level)), range))
}

/// The VIDEO-RANGE a copied variant names when it keeps no Dolby Vision. Without one a player takes the stream as
/// SDR, so an HDR copy has to say so: an AV1 by the transfer its codec string names, an HEVC by HLG where the
/// container says so and PQ otherwise, since an HDR release that names only its Rec. 2020 colours is HDR10, and a VP9
/// the same way — though it is HDR only by its transfer (`vp9_track`). `None` for SDR.
pub(super) fn copied_range(info: &MediaInfo, codecs: &str) -> Option<&'static str> {
    match info.video {
        VideoCodec::Av1 => crate::probe::av1_video_range(codecs),
        VideoCodec::Hevc | VideoCodec::Vp9 if info.hdr => Some(if info.hlg { "HLG" } else { "PQ" }),
        _ => None,
    }
}

/// Whether a release would play with no picture: Dolby Vision profile 5, which has no base layer, so stripped or
/// transcoded it comes out green and purple — unless the player shows profile 5 and takes the release as it is.
///
/// A profile 5 whose container has no Dolby Vision record (`dolby_vision_recordless`) is kept the same way: the
/// init segment is given the record it lacks (`job::add_dovi_record`).
pub(super) fn no_picture(playable: Option<&Playable>, takes_hevc: bool, info: &MediaInfo) -> bool {
    // A contradictory P5 record sits over a regular BT.2020 PQ/HLG base layer. The record itself must not make us
    // refuse that safe fallback when its RPU was unreadable.
    if info.dolby_vision_record_mismatch {
        return false;
    }
    info.dolby_vision.is_some_and(|dv| {
        if dv.has_fallback() {
            return false;
        }
        !(keeps_dolby_vision(dv, playable) && plays(playable, takes_hevc, info))
    })
}

/// The HLS codec a Dolby track is copied as, from the container's name for it — Matroska's `A_EAC3`/`A_AC3`
/// (and its `A_AC3/BSID…` variants), MP4's `ec-3`/`ac-3` — or `None` for anything else.
pub(crate) fn dolby_codec(container_codec: &str) -> Option<&'static str> {
    match container_codec {
        "A_EAC3" | "ec-3" => Some("ec-3"),
        c if c.starts_with("A_AC3") || c == "ac-3" => Some("ac-3"),
        _ => None,
    }
}

/// The HLS codec a FLAC track is copied as, from Matroska's `A_FLAC` or MP4's `fLaC`; `None` for anything else.
pub(crate) fn flac_codec(container_codec: &str) -> Option<&'static str> {
    matches!(container_codec, "A_FLAC" | "fLaC").then_some("fLaC")
}

/// How a track of `channels` plays, by its container's codec name: copied as the HLS codec the player takes it as —
/// E-AC-3 or AC-3 (`playable.eac3`), FLAC (`playable.flac`) — or converted to AAC-LC: 7.1 from eight channels or more
/// where the player plays 8-channel AAC, 5.1 from six or more where it plays multichannel AAC, stereo otherwise.
pub(crate) fn audio_plan(
    container_codec: &str,
    channels: u32,
    playable: Option<&Playable>,
) -> (Option<&'static str>, job::AudioOut) {
    let p = playable.copied().unwrap_or_default();
    let copy = dolby_codec(container_codec)
        .filter(|_| p.eac3)
        .or_else(|| flac_codec(container_codec).filter(|_| p.flac));
    let out = match copy {
        Some(_) => job::AudioOut::Copy,
        None if channels >= 8 && p.aac71 => job::AudioOut::Surround71,
        None if channels >= 6 && p.aac_multichannel => job::AudioOut::Surround,
        None => job::AudioOut::Stereo,
    };
    (copy, out)
}

/// Whether a transcode of this release has to tone-map it to SDR. The container's transfer says so where it is
/// written down — a UHD Blu-ray remux often leaves Matroska's Colour element out — and Dolby Vision says so too:
/// its base layer is HDR10 or HLG, except profile 8.2's, which is already SDR.
pub(crate) fn tonemaps(info: &crate::probe::MediaInfo) -> bool {
    info.hdr || info.dolby_vision.is_some_and(|dv| dv.compat != 2)
}

/// Whether a release plays only converted on the GPU: an MPEG-TS or AVI, whose keyframes are the probe's grid
/// (`MediaInfo::grid`), or HEVC the player can't take as it is.
pub(super) fn converts_only(
    playable: Option<&Playable>,
    takes_hevc: bool,
    info: &crate::probe::MediaInfo,
) -> bool {
    info.grid() || (info.video == VideoCodec::Hevc && !plays(playable, takes_hevc, info))
}

/// Whether the player takes the release's video as it is: by `playable`, else by `videoCodecs`, which can only say
/// that it takes no HEVC — and never that it takes AV1 or VP9, which only `playable` reports.
pub(super) fn plays(playable: Option<&Playable>, takes_hevc: bool, info: &crate::probe::MediaInfo) -> bool {
    match playable {
        Some(p) => p.takes(info),
        None => match info.video {
            VideoCodec::Hevc => takes_hevc,
            VideoCodec::Av1 | VideoCodec::Vp9 => false,
            _ => true,
        },
    }
}

/// How a release will play for this player, by what scout says of it before anything is opened.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum Fit {
    /// As it is — or nothing says otherwise, and the probe decides.
    Copy,
    /// Only converted on the GPU.
    Convert,
    /// Not at all, so it isn't opened.
    Never,
}

/// A release's `Fit` from scout's attributes, only where they say so. The probe still has the last word on
/// whatever is tried; this decides the order, and which not to try.
pub(crate) fn fit(a: &scout::Attributes, playable: Option<&Playable>, takes_hevc: bool) -> Fit {
    // Most Profile 5 has no base layer, but AE #532 established a real class whose container falsely says P5 over a
    // normal HDR base layer. Put these last rather than ruling them out: this probe reads the VUI and first RPU before
    // either refusing genuine P5 or safely stripping a contradictory record.
    if a.dv_profile == 5 && !playable.is_some_and(|p| p.dolby_vision.p5) {
        return Fit::Convert;
    }
    let uhd =
        a.resolution.as_deref().is_some_and(|r| matches!(r.to_ascii_lowercase().as_str(), "2160p" | "4k"));
    let ten_bit = a.bit_depth >= 10;
    match (a.codec.as_deref().map(str::to_ascii_lowercase).as_deref(), playable) {
        (Some("h264"), Some(p)) => {
            // A 10 the name inferred from "HDR" is no evidence of High 10: H.264 releases are not HDR.
            let max = if ten_bit && (a.probed || !a.hdr) { p.h264_high10 } else { p.h264 };
            // Nothing here makes H.264 smaller; 3840 × 2160 needs level 5.1.
            if max == 0 || (uhd && max < 51) {
                Fit::Never
            } else {
                Fit::Copy
            }
        }
        (Some("hevc"), Some(p)) => {
            let max = if ten_bit { p.hevc_main10 } else { p.hevc_main.max(p.hevc_main10) };
            // 3840 × 2160 needs level 5.0.
            if max == 0 || (uhd && max < 150) || (a.hdr && !p.hdr) {
                Fit::Convert
            } else {
                Fit::Copy
            }
        }
        (Some("av1"), p) => {
            let max = p.map_or(0, |p| if ten_bit { p.av1_main10 } else { p.av1.max(p.av1_main10) });
            // 3840 × 2160 needs level 5.0, `seq_level_idx` 12. Nothing converts AV1: what won't play isn't opened.
            if max == 0 || (uhd && max < 12) || (a.hdr && !p.is_some_and(|p| p.av1_hdr)) {
                Fit::Never
            } else {
                Fit::Copy
            }
        }
        (Some("vp9"), p) => {
            // Nothing converts VP9 either. A 10 is profile 2's; a depth nobody read is left to the probe.
            let takes = p.is_some_and(|p| match a.bit_depth {
                0 => p.takes_vp9(),
                d if d >= 10 => p.vp9_profile2,
                _ => p.vp9,
            });
            if takes {
                Fit::Copy
            } else {
                Fit::Never
            }
        }
        (Some("hevc"), None) if !takes_hevc => Fit::Convert,
        // MPEG-4 Part 2, which only an AVI or MPEG-TS gets this far with (`scout::candidates`): always transcoded.
        (Some("mpeg4"), _) => Fit::Convert,
        _ => Fit::Copy,
    }
}

/// A release's average bitrate, from its size and duration; `None` when either isn't known.
pub(super) fn average_bitrate(size: Option<u64>, duration: f64) -> Option<u64> {
    size.filter(|_| duration > 0.0).map(|s| (s as f64 * 8.0 / duration) as u64)
}

/// The longest a player is asked to wait before a copy plays through: a link that needs more of a wait than this
/// for a release is too slow for it. Ten seconds is under the thirty that hls.js and Safari hold ahead, so a player
/// can build that lead at all, and about what a person waits for a start before giving up on it.
pub(super) const MAX_PREBUFFER: f64 = 10.0;
/// The hard cap on any head start: a copy that asks at most this ranks next after those within `MAX_PREBUFFER`, and
/// the player shows a countdown while it waits. Longer than this and a viewer is left in front of a spinner, which is
/// never acceptable; such a copy ranks below a transcode.
pub(super) const LONG_PREBUFFER: f64 = 30.0;
/// What a release with no byte index is taken to need over its average. An encode at a constant quality peaks at
/// three or four times its average for seconds at a time, but over the stretches a buffer has to carry, half again is
/// what the indexed releases measured.
const NO_INDEX_HEADROOM: f64 = 1.5;

/// What a copy needs of the link, in bits a second: from where the player starts, the rate at which it waits at most
/// `MAX_PREBUFFER` and then never runs dry (`playlist::needed_rate`) — or, from a file with no byte index, its average
/// with `NO_INDEX_HEADROOM`, which `indexed` says it is.
#[derive(Clone, Copy, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct Need {
    pub bitrate: u64,
    pub indexed: bool,
}

impl std::fmt::Display for Need {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.indexed {
            true => write!(f, "{} kbit/s to start within {MAX_PREBUFFER:.0}s", self.bitrate / 1000),
            false => write!(
                f,
                "{} kbit/s, its average with {:.0}% headroom: the file has no byte index",
                self.bitrate / 1000,
                (NO_INDEX_HEADROOM - 1.0) * 100.0
            ),
        }
    }
}

/// How far ahead a player buffers, by the one it named (`player` in `POST /remux/session`): hls.js as den-edge sets it
/// up, two minutes or 150 MB; the cast page's hls.js, a minute or 50 MB, a receiver having little memory; Safari's
/// own player, and any that didn't say, about thirty seconds.
pub(crate) fn buffer_of(player: Option<&str>) -> playlist::Buffer {
    match player {
        Some("hls.js") => playlist::Buffer { secs: 120.0, bytes: Some(150_000_000) },
        Some("cast") => playlist::Buffer { secs: 60.0, bytes: Some(50_000_000) },
        _ => playlist::Buffer { secs: 30.0, bytes: None },
    }
}

/// What a session would deliver of a release, and to what: where the player starts, how far ahead it buffers, and the
/// bytes it is sent — the video and the one audio track it plays (`delivered_bytes`), else the whole file.
#[derive(Clone, Copy, Debug, PartialEq)]
pub(crate) struct Delivery {
    pub start_at: f64,
    pub buffer: playlist::Buffer,
    pub bytes: Option<u64>,
}

impl Delivery {
    /// A session of `want` on a release of `size` bytes: `start_at` as the session will take it, a start at or past
    /// the end being a start from zero.
    pub(crate) fn of(info: &MediaInfo, size: Option<u64>, want: &Want<'_>) -> Delivery {
        Delivery {
            start_at: effective_start(want.start_at, info.duration),
            buffer: buffer_of(want.player),
            bytes: delivered_bytes(info, want).or(size),
        }
    }
}

/// Where a session starts: `start_at`, or 0 for one at or past the end, which starts the title over.
pub(crate) fn effective_start(start_at: f64, duration: f64) -> f64 {
    Some(start_at).filter(|t| *t > 0.0 && *t < duration).unwrap_or(0.0)
}

/// The bytes a session sends of a release: its video, and the audio track `want` gets — as the track is where it is
/// copied, at the AAC encoder's rate where it is converted. `None` where the file doesn't say how big its tracks are.
/// The other audio tracks and the subtitles a Matroska file interleaves with them are never sent.
pub(super) fn delivered_bytes(info: &MediaInfo, want: &Want<'_>) -> Option<u64> {
    let video = info.video_bytes?;
    let n = match want.audio_track {
        Some(n) if n < info.audio.len() => n,
        _ => crate::lang::pick_audio(&info.audio, want.audio).0,
    };
    let track = info.audio.get(n)?;
    let (_, out) = audio_plan(&track.codec, track.channels, want.playable);
    let audio = match out.aac_bitrate() {
        Some(rate) => (rate as f64 * info.duration / 8.0) as u64,
        None => track.bytes?,
    };
    Some(video + audio)
}

/// `Need` for a release as `delivery` would send it; `None` when neither its index nor its size and duration say.
pub(crate) fn need(info: &MediaInfo, delivery: &Delivery) -> Option<Need> {
    let segs = playlist::segments(&info.keyframes, info.duration, playlist::TARGET_SECS);
    match playlist::segment_bytes(&segs, &info.byte_index, delivery.bytes) {
        Some(bytes) => {
            let from = start_segment(&segs, delivery.start_at);
            let rate = playlist::needed_rate(&segs, &bytes, from, MAX_PREBUFFER, delivery.buffer);
            Some(Need { bitrate: rate as u64, indexed: true })
        }
        None => average_bitrate(delivery.bytes, info.duration)
            .map(|a| Need { bitrate: (a as f64 * NO_INDEX_HEADROOM) as u64, indexed: false }),
    }
}

/// How long a player on a `max_bitrate` link waits before a copy as `delivery` sends it plays through
/// (`playlist::startup_delay`): what it can take as its pre-buffer target. `None` without a link or a byte index, and
/// where no wait would do.
pub(crate) fn prebuffer(info: &MediaInfo, delivery: &Delivery, max_bitrate: Option<u64>) -> Option<f64> {
    let rate = max_bitrate.filter(|r| *r > 0)? as f64;
    let segs = playlist::segments(&info.keyframes, info.duration, playlist::TARGET_SECS);
    let bytes = playlist::segment_bytes(&segs, &info.byte_index, delivery.bytes)?;
    let from = start_segment(&segs, delivery.start_at);
    Some(playlist::startup_delay(&segs, &bytes, from, rate, delivery.buffer)).filter(|w| w.is_finite())
}

/// The share of the player's `maxBitrate` a copy may need and still count as fitting. A link is a measurement, taken
/// once, from one moment; it is not a promise. What a player then gets in a session runs below it (a 30 Mbit/s figure,
/// the box's uplink, against hls.js's own 14-15 Mbit/s estimate), varies by a tenth or more from one minute to the
/// next, and is shared with whatever else the connection carries. `need` already weighs the heavy stretches, so this
/// is for the link, not the file: 70% leaves 30% for both. A 12 GB, 3818 s episode (25.3 Mbit/s) does not fit 30
/// Mbit/s; a 1.5 GB one (3.1) does.
pub(super) const LINK_HEADROOM: f64 = 0.7;

/// Whether a copy of the release needs more than `LINK_HEADROOM` of the player's `maxBitrate`. Without one every
/// release fits, and so does one whose need isn't known: nothing says it doesn't.
pub(crate) fn over(max_bitrate: Option<u64>, need: Option<Need>) -> bool {
    max_bitrate.zip(need).is_some_and(|(max, need)| need.bitrate as f64 > max as f64 * LINK_HEADROOM)
}

/// How many times the player's link a source may need and still be converted from. A conversion shrinks what the
/// player downloads, not what the box reads: the whole source comes in at its own rate, in real time, from a debrid
/// that delivers a fraction of a gigabit (about 13 Mbit/s measured on an uncached TorBox). A 4x margin keeps the
/// heaviest UHD Blu-ray remuxes (about 120 Mbit/s) convertible for a 30 Mbit/s link, and turns away a 220 Mbit/s WEB-DL.
const TRANSCODE_SOURCE_MARGIN: u64 = 4;

/// Whether a source needing `bitrate` is worth converting for a player with `max_bitrate` of link. Any source is where
/// no link is named.
pub(super) fn source_within_reach(bitrate: u64, max_bitrate: Option<u64>) -> bool {
    max_bitrate.is_none_or(|link| bitrate <= link.saturating_mul(TRANSCODE_SOURCE_MARGIN))
}

/// What this session's picking may treat as the player's link: its own measurement (`own`), narrowed by what
/// is left of the box's configured upload (`uplink_bps`, `UPLINK_MBPS`) once `other_bps` — every other
/// currently open remote session's own `need` — is taken out. A release that still doesn't fit in what's left
/// ranks as `over` just as one over the player's own measurement does; the uplink is never consulted for a
/// player at home (`own` is `None`), and leaves `own` alone where it is unset, exactly today's behaviour.
pub(crate) fn budgeted_max_bitrate(own: Option<u64>, uplink_bps: Option<u64>, other_bps: u64) -> Option<u64> {
    let own = own?;
    Some(match uplink_bps {
        Some(uplink) => own.min(uplink.saturating_sub(other_bps)),
        None => own,
    })
}

/// What every other currently open remote session already needs of the shared uplink: the sum of `need`'s
/// bitrate for every session with a link of its own (`max_bitrate.is_some()`) other than `exclude_sid` — the
/// one now picking, not yet in the registry, but excluded by id all the same in case a replacement overlaps it
/// briefly. A session still picking, or transcoding (whose `need` is `None`), counts for nothing yet.
pub(crate) fn other_remote_bps(
    sessions: &std::collections::HashMap<String, Arc<Session>>,
    exclude_sid: &str,
) -> u64 {
    sessions
        .values()
        .filter(|s| s.sid != exclude_sid && s.max_bitrate.is_some())
        .filter_map(|s| s.need.map(|n| n.bitrate))
        .sum()
}

/// A transcode's output size: the source's, fitted inside the preset's with its aspect kept (a 2.4:1 4K film becomes
/// 1920 × 800 at 1080p — 1080 lines of it would be wider than level 4.1 allows), never scaled up, both even. 0 × 0
/// when the source's is unknown.
pub(crate) fn transcode_size(w: u32, h: u32, preset: job::Preset) -> (u32, u32) {
    if w == 0 || h == 0 {
        return (0, 0);
    }
    let scale = (preset.width as f64 / w as f64).min(preset.height as f64 / h as f64).min(1.0);
    let even = |x: u32| ((x as f64 * scale).round() as u32 & !1).max(2);
    (even(w), even(h))
}

/// Final output contract after applying device capability, Dolby Vision, and bandwidth policy.
pub(crate) enum OutputPlan {
    Direct {
        codecs: String,
        resolution: (u32, u32),
        peak: u64,
        average: u64,
        dovi: job::Dovi,
        signal: Option<playlist::DolbyVision>,
    },
    Transcode {
        resolution: (u32, u32),
        peak: u64,
        average: u64,
        dovi: job::Dovi,
    },
}

#[derive(Clone, Copy)]
pub(crate) enum PlaybackMode {
    Direct,
    Transcode(job::Preset),
}

impl OutputPlan {
    pub(crate) fn for_release(
        info: &MediaInfo,
        size: Option<u64>,
        playable: Option<&Playable>,
        mode: PlaybackMode,
    ) -> Self {
        if let PlaybackMode::Transcode(preset) = mode {
            return Self::Transcode {
                resolution: transcode_size(info.width, info.height, preset),
                peak: preset.peak + 192_000,
                average: preset.bitrate,
                dovi: if info.dolby_vision.is_some() { job::Dovi::Strip } else { job::Dovi::Absent },
            };
        }
        let codecs = info.codecs.clone().unwrap_or_else(|| {
            match info.video {
                VideoCodec::Hevc => "hvc1.1.6.L150.90",
                VideoCodec::Av1 => "av01.0.08M.08",
                _ => "avc1.640028",
            }
            .to_string()
        });
        let kept = kept_dolby_vision(info, false, playable);
        let dovi = match (info.dolby_vision, kept) {
            (None, _) => job::Dovi::Absent,
            (Some(_), None) => job::Dovi::Strip,
            (Some(_), Some(_)) => job::Dovi::Keep,
        };
        let (codecs, signal) = match kept.and_then(dolby_vision_signal) {
            Some((own, supplemental, range)) => {
                (own.unwrap_or(codecs), Some(playlist::DolbyVision { supplemental, range }))
            }
            None => {
                let range = copied_range(info, &codecs);
                (codecs, range.map(|range| playlist::DolbyVision { supplemental: None, range }))
            }
        };
        let (peak, average) = playlist::bandwidth(size, info.duration);
        Self::Direct { codecs, resolution: (info.width, info.height), peak, average, dovi, signal }
    }

    pub(crate) fn parts(&self) -> (&str, (u32, u32), u64, u64, job::Dovi, Option<&playlist::DolbyVision>) {
        match self {
            Self::Direct { codecs, resolution, peak, average, dovi, signal } => {
                (codecs, *resolution, *peak, *average, *dovi, signal.as_ref())
            }
            Self::Transcode { resolution, peak, average, dovi } => {
                (job::TRANSCODE_CODECS, *resolution, *peak, *average, *dovi, None)
            }
        }
    }
}

pub(crate) struct PlaybackPlan {
    pub output: OutputPlan,
    pub audio_copy: Option<&'static str>,
    pub audio_out: job::AudioOut,
    pub prebuffer: Option<f64>,
    pub need: Option<Need>,
    pub demand: Option<Vec<(f64, u64)>>,
}

impl PlaybackPlan {
    pub(crate) fn for_release(
        info: &MediaInfo,
        size: Option<u64>,
        want: &Want<'_>,
        usable_bitrate: Option<u64>,
        segments: &[playlist::Segment],
        mode: PlaybackMode,
        audio: usize,
    ) -> Self {
        let output = OutputPlan::for_release(info, size, want.playable, mode);
        let (audio_copy, audio_out) =
            audio_plan(&info.audio[audio].codec, info.audio[audio].channels, want.playable);
        if let PlaybackMode::Transcode(preset) = mode {
            return Self {
                output,
                audio_copy,
                audio_out,
                prebuffer: usable_bitrate.map(|rate| preset.head_start(rate)),
                need: None,
                demand: None,
            };
        }
        let delivery = Delivery::of(info, size, want);
        let bytes = playlist::segment_bytes(segments, &info.byte_index, delivery.bytes);
        Self {
            output,
            audio_copy,
            audio_out,
            prebuffer: prebuffer(info, &delivery, usable_bitrate),
            need: need(info, &delivery),
            demand: bytes.map(|bytes| {
                segments
                    .iter()
                    .zip(bytes)
                    .map(|(segment, bytes)| (segment.start, bytes.round() as u64))
                    .collect()
            }),
        }
    }
}
