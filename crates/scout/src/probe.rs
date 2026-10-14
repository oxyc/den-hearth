//! Port of `internal/scout/probe.go`: reading the head of a resolved link to learn what the file itself
//! says (container, codecs, audio layout, HDR, Dolby Vision, languages), Matroska first; MP4 and AVI are
//! in `probe_mp4.rs` and `probe_avi.rs`. `Probe` is FROZEN (see PORTING.md): `RawStream` carries it and
//! the cache stores it as JSON.

use serde::{Deserialize, Serialize};

fn is_zero_i64(v: &i64) -> bool {
    *v == 0
}
fn is_zero_i32(v: &i32) -> bool {
    *v == 0
}
fn is_zero_f64(v: &f64) -> bool {
    *v == 0.0
}
fn is_false(v: &bool) -> bool {
    !*v
}

/// FROZEN. Go `Probe`. The serde attributes reproduce the Go JSON tags (`omitempty` is
/// `skip_serializing_if`); `audioLanguages` and `subtitleLanguages` carry no `omitempty` in Go, and
/// absent decodes as empty.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct Probe {
    #[serde(rename = "container", default, skip_serializing_if = "String::is_empty")]
    pub container: String,
    #[serde(rename = "videoCodec", default, skip_serializing_if = "String::is_empty")]
    pub video_codec: String,
    /// "2.0", "5.1", "7.1"
    #[serde(rename = "audioChannels", default, skip_serializing_if = "String::is_empty")]
    pub audio_channels: String,
    /// "HDR10", "HLG"
    #[serde(rename = "hdrFormat", default, skip_serializing_if = "String::is_empty")]
    pub hdr_format: String,
    #[serde(rename = "dolbyVision", default, skip_serializing_if = "is_false")]
    pub dolby_vision: bool,
    /// 0 means the file didn't say or this couldn't read it.
    #[serde(rename = "bitDepth", default, skip_serializing_if = "is_zero_i32")]
    pub bit_depth: i32,
    #[serde(rename = "dvProfile", default, skip_serializing_if = "is_zero_i32")]
    pub dv_profile: i32,
    #[serde(rename = "videoProfile", default, skip_serializing_if = "is_zero_i32")]
    pub video_profile: i32,
    #[serde(rename = "videoLevel", default, skip_serializing_if = "is_zero_i32")]
    pub video_level: i32,
    #[serde(rename = "highTier", default, skip_serializing_if = "is_false")]
    pub high_tier: bool,
    #[serde(rename = "width", default, skip_serializing_if = "is_zero_i32")]
    pub width: i32,
    #[serde(rename = "height", default, skip_serializing_if = "is_zero_i32")]
    pub height: i32,
    #[serde(rename = "frameRate", default, skip_serializing_if = "is_zero_f64")]
    pub frame_rate: f64,
    #[serde(rename = "audioLanguages", default)]
    pub audio: Vec<String>,
    #[serde(rename = "subtitleLanguages", default)]
    pub subtitles: Vec<String>,
    /// How many tracks carried no language at all.
    #[serde(rename = "untaggedAudioTracks", default)]
    pub untagged_audio: i32,
    #[serde(rename = "untaggedSubtitleTracks", default)]
    pub untagged_subtitles: i32,
    /// The resolved link's own total size, from Content-Range (or Content-Length). 0 when unusable.
    #[serde(rename = "sizeBytes", default, skip_serializing_if = "is_zero_i64")]
    pub size_bytes: i64,
}

/// Go `probeBytes`: how much of a file's head a probe reads.
pub(crate) const PROBE_BYTES: usize = 1 << 20;

/// Go `ErrNoTracks`: a head with no Tracks element. A reason constant, per PORTING.md's convention for
/// sentinel errors.
pub(crate) const ERR_NO_TRACKS: &str = "no track information in the file head";

/// Go `idTrackType`: the Matroska TrackType element id.
pub(crate) const ID_TRACK_TYPE: u8 = 0x83;
/// Go `trackTypeAudio`.
pub(crate) const TRACK_TYPE_AUDIO: u8 = 2;

/// Go `ProbeTracks`: reads the head of a resolved link and parses it. The size comes from the response's
/// total, not from the bytes read.
pub(crate) async fn probe_tracks(_client: &reqwest::Client, _url: &str) -> Result<Probe, String> {
    todo!()
}

/// Go `ParseHead`: detects the container from the head and dispatches to the matching parser.
pub(crate) fn parse_head(_head: &[u8]) -> Result<Probe, &'static str> {
    todo!()
}

/// Go `ParseMatroskaTracks`.
pub(crate) fn parse_matroska_tracks(_head: &[u8]) -> Result<Probe, &'static str> {
    todo!()
}

/// Go `parseTrackEntry`: the track's type and its normalized language ("" when none).
pub(crate) fn parse_track_entry(_entry: &[u8]) -> (u8, String) {
    todo!()
}

/// Go `cleanLang`: normalizes a language tag to a two-letter code, "" when it names none.
pub(crate) fn clean_lang(_b: &[u8]) -> String {
    todo!()
}

/// Go `channelLayout`: "7.1", "5.1", "2.0", ... for a channel count, "" when there is no layout for it.
pub(crate) fn channel_layout(_n: i64) -> &'static str {
    todo!()
}

/// Go `channelCount`: the inverse of `channel_layout`; 0 for a layout it does not know.
pub(crate) fn channel_count(_layout: &str) -> i64 {
    todo!()
}
