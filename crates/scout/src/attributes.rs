//! Port of `internal/scout/attributes.go`. Work package 5.

use serde::{Deserialize, Serialize};

use crate::probe::Probe;
use crate::rank::RawStream;

/// Go `StreamAttributes`: structured, pre-parsed display attributes, emitted on each stream so the client
/// renders badges without re-parsing titles. Nullable fields are `Option` (JSON null, no omitempty).
/// Declared field by field from the Go struct; the serde `omitempty` details come with the implementation.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct StreamAttributes {
    pub resolution: Option<String>,
    pub source: Option<String>,
    pub codec: Option<String>,
    pub hdr: bool,
    pub dolby_vision: bool,
    /// "HDR10+", "HDR10", "HLG", "HDR", or None.
    pub hdr_format: Option<String>,
    /// The display string: "Atmos", "TrueHD", ...
    pub audio: Option<String>,
    /// The normalized audio family: "eac3", "truehd", "dtshdma", ...
    pub audio_codec: Option<String>,
    /// "7.1", "5.1", "2.0"
    pub audio_channels: Option<String>,
    pub atmos: bool,
    /// Burned-in (hardcoded) subtitles.
    pub hardcoded_subs: bool,
    pub three_d: bool,
    /// The size of the file this stream actually plays; see `RawStream::size_bytes`.
    pub size_bytes: Option<i64>,
    /// The season pack's own total, display-only; see `RawStream::pack_size_bytes`.
    pub pack_size_bytes: Option<i64>,
    pub seeders: Option<i64>,
    /// Whether the debrid holds this release. None is "nobody could ask", not false.
    pub cached: Option<bool>,
    /// Read from the file when the release was probed; empty when it was not.
    pub audio_languages: Vec<String>,
    pub subtitle_languages: Vec<String>,
    pub untagged_audio: i64,
    pub probed: bool,
    pub label: String,
    /// 0 means nobody has read it.
    pub bit_depth: i64,
    /// 0 means nobody has read it.
    pub dv_profile: i64,
    /// The profile the release name points to while the real one is unread; 0 when the name points nowhere.
    pub dv_profile_guess: i64,
    /// The codec's level: H.264 level_idc, HEVC level × 30, AV1 seq_level_idx.
    pub video_level: i64,
    pub high_tier: bool,
    pub width: i64,
    pub height: i64,
    pub frame_rate: f64,
}

/// Go `streamAttributes`: the attributes of one stream, read from its title and, when probed, the file.
pub(crate) fn stream_attributes(_s: RawStream) -> StreamAttributes {
    todo!()
}

/// Go `withProbe`: lets the file override the title's guesses wherever the probe read something; an
/// unprobed release (`None`) comes back unchanged.
pub(crate) fn with_probe(_attrs: StreamAttributes, _p: Option<&Probe>) -> StreamAttributes {
    todo!()
}

/// Go `detectAudioCodec`: the normalized audio family of a release title, "" when it names none.
pub(crate) fn detect_audio_codec(_t: &str) -> &'static str {
    todo!()
}

/// Go `containerOf`: the file's container, from the probe when it read the file, else the release name's
/// extension, else "".
pub(crate) fn container_of(_s: RawStream) -> &'static str {
    todo!()
}
