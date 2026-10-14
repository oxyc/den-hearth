//! Port of `internal/scout/attributes.go`. Work package 5.

use std::sync::LazyLock;

use regex::Regex;
use serde::{Deserialize, Serialize};

use crate::label::clean_label_lower;
use crate::probe::Probe;
use crate::util::go_to_lower;
use crate::rank::{
    detect_resolution_lower, junk_class_of, re2, RawStream, RE_3D, RE_AC3, RE_ATMOS, RE_AV1, RE_BLURAY,
    RE_BR_RIP, RE_DTS, RE_DTSHD_MA, RE_DTSX, RE_DVD_RIP, RE_EAC3, RE_HDR10P, RE_HDTV, RE_KORSUB_HC, RE_REMUX,
    RE_TRUEHD, RE_WEB, RE_WEB_DL, RE_WEB_RIP,
};

fn is_zero_i64(v: &i64) -> bool {
    *v == 0
}
fn is_zero_f64(v: &f64) -> bool {
    *v == 0.0
}
fn is_false(v: &bool) -> bool {
    !*v
}

/// Go `StreamAttributes`: structured, pre-parsed display attributes, emitted on each stream so the client
/// renders badges without re-parsing titles. Nullable fields are `Option` (JSON null, no omitempty).
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct StreamAttributes {
    pub resolution: Option<String>,
    pub source: Option<String>,
    pub codec: Option<String>,
    pub hdr: bool,
    pub dolby_vision: bool,
    /// The HDR10-family variant ("HDR10+", "HDR10", "HLG", "HDR"), or None. Independent of `dolby_vision`.
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
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pack_size_bytes: Option<i64>,
    pub seeders: Option<i64>,
    /// Whether the debrid holds this release. None is "nobody could ask", not false.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cached: Option<bool>,
    /// Read from the file when the release was probed; empty when it was not.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub audio_languages: Vec<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub subtitle_languages: Vec<String>,
    #[serde(rename = "untaggedAudioTracks", skip_serializing_if = "is_zero_i64")]
    pub untagged_audio: i64,
    #[serde(skip_serializing_if = "is_false")]
    pub probed: bool,
    pub label: String,
    /// 0 means nobody has read it, and is omitted.
    #[serde(skip_serializing_if = "is_zero_i64")]
    pub bit_depth: i64,
    /// 0 means nobody has read it, and is omitted.
    #[serde(skip_serializing_if = "is_zero_i64")]
    pub dv_profile: i64,
    /// The profile the release name points to while the real one is unread; 0 when the name points nowhere.
    #[serde(skip_serializing_if = "is_zero_i64")]
    pub dv_profile_guess: i64,
    /// The codec's level: H.264 level_idc, HEVC level x 30, AV1 seq_level_idx.
    #[serde(skip_serializing_if = "is_zero_i64")]
    pub video_level: i64,
    #[serde(skip_serializing_if = "is_false")]
    pub high_tier: bool,
    #[serde(skip_serializing_if = "is_zero_i64")]
    pub width: i64,
    #[serde(skip_serializing_if = "is_zero_i64")]
    pub height: i64,
    #[serde(skip_serializing_if = "is_zero_f64")]
    pub frame_rate: f64,
}

// The patterns below are attributes.go's own; the ones shared with rank.go are imported from `rank`.

static RE_DOVI_ATTR: LazyLock<Regex> = LazyLock::new(|| re2(r"dolby vision|dolbyvision|dovi|\bdv\b"));
static RE_HDR_EXTRA: LazyLock<Regex> = LazyLock::new(|| re2(r"hdr10|\bhdr\b|\bhlg\b"));
static RE_HLG: LazyLock<Regex> = LazyLock::new(|| re2(r"\bhlg\b"));
/// Matches "hdr10" and the "hdr10" in "hdr10+"; check RE_HDR10P first.
static RE_HDR10_ANY: LazyLock<Regex> = LazyLock::new(|| re2(r"hdr10"));
static RE_HDR_PLAIN: LazyLock<Regex> = LazyLock::new(|| re2(r"\bhdr\b"));
static RE_CH71: LazyLock<Regex> = LazyLock::new(|| re2(r"7\.1|7 1|\b8ch\b"));
static RE_CH51: LazyLock<Regex> = LazyLock::new(|| re2(r"5\.1|5 1|\b6ch\b"));
static RE_CH20: LazyLock<Regex> = LazyLock::new(|| re2(r"2\.0|2 0|stereo|\b2ch\b"));
static RE_HEVC: LazyLock<Regex> = LazyLock::new(|| re2(r"x265|h\.?265|hevc"));
static RE_AVC: LazyLock<Regex> = LazyLock::new(|| re2(r"x264|h\.?264|\bavc\b"));
static RE_DTSHD_ATTR: LazyLock<Regex> = LazyLock::new(|| re2(r"dts-hd|dts hd|dtshd"));
static RE_FLAC: LazyLock<Regex> = LazyLock::new(|| re2(r"flac|lpcm|pcm"));
/// A digit or a boundary after "aac" ("AAC2.0", "AAC5 1", "HE-AAC"), so "Aachen" is not audio.
static RE_AAC: LazyLock<Regex> = LazyLock::new(|| re2(r"\baac(?:[0-9]|\b)"));
/// Opus counts only once the technical half of the name has begun: "Mr.Hollands.Opus.1995.1080p" is a film
/// title, and a bare \bopus\b would call it Opus audio. RE2's `.` is a whole codepoint but its `\d` and `\b` are
/// ASCII, so this stays in Unicode mode (for the dots) and spells the ASCII classes out with `(?-u:…)`.
static RE_OPUS: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"(?:(?-u:\d){3,4}p|x26[45]|h\.?26[45]|hevc|av1|10.?bits?|web-?dl|webrip|bluray)(?-u:\b).*(?-u:\bopus\b)")
        .expect("static pattern")
});
static RE_MP3: LazyLock<Regex> = LazyLock::new(|| re2(r"\bmp3\b"));
static RE_HI10: LazyLock<Regex> = LazyLock::new(|| re2(r"\bhi10p?\b"));
static RE_TEN_BIT: LazyLock<Regex> = LazyLock::new(|| re2(r"\b10[ .\-_]?bits?\b|\bhi10p?\b|\bmain ?10\b"));
static RE_HYBRID: LazyLock<Regex> = LazyLock::new(|| re2(r"\bhybrid\b"));

/// Go `strPtr`: "" is nil.
fn str_opt(s: &str) -> Option<String> {
    if s.is_empty() {
        None
    } else {
        Some(s.to_string())
    }
}

/// Go `detectSourceAttr`.
pub(crate) fn detect_source_attr(t: &str) -> &'static str {
    let junk = junk_class_of(t);
    if matches!(junk, "cam" | "telesync" | "screener") {
        return junk;
    }
    if RE_REMUX.is_match(t) {
        "remux"
    } else if RE_BLURAY.is_match(t) || RE_BR_RIP.is_match(t) {
        "bluray"
    } else if RE_WEB_DL.is_match(t) {
        "webdl"
    } else if RE_WEB_RIP.is_match(t) {
        "webrip"
    } else if RE_WEB.is_match(t) {
        "web"
    } else if RE_HDTV.is_match(t) {
        "hdtv"
    } else if RE_DVD_RIP.is_match(t) {
        "dvdrip"
    } else {
        ""
    }
}

/// Go `detectCodec`: the codec off the release title. Its spellings must match the probe's, because
/// `with_probe` overwrites this field rather than merging with it. h264 is the spelling that wins because
/// three container parsers already produce it.
fn detect_codec(t: &str) -> &'static str {
    if RE_AV1.is_match(t) {
        "av1"
    } else if RE_HEVC.is_match(t) {
        "hevc"
    } else if RE_AVC.is_match(t) || RE_HI10.is_match(t) {
        // Hi10P is H.264's High 10 profile by name; anime releases give it instead of "x264".
        "h264"
    } else {
        ""
    }
}

/// Go `detectBitDepth`: 10 when the title says so or the release is HDR or Dolby Vision (both 10-bit by
/// definition). Otherwise 0: a title that says nothing is not evidence of 8-bit.
fn detect_bit_depth(t: &str, hdr: bool) -> i64 {
    if hdr || RE_TEN_BIT.is_match(t) {
        10
    } else {
        0
    }
}

/// Go `detectHDRFormat`: the HDR10-family label, or "". HDR10+ wins over a bare "hdr10", HLG over generic
/// "hdr". Dolby Vision is reported separately.
fn detect_hdr_format(t: &str) -> &'static str {
    if RE_HDR10P.is_match(t) {
        "HDR10+"
    } else if RE_HLG.is_match(t) {
        "HLG"
    } else if RE_HDR10_ANY.is_match(t) {
        "HDR10"
    } else if RE_HDR_PLAIN.is_match(t) {
        "HDR"
    } else {
        ""
    }
}

/// Go `detectAudioCodec`: the normalized source audio family, or "". Most specific first, so "DTS-HD MA" is
/// not caught by the bare "dts" rule. The browser codecs come last: a release naming DD+ and AAC carries
/// DD+ as its main track and AAC as the stereo companion.
pub(crate) fn detect_audio_codec(t: &str) -> &'static str {
    if RE_DTSX.is_match(t) {
        "dtsx"
    } else if RE_TRUEHD.is_match(t) {
        "truehd"
    } else if RE_DTSHD_MA.is_match(t) {
        "dtshdma"
    } else if RE_DTSHD_ATTR.is_match(t) {
        "dtshd"
    } else if RE_FLAC.is_match(t) {
        "flac"
    } else if RE_EAC3.is_match(t) {
        "eac3"
    } else if RE_DTS.is_match(t) {
        "dts"
    } else if RE_AC3.is_match(t) {
        "ac3"
    } else if RE_AAC.is_match(t) {
        "aac"
    } else if RE_OPUS.is_match(t) {
        "opus"
    } else if RE_MP3.is_match(t) {
        "mp3"
    } else {
        ""
    }
}

/// Go `detectChannels`: "7.1", "5.1", "2.0", or "" (e.g. "DDP5.1" and "DDP5 1" give "5.1").
fn detect_channels(t: &str) -> &'static str {
    if RE_CH71.is_match(t) {
        "7.1"
    } else if RE_CH51.is_match(t) {
        "5.1"
    } else if RE_CH20.is_match(t) {
        "2.0"
    } else {
        ""
    }
}

/// Go `detectAudio`: the display string for the audio.
fn detect_audio(t: &str) -> &'static str {
    if RE_ATMOS.is_match(t) {
        "Atmos"
    } else if RE_DTSX.is_match(t) {
        "DTS:X"
    } else if RE_TRUEHD.is_match(t) {
        "TrueHD"
    } else if RE_DTSHD_ATTR.is_match(t) {
        "DTS-HD"
    } else if RE_FLAC.is_match(t) {
        "FLAC"
    } else if RE_EAC3.is_match(t) {
        "EAC3"
    } else if RE_DTS.is_match(t) {
        "DTS"
    } else if RE_AC3.is_match(t) {
        "AC3"
    } else {
        ""
    }
}

/// Go `streamAttributes`: the attributes of one stream, read from its title and, when probed, the file.
pub(crate) fn stream_attributes(s: RawStream) -> StreamAttributes {
    let t = go_to_lower(&s.title);
    let dolby_vision = RE_DOVI_ATTR.is_match(&t);
    let hdr = dolby_vision || RE_HDR_EXTRA.is_match(&t);
    let mut attrs = StreamAttributes {
        resolution: str_opt(detect_resolution_lower(&t)),
        source: str_opt(detect_source_attr(&t)),
        codec: str_opt(detect_codec(&t)),
        hdr,
        dolby_vision,
        hdr_format: str_opt(detect_hdr_format(&t)),
        audio: str_opt(detect_audio(&t)),
        audio_codec: str_opt(detect_audio_codec(&t)),
        audio_channels: str_opt(detect_channels(&t)),
        atmos: RE_ATMOS.is_match(&t),
        hardcoded_subs: RE_KORSUB_HC.is_match(&t),
        three_d: RE_3D.is_match(&t),
        size_bytes: s.size_bytes,
        pack_size_bytes: s.pack_size_bytes,
        seeders: s.seeders,
        cached: cached_claim(&s),
        label: clean_label_lower(&t, &s), // reuse the title we already lowercased
        bit_depth: detect_bit_depth(&t, hdr),
        ..Default::default()
    };
    attrs = with_probe(attrs, s.probe.as_ref());
    if attrs.dolby_vision && attrs.dv_profile == 0 {
        attrs.dv_profile_guess = guess_dv_profile(&t, attrs.hdr_format.is_some());
    }
    attrs
}

/// Go `guessDVProfile`: the likely Dolby Vision profile from the name, for when no probe has read the real
/// one. A remux keeps profile 7. An encode from Blu-ray is converted to 8.1, as is a HYBRID, and a name that
/// also gives an HDR10-family base names the base layer profile 8 has. A web release naming no base is taken
/// as a streaming service's profile 5. Nothing else is guessed.
fn guess_dv_profile(t: &str, names_hdr_base: bool) -> i64 {
    if RE_REMUX.is_match(t) {
        7
    } else if names_hdr_base || RE_HYBRID.is_match(t) || RE_BLURAY.is_match(t) || RE_BR_RIP.is_match(t) {
        8
    } else if RE_WEB_DL.is_match(t) || RE_WEB_RIP.is_match(t) || RE_WEB.is_match(t) {
        5
    } else {
        0
    }
}

/// Go `withProbe`: lets the FILE override the title wherever it has something to say. Codec and channel
/// layout are corrected where they disagree, and languages are added. Only ever overrides with a non-empty
/// value: a probe that couldn't read a field leaves the title's guess standing.
pub(crate) fn with_probe(attrs: StreamAttributes, p: Option<&Probe>) -> StreamAttributes {
    let Some(p) = p else {
        return attrs;
    };
    let mut attrs = attrs;
    attrs.probed = true;
    attrs.audio_languages = p.audio.clone();
    attrs.subtitle_languages = p.subtitles.clone();
    attrs.untagged_audio = i64::from(p.untagged_audio);
    if !p.video_codec.is_empty() {
        attrs.codec = Some(p.video_codec.clone());
    }
    if !p.audio_channels.is_empty() {
        attrs.audio_channels = Some(p.audio_channels.clone());
    }
    if p.dolby_vision {
        attrs.dolby_vision = true;
        attrs.hdr = true;
    }
    if p.bit_depth != 0 {
        attrs.bit_depth = i64::from(p.bit_depth);
    }
    if p.dv_profile != 0 {
        attrs.dv_profile = i64::from(p.dv_profile);
    }
    if !p.hdr_format.is_empty() {
        attrs.hdr_format = Some(p.hdr_format.clone());
        attrs.hdr = true;
    }
    if p.width != 0 {
        attrs.resolution = Some(resolution_of_width(i64::from(p.width)).to_string());
    }
    attrs.video_level = i64::from(p.video_level);
    attrs.high_tier = p.high_tier;
    attrs.width = i64::from(p.width);
    attrs.height = i64::from(p.height);
    attrs.frame_rate = p.frame_rate;
    attrs
}

/// Go `resolutionOfWidth`: buckets a probed picture as the release names do. By width, not height: a scope
/// film at 3840 x 1608 is a 2160p release.
fn resolution_of_width(w: i64) -> &'static str {
    if w >= 3200 {
        "2160p"
    } else if w >= 1700 {
        "1080p"
    } else if w >= 1100 {
        "720p"
    } else {
        "480p"
    }
}

/// Go `containerOf`: the probe's container when the file was read, else the release name's extension, else
/// "". `notWebReady` and a browser's ranking both ask this, so they cannot disagree about a file.
pub(crate) fn container_of(s: RawStream) -> String {
    if let Some(p) = &s.probe {
        if !p.container.is_empty() {
            return p.container.clone();
        }
    }
    let t = go_to_lower(&s.title);
    if t.ends_with(".mp4") || t.ends_with(".m4v") {
        "mp4".to_string()
    } else if t.ends_with(".mkv") {
        "matroska".to_string()
    } else if t.ends_with(".webm") {
        "webm".to_string()
    } else if t.ends_with(".avi") {
        "avi".to_string()
    } else {
        String::new()
    }
}

/// Go `cachedClaim`: cachedness only when it was actually observed. A failed cache check is nil, and sending
/// `false` would be a claim nobody made.
fn cached_claim(s: &RawStream) -> Option<bool> {
    if s.cache_known {
        Some(s.cached)
    } else {
        None
    }
}
