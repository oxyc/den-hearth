//! Port of `internal/scout/attributes.go`.

use serde::{Deserialize, Serialize};

use crate::label::clean_label_traits;
use crate::probe::Probe;
use crate::rank::{resolution_bucket, RawStream};
use crate::traits::{Pat, ReleaseTraits};

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

/// Go `strPtr`: "" is nil.
fn str_opt(s: &str) -> Option<String> {
    if s.is_empty() {
        None
    } else {
        Some(s.to_string())
    }
}

/// Go `detectSourceAttr`.
pub(crate) fn detect_source_attr(t: &ReleaseTraits) -> &'static str {
    let junk = t.junk_class();
    if matches!(junk, "cam" | "telesync" | "screener") {
        return junk;
    }
    if t.has(Pat::Remux) {
        "remux"
    } else if t.has(Pat::Bluray) || t.has(Pat::BrRip) {
        "bluray"
    } else if t.has(Pat::WebDl) {
        "webdl"
    } else if t.has(Pat::WebRip) {
        "webrip"
    } else if t.has(Pat::Web) {
        "web"
    } else if t.has(Pat::Hdtv) {
        "hdtv"
    } else if t.has(Pat::DvdRip) {
        "dvdrip"
    } else {
        ""
    }
}

/// Go `detectCodec`: the codec off the release title. Its spellings must match the probe's, because
/// `with_probe` overwrites this field rather than merging with it. h264 is the spelling that wins because
/// three container parsers already produce it.
fn detect_codec(t: &ReleaseTraits) -> &'static str {
    if t.has(Pat::Av1) {
        "av1"
    } else if t.has(Pat::Hevc) {
        "hevc"
    } else if t.has(Pat::Avc) || t.has(Pat::Hi10) {
        // Hi10P is H.264's High 10 profile by name; anime releases give it instead of "x264".
        "h264"
    } else {
        ""
    }
}

/// Go `detectBitDepth`: 10 when the title says so or the release is HDR or Dolby Vision (both 10-bit by
/// definition). Otherwise 0: a title that says nothing is not evidence of 8-bit.
fn detect_bit_depth(t: &ReleaseTraits, hdr: bool) -> i64 {
    if hdr || t.has(Pat::TenBit) {
        10
    } else {
        0
    }
}

/// Go `detectHDRFormat`: the HDR10-family label, or "". HDR10+ wins over a bare "hdr10", HLG over generic
/// "hdr". Dolby Vision is reported separately.
fn detect_hdr_format(t: &ReleaseTraits) -> &'static str {
    if t.has(Pat::Hdr10p) {
        "HDR10+"
    } else if t.has(Pat::Hlg) {
        "HLG"
    } else if t.has(Pat::Hdr10Any) {
        "HDR10"
    } else if t.has(Pat::HdrPlain) {
        "HDR"
    } else {
        ""
    }
}

/// Go `detectAudioCodec` on an already-lowercased title.
#[cfg(test)]
pub(crate) fn detect_audio_codec(t: &str) -> &'static str {
    audio_codec(&ReleaseTraits::parse_lower(t))
}

/// Go `detectAudioCodec`: the normalized source audio family, or "". Most specific first, so "DTS-HD MA" is
/// not caught by the bare "dts" rule. The browser codecs come last: a release naming DD+ and AAC carries
/// DD+ as its main track and AAC as the stereo companion.
fn audio_codec(t: &ReleaseTraits) -> &'static str {
    if t.has(Pat::Dtsx) {
        "dtsx"
    } else if t.has(Pat::Truehd) {
        "truehd"
    } else if t.has(Pat::DtshdMa) {
        "dtshdma"
    } else if t.has(Pat::DtshdAttr) {
        "dtshd"
    } else if t.has(Pat::Flac) {
        "flac"
    } else if t.has(Pat::Eac3) {
        "eac3"
    } else if t.has(Pat::Dts) {
        "dts"
    } else if t.has(Pat::Ac3) {
        "ac3"
    } else if t.has(Pat::Aac) {
        "aac"
    } else if t.opus() {
        "opus"
    } else if t.has(Pat::Mp3) {
        "mp3"
    } else {
        ""
    }
}

/// Go `detectChannels`: "7.1", "5.1", "2.0", or "" (e.g. "DDP5.1" and "DDP5 1" give "5.1").
fn detect_channels(t: &ReleaseTraits) -> &'static str {
    if t.has(Pat::Ch71) {
        "7.1"
    } else if t.has(Pat::Ch51) {
        "5.1"
    } else if t.has(Pat::Ch20) {
        "2.0"
    } else {
        ""
    }
}

/// Go `detectAudio`: the display string for the audio.
fn detect_audio(t: &ReleaseTraits) -> &'static str {
    if t.has(Pat::Atmos) {
        "Atmos"
    } else if t.has(Pat::Dtsx) {
        "DTS:X"
    } else if t.has(Pat::Truehd) {
        "TrueHD"
    } else if t.has(Pat::DtshdAttr) {
        "DTS-HD"
    } else if t.has(Pat::Flac) {
        "FLAC"
    } else if t.has(Pat::Eac3) {
        "EAC3"
    } else if t.has(Pat::Dts) {
        "DTS"
    } else if t.has(Pat::Ac3) {
        "AC3"
    } else {
        ""
    }
}

/// Go `streamAttributes`: the attributes of one stream, read from its title and, when probed, the file.
#[cfg(test)]
pub(crate) fn stream_attributes(s: RawStream) -> StreamAttributes {
    stream_attributes_ref(&s)
}

/// Borrowed form for ranking and presentation paths that retain ownership of the stream.
pub(crate) fn stream_attributes_ref(s: &RawStream) -> StreamAttributes {
    let mut attrs = cost_attributes(s);
    attrs.label = clean_label_traits(s.traits(), s);
    attrs
}

/// `stream_attributes_ref` without the display label, which nothing that weighs a release reads.
pub(crate) fn cost_attributes(s: &RawStream) -> StreamAttributes {
    let t = s.traits();
    let dolby_vision = t.has(Pat::Dovi);
    let hdr = dolby_vision || t.has(Pat::Hdr10Any) || t.has(Pat::HdrPlain) || t.has(Pat::Hlg);
    let mut attrs = StreamAttributes {
        resolution: str_opt(resolution_bucket(t)),
        source: str_opt(detect_source_attr(t)),
        codec: str_opt(detect_codec(t)),
        hdr,
        dolby_vision,
        hdr_format: str_opt(detect_hdr_format(t)),
        audio: str_opt(detect_audio(t)),
        audio_codec: str_opt(audio_codec(t)),
        audio_channels: str_opt(detect_channels(t)),
        atmos: t.has(Pat::Atmos),
        hardcoded_subs: t.has(Pat::KorsubHc),
        three_d: t.has(Pat::ThreeD),
        size_bytes: s.size_bytes,
        pack_size_bytes: s.pack_size_bytes,
        seeders: s.seeders,
        cached: cached_claim(s),
        bit_depth: detect_bit_depth(t, hdr),
        ..Default::default()
    };
    attrs = with_probe(attrs, s.probe.as_ref());
    if attrs.dolby_vision && attrs.dv_profile == 0 {
        attrs.dv_profile_guess = guess_dv_profile(t, attrs.hdr_format.is_some());
    }
    attrs
}

/// Go `guessDVProfile`: the likely Dolby Vision profile from the name, for when no probe has read the real
/// one. A remux keeps profile 7. An encode from Blu-ray is converted to 8.1, as is a HYBRID, and a name that
/// also gives an HDR10-family base names the base layer profile 8 has. A web release naming no base is taken
/// as a streaming service's profile 5. Nothing else is guessed.
fn guess_dv_profile(t: &ReleaseTraits, names_hdr_base: bool) -> i64 {
    if t.has(Pat::Remux) {
        7
    } else if names_hdr_base || t.has(Pat::Hybrid) || t.has(Pat::Bluray) || t.has(Pat::BrRip) {
        8
    } else if t.has(Pat::WebDl) || t.has(Pat::WebRip) || t.has(Pat::Web) {
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
#[cfg(test)]
pub(crate) fn container_of(s: RawStream) -> String {
    container_of_ref(&s).to_string()
}

/// Borrowed form for callers that only need to inspect a retained stream.
pub(crate) fn container_of_ref(s: &RawStream) -> &str {
    if let Some(p) = &s.probe {
        if !p.container.is_empty() {
            return &p.container;
        }
    }
    s.traits().name_container().as_str()
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
