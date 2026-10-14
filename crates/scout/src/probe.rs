//! Port of `internal/scout/probe.go`: reading the head of a resolved link to learn what the file itself
//! says (container, codecs, audio layout, HDR, Dolby Vision, languages), Matroska first; MP4 and AVI are
//! in `probe_mp4.rs` and `probe_avi.rs`. `Probe` is FROZEN (see PORTING.md): `RawStream` carries it and
//! the cache stores it as JSON.

use serde::{Deserialize, Serialize};

use crate::indexerconfig::null_default;
use crate::util::go_to_lower;

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
    /// Go writes a probe with no languages as `null` (a nil slice, and no `omitempty`), so a cached one reads as
    /// empty here.
    #[serde(rename = "audioLanguages", default, deserialize_with = "null_default")]
    pub audio: Vec<String>,
    #[serde(rename = "subtitleLanguages", default, deserialize_with = "null_default")]
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
/// Go `trackTypeSub`.
const TRACK_TYPE_SUB: u8 = 17;

/// Go `ProbeTracks`: reads the head of a resolved link and parses it. The size comes from the response's
/// total, not from the bytes read.
pub(crate) async fn probe_tracks(client: &reqwest::Client, url: &str) -> Result<Probe, String> {
    let mut resp = client
        .get(url)
        .header(reqwest::header::RANGE, format!("bytes=0-{}", PROBE_BYTES - 1))
        .send()
        .await
        .map_err(|e| e.without_url().to_string())?;
    let status = resp.status();
    if status != reqwest::StatusCode::PARTIAL_CONTENT && status != reqwest::StatusCode::OK {
        return Err(format!("probe: unexpected status {}", status.as_u16()));
    }
    // Bounded regardless of what the server does with the Range header — a 200 means the whole file is
    // coming, and reading all of a 20 GB remux to find a 5 KB header would be its own outage.
    let mut head = Vec::new();
    while head.len() < PROBE_BYTES {
        match resp.chunk().await.map_err(|e| e.without_url().to_string())? {
            Some(chunk) => head.extend_from_slice(&chunk),
            None => break,
        }
    }
    head.truncate(PROBE_BYTES);
    // Drain a BOUNDED amount before closing, never the whole body. Draining aids connection reuse, but an
    // unbounded drain here would download an entire 20 GB remux to throw it away the moment a server
    // ignored the Range header and answered 200 — turning a 1 MiB probe into the download it exists to
    // avoid. The cap is the same megabyte we asked for.
    let mut drained = 0usize;
    while drained < 1 << 16 {
        match resp.chunk().await {
            Ok(Some(chunk)) => drained += chunk.len(),
            _ => break,
        }
    }
    let mut p = parse_head(&head).map_err(str::to_string)?;
    p.size_bytes = probed_total_size(&resp);
    Ok(p)
}

/// Go `probedTotalSize`: reads the FILE's size off the response to the ranged GET `probe_tracks` just made:
/// the Content-Range total on a 206, or Content-Length on a 200 (a server that ignored the Range and is
/// sending the whole thing). 0 when neither says — a CDN that gives no total leaves the caller with
/// nothing to correct the indexer's number with, same as never having probed.
fn probed_total_size(resp: &reqwest::Response) -> i64 {
    if resp.status() == reqwest::StatusCode::PARTIAL_CONTENT {
        let v = resp.headers().get("content-range").and_then(|v| v.to_str().ok()).unwrap_or("");
        return content_range_total(v).unwrap_or_default();
    }
    // The header, as Go's ContentLength is: reqwest's `content_length()` reports the body bytes still
    // unread, which shrink as the head is read.
    resp.headers()
        .get(reqwest::header::CONTENT_LENGTH)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.parse::<i64>().ok())
        .filter(|n| *n > 0)
        .unwrap_or(0)
}

/// Go `contentRangeTotal` (linkcheck.go): the total after the first `/` of a Content-Range value, when it
/// parses as a positive integer. A private copy of linkcheck.go's function of the same name, which belongs to
/// linkcheck.rs; the two are to be one function once that module offers it.
fn content_range_total(v: &str) -> Option<i64> {
    let (_, total) = v.split_once('/')?;
    let n: i64 = total.trim().parse().ok()?;
    (n > 0).then_some(n)
}

/// Go `ParseHead`: detects the container from the head and dispatches to the matching parser.
///
/// Deliberately three small parsers instead of ffmpeg. Everything needed here — track types, languages,
/// codec — sits in structures a few hundred lines can read, and the alternative is a 30-80 MB binary in a
/// distroless image plus ffmpeg's parser exposed to untrusted remote bytes. If richer facts are ever wanted
/// (channel counts, HDR metadata), ffprobe belongs BEHIND this as a fallback, not in front of it.
pub(crate) fn parse_head(head: &[u8]) -> Result<Probe, &'static str> {
    let (p, ok) = crate::probe_avi::parse_avi(head);
    if ok {
        return Ok(p);
    }
    let (p, ok) = crate::probe_mp4::parse_mp4(head);
    if ok {
        return Ok(p);
    }
    parse_matroska_tracks(head)
}

/// Go `ParseMatroskaTracks`: extracts the languages from a Matroska head. Pure, so the parsing is testable
/// without a network or a whole file.
pub(crate) fn parse_matroska_tracks(head: &[u8]) -> Result<Probe, &'static str> {
    let payload = find_tracks_payload(head).ok_or(ERR_NO_TRACKS)?;
    let mut out = Probe { container: "matroska".into(), ..Default::default() };
    let mut pos = 0usize;
    while pos < payload.len() {
        let Some((id, id_len)) = read_id(&payload[pos..]) else { break };
        let Some((body, next)) = child_payload(payload, pos + id_len) else { break };
        if id == ID_TRACK_ENTRY {
            read_track_facts(body, &mut out);
            let (kind, lang) = parse_track_entry(body);
            if lang.is_empty() {
                // A track with no language is not a claim about the language.
            } else if kind == TRACK_TYPE_AUDIO && lang == "und" {
                out.untagged_audio += 1;
            } else if kind == TRACK_TYPE_SUB && lang == "und" {
                out.untagged_subtitles += 1;
            } else if kind == TRACK_TYPE_AUDIO {
                append_unique(&mut out.audio, &lang);
            } else if kind == TRACK_TYPE_SUB {
                append_unique(&mut out.subtitles, &lang);
            }
        }
        pos = next;
    }
    if out.audio.is_empty()
        && out.subtitles.is_empty()
        && out.untagged_audio == 0
        && out.untagged_subtitles == 0
    {
        return Err(ERR_NO_TRACKS);
    }
    Ok(out)
}

/// Go `findTracksPayload`: walks the container to Tracks: top level for Segment, then Segment's children.
///
/// Deliberately a WALK and not a search for the id. Matroska's SeekHead stores the ids of the elements it
/// points at as DATA, so a file carries the four bytes of the Tracks id long before Tracks itself — the
/// first version scanned, matched the pointer at offset 83, and parsed the seek table as if it were a track
/// list. The real element was at 316.
fn find_tracks_payload(head: &[u8]) -> Option<&[u8]> {
    let mut pos = 0usize;
    while pos < head.len() {
        let (id, id_len) = read_id(&head[pos..])?;
        let (size, size_len, unknown) = read_size_at(head, pos + id_len)?;
        let body = pos + id_len + size_len;
        if id == ID_SEGMENT {
            // Segment routinely declares an unknown size (a muxer writing a stream it hasn't finished),
            // in which case its children simply run to the end of what we read.
            let mut end = head.len();
            if !unknown {
                if let Some(candidate) = usize::try_from(size).ok().and_then(|s| body.checked_add(s)) {
                    if candidate < end {
                        end = candidate;
                    }
                }
            }
            return find_tracks_in(head.get(body..end)?);
        }
        if unknown {
            return None;
        }
        pos = body.checked_add(usize::try_from(size).ok()?)?;
    }
    None
}

/// Go `findTracksIn`: the Tracks element among a Segment's children.
fn find_tracks_in(segment: &[u8]) -> Option<&[u8]> {
    let mut pos = 0usize;
    while pos < segment.len() {
        let (id, id_len) = read_id(&segment[pos..])?;
        let (size, size_len, unknown) = read_size_at(segment, pos + id_len)?;
        if unknown {
            return None;
        }
        let body = pos + id_len + size_len;
        let end = body.checked_add(usize::try_from(size).ok()?)?;
        if end > segment.len() {
            return None;
        }
        if id == ID_TRACKS {
            return Some(&segment[body..end]);
        }
        pos = end;
    }
    None
}

/// Go `parseTrackEntry`: returns the track's type and language, or "und" when the file doesn't say.
///
/// Matroska specifies that an absent Language means "eng", and the first version honoured that. On real
/// files it is a trap: an untagged AC-3 track in a SWEDISH series would have been announced as English
/// audio. In practice an absent tag means the muxer didn't fill it in, not that the audio is English — and
/// a confident wrong language is worse for the viewer than an honest unknown.
pub(crate) fn parse_track_entry(entry: &[u8]) -> (u8, String) {
    let mut kind: u64 = 0;
    let mut lang = String::from("und");
    let mut pos = 0usize;
    while pos < entry.len() {
        let Some((id, id_len)) = read_id(&entry[pos..]) else { return (0, String::new()) };
        let Some((body, next)) = child_payload(entry, pos + id_len) else { return (0, String::new()) };
        if id == u32::from(ID_TRACK_TYPE) {
            kind = uint_from(body);
        } else if id == ID_LANGUAGE || id == ID_LANGUAGE_BCP {
            // Each tag replaces the one before it, so the last one in the entry wins. Go's comment on the BCP-47
            // case says "BCP-47 wins where both exist: it is the field a modern muxer fills in", but its code
            // is this same last-wins assignment for both elements, so a BCP-47 tag written BEFORE the ISO 639-2 one
            // loses to it. The code is what Go does, so it is what this does.
            let s = clean_lang(body);
            if !s.is_empty() {
                lang = s;
            }
        }
        pos = next;
    }
    // An out-of-range type can never be audio or subtitles, so it clamps to a value that matches neither.
    let kind = u8::try_from(kind).unwrap_or(u8::MAX);
    if kind != TRACK_TYPE_AUDIO && kind != TRACK_TYPE_SUB {
        return (kind, String::new());
    }
    (kind, lang)
}

/// Go `cleanLang`: normalises a tag to a two-letter code where one exists.
///
/// The same file can carry both ISO 639-2 ("swe") and BCP-47 ("sv") for the same track, and across the
/// sweep the output mixed the two — "eng" for one release and "en" for the next, which no consumer can
/// group or match on. Everything lands on ISO 639-1 where a mapping exists; anything else passes through
/// as given rather than being dropped.
pub(crate) fn clean_lang(b: &[u8]) -> String {
    let raw = go_lossy_utf8(b);
    let s = go_to_lower(raw.trim_end_matches('\0').trim());
    if s == "und" || s.is_empty() {
        return String::new();
    }
    // "pt-BR" and "pt" are the same shelf for a viewer choosing a track.
    let base = match s.find(['-', '_']) {
        Some(i) if i > 0 => &s[..i],
        _ => s.as_str(),
    };
    match ISO_639_2.iter().find(|(code, _)| *code == base) {
        Some((_, two)) => (*two).to_string(),
        None => base.to_string(),
    }
}

/// Go's reading of bytes that may not be UTF-8: `strings.ToLower` and friends turn each byte that is not part of a
/// valid sequence into its own U+FFFD, where `String::from_utf8_lossy` turns a whole broken sequence into one. A
/// language tag is whatever bytes the muxer wrote, and it is passed through as given, so the count shows.
fn go_lossy_utf8(mut b: &[u8]) -> String {
    let mut out = String::with_capacity(b.len());
    while !b.is_empty() {
        match std::str::from_utf8(b) {
            Ok(valid) => {
                out.push_str(valid);
                break;
            }
            Err(e) => {
                let (valid, rest) = b.split_at(e.valid_up_to());
                out.push_str(std::str::from_utf8(valid).unwrap_or_default());
                out.push('\u{FFFD}');
                b = rest.get(1..).unwrap_or_default();
            }
        }
    }
    out
}

/// Go `iso639`: ISO 639-2 (bibliographic AND terminological — muxers emit both) to 639-1, for the languages
/// that actually turn up on releases. Unlisted codes pass through unchanged.
const ISO_639_2: &[(&str, &str)] = &[
    ("eng", "en"),
    ("swe", "sv"),
    ("nor", "no"),
    ("nob", "no"),
    ("dan", "da"),
    ("fin", "fi"),
    ("isl", "is"),
    ("ice", "is"),
    ("ger", "de"),
    ("deu", "de"),
    ("fre", "fr"),
    ("fra", "fr"),
    ("spa", "es"),
    ("ita", "it"),
    ("por", "pt"),
    ("dut", "nl"),
    ("nld", "nl"),
    ("pol", "pl"),
    ("rus", "ru"),
    ("ukr", "uk"),
    ("cze", "cs"),
    ("ces", "cs"),
    ("slo", "sk"),
    ("slk", "sk"),
    ("hun", "hu"),
    ("rum", "ro"),
    ("ron", "ro"),
    ("gre", "el"),
    ("ell", "el"),
    ("tur", "tr"),
    ("ara", "ar"),
    ("heb", "he"),
    ("hin", "hi"),
    ("jpn", "ja"),
    ("kor", "ko"),
    ("chi", "zh"),
    ("zho", "zh"),
    ("cmn", "zh"),
    ("yue", "zh"),
    ("tha", "th"),
    ("vie", "vi"),
    ("ind", "id"),
    ("may", "ms"),
    ("msa", "ms"),
    ("fil", "fil"),
    ("hrv", "hr"),
    ("srp", "sr"),
    ("bul", "bg"),
    ("est", "et"),
    ("lav", "lv"),
    ("lit", "lt"),
    ("cat", "ca"),
    ("baq", "eu"),
    ("glg", "gl"),
    ("per", "fa"),
    ("fas", "fa"),
];

/// Go `appendUnique`: adds `v` unless the list already holds it.
pub(crate) fn append_unique(list: &mut Vec<String>, v: &str) {
    if !list.iter().any(|existing| existing == v) {
        list.push(v.to_string());
    }
}

/// Go `readID`: reads a variable-length element id, returning it and its byte length.
fn read_id(buf: &[u8]) -> Option<(u32, usize)> {
    let first = *buf.first()?;
    let n = leading_length(first);
    if n == 0 || n > 4 || buf.len() < n {
        return None;
    }
    let id = buf[..n].iter().fold(0u32, |id, &b| id << 8 | u32::from(b));
    Some((id, n))
}

/// Go `childPayload`: reads a size at `p` and returns the payload plus the offset just past it.
fn child_payload(buf: &[u8], p: usize) -> Option<(&[u8], usize)> {
    let (size, n, _) = read_size_at(buf, p)?;
    let start = p + n;
    let end = start.checked_add(usize::try_from(size).ok()?)?;
    if end > buf.len() {
        return None;
    }
    Some((&buf[start..end], end))
}

/// Go `readSizeAt`. It also reports EBML's unknown-size marker (all value bits set), which a Segment
/// commonly uses.
fn read_size_at(buf: &[u8], p: usize) -> Option<(u64, usize, bool)> {
    let first = *buf.get(p)?;
    let n = leading_length(first);
    if n == 0 || p.checked_add(n)? > buf.len() {
        return None;
    }
    // `0xFF >> n` is 0 at n == 8, which a u32 shift handles where a u8 one would overflow.
    let mask = (0xFFu32 >> n) as u8;
    let mut v = u64::from(first & mask);
    let mut all_ones = (first & mask) == mask;
    for &b in &buf[p + 1..p + n] {
        v = v << 8 | u64::from(b);
        all_ones = all_ones && b == 0xFF;
    }
    Some((v, n, all_ones))
}

/// Go `leadingLength`: the EBML width marker, the number of leading zero bits before the first 1, plus one.
fn leading_length(b: u8) -> usize {
    if b == 0 {
        0
    } else {
        b.leading_zeros() as usize + 1
    }
}

/// Go `uintFrom`: a big-endian unsigned integer of any width. Bits shifted out of the top are discarded,
/// as Go's `uint64` shift discards them.
fn uint_from(b: &[u8]) -> u64 {
    b.iter().fold(0u64, |v, &c| v << 8 | u64::from(c))
}

/// Go `readTrackFacts`: pulls the descriptive fields off a Matroska TrackEntry: the codec the video declares
/// and how many channels its audio carries.
///
/// Audio takes the BEST layout present, not the first: a release carrying both 5.1 and 7.1 is a 7.1
/// release, and the track listed first is routinely the plainer one — one real file listed 5.1 ahead of
/// three 7.1 tracks.
fn read_track_facts(entry: &[u8], out: &mut Probe) {
    let mut kind: u64 = 0;
    let mut dv_profile: i64 = 0;
    let mut width: i64 = 0;
    let mut height: i64 = 0;
    let mut transfer: i64 = 0;
    let mut codec: &'static str = "";
    let mut private: &[u8] = &[];
    let mut frame_duration: u64 = 0;
    let mut pos = 0usize;
    while pos < entry.len() {
        let Some((id, id_len)) = read_id(&entry[pos..]) else { return };
        let Some((body, next)) = child_payload(entry, pos + id_len) else { return };
        if id == u32::from(ID_TRACK_TYPE) {
            kind = uint_from(body);
        } else if id == ID_CODEC_ID {
            codec = codec_from_matroska_id(&String::from_utf8_lossy(body));
        } else if id == ID_CODEC_PRIVATE {
            private = body;
        } else if id == ID_BLOCK_ADDITION_MAPPING {
            let p = matroska_dv_profile(body);
            if p != 0 {
                dv_profile = p;
            }
        } else if id == ID_AUDIO {
            let n = child_uint(body, ID_CHANNELS) as i64;
            if n > channel_count(&out.audio_channels) {
                out.audio_channels = channel_layout(n).to_string();
            }
        } else if id == ID_DEFAULT_DURATION {
            frame_duration = uint_from(body);
        } else if id == ID_VIDEO {
            width = child_uint(body, ID_PIXEL_WIDTH) as i64;
            height = child_uint(body, ID_PIXEL_HEIGHT) as i64;
            if let Some((colour, _)) = find_child(body, ID_COLOUR) {
                transfer = child_uint(colour, ID_TRANSFER_CHARACTERISTICS) as i64;
            }
        }
        pos = next;
    }
    if kind == u64::from(TRACK_TYPE_VIDEO) && !codec.is_empty() && out.video_codec.is_empty() {
        out.video_codec = codec.to_string();
        out.bit_depth = bit_depth_from_config(codec, private) as i32;
        (out.video_profile, out.video_level, out.high_tier) = video_shape(codec, private);
        out.width = i32::try_from(width).unwrap_or(0);
        out.height = i32::try_from(height).unwrap_or(0);
        if frame_duration > 0 {
            out.frame_rate = round_rate(1e9 / frame_duration as f64);
        }
        out.hdr_format = hdr_from_transfer(transfer).to_string();
    }
    // Dolby Vision rides as a block-addition mapping whose type is the configuration box's name. The
    // literal tag anywhere in the entry still marks it present when the mapping is too damaged to read a
    // profile from.
    if kind == u64::from(TRACK_TYPE_VIDEO) && (dv_profile != 0 || has_dv_config_tag(entry)) {
        out.dolby_vision = true;
        if out.dv_profile == 0 {
            out.dv_profile = dv_profile as i32;
        }
    }
}

/// The boxes a Dolby Vision configuration record travels in. Per Dolby's ISOBMFF spec, dvcC holds
/// profiles up to 7, dvvC 8 to 10, and dvwC anything past 10. Matroska uses the same four characters,
/// as a big-endian uint, for its BlockAddIDType.
const DV_CONFIG_TYPES: [[u8; 4]; 3] = [*b"dvcC", *b"dvvC", *b"dvwC"];

/// Go `hasDVConfigTag`.
fn has_dv_config_tag(entry: &[u8]) -> bool {
    DV_CONFIG_TYPES.iter().any(|tag| entry.windows(4).any(|w| w == tag))
}

/// Go `matroskaDVProfile`: reads one BlockAdditionMapping. Its BlockAddIDType names a Dolby Vision
/// configuration box, and its BlockAddIDExtraData holds the record itself. That is the Matroska
/// codec-mappings spec, and what ffmpeg's matroskadec reads (mkv_parse_block_addition_mappings).
fn matroska_dv_profile(mapping: &[u8]) -> i64 {
    let typ = child_uint(mapping, ID_BLOCK_ADD_ID_TYPE);
    for tag in DV_CONFIG_TYPES {
        if typ == u64::from(u32::from_be_bytes(tag)) {
            let record: &[u8] = match find_child(mapping, ID_BLOCK_ADD_ID_EXTRA_DATA) {
                Some((body, _)) => body,
                None => &[],
            };
            return dovi_profile(record);
        }
    }
    0
}

/// Go `doviProfile`: reads dv_profile from a DOVIDecoderConfigurationRecord, which starts:
///
///   byte 0   dv_version_major                 8 bits
///   byte 1   dv_version_minor                 8 bits
///   byte 2   dv_profile                       high 7 bits
///            dv_level                         low bit of byte 2 + high 5 bits of byte 3
///   byte 3   rpu / el / bl_present_flag       low 3 bits
///   byte 4   dv_bl_signal_compatibility_id    high 4 bits
///
/// Source: Dolby, "Dolby Vision Streams Within the ISO Base Media File Format", and the same fields read
/// in the same order by ffmpeg's libavformat/dovi_isom.c (ff_isom_parse_dvcc_dvvc). MP4's dvcC/dvvC/dvwC
/// box and Matroska's BlockAddIDExtraData carry this record byte for byte. Like ffmpeg, this refuses a
/// record shorter than 4 bytes.
pub(crate) fn dovi_profile(record: &[u8]) -> i64 {
    if record.len() < 4 {
        return 0;
    }
    i64::from(record[2] >> 1)
}

/// Go `bitDepthFromConfig`: reads the luma bit depth from the configuration record a container stores for
/// the video: MP4's avcC/hvcC box, or Matroska's CodecPrivate, which holds the same bytes. 0 when it can't
/// tell.
pub(crate) fn bit_depth_from_config(codec: &str, record: &[u8]) -> i64 {
    match codec {
        "h264" => avc_bit_depth(record),
        "hevc" => hevc_bit_depth(record),
        "av1" => av1_bit_depth(record),
        _ => 0,
    }
}

/// Go `videoShape`: reads the profile, level and tier from the same configuration record, laid out as
/// ISO/IEC 14496-15 and the AV1 ISOBMFF binding specify:
///
///   avcC  byte 1  AVCProfileIndication                byte 3  AVCLevelIndication
///   hvcC  byte 1  general_profile_space (2 bits), general_tier_flag (1), general_profile_idc (5)
///         byte 12 general_level_idc
///   av1C  byte 0  marker (1) and version (7), 0x81    byte 1  seq_profile (3), seq_level_idx_0 (5)
///         byte 2  seq_tier_0 (1), high_bitdepth (1), twelve_bit (1), …
///
/// Zeros where the record is too short or isn't the codec's.
pub(crate) fn video_shape(codec: &str, record: &[u8]) -> (i32, i32, bool) {
    match codec {
        "h264" if record.len() >= 4 => (i32::from(record[1]), i32::from(record[3]), false),
        "hevc" if record.len() >= 13 => {
            (i32::from(record[1] & 0x1F), i32::from(record[12]), record[1] & 0x20 != 0)
        }
        "av1" if record.len() >= 3 && record[0] == 0x81 => {
            (i32::from(record[1] >> 5), i32::from(record[1] & 0x1F), record[2] & 0x80 != 0)
        }
        _ => (0, 0, false),
    }
}

/// Go `av1BitDepth`: reads high_bitdepth and twelve_bit from byte 2 of the AV1CodecConfigurationRecord:
/// 10 bits takes the first, 12 both.
fn av1_bit_depth(record: &[u8]) -> i64 {
    match record {
        [0x81, _, b2, ..] => {
            if b2 & 0x40 == 0 {
                8
            } else if b2 & 0x20 != 0 {
                12
            } else {
                10
            }
        }
        _ => 0,
    }
}

/// Go `hdrFromTransfer`: names the HDR a transfer characteristic implies, in the ITU-T H.273 numbering that
/// MP4's nclx colour box and Matroska's Colour element both use: 16 is SMPTE ST 2084, the PQ that releases
/// call HDR10, and 18 is HLG. "" for anything else, including a container that doesn't say — an HEVC file
/// whose colours are only in its SPS reads as nothing here, and den-remux's probe reads the SPS when it
/// opens one.
pub(crate) fn hdr_from_transfer(tc: i64) -> &'static str {
    match tc {
        16 => "HDR10",
        18 => "HLG",
        _ => "",
    }
}

/// Go `roundRate`: keeps three decimals, so 24000/1001 reads 23.976 rather than a float's tail.
pub(crate) fn round_rate(r: f64) -> f64 {
    (r * 1000.0).round() / 1000.0
}

/// Go `avcBitDepth`: goes by AVCProfileIndication, byte 1 of the AVCDecoderConfigurationRecord (ISO/IEC
/// 14496-15). High 10 (110) answers 10 even for 8-bit content, because a decoder without High 10 refuses
/// the profile whatever the samples are. Baseline, Main, Extended and High (66, 77, 88, 100) are 8-bit by
/// definition. The 4:2:2 and 4:4:4 profiles allow either depth and stay unknown: reading theirs means
/// parsing the SPS, for profiles no release uses.
pub(crate) fn avc_bit_depth(record: &[u8]) -> i64 {
    match record.get(1) {
        Some(110) => 10,
        Some(66 | 77 | 88 | 100) => 8,
        _ => 0,
    }
}

/// Go `hevcBitDepth`: reads bitDepthLumaMinus8, the low 3 bits of byte 17 of the HEVCDecoderConfigurationRecord
/// (ISO/IEC 14496-15). The 17 bytes before it are version (1), profile (1), compatibility flags (4),
/// constraint flags (6), level (1), min_spatial_segmentation (2), parallelismType (1) and chromaFormat (1).
pub(crate) fn hevc_bit_depth(record: &[u8]) -> i64 {
    if record.len() < 18 {
        return 0;
    }
    i64::from(record[17] & 0x07) + 8
}

/// Go `findChild`: the body of the first child with this id, and the offset just past it.
fn find_child(buf: &[u8], want: u32) -> Option<(&[u8], usize)> {
    let mut pos = 0usize;
    while pos < buf.len() {
        let (id, id_len) = read_id(&buf[pos..])?;
        let (body, next) = child_payload(buf, pos + id_len)?;
        if id == want {
            return Some((body, next));
        }
        pos = next;
    }
    None
}

/// Go `childUint`: the unsigned value of a child, 0 when absent.
fn child_uint(buf: &[u8], want: u32) -> u64 {
    find_child(buf, want).map_or(0, |(body, _)| uint_from(body))
}

/// Go `channelLayout`: names what a viewer recognises. 6 channels is 5.1 and 8 is 7.1 in every release that
/// turns up; the exact speaker mask is more than the list can show.
pub(crate) fn channel_layout(n: i64) -> &'static str {
    if n >= 8 {
        "7.1"
    } else if n >= 6 {
        "5.1"
    } else if n >= 2 {
        "2.0"
    } else if n == 1 {
        "1.0"
    } else {
        ""
    }
}

/// Go `codecFromMatroskaID`: maps Matroska's CodecID strings to the families Den ranks on.
pub(crate) fn codec_from_matroska_id(id: &str) -> &'static str {
    if id.starts_with("V_MPEG4/ISO/AVC") {
        "h264"
    } else if id.starts_with("V_MPEGH/ISO/HEVC") {
        "hevc"
    } else if id.starts_with("V_AV1") {
        "av1"
    } else if id.starts_with("V_VP9") {
        "vp9"
    } else if id.starts_with("V_MPEG4") {
        // ASP / DivX / XviD in Matroska
        "mpeg4"
    } else {
        ""
    }
}

/// Go `channelCount`: channelLayout's inverse, for comparing which layout is richer.
pub(crate) fn channel_count(layout: &str) -> i64 {
    match layout {
        "7.1" => 8,
        "5.1" => 6,
        "2.0" => 2,
        "1.0" => 1,
        _ => 0,
    }
}

/// Matroska EBML ids, as in Go's `probe.go` const block (kept with the parser that reads them). Ids are
/// `u32`, so they can be fed to the EBML builders in the tests as well as the parser.
pub(crate) const ID_SEGMENT: u32 = 0x1853_8067;
pub(crate) const ID_TRACKS: u32 = 0x1654_AE6B;
pub(crate) const ID_TRACK_ENTRY: u32 = 0xAE;
pub(crate) const ID_CODEC_ID: u32 = 0x86;
pub(crate) const ID_VIDEO: u32 = 0xE0;
pub(crate) const TRACK_TYPE_VIDEO: u8 = 1;
pub(crate) const ID_LANGUAGE: u32 = 0x22_B59C;
pub(crate) const ID_CODEC_PRIVATE: u32 = 0x63A2;
pub(crate) const ID_BLOCK_ADDITION_MAPPING: u32 = 0x41E4;
pub(crate) const ID_BLOCK_ADD_ID_TYPE: u32 = 0x41E7;
pub(crate) const ID_BLOCK_ADD_ID_EXTRA_DATA: u32 = 0x41ED;
pub(crate) const ID_DEFAULT_DURATION: u32 = 0x23_E383;
pub(crate) const ID_PIXEL_WIDTH: u32 = 0xB0;
pub(crate) const ID_PIXEL_HEIGHT: u32 = 0xBA;
pub(crate) const ID_COLOUR: u32 = 0x55B0;
pub(crate) const ID_TRANSFER_CHARACTERISTICS: u32 = 0x55BA;
/// Go `idLanguageBCP`: the BCP-47 language element.
const ID_LANGUAGE_BCP: u32 = 0x22_B59D;
/// Go `idAudio`.
const ID_AUDIO: u32 = 0xE1;
/// Go `idChannels`.
const ID_CHANNELS: u32 = 0x9F;
