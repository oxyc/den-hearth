//! Strict, allocation-bounded parsing for indexed Matroska text subtitles.
//!
//! This module does no I/O. Callers fetch the exact elements named by a Segment's SeekHead and feed their
//! payloads here. A track is usable only when every cue carries an exact block position and duration and the
//! Matroska statistics tag proves that the cue list names every frame.

use std::collections::HashMap;

use crate::{ebml, mkv};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SegmentIndex {
    pub payload_at: u64,
    pub elements: HashMap<u32, u64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TextTrack {
    pub ordinal: usize,
    pub number: u64,
    pub uid: u64,
    pub language: Option<String>,
    pub name: Option<String>,
    pub codec: TextCodec,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TextCodec {
    Plain,
    Ass,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Cue {
    pub timestamp: u64,
    pub duration: Option<u64>,
    pub cluster: u64,
    pub relative: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CueTrack {
    pub number: u64,
    pub cues: Vec<Cue>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SubtitleBlock<'a> {
    pub track: u64,
    pub relative_timestamp: i16,
    pub duration: Option<u64>,
    pub text: &'a str,
}

fn text(body: &[u8]) -> Option<String> {
    let value = std::str::from_utf8(body).ok()?.trim_matches('\0').trim();
    (!value.is_empty()).then(|| value.to_owned())
}

fn element_extent(buf: &[u8]) -> Option<(u32, usize, usize)> {
    let (id, size, header) = ebml::header(buf)?;
    let end = header.checked_add(usize::try_from(size?).ok()?)?;
    (end <= buf.len()).then_some((id, header, end))
}

/// Finds the Segment and its first complete SeekHead in a bounded file prefix.
pub fn segment_index(prefix: &[u8]) -> Option<SegmentIndex> {
    let mut at = 0usize;
    while at < prefix.len() {
        let (id, size, header) = ebml::header(prefix.get(at..)?)?;
        if id == mkv::SEGMENT {
            let payload_at = u64::try_from(at.checked_add(header)?).ok()?;
            let available = prefix.get(at + header..)?;
            for (child, body) in ebml::children(available) {
                if child == mkv::SEEK_HEAD {
                    return seek_head(body, payload_at);
                }
            }
            return None;
        }
        let size = usize::try_from(size?).ok()?;
        at = at.checked_add(header)?.checked_add(size)?;
    }
    None
}

/// Parses one SeekHead. Positions are made absolute against the Segment payload.
pub fn seek_head(body: &[u8], segment_payload_at: u64) -> Option<SegmentIndex> {
    let mut elements = HashMap::new();
    let mut children = ebml::children(body);
    for (id, seek) in children.by_ref() {
        if id != mkv::SEEK {
            continue;
        }
        let raw_id = ebml::child(seek, mkv::SEEK_ID)?;
        if raw_id.is_empty() || raw_id.len() > 4 {
            return None;
        }
        let element_id = raw_id.iter().fold(0u32, |value, byte| value << 8 | u32::from(*byte));
        let position = ebml::child(seek, mkv::SEEK_POSITION).map(ebml::uint)?;
        elements.entry(element_id).or_insert(segment_payload_at.checked_add(position)?);
    }
    children.reached_end().then_some(SegmentIndex { payload_at: segment_payload_at, elements })
}

/// Returns supported, uncompressed UTF-8, ASCII, WebVTT, and ASS/SSA text tracks only.
pub fn text_tracks(body: &[u8]) -> Option<Vec<TextTrack>> {
    let mut tracks = Vec::new();
    let mut subtitle_ordinal = 0usize;
    let mut children = ebml::children(body);
    for (id, entry) in children.by_ref() {
        if id != mkv::TRACK_ENTRY {
            continue;
        }
        if ebml::child_uint(entry, mkv::TRACK_TYPE) != mkv::TRACK_TYPE_SUBTITLE.into() {
            continue;
        }
        let ordinal = subtitle_ordinal;
        subtitle_ordinal = subtitle_ordinal.checked_add(1)?;
        if ebml::child(entry, mkv::CONTENT_ENCODINGS).is_some() {
            continue;
        }
        let codec = ebml::child(entry, mkv::CODEC_ID).and_then(text);
        let codec = match codec.as_deref() {
            Some("S_TEXT/UTF8" | "S_TEXT/ASCII" | "S_TEXT/WEBVTT") => TextCodec::Plain,
            Some("S_TEXT/ASS" | "S_ASS" | "S_TEXT/SSA" | "S_SSA") => TextCodec::Ass,
            _ => continue,
        };
        let number = ebml::child(entry, mkv::TRACK_NUMBER).map(ebml::uint)?;
        let uid = ebml::child(entry, mkv::TRACK_UID).map(ebml::uint)?;
        if number == 0 || uid == 0 || tracks.iter().any(|track: &TextTrack| track.number == number) {
            return None;
        }
        tracks.push(TextTrack {
            ordinal,
            number,
            uid,
            language: ebml::child(entry, mkv::LANGUAGE_BCP47)
                .or_else(|| ebml::child(entry, mkv::LANGUAGE))
                .and_then(text),
            name: ebml::child(entry, mkv::TRACK_NAME).and_then(text),
            codec,
        });
    }
    children.reached_end().then_some(tracks)
}

/// Reads the timestamp scale in nanoseconds. Matroska defaults to 1 ms when it is absent.
pub fn timestamp_scale(info: &[u8]) -> Option<u64> {
    let scale = ebml::child(info, mkv::TIMESTAMP_SCALE).map(ebml::uint).unwrap_or(1_000_000);
    (scale > 0).then_some(scale)
}

/// Reads CuePoints, grouping the positions for each track while rejecting ambiguous duplicates.
pub fn cue_tracks(body: &[u8], max_cues: usize) -> Option<Vec<CueTrack>> {
    let mut tracks: Vec<CueTrack> = Vec::new();
    let mut total = 0usize;
    let mut children = ebml::children(body);
    for (id, point) in children.by_ref() {
        if id != mkv::CUE_POINT {
            continue;
        }
        let timestamp = ebml::child(point, mkv::CUE_TIME).map(ebml::uint)?;
        let mut positions = ebml::children(point);
        for (position_id, position) in positions.by_ref() {
            if position_id != mkv::CUE_TRACK_POSITIONS {
                continue;
            }
            let number = ebml::child(position, mkv::CUE_TRACK).map(ebml::uint)?;
            let cue = Cue {
                timestamp,
                duration: ebml::child(position, mkv::CUE_DURATION).map(ebml::uint),
                cluster: ebml::child(position, mkv::CUE_CLUSTER_POSITION).map(ebml::uint)?,
                relative: ebml::child(position, mkv::CUE_RELATIVE_POSITION).map(ebml::uint)?,
            };
            if number == 0 || cue.duration == Some(0) {
                return None;
            }
            total = total.checked_add(1)?;
            if total > max_cues {
                return None;
            }
            let index = tracks.iter().position(|track| track.number == number).unwrap_or_else(|| {
                tracks.push(CueTrack { number, cues: Vec::new() });
                tracks.len() - 1
            });
            if tracks[index].cues.last().is_some_and(|previous| previous.timestamp > cue.timestamp) {
                return None;
            }
            tracks[index].cues.push(cue);
        }
        if !positions.reached_end() {
            return None;
        }
    }
    children.reached_end().then_some(tracks)
}

/// Reads mkvmerge's per-track NUMBER_OF_FRAMES statistics tags.
pub fn frame_counts(body: &[u8]) -> Option<HashMap<u64, usize>> {
    let mut counts = HashMap::new();
    let mut tags = ebml::children(body);
    for (id, tag) in tags.by_ref() {
        if id != mkv::TAG {
            continue;
        }
        let targets = ebml::child(tag, mkv::TARGETS)?;
        // An empty Targets (or one naming an edition/chapter/attachment) is a valid global/non-track tag.
        // It has no bearing on per-track statistics and must not make all subtitle tracks ineligible.
        let Some(uid) = ebml::child(targets, mkv::TARGET_TRACK_UID).map(ebml::uint) else {
            continue;
        };
        if uid == 0 {
            continue;
        }
        let mut children = ebml::children(tag);
        for (child_id, simple) in children.by_ref() {
            if child_id != mkv::SIMPLE_TAG {
                continue;
            }
            let name = ebml::child(simple, mkv::TAG_NAME).and_then(text);
            if name.as_deref().is_some_and(|name| name.eq_ignore_ascii_case("NUMBER_OF_FRAMES")) {
                let value = ebml::child(simple, mkv::TAG_STRING).and_then(text)?.parse().ok()?;
                if counts.insert(uid, value).is_some() {
                    return None;
                }
            }
        }
        if !children.reached_end() {
            return None;
        }
    }
    tags.reached_end().then_some(counts)
}

fn block_payload(buf: &[u8]) -> Option<(&[u8], Option<u64>)> {
    let (id, header, end) = element_extent(buf)?;
    match id {
        mkv::SIMPLE_BLOCK => Some((buf.get(header..end)?, None)),
        mkv::BLOCK_GROUP => {
            let group = buf.get(header..end)?;
            Some((ebml::child(group, mkv::BLOCK)?, ebml::child(group, mkv::BLOCK_DURATION).map(ebml::uint)))
        }
        _ => None,
    }
}

/// Decodes one uncompressed, unlaced subtitle block at a CueRelativePosition.
pub fn subtitle_block(buf: &[u8]) -> Option<SubtitleBlock<'_>> {
    let (block, duration) = block_payload(buf)?;
    let first = *block.first()?;
    let width = first.leading_zeros() as usize + 1;
    if width > 8 || block.len() < width + 3 {
        return None;
    }
    let mask = (0xFFu16 >> width) as u8;
    let mut track = u64::from(first & mask);
    for byte in &block[1..width] {
        track = track.checked_shl(8)? | u64::from(*byte);
    }
    let timestamp = i16::from_be_bytes([block[width], block[width + 1]]);
    let flags = block[width + 2];
    if track == 0 || flags & 0x06 != 0 {
        return None;
    }
    let text = std::str::from_utf8(&block[width + 3..]).ok()?;
    Some(SubtitleBlock { track, relative_timestamp: timestamp, duration, text })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn e(id: &[u8], body: &[u8]) -> Vec<u8> {
        assert!(body.len() < 127);
        [id, &[0x80 | body.len() as u8], body].concat()
    }

    #[test]
    fn a_simple_utf8_block_is_decoded_and_lacing_is_refused() {
        let mut block = e(&[0xA3], &[0x81, 0, 2, 0, b'H', b'i']);
        assert_eq!(
            subtitle_block(&block),
            Some(SubtitleBlock { track: 1, relative_timestamp: 2, duration: None, text: "Hi" })
        );
        block[5] = 0x02;
        assert_eq!(subtitle_block(&block), None);
    }

    #[test]
    fn a_block_group_carries_the_duration_when_the_cue_does_not() {
        let block = e(&[0xA1], &[0x81, 0, 2, 0, b'H', b'i']);
        let duration = e(&[0x9B], &[42]);
        let group = e(&[0xA0], &[block, duration].concat());
        assert_eq!(
            subtitle_block(&group),
            Some(SubtitleBlock { track: 1, relative_timestamp: 2, duration: Some(42), text: "Hi" })
        );
    }

    #[test]
    fn global_tags_do_not_hide_valid_track_statistics() {
        let global = e(&[0x73, 0x73], &e(&[0x63, 0xC0], &[]));
        let targets = e(&[0x63, 0xC0], &e(&[0x63, 0xC5], &[9]));
        let simple =
            e(&[0x67, 0xC8], &[e(&[0x45, 0xA3], b"NUMBER_OF_FRAMES"), e(&[0x44, 0x87], b"2")].concat());
        let track = e(&[0x73, 0x73], &[targets, simple].concat());
        assert_eq!(frame_counts(&[global, track].concat()), Some(HashMap::from([(9, 2)])));
    }

    #[test]
    fn supported_tracks_reject_content_encoding() {
        let entry = |codec: &[u8], encoded: bool| {
            let mut body =
                [e(&[0xD7], &[1]), e(&[0x73, 0xC5], &[9]), e(&[0x83], &[17]), e(&[0x86], codec)].concat();
            if encoded {
                body.extend(e(&[0x6D, 0x80], &[]));
            }
            e(&[0xAE], &body)
        };
        for codec in
            [b"S_TEXT/UTF8".as_slice(), b"S_TEXT/ASCII", b"S_TEXT/WEBVTT", b"S_TEXT/ASS", b"S_TEXT/SSA"]
        {
            assert_eq!(text_tracks(&entry(codec, false)).unwrap().len(), 1);
        }
        assert!(text_tracks(&entry(b"S_TEXT/UTF8", true)).unwrap().is_empty());
    }
}
