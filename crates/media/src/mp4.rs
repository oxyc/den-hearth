//! ISO base media boxes (MP4, MOV, M4V): a 32-bit size, a four-character type, then the payload, which for a
//! container box is more boxes.
//!
//! The walk is the part that reads sizes out of untrusted bytes, so a size that is wrong ends it: one that
//! overflows, runs past the buffer or is smaller than its own header never indexes outside the slice.

/// Whether truncated input is an error or a useful, explicitly marked prefix. Structurally impossible sizes are an
/// error in both modes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WalkMode {
    Strict,
    Prefix,
}

/// Absolute byte boundaries of one box. `body` is the first payload byte; all offsets are relative to the source
/// containing `buf`, not to `buf` itself.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BoxSpan {
    pub kind: [u8; 4],
    pub start: u64,
    pub body: u64,
    pub end: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BoxWalkErrorKind {
    TruncatedHeader,
    TruncatedBox,
    SizeSmallerThanHeader,
    OffsetOverflow,
    InvalidExtendedSize,
}

/// Why a checked walk stopped. `offset` is absolute and `kind` is present once the type field was available.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BoxWalkError {
    pub offset: u64,
    pub kind: Option<[u8; 4]>,
    pub reason: BoxWalkErrorKind,
}

/// A complete walk, or in [`WalkMode::Prefix`] the valid boxes before `stopped`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BoxWalk {
    pub boxes: Vec<BoxSpan>,
    pub stopped: Option<BoxWalkError>,
}

/// Walks sibling boxes with checked arithmetic and absolute offsets. Strict mode rejects the first incomplete or
/// impossible box; prefix mode returns the valid prefix only for a cut header/body and carries the stop in
/// [`BoxWalk::stopped`].
pub fn walk_boxes(buf: &[u8], base_offset: u64, mode: WalkMode) -> Result<BoxWalk, BoxWalkError> {
    let mut boxes = Vec::new();
    let mut pos = 0usize;
    while pos < buf.len() {
        let absolute = base_offset.checked_add(pos as u64).ok_or(BoxWalkError {
            offset: base_offset,
            kind: None,
            reason: BoxWalkErrorKind::OffsetOverflow,
        })?;
        let rest = &buf[pos..];
        let kind = rest.get(4..8).and_then(|v| v.try_into().ok());
        let fail = |reason| BoxWalkError { offset: absolute, kind, reason };
        let Some(size32) = u32_at(rest, 0) else {
            return finish_walk(boxes, fail(BoxWalkErrorKind::TruncatedHeader), mode);
        };
        let Some(kind) = kind else {
            return finish_walk(boxes, fail(BoxWalkErrorKind::TruncatedHeader), mode);
        };
        let (size, header_len) = match size32 {
            0 => (rest.len() as u64, 8usize),
            1 => match u64_at(rest, 8) {
                Some(0) => return finish_walk(boxes, fail(BoxWalkErrorKind::InvalidExtendedSize), mode),
                Some(size) => (size, 16),
                None => return finish_walk(boxes, fail(BoxWalkErrorKind::TruncatedHeader), mode),
            },
            size => (u64::from(size), 8),
        };
        if size < header_len as u64 {
            return finish_walk(boxes, fail(BoxWalkErrorKind::SizeSmallerThanHeader), mode);
        }
        let Some(end_pos) = (pos as u64).checked_add(size).and_then(|end| usize::try_from(end).ok()) else {
            return finish_walk(boxes, fail(BoxWalkErrorKind::OffsetOverflow), mode);
        };
        if end_pos > buf.len() {
            return finish_walk(boxes, fail(BoxWalkErrorKind::TruncatedBox), mode);
        }
        let Some(body) = absolute.checked_add(header_len as u64) else {
            return finish_walk(boxes, fail(BoxWalkErrorKind::OffsetOverflow), mode);
        };
        let Some(end) = absolute.checked_add(size) else {
            return finish_walk(boxes, fail(BoxWalkErrorKind::OffsetOverflow), mode);
        };
        boxes.push(BoxSpan { kind, start: absolute, body, end });
        pos = end_pos;
    }
    Ok(BoxWalk { boxes, stopped: None })
}

fn finish_walk(boxes: Vec<BoxSpan>, error: BoxWalkError, mode: WalkMode) -> Result<BoxWalk, BoxWalkError> {
    match mode {
        WalkMode::Strict => Err(error),
        WalkMode::Prefix
            if matches!(error.reason, BoxWalkErrorKind::TruncatedHeader | BoxWalkErrorKind::TruncatedBox) =>
        {
            Ok(BoxWalk { boxes, stopped: Some(error) })
        }
        WalkMode::Prefix => Err(error),
    }
}

/// A box's size, type and header length. A 32-bit size of 0 means "to the end of the buffer" and is reported as
/// 0; a size of 1 means the real, 64-bit size follows the type.
pub fn header(buf: &[u8]) -> Option<(u64, [u8; 4], usize)> {
    let size = u64::from(u32_at(buf, 0)?);
    let typ: [u8; 4] = buf.get(4..8)?.try_into().ok()?;
    if size == 1 {
        return Some((u64_at(buf, 8)?, typ, 16));
    }
    Some((size, typ, 8))
}

/// The boxes of a buffer, as `(type, payload)`. A box that is cut off, or whose size is impossible, ends the
/// walk.
pub fn boxes(buf: &[u8]) -> impl Iterator<Item = ([u8; 4], &[u8])> {
    let mut pos = 0usize;
    std::iter::from_fn(move || {
        let rest = buf.get(pos..)?;
        let (size, typ, header_len) = header(rest)?;
        // A 32-bit size of 0 runs to the end of the buffer. A 64-bit one of 0 is no size at all.
        let size = if rest.starts_with(&[0; 4]) { rest.len() as u64 } else { size };
        let size = usize::try_from(size).ok()?;
        let end = pos.checked_add(size)?;
        if end > buf.len() || size < header_len {
            return None;
        }
        let body = &buf[pos + header_len..end];
        pos = end;
        Some((typ, body))
    })
}

/// The payload of the first child box of this type.
pub fn child<'a>(buf: &'a [u8], typ: &[u8; 4]) -> Option<&'a [u8]> {
    boxes(buf).find(|(t, _)| t == typ).map(|(_, body)| body)
}

/// The payload at the end of a path of child boxes (`minf`, `stbl`, ...).
pub fn path<'a>(buf: &'a [u8], steps: &[&[u8; 4]]) -> Option<&'a [u8]> {
    steps.iter().try_fold(buf, |cur, typ| child(cur, typ))
}

pub fn u16_at(buf: &[u8], at: usize) -> Option<u16> {
    Some(u16::from_be_bytes(buf.get(at..at.checked_add(2)?)?.try_into().ok()?))
}

pub fn u32_at(buf: &[u8], at: usize) -> Option<u32> {
    Some(u32::from_be_bytes(buf.get(at..at.checked_add(4)?)?.try_into().ok()?))
}

pub fn u64_at(buf: &[u8], at: usize) -> Option<u64> {
    Some(u64::from_be_bytes(buf.get(at..at.checked_add(8)?)?.try_into().ok()?))
}

/// The four-character handler type of a `mdia` box's `hdlr` (`vide`, `soun`, `sbtl`, ...).
pub fn handler_type(mdia: &[u8]) -> Option<[u8; 4]> {
    child(mdia, b"hdlr")?.get(8..12)?.try_into().ok()
}

/// What a `mdhd` box says of its track.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct MediaHeader {
    /// Ticks a second of the track's sample times.
    pub timescale: u32,
    /// The ISO 639-2 code, three lowercase ASCII letters, where the box holds one (`und` included: it is a code,
    /// and the callers decide what it means). `None` where it holds anything else, as the all-zero placeholder.
    pub language: Option<[u8; 3]>,
}

/// Reads a `mdhd` payload: version and flags, created and modified times (32-bit in version 0, 64-bit in version
/// 1), the timescale, the duration (the width of the times), then the language packed as three 5-bit letters, each
/// an offset from 0x60. `None` when the timescale is not there.
pub fn media_header(mdhd: &[u8]) -> Option<MediaHeader> {
    let (timescale_at, language_at) = if mdhd.first() == Some(&1) { (20, 32) } else { (12, 20) };
    let timescale = u32_at(mdhd, timescale_at)?;
    let packed = u16_at(mdhd, language_at).unwrap_or(0);
    // Each letter is 0x60 + 0..=31, so only the 26 that land on a-z make a code.
    let letters = [(packed >> 10) & 31, (packed >> 5) & 31, packed & 31].map(|c| 0x60 + c as u8);
    let language = letters.iter().all(u8::is_ascii_lowercase).then_some(letters);
    Some(MediaHeader { timescale, language })
}

/// The type and payload of the first sample entry in a sample table's `stsd` (after its version, flags and entry
/// count).
pub fn first_sample_entry(stbl: &[u8]) -> Option<([u8; 4], &[u8])> {
    boxes(child(stbl, b"stsd")?.get(8..)?).next()
}

/// Where a video sample entry's own boxes (`avcC`, `colr`, `dvcC`, ...) start: after the 8 bytes of SampleEntry
/// and the 70 of VisualSampleEntry. Width and height are the two 16-bit values at 24 and 26.
pub const VISUAL_ENTRY_HEADER: usize = 78;

/// The channel count of an audio sample entry: 8 bytes of SampleEntry, 8 reserved, then `channelcount`.
pub fn audio_channels(entry: &[u8]) -> Option<u16> {
    u16_at(entry, 16)
}

/// What an `nclx` colour box says of the colours, as H.273 code points.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Nclx {
    pub primaries: u16,
    pub transfer: u16,
    pub matrix: u16,
    /// The full-range flag, the top bit of the byte after the matrix; `None` where the box ends before it.
    pub full_range: Option<bool>,
}

/// Reads a `colr` payload of type `nclx`: the type, then primaries, transfer characteristics and matrix as 16-bit
/// values and the full-range flag. A field the box is too short for reads as 0 (H.273's "not said"). `None` for
/// any other colour type.
pub fn nclx(colr: &[u8]) -> Option<Nclx> {
    if colr.get(..4)? != b"nclx" {
        return None;
    }
    let code = |at| u16_at(colr, at).unwrap_or(0);
    Some(Nclx {
        primaries: code(4),
        transfer: code(6),
        matrix: code(8),
        full_range: colr.get(10).map(|b| b & 0x80 != 0),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn boxed(typ: &[u8; 4], body: &[u8]) -> Vec<u8> {
        let mut b = (8 + body.len() as u32).to_be_bytes().to_vec();
        b.extend_from_slice(typ);
        b.extend_from_slice(body);
        b
    }

    #[test]
    fn a_64_bit_box_size_is_read() {
        let mut b = vec![0, 0, 0, 1];
        b.extend_from_slice(b"mdat");
        b.extend_from_slice(&(5u64 << 32).to_be_bytes());
        assert_eq!(header(&b), Some((5u64 << 32, *b"mdat", 16)));
        assert_eq!(header(&b[..6]), None);

        let mut wide = vec![0, 0, 0, 1];
        wide.extend_from_slice(b"wide");
        wide.extend_from_slice(&20u64.to_be_bytes());
        wide.extend_from_slice(&[7; 4]);
        assert_eq!(boxes(&wide).collect::<Vec<_>>(), vec![(*b"wide", &[7u8; 4][..])]);
    }

    #[test]
    fn a_size_of_zero_runs_to_the_end_but_only_in_the_32_bit_field() {
        let mut last = vec![0, 0, 0, 0];
        last.extend_from_slice(b"last");
        last.extend_from_slice(&[1, 2, 3, 4]);
        assert_eq!(boxes(&last).collect::<Vec<_>>(), vec![(*b"last", &[1u8, 2, 3, 4][..])]);

        let mut wide_zero = vec![0, 0, 0, 1];
        wide_zero.extend_from_slice(b"wide");
        wide_zero.extend_from_slice(&0u64.to_be_bytes());
        wide_zero.extend_from_slice(&[1, 2, 3, 4]);
        assert_eq!(boxes(&wide_zero).count(), 0, "a 64-bit size of 0 is no size");
    }

    #[test]
    fn truncated_and_impossible_sizes_end_the_walk() {
        let bad: [Vec<u8>; 4] = [
            // size past the end
            vec![0, 0, 0, 0xFF, b'b', b'o', b'x', b'x', 1, 2],
            // size smaller than its own header
            vec![0, 0, 0, 4, b'b', b'o', b'x', b'x'],
            // 64-bit flagged, no room for the width
            vec![0, 0, 0, 1, b'b', b'o', b'x', b'x'],
            // 64-bit size that doesn't fit a usize's worth of buffer
            [&[0, 0, 0, 1][..], b"boxx", &u64::MAX.to_be_bytes()].concat(),
        ];
        for (i, buf) in bad.iter().enumerate() {
            assert_eq!(boxes(buf).count(), 0, "case {i}: walked into a bad size");
        }
        // The boxes before a bad one are still there.
        let mut two = boxed(b"free", &[1]);
        two.extend_from_slice(&[0, 0, 0, 0xFF, b'b', b'o', b'x', b'x']);
        assert_eq!(boxes(&two).count(), 1);
    }

    #[test]
    fn checked_walk_reports_absolute_spans_and_the_partial_stop() {
        let mut data = boxed(b"free", &[1, 2]);
        data.extend_from_slice(&[0, 0, 0, 12, b'm', b'd', b'a', b't', 9]);
        let error = walk_boxes(&data, 1_000, WalkMode::Strict).unwrap_err();
        assert_eq!(
            error,
            BoxWalkError { offset: 1_010, kind: Some(*b"mdat"), reason: BoxWalkErrorKind::TruncatedBox }
        );
        let prefix = walk_boxes(&data, 1_000, WalkMode::Prefix).unwrap();
        assert_eq!(prefix.boxes, vec![BoxSpan { kind: *b"free", start: 1_000, body: 1_008, end: 1_010 }]);
        assert_eq!(prefix.stopped, Some(error));
    }

    #[test]
    fn checked_walk_distinguishes_cut_headers_and_impossible_sizes() {
        let cut = walk_boxes(&[0, 0, 0], 7, WalkMode::Strict).unwrap_err();
        assert_eq!(cut.reason, BoxWalkErrorKind::TruncatedHeader);
        assert_eq!(cut.offset, 7);

        let tiny = [0, 0, 0, 4, b'f', b'r', b'e', b'e'];
        let bad = walk_boxes(&tiny, 20, WalkMode::Strict).unwrap_err();
        assert_eq!(bad.kind, Some(*b"free"));
        assert_eq!(bad.reason, BoxWalkErrorKind::SizeSmallerThanHeader);
        assert_eq!(walk_boxes(&tiny, 20, WalkMode::Prefix), Err(bad));

        let mut wide_zero = vec![0, 0, 0, 1];
        wide_zero.extend_from_slice(b"wide");
        wide_zero.extend_from_slice(&0u64.to_be_bytes());
        assert_eq!(
            walk_boxes(&wide_zero, 0, WalkMode::Strict).unwrap_err().reason,
            BoxWalkErrorKind::InvalidExtendedSize
        );
    }

    #[test]
    fn a_path_of_children_is_followed_to_its_end() {
        let stbl = boxed(b"stbl", &boxed(b"stts", &[1]));
        let moov = boxed(b"moov", &boxed(b"minf", &stbl));
        assert_eq!(path(&moov, &[b"moov", b"minf", b"stbl", b"stts"]), Some(&[1u8][..]));
        assert_eq!(path(&moov, &[b"moov", b"trak"]), None);
        assert_eq!(child(&moov, b"moov").map(<[u8]>::len), Some(moov.len() - 8));
    }

    #[test]
    fn a_media_header_gives_its_timescale_and_language() {
        // Version 0: version+flags, created, modified, timescale 24000, duration, language "swe", quality.
        let mut v0 = vec![0u8; 24];
        v0[12..16].copy_from_slice(&24000u32.to_be_bytes());
        let packed = (u16::from(b's' - 0x60) << 10) | (u16::from(b'w' - 0x60) << 5) | u16::from(b'e' - 0x60);
        v0[20..22].copy_from_slice(&packed.to_be_bytes());
        assert_eq!(media_header(&v0), Some(MediaHeader { timescale: 24000, language: Some(*b"swe") }));
        // Version 1 widens created, modified and duration to 64 bits.
        let mut v1 = vec![0u8; 36];
        v1[0] = 1;
        v1[20..24].copy_from_slice(&90000u32.to_be_bytes());
        v1[32..34].copy_from_slice(&packed.to_be_bytes());
        assert_eq!(media_header(&v1), Some(MediaHeader { timescale: 90000, language: Some(*b"swe") }));
        // The all-zero placeholder is no language, and a header cut before the language still has a timescale.
        assert_eq!(media_header(&[0u8; 24]).map(|m| m.language), Some(None));
        assert_eq!(media_header(&v0[..16]), Some(MediaHeader { timescale: 24000, language: None }));
        assert_eq!(media_header(&v0[..15]), None);
    }

    #[test]
    fn a_colour_box_is_read_field_by_field() {
        let colr = [b'n', b'c', b'l', b'x', 0, 9, 0, 16, 0, 9, 0x80];
        assert_eq!(nclx(&colr), Some(Nclx { primaries: 9, transfer: 16, matrix: 9, full_range: Some(true) }));
        assert_eq!(nclx(&colr[..8]).map(|c| (c.transfer, c.matrix, c.full_range)), Some((16, 0, None)));
        assert_eq!(nclx(b"prof...."), None);
        assert_eq!(nclx(b"nc"), None);
    }

    #[test]
    fn a_sample_entry_is_the_first_box_after_the_stsd_count() {
        let mut stsd = vec![0, 0, 0, 0, 0, 0, 0, 1];
        stsd.extend(boxed(b"avc1", &[9, 9]));
        stsd.extend(boxed(b"hvc1", &[8]));
        let stbl = boxed(b"stsd", &stsd);
        assert_eq!(first_sample_entry(&stbl), Some((*b"avc1", &[9u8, 9][..])));
        assert_eq!(first_sample_entry(&boxed(b"stsd", &[0; 7])), None);
        assert_eq!(audio_channels(&[0; 18]), Some(0));
        assert_eq!(audio_channels(&[0; 17]), None);
    }

    #[test]
    fn no_short_buffer_makes_the_walkers_panic() {
        for a in 0..=255u8 {
            for b in [0u8, 1, 8, 0xFF] {
                for c in [0u8, 1, 0x7F, 0xFF] {
                    let buf = [0, 0, a, b, b'm', b'o', b'o', b'v', c, c, c, c, c, c, c, c, a, b];
                    for cut in 0..buf.len() {
                        let _ = boxes(&buf[..cut]).count();
                        let _ = media_header(&buf[..cut]);
                        let _ = nclx(&buf[..cut]);
                        let _ = first_sample_entry(&buf[..cut]);
                    }
                }
            }
        }
    }
}
