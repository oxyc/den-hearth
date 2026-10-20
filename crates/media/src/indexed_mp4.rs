//! Strict sample-table parsing for bounded extraction of MP4 timed-text tracks.

use crate::mp4;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TextCodec {
    Tx3g,
    Wvtt,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Sample {
    pub offset: u64,
    pub size: u32,
    pub start: u64,
    pub duration: u32,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TextTrack {
    pub ordinal: usize,
    pub language: Option<String>,
    pub timescale: u32,
    pub codec: TextCodec,
    pub samples: Vec<Sample>,
}

#[derive(Clone, Copy)]
struct Edit {
    empty: u64,
    media_start: u64,
    media_end: u64,
}

fn entries(body: &[u8], width: usize) -> Option<impl Iterator<Item = &[u8]>> {
    let count = usize::try_from(mp4::u32_at(body, 4)?).ok()?;
    let data = body.get(8..)?;
    (data.len() == count.checked_mul(width)?).then(|| data.chunks_exact(width))
}

fn sample_sizes(stsz: &[u8], max_samples: usize) -> Option<Vec<u32>> {
    let fixed = mp4::u32_at(stsz, 4)?;
    let count = usize::try_from(mp4::u32_at(stsz, 8)?).ok()?;
    if count == 0 || count > max_samples {
        return None;
    }
    if fixed != 0 {
        return (stsz.len() == 12).then(|| vec![fixed; count]);
    }
    let data = stsz.get(12..)?;
    if data.len() != count.checked_mul(4)? {
        return None;
    }
    data.chunks(4).map(|entry| mp4::u32_at(entry, 0)).collect()
}

fn timings(stts: &[u8], ctts: Option<&[u8]>, count: usize) -> Option<Vec<(u64, u32)>> {
    let mut decode = Vec::with_capacity(count);
    let mut at = 0u64;
    for entry in entries(stts, 8)? {
        let samples = usize::try_from(mp4::u32_at(entry, 0)?).ok()?;
        let duration = mp4::u32_at(entry, 4)?;
        if duration == 0 || decode.len().checked_add(samples)? > count {
            return None;
        }
        for _ in 0..samples {
            decode.push((at, duration));
            at = at.checked_add(u64::from(duration))?;
        }
    }
    if decode.len() != count {
        return None;
    }
    let Some(ctts) = ctts else { return Some(decode) };
    let version = *ctts.first()?;
    if version > 1 {
        return None;
    }
    let mut offsets = Vec::with_capacity(count);
    for entry in entries(ctts, 8)? {
        let samples = usize::try_from(mp4::u32_at(entry, 0)?).ok()?;
        let raw = mp4::u32_at(entry, 4)?;
        let offset = if version == 0 { i64::from(raw) } else { i64::from(raw as i32) };
        if offsets.len().checked_add(samples)? > count {
            return None;
        }
        offsets.extend(std::iter::repeat_n(offset, samples));
    }
    if offsets.len() != count {
        return None;
    }
    decode
        .into_iter()
        .zip(offsets)
        .map(|((time, duration), offset)| {
            let time = i128::from(time).checked_add(i128::from(offset))?;
            Some((u64::try_from(time).ok()?, duration))
        })
        .collect()
}

fn chunk_offsets(stbl: &[u8]) -> Option<Vec<u64>> {
    if let Some(stco) = mp4::child(stbl, b"stco") {
        return entries(stco, 4)?.map(|entry| mp4::u32_at(entry, 0).map(u64::from)).collect();
    }
    let co64 = mp4::child(stbl, b"co64")?;
    entries(co64, 8)?.map(|entry| mp4::u64_at(entry, 0)).collect()
}

fn sample_offsets(stbl: &[u8], sizes: &[u32]) -> Option<Vec<u64>> {
    let chunks = chunk_offsets(stbl)?;
    let stsc = mp4::child(stbl, b"stsc")?;
    let mapping = entries(stsc, 12)?
        .map(|entry| Some((mp4::u32_at(entry, 0)?, mp4::u32_at(entry, 4)?, mp4::u32_at(entry, 8)?)))
        .collect::<Option<Vec<_>>>()?;
    if mapping.is_empty() || mapping[0].0 != 1 || mapping.windows(2).any(|pair| pair[0].0 >= pair[1].0) {
        return None;
    }
    let mut offsets = Vec::with_capacity(sizes.len());
    let mut sample = 0usize;
    for (chunk_index, chunk_start) in chunks.into_iter().enumerate() {
        let number = u32::try_from(chunk_index).ok()?.checked_add(1)?;
        let (_, per_chunk, description) = mapping.iter().rev().find(|entry| entry.0 <= number).copied()?;
        if description != 1 || per_chunk == 0 {
            return None;
        }
        let mut at = chunk_start;
        for _ in 0..per_chunk {
            let size = *sizes.get(sample)?;
            offsets.push(at);
            at = at.checked_add(u64::from(size))?;
            sample += 1;
        }
    }
    (sample == sizes.len()).then_some(offsets)
}

fn scaled(value: u64, from: u32, to: u32) -> Option<u64> {
    let numerator = value.checked_mul(u64::from(to))?;
    (numerator % u64::from(from) == 0).then_some(numerator / u64::from(from))
}

/// Accepts the common exact edit layout: an optional empty lead followed by one rate-1 media edit. Anything
/// spliced, rate-adjusted, or requiring fractional tick rounding falls back rather than shifting dialogue.
fn edit(trak: &[u8], movie_scale: u32, media_scale: u32) -> Option<Option<Edit>> {
    let Some(edts) = mp4::child(trak, b"edts") else { return Some(None) };
    let elst = mp4::child(edts, b"elst")?;
    let version = *elst.first()?;
    let width = match version {
        0 => 12,
        1 => 20,
        _ => return None,
    };
    if elst.get(1..4)? != [0, 0, 0] {
        return None;
    }
    let rows = entries(elst, width)?.collect::<Vec<_>>();
    if rows.is_empty() || rows.len() > 2 {
        return None;
    }
    let read = |row: &[u8]| -> Option<(u64, i64)> {
        let (duration, media_time, rate_at) = if version == 0 {
            (u64::from(mp4::u32_at(row, 0)?), i64::from(mp4::u32_at(row, 4)? as i32), 8)
        } else {
            (mp4::u64_at(row, 0)?, mp4::u64_at(row, 8)? as i64, 16)
        };
        (mp4::u16_at(row, rate_at)? == 1 && mp4::u16_at(row, rate_at + 2)? == 0)
            .then_some((duration, media_time))
    };
    let mut at = 0usize;
    let empty = if rows.len() == 2 {
        let (duration, media_time) = read(rows[0])?;
        if media_time != -1 {
            return None;
        }
        at = 1;
        scaled(duration, movie_scale, media_scale)?
    } else {
        0
    };
    let (duration, media_time) = read(rows[at])?;
    let media_start = u64::try_from(media_time).ok()?;
    let media_duration = scaled(duration, movie_scale, media_scale)?;
    Some(Some(Edit { empty, media_start, media_end: media_start.checked_add(media_duration)? }))
}

fn track(trak: &[u8], ordinal: usize, max_samples: usize, movie_scale: u32) -> Option<TextTrack> {
    let mdia = mp4::child(trak, b"mdia")?;
    let header = mp4::media_header(mp4::child(mdia, b"mdhd")?)?;
    if header.timescale == 0 {
        return None;
    }
    let stbl = mp4::path(mdia, &[b"minf", b"stbl"])?;
    let codec = match mp4::first_sample_entry(stbl)?.0 {
        kind if kind == *b"tx3g" => TextCodec::Tx3g,
        kind if kind == *b"wvtt" => TextCodec::Wvtt,
        _ => return None,
    };
    let sizes = sample_sizes(mp4::child(stbl, b"stsz")?, max_samples)?;
    let offsets = sample_offsets(stbl, &sizes)?;
    let times = timings(mp4::child(stbl, b"stts")?, mp4::child(stbl, b"ctts"), sizes.len())?;
    let edit = edit(trak, movie_scale, header.timescale)?;
    let samples = offsets
        .into_iter()
        .zip(sizes)
        .zip(times)
        .filter_map(|((offset, size), (start, duration))| {
            let Some(edit) = edit else { return Some(Sample { offset, size, start, duration }) };
            let end = start.checked_add(u64::from(duration))?;
            let shown_start = start.max(edit.media_start);
            let shown_end = end.min(edit.media_end);
            if shown_start >= shown_end {
                return None;
            }
            Some(Sample {
                offset,
                size,
                start: shown_start.checked_sub(edit.media_start)?.checked_add(edit.empty)?,
                duration: u32::try_from(shown_end.checked_sub(shown_start)?).ok()?,
            })
        })
        .collect();
    Some(TextTrack {
        ordinal,
        language: header.language.map(|value| String::from_utf8_lossy(&value).into_owned()),
        timescale: header.timescale,
        codec,
        samples,
    })
}

/// Parses complete non-fragmented MP4 subtitle sample tables from a `moov` payload.
pub fn text_tracks(moov: &[u8], max_samples: usize) -> Option<Vec<TextTrack>> {
    let mvhd = mp4::child(moov, b"mvhd")?;
    let movie_scale = if mvhd.first() == Some(&1) { mp4::u32_at(mvhd, 20)? } else { mp4::u32_at(mvhd, 12)? };
    if movie_scale == 0 {
        return None;
    }
    let mut subtitle_ordinal = 0usize;
    let mut output = Vec::new();
    let mut walked = 0usize;
    for (kind, trak) in mp4::boxes(moov) {
        walked = walked.checked_add(8)?.checked_add(trak.len())?;
        if kind != *b"trak" {
            continue;
        }
        let mdia = mp4::child(trak, b"mdia")?;
        if !matches!(mp4::handler_type(mdia), Some(kind) if matches!(&kind, b"sbtl" | b"subt" | b"text")) {
            continue;
        }
        let ordinal = subtitle_ordinal;
        subtitle_ordinal = subtitle_ordinal.checked_add(1)?;
        if let Some(track) = track(trak, ordinal, max_samples, movie_scale) {
            output.push(track);
        }
    }
    (walked == moov.len()).then_some(output)
}

/// Decodes the payload of one UTF-8 tx3g or WebVTT sample. Styling boxes are deliberately ignored.
pub fn sample_text(codec: TextCodec, sample: &[u8]) -> Option<String> {
    match codec {
        TextCodec::Tx3g => {
            let length = usize::from(mp4::u16_at(sample, 0)?);
            let bytes = sample.get(2..2usize.checked_add(length)?)?;
            Some(std::str::from_utf8(bytes).ok()?.to_owned())
        }
        TextCodec::Wvtt => {
            let mut text = String::new();
            let mut walked = 0usize;
            for (kind, cue) in mp4::boxes(sample) {
                walked = walked.checked_add(8)?.checked_add(cue.len())?;
                if kind != *b"vttc" {
                    continue;
                }
                if let Some(payload) = mp4::child(cue, b"payl") {
                    if !text.is_empty() {
                        text.push('\n');
                    }
                    text.push_str(std::str::from_utf8(payload).ok()?);
                }
            }
            (walked == sample.len()).then_some(text)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn boxed(kind: &[u8; 4], body: &[u8]) -> Vec<u8> {
        let mut output = (body.len() as u32 + 8).to_be_bytes().to_vec();
        output.extend(kind);
        output.extend(body);
        output
    }

    #[test]
    fn text_samples_ignore_style_payloads() {
        let mut tx3g = 5u16.to_be_bytes().to_vec();
        tx3g.extend(b"Hello");
        tx3g.extend(boxed(b"styl", &[0, 0]));
        assert_eq!(sample_text(TextCodec::Tx3g, &tx3g).as_deref(), Some("Hello"));
        let wvtt = boxed(b"vttc", &boxed(b"payl", b"World"));
        assert_eq!(sample_text(TextCodec::Wvtt, &wvtt).as_deref(), Some("World"));
    }

    #[test]
    fn complete_sample_tables_map_every_timed_text_sample() {
        let mut mdhd = vec![0u8; 24];
        mdhd[12..16].copy_from_slice(&1000u32.to_be_bytes());
        let packed = (u16::from(b'e' - 0x60) << 10) | (u16::from(b'n' - 0x60) << 5) | u16::from(b'g' - 0x60);
        mdhd[20..22].copy_from_slice(&packed.to_be_bytes());
        let mut hdlr = vec![0u8; 12];
        hdlr[8..12].copy_from_slice(b"sbtl");
        let full = |count: u32, rows: &[u8]| [&[0u8; 4][..], &count.to_be_bytes(), rows].concat();
        let stsd = full(1, &boxed(b"tx3g", &[0; 8]));
        let stts = full(1, &[2u32.to_be_bytes(), 1000u32.to_be_bytes()].concat());
        let stsc = full(1, &[1u32.to_be_bytes(), 2u32.to_be_bytes(), 1u32.to_be_bytes()].concat());
        let stsz = [
            &[0u8; 4][..],
            &0u32.to_be_bytes(),
            &2u32.to_be_bytes(),
            &5u32.to_be_bytes(),
            &6u32.to_be_bytes(),
        ]
        .concat();
        let stco = full(1, &100u32.to_be_bytes());
        let stbl = [
            boxed(b"stsd", &stsd),
            boxed(b"stts", &stts),
            boxed(b"stsc", &stsc),
            boxed(b"stsz", &stsz),
            boxed(b"stco", &stco),
        ]
        .concat();
        let minf = boxed(b"minf", &boxed(b"stbl", &stbl));
        let mdia = boxed(b"mdia", &[boxed(b"mdhd", &mdhd), boxed(b"hdlr", &hdlr), minf].concat());
        let mut mvhd = vec![0u8; 20];
        mvhd[12..16].copy_from_slice(&1000u32.to_be_bytes());
        let moov = [boxed(b"mvhd", &mvhd), boxed(b"trak", &mdia)].concat();
        let tracks = text_tracks(&moov, 10).unwrap();
        assert_eq!(tracks.len(), 1);
        assert_eq!(tracks[0].language.as_deref(), Some("eng"));
        assert_eq!(
            tracks[0].samples,
            vec![
                Sample { offset: 100, size: 5, start: 0, duration: 1000 },
                Sample { offset: 105, size: 6, start: 1000, duration: 1000 },
            ]
        );
    }

    #[test]
    fn a_common_edit_list_shifts_media_time_after_an_empty_lead() {
        let row = |duration: u32, media_time: i32| {
            [
                duration.to_be_bytes().as_slice(),
                media_time.to_be_bytes().as_slice(),
                1u16.to_be_bytes().as_slice(),
                0u16.to_be_bytes().as_slice(),
            ]
            .concat()
        };
        let elst = [&[0u8; 4][..], &2u32.to_be_bytes(), &row(500, -1), &row(2_000, 1_000)].concat();
        let trak = boxed(b"edts", &boxed(b"elst", &elst));
        let parsed = edit(&trak, 1_000, 1_000).unwrap().unwrap();
        assert_eq!(parsed.empty, 500);
        assert_eq!(parsed.media_start, 1_000);
        assert_eq!(parsed.media_end, 3_000);
    }
}
