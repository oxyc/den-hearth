//! AVI: the main header and each stream's from `hdrl` in the head, and whether the file has an index — the legacy
//! `idx1` right after `movi`, found by walking the top-level chunk headers past it, or OpenDML's `indx` in a stream's
//! own list. One with neither is refused: ffmpeg would read the whole file to index it before it could seek, and
//! over HTTP that is the whole film.
//!
//! The index is ffmpeg's to read, not this probe's: the keyframes are a grid (`super::grid`), as an MPEG-TS's are.

use super::{AudioTrack, MediaInfo, ProbeError, Source, VideoCodec, GRID_MAX_DURATION, MAX_ELEMENT};

/// Why an AVI with no index is refused.
pub const NO_INDEX: &str = "an AVI with no index";

/// A real file has a handful of top-level chunks; the bound only stops a malformed one looping.
const MAX_CHUNKS: usize = 64;

fn u16_at(b: &[u8], at: usize) -> Option<u16> {
    Some(u16::from_le_bytes(b.get(at..at + 2)?.try_into().ok()?))
}

fn u32_at(b: &[u8], at: usize) -> Option<u32> {
    Some(u32::from_le_bytes(b.get(at..at + 4)?.try_into().ok()?))
}

/// `len` bytes at `at`, from the head where it holds them.
async fn read_at(src: &Source<'_>, head: &[u8], at: u64, len: u64) -> Result<Vec<u8>, ProbeError> {
    match head.get(at as usize..at.saturating_add(len) as usize) {
        Some(b) if at.saturating_add(len) <= head.len() as u64 => Ok(b.to_vec()),
        _ => src.read(at, len).await,
    }
}

/// The chunks in `b`, each its id and body; a LIST's body starts with its list type.
fn chunks(b: &[u8]) -> impl Iterator<Item = ([u8; 4], &[u8])> {
    let mut at = 0usize;
    std::iter::from_fn(move || {
        let id: [u8; 4] = b.get(at..at + 4)?.try_into().ok()?;
        let size = u32_at(b, at + 4)? as usize;
        let body = b.get(at + 8..(at + 8).checked_add(size)?.min(b.len()))?;
        // Chunks are word-aligned.
        at = at.checked_add(8 + size + (size & 1))?;
        Some((id, body))
    })
}

struct Stream<'a> {
    kind: [u8; 4],
    handler: [u8; 4],
    strh: &'a [u8],
    strf: &'a [u8],
    indexed: bool,
}

fn streams(hdrl: &[u8]) -> Vec<Stream<'_>> {
    chunks(hdrl)
        .filter(|(id, body)| id == b"LIST" && body.starts_with(b"strl"))
        .filter_map(|(_, strl)| {
            let mut s = Stream { kind: [0; 4], handler: [0; 4], strh: &[], strf: &[], indexed: false };
            for (id, body) in chunks(&strl[4..]) {
                match &id {
                    b"strh" => s.strh = body,
                    b"strf" => s.strf = body,
                    b"indx" => s.indexed = true,
                    _ => {}
                }
            }
            s.kind = s.strh.get(..4)?.try_into().ok()?;
            s.handler = s.strh.get(4..8)?.try_into().ok()?;
            Some(s)
        })
        .collect()
}

/// The video codec a fourcc names — the `strf`'s compression, else the `strh`'s handler — as a transcode decodes it.
fn video_codec(fourcc: &[u8; 4]) -> Result<VideoCodec, ProbeError> {
    let name = String::from_utf8_lossy(fourcc).trim_end_matches(['\0', ' ']).to_ascii_lowercase();
    match name.as_str() {
        "xvid" | "divx" | "dx50" | "fmp4" | "mp4v" | "xvix" | "dxgm" | "3iv2" => {
            Ok(VideoCodec::Other("mpeg4".into()))
        }
        "h264" | "avc1" | "x264" => Ok(VideoCodec::H264),
        "hevc" | "hev1" | "hvc1" | "x265" => Ok(VideoCodec::Hevc),
        "mpg2" | "mpeg" | "mp2v" => Ok(VideoCodec::Other("mpeg2video".into())),
        _ => Err(ProbeError::Unsupported(format!("video is {name}"))),
    }
}

/// An audio stream's codec by its `WAVEFORMATEX` format tag, in ffmpeg's names — never Matroska's or MP4's, so no
/// session copies it (`ts::Kind::Audio` says why).
fn audio_codec(tag: u16) -> String {
    match tag {
        0x0001 => "pcm".into(),
        0x0050 => "mp2".into(),
        0x0055 => "mp3".into(),
        0x00ff | 0x1600 | 0x1601 | 0x706d => "aac".into(),
        0x2000 => "ac3".into(),
        0x2001 => "dts".into(),
        t => format!("format tag 0x{t:04x}"),
    }
}

pub async fn probe(src: &Source<'_>, head: &[u8]) -> Result<MediaInfo, ProbeError> {
    let (mut hdrl, mut indexed) = (None::<Vec<u8>>, false);
    let mut at = 12u64;
    // Walked to the index: `hdrl` comes first, and OpenDML's `indx` in it ends the walk there; else past `movi`'s
    // header, which says how far to skip, to an `idx1` — or to the end of a file that has none.
    for _ in 0..MAX_CHUNKS {
        let hdr = read_at(src, head, at, 12).await?;
        let Some(size) = u32_at(&hdr, 4).map(u64::from) else { break };
        match (&hdr[..4], hdr.get(8..12)) {
            (b"LIST", Some(b"hdrl")) => {
                if size > MAX_ELEMENT {
                    return Err(ProbeError::Unsupported(format!("a {size}-byte AVI header")));
                }
                let body = read_at(src, head, at + 12, size.saturating_sub(4)).await?;
                indexed = streams(&body).iter().any(|s| s.indexed);
                hdrl = Some(body);
            }
            (b"idx1", _) => indexed = true,
            _ => {}
        }
        if indexed {
            break;
        }
        at = at.saturating_add(8 + size + (size & 1));
    }
    let hdrl = hdrl.ok_or_else(|| ProbeError::Unsupported("an AVI with no header list".into()))?;
    let streams = streams(&hdrl);
    if !indexed {
        return Err(ProbeError::Unsupported(NO_INDEX.into()));
    }
    let avih = chunks(&hdrl).find(|(id, _)| id == b"avih").map(|(_, b)| b).unwrap_or(&[]);
    let video = streams
        .iter()
        .find(|s| &s.kind == b"vids")
        .ok_or_else(|| ProbeError::Unsupported("an AVI with no video".into()))?;
    // BITMAPINFOHEADER: the width, the height (negative for a top-down picture), and the compression's fourcc.
    let compression: Option<[u8; 4]> = video.strf.get(16..20).and_then(|b| b.try_into().ok());
    let codec = match compression.filter(|c| c.iter().any(|b| *b != 0)) {
        Some(c) => video_codec(&c).or_else(|_| video_codec(&video.handler))?,
        None => video_codec(&video.handler)?,
    };
    let width = u32_at(video.strf, 4).or_else(|| u32_at(avih, 32)).unwrap_or(0);
    let height =
        u32_at(video.strf, 8).map(|h| (h as i32).unsigned_abs()).or_else(|| u32_at(avih, 36)).unwrap_or(0);
    // The stream's length in its own units (`dwLength`, which OpenDML keeps for the whole file), each `dwScale` /
    // `dwRate` seconds; the main header's frame count and frame time where those are missing.
    let (scale, rate, length) = (u32_at(video.strh, 20), u32_at(video.strh, 24), u32_at(video.strh, 32));
    let frame_rate = match (scale, rate) {
        (Some(s), Some(r)) if s > 0 && r > 0 => Some(r as f64 / s as f64),
        _ => u32_at(avih, 0).filter(|us| *us > 0).map(|us| 1e6 / us as f64),
    };
    let frames = length.filter(|l| *l > 0).or_else(|| u32_at(avih, 16)).unwrap_or(0);
    let duration = frame_rate.map_or(0.0, |fps| frames as f64 / fps);
    if !(duration > 0.0 && duration <= GRID_MAX_DURATION) {
        return Err(ProbeError::Unsupported(format!("an AVI {duration} s long")));
    }
    let audio = streams
        .iter()
        .filter(|s| &s.kind == b"auds")
        .map(|s| AudioTrack {
            codec: audio_codec(u16_at(s.strf, 0).unwrap_or(0)),
            language: None,
            channels: u16_at(s.strf, 2).map_or(2, u32::from).max(1),
            name: None,
            default: true,
            commentary: false,
            bytes: None,
        })
        .collect();
    Ok(MediaInfo {
        container: "avi",
        duration,
        video: codec,
        codecs: None,
        width,
        height,
        hdr: false,
        hlg: false,
        frame_rate,
        dolby_vision: None,
        dolby_vision_record_mismatch: false,
        dolby_vision_recordless: false,
        audio,
        subtitles: Vec::new(),
        keyframes: super::grid(0.0, duration),
        // A transcode's GOPs are its encoder's, closed.
        closed_gops: true,
        byte_index: Vec::new(),
        video_bytes: None,
    })
}
