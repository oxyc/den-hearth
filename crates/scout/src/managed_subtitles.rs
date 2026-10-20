//! Bounded extraction of complete indexed Matroska and ISO-BMFF text subtitles from a TorBox direct-file
//! capability.

use std::sync::atomic::{AtomicUsize, Ordering::Relaxed};
use std::sync::Arc;
use std::time::Duration;

use futures_util::stream::{self, StreamExt};
use reqwest::header::{ACCEPT_ENCODING, CONTENT_ENCODING, CONTENT_RANGE, RANGE};

use den_media::indexed_mkv::{self, Cue, TextCodec, TextTrack};
use den_media::indexed_mp4;
use den_media::mkv;
use den_upstream::{ClientProfile, Guard, Redirect};

use crate::loglimit::log_limited;

const HEAD_BYTES: u64 = 64 * 1024;
const MAX_ELEMENT_BYTES: usize = 8 * 1024 * 1024;
const MAX_INDEX_BYTES: usize = 16 * 1024 * 1024;
const MAX_REQUESTS: usize = 4096;
const MAX_CUES_PER_TRACK: usize = 2000;
const BLOCK_PREFIX_BYTES: usize = 4096;
const MAX_BLOCK_BYTES: usize = 64 * 1024;
const MAX_VTT_BYTES: usize = 2 * 1024 * 1024;
const PARALLEL_BLOCKS: usize = 32;
const DEADLINE: Duration = Duration::from_secs(8);

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Sidecar {
    pub(crate) ordinal: i64,
    pub(crate) vtt: String,
}

#[derive(Clone)]
struct Reader {
    client: reqwest::Client,
    url: reqwest::Url,
    bytes: Arc<AtomicUsize>,
    requests: Arc<AtomicUsize>,
}

impl Reader {
    fn new(raw: &str) -> Option<Self> {
        let url = reqwest::Url::parse(raw).ok()?;
        if url.scheme() != "https:"
            || url.host_str().is_none()
            || !url.username().is_empty()
            || url.password().is_some()
            || url.port_or_known_default() != Some(443)
            || url.fragment().is_some()
        {
            return None;
        }
        let client = ClientProfile::new()
            .connect(Duration::from_secs(3))
            .total(Duration::from_secs(5))
            .redirect(Redirect::Never)
            .guard(Guard::PublicDial)
            .pool(PARALLEL_BLOCKS, Duration::from_secs(15))
            .build()
            .ok()?;
        Some(Self {
            client,
            url,
            bytes: Arc::new(AtomicUsize::new(0)),
            requests: Arc::new(AtomicUsize::new(0)),
        })
    }

    async fn range(&self, start: u64, len: usize) -> Option<(Vec<u8>, u64)> {
        if len == 0 || len > MAX_ELEMENT_BYTES {
            return None;
        }
        if self.requests.fetch_add(1, Relaxed) >= MAX_REQUESTS {
            return None;
        }
        let mut used = self.bytes.load(Relaxed);
        loop {
            let reserved = used.checked_add(len).filter(|total| *total <= MAX_INDEX_BYTES)?;
            match self.bytes.compare_exchange_weak(used, reserved, Relaxed, Relaxed) {
                Ok(_) => break,
                Err(actual) => used = actual,
            }
        }
        let end = start.checked_add(u64::try_from(len).ok()?)?.checked_sub(1)?;
        let response = self
            .client
            .get(self.url.clone())
            .header(ACCEPT_ENCODING, "identity")
            .header(RANGE, format!("bytes={start}-{end}"))
            .send()
            .await
            .ok()?;
        if response.status() != reqwest::StatusCode::PARTIAL_CONTENT || response.url() != &self.url {
            return None;
        }
        if response.headers().get(CONTENT_ENCODING).is_some_and(|value| value.as_bytes() != b"identity") {
            return None;
        }
        let (actual_start, actual_end, total) =
            parse_content_range(response.headers().get(CONTENT_RANGE)?.to_str().ok()?)?;
        if actual_start != start || actual_end != end || end >= total {
            return None;
        }
        let body = response.bytes().await.ok()?;
        if body.len() != len {
            return None;
        }
        Some((body.to_vec(), total))
    }

    async fn element(&self, at: u64, max: usize) -> Option<(u32, Vec<u8>)> {
        let (head, total) = self.range(at, 16).await?;
        let (id, size, header) = den_media::ebml::header(&head)?;
        let size = usize::try_from(size?).ok()?;
        if size > max || size > MAX_ELEMENT_BYTES {
            return None;
        }
        let whole = header.checked_add(size)?;
        if at.checked_add(u64::try_from(whole).ok()?)? > total {
            return None;
        }
        let bytes = if whole <= head.len() { head[..whole].to_vec() } else { self.range(at, whole).await?.0 };
        Some((id, bytes[header..].to_vec()))
    }
}

fn parse_content_range(raw: &str) -> Option<(u64, u64, u64)> {
    let value = raw.strip_prefix("bytes ")?;
    let (range, total) = value.split_once('/')?;
    let (start, end) = range.split_once('-')?;
    let parsed = (start.parse().ok()?, end.parse().ok()?, total.parse().ok()?);
    (parsed.0 <= parsed.1 && parsed.1 < parsed.2).then_some(parsed)
}

async fn index(
    reader: &Reader,
) -> Option<(u64, u64, u64, Vec<TextTrack>, Vec<indexed_mkv::CueTrack>, std::collections::HashMap<u64, usize>)>
{
    let (head, total) =
        reader.range(0, usize::try_from(HEAD_BYTES.min(total_hint(reader).await?)).ok()?).await?;
    let mut index = indexed_mkv::segment_index(&head)?;
    if let Some(next) = index.elements.get(&mkv::SEEK_HEAD).copied().filter(|at| *at >= HEAD_BYTES) {
        let (id, body) = reader.element(next, 1024 * 1024).await?;
        if id == mkv::SEEK_HEAD {
            let chained = indexed_mkv::seek_head(&body, index.payload_at)?;
            for (id, at) in chained.elements {
                index.elements.entry(id).or_insert(at);
            }
        }
    }
    let fetch = |id: u32| index.elements.get(&id).copied();
    let (info_id, info) = reader.element(fetch(mkv::INFO)?, 1024 * 1024).await?;
    let (tracks_id, tracks) = reader.element(fetch(mkv::TRACKS)?, 1024 * 1024).await?;
    let (cues_id, cues) = reader.element(fetch(mkv::CUES)?, MAX_ELEMENT_BYTES).await?;
    let (tags_id, tags) = reader.element(fetch(mkv::TAGS)?, 4 * 1024 * 1024).await?;
    if (info_id, tracks_id, cues_id, tags_id) != (mkv::INFO, mkv::TRACKS, mkv::CUES, mkv::TAGS) {
        return None;
    }
    let scale = indexed_mkv::timestamp_scale(&info)?;
    let tracks = indexed_mkv::text_tracks(&tracks)?;
    let cues = indexed_mkv::cue_tracks(&cues, tracks.len().checked_mul(MAX_CUES_PER_TRACK)?)?;
    let counts = indexed_mkv::frame_counts(&tags)?;
    Some((total, index.payload_at, scale, tracks, cues, counts))
}

// One-byte range first: unlike HEAD it proves range support and tells us the exact object length.
async fn total_hint(reader: &Reader) -> Option<u64> {
    reader.range(0, 1).await.map(|(_, total)| total)
}

fn clean_text(value: &str, codec: TextCodec) -> Option<String> {
    let value = match codec {
        TextCodec::Plain => value,
        TextCodec::Ass => value.splitn(9, ',').nth(8)?,
    };
    let mut normalized = value.replace("\r\n", "\n").replace('\r', "\n").replace('\0', "");
    if codec == TextCodec::Ass {
        normalized = normalized.replace("\\N", "\n").replace("\\n", "\n").replace("\\h", " ");
    }
    let mut output = String::with_capacity(normalized.len());
    let mut rest = normalized.as_str();
    while !rest.is_empty() {
        if rest.starts_with("{\\") {
            if let Some(end) = rest.find('}') {
                rest = &rest[end + 1..];
                continue;
            }
        }
        if rest.starts_with('<') {
            if let Some(end) = rest.find('>') {
                let tag = rest[1..end].trim().trim_start_matches('/');
                let name = tag.split([' ', '.']).next().unwrap_or("");
                if matches!(name, "b" | "i" | "u" | "font" | "c" | "v" | "lang" | "ruby" | "rt")
                    || (name.as_bytes().first().is_some_and(u8::is_ascii_digit) && name.contains(':'))
                {
                    rest = &rest[end + 1..];
                    continue;
                }
            }
        }
        let ch = rest.chars().next()?;
        output.push(ch);
        rest = &rest[ch.len_utf8()..];
    }
    Some(output.trim().to_owned())
}

fn timestamp(units: u64, scale_ns: u64) -> Option<String> {
    let millis = units.checked_mul(scale_ns)?.checked_div(1_000_000)?;
    let hours = millis / 3_600_000;
    let minutes = millis / 60_000 % 60;
    let seconds = millis / 1_000 % 60;
    Some(format!("{hours:02}:{minutes:02}:{seconds:02}.{:03}", millis % 1000))
}

fn mp4_timestamp(units: u64, timescale: u32) -> Option<String> {
    let millis = units.checked_mul(1000)?.checked_div(u64::from(timescale))?;
    let hours = millis / 3_600_000;
    let minutes = millis / 60_000 % 60;
    let seconds = millis / 1_000 % 60;
    Some(format!("{hours:02}:{minutes:02}:{seconds:02}.{:03}", millis % 1000))
}

fn cluster_block_position(segment: u64, cluster: u64, cluster_header: usize, relative: u64) -> Option<u64> {
    segment.checked_add(cluster)?.checked_add(u64::try_from(cluster_header).ok()?)?.checked_add(relative)
}

async fn block(
    reader: Reader,
    segment: u64,
    total: u64,
    track_number: u64,
    codec: TextCodec,
    cue: Cue,
) -> Option<(Cue, u64, String)> {
    let cluster_at = segment.checked_add(cue.cluster)?;
    let cluster_head = reader.range(cluster_at, 16).await?.0;
    let (cluster_id, _, cluster_header) = den_media::ebml::header(&cluster_head)?;
    if cluster_id != mkv::CLUSTER {
        return None;
    }
    // CueRelativePosition's zero is the first possible child in the Cluster, after its EBML header.
    let at = cluster_block_position(segment, cue.cluster, cluster_header, cue.relative)?;
    let remaining = usize::try_from(total.checked_sub(at)?).ok()?;
    let first_len = remaining.min(BLOCK_PREFIX_BYTES);
    let first = reader.range(at, first_len).await?.0;
    let decoded = match indexed_mkv::subtitle_block(&first) {
        Some(block) => block,
        None => {
            let (_, size, header) = den_media::ebml::header(&first)?;
            let whole = header.checked_add(usize::try_from(size?).ok()?)?;
            if whole <= first.len() || whole > MAX_BLOCK_BYTES || whole > remaining {
                return None;
            }
            let exact = reader.range(at, whole).await?.0;
            let decoded = indexed_mkv::subtitle_block(&exact)?;
            if decoded.track != track_number {
                return None;
            }
            let duration = cue.duration.or(decoded.duration).filter(|duration| *duration > 0)?;
            return Some((cue, duration, clean_text(decoded.text, codec)?));
        }
    };
    if decoded.track != track_number {
        return None;
    }
    let duration = cue.duration.or(decoded.duration).filter(|duration| *duration > 0)?;
    Some((cue, duration, clean_text(decoded.text, codec)?))
}

async fn extract(
    reader: &Reader,
    total: u64,
    segment: u64,
    scale: u64,
    track: &TextTrack,
    cues: Vec<Cue>,
) -> Option<String> {
    if cues.is_empty() || cues.len() > MAX_CUES_PER_TRACK {
        return None;
    }
    let requests = cues
        .into_iter()
        .map(|cue| block(reader.clone(), segment, total, track.number, track.codec, cue))
        .collect::<Vec<_>>();
    let rows = stream::iter(requests).buffered(PARALLEL_BLOCKS).collect::<Vec<_>>().await;
    let mut vtt = String::from("WEBVTT\n\n");
    for row in rows {
        let (cue, duration, text) = row?;
        let end = cue.timestamp.checked_add(duration)?;
        vtt.push_str(&timestamp(cue.timestamp, scale)?);
        vtt.push_str(" --> ");
        vtt.push_str(&timestamp(end, scale)?);
        vtt.push('\n');
        vtt.push_str(&text);
        vtt.push_str("\n\n");
        if vtt.len() > MAX_VTT_BYTES {
            return None;
        }
    }
    Some(vtt)
}

async fn mp4_moov(reader: &Reader, total: u64) -> Option<Vec<u8>> {
    let mut at = 0u64;
    for _ in 0..64 {
        let available = usize::try_from(total.checked_sub(at)?.min(16)).ok()?;
        if available < 8 {
            return None;
        }
        let head = reader.range(at, available).await?.0;
        let (declared, kind, header) = den_media::mp4::header(&head)?;
        let size = if head.starts_with(&[0; 4]) { total.checked_sub(at)? } else { declared };
        if size < header as u64 || at.checked_add(size)? > total {
            return None;
        }
        if kind == *b"moov" {
            let body_len = usize::try_from(size.checked_sub(header as u64)?).ok()?;
            if body_len > MAX_ELEMENT_BYTES {
                return None;
            }
            return reader.range(at.checked_add(header as u64)?, body_len).await.map(|value| value.0);
        }
        at = at.checked_add(size)?;
        if at == total {
            return None;
        }
    }
    None
}

async fn extract_mp4_track(reader: &Reader, total: u64, track: indexed_mp4::TextTrack) -> Option<Sidecar> {
    let reads = track
        .samples
        .iter()
        .map(|sample| {
            let reader = reader.clone();
            let sample = sample.clone();
            async move {
                if sample.size as usize > MAX_BLOCK_BYTES
                    || sample.offset.checked_add(u64::from(sample.size))? > total
                {
                    return None;
                }
                let bytes = if sample.size == 0 {
                    Vec::new()
                } else {
                    reader.range(sample.offset, sample.size as usize).await?.0
                };
                let text = if bytes.is_empty() {
                    String::new()
                } else {
                    clean_text(&indexed_mp4::sample_text(track.codec, &bytes)?, TextCodec::Plain)?
                };
                Some((sample, text))
            }
        })
        .collect::<Vec<_>>();
    let rows = stream::iter(reads).buffered(PARALLEL_BLOCKS).collect::<Vec<_>>().await;
    let mut vtt = String::from("WEBVTT\n\n");
    for row in rows {
        let (sample, text) = row?;
        if text.is_empty() {
            continue;
        }
        let end = sample.start.checked_add(u64::from(sample.duration))?;
        vtt.push_str(&mp4_timestamp(sample.start, track.timescale)?);
        vtt.push_str(" --> ");
        vtt.push_str(&mp4_timestamp(end, track.timescale)?);
        vtt.push('\n');
        vtt.push_str(&text);
        vtt.push_str("\n\n");
        if vtt.len() > MAX_VTT_BYTES {
            return None;
        }
    }
    (vtt.len() > "WEBVTT\n\n".len()).then_some(Sidecar { ordinal: track.ordinal as i64, vtt })
}

async fn mp4_sidecars(reader: &Reader, selected: Option<i64>) -> Vec<Sidecar> {
    let Some(total) = total_hint(reader).await else { return Vec::new() };
    let Some(moov) = mp4_moov(reader, total).await else { return Vec::new() };
    let Some(tracks) = indexed_mp4::text_tracks(&moov, MAX_CUES_PER_TRACK) else { return Vec::new() };
    let mut output = Vec::new();
    for track in tracks {
        if selected.is_some_and(|ordinal| ordinal != track.ordinal as i64) {
            continue;
        }
        if let Some(sidecar) = extract_mp4_track(reader, total, track).await {
            output.push(sidecar);
        }
    }
    output
}

/// Returns only tracks whose complete frame count can be proven. Any failure preserves TorBox burn-in and emits
/// bounded, credential-free diagnostics describing whether eligibility or the deadline stopped extraction.
pub(crate) async fn sidecars(raw_url: &str, selected: Option<i64>) -> Vec<Sidecar> {
    let Some(reader) = Reader::new(raw_url) else { return Vec::new() };
    let result = tokio::time::timeout(DEADLINE, async {
        if let Some((total, segment, scale, tracks, cue_tracks, counts)) = index(&reader).await {
            let mut output = Vec::new();
            for track in &tracks {
                if selected.is_some_and(|ordinal| ordinal != track.ordinal as i64) {
                    continue;
                }
                let Some(cues) = cue_tracks.iter().find(|cues| cues.number == track.number) else { continue };
                if counts.get(&track.uid).copied() != Some(cues.cues.len()) {
                    continue;
                }
                if let Some(vtt) = extract(&reader, total, segment, scale, track, cues.cues.clone()).await {
                    output.push(Sidecar { ordinal: track.ordinal as i64, vtt });
                }
            }
            if !output.is_empty() {
                return output;
            }
        }
        mp4_sidecars(&reader, selected).await
    })
    .await;
    let requests = reader.requests.load(Relaxed);
    let bytes = reader.bytes.load(Relaxed);
    match result {
        Ok(sidecars) => {
            if sidecars.is_empty() {
                log_limited(
                    &format!("managed-subtitle-sidecar-ineligible-{}", selected.unwrap_or(-1)),
                    &format!(
                        "managed HLS: indexed subtitle sidecar unavailable: outcome=ineligible selected={} requests={requests} reserved_bytes={bytes}",
                        selected.map_or_else(|| "all".to_owned(), |value| value.to_string())
                    ),
                );
            }
            sidecars
        }
        Err(_) => {
            log_limited(
                &format!("managed-subtitle-sidecar-timeout-{}", selected.unwrap_or(-1)),
                &format!(
                    "managed HLS: indexed subtitle sidecar unavailable: outcome=timeout selected={} requests={requests} reserved_bytes={bytes}",
                    selected.map_or_else(|| "all".to_owned(), |value| value.to_string())
                ),
            );
            Vec::new()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn content_ranges_are_exact() {
        assert_eq!(parse_content_range("bytes 4-9/10"), Some((4, 9, 10)));
        assert_eq!(parse_content_range("bytes 9-4/10"), None);
        assert_eq!(parse_content_range("bytes 0-9/*"), None);
    }

    #[test]
    fn styling_is_removed_but_words_and_line_breaks_remain() {
        assert_eq!(
            clean_text("<i>Hello</i>\r\n{\\an8}world", TextCodec::Plain).as_deref(),
            Some("Hello\nworld")
        );
        assert_eq!(
            clean_text("1,0,Default,,0000,0000,0000,,{\\an8}Hello\\Nworld", TextCodec::Ass,).as_deref(),
            Some("Hello\nworld")
        );
        assert_eq!(clean_text("2 < 3 and {visible", TextCodec::Plain).as_deref(), Some("2 < 3 and {visible"));
    }

    #[test]
    fn cue_relative_positions_begin_after_the_cluster_header() {
        assert_eq!(cluster_block_position(100, 20, 7, 3), Some(130));
    }
}
