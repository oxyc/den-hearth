//! Bounded extraction of complete indexed Matroska and ISO-BMFF text subtitles from a TorBox direct-file
//! capability.

use std::sync::atomic::{AtomicU8, AtomicUsize, Ordering::Relaxed};
use std::sync::Arc;
use std::time::Duration;

use futures_util::stream::{self, StreamExt};
use reqwest::header::{ACCEPT_ENCODING, CONTENT_ENCODING, CONTENT_RANGE, CONTENT_TYPE, RANGE};

use den_media::indexed_mkv::{self, Cue, TextCodec, TextTrack};
use den_media::indexed_mp4;
use den_media::mkv;
use den_upstream::{ClientProfile, Guard, Redirect};

use crate::loglimit::log_limited;

const HEAD_BYTES: u64 = 64 * 1024;
const MKV_HEAD_PROBES: [usize; 4] = [HEAD_BYTES as usize, 256 * 1024, 1024 * 1024, 4 * 1024 * 1024];
const MAX_ELEMENT_BYTES: usize = 8 * 1024 * 1024;
const MAX_INDEX_BYTES: usize = 16 * 1024 * 1024;
const MAX_REQUESTS: usize = 4096;
const MAX_CUES_PER_TRACK: usize = 2000;
const BLOCK_PREFIX_BYTES: usize = 4096;
const MAX_BLOCK_BYTES: usize = 64 * 1024;
const MAX_VTT_BYTES: usize = 2 * 1024 * 1024;
const PARALLEL_BLOCKS: usize = 128;
const MULTI_RANGE_PARTS: usize = 32;
const MULTI_RANGE_OVERHEAD: usize = 64 * 1024;
const DEADLINE: Duration = Duration::from_secs(8);

const INPUT_UNKNOWN: u8 = 0;
const INPUT_OTHER: u8 = 1;
const INPUT_MKV_NO_INDEX: u8 = 2;
const INPUT_MKV_INDEXED: u8 = 3;
const INPUT_MP4_INDEXED: u8 = 4;

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
    input: Arc<AtomicU8>,
}

impl Reader {
    fn new(raw: &str) -> Result<Self, &'static str> {
        let url = reqwest::Url::parse(raw).map_err(|_| "parse")?;
        if url.scheme() != "https" {
            return Err("scheme");
        }
        if url.host_str().is_none() {
            return Err("host");
        }
        if !url.username().is_empty() || url.password().is_some() {
            return Err("credentials");
        }
        if url.port_or_known_default() != Some(443) {
            return Err("port");
        }
        if url.fragment().is_some() {
            return Err("fragment");
        }
        let client = ClientProfile::new()
            .connect(Duration::from_secs(3))
            .total(Duration::from_secs(5))
            .redirect(Redirect::Never)
            .guard(Guard::PublicDial)
            .pool(PARALLEL_BLOCKS, Duration::from_secs(15))
            .build()
            .map_err(|_| "client")?;
        Ok(Self {
            client,
            url,
            bytes: Arc::new(AtomicUsize::new(0)),
            requests: Arc::new(AtomicUsize::new(0)),
            input: Arc::new(AtomicU8::new(INPUT_UNKNOWN)),
        })
    }

    fn input_kind(&self) -> &'static str {
        match self.input.load(Relaxed) {
            INPUT_OTHER => "other",
            INPUT_MKV_NO_INDEX => "mkv_no_index",
            INPUT_MKV_INDEXED => "mkv_indexed",
            INPUT_MP4_INDEXED => "mp4_indexed",
            _ => "unknown",
        }
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

    async fn ranges(&self, ranges: &[(u64, usize)]) -> Option<(Vec<Vec<u8>>, u64)> {
        if ranges.is_empty() || ranges.len() > MULTI_RANGE_PARTS {
            return None;
        }
        if ranges.len() == 1 {
            let (bytes, total) = self.range(ranges[0].0, ranges[0].1).await?;
            return Some((vec![bytes], total));
        }
        let requested = ranges.iter().try_fold(0usize, |sum, (_, len)| {
            (*len > 0 && *len <= MAX_ELEMENT_BYTES).then_some(())?;
            sum.checked_add(*len)
        })?;
        if self.requests.fetch_add(1, Relaxed) >= MAX_REQUESTS {
            return None;
        }
        let mut used = self.bytes.load(Relaxed);
        loop {
            let reserved = used.checked_add(requested).filter(|total| *total <= MAX_INDEX_BYTES)?;
            match self.bytes.compare_exchange_weak(used, reserved, Relaxed, Relaxed) {
                Ok(_) => break,
                Err(actual) => used = actual,
            }
        }
        let mut value = String::from("bytes=");
        for (index, (start, len)) in ranges.iter().enumerate() {
            let end = start.checked_add(u64::try_from(*len).ok()?)?.checked_sub(1)?;
            if index > 0 {
                value.push(',');
            }
            use std::fmt::Write as _;
            write!(&mut value, "{start}-{end}").ok()?;
        }
        let response = self
            .client
            .get(self.url.clone())
            .header(ACCEPT_ENCODING, "identity")
            .header(RANGE, value)
            .send()
            .await
            .ok()?;
        if response.status() != reqwest::StatusCode::PARTIAL_CONTENT || response.url() != &self.url {
            return None;
        }
        if response.headers().get(CONTENT_ENCODING).is_some_and(|value| value.as_bytes() != b"identity") {
            return None;
        }
        let boundary = multipart_boundary(response.headers().get(CONTENT_TYPE)?.to_str().ok()?)?;
        let cap = requested.checked_add(MULTI_RANGE_OVERHEAD)?;
        if response.content_length().is_none_or(|length| length > cap as u64) {
            return None;
        }
        let body = response.bytes().await.ok()?;
        if body.len() > cap {
            return None;
        }
        parse_multipart_ranges(&body, &boundary, ranges)
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

fn multipart_boundary(raw: &str) -> Option<Vec<u8>> {
    let mut fields = raw.split(';');
    if !fields.next()?.trim().eq_ignore_ascii_case("multipart/byteranges") {
        return None;
    }
    let value = fields.find_map(|field| {
        let (name, value) = field.trim().split_once('=')?;
        name.trim().eq_ignore_ascii_case("boundary").then_some(value.trim())
    })?;
    let value = value.strip_prefix('"').and_then(|value| value.strip_suffix('"')).unwrap_or(value);
    (1..=70)
        .contains(&value.len())
        .then_some(())
        .filter(|_| value.bytes().all(|byte| byte.is_ascii_graphic() && !matches!(byte, b'"' | b'\\')))?;
    Some(value.as_bytes().to_vec())
}

fn find_bytes(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    (!needle.is_empty()).then_some(())?;
    haystack.windows(needle.len()).position(|window| window == needle)
}

fn parse_multipart_ranges(
    body: &[u8],
    boundary: &[u8],
    requested: &[(u64, usize)],
) -> Option<(Vec<Vec<u8>>, u64)> {
    let mut marker = Vec::with_capacity(boundary.len() + 4);
    marker.extend_from_slice(b"--");
    marker.extend_from_slice(boundary);
    let mut cursor = 0usize;
    let mut total = None;
    let mut output = vec![None; requested.len()];
    for _ in 0..requested.len() {
        if cursor == 0 {
            body.get(..marker.len()).filter(|value| *value == marker.as_slice())?;
            cursor = marker.len();
        } else {
            let mut next = Vec::with_capacity(marker.len() + 2);
            next.extend_from_slice(b"\r\n");
            next.extend_from_slice(&marker);
            let at = find_bytes(body.get(cursor..)?, &next)?;
            cursor = cursor.checked_add(at)?.checked_add(next.len())?;
        }
        body.get(cursor..cursor + 2).filter(|value| *value == b"\r\n")?;
        cursor += 2;
        let headers_end = find_bytes(body.get(cursor..)?, b"\r\n\r\n")?;
        if headers_end > 4096 {
            return None;
        }
        let headers = std::str::from_utf8(body.get(cursor..cursor + headers_end)?).ok()?;
        let content_range = headers.lines().find_map(|line| {
            let (name, value) = line.split_once(':')?;
            name.trim().eq_ignore_ascii_case("content-range").then_some(value.trim())
        })?;
        let (start, end, part_total) = parse_content_range(content_range)?;
        if total.replace(part_total).is_some_and(|value| value != part_total) {
            return None;
        }
        cursor = cursor.checked_add(headers_end)?.checked_add(4)?;
        let len = usize::try_from(end.checked_sub(start)?.checked_add(1)?).ok()?;
        let index = requested
            .iter()
            .position(|(wanted_start, wanted_len)| *wanted_start == start && *wanted_len == len)?;
        if output[index].is_some() {
            return None;
        }
        output[index] = Some(body.get(cursor..cursor.checked_add(len)?)?.to_vec());
        cursor = cursor.checked_add(len)?;
    }
    let mut closing = Vec::with_capacity(marker.len() + 6);
    closing.extend_from_slice(b"\r\n");
    closing.extend_from_slice(&marker);
    closing.extend_from_slice(b"--");
    body.get(cursor..cursor + closing.len()).filter(|value| *value == closing.as_slice())?;
    let values = output.into_iter().collect::<Option<Vec<_>>>()?;
    Some((values, total?))
}

async fn index(
    reader: &Reader,
) -> Option<(u64, u64, u64, Vec<TextTrack>, Vec<indexed_mkv::CueTrack>, std::collections::HashMap<u64, usize>)>
{
    // A one-byte range is not a reliable length probe on every TorBox CDN path: some valid large objects have
    // answered `bytes 0-0/1`. Start with a small real header, then grow it only when the Segment's first
    // complete SeekHead is farther into the file. Large muxer padding and an early large Void are both legal;
    // treating 64 KiB as a format boundary incorrectly rejected otherwise indexed MKVs. The progressive reads
    // remain inside the extractor's existing byte and time budgets.
    let mut found = None;
    for (probe, requested) in MKV_HEAD_PROBES.into_iter().enumerate() {
        let (head, total) = reader.range(0, requested).await?;
        if probe == 0 && !head.starts_with(&[0x1a, 0x45, 0xdf, 0xa3]) {
            reader.input.store(INPUT_OTHER, Relaxed);
            return None;
        }
        if let Some(index) = indexed_mkv::segment_index(&head) {
            found = Some((index, total, requested));
            break;
        }
    }
    let Some((mut index, total, head_bytes)) = found else {
        reader.input.store(INPUT_MKV_NO_INDEX, Relaxed);
        return None;
    };
    reader.input.store(INPUT_MKV_INDEXED, Relaxed);
    if let Some(next) = index.elements.get(&mkv::SEEK_HEAD).copied().filter(|at| *at >= head_bytes as u64) {
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

async fn read_many(reader: &Reader, ranges: &[(u64, usize)]) -> Option<Vec<Vec<u8>>> {
    if ranges.is_empty() {
        return Some(Vec::new());
    }
    // A server may merge duplicate byte ranges into one multipart section. Deduplicate before asking, then
    // restore the caller's order; repeated Cluster positions are common in Matroska cues.
    let mut unique = Vec::with_capacity(ranges.len());
    let mut order = Vec::with_capacity(ranges.len());
    for range in ranges {
        let index = unique.iter().position(|candidate| candidate == range).unwrap_or_else(|| {
            unique.push(*range);
            unique.len() - 1
        });
        order.push(index);
    }
    let batches = unique.chunks(MULTI_RANGE_PARTS).map(<[_]>::to_vec).collect::<Vec<_>>();
    let first = reader.ranges(&batches[0]).await;
    let unique_values = if let Some((first, _)) = first {
        let reads = batches.into_iter().skip(1).map(|batch| {
            let reader = reader.clone();
            async move { reader.ranges(&batch).await.map(|value| value.0) }
        });
        let rows = stream::iter(reads).buffered(PARALLEL_BLOCKS).collect::<Vec<_>>().await;
        let mut values = first;
        for row in rows {
            values.extend(row?);
        }
        values
    } else {
        // Multiple ranges are optional in HTTP. Keep correctness on a CDN that supports only a single range,
        // with the same global concurrency bound as the original extractor.
        let reads = unique.into_iter().map(|(start, len)| {
            let reader = reader.clone();
            async move { reader.range(start, len).await.map(|value| value.0) }
        });
        stream::iter(reads)
            .buffered(PARALLEL_BLOCKS)
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .collect::<Option<Vec<_>>>()?
    };
    order.into_iter().map(|index| unique_values.get(index).cloned()).collect()
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
    let first_cluster = segment.checked_add(cues.first()?.cluster)?;
    let cluster_head = reader.range(first_cluster, 16).await?.0;
    let (cluster_id, _, cluster_header) = den_media::ebml::header(&cluster_head)?;
    if cluster_id != mkv::CLUSTER {
        return None;
    }
    // Matroska muxers encode one file's Cluster sizes with a consistent VINT width. CueRelativePosition starts
    // after that header, so learning it once avoids one high-latency CDN request per cue. This remains fail
    // closed: every resulting element must decode as the requested subtitle track and every indexed frame must
    // be present before a sidecar is returned.
    let block_ranges = cues
        .iter()
        .map(|cue| {
            let at = cluster_block_position(segment, cue.cluster, cluster_header, cue.relative)?;
            let remaining = usize::try_from(total.checked_sub(at)?).ok()?;
            Some((at, remaining.min(BLOCK_PREFIX_BYTES)))
        })
        .collect::<Option<Vec<_>>>()?;
    let prefixes = read_many(reader, &block_ranges).await?;
    let decode = cues
        .into_iter()
        .zip(block_ranges)
        .zip(prefixes)
        .map(|((cue, (at, _)), first)| {
            let reader = reader.clone();
            async move {
                let (decoded_track, decoded_duration, text) = match indexed_mkv::subtitle_block(&first) {
                    Some(block) => (block.track, block.duration, clean_text(block.text, track.codec)?),
                    None => {
                        let (_, size, header) = den_media::ebml::header(&first)?;
                        let whole = header.checked_add(usize::try_from(size?).ok()?)?;
                        let remaining = usize::try_from(total.checked_sub(at)?).ok()?;
                        if whole <= first.len() || whole > MAX_BLOCK_BYTES || whole > remaining {
                            return None;
                        }
                        let exact = reader.range(at, whole).await?.0;
                        let block = indexed_mkv::subtitle_block(&exact)?;
                        (block.track, block.duration, clean_text(block.text, track.codec)?)
                    }
                };
                if decoded_track != track.number {
                    return None;
                }
                let duration = cue.duration.or(decoded_duration).filter(|duration| *duration > 0)?;
                Some((cue, duration, text))
            }
        })
        .collect::<Vec<_>>();
    let rows = stream::iter(decode).buffered(PARALLEL_BLOCKS).collect::<Vec<_>>().await;
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
    let ranges = track
        .samples
        .iter()
        .filter(|sample| sample.size > 0)
        .map(|sample| {
            ((sample.size as usize) <= MAX_BLOCK_BYTES
                && sample.offset.checked_add(u64::from(sample.size)).is_some_and(|end| end <= total))
            .then_some((sample.offset, sample.size as usize))
        })
        .collect::<Option<Vec<_>>>()?;
    let mut reads = read_many(reader, &ranges).await?.into_iter();
    let mut vtt = String::from("WEBVTT\n\n");
    for sample in &track.samples {
        let bytes = if sample.size == 0 { Vec::new() } else { reads.next()? };
        let text = if bytes.is_empty() {
            String::new()
        } else {
            clean_text(&indexed_mp4::sample_text(track.codec, &bytes)?, TextCodec::Plain)?
        };
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
    let Some((_, total)) = reader.range(0, HEAD_BYTES as usize).await else { return Vec::new() };
    let Some(moov) = mp4_moov(reader, total).await else { return Vec::new() };
    reader.input.store(INPUT_MP4_INDEXED, Relaxed);
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
    let reader = match Reader::new(raw_url) {
        Ok(reader) => reader,
        Err(url_reason) => {
            log_limited(
            &format!("managed-subtitle-sidecar-invalid-direct-url-{url_reason}"),
            &format!(
                "managed HLS: indexed subtitle sidecar unavailable: outcome=skipped reason=invalid_direct_url url_reason={url_reason} selected={}",
                selected.map_or_else(|| "all".to_owned(), |value| value.to_string())
            ),
        );
            return Vec::new();
        }
    };
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
                        "managed HLS: indexed subtitle sidecar unavailable: outcome=ineligible input={} selected={} requests={requests} reserved_bytes={bytes}",
                        reader.input_kind(),
                        selected.map_or_else(|| "all".to_owned(), |value| value.to_string()),
                    ),
                );
            } else {
                log_limited(
                    &format!("managed-subtitle-sidecar-ready-{}", selected.unwrap_or(-1)),
                    &format!(
                        "managed HLS: indexed subtitle sidecar ready: selected={} tracks={} requests={requests} reserved_bytes={bytes}",
                        selected.map_or_else(|| "all".to_owned(), |value| value.to_string()),
                        sidecars.len()
                    ),
                );
            }
            sidecars
        }
        Err(_) => {
            log_limited(
                &format!("managed-subtitle-sidecar-timeout-{}", selected.unwrap_or(-1)),
                &format!(
                    "managed HLS: indexed subtitle sidecar unavailable: outcome=timeout input={} selected={} requests={requests} reserved_bytes={bytes}",
                    reader.input_kind(),
                    selected.map_or_else(|| "all".to_owned(), |value| value.to_string()),
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
    fn multipart_byte_ranges_are_reordered_and_exact() {
        let body = concat!(
            "--den-boundary\r\n",
            "Content-Type: application/octet-stream\r\n",
            "Content-Range: bytes 20-22/100\r\n\r\n",
            "two\r\n",
            "--den-boundary\r\n",
            "Content-Range: bytes 4-6/100\r\n\r\n",
            "one\r\n",
            "--den-boundary--\r\n"
        );
        assert_eq!(
            parse_multipart_ranges(body.as_bytes(), b"den-boundary", &[(4, 3), (20, 3)]),
            Some((vec![b"one".to_vec(), b"two".to_vec()], 100))
        );
    }

    #[test]
    fn multipart_boundaries_are_bounded() {
        assert_eq!(multipart_boundary("multipart/byteranges; boundary=abc"), Some(b"abc".to_vec()));
        assert_eq!(multipart_boundary("multipart/byteranges; boundary=\"abc\""), Some(b"abc".to_vec()));
        assert_eq!(multipart_boundary("text/plain; boundary=abc"), None);
        assert_eq!(multipart_boundary("multipart/byteranges; boundary=bad\\value"), None);
    }

    #[test]
    fn direct_url_validation_reports_only_a_closed_reason() {
        assert!(Reader::new("https://cdn.example/file.mkv?token=secret").is_ok());
        for (url, reason) in [
            ("not a url", "parse"),
            ("http://cdn.example/file.mkv", "scheme"),
            ("https://user:secret@cdn.example/file.mkv", "credentials"),
            ("https://cdn.example:8443/file.mkv", "port"),
            ("https://cdn.example/file.mkv#private", "fragment"),
        ] {
            assert_eq!(Reader::new(url).err(), Some(reason));
        }
    }

    #[tokio::test]
    async fn an_invalid_direct_url_is_reported_without_being_logged() {
        let secret = "http://user:password@example.invalid/private-file.mkv";
        let captured = crate::loglimit::Capture::start();
        assert!(sidecars(secret, Some(1)).await.is_empty());
        let log = captured.text();
        assert!(log.contains("reason=invalid_direct_url url_reason=scheme selected=1"), "{log}");
        for private in ["user", "password", "example.invalid", "private-file"] {
            assert!(!log.contains(private), "log leaked {private:?}: {log}");
        }
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
