//! MPEG-TS: the program's streams from the PAT and PMT in the head, its first timestamps there too, and its last ones
//! from a read of the file's end — the codecs, the picture's size and the duration, in two reads.
//!
//! A transport stream has no index at all: a seek into one is a bisection over its timestamps. Its keyframes are a
//! grid from the first frame (`super::grid`), which only a transcode that forces them there makes true.
//!
//! The timeline is ffmpeg's under `-copyts -start_at_zero`: zero is the earliest timestamp of any stream, the first
//! audio frame as often as the first picture.

use super::{AudioTrack, MediaInfo, ProbeError, Source, VideoCodec, NEITHER};

const PACKET: usize = 188;
/// How much of the file's end is read for its last timestamps: a second or more of a Blu-ray's rate.
const TAIL_BYTES: u64 = 4 << 20;
/// The 90 kHz clock's 33 bits: a timestamp past this has wrapped back round to zero.
const WRAP: u64 = 1 << 33;
/// Of the first picture's bytes, at most this much is gathered to find its sequence header or SPS.
const VIDEO_HEAD: usize = 64 * 1024;

/// Three packets' sync bytes, 188 bytes apart: an MPEG-TS from its first byte. (An M2TS's 192-byte packets are not
/// one, and are refused by name.)
pub fn starts(head: &[u8]) -> bool {
    head.len() >= 3 * PACKET && (0..3).all(|i| head[i * PACKET] == 0x47)
}

struct Packet<'a> {
    pid: u16,
    /// Starts a PES packet or a PSI section.
    start: bool,
    payload: &'a [u8],
}

fn packet(p: &[u8]) -> Option<Packet<'_>> {
    // A transport error flags a packet its own sender knew was damaged.
    if p.len() < PACKET || p[0] != 0x47 || p[1] & 0x80 != 0 {
        return None;
    }
    let pid = u16::from_be_bytes([p[1] & 0x1f, p[2]]);
    let control = (p[3] >> 4) & 3;
    let at = match control & 2 {
        0 => 4,
        _ => 5 + p[4] as usize,
    };
    let payload = match control & 1 == 1 && at <= PACKET {
        true => &p[at..PACKET],
        false => &[][..],
    };
    Some(Packet { pid, start: p[1] & 0x40 != 0, payload })
}

/// The packets of `b` from its first, which is a sync byte.
fn packets(b: &[u8]) -> impl Iterator<Item = Packet<'_>> {
    b.chunks_exact(PACKET).filter_map(packet)
}

/// The PSI section `pid` carries, gathered from the packet that starts it and the ones after: its body after the
/// three-byte header, without the CRC.
fn section(b: &[u8], pid: u16, table: u8) -> Option<Vec<u8>> {
    let mut out: Vec<u8> = Vec::new();
    let mut want = None;
    for p in packets(b).filter(|p| p.pid == pid) {
        match want {
            None if p.start => {
                let pointer = *p.payload.first()? as usize;
                let s = p.payload.get(1 + pointer..)?;
                if s.len() < 3 || s[0] != table {
                    continue;
                }
                let len = (u16::from_be_bytes([s[1], s[2]]) & 0x0fff) as usize;
                out.extend_from_slice(s);
                want = Some(3 + len);
            }
            None => {}
            Some(_) => out.extend_from_slice(p.payload),
        }
        if let Some(n) = want.filter(|n| out.len() >= *n) {
            // A section shorter than its own header and CRC says nothing.
            return out.get(3..n.checked_sub(4)?).map(<[u8]>::to_vec);
        }
    }
    None
}

/// A stream of the program, as its PMT entry names it.
#[derive(Debug)]
struct Stream {
    pid: u16,
    kind: Kind,
    language: Option<String>,
}

#[derive(Debug, PartialEq)]
enum Kind {
    Video(VideoCodec),
    /// ffmpeg's name for the codec. Never Matroska's or MP4's names, so a session never takes an MPEG-TS track for one
    /// it copies (`session::dolby_codec`): with the start of a run cut to a keyframe of the grid, the audio has to be
    /// cut there too, which only a re-encode does.
    Audio(String),
    Other,
}

/// What the PMT says each stream is: its stream type, else — for private data (0x06) — the descriptors that name
/// AC-3, E-AC-3 or DTS, as ffmpeg's demuxer reads them.
fn kind(stream_type: u8, descriptors: &[(u8, &[u8])]) -> Kind {
    let has = |tag: u8| descriptors.iter().any(|(t, _)| *t == tag);
    let registered = |id: &[u8]| descriptors.iter().any(|(t, d)| *t == 0x05 && d.get(..4) == Some(id));
    let audio = |name: &str| Kind::Audio(name.into());
    match stream_type {
        0x02 => Kind::Video(VideoCodec::Other("mpeg2video".into())),
        0x1b => Kind::Video(VideoCodec::H264),
        0x24 => Kind::Video(VideoCodec::Hevc),
        0x01 => Kind::Video(VideoCodec::Other("mpeg1video".into())),
        0x03 | 0x04 => audio("mp2"),
        0x0f => audio("aac"),
        0x11 => audio("aac_latm"),
        0x81 => audio("ac3"),
        0x84 | 0x87 => audio("eac3"),
        0x82 | 0x85 | 0x86 | 0x8a => audio("dts"),
        0x83 => audio("truehd"),
        0x06 if has(0x6a) || registered(b"AC-3") => audio("ac3"),
        0x06 if has(0x7a) || registered(b"EAC3") => audio("eac3"),
        0x06 if has(0x7b) || registered(b"DTS1") || registered(b"DTS2") || registered(b"DTS3") => {
            audio("dts")
        }
        0x06 if registered(b"HEVC") => Kind::Video(VideoCodec::Hevc),
        _ => Kind::Other,
    }
}

/// The streams of the first program the PAT names, from its PMT.
fn program(head: &[u8]) -> Option<Vec<Stream>> {
    let pat = section(head, 0, 0x00)?;
    // After the transport stream id, version and section numbers: four bytes a program, number 0 the network's.
    let pmt_pid = pat.get(5..)?.chunks_exact(4).find_map(|e| {
        let number = u16::from_be_bytes([e[0], e[1]]);
        (number != 0).then(|| u16::from_be_bytes([e[2] & 0x1f, e[3]]))
    })?;
    let pmt = section(head, pmt_pid, 0x02)?;
    let info_len = (u16::from_be_bytes([*pmt.get(7)?, *pmt.get(8)?]) & 0x0fff) as usize;
    let mut at = 9 + info_len;
    let mut out = Vec::new();
    while at + 5 <= pmt.len() {
        let stream_type = pmt[at];
        let pid = u16::from_be_bytes([pmt[at + 1] & 0x1f, pmt[at + 2]]);
        let es_len = (u16::from_be_bytes([pmt[at + 3], pmt[at + 4]]) & 0x0fff) as usize;
        let es = pmt.get(at + 5..at + 5 + es_len)?;
        let mut descriptors = Vec::new();
        let mut d = es;
        while let [tag, len, rest @ ..] = d {
            let Some(body) = rest.get(..*len as usize) else { break };
            descriptors.push((*tag, body));
            d = &rest[*len as usize..];
        }
        let language = descriptors
            .iter()
            .find(|(t, d)| *t == 0x0a && d.len() >= 3)
            .map(|(_, d)| String::from_utf8_lossy(&d[..3]).to_ascii_lowercase())
            .filter(|l| l.bytes().all(|b| b.is_ascii_lowercase()) && l != "und");
        out.push(Stream { pid, kind: kind(stream_type, &descriptors), language });
        at += 5 + es_len;
    }
    Some(out)
}

/// A PES packet's presentation timestamp, from the payload of the transport packet that starts it.
fn pts(payload: &[u8]) -> Option<u64> {
    if payload.get(..3)? != [0, 0, 1] || payload.get(7)? & 0x80 == 0 {
        return None;
    }
    let b = payload.get(9..14)?;
    Some(
        ((b[0] as u64 >> 1) & 7) << 30
            | (b[1] as u64) << 22
            | (b[2] as u64 >> 1) << 15
            | (b[3] as u64) << 7
            | b[4] as u64 >> 1,
    )
}

/// The elementary stream's bytes in a PES packet: past its header.
fn es_data(payload: &[u8]) -> &[u8] {
    match payload.get(8) {
        Some(&n) if payload.get(..3) == Some(&[0, 0, 1]) => payload.get(9 + n as usize..).unwrap_or(&[]),
        _ => &[],
    }
}

/// Every PES timestamp of the program's streams in `b`, by stream, in the order they were read.
fn timestamps(b: &[u8], streams: &[Stream]) -> Vec<Vec<u64>> {
    let mut out = vec![Vec::new(); streams.len()];
    for p in packets(b).filter(|p| p.start) {
        if let Some(i) = streams.iter().position(|s| s.pid == p.pid && s.kind != Kind::Other) {
            out[i].extend(pts(p.payload));
        }
    }
    out
}

/// The picture's size and codec string from the first picture's own headers: an H.264 SPS, an MPEG-2 sequence header.
/// An HEVC SPS gives its colours instead (whether it is HDR, and HLG), which a transcode has to tone-map; its size is
/// left to ffmpeg (0 × 0).
struct Picture {
    width: u32,
    height: u32,
    codecs: Option<String>,
    frame_rate: Option<f64>,
    hdr: bool,
    hlg: bool,
}

fn picture(codec: &VideoCodec, es: &[u8]) -> Picture {
    let mut out = Picture { width: 0, height: 0, codecs: None, frame_rate: None, hdr: false, hlg: false };
    // Annex B: each unit after a 00 00 01 start code.
    let mut units = es.windows(3).enumerate().filter(|(_, w)| *w == [0, 0, 1]).map(|(i, _)| i + 3);
    match codec {
        VideoCodec::H264 => {
            let found = units.find(|&at| es.get(at).is_some_and(|b| b & 0x1f == 7));
            if let Some(sps) = found.and_then(|at| es.get(at + 1..)) {
                let rbsp = super::unescape(&sps[..sps.len().min(256)]);
                if let Some((w, h)) = avc_size(&rbsp) {
                    (out.width, out.height) = (w, h);
                }
                out.codecs = rbsp.get(..3).map(|b| format!("avc1.{:02x}{:02x}{:02x}", b[0], b[1], b[2]));
            }
        }
        VideoCodec::Hevc => {
            let found = units.find(|&at| es.get(at).is_some_and(|b| (b >> 1) & 0x3f == 33));
            if let Some(sps) = found.and_then(|at| es.get(at + 2..)) {
                if let Some(c) = super::sps_colour(&super::unescape(&sps[..sps.len().min(512)])) {
                    out.hdr = super::is_hdr(c.transfer, c.primaries, c.matrix);
                    out.hlg = c.transfer == 18;
                }
            }
        }
        VideoCodec::Other(_) => {
            // sequence_header_code 0xB3: 12 bits of width, 12 of height, the aspect ratio and a frame rate code.
            let found = units.find(|&at| es.get(at) == Some(&0xb3));
            if let Some(b) = found.and_then(|at| es.get(at + 1..at + 5)) {
                out.width = (b[0] as u32) << 4 | (b[1] as u32) >> 4;
                out.height = (b[1] as u32 & 0x0f) << 8 | b[2] as u32;
                const RATES: [f64; 9] =
                    [0.0, 24000.0 / 1001.0, 24.0, 25.0, 30000.0 / 1001.0, 30.0, 50.0, 60000.0 / 1001.0, 60.0];
                out.frame_rate = RATES.get((b[3] & 0x0f) as usize).copied().filter(|r| *r > 0.0);
            }
        }
        _ => {}
    }
    out
}

/// An H.264 SPS's picture size (H.264 7.3.2.1.1), cropped, from its payload after the NAL unit header.
fn avc_size(rbsp: &[u8]) -> Option<(u32, u32)> {
    let mut r = super::Bits { b: rbsp, pos: 0 };
    let profile = r.read(8)?;
    r.read(16)?; // constraint flags, level_idc
    r.uvlc()?; // seq_parameter_set_id
    let mut chroma = 1;
    if matches!(profile, 100 | 110 | 122 | 244 | 44 | 83 | 86 | 118 | 128 | 138 | 139 | 134 | 135) {
        chroma = r.uvlc()?;
        if chroma == 3 {
            r.read(1)?; // separate_colour_plane_flag
        }
        r.uvlc()?; // bit_depth_luma_minus8
        r.uvlc()?; // bit_depth_chroma_minus8
        r.read(1)?; // qpprime_y_zero_transform_bypass_flag
        if r.flag()? {
            for i in 0..if chroma == 3 { 12 } else { 8 } {
                if r.flag()? {
                    // scaling_list(): delta_scale until the next scale is 0.
                    let (mut last, mut next) = (8i64, 8i64);
                    for _ in 0..if i < 6 { 16 } else { 64 } {
                        if next != 0 {
                            next = (last + se(&mut r)? + 256) % 256;
                        }
                        last = if next == 0 { last } else { next };
                    }
                }
            }
        }
    }
    r.uvlc()?; // log2_max_frame_num_minus4
    match r.uvlc()? {
        0 => {
            r.uvlc()?; // log2_max_pic_order_cnt_lsb_minus4
        }
        1 => {
            r.read(1)?; // delta_pic_order_always_zero_flag
            se(&mut r)?;
            se(&mut r)?;
            for _ in 0..r.uvlc()?.min(255) {
                se(&mut r)?;
            }
        }
        _ => {}
    }
    r.uvlc()?; // max_num_ref_frames
    r.read(1)?; // gaps_in_frame_num_value_allowed_flag
    let width_mbs = r.uvlc()? + 1;
    let height_units = r.uvlc()? + 1;
    let frame_mbs_only = r.flag()?;
    if !frame_mbs_only {
        r.read(1)?; // mb_adaptive_frame_field_flag
    }
    r.read(1)?; // direct_8x8_inference_flag
    let fields = if frame_mbs_only { 1 } else { 2 };
    let (mut width, mut height) = (width_mbs * 16, fields * height_units * 16);
    if r.flag()? {
        let (left, right, top, bottom) = (r.uvlc()?, r.uvlc()?, r.uvlc()?, r.uvlc()?);
        let (x, y) = match chroma {
            1 => (2, 2 * fields),
            2 => (2, fields),
            _ => (1, fields),
        };
        width = width.checked_sub(x * (left + right))?;
        height = height.checked_sub(y * (top + bottom))?;
    }
    Some((u32::try_from(width).ok()?, u32::try_from(height).ok()?))
}

/// `se(v)`: an exp-Golomb code mapped to 0, 1, −1, 2, −2…
fn se(r: &mut super::Bits<'_>) -> Option<i64> {
    let k = r.uvlc()? as i64;
    Some(if k % 2 == 1 { (k + 1) / 2 } else { -(k / 2) })
}

/// The channels the first frame of an audio track in `es` says it carries: ADTS's channel configuration, AC-3's and
/// E-AC-3's channel mode and LFE, MPEG audio's mode. Stereo where the frame isn't found.
fn channels(codec: &str, es: &[u8]) -> u32 {
    let found = match codec {
        "aac" => es.windows(4).find(|w| w[0] == 0xff && w[1] & 0xf6 == 0xf0).map(|w| {
            let config = ((w[2] & 1) << 2 | w[3] >> 6) as u32;
            if config == 7 {
                8
            } else {
                config
            }
        }),
        "ac3" | "eac3" => {
            es.windows(2).position(|w| w == [0x0b, 0x77]).and_then(|at| ac3_channels(&es[at..]))
        }
        "mp2" => es.windows(4).find(|w| w[0] == 0xff && w[1] & 0xe0 == 0xe0).map(|w| match w[3] >> 6 {
            3 => 1,
            _ => 2,
        }),
        _ => None,
    };
    found.filter(|n| *n > 0).unwrap_or(2)
}

/// AC-3's (A/52 5.3) or E-AC-3's (A/52 E.1.2) frame header: full-bandwidth channels by `acmod`, plus the LFE.
fn ac3_channels(frame: &[u8]) -> Option<u32> {
    const FULL: [u32; 8] = [2, 1, 2, 3, 3, 4, 4, 5];
    let bsid = frame.get(5)? >> 3;
    if bsid > 10 {
        // E-AC-3: strmtyp, substreamid, frmsiz, then fscod and numblkscod (or fscod2), acmod and lfeon.
        let b = frame.get(4)?;
        return Some(FULL[((b >> 1) & 7) as usize] + (b & 1) as u32);
    }
    let mut r = super::Bits { b: frame.get(6..)?, pos: 0 };
    let acmod = r.read(3)? as usize;
    if acmod & 1 == 1 && acmod != 1 {
        r.read(2)?; // cmixlev
    }
    if acmod & 4 != 0 {
        r.read(2)?; // surmixlev
    }
    if acmod == 2 {
        r.read(2)?; // dsurmod
    }
    Some(FULL[acmod] + r.read(1)? as u32)
}

/// The elementary stream bytes of a PID's first PES packets in `b`, up to `cap`.
fn first_es(b: &[u8], pid: u16, cap: usize) -> Vec<u8> {
    let mut out = Vec::new();
    let mut started = false;
    for p in packets(b).filter(|p| p.pid == pid) {
        if p.start {
            started = true;
            out.extend_from_slice(es_data(p.payload));
        } else if started {
            out.extend_from_slice(p.payload);
        }
        if out.len() >= cap {
            break;
        }
    }
    out
}

pub async fn probe(src: &Source<'_>, head: &[u8]) -> Result<MediaInfo, ProbeError> {
    // A run of sync bytes with no program in it is no transport stream this can play.
    let streams = program(head).ok_or_else(|| ProbeError::Unsupported(NEITHER.into()))?;
    let video = streams
        .iter()
        .position(|s| matches!(s.kind, Kind::Video(_)))
        .ok_or_else(|| ProbeError::Unsupported("an MPEG-TS with no video".into()))?;
    let Kind::Video(codec) = &streams[video].kind else { unreachable!() };
    if let VideoCodec::Other(c) = codec {
        if c != "mpeg2video" {
            return Err(ProbeError::Unsupported(format!("video is {c}")));
        }
    }
    let size = src.size().ok_or_else(|| ProbeError::Unsupported("an MPEG-TS of unknown size".into()))?;
    let first = timestamps(head, &streams);
    let zero = first.iter().flatten().copied().min().ok_or(ProbeError::Truncated("MPEG-TS timestamps"))?;
    // Read relative to the first: a timestamp that wrapped past 2^33 is still later, and one less than half the clock's
    // range before it — of a stream whose first packet came after the head, or read again where the tail reaches back
    // into it — is earlier.
    let since = |t: u64| -> i64 {
        let d = ((t + WRAP - zero) % WRAP) as i64;
        if d > (WRAP / 2) as i64 {
            d - WRAP as i64
        } else {
            d
        }
    };
    let video_first =
        first[video].iter().copied().map(since).min().ok_or(ProbeError::Truncated("MPEG-TS video"))?;

    let start = size.saturating_sub(TAIL_BYTES);
    let tail = src.read(start, size - start).await?;
    // The tail starts wherever the read did: the first of three sync bytes 188 apart is a packet's.
    let aligned = (0..PACKET.min(tail.len()))
        .find(|&i| (0..3).all(|k| tail.get(i + k * PACKET) == Some(&0x47)))
        .ok_or(ProbeError::Truncated("the MPEG-TS's end"))?;
    let last = timestamps(&tail[aligned..], &streams);
    let mut video_last: Vec<i64> = last[video].iter().copied().map(since).collect();
    video_last.sort_unstable();
    // A frame's length, from the smallest step between the last pictures' timestamps, so the last one counts whole.
    let frame = video_last.windows(2).map(|w| w[1] - w[0]).filter(|d| *d > 0).min().unwrap_or(0);
    let end = last
        .iter()
        .flatten()
        .copied()
        .map(since)
        .chain(video_last.last().map(|t| t + frame))
        .max()
        .ok_or(ProbeError::Truncated("the MPEG-TS's last timestamps"))?;
    let duration = end as f64 / 90_000.0;
    let first_frame = video_first as f64 / 90_000.0;
    if !(duration > first_frame && duration <= super::GRID_MAX_DURATION) {
        return Err(ProbeError::Unsupported(format!(
            "an MPEG-TS whose timestamps run {first_frame} s to {duration} s"
        )));
    }

    let pic = picture(codec, &first_es(head, streams[video].pid, VIDEO_HEAD));
    let audio = streams
        .iter()
        .filter_map(|s| match &s.kind {
            Kind::Audio(name) => Some(AudioTrack {
                codec: name.clone(),
                language: s.language.clone(),
                channels: channels(name, &first_es(head, s.pid, 4096)),
                name: None,
                default: true,
                commentary: false,
                bytes: None,
            }),
            _ => None,
        })
        .collect();
    Ok(MediaInfo {
        container: "mpegts",
        duration,
        video: codec.clone(),
        codecs: pic.codecs,
        width: pic.width,
        height: pic.height,
        hdr: pic.hdr,
        hlg: pic.hlg,
        frame_rate: pic.frame_rate.or_else(|| (frame > 0).then(|| 90_000.0 / frame as f64)),
        dolby_vision: None,
        dolby_vision_record_mismatch: false,
        dolby_vision_recordless: false,
        audio,
        subtitles: Vec::new(),
        keyframes: super::grid(first_frame, duration),
        // A transcode's GOPs are its encoder's, closed.
        closed_gops: true,
        byte_index: Vec::new(),
        video_bytes: None,
    })
}
