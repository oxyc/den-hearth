//! Immutable media-file assembly and HTTP range serving.

use super::*;

/// A segment's GOP files opened for one response, and the validator that names these bytes.
pub(super) struct Media {
    parts: Vec<(std::fs::File, u64)>,
    pub(super) etag: String,
}

/// Open each GOP file, positioned past the `styp` box of every file but the first: the joined segment
/// is one segment, and a segment type box belongs only at its start.
///
/// The ETag hashes each file's path, length and modification time. A segment made again by a later job (after the
/// window pruned it) is a different run's files, so its bytes — which need not match the first run's — never answer
/// an `If-Range` that names the first.
pub(super) fn open_parts(paths: &[PathBuf]) -> std::io::Result<Media> {
    use sha2::{Digest, Sha256};
    use std::io::{Read, Seek, SeekFrom};
    let mut parts = Vec::with_capacity(paths.len());
    let mut id = Sha256::new();
    for (k, p) in paths.iter().enumerate() {
        let mut f = std::fs::File::open(p)?;
        let meta = f.metadata()?;
        let mut len = meta.len();
        let mtime = meta.modified().ok().and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok());
        id.update(p.as_os_str().as_encoded_bytes());
        id.update(len.to_be_bytes());
        id.update(mtime.map_or(0, |d| d.as_nanos()).to_be_bytes());
        if k > 0 {
            let mut hdr = [0u8; 8];
            f.read_exact(&mut hdr)?;
            let size = u32::from_be_bytes([hdr[0], hdr[1], hdr[2], hdr[3]]) as u64;
            if &hdr[4..8] == b"styp" && size >= 8 && size <= len {
                len -= size;
                f.seek(SeekFrom::Start(size))?;
            } else {
                f.seek(SeekFrom::Start(0))?;
            }
        }
        parts.push((f, len));
    }
    let digest = id.finalize();
    let etag = format!("\"{}\"", digest[..12].iter().map(|b| format!("{b:02x}")).collect::<String>());
    Ok(Media { parts, etag })
}

/// The one byte range a request asks for, inclusive, within a body of `len` bytes: `Ok(None)` for the whole body
/// (no `Range`, one not in bytes, several ranges, or an `If-Range` that doesn't name these bytes), `Err(())` for a
/// range that starts past the end.
pub(super) fn wanted_range(
    headers: &hyper::HeaderMap,
    etag: &str,
    len: u64,
) -> Result<Option<(u64, u64)>, ()> {
    let Some(spec) = headers.get(hyper::header::RANGE).and_then(|v| v.to_str().ok()) else {
        return Ok(None);
    };
    // A validator that isn't this body's ETag (a date, or another run's) asks for the whole of what is here now.
    if let Some(v) = headers.get(hyper::header::IF_RANGE) {
        if v.to_str().ok().map(str::trim) != Some(etag) {
            return Ok(None);
        }
    }
    let Some(spec) = spec.trim().strip_prefix("bytes=") else {
        return Ok(None);
    };
    if spec.contains(',') {
        return Ok(None);
    }
    let Some((a, b)) = spec.trim().split_once('-') else {
        return Ok(None);
    };
    let (a, b) = (a.trim(), b.trim());
    let range = match (a.parse::<u64>(), b.parse::<u64>()) {
        (Ok(start), Ok(end)) if start <= end => (start, end.min(len.saturating_sub(1))),
        (Ok(start), Err(_)) if b.is_empty() => (start, len.saturating_sub(1)),
        (Err(_), Ok(suffix)) if a.is_empty() && suffix > 0 => {
            (len.saturating_sub(suffix), len.saturating_sub(1))
        }
        _ => return Ok(None),
    };
    if range.0 >= len {
        return Err(());
    }
    Ok(Some(range))
}

/// The parts cut to bytes `start..=end` of their joined body: the files before `start` dropped, the first one kept
/// sought forward to it, and the lengths capped at `end`.
pub(super) fn slice_parts(
    parts: Vec<(std::fs::File, u64)>,
    start: u64,
    end: u64,
) -> std::io::Result<Vec<(std::fs::File, u64)>> {
    use std::io::{Seek, SeekFrom};
    let mut out = Vec::new();
    let (mut skip, mut left) = (start, end - start + 1);
    for (mut f, len) in parts {
        if left == 0 {
            break;
        }
        if skip >= len {
            skip -= len;
            continue;
        }
        f.seek(SeekFrom::Current(skip as i64))?;
        let take = (len - skip).min(left);
        out.push((f, take));
        left -= take;
        skip = 0;
    }
    Ok(out)
}

/// `init.mp4` or a segment. Kept until the session expires at `exp`: its URL is signed for this session alone, and
/// a player that seeks back to a segment already pruned from scratch takes it from its cache rather than restarting
/// ffmpeg for it.
///
/// Only a finished segment is ever served (`ready` waits for the last of its GOPs), so a `Range` of it is honoured:
/// a player whose connection broke partway asks for the rest (`bytes=N-`) rather than the whole again, with
/// `If-Range` naming the ETag of what it already holds.
pub(super) fn media_response(
    media: Media,
    head: bool,
    exp: u64,
    headers: &hyper::HeaderMap,
) -> Response<Body> {
    let len: u64 = media.parts.iter().map(|(_, l)| l).sum();
    let cache_control = format!("private, max-age={}, immutable", exp.saturating_sub(unix_now()));
    let b = Response::builder()
        .header("content-type", "video/mp4")
        .header("cache-control", cache_control)
        .header("accept-ranges", "bytes")
        .header("etag", &media.etag);
    let (status, range, parts) = match wanted_range(headers, &media.etag, len) {
        Err(()) => {
            return b
                .status(StatusCode::RANGE_NOT_SATISFIABLE)
                .header("content-range", format!("bytes */{len}"))
                .header("content-length", 0)
                .body(httputil::full(""))
                .unwrap();
        }
        Ok(None) => (StatusCode::OK, None, media.parts),
        Ok(Some((start, end))) => match slice_parts(media.parts, start, end) {
            Ok(parts) => (StatusCode::PARTIAL_CONTENT, Some((start, end)), parts),
            Err(_) => return busy(),
        },
    };
    let sent: u64 = parts.iter().map(|(_, l)| l).sum();
    let body = if head { httputil::full("") } else { httputil::files_body(parts) };
    let mut b = b.status(status).header("content-length", sent);
    if let Some((start, end)) = range {
        b = b.header("content-range", format!("bytes {start}-{end}/{len}"));
    }
    b.body(body).unwrap()
}

pub fn gone() -> Response<Body> {
    httputil::error(StatusCode::GONE, "session_ended", "This session has ended; start a new one.")
}

pub fn busy() -> Response<Body> {
    httputil::json(
        StatusCode::SERVICE_UNAVAILABLE,
        &serde_json::json!({"error": "not_ready", "detail": "The segment is still being made."}),
        &[("retry-after", "2"), ("cache-control", "no-store")],
    )
}

impl Session {
    pub async fn serve_segment(
        self: &Arc<Self>,
        st: &Arc<AppState>,
        n: usize,
        head: bool,
        headers: &hyper::HeaderMap,
    ) -> Response<Body> {
        let deadline = Instant::now() + SEGMENT_WAIT;
        loop {
            self.reresolve_if_needed(st).await;
            let (paths, start) = {
                let mut i = self.lock();
                if i.ended {
                    return gone();
                }
                i.last_seen = Instant::now();
                i.want = n;
                self.refresh(&mut i, st);
                let ready = self.ready(&i, n);
                let start = match ready.is_none().then(|| self.producer_needed(&i, n)).transpose() {
                    Ok(Some(needed)) => needed,
                    Ok(None) => false,
                    Err(()) => return source_failed(),
                };
                // A rebuilt session serving from scratch: once what scratch holds runs out within `RESUME_LEAD`
                // segments of this one, its release is fetched and a run started there, while the player still
                // plays what it has.
                if ready.is_some() && self.door.get().is_none() && !i.warming {
                    let last = self.segments.len() - 1;
                    if let Some(m) =
                        (n + 1..=(n + RESUME_LEAD).min(last)).find(|&m| self.ready(&i, m).is_none())
                    {
                        i.warming = true;
                        let (s, st) = (self.clone(), st.clone());
                        tokio::spawn(async move {
                            if s.ensure_resolved(&st).await.is_ok() {
                                let _ = s.start_producer(&st, m, Instant::now() + SEGMENT_WAIT).await;
                            }
                            s.lock().warming = false;
                        });
                    }
                }
                self.gate(&mut i, st);
                (ready, start)
            };
            self.wake.notify_one();
            if let Some(paths) = paths {
                // Opened before pruning, so a GOP deleted from the window keeps serving this body.
                if let Ok(Ok(media)) = tokio::task::spawn_blocking(move || open_parts(&paths)).await {
                    {
                        let mut i = self.lock();
                        self.prune(&mut i, n, st);
                        self.gate(&mut i, st);
                        // Not on a HEAD: it never carries the body, so counting it would overstate what
                        // actually reached the player — the end-of-session summary line's `segments`/`seconds`.
                        if !head {
                            let seg = self.segments[n];
                            i.segments_served += 1;
                            i.seconds_served += seg.end - seg.start;
                        }
                    }
                    return media_response(media, head, self.exp, headers);
                }
            }
            if start && self.start_producer(st, n, deadline).await.is_err() && Instant::now() >= deadline {
                return busy();
            }
            if Instant::now() >= deadline {
                return busy();
            }
            tokio::time::sleep(POLL).await;
        }
    }

    pub async fn serve_init(
        &self,
        st: &Arc<AppState>,
        head: bool,
        headers: &hyper::HeaderMap,
    ) -> Response<Body> {
        let deadline = Instant::now() + SEGMENT_WAIT;
        loop {
            self.reresolve_if_needed(st).await;
            let (init, start) = {
                let mut i = self.lock();
                if i.ended {
                    return gone();
                }
                i.last_seen = Instant::now();
                self.refresh(&mut i, st);
                let start = match i.init.is_none().then(|| self.producer_needed(&i, i.want)).transpose() {
                    Ok(Some(needed)) => needed,
                    Ok(None) => false,
                    Err(()) => return source_failed(),
                };
                self.gate(&mut i, st);
                (i.init.clone(), start)
            };
            self.wake.notify_one();
            if let Some(path) = init {
                if let Ok(Ok(media)) = tokio::task::spawn_blocking(move || open_parts(&[path])).await {
                    return media_response(media, head, self.exp, headers);
                }
            }
            if start {
                let want = self.lock().want;
                let _ = self.start_producer(st, want, deadline).await;
            }
            if Instant::now() >= deadline {
                return busy();
            }
            tokio::time::sleep(POLL).await;
        }
    }

    /// Start the job for the segment the session opens on and wait, up to `INIT_WAIT`, for its `init.mp4`, so a
    /// native player's first request for it is answered at once.
    ///
    /// Apple's players give up on the map after about five seconds — AVFoundation's "No response for map", which
    /// Safari reports as "Media failed to decode" — and a job that seeks into a remote file, and perhaps starts a
    /// transcode, takes longer than that to write one. hls.js waits as long as it takes. Whatever stops this early
    /// (a run that fails, a job that won't start) is left to the player's own request, which handles it as before.
    pub async fn prepare(&self, st: &Arc<AppState>) {
        let deadline = Instant::now() + INIT_WAIT;
        loop {
            let start = {
                let mut i = self.lock();
                if i.ended {
                    return;
                }
                self.refresh(&mut i, st);
                if i.init.is_some() || i.failures > 0 {
                    return;
                }
                let want = i.want;
                let start = self.producer_needed(&i, want).unwrap_or(false);
                self.gate(&mut i, st);
                start
            };
            self.wake.notify_one();
            if start {
                let want = self.lock().want;
                let _ = self.start_producer(st, want, deadline).await;
            }
            if Instant::now() >= deadline {
                // The session is answered anyway, and Safari will likely give up on the map before it is written.
                eprintln!(
                    "session {}: init.mp4 not written after {}s; answering without it{}",
                    self.short(),
                    INIT_WAIT.as_secs(),
                    rid_tail(self.rid.as_deref())
                );
                return;
            }
            tokio::time::sleep(POLL).await;
        }
    }

    /// Whether the session's `init.mp4` has been written. Tests use it to see what `prepare` left behind.
    #[cfg(test)]
    pub fn init_ready(&self) -> bool {
        self.lock().init.is_some()
    }
}

pub(super) fn source_failed() -> Response<Body> {
    httputil::error(
        StatusCode::BAD_GATEWAY,
        "source_failed",
        "The release stopped answering; start a new session.",
    )
}
