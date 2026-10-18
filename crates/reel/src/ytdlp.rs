//! The yt-dlp/ffmpeg subprocess layer: probe a candidate (fast, no download), download+mux a
//! faststart MP4, and map yt-dlp's stderr to an HTTP status so `/play` can say *why* it failed.

use std::path::Path;
use std::process::Stdio;
use std::time::Duration;

use tokio::io::{AsyncRead, AsyncReadExt};
use tokio::process::Command;

use crate::config::Config;

const PROBE_TIMEOUT_SECS: u64 = 30; // yt-dlp --simulate should be quick; backstop a hang
const DOWNLOAD_TIMEOUT_SECS: u64 = 240; // download+mux backstop (yt-dlp also gets --socket-timeout)
/// yt-dlp is normally quiet: stdout is one format line and stderr is empty or a short error. Keep a
/// generous diagnostic tail from either pipe, but never let a runaway child reserve memory in
/// proportion to what it prints (there may be three downloads at once).
pub(crate) const CHILD_OUTPUT_MAX_BYTES: usize = 64 * 1024;
/// Metadata probes print only a line or a handful of ids. This is deliberately separate from the
/// download diagnostic ceiling so callers have to choose a bound appropriate to their protocol.
pub(crate) const PROBE_OUTPUT_MAX_BYTES: usize = 16 * 1024;
/// A direct resolve also prints the format inventory's HLS manifest URL field. It is usually only a
/// few KiB, but can be appreciably larger than the probe protocol without being pathological.
pub(crate) const RESOLVE_OUTPUT_MAX_BYTES: usize = 256 * 1024;
/// ffmpeg cropdetect writes one diagnostic record per sampled keyframe. Keep all normal diagnostics
/// available to the parser while bounding a corrupt/noisy input independently of process lifetime.
pub(crate) const TOOL_OUTPUT_MAX_BYTES: usize = 256 * 1024;
/// How long to wait for yt-dlp's stderr pipe to close after it exits, before assuming a descendant
/// is holding it open and killing the group. Long enough for a normal exit, short enough that a
/// stuck one costs a download slot for seconds rather than the full download timeout.
const STDERR_DRAIN_GRACE: Duration = Duration::from_secs(5);
/// A process outside the child's group can inherit a pipe before daemonising. Once the group has
/// been killed, EOF is therefore no longer something we may wait for indefinitely.
const POST_KILL_DRAIN_GRACE: Duration = Duration::from_millis(500);
const _: () = assert!(
    STDERR_DRAIN_GRACE.as_secs() * 4 < DOWNLOAD_TIMEOUT_SECS,
    "the stderr grace must stay well inside the download timeout, or it is unreachable"
);

/// A typed `/play` failure: an HTTP status + a short machine reason + a user-facing message.
/// Clone-able because the in-flight de-dupe shares one download future across waiters.
#[derive(Clone, Debug)]
pub struct PlayError {
    pub status: u16,
    pub reason: String,
    pub message: String,
    /// Diagnostic detail (last of stderr / spawn error) — logged, never sent to the client.
    pub detail: String,
}

impl PlayError {
    fn spawn(e: std::io::Error) -> PlayError {
        PlayError {
            status: 502,
            reason: "extraction_failed".into(),
            message: "Could not fetch this trailer.".into(),
            detail: format!("spawn yt-dlp: {e}"),
        }
    }
    /// yt-dlp exited 0 but we do not trust what is on disk — no file, or something was still
    /// writing it when the grace expired. A LOCAL failure, deliberately not `extraction_failed`:
    /// that reason feeds /health's systemic signal, and telling the operator to bump yt-dlp for a
    /// grace kill or a full disk points at the wrong thing entirely.
    pub(crate) fn incomplete(detail: String) -> PlayError {
        PlayError {
            status: 502,
            reason: "incomplete_download".into(),
            message: "Could not fetch this trailer.".into(),
            detail,
        }
    }
    /// The clap bake was killed part-way through an in-place rewrite, so the file on disk cannot be
    /// trusted. Local, like `incomplete`: nothing to do with the extractor.
    pub(crate) fn bake_interrupted() -> PlayError {
        PlayError {
            status: 502,
            reason: "incomplete_download".into(),
            message: "Could not fetch this trailer.".into(),
            detail: "the crop bake was interrupted mid-rewrite".into(),
        }
    }
    /// Too many distinct ids already have a download outstanding. A 503 rather than a 502: nothing
    /// is wrong with the video or the extractor, this instance is simply full, and the same request
    /// a minute later will very likely work. Deliberately not cached in `play_fails` — it says
    /// nothing about the id, and pinning it would turn a busy minute into a dead trailer.
    pub(crate) fn overloaded() -> PlayError {
        PlayError {
            status: 503,
            reason: "busy".into(),
            message: "Too many trailers are being fetched right now.".into(),
            detail: "in-flight download cap reached".into(),
        }
    }
    /// YouTube is throttling this box, so no new extraction is started until the pause lifts. A 503
    /// like `overloaded`, for the same reason: it says nothing about the video, and is never
    /// remembered against the id.
    pub(crate) fn throttled() -> PlayError {
        PlayError {
            status: 503,
            reason: "throttled".into(),
            message: THROTTLED_MESSAGE.into(),
            detail: "extractions paused: YouTube is throttling this server".into(),
        }
    }
    fn timed_out() -> PlayError {
        PlayError {
            status: 504,
            reason: "timeout".into(),
            message: "This trailer took too long to fetch.".into(),
            detail: format!("yt-dlp exceeded {DOWNLOAD_TIMEOUT_SECS}s"),
        }
    }

    pub(crate) fn panicked(operation: &str) -> PlayError {
        PlayError {
            status: 502,
            reason: "internal_failure".into(),
            message: "Could not fetch this trailer.".into(),
            detail: format!("{operation} panicked; its shared flight was discarded"),
        }
    }

    fn output_too_large(stream: &str, tail: &[u8]) -> PlayError {
        PlayError {
            status: 502,
            reason: "extraction_failed".into(),
            message: "Could not fetch this trailer.".into(),
            detail: format!(
                "yt-dlp {stream} exceeded {CHILD_OUTPUT_MAX_BYTES} bytes; tail: {}",
                stderr_tail(tail)
            ),
        }
    }
}

const THROTTLED_MESSAGE: &str = "Trailers can't be fetched right now. Try again later.";

/// Is this YouTube refusing the BOX rather than the video? A 429, the "confirm you're not a bot"
/// wall (spelled with a straight or a typographic apostrophe, depending on the release), or its
/// session rate-limit notice. All of them fail every video alike until they lift.
fn is_throttle(lowered: &str) -> bool {
    [
        "http error 429",
        "too many requests",
        "confirm you're not a bot",
        "confirm you\u{2019}re not a bot",
        "rate-limit",
        "rate limit",
    ]
    .iter()
    .any(|p| lowered.contains(p))
}

/// Map a yt-dlp failure to an HTTP status + short reason (the cause is in stderr; match the common
/// YouTube ones). Anything unrecognized is a blanket 502.
pub fn classify(code: Option<i32>, stderr: &str) -> PlayError {
    let s = stderr.to_lowercase();
    // First: a throttle's message can carry wording the per-video branches below would match.
    let (status, reason, message) = if is_throttle(&s) {
        (503, "throttled", THROTTLED_MESSAGE)
    } else if s.contains("available in your country")
        || s.contains("available in your location")
        || s.contains("blocked it in your country")
        || s.contains("not available from your location")
    {
        (451, "geo_blocked", "This trailer is not available in your region.")
    } else if s.contains("private video")
        || s.contains("sign in to confirm your age")
        || s.contains("age-restricted")
        || s.contains("members-only")
    {
        (403, "restricted", "This trailer is private or age-restricted.")
    } else if s.contains("video unavailable")
        || s.contains("has been removed")
        || s.contains("no longer available")
        || s.contains("does not exist")
        || s.contains("removed by the uploader")
    {
        (404, "unavailable", "This trailer is no longer available.")
    } else {
        (502, "extraction_failed", "Could not fetch this trailer.")
    };
    let tail: String = stderr.chars().rev().take(300).collect::<Vec<_>>().into_iter().rev().collect();
    PlayError {
        status,
        reason: reason.into(),
        message: message.into(),
        detail: format!("yt-dlp exit {code:?}: {tail}"),
    }
}

fn watch_url(vid: &str) -> String {
    format!("https://www.youtube.com/watch?v={vid}")
}

/// yt-dlp's chosen format for one resolve or download: enough to say *why* a trailer looks the way
/// it does (silent, soft, cropped to the wrong rung) without re-running yt-dlp to find out.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Format {
    pub format_id: String,
    pub height: Option<u32>,
    pub vcodec: String,
    pub acodec: String,
}

impl Format {
    /// `acodec` reads `"none"` for a video-only format — the common case now that YouTube answers
    /// adaptively — and empty when yt-dlp printed nothing at all.
    pub(crate) fn has_audio(&self) -> bool {
        !self.acodec.is_empty() && self.acodec != "none"
    }
}

/// Parse the `%(format_id)s|%(height)s|%(vcodec)s|%(acodec)s` print line. Best-effort and never
/// fatal to the download it describes: an unparsable or missing line just means the decision log
/// carries no format for this one.
pub(crate) fn parse_format(line: &str) -> Option<Format> {
    let mut parts = line.trim().splitn(4, '|');
    let format_id = parts.next()?.to_string();
    let height = parts.next().and_then(|h| h.parse().ok());
    let vcodec = parts.next()?.to_string();
    let acodec = parts.next()?.to_string();
    (!format_id.is_empty()).then_some(Format { format_id, height, vcodec, acodec })
}

/// One decision-log line for a yt-dlp cache fill (`/direct`'s resolve cache) or download (`/play`'s
/// on-disk trailer cache): a cache hit costs nothing further, a miss ran yt-dlp and — on success —
/// chose `format`. `vid` and `format` are identity fields (`LOG_IDENTITY`, on by default); the rest
/// of the line — event, outcome, reason, duration — is written either way.
pub(crate) fn cache_line(
    cfg: &Config,
    event: &str,
    outcome: &str,
    reason: &str,
    dur_ms: u128,
    vid: &str,
    format: Option<&Format>,
) -> String {
    let mut line =
        format!("event={event} outcome={outcome} reason={reason} upstream=youtube dur_ms={dur_ms}");
    if cfg.log_identity {
        line.push_str(&format!(" vid={vid}"));
        if let Some(f) = format {
            line.push_str(&format!(
                " format_id={} height={} vcodec={} acodec={} has_audio={}",
                f.format_id,
                f.height.map(|h| h.to_string()).unwrap_or_else(|| "?".into()),
                f.vcodec,
                f.acodec,
                f.has_audio()
            ));
        }
    }
    line
}

/// Append the configured YouTube `--extractor-args` (the player-client override) to a yt-dlp command
/// when one is set. Applied to every command that actually extracts a video (probe + download) so the
/// probe validates exactly what playback fetches. yt-dlp accepts options after the URL, so this can be
/// appended to an already-built command.
fn apply_extractor_args(cmd: &mut Command, cfg: &Config) {
    if let Some(ea) = &cfg.ytdlp_extractor_args {
        cmd.args(["--extractor-args", ea]);
    }
}

/// Does yt-dlp think this id is extractable HERE (right region, decodable formats)? Fast:
/// Outcome of probing a candidate without downloading: whether yt-dlp can extract it here, and (when
/// known) whether it's landscape. So the resolver can prefer a landscape trailer over a portrait one —
/// a portrait trailer plays as a tall sliver on the landscape billboard. Unknown dimensions default to
/// landscape, so we never skip a good trailer over missing metadata.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Probe {
    Unplayable,
    Playable { landscape: bool },
}

/// `--print` the selected format's dimensions (implies `--simulate`, so no download) — this both
/// validates extractability (exit 0 ⇔ the format resolves here, same as the old `--simulate`) and
/// yields orientation. Any spawn/exec error, non-zero exit, or timeout → `Unplayable`; exit 0 with
/// unparsable/missing dims → `Playable { landscape: true }` (don't penalise a good trailer).
pub async fn probe(cfg: &Config, vid: &str) -> Probe {
    let cache = cfg.ytdlp_cache.to_string_lossy().into_owned();
    let mut cmd = Command::new(&cfg.ytdlp);
    cmd.args([
        "-q",
        "--no-warnings",
        "--socket-timeout",
        "15",
        "--cache-dir",
        &cache,
        "-f",
        &cfg.ytdlp_format,
        "--print",
        "%(width)s %(height)s",
        &watch_url(vid),
    ])
    .stdin(Stdio::null())
    .stdout(Stdio::piped())
    .stderr(Stdio::piped()) // capture, never swallow — a silent probe error hid a total-outage regression
    .kill_on_drop(true); // cancelled/timed-out probe kills its yt-dlp too
    apply_extractor_args(&mut cmd, cfg);
    match output_in_group(
        &mut cmd,
        Duration::from_secs(PROBE_TIMEOUT_SECS),
        PROBE_OUTPUT_MAX_BYTES,
        PROBE_OUTPUT_MAX_BYTES,
    )
    .await
    {
        Ok(o) if o.status.success() => {
            Probe::Playable { landscape: parse_landscape(&String::from_utf8_lossy(&o.stdout)) }
        }
        Ok(o) => {
            // Non-zero exit. Surface WHY (was swallowed), then fall back to the proven plain
            // `--simulate` extractability gate: if yt-dlp can still extract the title, serve it (unknown
            // orientation → landscape) rather than dropping a good trailer over a `--print`/format quirk.
            // Only a genuine failure (geo-block, removed, bot-check) fails both → Unplayable.
            eprintln!("probe {vid}: --print exit {:?} — {}", o.status.code(), stderr_tail(&o.stderr));
            if probe_extractable(cfg, vid).await {
                eprintln!(
                    "probe {vid}: extractable via --simulate → serving (orientation unknown → landscape)"
                );
                Probe::Playable { landscape: true }
            } else {
                Probe::Unplayable
            }
        }
        Err(BoundedOutputError::Io(e)) => {
            eprintln!("probe {vid}: yt-dlp spawn error — {e}");
            Probe::Unplayable
        }
        Err(BoundedOutputError::TimedOut) => {
            eprintln!("probe {vid}: timed out after {PROBE_TIMEOUT_SECS}s");
            Probe::Unplayable
        }
        Err(e @ BoundedOutputError::TooLarge { .. }) => {
            eprintln!("probe {vid}: {e}");
            Probe::Unplayable
        }
    }
}

/// The proven extractability gate (pre-0.3.2): `--simulate`, exit 0 ⇔ yt-dlp can extract the selected
/// format here. Used as the safety net when the richer `--print` probe fails, so a `--print`/dimension
/// quirk can't drop an otherwise-playable trailer.
async fn probe_extractable(cfg: &Config, vid: &str) -> bool {
    let cache = cfg.ytdlp_cache.to_string_lossy().into_owned();
    let mut cmd = Command::new(&cfg.ytdlp);
    cmd.args([
        "-q",
        "--simulate",
        "--no-warnings",
        "--socket-timeout",
        "15",
        "--cache-dir",
        &cache,
        "-f",
        &cfg.ytdlp_format,
        &watch_url(vid),
    ])
    .stdin(Stdio::null())
    .stdout(Stdio::null())
    .stderr(Stdio::null())
    .kill_on_drop(true);
    apply_extractor_args(&mut cmd, cfg);
    matches!(
        output_in_group(
            &mut cmd,
            Duration::from_secs(PROBE_TIMEOUT_SECS),
            PROBE_OUTPUT_MAX_BYTES,
            PROBE_OUTPUT_MAX_BYTES,
        )
        .await,
        Ok(output) if output.status.success()
    )
}

/// YouTube-search fallback: `yt-dlp "ytsearchN:<query>"` → up to `n` video ids (flat, no per-video
/// extraction, no download). Used when TMDB/KinoCheck carry no trailer for a title; the ids are then
/// probed like any other candidate. Empty on any error (logged, never swallowed).
/// `None` means the search could not be run (spawn error, non-zero exit, timeout) — distinct from
/// `Some(vec![])`, "YouTube has nothing". The caller negative-caches an empty answer, so collapsing
/// the two pinned "this title has no trailer" for an hour every time yt-dlp was broken.
pub async fn search(cfg: &Config, query: &str, n: usize) -> Option<Vec<String>> {
    let mut cmd = Command::new(&cfg.ytdlp);
    cmd.args([
        "-q",
        "--no-warnings",
        "--flat-playlist",
        "--socket-timeout",
        "15",
        "--print",
        "id",
        &format!("ytsearch{n}:{query}"),
    ])
    .stdin(Stdio::null())
    .stdout(Stdio::piped())
    .stderr(Stdio::piped())
    .kill_on_drop(true);
    match output_in_group(
        &mut cmd,
        Duration::from_secs(PROBE_TIMEOUT_SECS),
        PROBE_OUTPUT_MAX_BYTES,
        PROBE_OUTPUT_MAX_BYTES,
    )
    .await
    {
        Ok(o) if o.status.success() => String::from_utf8_lossy(&o.stdout)
            .lines()
            .map(|l| l.trim().to_string())
            // Same gate the TMDB and KinoCheck candidates get: this is the third source of ids and
            // they all end up as filenames.
            .filter(|l| crate::is_valid_vid(l))
            .collect::<Vec<_>>()
            .into(),
        Ok(o) => {
            crate::log_limited("search exit", || {
                format!("search {query:?}: yt-dlp exit {:?} — {}", o.status.code(), stderr_tail(&o.stderr))
            });
            None
        }
        Err(BoundedOutputError::Io(e)) => {
            crate::log_limited("search spawn", || format!("search {query:?}: yt-dlp spawn error — {e}"));
            None
        }
        Err(BoundedOutputError::TimedOut) => {
            crate::log_limited("search timeout", || {
                format!("search {query:?}: timed out after {PROBE_TIMEOUT_SECS}s")
            });
            None
        }
        Err(e @ BoundedOutputError::TooLarge { .. }) => {
            crate::log_limited("search output", || format!("search {query:?}: {e}"));
            None
        }
    }
}

/// Last ~200 chars of stderr on one line, for a compact diagnostic log.
pub(crate) fn stderr_tail(stderr: &[u8]) -> String {
    let s = String::from_utf8_lossy(stderr);
    let tail: String = s.chars().rev().take(200).collect::<Vec<_>>().into_iter().rev().collect();
    tail.replace('\n', " ").trim().to_string()
}

/// Parse yt-dlp's `"W H"` print → is it landscape (`w >= h`)? Missing/unparsable dims → `true`.
pub fn parse_landscape(s: &str) -> bool {
    let mut it = s.split_whitespace();
    match (it.next().and_then(|w| w.parse::<u32>().ok()), it.next().and_then(|h| h.parse::<u32>().ok())) {
        (Some(w), Some(h)) => w >= h,
        _ => true,
    }
}

/// Run yt-dlp+ffmpeg to produce a faststart MP4 at `tmp`. Returns the format yt-dlp chose iff the
/// process exited 0 and wrote a non-empty file; otherwise a classified [`PlayError`]. Caller owns
/// `tmp`'s lifecycle (rename on success, unlink on failure).
///
/// A googlevideo 403 part-way through is run once more. Each run extracts afresh, and on 2026-10-11
/// about one extraction in four (3 of 13) came back with URLs refused past ~60 s of media, refused
/// again ten minutes later, while the next extraction of the same video served to the end — see
/// `direct::refuses_its_end`. 2 of 8 hand-run downloads failed that way the same day.
pub async fn download_to(cfg: &Config, vid: &str, tmp: &Path) -> Result<Format, PlayError> {
    match download_once(cfg, vid, tmp).await {
        Err(e) if e.reason == "extraction_failed" && e.detail.contains("HTTP Error 403") => {
            eprintln!("[{vid}] googlevideo refused part of the download (403); downloading again");
            download_once(cfg, vid, tmp).await
        }
        done => done,
    }
}

async fn download_once(cfg: &Config, vid: &str, tmp: &Path) -> Result<Format, PlayError> {
    let cache = cfg.ytdlp_cache.to_string_lossy().into_owned();
    let tmp_s = tmp.to_string_lossy().into_owned();
    // Shared so the guard below can reap the group even when the future is dropped mid-flight.
    let group = std::sync::Arc::new(SharedGroup::new());
    let group_for_work = group.clone();
    let work = async {
        let mut cmd = Command::new(&cfg.ytdlp);
        cmd.args([
            "-q",
            "--no-playlist",
            "--no-warnings",
            "--socket-timeout",
            "15", // yt-dlp aborts stalled sockets itself; the outer timeout is a backstop
            "--cache-dir",
            &cache, // reuse the nsig/player-JS work the probe already did
            "-N",
            "4", // parallel DASH fragments → faster download
            // AVPlayer hardware-decodable: H.264 (avc1) + AAC (mp4a) — same string the probe validates.
            "-f",
            &cfg.ytdlp_format,
            "--merge-output-format",
            "mp4",
            // faststart during the merge's ffmpeg (one pass), not a separate whole-file rewrite.
            "--postprocessor-args",
            "Merger+ffmpeg:-movflags +faststart",
            // What the ladder actually picked — for the decision log, never for playback. Printed
            // once per video, well before the download itself writes anything to stdout.
            "--print",
            "%(format_id)s|%(height)s|%(vcodec)s|%(acodec)s",
            // `--print` implies `--simulate`. Without this the download exited 0 having written
            // nothing — every fresh trailer, measured on the box with yt-dlp 2026.08.19.
            "--no-simulate",
            "-o",
            &tmp_s,
            &watch_url(vid),
        ])
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        // Its own process group, so a timeout can reap the whole tree. yt-dlp forks ffmpeg to do
        // the merge, and kill_on_drop signals only the direct child — the ffmpeg survived, kept
        // writing, and could recreate the temp file we had just deleted.
        .process_group(0)
        .kill_on_drop(true);
        apply_extractor_args(&mut cmd, cfg); // same player-client override the probe validated with
        let mut child = cmd.spawn().map_err(PlayError::spawn)?;
        let pgid = child.id();
        group_for_work.set(pgid);
        register_group(pgid);

        // Drain stderr AND stdout concurrently with wait() so a chatty yt-dlp can't deadlock on a
        // full pipe. Each drain retains only a fixed-size tail. Besides bounding memory, it reports
        // the first overflow to the waiter so a runaway printer is killed immediately rather than
        // being allowed to spend the remainder of the 240-second download timeout.
        let (oversize_tx, mut oversize_rx) = tokio::sync::mpsc::channel(2);
        let mut stderr_pipe = child.stderr.take().expect("stderr piped");
        let stderr_tx = oversize_tx.clone();
        let mut drain = tokio::spawn(async move {
            capture_tail(&mut stderr_pipe, "stderr", CHILD_OUTPUT_MAX_BYTES, stderr_tx).await
        });
        let mut stdout_pipe = child.stdout.take().expect("stdout piped");
        let mut drain_stdout = tokio::spawn(async move {
            capture_tail(&mut stdout_pipe, "stdout", CHILD_OUTPUT_MAX_BYTES, oversize_tx).await
        });
        let (status, overflow_seen) = tokio::select! {
            status = child.wait() => (status.map_err(PlayError::spawn)?, None),
            stream = oversize_rx.recv() => {
                // Killing the group closes both inherited pipes and reaps ffmpeg as well as yt-dlp.
                group_for_work.kill();
                (child.wait().await.map_err(PlayError::spawn)?, stream)
            }
        };
        // The stderr pipe is inherited by every descendant, so EOF means "the last of them exited",
        // not "yt-dlp exited". Preserve a short grace for the late diagnostic lines that are part
        // of yt-dlp's effective protocol; unlike the generic grouped runner, classification depends
        // on them. The grace is bounded, and a holder that outlives it is killed before the drain.
        let forced = overflow_seen.is_some();
        let stderr = if forced {
            finish_after_kill(&mut drain).await
        } else {
            match tokio::time::timeout(STDERR_DRAIN_GRACE, &mut drain).await {
                Ok(r) => r.unwrap_or_default(),
                Err(_) => {
                    eprintln!("yt-dlp {vid}: stderr still held after exit; killing the group");
                    group_for_work.kill();
                    finish_after_kill(&mut drain).await
                }
            }
        };
        let stdout = if forced {
            finish_after_kill(&mut drain_stdout).await
        } else {
            match tokio::time::timeout(STDERR_DRAIN_GRACE, &mut drain_stdout).await {
                Ok(r) => r.unwrap_or_default(),
                Err(_) => {
                    group_for_work.kill();
                    finish_after_kill(&mut drain_stdout).await
                }
            }
        };
        // The bounded protocol grace is over. Kill any pipe-less helper, remove the registry entry,
        // and clear the numeric pgid before metadata/classification awaits can expose it to reuse.
        group_for_work.reaped();
        if let Some(stream) =
            overflow_seen.or(stderr.overflow.then_some("stderr")).or(stdout.overflow.then_some("stdout"))
        {
            let tail = if stream == "stderr" { &stderr.tail } else { &stdout.tail };
            return Err(PlayError::output_too_large(stream, tail));
        }
        let stderr = String::from_utf8_lossy(&stderr.tail);
        let stdout = String::from_utf8_lossy(&stdout.tail);
        let format = stdout.lines().map(str::trim).find(|l| !l.is_empty()).and_then(parse_format);

        let wrote = tokio::fs::metadata(tmp).await.map(|m| m.len() > 0).unwrap_or(false);
        if !status.success() {
            return Err(classify(status.code(), &stderr));
        }
        // `tmp` cannot be a half-written file. yt-dlp downloads to `<tmp>.part` and merges into
        // `<tmp>.temp.<ext>`, then publishes by rename — so `tmp` appears atomically, already
        // complete. An earlier version sampled its size to decide whether a descendant was still
        // writing it; that could not work, and was wrong in both directions. Blind to a merge in
        // progress, because the merge grows a sibling and never `tmp`. And its only possible
        // positive was the rename itself, so the one thing it ever detected was a download
        // COMPLETING — which it then rejected and deleted.
        //
        // Exit 0 is yt-dlp's verdict on the extraction; whether a file arrived is ours to check.
        if !wrote {
            return Err(PlayError::incomplete("yt-dlp exited 0 with no output file".into()));
        }
        Ok(format.unwrap_or_default())
    };
    // Dropping the future kills yt-dlp; `guard` kills whatever it forked.
    let guard = GroupGuard(group.clone());
    let out = tokio::time::timeout(Duration::from_secs(DOWNLOAD_TIMEOUT_SECS), work).await;
    drop(guard);
    match out {
        Ok(r) => r,
        Err(_) => Err(PlayError::timed_out()),
    }
}

#[derive(Default)]
struct TailCapture {
    tail: Vec<u8>,
    overflow: bool,
}

/// Drain a child pipe to EOF while retaining at most [`CHILD_OUTPUT_MAX_BYTES`] of its suffix.
/// The suffix is the useful part for diagnostics (yt-dlp writes its final ERROR last), while an
/// in-limit stdout remains complete and can still be parsed for the selected format.
async fn capture_tail<R: AsyncRead + Unpin>(
    reader: &mut R,
    stream: &'static str,
    max_bytes: usize,
    oversize: tokio::sync::mpsc::Sender<&'static str>,
) -> TailCapture {
    let mut captured = TailCapture { tail: Vec::with_capacity(max_bytes), overflow: false };
    let mut chunk = [0u8; 8192];
    loop {
        let read = match reader.read(&mut chunk).await {
            Ok(0) | Err(_) => break,
            Ok(read) => read,
        };
        if !captured.overflow && captured.tail.len().saturating_add(read) > max_bytes {
            captured.overflow = true;
            let _ = oversize.try_send(stream);
        }
        if read >= max_bytes {
            captured.tail.clear();
            captured.tail.extend_from_slice(&chunk[read - max_bytes..read]);
        } else {
            let excess = captured.tail.len().saturating_add(read).saturating_sub(max_bytes);
            if excess != 0 {
                captured.tail.drain(..excess);
            }
            captured.tail.extend_from_slice(&chunk[..read]);
        }
    }
    captured
}

/// Kills the download's process group on the way out, however the future ended.
struct SharedGroup {
    pgid: std::sync::atomic::AtomicU32,
    signalled: std::sync::atomic::AtomicBool,
}

impl SharedGroup {
    fn new() -> Self {
        Self {
            pgid: std::sync::atomic::AtomicU32::new(0),
            signalled: std::sync::atomic::AtomicBool::new(false),
        }
    }

    fn set(&self, pgid: Option<u32>) {
        self.pgid.store(pgid.unwrap_or(0), std::sync::atomic::Ordering::Release);
    }

    fn kill(&self) {
        if self.signalled.swap(true, std::sync::atomic::Ordering::AcqRel) {
            return;
        }
        let pgid = Some(self.pgid.load(std::sync::atomic::Ordering::Acquire)).filter(|&p| p != 0);
        kill_group(pgid);
    }

    fn reaped(&self) {
        self.kill();
        let pgid = Some(self.pgid.swap(0, std::sync::atomic::Ordering::AcqRel)).filter(|&p| p != 0);
        unregister_group(pgid);
    }
}

struct GroupGuard(std::sync::Arc<SharedGroup>);

impl Drop for GroupGuard {
    fn drop(&mut self) {
        self.0.reaped();
    }
}

/// Every live download's process group.
///
/// The guard above only fires when the download future is DROPPED, and on shutdown it is not:
/// `in_flight` holds a `Shared` clone of a future that itself captures the `Arc<AppState>` the map
/// lives in, so the cycle keeps the child alive past runtime teardown. Registering the groups gives
/// shutdown something it can act on directly, without depending on drop order.
static LIVE_GROUPS: den_process::Registry = den_process::Registry::new();

pub(crate) fn register_group(pgid: Option<u32>) {
    LIVE_GROUPS.register(pgid);
}

pub(crate) fn unregister_group(pgid: Option<u32>) {
    LIVE_GROUPS.unregister(pgid);
}

#[derive(Debug)]
pub(crate) enum BoundedOutputError {
    Io(std::io::Error),
    TimedOut,
    TooLarge { stream: &'static str, limit: usize, tail: Vec<u8> },
}

impl std::fmt::Display for BoundedOutputError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io(e) => e.fmt(f),
            Self::TimedOut => f.write_str("child process timed out"),
            Self::TooLarge { stream, limit, tail } => {
                write!(f, "child {stream} exceeded {limit} bytes; tail: {}", stderr_tail(tail))
            }
        }
    }
}

impl std::error::Error for BoundedOutputError {}

impl From<std::io::Error> for BoundedOutputError {
    fn from(value: std::io::Error) -> Self {
        Self::Io(value)
    }
}

fn pipe_drain(
    mut pipe: Option<impl AsyncRead + Unpin + Send + 'static>,
    stream: &'static str,
    max_bytes: usize,
    oversize: tokio::sync::mpsc::Sender<&'static str>,
) -> tokio::task::JoinHandle<TailCapture> {
    tokio::spawn(async move {
        match pipe.as_mut() {
            Some(pipe) => capture_tail(pipe, stream, max_bytes, oversize).await,
            None => TailCapture::default(),
        }
    })
}

async fn finish_after_kill(drain: &mut tokio::task::JoinHandle<TailCapture>) -> TailCapture {
    match tokio::time::timeout(POST_KILL_DRAIN_GRACE, &mut *drain).await {
        Ok(result) => result.unwrap_or_default(),
        Err(_) => {
            drain.abort();
            let _ = drain.await;
            TailCapture::default()
        }
    }
}

/// Run `cmd` in its own process group, concurrently draining both pipes with explicit byte ceilings.
/// The deadline covers spawning, execution, and the normal post-exit pipe grace. Overflow and timeout
/// kill the complete group before the direct child is reaped; cancellation does the same through the
/// guard. Output at or below each ceiling remains byte-for-byte complete for existing parsers.
pub(crate) async fn output_in_group(
    cmd: &mut Command,
    timeout: Duration,
    stdout_max: usize,
    stderr_max: usize,
) -> Result<std::process::Output, BoundedOutputError> {
    #[cfg(unix)]
    cmd.process_group(0);
    let deadline = tokio::time::Instant::now() + timeout;
    let mut child = cmd.spawn()?;
    let pgid = child.id();
    register_group(pgid);
    // A guard, not statements after the await: the await is a cancellation point, and the routine
    // way to reach it is a client hanging up mid-/crop, which main.rs correctly treats as normal.
    // Cleaning up only on the happy path left the pgid registered forever — nothing else prunes it
    // — so shutdown signalled a group that had been dead for hours, and pid reuse makes that
    // somebody else's group. Same reason download_to uses GroupGuard.
    let mut entry = GroupEntry::new(pgid);
    let (oversize_tx, mut oversize_rx) = tokio::sync::mpsc::channel(2);
    let stderr_tx = oversize_tx.clone();
    let mut stdout = pipe_drain(child.stdout.take(), "stdout", stdout_max, oversize_tx);
    let mut stderr = pipe_drain(child.stderr.take(), "stderr", stderr_max, stderr_tx);
    let mut timed_out = false;
    let (status, overflow) = tokio::select! {
        result = child.wait() => (result?, None),
        Some(stream) = oversize_rx.recv() => {
            entry.kill();
            (child.wait().await?, Some(stream))
        }
        () = tokio::time::sleep_until(deadline) => {
            timed_out = true;
            entry.kill();
            (child.wait().await?, None)
        }
    };
    // Once wait has reaped the leader, descendants are killed and the registry entry is removed
    // before any pipe drain can await. Keeping a numeric pgid armed past this point risks signalling
    // an unrelated group after pid reuse.
    entry.reaped();
    let stdout = finish_after_kill(&mut stdout).await;
    let stderr = finish_after_kill(&mut stderr).await;
    if timed_out || tokio::time::Instant::now() > deadline {
        return Err(BoundedOutputError::TimedOut);
    }
    if let Some(stream) =
        overflow.or(stdout.overflow.then_some("stdout")).or(stderr.overflow.then_some("stderr"))
    {
        let (limit, tail) =
            if stream == "stdout" { (stdout_max, stdout.tail) } else { (stderr_max, stderr.tail) };
        return Err(BoundedOutputError::TooLarge { stream, limit, tail });
    }
    Ok(std::process::Output { status, stdout: stdout.tail, stderr: stderr.tail })
}

/// Kills the group and deregisters it however the future ended — return, error, or cancellation.
struct GroupEntry {
    pgid: Option<u32>,
    signalled: bool,
}

impl GroupEntry {
    fn new(pgid: Option<u32>) -> Self {
        Self { pgid, signalled: false }
    }

    fn kill(&mut self) {
        if self.signalled {
            return;
        }
        self.signalled = true;
        kill_group(self.pgid);
    }

    fn reaped(&mut self) {
        self.kill();
        unregister_group(self.pgid.take());
    }
}

impl Drop for GroupEntry {
    fn drop(&mut self) {
        self.kill();
        unregister_group(self.pgid.take());
    }
}

/// Is this group still registered as live? Tests only — the registry is process-wide, so asserting
/// on its size races other tests' subprocesses; asking about one id does not.
#[cfg(test)]
pub(crate) fn is_group_live(pgid: u32) -> bool {
    LIVE_GROUPS.contains(pgid)
}

/// SIGKILL every download still running. Returns how many groups it signalled.
pub fn kill_live_groups() -> usize {
    LIVE_GROUPS.kill_all()
}

pub(crate) fn kill_group(pgid: Option<u32>) {
    if let Some(pgid) = pgid {
        den_process::kill_group(pgid);
    }
}
