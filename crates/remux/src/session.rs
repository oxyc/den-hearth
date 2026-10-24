//! A playback session: one release, its keyframe-cut playlist, and the ffmpeg run producing it.
//!
//! The playlist is fixed at creation. Segments are made on demand: a request for segment N joins the
//! GOP files that cover it, waiting briefly if the running job is about to reach it, or restarting the
//! job at N's keyframe if N is behind the job or far ahead of it. A job that has produced enough ahead
//! of the player is paused with SIGSTOP, and the GOPs behind the player are deleted, so a session costs
//! one idle process and a window of a few segments on disk.
//!
//! There are no timers while nothing is playing: each session has one supervising task, which ticks
//! only while its ffmpeg is actually running and otherwise sleeps until a request or its idle deadline.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU8, Ordering::Relaxed};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant};

use futures_util::stream::{FuturesOrdered, StreamExt};
use hyper::{Response, StatusCode};
use tokio::sync::Notify;

use crate::httputil::{self, Body};
use crate::job::{self, Job, Spec};
use crate::playlist::{self, Segment};
use crate::probe::{MediaInfo, Source, VideoCodec};
use crate::scout;
use crate::state::{unix_now, AppState, Refused};

mod open;
mod plan;
pub(crate) mod resync;
mod select;
mod serve;
mod supervise;
use open::*;
#[cfg(test)]
pub(crate) use plan::audio_plan;
pub use plan::Playable;
use plan::*;
pub(crate) use plan::{tonemaps, transcode_size, Need};
pub use select::create;
pub use serve::{busy, gone};
#[cfg(test)]
use serve::{media_response, open_parts};
use supervise::supervise;

#[cfg(test)]
use den_net::DolbyVisionPlay;

/// How far ahead of the newest request a job may produce before it is paused, in segments.
const AHEAD_SEGMENTS: usize = 4;
/// A request further than this past what the job has produced restarts the job there rather than
/// waiting for it to arrive. Copying runs far faster than real time, so a few segments is a wait of
/// seconds; more is a seek.
const RESTART_GAP_SEGMENTS: usize = 3;
/// How long a segment request waits for ffmpeg before answering 503 with Retry-After.
const SEGMENT_WAIT: Duration = Duration::from_secs(20);
/// How long creating a session for a native player waits for its `init.mp4` (`Session::prepare`).
const INIT_WAIT: Duration = Duration::from_secs(15);
/// `start_producer` taking at least this long is worth a log line: resuming a merely paused job answers in
/// microseconds, so crossing this means a job was actually (re)spawned — a cold restart worth watching
/// against a native player's own, much shorter patience for a stalled request.
const RESTART_LOG_THRESHOLD: Duration = Duration::from_secs(2);
const POLL: Duration = Duration::from_millis(100);
/// How often the supervisor looks at a RUNNING job. A paused one is not looked at at all.
const TICK: Duration = Duration::from_millis(500);
/// Timestamps within this of each other name the same keyframe. Keyframes are at least a few frames
/// apart; ffmpeg's playlist rounds durations to the microsecond.
const SNAP: f64 = 0.05;
/// A resume's first job starts at the segment this far before `startAt`, not the one `startAt` falls in: a
/// player may ask for the segment before when the point is near a boundary (hls.js allows a quarter second),
/// and a job already behind that request is kept, where one ahead of it would be restarted.
const START_SLACK: f64 = 1.0;
/// A session rebuilt after a restart fetches its release again once a request comes within this many segments of
/// the first one scratch doesn't hold. A player asks for segments as far ahead as it buffers — 30 s to two minutes —
/// so that fetch and the run's start happen while it still plays what it has.
const RESUME_LEAD: usize = 2;
/// How soon fetching a rebuilt session's release is tried again after it failed.
const RESOLVE_RETRY: Duration = Duration::from_secs(5);
/// ffmpeg runs in a row that produced nothing before the session gives up on its source.
const MAX_FAILURES: u32 = 3;
/// How many releases to resolve and probe before giving up on a title. Past the first few, each is one debrid
/// link and a handful of ranged reads, so a title whose top releases won't open still finds one further down.
const MAX_TRIES: usize = 12;
/// Releases opened at once while picking. The pick still goes by rank: one that answers sooner waits for those
/// ranked above it.
const PARALLEL_OPENS: usize = 3;
/// No new release is opened once picking has taken this long — about where a person gives up on a play
/// button; those already opening finish.
const PICK_BUDGET: Duration = Duration::from_secs(30);
/// One release's resolve and probe. A debrid slower than this to hand over the head of a file plays badly anyway.
const OPEN_TIMEOUT: Duration = Duration::from_secs(20);
/// A session lives for the film plus this, and never longer than `SESSION_MAX_SECS`.
const SESSION_GRACE_SECS: u64 = 60 * 60;
const SESSION_MAX_SECS: u64 = 6 * 60 * 60;
/// How long a session with no live signal may sit before it ends — far shorter than `SESSION_IDLE_SECS`,
/// for the two cases with no "paused mid-film" grace to honour: one that has never served a single segment
/// (a replacement whose viewer never opened it, or gave up right after a fatal error), and one whose
/// heartbeat — a paused viewer's explicit "still here", POST …/heartbeat — has itself stopped arriving.
/// Not env-configurable, like `REPLACE_GRACE_SECS`: both are short-and-fixed by design, not a knob an
/// operator should need to reach for.
const UNATTENDED_IDLE: Duration = Duration::from_secs(60);

/// Select the input ffmpeg may dial. Remote URLs are accepted only through a loopback Door; local
/// paths remain valid for tests and future local sources.
fn ffmpeg_input(door_url: Option<String>, resolved: &str) -> Option<String> {
    door_url.or_else(|| {
        (!resolved.starts_with("http://") && !resolved.starts_with("https://")).then(|| resolved.to_string())
    })
}

pub fn seg_index(file: &str) -> Option<usize> {
    let n = file.strip_prefix("seg")?.strip_suffix(".m4s")?;
    (!n.is_empty() && n.bytes().all(|c| c.is_ascii_digit()) && n.len() < 8).then(|| n.parse().ok())?
}

/// A subtitle rendition's file.
#[derive(Debug, PartialEq)]
pub enum SubFile {
    /// `sub<N>.m3u8`
    Playlist(usize),
    /// `sub<N>.vtt`: den-subtitles' one document spanning the film.
    Document(usize),
    /// `sub<N>_<W>.vtt`: the release's own track over video segment `W`.
    Window(usize, usize),
}

pub fn sub_file(file: &str) -> Option<SubFile> {
    let rest = file.strip_prefix("sub")?;
    let digits = |s: &str, max: usize| {
        (!s.is_empty() && s.len() <= max && s.bytes().all(|c| c.is_ascii_digit()))
            .then(|| s.parse::<usize>().ok())?
    };
    let rendition = |s: &str| digits(s, 1).filter(|n| *n < crate::subs::MAX_LANGUAGES);
    if let Some(n) = rest.strip_suffix(".m3u8") {
        return rendition(n).map(SubFile::Playlist);
    }
    let body = rest.strip_suffix(".vtt")?;
    match body.split_once('_') {
        Some((n, w)) => Some(SubFile::Window(rendition(n)?, digits(w, 7)?)),
        None => rendition(body).map(SubFile::Document),
    }
}

pub fn is_session_file(file: &str) -> bool {
    matches!(file, "master.m3u8" | "media.m3u8" | "init.mp4" | "report" | "heartbeat" | "end")
        || seg_index(file).is_some()
        || sub_file(file).is_some()
        || file == "subtitle-resync"
        || resync::aligned_file(file).is_some()
}

pub struct Release {
    pub label: String,
    pub filename: String,
    pub size: Option<u64>,
    /// The season pack this release's file was picked out of, when scout named one — display context
    /// alongside `label`/`size`, which are already the episode's own (resolved.size, not the pack's).
    pub pack_size: Option<u64>,
    /// The release the player named when this one was played instead, and why it was passed over — where that is
    /// known.
    pub requested: Option<Requested>,
}

pub struct Requested {
    pub filename: String,
    pub why: String,
}

/// How long `create` took in each of its stretches (den-edge#234's step 0, the `Server-Timing` header on the 201):
/// scout's list ("resolve"), and picking a release from it ("open", with how many were tried). Measured so the
/// wait a slow release shows can be read back from a real session rather than guessed at.
pub struct Timing {
    pub resolve_ms: u64,
    pub open_ms: u64,
    pub tried: usize,
}

#[derive(Clone, Debug)]
pub struct Gop {
    pub job: u32,
    /// Its position within its job's run.
    pub idx: u32,
    pub start: f64,
    pub dur: f64,
    pub path: PathBuf,
    pub size: u64,
}

pub struct Inner {
    job: Option<Job>,
    next_job: u32,
    gops: Vec<Gop>,
    /// Jobs that ran to the end of the file, so their last GOP ends the last segment.
    finished: Vec<u32>,
    init: Option<PathBuf>,
    last_seen: Instant,
    /// The last `POST …/heartbeat`, a paused viewer's own "still here" — distinct from `last_seen`, which
    /// playback itself keeps fresh. `None` until the first one arrives.
    last_heartbeat: Option<Instant>,
    /// The newest segment asked for: production follows it.
    want: usize,
    ended: bool,
    bytes: u64,
    failures: u32,
    /// The vetted debrid link the loopback Door reads — scout's play URL, resolved — and whether it
    /// has been re-fetched.
    input: String,
    reresolved: bool,
    /// Scout's play URL for the release, a secret: what a fresh debrid link is fetched through when the one ffmpeg
    /// reads stops working, or when a session rebuilt after a restart needs one (`ensure_resolved`). A rebuild that had
    /// to list the title again holds the new ticket here.
    play_url: String,
    /// A rebuilt session's fetch of its release is under way in the background (`serve_segment`).
    warming: bool,
    /// When fetching the release again last failed, so a player's retries don't each ask scout again at once.
    resolve_failed: Option<Instant>,
    /// Segments actually sent to a player (never a HEAD), for the end-of-session summary line.
    segments_served: u64,
    /// The sum of those segments' own durations: seconds of the film actually delivered, as against
    /// the session's wall-clock age.
    seconds_served: f64,
    /// The last subtitle rendition a player was actually sent cues from — its index, and whether they
    /// came from the release's own track or den-subtitles — for the summary line. `None` when no
    /// subtitle was ever served.
    subtitle_used: Option<(usize, &'static str)>,
}

impl Inner {
    /// When the supervisor should end this session if nothing more arrives.
    ///
    /// `UNATTENDED_IDLE` past the last heartbeat, once one has come in more recently than any real request: a
    /// paused viewer who goes quiet is freed soon after their own heartbeat stops, rather than riding out the
    /// full silence grace. `UNATTENDED_IDLE` past the last real request too when nothing has ever been
    /// served: there is no "paused mid-film" to protect for a session nothing has ever played from. Otherwise
    /// `session_idle` past the last real request — the original, generous grace for playback that simply
    /// stopped making requests, kept for a player with no heartbeat of its own.
    fn idle_deadline(&self, session_idle: Duration) -> Instant {
        match self.last_heartbeat {
            Some(hb) if hb > self.last_seen => hb + UNATTENDED_IDLE,
            _ if self.segments_served == 0 => self.last_seen + UNATTENDED_IDLE,
            _ => self.last_seen + session_idle,
        }
    }
}

pub struct Session {
    pub sid: String,
    pub sig: String,
    pub exp: u64,
    /// Whom it counts against: a logged-in browser's id, or the install it was started with
    /// (`auth::install_id`).
    pub owner: String,
    /// Created through den-edge's authenticated public origin. Used only to release the public firewall gate;
    /// signed media authorization remains the session URL itself.
    pub public: bool,
    /// When it was set up, so an install past its share ends its oldest.
    pub started: Instant,
    /// The creating request's `X-Request-Id`, carried for the lifetime of the session so every line it
    /// writes — including the end-of-session summary — can be joined back to that request.
    pub rid: Option<String>,
    pub imdb: String,
    pub dir: PathBuf,
    pub release: Release,
    pub info: MediaInfo,
    /// The audio track played, counting audio tracks only.
    pub audio: usize,
    /// The track is copied as it is, as this codec (`ec-3`, `ac-3`, `fLaC`); `None` re-encodes it to AAC.
    pub audio_copy: Option<&'static str>,
    /// What happens to the track: copied, or AAC stereo, 5.1 or 7.1.
    pub audio_out: job::AudioOut,
    /// The channels the session's audio carries: the track's own when copied, else 2, 6 or 8.
    pub audio_channels: u32,
    /// den-subtitles, when the browser named an install and languages.
    pub subs: Option<crate::subs::Subs>,
    /// What a player can pick: the languages asked for, then the release's own others, in rendition order.
    pub renditions: Vec<crate::subs::Rendition>,
    /// The release's own tracks the runs write, as ffmpeg numbers its subtitle streams.
    text_tracks: Vec<usize>,
    /// What the runs have written of them.
    own: Mutex<crate::subs::OwnCues>,
    /// What a copy does with the video's Dolby Vision: kept for a player that shows it, else stripped.
    pub dovi: job::Dovi,
    /// The HEVC is transcoded to H.264 on the GPU, for a player that cannot take it.
    pub transcoded: bool,
    /// Why it is transcoded, for the end-of-session summary line; `None` when `transcoded` is false.
    pub transcode_reason: Option<String>,
    /// The size and rate a transcode comes down to: 720p for a player whose link can't carry 1080p.
    pub preset: job::Preset,
    pub segments: Vec<Segment>,
    /// How long a player on the link it named waits before a copy plays through without running dry (`prebuffer`):
    /// its pre-buffer target. `None` for a transcode, or without a link or a byte index.
    pub prebuffer: Option<f64>,
    /// What a copy asks of a link (`need`); `None` for a transcode, whose rate is its preset's.
    pub need: Option<Need>,
    /// The bits a second the player's own link measured for itself (`Want::max_bitrate`), before any
    /// `UPLINK_MBPS` budget narrowed it: `None` is a player at home. Also this session's mark as "away from
    /// home" for `other_remote_bps` — what every other such session already needs off the shared uplink.
    pub max_bitrate: Option<u64>,
    /// Each segment's start and bytes as the session sends them (`playlist::segment_bytes`), for a copy with a byte
    /// index: what a player needs to work out, as the film plays, whether its link keeps up.
    pub demand: Option<Vec<(f64, u64)>>,
    pub master: String,
    pub media: String,
    reports: AtomicU8,
    speed_probes: AtomicU8,
    /// The loopback door its ffmpeg reads the release through (`source`). `None` inside means no safe
    /// remote input is available (including an unknown-size release), so production is refused rather
    /// than falling back to the raw URL. Unset while a rebuilt session has not fetched its release again.
    door: std::sync::OnceLock<Option<crate::source::Door>>,
    /// The scout the release came from, a secret, for fetching a fresh debrid link.
    source: scout::ScoutSource,
    /// What the release's link and probe are remembered under (`opened_key`), so a link that stops working is
    /// forgotten there too.
    opened_key: String,
    /// The release's file as `file_key` names it: a link fetched again must lead to the same file.
    file_key: Option<String>,
    /// One fetch of the release at a time (`ensure_resolved`).
    resolve_gate: tokio::sync::Mutex<()>,
    inner: Mutex<Inner>,
    /// One request at a time may queue for and start this session's producer.
    producer_gate: tokio::sync::Mutex<()>,
    wake: Notify,
    resync: resync::State,
}

pub struct ApiError {
    pub status: StatusCode,
    pub code: &'static str,
    pub detail: String,
    /// The extensions of cached releases left out unopened by their names (`scout::left_out_by_name`), on an
    /// answer that found nothing to play: what lets a player say "only .ts releases are ready" instead of "none".
    pub formats: Vec<&'static str>,
}

fn api(status: StatusCode, code: &'static str, detail: impl Into<String>) -> ApiError {
    ApiError { status, code, detail: detail.into(), formats: Vec::new() }
}

/// The nearest keyframe to `t` when there is one within `SNAP`, else `t`. Keeps a job's running sum of
/// GOP durations pinned to the real keyframes, so rounding cannot drift across a long film.
pub(crate) fn snap(keyframes: &[f64], t: f64) -> f64 {
    let i = keyframes.partition_point(|k| *k < t);
    [i.checked_sub(1), Some(i)]
        .into_iter()
        .flatten()
        .filter_map(|j| keyframes.get(j))
        .copied()
        .filter(|k| (k - t).abs() < SNAP)
        .min_by(|a, b| (a - t).abs().total_cmp(&(b - t).abs()))
        .unwrap_or(t)
}

/// The segment playing at `t`.
fn seg_at(segments: &[Segment], t: f64) -> usize {
    segments.partition_point(|s| s.start <= t + SNAP).saturating_sub(1)
}

/// The segment a session's first job heads for: the one a resume at `start_at` plays, less `START_SLACK`. The
/// init request starts that job, so a resume runs ffmpeg once, from there, instead of from zero and again
/// after the player's seek.
pub(crate) fn start_segment(segments: &[Segment], start_at: f64) -> usize {
    seg_at(segments, (start_at - START_SLACK).max(0.0))
}

/// The GOPs that make up `seg`, in order, when they are all on disk: a GOP starting on the segment's
/// first keyframe, then its job's following GOPs until the segment's end is covered — or until the
/// job's last GOP, when that job ran to the end of the file.
pub fn ready_chain(gops: &[Gop], finished: &[u32], seg: Segment, first: bool) -> Option<Vec<usize>> {
    'candidates: for (i, g) in gops.iter().enumerate() {
        if (g.start - seg.start).abs() >= SNAP && !(first && g.start < seg.end) {
            continue;
        }
        let mut chain = vec![i];
        let mut cur = g;
        loop {
            if cur.start + cur.dur >= seg.end - SNAP {
                return Some(chain);
            }
            match gops.iter().position(|n| n.job == cur.job && n.idx == cur.idx + 1) {
                Some(j) => {
                    chain.push(j);
                    cur = &gops[j];
                }
                None if finished.contains(&cur.job) && seg.end - cur.start < 1e9 && is_last(gops, cur) => {
                    return Some(chain);
                }
                None => continue 'candidates,
            }
        }
    }
    None
}

/// Is `g` the last GOP its job will ever write? Only asked of finished jobs, whose GOPs are all read.
fn is_last(gops: &[Gop], g: &Gop) -> bool {
    !gops.iter().any(|o| o.job == g.job && o.idx > g.idx)
}

/// What trying one subtitle candidate produced. A refusal splits in two because den-remux cannot
/// otherwise tell them apart — both are a non-2xx — and only one of them is worth a second request:
/// `Sparse` (den-subtitles' `X-Den-Degraded: sparse`) names a real, too-sparse-for-its-span track
/// that its own `?sparse=1` can still serve; `Refused` covers everything else (a cue-less body, an
/// expired link, a dead upload, a transient failure, an origin this install does not allow) — asking
/// again with the flag would refuse those identically and spend a metered credit doing it.
enum Fetched {
    Found(String),
    Sparse,
    Refused,
}

impl Session {
    fn lock(&self) -> MutexGuard<'_, Inner> {
        self.inner.lock().unwrap_or_else(|e| e.into_inner())
    }

    pub fn short(&self) -> &str {
        &self.sid[..6]
    }

    /// The ffmpeg this session is running, if any. Tests use it to prove the process is gone.
    pub fn job_pid(&self) -> Option<u32> {
        self.lock().job.as_ref().filter(|j| j.exit.is_none()).map(|j| j.pid)
    }

    pub fn touch(&self) {
        self.lock().last_seen = Instant::now();
    }

    /// A paused viewer's own "still here" (`POST …/heartbeat`): holds the session past `UNATTENDED_IDLE` for
    /// as long as it keeps arriving, without needing the segment requests actual playback would make. Wakes
    /// the supervisor so the fresh heartbeat reschedules its sleep now, not on whatever it next wakes for.
    pub fn heartbeat(&self) {
        self.lock().last_heartbeat = Some(Instant::now());
        self.wake.notify_one();
    }

    /// Make the last request `ago` old and let the supervisor look again: a silence, in a test's time.
    #[cfg(test)]
    pub fn silent_for(&self, ago: Duration) {
        self.lock().last_seen = Instant::now() - ago;
        self.wake.notify_one();
    }

    /// A player's diagnostic is useful once and bounded to three attempts. The signed URL is a bearer
    /// credential; it must not also be an unbounded log-writing endpoint.
    pub fn take_report_slot(&self) -> bool {
        self.reports.try_update(Relaxed, Relaxed, |count| (count < 3).then_some(count + 1)).is_ok()
    }

    /// A receiver may retry one interrupted measurement. Beyond that, the signed bearer must not become an
    /// unbounded random-byte and home-upload generator for the rest of the session lifetime.
    pub fn take_speed_slot(&self) -> bool {
        self.speed_probes.try_update(Relaxed, Relaxed, |count| (count < 2).then_some(count + 1)).is_ok()
    }

    /// `Cache-Control` for what the session's URL serves the same for its whole life: its playlists, a found
    /// subtitle. The URL names the session and is signed for it, so nothing is kept past it or shared with another.
    pub fn cache_control(&self) -> String {
        format!("private, max-age={}", self.exp.saturating_sub(unix_now()))
    }

    /// The segment production follows. Tests use it to see where a resume's first job will start.
    #[cfg(test)]
    pub fn wanted(&self) -> usize {
        self.lock().want
    }

    /// Put a job's init segment where the session serves it. A kept Dolby Vision profile 5 with no record in its
    /// container has no side data for ffmpeg's muxer to write a `dvcC` from, so it is added here; a layout this
    /// can't patch is not served, and the session fails as any whose init never appears.
    ///
    /// Written beside it and renamed into place: a process that dies midway leaves no short `init.mp4` for the next
    /// one to rebuild the session with (`resume::adopt`).
    fn write_init(&self, from: &Path, to: &Path) -> bool {
        let recordless = self.dovi == job::Dovi::Keep && self.info.dolby_vision_recordless;
        let init = std::fs::read(from).map_err(|e| e.to_string());
        let init = match self.info.dolby_vision.filter(|_| recordless) {
            Some(dv) => init.and_then(|init| job::add_dovi_record(&init, dv.profile, dv.level)),
            None => init,
        };
        let tmp = to.with_extension("mp4.tmp");
        let written = init.and_then(|init| {
            std::fs::write(&tmp, init).and_then(|()| std::fs::rename(&tmp, to)).map_err(|e| e.to_string())
        });
        match written {
            Ok(()) => true,
            Err(e) => {
                crate::log_limited("init_write", || {
                    format!("session {}: init.mp4 was not written: {e}", self.short())
                });
                false
            }
        }
    }

    /// Take what the job has finished since the last look: its exit, and the GOPs in its playlist.
    fn refresh(&self, i: &mut Inner, st: &AppState) {
        let Inner { job, gops, finished, init, bytes, failures, input, play_url, .. } = i;
        let Some(job) = job.as_mut() else { return };
        let newly_exited = job.exit.is_none() && job.poll_exit().is_some();
        // After the exit check, so everything written before the exit is read.
        let text = std::fs::read_to_string(job.dir.join("gops.m3u8")).unwrap_or_default();
        for (name, dur) in job::parse_gops(&text).into_iter().skip(job.read) {
            let start = snap(&self.info.keyframes, job.next_start);
            job.next_start = start + dur;
            job.read += 1;
            let path = job.dir.join(&name);
            let size = std::fs::metadata(&path).map(|m| m.len()).unwrap_or(0);
            gops.push(Gop { job: job.id, idx: (job.read - 1) as u32, start, dur, path, size });
            *bytes += size;
            job.bytes += size;
            st.scratch_bytes.fetch_add(size, Relaxed);
            *failures = 0;
            if init.is_none() {
                // Every run writes a byte-identical init (see job.rs), so the first one serves all.
                let dst = self.dir.join("init.mp4");
                if self.write_init(&job.dir.join("init.mp4"), &dst) {
                    *init = Some(dst);
                }
            }
        }
        gops.sort_by(|a, b| a.start.total_cmp(&b.start));
        if !job.text.is_empty() {
            // The run has read the film as far as its last finished GOP (less what the demuxer may still hand over
            // for it), or to the end when it ran that far.
            let upto = match job.exit {
                Some(true) => self.info.duration,
                _ => job.next_start - crate::subs::TRAIL,
            };
            let mut own = self.own.lock().unwrap_or_else(|e| e.into_inner());
            for &t in &job.text {
                own.take(job.id, t, &job::text_file(&job.dir, t), job.start, upto);
            }
        }
        if newly_exited {
            let why = if job.exit == Some(true) { "reached the end" } else { "failed" };
            eprintln!("session {}: {}{}", self.short(), job.pull_line(why), rid_tail(self.rid.as_deref()));
            if job.exit == Some(true) {
                finished.push(job.id);
            } else {
                if job.read == 0 {
                    *failures += 1;
                }
                if let Some(tail) = job.take_stderr() {
                    let (sid, start, rid) =
                        (self.short().to_string(), job.start, rid_tail(self.rid.as_deref()));
                    let secrets = [input.clone(), play_url.clone(), self.source.base.clone()];
                    tokio::spawn(async move {
                        let tail = tail.await.unwrap_or_default();
                        let refs: Vec<&str> = secrets.iter().map(String::as_str).collect();
                        let tail = crate::redact::scrub(tail.trim(), &refs);
                        eprintln!(
                            "session {sid}: ffmpeg from {start:.3}s failed: {}{rid}",
                            tail.lines().last().unwrap_or("(no output)")
                        );
                    });
                }
            }
        }
    }

    fn ready(&self, i: &Inner, n: usize) -> Option<Vec<PathBuf>> {
        let chain = ready_chain(&i.gops, &i.finished, self.segments[n], n == 0)?;
        Some(chain.into_iter().map(|k| i.gops[k].path.clone()).collect())
    }

    /// Where a job has to start to produce segment `n`: `n`'s own keyframe, unless the next keyframe
    /// is too close behind it to land on reliably (see `job::MIN_SEEK_GAP`), in which case an earlier
    /// segment's. Returns the keyframe and the `-ss` for it.
    fn restart_point(&self, n: usize) -> (f64, Option<f64>) {
        let kfs = &self.info.keyframes;
        for m in (1..=n).rev() {
            let k = self.segments[m].start;
            let next = kfs.iter().copied().find(|x| *x > k + 1e-6).unwrap_or(self.info.duration);
            if next - k >= job::MIN_SEEK_GAP {
                let seek = if self.info.grid() { job::grid_seek(k) } else { job::seek_for(k) };
                return (k, seek);
            }
        }
        (kfs.first().copied().unwrap_or(0.0), None)
    }

    /// What a job starting on the keyframe at `start` does with the video: a copy, or this session's transcode.
    fn video(&self, st: &AppState, start: f64) -> job::Video {
        match self.transcoded {
            true => {
                let (width, height) = transcode_size(self.info.width, self.info.height, self.preset);
                let keyframes = match self.info.grid() {
                    true => job::Keyframes::Grid { times: self.info.keyframes.clone(), start },
                    false => job::Keyframes::Source,
                };
                job::Video::Transcode {
                    device: st.cfg.vaapi_device.to_string_lossy().into_owned(),
                    width,
                    height,
                    tonemap: tonemaps(&self.info),
                    preset: self.preset,
                    software_decode: !matches!(self.info.video, VideoCodec::H264 | VideoCodec::Hevc),
                    keyframes,
                }
            }
            false => {
                // Profile 5 kept as it is goes under its own sample entry; its HEVC has no other picture.
                let p5 =
                    self.dovi == job::Dovi::Keep && self.info.dolby_vision.is_some_and(|dv| dv.profile == 5);
                let tag = match self.info.video {
                    VideoCodec::Hevc if p5 => Some("dvh1"),
                    VideoCodec::Hevc => Some("hvc1"),
                    _ => None,
                };
                job::Video::Copy { tag, dovi: self.dovi }
            }
        }
    }

    /// Whether a running job — started at `start`, its next GOP at `next_start` — gives segment `n`: it began at or
    /// before the segment, is no more than `RESTART_GAP_SEGMENTS` short of it, and hasn't gone past the segment's
    /// end with none of its GOPs there still kept. A job past it doesn't come back for it: a segment's GOPs are
    /// pruned once later ones are served, so a player asking for it again — a seek back, a player with no cache of
    /// its own — used to wait on that job until its request timed out.
    ///
    /// Segment 0 starts at 0 whatever the file's first keyframe says, and a job for it starts at that keyframe
    /// (`restart_point`), so `first_keyframe` is where "at or before" is counted from there.
    fn heads_for(
        segments: &[Segment],
        n: usize,
        first_keyframe: f64,
        job: u32,
        start: f64,
        next_start: f64,
        gops: &[Gop],
    ) -> bool {
        let seg = segments[n];
        let passed = next_start >= seg.end - SNAP;
        let kept = gops
            .iter()
            .any(|g| g.job == job && g.start < seg.end - SNAP && g.start + g.dur > seg.start + SNAP);
        start <= seg.start.max(first_keyframe) + SNAP
            && n <= seg_at(segments, next_start) + RESTART_GAP_SEGMENTS
            && (!passed || kept)
    }

    /// Whether the session's current job already heads for `n` (`heads_for`) — shared by `producer_needed` and
    /// `ensure_job`, which both need the same answer for different reasons.
    fn job_heads_for(&self, i: &Inner, n: usize) -> bool {
        let first_keyframe = self.info.keyframes.first().copied().unwrap_or(0.0);
        i.job.as_ref().is_some_and(|j| {
            j.exit.is_none()
                && Self::heads_for(&self.segments, n, first_keyframe, j.id, j.start, j.next_start, &i.gops)
        })
    }

    fn producer_needed(&self, i: &Inner, n: usize) -> Result<bool, ()> {
        if i.ended {
            return Err(());
        }
        if self.job_heads_for(i, n) {
            // Running, it is only a matter of time. Stopped, it gave its permit back when it paused
            // (`Job::pause`) and needs one again before it can produce any further — not a restart, `gate`'s
            // own best-effort resume may already be racing this, but "needed" all the same so a request this
            // job alone can answer isn't left relying only on that.
            return Ok(i.job.as_ref().is_some_and(|j| j.stopped));
        }
        // A run failed without output: the next one waits for `reresolve_if_needed` to fetch a fresh link.
        if i.failures > 0 && !i.reresolved {
            return Ok(false);
        }
        if i.failures >= MAX_FAILURES {
            return Err(());
        }
        Ok(true)
    }

    /// Start a producer for `n` with resources acquired outside the session lock. Rechecks because another request may
    /// have made it ready while this one waited fairly.
    fn ensure_job(
        &self,
        i: &mut Inner,
        n: usize,
        st: &Arc<AppState>,
        permit: crate::state::ProducerPermit,
    ) -> Result<(), ()> {
        if !self.producer_needed(i, n)? {
            return Ok(());
        }
        // Already heads for it, only stopped for want of a permit: a SIGCONT, not a seek into the source and a
        // fresh ffmpeg. `gate`'s own best-effort resume may have already won this race while the permit above
        // was being waited for — in which case the job is running again and this permit simply isn't used,
        // dropped back to the pool for whoever is next.
        if self.job_heads_for(i, n) {
            if let Some(job) = i.job.as_mut().filter(|j| j.resumable()) {
                job.resume(permit);
            }
            return Ok(());
        }
        // A rebuilt session that has not fetched its release again has nothing to give ffmpeg yet.
        let Some(door) = self.door.get() else { return Err(()) };
        let (start, seek) = self.restart_point(n);
        let id = i.next_job;
        i.next_job += 1;
        let dir = self.dir.join(format!("j{id}"));
        // Through the session's door where there is one: the head from memory, the rest over warm connections.
        let door_url = door.as_ref().and_then(|d| st.doors.url(d));
        // ffmpeg's own http protocol follows a `Location` without asking, and has no option to stop it, so a link is
        // never handed to it: only the door (whose reads follow nothing) or a local file.
        let Some(input) = ffmpeg_input(door_url, &i.input) else {
            crate::log_limited("no_door", || {
                format!(
                    "session {}: no loopback door for the release, and ffmpeg is not given a link to read{}",
                    self.short(),
                    rid_tail(self.rid.as_deref())
                )
            });
            return Err(());
        };
        let spec = Spec {
            input: &input,
            seek,
            video: self.video(st, start),
            audio: self.audio,
            audio_out: self.audio_out,
            text: &self.text_tracks,
            dir: &dir,
        };
        match Job::spawn(&st.cfg.ffmpeg, id, start, &spec, permit) {
            Ok(new) => {
                // So a process after this one can place the run's GOPs (`resume::adopt`).
                crate::resume::note_job_start(&dir, start);
                if let Some(old) = i.job.replace(new) {
                    // A run that already exited said so when it did.
                    if old.exit.is_none() {
                        eprintln!(
                            "session {}: {}{}",
                            self.short(),
                            old.pull_line(&format!("replaced for segment {n}")),
                            rid_tail(self.rid.as_deref())
                        );
                    }
                    tokio::spawn(old.stop());
                }
                st.jobs_started.fetch_add(1, Relaxed);
                Ok(())
            }
            Err(e) => {
                crate::log_limited("ffmpeg_spawn", || {
                    format!(
                        "session {}: ffmpeg would not start: {e}{}",
                        self.short(),
                        rid_tail(self.rid.as_deref())
                    )
                });
                i.failures += 1;
                Err(())
            }
        }
    }

    /// The sid of the best RUNNING job to reap for a session about to wait for a producer slot: among
    /// `candidates` (every OTHER session with one actually running, paired with how long since its own last
    /// request), the one idle longest — but only once idle for at least `min_idle`. `None` when every
    /// candidate is still within `min_idle`: none of them is a sound victim, and the waiter queues fairly
    /// instead. A job merely paused for filling its ahead window is never a candidate at all — it gave its
    /// permit back when it paused (`Job::pause`), so there is nothing to take from it; only a job genuinely
    /// still running past its own session going quiet is an abandoned one worth taking back.
    fn pick_reap_victim<'a>(
        candidates: impl Iterator<Item = (&'a str, Duration)>,
        min_idle: Duration,
    ) -> Option<&'a str> {
        candidates.filter(|(_, idle)| *idle >= min_idle).max_by_key(|&(_, idle)| idle).map(|(sid, _)| sid)
    }

    /// Free a producer slot for `waiter`, about to wait for one, by reaping the best candidate among every
    /// OTHER session's RUNNING job (`pick_reap_victim`) — a producer still running after its own session has
    /// gone quiet for `min_idle`, which, with `gate`'s pause already giving back a merely ahead-and-paused
    /// job's permit on its own, is now the only kind of "stuck" producer there is. Does nothing, and logs
    /// nothing, when none qualifies: every running job's session is still within `min_idle` of its own last
    /// request, so there is genuinely no spare capacity right now, and `waiter` simply queues fairly.
    async fn reap_for_contention(st: &AppState, waiter: &str, min_idle: Duration) {
        let victim = {
            let sessions = st.sessions();
            let ages: Vec<(String, Duration)> = sessions
                .iter()
                .filter(|(sid, _)| sid.as_str() != waiter)
                .filter_map(|(sid, s)| {
                    let i = s.lock();
                    i.job
                        .as_ref()
                        .filter(|j| j.exit.is_none() && !j.stopped)
                        .map(|_| (sid.clone(), i.last_seen.elapsed()))
                })
                .collect();
            Self::pick_reap_victim(ages.iter().map(|(sid, age)| (sid.as_str(), *age)), min_idle)
                .map(str::to_owned)
        };
        let Some(victim_sid) = victim else { return };
        let Some(victim_session) = st.sessions().get(&victim_sid).cloned() else { return };
        let reaped = {
            let mut i = victim_session.lock();
            let idle = i.last_seen.elapsed();
            match i.job.as_ref().filter(|j| j.exit.is_none() && !j.stopped) {
                Some(_) if idle >= min_idle => i.job.take().map(|job| (job, idle)),
                _ => None,
            }
        };
        let Some((job, idle)) = reaped else { return };
        eprintln!(
            "session {}: {}{} — idle {:.0}s, its producer slot goes to {waiter}'s wait",
            victim_session.short(),
            job.pull_line("reaped under contention"),
            rid_tail(victim_session.rid.as_deref()),
            idle.as_secs_f64(),
        );
        job.stop().await;
    }

    async fn start_producer(&self, st: &Arc<AppState>, n: usize, deadline: Instant) -> Result<(), ()> {
        let started = Instant::now();
        tokio::time::timeout_at(deadline.into(), self.ensure_resolved(st)).await.map_err(|_| ())??;
        let gate =
            tokio::time::timeout_at(deadline.into(), self.producer_gate.lock()).await.map_err(|_| ())?;
        let old = {
            let mut i = self.lock();
            self.refresh(&mut i, st);
            if !self.producer_needed(&i, n)? {
                return Ok(());
            }
            i.job.take()
        };
        if let Some(old) = old {
            if old.exit.is_none() {
                eprintln!(
                    "session {}: {}{}",
                    self.short(),
                    old.pull_line(&format!("replaced for segment {n}")),
                    rid_tail(self.rid.as_deref())
                );
            }
            old.stop().await;
        }
        // Only worth the walk over every other session when there is in fact no spare slot: the common case,
        // one or more of `MAX_ACTIVE_REMUXES` free, costs nothing extra.
        if st.producer_slots_busy() {
            Self::reap_for_contention(st, &self.sid, st.cfg.producer_idle).await;
        }
        let permit = tokio::time::timeout_at(deadline.into(), st.acquire_producer(self.transcoded))
            .await
            .map_err(|_| ())?
            .ok_or(())?;
        let result = {
            let mut i = self.lock();
            self.refresh(&mut i, st);
            self.ensure_job(&mut i, n, st, permit)
        };
        drop(gate);
        self.wake.notify_one();
        // A resumed (merely SIGSTOPped) job answers in microseconds; this path is only reached once one had to
        // be freshly spawned, so a slow one here is a cold restart — a seek into the source, a new ffmpeg —
        // taking long enough to matter against a native player's own, much shorter patience for a stalled
        // request (`Session::prepare`'s doc).
        let elapsed = started.elapsed();
        if result.is_ok() && elapsed >= RESTART_LOG_THRESHOLD {
            eprintln!(
                "session {}: segment {n}'s producer took {:.1}s to restart{}",
                self.short(),
                elapsed.as_secs_f64(),
                rid_tail(self.rid.as_deref())
            );
        }
        result
    }

    /// Pause the job once it is `AHEAD_SEGMENTS` past the newest request — or, once that request's segment is
    /// done, while scratch is over its cap — and resume it when the player catches up, given a free permit.
    ///
    /// The resume is best-effort and non-blocking: this runs from inside the session's own lock, so it cannot
    /// wait for one. Finding none free costs nothing beyond leaving the job paused a moment longer — the
    /// session's own next request for a segment this job no longer covers falls back to the fair, async wait
    /// in `start_producer` regardless, which resumes the very same job rather than restarting it (`ensure_job`).
    fn gate(&self, i: &mut Inner, st: &AppState) {
        let last = self.segments.len() - 1;
        // A segment further, with the release's own subtitles: a player asks for a subtitle segment as far ahead as
        // it buffers video, and one is only made once the job has read a little past it.
        let ahead_segments = AHEAD_SEGMENTS + usize::from(!self.text_tracks.is_empty());
        let limit = self.segments[(i.want + ahead_segments).min(last)].end;
        let wanted_done = self.segments[i.want.min(last)].end;
        let over_cap = st.scratch_bytes.load(Relaxed) > st.cfg.scratch_max_bytes;
        if let Some(j) = i.job.as_mut().filter(|j| j.exit.is_none()) {
            let ahead = j.next_start >= limit - SNAP;
            if ahead || (over_cap && j.next_start >= wanted_done - SNAP) {
                j.pause();
            } else if j.resumable() {
                if let Some(permit) = st.try_acquire_producer_slot(self.transcoded) {
                    j.resume(permit);
                }
            }
        }
    }

    /// Delete the GOPs outside the window around segment `n`: everything before the previous segment,
    /// and anything a dead job left far ahead.
    fn prune(&self, i: &mut Inner, n: usize, st: &AppState) {
        let last = self.segments.len() - 1;
        let lo = self.segments[n.saturating_sub(1)].start - SNAP;
        let hi = self.segments[(n + AHEAD_SEGMENTS + 1).min(last)].end + SNAP;
        let current = i.job.as_ref().map(|j| j.id);
        let mut freed = 0;
        i.gops.retain(|g| {
            let keep = g.start + g.dur > lo && (g.start < hi || Some(g.job) == current);
            if !keep {
                let _ = std::fs::remove_file(&g.path);
                freed += g.size;
            }
            keep
        });
        i.bytes -= freed;
        st.scratch_bytes.fetch_sub(freed, Relaxed);
    }

    /// After a run that produced nothing, fetch a fresh link through scout, once: a debrid link can
    /// expire inside a long session. Done here rather than by handing ffmpeg scout's play URL, because
    /// following scout's redirect is where the service key has to be kept away from the debrid.
    async fn reresolve_if_needed(&self, st: &AppState) {
        {
            let mut i = self.lock();
            if i.ended || i.failures == 0 || i.reresolved {
                return;
            }
            i.reresolved = true;
        }
        // The next session for this release must not be handed the link that just failed.
        st.opened().forget(&self.opened_key);
        let play_url = crate::config::local(&self.lock().play_url, &st.cfg.origin_aliases);
        match scout::resolve(st.scout_via(), &play_url, &self.source, st.source_guard).await {
            Ok(r) => {
                if let Some(door) = self.door.get().and_then(Option::as_ref) {
                    door.set_upstream(r.client.clone(), &r.url);
                }
                self.lock().input = r.url;
            }
            Err(e) => eprintln!(
                "session {}: re-resolving the release failed: {}{}",
                self.short(),
                crate::redact::scrub(&e, &[&self.source.base]),
                rid_tail(self.rid.as_deref())
            ),
        }
    }

    /// A session rebuilt after a restart (`revive`) serves what scratch holds without its release; this fetches a
    /// link to the release again and opens its door, once, before the first ffmpeg run needs it. Through the play URL
    /// it was opened with, and — where that ticket no longer plays — through the title's listing again, for the same
    /// release. The link must lead to the very file the session was made from (`file_key`), or the segments made from
    /// it would not be the ones the player's playlist promises: such a session ends. Any other failure is passing, and
    /// is tried again no sooner than `RESOLVE_RETRY` later.
    async fn ensure_resolved(&self, st: &AppState) -> Result<(), ()> {
        if self.door.get().is_some() {
            return Ok(());
        }
        let _gate = self.resolve_gate.lock().await;
        if self.door.get().is_some() {
            return Ok(());
        }
        let play_url = {
            let i = self.lock();
            if i.ended || i.resolve_failed.is_some_and(|at| at.elapsed() < RESOLVE_RETRY) {
                return Err(());
            }
            i.play_url.clone()
        };
        /// Counts a fetch that didn't succeed as failed for `RESOLVE_RETRY` — also one cancelled midway, by a request's
        /// deadline (`start_producer`) or a dropped connection, which would otherwise send the next request straight
        /// back to scout.
        struct Backoff<'a>(&'a Session, bool);
        impl Drop for Backoff<'_> {
            fn drop(&mut self) {
                if self.1 {
                    self.0.lock().resolve_failed = Some(Instant::now());
                }
            }
        }
        let mut backoff = Backoff(self, true);
        let started = Instant::now();
        let secrets = [self.source.base.as_str(), play_url.as_str()];
        let fetched = match self.fetch_release(st, &play_url).await {
            Ok(fetched) => fetched,
            Err(e) => {
                crate::log_limited("resume_resolve", || {
                    format!(
                        "session {}: fetching the release again failed: {}{}",
                        self.short(),
                        crate::redact::scrub(&e, &secrets),
                        rid_tail(self.rid.as_deref())
                    )
                });
                return Err(());
            }
        };
        let (r, relisted) = fetched;
        // Without a file identity only the ticket the session was made with is trusted to lead to its file: a release
        // matched in a new listing by its name and size alone might not be the same one.
        let changed = match &self.file_key {
            Some(k) => file_key(&r).as_ref() != Some(k),
            None => relisted.is_some(),
        };
        if changed {
            eprintln!(
                "session {}: the release fetched again can't be shown to be the file the session was made from; \
                 ending it{}",
                self.short(),
                rid_tail(self.rid.as_deref())
            );
            st.end_session(&self.sid, "release changed").await;
            return Err(());
        }
        backoff.1 = false;
        let door = r.size.map(|size| st.doors.open(r.client.clone(), &r.url, &r.head, size));
        let _ = self.door.set(door);
        {
            let mut i = self.lock();
            i.input = r.url;
            i.resolve_failed = None;
            if let Some(url) = relisted {
                i.play_url = url;
            }
        }
        eprintln!(
            "session {}: release fetched again after a restart in {} ms{}",
            self.short(),
            started.elapsed().as_millis(),
            rid_tail(self.rid.as_deref())
        );
        Ok(())
    }

    /// The release behind `play_url`, or — where scout no longer plays that ticket — behind the play URL a fresh
    /// listing of the title gives the same release (`opened_key`), with that play URL.
    async fn fetch_release(
        &self,
        st: &AppState,
        play_url: &str,
    ) -> Result<(scout::Resolved, Option<String>), String> {
        let local = |url: &str| crate::config::local(url, &st.cfg.origin_aliases);
        let first =
            match scout::resolve(st.scout_via(), &local(play_url), &self.source, st.source_guard).await {
                Ok(r) => return Ok((r, None)),
                Err(e) => e,
            };
        let list = scout::list(st.scout_via(), &self.source, &self.imdb, None)
            .await
            .map_err(|e| format!("{first}; listing the title again: {}", e.detail))?;
        let again = list
            .iter()
            .find(|s| opened_key(&self.source.base, s) == self.opened_key)
            .ok_or_else(|| format!("{first}; the title's listing no longer has the release"))?;
        let r = scout::resolve(st.scout_via(), &local(&again.url), &self.source, st.source_guard).await?;
        Ok((r, Some(again.url.clone())))
    }

    /// Whether this session has its release: only one rebuilt after a restart may not yet (`ensure_resolved`).
    #[cfg(test)]
    pub fn resolved(&self) -> bool {
        self.door.get().is_some()
    }

    /// `ensure_resolved`, for a test to cancel or repeat.
    #[cfg(test)]
    pub async fn resolve_again(&self, st: &AppState) -> bool {
        self.ensure_resolved(st).await.is_ok()
    }

    /// Rendition `n`'s playlist: a segment per video segment from the release's own track where `subs::plan` chose
    /// it, else the one document den-subtitles serves spanning the film. A language with neither is the document,
    /// which serves as empty.
    pub fn subtitle_playlist(&self, n: usize) -> String {
        match self.renditions.get(n).map(|r| r.own) {
            None => String::new(),
            Some(Some(_)) => playlist::subtitle_segments(&self.segments, n),
            Some(None) => playlist::subtitle_media(self.info.duration, n),
        }
    }

    /// Rendition `n`'s own track over video segment `w`, once the job has read the film past it: the cues showing in
    /// it, as WebVTT. It waits a while for a job that is about to, and answers 503 like a video segment when none
    /// does. A subtitle never starts a job: the video's requests drive that, and a player that wants both asks for
    /// both.
    pub async fn serve_subtitle_window(&self, st: &AppState, n: usize, w: usize) -> Response<Body> {
        let (Some(track), Some(seg)) = (self.renditions.get(n).and_then(|r| r.own), self.segments.get(w))
        else {
            return httputil::not_found();
        };
        // Segment 0 starts at 0 whatever the file's first keyframe says, and a job for it starts at that keyframe.
        let from = seg.start.max(self.info.keyframes.first().copied().unwrap_or(0.0));
        let deadline = Instant::now() + SEGMENT_WAIT;
        loop {
            {
                let mut i = self.lock();
                if i.ended {
                    return gone();
                }
                i.last_seen = Instant::now();
                self.refresh(&mut i, st);
                self.gate(&mut i, st);
            }
            self.wake.notify_one();
            {
                let own = self.own.lock().unwrap_or_else(|e| e.into_inner());
                if own.covers(track, from, seg.end) {
                    let doc = own.window(track, seg.start, seg.end);
                    self.lock().subtitle_used = Some((n, "embedded"));
                    return httputil::text("text/vtt; charset=utf-8", &self.cache_control(), doc);
                }
            }
            if Instant::now() >= deadline {
                return busy();
            }
            tokio::time::sleep(POLL).await;
        }
    }

    /// Rendition `n`'s WebVTT: the first subtitle in its language that den-subtitles offers and serves, made once
    /// and kept for the session; `None` when there is none yet. Only what was found is kept: den-subtitles' list is
    /// kept once it answers, and a failed list or subtitle is asked for again on the player's next request, so one
    /// blip doesn't leave the session without subtitles.
    pub async fn subtitle(&self, st: &AppState, n: usize) -> Option<String> {
        let want = &self.renditions.get(n).filter(|r| r.den)?.lang;
        let sb = self.subs.as_ref()?;
        let mut cache = sb.cache.lock().await;
        if let Some(Some(doc)) = cache.docs.get(n) {
            return Some(doc.clone());
        }
        if cache.list.is_none() {
            cache.list = self.list_subtitles(st, sb).await;
        }
        let offered: Vec<crate::subs::Entry> = cache
            .list
            .iter()
            .flatten()
            .filter(|e| crate::lang::canonical(&e.lang) == *want)
            .take(3)
            .cloned()
            .collect();
        let mut doc = None;
        let mut selected = None;
        // Only a candidate den-subtitles itself named `sparse` (`X-Den-Degraded`, read by
        // `fetch_subtitle`) is worth asking again below — never one refused for any other reason
        // (a cue-less body, an expired link, a dead upload, a transient blip): `?sparse=1` would
        // refuse those identically, for a second metered credit spent finding that out again.
        let mut sparse_candidates: Vec<&crate::subs::Entry> = Vec::new();
        for entry in &offered {
            match self.fetch_subtitle(st, sb, &entry.url).await {
                Fetched::Found(d) => {
                    doc = Some(d);
                    selected = Some(entry.clone());
                    break;
                }
                Fetched::Sparse => sparse_candidates.push(entry),
                Fetched::Refused => {}
            }
        }
        // Every candidate den-subtitles offered for this language was tried, and the only ones that
        // refused as `sparse` are retried with `sparse=1` — the caller's own last resort — best-ranked
        // first, same as the strict pass above. This never overrides a non-sparse candidate: it only
        // runs once the whole strict pass has already failed to find one.
        if doc.is_none() {
            for entry in sparse_candidates {
                if let Fetched::Found(d) =
                    self.fetch_subtitle(st, sb, &crate::subs::with_sparse(&entry.url)).await
                {
                    doc = Some(d);
                    selected = Some(entry.clone());
                    break;
                }
            }
        }
        let doc = doc?;
        if cache.docs.len() <= n {
            cache.docs.resize(n + 1, None);
        }
        cache.docs[n] = Some(doc.clone());
        if cache.selected.len() <= n {
            cache.selected.resize(n + 1, None);
        }
        cache.selected[n] = selected;
        self.lock().subtitle_used = Some((n, "den-subtitles"));
        Some(doc)
    }

    /// den-subtitles' list for this title, with the release's hash, size and filename as its hints; `None` when it
    /// could not be had.
    async fn list_subtitles(&self, st: &AppState, sb: &crate::subs::Subs) -> Option<Vec<crate::subs::Entry>> {
        use crate::subs;
        // The list is asked with the release's hash, which reads the file's tail through its link.
        self.ensure_resolved(st).await.ok()?;
        // Hash through the session's loopback door, whose entry owns the vetted/pinned client. If
        // no door could be opened, omit the optional hash rather than redialing the release through
        // the ordinary service client.
        let door_input = self.door.get().and_then(|door| door.as_ref()).and_then(|door| st.doors.url(door));
        let input = door_input.clone().unwrap_or_default();
        let secrets = [sb.base.as_str()];
        let release = subs::Release {
            id: &self.imdb,
            input: &input,
            head_sum: door_input.and(sb.head_sum),
            size: self.release.size,
            filename: &self.release.filename,
        };
        subs::fetch_list(&st.scout_http, st.local_scout.get(), &st.source_http, &sb.base, &release)
            .await
            .inspect_err(|e| {
                crate::log_limited("subtitles_list", || {
                    format!(
                        "session {}: subtitles: {}{}",
                        self.short(),
                        crate::redact::scrub(e, &secrets),
                        rid_tail(self.rid.as_deref())
                    )
                });
            })
            .ok()
    }

    /// One subtitle as HLS WebVTT, if it is on an allowed origin and is WebVTT.
    async fn fetch_subtitle(&self, st: &AppState, sb: &crate::subs::Subs, url: &str) -> Fetched {
        use crate::subs;
        let vtt = subs::vtt_url(url);
        if !subs::on_origin(&vtt, &st.cfg.subtitle_origins) {
            crate::log_limited("subtitle_origin", || {
                format!(
                    "session {}: skipped a subtitle not on SUBTITLE_ORIGINS{}",
                    self.short(),
                    rid_tail(self.rid.as_deref())
                )
            });
            return Fetched::Refused;
        }
        let vtt = crate::config::local(&vtt, &st.cfg.origin_aliases);
        if let Some(call) = st
            .local_scout
            .get()
            .and_then(|local| local.get(&vtt, hyper::HeaderMap::new(), subs::MAX_SUBTITLE as usize))
        {
            let Ok(Ok(resp)) = tokio::time::timeout(crate::scout::SCOUT_TOTAL, call).await else {
                return Fetched::Refused;
            };
            if !resp.status().is_success() {
                return if resp.headers().get("x-den-degraded").is_some_and(|v| v == "sparse") {
                    Fetched::Sparse
                } else {
                    Fetched::Refused
                };
            }
            return subs::for_hls(resp.body()).map_or(Fetched::Refused, Fetched::Found);
        }
        let resp = match st.scout_http.get(&vtt).send().await {
            Ok(resp) => resp,
            Err(e) => {
                let e = e.without_url().to_string();
                crate::log_limited("subtitle_fetch", || {
                    format!(
                        "session {}: {}{}",
                        self.short(),
                        crate::redact::scrub(&e, &[sb.base.as_str()]),
                        rid_tail(self.rid.as_deref())
                    )
                });
                return Fetched::Refused;
            }
        };
        if !resp.status().is_success() {
            let status = resp.status().as_u16();
            let sparse = resp.headers().get("x-den-degraded").is_some_and(|v| v == "sparse");
            crate::log_limited("subtitle_fetch", || {
                format!(
                    "session {}: a subtitle answered {status}{}",
                    self.short(),
                    rid_tail(self.rid.as_deref())
                )
            });
            return if sparse { Fetched::Sparse } else { Fetched::Refused };
        }
        match crate::probe::read_capped(resp, subs::MAX_SUBTITLE).await {
            Ok(body) => match subs::for_hls(&body) {
                Some(doc) => Fetched::Found(doc),
                None => Fetched::Refused,
            },
            Err(e) => {
                crate::log_limited("subtitle_fetch", || {
                    format!(
                        "session {}: {}{}",
                        self.short(),
                        crate::redact::scrub(&e, &[sb.base.as_str()]),
                        rid_tail(self.rid.as_deref())
                    )
                });
                Fetched::Refused
            }
        }
    }

    /// Stop the job, reap it, and delete the session's scratch, after logging the one line that explains —
    /// and, with its session id and `rid`, lets a reader reproduce — what played. `why` names how it ended
    /// (`idle`, `expired`, `deleted`, `replaced`, …). `false` if it had already ended, which logs nothing.
    pub async fn end(&self, st: &AppState, why: &str) -> bool {
        let (job, segments_served, seconds_served, subtitle_used) = {
            let mut i = self.lock();
            if i.ended {
                return false;
            }
            i.ended = true;
            // Before anything this can be cancelled at: a dropped request ending it, or a shutdown's cut, must never
            // leave a record a process after this one rebuilds an ended session from.
            crate::resume::forget(&self.dir);
            st.scratch_bytes.fetch_sub(i.bytes, Relaxed);
            i.bytes = 0;
            i.gops.clear();
            (i.job.take(), i.segments_served, i.seconds_served, i.subtitle_used)
        };
        self.resync.cancel.send_replace(true);
        let subtitle = subtitle_used
            .map(|(idx, source)| (idx, self.renditions.get(idx).map_or("und", |r| r.lang.as_str()), source));
        eprintln!(
            "{}",
            session_summary_line(
                self.short(),
                self.rid.as_deref(),
                st.cfg.log_identity,
                why,
                &self.imdb,
                &self.release.label,
                self.release.size,
                self.audio,
                self.info.audio.get(self.audio).and_then(|a| a.language.as_deref()).unwrap_or("und"),
                self.audio_copy.unwrap_or("aac"),
                self.audio_channels,
                subtitle,
                self.transcoded,
                self.transcode_reason.as_deref(),
                segments_served,
                seconds_served,
            )
        );
        if let Some(gid) = self.owner.strip_prefix("grant:") {
            report_usage(st, gid, self.started.elapsed().as_secs());
        }
        if let Some(j) = job {
            if j.exit.is_none() {
                eprintln!(
                    "session {}: {}{}",
                    self.short(),
                    j.pull_line("session ended"),
                    rid_tail(self.rid.as_deref())
                );
            }
            j.stop().await;
        }
        let dir = self.dir.clone();
        let _ = tokio::task::spawn_blocking(move || crate::resume::remove(&dir)).await;
        self.wake.notify_one();
        true
    }

    /// Stop the session for a shutdown without ending it: its ffmpeg is killed and reaped, and its scratch — its
    /// record, `init.mp4` and finished GOPs — is kept for the next process to rebuild it from (`resume`).
    pub async fn suspend(&self, st: &AppState) {
        let job = {
            let mut i = self.lock();
            if i.ended {
                return;
            }
            i.ended = true;
            st.scratch_bytes.fetch_sub(i.bytes, Relaxed);
            i.bytes = 0;
            i.job.take()
        };
        self.resync.cancel.send_replace(true);
        if let Some(j) = job {
            if j.exit.is_none() {
                eprintln!(
                    "session {}: {}{}",
                    self.short(),
                    j.pull_line("suspended for a restart"),
                    rid_tail(self.rid.as_deref())
                );
            }
            j.stop().await;
        }
        let dir = self.dir.clone();
        let _ = tokio::task::spawn_blocking(move || crate::resume::touch(&dir)).await;
        self.wake.notify_one();
    }

    /// What this session is rebuilt from after a restart (`resume`): `start_at` is where its playlist was cut for,
    /// `started_at` when it was created, in unix seconds.
    fn record(&self, start_at: f64, started_at: u64) -> crate::resume::Record {
        let i = self.lock();
        crate::resume::Record {
            schema: crate::resume::RECORD_SCHEMA,
            sid: self.sid.clone(),
            exp: self.exp,
            owner: self.owner.clone(),
            public: self.public,
            started_at,
            rid: self.rid.clone(),
            imdb: self.imdb.clone(),
            label: self.release.label.clone(),
            filename: self.release.filename.clone(),
            size: self.release.size,
            pack_size: self.release.pack_size,
            scout_base: self.source.base.clone(),
            play_url: i.play_url.clone(),
            service_key: self.source.key.is_some(),
            opened_key: self.opened_key.clone(),
            file_key: self.file_key.clone(),
            info: crate::state::StoredMediaInfo::from(&self.info),
            start_at,
            audio: self.audio,
            audio_copy: self.audio_copy.map(String::from),
            audio_out: self.audio_out,
            audio_channels: self.audio_channels,
            subtitles: self.subs.as_ref().map(|s| s.base.clone()),
            head_sum: self.subs.as_ref().and_then(|s| s.head_sum),
            renditions: self.renditions.clone(),
            dovi: self.dovi,
            transcoded: self.transcoded,
            transcode_reason: self.transcode_reason.clone(),
            preset: self.preset,
            max_bitrate: self.max_bitrate,
            need: self.need,
            master: self.master.clone(),
            media: self.media.clone(),
        }
    }

    /// The session `rec` describes, with what scratch still holds of it (`adopted`), and no release yet
    /// (`ensure_resolved`). `None` when the record's playlist is not the one its own probe and start cut — a record
    /// from rules this build no longer cuts by, which a player holding that playlist could not be served from.
    fn from_record(
        st: &AppState,
        rec: crate::resume::Record,
        info: MediaInfo,
        adopted: crate::resume::Adopted,
    ) -> Option<Session> {
        let closed_gops = rec.transcoded || info.closed_gops;
        let segments =
            playlist::segments_for_start(&info.keyframes, info.duration, playlist::TARGET_SECS, rec.start_at);
        if segments.is_empty() || playlist::media(&segments, rec.start_at, closed_gops) != rec.media {
            return None;
        }
        let text_tracks: Vec<usize> = rec.renditions.iter().filter_map(|r| r.own).collect();
        let mut own = crate::subs::OwnCues::default();
        for run in &adopted.runs {
            for &t in &text_tracks {
                own.take(run.id, t, &job::text_file(&run.dir, t), run.start, run.upto);
            }
        }
        let audio_copy = match rec.audio_copy.as_deref() {
            None => None,
            Some(c) => Some(dolby_codec(c).or_else(|| flac_codec(c))?),
        };
        let ago = Duration::from_secs(unix_now().saturating_sub(rec.started_at));
        Some(Session {
            sid: rec.sid.clone(),
            sig: crate::auth::url_sig(&st.cfg.url_key, &rec.sid, rec.exp),
            exp: rec.exp,
            owner: rec.owner,
            public: rec.public,
            started: Instant::now().checked_sub(ago).unwrap_or_else(Instant::now),
            rid: rec.rid,
            imdb: rec.imdb,
            dir: st.cfg.scratch_dir.join(format!("s-{}", rec.sid)),
            release: Release {
                label: rec.label,
                filename: rec.filename,
                size: rec.size,
                pack_size: rec.pack_size,
                requested: None,
            },
            info,
            audio: rec.audio,
            audio_copy,
            audio_out: rec.audio_out,
            audio_channels: rec.audio_channels,
            subs: rec.subtitles.map(|base| crate::subs::Subs {
                base,
                head_sum: rec.head_sum,
                cache: Default::default(),
            }),
            renditions: rec.renditions,
            text_tracks,
            own: Mutex::new(own),
            dovi: rec.dovi,
            transcoded: rec.transcoded,
            transcode_reason: rec.transcode_reason,
            preset: rec.preset,
            segments,
            // What only the answer to `POST /remux/session` carried.
            prebuffer: None,
            demand: None,
            need: rec.need,
            max_bitrate: rec.max_bitrate,
            master: rec.master,
            media: rec.media,
            reports: AtomicU8::new(0),
            speed_probes: AtomicU8::new(0),
            door: std::sync::OnceLock::new(),
            source: scout::ScoutSource {
                base: rec.scout_base,
                key: if rec.service_key { st.cfg.scout_key.clone() } else { None },
            },
            opened_key: rec.opened_key,
            file_key: rec.file_key,
            resolve_gate: tokio::sync::Mutex::new(()),
            inner: Mutex::new(Inner {
                job: None,
                next_job: adopted.next_job,
                gops: adopted.gops,
                finished: adopted.finished,
                init: adopted.init,
                last_seen: Instant::now(),
                last_heartbeat: None,
                want: 0,
                ended: false,
                bytes: adopted.bytes,
                failures: 0,
                input: String::new(),
                reresolved: false,
                play_url: rec.play_url,
                warming: false,
                resolve_failed: None,
                // Counted from the restart on: the summary line says what this process served.
                segments_served: 0,
                seconds_served: 0.0,
                subtitle_used: None,
            }),
            producer_gate: tokio::sync::Mutex::new(()),
            wake: Notify::new(),
            resync: Default::default(),
        })
    }
}

/// What `revive` made of a request for a session this process doesn't know.
pub enum Revived {
    /// Rebuilt from its record, or by a request just ahead of this one.
    Session(Arc<Session>),
    /// No record vouches for it under this signature: as absent as a session that never existed.
    Unknown,
    /// Its signature holds, but it can't come back: expired, or a transcode with no GPU to run it now.
    Gone,
    /// Its owner's share or the server is full right now; a player's retry may find room.
    Busy,
}

/// A request for the session `sid` that this process doesn't know: rebuild it from its record in scratch, if one opens
/// and `sig` signs it (`resume`). One rebuild at a time, so many requests arriving together after a restart build it
/// once and the rest find it built. Nothing here calls scout: the session serves what scratch holds, and fetches its
/// release only as that runs out.
///
/// Only a session startup found suspended (`AppState::take_up_scratch`) and nothing has since ended or revoked is ever
/// rebuilt: any other sid is unknown without a look at the disk.
pub async fn revive(st: &Arc<AppState>, sid: &str, sig: &str) -> Revived {
    if st.kept(sid).is_none() {
        return Revived::Unknown;
    }
    let _one = st.revive_lock.lock().await;
    if let Some(s) = st.session(sid) {
        return Revived::Session(s);
    }
    // Revoked, or forgotten, while this waited its turn.
    if st.kept(sid).is_none() {
        return Revived::Unknown;
    }
    let dir = st.cfg.scratch_dir.join(format!("s-{sid}"));
    let (key, owned_sid, owned_sig, d) =
        (st.cfg.url_key.clone(), sid.to_string(), sig.to_string(), dir.clone());
    // The signature is checked before anything in scratch is touched; the expiry it covers is the record's.
    let opened = tokio::task::spawn_blocking(move || {
        let rec = crate::resume::read(&d, &key, &owned_sid)?;
        if !crate::auth::url_sig_ok(&key, &owned_sid, rec.exp, &owned_sig) {
            return None;
        }
        let info = rec.info.clone().into_media()?;
        let adopted =
            (unix_now() < rec.exp).then(|| crate::resume::adopt(&d, &info.keyframes, info.duration));
        Some((rec, info, adopted))
    })
    .await
    .ok()
    .flatten();
    let Some((rec, info, adopted)) = opened else { return Revived::Unknown };
    let short = &sid[..6];
    let gone = |why: &str| {
        eprintln!("session {short}: not resumed after a restart: {why}");
        st.unkeep(sid);
        let dir = dir.clone();
        tokio::task::spawn_blocking(move || crate::resume::remove(&dir))
    };
    let Some(adopted) = adopted else {
        let _ = gone("expired").await;
        return Revived::Gone;
    };
    if rec.transcoded && !st.can_transcode() {
        let _ = gone("it is a transcode, and transcoding is unavailable").await;
        return Revived::Gone;
    }
    let (gops, init) = (adopted.gops.len(), adopted.init.is_some());
    let Some(session) = Session::from_record(st, rec, info, adopted) else {
        let _ = gone("its record no longer cuts the playlist it promised").await;
        return Revived::Gone;
    };
    let session = Arc::new(session);
    let bytes = session.lock().bytes;
    if !st.insert_revived(session.clone()) {
        crate::log_limited("resume_full", || {
            format!("session {short}: not resumed yet: its owner's share or the server is full")
        });
        return Revived::Busy;
    }
    // Its scratch is the session's to count now: what startup measured gives way to what it adopted.
    st.unkeep(sid);
    st.scratch_bytes.fetch_add(bytes, Relaxed);
    eprintln!(
        "session {short}: resumed after a restart with {gops} GOP(s){} from scratch{}",
        if init { " and its init.mp4" } else { "" },
        rid_tail(session.rid.as_deref())
    );
    tokio::spawn(supervise(st.clone(), session.clone()));
    Revived::Session(session)
}

/// oxyc/den#100's follow-up: tell den-edge how long a guest grant's session played, so its "your guests" list can
/// show plays and hours without this box ever naming a title, a release or an address — only `gid` and `seconds`
/// leave. Fire-and-forget, on the one secret the two already share (`EDGE_SECRET`/`x-den-edge-secret`): a lost
/// report costs the host an undercount, never a session that fails to end.
/// `pub(crate)` only so `tests.rs` can call it directly, without a real ffmpeg session, to prove what it sends.
pub(crate) fn report_usage(st: &AppState, gid: &str, seconds: u64) {
    let (Some(base), Some(secret)) = (st.cfg.edge_report_url.clone(), st.cfg.edge_secret.clone()) else {
        return;
    };
    if seconds == 0 {
        return;
    }
    let http = st.http.clone();
    let gid = gid.to_owned();
    tokio::spawn(async move {
        let body = serde_json::json!({ "gid": gid, "seconds": seconds }).to_string();
        let sent = http
            .post(format!("{base}/grant/usage"))
            .header("x-den-edge-secret", secret)
            .header("content-type", "application/json")
            .timeout(Duration::from_secs(5))
            .body(body)
            .send()
            .await;
        if let Err(e) = sent {
            // `without_url`: a reqwest::Error's Display otherwise carries the request URL — den-edge's own
            // base, not a secret, but still never worth a stray log line naming an address.
            eprintln!("grant usage report: {}", e.without_url());
        }
    });
}

/// What a browser asked for in `POST /remux/session`.
pub struct Want<'a> {
    /// Scout's title id: `tt…`, or `tt…:<season>:<episode>` for an episode.
    pub id: &'a str,
    /// The release to prefer, when it is playable here.
    pub filename: Option<&'a str>,
    /// The scout install from the web app's library, accepted only on a `SCOUT_ORIGINS` origin: a full
    /// one, or — for a logged-in browser, presented with `REMUX_SCOUT_KEY` — a scope=availability one.
    /// Without it this service's own `SCOUT_INSTALL_URL` is used, for a logged-in browser only, which is
    /// how it can be driven by hand.
    pub scout: Option<&'a str>,
    /// Audio languages, most wanted first.
    pub audio: &'a [String],
    /// One audio track by index, overriding `audio` — from an earlier session's `audioTracks`, with that
    /// session's `filename`.
    pub audio_track: Option<usize>,
    /// den-subtitles' install from the web app's library, accepted only on a `SUBTITLE_ORIGINS` origin.
    pub subtitles: Option<&'a str>,
    /// Subtitle languages to offer as renditions, most wanted first.
    pub subtitle_languages: &'a [String],
    /// The title's original language, as its catalogue names it: what tells a dub from original audio
    /// (`subs::dub_language`). `None` is read as original audio.
    pub original_language: Option<&'a str>,
    /// The video codecs the player takes (`h264`, `hevc`); empty is both. Without HEVC, H.264 releases
    /// are tried first and an HEVC one is transcoded on the GPU.
    pub video_codecs: &'a [String],
    /// What the player decodes, level by level; when given, it decides over `video_codecs`.
    pub playable: Option<&'a Playable>,
    /// Seconds into the title the player starts at: a resume. 0 from the start.
    pub start_at: f64,
    /// The bits a second the player's link carries, as it measured them: a remote player's. A copy needing more is
    /// taken only when nothing fits, after a transcode at the preset the link takes. `None` is a player at home.
    pub max_bitrate: Option<u64>,
    /// Which browser asked, for the log (`client::label`): `Chrome 151 · macOS · hls.js`.
    pub client: String,
    /// The HLS player it plays in (`native`, `hls.js`, `cast`): how far ahead it buffers (`buffer_of`).
    pub player: Option<&'a str>,
    /// Releases not to open, by filename: the one a player is switching away from.
    pub exclude: &'a [String],
    /// No transcode for this request: a player replacing a session mid-film, which a conversion never replaces one.
    pub no_transcode: bool,
    /// Only a copy that fits the link will do: no copy that would starve on it, and no transcode. A player switching
    /// away from a release its link can't carry asks this, and keeps playing what it has when nothing fits.
    pub fits_only: bool,
    /// The session this one replaces mid-film, which plays on until this one has served a segment
    /// (`AppState::reserve`).
    pub replaces: Option<&'a str>,
    /// The creating request's `X-Request-Id` (`redact::request_id`), so the session's own log lines —
    /// including the ones this call writes before the session exists, while still picking a release —
    /// can be joined back to the request that started it with one grep.
    pub rid: Option<&'a str>,
}

/// What the player decodes (`playable` in `POST /remux/session`). An HEVC release beyond it is transcoded on the GPU;
/// an H.264, AV1 or VP9 one is passed over.
pub enum Admission {
    /// A browser logged in with its key. For it den-remux presents its own scout key, so an
    /// availability-only install — or the `SCOUT_INSTALL_URL` fallback — can play.
    Browser(String),
    /// No login: the scout install the request names is the credential, as every addon's install URL is.
    /// den-remux's key is not sent, so scout alone decides: a full install lists and plays, and an
    /// availability-only, revoked or foreign one does not.
    Install,
    /// A guest's grant, vouched for by den-edge (`EDGE_SECRET`): the install is as for `Install`, but the sessions
    /// belong to the grant (`grant:<gid>`), are capped by `GUEST_MAX_SESSIONS`, and share the one transcode with every host.
    Guest(String),
}

/// The scout install a request names, or this service's own. Validated as sent, then fetched at its LAN address
/// when that is a public name: scout is on this box. The LAN form is also what names the install, whichever name
/// the browser used.
fn scout_base(st: &AppState, scout: Option<&str>) -> Result<String, ApiError> {
    match scout {
        Some(url) => Ok(crate::config::local(
            &scout::validate_scoped(url, &st.cfg.scout_origins).map_err(|why| {
                api(StatusCode::BAD_REQUEST, "bad_scout", format!("The scout URL was refused: {why}."))
            })?,
            &st.cfg.origin_aliases,
        )),
        None => st.cfg.scout_install_url.clone().ok_or_else(|| {
            api(
                StatusCode::SERVICE_UNAVAILABLE,
                "scout_unconfigured",
                "No scout URL in the request, and no SCOUT_INSTALL_URL.",
            )
        }),
    }
}

/// Scout's releases for `id`, ranked for the browser that sent `playable`, its refusals put in this API's terms.
async fn scout_list(
    st: &AppState,
    source: &scout::ScoutSource,
    id: &str,
    by_install: bool,
    playable: Option<&Playable>,
) -> Result<Vec<scout::Stream>, ApiError> {
    let secrets = [source.base.as_str()];
    scout::list(st.scout_via(), source, id, playable).await.map_err(|e| match e.status {
        // Out of scope: an availability-only install, which plays only with den-remux's key — sent for a
        // logged-in browser alone.
        Some(403) if by_install => api(
            StatusCode::UNAUTHORIZED,
            "not_logged_in",
            "This scout install can't play on its own; log in with this browser's key.",
        ),
        // Scout would not open the install: revoked, out of scope, or not one of its own.
        Some(status @ (400 | 403)) => {
            crate::log_limited("scout_refused", || format!("scout: refused the install ({status})"));
            api(
                StatusCode::FORBIDDEN,
                "scout_refused",
                "Scout refused this install: revoked, or one it can't open.",
            )
        }
        _ => {
            crate::log_limited("scout_list", || {
                format!("scout: {}", crate::redact::scrub(&e.detail, &secrets))
            });
            api(StatusCode::BAD_GATEWAY, "scout_unavailable", "Could not list releases.")
        }
    })
}

/// Whether the player takes HEVC as it is: by `playable`, else by `videoCodecs` (empty is both).
fn player_takes_hevc(playable: Option<&Playable>, video_codecs: &[String]) -> bool {
    let named = video_codecs.is_empty()
        || video_codecs
            .iter()
            .any(|c| matches!(c.to_ascii_lowercase().as_str(), "hevc" | "h265" | "hvc1" | "hev1"));
    playable.map_or(named, Playable::takes_hevc)
}

/// Whether a release plays for the player that asked, as `POST /remux/releases` says it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Plays {
    /// As it is (or nothing says otherwise).
    Yes,
    /// Only converted on the server's GPU.
    Convert,
    /// Not at all: a session skips it.
    No,
}

impl Plays {
    pub fn as_str(self) -> &'static str {
        match self {
            Plays::Yes => "yes",
            Plays::Convert => "convert",
            Plays::No => "no",
        }
    }
}

/// A listed release and how it plays for the player that asked.
pub struct Verdict {
    pub stream: scout::Stream,
    pub plays: Plays,
    /// A short reason a person can read, where it is not a plain yes.
    pub why: Option<String>,
}

/// What opening a release showed of it, as a verdict for this player: the probe's own answer, by the very tests a
/// session applies to what it opens.
fn probed_verdict(
    info: &MediaInfo,
    playable: Option<&Playable>,
    takes_hevc: bool,
) -> (Plays, Option<String>) {
    if no_picture(playable, takes_hevc, info) {
        let profile = info.dolby_vision.map_or(5, |dv| dv.profile);
        let record = if info.dolby_vision_recordless { " (no record in the file)" } else { "" };
        return (
            Plays::No,
            Some(format!("Dolby Vision profile {profile}{record} — this browser can't show it")),
        );
    }
    if info.grid() {
        let container = if info.container == "avi" { "AVI" } else { "MPEG-TS" };
        return (Plays::Convert, Some(format!("{container} is converted on the server")));
    }
    if plays(playable, takes_hevc, info) {
        return (Plays::Yes, None);
    }
    let codec = info.codecs.as_deref().unwrap_or("its video");
    match info.video {
        VideoCodec::Hevc => {
            (Plays::Convert, Some(format!("{codec} is converted on the server for this browser")))
        }
        _ => (Plays::No, Some(format!("{codec} is beyond this browser"))),
    }
}

/// A release's verdict before it is opened, from scout's attributes.
fn scouted_verdict(
    a: &scout::Attributes,
    playable: Option<&Playable>,
    takes_hevc: bool,
) -> (Plays, Option<String>) {
    match fit(a, playable, takes_hevc) {
        Fit::Copy => (Plays::Yes, None),
        Fit::Convert if a.dv_profile == 5 => (
            Plays::Convert,
            Some("Dolby Vision profile 5 — this browser can't show it, so it plays only if its picture has a base layer".into()),
        ),
        Fit::Convert => (Plays::Convert, Some("converted on the server for this browser".into())),
        Fit::Never => (Plays::No, Some(format!("{} — this browser can't decode it", a.codec.as_deref().unwrap_or("its video")))),
    }
}

/// `POST /remux/releases`: the releases a session could play, in the order it would try them, so a player can name
/// one (`filename`), each with how it plays for a player that says what it decodes (`playable`, `videoCodecs`):
/// by scout's attributes, or — once a session has opened the release — by what the probe found. The caller gets
/// their labels, names and sizes — never a URL. Without a report every release is `yes`, but one a session found
/// plays in no browser (`AppState::unplayable`).
pub async fn releases(
    st: &Arc<AppState>,
    admission: Admission,
    scout: Option<&str>,
    id: &str,
    playable: Option<&Playable>,
    video_codecs: &[String],
) -> Result<Vec<Verdict>, ApiError> {
    let by_install = !matches!(admission, Admission::Browser(_));
    let key = match admission {
        Admission::Browser(_) => st.cfg.scout_key.clone(),
        Admission::Install | Admission::Guest(_) => None,
    };
    let source = scout::ScoutSource { base: scout_base(st, scout)?, key };
    // Without a capability report no AV1 or VP9: nothing says the player decodes them.
    let list = scout_list(st, &source, id, by_install, playable).await?;
    let candidates = scout::candidates(
        &list,
        None,
        playable.is_some_and(Playable::takes_av1),
        playable.is_some_and(Playable::takes_vp9),
    );
    let reported = playable.is_some() || !video_codecs.is_empty();
    let takes_hevc = player_takes_hevc(playable, video_codecs);
    Ok(candidates
        .into_iter()
        .map(|stream| {
            let nowhere = unplayable_key(id, &stream).and_then(|k| st.unplayable().get(&k, unix_now()));
            let (plays, why) = if let Some(why) = nowhere {
                (Plays::No, Some(why))
            } else if !reported {
                (Plays::Yes, None)
            } else if let Some(info) = st.known().get(&known_key(id, &stream)) {
                probed_verdict(&info, playable, takes_hevc)
            } else {
                scouted_verdict(&stream.attributes, playable, takes_hevc)
            };
            Verdict { stream, plays, why }
        })
        .collect())
}

/// ` rid=<id>` when the session's creating request had one, else nothing: the tail every one of a
/// session's own log lines ends with, besides its `session <short>:` prefix, so one grep over either
/// joins a request's own log line to everything its session went on to do.
fn rid_tail(rid: Option<&str>) -> String {
    rid.map(|r| format!(" rid={r}")).unwrap_or_default()
}

/// The one line logged once per session (`event=audio_track`), naming the audio track that plays and
/// why: `idx` and `lang` are identity under `LOG_IDENTITY` — together they say which of the release's
/// language tracks was picked — `codec`, `channels` and the reason itself are not, since none of them
/// alone says what title is playing.
#[allow(clippy::too_many_arguments)]
fn audio_track_line(
    short: &str,
    rid: Option<&str>,
    identity: bool,
    idx: usize,
    lang: &str,
    codec: &str,
    channels: u32,
    reason: &crate::lang::AudioReason,
) -> String {
    let rid = rid_tail(rid);
    let who = if identity { format!(" idx={idx} lang={lang}") } else { String::new() };
    format!(
        "session {short}: event=audio_track outcome=served reason={reason}{rid} session={short} codec={codec} \
         channels={channels}{who}"
    )
}

/// The one line logged when a session ends (`event=session`): with the session id and `rid` already on every
/// line it wrote while playing, this is what lets a report be explained AND reproduced from the box's journal
/// alone. `why` (`idle`, `expired`, `deleted`, `replaced`, `replacement never played`, a source error, …),
/// the session id, codecs, channel counts, whether a track was chosen and where from, and the segment/second
/// counts are always on; the title id, release label, size and the chosen audio/subtitle's own language are
/// identity, gated by `LOG_IDENTITY`.
#[allow(clippy::too_many_arguments)]
fn session_summary_line(
    short: &str,
    rid: Option<&str>,
    identity: bool,
    why: &str,
    imdb: &str,
    label: &str,
    size: Option<u64>,
    audio_idx: usize,
    audio_lang: &str,
    audio_codec: &str,
    audio_channels: u32,
    subtitle: Option<(usize, &str, &str)>,
    transcoded: bool,
    transcode_reason: Option<&str>,
    segments_served: u64,
    seconds_served: f64,
) -> String {
    let rid = rid_tail(rid);
    let id = if identity {
        format!(" imdb={imdb} label={label:?} size={}", size.map_or("unknown".to_string(), |s| s.to_string()))
    } else {
        String::new()
    };
    let audio_lang = if identity { format!(" audio_lang={audio_lang}") } else { String::new() };
    let sub = match subtitle {
        Some((idx, lang, source)) => {
            let who =
                if identity { format!(" subtitle_idx={idx} subtitle_lang={lang}") } else { String::new() };
            format!(" subtitle={source}{who}")
        }
        None => " subtitle=none".to_string(),
    };
    let transcode = match (transcoded, transcode_reason) {
        (true, Some(why)) => format!(" transcoded=yes transcode_reason={why:?}"),
        (true, None) => " transcoded=yes".to_string(),
        (false, _) => " transcoded=no".to_string(),
    };
    format!(
        "session {short}: event=session outcome=ended reason={why}{rid} session={short} audio_idx={audio_idx} \
         audio_codec={audio_codec} audio_channels={audio_channels}{audio_lang}{sub}{transcode} \
         segments={segments_served} seconds={seconds_served:.1}{id}"
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ffmpeg_never_receives_an_unvetted_remote_fallback() {
        assert_eq!(ffmpeg_input(None, "https://debrid.invalid/secret"), None);
        assert_eq!(
            ffmpeg_input(Some("http://127.0.0.1:4321/token".into()), "https://debrid.invalid/secret"),
            Some("http://127.0.0.1:4321/token".into())
        );
        assert_eq!(ffmpeg_input(None, "/var/tmp/local.mkv"), Some("/var/tmp/local.mkv".into()));
    }

    fn gop(job: u32, idx: u32, start: f64, dur: f64) -> Gop {
        Gop { job, idx, start, dur, path: PathBuf::from(format!("j{job}/g{idx}")), size: 1 }
    }

    #[test]
    fn audio_track_line_drops_identity_when_asked() {
        let reason = crate::lang::AudioReason::Preferred("fi".into());
        let on = audio_track_line("abc123", Some("r1"), true, 2, "fi", "ec-3", 6, &reason);
        assert_eq!(
            on,
            "session abc123: event=audio_track outcome=served reason=preferred language fi rid=r1 \
             session=abc123 codec=ec-3 channels=6 idx=2 lang=fi"
        );
        let off = audio_track_line("abc123", Some("r1"), false, 2, "fi", "ec-3", 6, &reason);
        assert_eq!(
            off,
            "session abc123: event=audio_track outcome=served reason=preferred language fi rid=r1 \
             session=abc123 codec=ec-3 channels=6"
        );
        assert!(!off.contains("idx=") && !off.contains("lang="), "{off}");
        let no_rid =
            audio_track_line("abc123", None, true, 0, "en", "aac", 2, &crate::lang::AudioReason::First);
        assert!(!no_rid.contains("rid="), "{no_rid}");
    }

    #[test]
    fn session_summary_line_drops_identity_when_asked() {
        let on = session_summary_line(
            "abc123",
            Some("r1"),
            true,
            "idle",
            "tt1234567",
            "Movie.2160p.mkv",
            Some(123),
            0,
            "en",
            "ec-3",
            6,
            Some((1, "fi", "den-subtitles")),
            true,
            Some("no copy plays within a 10s head start"),
            42,
            252.0,
        );
        assert_eq!(
            on,
            "session abc123: event=session outcome=ended reason=idle rid=r1 session=abc123 audio_idx=0 \
             audio_codec=ec-3 audio_channels=6 audio_lang=en subtitle=den-subtitles subtitle_idx=1 \
             subtitle_lang=fi transcoded=yes transcode_reason=\"no copy plays within a 10s head start\" \
             segments=42 seconds=252.0 imdb=tt1234567 label=\"Movie.2160p.mkv\" size=123"
        );
        let off = session_summary_line(
            "abc123",
            Some("r1"),
            false,
            "idle",
            "tt1234567",
            "Movie.2160p.mkv",
            Some(123),
            0,
            "en",
            "ec-3",
            6,
            Some((1, "fi", "den-subtitles")),
            true,
            Some("no copy plays within a 10s head start"),
            42,
            252.0,
        );
        assert_eq!(
            off,
            "session abc123: event=session outcome=ended reason=idle rid=r1 session=abc123 audio_idx=0 \
             audio_codec=ec-3 audio_channels=6 subtitle=den-subtitles transcoded=yes \
             transcode_reason=\"no copy plays within a 10s head start\" segments=42 seconds=252.0"
        );
        assert!(!off.contains("imdb=") && !off.contains("label=") && !off.contains("lang="), "{off}");
        let no_subtitle = session_summary_line(
            "abc123", None, true, "expired", "tt1", "X", None, 0, "en", "aac", 2, None, false, None, 0, 0.0,
        );
        assert!(no_subtitle.contains("subtitle=none"), "{no_subtitle}");
        assert!(no_subtitle.contains("transcoded=no"), "{no_subtitle}");
        assert!(!no_subtitle.contains("rid="), "{no_subtitle}");
    }

    const SEG: Segment = Segment { start: 8.0, end: 13.0 };

    fn info(video: VideoCodec, codecs: &str, hdr: bool) -> crate::probe::MediaInfo {
        crate::probe::MediaInfo {
            container: "matroska",
            duration: 1.0,
            video,
            codecs: Some(codecs.into()),
            width: 3840,
            height: 2160,
            hdr,
            hlg: false,
            frame_rate: None,
            dolby_vision: None,
            dolby_vision_record_mismatch: false,
            dolby_vision_recordless: false,
            audio: Vec::new(),
            subtitles: Vec::new(),
            keyframes: vec![0.0],
            closed_gops: true,
            byte_index: Vec::new(),
            video_bytes: None,
        }
    }

    #[test]
    fn a_copied_hdr_variant_names_its_range() {
        let hdr10 = info(VideoCodec::Hevc, "hvc1.2.4.L153.B0", true);
        assert_eq!(
            copied_range(&hdr10, "hvc1.2.4.L153.B0"),
            Some("PQ"),
            "HDR HEVC without a transfer is HDR10"
        );
        let hlg = crate::probe::MediaInfo { hlg: true, ..hdr10.clone() };
        assert_eq!(copied_range(&hlg, "hvc1.2.4.L153.B0"), Some("HLG"));
        let sdr = info(VideoCodec::Hevc, "hvc1.1.6.L120.90", false);
        assert_eq!(copied_range(&sdr, "hvc1.1.6.L120.90"), None, "SDR names no range");
        let av1 = info(VideoCodec::Av1, "av01.0.13M.10.0.110.09.16.09.0", true);
        assert_eq!(copied_range(&av1, "av01.0.13M.10.0.110.09.16.09.0"), Some("PQ"));
        assert_eq!(copied_range(&info(VideoCodec::H264, "avc1.640028", false), "avc1.640028"), None);
        let hlg_vp9 = crate::probe::MediaInfo {
            hlg: true,
            ..info(VideoCodec::Vp9, "vp09.02.51.10.01.09.18.09.00", true)
        };
        assert_eq!(copied_range(&hlg_vp9, "vp09.02.51.10.01.09.18.09.00"), Some("HLG"));
        assert_eq!(copied_range(&info(VideoCodec::Vp9, "vp09.00.41.08", false), "vp09.00.41.08"), None);
    }

    #[test]
    fn vp9_plays_as_it_is_in_the_profile_the_player_reports() {
        let chrome = Playable { h264: 0x33, vp9: true, vp9_profile2: true, ..Playable::default() };
        let p0 = info(VideoCodec::Vp9, "vp09.00.41.08", false);
        let p2 = info(VideoCodec::Vp9, "vp09.02.51.10.01.09.16.09.00", true);
        assert!(chrome.takes(&p0) && chrome.takes(&p2));
        assert!(!Playable { vp9: false, ..chrome }.takes(&p0), "profile 2 only");
        assert!(!Playable { vp9_profile2: false, ..chrome }.takes(&p2), "profile 0 only");
        assert!(!chrome.takes(&info(VideoCodec::Vp9, "vp09.02.51.12", false)), "12-bit: never asked about");
        assert!(!chrome.takes(&info(VideoCodec::Vp9, "vp09.01.41.08.03.01.01.01.00", false)), "profile 1");
        // Every player now gets the report as the browser gave it. Profile 2 was cleared for a native session
        // while 10-bit VP9 in Apple's own player was unmeasured; codec lab played it there on an iPhone (842
        // frames of a thirty-second clip) and in every path on macOS Safari (oxyc/den#37).
        let sent = serde_json::to_value(chrome).unwrap();
        assert!(sent["vp9"] == true && sent["vp9Profile2"] == true, "scout is sent it as it holds: {sent}");
        assert!(chrome.to_string().ends_with("AV1 10-bit L0, VP9, VP9 profile 2"), "{chrome}");

        let a = |bit_depth| scout::Attributes { codec: Some("vp9".into()), bit_depth, ..Default::default() };
        assert_eq!(fit(&a(8), Some(&chrome), false), Fit::Copy, "VP9 the player decodes is copied");
        assert_eq!(
            fit(&a(10), Some(&Playable { vp9_profile2: false, ..chrome }), false),
            Fit::Never,
            "8-bit only"
        );
        assert_eq!(fit(&a(8), Some(&Playable { vp9: false, ..chrome }), false), Fit::Never, "10-bit only");
        let ten_bit_only = Playable { vp9: false, ..chrome };
        assert_eq!(
            fit(&a(0), Some(&ten_bit_only), false),
            Fit::Copy,
            "a depth nobody read: the probe decides"
        );
        let no_vp9 = Playable { vp9: false, vp9_profile2: false, ..chrome };
        assert_eq!(fit(&a(0), Some(&no_vp9), false), Fit::Never, "and never converted");
        assert_eq!(fit(&a(0), None, true), Fit::Never, "without a report nothing says it plays");
    }

    #[test]
    fn flac_is_copied_and_7_1_stays_7_1_where_the_player_says_so() {
        use job::AudioOut as Out;
        let chrome =
            Playable { h264: 0x33, aac_multichannel: true, flac: true, aac71: true, ..Playable::default() };
        assert_eq!(audio_plan("A_FLAC", 6, Some(&chrome)), (Some("fLaC"), Out::Copy));
        assert_eq!(audio_plan("fLaC", 2, Some(&chrome)), (Some("fLaC"), Out::Copy), "MP4's name for it");
        assert_eq!(audio_plan("A_FLAC", 6, Some(&Playable { flac: false, ..chrome })), (None, Out::Surround));
        assert_eq!(audio_plan("A_TRUEHD", 8, Some(&chrome)), (None, Out::Surround71));
        let no_71 = Playable { aac71: false, ..chrome };
        assert_eq!(audio_plan("A_DTS", 8, Some(&no_71)), (None, Out::Surround), "7.1 folds down to 5.1");
        assert_eq!(audio_plan("A_DTS", 6, Some(&chrome)), (None, Out::Surround), "5.1 stays 5.1");
        assert_eq!(
            audio_plan("A_EAC3", 8, Some(&chrome)),
            (None, Out::Surround71),
            "no eac3, no E-AC-3 copy"
        );
        assert_eq!(
            audio_plan("A_EAC3", 6, Some(&Playable { eac3: true, ..chrome })),
            (Some("ec-3"), Out::Copy)
        );
        assert_eq!(audio_plan("A_FLAC", 8, None), (None, Out::Stereo), "no report: stereo, as before");
        assert!(chrome.to_string().ends_with("AAC 5.1, AAC 7.1, FLAC"), "{chrome}");
    }

    #[test]
    fn dolby_vision_counts_as_hdr_unless_its_base_layer_is_sdr() {
        let mut release = info(VideoCodec::Hevc, "hvc1.2.4.L153.B0", false);
        assert!(!tonemaps(&release));
        release.dolby_vision = Some(crate::probe::DolbyVision { profile: 8, compat: 1, level: 6 });
        assert!(tonemaps(&release), "a remux often names no transfer in its header");
        release.dolby_vision = Some(crate::probe::DolbyVision { profile: 8, compat: 2, level: 6 });
        assert!(!tonemaps(&release), "profile 8.2's base layer is SDR already");
        assert!(tonemaps(&info(VideoCodec::Hevc, "hvc1.2.4.L153.B0", true)));
    }

    #[test]
    fn a_player_takes_what_its_levels_and_hdr_allow() {
        let phone = Playable {
            h264: 0x33,
            hevc_main: 153,
            hevc_main10: 153,
            hevc_high_tier: 0,
            hdr: true,
            ..Playable::default()
        };
        let hobbit = info(VideoCodec::Hevc, "hvc1.2.4.L153.B0", true);
        assert!(phone.takes(&hobbit));
        let bluray = info(VideoCodec::Hevc, "hvc1.2.4.H153.B0", true);
        assert!(!phone.takes(&bluray), "High tier, which an iPhone doesn't decode");
        assert!(Playable { hevc_high_tier: 153, ..phone }.takes(&bluray), "an Android phone that does");
        assert_eq!(
            phone.to_string(),
            "H.264 L51, High 10 L0, HEVC L153, 10-bit L153, High tier L0, HDR, AV1 L0, AV1 10-bit L0"
        );
        assert!(!Playable { hdr: false, ..phone }.takes(&hobbit), "HDR it can't decode is converted");
        assert!(!Playable { hevc_main10: 0, ..phone }.takes(&hobbit), "8-bit HEVC only");
        assert!(!Playable { hevc_main10: 123, ..phone }.takes(&hobbit), "1080p at most");
        let sdr_1080 = info(VideoCodec::Hevc, "hvc1.1.6.L120.90", false);
        assert!(Playable { hevc_main10: 0, ..phone }.takes(&sdr_1080), "8-bit 1080p");
        let firefox = Playable { h264: 0x33, ..Playable::default() };
        assert!(firefox.takes(&info(VideoCodec::H264, "avc1.640029", false)));
        assert!(!firefox.takes(&hobbit) && !firefox.takes_hevc());
        assert!(!Playable { h264: 0x29, ..firefox }.takes(&info(VideoCodec::H264, "avc1.640033", false)));
    }

    #[test]
    fn av1_plays_as_it_is_at_the_levels_depth_and_hdr_the_player_reports() {
        let chrome = Playable { h264: 0x33, av1: 13, av1_main10: 13, av1_hdr: true, ..Playable::default() };
        let hdr10 = info(VideoCodec::Av1, "av01.0.12M.10.0.110.09.16.09.0", true);
        assert!(chrome.takes(&hdr10));
        assert!(!Playable { av1_hdr: false, ..chrome }.takes(&hdr10), "PQ it can't decode is passed over");
        assert!(!Playable { av1_main10: 0, ..chrome }.takes(&hdr10), "8-bit AV1 only");
        assert!(!Playable { av1_main10: 9, ..chrome }.takes(&hdr10), "1080p at most");
        let sdr = info(VideoCodec::Av1, "av01.0.08M.08", false);
        assert!(Playable { av1: 0, av1_main10: 8, ..chrome }.takes(&sdr), "a 10-bit decoder takes 8-bit");
        assert!(!Playable { av1: 0, av1_main10: 0, ..chrome }.takes(&sdr));
        assert!(
            !chrome.takes(&info(VideoCodec::Av1, "av01.0.13H.10", false)),
            "High tier: never asked about"
        );
        let high = info(VideoCodec::Av1, "av01.1.08M.08.0.000.01.01.01.0", false);
        assert!(!chrome.takes(&high), "High profile, 4:4:4");
        assert!(!chrome.takes(&info(VideoCodec::Av1, "av01.2.08M.12", false)), "12-bit");
        assert!(chrome.to_string().ends_with("AV1 L13, AV1 10-bit L13, AV1 HDR"), "{chrome}");
    }

    #[test]
    fn dolby_vision_is_kept_and_named_where_the_player_shows_it() {
        use crate::probe::DolbyVision as Dv;
        let shows = |p5, p8| Playable { dolby_vision: DolbyVisionPlay { p5, p8 }, ..Playable::default() };
        let p81 = Dv { profile: 8, compat: 1, level: 6 };
        let p5 = Dv { profile: 5, compat: 0, level: 6 };
        let p76 = Dv { profile: 7, compat: 6, level: 6 };
        assert!(keeps_dolby_vision(p81, Some(&shows(false, true))));
        assert!(!keeps_dolby_vision(p81, Some(&shows(true, false))) && !keeps_dolby_vision(p81, None));
        assert!(keeps_dolby_vision(p5, Some(&shows(true, false))));
        assert!(!keeps_dolby_vision(p5, Some(&shows(false, true))));
        assert!(!keeps_dolby_vision(p76, Some(&shows(true, true))), "profile 7 always plays its base layer");
        let p86 = Dv { compat: 6, ..p81 };
        assert!(!keeps_dolby_vision(p86, Some(&shows(true, true))), "8.6 has no HLS brand");
        assert_eq!(dolby_vision_signal(p81), Some((None, Some("dvh1.08.06/db1p".into()), "PQ")));
        let sdr = Dv { compat: 2, level: 9, ..p81 };
        assert_eq!(dolby_vision_signal(sdr), Some((None, Some("dvh1.08.09/db2g".into()), "SDR")));
        assert_eq!(dolby_vision_signal(Dv { compat: 4, ..p81 }).map(|s| s.2), Some("HLG"));
        assert_eq!(dolby_vision_signal(p5), Some((Some("dvh1.05.06".into()), None, "PQ")));
        assert_eq!(dolby_vision_signal(p76), None);
    }

    #[test]
    fn a_contradictory_profile5_record_always_uses_the_hdr_base_layer() {
        let mut release = info(VideoCodec::Hevc, "hvc1.2.4.L153.B0", true);
        release.dolby_vision = Some(crate::probe::DolbyVision { profile: 5, compat: 0, level: 6 });
        release.dolby_vision_record_mismatch = true;
        let player = Playable {
            hevc_main10: 153,
            hdr: true,
            dolby_vision: DolbyVisionPlay { p5: true, p8: true },
            ..Playable::default()
        };
        assert_eq!(
            kept_dolby_vision(&release, false, Some(&player)),
            None,
            "never signal the false P5 record"
        );
        let want = Want {
            id: "tt1",
            filename: None,
            scout: None,
            audio: &[],
            audio_track: None,
            subtitles: None,
            subtitle_languages: &[],
            original_language: None,
            video_codecs: &[],
            playable: Some(&player),
            start_at: 0.0,
            max_bitrate: None,
            client: "test".into(),
            player: None,
            exclude: &[],
            no_transcode: false,
            fits_only: false,
            replaces: None,
            rid: None,
        };
        assert!(!no_picture(want.playable, true, &release), "the BT.2020 base layer has a picture");
    }

    #[test]
    fn a_recordless_profile5_plays_only_as_it_is_for_a_player_that_shows_profile5() {
        let mut release = info(VideoCodec::Hevc, "hvc1.2.4.L153.B0", false);
        release.dolby_vision = Some(crate::probe::DolbyVision { profile: 5, compat: 0, level: 6 });
        release.dolby_vision_recordless = true;
        let safari = &Playable {
            hevc_main10: 153,
            hdr: true,
            dolby_vision: DolbyVisionPlay { p5: true, p8: true },
            ..Playable::default()
        };
        let chrome = &Playable { hevc_main10: 153, hdr: true, ..Playable::default() };
        let no_hevc =
            &Playable { dolby_vision: DolbyVisionPlay { p5: true, p8: true }, ..Playable::default() };
        // Safari keeps it: its master names `dvh1.05.06` and VIDEO-RANGE=PQ, and the copy keeps its RPUs.
        assert!(!no_picture(Some(safari), true, &release));
        let kept = kept_dolby_vision(&release, false, Some(safari)).expect("kept as it is");
        assert_eq!(dolby_vision_signal(kept), Some((Some("dvh1.05.06".into()), None, "PQ")));
        // Anywhere else it is refused, never stripped and never converted: profile 5 has no picture without its RPU.
        assert!(no_picture(Some(chrome), true, &release));
        assert!(
            no_picture(Some(no_hevc), false, &release),
            "a player without HEVC is not offered a conversion"
        );
        assert_eq!(probed_verdict(&release, Some(safari), true), (Plays::Yes, None));
        let (plays, why) = probed_verdict(&release, Some(chrome), true);
        assert_eq!(plays, Plays::No);
        assert_eq!(
            why.unwrap(),
            "Dolby Vision profile 5 (no record in the file) — this browser can't show it"
        );
        assert_eq!(kept_dolby_vision(&release, true, Some(safari)), None, "a transcode drops Dolby Vision");
    }

    #[test]
    fn only_dolby_tracks_are_copied() {
        assert_eq!(dolby_codec("A_EAC3"), Some("ec-3"));
        assert_eq!(dolby_codec("ec-3"), Some("ec-3"));
        assert_eq!(dolby_codec("A_AC3"), Some("ac-3"));
        assert_eq!(dolby_codec("A_AC3/BSID9"), Some("ac-3"));
        assert_eq!(dolby_codec("ac-3"), Some("ac-3"));
        for other in ["A_TRUEHD", "A_DTS", "A_AAC", "mp4a", "A_OPUS"] {
            assert_eq!(dolby_codec(other), None, "{other}");
        }
    }

    #[test]
    fn scouts_attributes_rank_releases_before_any_is_opened() {
        let a = |codec: &str| scout::Attributes { codec: Some(codec.into()), ..scout::Attributes::default() };
        let safari =
            Playable { h264: 0x33, hevc_main: 153, hevc_main10: 153, hdr: true, ..Playable::default() };
        let firefox = Playable { h264: 0x33, ..Playable::default() };
        let p = Some(&safari);
        assert_eq!(fit(&a("h264"), p, true), Fit::Copy);
        let hi10p = scout::Attributes { bit_depth: 10, ..a("h264") };
        assert_eq!(fit(&hi10p, p, true), Fit::Never, "10-bit H.264 and no High 10 decoder");
        assert_eq!(fit(&hi10p, Some(&Playable { h264_high10: 0x33, ..safari }), true), Fit::Copy);
        let hdr_word = scout::Attributes { bit_depth: 10, hdr: true, ..a("h264") };
        assert_eq!(fit(&hdr_word, p, true), Fit::Copy, "a 10 inferred from an HDR word is not High 10");
        let uhd_h264 = scout::Attributes { resolution: Some("2160p".into()), ..a("h264") };
        assert_eq!(fit(&uhd_h264, Some(&Playable { h264: 0x29, ..firefox }), false), Fit::Never);
        let p5 = scout::Attributes { dv_profile: 5, hdr: true, ..a("hevc") };
        assert_eq!(
            fit(&p5, p, true),
            Fit::Convert,
            "opened last so a false P5 record can be corrected by the probe"
        );
        let shows_p5 = Playable { dolby_vision: DolbyVisionPlay { p5: true, p8: false }, ..safari };
        assert_eq!(fit(&p5, Some(&shows_p5), true), Fit::Copy, "a player that shows profile 5 opens it");
        let uhd = scout::Attributes { resolution: Some("2160p".into()), ..a("hevc") };
        let hd_only = Playable { hevc_main: 123, hevc_main10: 123, ..safari };
        assert_eq!(fit(&uhd, Some(&hd_only), true), Fit::Convert, "4K past a 1080p decoder");
        let hdr10 = scout::Attributes { hdr: true, bit_depth: 10, ..a("hevc") };
        assert_eq!(fit(&hdr10, Some(&Playable { hdr: false, ..safari }), true), Fit::Convert);
        assert_eq!(fit(&hdr10, p, true), Fit::Copy);
        assert_eq!(fit(&a("hevc"), Some(&firefox), false), Fit::Convert);
        assert_eq!(fit(&a("hevc"), None, false), Fit::Convert, "videoCodecs without HEVC");
        let unknown = scout::Attributes::default();
        assert_eq!(fit(&unknown, Some(&firefox), false), Fit::Copy, "unknown: the probe decides");
        let chrome = Playable { h264: 0x33, av1: 13, av1_main10: 13, ..Playable::default() };
        assert_eq!(fit(&a("av1"), Some(&chrome), false), Fit::Copy, "AV1 the player decodes is copied");
        assert_eq!(fit(&a("av1"), Some(&firefox), false), Fit::Never, "and never converted");
        assert_eq!(fit(&a("av1"), None, true), Fit::Never, "without a report nothing says it plays");
        let ten_bit_av1 = scout::Attributes { bit_depth: 10, ..a("av1") };
        assert_eq!(
            fit(&ten_bit_av1, Some(&Playable { av1_main10: 0, ..chrome }), false),
            Fit::Never,
            "8-bit only"
        );
        let uhd_av1 = scout::Attributes { resolution: Some("2160p".into()), ..ten_bit_av1 };
        assert_eq!(
            fit(&uhd_av1, Some(&Playable { av1_main10: 9, ..chrome }), false),
            Fit::Never,
            "1080p at most"
        );
        assert_eq!(fit(&uhd_av1, Some(&chrome), false), Fit::Copy);
        let hdr_av1 = scout::Attributes { hdr: true, ..uhd_av1 };
        assert_eq!(fit(&hdr_av1, Some(&chrome), false), Fit::Never, "HDR without av1Hdr");
        assert_eq!(fit(&hdr_av1, Some(&Playable { av1_hdr: true, ..chrome }), false), Fit::Copy);
    }

    #[test]
    fn an_opened_release_is_remembered_per_install_whatever_its_ticket() {
        let s = scout::Stream {
            title: "f.mkv".into(),
            url: "http://scout/p/t1".into(),
            attributes: scout::Attributes::default(),
            hints: scout::Hints::default(),
        };
        let key = opened_key("http://scout/c2VhbGVk", &s);
        assert!(!key.contains("c2VhbGVk"), "the install is named by its hash, never its URL: {key}");
        assert_ne!(key, opened_key("http://scout/b3RoZXI", &s), "another install opens its own");
        let reticketed = scout::Stream { url: "http://scout/p/t2".into(), ..s.clone() };
        let again = opened_key("http://scout/c2VhbGVk", &reticketed);
        assert_eq!(key, again, "a new listing's ticket, the same release");
    }

    #[test]
    fn ten_bit_h264_plays_only_where_high_10_is_decoded() {
        let hi10p = info(VideoCodec::H264, "avc1.6e0028", false);
        let safari = Playable { h264: 0x33, ..Playable::default() };
        assert!(!safari.takes(&hi10p), "8-bit H.264 at any level is not High 10");
        let chrome = Playable { h264_high10: 0x33, ..safari };
        assert!(chrome.takes(&hi10p));
        assert!(!Playable { h264_high10: 0x1f, ..chrome }.takes(&hi10p), "level 4.0 is past 3.1");
        assert!(chrome.takes(&info(VideoCodec::H264, "avc1.640028", false)), "8-bit High still by `h264`");
    }

    #[test]
    fn a_segment_is_ready_once_its_gops_cover_it() {
        // 8 → 10.5 → 13, as h264.mkv's keyframes fall.
        let gops = [gop(1, 0, 0.0, 2.5), gop(1, 3, 8.0, 2.5), gop(1, 4, 10.5, 2.5), gop(1, 5, 13.0, 3.0)];
        assert_eq!(ready_chain(&gops, &[], SEG, false), Some(vec![1, 2]));
        // Only the first half written so far.
        assert_eq!(ready_chain(&gops[..2], &[], SEG, false), None);
    }

    #[test]
    fn gops_are_never_joined_across_jobs() {
        // Job 1 made 8 → 10.5 and was replaced; job 2 started at 10.5. Their GOPs are compatible, but a
        // chain has to come from one run to be known to be contiguous.
        let gops = [gop(1, 0, 8.0, 2.5), gop(2, 0, 10.5, 2.5)];
        assert_eq!(ready_chain(&gops, &[], SEG, false), None);
        let gops = [gop(1, 0, 8.0, 2.5), gop(2, 0, 10.5, 2.5), gop(3, 0, 8.0, 5.0)];
        assert_eq!(ready_chain(&gops, &[], SEG, false), Some(vec![2]), "a complete chain from another job");
    }

    #[test]
    fn the_last_segment_ends_with_its_jobs_last_gop() {
        // The container's duration runs a little past the last video frame.
        let last = Segment { start: 24.0, end: 30.021 };
        let gops = [gop(1, 0, 24.0, 3.5), gop(1, 1, 27.5, 2.46)];
        assert_eq!(ready_chain(&gops, &[], last, false), None, "still running: more may come");
        assert_eq!(ready_chain(&gops, &[1], last, false), Some(vec![0, 1]));
    }

    #[test]
    fn a_transcode_comes_down_to_its_preset_with_its_aspect() {
        let hd = job::HD1080;
        assert_eq!(transcode_size(3840, 2160, hd), (1920, 1080));
        assert_eq!(transcode_size(3840, 1600, hd), (1920, 800), "a wide film is fitted to the width");
        assert_eq!(transcode_size(4096, 2160, hd), (1920, 1012));
        assert_eq!(transcode_size(1440, 1080, hd), (1440, 1080));
        assert_eq!(transcode_size(1280, 720, hd), (1280, 720), "never scaled up");
        assert_eq!(transcode_size(1920, 803, hd), (1920, 802), "both even");
        assert_eq!(transcode_size(0, 0, hd), (0, 0));
        assert_eq!(transcode_size(3840, 2160, job::HD720), (1280, 720));
        assert_eq!(transcode_size(3840, 1600, job::HD720), (1280, 532));
    }

    /// Twelve minutes with a keyframe every 2 s: two cheap minutes at 1 Mbit/s, two heavy ones at 10, then eight cheap
    /// ones again — the shape of a film that opens on titles and then cuts to its first real scene.
    fn front_loaded() -> (crate::probe::MediaInfo, u64) {
        let keyframes: Vec<f64> = (0..360).map(|i| i as f64 * 2.0).collect();
        let mut at = 0u64;
        let byte_index: Vec<(f64, u64)> = keyframes
            .iter()
            .map(|&t| {
                let here = at;
                at += if (120.0..240.0).contains(&t) { 2_500_000 } else { 250_000 };
                (t, here)
            })
            .collect();
        let info = crate::probe::MediaInfo {
            duration: 720.0,
            keyframes,
            byte_index,
            ..info(VideoCodec::H264, "avc1.640028", false)
        };
        (info, at)
    }

    /// Delivering all of `size` bytes from `start_at` to a player that buffers as `player` does.
    fn delivery(size: u64, start_at: f64, player: Option<&str>) -> Delivery {
        Delivery { start_at, buffer: buffer_of(player), bytes: Some(size) }
    }

    #[test]
    fn a_link_that_covers_the_average_but_not_the_heavy_stretch_is_refused() {
        let (info, size) = front_loaded();
        assert_eq!(average_bitrate(Some(size), info.duration), Some(2_500_000));
        // hls.js holds two minutes and 150 MB: all of the heavy stretch. Everything to its end, 1320 Mbit, must be in by
        // the time its last 6 s segment plays (234 s) plus the 10 s allowed: 1320 / 244.
        let deep = delivery(size, 0.0, Some("hls.js"));
        let from_start = need(&info, &deep).unwrap();
        assert!(from_start.indexed);
        assert!((5_400_000..5_420_000).contains(&from_start.bitrate), "{from_start:?}");
        assert!(over(Some(4_000_000), Some(from_start)), "4 Mbit/s carries the average, not the film");
        assert!(
            over(Some(6_000_000), Some(from_start)),
            "6 Mbit/s carries it with 10% to spare, short of the 30%"
        );
        assert!(!over(Some(8_000_000), Some(from_start)));
        let wait = prebuffer(&info, &deep, Some(6_000_000)).unwrap();
        assert!((0.0..=MAX_PREBUFFER).contains(&wait), "{wait}");
        // At 4 Mbit/s even two minutes ahead can't carry the 1200 Mbit stretch (it needs 5.1): no wait up front would do.
        assert_eq!(prebuffer(&info, &deep, Some(4_000_000)), None);
        assert!(prebuffer(&info, &deep, Some(5_300_000)).unwrap() > MAX_PREBUFFER);
        // Resumed at the heavy stretch, the cheap opening no longer builds a lead: 6 Mbit/s isn't enough there.
        let resumed = need(&info, &delivery(size, 121.0, Some("hls.js"))).unwrap();
        assert!(resumed.bitrate > 9_000_000, "{resumed:?}");
        assert!(over(Some(6_000_000), Some(resumed)));
    }

    #[test]
    fn a_player_that_holds_thirty_seconds_needs_the_heavy_stretch_carried_as_it_plays() {
        let (info, size) = front_loaded();
        // Safari's thirty seconds ahead: the stretch's 1200 Mbit come in 30 s plus the 114 s between its first and last
        // segment's starts, whatever the cheap opening did.
        let native = delivery(size, 0.0, Some("native"));
        let held = need(&info, &native).unwrap();
        assert!((8_300_000..8_350_000).contains(&held.bitrate), "{held:?}");
        assert!(over(Some(6_000_000), Some(held)), "what carries it for hls.js starves Safari");
        assert_eq!(prebuffer(&info, &native, Some(6_000_000)), None, "and no wait up front would do");
        assert!(prebuffer(&info, &native, Some(9_000_000)).is_some_and(|w| w <= MAX_PREBUFFER));
        assert_eq!(
            buffer_of(None),
            buffer_of(Some("native")),
            "a player that says nothing is taken for the least"
        );
    }

    #[test]
    fn the_demand_is_what_the_session_sends_not_every_track_in_the_file() {
        let (info, size) = front_loaded();
        // Half the file is video; the rest, two 3 Mbit/s audio tracks.
        let track = |bytes| crate::probe::AudioTrack {
            codec: "A_EAC3".into(),
            language: Some("eng".into()),
            channels: 6,
            name: None,
            default: true,
            commentary: false,
            bytes: Some(bytes),
        };
        let info = crate::probe::MediaInfo {
            video_bytes: Some(size / 2),
            audio: vec![track(size / 4), track(size / 4)],
            ..info
        };
        let want = |playable| Want { playable, player: Some("hls.js"), ..test_want() };
        // Copied, the one track played: three quarters of the file.
        let eac3 = Playable { eac3: true, ..Default::default() };
        assert_eq!(Delivery::of(&info, Some(size), &want(Some(&eac3))).bytes, Some(size / 2 + size / 4));
        // Converted to stereo AAC: the video and 192 kbit/s for 720 s.
        assert_eq!(Delivery::of(&info, Some(size), &want(None)).bytes, Some(size / 2 + 17_280_000));
        let whole = need(&info, &delivery(size, 0.0, Some("hls.js"))).unwrap().bitrate;
        let sent = need(&info, &Delivery::of(&info, Some(size), &want(None))).unwrap().bitrate;
        assert!(sent < whole * 6 / 10, "{sent} of {whole}");
        // Where the file doesn't say how big its tracks are, the whole file stands in.
        let untold = crate::probe::MediaInfo { video_bytes: None, ..info.clone() };
        assert_eq!(Delivery::of(&untold, Some(size), &want(None)).bytes, Some(size));
    }

    #[test]
    fn a_resume_past_the_end_is_weighed_from_the_start() {
        let (info, size) = front_loaded();
        let want_at = |start_at| Delivery::of(&info, Some(size), &Want { start_at, ..test_want() }).start_at;
        assert_eq!(want_at(5_000.0), 0.0, "a start the session will make from zero");
        assert_eq!(want_at(720.0), 0.0);
        assert_eq!(want_at(121.0), 121.0);
    }

    fn test_want() -> Want<'static> {
        Want {
            id: "tt1",
            filename: None,
            scout: None,
            audio: &[],
            audio_track: None,
            subtitles: None,
            subtitle_languages: &[],
            original_language: None,
            video_codecs: &[],
            playable: None,
            start_at: 0.0,
            max_bitrate: None,
            client: "test".into(),
            player: None,
            exclude: &[],
            no_transcode: false,
            fits_only: false,
            replaces: None,
            rid: None,
        }
    }

    #[test]
    fn with_no_byte_index_a_copy_needs_its_average_and_half_again() {
        let plain = info(VideoCodec::H264, "avc1.640028", false);
        let plain = crate::probe::MediaInfo { duration: 30.0, ..plain };
        // 3.75 MB over 30 s is 1 Mbit/s.
        let n = need(&plain, &delivery(3_750_000, 0.0, None)).unwrap();
        assert_eq!(n, Need { bitrate: 1_500_000, indexed: false });
        assert!(n.to_string().contains("no byte index"), "the log says it is a guess: {n}");
        assert!(!over(None, Some(n)), "no maxBitrate: every release fits");
        assert!(!over(Some(2_142_858), Some(n)), "at the limit, which keeps its headroom, fits");
        assert!(over(Some(2_142_000), Some(n)));
        let unknown = Delivery { bytes: None, ..delivery(0, 0.0, None) };
        assert_eq!(need(&plain, &unknown), None, "an unknown size says nothing");
        assert!(!over(Some(1), None), "and nothing said fits");
        assert_eq!(
            need(&crate::probe::MediaInfo { duration: 0.0, ..plain.clone() }, &delivery(1, 0.0, None)),
            None
        );
        assert_eq!(
            prebuffer(&plain, &delivery(3_750_000, 0.0, None), Some(1)),
            None,
            "no index, no pre-buffer"
        );
    }

    #[test]
    fn snapping_pins_drift_to_the_real_keyframes() {
        let kf = [0.0, 2.5, 5.0];
        assert_eq!(snap(&kf, 2.500_004), 2.5);
        assert_eq!(snap(&kf, 4.99), 5.0);
        assert_eq!(snap(&kf, 3.7), 3.7, "a keyframe the index does not list keeps its own time");
    }

    fn inner(last_seen: Instant, last_heartbeat: Option<Instant>, segments_served: u64) -> Inner {
        Inner {
            job: None,
            next_job: 0,
            gops: Vec::new(),
            finished: Vec::new(),
            init: None,
            last_seen,
            last_heartbeat,
            want: 0,
            ended: false,
            bytes: 0,
            failures: 0,
            input: String::new(),
            reresolved: false,
            play_url: String::new(),
            warming: false,
            resolve_failed: None,
            segments_served,
            seconds_served: 0.0,
            subtitle_used: None,
        }
    }

    #[test]
    fn a_never_watched_session_or_a_lapsed_heartbeat_idles_far_sooner_than_session_idle() {
        let now = Instant::now();
        let session_idle = Duration::from_secs(600);

        // Nothing has ever been served: no "paused mid-film" to protect, so it is freed quickly.
        assert_eq!(inner(now, None, 0).idle_deadline(session_idle), now + UNATTENDED_IDLE);

        // Actively playing, or a player with no heartbeat of its own: the full, original grace.
        assert_eq!(inner(now, None, 3).idle_deadline(session_idle), now + session_idle);

        // Paused with a fresh heartbeat: the short grace, but counted from the heartbeat, not the
        // (older) last real request — so it keeps renewing for as long as the heartbeat does.
        let paused_since = now - Duration::from_secs(120);
        assert_eq!(inner(paused_since, Some(now), 3).idle_deadline(session_idle), now + UNATTENDED_IDLE);

        // Resumed after a stale heartbeat: fresher real activity supersedes it, back to the full grace —
        // a resumed, actively-playing session must not inherit a pause's short fuse.
        let stale_heartbeat = now - Duration::from_secs(120);
        assert_eq!(inner(now, Some(stale_heartbeat), 3).idle_deadline(session_idle), now + session_idle);
    }

    fn body_of(r: Response<Body>) -> Vec<u8> {
        use http_body_util::BodyExt;
        let rt = tokio::runtime::Builder::new_current_thread().build().unwrap();
        rt.block_on(r.into_body().collect()).unwrap().to_bytes().to_vec()
    }

    #[test]
    fn media_is_kept_until_the_session_expires() {
        let path = std::env::temp_dir().join(format!("den-remux-media-{}", std::process::id()));
        std::fs::write(&path, b"12345").unwrap();
        let exp = unix_now() + 3600;
        let r =
            media_response(open_parts(std::slice::from_ref(&path)).unwrap(), false, exp, &Default::default());
        let _ = std::fs::remove_file(&path);
        assert_eq!(r.status(), StatusCode::OK);
        assert_eq!(r.headers()["content-type"], "video/mp4");
        assert_eq!(r.headers()["content-length"], "5");
        assert_eq!(r.headers()["accept-ranges"], "bytes");
        assert!(r.headers().contains_key("etag"));
        let cc = r.headers()["cache-control"].to_str().unwrap();
        let age: u64 = cc
            .strip_prefix("private, max-age=")
            .and_then(|v| v.strip_suffix(", immutable"))
            .and_then(|v| v.parse().ok())
            .unwrap_or_else(|| panic!("{cc}"));
        assert!((3598..=3600).contains(&age), "the session's remaining life: {cc}");
        assert_eq!(busy().headers()["cache-control"], "no-store", "not ready is never kept");
        assert_eq!(gone().headers()["cache-control"], "no-store");
    }

    /// A finished segment, joined from GOP files with the later ones' `styp` skipped, answers a range of the joined
    /// bytes: open-ended, bounded, suffix, across a file boundary, and past its end.
    #[test]
    fn a_finished_segment_answers_a_range_of_its_joined_bytes() {
        let dir = std::env::temp_dir().join(format!("den-remux-range-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let (a, b) = (dir.join("g0.m4s"), dir.join("g1.m4s"));
        std::fs::write(&a, b"\0\0\0\x08stypAAAAAAAA").unwrap();
        std::fs::write(&b, b"\0\0\0\x08stypBBBBBBBB").unwrap();
        let paths = [a, b];
        let whole: &[u8] = b"\0\0\0\x08stypAAAAAAAABBBBBBBB";
        let etag = open_parts(&paths).unwrap().etag;
        let ask = |pairs: &[(&str, &str)]| {
            let mut h = hyper::HeaderMap::new();
            for (k, v) in pairs {
                h.insert(hyper::header::HeaderName::from_bytes(k.as_bytes()).unwrap(), v.parse().unwrap());
            }
            media_response(open_parts(&paths).unwrap(), false, unix_now() + 60, &h)
        };
        let full = ask(&[]);
        assert_eq!(full.headers()["etag"], etag.as_str(), "the same files, the same validator");
        assert_eq!(body_of(full), whole);

        let r = ask(&[("range", "bytes=10-"), ("if-range", &etag)]);
        assert_eq!(r.status(), StatusCode::PARTIAL_CONTENT);
        assert_eq!(r.headers()["content-range"], "bytes 10-23/24");
        assert_eq!(r.headers()["content-length"], "14");
        assert_eq!(body_of(r), &whole[10..], "open-ended, across the join");

        let r = ask(&[("range", "bytes=14-17")]);
        assert_eq!(r.headers()["content-range"], "bytes 14-17/24");
        assert_eq!(body_of(r), b"AABB");
        let r = ask(&[("range", "bytes=-3")]);
        assert_eq!(body_of(r), b"BBB", "the last bytes");
        let r = ask(&[("range", "bytes=20-99")]);
        assert_eq!(r.headers()["content-range"], "bytes 20-23/24", "an end past the body is cut to it");

        let r = ask(&[("range", "bytes=24-")]);
        assert_eq!(r.status(), StatusCode::RANGE_NOT_SATISFIABLE);
        assert_eq!(r.headers()["content-range"], "bytes */24");

        for whole_again in [
            vec![("range", "bytes=10-"), ("if-range", "\"another run\"")],
            vec![("range", "bytes=0-1,4-5")],
            vec![("range", "items=0-1")],
            vec![("range", "bytes=5-2")],
        ] {
            let r = ask(&whole_again);
            assert_eq!(r.status(), StatusCode::OK, "{whole_again:?}");
            assert_eq!(body_of(r), whole, "{whole_again:?}");
        }

        // A later run's files are another body, whatever their bytes.
        std::fs::write(dir.join("g2.m4s"), b"\0\0\0\x08stypAAAAAAAA").unwrap();
        let other = open_parts(&[dir.join("g2.m4s"), paths[1].clone()]).unwrap().etag;
        assert_ne!(other, etag);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn session_file_names_are_a_closed_set() {
        assert_eq!(seg_index("seg0.m4s"), Some(0));
        assert_eq!(seg_index("seg12.m4s"), Some(12));
        assert_eq!(seg_index("seg+1.m4s"), None);
        assert_eq!(seg_index("seg.m4s"), None);
        assert_eq!(seg_index("seg99999999.m4s"), None);
        assert!(is_session_file("init.mp4") && is_session_file("media.m3u8"));
        assert!(!is_session_file("../gops.m3u8"));
        assert_eq!(sub_file("sub0.m3u8"), Some(SubFile::Playlist(0)));
        assert_eq!(sub_file("sub7.vtt"), Some(SubFile::Document(7)));
        assert_eq!(sub_file("sub3_12.vtt"), Some(SubFile::Window(3, 12)));
        for not in
            ["sub8.vtt", "sub.vtt", "sub0_.vtt", "sub_1.vtt", "sub0_+1.vtt", "sub0_12345678.vtt", "sub01.vtt"]
        {
            assert_eq!(sub_file(not), None, "{not}");
        }
        assert!(is_session_file("sub2_0.vtt") && !is_session_file("sub2_0.m3u8"));
    }

    #[test]
    fn a_resume_starts_its_job_a_little_before_the_point() {
        let segs =
            playlist::segments(&[0.0, 2.5, 5.0, 8.0, 10.5, 13.0, 16.0, 19.5, 22.0, 24.0, 27.5], 30.021, 6.0);
        assert_eq!(start_segment(&segs, 0.0), 0);
        assert_eq!(start_segment(&segs, 17.0), 2, "13 → 19.5 plays 17");
        assert_eq!(start_segment(&segs, 13.5), 1, "near 13 a player may ask for the segment before it");
        assert_eq!(start_segment(&segs, 0.5), 0);
    }

    /// A segment asked for again after the job has gone past it and its GOPs were pruned — a seek back, or a player
    /// with no cache of its own — gets a new job. It used to be left to the running one, which never came back, and
    /// the request answered 503 after its full wait.
    #[test]
    fn a_job_past_a_pruned_segment_is_not_waited_on() {
        let segs: Vec<Segment> =
            (0..6).map(|k| Segment { start: k as f64 * 6.0, end: (k + 1) as f64 * 6.0 }).collect();
        let heads = |n, start, next, gops: &[Gop]| Session::heads_for(&segs, n, 0.0, 1, start, next, gops);
        let later = [gop(1, 7, 18.0, 6.0), gop(1, 8, 24.0, 6.0)];
        assert!(!heads(1, 0.0, 30.0, &later), "past segment 1 with its GOPs pruned");
        assert!(
            heads(1, 0.0, 30.0, &[gop(1, 1, 6.0, 6.0), gop(1, 7, 18.0, 6.0)]),
            "past it, but it is still kept"
        );
        assert!(heads(1, 0.0, 8.0, &[]), "part way through it");
        assert!(heads(3, 0.0, 6.0, &[]), "a little short of it");
        assert!(!heads(5, 0.0, 6.0, &[]), "too far short of it");
        assert!(!heads(1, 12.0, 14.0, &[]), "started after it");
        // The last segment: its end is the job's, and its GOPs stay kept while the job winds up.
        assert!(heads(5, 0.0, 36.0, &[gop(1, 9, 30.0, 6.0)]), "the last segment waits for its job to finish");
        // A file whose first keyframe is late: segment 0 still starts at 0, and a job begun on that keyframe is
        // heading for it rather than restarted on every look.
        assert!(
            Session::heads_for(&segs, 0, 0.5, 1, 0.5, 0.5, &[]),
            "segment 0 behind a late first keyframe"
        );
        assert!(!Session::heads_for(&segs, 0, 0.5, 1, 6.0, 6.0, &[]), "but not a job begun past it");
        assert!(!heads(1, 0.0, 30.0, &[gop(2, 1, 6.0, 6.0)]), "a GOP of another job is not this job's");
    }

    /// Two sessions still actively watched (idle under the threshold) plus a third, newcomer session about to
    /// wait for a slot: neither watched job may be the victim, whatever their relative ages — only an idle one
    /// is ever a sound victim, and the newcomer gets nothing when there isn't one.
    #[test]
    fn a_newcomer_never_reaps_an_actively_watched_job() {
        let threshold = Duration::from_secs(30);
        // Both well under the threshold: one freshly paused, one a little longer, neither idle enough to take.
        let watched = [("alice", Duration::from_secs(1)), ("bob", Duration::from_secs(25))];
        assert_eq!(
            Session::pick_reap_victim(watched.into_iter(), threshold),
            None,
            "a newcomer waits rather than steal from either watched session"
        );
    }

    /// Among several candidates, once one has gone unwatched for the full threshold it is a sound victim even
    /// next to others also past it — and the one idle LONGEST is picked, not merely any that qualifies.
    #[test]
    fn the_longest_idle_candidate_is_reaped_first() {
        let threshold = Duration::from_secs(30);
        let candidates = [
            ("alice", Duration::from_secs(5)),  // still watched: never a candidate
            ("bob", Duration::from_secs(40)),   // idle longer than the threshold
            ("carol", Duration::from_secs(90)), // idle longest of all: the one to take
        ];
        assert_eq!(Session::pick_reap_victim(candidates.into_iter(), threshold), Some("carol"));
    }

    /// Exactly at the threshold counts as idle enough — the same boundary `supervise`'s own housekeeping reap
    /// uses, so a session is never protected by one path and not the other.
    #[test]
    fn exactly_the_threshold_is_a_sound_victim() {
        let threshold = Duration::from_secs(30);
        assert_eq!(Session::pick_reap_victim([("alice", threshold)].into_iter(), threshold), Some("alice"));
        assert_eq!(
            Session::pick_reap_victim(
                [("alice", threshold - Duration::from_millis(1))].into_iter(),
                threshold
            ),
            None,
            "a millisecond short of it is still watched"
        );
    }

    /// A copy fits a link only with headroom: the 12 GB, 3818 s episode (25.3 Mbit/s) does not fit 30 Mbit/s, the
    /// 1.5 GB one (3.1 Mbit/s) does, and a link nobody named or a need nobody knows demotes nothing.
    #[test]
    fn a_copy_fits_a_link_only_with_headroom() {
        let need =
            |bytes: u64| Some(Need { bitrate: average_bitrate(Some(bytes), 3818.0).unwrap(), indexed: true });
        assert!(over(Some(30_000_000), need(12_081_049_022)));
        assert!(!over(Some(30_000_000), need(1_499_990_620)));
        assert!(!over(Some(30_000_000), need(7_000_000_000)), "14.7 Mbit/s is under 70% of 30");
        assert!(!over(None, need(12_081_049_022)));
        assert!(!over(Some(30_000_000), None));
    }

    /// A season pack's size, listed as the episode's, is moved out of the label once the file is open; a size that
    /// agrees, or one already named a pack, is left alone.
    #[test]
    fn an_opened_files_size_replaces_a_packs_in_the_label() {
        let (gb105, gb12) = (105 * (1u64 << 30), 12_081_049_022);
        let label = "4K • WEB-DL • Dolby Vision • Atmos • 105 GB";
        assert_eq!(
            opened_label(label, Some(gb105), None, Some(gb12)),
            ("4K • WEB-DL • Dolby Vision • Atmos • 11 GB · from a 105 GB pack".to_string(), Some(gb105))
        );
        assert_eq!(opened_label(label, Some(gb105), None, Some(gb105 + 1000)), (label.to_string(), None));
        assert_eq!(
            opened_label(label, Some(gb105), Some(gb105), Some(gb12)),
            (label.to_string(), Some(gb105))
        );
        assert_eq!(opened_label(label, None, None, Some(gb12)), (label.to_string(), None));
        assert_eq!(
            opened_label("4K • WEB-DL", Some(gb105), None, Some(gb12)),
            ("4K • WEB-DL • 11 GB · from a 105 GB pack".to_string(), Some(gb105)),
            "a label without the listed size gets the file's"
        );
    }

    /// Without a configured uplink, a session's own measured link is the whole budget — today's behaviour,
    /// unchanged. With one, the budget is the smaller of that and what the uplink has left once every other
    /// remote session's own need is taken out; a fully spent uplink floors at 0 rather than going negative.
    #[test]
    fn the_uplink_budget_narrows_a_sessions_own_measurement() {
        assert_eq!(budgeted_max_bitrate(Some(5_000_000), None, 3_000_000), Some(5_000_000));
        assert_eq!(
            budgeted_max_bitrate(None, Some(30_000_000), 3_000_000),
            None,
            "a player at home has no link to narrow"
        );
        assert_eq!(
            budgeted_max_bitrate(Some(10_000_000), Some(30_000_000), 25_000_000),
            Some(5_000_000),
            "the uplink has 5 Mbit/s left, less than this session's own 10"
        );
        assert_eq!(
            budgeted_max_bitrate(Some(10_000_000), Some(30_000_000), 1_000_000),
            Some(10_000_000),
            "the uplink has plenty left; this session's own measurement still governs"
        );
        assert_eq!(
            budgeted_max_bitrate(Some(10_000_000), Some(30_000_000), 40_000_000),
            Some(0),
            "other sessions already claim more than the whole uplink: nothing is left, not a negative budget"
        );
    }

    #[test]
    fn seg_at_finds_the_segment_playing() {
        let segs =
            playlist::segments(&[0.0, 2.5, 5.0, 8.0, 10.5, 13.0, 16.0, 19.5, 22.0, 24.0, 27.5], 30.021, 6.0);
        assert_eq!(seg_at(&segs, 0.0), 0);
        assert_eq!(seg_at(&segs, 7.9), 0);
        assert_eq!(seg_at(&segs, 7.99), 1, "within SNAP of a boundary is at the boundary");
        assert_eq!(seg_at(&segs, 8.0), 1);
        assert_eq!(seg_at(&segs, 29.0), 4);
    }
}
