//! A resident yt-dlp, so a resolve costs the extraction and not the interpreter.
//!
//! Spawning the yt-dlp CLI spends time before it has looked at anything — Python starting and yt-dlp
//! importing itself — and pays that again for every trailer. This keeps one process alive and talks
//! to it over a pipe: one JSON object per line each way (`worker/resolve.py`).
//!
//! **Everything here is optional.** No worker configured, one that will not start, one that dies
//! mid-answer, one that takes too long — every path returns `None`, and the caller spawns the binary
//! exactly as it always did. That is the point: the fast path may fail in any way it likes and the
//! service only gets slower, never broken.
//!
//! **One at a time, deliberately.** A resolve is about a second and a half of waiting on YouTube, and
//! the requests that arrive together are usually for the SAME video — which `direct::answer` already
//! collapses into one. What is left is a handful of distinct ids a browse kicks off, and serialising
//! those behind one interpreter is a better trade than supervising a pool. If that ever stops being
//! true, this is where a pool would go.
//!
//! **It does not stay forever.** The box has a 1 GiB cap for this container and a resident Python is
//! ~40 MB of it, so a worker that has not been asked anything in `IDLE_TIMEOUT` is shut down and the
//! next resolve starts a new one, paying the 840ms once more. Trailers are bursty — a browse, then
//! nothing for hours — and that is exactly the shape this suits.

use std::process::Stdio;
use std::time::{Duration, Instant};

use serde::Deserialize;
use tokio::io::{AsyncBufRead, AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::process::{Child, ChildStdin, ChildStdout, Command};
use tokio::sync::Mutex;

use crate::config::Config;

/// How long one answer may take before the worker is presumed wedged. Generously past a real resolve
/// (~1.3s once warm) and inside `direct`'s own patience, so a stuck worker costs one slow request and
/// then goes away rather than holding the lock for everyone behind it.
const ANSWER_TIMEOUT: Duration = Duration::from_secs(30);

/// A normal flattened resolver answer is a few KiB. This leaves ample room for a large format
/// ladder without allowing a worker that forgets its newline to grow one request for 30 seconds.
pub(crate) const REPLY_MAX_BYTES: usize = 256 * 1024;

/// How long a worker may sit unasked before it is shut down.
pub const IDLE_TIMEOUT: Duration = Duration::from_secs(10 * 60);

/// What the worker answers with; the caller turns it into a `Direct`.
#[derive(Debug, Deserialize)]
pub struct Answer {
    pub width: Option<u32>,
    pub height: Option<u32>,
    #[serde(default)]
    pub urls: Vec<String>,
    pub hls: Option<String>,
}
/// The wire reply: an answer, or yt-dlp's own words about why there isn't one.
#[derive(Deserialize)]
struct Reply {
    ok: bool,
    error: Option<String>,
    #[serde(flatten)]
    answer: Option<Answer>,
}

struct Resident {
    child: Child,
    pgid: Option<u32>,
    stdin: ChildStdin,
    stdout: BufReader<ChildStdout>,
    /// When it last answered, so `reap_idle` can tell whether anyone still wants it.
    used: Instant,
}

/// Which resident process a [`Worker`] is. Both speak the one line protocol; they differ in what
/// starts them and how long they are given.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Kind {
    YtDlp,
    Youtubei,
}

/// What starts a worker: the program, its arguments, and the environment it is given.
type Launch = (String, Vec<String>, Vec<(&'static str, String)>);

impl Kind {
    fn label(self) -> &'static str {
        match self {
            Kind::YtDlp => "resident yt-dlp",
            Kind::Youtubei => "resident youtubei.js",
        }
    }

    /// A youtubei.js answer is ~0.12 s, so one that has not come in 10 s is wedged, and the request
    /// it holds is better spent on yt-dlp than waited on.
    fn timeout(self) -> Duration {
        match self {
            Kind::YtDlp => ANSWER_TIMEOUT,
            Kind::Youtubei => Duration::from_secs(10),
        }
    }

    /// The program, its arguments (script last) and its environment, or `None` when this one is not
    /// configured.
    fn command(self, cfg: &Config) -> Option<Launch> {
        match self {
            Kind::YtDlp => Some((cfg.python.clone(), vec![cfg.ytdlp_worker.clone()?], Vec::new())),
            Kind::Youtubei => {
                let script = cfg.youtubei_worker.clone()?;
                // The packages deno was locked to sit in `deno/` beside the script. Pointed at here
                // and not image-wide, because yt-dlp's own deno keeps a cache of its own.
                let cache = std::path::Path::new(&script).parent().map(|d| d.join("deno"));
                let env =
                    cache.map(|c| vec![("DENO_DIR", c.to_string_lossy().into_owned())]).unwrap_or_default();
                let mut args = cfg.deno_args.clone();
                args.push(script);
                Some((cfg.deno.clone(), args, env))
            }
        }
    }
}

/// The resident process, when there is one.
pub struct Worker {
    kind: Kind,
    held: Mutex<Option<Resident>>,
}

impl Default for Worker {
    fn default() -> Self {
        Worker { kind: Kind::YtDlp, held: Mutex::new(None) }
    }
}

impl Worker {
    pub fn youtubei() -> Worker {
        Worker { kind: Kind::Youtubei, held: Mutex::new(None) }
    }

    /// Is this one configured at all? One that is not is skipped silently; one that is and does not
    /// answer is a fallback worth counting.
    pub fn configured(&self, cfg: &Config) -> bool {
        self.kind.command(cfg).is_some()
    }

    /// Resolve `vid` through the resident process. `rungs` is the ladder's heights, tallest first,
    /// for the resolver that does not read yt-dlp's selector string.
    ///
    /// `None` means "this path is not available" — not configured, could not start, died, or took
    /// too long — and the caller should use the next resolver. `Some(Err)` is the resolver's own
    /// failure for this video, which the caller classifies exactly as it classifies stderr from the
    /// binary.
    pub async fn resolve(
        &self,
        cfg: &Config,
        vid: &str,
        format: &str,
        rungs: &[u32],
    ) -> Option<Result<Answer, String>> {
        let label = self.kind.label();
        let (program, args, env) = self.kind.command(cfg)?;
        let mut held = self.held.lock().await;
        if held.is_none() {
            *held = start(label, &program, &args, &env);
        }
        let resident = held.as_mut()?;

        let request = serde_json::json!({
            "id": vid,
            "format": format,
            "rungs": rungs,
            "cache": cfg.ytdlp_cache.to_string_lossy(),
            "extractor_args": cfg.ytdlp_extractor_args,
        });
        let patience = self.kind.timeout();
        match tokio::time::timeout(patience, exchange(resident, &request.to_string())).await {
            Ok(Ok(outcome)) => {
                resident.used = Instant::now();
                Some(outcome)
            }
            // The pipe broke, or it answered with something unreadable: it is not a worker any more.
            Ok(Err(why)) => {
                crate::log_limited("worker lost", || format!("{label}: {why}"));
                stop(held.take()).await;
                None
            }
            Err(_) => {
                eprintln!("{label}: no answer for {vid} in {patience:?}; restarting it");
                stop(held.take()).await;
                None
            }
        }
    }

    /// Shut the worker down if nothing has asked it anything for `IDLE_TIMEOUT`.
    pub async fn reap_idle(&self) {
        let mut held = self.held.lock().await;
        if held.as_ref().is_some_and(|r| r.used.elapsed() >= IDLE_TIMEOUT) {
            eprintln!("{}: idle for {IDLE_TIMEOUT:?}; shutting it down", self.kind.label());
            stop(held.take()).await;
        }
    }

    /// Shut it down on the way out, so a redeploy does not leave an interpreter behind.
    pub async fn shutdown(&self) {
        stop(self.held.lock().await.take()).await;
    }
}

/// Start one, or `None` (said once) if it cannot be started — the caller then uses the next resolver.
fn start(label: &'static str, program: &str, args: &[String], env: &[(&str, String)]) -> Option<Resident> {
    let mut command = Command::new(program);
    command
        .args(args)
        .envs(env.iter().map(|(k, v)| (*k, v)))
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        // Inherited, not piped: a traceback belongs in the log, and nothing here drains a third pipe.
        .stderr(Stdio::inherit())
        .kill_on_drop(true);
    #[cfg(unix)]
    command.process_group(0);
    let mut child = command
        .spawn()
        .map_err(|e| {
            crate::log_limited("worker spawn", || format!("{label}: cannot start {program} {args:?}: {e}"))
        })
        .ok()?;
    let pgid = child.id();
    crate::ytdlp::register_group(pgid);
    let stdin = child.stdin.take()?;
    let stdout = BufReader::new(child.stdout.take()?);
    Some(Resident { child, pgid, stdin, stdout, used: Instant::now() })
}

/// One reply line as the caller wants it: an answer, or what yt-dlp said about this video. The OUTER
/// error is different in kind — the line itself was unreadable, which means whatever is on the far
/// end has stopped being a worker and should be replaced rather than believed.
pub(crate) fn read_reply(line: &str) -> Result<Result<Answer, String>, String> {
    let reply: Reply = serde_json::from_str(line).map_err(|e| format!("unreadable reply: {e}"))?;
    Ok(match (reply.ok, reply.answer, reply.error) {
        (true, Some(answer), _) => Ok(answer),
        (_, _, said) => Err(said.unwrap_or_else(|| "no answer".into())),
    })
}

/// One request in, one reply out. Any I/O failure means the process is gone as far as we care.
async fn exchange(resident: &mut Resident, request: &str) -> Result<Result<Answer, String>, String> {
    resident.stdin.write_all(request.as_bytes()).await.map_err(|e| e.to_string())?;
    resident.stdin.write_all(b"\n").await.map_err(|e| e.to_string())?;
    resident.stdin.flush().await.map_err(|e| e.to_string())?;
    let line = read_bounded_line(&mut resident.stdout).await?;
    read_reply(&line)
}

/// Read one newline-delimited reply without ever retaining more than the protocol ceiling. At EOF,
/// match `read_line` and accept a final unterminated line; an overlong unterminated stream returns as
/// soon as byte `REPLY_MAX_BYTES + 1` is visible, causing the caller to kill and replace the worker.
async fn read_bounded_line<R: AsyncBufRead + Unpin>(reader: &mut R) -> Result<String, String> {
    let mut line = Vec::with_capacity(4096);
    loop {
        let available = reader.fill_buf().await.map_err(|e| e.to_string())?;
        if available.is_empty() {
            if line.is_empty() {
                return Err("it closed its output".into());
            }
            return String::from_utf8(line).map_err(|e| e.to_string());
        }
        if let Some(newline) = available.iter().position(|&byte| byte == b'\n') {
            if line.len().saturating_add(newline) > REPLY_MAX_BYTES {
                return Err(format!("reply exceeded {REPLY_MAX_BYTES} bytes"));
            }
            line.extend_from_slice(&available[..newline]);
            reader.consume(newline + 1);
            return String::from_utf8(line).map_err(|e| e.to_string());
        }
        if line.len().saturating_add(available.len()) > REPLY_MAX_BYTES {
            return Err(format!("reply exceeded {REPLY_MAX_BYTES} bytes without a newline"));
        }
        let consumed = available.len();
        line.extend_from_slice(available);
        reader.consume(consumed);
    }
}

/// Kill the worker's complete process group and reap its leader before a replacement is started.
/// Resolver workers may fork runtimes of their own; killing only the direct interpreter left those
/// descendants holding resources after timeout, malformed output, idle reap, and shutdown alike.
async fn stop(resident: Option<Resident>) {
    if let Some(mut resident) = resident {
        crate::ytdlp::kill_group(resident.pgid);
        let _ = tokio::time::timeout(Duration::from_secs(2), resident.child.wait()).await;
        crate::ytdlp::unregister_group(resident.pgid);
    }
}
