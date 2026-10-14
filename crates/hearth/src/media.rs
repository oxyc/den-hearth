//! Admission for streamed media bodies (Reel's trailers and segments, Remux's segments).
//!
//! A media body outlives the handler that built it, and it is what actually costs memory: the upstream
//! socket, the buffers behind it, an ffmpeg child. So its permit belongs to the BODY, not to the handler
//! future, and comes back on every way a body can end: it finished, the client went away (the response
//! was dropped), the upstream failed, nothing arrived for `idle`, or `lifetime` ran out.
//!
//! The pump is a task that owns both the upstream body and the permit, and hands frames to Hyper through
//! a ONE-frame channel. That is the backpressure: the upstream is not read again until Hyper has taken
//! the previous frame, so a slow client holds back the upstream socket instead of growing a buffer.
//! Because the task (not Hyper) owns the clocks, a response Hyper never polls still times out.
//!
//! After den-reel's `MediaGate`, generalised from one class to a total plus a limit per class.

use std::future::Future;
use std::io;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering::Relaxed};
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use bytes::Bytes;
use http_body_util::BodyExt;
use hyper::body::Frame;
use hyper::{Response, StatusCode};
use tokio::sync::{mpsc, oneshot, OwnedSemaphorePermit, Semaphore};
use tokio::time::Instant;

use crate::route::MediaClass;
use crate::Body;

/// Streams in flight at once, across both classes. The classes sum to more than this so that it, not the
/// per-class limits, is what binds when both are busy.
pub const TOTAL: usize = 24;
/// A player fetches a few segments in parallel and a household has a handful of players.
pub const REMUX: usize = 16;
/// Trailers: short, and a browse screen starts several at once.
pub const REEL: usize = 12;
/// A Reel stream that moves no byte for this long is cut. A paused `<video>` or AVPlayer stops reading a
/// progressive file once its buffer is full, mid-body, and asks again (with a Range) when it resumes, so this
/// stays long enough that an ordinary pause does not cost a re-request.
pub const IDLE: Duration = Duration::from_secs(30);
/// The same for a Remux body. A segment or init section is a few MB that a player fetches whole, in seconds, and
/// it stops asking BETWEEN segments when its buffer is full, not in the middle of one; a reader that moves no
/// byte for this long is gone, and with 16 permits a stalled one should not hold a position for half a minute.
/// This clock only runs while the client is not reading: a slow reader that keeps moving bytes never trips it
/// (it is held by `LIFETIME` alone).
pub const REMUX_IDLE: Duration = Duration::from_secs(10);
/// No single body outlives this, however steadily it moves.
pub const LIFETIME: Duration = Duration::from_secs(15 * 60);

#[derive(Clone, Copy, Debug)]
pub struct Limits {
    pub total: usize,
    pub reel: usize,
    pub remux: usize,
    /// Idle deadline for Reel bodies.
    pub idle: Duration,
    /// Idle deadline for Remux bodies.
    pub remux_idle: Duration,
    pub lifetime: Duration,
}

impl Default for Limits {
    fn default() -> Self {
        Limits {
            total: TOTAL,
            reel: REEL,
            remux: REMUX,
            idle: IDLE,
            remux_idle: REMUX_IDLE,
            lifetime: LIFETIME,
        }
    }
}

#[derive(Default)]
struct ClassStats {
    active: AtomicU64,
    high_water: AtomicU64,
    refused: AtomicU64,
}

pub struct MediaGate {
    total: Arc<Semaphore>,
    classes: [Arc<Semaphore>; 2],
    /// Idle deadline by class, indexed like `classes`.
    idle: [Duration; 2],
    lifetime: Duration,
    stats: [ClassStats; 2],
    idle_timeouts: AtomicU64,
    lifetime_timeouts: AtomicU64,
    /// Bodies dropped before they finished: a client that went away mid-stream.
    cancellations: Arc<AtomicU64>,
}

/// One stream's right to be in flight.
pub struct Lease {
    gate: Arc<MediaGate>,
    class: MediaClass,
    _total: OwnedSemaphorePermit,
    _class: OwnedSemaphorePermit,
}

impl Drop for Lease {
    fn drop(&mut self) {
        self.gate.stats[self.class as usize].active.fetch_sub(1, Relaxed);
    }
}

/// What `guard` made of a response.
pub enum Guarded {
    /// A media body now owned by the lease.
    Streaming(Response<Body>),
    /// Not a media body (an error, a redirect, an empty answer): untouched, and the lease is already back.
    Plain(Response<Body>),
}

impl MediaGate {
    pub fn new(limits: Limits) -> Arc<Self> {
        Arc::new(MediaGate {
            total: Arc::new(Semaphore::new(limits.total)),
            classes: [Arc::new(Semaphore::new(limits.reel)), Arc::new(Semaphore::new(limits.remux))],
            idle: [limits.idle, limits.remux_idle],
            lifetime: limits.lifetime,
            stats: Default::default(),
            idle_timeouts: AtomicU64::new(0),
            lifetime_timeouts: AtomicU64::new(0),
            cancellations: Arc::new(AtomicU64::new(0)),
        })
    }

    /// A lease if the class and the total both have room. Never waits: a media refusal is a clean
    /// "try again" the player already handles, and a queue of waiting streams is memory too.
    pub fn try_enter(self: &Arc<Self>, class: MediaClass) -> Option<Lease> {
        let stats = &self.stats[class as usize];
        let held = self.classes[class as usize]
            .clone()
            .try_acquire_owned()
            .ok()
            .and_then(|per_class| Some((per_class, self.total.clone().try_acquire_owned().ok()?)));
        let Some((per_class, total)) = held else {
            stats.refused.fetch_add(1, Relaxed);
            return None;
        };
        let active = stats.active.fetch_add(1, Relaxed) + 1;
        stats.high_water.fetch_max(active, Relaxed);
        Some(Lease { gate: self.clone(), class, _total: total, _class: per_class })
    }

    /// Hand `lease` to a successful media body. Anything else comes back untouched with the lease
    /// released: it holds no stream.
    pub fn guard(&self, response: Response<Body>, lease: Lease) -> Guarded {
        let streams = matches!(response.status(), StatusCode::OK | StatusCode::PARTIAL_CONTENT)
            && !hyper::body::Body::is_end_stream(response.body());
        if !streams {
            return Guarded::Plain(response);
        }
        let (parts, inner) = response.into_parts();
        let hint = hyper::body::Body::size_hint(&inner);
        let (tx, rx) = mpsc::channel(1);
        let (terminal_tx, terminal) = oneshot::channel();
        let (start, started_body) = oneshot::channel();
        let completed = Arc::new(AtomicBool::new(false));
        let started = Instant::now();
        let idle = self.idle[lease.class as usize];
        let control = PumpControl {
            terminal_tx,
            lease,
            start: started_body,
            idle_at: started + idle,
            lifetime_at: started + self.lifetime,
            idle,
            completed: completed.clone(),
        };
        let task = tokio::spawn(pump(inner, tx, control));
        let body = PumpBody {
            rx,
            terminal,
            task,
            hint,
            start: Some(start),
            finished: false,
            completed,
            cancellations: self.cancellations.clone(),
        };
        Guarded::Streaming(Response::from_parts(parts, body.boxed()))
    }

    pub fn render(&self, out: &mut String) {
        use std::fmt::Write;
        let series = |out: &mut String, name: &str, kind: &str, help: &str| {
            let _ = writeln!(out, "# HELP den_hearth_{name} {help}\n# TYPE den_hearth_{name} {kind}");
        };
        series(out, "media_active", "gauge", "Media bodies currently streaming, by class.");
        series(out, "media_high_water", "gauge", "Highest simultaneous media bodies, by class.");
        series(
            out,
            "media_refused_total",
            "counter",
            "Media requests refused for lack of a permit, by class.",
        );
        for (class, label) in [(MediaClass::Reel, "reel"), (MediaClass::Remux, "remux")] {
            let s = &self.stats[class as usize];
            let _ = writeln!(out, "den_hearth_media_active{{class=\"{label}\"}} {}", s.active.load(Relaxed));
            let _ = writeln!(
                out,
                "den_hearth_media_high_water{{class=\"{label}\"}} {}",
                s.high_water.load(Relaxed)
            );
            let _ = writeln!(
                out,
                "den_hearth_media_refused_total{{class=\"{label}\"}} {}",
                s.refused.load(Relaxed)
            );
        }
        series(out, "media_timeouts_total", "counter", "Media bodies cut by a deadline, by kind.");
        let _ = writeln!(
            out,
            "den_hearth_media_timeouts_total{{kind=\"idle\"}} {}",
            self.idle_timeouts.load(Relaxed)
        );
        let _ = writeln!(
            out,
            "den_hearth_media_timeouts_total{{kind=\"lifetime\"}} {}",
            self.lifetime_timeouts.load(Relaxed)
        );
        series(
            out,
            "media_cancelled_total",
            "counter",
            "Media bodies dropped by the client before they finished.",
        );
        let _ = writeln!(out, "den_hearth_media_cancelled_total {}", self.cancellations.load(Relaxed));
    }
}

struct PumpControl {
    terminal_tx: oneshot::Sender<Option<io::Error>>,
    lease: Lease,
    start: oneshot::Receiver<()>,
    idle_at: Instant,
    lifetime_at: Instant,
    idle: Duration,
    completed: Arc<AtomicBool>,
}

enum End {
    Clean,
    Error(io::Error),
    ConsumerGone,
    Idle,
    Lifetime,
}

async fn pump(mut inner: Body, tx: mpsc::Sender<Frame<Bytes>>, control: PumpControl) {
    let PumpControl { terminal_tx, lease, mut start, idle_at, lifetime_at, idle, completed } = control;
    // Nothing is read from upstream until Hyper asks for the first frame, so a HEAD, a 304 or a response
    // dropped before it was written opens no upstream range. The task still times the lease meanwhile.
    let end = tokio::select! {
        () = tokio::time::sleep_until(lifetime_at) => End::Lifetime,
        () = tokio::time::sleep_until(idle_at) => End::Idle,
        began = &mut start => {
            if began.is_ok() {
                pump_frames(&mut inner, &tx, idle_at, lifetime_at, idle).await
            } else {
                End::Clean
            }
        }
    };
    match &end {
        End::Idle => {
            lease.gate.idle_timeouts.fetch_add(1, Relaxed);
        }
        End::Lifetime => {
            lease.gate.lifetime_timeouts.fetch_add(1, Relaxed);
        }
        End::Clean | End::Error(_) | End::ConsumerGone => {}
    }
    // Upstream first, then the permit, then the channel: a reader that sees EOF has found the upstream
    // already closed and the stream already given back, rather than racing this task's destructors.
    drop(inner);
    drop(lease);
    drop(tx);
    completed.store(true, std::sync::atomic::Ordering::Release);
    let error = match end {
        End::Idle => Some(io::Error::new(io::ErrorKind::TimedOut, "media response idle timeout")),
        End::Lifetime => Some(io::Error::new(io::ErrorKind::TimedOut, "media response lifetime exceeded")),
        End::Error(error) => Some(error),
        End::Clean | End::ConsumerGone => None,
    };
    let _ = terminal_tx.send(error);
}

async fn pump_frames(
    inner: &mut Body,
    tx: &mpsc::Sender<Frame<Bytes>>,
    mut idle_at: Instant,
    lifetime_at: Instant,
    idle: Duration,
) -> End {
    loop {
        let frame = tokio::select! {
            () = tokio::time::sleep_until(lifetime_at) => return End::Lifetime,
            () = tokio::time::sleep_until(idle_at) => return End::Idle,
            frame = inner.frame() => frame,
        };
        let Some(frame) = frame else { return End::Clean };
        let frame = match frame {
            Ok(frame) => frame,
            Err(error) => return End::Error(error),
        };
        // Waiting for Hyper to take the previous frame is the backpressure, and it is under the same
        // clocks: a client that stops reading is an idle stream.
        let sent = tokio::select! {
            () = tokio::time::sleep_until(lifetime_at) => return End::Lifetime,
            () = tokio::time::sleep_until(idle_at) => return End::Idle,
            sent = tx.send(frame) => sent,
        };
        if sent.is_err() {
            return End::ConsumerGone;
        }
        idle_at = Instant::now() + idle;
    }
}

struct PumpBody {
    rx: mpsc::Receiver<Frame<Bytes>>,
    terminal: oneshot::Receiver<Option<io::Error>>,
    task: tokio::task::JoinHandle<()>,
    hint: hyper::body::SizeHint,
    start: Option<oneshot::Sender<()>>,
    finished: bool,
    completed: Arc<AtomicBool>,
    cancellations: Arc<AtomicU64>,
}

impl hyper::body::Body for PumpBody {
    type Data = Bytes;
    type Error = io::Error;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, io::Error>>> {
        if let Some(start) = self.start.take() {
            let _ = start.send(());
        }
        if self.finished {
            return Poll::Ready(None);
        }
        match self.rx.poll_recv(cx) {
            Poll::Ready(Some(frame)) => Poll::Ready(Some(Ok(frame))),
            Poll::Pending => Poll::Pending,
            // The task has dropped its sender: its verdict (a clean end or one error) follows.
            Poll::Ready(None) => match Pin::new(&mut self.terminal).poll(cx) {
                Poll::Pending => Poll::Pending,
                Poll::Ready(Ok(Some(error))) => {
                    self.finished = true;
                    Poll::Ready(Some(Err(error)))
                }
                Poll::Ready(Ok(None)) => {
                    self.finished = true;
                    Poll::Ready(None)
                }
                // The task ended without a verdict (it panicked): the body is truncated, and a chunked
                // response must not end as if it were complete.
                Poll::Ready(Err(_)) => {
                    self.finished = true;
                    Poll::Ready(Some(Err(io::Error::other("media pump ended without finishing the body"))))
                }
            },
        }
    }

    fn is_end_stream(&self) -> bool {
        self.finished && self.rx.is_empty()
    }

    fn size_hint(&self) -> hyper::body::SizeHint {
        self.hint
    }
}

impl Drop for PumpBody {
    fn drop(&mut self) {
        // A dropped JoinHandle detaches its task. Abort it, so a client that went away releases the
        // upstream body and the lease now rather than at the next deadline.
        if !self.finished && !self.completed.load(std::sync::atomic::Ordering::Acquire) {
            self.cancellations.fetch_add(1, Relaxed);
        }
        self.task.abort();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use http_body_util::StreamBody;

    fn gate(total: usize, idle: u64, lifetime: u64) -> Arc<MediaGate> {
        MediaGate::new(Limits {
            total,
            reel: total,
            remux: total,
            idle: Duration::from_secs(idle),
            remux_idle: Duration::from_secs(idle),
            lifetime: Duration::from_secs(lifetime),
        })
    }

    fn full(body: &'static str) -> Response<Body> {
        Response::new(
            http_body_util::Full::new(Bytes::from_static(body.as_bytes())).map_err(|n| match n {}).boxed(),
        )
    }

    fn pending() -> Response<Body> {
        let frames = futures_util::stream::pending::<Result<Frame<Bytes>, io::Error>>();
        Response::new(StreamBody::new(frames).boxed())
    }

    fn streaming(g: &Guarded) -> bool {
        matches!(g, Guarded::Streaming(_))
    }

    fn body(g: Guarded) -> Body {
        match g {
            Guarded::Streaming(r) | Guarded::Plain(r) => r.into_body(),
        }
    }

    fn active(g: &MediaGate, class: MediaClass) -> u64 {
        g.stats[class as usize].active.load(Relaxed)
    }

    #[tokio::test]
    async fn the_permit_lives_until_the_body_finishes_or_is_dropped() {
        let g = gate(1, 30, 60);
        let guarded = g.guard(full("one"), g.try_enter(MediaClass::Remux).unwrap());
        assert!(streaming(&guarded));
        assert_eq!(active(&g, MediaClass::Remux), 1);
        assert!(g.try_enter(MediaClass::Remux).is_none());
        assert_eq!(g.stats[MediaClass::Remux as usize].refused.load(Relaxed), 1);

        // Cancellation: the client drops the response before reading a byte.
        drop(guarded);
        tokio::task::yield_now().await;
        assert_eq!(active(&g, MediaClass::Remux), 0);
        assert_eq!(g.cancellations.load(Relaxed), 1);

        let guarded = g.guard(full("two"), g.try_enter(MediaClass::Remux).unwrap());
        assert_eq!(body(guarded).collect().await.unwrap().to_bytes(), "two");
        tokio::task::yield_now().await;
        assert_eq!(active(&g, MediaClass::Remux), 0);
        assert_eq!(g.cancellations.load(Relaxed), 1, "a drained response was not a cancellation");
    }

    #[tokio::test]
    async fn cancelling_mid_stream_drops_the_upstream_body() {
        // The upstream reports when it is dropped; a leaked pump would keep it alive.
        struct Tracked(Arc<AtomicBool>);
        impl Drop for Tracked {
            fn drop(&mut self) {
                self.0.store(true, Relaxed);
            }
        }
        let dropped = Arc::new(AtomicBool::new(false));
        let tracked = Tracked(dropped.clone());
        let frames = futures_util::stream::unfold(tracked, |t| async move {
            Some((Ok::<_, io::Error>(Frame::data(Bytes::from_static(&[7; 1024]))), t))
        });
        let g = gate(1, 30, 60);
        let response = Response::new(StreamBody::new(frames).boxed());
        let mut body = body(g.guard(response, g.try_enter(MediaClass::Reel).unwrap()));
        assert_eq!(body.frame().await.unwrap().unwrap().into_data().unwrap().len(), 1024);
        drop(body);
        for _ in 0..10 {
            tokio::task::yield_now().await;
        }
        assert!(dropped.load(Relaxed), "the upstream body outlived its cancelled client");
        assert_eq!(active(&g, MediaClass::Reel), 0);
        assert!(g.try_enter(MediaClass::Reel).is_some());
    }

    #[tokio::test]
    async fn only_a_body_that_streams_keeps_the_permit() {
        let g = gate(1, 30, 60);
        for status in [StatusCode::NOT_FOUND, StatusCode::NOT_MODIFIED, StatusCode::BAD_GATEWAY] {
            let mut response = full("short");
            *response.status_mut() = status;
            let guarded = g.guard(response, g.try_enter(MediaClass::Remux).unwrap());
            assert!(!streaming(&guarded), "{status}");
            assert_eq!(active(&g, MediaClass::Remux), 0, "{status} kept a stream permit");
        }
        let empty = Response::new(http_body_util::Empty::<Bytes>::new().map_err(|n| match n {}).boxed());
        assert!(!streaming(&g.guard(empty, g.try_enter(MediaClass::Remux).unwrap())));
    }

    #[tokio::test]
    async fn a_partial_range_keeps_its_contract_while_guarded() {
        let g = gate(1, 30, 60);
        let mut response = full("abc");
        *response.status_mut() = StatusCode::PARTIAL_CONTENT;
        response.headers_mut().insert("content-range", "bytes 10-12/100".parse().unwrap());
        let Guarded::Streaming(response) = g.guard(response, g.try_enter(MediaClass::Remux).unwrap()) else {
            panic!("a 206 body streams");
        };
        assert_eq!(response.status(), StatusCode::PARTIAL_CONTENT);
        assert_eq!(response.headers()["content-range"], "bytes 10-12/100");
        assert_eq!(hyper::body::Body::size_hint(response.body()).exact(), Some(3));
        assert_eq!(response.into_body().collect().await.unwrap().to_bytes(), "abc");
        assert_eq!(active(&g, MediaClass::Remux), 0);
    }

    #[tokio::test]
    async fn the_total_binds_when_both_classes_are_busy() {
        let g = MediaGate::new(Limits {
            total: 3,
            reel: 2,
            remux: 2,
            idle: Duration::from_secs(30),
            remux_idle: Duration::from_secs(30),
            lifetime: Duration::from_secs(60),
        });
        let _reel = [g.try_enter(MediaClass::Reel).unwrap(), g.try_enter(MediaClass::Reel).unwrap()];
        assert!(g.try_enter(MediaClass::Reel).is_none(), "the class limit");
        let _remux = g.try_enter(MediaClass::Remux).unwrap();
        assert!(g.try_enter(MediaClass::Remux).is_none(), "the total, with the class not yet full");
        // A refusal must not leak the permit it took on the way to failing.
        assert_eq!(g.classes[MediaClass::Remux as usize].available_permits(), 1);
        assert_eq!(g.total.available_permits(), 0);
    }

    #[tokio::test(start_paused = true)]
    async fn an_idle_stream_is_cut_and_returns_its_permit() {
        let g = gate(1, 5, 60);
        let b = body(g.guard(pending(), g.try_enter(MediaClass::Remux).unwrap()));
        let read = tokio::spawn(async move { b.collect().await });
        tokio::time::advance(Duration::from_secs(6)).await;
        assert_eq!(read.await.unwrap().unwrap_err().kind(), io::ErrorKind::TimedOut);
        assert_eq!(active(&g, MediaClass::Remux), 0);
        assert_eq!(g.idle_timeouts.load(Relaxed), 1);
    }

    /// An upstream body that panics kills the pump task before it reports: the client must see an error, not a
    /// clean end, and the permit must come back.
    #[tokio::test]
    async fn a_pump_that_panics_is_an_error_not_a_clean_end() {
        let g = gate(1, 30, 60);
        let boom = futures_util::stream::poll_fn(|_| -> Poll<Option<Result<Frame<Bytes>, io::Error>>> {
            panic!("upstream body panicked (expected by the test)")
        });
        let b = body(
            g.guard(Response::new(StreamBody::new(boom).boxed()), g.try_enter(MediaClass::Remux).unwrap()),
        );
        let error = b.collect().await.expect_err("a truncated body ended as if it were complete");
        assert_eq!(error.kind(), io::ErrorKind::Other);
        assert_eq!(active(&g, MediaClass::Remux), 0);
    }

    /// A stalled Remux reader is cut at the shorter Remux deadline; a Reel reader that pauses the same time
    /// (a paused player on a progressive file) is still there.
    #[tokio::test(start_paused = true)]
    async fn remux_has_a_shorter_idle_deadline_than_reel() {
        let g = MediaGate::new(Limits {
            total: 2,
            reel: 1,
            remux: 1,
            idle: Duration::from_secs(30),
            remux_idle: Duration::from_secs(10),
            lifetime: Duration::from_secs(600),
        });
        let remux = body(g.guard(pending(), g.try_enter(MediaClass::Remux).unwrap()));
        let reel = body(g.guard(pending(), g.try_enter(MediaClass::Reel).unwrap()));
        let remux = tokio::spawn(async move { remux.collect().await });
        let reel = tokio::spawn(async move { reel.collect().await });
        tokio::time::advance(Duration::from_secs(11)).await;
        assert_eq!(remux.await.unwrap().unwrap_err().kind(), io::ErrorKind::TimedOut);
        assert_eq!(active(&g, MediaClass::Remux), 0);
        assert_eq!(active(&g, MediaClass::Reel), 1, "a 11 s pause cut a Reel stream");
        tokio::time::advance(Duration::from_secs(20)).await;
        assert_eq!(reel.await.unwrap().unwrap_err().kind(), io::ErrorKind::TimedOut);
    }

    #[tokio::test(start_paused = true)]
    async fn a_body_hyper_never_polls_still_times_out() {
        let g = gate(1, 5, 60);
        let _unpolled = g.guard(pending(), g.try_enter(MediaClass::Remux).unwrap());
        assert_eq!(active(&g, MediaClass::Remux), 1);
        tokio::time::advance(Duration::from_secs(6)).await;
        tokio::task::yield_now().await;
        assert_eq!(active(&g, MediaClass::Remux), 0);
        assert!(g.try_enter(MediaClass::Remux).is_some());
    }

    #[tokio::test(start_paused = true)]
    async fn the_lifetime_ceiling_cuts_a_stream_that_is_not_idle() {
        let g = gate(1, 60, 5);
        // One frame a second: never idle, always too long.
        let frames = futures_util::stream::unfold((), |()| async {
            tokio::time::sleep(Duration::from_secs(1)).await;
            Some((Ok::<_, io::Error>(Frame::data(Bytes::from_static(b"x"))), ()))
        });
        let b = body(
            g.guard(Response::new(StreamBody::new(frames).boxed()), g.try_enter(MediaClass::Reel).unwrap()),
        );
        let read = tokio::spawn(async move { b.collect().await });
        tokio::time::advance(Duration::from_secs(7)).await;
        assert_eq!(read.await.unwrap().unwrap_err().kind(), io::ErrorKind::TimedOut);
        assert_eq!(g.lifetime_timeouts.load(Relaxed), 1);
        assert_eq!(active(&g, MediaClass::Reel), 0);
    }

    #[tokio::test]
    async fn an_upstream_error_is_terminal_and_returns_the_permit() {
        let g = gate(1, 30, 60);
        let frames = futures_util::stream::iter([
            Err::<Frame<Bytes>, _>(io::Error::other("upstream broke")),
            Ok(Frame::data(Bytes::from_static(b"must not follow"))),
        ]);
        let mut b = body(
            g.guard(Response::new(StreamBody::new(frames).boxed()), g.try_enter(MediaClass::Reel).unwrap()),
        );
        assert_eq!(b.frame().await.unwrap().unwrap_err().kind(), io::ErrorKind::Other);
        assert!(b.frame().await.is_none());
        assert_eq!(active(&g, MediaClass::Reel), 0);
    }

    #[tokio::test]
    async fn one_frame_of_backpressure_reaches_the_upstream() {
        // An upstream that counts how many frames were pulled from it. With nobody reading, the pump
        // may hold one frame in the channel and one in hand: never an unbounded read-ahead.
        let pulled = Arc::new(AtomicU64::new(0));
        let counter = pulled.clone();
        let frames = futures_util::stream::unfold((), move |()| {
            let counter = counter.clone();
            async move {
                counter.fetch_add(1, Relaxed);
                Some((Ok::<_, io::Error>(Frame::data(Bytes::from_static(&[1; 4096]))), ()))
            }
        });
        let g = gate(1, 30, 60);
        let mut b = Box::pin(body(
            g.guard(Response::new(StreamBody::new(frames).boxed()), g.try_enter(MediaClass::Remux).unwrap()),
        ));
        // Start the pump without consuming a frame.
        {
            let waker = futures_util::task::noop_waker();
            let mut cx = Context::from_waker(&waker);
            let _ = hyper::body::Body::poll_frame(b.as_mut(), &mut cx);
        }
        for _ in 0..50 {
            tokio::task::yield_now().await;
        }
        let pulled_unread = pulled.load(Relaxed);
        assert!(pulled_unread <= 3, "read {pulled_unread} frames ahead of a reader that read none");
        // Reading one releases exactly one more.
        let _ = b.as_mut().frame().await;
        for _ in 0..50 {
            tokio::task::yield_now().await;
        }
        assert!(pulled.load(Relaxed) <= pulled_unread + 1);
    }
}
