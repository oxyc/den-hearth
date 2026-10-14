//! Request and connection admission, after den-edge#120/#130.
//!
//! One process serves three services whose work differs by orders of magnitude: a `/health` probe is
//! microseconds, a Remux segment is megabytes held for seconds. Without a bound, enough of the second
//! starves the first (the probe times out, the supervisor restarts a healthy node) and every in-flight
//! request owns buffers, so memory grows with offered load rather than with a limit.
//!
//! Three bounds, outermost first:
//!
//! 1. **Connections** — a permit is taken BEFORE `accept`, so when the cap is reached the next peer waits
//!    in the kernel's listen backlog and costs no task, buffer or descriptor in userspace.
//! 2. **Requests** — a total with a reserved control lane. Bulk requests take the bulk permit first and
//!    then a total permit, so queued bulk work can never occupy the positions held back for control. A
//!    request waits a short, fixed time for a permit, then gets a stable `503` with `Retry-After`.
//! 3. **Media** (`media.rs`) — streamed bodies, which outlive their handler, own their own permits.
//!
//! The request permit belongs to the response BODY, not the handler future: Hyper may spend far longer
//! delivering a body than building its headers, and a dropped (cancelled) body releases it.

use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use hyper::body::Frame;
use hyper::{Response, StatusCode};
use tokio::sync::{OwnedSemaphorePermit, Semaphore, TryAcquireError};

use crate::route::Lane;
use crate::Body;

/// Accepted HTTP/1 connections across every listener (the main port and each service's own), idle
/// keep-alives and half-sent request heads included. At the `SCOUT_READ_BUFFER` ceiling this is a ~8 MiB
/// protocol-buffer upper bound.
pub const CONNECTIONS: usize = 256;
/// Connections accepted PAST the cap, on probation: they may carry control requests only and are closed after
/// one response. Without them a flood of persistent clients holding all 256 positions keeps every new
/// connection, a `/health` probe included, waiting in the listen backlog for as long as the flood lasts. A
/// probation connection is also the cheap way to tell an overflowing client "busy" (a stable 503) instead of
/// leaving it silent. They are few, short (`PROBATION_HEADER_TIMEOUT`) and never kept alive, so the memory they
/// add is a constant (64 x 32 KiB at most). The count is what lets a control connection get through a flood
/// that reconnects as fast as it is refused: each overflow connection holds a slot until it has sent its head.
pub const PROBATION_CONNECTIONS: usize = 64;
/// How long a probation connection may take to send its request head. A real client sends it at once.
pub const PROBATION_HEADER_TIMEOUT: Duration = Duration::from_secs(1);
/// Hyper's per-connection read buffer ceiling. Its default lets an adaptive buffer grow to ~400 KiB.
/// It also bounds a request head, so it stays comfortably above any real URL + headers.
pub const CONNECTION_READ_BUFFER: usize = 16 * 1024;
/// The ceiling where Scout can be the target: its own listener and the main port's `/scout`. A configured
/// install's sealed config rides in the URL path and Go served up to 32 KiB of head (`MaxHeaderBytes`),
/// twice the largest segment a URL legitimately carries; 16 KiB would refuse a real install's link.
pub const SCOUT_READ_BUFFER: usize = crate::scout::MAX_HEAD;

/// The read buffer ceiling for a connection on the listener of `own` (`None` is the main port).
pub fn read_buffer(own: Option<crate::route::Service>) -> usize {
    match own {
        None | Some(crate::route::Service::Scout) => SCOUT_READ_BUFFER,
        Some(_) => CONNECTION_READ_BUFFER,
    }
}

/// Live handlers and response bodies. The bulk share leaves `REQUESTS - BULK_REQUESTS` positions only
/// control routes can take.
pub const REQUESTS: usize = 64;
pub const BULK_REQUESTS: usize = 48;
/// How long a request waits for a permit before it is refused: long enough to absorb a burst, short
/// enough that a refusal is prompt.
pub const ADMISSION_WAIT: Duration = Duration::from_millis(25);
/// A non-media response body (a subtitle file, a playlist, a release list) that moves no frame for this long
/// gives its request position back. Without it a client that stops reading holds a position for as long as
/// its connection lasts, and 48 of them would pin the whole bulk lane. Media has its own clocks (`media.rs`).
pub const BODY_IDLE: Duration = Duration::from_secs(30);

/// What the node may hold; `Default` is production. Tests shrink it.
#[derive(Clone, Copy, Debug)]
pub struct Limits {
    pub connections: usize,
    pub probation: usize,
    pub requests: usize,
    pub bulk_requests: usize,
    pub wait: Duration,
    pub body_idle: Duration,
}

impl Default for Limits {
    fn default() -> Self {
        Limits {
            connections: CONNECTIONS,
            probation: PROBATION_CONNECTIONS,
            requests: REQUESTS,
            bulk_requests: BULK_REQUESTS,
            wait: ADMISSION_WAIT,
            body_idle: BODY_IDLE,
        }
    }
}

#[derive(Default)]
struct LaneStats {
    active: AtomicU64,
    high_water: AtomicU64,
    waited: AtomicU64,
    refused: AtomicU64,
}

#[derive(Default)]
struct ConnectionStats {
    active: AtomicU64,
    high_water: AtomicU64,
    waited: AtomicU64,
    /// Connections accepted past the cap, on probation.
    probation: AtomicU64,
    /// Requests on a probation connection that were not control and so were refused.
    probation_refused: AtomicU64,
}

pub struct Admission {
    wait: Duration,
    body_idle: Duration,
    /// Bodies that moved no frame for `body_idle` and gave their position back.
    body_idle_timeouts: AtomicU64,
    connections: Arc<Semaphore>,
    probation: Arc<Semaphore>,
    total: Arc<Semaphore>,
    bulk: Arc<Semaphore>,
    lanes: [LaneStats; 2],
    conn_stats: ConnectionStats,
}

/// Why a request was not admitted. There is only one reason, and it is told to the client the same way
/// every time.
#[derive(Debug)]
pub struct Refused;

impl Admission {
    pub fn new(limits: Limits) -> Arc<Self> {
        assert!(limits.bulk_requests < limits.requests, "the control lane would have no reserved position");
        Arc::new(Admission {
            wait: limits.wait,
            body_idle: limits.body_idle,
            body_idle_timeouts: AtomicU64::new(0),
            connections: Arc::new(Semaphore::new(limits.connections)),
            probation: Arc::new(Semaphore::new(limits.probation)),
            total: Arc::new(Semaphore::new(limits.requests)),
            bulk: Arc::new(Semaphore::new(limits.bulk_requests)),
            lanes: Default::default(),
            conn_stats: ConnectionStats::default(),
        })
    }

    /// A connection permit if one is free right now.
    pub fn try_connection(self: &Arc<Self>) -> Option<ConnectionPermit> {
        match self.connections.clone().try_acquire_owned() {
            Ok(permit) => Some(self.admit_connection(permit)),
            Err(TryAcquireError::NoPermits) => None,
            Err(TryAcquireError::Closed) => unreachable!("the connection semaphore is never closed"),
        }
    }

    /// A probation permit if one is free right now: the right to accept ONE connection past the cap and
    /// serve it control requests only. Never waits.
    pub fn try_probation(self: &Arc<Self>) -> Option<ConnectionPermit> {
        let permit = self.probation.clone().try_acquire_owned().ok()?;
        Some(self.admit_connection(permit))
    }

    /// Count a connection that was in the end served on probation.
    pub fn probation_accepted(&self) {
        self.conn_stats.probation.fetch_add(1, Relaxed);
    }

    /// Count a request refused on a probation connection.
    pub fn probation_refused(&self) {
        self.conn_stats.probation_refused.fetch_add(1, Relaxed);
    }

    /// Wait for a connection permit; the caller has already counted the wait.
    pub async fn connection(self: &Arc<Self>) -> ConnectionPermit {
        self.conn_stats.waited.fetch_add(1, Relaxed);
        let permit =
            self.connections.clone().acquire_owned().await.expect("the connection semaphore is never closed");
        self.admit_connection(permit)
    }

    fn admit_connection(self: &Arc<Self>, permit: OwnedSemaphorePermit) -> ConnectionPermit {
        let active = self.conn_stats.active.fetch_add(1, Relaxed) + 1;
        self.conn_stats.high_water.fetch_max(active, Relaxed);
        ConnectionPermit { _permit: permit, admission: self.clone() }
    }

    /// Admit one request into `lane`, waiting at most `wait` for a position.
    pub async fn admit(self: &Arc<Self>, lane: Lane) -> Result<RequestPermit, Refused> {
        let stats = &self.lanes[lane as usize];
        // The bulk subcap first: a bulk waiter must never sit on a total position while it queues, or
        // the positions reserved for control would be reachable through it.
        let (bulk, bulk_waited) = match lane {
            Lane::Bulk => match self.permit(&self.bulk).await {
                Ok((permit, waited)) => (Some(permit), waited),
                Err(Refused) => {
                    stats.refused.fetch_add(1, Relaxed);
                    return Err(Refused);
                }
            },
            Lane::Control => (None, false),
        };
        let (total, total_waited) = match self.permit(&self.total).await {
            Ok(pair) => pair,
            Err(Refused) => {
                stats.refused.fetch_add(1, Relaxed);
                return Err(Refused);
            }
        };
        if bulk_waited || total_waited {
            stats.waited.fetch_add(1, Relaxed);
        }
        let active = stats.active.fetch_add(1, Relaxed) + 1;
        stats.high_water.fetch_max(active, Relaxed);
        Ok(RequestPermit { _total: total, _bulk: bulk, admission: self.clone(), lane })
    }

    async fn permit(&self, semaphore: &Arc<Semaphore>) -> Result<(OwnedSemaphorePermit, bool), Refused> {
        match semaphore.clone().try_acquire_owned() {
            Ok(permit) => Ok((permit, false)),
            Err(TryAcquireError::NoPermits) => {
                match tokio::time::timeout(self.wait, semaphore.clone().acquire_owned()).await {
                    Ok(Ok(permit)) => Ok((permit, true)),
                    _ => Err(Refused),
                }
            }
            Err(TryAcquireError::Closed) => Err(Refused),
        }
    }

    /// Prometheus samples, appended to hearth's `/metrics`.
    pub fn render(&self, out: &mut String) {
        use std::fmt::Write;
        let series = |out: &mut String, name: &str, kind: &str, help: &str| {
            let _ = writeln!(out, "# HELP den_hearth_{name} {help}\n# TYPE den_hearth_{name} {kind}");
        };
        series(
            out,
            "request_admission_active",
            "gauge",
            "Requests whose handler or response body is live, by lane.",
        );
        series(
            out,
            "request_admission_high_water",
            "gauge",
            "Highest simultaneous admitted requests, by lane.",
        );
        series(
            out,
            "request_admission_waited_total",
            "counter",
            "Requests that waited for admission, by lane.",
        );
        series(
            out,
            "request_admission_refused_total",
            "counter",
            "Requests refused after the wait, by lane.",
        );
        for (lane, label) in [(Lane::Control, "control"), (Lane::Bulk, "bulk")] {
            let s = &self.lanes[lane as usize];
            let _ = writeln!(
                out,
                "den_hearth_request_admission_active{{lane=\"{label}\"}} {}",
                s.active.load(Relaxed)
            );
            let _ = writeln!(
                out,
                "den_hearth_request_admission_high_water{{lane=\"{label}\"}} {}",
                s.high_water.load(Relaxed)
            );
            let _ = writeln!(
                out,
                "den_hearth_request_admission_waited_total{{lane=\"{label}\"}} {}",
                s.waited.load(Relaxed)
            );
            let _ = writeln!(
                out,
                "den_hearth_request_admission_refused_total{{lane=\"{label}\"}} {}",
                s.refused.load(Relaxed)
            );
        }
        series(
            out,
            "request_body_idle_timeouts_total",
            "counter",
            "Non-media response bodies that moved nothing for the idle deadline and gave their position back.",
        );
        let _ = writeln!(
            out,
            "den_hearth_request_body_idle_timeouts_total {}",
            self.body_idle_timeouts.load(Relaxed)
        );
        series(out, "connection_admission_active", "gauge", "Accepted HTTP connections currently live.");
        let _ =
            writeln!(out, "den_hearth_connection_admission_active {}", self.conn_stats.active.load(Relaxed));
        series(out, "connection_admission_high_water", "gauge", "Highest simultaneous accepted connections.");
        let _ = writeln!(
            out,
            "den_hearth_connection_admission_high_water {}",
            self.conn_stats.high_water.load(Relaxed)
        );
        series(
            out,
            "connection_admission_waited_total",
            "counter",
            "Times the accept loop waited for capacity.",
        );
        let _ = writeln!(
            out,
            "den_hearth_connection_admission_waited_total {}",
            self.conn_stats.waited.load(Relaxed)
        );
        series(
            out,
            "connection_admission_probation_total",
            "counter",
            "Connections accepted past the cap, on probation (control requests only).",
        );
        let _ = writeln!(
            out,
            "den_hearth_connection_admission_probation_total {}",
            self.conn_stats.probation.load(Relaxed)
        );
        series(
            out,
            "connection_admission_probation_refused_total",
            "counter",
            "Non-control requests refused on a probation connection.",
        );
        let _ = writeln!(
            out,
            "den_hearth_connection_admission_probation_refused_total {}",
            self.conn_stats.probation_refused.load(Relaxed)
        );
    }
}

/// Held for the life of an accepted connection.
pub struct ConnectionPermit {
    _permit: OwnedSemaphorePermit,
    admission: Arc<Admission>,
}

impl Drop for ConnectionPermit {
    fn drop(&mut self) {
        self.admission.conn_stats.active.fetch_sub(1, Relaxed);
    }
}

/// One admitted request. Dropping it gives both positions back.
pub struct RequestPermit {
    _total: OwnedSemaphorePermit,
    _bulk: Option<OwnedSemaphorePermit>,
    admission: Arc<Admission>,
    lane: Lane,
}

impl Drop for RequestPermit {
    fn drop(&mut self) {
        self.admission.lanes[self.lane as usize].active.fetch_sub(1, Relaxed);
    }
}

impl RequestPermit {
    /// Tie this permit to `response`'s body: released at its last frame, on an error, or when it is
    /// dropped unread. A body that is already finished does not need to hold anything.
    pub fn hold(self, response: Response<Body>) -> Response<Body> {
        if hyper::body::Body::is_end_stream(response.body()) {
            return response;
        }
        let idle = self.admission.body_idle;
        let shared = Arc::new(HeldShared {
            permit: std::sync::Mutex::new(Some(self)),
            quiet_since: AtomicU64::new(0),
            expired: std::sync::atomic::AtomicBool::new(false),
            began: tokio::time::Instant::now(),
        });
        // Hyper does not poll a body whose client has stopped reading, so the clock cannot live in
        // `poll_frame`: a watcher gives the position back when no frame has moved for `idle`.
        let watcher = tokio::spawn(watch_idle(shared.clone(), idle));
        response.map(|body| Held { body, shared, watcher }.boxed())
    }
}

/// What a held body and its watcher share: the position, and when a frame last moved.
struct HeldShared {
    permit: std::sync::Mutex<Option<RequestPermit>>,
    /// Milliseconds after `began` of the last frame (or of the start).
    quiet_since: AtomicU64,
    expired: std::sync::atomic::AtomicBool,
    began: tokio::time::Instant,
}

impl HeldShared {
    fn release(&self) {
        // A poisoned lock only means a holder panicked; the position still has to come back.
        let mut permit = self.permit.lock().unwrap_or_else(|e| e.into_inner());
        permit.take();
    }
}

async fn watch_idle(shared: Arc<HeldShared>, idle: Duration) {
    loop {
        let seen = shared.quiet_since.load(Relaxed);
        tokio::time::sleep_until(shared.began + Duration::from_millis(seen) + idle).await;
        if shared.quiet_since.load(Relaxed) == seen {
            let admission = {
                let permit = shared.permit.lock().unwrap_or_else(|e| e.into_inner());
                permit.as_ref().map(|p| p.admission.clone())
            };
            if let Some(admission) = admission {
                admission.body_idle_timeouts.fetch_add(1, Relaxed);
            }
            shared.expired.store(true, Relaxed);
            shared.release();
            return;
        }
    }
}

struct Held {
    body: Body,
    shared: Arc<HeldShared>,
    watcher: tokio::task::JoinHandle<()>,
}

impl Drop for Held {
    fn drop(&mut self) {
        self.watcher.abort();
        // At once, not when the aborted watcher is next scheduled: a dropped body gives its position back now.
        self.shared.release();
    }
}

impl hyper::body::Body for Held {
    type Data = Bytes;
    type Error = std::io::Error;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, std::io::Error>>> {
        if self.shared.expired.load(Relaxed) {
            return Poll::Ready(Some(Err(std::io::Error::new(
                std::io::ErrorKind::TimedOut,
                "response body idle timeout",
            ))));
        }
        let frame = Pin::new(&mut self.body).poll_frame(cx);
        match &frame {
            Poll::Ready(None | Some(Err(_))) => {
                self.shared.release();
                self.watcher.abort();
            }
            Poll::Ready(Some(Ok(_))) => {
                let now = self.shared.began.elapsed().as_millis() as u64;
                self.shared.quiet_since.store(now, Relaxed);
            }
            Poll::Pending => {}
        }
        frame
    }

    fn is_end_stream(&self) -> bool {
        self.body.is_end_stream()
    }

    fn size_hint(&self) -> hyper::body::SizeHint {
        self.body.size_hint()
    }
}

/// A 503 a SERVICE answers (Remux's `scratch_unavailable`, `scout_unconfigured`, Subtitles'
/// `service_unavailable`, `translation_unavailable`) carries no `Retry-After`, unlike hearth's own refusals.
/// Give it one so every 503 from this node tells a client the same thing. Five seconds, not the one second of
/// "busy": nothing here clears in a moment.
pub fn with_retry_after(mut response: Response<Body>) -> Response<Body> {
    if response.status() == StatusCode::SERVICE_UNAVAILABLE
        && !response.headers().contains_key(hyper::header::RETRY_AFTER)
    {
        response
            .headers_mut()
            .insert(hyper::header::RETRY_AFTER, hyper::header::HeaderValue::from_static("5"));
    }
    response
}

/// The refusal for a request that found no position: always this status, this body and this
/// `Retry-After`, so a client can act on it. `Retry-After` is never `0`, which invites an immediate retry.
pub fn busy(code: &str) -> Response<Body> {
    let body = serde_json::json!({"error": code}).to_string();
    Response::builder()
        .status(StatusCode::SERVICE_UNAVAILABLE)
        .header("content-type", "application/json")
        .header("cache-control", "no-store")
        .header("retry-after", "1")
        // A browser client can only read the refusal (and honour the cooldown) if CORS lets it.
        .header("access-control-allow-origin", "*")
        .header("access-control-expose-headers", "Retry-After")
        .body(Full::new(Bytes::from(body)).map_err(|never| match never {}).boxed())
        .expect("static response")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn small() -> Arc<Admission> {
        Admission::new(Limits {
            connections: 2,
            probation: 1,
            requests: 3,
            bulk_requests: 2,
            wait: Duration::from_millis(5),
            body_idle: Duration::from_secs(30),
        })
    }

    #[tokio::test]
    async fn bulk_cannot_take_the_reserved_control_position() {
        let adm = small();
        let _a = adm.admit(Lane::Bulk).await.unwrap();
        let _b = adm.admit(Lane::Bulk).await.unwrap();
        // The bulk subcap is spent; the total still has one position, and it is control's.
        assert!(adm.admit(Lane::Bulk).await.is_err());
        assert_eq!(adm.total.available_permits(), 1, "a refused bulk request held a total position");
        let control = adm.admit(Lane::Control).await.expect("control is served while bulk is saturated");
        assert!(adm.admit(Lane::Control).await.is_err(), "the total is the ceiling for control too");
        drop(control);
        assert!(adm.admit(Lane::Control).await.is_ok());
        assert_eq!(adm.lanes[Lane::Bulk as usize].refused.load(Relaxed), 1);
        assert_eq!(adm.lanes[Lane::Control as usize].refused.load(Relaxed), 1);
    }

    #[tokio::test]
    async fn a_queued_bulk_request_takes_a_position_when_one_frees_within_the_wait() {
        let adm = Admission::new(Limits {
            connections: 1,
            probation: 1,
            requests: 2,
            bulk_requests: 1,
            wait: Duration::from_millis(500),
            body_idle: Duration::from_secs(30),
        });
        let held = adm.admit(Lane::Bulk).await.unwrap();
        let waiting = tokio::spawn({
            let adm = adm.clone();
            async move { adm.admit(Lane::Bulk).await.is_ok() }
        });
        tokio::task::yield_now().await;
        drop(held);
        assert!(waiting.await.unwrap());
        assert_eq!(adm.lanes[Lane::Bulk as usize].waited.load(Relaxed), 1);
    }

    #[tokio::test]
    async fn the_permit_follows_the_body_not_the_handler() {
        let adm = small();
        let permit = adm.admit(Lane::Bulk).await.unwrap();
        let (tx, rx) = tokio::sync::mpsc::channel::<Result<Frame<Bytes>, std::io::Error>>(1);
        let stream = futures_util::stream::unfold(rx, |mut rx| async { rx.recv().await.map(|f| (f, rx)) });
        let response = Response::new(http_body_util::StreamBody::new(stream).boxed());
        let response = permit.hold(response);
        assert_eq!(
            adm.lanes[Lane::Bulk as usize].active.load(Relaxed),
            1,
            "released when the headers were built"
        );
        assert_eq!(adm.total.available_permits(), 2);
        // Dropped unread, as when the client goes away.
        drop(response);
        drop(tx);
        assert_eq!(adm.lanes[Lane::Bulk as usize].active.load(Relaxed), 0);
        assert_eq!(adm.total.available_permits(), 3);
        assert_eq!(adm.bulk.available_permits(), 2);
    }

    #[tokio::test]
    async fn the_permit_is_released_at_the_last_frame_while_the_body_still_exists() {
        let adm = small();
        let permit = adm.admit(Lane::Bulk).await.unwrap();
        let response = Response::new(Full::new(Bytes::from_static(b"x")).map_err(|n| match n {}).boxed());
        // A body whose only frame has not been read is not "finished".
        let mut body = permit.hold(response).into_body();
        assert_eq!(adm.lanes[Lane::Bulk as usize].active.load(Relaxed), 1);
        assert!(body.frame().await.unwrap().unwrap().is_data());
        assert!(body.frame().await.is_none());
        assert_eq!(adm.lanes[Lane::Bulk as usize].active.load(Relaxed), 0, "kept its position past EOF");
    }

    /// A client that stops reading is never polled again, so the position must come back without a poll;
    /// one that keeps moving frames keeps it.
    #[tokio::test(start_paused = true)]
    async fn a_body_that_moves_nothing_gives_its_position_back_without_being_polled() {
        let adm = small();
        let lane = || adm.lanes[Lane::Bulk as usize].active.load(Relaxed);
        let permit = adm.admit(Lane::Bulk).await.unwrap();
        let (tx, rx) = tokio::sync::mpsc::channel::<Result<Frame<Bytes>, std::io::Error>>(4);
        let stream = futures_util::stream::unfold(rx, |mut rx| async { rx.recv().await.map(|f| (f, rx)) });
        let mut body =
            permit.hold(Response::new(http_body_util::StreamBody::new(stream).boxed())).into_body();

        // Frames inside the deadline keep resetting it.
        for _ in 0..3 {
            tokio::time::advance(Duration::from_secs(20)).await;
            tx.send(Ok(Frame::data(Bytes::from_static(b"x")))).await.unwrap();
            assert!(body.frame().await.unwrap().unwrap().is_data());
            assert_eq!(lane(), 1, "a body that is moving lost its position");
        }
        // Then nothing is read for longer than the deadline, and nobody polls the body.
        tokio::time::advance(Duration::from_secs(31)).await;
        tokio::task::yield_now().await;
        assert_eq!(lane(), 0, "a stalled body kept its position");
        assert_eq!(adm.body_idle_timeouts.load(Relaxed), 1);
        // When it is polled again it is told, rather than ending as if it were complete.
        let error = body.frame().await.unwrap().unwrap_err();
        assert_eq!(error.kind(), std::io::ErrorKind::TimedOut);
    }

    #[tokio::test]
    async fn a_body_that_is_already_finished_holds_nothing() {
        let adm = small();
        let permit = adm.admit(Lane::Bulk).await.unwrap();
        let response = Response::new(http_body_util::Empty::<Bytes>::new().map_err(|n| match n {}).boxed());
        let _response = permit.hold(response);
        assert_eq!(adm.lanes[Lane::Bulk as usize].active.load(Relaxed), 0);
    }

    #[test]
    fn a_service_503_gets_a_retry_after_and_nothing_else_is_touched() {
        let status = |code: StatusCode, retry: Option<&str>| {
            let mut b = Response::builder().status(code);
            if let Some(r) = retry {
                b = b.header("retry-after", r);
            }
            b.body(Full::new(Bytes::new()).map_err(|n| match n {}).boxed()).unwrap()
        };
        let plain = with_retry_after(status(StatusCode::SERVICE_UNAVAILABLE, None));
        assert_eq!(plain.headers()["retry-after"], "5");
        // One the service already set is its own.
        let own = with_retry_after(status(StatusCode::SERVICE_UNAVAILABLE, Some("30")));
        assert_eq!(own.headers()["retry-after"], "30");
        assert!(!with_retry_after(status(StatusCode::NOT_FOUND, None)).headers().contains_key("retry-after"));
        assert!(!with_retry_after(status(StatusCode::OK, None)).headers().contains_key("retry-after"));
    }

    #[tokio::test]
    async fn the_overload_contract_is_stable() {
        let response = busy("server_busy");
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(response.headers()["retry-after"], "1");
        assert_eq!(response.headers()["content-type"], "application/json");
        assert_eq!(response.headers()["cache-control"], "no-store");
        let body = response.into_body().collect().await.unwrap().to_bytes();
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&body).unwrap(),
            serde_json::json!({"error": "server_busy"})
        );
    }

    #[tokio::test]
    async fn connections_are_counted_and_capped() {
        let adm = small();
        let a = adm.try_connection().unwrap();
        let _b = adm.try_connection().unwrap();
        assert!(adm.try_connection().is_none());
        let waiter = tokio::spawn({
            let adm = adm.clone();
            async move { adm.connection().await }
        });
        tokio::task::yield_now().await;
        assert!(!waiter.is_finished());
        drop(a);
        let _c = waiter.await.unwrap();
        assert_eq!(adm.conn_stats.active.load(Relaxed), 2);
        assert_eq!(adm.conn_stats.high_water.load(Relaxed), 2);
        assert_eq!(adm.conn_stats.waited.load(Relaxed), 1);
    }

    #[test]
    fn metrics_name_every_lane_even_when_idle() {
        let adm = small();
        let mut text = String::new();
        adm.render(&mut text);
        for needle in [
            "den_hearth_request_admission_active{lane=\"control\"} 0",
            "den_hearth_request_admission_refused_total{lane=\"bulk\"} 0",
            "den_hearth_connection_admission_high_water 0",
        ] {
            assert!(text.contains(needle), "{needle}\n{text}");
        }
    }
}
