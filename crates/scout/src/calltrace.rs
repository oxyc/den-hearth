//! The upstream calls one request made, and how long each took, for its Server-Timing and its log line.
//!
//! A /play that took five seconds says only `resolve;dur=5000` without this: which of the half-dozen TorBox calls
//! behind it was slow is the question that decides what to shorten. A request opens a trace with [`scope`]; every
//! TorBox call made under it adds one entry. Shared work (a `DetachedFlight`) carries its leader's trace onto its
//! own task, so the leader's line names the calls the flight made; a caller that joined someone else's flight
//! sees the wait in its own phase timing and no calls, which is what it paid for.

use std::future::Future;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

/// One request's calls, in the order they finished.
pub(crate) type Trace = Arc<Mutex<Vec<(&'static str, Duration)>>>;

tokio::task_local! {
    static TRACE: Trace;
}

/// A new, empty trace.
pub(crate) fn new_trace() -> Trace {
    Arc::new(Mutex::new(Vec::new()))
}

/// The trace the current task is recording into, if any.
pub(crate) fn current() -> Option<Trace> {
    TRACE.try_with(Arc::clone).ok()
}

/// Runs `future` recording into `trace`; with `None` it runs untraced.
pub(crate) async fn scope<T>(trace: Option<Trace>, future: impl Future<Output = T>) -> T {
    match trace {
        Some(trace) => TRACE.scope(trace, future).await,
        None => future.await,
    }
}

/// Adds one call to the current task's trace. Untraced work records nothing.
pub(crate) fn record(name: &'static str, took: Duration) {
    let _ = TRACE.try_with(|t| t.lock().unwrap_or_else(PoisonError::into_inner).push((name, took)));
}

/// The trace as Server-Timing entries, `torbox_<endpoint>;dur=<ms>`, in call order. Empty when nothing was called.
pub(crate) fn timings(trace: &Trace) -> String {
    let calls = trace.lock().unwrap_or_else(PoisonError::into_inner);
    calls
        .iter()
        .map(|(name, took)| den_http::timing(&format!("torbox_{name}"), *took))
        .collect::<Vec<_>>()
        .join(", ")
}
