//! What only the TorBox store needs beyond the shared store helpers (`stores/mod.rs`, `stores/refusal.rs`): the
//! sentinel errors it raises, the readers for the JSON shapes it decodes the way Go's `encoding/json` does, and the
//! detached single-flight its shared reads run through.

use std::collections::BTreeMap;
use std::future::Future;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::{Mutex, MutexGuard, PoisonError};

use serde_json::Value;
use tokio::sync::watch;

use crate::config::DebridService;
use crate::loglimit::log_limited;
use crate::stores::{dead_link, unavailable, StoreError, ERR_TORRENT_GONE, ERR_WOULD_ADD};
use crate::util::{fold_json_keys, Shape};

/// A poisoned lock still holds consistent state here (every edit finishes before anything can panic).
pub(super) fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(PoisonError::into_inner)
}

pub(crate) fn torrent_gone() -> StoreError {
    dead_link(ERR_TORRENT_GONE)
}

/// Go `errWouldAdd`: a NoAdd target could only be resolved by queueing the torrent, so it was not resolved.
pub(crate) fn would_add() -> StoreError {
    dead_link(ERR_WOULD_ADD)
}

/// Go `errNoFileList`: mylist gave no usable answer. Distinct from errTorrentGone, which is mylist ANSWERING
/// that the account no longer holds this id. Collapsing the two is what wedged a stale id: the refusal to guess
/// an episode fired before requestDownload, whose 404 was the only producer of errTorrentGone and so the only
/// thing that could forget the id and re-add the torrent.
pub(crate) fn no_file_list() -> StoreError {
    unavailable(DebridService::TorBox, "mylist gave no usable answer", 0)
}

// --- JSON as Go's encoding/json reads it ---

/// The first JSON value in the body, the rest ignored: Go's `json.NewDecoder(r).Decode(&v)`. A value whose keys
/// are then matched to `shape`'s fields case-insensitively, as Go's decoder matches them.
pub(crate) fn first_value(raw: &[u8], shape: &Shape) -> Option<Value> {
    let mut value = serde_json::Deserializer::from_slice(raw).into_iter::<Value>().next()?.ok()?;
    fold_json_keys(&mut value, shape);
    Some(value)
}

/// A response body as the struct Go decodes it into: an object, or null, which decodes cleanly into the zero
/// value. Anything else is a decode error.
pub(crate) fn envelope(v: Value) -> Option<serde_json::Map<String, Value>> {
    match v {
        Value::Object(m) => Some(m),
        Value::Null => Some(serde_json::Map::new()),
        _ => None,
    }
}

/// A field that Go declares as a plain `int`: absent and null leave the zero value, an integer is read, and
/// anything else is a type error.
pub(crate) fn int_or_zero(v: Option<&Value>) -> Result<i64, ()> {
    match v {
        None | Some(Value::Null) => Ok(0),
        Some(Value::Number(n)) => n.as_i64().ok_or(()),
        Some(_) => Err(()),
    }
}

/// A field that Go declares as `*int`.
pub(crate) fn opt_int(v: Option<&Value>) -> Result<Option<i64>, ()> {
    match v {
        None | Some(Value::Null) => Ok(None),
        Some(Value::Number(n)) => n.as_i64().map(Some).ok_or(()),
        Some(_) => Err(()),
    }
}

/// A field that Go declares as `*bool`.
pub(crate) fn opt_bool(v: Option<&Value>) -> Result<Option<bool>, ()> {
    match v {
        None | Some(Value::Null) => Ok(None),
        Some(Value::Bool(b)) => Ok(Some(*b)),
        Some(_) => Err(()),
    }
}

/// A field that Go declares as `*float64`.
pub(crate) fn opt_float(v: Option<&Value>) -> Result<Option<f64>, ()> {
    match v {
        None | Some(Value::Null) => Ok(None),
        Some(Value::Number(n)) => n.as_f64().map(Some).ok_or(()),
        Some(_) => Err(()),
    }
}

/// A field that Go declares as `*string`.
pub(crate) fn opt_string(v: Option<&Value>) -> Result<Option<String>, ()> {
    match v {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(s)) => Ok(Some(s.clone())),
        Some(_) => Err(()),
    }
}

/// A field that Go declares as a plain `string`.
pub(crate) fn string_or_empty(v: Option<&Value>) -> Result<String, ()> {
    Ok(opt_string(v)?.unwrap_or_default())
}

// --- concurrency ---

/// A key for work that is shared between callers, scoped to the runtime the caller is on.
///
/// Shared work runs in a task on the runtime that started it, and a caller on another runtime could be left
/// waiting for a task whose runtime has since shut down. The server has one runtime, so this changes nothing
/// there; it is also what keeps the work of two independent runtimes in one process (each test has its own)
/// from being mistaken for one account's.
pub(crate) fn scoped(key: &str) -> String {
    format!("{}:{key}", tokio::runtime::Handle::current().id())
}

/// Go `singleflight.Group.DoChan`: concurrent callers of one key share one run of the work and its answer, and the
/// work is NOT tied to the caller that started it. Dropping the caller that started the work (the request went
/// away, its budget ran out) leaves the work running for everyone still waiting, which is the property the
/// account listing and the shared status reads were built to have: "the leader now runs detached from
/// cancellation".
///
/// `indexerconfig::Flight` is not that: when its leader is dropped a waiter starts the work again. That is right
/// for a mint and wrong here, where the work is a 64 MiB listing a follower is still waiting on.
///
/// The work runs in its own task, so a panic in it ends that task and nothing else: every waiter is answered
/// `None` rather than left waiting, where Go would have died. This is Go's `recoverBackground`: a singleflight
/// closure that panics re-raises on a bare goroutine nothing can recover, so each of these jobs recovers its own,
/// counts it, and abandons the work.
pub(super) struct DetachedFlight<T> {
    calls: Mutex<BTreeMap<String, watch::Receiver<Option<Option<T>>>>>,
    /// What the work is, for the log line a panic writes. Always a literal, so it is a fixed vocabulary for the
    /// limiter's key.
    what: &'static str,
}

impl<T: Clone + Send + Sync + 'static> DetachedFlight<T> {
    pub(super) const fn new(what: &'static str) -> Self {
        DetachedFlight { calls: Mutex::new(BTreeMap::new()), what }
    }

    /// Runs `make()` for the first caller of `key`; a caller that arrives while it is in flight waits for the same
    /// answer. The key is forgotten before the answer is published, so a caller arriving after it runs the work
    /// again: nothing is cached here. The answer is `None` for a run that panicked, and the flag says whether
    /// this caller shared a flight someone else started (Go's `result.Shared`).
    pub(super) async fn run<F, Fut>(&'static self, key: &str, make: F) -> (Option<T>, bool)
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = T> + Send + 'static,
    {
        let key = scoped(key);
        let (mut rx, shared) = {
            let mut calls = lock(&self.calls);
            match calls.get(&key) {
                Some(rx) => (rx.clone(), true),
                None => {
                    let (tx, rx) = watch::channel(None);
                    calls.insert(key.clone(), rx.clone());
                    let work = make();
                    tokio::spawn(async move {
                        let answer = match tokio::spawn(work).await {
                            Ok(answer) => Some(answer),
                            Err(e) => {
                                if e.is_panic() {
                                    crate::metrics::metrics().background_panic.fetch_add(1, Ordering::Relaxed);
                                    log_limited(
                                        &format!("panic:{}", self.what),
                                        &format!("{} panicked, abandoning it: {}", self.what, panic_text(e.into_panic())),
                                    );
                                }
                                None
                            }
                        };
                        lock(&self.calls).remove(&key);
                        let _ = tx.send(Some(answer));
                    });
                    (rx, false)
                }
            }
        };
        let Ok(answered) = rx.wait_for(|v| v.is_some()).await else {
            return (None, shared);
        };
        let answer = (*answered).clone().flatten();
        drop(answered);
        (answer, shared)
    }
}

/// A panic's payload as Go's `%v` of the recovered value would print it.
fn panic_text(payload: Box<dyn std::any::Any + Send>) -> String {
    match payload.downcast::<&str>() {
        Ok(s) => (*s).to_owned(),
        Err(payload) => match payload.downcast::<String>() {
            Ok(s) => *s,
            Err(_) => "a panic with no message".to_owned(),
        },
    }
}

/// Adds one to a gauge while it lives (Go's `Add(1)` and a deferred `Add(-1)`).
pub(super) struct GaugeGuard(&'static AtomicI64);

impl GaugeGuard {
    pub(super) fn new(gauge: &'static AtomicI64) -> GaugeGuard {
        gauge.fetch_add(1, Ordering::Relaxed);
        GaugeGuard(gauge)
    }
}

impl Drop for GaugeGuard {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::Relaxed);
    }
}
