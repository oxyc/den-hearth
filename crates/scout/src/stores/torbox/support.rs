//! What only the TorBox store needs beyond the shared store helpers (`stores/mod.rs`, `stores/refusal.rs`): the
//! sentinel errors it raises, the readers for the JSON shapes it decodes the way Go's `encoding/json` does, and the
//! detached single-flight its shared reads run through.

use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::{Mutex, MutexGuard, PoisonError};

use serde_json::Value;

use crate::config::DebridService;
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
