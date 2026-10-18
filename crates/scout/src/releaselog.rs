//! One log line each time a release's download state changes, per episode.
//!
//! The decision lines on the play route go through `log_limited`, keyed by condition, so a season of downloads polled
//! together writes one line a minute between them ("probe served ready … (37 more like this)"): the episode that sat
//! at "not queued" for an hour behind its siblings never shows up. This keeps the last state each release answered
//! with, for each episode it was asked about, and writes a line whenever that changes, with the state it left. The
//! state is what the client was told (`downloading`, `stalled`, `not_queued`, `store_unavailable`, …), so a stuck
//! download reads as a history: `downloading -> stalled -> not_queued`.

use std::collections::{HashMap, VecDeque};
#[cfg(not(test))]
use std::sync::{Mutex, PoisonError};
use std::time::Duration;

use http::StatusCode;

use crate::decisionlog::{decision_line, resolve_identity};
use crate::handler::Resp;
use crate::loglimit::emit;
use crate::stores::ResolveTarget;

/// How many releases' states are remembered. A season pack polled for every episode is one entry per episode; past
/// the cap the longest-remembered goes, and its next answer logs again as a first sight.
pub(crate) const REMEMBERED: usize = 4096;

/// What a play-route answer told the client about its release. The helpers that build those answers attach it, so
/// the route logs every answer at the one place it passes.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ReleaseAnswer {
    pub(crate) state: String,
    pub(crate) service: String,
}

impl ReleaseAnswer {
    pub(crate) fn new(state: &str, service: &str) -> Self {
        Self { state: state.to_owned(), service: service.to_owned() }
    }
}

#[derive(Default)]
pub(crate) struct States {
    last: HashMap<String, String>,
    order: VecDeque<String>,
}

impl States {
    /// Records `state` for `key`, and answers the state it replaces when it changed: `Some("none")` on first sight.
    pub(crate) fn change(&mut self, key: &str, state: &str) -> Option<String> {
        match self.last.get_mut(key) {
            Some(last) if last == state => None,
            Some(last) => Some(std::mem::replace(last, state.to_owned())),
            None => {
                if self.order.len() >= REMEMBERED {
                    if let Some(oldest) = self.order.pop_front() {
                        self.last.remove(&oldest);
                    }
                }
                self.order.push_back(key.to_owned());
                self.last.insert(key.to_owned(), state.to_owned());
                Some("none".to_owned())
            }
        }
    }
}

#[cfg(not(test))]
static STATES: Mutex<Option<States>> = Mutex::new(None);

// A TEST build keeps it per thread, as `loglimit` does: a release one test saw would make another's first line a
// non-change.
#[cfg(test)]
thread_local! {
    static STATES: std::cell::RefCell<States> = std::cell::RefCell::new(States::default());
}

fn with_states<R>(f: impl FnOnce(&mut States) -> R) -> R {
    #[cfg(not(test))]
    {
        f(STATES.lock().unwrap_or_else(PoisonError::into_inner).get_or_insert_with(States::default))
    }
    #[cfg(test)]
    {
        STATES.with(|s| f(&mut s.borrow_mut()))
    }
}

/// The state an answer stands for: a redirect or a 200 is `ready`; anything else is the state its helper attached.
fn state_of(resp: &Resp) -> Option<ReleaseAnswer> {
    if resp.status() == StatusCode::FOUND || resp.status() == StatusCode::OK {
        return Some(ReleaseAnswer::new("ready", ""));
    }
    resp.extensions().get::<ReleaseAnswer>().cloned()
}

/// Logs `resp` as `rt`'s new state when it differs from the last one this release answered for this episode:
/// `event=download outcome=changed reason=<route>: <from> -> <to> upstream=<service> … rid=… imdb=… season=…
/// episode=… hash=… release=…`. `route` is `probe` or `play`.
pub(crate) fn observe(rt: &ResolveTarget, rid: &str, route: &str, dur: Duration, resp: &Resp) {
    if rt.info_hash.is_empty() {
        return;
    }
    let Some(answer) = state_of(resp) else { return };
    let key = format!("{}:{:?}:{:?}", rt.info_hash, rt.season, rt.episode);
    let Some(from) = with_states(|s| s.change(&key, &answer.state)) else { return };
    emit(&decision_line(
        "download",
        "changed",
        &format!("{route}: {from} -> {}", answer.state),
        &answer.service,
        dur,
        rid,
        &resolve_identity(rt),
    ));
}
