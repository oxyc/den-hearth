//! Port of `internal/scout/linkmemo.go`. Work package 6.
//!
//! Remembers minted playback links, in process memory only: a playback link is a credential for as long as it
//! lives, so it never goes through the shared cache, which writes through to disk.

use std::collections::{HashMap, VecDeque};
use std::sync::{Mutex, MutexGuard, PoisonError};
use std::time::{Duration, SystemTime};

use crate::config::Config;
use crate::stores::ResolveTarget;
use crate::util::key_hash;

/// Go `linkMemoTTL`: how long a minted playback link is reused before /play mints another.
///
/// Well under what any provider gives a link (TorBox's and Real-Debrid's both live for hours), so a memo hit is
/// never the reason a link has expired. The memo exists for the minutes around a play: the stream list's probe
/// mints a link and throws it away, then /play mints the same file again; a viewer backs out and presses play
/// again; the player retries. Each of those was a full resolve against the debrid.
pub const LINK_MEMO_TTL: Duration = Duration::from_secs(10 * 60);

/// Go `linkMemoMaxEntries`: how many links are kept. One household opens a handful of titles in ten minutes;
/// this is room for every release a probe fan-out touches across many lists, and a ceiling on what a caller can
/// make the process hold, since both the key and the set of keys come from requests.
pub const LINK_MEMO_MAX_ENTRIES: usize = 256;

/// Go `memoLink`: one minted link. `checked` says the link has passed `checkLink`, so a hit can skip it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MemoLink {
    pub key: String,
    pub link: String,
    pub checked: bool,
    pub expires: SystemTime,
}

/// Go `linkMemo`. Usable as a default value, because tests build the handler as a struct literal.
///
/// Deliberately NOT the shared Cache: that one writes through to disk in production, and a playback link is a
/// credential for as long as it lives. Losing the memo on a restart costs one resolve per title.
#[derive(Default)]
pub struct LinkMemo {
    inner: Mutex<LinkMemoInner>,
}

/// The LRU list and the key map, behind the one lock. The owner decides the representation.
#[derive(Default)]
struct LinkMemoInner {
    /// Go `ll` (front = most recently used), as keys; `items` holds the entries.
    order: VecDeque<String>,
    items: HashMap<String, MemoLink>,
}

impl LinkMemoInner {
    /// Go `ll.MoveToFront`.
    fn touch(&mut self, key: &str) {
        if let Some(pos) = self.order.iter().position(|k| k == key) {
            if let Some(k) = self.order.remove(pos) {
                self.order.push_front(k);
            }
        }
    }

    /// Go `ll.Remove` and `delete(items, key)`.
    fn remove(&mut self, key: &str) {
        self.items.remove(key);
        self.order.retain(|k| k != key);
    }
}

impl LinkMemo {
    fn lock(&self) -> MutexGuard<'_, LinkMemoInner> {
        self.inner.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Go `get`: a live entry, refreshing its recency. An expired one is dropped on the way past.
    pub fn get(&self, key: &str, now: SystemTime) -> Option<MemoLink> {
        let mut inner = self.lock();
        let entry = inner.items.get(key)?.clone();
        if now >= entry.expires {
            inner.remove(key);
            return None;
        }
        inner.touch(key);
        Some(entry)
    }

    /// Go `put`: stores a freshly minted link, replacing whatever was there and starting its TTL now. Evicts the
    /// least recently used past `LINK_MEMO_MAX_ENTRIES`.
    pub fn put(&self, key: &str, link: &str, checked: bool, now: SystemTime) {
        let mut inner = self.lock();
        let entry = MemoLink { key: key.to_owned(), link: link.to_owned(), checked, expires: now + LINK_MEMO_TTL };
        if inner.items.contains_key(key) {
            inner.items.insert(key.to_owned(), entry);
            inner.touch(key);
            return;
        }
        inner.items.insert(key.to_owned(), entry);
        inner.order.push_front(key.to_owned());
        while inner.order.len() > LINK_MEMO_MAX_ENTRIES {
            let Some(oldest) = inner.order.pop_back() else { break };
            inner.items.remove(&oldest);
        }
    }

    /// Go `markChecked`: records that the remembered link passed `checkLink`, without restarting its TTL. The TTL
    /// counts from when the debrid minted it, not from when it was last looked at.
    pub fn mark_checked(&self, key: &str, link: &str) {
        let mut inner = self.lock();
        if let Some(entry) = inner.items.get_mut(key) {
            if entry.link == link {
                entry.checked = true;
            }
        }
    }

    /// Go `forget`: drops an entry, a link that failed or one the client says it could not open.
    pub fn forget(&self, key: &str) {
        self.lock().remove(key);
    }

    /// Go `ll.Len()`: how many entries the recency list holds.
    pub(crate) fn lru_len(&self) -> usize {
        self.lock().order.len()
    }

    /// Go `len(items)`: how many keys the map holds.
    pub(crate) fn map_len(&self) -> usize {
        self.lock().items.len()
    }
}

/// Go `linkMemoKey`: names the FILE for these accounts: whose debrid minted it, which torrent, which file in it.
///
/// The accounts are part of the key because a link is minted on one account's behalf: two installs on one
/// instance must never be handed each other's. Hashed, so the tokens are never a map key in memory.
///
/// `no_add` is left out on purpose. The probe resolves read-only and /play does not, but they name the same file,
/// and the probe's link is the one /play would otherwise mint again.
pub fn link_memo_key(config: &Config, t: &ResolveTarget) -> String {
    let mut accounts = String::new();
    for d in &config.debrid {
        accounts.push_str(d.service.as_str());
        accounts.push('\0');
        accounts.push_str(&d.token);
        accounts.push('\0');
    }
    let mut key = format!("link:{}:{}", key_hash(&accounts), t.info_hash);
    if let Some(idx) = t.file_idx {
        key.push_str(&format!(":f{idx}"));
    }
    if let (Some(season), Some(episode)) = (t.season, t.episode) {
        key.push_str(&format!(":s{season}e{episode}"));
    }
    key
}
