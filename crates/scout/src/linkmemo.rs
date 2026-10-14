//! Port of `internal/scout/linkmemo.go`. Work package 6.
//!
//! Remembers minted playback links, in process memory only: a playback link is a credential for as long as it
//! lives, so it never goes through the shared cache, which writes through to disk.

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{Duration, SystemTime};

use crate::config::Config;
use crate::stores::ResolveTarget;

/// Go `linkMemoTTL`: how long a minted playback link is reused before /play mints another.
pub const LINK_MEMO_TTL: Duration = Duration::from_secs(10 * 60);

/// Go `linkMemoMaxEntries`: how many links are kept.
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
#[derive(Default)]
pub struct LinkMemo {
    inner: Mutex<LinkMemoInner>,
}

/// The LRU list and the key map, behind the one lock. The owner decides the representation.
#[derive(Default)]
struct LinkMemoInner {
    /// Go `ll` (front = most recently used), and `items`.
    order: Vec<String>,
    items: HashMap<String, MemoLink>,
}

impl LinkMemo {
    /// Go `get`: a live entry, refreshing its recency. An expired one is dropped on the way past.
    pub fn get(&self, key: &str, now: SystemTime) -> Option<MemoLink> {
        todo!("linkmemo.go linkMemo.get")
    }

    /// Go `put`: stores a freshly minted link, replacing whatever was there and starting its TTL now. Evicts the
    /// least recently used past `LINK_MEMO_MAX_ENTRIES`.
    pub fn put(&self, key: &str, link: &str, checked: bool, now: SystemTime) {
        todo!("linkmemo.go linkMemo.put")
    }

    /// Go `markChecked`: records that the remembered link passed `checkLink`, without restarting its TTL.
    pub fn mark_checked(&self, key: &str, link: &str) {
        todo!("linkmemo.go linkMemo.markChecked")
    }

    /// Go `forget`: drops an entry.
    pub fn forget(&self, key: &str) {
        todo!("linkmemo.go linkMemo.forget")
    }

    /// Go `ll.Len()`: how many entries the recency list holds.
    pub(crate) fn lru_len(&self) -> usize {
        todo!("linkmemo.go linkMemo.ll.Len")
    }

    /// Go `len(items)`: how many keys the map holds.
    pub(crate) fn map_len(&self) -> usize {
        todo!("linkmemo.go len(linkMemo.items)")
    }
}

/// Go `linkMemoKey`: names the FILE for these accounts: whose debrid minted it, which torrent, which file in it.
/// Hashed accounts, and `no_add` is left out on purpose, so the probe's read-only target lands on /play's entry.
pub fn link_memo_key(config: &Config, t: &ResolveTarget) -> String {
    todo!("linkmemo.go linkMemoKey")
}
