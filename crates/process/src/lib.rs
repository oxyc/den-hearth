//! Safe ownership rules around Unix process groups used by Reel and Remux.
//!
//! A registered id is removed as soon as its direct child is reaped. That is the PID-reuse safety
//! boundary: after reap, an old numeric id must never be signalled again. Each service owns its own
//! [`Registry`] value, so shutdown only drains subprocesses started by that service.

#![deny(unsafe_op_in_unsafe_fn)]

use std::collections::HashSet;
use std::sync::{Mutex, PoisonError};

/// Live process groups belonging to one service.
pub struct Registry {
    groups: Mutex<Option<HashSet<u32>>>,
}

impl Registry {
    pub const fn new() -> Self {
        Self { groups: Mutex::new(None) }
    }

    /// Register a freshly spawned child whose pid is also its pgid.
    pub fn register(&self, pgid: Option<u32>) {
        if let Some(pgid) = pgid.filter(|p| valid(*p)) {
            self.groups
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .get_or_insert_with(HashSet::new)
                .insert(pgid);
        }
    }

    /// Forget a group immediately after the direct child is reaped.
    pub fn unregister(&self, pgid: Option<u32>) {
        if let Some(pgid) = pgid {
            if let Some(groups) = self.groups.lock().unwrap_or_else(PoisonError::into_inner).as_mut() {
                groups.remove(&pgid);
            }
        }
    }

    /// Drain and SIGKILL every group still owned at shutdown.
    pub fn kill_all(&self) -> usize {
        let groups = self.groups.lock().unwrap_or_else(PoisonError::into_inner).take().unwrap_or_default();
        for pgid in &groups {
            kill_group(*pgid);
        }
        groups.len()
    }

    pub fn len(&self) -> usize {
        self.groups.lock().unwrap_or_else(PoisonError::into_inner).as_ref().map_or(0, HashSet::len)
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn contains(&self, pgid: u32) -> bool {
        self.groups
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .as_ref()
            .is_some_and(|groups| groups.contains(&pgid))
    }
}

impl Default for Registry {
    fn default() -> Self {
        Self::new()
    }
}

/// SIGKILL one child process group, refusing ids whose negation has special or lossy meaning.
pub fn kill_group(pgid: u32) {
    signal_group(pgid, signal::KILL);
}

/// Signal one child process group, refusing ids whose negation has special or lossy meaning.
pub fn signal_group(pgid: u32, signal: i32) {
    if !valid(pgid) {
        return;
    }
    platform::signal_group(pgid, signal);
}

fn valid(pgid: u32) -> bool {
    pgid > 1 && pgid <= i32::MAX as u32
}

pub mod signal {
    #[cfg(unix)]
    pub const KILL: i32 = libc::SIGKILL;
    #[cfg(unix)]
    pub const STOP: i32 = libc::SIGSTOP;
    #[cfg(unix)]
    pub const CONTINUE: i32 = libc::SIGCONT;

    #[cfg(not(unix))]
    pub const KILL: i32 = 0;
    #[cfg(not(unix))]
    pub const STOP: i32 = 0;
    #[cfg(not(unix))]
    pub const CONTINUE: i32 = 0;
}

#[cfg(unix)]
mod platform {
    pub fn signal_group(pgid: u32, signal: i32) {
        // SAFETY: the caller has rejected 0, 1 and ids whose conversion/negation is lossy.
        unsafe {
            libc::kill(-(pgid as i32), signal);
        }
    }
}

#[cfg(not(unix))]
mod platform {
    pub fn signal_group(_pgid: u32, _signal: i32) {}
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn registries_are_isolated_and_reap_removes_pid_reuse_hazard() {
        let reel = Registry::new();
        let remux = Registry::new();
        reel.register(Some(42));
        remux.register(Some(43));
        assert!(reel.contains(42));
        assert!(!reel.contains(43));
        reel.unregister(Some(42));
        assert!(!reel.contains(42));
        assert!(remux.contains(43));
    }

    #[test]
    fn dangerous_group_ids_are_never_registered() {
        let groups = Registry::new();
        for pgid in [0, 1, i32::MAX as u32 + 1, u32::MAX] {
            groups.register(Some(pgid));
        }
        assert!(groups.is_empty());
    }
}
