//! Port of `internal/scout/playwarm.go`.

use std::time::Duration;

use crate::cache::CacheClass;
use crate::handler::{write_queued, Inner, Resp};
use crate::stores::{ResolveTarget, StoreStatus};

/// How long /play remembers answering "downloading" for a file, so the next poll skips the held step and
/// asks status straight away. Refreshed by every such answer, so it lasts as long as the polling does and
/// lapses soon after; the client polls every couple of seconds, so this is many polls of slack.
pub(crate) const PLAY_PENDING_TTL: Duration = Duration::from_secs(30);

/// playPendingKey is scoped like the link memo — the same accounts, the same file.
pub(crate) fn play_pending_key(memo_key: &str) -> String {
    format!("play:pending:{memo_key}")
}

impl Inner {
    /// warmResolveBudget is the slice of the resolve clock the held step may take. Derived from the handler's
    /// status budget on each call rather than once, so it follows a status budget a test has shortened — the trap
    /// escalatedStatusBudget records falling into.
    pub(crate) fn warm_resolve_budget(&self) -> Duration {
        self.status_budget() / 2
    }

    /// writePlayQueued is /play's 202, noting that this file is being waited on.
    pub(crate) fn write_play_queued(
        &self,
        pending_key: &str,
        rt: &ResolveTarget,
        rid: &str,
        dur: Duration,
        status: &StoreStatus,
    ) -> Resp {
        self.deps.cache.put_class(pending_key, "1", PLAY_PENDING_TTL, CacheClass::Volatile);
        write_queued(rt, rid, dur, status)
    }
}
