//! Per-key ordering for cache-producing work that is allowed to overlap.
//!
//! Fresh and ordinary work deliberately use separate flights: a fresh request must not join work that may
//! have read the cache it is bypassing. Their successful results still publish to the same cache key, though,
//! so the order in which the upstream work started must survive through the eventual write. This fence keeps
//! only keys with overlapping live producers. Reads never touch it, and the cache write itself is ordered
//! only against another writer for the same key.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

#[derive(Default)]
pub(crate) struct PublicationFence {
    next: AtomicU64,
    live: Mutex<HashMap<String, Arc<KeyState>>>,
}

#[derive(Default)]
struct KeyState {
    producers: AtomicUsize,
    published: Mutex<Option<Version>>,
}

/// Fresh work is authoritative over ordinary work admitted while it is still live, regardless of which
/// started first. Generation orders two producers with the same authority.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum Authority {
    Ordinary,
    Fresh,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct Version {
    authority: Authority,
    generation: u64,
}

/// One cache-producing operation, ordered by freshness authority and then by when it began.
pub(crate) struct Publication {
    fence: Arc<PublicationFence>,
    key: String,
    state: Arc<KeyState>,
    version: Version,
    released: bool,
}

impl PublicationFence {
    pub(crate) fn begin(self: &Arc<Self>, key: String, authority: Authority) -> Publication {
        let (generation, state) = {
            let mut live = lock(&self.live);
            // Allocate while holding the registry lock. Otherwise a producer could take an older generation,
            // be descheduled before attaching to the key, and arrive after a newer producer had already
            // published and removed the state — losing the very authority it needs to observe.
            let generation = self
                .next
                .try_update(Ordering::Relaxed, Ordering::Relaxed, |generation| generation.checked_add(1))
                .expect("cache publication generation space exhausted")
                + 1;
            let state = Arc::clone(live.entry(key.clone()).or_default());
            // Increment while the registry is locked, paired with release's decrement under the same lock. A
            // new producer can therefore never attach to a state another thread is in the act of removing.
            state.producers.fetch_add(1, Ordering::Relaxed);
            (generation, state)
        };
        Publication {
            fence: Arc::clone(self),
            key,
            state,
            version: Version { authority, generation },
            released: false,
        }
    }

    #[cfg(test)]
    pub(crate) fn live_keys(&self) -> usize {
        lock(&self.live).len()
    }
}

impl Publication {
    /// Publishes a successful result unless a higher-authority producer, or a later peer, already published.
    /// The closure runs while this key's publication lock is held, so the authority check and both cache tiers'
    /// write are one ordered action. A failed/cancelled producer simply drops its ticket and advances nothing.
    pub(crate) fn publish(mut self, write: impl FnOnce()) -> bool {
        let accepted = {
            let mut published = lock(&self.state.published);
            if published.is_some_and(|current| self.version < current) {
                false
            } else {
                write();
                *published = Some(self.version);
                true
            }
        };
        self.release();
        accepted
    }

    fn release(&mut self) {
        if self.released {
            return;
        }
        let mut live = lock(&self.fence.live);
        let was = self.state.producers.fetch_sub(1, Ordering::Relaxed);
        debug_assert!(was > 0, "publication released more than once");
        if was == 1 && live.get(&self.key).is_some_and(|state| Arc::ptr_eq(state, &self.state)) {
            live.remove(&self.key);
        }
        self.released = true;
    }
}

impl Drop for Publication {
    fn drop(&mut self) {
        self.release();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn newer_success_fences_an_older_writer_but_newer_failure_does_not() {
        let fence = Arc::new(PublicationFence::default());
        let ordinary = fence.begin("answer:k".to_owned(), Authority::Ordinary);
        let fresh = fence.begin("answer:k".to_owned(), Authority::Fresh);
        let value = Mutex::new(String::new());
        assert!(fresh.publish(|| *lock(&value) = "fresh".to_owned()));
        assert!(!ordinary.publish(|| *lock(&value) = "ordinary".to_owned()));
        assert_eq!(&*lock(&value), "fresh");
        assert_eq!(fence.live_keys(), 0);

        let ordinary = fence.begin("answer:k".to_owned(), Authority::Ordinary);
        let failed_fresh = fence.begin("answer:k".to_owned(), Authority::Fresh);
        drop(failed_fresh);
        assert!(ordinary.publish(|| *lock(&value) = "recovered".to_owned()));
        assert_eq!(&*lock(&value), "recovered");
        assert_eq!(fence.live_keys(), 0);
    }

    #[test]
    fn two_successful_generations_converge_on_the_later_start() {
        let fence = Arc::new(PublicationFence::default());
        let first_fresh = fence.begin("list:k".to_owned(), Authority::Fresh);
        let second_fresh = fence.begin("list:k".to_owned(), Authority::Fresh);
        let value = Mutex::new(String::new());

        assert!(first_fresh.publish(|| *lock(&value) = "first".to_owned()));
        assert!(second_fresh.publish(|| *lock(&value) = "second".to_owned()));
        assert_eq!(&*lock(&value), "second");
        assert_eq!(fence.live_keys(), 0);
    }

    #[test]
    fn unrelated_keys_do_not_share_the_publication_lock() {
        let fence = Arc::new(PublicationFence::default());
        let one = fence.begin("list:one".to_owned(), Authority::Ordinary);
        let two = fence.begin("list:two".to_owned(), Authority::Ordinary);
        let one_state = Arc::clone(&one.state);
        let _one_locked = lock(&one_state.published);
        assert!(two.publish(|| {}), "another key waited on this key's publication");
        drop(one);
    }

    #[test]
    fn fresh_authority_wins_even_when_the_ordinary_producer_started_later() {
        let fence = Arc::new(PublicationFence::default());
        let fresh = fence.begin("list:k".to_owned(), Authority::Fresh);
        let later_ordinary = fence.begin("list:k".to_owned(), Authority::Ordinary);
        let value = Mutex::new(String::new());

        assert!(fresh.publish(|| *lock(&value) = "fresh".to_owned()));
        assert!(!later_ordinary.publish(|| *lock(&value) = "ordinary".to_owned()));
        assert_eq!(&*lock(&value), "fresh");
        assert_eq!(fence.live_keys(), 0);
    }
}
