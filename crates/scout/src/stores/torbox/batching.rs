//! State carried by one TorBox checkcached micro-batch.

use std::collections::{BTreeSet, HashMap};
use std::sync::{Arc, Mutex};

use tokio::sync::{oneshot, Notify};

use super::{
    lock, metrics, TorBoxStore, TorboxCheckResult, CACHE_BATCH, DEMAND_WINDOW, MAX_BATCHERS, MAX_DEMANDS,
    SHARED_DEADLINE,
};
use crate::util::{key_hash, scoped};

pub(super) struct Demand {
    pub(super) hashes: Vec<String>,
    pub(super) reply: oneshot::Sender<TorboxCheckResult>,
}

pub(super) struct Batcher {
    pub(super) store: TorBoxStore,
    pub(super) queue: Mutex<Vec<Demand>>,
    /// Signalled when the queue reaches its bound, ending the coalescing window early.
    pub(super) full: Notify,
}

pub(super) type SharedBatcher = Arc<Batcher>;

impl TorBoxStore {
    /// Go `torBoxStore.demandCheckCached`: asks for these hashes through the account's batcher, which folds the
    /// demands of every request in a short window into one checkcached call.
    pub(super) async fn demand_check_cached(&self, hashes: Vec<String>) -> TorboxCheckResult {
        let key = scoped(&key_hash(&self.account_identity()));
        let (reply, answer) = oneshot::channel();
        let mut demand = Some(Demand { hashes: hashes.clone(), reply });
        {
            let runtime = crate::runtime::current_runtime();
            let mut batchers = lock(&runtime.torbox.batchers);
            let batcher = match batchers.get(&key) {
                Some(b) => Some(b.clone()),
                None if batchers.len() < MAX_BATCHERS => {
                    let b = Arc::new(Batcher {
                        store: self.clone(),
                        queue: Mutex::new(Vec::new()),
                        full: Notify::new(),
                    });
                    batchers.insert(key.clone(), b.clone());
                    tokio::spawn(crate::runtime::scope(runtime.clone(), b.clone().run(key)));
                    Some(b)
                }
                None => None,
            };
            if let (Some(b), Some(d)) = (batcher, demand.take()) {
                // Every sender holds the registry lock while enqueueing, so the coordinator draining under
                // it closes the race with a sender that found the batcher just as its timer fired.
                if let Err(d) = b.try_push(d) {
                    demand = Some(d);
                }
            }
        }
        if demand.is_none() {
            return answer.await.unwrap_or_default();
        }
        // No batcher to join, or its queue was full: ask directly, still bounded by the outbound limiter.
        tokio::time::timeout(SHARED_DEADLINE, self.fetch_cached(&hashes)).await.unwrap_or_default()
    }
}

// --- the checkcached batcher ---

impl Batcher {
    /// Enqueues a demand if there is room (Go's non-blocking channel send).
    pub(super) fn try_push(&self, d: Demand) -> Result<(), Demand> {
        let mut queue = lock(&self.queue);
        if queue.len() >= MAX_DEMANDS {
            return Err(d);
        }
        queue.push(d);
        if queue.len() == MAX_DEMANDS {
            self.full.notify_one();
        }
        Ok(())
    }

    /// Go `torboxCheckBatcher.run`.
    pub(super) async fn run(self: Arc<Self>, key: String) {
        // If this task ends any other way than by answering (a panic), the batcher is unpublished and its
        // demands dropped, so their callers stop waiting instead of queueing behind a coordinator that is gone.
        let _retire = Retire { key: key.clone(), batcher: self.clone() };
        let _ = tokio::time::timeout(DEMAND_WINDOW, self.full.notified()).await;
        // A sender may have found this batcher just as the timer fired. Every sender holds the registry lock
        // while enqueueing, so draining under it closes that race before the coordinator is unpublished.
        let demands = {
            let runtime = crate::runtime::current_runtime();
            let mut batchers = lock(&runtime.torbox.batchers);
            if batchers.get(&key).is_some_and(|b| Arc::ptr_eq(b, &self)) {
                batchers.remove(&key);
            }
            std::mem::take(&mut *lock(&self.queue))
        };

        let union: BTreeSet<&String> = demands.iter().flat_map(|d| d.hashes.iter()).collect();
        let all: Vec<String> = union.into_iter().cloned().collect();
        // Every call the demands would have made on their own, less the ones this pass makes, was saved by folding
        // them together.
        let avoided = demands.len().saturating_sub(all.len().div_ceil(CACHE_BATCH));
        for _ in 0..avoided {
            metrics().torbox_avoid("checkcached_batch");
        }
        let deadline = tokio::time::Instant::now() + SHARED_DEADLINE;
        let mut answers: HashMap<String, bool> = HashMap::with_capacity(all.len());
        for batch in all.chunks(CACHE_BATCH) {
            let Ok(answer) = tokio::time::timeout_at(deadline, self.store.fetch_cached(batch)).await else {
                continue;
            };
            if !answer.ok {
                continue;
            }
            answers.extend(answer.cached);
        }
        for demand in demands {
            let mut answer = HashMap::with_capacity(demand.hashes.len());
            let mut ok = true;
            for hash in &demand.hashes {
                match answers.get(hash) {
                    Some(cached) => {
                        answer.insert(hash.clone(), *cached);
                    }
                    None => ok = false,
                }
            }
            let _ = demand.reply.send(TorboxCheckResult { cached: answer, ok });
        }
    }
}

/// Unpublishes a batcher and drops what it still holds when its coordinator ends.
struct Retire {
    key: String,
    batcher: Arc<Batcher>,
}

impl Drop for Retire {
    fn drop(&mut self) {
        let runtime = crate::runtime::current_runtime();
        let mut batchers = lock(&runtime.torbox.batchers);
        if batchers.get(&self.key).is_some_and(|b| Arc::ptr_eq(b, &self.batcher)) {
            batchers.remove(&self.key);
        }
        lock(&self.batcher.queue).clear();
    }
}
