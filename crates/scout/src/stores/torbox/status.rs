//! TorBox status snapshot representation and stable queued answer.

use super::*;

impl TorBoxStore {
    pub(super) async fn status_for(&self, t: &ResolveTarget) -> ProviderStatus {
        let key = torbox_status_key(&self.account_identity(), &t.info_hash);
        if let Some(raw) = self.cache.as_ref().and_then(|c| c.get(&key)) {
            if let Ok(hit) = serde_json::from_str::<CachedStatus>(&raw) {
                metrics().torbox_status_avoided.fetch_add(1, Ordering::Relaxed);
                metrics().torbox_avoid("status_snapshot");
                return hit.into_status();
            }
        }
        let store = self.clone();
        let target = t.clone();
        let flight_key = key.clone();
        let _waiting = GaugeGuard::new(metrics(), Gauge::StatusWaiters);
        let runtime = crate::runtime::current_runtime();
        let status_slots = runtime.torbox.status_slots.clone();
        let (answer, shared) = runtime
            .torbox
            .status_flights
            .run(&key, move || async move {
                // Bounded: a status read that finds the slots full answers "could not find out" at once, and
                // does not remember it.
                let Ok(_slot) = status_slots.try_acquire_owned() else {
                    return CachedStatus::new(ProviderStatus::Unknown);
                };
                let _active = GaugeGuard::new(metrics(), Gauge::StatusActive);
                // Shared work cannot inherit one waiter's cancellation: another viewer may still need the
                // answer.
                let answer = tokio::time::timeout(SHARED_DEADLINE, store.status_uncached(&target))
                    .await
                    .unwrap_or(ProviderStatus::Unknown);
                let unknown = matches!(answer, ProviderStatus::Unknown);
                let ttl = if unknown { UNKNOWN_STATUS_TTL } else { STATUS_TTL };
                let shared = CachedStatus::new(answer);
                if !unknown {
                    if let (Some(cache), Ok(raw)) = (&store.cache, serde_json::to_string(&shared)) {
                        cache.put_class(&flight_key, &raw, ttl, CacheClass::Volatile);
                    }
                }
                shared
            })
            .await;
        if shared {
            metrics().torbox_status_avoided.fetch_add(1, Ordering::Relaxed);
            metrics().torbox_avoid("status_snapshot");
        }
        match answer {
            Some(shared) => shared.into_status(),
            // The shared read panicked: nothing is known.
            None => ProviderStatus::Unknown,
        }
    }

    /// Go `torBoxStore.statusAnswerUncached`.
    async fn status_uncached(&self, t: &ResolveTarget) -> ProviderStatus {
        let identity = self.account_identity();
        // A parked add, the warm case: Resolve already recorded the queued-id marker (from a success body
        // or, now, from addMagnet's DIFF_ISSUE branch), so this is a cache read, not an upstream call. Checked
        // before torrentID below, which only ever finds a TORRENT — asking it alone answered "nothing here"
        // for a release that was waiting in TorBox's own queue, never a torrent at all yet.
        if self.known_queued_id(&t.info_hash).is_some() {
            return ProviderStatus::Downloading(queued_status());
        }
        let (torrent_id, ok, authoritative) = self.torrent_id(&t.info_hash).await;
        if !ok {
            // A miss here is only trustworthy when the listing was actually read. torrentID declines to
            // remember one otherwise, and the same doubt has to reach the caller — taken from the lookup that
            // already happened rather than by asking again, which was a second full account fetch.
            if !authoritative {
                return ProviderStatus::Unknown;
            }
            // Not an active torrent — but mylist and TorBox's own queue are two different listings, and a
            // release parked in the queue (with no cached marker yet: a cold hash, or one whose marker's TTL
            // lapsed) answers mylist exactly like one TorBox never heard of at all. Without this, the probe
            // route read a genuinely queued download as "not queued" forever, which is what made it drop out
            // of Home's in-flight states once the fallback's earlier release gave up. Found here, the marker
            // is written so the next poll takes the cheap `knownQueuedID` path above instead.
            //
            // Gated by its own remembered miss, same as torrentMissKey above it: a release that is genuinely
            // nowhere — neither listing holds it — must not pay for the queue listing again on every poll of
            // a two-second cadence, which is exactly the cost torrentMissKey already exists to keep off this
            // path for the torrent listing.
            if live(self.cache(), &torbox_queued_miss_key(&identity, &t.info_hash)).is_some() {
                return ProviderStatus::NotFetching;
            }
            if let Ok((qid, found)) = self.find_queued_by_hash(&t.info_hash).await {
                if found {
                    self.cache().put_class(
                        &torbox_queued_key(&identity, &t.info_hash),
                        &qid.to_string(),
                        RESOLVE_CACHE_TTL,
                        CacheClass::Volatile,
                    );
                    return ProviderStatus::Downloading(queued_status());
                }
                self.cache().put_class(
                    &torbox_queued_miss_key(&identity, &t.info_hash),
                    "1",
                    TORRENT_MISS_TTL,
                    CacheClass::Volatile,
                );
            }
            return ProviderStatus::NotFetching;
        }
        let Ok(mut reply) =
            self.get(&format!("{}/torrents/mylist?id={torrent_id}&bypass_cache=true", self.api)).await
        else {
            return ProviderStatus::Unknown;
        };
        if reply.status() != 200 {
            return ProviderStatus::Unknown;
        }
        static SHAPE: Shape = Shape::Object(&[("success", &LEAF), ("data", &LEAF)]);
        // a body we could not read is not the account saying no
        let Some(obj) = reply.json(&SHAPE).await.and_then(envelope) else {
            return ProviderStatus::Unknown;
        };
        let Ok(success) = opt_bool(obj.get("success")) else {
            return ProviderStatus::Unknown;
        };
        // TorBox answers 200 with success:false / data:null for an id it no longer holds, and unmarshalling
        // null into a struct succeeds silently — so both have to be rejected explicitly.
        if success == Some(false) {
            return ProviderStatus::NotFetching;
        }
        // The ID is what makes an answer about OUR torrent. mylist is asked by id and should reply with one
        // entry, but it can reply with the account list — that is why the array fallback exists at all — and
        // this took arr[0] blind, the very mistake listFiles was hardened against. It has no id field even in
        // principle, so it could not check. A finished arr[0] made a live download read as ok=false, which
        // handlePlay answers 404 dead_link: the client blacklists a release that is downloading right now,
        // which is the single failure Status exists to prevent. A downloading arr[0] is quieter and no better
        // — the viewer watches another torrent's percentage, ETA and rate.
        //
        // An EXACT id wins; an id-less entry is only a fallback — the rule listFiles uses on this very payload.
        // First-of-either let an unlabelled entry ahead of ours answer for it, and here that is the reading
        // that decides "downloading" against "dead": a finished stranger at the head of the array made a live
        // download report nothing. Two readers of one shape must not disagree about it. An entry with no id is
        // taken at its word, since the single-entry answer to a by-id query need not repeat it; one that names
        // a DIFFERENT torrent never is.
        let Some(st) = pick_entry(obj.get("data"), torrent_id, StatusEntry::read) else {
            return ProviderStatus::NotFetching;
        };
        // Finished but Resolve still failed → not a wait we can promise anything about; reads as dead.
        if st.download_finished == Some(true) {
            return ProviderStatus::NotFetching;
        }
        // Nothing said about the download at all (an empty object, a deleted torrent) — not a wait either.
        if st.progress.is_none() && st.download_finished.is_none() {
            return ProviderStatus::NotFetching;
        }
        let mut out = StoreStatus::default();
        if let Some(p) = st.progress {
            out.progress = p;
        }
        if let Some(eta) = st.eta.filter(|eta| *eta > 0) {
            out.eta_seconds = Some(eta);
        }
        // Zero is meaningful here — it is the stall — so only a missing or negative figure is dropped.
        out.bytes_per_second = st.download_speed.filter(|s| *s >= 0);
        if let Some(state) = &st.download_state {
            out.state = fetch_state(state).to_owned();
        }
        out.seeds = st.seeds.filter(|n| *n >= 0);
        out.peers = st.peers.filter(|n| *n >= 0);
        ProviderStatus::Downloading(out)
    }
}

/// A status read and its three-valued answer, as the shared snapshot keeps them.
#[derive(Clone, Serialize, Deserialize)]
pub(super) struct CachedStatus {
    progress: f64,
    eta_seconds: Option<i64>,
    bytes_per_second: Option<i64>,
    state: String,
    seeds: Option<i64>,
    peers: Option<i64>,
    service: Option<String>,
    /// 0 no, 1 downloading, 2 unknown.
    answer: u8,
}

impl CachedStatus {
    pub(super) fn new(answer: ProviderStatus) -> Self {
        let (status, code) = match answer {
            ProviderStatus::NotFetching => (StoreStatus::default(), 0),
            ProviderStatus::Downloading(status) => (status, 1),
            ProviderStatus::Unknown => (StoreStatus::default(), 2),
        };
        Self {
            progress: status.progress,
            eta_seconds: status.eta_seconds,
            bytes_per_second: status.bytes_per_second,
            state: status.state,
            seeds: status.seeds,
            peers: status.peers,
            service: status.service.map(|service| service.as_str().to_owned()),
            answer: code,
        }
    }

    pub(super) fn into_status(self) -> ProviderStatus {
        let status = StoreStatus {
            progress: self.progress,
            eta_seconds: self.eta_seconds,
            bytes_per_second: self.bytes_per_second,
            state: self.state,
            seeds: self.seeds,
            peers: self.peers,
            service: self.service.as_deref().and_then(DebridService::parse),
        };
        match self.answer {
            0 => ProviderStatus::NotFetching,
            1 => ProviderStatus::Downloading(status),
            _ => ProviderStatus::Unknown,
        }
    }
}

pub(super) fn queued_status() -> StoreStatus {
    StoreStatus { state: FETCH_QUEUED.to_owned(), service: Some(DebridService::TorBox), ..Default::default() }
}
