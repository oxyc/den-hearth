//! Port of the cross-store bookkeeping in `internal/scout/stores.go` (lines 780-1560): cache keys for
//! resolve and torrent ids, add attempts, queued and held markers, the unknown-outcome rules, refusal
//! backoff, and the `StorePool` reporters built on them. Owned by the "Stores II" work package; the
//! TorBox store (`torbox.rs`) calls into it.
