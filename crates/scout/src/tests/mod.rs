//! The Go test files of `internal/scout`, one Rust module each, declared here so that no porter has to edit
//! this file. Each module is an empty shell with a header naming its Go file; see PORTING.md for who ports
//! which. They live inside the crate (not under `tests/`) because the Go tests reach unexported items.
//!
//! Two lints are off for tests only: the Go table tests port as tuple arrays, which `type_complexity`
//! flags, and tests that share process-wide state (log limiter, minted-config map) serialise on a std
//! mutex held across awaits, which is the intent on a single-threaded test runtime.
#![allow(clippy::type_complexity, clippy::await_holding_lock)]

mod account;
mod addbudget;
mod answercache;
mod attributes_web;
mod availability;
mod cancel;
mod client;
mod core;
mod coverage;
mod decisionlog;
mod deps;
mod diskcache;
mod extra;
mod go_wire_vectors;
mod handler;
mod hostlimit;
mod indexerconfig;
mod linkcheck;
mod linkmemo;
mod loglimit;
mod metrics;
mod mock;
mod playreserve;
mod playwarm;
mod probe;
mod probe_containers;
mod probe_fanout;
mod probe_mp4;
mod queued;
mod rank;
mod rank_show;
mod redirect;
mod refusal;
mod remux;
mod reqlog;
mod scrape;
mod seal;
mod season_bench;
mod shape;
mod sources;
mod stores;
mod stores_failure;
mod stores_fidelity;
mod stores_pick;
mod stores_preference;
mod ticket;
mod token_leak;
mod upstreampause;
mod validate;
mod version;
mod webready;
