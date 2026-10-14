//! den-scout, ported from `internal/scout` in the Go repository. A skeleton: one module per Go source
//! file, the shared types the tests need, and no logic. See `PORTING.md`.
//!
//! den-hearth serves it (`crates/hearth`): `handler::new_handler` over `deps::build_deps`.

// Every module is a stub until its work package lands, so unused items are expected for now.
#![allow(dead_code, unused_variables)]

pub mod addbudget;
pub mod attributes;
pub mod availability;
pub mod cache;
pub mod cinemeta;
pub mod client;
pub mod config;
pub mod configure;
pub mod coverage;
pub mod decisionlog;
pub mod deps;
pub mod diskcache;
pub mod handler;
pub mod hostlimit;
pub mod id;
pub mod indexerconfig;
pub mod label;
pub mod linkcheck;
pub mod linkmemo;
pub mod loglimit;
pub mod manifest;
pub mod metrics;
pub mod play;
pub mod playwarm;
pub mod probe;
pub mod probe_avi;
pub mod probe_fanout;
pub mod probe_mp4;
pub mod rank;
pub mod reqlog;
pub mod scrape;
pub mod seal;
pub mod season;
pub mod sources;
pub mod stores;
pub mod ticket;
pub mod upstreampause;
pub mod util;
pub mod validate;

/// The release this process reports (manifest, startup line, `/metrics`).
pub fn release_version() -> &'static str {
    manifest::manifest_version()
}

#[cfg(test)]
mod tests;
