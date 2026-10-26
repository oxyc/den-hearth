//! den-scout: Hearth's stream-list and play service. den-hearth serves it (`crates/hearth`):
//! `handler::new_handler` over `deps::build_deps`.

pub mod addbudget;
pub mod attributes;
pub mod availability;
pub mod cache;
pub mod cinemeta;
pub mod client;
pub mod config;
mod continuity;
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
pub mod managed_hls;
mod managed_subtitles;
pub mod manifest;
pub mod metrics;
pub mod play;
pub mod playwarm;
pub mod probe;
pub mod probe_avi;
pub mod probe_fanout;
pub mod probe_mp4;
pub mod rank;
pub mod releaselog;
pub mod reqlog;
pub mod runtime;
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

#[cfg(feature = "bench")]
pub mod bench_api;

#[cfg(test)]
mod tests;
