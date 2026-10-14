//! Port of `internal/scout/sources.go`. Work package 5.

use std::net::IpAddr;

/// Go `maxSources`: how many household sources one config may name.
pub(crate) const MAX_SOURCES: usize = 3;
/// Go `maxSourceURL`: the longest source URL kept.
pub(crate) const MAX_SOURCE_URL: usize = 2048;
/// Go `errNotPublic`: the refusal a source connection gets for a non-public address. A reason constant.
pub(crate) const ERR_NOT_PUBLIC: &str = "household source: refusing a non-public address";

/// Go `normalizeSources`: cleans the household's configured source URLs, dropping unusable ones.
pub(crate) fn normalize_sources(_raw: &[String]) -> Vec<String> {
    todo!()
}

/// Go `publicAddr`: whether an address is one a source may be fetched from.
pub(crate) fn public_addr(_ip: IpAddr) -> bool {
    todo!()
}

/// Go `newSourceClient`: the HTTP client for household sources, whose dial refuses non-public addresses.
pub(crate) fn new_source_client() -> reqwest::Client {
    todo!()
}
