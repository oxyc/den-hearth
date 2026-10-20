//! Reel's HTTP plumbing: the shared builders and byte-range parser from `den-http`, plus the query-string helper
//! only the trailer routes need.

pub use den_http::percent_decode_path as percent_decode;
pub use den_http::{
    apply_conditional, error, etag_of, full, if_range_holds, json, not_found, parse_range, timed, timing,
    Body, RangeReq,
};

/// The `Vary` for a body built from [`crate::addon::self_base`]. Its play URLs come from the
/// forwarded host and scheme, or from `Host` when nothing is forwarded, so all three are part of what
/// the body says — and a cache that ignored them could hand one requester's answer, pointing at an
/// authority they chose, to everyone else. Only those bodies carry it: on one that embeds no host it
/// would split a cache for nothing.
pub const VARY_SELF_BASE: (&str, &str) = ("vary", "X-Forwarded-Host, X-Forwarded-Proto, Host");

/// Look up a query-string parameter without pulling in a URL parser. Returns the decoded value of
/// the first occurrence of `key`.
pub fn query_param(query: &str, key: &str) -> Option<String> {
    query.split('&').find_map(|pair| {
        let (k, v) = pair.split_once('=').unwrap_or((pair, ""));
        (percent_decode(k) == key).then(|| percent_decode(v))
    })
}
