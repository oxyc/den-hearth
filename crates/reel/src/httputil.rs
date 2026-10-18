//! Reel's HTTP plumbing: the shared builders from `den-http`, plus the query-string and byte-range helpers only the
//! trailer routes need.

pub use den_http::percent_decode_path as percent_decode;
pub use den_http::{
    apply_conditional, error, etag_of, full, if_range_holds, json, not_found, timed, timing, Body,
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

/// A parsed `Range: bytes=start-end`, `bytes=start-` or `bytes=-length` against a known file size. A header
/// that is none of those — several ranges among them — is treated as no range at all, and the whole file goes
/// out, which RFC 9110 §14.2 allows.
pub enum RangeReq {
    /// Serve `[start, end]` inclusive (206).
    Satisfiable { start: u64, end: u64 },
    /// Range asks past EOF → 416.
    Unsatisfiable,
}

pub fn parse_range(header: Option<&str>, size: u64) -> Option<RangeReq> {
    let h = header?;
    let rest = h.trim().strip_prefix("bytes=")?;
    let (a, b) = rest.split_once('-')?;
    if a.trim().is_empty() {
        // The last `length` bytes: how a player reads an index kept at the end of a file.
        let length: u64 = b.trim().parse().ok()?;
        if length == 0 || size == 0 {
            return Some(RangeReq::Unsatisfiable);
        }
        return Some(RangeReq::Satisfiable { start: size.saturating_sub(length), end: size - 1 });
    }
    let start: u64 = a.trim().parse().ok()?;
    if !b.trim().is_empty() && b.trim().parse::<u64>().is_err() {
        return None; // trailing junk that isn't `\d*` → not a match, fall back to full body
    }
    let end = match b.trim().parse::<u64>() {
        Ok(e) => e.min(size.saturating_sub(1)),
        Err(_) => size.saturating_sub(1),
    };
    if start >= size || start > end {
        return Some(RangeReq::Unsatisfiable);
    }
    Some(RangeReq::Satisfiable { start, end })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_suffix_range_is_the_end_of_the_file() {
        let at = |h: &str, size| match parse_range(Some(h), size) {
            Some(RangeReq::Satisfiable { start, end }) => Some((start, end)),
            Some(RangeReq::Unsatisfiable) => None,
            None => panic!("{h} was read as no range at all"),
        };
        assert_eq!(at("bytes=-100", 1000), Some((900, 999)));
        assert_eq!(at("bytes=-5000", 1000), Some((0, 999)), "longer than the file: all of it");
        assert_eq!(at("bytes=-0", 1000), None, "none of it");
        assert_eq!(at("bytes=10-", 1000), Some((10, 999)));
        assert_eq!(at("bytes=10-19", 1000), Some((10, 19)));
        assert!(parse_range(Some("bytes=-"), 1000).is_none());
        assert!(parse_range(Some("bytes=0-1,5-6"), 1000).is_none(), "several ranges: the whole file");
    }
}
