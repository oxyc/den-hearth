//! Validators: the ETag every addon computes the same way, and the conditional-GET rules that turn a matching one
//! into a `304`.

use http::header::{
    HeaderMap, HeaderValue, CACHE_CONTROL, ETAG, IF_MODIFIED_SINCE, IF_NONE_MATCH, LAST_MODIFIED, RANGE,
};
use http::{Method, Response, StatusCode};

use crate::response::{full, Body};

/// FNV-1a-64: a FIXED, non-crypto hash — deterministic across restarts AND toolchain versions.
///
/// Anything whose value outlives the process must use this rather than std's `DefaultHasher`, whose algorithm std
/// may change between compiler releases. A toolchain bump would otherwise silently rename every hashed cache file,
/// reset every install's daily allowance, move every failure marker, and invalidate every ETag a client or proxy
/// holds — all at once, and invisibly.
pub fn stable_hash(bytes: &[u8]) -> u64 {
    let mut h: u64 = 0xcbf2_9ce4_8422_2325; // FNV offset basis
    for &b in bytes {
        h ^= b as u64;
        h = h.wrapping_mul(0x0000_0100_0000_01b3); // FNV prime
    }
    h
}

/// A strong, quoted ETag derived from the response body: `"<16 hex of FNV-1a-64>-<length in hex>"`. An ETag only
/// needs to change when the bytes change, not resist an adversary; the length is folded in as a cheap extra guard
/// against collisions. The string is what clients and the CDN cache on, so its shape is pinned by a test.
pub fn etag_of(bytes: &[u8]) -> String {
    format!("\"{:016x}-{:x}\"", stable_hash(bytes), bytes.len())
}

/// RFC 9110 `If-None-Match`: `*` matches anything; otherwise any entry in the comma-separated list that equals the
/// ETag matches. Our ETags are strong, but the comparison is weak (§13.1.2): the `W/` prefix is stripped from both
/// sides, so a proxy that weakened it (Cloudflare does, when it compresses) still gets its 304.
pub fn if_none_match_matches(if_none_match: &str, etag: &str) -> bool {
    let strong = |tag: &str| tag.trim().trim_start_matches("W/").to_owned();
    let etag = strong(etag);
    if_none_match.split(',').any(|tag| tag.trim() == "*" || strong(tag) == etag)
}

/// Honor a conditional GET/HEAD: if the request's `If-None-Match` matches the response's `ETag` — or, when it sends
/// none, its `If-Modified-Since` is no earlier than the response's `Last-Modified` — collapse to a `304 Not
/// Modified` that keeps the validators + `Cache-Control` and drops the body. A HEAD answer, 304 or not, carries no
/// body (RFC 9110 §9.3.2) but every header a GET would have returned. A no-op for unsafe methods, responses without
/// an ETag (errors, `no-store`), or a non-matching request.
pub fn apply_conditional(method: &Method, req_headers: &HeaderMap, resp: Response<Body>) -> Response<Body> {
    if !matches!(*method, Method::GET | Method::HEAD) {
        return resp;
    }
    // Never collapse a partial/non-200 or a `Range` request to a bare `304`: a range request must get its bytes,
    // not an empty body — a 304 there silently breaks player seeks. Such responses still get HEAD-stripped. A plain
    // GET of a video DOES collapse: its ETag is stable for the cached file, and a revalidation that re-sent the
    // whole trailer was tens of megabytes for nothing.
    if resp.status() != StatusCode::OK || req_headers.contains_key(RANGE) {
        return head_stripped(method, resp);
    }
    let Some(etag) = resp.headers().get(ETAG) else {
        return head_stripped(method, resp);
    };
    // RFC 9110 §13.2.2: If-Modified-Since is only consulted when If-None-Match is absent.
    let matched = match req_headers.get(IF_NONE_MATCH) {
        Some(inm) => match (inm.to_str(), etag.to_str()) {
            (Ok(inm), Ok(etag)) => if_none_match_matches(inm, etag),
            _ => false,
        },
        None => not_modified_since(req_headers.get(IF_MODIFIED_SINCE), resp.headers().get(LAST_MODIFIED)),
    };
    if !matched {
        return head_stripped(method, resp);
    }
    let mut b = Response::builder().status(StatusCode::NOT_MODIFIED);
    let headers = b.headers_mut().expect("fresh builder has headers");
    for name in [ETAG, LAST_MODIFIED] {
        if let Some(v) = resp.headers().get(&name) {
            headers.insert(name, v.clone());
        }
    }
    if let Some(v) = resp.headers().get(CACHE_CONTROL) {
        headers.insert(CACHE_CONTROL, v.clone());
    }
    // RFC 9110 §15.4.5: a 304 carries the Vary the 200 would have. Caches keep the stored one, but it is the
    // header this response exists to be correct about. Every line of it: a body can vary on a client report and on
    // the host both, sent as two.
    for v in resp.headers().get_all("vary") {
        headers.append("vary", v.clone());
    }
    // What the handler did, and whether the answer is a fallback, describe THIS response; the client's stored copy
    // can say neither. The same body can be fresh one hour and a stand-in the next, so the ETag alone would hide
    // the change.
    for name in ["server-timing", "x-den-degraded"] {
        if let Some(v) = resp.headers().get(name) {
            headers.insert(name, v.clone());
        }
    }
    b.body(full("")).unwrap()
}

/// A HEAD response must not carry a body (RFC 9110 §9.3.2) — drop it while keeping every header (including the
/// `Content-Length` a GET would have returned). A no-op for GET.
fn head_stripped(method: &Method, resp: Response<Body>) -> Response<Body> {
    if *method == Method::HEAD {
        let (parts, _body) = resp.into_parts();
        return Response::from_parts(parts, full(""));
    }
    resp
}

/// RFC 9110 `If-Modified-Since`: unchanged when the response's `Last-Modified` is no later than the date the client
/// holds. Both are whole seconds, so comparing the parsed dates is exact. False when either is missing or
/// unreadable, which serves the body.
fn not_modified_since(since: Option<&HeaderValue>, last_modified: Option<&HeaderValue>) -> bool {
    let date = |v: Option<&HeaderValue>| v?.to_str().ok().and_then(|s| httpdate::parse_http_date(s).ok());
    matches!((date(since), date(last_modified)), (Some(since), Some(modified)) if modified <= since)
}

/// RFC 9110 §13.1.5 `If-Range`: whether a range still applies to the representation being served. An entity tag
/// must match strongly — a weak one never does — and a date must equal `Last-Modified`. When it does not hold, the
/// caller serves the whole representation instead of a part of it.
pub fn if_range_holds(if_range: &str, etag: &str, last_modified: &str) -> bool {
    let if_range = if_range.trim();
    if if_range.starts_with('"') || if_range.starts_with("W/") {
        return if_range == etag;
    }
    match (httpdate::parse_http_date(if_range), httpdate::parse_http_date(last_modified)) {
        (Ok(asked), Ok(modified)) => asked == modified,
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The ETag is what clients and Cloudflare cache on. Reel and Subtitles each formatted it, identically; this
    /// pins the shape so a refactor cannot move every cached copy at once.
    #[test]
    fn the_etag_string_is_pinned() {
        assert_eq!(etag_of(b""), "\"cbf29ce484222325-0\"");
        assert_eq!(etag_of(b"a"), "\"af63dc4c8601ec8c-1\"");
        assert_eq!(etag_of(b"foobar"), "\"85944171f73967e8-6\"");
        // Length is in hex.
        assert!(etag_of(&[7u8; 255]).ends_with("-ff\""));
    }

    #[test]
    fn the_hash_is_fnv_1a_64() {
        // Published FNV-1a 64-bit test vectors.
        assert_eq!(stable_hash(b""), 0xcbf2_9ce4_8422_2325);
        assert_eq!(stable_hash(b"a"), 0xaf63_dc4c_8601_ec8c);
        assert_eq!(stable_hash(b"foobar"), 0x8594_4171_f739_67e8);
    }

    #[test]
    fn if_none_match_compares_weakly_over_a_list() {
        assert!(if_none_match_matches("\"x\"", "\"x\""));
        assert!(if_none_match_matches("W/\"x\"", "\"x\""));
        assert!(if_none_match_matches("\"x\"", "W/\"x\""));
        assert!(if_none_match_matches("\"a\" , W/\"x\" ,\"b\"", "\"x\""), "a list with spaces");
        assert!(if_none_match_matches("*", "\"x\""));
        assert!(if_none_match_matches(" * ", "\"x\""));
        assert!(!if_none_match_matches("\"y\"", "\"x\""));
        assert!(!if_none_match_matches("W/\"y\", \"z\"", "\"x\""));
        assert!(!if_none_match_matches("", "\"x\""));
    }

    fn ok(etag: &str) -> Response<Body> {
        Response::builder()
            .status(StatusCode::OK)
            .header(ETAG, etag)
            .header(CACHE_CONTROL, "public")
            .body(full("x"))
            .unwrap()
    }

    fn inm(value: &'static str) -> HeaderMap {
        let mut h = HeaderMap::new();
        h.insert(IF_NONE_MATCH, HeaderValue::from_static(value));
        h
    }

    #[test]
    fn a_304_keeps_every_vary_the_200_had() {
        let resp = Response::builder()
            .status(StatusCode::OK)
            .header(ETAG, "\"a\"")
            .header("vary", "X-Den-Playable")
            .header("vary", "Host")
            .body(full("x"))
            .unwrap();
        let out = apply_conditional(&Method::GET, &inm("\"a\""), resp);
        assert_eq!(out.status(), StatusCode::NOT_MODIFIED);
        let vary: Vec<&str> = out.headers().get_all("vary").iter().map(|v| v.to_str().unwrap()).collect();
        assert_eq!(vary, ["X-Den-Playable", "Host"]);
    }

    #[test]
    fn a_304_keeps_the_handlers_timing_and_degraded_marker() {
        let mut resp = ok("\"a\"");
        resp.headers_mut().insert("server-timing", HeaderValue::from_static("sync;dur=5"));
        resp.headers_mut().insert("x-den-degraded", HeaderValue::from_static("sync_failed"));
        let out = apply_conditional(&Method::GET, &inm("\"a\""), resp);
        assert_eq!(out.status(), StatusCode::NOT_MODIFIED);
        assert_eq!(out.headers().get("server-timing").unwrap(), "sync;dur=5");
        assert_eq!(out.headers().get("x-den-degraded").unwrap(), "sync_failed");
        assert_eq!(out.headers().get(CACHE_CONTROL).unwrap(), "public");
    }

    #[test]
    fn a_range_request_is_never_collapsed_and_a_head_never_has_a_body() {
        let mut ranged = inm("\"a\"");
        ranged.insert(RANGE, HeaderValue::from_static("bytes=0-0"));
        assert_eq!(apply_conditional(&Method::GET, &ranged, ok("\"a\"")).status(), StatusCode::OK);
        let head = apply_conditional(&Method::HEAD, &HeaderMap::new(), ok("\"a\""));
        assert_eq!(head.status(), StatusCode::OK);
        assert!(head.headers().get(ETAG).is_some(), "a HEAD keeps the headers a GET would send");
        assert_eq!(apply_conditional(&Method::POST, &inm("\"a\""), ok("\"a\"")).status(), StatusCode::OK);
    }

    #[test]
    fn if_range_wants_a_strong_match_or_the_same_date() {
        let date = "Tue, 15 Nov 1994 08:12:31 GMT";
        assert!(if_range_holds("\"a\"", "\"a\"", date));
        assert!(!if_range_holds("W/\"a\"", "\"a\"", date), "a weak tag never holds");
        assert!(if_range_holds(date, "\"a\"", date));
        assert!(!if_range_holds("Wed, 16 Nov 1994 08:12:31 GMT", "\"a\"", date));
    }
}
