//! Subtitles' HTTP plumbing: the shared builders from `den-http`, plus the subtitle-specific bodies (SRT, WebVTT)
//! and the validators only they carry.

use bytes::Bytes;
use hyper::header::{
    HeaderMap, HeaderValue, ACCEPT_RANGES, CACHE_CONTROL, CONTENT_LENGTH, CONTENT_RANGE, CONTENT_TYPE, ETAG,
    IF_NONE_MATCH, IF_RANGE, RANGE,
};
use hyper::{Response, StatusCode};
use serde::Serialize;

pub use den_http::{
    add_timing, apply_conditional, degraded, etag_of, full, not_found, percent_decode, percent_decode_path,
    preflight, stable_hash, Body,
};
/// The header names the tests read a response by.
#[cfg(test)]
pub use den_http::{SERVER_TIMING, X_DEN_DEGRADED};

/// An error reply: `{"error":"<slug>"}`, the body shape every den addon uses, and always `no-store` —
/// a transient upstream failure must never be cached, or a cached 502 would wedge a subtitle until
/// the entry expired.
pub fn error(status: StatusCode, slug: &str) -> Response<Body> {
    json(status, &serde_json::json!({ "error": slug }), "no-store")
}

/// Tell the client how long to wait, in whole seconds rounded up so it never comes back a moment
/// early, and at least one.
pub fn retry_after(mut resp: Response<Body>, wait: std::time::Duration) -> Response<Body> {
    let secs = wait.as_secs() + u64::from(wait.subsec_nanos() > 0);
    resp.headers_mut().insert(hyper::header::RETRY_AFTER, HeaderValue::from(secs.max(1)));
    resp
}

/// JSON under `cache_control` (none when empty). A cacheable 200 carries an ETag; see `den_http::json`.
pub fn json<T: Serialize + ?Sized>(status: StatusCode, value: &T, cache_control: &str) -> Response<Body> {
    if cache_control.is_empty() {
        den_http::json(status, value, &[])
    } else {
        den_http::json(status, value, &[("cache-control", cache_control)])
    }
}

/// The caching directive of a settled subtitle: this file_id/sync variant is byte-stable forever, so
/// `immutable` and a year. `private` because it is served under an install's config segment, which
/// is that install's credentials — a shared cache keyed on the URL would be holding them.
pub const SETTLED_SRT: &str = "private, max-age=31536000, immutable";

/// A settled subtitle (see `SETTLED_SRT`).
pub fn srt(body: String) -> Response<Body> {
    let body = Bytes::from(body);
    let etag = etag_of(&body);
    srt_cached(body, SETTLED_SRT, &etag)
}

/// `srt` for a body that is already shared (a cache hit): the response holds a view of the cached
/// allocation instead of a copy. `etag` is that body's `etag_of`, which a cached body computes once
/// (`cache::Text::etag`) — hashing 157 KB byte by byte on every hit cost more than everything else.
pub fn srt_shared(body: Bytes, etag: &str) -> Response<Body> {
    srt_cached(body, SETTLED_SRT, etag)
}

/// `srt_provisional` for a shared body.
pub fn srt_provisional_shared(body: Bytes, etag: &str) -> Response<Body> {
    srt_cached(body, "private, no-cache", etag)
}

/// A subtitle we may yet serve differently — the requested alignment failed and this is the raw
/// fallback. `immutable` would pin the client to it for a year, so it is stored but revalidated on
/// every use; the ETag makes that revalidation a 304 while the body has not changed.
pub fn srt_provisional(body: String) -> Response<Body> {
    let body = Bytes::from(body);
    let etag = etag_of(&body);
    srt_cached(body, "private, no-cache", &etag)
}

fn srt_cached(body: Bytes, cache_control: &str, etag: &str) -> Response<Body> {
    Response::builder()
        .status(StatusCode::OK)
        .header(CONTENT_TYPE, "application/x-subrip; charset=utf-8")
        .header(CACHE_CONTROL, cache_control)
        .header(ETAG, etag)
        .body(full(body))
        .unwrap()
}

/// Answer one byte range of a subtitle body (RFC 9110 §14): `206` with `Content-Range` for `bytes=a-b`, `bytes=a-`
/// or the suffix `bytes=-n`, `416` with `Content-Range: bytes */<length>` for a range wholly past the end. Every
/// subtitle body says `Accept-Ranges: bytes`. A range whose `If-Range` no longer holds, a multi-range or malformed
/// header, and anything that is not a subtitle body pass through whole.
///
/// A player learns whether a host serves ranges from what it answers, and remembers it per host and port.
/// AetherEngine asked a subtitle here for `bytes=-65536`, got the whole body as a 200, and from then on took the
/// video's host and port — the same — to have no suffix ranges, so every play skipped its tail prefetch.
///
/// Applied at the router after `to_vtt`, so the range is of the bytes actually sent.
pub async fn ranged(resp: Response<Body>, req_headers: &HeaderMap) -> Response<Body> {
    use http_body_util::BodyExt;

    let is_subtitle = resp
        .headers()
        .get(CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .is_some_and(|v| v.starts_with("application/x-subrip") || v.starts_with("text/vtt"));
    if resp.status() != StatusCode::OK || !is_subtitle {
        return resp;
    }
    let (mut parts, body) = resp.into_parts();
    parts.headers.insert(ACCEPT_RANGES, HeaderValue::from_static("bytes"));
    let header = |name| req_headers.get(name).and_then(|v: &HeaderValue| v.to_str().ok());
    let etag = parts.headers.get(ETAG).and_then(|v| v.to_str().ok()).unwrap_or("");
    let still = header(IF_RANGE).is_none_or(|v| den_http::if_range_holds(v, etag, ""));
    let Some(range) = header(RANGE).filter(|_| still) else { return Response::from_parts(parts, body) };
    // The body is a `full` buffer, already in memory, as in `to_vtt`.
    let bytes = match body.collect().await {
        Ok(collected) => collected.to_bytes(),
        Err(_) => return error(StatusCode::INTERNAL_SERVER_ERROR, "subtitle_unavailable"),
    };
    let size = bytes.len() as u64;
    match den_http::parse_range(Some(range), size) {
        None => Response::from_parts(parts, full(bytes)),
        Some(den_http::RangeReq::Unsatisfiable) => Response::builder()
            .status(StatusCode::RANGE_NOT_SATISFIABLE)
            .header(CONTENT_RANGE, format!("bytes */{size}"))
            .header(ACCEPT_RANGES, "bytes")
            .header(CACHE_CONTROL, "no-store")
            .body(full(""))
            .unwrap(),
        Some(den_http::RangeReq::Satisfiable { start, end }) => {
            parts.status = StatusCode::PARTIAL_CONTENT;
            parts.headers.insert(
                CONTENT_RANGE,
                HeaderValue::from_str(&format!("bytes {start}-{end}/{size}"))
                    .expect("digits are a header value"),
            );
            parts.headers.remove(CONTENT_LENGTH);
            Response::from_parts(parts, full(bytes.slice(start as usize..=end as usize)))
        }
    }
}

/// A strong ETag naming WHAT a body is rather than hashing its bytes, for a response whose bytes
/// are fixed by its identity. It can be computed before the body exists, so a conditional request
/// is answered without fetching anything. The `-id` suffix is outside the hex alphabet `etag_of`
/// ends in, so the two kinds can never be equal.
pub fn identity_etag(identity: &str) -> String {
    format!("\"{:016x}-id\"", stable_hash(identity.as_bytes()))
}

/// Whether a request's `If-None-Match` matches `etag`.
pub fn if_none_match(req_headers: &HeaderMap, etag: &str) -> bool {
    req_headers
        .get(IF_NONE_MATCH)
        .and_then(|inm| inm.to_str().ok())
        .is_some_and(|inm| den_http::if_none_match_matches(inm, etag))
}

/// A bodiless `304 Not Modified` carrying the validator and the caching directive it revalidated.
pub fn not_modified(etag: &str, cache_control: &str) -> Response<Body> {
    Response::builder()
        .status(StatusCode::NOT_MODIFIED)
        .header(CACHE_CONTROL, cache_control)
        .header(ETAG, etag)
        .body(full(""))
        .unwrap()
}

/// The ETag of the WebVTT rendering of an SRT whose ETag is `srt_etag`.
///
/// Derived from the SRT one rather than from the converted bytes, so a conditional request can be
/// answered WITHOUT converting. Conversion is a full parse and re-serialize of the whole document,
/// and `apply_conditional` runs after the router — so every `If-None-Match` hit was paying for a body
/// it then threw away, on the one runtime thread. Derived, not invented: the SRT ETag already changes
/// whenever the bytes do.
pub fn vtt_etag(srt_etag: &str) -> String {
    /// Bumped whenever `serialize_vtt` changes what it emits. The validator is derived from the SRT
    /// ETag, which does not move when the CONVERTER moves — so without this, changing the rendering
    /// (as escaping the payload did) leaves every revalidating client and proxy on the old output.
    const RENDERING: u32 = 2;

    let mut seed = srt_etag.as_bytes().to_vec();
    seed.extend_from_slice(&RENDERING.to_le_bytes());
    format!("\"{:016x}-vtt\"", stable_hash(&seed))
}

/// Re-render a subtitle response as WebVTT.
///
/// Applied at the router, once, rather than threaded through the sync ladder as a format flag: every
/// path that can produce a subtitle — a cache hit, a fresh download, an alignment, a translation, the
/// provisional fallback — would otherwise need to carry it, and each is a place to get it wrong.
///
/// Anything that is not a subtitle body (an error, a 304) passes through untouched. The ETag is
/// recomputed because the bytes are genuinely different, and the caching directive is carried over
/// because whether the body is settled or provisional is not changed by re-rendering it.
pub async fn to_vtt(resp: Response<Body>, req_headers: &HeaderMap) -> Response<Body> {
    use http_body_util::BodyExt;

    let is_subtitle = resp
        .headers()
        .get(CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .is_some_and(|v| v.starts_with("application/x-subrip"));
    if resp.status() != StatusCode::OK || !is_subtitle {
        return resp;
    }
    let cache_control =
        resp.headers().get(CACHE_CONTROL).and_then(|v| v.to_str().ok()).unwrap_or("no-store").to_string();

    // See `vtt_etag` for why the validator is derived rather than hashed from the converted bytes.
    let derived = resp.headers().get(ETAG).and_then(|v| v.to_str().ok()).map(vtt_etag);
    if let Some(etag) = &derived {
        if if_none_match(req_headers, etag) {
            return not_modified(etag, &cache_control);
        }
    }
    // The body is a `full` buffer, already in memory, so this await resolves immediately and cannot stall the thread.
    let (mut parts, body) = resp.into_parts();
    let bytes = match body.collect().await {
        Ok(collected) => collected.to_bytes(),
        Err(_) => return error(StatusCode::INTERNAL_SERVER_ERROR, "subtitle_unavailable"),
    };
    let Ok(srt) = std::str::from_utf8(&bytes) else {
        return error(StatusCode::INTERNAL_SERVER_ERROR, "subtitle_not_utf8");
    };
    let vtt = crate::srt::serialize_vtt(&crate::srt::parse(srt));
    // The SRT response's own head, so everything else the handler said about this body — its caching
    // directive, `Server-Timing`, `X-Den-Degraded` — carries over; only the type and validator change.
    let etag = derived.unwrap_or_else(|| etag_of(vtt.as_bytes()));
    parts.headers.insert(CONTENT_TYPE, HeaderValue::from_static("text/vtt; charset=utf-8"));
    parts.headers.insert(ETAG, HeaderValue::from_str(&etag).unwrap());
    Response::from_parts(parts, full(vtt))
}

#[cfg(test)]
mod tests {
    use super::*;

    const SRT: &str = "1\n00:00:01,000 --> 00:00:02,000\n<i>Hello</i> & goodbye\n";

    async fn body_of(resp: Response<Body>) -> String {
        use http_body_util::BodyExt;
        let bytes = resp.into_body().collect().await.unwrap().to_bytes();
        String::from_utf8_lossy(&bytes).into_owned()
    }

    #[tokio::test]
    async fn vtt_conversion_rewrites_the_body_and_the_validator() {
        let out = to_vtt(srt(SRT.to_string()), &HeaderMap::new()).await;
        assert_eq!(out.status(), StatusCode::OK);
        assert_eq!(out.headers().get(CONTENT_TYPE).unwrap(), "text/vtt; charset=utf-8");
        // The caching directive carries over: re-rendering does not change whether the body is
        // settled or provisional.
        assert!(out.headers().get(CACHE_CONTROL).unwrap().to_str().unwrap().contains("immutable"));
        // A `.vtt` and a `.srt` of the same subtitle are different bytes and must not share a
        // validator, or a client that fetched one gets a 304 for the other.
        let srt_etag = srt(SRT.to_string()).headers().get(ETAG).unwrap().clone();
        let vtt_etag = out.headers().get(ETAG).unwrap().clone();
        assert_ne!(&vtt_etag, &srt_etag);

        // And the validator must depend on the RENDERING VERSION, not only on the SRT bytes. It is
        // derived from the SRT ETag, which does not move when the converter moves — so without the
        // version folded in, changing what `serialize_vtt` emits (as escaping the payload did) leaves
        // every revalidating client on the old output. Asserting the derivation is not the bare hash
        // of the SRT ETag is what pins the fold; a test comparing it to the SRT ETag alone passes
        // with the fold deleted, since the `-vtt` suffix already differs.
        let unversioned = format!("\"{:016x}-vtt\"", stable_hash(srt_etag.to_str().unwrap().as_bytes()));
        assert_ne!(
            vtt_etag.to_str().unwrap(),
            unversioned.as_str(),
            "the VTT validator ignores the rendering version"
        );

        let body = body_of(out).await;
        assert!(body.starts_with("WEBVTT"));
        assert!(body.contains("<i>Hello</i> &amp; goodbye"), "payload not rendered as VTT: {body:?}");
    }

    /// Re-rendering keeps what the handler said about the body — its timing and whether it is a
    /// fallback — and not only its caching directive.
    #[tokio::test]
    async fn vtt_conversion_keeps_the_handlers_headers() {
        let resp = degraded(add_timing(srt_provisional(SRT.to_string()), "sync;dur=5"), "sync_failed");
        let out = to_vtt(resp, &HeaderMap::new()).await;
        assert_eq!(out.headers().get(CONTENT_TYPE).unwrap(), "text/vtt; charset=utf-8");
        assert_eq!(out.headers().get(X_DEN_DEGRADED).unwrap(), "sync_failed");
        assert_eq!(out.headers().get(SERVER_TIMING).unwrap(), "sync;dur=5");
    }

    #[test]
    fn the_first_degraded_reason_stands() {
        let blank = || json(StatusCode::OK, &"", "no-store");
        let resp = degraded(degraded(blank(), "sync_failed"), "upstream_unavailable");
        assert_eq!(resp.headers().get(X_DEN_DEGRADED).unwrap(), "sync_failed");
        let resp = add_timing(add_timing(blank(), "download;dur=3"), "total;dur=4");
        assert_eq!(resp.headers().get(SERVER_TIMING).unwrap(), "download;dur=3, total;dur=4");
    }

    /// A provisional body stays provisional through the conversion — `immutable` on a stand-in would
    /// pin a client to an unaligned subtitle for a year.
    #[tokio::test]
    async fn a_provisional_body_is_still_provisional_as_vtt() {
        let out = to_vtt(srt_provisional(SRT.to_string()), &HeaderMap::new()).await;
        let cc = out.headers().get(CACHE_CONTROL).unwrap().to_str().unwrap().to_string();
        assert_eq!(cc, "private, no-cache", "a stand-in is revalidated on every use");
        assert!(out.headers().get(ETAG).is_some(), "and carries the validator that makes that a 304");
    }

    /// Subtitle bodies are served under an install's config segment, so no shared cache may keep
    /// them; a settled one is still a year and `immutable` for the client that fetched it.
    #[test]
    fn subtitle_bodies_are_private() {
        let cc = |r: Response<Body>| r.headers().get(CACHE_CONTROL).unwrap().to_str().unwrap().to_string();
        assert_eq!(cc(srt(SRT.to_string())), "private, max-age=31536000, immutable");
        assert_eq!(cc(srt_provisional(SRT.to_string())), "private, no-cache");
    }

    /// Clients and Cloudflare cache on these strings. The validator of a settled subtitle, of a cacheable JSON body
    /// is FNV-1a-64 of the bytes plus their length in hex, quoted — and `no-store` has none.
    #[test]
    fn the_validators_are_pinned() {
        let etag = |r: Response<Body>| r.headers().get(ETAG).map(|v| v.to_str().unwrap().to_string());
        assert_eq!(
            etag(srt("1\n00:00:01,000 --> 00:00:02,000\nHi\n".to_string())).as_deref(),
            Some("\"fdef2c0328292f7c-23\"")
        );
        assert_eq!(
            etag(json(StatusCode::OK, &serde_json::json!({"a": 1}), "public, max-age=60")).as_deref(),
            Some("\"9c3e82dd6fcae8b1-7\"")
        );
        assert_eq!(etag(json(StatusCode::OK, &serde_json::json!({"a": 1}), "no-store")), None);
        assert_eq!(etag(error(StatusCode::BAD_GATEWAY, "upstream_unavailable")), None);
    }

    /// An error body is `{"error":"<slug>"}` and never cached; the 404 is the one every addon sends.
    #[tokio::test]
    async fn errors_are_the_slug_body_and_no_store() {
        let err = error(StatusCode::BAD_GATEWAY, "upstream_unavailable");
        assert_eq!(err.headers().get(CACHE_CONTROL).unwrap(), "no-store");
        assert_eq!(body_of(err).await, r#"{"error":"upstream_unavailable"}"#);
        let missing = not_found();
        assert_eq!(missing.status(), StatusCode::NOT_FOUND);
        assert_eq!(missing.headers().get(CACHE_CONTROL).unwrap(), "no-store");
        assert_eq!(body_of(missing).await, r#"{"error":"not_found"}"#);
    }

    /// An identity validator can never collide with a body-hash one: a stand-in's ETag must not be
    /// mistaken for the settled body's by the early 304.
    #[test]
    fn an_identity_etag_is_never_a_body_etag() {
        let id = identity_etag("os:42");
        assert!(id.starts_with('"') && id.ends_with("-id\""), "{id}");
        assert_eq!(id, identity_etag("os:42"), "stable for one identity");
        assert_ne!(id, identity_etag("os:42:ref:7"));
        assert!(!etag_of(b"os:42").ends_with("-id\""));
    }

    /// A conditional request is answered WITHOUT converting. Conversion is a full parse and
    /// re-serialize of the document, and `apply_conditional` runs after the router — so every 304
    /// was paying for a body it then threw away.
    #[tokio::test]
    async fn a_conditional_vtt_request_is_answered_without_converting() {
        let first = to_vtt(srt(SRT.to_string()), &HeaderMap::new()).await;
        let etag = first.headers().get(ETAG).unwrap().clone();

        let mut headers = HeaderMap::new();
        headers.insert(IF_NONE_MATCH, etag.clone());
        let second = to_vtt(srt(SRT.to_string()), &headers).await;

        assert_eq!(second.status(), StatusCode::NOT_MODIFIED);
        assert_eq!(second.headers().get(ETAG).unwrap(), &etag);
        assert!(second.headers().get(CACHE_CONTROL).is_some(), "a 304 keeps its caching directive");
        assert!(body_of(second).await.is_empty(), "a 304 has no body");
    }

    /// Anything that is not a subtitle body passes through untouched — an error, or a body some
    /// other part of the router already shaped.
    #[tokio::test]
    async fn a_non_subtitle_response_passes_through() {
        let err = to_vtt(error(StatusCode::BAD_GATEWAY, "upstream_unavailable"), &HeaderMap::new()).await;
        assert_eq!(err.status(), StatusCode::BAD_GATEWAY);
        assert_eq!(body_of(err).await, r#"{"error":"upstream_unavailable"}"#);

        let js = to_vtt(json(StatusCode::OK, &"x", "no-store"), &HeaderMap::new()).await;
        assert_eq!(js.headers().get(CONTENT_TYPE).unwrap(), "application/json");
    }
}
