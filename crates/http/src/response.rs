//! The one response body type, the JSON/HTML/error builders, and the headers every addon stamps the same way.

use std::time::Duration;

use bytes::Bytes;
use http::header::{
    HeaderMap, HeaderValue, ACCESS_CONTROL_ALLOW_HEADERS, ACCESS_CONTROL_ALLOW_METHODS,
    ACCESS_CONTROL_ALLOW_ORIGIN, ACCESS_CONTROL_EXPOSE_HEADERS, ACCESS_CONTROL_MAX_AGE, ETAG,
};
use http::{Response, StatusCode};
use http_body_util::combinators::BoxBody;
use http_body_util::{BodyExt, Full};
use serde::Serialize;

use crate::conditional::etag_of;

/// `Server-Timing`: which phases a response spent its time in, in milliseconds. Phase names only: never a title,
/// an install or a URL.
pub const SERVER_TIMING: &str = "server-timing";

/// `X-Den-Degraded: <reason>` marks an answer that is a fallback: a list left empty because the search failed, a
/// subtitle served unaligned because its sync failed. Absent on a normal answer. Reasons are the /health slugs, so
/// the app can read the header without knowing which addon sent it.
pub const X_DEN_DEGRADED: &str = "x-den-degraded";

/// The one body type every handler returns: bytes in, `io::Error` out (streamed file bodies can fail mid-flight,
/// full-buffer bodies never do).
pub type Body = BoxBody<Bytes, std::io::Error>;

/// A fully-buffered body from anything byte-ish.
pub fn full(data: impl Into<Bytes>) -> Body {
    Full::new(data.into()).map_err(|never| match never {}).boxed()
}

/// The `Cache-Control` value from a handler's extra-header slice, if any (case-insensitive key).
fn cache_control_of<'a>(extra: &'a [(&str, &str)]) -> Option<&'a str> {
    extra.iter().find(|(k, _)| k.eq_ignore_ascii_case("cache-control")).map(|(_, v)| *v)
}

/// Whether a response is a cacheable success that should carry a validator (ETag): a 200 with a caching directive
/// that isn't `no-store`. Errors and `no-store` bodies never get an ETag.
fn cacheable(status: StatusCode, cache_control: Option<&str>) -> bool {
    status == StatusCode::OK && cache_control.is_some_and(|cc| !cc.is_empty() && !cc.contains("no-store"))
}

/// JSON with an explicit Content-Length, plus any extra headers (e.g. Cache-Control). CORS is not set here: each
/// service stamps it on every response on the way out. Serialization can't fail for our own types, but stay total.
///
/// Errors are never cached (a transient 502/503/504 mustn't stick in a client or proxy cache): a non-success answer
/// with no `Cache-Control` of its own gets `no-store`. A cacheable 200 gets a strong ETag, so a conditional GET can
/// collapse to a 304.
pub fn json<T: Serialize + ?Sized>(status: StatusCode, value: &T, extra: &[(&str, &str)]) -> Response<Body> {
    let s = serde_json::to_vec(value).unwrap_or_else(|_| b"{}".to_vec());
    let mut b = Response::builder()
        .status(status)
        .header("content-type", "application/json")
        .header("content-length", s.len());
    let cc = cache_control_of(extra);
    for (k, v) in extra {
        b = b.header(*k, *v);
    }
    if !status.is_success() && cc.is_none() {
        b = b.header("cache-control", "no-store");
    }
    if cacheable(status, cc) {
        b = b.header(ETAG, etag_of(&s));
    }
    b.body(full(s)).unwrap()
}

/// HTML response (an embedded /configure page), plus any extra headers (e.g. Cache-Control).
pub fn html(status: StatusCode, body: &'static str, extra: &[(&str, &str)]) -> Response<Body> {
    let mut b = Response::builder()
        .status(status)
        .header("content-type", "text/html; charset=utf-8")
        .header("content-length", body.len());
    let cc = cache_control_of(extra);
    for (k, v) in extra {
        b = b.header(*k, *v);
    }
    if cacheable(status, cc) {
        b = b.header(ETAG, etag_of(body.as_bytes()));
    }
    b.body(full(body)).unwrap()
}

/// The one 404 every Den addon answers, for an unknown path and a refused `/metrics` alike, so a refusal cannot be
/// told apart from a route that does not exist.
pub fn not_found() -> Response<Body> {
    json(StatusCode::NOT_FOUND, &serde_json::json!({"error": "not_found"}), &[("cache-control", "no-store")])
}

/// A typed error body `{ "error": <code>, "detail": <text> }` — the shape every Den addon answers with (no-store
/// applied via [`json`]).
pub fn error(status: StatusCode, code: &str, detail: &str) -> Response<Body> {
    json(status, &serde_json::json!({ "error": code, "detail": detail }), &[])
}

/// One `Server-Timing` entry: a phase and how long it took, in milliseconds.
pub fn timing(name: &str, d: Duration) -> String {
    format!("{name};dur={:.1}", d.as_secs_f64() * 1000.0)
}

/// Name what a handler spent its time on in `Server-Timing`, replacing whatever was there. An empty list sends
/// nothing.
pub fn timed(mut resp: Response<Body>, phases: &str) -> Response<Body> {
    if !phases.is_empty() {
        if let Ok(v) = HeaderValue::from_str(phases) {
            resp.headers_mut().insert(SERVER_TIMING, v);
        }
    }
    resp
}

/// Append one `Server-Timing` entry to whatever the response already names.
pub fn add_timing<B>(mut resp: Response<B>, entry: &str) -> Response<B> {
    let value = match resp.headers().get(SERVER_TIMING).and_then(|v| v.to_str().ok()) {
        Some(prior) => format!("{prior}, {entry}"),
        None => entry.to_string(),
    };
    if let Ok(v) = HeaderValue::from_str(&value) {
        resp.headers_mut().insert(SERVER_TIMING, v);
    }
    resp
}

/// Close `Server-Timing` with `total`, on a response whose handler named its phases. `total` only where a handler
/// named some, so it is never the whole of the header.
pub fn with_total_timing<B>(resp: Response<B>, elapsed: Duration) -> Response<B> {
    if resp.headers().contains_key(SERVER_TIMING) {
        add_timing(resp, &timing("total", elapsed))
    } else {
        resp
    }
}

/// Mark a response as a fallback. The first reason stands: it is the one closest to the cause, and a later layer
/// adding its own would describe a consequence.
pub fn degraded<B>(mut resp: Response<B>, reason: &'static str) -> Response<B> {
    resp.headers_mut().entry(X_DEN_DEGRADED).or_insert(HeaderValue::from_static(reason));
    resp
}

/// Every reply readable from a browser: any origin, `expose` named as readable, and `Server-Timing` visible to
/// Resource Timing. Nothing here rides on a cookie — an install's credentials are in its path — so a wildcard
/// origin grants a page nothing it could not already fetch.
///
/// `expose` is each service's own list: a cross-origin fetch sees only the CORS-safelisted headers unless
/// Expose-Headers names more, so it must name what that service's clients read (`ETag` to send back as
/// `If-None-Match`, `Retry-After` to honour a cooldown, `Content-Range` for a player fetching parts).
pub fn cors_stamp(headers: &mut HeaderMap, expose: &'static str) {
    headers.insert(ACCESS_CONTROL_ALLOW_ORIGIN, HeaderValue::from_static("*"));
    headers.insert(ACCESS_CONTROL_EXPOSE_HEADERS, HeaderValue::from_static(expose));
    headers.insert("timing-allow-origin", HeaderValue::from_static("*"));
}

/// The CORS preflight answer for a read-only addon, for any path: `GET, HEAD, OPTIONS`, any request header, cached
/// for a day so a browser stops preflighting every request. The allow-origin is stamped on it with every other
/// reply, by [`cors_stamp`].
pub fn preflight() -> Response<Body> {
    Response::builder()
        .status(StatusCode::NO_CONTENT)
        .header(ACCESS_CONTROL_ALLOW_METHODS, "GET, HEAD, OPTIONS")
        .header(ACCESS_CONTROL_ALLOW_HEADERS, "*")
        .header(ACCESS_CONTROL_MAX_AGE, "86400")
        .body(full(""))
        .unwrap()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn header(resp: &Response<Body>, name: &str) -> Option<String> {
        resp.headers().get(name).map(|v| v.to_str().unwrap().to_string())
    }

    /// The ETag on a cacheable body is what clients and Cloudflare key revalidation on: a pinned string, over the
    /// exact bytes serde_json writes.
    #[test]
    fn a_cacheable_json_200_carries_the_pinned_etag() {
        let resp =
            json(StatusCode::OK, &serde_json::json!({"a": 1}), &[("cache-control", "public, max-age=60")]);
        assert_eq!(header(&resp, "etag").as_deref(), Some("\"9c3e82dd6fcae8b1-7\""));
        assert_eq!(header(&resp, "content-length").as_deref(), Some("7"));
        assert_eq!(header(&resp, "content-type").as_deref(), Some("application/json"));
        assert_eq!(header(&resp, "cache-control").as_deref(), Some("public, max-age=60"));
    }

    #[test]
    fn html_gets_the_same_etag_shape() {
        let resp = html(StatusCode::OK, "<p>hi</p>", &[("cache-control", "public, max-age=60")]);
        assert_eq!(header(&resp, "etag").as_deref(), Some("\"2af7677882c30339-9\""));
        assert_eq!(header(&resp, "content-type").as_deref(), Some("text/html; charset=utf-8"));
    }

    #[test]
    fn errors_and_no_store_bodies_have_no_etag_and_errors_are_never_cached() {
        let ok = json(StatusCode::OK, &serde_json::json!({"a": 1}), &[("cache-control", "no-store")]);
        assert!(ok.headers().get("etag").is_none());
        let bare = json(StatusCode::OK, &serde_json::json!({"a": 1}), &[]);
        assert!(bare.headers().get("etag").is_none() && bare.headers().get("cache-control").is_none());
        let err = error(StatusCode::BAD_GATEWAY, "upstream_unavailable", "x");
        assert_eq!(header(&err, "cache-control").as_deref(), Some("no-store"));
        assert!(err.headers().get("etag").is_none());
        let missing = not_found();
        assert_eq!(missing.status(), StatusCode::NOT_FOUND);
        assert_eq!(header(&missing, "cache-control").as_deref(), Some("no-store"));
    }

    #[tokio::test]
    async fn the_error_and_not_found_bodies_are_pinned() {
        let body = |resp: Response<Body>| async move {
            let bytes = resp.into_body().collect().await.unwrap().to_bytes();
            serde_json::from_slice::<serde_json::Value>(&bytes).unwrap()
        };
        assert_eq!(body(not_found()).await, serde_json::json!({"error": "not_found"}));
        assert_eq!(
            body(error(StatusCode::BAD_REQUEST, "bad_request", "Expected a u= parameter.")).await,
            serde_json::json!({"error": "bad_request", "detail": "Expected a u= parameter."})
        );
    }

    #[test]
    fn timing_is_tenths_of_a_millisecond_and_total_closes_a_named_header() {
        assert_eq!(timing("tmdb", Duration::from_micros(12_345)), "tmdb;dur=12.3");
        let named = timed(json(StatusCode::OK, &1, &[]), "resolve;dur=5.0");
        let closed = with_total_timing(named, Duration::from_millis(9));
        assert_eq!(header(&closed, "server-timing").as_deref(), Some("resolve;dur=5.0, total;dur=9.0"));
        let unnamed = with_total_timing(json(StatusCode::OK, &1, &[]), Duration::from_millis(9));
        assert!(unnamed.headers().get("server-timing").is_none(), "total is never the whole header");
    }

    #[test]
    fn the_first_degraded_reason_stands() {
        let resp = degraded(degraded(json(StatusCode::OK, &1, &[]), "sync_failed"), "upstream_unavailable");
        assert_eq!(header(&resp, X_DEN_DEGRADED).as_deref(), Some("sync_failed"));
    }

    #[test]
    fn cors_is_a_wildcard_with_the_services_own_expose_list() {
        let mut resp = json(StatusCode::OK, &1, &[]);
        cors_stamp(resp.headers_mut(), "ETag, Retry-After");
        assert_eq!(header(&resp, "access-control-allow-origin").as_deref(), Some("*"));
        assert_eq!(header(&resp, "access-control-expose-headers").as_deref(), Some("ETag, Retry-After"));
        assert_eq!(header(&resp, "timing-allow-origin").as_deref(), Some("*"));
        let pre = preflight();
        assert_eq!(pre.status(), StatusCode::NO_CONTENT);
        assert_eq!(header(&pre, "access-control-allow-methods").as_deref(), Some("GET, HEAD, OPTIONS"));
        assert_eq!(header(&pre, "access-control-allow-headers").as_deref(), Some("*"));
        assert_eq!(header(&pre, "access-control-max-age").as_deref(), Some("86400"));
    }
}
