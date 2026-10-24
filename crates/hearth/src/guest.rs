//! Edge-authenticated guest Hearth overlay.
//!
//! Edge owns grant authentication and supplies only config segments from that grant's record. Hearth still opens
//! every bearer and owns the effective service choice: a personal Scout may prepare direct media, but Remux always
//! receives the host Scout; personal Subtitles override the host; Reel is always the host's.

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use hyper::header::CONTENT_TYPE;
use hyper::header::{
    HeaderValue, ACCEPT_ENCODING, CACHE_CONTROL, CONTENT_ENCODING, CONTENT_LENGTH, ETAG, LAST_MODIFIED, VARY,
};
use hyper::{Method, Request, Response, StatusCode, Uri};
use serde::Deserialize;
use serde_json::Value;

use crate::{den, playback, Body, Hearth};

const MAX_INPUT: usize = 32 * 1024;
const MAX_OUTPUT: usize = 8 * 1024 * 1024;
const TIMEOUT: Duration = Duration::from_secs(15);

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Input {
    personal: Option<String>,
    #[serde(default)]
    host: Host,
    request: Operation,
}

#[derive(Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct Host {
    den: Option<String>,
    scout: Option<String>,
    subs: Option<String>,
    reel: Option<String>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Operation {
    method: String,
    path: String,
    query: Option<String>,
    body: Option<Value>,
}

struct Opened {
    personal: Option<(String, den::Install)>,
    host: OpenedHost,
}

#[derive(Default)]
struct OpenedHost {
    den: Option<(String, den::Install)>,
    scout: Option<String>,
    subs: Option<String>,
    reel: Option<String>,
}

struct PlaybackSources<'a> {
    direct: Option<&'a str>,
    remux_scout: Option<String>,
    subtitles: Option<String>,
}

pub async fn handle<B>(hearth: &Arc<Hearth>, req: Request<B>) -> Response<Body>
where
    B: hyper::body::Body<Data = Bytes> + Send + 'static,
    B::Error: Into<Box<dyn std::error::Error + Send + Sync>>,
{
    let (parts, body) = req.into_parts();
    let owner = parts.headers.get("x-den-owner").and_then(|value| value.to_str().ok());
    if !playback::trusted_edge(hearth, &parts) || !owner.is_some_and(den_remux::is_grant_owner) {
        return den_http::not_found();
    }
    if parts.method != Method::POST || !json_content_type(&parts.headers) {
        return refused(StatusCode::BAD_REQUEST);
    }
    let bytes =
        match tokio::time::timeout(TIMEOUT, http_body_util::Limited::new(body, MAX_INPUT).collect()).await {
            Ok(Ok(body)) => body.to_bytes(),
            _ => return refused(StatusCode::BAD_REQUEST),
        };
    let Ok(input) = serde_json::from_slice::<Input>(&bytes) else { return refused(StatusCode::BAD_REQUEST) };
    let Some(opened) = open(hearth, input.personal, input.host) else {
        return refused(StatusCode::BAD_REQUEST);
    };
    let Some(mut request) = operation_request(&parts.headers, input.request) else {
        return refused(StatusCode::BAD_REQUEST);
    };
    // Edge proves managed playback on the outer RPC. A browser cannot smuggle this internal marker in its body.
    request.headers_mut().insert("x-den-managed-playback", HeaderValue::from_static("1"));
    request.headers_mut().insert(
        "x-den-edge-secret",
        parts.headers.get("x-den-edge-secret").expect("trusted Edge presented a secret").clone(),
    );
    request.headers_mut().insert("x-den-owner", parts.headers["x-den-owner"].clone());
    // The overlay rewrites JSON URLs, so it must receive identity bytes rather than a compressed
    // representation whose validators and encoding no longer describe the answer it returns.
    request.headers_mut().insert(ACCEPT_ENCODING, HeaderValue::from_static("identity"));
    if let Some(peer) = parts.extensions.get::<den_remux::Peer>() {
        request.extensions_mut().insert(*peer);
    }

    let path = request.uri().path().to_string();
    let response = if path == "/manifest.json" {
        manifest(&opened, request.method())
    } else if path == "/playback" {
        let sources = playback_sources(&opened);
        if sources.direct.is_none() && sources.remux_scout.is_none() {
            return refused(StatusCode::BAD_REQUEST);
        }
        playback::handle_guest(
            hearth,
            sources.direct,
            opened.personal.as_ref().map(|(segment, _)| segment.as_str()),
            sources.remux_scout.as_deref(),
            sources.subtitles.as_deref(),
            request,
        )
        .await
    } else if let crate::den::Route::Sidecar(token, track) = crate::den::route(&path) {
        playback::sidecar(hearth, &token, track, request).await
    } else {
        resource(hearth, &opened, request).await
    };
    scrub(response, owner.expect("checked owner"), &opened).await
}

fn playback_sources(opened: &Opened) -> PlaybackSources<'_> {
    let direct = opened
        .personal
        .as_ref()
        .filter(|(_, install)| install.members.contains(&"scout"))
        .map(|(segment, _)| segment.as_str());
    let personal_subs = opened
        .personal
        .as_ref()
        .filter(|(_, install)| install.members.contains(&"subs"))
        .map(|(segment, _)| format!("{}/{segment}", playback::DEN_ADDON_BASE));
    PlaybackSources {
        direct,
        remux_scout: opened
            .host
            .den
            .as_ref()
            .filter(|(_, install)| install.members.contains(&"scout"))
            .map(|(segment, _)| format!("{}/{segment}", playback::DEN_ADDON_BASE))
            .or_else(|| {
                opened
                    .host
                    .scout
                    .as_deref()
                    .map(|segment| format!("{}/{segment}", playback::LEGACY_SCOUT_BASE))
            }),
        subtitles: personal_subs.or_else(|| {
            opened
                .host
                .den
                .as_ref()
                .filter(|(_, install)| install.members.contains(&"subs"))
                .map(|(segment, _)| format!("{}/{segment}", playback::DEN_ADDON_BASE))
                .or_else(|| {
                    opened
                        .host
                        .subs
                        .as_deref()
                        .map(|segment| format!("{}/{segment}", playback::LEGACY_SUBS_BASE))
                })
        }),
    }
}

fn json_content_type(headers: &hyper::HeaderMap) -> bool {
    headers.get(CONTENT_TYPE).and_then(|value| value.to_str().ok()).is_some_and(|value| {
        value.split(';').next().is_some_and(|mime| mime.trim().eq_ignore_ascii_case("application/json"))
    })
}

fn open(hearth: &Hearth, personal: Option<String>, host: Host) -> Option<Opened> {
    let personal = match personal {
        Some(segment) => {
            let install = hearth.den.open(&segment).ok()?;
            if !has_components(hearth, &install) {
                return None;
            }
            Some((segment.clone(), install))
        }
        None => None,
    };
    let den = match host.den {
        Some(segment) => {
            let install = hearth.den.open(&segment).ok()?;
            if !has_components(hearth, &install) {
                return None;
            }
            Some((segment.clone(), install))
        }
        None => None,
    };
    if host.scout.as_deref().is_some_and(|segment| {
        hearth.scout.as_ref().and_then(|scout| scout.validated_install_member(segment)).is_none()
    }) || host.subs.as_deref().is_some_and(|segment| {
        hearth
            .subs
            .as_deref()
            .and_then(|subs| den_subtitles::validated_install_member(subs, segment))
            .is_none()
    }) || host.reel.as_deref().is_some_and(|segment| {
        hearth.reel.as_deref().and_then(|reel| den_reel::validated_install_member(reel, segment)).is_none()
    }) {
        return None;
    }
    Some(Opened { personal, host: OpenedHost { den, scout: host.scout, subs: host.subs, reel: host.reel } })
}

fn has_components(hearth: &Hearth, install: &den::Install) -> bool {
    install.members.iter().all(|member| match *member {
        "scout" => hearth.scout.is_some(),
        "subs" => hearth.subs.is_some(),
        "reel" => hearth.reel.is_some(),
        _ => false,
    })
}

fn operation_request(headers: &hyper::HeaderMap, operation: Operation) -> Option<Request<Full<Bytes>>> {
    let method = operation.method.parse::<Method>().ok()?;
    if !matches!(method, Method::GET | Method::HEAD | Method::POST | Method::OPTIONS)
        || operation.path.len() > 4096
        || !operation.path.starts_with('/')
        || operation.path.starts_with("//")
        || operation.path.split('/').any(|part| matches!(part, "." | "..") || part.contains(['\\', '\0']))
        || operation.query.as_ref().is_some_and(|query| query.len() > 4096 || query.starts_with('?'))
    {
        return None;
    }
    let allowed = match operation.path.as_str() {
        "/manifest.json" => matches!(method, Method::GET | Method::HEAD),
        "/playback" => matches!(method, Method::POST | Method::OPTIONS),
        "/availability" => method == Method::POST,
        path if path.starts_with("/stream/") => matches!(method, Method::GET | Method::HEAD),
        path if path.starts_with("/subtitles/") || path.starts_with("/subtitle/") => {
            matches!(method, Method::GET | Method::HEAD)
        }
        path if path.starts_with("/meta/") => matches!(method, Method::GET | Method::HEAD),
        _ => false,
    };
    if !allowed || operation.body.is_some() != (method == Method::POST) {
        return None;
    }
    let uri = match operation.query {
        Some(query) if !query.is_empty() => format!("{}?{query}", operation.path),
        _ => operation.path,
    }
    .parse::<Uri>()
    .ok()?;
    let body = operation.body.and_then(|value| serde_json::to_vec(&value).ok()).unwrap_or_default();
    let mut request = Request::builder().method(method).uri(uri);
    for name in [
        "idempotency-key",
        "x-request-id",
        "x-forwarded-for",
        "x-den-public-session",
        "x-den-playback-finalize",
        "x-den-playback-finalization",
    ] {
        if let Some(value) = headers.get(name) {
            request = request.header(name, value);
        }
    }
    if !body.is_empty() {
        request = request.header(CONTENT_TYPE, "application/json");
    }
    let request = request.body(Full::new(Bytes::from(body))).ok()?;
    if request.uri().path() == "/playback" && request.method() == Method::POST {
        playback::idempotency_key(request.headers())?;
    }
    Some(request)
}

fn manifest(opened: &Opened, method: &Method) -> Response<Body> {
    let personal = opened.personal.as_ref().map(|(_, install)| &install.members);
    let host_den = opened.host.den.as_ref().map(|(_, install)| &install.members);
    let mut members = Vec::new();
    if opened.host.scout.is_some()
        || personal.is_some_and(|members| members.contains(&"scout"))
        || host_den.is_some_and(|members| members.contains(&"scout"))
    {
        members.push("scout");
    }
    if opened.host.subs.is_some()
        || personal.is_some_and(|members| members.contains(&"subs"))
        || host_den.is_some_and(|members| members.contains(&"subs"))
    {
        members.push("subs");
    }
    if opened.host.reel.is_some() || host_den.is_some_and(|members| members.contains(&"reel")) {
        members.push("reel");
    }
    if members.is_empty() {
        return refused(StatusCode::BAD_REQUEST);
    }
    let value = den::manifest(Some(&den::Install { iid: None, members }));
    let body =
        if *method == Method::HEAD { Bytes::new() } else { Bytes::from(serde_json::to_vec(&value).unwrap()) };
    Response::builder()
        .status(StatusCode::OK)
        .header(CONTENT_TYPE, "application/json")
        .header(CACHE_CONTROL, "no-store")
        .body(den_http::full(body))
        .expect("static response")
}

async fn resource(hearth: &Arc<Hearth>, opened: &Opened, req: Request<Full<Bytes>>) -> Response<Body> {
    let path = req.uri().path().to_string();
    if path == "/availability" || path.starts_with("/stream/") {
        let Some((personal, install)) = &opened.personal else { return den_http::not_found() };
        if !install.members.contains(&"scout") {
            return den_http::not_found();
        }
        let Some(req) = crate::with_path(req, &format!("/{personal}{path}")) else {
            return refused(StatusCode::BAD_REQUEST);
        };
        return hearth.scout.as_ref().expect("guest overlay requires Scout").serve_den(req).await;
    }
    if path.starts_with("/subtitles/") || path.starts_with("/subtitle/") {
        if let Some((personal, install)) = &opened.personal {
            if install.members.contains(&"subs") {
                let Some(mut req) = crate::with_path(req, &format!("/{personal}{path}")) else {
                    return refused(StatusCode::BAD_REQUEST);
                };
                req.extensions_mut().insert(hearth.den.subs.clone());
                return den_subtitles::handle_request(
                    hearth.subs.clone().expect("guest overlay requires Subtitles"),
                    req,
                )
                .await;
            }
        }
        if let Some((host, install)) = &opened.host.den {
            if install.members.contains(&"subs") {
                let Some(mut req) = crate::with_path(req, &format!("/{host}{path}")) else {
                    return refused(StatusCode::BAD_REQUEST);
                };
                req.extensions_mut().insert(hearth.den.subs.clone());
                return den_subtitles::handle_request(
                    hearth.subs.clone().expect("guest overlay requires Subtitles"),
                    req,
                )
                .await;
            }
        }
        let Some(host) = opened.host.subs.as_deref() else {
            return den_http::not_found();
        };
        let Some(req) = crate::with_path(req, &format!("/{host}{path}")) else {
            return refused(StatusCode::BAD_REQUEST);
        };
        return den_subtitles::handle_request(
            hearth.subs.clone().expect("guest overlay requires Subtitles"),
            req,
        )
        .await;
    }
    if path.starts_with("/meta/") {
        if let Some((host, install)) = &opened.host.den {
            if install.members.contains(&"reel") {
                let Some(mut req) = crate::with_path(req, &format!("/{host}{path}")) else {
                    return refused(StatusCode::BAD_REQUEST);
                };
                req.extensions_mut().insert(hearth.den.reel.clone());
                return den_reel::handle_request(
                    hearth.reel.clone().expect("guest overlay requires Reel"),
                    req,
                )
                .await;
            }
        }
        let Some(host) = opened.host.reel.as_deref() else {
            return den_http::not_found();
        };
        let Some(req) = crate::with_path(req, &format!("/{host}{path}")) else {
            return refused(StatusCode::BAD_REQUEST);
        };
        return den_reel::handle_request(hearth.reel.clone().expect("guest overlay requires Reel"), req)
            .await;
    }
    den_http::not_found()
}

async fn scrub(response: Response<Body>, owner: &str, opened: &Opened) -> Response<Body> {
    let (parts, body) = response.into_parts();
    let json = parts
        .headers
        .get(CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|v| v.contains("json"));
    if !json {
        let mut response = Response::from_parts(parts, body);
        response.headers_mut().insert(CACHE_CONTROL, HeaderValue::from_static("no-store"));
        return response;
    }
    if parts
        .headers
        .get(CONTENT_ENCODING)
        .is_some_and(|value| !value.as_bytes().eq_ignore_ascii_case(b"identity"))
    {
        return refused(StatusCode::BAD_GATEWAY);
    }
    let bytes = match http_body_util::Limited::new(body, MAX_OUTPUT).collect().await {
        Ok(body) => body.to_bytes(),
        Err(_) => return refused(StatusCode::BAD_GATEWAY),
    };
    let Some(gid) = owner.strip_prefix("grant:") else { return den_http::not_found() };
    let mut text = match String::from_utf8(bytes.to_vec()) {
        Ok(text) => text,
        Err(_) => return refused(StatusCode::BAD_GATEWAY),
    };
    for segment in [
        opened.personal.as_ref().map(|(segment, _)| segment.as_str()),
        opened.host.den.as_ref().map(|(segment, _)| segment.as_str()),
        opened.host.scout.as_deref(),
        opened.host.subs.as_deref(),
        opened.host.reel.as_deref(),
    ]
    .into_iter()
    .flatten()
    {
        text = text.replace(segment, &format!("~{gid}"));
    }
    let mut response = Response::from_parts(parts, den_http::full(text));
    response.headers_mut().insert(CACHE_CONTROL, HeaderValue::from_static("no-store"));
    for name in [CONTENT_LENGTH, CONTENT_ENCODING, ETAG, LAST_MODIFIED, VARY] {
        response.headers_mut().remove(name);
    }
    response
}

fn refused(status: StatusCode) -> Response<Body> {
    den_http::json(
        status,
        &serde_json::json!({"error": "guest_overlay_refused"}),
        &[("cache-control", "no-store")],
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn opened(members: Vec<&'static str>) -> Opened {
        Opened {
            personal: Some(("personal".into(), den::Install { iid: None, members })),
            host: OpenedHost {
                den: None,
                scout: Some("host-scout".into()),
                subs: Some("host-subs".into()),
                reel: Some("host-reel".into()),
            },
        }
    }

    #[test]
    fn personal_scout_is_direct_but_host_scout_is_always_remux() {
        let opened = opened(vec!["scout", "subs", "reel"]);
        let sources = playback_sources(&opened);
        assert_eq!(sources.direct, Some("personal"));
        assert_eq!(sources.remux_scout.as_deref(), Some("http://den-hearth.invalid/scout/host-scout"));
        assert_eq!(sources.subtitles.as_deref(), Some("http://den-hearth.invalid/den/personal"));
        // A personal Reel member is intentionally ignored by resource(), which always selects host-reel.
        assert_eq!(opened.host.reel.as_deref(), Some("host-reel"));
    }

    #[test]
    fn host_subtitles_are_inherited_only_without_a_personal_override() {
        let opened = opened(vec!["scout"]);
        let sources = playback_sources(&opened);
        assert_eq!(sources.subtitles.as_deref(), Some("http://den-hearth.invalid/subs/host-subs"));
    }

    #[test]
    fn a_migrated_host_hearth_install_is_the_remux_and_subtitle_fallback() {
        let mut opened = opened(vec!["scout", "subs"]);
        opened.host.den =
            Some(("host-hearth".into(), den::Install { iid: None, members: vec!["scout", "subs", "reel"] }));
        let sources = playback_sources(&opened);
        assert_eq!(sources.direct, Some("personal"), "the guest's personal Scout stays preferred");
        assert_eq!(sources.remux_scout.as_deref(), Some("http://den-hearth.invalid/den/host-hearth"));
        assert_eq!(
            sources.subtitles.as_deref(),
            Some("http://den-hearth.invalid/den/personal"),
            "the personal subtitle member overrides the host"
        );

        opened.personal = None;
        let sources = playback_sources(&opened);
        assert_eq!(sources.direct, None);
        assert_eq!(sources.remux_scout.as_deref(), Some("http://den-hearth.invalid/den/host-hearth"));
        assert_eq!(sources.subtitles.as_deref(), Some("http://den-hearth.invalid/den/host-hearth"));
    }

    #[test]
    fn the_inner_operation_is_a_closed_bounded_surface() {
        let mut headers = hyper::HeaderMap::new();
        headers.insert("idempotency-key", HeaderValue::from_static("attempt-1"));
        let operation = |method: &str, path: &str| Operation {
            method: method.into(),
            path: path.into(),
            query: None,
            body: (method == "POST").then(|| serde_json::json!({})),
        };
        for (method, path) in [
            ("GET", "/manifest.json"),
            ("GET", "/stream/movie/tt1.json"),
            ("GET", "/subtitles/movie/tt1.json"),
            ("GET", "/subtitle/1.vtt"),
            ("GET", "/meta/movie/tt1.json"),
            ("POST", "/availability"),
            ("POST", "/playback"),
        ] {
            assert!(operation_request(&headers, operation(method, path)).is_some(), "{method} {path}");
        }
        for (method, path) in
            [("GET", "/configure"), ("GET", "/play/ticket"), ("GET", "/../metrics"), ("DELETE", "/playback")]
        {
            assert!(operation_request(&headers, operation(method, path)).is_none(), "{method} {path}");
        }
        headers.insert("idempotency-key", HeaderValue::from_static("two,values"));
        assert!(operation_request(&headers, operation("POST", "/playback")).is_none());
        headers.insert("idempotency-key", HeaderValue::from_static("attempt-1"));
        let missing_body =
            Operation { method: "POST".into(), path: "/playback".into(), query: None, body: None };
        assert!(operation_request(&headers, missing_body).is_none());
    }

    #[tokio::test]
    async fn config_bearers_are_replaced_before_json_leaves_the_overlay() {
        let opened = opened(vec!["scout", "subs"]);
        let response = Response::builder()
            .header(CONTENT_TYPE, "application/json")
            .header(CACHE_CONTROL, "public, max-age=3600")
            .header(ETAG, "\"upstream\"")
            .header(LAST_MODIFIED, "Wed, 21 Oct 2015 07:28:00 GMT")
            .header(VARY, "accept-encoding")
            .header(CONTENT_ENCODING, "identity")
            .body(den_http::full(
                r#"{"personal":"/den/personal/subtitle/1.vtt","host":"/subs/host-subs/subtitle/2.vtt"}"#,
            ))
            .unwrap();
        let response = scrub(response, "grant:deadbeef", &opened).await;
        assert_eq!(response.headers()[CACHE_CONTROL], "no-store");
        for name in [CONTENT_LENGTH, CONTENT_ENCODING, ETAG, LAST_MODIFIED, VARY] {
            assert!(!response.headers().contains_key(name));
        }
        let bytes = response.into_body().collect().await.unwrap().to_bytes();
        let text = std::str::from_utf8(&bytes).unwrap();
        assert!(!text.contains("personal") && !text.contains("host-subs"), "{text}");
        assert!(text.contains("~deadbeef"), "{text}");
    }

    #[tokio::test]
    async fn encoded_json_is_refused_instead_of_being_rewritten_as_text() {
        let response = Response::builder()
            .header(CONTENT_TYPE, "application/json")
            .header(CONTENT_ENCODING, "gzip")
            .body(den_http::full("compressed bytes"))
            .unwrap();
        let response = scrub(response, "grant:deadbeef", &opened(vec!["scout"])).await;
        assert_eq!(response.status(), StatusCode::BAD_GATEWAY);
        assert_eq!(response.headers()[CACHE_CONTROL], "no-store");
    }
}
