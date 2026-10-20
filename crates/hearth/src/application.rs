//! Hearth state and request dispatch.

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;

use http_body_util::{BodyExt, Empty};

use hyper::{Method, Request, Response, StatusCode, Uri};

use serde_json::Value;

use crate::media::Guarded;

use crate::route::{Service, Target};

use crate::Body;
use crate::{admit, den, guest, media, playback, route, wanted};

pub(crate) struct Hearth {
    pub(crate) reel: Option<Arc<den_reel::AppState>>,
    pub(crate) subs: Option<Arc<den_subtitles::AppState>>,
    pub(crate) remux: Option<Arc<den_remux::AppState>>,
    pub(crate) scout: Option<crate::scout::Scout>,
    /// The combined Den addon's terms (`/den`).
    pub(crate) den: den::Den,
    /// One budget for the whole node: the main port and every service's own listener draw on it, so the
    /// bound on memory does not grow with the number of ports.
    pub(crate) admission: Arc<admit::Admission>,
    pub(crate) media: Arc<media::MediaGate>,
    /// Bearer token for hearth's own `/metrics`; `None` turns it off (a 404, as in the services).
    pub(crate) metrics_token: Option<String>,
    /// The new unified playback API. Off unless explicitly enabled after deployment validation.
    pub(crate) playback: playback::Playback,
    /// Legacy config exchange. Off until the matching Edge client and keyrings are deployed.
    pub(crate) legacy_migration: bool,
    /// The downloads each install asked Hearth to keep looking for (`/den/<config>/wanted`).
    pub(crate) wanted: wanted::Wanted,
}

/// Every bound the node enforces; `Default` is production, tests shrink it.
#[derive(Clone, Copy, Default)]
pub(crate) struct Limits {
    pub(crate) admission: admit::Limits,
    pub(crate) media: media::Limits,
}

#[derive(Default)]
pub(crate) struct Options {
    pub(crate) metrics_token: Option<String>,
    pub(crate) managed_playback: bool,
    pub(crate) legacy_migration: bool,
    /// Where the wanted lists are kept across a restart; None keeps them in RAM only.
    pub(crate) wanted_file: Option<std::path::PathBuf>,
}

impl Hearth {
    pub(crate) fn new(
        reel: Option<Arc<den_reel::AppState>>,
        subs: Option<Arc<den_subtitles::AppState>>,
        remux: Option<Arc<den_remux::AppState>>,
        scout: Option<crate::scout::Scout>,
        den: den::Den,
        limits: Limits,
        options: Options,
    ) -> Arc<Self> {
        // A viewer's player reads a release straight from the debrid; Scout's play redirect is all this process
        // sees of it, and Remux's intro scan must not read the same episode then.
        if let (Some(scout), Some(remux)) = (&scout, &remux) {
            let segments = remux.segments.clone();
            scout.observe_plays(Arc::new(move |imdb, season, episode| {
                segments.note_play(imdb, season, episode)
            }));
        }
        Arc::new(Hearth {
            reel,
            subs,
            remux,
            scout,
            den,
            admission: admit::Admission::new(limits.admission),
            media: media::MediaGate::new(limits.media),
            metrics_token: options.metrics_token,
            playback: playback::Playback::new(options.managed_playback),
            legacy_migration: options.legacy_migration,
            wanted: wanted::Wanted::new(Box::new(wanted::InProcess), options.wanted_file),
        })
    }
}

pub(crate) fn json(status: StatusCode, value: &serde_json::Value) -> Response<Body> {
    den_http::json(status, value, &[("cache-control", "no-store")])
}

/// The request as the service expects it: the same method, headers and body, with `path` in place of
/// the path hearth routed on and the query untouched.
pub(crate) fn with_path<B>(mut req: Request<B>, path: &str) -> Option<Request<B>> {
    let path_and_query = match req.uri().query() {
        Some(query) => format!("{path}?{query}"),
        None => path.to_string(),
    };
    *req.uri_mut() = Uri::try_from(path_and_query).ok()?;
    Some(req)
}

pub(crate) fn bad_path() -> Response<Body> {
    json(StatusCode::BAD_REQUEST, &serde_json::json!({"error": "bad_path"}))
}

// Generic over the request body only so tests can drive it with an empty one; the bounds are Remux's.
// `own`: the service whose own listener the request arrived on, or `None` for the main one.
//
// Admission wraps routing and is the same on every listener: a request is admitted into its lane before
// any service sees it, and what it was admitted to follows its response body
// (`admit::RequestPermit::hold`, `media::MediaGate::guard`).
//
// `probation`: the connection was accepted past the connection cap, and may ask control routes only.
pub(crate) async fn handle<B>(
    hearth: Arc<Hearth>,
    own: Option<Service>,
    probation: bool,
    req: Request<B>,
) -> Response<Body>
where
    B: hyper::body::Body<Data = Bytes> + Send + 'static,
    B::Error: Into<Box<dyn std::error::Error + Send + Sync>>,
{
    let target = match own {
        Some(service) => route::legacy(service, req.uri().path()),
        None => route::target(req.uri().path()),
    };
    let class = route::classify(&target, req.method());
    if probation && class.lane != route::Lane::Control {
        hearth.admission.probation_refused();
        return admit::busy("server_busy");
    }
    let Ok(permit) = hearth.admission.admit(class.lane).await else {
        return admit::busy("server_busy");
    };
    // A stream permit is taken up front and never waited for: a player's retry is cheaper than a queue.
    let lease = match class.media {
        Some(media) => match hearth.media.try_enter(media) {
            Some(lease) => Some(lease),
            None => return admit::busy("media_busy"),
        },
        None => None,
    };
    let response = admit::with_retry_after(dispatch(&hearth, own, target, req).await);
    match lease {
        Some(lease) => match hearth.media.guard(response, lease) {
            // The stream permit owns the body from here, so the request position is free for the next
            // request instead of idling for as long as a viewer takes to watch a segment.
            Guarded::Streaming(response) => response,
            Guarded::Plain(response) => permit.hold(response),
        },
        None => permit.hold(response),
    }
}

async fn dispatch<B>(
    hearth: &Arc<Hearth>,
    own: Option<Service>,
    target: Target,
    req: Request<B>,
) -> Response<Body>
where
    B: hyper::body::Body<Data = Bytes> + Send + 'static,
    B::Error: Into<Box<dyn std::error::Error + Send + Sync>>,
{
    match target {
        Target::Metrics => metrics(hearth, req.headers()),
        Target::Health => json(StatusCode::OK, &health(hearth).await),
        Target::Scout(path) => match with_path(req, &path) {
            Some(req) => match &hearth.scout {
                Some(scout) => scout.serve(own == Some(Service::Scout), req).await,
                None => den_http::not_found(),
            },
            None => bad_path(),
        },
        Target::Reel(path) => match with_path(req, &path) {
            Some(req) => match &hearth.reel {
                Some(reel) => den_reel::handle_request(reel.clone(), req).await,
                None => den_http::not_found(),
            },
            None => bad_path(),
        },
        Target::Subs(path) => match with_path(req, &path) {
            Some(req) => match &hearth.subs {
                Some(subs) => den_subtitles::handle_request(subs.clone(), req).await,
                None => den_http::not_found(),
            },
            None => bad_path(),
        },
        Target::Remux(path) => match with_path(req, &path) {
            Some(req) => match &hearth.remux {
                Some(remux) => den_remux::handle_request(remux.clone(), req).await,
                None => den_http::not_found(),
            },
            None => bad_path(),
        },
        Target::Den(route) if hearth.scout.is_some() && hearth.reel.is_some() && hearth.subs.is_some() => {
            den_dispatch(hearth, route, req).await
        }
        Target::Den(_) => den_http::not_found(),
        Target::NotFound => json(StatusCode::NOT_FOUND, &serde_json::json!({"error": "not_found"})),
    }
}

/// A request under `/den`: a resource goes to its service, with the path it routes and, for Subtitles and Reel,
/// the addon's terms in its extensions (Scout has a handler of its own on them); the rest hearth answers.
async fn den_dispatch<B>(hearth: &Arc<Hearth>, route: den::Route, req: Request<B>) -> Response<Body>
where
    B: hyper::body::Body<Data = Bytes> + Send + 'static,
    B::Error: Into<Box<dyn std::error::Error + Send + Sync>>,
{
    let with_terms = |req: Request<B>, path: &str, terms: &Arc<den_seal::Addon>| {
        with_path(req, path).map(|mut req| {
            req.extensions_mut().insert(terms.clone());
            req
        })
    };
    match route {
        den::Route::Scout(path) => match with_path(req, &path) {
            Some(req) => hearth.scout.as_ref().expect("Den requires Scout").serve_den(req).await,
            None => bad_path(),
        },
        den::Route::Subs(path) => match with_terms(req, &path, &hearth.den.subs) {
            Some(req) => {
                den_subtitles::handle_request(hearth.subs.clone().expect("Den requires Subtitles"), req).await
            }
            None => bad_path(),
        },
        den::Route::Reel(path) => match with_terms(req, &path, &hearth.den.reel) {
            Some(req) => den_reel::handle_request(hearth.reel.clone().expect("Den requires Reel"), req).await,
            None => bad_path(),
        },
        den::Route::Playback(config) => playback::handle(hearth, &config, req).await,
        den::Route::Wanted(config) => wanted::handle(hearth, &config, req).await,
        den::Route::Guest => guest::handle(hearth, req).await,
        den::Route::Sidecar(token, track) => playback::sidecar(hearth, &token, track, req).await,
        den::Route::Migrate if hearth.legacy_migration => migrate_legacy(hearth, req).await,
        den::Route::Migrate => den_http::not_found(),
        own => hearth.den.answer(own, req.method(), req.headers()),
    }
}

/// Exchange legacy first-party bearer segments for one Hearth bearer segment. Possessing and passing validation of
/// a legacy segment is the authority; all failures are deliberately the same response, so this is not a config
/// oracle. No upstream is contacted and no plaintext credential is returned.
async fn migrate_legacy<B>(hearth: &Hearth, req: Request<B>) -> Response<Body>
where
    B: hyper::body::Body<Data = Bytes> + Send + 'static,
    B::Error: Into<Box<dyn std::error::Error + Send + Sync>>,
{
    use http_body_util::BodyExt;

    const MAX: usize = 32 * 1024;
    if !migration_authorized(hearth, &req) {
        return den_http::not_found();
    }
    if req.method() == Method::OPTIONS {
        let mut response = Response::builder()
            .status(StatusCode::NO_CONTENT)
            .header("allow", "POST, OPTIONS")
            .header("access-control-allow-methods", "POST, OPTIONS")
            .header("access-control-allow-headers", "content-type")
            .body(den_http::full(""))
            .unwrap();
        den_http::cors_stamp(response.headers_mut(), "");
        return response;
    }
    if req.method() != Method::POST {
        let mut response = migration_json(StatusCode::METHOD_NOT_ALLOWED, "method_not_allowed");
        response.headers_mut().insert("allow", hyper::header::HeaderValue::from_static("POST, OPTIONS"));
        return response;
    }
    if !req.headers().get(hyper::header::CONTENT_TYPE).and_then(|v| v.to_str().ok()).is_some_and(|v| {
        v.split(';').next().is_some_and(|mime| mime.trim().eq_ignore_ascii_case("application/json"))
    }) {
        return migration_json(StatusCode::UNSUPPORTED_MEDIA_TYPE, "unsupported_media_type");
    }
    let collected = tokio::time::timeout(
        Duration::from_secs(10),
        http_body_util::Limited::new(req.into_body(), MAX).collect(),
    )
    .await;
    let bytes = match collected {
        Ok(Ok(body)) => body.to_bytes(),
        Ok(Err(_)) | Err(_) => return migration_json(StatusCode::BAD_REQUEST, "migration_refused"),
    };
    let Ok(Value::Object(input)) = serde_json::from_slice::<Value>(&bytes) else {
        return migration_json(StatusCode::BAD_REQUEST, "migration_refused");
    };
    if input.is_empty()
        || input.keys().any(|key| !matches!(key.as_str(), "scout" | "subs" | "reel" | "target"))
    {
        return migration_json(StatusCode::BAD_REQUEST, "migration_refused");
    }
    let target = match input.get("target") {
        Some(Value::String(target)) => Some(target.clone()),
        Some(_) => return migration_json(StatusCode::BAD_REQUEST, "migration_refused"),
        None => None,
    };
    let mut members = serde_json::Map::new();
    for (name, value) in input {
        if name == "target" {
            continue;
        }
        let Some(segment) = value.as_str() else {
            return migration_json(StatusCode::BAD_REQUEST, "migration_refused");
        };
        let member = match name.as_str() {
            "scout" => hearth.scout.as_ref().and_then(|state| state.migration_member(segment)),
            "subs" => {
                hearth.subs.as_deref().and_then(|state| den_subtitles::migration_member(state, segment))
            }
            "reel" => hearth.reel.as_deref().and_then(|state| den_reel::migration_member(state, segment)),
            _ => None,
        };
        let Some(member) = member else {
            return migration_json(StatusCode::BAD_REQUEST, "migration_refused");
        };
        members.insert(name, member);
    }
    if members.is_empty() {
        return migration_json(StatusCode::BAD_REQUEST, "migration_refused");
    }
    let segment = match target {
        Some(target) if hearth.den.migration_target_matches(&target, &members) => target,
        Some(_) => return migration_json(StatusCode::BAD_REQUEST, "migration_refused"),
        None => {
            let Ok(segment) = hearth.den.migrate_segment(members) else {
                return migration_json(StatusCode::SERVICE_UNAVAILABLE, "migration_unavailable");
            };
            segment
        }
    };
    if hearth.den.open(&segment).is_err() {
        return migration_json(StatusCode::SERVICE_UNAVAILABLE, "migration_unavailable");
    }
    let mut response =
        json(StatusCode::OK, &serde_json::json!({"manifestUrl": format!("/den/{segment}/manifest.json")}));
    den_http::cors_stamp(response.headers_mut(), "");
    response
}

/// Direct LAN callers prove authority by possessing each legacy bearer. Calls arriving from the configured public
/// Edge proxy additionally need Edge's secret and the owner-only marker; a guest owner header always disqualifies
/// the request. Internal headers from any other peer grant nothing.
pub(crate) fn migration_authorized<B>(hearth: &Hearth, req: &Request<B>) -> bool {
    let Some(remux) = hearth.remux.as_ref() else { return true };
    let peer = req.extensions().get::<den_remux::Peer>().map(|peer| peer.0);
    if peer.is_none() || peer != remux.cfg.public_session_proxy {
        return true;
    }
    let secret = req.headers().get("x-den-edge-secret").map(hyper::header::HeaderValue::as_bytes);
    den_remux::edge_secret_ok(remux.cfg.edge_secret.as_deref(), secret)
        && req.headers().get("x-den-migration-owner").is_some_and(|value| value == "1")
        && !req.headers().contains_key("x-den-owner")
}

fn migration_json(status: StatusCode, error: &'static str) -> Response<Body> {
    let mut response = json(status, &serde_json::json!({"error": error}));
    den_http::cors_stamp(response.headers_mut(), "");
    response
}

/// Hearth's own `/metrics`: what admission is holding. The services keep theirs at `/scout/metrics`,
/// `/reel/metrics`, `/subs/metrics` and `/remux/metrics` (and at `/metrics` on their own ports). Behind a
/// bearer token and a plain 404 without it, so nobody is told there is something to poke at; the series
/// are low-cardinality by construction (two lanes, two media classes), never per-route or per-peer.
fn metrics(hearth: &Hearth, headers: &hyper::HeaderMap) -> Response<Body> {
    if !den_http::metrics_authorized(headers, hearth.metrics_token.as_deref()) {
        return den_http::not_found();
    }
    let mut text = String::with_capacity(4096);
    hearth.admission.render(&mut text);
    hearth.media.render(&mut text);
    hearth.playback.render(&mut text);
    Response::builder()
        .status(StatusCode::OK)
        .header("content-type", "text/plain; version=0.0.4; charset=utf-8")
        .header("cache-control", "no-store")
        .body(den_http::full(text))
        .expect("static response")
}

/// Each service's own `/health` answer, side by side. `degraded` when any of them says anything but
/// `ok`; the response is a 200 either way, as the services' are: it answers "is the process up".
async fn health(hearth: &Hearth) -> serde_json::Value {
    let get = || Request::get("/health").body(Empty::<Bytes>::new()).expect("static request");
    let scout = async {
        match &hearth.scout {
            Some(scout) => Some(report(scout.health().await).await),
            None => None,
        }
    };
    let reel = async {
        match &hearth.reel {
            Some(reel) => Some(report(den_reel::handle_request(reel.clone(), get()).await).await),
            None => None,
        }
    };
    let subs = async {
        match &hearth.subs {
            Some(subs) => Some(report(den_subtitles::handle_request(subs.clone(), get()).await).await),
            None => None,
        }
    };
    let remux = async {
        match &hearth.remux {
            Some(remux) => Some(report(den_remux::handle_request(remux.clone(), get()).await).await),
            None => None,
        }
    };
    let (scout, reel, subs, remux) = tokio::join!(scout, reel, subs, remux);

    let mut services = serde_json::Map::new();
    if let Some(report) = scout {
        services.insert("scout".into(), report);
    }
    if let Some(report) = reel {
        services.insert("reel".into(), report);
    }
    if let Some(report) = subs {
        services.insert("subs".into(), report);
    }
    if let Some(report) = remux {
        services.insert("remux".into(), report);
    }
    let services = serde_json::Value::Object(services);
    let all_ok = services.as_object().is_some_and(|all| all.values().all(|s| s["status"] == "ok"));
    serde_json::json!({"status": if all_ok { "ok" } else { "degraded" }, "services": services})
}

/// A service's health response body as JSON, or `unavailable` when it did not answer with one.
async fn report(resp: Response<Body>) -> serde_json::Value {
    let unavailable = || serde_json::json!({"status": "unavailable"});
    match resp.into_body().collect().await {
        Ok(body) => serde_json::from_slice(&body.to_bytes()).unwrap_or_else(|_| unavailable()),
        Err(_) => unavailable(),
    }
}
