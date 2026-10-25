//! Hearth state and request dispatch.

use std::sync::Arc;

use bytes::Bytes;

use http_body_util::{BodyExt, Empty};

use hyper::{Request, Response, StatusCode, Uri};

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
        own => hearth.den.answer(own, req.method(), req.headers()),
    }
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
    let mut out = serde_json::json!({"status": if all_ok { "ok" } else { "degraded" }, "services": services});
    // Scout's account of the debrid, at the top as well, so a client deciding whether to warn the viewer reads one
    // field rather than knowing which service owns it.
    if let Some(debrid) = out["services"]["scout"].get("debrid").cloned() {
        out["debrid"] = debrid;
    }
    out
}

/// A service's health response body as JSON, or `unavailable` when it did not answer with one.
async fn report(resp: Response<Body>) -> serde_json::Value {
    let unavailable = || serde_json::json!({"status": "unavailable"});
    match resp.into_body().collect().await {
        Ok(body) => serde_json::from_slice(&body.to_bytes()).unwrap_or_else(|_| unavailable()),
        Err(_) => unavailable(),
    }
}
