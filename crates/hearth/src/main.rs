//! den-hearth — Den's self-hosted node: Scout, Reel, Subtitles and Remux in one process.
//!
//!   /scout/...  → den-scout     (`/scout/<config>/manifest.json` is Scout's `/<config>/manifest.json`)
//!   /reel/...   → den-reel      (`/reel/play/<id>.mp4` is Reel's `/play/<id>.mp4`)
//!   /subs/...   → den-subtitles (`/subs/<config>/manifest.json` is Subtitles' `/<config>/manifest.json`)
//!   /remux/...  → den-remux     (unchanged: Remux already serves everything under `/remux`)
//!   /den/...    → the combined Den addon: one manifest and config for Scout, Subtitles and Reel (`den`)
//!   /health     → all four, each as its own service reports it
//!
//! Each service also has a listener of its own on the port its old container used (`ports`), where every
//! request goes to that service with the path unchanged.
//!
//! Each service is the same library its standalone binary runs; hearth supplies the listeners, the
//! runtime and the signal handling, and gives each service its own environment (see `env`).

mod admit;
mod alloc;
mod den;
mod env;
mod inproc;
mod media;
mod playback;
mod ports;
mod route;
mod scout;
mod sock;

use std::convert::Infallible;
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use http_body_util::{BodyExt, Empty};
use hyper::body::Incoming;
use hyper::service::service_fn;
use hyper::{Request, Response, StatusCode, Uri};
use hyper_util::rt::{TokioIo, TokioTimer};
use hyper_util::server::graceful::GracefulShutdown;
use tokio::net::TcpListener;
use tokio::sync::watch;

use media::Guarded;
use route::{Service, Target};

pub(crate) use den_http::Body;

const DEFAULT_PORT: u16 = 8090;

/// How long in-flight requests get to finish after SIGTERM: under podman's default 10 s stop timeout.
const DRAIN_GRACE: Duration = Duration::from_secs(8);

/// How long a client may take to send a request head.
const HEADER_READ_TIMEOUT: Duration = Duration::from_secs(10);

struct Hearth {
    reel: Arc<den_reel::AppState>,
    subs: Arc<den_subtitles::AppState>,
    remux: Arc<den_remux::AppState>,
    scout: scout::Scout,
    /// The combined Den addon's terms (`/den`).
    den: den::Den,
    /// One budget for the whole node: the main port and every service's own listener draw on it, so the
    /// bound on memory does not grow with the number of ports.
    admission: Arc<admit::Admission>,
    media: Arc<media::MediaGate>,
    /// Bearer token for hearth's own `/metrics`; `None` turns it off (a 404, as in the services).
    metrics_token: Option<String>,
    /// The new unified playback API. Off unless explicitly enabled after deployment validation.
    playback: playback::Playback,
}

/// Every bound the node enforces; `Default` is production, tests shrink it.
#[derive(Clone, Copy, Default)]
struct Limits {
    admission: admit::Limits,
    media: media::Limits,
}

#[derive(Default)]
struct Options {
    metrics_token: Option<String>,
    managed_playback: bool,
}

impl Hearth {
    fn new(
        reel: Arc<den_reel::AppState>,
        subs: Arc<den_subtitles::AppState>,
        remux: Arc<den_remux::AppState>,
        scout: scout::Scout,
        den: den::Den,
        limits: Limits,
        options: Options,
    ) -> Arc<Self> {
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
        })
    }
}

fn json(status: StatusCode, value: &serde_json::Value) -> Response<Body> {
    den_http::json(status, value, &[("cache-control", "no-store")])
}

/// The request as the service expects it: the same method, headers and body, with `path` in place of
/// the path hearth routed on and the query untouched.
fn with_path<B>(mut req: Request<B>, path: &str) -> Option<Request<B>> {
    let path_and_query = match req.uri().query() {
        Some(query) => format!("{path}?{query}"),
        None => path.to_string(),
    };
    *req.uri_mut() = Uri::try_from(path_and_query).ok()?;
    Some(req)
}

fn bad_path() -> Response<Body> {
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
async fn handle<B>(
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
            Some(req) => hearth.scout.serve(own == Some(Service::Scout), req).await,
            None => bad_path(),
        },
        Target::Reel(path) => match with_path(req, &path) {
            Some(req) => den_reel::handle_request(hearth.reel.clone(), req).await,
            None => bad_path(),
        },
        Target::Subs(path) => match with_path(req, &path) {
            Some(req) => den_subtitles::handle_request(hearth.subs.clone(), req).await,
            None => bad_path(),
        },
        Target::Remux(path) => match with_path(req, &path) {
            Some(req) => den_remux::handle_request(hearth.remux.clone(), req).await,
            None => bad_path(),
        },
        Target::Den(route) => den_dispatch(hearth, route, req).await,
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
            Some(req) => hearth.scout.serve_den(req).await,
            None => bad_path(),
        },
        den::Route::Subs(path) => match with_terms(req, &path, &hearth.den.subs) {
            Some(req) => den_subtitles::handle_request(hearth.subs.clone(), req).await,
            None => bad_path(),
        },
        den::Route::Reel(path) => match with_terms(req, &path, &hearth.den.reel) {
            Some(req) => den_reel::handle_request(hearth.reel.clone(), req).await,
            None => bad_path(),
        },
        den::Route::Playback(config) => playback::handle(hearth, &config, req).await,
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
    let reel = den_reel::handle_request(hearth.reel.clone(), get()).await;
    let subs = den_subtitles::handle_request(hearth.subs.clone(), get()).await;
    let remux = den_remux::handle_request(hearth.remux.clone(), get()).await;
    let services = serde_json::json!({
        "scout": report(hearth.scout.health().await).await,
        "reel": report(reel).await,
        "subs": report(subs).await,
        "remux": report(remux).await,
    });
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

/// Serve until `shutdown` resolves, then let in-flight requests finish for at most `grace`; `true` when
/// they all did.
async fn serve_until(
    listener: TcpListener,
    own: Option<Service>,
    hearth: Arc<Hearth>,
    mut shutdown: watch::Receiver<bool>,
    grace: Duration,
) -> bool {
    let graceful = GracefulShutdown::new();
    // Hyper's adaptive read buffer would otherwise grow to ~400 KiB per connection.
    let mut http = hyper::server::conn::http1::Builder::new();
    http.timer(TokioTimer::new())
        .header_read_timeout(HEADER_READ_TIMEOUT)
        .max_buf_size(admit::read_buffer(own));
    // Connections past the cap: control requests only, one response, a short head timeout.
    let mut probation_http = hyper::server::conn::http1::Builder::new();
    probation_http
        .timer(TokioTimer::new())
        .header_read_timeout(admit::PROBATION_HEADER_TIMEOUT)
        .keep_alive(false)
        .max_buf_size(admit::read_buffer(own));
    loop {
        // Capacity is taken BEFORE `accept`. Once the cap is reached the next peer waits in the kernel's
        // listen backlog and costs hearth no task, buffer or descriptor. Every listener draws on the same
        // cap, so a service's own port cannot add connections on top of the main port's.
        //
        // But a flood of persistent clients can hold every position for as long as it lasts, and a new
        // connection (the container's /health probe) would wait behind it the whole time. So past the cap a
        // few connections are accepted on probation: they may ask control routes only, get one response and
        // are closed (see `handle`). A general position that frees up while the loop waits is preferred.
        let mut probation = None;
        let mut accepted = None;
        let mut connection = hearth.admission.try_connection();
        if connection.is_none() {
            probation = hearth.admission.try_probation();
            tokio::select! {
                biased;
                _ = async { let _ = shutdown.wait_for(|stop| *stop).await; } => break,
                general = hearth.admission.connection() => {
                    connection = Some(general);
                    probation = None;
                }
                pair = listener.accept(), if probation.is_some() => accepted = Some(pair),
            }
        }
        let mut on_probation = false;
        let pair = match accepted {
            Some(pair) => pair,
            None => tokio::select! {
                pair = listener.accept() => pair,
                _ = async { let _ = shutdown.wait_for(|stop| *stop).await; } => break,
            },
        };
        let (stream, peer) = match pair {
            Ok(pair) => pair,
            Err(e) => {
                eprintln!("accept: {e}");
                // The listener stays readable while the process is out of descriptors; back off
                // rather than spin.
                tokio::time::sleep(Duration::from_millis(100)).await;
                continue;
            }
        };
        // A position that freed up while this connection was arriving makes it an ordinary one.
        let connection = match (connection, probation) {
            (Some(general), _) => general,
            (None, Some(extra)) => match hearth.admission.try_connection() {
                Some(general) => general,
                None => {
                    on_probation = true;
                    hearth.admission.probation_accepted();
                    extra
                }
            },
            (None, None) => unreachable!("a connection is accepted only with a permit"),
        };
        // A connection whose kernel queues cannot be bounded is refused rather than served unbounded.
        if let Err(e) = sock::bound_connection_buffers(&stream) {
            eprintln!("refusing {peer}: cannot bound TCP buffers: {e}");
            continue;
        }
        let hearth = hearth.clone();
        let peer = den_remux::Peer(peer.ip());
        let service = service_fn(move |mut req: Request<Incoming>| {
            // Remux decides who is a trusted proxy and who is the visitor from the socket's address.
            req.extensions_mut().insert(peer);
            let hearth = hearth.clone();
            async move { Ok::<_, Infallible>(handle(hearth, own, on_probation, req).await) }
        });
        let builder = if on_probation { &probation_http } else { &http };
        let conn = graceful.watch(builder.serve_connection(TokioIo::new(stream), service));
        tokio::spawn(async move {
            let _connection = connection;
            let _ = conn.await;
        });
    }
    drop(listener);
    tokio::select! {
        _ = graceful.shutdown() => true,
        _ = tokio::time::sleep(grace) => {
            eprintln!("drain deadline ({grace:?}) reached with requests still in flight");
            false
        }
    }
}

/// Resolves on SIGTERM (a redeploy) or SIGINT (a terminal); a second signal exits at once.
///
/// Both are registered NOW, by the caller, not when the future is first polled: until then SIGTERM keeps
/// its default disposition, and a stop in that window would kill the process outright.
fn shutdown_signal() -> impl Future<Output = ()> {
    use tokio::signal::unix::{signal, SignalKind};
    let term = signal(SignalKind::terminate());
    let int = signal(SignalKind::interrupt());
    async move {
        tokio::select! {
            _ = wait_for(term, "SIGTERM") => {}
            _ = wait_for(int, "SIGINT") => {}
        }
        // Tokio does not restore the default disposition when a `Signal` drops, so without this a
        // second ^C would be caught and discarded and the drain could only be left with SIGKILL.
        tokio::spawn(async move {
            tokio::select! {
                _ = quietly(signal(SignalKind::terminate())) => {}
                _ = quietly(signal(SignalKind::interrupt())) => {}
            }
            eprintln!("second signal — exiting without finishing the drain");
            den_remux::kill_live_groups();
            den_reel::kill_live_groups();
            std::process::exit(0);
        });
    }
}

/// Resolve when this signal arrives, or never if it could not be registered: returning at once would
/// stop the server the moment it started.
async fn wait_for(registered: std::io::Result<tokio::signal::unix::Signal>, name: &str) {
    match registered {
        Ok(mut sig) => {
            sig.recv().await;
            eprintln!("{name} — draining in-flight requests");
        }
        Err(e) => {
            eprintln!("{name} handler unavailable ({e}); it will be a hard kill");
            std::future::pending::<()>().await
        }
    }
}

/// Like `wait_for`, but says nothing.
async fn quietly(registered: std::io::Result<tokio::signal::unix::Signal>) {
    match registered {
        Ok(mut sig) => {
            sig.recv().await;
        }
        Err(_) => std::future::pending::<()>().await,
    }
}

/// Everything read from the environment before the runtime exists.
struct Configs {
    port: u16,
    /// The extra listeners: a service and the port its old container used.
    own: Vec<(Service, u16)>,
    reel: den_reel::Config,
    subs: den_subtitles::Config,
    remux: den_remux::Config,
    /// Scout behind `/scout` and on its own listener: the same environment but for the public origin.
    scout_mount: den_scout::deps::Settings,
    scout_own: den_scout::deps::Settings,
    den: den::Den,
    /// Where Remux and Subtitles reach Scout in process (`inproc`).
    scout_bases: Vec<inproc::Base>,
    metrics_token: Option<String>,
    managed_playback: bool,
}

async fn bind(port: u16) -> std::io::Result<TcpListener> {
    TcpListener::bind(("0.0.0.0", port))
        .await
        .map_err(|e| std::io::Error::new(e.kind(), format!("cannot listen on :{port}: {e}")))
}

async fn run(c: Configs) -> std::io::Result<()> {
    let port = c.port;
    let hearth = Hearth::new(
        den_reel::init(c.reel),
        den_subtitles::init(c.subs),
        den_remux::init(c.remux).await,
        scout::Scout::new(c.scout_mount, c.scout_own, &c.den.scout),
        c.den,
        Limits::default(),
        Options { metrics_token: c.metrics_token, managed_playback: c.managed_playback },
    );
    hearth.scout.start_sweep();
    let in_process = inproc::install(&hearth, &c.scout_bases);
    let (stop, shutdown) = watch::channel(false);
    let signal = shutdown_signal();
    tokio::spawn(async move {
        signal.await;
        let _ = stop.send(true);
    });
    let mut listeners = vec![(None, bind(port).await?)];
    for (service, own_port) in &c.own {
        listeners.push((Some(*service), bind(*own_port).await?));
    }
    eprintln!(
        "den-hearth {} listening on :{port} — /scout (den-scout {}), /reel (den-reel {}), /subs (den-subtitles {}), /remux (den-remux {}); own listeners: {}",
        env!("CARGO_PKG_VERSION"),
        den_scout::release_version(),
        den_reel::release_version(),
        den_subtitles::release_version(),
        den_remux::release_version(),
        if c.own.is_empty() {
            "none".to_string()
        } else {
            c.own.iter().map(|(s, p)| format!("{} :{p}", ports::name(*s))).collect::<Vec<_>>().join(", ")
        },
    );
    eprintln!("{}", alloc::startup_line());
    eprintln!("scout: {}", hearth.scout.summary());
    eprintln!("{}", hearth.den.summary());
    eprintln!("managed playback: {}", hearth.playback.summary());
    eprintln!("scout in process for remux and subs at: {}", in_process.join(", "));
    // Each service's own startup line: operators read `transcode=` and the like from them.
    eprintln!("{}", den_reel::startup_line(&hearth.reel));
    eprintln!("{}", den_remux::startup_line(&hearth.remux));
    eprintln!("{}", den_subtitles::startup_line(&hearth.subs));
    let (adm, media) = (admit::Limits::default(), media::Limits::default());
    eprintln!(
        "bounds (all listeners): connections={} requests={} (bulk {}) media={} (reel {}, remux {}) idle={}s lifetime={}s metrics={}",
        adm.connections,
        adm.requests,
        adm.bulk_requests,
        media.total,
        media.reel,
        media.remux,
        media.idle.as_secs(),
        media.lifetime.as_secs(),
        if hearth.metrics_token.is_some() { "on" } else { "off" },
    );

    let servers: Vec<_> = listeners
        .into_iter()
        .map(|(own, listener)| {
            tokio::spawn(serve_until(listener, own, hearth.clone(), shutdown.clone(), DRAIN_GRACE))
        })
        .collect();
    let mut drained = true;
    for server in servers {
        drained &= server.await.unwrap_or(false);
    }

    den_reel::finish(&hearth.reel).await;
    den_remux::finish(&hearth.remux).await;
    if drained {
        eprintln!("shut down cleanly");
    }
    Ok(())
}

/// Benchmark builds only (`--features stub-upstreams`, `bench/http/`): the services' upstream bases are fixed to
/// the public APIs, so a load test that must make no external call points them at local stubs here. The feature
/// is off in every image that ships, because a base taken from the environment would let whoever sets it
/// collect the TMDB and OpenSubtitles keys.
#[cfg(feature = "stub-upstreams")]
fn stub_upstreams(reel: &mut den_reel::Config, subs: &mut den_subtitles::Config) {
    if let Ok(base) = std::env::var("HEARTH_STUB_TMDB") {
        reel.tmdb_base = base;
    }
    if let Ok(base) = std::env::var("HEARTH_STUB_KINOCHECK") {
        reel.kinocheck_base = base;
    }
    if let Ok(base) = std::env::var("HEARTH_STUB_OPENSUBTITLES") {
        subs.os_api_base = base;
    }
    eprintln!("stub-upstreams: the TMDB, KinoCheck and OpenSubtitles bases come from HEARTH_STUB_* (benchmark build)");
}

/// `raw`, the value of variable `name`, parsed as a `T`; `default` when it is unset, and also when it
/// is set but unusable, which is said rather than silently replaced.
fn parsed_or<T: std::str::FromStr + std::fmt::Display>(name: &str, raw: Option<String>, default: T) -> T {
    let Some(raw) = raw else { return default };
    match raw.parse() {
        Ok(value) => value,
        Err(_) => {
            eprintln!("{name}={raw:?} is not a valid value; using {default}");
            default
        }
    }
}

fn main() {
    // Read before the runtime exists: the services read their configuration from the environment
    // inside `from_env`, and `env::scoped` edits the environment to give each its own.
    let port = parsed_or("PORT", std::env::var("PORT").ok(), DEFAULT_PORT);
    let threads = parsed_or("HEARTH_WORKER_THREADS", std::env::var("HEARTH_WORKER_THREADS").ok(), 2);
    let own = ports::legacy_ports(|name| std::env::var(name).ok(), port).unwrap_or_else(|e| {
        eprintln!("fatal: {e}");
        std::process::exit(1);
    });
    // `HEARTH_PUBLIC_BASE_URL` gives a service the prefix it is mounted under. A service with a listener of
    // its own keeps the origin its own `PUBLIC_BASE_URL` names, as its old container did, so it gets none.
    let mount =
        |service: Service, name: &'static str| (!own.iter().any(|(s, _)| *s == service)).then_some(name);
    #[allow(unused_mut)]
    let mut reel = env::scoped("REEL_", mount(Service::Reel, "reel"), den_reel::Config::from_env);
    #[allow(unused_mut)]
    let mut subs = env::scoped("SUBS_", mount(Service::Subs, "subs"), den_subtitles::Config::from_env);
    let mut remux = env::scoped("REMUX_", None, den_remux::Config::from_env);
    // This reserved origin never leaves the process. Remux still applies its ordinary origin SSRF guards, then
    // `inproc` serves only the combined addon's Scout and Subtitles resource slices for it.
    if !remux.scout_origins.iter().any(|origin| origin == playback::DEN_SCOUT_ORIGIN) {
        remux.scout_origins.push(playback::DEN_SCOUT_ORIGIN.to_string());
    }
    if !remux.subtitle_origins.iter().any(|origin| origin == playback::DEN_SCOUT_ORIGIN) {
        remux.subtitle_origins.push(playback::DEN_SCOUT_ORIGIN.to_string());
    }
    #[cfg(feature = "stub-upstreams")]
    stub_upstreams(&mut reel, &mut subs);
    let scout_settings =
        || den_scout::deps::settings_from_env(|name| std::env::var(name).unwrap_or_default());
    let scout_mount = env::scoped("SCOUT_", Some("scout"), scout_settings);
    let mut scout_own = env::scoped("SCOUT_", None, scout_settings);
    // The startup line names the port Scout answers on.
    let scout_port = own.iter().find(|(s, _)| *s == Service::Scout).map(|(_, p)| *p);
    if let Some(p) = scout_port {
        scout_own.port = p.to_string();
    } else {
        scout_own.port = format!("{port}/scout");
    }
    let scout_bases = inproc::scout_bases(
        port,
        scout_port,
        std::env::var("HEARTH_PUBLIC_BASE_URL").ok().as_deref(),
        &scout_own.public_url,
    );

    // Scout's key as Scout reads it: the Den addon's ring is derived from it when it has none of its own.
    let den = den::Den::from_env(
        |name| std::env::var(name).unwrap_or_default(),
        &scout_own.config_key,
        &scout_own.config_keys_prev,
    );

    let metrics_token = std::env::var("HEARTH_METRICS_TOKEN")
        .or_else(|_| std::env::var("METRICS_TOKEN"))
        .ok()
        .filter(|token| !token.is_empty());
    let managed_playback = std::env::var("HEARTH_MANAGED_PLAYBACK")
        .ok()
        .is_some_and(|value| !value.is_empty() && value != "0" && value != "false");

    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(threads)
        .enable_all()
        .build()
        .expect("tokio runtime");
    let configs = Configs {
        port,
        own,
        reel,
        subs,
        remux,
        scout_mount,
        scout_own,
        den,
        scout_bases,
        metrics_token,
        managed_playback,
    };
    if let Err(e) = rt.block_on(run(configs)) {
        eprintln!("fatal: {e}");
        std::process::exit(1);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    mod den_addon;

    fn hearth_with(limits: Limits, token: Option<&str>) -> Arc<Hearth> {
        hearth_den(limits, token, den::Den::new(None, false, Default::default(), None, "none"))
    }

    fn hearth_den(limits: Limits, token: Option<&str>, den: den::Den) -> Arc<Hearth> {
        // No disk tier: a test must not write the temp dir.
        let settings = || {
            let mut s = den_scout::deps::settings_from_env(|_| String::new());
            s.cache_dir = String::new();
            s
        };
        Hearth::new(
            den_reel::AppState::new(den_reel::Config::from_env()),
            den_subtitles::AppState::new(den_subtitles::Config::from_env()),
            den_remux::AppState::new(den_remux::Config::from_env()),
            scout::Scout::new(settings(), settings(), &den.scout),
            den,
            limits,
            Options { metrics_token: token.map(str::to_string), ..Default::default() },
        )
    }

    fn hearth() -> Arc<Hearth> {
        hearth_with(Limits::default(), None)
    }

    async fn send_on(hearth: &Arc<Hearth>, own: Option<Service>, path: &str) -> Response<Body> {
        handle(hearth.clone(), own, false, Request::get(path).body(Empty::<Bytes>::new()).unwrap()).await
    }

    async fn send(hearth: &Arc<Hearth>, path: &str) -> Response<Body> {
        send_on(hearth, None, path).await
    }

    async fn json_of(resp: Response<Body>) -> serde_json::Value {
        let bytes = resp.into_body().collect().await.unwrap().to_bytes();
        serde_json::from_slice(&bytes).unwrap_or(serde_json::Value::Null)
    }

    async fn get_on(own: Option<Service>, path: &str) -> (StatusCode, serde_json::Value) {
        let resp = send_on(&hearth(), own, path).await;
        (resp.status(), json_of(resp).await)
    }

    #[tokio::test]
    async fn unified_playback_is_a_concealed_404_by_default() {
        let request = Request::post("/den/not-a-config/playback")
            .header("content-type", "application/json")
            .header("idempotency-key", "attempt-1")
            .body(Empty::<Bytes>::new())
            .unwrap();
        let response = handle(hearth(), None, false, request).await;
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    async fn get(path: &str) -> (StatusCode, serde_json::Value) {
        get_on(None, path).await
    }

    #[test]
    fn a_bad_value_falls_back_to_the_default() {
        assert_eq!(parsed_or("PORT", None, 8090u16), 8090);
        assert_eq!(parsed_or("PORT", Some("9000".into()), 8090u16), 9000);
        assert_eq!(parsed_or("PORT", Some("http".into()), 8090u16), 8090);
        assert_eq!(parsed_or("HEARTH_WORKER_THREADS", Some("-1".into()), 2usize), 2);
    }

    /// Scout's header ceiling (Go's MaxHeaderBytes) holds under `/scout` too, not only on its own port.
    #[tokio::test]
    async fn scout_under_the_main_port_refuses_an_oversized_head() {
        let (status, _) = get(&format!("/scout/{}/manifest.json", "a".repeat(40 << 10))).await;
        assert_eq!(status, StatusCode::REQUEST_HEADER_FIELDS_TOO_LARGE);
        let (status, _) = get(&format!("/scout/{}/manifest.json", "a".repeat(16 << 10))).await;
        assert_ne!(status, StatusCode::REQUEST_HEADER_FIELDS_TOO_LARGE);
    }

    /// A node with room for two requests, one of them bulk, and one media stream.
    fn tiny() -> Arc<Hearth> {
        let limits = Limits {
            admission: admit::Limits {
                connections: 2,
                probation: 1,
                requests: 2,
                bulk_requests: 1,
                wait: Duration::from_millis(5),
                body_idle: Duration::from_secs(30),
            },
            media: media::Limits {
                total: 1,
                reel: 1,
                remux: 1,
                idle: Duration::from_secs(30),
                remux_idle: Duration::from_secs(30),
                lifetime: Duration::from_secs(60),
            },
        };
        hearth_with(limits, Some("secret"))
    }

    #[tokio::test]
    async fn the_control_lane_answers_while_bulk_is_saturated() {
        let hearth = tiny();
        let _bulk = hearth.admission.admit(route::Lane::Bulk).await.unwrap();

        // Bulk is full: a bulk request is refused, promptly and in the stable shape.
        let refused = send(&hearth, "/reel/meta/movie/tt0111161.json").await;
        assert_eq!(refused.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(refused.headers()["retry-after"], "1");
        assert_eq!(json_of(refused).await["error"], "server_busy");

        // The reserved position is still there for health and the manifests.
        for path in
            ["/health", "/scout/manifest.json", "/reel/manifest.json", "/subs/manifest.json", "/remux/health"]
        {
            let resp = send(&hearth, path).await;
            assert_eq!(resp.status(), StatusCode::OK, "{path} starved by bulk work");
        }
    }

    /// Scout and every service's own listener are in the same lanes as the main port: the bulk work of each
    /// is refused when bulk is full, and each one's probe and manifest still answer.
    #[tokio::test]
    async fn every_listener_shares_the_lanes() {
        let hearth = tiny();
        let _bulk = hearth.admission.admit(route::Lane::Bulk).await.unwrap();

        for (own, bulk, control) in [
            (Service::Scout, "/CONFIG/stream/movie/tt1.json", "/manifest.json"),
            (Service::Scout, "/p/ticket", "/health"),
            (Service::Reel, "/meta/movie/tt0111161.json", "/manifest.json"),
            (Service::Subs, "/CONFIG/subtitles/movie/tt1.json", "/manifest.json"),
            (Service::Remux, "/remux/session", "/remux/health"),
        ] {
            let refused = send_on(&hearth, Some(own), bulk).await;
            assert_eq!(refused.status(), StatusCode::SERVICE_UNAVAILABLE, "{own:?} {bulk}");
            assert_eq!(refused.headers()["retry-after"], "1", "{own:?} {bulk}");
            assert_eq!(json_of(refused).await["error"], "server_busy", "{own:?} {bulk}");

            let resp = send_on(&hearth, Some(own), control).await;
            assert_eq!(resp.status(), StatusCode::OK, "{own:?} {control} starved by bulk work");
        }
    }

    #[tokio::test]
    async fn a_refusal_gives_nothing_back_to_leak() {
        let hearth = tiny();
        let held = hearth.admission.admit(route::Lane::Bulk).await.unwrap();
        for _ in 0..20 {
            assert_eq!(send(&hearth, "/reel/nope").await.status(), StatusCode::SERVICE_UNAVAILABLE);
            assert_eq!(
                send_on(&hearth, Some(Service::Scout), "/nope/nope").await.status(),
                StatusCode::SERVICE_UNAVAILABLE
            );
        }
        drop(held);
        // Every refused request returned whatever it had taken.
        assert_eq!(send(&hearth, "/reel/nope").await.status(), StatusCode::NOT_FOUND);
        assert_eq!(
            send_on(&hearth, Some(Service::Scout), "/nope/nope").await.status(),
            StatusCode::NOT_FOUND
        );
    }

    #[tokio::test]
    async fn media_is_refused_with_its_own_stable_503_when_its_permits_are_spent() {
        let hearth = tiny();
        let _stream = hearth.media.try_enter(route::MediaClass::Remux).unwrap();

        let refused = send(&hearth, "/remux/s/abc/def/seg0.m4s").await;
        assert_eq!(refused.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(refused.headers()["retry-after"], "1");
        assert_eq!(json_of(refused).await["error"], "media_busy");
        // The class limit is per class and the total is shared: Reel is refused too.
        assert_eq!(
            send(&hearth, "/reel/play/dQw4w9WgXcQ.mp4").await.status(),
            StatusCode::SERVICE_UNAVAILABLE
        );

        // Playlists are not media: they do not need a stream permit, so a player can still ask.
        let playlist = send(&hearth, "/remux/s/abc/def/master.m3u8").await;
        assert_ne!(playlist.status(), StatusCode::SERVICE_UNAVAILABLE);
        // And the request positions the refusals passed through are free again.
        assert_eq!(send(&hearth, "/health").await.status(), StatusCode::OK);
    }

    /// The streaming services' own ports draw on the same media permits as their prefixes on the main port.
    #[tokio::test]
    async fn media_on_a_service_port_spends_the_same_permits() {
        let hearth = tiny();
        let _stream = hearth.media.try_enter(route::MediaClass::Reel).unwrap();

        for (own, path) in [
            (Service::Reel, "/play/dQw4w9WgXcQ.mp4"),
            (Service::Reel, "/progressive/dQw4w9WgXcQ.mp4"),
            // The total is shared across classes.
            (Service::Remux, "/remux/s/abc/def/seg0.m4s"),
            (Service::Remux, "/remux/speed"),
        ] {
            let refused = send_on(&hearth, Some(own), path).await;
            assert_eq!(refused.status(), StatusCode::SERVICE_UNAVAILABLE, "{own:?} {path}");
            assert_eq!(json_of(refused).await["error"], "media_busy", "{own:?} {path}");
        }
        // Scout and Subtitles have nothing to stream, so a spent media budget does not reach them.
        for (own, path) in [(Service::Scout, "/manifest.json"), (Service::Subs, "/manifest.json")] {
            assert_eq!(send_on(&hearth, Some(own), path).await.status(), StatusCode::OK, "{own:?}");
        }
    }

    #[tokio::test]
    async fn metrics_need_the_token_and_name_the_lanes() {
        let hearth = tiny();
        let anonymous = send(&hearth, "/metrics").await;
        assert_eq!(anonymous.status(), StatusCode::NOT_FOUND);

        let req = Request::get("/metrics")
            .header("authorization", "Bearer secret")
            .body(Empty::<Bytes>::new())
            .unwrap();
        let resp = handle(hearth.clone(), None, false, req).await;
        assert_eq!(resp.status(), StatusCode::OK);
        let text = String::from_utf8(resp.into_body().collect().await.unwrap().to_bytes().to_vec()).unwrap();
        for needle in [
            "den_hearth_request_admission_active{lane=\"control\"}",
            "den_hearth_request_admission_refused_total{lane=\"bulk\"}",
            "den_hearth_connection_admission_waited_total",
            "den_hearth_media_active{class=\"remux\"}",
            "den_hearth_media_refused_total{class=\"reel\"}",
        ] {
            assert!(text.contains(needle), "{needle}\n{text}");
        }
        // No token configured, no endpoint.
        assert_eq!(
            send(&hearth_with(Limits::default(), None), "/metrics").await.status(),
            StatusCode::NOT_FOUND
        );
    }

    /// A service's own `/metrics` is the service's, not hearth's: admission counters live on the main port.
    #[tokio::test]
    async fn a_service_port_metrics_is_the_services_own() {
        let hearth = tiny();
        let resp = send_on(&hearth, Some(Service::Scout), "/metrics").await;
        // Scout's own endpoint is off without its token: its 404, not hearth's text.
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
        let text = String::from_utf8_lossy(&resp.into_body().collect().await.unwrap().to_bytes()).to_string();
        assert!(!text.contains("den_hearth_"), "{text}");
    }

    async fn serve_on(
        hearth: Arc<Hearth>,
        own: Option<Service>,
    ) -> (std::net::SocketAddr, watch::Sender<bool>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (stop, stopped) = watch::channel(false);
        tokio::spawn(serve_until(listener, own, hearth, stopped, Duration::from_millis(200)));
        (addr, stop)
    }

    async fn serve(hearth: Arc<Hearth>) -> (std::net::SocketAddr, watch::Sender<bool>) {
        serve_on(hearth, None).await
    }

    /// `serve_until` with a drain grace of `grace`, keeping the server's handle so a test can watch it stop.
    async fn serve_draining(
        grace: Duration,
    ) -> (std::net::SocketAddr, watch::Sender<bool>, tokio::task::JoinHandle<bool>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (stop, stopped) = watch::channel(false);
        let server = tokio::spawn(serve_until(listener, None, hearth(), stopped, grace));
        (addr, stop, server)
    }

    #[tokio::test]
    async fn an_idle_server_stops_at_once() {
        let (_, stop, server) = serve_draining(Duration::from_secs(5)).await;
        stop.send(true).unwrap();
        let drained = tokio::time::timeout(Duration::from_secs(1), server)
            .await
            .expect("an idle server waited out the grace instead of stopping")
            .unwrap();
        assert!(drained);
    }

    /// A graceful shutdown waits for every connection, so without a deadline a client that sends half a
    /// request head and goes quiet would hold the stop open for as long as it liked.
    #[tokio::test]
    async fn a_half_sent_request_cannot_hold_the_stop_open() {
        use tokio::io::AsyncWriteExt;
        let (addr, stop, server) = serve_draining(Duration::from_millis(300)).await;
        let mut sock = tokio::net::TcpStream::connect(addr).await.unwrap();
        sock.write_all(b"GET /health HTTP/1.1\r\nHost: x\r\n").await.unwrap(); // no terminating blank line
        tokio::time::sleep(Duration::from_millis(50)).await; // let the server accept it first
        stop.send(true).unwrap();
        let drained = tokio::time::timeout(Duration::from_secs(3), server)
            .await
            .expect("the drain is unbounded")
            .unwrap();
        assert!(!drained, "the half-sent request should have run out the grace");
    }

    /// Send one request on `sock` and read the status line back.
    async fn status_line(sock: &mut tokio::net::TcpStream, request: &[u8]) -> String {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        sock.write_all(request).await.unwrap();
        let mut buf = [0u8; 256];
        let n = sock.read(&mut buf).await.unwrap();
        String::from_utf8_lossy(&buf[..n]).lines().next().unwrap_or("").to_string()
    }

    const HEALTH: &[u8] = b"GET /health HTTP/1.1\r\nHost: x\r\n\r\n";

    /// The cap is one budget for the node: a connection on a service's own port counts against the same
    /// permits as one on the main port, so adding listeners does not add connections.
    #[tokio::test]
    async fn the_connection_cap_holds_peers_in_the_backlog_not_in_userspace() {
        let hearth = hearth_with(
            Limits {
                // No probation, so the cap itself is what is under test.
                admission: admit::Limits { connections: 1, probation: 0, ..admit::Limits::default() },
                media: media::Limits::default(),
            },
            Some("secret"),
        );
        let (main_addr, _stop_main) = serve(hearth.clone()).await;
        let (scout_addr, _stop_scout) = serve_on(hearth.clone(), Some(Service::Scout)).await;

        let mut first = tokio::net::TcpStream::connect(main_addr).await.unwrap();
        assert!(status_line(&mut first, HEALTH).await.contains("200"));

        // The second connect, to ANOTHER listener, succeeds (the kernel completes the handshake) but is not
        // served: hearth has not accepted it, so it has no task and no buffer.
        let mut second = tokio::net::TcpStream::connect(scout_addr).await.unwrap();
        let held = tokio::time::timeout(Duration::from_millis(300), status_line(&mut second, HEALTH)).await;
        assert!(held.is_err(), "a connection beyond the cap was served");

        // The first closes, capacity returns, and the waiting peer is served without reconnecting.
        drop(first);
        let served = tokio::time::timeout(Duration::from_secs(5), async {
            use tokio::io::AsyncReadExt;
            let mut buf = [0u8; 256];
            let n = second.read(&mut buf).await.unwrap();
            String::from_utf8_lossy(&buf[..n]).lines().next().unwrap_or("").to_string()
        })
        .await
        .expect("the waiting peer was never served");
        assert!(served.contains("200"), "{served}");
        let mut text = String::new();
        hearth.admission.render(&mut text);
        // The accept loops waited for capacity while the first connection held it (and again for the
        // second's, which is why this is not an exact count).
        assert!(!text.contains("den_hearth_connection_admission_waited_total 0"), "{text}");
        assert!(
            text.contains("den_hearth_connection_admission_high_water 1"),
            "never more than the cap: {text}"
        );
    }

    /// A full connection pool must not keep a new control connection out: past the cap a few connections are
    /// accepted on probation, answer control routes once and are closed, and refuse everything else in the
    /// stable shape.
    #[tokio::test]
    async fn a_full_pool_still_answers_a_new_control_connection() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let hearth = hearth_with(
            Limits {
                admission: admit::Limits { connections: 1, probation: 2, ..admit::Limits::default() },
                media: media::Limits::default(),
            },
            Some("secret"),
        );
        let (addr, _stop) = serve(hearth.clone()).await;

        // A persistent client takes the only general position and keeps it.
        let mut holder = tokio::net::TcpStream::connect(addr).await.unwrap();
        assert!(status_line(&mut holder, HEALTH).await.contains("200"));

        // New control connections are served however long the holder stays, and are closed after one answer.
        for _ in 0..6 {
            let mut probe = tokio::net::TcpStream::connect(addr).await.unwrap();
            probe.write_all(HEALTH).await.unwrap();
            let mut all = Vec::new();
            tokio::time::timeout(Duration::from_secs(5), probe.read_to_end(&mut all))
                .await
                .expect("a probation connection must be closed after its answer")
                .unwrap();
            let text = String::from_utf8_lossy(&all).to_string();
            assert!(text.starts_with("HTTP/1.1 200"), "{text}");
            assert!(text.to_ascii_lowercase().contains("connection: close"), "{text}");
        }

        // A non-control request on probation is told so, in the contract's shape, and the connection closes.
        let mut bulk = tokio::net::TcpStream::connect(addr).await.unwrap();
        bulk.write_all(b"GET /reel/meta/movie/tt0111161.json HTTP/1.1\r\nHost: x\r\n\r\n").await.unwrap();
        let mut all = Vec::new();
        tokio::time::timeout(Duration::from_secs(5), bulk.read_to_end(&mut all)).await.unwrap().unwrap();
        let text = String::from_utf8_lossy(&all).to_ascii_lowercase();
        assert!(text.starts_with("http/1.1 503"), "{text}");
        assert!(text.contains("retry-after: 1") && text.contains("server_busy"), "{text}");

        // The holder was never disturbed. (`status_line` read only the head of its first answer: drain the rest.)
        let mut rest = [0u8; 8192];
        let _ = tokio::time::timeout(Duration::from_millis(100), holder.read(&mut rest)).await;
        assert!(status_line(&mut holder, HEALTH).await.contains("200"));
        let mut metrics = String::new();
        hearth.admission.render(&mut metrics);
        assert!(metrics.contains("den_hearth_connection_admission_probation_total 7"), "{metrics}");
        assert!(metrics.contains("den_hearth_connection_admission_probation_refused_total 1"), "{metrics}");
    }

    /// A position that frees up while the loop waits goes to the next peer as an ordinary connection, which
    /// may then ask anything: probation is for overflow, not for the first connection after a busy moment.
    #[tokio::test]
    async fn a_connection_after_the_pool_frees_is_not_on_probation() {
        let hearth = hearth_with(
            Limits {
                admission: admit::Limits { connections: 1, probation: 1, ..admit::Limits::default() },
                media: media::Limits::default(),
            },
            Some("secret"),
        );
        let (addr, _stop) = serve(hearth.clone()).await;
        let mut holder = tokio::net::TcpStream::connect(addr).await.unwrap();
        assert!(status_line(&mut holder, HEALTH).await.contains("200"));
        // The loop is now parked in `accept` holding a probation permit; free the general position.
        drop(holder);
        tokio::time::sleep(Duration::from_millis(100)).await;
        let mut next = tokio::net::TcpStream::connect(addr).await.unwrap();
        let line = status_line(&mut next, b"GET /reel/nope HTTP/1.1\r\nHost: x\r\n\r\n").await;
        assert!(line.contains("404"), "an ordinary connection was refused as overflow: {line}");
    }

    async fn head_status(addr: std::net::SocketAddr, pad: usize) -> String {
        use tokio::io::AsyncWriteExt;
        let mut sock = tokio::net::TcpStream::connect(addr).await.unwrap();
        let head = format!("GET /health HTTP/1.1\r\nHost: x\r\nX-Pad: {}\r\n\r\n", "a".repeat(pad));
        let _ = sock.write_all(head.as_bytes()).await;
        let mut buf = [0u8; 256];
        let n = tokio::io::AsyncReadExt::read(&mut sock, &mut buf).await.unwrap_or(0);
        String::from_utf8_lossy(&buf[..n]).lines().next().unwrap_or("").to_string()
    }

    #[tokio::test]
    async fn a_request_head_past_the_read_buffer_is_refused_not_buffered() {
        let hearth = hearth();
        // Reel's port holds the 16 KiB ceiling; Scout's and the main port hold 32 KiB because a sealed
        // config rides in the URL.
        let (reel, _a) = serve_on(hearth.clone(), Some(Service::Reel)).await;
        let (scout, _b) = serve_on(hearth.clone(), Some(Service::Scout)).await;
        let (main, _c) = serve(hearth.clone()).await;

        let over_reel = admit::CONNECTION_READ_BUFFER * 2;
        assert!(!head_status(reel, over_reel).await.contains("200"), "an oversized head was served");
        // A head between the two ceilings: refused where the buffer is 16 KiB, served where it is 32 KiB.
        let between = admit::CONNECTION_READ_BUFFER + 4096;
        assert!(!head_status(reel, between).await.contains("200"));
        assert!(head_status(scout, between).await.contains("200"));
        assert!(head_status(main, between).await.contains("200"));
        let over_scout = admit::SCOUT_READ_BUFFER * 2;
        assert!(!head_status(scout, over_scout).await.contains("200"));
        assert!(!head_status(main, over_scout).await.contains("200"));

        // A normal head still works on a fresh connection.
        assert!(head_status(reel, 16).await.contains("200"));
    }

    #[tokio::test]
    async fn each_legacy_port_serves_its_service_at_the_root() {
        // The paths the old containers' clients use, with no prefix.
        assert_eq!(get_on(Some(Service::Scout), "/manifest.json").await.1["id"], "com.den.scout");
        assert_eq!(get_on(Some(Service::Reel), "/manifest.json").await.1["id"], "com.den.reel");
        assert_eq!(get_on(Some(Service::Subs), "/manifest.json").await.1["id"], "com.den.subtitles");
        let (status, remux) = get_on(Some(Service::Remux), "/remux/health").await;
        assert_eq!(status, StatusCode::OK);
        assert!(remux["status"].is_string(), "{remux}");
    }

    #[tokio::test]
    async fn a_legacy_port_answers_health_as_its_service_not_as_hearth() {
        for service in [Service::Scout, Service::Reel, Service::Subs] {
            let (status, body) = get_on(Some(service), "/health").await;
            assert_eq!(status, StatusCode::OK, "{service:?}");
            assert!(body["status"].is_string(), "{service:?}: {body}");
            assert!(body.get("services").is_none(), "{service:?} answered with hearth's combined health");
        }
    }

    #[tokio::test]
    async fn a_legacy_port_does_not_strip_or_honour_prefixes() {
        // The path reaches the service as it came, so a prefix is the service's to judge (Scout reads
        // `scout` as a config segment and refuses it; Reel and Subtitles have no such route): never the
        // manifest the prefixed path would have served on :8090.
        for (service, path) in [
            (Service::Scout, "/scout/manifest.json"),
            (Service::Reel, "/reel/manifest.json"),
            (Service::Subs, "/subs/manifest.json"),
            // Another service's prefix is not routed from a legacy port either.
            (Service::Scout, "/reel/manifest.json"),
        ] {
            let (status, body) = get_on(Some(service), path).await;
            assert!(status.is_client_error(), "{service:?} {path}: {status}");
            assert!(body["id"].is_null(), "{service:?} {path}: {body}");
        }
    }

    #[tokio::test]
    async fn the_main_port_keeps_its_prefixes_and_scout_among_them() {
        assert_eq!(get("/scout/manifest.json").await.1["id"], "com.den.scout");
        assert_eq!(get("/scout/CONFIG/manifest.json").await.0, StatusCode::BAD_REQUEST);
        assert_eq!(get("/scout/config-key").await.0, StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn each_prefix_reaches_its_own_service() {
        assert_eq!(get("/reel/manifest.json").await.1["id"], "com.den.reel");
        assert_eq!(get("/subs/manifest.json?x=1").await.1["id"], "com.den.subtitles");
        // Remux is not stripped: its health answer lives at /remux/health.
        let (status, remux) = get("/remux/health").await;
        assert_eq!(status, StatusCode::OK);
        assert!(remux["status"].is_string(), "{remux}");
    }

    #[tokio::test]
    async fn an_unrouted_path_is_hearths_404_not_a_services() {
        let (status, body) = get("/manifest.json").await;
        assert_eq!(status, StatusCode::NOT_FOUND);
        assert_eq!(body["error"], "not_found");
    }

    #[tokio::test]
    async fn health_reports_all_four() {
        let (status, body) = get("/health").await;
        assert_eq!(status, StatusCode::OK);
        for service in ["scout", "reel", "subs", "remux"] {
            assert!(body["services"][service]["status"].is_string(), "{service}: {body}");
        }
        let all_ok = ["scout", "reel", "subs", "remux"].iter().all(|s| body["services"][s]["status"] == "ok");
        assert_eq!(body["status"], if all_ok { "ok" } else { "degraded" });
    }
}
