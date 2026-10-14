//! den-hearth — Den's self-hosted node: Scout, Reel, Subtitles and Remux in one process.
//!
//!   /scout/...  → den-scout     (`/scout/<config>/manifest.json` is Scout's `/<config>/manifest.json`)
//!   /reel/...   → den-reel      (`/reel/play/<id>.mp4` is Reel's `/play/<id>.mp4`)
//!   /subs/...   → den-subtitles (`/subs/<config>/manifest.json` is Subtitles' `/<config>/manifest.json`)
//!   /remux/...  → den-remux     (unchanged: Remux already serves everything under `/remux`)
//!   /health     → all four, each as its own service reports it
//!
//! Each service also has a listener of its own on the port its old container used (`ports`), where every
//! request goes to that service with the path unchanged.
//!
//! Each service is the same library its standalone binary runs; hearth supplies the listeners, the
//! runtime and the signal handling, and gives each service its own environment (see `env`).

mod env;
mod ports;
mod route;
mod scout;

use std::convert::Infallible;
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use http_body_util::combinators::BoxBody;
use http_body_util::{BodyExt, Empty};
use hyper::body::Incoming;
use hyper::service::service_fn;
use hyper::{Request, Response, StatusCode, Uri};
use hyper_util::rt::{TokioIo, TokioTimer};
use hyper_util::server::graceful::GracefulShutdown;
use tokio::net::TcpListener;
use tokio::sync::watch;

use route::{Service, Target};

type Body = BoxBody<Bytes, std::io::Error>;

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
}

fn json(status: StatusCode, value: &serde_json::Value) -> Response<Body> {
    let body = value.to_string();
    Response::builder()
        .status(status)
        .header("content-type", "application/json")
        .header("cache-control", "no-store")
        .body(Body::new(http_body_util::Full::new(Bytes::from(body)).map_err(|never| match never {})))
        .expect("static response")
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

/// Subtitles' body can never fail; hearth's can, so say so.
fn boxed(resp: Response<http_body_util::Full<Bytes>>) -> Response<Body> {
    resp.map(|body| Body::new(body.map_err(|never| match never {})))
}

// Generic over the request body only so tests can drive it with an empty one; the bounds are Remux's.
// `own`: the service whose own listener the request arrived on, or `None` for the main one.
async fn handle<B>(hearth: Arc<Hearth>, own: Option<Service>, req: Request<B>) -> Response<Body>
where
    B: hyper::body::Body<Data = Bytes> + Send + 'static,
    B::Error: Into<Box<dyn std::error::Error + Send + Sync>>,
{
    let target = match own {
        Some(service) => route::legacy(service, req.uri().path()),
        None => route::target(req.uri().path()),
    };
    match target {
        Target::Health => json(StatusCode::OK, &health(&hearth).await),
        Target::Scout(path) => match with_path(req, &path) {
            Some(req) => hearth.scout.serve(own == Some(Service::Scout), req).await,
            None => bad_path(),
        },
        Target::Reel(path) => match with_path(req, &path) {
            Some(req) => den_reel::handle_request(hearth.reel.clone(), req).await,
            None => bad_path(),
        },
        Target::Subs(path) => match with_path(req, &path) {
            Some(req) => boxed(den_subtitles::handle_request(hearth.subs.clone(), req).await),
            None => bad_path(),
        },
        Target::Remux(path) => match with_path(req, &path) {
            Some(req) => den_remux::handle_request(hearth.remux.clone(), req).await,
            None => bad_path(),
        },
        Target::NotFound => json(StatusCode::NOT_FOUND, &serde_json::json!({"error": "not_found"})),
    }
}

/// Each service's own `/health` answer, side by side. `degraded` when any of them says anything but
/// `ok`; the response is a 200 either way, as the services' are: it answers "is the process up".
async fn health(hearth: &Hearth) -> serde_json::Value {
    let get = || Request::get("/health").body(Empty::<Bytes>::new()).expect("static request");
    let reel = den_reel::handle_request(hearth.reel.clone(), get()).await;
    let subs = boxed(den_subtitles::handle_request(hearth.subs.clone(), get()).await);
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
    loop {
        let (stream, peer) = tokio::select! {
            accepted = listener.accept() => match accepted {
                Ok(pair) => pair,
                Err(e) => {
                    eprintln!("accept: {e}");
                    // The listener stays readable while the process is out of descriptors; back off
                    // rather than spin.
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    continue;
                }
            },
            _ = async { let _ = shutdown.wait_for(|stop| *stop).await; } => break,
        };
        let hearth = hearth.clone();
        let peer = den_remux::Peer(peer.ip());
        let service = service_fn(move |mut req: Request<Incoming>| {
            // Remux decides who is a trusted proxy and who is the visitor from the socket's address.
            req.extensions_mut().insert(peer);
            let hearth = hearth.clone();
            async move { Ok::<_, Infallible>(handle(hearth, own, req).await) }
        });
        let mut http = hyper::server::conn::http1::Builder::new();
        http.timer(TokioTimer::new()).header_read_timeout(HEADER_READ_TIMEOUT);
        if own == Some(Service::Scout) {
            // Go's MaxHeaderBytes: twice the largest config segment a URL legitimately carries.
            http.max_buf_size(32 << 10);
        }
        let conn = http.serve_connection(TokioIo::new(stream), service);
        let conn = graceful.watch(conn);
        tokio::spawn(async move {
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
}

async fn bind(port: u16) -> std::io::Result<TcpListener> {
    TcpListener::bind(("0.0.0.0", port))
        .await
        .map_err(|e| std::io::Error::new(e.kind(), format!("cannot listen on :{port}: {e}")))
}

async fn run(c: Configs) -> std::io::Result<()> {
    let port = c.port;
    let hearth = Arc::new(Hearth {
        reel: den_reel::init(c.reel),
        subs: den_subtitles::init(c.subs),
        remux: den_remux::init(c.remux).await,
        scout: scout::Scout::new(c.scout_mount, c.scout_own),
    });
    hearth.scout.start_sweep();
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
    eprintln!("scout: {}", hearth.scout.summary());

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

fn main() {
    // Read before the runtime exists: the services read their configuration from the environment
    // inside `from_env`, and `env::scoped` edits the environment to give each its own.
    let port = std::env::var("PORT").ok().and_then(|v| v.parse().ok()).unwrap_or(DEFAULT_PORT);
    let threads = std::env::var("HEARTH_WORKER_THREADS").ok().and_then(|v| v.parse().ok()).unwrap_or(2);
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
    let remux = env::scoped("REMUX_", None, den_remux::Config::from_env);
    #[cfg(feature = "stub-upstreams")]
    stub_upstreams(&mut reel, &mut subs);
    let scout_settings =
        || den_scout::deps::settings_from_env(|name| std::env::var(name).unwrap_or_default());
    let scout_mount = env::scoped("SCOUT_", Some("scout"), scout_settings);
    let mut scout_own = env::scoped("SCOUT_", None, scout_settings);
    // The startup line names the port Scout answers on.
    if let Some((_, p)) = own.iter().find(|(s, _)| *s == Service::Scout) {
        scout_own.port = p.to_string();
    } else {
        scout_own.port = format!("{port}/scout");
    }

    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(threads)
        .enable_all()
        .build()
        .expect("tokio runtime");
    let configs = Configs { port, own, reel, subs, remux, scout_mount, scout_own };
    if let Err(e) = rt.block_on(run(configs)) {
        eprintln!("fatal: {e}");
        std::process::exit(1);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn hearth() -> Arc<Hearth> {
        // No disk tier: a test must not write the temp dir.
        let settings = || {
            let mut s = den_scout::deps::settings_from_env(|_| String::new());
            s.cache_dir = String::new();
            s
        };
        Arc::new(Hearth {
            reel: den_reel::AppState::new(den_reel::Config::from_env()),
            subs: den_subtitles::AppState::new(den_subtitles::Config::from_env()),
            remux: den_remux::AppState::new(den_remux::Config::from_env()),
            scout: scout::Scout::new(settings(), settings()),
        })
    }

    async fn get_on(own: Option<Service>, path: &str) -> (StatusCode, serde_json::Value) {
        let req = Request::get(path).body(Empty::<Bytes>::new()).unwrap();
        let resp = handle(hearth(), own, req).await;
        let status = resp.status();
        let bytes = resp.into_body().collect().await.unwrap().to_bytes();
        (status, serde_json::from_slice(&bytes).unwrap_or(serde_json::Value::Null))
    }

    async fn get(path: &str) -> (StatusCode, serde_json::Value) {
        get_on(None, path).await
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
