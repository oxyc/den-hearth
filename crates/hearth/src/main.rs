//! den-hearth — Den's self-hosted media node: Reel, Subtitles and Remux in one process.
//!
//!   /reel/...   → den-reel      (`/reel/play/<id>.mp4` is Reel's `/play/<id>.mp4`)
//!   /subs/...   → den-subtitles (`/subs/<config>/manifest.json` is Subtitles' `/<config>/manifest.json`)
//!   /remux/...  → den-remux     (unchanged: Remux already serves everything under `/remux`)
//!   /health     → all three, each as its own service reports it
//!
//! Each service is the same library its standalone binary runs; hearth supplies the listener, the
//! runtime and the signal handling, and gives each service its own environment (see `env`).

mod env;
mod route;

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

use route::Target;

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
async fn handle<B>(hearth: Arc<Hearth>, req: Request<B>) -> Response<Body>
where
    B: hyper::body::Body<Data = Bytes> + Send + 'static,
    B::Error: Into<Box<dyn std::error::Error + Send + Sync>>,
{
    match route::target(req.uri().path()) {
        Target::Health => json(StatusCode::OK, &health(&hearth).await),
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
    hearth: Arc<Hearth>,
    shutdown: impl Future<Output = ()>,
    grace: Duration,
) -> bool {
    let graceful = GracefulShutdown::new();
    tokio::pin!(shutdown);
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
            _ = &mut shutdown => break,
        };
        let hearth = hearth.clone();
        let peer = den_remux::Peer(peer.ip());
        let service = service_fn(move |mut req: Request<Incoming>| {
            // Remux decides who is a trusted proxy and who is the visitor from the socket's address.
            req.extensions_mut().insert(peer);
            let hearth = hearth.clone();
            async move { Ok::<_, Infallible>(handle(hearth, req).await) }
        });
        let conn = hyper::server::conn::http1::Builder::new()
            .timer(TokioTimer::new())
            .header_read_timeout(HEADER_READ_TIMEOUT)
            .serve_connection(TokioIo::new(stream), service);
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

async fn run(
    port: u16,
    reel: den_reel::Config,
    subs: den_subtitles::Config,
    remux: den_remux::Config,
) -> std::io::Result<()> {
    let hearth = Arc::new(Hearth {
        reel: den_reel::init(reel),
        subs: den_subtitles::init(subs),
        remux: den_remux::init(remux).await,
    });
    let shutdown = shutdown_signal();
    let listener = TcpListener::bind(("0.0.0.0", port)).await?;
    eprintln!(
        "den-hearth {} listening on :{port} — /reel (den-reel {}), /subs (den-subtitles {}), /remux (den-remux {})",
        env!("CARGO_PKG_VERSION"),
        den_reel::release_version(),
        den_subtitles::release_version(),
        den_remux::release_version(),
    );

    let drained = serve_until(listener, hearth.clone(), shutdown, DRAIN_GRACE).await;

    den_reel::finish(&hearth.reel).await;
    den_remux::finish(&hearth.remux).await;
    if drained {
        eprintln!("shut down cleanly");
    }
    Ok(())
}

fn main() {
    // Read before the runtime exists: the services read their configuration from the environment
    // inside `from_env`, and `env::scoped` edits the environment to give each its own.
    let port = std::env::var("PORT").ok().and_then(|v| v.parse().ok()).unwrap_or(DEFAULT_PORT);
    let threads = std::env::var("HEARTH_WORKER_THREADS").ok().and_then(|v| v.parse().ok()).unwrap_or(2);
    let reel = env::scoped("REEL_", Some("reel"), den_reel::Config::from_env);
    let subs = env::scoped("SUBS_", Some("subs"), den_subtitles::Config::from_env);
    let remux = env::scoped("REMUX_", None, den_remux::Config::from_env);

    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(threads)
        .enable_all()
        .build()
        .expect("tokio runtime");
    if let Err(e) = rt.block_on(run(port, reel, subs, remux)) {
        eprintln!("fatal: {e}");
        std::process::exit(1);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn hearth() -> Arc<Hearth> {
        Arc::new(Hearth {
            reel: den_reel::AppState::new(den_reel::Config::from_env()),
            subs: den_subtitles::AppState::new(den_subtitles::Config::from_env()),
            remux: den_remux::AppState::new(den_remux::Config::from_env()),
        })
    }

    async fn get(path: &str) -> (StatusCode, serde_json::Value) {
        let req = Request::get(path).body(Empty::<Bytes>::new()).unwrap();
        let resp = handle(hearth(), req).await;
        let status = resp.status();
        let bytes = resp.into_body().collect().await.unwrap().to_bytes();
        (status, serde_json::from_slice(&bytes).unwrap_or(serde_json::Value::Null))
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
    async fn health_reports_all_three() {
        let (status, body) = get("/health").await;
        assert_eq!(status, StatusCode::OK);
        for service in ["reel", "subs", "remux"] {
            assert!(body["services"][service]["status"].is_string(), "{service}: {body}");
        }
        let all_ok = ["reel", "subs", "remux"].iter().all(|s| body["services"][s]["status"] == "ok");
        assert_eq!(body["status"], if all_ok { "ok" } else { "degraded" });
    }
}
