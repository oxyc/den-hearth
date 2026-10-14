//! Scout as a hearth component: the outbound client, the cache, the two handlers, and the bounded body read in
//! front of them.
//!
//! Scout is a library (`handler::new_handler`) with no server of its own. What Go's `cmd/den-scout/main.go`
//! did around it is done here: a pooled client that refuses to replay a body across a redirect, a tiered
//! cache with an hourly sweep, a bounded request read.
//!
//! There are TWO handlers over one client and one cache. They differ only in the public origin they put in
//! absolute URLs: the one behind `/scout` keeps that prefix (`HEARTH_PUBLIC_BASE_URL/scout`), the one on
//! Scout's own listener uses `SCOUT_PUBLIC_BASE_URL` as-is, which is what the old container produced.

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use http_body_util::{BodyExt, Limited};
use hyper::{Request, Response, StatusCode};

use den_scout::cache::Cache;
use den_scout::deps::{build_deps_shared, shared_http, startup_summary, GuardedClient, Settings};
use den_scout::diskcache::{TieredCache, SWEEP_INTERVAL};
use den_scout::handler::{new_handler, Handler};
use den_scout::scrape::Doer;

use crate::Body;

/// Go's `ReadTimeout`: the whole request, body included.
const READ_TIMEOUT: Duration = Duration::from_secs(15);
/// Go's MaxHeaderBytes: twice the largest config segment a URL legitimately carries.
pub const MAX_HEAD: usize = 32 << 10;
/// The largest request body read. The one route that reads a body (`/validate`) looks at 4 KiB of it.
const MAX_BODY: usize = 64 << 10;

pub struct Scout {
    mount: Handler,
    legacy: Handler,
    cache: Arc<TieredCache>,
    summary: String,
}

/// Go's pooled keep-alive client (cmd/den-scout/main.go), built once for both handlers so they share one memory of
/// which upstreams asked to be left alone. A redirect is refused where it would replay a body or hand a credential
/// to another host (see `GuardedClient`).
fn client() -> Arc<dyn Doer> {
    let guarded = GuardedClient::new(|| {
        reqwest::Client::builder()
            .timeout(Duration::from_secs(30))
            .pool_max_idle_per_host(32)
            .pool_idle_timeout(Duration::from_secs(90))
    })
    .expect("scout http client");
    shared_http(guarded)
}

impl Scout {
    /// `mount` configures the handler behind `/scout`, `legacy` the one on Scout's own port. They must
    /// agree on everything but `public_url`. Call from inside the runtime.
    pub fn new(mount: Settings, legacy: Settings) -> Scout {
        let cache = Arc::new(TieredCache::new(legacy.cache_bytes, std::path::Path::new(&legacy.cache_dir)));
        let client = client();
        let handler = |settings: &Settings| {
            new_handler(build_deps_shared(settings, client.clone(), cache.clone() as Arc<dyn Cache>))
        };
        let summary = startup_summary(&legacy, cache.persistent().unwrap_or(false));
        Scout { mount: handler(&mount), legacy: handler(&legacy), cache, summary }
    }

    /// Scout's own startup line, as Go printed it.
    pub fn summary(&self) -> &str {
        &self.summary
    }

    /// The hourly sweep that gives the disk tier a ceiling. Supervised: a panic in it is said, not silent,
    /// and does not touch a request.
    pub fn start_sweep(&self) {
        let cache = self.cache.clone();
        let task = tokio::spawn(async move { cache.sweep_every(SWEEP_INTERVAL).await });
        tokio::spawn(async move {
            if let Err(e) = task.await {
                if e.is_panic() {
                    eprintln!("scout: cache sweep panicked ({e}); the disk tier is no longer swept");
                }
            }
        });
    }

    /// Serve one request already routed to Scout (its path is final). `own_listener` picks the handler
    /// whose public origin has no prefix.
    pub async fn serve<B>(&self, own_listener: bool, req: Request<B>) -> Response<Body>
    where
        B: hyper::body::Body<Data = Bytes> + Send + 'static,
        B::Error: Into<Box<dyn std::error::Error + Send + Sync>>,
    {
        let (parts, body) = req.into_parts();
        // Scout's own listener caps the head at the connection (`max_buf_size`); the shared one cannot,
        // since the other services' heads are not Scout's, so the same ceiling is applied here.
        if !own_listener && head_len(&parts) > MAX_HEAD {
            return error(StatusCode::REQUEST_HEADER_FIELDS_TOO_LARGE, "header_too_large");
        }
        let body = match tokio::time::timeout(READ_TIMEOUT, Limited::new(body, MAX_BODY).collect()).await {
            Ok(Ok(collected)) => collected.to_bytes(),
            Ok(Err(_)) => return error(StatusCode::PAYLOAD_TOO_LARGE, "body_too_large"),
            Err(_) => return error(StatusCode::REQUEST_TIMEOUT, "request_timeout"),
        };
        let handler = if own_listener { &self.legacy } else { &self.mount };
        handler.serve(Request::from_parts(parts, body)).await
    }

    /// Scout's own `/health`, for hearth's combined one.
    pub async fn health(&self) -> Response<Body> {
        self.mount.serve(Request::get("/health").body(Bytes::new()).expect("static request")).await
    }
}

/// The size of the request line and headers as they went over the wire, near enough to compare with
/// `MAX_HEAD`.
fn head_len(parts: &hyper::http::request::Parts) -> usize {
    let line = parts.method.as_str().len() + parts.uri.to_string().len() + " HTTP/1.1\r\n".len();
    parts.headers.iter().fold(line, |sum, (name, value)| sum + name.as_str().len() + value.len() + 4)
}

fn error(status: StatusCode, name: &str) -> Response<Body> {
    crate::json(status, &serde_json::json!({ "error": name }))
}
