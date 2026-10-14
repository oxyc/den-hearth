//! Scout as a hearth component: the outbound client, the cache, the two handlers, and the bounded body read in
//! front of them.
//!
//! Scout is a library (`handler::new_handler`) with no server of its own. What Go's `cmd/den-scout/main.go`
//! did around it is done here: a pooled client that refuses to replay a body across a redirect, a tiered
//! cache with an hourly sweep, a bounded request read.
//!
//! There are THREE handlers over one client and one cache. Two differ only in the public origin they put in
//! absolute URLs: the one behind `/scout` keeps that prefix (`HEARTH_PUBLIC_BASE_URL/scout`), the one on
//! Scout's own listener uses `SCOUT_PUBLIC_BASE_URL` as-is, which is what the old container produced. The
//! third is the combined Den addon's (`den.rs`), on that addon's keys, revocation and base.

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use http_body_util::{BodyExt, Limited};
use hyper::{Request, Response, StatusCode};

use den_scout::cache::Cache;
use den_scout::deps::{
    build_deps_shared, shared_http, shared_profile, startup_summary, GuardedClient, Settings,
};
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
    /// The combined Den addon's (`/den`): `mount`'s settings, on that addon's keys and revocation, reading its
    /// Scout member. Its play tickets are keyed from those keys, so they open only here.
    den: Handler,
    cache: Arc<TieredCache>,
    summary: String,
}

/// Go's pooled keep-alive client (cmd/den-scout/main.go), built once for both handlers so they share one memory of
/// which upstreams asked to be left alone. A redirect is refused where it would replay a body or hand a credential
/// to another host (see `GuardedClient`).
fn client() -> Arc<dyn Doer> {
    let guarded = GuardedClient::from_profile(&shared_profile()).expect("scout http client");
    shared_http(guarded)
}

impl Scout {
    /// `mount` configures the handler behind `/scout`, `legacy` the one on Scout's own port. They must
    /// agree on everything but `public_url`. `den` is the combined Den addon's terms for Scout's member.
    /// Call from inside the runtime.
    pub fn new(mount: Settings, legacy: Settings, den: &den_seal::Addon) -> Scout {
        let cache = Arc::new(TieredCache::new(legacy.cache_bytes, std::path::Path::new(&legacy.cache_dir)));
        let client = client();
        let deps = |settings: &Settings| {
            build_deps_shared(settings, client.clone(), cache.clone() as Arc<dyn Cache>)
        };
        let mut den_deps = deps(&mount);
        den_deps.seal_keyring = den.keyring.clone().map(Arc::new);
        den_deps.config_key_invalid = den.key_invalid;
        den_deps.revocation = den.revocation.clone();
        den_deps.config_slice = Some(den.slice);
        den_deps.public_url = den.public_base.clone().unwrap_or_default();
        den_deps.mount = den_seal::addon::MOUNT;
        // No list from before the Den addon can name a `/<config>/play` URL, so with tickets on that route is
        // closed from the start; without them (no key) it is the only play route there is.
        den_deps.legacy_play_until = den.keyring.as_ref().map(|_| std::time::UNIX_EPOCH);
        let summary = startup_summary(&legacy, cache.persistent().unwrap_or(false));
        Scout {
            mount: new_handler(deps(&mount)),
            legacy: new_handler(deps(&legacy)),
            den: new_handler(den_deps),
            cache,
            summary,
        }
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
        let handler = if own_listener { &self.legacy } else { &self.mount };
        Self::bounded(handler, !own_listener, req).await
    }

    /// Serve one request the combined Den addon routed to Scout (its path is final), on the main port.
    pub async fn serve_den<B>(&self, req: Request<B>) -> Response<Body>
    where
        B: hyper::body::Body<Data = Bytes> + Send + 'static,
        B::Error: Into<Box<dyn std::error::Error + Send + Sync>>,
    {
        Self::bounded(&self.den, true, req).await
    }

    /// `handler` serving `req` after the head and body bounds. `shared_listener`: the connection is not
    /// Scout's own, so its head was not capped there.
    async fn bounded<B>(handler: &Handler, shared_listener: bool, req: Request<B>) -> Response<Body>
    where
        B: hyper::body::Body<Data = Bytes> + Send + 'static,
        B::Error: Into<Box<dyn std::error::Error + Send + Sync>>,
    {
        let (parts, body) = req.into_parts();
        // Scout's own listener caps the head at the connection (`max_buf_size`); the shared one cannot,
        // since the other services' heads are not Scout's, so the same ceiling is applied here.
        if shared_listener && head_len(&parts) > MAX_HEAD {
            return error(StatusCode::REQUEST_HEADER_FIELDS_TOO_LARGE, "header_too_large");
        }
        let body = match tokio::time::timeout(READ_TIMEOUT, Limited::new(body, MAX_BODY).collect()).await {
            Ok(Ok(collected)) => collected.to_bytes(),
            Ok(Err(_)) => return error(StatusCode::PAYLOAD_TOO_LARGE, "body_too_large"),
            Err(_) => return error(StatusCode::REQUEST_TIMEOUT, "request_timeout"),
        };
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
