//! Port of `internal/scout/handler.go`: the runtime-agnostic core as an HTTP service (ported from
//! src/handler.ts). Off-device by design: the app never sees a torrent or a debrid token. Routes are served
//! at the service root. Work package 4.
//!
//! Tests-first skeleton: the signatures the tests call, with `todo!()` bodies. The Go `http.Handler` becomes
//! `Handler::serve`, which takes an `http::Request` and returns an `http::Response`; cancellation is dropping
//! the future, so the Go request context is gone.

use std::time::Duration;

use crate::deps::Deps;

/// Go `staticCache`.
pub const STATIC_CACHE: &str = "public, max-age=3600, stale-while-revalidate=600";
/// Go `noStore`.
pub const NO_STORE: &str = "no-store";
/// Go `jsonType`.
pub const JSON_TYPE: &str = "application/json";
/// Go `defaultTimeout`.
pub const DEFAULT_TIMEOUT: Duration = Duration::from_secs(8);
/// Go `defaultListTTL`.
pub const DEFAULT_LIST_TTL: Duration = Duration::from_secs(5 * 60);
/// Go `partialListTTL`: how long a list is kept when an indexer did not answer.
pub const PARTIAL_LIST_TTL: Duration = Duration::from_secs(60);
/// Go `scrapeFailThreshold`: after this many consecutive builds where no indexer responded, /health says so.
pub const SCRAPE_FAIL_THRESHOLD: i32 = 3;

/// Go `etagFor`: the quoted 64-bit ETag of a body.
pub fn etag_for(body: &str) -> String {
    todo!("handler.go etagFor")
}

/// Go `handler`: the HTTP service the scout routes are served from. Built by `new_handler`.
pub struct Handler {
    deps: Deps,
}

/// Go `NewHandler`: builds the scout HTTP handler.
pub fn new_handler(deps: Deps) -> Handler {
    todo!("handler.go NewHandler")
}

impl Handler {
    /// Serves one request, as Go's `http.Handler.ServeHTTP` does. The response body is buffered.
    pub async fn serve(&self, req: http::Request<Vec<u8>>) -> http::Response<Vec<u8>> {
        todo!("handler.go handler.ServeHTTP")
    }
}
