//! Go: `internal/scout/mock_test.go` (18 lines). Work package 6, and the first thing it lands.
//!
//! Not a test: the shared helpers (`mockDoer`, `resp`) every other test module uses. A `Doer` that
//! routes requests to a closure, and a `reqwest::Response` built from a status and a body. Every other
//! work package waits on this one file, so it goes in before anything else.

use async_trait::async_trait;

use crate::scrape::{DoError, Doer};

/// mockDoer routes requests to a test function.
pub(crate) struct MockDoer<F>(pub(crate) F)
where
    F: Fn(reqwest::Request) -> Result<reqwest::Response, DoError> + Send + Sync;

#[async_trait]
impl<F> Doer for MockDoer<F>
where
    F: Fn(reqwest::Request) -> Result<reqwest::Response, DoError> + Send + Sync,
{
    async fn do_request(&self, req: reqwest::Request) -> Result<reqwest::Response, DoError> {
        (self.0)(req)
    }
}

pub(crate) fn resp(status: u16, body: &str) -> reqwest::Response {
    let r = http::Response::builder().status(status).body(body.to_owned()).expect("valid status");
    reqwest::Response::from(r)
}
