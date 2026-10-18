//! TorBox HTTP response lifetime, body decoding, and fixed endpoint metrics.

use tokio::sync::OwnedSemaphorePermit;

use crate::metrics::metrics;
use crate::stores::{read_limited, read_store_error, redact_token, MAX_STORE_BYTES};
use crate::util::Shape;

use super::support::first_value;
use super::{build_request, encode_form, TorBoxStore};
use crate::scrape::DoError;

impl TorBoxStore {
    /// Go `torBoxStore.get`.
    pub(super) async fn get(&self, u: &str) -> Result<Reply, DoError> {
        let auth = format!("Bearer {}", self.token);
        let req = build_request(
            reqwest::Method::GET,
            u,
            &[("authorization", &auth), ("accept", "application/json")],
            None,
        )
        .map_err(|e| DoError::from(e.to_string()))?;
        self.send(req).await
    }

    /// Go `torBoxStore.do`: every TorBox request waits for an outbound slot first. The call is counted by endpoint
    /// and outcome (Go does it beside the request in `get`, `addMagnet` and `controlRequest`; it is the same count).
    pub(super) async fn send(&self, req: reqwest::Request) -> Result<Reply, DoError> {
        let mut count = CallCount::new(req.url());
        let runtime = crate::runtime::current_runtime();
        let slot = runtime
            .torbox
            .outbound
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| DoError::from("the outbound limiter is closed"))?;
        let resp = self.client.do_request(req).await;
        count.succeeded(resp.as_ref().is_ok_and(|r| r.status().is_success()));
        Ok(Reply::new(resp?, slot))
    }

    /// Go `torBoxStore.post`: a form POST, authorised by header.
    pub(super) async fn post(&self, path: &str, form: &[(&str, &str)]) -> Result<Reply, DoError> {
        let auth = format!("Bearer {}", self.token);
        let req = build_request(
            reqwest::Method::POST,
            &format!("{}{path}", self.api),
            &[("authorization", &auth), ("content-type", "application/x-www-form-urlencoded")],
            Some(encode_form(form)),
        )
        .map_err(|e| DoError::from(e.to_string()))?;
        self.send(req).await
    }
}

pub(super) struct Reply {
    resp: reqwest::Response,
    _slot: OwnedSemaphorePermit,
}

impl Reply {
    pub(super) fn new(resp: reqwest::Response, slot: OwnedSemaphorePermit) -> Self {
        Self { resp, _slot: slot }
    }

    pub(super) fn status(&self) -> u16 {
        self.resp.status().as_u16()
    }

    pub(super) async fn read(&mut self, limit: usize) -> Result<Vec<u8>, reqwest::Error> {
        read_limited(&mut self.resp, limit).await
    }

    pub(super) async fn chunk(&mut self) -> Result<Option<bytes::Bytes>, reqwest::Error> {
        self.resp.chunk().await
    }

    pub(super) async fn json(&mut self, shape: &Shape) -> Option<serde_json::Value> {
        let raw = self.read(MAX_STORE_BYTES).await.ok()?;
        first_value(&raw, shape)
    }

    pub(super) async fn detail(&mut self, token: &str) -> String {
        redact_token(&read_store_error(&mut self.resp).await, token)
    }
}

pub(super) struct CallCount {
    endpoint: &'static str,
    ok: bool,
}

impl CallCount {
    pub(super) fn new(url: &reqwest::Url) -> Self {
        Self { endpoint: endpoint(url), ok: false }
    }

    pub(super) fn succeeded(&mut self, ok: bool) {
        self.ok = ok;
    }
}

impl Drop for CallCount {
    fn drop(&mut self) {
        metrics().torbox_call(self.endpoint, self.ok);
    }
}

fn endpoint(url: &reqwest::Url) -> &'static str {
    let path = url.path();
    if path.ends_with("/torrents/checkcached") {
        "checkcached"
    } else if path.ends_with("/torrents/mylist")
        && url.query_pairs().any(|(key, value)| key == "id" && !value.is_empty())
    {
        "mylist_id"
    } else if path.ends_with("/torrents/mylist") {
        "mylist_account"
    } else if path.ends_with("/torrents/requestdl") {
        "requestdl"
    } else if path.ends_with("/torrents/createtorrent") {
        "createtorrent"
    } else if path.ends_with("/torrents/controltorrent") {
        "controltorrent"
    } else if path.ends_with("/queued/controlqueued") {
        "controlqueued"
    } else if path.ends_with("/queued/getqueued") {
        "getqueued"
    } else if path.ends_with("/user/me") {
        "user"
    } else {
        ""
    }
}
