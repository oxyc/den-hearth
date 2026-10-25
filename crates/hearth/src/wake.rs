//! Authenticated installed-TV registration for the local Apple TV wake controller.
//!
//! The config segment is the install credential. Hearth opens and admits it before reading the small body, then
//! forwards only the target registration over a permissioned Unix socket. Controller state and Apple TV pairing
//! credentials remain in the sidecar and are never mounted into this process.

use std::time::Duration;

use bytes::Bytes;
use http_body_util::{BodyExt, Limited};
use hyper::{Method, Request, Response, StatusCode};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::UnixStream;

use crate::{Body, Hearth};

const MAX_BODY: usize = 8 * 1024;
const MAX_REPLY: u64 = 8 * 1024 + 1;
const BODY_TIMEOUT: Duration = Duration::from_secs(5);
const IPC_TIMEOUT: Duration = Duration::from_secs(3);

#[derive(Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct Target {
    v: u8,
    device_id: String,
    name: String,
    household_inbox: String,
    assistant_lane: String,
    bundle_id: String,
}

impl Target {
    fn valid(&self) -> bool {
        self.v == 1
            && lower_hex(&self.device_id, 16)
            && label(&self.name)
            && lower_hex(&self.household_inbox, 64)
            && lower_hex(&self.assistant_lane, 32)
            && bundle(&self.bundle_id)
    }
}

fn lower_hex(value: &str, len: usize) -> bool {
    value.len() == len && value.bytes().all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn label(value: &str) -> bool {
    let canonical = value.split_whitespace().collect::<Vec<_>>().join(" ");
    !value.is_empty()
        && value.chars().count() <= 40
        && canonical == value
        && !value.chars().any(char::is_control)
}

fn bundle(value: &str) -> bool {
    let bytes = value.as_bytes();
    (2..=255).contains(&bytes.len())
        && bytes[0].is_ascii_alphanumeric()
        && bytes.iter().all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'-'))
}

fn answer(status: StatusCode, error: &str) -> Response<Body> {
    den_http::json(status, &json!({"error": error}), &[("cache-control", "no-store")])
}

pub(crate) async fn handle<B>(hearth: &Hearth, config: &str, req: Request<B>) -> Response<Body>
where
    B: hyper::body::Body<Data = Bytes> + Send + 'static,
    B::Error: Into<Box<dyn std::error::Error + Send + Sync>>,
{
    if req.method() != Method::PUT {
        return den_http::json(
            StatusCode::METHOD_NOT_ALLOWED,
            &json!({"error": "method_not_allowed"}),
            &[("allow", "PUT"), ("cache-control", "no-store")],
        );
    }
    if hearth.den.scout.key_invalid {
        return answer(StatusCode::SERVICE_UNAVAILABLE, "config_key_invalid");
    }
    if hearth.den.open(config).is_err() {
        return answer(StatusCode::BAD_REQUEST, "bad_config");
    }
    let bytes =
        match tokio::time::timeout(BODY_TIMEOUT, Limited::new(req.into_body(), MAX_BODY).collect()).await {
            Ok(Ok(body)) => body.to_bytes(),
            Ok(Err(_)) => return answer(StatusCode::PAYLOAD_TOO_LARGE, "body_too_large"),
            Err(_) => return answer(StatusCode::REQUEST_TIMEOUT, "body_timeout"),
        };
    let Ok(target) = serde_json::from_slice::<Target>(&bytes) else {
        return answer(StatusCode::BAD_REQUEST, "invalid_target");
    };
    if !target.valid() {
        return answer(StatusCode::BAD_REQUEST, "invalid_target");
    }
    match forward(&hearth.controller_socket, &target).await {
        Ok(()) => Response::builder()
            .status(StatusCode::NO_CONTENT)
            .header("cache-control", "no-store")
            .body(den_http::full(""))
            .expect("static response"),
        Err(Forward::Rejected(status, error)) => answer(status, error),
        Err(Forward::Unavailable) => answer(StatusCode::SERVICE_UNAVAILABLE, "controller_unavailable"),
    }
}

enum Forward {
    Rejected(StatusCode, &'static str),
    Unavailable,
}

async fn forward(path: &std::path::Path, target: &Target) -> Result<(), Forward> {
    let request =
        serde_json::to_vec(&json!({"op": "upsert", "target": target})).map_err(|_| Forward::Unavailable)?;
    let exchange = async {
        let mut stream = UnixStream::connect(path).await?;
        stream.write_all(&request).await?;
        stream.write_all(b"\n").await?;
        stream.shutdown().await?;
        let mut reply = Vec::new();
        stream.take(MAX_REPLY).read_to_end(&mut reply).await?;
        Ok::<_, std::io::Error>(reply)
    };
    let reply = tokio::time::timeout(IPC_TIMEOUT, exchange)
        .await
        .map_err(|_| Forward::Unavailable)?
        .map_err(|_| Forward::Unavailable)?;
    if reply.len() > MAX_BODY || !reply.ends_with(b"\n") {
        return Err(Forward::Unavailable);
    }
    let value: Value = serde_json::from_slice(&reply).map_err(|_| Forward::Unavailable)?;
    if value.get("ok") == Some(&Value::Bool(true)) {
        return Ok(());
    }
    let (status, error) = match value.get("error").and_then(Value::as_str) {
        Some("invalid_device") => (StatusCode::BAD_REQUEST, "invalid_device"),
        Some("invalid_name") => (StatusCode::BAD_REQUEST, "invalid_name"),
        Some("invalid_inbox") => (StatusCode::BAD_REQUEST, "invalid_inbox"),
        Some("invalid_assistant") => (StatusCode::BAD_REQUEST, "invalid_assistant"),
        Some("invalid_bundle") => (StatusCode::BAD_REQUEST, "invalid_bundle"),
        Some("targets_full") => (StatusCode::CONFLICT, "targets_full"),
        Some("target_conflict") => (StatusCode::CONFLICT, "target_conflict"),
        _ => return Err(Forward::Unavailable),
    };
    Err(Forward::Rejected(status, error))
}
