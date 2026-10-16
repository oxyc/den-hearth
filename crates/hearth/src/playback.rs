//! One playback decision for the combined Den addon.
//!
//! This is deliberately a facade, not another session implementation. It derives the private Scout and
//! Subtitles installs from the selected combined config, asks the existing Remux library to create its ordinary
//! session, and translates that answer into the small `/playback` contract. Managed-provider adapters may return
//! a URL before this fallback, but never own a second lifecycle.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use http_body_util::BodyExt;
use hyper::header::{HeaderValue, CONTENT_LENGTH, CONTENT_TYPE};
use hyper::{HeaderMap, Method, Request, Response, StatusCode, Uri};
use serde_json::{json, Value};
use tokio::sync::Mutex;

use crate::{Body, Hearth};

/// A name that cannot resolve on the network. `inproc` registers the `/den` base and serves only the Scout and
/// Subtitles resource slices used by Remux.
pub const DEN_SCOUT_ORIGIN: &str = "http://den-hearth.invalid";
pub const DEN_ADDON_BASE: &str = "http://den-hearth.invalid/den";

const MAX_BODY: usize = 16 * 1024;
const MAX_KEY: usize = 128;
const MAX_RECORDS: usize = 256;
const REPLAY_FOR: Duration = Duration::from_secs(120);
// Existing Remux may spend 60 s resolving, 30 s choosing and 20 s opening an already-started candidate. This
// outer bound stops abandoned work without shortening that retained path.
const OPERATION_TIMEOUT: Duration = Duration::from_secs(120);
const MANAGED_PREPARE_TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Clone)]
struct Snapshot {
    status: StatusCode,
    headers: HeaderMap,
    body: Bytes,
}

impl Snapshot {
    fn response(&self) -> Response<Body> {
        let mut response = Response::new(den_http::full(self.body.clone()));
        *response.status_mut() = self.status;
        *response.headers_mut() = self.headers.clone();
        response
    }
}

struct Record {
    request: Bytes,
    gate: Arc<Mutex<()>>,
    completed: Option<(Instant, Snapshot)>,
}

fn retain_live_records(records: &mut HashMap<(String, String, String), Record>, now: Instant) {
    records.retain(|_, record| match &record.completed {
        Some((at, _)) => now.duration_since(*at) <= REPLAY_FOR,
        // The record itself owns one Arc. Any handler executing or waiting on this operation owns another.
        // If none does, its request was cancelled before it could publish an answer; reclaim the reservation so
        // disconnected clients cannot permanently fill the bounded replay table.
        None => Arc::strong_count(&record.gate) > 1,
    });
}

/// Bounded, process-local replay state. Signed URLs and idempotency keys are neither persisted nor logged.
pub struct Playback {
    enabled: bool,
    records: Mutex<HashMap<(String, String, String), Record>>,
}

impl Playback {
    pub fn new(enabled: bool) -> Self {
        Self { enabled, records: Mutex::new(HashMap::new()) }
    }

    pub fn summary(&self) -> &'static str {
        if self.enabled {
            "on"
        } else {
            "off"
        }
    }

    #[cfg(test)]
    pub(crate) async fn seed_completed_for_test(&self, owner: &str, config: &str, key: &str) {
        self.records.lock().await.insert(
            (owner.to_string(), config.to_string(), key.to_string()),
            Record {
                request: Bytes::new(),
                gate: Arc::new(Mutex::new(())),
                completed: Some((
                    Instant::now(),
                    Snapshot {
                        status: StatusCode::OK,
                        headers: HeaderMap::new(),
                        body: Bytes::from_static(br#"{"url":"should-not-replay"}"#),
                    },
                )),
            },
        );
    }
}

fn answer(status: StatusCode, error: &str, retry_after: Option<&'static str>) -> Response<Body> {
    let mut response = den_http::json(status, &json!({"error": error}), &[("cache-control", "no-store")]);
    if let Some(seconds) = retry_after {
        response.headers_mut().insert("retry-after", HeaderValue::from_static(seconds));
    }
    stamp(&mut response);
    response
}

fn stamp(response: &mut Response<Body>) {
    response.headers_mut().insert("cache-control", HeaderValue::from_static("no-store"));
    den_http::cors_stamp(response.headers_mut(), "Retry-After, Server-Timing");
}

fn preflight() -> Response<Body> {
    let mut response = Response::builder()
        .status(StatusCode::NO_CONTENT)
        .header("access-control-allow-methods", "POST, OPTIONS")
        .header("access-control-allow-headers", "content-type, idempotency-key")
        .header("access-control-max-age", "86400")
        .header("cache-control", "no-store")
        .body(den_http::full(""))
        .expect("static response");
    stamp(&mut response);
    response
}

fn content_type_is_json(headers: &HeaderMap) -> bool {
    headers.get(CONTENT_TYPE).and_then(|v| v.to_str().ok()).is_some_and(|v| {
        v.split(';').next().is_some_and(|mime| mime.trim().eq_ignore_ascii_case("application/json"))
    })
}

fn idempotency_key(headers: &HeaderMap) -> Option<String> {
    let value = headers.get("idempotency-key")?.to_str().ok()?;
    (!value.is_empty() && value.len() <= MAX_KEY && value.bytes().all(|b| b.is_ascii_graphic() && b != b','))
        .then(|| value.to_string())
}

fn trusted_edge(hearth: &Hearth, parts: &hyper::http::request::Parts) -> bool {
    let secret = parts.headers.get("x-den-edge-secret").map(HeaderValue::as_bytes);
    let secret_ok = den_remux::edge_secret_ok(hearth.remux.cfg.edge_secret.as_deref(), secret);
    let peer = parts.extensions.get::<den_remux::Peer>().map(|p| p.0);
    secret_ok && peer.is_some() && peer == hearth.remux.cfg.public_session_proxy
}

fn owner_scope(hearth: &Hearth, parts: &hyper::http::request::Parts) -> String {
    if trusted_edge(hearth, parts) {
        if let Some(owner) = parts.headers.get("x-den-owner").and_then(|v| v.to_str().ok()) {
            if den_remux::is_grant_owner(owner) {
                return owner.to_string();
            }
        }
        return "edge:owner".to_string();
    }
    "local".to_string()
}

fn validate_request(mut value: Value) -> Result<(Value, bool), ()> {
    const FIELDS: &[&str] = &[
        "imdb",
        "season",
        "episode",
        "filename",
        "audio",
        "audioTrack",
        "subtitleLanguages",
        "videoCodecs",
        "playable",
        "startAt",
        "maxBitrate",
        "player",
        "exclude",
        "transcode",
        "fitsOnly",
        "replaces",
        "directFailed",
    ];
    let object = value.as_object_mut().ok_or(())?;
    if object.keys().any(|key| !FIELDS.contains(&key.as_str())) {
        return Err(());
    }
    let direct_failed = match object.remove("directFailed") {
        None => false,
        Some(Value::Bool(value)) => value,
        Some(_) => return Err(()),
    };
    let imdb = object.get("imdb").and_then(Value::as_str).ok_or(())?;
    if imdb.len() < 3
        || imdb.len() > 12
        || !imdb.starts_with("tt")
        || !imdb[2..].bytes().all(|byte| byte.is_ascii_digit())
    {
        return Err(());
    }
    let integer = |name: &str| -> Result<Option<u64>, ()> {
        match object.get(name) {
            None => Ok(None),
            Some(value) => value.as_u64().filter(|value| *value <= u32::MAX as u64).map(Some).ok_or(()),
        }
    };
    let season = integer("season")?;
    let episode = integer("episode")?;
    if season.is_some() != episode.is_some() {
        return Err(());
    }
    if object.get("filename").is_some_and(|value| !value.is_string())
        || object.get("player").is_some_and(|value| !value.is_string())
        || object.get("replaces").is_some_and(|value| !value.is_string())
        || object.get("audioTrack").is_some_and(|value| value.as_u64().is_none())
        || object.get("fitsOnly").is_some_and(|value| !value.is_boolean())
        || object.get("playable").is_some_and(|value| !value.is_object())
    {
        return Err(());
    }
    if object
        .get("startAt")
        .is_some_and(|value| value.as_f64().is_none_or(|value| !value.is_finite() || value < 0.0))
        || object.get("maxBitrate").is_some_and(|value| value.as_u64().is_none_or(|value| value == 0))
    {
        return Err(());
    }
    if object.get("transcode").is_some_and(|value| !matches!(value.as_str(), Some("allow" | "never"))) {
        return Err(());
    }
    for name in ["audio", "subtitleLanguages", "videoCodecs"] {
        if object.get(name).is_some_and(|value| {
            value.as_array().is_none_or(|values| {
                values.len() > 8
                    || values.iter().any(|value| value.as_str().is_none_or(|value| value.len() > 35))
            })
        }) {
            return Err(());
        }
    }
    if object.get("exclude").is_some_and(|value| {
        value.as_array().is_none_or(|values| {
            values.len() > 16
                || values.iter().any(|value| value.as_str().is_none_or(|value| value.len() > 512))
        })
    }) {
        return Err(());
    }
    Ok((value, direct_failed))
}

fn install_url(config: &str) -> String {
    format!("{DEN_ADDON_BASE}/{config}")
}

fn managed_request(request: &Value) -> Option<den_scout::managed_hls::ManagedHlsRequest> {
    let object = request.as_object()?;
    // TorBox v1 intentionally uses its default audio and has no Remux session to replace. Preserve explicit
    // track and replacement behavior by sending those requests to Remux.
    let excluded = object.get("exclude").and_then(Value::as_array).is_some_and(|values| !values.is_empty());
    if object.contains_key("audioTrack")
        || object.contains_key("replaces")
        || object.get("transcode").and_then(Value::as_str) == Some("never")
        || excluded
    {
        return None;
    }
    let integer = |name: &str| -> Option<Option<i64>> {
        match object.get(name) {
            None => Some(None),
            Some(value) => value.as_i64().filter(|value| *value >= 0).map(Some),
        }
    };
    let playable = match object.get("playable") {
        None => None,
        Some(value) => Some(serde_json::from_value(value.clone()).ok()?),
    };
    let filename = match object.get("filename") {
        None => None,
        Some(value) => Some(value.as_str()?.to_string()),
    };
    Some(den_scout::managed_hls::ManagedHlsRequest {
        imdb: object.get("imdb")?.as_str()?.to_string(),
        season: integer("season")?,
        episode: integer("episode")?,
        filename,
        playable,
    })
}

fn direct_response(result: den_scout::managed_hls::ManagedHlsResult, prepare_ms: u64) -> Response<Body> {
    let mut response = den_http::json(
        StatusCode::OK,
        &json!({
            "url": result.url.into_string(),
            "release": {
                "label": result.release.label,
                "filename": result.release.filename,
                "size": result.release.size,
            },
        }),
        &[("cache-control", "no-store")],
    );
    if let Ok(value) = HeaderValue::from_str(&format!("prepare;dur={prepare_ms}")) {
        response.headers_mut().insert("server-timing", value);
    }
    stamp(&mut response);
    response
}

async fn remux(
    hearth: &Arc<Hearth>,
    config: &str,
    has_subtitles: bool,
    mut parts: hyper::http::request::Parts,
    mut request: Value,
) -> Response<Body> {
    let Some(object) = request.as_object_mut() else {
        return answer(StatusCode::BAD_REQUEST, "bad_request", None);
    };
    let install = install_url(config);
    object.insert("scout".to_string(), Value::String(install.clone()));
    if has_subtitles {
        object.insert("subtitles".to_string(), Value::String(install));
    }
    let Ok(body) = serde_json::to_vec(&request) else {
        return answer(StatusCode::BAD_REQUEST, "bad_request", None);
    };
    parts.method = Method::POST;
    parts.uri = Uri::from_static("/remux/session");
    parts.headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
    parts.headers.remove(CONTENT_LENGTH);
    let response = den_remux::handle_request(
        hearth.remux.clone(),
        Request::from_parts(parts, http_body_util::Full::new(Bytes::from(body))),
    )
    .await;
    translate_remux(response).await
}

async fn translate_remux(response: Response<Body>) -> Response<Body> {
    let status = response.status();
    let retry_after = response.headers().get("retry-after").cloned();
    let server_timing = response.headers().get("server-timing").cloned();
    let bytes = match http_body_util::Limited::new(response.into_body(), MAX_BODY).collect().await {
        Ok(body) => body.to_bytes(),
        Err(_) => return answer(StatusCode::BAD_GATEWAY, "source_unavailable", None),
    };
    if status != StatusCode::CREATED {
        let (status, code, retry) = match status {
            StatusCode::BAD_REQUEST => (StatusCode::BAD_REQUEST, "bad_request", None),
            StatusCode::NOT_FOUND => (StatusCode::NOT_FOUND, "no_release", None),
            StatusCode::CONFLICT => (StatusCode::CONFLICT, "replacement_pending", None),
            StatusCode::TOO_MANY_REQUESTS => (StatusCode::TOO_MANY_REQUESTS, "playback_busy", Some("60")),
            StatusCode::GATEWAY_TIMEOUT => (StatusCode::GATEWAY_TIMEOUT, "playback_timeout", None),
            StatusCode::BAD_GATEWAY => (StatusCode::BAD_GATEWAY, "source_unavailable", None),
            _ => (StatusCode::SERVICE_UNAVAILABLE, "playback_unavailable", Some("5")),
        };
        let mut out = answer(status, code, retry);
        if let Some(value) = retry_after {
            out.headers_mut().insert("retry-after", value);
        }
        return out;
    }
    let Ok(value) = serde_json::from_slice::<Value>(&bytes) else {
        return answer(StatusCode::BAD_GATEWAY, "source_unavailable", None);
    };
    let Some(playlist) = value.get("playlist").and_then(Value::as_str) else {
        return answer(StatusCode::BAD_GATEWAY, "source_unavailable", None);
    };
    let Some(sid) = value.get("sid").and_then(Value::as_str) else {
        return answer(StatusCode::BAD_GATEWAY, "source_unavailable", None);
    };
    let base = playlist.strip_suffix("/master.m3u8").unwrap_or(playlist);
    // Preserve Remux's session payload additively. The facade owns only the outer URL/release and the names of
    // lifecycle controls; a new Remux field should not need a parallel Hearth change before Web can consume it.
    let mut session = value.as_object().cloned().unwrap_or_default();
    session.remove("sid");
    session.remove("playlist");
    session.remove("release");
    session.remove("speed");
    session.insert("id".into(), Value::String(sid.to_string()));
    if let Some(speed) = value.get("speed") {
        session.insert("speedUrl".into(), speed.clone());
    }
    session.insert("endUrl".into(), Value::String(base.to_string()));
    session.insert("heartbeatUrl".into(), Value::String(format!("{base}/heartbeat")));
    session.insert("reportUrl".into(), Value::String(format!("{base}/report")));
    let mut out = den_http::json(
        StatusCode::OK,
        &json!({"url": playlist, "release": value.get("release").cloned().unwrap_or(Value::Null), "session": session}),
        &[("cache-control", "no-store")],
    );
    if let Some(value) = server_timing {
        out.headers_mut().insert("server-timing", value);
    }
    stamp(&mut out);
    out
}

async fn snapshot(response: Response<Body>) -> Snapshot {
    let (parts, body) = response.into_parts();
    match http_body_util::Limited::new(body, MAX_BODY).collect().await {
        Ok(body) => Snapshot { status: parts.status, headers: parts.headers, body: body.to_bytes() },
        Err(_) => {
            // Every ordinary facade answer is well below this ceiling. If release metadata from an untrusted
            // indexer is not, do not retain it in the replay table: 256 individually bounded records must not
            // turn a bounded upstream list into hundreds of MiB of duplicate response data.
            let (parts, body) = answer(StatusCode::BAD_GATEWAY, "source_unavailable", None).into_parts();
            let body = body.collect().await.map(|body| body.to_bytes()).unwrap_or_default();
            Snapshot { status: parts.status, headers: parts.headers, body }
        }
    }
}

/// Serve `POST /den/<config>/playback`. The entire route is absent while its rollout switch is off.
pub async fn handle<B>(hearth: &Arc<Hearth>, config: &str, req: Request<B>) -> Response<Body>
where
    B: hyper::body::Body<Data = Bytes> + Send + 'static,
    B::Error: Into<Box<dyn std::error::Error + Send + Sync>>,
{
    if !hearth.playback.enabled {
        return den_http::not_found();
    }
    if req.method() == Method::OPTIONS {
        return preflight();
    }
    if req.method() != Method::POST {
        let mut response = answer(StatusCode::METHOD_NOT_ALLOWED, "bad_request", None);
        response.headers_mut().insert("allow", HeaderValue::from_static("POST, OPTIONS"));
        return response;
    }
    if !content_type_is_json(req.headers()) {
        return answer(StatusCode::UNSUPPORTED_MEDIA_TYPE, "unsupported_media_type", None);
    }
    let Some(key) = idempotency_key(req.headers()) else {
        return answer(StatusCode::BAD_REQUEST, "bad_request", None);
    };
    // Opening and revocation checks deliberately happen before consulting replay state.
    let install = match hearth.den.open(config) {
        Ok(install) if install.members.contains(&"scout") => install,
        _ => return answer(StatusCode::BAD_REQUEST, "bad_request", None),
    };
    let (parts, body) = req.into_parts();
    let body = match http_body_util::Limited::new(body, MAX_BODY).collect().await {
        Ok(body) => body.to_bytes(),
        Err(_) => return answer(StatusCode::BAD_REQUEST, "bad_request", None),
    };
    let parsed = match serde_json::from_slice(&body).ok().and_then(|value| validate_request(value).ok()) {
        Some(parsed) => parsed,
        None => return answer(StatusCode::BAD_REQUEST, "bad_request", None),
    };
    let (request, direct_failed) = parsed;
    let canonical =
        Bytes::from(serde_json::to_vec(&json!({"request": request, "directFailed": direct_failed})).unwrap());
    let scope = (owner_scope(hearth, &parts), config.to_string(), key);

    let gate = {
        let mut records = hearth.playback.records.lock().await;
        let now = Instant::now();
        retain_live_records(&mut records, now);
        if let Some(record) = records.get(&scope) {
            if record.request != canonical {
                return answer(StatusCode::CONFLICT, "idempotency_conflict", None);
            }
            record.gate.clone()
        } else {
            if records.len() >= MAX_RECORDS {
                return answer(StatusCode::TOO_MANY_REQUESTS, "playback_busy", Some("5"));
            }
            let gate = Arc::new(Mutex::new(()));
            records.insert(scope.clone(), Record { request: canonical, gate: gate.clone(), completed: None });
            gate
        }
    };
    let _one = gate.lock().await;
    if let Some(response) = hearth
        .playback
        .records
        .lock()
        .await
        .get(&scope)
        .and_then(|record| record.completed.as_ref().map(|(_, response)| response.response()))
    {
        return response;
    }

    let has_subtitles = install.members.contains(&"subs");
    let managed = trusted_edge(hearth, &parts)
        && parts.headers.get("x-den-managed-playback").is_some_and(|value| value.as_bytes() == b"1")
        && !direct_failed;
    let direct = managed_request(&request);
    let operation = async {
        if managed {
            if let Some(direct) = direct.as_ref() {
                let started = Instant::now();
                if let Ok(Ok(result)) =
                    tokio::time::timeout(MANAGED_PREPARE_TIMEOUT, hearth.scout.managed_hls(config, direct))
                        .await
                {
                    return direct_response(result, started.elapsed().as_millis() as u64);
                }
            }
        }
        remux(hearth, config, has_subtitles, parts, request).await
    };
    let response = match tokio::time::timeout(OPERATION_TIMEOUT, operation).await {
        Ok(response) => response,
        Err(_) => answer(StatusCode::GATEWAY_TIMEOUT, "playback_timeout", None),
    };
    let saved = snapshot(response).await;
    if let Some(record) = hearth.playback.records.lock().await.get_mut(&scope) {
        record.completed = Some((Instant::now(), saved.clone()));
    }
    saved.response()
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn body(response: Response<Body>) -> Value {
        let bytes = response.into_body().collect().await.unwrap().to_bytes();
        serde_json::from_slice(&bytes).unwrap()
    }

    #[test]
    fn request_fields_are_closed_and_direct_failed_is_not_forwarded() {
        let (value, failed) = validate_request(json!({"imdb": "tt1", "directFailed": true})).unwrap();
        assert!(failed);
        assert_eq!(value, json!({"imdb": "tt1"}));
        assert!(validate_request(json!({"imdb": "tt1", "scout": "https://secret"})).is_err());
        assert!(validate_request(json!({"imdb": "tt1", "subtitles": "https://secret"})).is_err());
        assert!(validate_request(json!({"imdb": "tt1", "future": true})).is_err());
        assert!(validate_request(json!({"imdb": "tt1", "startAt": -1})).is_err());
        assert!(validate_request(json!({"imdb": "tt1", "maxBitrate": 0})).is_err());
        assert!(validate_request(json!({"imdb": "tt1", "season": 1})).is_err());
    }

    #[test]
    fn media_type_and_idempotency_key_are_strict_but_practical() {
        let mut headers = HeaderMap::new();
        headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json; charset=utf-8"));
        headers.insert("idempotency-key", HeaderValue::from_static("play-1.abc"));
        assert!(content_type_is_json(&headers));
        assert_eq!(idempotency_key(&headers).as_deref(), Some("play-1.abc"));
        headers.insert("idempotency-key", HeaderValue::from_static("two,values"));
        assert!(idempotency_key(&headers).is_none());
    }

    #[test]
    fn replay_table_reclaims_cancelled_and_expired_operations() {
        let key = ("owner".to_string(), "install".to_string(), "attempt".to_string());
        let gate = Arc::new(Mutex::new(()));
        let active = gate.clone();
        let mut records = HashMap::from([(
            key.clone(),
            Record { request: Bytes::from_static(b"request"), gate, completed: None },
        )]);

        retain_live_records(&mut records, Instant::now());
        assert!(records.contains_key(&key), "an active handler keeps its reservation");

        drop(active);
        retain_live_records(&mut records, Instant::now());
        assert!(records.is_empty(), "a cancelled handler cannot leak one of the bounded slots");

        let response = Snapshot {
            status: StatusCode::OK,
            headers: HeaderMap::new(),
            body: Bytes::from_static(b"answer"),
        };
        let gate = Arc::new(Mutex::new(()));
        records.insert(
            key.clone(),
            Record {
                request: Bytes::from_static(b"request"),
                gate,
                completed: Some((Instant::now(), response.clone())),
            },
        );
        retain_live_records(&mut records, Instant::now());
        assert!(records.contains_key(&key), "a fresh answer remains replayable");

        records.get_mut(&key).unwrap().completed =
            Some((Instant::now().checked_sub(REPLAY_FOR + Duration::from_secs(1)).unwrap(), response));
        retain_live_records(&mut records, Instant::now());
        assert!(records.is_empty(), "a stale signed answer is not retained");
    }

    #[tokio::test]
    async fn remux_success_is_the_small_facade_contract_with_explicit_controls() {
        let response = den_http::json(
            StatusCode::CREATED,
            &json!({
                "sid": "abc",
                "playlist": "/remux/s/abc/sig/master.m3u8",
                "speed": "/remux/s/abc/sig/speed",
                "release": {"filename": "Film.mkv", "size": 42},
                "video": {"codec": "h264"},
                "expiresAt": 123,
                "audioTrack": 0,
                "audioTracks": [],
                "subtitles": []
            }),
            &[("server-timing", "resolve;dur=1")],
        );
        let response = translate_remux(response).await;
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(response.headers()["cache-control"], "no-store");
        assert_eq!(response.headers()["server-timing"], "resolve;dur=1");
        let value = body(response).await;
        assert_eq!(value["url"], "/remux/s/abc/sig/master.m3u8");
        assert_eq!(value["session"]["id"], "abc");
        assert_eq!(value["session"]["endUrl"], "/remux/s/abc/sig");
        assert_eq!(value["session"]["heartbeatUrl"], "/remux/s/abc/sig/heartbeat");
        assert_eq!(value["session"]["reportUrl"], "/remux/s/abc/sig/report");
        assert!(value.get("delivery").is_none());
        assert!(value.get("provider").is_none());
    }

    #[tokio::test]
    async fn an_in_flight_waiter_observes_the_first_operation_s_snapshot() {
        let scope = ("owner".to_string(), "install".to_string(), "attempt".to_string());
        let gate = Arc::new(Mutex::new(()));
        let playback = Arc::new(Playback::new(true));
        playback.records.lock().await.insert(
            scope.clone(),
            Record { request: Bytes::from_static(b"same"), gate: gate.clone(), completed: None },
        );

        let first = gate.lock().await;
        let waiting = {
            let playback = playback.clone();
            let scope = scope.clone();
            let gate = gate.clone();
            tokio::spawn(async move {
                let _one = gate.lock().await;
                playback
                    .records
                    .lock()
                    .await
                    .get(&scope)
                    .and_then(|record| record.completed.as_ref().map(|(_, response)| response.clone()))
            })
        };
        tokio::task::yield_now().await;
        assert!(!waiting.is_finished(), "a duplicate must wait rather than run beside the first request");

        let expected = Snapshot {
            status: StatusCode::BAD_GATEWAY,
            headers: HeaderMap::new(),
            body: Bytes::from_static(br#"{"error":"source_unavailable"}"#),
        };
        playback.records.lock().await.get_mut(&scope).unwrap().completed =
            Some((Instant::now(), expected.clone()));
        drop(first);

        let observed = waiting.await.unwrap().expect("the first operation published an answer");
        assert_eq!(observed.status, expected.status);
        assert_eq!(observed.headers, expected.headers);
        assert_eq!(observed.body, expected.body);
    }

    #[tokio::test]
    async fn remux_errors_are_redacted_to_the_stable_set() {
        let response = den_http::json(
            StatusCode::BAD_GATEWAY,
            &json!({"error": "provider_secret", "detail": "torrent 123"}),
            &[],
        );
        let response = translate_remux(response).await;
        assert_eq!(response.status(), StatusCode::BAD_GATEWAY);
        assert_eq!(body(response).await, json!({"error": "source_unavailable"}));
    }

    #[tokio::test]
    async fn remux_answer_is_bounded_while_it_is_read() {
        let response = Response::builder()
            .status(StatusCode::CREATED)
            .body(den_http::full(vec![b'x'; MAX_BODY + 1]))
            .unwrap();
        let response = translate_remux(response).await;
        assert_eq!(response.status(), StatusCode::BAD_GATEWAY);
        assert_eq!(body(response).await, json!({"error": "source_unavailable"}));
    }

    #[tokio::test]
    async fn replay_snapshot_does_not_retain_oversized_release_metadata() {
        let response = Response::builder()
            .status(StatusCode::OK)
            .body(den_http::full(vec![b'x'; MAX_BODY + 1]))
            .unwrap();
        let response = snapshot(response).await.response();
        assert_eq!(response.status(), StatusCode::BAD_GATEWAY);
        assert_eq!(body(response).await, json!({"error": "source_unavailable"}));
    }
}
