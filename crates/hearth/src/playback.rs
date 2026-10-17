//! One playback decision for the combined Den addon.
//!
//! This is deliberately a facade, not another session implementation. It derives the private Scout and
//! Subtitles installs from the selected combined config, asks the existing Remux library to create its ordinary
//! session, and translates that answer into the small `/playback` contract. Managed-provider adapters may return
//! a URL before this fallback, but never own a second lifecycle.

use std::collections::HashMap;
use std::fmt::Write as _;
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use http_body_util::BodyExt;
use hyper::header::{HeaderValue, CONTENT_LENGTH, CONTENT_TYPE};
use hyper::{HeaderMap, Method, Request, Response, StatusCode, Uri};
use serde_json::{json, Value};
use tokio::sync::Mutex;

use crate::{Body, Hearth};

static LOG: den_http::log::LimitedLog = den_http::log::LimitedLog::new();

/// A name that cannot resolve on the network. `inproc` registers the `/den` base and serves only the Scout and
/// Subtitles resource slices used by Remux.
pub const DEN_SCOUT_ORIGIN: &str = "http://den-hearth.invalid";
pub const DEN_ADDON_BASE: &str = "http://den-hearth.invalid/den";
pub const LEGACY_SCOUT_BASE: &str = "http://den-hearth.invalid/scout";
pub const LEGACY_SUBS_BASE: &str = "http://den-hearth.invalid/subs";

const MAX_BODY: usize = 16 * 1024;
// Remux's session answer names every segment as `[start, bytes]`, about 20 bytes each, so it grows with the film:
// a 2.6-hour film's 1,224 segments came to ~25 KiB and broke the old 16 KiB ceiling. This one holds ~6 hours.
const MAX_ANSWER: usize = 256 * 1024;
const MAX_KEY: usize = 128;
const MAX_RECORDS: usize = 256;
const MAX_RECORDS_PER_OWNER: usize = 32;
const REPLAY_FOR: Duration = Duration::from_secs(120);
// Existing Remux may spend 60 s resolving, 30 s choosing and 20 s opening an already-started candidate. This
// outer bound stops abandoned work without shortening that retained path.
const OPERATION_TIMEOUT: Duration = Duration::from_secs(120);
const MANAGED_PREPARE_TIMEOUT: Duration = Duration::from_secs(10);
// Match the shared outbound HTTP ceiling: an upstream cannot ask the browser to stop trying for a day.
const MAX_DIRECT_RETRY_AFTER: Duration = Duration::from_secs(60 * 60);
const FINALIZE_HEADER: &str = "x-den-playback-finalize";
const FINALIZATION_PROTOCOL_HEADER: &str = "x-den-playback-finalization";

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
    completed: Option<(Instant, ReplayState)>,
}

#[derive(Clone)]
enum ReplayState {
    Pending(Snapshot),
    Committed(Snapshot),
    Aborted,
}

impl ReplayState {
    fn response(&self) -> Response<Body> {
        match self {
            Self::Pending(_) => answer(StatusCode::SERVICE_UNAVAILABLE, "playback_pending", Some("1")),
            Self::Committed(snapshot) => snapshot.response(),
            Self::Aborted => answer(StatusCode::SERVICE_UNAVAILABLE, "source_unavailable", Some("1")),
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Finalize {
    Commit,
    Abort,
}

fn finalize_record(record: &mut Record, request: &Bytes, outcome: Finalize) -> Result<(), ()> {
    if record.request != *request {
        return Err(());
    }
    let Some((_, state)) = record.completed.as_mut() else {
        if outcome == Finalize::Abort {
            record.completed = Some((Instant::now(), ReplayState::Aborted));
            return Ok(());
        }
        return Err(());
    };
    match (&*state, outcome) {
        (ReplayState::Pending(snapshot), Finalize::Commit) => {
            *state = ReplayState::Committed(snapshot.clone());
            Ok(())
        }
        (ReplayState::Pending(_), Finalize::Abort) => {
            *state = ReplayState::Aborted;
            Ok(())
        }
        (ReplayState::Committed(_), Finalize::Commit) | (ReplayState::Aborted, Finalize::Abort) => Ok(()),
        (ReplayState::Committed(_), Finalize::Abort) | (ReplayState::Aborted, Finalize::Commit) => Err(()),
    }
}

#[derive(Debug)]
enum ReservationError {
    Conflict,
    Busy,
}

fn reserve_record(
    records: &mut HashMap<(String, String, String), Record>,
    scope: &(String, String, String),
    request: &Bytes,
) -> Result<Arc<Mutex<()>>, ReservationError> {
    if let Some(record) = records.get(scope) {
        return if record.request == *request {
            Ok(record.gate.clone())
        } else {
            Err(ReservationError::Conflict)
        };
    }
    let owner_records = records.keys().filter(|(owner, _, _)| owner == &scope.0).count();
    if owner_records >= MAX_RECORDS_PER_OWNER || records.len() >= MAX_RECORDS {
        return Err(ReservationError::Busy);
    }
    let gate = Arc::new(Mutex::new(()));
    records.insert(scope.clone(), Record { request: request.clone(), gate: gate.clone(), completed: None });
    Ok(gate)
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
    metrics: Metrics,
}

#[derive(Default)]
struct Metrics {
    direct_ok: AtomicU64,
    direct_not_found: AtomicU64,
    direct_refused: AtomicU64,
    direct_unavailable: AtomicU64,
    direct_timeout: AtomicU64,
    operation_direct: AtomicU64,
    operation_remux: AtomicU64,
    operation_error: AtomicU64,
    operation_replay: AtomicU64,
}

#[derive(Clone, Copy, Debug)]
enum DirectFailureClass {
    NotFound,
    Refused,
    Unavailable,
    Timeout,
}

#[derive(Clone, Copy, Debug)]
struct DirectFailure {
    code: &'static str,
    retry_after: Option<Duration>,
    class: DirectFailureClass,
}

impl DirectFailure {
    const TIMEOUT: Self =
        Self { code: "managed_prepare_timeout", retry_after: None, class: DirectFailureClass::Timeout };

    fn from_managed(error: den_scout::managed_hls::ManagedHlsError) -> Self {
        use den_scout::managed_hls::ManagedHlsError;

        let class = match error {
            ManagedHlsError::NotFound => DirectFailureClass::NotFound,
            ManagedHlsError::Refused | ManagedHlsError::Authentication | ManagedHlsError::Entitlement => {
                DirectFailureClass::Refused
            }
            _ => DirectFailureClass::Unavailable,
        };
        Self { code: error.code(), retry_after: error.retry_after(), class }
    }
}

impl Playback {
    pub fn new(enabled: bool) -> Self {
        Self { enabled, records: Mutex::new(HashMap::new()), metrics: Metrics::default() }
    }

    pub fn summary(&self) -> &'static str {
        if self.enabled {
            "on"
        } else {
            "off"
        }
    }

    /// Fixed-cardinality playback counters for Hearth's authenticated `/metrics`. No install, account,
    /// provider identifier, request key or URL becomes a label.
    pub fn render(&self, out: &mut String) {
        let _ = writeln!(
            out,
            "# HELP den_hearth_playback_managed_prepare_total Managed playback preparation attempts by outcome.\n\
             # TYPE den_hearth_playback_managed_prepare_total counter"
        );
        for (outcome, value) in [
            ("ok", self.metrics.direct_ok.load(Relaxed)),
            ("not_found", self.metrics.direct_not_found.load(Relaxed)),
            ("refused", self.metrics.direct_refused.load(Relaxed)),
            ("unavailable", self.metrics.direct_unavailable.load(Relaxed)),
            ("timeout", self.metrics.direct_timeout.load(Relaxed)),
        ] {
            let _ =
                writeln!(out, "den_hearth_playback_managed_prepare_total{{outcome=\"{outcome}\"}} {value}");
        }
        let _ = writeln!(
            out,
            "# HELP den_hearth_playback_operation_total Completed playback operations by outcome.\n\
             # TYPE den_hearth_playback_operation_total counter"
        );
        for (outcome, value) in [
            ("direct", self.metrics.operation_direct.load(Relaxed)),
            ("remux", self.metrics.operation_remux.load(Relaxed)),
            ("error", self.metrics.operation_error.load(Relaxed)),
            ("replay", self.metrics.operation_replay.load(Relaxed)),
        ] {
            let _ = writeln!(out, "den_hearth_playback_operation_total{{outcome=\"{outcome}\"}} {value}");
        }
    }

    fn direct_failure(&self, failure: DirectFailure) {
        match failure.class {
            DirectFailureClass::NotFound => &self.metrics.direct_not_found,
            DirectFailureClass::Refused => &self.metrics.direct_refused,
            DirectFailureClass::Unavailable => &self.metrics.direct_unavailable,
            DirectFailureClass::Timeout => &self.metrics.direct_timeout,
        }
        .fetch_add(1, Relaxed);
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
                    ReplayState::Committed(Snapshot {
                        status: StatusCode::OK,
                        headers: HeaderMap::new(),
                        body: Bytes::from_static(br#"{"url":"should-not-replay"}"#),
                    }),
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

fn finalized() -> Response<Body> {
    Response::builder()
        .status(StatusCode::NO_CONTENT)
        .header("cache-control", "no-store")
        .body(den_http::full(Bytes::new()))
        .expect("static playback finalization response")
}

fn replay_state(snapshot: Snapshot, delivered_direct: bool, finalization_protocol: bool) -> ReplayState {
    if delivered_direct || !finalization_protocol || !snapshot.status.is_success() {
        ReplayState::Committed(snapshot)
    } else {
        ReplayState::Pending(snapshot)
    }
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

pub(crate) fn idempotency_key(headers: &HeaderMap) -> Option<String> {
    let value = headers.get("idempotency-key")?.to_str().ok()?;
    (!value.is_empty() && value.len() <= MAX_KEY && value.bytes().all(|b| b.is_ascii_graphic() && b != b','))
        .then(|| value.to_string())
}

pub(crate) fn trusted_edge(hearth: &Hearth, parts: &hyper::http::request::Parts) -> bool {
    let Some(remux) = hearth.remux.as_ref() else { return false };
    let secret = parts.headers.get("x-den-edge-secret").map(HeaderValue::as_bytes);
    let secret_ok = den_remux::edge_secret_ok(remux.cfg.edge_secret.as_deref(), secret);
    let peer = parts.extensions.get::<den_remux::Peer>().map(|p| p.0);
    secret_ok && peer.is_some() && peer == remux.cfg.public_session_proxy
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

fn with_prepare_timing(mut response: Response<Body>, elapsed: Duration) -> Response<Body> {
    let prepare = den_http::timing("prepare", elapsed);
    let value = match response.headers().get("server-timing").and_then(|value| value.to_str().ok()) {
        Some(rest) => format!("{prepare}, {rest}"),
        None => prepare,
    };
    if let Ok(value) = HeaderValue::from_str(&value) {
        response.headers_mut().insert("server-timing", value);
    }
    response
}

fn log_direct_fallback(failure: DirectFailure, has_fallback: bool, rid: Option<&str>) {
    LOG.log(&format!("playback_direct_{}", failure.code), || {
        let action = if has_fallback { "remux" } else { "unavailable" };
        let mut line =
            format!("managed playback direct preparation failed reason={} action={action}", failure.code);
        if let Some(wait) = failure.retry_after {
            let _ = write!(line, " retry_after_seconds={}", wait.as_secs());
        }
        den_http::log::push_rid(&mut line, rid);
        line
    });
}

fn error_code(snapshot: &Snapshot) -> &'static str {
    let parsed = serde_json::from_slice::<Value>(&snapshot.body).ok();
    match parsed.as_ref().and_then(|value| value.get("error")).and_then(Value::as_str) {
        Some("bad_request") => "bad_request",
        Some("no_release") => "no_release",
        Some("idempotency_conflict") => "idempotency_conflict",
        Some("replacement_pending") => "replacement_pending",
        Some("playback_busy") => "playback_busy",
        Some("source_unavailable") => "source_unavailable",
        Some("playback_unavailable") => "playback_unavailable",
        Some("playback_timeout") => "playback_timeout",
        _ => "unknown",
    }
}

/// Add only the adapter's closed, non-secret direct outcome. A successful Remux fallback is explicitly degraded;
/// after both paths fail, the primary error remains Hearth's stable contract. This happens before the snapshot
/// enters the replay table, so an ambiguous retry receives exactly the same body and headers as the first answer.
fn annotate_direct_failure(snapshot: &mut Snapshot, failure: DirectFailure) {
    let Ok(mut value) = serde_json::from_slice::<Value>(&snapshot.body) else { return };
    let Some(object) = value.as_object_mut() else { return };
    if snapshot.status.is_success() {
        object.insert("degraded".to_string(), Value::Bool(true));
        object.insert("directFailure".to_string(), Value::String(failure.code.to_string()));
    } else {
        object.insert("cause".to_string(), Value::String(failure.code.to_string()));
    }
    let Ok(body) = serde_json::to_vec(&value) else { return };
    if body.len() > MAX_ANSWER {
        return;
    }
    snapshot.body = Bytes::from(body);
    if let Ok(length) = HeaderValue::from_str(&snapshot.body.len().to_string()) {
        snapshot.headers.insert(CONTENT_LENGTH, length);
    }
    if !snapshot.status.is_success() && !snapshot.headers.contains_key("retry-after") {
        if let Some(wait) = failure.retry_after {
            let seconds = wait.min(MAX_DIRECT_RETRY_AFTER).as_secs().max(1);
            if let Ok(value) = HeaderValue::from_str(&seconds.to_string()) {
                snapshot.headers.insert("retry-after", value);
            }
        }
    }
}

fn log_operation_failure(snapshot: &Snapshot, direct: Option<DirectFailure>, rid: Option<&str>) {
    if snapshot.status.is_success() {
        return;
    }
    log_failure(snapshot.status, error_code(snapshot), direct, rid);
}

fn log_failure(status: StatusCode, code: &'static str, direct: Option<DirectFailure>, rid: Option<&str>) {
    let condition = format!("playback_final_{code}");
    LOG.log(&condition, || {
        let mut line = format!("managed playback failed status={} error={code}", status.as_u16());
        if let Some(direct) = direct {
            let _ = write!(line, " direct={}", direct.code);
        }
        den_http::log::push_rid(&mut line, rid);
        line
    });
}

fn refuse(
    status: StatusCode,
    code: &'static str,
    retry_after: Option<&'static str>,
    rid: Option<&str>,
) -> Response<Body> {
    log_failure(status, code, None, rid);
    answer(status, code, retry_after)
}

async fn remux(
    hearth: &Arc<Hearth>,
    scout_install: &str,
    subtitles_install: Option<&str>,
    mut parts: hyper::http::request::Parts,
    mut request: Value,
) -> Response<Body> {
    let Some(object) = request.as_object_mut() else {
        return answer(StatusCode::BAD_REQUEST, "bad_request", None);
    };
    object.insert("scout".to_string(), Value::String(scout_install.to_string()));
    if let Some(install) = subtitles_install {
        object.insert("subtitles".to_string(), Value::String(install.to_string()));
    }
    let Ok(body) = serde_json::to_vec(&request) else {
        return answer(StatusCode::BAD_REQUEST, "bad_request", None);
    };
    parts.method = Method::POST;
    parts.uri = Uri::from_static("/remux/session");
    parts.headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
    parts.headers.remove(CONTENT_LENGTH);
    let response = den_remux::handle_request(
        hearth.remux.clone().expect("playback requires Remux"),
        Request::from_parts(parts, http_body_util::Full::new(Bytes::from(body))),
    )
    .await;
    translate_remux(response).await
}

/// The containers Remux left out unopened, from its 404 body. Only short `.ext` names pass: the rest of that body
/// is Remux's own detail, which the facade never repeats.
fn skipped_formats(body: &[u8]) -> Vec<String> {
    let Ok(value) = serde_json::from_slice::<Value>(body) else { return Vec::new() };
    let Some(formats) = value.get("formats").and_then(Value::as_array) else { return Vec::new() };
    formats
        .iter()
        .filter_map(Value::as_str)
        .filter(|ext| {
            ext.len() <= 6 && ext.starts_with('.') && ext[1..].bytes().all(|b| b.is_ascii_alphanumeric())
        })
        .take(20)
        .map(str::to_string)
        .collect()
}

async fn translate_remux(response: Response<Body>) -> Response<Body> {
    let status = response.status();
    let retry_after = response.headers().get("retry-after").cloned();
    let server_timing = response.headers().get("server-timing").cloned();
    let bytes = match http_body_util::Limited::new(response.into_body(), MAX_ANSWER).collect().await {
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
        let formats = if status == StatusCode::NOT_FOUND { skipped_formats(&bytes) } else { Vec::new() };
        let mut out = if formats.is_empty() {
            answer(status, code, retry)
        } else {
            let mut out = den_http::json(
                status,
                &json!({"error": code, "formats": formats}),
                &[("cache-control", "no-store")],
            );
            stamp(&mut out);
            out
        };
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
    match http_body_util::Limited::new(body, MAX_ANSWER).collect().await {
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
    // Opening and revocation checks deliberately happen before consulting replay state.
    let install = match hearth.den.open(config) {
        Ok(install) if install.members.contains(&"scout") => install,
        _ => {
            return refuse(
                StatusCode::BAD_REQUEST,
                "bad_request",
                None,
                den_http::log::request_id(req.headers()).as_deref(),
            )
        }
    };

    let combined = install_url(config);
    let subtitles = install.members.contains(&"subs").then_some(combined.as_str());
    handle_with_sources(hearth, Some(config), config, Some(&combined), subtitles, req).await
}

/// Serve Edge's authenticated guest overlay playback. The personal config can only supply managed direct
/// playback and a subtitle override; Remux always receives the host's legacy Scout install.
pub async fn handle_guest<B>(
    hearth: &Arc<Hearth>,
    direct_config: Option<&str>,
    personal_scope: Option<&str>,
    host_scout: Option<&str>,
    effective_subtitles: Option<&str>,
    req: Request<B>,
) -> Response<Body>
where
    B: hyper::body::Body<Data = Bytes> + Send + 'static,
    B::Error: Into<Box<dyn std::error::Error + Send + Sync>>,
{
    let scope = guest_scope(personal_scope, host_scout, effective_subtitles);
    handle_with_sources(hearth, direct_config, &scope, host_scout, effective_subtitles, req).await
}

/// All sources that can change a guest playback answer belong to its replay scope. Keeping the tuple encoded
/// rather than concatenated prevents one segment boundary from being confused for another.
fn guest_scope(
    personal: Option<&str>,
    host_scout: Option<&str>,
    effective_subtitles: Option<&str>,
) -> String {
    serde_json::to_string(&("guest", personal, host_scout, effective_subtitles))
        .expect("a tuple of strings always serializes")
}

async fn handle_with_sources<B>(
    hearth: &Arc<Hearth>,
    direct_config: Option<&str>,
    scope_config: &str,
    remux_scout: Option<&str>,
    remux_subtitles: Option<&str>,
    req: Request<B>,
) -> Response<Body>
where
    B: hyper::body::Body<Data = Bytes> + Send + 'static,
    B::Error: Into<Box<dyn std::error::Error + Send + Sync>>,
{
    if !hearth.playback.enabled {
        return den_http::not_found();
    }
    let rid = den_http::log::request_id(req.headers());
    if req.method() == Method::OPTIONS {
        return preflight();
    }
    if req.method() != Method::POST {
        let mut response = refuse(StatusCode::METHOD_NOT_ALLOWED, "bad_request", None, rid.as_deref());
        response.headers_mut().insert("allow", HeaderValue::from_static("POST, OPTIONS"));
        return response;
    }
    if !content_type_is_json(req.headers()) {
        return refuse(StatusCode::UNSUPPORTED_MEDIA_TYPE, "unsupported_media_type", None, rid.as_deref());
    }
    let Some(key) = idempotency_key(req.headers()) else {
        return refuse(StatusCode::BAD_REQUEST, "bad_request", None, rid.as_deref());
    };
    let (parts, body) = req.into_parts();
    let body = match http_body_util::Limited::new(body, MAX_BODY).collect().await {
        Ok(body) => body.to_bytes(),
        Err(_) => return refuse(StatusCode::BAD_REQUEST, "bad_request", None, rid.as_deref()),
    };
    let parsed = match serde_json::from_slice(&body).ok().and_then(|value| validate_request(value).ok()) {
        Some(parsed) => parsed,
        None => return refuse(StatusCode::BAD_REQUEST, "bad_request", None, rid.as_deref()),
    };
    let (request, direct_failed) = parsed;
    let canonical =
        Bytes::from(serde_json::to_vec(&json!({"request": request, "directFailed": direct_failed})).unwrap());
    let scope = (owner_scope(hearth, &parts), scope_config.to_string(), key);
    let finalize = parts.headers.get(FINALIZE_HEADER).and_then(|value| match value.as_bytes() {
        b"commit" => Some(Finalize::Commit),
        b"abort" => Some(Finalize::Abort),
        _ => None,
    });

    // A managed Remux answer is not safe to replay until Edge has opened the public media listener. Edge sends
    // this authenticated outcome with the identical operation after that attempt. Keeping the decision here means
    // Hearth owns its replay table while Edge owns only listener reachability; neither layer learns the other's
    // implementation details.
    if parts.headers.contains_key(FINALIZE_HEADER) {
        if !trusted_edge(hearth, &parts)
            || parts.headers.get("x-den-managed-playback").is_none_or(|value| value.as_bytes() != b"1")
            || parts.headers.get(FINALIZATION_PROTOCOL_HEADER).is_none_or(|value| value.as_bytes() != b"1")
            || finalize.is_none()
        {
            return refuse(StatusCode::BAD_REQUEST, "bad_request", None, rid.as_deref());
        }
        let outcome = finalize.expect("validated finalization outcome");
        let gate = {
            let mut records = hearth.playback.records.lock().await;
            retain_live_records(&mut records, Instant::now());
            if !records.contains_key(&scope) && outcome == Finalize::Commit {
                return answer(StatusCode::CONFLICT, "playback_pending", Some("1"));
            }
            match reserve_record(&mut records, &scope, &canonical) {
                Ok(gate) => gate,
                Err(ReservationError::Conflict) => {
                    return refuse(StatusCode::CONFLICT, "idempotency_conflict", None, rid.as_deref())
                }
                Err(ReservationError::Busy) => {
                    return refuse(StatusCode::TOO_MANY_REQUESTS, "playback_busy", Some("5"), rid.as_deref())
                }
            }
        };
        let _one = gate.lock().await;
        let mut records = hearth.playback.records.lock().await;
        let Some(record) = records.get_mut(&scope) else {
            return answer(StatusCode::CONFLICT, "playback_pending", Some("1"));
        };
        if finalize_record(record, &canonical, outcome).is_err() {
            return refuse(StatusCode::CONFLICT, "idempotency_conflict", None, rid.as_deref());
        }
        return finalized();
    }

    let gate = {
        let mut records = hearth.playback.records.lock().await;
        let now = Instant::now();
        retain_live_records(&mut records, now);
        match reserve_record(&mut records, &scope, &canonical) {
            Ok(gate) => gate,
            Err(ReservationError::Conflict) => {
                return refuse(StatusCode::CONFLICT, "idempotency_conflict", None, rid.as_deref())
            }
            Err(ReservationError::Busy) => {
                return refuse(StatusCode::TOO_MANY_REQUESTS, "playback_busy", Some("5"), rid.as_deref())
            }
        }
    };
    let _one = gate.lock().await;
    if let Some(response) = hearth
        .playback
        .records
        .lock()
        .await
        .get(&scope)
        .and_then(|record| record.completed.as_ref().map(|(_, state)| state.response()))
    {
        hearth.playback.metrics.operation_replay.fetch_add(1, Relaxed);
        return response;
    }

    let edge_managed = trusted_edge(hearth, &parts)
        && parts.headers.get("x-den-managed-playback").is_some_and(|value| value.as_bytes() == b"1");
    let finalization_protocol = edge_managed
        && parts.headers.get(FINALIZATION_PROTOCOL_HEADER).is_some_and(|value| value.as_bytes() == b"1");
    let managed = edge_managed && direct_config.is_some() && !direct_failed;
    let direct = managed_request(&request);
    // Kept outside the bounded future so a completed direct attempt is still available when Remux consumes the
    // rest of the overall deadline and the future is cancelled by the outer timeout.
    let mut direct_failure = None;
    let mut prepare_elapsed = None;
    let operation = async {
        if managed {
            if let Some(direct) = direct.as_ref() {
                let started = Instant::now();
                match tokio::time::timeout(
                    MANAGED_PREPARE_TIMEOUT,
                    hearth.scout.as_ref().expect("playback requires Scout").managed_hls(
                        direct_config.expect("a managed direct request has a personal config"),
                        direct,
                    ),
                )
                .await
                {
                    Ok(Ok(result)) => {
                        hearth.playback.metrics.direct_ok.fetch_add(1, Relaxed);
                        return (direct_response(result, started.elapsed().as_millis() as u64), true);
                    }
                    Ok(Err(error)) => {
                        let failure = DirectFailure::from_managed(error);
                        hearth.playback.direct_failure(failure);
                        direct_failure = Some(failure);
                    }
                    Err(_) => {
                        hearth.playback.direct_failure(DirectFailure::TIMEOUT);
                        direct_failure = Some(DirectFailure::TIMEOUT);
                    }
                }
                let elapsed = started.elapsed();
                prepare_elapsed = Some(elapsed);
                log_direct_fallback(
                    direct_failure.expect("a failed direct attempt has a reason"),
                    remux_scout.is_some(),
                    rid.as_deref(),
                );
            }
        }
        let mut response = match remux_scout {
            Some(scout) => remux(hearth, scout, remux_subtitles, parts, request).await,
            None => answer(StatusCode::BAD_GATEWAY, "playback_unavailable", None),
        };
        if let Some(elapsed) = prepare_elapsed {
            response = with_prepare_timing(response, elapsed);
        }
        (response, false)
    };
    let (response, delivered_direct) = match tokio::time::timeout(OPERATION_TIMEOUT, operation).await {
        Ok(result) => result,
        Err(_) => (answer(StatusCode::GATEWAY_TIMEOUT, "playback_timeout", None), false),
    };
    let mut saved = snapshot(response).await;
    if let Some(failure) = direct_failure {
        annotate_direct_failure(&mut saved, failure);
    }
    if saved.status.is_success() {
        if delivered_direct {
            hearth.playback.metrics.operation_direct.fetch_add(1, Relaxed);
        } else {
            hearth.playback.metrics.operation_remux.fetch_add(1, Relaxed);
        }
    } else {
        hearth.playback.metrics.operation_error.fetch_add(1, Relaxed);
        log_operation_failure(&saved, direct_failure, rid.as_deref());
    }
    if let Some(record) = hearth.playback.records.lock().await.get_mut(&scope) {
        // Direct provider URLs need no Edge listener. A managed Remux fallback does, so its replay stays
        // provisional until Edge explicitly commits or aborts it after attempting that listener.
        let state = replay_state(saved.clone(), delivered_direct, finalization_protocol);
        record.completed = Some((Instant::now(), state));
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
                completed: Some((Instant::now(), ReplayState::Committed(response.clone()))),
            },
        );
        retain_live_records(&mut records, Instant::now());
        assert!(records.contains_key(&key), "a fresh answer remains replayable");

        records.get_mut(&key).unwrap().completed = Some((
            Instant::now().checked_sub(REPLAY_FOR + Duration::from_secs(1)).unwrap(),
            ReplayState::Committed(response),
        ));
        retain_live_records(&mut records, Instant::now());
        assert!(records.is_empty(), "a stale signed answer is not retained");
    }

    #[test]
    fn one_owner_cannot_consume_another_owner_s_replay_capacity() {
        let mut records = HashMap::new();
        let request = Bytes::from_static(b"same request");
        for n in 0..MAX_RECORDS_PER_OWNER {
            let scope = ("grant:one".to_string(), "install".to_string(), format!("attempt-{n}"));
            assert!(reserve_record(&mut records, &scope, &request).is_ok());
        }

        let first = ("grant:one".to_string(), "install".to_string(), "attempt-0".to_string());
        let replay =
            reserve_record(&mut records, &first, &request).expect("an existing operation still replays");
        assert!(Arc::ptr_eq(&replay, &records[&first].gate));
        assert!(matches!(
            reserve_record(
                &mut records,
                &("grant:one".to_string(), "install".to_string(), "one-too-many".to_string()),
                &request,
            ),
            Err(ReservationError::Busy)
        ));

        let other = ("grant:two".to_string(), "install".to_string(), "attempt".to_string());
        assert!(reserve_record(&mut records, &other, &request).is_ok());
        assert!(records.contains_key(&other));
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
                playback.records.lock().await.get(&scope).and_then(|record| match record.completed.as_ref() {
                    Some((_, ReplayState::Committed(response))) => Some(response.clone()),
                    _ => None,
                })
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
            Some((Instant::now(), ReplayState::Committed(expected.clone())));
        drop(first);

        let observed = waiting.await.unwrap().expect("the first operation published an answer");
        assert_eq!(observed.status, expected.status);
        assert_eq!(observed.headers, expected.headers);
        assert_eq!(observed.body, expected.body);
    }

    #[tokio::test]
    async fn managed_remux_replay_is_not_exposed_until_edge_commits_it() {
        let scope = ("edge:owner".to_string(), "install".to_string(), "attempt".to_string());
        let snapshot = Snapshot {
            status: StatusCode::OK,
            headers: HeaderMap::new(),
            body: Bytes::from_static(br#"{"url":"/remux/s/id/sig/master.m3u8"}"#),
        };
        let mut records = HashMap::from([(
            scope.clone(),
            Record {
                request: Bytes::from_static(b"same"),
                gate: Arc::new(Mutex::new(())),
                completed: Some((Instant::now(), ReplayState::Pending(snapshot.clone()))),
            },
        )]);

        let request = Bytes::from_static(b"same");
        finalize_record(records.get_mut(&scope).unwrap(), &request, Finalize::Commit).unwrap();
        assert!(matches!(records[&scope].completed, Some((_, ReplayState::Committed(_)))));
        assert_eq!(records[&scope].completed.as_ref().unwrap().1.response().status(), StatusCode::OK);
    }

    #[test]
    fn two_phase_replay_is_opt_in_for_a_rolling_edge_deploy() {
        let snapshot = || Snapshot {
            status: StatusCode::OK,
            headers: HeaderMap::new(),
            body: Bytes::from_static(br#"{"url":"/remux/s/id/sig/master.m3u8"}"#),
        };
        assert!(matches!(replay_state(snapshot(), false, false), ReplayState::Committed(_)));
        assert!(matches!(replay_state(snapshot(), false, true), ReplayState::Pending(_)));
        assert!(matches!(replay_state(snapshot(), true, true), ReplayState::Committed(_)));
    }

    #[test]
    fn delayed_commit_and_abort_are_serialized_by_the_first_terminal_outcome() {
        let request = Bytes::from_static(b"same");
        let pending = || Record {
            request: request.clone(),
            gate: Arc::new(Mutex::new(())),
            completed: Some((
                Instant::now(),
                ReplayState::Pending(Snapshot {
                    status: StatusCode::OK,
                    headers: HeaderMap::new(),
                    body: Bytes::from_static(br#"{"url":"/remux/s/id/sig/master.m3u8"}"#),
                }),
            )),
        };

        let mut aborted = pending();
        assert!(finalize_record(&mut aborted, &request, Finalize::Abort).is_ok());
        assert!(finalize_record(&mut aborted, &request, Finalize::Abort).is_ok());
        assert!(finalize_record(&mut aborted, &request, Finalize::Commit).is_err());
        assert!(matches!(aborted.completed, Some((_, ReplayState::Aborted))));

        let mut committed = pending();
        assert!(finalize_record(&mut committed, &request, Finalize::Commit).is_ok());
        assert!(finalize_record(&mut committed, &request, Finalize::Commit).is_ok());
        assert!(finalize_record(&mut committed, &request, Finalize::Abort).is_err());
        assert!(matches!(committed.completed, Some((_, ReplayState::Committed(_)))));
    }

    #[tokio::test]
    async fn an_aborted_key_replays_a_stable_failure_instead_of_starting_another_session() {
        let scope = ("edge:owner".to_string(), "install".to_string(), "attempt".to_string());
        let request = Bytes::from_static(b"same");
        let gate = Arc::new(Mutex::new(()));
        let mut records = HashMap::from([(
            scope.clone(),
            Record {
                request: request.clone(),
                gate: gate.clone(),
                completed: Some((Instant::now(), ReplayState::Aborted)),
            },
        )]);

        let replay = reserve_record(&mut records, &scope, &request).unwrap();
        assert!(Arc::ptr_eq(&replay, &gate));
        let response = records[&scope].completed.as_ref().unwrap().1.response();
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(response.headers()["retry-after"], "1");
        assert_eq!(body(response).await, json!({"error": "source_unavailable"}));
        assert!(matches!(records[&scope].completed, Some((_, ReplayState::Aborted))));
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
    async fn a_no_release_answer_names_only_the_skipped_formats() {
        let response = den_http::json(
            StatusCode::NOT_FOUND,
            &json!({
                "error": "no_playable_release",
                "detail": "torrent 123",
                "formats": [".m2ts", ".wmv", "../secret", ".toolongext", 7],
            }),
            &[],
        );
        let response = translate_remux(response).await;
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
        assert_eq!(body(response).await, json!({"error": "no_release", "formats": [".m2ts", ".wmv"]}));

        let plain = den_http::json(StatusCode::NOT_FOUND, &json!({"error": "no_release"}), &[]);
        assert_eq!(body(translate_remux(plain).await).await, json!({"error": "no_release"}));
    }

    #[tokio::test]
    async fn remux_answer_is_bounded_while_it_is_read() {
        let response = Response::builder()
            .status(StatusCode::CREATED)
            .body(den_http::full(vec![b'x'; MAX_ANSWER + 1]))
            .unwrap();
        let response = translate_remux(response).await;
        assert_eq!(response.status(), StatusCode::BAD_GATEWAY);
        assert_eq!(body(response).await, json!({"error": "source_unavailable"}));
    }

    /// A long film's answer lists every segment; 1,224 of them (a 2.6-hour film) failed the old 16 KiB ceiling.
    #[tokio::test]
    async fn a_long_films_session_answer_is_translated_and_replayed_whole() {
        let segments: Vec<Value> = (0..1_224).map(|i| json!([f64::from(i) * 7.697, 2_345_678 + i])).collect();
        let created = den_http::json(
            StatusCode::CREATED,
            &json!({"sid": "sid", "playlist": "/remux/s/sid/sig/master.m3u8", "segments": segments}),
            &[],
        );
        let response = snapshot(translate_remux(created).await).await.response();
        assert_eq!(response.status(), StatusCode::OK);
        let value = body(response).await;
        assert_eq!(value["url"], "/remux/s/sid/sig/master.m3u8");
        assert_eq!(value["session"]["segments"].as_array().map(Vec::len), Some(1_224));
    }

    #[tokio::test]
    async fn replay_snapshot_does_not_retain_oversized_release_metadata() {
        let response = Response::builder()
            .status(StatusCode::OK)
            .body(den_http::full(vec![b'x'; MAX_ANSWER + 1]))
            .unwrap();
        let response = snapshot(response).await.response();
        assert_eq!(response.status(), StatusCode::BAD_GATEWAY);
        assert_eq!(body(response).await, json!({"error": "source_unavailable"}));
    }

    #[test]
    fn fallback_timing_keeps_the_existing_remux_phases_without_detail() {
        let response = den_http::json(
            StatusCode::OK,
            &json!({"url": "/remux/s/id/signature/master.m3u8"}),
            &[("server-timing", "resolve;dur=4.0")],
        );
        let response = with_prepare_timing(response, Duration::from_millis(3));
        assert_eq!(response.headers()["server-timing"], "prepare;dur=3.0, resolve;dur=4.0");
    }

    #[test]
    fn playback_metrics_have_only_fixed_non_secret_labels() {
        let playback = Playback::new(true);
        playback.metrics.direct_ok.fetch_add(2, Relaxed);
        playback.direct_failure(DirectFailure::from_managed(
            den_scout::managed_hls::ManagedHlsError::Unavailable,
        ));
        playback.metrics.operation_direct.fetch_add(1, Relaxed);
        playback.metrics.operation_error.fetch_add(1, Relaxed);
        let mut rendered = String::new();
        playback.render(&mut rendered);

        assert!(rendered.contains("managed_prepare_total{outcome=\"ok\"} 2"));
        assert!(rendered.contains("managed_prepare_total{outcome=\"unavailable\"} 1"));
        assert!(rendered.contains("operation_total{outcome=\"direct\"} 1"));
        assert!(rendered.contains("operation_total{outcome=\"error\"} 1"));
        for secret in ["config", "owner", "idempotency", "url", "torrent", "file_id"] {
            assert!(!rendered.contains(secret), "metrics disclosed {secret:?}: {rendered}");
        }
    }

    #[test]
    fn final_error_classification_never_repeats_upstream_detail() {
        let snapshot = Snapshot {
            status: StatusCode::BAD_GATEWAY,
            headers: HeaderMap::new(),
            body: Bytes::from_static(br#"{"error":"provider_secret","detail":"torrent 123"}"#),
        };
        assert_eq!(error_code(&snapshot), "unknown");
    }

    #[test]
    fn managed_failure_keeps_only_the_stable_code_and_retry_delay() {
        let failure = DirectFailure::from_managed(den_scout::managed_hls::ManagedHlsError::RateLimited {
            retry_after: Some(Duration::from_secs(17)),
        });
        assert_eq!(failure.code, "torbox_rate_limited");
        assert_eq!(failure.retry_after, Some(Duration::from_secs(17)));
        assert!(matches!(failure.class, DirectFailureClass::Unavailable));
    }

    fn error_snapshot(headers: HeaderMap) -> Snapshot {
        Snapshot {
            status: StatusCode::BAD_GATEWAY,
            headers,
            body: Bytes::from_static(br#"{"error":"source_unavailable"}"#),
        }
    }

    #[test]
    fn final_error_names_only_sanitized_auth_and_entitlement_causes() {
        for (error, expected) in [
            (den_scout::managed_hls::ManagedHlsError::Authentication, "torbox_authentication_failed"),
            (
                den_scout::managed_hls::ManagedHlsError::Entitlement,
                "torbox_managed_streaming_unavailable_for_plan",
            ),
        ] {
            let mut snapshot = error_snapshot(HeaderMap::new());
            annotate_direct_failure(&mut snapshot, DirectFailure::from_managed(error));
            let value: Value = serde_json::from_slice(&snapshot.body).unwrap();
            assert_eq!(value["error"], "source_unavailable");
            assert_eq!(value["cause"], expected);
            assert_eq!(snapshot.headers[CONTENT_LENGTH], snapshot.body.len().to_string());
            assert!(!snapshot.headers.contains_key("retry-after"));
        }

        let mut timed_out = error_snapshot(HeaderMap::new());
        annotate_direct_failure(&mut timed_out, DirectFailure::TIMEOUT);
        let value: Value = serde_json::from_slice(&timed_out.body).unwrap();
        assert_eq!(value["cause"], "managed_prepare_timeout");
    }

    #[test]
    fn direct_rate_limit_supplies_one_bounded_retry_after_without_overriding_remux() {
        let failure = DirectFailure::from_managed(den_scout::managed_hls::ManagedHlsError::RateLimited {
            retry_after: Some(Duration::from_secs(2 * 60 * 60)),
        });
        let mut snapshot = error_snapshot(HeaderMap::new());
        annotate_direct_failure(&mut snapshot, failure);
        assert_eq!(snapshot.headers["retry-after"], "3600");
        let value: Value = serde_json::from_slice(&snapshot.body).unwrap();
        assert_eq!(value["cause"], "torbox_rate_limited");

        snapshot.headers.insert("retry-after", HeaderValue::from_static("7"));
        annotate_direct_failure(&mut snapshot, failure);
        assert_eq!(snapshot.headers["retry-after"], "7", "the final Remux answer takes precedence");
    }

    #[test]
    fn a_successful_remux_fallback_exposes_only_the_stable_degraded_reason() {
        let original = br#"{"url":"/remux/s/id/signature/master.m3u8"}"#;
        let mut snapshot = Snapshot {
            status: StatusCode::OK,
            headers: HeaderMap::new(),
            body: Bytes::from_static(original),
        };
        annotate_direct_failure(
            &mut snapshot,
            DirectFailure::from_managed(den_scout::managed_hls::ManagedHlsError::Authentication),
        );
        assert_eq!(
            serde_json::from_slice::<Value>(&snapshot.body).unwrap(),
            json!({
                "url": "/remux/s/id/signature/master.m3u8",
                "degraded": true,
                "directFailure": "torbox_authentication_failed"
            })
        );
        assert_eq!(snapshot.headers[CONTENT_LENGTH], snapshot.body.len().to_string());
        assert!(!snapshot.headers.contains_key("retry-after"));
    }

    #[test]
    fn guest_replay_scope_changes_with_every_effective_config_source() {
        let baseline = guest_scope(Some("personal"), Some("host-scout"), Some("host-subs"));
        for changed in [
            guest_scope(Some("personal-2"), Some("host-scout"), Some("host-subs")),
            guest_scope(Some("personal"), Some("host-scout-2"), Some("host-subs")),
            guest_scope(Some("personal"), Some("host-scout"), Some("host-subs-2")),
            guest_scope(Some("personal"), Some("host-scout"), None),
        ] {
            assert_ne!(baseline, changed);
        }
    }

    #[tokio::test]
    async fn the_sanitized_final_failure_replays_exactly_without_private_detail() {
        let mut snapshot = error_snapshot(HeaderMap::new());
        annotate_direct_failure(
            &mut snapshot,
            DirectFailure::from_managed(den_scout::managed_hls::ManagedHlsError::Authentication),
        );
        let read = async |response: Response<Body>| {
            let (parts, body) = response.into_parts();
            (parts.status, parts.headers, body.collect().await.unwrap().to_bytes())
        };
        let first = read(snapshot.response()).await;
        let replay = read(snapshot.response()).await;
        assert_eq!(first, replay);
        let text = String::from_utf8(first.2.to_vec()).unwrap();
        assert_eq!(
            serde_json::from_str::<Value>(&text).unwrap(),
            json!({"error": "source_unavailable", "cause": "torbox_authentication_failed"})
        );
        for private in ["token", "signed", "torrent", "file_id", "config", "grant", "idempotency"] {
            assert!(!text.contains(private), "replayed final failure disclosed {private:?}: {text}");
        }
    }
}
