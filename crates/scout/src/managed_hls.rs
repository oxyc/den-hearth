//! TorBox's managed-HLS adapter.
//!
//! This module only prepares a direct HLS result. Hearth owns whether it is eligible, candidate ordering,
//! and fallback to Remux. It is deliberately not wired into Scout's existing play route.

use std::fmt;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use async_trait::async_trait;
use reqwest::header::{HeaderValue, ACCEPT, AUTHORIZATION};
use serde::Deserialize;

use crate::cache::Cache;
use crate::client::ClientPlayable;
use crate::config::{Config, DebridService};
use crate::loglimit::log_limited;
use crate::metrics::metrics;
use crate::scrape::Doer;
use crate::stores::refusal::{is_torrent_gone, refusal_is_about_the_account};
use crate::stores::torbox::{TorBoxStore, TORBOX_API};
use crate::stores::{ResolveTarget, StoreError};

const CREATE_STREAM_URL: &str = "https://api.torbox.app/v1/api/stream/createstream";
const CREATE_BODY_LIMIT: usize = 64 << 10;
const PLAYLIST_BODY_LIMIT: usize = 256 << 10;
const PLAYLIST_PROBE_TIMEOUT: Duration = Duration::from_secs(5);

/// The only distinctions Hearth needs while trying candidates and deciding whether to fall back.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ManagedHlsError {
    /// The selected TorBox item or file does not exist. Another candidate may still work.
    NotFound,
    /// The account was not authorized to create the stream.
    Refused,
    /// TorBox rejected the API credential. Kept separate so the caller can tell the user to fix their key.
    Authentication,
    /// The account is valid but its plan does not permit managed streaming.
    Entitlement,
    /// TorBox asked this credential to wait. The shared transport also remembers and enforces the pause.
    RateLimited { retry_after: Option<Duration> },
    /// The request failed before TorBox returned an HTTP response.
    Transport,
    /// TorBox returned a response that did not match the documented/observed JSON contract.
    MalformedResponse,
    /// TorBox returned a URL outside the deliberately narrow HTTPS CDN allow-list.
    UnsafeResponse,
    /// The signed playlist could not be verified without following redirects or exceeding its strict bounds.
    PlaylistUnavailable,
    /// TorBox, its answer, or the returned playlist was temporarily unusable.
    Unavailable,
}

impl ManagedHlsError {
    /// Stable, non-secret detail suitable for an error response, metric label, or log line.
    pub fn code(self) -> &'static str {
        match self {
            Self::NotFound => "torbox_item_not_found",
            Self::Refused => "torbox_refused",
            Self::Authentication => "torbox_authentication_failed",
            Self::Entitlement => "torbox_managed_streaming_unavailable_for_plan",
            Self::RateLimited { .. } => "torbox_rate_limited",
            Self::Transport => "torbox_unreachable",
            Self::MalformedResponse => "torbox_malformed_response",
            Self::UnsafeResponse => "torbox_unsafe_stream_url",
            Self::PlaylistUnavailable => "torbox_playlist_unavailable",
            Self::Unavailable => "torbox_unavailable",
        }
    }

    pub fn retry_after(self) -> Option<Duration> {
        match self {
            Self::RateLimited { retry_after } => retry_after,
            _ => None,
        }
    }
}

/// Hearth's title-level managed-playback question. Provider identifiers deliberately do not appear here.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct ManagedHlsRequest {
    pub imdb: String,
    pub season: Option<i64>,
    pub episode: Option<i64>,
    /// When present, keep the user's explicit release selection instead of choosing the first ranked release.
    pub filename: Option<String>,
    /// The same browser capability report Scout already uses for stream-list ranking.
    pub playable: Option<ClientPlayable>,
}

/// Non-secret release metadata returned beside a managed URL.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ManagedHlsRelease {
    pub label: String,
    pub filename: String,
    pub size: Option<i64>,
}

/// A verified direct result. The URL's `Debug` output remains redacted through `ManagedHlsUrl`.
#[derive(Clone, PartialEq, Eq)]
pub struct ManagedHlsResult {
    pub url: ManagedHlsUrl,
    pub release: ManagedHlsRelease,
}

/// The internal seam Handler uses, so title selection and provider resolution stay separate responsibilities.
#[async_trait]
pub trait ManagedHlsResolver: Send + Sync {
    async fn resolve(
        &self,
        config: &Config,
        target: &ResolveTarget,
    ) -> Result<ManagedHlsUrl, ManagedHlsError>;
}

impl fmt::Debug for ManagedHlsResult {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ManagedHlsResult").field("url", &self.url).field("release", &self.release).finish()
    }
}

/// A validated TorBox CDN URL. Its `Debug` representation is intentionally redacted because the query is a
/// bearer credential.
#[derive(Clone, PartialEq, Eq)]
pub struct ManagedHlsUrl(reqwest::Url);

impl ManagedHlsUrl {
    pub fn as_str(&self) -> &str {
        self.0.as_str()
    }

    pub fn into_string(self) -> String {
        self.0.into()
    }

    #[cfg(test)]
    pub(crate) fn fixture(raw: &str) -> Self {
        Self(validate_hls_url(raw).expect("a valid managed-HLS fixture URL"))
    }
}

impl fmt::Debug for ManagedHlsUrl {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("ManagedHlsUrl([redacted])")
    }
}

/// A small client Hearth can retain and call after it has selected a TorBox torrent and file.
pub struct TorBoxManagedHls {
    create_client: Arc<dyn Doer>,
    cache: Arc<dyn Cache>,
    // Separate from Scout's normal read client: a signed playlist URL must never be replayed to a redirect
    // target. The five-second outer deadline is the total bound; the client timeout is defence in depth.
    probe_client: reqwest::Client,
}

impl TorBoxManagedHls {
    pub fn new(create_client: Arc<dyn Doer>, cache: Arc<dyn Cache>) -> Result<Self, ManagedHlsError> {
        let probe_client = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .timeout(PLAYLIST_PROBE_TIMEOUT)
            .build()
            .map_err(|_| ManagedHlsError::Unavailable)?;
        Ok(Self { create_client, cache, probe_client })
    }

    /// Resolves Scout's ordinary target to the account-local TorBox torrent and file, then creates and probes
    /// one managed stream. Provider ids never cross this boundary. Torrent acquisition goes through the same
    /// add-budgeted, refusal-aware path as ordinary Scout playback.
    async fn resolve_target(
        &self,
        config: &Config,
        target: &ResolveTarget,
    ) -> Result<ManagedHlsUrl, ManagedHlsError> {
        let token = config
            .debrid
            .iter()
            .find(|account| account.service == DebridService::TorBox)
            .map(|account| account.token.as_str())
            .ok_or(ManagedHlsError::Refused)?;

        let store = TorBoxStore {
            token: token.to_owned(),
            client: self.create_client.clone(),
            cache: Some(self.cache.clone()),
            api: TORBOX_API.to_owned(),
        };
        let (torrent_id, file_id) = store
            .prepare_managed_item(target)
            .await
            .map_err(classify_store_error)
            .map_err(report_resolution_failure)?;

        match self.create(token, torrent_id, file_id).await {
            Err(ManagedHlsError::NotFound) => {
                store.forget_torrent_id(&target.info_hash);
                Err(ManagedHlsError::NotFound)
            }
            result => result,
        }
    }

    async fn create(
        &self,
        token: &str,
        torrent_id: i64,
        file_id: Option<i64>,
    ) -> Result<ManagedHlsUrl, ManagedHlsError> {
        let request =
            create_request(token, torrent_id, file_id).map_err(report_create_failure_without_status)?;
        let mut response = match self.create_client.do_request(request).await {
            Ok(response) => response,
            Err(_) => {
                metrics().torbox_call("createstream", false);
                return Err(report_create_failure(ManagedHlsError::Transport, None));
            }
        };
        let status = response.status();
        if status == reqwest::StatusCode::UNAUTHORIZED {
            metrics().torbox_call("createstream", false);
            return Err(report_create_failure(ManagedHlsError::Authentication, Some(status)));
        }
        if status == reqwest::StatusCode::FORBIDDEN {
            metrics().torbox_call("createstream", false);
            return Err(report_create_failure(ManagedHlsError::Refused, Some(status)));
        }
        if status == reqwest::StatusCode::PAYMENT_REQUIRED {
            metrics().torbox_call("createstream", false);
            return Err(report_create_failure(ManagedHlsError::Entitlement, Some(status)));
        }
        if let Some(throttle) = den_upstream::throttle(status, response.headers(), SystemTime::now()) {
            let retry_after = match throttle {
                den_upstream::Throttle::Stated(wait) => Some(wait),
                den_upstream::Throttle::Unstated => None,
            };
            metrics().torbox_call("createstream", false);
            return Err(report_create_failure(ManagedHlsError::RateLimited { retry_after }, Some(status)));
        }
        let body = read_capped(&mut response, CREATE_BODY_LIMIT).await.map_err(|_| {
            metrics().torbox_call("createstream", false);
            report_create_failure(ManagedHlsError::MalformedResponse, Some(status))
        })?;
        if status != reqwest::StatusCode::OK {
            metrics().torbox_call("createstream", false);
            return Err(report_create_failure(classify_failure(&body), Some(status)));
        }

        let url = parse_success(&body).map_err(|error| {
            metrics().torbox_call("createstream", false);
            report_create_failure(error, Some(status))
        })?;
        metrics().torbox_call("createstream", true);
        probe_playlist(&self.probe_client, &url).await.map_err(report_probe_failure)?;
        Ok(ManagedHlsUrl(url))
    }
}

fn report_create_failure(error: ManagedHlsError, status: Option<reqwest::StatusCode>) -> ManagedHlsError {
    let status = status.map_or_else(|| "none".to_owned(), |status| status.as_u16().to_string());
    let retry = error.retry_after().map_or_else(|| "none".to_owned(), |wait| wait.as_secs().to_string());
    log_limited(
        error.code(),
        &format!(
            "managed HLS: TorBox createstream failed: cause={} http_status={status} retry_after_seconds={retry}",
            error.code()
        ),
    );
    error
}

fn report_create_failure_without_status(error: ManagedHlsError) -> ManagedHlsError {
    report_create_failure(error, None)
}

fn report_probe_failure(error: ManagedHlsError) -> ManagedHlsError {
    log_limited(error.code(), &format!("managed HLS: TorBox playlist probe failed: cause={}", error.code()));
    error
}

fn report_resolution_failure(error: ManagedHlsError) -> ManagedHlsError {
    log_limited(
        error.code(),
        &format!("managed HLS: TorBox item preparation failed: cause={}", error.code()),
    );
    error
}

#[async_trait]
impl ManagedHlsResolver for TorBoxManagedHls {
    async fn resolve(
        &self,
        config: &Config,
        target: &ResolveTarget,
    ) -> Result<ManagedHlsUrl, ManagedHlsError> {
        self.resolve_target(config, target).await
    }
}

fn classify_store_error(error: crate::stores::StoreError) -> ManagedHlsError {
    if is_torrent_gone(&error) {
        ManagedHlsError::NotFound
    } else if matches!(
        &error,
        StoreError::Unavailable(unavailable) | StoreError::ScoutSide(unavailable)
            if unavailable.status == 429
    ) {
        ManagedHlsError::RateLimited { retry_after: None }
    } else if refusal_is_about_the_account(&error) {
        ManagedHlsError::Authentication
    } else if matches!(error, StoreError::Cancelled) {
        ManagedHlsError::Transport
    } else {
        ManagedHlsError::Unavailable
    }
}

fn create_request(
    token: &str,
    torrent_id: i64,
    file_id: Option<i64>,
) -> Result<reqwest::Request, ManagedHlsError> {
    let mut url = reqwest::Url::parse(CREATE_STREAM_URL).map_err(|_| ManagedHlsError::Unavailable)?;
    {
        let mut query = url.query_pairs_mut();
        query.append_pair("id", &torrent_id.to_string());
        if let Some(file_id) = file_id {
            query.append_pair("file_id", &file_id.to_string());
        }
        query.append_pair("type", "torrent");
        // TorBox's default audio is index zero. Do not send chosen_subtitle_index: selecting the observed
        // embedded subtitle made the playlist stay unusable, while an omitted selection was immediately ready.
        query.append_pair("chosen_audio_index", "0");
        query.append_pair("scrobbling_enabled", "false");
    }
    let bearer = HeaderValue::from_str(&format!("Bearer {token}")).map_err(|_| ManagedHlsError::Refused)?;
    let mut request = reqwest::Request::new(reqwest::Method::GET, url);
    request.headers_mut().insert(AUTHORIZATION, bearer);
    request.headers_mut().insert(ACCEPT, HeaderValue::from_static("application/json"));
    Ok(request)
}

#[derive(Deserialize)]
struct CreateEnvelope {
    success: bool,
    error: Option<String>,
    data: Option<CreateData>,
}

#[derive(Deserialize)]
struct CreateData {
    audio_index: i64,
    subtitle_index: String,
    scrobbling_enabled: bool,
    hls_url: String,
}

fn parse_success(body: &[u8]) -> Result<reqwest::Url, ManagedHlsError> {
    // from_slice consumes exactly one complete JSON document. Unknown response fields remain additive, but
    // every field this adapter relies on must be present with the observed type and value.
    let answer: CreateEnvelope =
        serde_json::from_slice(body).map_err(|_| ManagedHlsError::MalformedResponse)?;
    if !answer.success {
        return Err(match answer.error.as_deref() {
            Some("ITEM_NOT_FOUND") => ManagedHlsError::NotFound,
            _ => ManagedHlsError::Unavailable,
        });
    }
    let data = answer
        .data
        .filter(|_| answer.success && answer.error.is_none())
        .ok_or(ManagedHlsError::MalformedResponse)?;
    if data.audio_index != 0 || data.subtitle_index != "null" || data.scrobbling_enabled {
        return Err(ManagedHlsError::MalformedResponse);
    }
    validate_hls_url(&data.hls_url)
}

fn validate_hls_url(raw: &str) -> Result<reqwest::Url, ManagedHlsError> {
    let url = reqwest::Url::parse(raw).map_err(|_| ManagedHlsError::UnsafeResponse)?;
    let host = url.host_str().ok_or(ManagedHlsError::UnsafeResponse)?;
    let cdn_label = host.strip_suffix(".tb-cdn.io").filter(|label| !label.is_empty());
    let safe = url.scheme() == "https"
        && url.username().is_empty()
        && url.password().is_none()
        && url.fragment().is_none()
        && url.port().is_none_or(|port| port == 443)
        && cdn_label.is_some()
        && url.query_pairs().any(|(name, value)| name == "scrobbling_enabled" && value == "false");
    safe.then_some(url).ok_or(ManagedHlsError::UnsafeResponse)
}

fn classify_failure(body: &[u8]) -> ManagedHlsError {
    #[derive(Deserialize)]
    struct Failure {
        success: bool,
        error: String,
    }

    match serde_json::from_slice::<Failure>(body) {
        Ok(failure) if !failure.success && failure.error == "ITEM_NOT_FOUND" => ManagedHlsError::NotFound,
        _ => ManagedHlsError::Unavailable,
    }
}

async fn probe_playlist(client: &reqwest::Client, url: &reqwest::Url) -> Result<(), ManagedHlsError> {
    tokio::time::timeout(PLAYLIST_PROBE_TIMEOUT, async {
        let request = client
            .get(url.clone())
            .header(ACCEPT, "application/vnd.apple.mpegurl, application/x-mpegURL")
            .build()
            .map_err(|_| ManagedHlsError::PlaylistUnavailable)?;
        let mut response = client.execute(request).await.map_err(|_| ManagedHlsError::PlaylistUnavailable)?;
        // Redirects are deliberately not followed. Only a complete, successful playlist counts as ready.
        if response.status() != reqwest::StatusCode::OK {
            return Err(ManagedHlsError::PlaylistUnavailable);
        }
        let body = read_capped(&mut response, PLAYLIST_BODY_LIMIT)
            .await
            .map_err(|_| ManagedHlsError::PlaylistUnavailable)?;
        let text = std::str::from_utf8(&body).map_err(|_| ManagedHlsError::PlaylistUnavailable)?;
        let first = text.lines().next().map(str::trim_end);
        if first != Some("#EXTM3U") {
            return Err(ManagedHlsError::PlaylistUnavailable);
        }
        Ok(())
    })
    .await
    .map_err(|_| ManagedHlsError::PlaylistUnavailable)?
}

async fn read_capped(response: &mut reqwest::Response, limit: usize) -> Result<Vec<u8>, ManagedHlsError> {
    // Refuse an already-known oversize answer before reading it, and reserve only within our hard ceiling. This
    // keeps the small create response to one allocation while an upstream cannot make us reserve arbitrary
    // memory with a hostile Content-Length. A decoded/chunked answer still meets the streaming check below.
    let declared = response.content_length().and_then(|length| usize::try_from(length).ok());
    if declared.is_some_and(|length| length > limit) {
        return Err(ManagedHlsError::Unavailable);
    }
    let mut body = Vec::with_capacity(declared.unwrap_or(0));
    while let Some(chunk) = response.chunk().await.map_err(|_| ManagedHlsError::Unavailable)? {
        if body.len().saturating_add(chunk.len()) > limit {
            return Err(ManagedHlsError::Unavailable);
        }
        body.extend_from_slice(&chunk);
    }
    Ok(body)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cache::MemoryCache;
    use crate::config::DebridAccount;

    struct MockDoer<F>(F)
    where
        F: Fn(reqwest::Request) -> Result<reqwest::Response, crate::scrape::DoError> + Send + Sync;

    #[async_trait]
    impl<F> Doer for MockDoer<F>
    where
        F: Fn(reqwest::Request) -> Result<reqwest::Response, crate::scrape::DoError> + Send + Sync,
    {
        async fn do_request(
            &self,
            request: reqwest::Request,
        ) -> Result<reqwest::Response, crate::scrape::DoError> {
            (self.0)(request)
        }
    }

    fn response(status: u16, body: &str) -> reqwest::Response {
        reqwest::Response::from(http::Response::builder().status(status).body(body.to_owned()).unwrap())
    }

    fn response_with_headers(status: u16, headers: &[(&str, &str)], body: &str) -> reqwest::Response {
        let mut answer = http::Response::builder().status(status);
        for (name, value) in headers {
            answer = answer.header(*name, *value);
        }
        reqwest::Response::from(answer.body(body.to_owned()).unwrap())
    }

    fn adapter(
        answer: impl Fn() -> Result<reqwest::Response, crate::scrape::DoError> + Send + Sync + 'static,
    ) -> TorBoxManagedHls {
        let doer = Arc::new(MockDoer(move |_| answer()));
        TorBoxManagedHls::new(doer, Arc::new(MemoryCache::new(1 << 20))).unwrap()
    }

    const SUCCESS: &[u8] = include_bytes!("../testdata/torbox-stream/create-success.json");
    const NOT_FOUND: &[u8] = include_bytes!("../testdata/torbox-stream/create-item-not-found.json");
    const PLAYLIST: &[u8] = include_bytes!("../testdata/torbox-stream/media-playlist.m3u8");

    #[test]
    fn successful_fixture_is_strict_and_url_is_redacted() {
        let url = parse_success(SUCCESS).expect("observed create response");
        assert_eq!(url.host_str(), Some("flux-002.wnam.tb-cdn.io"));
        let wrapped = ManagedHlsUrl(url);
        assert_eq!(format!("{wrapped:?}"), "ManagedHlsUrl([redacted])");
        assert!(!format!("{wrapped:?}").contains("token="));

        let mut changed: serde_json::Value = serde_json::from_slice(SUCCESS).unwrap();
        changed["data"]["scrobbling_enabled"] = serde_json::Value::Bool(true);
        assert_eq!(
            parse_success(&serde_json::to_vec(&changed).unwrap()),
            Err(ManagedHlsError::MalformedResponse)
        );
        changed["data"]["scrobbling_enabled"] = serde_json::Value::Bool(false);
        changed["data"]["subtitle_index"] = serde_json::json!(0);
        assert_eq!(
            parse_success(&serde_json::to_vec(&changed).unwrap()),
            Err(ManagedHlsError::MalformedResponse)
        );
    }

    #[test]
    fn item_not_found_is_the_only_provider_error_that_escapes() {
        assert_eq!(classify_failure(NOT_FOUND), ManagedHlsError::NotFound);
        assert_eq!(
            classify_failure(br#"{"success":false,"error":"ACTIVE_LIMIT","detail":"private"}"#),
            ManagedHlsError::Unavailable
        );
        assert_eq!(classify_failure(b"not json"), ManagedHlsError::Unavailable);
    }

    #[test]
    fn create_request_has_only_the_safe_v1_choices() {
        let request = create_request("secret-token", 42, Some(7)).expect("request");
        assert_eq!(request.method(), reqwest::Method::GET);
        assert_eq!(request.url().path(), "/v1/api/stream/createstream");
        let pairs: std::collections::HashMap<_, _> = request.url().query_pairs().into_owned().collect();
        assert_eq!(pairs.get("id").map(String::as_str), Some("42"));
        assert_eq!(pairs.get("file_id").map(String::as_str), Some("7"));
        assert_eq!(pairs.get("type").map(String::as_str), Some("torrent"));
        assert_eq!(pairs.get("chosen_audio_index").map(String::as_str), Some("0"));
        assert_eq!(pairs.get("scrobbling_enabled").map(String::as_str), Some("false"));
        assert!(!pairs.contains_key("chosen_subtitle_index"));
        assert!(!request.url().as_str().contains("secret-token"));
        assert_eq!(request.headers()[AUTHORIZATION], "Bearer secret-token");
    }

    #[tokio::test]
    async fn create_respects_rate_limits_without_retrying_and_preserves_safe_detail() {
        let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let seen = calls.clone();
        let managed = adapter(move || {
            seen.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            Ok(response_with_headers(429, &[("retry-after", "37")], "private provider detail"))
        });
        let captured = crate::loglimit::Capture::start();
        assert_eq!(
            managed.create("never-log-this-key", 123456, Some(789)).await,
            Err(ManagedHlsError::RateLimited { retry_after: Some(Duration::from_secs(37)) })
        );
        assert_eq!(calls.load(std::sync::atomic::Ordering::Relaxed), 1, "a throttle must not be retried");
        let log = captured.text();
        assert!(log.contains("cause=torbox_rate_limited http_status=429 retry_after_seconds=37"), "{log}");
        for secret in ["never-log-this-key", "123456", "789", "private provider detail"] {
            assert!(!log.contains(secret), "log leaked {secret:?}: {log}");
        }
    }

    #[tokio::test]
    async fn create_accepts_retry_after_http_date_and_caps_the_wait() {
        let managed = adapter(|| {
            Ok(response_with_headers(503, &[("retry-after", "Sat, 06 Nov 2094 08:49:37 GMT")], "ignored"))
        });
        assert_eq!(
            managed.create("key", 1, None).await,
            Err(ManagedHlsError::RateLimited { retry_after: Some(den_upstream::MAX_RETRY_AFTER) })
        );
    }

    #[tokio::test]
    async fn create_distinguishes_account_item_transport_and_shape_failures() {
        let cases = [
            (401, "{}", ManagedHlsError::Authentication),
            (403, "{}", ManagedHlsError::Refused),
            (402, "{}", ManagedHlsError::Entitlement),
            (500, std::str::from_utf8(NOT_FOUND).unwrap(), ManagedHlsError::NotFound),
            (200, "not json", ManagedHlsError::MalformedResponse),
            (
                200,
                r#"{"success":false,"error":"ACTIVE_LIMIT","detail":"private"}"#,
                ManagedHlsError::Unavailable,
            ),
        ];
        for (status, body, expected) in cases {
            let body = body.to_owned();
            let managed = adapter(move || Ok(response(status, &body)));
            assert_eq!(managed.create("key", 1, None).await, Err(expected), "http {status}");
        }

        let managed = adapter(|| Err(Box::new(std::io::Error::other("connection refused"))));
        assert_eq!(managed.create("key", 1, None).await, Err(ManagedHlsError::Transport));
    }

    #[tokio::test]
    async fn target_resolution_keeps_torbox_ids_inside_the_adapter() {
        let calls = Arc::new(std::sync::Mutex::new(Vec::new()));
        let seen = calls.clone();
        let doer = Arc::new(MockDoer(move |request| {
            let url = request.url().clone();
            seen.lock().unwrap().push(url.clone());
            if url.path().ends_with("/stream/createstream") {
                return Ok(response(500, std::str::from_utf8(NOT_FOUND).unwrap()));
            }
            if url.path().ends_with("/torrents/createtorrent") {
                return Ok(response(200, r#"{"success":true,"data":{"torrent_id":42}}"#));
            }
            if url.path().ends_with("/torrents/requestdl") {
                return Ok(response(200, r#"{"success":true,"data":"https://download.example/file.mkv"}"#));
            }
            let by_id = url.query_pairs().any(|(name, _)| name == "id");
            if by_id {
                return Ok(response(
                    200,
                    r#"{"success":true,"data":{"id":42,"files":[{"id":6,"name":"Show.S01E01.mkv","size":10},{"id":7,"name":"Show.S01E02.mkv","size":20}]}}"#,
                ));
            }
            Ok(response(
                200,
                r#"{"success":true,"data":[{"id":42,"hash":"0123456789abcdef0123456789abcdef01234567"}]}"#,
            ))
        }));
        let cache: Arc<dyn Cache> = Arc::new(MemoryCache::new(1 << 20));
        let adapter = TorBoxManagedHls::new(doer, cache).unwrap();
        let config = Config {
            debrid: vec![DebridAccount {
                service: DebridService::TorBox,
                token: "managed-target-test".to_owned(),
            }],
            ..Default::default()
        };
        let target = ResolveTarget {
            info_hash: "0123456789abcdef0123456789abcdef01234567".to_owned(),
            season: Some(1),
            episode: Some(2),
            ..Default::default()
        };
        assert_eq!(adapter.resolve_target(&config, &target).await, Err(ManagedHlsError::NotFound));
        let calls = calls.lock().unwrap();
        let create = calls.iter().find(|url| url.path().ends_with("/stream/createstream")).unwrap();
        let query: std::collections::HashMap<_, _> = create.query_pairs().into_owned().collect();
        assert_eq!(query.get("id").map(String::as_str), Some("42"));
        assert_eq!(query.get("file_id").map(String::as_str), Some("7"));
    }

    #[test]
    fn only_https_torbox_cdn_subdomains_are_accepted() {
        let good = "https://a.tb-cdn.io/x.m3u8?token=x&scrobbling_enabled=false";
        assert!(validate_hls_url(good).is_ok());
        for bad in [
            "http://a.tb-cdn.io/x.m3u8?scrobbling_enabled=false",
            "https://tb-cdn.io/x.m3u8?scrobbling_enabled=false",
            "https://eviltb-cdn.io/x.m3u8?scrobbling_enabled=false",
            "https://tb-cdn.io.evil.example/x.m3u8?scrobbling_enabled=false",
            "https://user:pass@a.tb-cdn.io/x.m3u8?scrobbling_enabled=false",
            "https://a.tb-cdn.io:444/x.m3u8?scrobbling_enabled=false",
            "https://a.tb-cdn.io/x.m3u8?scrobbling_enabled=false#fragment",
            "https://a.tb-cdn.io/x.m3u8?scrobbling_enabled=true",
        ] {
            assert_eq!(validate_hls_url(bad), Err(ManagedHlsError::UnsafeResponse), "accepted {bad}");
        }
    }

    #[tokio::test]
    async fn playlist_probe_accepts_the_fixture_and_refuses_redirects_and_oversize() {
        async fn serve(response: Vec<u8>) -> reqwest::Url {
            use tokio::io::{AsyncReadExt, AsyncWriteExt};
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let addr = listener.local_addr().unwrap();
            tokio::spawn(async move {
                let (mut socket, _) = listener.accept().await.unwrap();
                let mut request = [0u8; 2048];
                let _ = socket.read(&mut request).await;
                socket.write_all(&response).await.unwrap();
            });
            reqwest::Url::parse(&format!("http://{addr}/playlist.m3u8")).unwrap()
        }

        let client = reqwest::Client::builder().redirect(reqwest::redirect::Policy::none()).build().unwrap();
        let ok =
            format!("HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n", PLAYLIST.len())
                .into_bytes();
        let mut ok = ok;
        ok.extend_from_slice(PLAYLIST);
        assert_eq!(probe_playlist(&client, &serve(ok).await).await, Ok(()));

        let redirect = b"HTTP/1.1 302 Found\r\nLocation: https://evil.example/x.m3u8\r\nContent-Length: 0\r\nConnection: close\r\n\r\n".to_vec();
        assert_eq!(
            probe_playlist(&client, &serve(redirect).await).await,
            Err(ManagedHlsError::PlaylistUnavailable)
        );

        let large = vec![b'x'; PLAYLIST_BODY_LIMIT + 1];
        let mut response =
            format!("HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n", large.len())
                .into_bytes();
        response.extend_from_slice(&large);
        assert_eq!(
            probe_playlist(&client, &serve(response).await).await,
            Err(ManagedHlsError::PlaylistUnavailable)
        );
    }

    #[tokio::test]
    async fn capped_read_rejects_a_declared_oversize_body_before_buffering() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut request = [0u8; 2048];
            let _ = socket.read(&mut request).await;
            socket
                .write_all(
                    b"HTTP/1.1 200 OK\r\nContent-Length: 17\r\nConnection: close\r\n\r\n0123456789abcdefg",
                )
                .await
                .unwrap();
        });
        let mut response = reqwest::get(format!("http://{addr}/body")).await.unwrap();
        assert_eq!(read_capped(&mut response, 16).await, Err(ManagedHlsError::Unavailable));
    }
}
