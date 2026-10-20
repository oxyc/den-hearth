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

use crate::cache::{Cache, CacheClass};
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
const SUBTITLE_SIDECAR_TTL: Duration = Duration::from_secs(30 * 24 * 60 * 60);
/// TorBox's published managed-stream CDN base domains. Keep this an explicit list: accepting an arbitrary
/// `tb-cdn.*` suffix would turn TorBox's signed URL into an open redirect to a lookalike domain.
const MANAGED_HLS_CDN_SUFFIXES: [&str; 7] =
    [".tb-cdn.cx", ".tb-cdn.io", ".tb-cdn.pw", ".tb-cdn.sh", ".tb-cdn.st", ".tb-cdn.to", ".tb-cdn.earth"];

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
    /// Asked for a Cast receiver, which plays TorBox's stream as it is: a release `playable` says its video would
    /// have to be converted for is left to Remux, rather than handed over to fail on the TV.
    pub receiver: bool,
    /// TorBox's zero-based position in `metadata.subtitles`. None keeps subtitles out of the managed playlist.
    pub subtitle: Option<i64>,
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
    pub subtitles: Vec<ManagedHlsSubtitle>,
    pub selected_subtitle: Option<i64>,
}

/// One embedded track TorBox found in the selected file. All strings are display metadata, never capabilities.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ManagedHlsSubtitle {
    pub index: i64,
    pub language: Option<String>,
    pub name: Option<String>,
    pub codec: Option<String>,
    /// Complete, formatting-neutral WebVTT extracted through Matroska's index. Never an upstream capability.
    pub sidecar: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ManagedHlsPrepared {
    pub url: ManagedHlsUrl,
    pub subtitles: Vec<ManagedHlsSubtitle>,
    pub selected_subtitle: Option<i64>,
}

/// The internal seam Handler uses, so title selection and provider resolution stay separate responsibilities.
#[async_trait]
pub trait ManagedHlsResolver: Send + Sync {
    async fn resolve(
        &self,
        config: &Config,
        target: &ResolveTarget,
        subtitle: Option<i64>,
    ) -> Result<ManagedHlsPrepared, ManagedHlsError>;
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
    subtitle_slots: Arc<tokio::sync::Semaphore>,
}

impl TorBoxManagedHls {
    pub fn new(create_client: Arc<dyn Doer>, cache: Arc<dyn Cache>) -> Result<Self, ManagedHlsError> {
        let probe_client = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .timeout(PLAYLIST_PROBE_TIMEOUT)
            .build()
            .map_err(|_| ManagedHlsError::Unavailable)?;
        Ok(Self {
            create_client,
            cache,
            probe_client,
            // One extractor may use high concurrency for tiny, sparse CDN ranges. Keeping a single global slot
            // prevents simultaneous viewers from multiplying that bounded fan-out against TorBox.
            subtitle_slots: Arc::new(tokio::sync::Semaphore::new(1)),
        })
    }

    /// Resolves Scout's ordinary target to the account-local TorBox torrent and file, then creates and probes
    /// one managed stream. Provider ids never cross this boundary. Torrent acquisition goes through the same
    /// add-budgeted, refusal-aware path as ordinary Scout playback.
    async fn resolve_target(
        &self,
        config: &Config,
        target: &ResolveTarget,
        subtitle: Option<i64>,
    ) -> Result<ManagedHlsPrepared, ManagedHlsError> {
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
        let (torrent_id, file_id, direct_file) = store
            .prepare_managed_item(target)
            .await
            .map_err(classify_store_error)
            .map_err(report_resolution_failure)?;

        let sidecar_key = subtitle.map(|ordinal| {
            format!(
                "torbox-sidecar-v1:{}:{}:{ordinal}",
                target.info_hash,
                file_id.map_or_else(|| "single".to_owned(), |value| value.to_string())
            )
        });
        let cached_sidecar = sidecar_key
            .as_deref()
            .and_then(|key| self.cache.get(key))
            .and_then(|vtt| (vtt.starts_with("WEBVTT\n") && vtt.len() <= 2 * 1024 * 1024).then_some(vtt));
        // A hit never competes for the extraction slot. This also makes switching back to a track instant after
        // its first complete extraction, across sessions and service restarts when the configured cache persists.
        let subtitle_slot =
            cached_sidecar.is_none().then(|| self.subtitle_slots.clone().try_acquire_owned().ok()).flatten();
        let cache = self.cache.clone();
        let extract = async move {
            match (direct_file, subtitle_slot, subtitle, cached_sidecar) {
                (_, _, Some(ordinal), Some(vtt)) => {
                    vec![crate::managed_subtitles::Sidecar { ordinal, vtt: vtt.to_string() }]
                }
                // The metadata-only request has no selected track to attach. Extracting every track here both
                // delays initial playback and repeats the chosen track's work when the player applies its
                // default immediately afterwards.
                (_, _, None, _) => Vec::new(),
                (Some(direct_file), Some(_slot), Some(_), None) => {
                    let sidecars = crate::managed_subtitles::sidecars(&direct_file, subtitle).await;
                    if let (Some(key), [sidecar]) = (sidecar_key.as_deref(), sidecars.as_slice()) {
                        cache.put_class(key, &sidecar.vtt, SUBTITLE_SIDECAR_TTL, CacheClass::Durable);
                    }
                    sidecars
                }
                (None, _, _, None) => {
                    report_sidecar_skipped("direct_file_unavailable", subtitle);
                    Vec::new()
                }
                _ => {
                    report_sidecar_skipped("extractor_busy", subtitle);
                    Vec::new()
                }
            }
        };
        // A successful sidecar must sit beside a subtitle-free TorBox stream. Asking TorBox for the same
        // subtitle here burns it into the video, leaving the browser to draw a second copy over it. Start the
        // clean stream while the indexed extraction runs; only ask TorBox for its legacy selected-subtitle
        // stream when extraction cannot produce a complete sidecar.
        let (created, sidecars) = tokio::join!(self.create(token, torrent_id, file_id, None), extract);
        let created = match (created, subtitle, sidecars.is_empty()) {
            (Ok(_), Some(ordinal), true) => self.create(token, torrent_id, file_id, Some(ordinal)).await,
            (created, _, _) => created,
        };
        match created {
            Err(ManagedHlsError::NotFound) => {
                store.forget_torrent_id(&target.info_hash);
                Err(ManagedHlsError::NotFound)
            }
            Ok(mut prepared) => {
                if !sidecars.is_empty() {
                    log_limited(
                        "managed-subtitle-sidecar-ready",
                        &format!("managed HLS: prepared {} indexed subtitle sidecar(s)", sidecars.len()),
                    );
                }
                for sidecar in sidecars {
                    if let Some(track) =
                        prepared.subtitles.iter_mut().find(|track| track.index == sidecar.ordinal)
                    {
                        track.sidecar = Some(sidecar.vtt);
                    }
                }
                Ok(prepared)
            }
            Err(error) => Err(error),
        }
    }

    async fn create(
        &self,
        token: &str,
        torrent_id: i64,
        file_id: Option<i64>,
        subtitle: Option<i64>,
    ) -> Result<ManagedHlsPrepared, ManagedHlsError> {
        let request = create_request(token, torrent_id, file_id, subtitle)
            .map_err(report_create_failure_without_status)?;
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

        let prepared = parse_success(&body, subtitle).map_err(|error| {
            metrics().torbox_call("createstream", false);
            report_create_failure(error, Some(status))
        })?;
        metrics().torbox_call("createstream", true);
        probe_playlist(&self.probe_client, &prepared.url.0).await.map_err(report_probe_failure)?;
        Ok(prepared)
    }
}

fn report_sidecar_skipped(reason: &'static str, selected: Option<i64>) {
    log_limited(
        &format!("managed-subtitle-sidecar-skipped-{reason}"),
        &format!(
            "managed HLS: indexed subtitle sidecar unavailable: outcome=skipped reason={reason} selected={}",
            selected.map_or_else(|| "all".to_owned(), |value| value.to_string())
        ),
    );
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
        subtitle: Option<i64>,
    ) -> Result<ManagedHlsPrepared, ManagedHlsError> {
        self.resolve_target(config, target, subtitle).await
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
    subtitle: Option<i64>,
) -> Result<reqwest::Request, ManagedHlsError> {
    let mut url = reqwest::Url::parse(CREATE_STREAM_URL).map_err(|_| ManagedHlsError::Unavailable)?;
    {
        let mut query = url.query_pairs_mut();
        query.append_pair("id", &torrent_id.to_string());
        if let Some(file_id) = file_id {
            query.append_pair("file_id", &file_id.to_string());
        }
        query.append_pair("type", "torrent");
        if let Some(subtitle) = subtitle {
            query.append_pair("chosen_subtitle_index", &subtitle.to_string());
        }
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
    subtitle_index: serde_json::Value,
    scrobbling_enabled: bool,
    hls_url: String,
    metadata: CreateMetadata,
}

#[derive(Deserialize)]
struct CreateMetadata {
    #[serde(default)]
    subtitles: Vec<CreateSubtitle>,
}

#[derive(Deserialize)]
struct CreateSubtitle {
    #[serde(rename = "index")]
    _stream_index: i64,
    language: Option<String>,
    language_full: Option<String>,
    title: Option<String>,
    codec: Option<String>,
}

fn parse_success(
    body: &[u8],
    requested_subtitle: Option<i64>,
) -> Result<ManagedHlsPrepared, ManagedHlsError> {
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
    let selected_subtitle = match &data.subtitle_index {
        serde_json::Value::Null => None,
        serde_json::Value::String(value) if value == "null" => None,
        serde_json::Value::String(value) => value.parse().ok(),
        serde_json::Value::Number(value) => value.as_i64(),
        _ => return Err(ManagedHlsError::MalformedResponse),
    };
    if data.audio_index != 0 || selected_subtitle != requested_subtitle || data.scrobbling_enabled {
        return Err(ManagedHlsError::MalformedResponse);
    }
    let url = ManagedHlsUrl(validate_hls_url(&data.hls_url)?);
    let subtitles = data
        .metadata
        .subtitles
        .into_iter()
        // TorBox's own player sends the array position to `chosen_subtitle_index`. `track.index` is the
        // underlying media stream index and can include video/audio streams, so sending it selects another
        // subtitle (or no useful one) in multi-track files.
        .enumerate()
        .map(|(selection_index, track)| ManagedHlsSubtitle {
            index: selection_index as i64,
            language: track.language,
            name: track.title.or(track.language_full),
            codec: track.codec,
            sidecar: None,
        })
        .collect();
    Ok(ManagedHlsPrepared { url, subtitles, selected_subtitle })
}

fn validate_hls_url(raw: &str) -> Result<reqwest::Url, ManagedHlsError> {
    let url = reqwest::Url::parse(raw).map_err(|_| unsafe_hls_url("not_a_url", None))?;
    let host = url.host_str().ok_or_else(|| unsafe_hls_url("no_host", Some(&url)))?;
    let torbox_cdn = MANAGED_HLS_CDN_SUFFIXES
        .iter()
        .any(|suffix| host.strip_suffix(suffix).is_some_and(|label| !label.is_empty()));
    // The signed path and query are opaque TorBox output. Privacy is enforced by the false request parameter
    // and the false response field checked above; interpreting a duplicate provider query here made harmless
    // signing changes break playback. Keep this boundary about URL transport and ownership instead.
    let reason = if url.scheme() != "https" {
        Some("not_https")
    } else if !url.username().is_empty() || url.password().is_some() {
        Some("credentials")
    } else if url.fragment().is_some() {
        Some("fragment")
    } else if url.port().is_some_and(|port| port != 443) {
        Some("nonstandard_port")
    } else if !torbox_cdn {
        Some("unrecognized_cdn")
    } else {
        None
    };
    match reason {
        Some(reason) => Err(unsafe_hls_url(reason, Some(&url))),
        None => Ok(url),
    }
}

/// Record enough of a refused URL to diagnose a TorBox CDN contract change without ever recording its signed
/// path or query. The suffix is deliberately restricted to a short DNS-shaped `tb-cdn.*` value; an arbitrary
/// upstream hostname is reported only as `other`.
fn unsafe_hls_url(reason: &'static str, url: Option<&reqwest::Url>) -> ManagedHlsError {
    let scheme = match url.map(reqwest::Url::scheme) {
        Some("https") => "https",
        Some(_) => "other",
        None => "unknown",
    };
    let port = match url.and_then(reqwest::Url::port) {
        Some(443) => "443".to_owned(),
        Some(port) => port.to_string(),
        None => "default".to_owned(),
    };
    let cdn_suffix = url
        .and_then(reqwest::Url::host_str)
        .and_then(|host| host.find("tb-cdn.").map(|at| &host[at..]))
        .filter(|suffix| {
            suffix.strip_prefix("tb-cdn.").is_some_and(|tld| {
                (2..=10).contains(&tld.len()) && tld.bytes().all(|byte| byte.is_ascii_lowercase())
            })
        })
        .unwrap_or("other");
    log_limited(
        "torbox_unsafe_stream_url_detail",
        &format!(
            "event=managed_hls_url_validation outcome=refused reason={reason} scheme={scheme} \
             cdn_suffix={cdn_suffix} port={port}"
        ),
    );
    ManagedHlsError::UnsafeResponse
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
    fn skipped_sidecar_log_names_the_reason_without_provider_details() {
        let captured = crate::loglimit::capture(|| {
            report_sidecar_skipped("direct_file_unavailable", Some(2));
        });
        assert!(captured.contains("outcome=skipped reason=direct_file_unavailable selected=2"), "{captured}");
    }

    #[test]
    fn successful_fixture_is_strict_and_url_is_redacted() {
        let prepared = parse_success(SUCCESS, None).expect("observed create response");
        assert_eq!(prepared.url.0.host_str(), Some("flux-002.wnam.tb-cdn.io"));
        assert_eq!(format!("{:?}", prepared.url), "ManagedHlsUrl([redacted])");
        assert_eq!(prepared.subtitles.len(), 1);
        assert_eq!(prepared.subtitles[0].language.as_deref(), Some("eng"));

        let mut changed: serde_json::Value = serde_json::from_slice(SUCCESS).unwrap();
        changed["data"]["hls_url"] =
            serde_json::json!("https://flux-002.wnam.tb-cdn.io/opaque/index.m3u8?token=secret");
        assert!(parse_success(&serde_json::to_vec(&changed).unwrap(), None).is_ok());

        changed["data"]["scrobbling_enabled"] = serde_json::Value::Bool(true);
        assert_eq!(
            parse_success(&serde_json::to_vec(&changed).unwrap(), None),
            Err(ManagedHlsError::MalformedResponse)
        );
        changed["data"]["scrobbling_enabled"] = serde_json::Value::Bool(false);
        changed["data"]["subtitle_index"] = serde_json::json!(0);
        assert_eq!(
            parse_success(&serde_json::to_vec(&changed).unwrap(), Some(0))
                .map(|prepared| prepared.selected_subtitle),
            Ok(Some(0))
        );
    }

    #[test]
    fn subtitle_choices_are_metadata_positions_not_media_stream_indexes() {
        let mut changed: serde_json::Value = serde_json::from_slice(SUCCESS).unwrap();
        changed["data"]["metadata"]["subtitles"] = serde_json::json!([
            {
                "codec": "subrip",
                "codec_type": "subtitle",
                "default": false,
                "index": 7,
                "language": "ara",
                "language_full": "Arabic",
                "title": "Arabic"
            },
            {
                "codec": "subrip",
                "codec_type": "subtitle",
                "default": false,
                "index": 11,
                "language": "eng",
                "language_full": "English",
                "title": "English"
            }
        ]);

        let prepared = parse_success(&serde_json::to_vec(&changed).unwrap(), None).unwrap();
        assert_eq!(prepared.subtitles[0].index, 0);
        assert_eq!(prepared.subtitles[1].index, 1);
        assert_eq!(prepared.subtitles[1].language.as_deref(), Some("eng"));
    }

    #[test]
    fn every_published_torbox_managed_stream_domain_is_accepted() {
        for suffix in MANAGED_HLS_CDN_SUFFIXES {
            let raw = format!("https://flux-002.wnam{suffix}/playlist.m3u8?token=secret");
            let url = validate_hls_url(&raw).unwrap_or_else(|error| panic!("{suffix}: {error:?}"));
            assert_eq!(url.host_str(), Some(format!("flux-002.wnam{suffix}").as_str()));
        }
    }

    #[test]
    fn torbox_cdn_allow_list_does_not_accept_lookalikes_or_base_domains() {
        for host in [
            "tb-cdn.io",
            "tb-cdn.earth",
            "flux.tb-cdn.invalid",
            "flux.tb-cdn.io.example.com",
            "flux.tb-cdn.earth.example.com",
            "flux.not-tb-cdn.io",
        ] {
            let raw = format!("https://{host}/playlist.m3u8?scrobbling_enabled=false");
            assert_eq!(validate_hls_url(&raw), Err(ManagedHlsError::UnsafeResponse), "{host}");
        }
    }

    #[test]
    fn unsafe_url_log_names_the_stage_without_leaking_the_signed_url() {
        let captured = crate::loglimit::capture(|| {
            assert_eq!(
                validate_hls_url(
                    "https://flux.tb-cdn.future/secret/movie/path.m3u8?token=never-log-me&scrobbling_enabled=false"
                ),
                Err(ManagedHlsError::UnsafeResponse)
            );
        });
        assert!(captured.contains("event=managed_hls_url_validation outcome=refused"), "{captured}");
        assert!(captured.contains("reason=unrecognized_cdn"), "{captured}");
        assert!(captured.contains("scheme=https cdn_suffix=tb-cdn.future port=default"), "{captured}");
        for secret in ["never-log-me", "secret", "movie", "path.m3u8", "flux"] {
            assert!(!captured.contains(secret), "log leaked {secret:?}: {captured}");
        }
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
        let request = create_request("secret-token", 42, Some(7), Some(3)).expect("request");
        assert_eq!(request.method(), reqwest::Method::GET);
        assert_eq!(request.url().path(), "/v1/api/stream/createstream");
        let pairs: std::collections::HashMap<_, _> = request.url().query_pairs().into_owned().collect();
        assert_eq!(pairs.get("id").map(String::as_str), Some("42"));
        assert_eq!(pairs.get("file_id").map(String::as_str), Some("7"));
        assert_eq!(pairs.get("type").map(String::as_str), Some("torrent"));
        assert_eq!(pairs.get("chosen_audio_index").map(String::as_str), Some("0"));
        assert_eq!(pairs.get("scrobbling_enabled").map(String::as_str), Some("false"));
        assert_eq!(pairs.get("chosen_subtitle_index").map(String::as_str), Some("3"));
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
            managed.create("never-log-this-key", 123456, Some(789), None).await,
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
            managed.create("key", 1, None, None).await,
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
            assert_eq!(managed.create("key", 1, None, None).await, Err(expected), "http {status}");
        }

        let managed = adapter(|| Err(Box::new(std::io::Error::other("connection refused"))));
        assert_eq!(managed.create("key", 1, None, None).await, Err(ManagedHlsError::Transport));
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
        assert_eq!(adapter.resolve_target(&config, &target, None).await, Err(ManagedHlsError::NotFound));
        let calls = calls.lock().unwrap();
        let create = calls.iter().find(|url| url.path().ends_with("/stream/createstream")).unwrap();
        let query: std::collections::HashMap<_, _> = create.query_pairs().into_owned().collect();
        assert_eq!(query.get("id").map(String::as_str), Some("42"));
        assert_eq!(query.get("file_id").map(String::as_str), Some("7"));
    }

    #[test]
    fn only_https_torbox_cdn_subdomains_are_accepted() {
        for good in [
            "https://a.tb-cdn.io/x.m3u8?token=x&future=value",
            "https://a.tb-cdn.io/x.m3u8?token=x&scrobbling_enabled=false&future=value",
            "https://a.tb-cdn.io/x.m3u8?scrobbling_enabled=false&scrobbling_enabled=false",
            "https://a.tb-cdn.io/x.m3u8?scrobbling_enabled=true",
            "https://a.tb-cdn.io/x.m3u8?scrobbling_enabled=true&scrobbling_enabled=false",
        ] {
            assert!(validate_hls_url(good).is_ok(), "refused {good}");
        }
        for bad in [
            "http://a.tb-cdn.io/x.m3u8?scrobbling_enabled=false",
            "https://tb-cdn.io/x.m3u8?scrobbling_enabled=false",
            "https://eviltb-cdn.io/x.m3u8?scrobbling_enabled=false",
            "https://tb-cdn.io.evil.example/x.m3u8?scrobbling_enabled=false",
            "https://user:pass@a.tb-cdn.io/x.m3u8?scrobbling_enabled=false",
            "https://a.tb-cdn.io:444/x.m3u8?scrobbling_enabled=false",
            "https://a.tb-cdn.io/x.m3u8?scrobbling_enabled=false#fragment",
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
