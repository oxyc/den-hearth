//! TorBox's public status page as a slow, cached, secondary health signal.
//!
//! The page is never read from a play, probe, or list request. One process task refreshes it every five minutes
//! with a two-second whole-request deadline. Scout's observations of the API remain authoritative whenever they
//! exist; this snapshot can only corroborate them, or provide an informational fallback before Scout has made a
//! TorBox call. A failed refresh keeps a bounded stale snapshot and otherwise says nothing (fail open).

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use serde::Deserialize;
use serde_json::{json, Value};
use tokio::time::Instant;

use crate::loglimit::{emit, log_limited};
use crate::scrape::Doer;

const STATUS_URL: &str = "https://status.torbox.app/index.json";
const POLL_EVERY: Duration = Duration::from_secs(5 * 60);
const POLL_TIMEOUT: Duration = Duration::from_secs(2);
/// The live document is about 130 KiB. This admits normal growth but prevents an upstream response from becoming
/// an unbounded allocation in a background observer.
const MAX_STATUS_BYTES: usize = 512 << 10;
/// Two missed polls make a snapshot visibly stale. It remains useful during a status-site failure for a bounded
/// period, then disappears rather than leaving a recovered provider marked down indefinitely.
const FRESH_FOR: Duration = Duration::from_secs(10 * 60);
const RETAIN_FOR: Duration = Duration::from_secs(30 * 60);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PublicState {
    Operational,
    Degraded,
    Down,
}

impl PublicState {
    fn as_str(self) -> &'static str {
        match self {
            Self::Operational => "operational",
            Self::Degraded => "degraded",
            Self::Down => "down",
        }
    }
}

#[derive(Clone, Copy)]
struct Snapshot {
    state: PublicState,
    checked_at_ms: u64,
    stored_at: Instant,
}

pub(crate) struct PublicStatus {
    snapshot: Mutex<Option<Snapshot>>,
    started: AtomicBool,
}

impl Default for PublicStatus {
    fn default() -> Self {
        Self { snapshot: Mutex::new(None), started: AtomicBool::new(false) }
    }
}

#[derive(Deserialize)]
struct Document {
    #[serde(default)]
    included: Vec<Included>,
}

#[derive(Deserialize)]
struct Included {
    #[serde(rename = "type")]
    kind: String,
    #[serde(default)]
    attributes: Attributes,
}

#[derive(Default, Deserialize)]
struct Attributes {
    public_name: Option<String>,
    status: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PollOutcome {
    Ok(PublicState),
    Timeout,
    Unreachable,
    Http,
    Oversized,
    Invalid,
}

impl PollOutcome {
    fn metric(self) -> &'static str {
        match self {
            Self::Ok(_) => "ok",
            Self::Timeout => "timeout",
            Self::Unreachable => "unreachable",
            Self::Http => "http_error",
            Self::Oversized => "oversized",
            Self::Invalid => "invalid",
        }
    }
}

fn now_ms() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).map_or(0, |d| d.as_millis() as u64)
}

impl PublicStatus {
    fn lock(&self) -> std::sync::MutexGuard<'_, Option<Snapshot>> {
        self.snapshot.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Idempotently start the background refresh. The first fetch happens immediately; later ones are paced from
    /// completion, so a slow status service can never make requests pile up.
    pub(crate) fn start(self: &Arc<Self>, client: Arc<dyn Doer>) {
        if self.started.swap(true, Ordering::AcqRel) {
            return;
        }
        let state = Arc::clone(self);
        tokio::spawn(async move {
            loop {
                let outcome = poll(client.as_ref()).await;
                match outcome {
                    PollOutcome::Ok(value) => state.record(value),
                    failure => log_limited(
                        "torbox-public-status-poll",
                        &format!("TorBox public status poll failed ({})", failure.metric()),
                    ),
                }
                tokio::time::sleep(POLL_EVERY).await;
            }
        });
    }

    fn record(&self, state: PublicState) {
        let mut slot = self.lock();
        if slot.is_none_or(|old| old.state != state) {
            let previous = slot.map_or("unknown", |old| old.state.as_str());
            emit(&format!("TorBox public status: {previous} → {}", state.as_str()));
        }
        *slot = Some(Snapshot { state, checked_at_ms: now_ms(), stored_at: Instant::now() });
    }

    /// A sanitized report containing only Den's fixed vocabulary. Titles, incident messages, URLs, and any other
    /// status-page text are deliberately never copied into logs or a response.
    pub(crate) fn report(&self) -> Option<Value> {
        let snapshot = (*self.lock())?;
        let age = snapshot.stored_at.elapsed();
        if age > RETAIN_FOR {
            return None;
        }
        Some(json!({
            "source": "torbox_status_page",
            "state": snapshot.state.as_str(),
            "checkedAt": snapshot.checked_at_ms,
            "stale": age > FRESH_FOR,
        }))
    }

    #[cfg(test)]
    fn record_at(&self, state: PublicState, stored_at: Instant) {
        *self.lock() = Some(Snapshot { state, checked_at_ms: now_ms(), stored_at });
    }

    #[cfg(feature = "test-support")]
    pub(crate) fn record_for_test(&self, state: &str) {
        let state = match state {
            "operational" => PublicState::Operational,
            "degraded" => PublicState::Degraded,
            "down" => PublicState::Down,
            _ => panic!("unknown TorBox public status test state: {state}"),
        };
        self.record(state);
    }
}

async fn poll(client: &dyn Doer) -> PollOutcome {
    match tokio::time::timeout(POLL_TIMEOUT, fetch(client)).await {
        Ok(outcome) => outcome,
        Err(_) => PollOutcome::Timeout,
    }
}

async fn fetch(client: &dyn Doer) -> PollOutcome {
    let Ok(url) = reqwest::Url::parse(STATUS_URL) else { return PollOutcome::Invalid };
    let mut request = reqwest::Request::new(reqwest::Method::GET, url);
    request
        .headers_mut()
        .insert(reqwest::header::ACCEPT, reqwest::header::HeaderValue::from_static("application/json"));
    request.headers_mut().insert(
        reqwest::header::USER_AGENT,
        reqwest::header::HeaderValue::from_static("den-hearth torbox-status-observer"),
    );
    let mut response = match client.do_request(request).await {
        Ok(response) => response,
        Err(_) => return PollOutcome::Unreachable,
    };
    if !response.status().is_success() {
        return PollOutcome::Http;
    }
    let mut body = Vec::new();
    loop {
        match response.chunk().await {
            Ok(Some(chunk)) if body.len().saturating_add(chunk.len()) <= MAX_STATUS_BYTES => {
                body.extend_from_slice(&chunk);
            }
            Ok(Some(_)) => return PollOutcome::Oversized,
            Ok(None) => break,
            Err(_) => return PollOutcome::Unreachable,
        }
    }
    parse(&body).map_or(PollOutcome::Invalid, PollOutcome::Ok)
}

/// The current Better Uptime JSON: resources live in `included`, where `type=status_page_resource` and
/// `attributes.public_name/status` identify them. Only API and Database describe the calls Scout makes; a CDN,
/// WebDAV, Usenet, or transcoding incident must not be presented as a debrid-API outage.
fn parse(body: &[u8]) -> Option<PublicState> {
    let document: Document = serde_json::from_slice(body).ok()?;
    let mut api = None;
    let mut database = None;
    for item in document.included {
        if item.kind != "status_page_resource" {
            continue;
        }
        match item.attributes.public_name.as_deref() {
            Some("API") => api = Some(page_state(item.attributes.status.as_deref()?)?),
            Some("Database") => database = Some(page_state(item.attributes.status.as_deref()?)?),
            _ => {}
        }
    }
    Some(worst(api?, database?))
}

fn page_state(value: &str) -> Option<PublicState> {
    match value {
        "operational" => Some(PublicState::Operational),
        "degraded" | "maintenance" => Some(PublicState::Degraded),
        "downtime" => Some(PublicState::Down),
        // `not_monitored` is not green, and a future spelling is not evidence. Unknown schema fails open.
        _ => None,
    }
}

fn worst(a: PublicState, b: PublicState) -> PublicState {
    match (a, b) {
        (PublicState::Down, _) | (_, PublicState::Down) => PublicState::Down,
        (PublicState::Degraded, _) | (_, PublicState::Degraded) => PublicState::Degraded,
        _ => PublicState::Operational,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::stores::torbox::health::{CallOutcome, DOWN_AFTER};
    use crate::stores::torbox::TorboxRuntime;

    fn document(api: &str, database: &str) -> Vec<u8> {
        format!(
            r#"{{"data":{{"attributes":{{"aggregate_state":"operational"}}}},"included":[
                {{"type":"status_page_resource","attributes":{{"public_name":"API","status":"{api}"}}}},
                {{"type":"status_page_resource","attributes":{{"public_name":"Database","status":"{database}"}}}},
                {{"type":"status_page_resource","attributes":{{"public_name":"WEUR CDN","status":"downtime"}}}},
                {{"type":"status_page_resource","attributes":{{"public_name":"Transcoding Service","status":"not_monitored"}}}},
                {{"type":"status_report","attributes":{{"title":"untrusted text","aggregate_state":"ongoing"}}}}
            ]}}"#
        )
        .into_bytes()
    }

    #[test]
    fn parses_only_the_api_and_database_resources() {
        assert_eq!(parse(&document("operational", "operational")), Some(PublicState::Operational));
        assert_eq!(parse(&document("degraded", "operational")), Some(PublicState::Degraded));
        assert_eq!(parse(&document("operational", "downtime")), Some(PublicState::Down));
    }

    #[test]
    fn unknown_or_incomplete_schema_fails_open() {
        assert_eq!(parse(&document("not_monitored", "operational")), None);
        assert_eq!(parse(br#"{"included":[]}"#), None);
        assert_eq!(parse(b"not json"), None);
    }

    #[tokio::test(start_paused = true)]
    async fn a_snapshot_goes_stale_then_expires() {
        let status = PublicStatus::default();
        status.record_at(PublicState::Down, Instant::now());
        assert_eq!(status.report().unwrap()["stale"], false);
        tokio::time::advance(FRESH_FOR + Duration::from_secs(1)).await;
        assert_eq!(status.report().unwrap()["stale"], true);
        tokio::time::advance(RETAIN_FOR - FRESH_FOR).await;
        assert!(status.report().is_none());
    }

    #[test]
    fn public_status_is_a_fallback_then_only_corroboration() {
        let runtime = TorboxRuntime::new();
        runtime.public_status.record_at(PublicState::Down, Instant::now());
        let fallback = runtime.health_report().expect("public fallback");
        assert_eq!(fallback["state"], "down");
        assert_eq!(fallback["source"], "status_page");

        // One successful local API answer is authoritative even while the public page says down. The page remains
        // attached so a client can say that TorBox itself reports an incident without changing local admission.
        runtime.health.record(CallOutcome::Answered(Duration::from_millis(50)), Instant::now());
        let local = runtime.health_report().expect("local report");
        assert_eq!(local["state"], "ok");
        assert_eq!(local["source"], "local");
        assert_eq!(local["statusPage"]["state"], "down");
    }

    #[test]
    fn a_green_status_page_never_overrides_local_failure() {
        let runtime = TorboxRuntime::new();
        runtime.public_status.record_at(PublicState::Operational, Instant::now());
        for _ in 0..DOWN_AFTER {
            runtime.health.record(CallOutcome::Failed("timeout"), Instant::now());
        }
        let report = runtime.health_report().expect("local report");
        assert_eq!(report["state"], "down");
        assert_eq!(report["source"], "local");
        assert_eq!(report["reason"], "timeout");
        assert_eq!(report["statusPage"]["state"], "operational");
    }
}
