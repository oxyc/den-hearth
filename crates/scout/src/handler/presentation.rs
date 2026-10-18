//! Stable HTTP and log presentation for queued playback answers.

use super::*;

pub(crate) fn write_queued(
    target: &ResolveTarget,
    request_id: &str,
    duration: Duration,
    status: &StoreStatus,
) -> Resp {
    let rate = status
        .bytes_per_second
        .map_or_else(|| "unknown".to_owned(), |bytes| format!("{:.1} Mbps", bytes as f64 * 8.0 / 1e6));
    let eta = status.eta_seconds.map_or_else(|| "none".to_owned(), |seconds| format!("{seconds}s"));
    let swarm = if status.seeds.is_some() || status.peers.is_some() {
        format!(", seeds {}, peers {}", optional_count(status.seeds), optional_count(status.peers))
    } else {
        String::new()
    };
    let reason =
        format!("{} {:.1}% at {rate}, eta {eta}{swarm}", queued_state(status), status.progress * 100.0);
    log_limited(
        "downloading",
        &play_decision(
            "play",
            "fallback",
            &reason,
            status.service.map_or("", |service| service.as_str()),
            duration,
            request_id,
            target,
        ),
    );
    let mut response = write_queued_body(status);
    response.extensions_mut().insert(ReleaseAnswer::new(
        queued_state(status),
        status.service.map_or("", |service| service.as_str()),
    ));
    response
}

fn optional_count(value: Option<i64>) -> String {
    value.map_or_else(|| "?".to_owned(), |value| value.to_string())
}

fn queued_state(status: &StoreStatus) -> &str {
    if status.state.is_empty() {
        FETCH_DOWNLOADING
    } else {
        &status.state
    }
}

pub(crate) fn write_queued_body(status: &StoreStatus) -> Resp {
    json_response(StatusCode::ACCEPTED, &queued_body(status))
}

pub(crate) fn queued_body(status: &StoreStatus) -> serde_json::Value {
    let mut body = serde_json::Map::new();
    body.insert("state".to_owned(), json!(queued_state(status)));
    body.insert("progress".to_owned(), progress_number(status.progress));
    if let Some(service) = status.service {
        body.insert("service".to_owned(), json!(service.as_str()));
    }
    if let Some(seeds) = status.seeds {
        body.insert("seeds".to_owned(), json!(seeds));
    }
    if let Some(peers) = status.peers {
        body.insert("peers".to_owned(), json!(peers));
    }
    if let Some(eta) = status.eta_seconds {
        body.insert("etaSeconds".to_owned(), json!(eta));
    }
    if let Some(bytes_per_second) = status.bytes_per_second {
        body.insert("bytesPerSecond".to_owned(), json!(bytes_per_second));
    }
    serde_json::Value::Object(body)
}
