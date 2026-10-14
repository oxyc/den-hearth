//! Port of `internal/scout/validate.go`. Work package 6.
//!
//! Checks a debrid token at the moment it is pasted: one unauthenticated read per service, judged from the right
//! place for that service. A provider being down is never reported as a wrong token.

use std::sync::{Arc, Mutex, PoisonError};
use std::time::Instant;

use reqwest::header::{HeaderValue, ACCEPT, AUTHORIZATION};
use reqwest::StatusCode;
use serde::Deserialize;

use crate::config::DebridService;
use crate::hostlimit::{Bucket, HostLimiter};
use crate::scrape::Doer;
use crate::stores::premiumize::PREMIUMIZE_API;
use crate::stores::realdebrid::REAL_DEBRID_API;
use crate::stores::torbox::TORBOX_API;
use crate::util::{fold_json_keys, Shape, LEAF};

/// Go `validateResult`. `throttled` is set when our own pacing refused the check; the route answers 429 for it.
#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize, Deserialize)]
pub struct ValidateResult {
    pub valid: bool,
    /// Why not, in words a configure page can show. Never the upstream body.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub reason: String,
    /// Not serialised: it only decides the status code, and the client's distinction is already in `reason`.
    #[serde(skip)]
    pub throttled: bool,
}

/// Go Premiumize's account reply: only the verdict is read.
#[derive(Deserialize)]
struct PremiumizeReply {
    /// A missing or `null` status reads as "", which is not "success".
    status: Option<String>,
}

/// Go `validateBudget`: how long the whole check may take.
pub const VALIDATE_BUDGET: std::time::Duration = std::time::Duration::from_secs(10);

/// Go `maxValidateBody`: bounds the request body.
pub const MAX_VALIDATE_BODY: usize = 4 << 10;

/// Go `validateDebridToken`: asks the service whether this token identifies an account. A transport failure is
/// reported as "could not reach", never as invalid — telling someone their key is wrong because a provider was
/// down is worse than telling them nothing. The Go function reads the package-level `validateLimiter`; the port
/// takes it as `limiter` so a test can pass its own without a global swap. The caller bounds the whole check with
/// `VALIDATE_BUDGET` (`tokio::time::timeout`), since a dropped future is the cancellation.
pub async fn validate_debrid_token(doer: &dyn Doer, limiter: &HostLimiter, svc: DebridService, token: &str) -> ValidateResult {
    let Some(req) = validate_request(svc, token) else {
        return ValidateResult { reason: "validation unavailable".to_owned(), ..Default::default() };
    };
    // Go keys the bucket by `url.Host`, which carries the port when the URL names one.
    let host = match (req.url().host_str(), req.url().port()) {
        (Some(h), Some(p)) => format!("{h}:{p}"),
        (Some(h), None) => h.to_owned(),
        (None, _) => String::new(),
    };
    if !allow(limiter, &host) {
        return ValidateResult {
            reason: "too many checks just now — try again in a moment".to_owned(),
            throttled: true,
            ..Default::default()
        };
    }
    let resp = doer.do_request(req).await;
    // TODO(metrics): Go records `metrics.torboxCall("user", <2xx>)` here for TorBox, success or failure; metrics.rs
    // is not ported yet.
    let resp = match resp {
        Ok(resp) => resp,
        // The indexer scrapers log the service name and never the URL, because MediaFusion's carries an
        // encrypted config. Same rule here for a different reason: the URL would be fine, the token would not,
        // and the two travel together.
        Err(_) => return ValidateResult { reason: format!("could not reach {}", svc.as_str()), ..Default::default() },
    };

    let status = resp.status();
    if status == StatusCode::UNAUTHORIZED || status == StatusCode::FORBIDDEN {
        return rejected();
    }
    if status != StatusCode::OK {
        return ValidateResult { reason: format!("{} answered {}", svc.as_str(), status.as_u16()), ..Default::default() };
    }
    // Premiumize answers 200 for a bad key and puts the verdict in the body, so the status alone is not the
    // answer for every service.
    if svc == DebridService::Premiumize {
        let body = read_prefix(resp).await;
        // Go decodes the first JSON value and matches `status` case-insensitively, as for every struct field.
        static SHAPE: Shape = Shape::Object(&[("status", &LEAF)]);
        let first = serde_json::Deserializer::from_slice(&body).into_iter::<serde_json::Value>().next();
        let Some(Ok(mut value)) = first else {
            return ValidateResult { reason: "could not read the reply".to_owned(), ..Default::default() };
        };
        fold_json_keys(&mut value, &SHAPE);
        return match serde_json::from_value::<PremiumizeReply>(value) {
            Ok(pm) if pm.status.as_deref().unwrap_or_default().eq_ignore_ascii_case("success") => {
                ValidateResult { valid: true, ..Default::default() }
            }
            Ok(_) => rejected(),
            Err(_) => ValidateResult { reason: "could not read the reply".to_owned(), ..Default::default() },
        };
    }
    ValidateResult { valid: true, ..Default::default() }
}

fn rejected() -> ValidateResult {
    ValidateResult { reason: "the service rejected this token".to_owned(), ..Default::default() }
}

/// Go `validateRequest`: builds the per-service account read. Each is the cheapest authenticated GET the service
/// offers — enough to prove the token identifies an account, and nothing more.
fn validate_request(svc: DebridService, token: &str) -> Option<reqwest::Request> {
    let url = match svc {
        DebridService::TorBox => reqwest::Url::parse(&format!("{TORBOX_API}/user/me")).ok()?,
        DebridService::RealDebrid => reqwest::Url::parse(&format!("{REAL_DEBRID_API}/user")).ok()?,
        // Premiumize authenticates in the query string, so it sends no bearer header.
        DebridService::Premiumize => {
            reqwest::Url::parse_with_params(&format!("{PREMIUMIZE_API}/account/info"), &[("apikey", token)]).ok()?
        }
    };
    let mut req = reqwest::Request::new(reqwest::Method::GET, url);
    req.headers_mut().insert(ACCEPT, HeaderValue::from_static("application/json"));
    if svc != DebridService::Premiumize {
        req.headers_mut().insert(AUTHORIZATION, HeaderValue::from_str(&format!("Bearer {token}")).ok()?);
    }
    Some(req)
}

/// Reads at most `MAX_VALIDATE_BODY` bytes of the reply, the way Go's `MaxBytesReader` bounds the decode. The
/// verdict is the first JSON value in that prefix, so a longer tail is not read.
async fn read_prefix(mut resp: reqwest::Response) -> Vec<u8> {
    let mut buf = Vec::new();
    while buf.len() < MAX_VALIDATE_BODY {
        match resp.chunk().await {
            Ok(Some(chunk)) => buf.extend_from_slice(&chunk),
            _ => break,
        }
    }
    buf.truncate(MAX_VALIDATE_BODY);
    buf
}

/// Go `(*hostLimiter).allow`: takes a whole token if one is free right now and never queues. An empty key is never
/// limited.
///
/// TODO(hostlimit): this duplicates `hostLimiter.allow` and `bucket.tryTake`, which belong in `hostlimit.rs` as
/// `HostLimiter::allow` / `Bucket::try_take`. They are still `todo!()` there, so the logic is kept here, on the
/// limiter's crate-visible fields, until that file lands; then this function is deleted and the call becomes
/// `limiter.allow(host)`.
fn allow(limiter: &HostLimiter, key: &str) -> bool {
    if key.is_empty() {
        return true;
    }
    let bucket = {
        // A poisoned lock still holds consistent state: every edit under it finishes before anything can panic.
        let mut buckets = limiter.buckets.lock().unwrap_or_else(PoisonError::into_inner);
        let burst = limiter.burst as f64;
        Arc::clone(buckets.entry(key.to_owned()).or_insert_with(|| {
            Arc::new(Mutex::new(Bucket { tokens: burst, last: Instant::now() }))
        }))
    };
    let mut b = bucket.lock().unwrap_or_else(PoisonError::into_inner);
    let now = Instant::now();
    b.tokens += now.duration_since(b.last).as_secs_f64() / limiter.every.as_secs_f64();
    if b.tokens > limiter.burst as f64 {
        b.tokens = limiter.burst as f64;
    }
    b.last = now;
    if b.tokens < 1.0 {
        return false;
    }
    b.tokens -= 1.0;
    true
}
