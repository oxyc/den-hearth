//! Go: `internal/scout/validate_test.go` (188 lines). Work package 6.
//!
//! The per-service verdicts and the unreachable case run against a mocked `Doer`. The route tests drive
//! `handleValidate` through the handler, with the mock set as `Deps::probe_doer` (Go swaps the `*http.Client`'s
//! transport) and the pacing replaced through `Handler::set_validate_limiter` (Go swaps the package global).

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use super::handler::{get, post_json, test_deps};
use super::mock::{resp, MockDoer};
use crate::config::DebridService;
use crate::handler::{new_handler, Handler, NO_STORE};
use crate::hostlimit::new_host_limiter;
use crate::scrape::DoError;
use crate::stores::realdebrid::REAL_DEBRID_API;
use crate::stores::torbox::TORBOX_API;
use crate::validate::{validate_debrid_token, ValidateResult};

/// The limiter paces per upstream host and would otherwise make a test that checks several tokens sit through its
/// cooldown. The Go test swaps the package global for a generous one; here it is passed in.
fn unthrottled() -> crate::hostlimit::HostLimiter {
    new_host_limiter(Duration::from_millis(1), 10_000)
}

// Each service is asked the cheapest authenticated read it offers, and the verdict comes from the right
// place — which is not the status code for all of them.
#[tokio::test]
async fn validate_debrid_token_per_service() {
    let limiter = unthrottled();
    // (name, service, upstream status, upstream body, want valid, want URL, want bearer header)
    let torbox_me = format!("{TORBOX_API}/user/me");
    let rd_user = format!("{REAL_DEBRID_API}/user");
    let cases: [(&str, DebridService, u16, &str, bool, Option<&str>, bool); 7] = [
        ("torbox ok", DebridService::TorBox, 200, r#"{"data":{}}"#, true, Some(torbox_me.as_str()), true),
        ("torbox rejected", DebridService::TorBox, 401, "{}", false, None, true),
        ("torbox down", DebridService::TorBox, 500, "{}", false, None, true),
        ("realdebrid ok", DebridService::RealDebrid, 200, r#"{"id":1}"#, true, Some(rd_user.as_str()), true),
        ("realdebrid rejected", DebridService::RealDebrid, 403, "{}", false, None, true),
        // Premiumize answers 200 for a bad key and puts the verdict in the body, so the status alone is
        // not the answer for every service.
        ("premiumize ok", DebridService::Premiumize, 200, r#"{"status":"success"}"#, true, None, false),
        ("premiumize rejected", DebridService::Premiumize, 200, r#"{"status":"error"}"#, false, None, false),
    ];
    for (name, svc, status, body, want_valid, want_url, want_auth) in cases {
        let seen: Arc<Mutex<(String, String)>> = Arc::new(Mutex::new((String::new(), String::new())));
        let doer = MockDoer({
            let seen = Arc::clone(&seen);
            move |req: reqwest::Request| {
                let auth = req
                    .headers()
                    .get("authorization")
                    .and_then(|v| v.to_str().ok())
                    .unwrap_or_default()
                    .to_owned();
                *seen.lock().unwrap() = (req.url().to_string(), auth);
                Ok(resp(status, body))
            }
        });
        let got = validate_debrid_token(&doer, &limiter, svc, "the-token").await;
        assert_eq!(
            got.valid, want_valid,
            "{name}: valid = {} (reason {:?}), want {want_valid}",
            got.valid, got.reason
        );
        assert!(
            got.valid || !got.reason.is_empty(),
            "{name}: an invalid verdict came back with no reason to show anyone"
        );
        let (url, auth) = seen.lock().unwrap().clone();
        if let Some(want_url) = want_url {
            assert_eq!(url, want_url, "{name}: asked {url:?}, want {want_url:?}");
        }
        if want_auth {
            assert_eq!(auth, "Bearer the-token", "{name}: authorization");
        } else {
            // Premiumize authenticates in the query string, so it must NOT also send a bearer header.
            assert!(auth.is_empty(), "{name}: premiumize sent an authorization header: {auth:?}");
        }
    }
}

// A provider being down is not a wrong token. Telling someone their key is bad because a service was
// unreachable is worse than telling them nothing, because they will go and regenerate a working key.
#[tokio::test]
async fn validate_unreachable_is_not_invalid() {
    let limiter = unthrottled();
    let doer = MockDoer(|_req: reqwest::Request| Err(DoError::from("dial tcp: connection refused")));
    let got = validate_debrid_token(&doer, &limiter, DebridService::TorBox, "t").await;
    assert!(!got.valid, "an unreachable service reported the token as valid");
    assert!(
        got.reason.contains("could not reach"),
        "reason = {:?}, want it to say the service could not be reached",
        got.reason
    );
    // The reason is shown to a user and must never carry the token or the upstream URL — they travel
    // together, and this is the one route holding an unsealed token. The Go check compares against torboxAPI
    // (package 1); this asserts the reason carries no URL and no transport text at all.
    assert!(
        !got.reason.contains("://") && !got.reason.contains("dial tcp") && !got.reason.contains("refused"),
        "reason leaked the request: {:?}",
        got.reason
    );
}

/// Go's `roundTripperFunc`-driven `Deps.ProbeClient`: a handler whose link check and token validation go through a
/// `Doer` that counts the requests it is asked and answers `{"data":{}}`.
fn counting_handler(asked: Arc<AtomicUsize>, limiter: crate::hostlimit::HostLimiter) -> Handler {
    let h = new_handler(test_deps(move |d| {
        d.probe_doer = Some(Arc::new(MockDoer(move |_req: reqwest::Request| {
            asked.fetch_add(1, Ordering::SeqCst);
            Ok(resp(200, r#"{"data":{}}"#))
        })));
    }));
    h.set_validate_limiter(limiter);
    h
}

// Refused, not queued. The route is an unauthenticated credential-testing oracle pointed at three third
// parties, and the first version made it WAIT for its token — so a flood did not slow down, it parked a
// goroutine and a connection per request for the whole budget, inside a 230 MiB heap. 429 costs nothing.
#[tokio::test]
async fn validate_route_refuses_rather_than_queues() {
    // One allowed, then the allowance is gone for an hour.
    let limiter = new_host_limiter(Duration::from_secs(3600), 1);
    let upstream = Arc::new(AtomicUsize::new(0));
    let h = counting_handler(Arc::clone(&upstream), limiter);
    let body = r#"{"service":"torbox","token":"tb-secret"}"#;

    let first = post_json(&h, "/validate", body).await;
    assert_eq!(first.status(), 200, "first check: {}, want 200", first.status());
    let start = std::time::Instant::now();
    let rr = post_json(&h, "/validate", body).await;
    assert_eq!(rr.status(), 429, "second check: {}, want 429", rr.status());
    // The point of the 429 is that it is immediate — a queued request is the cost being avoided.
    let elapsed = start.elapsed();
    assert!(
        elapsed <= Duration::from_secs(1),
        "the refusal took {elapsed:?} — it queued instead of shedding"
    );
    assert!(rr.headers().get("retry-after").is_some_and(|v| !v.is_empty()), "429 without a Retry-After");
    let n = upstream.load(Ordering::SeqCst);
    assert_eq!(n, 1, "upstream called {n} times, want 1 — a refused check must not reach the provider");
}

// The route: POST only, strict about what it accepts, and never cached.
#[tokio::test]
async fn validate_route() {
    let asked = Arc::new(AtomicUsize::new(0));
    let h = counting_handler(Arc::clone(&asked), unthrottled());

    let rr = post_json(&h, "/validate", r#"{"service":"torbox","token":"tb-secret"}"#).await;
    assert_eq!(rr.status(), 200, "validate: {}", rr.status());
    let cc = rr.headers().get("cache-control").and_then(|v| v.to_str().ok()).unwrap_or_default();
    assert_eq!(cc, NO_STORE, "cache-control = {cc:?}, want no-store — this reply is about a secret");
    let got: ValidateResult = serde_json::from_slice(rr.body()).expect("json");
    assert!(got.valid, "valid token reported invalid: {got:?}");

    // A GET must not reach the upstream — this route exists to spend one read, on purpose, per press.
    let before = asked.load(Ordering::SeqCst);
    let rr = get(&h, "/validate", &[]).await;
    assert_eq!(rr.status(), 405, "GET /validate: {}, want 405", rr.status());
    assert_eq!(asked.load(Ordering::SeqCst), before, "a GET reached the debrid service");

    // Unknown service, empty token, oversized token and malformed JSON are all refused before any
    // upstream call — the route must not become a way to send arbitrary strings to a third party.
    let before = asked.load(Ordering::SeqCst);
    let oversized = format!(r#"{{"service":"torbox","token":"{}"}}"#, "x".repeat(513));
    for body in [
        r#"{"service":"nope","token":"x"}"#,
        r#"{"service":"torbox","token":""}"#,
        oversized.as_str(),
        "not json",
    ] {
        let rr = post_json(&h, "/validate", body).await;
        assert_eq!(rr.status(), 400, "body {body:.40}: {}, want 400", rr.status());
    }
    let n = asked.load(Ordering::SeqCst) - before;
    assert_eq!(n, 0, "a rejected request still reached the upstream ({n} calls)");
}
