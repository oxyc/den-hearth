//! Go: `internal/scout/validate_test.go` (188 lines). Work package 6.
//!
//! The per-service verdicts and the unreachable case are live against a mocked `Doer`. The two route tests drive
//! `handleValidate` through the handler (package 4), so they are ignored with their bodies compiled out.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use super::mock::{resp, MockDoer};
use crate::config::DebridService;
use crate::hostlimit::new_host_limiter;
use crate::scrape::DoError;
use crate::validate::validate_debrid_token;

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
    // (name, service, upstream status, upstream body, want valid, want URL suffix, want bearer header)
    //
    // The Go test compares the full URL against `torboxAPI`/`realDebridAPI` (package 1). Until those constants
    // land, the path suffix is what is asserted.
    let cases: [(&str, DebridService, u16, &str, bool, Option<&str>, bool); 7] = [
        ("torbox ok", DebridService::TorBox, 200, r#"{"data":{}}"#, true, Some("/user/me"), true),
        ("torbox rejected", DebridService::TorBox, 401, "{}", false, None, true),
        ("torbox down", DebridService::TorBox, 500, "{}", false, None, true),
        ("realdebrid ok", DebridService::RealDebrid, 200, r#"{"id":1}"#, true, Some("/user"), true),
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
        if let Some(suffix) = want_url {
            assert!(url.ends_with(suffix), "{name}: asked {url:?}, want a URL ending in {suffix:?}");
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

// Needs `handleValidate`, `Deps::for_test` and `postJSON` through the handler (package 4).
#[test]
#[ignore = "needs handler::serve / Deps::for_test / the /validate route (package 4)"]
fn validate_route_refuses_rather_than_queues() {
    // Enable when package 4 lands: move this body up and delete the wrapper.
    #[cfg(any())]
    fn pending() {
        use crate::deps::Deps;
        use crate::handler::serve;
        use crate::hostlimit::new_host_limiter;
        use std::sync::atomic::{AtomicUsize, Ordering};
        // One allowed, then the allowance is gone for an hour.
        let limiter = new_host_limiter(Duration::from_secs(3600), 1);
        let upstream = Arc::new(AtomicUsize::new(0));
        let deps = Deps::for_test(|d| {
            d.validate_limiter = limiter;
            d.probe_client = Some(counting_client(Arc::clone(&upstream), resp(200, r#"{"data":{}}"#)));
        });
        let body = r#"{"service":"torbox","token":"tb-secret"}"#;

        assert_eq!(post_json(&deps, "/validate", body).status(), 200, "first check");
        let start = std::time::Instant::now();
        let rr = post_json(&deps, "/validate", body);
        assert_eq!(rr.status(), 429, "second check");
        // The point of the 429 is that it is immediate — a queued request is the cost being avoided.
        let elapsed = start.elapsed();
        assert!(
            elapsed <= Duration::from_secs(1),
            "the refusal took {elapsed:?} — it queued instead of shedding"
        );
        assert!(rr.headers().get("retry-after").is_some(), "429 without a Retry-After");
        assert_eq!(
            upstream.load(Ordering::SeqCst),
            1,
            "upstream called more than once — a refused check must not reach the provider"
        );
    }
    todo!("pending: validate_test.go:~91 TestValidateRoute_refusesRatherThanQueues")
}

// The route: POST only, strict about what it accepts, and never cached.
#[test]
#[ignore = "needs handler::serve / Deps::for_test / the /validate route (package 4)"]
fn validate_route() {
    // Enable when package 4 lands: move this body up and delete the wrapper.
    #[cfg(any())]
    fn pending() {
        use crate::deps::Deps;
        use crate::handler::serve;
        use std::sync::atomic::{AtomicUsize, Ordering};
        let asked = Arc::new(AtomicUsize::new(0));
        let deps = Deps::for_test(|d| {
            d.validate_limiter = unthrottled();
            d.probe_client = Some(counting_client(Arc::clone(&asked), resp(200, r#"{"data":{}}"#)));
        });

        let rr = post_json(&deps, "/validate", r#"{"service":"torbox","token":"tb-secret"}"#);
        assert_eq!(rr.status(), 200, "validate");
        assert_eq!(
            rr.headers()["cache-control"],
            NO_STORE,
            "cache-control: want no-store, this reply is about a secret"
        );
        let got: ValidateResult = serde_json::from_str(rr.body()).expect("json");
        assert!(got.valid, "valid token reported invalid: {got:?}");

        // A GET must not reach the upstream — this route exists to spend one read, on purpose, per press.
        let before = asked.load(Ordering::SeqCst);
        assert_eq!(get(&deps, "/validate", &[]).status(), 405, "GET /validate");
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
            assert_eq!(post_json(&deps, "/validate", body).status(), 400, "body {body:.40}");
        }
        assert_eq!(asked.load(Ordering::SeqCst), before, "a rejected request still reached the upstream");
    }
    todo!("pending: validate_test.go:~133 TestValidateRoute")
}
