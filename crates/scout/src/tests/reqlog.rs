//! Go: `internal/scout/reqlog_test.go` (132 lines). Work package 6.
//!
//! `capture_log` and `LogCapture` are the Go `captureLog` helper over the crate's thread-local log sink.

use regex::Regex;

use crate::cache::MemoryCache;
use crate::deps::{build_deps, settings_from_env};
use crate::handler::new_handler;
use crate::tests::handler::{get, test_deps};

/// Go `captureLog`: runs `f` with the crate's log output captured, and returns it.
pub(crate) fn capture_log(f: impl FnOnce()) -> String {
    crate::loglimit::capture(f)
}

/// Go `captureLog(t)`: log output collected from now until the guard drops, readable meanwhile with `text()`.
pub(crate) use crate::loglimit::Capture as LogCapture;

/// Go `forgetLogged`: lets a condition speak again, whatever an earlier test's line did to its minute.
pub(crate) fn forget_logged(key: &str) {
    crate::loglimit::with_limited_log(|last| last.remove(key));
}

// Only the per-install segments go; the route and the stream id stay, so the line still says what was
// asked for. A path cannot smuggle a newline in and forge a line of its own.
#[test]
fn redact_path() {
    let config = crate::tests::ticket::valid_blob();
    let cases: Vec<(String, &str)> = vec![
        (String::new(), "/"),
        ("/".to_owned(), "/"),
        ("/configure/".to_owned(), "/configure/"),
        ("/health".to_owned(), "/health"),
        ("/metrics".to_owned(), "/metrics"),
        ("/manifest.json".to_owned(), "/manifest.json"),
        ("/config-key".to_owned(), "/config-key"),
        ("/validate".to_owned(), "/validate"),
        (format!("/{config}/manifest.json"), "/<config>/manifest.json"),
        (format!("/{config}/stream/series/tt1:2:3.json"), "/<config>/stream/series/tt1:2:3.json"),
        (format!("/{config}/play/secret-token"), "/<config>/play/<token>"),
        (format!("//{config}/play/secret-token"), "/<config>/play/<token>"),
        ("/p/secret-ticket".to_owned(), "/p/<ticket>"),
        ("//p//secret-ticket".to_owned(), "/p/<ticket>"),
        ("/nope".to_owned(), "/<config>"),
        // A slash is a segment boundary, as it is to the handler; the newline and spaces are escaped.
        (
            "/cfg/stream/movie/tt1\nGET /health 200 0ms.json".to_owned(),
            "/<config>/stream/movie/tt1%0AGET%20/health%20200%200ms.json",
        ),
    ];
    for (input, want) in cases {
        let got = crate::reqlog::redact_path(&input);
        assert_eq!(got, want, "redact_path({input:?})");
    }
}

// Unset, empty and exactly "0" leave it off; any other value turns it on.
#[test]
fn log_requests_setting() {
    for (v, want) in [("", false), ("0", false), ("1", true), ("true", true), ("yes", true)] {
        let s = settings_from_env(|k| if k == "LOG_REQUESTS" { v.to_owned() } else { String::new() });
        let d = build_deps(&s, reqwest::Client::new(), std::sync::Arc::new(MemoryCache::new(1 << 10)));
        assert!(
            s.log_requests == want && d.log_requests == want,
            "LOG_REQUESTS={v:?}: got {}, want {want}",
            s.log_requests
        );
    }
}

// Off by default and silent; on, one line per response with the config, the token and the whole query
// string gone.
#[tokio::test]
async fn request_log() {
    let out = LogCapture::start();
    get(&new_handler(test_deps(|_| {})), "/health", &[]).await;
    assert!(!out.text().contains("/health"), "a request was logged with LOG_REQUESTS off:\n{}", out.text());

    let h = new_handler(test_deps(|d| d.log_requests = true));
    get(&h, "/health", &[]).await;
    let blob = crate::tests::ticket::valid_blob();
    get(&h, &format!("/{blob}/play/tok-secret?probe=1&token=query-secret"), &[]).await;
    let got = out.text();
    for want in [r"(?m)^GET /health 200 \d+ms$", r"(?m)^GET /<config>/play/<token> 400 \d+ms$"] {
        assert!(Regex::new(want).unwrap().is_match(&got), "no line matching {want} in:\n{got}");
    }
    for leak in [blob.as_str(), "tok-secret", "query-secret", "probe"] {
        assert!(!got.contains(leak), "the request log carried {leak:?}:\n{got}");
    }
}

// The app's X-Request-Id ends the line, so its log and this one join exactly; a hostile value is cut down
// to what cannot forge a line, and no header means no rid at all.
//
// A header value cannot carry a newline in an `http::HeaderValue`, so the two hostile values lose their "\n"; the
// newline was only ever one more character the cut drops, and the spaces and angle brackets still exercise it.
#[tokio::test]
async fn request_log_carries_the_request_id() {
    let out = LogCapture::start();
    let h = new_handler(test_deps(|d| d.log_requests = true));
    get(&h, "/health", &[("X-Request-Id", "a1B2-c3_d4")]).await;
    let hostile = format!("x yGET /<config>/play 200 0ms{}", "z".repeat(100));
    get(&h, "/manifest.json", &[("X-Request-Id", hostile.as_str())]).await;
    get(&h, "/config-key", &[]).await;
    get(&h, "/configure", &[("X-Request-Id", " <>")]).await;
    let got = out.text();
    for want in [
        r"(?m)^GET /health 200 \d+ms rid=a1B2-c3_d4$",
        r"(?m)^GET /manifest\.json 200 \d+ms rid=xyGETconfigplay2000mszzzzzzzzzzz$",
        r"(?m)^GET /config-key \d+ \d+ms$",
        r"(?m)^GET /configure 200 \d+ms$",
    ] {
        assert!(Regex::new(want).unwrap().is_match(&got), "no line matching {want} in:\n{got}");
    }
    assert!(!got.contains("<config>/play"), "a request id forged a line:\n{got}");
}
