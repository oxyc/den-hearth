//! Go: `internal/scout/reqlog_test.go` (132 lines). Work package 6.
//!
//! `redact_path` is live. The request-log tests drive the handler and the deps (packages 4 and 6), so they are
//! ignored with their bodies compiled out. `capture_log` is the Go `captureLog` helper; the crate has no log sink
//! to redirect yet, so it is `todo!()` until the logging owner lands one.

/// Go `captureLog`: runs `f` with the crate's log output captured, and returns it.
pub(crate) fn capture_log(f: impl FnOnce()) -> String {
    crate::loglimit::capture(f)
}

/// Stands in for Go's `(?m)^<prefix>\d+<suffix>$` regexes: a line that starts with `prefix` and ends with `suffix`.
fn has_line(log: &str, prefix: &str, suffix: &str) -> bool {
    log.lines().any(|l| l.starts_with(prefix) && l.ends_with(suffix))
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
#[ignore = "needs settings_from_env / build_deps from deps.rs (package 4)"]
fn log_requests_setting() {
    // Enable when package 4 lands: move this body up and delete the wrapper.
    #[cfg(any())]
    fn pending() {
        use crate::cache::MemoryCache;
        use crate::deps::{build_deps, settings_from_env};
        for (v, want) in [("", false), ("0", false), ("1", true), ("true", true), ("yes", true)] {
            let s = settings_from_env(|k| if k == "LOG_REQUESTS" { v.to_owned() } else { String::new() });
            let d = build_deps(&s, None, MemoryCache::new(1 << 10));
            assert!(
                s.log_requests == want && d.log_requests == want,
                "LOG_REQUESTS={v:?}: got {}, want {want}",
                s.log_requests
            );
        }
    }
    todo!("pending: reqlog_test.go:~59 TestLogRequestsSetting")
}

// Off by default and silent; on, one line per response with the config, the token and the whole query
// string gone.
#[test]
#[ignore = "needs handler::serve / Deps::for_test and capture_log (packages 4, 6)"]
fn request_log() {
    // Enable when packages 4 and 6 land: move this body up and delete the wrapper.
    #[cfg(any())]
    fn pending() {
        use crate::deps::Deps;
        let out = capture_log(|| {
            get(&Deps::for_test(|_| {}), "/health", &[]);
        });
        assert!(!out.contains("/health"), "a request was logged with LOG_REQUESTS off:\n{out}");

        let deps = Deps::for_test(|d| d.log_requests = true);
        let out = capture_log(|| {
            get(&deps, "/health", &[]);
            get(
                &deps,
                &format!(
                    "/{}/play/tok-secret?probe=1&token=query-secret",
                    crate::tests::ticket::valid_blob()
                ),
                &[],
            );
        });
        // One line per response, in the shape `<METHOD> <redacted path> <status> <ms>ms`.
        assert!(has_line(&out, "GET /health 200 ", "ms"), "no line for /health in:\n{out}");
        assert!(
            has_line(&out, "GET /<config>/play/<token> 400 ", "ms"),
            "no line for the play route in:\n{out}"
        );
        for leak in [crate::tests::ticket::valid_blob().as_str(), "tok-secret", "query-secret", "probe"] {
            assert!(!out.contains(leak), "the request log carried {leak:?}:\n{out}");
        }
    }
    todo!("pending: reqlog_test.go:~82 TestRequestLog")
}

// The app's X-Request-Id ends the line, so its log and this one join exactly; a hostile value is cut down
// to what cannot forge a line, and no header means no rid at all.
#[test]
#[ignore = "needs handler::serve / Deps::for_test and capture_log (packages 4, 6)"]
fn request_log_carries_the_request_id() {
    // Enable when packages 4 and 6 land: move this body up and delete the wrapper.
    #[cfg(any())]
    fn pending() {
        use crate::deps::Deps;
        let deps = Deps::for_test(|d| d.log_requests = true);
        let out = capture_log(|| {
            get(&deps, "/health", &[("X-Request-Id", "a1B2-c3_d4")]);
            get(&deps, "/manifest.json", &[("X-Request-Id", "x y\nGET /<config>/play 200 0ms zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz")]);
            get(&deps, "/config-key", &[]);
            get(&deps, "/configure", &[("X-Request-Id", " \n<>")]);
        });
        // The id is cut to [A-Za-z0-9_-] and then to 32 characters.
        assert!(
            has_line(&out, "GET /health 200 ", "ms rid=a1B2-c3_d4"),
            "rid line for /health missing:\n{out}"
        );
        assert!(
            out.contains("rid=xyGETconfigplay2000mszzzzzzzzzzz"),
            "rid line for /manifest.json missing:\n{out}"
        );
        assert!(has_line(&out, "GET /config-key ", "ms"), "config-key line missing:\n{out}");
        assert!(!out.contains("<config>/play"), "a request id forged a line:\n{out}");
    }
    todo!("pending: reqlog_test.go:~111 TestRequestLogCarriesTheRequestID")
}
