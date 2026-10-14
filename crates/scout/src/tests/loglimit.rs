//! Go: `internal/scout/loglimit_test.go` (104 lines). Work package 6.
//!
//! `log_limited` is live, but its output comes through `capture_log`, which is not ported yet, so it fails at that
//! `todo!()` for now. The health-flip and startup-summary tests drive the handler and deps (packages 4 and 6), so
//! they are ignored with their bodies compiled out.

use crate::loglimit::{LIMITED_LOG, LOG_INTERVAL};

use super::reqlog::capture_log;

/// Go `ageLogLimit`: pushes a condition's last-seen time back a full interval, so a test that checks for its exact
/// line is not silenced by unrelated coverage of the same condition moments earlier in this run.
pub(crate) fn age_log_limit(key: &str) {
    let mut last = LIMITED_LOG.lock().unwrap();
    if let Some(e) = last.get_mut(key) {
        e.at -= LOG_INTERVAL;
    }
}

// /health's flip is logged as a change of state: one line going degraded however many builds keep
// failing, and one coming back however many succeed.
#[test]
#[ignore = "needs handler::serve / Deps::for_test and capture_log (packages 4, 6)"]
fn health_flip_is_logged_once_each_way() {
    // Enable when packages 4 and 6 land: move this body up and delete the wrapper.
    #[cfg(any())]
    fn pending() {
        use crate::deps::Deps;
        use crate::scrape::SCRAPE_FAIL_THRESHOLD;
        use std::sync::atomic::{AtomicBool, Ordering};
        let healthy = Arc::new(AtomicBool::new(false));
        let out = capture_log(|| {
            let h = handler_with_scrapers(Arc::clone(&healthy));
            for i in 0..SCRAPE_FAIL_THRESHOLD + 3 {
                // distinct titles, so each one builds
                get(&h, &format!("/{}/stream/movie/tt9{i}.json", crate::tests::ticket::valid_blob()));
            }
            let n = out_count("health: ok → degraded (indexers): ");
            assert_eq!(n, 1, "going degraded logged {n} times, want 1");
            healthy.store(true, Ordering::SeqCst);
            get(&h, &format!("/{}/stream/movie/tt980.json", crate::tests::ticket::valid_blob()));
            get(&h, &format!("/{}/stream/movie/tt981.json", crate::tests::ticket::valid_blob()));
        });
        let n = out.matches("health: degraded → ok").count();
        assert_eq!(n, 1, "recovering logged {n} times, want 1:\n{out}");
    }
    todo!("pending: loglimit_test.go:~22 TestHealthFlipIsLoggedOnceEachWay")
}

// The startup line says what is running without saying anything secret.
#[test]
#[ignore = "needs settings_from_env / startup_summary from deps.rs (package 4)"]
fn startup_summary() {
    // Enable when package 4 lands: move this body up and delete the wrapper.
    #[cfg(any())]
    fn pending() {
        use crate::deps::{settings_from_env, startup_summary};
        use crate::manifest::MANIFEST_VERSION;
        let env = [
            ("MEDIAFUSION_URL", "https://mf.self/SECRET-CONFIG"),
            ("CONFIG_KEY", "key-secret"),
            ("METRICS_TOKEN", "metrics-secret"),
            ("CACHE_DIR", "/cache"),
        ];
        let lookup =
            |k: &str| env.iter().find(|(name, _)| *name == k).map(|(_, v)| v.to_string()).unwrap_or_default();
        let s = settings_from_env(lookup);
        let line = startup_summary(&s, true);
        let wanted = [
            format!("den-scout {MANIFEST_VERSION} listening on :8080 — "),
            "indexers=torrentio,mediafusion".to_owned(),
            "overrides=mediafusion".to_owned(),
            "disabled=torz".to_owned(),
            "minting=off".to_owned(),
            "cache=/cache cache_persistent=on".to_owned(),
            "sealed=on".to_owned(),
            "metrics=on".to_owned(),
            "log_requests=off".to_owned(),
        ];
        for want in wanted {
            assert!(line.contains(&want), "startup line lacks {want:?}: {line}");
        }
        for leak in ["SECRET-CONFIG", "mf.self", "key-secret", "metrics-secret"] {
            assert!(!line.contains(leak), "startup line carries {leak:?}: {line}");
        }
        let unwritable = startup_summary(&s, false);
        assert!(
            unwritable.contains("cache_persistent=off"),
            "an unwritable cache should say so: {unwritable}"
        );
    }
    todo!("pending: loglimit_test.go:~59 TestStartupSummary")
}

// One condition, many occurrences: one line a minute, and the next line admits what it held back.
#[test]
fn log_limited() {
    let out = capture_log(|| {
        for i in 0..3 {
            crate::loglimit::log_limited("test-condition", &format!("upstream down ({i})"));
        }
        // A different condition is not held back by this one.
        crate::loglimit::log_limited("test-other-condition", "another thing");
        // Once the minute has passed the condition speaks again, and says how many it swallowed.
        age_log_limit("test-condition");
        crate::loglimit::log_limited("test-condition", "upstream down (3)");
    });
    // Three occurrences inside a minute write one line: the first. The output is checked once the whole run is
    // captured, because the capture helper returns one string.
    assert!(out.contains("upstream down (0)"), "the first occurrence was not written:\n{out}");
    assert!(
        !out.contains("upstream down (1)") && !out.contains("upstream down (2)"),
        "three occurrences inside a minute wrote more than one line:\n{out}"
    );
    assert!(out.contains("another thing"), "an unrelated condition was suppressed:\n{out}");
    assert!(
        out.contains("upstream down (3) (2 more like this since the last line)"),
        "the next line should report the two held back:\n{out}"
    );
}
