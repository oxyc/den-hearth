//! Go: `internal/scout/loglimit_test.go` (104 lines). Work package 6.
//!

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use crate::deps::{settings_from_env, startup_summary as startup_line};
use crate::handler::{new_handler, INDEXERS_DOWN_DETAIL, SCRAPE_FAIL_THRESHOLD};
use crate::loglimit::{with_limited_log, LOG_INTERVAL};
use crate::manifest::manifest_version;
use crate::scrape::DoError;

use super::handler::{fake_scraper, get, test_deps, test_seeds, valid_blob};
use super::reqlog::{capture_log, LogCapture};

/// Go `ageLogLimit`: pushes a condition's last-seen time back a full interval, so a test that checks for its exact
/// line is not silenced by unrelated coverage of the same condition moments earlier in this run.
pub(crate) fn age_log_limit(key: &str) {
    with_limited_log(|last| {
        if let Some(e) = last.get_mut(key) {
            e.at -= LOG_INTERVAL;
        }
    });
}

// /health's flip is logged as a change of state: one line going degraded however many builds keep
// failing, and one coming back however many succeed.
#[tokio::test]
async fn health_flip_is_logged_once_each_way() {
    let out = LogCapture::start();
    let healthy = Arc::new(AtomicBool::new(false));
    let flag = healthy.clone();
    let h = new_handler(test_deps(move |d| {
        let flag = flag.clone();
        d.make_scrapers = Arc::new(move |_| {
            let flag = flag.clone();
            vec![fake_scraper("torrentio", move || {
                let flag = flag.clone();
                Box::pin(async move {
                    if flag.load(Ordering::SeqCst) {
                        Ok(test_seeds())
                    } else {
                        Err(Box::new(std::io::Error::other("context canceled")) as DoError)
                    }
                })
            })]
        });
    }));
    let build = |i: i32| {
        let h = &h;
        async move {
            get(h, &format!("/{}/stream/movie/tt9{i}.json", valid_blob()), &[]).await;
        }
    };
    for i in 0..SCRAPE_FAIL_THRESHOLD + 3 {
        build(i).await; // distinct titles, so each one builds
    }
    let want = format!("health: ok → degraded (indexers): {INDEXERS_DOWN_DETAIL}");
    let n = out.text().matches(&want).count();
    assert_eq!(n, 1, "going degraded logged {n} times, want 1:\n{}", out.text());
    healthy.store(true, Ordering::SeqCst);
    build(80).await;
    build(81).await;
    let n = out.text().matches("health: degraded → ok").count();
    assert_eq!(n, 1, "recovering logged {n} times, want 1:\n{}", out.text());
}

// The startup line says what is running without saying anything secret.
#[test]
fn startup_summary() {
    let env = [
        ("MEDIAFUSION_URL", "https://mf.self/SECRET-CONFIG"),
        ("CONFIG_KEY", "AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8="),
        ("METRICS_TOKEN", "metrics-secret"),
        ("CACHE_DIR", "/cache"),
    ];
    let lookup =
        |k: &str| env.iter().find(|(name, _)| *name == k).map(|(_, v)| (*v).to_owned()).unwrap_or_default();
    let s = settings_from_env(lookup);
    let line = startup_line(&s, true);
    let first = format!("den-scout {} listening on :8080 — ", manifest_version());
    for want in [
        first.as_str(),
        "indexers=torrentio,mediafusion",
        "overrides=mediafusion",
        "disabled=torz",
        "minting=off",
        "cache=/cache cache_persistent=on",
        "sealed=on",
        "metrics=on",
        "log_requests=off",
    ] {
        assert!(line.contains(want), "startup line lacks {want:?}: {line}");
    }
    for leak in ["SECRET-CONFIG", "mf.self", "AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8=", "metrics-secret"]
    {
        assert!(!line.contains(leak), "startup line carries {leak:?}: {line}");
    }
    let unwritable = startup_line(&s, false);
    assert!(unwritable.contains("cache_persistent=off"), "an unwritable cache should say so: {unwritable}");
}

// A set-but-malformed CONFIG_KEY is `sealed=invalid` (not `on`), and unset is `off`.
#[test]
fn startup_summary_names_an_invalid_config_key() {
    let line = |key: &'static str| {
        startup_line(
            &settings_from_env(move |k| if k == "CONFIG_KEY" { key.to_owned() } else { String::new() }),
            true,
        )
    };
    assert!(line("not-a-key").contains("sealed=invalid"));
    assert!(line("").contains("sealed=off"));
    assert!(!line("not-a-key").contains("not-a-key"));
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
