//! Go: `internal/scout/metrics_test.go` (218 lines). Work package 6.
//!
//! The fixed-cardinality and persistence-gauge tests are live. The `/metrics` route tests drive the handler
//! (package 4), so they are ignored and their bodies are ported and compiled out until it lands.

use tempfile::TempDir;

use crate::cache::MemoryCache;
use crate::diskcache::{write_file, TieredCache};
use crate::metrics::{cache_persistent_gauge, new_metric_set, METRIC_TORBOX_AVOIDED, METRIC_TORBOX_ENDPOINTS, METRIC_TORBOX_OUTCOMES};

const METRICS_TOKEN: &str = "metrics-shared-secret";

/// Go `sampleValue`: one series' value out of the exposition text, so a test can assert on the NUMBER rather
/// than on the series existing. `render()` emits every series unconditionally, so presence proves only that
/// render ran.
fn sample_value(body: &str, series: &str) -> i64 {
    let prefix = format!("{series} ");
    let line = body
        .lines()
        .find(|l| l.starts_with(&prefix))
        .unwrap_or_else(|| panic!("no series {series:?} in:\n{body}"));
    line[prefix.len()..]
        .trim()
        .parse()
        .unwrap_or_else(|_| panic!("unparseable sample {line:?}"))
}

// The counters are process-wide and every other test in this package feeds them too, so the route tests assert
// deltas. The route is gated and OFF entirely when no token is configured: 404, not an empty 200.
#[test]
#[ignore = "needs handler::serve / Deps::for_test (package 4)"]
fn metrics_is_gated() {
    // Enable when package 4 lands: move this body up and delete the wrapper.
    #[cfg(any())]
    fn pending() {
        use crate::deps::Deps;
        use crate::handler::serve;
        // The counters say when this install is being watched and which indexers it uses, so the route is gated
        // and OFF entirely when no token is configured — 404, not an empty 200, so an unconfigured deployment
        // does not advertise that there is something here.
        let off = Deps::for_test(|_| {});
        assert_eq!(get(&off, "/metrics", &[]).status(), 404, "no token configured");

        let on = Deps::for_test(|d| d.metrics_token = Some(METRICS_TOKEN.to_owned()));
        let cases: [(&str, Vec<(&str, &str)>, u16); 5] = [
            ("no header", vec![], 404),
            ("wrong token", vec![("authorization", "Bearer nope")], 404),
            ("no bearer prefix", vec![("authorization", METRICS_TOKEN)], 404),
            ("padded token", vec![("authorization", &format!("Bearer  {METRICS_TOKEN} "))], 200),
            ("correct", vec![("authorization", &format!("Bearer {METRICS_TOKEN}"))], 200),
        ];
        for (name, headers, want) in cases {
            let rr = get(&on, "/metrics", &headers);
            assert_eq!(rr.status(), want, "{name}");
        }
    }
    todo!("pending: metrics_test.go:~24 TestMetrics_isGated")
}

// The route answers in the exposition format, and the numbers actually move — asserted as DELTAS, because
// the counters are process-wide and every other test in this package feeds them too.
#[test]
#[ignore = "needs handler::serve / Deps::for_test (package 4)"]
fn metrics_reports_what_was_previously_log_only() {
    // Enable when package 4 lands: move this body up and delete the wrapper.
    #[cfg(any())]
    fn pending() {
        use crate::deps::Deps;
        use crate::handler::{manifest_version, NO_STORE};
        let deps = Deps::for_test(|d| d.metrics_token = Some(METRICS_TOKEN.to_owned()));
        let auth = [("authorization", &format!("Bearer {METRICS_TOKEN}") as &str)];
        let before = get(&deps, "/metrics", &auth).body().clone();

        // One cold build (a miss) then a warm hit, so each counter has a known increment.
        let path = format!("/{}/stream/movie/tt1234567.json", crate::tests::ticket::valid_blob());
        get(&deps, &path, &[]);
        get(&deps, &path, &[]);

        let rr = get(&deps, "/metrics", &auth);
        assert_eq!(rr.status(), 200, "/metrics");
        let after = rr.body().clone();
        let deltas: [(&str, i64); 5] = [
            (r#"scout_list_cache_total{result="miss"}"#, 1),
            (r#"scout_list_cache_total{result="hit"}"#, 1),
            (r#"scout_list_builds_total{result="ok"}"#, 1),
            (r#"scout_indexer_requests_total{indexer="torrentio"}"#, 1),
            (r#"scout_source_coverage_total{indexer="torrentio",outcome="answered"}"#, 1),
        ];
        for (series, want) in deltas {
            let got = sample_value(&after, series) - sample_value(&before, series);
            assert_eq!(got, want, "{series} moved by {got}, want {want}");
        }
        // A counter for something that did NOT happen must stay put.
        let degraded = r#"scout_list_builds_total{result="degraded"}"#;
        let d = sample_value(&after, degraded) - sample_value(&before, degraded);
        assert_eq!(d, 0, "a healthy build counted as degraded (delta {d})");
        let ct = rr.headers().get("content-type").and_then(|v| v.to_str().ok()).unwrap_or_default();
        assert!(ct.starts_with("text/plain"), "content-type = {ct:?}");
        let cc = rr.headers().get("cache-control").and_then(|v| v.to_str().ok()).unwrap_or_default();
        assert_eq!(cc, NO_STORE, "cache-control = {cc:?}, want no-store");
        let body = rr.body();
        for want in [
            r#"scout_list_cache_total{result="hit"}"#,
            r#"scout_list_cache_total{result="miss"}"#,
            r#"scout_list_builds_total{result="ok"}"#,
            r#"scout_indexer_requests_total{indexer="torrentio"}"#,
            r#"scout_indexer_failures_total{indexer="torrentio"}"#,
            r#"scout_probe_cache_total{result="hit"}"#,
            "scout_add_budget_remaining",
            "scout_cache_persistent",
            "# TYPE scout_list_cache_total counter",
            "# TYPE scout_build_info gauge",
        ] {
            assert!(body.contains(want), "missing series {want:?}");
        }
        let build_info = format!(r#"scout_build_info{{version="{}"}} 1"#, manifest_version());
        assert!(body.contains(&build_info), "missing series {build_info:?}");
        // Every indexer is present from the start — a series that only appears after the first failure is one
        // nobody can alert on.
        for id in ALL_INDEXERS {
            let series = format!(r#"scout_indexer_requests_total{{indexer="{id}"}}"#);
            assert!(body.contains(&series), "indexer {id} has no series before it is used");
        }
    }
    todo!("pending: metrics_test.go:~54 TestMetrics_reportsWhatWasPreviouslyLogOnly")
}

// The stale path has its own counter, and it is the one the stale-while-revalidate work exists to make
// visible — how often viewers are being served a list while it is rebuilt behind them.
#[test]
#[ignore = "needs handler::serve / Deps::for_test / recordingCache (package 4)"]
fn metrics_counts_stale_hits() {
    // Enable when package 4 lands: move this body up and delete the wrapper.
    #[cfg(any())]
    fn pending() {
        use crate::deps::Deps;
        use crate::handler::{join_cached, split_cached};
        use crate::tests::handler::RecordingCache;
        use std::time::{Duration, SystemTime, UNIX_EPOCH};
        let cache = Arc::new(RecordingCache::new(MemoryCache::new(1 << 20)));
        let deps = Deps::for_test(|d| {
            d.cache = cache.clone();
            d.metrics_token = Some(METRICS_TOKEN.to_owned());
        });
        let auth = [("authorization", &format!("Bearer {METRICS_TOKEN}") as &str)];
        let path = format!("/{}/stream/movie/tt1234567.json", crate::tests::ticket::valid_blob());
        get(&deps, &path, &[]);

        let before = sample_value(get(&deps, "/metrics", &auth).body(), r#"scout_list_cache_total{result="stale"}"#);

        let key = cache.last_key();
        let held = cache.get(&key).expect("nothing cached");
        let (complete, _, etag, body) = split_cached(&held);
        let backdated = (SystemTime::now() - Duration::from_secs(1)).duration_since(UNIX_EPOCH).expect("epoch").as_secs() as i64;
        cache.put(&key, &join_cached(&complete, backdated, &etag, &body), Duration::from_secs(60));
        get(&deps, &path, &[]);

        let after = sample_value(get(&deps, "/metrics", &auth).body(), r#"scout_list_cache_total{result="stale"}"#);
        assert_eq!(after - before, 1, "stale hits moved by {}, want 1", after - before);
    }
    todo!("pending: metrics_test.go:~101 TestMetrics_countsStaleHits")
}

// The route is unauthenticated, like /health, and must therefore withhold exactly what /health withholds:
// anything that says which debrid services or accounts this install uses. addbudget.go calls that a
// confirmation oracle and refuses to publish it; publishing it one route over would simply move the leak.
#[test]
#[ignore = "needs handler::serve / Deps::for_test (package 4)"]
fn metrics_carries_no_account_or_service_detail() {
    // Enable when package 4 lands: move this body up and delete the wrapper.
    #[cfg(any())]
    fn pending() {
        use crate::deps::Deps;
        use crate::config::DebridService;
        let deps = Deps::for_test(|d| d.metrics_token = Some(METRICS_TOKEN.to_owned()));
        get(&deps, &format!("/{}/stream/movie/tt1234567.json", crate::tests::ticket::valid_blob()), &[]);
        let auth = [("authorization", &format!("Bearer {METRICS_TOKEN}") as &str)];
        let body = get(&deps, "/metrics", &auth).body().clone();

        // Only the SAMPLES are data. HELP text is documentation and says "accounts" on purpose.
        let forbidden = [
            DebridService::TorBox.as_str(),
            DebridService::RealDebrid.as_str(),
            DebridService::Premiumize.as_str(),
            "tb-secret",
            "tt1234567",
        ];
        for line in body.lines() {
            if line.starts_with('#') || line.is_empty() {
                continue;
            }
            for f in forbidden {
                assert!(!line.contains(f), "/metrics sample discloses {f:?}: {line}");
            }
        }
    }
    todo!("pending: metrics_test.go:~158 TestMetrics_carriesNoAccountOrServiceDetail")
}

#[test]
fn metrics_debrid_series_have_fixed_cardinality() {
    let out = new_metric_set().render(-1);
    for endpoint in METRIC_TORBOX_ENDPOINTS {
        for outcome in METRIC_TORBOX_OUTCOMES {
            let want = format!(r#"scout_debrid_calls_total{{endpoint="{endpoint}",outcome="{outcome}"}} 0"#);
            assert!(out.contains(&want), "missing fixed series {want:?}");
        }
    }
    for reason in METRIC_TORBOX_AVOIDED {
        let want = format!(r#"scout_debrid_calls_avoided_total{{reason="{reason}"}} 0"#);
        assert!(out.contains(&want), "missing fixed series {want:?}");
    }
}

// The disk tier disables itself silently — one log line, then memory keeps serving — so the gauge is the
// only thing that separates "persisting" from "quietly re-paying a debrid resolve per probe on every
// redeploy". Three states, because "the backend does not report" is not the same as "it is off".
#[test]
fn metrics_cache_persistence_gauge() {
    // Go's optional `persistenceReporter`: a memory cache does not report, so the gauge is -1. The frozen
    // `Cache::persistent` defaults to `false`, which reads as 0 here; this assertion stays red until that
    // method can answer "not reporting" (see the report).
    assert_eq!(cache_persistent_gauge(&MemoryCache::new(1 << 20)), -1, "a backend with no durable tier");
    let dir = TempDir::new().unwrap();
    let working = TieredCache::new(1 << 20, dir.path());
    assert_eq!(cache_persistent_gauge(&working), 1, "a writable tier");
    // A file where the directory should be: MkdirAll fails, persistence disables itself.
    let base = TempDir::new().unwrap();
    let blocked = base.path().join("not-a-dir");
    write_file(&blocked, "x").unwrap();
    assert_eq!(cache_persistent_gauge(&TieredCache::new(1 << 20, &blocked)), 0, "an unwritable tier");
}
