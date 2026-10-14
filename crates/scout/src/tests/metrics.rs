//! Go: `internal/scout/metrics_test.go` (218 lines).

use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use tempfile::TempDir;

use crate::cache::{Cache, MemoryCache};
use crate::config::{DebridService, ALL_INDEXERS};
use crate::diskcache::{write_file, TieredCache};
use crate::handler::{join_cached, new_handler, split_cached, NO_STORE};
use crate::manifest::manifest_version;
use crate::metrics::{
    cache_persistent_gauge, new_metric_set, METRIC_TORBOX_AVOIDED, METRIC_TORBOX_ENDPOINTS,
    METRIC_TORBOX_OUTCOMES,
};
use crate::tests::handler::{body_str, get, test_deps, valid_blob, RecordingCache};

const METRICS_TOKEN: &str = "metrics-shared-secret";

fn metrics_auth() -> [(&'static str, String); 1] {
    [("authorization", format!("Bearer {METRICS_TOKEN}"))]
}

/// handler_test.go's `do(h, path, headers)` with owned header values.
async fn get_with(
    h: &crate::handler::Handler,
    path: &str,
    headers: &[(&str, String)],
) -> http::Response<Vec<u8>> {
    let borrowed: Vec<(&str, &str)> = headers.iter().map(|(k, v)| (*k, v.as_str())).collect();
    get(h, path, &borrowed).await
}

/// Go `sampleValue`: one series' value out of the exposition text, so a test can assert on the NUMBER rather
/// than on the series existing. `render()` emits every series unconditionally, so presence proves only that
/// render ran.
fn sample_value(body: &str, series: &str) -> i64 {
    let prefix = format!("{series} ");
    let line = body
        .lines()
        .find(|l| l.starts_with(&prefix))
        .unwrap_or_else(|| panic!("no series {series:?} in:\n{body}"));
    line[prefix.len()..].trim().parse().unwrap_or_else(|_| panic!("unparseable sample {line:?}"))
}

// The counters say when this install is being watched and which indexers it uses, so the route is gated
// and OFF entirely when no token is configured — 404, not an empty 200, so an unconfigured deployment
// does not advertise that there is something here.
#[tokio::test]
async fn metrics_is_gated() {
    let off = new_handler(test_deps(|_| {}));
    let rr = get(&off, "/metrics", &[]).await;
    assert_eq!(rr.status(), 404, "no token configured: {}, want 404", rr.status());

    let on = new_handler(test_deps(|d| d.metrics_token = METRICS_TOKEN.to_owned()));
    let padded = format!("Bearer  {METRICS_TOKEN} ");
    let correct = format!("Bearer {METRICS_TOKEN}");
    let cases: [(&str, Vec<(&str, &str)>, u16); 5] = [
        ("no header", vec![], 404),
        ("wrong token", vec![("authorization", "Bearer nope")], 404),
        ("no bearer prefix", vec![("authorization", METRICS_TOKEN)], 404),
        ("padded token", vec![("authorization", padded.as_str())], 200),
        ("correct", vec![("authorization", correct.as_str())], 200),
    ];
    for (name, headers, want) in cases {
        let rr = get(&on, "/metrics", &headers).await;
        assert_eq!(rr.status(), want, "{name}: {}, want {want}", rr.status());
    }
}

// The route answers in the exposition format, and the numbers actually move — asserted as DELTAS,
// because the counters are process-wide and every other test in this package feeds them too.
#[tokio::test]
async fn metrics_reports_what_was_previously_log_only() {
    let h = new_handler(test_deps(|d| d.metrics_token = METRICS_TOKEN.to_owned()));
    let before = body_str(&get_with(&h, "/metrics", &metrics_auth()).await);

    // One cold build (a miss) then a warm hit, so each counter has a known increment.
    let path = format!("/{}/stream/movie/tt1234567.json", valid_blob());
    get(&h, &path, &[]).await;
    get(&h, &path, &[]).await;

    let rr = get_with(&h, "/metrics", &metrics_auth()).await;
    assert_eq!(rr.status(), 200, "/metrics: {}", rr.status());
    let after = body_str(&rr);
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
    let body = after;
    let build_info = format!(r#"scout_build_info{{version="{}"}} 1"#, manifest_version());
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
        build_info.as_str(),
    ] {
        assert!(body.contains(want), "missing series {want:?}");
    }
    // Every indexer is present from the start — a series that only appears after the first failure is one
    // nobody can alert on.
    for id in ALL_INDEXERS {
        let series = format!(r#"scout_indexer_requests_total{{indexer="{id}"}}"#);
        assert!(body.contains(&series), "indexer {id} has no series before it is used");
    }
}

// The stale path has its own counter, and it is the one the stale-while-revalidate work exists to make
// visible — how often viewers are being served a list while it is rebuilt behind them.
#[tokio::test]
async fn metrics_counts_stale_hits() {
    let cache =
        Arc::new(RecordingCache { inner: Arc::new(MemoryCache::new(1 << 20)), last: Default::default() });
    let h = new_handler(test_deps(|d| {
        d.cache = cache.clone();
        d.metrics_token = METRICS_TOKEN.to_owned();
    }));
    let path = format!("/{}/stream/movie/tt1234567.json", valid_blob());
    get(&h, &path, &[]).await;

    let stale = r#"scout_list_cache_total{result="stale"}"#;
    let before = sample_value(&body_str(&get_with(&h, "/metrics", &metrics_auth()).await), stale);

    let key = cache.last_key();
    let held = cache.get(&key).expect("nothing cached");
    let (complete, _, etag, body) = split_cached(&held);
    let backdated =
        (SystemTime::now() - Duration::from_secs(1)).duration_since(UNIX_EPOCH).expect("epoch").as_secs()
            as i64;
    cache.put(&key, &join_cached(complete, backdated, etag, body), Duration::from_secs(60));
    get(&h, &path, &[]).await;

    let after = sample_value(&body_str(&get_with(&h, "/metrics", &metrics_auth()).await), stale);
    assert_eq!(after - before, 1, "stale hits moved by {}, want 1", after - before);
}

// The route is unauthenticated, like /health, and must therefore withhold exactly what /health withholds:
// anything that says which debrid services or accounts this install uses. addbudget.go calls that a
// confirmation oracle and refuses to publish it; publishing it one route over would simply move the leak.
#[tokio::test]
async fn metrics_carries_no_account_or_service_detail() {
    let h = new_handler(test_deps(|d| d.metrics_token = METRICS_TOKEN.to_owned()));
    get(&h, &format!("/{}/stream/movie/tt1234567.json", valid_blob()), &[]).await;
    let body = body_str(&get_with(&h, "/metrics", &metrics_auth()).await);

    // Only the SAMPLES are data. HELP text is documentation and says "accounts" on purpose.
    let blob = valid_blob();
    let forbidden = [
        DebridService::TorBox.as_str(),
        DebridService::RealDebrid.as_str(),
        DebridService::Premiumize.as_str(),
        "tb-secret",
        "tt1234567",
        blob.as_str(),
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
    // Go's optional `persistenceReporter`: a memory cache does not report (`Cache::persistent` is `None`), so
    // the gauge is -1.
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
