//! `GET /metrics`: the Prometheus text exposition format, written by hand — it is a dozen lines of
//! printing, and a client library would be a dependency for that alone.
//!
//! Only facts the addon already keeps for its own reasons, read when a scrape asks: the OpenSubtitles
//! failure streak behind /health, the cache's live size and disk-write failures, and the sync slots
//! and progress entries that exist to bound and report long work. Nothing is counted just for this
//! route, and nothing runs between scrapes.
//!
//! Behind a bearer token, and off without one, as den-scout's is. No series carries an install's
//! config, but polled over time the gauges still say when the household is watching — a sync slot
//! taken, a translation running — which /health does not. Unauthorized answers the same 404 as an
//! unknown path, so a box with no token configured does not advertise that there is anything here.

use std::fmt::Write;
use std::sync::atomic::Ordering;

use den_http::prom::series;
use hyper::header::{HeaderMap, CACHE_CONTROL, CONTENT_TYPE};
use hyper::{Response, StatusCode};

use crate::httputil::{self, full, Body};
use crate::state::{AppState, SyncTier};

pub fn handle(state: &AppState, headers: &HeaderMap) -> Response<Body> {
    // An empty token disables the route rather than matching an empty header.
    if !den_http::metrics_authorized(headers, Some(state.cfg.metrics_token.as_str())) {
        return httputil::not_found();
    }
    Response::builder()
        .status(StatusCode::OK)
        .header(CONTENT_TYPE, "text/plain; version=0.0.4; charset=utf-8")
        .header(CACHE_CONTROL, "no-store")
        .body(full(render(state)))
        .unwrap()
}

fn render(state: &AppState) -> String {
    let cache = state.cache.stats();
    let mut out = String::with_capacity(1024);
    let version = format!("version=\"{}\"", crate::release_version());
    series(
        &mut out,
        "subtitles_build_info",
        "gauge",
        "Always 1; the label is the running build.",
        &version,
        1,
    );
    series(
        &mut out,
        "subtitles_consecutive_failures",
        "gauge",
        "Upstream calls failed in a row, by kind; opensubtitles is the search streak /health reports \
         degraded from at 3.",
        "kind=\"opensubtitles\"",
        state.os_fails.load(Ordering::Relaxed),
    );
    series(
        &mut out,
        "subtitles_cache_memory_bytes",
        "gauge",
        "Bytes held by the in-memory cache tier, against CACHE_MAX_BYTES.",
        "",
        cache.bytes,
    );
    series(
        &mut out,
        "subtitles_cache_memory_entries",
        "gauge",
        "Entries in the in-memory cache tier.",
        "",
        cache.entries,
    );
    series(
        &mut out,
        "subtitles_cache_disk_enabled",
        "gauge",
        "1 when the disk cache tier is on, 0 when its directory was unwritable at boot.",
        "",
        u8::from(cache.disk),
    );
    series(
        &mut out,
        "subtitles_cache_disk_write_failures_total",
        "counter",
        "Disk cache writes that failed; the log warns on the first only.",
        "",
        cache.disk_write_failures,
    );
    sync_series(&mut out, state);
    series(
        &mut out,
        "subtitles_translations_running",
        "gauge",
        "Translations in progress.",
        "",
        state.progress.tracked(),
    );
    out
}

fn sync_series(out: &mut String, state: &AppState) {
    let tiers = [
        ("tier1", state.sync_admission.stats(SyncTier::Tier1)),
        ("tier2", state.sync_admission.stats(SyncTier::Tier2)),
    ];
    let _ = writeln!(
        out,
        "# HELP subtitles_sync_jobs_running Sync binaries running now, by tier.\n\
         # TYPE subtitles_sync_jobs_running gauge"
    );
    for (tier, stats) in tiers {
        let _ = writeln!(out, "subtitles_sync_jobs_running{{tier=\"{tier}\"}} {}", stats.running);
    }
    let _ = writeln!(
        out,
        "# HELP subtitles_sync_queue_duration_seconds_total Time spent waiting for sync admission.\n\
         # TYPE subtitles_sync_queue_duration_seconds_total counter"
    );
    for (tier, stats) in tiers {
        let seconds = stats.queue_ns as f64 / 1_000_000_000.0;
        let _ = writeln!(out, "subtitles_sync_queue_duration_seconds_total{{tier=\"{tier}\"}} {seconds}");
    }
    let _ = writeln!(
        out,
        "# HELP subtitles_sync_queue_requests_total Sync jobs admitted after queueing.\n\
         # TYPE subtitles_sync_queue_requests_total counter"
    );
    for (tier, stats) in tiers {
        let _ = writeln!(out, "subtitles_sync_queue_requests_total{{tier=\"{tier}\"}} {}", stats.queued);
    }
    let _ = writeln!(
        out,
        "# HELP subtitles_sync_jobs_total Finished sync jobs by tier and outcome.\n\
         # TYPE subtitles_sync_jobs_total counter"
    );
    for (tier, stats) in tiers {
        for (outcome, value) in [
            ("completed", stats.completed),
            ("timed_out", stats.timed_out),
            ("failed", stats.failed),
            ("cancelled", stats.cancelled),
        ] {
            let _ =
                writeln!(out, "subtitles_sync_jobs_total{{tier=\"{tier}\",outcome=\"{outcome}\"}} {value}");
        }
    }
}
