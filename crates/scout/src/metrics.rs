//! Port of `internal/scout/metrics.go`.
//!
//! Counters for the questions this package used to answer only by grepping logs: which indexer is earning
//! its slice of the scrape budget, how often a list is served from cache, how many builds come back
//! degraded or short, whether the probe cache is warming.
//!
//! Every one of these facts was already computed and then dropped — scrapeAll knew which scrapers
//! answered, buildStreamList knew whether the list was degraded or partial, probeTop knew whether the key
//! was warm — so this adds observation, not work.
//!
//! Package-level rather than threaded through, because the interesting events happen in free functions
//! (scrapeAll) as much as on the handler, and passing a collector into all of them would be a much larger
//! change than the counters justify. Atomics only: these sit on the request path and must not allocate or
//! contend. The Prometheus text is written with `den_http::prom`, as every den addon writes its own.

use std::collections::HashMap;
use std::sync::atomic::{AtomicI64, Ordering};
#[cfg(not(test))]
use std::sync::LazyLock;

use den_http::prom;

use crate::addbudget::global_add_budget;
use crate::attributes::StreamAttributes;
use crate::cache::Cache;
use crate::config::ALL_INDEXERS;
use crate::coverage::{SourceReport, SOURCE_OUTCOMES};
use crate::manifest::manifest_version;
use crate::sources::OWN_SOURCES;

/// One counter per value, built once so the request path only ever reads this map.
type Series = HashMap<&'static str, AtomicI64>;

/// The attribute values released streams are counted under. Fixed, for the same reason the indexer series are:
/// a label that appears only once something matches it is a label nobody can alert on or divide by. "unknown"
/// is one of the values rather than a gap — an unprobed release is most of a real population, and leaving it
/// out would flatter every share computed from these.
const METRIC_CODECS: [&str; 4] = ["h264", "hevc", "av1", "unknown"];
const METRIC_RESOLUTIONS: [&str; 5] = ["2160p", "1080p", "720p", "480p", "unknown"];
const METRIC_AUDIO_CODECS: [&str; 12] =
    ["eac3", "ac3", "truehd", "dts", "dtshd", "dtshdma", "dtsx", "flac", "aac", "opus", "mp3", "unknown"];

/// Go `metricTorboxEndpoints`: the fixed debrid endpoints, one series per endpoint and outcome.
pub const METRIC_TORBOX_ENDPOINTS: [&str; 9] = [
    "checkcached",
    "mylist_account",
    "mylist_id",
    "requestdl",
    "createtorrent",
    "user",
    "controltorrent",
    "controlqueued",
    "getqueued",
];

/// Go `metricTorboxOutcomes`.
pub const METRIC_TORBOX_OUTCOMES: [&str; 2] = ["ok", "error"];

/// Go `metricTorboxAvoided`: the reasons a debrid call was avoided.
pub const METRIC_TORBOX_AVOIDED: [&str; 5] = [
    "availability_default",
    "status_snapshot",
    "checkcached_singleflight",
    "checkcached_batch",
    "probe_frontier",
];

/// Go `metricIndexers`: the indexer series are every built-in indexer, and one for every household's own sources
/// together — their hosts are chosen per install, so they are never a label.
fn metric_indexers() -> impl Iterator<Item = &'static str> {
    ALL_INDEXERS.into_iter().chain([OWN_SOURCES])
}

/// Go `metricSet`. Every series in `render` is fixed at construction.
pub struct MetricSet {
    pub list_cache_hit: AtomicI64,
    pub list_cache_stale: AtomicI64,
    pub list_cache_miss: AtomicI64,

    pub build_ok: AtomicI64,
    pub build_partial: AtomicI64,
    pub build_degraded: AtomicI64,

    pub probe_cache_hit: AtomicI64,
    pub probe_cache_miss: AtomicI64,
    /// Panics recovered on ANY background task — the probe fan-out, the stale-list rebuild and the
    /// account-listing fetch all route here. Deliberately not named for the probe: they share one recover
    /// helper, and booking a rebuild's panic to a probe series would ruin the metric an operator alerts on
    /// for a parser crash.
    pub background_panic: AtomicI64,

    /// Adds scout's own budget refused, by intent: a viewer waiting (the budget spent) or a prefetch (held
    /// back by the reserve kept for Play).
    pub add_refused_play: AtomicI64,
    pub add_refused_prefetch: AtomicI64,
    pub torbox_status_avoided: AtomicI64,
    pub torbox_status_active: AtomicI64,
    pub torbox_status_waiters: AtomicI64,

    torbox_calls: HashMap<&'static str, Series>,
    torbox_avoided: Series,

    // Fixed keys, populated once at construction and never written again, so concurrent reads need no
    // lock. Every indexer this build knows about gets an entry whether or not any install names it —
    // a counter that appears only after the first request is a counter you cannot alert on.
    indexer_requests: Series,
    indexer_failures: Series,
    /// Releases each indexer answered with, before dedupe: whether one that answers is also one that adds anything.
    indexer_releases: Series,
    /// How each configured source figured in a list build's coverage, by outcome. Unlike the request counters
    /// this includes the sources that were never asked (misconfigured, quarantined) and answers reused from the
    /// indexer answer cache, because coverage does.
    source_coverage: HashMap<&'static str, Series>,

    // Streams as they are SERVED, by the attributes a client ranks on. oxyc/den#26's "practical upshot"
    // table estimates what a modern debrid cache holds and says plainly that it is an estimate — nothing
    // in these repos sampled it. This is that sample, and it is deliberately taken at the point of
    // delivery rather than at the scrape: what matters is the population Den actually offers a viewer,
    // after dedupe and ranking, not the one an indexer happens to hold.
    release_codec: Series,
    release_resolution: Series,
    release_audio: Series,

    pub release_total: AtomicI64,
    pub release_hdr: AtomicI64,
    pub release_dolby_vision: AtomicI64,
    pub release_cached: AtomicI64,
}

/// Go `fixedSeries`.
fn fixed_series(values: &[&'static str]) -> Series {
    values.iter().map(|v| (*v, AtomicI64::new(0))).collect()
}

/// Go `newMetricSet`.
pub fn new_metric_set() -> MetricSet {
    MetricSet {
        list_cache_hit: AtomicI64::new(0),
        list_cache_stale: AtomicI64::new(0),
        list_cache_miss: AtomicI64::new(0),
        build_ok: AtomicI64::new(0),
        build_partial: AtomicI64::new(0),
        build_degraded: AtomicI64::new(0),
        probe_cache_hit: AtomicI64::new(0),
        probe_cache_miss: AtomicI64::new(0),
        background_panic: AtomicI64::new(0),
        add_refused_play: AtomicI64::new(0),
        add_refused_prefetch: AtomicI64::new(0),
        torbox_status_avoided: AtomicI64::new(0),
        torbox_status_active: AtomicI64::new(0),
        torbox_status_waiters: AtomicI64::new(0),
        torbox_calls: METRIC_TORBOX_ENDPOINTS
            .iter()
            .map(|e| (*e, fixed_series(&METRIC_TORBOX_OUTCOMES)))
            .collect(),
        torbox_avoided: fixed_series(&METRIC_TORBOX_AVOIDED),
        indexer_requests: fixed_series(&metric_indexers().collect::<Vec<_>>()),
        indexer_failures: fixed_series(&metric_indexers().collect::<Vec<_>>()),
        indexer_releases: fixed_series(&metric_indexers().collect::<Vec<_>>()),
        source_coverage: metric_indexers().map(|id| (id, fixed_series(&SOURCE_OUTCOMES))).collect(),
        release_codec: fixed_series(&METRIC_CODECS),
        release_resolution: fixed_series(&METRIC_RESOLUTIONS),
        release_audio: fixed_series(&METRIC_AUDIO_CODECS),
        release_total: AtomicI64::new(0),
        release_hdr: AtomicI64::new(0),
        release_dolby_vision: AtomicI64::new(0),
        release_cached: AtomicI64::new(0),
    }
}

#[cfg(not(test))]
static METRICS: LazyLock<MetricSet> = LazyLock::new(new_metric_set);

// A TEST build keeps the counters per thread, as the add budget does (addbudget.rs). Go's tests run one at a time
// and assert on DELTAS of the process-wide counters, which holds only when nothing else is counting meanwhile;
// cargo runs ours in parallel. Each test runs on its own thread (and a `#[tokio::test]` keeps all its tasks
// there), so the counters it reads are the ones it fed.
#[cfg(test)]
thread_local! {
    static METRICS: &'static MetricSet = Box::leak(Box::new(new_metric_set()));
}

/// Go's package-level `metrics`.
pub fn metrics() -> &'static MetricSet {
    #[cfg(not(test))]
    {
        &METRICS
    }
    #[cfg(test)]
    {
        METRICS.with(|m| *m)
    }
}

/// Panics recovered on a background task, for a test to compare before and after.
pub fn background_panic() -> i64 {
    metrics().background_panic.load(Ordering::Relaxed)
}

impl MetricSet {
    /// Go `torboxCall`.
    pub fn torbox_call(&self, endpoint: &str, ok: bool) {
        let outcome = if ok { "ok" } else { "error" };
        if let Some(c) = self.torbox_calls.get(endpoint).and_then(|s| s.get(outcome)) {
            c.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Go `torboxAvoid`.
    pub fn torbox_avoid(&self, reason: &str) {
        if let Some(c) = self.torbox_avoided.get(reason) {
            c.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Go `indexerResult` records one scrape attempt and the releases it returned. An indexer that is not in the
    /// fixed set is ignored rather than added, so a caller cannot grow this map at runtime and race the readers.
    pub fn indexer_result(&self, id: &str, ok: bool, releases: i64) {
        if let Some(c) = self.indexer_requests.get(id) {
            c.fetch_add(1, Ordering::Relaxed);
        }
        if let Some(c) = self.indexer_releases.get(id) {
            c.fetch_add(releases, Ordering::Relaxed);
        }
        if !ok {
            if let Some(c) = self.indexer_failures.get(id) {
                c.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    /// Go `coverage` records one build's source reports. Ids and outcomes outside the fixed sets are ignored.
    pub fn coverage(&self, sources: &[SourceReport]) {
        for s in sources {
            if let Some(c) = self.source_coverage.get(s.id.as_str()).and_then(|o| o.get(s.outcome.as_str())) {
                c.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    /// Go `release` records one stream as it goes out. Called per stream on the response path, so it only reads the
    /// fixed maps and adds to atomics — no allocation, no lock, nothing that can grow at runtime.
    pub(crate) fn release(&self, a: &StreamAttributes) {
        self.release_total.fetch_add(1, Ordering::Relaxed);
        bump(&self.release_codec, a.codec.as_deref().unwrap_or_default());
        bump(&self.release_resolution, a.resolution.as_deref().unwrap_or_default());
        bump(&self.release_audio, a.audio_codec.as_deref().unwrap_or_default());
        if a.hdr {
            self.release_hdr.fetch_add(1, Ordering::Relaxed);
        }
        if a.dolby_vision {
            self.release_dolby_vision.fetch_add(1, Ordering::Relaxed);
        }
        // Three answers, and only one of them counts: a nil Cached means nobody could ask, which is not "no".
        if a.cached == Some(true) {
            self.release_cached.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Go `render`: the Prometheus text exposition, written by hand — the format is a dozen lines of printing,
    /// and a client library would be the first runtime dependency this binary has.
    ///
    /// WHAT IS DELIBERATELY ABSENT: anything keyed by debrid account or service. addbudget.go withholds
    /// per-account detail from /health for a reason that applies here word for word — this route is
    /// unauthenticated, every other route is protected by an unguessable config segment, and on a single-
    /// install box a per-service counter says which services that install uses. The add-budget gauge is the
    /// same aggregate /health already publishes.
    ///
    /// Indexer names ARE published. They are not a credential: the set is compiled into this binary, listed
    /// in the README, and shared by every install running the defaults, and "torrentio failed 40 times" says
    /// nothing about anybody's account. Being able to see which indexer is failing is the main reason this
    /// endpoint exists.
    ///
    /// `cache_persistent`: 1 when the durable tier is writing, 0 when it disabled itself, -1 when the backend
    /// does not report. Worth a series of its own because that failure is otherwise SILENT — one log line at
    /// startup and then a service that looks perfectly healthy while re-paying a debrid resolve per probed
    /// release on every redeploy. A monitor can see it now.
    pub fn render(&self, cache_persistent: i64) -> String {
        let mut b = String::with_capacity(2048);
        let ld = |c: &AtomicI64| c.load(Ordering::Relaxed);

        // Which release is answering, in the same shape every den addon publishes (<addon>_build_info), so one
        // query across the stack shows what each box is running without asking each manifest.
        let version = format!(r#"version="{}""#, manifest_version());
        prom::series(&mut b, "scout_build_info", "gauge", "The running den-scout version.", &version, 1);

        counter(
            &mut b,
            "scout_list_cache_total",
            "Stream-list cache lookups by outcome.",
            &[
                (r#"result="hit""#.to_owned(), ld(&self.list_cache_hit)),
                (r#"result="stale""#.to_owned(), ld(&self.list_cache_stale)),
                (r#"result="miss""#.to_owned(), ld(&self.list_cache_miss)),
            ],
        );
        counter(
            &mut b,
            "scout_list_builds_total",
            "Stream-list builds by outcome.",
            &[
                (r#"result="ok""#.to_owned(), ld(&self.build_ok)),
                (r#"result="partial""#.to_owned(), ld(&self.build_partial)),
                (r#"result="degraded""#.to_owned(), ld(&self.build_degraded)),
            ],
        );
        counter(
            &mut b,
            "scout_probe_cache_total",
            "Track-probe cache lookups by outcome.",
            &[
                (r#"result="hit""#.to_owned(), ld(&self.probe_cache_hit)),
                (r#"result="miss""#.to_owned(), ld(&self.probe_cache_miss)),
            ],
        );
        counter(
            &mut b,
            "scout_background_panics_total",
            "Panics recovered on a background goroutine (probe fan-out, stale list rebuild, account listing fetch).",
            &[(String::new(), ld(&self.background_panic))],
        );

        let mut torbox_calls =
            Vec::with_capacity(METRIC_TORBOX_ENDPOINTS.len() * METRIC_TORBOX_OUTCOMES.len());
        for endpoint in METRIC_TORBOX_ENDPOINTS {
            for outcome in METRIC_TORBOX_OUTCOMES {
                torbox_calls.push((
                    format!(r#"endpoint="{endpoint}",outcome="{outcome}""#),
                    ld(&self.torbox_calls[endpoint][outcome]),
                ));
            }
        }
        counter(
            &mut b,
            "scout_debrid_calls_total",
            "Debrid API calls by fixed endpoint and outcome.",
            &torbox_calls,
        );
        counter(
            &mut b,
            "scout_debrid_calls_avoided_total",
            "Debrid API calls avoided by coordination or request policy.",
            &series(&self.torbox_avoided, &METRIC_TORBOX_AVOIDED, "reason"),
        );
        let active = ld(&self.torbox_status_active);
        prom::series(
            &mut b,
            "scout_debrid_status_active",
            "gauge",
            "Active shared status reads.",
            "",
            active,
        );
        let waiters = ld(&self.torbox_status_waiters);
        let help = "Requests waiting on shared status reads.";
        prom::series(&mut b, "scout_debrid_status_waiters", "gauge", help, "", waiters);

        let per_indexer = |counters: &Series| -> Vec<(String, i64)> {
            metric_indexers().map(|id| (format!(r#"indexer="{id}""#), ld(&counters[id]))).collect()
        };
        counter(
            &mut b,
            "scout_indexer_requests_total",
            "Scrape attempts per indexer.",
            &per_indexer(&self.indexer_requests),
        );
        counter(
            &mut b,
            "scout_indexer_failures_total",
            "Scrape attempts that did not answer, per indexer.",
            &per_indexer(&self.indexer_failures),
        );
        counter(
            &mut b,
            "scout_indexer_releases_total",
            "Releases each indexer answered with, before dedupe.",
            &per_indexer(&self.indexer_releases),
        );

        let mut cov = Vec::with_capacity((ALL_INDEXERS.len() + 1) * SOURCE_OUTCOMES.len());
        for id in metric_indexers() {
            for outcome in SOURCE_OUTCOMES {
                cov.push((
                    format!(r#"indexer="{id}",outcome="{outcome}""#),
                    ld(&self.source_coverage[id][outcome]),
                ));
            }
        }
        counter(
            &mut b,
            "scout_source_coverage_total",
            "Configured sources in each list build's coverage, by outcome.",
            &cov,
        );

        counter(
            &mut b,
            "scout_released_streams_total",
            "Streams served to a client, the denominator for the shares below.",
            &[(String::new(), ld(&self.release_total))],
        );
        counter(
            &mut b,
            "scout_released_video_codec_total",
            "Streams served, by video codec.",
            &series(&self.release_codec, &METRIC_CODECS, "codec"),
        );
        counter(
            &mut b,
            "scout_released_resolution_total",
            "Streams served, by resolution.",
            &series(&self.release_resolution, &METRIC_RESOLUTIONS, "resolution"),
        );
        counter(
            &mut b,
            "scout_released_audio_codec_total",
            "Streams served, by source audio codec.",
            &series(&self.release_audio, &METRIC_AUDIO_CODECS, "codec"),
        );
        counter(
            &mut b,
            "scout_released_feature_total",
            "Streams served carrying a feature, as a share of scout_released_streams_total.",
            &[
                (r#"feature="hdr""#.to_owned(), ld(&self.release_hdr)),
                (r#"feature="dolby_vision""#.to_owned(), ld(&self.release_dolby_vision)),
                (r#"feature="cached""#.to_owned(), ld(&self.release_cached)),
            ],
        );

        // The tightest remaining debrid add allowance, aggregated across accounts exactly as /health reports
        // it. -1 when nothing has been spent yet, which is distinct from 0 (spent out).
        let budget = global_add_budget();
        let (mut left, accounts) = budget.lowest();
        if accounts == 0 {
            left = -1;
        }
        let help = "Smallest remaining hourly add allowance across accounts (-1 = none spent).";
        prom::series(&mut b, "scout_add_budget_remaining", "gauge", help, "", left);

        // The same allowance as each intent sees it: a prefetch stops the reserve short of zero.
        let mut prefetch_left = left;
        if left >= 0 {
            prefetch_left = (left - budget.play_reserve()).max(0);
        }
        prom::family(
            &mut b,
            "scout_add_budget_remaining_by_intent",
            "gauge",
            "Smallest remaining hourly add allowance across accounts, as a viewer waiting (play) and a prefetch \
             see it (-1 = none spent).",
            &[(r#"intent="play""#, left), (r#"intent="prefetch""#, prefetch_left)],
        );
        counter(
            &mut b,
            "scout_add_budget_refusals_total",
            "Adds scout's own hourly budget refused, by intent.",
            &[
                (r#"intent="play""#.to_owned(), ld(&self.add_refused_play)),
                (r#"intent="prefetch""#.to_owned(), ld(&self.add_refused_prefetch)),
            ],
        );

        let help = "Durable cache tier writing (1), disabled (0), not reported (-1).";
        prom::series(&mut b, "scout_cache_persistent", "gauge", help, "", cache_persistent);

        b
    }
}

/// Go `series` renders one fixed set of counters in a stable order, so a scrape's output does not reorder between
/// reads the way a map's iteration would.
fn series(counters: &Series, values: &[&'static str], label: &str) -> Vec<(String, i64)> {
    values.iter().map(|v| (format!(r#"{label}="{v}""#), counters[*v].load(Ordering::Relaxed))).collect()
}

/// Go `bump` counts a value against its series, or against "unknown" when the value is empty or not one this
/// build knows — so a spelling nobody anticipated lands somewhere visible instead of vanishing.
fn bump(counters: &Series, value: &str) {
    if let Some(c) = counters.get(value) {
        c.fetch_add(1, Ordering::Relaxed);
        return;
    }
    if let Some(c) = counters.get("unknown") {
        c.fetch_add(1, Ordering::Relaxed);
    }
}

/// Go `cachePersistentGauge` maps a cache onto the gauge's three states: 1 when the durable tier is writing, 0 when
/// it disabled itself, -1 when the backend does not report (`Cache::persistent` answering `None`).
pub fn cache_persistent_gauge(c: &dyn Cache) -> i64 {
    match c.persistent() {
        None => -1,
        Some(true) => 1,
        Some(false) => 0,
    }
}

/// One counter's HELP/TYPE block and its samples. An empty label string means an unlabelled series.
fn counter(b: &mut String, name: &str, help: &str, samples: &[(String, i64)]) {
    prom::family::<i64>(b, name, "counter", help, &[]);
    for (labels, value) in samples {
        prom::sample(b, name, labels, value);
    }
}
