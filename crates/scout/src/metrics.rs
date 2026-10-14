//! Port of `internal/scout/metrics.go`. Work package 6.
//!
//! Counters for the questions the Go package used to answer only by grepping logs. Atomics only, so the request
//! path never allocates or contends. The Prometheus text is written by hand: a client library would be the first
//! runtime dependency this binary has.

use crate::cache::Cache;

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

/// Go `metricSet`. The counters are the porter's; every series in `render` is fixed at construction.
pub struct MetricSet {}

/// Go `newMetricSet`.
pub fn new_metric_set() -> MetricSet {
    todo!("metrics.go newMetricSet")
}

impl MetricSet {
    /// Go `render`: the Prometheus text exposition. `cache_persistent` is the durable-tier gauge (1, 0 or -1).
    pub fn render(&self, cache_persistent: i64) -> String {
        todo!("metrics.go metricSet.render")
    }
}

/// Go `cachePersistentGauge`: 1 when the durable tier is writing, 0 when it disabled itself, -1 when the backend
/// does not report. A backend that does not report is `Cache::persistent` answering `None`, which the frozen
/// trait does not yet express (see the report on tests/metrics.rs).
pub fn cache_persistent_gauge(c: &dyn Cache) -> i64 {
    todo!("metrics.go cachePersistentGauge")
}
