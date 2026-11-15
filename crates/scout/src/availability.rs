//! Port of `internal/scout/availability.go`.
//!
//! Stream availability for a page of titles at once: the question Den's poster rows ask — does this movie
//! have anything to play? — answered in one request instead of a probe per poster, for the TV and for a
//! browser that cannot probe addons itself.
//!
//! It answers AT ONCE from the verdicts on hand and says "unknown" for the rest, checking those behind the
//! reply: an uncached title can take the whole scrape timeout, and a row of posters must not wait on its
//! slowest title. The client asks again for what came back unknown, and unknown never hides a title.
//!
//! Movies and resolved series episodes. A series poster still has to choose an episode first; once it has one,
//! `tt…:S:E` uses the same scrape and verdict key as the ordinary series stream route.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use http::{Method, Request, StatusCode};
use serde::Deserialize;
use serde_json::json;
use tokio::time::{timeout_at, Instant as TokioInstant};

use crate::cache::CacheClass;
use crate::config::{Config, DebridService};
use crate::handler::{
    error, json_response, lock, method_not_allowed, spawn_background, split_path, to_json, Inner, RankOpts,
    RankedList, Resp, LIST_BUILD_SLACK, MAX_SEEDS,
};
use crate::id::{parse_stream_id, StreamId};
use crate::metrics::metrics;
use crate::rank::RawStream;
use crate::stores::pool::{config_has_service, has_cache_truth, StorePool};
use crate::stores::torbox::CACHE_BATCH;
use crate::stores::CacheTruth;
use crate::util::{fold_json_keys, key_hash, Shape, LEAF};

/// The TV's verdict lifetimes, asymmetric for its reason: a title with streams keeps them, while a
/// just-released one gains them within hours, so "none" is asked again in minutes.
const AVAILABLE_TTL: Duration = Duration::from_secs(30 * 24 * 60 * 60);
const UNAVAILABLE_TTL: Duration = Duration::from_secs(10 * 60);
/// How long a check that could not tell holds off the next one for that title. Without it, every ask
/// during an indexer outage starts another scrape for every unknown title on the page.
const AVAILABILITY_RETRY_AFTER: Duration = Duration::from_secs(60);
/// Go `maxAvailabilityIDs`: a page of posters is a few dozen titles; a request naming more is not a page.
pub(crate) const MAX_AVAILABILITY_IDS: usize = 100;
const MAX_AVAILABILITY_BODY: usize = 16 << 10;
/// Checks running behind replies, across the process. Each is a full scrape plus a debrid cache check,
/// paced by the indexer limiter; a title past this ceiling stays unknown and is checked on a later ask.
const MAX_AVAILABILITY_CHECKS: usize = 4;

// Cached verdicts. verdictUndetermined is a check that could not tell, kept for availabilityRetryAfter.
const VERDICT_AVAILABLE: &str = "1";
const VERDICT_UNAVAILABLE: &str = "0";
/// Go `verdictUndetermined`: the marker a failed or partial check leaves, which holds off the next check.
pub(crate) const VERDICT_UNDETERMINED: &str = "?";

/// Parse the ids accepted by the batch route through the stream route's parser, so the two entry points ask
/// indexers the same question. A movie has no separators; a resolved series episode has exactly `tt…:S:E`.
fn parse_availability_id(raw: &str) -> Option<StreamId> {
    // Unlike the stream route, this JSON contract carries a raw id rather than an `<id>.json` path segment.
    // Keep the old movie validation strict and do not let the shared route parser silently strip a suffix.
    if raw.ends_with(".json") {
        return None;
    }
    let separators = raw.bytes().filter(|b| *b == b':').count();
    match separators {
        0 => parse_stream_id("movie", raw),
        2 => parse_stream_id("series", raw),
        _ => None,
    }
}

/// The stable suffix shared by batch checks and stream-list read-through. Numeric aliases such as `01` collapse
/// to one verdict, avoiding duplicate background scrapes and preventing an episode verdict from becoming a
/// series-wide verdict.
pub(crate) fn verdict_id(sid: &StreamId) -> String {
    if sid.has_ep {
        format!("{}:{}:{}", sid.imdb, sid.season, sid.episode)
    } else {
        sid.imdb.clone()
    }
}

/// Go `verdictPrefix` keys a config's verdicts. The scope is left out, so the scoped config a browser holds reads
/// the verdicts the TV's full config writes: the same account, indexers and filters give the same answer.
/// The install id and epoch are left out for the same reason — the browser mints its scoped config
/// separately, so it carries an id of its own.
pub(crate) fn verdict_prefix(config: &Config) -> String {
    let f = &config.filters;
    // The hash of the config with its scope, install id and epoch cleared; what matters is that every other field
    // takes part, so two configs that ask different questions never share a verdict.
    let canonical = to_json(&json!({
        "CachedOnly": config.cached_only,
        "Debrid": config.debrid.iter().map(|d| json!({"Service": d.service.as_str(), "Token": d.token})).collect::<Vec<_>>(),
        "Epoch": 0,
        "Filters": {
            "ExcludeCam": f.exclude_cam,
            "ExcludeRegex": f.exclude_regex,
            "HDROnly": f.hdr_only,
            "MaxSizeGB": f.max_size_gb,
            "MinSeeders": f.min_seeders,
            "PreferResolution": f.prefer_resolution,
            "Resolutions": f.resolutions,
        },
        "IID": "",
        "Indexers": config.indexers.iter().map(|i| i.0.as_str()).collect::<Vec<_>>(),
        "ResultCap": config.result_cap,
        "Scope": "",
        "Sources": config.sources,
    }));
    format!("avail:{}:", key_hash(&canonical))
}

/// Releases the key of an availability check running behind a reply, however the check ends.
struct CheckDone {
    inner: Arc<Inner>,
    key: String,
}

impl Drop for CheckDone {
    fn drop(&mut self) {
        lock(&self.inner.avail_in_flight).remove(&self.key);
    }
}

impl Inner {
    /// handleAvailability — POST /<config>/availability {"ids":["tt…", "tt…:S:E"]} →
    /// {"availability":{"tt…":"available"|"unavailable"|"unknown"}}.
    pub(crate) fn handle_availability(self: &Arc<Self>, req: &Request<Bytes>, path: &str) -> Resp {
        if req.method() != Method::POST {
            return method_not_allowed("POST");
        }
        let parts = split_path(path);
        if parts.len() != 2 {
            return error(StatusCode::NOT_FOUND, "not_found");
        }
        let Some(config) = self.open_config(parts[0]) else {
            return error(StatusCode::BAD_REQUEST, "bad_config");
        };

        #[derive(Deserialize, Default)]
        struct Asked {
            #[serde(default)]
            ids: Option<Vec<String>>,
        }
        static SHAPE: Shape = Shape::Object(&[("ids", &LEAF)]);
        // The first JSON value in at most maxAvailabilityBody bytes; whatever follows it is ignored.
        let body = req.body();
        let window = &body[..body.len().min(MAX_AVAILABILITY_BODY)];
        let decoded = serde_json::Deserializer::from_slice(window)
            .into_iter::<serde_json::Value>()
            .next()
            .and_then(Result::ok)
            .and_then(|mut v| {
                fold_json_keys(&mut v, &SHAPE);
                serde_json::from_value::<Asked>(v).ok()
            });
        let ids = match decoded {
            Some(body) => body.ids.unwrap_or_default(),
            None => return error(StatusCode::BAD_REQUEST, "bad_request"),
        };
        if ids.len() > MAX_AVAILABILITY_IDS {
            return error(StatusCode::BAD_REQUEST, "bad_request");
        }
        let Some(parsed): Option<Vec<StreamId>> = ids.iter().map(|id| parse_availability_id(id)).collect()
        else {
            return error(StatusCode::BAD_REQUEST, "bad_id");
        };
        let prefix = verdict_prefix(&config);
        let mut out: BTreeMap<&str, &str> = BTreeMap::new();
        for (id, sid) in ids.iter().zip(parsed) {
            let key = format!("{prefix}{}", verdict_id(&sid));
            let verdict = match self.deps.cache.get(&key).as_deref() {
                Some(VERDICT_AVAILABLE) => "available",
                Some(VERDICT_UNAVAILABLE) => "unavailable",
                Some(VERDICT_UNDETERMINED) => "unknown",
                _ => {
                    self.check_behind(&config, sid, &key);
                    "unknown"
                }
            };
            out.insert(id, verdict);
        }
        json_response(StatusCode::OK, &json!({ "availability": out }))
    }

    fn record_verdict(&self, key: &str, available: bool) {
        if available {
            self.deps.cache.put_class(key, VERDICT_AVAILABLE, AVAILABLE_TTL, CacheClass::Durable);
        } else {
            self.deps.cache.put_class(key, VERDICT_UNAVAILABLE, UNAVAILABLE_TTL, CacheClass::Volatile);
        }
    }

    /// One playable release settles availability; an empty partial list cannot settle absence. This also
    /// covers a nonempty scrape whose surviving releases were all removed by the viewer's filters.
    pub(crate) fn record_list_verdict(&self, key: &str, list: &RankedList) {
        if !list.degraded.is_empty() || (list.ranked.is_empty() && !list.complete) {
            self.deps.cache.put_class(
                key,
                VERDICT_UNDETERMINED,
                AVAILABILITY_RETRY_AFTER,
                CacheClass::Volatile,
            );
            return;
        }
        self.record_verdict(key, !list.ranked.is_empty());
    }

    /// checkBehind starts a background check of one movie or resolved episode, unless one is already running for it or the ceiling
    /// is reached. Booked before the task exists, so a flood of asks costs a map lookup each rather than a
    /// task each.
    ///
    /// Detached from the request: the reply has gone by the time the check finishes, which is the point.
    fn check_behind(self: &Arc<Self>, config: &Config, sid: StreamId, key: &str) {
        {
            let mut running = lock(&self.avail_in_flight);
            if running.contains(key) || running.len() >= MAX_AVAILABILITY_CHECKS {
                return;
            }
            running.insert(key.to_owned());
        }
        let (this, config, key) = (self.clone(), config.clone(), key.to_owned());
        // A background task, so the request's recovery cannot see a panic here — and the booking is released from
        // a guard, so a panic cannot strand the key either.
        spawn_background(self.deps.runtime.clone(), "availability check", async move {
            let _done = CheckDone { inner: this.clone(), key: key.clone() };
            let deadline = TokioInstant::now() + this.deps.scrape_timeout + LIST_BUILD_SLACK;
            // Cache truth only changes ordering unless the install explicitly asks for cached-only results.
            // Availability is a boolean, so the default path can answer without spending a debrid cache check.
            if !config.cached_only && config_has_service(&config, DebridService::TorBox) {
                metrics().torbox_avoid("availability_default");
            }
            let list = this.rank_availability(deadline, &config, &sid).await;
            this.record_list_verdict(&key, &list);
        });
    }

    async fn rank_availability(
        self: &Arc<Self>,
        deadline: TokioInstant,
        config: &Config,
        sid: &StreamId,
    ) -> RankedList {
        // No rid: this runs detached from any request (checkBehind), well after the one that triggered it has
        // answered. The rank decision line simply omits it, like reqlog.go's own line does when one is absent.
        let opts = || RankOpts {
            client: None,
            check_cache: false,
            rid: "",
            audio: &[],
            fresh: false,
            answer_early: false,
        };
        if !config.cached_only || !has_cache_truth(config) {
            return self.rank_list(deadline, config, sid, None, opts()).await;
        }
        // Apply every ordinary filter before asking the debrid, but do not let the display cap hide a lower
        // ranked held release: availability asks whether ANY playable candidate exists, not which twenty to
        // show. Cache truth is then read one provider-sized batch at a time and stops on the first certain yes.
        let filter_config = Config { cached_only: false, result_cap: MAX_SEEDS as i64, ..config.clone() };
        let mut list = self.rank_list(deadline, &filter_config, sid, None, opts()).await;
        if !list.degraded.is_empty() || list.ranked.is_empty() {
            return list;
        }
        let pool = StorePool { stores: (self.deps.make_stores)(config) };
        let mut unknown = false;
        for start in (0..list.ranked.len()).step_by(CACHE_BATCH) {
            let end = (start + CACHE_BATCH).min(list.ranked.len());
            let hashes: Vec<String> = list.ranked[start..end].iter().map(|s| s.info_hash.clone()).collect();
            let (truth, ok) = match timeout_at(deadline, pool.cache_check(&hashes)).await {
                Ok(answer) => answer,
                Err(_) => (
                    CacheTruth { holders: Default::default(), known: Default::default(), complete: false },
                    false,
                ),
            };
            for (i, hash) in hashes.iter().enumerate() {
                if truth.cached(hash) {
                    let mut stream: RawStream = list.ranked[start + i].clone();
                    stream.cached = true;
                    stream.cache_known = true;
                    list.ranked = vec![stream];
                    list.truth = truth;
                    return list;
                }
            }
            if !ok || !truth.complete() {
                unknown = true;
            }
        }
        list.ranked = Vec::new();
        if unknown {
            list.degraded = "cache-check".to_owned();
        }
        list
    }
}
