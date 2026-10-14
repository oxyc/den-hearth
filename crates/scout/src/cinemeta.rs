//! Port of `internal/scout/cinemeta.go`. Work package 5.
//!
//! Cinemeta (the public Stremio metadata addon) maps an IMDb id → title/year. den-scout uses those to drop
//! torrents a tracker mistagged with another title's id. It's a best-effort side lookup: any failure
//! returns ok=false and the stream list is served unfiltered.
//!
//! A series is looked up for its title alone, and that title only SINKS a release naming another show
//! (rank.go's otherShow, wrongShowSink) — it never drops one. Sinking is safe where dropping is not: when
//! every release is named in romaji or the original title, they all sink together and keep their order.
//!
//! The DROP filters are movies only, for two reasons. The first is the obvious one: Cinemeta gives a series a span —
//! "2019–2023" — of which firstYear can only take the start, and rank.go's year check allows ±1 around
//! it, so a correctly named Show.S04.2023.1080p carrying the SEASON's air year would be dropped as a
//! mistag.
//!
//! The second is why returning a title-without-year for series does not work either, which was tried and
//! reverted. rank.go's title-token check is only safe because of its escape hatch: it judges a release
//! solely when that release carries NO parseable year. A movie release almost always carries "(2024)", so
//! the check sees a small tail of nameless junk — which is what it was written for. A series EPISODE
//! release essentially never carries a year (it carries S04E01), so for series the escape hatch is
//! structurally unavailable and the token check would judge nearly every release. Measured against real
//! naming, that empties whole shows:
//!
//!   Attack on Titan → vs [Erai-raws] Shingeki no Kyojin - 01          (romaji)              → dropped
//!   Money Heist     → vs La.Casa.de.Papel.S01E01…                     (original title)      → dropped
//!
//! (Shōgun and Pokémon were dropped too, until titleTokens folded diacritics.) The whole filter is
//! speculative for series: the junk it was written to remove has only ever been observed on movies. So no
//! drop filter is applied to them.

use std::sync::Arc;
use std::time::Duration;

use reqwest::header::{HeaderValue, ACCEPT, USER_AGENT};
use serde::{Deserialize, Serialize};

use crate::cache::Cache;
use crate::indexerconfig::{null_default, read_capped, Flight};
use crate::rank::YEAR_TOKEN;
use crate::scrape::{Doer, MAX_SCRAPE_BYTES, SCRAPE_USER_AGENT};
use crate::util::key_hash;

/// Go `cinemetaBase`: the public Cinemeta service.
pub(crate) const CINEMETA_BASE: &str = "https://v3-cinemeta.strem.io";

/// Go `cinemetaHitTTL` is how long a lookup is remembered. A film's title and year do not change, so an
/// answer is kept for a week.
pub(crate) const CINEMETA_HIT_TTL: Duration = Duration::from_secs(7 * 24 * 60 * 60);
/// Go `cinemetaMissTTL`: a failure is remembered for ten minutes, so a Cinemeta outage or a title it has not
/// indexed yet is asked about again soon.
pub(crate) const CINEMETA_MISS_TTL: Duration = Duration::from_secs(10 * 60);
/// Go `cinemetaMissValue`: the cached value for a lookup that found nothing usable.
pub(crate) const CINEMETA_MISS_VALUE: &str = "-";

/// Go `cineMeta` is the subset of a Cinemeta record used to sanity-check tracker results. Year is 0 when
/// unknown; Title is "" when unknown.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub(crate) struct CineMeta {
    #[serde(rename = "Title")]
    pub title: String,
    #[serde(rename = "Year")]
    pub year: i64,
}

/// Go `cinemetaFlight`: concurrent builds of one film (several installs, or a list and its rebuild) share one
/// lookup.
static CINEMETA_FLIGHT: Flight<CineMeta> = Flight::new();

/// Go `cinemetaMeta` is the Meta dependency against a Cinemeta-compatible base URL, with lookups cached in
/// `cache`. Cinemeta-compatible providers are not interchangeable, so each base keeps its own cache keys.
pub(crate) struct Cinemeta {
    client: Arc<dyn Doer>,
    base: String,
    cache: Arc<dyn Cache>,
}

/// Go `cinemetaMeta`: builds the lookup against `base`, with its answers kept in `cache`.
pub(crate) fn cinemeta_meta(client: Arc<dyn Doer>, base: &str, cache: Arc<dyn Cache>) -> Cinemeta {
    Cinemeta { client, base: base.trim_end_matches('/').to_owned(), cache }
}

impl Cinemeta {
    /// Looks up a movie or series by its IMDb id. The bool is false when nothing usable was learned.
    pub(crate) async fn lookup(&self, typ: &str, imdb: &str) -> (CineMeta, bool) {
        if typ != "movie" && typ != "series" {
            return (CineMeta::default(), false);
        }
        let key = cinemeta_key(&self.base, typ, imdb);
        if let Some(v) = self.cache.get(&key) {
            if &*v != CINEMETA_MISS_VALUE {
                if let Ok(m) = serde_json::from_str::<CineMeta>(&v) {
                    return (m, true);
                }
            }
            return (CineMeta::default(), false);
        }
        let m = CINEMETA_FLIGHT
            .run(&key, async {
                let (m, ok) = fetch_cinemeta(self.client.as_ref(), &self.base, typ, imdb).await;
                if ok {
                    if let Ok(b) = serde_json::to_string(&m) {
                        self.cache.put(&key, &b, CINEMETA_HIT_TTL);
                    }
                } else {
                    // A lookup cut short by the build's own deadline says nothing about Cinemeta, so only a failure
                    // that ran its course is remembered. A dropped lookup never reaches this point.
                    self.cache.put(&key, CINEMETA_MISS_VALUE, CINEMETA_MISS_TTL);
                }
                m
            })
            .await;
        let found = m.year != 0 || !m.title.is_empty();
        (m, found)
    }
}

/// Go `cinemetaCacheKey`: the key a lookup is cached under. The base is hashed, so the URL itself is not stored.
pub(crate) fn cinemeta_key(base: &str, typ: &str, imdb: &str) -> String {
    format!("cinemeta:v2:{}:{typ}:{imdb}", key_hash(base.trim_end_matches('/')))
}

/// The record Cinemeta answers a lookup with, as far as it is read.
///
/// A JSON `null` leaves a field empty, as it does in Go's decoder, where serde would refuse the whole answer.
#[derive(Deserialize, Default)]
#[serde(default)]
struct CinemetaBody {
    #[serde(deserialize_with = "null_default")]
    meta: CinemetaMeta,
}

#[derive(Deserialize, Default)]
#[serde(default)]
struct CinemetaMeta {
    #[serde(deserialize_with = "null_default")]
    name: String,
    #[serde(deserialize_with = "null_default")]
    year: String,
    #[serde(rename = "releaseInfo", deserialize_with = "null_default")]
    release_info: String,
}

/// Go `fetchCinemeta` asks Cinemeta for one film or series.
async fn fetch_cinemeta(client: &dyn Doer, base: &str, typ: &str, imdb: &str) -> (CineMeta, bool) {
    let none = (CineMeta::default(), false);
    let Ok(url) = reqwest::Url::parse(&format!("{base}/meta/{typ}/{imdb}.json")) else { return none };
    let mut req = reqwest::Request::new(reqwest::Method::GET, url);
    req.headers_mut().insert(ACCEPT, HeaderValue::from_static("application/json"));
    req.headers_mut().insert(USER_AGENT, HeaderValue::from_static(SCRAPE_USER_AGENT));
    let Ok(resp) = client.do_request(req).await else { return none };
    if resp.status() != reqwest::StatusCode::OK {
        return none;
    }
    let Some(raw) = read_capped(resp, MAX_SCRAPE_BYTES).await else { return none };
    // Go decodes the first JSON value in the body, and ignores whatever follows it.
    let Some(Ok(body)) = serde_json::Deserializer::from_slice(&raw).into_iter::<CinemetaBody>().next() else {
        return none;
    };
    let mut m = CineMeta { title: body.meta.name.trim().to_owned(), year: 0 };
    let year = first_year(&body.meta.year);
    m.year = if year != 0 { year } else { first_year(&body.meta.release_info) };
    // Usable only if we learned at least one signal.
    let ok = m.year != 0 || !m.title.is_empty();
    (m, ok)
}

/// Go `firstYear` pulls the first plausible 4-digit year out of a string like "2026" or "2019–2023".
fn first_year(s: &str) -> i64 {
    YEAR_TOKEN.find(&str::to_lowercase(s)).and_then(|m| m.as_str().parse().ok()).unwrap_or(0)
}
