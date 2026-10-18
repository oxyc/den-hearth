//! Downloads that keep looking while no Den client is open.
//!
//! A Den client searches a wanted download again only while it is open and holds the download lease (den-spec
//! library-v4 §17 *Searching again*), and neither an Apple TV in the background nor a closed tab is. So the lease
//! holder hands Hearth a "look for these" list on `POST /den/<config>/wanted`: per wanted (exhausted) row, only the
//! content's IMDb id, season and episode, its air date, the releases already tried, the row's search count and when
//! it ran out, and the two languages the ranking's dub rule reads. No library key, no title, no history. A POST
//! replaces the install's list (an empty one clears it); `GET` reads it back. Both answer each item's state.
//!
//! Hearth searches each item through the install's own Scout on den-core's `download_retry` schedule, from the row's
//! own count and time, so it searches when the row would; picks with den-core's `rank_releases`, as the clients do;
//! and makes the add a client makes (`/p/<ticket>?prefetch=1`). A started item is answered with the stream Scout
//! listed, which the client adopts into its row as if it had picked it, and its next list leaves the item out.
//!
//! Bounded: `MAX_ITEMS` per install, `MAX_INSTALLS` installs, `SEARCHES_PER_TICK` searches a minute across all of
//! them, and an install untouched for 30 days is forgotten.
//!
//! Held in RAM. With sealing on, the lists — with the sealed config each was sent under, which Hearth needs to search
//! later — are also kept in one small JSON file beside Scout's disk cache, so a redeploy (several a week) doesn't
//! forget them until a client next opens. It is written when a list changes or an item starts, else at most every 15
//! minutes. Without sealing nothing is written: a plaintext config segment is a debrid token.

use std::collections::HashMap;
use std::future::Future;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;
use http_body_util::{BodyExt, Empty, Limited};
use hyper::{Method, Request, Response, StatusCode};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use crate::{Body, Hearth};

/// Items one install may list.
pub const MAX_ITEMS: usize = 200;
/// Installs held at once.
pub const MAX_INSTALLS: usize = 32;
/// Searches made each tick, across every install: Hearth's share of the indexers and the debrid, beside the clients'.
pub const SEARCHES_PER_TICK: usize = 4;
pub const TICK: Duration = Duration::from_secs(60);
/// An install whose list was neither sent nor read for this long is forgotten.
pub const EXPIRY_MS: i64 = 30 * 86_400_000;
/// A search whose answer couldn't settle anything is asked again after this, as a client asks again.
pub const UNANSWERED_WAIT_MS: i64 = 5 * 60_000;
const FLUSH_EVERY_MS: i64 = 15 * 60_000;
const MAX_TRIED: usize = 64;
const MAX_BODY: usize = 256 << 10;
const MAX_LISTING: usize = 4 << 20;
const MAX_SAFE: i64 = 9_007_199_254_740_991;

/// One wanted download, as the client sends it.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Item {
    /// The row's content key, `<movie|tv>:<tmdb id>:<season or -1>:<episode or -1>`; the answer names it by this.
    pub key: String,
    pub imdb: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub season: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub episode: Option<u32>,
    /// When the content airs (ms): nothing is searched before it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub air_date: Option<i64>,
    #[serde(default)]
    pub tried: Vec<String>,
    /// The row's `retries`, and the wall time (ms) its `exhausted` was last written: `download_retry`'s inputs.
    #[serde(default)]
    pub retries: i64,
    pub since: i64,
    /// When the download was asked for (the row's `queuedAt`, ms): the stage the pick's time-to-watchable counts from.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub queued_at: Option<i64>,
    /// Releases still being fetched for this content (a row that is not merely wanted): the pick weighs them.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub in_flight: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub original_language: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub preferred_language: Option<String>,
}

impl Item {
    fn valid(&self) -> bool {
        let short = |s: &str, max: usize| !s.is_empty() && s.len() <= max;
        let language = |l: &Option<String>| l.as_deref().is_none_or(|l| short(l, 16));
        short(&self.key, 64)
            && self.imdb.len() <= 16
            && self
                .imdb
                .strip_prefix("tt")
                .is_some_and(|n| !n.is_empty() && n.bytes().all(|b| b.is_ascii_digit()))
            && self.season.is_some() == self.episode.is_some()
            && self.air_date.is_none_or(|at| (0..=MAX_SAFE).contains(&at))
            && (0..=MAX_SAFE).contains(&self.since)
            && (0..=64).contains(&self.retries)
            && self.tried.len() <= MAX_TRIED
            && self.tried.iter().all(|t| short(t, 512))
            && self.queued_at.is_none_or(|at| (0..=MAX_SAFE).contains(&at))
            && self.in_flight.len() <= MAX_TRIED
            && self.in_flight.iter().all(|t| short(t, 512))
            && language(&self.original_language)
            && language(&self.preferred_language)
    }

    /// The id Scout lists it under.
    fn stream_id(&self) -> String {
        match (self.season, self.episode) {
            (Some(season), Some(episode)) => format!("{}:{season}:{episode}", self.imdb),
            _ => self.imdb.clone(),
        }
    }
}

/// A release Hearth started for an item.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct Started {
    at: i64,
    identity: String,
    /// Viable releases the search offered, for the row's `candidates`.
    candidates: usize,
    /// The stream as Scout listed it, which the client reads as it reads its own list.
    stream: Value,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct Tracked {
    item: Item,
    /// The search count and the time the schedule counts from, the client's until Hearth searches itself.
    retries: i64,
    since: i64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    started: Option<Started>,
    #[serde(skip)]
    unanswered_until: i64,
}

#[derive(Serialize, Deserialize)]
struct Install {
    config: String,
    touched: i64,
    items: Vec<Tracked>,
}

#[derive(Default)]
struct State {
    installs: HashMap<String, Install>,
    dirty: bool,
    flushed: i64,
}

type Fut<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// Where Hearth asks for a listing and makes an add: Scout in process, or a fake in tests.
pub trait Finder: Send + Sync {
    /// Scout's answer to `/<config>/stream/<movie|series>/<id>.json`, or None when it gave none.
    fn streams<'a>(
        &'a self,
        hearth: &'a Hearth,
        config: &'a str,
        id: &'a str,
        series: bool,
    ) -> Fut<'a, Option<Value>>;
    /// The status Scout answered the add of the release at `url` with.
    fn add<'a>(&'a self, hearth: &'a Hearth, url: &'a str) -> Fut<'a, StatusCode>;
}

/// Scout, through the combined addon's handler: the lists and tickets a client gets.
pub struct InProcess;

impl Finder for InProcess {
    fn streams<'a>(
        &'a self,
        hearth: &'a Hearth,
        config: &'a str,
        id: &'a str,
        series: bool,
    ) -> Fut<'a, Option<Value>> {
        Box::pin(async move {
            let scout = hearth.scout.as_ref()?;
            let kind = if series { "series" } else { "movie" };
            let req = Request::get(format!("/{config}/stream/{kind}/{id}.json"))
                .body(Empty::<Bytes>::new())
                .ok()?;
            let resp = scout.serve_den(req).await;
            if !resp.status().is_success() {
                return None;
            }
            let body = Limited::new(resp.into_body(), MAX_LISTING).collect().await.ok()?.to_bytes();
            serde_json::from_slice(&body).ok()
        })
    }

    fn add<'a>(&'a self, hearth: &'a Hearth, url: &'a str) -> Fut<'a, StatusCode> {
        Box::pin(async move {
            let (Some(scout), Some(path)) = (hearth.scout.as_ref(), ticket_path(url)) else {
                return StatusCode::BAD_REQUEST;
            };
            match Request::get(format!("{path}?prefetch=1")).body(Empty::<Bytes>::new()) {
                Ok(req) => scout.serve_den(req).await.status(),
                Err(_) => StatusCode::BAD_REQUEST,
            }
        })
    }
}

/// `/p/<ticket>` from a play URL Scout's Den handler listed, absolute or not, under whatever public base it names.
fn ticket_path(url: &str) -> Option<&str> {
    let path = match url.split_once("://") {
        Some((_, rest)) => &rest[rest.find('/')?..],
        None => url,
    };
    let path = &path[path.rfind("/p/")?..];
    let ticket = &path["/p/".len()..];
    (!ticket.is_empty() && ticket.bytes().all(|b| b.is_ascii_alphanumeric() || b"-_.~%".contains(&b)))
        .then_some(path)
}

pub struct Wanted {
    state: Mutex<State>,
    finder: Box<dyn Finder>,
    /// Where the lists are kept across a restart; None keeps them in RAM only.
    file: Option<PathBuf>,
}

/// A due search, taken out of the state so the lock is not held across Scout.
struct Job {
    install: String,
    config: String,
    item: Item,
    attempt: i64,
    /// The row it stands for, as `download_retry` read it.
    row: Value,
}

enum Outcome {
    /// Nothing settled: a source didn't answer, or the add wasn't accepted. Asked again shortly.
    Unanswered,
    /// A complete answer with nothing to fetch: the next search waits longer.
    Nothing,
    Started(Started),
}

impl Wanted {
    pub fn new(finder: Box<dyn Finder>, file: Option<PathBuf>) -> Wanted {
        let installs = file.as_ref().map(load).unwrap_or_default();
        Wanted { state: Mutex::new(State { installs, ..State::default() }), finder, file }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    /// Hearth's startup line.
    pub fn summary(&self) -> String {
        let state = self.lock();
        let items: usize = state.installs.values().map(|i| i.items.len()).sum();
        format!(
            "wanted downloads: {} install(s), {items} item(s); kept across restarts: {}",
            state.installs.len(),
            if self.file.is_some() { "yes" } else { "no (RAM only)" }
        )
    }

    /// Replace an install's list, keeping what Hearth learned of each item still on it.
    fn replace(
        &self,
        key: &str,
        config: &str,
        items: Vec<Item>,
        now: i64,
    ) -> Result<Value, (StatusCode, &'static str)> {
        let mut state = self.lock();
        if items.is_empty() {
            if state.installs.remove(key).is_some() {
                state.dirty = true;
            }
            drop(state);
            self.flush(now, true);
            return Ok(json!({"items": []}));
        }
        if !state.installs.contains_key(key) && state.installs.len() >= MAX_INSTALLS {
            return Err((StatusCode::SERVICE_UNAVAILABLE, "wanted_full"));
        }
        let mut list: Vec<Item> = Vec::with_capacity(items.len());
        for item in items {
            list.retain(|other| other.key != item.key);
            list.push(item);
        }
        let install = state.installs.entry(key.to_owned()).or_insert_with(|| Install {
            config: config.to_owned(),
            touched: now,
            items: Vec::new(),
        });
        let before = serde_json::to_vec(&(&install.config, &install.items)).unwrap_or_default();
        let mut kept = std::mem::take(&mut install.items);
        install.items = list
            .into_iter()
            .map(|item| match kept.iter().position(|t| t.item.key == item.key) {
                Some(at) => {
                    let mut tracked = kept.swap_remove(at);
                    // The row moved on (its holder searched): its schedule wins over Hearth's.
                    if (item.retries, item.since) > (tracked.retries, tracked.since) {
                        tracked.retries = item.retries;
                        tracked.since = item.since;
                        tracked.unanswered_until = 0;
                    }
                    // A start the client tried and gave up on is no start.
                    if tracked.started.as_ref().is_some_and(|s| item.tried.contains(&s.identity)) {
                        tracked.started = None;
                    }
                    tracked.item = item;
                    tracked
                }
                None => Tracked {
                    retries: item.retries,
                    since: item.since,
                    item,
                    started: None,
                    unanswered_until: 0,
                },
            })
            .collect();
        install.config = config.to_owned();
        install.touched = now;
        let changed = before != serde_json::to_vec(&(&install.config, &install.items)).unwrap_or_default();
        let answer = statuses(install, now);
        state.dirty |= changed;
        drop(state);
        if changed {
            self.flush(now, true);
        }
        Ok(answer)
    }

    fn read(&self, key: &str, now: i64) -> Value {
        let mut state = self.lock();
        match state.installs.get_mut(key) {
            Some(install) => {
                install.touched = now;
                statuses(install, now)
            }
            None => json!({"items": []}),
        }
    }

    /// The searches due now, the earliest first, at most `SEARCHES_PER_TICK`; installs past their expiry dropped.
    fn due(&self, now: i64) -> Vec<Job> {
        let mut state = self.lock();
        let before = state.installs.len();
        state.installs.retain(|_, install| now - install.touched < EXPIRY_MS);
        if state.installs.len() != before {
            state.dirty = true;
        }
        let mut due: Vec<(i64, Job)> = Vec::new();
        for (key, install) in &state.installs {
            for tracked in &install.items {
                if tracked.started.is_some() || tracked.unanswered_until > now {
                    continue;
                }
                let Some((is_due, at, attempt)) = retry(tracked, now) else { continue };
                if is_due {
                    let job = Job {
                        install: key.clone(),
                        config: install.config.clone(),
                        item: tracked.item.clone(),
                        attempt,
                        row: row(tracked),
                    };
                    due.push((at, job));
                }
            }
        }
        due.sort_by_key(|(at, _)| *at);
        due.into_iter().take(SEARCHES_PER_TICK).map(|(_, job)| job).collect()
    }

    async fn search(&self, hearth: &Hearth, job: &Job) -> Outcome {
        let item = &job.item;
        let answer = self.finder.streams(hearth, &job.config, &item.stream_id(), item.season.is_some()).await;
        let Some((stream, identity, candidates)) = decide(answer.as_ref(), &job.row) else {
            return if answer.as_ref().is_some_and(complete) {
                Outcome::Nothing
            } else {
                Outcome::Unanswered
            };
        };
        let url = stream["url"].as_str().unwrap_or_default();
        let status = self.finder.add(hearth, url).await;
        if status.is_success() || status.is_redirection() {
            Outcome::Started(Started { at: 0, identity, candidates, stream })
        } else {
            eprintln!("wanted: {} search {}: the add was answered {status}", item.key, job.attempt + 1);
            Outcome::Unanswered
        }
    }

    fn settle(&self, job: &Job, outcome: Outcome, now: i64) {
        let mut state = self.lock();
        let Some(tracked) = state
            .installs
            .get_mut(&job.install)
            .and_then(|install| install.items.iter_mut().find(|t| t.item == job.item))
        else {
            // The list changed while Scout was asked: the next tick decides against what it says now.
            return;
        };
        let what = format!("wanted: {} search {}", job.item.key, job.attempt + 1);
        let urgent = match outcome {
            Outcome::Unanswered => {
                tracked.unanswered_until = now + UNANSWERED_WAIT_MS;
                eprintln!("{what} got no full answer; asking again in {} min", UNANSWERED_WAIT_MS / 60_000);
                return;
            }
            Outcome::Nothing => {
                tracked.retries = job.attempt + 1;
                tracked.since = now;
                eprintln!("{what} found nothing to fetch");
                false
            }
            Outcome::Started(started) => {
                eprintln!("{what} started {}", started.identity);
                tracked.retries = job.attempt + 1;
                tracked.started = Some(Started { at: now, ..started });
                true
            }
        };
        state.dirty = true;
        drop(state);
        self.flush(now, urgent);
    }

    /// Write the lists down, when there is a file and something changed: at once when `urgent`, else at most every
    /// `FLUSH_EVERY_MS`.
    pub fn flush(&self, now: i64, urgent: bool) {
        let Some(file) = &self.file else { return };
        let bytes = {
            let mut state = self.lock();
            if !state.dirty || (!urgent && now - state.flushed < FLUSH_EVERY_MS) {
                return;
            }
            state.dirty = false;
            state.flushed = now;
            serde_json::to_vec(&state.installs).unwrap_or_default()
        };
        if let Err(e) = write_private(file, &bytes) {
            eprintln!("wanted: couldn't keep the lists at {}: {e}", file.display());
        }
    }
}

/// `download_retry` on the row an item stands for: whether it is due, when, and which search it is.
fn retry(tracked: &Tracked, now: i64) -> Option<(bool, i64, i64)> {
    let answer = core(json!({"op": "download_retry", "row": row(tracked), "now": now}))?;
    Some((answer["due"].as_bool()?, answer["at"].as_i64()?, answer["attempt"].as_i64()?))
}

/// The download row an item stands for, as den-core's rules read it: exhausted when `since` says, its search count,
/// what was tried (a release still in flight counts too: it is never added twice), and the title facts they read.
fn row(tracked: &Tracked) -> Value {
    let item = &tracked.item;
    let at = json!([tracked.since, 0, "hearth"]);
    let mut title = serde_json::Map::new();
    for (key, value) in [
        ("airDate", item.air_date.map(Value::from)),
        ("originalLanguage", item.original_language.clone().map(Value::from)),
        ("preferredLanguage", item.preferred_language.clone().map(Value::from)),
    ] {
        if let Some(value) = value {
            title.insert(key.into(), value);
        }
    }
    let tried: Vec<&String> = item.tried.iter().chain(&item.in_flight).collect();
    let values = json!({
        "queuedAt": {"value": {"int": item.queued_at.unwrap_or(tracked.since)}, "at": at},
        "exhausted": {"value": {"bool": true}, "at": at},
        "retries": {"value": {"int": tracked.retries}, "at": at},
        "tried": {"value": {"strings": tried}, "at": at},
        "title": {"value": {"string": Value::Object(title).to_string()}, "at": at},
    });
    json!({"kind": "set", "schema": 2, "name": format!("download:{}", item.key), "values": values})
}

/// One den-core op through its JSON contract, the one both clients call.
fn core(request: Value) -> Option<Value> {
    let mut answer: Value = serde_json::from_str(&den_sync::evaluate(&request.to_string())).ok()?;
    match answer.get_mut("ok") {
        Some(ok) => Some(ok.take()),
        None => {
            eprintln!("wanted: den-core refused {}: {}", request["op"], answer["error"]);
            None
        }
    }
}

/// Whether Scout's answer settles anything, as the clients read it: every source answered, and the list is not one
/// held through an outage.
fn complete(answer: &Value) -> bool {
    let den = &answer["den"];
    if !answer["streams"].is_array() {
        return false;
    }
    if matches!(den["answerKind"].as_str(), Some("partial" | "unknown" | "stale"))
        || den["degraded"] == "stale_list"
    {
        return false;
    }
    let settled = ["answered", "skipped_misconfigured", "quarantined"];
    den["coverage"]["sources"].as_array().is_none_or(|sources| {
        sources.iter().all(|s| s["outcome"].as_str().is_some_and(|o| settled.contains(&o)))
    })
}

/// What makes a release this release across resolves (the clients' `releaseIdentity`): an info-hash as a whole path
/// segment, then the release's file name, then the URL.
fn identity(url: &str, filename: Option<&str>) -> String {
    let path = url.split_once("://").map_or(url, |(_, rest)| rest.find('/').map_or("", |at| &rest[at..]));
    let path = path.split(['?', '#']).next().unwrap_or_default();
    if let Some(hash) = path.split('/').find(|s| s.len() == 40 && s.bytes().all(|b| b.is_ascii_hexdigit())) {
        return hash.to_ascii_lowercase();
    }
    match filename.filter(|f| !f.is_empty()) {
        Some(name) => name.to_lowercase(),
        None => url.to_owned(),
    }
}

/// The release a complete answer starts, or None when there is nothing to start or nothing is settled.
///
/// Which release, and whether any, is den-core's alone: Hearth hands it every release Scout listed, as the clients
/// hand them (`{...attributes, identity}`), with the row the item stands for, and starts what it names. That call is
/// `download_next`, the rule a client moves a download on with, so the releases tried (and any still in flight) are
/// never started again and a dead swarm never is. When den-core's time-to-watchable pick lands (candidates, the
/// in-flight set, stage times, `now`), it replaces this one call; the item already carries what it takes.
fn decide(answer: Option<&Value>, row: &Value) -> Option<(Value, String, usize)> {
    let answer = answer.filter(|a| complete(a))?;
    let mut listed: Vec<(Value, String)> = Vec::new();
    let mut releases: Vec<Value> = Vec::new();
    for stream in answer["streams"].as_array()? {
        let Some(url) = stream["url"].as_str().filter(|u| ticket_path(u).is_some()) else { continue };
        let identity = identity(url, stream["behaviorHints"]["filename"].as_str());
        let mut facts = stream["attributes"].as_object().cloned().unwrap_or_default();
        facts.insert("identity".into(), json!(identity));
        releases.push(Value::Object(facts));
        listed.push((stream.clone(), identity));
    }
    let resolution = if releases.is_empty() { "none" } else { "streams" };
    let next = core(json!({
        "op": "download_next", "row": row, "releases": releases, "resolution": resolution, "complete": true,
    }))?;
    if next["decision"] != "next" {
        return None;
    }
    let candidates = next["candidates"].as_u64().unwrap_or(0) as usize;
    let (stream, identity) = listed.into_iter().nth(next["index"].as_u64()? as usize)?;
    Some((stream, identity, candidates))
}

/// Each item's state, as both routes answer it.
fn statuses(install: &Install, now: i64) -> Value {
    let items: Vec<Value> = install
        .items
        .iter()
        .map(|tracked| match &tracked.started {
            Some(started) => json!({
                "key": tracked.item.key,
                "state": "started",
                "startedAt": started.at,
                "identity": started.identity,
                "candidates": started.candidates,
                "retries": tracked.retries,
                "stream": started.stream,
            }),
            None => {
                let next = retry(tracked, now).map(|(_, at, _)| at.max(tracked.unanswered_until));
                json!({"key": tracked.item.key, "state": "searching", "retries": tracked.retries, "nextAt": next})
            }
        })
        .collect();
    json!({ "items": items })
}

fn load(file: &PathBuf) -> HashMap<String, Install> {
    match std::fs::read(file) {
        Ok(bytes) => serde_json::from_slice(&bytes).unwrap_or_else(|e| {
            eprintln!("wanted: {} is unreadable ({e}); starting with no lists", file.display());
            HashMap::new()
        }),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => HashMap::new(),
        Err(e) => {
            eprintln!("wanted: couldn't read {} ({e}); starting with no lists", file.display());
            HashMap::new()
        }
    }
}

/// Write `bytes` to `file` whole or not at all, readable by this user alone: it holds sealed config segments.
fn write_private(file: &PathBuf, bytes: &[u8]) -> std::io::Result<()> {
    use std::io::Write;
    use std::os::unix::fs::OpenOptionsExt;
    let tmp = file.with_extension("json.tmp");
    let mut out =
        std::fs::OpenOptions::new().write(true).create(true).truncate(true).mode(0o600).open(&tmp)?;
    out.write_all(bytes)?;
    out.sync_all()?;
    std::fs::rename(&tmp, file)
}

pub fn now_ms() -> i64 {
    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map_or(0, |d| d.as_millis() as i64)
}

/// One pass of the scheduler: the due searches, then the lists written down if they changed.
pub async fn tick(hearth: &Hearth, now: i64) {
    let wanted = &hearth.wanted;
    for job in wanted.due(now) {
        let outcome = wanted.search(hearth, &job).await;
        wanted.settle(&job, outcome, now);
    }
    wanted.flush(now, false);
}

/// Search on every `TICK` for as long as the process runs.
pub fn start(hearth: Arc<Hearth>) {
    tokio::spawn(async move {
        let mut every = tokio::time::interval(TICK);
        every.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            every.tick().await;
            tick(&hearth, now_ms()).await;
        }
    });
}

fn answer(status: StatusCode, value: &Value) -> Response<Body> {
    let mut resp = den_http::json(status, value, &[("cache-control", "no-store")]);
    den_http::cors_stamp(resp.headers_mut(), "");
    resp
}

/// Serve `/den/<config>/wanted`: `POST {"items": [...]}` replaces the install's list, `GET` reads it; both answer
/// `{"items": [{key, state: searching|started, …}]}`. The config is opened and admitted as every resource's is.
pub async fn handle<B>(hearth: &Arc<Hearth>, config: &str, req: Request<B>) -> Response<Body>
where
    B: hyper::body::Body<Data = Bytes> + Send + 'static,
    B::Error: Into<Box<dyn std::error::Error + Send + Sync>>,
{
    if req.method() == Method::OPTIONS {
        let mut resp = den_http::preflight();
        let headers = resp.headers_mut();
        headers.insert(
            "access-control-allow-methods",
            hyper::header::HeaderValue::from_static("GET, POST, OPTIONS"),
        );
        headers
            .insert("access-control-allow-headers", hyper::header::HeaderValue::from_static("content-type"));
        return resp;
    }
    let install = match hearth.den.open(config) {
        Ok(install) if install.members.contains(&"scout") => install,
        _ => return answer(StatusCode::BAD_REQUEST, &json!({"error": "bad_config"})),
    };
    let key = install.iid.unwrap_or_else(|| config.to_owned());
    let now = now_ms();
    match *req.method() {
        Method::GET | Method::HEAD => answer(StatusCode::OK, &hearth.wanted.read(&key, now)),
        Method::POST => {
            let read = tokio::time::timeout(
                Duration::from_secs(10),
                Limited::new(req.into_body(), MAX_BODY).collect(),
            );
            let Ok(Ok(body)) = read.await else {
                return answer(StatusCode::PAYLOAD_TOO_LARGE, &json!({"error": "body_too_large"}));
            };
            #[derive(Deserialize)]
            struct List {
                items: Vec<Item>,
            }
            let Ok(list) = serde_json::from_slice::<List>(&body.to_bytes()) else {
                return answer(StatusCode::BAD_REQUEST, &json!({"error": "bad_list"}));
            };
            if list.items.len() > MAX_ITEMS {
                return answer(
                    StatusCode::PAYLOAD_TOO_LARGE,
                    &json!({"error": "too_many_items", "max": MAX_ITEMS}),
                );
            }
            if !list.items.iter().all(Item::valid) {
                return answer(StatusCode::BAD_REQUEST, &json!({"error": "bad_item"}));
            }
            match hearth.wanted.replace(&key, config, list.items, now) {
                Ok(value) => answer(StatusCode::OK, &value),
                Err((status, error)) => answer(status, &json!({ "error": error })),
            }
        }
        _ => {
            let mut resp = answer(StatusCode::METHOD_NOT_ALLOWED, &json!({"error": "method_not_allowed"}));
            resp.headers_mut().insert("allow", hyper::header::HeaderValue::from_static("GET, POST, OPTIONS"));
            resp
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_lists_outlive_a_restart_in_a_file_only_this_user_reads() {
        use std::os::unix::fs::PermissionsExt;
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("hearth-wanted.json");
        let item = Item {
            key: "tv:70742:2:3".into(),
            imdb: "tt6189176".into(),
            season: Some(2),
            episode: Some(3),
            air_date: None,
            tried: vec!["a.mkv".into()],
            retries: 1,
            since: 1_000,
            queued_at: Some(900),
            in_flight: Vec::new(),
            original_language: Some("sv".into()),
            preferred_language: None,
        };
        let before = Wanted::new(Box::new(InProcess), Some(file.clone()));
        before.replace("iid", "SEALED", vec![item.clone()], 2_000).unwrap();
        // Written at once: a list change is not left to the next 15-minute flush.
        assert_eq!(std::fs::metadata(&file).unwrap().permissions().mode() & 0o777, 0o600);

        let after = Wanted::new(Box::new(InProcess), Some(file.clone()));
        let read = after.read("iid", 3_000);
        assert_eq!(read["items"][0]["key"], "tv:70742:2:3");
        assert_eq!(read["items"][0]["retries"], 1);
        assert_eq!(after.due(3_000 + 2 * EXPIRY_MS).len(), 0, "an expired install came back");

        // An empty list clears the file's copy too.
        after.replace("iid", "SEALED", Vec::new(), 4_000).unwrap();
        assert_eq!(Wanted::new(Box::new(InProcess), Some(file)).read("iid", 5_000), json!({"items": []}));
    }

    #[test]
    fn a_ticket_is_read_from_the_url_scout_listed() {
        assert_eq!(ticket_path("http://node.test/den/p/abc-1_2"), Some("/p/abc-1_2"));
        assert_eq!(ticket_path("/den/p/abc"), Some("/p/abc"));
        assert_eq!(ticket_path("/p/abc"), Some("/p/abc"));
        assert_eq!(ticket_path("https://node.test/hearth/den/p/abc"), Some("/p/abc"));
        for not in ["http://node.test/den/play/abc", "http://node.test/den/p/", "/p/a/b", "/p/a?x=1"] {
            assert_eq!(ticket_path(not), None, "{not}");
        }
    }

    #[test]
    fn identity_is_the_hash_then_the_file_name() {
        let hash = "0123456789abcdef0123456789ABCDEF01234567";
        assert_eq!(identity(&format!("http://x/{hash}/1"), Some("A.mkv")), hash.to_ascii_lowercase());
        assert_eq!(identity("http://x/den/p/t", Some("Some.Show.S02E03.mkv")), "some.show.s02e03.mkv");
        assert_eq!(identity("http://x/den/p/t", None), "http://x/den/p/t");
    }
}
