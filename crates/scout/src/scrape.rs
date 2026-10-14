//! Port of `internal/scout/scrape.go`: the indexer scrapers (Stremio-addon `/stream` endpoints), the
//! shared parse, scrape quorum and the per-provider answer cache. `Doer`, `Scraper` and `ScrapeQuery` are
//! FROZEN (see PORTING.md): stores, probes and the handler all take them.
//!
//! Indexer scrapers (ported from src/scrape/*). One shared Stremio-protocol client; fan-out with a
//! per-indexer timeout, gather-what-responded, dedupe by infohash.

use std::collections::{HashMap, HashSet};
use std::error::Error;
use std::fmt;
use std::future::Future;
use std::hash::{BuildHasher, Hasher};
use std::pin::Pin;
use std::sync::{Arc, LazyLock, Mutex, MutexGuard, PoisonError};
use std::task::Poll;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use reqwest::header::{HeaderValue, ACCEPT, USER_AGENT};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

use crate::cache::Cache;
use crate::config::{Config, Indexer, CONFIG_PATH_INDEXERS};
use crate::coverage::{
    SourceReport, OUTCOME_ANSWERED, OUTCOME_MISCONFIGURED, OUTCOME_QUARANTINED, OUTCOME_REFUSED, OUTCOME_TIMEOUT,
    OUTCOME_UNREACHABLE,
};
use crate::decisionlog::{decision_line, scrape_identity};
use crate::hostlimit::{new_host_limiter, wait, HostLimiter};
use crate::indexerconfig::{indexer_base_with_config, mint_indexer_configs_enabled, read_capped, UnaskableScraper};
use crate::label::MIB;
use crate::loglimit::{emit, log_limited};
use crate::probe::Probe;
use crate::rank::{re2, RawStream, GIB};
use crate::sources::{new_source_client, source_host, ERR_NOT_PUBLIC, OWN_SOURCES, SOURCE_LIMITER};
use crate::upstreampause::pause_on_refusal;
use crate::util::{fold_json_keys, go_to_lower, key_hash, Shape, LEAF};

/// What a `Doer` can fail with. Go returns `error`; a mock in a test returns whichever it likes.
pub type DoError = Box<dyn std::error::Error + Send + Sync>;

/// FROZEN. Go `doer`: the one HTTP seam, `Do(*http.Request) (*http.Response, error)`. A production impl
/// wraps a `reqwest::Client`; a test builds responses from `http::Response` (reqwest converts them), the
/// way the Go tests use `mockDoer`.
#[async_trait]
pub trait Doer: Send + Sync {
    async fn do_request(&self, req: reqwest::Request) -> Result<reqwest::Response, DoError>;
}

/// FROZEN. Go `scrapeQuery`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ScrapeQuery {
    /// "movie" | "series"
    pub kind: String,
    pub imdb: String,
    pub season: i64,
    pub episode: i64,
    pub has_ep: bool,
    /// The request's X-Request-Id, carried through for the per-indexer decision line. Not part of any
    /// cache key.
    pub rid: String,
}

/// FROZEN. Go `scraper`.
#[async_trait]
pub trait Scraper: Send + Sync {
    fn id(&self) -> Indexer;
    async fn scrape(&self, q: &ScrapeQuery) -> Result<Vec<RawStream>, DoError>;

    /// Go's `answerKey`: what `scrapeAllCached` keys this scraper's cached answer by. `None` for a scraper
    /// whose answers are not cached.
    fn answer_key(&self) -> Option<String> {
        None
    }
}

/// Go `maxScrapeBytes`: the cap on an addon response body (a stream list is far smaller).
pub(crate) const MAX_SCRAPE_BYTES: usize = 2 << 20; // 2 MiB

/// Go `scrapeUserAgent` is a browser-like UA: Torrentio & co. 403 the default Go-http-client signature.
pub(crate) const SCRAPE_USER_AGENT: &str =
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36";

/// Locks a module-level map. A poisoned lock still holds a valid map (every edit under it finishes before anything
/// can panic), so it is used as it is.
fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(PoisonError::into_inner)
}

// --- errors ---

/// Go `httpStatusError` is an indexer answering with a status other than 200, kept typed so coverage can tell a
/// refusal from an outage.
#[derive(Debug)]
pub(crate) struct HttpStatusError {
    pub(crate) name: String,
    pub(crate) code: u16,
}

impl fmt::Display for HttpStatusError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} http {}", self.name, self.code)
    }
}

impl Error for HttpStatusError {}

/// Go `unaskableScraper`'s error: an indexer that cannot be addressed (it needs a per-install config segment and
/// none was supplied or could be minted) fails without making a request. Go tells such a scraper apart by its
/// type; the `Scraper` trait has no such hook, so the failure itself says what it is, and `classify_outcome` reads
/// it back.
///
/// `transient` separates "not configured" from "the mint failed just now". Only the first is excluded from the
/// empty-result quorum; a transient failure means we genuinely do not know what this indexer would have said,
/// which is exactly the state the quorum exists to detect.
#[derive(Debug)]
pub(crate) struct UnaskableError {
    pub(crate) indexer: String,
    pub(crate) transient: bool,
}

impl fmt::Display for UnaskableError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: no config URL, not asked", self.indexer)
    }
}

impl Error for UnaskableError {}

/// Go `context.DeadlineExceeded`: a scrape did not answer inside its budget.
#[derive(Debug)]
pub(crate) struct DeadlineExceeded;

impl fmt::Display for DeadlineExceeded {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("context deadline exceeded")
    }
}

impl Error for DeadlineExceeded {}

/// Go `classifyScrape` (coverage.go), less its type switch on the scraper: what one scrape's error means for
/// coverage. `None` is a scrape that answered. The unaskable case is read from the error, which
/// `UnaskableScraper` and the minting scraper make a `UnaskableError`.
pub(crate) fn classify_outcome(err: Option<&(dyn Error + 'static)>) -> &'static str {
    let Some(err) = err else { return OUTCOME_ANSWERED };
    let chain = || std::iter::successors(Some(err), |&e| e.source());
    if let Some(u) = chain().find_map(|e| e.downcast_ref::<UnaskableError>()) {
        return if u.transient { OUTCOME_UNREACHABLE } else { OUTCOME_MISCONFIGURED };
    }
    let timed_out = |e: &(dyn Error + 'static)| {
        if e.is::<DeadlineExceeded>() {
            return true;
        }
        if let Some(r) = e.downcast_ref::<reqwest::Error>() {
            return r.is_timeout();
        }
        if let Some(io) = e.downcast_ref::<std::io::Error>() {
            return io.kind() == std::io::ErrorKind::TimedOut;
        }
        let text = e.to_string();
        text.contains("context deadline exceeded")
            || text.contains("deadline has elapsed")
            || text.contains("queued past the scrape budget")
    };
    if chain().any(timed_out) {
        return OUTCOME_TIMEOUT;
    }
    if chain().any(|e| e.to_string().contains(ERR_NOT_PUBLIC)) {
        return OUTCOME_REFUSED;
    }
    if let Some(status) = chain().find_map(|e| e.downcast_ref::<HttpStatusError>()) {
        match status.code {
            408 => return OUTCOME_TIMEOUT,
            400..=499 => return OUTCOME_REFUSED,
            _ => {}
        }
    }
    OUTCOME_UNREACHABLE
}

/// Go `excusedFromQuorum` (coverage.go): whether a source's failure to answer leaves the list complete, only
/// when it can never be asked, so waiting on it would make completeness unreachable.
fn excused_from_quorum(outcome: &str) -> bool {
    outcome == OUTCOME_MISCONFIGURED || outcome == OUTCOME_QUARANTINED
}

// --- shared parse ---

/// Go `sizeRe`. RE2's `\s` is `[\t\n\f\r ]`, spelled out because the regex crate's ASCII `\s` adds the vertical tab.
static SIZE_RE: LazyLock<regex::Regex> =
    LazyLock::new(|| re2(r"(?i)(?:💾[\t\n\f\r ]*)?([0-9.]+)[\t\n\f\r ]*(gib|gb|mib|mb)\b"));
/// Go `seedEmoji`.
static SEED_EMOJI: LazyLock<regex::Regex> = LazyLock::new(|| re2(r"(?:👤|👥)[\t\n\f\r ]*(\d+)"));
/// Go `seedWord`. Go's `(?i)` folds with Unicode's simple case folding, which gives "s" a second spelling, U+017F
/// (long s); the regex crate's ASCII-only fold does not, so it is written out.
static SEED_WORD: LazyLock<regex::Regex> =
    LazyLock::new(|| re2(r"(?i)(?:s|ſ)eed(?:er(?:s|ſ))?[:\t\n\f\r ]+(\d+)"));
/// Go `titleToken`.
static TITLE_TOKEN: LazyLock<regex::Regex> = LazyLock::new(|| {
    re2(r"(?i)\b(2160p|1080p|720p|480p|remux|bluray|web[ .\-_]?dl|web[ .\-_]?rip|hdtv|x264|x265|hevc)\b")
});

/// Go `parseSize`: the size in bytes of a release described as "15.2 GB" or "700 MB", None when it says none.
pub(crate) fn parse_size(s: &str) -> Option<i64> {
    let m = SIZE_RE.captures(s)?;
    let v: f64 = m[1].parse().ok().filter(|v| *v > 0.0)?;
    let unit = m[2].to_ascii_lowercase();
    let bytes = if unit.starts_with('g') { v * GIB as f64 } else { v * MIB as f64 };
    // Go's conversion of an out-of-range float is implementation-defined; this saturates.
    Some((bytes + 0.5) as i64)
}

/// Go `parseSeeders`: the seeder count an indexer's description names, None when it names none.
pub(crate) fn parse_seeders(s: &str) -> Option<i64> {
    let m = SEED_EMOJI.captures(s).or_else(|| SEED_WORD.captures(s))?;
    m[1].parse().ok()
}

/// Go `normalizeHash`: a 40- or 32-character lower-case alphanumeric hash, trimmed and lower-cased.
fn normalize_hash(h: &str) -> Option<String> {
    let h = go_to_lower(h.trim());
    let well_formed = (h.len() == 40 || h.len() == 32) && h.bytes().all(|b| b.is_ascii_lowercase() || b.is_ascii_digit());
    well_formed.then_some(h)
}

/// Go `url.Parse(link)` then `u.Path`, as far as a hash search needs it: the decoded path, or `None` where Go
/// refuses the link, where the link is opaque (`mailto:x`), or where it has no path. The host is not validated.
fn go_url_path(link: &str) -> Option<Vec<u8>> {
    if link.bytes().any(|b| b < 0x20 || b == 0x7f) {
        return None;
    }
    let (rest, fragment) = link.split_once('#').unwrap_or((link, ""));
    percent_decode(fragment)?;
    // A scheme is letters, digits, `+`, `-` and `.`, opened by a letter and closed by `:`.
    let scheme_end = rest.find(':').filter(|&i| {
        let s = &rest[..i];
        s.starts_with(|c: char| c.is_ascii_alphabetic())
            && s.chars().all(|c| c.is_ascii_alphanumeric() || matches!(c, '+' | '-' | '.'))
    });
    if rest.starts_with(':') {
        return None; // missing protocol scheme
    }
    let has_scheme = scheme_end.is_some();
    let rest = scheme_end.map_or(rest, |i| &rest[i + 1..]);
    let rest = rest.split_once('?').map_or(rest, |(path, _)| path);
    if !rest.starts_with('/') {
        if has_scheme {
            return None; // opaque
        }
        // A relative path whose first segment holds a colon would have read as a scheme.
        if rest.split('/').next().is_some_and(|seg| seg.contains(':')) {
            return None;
        }
    }
    let path = match rest.strip_prefix("//") {
        Some(after) if has_scheme || !rest.starts_with("///") => after.find('/').map_or("", |i| &after[i..]),
        _ => rest,
    };
    percent_decode(path)
}

/// Go's `unescape` in path mode: `%XX` to its byte, `None` for a `%` not followed by two hex digits.
fn percent_decode(s: &str) -> Option<Vec<u8>> {
    let b = s.as_bytes();
    let mut out = Vec::with_capacity(b.len());
    let mut i = 0;
    while i < b.len() {
        if b[i] == b'%' {
            let hex = b.get(i + 1..i + 3)?;
            if !hex.iter().all(u8::is_ascii_hexdigit) {
                return None;
            }
            out.push(u8::from_str_radix(std::str::from_utf8(hex).ok()?, 16).ok()?);
            i += 3;
        } else {
            out.push(b[i]);
            i += 1;
        }
    }
    Some(out)
}

/// Go `hashInPath` finds the infohash in a debrid playback link. An addon configured with a debrid account
/// answers with links to its own resolver instead of an `infoHash` — MediaFusion's
/// `/streaming_provider/<secret>/playback/<provider>/<hash>`, Comet's `/<config>/playback/<hash>/…`,
/// Torrentio's `/resolve/<service>/<key>/<hash>/…` — and the hash is all scout needs to check its own
/// debrid. Only a whole path segment of 40 hex characters counts, and only in the path: the query is where a
/// link would carry a key.
fn hash_in_path(link: &str) -> Option<String> {
    let path = go_url_path(link)?;
    path.split(|&b| b == b'/')
        .find(|seg| seg.len() == 40 && seg.iter().all(u8::is_ascii_hexdigit))
        .map(|seg| String::from_utf8_lossy(&seg.to_ascii_lowercase()).into_owned())
}

/// Go `firstMeaningfulLine`: the first line that looks like a release name, else the first line.
fn first_meaningful_line(text: &str) -> String {
    let lines: Vec<&str> = text.split('\n').map(str::trim).filter(|l| !l.is_empty()).collect();
    if let Some(l) = lines.iter().find(|l| TITLE_TOKEN.is_match(l)) {
        return (*l).to_owned();
    }
    lines.first().map_or_else(String::new, |l| (*l).to_owned())
}

/// Go's lossy decoding of a JSON string: every byte that is not part of a valid UTF-8 sequence becomes U+FFFD on its
/// own, where `String::from_utf8_lossy` folds a broken sequence into one. Outside a string such a byte is a syntax
/// error in Go, and U+FFFD is one here.
fn go_lossy_utf8(mut b: &[u8]) -> String {
    let mut out = String::with_capacity(b.len());
    while !b.is_empty() {
        match std::str::from_utf8(b) {
            Ok(s) => {
                out.push_str(s);
                break;
            }
            Err(e) => {
                let (valid, rest) = b.split_at(e.valid_up_to());
                out.push_str(std::str::from_utf8(valid).unwrap_or_default());
                out.push('\u{fffd}');
                b = &rest[1..];
            }
        }
    }
    out
}

/// The fields of Go's `wireStream` that `json.Unmarshal` reads, in their exact spelling.
static BEHAVIOR_SHAPE: Shape = Shape::Object(&[("filename", &LEAF), ("videoSize", &LEAF)]);
static STREAM_SHAPE: Shape = Shape::Object(&[
    ("name", &LEAF),
    ("title", &LEAF),
    ("description", &LEAF),
    ("infoHash", &LEAF),
    ("url", &LEAF),
    ("fileIdx", &LEAF),
    ("behaviorHints", &BEHAVIOR_SHAPE),
]);
static STREAMS_SHAPE: Shape = Shape::Array(&STREAM_SHAPE);
static RESPONSE_SHAPE: Shape = Shape::Object(&[("streams", &STREAMS_SHAPE)]);

/// Go `wireStream`.
struct WireStream {
    title: String,
    description: String,
    info_hash: String,
    url: String,
    file_idx: Option<i64>,
    /// `BehaviorHints.Filename`, and `BehaviorHints.VideoSize`; `None` when the object is absent.
    hints: Option<(String, Option<i64>)>,
}

/// A JSON string field as Go's decoder reads it into a `string`: absent and `null` leave it empty, anything but a
/// string is a type error.
fn wire_string(o: &Map<String, Value>, key: &str) -> Option<String> {
    match o.get(key) {
        None | Some(Value::Null) => Some(String::new()),
        Some(Value::String(s)) => Some(s.clone()),
        Some(_) => None,
    }
}

/// A JSON number field as Go's decoder reads it into a `*int`: `null` is nil, and a number with a fraction or an
/// exponent, or one that does not fit, is a type error.
fn wire_int(o: &Map<String, Value>, key: &str) -> Option<Option<i64>> {
    match o.get(key) {
        None | Some(Value::Null) => Some(None),
        Some(Value::Number(n)) => n.as_i64().map(Some),
        Some(_) => None,
    }
}

/// `json.Unmarshal(raw, &wireStream)`: `None` where Go reports an error, which skips the element.
fn wire_stream(v: &Value) -> Option<WireStream> {
    // A `null` element decodes to the zero value without error, but a stream with neither a hash nor a link is
    // skipped all the same.
    let Value::Object(o) = v else { return None };
    wire_string(o, "name")?;
    let hints = match o.get("behaviorHints") {
        None | Some(Value::Null) => None,
        Some(Value::Object(h)) => Some((wire_string(h, "filename")?, wire_int(h, "videoSize")?)),
        Some(_) => return None,
    };
    Some(WireStream {
        title: wire_string(o, "title")?,
        description: wire_string(o, "description")?,
        info_hash: wire_string(o, "infoHash")?,
        url: wire_string(o, "url")?,
        file_idx: wire_int(o, "fileIdx")?,
        hints,
    })
}

/// Go `parseStremioStreams`: the streams in a Stremio `/stream` response, `source` naming the indexer
/// they came from.
pub(crate) fn parse_stremio_streams(body: &[u8], source: &str) -> Vec<RawStream> {
    let Ok(mut doc) = serde_json::from_str::<Value>(&go_lossy_utf8(body)) else { return Vec::new() };
    fold_json_keys(&mut doc, &RESPONSE_SHAPE);
    let streams = match &doc {
        Value::Object(o) => match o.get("streams") {
            Some(Value::Array(a)) => a,
            _ => return Vec::new(),
        },
        _ => return Vec::new(),
    };
    let mut out = Vec::new();
    for raw in streams {
        let Some(s) = wire_stream(raw) else { continue }; // tolerate a non-object element
        let mut hash = normalize_hash(&s.info_hash);
        if hash.is_none() && s.info_hash.is_empty() {
            hash = hash_in_path(&s.url);
        }
        let Some(hash) = hash else { continue };
        // Title/metadata come from the release fields only — NOT s.Name, which is the indexer's label
        // ("Torrentio", "Torrentio\n1080p"): feeding it in made the title fall back to "Torrentio" when
        // no line looked like a release name.
        let text = [s.title.as_str(), s.description.as_str()].into_iter().filter(|t| !t.is_empty()).collect::<Vec<_>>().join("\n");
        let mut title = s.hints.as_ref().map_or_else(String::new, |(filename, _)| filename.trim().to_owned());
        if title.is_empty() {
            title = first_meaningful_line(&text);
        }
        if title.is_empty() {
            title = hash.clone();
        }
        let size = match s.hints.as_ref().and_then(|(_, video_size)| *video_size) {
            Some(v) if v > 0 => Some(v),
            _ => parse_size(&text),
        };
        out.push(RawStream {
            info_hash: hash,
            file_idx: s.file_idx.filter(|&i| i >= 0),
            title,
            size_bytes: size,
            seeders: parse_seeders(&text),
            source: source.to_owned(),
            ..Default::default()
        });
    }
    out
}

// --- Stremio addon scraper ---

/// Go `stremioScraper`: one Stremio-addon indexer, reached over `base_url`, paced by `indexer_limiter`. A household
/// source is a `SourceScraper`, which adds a label and a limiter of its own to this.
pub(crate) struct StremioScraper {
    pub(crate) indexer: Indexer,
    pub(crate) base_url: String,
    pub(crate) client: Arc<dyn Doer>,
}

/// A shed request is not an answer.
///
/// Opening a season asks for every episode at once, so an indexer sees a burst and sheds part of it —
/// torrentio answers 502 to some of them. Treated as a result, that turns into "no source found for this
/// episode" for a release the same indexer serves 50 of a second later. These statuses are the indexer
/// declining to answer right now, so the request is made again rather than reported as an outcome.
///
/// Not a 429. That is the indexer saying it is rate limiting, and asking again inside the same second only spends
/// more of the limit; the transport pauses the host instead, for as long as it asked (upstreampause.go). The same
/// goes for a shed request carrying a Retry-After: the transport has paused the host, so a retry would be answered
/// by that pause.
fn retryable_scrape_status(status: u16) -> bool {
    status == 408 || status >= 500
}

/// Short and jittered: the whole scrape is under an 8 s budget shared with three other indexers, so this
/// buys a second chance without spending someone else's timeout. Jitter because a burst that is shed
/// together would otherwise retry together and be shed together again.
const SCRAPE_RETRY_BACKOFF: [Duration; 2] = [Duration::from_millis(250), Duration::from_millis(700)];

/// Sized against the largest batch the client actually issues, not picked round.
///
/// Den's poster grid fires up to 8 availability checks at once, so a burst of 6 made every poster screen
/// pay a queue for no benefit — the limiter is not there to tax the normal case. 10 covers that batch plus
/// a focus dwell and a play.
///
/// The sustained rate is a runaway detector, not a demand ceiling. A heavy evening is a few hundred
/// requests an hour; 1/s per host is still an order of magnitude above that, while a loop that re-asks
/// forever — the documented failure here — meets a wall it can be seen hitting. Per host, so a slow
/// indexer cannot delay a healthy one.
/// Burst sized to a household's largest legitimate burst — opening a season is one request per episode,
/// and 10 turned a 24-episode show into fourteen seconds of queueing against an eight-second budget. The
/// sustained rate is what actually protects the indexer; the burst only decides whether normal use waits.
static INDEXER_LIMITER: LazyLock<HostLimiter> = LazyLock::new(|| new_host_limiter(Duration::from_secs(1), 30));

/// Go `indexerLimiter`.
pub(crate) fn indexer_limiter() -> &'static HostLimiter {
    &INDEXER_LIMITER
}

/// Go's `rand.Int63n(max)` for the retry jitter: a random duration in `[0, max)`. The standard library's hasher keys
/// are random per process, which is all a jitter needs.
fn jitter(max: Duration) -> Duration {
    let max = u64::try_from(max.as_nanos()).unwrap_or(u64::MAX);
    if max == 0 {
        return Duration::ZERO;
    }
    Duration::from_nanos(std::collections::hash_map::RandomState::new().build_hasher().finish() % max)
}

/// The host a limiter paces a base URL by (Go `url.URL.Host`: the host and, when written, the port).
fn limiter_host(base: &str) -> Option<String> {
    let u = reqwest::Url::parse(base.trim_end_matches('/')).ok()?;
    let host = u.host_str()?;
    Some(match u.port() {
        Some(p) => format!("{host}:{p}"),
        None => host.to_owned(),
    })
}

impl StremioScraper {
    /// Go `stremioScraper.name`: how the scraper reads in a log line. `label` names a household source's host.
    fn name(&self, label: &str) -> String {
        if label.is_empty() {
            self.indexer.0.clone()
        } else {
            format!("{} source {label}", self.indexer.0)
        }
    }

    /// Go `stremioScraper.scrape`: one logical scrape, paced by `limiter` and retried while the indexer sheds it.
    async fn scrape_paced(&self, label: &str, limiter: &HostLimiter, q: &ScrapeQuery) -> Result<Vec<RawStream>, DoError> {
        // One token per logical scrape, not per attempt. Pacing inside the retry loop spent up to three, and
        // spent the extra two exactly when the host was already shedding — so the limiter throttled hardest
        // during the one failure it exists for, and each abandoned wait left its token spent, deepening the
        // debt for the next caller. The jittered backoff below already paces the retries.
        if let Some(host) = limiter_host(&self.base_url) {
            wait(Some(limiter), &host).await;
        }
        // Go checks here that the caller's deadline did not pass while queued, and reports that as its own error
        // rather than asking an indexer a question that cannot succeed. A caller whose deadline passes drops this
        // future, so a scrape that spent its budget queueing never reaches the request below.
        let mut attempt = 0;
        loop {
            let (result, retryable) = self.scrape_once(label, q).await;
            let err = match result {
                Ok(streams) => {
                    if attempt > 0 {
                        log_limited(
                            &format!("indexer-retry:{}", self.indexer.0),
                            &format!("{} indexer answered on retry {attempt}", self.name(label)),
                        );
                    }
                    return Ok(streams);
                }
                Err(err) => err,
            };
            if !retryable || attempt >= SCRAPE_RETRY_BACKOFF.len() {
                return Err(err);
            }
            let delay = SCRAPE_RETRY_BACKOFF[attempt];
            tokio::time::sleep(delay + jitter(delay / 2)).await;
            attempt += 1;
        }
    }

    /// Go `scrapeOnce` performs one request. The second result says whether the failure is worth another attempt.
    pub(crate) async fn scrape_once(&self, label: &str, q: &ScrapeQuery) -> (Result<Vec<RawStream>, DoError>, bool) {
        let strem_id = if q.has_ep { format!("{}:{}:{}", q.imdb, q.season, q.episode) } else { q.imdb.clone() };
        // The id goes in as Stremio clients send it, colons and all (`tt123:2:1`), not percent-encoded. It needs no
        // escaping — an IMDb id and two numbers — and the encoded form is a different URL to a CDN: with torrentio's
        // origin down, Cloudflare answers from `stale-if-error` only for the URL the whole world requests, so every
        // series episode came back 521 while the same `tt24022296:2:1` served five releases to a plain curl.
        let u = format!("{}/stream/{}/{strem_id}.json", self.base_url.trim_end_matches('/'), q.kind);
        let Ok(url) = reqwest::Url::parse(&u) else {
            // a malformed request will be malformed again
            return (Err("malformed indexer url".into()), false);
        };
        let mut req = reqwest::Request::new(reqwest::Method::GET, url);
        req.headers_mut().insert(ACCEPT, HeaderValue::from_static("application/json"));
        // Torrentio (and peers) 403 the default Go-http-client User-Agent as a bot signature — send a
        // browser UA so the scrape isn't rejected. Without this every indexer returns 403 → zero streams.
        req.headers_mut().insert(USER_AGENT, HeaderValue::from_static(SCRAPE_USER_AGENT));
        let start = Instant::now();
        let resp = match self.client.do_request(req).await {
            Ok(resp) => resp,
            Err(err) => {
                // Log the indexer name + reason (never the URL — MediaFusion's carries its encrypted config) so a
                // scrape outage is visible in the server log instead of silently becoming an empty stream list.
                // Once a minute per indexer: an outage fails every scrape, and the line only has to say it is down.
                // outcome is classify_outcome's own vocabulary (unreachable/timeout/refused — coverage.rs), never the
                // error itself: a transport error can carry the full request URL, which for a debrid-backed
                // indexer carries the account token in its query string (see token_leak.rs).
                log_limited(
                    &format!("indexer-unreachable:{}", self.indexer.0),
                    &decision_line(
                        "scrape",
                        classify_outcome(Some(&*err)),
                        "no response",
                        &self.name(label),
                        start.elapsed(),
                        &q.rid,
                        &scrape_identity(q),
                    ),
                );
                // A refused address will be refused again; a retry would only spend the budget.
                let retryable = !chain_mentions(&*err, ERR_NOT_PUBLIC);
                return (Err(err), retryable);
            }
        };
        let status = resp.status().as_u16();
        if status != 200 {
            let status_err = HttpStatusError { name: self.name(label), code: status };
            log_limited(
                &format!("indexer-status:{}", self.indexer.0),
                &decision_line(
                    "scrape",
                    classify_outcome(Some(&status_err)),
                    &format!("http {status}"),
                    &self.name(label),
                    start.elapsed(),
                    &q.rid,
                    &scrape_identity(q),
                ),
            );
            return (Err(Box::new(status_err)), retryable_scrape_status(status));
        }
        // The error is not carried: reqwest's names the request URL, which is a credential for a configured indexer.
        let Some(body) = read_capped(resp, MAX_SCRAPE_BYTES).await else {
            return (Err(format!("{}: reading the response failed", self.name(label)).into()), true);
        };
        (Ok(parse_stremio_streams(&body, &self.indexer.0)), false)
    }
}

/// Go `errors.Is(err, sentinel)` for a reason constant: whether any error in the chain carries that text.
fn chain_mentions(err: &(dyn Error + 'static), text: &str) -> bool {
    std::iter::successors(Some(err), |&e| e.source()).any(|e| e.to_string().contains(text))
}

#[async_trait]
impl Scraper for StremioScraper {
    fn id(&self) -> Indexer {
        self.indexer.clone()
    }

    async fn scrape(&self, q: &ScrapeQuery) -> Result<Vec<RawStream>, DoError> {
        self.scrape_paced("", indexer_limiter(), q).await
    }

    /// Go `stremioScraper.answerKey`: every install that asks this URL about a title gets the same answer. Hashed,
    /// because a MediaFusion URL carries its encrypted config.
    fn answer_key(&self) -> Option<String> {
        Some(key_hash(self.base_url.trim_end_matches('/')))
    }
}

/// A household's own source (sources.rs): a `StremioScraper` reached over the guarded client, named by its host and
/// paced by `SOURCE_LIMITER`, which forgets hosts past a ceiling where `indexer_limiter` keeps one forever.
pub(crate) struct SourceScraper {
    pub(crate) stremio: StremioScraper,
    /// Names the scraper in log lines when its indexer id is shared: a household source's host.
    pub(crate) label: String,
}

impl SourceScraper {
    fn new(base: &str, client: Arc<dyn Doer>) -> SourceScraper {
        SourceScraper {
            stremio: StremioScraper { indexer: Indexer(OWN_SOURCES.to_owned()), base_url: base.to_owned(), client },
            label: source_host(base),
        }
    }
}

#[async_trait]
impl Scraper for SourceScraper {
    fn id(&self) -> Indexer {
        self.stremio.id()
    }

    async fn scrape(&self, q: &ScrapeQuery) -> Result<Vec<RawStream>, DoError> {
        self.stremio.scrape_paced(&self.label, &SOURCE_LIMITER, q).await
    }

    fn answer_key(&self) -> Option<String> {
        self.stremio.answer_key()
    }
}

/// An indexer that needs a per-install config segment, with minting on: the segment is minted from the household's
/// debrid account when the scraper is first asked, then the indexer is asked like any other.
///
/// Go mints inside `makeScrapers`, before the scrape and on its own thread of control. `Deps::make_scrapers` is a
/// plain function here, so the mint happens at the head of the scrape instead (indexerconfig.rs coalesces and caches
/// it either way). A mint that fails leaves the scraper unaskable for this scrape, which is what Go's
/// `unaskableScraper` is: `UnaskableError`, with the same transient/misconfigured split.
pub(crate) struct MintingScraper {
    pub(crate) indexer: Indexer,
    /// Only the debrid accounts are read, to mint from.
    pub(crate) config: Config,
    pub(crate) env_var: &'static str,
    pub(crate) client: Arc<dyn Doer>,
}

#[async_trait]
impl Scraper for MintingScraper {
    fn id(&self) -> Indexer {
        self.indexer.clone()
    }

    async fn scrape(&self, q: &ScrapeQuery) -> Result<Vec<RawStream>, DoError> {
        let (base, transient) = indexer_base_with_config(&self.indexer, &self.config, Some(&*self.client)).await;
        if base.is_empty() {
            // A transient mint failure is an outage, not a misconfiguration — don't tell the operator
            // to configure something that already is. The mint logs its own reason.
            if !transient {
                log_indexer_skip_once(&self.indexer, self.env_var);
            }
            return Err(Box::new(UnaskableError { indexer: self.indexer.0.clone(), transient }));
        }
        StremioScraper { indexer: self.indexer.clone(), base_url: base, client: Arc::clone(&self.client) }.scrape(q).await
    }

    /// The same answer for the same indexer and account, however many times its minted URL has been re-minted. The
    /// account's token is hashed, as everywhere a key is made from one.
    fn answer_key(&self) -> Option<String> {
        let acct = self.config.debrid.first()?;
        Some(key_hash(&format!("mint:{}:{}:{}", self.indexer.0, acct.service.as_str(), acct.token)))
    }
}

// --- fan-out + dedupe ---

/// Go `defaultIndexerURLs`: the base URL each indexer is asked at unless the config overrides it.
pub(crate) const DEFAULT_INDEXER_URLS: [(&str, &str); 4] = [
    ("torrentio", "https://torrentio.strem.fun"),
    ("comet", "https://comet.elfhosted.com"),
    ("mediafusion", "https://mediafusion.elfhosted.com"),
    ("torz", "https://torz.strem.fun"),
];

/// Go `newSourceClient` as the `Doer` household sources are fetched with: the guarded client behind
/// `PauseOnRefusal`, so a source that asks to be left alone is. Built once and shared, since the pause is state.
pub(crate) fn new_source_doer() -> Arc<dyn Doer> {
    Arc::new(pause_on_refusal(new_source_client()))
}

/// Go `makeScrapers`.
pub(crate) fn make_scrapers(
    config: &Config,
    client: Arc<dyn Doer>,
    source_client: Arc<dyn Doer>,
    urls: &HashMap<Indexer, String>,
) -> Vec<Box<dyn Scraper>> {
    let mut out: Vec<Box<dyn Scraper>> = Vec::with_capacity(config.indexers.len() + config.sources.len());
    // Household sources are asked like any indexer, and count in the quorum like one: the household chose
    // them, so a silent one leaves the list incomplete. Fetched only through source_client, which refuses to
    // connect anywhere but the public internet.
    for base in &config.sources {
        out.push(Box::new(SourceScraper::new(base, Arc::clone(&source_client))));
    }
    for id in &config.indexers {
        // An indexer that needs a per-install config segment and hasn't been given one is not asked. It
        // cannot answer usefully, and its failure is silent in the worst way: mediafusion returns 200
        // with an empty list, which counts as a response and votes on whether a release exists. Leaving
        // it out means a genuine "nobody has this" still requires everyone who WAS asked to say so.
        let overridden = urls.get(id).is_some_and(|u| !u.is_empty());
        if let (Some(&(_, env_var)), false) = (CONFIG_PATH_INDEXERS.iter().find(|(name, _)| *name == id.0), overridden) {
            // Mint one from the debrid account we already hold, when the operator has allowed it. An
            // explicit URL always wins, so a token-free config that was pasted in stays in use.
            if mint_indexer_configs_enabled() {
                out.push(Box::new(MintingScraper {
                    indexer: id.clone(),
                    config: Config { debrid: config.debrid.clone(), ..Default::default() },
                    env_var,
                    client: Arc::clone(&client),
                }));
            } else {
                log_indexer_skip_once(id, env_var);
                // Kept in the list as a scraper that cannot answer, rather than dropped from it. Dropping
                // removed it from the quorum too, so "an empty result is authoritative only when EVERY
                // indexer answered" silently became "…every indexer we still bother asking" — and with
                // the other two unconfigured, one torrentio 200-empty was again a confident "this release
                // does not exist". That is the same bug the skip was introduced to fix, one level up.
                out.push(Box::new(UnaskableScraper { indexer: id.clone(), transient: false }));
            }
            continue;
        }
        out.push(Box::new(StremioScraper {
            indexer: id.clone(),
            base_url: base_url_for(id, urls),
            client: Arc::clone(&client),
        }));
    }
    out
}

/// Once per indexer per process: this runs on every request, and the operator needs the line once.
static INDEXER_SKIP_LOGGED: LazyLock<Mutex<HashSet<String>>> = LazyLock::new(|| Mutex::new(HashSet::new()));

/// Go `logIndexerSkipOnce`.
fn log_indexer_skip_once(id: &Indexer, env_var: &str) {
    if !lock(&INDEXER_SKIP_LOGGED).insert(id.0.clone()) {
        return;
    }
    emit(&format!(
        "{} needs a per-install config URL and has none — not querying it. Set {env_var} to the address from that \
         addon's /configure page.",
        id.0
    ));
}

/// Go `baseURLFor`.
fn base_url_for(id: &Indexer, urls: &HashMap<Indexer, String>) -> String {
    if let Some(u) = urls.get(id).filter(|u| !u.is_empty()) {
        return u.clone();
    }
    // Deliberately no torrentio options segment. Asking for `sort=qualitysize|qualityfilter=cam,scr`
    // bought nothing — `rankStreams` drops cam/scr itself and re-sorts by its own quality score — and it
    // cost everything: with torrentio's origin down, Cloudflare answers from `stale-if-error` only for a
    // URL it already holds. The bare path is warm because the whole world requests it; a private options
    // path is warm essentially never, so scout got a 502 for an episode that served 50 releases to a
    // plain curl from the same machine, and reported it as "no source found".
    DEFAULT_INDEXER_URLS.iter().find(|(name, _)| *name == id.0).map_or_else(String::new, |&(_, url)| url.to_owned())
}

/// Go `scrapeAll` runs every scraper concurrently under a per-indexer timeout; drops those that error/time
/// out; then dedupes by infohash. The bool reports whether at least one scraper responded — a false
/// (every indexer failed/timed out) means an empty result is a degraded blip, not a genuine "no
/// torrents", so the caller must not cache it.
/// Returns the releases, whether an EMPTY result may be trusted, and whether every askable indexer
/// answered. The last is not the same question as the second: a partial non-empty list is worth serving
/// and worth caching, just for less time.
pub(crate) async fn scrape_all(
    scrapers: &[Arc<dyn Scraper>],
    q: &ScrapeQuery,
    timeout: Duration,
) -> (Vec<RawStream>, bool, bool) {
    scrape_all_cached(scrapers, q, timeout, None, Duration::ZERO).await
}

// --- the per-provider answer cache ---

// An indexer's answer to one question, kept apart from every list built from it.
//
// Every install that asks torrentio about a title asks the same URL and gets the same answer, and a list ranked
// for a browser rather than the TV, or filtered differently, is built from those same answers again. So each is
// cached on its own, keyed by what was asked: the indexer's URL, hashed because a MediaFusion URL carries its
// encrypted config, and the title. Answers only, never a failure, and for as long as a list stays fresh.

/// Go `answerCacheKey`: `None` for a scraper whose answers are not cached.
fn answer_cache_key(sc: &dyn Scraper, q: &ScrapeQuery) -> Option<String> {
    let key = sc.answer_key()?;
    let id = if q.has_ep { format!("{}:{}:{}", q.imdb, q.season, q.episode) } else { q.imdb.clone() };
    Some(format!("answer:v1:{key}:{}:{id}", q.kind))
}

/// A `RawStream` as Go's `encoding/json` writes it (the struct has no tags, so the keys are the field names), which
/// is what keeps an answer one build wrote readable by the other across a redeploy.
#[derive(Serialize, Deserialize, Default)]
#[serde(rename_all = "PascalCase", default)]
struct CachedStream {
    info_hash: String,
    file_idx: Option<i64>,
    title: String,
    size_bytes: Option<i64>,
    pack_size_bytes: Option<i64>,
    seeders: Option<i64>,
    cached: bool,
    cache_known: bool,
    source: String,
    probe: Option<Probe>,
}

impl From<&RawStream> for CachedStream {
    fn from(s: &RawStream) -> CachedStream {
        CachedStream {
            info_hash: s.info_hash.clone(),
            file_idx: s.file_idx,
            title: s.title.clone(),
            size_bytes: s.size_bytes,
            pack_size_bytes: s.pack_size_bytes,
            seeders: s.seeders,
            cached: s.cached,
            cache_known: s.cache_known,
            source: s.source.clone(),
            probe: s.probe.clone(),
        }
    }
}

impl From<CachedStream> for RawStream {
    fn from(s: CachedStream) -> RawStream {
        RawStream {
            info_hash: s.info_hash,
            file_idx: s.file_idx,
            title: s.title,
            size_bytes: s.size_bytes,
            pack_size_bytes: s.pack_size_bytes,
            seeders: s.seeders,
            cached: s.cached,
            cache_known: s.cache_known,
            source: s.source,
            probe: s.probe,
        }
    }
}

/// Go `keptAnswer`.
pub(crate) fn kept_answer(cache: Option<&dyn Cache>, sc: &dyn Scraper, q: &ScrapeQuery) -> Option<Vec<RawStream>> {
    let key = answer_cache_key(sc, q)?;
    let raw = cache?.get(&key)?;
    // Go's nil slice marshals as `null`, which a build of either language may have written.
    let kept: Option<Vec<CachedStream>> = serde_json::from_str(&raw).ok()?;
    Some(kept.unwrap_or_default().into_iter().map(RawStream::from).collect())
}

/// Go `keepAnswer`.
pub(crate) fn keep_answer(cache: Option<&dyn Cache>, ttl: Duration, sc: &dyn Scraper, q: &ScrapeQuery, r: &[RawStream]) {
    let Some(key) = answer_cache_key(sc, q) else { return };
    let Some(cache) = cache else { return };
    if ttl.is_zero() {
        return;
    }
    let kept: Vec<CachedStream> = r.iter().map(CachedStream::from).collect();
    if let Ok(b) = serde_json::to_string(&kept) {
        cache.put(&key, &b, ttl);
    }
}

/// The error of a finished attempt, shareable between the callers of one in-flight question.
type SharedError = Arc<dyn Error + Send + Sync>;

/// Go `scrapeAttempt`.
#[derive(Clone)]
struct ScrapeAttempt {
    streams: Vec<RawStream>,
    err: Option<SharedError>,
    cached: bool,
    latency_ms: i64,
    observed_at: Option<SystemTime>,
}

impl ScrapeAttempt {
    fn failed(err: SharedError) -> ScrapeAttempt {
        ScrapeAttempt { streams: Vec::new(), err: Some(err), cached: false, latency_ms: 0, observed_at: Some(now_millis()) }
    }
}

/// `time.Now().UTC().Truncate(time.Millisecond)`.
fn now_millis() -> SystemTime {
    let since = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default();
    UNIX_EPOCH + Duration::from_millis(u64::try_from(since.as_millis()).unwrap_or(u64::MAX))
}

fn millis(d: Duration) -> i64 {
    i64::try_from(d.as_millis()).unwrap_or(i64::MAX)
}

/// The request itself, under its own timeout. A timeout drops the scrape, which is how a deadline cancels it.
async fn ask(sc: &dyn Scraper, q: &ScrapeQuery, timeout: Duration) -> ScrapeAttempt {
    let start = Instant::now();
    let answer = tokio::time::timeout(timeout, sc.scrape(q)).await;
    let (streams, err) = match answer {
        Ok(Ok(streams)) => (streams, None),
        Ok(Err(err)) => (Vec::new(), Some(SharedError::from(err))),
        Err(_) => (Vec::new(), Some(Arc::new(DeadlineExceeded) as SharedError)),
    };
    ScrapeAttempt { streams, err, cached: false, latency_ms: millis(start.elapsed()), observed_at: Some(now_millis()) }
}

/// The question in flight for each provider and title: Go's `answerFlights` singleflight group.
///
/// One upstream question per provider identity and title at a time. The settled-answer cache collapses later
/// requests, but without this gate every list variant that missed it concurrently still reached the indexer.
type Flight = tokio::sync::watch::Receiver<Option<ScrapeAttempt>>;
static ANSWER_FLIGHTS: LazyLock<Mutex<HashMap<String, Flight>>> = LazyLock::new(|| Mutex::new(HashMap::new()));

/// Forgets a flight when its task ends, however it ends: a task that panicked must not leave every later caller
/// joining a question nobody is asking.
struct ForgetFlight(String);

impl Drop for ForgetFlight {
    fn drop(&mut self) {
        lock(&ANSWER_FLIGHTS).remove(&self.0);
    }
}

/// Joins the question already in flight under `key`, or starts it.
///
/// The request has its own task and its own timeout: a page leaving stops waiting, but does not cancel useful work
/// for another page. (Go also lets it warm the cache after every caller has left. The cache here is borrowed for the
/// call, so it cannot move into the task; whichever caller is still waiting keeps the answer.)
fn join_or_start(key: &str, sc: &Arc<dyn Scraper>, q: &ScrapeQuery, timeout: Duration) -> Flight {
    let (tx, rx) = {
        let mut flights = lock(&ANSWER_FLIGHTS);
        if let Some(rx) = flights.get(key) {
            return rx.clone();
        }
        let (tx, rx) = tokio::sync::watch::channel(None);
        flights.insert(key.to_owned(), rx.clone());
        (tx, rx)
    };
    // The guard is taken only now, with the map unlocked: it locks the map when it drops.
    let (sc, q, forget) = (Arc::clone(sc), q.clone(), ForgetFlight(key.to_owned()));
    tokio::spawn(async move {
        let attempt = ask(&*sc, &q, timeout).await;
        drop(forget);
        let _ = tx.send(Some(attempt));
    });
    rx
}

/// Go `scrapeOneCached` answers one provider question from the settled cache or the one identical request already in
/// flight.
async fn scrape_one_cached(
    sc: &Arc<dyn Scraper>,
    q: &ScrapeQuery,
    timeout: Duration,
    cache: Option<&dyn Cache>,
    ttl: Duration,
) -> ScrapeAttempt {
    if let Some(streams) = kept_answer(cache, &**sc, q) {
        return ScrapeAttempt { streams, err: None, cached: true, latency_ms: 0, observed_at: None };
    }
    let Some(key) = answer_cache_key(&**sc, q) else {
        let attempt = ask(&**sc, q, timeout).await;
        if attempt.err.is_none() {
            keep_answer(cache, ttl, &**sc, q, &attempt.streams);
        }
        return attempt;
    };
    let waiting = Instant::now();
    let mut flight = join_or_start(&key, sc, q, timeout);
    // Cached answers are decoded for each caller. Preserve that ownership for a shared live answer too: later
    // phases annotate RawStream values with cache and probe facts.
    let shared = match flight.wait_for(Option::is_some).await {
        Ok(done) => done.clone(),
        // The task ended without answering: it panicked.
        Err(_) => None,
    };
    let Some(mut attempt) = shared else {
        return ScrapeAttempt::failed(Arc::new(std::io::Error::other("the scrape ended without an answer")));
    };
    attempt.latency_ms = millis(waiting.elapsed());
    attempt.observed_at = Some(now_millis());
    if attempt.err.is_none() {
        keep_answer(cache, ttl, &**sc, q, &attempt.streams);
    }
    attempt
}

/// `futures::future::join_all`, for the one place a fan-out is needed: polls every future on each wake-up and keeps
/// the answers in order.
async fn join_all<'a, T>(futures: Vec<Pin<Box<dyn Future<Output = T> + Send + 'a>>>) -> Vec<T> {
    let mut pending: Vec<Option<Pin<Box<dyn Future<Output = T> + Send + 'a>>>> = futures.into_iter().map(Some).collect();
    let mut done: Vec<Option<T>> = pending.iter().map(|_| None).collect();
    std::future::poll_fn(|cx| {
        let mut waiting = false;
        for (slot, future) in done.iter_mut().zip(pending.iter_mut()) {
            if let Some(f) = future {
                match f.as_mut().poll(cx) {
                    Poll::Ready(v) => {
                        *slot = Some(v);
                        *future = None;
                    }
                    Poll::Pending => waiting = true,
                }
            }
        }
        if waiting {
            Poll::Pending
        } else {
            Poll::Ready(())
        }
    })
    .await;
    done.into_iter().flatten().collect()
}

/// Go `scrapeAllCached` is scrapeAll answering from each indexer's kept answer where there is one, and keeping
/// each new one for ttl.
pub(crate) async fn scrape_all_cached(
    scrapers: &[Arc<dyn Scraper>],
    q: &ScrapeQuery,
    timeout: Duration,
    cache: Option<&dyn Cache>,
    ttl: Duration,
) -> (Vec<RawStream>, bool, bool) {
    let res = scrape_covered(scrapers, q, timeout, cache, ttl).await;
    (res.seeds, res.any_ok, res.complete)
}

/// Go `scrapeResult` is one pass over every scraper: the deduped releases, whether an empty list may be trusted,
/// whether every askable source answered, and what each source did, in scraper order.
pub(crate) struct ScrapeResult {
    pub(crate) seeds: Vec<RawStream>,
    pub(crate) any_ok: bool,
    pub(crate) complete: bool,
    pub(crate) sources: Vec<SourceReport>,
}

/// Go `scrapeCovered` is scrapeAllCached keeping each source's report.
pub(crate) async fn scrape_covered(
    scrapers: &[Arc<dyn Scraper>],
    q: &ScrapeQuery,
    timeout: Duration,
    cache: Option<&dyn Cache>,
    ttl: Duration,
) -> ScrapeResult {
    let attempts = join_all(
        scrapers
            .iter()
            .map(|sc| Box::pin(scrape_one_cached(sc, q, timeout, cache, ttl)) as Pin<Box<dyn Future<Output = ScrapeAttempt> + Send + '_>>)
            .collect(),
    )
    .await;
    let mut all: Vec<RawStream> = Vec::new();
    let mut reports: Vec<SourceReport> = Vec::with_capacity(scrapers.len());
    for (sc, attempt) in scrapers.iter().zip(attempts) {
        let err = attempt.err.as_deref().map(|e| e as &(dyn Error + 'static));
        let outcome = classify_outcome(err);
        let unasked = err.is_some_and(|e| e.is::<UnaskableError>());
        // The report is the scraper's own id. Go also names a household source by its host (`reportFor`); the
        // `Scraper` trait has no label to read it from, so that is left to a trait method.
        let mut report = SourceReport {
            id: sc.id().0,
            label: String::new(),
            outcome: outcome.to_owned(),
            items: 0,
            latency_ms: None,
            observed_at: None,
            cached: attempt.cached,
        };
        if !unasked {
            report.latency_ms = Some(attempt.latency_ms).filter(|&ms| ms != 0);
            report.observed_at = attempt.observed_at;
        }
        if attempt.err.is_none() {
            report.items = i64::try_from(attempt.streams.len()).unwrap_or(i64::MAX);
            all.extend(attempt.streams);
        }
        reports.push(report);
    }
    // Nothing to ask is not an answer. This used to read as success on the grounds that "no indexers
    // configured" is an operator choice rather than a scrape failure — but defaults mean a config can no
    // longer end up with none, so an empty scraper list now means every one of them was skipped for
    // want of a config URL. Reporting that as a confirmed-empty result is how a misconfiguration
    // becomes "this release does not exist".
    let mut any_ok = reports.iter().any(|r| r.outcome == OUTCOME_ANSWERED);
    // An EMPTY result is only authoritative when EVERY indexer answered. "Someone responded" is enough to
    // trust a non-empty list — whatever came back is real — but not enough to state that a release does
    // not exist. torrentio 502s while another indexer legitimately has nothing, and the union of those
    // two was reported to the app as a confident "not available" for an episode that does exist.
    // An indexer that can NEVER be asked is excluded from this judgement. Counting it as one that did not
    // answer looked right and made the condition unsatisfiable: with mediafusion unconfigured — the
    // shipped default — every genuinely empty result was reported as an indexer outage, negative caching
    // never ran, and three unavailable titles in a row flipped /health to degraded on a healthy service.
    // A permanent misconfiguration is not a transient failure, and feeding both into one counter loses
    // the difference. It is reported once per process by `log_indexer_skip_once` instead.
    //
    // A NON-empty partial list is a different case, and conflating the two was a mistake worth recording.
    // Marking it non-authoritative made every response uncacheable whenever ONE indexer was flaky: the
    // full 8s scrape and a fresh debrid cache-check fan-out on every single /stream, plus a degraded
    // header that has the app tell the viewer "sources temporarily unavailable" over a perfectly good
    // list. That is a worse answer than the one it was fixing. The list is served AND cached; `complete`
    // carries the incompleteness so the caller can hold it for a shorter time instead.
    let mut complete = true;
    for rep in &reports {
        // Only a PERMANENTLY unaskable indexer is excused. One whose config could not be minted this
        // minute is an outage: it would have voted, and we do not know how.
        if excused_from_quorum(&rep.outcome) {
            continue;
        }
        if rep.outcome != OUTCOME_ANSWERED {
            complete = false;
            if all.is_empty() {
                any_ok = false; // an EMPTY list is only authoritative when everyone answered
            }
            break;
        }
    }
    ScrapeResult { seeds: dedupe(all), any_ok, complete, sources: reports }
}

/// Go `dedupe` by infohash, merging the richest facts (fill missing fileIdx/size, max seeders); first-seen
/// order preserved.
fn dedupe(seeds: Vec<RawStream>) -> Vec<RawStream> {
    let mut index: HashMap<String, usize> = HashMap::new();
    let mut out: Vec<RawStream> = Vec::new();
    for s in seeds {
        if let Some(&pos) = index.get(&s.info_hash) {
            let e = &mut out[pos];
            if e.file_idx.is_none() && s.file_idx.is_some() {
                e.file_idx = s.file_idx;
            }
            if e.size_bytes.is_none() && s.size_bytes.is_some() {
                e.size_bytes = s.size_bytes;
            }
            if s.seeders.unwrap_or(0) > e.seeders.unwrap_or(0) {
                e.seeders = s.seeders;
            }
            continue;
        }
        index.insert(s.info_hash.clone(), out.len());
        out.push(s);
    }
    out
}
