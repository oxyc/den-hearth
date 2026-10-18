//! Addon request path: the Stremio `subtitles` resource plus the Den-specific translate/serve
//! endpoints.
//!
//!   GET /<config>/subtitles/<type>/<id>/<extra>.json    native subs (OpenSubtitles, hash-matched)
//!   GET /<config>/subtitle/<file_id>.(srt|vtt)          proxy+cache one OpenSubtitles file
//!   GET /<config>/translate/<type>/<id>/<extra>/<lang>.json  app-driven: kick off/await a translation → { url }
//!   GET /<config>/translate/<type>/<id>/<extra>/<lang>.srt   the translated SRT (cache hit after the .json warmed it)
//!   GET /<config>/translate/<type>/<id>/<extra>/<lang>.status how far a running translation has got
//!
//! `<id>` is `tt<digits>` or `tt<digits>:<season>:<episode>`. `<extra>` is the Stremio query blob
//! carrying `videoHash`/`videoSize` (the OSHash the app computed). On the translate routes it is
//! optional — the older five-segment form still resolves — but without it a translation can find no
//! Tier-1 anchor and is served on whatever timing its source happened to have.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use hyper::header::{HeaderMap, HeaderValue};
use hyper::{Response, StatusCode};
use serde_json::{json, Value};

use crate::cache;
use crate::httputil::{self, Body};
use crate::inflight::Guard as InFlightGuard;
use crate::logging::{self, LogGate};
use crate::opensubtitles;
use crate::ratelimit;
use crate::state::{AppState, SyncOutcome, SyncReservation, SyncTier};
use crate::userconfig::{LlmConfig, UserConfig};
use crate::{resync, srt, translate};

mod alignment;
mod translation;
use alignment::*;
use translation::*;

// Ordinary downloaded and translated subtitle artifacts mirror the app cache.
const CACHE_TTL: Duration = Duration::from_secs(60 * 60 * 24 * 60);
// A successful audio alignment is expensive to reproduce and only a few KiB to retain. Ten years is
// effectively "until the bounded disk cache evicts it", while keeping the cache format's ordinary
// finite-expiry semantics and allowing a future alignment-version bump to replace it cleanly.
const SYNC_RESULT_TTL: Duration = Duration::from_secs(60 * 60 * 24 * 365 * 10);
/// How long a raw sub stands in for an alignment that failed. Long enough that a retrying client
/// doesn't re-spawn the tier binary per request, short enough that a fixed deploy heals itself.
const SYNC_RETRY_TTL: Duration = Duration::from_secs(600);
/// Namespace for the "this sync just failed" marker. Kept off every prefix a BODY is stored under
/// (`BODY_PREFIXES`) so a marker can never be read back and served as a subtitle.
const SYNCFAIL: &str = "syncfail:";
/// Digits in an IMDb id. Real ones run to seven or eight; ten leaves room and still bounds the key.
const MAX_IMDB_DIGITS: usize = 10;
/// Longest a target-language name may be. The longest real one is a couple of dozen characters.
const MAX_LANG: usize = 64;
#[cfg(test)]
const BODY_PREFIXES: [&str; 3] = [cache::OS_NS, cache::SEARCH_NS, cache::TRANSLATE_NS];
// Search results turn over as new subs are uploaded, so a short TTL — enough to spare repeated
// round-trips when the app reopens a title, not so long that fresh uploads stay hidden.
const SEARCH_TTL: Duration = Duration::from_secs(60 * 60 * 6); // 6 hours
/// How long a search list is kept past `SEARCH_TTL`: not served as fresh, only as the fallback when a
/// live search fails, which beats an empty list for a title that had subtitles last week.
const SEARCH_STALE_GRACE: Duration = Duration::from_secs(60 * 60 * 24 * 7);
/// How long a failed search is remembered as failed. Short — this is a blip, not a verdict — but
/// long enough that the queue behind a single-flighted miss does not run one live search each,
/// serially, at up to the client timeout apiece.
const SEARCH_FAIL_TTL: Duration = Duration::from_secs(30);
/// How long a file the API says does not exist — or a repeat `Suspect` refusal `remember_failure`
/// has promoted to the same verdict — is remembered as not existing. A week: a day was long enough
/// to stop a burst of requests within one sitting, but not across sittings — a viewer revisiting the
/// same title days apart re-confirmed the same dead file each time, and each confirmation is a
/// metered download. Long enough that a dead track in a picker list stops costing a download credit
/// across a typical gap between revisits, short enough that an upload restored later is picked up.
const DEAD_FILE_TTL: Duration = Duration::from_secs(60 * 60 * 24 * 7);
/// Lifetime of a pinned translation source. Long because the pin is what keeps every language of one
/// film reading from ONE source file: that is a single metered download for the title rather than one
/// per language, and it is the whole job now.
///
/// It used to be the bill that was at stake — the body key named the source, so a re-pick produced a
/// different key and re-bought a film that was still in the cache. That is no longer true, and the
/// value did not change: `translate_body_key` names the title, so a lost pin costs a download and
/// nothing more. Kept long anyway; the pin is fifty bytes and the download is metered.
const SOURCE_PIN_TTL: Duration = Duration::from_secs(60 * 60 * 24 * 180);
/// Bumped when Tier-1 output semantics change, so a deploy never serves a 60-day artifact produced
/// by the previous aligner under a key that claims it is current.
const TIER1_CACHE_VERSION: u8 = 2;

/// The `Server-Timing` entry for an answer that came out of the cache rather than out of work.
const CACHE_HIT: &str = "cache;desc=hit";

/// Monotonic counter making sync scratch-file names unique per invocation.
static SYNC_SEQ: AtomicU64 = AtomicU64::new(0);

// One line a minute for each kind of failure that can repeat on every request while an upstream is
// down or a client keeps retrying (see `LogGate`).
static SEARCH_FAILED: LogGate = LogGate::new();
static DOWNLOAD_FAILED: LogGate = LogGate::new();
static SYNC_FAILED: LogGate = LogGate::new();
static UNSAFE_RESYNC: LogGate = LogGate::new();
static ALLOWANCE_REFUSED: LogGate = LogGate::new();
static CREDENTIAL_REFUSED: LogGate = LogGate::new();
static TRANSLATE_FAILED: LogGate = LogGate::new();

/// `os:<id>` — how a file id reads everywhere it reaches the decision log. Distinct from the
/// Stremio subtitle id (`os-{file_id}`, a dash, built into the picker response above) so the two
/// never get mistaken for each other when grepping.
fn os_id(file_id: i64) -> String {
    format!("os:{file_id}")
}

/// Identity for one decision-log line, gated by `LOG_IDENTITY` through `logging::Line::id`. Every
/// field is optional because no single caller knows all of them: the plain proxy route
/// (`/subtitle/<file_id>`) carries no imdb id at all, and the translate route has no servable file
/// id until a source is picked.
#[derive(Clone, Copy, Default, Debug)]
struct ServeCtx<'a> {
    rid: Option<&'a str>,
    file_id: Option<i64>,
    imdb: Option<&'a str>,
    season: Option<i64>,
    episode: Option<i64>,
    lang: Option<&'a str>,
}

impl ServeCtx<'_> {
    fn identity_fields(&self, mut line: logging::Line) -> logging::Line {
        if let Some(id) = self.file_id {
            line = line.id("file", os_id(id));
        }
        if let Some(imdb) = self.imdb {
            line = line.id("imdb", imdb);
        }
        if let Some(s) = self.season {
            line = line.id("season", s);
        }
        if let Some(e) = self.episode {
            line = line.id("episode", e);
        }
        if let Some(l) = self.lang {
            line = line.id("lang", l);
        }
        line
    }
}

/// `event=serve` — one download/serve decision: a cache hit, a sync tier that ran (and which one),
/// a refusal, or a fallback to an unaligned stand-in. `body`, when given, is parsed once more for
/// cue density (`srt::stats`) — count, span, cues/minute, largest gap — which is what makes a sparse
/// or dubbed-release file diagnosable from the journal rather than merely "served". Returns the line
/// rather than printing it, so a caller can test what it built; every real call site prints it.
/// The `cues`/`span_s`/`cues_per_min`/`max_gap_s` fields, shared by a served line (measured from the
/// body it is about to answer with) and a refused one (measured by the sparse check that refused it,
/// carried on the `DownloadError` rather than a body this call site no longer holds).
fn stats_fields(mut line: logging::Line, s: &srt::Stats) -> logging::Line {
    line = line
        .field("cues", s.count)
        .field("span_s", s.span_ms / 1000)
        .field("cues_per_min", format!("{:.1}", s.per_minute))
        .field("max_gap_s", s.max_gap_ms / 1000);
    line
}

fn serve_line(
    state: &AppState,
    ctx: ServeCtx<'_>,
    outcome: &str,
    reason: &str,
    upstream: Option<&str>,
    cache: &str,
    body: Option<&str>,
) -> String {
    serve_line_measured(
        state,
        ctx,
        outcome,
        reason,
        upstream,
        cache,
        body.and_then(|b| srt::stats(&srt::parse(b))),
    )
}

/// `serve_line` for a body whose cue density is already measured. A cached body measures itself once
/// (`Text::stats`) instead of being parsed again on every hit.
fn serve_line_measured(
    state: &AppState,
    ctx: ServeCtx<'_>,
    outcome: &str,
    reason: &str,
    upstream: Option<&str>,
    cache: &str,
    stats: Option<srt::Stats>,
) -> String {
    let mut line = logging::Line::new("serve", outcome, state.cfg.log_identity).reason(reason);
    if let Some(u) = upstream {
        line = line.upstream(u);
    }
    line = line.field("cache", cache);
    line = ctx.identity_fields(line);
    if let Some(s) = stats {
        line = stats_fields(line, &s);
    }
    line.rid(ctx.rid).finish()
}

/// `log_serve` for a cache hit: the line carries the body's cue stats, measured at most once per
/// cached body rather than parsed on every hit.
fn log_serve_cached(state: &AppState, ctx: ServeCtx<'_>, outcome: &str, reason: &str, body: &cache::Text) {
    eprintln!("{}", serve_line_measured(state, ctx, outcome, reason, None, "hit", body.stats()));
}

/// Prints what `serve_line` builds. The split exists for the test above it: this one call site is
/// the only thing between `log_serve` and the journal.
fn log_serve(
    state: &AppState,
    ctx: ServeCtx<'_>,
    outcome: &str,
    reason: &str,
    upstream: Option<&str>,
    cache: &str,
    body: Option<&str>,
) {
    eprintln!("{}", serve_line(state, ctx, outcome, reason, upstream, cache, body));
}

/// `event=serve outcome=refused` carrying the density numbers that caused it (`DownloadError::stats`),
/// when there are any to carry — the sparse/dubbed verdict's own measurement, not a body this call
/// site holds. Every other refusal (quota, an expired link, a cue-less body) has no numbers to give
/// and reads exactly as `log_serve` would print it.
fn log_serve_refused(
    state: &AppState,
    ctx: ServeCtx<'_>,
    reason: &str,
    upstream: Option<&str>,
    cache: &str,
    stats: Option<srt::Stats>,
) {
    let mut line = logging::Line::new("serve", "refused", state.cfg.log_identity).reason(reason);
    if let Some(u) = upstream {
        line = line.upstream(u);
    }
    line = line.field("cache", cache);
    line = ctx.identity_fields(line);
    if let Some(s) = &stats {
        line = stats_fields(line, s);
    }
    eprintln!("{}", line.rid(ctx.rid).finish());
}

/// `event=sync` — a resync/align step in the ladder (Tier 1 reference-align, Tier 2 audio VAD) that
/// failed, naming the file, the title and the step. `ref_file` is the reference sub Tier 1 aligned
/// against, when there is one. A step that SUCCEEDS is reported by the `serve` line that follows it
/// (`reason=tier1`/`tier2`) rather than a second line here — this one exists for the failure a
/// retry-marker would otherwise leave unexplained.
fn log_sync(state: &AppState, ctx: ServeCtx<'_>, tier: &str, reason: &str, ref_file: Option<i64>) -> String {
    let mut line =
        logging::Line::new("sync", "failed", state.cfg.log_identity).reason(reason).field("tier", tier);
    if let Some(r) = ref_file {
        line = line.id("ref", os_id(r));
    }
    line = ctx.identity_fields(line);
    line.rid(ctx.rid).finish()
}

/// `event=translate` — a translation that could not even start: the daily allowance, a refused
/// provider credential, a source that will not download, or a run that failed outright. Distinct
/// from `serve`: these are refusals to PRODUCE a translation, not a decision about what to serve.
fn log_translate(
    state: &AppState,
    ctx: ServeCtx<'_>,
    outcome: &str,
    reason: &str,
    upstream: Option<&str>,
) -> String {
    let mut line = logging::Line::new("translate", outcome, state.cfg.log_identity).reason(reason);
    if let Some(u) = upstream {
        line = line.upstream(u);
    }
    line = ctx.identity_fields(line);
    line.rid(ctx.rid).finish()
}

/// The resource descriptor Hearth folds into the combined Den manifest.
pub fn manifest() -> Value {
    json!({
        "id": "com.den.subtitles",
        // Single source of truth: the VERSION file, which the image build passes in as DEN_VERSION
        // (docker-publish asserts it == the v* tag). So the manifest can't drift from the tag.
        "version": crate::release_version(),
        "name": "Den Subtitles",
        "description": "Self-hosted subtitles for Den — configure with your OpenSubtitles key (AI translation optional).",
        "resources": ["subtitles"],
        "types": ["movie", "series"],
        "idPrefixes": ["tt"],
        "catalogs": [],
        "behaviorHints": { "configurable": true, "configurationRequired": true },
        // The source this addon's subtitles come from, for the client's credits (den-spec attribution-v1).
        "denAttribution": [{
            "text": "Subtitles from OpenSubtitles.com.",
            "link": "OpenSubtitles.com",
            "url": "https://www.opensubtitles.com",
        }],
    })
}

/// The base URL this server is reachable at (for the `url` we hand back to the client/app). Under the
/// combined Den addon (`den`) that is the addon's base, so the URL comes back on its terms.
pub fn self_base(
    state: &AppState,
    den: Option<&den_seal::Addon>,
    headers: &HeaderMap,
    config: &str,
) -> String {
    let public = if den.is_some() { None } else { state.cfg.public_base_url.as_ref() };
    let root = if let Some(b) = public {
        b.trim_end_matches('/').to_string()
    } else {
        let hdr = |name: &str| headers.get(name).and_then(|v| v.to_str().ok());
        let proto = hdr("x-forwarded-proto")
            .map(|p| p.split(',').next().unwrap_or("http").trim().to_string())
            .unwrap_or_else(|| "http".to_string());
        // Reflecting a client-controlled Host into a URL we hand back is a poisoning vector; only
        // accept a sane host charset, else fall back. Set PUBLIC_BASE_URL in deploy to avoid this
        // path entirely (see .env.example).
        let host = hdr("x-forwarded-host")
            .or_else(|| hdr("host"))
            .filter(|h| is_sane_host(h))
            .unwrap_or("localhost")
            .to_string();
        format!("{proto}://{host}")
    };
    match den {
        Some(den) => format!("{}/{config}", den.base(&root)),
        None => format!("{root}/{config}"),
    }
}

/// A hostname/authority we're willing to reflect into a returned URL: letters, digits, and the few
/// punctuation chars a host+port legitimately uses. Rejects spaces, slashes, `@`, etc.
fn is_sane_host(h: &str) -> bool {
    !h.is_empty()
        && h.len() <= 255
        && h.bytes().all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'-' | b':' | b'_'))
}

/// `tt123` or `tt123:1:2` → (imdb, season, episode).
fn parse_id(id: &str) -> Option<(String, Option<i64>, Option<i64>)> {
    let mut parts = id.split(':');
    let imdb = parts.next()?;
    // `tt` and digits, and not many of them. It goes into a cache key that becomes a filename —
    // the same hazard `search_hash` bounds `videoHash` against and `MAX_LANG` bounds `lang`
    // against. An id long enough to overflow NAME_MAX fails the disk write and burns the process's
    // one-shot "persistence degraded" warning on a request that was never a real title.
    let digits = imdb.strip_prefix("tt")?;
    if digits.is_empty() || digits.len() > MAX_IMDB_DIGITS || !digits.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    let season = parts.next().and_then(|s| s.parse().ok());
    let episode = parts.next().and_then(|s| s.parse().ok());
    Some((imdb.to_string(), season, episode))
}

/// The client's OSHash, if it really is one: 16 hex digits. Anything else is not a hash, and this
/// value goes into a cache key that becomes a filename — an over-long one fails the disk write and
/// burns the process's one-shot "persistence degraded" warning on a request that was never valid.
fn search_hash(extra: &str) -> Option<String> {
    extra_field(extra, "videoHash").filter(|h| h.len() == 16 && h.bytes().all(|b| b.is_ascii_hexdigit()))
}

/// The client's filename, bounded. A real release name is well under this; the bound is here because
/// of where the value GOES, which is `rank`'s sort comparator.
///
/// `rank` calls `fit_score` twice per comparison and each call re-tokenizes the filename, so the
/// work is (comparisons × filename length), and the comparison count is set by a 50-result page.
/// Nothing else on this path bounded it — `videoHash`, the imdb id and the language all are, for the
/// weaker reason that they become filenames — so a long enough `filename=` turned one cached search
/// into seconds of tokenizing on the single runtime thread, per request, for every request after the
/// first, including a 304 revalidation. That is the shape of defect this tree already fixed once in
/// the VTT escaper, arriving through the one field that had no length of its own.
fn search_filename(extra: &str) -> Option<String> {
    /// Longer than any real release name, and short enough that the tokenizing above is noise.
    const MAX_FILENAME: usize = 300;
    extra_field(extra, "filename").filter(|f| f.len() <= MAX_FILENAME)
}

/// Pull a `key=value` field out of the Stremio extra-args blob (already `.json`-stripped). The
/// client sends `videoHash`, `videoSize`, and `filename` here.
fn extra_field(extra: &str, key: &str) -> Option<String> {
    // Split first, decode second. Decoding the whole blob turned a %26 inside a value into a real
    // separator, so "Fast %26 Furious 6.mkv" arrived as the filename "Fast " — and a filename
    // carrying %26videoHash%3D... replaced the hash the client actually sent, which then went to
    // OpenSubtitles as the moviehash and silently disabled the whole sync ladder.
    let prefix = format!("{key}=");
    for pair in extra.split('&') {
        if let Some(v) = pair.strip_prefix(&prefix) {
            let v = httputil::percent_decode_path(v);
            if !v.is_empty() {
                return Some(v);
            }
        }
    }
    None
}

/// GET /<config>/subtitles/<type>/<id>/<extra>.json — native OpenSubtitles list. Each `url` points
/// at our own `/subtitle/<file_id>.srt` proxy so results are cached and served from our origin
/// (dodging the per-IP download quota). Hash matches are floated to the top by `search`.
pub async fn handle_subtitles(
    state: &Arc<AppState>,
    den: Option<&den_seal::Addon>,
    headers: &HeaderMap,
    config: &str,
    id: &str,
    extra: &str,
) -> Response<Body> {
    let Some(cfg) = state.decode_config(den, config) else {
        return httputil::json(StatusCode::BAD_REQUEST, &json!({"error": "bad_config"}), "no-store");
    };
    let Some((imdb, season, episode)) = parse_id(id) else {
        return httputil::json(StatusCode::BAD_REQUEST, &json!({"error": "bad_id"}), "no-store");
    };
    // A missing client was logged once, at boot; repeating it per request says nothing new. The empty
    // list is a stand-in, not an answer, and says so.
    let Some(http) = state.http.as_ref() else {
        return httputil::degraded(
            httputil::json(StatusCode::OK, &json!({"subtitles": []}), "no-store"),
            "upstream_unavailable",
        );
    };

    // An OSHash is 16 hex digits. Anything else is not one, and this value goes into a cache key
    // that becomes a filename — an over-long one fails the disk write and burns the process's
    // one-shot "persistence degraded" warning on a request that was never valid.
    let hash = search_hash(extra);
    let filename = search_filename(extra);
    let client = os_client(state, http, &cfg);
    let started = Instant::now();
    let searched = cached_search(state, &client, config, &imdb, season, episode, hash.as_deref()).await;
    let timing = match &searched {
        Ok((_, Searched::Cached)) => CACHE_HIT.to_string(),
        _ => format!("opensubtitles;dur={}", started.elapsed().as_millis()),
    };
    let stale = matches!(searched, Ok((_, Searched::Stale)));
    let mut subs = match searched {
        Ok((s, _)) => s,
        // Empty-200 is the correct Stremio shape for "nothing"; `cached_search` has already logged
        // the cause and counted it for /health. The header is what tells this "nothing" apart from a
        // title that has no subtitles.
        Err(_) => {
            let empty = httputil::json(StatusCode::OK, &json!({"subtitles": []}), "no-store");
            return httputil::add_timing(httputil::degraded(empty, "upstream_unavailable"), &timing);
        }
    };

    // Drop candidates already confirmed `Gone` (the API's own 404/410, or two occasions of a
    // cue-less body/expired link — see `remember_failure`) before they are offered at all: nothing
    // asking for one of these would ever get it served, picker or fallback alike. A SPARSE verdict
    // never escalates to `Gone` any more (same function), and a `dead_file_key` tag of `sparse` is
    // always a stale marker from before either that fix or the relative-sparse rule — one
    // `remembered_failure` purges on sight rather than trust — so either way it stays listed here.
    // The language disappears from `out` below only when EVERY one of its candidates is actually
    // dead, never because the only ones left are sparse.
    subs.retain(|s| state.cache.get(&dead_file_key(s.file_id)).is_none_or(|tag| tag == "sparse"));

    // Rank for THIS stream: hash-match, then release/filename fit, then quality; grouped by language
    // best-first. Machine/AI subs sink to the bottom.
    let rank_started = Instant::now();
    opensubtitles::rank(&mut subs, filename.as_deref());
    log_rank(state, headers, &imdb, season, episode, filename.as_deref(), &subs, rank_started.elapsed());

    let base = self_base(state, den, headers, config);
    // Tier-1 auto-sync anchor: any hash-matched sub is a trusted, already-in-sync timing reference
    // for the rest. `None` when auto-sync is off for this install, or when the search returned no
    // hash match at all — the out-of-sync gap Tier-1 can't close (see `tier1_reference`); either way
    // those subs are served as-is and only the Tier-2 audio resync (a user action) can fix them.
    let reference = if cfg.auto_sync { tier1_reference(&subs) } else { None };
    // The download route is addressed by file id alone and carries no title at all — everything it
    // needs to judge a candidate relative to its siblings (see `judge_sparse`) rides along in the
    // URL instead, exactly like `ref`/`lang` below. `episode_key` (shared by every language in this
    // answer) is what lets the per-episode "fullest seen" cache find the same record from any of
    // them; `dub_flagged` is decisive on its own regardless of density.
    let episode_key = self::episode_key(&imdb, season, episode);
    let out: Vec<Value> = subs
        .iter()
        .map(|s| {
            // A sub that needs Tier-1 gets `?ref=<id>` so the proxy reference-aligns it on fetch; a
            // hash match (or anything, when there's no anchor) is served as-is.
            let mut query = vec![format!("ep={episode_key}")];
            if opensubtitles::looks_dubbed(&s.release) || s.foreign_parts_only {
                query.push("dub=1".to_string());
            }
            if let Some(ref_id) = tier1_ref_for(s, reference) {
                query.push(format!("ref={ref_id}"));
            }
            // The language rides along when it narrows which legacy encoding the file may be in
            // (see `fetch::encoding_tld`); a code outside the URL-safe set is left off rather than
            // escaped, since it would map to no hint anyway.
            if crate::fetch::encoding_tld(&s.lang).is_some()
                && s.lang.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'-')
            {
                query.push(format!("lang={}", s.lang));
            }
            let mut url = format!("{base}/subtitle/{}.srt", s.file_id);
            if !query.is_empty() {
                url = format!("{url}?{}", query.join("&"));
            }
            // Standard Stremio fields (id/url/lang) plus Den-specific detail the app renders in the
            // subtitle picker; a generic client ignores the unknown fields.
            json!({
                "id": format!("os-{}", s.file_id),
                "url": url,
                "lang": s.lang,
                "release": s.release,
                "hd": s.hd,
                "fps": s.fps,
                "hashMatch": s.hash_match,
                "trusted": s.from_trusted,
                "downloads": s.downloads,
                "machineTranslated": s.machine_translated,
                "aiTranslated": s.ai_translated,
                // OpenSubtitles' own flag, and ours read off the release string (`looks_dubbed`):
                // both mean "this track's cues cover only part of the dialogue", which a client that
                // knows its own session's audio language can rank against an embedded track rather
                // than trust language-blind "first in the list" alone.
                "foreignPartsOnly": s.foreign_parts_only,
                "looksDubbed": opensubtitles::looks_dubbed(&s.release),
            })
        })
        .collect();
    // A strong ETag (hash of this serialized ranked list) is attached by `json()`; add
    // stale-while-revalidate so a client can serve the last list instantly while refreshing.
    // `private`: the URL is the install's config segment and every `url` in the body carries it too.
    if stale {
        // The last good list, past its freshness, because the live search failed. Worth serving over
        // an empty one, but only briefly: the client should ask again soon for the real answer.
        let resp = httputil::json(StatusCode::OK, &json!({"subtitles": out}), "private, max-age=60");
        return httputil::add_timing(httputil::degraded(resp, "stale_search"), &timing);
    }
    let resp = httputil::json(
        StatusCode::OK,
        &json!({"subtitles": out}),
        "private, max-age=3600, stale-while-revalidate=3600, stale-if-error=86400",
    );
    httputil::add_timing(resp, &timing)
}

/// `event=rank` — the ranking result of one catalog/search answer, ONE LINE PER LANGUAGE present in
/// the results. The search always asks OpenSubtitles for every language (`cached_search` passes
/// `"all"`), and `rank` groups the returned list by language, best fit first within each — so a
/// single global line named whichever language's code happened to sort first alphabetically (often
/// not the one the viewer's own language setting wants) and said nothing about what any other
/// language got offered. A line per language instead names that language's own top file, the next 2
/// behind it, and the deciding factors (`ranking_factors`) — the line that would have shown the
/// Fauda bug at a glance for the language it actually happened in, a DUBBED release's English file
/// ranked first with `looks_dubbed` sitting right there in its factors.
/// Returns the lines rather than printing them, so a caller can test what they built.
#[allow(clippy::too_many_arguments)]
fn rank_lines(
    identity: bool,
    rid: Option<&str>,
    imdb: &str,
    season: Option<i64>,
    episode: Option<i64>,
    filename: Option<&str>,
    subs: &[opensubtitles::Subtitle],
    dur: Duration,
) -> Vec<String> {
    if subs.is_empty() {
        let mut line = logging::Line::new("rank", "skipped", identity)
            .reason("no_candidates")
            .dur_ms(dur.as_millis())
            .field("candidates", 0)
            .id("imdb", imdb);
        if let Some(s) = season {
            line = line.id("season", s);
        }
        if let Some(e) = episode {
            line = line.id("episode", e);
        }
        return vec![line.rid(rid).finish()];
    }
    // Already grouped by language — `rank`'s sort order — so a run of the same `lang` is one
    // contiguous slice, and chunking on that is enough: no re-sorting or collecting into a map.
    subs.chunk_by(|a, b| a.lang == b.lang)
        .map(|group| {
            let lang = &group[0].lang;
            let mut line = logging::Line::new("rank", "served", identity)
                .reason("ranked")
                .dur_ms(dur.as_millis())
                .field("candidates", group.len())
                .id("imdb", imdb);
            if let Some(s) = season {
                line = line.id("season", s);
            }
            if let Some(e) = episode {
                line = line.id("episode", e);
            }
            line = line.id("lang", lang);
            // The order `rank` produced within this language: its top file, then the next 2, each
            // with why.
            for (i, s) in group.iter().take(3).enumerate() {
                let tag = if i == 0 { "top".to_string() } else { format!("next{i}") };
                let factors = opensubtitles::ranking_factors(s, filename);
                let factors = if factors.is_empty() { "none".to_string() } else { factors.join(",") };
                line = line.id(&tag, os_id(s.file_id)).field(&format!("{tag}_factors"), factors);
            }
            line.rid(rid).finish()
        })
        .collect()
}

/// Prints what `rank_lines` builds, one `eprintln!` per language. The split exists for the test above
/// it: this one call site is the only thing between `log_rank` and the journal.
#[allow(clippy::too_many_arguments)]
fn log_rank(
    state: &AppState,
    headers: &HeaderMap,
    imdb: &str,
    season: Option<i64>,
    episode: Option<i64>,
    filename: Option<&str>,
    subs: &[opensubtitles::Subtitle],
    dur: Duration,
) {
    let rid = logging::request_id(headers);
    for line in rank_lines(state.cfg.log_identity, rid.as_deref(), imdb, season, episode, filename, subs, dur)
    {
        eprintln!("{line}");
    }
}

/// The OpenSubtitles client for one request, from that install's BYOK credentials.
fn os_client<'a>(
    state: &'a AppState,
    http: &'a reqwest::Client,
    cfg: &'a UserConfig,
) -> opensubtitles::Client<'a> {
    opensubtitles::Client {
        http,
        api_key: &cfg.opensubtitles_key,
        token: cfg.opensubtitles_token.as_deref(),
        api_base: &state.cfg.os_api_base,
        limits: &state.os_limits,
    }
}

/// The search for one title, cached. Config-independent (file_ids and languages are the same for
/// everyone), keyed by the query params including the file hash. Short TTL — new subs get uploaded —
/// but enough to spare a live round-trip every time the app reopens a title.
///
/// Always asks for every language: the app filters by its own preferred-language rules, and the
/// translate path needs to see every candidate source.
///
/// Entries are per hash, so the picker (which sends one) and the translate path's source lookup
/// (which deliberately does not — see `handle_translate`) do NOT share an entry. A translate on a
/// cold title therefore pays a second search. Searches spend no download credit, and the alternative
/// is choosing the source from a list whose contents depend on the encode, which resolves two
/// encodes of one film to two sources: a second metered download, and a pin that moves under the
/// other encode every time the viewer switches between them.
///
/// Returned UNRANKED, and cached that way: ranking is filename-specific, so each caller ranks the
/// list for its own request.
///
/// A list past its freshness is kept for `SEARCH_STALE_GRACE` and answered with when the live search
/// fails, marked `Searched::Stale`; with none to fall back on the failure is an `Err`.
#[allow(clippy::too_many_arguments)]
async fn cached_search(
    state: &Arc<AppState>,
    client: &opensubtitles::Client<'_>,
    config: &str,
    imdb: &str,
    season: Option<i64>,
    episode: Option<i64>,
    hash: Option<&str>,
) -> Result<(Vec<opensubtitles::Subtitle>, Searched), String> {
    let search_key = format!(
        "{}{imdb}:{}:{}:{}",
        cache::SEARCH_NS,
        season.unwrap_or(0),
        episode.unwrap_or(0),
        hash.unwrap_or("")
    );
    let mut stale = None;
    match read_search(state, &search_key) {
        Some((hit, true)) => return Ok((hit, Searched::Cached)),
        Some((old, false)) => stale = Some(old),
        None => {}
    }
    // Single-flighted. Two requests arriving on a cold entry would each run a live search, and
    // OpenSubtitles returns one ordered page — so the two lists can differ and `translation_source`
    // can pick differently from each. One flight means one list, so both pick the same source.
    //
    // SHARED, deliberately — not scoped to the install like the failure marker below. Scoping it
    // seemed tidier and was the same bug at install granularity: two installs on a cold title each
    // ran their own live search and could each land on a different source.
    //
    // What that costs is now a DOWNLOAD, not a film. It used to cost the film: the body key named
    // the source, so two picks meant two keys and no later guard could collapse them. Since the
    // body is keyed by title, the guard on `body_key` is taken before any source is resolved and
    // collapses the two runs whatever they pick — so the exposure is one extra metered credit and a
    // pin that flips between two sources, which on a free tier of a handful a day is still worth a
    // flight, and one the plain picker's own downloads are drawn from.
    //
    // The cost of keeping it shared is that during an outage installs queue behind one guard and
    // then each miss their own marker, so they fail one after another rather than together. That is
    // latency, during an outage, bounded by install count. A metered quota still outranks it — but
    // it is a closer call than it was, so anyone widening this should re-weigh it rather than cite
    // a film's LLM bill, which is no longer what is at stake.
    let _flight = state.inflight.acquire(&search_key).await;
    match read_search(state, &search_key) {
        Some((hit, true)) => return Ok((hit, Searched::Live)),
        Some((old, false)) => stale = Some(old),
        None => {}
    }
    // The flight ahead of us may have failed. Nothing caches a failed search, so without this the
    // queue behind one miss ran a live search EACH, one after another, at up to the client timeout
    // apiece — the tenth caller waiting ten times as long as it used to for the same failure.
    // Scoped to the INSTALL, unlike the result. A successful search is install-independent — the
    // same file ids for everyone — which is why the positive entry is shared. A FAILURE is not: it
    // depends on the caller's key, token and rate limit, and a 401 from one revoked key would
    // otherwise deny that title to every other install for the marker's lifetime, re-armed every
    // time the bad install polled. `translate_fail_key` learned this already.
    //
    // `get_mem` to match the `put_mem` below: going through `get` would fall through to a disk probe
    // for a key nothing ever writes to disk — a blocking ENOENT `open` per cache-miss search.
    let fail_key = format!("{SYNCFAIL}{:016x}:{search_key}", short_hash(config));
    if state.cache.get_mem(&fail_key).is_some() {
        return match stale {
            Some(old) => Ok((old, Searched::Stale)),
            None => Err("search failed recently".to_string()),
        };
    }
    match client.search(imdb, season, episode, "all", hash).await {
        Ok(s) => {
            state.search_succeeded();
            let entry = SearchEntry { fresh_until: unix_seconds() + SEARCH_TTL.as_secs(), subs: s };
            if let Ok(json) = serde_json::to_string(&entry) {
                state.cache.put(search_key, json, SEARCH_TTL + SEARCH_STALE_GRACE);
            }
            Ok((entry.subs, Searched::Live))
        }
        // Count it so /health can report `degraded` (ADDON-02). Counted HERE rather than at one call
        // site, so a translation that cannot reach OpenSubtitles is visible on /health too — it was
        // not before. The cause is logged at most once a minute (our error strings carry no key):
        // during an outage every request fails the same way.
        Err(e) => {
            state.search_failed();
            if SEARCH_FAILED.allow() {
                eprintln!("search: opensubtitles failed for {imdb}: {e}");
            }
            state.cache.put_mem(fail_key, "1".into(), SEARCH_FAIL_TTL);
            match stale {
                Some(old) => Ok((old, Searched::Stale)),
                None => Err(e),
            }
        }
    }
}

/// Where `cached_search`'s list came from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Searched {
    /// Straight out of the cache, for `Server-Timing`.
    Cached,
    /// From a live search — or read from the cache after waiting on another request's search, which
    /// counts as a search: that is where the wait went.
    Live,
    /// Past its freshness, because the live search failed.
    Stale,
}

/// A cached search list and the wall-clock second it stops being fresh. The cache entry itself lives
/// `SEARCH_STALE_GRACE` longer, so the list is still there to fall back on when a search fails.
#[derive(serde::Serialize, serde::Deserialize)]
struct SearchEntry {
    fresh_until: u64,
    subs: Vec<opensubtitles::Subtitle>,
}

/// The cached list under `key` and whether it is still fresh.
fn read_search(state: &AppState, key: &str) -> Option<(Vec<opensubtitles::Subtitle>, bool)> {
    let raw = state.cache.get(key)?;
    if let Ok(entry) = serde_json::from_str::<SearchEntry>(&raw) {
        return Some((entry.subs, entry.fresh_until > unix_seconds()));
    }
    // A bare list is what entries were before the stale grace: written with the fresh TTL alone, so
    // one that is still present is still fresh.
    serde_json::from_str(&raw).ok().map(|subs| (subs, true))
}

/// The `file_id` of a trusted timing reference for Tier-1 reference alignment: the BEST hash-matched
/// sub, authored against this exact encode, so its timing is correct by construction (language
/// doesn't matter — timing is language-independent).
///
/// Best, not first. This runs after `rank`, which sorts by language ascending, so taking the first
/// hash match took the one belonging to the alphabetically-lowest language code — Arabic before
/// English. Every hash match is equally correct about timing, but they are not equally useful AS a
/// reference: a hash-matched forced/signs-only track is a handful of cues, and handing the aligner a
/// near-empty reference produces a bad alignment that then caches for sixty days. `fit_score` has no
/// cue count to work from, but trust/ratings/downloads separate a full dialogue track from a signs
/// track well enough.
///
/// The anchor decides the `?ref=` in every URL we hand back, so it has to be the same choice on
/// every request over the same title — not merely a valid one. Ties are common here (two untrusted,
/// unrated hash matches), so the score alone does not settle it and `file_id` breaks the tie.
///
/// It used to rest on list order instead, with `Reverse` + `min_by_key` to take the first of equal
/// maxima. That held only for a list someone had already ranked, and it was a precondition the two
/// callers did not both meet: the picker ranks, and the translate path reads `cached_search`, which
/// caches deliberately UNRANKED. So one title could anchor two different ways — a second metered
/// download, a second alignment and a duplicate `:ref:` entry — and the anchor could flip again
/// whenever the six-hour search entry lapsed and the API answered in a different order. Ordering by
/// something intrinsic to the subtitle costs nothing and removes the precondition.
///
/// `None` when the search produced no hash match at all. That is the gap behind the out-of-sync
/// complaint: with no trusted anchor we deliberately do NOT align to an untrusted sub (that could
/// make timing worse), so those subs are served as-is and only the Tier-2 audio resync — a user
/// action, see `resync::vet` — can fix them.
fn tier1_reference(subs: &[opensubtitles::Subtitle]) -> Option<i64> {
    subs.iter()
        .filter(|s| s.hash_match)
        .min_by_key(|s| (std::cmp::Reverse(opensubtitles::fit_score(s, None)), s.file_id))
        .map(|s| s.file_id)
}

/// The reference `s` should be Tier-1 aligned against on fetch (`?ref=`), or `None` when it needs no
/// alignment: a hash match is already in sync, a sub can't be aligned to itself, and with no anchor
/// nothing is aligned.
fn tier1_ref_for(s: &opensubtitles::Subtitle, reference: Option<i64>) -> Option<i64> {
    if s.hash_match {
        return None;
    }
    reference.filter(|&r| r != s.file_id)
}

/// GET /<config>/subtitle/<file_id>.srt[?ref=<id>|?resync=<url>][&lang=<code>] — download one
/// OpenSubtitles file, optionally auto-sync it (Tier 1 against a reference sub, or Tier 2 against the
/// stream audio), cache, serve. Any sync failure falls back to the raw sub — a slightly-off sub beats
/// none. `lang` only hints the encoding detection on a download; it is not part of any cache key.
///
/// A settled body's ETag names the vetted cache key rather than hashing the bytes (see
/// `settled_subtitle_etag`), so an `If-None-Match` for it is answered with a 304 before anything is
/// read, downloaded or aligned. `want_vtt` only decides which validator that 304 carries; the
/// router renders the body.
#[allow(clippy::too_many_arguments)]
pub async fn handle_subtitle_file(
    state: &Arc<AppState>,
    den: Option<&den_seal::Addon>,
    req_headers: &HeaderMap,
    config: &str,
    file_id: i64,
    ref_id: Option<i64>,
    resync_url: Option<String>,
    resync_key: Option<String>,
    resync_audio: Option<u32>,
    lang: Option<&str>,
    want_vtt: bool,
    allow_sparse: bool,
    episode_key: Option<String>,
    dub_flagged: bool,
) -> Response<Body> {
    let Some(cfg) = state.decode_config(den, config) else {
        return httputil::error(StatusCode::BAD_REQUEST, "bad_config");
    };
    // Vet BEFORE keying: a rejected URL changes which tier runs, so keying off the raw one filed a
    // reference-aligned (or raw) body under a `resync` key. Everything downstream reads the VETTED
    // value, so a refused target is simply a request that asked for no sync.
    let resync_url = resync_url.and_then(|u| match resync::vet(&u, &state.cfg.scout_origins) {
        Some(url) => Some(url.to_string()),
        // Never the URL itself: a stream target carries the provider's token.
        None => {
            if UNSAFE_RESYNC.allow() {
                eprintln!("subtitle: refusing resync target for {file_id} (not a scout play URL at a SCOUT_ORIGINS origin)");
            }
            None
        }
    });
    let ref_id = vetted_ref(file_id, ref_id);
    let install_scope = cfg.iid.as_deref().unwrap_or(config);
    let resync_cache_identity = resync_cache_identity(install_scope, &resync_url, resync_key, resync_audio);
    // Cache identity depends on the sync mode so the raw and aligned variants don't collide.
    let cache_key = sync_cache_key(&os_base_key(file_id), &resync_cache_identity, ref_id);
    let settled_etag = settled_subtitle_etag(&cache_key);
    let conditional_etag = match want_vtt {
        true => httputil::vtt_etag(&settled_etag),
        false => settled_etag.clone(),
    };
    if httputil::if_none_match(req_headers, &conditional_etag) {
        let resp = httputil::not_modified(&conditional_etag, httputil::SETTLED_SRT);
        return with_cached_subtitle_offset(state, &cache_key, resp);
    }
    let rid = logging::request_id(req_headers);
    // No imdb here at all: this route is addressed by OpenSubtitles file id alone, not by title —
    // the app already picked this file from the `subtitles` answer, which is where the imdb-scoped
    // `rank` line was logged.
    let ctx = ServeCtx {
        rid: rid.as_deref(),
        file_id: Some(file_id),
        imdb: None,
        season: None,
        episode: None,
        lang,
    };
    if let Some(hit) = state.cache.get_shared(&cache_key) {
        log_serve_cached(state, ctx, "served", "cache_hit", &hit);
        let resp =
            with_cached_subtitle_offset(state, &cache_key, httputil::srt_shared(hit.to_bytes(), hit.etag()));
        return with_settled_etag(httputil::add_timing(resp, CACHE_HIT), &settled_etag);
    }
    let Some(http) = state.http.as_ref() else {
        log_serve(state, ctx, "refused", "service_unavailable", None, "miss", None);
        return httputil::error(StatusCode::SERVICE_UNAVAILABLE, "service_unavailable");
    };
    let client = os_client(state, http, &cfg);

    // Exact-key coalescing stays ahead of both admission and body retention. A follower waits with
    // request metadata only, then observes the settled cache entry before spending a download.
    let flight = if resync_url.is_some() || ref_id.is_some() {
        let guard = state.inflight.acquire(&cache_key).await;
        if let Some(hit) = state.cache.get_shared(&cache_key) {
            log_serve_cached(state, ctx, "served", "cache_hit", &hit);
            let resp = with_cached_subtitle_offset(
                state,
                &cache_key,
                httputil::srt_shared(hit.to_bytes(), hit.etag()),
            );
            return with_settled_etag(httputil::add_timing(resp, CACHE_HIT), &settled_etag);
        }
        Some(guard)
    } else {
        None
    };
    // Admission starts before the target download. Requests outside this bound retain only request
    // metadata, not a capped subtitle body; otherwise a burst of distinct keys could queue an
    // unbounded number of 2 MiB targets behind four subprocess slots.
    let reservation = match (resync_url.is_some(), ref_id.is_some()) {
        (true, _) => Some(state.sync_admission.reserve(SyncTier::Tier2).await),
        (false, true) => Some(state.sync_admission.reserve(SyncTier::Tier1).await),
        _ => None,
    };
    let started = Instant::now();
    // Read before the call: a file already in the negative cache answers from it without touching
    // OpenSubtitles at all, so the `serve` line below must say `cache=hit`, not the "miss" every
    // refusal used to carry regardless of whether a credit was actually spent finding it out. A plain
    // read, same as `subtitle_srt`'s own check a moment later — reading it twice costs nothing and
    // the second read only ever confirms the first on the single-threaded path a request takes here.
    let refusal_cached = remembered_failure(state, &client, file_id, allow_sparse).is_some();
    let target =
        match subtitle_srt(state, &client, file_id, lang, allow_sparse, episode_key.as_deref(), dub_flagged)
            .await
        {
            Ok(body) => body,
            Err(e) => {
                let tag = e.reason_tag();
                let stats = e.stats();
                if DOWNLOAD_FAILED.allow() {
                    eprintln!("subtitle: download of file {file_id} failed: {}", e.message());
                }
                let cache = if refusal_cached { "hit" } else { "miss" };
                log_serve_refused(state, ctx, tag, Some("opensubtitles"), cache, stats);
                // `X-Den-Degraded: <tag>` (the same convention every den addon uses) is what lets
                // den-remux tell a `sparse` refusal apart from any other failure, so only THAT one is
                // worth retrying with `?sparse=1` — retrying a cue-less body or a transient blip the same
                // way would spend a second metered credit for no better chance of succeeding.
                return httputil::degraded(
                    httputil::error(StatusCode::BAD_GATEWAY, "upstream_unavailable"),
                    tag,
                );
            }
        };
    // `allow_sparse` only ever rescues a sparse body (download() refuses anything else regardless),
    // so re-measuring it here just tells `run_alignment_workflow` whether to log `sparse_fallback` rather
    // than the tier that actually served it — never whether to accept the body at all. The fullest
    // sibling count may already include THIS file by now (`download_and_cache` just recorded it), so
    // this read is a best-effort echo for the log line, not a re-run of the real gate.
    let sparse_fallback = allow_sparse
        && (dub_flagged
            || srt::looks_incomplete(
                &srt::parse(&target),
                fullest_sibling_cues(state, episode_key.as_deref()),
            ));

    // Settled: this handler is told which reference to use, so "no ref" here means none was asked
    // for, never that we failed to find out.
    let download = format!("download;dur={}", started.elapsed().as_millis());
    let what = format!("subtitle {file_id}");
    let resp = run_alignment_workflow(
        state,
        &client,
        SyncWorkflowInput {
            cache_key,
            target: Some(target),
            translated_body_key: None,
            ref_id,
            resync_url,
            resync_audio,
            reservation,
            flight,
            what: &what,
            settled: true,
            log: ctx,
            sparse_fallback,
        },
    )
    .await
    .response;
    with_settled_etag(httputil::add_timing(resp, &download), &settled_etag)
}

/// Bumped whenever the bytes this service serves for an existing subtitle key change — a decoding or
/// cleaning change in `subtitle_srt`, an aligner that now produces different timings. The settled
/// validator names the key, not the bytes, so without a bump a revalidating client keeps the old body.
const SETTLED_SUBTITLE_BYTES: u32 = 1;

/// The ETag of a settled proxied subtitle, computed from its vetted cache key alone. Sound because
/// the key fixes the bytes: an OpenSubtitles file id names one immutable upload, and the key names
/// the alignment applied to it — which is what `immutable` on the same response already promises.
/// A stand-in (`srt_provisional`) keeps its body-hash ETag, which can never equal this one, so it
/// never earns the early 304. Translations are not served through here: a re-run can change their
/// text under the same key.
fn settled_subtitle_etag(cache_key: &str) -> String {
    httputil::identity_etag(&format!("{cache_key}#{SETTLED_SUBTITLE_BYTES}"))
}

/// Give a settled subtitle response the identity validator the early 304 matches; anything else (an
/// error, a stand-in) passes through untouched.
fn with_settled_etag(mut resp: Response<Body>, etag: &str) -> Response<Body> {
    let settled = resp.status() == StatusCode::OK
        && resp.headers().get(hyper::header::CACHE_CONTROL).is_some_and(|v| v == httputil::SETTLED_SRT);
    if settled {
        if let Ok(v) = HeaderValue::from_str(etag) {
            resp.headers_mut().insert(hyper::header::ETAG, v);
        }
    }
    resp
}

/// A cache key turned into one safe filename component, plus a per-invocation sequence number.
///
/// Two concurrent requests for the same file must not share scratch paths — one would read the
/// other's half-written output and cache it for sixty days — which is what the sequence number is
/// for. The rest is making a filename out of something that was never one.
///
/// Every byte that is not `[A-Za-z0-9]` becomes `-`, rather than the `:` this replaced. `:` was the
/// separator the keys are built from, so it looked like the whole story, and it is not: an
/// OpenRouter model id is `openai/gpt-4o-mini`, and `translate_body_key` interpolates it verbatim.
/// The slash landed in the middle of `{tag}-target.srt`, `write_temp` creates only `work_dir` and
/// not the parent of the file, and every alignment for the install failed ENOENT — silently, since
/// a failed alignment is a `syncfail:` marker and a provisional body. That is Tier 1 and Tier 2
/// dead for one of the six providers, on its DEFAULT model, while the viewer still paid the whole
/// LLM bill for a translation served with the source's uncorrected timings.
///
/// The bound is the other half, and it is why this counts bytes rather than chars: the result is
/// `{tag}-reference.srt` in one directory, a translate key already runs to ~255 bytes with a long
/// model name, and past NAME_MAX the write fails exactly the same silent way. Mapping to ASCII
/// first makes the two agree — a multibyte model name counted 80 chars and wrote up to 320 bytes.
fn scratch_tag(cache_key: &str, seq: u64) -> String {
    let safe: String =
        cache_key.chars().map(|c| if c.is_ascii_alphanumeric() { c } else { '-' }).take(80).collect();
    format!("{safe}-{seq}")
}

fn with_cached_subtitle_offset(state: &AppState, cache_key: &str, resp: Response<Body>) -> Response<Body> {
    let offset_ms = state.cache.get(&subtitle_offset_key(cache_key)).and_then(|v| v.parse().ok());
    with_subtitle_offset(resp, offset_ms)
}

/// Fetch a subtitle's SRT, cached by file id (the raw, un-synced text — reused as a sync input).
/// `lang` hints the encoding detection when this call is the one that downloads. `allow_sparse` is
/// forwarded to `Client::download` and to `remembered_failure`'s backoff check — see both for why.
/// `episode_key`/`dub_flagged` feed `download_and_cache`'s sparse judgement; see `judge_sparse`.
#[allow(clippy::too_many_arguments)]
async fn subtitle_srt(
    state: &Arc<AppState>,
    client: &opensubtitles::Client<'_>,
    file_id: i64,
    lang: Option<&str>,
    allow_sparse: bool,
    episode_key: Option<&str>,
    dub_flagged: bool,
) -> Result<String, opensubtitles::DownloadError> {
    let key = os_base_key(file_id);
    if let Some(hit) = state.cache.get(&key) {
        return Ok(hit);
    }
    // Single-flighted, because this is the one call that spends a METERED credit: OpenSubtitles
    // charges the viewer's daily download allowance on the API call itself, and the anonymous
    // allowance is a handful per day. Two devices opening the same title, or the twenty picker URLs
    // that all carry the same `?ref=`, would each buy the same file.
    // A download that just failed is not retried on every request, because every attempt SPENDS.
    // OpenSubtitles charges the daily allowance on the API call, and for a dead upload the 404
    // arrives on the CDN fetch afterwards — so a request for a file that will never resolve cost a
    // credit and made no progress, for as long as anything kept asking. The picker hands back a URL
    // per subtitle and re-ranks the same dead track every playback, so a handful of clicks emptied
    // the day's allowance and then broke every other download with it.
    if let Some(e) = remembered_failure(state, client, file_id, allow_sparse) {
        return Err(e);
    }
    // A key that is rate limited or out of today's quota would get the same refusal for this file as
    // for every other one, so it is not asked. Nor is that refusal remembered against the file: the
    // file had nothing to do with it.
    if let Some(e) = client.download_paused() {
        return Err(e);
    }
    let flight = state.inflight.acquire(&key).await;
    if let Some(hit) = state.cache.get(&key) {
        return Ok(hit);
    }
    // Re-read after the wait, the way every other flight here does. Twenty picker URLs all carry the
    // same `?ref=R` under twenty different cache keys, so all twenty flights are distinct and all
    // twenty call this for the SAME reference — they clear the check above together, queue on
    // `os:R`, and then every one of them re-issues the download on release. Twenty metered credits
    // out of a daily handful, for a file already known to be failing.
    if let Some(e) =
        remembered_failure(state, client, file_id, allow_sparse).or_else(|| client.download_paused())
    {
        return Err(e);
    }
    // Admission is still request-owned: disconnecting while queued cancels the waiter without
    // spending. Once admitted, the bounded producer is detached so a committed credit reaches cache.
    let permit = client.admit_download().await?;
    // The worker owns the flight and its credentials. Once this task is spawned, a browser navigation
    // or request timeout can stop waiting without canceling the metered `/download` pipeline: the
    // worker finishes the CDN fetch and fills the cache, while a later caller waits on the same key.
    let worker_state = state.clone();
    let http = (*client.http).clone();
    let api_key = client.api_key.to_string();
    let token = client.token.map(str::to_string);
    let api_base = client.api_base.to_string();
    let lang = lang.map(str::to_string);
    let episode_key = episode_key.map(str::to_string);
    let worker = tokio::spawn(async move {
        let client = opensubtitles::Client {
            http: &http,
            api_key: &api_key,
            token: token.as_deref(),
            api_base: &api_base,
            limits: &worker_state.os_limits,
        };
        download_and_cache(
            &worker_state,
            &client,
            file_id,
            lang.as_deref(),
            allow_sparse,
            episode_key.as_deref(),
            dub_flagged,
            key,
            flight,
            permit,
        )
        .await
    });
    worker.await.unwrap_or_else(|e| {
        Err(opensubtitles::DownloadError::Unavailable(format!("download worker failed: {e}")))
    })
}

#[allow(clippy::too_many_arguments)]
async fn download_and_cache(
    state: &Arc<AppState>,
    client: &opensubtitles::Client<'_>,
    file_id: i64,
    lang: Option<&str>,
    allow_sparse: bool,
    episode_key: Option<&str>,
    dub_flagged: bool,
    key: String,
    _flight: InFlightGuard,
    permit: tokio::sync::OwnedSemaphorePermit,
) -> Result<String, opensubtitles::DownloadError> {
    // Read before the download: this file's own verdict must compare against what was already known
    // BEFORE it, not against a record it is about to contribute to itself.
    let fullest = fullest_sibling_cues(state, episode_key);
    let body =
        match client.download_admitted(file_id, lang, allow_sparse, dub_flagged, fullest, &permit).await {
            Ok(body) => {
                // A success clears the record. Without this the counter tallies failures across a whole
                // day with no credit for the successes between them, so a file that works nine times out
                // of ten still gets convicted on its second bad afternoon — and the verdict is the one
                // thing trusted to drop a pin shared by every install.
                forget_failure(state, file_id);
                // Measured regardless of `allow_sparse`/the sparse verdict: a low count can only ever
                // leave the running max unchanged, and a credit was already spent learning it either
                // way. Never from a dub-flagged file — its own count must not become a future sibling's
                // baseline, flagged or not.
                if !dub_flagged {
                    if let Some(s) = srt::stats(&srt::parse(&body)) {
                        record_episode_cues(state, episode_key, s.count);
                    }
                }
                body
            }
            Err(e) => {
                // A failure that paused the key — a 429, a spent quota — is remembered by that pause, for
                // as long as the service said. A per-file marker on top would outlast it by ten minutes.
                if client.download_paused().is_none() {
                    remember_failure(state, client, file_id, &e);
                }
                return Err(e);
            }
        };
    state.cache.put(key, body.clone(), CACHE_TTL);
    Ok(body)
}

/// Key of the pinned translation source for one title.
///
/// Scoped to the title and nothing else: every language and every install translates the same film
/// from the same file, which is what keeps one title to one paid translation per language rather
/// than one per re-pick. Deliberately NOT under `translate:`, since it holds a file id rather than a
/// subtitle and must never be reachable as a body.
fn source_pin_key(imdb: &str, season: Option<i64>, episode: Option<i64>) -> String {
    format!("source:{imdb}:{}:{}", season.unwrap_or(0), episode.unwrap_or(0))
}

/// Identity for one episode (or movie), shared by every language `handle_subtitles` answers for it —
/// carried in each URL's `?ep=` so the file-id-only download route can key the per-episode "fullest
/// seen" sibling record (`episode_stats_key`) without knowing the title any other way.
fn episode_key(imdb: &str, season: Option<i64>, episode: Option<i64>) -> String {
    format!("{imdb}:{}:{}", season.unwrap_or(0), episode.unwrap_or(0))
}

/// Cache key for an episode's running "fullest candidate seen" cue count (any non-dub-flagged
/// language) — what `judge_sparse` compares a new candidate against, since the download route sees
/// one file at a time and has no other way to know what else exists for the same episode.
fn episode_stats_key(episode_key: &str) -> String {
    format!("{SYNCFAIL}ep-stats:{episode_key}")
}

/// How long the per-episode "fullest seen" fact is kept — a week, the same lifetime as
/// `DEAD_FILE_TTL`'s other per-title fact: long enough to span a viewer's whole run at an episode
/// (replays, a resync days later), short enough to refresh itself if the candidate list changes.
const EPISODE_STATS_TTL: Duration = DEAD_FILE_TTL;

/// The most cues any candidate for this episode is already known to have, read before judging a new
/// one — `None` when there's no record yet (a lone candidate, or the first of its episode this
/// process has seen).
fn fullest_sibling_cues(state: &Arc<AppState>, episode_key: Option<&str>) -> Option<usize> {
    state.cache.get(&episode_stats_key(episode_key?))?.parse().ok()
}

/// Record that a real (non-dub-flagged) candidate for this episode had `count` cues, so a sibling
/// judged later can compare against it. A plain read-modify-write: two downloads for the same
/// episode racing here could drop one update, but this is a soft heuristic feeding a density
/// comparison, not billing or security, and the next candidate's own write corrects it.
fn record_episode_cues(state: &Arc<AppState>, episode_key: Option<&str>, count: usize) {
    let Some(key) = episode_key else { return };
    if count > fullest_sibling_cues(state, Some(key)).unwrap_or(0) {
        state.cache.put(episode_stats_key(key), count.to_string(), EPISODE_STATS_TTL);
    }
}

/// "This file will not download." A fact about the file, so shared by every install — and its own
/// namespace inside `syncfail:`, since `run_alignment_workflow`'s retry marker is `syncfail:{cache_key}` and
/// for a request that asked for no sync that key IS `os:{id}`.
fn dead_file_key(file_id: i64) -> String {
    format!("{SYNCFAIL}dl:gone:{}", os_base_key(file_id))
}

/// "This file is too big to translate." Also a fact about the file and shared, but distinct from
/// `dead_file_key`: this one downloads perfectly well, so it stays a fine subtitle for the plain
/// picker to hand over. It is only unusable as a translation SOURCE, and only the re-pick reads it.
fn oversized_file_key(file_id: i64) -> String {
    format!("{SYNCFAIL}dl:oversized:{}", os_base_key(file_id))
}

/// "Stop asking about this file for a moment." Short, shared, and it DOES gate — which is what keeps
/// a burst of requests for one failing file to a single spent credit.
fn suspect_backoff_key(file_id: i64) -> String {
    format!("{SYNCFAIL}dl:backoff:{}", os_base_key(file_id))
}

/// "This file failed at this time." The promotion counter, holding the unix second of the first
/// failure — deliberately NOT the same key as the gate above.
///
/// One key tried to be both and could not. Gating on the counter made the promotion unreachable,
/// since the attempt whose failure would confirm the strike was the attempt being refused. Not
/// gating on it made the promotion instant: twenty picker URLs all fetch the same reference, so
/// waiter one wrote the strike and waiter two confirmed it milliseconds later — two failures from
/// one incident, fabricating a verdict that is supposed to mean two occasions.
fn suspect_strike_key(file_id: i64) -> String {
    format!("{SYNCFAIL}dl:strike:{}", os_base_key(file_id))
}

/// "This credential could not fetch this file just now." Quota, a revoked key, a blip — facts about
/// one install's OpenSubtitles credential and nobody else's, so keyed by the credential rather than
/// by the install: two installs sharing a key really do share the quota that produced it.
fn unavailable_file_key(client: &opensubtitles::Client<'_>, file_id: i64) -> String {
    format!("{SYNCFAIL}dl:{:016x}:{}", os_download_identity(client), os_base_key(file_id))
}

/// The identity OpenSubtitles uses for download allowance: API key plus the optional account token.
/// Neither secret reaches the cache filename; the separator keeps two different pairs unambiguous.
fn os_download_identity(client: &opensubtitles::Client<'_>) -> u64 {
    short_hash(&format!("{}\n{}", client.api_key, client.token.unwrap_or("")))
}

/// Rebuild the failure a previous attempt recorded, so a request that cannot succeed does not spend
/// a metered credit finding that out again.
///
/// `allow_sparse` bypasses a remembered `sparse` backoff only: the caller has established nothing
/// better exists for this language, and a file that was merely too sparse for a STRICT request a
/// moment ago is exactly what this request wants. Every other remembered verdict — `gone`, a
/// backed-off `no_cues`/`expired_link`, an unavailable credential — still short-circuits: those name
/// a file (or a credential) that really cannot serve, which `allow_sparse` has no power over. A
/// bypass costs a fresh credit re-confirming what was already known, but it is the rare last resort,
/// not every request for a dead file.
fn remembered_failure(
    state: &Arc<AppState>,
    client: &opensubtitles::Client<'_>,
    file_id: i64,
    allow_sparse: bool,
) -> Option<opensubtitles::DownloadError> {
    // The stored value is the reason tag (`sparse`/`no_cues`/`expired_link`/`gone`/`suspect`) the
    // original failure classified as, not a bare marker — so a request answered from this negative
    // cache logs the SAME specific reason a live failure would, instead of collapsing every
    // remembered refusal into one generic verdict. `reason_tag` reads it back out of the
    // `(remembered: <tag>)` wrapper below.
    //
    // A `sparse` tag here is never a NEW escalation (`remember_failure` stopped promoting it) — it
    // can only be a marker from before that fix, written under a density rule this build no longer
    // runs (the old absolute 8/minute floor, not today's relative-to-siblings or dub-flagged one). A
    // file judged sparse then can easily not be now — Springfloden's three English files, all
    // comfortably clear of the current floors, would otherwise sit refused for the rest of a week on
    // a verdict nothing here still measures. Purged rather than trusted, so the file is judged fresh
    // at the one-download cost a live sparse refusal already pays.
    if let Some(tag) = state.cache.get(&dead_file_key(file_id)) {
        if tag == "sparse" {
            state.cache.remove(&dead_file_key(file_id));
        } else {
            return Some(opensubtitles::DownloadError::Gone(format!(
                "file {file_id} is gone (remembered: {tag})"
            )));
        }
    }
    // The BACKOFF gates; the strike counter deliberately does not. A burst of requests for one
    // failing file must cost one credit, not one each — but the attempt that confirms a strike has
    // to be allowed through, and it is, ten minutes later when this lapses.
    //
    // `get_mem` to match `put_mem` — going through `get` would probe a disk tier nothing writes to.
    if let Some(tag) = state.cache.get_mem(&suspect_backoff_key(file_id)) {
        if !(allow_sparse && tag == "sparse") {
            return Some(opensubtitles::DownloadError::Suspect(format!(
                "file {file_id} would not download (remembered: {tag})"
            )));
        }
    }
    if state.cache.get_mem(&unavailable_file_key(client, file_id)).is_some() {
        return Some(opensubtitles::DownloadError::Unavailable(format!(
            "file {file_id} unavailable (remembered)"
        )));
    }
    None
}

/// Seconds since the epoch. Wall clock rather than a monotonic instant because it is stored and
/// compared across requests; a backward clock step just delays a promotion.
fn unix_seconds() -> u64 {
    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map(|d| d.as_secs()).unwrap_or(0)
}

/// Drop the suspicion record for a file that has just downloaded cleanly.
///
/// Only the two `Suspect` keys. A `Gone` verdict is not forgotten here: it is the API's own answer
/// about the id, or a confirmed pair, and it gates this function's caller — so reaching a success
/// with one live is not something a clean download should quietly overwrite.
fn forget_failure(state: &Arc<AppState>, file_id: i64) {
    state.cache.remove_mem(&suspect_backoff_key(file_id));
    state.cache.remove_mem(&suspect_strike_key(file_id));
}

/// Remember a download failure so the next request does not spend a credit rediscovering it.
///
/// A `Suspect` escalates on repeat: the first one is a short shared strike, and a second inside that
/// window promotes to the week-long `Gone` marker. That is what separates a CDN interstitial — which
/// clears on its own and must not touch the shared source pin — from an upload that really is junk,
/// which would otherwise be re-fetched every ten minutes for a metered credit each time.
///
/// `sparse` never escalates: it names a real, correctly-timed track that simply doesn't cover
/// enough of the episode, not a broken upload or a CDN blip — the one shape `allow_sparse` exists to
/// rescue, which a `Gone` verdict (shared, week-long, meant for files that truly cannot serve) would
/// block just as hard as the short backoff it would otherwise promote past.
fn remember_failure(
    state: &Arc<AppState>,
    client: &opensubtitles::Client<'_>,
    file_id: i64,
    e: &opensubtitles::DownloadError,
) {
    use opensubtitles::DownloadError::*;
    // The reason this exact failure classified as — stored as the cache VALUE (not a bare "1") so a
    // later `remembered_failure` hit can log the same specific tag a live failure would.
    let tag = e.reason_tag();
    match e {
        Gone(_) => state.cache.put(dead_file_key(file_id), tag.into(), DEAD_FILE_TTL),
        // Two failures on two OCCASIONS promote. The gate is always refreshed, so a burst costs one
        // credit; the counter promotes only when the earlier failure is at least a backoff window
        // old, so the confirming attempt is one that genuinely had to be let through rather than a
        // sibling from the same instant.
        Suspect(_) if tag == "sparse" => {
            state.cache.put_mem(suspect_backoff_key(file_id), tag.into(), SYNC_RETRY_TTL);
        }
        Suspect(_) => {
            state.cache.put_mem(suspect_backoff_key(file_id), tag.into(), SYNC_RETRY_TTL);
            let strike = suspect_strike_key(file_id);
            let first = state.cache.get_mem(&strike).and_then(|v| v.parse::<u64>().ok());
            match first {
                Some(then) if unix_seconds().saturating_sub(then) >= SYNC_RETRY_TTL.as_secs() => {
                    state.cache.put(dead_file_key(file_id), tag.into(), DEAD_FILE_TTL)
                }
                // Already counted, too recent to confirm — leave it, so the clock keeps running from
                // the FIRST failure rather than being pushed forward by every sibling in a burst.
                Some(_) => {}
                None => state.cache.put_mem(strike, unix_seconds().to_string(), DEAD_FILE_TTL),
            }
        }
        Unavailable(_) => {
            state.cache.put_mem(unavailable_file_key(client, file_id), "1".into(), SYNC_RETRY_TTL)
        }
    }
}

/// The `ref` a request may actually use. `tier1_ref_for` refuses a self-reference when it BUILDS a
/// URL, but this value arrives on the query string where anyone can name it: `?ref=5` on file 5
/// spawned a tier binary to align a file to itself — a subprocess, on the one runtime thread, for a
/// guaranteed no-op — and filed the result under a reference key as though an alignment had happened.
fn vetted_ref(file_id: i64, ref_id: Option<i64>) -> Option<i64> {
    ref_id.filter(|&r| r != file_id)
}

/// The cache key for one raw OpenSubtitles file: the unaligned body that every sync variant of it
/// hangs off. Also what `subtitle_srt` files it under.
fn os_base_key(file_id: i64) -> String {
    format!("{}{file_id}", cache::OS_NS)
}

/// Cache key for one servable subtitle, namespaced by sync mode so the unaligned / reference-aligned
/// / audio-resynced variants of the same body never collide (else an aligned sub would be served
/// from the unaligned entry, or vice versa). A resync URL takes precedence over a `ref` — it's the
/// stronger Tier-2 correction — mirroring the dispatch in `run_alignment_workflow`.
///
/// `base` is the identity of the UNALIGNED body: `os:{file_id}` for a proxied subtitle, or the
/// translate key for a translated one. Taking a base rather than a file id is what lets a translated
/// body carry the same tier namespacing, on the same rules, without a second copy of them.
fn sync_cache_key(base: &str, resync_url: &Option<String>, ref_id: Option<i64>) -> String {
    match (resync_url, ref_id) {
        (Some(url), _) => format!("{base}:resync:{}", short_hash(url)),
        (None, Some(r)) => format!("{base}:ref:v{TIER1_CACHE_VERSION}:{r}"),
        (None, None) => base.to_string(),
    }
}

/// Stable Tier-2 cache identity supplied by Den, scoped to the install that owns the scout ticket.
/// Den hashes its release identity (info-hash, then filename), so rotating `/p/` tickets for the
/// same encode converge without putting a release name in a URL. The install scope prevents a
/// caller with another valid configuration from deliberately filling someone else's key. New
/// configs use their durable install id; legacy configs fall back to their full config blob.
/// Invalid/absent keys retain the old exact-target behavior for older clients.
fn resync_cache_identity(
    install_scope: &str,
    resync_url: &Option<String>,
    requested: Option<String>,
    audio_stream: Option<u32>,
) -> Option<String> {
    let url = resync_url.as_ref()?;
    let stable = requested.filter(|key| key.len() == 64 && key.bytes().all(|byte| byte.is_ascii_hexdigit()));
    stable
        .map(|key| {
            format!(
                "stable:v2:{:016x}:{}:a{}",
                short_hash(install_scope),
                key.to_ascii_lowercase(),
                audio_stream.map_or_else(|| "default".into(), |index| index.to_string())
            )
        })
        .or_else(|| {
            Some(format!("{url}#audio={}", audio_stream.map_or_else(|| "default".into(), |i| i.to_string())))
        })
}

/// Key of the "this translation failed recently" marker: title-scoped, and answerable before any
/// network call. That is the whole point of it — a whole film's LLM bill must not be re-paid on the
/// app's next tap, and the app's own `.json`-then-`.srt` flow retries once by design.
///
/// Deliberately NOT the key the translated body lives under (`translate_body_key`), for the scoping
/// reason below rather than a structural one: both are answerable from the request alone now that the
/// body is keyed by title, so either could be consulted this early — but they must not be the same
/// key, because one is shared and the other cannot be. Built here rather than inline so the handler
/// and the tests cannot drift apart on it — the same reason `sync_cache_key` exists, and `lang` in
/// particular is canonicalized rather than taken verbatim (see `translate::canonical_lang`).
///
/// Scoped to the INSTALL, unlike the body key. A translated body is the same bytes whoever asked for
/// it and is deliberately shared; a failure is not. One install with a dead provider key would
/// otherwise hand every other install on the same provider and model a 502 and a `.status` of
/// `failed` for ten minutes, and show them its progress bar — the cheapest cross-tenant lever in the
/// service, from something that was only ever meant to protect one viewer's bill.
fn translate_fail_key(
    config: &str,
    imdb: &str,
    season: Option<i64>,
    episode: Option<i64>,
    lang_key: &str,
    llm: &LlmConfig,
) -> String {
    format!(
        "{}{:016x}:{imdb}:{}:{}:{lang_key}:{}:{}",
        cache::TRANSLATE_NS,
        short_hash(config),
        season.unwrap_or(0),
        episode.unwrap_or(0),
        llm.provider.tag(),
        llm.model,
    )
}

/// Cache key for one title's translated text.
///
/// Keyed by the TITLE, not by the source file it was translated from — and that is a correction of
/// the obvious design, so it is worth saying why.
///
/// Source-keying was there to stop two encodes of one film resolving to two sources and buying the
/// dialogue twice. The source pin already guarantees that: one title translates from one file, and
/// every encode and every language follows the pin. So source-keying bought nothing the pin was not
/// already providing, and it cost something serious — the pin is the ONLY pointer to that source id,
/// so if it ever moved, every language already paid for became unreachable. A source dying while
/// adding a second language re-bought the first one at full price, from a cache that still held it.
///
/// With the title as the key, a re-pick changes which file a future untranslated language reads from
/// and nothing else. Paid bodies stay reachable, whatever happens to the pin.
///
/// Keyed by provider+model too: stepping up to a bigger model to re-translate a title that read badly
/// is meant to overwrite, and it can only do that if the model is part of the identity. NOT keyed by
/// install: these are the same bytes whoever asked for them, and sharing them is deliberate.
fn translate_body_key(
    imdb: &str,
    season: Option<i64>,
    episode: Option<i64>,
    lang_key: &str,
    llm: &LlmConfig,
) -> String {
    format!(
        "{}{imdb}:{}:{}:{lang_key}:{}:{}",
        cache::TRANSLATE_NS,
        season.unwrap_or(0),
        episode.unwrap_or(0),
        llm.provider.tag(),
        llm.model,
    )
}

/// Keep timing provenance in the same cache entry as the paid text. The title's source pin may
/// change when another language is added; it says nothing about an existing translation's timings.
#[derive(serde::Serialize, serde::Deserialize)]
struct TranslatedBody {
    source: Option<i64>,
    srt: String,
}

#[allow(clippy::too_many_arguments)]
pub async fn handle_translate(
    state: &Arc<AppState>,
    den: Option<&den_seal::Addon>,
    headers: &HeaderMap,
    config: &str,
    id: &str,
    extra: &str,
    lang: &str,
    want_json: bool,
    resync_url: Option<String>,
    resync_key: Option<String>,
    resync_audio: Option<u32>,
) -> Response<Body> {
    run_translation_workflow(
        state,
        TranslationWorkflowInput {
            den,
            headers,
            config,
            id,
            extra,
            lang,
            want_json,
            resync_url,
            resync_key,
            resync_audio,
        },
    )
    .await
}

#[allow(clippy::too_many_arguments)]
fn translate_url_response(
    state: &Arc<AppState>,
    den: Option<&den_seal::Addon>,
    headers: &HeaderMap,
    config: &str,
    season: Option<i64>,
    id: &str,
    extra: &str,
    lang_seg: &str,
) -> Response<Body> {
    let base = self_base(state, den, headers, config);
    // The segment as it arrived, not the decoded name: this is a URL, and handing back a decoded
    // "Brazilian Portuguese" would put a raw space in it. The client fetching this lands on the same
    // cache key it just warmed, whichever spelling it used — and it must carry the same extras, or
    // that fetch would resolve a different source and a different anchor.
    let url = match extra.is_empty() {
        true => format!("{base}/translate/{}/{}/{}.srt", type_of(season), id, lang_seg),
        false => format!("{base}/translate/{}/{}/{}/{}.srt", type_of(season), id, extra, lang_seg),
    };
    httputil::json(StatusCode::OK, &json!({ "url": url }), "no-store")
}

/// GET /<config>/translate/<type>/<id>[/<extra>]/<lang>.status — how far a running translation has
/// got, so the app can draw a bar instead of a spinner while the `.json` call it made on another
/// connection is still working.
///
/// Answered from the request alone: no search, no upstream call, no cache write. A poll arrives every
/// second or so while the expensive thing runs, and it must not itself become expensive — which is
/// why progress is filed under the title-scoped job key rather than the key the translation lands on.
///
/// `working` means a run is in progress in THIS process. Anything else is `idle`, `done` or `failed`,
/// and a client that gets `idle` should simply keep waiting on its `.json`: a run that has not
/// reached its first completed batch, or one being carried out by another instance, looks the same
/// from here and is not worth inventing a state for.
pub async fn handle_translate_status(
    state: &Arc<AppState>,
    den: Option<&den_seal::Addon>,
    config: &str,
    id: &str,
    lang: &str,
) -> Response<Body> {
    let Some(cfg) = state.decode_config(den, config) else {
        return httputil::json(StatusCode::BAD_REQUEST, &json!({"error": "bad_config"}), "no-store");
    };
    let Some(llm) = &cfg.llm else {
        return httputil::json(StatusCode::BAD_REQUEST, &json!({"error": "no_llm"}), "no-store");
    };
    let Some((imdb, season, episode)) = parse_id(id) else {
        return httputil::json(StatusCode::BAD_REQUEST, &json!({"error": "bad_id"}), "no-store");
    };
    let lang = httputil::percent_decode_path(lang);
    if lang.is_empty() || lang.len() > MAX_LANG {
        return httputil::json(StatusCode::BAD_REQUEST, &json!({"error": "bad_lang"}), "no-store");
    }
    let lang_key = translate::canonical_lang(&lang);
    if lang_key.is_empty() {
        return httputil::json(StatusCode::BAD_REQUEST, &json!({"error": "bad_lang"}), "no-store");
    }
    let job_key = translate_fail_key(config, &imdb, season, episode, &lang_key, llm);

    let body = if let Some((done, total)) = state.progress.get(&job_key) {
        json!({"state": "working", "done": done, "total": total})
    } else if state.cache.get(&format!("{SYNCFAIL}{job_key}")).is_some() {
        json!({"state": "failed"})
    } else {
        json!({"state": "idle"})
    };
    httputil::json(StatusCode::OK, &body, "no-store")
}

/// Why a translation did not happen, split by whether the SOURCE is to blame.
///
/// The distinction decides whether the pin survives. `Source` means this file cannot be translated
/// from — it will not download, or it parses to nothing — so the pin naming it is wrong and has to
/// go. `Model` means the provider timed out, refused, or produced junk: nothing to do with which
/// file was chosen, so dropping the pin buys a metered download and learns nothing. And the pin is
/// scoped to neither install nor language, so one install's rate limit would spend that credit on
/// behalf of everybody.
#[cfg(test)]
mod tests {
    use super::*;
    use crate::opensubtitles::Subtitle;
    use crate::userconfig;

    fn sub(file_id: i64, hash_match: bool) -> Subtitle {
        Subtitle {
            file_id,
            lang: "en".into(),
            hash_match,
            downloads: 0,
            release: String::new(),
            hd: false,
            fps: 0.0,
            from_trusted: false,
            machine_translated: false,
            ai_translated: false,
            foreign_parts_only: false,
            hearing_impaired: false,
            ratings: 0.0,
        }
    }

    #[test]
    fn tier1_reference_is_the_hash_match_or_none() {
        // No hash match anywhere → no anchor. This is the out-of-sync gap: Tier-1 can't run.
        assert_eq!(tier1_reference(&[sub(1, false), sub(2, false)]), None);
        // A hash match becomes the anchor, and a non-match never does.
        assert_eq!(tier1_reference(&[sub(1, false), sub(2, true), sub(3, true)]), Some(2));
        // Among equally-scoring hash matches the choice is stable AND independent of list order —
        // the anchor decides the `?ref=` in every URL handed back, so it must be the same answer
        // every time, not merely a valid one. Pinning it to list order was the bug: the picker ranks
        // its list and the translate path does not, so one title anchored two ways and paid a second
        // metered download for it.
        assert_eq!(tier1_reference(&[sub(3, true), sub(2, true), sub(1, true)]), Some(1));
        assert_eq!(tier1_reference(&[sub(1, true), sub(2, true), sub(3, true)]), Some(1));
        // Every permutation, since "some order happened to agree" is what the old assertion proved.
        for order in [[1, 2, 3], [1, 3, 2], [2, 1, 3], [2, 3, 1], [3, 1, 2], [3, 2, 1]] {
            let subs: Vec<Subtitle> = order.iter().map(|&i| sub(i, true)).collect();
            assert_eq!(tier1_reference(&subs), Some(1), "order {order:?} chose a different anchor");
        }
        // The tie-break is the LAST word, not the first: a better score still wins over a lower id.
        let better = Subtitle { downloads: 50_000, from_trusted: true, ..sub(9, true) };
        assert_eq!(tier1_reference(&[sub(1, true), better]), Some(9));
    }

    /// The anchor is the BEST hash match, not the first one in language order. `tier1_reference`
    /// runs after `rank` sorts by language ascending, so `find` handed the aligner whichever hash match
    /// belonged to the alphabetically-lowest language code — and a hash-matched forced/signs-only
    /// track is a handful of cues, which makes a poor reference and a bad alignment that then caches
    /// for sixty days.
    #[test]
    fn the_anchor_is_the_best_hash_match_not_the_first() {
        // A sparse signs track (first in language order) must lose to a well-used dialogue track.
        let signs = Subtitle { downloads: 3, ..sub(1, true) };
        let dialogue = Subtitle { downloads: 50_000, from_trusted: true, ..sub(2, true) };
        assert_eq!(tier1_reference(&[signs.clone(), dialogue.clone()]), Some(2));
        // And the order it arrives in doesn't change the answer.
        assert_eq!(tier1_reference(&[dialogue, signs]), Some(2));

        // A non-hash sub never becomes the anchor, however popular — being the same encode is the
        // whole claim, and downloads are not evidence of that.
        let popular_guess = Subtitle { downloads: 999_999, from_trusted: true, ..sub(9, false) };
        assert_eq!(tier1_reference(&[popular_guess, sub(4, true)]), Some(4));
    }

    #[test]
    fn tier1_ref_for_skips_hash_matches_self_and_missing_anchor() {
        let anchor = Some(2);
        // A non-hash sub aligns to the anchor.
        assert_eq!(tier1_ref_for(&sub(1, false), anchor), Some(2));
        // The hash-matched anchor is already in sync — no alignment.
        assert_eq!(tier1_ref_for(&sub(2, true), anchor), None);
        // A sub is never aligned to itself (its own id as the anchor is a no-op).
        assert_eq!(tier1_ref_for(&sub(2, false), anchor), None);
        // With no anchor, nothing is aligned — the gap case, served as-is.
        assert_eq!(tier1_ref_for(&sub(1, false), None), None);
    }

    /// The translation source is chosen as TEXT, not as a fit to this encode. If the hash match got
    /// a vote here, two encodes of one film would resolve to two different sources, so the pin would
    /// flip every time the viewer switched between them and each flip is another metered download
    /// out of a free tier of a handful a day.
    #[test]
    fn the_translation_source_does_not_depend_on_the_encode() {
        let good = Subtitle { downloads: 50_000, from_trusted: true, ..sub(1, false) };
        let poor = Subtitle { downloads: 2, ..sub(2, false) };
        // One encode hash-matches the poor sub; another hash-matches nothing. Same answer both times.
        let hashes_the_poor_one = [Subtitle { hash_match: true, ..poor.clone() }, good.clone()];
        let hashes_nothing = [poor.clone(), good.clone()];
        assert_eq!(translation_source(&hashes_the_poor_one).unwrap().file_id, 1);
        assert_eq!(translation_source(&hashes_nothing).unwrap().file_id, 1);

        // With no English at all, the best of what exists — and a machine/AI sub is the last resort,
        // never the first, so we don't translate a translation.
        let spanish = Subtitle { lang: "es".into(), downloads: 10, ..sub(3, false) };
        let ai_english = Subtitle { ai_translated: true, downloads: 999_999, ..sub(4, false) };
        assert_eq!(translation_source(&[spanish.clone(), ai_english.clone()]).unwrap().file_id, 3);
        // An English sub still wins when there is one, even a modest one.
        assert_eq!(translation_source(&[spanish, ai_english, poor]).unwrap().file_id, 2);

        assert!(translation_source(&[]).is_none());
    }

    /// A translated body carries the same tier namespacing a downloaded one does, hanging off the
    /// translate key instead of `os:{id}`. The unaligned translation and its aligned variants must
    /// not collide, and none of them may look like a retry marker.
    #[test]
    fn a_translated_body_carries_the_same_tier_namespacing() {
        let llm = LlmConfig {
            provider: userconfig::Provider::OpenAI,
            model: "gpt-4o-mini".into(),
            api_key: "k".into(),
        };
        let base = translate_body_key("tt0111161", None, None, "SV", &llm);
        let raw = sync_cache_key(&base, &None, None);
        let aligned = sync_cache_key(&base, &None, Some(9));
        let resynced = sync_cache_key(&base, &Some("http://host/s.mkv".into()), None);
        assert_eq!(raw, base, "the unaligned body is the base key itself");
        assert_ne!(raw, aligned);
        assert_ne!(raw, resynced);
        assert_ne!(aligned, resynced);
        for key in [&raw, &aligned, &resynced] {
            assert!(BODY_PREFIXES.iter().any(|p| key.starts_with(p)), "unnamespaced body key: {key}");
            assert!(!key.starts_with(SYNCFAIL), "a body key must never look like a marker: {key}");
        }

        // Two encodes of one film: the same translated text, two different anchors. One LLM bill and
        // two cheap alignments hanging off it.
        let encode_a = sync_cache_key(&base, &None, Some(11));
        let encode_b = sync_cache_key(&base, &None, Some(22));
        assert_ne!(encode_a, encode_b);
        assert!(encode_a.starts_with(&base) && encode_b.starts_with(&base));

        // A different title, or a different model, is a different translation. A different SOURCE is
        // not — the pin decides which file a title reads from, and a body already paid for stays
        // reachable when that decision changes. Were the source in this key, one language's source
        // dying would have re-bought every other language of the film.
        assert_ne!(translate_body_key("tt0068646", None, None, "SV", &llm), base);
        assert_ne!(translate_body_key("tt0111161", Some(1), Some(2), "SV", &llm), base);
        let bigger = LlmConfig { model: "gpt-4o".into(), ..llm.clone() };
        assert_ne!(translate_body_key("tt0111161", None, None, "SV", &bigger), base);
    }

    /// A subtitle is never its own reference. `tier1_ref_for` refuses that when it builds a URL, but
    /// `ref` arrives on the query string and anyone can name it — and `?ref=5` on file 5 spawned a
    /// tier binary to align a file to itself: a subprocess on the one runtime thread, for a
    /// guaranteed no-op, cached afterwards under a key that claims an alignment happened.
    #[test]
    fn a_subtitle_is_never_its_own_reference() {
        // Through the real function the handler calls, not a re-typed copy of the rule — a copy
        // cannot catch the handler dropping the check.
        assert_eq!(vetted_ref(5, Some(5)), None, "a file was accepted as its own reference");
        assert_eq!(vetted_ref(5, Some(9)), Some(9), "a real reference was rejected");
        assert_eq!(vetted_ref(5, None), None);
        // And with it refused, the request keys on the plain body rather than claiming an alignment.
        assert_eq!(sync_cache_key(&os_base_key(5), &None, vetted_ref(5, Some(5))), "os:5");
    }

    #[test]
    fn sync_cache_key_separates_raw_ref_and_resync() {
        let raw = sync_cache_key(&os_base_key(5), &None, None);
        let aligned = sync_cache_key(&os_base_key(5), &None, Some(9));
        let resynced = sync_cache_key(&os_base_key(5), &Some("http://host/s.mkv".into()), None);
        assert_eq!(raw, "os:5");
        assert_eq!(aligned, "os:5:ref:v2:9");
        assert!(resynced.starts_with("os:5:resync:"));
        // The three variants must never collide — else an aligned sub is served from the raw entry.
        assert_ne!(raw, aligned);
        assert_ne!(raw, resynced);
        assert_ne!(aligned, resynced);
        // resync (Tier-2) takes precedence over a ref (Tier-1) when both are present.
        let both = sync_cache_key(&os_base_key(5), &Some("http://host/s.mkv".into()), Some(9));
        assert_eq!(both, resynced);
    }

    #[test]
    fn a_stable_release_key_survives_ticket_rotation_but_stays_install_scoped() {
        let release = "a".repeat(64);
        let ticket_a = Some("https://scout.example/p/ticket-a".into());
        let ticket_b = Some("https://scout.example/p/ticket-b".into());
        let a = resync_cache_identity("install-a", &ticket_a, Some(release.clone()), Some(3)).unwrap();
        let b = resync_cache_identity("install-a", &ticket_b, Some(release.clone()), Some(3)).unwrap();
        assert_eq!(
            sync_cache_key("os:5", &Some(a.clone()), None),
            sync_cache_key("os:5", &Some(b.clone()), None)
        );

        let other_install = resync_cache_identity("install-b", &ticket_b, Some(release), Some(3)).unwrap();
        assert_ne!(
            sync_cache_key("os:5", &Some(other_install), None),
            sync_cache_key("os:5", &Some(b), None),
        );

        let invalid = resync_cache_identity("install-a", &ticket_b, Some("not-a-digest".into()), Some(3));
        assert_eq!(
            invalid,
            ticket_b.map(|url| format!("{url}#audio=3")),
            "older or malformed clients stay keyed by their exact target and audio stream"
        );
        assert!(resync_cache_identity("install-a", &None, Some("a".repeat(64)), Some(3)).is_none());

        let other_audio =
            resync_cache_identity("install-a", &ticket_a, Some("a".repeat(64)), Some(4)).unwrap();
        assert_ne!(a, other_audio, "different audio tracks must not share an aligned subtitle");
    }
}

#[cfg(test)]
mod id_tests {
    use super::*;

    /// The id is `tt` and digits, and it lands in a cache key that becomes a filename. Left
    /// unbounded it overflowed NAME_MAX, so the entry never persisted AND the process's one-shot
    /// "persistence degraded" warning was spent on it — the next real disk problem then said
    /// nothing. Two siblings of this value were already bounded for exactly that reason; this one
    /// was not, which is the shape of gap a per-field fix leaves behind.
    #[test]
    fn an_id_is_tt_and_a_sane_number_of_digits() {
        assert_eq!(parse_id("tt0111161").unwrap().0, "tt0111161");
        assert_eq!(parse_id("tt0111161:2:5").unwrap(), ("tt0111161".into(), Some(2), Some(5)));

        for bad in [
            "tt",          // no digits
            "tt12a4",      // not digits
            "nope0111161", // no prefix
            "",
        ] {
            assert!(parse_id(bad).is_none(), "accepted {bad:?}");
        }
        // Long enough to overflow a filename once base64'd into a cache key.
        let huge = format!("tt{}", "1".repeat(2000));
        assert!(parse_id(&huge).is_none(), "accepted a 2000-digit id");
        assert!(parse_id(&format!("tt{}", "1".repeat(MAX_IMDB_DIGITS + 1))).is_none());
        assert!(parse_id(&format!("tt{}", "1".repeat(MAX_IMDB_DIGITS))).is_some());
    }
}

#[cfg(test)]
mod sync_fallback_tests {
    use super::*;
    use hyper::header::CACHE_CONTROL;

    #[test]
    fn tier2_offset_is_exposed_as_response_metadata() {
        let response = with_subtitle_offset(httputil::srt("x".into()), Some(-5_880));
        assert_eq!(response.headers().get(X_DEN_SUBTITLE_OFFSET_MS).unwrap(), "-5880");

        let response = with_subtitle_offset(httputil::srt("x".into()), None);
        assert!(response.headers().get(X_DEN_SUBTITLE_OFFSET_MS).is_none());
    }

    /// The two response shapes must differ in the one way that matters: `immutable` tells a client
    /// never to come back, which is a year of an out-of-sync subtitle if the body is a stand-in.
    #[test]
    fn a_stand_in_is_revalidated_and_a_settled_body_is_not() {
        let cc = |r: Response<Body>| r.headers().get(CACHE_CONTROL).unwrap().to_str().unwrap().to_string();
        let provisional = cc(httputil::srt_provisional("x".into()));
        assert!(!provisional.contains("immutable"), "a stand-in must not be immutable: {provisional}");
        assert!(provisional.contains("no-cache"), "the client has to come back: {provisional}");
        assert!(cc(httputil::srt("x".into())).contains("immutable"));
    }

    /// A rejected resync target leaves a request that asked for no sync at all: the key must not
    /// keep claiming a resync, and `os:5` — the same key `subtitle_srt` fills — must not be
    /// downgraded to a stand-in, or the raw sub is re-downloaded on the next request.
    #[test]
    fn a_rejected_resync_target_leaves_a_plain_request() {
        let vetted: Option<String> = None; // what the SSRF guard leaves behind
        assert_eq!(sync_cache_key(&os_base_key(5), &vetted, Some(9)), "os:5:ref:v2:9");
        assert_eq!(sync_cache_key(&os_base_key(5), &vetted, None), "os:5");
        // Two different targets stay distinct, so one stream's alignment is never served for another.
        assert_ne!(
            sync_cache_key(&os_base_key(5), &Some("http://host/a.mkv".into()), None),
            sync_cache_key(&os_base_key(5), &Some("http://host/b.mkv".into()), None)
        );
    }

    /// The invariant the whole fix rests on: a retry marker can never be read back as a body. Built
    /// from the real key builders rather than literals, so a change to any of them is caught here.
    #[test]
    fn a_retry_marker_can_never_be_read_as_a_body() {
        // Every body key in the service starts with one of these; the marker starts with none.
        for prefix in BODY_PREFIXES {
            assert!(!prefix.starts_with(SYNCFAIL), "{SYNCFAIL} shadows the body prefix {prefix}");
            assert!(!SYNCFAIL.starts_with(prefix), "a {prefix} read would match {SYNCFAIL}");
        }
        // And the key builder really does produce keys inside that namespace, for every tier.
        for key in [
            sync_cache_key(&os_base_key(5), &None, None),
            sync_cache_key(&os_base_key(5), &None, Some(9)),
            sync_cache_key(&os_base_key(5), &Some("http://host/a.mkv".into()), None),
        ] {
            assert!(BODY_PREFIXES.iter().any(|p| key.starts_with(p)), "unnamespaced body key: {key}");
            assert!(!key.starts_with(SYNCFAIL));
        }
    }

    /// The extras blob is `key=value` pairs joined by `&`, each value percent-encoded. Decoding the
    /// blob before splitting let a value's own `%26` become a separator.
    #[test]
    fn an_encoded_ampersand_stays_inside_its_value() {
        let extra = "videoHash=8e24&videoSize=734003200&filename=Fast%20%26%20Furious%206%20(2013).mkv";
        assert_eq!(extra_field(extra, "filename").as_deref(), Some("Fast & Furious 6 (2013).mkv"));
        assert_eq!(extra_field(extra, "videoHash").as_deref(), Some("8e24"));

        // And a value cannot forge a field the client never sent.
        let forged = "filename=movie%26videoHash%3Dcafebabecafebabe.mkv";
        assert_eq!(extra_field(forged, "videoHash"), None, "a filename forged a videoHash");

        // A videoHash is 16 hex digits. Anything else is not one — and it lands in a cache key
        // that becomes a filename, where an over-long value fails the write and consumes the
        // process's only "persistence degraded" warning on a request that was never valid.
        assert_eq!(
            extra_field("videoHash=8e245d9679d31e12", "videoHash").as_deref(),
            Some("8e245d9679d31e12")
        );
        for bad in ["videoHash=8e24", "videoHash=zzzzzzzzzzzzzzzz", &format!("videoHash={}", "a".repeat(300))]
        {
            assert_eq!(search_hash(bad), None, "accepted a non-hash: {bad}");
        }

        // The extras arrive as a PATH segment, where `+` is a literal plus. Form-decoding it turned
        // every HDR10+ / DTS-HD 7.1+ release name into one the ranker scores differently.
        let plus = "filename=Movie.2013.2160p.HDR10+.WEB-DL.mkv";
        assert_eq!(extra_field(plus, "filename").as_deref(), Some("Movie.2013.2160p.HDR10+.WEB-DL.mkv"));
    }

    /// The filename is bounded because of where it goes, not because it becomes a filename like the
    /// other extras. `rank` re-tokenizes it twice per comparison over a 50-result page, so its
    /// length multiplies into the sort — it was the one client value on this path with no bound, and
    /// a long enough one spent seconds of the single runtime thread per request, on a cached search,
    /// including on a 304 revalidation.
    #[test]
    fn the_filename_the_ranker_sees_is_bounded() {
        // A real release name, however baroque, survives intact — the bound must not change ranking.
        let real = "Movie.Title.2013.2160p.UHD.BluRay.REMUX.HDR10+.DV.HEVC.DTS-HD.MA.7.1-GROUP.mkv";
        assert_eq!(search_filename(&format!("filename={real}")).as_deref(), Some(real));

        // And an absurd one reaches the ranker as nothing rather than as work. Asserted through
        // `rank` itself, since the bound exists for the ranker and a check on the extractor alone
        // would keep passing if the call site went back to `extra_field`.
        let absurd = "a.".repeat(100_000);
        let seen = search_filename(&format!("filename={absurd}"));
        assert_eq!(seen, None);
        let mut subs = vec![opensubtitles::Subtitle {
            file_id: 2,
            lang: "en".into(),
            hash_match: false,
            downloads: 0,
            release: String::new(),
            hd: false,
            fps: 0.0,
            from_trusted: false,
            machine_translated: false,
            ai_translated: false,
            foreign_parts_only: false,
            hearing_impaired: false,
            ratings: 0.0,
        }];
        opensubtitles::rank(&mut subs, seen.as_deref());
        assert_eq!(subs.len(), 1, "ranking dropped a candidate");
    }
}

#[cfg(test)]
mod translate_retry_tests {
    use super::*;
    use crate::config::Config;
    use crate::state::AppState;
    use crate::userconfig;

    /// A state with its own cache directory. The disk tier is real and persists between runs, so a
    /// shared directory would carry one test's markers into another's preconditions.
    fn state(name: &str) -> Arc<AppState> {
        state_with_base(name, "http://127.0.0.1:1".into())
    }

    fn state_with_base(name: &str, os_api_base: String) -> Arc<AppState> {
        let dir = std::env::temp_dir().join(format!("den-subs-translate-retry-{name}"));
        let _ = std::fs::remove_dir_all(&dir);
        AppState::new(Config {
            port: 0,
            cache_dir: dir,
            cache_max_bytes: 1 << 20,
            public_base_url: None,
            alass: "alass".into(),
            ffmpeg: "ffmpeg".into(),
            config_key: String::new(),
            config_keys_prev: String::new(),
            metrics_token: String::new(),
            log_requests: false,
            log_identity: true,
            // Port 1 refuses instantly. These cases are about the handler's own guards — the marker,
            // the language bound, the allowance — and every one of them has to get past a search
            // first. Pointed at the real API root they made a live request to api.opensubtitles.com
            // on every `cargo test`, which passed whether it 401'd or the network was down, so the
            // dependency was invisible.
            os_api_base,
            scout_origins: Vec::new(),
            scout_aliases: Vec::new(),
            revocation: Default::default(),
        })
    }

    #[tokio::test]
    async fn a_cancelled_waiter_does_not_cancel_a_metered_download_worker() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let seen = Arc::new(AtomicUsize::new(0));
        let server_seen = seen.clone();
        let cdn_started = Arc::new(tokio::sync::Notify::new());
        let server_started = cdn_started.clone();
        let release_cdn = Arc::new(tokio::sync::Notify::new());
        let server_release = release_cdn.clone();
        tokio::spawn(async move {
            for request_number in 0..2 {
                let (mut socket, _) = listener.accept().await.unwrap();
                let mut request = [0u8; 4096];
                let _ = socket.read(&mut request).await;
                server_seen.fetch_add(1, Ordering::SeqCst);
                let body = if request_number == 0 {
                    format!(r#"{{"link":"http://{addr}/cdn"}}"#)
                } else {
                    server_started.notify_one();
                    server_release.notified().await;
                    "1\n00:00:01,000 --> 00:00:02,000\nHello\n".to_string()
                };
                let response = format!(
                    "HTTP/1.1 200 OK\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
                    body.len()
                );
                socket.write_all(response.as_bytes()).await.unwrap();
                socket.shutdown().await.unwrap();
            }
        });

        let state = state_with_base("cancelled-download", format!("http://{addr}"));
        let first_state = state.clone();
        let first = tokio::spawn(async move {
            let http = reqwest::Client::new();
            let client = opensubtitles::Client {
                http: &http,
                api_key: "key",
                token: Some("token"),
                api_base: &first_state.cfg.os_api_base,
                limits: &first_state.os_limits,
            };
            subtitle_srt(&first_state, &client, 77, None, true, None, false).await
        });
        cdn_started.notified().await;
        first.abort();
        let _ = first.await;

        let second_state = state.clone();
        let second = tokio::spawn(async move {
            let http = reqwest::Client::new();
            let client = opensubtitles::Client {
                http: &http,
                api_key: "key",
                token: Some("token"),
                api_base: &second_state.cfg.os_api_base,
                limits: &second_state.os_limits,
            };
            subtitle_srt(&second_state, &client, 77, None, true, None, false).await
        });
        tokio::task::yield_now().await;
        release_cdn.notify_one();

        let body = second.await.unwrap().expect("the detached worker did not populate the cache");
        assert!(body.contains("Hello"));
        assert_eq!(seen.load(Ordering::SeqCst), 2, "the retry spent a second /download credit");
    }

    /// A config segment with an LLM key, so `handle_translate` gets past its own guards.
    fn config_segment() -> String {
        use base64::Engine;
        base64::engine::general_purpose::URL_SAFE_NO_PAD
            .encode(r#"{"osKey":"os-test","provider":"openai","apiKey":"llm-test","model":"m"}"#)
    }

    #[tokio::test]
    async fn cached_translation_alignment_uses_its_own_source_after_a_repick() {
        const RAW: &str = "1\n00:00:01,000 --> 00:00:02,000\nBonjour\n";
        const ALIGNED: &str = "1\n00:00:50,000 --> 00:00:51,000\nBonjour\n";
        // An old plain-SRT entry has unknown provenance; an entry actually made from the new
        // hash-matched source is already aligned. Neither case should require another LLM call.
        for (name, source, legacy, want) in [
            ("old-source", Some(1), false, ALIGNED),
            ("legacy-source", None, true, ALIGNED),
            ("current-source", Some(2), false, RAW),
        ] {
            let state = state(name);
            let config = config_segment();
            let cfg = state.decode_config(None, &config).unwrap();
            let key = translate_body_key("tt123", None, None, "FR", cfg.llm.as_ref().unwrap());
            let stored = if legacy {
                RAW.to_string()
            } else {
                serde_json::to_string(&TranslatedBody { source, srt: RAW.into() }).unwrap()
            };
            state.cache.put(key.clone(), stored, CACHE_TTL);
            state.cache.put(source_pin_key("tt123", None, None), "2".into(), SOURCE_PIN_TTL);
            let anchor = opensubtitles::Subtitle {
                file_id: 2,
                lang: "en".into(),
                hash_match: true,
                downloads: 10,
                release: String::new(),
                hd: false,
                fps: 24.0,
                from_trusted: true,
                machine_translated: false,
                ai_translated: false,
                foreign_parts_only: false,
                hearing_impaired: false,
                ratings: 8.0,
            };
            state.cache.put(
                "search:tt123:0:0:1111111111111111".into(),
                serde_json::to_string(&vec![anchor]).unwrap(),
                SEARCH_TTL,
            );
            state.cache.put(sync_cache_key(&key, &None, Some(2)), ALIGNED.into(), CACHE_TTL);
            let resp = handle_translate(
                &state,
                None,
                &HeaderMap::new(),
                &config,
                "tt123",
                "videoHash=1111111111111111",
                "French",
                false,
                None,
                None,
                None,
            )
            .await;
            assert_eq!(resp.status(), StatusCode::OK, "{name}");
            use http_body_util::BodyExt;
            let body = resp.into_body().collect().await.unwrap().to_bytes();
            assert_eq!(&body[..], want.as_bytes(), "{name}");
        }
    }

    fn listed_sub(file_id: i64) -> opensubtitles::Subtitle {
        opensubtitles::Subtitle {
            file_id,
            lang: "en".into(),
            hash_match: false,
            downloads: 1,
            release: String::new(),
            hd: false,
            fps: 0.0,
            from_trusted: false,
            machine_translated: false,
            ai_translated: false,
            foreign_parts_only: false,
            hearing_impaired: false,
            ratings: 0.0,
        }
    }

    fn store_search(state: &AppState, imdb: &str, file_id: i64, fresh_until: u64) {
        let entry = SearchEntry { fresh_until, subs: vec![listed_sub(file_id)] };
        state.cache.put(
            format!("{}{imdb}:0:0:", cache::SEARCH_NS),
            serde_json::to_string(&entry).unwrap(),
            SEARCH_STALE_GRACE,
        );
    }

    /// A failed search answers with the last good list rather than an empty one, marked as a fallback
    /// and cacheable only briefly. The upstream here refuses every connection.
    #[tokio::test]
    async fn a_failed_search_serves_the_stale_list() {
        use hyper::header::CACHE_CONTROL;
        let state = state("stale-search");
        store_search(&state, "tt0000094", 4242, unix_seconds() - 1);
        // Twice: the second request lands inside the failure marker's window, which must not turn the
        // fallback back into an empty list.
        for attempt in ["live search failed", "failure marker"] {
            let resp =
                handle_subtitles(&state, None, &HeaderMap::new(), &config_segment(), "tt0000094", "").await;
            assert_eq!(resp.status(), StatusCode::OK, "{attempt}");
            assert_eq!(resp.headers().get(httputil::X_DEN_DEGRADED).unwrap(), "stale_search", "{attempt}");
            assert_eq!(resp.headers().get(CACHE_CONTROL).unwrap(), "private, max-age=60", "{attempt}");
            let timing = resp.headers().get(httputil::SERVER_TIMING).unwrap().to_str().unwrap().to_string();
            assert!(timing.starts_with("opensubtitles;dur="), "a stale list is not a cache hit: {timing}");
            use http_body_util::BodyExt;
            let body = resp.into_body().collect().await.unwrap().to_bytes();
            assert!(String::from_utf8_lossy(&body).contains("\"os-4242\""), "{attempt}");
        }
    }

    /// A list inside its freshness is answered from the cache without a search, as a normal answer.
    #[tokio::test]
    async fn a_fresh_search_entry_is_a_plain_hit() {
        use hyper::header::CACHE_CONTROL;
        let state = state("fresh-search");
        store_search(&state, "tt0000095", 4243, unix_seconds() + 3600);
        let resp =
            handle_subtitles(&state, None, &HeaderMap::new(), &config_segment(), "tt0000095", "").await;
        assert_eq!(resp.status(), StatusCode::OK);
        assert!(resp.headers().get(httputil::X_DEN_DEGRADED).is_none());
        assert_eq!(
            resp.headers().get(CACHE_CONTROL).unwrap(),
            "private, max-age=3600, stale-while-revalidate=3600, stale-if-error=86400"
        );
        assert_eq!(resp.headers().get(httputil::SERVER_TIMING).unwrap(), CACHE_HIT);
    }

    /// The flags a client needs to rank a partial track against its own embedded one — OpenSubtitles'
    /// `foreign_parts_only` and our own release-string read, `looks_dubbed` — ride along on every
    /// listed subtitle, not only the ones `rank` demotes: den-remux is the one that knows its
    /// session's audio language, so it needs both signals even on a track that sorted first.
    #[tokio::test]
    async fn the_list_carries_the_partial_track_flags() {
        let state = state("partial-flags");
        let entry = SearchEntry {
            fresh_until: unix_seconds() + 3600,
            subs: vec![
                opensubtitles::Subtitle { foreign_parts_only: true, ..listed_sub(5001) },
                opensubtitles::Subtitle { release: "Fauda.S01E01.DUBBED.WEB-DL".into(), ..listed_sub(5002) },
            ],
        };
        state.cache.put(
            format!("{}tt0000096:0:0:", cache::SEARCH_NS),
            serde_json::to_string(&entry).unwrap(),
            SEARCH_STALE_GRACE,
        );
        let resp =
            handle_subtitles(&state, None, &HeaderMap::new(), &config_segment(), "tt0000096", "").await;
        assert_eq!(resp.status(), StatusCode::OK);
        use http_body_util::BodyExt;
        let body = resp.into_body().collect().await.unwrap().to_bytes();
        let v: serde_json::Value = serde_json::from_slice(&body).unwrap();
        let subs = v["subtitles"].as_array().unwrap();
        let flagged = subs.iter().find(|s| s["id"] == "os-5001").unwrap();
        assert_eq!(flagged["foreignPartsOnly"], true, "{body:?}");
        assert_eq!(flagged["looksDubbed"], false, "{body:?}");
        let dubbed = subs.iter().find(|s| s["id"] == "os-5002").unwrap();
        assert_eq!(dubbed["foreignPartsOnly"], false, "{body:?}");
        assert_eq!(dubbed["looksDubbed"], true, "{body:?}");
    }

    /// A candidate already confirmed `Gone` (the API's own verdict, or two occasions of a cue-less
    /// body/expired link) is never offered — nothing asking for it would ever get it served. A
    /// language with at least one LIVE candidate keeps every one of its entries, dead or not, since
    /// den-remux's own rank-order retry is what picks among them; only a language where EVERY
    /// candidate is dead disappears from the list entirely.
    #[tokio::test]
    async fn a_confirmed_dead_candidate_is_dropped_and_an_all_dead_language_vanishes() {
        let state = state("dead-candidates");
        let entry = SearchEntry {
            fresh_until: unix_seconds() + 3600,
            subs: vec![
                listed_sub(6001),                                                  // en, live
                opensubtitles::Subtitle { downloads: 2, ..listed_sub(6002) },      // en, confirmed gone
                opensubtitles::Subtitle { lang: "he".into(), ..listed_sub(6003) }, // he, confirmed gone
            ],
        };
        state.cache.put(
            format!("{}tt0000097:0:0:", cache::SEARCH_NS),
            serde_json::to_string(&entry).unwrap(),
            SEARCH_STALE_GRACE,
        );
        state.cache.put(dead_file_key(6002), "no_cues".into(), DEAD_FILE_TTL);
        state.cache.put(dead_file_key(6003), "gone".into(), DEAD_FILE_TTL);

        let resp =
            handle_subtitles(&state, None, &HeaderMap::new(), &config_segment(), "tt0000097", "").await;
        assert_eq!(resp.status(), StatusCode::OK);
        use http_body_util::BodyExt;
        let body = resp.into_body().collect().await.unwrap().to_bytes();
        let v: serde_json::Value = serde_json::from_slice(&body).unwrap();
        let subs = v["subtitles"].as_array().unwrap();

        assert!(subs.iter().any(|s| s["id"] == "os-6001"), "the live English candidate must stay: {body:?}");
        assert!(
            !subs.iter().any(|s| s["id"] == "os-6002"),
            "a confirmed-dead candidate must not be offered: {body:?}"
        );
        assert!(
            !subs.iter().any(|s| s["lang"] == "he"),
            "Hebrew's only candidate is dead, so the language must not appear at all: {body:?}"
        );
    }

    /// The owner's call on Fauda S1E3: a sparse candidate is NOT dead — it never escalates to `Gone`
    /// (see `remember_failure`) — so it must stay listed even though a strict fetch of it 502s. Only
    /// `allow_sparse` (den-remux's own last-resort retry) is allowed to serve it; the list's job is
    /// just to keep offering the language at all.
    #[tokio::test]
    async fn a_sparse_but_not_dead_candidate_stays_listed() {
        let state = state("sparse-not-dead");
        let entry = SearchEntry { fresh_until: unix_seconds() + 3600, subs: vec![listed_sub(3529838)] };
        state.cache.put(
            format!("{}tt4565380:1:3:", cache::SEARCH_NS),
            serde_json::to_string(&entry).unwrap(),
            SEARCH_STALE_GRACE,
        );
        // A strict refusal was remembered for it (exactly what a real request would have logged) —
        // `sparse`, never promoted to `dead_file_key`.
        state.cache.put_mem(suspect_backoff_key(3529838), "sparse".into(), SYNC_RETRY_TTL);

        let resp =
            handle_subtitles(&state, None, &HeaderMap::new(), &config_segment(), "tt4565380:1:3", "").await;
        assert_eq!(resp.status(), StatusCode::OK);
        use http_body_util::BodyExt;
        let body = resp.into_body().collect().await.unwrap().to_bytes();
        let v: serde_json::Value = serde_json::from_slice(&body).unwrap();
        let subs = v["subtitles"].as_array().unwrap();
        assert!(
            subs.iter().any(|s| s["id"] == "os-3529838"),
            "a sparse (not dead) candidate must still be listed: {body:?}"
        );
    }

    /// Every URL `handle_subtitles` hands out for the same episode must carry the SAME `?ep=` value
    /// (any language, so the download route's per-episode "fullest seen" record finds it from
    /// whichever file is fetched first) and `?dub=1` only on the release `judge_sparse` would refuse
    /// outright — the file-id-only download route has no other way to know either fact.
    #[tokio::test]
    async fn handle_subtitles_urls_carry_the_shared_episode_key_and_the_dub_flag() {
        let state = state("ep-key-and-dub-flag");
        let mut dubbed = listed_sub(5002);
        dubbed.release = "Fauda.S01E03.DUBBED.WEB-DL".into();
        let entry = SearchEntry { fresh_until: unix_seconds() + 3600, subs: vec![listed_sub(5001), dubbed] };
        state.cache.put(
            format!("{}tt4565380:1:3:", cache::SEARCH_NS),
            serde_json::to_string(&entry).unwrap(),
            SEARCH_STALE_GRACE,
        );

        let resp =
            handle_subtitles(&state, None, &HeaderMap::new(), &config_segment(), "tt4565380:1:3", "").await;
        use http_body_util::BodyExt;
        let body = resp.into_body().collect().await.unwrap().to_bytes();
        let v: serde_json::Value = serde_json::from_slice(&body).unwrap();
        let subs = v["subtitles"].as_array().unwrap();
        let url =
            |id: &str| subs.iter().find(|s| s["id"] == id).unwrap()["url"].as_str().unwrap().to_string();
        let (plain_url, dub_url) = (url("os-5001"), url("os-5002"));

        // No URL-parsing crate in scope here — a plain split on the one query param this test cares
        // about, since the URLs are built by plain string formatting, not escaped.
        fn param(url: &str, name: &str) -> Option<String> {
            url.split('?').nth(1)?.split('&').find_map(|kv| {
                let (k, v) = kv.split_once('=')?;
                (k == name).then(|| v.to_string())
            })
        }
        let ep_plain = param(&plain_url, "ep").expect("ep= missing from the non-dub URL");
        let ep_dub = param(&dub_url, "ep").expect("ep= missing from the dub URL");
        assert_eq!(ep_plain, ep_dub, "siblings in the same episode must share one ep= key");
        assert_eq!(ep_plain, "tt4565380:1:3");
        assert_eq!(param(&plain_url, "dub"), None, "a plain release must not carry dub=1");
        assert_eq!(param(&dub_url, "dub"), Some("1".to_string()), "a DUBBED release must carry dub=1");
    }

    /// `record_episode_cues`/`fullest_sibling_cues` — the per-episode cache record `judge_sparse`'s
    /// relative check reads. A lower count never lowers the running max, and no record at all (a
    /// lone candidate, or the first of its episode) reads back as `None`.
    #[test]
    fn episode_cue_stats_only_ever_grow() {
        let state = state("episode-cue-stats");
        let key = "tt0000000:1:1";
        assert_eq!(fullest_sibling_cues(&state, Some(key)), None, "nothing recorded yet");
        assert_eq!(fullest_sibling_cues(&state, None), None, "no episode key at all");

        record_episode_cues(&state, Some(key), 276);
        assert_eq!(fullest_sibling_cues(&state, Some(key)), Some(276));

        record_episode_cues(&state, Some(key), 200); // a sparser sibling arrives later
        assert_eq!(
            fullest_sibling_cues(&state, Some(key)),
            Some(276),
            "a lower count must not lower the max"
        );

        record_episode_cues(&state, Some(key), 321); // then a fuller one
        assert_eq!(fullest_sibling_cues(&state, Some(key)), Some(321));

        // A different episode's record is untouched.
        assert_eq!(fullest_sibling_cues(&state, Some("tt9999999:1:1")), None);
    }

    /// Fauda S1E3's real case: before den-subtitles 0.20.0 stopped a repeat `sparse` verdict from
    /// ever escalating, a second sparse refusal on the same file promoted it straight into the
    /// week-long `Gone` marker — exactly the shared value the listing filter otherwise reads as
    /// genuinely dead. That old marker's stored value is still `sparse` (`remember_failure` always
    /// wrote the reason tag as the cache value, escalation path or not), so it must be read the same
    /// way a live sparse backoff is: stays listed, not dropped as confirmed dead.
    #[tokio::test]
    async fn an_old_gone_marker_recorded_for_sparseness_stays_listed() {
        let state = state("old-gone-sparse");
        let entry = SearchEntry { fresh_until: unix_seconds() + 3600, subs: vec![listed_sub(3529838)] };
        state.cache.put(
            format!("{}tt4565380:1:3:", cache::SEARCH_NS),
            serde_json::to_string(&entry).unwrap(),
            SEARCH_STALE_GRACE,
        );
        // A pre-0.20.0 Gone marker: a second sparse refusal that used to escalate.
        state.cache.put(dead_file_key(3529838), "sparse".into(), DEAD_FILE_TTL);

        let resp =
            handle_subtitles(&state, None, &HeaderMap::new(), &config_segment(), "tt4565380:1:3", "").await;
        assert_eq!(resp.status(), StatusCode::OK);
        use http_body_util::BodyExt;
        let body = resp.into_body().collect().await.unwrap().to_bytes();
        let v: serde_json::Value = serde_json::from_slice(&body).unwrap();
        let subs = v["subtitles"].as_array().unwrap();
        assert!(
            subs.iter().any(|s| s["id"] == "os-3529838"),
            "an old Gone marker recorded for sparseness must still be listed: {body:?}"
        );
    }

    /// A film's LLM bill must not be re-paid on every tap. The `.json` and `.srt` forms are two
    /// requests, so the app retries once by design, and nothing was remembered about a failure.
    /// The marker has to short-circuit — and it must never be mistaken for the subtitle itself:
    /// its key is in the `syncfail:` namespace, and every body read uses a `translate:` key.
    #[tokio::test]
    async fn a_remembered_failure_short_circuits_without_being_served() {
        let state = state("short-circuit");
        let config = config_segment();
        let cfg = userconfig::decode(state.config_keyring.as_ref(), &config).expect("test config decodes");
        let llm = cfg.llm.as_ref().expect("test config carries an llm");
        // From the real key builder, not a literal: `lang` is canonicalized on the way into the key,
        // so a literal silently stopped addressing the same entry the handler reads.
        let cache_key =
            translate_fail_key(&config, "tt0111161", None, None, &translate::canonical_lang("Swedish"), llm);
        state.cache.put(format!("{SYNCFAIL}{cache_key}"), "1".into(), SYNC_RETRY_TTL);

        let resp = handle_translate(
            &state,
            None,
            &HeaderMap::new(),
            &config,
            "tt0111161",
            "",
            "Swedish",
            false,
            None,
            None,
            None,
        )
        .await;
        assert_eq!(resp.status(), StatusCode::BAD_GATEWAY);
        // Still a 502, and it says how much of the backoff is left.
        let wait: u64 =
            resp.headers().get(hyper::header::RETRY_AFTER).unwrap().to_str().unwrap().parse().unwrap();
        assert!((1..=SYNC_RETRY_TTL.as_secs()).contains(&wait), "Retry-After {wait}");

        // The marker's value must never reach the client as a subtitle.
        let body = String::from_utf8_lossy(
            &http_body_util::BodyExt::collect(resp.into_body()).await.unwrap().to_bytes(),
        )
        .to_string();
        assert_ne!(body.trim(), "1", "the retry marker was served as the response body");
        // `translation_backoff` is what says the marker short-circuited rather than a fresh attempt
        // failing. The config's OpenSubtitles key is a test string, so an attempt that got as far as
        // the upstream would ALSO come back 502 — the status alone proves nothing, only the body does.
        assert!(body.contains("translation_backoff"), "the marker did not short-circuit; body: {body}");
    }

    /// A refusal for the daily allowance says when the allowance comes back — the end of the UTC day
    /// its counter is keyed by — both as `Retry-After` and in the draft RateLimit headers.
    #[tokio::test]
    async fn an_allowance_refusal_says_when_the_allowance_returns() {
        let state = state("allowance-headers");
        let config = config_segment();
        let now = std::time::SystemTime::now();
        state.cache.put(quota_key(&config, now), DAILY_TRANSLATIONS.to_string(), QUOTA_TTL);

        let resp = handle_translate(
            &state,
            None,
            &HeaderMap::new(),
            &config,
            "tt0111161",
            "",
            "Swedish",
            false,
            None,
            None,
            None,
        )
        .await;
        assert_eq!(resp.status(), StatusCode::TOO_MANY_REQUESTS);
        let header = |name: &str| resp.headers().get(name).expect(name).to_str().unwrap().to_string();
        let wait: u64 = header("retry-after").parse().unwrap();
        assert!((1..=86_400).contains(&wait), "Retry-After {wait}");
        assert!(wait.abs_diff(ratelimit::until_utc_midnight(now).as_secs()) <= 1, "not the end of the day");
        assert_eq!(header("ratelimit-policy"), format!("\"daily\";q={DAILY_TRANSLATIONS};w=86400"));
        assert_eq!(header("ratelimit"), format!("\"daily\";r=0;t={wait}"));
    }

    /// Spellings of one language are one cache key. A translate key is a whole film's LLM bill
    /// against the viewer's own provider account, and "Swedish" / "swedish" / " Swedish " / "sv"
    /// each bought that film separately — no repeat request could ever hit the cache. The encoded
    /// spelling counted too: the segment was never percent-decoded, so "Brazilian%20Portuguese" was
    /// a fifth key AND the language name the model was handed in every batch prompt.
    #[test]
    fn spellings_of_one_language_share_a_cache_key() {
        let llm = LlmConfig {
            provider: userconfig::Provider::OpenAI,
            model: "gpt-4o-mini".into(),
            api_key: "k".into(),
        };
        let config = config_segment();
        let key = |lang: &str| {
            let decoded = httputil::percent_decode_path(lang);
            translate_fail_key(&config, "tt0111161", None, None, &translate::canonical_lang(&decoded), &llm)
        };
        // The body key carries the same canonicalized language, so the collapsing has to hold there
        // too — that is the key the film's text actually lives under, and the one whose duplicates
        // are paid for in full.
        let body_key = |lang: &str| {
            let decoded = httputil::percent_decode_path(lang);
            translate_body_key("tt0111161", None, None, &translate::canonical_lang(&decoded), &llm)
        };
        assert_eq!(body_key("sv"), body_key("Swedish"));
        assert_ne!(body_key("Swedish"), body_key("Finnish"));

        let swedish = key("Swedish");
        for same in ["swedish", "SWEDISH", "  Swedish  ", "sv", "SV"] {
            assert_eq!(key(same), swedish, "{same:?} was billed as a separate film");
        }
        // Percent-encoding is a spelling of the same language too.
        assert_eq!(key("Brazilian%20Portuguese"), key("brazilian portuguese"));

        // Different languages must still be different films.
        assert_ne!(key("Finnish"), swedish);
        // And a language the table doesn't carry keeps its own identity rather than collapsing into
        // some neighbour — the whole reason `canonical_lang` refuses to guess a code.
        assert_ne!(key("Hebrew"), key("Thai"));
        assert_ne!(key("Brazilian Portuguese"), key("Portuguese"));
    }

    /// The allowance bounds BREADTH, which nothing else did: every other limit caps one run, and an
    /// install URL in the wrong hands spends the viewer's own provider account one fresh film at a
    /// time. It is charged per install per day, and two installs do not share one.
    #[test]
    fn the_daily_allowance_is_per_install_and_per_day() {
        let state = state("quota");
        let one = config_segment();
        let two = format!("{}x", config_segment());

        for i in 0..DAILY_TRANSLATIONS {
            assert!(charge_translation(&state, &one), "refused run {i} inside the allowance");
        }
        assert!(!charge_translation(&state, &one), "the allowance did not stop anything");

        // A different install has its own, and is not held back by the first one's spending.
        assert!(charge_translation(&state, &two), "one install's allowance blocked another's");

        // And the counter is per day: yesterday's key is a different key, so a spent day does not
        // carry over into the next one.
        let now = std::time::SystemTime::now();
        let yesterday = now - Duration::from_secs(86_400);
        assert_ne!(quota_key(&one, now), quota_key(&one, yesterday));
        // The hash is of the config, so the secret itself never lands in a filename.
        assert!(!quota_key(&one, now).contains(&one));
    }

    /// A run the client hangs up on must not keep the slot it charged — and must not get it back
    /// once it has put a request on the wire.
    ///
    /// Cancellation is the one ending that produces no `Err` to inspect, so the explicit refund
    /// never runs and the retry charged a second slot. A client timeout shorter than a run (up to
    /// RUN_DEADLINE, ten minutes) turned a handful of films into an empty day.
    ///
    /// The condition is DISPATCH, not billing, and the difference is the whole test. A finished run
    /// has seen every answer and can honestly ask whether it was charged. A cancelled run has seen
    /// none: a request in flight is charged for whether or not we are still listening, and a real
    /// call may take LLM_TIMEOUT, so hanging up inside 100ms beats every response. Asking `billed`
    /// here refunded the slot on every such attempt while each attempt still sent a glossary call
    /// carrying the film's dialogue — the ceiling never moved, and the guard meant to bound spend
    /// became the way to spend unbounded.
    #[test]
    fn a_cancelled_run_keeps_its_slot_once_it_has_sent_anything() {
        let state = state("cancel");
        let config = config_segment();
        let used = |s: &Arc<AppState>| -> u64 {
            s.cache
                .get(&quota_key(&config, std::time::SystemTime::now()))
                .and_then(|v| v.parse().ok())
                .unwrap_or(0)
        };
        let guard = |spend: &'static translate::Spend, source_was_cached: bool, armed: bool| ChargeGuard {
            state: &state,
            config: &config,
            spend,
            source_was_cached,
            armed,
        };

        // Cancelled having sent nothing: the slot comes back.
        assert!(charge_translation(&state, &config));
        assert_eq!(used(&state), 1);
        {
            let untouched: &'static translate::Spend = Box::leak(Box::default());
            let _slot = guard(untouched, true, true);
        }
        assert_eq!(used(&state), 0, "a run that sent nothing kept its slot");

        // Cancelled with a request ON THE WIRE and no answer yet — `billed` is still false here, and
        // this is the case that made the guard a bypass rather than a bound.
        assert!(charge_translation(&state, &config));
        {
            let in_flight: &'static translate::Spend = Box::leak(Box::default());
            in_flight.dispatch_for_test();
            assert!(!in_flight.was_billed(), "precondition: no answer has arrived");
            let _slot = guard(in_flight, true, true);
        }
        assert_eq!(used(&state), 1, "a run with a request in flight refunded its slot");

        // And a request that spent a metered download keeps it too — the second condition, which is
        // what stops an unbounded number of distinct titles each burning a credit for free.
        assert!(charge_translation(&state, &config));
        {
            let untouched: &'static translate::Spend = Box::leak(Box::default());
            let _slot = guard(untouched, false, true);
        }
        assert_eq!(used(&state), 2, "a cancelled run refunded a metered download");

        // Disarmed, the guard does nothing: every ordinary ending decides for itself.
        {
            let untouched: &'static translate::Spend = Box::leak(Box::default());
            let _slot = guard(untouched, true, false);
        }
        assert_eq!(used(&state), 2, "a disarmed guard refunded anyway");
    }

    /// A refused credential is a fact about the install, and has to be remembered as one.
    ///
    /// The title-scoped marker can never catch it — every film is a different key — so a wrong or
    /// expired provider key, the likeliest misconfiguration in a BYOK product since the key rides in
    /// the install URL, cost a metered subtitle download per title browsed and would have drained the
    /// whole daily allowance on runs that sent no tokens at all.
    #[test]
    fn a_refused_key_is_remembered_for_the_install_not_the_title() {
        let llm = LlmConfig {
            provider: userconfig::Provider::OpenAI,
            model: "gpt-4o-mini".into(),
            api_key: "k".into(),
        };
        let one = credential_refused_key("install-one", &llm);

        // Not scoped to a title, so the next film is short-circuited too.
        assert!(!one.contains("tt"), "the credential marker names a title: {one}");
        // Another install is unaffected, and so is the same install on another provider or model —
        // switching either is the fix, and it must take effect at once.
        assert_ne!(one, credential_refused_key("install-two", &llm));
        let elsewhere = LlmConfig { model: "gpt-4o".into(), ..llm.clone() };
        assert_ne!(one, credential_refused_key("install-one", &elsewhere));
        // And it never leaks the config segment itself, which is a bearer secret.
        assert!(!one.contains("install-one"));

        // A refund gives back exactly the slot the refused run took, and cannot run the counter
        // below zero if it is somehow called twice.
        let state = state("refund");
        assert!(charge_translation(&state, "install-one"));
        refund_translation(&state, "install-one");
        refund_translation(&state, "install-one");
        for i in 0..DAILY_TRANSLATIONS {
            assert!(charge_translation(&state, "install-one"), "the refund lost a slot at {i}");
        }
    }

    /// An ambiguous refusal blocks the install only once a SECOND title has hit it.
    ///
    /// A 400 or a 403 might be the key and might be one film's dialogue tripping a content filter.
    /// Guessing per provider got that wrong twice in two rounds, in both directions — first blocking
    /// the whole library on one filtered series, then letting a dead key spend a metered download per
    /// title browsed. Two different titles refusing the same way is what actually distinguishes them.
    #[test]
    fn an_ambiguous_refusal_needs_two_titles_to_block_the_install() {
        let films = state("ambiguous");
        let llm =
            LlmConfig { provider: userconfig::Provider::OpenRouter, model: "m".into(), api_key: "k".into() };
        // Through the real function the handler calls — a re-typed copy of the rule cannot catch the
        // handler diverging from it, which is how the series case got in.
        let refuse = |title: &str| ambiguous_refusal_escalates(&films, "install-one", &llm, title);

        assert!(matches!(refuse("tt0111161:0:0"), Corroboration::None), "one title corroborated");
        // The same title again is still one film — a filtered film must not escalate by repeating.
        assert!(
            matches!(refuse("tt0111161:0:0"), Corroboration::None),
            "the same title twice corroborated itself"
        );
        // An UNRELATED one is the signal that this is the key, not the dialogue.
        assert!(
            matches!(refuse("tt0068646:0:0"), Corroboration::Unrelated),
            "two unrelated films did not implicate the credential"
        );

        // Episodes of one series are DIFFERENT titles here. `parse_id` returns the SERIES id, so
        // building this from that alone made every episode read as one title and the escalation
        // could never fire across a season. Asserted against the real builder: spelled inline in the
        // handler, reverting it compiled and passed.
        assert_ne!(
            title_id("tt1234567", Some(2), Some(5)),
            title_id("tt1234567", Some(2), Some(6)),
            "two episodes of one series count as the same title"
        );
        assert_ne!(title_id("tt0111161", None, None), title_id("tt0068646", None, None));
        // And a film is stable across requests, so repeating it never escalates on its own.
        assert_eq!(title_id("tt0111161", None, None), title_id("tt0111161", None, None));

        let series = state("ambiguous-series");
        let refuse = |title: &str| ambiguous_refusal_escalates(&series, "install-one", &llm, title);
        let ep = |s, e| title_id("tt1234567", Some(s), Some(e));
        assert!(matches!(refuse(&ep(2, 5)), Corroboration::None), "one episode corroborated");
        // A second EPISODE is a second title, so it is evidence — but only about the show. A content
        // filter objects to a script, and a season is the fastest way to produce two refusals, so
        // reading this as the credential let one filtered show block the library ten minutes at a
        // time. It takes something unrelated to say anything about the key.
        assert!(
            matches!(refuse(&ep(2, 6)), Corroboration::SameSeries),
            "the next episode of the same series did not corroborate the show"
        );
        assert!(
            matches!(refuse("tt0111161:0:0"), Corroboration::Unrelated),
            "a film after a series did not implicate the credential"
        );
        // Series ids compared as ids, not as prefixes of the whole title string.
        assert_eq!(series_of(&ep(2, 6)), "tt1234567");
        assert_ne!(series_of(&ep(2, 6)), series_of("tt12345678:2:6"));

        // The real decision, not a copy of it spelled inline — the previous version of this test
        // re-typed the rule as a closure, which passes just as happily when the handler stops
        // agreeing with it.
        let scope = |key_certain: bool, spent: bool, title: &str| {
            refusal_scope(&series, "install-two", &llm, title, key_certain, spent)
        };
        // A refusal that arrives after a billed batch is the content, so it is the SERIES. The
        // provider accepted this key in this run; blocking the install on that let one filtered show
        // re-arm the install-wide marker every time the previous one lapsed.
        assert!(matches!(scope(false, true, &ep(3, 1)), RefusalScope::Series));
        // Every time, not only the first: this is a measurement, so it does not need corroborating.
        assert!(matches!(scope(false, true, &ep(3, 2)), RefusalScope::Series));
        // And it is a real throttle — leaving it at the per-title marker alone was the regression
        // this replaced, where each next episode re-bought a download, a slot and half a bill.
        assert_ne!(
            series_refused_key("install-two", &llm, "tt1234567"),
            series_refused_key("install-two", &llm, "tt7654321"),
            "one filtered series backed off an unrelated one"
        );
        assert_ne!(
            series_refused_key("install-two", &llm, "tt1234567"),
            series_refused_key("install-three", &llm, "tt1234567"),
            "one install's filtered series backed off another install"
        );
        // A dead key bills nothing, so it rests on corroboration — and a second EPISODE is not the
        // corroboration that implicates a key. This is the case `spent` cannot catch: a content
        // filter that objects to a show's dialogue objects to the glossary sample drawn from that
        // same dialogue, so the filtered run often bills nothing and arrives here rather than above.
        assert!(matches!(scope(false, false, &ep(4, 1)), RefusalScope::TitleOnly));
        assert!(matches!(scope(false, false, &ep(4, 2)), RefusalScope::Series));
        // An unrelated title is what implicates the credential.
        assert!(matches!(scope(false, false, "tt0068646:0:0"), RefusalScope::Install));
        // And an unambiguous status blocks at once, billed or not — a key revoked mid-run is dead.
        assert!(matches!(scope(true, true, &ep(5, 1)), RefusalScope::Install));

        // And the markers are distinct namespaces, so none can be read as another.
        assert_ne!(ambiguous_refusal_key("install-one", &llm), credential_refused_key("install-one", &llm));
        assert_ne!(
            series_refused_key("install-one", &llm, "tt1234567"),
            credential_refused_key("install-one", &llm)
        );
    }

    /// A scratch tag has to be a FILENAME, and a cache key is not one. The ladder's temp writer
    /// creates `work_dir` and nothing below it, so a single separator anywhere in the key put the
    /// file in a directory that does not exist and every alignment failed ENOENT — as a `syncfail:`
    /// marker and a provisional body, which is to say silently.
    #[test]
    fn a_scratch_tag_is_a_single_filename_component() {
        // The real default model of a real provider, not a hand-written string: this was live for
        // every OpenRouter install, and asserting a literal would not have noticed.
        let model = userconfig::Provider::OpenRouter.default_model();
        assert!(model.contains('/'), "the case this guards is gone; find what replaced it");
        let key = translate_body_key(
            "tt0111161",
            None,
            None,
            "SV",
            &LlmConfig {
                provider: userconfig::Provider::OpenRouter,
                model: model.into(),
                api_key: "k".into(),
            },
        );
        let tag = scratch_tag(&key, 0);
        assert!(!tag.contains('/'), "the tag names a subdirectory: {tag}");
        assert_eq!(std::path::Path::new(&tag).components().count(), 1, "{tag}");

        // Nothing else can escape either — a `lang` arrives percent-decoded, so it can carry any
        // byte the length bound allows.
        for hostile in ["..", "../../etc/passwd", "a\\b", "a\0b", "a.b", "sv-SE"] {
            let tag = scratch_tag(hostile, 1);
            assert!(
                tag.chars().all(|c| c.is_ascii_alphanumeric() || c == '-'),
                "{hostile:?} survived as {tag:?}"
            );
            assert_eq!(std::path::Path::new(&tag).components().count(), 1, "{tag}");
        }

        // Bounded in BYTES, which is what NAME_MAX counts. Mapping to ASCII first is what makes the
        // char-wise take agree with it; a multibyte model name wrote up to four times the bound.
        let wide = format!("translate:tt1:0:0:SV:openai:{}", "é".repeat(200));
        assert!(scratch_tag(&wide, 999).len() <= 96, "{}", scratch_tag(&wide, 999).len());

        // And it is still unique per invocation, which is what stops one alignment reading another's
        // half-written scratch file and caching it for sixty days.
        assert_ne!(scratch_tag(&key, 0), scratch_tag(&key, 1));
    }

    /// A failure belongs to the install that had it. The translated BODY is the same bytes whoever
    /// asked for it and is deliberately shared; a failure is not. One install with a dead provider
    /// key would otherwise hand every other install on the same provider and model a 502, a
    /// `.status` of `failed`, and its progress bar — the cheapest cross-tenant lever in the service,
    /// out of something meant to protect one viewer's bill.
    #[test]
    fn a_failure_belongs_to_the_install_that_had_it() {
        let llm = LlmConfig {
            provider: userconfig::Provider::OpenAI,
            model: "gpt-4o-mini".into(),
            api_key: "k".into(),
        };
        let key = |config: &str| translate_fail_key(config, "tt0111161", None, None, "SV", &llm);

        assert_ne!(key("install-one"), key("install-two"), "two installs shared a failure marker");
        assert_eq!(key("install-one"), key("install-one"), "the key is not stable for one install");

        // A paid body must survive the source choice changing, and it does so structurally: the key
        // is built from the title and names no file id at all. The pin is the only pointer to a
        // source, so while the key named one, a source dying as a SECOND language was added
        // re-bought the FIRST at full price out of a cache that still held it.
        let body_of = translate_body_key("tt0111161", None, None, "SV", &llm);
        assert!(body_of.contains("tt0111161"), "the body key does not identify the title: {body_of}");
        assert_eq!(body_of.matches(':').count(), 6, "unexpected key shape: {body_of}");

        // The body key, by contrast, carries no install at all — that sharing is the point of it.
        let body = translate_body_key("tt0111161", None, None, "SV", &llm);
        assert!(!body.contains("install-one"));
        // And neither key may leak the config segment itself: it is a bearer secret, and these
        // become filenames.
        assert!(!key("install-one").contains("install-one"), "the config segment reached the key");
    }

    /// Only a failure that indicts the SOURCE may drop the pin, and this is the mapping that decides
    /// it. Blaming the source for a quota exhaustion or a rate limit re-picks and owes a metered
    /// credit for the replacement, at the moment the viewer has run out of them; blaming the service
    /// for a deleted upload leaves the pin resolving to nothing, and the ten-minute marker then
    /// cycles the same failure for the pin's whole 180-day life, for every install and language.
    #[test]
    fn only_a_dead_source_unpins() {
        use opensubtitles::DownloadError;

        let dead = classify_download(DownloadError::Gone("opensubtitles download 404".into()));
        assert!(matches!(dead, TranslationFailure::Source(_)), "a dead upload must drop the pin");

        for service in [
            DownloadError::Unavailable("opensubtitles download 429".into()),
            DownloadError::Unavailable("opensubtitles download 406".into()),
            DownloadError::Unavailable("download request failed: connection reset".into()),
            // A single suspect link fetch is not evidence about the file either. Only a repeat,
            // which `remember_failure` promotes to `Gone`, may drop a pin shared by every install.
            DownloadError::Suspect("subtitle link 404 Not Found".into()),
            DownloadError::Suspect("subtitle link returned no cues".into()),
        ] {
            let message = format!("{service:?}");
            assert!(
                matches!(classify_download(service), TranslationFailure::Model(_)),
                "a service failure dropped the pin: {message}"
            );
        }
    }

    /// A failed download must not be re-bought on the next request — the credit is charged on the
    /// API call, so a file that will not resolve costs one every time anything asks. And the two
    /// kinds of failure are remembered differently: the API naming an id is a fact about the file
    /// and shared, a credential's quota is not.
    #[test]
    fn a_failed_download_is_remembered_and_a_repeat_escalates() {
        use opensubtitles::DownloadError;

        let state = state("dl-fail");
        let http = reqwest::Client::new();
        let cfg = userconfig::decode(state.config_keyring.as_ref(), &config_segment()).unwrap();
        let client = os_client(&state, &http, &cfg);

        assert!(remembered_failure(&state, &client, 5, false).is_none(), "precondition: nothing remembered");

        // The API's verdict lands straight on the shared marker.
        remember_failure(&state, &client, 5, &DownloadError::Gone("404".into()));
        assert!(matches!(remembered_failure(&state, &client, 5, false), Some(DownloadError::Gone(_))));

        // One suspect link fetch earns a short backoff, so a burst of requests for the same file
        // costs one credit rather than one each. It reads back as `Suspect`, which refuses without
        // unpinning anything.
        remember_failure(&state, &client, 6, &DownloadError::Suspect("link 404".into()));
        assert!(matches!(remembered_failure(&state, &client, 6, false), Some(DownloadError::Suspect(_))));

        // A sibling from the SAME incident must not confirm it. Twenty picker URLs all fetch the
        // same reference, so two failures land milliseconds apart — one event, and the verdict they
        // would fabricate is the only one trusted to drop a pin shared by every install.
        remember_failure(&state, &client, 6, &DownloadError::Suspect("link 404".into()));
        assert!(
            state.cache.get(&dead_file_key(6)).is_none(),
            "a burst confirmed itself and fabricated a verdict"
        );

        // A failure on a LATER occasion does confirm it. Backdating the strike past the backoff
        // window stands in for waiting one out.
        let long_ago = (unix_seconds() - SYNC_RETRY_TTL.as_secs() - 1).to_string();
        state.cache.put_mem(suspect_strike_key(6), long_ago, DEAD_FILE_TTL);
        remember_failure(&state, &client, 6, &DownloadError::Suspect("link 404".into()));
        assert!(
            matches!(remembered_failure(&state, &client, 6, false), Some(DownloadError::Gone(_))),
            "a second occasion did not confirm the file is dead"
        );

        // "Too big to translate" is a different verdict from "will not download", and must not be
        // filed as one. This file fetches perfectly; the plain picker should keep handing it over,
        // and only the choice of a translation SOURCE skips it.
        assert_ne!(oversized_file_key(6), dead_file_key(6));
        assert_ne!(oversized_file_key(6), suspect_backoff_key(6));
        state.cache.put(oversized_file_key(9), "1".into(), SOURCE_PIN_TTL);
        assert!(
            remembered_failure(&state, &client, 9, false).is_none(),
            "an oversized file was read back as one that will not download"
        );

        // And a clean download clears the record, so a file that works nine times in ten is never
        // convicted by two bad afternoons a day apart.
        remember_failure(&state, &client, 8, &DownloadError::Suspect("blip".into()));
        forget_failure(&state, 8);
        assert!(
            remembered_failure(&state, &client, 8, false).is_none(),
            "a success left the file under suspicion"
        );

        // A credential's own trouble is remembered per credential, not for everyone: one install
        // exhausting its allowance must not deny the file to another.
        remember_failure(&state, &client, 7, &DownloadError::Unavailable("429".into()));
        assert!(matches!(remembered_failure(&state, &client, 7, false), Some(DownloadError::Unavailable(_))));
        let other_token = opensubtitles::Client {
            http: client.http,
            api_key: client.api_key,
            token: Some("another-user"),
            api_base: client.api_base,
            limits: client.limits,
        };
        assert!(
            remembered_failure(&state, &other_token, 7, false).is_none(),
            "one account token's refusal denied another account sharing the API key"
        );
        let other = opensubtitles::Client { api_key: "a-different-install-key", ..client };
        assert!(
            remembered_failure(&state, &other, 7, false).is_none(),
            "one install's quota denied the file to another"
        );
        // And the file-scoped ones are shared, because they are facts about the file.
        assert!(matches!(remembered_failure(&state, &other, 5, false), Some(DownloadError::Gone(_))));

        // The SYNC retry marker has to make the same split one level up, and did not. A failed
        // alignment is a fact about the work and is shared, so one install's attempt spares the
        // rest — but a reference download refused for quota is this credential's own trouble, and
        // remembering it globally served every other install the unaligned body for ten minutes,
        // re-armed each time the exhausted install polled. That is the mis-timed track the ladder
        // exists to fix, handed to installs that had done nothing.
        let key = sync_cache_key(&os_base_key(42), &None, Some(43));
        let shared = format!("{SYNCFAIL}{key}");
        let mine = format!("{SYNCFAIL}{:016x}:{key}", os_download_identity(&client));
        let theirs = format!("{SYNCFAIL}{:016x}:{key}", os_download_identity(&other));
        assert_ne!(shared, mine, "the per-credential marker collided with the shared one");
        assert_ne!(mine, theirs, "two credentials shared a backoff");
        // And neither may be mistaken for the key that holds the settled alignment.
        assert_ne!(shared, key);
        assert_ne!(mine, key);
    }

    /// Issue: the same two files kept getting re-downloaded from OpenSubtitles over and over, each a
    /// metered credit, while the log said `reason=suspect` every time — undifferentiated, and
    /// `cache=miss` even on the requests `remembered_failure` was gating. A remembered refusal must
    /// name the SAME specific reason a live failure would (not collapse to a generic verdict), so an
    /// incident reads the same whether this request spent a credit or answered from the negative
    /// cache.
    #[test]
    fn a_remembered_refusal_keeps_its_specific_reason() {
        use opensubtitles::DownloadError;

        let state = state("tag-roundtrip");
        let http = reqwest::Client::new();
        let cfg = userconfig::decode(state.config_keyring.as_ref(), &config_segment()).unwrap();
        let client = os_client(&state, &http, &cfg);

        // Backed off, not yet promoted: the remembered error still reports `no_cues`, not the
        // generic `suspect` a bare "1" marker used to force every remembered refusal into.
        remember_failure(
            &state,
            &client,
            11,
            &DownloadError::Suspect("subtitle link returned no cues".into()),
        );
        let remembered = remembered_failure(&state, &client, 11, false).expect("backed off");
        assert_eq!(remembered.reason_tag(), "no_cues", "{remembered:?}");

        // Promoted to the week-long `Gone` marker (a later occasion confirms it): the verdict is now
        // `gone` — the confirmed fact, not the shape of whichever occasion triggered the promotion.
        let long_ago = (unix_seconds() - SYNC_RETRY_TTL.as_secs() - 1).to_string();
        state.cache.put_mem(suspect_strike_key(11), long_ago, DEAD_FILE_TTL);
        remember_failure(&state, &client, 11, &DownloadError::Suspect("subtitle link 404".into()));
        let dead = remembered_failure(&state, &client, 11, false).expect("promoted");
        assert_eq!(dead.reason_tag(), "gone", "{dead:?}");
    }

    /// Before den-subtitles 0.20.0 stopped it, a repeat `sparse` verdict escalated the same way
    /// `no_cues`/`expired_link` still do, straight into `dead_file_key`'s week-long `Gone` marker —
    /// and its stored value is still the reason tag that triggered it, `sparse`. That OLD marker
    /// predates the relative-sparse rule too, so it is PURGED rather than trusted on sight: a file
    /// judged sparse under the old absolute floor can easily clear today's relative/lone one
    /// (Springfloden's three English files would otherwise sit refused for the rest of a week on a
    /// verdict nothing here still measures), and `remembered_failure` must not be the thing a fixed
    /// rule can never see past. Purging it (not merely bypassing it, which only `allow_sparse` did
    /// before) means even a STRICT request gets a fresh, current judgement.
    #[test]
    fn an_old_sparse_tagged_gone_marker_is_purged_not_trusted() {
        use opensubtitles::DownloadError;

        let state = state("old-gone-sparse-remembered");
        let http = reqwest::Client::new();
        let cfg = userconfig::decode(state.config_keyring.as_ref(), &config_segment()).unwrap();
        let client = os_client(&state, &http, &cfg);
        const FILE_ID: i64 = 3529838;

        // Simulates the marker a pre-0.20.0 escalation left behind — `remember_failure` never writes
        // this combination any more, but an existing one in the shared cache must still be handled.
        state.cache.put(dead_file_key(FILE_ID), "sparse".into(), DEAD_FILE_TTL);

        assert!(
            remembered_failure(&state, &client, FILE_ID, false).is_none(),
            "a strict request must not be blocked by a stale sparse verdict — it gets a fresh download"
        );
        assert!(
            state.cache.get(&dead_file_key(FILE_ID)).is_none(),
            "the stale marker must be purged, not merely bypassed, so it can't outlast this one read"
        );

        // A genuinely dead marker (any other tag) is unaffected: still `Gone`, still unbypassable.
        state.cache.put(dead_file_key(FILE_ID + 1), "gone".into(), DEAD_FILE_TTL);
        let still_dead = remembered_failure(&state, &client, FILE_ID + 1, true);
        assert!(matches!(still_dead, Some(DownloadError::Gone(_))), "{still_dead:?}");
    }

    /// The exact case that lost Fauda S1E3's English subtitles end to end: both of OpenSubtitles'
    /// English candidates are a real, correctly-timed, but DUBBED-release track (too sparse for its
    /// span) — the only shape the owner decided is "better than nothing" when it's all a language
    /// has. A STRICT request (no `allow_sparse`) must still refuse it so a non-sparse candidate
    /// always wins when one exists; the fallback pass (`allow_sparse`) must then serve the SAME file
    /// rather than being blocked by the backoff the strict refusal just remembered — that bypass
    /// (`remembered_failure`'s `allow_sparse && tag == "sparse"` check) is the one new piece of state
    /// this fix adds, and it is the thing a request-level test like this proves that a plain
    /// `Client::download` test cannot: that the SAME file_id, asked for twice moments apart, refuses
    /// then serves rather than refusing twice.
    #[tokio::test]
    async fn the_fauda_s1e3_english_track_refuses_strict_and_serves_as_a_fallback() {
        use opensubtitles::DownloadError;

        let state = state("fauda-s1e3-en");
        let http = reqwest::Client::new();
        let cfg = userconfig::decode(state.config_keyring.as_ref(), &config_segment()).unwrap();
        let client = os_client(&state, &http, &cfg);
        const FILE_ID: i64 = 3529838;

        // The strict pass refuses, and remembers the SPECIFIC reason — matching the shape OpenSubtitles'
        // own CDN would hand back for a dubbed-release track too sparse for its ~40-minute span.
        remember_failure(
            &state,
            &client,
            FILE_ID,
            &DownloadError::Suspect(
                "file 3529838 has too few cues for its span cues=200 span_s=2402 cues_per_min=5.0 max_gap_s=180"
                    .into(),
            ),
        );
        let strict = remembered_failure(&state, &client, FILE_ID, false);
        assert!(
            matches!(&strict, Some(DownloadError::Suspect(_))),
            "a strict request must still see the file as refused: {strict:?}"
        );
        assert_eq!(strict.unwrap().reason_tag(), "sparse");

        // The fallback pass is not blocked by the backoff the strict refusal just wrote — it is
        // exactly the request that backoff must let through.
        assert!(
            remembered_failure(&state, &client, FILE_ID, true).is_none(),
            "allow_sparse must bypass a remembered SPARSE backoff, not repeat the strict refusal"
        );

        // A truly broken file (no cues at all) must stay refused on the fallback pass too — the
        // owner's "keep refusing truly broken files" half of the same decision.
        remember_failure(
            &state,
            &client,
            FILE_ID + 1,
            &DownloadError::Suspect("subtitle link returned no cues".into()),
        );
        let still_refused = remembered_failure(&state, &client, FILE_ID + 1, true);
        assert!(
            matches!(&still_refused, Some(DownloadError::Suspect(_))),
            "a cue-less file must stay refused even on the fallback pass: {still_refused:?}"
        );
        assert_eq!(still_refused.unwrap().reason_tag(), "no_cues");

        // And repeating the SAME sparse file on a LATER occasion — backdating the strike past the
        // backoff window, the same way `a_failed_download_is_remembered_and_a_repeat_escalates`
        // confirms a genuine blip DOES escalate — never promotes it to the shared `Gone` verdict. A
        // sparse track is real content, not junk, and `Gone` would block the fallback just as hard
        // as the short backoff it would otherwise promote past.
        let long_ago = (unix_seconds() - SYNC_RETRY_TTL.as_secs() - 1).to_string();
        state.cache.put_mem(suspect_strike_key(FILE_ID), long_ago, DEAD_FILE_TTL);
        remember_failure(
            &state,
            &client,
            FILE_ID,
            &DownloadError::Suspect("file 3529838 has too few cues for its span cues=200".into()),
        );
        assert!(
            state.cache.get(&dead_file_key(FILE_ID)).is_none(),
            "a second occasion of the same SPARSE verdict must not escalate to the shared Gone marker"
        );
    }

    /// den-remux cannot tell a `sparse` refusal apart from any other failure by status code alone —
    /// both are a 502 — so the `X-Den-Degraded` header (the same convention every den addon uses) is
    /// what lets it retry with `?sparse=1` ONLY for the one reason that flag can actually rescue,
    /// rather than spending a second metered credit on a cue-less body or a transient blip that would
    /// refuse it again regardless.
    #[tokio::test]
    async fn a_refused_subtitle_names_its_reason_in_the_degraded_header() {
        const FILE_ID: i64 = 90001;
        let state = state("degraded-header");
        state.cache.put_mem(suspect_backoff_key(FILE_ID), "sparse".into(), SYNC_RETRY_TTL);

        let resp = handle_subtitle_file(
            &state,
            None,
            &HeaderMap::new(),
            &config_segment(),
            FILE_ID,
            None,
            None,
            None,
            None,
            None,
            false,
            false,
            None,
            false,
        )
        .await;
        assert_eq!(resp.status(), StatusCode::BAD_GATEWAY);
        assert_eq!(
            resp.headers().get(httputil::X_DEN_DEGRADED).unwrap(),
            "sparse",
            "a strict refusal must name its specific reason, not a generic one"
        );
    }

    /// The pin is what keeps a drifting source pick from spending a metered credit each time it
    /// drifts. It has to be scoped to the title alone — every language of one film translating from
    /// the same source, so a film costs one download rather than one per language — and removable,
    /// because a pin naming a file that will not download would otherwise be honoured
    /// forever: the ten-minute marker expires, the same pin is read, the same failure follows, and
    /// the title is stuck for every install and language, since the pin is scoped to neither.
    #[test]
    fn the_source_pin_is_per_title_and_can_be_dropped() {
        let state = state("pin");
        let film = source_pin_key("tt0111161", None, None);

        // A film and one of its episodes are different pins; so are two films.
        assert_ne!(film, source_pin_key("tt0111161", Some(1), Some(2)));
        assert_ne!(film, source_pin_key("tt0068646", None, None));
        // It holds a file id, and lives outside every namespace a body is read from.
        assert!(!BODY_PREFIXES.iter().any(|p| film.starts_with(p)), "a pin sits in a body namespace");

        state.cache.put(film.clone(), "77".into(), CACHE_TTL);
        assert_eq!(state.cache.get(&film).and_then(|v| v.parse::<i64>().ok()), Some(77));

        // Removable in both tiers — a dead pin has to actually go, not be written back with a
        // zero lifetime, which would be a blocking disk write to say "forget this".
        state.cache.remove(&film);
        assert_eq!(state.cache.get(&film), None, "a dropped pin was still honoured");
    }

    /// `.status` answers from the request alone — no search, no upstream call — because it is polled
    /// every second or so while the expensive thing runs. It reports what the run is doing, and it
    /// reports a remembered failure rather than leaving a client polling a job that will not start.
    #[tokio::test]
    async fn status_reports_the_run_without_touching_the_upstream() {
        let state = state("status");
        let config = config_segment();
        let cfg = userconfig::decode(state.config_keyring.as_ref(), &config).unwrap();
        let llm = cfg.llm.as_ref().unwrap();
        let job_key =
            translate_fail_key(&config, "tt0111161", None, None, &translate::canonical_lang("Swedish"), llm);

        let read = |resp: Response<Body>| async {
            let bytes = http_body_util::BodyExt::collect(resp.into_body()).await.unwrap().to_bytes();
            serde_json::from_slice::<Value>(&bytes).expect("status is json")
        };

        // Nothing running, nothing remembered.
        let resp = handle_translate_status(&state, None, &config, "tt0111161", "Swedish").await;
        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(read(resp).await["state"], "idle");

        // A run in progress reports how far it has got.
        let reporter = state.progress.start(&job_key, 1200);
        reporter.set(340, 1200);
        let body = read(handle_translate_status(&state, None, &config, "tt0111161", "Swedish").await).await;
        assert_eq!(body["state"], "working");
        assert_eq!(body["done"], 340);
        assert_eq!(body["total"], 1200);

        // Another language is a different job and must not read this one's progress.
        let other = read(handle_translate_status(&state, None, &config, "tt0111161", "Finnish").await).await;
        assert_eq!(other["state"], "idle", "a different language saw Swedish's progress");

        // However the run ends — including cancelled mid-await, which is what the guard is for — the
        // entry goes. A remembered failure is then reported as one, so a client stops waiting for a
        // run that is being backed off rather than polling a stale "working" forever.
        drop(reporter);
        state.cache.put(format!("{SYNCFAIL}{job_key}"), "1".into(), SYNC_RETRY_TTL);
        let body = read(handle_translate_status(&state, None, &config, "tt0111161", "Swedish").await).await;
        assert_eq!(body["state"], "failed");

        // Spellings of one language are one job here too, or a poll would never find its own run.
        let reporter = state.progress.start(&job_key, 2);
        reporter.set(1, 2);
        let by_code = read(handle_translate_status(&state, None, &config, "tt0111161", "sv").await).await;
        assert_eq!(by_code["state"], "working", "a code and a name polled different jobs");
    }

    /// An over-long language name is refused before it can become a filename or a prompt. The same
    /// file already bounds `videoHash` for the first reason; the second is that `lang` goes into
    /// every batch's prompt, so its length multiplies the bill against the viewer's own key.
    #[tokio::test]
    async fn an_over_long_language_is_refused() {
        let state = state("long-lang");
        let config = config_segment();
        let long = "x".repeat(MAX_LANG + 1);
        let resp = handle_translate(
            &state,
            None,
            &HeaderMap::new(),
            &config,
            "tt0111161",
            "",
            &long,
            false,
            None,
            None,
            None,
        )
        .await;
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST, "an over-long language was accepted");

        // Nothing was written for it — not the body key, and not the failure marker either.
        // `expect`, not `unwrap_or(0)`: a missing directory would otherwise score zero and pass this
        // assertion without ever testing anything.
        let store = state.cfg.cache_dir.join("store");
        let files = std::fs::read_dir(&store).expect("the store directory is created at boot").count();
        assert_eq!(files, 0, "an invalid request left {files} cache files behind");

        // A real language still works its way through to the upstream check.
        let resp = handle_translate(
            &state,
            None,
            &HeaderMap::new(),
            &config,
            "tt0111161",
            "",
            "Swedish",
            false,
            None,
            None,
            None,
        )
        .await;
        assert_eq!(resp.status(), StatusCode::BAD_GATEWAY, "a valid language must not be refused");
    }

    /// And the marker is scoped to the translation it belongs to: another language is a different
    /// job and must still be attempted.
    ///
    /// Proven by the response body, not by a marker the attempt leaves behind — a failed SEARCH no
    /// longer writes one, deliberately: a search costs nothing, and marking it made a moment's
    /// upstream trouble outlive itself by ten minutes. The body is what separates the two outcomes
    /// anyway, since both are a 502.
    #[tokio::test]
    async fn a_remembered_failure_does_not_block_a_different_translation() {
        let state = state("scoping");
        let config = config_segment();
        let cfg = userconfig::decode(state.config_keyring.as_ref(), &config).unwrap();
        let llm = cfg.llm.as_ref().unwrap();
        // Through the real builder and the real canonicalizer, so the keys the test addresses are
        // the keys the handler writes.
        let key_for = |lang: &str| {
            translate_fail_key(&config, "tt0111161", None, None, &translate::canonical_lang(lang), llm)
        };
        state.cache.put(format!("{SYNCFAIL}{}", key_for("Swedish")), "1".into(), SYNC_RETRY_TTL);

        let body_of = |resp: Response<Body>| async {
            let bytes = http_body_util::BodyExt::collect(resp.into_body()).await.unwrap().to_bytes();
            String::from_utf8_lossy(&bytes).into_owned()
        };

        // Finnish is a different job: it must get past the marker and fail on its own merits.
        let resp = handle_translate(
            &state,
            None,
            &HeaderMap::new(),
            &config,
            "tt0111161",
            "",
            "Finnish",
            false,
            None,
            None,
            None,
        )
        .await;
        assert_eq!(resp.status(), StatusCode::BAD_GATEWAY);
        let finnish = body_of(resp).await;
        assert!(
            !finnish.contains("translation_backoff"),
            "Finnish was short-circuited by Swedish's marker instead of being attempted: {finnish}"
        );

        // Swedish, the marked one, is still short-circuited.
        let resp = handle_translate(
            &state,
            None,
            &HeaderMap::new(),
            &config,
            "tt0111161",
            "",
            "Swedish",
            false,
            None,
            None,
            None,
        )
        .await;
        assert_eq!(resp.status(), StatusCode::BAD_GATEWAY);
        let swedish = body_of(resp).await;
        assert!(
            swedish.contains("translation_backoff"),
            "the marked language was attempted anyway: {swedish}"
        );
    }
}

#[cfg(test)]
mod decision_log_tests {
    use super::*;
    use crate::config::Config;
    use crate::state::AppState;

    const SRT: &str = "1\n00:00:01,000 --> 00:00:02,000\nhello\n\n2\n00:00:03,000 --> 00:00:04,000\nworld\n";

    /// A state carrying a real secret in its install config — never in `Config`/`AppState` itself,
    /// which hold only addon-level infrastructure, but the shape every real call site is built next
    /// to: `os_client`/`produce_translation` read the key out of a decoded `UserConfig`/`LlmConfig`
    /// right where these log lines are built.
    fn state_with_identity(log_identity: bool, name: &str) -> Arc<AppState> {
        let dir = std::env::temp_dir().join(format!("den-subs-decision-log-{name}"));
        let _ = std::fs::remove_dir_all(&dir);
        AppState::new(Config {
            port: 0,
            cache_dir: dir,
            cache_max_bytes: 1 << 20,
            public_base_url: None,
            alass: "alass".into(),
            ffmpeg: "ffmpeg".into(),
            config_key: String::new(),
            config_keys_prev: String::new(),
            metrics_token: String::new(),
            log_requests: false,
            log_identity,
            os_api_base: "http://127.0.0.1:1".into(),
            scout_origins: Vec::new(),
            scout_aliases: Vec::new(),
            revocation: Default::default(),
        })
    }

    fn ctx<'a>(imdb: &'a str, lang: &'a str) -> ServeCtx<'a> {
        ServeCtx {
            rid: Some("rid-abc123"),
            file_id: Some(42),
            imdb: Some(imdb),
            season: Some(1),
            episode: Some(2),
            lang: Some(lang),
        }
    }

    #[test]
    fn identity_fields_vanish_with_the_flag_off_but_everything_else_stays() {
        let on = state_with_identity(true, "on");
        let off = state_with_identity(false, "off");
        let line_on = serve_line(&on, ctx("tt1234567", "en"), "served", "tier1", None, "hit", Some(SRT));
        let line_off = serve_line(&off, ctx("tt1234567", "en"), "served", "tier1", None, "hit", Some(SRT));
        for field in ["file=os:42", "imdb=tt1234567", "season=1", "episode=2", "lang=en"] {
            assert!(line_on.contains(field), "identity on should carry {field}: {line_on}");
            assert!(!line_off.contains(field), "identity off must drop {field}: {line_off}");
        }
        // Non-identity fields survive the flag either way — `rid` included, since it is the join
        // key to the app's own line and not itself identifying.
        for field in
            ["event=serve", "outcome=served", "reason=tier1", "cache=hit", "cues=2", "rid=rid-abc123"]
        {
            assert!(line_on.contains(field), "{field} missing with identity on: {line_on}");
            assert!(line_off.contains(field), "{field} missing with identity off: {line_off}");
        }
    }

    /// The exact call shape `handle_subtitle_file`'s download-failure branch uses — a `DownloadError`
    /// classified into a stable tag, with the real OpenSubtitles upstream named. A real install's
    /// `opensubtitles_key` never appears here: `log_serve`'s only inputs are `ServeCtx`, a slug, and
    /// an optional borrowed body, none of which is (or could carry) a credential.
    #[test]
    fn a_key_never_reaches_the_serve_or_sync_or_translate_line() {
        let secret = "sk-do-not-log-this-secret";
        let state = state_with_identity(true, "key");
        let c = ctx("tt7777777", "en");

        let download_err =
            opensubtitles::DownloadError::Suspect("file 42 has too few cues for its span".into());
        let serve =
            serve_line(&state, c, "refused", download_err.reason_tag(), Some("opensubtitles"), "miss", None);
        assert!(!serve.contains(secret), "{serve}");

        let sync = log_sync(&state, c, "tier1", "reference_unavailable", Some(99));
        assert!(!sync.contains(secret), "{sync}");

        // A credential refusal's `message` is the PROVIDER's own text (never the key itself — see
        // `produce_translation`'s contract), but even if an upstream ever echoed something unusual
        // back, the function that builds this line takes no `LlmConfig`/`UserConfig` at all, so
        // there is no path for `secret` to reach it.
        let translate =
            log_translate(&state, c, "refused", &format!("credential_refused:{secret}"), Some("translator"));
        // This line is built FROM the given reason text, which is the one place a provider's own
        // message is interpolated — so it reaching the line is expected. What matters is that the
        // key lives nowhere else this function could have picked it up from: `ServeCtx` carries no
        // credential field, and `AppState`/`Config` hold none either.
        assert!(translate.contains(secret), "sanity: the explicit reason text is the only input");
        assert!(
            !format!("{c:?}", c = c).contains(secret),
            "ServeCtx itself must never be able to carry a credential"
        );
    }

    #[test]
    fn rank_lines_name_the_top_pick_and_its_factors() {
        let mut dubbed = opensubtitles::Subtitle {
            file_id: 555,
            lang: "en".into(),
            hash_match: true,
            downloads: 10,
            release: "Fauda.S01E01.DUBBED.WEB-DL".into(),
            hd: false,
            fps: 0.0,
            from_trusted: false,
            machine_translated: false,
            ai_translated: false,
            foreign_parts_only: false,
            hearing_impaired: false,
            ratings: 0.0,
        };
        let plain = opensubtitles::Subtitle { file_id: 556, release: String::new(), ..dubbed.clone() };
        dubbed.hash_match = true;
        let subs = vec![dubbed, plain];
        let lines = rank_lines(
            true,
            Some("rid-xyz"),
            "tt1111111",
            Some(1),
            Some(1),
            Some("Fauda.S01E01.DUBBED.WEB-DL.mkv"),
            &subs,
            Duration::from_millis(3),
        );
        // Both candidates are "en", so this is one language and one line.
        assert_eq!(lines.len(), 1, "{lines:?}");
        let line = &lines[0];
        assert!(line.contains("event=rank outcome=served"), "{line}");
        assert!(line.contains("candidates=2"), "{line}");
        assert!(line.contains("imdb=tt1111111"), "{line}");
        assert!(line.contains("lang=en"), "{line}");
        assert!(line.contains("top=os:555"), "{line}");
        assert!(line.contains("top_factors=") && line.contains("looks_dubbed"), "{line}");
        assert!(line.contains("next1=os:556"), "{line}");
        assert!(line.contains("rid=rid-xyz"), "{line}");
    }

    /// The bug this line exists to fix: a global "top" named whichever language sorted first
    /// alphabetically and said nothing about any other language a viewer might actually have asked
    /// for. Arabic sorting ahead of Swedish must not bury what Swedish got — each language gets its
    /// own line, with its own top file and the next 2 behind it.
    #[test]
    fn a_line_per_language_names_each_one_s_own_top_pick() {
        let base = opensubtitles::Subtitle {
            file_id: 0,
            lang: String::new(),
            hash_match: false,
            downloads: 0,
            release: String::new(),
            hd: false,
            fps: 0.0,
            from_trusted: false,
            machine_translated: false,
            ai_translated: false,
            foreign_parts_only: false,
            hearing_impaired: false,
            ratings: 0.0,
        };
        // Already in `rank`'s own order: grouped by language, ascending — Arabic before Swedish.
        let subs = vec![
            opensubtitles::Subtitle { file_id: 1, lang: "ar".into(), ..base.clone() },
            opensubtitles::Subtitle { file_id: 2, lang: "ar".into(), ..base.clone() },
            opensubtitles::Subtitle { file_id: 3, lang: "sv".into(), ..base.clone() },
        ];
        let lines = rank_lines(true, None, "tt3333333", None, None, None, &subs, Duration::from_millis(1));
        assert_eq!(lines.len(), 2, "one line per language: {lines:?}");

        let ar = &lines[0];
        assert!(ar.contains("lang=ar"), "{ar}");
        assert!(ar.contains("candidates=2"), "{ar}");
        assert!(ar.contains("top=os:1"), "{ar}");
        assert!(ar.contains("next1=os:2"), "{ar}");

        // Swedish gets its OWN line naming its OWN top file — not buried behind Arabic's.
        let sv = &lines[1];
        assert!(sv.contains("lang=sv"), "{sv}");
        assert!(sv.contains("candidates=1"), "{sv}");
        assert!(sv.contains("top=os:3"), "{sv}");
        assert!(!sv.contains("os:1") && !sv.contains("os:2"), "{sv}");
    }

    #[test]
    fn rank_lines_with_no_candidates_is_skipped_not_served() {
        let lines = rank_lines(true, None, "tt2222222", None, None, None, &[], Duration::from_millis(0));
        assert_eq!(lines.len(), 1, "{lines:?}");
        let line = &lines[0];
        assert!(line.contains("outcome=skipped"), "{line}");
        assert!(line.contains("reason=no_candidates"), "{line}");
        assert!(line.contains("candidates=0"), "{line}");
    }
}
