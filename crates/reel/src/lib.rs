//! den-reel — the whole trailer path for Den in one small binary:
//!
//!   1. ADDON (Den/Fusion protocol):  imdbId -> TMDB /videos (KinoCheck fallback) -> ytId
//!      GET /manifest.json                     -> addon manifest
//!      GET /meta/<movie|series>/<imdbId>.json -> { meta: { links:[{ trailers: <play url> }] } }
//!
//!   2. PLAYBACK (yt-dlp + ffmpeg proxy):  ytId -> App-Store-safe, seekable MP4
//!      GET /play/<id>.mp4  (or /play?v=<id>)  -> 200/206 video/mp4
//!      GET /direct/<id>.json                  -> the googlevideo URLs themselves (see `direct.rs`)
//!      GET /health                            -> 200 ok
//!
//! Extraction: yt-dlp rotates innertube clients that don't need a BotGuard poToken; ffmpeg muxes a
//! faststart H.264/AAC MP4, which we cache and PROXY.
//!
//! The proxy is for AVPlayer's sake, not the URL's. A googlevideo URL carries `ip=<this box>` inside
//! its signed `sparams`, which reads like a binding to this server — but Google does not enforce it,
//! and `/direct` hands that same URL to a browser, which plays it. What the Apple TV cannot do is
//! take a video stream plus a separate audio one, which is what YouTube now answers with.

mod addon;
mod client;
mod config;
mod crop;
mod direct;
mod hls;
mod httputil;
mod media_gate;
mod play;
mod prepare;
mod progressive;
mod sign;
mod sources;
mod state;
mod upstream;
mod userconfig;
mod worker;
mod ytdlp;

#[cfg(feature = "bench")]
pub mod bench_api;

#[cfg(test)]
mod tests;

use hyper::{Request, Response, StatusCode};

pub use crate::config::Config;
use crate::httputil::query_param;
pub use crate::httputil::Body;
pub use crate::state::AppState;
pub use crate::userconfig::is_valid as is_valid_config;
pub use crate::ytdlp::kill_live_groups;

/// The unconfigured manifest, for the combined Den addon's manifest to take Reel's resource from.
pub fn unconfigured_manifest() -> serde_json::Value {
    addon::manifest(None)
}
use std::sync::{Arc, OnceLock};

/// The release the image carries, from `DEN_VERSION` (set by the Dockerfile from the repo's `VERSION` file).
/// Keeping it out of Cargo.toml means a release does not change the manifest the dependency layer is keyed on,
/// so a version bump recompiles nothing. A developer build or a test has no image metadata and falls back to
/// the package version.
pub fn release_version() -> &'static str {
    static VERSION: OnceLock<String> = OnceLock::new();
    VERSION
        .get_or_init(|| {
            std::env::var("DEN_VERSION")
                .ok()
                .filter(|version| !version.is_empty())
                .unwrap_or_else(|| env!("CARGO_PKG_VERSION").to_owned())
        })
        .as_str()
}

pub const MAX_PROBE: usize = 6; // cap how many trailer candidates we validate per movie
pub const SEARCH_MAX: usize = 4; // YouTube-search fallback: how many results to consider (then probe)
                                 // Strictly below DOWNLOAD_CONCURRENCY, and that relationship is the point: the prewarm permit and
                                 // the download permit are different semaphores, so equal caps let three speculative prewarms take
                                 // every download permit and — the semaphore being FIFO-fair — park the /play the viewer is actually
                                 // waiting on behind them, for up to DOWNLOAD_TIMEOUT_SECS. One permit stays reserved for real work.
pub const PREWARM_MAX: usize = 2;
pub const YT_TTL_MS: u64 = 24 * 60 * 60 * 1000;
pub const YT_NEG_TTL_MS: u64 = 60 * 60 * 1000; // "nothing playable" caches shorter (geo/transient may lift)
                                               // A lookup that FAILED, rather than one that answered "nothing": long enough to stop a browse from
                                               // stampeding a sick upstream, short enough that a recovery is visible in about a minute.
pub const YT_FAIL_TTL_MS: u64 = 60 * 1000;
// How long PAST its normal expiry a known-good answer may keep standing in for a failing lookup,
// measured from when it was last CONFIRMED (re-serving rewrites the expiry, so that is the only
// clock that still means anything). Serving the last answer beats serving none during an outage,
// but a trailer that was REMOVED upstream has to stop being handed out eventually — and /meta lets a
// client keep it for up to a week, so "eventually" cannot mean "while anything is still faulting".
pub const STALE_GRACE_MS: u64 = 24 * 60 * 60 * 1000;
const _: () =
    assert!(YT_FAIL_TTL_MS < YT_NEG_TTL_MS, "a failure must be re-asked sooner than a real 'no trailer'");
pub const YT_CACHE_MAX: usize = 10_000; // sweep expired entries once the resolve cache grows past this
pub const CROP_CACHE_MAX: usize = 10_000; // bound the crop-report cache the same way
                                          // Bound the /play failure cache. Far smaller than the two above on purpose: those hold answers worth
                                          // keeping, this holds a reason to not re-spawn yt-dlp for a minute or six hours, every entry expires
                                          // on its own, and losing one costs exactly one repeated download. A homelab sees a few hundred
                                          // distinct trailers, so 512 covers the working set without reserving memory for a library.
pub const PLAY_FAIL_MAX: usize = 512;
// Same shape, same reasoning, for the ids cropdetect could not read. A vid and a timestamp each.
pub const CROP_UNKNOWN_MAX: usize = 512;
// How long an unreadable crop stands before ffmpeg is spent on it again. The cached MP4 is immutable,
// so a second pass over the same bytes usually reaches the same nothing — but the other way to land
// here is a cropdetect that timed out under load, and that deserves another go before long.
pub const CROP_UNKNOWN_TTL_MS: u64 = 10 * 60 * 1000;
pub const DOWNLOAD_CONCURRENCY: usize = 3; // global cap on concurrent yt-dlp downloads (bounds CPU/disk/fd)
const _: () =
    assert!(PREWARM_MAX < DOWNLOAD_CONCURRENCY, "prewarm must leave a download permit for a real /play");
// How long every new extraction waits after YouTube throttles this box (a 429, a bot check), doubling
// per consecutive throttle up to the cap. Such a throttle is about the box's address, not the video, so
// it pauses all of them: a per-id cooldown let every new id spawn another yt-dlp into it.
pub const YOUTUBE_PAUSE_BASE_MS: u64 = 5 * 60 * 1000;
pub const YOUTUBE_PAUSE_CAP_MS: u64 = 3 * 60 * 60 * 1000;
pub const PROBE_CONCURRENCY: usize = 6; // global cap on concurrent yt-dlp --simulate probes
                                        // Cap on DISTINCT ids with a download outstanding. `download_sem` bounds how many run at once, but
                                        // the permit is taken inside `download_cached` — so every new id got a map entry and a spawned
                                        // driver that could sit queued for up to DOWNLOAD_TIMEOUT_SECS. That is request-driven growth: on an
                                        // instance without PLAY_SECRET, anyone who can reach /play can add to it by asking for ids that
                                        // are merely well-formed.
                                        //
                                        // Sized by what the queue can plausibly SERVE, not just by memory. The wait is un-timed — only the
                                        // yt-dlp run itself has a timeout — so a queue this deep is also a promise: at three at a time and a
                                        // 240s worst case per download, 24 drains in about half an hour, where 64 would take an hour and a
                                        // half of clients waiting on requests that will mostly have been abandoned. Still many times what
                                        // legitimate use puts in flight (three downloading, two prewarming, a few waiting their turn).
pub const IN_FLIGHT_MAX: usize = 24;
const _: () = assert!(
    IN_FLIGHT_MAX > DOWNLOAD_CONCURRENCY + PREWARM_MAX,
    "the queue has to be able to hold everything that may legitimately be running at once"
);

/// The /configure page, embedded so the binary is self-contained (seals a BYOK TMDB key into the URL).
const CONFIGURE_PAGE: &str = include_str!("configure.html");

/// A YouTube id as it appears in a /play path or `?v=`: `[A-Za-z0-9_-]{11}`.
///
/// Exactly 11, not a 6..=15 window. A YouTube video id is a base64url-encoded 64-bit value and has
/// been 11 characters for the life of the service. This is the ONLY gate on /play and /crop — both
/// of which spend a download permit and a yt-dlp process on whatever they are handed — and it also
/// decides what the cache sweep considers a published trailer, so the loosest thing that still
/// accepts every real id is the right thing.
pub fn is_valid_vid(id: &str) -> bool {
    id.len() == 11 && id.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
}

/// Log a failure at most once a minute per `condition`, saying how many were held back since the last
/// line (`den_http::log::LimitedLog`). `line` is only built when it is written.
pub fn log_limited(condition: &str, line: impl FnOnce() -> String) {
    static LIMITED: den_http::log::LimitedLog = den_http::log::LimitedLog::new();
    LIMITED.log(condition, line);
}

/// Consecutive hard upstream faults before /health reports `degraded` (ADDON-02).
const HEALTH_FAIL_THRESHOLD: u32 = 3;

/// Build the /health JSON body (ADDON-02). Pure so the branch logic is unit-testable without app
/// state: `degraded` when trailers can't work at all (no server TMDB key AND no sealed-config keyring,
/// so no install can supply one), or when TMDB has been failing
/// (>= HEALTH_FAIL_THRESHOLD consecutive hard faults); otherwise `ok`. `/health` is addon-level (no
/// per-install config), so a keyring being present is enough to consider trailers workable.
fn health_body(
    tmdb_available: bool,
    recent_failures: u32,
    extract_fails: u32,
    local_fails: u32,
    youtube_paused_ms: Option<u64>,
) -> serde_json::Value {
    let paused = youtube_paused_ms.is_some();
    match health_verdict(tmdb_available, recent_failures, extract_fails, local_fails, paused) {
        Some((reason, detail)) => {
            let mut body = serde_json::json!({"status": "degraded", "reason": reason, "detail": detail});
            // A throttle lifts on its own, so say when — the one verdict here with a known end.
            if let (true, Some(ms)) = (reason == "youtube_throttled", youtube_paused_ms) {
                body["retry_after_s"] = ms.div_ceil(1000).into();
            }
            body
        }
        None => serde_json::json!({"status": "ok"}),
    }
}

/// `CONFIG_KEY` is set but malformed: the verdict that wins over every other, since no install config
/// can be trusted until it is fixed.
pub(crate) const CONFIG_KEY_INVALID: (&str, &str) = (
    "config_key_invalid",
    "CONFIG_KEY is set but not a valid base64 32-byte key — install configs are refused until it is fixed",
);

/// The 503 an install-config request gets while `CONFIG_KEY` is malformed. Retry-After is a hint to
/// clients, not a promise: only an operator fixing the variable and restarting clears it.
fn config_key_invalid_response() -> Response<Body> {
    httputil::json(
        StatusCode::SERVICE_UNAVAILABLE,
        &serde_json::json!({"error": "config_key_invalid"}),
        &[("cache-control", "no-store"), ("retry-after", "60")],
    )
}

/// The /health verdict: `None` when ok, else its reason and detail. Shared by the body above and by
/// the line `AppState::note_health` writes when the verdict changes.
fn health_verdict(
    tmdb_available: bool,
    recent_failures: u32,
    extract_fails: u32,
    local_fails: u32,
    youtube_paused: bool,
) -> Option<(&'static str, &'static str)> {
    if !tmdb_available {
        Some(("tmdb_key_missing", "set CONFIG_KEY (per-install BYOK) or TMDB_KEY"))
    } else if recent_failures >= HEALTH_FAIL_THRESHOLD {
        Some(("upstream_unavailable", "TMDB has been failing"))
    } else if youtube_paused {
        // Ahead of `extractor_unavailable` because the advice differs: bumping yt-dlp does nothing
        // for a throttle on this box's address. It lifts on its own, and a success ends it early.
        Some((
            "youtube_throttled",
            "YouTube is throttling this server (429 / bot check) — extractions are paused until it lifts",
        ))
    } else if extract_fails >= HEALTH_FAIL_THRESHOLD {
        // Trailers resolve upstream but yt-dlp can't extract any of them here — YouTube BotGuard or a
        // stale yt-dlp / broken nsig-JS runtime. Bumping YTDLP_VERSION is the fix that usually works,
        // and is named FIRST on purpose: pinning a client with YTDLP_PLAYER_CLIENTS is the advice that
        // produced a dead client name silently degrading extraction for who knows how long.
        Some((
            "extractor_unavailable",
            "yt-dlp can't extract YouTube here — bump YTDLP_VERSION first; pin YTDLP_PLAYER_CLIENTS only as a stopgap",
        ))
    } else if local_fails >= HEALTH_FAIL_THRESHOLD {
        // Downloads are failing for a reason that is ours, not YouTube's — no output file, or a
        // clap bake killed part-way. Named separately because "bump yt-dlp" is the wrong advice.
        Some((
            "downloads_failing",
            "yt-dlp extracts fine but no trailer file is being produced — check the cache volume and MP4Box",
        ))
    } else {
        None
    }
}

/// The `/metrics` body in the Prometheus text exposition format, written by hand: the format is a
/// few dozen lines of printing, and a client library would be a dependency for it. Every number is
/// either an atomic load or the length of a map we hold briefly — no I/O, no directory walk — and
/// none of it is computed until a scrape asks.
fn metrics_body(state: &AppState) -> String {
    use std::sync::atomic::Ordering::Relaxed;
    fn len<K, V>(m: &std::sync::Mutex<std::collections::HashMap<K, V>>) -> u64 {
        m.lock().unwrap_or_else(|e| e.into_inner()).len() as u64
    }
    let mut b = String::with_capacity(4096);
    // One HELP/TYPE block and its samples; an empty label set is an unlabelled series. Every series
    // here is a gauge — a level read now. Even the failure counts go down: a success resets them.
    let mut gauge = |name: &str, help: &str, samples: &[(&str, u64)]| {
        den_http::prom::family(&mut b, name, "gauge", help, samples)
    };
    let trailer_bytes = state.cache_trailer_bytes.load(Relaxed);
    let scratch_bytes = state.cache_scratch_bytes.load(Relaxed);
    let cap = state.cfg.cache_max_bytes;

    gauge(
        "reel_build_info",
        "The running build.",
        &[(format!("version=\"{}\"", release_version()).as_str(), 1)],
    );
    // The cache figures come from the last eviction pass, not from this request. 0 means no pass has
    // run yet, which on a fresh process lasts until the first download or the first hourly tick.
    gauge(
        "reel_cache_measured_at_seconds",
        "When the eviction pass last measured the cache volume, unix seconds (0 = not yet).",
        &[("", state.cache_measured_at.load(Relaxed) / 1000)],
    );
    gauge(
        "reel_cache_trailers",
        "Trailers on the cache volume.",
        &[("", state.cache_trailer_count.load(Relaxed))],
    );
    gauge("reel_cache_trailer_bytes", "Bytes of trailers on the cache volume.", &[("", trailer_bytes)]);
    gauge(
        "reel_cache_scratch_bytes",
        "Bytes of partial downloads and other scratch, counted against the cap.",
        &[("", scratch_bytes)],
    );
    gauge("reel_cache_max_bytes", "The cache size cap (CACHE_MAX_BYTES).", &[("", cap)]);
    // What a new trailer can still take. Scratch counts against the cap, so this is the number that
    // actually decides whether the next download evicts something.
    gauge(
        "reel_cache_free_bytes",
        "What the cap leaves after trailers and scratch.",
        &[("", cap.saturating_sub(trailer_bytes + scratch_bytes))],
    );
    gauge(
        "reel_downloads_in_flight",
        "Distinct ids with a download outstanding.",
        &[("", len(&state.in_flight))],
    );
    gauge(
        "reel_downloads_in_flight_max",
        "Outstanding ids past which /play answers 503 busy.",
        &[("", IN_FLIGHT_MAX as u64)],
    );
    gauge("reel_downloads_max", "Downloads that may run at once.", &[("", DOWNLOAD_CONCURRENCY as u64)]);
    gauge(
        "reel_direct_media_active",
        "Direct media requests currently holding a streaming permit.",
        &[("", state.media_gate.active())],
    );
    gauge(
        "reel_direct_media_high_water",
        "Largest number of simultaneous direct media responses since process start.",
        &[("", state.media_gate.high_water())],
    );
    gauge(
        "reel_direct_media_max",
        "Direct media responses allowed at once.",
        &[("", state.media_gate.limit() as u64)],
    );
    gauge(
        "reel_index_build_active",
        "Progressive index builds currently holding build admission.",
        &[(
            "",
            crate::progressive::INDEX_BUILD_CONCURRENCY
                .saturating_sub(state.index_build_sem.available_permits()) as u64,
        )],
    );
    gauge(
        "reel_prewarm_permits_available",
        "Speculative downloads that could start now.",
        &[("", state.prewarm_sem.available_permits() as u64)],
    );
    gauge(
        "reel_probe_permits_available",
        "yt-dlp probes or searches that could start now.",
        &[("", state.probe_sem.available_permits() as u64)],
    );
    gauge("reel_resolve_cache_entries", "Titles in the resolve cache.", &[("", len(&state.yt_cache))]);
    gauge(
        "reel_resolve_cache_max",
        "Resolve cache size past which expired entries are swept.",
        &[("", YT_CACHE_MAX as u64)],
    );
    gauge("reel_crop_cache_entries", "Crop reports cached.", &[("", len(&state.crop_cache))]);
    gauge(
        "reel_crop_unreadable_entries",
        "Ids cropdetect could not read, waiting out their retry.",
        &[("", len(&state.crop_unknown))],
    );
    // Ids /play has found unplayable and is not re-extracting yet.
    gauge(
        "reel_play_failure_cache_entries",
        "Ids /play is refusing to re-extract yet.",
        &[("", len(&state.play_fails))],
    );
    gauge("reel_play_failure_cache_max", "Bound on the /play failure cache.", &[("", PLAY_FAIL_MAX as u64)]);
    // The same three counters /health turns into a one-word verdict.
    gauge(
        "reel_consecutive_failures",
        "Consecutive failures by kind; /health reports degraded at 3.",
        &[
            ("kind=\"upstream\"", state.upstream.recent_failures() as u64),
            ("kind=\"extract\"", state.extract_fails.load(Relaxed) as u64),
            ("kind=\"local\"", state.local_fails.load(Relaxed) as u64),
        ],
    );
    // What the web's trailers cost: counts only rise, so these are counters.
    let mut counter = |name: &str, help: &str, samples: &[(&str, u64)]| {
        den_http::prom::family(&mut b, name, "counter", help, samples)
    };
    let (video, audio) = (&state.index_video, &state.index_audio);
    counter(
        "reel_index_builds_total",
        "/progressive indexes built, by the streams they index.",
        &[
            ("streams=\"video\"", video.count.load(Relaxed)),
            ("streams=\"video+audio\"", audio.count.load(Relaxed)),
        ],
    );
    counter(
        "reel_index_build_milliseconds_total",
        "Time spent building them; divide by the builds for the mean.",
        &[
            ("streams=\"video\"", video.total_ms.load(Relaxed)),
            ("streams=\"video+audio\"", audio.total_ms.load(Relaxed)),
        ],
    );
    counter(
        "reel_index_build_failures_total",
        "Index builds that failed.",
        &[("", state.index_failures.load(Relaxed))],
    );
    counter(
        "reel_index_build_refused_total",
        "Progressive index builds refused because build admission was full.",
        &[("", state.index_build_refused.load(Relaxed))],
    );
    counter(
        "reel_index_ranges_retried_total",
        "Ranges googlevideo refused for the moment (401, 429, 5xx) and an index build asked for again.",
        &[("", crate::progressive::RANGES_RETRIED.load(Relaxed))],
    );
    counter(
        "reel_direct_media_refused_total",
        "Direct media requests refused because every streaming permit was held.",
        &[("", state.media_gate.refused())],
    );
    counter(
        "reel_direct_media_timeouts_total",
        "Direct media bodies stopped by the configured deadline.",
        &[
            ("kind=\"idle\"", state.media_gate.idle_timeouts()),
            ("kind=\"lifetime\"", state.media_gate.lifetime_timeouts()),
        ],
    );
    counter(
        "reel_direct_media_cancellations_total",
        "Direct media bodies dropped by the downstream before completion.",
        &[("", state.media_gate.cancellations())],
    );
    counter(
        "reel_index_requests_total",
        "Requests for an index: found built, or waited for its build.",
        &[
            ("index=\"built\"", state.index_hits.load(Relaxed)),
            ("index=\"waited\"", state.index_waits.load(Relaxed)),
        ],
    );
    counter(
        "reel_resolves_total",
        "Resolves that ran, whichever resolver answered.",
        &[("", state.resolves.count.load(Relaxed))],
    );
    let resolver = &state.resolver;
    counter(
        "reel_resolver_answers_total",
        "Resolves that produced an answer, by the resolver that did.",
        &[
            ("resolver=\"youtubei\"", resolver.youtubei_answers.load(Relaxed)),
            ("resolver=\"ytdlp\"", resolver.ytdlp_answers.load(Relaxed)),
        ],
    );
    counter(
        "reel_youtubei_fallbacks_total",
        "Resolves the youtubei.js resolver handed to yt-dlp: it failed for the video, could not be asked, or answered with nothing usable.",
        &[
            ("reason=\"error\"", resolver.fallback_error.load(Relaxed)),
            ("reason=\"unavailable\"", resolver.fallback_unavailable.load(Relaxed)),
            ("reason=\"unusable\"", resolver.fallback_unusable.load(Relaxed)),
        ],
    );
    counter(
        "reel_resolve_milliseconds_total",
        "Time spent resolving, once each had its permit.",
        &[("", state.resolves.total_ms.load(Relaxed))],
    );
    // The last of each, for a glance without a second scrape to take a difference over.
    let mut gauge = |name: &str, help: &str, samples: &[(&str, u64)]| {
        den_http::prom::family(&mut b, name, "gauge", help, samples)
    };
    gauge(
        "reel_last_milliseconds",
        "How long the last one took.",
        &[
            ("of=\"index_video\"", video.last_ms.load(Relaxed)),
            ("of=\"index_video+audio\"", audio.last_ms.load(Relaxed)),
            ("of=\"resolve\"", state.resolves.last_ms.load(Relaxed)),
        ],
    );
    b
}

// Generic over the request body: this handler routes on path/query only and discards the body, so tests
// can drive it with a `Request<()>` while `run()` passes the real `Request<Incoming>`.
pub async fn handle_request<B>(state: Arc<AppState>, req: Request<B>) -> Response<Body> {
    let start = std::time::Instant::now();
    let log = state.cfg.log_requests;
    let (parts, _body) = req.into_parts();
    let mut resp = if parts.method == hyper::Method::OPTIONS {
        // CORS preflight, on any path: everything here is credential-free, and a browser-based
        // client asks before it sends anything with a header of its own.
        den_http::preflight()
    } else if !matches!(parts.method, hyper::Method::GET | hyper::Method::HEAD) {
        // Every route here is a read. Routing on the path alone meant a POST to /play started a
        // download like a GET would.
        httputil::json(
            StatusCode::METHOD_NOT_ALLOWED,
            &serde_json::json!({"error": "method_not_allowed"}),
            &[("allow", "GET, HEAD, OPTIONS"), ("cache-control", "no-store")],
        )
    } else {
        // Set by hearth on a request it routed under `/den`; no client can put one here.
        let den = parts.extensions.get::<Arc<den_seal::Addon>>().cloned();
        let resp = route(state, den.as_deref(), &parts).await;
        // Honor a conditional GET/HEAD: any cacheable 200 carries an ETag, so an `If-None-Match` hit
        // collapses to a 304 (a no-op for unsafe methods, errors, and `no-store` bodies).
        httputil::apply_conditional(&parts.method, &parts.headers, resp)
    };
    // Stamped here rather than in each builder, so no response can go out without it — the video,
    // the page, a plain-text error and a 304 included. A browser that cannot read an error body
    // reports a CORS failure instead of the error.
    //
    // The debug headers readable too: Server-Timing and X-Den-Degraded. Retry-After as well, or a browser client
    // cannot honour the cooldown a refusal carries, and the validators, or it cannot make the conditional and
    // If-Range requests they exist for. Content-Range and Accept-Ranges, or a Media Source player fetching parts
    // cannot learn a file's size or that it may seek.
    den_http::cors_stamp(
        resp.headers_mut(),
        "Server-Timing, X-Den-Degraded, Retry-After, ETag, Last-Modified, Content-Range, Accept-Ranges",
    );
    // What a shared cache may keep is named, so the deploy that changes it can drop it by name: den's
    // `den-cloudflare-purge reel` runs after every reel release. Cloudflare removes the header before a
    // browser sees the answer.
    let shared = resp
        .headers()
        .get(hyper::header::CACHE_CONTROL)
        .and_then(|v| v.to_str().ok())
        .is_some_and(|cc| cc.starts_with("public"));
    if shared {
        resp.headers_mut().insert("cache-tag", hyper::header::HeaderValue::from_static("reel"));
    }
    // Time to headers: a streamed /play body is still being written when this runs.
    let elapsed = start.elapsed();
    let resp = den_http::with_total_timing(resp, elapsed);
    if log {
        eprintln!("{}", request_line(&parts, resp.status(), elapsed));
    }
    resp
}

/// `<METHOD> <redacted path> <status> <ms>ms[ <sources ask>][ rid=<id>]`. The `rid` is the caller's `X-Request-Id`,
/// so a line here can be matched to the one the app logged for the same request.
fn request_line(
    parts: &hyper::http::request::Parts,
    status: StatusCode,
    elapsed: std::time::Duration,
) -> String {
    let mut line = den_http::log::request_head(
        parts.method.as_str(),
        &redact_path(parts.uri.path()),
        status.as_u16(),
        elapsed.as_millis(),
    );
    if let Some(ask) = sources_ask(&parts.uri) {
        line.push(' ');
        line.push_str(&ask);
    }
    den_http::log::push_rid(&mut line, den_http::log::request_id(&parts.headers).as_deref());
    line
}

/// What a `/sources` request asked: its surface, its player, and whether it was a warm-up. The same path answers a
/// detail page opening and a press on a title link, and only these tell the two apart. Each is written only as one of
/// its known values, so nothing a caller wrote reaches the log — and `s`, `i` and `e` never do.
fn sources_ask(uri: &hyper::Uri) -> Option<String> {
    let prepare = uri.path().starts_with("/prepare/") || uri.path().contains("/prepare/");
    if !uri.path().starts_with("/sources/") && !prepare {
        return None;
    }
    let query = uri.query().unwrap_or("");
    let known = |key: &str, values: [&'static str; 2]| {
        query_param(query, key).and_then(|v| values.into_iter().find(|known| *known == v)).unwrap_or("?")
    };
    let mut ask = format!(
        "{}surface={} player={}",
        if prepare { "prepare " } else { "" },
        known("surface", ["silent", "audible"]),
        known("player", ["native", "hls.js"])
    );
    if query_param(query, "intent").as_deref() == Some("warm") {
        ask.push_str(" intent=warm");
    }
    Some(ask)
}

/// The request path as the request log may show it. The query string never gets this far — `?s=`
/// is a play signature — and a per-install config segment carries a BYOK key, sealed or not, so any
/// first segment that is not one of our own routes is written as `<config>`. That covers
/// `/<config>/manifest.json` and `/<config>/meta/…`, and also a client probing `/<config>/configure`
/// or pasting the bare segment, which would otherwise put the key in the log through a 404. Extra leading
/// slashes are skipped, so `//<config>/…` and `//m/s/<cap>` are redacted as their one-slash forms are.
fn redact_path(path: &str) -> std::borrow::Cow<'_, str> {
    let rest = path.trim_start_matches('/');
    // Whatever follows `m`, in any case, is a capability or a mangled try at one. Only `n` and `s` are filed
    // routes, and the rest of the path is never shown.
    let (first, after) = rest.split_once('/').unwrap_or((rest, ""));
    if first.eq_ignore_ascii_case("m") {
        if after.is_empty() {
            return "/m".into();
        }
        return match (first, after.split_once('/')) {
            ("m", Some((filed @ ("n" | "s"), _))) => format!("/m/{filed}/<cap>").into(),
            _ => "/m/<cap>".into(),
        };
    }
    if rest.starts_with("_internal/validate/m/s/") {
        return "/_internal/validate/m/s/<cap>".into();
    }
    const ROUTES: [&str; 14] = [
        "health",
        "metrics",
        "manifest.json",
        "configure",
        "config-key",
        "meta",
        "prepare",
        "crop",
        "play",
        "direct",
        "progressive",
        "hls",
        "seg",
        "sources",
    ];
    den_http::log::redact_first_segment(path, &ROUTES)
}

/// `den` is the combined Den addon's terms when hearth routed the request under `/den`: its config and the
/// install-bound links minted from it are read on those terms (`userconfig::Terms`), never on Reel's own.
async fn route(
    state: Arc<AppState>,
    den: Option<&den_seal::Addon>,
    parts: &hyper::http::request::Parts,
) -> Response<Body> {
    let path = parts.uri.path();
    let query = parts.uri.query().unwrap_or("");
    // Held apart from `state`, which the handlers below take by value.
    let own = state.clone();
    let terms = userconfig::Terms::of(&own, den);

    if path == "/health" {
        // Standard Den addon health (ADDON-02): 200 for liveness, `degraded` when trailers can't work —
        // no server TMDB key AND no sealed-config keyring, or TMDB failing. KinoCheck is a
        // fallback: its outage does not mean trailers are broken, so it does not move this.
        let (tmdb_available, recent_failures, extract_fails, local_fails, paused) = state.health_inputs();
        let body = if state.config_key_invalid {
            let (reason, detail) = CONFIG_KEY_INVALID;
            serde_json::json!({"status": "degraded", "reason": reason, "detail": detail})
        } else {
            health_body(tmdb_available, recent_failures, extract_fails, local_fails, paused)
        };
        return httputil::json(StatusCode::OK, &body, &[("cache-control", "no-store")]);
    }
    // Operational detail /health has no room for. /health answers one question — can this instance
    // serve trailers — and answers it in three words; everything behind that verdict (how full the
    // volume is, how many downloads are in flight, how much of the resolve cache is standing) was
    // visible only by reading logs.
    //
    // Deliberately does NOT walk the cache directory: the figures come from the eviction pass, which
    // already walks it after every download and once an hour. An ops endpoint that stats a few
    // thousand files per request is a way to make a busy box busier.
    //
    // Behind a bearer token, and OFF when none is configured. In-flight downloads and cache
    // occupancy polled over time are a timeline of when the household is watching, which /health
    // does not give away. A refusal is the same 404 an unknown path gets, with or without a token
    // configured, so nobody is told there is something here to poke at.
    if path == "/metrics" {
        if !den_http::metrics_authorized(&parts.headers, state.cfg.metrics_token.as_deref()) {
            return httputil::not_found();
        }
        let body = metrics_body(&state);
        return Response::builder()
            .status(StatusCode::OK)
            .header("content-type", "text/plain; version=0.0.4; charset=utf-8")
            .header("content-length", body.len())
            .header("cache-control", "no-store")
            .body(httputil::full(body))
            .unwrap();
    }
    // Cheap capability validation for the LAN relay. It proves that an `/m/s` URL is a currently
    // valid signed bearer capability without resolving a video or opening an upstream media body.
    // The capability itself is the authority: there is deliberately no member/session check here.
    if let Some(blob) = path.strip_prefix("/_internal/validate/m/s/") {
        return sources::validate_direct_capability(&state, den, blob, query);
    }
    if path == "/manifest.json" {
        return httputil::json(
            StatusCode::OK,
            &addon::manifest(None),
            &[("cache-control", "public, max-age=3600, stale-while-revalidate=600")],
        );
    }
    // The /configure UI seals a BYOK TMDB key into the install URL (den-scout/docs/SEALED-CONFIG.md).
    if path == "/" || path == "/configure" || path == "/configure/" {
        return httputil::html(
            StatusCode::OK,
            CONFIGURE_PAGE,
            &[("cache-control", "public, max-age=3600, stale-while-revalidate=600")],
        );
    }
    // The current X25519 public key (base64) so /configure can seal the config to it; 404 when sealed
    // configs are disabled (no key) — the page then keeps plaintext. Five minutes, not an hour: the key
    // rotates, and the ETag lets a revalidation after that cost nothing. Both answers carry
    // CONFIG_EPOCH, which the page stamps into every link it builds, sealed or not: a link stamped
    // below it would be refused the moment it was built.
    if path == "/config-key" {
        // A malformed key has no public half to hand out; /configure then builds plaintext links, which
        // are refused like every other config until the key is fixed.
        if state.config_key_invalid {
            return config_key_invalid_response();
        }
        let epoch = state.cfg.revocation.epoch();
        return match state.config_keyring.as_ref().map(|kr| kr.current_pub_base64()) {
            Some(k) if !k.is_empty() => httputil::json(
                StatusCode::OK,
                &serde_json::json!({"key": k, "epoch": epoch}),
                &[("cache-control", "public, max-age=300")],
            ),
            _ => httputil::json(
                StatusCode::NOT_FOUND,
                &serde_json::json!({"error": "no_key", "epoch": epoch}),
                &[("cache-control", "no-store")],
            ),
        };
    }

    // Legacy config-less discovery: /meta/(movie|series)/(.+).json — resolves with the env TMDB key.
    if let Some(rest) = path.strip_prefix("/meta/") {
        if let Some(resp) = meta_from_rest(&state, &parts.headers, den, None, rest, query).await {
            return resp;
        }
    }
    // Combined discovery + source ladder for latency-sensitive web clients. This mints capabilities
    // but does not activate any listener; the edge that observed the client owns that decision.
    if let Some(rest) = path.strip_prefix("/prepare/") {
        if let Some(resp) = prepare_from_rest(&state, &parts.headers, den, None, rest, query).await {
            return resp;
        }
    }

    // Config-scoped discovery: /<config>/manifest.json and /<config>/meta/(movie|series)/(.+).json,
    // where <config> carries a BYOK TMDB key (sealed or legacy plaintext). The app pastes the manifest
    // URL; Stremio then derives the /meta calls from the same base. Fail CLOSED on a bad config.
    if let Some((cfg_seg, rest)) = path.strip_prefix('/').and_then(|p| p.split_once('/')) {
        if rest == "manifest.json" || rest.starts_with("meta/") || rest.starts_with("prepare/") {
            if terms.key_invalid {
                return config_key_invalid_response();
            }
            let cfg = match terms.decode_checked(cfg_seg) {
                Ok(c) => c,
                Err(why) => {
                    // Say something. A key rolled out of CONFIG_KEYS_PREV makes every install
                    // 400 at once, and this path logged nothing at all — leaving the operator to
                    // guess. Length only: the segment carries the key. Once a minute, because that
                    // is also every request those installs make. A revoked install gets the same
                    // 400 as an undecodable one; this line is the only place the two differ.
                    log_limited(refusal_condition(&why), || match why {
                        userconfig::Rejected::Undecodable => {
                            format!("bad_config: {rest} rejected a {}-byte config segment", cfg_seg.len())
                        }
                        refused => format!("bad_config: {rest} refused — {refused}"),
                    });
                    return httputil::json(
                        StatusCode::BAD_REQUEST,
                        &serde_json::json!({"error": "bad_config"}),
                        &[("cache-control", "no-store")],
                    );
                }
            };
            if rest == "manifest.json" {
                // `private`: the path carries the install's sealed key, and the body its install id.
                return httputil::json(
                    StatusCode::OK,
                    &addon::manifest(cfg.iid.as_deref()),
                    &[("cache-control", "private, max-age=3600, stale-while-revalidate=600")],
                );
            }
            if let Some(prepare_rest) = rest.strip_prefix("prepare/") {
                if let Some(resp) =
                    prepare_from_rest(&state, &parts.headers, den, Some(&cfg), prepare_rest, query).await
                {
                    return resp;
                }
                return httputil::not_found();
            }
            let meta_rest = &rest["meta/".len()..];
            if let Some(resp) =
                meta_from_rest(&state, &parts.headers, den, Some(&cfg), meta_rest, query).await
            {
                return resp;
            }
            return httputil::not_found();
        }
    }

    // crop hint: /crop/<id>.json → detected content rect so the app can trim baked-in letterbox.
    if let Some(id) = path.strip_prefix("/crop/").and_then(|r| r.strip_suffix(".json")) {
        if is_valid_vid(id) {
            // An unsigned /crop degrades instead of refusing. Nothing this server emits carries a
            // signed crop URL — the tag rides on the play URL, and it is the client that has to
            // carry it across — so a hard 403 here would turn "the app did not propagate `s`" into
            // de-letterboxing that silently disappears, with no error to report and /health green.
            // The gate exists to protect the DOWNLOAD, and `unsigned_response` reaches none of it.
            if !signature_ok(&state, &terms, id, query) {
                return crop::unsigned_response(id);
            }
            // `?detect=keyframes`: measure now, from YouTube's own keyframes, with no download.
            if query_param(query, "detect").as_deref() == Some("keyframes") {
                return crop::handle_keyframe_crop(state, id.to_string()).await;
            }
            return crop::handle_crop(state, id.to_string()).await;
        }
    }

    // direct URLs: /direct/<id>.json → the googlevideo URLs themselves, for a client that can play
    // them without this server in the middle (the web app; see `direct.rs`). Refuses like /play
    // rather than degrading like /crop: an unsigned caller would otherwise get a free yt-dlp run,
    // and unlike a crop hint there is no useful constant to answer with.
    if let Some(id) = path.strip_prefix("/direct/").and_then(|r| r.strip_suffix(".json")) {
        if is_valid_vid(id) {
            if !signature_ok(&state, &terms, id, query) {
                return bad_signature();
            }
            let cap = direct::height_cap(&state.cfg, query_param(query, "height").as_deref());
            return direct::handle_direct(state, id.to_string(), cap).await;
        }
    }

    // progressive: /progressive/<id>.mp4 → the stream /direct names, served with its index first so a
    // player need not read every fragment before it starts (see `progressive.rs`). Signed like /direct.
    if let Some(id) = path.strip_prefix("/progressive/").and_then(|r| r.strip_suffix(".mp4")) {
        if is_valid_vid(id) {
            if !signature_ok(&state, &terms, id, query) {
                return bad_signature();
            }
            let cap = direct::height_cap(&state.cfg, query_param(query, "height").as_deref());
            let audio = query_param(query, "audio").as_deref() == Some("1");
            return progressive::handle_progressive(state, &parts.headers, id.to_string(), cap, audio).await;
        }
    }

    // sources: /sources/<id>.json?surface=silent|audible&player=native|hls.js → the forms of this trailer
    // a page should try, in order, as URLs minted under /m/ (see `sources.rs`). Signed like /direct, whose
    // resolve it waits for.
    if let Some(id) = path.strip_prefix("/sources/").and_then(|r| r.strip_suffix(".json")) {
        if is_valid_vid(id) {
            if !signature_ok(&state, &terms, id, query) {
                return bad_signature();
            }
            return sources::handle_sources(state, den, &parts.headers, id.to_string(), query).await;
        }
    }

    // One URL out of a playlist this server rewrote. Its signature covers that URL rather than a
    // video id, so `signature_ok` is not the check here — `handle_segment` makes its own. It lives
    // beside the master rather than at the root so the rewritten URIs can be relative to it: a proxied
    // master served from /m/s/<blob> names `seg?u=…`, which lands on /m/s/seg.
    if path == "/hls/seg" || path == "/m/s/seg" {
        let direct = path == "/m/s/seg";
        return hls::handle_segment(state, query, &parts.headers, direct).await;
    }

    // media: /m/<n|s>/<blob> → a form /sources minted. The blob carries its own tag, expiry and install;
    // the segment says whether it is a native master, whose segments this box never carries. V2 also
    // carries the final downloaded-file fallback here as form d, so a client never derives /play.
    if let Some((filed, blob)) = path.strip_prefix("/m/").and_then(|r| r.split_once('/')) {
        if matches!(filed, "n" | "s") {
            return sources::handle_media(state, den, &parts.headers, filed, blob, query).await;
        }
    }

    // HLS: /hls/<id>.m3u8 → YouTube's own master playlist, best variant first, with every URI in it
    // rewritten to come back through the route above. Signed like /direct, which is also the work it
    // costs: one resolve, shared with whatever /meta already warmed.
    //
    // `?native=1` leaves the URIs on googlevideo, for a player that can fetch them itself: a bare
    // `<video>` is not CORS-checked, so only the playlist crosses this box and none of the video.
    if let Some(id) = path.strip_prefix("/hls/").and_then(|r| r.strip_suffix(".m3u8")) {
        if is_valid_vid(id) {
            if !signature_ok(&state, &terms, id, query) {
                return bad_signature();
            }
            let uris = match query_param(query, "native").as_deref() {
                Some("1") => hls::Uris::Native,
                _ => hls::Uris::Proxy,
            };
            // What the browser plays, in `X-Den-Playable` or `?playable=`: the master then lists only that.
            let playable = client::from_request(&parts.headers, query);
            return hls::handle_master(state, id.to_string(), uris, playable).await;
        }
    }

    // playback: id from /play/<id>.mp4 (overrides ?v=), else ?v= on the bare /play path.
    let mut vid = query_param(query, "v");
    let play_match =
        path.strip_prefix("/play/").and_then(|r| r.strip_suffix(".mp4")).filter(|id| is_valid_vid(id));
    if let Some(id) = play_match {
        vid = Some(id.to_string());
    } else if path != "/play" {
        return httputil::not_found();
    }
    let vid = match vid {
        Some(v) if is_valid_vid(&v) => v,
        _ => {
            return httputil::error(
                StatusCode::BAD_REQUEST,
                "bad_video_id",
                "Expected an 11-character YouTube id.",
            )
        }
    };
    if !signature_ok(&state, &terms, &vid, query) {
        return bad_signature();
    }
    play::handle_play(state, &parts.headers, vid).await
}

/// The log condition for each way a config or a bound link is refused, so each is rate-limited apart.
fn refusal_condition(why: &userconfig::Rejected) -> &'static str {
    match why {
        userconfig::Rejected::Undecodable => "bad_config",
        userconfig::Rejected::Plaintext => "plaintext_refused",
        userconfig::Rejected::Revoked { .. } => "install_revoked",
        userconfig::Rejected::EpochTooOld { .. } => "install_epoch_too_old",
        userconfig::Rejected::NoInstallId => "install_no_iid",
    }
}

/// May this request spend a download on this id? `true` for every request when `PLAY_SECRET` is
/// unset, which is the default. Otherwise the tag must verify over the id and the install named by
/// `i`/`e` (see `sign.rs`), and that install must still be admitted by `REVOKED_INSTALLS` /
/// `CONFIG_EPOCH` / `REQUIRE_INSTALL_ID`. While `PLAY_SIGNING_GRACE_UNTIL` is ahead, a request with
/// no tag, or with a tag over the id alone from before install binding, is served too.
///
/// A predicate, not a response. The two callers disagree about what a refusal looks like — `/play`
/// says 403, `/crop` degrades to "play the full frame" — and the `/crop` refusal is the EXPECTED
/// case there, since nothing this server emits is a signed crop URL. Returning a built response
/// meant serializing a JSON body and a header map on that path and dropping both.
///
/// Both endpoints authorise the same work for the same id, and the tag does not cover the path, so a
/// client can carry the query it was handed on the play URL straight over to `/crop`.
///
/// On the Den addon's routes (`terms`) an install-bound link is read as a Den one (`sign::Binding::Den`) and
/// admitted by that addon's revocation, so a link verifies only on the routes of the addon that minted it. The
/// Den addon mints only install-bound links, so there an unbound, untagged or pre-binding link is refused: those
/// are Reel's own (its config-less `/meta` and `/prepare`, and its grace), and verify only on Reel's routes.
fn signature_ok(state: &Arc<AppState>, terms: &userconfig::Terms, vid: &str, query: &str) -> bool {
    let Some(secret) = state.cfg.play_secret.as_deref() else { return true };
    let prev = &state.cfg.play_secrets_prev;
    let den = terms.addon.is_some();
    let Some(presented) = query_param(query, "s") else {
        // A missing tag rides the grace window: a URL issued before signing was turned on carries none.
        if !den && in_grace(&state.cfg, (state.clock)()) {
            note_served_in_grace(vid, "without a tag");
            return true;
        }
        return false;
    };
    let iid = query_param(query, "i");
    let binding = match query_param(query, "e") {
        None if den => return false,
        None if iid.is_none() => sign::Binding::Unbound,
        // Never minted: an install id always travels with its epoch.
        None => return false,
        Some(ep) => match ep.parse() {
            Ok(ep) => terms.binding(iid.as_deref(), ep),
            Err(_) => return false,
        },
    };
    if sign::verify_any(secret, prev, &sign::message(vid, binding), Some(&presented)) {
        return install_admitted(terms.revocation, vid, binding);
    }
    // A tag over the id alone, as releases before install binding minted it. It names no install,
    // so nothing can revoke it: honoured only inside the grace, like an untagged URL. A bound link
    // stripped of `i`/`e` does not land here, because its tag covers the binding.
    if binding == sign::Binding::Unbound
        && in_grace(&state.cfg, (state.clock)())
        && sign::verify_any(secret, prev, vid, Some(&presented))
    {
        note_served_in_grace(vid, "on a tag from before install binding");
        return true;
    }
    false
}

/// Is the install a verified link was minted for still admitted? An unbound link, from the
/// config-less `/meta`, names none and is. A refusal gets the same answer as a bad tag; this log
/// line, in the style of the config routes' `bad_config`, is the only place the two differ.
fn install_admitted(revocation: &userconfig::Revocation, vid: &str, binding: sign::Binding) -> bool {
    let (sign::Binding::Install { iid, ep } | sign::Binding::Den { iid, ep }) = binding else { return true };
    match revocation.check(iid, ep) {
        Ok(()) => true,
        Err(why) => {
            log_limited(&format!("play_{}", refusal_condition(&why)), || {
                format!("bad_signature: link for {vid} refused — {why}")
            });
            false
        }
    }
}

fn in_grace(cfg: &Config, now_ms: u64) -> bool {
    cfg.play_signing_grace.as_ref().is_some_and(|g| now_ms < g.until_ms)
}

/// Ids remembered by `note_served_in_grace`. Anyone who can reach `/play` can invent ids, so the
/// set is bounded; past it the rest share one rate-limited line.
const GRACE_LOGGED_IDS: usize = 4096;

/// Say, once per id and `how`, that a URL was served only because of the grace window, so the
/// operator can see which trailers clients are still holding old URLs for — and that the
/// stragglers have stopped before the deadline arrives.
fn note_served_in_grace(vid: &str, how: &str) {
    static SEEN: std::sync::Mutex<std::collections::BTreeSet<String>> =
        std::sync::Mutex::new(std::collections::BTreeSet::new());
    let key = format!("{vid} {how}");
    let fresh = {
        let mut seen = SEEN.lock().unwrap_or_else(|e| e.into_inner());
        if seen.contains(&key) {
            return;
        }
        seen.len() < GRACE_LOGGED_IDS && seen.insert(key)
    };
    if fresh {
        eprintln!("play signing grace: served {vid} {how}");
    } else {
        log_limited("served_in_grace", || {
            format!("play signing grace: served {vid} {how} (past {GRACE_LOGGED_IDS} distinct ids)")
        });
    }
}

/// The startup line's `play_signing=` value: `off`, `on`, or `grace(until=<ts>)` while untagged
/// requests are still served. A grace whose moment has passed reads `on`, because that is what it is.
fn play_signing_state(cfg: &Config, now_ms: u64) -> String {
    match &cfg.play_signing_grace {
        _ if cfg.play_secret.is_none() => "off".into(),
        Some(g) if now_ms < g.until_ms => format!("grace(until={})", g.until),
        _ => "on".into(),
    }
}

/// The `/play` refusal: terse, and no hint about what a correct tag would look like.
fn bad_signature() -> Response<Body> {
    httputil::json(
        StatusCode::FORBIDDEN,
        &serde_json::json!({"error": "bad_signature", "detail": "This trailer URL is not signed for this server."}),
        &[("cache-control", "no-store")],
    )
}

/// Parse `<movie|series>/<imdbId>.json` (the part after `meta/`) and dispatch to the meta handler.
/// `None` if the shape doesn't match, so the caller can fall through to the next route. `cfg` carries
/// the per-install BYOK keys (`None` = legacy config-less, resolve with the env key).
async fn meta_from_rest(
    state: &Arc<AppState>,
    headers: &hyper::HeaderMap,
    den: Option<&den_seal::Addon>,
    cfg: Option<&userconfig::UserConfig>,
    rest: &str,
    query: &str,
) -> Option<Response<Body>> {
    let (seg, tail) = rest.split_once('/')?;
    if (seg == "movie" || seg == "series") && tail.ends_with(".json") && tail.len() > 5 {
        let raw = httputil::percent_decode(&tail[..tail.len() - 5]);
        return Some(addon::handle_meta(state, headers, den, cfg, seg, &raw, query).await);
    }
    None
}

/// Parse `<movie|series>/<id>.json` (the part after `prepare/`) and dispatch to the combined
/// preparation handler. Kept beside `meta_from_rest` so both legacy and config-scoped routes accept
/// exactly the same title path shape.
async fn prepare_from_rest(
    state: &Arc<AppState>,
    headers: &hyper::HeaderMap,
    den: Option<&den_seal::Addon>,
    cfg: Option<&userconfig::UserConfig>,
    rest: &str,
    query: &str,
) -> Option<Response<Body>> {
    let (seg, tail) = rest.split_once('/')?;
    if (seg == "movie" || seg == "series") && tail.ends_with(".json") && tail.len() > 5 {
        let raw = httputil::percent_decode(&tail[..tail.len() - 5]);
        return Some(prepare::handle_prepare(state, headers, den, cfg, seg, &raw, query).await);
    }
    None
}

/// Everything the service does before it listens: prepare the cache, restore what the last process
/// parked, and start the periodic sweeps. Call it from inside a tokio runtime. `run` is this plus a
/// listener; den-hearth calls it and routes requests to `handle_request` itself.
pub fn init(cfg: Config) -> Arc<AppState> {
    // A bad CACHE_DIR must NOT crash-loop the process: discovery (/health, /manifest, /meta) doesn't
    // need the cache, only /play and /crop do — and those return a structured 503 when it's missing.
    if let Err(e) = std::fs::create_dir_all(&cfg.cache_dir) {
        eprintln!(
            "warning: cache dir {} is unusable ({e}); /play and /crop will 503 until it's writable",
            cfg.cache_dir.display()
        );
    }

    let state = AppState::new(cfg);
    // A verdict that is degraded from the start — no discovery key at all — is said once, here.
    state.note_health();

    // Pick up where the last process left off. A resolve is a TMDB round-trip per title, and a
    // redeploy otherwise makes the next browse pay for every title on screen again.
    {
        let restored = state::load_resolve_cache(&state.cfg, (state.clock)());
        if !restored.is_empty() {
            *state.yt_cache.lock().unwrap_or_else(|e| e.into_inner()) = restored;
        }
    }

    // And the resolved googlevideo URLs, which cost a yt-dlp run each rather than a TMDB call. Without
    // this, every deploy made the first open of every title pay 1.2–3.6 s again — measured on the box,
    // where it was the difference between a hero starting in 122 ms and starting in seconds.
    {
        let restored = state::load_direct_cache(&state.cfg, (state.clock)());
        if !restored.is_empty() {
            *state.direct_cache.lock().unwrap_or_else(|e| e.into_inner()) = restored;
        }
    }

    // And the letterboxes, which cost a second of cropdetect over up to 64 keyframes apiece and never change.
    {
        let restored = state::load_crop_cache(&state.cfg);
        if !restored.is_empty() {
            *state.crop_cache.lock().unwrap_or_else(|e| e.into_inner()) = restored;
        }
    }

    // And the built indexes, which are only worth restoring because the resolves above came back with them:
    // an index maps ranges into Google's file, so it is useful again only alongside the URLs it was built for.
    {
        let parked = state::load_index_cache(&state.cfg);
        if !parked.is_empty() {
            let kept = progressive::restore(&state, parked, (state.clock)());
            if kept > 0 {
                eprintln!("index cache: {kept} trailer index(es) still good");
            }
        }
    }

    // Periodic cache sweep so the last-access TTL is enforced during idle stretches too — eviction
    // otherwise only runs after a download. Hourly is ample for a day-scale TTL, and interval's first
    // tick fires immediately so a cache left stale over a long downtime is trimmed on boot.
    {
        let state = state.clone();
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(std::time::Duration::from_secs(3600));
            loop {
                tick.tick().await;
                let cfg = state.cfg.clone();
                let measured = tokio::task::spawn_blocking(move || {
                    crate::play::sweep_partials(&cfg);
                    crate::play::evict_if_needed(&cfg)
                })
                .await;
                if let Ok(Some(u)) = measured {
                    state.record_cache_usage(u);
                }
            }
        });
    }

    // Let an idle resident yt-dlp go. Trailers are bursty — a browse, then nothing for hours — and
    // an interpreter sitting on ~40 MB of a 1 GiB container for the quiet stretch is a poor trade
    // against paying its startup once more on the next browse.
    {
        let state = state.clone();
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(crate::worker::IDLE_TIMEOUT / 4);
            loop {
                tick.tick().await;
                state.worker.reap_idle().await;
                state.youtubei.reap_idle().await;
            }
        });
    }
    state
}

/// Everything the service does after its last request: kill the downloads, stop the worker, remove its
/// own temp files and park the caches for the next process.
pub async fn finish(state: &Arc<AppState>) {
    // Kill the downloads EXPLICITLY. Dropping the state does not do it: `in_flight` holds a Shared
    // clone of a future that captures the very Arc<AppState> the map lives in, so the cycle keeps
    // each Child alive past runtime teardown and neither kill-on-drop nor the group guard fires.
    // The sweep would then race a yt-dlp that is still writing, and recreate the file it deleted.
    // In the container this was masked by den-reel being PID 1 — namespace teardown SIGKILLs the
    // orphans just after — which is not a mechanism to rely on, and is absent outside a container.
    let killed = crate::ytdlp::kill_live_groups();
    if killed > 0 {
        eprintln!("shutdown: killed {killed} in-flight download(s)");
    }
    state.worker.shutdown().await;
    state.youtubei.shutdown().await;
    crate::play::sweep_own_temps(&state.cfg);
    state::save_resolve_cache(state);
    state::save_direct_cache(state);
    state::save_crop_cache(state);
    state::save_index_cache(state);
}

/// The line Reel logs once it is listening: its version, port and the switches an operator checks.
/// Hearth prints it too, since its own startup line only names the services.
pub fn startup_line(state: &AppState) -> String {
    let on = |b: bool| if b { "on" } else { "off" };
    let cfg = &state.cfg;
    let sealed = if state.config_key_invalid { "invalid" } else { on(state.config_keyring.is_some()) };
    format!(
        "den-reel {} listening on :{} — metrics={} log_requests={} sealed={} revoked={} epoch={} \
         require_iid={} play_signing={} direct_media_max={} env_tmdb_key={} cache={} max_height={}",
        release_version(),
        cfg.port,
        on(cfg.metrics_token.is_some()),
        on(cfg.log_requests),
        sealed,
        cfg.revocation.revoked_count(),
        cfg.revocation.epoch(),
        on(cfg.revocation.requires_install_id()),
        play_signing_state(cfg, (state.clock)()),
        cfg.direct_media_concurrency,
        on(cfg.tmdb_key.is_some()),
        cfg.cache_dir.display(),
        cfg.max_height,
    )
}
