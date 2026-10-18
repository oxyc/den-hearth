//! den-subtitles — Hearth's subtitles service:
//!
//! 1. FETCH — OpenSubtitles, hash-matched (the app sends the file's OSHash as `videoHash`), served
//!    from our own cache/origin so results are fast and dodge the per-IP download quota.
//! 2. TRANSLATE — BYOK LLM translation (gpt-4o-mini default) via a chunk→same-length→retry harness
//!    that survives a full film without losing sync between cue count and timing.
//! 3. SYNC — an auto-sync ladder (hash → reference-align → alass audio VAD) so subtitles line up.
//!
//! Both credentials (OpenSubtitles key, LLM key) are BYOK and ride in the combined Den install URL,
//! Keychain-stored by the app. No user credential lives in the environment.

mod addon;
mod cache;
mod cache_policy;
mod config;
mod fetch;
mod httputil;
mod inflight;
mod logging;
mod metrics;
mod opensubtitles;
mod ratelimit;
mod resync;
mod srt;
mod state;
mod sync;
mod translate;
mod userconfig;

#[cfg(feature = "bench")]
pub mod bench_api;

use std::sync::{Arc, Mutex, MutexGuard, OnceLock, PoisonError};
use std::time::Instant;

use hyper::{Request, Response, StatusCode};

pub use crate::config::Config;
pub use crate::httputil::Body;
pub use crate::state::AppState;
pub use crate::userconfig::is_valid as is_valid_config;

/// Open and validate one legacy install for Hearth's migration endpoint, then drop the member's old revocation
/// identity. The returned value is sealed immediately as a member of a fresh combined install.
pub fn migration_member(state: &AppState, blob: &str) -> Option<serde_json::Value> {
    userconfig::decode_checked(state.config_keyring.as_ref(), &state.cfg.revocation, blob).ok()?;
    let opened = den_seal::open_segment(state.config_keyring.as_ref(), blob).ok()?;
    let mut value: serde_json::Value = serde_json::from_slice(&opened.json).ok()?;
    let member = value.as_object_mut()?;
    member.remove("iid");
    member.remove("ep");
    Some(value)
}

/// The unconfigured manifest, for the combined Den addon's manifest to take Subtitles' resource from.
pub fn unconfigured_manifest() -> serde_json::Value {
    addon::manifest()
}

/// Lock `m`, recovering the data if a panic poisoned it. Every map behind these locks is left
/// consistent between statements, and a `Drop` that unwrapped a poisoned lock would panic while
/// unwinding and abort the whole process (hearth hosts four services in it).
pub(crate) fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(PoisonError::into_inner)
}

/// The release the image carries (`DEN_VERSION`, set from the repo's `VERSION` file at build time). Keeping it
/// out of Cargo.toml means a release does not invalidate the cached dependency build merely to change its tag.
/// Developer builds and tests have no image metadata, so they fall back to the package version.
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

/// Consecutive OpenSubtitles failures before /health reports `degraded` (ADDON-02).
const HEALTH_FAIL_THRESHOLD: u32 = 3;

/// The /health response body for a given consecutive-failure count (ADDON-02). Pure so the
/// degraded/ok decision is unit-testable without standing up the HTTP server.
fn health_body(os_fails: u32) -> serde_json::Value {
    if os_fails >= HEALTH_FAIL_THRESHOLD {
        serde_json::json!({"status": "degraded", "reason": "upstream_unavailable", "detail": "OpenSubtitles has been failing"})
    } else {
        serde_json::json!({"status": "ok"})
    }
}

/// The /health body while `CONFIG_KEY` is set but malformed: wins over every other verdict, since no
/// install config can be trusted until it is fixed.
fn config_key_invalid_health() -> serde_json::Value {
    serde_json::json!({
        "status": "degraded",
        "reason": "config_key_invalid",
        "detail": "CONFIG_KEY is set but not a valid base64 32-byte key — install configs are refused until it is fixed",
    })
}

/// The 503 an install-config request gets while `CONFIG_KEY` is malformed. Retry-After is a hint to
/// clients, not a promise: only an operator fixing the variable and restarting clears it.
fn config_key_invalid_response() -> Response<Body> {
    httputil::retry_after(
        httputil::json(
            StatusCode::SERVICE_UNAVAILABLE,
            &serde_json::json!({"error": "config_key_invalid"}),
            "no-store",
        ),
        std::time::Duration::from_secs(60),
    )
}

// Generic over the request body: this handler routes on path/query only and discards the body, so tests
// can drive it with a `Request<()>` while `run()` passes the real `Request<Incoming>`.
pub async fn handle_request<B>(state: Arc<AppState>, req: Request<B>) -> Response<Body> {
    let started = Instant::now();
    let (parts, _body) = req.into_parts();
    let mut resp = match parts.method {
        // A CORS preflight is answered for any path, before routing.
        hyper::Method::OPTIONS => httputil::preflight(),
        // These GET routes can search OpenSubtitles, spend a metered download, run ffmpeg/alass,
        // or invoke an LLM. Body stripping happens only after routing, so HEAD must be stopped here.
        hyper::Method::HEAD if head_requires_get(parts.uri.path()) => {
            httputil::apply_conditional(&parts.method, &parts.headers, method_not_allowed("GET, OPTIONS"))
        }
        // Honor conditional GET/HEAD: any cacheable 200 carries an ETag, so an `If-None-Match` hit
        // collapses to a 304. A HEAD answer never has a body (the router builds one regardless of method).
        hyper::Method::GET | hyper::Method::HEAD => {
            // Set by hearth on a request it routed under `/den`; no client can put one here.
            let den = parts.extensions.get::<Arc<den_seal::Addon>>().cloned();
            let resp = route(&state, den.as_deref(), &parts).await;
            httputil::apply_conditional(&parts.method, &parts.headers, resp)
        }
        // Every route is a read. Routing on the path alone let a POST reach the same handlers — and a
        // subtitle or translate route spends a metered download or an LLM bill.
        _ => method_not_allowed("GET, HEAD, OPTIONS"),
    };
    // `total` closes the header on any response whose handler named its phases, in tenths of a
    // millisecond like every other den addon.
    resp = den_http::with_total_timing(resp, started.elapsed());
    // Every reply is readable from a browser-based Stremio client. Added here, last, so the 304 and VTT paths
    // that rebuild a response keep it. Retry-After and the RateLimit pair are on refusals, and are how a browser
    // client knows when to come back. ETag is what a client that keeps its own cache sends back as
    // If-None-Match.
    den_http::cors_stamp(
        resp.headers_mut(),
        "Server-Timing, X-Den-Degraded, Retry-After, RateLimit, RateLimit-Policy, ETag",
    );
    // Off by default, and then this bool is the whole cost. The path is redacted and the query left
    // out: a config segment is an install's credentials, and `?resync=` carries a stream URL. The
    // caller's X-Request-Id rides along so the line can be matched to the app's.
    if state.cfg.log_requests {
        eprintln!(
            "{}",
            logging::request_line(
                parts.method.as_str(),
                parts.uri.path(),
                resp.status().as_u16(),
                started.elapsed().as_millis(),
                logging::request_id(&parts.headers).as_deref(),
            )
        );
    }
    resp
}

fn method_not_allowed(allow: &'static str) -> Response<Body> {
    let mut resp = httputil::error(StatusCode::METHOD_NOT_ALLOWED, "method_not_allowed");
    resp.headers_mut().insert(hyper::header::ALLOW, hyper::header::HeaderValue::from_static(allow));
    resp
}

/// A translation status poll and the operational endpoints are memory/cache-only and retain HEAD.
/// Unknown paths also retain their normal 404. Every actual search, artifact, or translation route
/// requires GET, regardless of whether this particular request might happen to find a warm cache.
fn head_requires_get(path: &str) -> bool {
    let segs = split_path(path);
    match segs.get(1).copied() {
        Some("subtitles" | "subtitle") => true,
        Some("translate") => !segs.last().is_some_and(|last| last.ends_with(".status")),
        _ => false,
    }
}

/// `den` is the combined Den addon's terms when hearth routed the request under `/den`: the config segment is
/// that addon's, and the links answered with point back under it (`AppState::decode_config`, `addon::self_base`).
async fn route(
    state: &Arc<AppState>,
    den: Option<&den_seal::Addon>,
    parts: &hyper::http::request::Parts,
) -> Response<Body> {
    let path = parts.uri.path();

    match path {
        // Standard Den addon health (ADDON-02): 200 for liveness, `degraded` when OpenSubtitles has been
        // failing so the app's Plugins screen can surface it.
        "/health" => {
            let body = if state.config_key_invalid {
                config_key_invalid_health()
            } else {
                health_body(state.os_fails.load(std::sync::atomic::Ordering::Relaxed))
            };
            return httputil::json(StatusCode::OK, &body, "no-store");
        }
        // Prometheus text behind `METRICS_TOKEN`; 404 without it (see metrics.rs).
        "/metrics" => return metrics::handle(state, &parts.headers),
        _ => {}
    }

    let segs = split_path(path);
    let config = segs.first().copied().unwrap_or("");
    let resource = segs.get(1).copied().unwrap_or("");

    if state.key_invalid(den) && matches!(resource, "subtitles" | "subtitle" | "translate") {
        return config_key_invalid_response();
    }

    match resource {
        "subtitles" => {
            // /<config>/subtitles/<type>/<id>[/<extra>].json
            let (id_seg, extra) = match segs.len() {
                5 => (segs[3], segs[4]),
                4 => (segs[3], ""),
                _ => return httputil::not_found(),
            };
            let id = strip_json(id_seg).unwrap_or(id_seg);
            let extra = strip_json(extra).unwrap_or(extra);
            addon::handle_subtitles(state, den, &parts.headers, config, id, extra).await
        }
        "subtitle" => {
            // /<config>/subtitle/<file_id>.(srt|vtt)[?ref=<id>|?resync=<stream-url>][&lang=<code>]
            //   [&resync_key=<sha256>][&resync_audio=<AVStream index>][&sparse=1]
            //   [&ep=<imdb>:<season>:<episode>][&dub=1]
            let file = segs.get(2).copied().unwrap_or("");
            let (stem, want_vtt) = split_subtitle_format(file);
            match stem.and_then(|n| n.parse::<i64>().ok()) {
                Some(file_id) => {
                    let query = parts.uri.query().unwrap_or("");
                    let ref_id = query_get(query, "ref").and_then(|v| v.parse().ok());
                    let resync = query_get(query, "resync");
                    let resync_key = query_get(query, "resync_key");
                    let resync_audio = query_get(query, "resync_audio")
                        .and_then(|value| value.parse::<u32>().ok())
                        .filter(|index| *index <= 4095);
                    let lang = query_get(query, "lang");
                    // The caller's own last resort: it has already established nothing better exists
                    // for this language (every other candidate was tried and refused), so a real but
                    // sparse track is worth serving rather than nothing. See `Client::download`.
                    let allow_sparse = query_get(query, "sparse").as_deref() == Some("1");
                    // Both set by `handle_subtitles` on every URL it hands out — see there for why
                    // the download route, addressed by file id alone, needs them at all.
                    let episode_key = query_get(query, "ep");
                    let dub_flagged = query_get(query, "dub").as_deref() == Some("1");
                    let resp = addon::handle_subtitle_file(
                        state,
                        den,
                        &parts.headers,
                        config,
                        file_id,
                        ref_id,
                        resync,
                        resync_key,
                        resync_audio,
                        lang.as_deref(),
                        want_vtt,
                        allow_sparse,
                        episode_key,
                        dub_flagged,
                    )
                    .await;
                    if want_vtt {
                        httputil::to_vtt(resp, &parts.headers).await
                    } else {
                        resp
                    }
                }
                None => httputil::error(StatusCode::BAD_REQUEST, "bad_file_id"),
            }
        }
        "translate" => {
            // /<config>/translate/<type>/<id>[/<extra>]/<lang>.(json|srt)
            //
            // The <extra> segment carries this stream's videoHash, which is what lets a translation
            // find a Tier-1 anchor. It is optional so installs built against the older five-segment
            // shape keep resolving — they just get no anchor, which is where every translation was.
            let (id, extra, last) = match segs.len() {
                6 => (segs[3], segs[4], segs[5]),
                5 => (segs[3], "", segs[4]),
                _ => return httputil::not_found(),
            };
            // `.status` is answered from the request alone, so it is dispatched before anything that
            // could make a poll expensive.
            if let Some(lang) = last.strip_suffix(".status") {
                return addon::handle_translate_status(state, den, config, id, lang).await;
            }
            let (lang, want_json, want_vtt) = if let Some(l) = last.strip_suffix(".json") {
                (l, true, false)
            } else if let Some(l) = last.strip_suffix(".srt") {
                (l, false, false)
            } else if let Some(l) = last.strip_suffix(".vtt") {
                (l, false, true)
            } else {
                return httputil::not_found();
            };
            let resync = query_get(parts.uri.query().unwrap_or(""), "resync");
            let resync_key = query_get(parts.uri.query().unwrap_or(""), "resync_key");
            let resync_audio = query_get(parts.uri.query().unwrap_or(""), "resync_audio")
                .and_then(|value| value.parse::<u32>().ok())
                .filter(|index| *index <= 4095);
            let resp = addon::handle_translate(
                state,
                den,
                &parts.headers,
                config,
                id,
                extra,
                lang,
                want_json,
                resync,
                resync_key,
                resync_audio,
            )
            .await;
            if want_vtt {
                httputil::to_vtt(resp, &parts.headers).await
            } else {
                resp
            }
        }
        _ => httputil::not_found(),
    }
}

fn split_path(path: &str) -> Vec<&str> {
    path.split('/').filter(|s| !s.is_empty()).collect()
}

/// Percent-decoded value of a query parameter, or None.
fn query_get(query: &str, key: &str) -> Option<String> {
    let prefix = format!("{key}=");
    query.split('&').find_map(|p| p.strip_prefix(&prefix)).map(httputil::percent_decode)
}

fn strip_json(seg: &str) -> Option<&str> {
    seg.strip_suffix(".json")
}

/// Split a subtitle filename into its stem and the format asked for. SRT is what the engine takes;
/// VTT is the same document for a client that needs a `<track>`, and is rendered from the finished
/// SRT at the response rather than anywhere inside the sync ladder.
fn split_subtitle_format(file: &str) -> (Option<&str>, bool) {
    match (file.strip_suffix(".srt"), file.strip_suffix(".vtt")) {
        (Some(stem), _) => (Some(stem), false),
        (_, Some(stem)) => (Some(stem), true),
        _ => (None, false),
    }
}

/// Everything the service does before it listens: prepare the cache and start the hourly sweep. Call it
/// from inside a tokio runtime. `run` is this plus a listener; den-hearth calls it and routes requests to
/// `handle_request` itself.
pub fn init(cfg: Config) -> Arc<AppState> {
    // Don't refuse to boot on an unwritable cache mount — the artifact cache is fully in-memory and
    // the sync scratch dir creates itself lazily. A failed pre-create is logged, not fatal (a hard
    // exit here would crash-loop the container and the app would see only connection-refused).
    if let Err(e) = std::fs::create_dir_all(&cfg.cache_dir) {
        eprintln!(
            "warning: cache dir {} not writable ({e}) — sync tiers will retry lazily",
            cfg.cache_dir.display()
        );
    }
    let state = AppState::new(cfg);

    // Reclaim the disk cache hourly. Opening the cache walks the directory once and drops what expired
    // or is torn while the process was down, which bounds the store across restarts but not within
    // one — a container that stays up keeps writing entries that only a repeat request for the same
    // key would ever expire.
    {
        let state = state.clone();
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(std::time::Duration::from_secs(3600));
            tick.tick().await; // fires immediately; the boot sweep already ran
            loop {
                tick.tick().await;
                // Off the runtime thread. The sweep stats every file in the store and header-reads
                // most of them — thousands of blocking opens at the default budget — and this
                // runtime has ONE thread, which is also the one serving every connection. Fast on a
                // local SSD; on a container bind-mount at a millisecond an open it is seconds of
                // total stall, every hour.
                let swept = state.clone();
                let done = tokio::task::spawn_blocking(move || {
                    swept.cache.sweep();
                    // The sync work dir too. `finish` is the only cleanup there and it is skipped
                    // when a request is cancelled mid-alignment, so without this each one leaks its
                    // scratch files permanently.
                    swept.sync.sweep_scratch();
                })
                .await;
                if let Err(e) = done {
                    eprintln!("cache sweep did not run: {e}");
                }
            }
        });
    }
    state
}

/// The line Subtitles logs once it is listening. Hearth prints it too, since its own startup line only
/// names the services.
///
/// What this process is running with, minus anything secret: the key and the token appear only as
/// on/off, and the origin is one the addon hands to every client anyway. Same shape as every other
/// den addon's line.
pub fn startup_line(state: &AppState) -> String {
    let on_off = |on: bool| if on { "on" } else { "off" };
    format!(
        "den-subtitles {} listening on :{} — metrics={} log_requests={} log_identity={} sealed={} \
         revoked={} epoch={} require_iid={} cache_dir={} cache_max_mib={} public_base={} resync={}",
        release_version(),
        state.cfg.port,
        on_off(!state.cfg.metrics_token.is_empty()),
        on_off(state.cfg.log_requests),
        on_off(state.cfg.log_identity),
        if state.config_key_invalid { "invalid" } else { on_off(state.config_keyring.is_some()) },
        state.cfg.revocation.revoked_count(),
        state.cfg.revocation.epoch(),
        on_off(state.cfg.revocation.requires_install_id()),
        state.cfg.cache_dir.display(),
        state.cfg.cache_max_bytes / (1024 * 1024),
        state.cfg.public_base_url.as_deref().unwrap_or("derived"),
        // Off says why a resync falls back to the raw sub: no SCOUT_ORIGINS.
        match state.cfg.scout_origins.is_empty() {
            true => "off".to_string(),
            false => state.cfg.scout_origins.iter().map(ToString::to_string).collect::<Vec<_>>().join(","),
        },
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use base64::Engine;
    use hyper::header::{HeaderValue, ACCESS_CONTROL_ALLOW_ORIGIN};
    use std::time::Duration;

    // Same fixed vector key + browser-minted sealed segment the seal/userconfig unit tests use, so the
    // HTTP layer is exercised against a real crypto_box_seal blob, not a mock.
    const VEC_PRIV_B64: &str = "AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8=";

    fn test_state(config_key: &str) -> Arc<AppState> {
        test_state_with(config_key, "")
    }

    fn test_state_with(config_key: &str, metrics_token: &str) -> Arc<AppState> {
        AppState::new(test_config(config_key, metrics_token))
    }

    fn test_config(config_key: &str, metrics_token: &str) -> Config {
        Config {
            port: 0,
            cache_dir: std::env::temp_dir().join("den-subtitles-test-cache"),
            cache_max_bytes: 8 * 1024 * 1024,
            public_base_url: None,
            alass: "alass".to_string(),
            ffmpeg: "ffmpeg".to_string(),
            config_key: config_key.to_string(),
            config_keys_prev: String::new(),
            metrics_token: metrics_token.to_string(),
            log_requests: false,
            log_identity: true,
            // Never the live API from a test: port 1 refuses instantly.
            os_api_base: "http://127.0.0.1:1".to_string(),
            scout_origins: Vec::new(),
            scout_aliases: Vec::new(),
            revocation: Default::default(),
        }
    }

    // Issue #8 R3: an install id as /den/configure mints it (bytes 0..16).
    const IID: &str = "AAECAwQFBgcICQoLDA0ODw";

    fn plain_segment(json: &str) -> String {
        base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(json)
    }

    /// A state refusing the installs in `revoked` and every link stamped below `epoch`, with a fixed
    /// public origin so the URLs it hands back can be requested again.
    fn revoking_state(revoked: &str, epoch: &str) -> Arc<AppState> {
        let mut cfg = test_config("", "");
        cfg.revocation = userconfig::Revocation::from_env(revoked, Some(epoch));
        cfg.public_base_url = Some("http://subs.test".to_string());
        AppState::new(cfg)
    }

    /// Every route that reads a config refuses a revoked install, with exactly the answer an
    /// undecodable segment gets on that route.
    #[tokio::test]
    async fn a_revoked_install_is_refused_on_every_config_route() {
        let seg = plain_segment(&format!(r#"{{"osKey":"os-test","iid":"{IID}"}}"#));
        let routes = [
            "subtitles/movie/tt0000001.json",
            "subtitle/42.srt",
            "translate/movie/tt0000001/Swedish.json",
            "translate/movie/tt0000001/Swedish.srt",
            "translate/movie/tt0000001/Swedish.status",
        ];
        for route in routes {
            let revoked = handle_request(
                revoking_state(IID, "0"),
                request(hyper::Method::GET, &format!("/{seg}/{route}")),
            )
            .await;
            let garbage = handle_request(
                revoking_state(IID, "0"),
                request(hyper::Method::GET, &format!("/not-a-config/{route}")),
            )
            .await;
            assert_eq!(revoked.status(), StatusCode::BAD_REQUEST, "{route}");
            assert_eq!(revoked.status(), garbage.status(), "{route}");
            let body = body_string(revoked).await;
            assert_eq!(body, r#"{"error":"bad_config"}"#, "{route}");
            assert_eq!(body, body_string(garbage).await, "{route}");
        }
        // The same install, not revoked, is served.
        let resp = handle_request(
            revoking_state("", "0"),
            request(hyper::Method::GET, &format!("/{seg}/subtitles/movie/tt0000001.json")),
        )
        .await;
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn an_install_stamped_below_config_epoch_is_refused() {
        for (json, want) in [
            (r#"{"osKey":"o","ep":1}"#, StatusCode::BAD_REQUEST),
            (r#"{"osKey":"o"}"#, StatusCode::BAD_REQUEST), // no epoch reads as 0
            (r#"{"osKey":"o","ep":2}"#, StatusCode::OK),
            (r#"{"osKey":"o","ep":3}"#, StatusCode::OK),
        ] {
            let uri = format!("/{}/subtitles/movie/tt0000001.json", plain_segment(json));
            let resp = handle_request(revoking_state("", "2"), request(hyper::Method::GET, &uri)).await;
            assert_eq!(resp.status(), want, "{json}");
        }
    }

    /// Links built before install ids existed carry neither field, and keep working until an epoch
    /// is raised.
    #[tokio::test]
    async fn a_config_without_an_install_id_works_at_epoch_zero() {
        let uri = format!("/{}/subtitles/movie/tt0000001.json", plain_segment(r#"{"osKey":"o"}"#));
        let resp = handle_request(revoking_state(IID, "0"), request(hyper::Method::GET, &uri)).await;
        assert_eq!(resp.status(), StatusCode::OK);
    }

    /// The subtitle URLs the subtitles resource hands back embed the install's config, so a
    /// revocation made after they were issued still reaches them.
    #[tokio::test]
    async fn a_revoked_install_s_issued_subtitle_url_is_refused() {
        let seg = plain_segment(&format!(r#"{{"osKey":"os-test","iid":"{IID}"}}"#));
        let before = revoking_state("", "0");
        let sub = crate::opensubtitles::Subtitle {
            file_id: 4242,
            lang: "en".into(),
            hash_match: false,
            downloads: 1,
            release: "Some.Film.2020.1080p".into(),
            hd: true,
            fps: 0.0,
            from_trusted: false,
            machine_translated: false,
            ai_translated: false,
            foreign_parts_only: false,
            hearing_impaired: false,
            ratings: 0.0,
        };
        let key = format!("{}tt0000093:0:0:", crate::cache::SEARCH_NS);
        before.cache.put(key, serde_json::to_string(&vec![sub]).unwrap(), Duration::from_secs(60));
        let list = handle_request(
            before,
            request(hyper::Method::GET, &format!("/{seg}/subtitles/movie/tt0000093.json")),
        )
        .await;
        let list: serde_json::Value = serde_json::from_str(&body_string(list).await).unwrap();
        let url = list["subtitles"][0]["url"].as_str().expect("a subtitle url").to_string();
        let path = url.strip_prefix("http://subs.test").expect("the fixed public origin");
        assert!(path.starts_with(&format!("/{seg}/subtitle/4242.srt")), "{url}");

        let resp = handle_request(revoking_state(IID, "0"), request(hyper::Method::GET, path)).await;
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
        assert_eq!(body_string(resp).await, r#"{"error":"bad_config"}"#);
    }

    /// The Den addon's terms for Subtitles (hearth's `/den`): plaintext configs, links under `http://node.test/den`.
    fn den_terms(revoked: &str, epoch: &str) -> den_seal::Addon {
        den_seal::Addon {
            keyring: None,
            key_invalid: false,
            revocation: userconfig::Revocation::from_env(revoked, Some(epoch)),
            public_base: Some("http://node.test/den".into()),
            slice: "subs",
        }
    }

    /// `uri` as hearth hands Subtitles a request it routed under `/den`.
    fn den_request(uri: &str, den: den_seal::Addon) -> Request<()> {
        let mut req = request(hyper::Method::GET, uri);
        req.extensions_mut().insert(Arc::new(den));
        req
    }

    /// A combined config carrying the Subtitles member and the install `IID`.
    fn combined() -> String {
        plain_segment(&format!(
            r#"{{"iid":"{IID}","ep":0,"subs":{{"osKey":"os-test"}},"reel":{{"tmdbKey":"t"}}}}"#
        ))
    }

    /// The subtitle URLs a Den install is handed come back under the Den addon, carrying its config.
    #[tokio::test]
    async fn a_den_install_s_subtitle_urls_come_back_under_the_den_addon() {
        let state = revoking_state("", "0");
        let sub = crate::opensubtitles::Subtitle {
            file_id: 4242,
            lang: "en".into(),
            hash_match: false,
            downloads: 1,
            release: "Some.Film.2020.1080p".into(),
            hd: true,
            fps: 0.0,
            from_trusted: false,
            machine_translated: false,
            ai_translated: false,
            foreign_parts_only: false,
            hearing_impaired: false,
            ratings: 0.0,
        };
        let key = format!("{}tt0000093:0:0:", crate::cache::SEARCH_NS);
        state.cache.put(key, serde_json::to_string(&vec![sub]).unwrap(), Duration::from_secs(60));
        let seg = combined();
        let list = handle_request(
            state,
            den_request(&format!("/{seg}/subtitles/movie/tt0000093.json"), den_terms("", "0")),
        )
        .await;
        let list: serde_json::Value = serde_json::from_str(&body_string(list).await).unwrap();
        let url = list["subtitles"][0]["url"].as_str().expect("a subtitle url");
        assert!(url.starts_with(&format!("http://node.test/den/{seg}/subtitle/4242.srt")), "{url}");
    }

    /// A Den install is admitted by the Den addon's revocation, never by Subtitles' own, and each refuses
    /// the other's configs.
    #[tokio::test]
    async fn the_den_addon_s_terms_decide_a_den_install() {
        let resource = |seg: String, state: Arc<AppState>, den: Option<den_seal::Addon>| async move {
            let uri = format!("/{seg}/subtitles/movie/tt0000001.json");
            let req = match den {
                Some(den) => den_request(&uri, den),
                None => request(hyper::Method::GET, &uri),
            };
            handle_request(state, req).await.status()
        };
        let own = plain_segment(&format!(r#"{{"osKey":"os-test","iid":"{IID}"}}"#));
        let ok = StatusCode::OK;
        let bad = StatusCode::BAD_REQUEST;
        assert_eq!(resource(combined(), revoking_state("", "0"), Some(den_terms("", "0"))).await, ok);
        // Revoked, or stamped below the epoch, by the Den addon.
        assert_eq!(resource(combined(), revoking_state("", "0"), Some(den_terms(IID, "0"))).await, bad);
        assert_eq!(resource(combined(), revoking_state("", "0"), Some(den_terms("", "1"))).await, bad);
        // Revoked by Subtitles alone: a Den install is not Subtitles' to revoke.
        assert_eq!(resource(combined(), revoking_state(IID, "3"), Some(den_terms("", "0"))).await, ok);
        // Neither kind of config opens as the other.
        assert_eq!(resource(own.clone(), revoking_state("", "0"), Some(den_terms("", "0"))).await, bad);
        assert_eq!(resource(combined(), revoking_state("", "0"), None).await, bad);
        assert_eq!(resource(own, revoking_state("", "0"), None).await, ok);
        // A Den config without a Subtitles member.
        let reel_only = plain_segment(r#"{"reel":{"tmdbKey":"t"}}"#);
        assert_eq!(resource(reel_only, revoking_state("", "0"), Some(den_terms("", "0"))).await, bad);
    }

    #[tokio::test]
    async fn a_malformed_den_key_refuses_den_installs_only() {
        let den = den_seal::Addon { key_invalid: true, ..den_terms("", "0") };
        let resp = handle_request(
            test_state(""),
            den_request(&format!("/{}/subtitles/movie/tt0000001.json", combined()), den),
        )
        .await;
        assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert!(body_string(resp).await.contains("config_key_invalid"));
        let own = plain_segment(r#"{"osKey":"o"}"#);
        let resp = handle_request(
            test_state(""),
            request(hyper::Method::GET, &format!("/{own}/subtitles/movie/tt0000001.json")),
        )
        .await;
        assert_eq!(resp.status(), StatusCode::OK);
    }

    async fn body_string(resp: Response<Body>) -> String {
        use http_body_util::BodyExt;
        let bytes = resp.into_body().collect().await.unwrap().to_bytes();
        String::from_utf8_lossy(&bytes).into_owned()
    }
    #[tokio::test]
    async fn no_store_replies_carry_no_etag_and_never_304() {
        use hyper::header::{ETAG, IF_NONE_MATCH};
        // /health is no-store — it must not get an ETag, and a wildcard If-None-Match can't 304 it.
        let resp = handle_request(
            test_state(VEC_PRIV_B64),
            Request::builder().uri("/health").header(IF_NONE_MATCH, "*").body(()).unwrap(),
        )
        .await;
        assert_eq!(resp.status(), StatusCode::OK);
        assert!(resp.headers().get(ETAG).is_none(), "no-store bodies get no ETag");
    }

    #[test]
    fn health_ok_below_threshold() {
        // Fewer than HEALTH_FAIL_THRESHOLD consecutive failures → healthy liveness.
        for fails in 0..HEALTH_FAIL_THRESHOLD {
            let body = health_body(fails);
            assert_eq!(body["status"], "ok", "fails={fails} should be ok");
            // No degraded fields leak into the healthy body.
            assert!(body.get("reason").is_none());
            assert!(body.get("detail").is_none());
        }
    }

    #[test]
    fn health_degraded_at_and_above_threshold() {
        // At the threshold and beyond, OpenSubtitles is treated as down (ADDON-02).
        for fails in [HEALTH_FAIL_THRESHOLD, HEALTH_FAIL_THRESHOLD + 1, 100] {
            let body = health_body(fails);
            assert_eq!(body["status"], "degraded", "fails={fails} should be degraded");
            assert_eq!(body["reason"], "upstream_unavailable");
            assert_eq!(body["detail"], "OpenSubtitles has been failing");
        }
    }

    /// A plaintext config segment carrying only an OpenSubtitles key, enough for `handle_subtitles`.
    fn os_only_segment() -> String {
        base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(r#"{"osKey":"os-test"}"#)
    }

    fn server_timing(resp: &Response<Body>) -> String {
        resp.headers().get(httputil::SERVER_TIMING).expect("Server-Timing").to_str().unwrap().to_string()
    }

    /// An empty list because the search failed must not read like a title with no subtitles.
    #[tokio::test]
    async fn a_failed_search_is_marked_degraded_and_timed() {
        let uri = format!("/{}/subtitles/movie/tt0000001.json", os_only_segment());
        let resp = handle_request(test_state(""), request(hyper::Method::GET, &uri)).await;
        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(resp.headers().get(httputil::X_DEN_DEGRADED).unwrap(), "upstream_unavailable");
        let timing = server_timing(&resp);
        assert!(timing.starts_with("opensubtitles;dur="), "{timing}");
        assert!(timing.contains(", total;dur="), "{timing}");
        assert_eq!(body_string(resp).await, r#"{"subtitles":[]}"#);
    }

    #[tokio::test]
    async fn a_cached_search_is_timed_as_a_hit_and_not_degraded() {
        let state = test_state("");
        let key = format!("{}tt0000002:0:0:", crate::cache::SEARCH_NS);
        state.cache.put(key, "[]".into(), Duration::from_secs(60));
        let uri = format!("/{}/subtitles/movie/tt0000002.json", os_only_segment());
        let resp = handle_request(state, request(hyper::Method::GET, &uri)).await;
        assert_eq!(resp.status(), StatusCode::OK);
        assert!(resp.headers().get(httputil::X_DEN_DEGRADED).is_none(), "a normal answer is not degraded");
        // Private: the URL is the install's config segment, and so is every `url` in the body.
        assert_eq!(
            resp.headers().get(hyper::header::CACHE_CONTROL).unwrap(),
            "private, max-age=3600, stale-while-revalidate=3600, stale-if-error=86400"
        );
        let timing = server_timing(&resp);
        assert!(timing.starts_with("cache;desc=hit, total;dur="), "{timing}");
    }

    /// /health changing state is logged once each way, not once per search.
    #[test]
    fn a_health_flip_is_reported_once_each_way() {
        let state = test_state("");
        for _ in 1..HEALTH_FAIL_THRESHOLD {
            assert!(!state.search_failed(), "still ok below the threshold");
        }
        assert!(state.search_failed(), "the failure that crosses the threshold flips /health");
        assert!(!state.search_failed(), "staying degraded is not a change");
        assert!(state.search_succeeded(), "the first success recovers /health");
        assert!(!state.search_succeeded(), "staying ok is not a change");
        assert!(!state.search_failed(), "one failure after a recovery does not flip it back");
    }

    fn metrics_request(auth: Option<&str>) -> Request<()> {
        let mut b = Request::builder().uri("/metrics");
        if let Some(a) = auth {
            b = b.header(hyper::header::AUTHORIZATION, a);
        }
        b.body(()).unwrap()
    }

    /// Unset means off: not an empty 200, and not a route an empty bearer header can open.
    #[tokio::test]
    async fn metrics_is_404_without_a_configured_token() {
        for auth in [None, Some("Bearer "), Some("Bearer anything")] {
            let resp = handle_request(test_state(""), metrics_request(auth)).await;
            assert_eq!(
                resp.status(),
                StatusCode::NOT_FOUND,
                "auth {auth:?} reached an unconfigured /metrics"
            );
        }
    }

    #[tokio::test]
    async fn metrics_is_404_for_a_wrong_or_missing_token() {
        for auth in [None, Some("Bearer wrong"), Some("Bearer s3cre"), Some("s3cret")] {
            let resp = handle_request(test_state_with("", "s3cret"), metrics_request(auth)).await;
            assert_eq!(resp.status(), StatusCode::NOT_FOUND, "auth {auth:?} was let in");
        }
    }

    fn request(method: hyper::Method, uri: &str) -> Request<()> {
        Request::builder().method(method).uri(uri).body(()).unwrap()
    }

    #[tokio::test]
    async fn options_is_a_preflight_on_any_path() {
        use hyper::header::{
            ACCESS_CONTROL_ALLOW_HEADERS, ACCESS_CONTROL_ALLOW_METHODS, ACCESS_CONTROL_MAX_AGE,
        };
        for uri in ["/manifest.json", "/no/such/path", "/cfg/subtitle/1.srt"] {
            let resp = handle_request(test_state(""), request(hyper::Method::OPTIONS, uri)).await;
            assert_eq!(resp.status(), StatusCode::NO_CONTENT, "{uri}");
            let h = resp.headers();
            assert_eq!(h.get(ACCESS_CONTROL_ALLOW_ORIGIN).unwrap(), "*", "{uri}");
            assert_eq!(h.get(ACCESS_CONTROL_ALLOW_METHODS).unwrap(), "GET, HEAD, OPTIONS");
            assert_eq!(h.get(ACCESS_CONTROL_ALLOW_HEADERS).unwrap(), "*");
            assert_eq!(h.get(ACCESS_CONTROL_MAX_AGE).unwrap(), "86400");
            assert!(body_string(resp).await.is_empty());
        }
    }

    /// The header is added after successful, HEAD, and error responses, so none can drop it.
    #[tokio::test]
    async fn every_reply_carries_the_wildcard_origin() {
        let ok = handle_request(test_state(""), request(hyper::Method::GET, "/health")).await;
        assert_eq!(ok.status(), StatusCode::OK);
        assert_eq!(ok.headers().get(ACCESS_CONTROL_ALLOW_ORIGIN).unwrap(), "*");

        let head = handle_request(test_state(""), request(hyper::Method::HEAD, "/health")).await;
        let missing = handle_request(test_state(""), request(hyper::Method::GET, "/no/such/path")).await;
        for resp in [head, missing] {
            assert_eq!(resp.headers().get(ACCESS_CONTROL_ALLOW_ORIGIN).unwrap(), "*", "{}", resp.status());
            // A cross-origin client reads only what is exposed, and a refusal's wait is the part it
            // most needs.
            let exposed = resp.headers().get("access-control-expose-headers").unwrap().to_str().unwrap();
            for name in ["Retry-After", "RateLimit", "RateLimit-Policy", "ETag"] {
                assert!(exposed.split(", ").any(|h| h == name), "{name} is not exposed: {exposed}");
            }
        }
    }

    /// An unknown path, a known resource with the wrong shape, and a refused /metrics all give the
    /// same JSON 404.
    #[tokio::test]
    async fn an_unknown_path_is_a_json_404() {
        let configured_manifest = format!("/{}/manifest.json", plain_segment(r#"{"osKey":"o"}"#));
        for uri in [
            "/no/such/path".to_string(),
            "/cfg/subtitles/movie".to_string(),
            "/cfg/translate/movie/tt1/Swedish.txt".to_string(),
            "/metrics".to_string(),
            "/".to_string(),
            "/configure".to_string(),
            "/config-key".to_string(),
            "/manifest.json".to_string(),
            configured_manifest,
        ] {
            let resp = handle_request(test_state(""), request(hyper::Method::GET, &uri)).await;
            assert_eq!(resp.status(), StatusCode::NOT_FOUND, "{uri}");
            assert_eq!(resp.headers().get(hyper::header::CONTENT_TYPE).unwrap(), "application/json", "{uri}");
            assert_eq!(resp.headers().get(hyper::header::CACHE_CONTROL).unwrap(), "no-store", "{uri}");
            assert_eq!(body_string(resp).await, r#"{"error":"not_found"}"#, "{uri}");
        }
    }

    #[tokio::test]
    async fn metrics_serves_prometheus_text_for_the_right_token() {
        let resp =
            handle_request(test_state_with("", "s3cret"), metrics_request(Some("Bearer s3cret"))).await;
        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(
            resp.headers().get(hyper::header::CONTENT_TYPE).unwrap(),
            "text/plain; version=0.0.4; charset=utf-8"
        );
        let body = body_string(resp).await;
        let build = format!("subtitles_build_info{{version=\"{}\"}} 1\n", release_version());
        assert!(body.contains(&build), "no build_info line in:\n{body}");
        assert!(body.contains("\nsubtitles_consecutive_failures{kind=\"opensubtitles\"} 0\n"), "{body}");
        assert!(body.contains("# TYPE subtitles_cache_disk_write_failures_total counter\n"), "{body}");
        for tier in ["tier1", "tier2"] {
            assert!(body.contains(&format!("subtitles_sync_jobs_running{{tier=\"{tier}\"}} 0")), "{body}");
            assert!(
                body.contains(&format!("subtitles_sync_queue_requests_total{{tier=\"{tier}\"}} 0")),
                "{body}"
            );
            for outcome in ["completed", "timed_out", "failed", "cancelled"] {
                assert!(
                    body.contains(&format!(
                        "subtitles_sync_jobs_total{{tier=\"{tier}\",outcome=\"{outcome}\"}} 0"
                    )),
                    "{body}"
                );
            }
        }
        assert_eq!(body.matches("# HELP subtitles_sync_jobs_total ").count(), 1, "{body}");
    }

    /// Every route is a read; anything else is refused before it can reach a handler that spends a
    /// metered download or an LLM bill.
    #[tokio::test]
    async fn a_write_method_is_a_json_405() {
        for method in [hyper::Method::POST, hyper::Method::PUT, hyper::Method::DELETE] {
            let resp = handle_request(test_state(""), request(method.clone(), "/manifest.json")).await;
            assert_eq!(resp.status(), StatusCode::METHOD_NOT_ALLOWED, "{method}");
            assert_eq!(resp.headers().get(hyper::header::ALLOW).unwrap(), "GET, HEAD, OPTIONS", "{method}");
            assert_eq!(resp.headers().get(hyper::header::CACHE_CONTROL).unwrap(), "no-store", "{method}");
            assert_eq!(resp.headers().get(ACCESS_CONTROL_ALLOW_ORIGIN).unwrap(), "*", "{method}");
            assert_eq!(body_string(resp).await, r#"{"error":"method_not_allowed"}"#, "{method}");
        }
    }

    /// HEAD is decided before any resource handler. This is the boundary that keeps a bodyless
    /// request from spending an OpenSubtitles credit, running sync, or invoking the LLM.
    #[tokio::test]
    async fn head_refuses_subtitle_work_routes_but_keeps_status_and_health() {
        let cfg = plain_segment(r#"{"osKey":"os-test","llm":{"provider":"openai","apiKey":"llm-test"}}"#);
        for route in [
            "subtitles/movie/tt0000001.json",
            "subtitle/42.srt?resync=http%3A%2F%2Fscout.test%2Fplay%2Fx",
            "translate/movie/tt0000001/Swedish.json",
            "translate/movie/tt0000001/Swedish.srt",
        ] {
            let resp =
                handle_request(test_state(""), request(hyper::Method::HEAD, &format!("/{cfg}/{route}")))
                    .await;
            assert_eq!(resp.status(), StatusCode::METHOD_NOT_ALLOWED, "{route}");
            assert_eq!(resp.headers().get(hyper::header::ALLOW).unwrap(), "GET, OPTIONS", "{route}");
            assert!(body_string(resp).await.is_empty(), "HEAD carried an error body for {route}");
        }

        let health = handle_request(test_state(""), request(hyper::Method::HEAD, "/health")).await;
        assert_eq!(health.status(), StatusCode::OK);
        assert!(body_string(health).await.is_empty());

        let status = handle_request(
            test_state(""),
            request(hyper::Method::HEAD, &format!("/{cfg}/translate/movie/tt0000001/Swedish.status")),
        )
        .await;
        assert_ne!(status.status(), StatusCode::METHOD_NOT_ALLOWED);
        assert!(body_string(status).await.is_empty());
    }

    /// An error is the fleet's JSON shape, not a text body.
    #[tokio::test]
    async fn an_error_reply_is_json() {
        let resp = handle_request(test_state(""), request(hyper::Method::GET, "/cfg/subtitle/abc.srt")).await;
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
        assert_eq!(resp.headers().get(hyper::header::CONTENT_TYPE).unwrap(), "application/json");
        assert_eq!(resp.headers().get(hyper::header::CACHE_CONTROL).unwrap(), "no-store");
        assert_eq!(body_string(resp).await, r#"{"error":"bad_file_id"}"#);
    }
    const SRT: &str = "1\n00:00:01,000 --> 00:00:02,000\nhello\n";

    fn isolated_state(name: &str) -> Arc<AppState> {
        let mut cfg = test_config("", "");
        cfg.cache_dir =
            std::env::temp_dir().join(format!("den-subtitles-test-{name}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&cfg.cache_dir);
        AppState::new(cfg)
    }

    async fn get_with(state: Arc<AppState>, uri: &str, inm: Option<&HeaderValue>) -> Response<Body> {
        let mut req = Request::builder().uri(uri);
        if let Some(v) = inm {
            req = req.header(hyper::header::IF_NONE_MATCH, v);
        }
        handle_request(state, req.body(()).unwrap()).await
    }

    /// A settled subtitle's validator names what it is, so a revalidation is answered before the cache
    /// is read or anything is downloaded. The upstream here refuses every connection and the cache is
    /// cold, so a 304 can only have come from that early check.
    #[tokio::test]
    async fn a_settled_subtitle_revalidates_without_a_download() {
        use hyper::header::{CACHE_CONTROL, ETAG};
        let seg = os_only_segment();
        let mut srt_etag = None;
        for ext in ["srt", "vtt"] {
            // Served once from a warm cache, which is how a client comes to hold the body.
            let warm = isolated_state(&format!("early-304-warm-{ext}"));
            warm.cache.put("os:91001".into(), SRT.into(), Duration::from_secs(60));
            let uri = format!("/{seg}/subtitle/91001.{ext}");
            let first = get_with(warm, &uri, None).await;
            assert_eq!(first.status(), StatusCode::OK, "{ext}");
            assert_eq!(first.headers().get(CACHE_CONTROL).unwrap(), httputil::SETTLED_SRT, "{ext}");
            let etag = first.headers().get(ETAG).expect("a settled body carries an ETag").clone();
            if ext == "srt" {
                srt_etag = Some(etag.clone());
            }

            let cold = || isolated_state(&format!("early-304-cold-{ext}"));
            let resp = get_with(cold(), &uri, Some(&etag)).await;
            assert_eq!(resp.status(), StatusCode::NOT_MODIFIED, "{ext}");
            assert_eq!(resp.headers().get(ETAG).unwrap(), &etag, "{ext}");
            assert_eq!(resp.headers().get(CACHE_CONTROL).unwrap(), httputil::SETTLED_SRT, "{ext}");
            assert!(body_string(resp).await.is_empty(), "{ext}");

            // A validator that does not match still has to fetch — and here the fetch fails.
            let resp = get_with(cold(), &uri, Some(&HeaderValue::from_static("\"other\""))).await;
            assert_eq!(resp.status(), StatusCode::BAD_GATEWAY, "{ext}");
        }

        // The SRT and VTT validators differ, so one never answers for the other.
        let srt_etag = srt_etag.unwrap();
        let vtt_uri = format!("/{seg}/subtitle/91001.vtt");
        let resp = get_with(isolated_state("early-304-cross"), &vtt_uri, Some(&srt_etag)).await;
        assert_eq!(resp.status(), StatusCode::BAD_GATEWAY);

        // Another alignment of the same file is a different body, with a different validator.
        let ref_uri = format!("/{seg}/subtitle/91001.srt?ref=91009");
        let resp = get_with(isolated_state("early-304-ref"), &ref_uri, Some(&srt_etag)).await;
        assert_ne!(resp.status(), StatusCode::NOT_MODIFIED);

        // The config is still checked first: a revoked install gets no 304 for a body it holds.
        let revoked_seg = plain_segment(&format!(r#"{{"osKey":"os-test","iid":"{IID}"}}"#));
        let uri = format!("/{revoked_seg}/subtitle/91001.srt");
        let resp = get_with(revoking_state(IID, "0"), &uri, Some(&srt_etag)).await;
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    }

    /// A stand-in's validator hashes its bytes, so it never earns the early 304: the client holding
    /// an unaligned body has to come back for the aligned one.
    #[tokio::test]
    async fn a_stand_in_subtitle_never_revalidates_early() {
        use hyper::header::{CACHE_CONTROL, ETAG};
        let seg = os_only_segment();
        let warm = isolated_state("stand-in-warm");
        warm.cache.put("os:91002".into(), SRT.into(), Duration::from_secs(60));
        // The reference cannot be downloaded, so the alignment fails and the raw body stands in.
        let uri = format!("/{seg}/subtitle/91002.srt?ref=91003");
        let first = get_with(warm, &uri, None).await;
        assert_eq!(first.status(), StatusCode::OK);
        assert_eq!(first.headers().get(httputil::X_DEN_DEGRADED).unwrap(), "sync_failed");
        assert_eq!(first.headers().get(CACHE_CONTROL).unwrap(), "private, no-cache");
        let etag = first.headers().get(ETAG).expect("a stand-in keeps its validator").clone();

        let resp = get_with(isolated_state("stand-in-cold"), &uri, Some(&etag)).await;
        assert_eq!(resp.status(), StatusCode::BAD_GATEWAY);
    }

    /// A cached hit is served as a view of the cached bytes; the body on the wire must still be
    /// exactly what was stored, however many times it is read.
    #[tokio::test]
    async fn a_cache_hit_serves_the_stored_bytes_unchanged() {
        let seg = os_only_segment();
        let uri = format!("/{seg}/subtitle/91004.srt");
        let warm = isolated_state("hit-bytes");
        warm.cache.put("os:91004".into(), SRT.into(), Duration::from_secs(60));
        for _ in 0..2 {
            let resp = get_with(warm.clone(), &uri, None).await;
            assert_eq!(resp.status(), StatusCode::OK);
            assert_eq!(body_string(resp).await, SRT);
        }
    }

    /// Tenths of a millisecond, the format every den addon's `total` uses.
    #[tokio::test]
    async fn server_timing_total_has_tenths_of_a_millisecond() {
        let uri = format!("/{}/subtitles/movie/tt0000001.json", os_only_segment());
        let resp = handle_request(test_state(""), request(hyper::Method::GET, &uri)).await;
        let timing = server_timing(&resp);
        let total = timing.rsplit("total;dur=").next().unwrap();
        assert!(total.contains('.') && total.split('.').nth(1).unwrap().len() == 1, "{timing}");
    }
}
