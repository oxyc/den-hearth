//! The combined Den addon, under `/den`: one Stremio manifest, one `/configure` and one sealed config for what
//! were three installs (Scout's streams, Subtitles' subtitles, Reel's trailers).
//!
//! Hearth answers the addon's own routes (the configure page, the sealing key, both manifests) and hands every
//! resource to the service that serves it, in process, with the path as that service routes it and the addon's
//! terms (`den_seal::Addon`) in the request's extensions:
//!
//!   /den/<config>/stream|play|account|availability…, /den/p/<ticket>, /den/validate → Scout (its Den handler)
//!   /den/<config>/subtitles|subtitle|translate…                                     → Subtitles
//!   /den/<config>/meta|prepare…, and the links Reel mints for them
//!   (/den/play|sources|m|hls|progressive|direct|crop…), and the LAN relay's
//!   capability check (/den/_internal/validate/m/s/…)                                → Reel
//!
//! Each service reads its own member of the combined config (`den_seal::addon`) with the code that reads its own
//! config, and mints the links it answers with under `/den`, signed or sealed as the Den addon's: they come back
//! here, are admitted by this addon's revocation, and are refused on the service's own routes (and its own on
//! these). Every per-service route, key, revocation list and response is unchanged.
//!
//! Keys: `HEARTH_CONFIG_KEY` (+ `HEARTH_CONFIG_KEYS_PREV`), or when it is unset a ring derived from Scout's
//! (`Keyring::derive`), so a node that seals Scout seals this too, rotates with it, and neither opens the other's
//! configs or play tickets; `scout` among the prior keys keeps that ring after moving to a key of its own. Play
//! tickets are keyed under the addon's own HKDF info, so they stay apart from Scout's even under one key. A
//! malformed key refuses every combined config, as each service does. Revocation:
//! `HEARTH_REVOKED_INSTALLS`, `HEARTH_CONFIG_EPOCH`, `HEARTH_REQUIRE_INSTALL_ID`.

use std::io::Write;
use std::sync::{Arc, LazyLock, OnceLock};

use flate2::write::GzEncoder;
use flate2::Compression;
use hyper::header::{HeaderValue, ACCEPT_ENCODING, CONTENT_ENCODING};
use hyper::{HeaderMap, Method, Response, StatusCode};
use serde_json::{json, Value};

use den_seal::addon::{Addon, MOUNT, SLICES};
use den_seal::{InstallId, Keyring, Rejected, Revocation, MAX_EPOCH};

use crate::Body;

/// Where a request under `/den` goes.
#[derive(Debug, PartialEq, Eq)]
pub enum Route {
    /// `/den`, `/den/`, `/den/configure`: the page that builds a combined config.
    Configure,
    /// `/den/manifest.json`: the manifest before configuring.
    Manifest,
    /// `/den/config-key`: the key the page seals to, and the epoch it stamps.
    ConfigKey,
    /// `POST /den/_guest`: Edge's authenticated guest overlay RPC.
    Guest,
    /// A short-lived, unguessable indexed subtitle capability.
    Sidecar(String, usize),
    /// A short-lived end capability that seals terminal player evidence to the household.
    Receipt(String),
    /// `/den/<config>/manifest.json`, with the config segment.
    Installed(String),
    /// `/den/<config>/playback`, with the config segment. Hearth chooses the delivery path.
    Playback(String),
    /// `/den/<config>/wanted`, with the config segment: the downloads Hearth keeps looking for (`wanted`).
    Wanted(String),
    /// `PUT /den/<config>/wake/target`: register this installed TV with the local wake controller.
    WakeTarget(String),
    /// A path as Scout, Subtitles or Reel routes it.
    Scout(String),
    Subs(String),
    Reel(String),
    NotFound,
}

/// The first segments of the links Reel mints for a configured install, which it redeems where it minted them.
const REEL_LINKS: [&str; 7] = ["play", "sources", "m", "hls", "progressive", "direct", "crop"];

/// The route for `path`, what follows `/den` (`/` for the mount itself).
pub fn route(path: &str) -> Route {
    match path {
        "/" | "/configure" | "/configure/" => return Route::Configure,
        "/manifest.json" => return Route::Manifest,
        "/config-key" => return Route::ConfigKey,
        "/_guest" => return Route::Guest,
        // The page's "Test token" button: a pasted token, no config.
        "/validate" => return Route::Scout(path.to_string()),
        _ => {}
    }
    if let Some(rest) = path.strip_prefix("/_sidecar/") {
        let mut parts = rest.split('/');
        let token = parts.next().unwrap_or("");
        let track =
            parts.next().and_then(|value| value.strip_suffix(".vtt")).and_then(|value| value.parse().ok());
        if !token.is_empty() && parts.next().is_none() {
            if let Some(track) = track {
                return Route::Sidecar(token.to_owned(), track);
            }
        }
        return Route::NotFound;
    }
    if let Some(rest) = path.strip_prefix("/_receipt/") {
        let token = rest.strip_suffix("/end").unwrap_or(rest);
        if token.len() == 43
            && token.bytes().all(|byte| byte.is_ascii_alphanumeric() || byte == b'-' || byte == b'_')
        {
            return Route::Receipt(token.to_owned());
        }
        return Route::NotFound;
    }
    // The LAN relay's check of a Reel media capability, as Reel's own: only the Den addon's validate here.
    if path.starts_with("/_internal/validate/m/s/") {
        return Route::Reel(path.to_string());
    }
    let mut segments = path.strip_prefix('/').unwrap_or(path).split('/');
    let (first, second, third) = (segments.next().unwrap_or(""), segments.next(), segments.next());
    match (first, second) {
        ("p", Some(_)) => Route::Scout(path.to_string()),
        (link, _) if REEL_LINKS.contains(&link) => Route::Reel(path.to_string()),
        ("", _) => Route::NotFound,
        (config, Some("manifest.json")) if third.is_none() => Route::Installed(config.to_string()),
        (config, Some("playback")) if third.is_none() => Route::Playback(config.to_string()),
        (config, Some("wanted")) if third.is_none() => Route::Wanted(config.to_string()),
        (config, Some("wake")) if third == Some("target") && segments.next().is_none() => {
            Route::WakeTarget(config.to_string())
        }
        (_, Some("stream" | "play" | "account" | "availability")) => Route::Scout(path.to_string()),
        (_, Some("subtitles" | "subtitle" | "translate")) => Route::Subs(path.to_string()),
        (_, Some("meta" | "prepare")) => Route::Reel(path.to_string()),
        _ => Route::NotFound,
    }
}

/// What the combined key ring is derived with when it comes from Scout's.
const DERIVED_FROM_SCOUT: &[u8] = b"den-hearth/den-addon/config-key/v1";

/// The combined addon's terms, one per service member (they differ only in the member), and what hearth needs
/// to answer the addon's own routes.
pub struct Den {
    pub scout: Addon,
    pub subs: Arc<Addon>,
    pub reel: Arc<Addon>,
    /// Where the key ring came from, for the startup line.
    key_source: &'static str,
    /// Public bodies whose bytes cannot change during this process. Their ETags and serialization are paid once.
    manifest: den_http::StaticBody,
    config_key: Option<den_http::StaticBody>,
}

impl Den {
    pub fn new(
        keyring: Option<Keyring>,
        key_invalid: bool,
        revocation: Revocation,
        public_base: Option<String>,
        key_source: &'static str,
    ) -> Den {
        let member = |slice| Addon {
            keyring: keyring.clone(),
            key_invalid,
            revocation: revocation.clone(),
            public_base: public_base.clone(),
            slice,
        };
        Den {
            scout: member("scout"),
            subs: Arc::new(member("subs")),
            reel: Arc::new(member("reel")),
            key_source,
            manifest: den_http::StaticBody::json(&manifest(None)),
            config_key: keyring.as_ref().map(|keyring| {
                den_http::StaticBody::json(
                    &json!({"key": keyring.current_pub_base64(), "epoch": revocation.epoch()}),
                )
            }),
        }
    }

    /// From the environment (`get` answers "" for an unset name). `scout_key`/`scout_prev` are Scout's
    /// `CONFIG_KEY` and `CONFIG_KEYS_PREV` as Scout reads them, the ring derived from when `HEARTH_CONFIG_KEY`
    /// is unset.
    pub fn from_env(get: impl Fn(&str) -> String, scout_key: &str, scout_prev: &str) -> Den {
        let own = get("HEARTH_CONFIG_KEY");
        let (keyring, key_invalid, key_source) = if own.trim().is_empty() {
            // Scout says what is wrong with its own key; a bad one refuses here too rather than sealing off.
            match Keyring::parse(scout_key, scout_prev, &|_| {}) {
                Ok(Some(scout)) => (Some(scout.derive(DERIVED_FROM_SCOUT)), false, "derived from scout"),
                Ok(None) => (None, false, "none"),
                Err(_) => (None, true, "scout's, invalid"),
            }
        } else {
            // `scout` among the prior keys is the ring derived from Scout's key: what a node that sealed under it
            // names to keep those installs opening after it moves to a key of its own (its private half is
            // nowhere to copy from).
            let prev = get("HEARTH_CONFIG_KEYS_PREV");
            let (scout_named, prev): (Vec<&str>, Vec<&str>) =
                prev.split(',').map(str::trim).filter(|p| !p.is_empty()).partition(|p| *p == "scout");
            match Keyring::from_env(&own, &prev.join(",")) {
                Ok(Some(mut keyring)) => {
                    let scout = Keyring::parse(scout_key, scout_prev, &|_| {}).ok().flatten();
                    if scout.as_ref().is_some_and(|scout| scout.keys[0].priv_key == keyring.keys[0].priv_key)
                    {
                        eprintln!(
                            "warning: HEARTH_CONFIG_KEY is Scout's CONFIG_KEY — each opens the other's configs, \
                             kept apart only by their shape; give the Den addon a key of its own (play tickets \
                             stay apart either way)"
                        );
                    }
                    match (scout, scout_named.is_empty()) {
                        (Some(scout), false) => keyring.keys.extend(scout.derive(DERIVED_FROM_SCOUT).keys),
                        (None, false) => eprintln!(
                            "warning: HEARTH_CONFIG_KEYS_PREV names `scout`, but Scout has no valid CONFIG_KEY to \
                             derive it from — skipped"
                        ),
                        _ => {}
                    }
                    (Some(keyring), false, "own")
                }
                Ok(None) => (None, false, "none"),
                Err(e) => {
                    eprintln!(
                        "error: HEARTH_CONFIG_KEY is set but invalid ({e}) — refusing every Den addon config until \
                         it is fixed"
                    );
                    (None, true, "own, invalid")
                }
            }
        };
        let flag = |name: &str| {
            let v = get(name);
            !v.is_empty() && v != "0"
        };
        let revocation =
            Revocation::from_env(&get("HEARTH_REVOKED_INSTALLS"), Some(&get("HEARTH_CONFIG_EPOCH")))
                .requiring_install_id(flag("HEARTH_REQUIRE_INSTALL_ID"));
        let public_base = Some(get("HEARTH_PUBLIC_BASE_URL"))
            .filter(|base| !base.is_empty())
            .map(|base| format!("{}{MOUNT}", base.trim_end_matches('/')));
        Den::new(keyring, key_invalid, revocation, public_base, key_source)
    }

    /// The addon's line in hearth's startup output: nothing secret, the key only as where it came from.
    pub fn summary(&self) -> String {
        let terms = &self.scout;
        let on = |b: bool| if b { "on" } else { "off" };
        let sealed = if terms.key_invalid { "invalid" } else { on(terms.keyring.is_some()) };
        format!(
            "den addon at {MOUNT} — sealed={sealed} key={} revoked={} epoch={} require_iid={} public_base={}",
            self.key_source,
            terms.revocation.revoked_count(),
            terms.revocation.epoch(),
            on(terms.revocation.requires_install_id()),
            terms.public_base.as_deref().unwrap_or("derived"),
        )
    }

    /// Answer one of the addon's own routes (`Configure`, `Manifest`, `ConfigKey`, `Installed`).
    pub fn answer(&self, route: Route, method: &Method, headers: &HeaderMap) -> Response<Body> {
        let mut resp = match *method {
            Method::OPTIONS => den_http::preflight(),
            Method::GET | Method::HEAD => {
                den_http::apply_conditional(method, headers, self.read(route, headers))
            }
            _ => den_http::json(
                StatusCode::METHOD_NOT_ALLOWED,
                &json!({"error": "method_not_allowed"}),
                &[("allow", "GET, HEAD, OPTIONS"), ("cache-control", "no-store")],
            ),
        };
        // A browser-based Stremio client reads the manifest cross-origin.
        den_http::cors_stamp(resp.headers_mut(), "Retry-After, ETag");
        resp
    }

    fn read(&self, route: Route, headers: &HeaderMap) -> Response<Body> {
        let terms = &self.scout;
        match route {
            Route::Configure => configure_page().response(headers),
            Route::Manifest => self.manifest.response("application/json", STATIC_CACHE, &[]),
            Route::ConfigKey => {
                if terms.key_invalid {
                    return key_invalid();
                }
                let epoch = terms.revocation.epoch();
                match &terms.keyring {
                    Some(_) => self
                        .config_key
                        .as_ref()
                        .expect("a configured key has a cached public answer")
                        .response("application/json", "public, max-age=300", &[]),
                    None => den_http::json(
                        StatusCode::NOT_FOUND,
                        &json!({"error": "no_key", "epoch": epoch}),
                        &[("cache-control", "no-store")],
                    ),
                }
            }
            Route::Installed(config) => {
                if terms.key_invalid {
                    return key_invalid();
                }
                match self.open(&config) {
                    // `private`: the path carries the install's keys, and the body its install id.
                    Ok(install) => den_http::json(
                        StatusCode::OK,
                        &manifest(Some(&install)),
                        &[("cache-control", "private, max-age=3600, stale-while-revalidate=600")],
                    ),
                    Err(why) => {
                        if why != Rejected::Undecodable {
                            LOG.log(refusal(&why), || format!("den: bad_config: {why}"));
                        }
                        den_http::json(
                            StatusCode::BAD_REQUEST,
                            &json!({"error": "bad_config"}),
                            &[("cache-control", "no-store")],
                        )
                    }
                }
            }
            Route::Guest
            | Route::Sidecar(_, _)
            | Route::Receipt(_)
            | Route::Playback(_)
            | Route::Wanted(_)
            | Route::WakeTarget(_)
            | Route::Scout(_)
            | Route::Subs(_)
            | Route::Reel(_)
            | Route::NotFound => den_http::not_found(),
        }
    }

    /// A combined config segment, opened and admitted: the install it names and the members it has, each one
    /// a config its service accepts. A config with none, or with one its service would refuse, is no config:
    /// refused here rather than installed with a resource that answers every request with an error.
    pub fn open(&self, blob: &str) -> Result<Install, Rejected> {
        let terms = &self.scout;
        let opened = den_seal::open_segment(terms.keyring.as_ref(), blob)?;
        let Ok(Value::Object(whole)) = serde_json::from_slice::<Value>(&opened.json) else {
            return Err(Rejected::Undecodable);
        };
        let iid = match whole.get("iid") {
            None => None,
            Some(Value::String(iid)) if InstallId::parse(iid).is_some() => Some(iid.clone()),
            Some(_) => return Err(Rejected::Undecodable),
        };
        let ep = match whole.get("ep") {
            None => 0,
            Some(ep) => ep.as_u64().filter(|ep| *ep <= MAX_EPOCH).ok_or(Rejected::Undecodable)?,
        };
        terms.revocation.check(iid.as_deref(), ep)?;
        let mut members = Vec::new();
        for slice in SLICES.into_iter().filter(|slice| whole.contains_key(*slice)) {
            let json = den_seal::addon::slice_json(&opened.json, slice).ok_or(Rejected::Undecodable)?;
            let accepted = match slice {
                "scout" => den_scout::config::validate_config(&json).is_some(),
                "subs" => den_subtitles::is_valid_config(&json),
                _ => den_reel::is_valid_config(&json),
            };
            if !accepted {
                return Err(Rejected::Undecodable);
            }
            members.push(slice);
        }
        if members.is_empty() {
            return Err(Rejected::Undecodable);
        }
        Ok(Install { iid, members })
    }
}

/// A combined config as the manifest reads it.
#[derive(Debug)]
pub struct Install {
    pub iid: Option<String>,
    /// The members it has, in `SLICES` order.
    pub members: Vec<&'static str>,
}

static LOG: den_http::log::LimitedLog = den_http::log::LimitedLog::new();

/// The log condition for a refusal, so each is rate-limited apart.
fn refusal(why: &Rejected) -> &'static str {
    match why {
        Rejected::Undecodable => "den_bad_config",
        Rejected::Plaintext => "den_plaintext_refused",
        Rejected::Revoked { .. } => "den_install_revoked",
        Rejected::EpochTooOld { .. } => "den_install_epoch_too_old",
        Rejected::NoInstallId => "den_install_no_iid",
    }
}

/// The page, the unconfigured manifest and their windows, as every den addon serves them.
const STATIC_CACHE: &str = "public, max-age=3600, stale-while-revalidate=600";

/// The 503 a config request gets while the combined key is malformed: nothing sealed can be opened and nothing
/// plaintext trusted until it is fixed.
fn key_invalid() -> Response<Body> {
    den_http::json(
        StatusCode::SERVICE_UNAVAILABLE,
        &json!({"error": "config_key_invalid"}),
        &[("cache-control", "no-store"), ("retry-after", "60")],
    )
}

/// The release this process carries (`DEN_VERSION`, from the image), else the package version.
fn version() -> &'static str {
    static VERSION: OnceLock<String> = OnceLock::new();
    VERSION.get_or_init(|| {
        std::env::var("DEN_VERSION")
            .ok()
            .filter(|v| !v.is_empty())
            .unwrap_or_else(|| env!("CARGO_PKG_VERSION").to_owned())
    })
}

/// What each member offers, in plain words, for the configured manifest's description.
fn offers(slice: &str) -> &'static str {
    match slice {
        "scout" => "streams",
        "subs" => "subtitles",
        _ => "trailers",
    }
}

/// The manifest: every resource before configuring (`install` None), else only the configured members'. Each
/// resource is taken from its service's own manifest — its name, types and id prefixes, and its credits — so
/// the two cannot drift apart.
pub fn manifest(install: Option<&Install>) -> Value {
    let services = [
        ("scout", den_scout::manifest::unconfigured_manifest()),
        ("subs", den_subtitles::unconfigured_manifest()),
        ("reel", den_reel::unconfigured_manifest()),
    ];
    let (mut resources, mut types, mut prefixes, mut credits) =
        (Vec::new(), Vec::new(), Vec::new(), Vec::new());
    for (slice, service) in services {
        if install.is_some_and(|install| !install.members.contains(&slice)) {
            continue;
        }
        resources.push(json!({
            "name": service["resources"][0],
            "types": service["types"],
            "idPrefixes": service["idPrefixes"],
        }));
        let each = |key: &str| service[key].as_array().cloned().unwrap_or_default();
        for value in each("types") {
            if !types.contains(&value) {
                types.push(value);
            }
        }
        for value in each("idPrefixes") {
            if !prefixes.contains(&value) {
                prefixes.push(value);
            }
        }
        credits.extend(each("denAttribution"));
    }
    let description = match install {
        Some(install) => {
            let offered: Vec<&str> = install.members.iter().map(|slice| offers(slice)).collect();
            format!("Den: {} from your own node.", offered.join(", "))
        }
        None => {
            "Den's self-hosted node: streams through your debrid account, subtitles and trailers — configure \
                 to choose which."
                .to_owned()
        }
    };
    let mut manifest = json!({
        "id": "com.den.hearth",
        "version": version(),
        "name": "Den Hearth",
        "description": description,
        "resources": resources,
        "types": types,
        "idPrefixes": prefixes,
        "catalogs": [],
        "behaviorHints": { "configurable": true, "configurationRequired": install.is_none() },
    });
    if !credits.is_empty() {
        manifest["denAttribution"] = Value::Array(credits);
    }
    if let Some(iid) = install.and_then(|install| install.iid.as_deref()) {
        manifest["denInstallId"] = json!(iid);
    }
    manifest
}

/// The marker in `den_configure.html` the DenSeal bundle replaces.
const SEAL_MARKER: &str = "<!-- DenSeal bundle -->";
/// The audited browser implementation Hearth embeds in its combined configure page.
const SEAL_JS: &str = include_str!("den_seal.js");
/// Edge replaces this exact, single JavaScript expression with the versioned guest bridge context.
pub const GUEST_CONTEXT_MARKER: &str = "/*__DEN_HEARTH_GUEST_CONTEXT_V1__*/ null";

/// The configure page, with the bundle put in once.
struct ConfigurePage {
    identity: den_http::StaticBody,
    gzip: den_http::StaticBody,
}

impl ConfigurePage {
    fn response(&self, headers: &HeaderMap) -> Response<Body> {
        let gzip = accepts_gzip(headers);
        let body = if gzip { &self.gzip } else { &self.identity };
        let mut response = body.response(
            "text/html; charset=utf-8",
            STATIC_CACHE,
            &[
                ("vary", "Accept-Encoding"),
                (
                    "content-security-policy",
                    "default-src 'none'; script-src 'unsafe-inline'; style-src 'unsafe-inline'; connect-src 'self' https:; base-uri 'none'; form-action 'none'; frame-ancestors 'none'",
                ),
                ("referrer-policy", "no-referrer"),
                ("x-content-type-options", "nosniff"),
                ("x-frame-options", "DENY"),
            ],
        );
        if gzip {
            response.headers_mut().insert(CONTENT_ENCODING, HeaderValue::from_static("gzip"));
        }
        response
    }
}

fn accepts_gzip(headers: &HeaderMap) -> bool {
    let mut wildcard = None;
    let mut gzip = None;
    for value in headers.get_all(ACCEPT_ENCODING) {
        let Ok(value) = value.to_str() else { continue };
        for item in value.split(',') {
            let mut parts = item.trim().split(';');
            let coding = parts.next().unwrap_or("").trim();
            let mut quality = 1.0;
            for parameter in parts {
                let parameter = parameter.trim();
                if parameter.get(..2).is_some_and(|prefix| prefix.eq_ignore_ascii_case("q=")) {
                    quality = parameter[2..]
                        .parse::<f32>()
                        .ok()
                        .filter(|quality| (0.0..=1.0).contains(quality))
                        .unwrap_or(0.0);
                }
            }
            if coding.eq_ignore_ascii_case("gzip") {
                gzip = Some(quality);
            } else if coding == "*" {
                wildcard = Some(quality);
            }
        }
    }
    gzip.or(wildcard).is_some_and(|quality| quality > 0.0)
}

fn configure_page() -> &'static ConfigurePage {
    static PAGE: LazyLock<ConfigurePage> = LazyLock::new(|| {
        let template = include_str!("den_configure.html");
        assert_eq!(
            template.matches(GUEST_CONTEXT_MARKER).count(),
            1,
            "the guest bridge marker must occur exactly once"
        );
        let bundle = format!("<script>\n{SEAL_JS}</script>");
        let identity = den_http::StaticBody::new(template.replace(SEAL_MARKER, &bundle));
        let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
        encoder.write_all(identity.bytes()).expect("writing gzip output to memory cannot fail");
        let gzip = den_http::StaticBody::new(encoder.finish().expect("finishing gzip output cannot fail"));
        ConfigurePage { identity, gzip }
    });
    &PAGE
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_mount_and_its_own_pages_are_hearths() {
        assert_eq!(route("/"), Route::Configure);
        assert_eq!(route("/configure"), Route::Configure);
        assert_eq!(route("/configure/"), Route::Configure);
        assert_eq!(route("/migrate"), Route::NotFound);
        assert_eq!(route("/_guest"), Route::Guest);
        assert_eq!(route("/manifest.json"), Route::Manifest);
        assert_eq!(route("/config-key"), Route::ConfigKey);
        assert_eq!(route("/CFG/manifest.json"), Route::Installed("CFG".into()));
        assert_eq!(route("/CFG/playback"), Route::Playback("CFG".into()));
        assert_eq!(route(&format!("/_receipt/{}", "A".repeat(43))), Route::Receipt("A".repeat(43)));
        assert_eq!(route(&format!("/_receipt/{}/end", "A".repeat(43))), Route::Receipt("A".repeat(43)));
        assert_eq!(route(&format!("/_receipt/{}/other", "A".repeat(43))), Route::NotFound);
        assert_eq!(route(&format!("/_receipt/{}", "A".repeat(42))), Route::NotFound);
        assert_eq!(route("/CFG/wanted"), Route::Wanted("CFG".into()));
        assert_eq!(route("/CFG/wanted/more"), Route::NotFound);
    }

    #[test]
    fn each_resource_goes_to_its_service_with_the_path_it_routes() {
        for path in [
            "/CFG/stream/movie/tt1.json",
            "/CFG/play/abc",
            "/CFG/account",
            "/CFG/availability",
            "/p/TICKET",
            "/validate",
        ] {
            assert_eq!(route(path), Route::Scout(path.into()), "{path}");
        }
        for path in [
            "/CFG/subtitles/movie/tt1.json",
            "/CFG/subtitles/series/tt1:1:2/videoHash=0123456789abcdef.json",
            "/CFG/subtitle/123.srt",
            "/CFG/translate/movie/tt1/sv.json",
        ] {
            assert_eq!(route(path), Route::Subs(path.into()), "{path}");
        }
        for path in [
            "/CFG/meta/movie/tt1.json",
            "/CFG/prepare/movie/tt1.json",
            "/play/dQw4w9WgXcQ.mp4",
            "/play",
            "/sources/dQw4w9WgXcQ.json",
            "/m/s/blob",
            "/m/s/seg",
            "/hls/dQw4w9WgXcQ.m3u8",
            "/hls/seg",
            "/progressive/dQw4w9WgXcQ.mp4",
            "/direct/dQw4w9WgXcQ.json",
            "/crop/dQw4w9WgXcQ.json",
            "/_internal/validate/m/s/blob",
        ] {
            assert_eq!(route(path), Route::Reel(path.into()), "{path}");
        }
    }

    #[test]
    fn what_no_service_serves_here_is_not_found() {
        for path in [
            "/health",
            "/metrics",
            "/meta/movie/tt1.json",
            "/prepare/movie/tt1.json",
            "/_internal/validate/m/n/blob",
            "/CFG/manifest.json/more",
            "/CFG/unknown/x",
            "/CFG",
            "//stream/movie/tt1.json",
            "/remux/session",
        ] {
            assert_eq!(route(path), Route::NotFound, "{path}");
        }
    }

    #[test]
    fn the_page_carries_hearths_embedded_seal_asset_once() {
        let page = std::str::from_utf8(configure_page().identity.bytes()).unwrap();
        assert!(!page.contains(SEAL_MARKER));
        assert!(SEAL_JS.contains("function denSeal("));
        assert!(SEAL_JS.contains("window.DenSeal"));
        assert_eq!(page.matches(SEAL_JS).count(), 1);
    }

    #[tokio::test]
    async fn the_configure_page_negotiates_one_precompressed_representation() {
        let page = configure_page();
        let plain = page.response(&HeaderMap::new());
        assert!(!plain.headers().contains_key(CONTENT_ENCODING));
        assert_eq!(plain.headers()["vary"], "Accept-Encoding");

        let mut headers = HeaderMap::new();
        headers.insert(ACCEPT_ENCODING, "br, gzip".parse().unwrap());
        let gzip = page.response(&headers);
        assert_eq!(gzip.headers()[CONTENT_ENCODING], "gzip");
        assert_ne!(gzip.headers()["etag"], plain.headers()["etag"]);
        assert!(
            gzip.headers()["content-length"].to_str().unwrap().parse::<usize>().unwrap()
                < plain.headers()["content-length"].to_str().unwrap().parse::<usize>().unwrap()
        );
        let compressed = gzip.into_body();
        let compressed = http_body_util::BodyExt::collect(compressed).await.unwrap().to_bytes();
        let mut decoder = flate2::read::GzDecoder::new(compressed.as_ref());
        let mut decoded = Vec::new();
        std::io::Read::read_to_end(&mut decoder, &mut decoded).unwrap();
        assert_eq!(decoded, page.identity.bytes());

        headers.insert(ACCEPT_ENCODING, "gzip;q=0, *;q=1".parse().unwrap());
        assert!(!page.response(&headers).headers().contains_key(CONTENT_ENCODING));
        headers.insert(ACCEPT_ENCODING, "gzip;q=invalid".parse().unwrap());
        assert!(!page.response(&headers).headers().contains_key(CONTENT_ENCODING));
    }

    #[test]
    fn the_selected_configure_representation_revalidates_without_a_body() {
        let mut headers = HeaderMap::new();
        headers.insert(ACCEPT_ENCODING, "gzip".parse().unwrap());
        let etag = configure_page().response(&headers).headers()["etag"].clone();
        headers.insert("if-none-match", etag.clone());
        let response =
            den_http::apply_conditional(&Method::GET, &headers, configure_page().response(&headers));
        assert_eq!(response.status(), StatusCode::NOT_MODIFIED);
        assert_eq!(response.headers()["etag"], etag);
        assert_eq!(response.headers()["vary"], "Accept-Encoding");
    }

    #[test]
    fn compression_never_changes_no_store_key_errors() {
        let den = Den::new(None, false, Default::default(), None, "none");
        let mut headers = HeaderMap::new();
        headers.insert(ACCEPT_ENCODING, "gzip".parse().unwrap());
        let response = den.answer(Route::ConfigKey, &Method::GET, &headers);
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
        assert_eq!(response.headers()["cache-control"], "no-store");
        assert!(!response.headers().contains_key(CONTENT_ENCODING));
        assert!(!response.headers().contains_key("etag"));
        assert!(!response.headers().contains_key("vary"));
    }

    /// The page builds `{iid, ep, scout?, subs?, reel?}` in the shapes each service consumes, and calls the routes
    /// beside it.
    #[test]
    fn the_page_builds_the_shape_the_server_reads() {
        let page = include_str!("den_configure.html");
        for needle in [
            "var config = {}, parts = { scout: collectScout, subs: collectSubs };",
            "Object.assign({ iid: mintInstallId(), ep: configEpoch }, config)",
            "BASE + \"/config-key\"",
            "BASE + \"/validate\"",
            "location.origin + BASE + \"/\" + segment + \"/manifest.json\"",
            "{ debrid: [{ service: $(\"service\").value, token: token }] }",
            "{ osKey: osKey, autoSync: $(\"autoSync\").checked }",
            "{ tmdbKey: tmdb }",
            "A service stays off until its main key is added.",
            "<details class=\"service\" aria-labelledby=\"streamsTitle\">",
            "<output class=\"service-state\" id=\"streamState\">Off</output>",
            "badge.textContent = state.ready ? \"On\" : state.partial ? \"Needs key\"",
            "$(\"go\").textContent = \"Save settings\"",
            "STORE_KEY = DEN_GUEST === null ? STORE_KEY : null",
            SEAL_MARKER,
        ] {
            assert!(page.contains(needle), "the page no longer has: {needle}");
        }
        assert_eq!(
            page.matches(GUEST_CONTEXT_MARKER).count(),
            1,
            "guest bridge marker must be exact and unique"
        );
        let injected = page.replacen(
            GUEST_CONTEXT_MARKER,
            r#"{"v":2,"base":"/den/~deadbeef","saveUrl":"/den/~deadbeef/config"}"#,
            1,
        );
        assert!(!injected.contains(GUEST_CONTEXT_MARKER));
        assert!(injected.contains(
            r#"var DEN_GUEST_CONTEXT = {"v":2,"base":"/den/~deadbeef","saveUrl":"/den/~deadbeef/config"};"#
        ));
        for needle in [
            "DEN_GUEST_CONTEXT.v === 2",
            "if (DEN_GUEST === false)",
            "type=\"button\" disabled>Checking encryption…</button>",
            "if (!sealPublicKey || !window.DenSeal)",
            "if (!segment || (DEN_GUEST && !lastBuildSealed))",
            "Nothing was saved.",
            "denFetch(DEN_GUEST.saveUrl",
            "JSON.stringify({ install: segment })",
            "Could not save these settings. Please try again.",
        ] {
            assert!(page.contains(needle), "the guest bridge no longer has: {needle}");
        }
        for forbidden in ["x-den-grant", "DEN_GUEST_CONTEXT.claim"] {
            assert!(!page.contains(forbidden), "the cookie-authenticated page exposed {forbidden}");
        }
        for needle in ["scout: collectScout", "subs: collectSubs", "parts.reel = collectReel"] {
            assert!(page.contains(needle), "{needle}");
        }
    }

    #[test]
    fn the_guest_bridge_requires_sealing_before_saving() {
        let page = include_str!("den_configure.html");
        let at =
            |needle: &str| page.find(needle).unwrap_or_else(|| panic!("missing guest invariant: {needle}"));

        assert!(
            at("type=\"button\" disabled>Checking encryption…</button>")
                < at("denFetch(BASE + \"/config-key\"")
        );
        assert!(at("denFetch(BASE + \"/config-key\"") < at("$(\"go\").disabled = false"));
        assert!(at("if (!sealPublicKey || !window.DenSeal)") < at("$(\"go\").disabled = false"));
        assert!(at("if (!segment || (DEN_GUEST && !lastBuildSealed))") < at("denFetch(DEN_GUEST.saveUrl"));
        let saved = page[at("if (!r.ok) throw new Error(\"save failed\")")..].to_owned();
        assert!(
            saved.find("throw").unwrap() < saved.find("$(\"out\").classList.remove(\"hidden\")").unwrap()
        );
        assert!(
            page.contains("if (!plaintextAllowed || DEN_GUEST !== null) {\n      lastBuildSealed = false;")
        );
    }

    #[test]
    fn the_page_waits_for_sealing_and_ignores_stale_validation_answers() {
        let page = include_str!("den_configure.html");
        for needle in [
            "var sourceCheckSerial = 0",
            "if (serial !== sourceCheckSerial) return",
            "$(\"sources\").addEventListener(\"input\", invalidateSourceCheck)",
            "var tokenCheckSerial = 0",
            "if (serial !== tokenCheckSerial) return",
            "$(\"test\").disabled = true",
            "out.setAttribute(\"aria-busy\", \"true\")",
            "var plaintextAvailable = r.status === 404 && j.error === \"no_key\"",
            "if (!keyAvailable && !plaintextAvailable) throw new Error(\"key check failed\")",
            "if (keyAvailable && !window.DenSeal) throw new Error(\"seal implementation unavailable\")",
            "if (!plaintextAllowed || DEN_GUEST !== null)",
            "Could not encrypt this install. Reload the page and try again; nothing was built.",
            "$(\"go\").textContent = DEN_GUEST !== null ? \"Settings unavailable\" : \"Encryption check failed\"",
        ] {
            assert!(page.contains(needle), "the configure safety invariant is missing: {needle}");
        }
    }

    #[test]
    fn the_guest_page_uses_shared_trailers_and_rejects_an_empty_override() {
        let page = include_str!("den_configure.html");
        for needle in [
            "Uses the shared TMDB settings.",
            "$(\"trailerService\").hidden = true",
            "$(\"rememberControl\").hidden = true",
            "$(\"forgetControl\").hidden = true",
            "if (!DEN_GUEST) parts.reel = collectReel",
            "if (!DEN_GUEST && s.reel) fillReel(s.reel)",
            "Add a personal stream or subtitle override. To use only shared settings, remove your personal settings in Den.",
            "personal override",
        ] {
            assert!(page.contains(needle), "the guest configure contract is missing: {needle}");
        }
        assert!(!page.contains("if (DEN_GUEST) return config"));
        assert!(page.contains("STORE_KEY = DEN_GUEST === null ? STORE_KEY : null"));
    }

    #[test]
    fn the_page_only_persists_or_copies_credentials_after_an_explicit_action() {
        let page = include_str!("den_configure.html");
        for needle in [
            "id=\"rememberSettings\" type=\"checkbox\"",
            "Available after a refresh and cleared when this tab closes.",
            "if ($(\"rememberSettings\").checked) sessionStorage.setItem",
            "if (!this.checked && STORE_KEY) try { sessionStorage.removeItem(STORE_KEY); }",
            "navigator.clipboard.writeText(field.value).then(flashCopied",
            "if (copied) flashCopied()",
            "el.addEventListener(\"input\", markOutputStale)",
            "if (buildGeneration !== configGeneration)",
            "Earlier settings were saved, but this form changed while saving.",
        ] {
            assert!(page.contains(needle), "the credential-handling contract is missing: {needle}");
        }
        assert_eq!(page.matches("copyInstallURL()").count(), 1);
    }
}
