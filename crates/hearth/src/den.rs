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

use std::sync::{Arc, LazyLock, OnceLock};

use base64::Engine;
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
    /// `POST /den/migrate`: exchange validated legacy first-party config segments for one combined install.
    Migrate,
    /// `/den/<config>/manifest.json`, with the config segment.
    Installed(String),
    /// `/den/<config>/playback`, with the config segment. Hearth chooses the delivery path.
    Playback(String),
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
        "/migrate" => return Route::Migrate,
        // The page's "Test token" button: a pasted token, no config.
        "/validate" => return Route::Scout(path.to_string()),
        _ => {}
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
            Method::GET | Method::HEAD => den_http::apply_conditional(method, headers, self.read(route)),
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

    fn read(&self, route: Route) -> Response<Body> {
        let terms = &self.scout;
        match route {
            Route::Configure => {
                den_http::html(StatusCode::OK, configure_page(), &[("cache-control", STATIC_CACHE)])
            }
            Route::Manifest => {
                den_http::json(StatusCode::OK, &manifest(None), &[("cache-control", STATIC_CACHE)])
            }
            Route::ConfigKey => {
                if terms.key_invalid {
                    return key_invalid();
                }
                let epoch = terms.revocation.epoch();
                match &terms.keyring {
                    Some(keyring) => den_http::json(
                        StatusCode::OK,
                        &json!({"key": keyring.current_pub_base64(), "epoch": epoch}),
                        &[("cache-control", "public, max-age=300")],
                    ),
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
            Route::Migrate
            | Route::Playback(_)
            | Route::Scout(_)
            | Route::Subs(_)
            | Route::Reel(_)
            | Route::NotFound => {
                den_http::not_found()
            }
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

    /// Mint one combined config from already-opened, validated legacy members. The response is another bearer
    /// credential, never plaintext: with sealing configured it is sealed before it leaves this process.
    pub fn migrate_segment(&self, mut members: serde_json::Map<String, Value>) -> Result<String, ()> {
        if self.scout.key_invalid || members.is_empty() {
            return Err(());
        }
        members.insert("iid".into(), Value::String(InstallId::random().as_str().to_owned()));
        members.insert("ep".into(), Value::from(self.scout.revocation.epoch()));
        let json = serde_json::to_vec(&Value::Object(members)).map_err(|_| ())?;
        let bytes = match &self.scout.keyring {
            Some(keyring) => {
                let mut framed = vec![den_seal::SEALED_VERSION];
                framed.extend(den_seal::seal(&keyring.keys[0].pub_key, &json).map_err(|_| ())?);
                framed
            }
            None => json,
        };
        Ok(base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(bytes))
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

/// The `<script>` element holding the DenSeal bundle (tweetnacl + blakejs, libsodium `crypto_box_seal`) in a
/// service's configure page. All three carry the same one; it is taken from Scout's.
fn seal_bundle(page: &str) -> &str {
    let start =
        page.find("<script>\n/*\n * DenSeal bundle").expect("the configure page carries the DenSeal bundle");
    let end =
        start + page[start..].find("</script>").expect("the bundle's script closes") + "</script>".len();
    &page[start..end]
}

/// The configure page, with the bundle put in once.
fn configure_page() -> &'static str {
    static PAGE: LazyLock<String> = LazyLock::new(|| {
        include_str!("den_configure.html")
            .replace(SEAL_MARKER, seal_bundle(den_scout::configure::CONFIGURE_PAGE))
    });
    PAGE.as_str()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_mount_and_its_own_pages_are_hearths() {
        assert_eq!(route("/"), Route::Configure);
        assert_eq!(route("/configure"), Route::Configure);
        assert_eq!(route("/configure/"), Route::Configure);
        assert_eq!(route("/migrate"), Route::Migrate);
        assert_eq!(route("/manifest.json"), Route::Manifest);
        assert_eq!(route("/config-key"), Route::ConfigKey);
        assert_eq!(route("/CFG/manifest.json"), Route::Installed("CFG".into()));
        assert_eq!(route("/CFG/playback"), Route::Playback("CFG".into()));
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
    fn the_page_carries_the_seal_bundle_every_service_page_carries() {
        let page = configure_page();
        assert!(!page.contains(SEAL_MARKER));
        let bundle = seal_bundle(den_scout::configure::CONFIGURE_PAGE);
        assert!(bundle.contains("function denSeal(") && bundle.ends_with("</script>"), "{}", &bundle[..80]);
        assert!(page.contains(bundle));
        // The same bytes in Subtitles' and Reel's pages: one audited bundle, wherever it is served.
        for other in [
            include_str!("../../subtitles/src/configure.html"),
            include_str!("../../reel/src/configure.html"),
        ] {
            assert_eq!(seal_bundle(other), bundle);
        }
    }

    /// The page builds `{iid, ep, scout?, subs?, reel?}`, each member as its service's own page builds it, and
    /// calls the routes beside it.
    #[test]
    fn the_page_builds_the_shape_the_server_reads() {
        let page = include_str!("den_configure.html");
        for needle in [
            "var config = {}, parts = { scout: collectScout, subs: collectSubs, reel: collectReel };",
            "Object.assign({ iid: mintInstallId(), ep: configEpoch }, config)",
            "BASE + \"/config-key\"",
            "BASE + \"/validate\"",
            "location.origin + BASE + \"/\" + toSegment(install) + \"/manifest.json\"",
            "{ debrid: [{ service: $(\"service\").value, token: token }] }",
            "{ osKey: osKey, autoSync: $(\"autoSync\").checked }",
            "{ tmdbKey: tmdb }",
            SEAL_MARKER,
        ] {
            assert!(page.contains(needle), "the page no longer has: {needle}");
        }
        for slice in SLICES {
            assert!(page.contains(&format!("{slice}: collect")), "{slice}");
        }
    }
}
