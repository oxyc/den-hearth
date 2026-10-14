//! Port of `internal/scout/deps.go`: `Settings` from the environment, the startup summary line, the
//! redirect policy every debrid-facing client carries, and `BuildDeps`. `Settings` is FROZEN (see
//! PORTING.md): the handler, metrics and the startup line all read it.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use den_seal::Revocation;

use crate::addbudget::{set_play_reserve, ADD_BUDGET_LIMIT, DEFAULT_PLAY_RESERVE};
use crate::cache::Cache;
use crate::cinemeta::{cinemeta_meta, Cinemeta, CINEMETA_BASE};
use crate::config::{Config, Indexer, ALL_INDEXERS, DEFAULT_INDEXERS, DISABLED_INDEXERS};
use crate::coverage::parse_time;
use crate::decisionlog::{quote_go, set_log_identity};
use crate::handler::{go_duration, DEFAULT_LIST_TTL, DEFAULT_TIMEOUT};
use crate::indexerconfig::enable_indexer_config_minting;
use crate::loglimit::emit;
use crate::manifest::manifest_version;
use crate::scrape::{make_scrapers, new_source_doer, DoError, Doer, Scraper};
use crate::seal::{parse_seal_keyring, SealKeyring};
use crate::stores::pool::build_stores;
use crate::stores::Store;
use crate::ticket::DEFAULT_PLAY_TICKET_TTL;
use crate::upstreampause::pause_on_refusal;

/// FROZEN. Go `Settings`: configuration from env so the same binary runs anywhere. No debrid secret
/// lives here; the token rides in the per-install addon URL. Go's zero `time.Time` is `None`.
#[derive(Debug, Clone)]
pub struct Settings {
    pub port: String,
    pub scrape_timeout: Duration,
    pub list_ttl: Duration,
    pub public_url: String,
    pub indexer_urls: HashMap<Indexer, String>,
    /// Byte budget for the in-memory cache.
    pub cache_bytes: usize,
    /// Where the durable tier writes. Empty disables persistence.
    pub cache_dir: String,
    /// Metadata source for the year mistag filter (default: public Cinemeta).
    pub cinemeta_url: String,
    /// CONFIG_KEY: the current X25519 private key (base64). Empty disables sealed URLs.
    pub config_key: String,
    /// CONFIG_KEYS_PREV: comma-separated prior keys.
    pub config_keys_prev: String,
    /// Bearer token for /metrics. Empty means the route 404s.
    pub metrics_token: String,
    /// MINT_INDEXER_CONFIGS: send the debrid token to comet and mediafusion to mint their config segments.
    pub mint_indexer_configs: bool,
    pub log_requests: bool,
    pub log_identity: bool,
    /// REVOKED_INSTALLS (valid install ids), CONFIG_EPOCH, and REQUIRE_INSTALL_ID.
    pub revocation: Revocation,
    /// REMUX_KEY: den-remux's service key.
    pub remux_key: String,
    /// PLAY_TICKET_TTL_SECS.
    pub play_ticket_ttl: Duration,
    /// LEGACY_PLAY_UNTIL: when the `/<config>/play` route closes. `None` means never.
    pub legacy_play_until: Option<SystemTime>,
    /// PLAY_RESERVE_ADDS.
    pub play_reserve_adds: i64,
}

/// Go `StartupSummary`: the one line an operator reads to confirm what this process is running with. Nothing
/// secret goes in it: an indexer URL override can carry an encrypted config segment, so overrides are named
/// by indexer, never by address, and keys and tokens appear only as on or off.
pub fn startup_summary(s: &Settings, persistent: bool) -> String {
    let names = |ids: &[&str]| -> String {
        if ids.is_empty() {
            "none".to_owned()
        } else {
            ids.join(",")
        }
    };
    let on_off = |b: bool| if b { "on" } else { "off" };
    let overridden: Vec<&str> = ALL_INDEXERS
        .iter()
        .copied()
        .filter(|id| s.indexer_urls.get(&Indexer((*id).to_owned())).is_some_and(|u| !u.is_empty()))
        .collect();
    let disabled: Vec<&str> = ALL_INDEXERS
        .iter()
        .copied()
        .filter(|id| DISABLED_INDEXERS.iter().any(|(name, _)| name == id))
        .collect();
    // The fleet's one startup shape: `den-<addon> <version> listening on :<port> — <shared k=v> <own k=v>`.
    format!(
        "den-scout {} listening on :{} — metrics={} log_requests={} log_identity={} sealed={} revoked={} epoch={} \
         require_iid={} remux={} indexers={} overrides={} disabled={} minting={} play_reserve={} cache={} \
         cache_persistent={}",
        manifest_version(),
        s.port,
        on_off(!s.metrics_token.is_empty()),
        on_off(s.log_requests),
        on_off(s.log_identity),
        on_off(!s.config_key.is_empty()),
        s.revocation.revoked_count(),
        s.revocation.epoch(),
        on_off(s.revocation.requires_install_id()),
        on_off(!s.remux_key.is_empty()),
        names(&DEFAULT_INDEXERS),
        names(&overridden),
        names(&disabled),
        on_off(s.mint_indexer_configs),
        s.play_reserve_adds,
        s.cache_dir,
        on_off(persistent),
    )
}

/// Go `SettingsFromEnv`. `get` returns "" for an unset variable, as Go's `os.Getenv` does.
pub fn settings_from_env(get: impl Fn(&str) -> String) -> Settings {
    let mut urls = HashMap::new();
    for id in ALL_INDEXERS {
        // The link an indexer's own configure page hands out is its manifest; the streams live beside it, so
        // `…/manifest.json/stream/…` would 404 on every scrape.
        let raw = get(&format!("{}_URL", id.to_ascii_uppercase()));
        let v = raw.strip_suffix("/manifest.json").unwrap_or(&raw);
        if !v.is_empty() {
            urls.insert(Indexer(id.to_owned()), v.to_owned());
        }
    }
    let flag = |name: &str| {
        let v = get(name);
        !v.is_empty() && v != "0"
    };
    Settings {
        port: or_default(get("PORT"), "8080"),
        scrape_timeout: dur_env(&get("SCRAPE_TIMEOUT_SECS"), DEFAULT_TIMEOUT),
        list_ttl: dur_env(&get("LIST_TTL_SECS"), DEFAULT_LIST_TTL),
        public_url: get("PUBLIC_BASE_URL"),
        indexer_urls: urls,
        cache_bytes: int_env(&get("MEMORY_CACHE_BYTES"), 48 << 20), // 48 MiB
        cache_dir: or_default(
            get("CACHE_DIR"),
            &std::env::temp_dir().join("den-scout-cache").to_string_lossy(),
        ),
        cinemeta_url: or_default(get("CINEMETA_URL"), CINEMETA_BASE),
        config_key: get("CONFIG_KEY"),
        config_keys_prev: get("CONFIG_KEYS_PREV"),
        metrics_token: get("METRICS_TOKEN"),
        mint_indexer_configs: get("MINT_INDEXER_CONFIGS").eq_ignore_ascii_case("true")
            || get("MINT_INDEXER_CONFIGS") == "1",
        log_requests: flag("LOG_REQUESTS"),
        log_identity: bool_env_or(&get("LOG_IDENTITY"), true),
        revocation: Revocation::parse(&get("REVOKED_INSTALLS"), Some(&get("CONFIG_EPOCH")), &emit)
            .requiring_install_id(flag("REQUIRE_INSTALL_ID")),
        remux_key: get("REMUX_KEY"),
        play_ticket_ttl: dur_env(&get("PLAY_TICKET_TTL_SECS"), DEFAULT_PLAY_TICKET_TTL),
        // Only read with a key set: without one there are no tickets, and the legacy route is the only one.
        legacy_play_until: parse_legacy_play_until(&get("LEGACY_PLAY_UNTIL"), !get("CONFIG_KEY").is_empty()),
        play_reserve_adds: parse_play_reserve(&get("PLAY_RESERVE_ADDS")),
    }
}

/// Go `parsePlayReserve`. Zero is a real value (no reserve), so unlike `int_env` it is kept; anything that does not
/// parse, or that would leave a prefetch no adds at all, is said and read as the default.
pub(crate) fn parse_play_reserve(v: &str) -> i64 {
    let v = v.trim();
    if v.is_empty() {
        return DEFAULT_PLAY_RESERVE;
    }
    match v.parse::<i64>() {
        Ok(n) if (0..ADD_BUDGET_LIMIT).contains(&n) => n,
        _ => {
            emit(&format!(
                "PLAY_RESERVE_ADDS={} is not an integer from 0 to {} — using the default of {}",
                quote_go(v.as_bytes()),
                ADD_BUDGET_LIMIT - 1,
                DEFAULT_PLAY_RESERVE
            ));
            DEFAULT_PLAY_RESERVE
        }
    }
}

/// Go `parseLegacyPlayUntil` reads LEGACY_PLAY_UNTIL, as den-reel reads PLAY_SIGNING_GRACE_UNTIL: unset (or no
/// tickets to move to) leaves the legacy route open (`None`), and a value that does not parse is said loudly and
/// read as already passed (the Unix epoch). A typo then refuses the stragglers and says why, instead of leaving open
/// a route that was meant to close.
pub(crate) fn parse_legacy_play_until(v: &str, tickets_on: bool) -> Option<SystemTime> {
    let v = v.trim();
    if v.is_empty() || !tickets_on {
        return None;
    }
    match parse_time(v) {
        Some(t) => t,
        None => {
            emit(&format!(
                "LEGACY_PLAY_UNTIL={} is not an RFC 3339 timestamp such as 2026-09-17T12:00:00Z — reading it as \
                 passed, so the legacy /play route is refused from now on",
                quote_go(v.as_bytes())
            ));
            Some(UNIX_EPOCH)
        }
    }
}

/// The debrid API hosts whose requests can carry the account credential in the query string (see
/// `refuse_redirect_replay`).
const CREDENTIAL_QUERY_HOSTS: [&str; 3] = ["api.torbox.app", "www.premiumize.me", "api.real-debrid.com"];

/// Go `RefuseRedirectReplay`: the redirect policy every client touching a debrid must carry.
///
/// A redirect must never REPLAY a request that carries a body. Every add is charged once through
/// spendAdd and then sent once — unless the endpoint answers 307 or 308, where Go's default policy
/// re-sends the same method AND body to the new location, up to ten hops. Measured against all three
/// stores: one charged add became two real POSTs on a single hop and ten on a redirect loop, so the
/// fifty-an-hour ceiling would have permitted five hundred real adds. (301/302/303 are safe — Go
/// downgrades them to GET and drops the body.)
///
/// It also moves a credential. Premiumize's apikey travels in the directdl FORM BODY, and Go strips the
/// Authorization header across hosts but replays the body regardless, so a 308 to a foreign host hands
/// that token to an unrelated server verbatim.
///
/// Refused by METHOD rather than by disabling redirects, because the same client reads playback links,
/// and following a CDN's redirects on a GET is the entire job there. GET and HEAD carry no body to
/// replay and keep working; anything else stops at the 3xx and the caller sees the non-2xx it is.
///
/// Reachability is the same class as the listing OOM in FOLLOWUP.md — the API hosts are constants, so it
/// needs the debrid itself, a terminator in front of it, or the operator's own proxy to answer 307/308.
/// The cost of being wrong is a multiplied add budget, so it is guarded rather than argued about.
///
/// The query string is the other channel. TorBox's requestdl carries `token=` and Premiumize's cache
/// check `apikey=`, and Go never strips a query across hosts, so a GET redirect from a debrid API host to
/// anywhere else would hand the credential over. Those redirects are refused by HOST: a playback link on
/// a CDN still follows its redirects, which is the probe's whole job on the same client.
///
/// Returns `true` when the redirect must be refused, so the caller sees the 3xx it is (Go's
/// `http.ErrUseLastResponse`). Takes the method and URL of the redirected request and of each earlier hop (`via`,
/// oldest first), not a `reqwest::Request`: reqwest's redirect `Attempt` exposes the URLs of the earlier hops but
/// not their methods, so the caller supplies them.
pub fn refuse_redirect_replay(
    method: &reqwest::Method,
    url: &reqwest::Url,
    via: &[(reqwest::Method, reqwest::Url)],
) -> bool {
    if method != reqwest::Method::GET && method != reqwest::Method::HEAD {
        return true;
    }
    if let Some((_, first)) = via.first() {
        if first.host_str().is_some_and(|h| CREDENTIAL_QUERY_HOSTS.contains(&h))
            && url.host_str() != first.host_str()
        {
            return true;
        }
    }
    false
}

/// The redirect policy for a request that is a GET or HEAD from the start (Go's `CheckRedirect` sees GET at every
/// hop): refuses by host only, and stops after ten hops as Go's client does.
fn follow_read_redirect(attempt: reqwest::redirect::Attempt) -> reqwest::redirect::Action {
    let via: Vec<_> = attempt.previous().iter().map(|url| (reqwest::Method::GET, url.clone())).collect();
    if refuse_redirect_replay(&reqwest::Method::GET, attempt.url(), &via) {
        attempt.stop()
    } else if attempt.previous().len() >= 10 {
        attempt.error("stopped after 10 redirects")
    } else {
        attempt.follow()
    }
}

/// The redirect policy for a request that carries a body. reqwest's hook is not told the method of the request it
/// is redirecting, so the method rule is applied from what the first hop's status implies: a 301, 302 or 303
/// turns the POST into a GET (as Go does, and as `refuse_redirect_replay` allows), so the chain continues as a
/// read; any other redirect would REPLAY the POST and is stopped, leaving the caller the 3xx. Scout sends only
/// GET, HEAD and POST.
fn follow_write_redirect(attempt: reqwest::redirect::Attempt) -> reqwest::redirect::Action {
    if attempt.previous().len() == 1 && !matches!(attempt.status().as_u16(), 301..=303) {
        return attempt.stop();
    }
    follow_read_redirect(attempt)
}

/// The outbound client Scout's stores, scrapers and probes share: enforces `refuse_redirect_replay` for real.
///
/// reqwest cannot judge a redirect by the method of the request it follows, so there are two clients that differ
/// only in redirect policy, and a request goes to the one for its method. Build it with the same pool and timeout
/// settings for both.
pub struct GuardedClient {
    reads: reqwest::Client,
    writes: reqwest::Client,
}

impl GuardedClient {
    /// `builder` is called twice, once per client, and must not set a redirect policy of its own.
    pub fn new(builder: impl Fn() -> reqwest::ClientBuilder) -> Result<GuardedClient, reqwest::Error> {
        Ok(GuardedClient {
            reads: builder().redirect(reqwest::redirect::Policy::custom(follow_read_redirect)).build()?,
            writes: builder().redirect(reqwest::redirect::Policy::custom(follow_write_redirect)).build()?,
        })
    }
}

#[async_trait]
impl Doer for GuardedClient {
    async fn do_request(&self, req: reqwest::Request) -> Result<reqwest::Response, DoError> {
        let client = if req.method() == reqwest::Method::GET || req.method() == reqwest::Method::HEAD {
            &self.reads
        } else {
            &self.writes
        };
        client.execute(req).await.map_err(|e| Box::new(e.without_url()) as DoError)
    }
}

/// Builds the indexer scrapers for one request's config.
pub type ScraperFactory = Arc<dyn Fn(&Config) -> Vec<Box<dyn Scraper>> + Send + Sync>;
/// Builds the debrid stores for one request's config.
pub type StoreFactory = Arc<dyn Fn(&Config) -> Vec<Box<dyn Store>> + Send + Sync>;

/// Go `Deps`: injects the environment (the cache, timeouts, public origin, and the scraper and store
/// factories, mocked in tests). Go declares it in `handler.go`; it lives here because `BuildDeps` is what
/// fills it.
pub struct Deps {
    pub cache: Arc<dyn Cache>,
    /// Client for reading the head of a resolved link. Separate from the scraper client so a probe's longer
    /// body read can't tie up the timeouts tuned for indexer queries. `None` means no probe runs.
    pub probe_client: Option<reqwest::Client>,
    /// What the probe, link check and token validation send through, in place of `probe_client` when set.
    /// `build_deps` sets it to the shared, pause-honouring client (Go's probe uses that same client); tests set
    /// their own doer here, since a `reqwest::Client` has no transport seam.
    pub(crate) probe_doer: Option<Arc<dyn Doer>>,
    pub scrape_timeout: Duration,
    pub list_ttl: Duration,
    /// Fixed public origin; when empty, fall back to forwarded headers.
    pub public_url: String,
    pub make_scrapers: ScraperFactory,
    pub make_stores: StoreFactory,
    /// Meta resolves an id → title + release year, for MOVIES only, so mistagged torrents can be dropped. Series are
    /// not looked up at all and get no mistag filter — cinemeta.rs records the two independent reasons, both of
    /// which were verified the hard way. Optional (None = no year/title filter); a lookup failure returns
    /// `(_, false)` and the list is served unfiltered.
    pub(crate) meta: Option<Arc<Cinemeta>>,
    /// Decrypts a sealed config path segment (docs/SEALED-CONFIG.md). None = sealed URLs disabled (legacy
    /// plaintext still works); the current key's public half is served at /config-key.
    pub seal_keyring: Option<Arc<SealKeyring>>,
    /// Gates /metrics. Empty disables the route entirely (404).
    pub metrics_token: String,
    /// One redacted line per response (reqlog.go).
    pub log_requests: bool,
    /// REVOKED_INSTALLS: install ids refused outright. CONFIG_EPOCH refuses every config minted under an older
    /// epoch. REQUIRE_INSTALL_ID refuses a full config with no install id. All reach play tickets too. See
    /// admitInstall.
    pub revocation: Revocation,
    /// den-remux's service key (REMUX_KEY); see `Settings::remux_key` and refuseScoped.
    pub remux_key: String,
    /// How long a /p/ ticket stays good; zero takes the default.
    pub play_ticket_ttl: Duration,
    /// Closes the /<config>/play route once passed, while tickets are on. None leaves it open.
    pub legacy_play_until: Option<SystemTime>,
}

/// A `reqwest::Client` as the `Doer` the stores, the scrapers and the link check take.
///
/// The error carries no URL: a debrid request's carries the account key, a playback link is a credential, and an
/// indexer's can carry an encrypted config segment.
pub struct ReqwestDoer(pub reqwest::Client);

#[async_trait]
impl Doer for ReqwestDoer {
    async fn do_request(&self, req: reqwest::Request) -> Result<reqwest::Response, DoError> {
        self.0.execute(req).await.map_err(|e| Box::new(e.without_url()) as DoError)
    }
}

/// Go `BuildDeps`: wires the core to a concrete HTTP client and cache.
pub fn build_deps(settings: &Settings, client: reqwest::Client, cache: Arc<dyn Cache>) -> Deps {
    // Go wraps the client's transport in PauseOnRefusal (cmd/den-scout/main.go): every debrid and indexer
    // request shares one memory of who asked to be left alone.
    let http: Arc<dyn Doer> = Arc::new(pause_on_refusal(ReqwestDoer(client.clone())));
    build_deps_with(settings, http, Some(client), cache)
}

/// `build_deps` over an outbound client built once by the caller. Go has one process-wide client, and so one memory
/// of who asked to be left alone; a process serving several handlers builds `http` once (see `shared_http`) and
/// passes it to each, so a pause one handler learns is honoured by the others.
pub fn build_deps_shared(settings: &Settings, http: Arc<dyn Doer>, cache: Arc<dyn Cache>) -> Deps {
    build_deps_with(settings, http, None, cache)
}

/// Go `main.go`'s client: `client` with its redirect policy enforced per method, under one pause memory.
pub fn shared_http(client: GuardedClient) -> Arc<dyn Doer> {
    Arc::new(pause_on_refusal(client))
}

fn build_deps_with(
    settings: &Settings,
    doer: Arc<dyn Doer>,
    probe_client: Option<reqwest::Client>,
    cache: Arc<dyn Cache>,
) -> Deps {
    // Decided once, at startup, from the operator's environment — never from a request.
    enable_indexer_config_minting(settings.mint_indexer_configs);
    set_play_reserve(settings.play_reserve_adds);
    set_log_identity(settings.log_identity);
    // A client can first use a list's tickets up to 3×LIST_TTL_SECS after the list was built (see
    // defaultPlayTicketTTL), so a TTL at or under that hands out tickets that are dead on arrival.
    if !settings.config_key.is_empty() && settings.play_ticket_ttl <= 3 * settings.list_ttl {
        emit(&format!(
            "PLAY_TICKET_TTL_SECS ({}) is not above 3×LIST_TTL_SECS ({}) — a stream list's play URLs can expire \
             before the client uses them",
            go_duration(settings.play_ticket_ttl),
            go_duration(3 * settings.list_ttl)
        ));
    }
    // Go's ProbeClient is that same client, so a probe, a link check and a token validation honour the pause too.
    let source_doer = new_source_doer();
    let indexer_urls = settings.indexer_urls.clone();
    let scraper_doer = doer.clone();
    let (store_doer, store_cache) = (doer.clone(), cache.clone());
    Deps {
        probe_doer: Some(doer.clone()),
        meta: Some(Arc::new(cinemeta_meta(doer, &settings.cinemeta_url, cache.clone()))),
        cache,
        probe_client,
        scrape_timeout: settings.scrape_timeout,
        list_ttl: settings.list_ttl,
        public_url: settings.public_url.clone(),
        make_scrapers: Arc::new(move |c| {
            make_scrapers(c, scraper_doer.clone(), source_doer.clone(), &indexer_urls)
        }),
        make_stores: Arc::new(move |c| build_stores(c, store_doer.clone(), Some(store_cache.clone()))),
        seal_keyring: build_keyring(&settings.config_key, &settings.config_keys_prev).map(Arc::new),
        metrics_token: settings.metrics_token.clone(),
        log_requests: settings.log_requests,
        revocation: settings.revocation.clone(),
        remux_key: settings.remux_key.clone(),
        play_ticket_ttl: settings.play_ticket_ttl,
        legacy_play_until: settings.legacy_play_until,
    }
}

/// Go `buildKeyring`: parses the sealed-config keyring from env. A malformed key disables sealed URLs (legacy
/// plaintext keeps working) and logs loudly rather than crashing the addon.
pub(crate) fn build_keyring(current: &str, prev: &str) -> Option<SealKeyring> {
    match parse_seal_keyring(current, prev) {
        Ok(kr) => kr,
        Err(e) => {
            emit(&format!("CONFIG_KEY invalid — sealed configs disabled: {e}"));
            None
        }
    }
}

fn or_default(v: String, d: &str) -> String {
    if v.is_empty() {
        d.to_owned()
    } else {
        v
    }
}

/// Go `durEnv` with a unit of one second: a positive whole number of seconds, else the default.
fn dur_env(v: &str, d: Duration) -> Duration {
    match v.parse::<i64>() {
        Ok(n) if n > 0 => Duration::from_secs(n as u64),
        _ => d,
    }
}

/// Go `intEnv`: a positive integer, else the default.
fn int_env(v: &str, d: usize) -> usize {
    match v.parse::<i64>() {
        Ok(n) if n > 0 => n as usize,
        _ => d,
    }
}

/// Go `boolEnvOr` reads a boolean env var that defaults to ON (unlike LogRequests's off-by-default check):
/// unset or blank keeps the default, "0", "false", "off" or "no" (any case) is off, anything else is on —
/// the rule every Den addon reads `LOG_IDENTITY` by.
pub(crate) fn bool_env_or(v: &str, d: bool) -> bool {
    let v = v.trim();
    if v.is_empty() {
        return d;
    }
    !["0", "false", "off", "no"].iter().any(|off| v.eq_ignore_ascii_case(off))
}
