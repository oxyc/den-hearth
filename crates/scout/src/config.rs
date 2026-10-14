//! Port of `internal/scout/config.go`: the per-install config carried in the URL path segment, and its
//! validation. The types here are shared by almost every other module and are FROZEN (see PORTING.md);
//! the functions are owned by the work package that owns this file.

use std::collections::HashSet;

use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine as _;

use crate::loglimit::log_limited;
use crate::play::non_neg_int;
use crate::seal::{SealKeyring, SEALED_VERSION};
use crate::sources::normalize_sources;
use crate::util::b64url_decode;

/// FROZEN. Go `DebridService` (a string type); the three services, in resolve priority order.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum DebridService {
    /// "torbox"
    TorBox,
    /// "realdebrid"
    RealDebrid,
    /// "premiumize"
    Premiumize,
}

impl DebridService {
    /// The wire spelling: "torbox", "realdebrid", "premiumize".
    pub fn as_str(&self) -> &'static str {
        match self {
            DebridService::TorBox => "torbox",
            DebridService::RealDebrid => "realdebrid",
            DebridService::Premiumize => "premiumize",
        }
    }

    /// Go `isDebridService` for a single value: `None` for anything not one of the three.
    pub fn parse(s: &str) -> Option<DebridService> {
        DEBRID_SERVICES.into_iter().find(|d| d.as_str() == s)
    }
}

/// Go `debridServices`: the resolve/priority order (TorBox first, it has the real cache API).
pub const DEBRID_SERVICES: [DebridService; 3] =
    [DebridService::TorBox, DebridService::RealDebrid, DebridService::Premiumize];

/// FROZEN. Go `Indexer` (a string type): "torrentio", "comet", "mediafusion" or "torz".
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Indexer(pub String);

/// Go `allIndexers`: every indexer a config may name.
pub const ALL_INDEXERS: [&str; 4] = ["torrentio", "comet", "mediafusion", "torz"];

/// Go `defaultIndexers`: what an install gets when it names none.
pub const DEFAULT_INDEXERS: [&str; 2] = ["torrentio", "mediafusion"];

/// Go `configPathIndexers`: indexer -> the env var naming its per-install config URL.
pub const CONFIG_PATH_INDEXERS: [(&str, &str); 3] =
    [("comet", "COMET_URL"), ("mediafusion", "MEDIAFUSION_URL"), ("torz", "TORZ_URL")];

/// Go `disabledIndexers`: named but known-broken indexers, removed from any config, with why.
pub const DISABLED_INDEXERS: [(&str, &str); 1] = [("torz", "host no longer resolves (no DNS)")];

/// Go `validResolutions`.
pub const VALID_RESOLUTIONS: [&str; 4] = ["2160p", "1080p", "720p", "480p"];

/// Ceiling on the base64 config path segment. See `decode_config`.
pub const MAX_CONFIG_BLOB: usize = 16 << 10;
/// Ceiling on accounts in one config.
pub const MAX_DEBRID_ACCOUNTS: usize = 8;
/// Ceiling on results in one response.
pub const MAX_RESULT_CAP: i64 = 50;

/// Go `scopeAvailability`: the read-only scope a browser holds.
pub const SCOPE_AVAILABILITY: &str = "availability";

/// An install id is 16 random bytes, unpadded base64url.
pub const INSTALL_ID_BYTES: usize = 16;
pub const INSTALL_ID_LEN: usize = 22;
/// Ceiling on an epoch, so it always fits an int and a config cannot carry an absurd one.
pub const MAX_CONFIG_EPOCH: i64 = i32::MAX as i64;

/// FROZEN. Go `DebridAccount`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DebridAccount {
    pub service: DebridService,
    pub token: String,
}

/// FROZEN. Go `Filters`. Go's `*int` is `Option<i64>`; `""` for a string means unset, as in Go.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Filters {
    pub exclude_cam: bool,
    /// Empty means all.
    pub resolutions: Vec<String>,
    /// Sinks every OTHER resolution below this one instead of dropping it. "" means no preference.
    pub prefer_resolution: String,
    pub hdr_only: bool,
    /// None means no filter.
    pub min_seeders: Option<i64>,
    /// None means no filter.
    pub max_size_gb: Option<i64>,
    pub exclude_regex: String,
}

/// FROZEN. Go `Config`: a validated install config.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Config {
    pub debrid: Vec<DebridAccount>,
    pub indexers: Vec<Indexer>,
    /// The household's own stream addons (sources.go), as base URLs, scraped beside `indexers`.
    pub sources: Vec<String>,
    pub filters: Filters,
    pub cached_only: bool,
    pub result_cap: i64,
    /// "" is the full config; `SCOPE_AVAILABILITY` is the read-only one a browser holds.
    pub scope: String,
    /// The install the config was minted for, so REVOKED_INSTALLS can refuse it. "" for a config minted
    /// before installs had ids.
    pub iid: String,
    /// The CONFIG_EPOCH the config was minted under (0 when absent).
    pub epoch: i64,
    /// Indexers the config names that `DISABLED_INDEXERS` removed. Never serialised in Go (`json:"-"`).
    pub quarantined: Vec<Indexer>,
}

/// Go `validInstallID`: an install id in its one canonical spelling (strict base64url, 16 bytes).
///
/// Strict decoding refuses the non-zero trailing bits that would let the same 16 bytes be written another
/// way, which would slip past REVOKED_INSTALLS.
pub fn valid_install_id(s: &str) -> bool {
    s.len() == INSTALL_ID_LEN && URL_SAFE_NO_PAD.decode(s).is_ok_and(|b| b.len() == INSTALL_ID_BYTES)
}

/// Go `hasCountedRepeat`: whether a user-supplied regex contains a `{n}` / `{n,}` / `{n,m}` quantifier,
/// scanned with real escape state.
///
/// A counted repetition is the one regex construct whose COMPILED size is not bounded by the pattern's
/// length: `{n}` is expanded into the program, so 250 characters of `(?:x{240}){1000}` compile to 53 MiB of
/// live heap. The scan tracks escapes rather than looking one character back, because in `a\\{1000}` the
/// `\\` is an escaped backslash and the brace after it is a live quantifier.
///
/// Everything with a counted repetition is dropped, however small, rather than count-capped: a cap has to
/// reason about nested counts, which multiply. Also dropped: `\x{...}` escapes with an all-digit payload, and
/// braces inside a character class. All of that is over-rejection, which is the safe direction: the filter
/// goes away, the list does not.
pub fn has_counted_repeat(s: &str) -> bool {
    let b = s.as_bytes();
    let mut i = 0;
    while i < b.len() {
        match b[i] {
            // Whatever follows a backslash is escaped, including a backslash or a brace.
            b'\\' => i += 1,
            b'{' if counted_repeat_at(&b[i..]) => return true,
            _ => {}
        }
        i += 1;
    }
    false
}

/// Go `countedRepeatAt`: `^\{\d+(,\d*)?\}` matched against the start of `b`.
fn counted_repeat_at(b: &[u8]) -> bool {
    let skip_digits = |mut j: usize| {
        while j < b.len() && b[j].is_ascii_digit() {
            j += 1;
        }
        j
    };
    // b[0] is the opening brace.
    let min_end = skip_digits(1);
    if min_end == 1 {
        return false;
    }
    let mut j = min_end;
    if j < b.len() && b[j] == b',' {
        j = skip_digits(j + 1);
    }
    j < b.len() && b[j] == b'}'
}

/// Go `decodeConfig`: the config path segment into a validated config, or `None` (the route answers 400).
/// Sealed with no keyring, a decrypt failure, a plaintext config while sealing is on, and a scope on a
/// plaintext config all fail closed.
///
/// The segment is base64url; the decoded bytes are either a SEALED blob (first byte == `SEALED_VERSION`,
/// decrypted with the keyring) or a legacy plaintext JSON config (first byte '{'). Sealed with no keyring,
/// or a decrypt failure, fails CLOSED: it never falls through to an empty or partial config.
///
/// The segment ceiling bounds the segment, NOT what a request retains: the reply embeds a copy of the
/// segment in every stream's /play URL, so `MAX_RESULT_CAP` bounds the other half. The ceiling is 16 KiB
/// because the largest config the field caps admit (`MAX_DEBRID_ACCOUNTS` accounts with 512-character
/// tokens, all four indexers, the household sources, every filter, a 256-character regex) is about 90% of
/// it, plain and sealed. A config from /configure is ~1 KB, and ~2 KB with one source.
pub fn decode_config(kr: Option<&SealKeyring>, blob: &str) -> Option<Config> {
    // Refuse an absurd segment before decoding it. net/http accepts a request line up to its 1 MiB default,
    // and 716,770 characters of path decoded to a VALID config with 16,289 debrid accounts. The build
    // retains the blob and the parsed config for the length of a scrape, and distinct blobs defeat both the
    // cache and the singleflight, so 150 concurrent misses measured 207 MiB inside a 230 MiB GOMEMLIMIT.
    if blob.len() > MAX_CONFIG_BLOB {
        return None;
    }
    let mut data = b64url_decode(blob).ok()?;
    if data.is_empty() {
        return None;
    }
    let sealed = data[0] == SEALED_VERSION;
    if sealed {
        let kr = kr?; // sealed URL but no key configured → can't open; refuse
        data = kr.open(&data[1..]).ok()?;
    } else if kr.is_some() {
        // With a key configured, every install /configure issues is sealed, so a plaintext one was not
        // issued here: /config-key is public, and anyone could otherwise base64 their own debrid token into
        // a working install. Sealing hides the token; this refusal is what makes an install one we issued.
        // Without a key (local development) plaintext stays the only form there is.
        log_limited("plaintext-refused", "config refused: plaintext config while sealing is on");
        return None;
    }
    let raw: RawConfig = serde_json::from_slice(&data).ok()?;
    // A scope only means something sealed. A plaintext segment IS the debrid token in the clear, so a
    // "scoped" plaintext URL would hand its holder the very thing the scope exists to keep from them —
    // refusing it makes that mistake loud instead of quietly shipping the token to a browser.
    if raw.scope.is_some() && !sealed {
        return None;
    }
    validate(raw)
}

/// Go `validateConfig`, taking the decoded JSON text. The wire shape is `RawConfig`, parsed here.
pub fn validate_config(raw_json: &[u8]) -> Option<Config> {
    validate(serde_json::from_slice(raw_json).ok()?)
}

/// Go `isIndexer`.
pub fn is_indexer(s: &str) -> bool {
    ALL_INDEXERS.contains(&s)
}

/// Go `dedupeIndexers`.
pub fn dedupe_indexers(indexers: Vec<Indexer>) -> Vec<Indexer> {
    let mut seen = HashSet::with_capacity(indexers.len());
    indexers.into_iter().filter(|i| seen.insert(i.clone())).collect()
}

/// Go `clampInt`.
pub fn clamp_int(v: i64, lo: i64, hi: i64) -> i64 {
    v.max(lo).min(hi)
}

/// Go `clampPosInt`: rounds `v`; `None` if absent, non-finite or <= 0, else clamped to [1, max].
///
/// A value <= 0 is a no-op filter, so it is treated as unset (audit #12).
pub fn clamp_pos_int(v: Option<f64>, max: i64) -> Option<i64> {
    let n = v.filter(|v| is_finite(*v))?.round() as i64;
    (n > 0).then_some(n.min(max))
}

/// Go `isFinite`.
pub fn is_finite(f: f64) -> bool {
    f.is_finite()
}

/// Go `containsString`. A linear scan, which is right here: the slice it guards holds at most the four
/// valid resolutions, so a map would cost an allocation to save nothing.
pub fn contains_string(xs: &[String], want: &str) -> bool {
    xs.iter().any(|x| x.as_str() == want)
}

/// Go `rawConfig`: the untrusted wire JSON, before validation and clamping. Every field is an `Option`
/// because Go's pointer fields distinguish an absent value from a zero one, and a JSON `null` is absent.
#[derive(serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct RawConfig {
    debrid: Option<Vec<RawAccount>>,
    indexers: Option<Vec<String>>,
    sources: Option<Vec<String>>,
    filters: Option<RawFilters>,
    cached_only: Option<bool>,
    result_cap: Option<f64>,
    scope: Option<String>,
    iid: Option<String>,
    #[serde(rename = "ep")]
    epoch: Option<f64>,
}

/// One entry of `RawConfig::debrid`. A missing field reads as "", which validation then skips, as Go does.
#[derive(serde::Deserialize)]
struct RawAccount {
    #[serde(default)]
    service: String,
    #[serde(default)]
    token: String,
}

#[derive(serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct RawFilters {
    exclude_cam: Option<bool>,
    resolutions: Option<Vec<String>>,
    prefer_resolution: Option<String>,
    hdr_only: Option<bool>,
    min_seeders: Option<f64>,
    #[serde(rename = "maxSizeGB")]
    max_size_gb: Option<f64>,
    exclude_regex: Option<String>,
}

/// Go `validateConfig`: strict-whitelists and clamps an untrusted config (mirrors src/config.ts).
fn validate(raw: RawConfig) -> Option<Config> {
    let mut debrid = Vec::new();
    for d in raw.debrid.unwrap_or_default() {
        let Some(service) = DebridService::parse(&d.service) else {
            continue;
        };
        if d.token.is_empty() || d.token.len() > 512 {
            continue;
        }
        debrid.push(DebridAccount { service, token: d.token });
        // Stop at the ceiling rather than keeping every account named. With the blob capped this can no
        // longer be large, but it is the slice a whole build carries, and a bound here is one line.
        if debrid.len() == MAX_DEBRID_ACCOUNTS {
            break;
        }
    }
    if debrid.is_empty() {
        return None;
    }

    let idx: Vec<Indexer> =
        raw.indexers.unwrap_or_default().into_iter().filter(|i| is_indexer(i)).map(Indexer).collect();
    let idx = dedupe_indexers(idx); // audit #10
    let (mut kept, mut quarantined) = (Vec::new(), Vec::new());
    for i in idx {
        if DISABLED_INDEXERS.iter().any(|(name, _)| *name == i.0) {
            quarantined.push(i);
        } else {
            kept.push(i);
        }
    }
    let idx = if kept.is_empty() {
        DEFAULT_INDEXERS.iter().map(|s| Indexer((*s).to_owned())).collect()
    } else {
        kept
    };

    let mut f = Filters { exclude_cam: true, ..Default::default() }; // default on
    if let Some(rf) = raw.filters {
        if let Some(v) = rf.exclude_cam {
            f.exclude_cam = v;
        }
        // Deduped as well as whitelisted. There are four valid values, so a repeated one carries no
        // information, and a config naming "1080p" ten thousand times retained ten thousand strings for the
        // length of the build. (Not a per-release scan: rankStreams turns this into a set once per call, so
        // the cost was ten thousand map inserts per build plus the retention — the retention is the reason.)
        for r in rf.resolutions.unwrap_or_default() {
            if VALID_RESOLUTIONS.contains(&r.as_str()) && !contains_string(&f.resolutions, &r) {
                f.resolutions.push(r);
            }
        }
        // Whitelisted like every other resolution field; an unknown value is dropped, not carried, so it
        // becomes "no preference" rather than a preference nothing can ever satisfy.
        if let Some(p) = rf.prefer_resolution.filter(|p| VALID_RESOLUTIONS.contains(&p.as_str())) {
            f.prefer_resolution = p;
        }
        if let Some(v) = rf.hdr_only {
            f.hdr_only = v;
        }
        // audit #12: minSeeders/maxSizeGB <= 0 is a no-op filter, so treat it as unset (nil).
        f.min_seeders = clamp_pos_int(rf.min_seeders, 100_000);
        f.max_size_gb = clamp_pos_int(rf.max_size_gb, 1_000);
        if let Some(mut s) = rf.exclude_regex {
            // Go cuts at byte 256. Cut at a character boundary instead, so the cut never splits a character.
            if s.len() > 256 {
                let mut end = 256;
                while !s.is_char_boundary(end) {
                    end -= 1;
                }
                s.truncate(end);
            }
            // Counted repetition is dropped, and the length cap is not enough on its own. RE2 is linear at
            // MATCH time, but nothing bounded it at COMPILE time: `{n}` is expanded into the program, so 250
            // characters inside the cap, from an unauthenticated caller, compile to 53 MiB of live heap.
            // Dropped, not count-capped: this filter excludes words a viewer does not want in a release name,
            // and none of that needs counted repetition. Dropped, not rejected, to match the existing tolerance
            // for a malformed pattern.
            if has_counted_repeat(&s) {
                s.clear();
            }
            f.exclude_regex = s;
        }
    }

    // Default OFF. A config that omits this is deferring to the server, and hiding everything the debrid
    // hasn't already fetched is too strong a default to inherit silently: it can leave a title with one
    // scrap of a release, or nothing at all, while several good ones sit a download away. An explicit
    // `cachedOnly: true` still pins it for anyone who wants only instant playback.
    let cached_only = raw.cached_only.unwrap_or(false);
    let mut result_cap = 20;
    if let Some(rc) = raw.result_cap.filter(|v| is_finite(*v)) {
        result_cap = clamp_int(rc.round() as i64, 1, MAX_RESULT_CAP);
    }

    // An unknown scope fails closed: read as the full config, a typo would grant everything.
    let scope = match raw.scope {
        None => String::new(),
        Some(s) if s == SCOPE_AVAILABILITY => s,
        Some(_) => return None,
    };

    // Both fail closed, like the scope: an id that does not validate cannot be matched against the revoked
    // list, and an epoch that does not parse cannot be compared with CONFIG_EPOCH.
    let iid = match raw.iid {
        None => String::new(),
        Some(s) if valid_install_id(&s) => s,
        Some(_) => return None,
    };
    let epoch = match raw.epoch {
        None => 0,
        Some(e) => {
            let n = non_neg_int(Some(e))?;
            if e > MAX_CONFIG_EPOCH as f64 {
                return None;
            }
            n
        }
    };

    Some(Config {
        debrid,
        indexers: idx,
        sources: normalize_sources(&raw.sources.unwrap_or_default()),
        filters: f,
        cached_only,
        result_cap,
        scope,
        iid,
        epoch,
        quarantined,
    })
}
