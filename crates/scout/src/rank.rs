//! Port of `internal/scout/rank.go`: release-title parsing, filtering and ranking. Scrapers produce
//! `RawStream`, and the handler and probes consume it.

use std::collections::{BTreeMap, HashSet};
use std::sync::LazyLock;

use fancy_regex::{Regex as LookRegex, RegexBuilder as LookRegexBuilder};
use regex::{Regex, RegexBuilder};
use serde::Serialize;

use crate::attributes::stream_attributes;
use crate::client::ClientPlayable;
use crate::label::MIB;
use crate::probe::Probe;
use crate::season::{declares_episode, names_episode};

/// Go `RawStream`: one release from one indexer.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct RawStream {
    pub info_hash: String,
    pub file_idx: Option<i64>,
    pub title: String,
    /// The size of what this stream actually plays. For an episode picked out of a season pack it starts
    /// as the indexer's number (the whole pack) and is corrected to the episode's own size once a probe
    /// reads it. Every size-based decision reads this field, never `pack_size_bytes`.
    pub size_bytes: Option<i64>,
    /// The season pack's own total, kept only once a probe has shown it differs. Display-only.
    pub pack_size_bytes: Option<i64>,
    pub seeders: Option<i64>,
    pub cached: bool,
    /// Whether anyone could answer the cachedness question this request. False means `cached` is a
    /// default rather than an observation.
    pub cache_known: bool,
    pub source: String,
    /// What the file itself said, when probed. `None` means "not asked", distinct from "asked and it
    /// told us nothing".
    pub probe: Option<Probe>,
}

/// Go `gib` (rank.go).
pub(crate) const GIB: i64 = 1_073_741_824;

/// Go `rankFilters`.
#[derive(Debug, Clone, Default)]
pub(crate) struct RankFilters {
    pub exclude_cam: bool,
    pub cached_only: bool,
    pub hdr_only: bool,
    pub result_cap: i64,
    pub resolutions: Vec<String>,
    pub min_seeders: Option<i64>,
    pub max_size_gb: Option<i64>,
    pub exclude_regex: String,
    /// Sinks other resolutions below this one rather than dropping them. "" means off.
    pub prefer_resolution: String,
    /// Significant tokens of the show's title; a release naming another show sinks (never a filter).
    pub show_tokens: HashSet<String>,
    /// The requested season and episode; a release naming another episode sinks. `None` for a movie.
    pub episode: Option<[i64; 2]>,
    /// The browser the list is ranked for (X-Den-Playable); `None` for the Apple TV.
    pub client: Option<ClientPlayable>,
    /// Drops a release whose parsed year is clearly different from this one. `None` means no year filter.
    pub expected_year: Option<i64>,
    /// Significant tokens of the requested title, applied only to a release with no parseable year.
    pub expected_title_tokens: HashSet<String>,
}

/// Go `rankDebug`: where a list went, one count per filter, and what the survivors scored. Served only
/// when a request asks for it, and read by the per-build rank decision line.
#[derive(Debug, Clone, Default, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct RankDebug {
    /// Releases the indexers returned, after dedupe by infohash and before anything trims them.
    pub deduped: i64,
    /// Releases actually served, after filtering and the result cap.
    pub ranked: i64,
    /// How many releases each filter removed, keyed by filter name. A filter that removed nothing is
    /// absent rather than zero.
    #[serde(skip_serializing_if = "BTreeMap::is_empty")]
    pub dropped_by: BTreeMap<String, i64>,
    /// The served releases with the score that ordered them, in served order.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub kept: Vec<DebugScore>,
    /// Deliberately absent: indexer base URLs. See Go `rankDebug`.
    #[serde(skip_serializing_if = "String::is_empty")]
    pub note: String,
}

/// Go `debugScore`.
#[derive(Debug, Clone, Default, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct DebugScore {
    pub title: String,
    pub score: i64,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub resolution: String,
    pub cached: bool,
}

/// Go `mustRE2`. RE2's `\b`, `\w`, `\d` and `\s` are ASCII-only, which `unicode(false)` gives.
pub(crate) fn re2(pattern: &str) -> Regex {
    RegexBuilder::new(pattern).unicode(false).build().expect("static RE2 pattern")
}

/// Go `matcher`: RE2 for every pattern except the one lookbehind pattern, which Go runs on regexp2.
pub(crate) enum Matcher {
    Re2(Regex),
    Look(LookRegex),
}

impl Matcher {
    fn is_match(&self, s: &str) -> bool {
        match self {
            Matcher::Re2(r) => r.is_match(s),
            // regexp2's MatchString reports an error as no match; so does this. fancy-regex's only runtime error
            // is the backtrack limit, which `look` lifts.
            Matcher::Look(r) => r.is_match(s).unwrap_or(false),
        }
    }
}

/// Go `mustR2`: a pattern RE2 cannot express, run on fancy-regex as regexp2 runs it. regexp2 has no backtrack
/// ceiling, where fancy-regex stops after a million and reports an error that `Matcher::is_match` reads as "no
/// match" — so a long enough title would silently stop being judged. The pattern backtracks a bounded amount per
/// start position, so lifting the ceiling costs time linear in the title.
fn look(pattern: &str) -> Matcher {
    let re = LookRegexBuilder::new(pattern)
        .backtrack_limit(usize::MAX)
        .build()
        .expect("static lookaround pattern");
    Matcher::Look(re)
}

/// Go `goodSource`: markers that downgrade a bare cam/ts/scr to non-junk.
pub(crate) static RE_GOOD_SOURCE: LazyLock<Regex> =
    LazyLock::new(|| re2(r"remux|bluray|blu-ray|b[dr][ .\-_]?rip|web[ .\-_]?(dl|rip)?|hdtv|dvd[ .\-_]?rip"));

/// Go `unambiguousJunk`: always junk. The "upscaled" pattern uses a lookbehind, so it runs on fancy-regex.
pub(crate) static UNAMBIGUOUS_JUNK: LazyLock<Vec<(&'static str, Matcher)>> = LazyLock::new(|| {
    vec![
        ("cam", Matcher::Re2(re2(r"h[dq][ .\-_]?cam(rip)?|cam[ .\-_]?rip|s[ .\-]+print"))),
        ("telesync", Matcher::Re2(re2(r"telesynch?|hd[ .\-_]?ts(rip)?|ts[ .\-_]?rip"))),
        ("telecine", Matcher::Re2(re2(r"telecine|hd[ .\-_]?tc"))),
        ("screener", Matcher::Re2(re2(r"(dvd|bd|br|web|hd)[ .\-_]?scr|p(re)?dvd(rip)?|screener"))),
        ("workprint", Matcher::Re2(re2(r"workprint"))),
        ("r5", Matcher::Re2(re2(r"\br5\b"))),
        (
            "upscaled",
            look(
                r"1xbet|read[ .\-_]?note|(?<!not[ .\-_])(?<!non[ .\-_])(upscaled?|up[ .\-_]?rez)|ai[ .\-_]?(upscaled?|enhanced?)|re[ .\-_]?graded?",
            ),
        ),
    ]
});

pub(crate) static RE_BARE_CAM: LazyLock<Regex> = LazyLock::new(|| re2(r"\bcam\b"));
pub(crate) static RE_BARE_TS: LazyLock<Regex> = LazyLock::new(|| re2(r"\bts\b"));
pub(crate) static RE_BARE_SCR: LazyLock<Regex> = LazyLock::new(|| re2(r"\bscr\b"));

/// Go `containerExt`: a container extension at the end of a file name. `.ts` is MPEG transport stream,
/// not TeleSync: without this, `Show.S01E01.ts` matched the bare-TS rule and was dropped as junk. Only at
/// the end of a name (or of a line in a multi-line title); a `.TS.` in the middle is still a TeleSync tag.
/// RE2's `\s` is `[\t\n\f\r ]`, without the vertical tab the regex crate's ASCII `\s` adds, so it is spelled out.
static CONTAINER_EXT: LazyLock<Regex> =
    LazyLock::new(|| re2(r"\.(?:m2ts|ts|mkv|mp4|avi|m4v|webm)([\t\n\f\r ]|$)"));

/// Go `junkClassOf`: the junk class of an already-lowercased title, or "" for a legit source.
pub(crate) fn junk_class_of(t: &str) -> &'static str {
    let t = CONTAINER_EXT.replace_all(t, "${1}");
    for (class, re) in UNAMBIGUOUS_JUNK.iter() {
        if re.is_match(&t) {
            return class;
        }
    }
    if !RE_GOOD_SOURCE.is_match(&t) {
        if RE_BARE_CAM.is_match(&t) {
            return "cam";
        }
        if RE_BARE_TS.is_match(&t) {
            return "telesync";
        }
        if RE_BARE_SCR.is_match(&t) {
            return "screener";
        }
    }
    ""
}

/// Go `junkClass`: "" if the title is a legit source.
#[cfg(test)]
pub(crate) fn junk_class(title: &str) -> &'static str {
    junk_class_of(&str::to_lowercase(title))
}

pub(crate) static RES_2160: LazyLock<Regex> = LazyLock::new(|| re2(r"2160p?"));
pub(crate) static RES_1440: LazyLock<Regex> = LazyLock::new(|| re2(r"1440p?"));
pub(crate) static RES_1080: LazyLock<Regex> = LazyLock::new(|| re2(r"1080p?"));
pub(crate) static RES_720: LazyLock<Regex> = LazyLock::new(|| re2(r"720p?"));
pub(crate) static RES_576: LazyLock<Regex> = LazyLock::new(|| re2(r"576p?"));
pub(crate) static RES_540: LazyLock<Regex> = LazyLock::new(|| re2(r"540p?"));
pub(crate) static RES_480: LazyLock<Regex> = LazyLock::new(|| re2(r"480p?"));
pub(crate) static RES_4K_UHD: LazyLock<Regex> = LazyLock::new(|| re2(r"4k|uhd"));

/// Go `detectResolutionLower`: the coarse bucket for the resolutions filter ("" when untagged).
pub(crate) fn detect_resolution_lower(t: &str) -> &'static str {
    if RES_2160.is_match(t) {
        "2160p"
    } else if RES_1080.is_match(t) {
        "1080p"
    } else if RES_720.is_match(t) {
        "720p"
    } else if RES_480.is_match(t) || RES_576.is_match(t) || RES_540.is_match(t) {
        "480p"
    } else {
        ""
    }
}

/// Go `detectResolution`: "" when the title names none.
pub(crate) fn detect_resolution(title: &str) -> &'static str {
    detect_resolution_lower(&str::to_lowercase(title))
}

/// Go `fake4KFloor`: a "2160p" file smaller than this is not 4K in any sense that matters (mislabelled or
/// starved of bitrate), so it ranks as 1080p.
const FAKE_4K_FLOOR: i64 = 700 * MIB;

/// Go `resolutionBase`.
fn resolution_base(t: &str, size_bytes: Option<i64>) -> i64 {
    if RES_2160.is_match(t) {
        if size_bytes.is_some_and(|s| s < FAKE_4K_FLOOR) {
            return 1080;
        }
        return 4000;
    }
    if RES_1440.is_match(t) {
        1440
    } else if RES_1080.is_match(t) {
        1080
    } else if RES_720.is_match(t) {
        720
    } else if RES_576.is_match(t) {
        576
    } else if RES_540.is_match(t) {
        540
    } else if RES_480.is_match(t) {
        480
    } else if RES_4K_UHD.is_match(t) && size_bytes.unwrap_or(0) > 3 * GIB {
        4000
    } else {
        100
    }
}

pub(crate) static RE_REMUX: LazyLock<Regex> = LazyLock::new(|| re2(r"\bremux\b"));
pub(crate) static RE_BLURAY: LazyLock<Regex> = LazyLock::new(|| re2(r"bluray|blu-ray"));
pub(crate) static RE_BR_RIP: LazyLock<Regex> = LazyLock::new(|| re2(r"b[dr][ .\-_]?rip"));
pub(crate) static RE_WEB_DL: LazyLock<Regex> = LazyLock::new(|| re2(r"web[ .\-_]?dl"));
pub(crate) static RE_WEB_RIP: LazyLock<Regex> = LazyLock::new(|| re2(r"web[ .\-_]?rip"));
pub(crate) static RE_WEB: LazyLock<Regex> = LazyLock::new(|| re2(r"\bweb\b"));
pub(crate) static RE_HDTV: LazyLock<Regex> = LazyLock::new(|| re2(r"\bhdtv\b"));
pub(crate) static RE_DVD_RIP: LazyLock<Regex> = LazyLock::new(|| re2(r"dvd[ .\-_]?rip"));
pub(crate) static RE_LOW_SRC: LazyLock<Regex> = LazyLock::new(|| re2(r"tvrip|satrip|pdtv"));
/// Bare DV is the common scene tag for Dolby Vision.
pub(crate) static RE_DOVI: LazyLock<Regex> = LazyLock::new(|| re2(r"dolby vision|dolbyvision|dovi|\bdv\b"));
pub(crate) static RE_HDR10P: LazyLock<Regex> = LazyLock::new(|| re2(r"hdr10\+|hdr10plus"));
pub(crate) static RE_HDR: LazyLock<Regex> = LazyLock::new(|| re2(r"\bhdr\b|\bhlg\b"));
pub(crate) static RE_ATMOS: LazyLock<Regex> = LazyLock::new(|| re2(r"atmos"));
pub(crate) static RE_DTSX: LazyLock<Regex> = LazyLock::new(|| re2(r"dts:x|dtsx|dts-x"));
pub(crate) static RE_TRUEHD: LazyLock<Regex> = LazyLock::new(|| re2(r"truehd|true-hd"));
pub(crate) static RE_DTSHD_MA: LazyLock<Regex> = LazyLock::new(|| re2(r"dts-hd ma|dts-hd\.ma|dts-ma"));
pub(crate) static RE_DTSHD: LazyLock<Regex> = LazyLock::new(|| re2(r"dts-hd|dts hd|dtshd|flac|lpcm|pcm"));
pub(crate) static RE_EAC3: LazyLock<Regex> = LazyLock::new(|| re2(r"eac3|e-ac3|dd\+|ddp|ddplus"));
pub(crate) static RE_DTS: LazyLock<Regex> = LazyLock::new(|| re2(r"\bdts\b"));
pub(crate) static RE_AC3: LazyLock<Regex> = LazyLock::new(|| re2(r"ac3|\bdd\b|dolby digital"));
pub(crate) static RE_IS_4K: LazyLock<Regex> = LazyLock::new(|| re2(r"2160p?|4k|uhd"));
pub(crate) static RE_AV1: LazyLock<Regex> = LazyLock::new(|| re2(r"av1"));
pub(crate) static RE_3D: LazyLock<Regex> =
    LazyLock::new(|| re2(r"\b3d\b|hsbs|half[ .\-_]?sbs|sbs[ .\-_]?3d"));
pub(crate) static RE_KORSUB_HC: LazyLock<Regex> = LazyLock::new(|| re2(r"korsub|\bhc\b"));
/// HDROnly filter: matches the TS hdrOnly regex.
pub(crate) static RE_HDR_ONLY: LazyLock<Regex> =
    LazyLock::new(|| re2(r"dolby vision|dolbyvision|dovi|\bhdr\b|hdr10|\bhlg\b"));

/// Go `capSeeds`: keeps the best `limit` releases, not the first ones.
///
/// The cap bounds the debrid cache-check fan-out, so a misbehaving indexer returning thousands of tiny stream
/// objects cannot become hundreds of concurrent outbound requests. Trimming in scrape order was arbitrary:
/// past the cap a 2160p REMUX at the tail was dropped while cam junk at the head survived. Scored on
/// title-derived signals only, because this runs BEFORE the cache check. Seeders break ties.
pub(crate) fn cap_seeds(streams: &[RawStream], limit: i64) -> Vec<RawStream> {
    if limit <= 0 || streams.len() as i64 <= limit {
        return streams.to_vec();
    }
    let mut all: Vec<(RawStream, i64, i64)> =
        streams.iter().map(|s| (s.clone(), quality_score(s.clone()), s.seeders.unwrap_or(0))).collect();
    all.sort_by(|a, b| b.1.cmp(&a.1).then(b.2.cmp(&a.2)));
    all.into_iter().take(limit as usize).map(|(s, _, _)| s).collect()
}

/// Go `qualityScore`: additive, higher wins.
pub(crate) fn quality_score(s: RawStream) -> i64 {
    let t = str::to_lowercase(&s.title);
    quality_score_lower(&t, &s, junk_class_of(&t))
}

/// Go `qualityScoreLower`.
pub(crate) fn quality_score_lower(t: &str, s: &RawStream, junk: &str) -> i64 {
    let mut score = 0i64;
    if !junk.is_empty() {
        score -= 100_000;
    }
    if s.cached {
        score += 8000;
    }
    score += resolution_base(t, s.size_bytes);

    if RE_REMUX.is_match(t) {
        score += 230;
    } else if RE_BLURAY.is_match(t) || RE_BR_RIP.is_match(t) {
        score += 150;
    } else if RE_WEB_DL.is_match(t) {
        score += 75;
    } else if RE_WEB_RIP.is_match(t) {
        score += 50;
    } else if RE_WEB.is_match(t) {
        score += 75;
    }
    if RE_HDTV.is_match(t) {
        score -= 150;
    }
    if RE_DVD_RIP.is_match(t) {
        score -= 200;
    }
    if RE_LOW_SRC.is_match(t) {
        score -= 300;
    }

    if RE_DOVI.is_match(t) {
        score += 30;
    } else if RE_HDR10P.is_match(t) {
        score += 24;
    } else if RE_HDR.is_match(t) {
        score += 18;
    }

    // Audio scored by what Den DELIVERS, not the source spec: DD+/EAC3 stream-copies (and EAC3+JOC keeps
    // real Atmos), while TrueHD / DTS / DTS-HD bridge down to EAC3 5.1 (a transcode, losing Atmos + 7.1).
    let atmos = RE_ATMOS.is_match(t);
    if RE_EAC3.is_match(t) && atmos {
        score += 30; // DD+ Atmos: real Atmos, stream-copied on every route
    } else if RE_EAC3.is_match(t) {
        score += 20; // DD+ 5.1/7.1: stream-copy, native (7.1 preserved)
    } else if RE_AC3.is_match(t) {
        score += 14; // AC3: stream-copy, native 5.1
    } else if RE_DTSX.is_match(t) || RE_TRUEHD.is_match(t) || RE_DTSHD_MA.is_match(t) || RE_DTSHD.is_match(t)
    {
        score += 12; // lossless/object source, but Den bridges to EAC3 5.1 (below native DD+)
    } else if RE_DTS.is_match(t) {
        score += 8; // DTS core: bridged to 5.1
    } else if atmos {
        score += 20; // Atmos with no identifiable codec token: assume DD+ (most streaming Atmos is)
    }

    if let Some(size) = s.size_bytes {
        let add = ((size as f64 / GIB as f64) * 6.0).round() as i64;
        score += add.min(600);
    }

    let is_4k = RE_IS_4K.is_match(t);
    if RE_AV1.is_match(t) {
        if is_4k {
            score -= 1500;
        } else {
            score -= 150;
        }
    }
    if RE_3D.is_match(t) {
        score -= 2000;
    }
    if RE_KORSUB_HC.is_match(t) {
        score -= 200;
    }
    score
}

/// Go `preferenceSink`: what a soft preference costs a release that does not match it. Derived from the
/// highest score quality alone can reach (a 2160p BluRay REMUX with DV + HDR10, DD+ Atmos, at the 600-point
/// size ceiling, measures 4860), so the preferred bucket sits above every non-preferred release the ranker
/// does not actively penalise. It stays under cached (+8000), and junk (-100000) is untouched.
pub(crate) const PREFERENCE_SINK: i64 = 5000;

/// Go `wrongShowSink`: what an episode release costs when its own name says it is another show or another
/// episode. It clears everything a matching release can earn, so any release that names this show and
/// episode outranks it, and it stays above a release that cannot play at all and above junk.
const WRONG_SHOW_SINK: i64 = 30_000;

/// Go `real-debrid` anti-piracy filename block (src/rank.ts realDebridBlocked).
const RD_BLOCK_SUBSTRINGS: &[&str] = &["web-dl", "webrip", "bdrip", "hdrip", "dvdrip"];
const RD_BLOCK_DOT_ADJACENT: &[&str] = &["bluray.x264", "hdtv.x264", "hdtv.xvid", "web.x264", "web.h264"];

/// Go `realDebridBlocked`.
pub(crate) fn real_debrid_blocked(title: &str) -> bool {
    let t = str::to_lowercase(title);
    RD_BLOCK_SUBSTRINGS.iter().any(|s| t.contains(s)) || RD_BLOCK_DOT_ADJACENT.iter().any(|s| t.contains(s))
}

/// Go `titleTokenRe`: splits a release or title into alphanumeric tokens.
static TITLE_TOKEN_RE: LazyLock<Regex> = LazyLock::new(|| re2(r"[a-z0-9]+"));

/// Go `titleStop`: tokens ignored when comparing a release name to the requested title (release metadata
/// and stopwords that would create false overlaps).
const TITLE_STOP: &[&str] = &[
    "the", "a", "an", "of", "and", "to", "in", "el", "la", "los", "las", "de", "le", "il", "2160p", "1080p",
    "720p", "480p", "4k", "uhd", "hd", "x264", "x265", "h264", "h265", "hevc", "av1", "xvid", "web", "webdl",
    "webrip", "bluray", "bdrip", "brrip", "hdtv", "hdts", "hdtc", "remux", "cam", "camrip", "ts", "hdr",
    "hdr10", "dv", "sdr", "aac", "ac3", "dts", "atmos", "ddp", "mp4", "mkv", "avi", "esp", "eng", "lat",
    "dub", "dubbed", "sub", "subs", "multi", "dual",
];

/// Go `yearToken`: a plausible 4-digit film year (1900 to 2039). `release_years` then rejects matches that
/// are really resolution or codec digits.
pub(crate) static YEAR_TOKEN: LazyLock<Regex> = LazyLock::new(|| re2(r"19\d\d|20[0-3]\d"));

/// Go `foldDiacritics`: maps accented Latin letters onto their plain forms before tokenising. Tokens are
/// `[a-z0-9]` runs, so without it "Pokémon" split into "pok" and "mon".
fn fold_diacritics(s: &str) -> String {
    s.chars()
        .map(|c| match c {
            'á' | 'à' | 'â' | 'ä' | 'ã' | 'å' | 'ā' => 'a',
            'é' | 'è' | 'ê' | 'ë' | 'ē' => 'e',
            'í' | 'ì' | 'î' | 'ï' | 'ī' => 'i',
            'ó' | 'ò' | 'ô' | 'ö' | 'õ' | 'ø' | 'ō' => 'o',
            'ú' | 'ù' | 'û' | 'ü' | 'ū' => 'u',
            'ñ' => 'n',
            'ç' => 'c',
            'ý' | 'ÿ' => 'y',
            other => other,
        })
        .collect()
}

/// Go `titleTokens`: significant lowercase tokens (stopwords, format noise, bare years and single-letter
/// noise removed). Used to sanity-check a year-less release against the request.
pub(crate) fn title_tokens(s: &str) -> HashSet<String> {
    let folded = fold_diacritics(&str::to_lowercase(s));
    let mut out = HashSet::new();
    for m in TITLE_TOKEN_RE.find_iter(&folded) {
        let tok = m.as_str();
        if TITLE_STOP.contains(&tok) {
            continue;
        }
        if tok.len() == 4 && YEAR_TOKEN.is_match(tok) {
            continue; // a bare year
        }
        // Single-letter noise, but keep single digits (the "5" in "Toy Story 5").
        if tok.len() < 2 && !tok.as_bytes()[0].is_ascii_digit() {
            continue;
        }
        out.insert(tok.to_string());
    }
    out
}

/// Go `releasePrefixRe`: what comes before a release's show name and is not part of it (a group tag in
/// brackets, or a site stamp). It uses negated classes, which ASCII mode cannot express, so it stays in Unicode
/// mode, where a match is a whole codepoint as in RE2. RE2's `\s` is ASCII-only (`[\t\n\f\r ]`) and `\S` is its
/// complement, where the regex crate's Unicode `\s` also takes NBSP and the like, so both are spelled out.
static RELEASE_PREFIX_RE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"^([\t\n\f\r ]*(\[[^\]]*\]|\([^)]*\)|www\.[^\t\n\f\r ]+[\t\n\f\r ]*-))+")
        .expect("static pattern")
});

/// Go `showEndRe`: where a release's show name ends (an SxxEyy or 1x02 label, an anime " - 01", or an
/// E01/EP01 tag).
static SHOW_END_RE: LazyLock<Regex> = LazyLock::new(|| {
    re2(
        r"(?i)(\bs\d{1,2}[ ._-]*e\d{1,4}|\b\d{1,2}x\d{2,3}\b|[\t\n\f\r ]-[\t\n\f\r ]\d{1,4}\b|\bep?\d{1,4}\b)",
    )
});

/// Go `showNoise`: words a release puts between the show and the episode that name neither.
const SHOW_NOISE: &[&str] = &["season", "temporada", "saison", "staffel", "complete"];

/// Go `otherShow`: whether a release names a show with a word the requested title does not have (a spin-off
/// or a sequel series). A name with no episode marker, or one that starts with it, is not judged.
pub(crate) fn other_show(title: &str, expected: &HashSet<String>) -> bool {
    let stripped = RELEASE_PREFIX_RE.replace_all(title, "");
    let name: &str = &stripped;
    let Some(end) = SHOW_END_RE.find(name) else {
        return false;
    };
    if end.start() == 0 {
        return false;
    }
    title_tokens(&name[..end.start()]).iter().any(|tok| {
        !expected.contains(tok)
            && !SHOW_NOISE.contains(&tok.as_str())
            && !tok.trim_matches(|c: char| c.is_ascii_digit()).is_empty()
    })
}

/// Go `otherEpisode` reports whether a release's name labels an episode, and not this one: "Pokemon S00E82"
/// answered for S1E1. A range that holds the episode names it (`names_episode`).
pub(crate) fn other_episode(title: &str, season: i64, episode: i64) -> bool {
    declares_episode(title) && !names_episode(title, season, episode)
}

/// Go `titleOverlap`: whether the release shares at least one significant token with the expected title.
pub(crate) fn title_overlap(release_title: &str, expected: &HashSet<String>) -> bool {
    title_tokens(release_title).iter().any(|tok| expected.contains(tok))
}

/// Go `releaseYears`: every plausible year in a release name (usually one, e.g. "(2026)").
pub(crate) fn release_years(title: &str) -> Vec<i64> {
    let t = str::to_lowercase(title);
    let bytes = t.as_bytes();
    let mut out = Vec::new();
    for m in YEAR_TOKEN.find_iter(&t) {
        let before = if m.start() > 0 { bytes[m.start() - 1] } else { b' ' };
        let after = if m.end() < bytes.len() { bytes[m.end()] } else { b' ' };
        if before.is_ascii_digit() || before == b'x' {
            continue; // trailing digits of a bigger number, or a resolution like 1920x...
        }
        if after.is_ascii_digit() || after == b'p' || after == b'i' || after == b'x' {
            continue; // 2160p, 1920x..., etc.
        }
        if let Ok(y) = m.as_str().parse::<i64>() {
            out.push(y);
        }
    }
    out
}

/// Go `yearMismatch`: whether a title has a parseable year and none of them is within one of `expected`.
/// No parseable year means no mismatch: we can't tell, so the release is kept.
pub(crate) fn year_mismatch(title: &str, expected: i64) -> bool {
    let years = release_years(title);
    if years.is_empty() {
        return false;
    }
    !years.iter().any(|&y| y >= expected - 1 && y <= expected + 1)
}

/// Rewrites a user's pattern, written for Go's RE2, so the regex crate reads it the way RE2 does.
///
/// A `.` must match a whole codepoint, which needs the regex crate's Unicode mode, but in that mode its `\d`,
/// `\w`, `\s` and `\b` are Unicode-wide, where RE2's are ASCII-only (`\s` is `[\t\n\f\r ]`). So those are
/// spelled out, inside a class and outside one. Three more differences are mapped: `\Q…\E` (RE2 only), a `[`
/// inside a class (a nested class to the regex crate, a literal to RE2), and `&&`, `--` and `~~` inside a class
/// (set operations to the regex crate, literals to RE2).
///
/// Not mapped, so a pattern that uses them does not compile and the filter is ignored, as for any malformed
/// pattern: octal escapes (`\012`) and `\C`.
fn re2_user_pattern(p: &str) -> String {
    const SPACE: &str = r"\t\n\x0C\r ";
    const WORD: &str = "0-9A-Za-z_";
    let mut out = String::with_capacity(p.len());
    let mut chars = p.chars().peekable();
    let mut in_class = false;
    while let Some(c) = chars.next() {
        match c {
            '\\' => {
                let Some(n) = chars.next() else {
                    out.push('\\');
                    break;
                };
                match (n, in_class) {
                    ('d', false) => out.push_str("[0-9]"),
                    ('d', true) => out.push_str("0-9"),
                    ('D', _) => out.push_str("[^0-9]"),
                    ('w', false) => out.push_str(&format!("[{WORD}]")),
                    ('w', true) => out.push_str(WORD),
                    ('W', _) => out.push_str(&format!("[^{WORD}]")),
                    ('s', false) => out.push_str(&format!("[{SPACE}]")),
                    ('s', true) => out.push_str(SPACE),
                    ('S', _) => out.push_str(&format!("[^{SPACE}]")),
                    ('b', false) => out.push_str(r"(?-u:\b)"),
                    ('B', false) => out.push_str(r"(?-u:\B)"),
                    ('Q', _) => {
                        let mut literal = String::new();
                        while let Some(l) = chars.next() {
                            if l == '\\' && chars.peek() == Some(&'E') {
                                chars.next();
                                break;
                            }
                            literal.push(l);
                        }
                        out.push_str(&regex::escape(&literal));
                    }
                    _ => {
                        out.push('\\');
                        out.push(n);
                    }
                }
            }
            '[' if in_class => {
                if chars.peek() == Some(&':') {
                    // A POSIX class, `[:alpha:]`, which both syntaxes read alike: copied through its `:]`.
                    out.push('[');
                    let mut prev = '[';
                    for l in chars.by_ref() {
                        out.push(l);
                        if prev == ':' && l == ']' {
                            break;
                        }
                        prev = l;
                    }
                } else {
                    out.push_str(r"\[");
                }
            }
            '[' => {
                in_class = true;
                out.push('[');
                if chars.peek() == Some(&'^') {
                    chars.next();
                    out.push('^');
                }
                if chars.peek() == Some(&']') {
                    chars.next();
                    out.push_str(r"\]");
                }
            }
            ']' if in_class => {
                in_class = false;
                out.push(']');
            }
            '&' | '~' if in_class => {
                out.push('\\');
                out.push(c);
            }
            _ => out.push(c),
        }
    }
    out
}

/// Go `drop`/`dropN`: records `n` releases removed by a named filter. A `None` debug records nothing.
fn drop_n(debug: &mut Option<&mut RankDebug>, reason: &str, n: i64) {
    if let Some(d) = debug.as_deref_mut() {
        if n > 0 {
            *d.dropped_by.entry(reason.to_string()).or_insert(0) += n;
        }
    }
}

/// Go `rankStreams`: filters, then sorts by `qualityScore` (seeders break ties, then input order), then caps.
#[cfg(any(test, feature = "bench"))]
pub(crate) fn rank_streams(streams: &[RawStream], f: RankFilters) -> Vec<RawStream> {
    rank_streams_debug(streams, f, None)
}

/// Go `rankStreams` with its `Debug` field: the same ranking, also recording the drop counts and the
/// served scores into `debug` when one is passed.
pub(crate) fn rank_streams_debug(
    streams: &[RawStream],
    f: RankFilters,
    mut debug: Option<&mut RankDebug>,
) -> Vec<RawStream> {
    // Malformed or incompatible regex: nil, and the filter is ignored. Unicode mode, since a user pattern
    // may use `.` and RE2 matches whole codepoints there.
    let exclude_re = if f.exclude_regex.is_empty() {
        None
    } else {
        Regex::new(&format!("(?i){}", re2_user_pattern(&f.exclude_regex))).ok()
    };
    let allowed: HashSet<&str> = f.resolutions.iter().map(String::as_str).collect();

    struct Scored {
        idx: usize,
        score: i64,
        seeders: i64,
    }
    let mut out: Vec<Scored> = Vec::new();
    for (i, s) in streams.iter().enumerate() {
        let lower = str::to_lowercase(&s.title);
        let junk = junk_class_of(&lower);
        if f.exclude_cam && !junk.is_empty() {
            drop_n(&mut debug, junk, 1);
            continue;
        }
        if exclude_re.as_ref().is_some_and(|re| re.is_match(&s.title)) {
            drop_n(&mut debug, "excludeRegex", 1);
            continue;
        }
        // Drop a mistagged torrent (another film's IMDb id): its release year is clearly wrong.
        if f.expected_year.is_some_and(|y| year_mismatch(&s.title, y)) {
            drop_n(&mut debug, "yearMismatch", 1);
            continue;
        }
        // A release with no parseable year can't be year-checked; if we know the title, require at least one
        // significant-token overlap so year-less junk ("B-Bead.mp4") is dropped.
        if !f.expected_title_tokens.is_empty()
            && release_years(&s.title).is_empty()
            && !title_overlap(&s.title, &f.expected_title_tokens)
        {
            drop_n(&mut debug, "titleMismatch", 1);
            continue;
        }
        // Computed once for both the hard filter and the soft preference, and only when one is set.
        let mut res = "";
        if !allowed.is_empty() || !f.prefer_resolution.is_empty() {
            res = detect_resolution_lower(&lower);
        }
        if !allowed.is_empty() && !res.is_empty() && !allowed.contains(res) {
            drop_n(&mut debug, "resolution", 1);
            continue;
        }
        if f.hdr_only && !RE_HDR_ONLY.is_match(&lower) {
            drop_n(&mut debug, "hdrOnly", 1);
            continue;
        }
        // Only drop when the seeder count is known: many entries omit it, and treating missing as 0 would
        // discard otherwise-good results.
        if let (Some(min), Some(seeders)) = (f.min_seeders, s.seeders) {
            if seeders < min {
                drop_n(&mut debug, "minSeeders", 1);
                continue;
            }
        }
        if f.max_size_gb.is_some_and(|max| s.size_bytes.unwrap_or(0) > max.wrapping_mul(GIB)) {
            drop_n(&mut debug, "maxSizeGB", 1);
            continue;
        }
        // "Show me only what plays now" drops what is KNOWN not to be cached, not what could not be checked.
        // Cache checks batch, so one failed batch leaves a hundred releases unknown; dropping those would
        // remove playable releases with nothing saying so.
        if f.cached_only && !s.cached && s.cache_known {
            drop_n(&mut debug, "cachedOnly", 1);
            continue;
        }
        let mut score = quality_score_lower(&lower, s, junk);
        if let Some(client) = &f.client {
            score -= client.cost(s.clone(), stream_attributes(s.clone()));
        }
        // A release whose resolution could not be read is NOT sunk, matching every hard filter here.
        if !f.prefer_resolution.is_empty() && !res.is_empty() && res != f.prefer_resolution {
            score -= PREFERENCE_SINK;
        }
        if let Some([season, episode]) = f.episode {
            if (!f.show_tokens.is_empty() && other_show(&s.title, &f.show_tokens))
                || other_episode(&s.title, season, episode)
            {
                score -= WRONG_SHOW_SINK;
            }
        }
        out.push(Scored { idx: i, score, seeders: s.seeders.unwrap_or(0) });
    }

    out.sort_by(|a, b| b.score.cmp(&a.score).then(b.seeders.cmp(&a.seeders)).then(a.idx.cmp(&b.idx)));
    let cap = usize::try_from(f.result_cap).unwrap_or(0);
    if out.len() > cap {
        drop_n(&mut debug, "resultCap", (out.len() - cap) as i64);
        out.truncate(cap);
    }
    // After the cap, so the recorded scores are the ones that actually ordered what the viewer sees.
    if let Some(d) = debug {
        d.ranked = out.len() as i64;
        d.kept = out
            .iter()
            .map(|o| {
                let s = &streams[o.idx];
                DebugScore {
                    title: s.title.clone(),
                    score: o.score,
                    resolution: detect_resolution(&s.title).to_string(),
                    cached: s.cached,
                }
            })
            .collect();
    }
    out.iter().map(|o| streams[o.idx].clone()).collect()
}
