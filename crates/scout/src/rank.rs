//! Port of `internal/scout/rank.go`: release-title parsing, filtering and ranking. Scrapers produce
//! `RawStream`, and the handler and probes consume it.

use std::cmp::Reverse;
use std::collections::{BTreeMap, HashSet};
use std::sync::{LazyLock, OnceLock};

use regex::{Regex, RegexBuilder};
use serde::Serialize;

use crate::attributes::cost_attributes;
use crate::client::ClientPlayable;
use crate::label::MIB;
use crate::probe::Probe;
use crate::season::{declares_episode, names_episode};
use crate::traits::{fold_diacritics, Pat, ReleaseTraits, TagKind, YEAR_TOKEN};

/// The `ReleaseTraits` of a `RawStream`'s title, parsed the first time anything asks and then carried with the
/// stream (a clone carries it too). It is a cache of `title`, so it takes no part in equality, and a title is never
/// rewritten once a stream exists.
#[derive(Clone, Default)]
pub struct TraitsCell(OnceLock<ReleaseTraits>);

impl PartialEq for TraitsCell {
    fn eq(&self, _: &Self) -> bool {
        true
    }
}

impl std::fmt::Debug for TraitsCell {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("TraitsCell")
    }
}

impl RawStream {
    /// What the title says, parsed once per stream.
    pub(crate) fn traits(&self) -> &ReleaseTraits {
        self.traits.0.get_or_init(|| ReleaseTraits::parse(&self.title))
    }
}

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
    /// The parsed title; see `RawStream::traits`.
    pub traits: TraitsCell,
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
    /// ISO 639-1 languages a release's audio may be in without sinking: the title's original language(s), plus
    /// the viewer's preferred audio where the request names it. Empty means the original language is unknown,
    /// and then no release is judged by its language tags (see `foreign_language_sink`).
    pub languages: HashSet<String>,
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

/// Go `junkClass`: "" if the title is a legit source.
#[cfg(test)]
pub(crate) fn junk_class(title: &str) -> &'static str {
    ReleaseTraits::parse(title).junk_class()
}

/// Go `detectResolutionLower`: the coarse bucket for the resolutions filter ("" when untagged).
pub(crate) fn resolution_bucket(t: &ReleaseTraits) -> &'static str {
    if t.has(Pat::Res2160) {
        "2160p"
    } else if t.has(Pat::Res1080) {
        "1080p"
    } else if t.has(Pat::Res720) {
        "720p"
    } else if t.has(Pat::Res480) || t.has(Pat::Res576) || t.has(Pat::Res540) {
        "480p"
    } else {
        ""
    }
}

/// Go `detectResolution`: "" when the title names none.
#[cfg(test)]
pub(crate) fn detect_resolution(title: &str) -> &'static str {
    resolution_bucket(&ReleaseTraits::parse(title))
}

/// Go `fake4KFloor`: a "2160p" file smaller than this is not 4K in any sense that matters (mislabelled or
/// starved of bitrate), so it ranks as 1080p.
const FAKE_4K_FLOOR: i64 = 700 * MIB;

/// Go `resolutionBase`.
fn resolution_base(t: &ReleaseTraits, size_bytes: Option<i64>) -> i64 {
    if t.has(Pat::Res2160) {
        if size_bytes.is_some_and(|s| s < FAKE_4K_FLOOR) {
            return 1080;
        }
        return 4000;
    }
    if t.has(Pat::Res1440) {
        1440
    } else if t.has(Pat::Res1080) {
        1080
    } else if t.has(Pat::Res720) {
        720
    } else if t.has(Pat::Res576) {
        576
    } else if t.has(Pat::Res540) {
        540
    } else if t.has(Pat::Res480) {
        480
    } else if t.has(Pat::Res4kUhd) && size_bytes.unwrap_or(0) > 3 * GIB {
        4000
    } else {
        100
    }
}

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
    // Rank references, not owned streams. This cap exists specifically for hostile/accidental
    // multi-thousand-result scrapes; cloning every title, source and probe before throwing nearly all
    // of them away made the safety valve itself scale with the input it was meant to contain.
    let mut all: Vec<(usize, i64, i64)> = streams
        .iter()
        .enumerate()
        .map(|(idx, s)| (idx, quality_score_ref(s), s.seeders.unwrap_or(0)))
        .collect();
    all.sort_by(|a, b| b.1.cmp(&a.1).then(b.2.cmp(&a.2)));
    all.into_iter().take(limit as usize).map(|(idx, _, _)| streams[idx].clone()).collect()
}

/// Go `qualityScore`: additive, higher wins.
#[cfg(test)]
pub(crate) fn quality_score(s: RawStream) -> i64 {
    quality_score_ref(&s)
}

/// Borrowed scorer used by ranking paths that already own a stream collection.
pub(crate) fn quality_score_ref(s: &RawStream) -> i64 {
    quality_score_traits(s.traits(), s)
}

/// Go `qualityScoreLower`, over the parsed title.
pub(crate) fn quality_score_traits(t: &ReleaseTraits, s: &RawStream) -> i64 {
    let mut score = 0i64;
    if !t.junk_class().is_empty() {
        score -= 100_000;
    }
    if s.cached {
        score += 8000;
    }
    score += resolution_base(t, s.size_bytes);

    if t.has(Pat::Remux) {
        score += 230;
    } else if t.has(Pat::Bluray) || t.has(Pat::BrRip) {
        score += 150;
    } else if t.has(Pat::WebDl) {
        score += 75;
    } else if t.has(Pat::WebRip) {
        score += 50;
    } else if t.has(Pat::Web) {
        score += 75;
    }
    if t.has(Pat::Hdtv) {
        score -= 150;
    }
    if t.has(Pat::DvdRip) {
        score -= 200;
    }
    if t.has(Pat::LowSrc) {
        score -= 300;
    }

    if t.has(Pat::Dovi) {
        score += 30;
    } else if t.has(Pat::Hdr10p) {
        score += 24;
    } else if t.has(Pat::HdrPlain) || t.has(Pat::Hlg) {
        score += 18;
    }

    // Audio scored by what Den DELIVERS, not the source spec: DD+/EAC3 stream-copies (and EAC3+JOC keeps
    // real Atmos), while TrueHD / DTS / DTS-HD bridge down to EAC3 5.1 (a transcode, losing Atmos + 7.1).
    let atmos = t.has(Pat::Atmos);
    if t.has(Pat::Eac3) && atmos {
        score += 30; // DD+ Atmos: real Atmos, stream-copied on every route
    } else if t.has(Pat::Eac3) {
        score += 20; // DD+ 5.1/7.1: stream-copy, native (7.1 preserved)
    } else if t.has(Pat::Ac3) {
        score += 14; // AC3: stream-copy, native 5.1
    } else if t.has(Pat::Dtsx)
        || t.has(Pat::Truehd)
        || t.has(Pat::DtshdMa)
        || t.has(Pat::DtshdAttr)
        || t.has(Pat::Flac)
    {
        score += 12; // lossless/object source, but Den bridges to EAC3 5.1 (below native DD+)
    } else if t.has(Pat::Dts) {
        score += 8; // DTS core: bridged to 5.1
    } else if atmos {
        score += 20; // Atmos with no identifiable codec token: assume DD+ (most streaming Atmos is)
    }

    if let Some(size) = s.size_bytes {
        let add = ((size as f64 / GIB as f64) * 6.0).round() as i64;
        score += add.min(600);
    }

    let is_4k = t.has(Pat::Res2160) || t.has(Pat::Res4kUhd);
    if t.has(Pat::Av1) {
        if is_4k {
            score -= 1500;
        } else {
            score -= 150;
        }
    }
    if t.has(Pat::ThreeD) {
        score -= 2000;
    }
    if t.has(Pat::KorsubHc) {
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

/// What a release costs when every audio language its name declares is one nobody asked for: a dub made for
/// another market. It clears cached (+8000) plus the most quality alone can earn (4860), so an uncached
/// original-language release outranks a cached dub, and it stays under `WRONG_SHOW_SINK`, so the right episode
/// in any language still outranks the wrong one.
const FOREIGN_AUDIO_SINK: i64 = 15_000;

/// What a release costs when its audio is not declared but its subtitles are made for another market (VOSTFR).
/// The audio is most likely the original, so it sinks below untagged releases but stays above a dub.
const FOREIGN_SUBS_SINK: i64 = PREFERENCE_SINK;

/// What a release's language tags (`LANGUAGE_TAGS`) cost it against `f.languages`. Tokens of the title itself are
/// not read as tags, so a film whose name holds "Fin" is not taken for Finnish. Nothing is judged while the original
/// language is unknown: sinking an Italian film's ITA releases because nobody said it was Italian would trade the
/// original for a dub, which is the failure this exists to prevent.
pub(crate) fn foreign_language_sink(t: &ReleaseTraits, f: &RankFilters) -> i64 {
    if f.languages.is_empty() {
        return 0;
    }
    let (mut audio, mut audio_wanted, mut foreign_subs) = (false, false, false);
    for (tok, kind, lang) in t.language_tags() {
        if f.show_tokens.contains(*tok) || f.expected_title_tokens.contains(*tok) {
            continue;
        }
        match kind {
            TagKind::Multi => return 0,
            TagKind::Audio => {
                audio = true;
                audio_wanted |= f.languages.contains(*lang);
            }
            TagKind::Subs => foreign_subs |= *lang != "en" && !f.languages.contains(*lang),
        }
    }
    // A release named in Cyrillic is a release made for that market, almost always dubbed.
    if t.cyrillic() {
        audio = true;
        audio_wanted |= f.languages.contains("ru");
    }
    if audio {
        return if audio_wanted { 0 } else { FOREIGN_AUDIO_SINK };
    }
    if foreign_subs {
        FOREIGN_SUBS_SINK
    } else {
        0
    }
}

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

/// Go `showNoise`: words a release puts between the show and the episode that name neither. "Season" in the
/// languages releases are named in, folded as `title_tokens` folds them ("Säsong" is "sasong").
const SHOW_NOISE: &[&str] = &[
    "season",
    "temporada",
    "saison",
    "staffel",
    "complete",
    "sasong",
    "sesong",
    "kausi",
    "stagione",
    "seizoen",
    "sezon",
];

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
#[cfg(test)]
pub(crate) fn release_years(title: &str) -> Vec<i64> {
    crate::traits::year_matches(&str::to_lowercase(title)).collect()
}

/// Go `yearMismatch`: whether a title has a parseable year and none of them is within one of `expected`.
/// No parseable year means no mismatch: we can't tell, so the release is kept.
#[cfg(test)]
pub(crate) fn year_mismatch(title: &str, expected: i64) -> bool {
    year_mismatch_traits(&ReleaseTraits::parse(title), expected)
}

fn year_mismatch_traits(t: &ReleaseTraits, expected: i64) -> bool {
    t.has_year() && !t.year_in(expected - 1, expected + 1)
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

    // The sort key of each survivor, (score, seeders, input index): the index makes every key distinct, so an
    // unstable sort orders them exactly as a stable one would.
    let mut out: Vec<(i64, i64, usize)> = Vec::new();
    for (i, s) in streams.iter().enumerate() {
        let t = s.traits();
        let junk = t.junk_class();
        if f.exclude_cam && !junk.is_empty() {
            drop_n(&mut debug, junk, 1);
            continue;
        }
        if exclude_re.as_ref().is_some_and(|re| re.is_match(&s.title)) {
            drop_n(&mut debug, "excludeRegex", 1);
            continue;
        }
        // Drop a mistagged torrent (another film's IMDb id): its release year is clearly wrong.
        if f.expected_year.is_some_and(|y| year_mismatch_traits(t, y)) {
            drop_n(&mut debug, "yearMismatch", 1);
            continue;
        }
        // A release with no parseable year can't be year-checked; if we know the title, require at least one
        // significant-token overlap so year-less junk ("B-Bead.mp4") is dropped.
        if !f.expected_title_tokens.is_empty()
            && !t.has_year()
            && !title_overlap(&s.title, &f.expected_title_tokens)
        {
            drop_n(&mut debug, "titleMismatch", 1);
            continue;
        }
        // Computed once for both the hard filter and the soft preference, and only when one is set.
        let mut res = "";
        if !allowed.is_empty() || !f.prefer_resolution.is_empty() {
            res = resolution_bucket(t);
        }
        if !allowed.is_empty() && !res.is_empty() && !allowed.contains(res) {
            drop_n(&mut debug, "resolution", 1);
            continue;
        }
        if f.hdr_only && !t.has(Pat::HdrOnly) {
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
        let mut score = quality_score_traits(t, s);
        if let Some(client) = &f.client {
            score -= client.cost_ref(s, cost_attributes(s));
        }
        // A release whose resolution could not be read is NOT sunk, matching every hard filter here.
        if !f.prefer_resolution.is_empty() && !res.is_empty() && res != f.prefer_resolution {
            score -= PREFERENCE_SINK;
        }
        score -= foreign_language_sink(t, &f);
        if let Some([season, episode]) = f.episode {
            if (!f.show_tokens.is_empty() && other_show(&s.title, &f.show_tokens))
                || other_episode(&s.title, season, episode)
            {
                score -= WRONG_SHOW_SINK;
            }
        }
        out.push((score, s.seeders.unwrap_or(0), i));
    }

    out.sort_unstable_by_key(|&(score, seeders, idx)| (Reverse(score), Reverse(seeders), idx));
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
            .map(|&(score, _, idx)| {
                let s = &streams[idx];
                DebugScore {
                    title: s.title.clone(),
                    score,
                    resolution: resolution_bucket(s.traits()).to_string(),
                    cached: s.cached,
                }
            })
            .collect();
    }
    out.iter().map(|&(_, _, idx)| streams[idx].clone()).collect()
}
