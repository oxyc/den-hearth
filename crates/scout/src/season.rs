//! Port of `internal/scout/season.go`: picking the right file out of a season pack (SxxExx, 1x02,
//! "season 1 episode 2", absolute numbering). `TorrentFile` is FROZEN (see PORTING.md): every store lists
//! files in this shape.

use std::collections::HashMap;
use std::sync::{Arc, LazyLock, Mutex, MutexGuard, PoisonError};

use fancy_regex::{Captures, Regex as FancyRegex, RegexBuilder as FancyRegexBuilder};

use crate::rank::re2;
use crate::stores::StoreError;
use crate::util::go_to_lower;

/// fancy-regex gives up on a match after this many backtracks and reports an error, where Go's regexp2 has no
/// limit and always answers. Callers read an error as "no match", so with the default (1,000,000) a long enough
/// title would silently stop matching. These patterns backtrack a bounded amount per start position, so the work
/// stays linear in the title (an indexer title is capped by the 2 MiB response it arrives in) and the limit is
/// lifted rather than tuned.
const BACKTRACK_LIMIT: usize = usize::MAX;

/// Compiles one of this module's .NET-syntax patterns the way regexp2 runs it, with no backtrack ceiling.
fn fancy(pattern: &str) -> Result<FancyRegex, fancy_regex::Error> {
    FancyRegexBuilder::new(pattern).backtrack_limit(BACKTRACK_LIMIT).build()
}

/// .NET's `\w`, which regexp2 uses for `\b`: letters, non-spacing marks, decimal digits and connector
/// punctuation. The regex crate's `\w` also takes spacing marks, enclosing marks and letter numbers.
const NET_WORD: &str = r"[\p{L}\p{Mn}\p{Nd}\p{Pc}]";

/// .NET's `\b` spelled out over [`NET_WORD`].
fn net_word_boundary() -> String {
    format!("(?:(?<={NET_WORD})(?!{NET_WORD})|(?<!{NET_WORD})(?={NET_WORD}))")
}

/// .NET's `$` without the multiline option: the end of the text, or just before a final newline.
const NET_END: &str = r"(?=\n?\z)";

/// FROZEN. Go `TorrentFile`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TorrentFile {
    pub index: i64,
    pub name: String,
    pub size_bytes: Option<i64>,
}

/// Go `errEpisodeNotInTorrent`: the file list is episode-labelled and holds no file for the episode asked for.
pub(crate) const ERR_EPISODE_NOT_IN_TORRENT: &str = "torrent holds no file for that episode";

/// The error `pick_episode_file` returns when a labelled pack lacks the episode asked for.
pub(crate) fn err_episode_not_in_torrent() -> StoreError {
    StoreError::Other(ERR_EPISODE_NOT_IN_TORRENT.to_owned())
}

/// Go `episodePatterns`' result: the matchers for one season and episode, in the order Go builds them.
pub(crate) type EpisodePatterns = Vec<FancyRegex>;

static VIDEO_EXT_RE: LazyLock<regex::Regex> =
    LazyLock::new(|| re2(r"\.(mkv|mp4|avi|m4v|ts|mov|wmv|flv|webm)$"));

/// Go `labelledEpisodeRe` — does a filename DECLARE an episode, in a form that can't be anything else?
///
/// Deliberately narrower than episodePatterns: the bare concatenated forms there (102, 0102) are fine for
/// confirming an episode you already expect, but useless as evidence that a name is episode-labelled at
/// all — "Movie.2019.1080p" is full of three- and four-digit runs. Only the unambiguous forms count, so
/// the mismatch below is claimed only when a filename really does name a different episode.
static LABELLED_EPISODE_RE: LazyLock<regex::Regex> = LazyLock::new(|| {
    re2(r"(s\d{1,2}[ ._-]*e\d{1,3}|\b\d{1,2}x\d{2}\b|season[ ._-]*\d{1,2}[ ._-]*episode[ ._-]*\d{1,3})")
});

/// Go `episodeRangeRe` finds a multi-episode label: a season, a first episode, and everything chained onto it.
/// The chain is captured whole and read by `episode_chain`, because a file may name more than two.
///
/// Sonarr writes six styles and three of them chain past two numbers — `S01E01E02E03`,
/// `S01E01-E02-E03`, `S01E01-02-03`. Capturing exactly one far end read those as ending at the SECOND
/// number, so the third episode matched nothing; the file is labelled, so the miss became a hard refusal
/// and a third of a playing release answered 404 on all three stores.
static EPISODE_RANGE_RE: LazyLock<FancyRegex> = LazyLock::new(|| {
    fancy(r"s0*(\d{1,2})[ ._-]*e(\d{1,3})((?:[ ._-]*e\d{1,3}|-\d{1,3})*)(?![\da-z])")
        .expect("constant pattern compiles")
});

/// Go `episodeChainRe` splits the chain into its elements, keeping whether each carried its own `e`.
static EPISODE_CHAIN_RE: LazyLock<FancyRegex> =
    LazyLock::new(|| fancy(r"(?:[ ._-]*e(\d{1,3})|-(\d{1,3}))").expect("constant pattern compiles"));

/// Go `chainedEpisode`: one number after the first, and whether it named itself with an `e`.
struct ChainedEpisode {
    value: i64,
    digits: usize,
    marked: bool,
}

/// Go `episodePatterns` builds the SxxExx / 1x02 / "season 1 episode 2" / 102 / 0102 matchers once per pick.
/// These NAME the episode: each carries the season with it, so a match is a statement about the file.
pub(crate) fn episode_patterns(season: i64, episode: i64) -> EpisodePatterns {
    let b = net_word_boundary();
    let specs = [
        format!(r"s0*{season}[ ._-]*e0*{episode}(?!\d)"),
        format!(r"{b}0*{season}x0*{episode}(?!\d)"),
        format!(r"season[ ._-]*0*{season}[ ._-]*episode[ ._-]*0*{episode}(?!\d)"),
        // Bare concatenated forms (102 = S1E02) must not swallow a resolution token: a trailing p/i
        // makes "720" (S7E20) match "720p", so exclude those as well as a following digit.
        format!(r"{b}{season}{episode:02}(?![\dpi])"),
        format!(r"{b}{season:02}{episode:02}(?![\dpi])"),
        // A double episode names TWO of them, and only the first was ever matched: `Show.S01E05-E06.mkv`,
        // `S01E05E06`, `S01E05-06`, `S01E05.E06`. Asked for the second, nothing matched — and because such
        // a file DOES declare an episode it then ruled itself out of every guess below it, so a pack of
        // them answered 404 for half the season, while a lone unlabelled file beside them (the sample) was
        // served for the other half. The first number is left free and the requested one anchored second.
        // The first number needs its own `(?!\d)` or the match backtracks into a false split: without it
        // `s01e11-e12` read as "e1" followed by "1", and episode 1 matched the last double in the pack.
        //
        // The SECOND element has to look like an episode too. Written as an optional `e` over a possibly
        // empty separator it accepted the next bare number after the label, whatever that number was:
        // `Show.S01E01.7.Minutes.mkv` became a strong match for episode 7, `Show.S01E08.4K.HDR.mkv` for
        // episode 4 — every file in a 4K pack claimed to be episode 4 — and `Show - S02E01 - 014 - Title`
        // for episode 14. Strong matches outrank the indexer's fileIdx, so a correct position was
        // overridden by a title that happens to start with a digit.
        //
        // So the second number must carry its own `e` — an explicit marker nothing else in a release name
        // wears.
        format!(r"s0*{season}[ ._-]*e\d{{1,3}}(?!\d)[ ._-]*e0*{episode}(?!\d)"),
    ];
    // `S01E05-06` writes the far end without an `e`, and a tight dash was tried as the thing that made it
    // safe. It is not: `Show.S01E01-7.Minutes.mkv`, `Show.S01E08-4K.HDR.mkv` and `Show - S02E01-014 -
    // Title.mkv` are the same failures one separator over, and a pack of `Show.S01E%02d-60fps.mkv` stopped
    // refusing an episode it does not hold. Any digit-initial tag joined to the label does it: -4K, -8bit,
    // -2CH, -3D, -60fps, a group tag like -2HD, or a title starting with a number.
    //
    // What identifies this form is not the punctuation but how the two numbers RELATE. Three rules, and
    // each one is load-bearing against a different family of names:
    //
    //   1. It reads as a range: the near number is below the far one. `E08-4K` is not a range ending at 4.
    //   2. Both ends are written the same way. Releases pad a range consistently — `E01-06`, `E05-06`,
    //      `E01-12` — while a tag or a title is just whatever it is: `E01-7.Minutes`, `E07-8.Mile`,
    //      `E01-2.Broke.Girls`, `E04-5.1.AC3`, and Sonarr's `S02E01-014`, all of which mix widths.
    //   3. The far end ENDS there, at a separator or the end of the name, never running into letters —
    //      which is what makes `-3D` and `-2HD` tags rather than ranges, since those are consecutive by
    //      coincidence.
    //
    // Those rules are checked by reading the two numbers rather than by generating a pattern per span —
    // see `range_holds_episode`. Generating them anchored only the far END, so a bare `S01E01-06.mkv` matched
    // episodes 1 and 6 and not the four in between, and a pack of such files was condemned for two thirds
    // of its episodes. A range is membership, and membership is arithmetic.
    compile_all(&specs)
}

/// Go `bareEpisodePatterns`: the episode number standing alone as its own token: `[Grp] Show - 03
/// [1080p].mkv`, which is how most anime and a good deal of TV is packed. The delimiters must be real
/// separators rather than "any non-digit", or `[Group8]` matches episode 8, and the token must be the
/// WHOLE digit run, which keeps 1080p, 720p, x264 and a year out.
///
/// A DOT CANNOT OPEN THE TOKEN, and can only close it when a digit does not follow. Both rules exist for
/// the same family of names: `DDP5.1`, `TrueHD.7.1`, `AC3.5.1`, `H.264`, `AAC.2.0`, `2024.03.15`. With
/// the dot an ordinary separator, every one of those is a standalone number, so on any pack with 5.1
/// audio — which is most of them — EVERY file matched episode 1 and the largest was served.
///
/// It is kept SEPARATE, and consulted only when nothing above matched, because it carries no season and
/// cannot say which show a number belongs to. Unioned with the patterns above and resolved by file size,
/// a number in the title (`Blake's 7`, `SG-1`) or a tag like `[10-bit]` outranked the file that really
/// did name the episode — the strong evidence losing to the weak, on the largest file.
pub(crate) fn bare_episode_patterns(episode: i64) -> EpisodePatterns {
    compile_all(&[
        format!(r"(?:^|[ _\-\[\]()])0*{episode}(?:[ _\-\[\]()]|\.(?!\d)|{NET_END})"),
        // `Show - E05 [1080p].mkv` and `Show.E05.1080p.mkv`: an episode number with no season anywhere, so
        // the strong patterns cannot fire and labelledEpisodeRe (which needs the `s`) does not see it
        // either. Every file then looked identical to every other and the largest was served for all
        // twelve episodes of a pack.
        //
        // The `e` is what makes this safe where the bare token is not: it is why a dot may open the token
        // and why the trailing rule can relax to any separator. `DDP5.1`, `H.264` and `AAC.2.0` have no
        // `e` against their digits, and a file that names a season is excluded from this tier already.
        format!(r"(?:^|[ ._\-\[\]()])e0*{episode}(?:[ ._\-\[\]()]|{NET_END})"),
    ])
}

fn compile_all(specs: &[String]) -> EpisodePatterns {
    specs.iter().filter_map(|s| fancy(s).ok()).collect()
}

/// Go `episodeNamer` holds the compiled patterns for ONE (season, episode) so a pick pays for them once
/// instead of once per file.
struct EpisodeNamer {
    season: i64,
    episode: i64,
    patterns: Arc<EpisodePatterns>,
}

impl EpisodeNamer {
    fn new(season: i64, episode: i64) -> Self {
        EpisodeNamer { season, episode, patterns: cached_episode_patterns(season, episode) }
    }

    fn names(&self, name: &str) -> bool {
        matches_episode(name, &self.patterns)
            || range_holds_episode(&go_to_lower(base_name(name)), self.season, self.episode)
    }
}

/// Go `namesEpisode`: whether a filename declares this exact episode, by a single label or by a range that
/// contains it.
pub(crate) fn names_episode(name: &str, season: i64, episode: i64) -> bool {
    EpisodeNamer::new(season, episode).names(name)
}

/// Go `unresolvedSeasonPack` reports whether a release's OWN title gives no reason to trust its declared size
/// as this episode's: a plain single SxxEyy label (or an explicit two-episode label naming this one) is
/// trusted as it always was, but a title that names the season generically with no episode at all
/// ("Show.S01.COMPLETE.1080p", "Show.Season.1.1080p") or by a multi-episode range that merely happens to
/// span this one ("Show.S01E01-12.1080p" for episode 3) is a pack, not a per-episode file, whatever its
/// declared size names.
pub(crate) fn unresolved_season_pack(title: &str, season: i64, episode: i64) -> bool {
    if matches_episode(title, &cached_episode_patterns(season, episode)) {
        return false;
    }
    let lower = go_to_lower(title);
    if range_holds_episode(base_name(&lower), season, episode) {
        return true;
    }
    season_only_pattern(season).is_match(&lower)
}

static SEASON_ONLY_CACHE: Mutex<Option<HashMap<i64, regex::Regex>>> = Mutex::new(None);

/// Go `seasonOnlyPattern` matches a bare season marker — s01, season 1, season.01 — that episodePatterns
/// itself never anchors on, since none of its patterns fire without an episode number right beside it.
/// Word-bounded so it does not fire inside "s01e03" itself: digits run straight into a glued-on episode
/// marker with no boundary between them, so a true per-episode name never matches this.
pub(crate) fn season_only_pattern(season: i64) -> regex::Regex {
    let mut cache = lock(&SEASON_ONLY_CACHE);
    let map = cache.get_or_insert_with(HashMap::new);
    if let Some(re) = map.get(&season) {
        return re.clone();
    }
    // An integer cannot make this pattern invalid, so a failure here is a bug in the format string. RE2, so ASCII `\b`.
    let re = re2(&format!(r"\bs0*{season}\b|\bseason[ ._-]*0*{season}\b"));
    map.insert(season, re.clone());
    re
}

/// Go `episodePatternCap`: the most (season, episode) sets the cache holds. Full is the rare case — no real
/// library reaches 512 distinct episodes in a process lifetime — so it drops everything and refills.
pub(crate) const EPISODE_PATTERN_CAP: i64 = 512;

/// Compiled patterns keyed by (season, episode).
type EpisodePatternMap = HashMap<(i64, i64), Arc<EpisodePatterns>>;

static EPISODE_PATTERN_CACHE: Mutex<Option<EpisodePatternMap>> = Mutex::new(None);

/// Go `cachedEpisodePatterns` keeps one compiled set per (season, episode) for the life of the process.
///
/// The set depends on nothing but those two numbers, so per-pick is still one compile too many: a viewer
/// working through a season re-compiles the same seven patterns for every episode of it, and a second
/// pack for an episode already picked re-compiles that one. This is the same cost the per-file form had
/// — work whose price grows every time a pattern is added — one level further out.
///
/// Bounded, because season and episode arrive from the request.
pub(crate) fn cached_episode_patterns(season: i64, episode: i64) -> Arc<EpisodePatterns> {
    let mut cache = lock(&EPISODE_PATTERN_CACHE);
    let map = cache.get_or_insert_with(HashMap::new);
    if let Some(patterns) = map.get(&(season, episode)) {
        return Arc::clone(patterns);
    }
    if map.len() as i64 >= EPISODE_PATTERN_CAP {
        map.clear();
    }
    let patterns = Arc::new(episode_patterns(season, episode));
    map.insert((season, episode), Arc::clone(&patterns));
    patterns
}

/// Go `matchesEpisode`: whether a release title names the episode `patterns` was built for.
pub(crate) fn matches_episode(name: &str, patterns: &EpisodePatterns) -> bool {
    let n = go_to_lower(base_name(name));
    patterns.iter().any(|re| matches!(re.is_match(&n), Ok(true)))
}

/// Go `rangeHoldsEpisode` reports whether a multi-episode label covers this episode.
///
/// TWO numbers are a range and the episode may lie between them; THREE OR MORE are a list, and only the
/// numbers actually written count. `S01E01-06` holds episodes 2 to 5 without naming them, while
/// `S01E01E02E03` names exactly what it holds — reading the latter as a range would be harmless here but
/// would start guessing the moment a release lists non-consecutive episodes.
///
/// The rules exist to keep names that are not episode labels at all out of this. Each excludes a
/// different family:
///
///  1. It reads as a range: the near number is below the far one. `E08-4K` is not a range ending at 4.
///  2. A BARE element is written the same width as the first. Releases pad consistently — `E01-06`,
///     `E05-06`, `E01-12` — while a tag or a title is whatever it is: `E01-7.Minutes`, `E07-8.Mile`,
///     `E01-2.Broke.Girls`, `E04-5.1.AC3`, Sonarr's `S02E01-014`. All mix widths.
///  3. A bare range spans a season at most. `E01-42` is not one file.
///
/// An `e`-marked element is exempt from 2 and 3: the explicit `e` is evidence enough on its own.
fn range_holds_episode(name: &str, season: i64, episode: i64) -> bool {
    for caps in EPISODE_RANGE_RE.captures_iter(name) {
        let Ok(caps) = caps else { break };
        let Some(got_season) = atoi_ok(group(&caps, 1)) else { continue };
        if got_season != season {
            continue;
        }
        let first_text = group(&caps, 2);
        let Some(first) = atoi_ok(first_text) else { continue };
        let chain = episode_chain(group(&caps, 3));
        if chain.is_empty() {
            continue; // a lone SxxExx: the plain patterns already answered for it
        }
        // A bare element that is not written like the first is not part of this label.
        if chain.iter().any(|c| !c.marked && c.digits != first_text.len()) {
            continue;
        }
        if let [only] = chain.as_slice() {
            let hi = only.value;
            if hi <= first || episode < first || episode > hi {
                continue;
            }
            if !only.marked && hi - first > 12 {
                continue;
            }
            return true;
        }
        // Three or more written numbers: a list of exactly these episodes.
        if episode == first || chain.iter().any(|c| c.value == episode) {
            return true;
        }
    }
    false
}

/// Go `episodeChain` splits the captured tail of a multi-episode label into its numbers.
fn episode_chain(tail: &str) -> Vec<ChainedEpisode> {
    let mut out = Vec::new();
    for m in EPISODE_CHAIN_RE.captures_iter(tail) {
        let Ok(m) = m else { break };
        let marked = group(&m, 1);
        let bare = group(&m, 2);
        let text = if marked.is_empty() { bare } else { marked };
        let Some(value) = atoi_ok(text) else { continue };
        out.push(ChainedEpisode { value, digits: text.len(), marked: !marked.is_empty() });
    }
    out
}

/// The text of capture group `i`, empty when it did not participate (Go's `GroupByNumber(i).String()`).
fn group<'t>(caps: &Captures<'t, str>, i: usize) -> &'t str {
    caps.get(i).map_or("", |m| m.as_str())
}

/// Go `atoiOK`: the number a digit run spells, or None for an empty or non-numeric run.
fn atoi_ok(s: &str) -> Option<i64> {
    if s.is_empty() {
        None
    } else {
        s.parse().ok()
    }
}

/// Go `baseName` drops the directories. All three stores hand back PATHS — TorBox's `name`, RD's `path`,
/// Premiumize's `path` — so the torrent's root directory is part of every file's name, and matching the
/// whole thing let one directory speak for every file in the pack. A scene pack rooted at
/// `Show.S01E01-E10.1080p…/` matched a request for S01E01 in all ten files and served the largest; an
/// anime pack rooted at `[Grp] Show (Season 1) [BD 1080p][HEVC 10-bit]/` did the same for episodes 1 and
/// 10. In both cases a correct fileIdx was discarded on the way past. A directory describes the pack; only
/// the filename describes the file.
pub(crate) fn base_name(name: &str) -> &str {
    // `/` only. The BitTorrent path is a list of components and every client joins it with a forward
    // slash, which is what all three debrid APIs hand back. A backslash, meanwhile, is a legal character
    // in a POSIX filename, and nothing in the string says which one it is: splitting on it turned
    // `Show S01E01 - Part 1 \ Part 2.mkv` into ` Part 2.mkv`, losing the label and reclassifying a
    // perfectly well-named file as unlabelled.
    match name.rfind('/') {
        Some(i) => &name[i + 1..],
        None => name,
    }
}

/// Go `pickEpisodeFile` picks the file index for an episode: an SxxExx match among video files (largest on
/// ties), else the largest video file.
///
/// Returns `err_episode_not_in_torrent` when nothing matched but the pool IS episode-labelled. Falling back to
/// the largest file there serves a confidently wrong episode: the pack holds S01E01–E10, you asked for
/// E11, and it hands back E07 as though that were the answer. A single-episode release whose filename
/// carries no marker still falls back, because there the largest file is the only thing it could be.
pub(crate) fn pick_episode_file(
    files: &[TorrentFile],
    season: i64,
    episode: i64,
) -> Result<Option<i64>, StoreError> {
    let pool = episode_file_pool(files);
    if pool.is_empty() {
        return Ok(None);
    }

    let namer = EpisodeNamer::new(season, episode);
    let mut matched = Vec::new();
    let mut unlabelled = Vec::new();
    for &f in &pool {
        if namer.names(&f.name) {
            matched.push(f);
        }
        // Per FILE, not per pool. A pool-wide OR let one labelled file speak for the rest: an anime batch
        // carrying a single `S00E01` special alongside bare-numbered episodes had the weak tier switched
        // off for all of them, and the miss then became a hard refusal — a release that plays, answered
        // 404 and recorded dead by the client.
        if !declares_episode(&f.name) {
            unlabelled.push(f);
        }
    }
    if let Some(best) = largest(&matched) {
        return Ok(Some(best.index));
    }
    // Only now the weak evidence, and only against the files that do NOT name their own episode. A bare
    // number carries no season and cannot say which show it belongs to, so on a file already labelled
    // S01E05 it is noise; on one labelled nothing it is all there is.
    if !unlabelled.is_empty() {
        let bare_re = bare_episode_patterns(episode);
        let bare: Vec<&TorrentFile> =
            unlabelled.iter().copied().filter(|f| matches_episode(&f.name, &bare_re)).collect();
        // Exactly one, and it decides. In a numbered pack the episode's own number appears in one file; a
        // number turning up in several is either something they share — the show's title, which served
        // `Stargate SG-1 - 03` for a request for episode 1 — or the same episode twice, as in a
        // dual-quality pack. This function cannot tell those apart, so it does not try: several matches
        // answer nothing HERE and are offered to the caller lower down, beneath the indexer's fileIdx.
        if let [only] = bare.as_slice() {
            return Ok(Some(only.index));
        }
    }
    // A pack that NAMES its episodes, none of them yours, holds nothing to play. Two labelled files are
    // what make it a pack: one is a single episode, and a bare number can never prove absence — bare
    // numbering is frequently ABSOLUTE, so season 2 episode 1 is packed as `[Grp] Show - 13` and nothing
    // in the name says so, so refusing on that would kill every episode of an ordinary anime season.
    //
    // The labelled files must also be the MAJORITY, which is what makes them the pack rather than a few
    // extras inside one. Requiring every file to be labelled let a single sample disarm the refusal for a
    // pack that plainly held other episodes; requiring merely two let the opposite happen — an anime
    // batch of twelve absolutely-numbered episodes plus two labelled OVAs was condemned whole, and since
    // the error short-circuits every store's picker the indexer's correct fileIdx was never read. A
    // release that plays, answered 404. Both mistakes are the same one: counting labels instead of asking
    // whether the labels describe the pack.
    let labelled = pool.len() - unlabelled.len();
    if labelled > 1 && labelled > unlabelled.len() {
        return Err(err_episode_not_in_torrent());
    }
    // No opinion, rather than a guess. The fallback below is sound only for a pool of ONE, where "the
    // largest file is the only thing it could be" is literally true. For a multi-file pack it hands back
    // the biggest episode as though it were the one asked for — and because a non-nil answer makes every
    // caller return early, it also DISCARDS the indexer's fileIdx, which is a position in this very
    // torrent and was right. That is the whole of `[Grp] Show - 03 [1080p].mkv`: a bare episode number is
    // not one of the unambiguous forms labelledEpisodeRe accepts — it cannot be, "Movie.2019.1080p" is
    // full of digit runs — so a standard anime/TV pack was judged unlabelled and every episode of it
    // resolved to the same file, with a 302 and no error.
    if pool.len() > 1 {
        return Ok(None);
    }
    Ok(largest(&pool).map(|f| f.index))
}

/// Go `episodeFilePool`: the files an episode may be picked from: the videos, or everything when a list
/// carries no recognised video extension.
fn episode_file_pool(files: &[TorrentFile]) -> Vec<&TorrentFile> {
    let videos: Vec<&TorrentFile> =
        files.iter().filter(|f| VIDEO_EXT_RE.is_match(&go_to_lower(&f.name))).collect();
    if videos.is_empty() {
        files.iter().collect()
    } else {
        videos
    }
}

/// Whether a file's own name declares an episode (see `LABELLED_EPISODE_RE`): Go's
/// `labelledEpisodeRe.match(strings.ToLower(baseName(name)))`, which `pickEpisodeFile`, `unlabelledCandidates`
/// and `otherEpisode` all spell out.
pub(crate) fn declares_episode(name: &str) -> bool {
    LABELLED_EPISODE_RE.is_match(&go_to_lower(base_name(name)))
}

/// Go `largest`: the biggest file, the earliest one on a tie. None for an empty list.
fn largest<'a>(files: &[&'a TorrentFile]) -> Option<&'a TorrentFile> {
    let mut best: Option<&'a TorrentFile> = None;
    for &f in files {
        let beats = match best {
            None => true,
            Some(b) => f.size_bytes.unwrap_or(0) > b.size_bytes.unwrap_or(0),
        };
        if beats {
            best = Some(f);
        }
    }
    best
}

/// Go `ambiguousEpisodeGuess` is the answer `pick_episode_file` declined to give: the largest file whose name
/// carries the episode's number, when more than one does. It ranks BELOW the indexer's fileIdx — a
/// position in this very torrent beats a number that did not tell the files apart — and above the
/// last-resort "largest video anywhere in the pack", which is what a duplicate-numbered pack used to fall
/// through to. On a dual-quality release the two copies of the episode are the matches and the better one
/// wins; where the number is really the show's title the guess is no worse than the fallback it replaces.
pub(crate) fn ambiguous_episode_guess(files: &[TorrentFile], episode: i64) -> Option<i64> {
    // The same candidates the tier it continues uses. Iterating the whole pool admitted files that name
    // their OWN, different episode — and since this ranks above the last-resort largest, one of them won:
    // `Show.S01E12 - Part 5.mkv` was served for a request for episode 5, beating both copies of
    // `[Grp] Show - 05` sitting beside it. A bare number is noise on a file that already says what it is.
    let candidates = unlabelled_candidates(files);
    if candidates.is_empty() {
        return None;
    }
    let patterns = bare_episode_patterns(episode);
    let bare: Vec<&TorrentFile> =
        candidates.into_iter().filter(|f| matches_episode(&f.name, &patterns)).collect();
    largest(&bare).map(|f| f.index)
}

/// Go `unlabelledCandidates` is the pool minus every file whose own name declares an episode. It is only ever
/// reached once the strong matcher has found nothing, so a file that names an episode has named a
/// DIFFERENT one and cannot be the answer, whatever its size.
///
/// Every guess below the strong tier has to be taken from this set rather than the pool. Counting rules
/// decide whether a pack is condemned, and there is always a band they do not cover — labelled files
/// present but not a majority — where control fell through to "the largest video anywhere", picked a file
/// whose filename says S01E02, and served it for a request for S03E09 with a 302 and no error. Re-tuning
/// the threshold only moves that band; excluding the files that rule themselves out removes it.
fn unlabelled_candidates(files: &[TorrentFile]) -> Vec<&TorrentFile> {
    episode_file_pool(files).into_iter().filter(|f| !declares_episode(&f.name)).collect()
}

/// Go `largestEpisodeCandidate` is the last resort when an episode was asked for: the biggest file that has
/// not ruled itself out by naming a different episode. Where every file names one it falls back to the
/// whole playable pool, since something must be served and there is nothing left to prefer.
pub(crate) fn largest_episode_candidate(files: &[TorrentFile]) -> Option<&TorrentFile> {
    let candidates = unlabelled_candidates(files);
    if !candidates.is_empty() {
        return largest(&candidates);
    }
    largest_playable(files)
}

/// Go `isFeatureSized` reports whether `f` could be a torrent's feature: a video (when the list has any) at
/// least half the size of its largest video. A file whose size is unknown is given the benefit of the doubt.
pub(crate) fn is_feature_sized(files: &[TorrentFile], f: &TorrentFile) -> bool {
    let pool = episode_file_pool(files);
    if pool.len() < files.len() && !VIDEO_EXT_RE.is_match(&go_to_lower(&f.name)) {
        return false;
    }
    let size = f.size_bytes.unwrap_or(0);
    let top = largest(&pool).and_then(|t| t.size_bytes).unwrap_or(0);
    // `saturating_mul`: the size is a network value, and a doubled i64 must not panic.
    f.size_bytes.is_none() || top == 0 || size.saturating_mul(2) >= top
}

/// Go `largestPlayable` is largest() over the files that could be the feature — the same video-only pool
/// pickEpisodeFile picks from, so a store falling back on its own reaches the same kind of answer. Picking
/// over every file instead served a pack's `.iso` or `.rar` as the episode: a 302, no error, no video.
pub(crate) fn largest_playable(files: &[TorrentFile]) -> Option<&TorrentFile> {
    let pool = episode_file_pool(files);
    if !pool.is_empty() {
        return largest(&pool);
    }
    let all: Vec<&TorrentFile> = files.iter().collect();
    largest(&all)
}

/// Locks a module-level cache. A poisoned lock still holds a valid map, so the cache is used as it is.
fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(PoisonError::into_inner)
}
