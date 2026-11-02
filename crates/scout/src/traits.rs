//! A release title, parsed once. Ranking, the browser cost, the language check, the attributes, the label and the
//! binge group all read the same facts off a release name; each used to re-run its own regexes over a freshly
//! lowercased copy of it, so a release was parsed up to five times per list. `ReleaseTraits` holds every one of
//! those facts as bits, computed in one `RegexSet` pass, and `RawStream::traits` keeps it with the release.
//!
//! Only the title is read here. Everything that also depends on the size, the cache state, the probe or the request
//! (the fake-4K floor, the size bonus, the year window, the language set) is decided by the reader.

use std::collections::HashMap;
use std::sync::LazyLock;

use fancy_regex::{Regex as LookRegex, RegexBuilder as LookRegexBuilder};
use regex::{Regex, RegexSet, RegexSetBuilder};

use crate::rank::re2;

/// One pattern of the main set, matched against the lowercased title. The discriminant is the pattern's index in
/// `Pat::ALL` and its bit in `ReleaseTraits::pats`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub(crate) enum Pat {
    Res2160,
    Res1440,
    Res1080,
    Res720,
    Res576,
    Res540,
    Res480,
    Res4kUhd,
    Remux,
    Bluray,
    BrRip,
    WebDl,
    WebRip,
    Web,
    Hdtv,
    DvdRip,
    LowSrc,
    /// Bare DV is the common scene tag for Dolby Vision.
    Dovi,
    Hdr10p,
    /// The hdrOnly filter's own pattern: Dolby Vision without the bare DV tag, or any HDR10-family tag.
    HdrOnly,
    Hlg,
    /// Matches "hdr10" and the "hdr10" in "hdr10+"; check `Hdr10p` first.
    Hdr10Any,
    HdrPlain,
    Atmos,
    Dtsx,
    Truehd,
    DtshdMa,
    DtshdAttr,
    Flac,
    Eac3,
    Dts,
    Ac3,
    /// A digit or a boundary after "aac" ("AAC2.0", "AAC5 1", "HE-AAC"), so "Aachen" is not audio.
    Aac,
    Mp3,
    Av1,
    Hevc,
    Avc,
    Hi10,
    TenBit,
    Hybrid,
    ThreeD,
    KorsubHc,
    Ch71,
    Ch51,
    Ch20,
}

impl Pat {
    pub(crate) const ALL: [Pat; 45] = [
        Pat::Res2160,
        Pat::Res1440,
        Pat::Res1080,
        Pat::Res720,
        Pat::Res576,
        Pat::Res540,
        Pat::Res480,
        Pat::Res4kUhd,
        Pat::Remux,
        Pat::Bluray,
        Pat::BrRip,
        Pat::WebDl,
        Pat::WebRip,
        Pat::Web,
        Pat::Hdtv,
        Pat::DvdRip,
        Pat::LowSrc,
        Pat::Dovi,
        Pat::Hdr10p,
        Pat::HdrOnly,
        Pat::Hlg,
        Pat::Hdr10Any,
        Pat::HdrPlain,
        Pat::Atmos,
        Pat::Dtsx,
        Pat::Truehd,
        Pat::DtshdMa,
        Pat::DtshdAttr,
        Pat::Flac,
        Pat::Eac3,
        Pat::Dts,
        Pat::Ac3,
        Pat::Aac,
        Pat::Mp3,
        Pat::Av1,
        Pat::Hevc,
        Pat::Avc,
        Pat::Hi10,
        Pat::TenBit,
        Pat::Hybrid,
        Pat::ThreeD,
        Pat::KorsubHc,
        Pat::Ch71,
        Pat::Ch51,
        Pat::Ch20,
    ];

    /// RE2 syntax, ASCII classes (`re2`). A pattern that was the union of two others (rank's "dts-hd…|flac…", "4K",
    /// "HDR or HLG") is read as the OR of their bits instead of matched again.
    pub(crate) const fn pattern(self) -> &'static str {
        match self {
            Pat::Res2160 => r"2160p?",
            Pat::Res1440 => r"1440p?",
            Pat::Res1080 => r"1080p?",
            Pat::Res720 => r"720p?",
            Pat::Res576 => r"576p?",
            Pat::Res540 => r"540p?",
            Pat::Res480 => r"480p?",
            Pat::Res4kUhd => r"4k|uhd",
            Pat::Remux => r"\bremux\b",
            Pat::Bluray => r"bluray|blu-ray",
            Pat::BrRip => r"b[dr][ .\-_]?rip",
            Pat::WebDl => r"web[ .\-_]?dl",
            Pat::WebRip => r"web[ .\-_]?rip",
            Pat::Web => r"\bweb\b",
            Pat::Hdtv => r"\bhdtv\b",
            Pat::DvdRip => r"dvd[ .\-_]?rip",
            Pat::LowSrc => r"tvrip|satrip|pdtv",
            Pat::Dovi => r"dolby vision|dolbyvision|dovi|\bdv\b",
            Pat::Hdr10p => r"hdr10\+|hdr10plus",
            Pat::HdrOnly => r"dolby vision|dolbyvision|dovi|\bhdr\b|hdr10|\bhlg\b",
            Pat::Hlg => r"\bhlg\b",
            Pat::Hdr10Any => r"hdr10",
            Pat::HdrPlain => r"\bhdr\b",
            Pat::Atmos => r"atmos",
            Pat::Dtsx => r"dts:x|dtsx|dts-x",
            Pat::Truehd => r"truehd|true-hd",
            Pat::DtshdMa => r"dts-hd ma|dts-hd\.ma|dts-ma",
            Pat::DtshdAttr => r"dts-hd|dts hd|dtshd",
            Pat::Flac => r"flac|lpcm|pcm",
            Pat::Eac3 => r"eac3|e-ac3|dd\+|ddp|ddplus",
            Pat::Dts => r"\bdts\b",
            Pat::Ac3 => r"ac3|\bdd\b|dolby digital",
            Pat::Aac => r"\baac(?:[0-9]|\b)",
            Pat::Mp3 => r"\bmp3\b",
            Pat::Av1 => r"av1",
            Pat::Hevc => r"x265|h\.?265|hevc",
            Pat::Avc => r"x264|h\.?264|\bavc\b",
            Pat::Hi10 => r"\bhi10p?\b",
            Pat::TenBit => r"\b10[ .\-_]?bits?\b|\bhi10p?\b|\bmain ?10\b",
            Pat::Hybrid => r"\bhybrid\b",
            Pat::ThreeD => r"\b3d\b|hsbs|half[ .\-_]?sbs|sbs[ .\-_]?3d",
            Pat::KorsubHc => r"korsub|\bhc\b",
            Pat::Ch71 => r"7\.1|7 1|\b8ch\b",
            Pat::Ch51 => r"5\.1|5 1|\b6ch\b",
            Pat::Ch20 => r"2\.0|2 0|stereo|\b2ch\b",
        }
    }
}

static MAIN_SET: LazyLock<RegexSet> = LazyLock::new(|| {
    RegexSetBuilder::new(Pat::ALL.map(Pat::pattern)).unicode(false).build().expect("static RE2 pattern set")
});

/// Go `junkClassOf`'s classes, in its order: the first that matches names the release.
const JUNK_CLASSES: [&str; 7] = ["cam", "telesync", "telecine", "screener", "workprint", "r5", "upscaled"];

/// Go `unambiguousJunk` (all but "upscaled"), then `goodSource` and the bare cam/ts/scr tags, then a lookbehind-free
/// widening of "upscaled". The indexes are read in `parse_lower`.
const JUNK_PATTERNS: [&str; 11] = [
    r"h[dq][ .\-_]?cam(rip)?|cam[ .\-_]?rip|s[ .\-]+print",
    r"telesynch?|hd[ .\-_]?ts(rip)?|ts[ .\-_]?rip",
    r"telecine|hd[ .\-_]?tc",
    r"(dvd|bd|br|web|hd)[ .\-_]?scr|p(re)?dvd(rip)?|screener",
    r"workprint",
    r"\br5\b",
    // 6: Go `goodSource`, markers that downgrade a bare cam/ts/scr to non-junk.
    r"remux|bluray|blu-ray|b[dr][ .\-_]?rip|web[ .\-_]?(dl|rip)?|hdtv|dvd[ .\-_]?rip",
    r"\bcam\b",
    r"\bts\b",
    r"\bscr\b",
    // 10: `UPSCALED` without its lookbehinds. Dropping an assertion only widens a pattern, so a title this misses
    // cannot match `UPSCALED`, and the backtracking engine runs only on the few names that hold one of its words.
    r"1xbet|read[ .\-_]?note|upscaled?|up[ .\-_]?rez|ai[ .\-_]?(upscaled?|enhanced?)|re[ .\-_]?graded?",
];
const JUNK_GOOD: usize = 6;
const JUNK_BARE_CAM: usize = 7;
const JUNK_BARE_TS: usize = 8;
const JUNK_BARE_SCR: usize = 9;
const JUNK_UPSCALED_WIDE: usize = 10;

static JUNK_SET: LazyLock<RegexSet> = LazyLock::new(|| {
    RegexSetBuilder::new(JUNK_PATTERNS).unicode(false).build().expect("static RE2 pattern set")
});

/// The "upscaled" class, which needs a lookbehind and so runs on fancy-regex as Go runs it on regexp2. regexp2 has
/// no backtrack ceiling, where fancy-regex stops after a million and reports an error that `junk` reads as "no
/// match" — so a long enough title would silently stop being judged. The pattern backtracks a bounded amount per
/// start position, so lifting the ceiling costs time linear in the title.
static UPSCALED: LazyLock<LookRegex> = LazyLock::new(|| {
    LookRegexBuilder::new(
        r"1xbet|read[ .\-_]?note|(?<!not[ .\-_])(?<!non[ .\-_])(upscaled?|up[ .\-_]?rez)|ai[ .\-_]?(upscaled?|enhanced?)|re[ .\-_]?graded?",
    )
    .backtrack_limit(usize::MAX)
    .build()
    .expect("static lookaround pattern")
});

/// Go `containerExt`: a container extension at the end of a file name. `.ts` is MPEG transport stream, not
/// TeleSync: without this, `Show.S01E01.ts` matched the bare-TS rule and was dropped as junk. Only at the end of a
/// name (or of a line in a multi-line title); a `.TS.` in the middle is still a TeleSync tag. RE2's `\s` is
/// `[\t\n\f\r ]`, without the vertical tab the regex crate's ASCII `\s` adds, so it is spelled out.
static CONTAINER_EXT: LazyLock<Regex> =
    LazyLock::new(|| re2(r"\.(?:m2ts|ts|mkv|mp4|avi|m4v|webm)([\t\n\f\r ]|$)"));

/// Opus counts only once the technical half of the name has begun: "Mr.Hollands.Opus.1995.1080p" is a film title,
/// and a bare \bopus\b would call it Opus audio. RE2's `.` is a whole codepoint but its `\d` and `\b` are ASCII, so
/// this stays in Unicode mode (for the dots) and spells the ASCII classes out with `(?-u:…)`.
static RE_OPUS: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"(?:(?-u:\d){3,4}p|x26[45]|h\.?26[45]|hevc|av1|10.?bits?|web-?dl|webrip|bluray)(?-u:\b).*(?-u:\bopus\b)")
        .expect("static pattern")
});

/// Go `yearToken`: a plausible 4-digit film year (1900 to 2039). `parse_lower` then rejects matches that are
/// really resolution or codec digits.
pub(crate) static YEAR_TOKEN: LazyLock<Regex> = LazyLock::new(|| re2(r"19\d\d|20[0-3]\d"));

/// What a language tag says about a release.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TagKind {
    /// The audio is in this language.
    Audio,
    /// Only the subtitles are.
    Subs,
    /// Several audio tracks, which in practice includes the original.
    Multi,
}

/// Release-name tokens that declare a language, mapped to ISO 639-1, with what they declare. Scene names leave the
/// original language of an English title untagged and tag everything else, so an audio tag is evidence of the audio
/// track. English subtitles never sink a release: they are what a viewer reading along most often wants. The index
/// of a tag is its bit in `ReleaseTraits::tags`.
pub(crate) const LANGUAGE_TAGS: [(&str, TagKind, &str); 65] = [
    ("ita", TagKind::Audio, "it"),
    ("italian", TagKind::Audio, "it"),
    ("french", TagKind::Audio, "fr"),
    ("truefrench", TagKind::Audio, "fr"),
    ("vf", TagKind::Audio, "fr"),
    ("vff", TagKind::Audio, "fr"),
    ("vfq", TagKind::Audio, "fr"),
    ("vfi", TagKind::Audio, "fr"),
    ("vf2", TagKind::Audio, "fr"),
    ("german", TagKind::Audio, "de"),
    ("ger", TagKind::Audio, "de"),
    ("deutsch", TagKind::Audio, "de"),
    ("spanish", TagKind::Audio, "es"),
    ("castellano", TagKind::Audio, "es"),
    ("latino", TagKind::Audio, "es"),
    ("esp", TagKind::Audio, "es"),
    ("spa", TagKind::Audio, "es"),
    ("rus", TagKind::Audio, "ru"),
    ("russian", TagKind::Audio, "ru"),
    ("ukr", TagKind::Audio, "uk"),
    ("ukrainian", TagKind::Audio, "uk"),
    ("polish", TagKind::Audio, "pl"),
    ("pldub", TagKind::Audio, "pl"),
    ("lektor", TagKind::Audio, "pl"),
    ("portuguese", TagKind::Audio, "pt"),
    ("dublado", TagKind::Audio, "pt"),
    ("hindi", TagKind::Audio, "hi"),
    ("tamil", TagKind::Audio, "ta"),
    ("telugu", TagKind::Audio, "te"),
    ("turkish", TagKind::Audio, "tr"),
    ("japanese", TagKind::Audio, "ja"),
    ("jap", TagKind::Audio, "ja"),
    ("korean", TagKind::Audio, "ko"),
    ("kor", TagKind::Audio, "ko"),
    ("chinese", TagKind::Audio, "zh"),
    ("mandarin", TagKind::Audio, "zh"),
    ("cantonese", TagKind::Audio, "zh"),
    ("swedish", TagKind::Audio, "sv"),
    ("swe", TagKind::Audio, "sv"),
    ("norwegian", TagKind::Audio, "no"),
    ("nor", TagKind::Audio, "no"),
    ("danish", TagKind::Audio, "da"),
    ("dan", TagKind::Audio, "da"),
    ("finnish", TagKind::Audio, "fi"),
    ("fin", TagKind::Audio, "fi"),
    ("dutch", TagKind::Audio, "nl"),
    ("flemish", TagKind::Audio, "nl"),
    ("czech", TagKind::Audio, "cs"),
    ("cze", TagKind::Audio, "cs"),
    ("hungarian", TagKind::Audio, "hu"),
    ("hun", TagKind::Audio, "hu"),
    ("english", TagKind::Audio, "en"),
    ("eng", TagKind::Audio, "en"),
    ("vostfr", TagKind::Subs, "fr"),
    ("vost", TagKind::Subs, "fr"),
    ("subfrench", TagKind::Subs, "fr"),
    ("nlsub", TagKind::Subs, "nl"),
    ("nlsubs", TagKind::Subs, "nl"),
    ("plsub", TagKind::Subs, "pl"),
    ("korsub", TagKind::Subs, "ko"),
    ("ensub", TagKind::Subs, "en"),
    ("ensubbed", TagKind::Subs, "en"),
    ("engsub", TagKind::Subs, "en"),
    ("multi", TagKind::Multi, ""),
    ("dual", TagKind::Multi, ""),
];

static TAG_INDEX: LazyLock<HashMap<&'static str, u8>> =
    LazyLock::new(|| LANGUAGE_TAGS.iter().enumerate().map(|(i, (tok, _, _))| (*tok, i as u8)).collect());

/// Go `foldDiacritics`: maps accented Latin letters onto their plain forms before tokenising. Tokens are `[a-z0-9]`
/// runs, so without it "Pokémon" split into "pok" and "mon".
pub(crate) fn fold_diacritics(s: &str) -> String {
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

/// Go `titleTokenRe` (`[a-z0-9]+`, ASCII): the maximal runs of lowercase ASCII letters and digits.
pub(crate) fn ascii_tokens(s: &str) -> impl Iterator<Item = &str> {
    s.split(|c: char| !(c.is_ascii_lowercase() || c.is_ascii_digit())).filter(|t| !t.is_empty())
}

/// What a release name's extension says its container is (`container_of_ref`'s fallback).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) enum NameContainer {
    #[default]
    None,
    Mp4,
    Matroska,
    Webm,
    Avi,
}

impl NameContainer {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            NameContainer::None => "",
            NameContainer::Mp4 => "mp4",
            NameContainer::Matroska => "matroska",
            NameContainer::Webm => "webm",
            NameContainer::Avi => "avi",
        }
    }
}

/// Go `remuxRefusedExt`: den-remux opens Matroska, MP4, MPEG-TS and AVI, and refuses these by name (den-remux's
/// `scout::left_out_by_name`).
const REMUX_REFUSED_EXT: [&str; 7] = [".m2ts", ".iso", ".wmv", ".mpg", ".mpeg", ".vob", ".webm"];

const FLAG_OPUS: u8 = 1;
const FLAG_CYRILLIC: u8 = 1 << 1;
const FLAG_REMUX_REFUSED_EXT: u8 = 1 << 2;
const FLAG_TS_EXT: u8 = 1 << 3;

/// Everything the ranking, attribute and label code reads off a release title.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct ReleaseTraits {
    /// Bit `p as u8` is set when `Pat` p matches the lowercased title.
    pats: u64,
    /// Every plausible year the name gives (Go `releaseYears`), bit `year - 1900`.
    years: [u64; 3],
    /// Bit i is set when `LANGUAGE_TAGS[i]` is a token of the diacritic-folded title.
    tags: u128,
    /// 0 for a legit source, else 1 + the index of the class in `JUNK_CLASSES`.
    junk: u8,
    flags: u8,
    container: NameContainer,
}

impl ReleaseTraits {
    pub(crate) fn parse(title: &str) -> Self {
        Self::parse_lower(&str::to_lowercase(title))
    }

    /// `parse` for a title the caller has already lowercased.
    pub(crate) fn parse_lower(t: &str) -> Self {
        let mut out = ReleaseTraits::default();
        // `matches_read_at` into a stack array: `matches` allocates its answer on every call.
        let mut hit = [false; Pat::ALL.len()];
        MAIN_SET.matches_read_at(&mut hit, t, 0);
        for (i, _) in hit.iter().enumerate().filter(|(_, h)| **h) {
            out.pats |= 1 << i;
        }
        out.junk = junk_of(t);
        out.years = years_of(t);

        let folded;
        let tokens_of = if t.is_ascii() {
            t
        } else {
            folded = fold_diacritics(t);
            &folded
        };
        for tok in ascii_tokens(tokens_of) {
            if let Some(&i) = TAG_INDEX.get(tok) {
                out.tags |= 1 << i;
            }
        }
        if !t.is_ascii() && t.chars().any(|c| ('\u{0400}'..='\u{04FF}').contains(&c)) {
            out.flags |= FLAG_CYRILLIC;
        }
        // Opus is the second-to-last audio codec `detect_audio_codec` tries, and the one pattern that needs Unicode
        // mode, so it is matched only for a name that names none of the codecs ahead of it.
        let ahead = [
            Pat::Dtsx,
            Pat::Truehd,
            Pat::DtshdMa,
            Pat::DtshdAttr,
            Pat::Flac,
            Pat::Eac3,
            Pat::Dts,
            Pat::Ac3,
            Pat::Aac,
        ];
        if !ahead.iter().any(|&p| out.has(p)) && RE_OPUS.is_match(t) {
            out.flags |= FLAG_OPUS;
        }

        out.container = if t.ends_with(".mp4") || t.ends_with(".m4v") {
            NameContainer::Mp4
        } else if t.ends_with(".mkv") {
            NameContainer::Matroska
        } else if t.ends_with(".webm") {
            NameContainer::Webm
        } else if t.ends_with(".avi") {
            NameContainer::Avi
        } else {
            NameContainer::None
        };
        if REMUX_REFUSED_EXT.iter().any(|ext| t.ends_with(ext)) {
            out.flags |= FLAG_REMUX_REFUSED_EXT;
        }
        if t.ends_with(".ts") {
            out.flags |= FLAG_TS_EXT;
        }
        out
    }

    pub(crate) fn has(&self, p: Pat) -> bool {
        self.pats & (1 << p as u8) != 0
    }

    /// Go `junkClassOf`: the junk class, or "" for a legit source.
    pub(crate) fn junk_class(&self) -> &'static str {
        match self.junk {
            0 => "",
            n => JUNK_CLASSES[usize::from(n) - 1],
        }
    }

    /// Whether the name gives any year Go `releaseYears` reads.
    pub(crate) fn has_year(&self) -> bool {
        self.years != [0; 3]
    }

    /// Whether the name gives a year in `lo..=hi`.
    pub(crate) fn year_in(&self, lo: i64, hi: i64) -> bool {
        (lo.max(1900)..=hi.min(2039)).any(|y| {
            let bit = (y - 1900) as usize;
            self.years[bit / 64] & (1 << (bit % 64)) != 0
        })
    }

    /// The `LANGUAGE_TAGS` entries the name carries.
    pub(crate) fn language_tags(
        &self,
    ) -> impl Iterator<Item = &'static (&'static str, TagKind, &'static str)> + '_ {
        LANGUAGE_TAGS.iter().enumerate().filter(|(i, _)| self.tags & (1 << i) != 0).map(|(_, tag)| tag)
    }

    /// Whether the name is written in Cyrillic: a release made for that market, almost always dubbed.
    pub(crate) fn cyrillic(&self) -> bool {
        self.flags & FLAG_CYRILLIC != 0
    }

    /// Whether the name declares Opus audio; meaningful only when it names none of the codecs `detect_audio_codec`
    /// tries first.
    pub(crate) fn opus(&self) -> bool {
        self.flags & FLAG_OPUS != 0
    }

    pub(crate) fn name_container(&self) -> NameContainer {
        self.container
    }

    /// Whether the name ends on an extension den-remux refuses.
    pub(crate) fn remux_refused_ext(&self) -> bool {
        self.flags & FLAG_REMUX_REFUSED_EXT != 0
    }

    pub(crate) fn ts_ext(&self) -> bool {
        self.flags & FLAG_TS_EXT != 0
    }
}

/// Go `junkClassOf` over an already-lowercased title, as an index into `JUNK_CLASSES` plus one.
fn junk_of(t: &str) -> u8 {
    let t = CONTAINER_EXT.replace_all(t, "${1}");
    let mut m = [false; JUNK_PATTERNS.len()];
    JUNK_SET.matches_read_at(&mut m, &t, 0);
    if let Some(class) = (0..6).find(|&i| m[i]) {
        return class as u8 + 1;
    }
    // regexp2's MatchString reports an error as no match; so does this. fancy-regex's only runtime error is the
    // backtrack limit, which `UPSCALED` lifts.
    if m[JUNK_UPSCALED_WIDE] && UPSCALED.is_match(t.as_ref()).unwrap_or(false) {
        return 7;
    }
    if !m[JUNK_GOOD] {
        if m[JUNK_BARE_CAM] {
            return 1;
        }
        if m[JUNK_BARE_TS] {
            return 2;
        }
        if m[JUNK_BARE_SCR] {
            return 4;
        }
    }
    0
}

/// Go `releaseYears`: every plausible year in a lowercased release name (usually one, e.g. "(2026)"), in order.
pub(crate) fn year_matches(t: &str) -> impl Iterator<Item = i64> + '_ {
    let bytes = t.as_bytes();
    YEAR_TOKEN.find_iter(t).filter_map(move |m| {
        let before = if m.start() > 0 { bytes[m.start() - 1] } else { b' ' };
        let after = if m.end() < bytes.len() { bytes[m.end()] } else { b' ' };
        if before.is_ascii_digit() || before == b'x' {
            return None; // trailing digits of a bigger number, or a resolution like 1920x...
        }
        if after.is_ascii_digit() || after == b'p' || after == b'i' || after == b'x' {
            return None; // 2160p, 1920x..., etc.
        }
        m.as_str().parse::<i64>().ok()
    })
}

/// `year_matches` as a bitset, bit `year - 1900`.
fn years_of(t: &str) -> [u64; 3] {
    let mut out = [0u64; 3];
    for y in year_matches(t) {
        let bit = (y - 1900) as usize;
        out[bit / 64] |= 1 << (bit % 64);
    }
    out
}
