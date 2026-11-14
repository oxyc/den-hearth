//! `ReleaseTraits` parity: every reader of a release title (junk class, quality score, resolution, years, language
//! sink, attributes, label, binge group, browser cost and the whole ranking) gives what it gave when each one ran
//! its own regexes over the lowercased title. `legacy` is that code as it was, kept here as the oracle, and every
//! title the scout tests use, the benchmark releases when they have been generated, and a generated corpus that
//! crosses every tag the patterns know are run through both.

use std::collections::HashSet;

use crate::attributes::{cost_attributes, stream_attributes_ref, StreamAttributes};
use crate::client::{ClientDolbyVision, ClientPlayable};
use crate::label::{binge_group_of, clean_label_traits, MIB};
use crate::probe::Probe;
use crate::rank::{
    junk_class, quality_score_ref, rank_streams_debug, release_years, title_tokens, RankDebug, RankFilters,
    RawStream, GIB,
};
use crate::traits::{Pat, ReleaseTraits};

mod legacy {
    //! The title readers before `ReleaseTraits`, copied unchanged apart from paths.

    use std::collections::HashSet;
    use std::sync::LazyLock;

    use fancy_regex::{Regex as LookRegex, RegexBuilder as LookRegexBuilder};
    use regex::Regex;

    use crate::attributes::{with_probe, StreamAttributes};
    use crate::client::{
        ClientPlayable, AUDIO_CONVERTED_COST, NEVER_PLAYS_COST, VIDEO_CONVERTED_COST, VIDEO_UNNAMED_COST,
    };
    use crate::label::{release_group, size_label, MIB};
    use crate::rank::{
        other_episode, other_show, re2, title_overlap, DebugScore, RankDebug, RankFilters, RawStream, GIB,
        PREFERENCE_SINK,
    };

    pub(crate) enum Matcher {
        Re2(Regex),
        Look(LookRegex),
    }

    impl Matcher {
        fn is_match(&self, s: &str) -> bool {
            match self {
                Matcher::Re2(r) => r.is_match(s),
                Matcher::Look(r) => r.is_match(s).unwrap_or(false),
            }
        }
    }

    fn look(pattern: &str) -> Matcher {
        let re = LookRegexBuilder::new(pattern)
            .backtrack_limit(usize::MAX)
            .build()
            .expect("static lookaround pattern");
        Matcher::Look(re)
    }

    static RE_GOOD_SOURCE: LazyLock<Regex> = LazyLock::new(|| {
        re2(r"remux|bluray|blu-ray|b[dr][ .\-_]?rip|web[ .\-_]?(dl|rip)?|hdtv|dvd[ .\-_]?rip")
    });

    static UNAMBIGUOUS_JUNK: LazyLock<Vec<(&'static str, Matcher)>> = LazyLock::new(|| {
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

    static RE_BARE_CAM: LazyLock<Regex> = LazyLock::new(|| re2(r"\bcam\b"));
    static RE_BARE_TS: LazyLock<Regex> = LazyLock::new(|| re2(r"\bts\b"));
    static RE_BARE_SCR: LazyLock<Regex> = LazyLock::new(|| re2(r"\bscr\b"));
    static CONTAINER_EXT: LazyLock<Regex> =
        LazyLock::new(|| re2(r"\.(?:m2ts|ts|mkv|mp4|avi|m4v|webm)([\t\n\f\r ]|$)"));

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

    static RES_2160: LazyLock<Regex> = LazyLock::new(|| re2(r"2160p?"));
    static RES_1440: LazyLock<Regex> = LazyLock::new(|| re2(r"1440p?"));
    static RES_1080: LazyLock<Regex> = LazyLock::new(|| re2(r"1080p?"));
    static RES_720: LazyLock<Regex> = LazyLock::new(|| re2(r"720p?"));
    static RES_576: LazyLock<Regex> = LazyLock::new(|| re2(r"576p?"));
    static RES_540: LazyLock<Regex> = LazyLock::new(|| re2(r"540p?"));
    static RES_480: LazyLock<Regex> = LazyLock::new(|| re2(r"480p?"));
    static RES_4K_UHD: LazyLock<Regex> = LazyLock::new(|| re2(r"4k|uhd"));

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

    const FAKE_4K_FLOOR: i64 = 700 * MIB;

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

    static RE_REMUX: LazyLock<Regex> = LazyLock::new(|| re2(r"\bremux\b"));
    static RE_BLURAY: LazyLock<Regex> = LazyLock::new(|| re2(r"bluray|blu-ray"));
    static RE_BR_RIP: LazyLock<Regex> = LazyLock::new(|| re2(r"b[dr][ .\-_]?rip"));
    static RE_WEB_DL: LazyLock<Regex> = LazyLock::new(|| re2(r"web[ .\-_]?dl"));
    static RE_WEB_RIP: LazyLock<Regex> = LazyLock::new(|| re2(r"web[ .\-_]?rip"));
    static RE_WEB: LazyLock<Regex> = LazyLock::new(|| re2(r"\bweb\b"));
    static RE_HDTV: LazyLock<Regex> = LazyLock::new(|| re2(r"\bhdtv\b"));
    static RE_DVD_RIP: LazyLock<Regex> = LazyLock::new(|| re2(r"dvd[ .\-_]?rip"));
    static RE_LOW_SRC: LazyLock<Regex> = LazyLock::new(|| re2(r"tvrip|satrip|pdtv"));
    static RE_DOVI: LazyLock<Regex> = LazyLock::new(|| re2(r"dolby vision|dolbyvision|dovi|\bdv\b"));
    static RE_HDR10P: LazyLock<Regex> = LazyLock::new(|| re2(r"hdr10\+|hdr10plus"));
    static RE_HDR: LazyLock<Regex> = LazyLock::new(|| re2(r"\bhdr\b|\bhlg\b"));
    static RE_ATMOS: LazyLock<Regex> = LazyLock::new(|| re2(r"atmos"));
    static RE_DTSX: LazyLock<Regex> = LazyLock::new(|| re2(r"dts:x|dtsx|dts-x"));
    static RE_TRUEHD: LazyLock<Regex> = LazyLock::new(|| re2(r"truehd|true-hd"));
    static RE_DTSHD_MA: LazyLock<Regex> = LazyLock::new(|| re2(r"dts-hd ma|dts-hd\.ma|dts-ma"));
    static RE_DTSHD: LazyLock<Regex> = LazyLock::new(|| re2(r"dts-hd|dts hd|dtshd|flac|lpcm|pcm"));
    static RE_EAC3: LazyLock<Regex> = LazyLock::new(|| re2(r"eac3|e-ac3|dd\+|ddp|ddplus"));
    static RE_DTS: LazyLock<Regex> = LazyLock::new(|| re2(r"\bdts\b"));
    static RE_AC3: LazyLock<Regex> = LazyLock::new(|| re2(r"ac3|\bdd\b|dolby digital"));
    static RE_IS_4K: LazyLock<Regex> = LazyLock::new(|| re2(r"2160p?|4k|uhd"));
    static RE_AV1: LazyLock<Regex> = LazyLock::new(|| re2(r"av1"));
    static RE_3D: LazyLock<Regex> = LazyLock::new(|| re2(r"\b3d\b|hsbs|half[ .\-_]?sbs|sbs[ .\-_]?3d"));
    static RE_KORSUB_HC: LazyLock<Regex> = LazyLock::new(|| re2(r"korsub|\bhc\b"));
    static RE_HDR_ONLY: LazyLock<Regex> =
        LazyLock::new(|| re2(r"dolby vision|dolbyvision|dovi|\bhdr\b|hdr10|\bhlg\b"));

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

        let atmos = RE_ATMOS.is_match(t);
        if RE_EAC3.is_match(t) && atmos {
            score += 30;
        } else if RE_EAC3.is_match(t) {
            score += 20;
        } else if RE_AC3.is_match(t) {
            score += 14;
        } else if RE_DTSX.is_match(t)
            || RE_TRUEHD.is_match(t)
            || RE_DTSHD_MA.is_match(t)
            || RE_DTSHD.is_match(t)
        {
            score += 12;
        } else if RE_DTS.is_match(t) {
            score += 8;
        } else if atmos {
            score += 20;
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

    pub(crate) fn quality_score_ref(s: &RawStream) -> i64 {
        let t = str::to_lowercase(&s.title);
        quality_score_lower(&t, s, junk_class_of(&t))
    }

    const WRONG_SHOW_SINK: i64 = 30_000;
    const FOREIGN_AUDIO_SINK: i64 = 15_000;
    const FOREIGN_SUBS_SINK: i64 = PREFERENCE_SINK;

    const AUDIO_LANGUAGE_TAGS: &[(&str, &str)] = &[
        ("ita", "it"),
        ("italian", "it"),
        ("french", "fr"),
        ("truefrench", "fr"),
        ("vf", "fr"),
        ("vff", "fr"),
        ("vfq", "fr"),
        ("vfi", "fr"),
        ("vf2", "fr"),
        ("german", "de"),
        ("ger", "de"),
        ("deutsch", "de"),
        ("spanish", "es"),
        ("castellano", "es"),
        ("latino", "es"),
        ("esp", "es"),
        ("spa", "es"),
        ("rus", "ru"),
        ("russian", "ru"),
        ("ukr", "uk"),
        ("ukrainian", "uk"),
        ("polish", "pl"),
        ("pldub", "pl"),
        ("lektor", "pl"),
        ("portuguese", "pt"),
        ("dublado", "pt"),
        ("hindi", "hi"),
        ("tamil", "ta"),
        ("telugu", "te"),
        ("turkish", "tr"),
        ("japanese", "ja"),
        ("jap", "ja"),
        ("korean", "ko"),
        ("kor", "ko"),
        ("chinese", "zh"),
        ("mandarin", "zh"),
        ("cantonese", "zh"),
        ("swedish", "sv"),
        ("swe", "sv"),
        ("norwegian", "no"),
        ("nor", "no"),
        ("danish", "da"),
        ("dan", "da"),
        ("finnish", "fi"),
        ("fin", "fi"),
        ("dutch", "nl"),
        ("flemish", "nl"),
        ("czech", "cs"),
        ("cze", "cs"),
        ("hungarian", "hu"),
        ("hun", "hu"),
        ("english", "en"),
        ("eng", "en"),
    ];

    const SUBTITLE_LANGUAGE_TAGS: &[(&str, &str)] = &[
        ("vostfr", "fr"),
        ("vost", "fr"),
        ("subfrench", "fr"),
        ("nlsub", "nl"),
        ("nlsubs", "nl"),
        ("plsub", "pl"),
        ("korsub", "ko"),
        ("ensub", "en"),
        ("ensubbed", "en"),
        ("engsub", "en"),
    ];

    const MULTI_AUDIO_TAGS: &[&str] = &["multi", "dual"];

    fn is_cyrillic(c: char) -> bool {
        ('\u{0400}'..='\u{04FF}').contains(&c)
    }

    static TITLE_TOKEN_RE: LazyLock<Regex> = LazyLock::new(|| re2(r"[a-z0-9]+"));

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

    pub(crate) fn foreign_language_sink(lower: &str, f: &RankFilters) -> i64 {
        if f.languages.is_empty() {
            return 0;
        }
        let folded = fold_diacritics(lower);
        let mut audio: Vec<&str> = Vec::new();
        let mut subs: Vec<&str> = Vec::new();
        for m in TITLE_TOKEN_RE.find_iter(&folded) {
            let tok = m.as_str();
            if f.show_tokens.contains(tok) || f.expected_title_tokens.contains(tok) {
                continue;
            }
            if MULTI_AUDIO_TAGS.contains(&tok) {
                return 0;
            }
            if let Some((_, lang)) = AUDIO_LANGUAGE_TAGS.iter().find(|(t, _)| *t == tok) {
                audio.push(lang);
            } else if let Some((_, lang)) = SUBTITLE_LANGUAGE_TAGS.iter().find(|(t, _)| *t == tok) {
                subs.push(lang);
            }
        }
        if lower.chars().any(is_cyrillic) {
            audio.push("ru");
        }
        if !audio.is_empty() {
            return if audio.iter().any(|lang| f.languages.contains(*lang)) { 0 } else { FOREIGN_AUDIO_SINK };
        }
        if subs.iter().any(|lang| *lang != "en" && !f.languages.contains(*lang)) {
            FOREIGN_SUBS_SINK
        } else {
            0
        }
    }

    static YEAR_TOKEN: LazyLock<Regex> = LazyLock::new(|| re2(r"19\d\d|20[0-3]\d"));

    pub(crate) fn release_years(title: &str) -> Vec<i64> {
        let t = str::to_lowercase(title);
        let bytes = t.as_bytes();
        let mut out = Vec::new();
        for m in YEAR_TOKEN.find_iter(&t) {
            let before = if m.start() > 0 { bytes[m.start() - 1] } else { b' ' };
            let after = if m.end() < bytes.len() { bytes[m.end()] } else { b' ' };
            if before.is_ascii_digit() || before == b'x' {
                continue;
            }
            if after.is_ascii_digit() || after == b'p' || after == b'i' || after == b'x' {
                continue;
            }
            if let Ok(y) = m.as_str().parse::<i64>() {
                out.push(y);
            }
        }
        out
    }

    fn year_mismatch(title: &str, expected: i64) -> bool {
        let years = release_years(title);
        if years.is_empty() {
            return false;
        }
        !years.iter().any(|&y| y >= expected - 1 && y <= expected + 1)
    }

    fn drop_n(debug: &mut RankDebug, reason: &str, n: i64) {
        if n > 0 {
            *debug.dropped_by.entry(reason.to_string()).or_insert(0) += n;
        }
    }

    /// The old `rank_streams_debug`, for filters without an exclude pattern.
    pub(crate) fn rank_streams_debug(streams: &[RawStream], f: &RankFilters) -> (Vec<String>, RankDebug) {
        assert!(f.exclude_regex.is_empty(), "the oracle does not port the user pattern");
        let mut debug = RankDebug::default();
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
            if f.expected_year.is_some_and(|y| year_mismatch(&s.title, y)) {
                drop_n(&mut debug, "yearMismatch", 1);
                continue;
            }
            if !f.expected_title_tokens.is_empty()
                && release_years(&s.title).is_empty()
                && !title_overlap(&s.title, &f.expected_title_tokens)
            {
                drop_n(&mut debug, "titleMismatch", 1);
                continue;
            }
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
            if f.cached_only && !s.cached && s.cache_known {
                drop_n(&mut debug, "cachedOnly", 1);
                continue;
            }
            let mut score = quality_score_lower(&lower, s, junk);
            if let Some(client) = &f.client {
                score -= cost_ref(client, s, stream_attributes_ref(s));
            }
            if !f.prefer_resolution.is_empty() && !res.is_empty() && res != f.prefer_resolution {
                score -= PREFERENCE_SINK;
            }
            score -= foreign_language_sink(&lower, f);
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
        debug.ranked = out.len() as i64;
        debug.kept = out
            .iter()
            .map(|o| {
                let s = &streams[o.idx];
                DebugScore {
                    title: s.title.clone(),
                    score: o.score,
                    resolution: detect_resolution_lower(&str::to_lowercase(&s.title)).to_string(),
                    cached: s.cached,
                }
            })
            .collect();
        (out.iter().map(|o| streams[o.idx].info_hash.clone()).collect(), debug)
    }

    // ---- attributes.rs --------------------------------------------------------------------------------------

    static RE_DOVI_ATTR: LazyLock<Regex> = LazyLock::new(|| re2(r"dolby vision|dolbyvision|dovi|\bdv\b"));
    static RE_HDR_EXTRA: LazyLock<Regex> = LazyLock::new(|| re2(r"hdr10|\bhdr\b|\bhlg\b"));
    static RE_HLG: LazyLock<Regex> = LazyLock::new(|| re2(r"\bhlg\b"));
    static RE_HDR10_ANY: LazyLock<Regex> = LazyLock::new(|| re2(r"hdr10"));
    static RE_HDR_PLAIN: LazyLock<Regex> = LazyLock::new(|| re2(r"\bhdr\b"));
    static RE_CH71: LazyLock<Regex> = LazyLock::new(|| re2(r"7\.1|7 1|\b8ch\b"));
    static RE_CH51: LazyLock<Regex> = LazyLock::new(|| re2(r"5\.1|5 1|\b6ch\b"));
    static RE_CH20: LazyLock<Regex> = LazyLock::new(|| re2(r"2\.0|2 0|stereo|\b2ch\b"));
    static RE_HEVC: LazyLock<Regex> = LazyLock::new(|| re2(r"x265|h\.?265|hevc"));
    static RE_AVC: LazyLock<Regex> = LazyLock::new(|| re2(r"x264|h\.?264|\bavc\b"));
    static RE_DTSHD_ATTR: LazyLock<Regex> = LazyLock::new(|| re2(r"dts-hd|dts hd|dtshd"));
    static RE_FLAC: LazyLock<Regex> = LazyLock::new(|| re2(r"flac|lpcm|pcm"));
    static RE_AAC: LazyLock<Regex> = LazyLock::new(|| re2(r"\baac(?:[0-9]|\b)"));
    static RE_OPUS: LazyLock<Regex> = LazyLock::new(|| {
        Regex::new(
            r"(?:(?-u:\d){3,4}p|x26[45]|h\.?26[45]|hevc|av1|10.?bits?|web-?dl|webrip|bluray)(?-u:\b).*(?-u:\bopus\b)",
        )
        .expect("static pattern")
    });
    static RE_MP3: LazyLock<Regex> = LazyLock::new(|| re2(r"\bmp3\b"));
    static RE_HI10: LazyLock<Regex> = LazyLock::new(|| re2(r"\bhi10p?\b"));
    static RE_TEN_BIT: LazyLock<Regex> =
        LazyLock::new(|| re2(r"\b10[ .\-_]?bits?\b|\bhi10p?\b|\bmain ?10\b"));
    static RE_HYBRID: LazyLock<Regex> = LazyLock::new(|| re2(r"\bhybrid\b"));

    fn str_opt(s: &str) -> Option<String> {
        if s.is_empty() {
            None
        } else {
            Some(s.to_string())
        }
    }

    pub(crate) fn detect_source_attr(t: &str) -> &'static str {
        let junk = junk_class_of(t);
        if matches!(junk, "cam" | "telesync" | "screener") {
            return junk;
        }
        if RE_REMUX.is_match(t) {
            "remux"
        } else if RE_BLURAY.is_match(t) || RE_BR_RIP.is_match(t) {
            "bluray"
        } else if RE_WEB_DL.is_match(t) {
            "webdl"
        } else if RE_WEB_RIP.is_match(t) {
            "webrip"
        } else if RE_WEB.is_match(t) {
            "web"
        } else if RE_HDTV.is_match(t) {
            "hdtv"
        } else if RE_DVD_RIP.is_match(t) {
            "dvdrip"
        } else {
            ""
        }
    }

    fn detect_codec(t: &str) -> &'static str {
        if RE_AV1.is_match(t) {
            "av1"
        } else if RE_HEVC.is_match(t) {
            "hevc"
        } else if RE_AVC.is_match(t) || RE_HI10.is_match(t) {
            "h264"
        } else {
            ""
        }
    }

    fn detect_bit_depth(t: &str, hdr: bool) -> i64 {
        if hdr || RE_TEN_BIT.is_match(t) {
            10
        } else {
            0
        }
    }

    fn detect_hdr_format(t: &str) -> &'static str {
        if RE_HDR10P.is_match(t) {
            "HDR10+"
        } else if RE_HLG.is_match(t) {
            "HLG"
        } else if RE_HDR10_ANY.is_match(t) {
            "HDR10"
        } else if RE_HDR_PLAIN.is_match(t) {
            "HDR"
        } else {
            ""
        }
    }

    pub(crate) fn detect_audio_codec(t: &str) -> &'static str {
        if RE_DTSX.is_match(t) {
            "dtsx"
        } else if RE_TRUEHD.is_match(t) {
            "truehd"
        } else if RE_DTSHD_MA.is_match(t) {
            "dtshdma"
        } else if RE_DTSHD_ATTR.is_match(t) {
            "dtshd"
        } else if RE_FLAC.is_match(t) {
            "flac"
        } else if RE_EAC3.is_match(t) {
            "eac3"
        } else if RE_DTS.is_match(t) {
            "dts"
        } else if RE_AC3.is_match(t) {
            "ac3"
        } else if RE_AAC.is_match(t) {
            "aac"
        } else if RE_OPUS.is_match(t) {
            "opus"
        } else if RE_MP3.is_match(t) {
            "mp3"
        } else {
            ""
        }
    }

    fn detect_channels(t: &str) -> &'static str {
        if RE_CH71.is_match(t) {
            "7.1"
        } else if RE_CH51.is_match(t) {
            "5.1"
        } else if RE_CH20.is_match(t) {
            "2.0"
        } else {
            ""
        }
    }

    fn detect_audio(t: &str) -> &'static str {
        if RE_ATMOS.is_match(t) {
            "Atmos"
        } else if RE_DTSX.is_match(t) {
            "DTS:X"
        } else if RE_TRUEHD.is_match(t) {
            "TrueHD"
        } else if RE_DTSHD_ATTR.is_match(t) {
            "DTS-HD"
        } else if RE_FLAC.is_match(t) {
            "FLAC"
        } else if RE_EAC3.is_match(t) {
            "EAC3"
        } else if RE_DTS.is_match(t) {
            "DTS"
        } else if RE_AC3.is_match(t) {
            "AC3"
        } else {
            ""
        }
    }

    pub(crate) fn stream_attributes_ref(s: &RawStream) -> StreamAttributes {
        let t = str::to_lowercase(&s.title);
        let dolby_vision = RE_DOVI_ATTR.is_match(&t);
        let hdr = dolby_vision || RE_HDR_EXTRA.is_match(&t);
        let mut attrs = StreamAttributes {
            resolution: str_opt(detect_resolution_lower(&t)),
            source: str_opt(detect_source_attr(&t)),
            codec: str_opt(detect_codec(&t)),
            hdr,
            dolby_vision,
            hdr_format: str_opt(detect_hdr_format(&t)),
            audio: str_opt(detect_audio(&t)),
            audio_codec: str_opt(detect_audio_codec(&t)),
            audio_channels: str_opt(detect_channels(&t)),
            atmos: RE_ATMOS.is_match(&t),
            hardcoded_subs: RE_KORSUB_HC.is_match(&t),
            three_d: RE_3D.is_match(&t),
            size_bytes: s.size_bytes,
            pack_size_bytes: s.pack_size_bytes,
            seeders: s.seeders,
            cached: if s.cache_known { Some(s.cached) } else { None },
            label: clean_label_lower(&t, s),
            bit_depth: detect_bit_depth(&t, hdr),
            ..Default::default()
        };
        attrs = with_probe(attrs, s.probe.as_ref());
        if attrs.dolby_vision && attrs.dv_profile == 0 {
            attrs.dv_profile_guess = guess_dv_profile(&t, attrs.hdr_format.is_some());
        }
        attrs
    }

    fn guess_dv_profile(t: &str, names_hdr_base: bool) -> i64 {
        if RE_REMUX.is_match(t) {
            7
        } else if names_hdr_base || RE_HYBRID.is_match(t) || RE_BLURAY.is_match(t) || RE_BR_RIP.is_match(t) {
            8
        } else if RE_WEB_DL.is_match(t) || RE_WEB_RIP.is_match(t) || RE_WEB.is_match(t) {
            5
        } else {
            0
        }
    }

    pub(crate) fn container_of_ref(s: &RawStream) -> String {
        if let Some(p) = &s.probe {
            if !p.container.is_empty() {
                return p.container.clone();
            }
        }
        let t = str::to_lowercase(&s.title);
        if t.ends_with(".mp4") || t.ends_with(".m4v") {
            "mp4".to_string()
        } else if t.ends_with(".mkv") {
            "matroska".to_string()
        } else if t.ends_with(".webm") {
            "webm".to_string()
        } else if t.ends_with(".avi") {
            "avi".to_string()
        } else {
            String::new()
        }
    }

    // ---- label.rs -------------------------------------------------------------------------------------------

    static RE_HDR_LABEL: LazyLock<Regex> = LazyLock::new(|| re2(r"\bhdr\b|hdr10|\bhlg\b"));

    fn resolution_label(t: &str, size_bytes: Option<i64>) -> &'static str {
        if RES_2160.is_match(t) {
            return "4K";
        }
        if RES_1440.is_match(t) {
            return "1440p";
        }
        if RES_1080.is_match(t) {
            return "1080p";
        }
        if RES_720.is_match(t) {
            return "720p";
        }
        if RES_480.is_match(t) || RES_576.is_match(t) || RES_540.is_match(t) {
            return "SD";
        }
        if RES_4K_UHD.is_match(t) && size_bytes.unwrap_or(0) > 3 * GIB {
            return "4K";
        }
        ""
    }

    fn dynamic_range_label(t: &str) -> &'static str {
        if RE_DOVI.is_match(t) {
            "Dolby Vision"
        } else if RE_HDR10P.is_match(t) {
            "HDR10+"
        } else if RE_HDR_LABEL.is_match(t) {
            "HDR"
        } else {
            ""
        }
    }

    fn source_label_for_display(t: &str) -> &'static str {
        if RE_REMUX.is_match(t) {
            "REMUX"
        } else if RE_BLURAY.is_match(t) || RE_BR_RIP.is_match(t) {
            "BluRay"
        } else if RE_WEB_DL.is_match(t) {
            "WEB-DL"
        } else if RE_WEB_RIP.is_match(t) || RE_WEB.is_match(t) {
            "WEB"
        } else {
            ""
        }
    }

    pub(crate) fn binge_group(t: &str, title: &str) -> String {
        let dr = if RE_DOVI.is_match(t) {
            "dv"
        } else if RE_HDR_LABEL.is_match(t) || RE_HDR10P.is_match(t) {
            "hdr"
        } else {
            ""
        };
        let group = release_group(title);
        ["den-scout", detect_resolution_lower(t), detect_source_attr(t), dr, group.as_str()].join("|")
    }

    pub(crate) fn clean_label_lower(t: &str, s: &RawStream) -> String {
        let mut parts: Vec<String> = Vec::new();
        let res = resolution_label(t, s.size_bytes);
        if !res.is_empty() {
            parts.push(res.to_owned());
        }
        let src = source_label_for_display(t);
        if !src.is_empty() {
            parts.push(src.to_owned());
        }
        let dr = dynamic_range_label(t);
        if !dr.is_empty() {
            parts.push(dr.to_owned());
        }
        if RE_ATMOS.is_match(t) {
            parts.push("Atmos".to_owned());
        }
        if let Some(size) = s.size_bytes {
            parts.push(size_label(size));
        } else if let Some(pack) = s.pack_size_bytes {
            parts.push(format!("{} pack", size_label(pack)));
        }
        let mut label = if parts.is_empty() { "Stream".to_owned() } else { parts.join(" • ") };
        if let (Some(_), Some(pack)) = (s.size_bytes, s.pack_size_bytes) {
            label.push_str(" · from a ");
            label.push_str(&size_label(pack));
            label.push_str(" pack");
        }
        label
    }

    // ---- client.rs ------------------------------------------------------------------------------------------

    pub(crate) fn cost_ref(p: &ClientPlayable, s: &RawStream, a: StreamAttributes) -> i64 {
        if a.three_d || !remux_opens(s) {
            return NEVER_PLAYS_COST;
        }
        if remux_converts(s) {
            return VIDEO_CONVERTED_COST + AUDIO_CONVERTED_COST;
        }
        let mut cost = 0;
        if a.dv_profile == 5 && !p.dolby_vision.p5 {
            cost += VIDEO_CONVERTED_COST * 2;
        }
        if a.dv_profile == 0 && a.dv_profile_guess == 5 && !p.dolby_vision.p5 {
            cost += VIDEO_CONVERTED_COST;
        }
        let uhd = a.resolution.as_deref().unwrap_or("") == "2160p";
        let ten_bit = a.bit_depth >= 10;
        let beyond = |most: i64, uhd_level: i64| {
            if a.video_level > 0 {
                a.video_level > most
            } else {
                uhd && most < uhd_level
            }
        };
        match a.codec.as_deref().unwrap_or("") {
            "h264" => {
                let mut most = p.h264;
                if ten_bit && (a.probed || !a.hdr) {
                    most = p.h264_high10;
                }
                if most == 0 || beyond(most, 51) {
                    return NEVER_PLAYS_COST;
                }
            }
            "hevc" => {
                let mut most = p.hevc_main10;
                if !ten_bit {
                    most = most.max(p.hevc_main);
                }
                if a.high_tier {
                    most = p.hevc_high_tier;
                }
                if most == 0 || beyond(most, 150) || (a.hdr && !p.hdr) {
                    cost += VIDEO_CONVERTED_COST;
                }
            }
            "av1" => {
                let mut most = p.av1_main10;
                if !ten_bit {
                    most = most.max(p.av1);
                }
                if most == 0 || a.high_tier || beyond(most, 12) || (a.hdr && !p.av1_hdr) {
                    return NEVER_PLAYS_COST;
                }
            }
            "vp9" => {
                let takes = if ten_bit { p.vp9_profile2 } else { p.vp9 };
                if !takes {
                    return NEVER_PLAYS_COST;
                }
            }
            "" => cost += VIDEO_UNNAMED_COST,
            _ => return NEVER_PLAYS_COST,
        }
        match a.audio_codec.as_deref().unwrap_or("") {
            "aac" => {}
            "eac3" | "ac3" => {
                if !p.eac3 {
                    cost += AUDIO_CONVERTED_COST;
                }
            }
            "flac" => {
                if !p.flac {
                    cost += AUDIO_CONVERTED_COST;
                }
            }
            _ => cost += AUDIO_CONVERTED_COST,
        }
        cost
    }

    const REMUX_REFUSED_EXT: [&str; 7] = [".m2ts", ".iso", ".wmv", ".mpg", ".mpeg", ".vob", ".webm"];

    fn remux_opens(s: &RawStream) -> bool {
        if container_of_ref(s) == "webm" {
            return false;
        }
        let name = str::to_lowercase(&s.title);
        !REMUX_REFUSED_EXT.iter().any(|ext| name.ends_with(ext))
    }

    fn remux_converts(s: &RawStream) -> bool {
        let name = str::to_lowercase(&s.title);
        container_of_ref(s) == "avi" || name.ends_with(".ts") || name.ends_with(".avi")
    }

    pub(crate) fn hdr_only(lower: &str) -> bool {
        RE_HDR_ONLY.is_match(lower)
    }

    pub(crate) fn tokens(set: &[&str]) -> HashSet<String> {
        set.iter().map(|s| s.to_string()).collect()
    }
}

/// Every string literal in the scout sources: the titles every existing test feeds the readers, plus whatever
/// else is quoted there (harmless as input, and more names to compare on).
fn source_literals() -> Vec<String> {
    let mut out = Vec::new();
    for dir in ["src", "src/tests", "src/handler", "src/stores", "src/stores/torbox"] {
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join(dir);
        for entry in std::fs::read_dir(&path).unwrap_or_else(|e| panic!("{}: {e}", path.display())) {
            let file = entry.unwrap().path();
            if file.extension().is_some_and(|e| e == "rs" || e == "tsv") {
                let text = std::fs::read_to_string(&file).unwrap();
                out.extend(text.split('"').skip(1).step_by(2).map(|s| s.replace("\\n", "\n")));
            }
        }
    }
    out
}

/// The benchmark's releases, when `bench/gen_fixtures.py` has written them.
fn bench_titles() -> Vec<String> {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../bench/fixtures/releases.json");
    let Ok(body) = std::fs::read(path) else { return Vec::new() };
    let raw: Vec<serde_json::Value> = serde_json::from_slice(&body).unwrap();
    raw.iter().filter_map(|r| r["title"].as_str().map(str::to_owned)).collect()
}

/// Names built from every tag the patterns and tag tables know, in many orders and separators, from a fixed seed.
fn generated_titles(n: usize) -> Vec<String> {
    const NAMES: &[&str] = &[
        "Dune Part Two",
        "The Batman",
        "Mr Hollands Opus",
        "Pokémon",
        "Fin",
        "Il Divo",
        "Toy Story 5",
        "Upgrade",
        "Ready Player One",
        "Aachen",
        "Fast & Furious",
        "Кино",
        "Mad Max: Fury Road",
        "La La Land",
        "Dual",
        "Multi",
        "The Ring",
        "Not Upscaled",
        "Notting Hill",
    ];
    const TAGS: &[&str] = &[
        "2160p",
        "2160",
        "1440p",
        "1080p",
        "1080",
        "720p",
        "576p",
        "540p",
        "480p",
        "4K",
        "UHD",
        "REMUX",
        "BluRay",
        "Blu-Ray",
        "BDRip",
        "BRRip",
        "BD-Rip",
        "WEB-DL",
        "WEBDL",
        "WEB.DL",
        "WEBRip",
        "WEB",
        "HDTV",
        "DVDRip",
        "TVRip",
        "SATRip",
        "PDTV",
        "DV",
        "DoVi",
        "Dolby Vision",
        "DolbyVision",
        "HDR",
        "HDR10",
        "HDR10+",
        "HDR10Plus",
        "HLG",
        "SDR",
        "Atmos",
        "DTS:X",
        "DTSX",
        "DTS-X",
        "TrueHD",
        "True-HD",
        "DTS-HD MA",
        "DTS-HD.MA",
        "DTS-MA",
        "DTS-HD",
        "DTS HD",
        "DTSHD",
        "FLAC",
        "LPCM",
        "PCM",
        "EAC3",
        "E-AC3",
        "DD+",
        "DDP",
        "DDP5.1",
        "DDPlus",
        "DTS",
        "AC3",
        "DD",
        "Dolby Digital",
        "AAC",
        "AAC2.0",
        "HE-AAC",
        "Opus",
        "MP3",
        "AV1",
        "x265",
        "H.265",
        "H265",
        "HEVC",
        "x264",
        "H.264",
        "AVC",
        "Hi10P",
        "Hi10",
        "10bit",
        "10-bit",
        "10 bits",
        "Main10",
        "Main 10",
        "HYBRID",
        "3D",
        "HSBS",
        "Half-SBS",
        "SBS 3D",
        "KORSUB",
        "HC",
        "7.1",
        "7 1",
        "8ch",
        "5.1",
        "5 1",
        "6ch",
        "2.0",
        "2 0",
        "Stereo",
        "2ch",
        "HDCAM",
        "HQCAM",
        "CAMRip",
        "CAM",
        "S Print",
        "TELESYNC",
        "Telesynch",
        "HD-TS",
        "HDTS",
        "TSRip",
        "TS",
        "TELECINE",
        "HDTC",
        "DVDScr",
        "BDSCR",
        "WEBSCR",
        "SCR",
        "PDVD",
        "PreDVDRip",
        "Screener",
        "Workprint",
        "R5",
        "1xBET",
        "Read.Note",
        "ReadNote",
        "Upscaled",
        "Upscale",
        "Up-Rez",
        "UpRez",
        "Not Upscaled",
        "Non-Upscaled",
        "not.upscaled",
        "AI Upscaled",
        "AI.Enhanced",
        "AI-Enhance",
        "Regraded",
        "Re-Grade",
        "ITA",
        "Italian",
        "French",
        "TRUEFRENCH",
        "VF",
        "VFF",
        "VFQ",
        "VFI",
        "VF2",
        "German",
        "GER",
        "Deutsch",
        "Spanish",
        "Castellano",
        "Latino",
        "ESP",
        "SPA",
        "RUS",
        "Russian",
        "UKR",
        "Ukrainian",
        "Polish",
        "PLDUB",
        "Lektor",
        "Portuguese",
        "Dublado",
        "Hindi",
        "Tamil",
        "Telugu",
        "Turkish",
        "Japanese",
        "JAP",
        "Korean",
        "KOR",
        "Chinese",
        "Mandarin",
        "Cantonese",
        "Swedish",
        "SWE",
        "Norwegian",
        "NOR",
        "Danish",
        "DAN",
        "Finnish",
        "FIN",
        "Dutch",
        "Flemish",
        "Czech",
        "CZE",
        "Hungarian",
        "HUN",
        "English",
        "ENG",
        "VOSTFR",
        "VOST",
        "SUBFRENCH",
        "NLSUB",
        "NLSubs",
        "PLSUB",
        "ENSUB",
        "ENSubbed",
        "ENGSUB",
        "MULTI",
        "DUAL",
        "Dual-Audio",
        "Русский",
        "Дубляж",
        "S01E05",
        "S01E01",
        "S00E82",
        "1x05",
        "E05",
        "EP05",
        "- 05",
        "Season 1",
        "S01.COMPLETE",
        "Temporada 1",
        "Säsong 1",
        "1999",
        "2001",
        "2017",
        "2023",
        "2024",
        "2025",
        "(2024)",
        "1920x1080",
        "2039",
        "2040",
        "AMZN",
        "NF",
        "Rutracker",
        "www.site.org -",
        "[Group]",
        "(Grp)",
    ];
    const ENDS: &[&str] = &[
        "",
        "",
        "-FLUX",
        "-NTb",
        "-RARBG",
        ".mkv",
        ".mp4",
        ".m4v",
        ".avi",
        ".webm",
        ".ts",
        ".m2ts",
        ".iso",
        ".wmv",
        ".mpg",
        ".mpeg",
        ".vob",
        ".TS",
        ".MKV",
        ".ts\n",
        ".ts\u{b}",
        "\nextra line",
        " ",
    ];
    const SEPS: &[&str] = &[".", " ", "-", "_", "."];
    let mut seed: u64 = 0x5eed_c0ff_ee15_0001;
    let mut next = |bound: usize| {
        seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
        ((seed >> 33) as usize) % bound
    };
    (0..n)
        .map(|_| {
            let sep = SEPS[next(SEPS.len())];
            let mut parts = vec![NAMES[next(NAMES.len())].replace(' ', sep)];
            for _ in 0..next(9) {
                parts.push(TAGS[next(TAGS.len())].to_owned());
            }
            let mut title = parts.join(sep);
            title.push_str(ENDS[next(ENDS.len())]);
            if next(4) == 0 {
                title = title.to_uppercase();
            }
            title
        })
        .collect()
}

fn corpus() -> Vec<String> {
    let mut seen = HashSet::new();
    let mut out = Vec::new();
    for t in source_literals().into_iter().chain(bench_titles()).chain(generated_titles(8_000)) {
        if seen.insert(t.clone()) {
            out.push(t);
        }
    }
    out
}

fn probes() -> Vec<Option<Probe>> {
    vec![
        None,
        Some(Probe::default()),
        Some(Probe {
            container: "mp4".into(),
            video_codec: "hevc".into(),
            audio_channels: "5.1".into(),
            bit_depth: 10,
            width: 3840,
            height: 2160,
            video_level: 153,
            ..Default::default()
        }),
        Some(Probe {
            container: "matroska".into(),
            video_codec: "h264".into(),
            dolby_vision: true,
            dv_profile: 5,
            hdr_format: "HDR10".into(),
            audio: vec!["en".into(), "fr".into()],
            ..Default::default()
        }),
        Some(Probe { container: "avi".into(), video_codec: "mpeg4".into(), ..Default::default() }),
        Some(Probe {
            container: "webm".into(),
            video_codec: "vp9".into(),
            bit_depth: 10,
            ..Default::default()
        }),
    ]
}

fn clients() -> Vec<ClientPlayable> {
    vec![
        ClientPlayable::default(),
        ClientPlayable {
            h264: 51,
            hevc_main: 150,
            hevc_main10: 150,
            hdr: true,
            eac3: true,
            ..Default::default()
        },
        ClientPlayable {
            h264: 52,
            h264_high10: 51,
            hevc_main10: 153,
            hevc_high_tier: 153,
            av1: 12,
            av1_main10: 12,
            av1_hdr: true,
            flac: true,
            vp9: true,
            vp9_profile2: true,
            dolby_vision: ClientDolbyVision { p5: true, p8: true },
            ..Default::default()
        },
    ]
}

fn stream(title: &str, i: usize) -> RawStream {
    const SIZES: [Option<i64>; 7] = [
        None,
        Some(300 * MIB),
        Some(699 * MIB),
        Some(2 * GIB),
        Some(3 * GIB + 1),
        Some(20 * GIB),
        Some(400 * GIB),
    ];
    RawStream {
        info_hash: format!("{i:040x}"),
        title: title.to_owned(),
        size_bytes: SIZES[i % SIZES.len()],
        pack_size_bytes: i.is_multiple_of(5).then_some(60 * GIB),
        seeders: (!i.is_multiple_of(6)).then_some((i % 37) as i64),
        cached: i.is_multiple_of(3),
        cache_known: !i.is_multiple_of(4),
        ..Default::default()
    }
}

/// Every per-release reader gives what the regex-per-reader code gave, for every title, size, cache state and
/// probe the corpus crosses.
#[test]
fn every_title_reader_matches_the_regex_code() {
    let titles = corpus();
    assert!(titles.len() > 8_000, "corpus too small: {}", titles.len());
    let probes = probes();
    let clients = clients();
    let filter_sets = [
        RankFilters { languages: legacy::tokens(&["en"]), ..Default::default() },
        RankFilters { languages: legacy::tokens(&["it", "en"]), ..Default::default() },
        RankFilters {
            languages: legacy::tokens(&["fi"]),
            expected_title_tokens: title_tokens("Fin"),
            show_tokens: title_tokens("Dual Multi Ita"),
            ..Default::default()
        },
        RankFilters { languages: legacy::tokens(&["ru"]), ..Default::default() },
    ];
    for (i, title) in titles.iter().enumerate() {
        let lower = str::to_lowercase(title);
        assert_eq!(junk_class(title), legacy::junk_class_of(&lower), "junk class of {title:?}");
        assert_eq!(release_years(title), legacy::release_years(title), "years of {title:?}");
        assert_eq!(
            crate::attributes::detect_audio_codec(&lower),
            legacy::detect_audio_codec(&lower),
            "audio codec of {title:?}"
        );
        assert_eq!(
            crate::rank::detect_resolution(title),
            legacy::detect_resolution_lower(&lower),
            "resolution of {title:?}"
        );
        assert_eq!(
            crate::label::binge_group(&lower, title),
            legacy::binge_group(&lower, title),
            "binge group of {title:?}"
        );
        // Unprobed, and one of the probes in turn: the probe only overrides fields, so every title need not meet
        // every probe.
        for j in [0, 1 + i % (probes.len() - 1)] {
            let mut s = stream(title, i + j);
            s.probe = probes[j].clone();
            assert_eq!(quality_score_ref(&s), legacy::quality_score_ref(&s), "quality of {title:?} ({j})");
            let want: StreamAttributes = legacy::stream_attributes_ref(&s);
            assert_eq!(stream_attributes_ref(&s), want, "attributes of {title:?} ({j})");
            assert_eq!(cost_attributes(&s), StreamAttributes { label: String::new(), ..want.clone() });
            assert_eq!(clean_label_traits(s.traits(), &s), want.label, "label of {title:?} ({j})");
            assert_eq!(binge_group_of(&s), legacy::binge_group(&lower, title));
            assert_eq!(crate::attributes::container_of_ref(&s), legacy::container_of_ref(&s));
            for c in &clients {
                assert_eq!(
                    c.cost_ref(&s, cost_attributes(&s)),
                    legacy::cost_ref(c, &s, want.clone()),
                    "cost of {title:?} ({j}) for {c:?}"
                );
            }
        }
        let t = ReleaseTraits::parse(title);
        assert_eq!(t.has(Pat::HdrOnly), legacy::hdr_only(&lower), "hdrOnly of {title:?}");
        for f in &filter_sets {
            assert_eq!(
                crate::rank::foreign_language_sink(&t, f),
                legacy::foreign_language_sink(&lower, f),
                "language sink of {title:?} for {:?}",
                f.languages
            );
        }
    }
}

/// Whole lists rank the same: order, scores, drop counts and the served resolutions, under the filter sets the
/// handler builds for a movie, an episode, a browser and the strict user filters.
#[test]
fn ranking_matches_the_regex_code() {
    let titles = corpus();
    let filter_sets = vec![
        RankFilters {
            exclude_cam: true,
            result_cap: 100,
            expected_year: Some(2024),
            min_seeders: Some(3),
            resolutions: vec!["2160p".into(), "1080p".into()],
            expected_title_tokens: title_tokens("Dune Part Two"),
            ..Default::default()
        },
        RankFilters {
            exclude_cam: true,
            result_cap: 50,
            prefer_resolution: "1080p".into(),
            episode: Some([1, 5]),
            show_tokens: title_tokens("Pokemon"),
            languages: legacy::tokens(&["ja", "en"]),
            ..Default::default()
        },
        RankFilters {
            result_cap: 1000,
            expected_title_tokens: title_tokens("Fin"),
            client: Some(clients()[1].clone()),
            languages: legacy::tokens(&["fi"]),
            ..Default::default()
        },
        RankFilters {
            result_cap: 1000,
            client: Some(clients()[2].clone()),
            hdr_only: true,
            cached_only: true,
            max_size_gb: Some(30),
            expected_year: Some(2001),
            ..Default::default()
        },
        RankFilters { result_cap: 5000, ..Default::default() },
    ];
    let mut compared = 0;
    for (c, chunk) in titles.chunks(997).enumerate() {
        let streams: Vec<RawStream> = chunk.iter().enumerate().map(|(i, t)| stream(t, c * 997 + i)).collect();
        for f in &filter_sets {
            let (want_order, want_debug) = legacy::rank_streams_debug(&streams, f);
            let mut got_debug = RankDebug::default();
            let got = rank_streams_debug(&streams, f.clone(), Some(&mut got_debug));
            let got_order: Vec<String> = got.iter().map(|s| s.info_hash.clone()).collect();
            assert_eq!(got_order, want_order, "order, chunk {c}");
            assert_eq!(got_debug, want_debug, "debug, chunk {c}");
            compared += got.len();
        }
    }
    assert!(compared > 10_000, "too few ranked releases compared: {compared}");
}
