//! Language tags and the audio track a playback picks, shared by Remux (which reads every track's flags) and Scout
//! (which picks for TorBox from a probe of the file head).
//!
//! Releases tag tracks every way there is: Matroska's ISO 639-2 (`eng`, `ger`), its newer BCP-47 (`en`,
//! `de-DE`), MP4's ISO 639-2/T (`deu`), and a browser asks in BCP-47 (`navigator.languages`). Everything
//! is compared as the ISO 639-1 code where there is one.

/// ISO 639-1, ISO 639-2/B, ISO 639-2/T, English name — the languages releases actually carry.
const LANGS: &[(&str, &str, &str, &str)] = &[
    ("en", "eng", "eng", "English"),
    ("fi", "fin", "fin", "Finnish"),
    ("sv", "swe", "swe", "Swedish"),
    ("no", "nor", "nor", "Norwegian"),
    ("nb", "nob", "nob", "Norwegian"),
    ("nn", "nno", "nno", "Norwegian"),
    ("da", "dan", "dan", "Danish"),
    ("is", "ice", "isl", "Icelandic"),
    ("de", "ger", "deu", "German"),
    ("fr", "fre", "fra", "French"),
    ("es", "spa", "spa", "Spanish"),
    ("it", "ita", "ita", "Italian"),
    ("pt", "por", "por", "Portuguese"),
    ("nl", "dut", "nld", "Dutch"),
    ("pl", "pol", "pol", "Polish"),
    ("cs", "cze", "ces", "Czech"),
    ("sk", "slo", "slk", "Slovak"),
    ("hu", "hun", "hun", "Hungarian"),
    ("ro", "rum", "ron", "Romanian"),
    ("bg", "bul", "bul", "Bulgarian"),
    ("hr", "hrv", "hrv", "Croatian"),
    ("sr", "srp", "srp", "Serbian"),
    ("sl", "slv", "slv", "Slovenian"),
    ("el", "gre", "ell", "Greek"),
    ("tr", "tur", "tur", "Turkish"),
    ("ru", "rus", "rus", "Russian"),
    ("uk", "ukr", "ukr", "Ukrainian"),
    ("et", "est", "est", "Estonian"),
    ("lv", "lav", "lav", "Latvian"),
    ("lt", "lit", "lit", "Lithuanian"),
    ("ar", "ara", "ara", "Arabic"),
    ("he", "heb", "heb", "Hebrew"),
    ("fa", "per", "fas", "Persian"),
    ("hi", "hin", "hin", "Hindi"),
    ("ta", "tam", "tam", "Tamil"),
    ("te", "tel", "tel", "Telugu"),
    ("ja", "jpn", "jpn", "Japanese"),
    ("ko", "kor", "kor", "Korean"),
    ("zh", "chi", "zho", "Chinese"),
    ("th", "tha", "tha", "Thai"),
    ("vi", "vie", "vie", "Vietnamese"),
    ("id", "ind", "ind", "Indonesian"),
    ("ms", "may", "msa", "Malay"),
];

fn entry(tag: &str) -> Option<&'static (&'static str, &'static str, &'static str, &'static str)> {
    let primary = tag.split(['-', '_']).next().unwrap_or("").to_ascii_lowercase();
    LANGS.iter().find(|(a, b, t, _)| primary == *a || primary == *b || primary == *t)
}

/// The tag as it is compared: ISO 639-1 where known (Bokmål and Nynorsk count as Norwegian), else its
/// primary subtag, lower-cased.
pub fn canonical(tag: &str) -> String {
    match entry(tag) {
        Some(("nb" | "nn", ..)) => "no".to_string(),
        Some((a, ..)) => a.to_string(),
        None => tag.split(['-', '_']).next().unwrap_or("").to_ascii_lowercase(),
    }
}

/// The English name of a known language tag, `None` for one the table lacks.
pub fn english_name(tag: &str) -> Option<&'static str> {
    entry(tag).map(|e| e.3)
}

/// Why `pick_audio` chose the track it did — for the one log line a session writes naming its audio
/// track, never for the decision itself.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AudioReason {
    /// The player named the track by index, from an earlier session's `audioTracks`.
    Explicit,
    /// It is in this preferred language (most wanted first in `Want::audio`), the best-ranked one a
    /// track is in.
    Preferred(String),
    /// No preference matched; it is the container's own default-flagged track — usually the release's
    /// original language.
    Default,
    /// No preference matched and nothing is flagged default: the first track, counting audio tracks
    /// only (or track 0, where every track is a commentary).
    First,
}

impl std::fmt::Display for AudioReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AudioReason::Explicit => write!(f, "named by the player"),
            AudioReason::Preferred(lang) => write!(f, "preferred language {lang}"),
            AudioReason::Default => write!(f, "original language"),
            AudioReason::First => write!(f, "first"),
        }
    }
}

/// What `pick_audio` reads of an audio track. A reader that does not know a flag reports it `false`.
pub trait AudioCandidate {
    fn language(&self) -> Option<&str>;
    /// Flagged default by the container.
    fn is_default(&self) -> bool;
    fn is_commentary(&self) -> bool;
}

/// The audio track to play, counting audio tracks only: the first of `prefs` (most wanted first) that a
/// track is in — among those, the one flagged default — and never a commentary. Without a match, the
/// first default track that is not a commentary, then any that is not.
pub fn pick_audio<T: AudioCandidate>(tracks: &[T], prefs: &[String]) -> (usize, AudioReason) {
    let usable = || tracks.iter().enumerate().filter(|(_, t)| !t.is_commentary());
    for p in prefs {
        let want = canonical(p);
        let best = usable()
            .filter(|(_, t)| t.language().map(canonical).as_deref() == Some(want.as_str()))
            .min_by_key(|(i, t)| (!t.is_default(), *i));
        if let Some((i, _)) = best {
            return (i, AudioReason::Preferred(want));
        }
    }
    match usable().min_by_key(|(i, t)| (!t.is_default(), *i)) {
        Some((i, t)) if t.is_default() => (i, AudioReason::Default),
        Some((i, _)) => (i, AudioReason::First),
        None => (0, AudioReason::First),
    }
}
