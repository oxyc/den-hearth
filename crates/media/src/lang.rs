//! Language tags, shared by Remux and Scout. The audio track a playback picks is den-core's `audio_pick`, which
//! both call (`den_remux::lang::pick_audio`, Scout's `managed_audio_choice`).
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
