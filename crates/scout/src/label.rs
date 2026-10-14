//! Port of `internal/scout/label.go`. Work package 5.

use std::sync::OnceLock;

use regex::Regex;

use crate::rank::GIB;

/// Go `mib` (label.go).
pub(crate) const MIB: i64 = 1_048_576;

/// Go `sizeLabel`: bytes → "18 GB" / "720 MB" (one decimal under 10 GB so small files stay legible).
pub(crate) fn size_label(bytes: i64) -> String {
    if bytes >= GIB {
        let gb = bytes as f64 / GIB as f64;
        if gb >= 10.0 {
            return format!("{} GB", (gb + 0.5) as i64);
        }
        return format!("{gb:.1} GB");
    }
    let mb = ((bytes as f64 / MIB as f64) + 0.5) as i64;
    format!("{} MB", mb.max(1))
}

/// Go `bingeGroup`: the release identity two episodes of one season share, or differ on.
pub(crate) fn binge_group(_lower: &str, _title: &str) -> String {
    todo!("label.go bingeGroup: needs rank.go's dolby-vision/HDR matchers and attributes.go's detectors")
}

/// Go `fileExt`: the extension a release name may end on, which is dropped before its group is read.
fn file_ext() -> &'static Regex {
    static FILE_EXT: OnceLock<Regex> = OnceLock::new();
    FILE_EXT.get_or_init(|| Regex::new(r"(?i)\.[a-z0-9]{2,4}$").expect("fileExt is a valid pattern"))
}

/// Go `groupMetadata`: a name that ends on one of these ends on metadata, not on a group ("…-Pilot-1080p.x265").
const GROUP_METADATA: &[&str] = &[
    "1080p", "2160p", "720p", "480p", "4k", "x264", "x265", "h264", "h265", "hevc", "avc", "av1", "webrip", "web-dl",
    "webdl", "bluray", "hdtv", "aac", "dts", "ddp", "eac3", "atmos",
];

/// Go `releaseGroup`: the scene group — the token after the final hyphen, before the extension — lowercased, or ""
/// when the name doesn't end on one. Mirrors the app's parser in StreamSelectionView.
pub(crate) fn release_group(title: &str) -> String {
    let name = title.split('\n').next().unwrap_or_default();
    let base = file_ext().replace(name.trim(), "").into_owned();
    // Go's LastIndex is -1 when there is no hyphen, so both "none" and "leading" fail the `dash <= 0` test.
    let dash = match base.rfind('-') {
        Some(d) if d > 0 => d,
        _ => return String::new(),
    };
    // A spaced " - " is a title separator ("Show - S01E01 - Pilot"), not a group.
    if base.as_bytes()[dash - 1] == b' ' {
        return String::new();
    }
    let tail = base[dash + 1..].trim();
    let lower = tail.to_lowercase();
    if tail.is_empty() || tail.len() > 14 || tail.contains(' ') || lower.contains("www") {
        return String::new();
    }
    let lower_base = base.to_lowercase();
    for m in GROUP_METADATA {
        // The suffix test catches a hyphenated token straddling the last dash: "…1080p WEB-DL" ends on metadata,
        // and its "group" would otherwise be "dl".
        if lower.contains(m) || lower_base.ends_with(m) {
            return String::new();
        }
    }
    lower
}

/// Go `cleanLabel`: the short "1080p • WEB" caption shown under a stream.
pub(crate) fn clean_label(_s: &crate::rank::RawStream) -> String {
    todo!()
}
