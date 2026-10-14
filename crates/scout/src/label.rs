//! Port of `internal/scout/label.go`. Work package 5.

use std::sync::{LazyLock, OnceLock};

use regex::Regex;

use crate::attributes::detect_source_attr;
use crate::rank::{
    detect_resolution_lower, re2, RawStream, GIB, RES_1080, RES_1440, RES_2160, RES_480, RES_4K_UHD, RES_540,
    RES_576, RES_720, RE_ATMOS, RE_BLURAY, RE_BR_RIP, RE_DOVI, RE_HDR10P, RE_REMUX, RE_WEB, RE_WEB_DL,
    RE_WEB_RIP,
};
use crate::util::go_to_lower;

/// Go `mib` (label.go).
pub(crate) const MIB: i64 = 1_048_576;

/// Go `reHDRLabel`.
static RE_HDR_LABEL: LazyLock<Regex> = LazyLock::new(|| re2(r"\bhdr\b|hdr10|\bhlg\b"));

/// Go `resolutionLabel`.
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

/// Go `dynamicRangeLabel`.
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

/// Go `sourceLabelForDisplay`.
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

/// Go `bingeGroup` names the release a stream belongs to, for next-episode continuity: a client plays the next
/// episode from the source whose bingeGroup matches the one it just played. It used to be
/// `den-scout-<imdb>` — the same for every release of a title — so the first candidate always matched and
/// "stay on the same release" never happened. Resolution, source, dynamic range and release group are
/// what make two episodes "the same release".
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

/// Go `fileExt`: the extension a release name may end on, which is dropped before its group is read.
fn file_ext() -> &'static Regex {
    static FILE_EXT: OnceLock<Regex> = OnceLock::new();
    FILE_EXT.get_or_init(|| Regex::new(r"(?i)\.[a-z0-9]{2,4}$").expect("fileExt is a valid pattern"))
}

/// Go `groupMetadata`: a name that ends on one of these ends on metadata, not on a group ("…-Pilot-1080p.x265").
const GROUP_METADATA: &[&str] = &[
    "1080p", "2160p", "720p", "480p", "4k", "x264", "x265", "h264", "h265", "hevc", "avc", "av1", "webrip",
    "web-dl", "webdl", "bluray", "hdtv", "aac", "dts", "ddp", "eac3", "atmos",
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
    let lower = go_to_lower(tail);
    if tail.is_empty() || tail.len() > 14 || tail.contains(' ') || lower.contains("www") {
        return String::new();
    }
    let lower_base = go_to_lower(&base);
    for m in GROUP_METADATA {
        // The suffix test catches a hyphenated token straddling the last dash: "…1080p WEB-DL" ends on metadata,
        // and its "group" would otherwise be "dl".
        if lower.contains(m) || lower_base.ends_with(m) {
            return String::new();
        }
    }
    lower
}

/// Go `cleanLabel` is the bullet-joined quality summary shown as attributes.label.
pub(crate) fn clean_label(s: &RawStream) -> String {
    clean_label_lower(&go_to_lower(&s.title), s)
}

/// Go `cleanLabelLower` is `clean_label` when the caller already has the lowercased title.
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
        // The episode's own size is unknown — unresolvedSeasonPack (season.go) left SizeBytes nil rather
        // than keep the indexer's pack-wide number as if it were this file's. State what the number
        // actually is instead of staying silent or, worse, naming it plainly as this release's size.
        parts.push(format!("{} pack", size_label(pack)));
    }
    let mut label = if parts.is_empty() { "Stream".to_owned() } else { parts.join(" • ") };
    // The pack is context, not a correction — this stream still plays the file sized above. Appended after
    // the bullets rather than folded into them, so a client that wants only the short form can cut the
    // string at " · " instead of re-parsing bullets. Only when SizeBytes is ITSELF known: an unknown
    // episode size already named the pack as the headline bullet above, and repeating it here is noise.
    if let (Some(_), Some(pack)) = (s.size_bytes, s.pack_size_bytes) {
        label.push_str(" · from a ");
        label.push_str(&size_label(pack));
        label.push_str(" pack");
    }
    label
}
