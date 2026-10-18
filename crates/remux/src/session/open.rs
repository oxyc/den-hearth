//! Release identity, resolution, and probing.

use super::*;

/// How much bigger than the file opened a listed size has to be to be a pack's, not a rounding difference
/// (scout's `PACK_SIZE_RATIO`).
const PACK_SIZE_RATIO: f64 = 1.3;

/// Scout's `size_label`: `18 GB`, `720 MB`, one decimal under 10 GB.
pub(super) fn size_label(bytes: u64) -> String {
    const MIB: f64 = 1024.0 * 1024.0;
    let b = bytes as f64;
    if b >= 1024.0 * MIB {
        let gb = b / (1024.0 * MIB);
        return if gb >= 10.0 { format!("{} GB", (gb + 0.5) as u64) } else { format!("{gb:.1} GB") };
    }
    format!("{} MB", ((b / MIB) + 0.5).max(1.0) as u64)
}

/// A listed release's label and pack size once the file is open. Scout's list can carry the indexer's season-pack
/// size as the episode's (a title that names the episode, a pack's bytes); the label then names 105 GB for a 12 GB
/// file. Where the opened file is much smaller than the listed size, the label names the file and the listed size as
/// the pack it came from.
pub(crate) fn opened_label(
    label: &str,
    listed: Option<u64>,
    pack: Option<u64>,
    opened: Option<u64>,
) -> (String, Option<u64>) {
    let (Some(listed), Some(opened)) = (listed, opened) else { return (label.to_string(), pack) };
    if listed as f64 <= opened as f64 * PACK_SIZE_RATIO || pack.is_some() {
        return (label.to_string(), pack);
    }
    let (was, is) = (size_label(listed), size_label(opened));
    // A label that doesn't carry the listed size (another rounding, or none) gets the file's appended, so the pack
    // is never named without the size of what plays.
    let named = if label.contains(&was) { label.replacen(&was, &is, 1) } else { format!("{label} • {is}") };
    (format!("{named} · from a {was} pack"), Some(listed))
}

/// What an opened release is remembered under: the install that listed it, by its hash as a session's owner is,
/// and the release by its size and name. Not its play URL: scout issues a new ticket with every listing.
pub(crate) fn opened_key(base: &str, s: &scout::Stream) -> String {
    format!("{}/{}/{}", crate::auth::install_id(base), s.attributes.size_bytes.unwrap_or(0), s.filename())
}

/// A release's file by its size and the digest of its first `HASH_CHUNK` bytes (`state::probe_key`'s form): all of the
/// head a link remembered in `AppState::opened` still has, so a session opened from there names its file as one opened
/// afresh does. `None` without a size.
pub(super) fn file_key(r: &scout::Resolved) -> Option<String> {
    crate::state::probe_key(r.size, &r.head[..r.head.len().min(crate::subs::HASH_CHUNK as usize)])
}

/// What `AppState::known` remembers a release's probe under: the title and the release's name, whoever listed it.
pub(crate) fn known_key(title: &str, s: &scout::Stream) -> String {
    format!("{title}/{}", s.filename())
}

/// What `AppState::unplayable` remembers a release under: the title, the release's size and its name. Scout gives
/// no infohash or file index — its play URL is a ticket minted afresh with every listing — and its
/// `behaviorHints.filename` is what it names as a release's identity across listings. The size tells apart two files
/// under one name (a repack, one episode of a season pack from another), and the title keeps a pack's name for one
/// episode from standing for the next. `None` without a size: a name alone is too little to rule a release out by.
pub(crate) fn unplayable_key(title: &str, s: &scout::Stream) -> Option<String> {
    let size = s.attributes.size_bytes.filter(|n| *n > 0)?;
    Some(format!("{title}/{size}/{}", s.filename()))
}

/// Why opening a release failed. `lasting` when the file itself is why — no browser would play it — so the next
/// session skips it unopened; a timeout, a network error or a debrid's refusal never is, and neither is what depends
/// on the player (`create` decides that on an opened release, which `open` returns as `Ok`).
pub(crate) struct OpenFailure {
    pub why: String,
    pub lasting: bool,
}

impl OpenFailure {
    pub(super) fn passing(why: String) -> Self {
        OpenFailure { why, lasting: false }
    }
}

/// Whether a head that starts neither container is the file's own first bytes, and not an error page a debrid sent
/// in its place: as long as was asked for (or the whole file), and not text.
pub(super) fn the_files_own_head(r: &scout::Resolved) -> bool {
    let full = r.head.len() as u64 == scout::HEAD_BYTES.min(r.size.unwrap_or(u64::MAX));
    let text = r.head.iter().find(|b| !b.is_ascii_whitespace()).is_some_and(|b| matches!(b, b'<' | b'{'));
    full && !r.head.is_empty() && !text
}

/// Try one candidate: follow its play URL, read the head, and probe it — or take all of that from a recent
/// open of the same release by the same install. What the probe found is remembered for `releases`' verdicts.
pub(super) async fn open(
    st: &Arc<AppState>,
    src: &scout::ScoutSource,
    title: &str,
    s: &scout::Stream,
) -> Result<(scout::Resolved, MediaInfo), OpenFailure> {
    let key = opened_key(&src.base, s);
    if let Some(hit) = st.opened().get(&key, Instant::now()) {
        return Ok(hit);
    }
    // A ticket on scout's public name (d-play) is fetched at its LAN address, like the install it came from.
    let r = scout::resolve(
        st.scout_via(),
        &crate::config::local(&s.url, &st.cfg.origin_aliases),
        src,
        st.source_guard,
    )
    .await
    .map_err(OpenFailure::passing)?;
    let stable_key = crate::state::probe_key(r.size, &r.head);
    let cached = stable_key.as_deref().and_then(|key| st.probe(key));
    let from_cache = cached.is_some();
    let info = match cached {
        Some(info) => info,
        None => {
            let src = Source::Http { client: &r.client, url: &r.url, size: r.size };
            match crate::probe::probe(&src, &r.head).await {
                Ok(info) => info,
                Err(e) => {
                    // What the file is, not how the read went: an AVI without an index stays one.
                    let lasting = matches!(&e, crate::probe::ProbeError::Unsupported(w)
                        if w == crate::probe::NEITHER || w == crate::probe::avi::NO_INDEX);
                    return Err(OpenFailure {
                        why: e.to_string(),
                        lasting: lasting && the_files_own_head(&r),
                    });
                }
            }
        }
    };
    st.known().put(known_key(title, s), &info);
    // An MPEG-TS's or AVI's MPEG-2 or MPEG-4 Part 2 is decoded on the CPU for its transcode; nothing else decodes them.
    if let (VideoCodec::Other(c), false) = (&info.video, info.grid()) {
        return Err(OpenFailure { why: format!("video is {c}, which needs a re-encode"), lasting: true });
    }
    if info.audio.is_empty() {
        return Err(OpenFailure { why: "no audio track".into(), lasting: true });
    }
    if !from_cache {
        if let Some(key) = stable_key {
            st.remember_probe(key, &info);
        }
    }
    st.opened().put(key, &r, &info, Instant::now());
    Ok((r, info))
}
