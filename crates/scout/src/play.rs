//! Port of `internal/scout/play.go`: what `/play` resolves, and the legacy play token.

use crate::config::is_finite;
use crate::util::{b64url_decode, b64url_encode, fold_json_keys, go_to_lower, Shape, LEAF};

/// FROZEN. Go `PlayTarget`: an infohash plus either the exact file or series coordinates to pick from a
/// pack. The debrid token is NOT here; it rides in the config blob on the URL path.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PlayTarget {
    pub info_hash: String,
    pub file_idx: Option<i64>,
    pub season: Option<i64>,
    pub episode: Option<i64>,
    /// ReleaseSize is the size the indexer reported for this release, 0 when unknown. Only a ticket carries
    /// it — the legacy token's encoding is pinned — and /play uses it to notice a movie link that serves a
    /// file far smaller than the release (see verifyLink).
    pub release_size: i64,
    /// Title and IMDb are diagnostic only — never decoded from a wire format shared with another encoder,
    /// never used to decide anything a store does. Only a ticket carries them (ticket.go); the legacy
    /// token's encoding is pinned to the TS encoder and stays h/f/s/e. Empty on the legacy path, which a
    /// decision line simply omits (see decisionlog.go) rather than asserting an identity it does not have.
    pub title: String,
    pub imdb: String,
}

/// The only shape an infohash may take on the way in: 32 characters for base32, 40 for hex, lower-cased
/// before the match. Both ends are named because decodeListing filters the account listing on the same
/// range, and the two MUST agree — a listing key narrower than this range is one /play will still ask
/// about, and "not in the listing" is an authoritative miss and a 15 s marker against a torrent the
/// account holds. Built from the constants so the two cannot drift apart silently.
///
/// Not the only producer of a hash that reaches that lookup: scrape.go's hashNorm admits the same two
/// lengths on the probe path, which never passes through here. A test pins all three to 32 and 40 with
/// literals, because a fixture derived from these constants moves with them and asserts nothing.
pub const MIN_INFO_HASH_LEN: usize = 32;
pub const MAX_INFO_HASH_LEN: usize = 40;

/// Go `infoHashRe` (`^[a-z0-9]{32,40}$`) as a function.
pub fn is_info_hash(s: &str) -> bool {
    (MIN_INFO_HASH_LEN..=MAX_INFO_HASH_LEN).contains(&s.len())
        && s.bytes().all(|b| b.is_ascii_lowercase() || b.is_ascii_digit())
}

/// Go `playWire`: fixes field order (h,f,s,e) and omits absent fields, so the encoded token matches the TS
/// encoder byte for byte.
#[derive(serde::Serialize)]
struct PlayWire<'a> {
    h: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    f: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    s: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    e: Option<i64>,
}

/// Go `playRaw`: the token as decoded. Every number is read as a float and then checked by `non_neg_int`.
#[derive(serde::Deserialize)]
struct PlayRaw {
    /// Go leaves an absent or `null` string as "", which then fails the infohash check.
    h: Option<String>,
    f: Option<f64>,
    s: Option<f64>,
    e: Option<f64>,
}

/// Go `encodePlayToken`: base64url of `{"h":..,"f":..,"s":..,"e":..}` in that field order, with the
/// optional fields omitted when absent, so the token matches the TS encoder byte for byte.
pub fn encode_play_token(t: &PlayTarget) -> String {
    let wire = PlayWire { h: &t.info_hash, f: t.file_idx, s: t.season, e: t.episode };
    let b = serde_json::to_vec(&wire).expect("a struct of strings and integers always serializes");
    b64url_encode(&b)
}

/// Go `decodePlayToken`: decodes and validates a token, or `None` (the route answers 400).
/// Infohash must be 32–40 hex/base32.
pub fn decode_play_token(tok: &str) -> Option<PlayTarget> {
    let data = b64url_decode(tok).ok()?;
    // Go matches h/f/s/e case-insensitively, so a token spelled "H" still decodes.
    static SHAPE: Shape = Shape::Object(&[("h", &LEAF), ("f", &LEAF), ("s", &LEAF), ("e", &LEAF)]);
    let mut value: serde_json::Value = serde_json::from_slice(&data).ok()?;
    fold_json_keys(&mut value, &SHAPE);
    let raw: PlayRaw = serde_json::from_value(value).ok()?;
    let h = go_to_lower(&raw.h.unwrap_or_default());
    if !is_info_hash(&h) {
        return None;
    }
    Some(PlayTarget {
        info_hash: h,
        file_idx: non_neg_int(raw.f),
        season: non_neg_int(raw.s),
        episode: non_neg_int(raw.e),
        ..Default::default()
    })
}

/// Go `nonNegInt` returns *int for a non-negative integer JSON number, else nil (dropped, like the TS guard).
pub fn non_neg_int(v: Option<f64>) -> Option<i64> {
    let v = v.filter(|v| is_finite(*v) && *v >= 0.0 && v.trunc() == *v)?;
    Some(v as i64)
}
