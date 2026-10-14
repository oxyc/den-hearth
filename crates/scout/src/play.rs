//! Port of `internal/scout/play.go`: what `/play` resolves, and the legacy play token.

use crate::config::is_finite;
use crate::util::{b64url_decode, b64url_encode};

/// FROZEN. Go `PlayTarget`: an infohash plus either the exact file or series coordinates to pick from a
/// pack. The debrid token is NOT here; it rides in the config blob on the URL path.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PlayTarget {
    pub info_hash: String,
    pub file_idx: Option<i64>,
    pub season: Option<i64>,
    pub episode: Option<i64>,
    /// The indexer's size for the release, 0 when unknown. Only a ticket carries it.
    pub release_size: i64,
    /// Diagnostic only, never decoded from a wire format shared with another encoder.
    pub title: String,
    pub imdb: String,
}

/// Go `minInfoHashLen` / `maxInfoHashLen`: 32 for base32, 40 for hex, lower-cased before the match.
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
    #[serde(default)]
    h: String,
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
    let raw: PlayRaw = serde_json::from_slice(&data).ok()?;
    let h = raw.h.to_lowercase();
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

/// Go `nonNegInt`: the integer for a non-negative integral JSON number, else `None` (dropped, like the TS
/// guard).
pub fn non_neg_int(v: Option<f64>) -> Option<i64> {
    let v = v.filter(|v| is_finite(*v) && *v >= 0.0 && v.trunc() == *v)?;
    Some(v as i64)
}
