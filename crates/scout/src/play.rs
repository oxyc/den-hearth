//! Port of `internal/scout/play.go`: what `/play` resolves, and the legacy play token.

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
    todo!("play.go infoHashRe")
}

/// Go `encodePlayToken`: base64url of `{"h":..,"f":..,"s":..,"e":..}` in that field order, with the
/// optional fields omitted when absent, so the token matches the TS encoder byte for byte.
pub fn encode_play_token(t: &PlayTarget) -> String {
    todo!("play.go encodePlayToken")
}

/// Go `decodePlayToken`: decodes and validates a token, or `None` (the route answers 400).
pub fn decode_play_token(tok: &str) -> Option<PlayTarget> {
    todo!("play.go decodePlayToken")
}

/// Go `nonNegInt`: the integer for a non-negative integral JSON number, else `None`.
pub fn non_neg_int(v: Option<f64>) -> Option<i64> {
    todo!("play.go nonNegInt")
}
