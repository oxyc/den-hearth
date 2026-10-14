//! Port of `internal/scout/id.go`: parsing the Stremio stream id route segments.

/// FROZEN. Go `StreamID`: `tt<digits>` (movie) or `tt<digits>:S:E` (series episode).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamId {
    /// "movie" | "series"
    pub kind: String,
    pub imdb: String,
    pub season: i64,
    pub episode: i64,
    pub has_ep: bool,
}

/// Go `maxStreamIDLen`: bounds the whole id segment, checked BEFORE the split.
pub const MAX_STREAM_ID_LEN: usize = 64;

/// Go `parseStreamID`: the `<type>` + `<id>.json` route segments, or `None` (the route answers 400).
pub fn parse_stream_id(kind: &str, raw_id: &str) -> Option<StreamId> {
    todo!("id.go parseStreamID")
}
