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

/// Go `maxStreamIDLen` bounds the whole id segment. The longest real one is `tt1234567890:999:999.json`, at 25
/// characters.
///
/// Checked BEFORE the split, and that order is the point: Split allocates one string per part whatever
/// the regex below would later say, so an id of many colons is a slice of that many strings, paid before
/// the id ever reaches imdbRe to be rejected. Do not move this below the Split, and do not delete it as
/// redundant with `\d{1,10}` — the regex only sees parts[0]. TestParseStreamID has a case that ONLY this
/// check can refuse, so that neither of those edits can pass quietly.
pub const MAX_STREAM_ID_LEN: usize = 64;

/// Go `imdbRe` (`^tt\d{1,10}$`): `tt` and one to ten ASCII digits.
///
/// A real IMDb id is `tt` plus seven or eight digits; ten leaves room for a couple of decades of growth.
///
/// The bound matters because `\d+` did not have one. This segment is retained for the whole build — it
/// goes into the cache key and into the rebuild gate's map key — and it is also concatenated into the
/// outbound indexer URL. Measured on a server with net/http's DEFAULT 1 MiB header allowance: `tt`
/// followed by 900,000 digits was accepted, pinned 2.7 MiB per in-flight request (150 concurrent parked
/// at the scrape reached 408 MiB against a 230 MiB GOMEMLIMIT), and turned one small inbound GET into a
/// ~900 KB outbound URL per configured indexer — which is the "ban risk" hostlimit.go was written to
/// prevent, paced by the limiter but not sized by it.
///
/// main.go now sets MaxHeaderBytes, so the request line cannot carry an id anywhere near that size and
/// those figures are no longer reachable over HTTP. This stays as the second of the two bounds: the
/// transport limit is one number in one file, and this is the one that holds if it is ever raised or if a
/// caller reaches parseStreamID by some other route.
fn is_imdb_id(s: &str) -> bool {
    s.strip_prefix("tt").is_some_and(|d| (1..=10).contains(&d.len()) && d.bytes().all(|b| b.is_ascii_digit()))
}

/// Go `parseStreamID`: the `<type>` + `<id>.json` route segments, or `None` (the route answers 400).
pub fn parse_stream_id(kind: &str, raw_id: &str) -> Option<StreamId> {
    if kind != "movie" && kind != "series" {
        return None;
    }
    if raw_id.len() > MAX_STREAM_ID_LEN {
        return None;
    }
    let id = raw_id.strip_suffix(".json").unwrap_or(raw_id);
    let parts: Vec<&str> = id.split(':').collect();
    let imdb = parts[0];
    if !is_imdb_id(imdb) {
        return None;
    }
    if kind == "series" {
        if parts.len() < 3 {
            return None;
        }
        // Go's strconv.Atoi, like `str::parse`, accepts a leading sign; a negative number is refused below.
        let season: i64 = parts[1].parse().ok()?;
        let episode: i64 = parts[2].parse().ok()?;
        if season < 0 || episode < 0 {
            return None;
        }
        return Some(StreamId {
            kind: kind.to_owned(),
            imdb: imdb.to_owned(),
            season,
            episode,
            has_ep: true,
        });
    }
    Some(StreamId { kind: kind.to_owned(), imdb: imdb.to_owned(), season: 0, episode: 0, has_ep: false })
}
