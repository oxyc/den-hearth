//! Port of `internal/scout/scrape.go`: the indexer scrapers (Stremio-addon `/stream` endpoints), the
//! shared parse, scrape quorum and the per-provider answer cache. `Doer`, `Scraper` and `ScrapeQuery` are
//! FROZEN (see PORTING.md): stores, probes and the handler all take them.

use async_trait::async_trait;

use crate::config::Indexer;
use crate::rank::RawStream;

/// What a `Doer` can fail with. Go returns `error`; a mock in a test returns whichever it likes.
pub type DoError = Box<dyn std::error::Error + Send + Sync>;

/// FROZEN. Go `doer`: the one HTTP seam, `Do(*http.Request) (*http.Response, error)`. A production impl
/// wraps a `reqwest::Client`; a test builds responses from `http::Response` (reqwest converts them), the
/// way the Go tests use `mockDoer`.
#[async_trait]
pub trait Doer: Send + Sync {
    async fn do_request(&self, req: reqwest::Request) -> Result<reqwest::Response, DoError>;
}

/// FROZEN. Go `scrapeQuery`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ScrapeQuery {
    /// "movie" | "series"
    pub kind: String,
    pub imdb: String,
    pub season: i64,
    pub episode: i64,
    pub has_ep: bool,
    /// The request's X-Request-Id, carried through for the per-indexer decision line. Not part of any
    /// cache key.
    pub rid: String,
}

/// FROZEN. Go `scraper`.
#[async_trait]
pub trait Scraper: Send + Sync {
    fn id(&self) -> Indexer;
    async fn scrape(&self, q: &ScrapeQuery) -> Result<Vec<RawStream>, DoError>;
}

/// Go `parseStremioStreams`: the streams in a Stremio `/stream` response, `source` naming the indexer
/// they came from.
pub(crate) fn parse_stremio_streams(_body: &[u8], _source: &str) -> Vec<RawStream> {
    todo!()
}

/// Go `defaultIndexerURLs`: the base URL each indexer is asked at unless the config overrides it.
pub(crate) const DEFAULT_INDEXER_URLS: [(&str, &str); 4] = [
    ("torrentio", "https://torrentio.strem.fun"),
    ("comet", "https://comet.elfhosted.com"),
    ("mediafusion", "https://mediafusion.elfhosted.com"),
    ("torz", "https://torz.strem.fun"),
];

/// Go `parseSize`: the size in bytes of a release described as "15.2 GB" or "700 MB", None when it says none.
pub(crate) fn parse_size(_s: &str) -> Option<i64> {
    todo!()
}

/// Go `parseSeeders`: the seeder count an indexer's description names, None when it names none.
pub(crate) fn parse_seeders(_s: &str) -> Option<i64> {
    todo!()
}

/// Go `stremioScraper`: one Stremio-addon indexer, reached over `base_url`.
pub(crate) struct StremioScraper {
    pub(crate) indexer: Indexer,
    pub(crate) base_url: String,
    pub(crate) client: std::sync::Arc<dyn Doer>,
}

#[async_trait]
impl Scraper for StremioScraper {
    fn id(&self) -> Indexer {
        self.indexer.clone()
    }
    async fn scrape(&self, _q: &ScrapeQuery) -> Result<Vec<RawStream>, DoError> {
        todo!()
    }
}

/// Go `scrapeAll`: asks every scraper at once and gathers what answered within `timeout`. Returns the
/// releases, whether any indexer answered with a non-empty list, and whether every indexer answered.
pub(crate) async fn scrape_all(
    _scrapers: &[std::sync::Arc<dyn Scraper>],
    _q: &ScrapeQuery,
    _timeout: std::time::Duration,
) -> (Vec<RawStream>, bool, bool) {
    todo!()
}
