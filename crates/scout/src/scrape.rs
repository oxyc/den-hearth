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
