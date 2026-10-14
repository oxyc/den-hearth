//! Port of `internal/scout/linkcheck.go`. Work package 6.

use crate::scrape::Doer;

/// Go `linkVerdict`. `Unverified` is the zero value: the check could not reach a verdict.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LinkVerdict {
    /// The check could not reach a verdict — a timeout, a transport error, a throttled CDN. Says nothing
    /// about the link, so the link is served.
    Unverified,
    /// The link answered like a file: 200 or 206, not a web page, the right size where that is known.
    Playable,
    /// The link answered, and the answer was not the file.
    Broken,
    /// A movie link that serves a file under half the size of the release, where no store knew the file's own
    /// size to compare against. The release is very likely fine and the store picked another file out of it —
    /// TorBox taking the indexer's position in the torrent as its own file id — so /play re-resolves once
    /// with a file list before calling it dead.
    WrongFile,
}

/// Go `checkLink`: asks a freshly minted link for its first byte and judges the answer. `want_size` is the
/// file's size as a store listed it; `release_size` is the indexer's size for the whole release, consulted
/// only when `want_size` is unknown. The HTTP call goes through `Doer` (the Go `*http.Client`), and the
/// timeout is `linkCheckTimeout` inside this function.
pub async fn check_link(doer: &dyn Doer, link: &str, want_size: i64, release_size: i64) -> (LinkVerdict, String) {
    todo!("linkcheck.go checkLink")
}
