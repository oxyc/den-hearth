//! Port of `internal/scout/linkcheck.go`. Work package 6.

use std::time::Duration;

use tokio::time::{timeout_at, Instant};

use crate::loglimit::log_limited;
use crate::scrape::Doer;
use crate::season::TorrentFile;
use crate::stores::refusal::transport_kind;
use crate::stores::realdebrid::rd_torrent_key;
use crate::stores::{short_hash, ResolveTarget};
use crate::util::go_url_parse;

/// How long /play waits on the check of a link it just minted. One ranged GET for a single byte; a CDN
/// that cannot answer that in three seconds is slow, not proof of anything, and the link goes out anyway.
pub const LINK_CHECK_TIMEOUT: Duration = Duration::from_secs(3);

/// How far the served size may drift from the size the debrid listed before the link is called wrong.
/// Real files match to the byte; the slack is for a service that rounds, not for a different file.
pub const LINK_SIZE_TOLERANCE: f64 = 0.02;

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

/// Go `checkLink` asks a freshly minted link for its first byte and judges the answer. `want_size` is the
/// file's size as a store listed it; `release_size` is the indexer's size for the whole release, consulted
/// only when `want_size` is unknown. The HTTP call goes through `Doer` (the Go `*http.Client`), and the
/// timeout is `linkCheckTimeout` inside this function.
///
/// A debrid hands out links it cannot serve more often than it should: an error page at a CDN URL, a JSON
/// error body with a 200, a link to a different file in the pack. The player finds out only after it has
/// buffered, shown a spinner, and failed — and the client then cannot tell a bad link from a bad release.
/// One ranged byte finds out before the 302 goes out, and a link that fails gets the same 404 dead_link a
/// release that could not be resolved gets.
///
/// It must never condemn a link for its OWN failure. A timeout or a refused connection is the check not
/// getting an answer, and treating that as a dead link would turn a slow CDN into a blacklisted release —
/// the exact confusion StoreUnavailableError exists to keep out of this route. The same goes for 429 and
/// 5xx: the CDN declining to answer right now, not saying the link is wrong.
///
/// `want_size` is the file's size as a store listed it. `release_size` is the indexer's size for the whole
/// release, consulted only when `want_size` is unknown, and only as a floor: a served total under half of it is
/// `WrongFile`. Half, because a collection's film is legitimately smaller than the pack an indexer may
/// report, while the failure this exists for is a sample or an .nfo — kilobytes against gigabytes.
pub async fn check_link(doer: &dyn Doer, link: &str, want_size: i64, release_size: i64) -> (LinkVerdict, String) {
    let deadline = Instant::now() + LINK_CHECK_TIMEOUT;
    // Go's `http.NewRequest` refuses only what `url.Parse` refuses; that is "not a usable URL". The WHATWG parser
    // behind `reqwest::Url` disagrees both ways (it takes a control byte and a bad escape, and refuses a relative
    // reference), so Go's verdict on the text is asked first. A link Go takes but this parser cannot reaches
    // `client.Do` in Go and fails THERE, which is the check not getting an answer: "its own failure does not condemn
    // the link".
    let Some(go_url) = go_url_parse(link) else {
        return (LinkVerdict::Broken, "not a usable URL".to_owned());
    };
    let Ok(url) = reqwest::Url::parse(link) else {
        return if go_url.scheme == "http" || go_url.scheme == "https" {
            (LinkVerdict::Unverified, "invalid address".to_owned())
        } else {
            (LinkVerdict::Unverified, format!("unsupported protocol scheme {:?}", go_url.scheme))
        };
    };
    let mut req = reqwest::Request::new(reqwest::Method::GET, url);
    req.headers_mut().insert("range", reqwest::header::HeaderValue::from_static("bytes=0-0"));
    let mut resp = match timeout_at(deadline, doer.do_request(req)).await {
        // transport_kind, never the error itself: its message carries the URL, and a playback link is a
        // credential.
        Ok(Err(e)) => return (LinkVerdict::Unverified, transport_kind(Some(&*e))),
        Err(_) => return (LinkVerdict::Unverified, "timeout".to_owned()),
        Ok(Ok(resp)) => resp,
    };
    let verdict = judge(&resp, want_size, release_size);
    // Bounded, like the probe's drain: a server that ignores the Range answers 200 with the whole file.
    let _ = timeout_at(deadline, async {
        let mut drained = 0usize;
        while drained < 1 << 10 {
            match resp.chunk().await {
                Ok(Some(chunk)) => drained += chunk.len(),
                _ => break,
            }
        }
    })
    .await;
    verdict
}

/// The part of `checkLink` that reads the answer.
fn judge(resp: &reqwest::Response, want_size: i64, release_size: i64) -> (LinkVerdict, String) {
    let status = resp.status().as_u16();
    match status {
        200 | 206 => {}
        429 | 500.. => return (LinkVerdict::Unverified, format!("http {status}")),
        _ => return (LinkVerdict::Broken, format!("http {status}")),
    }
    // An error page served with a success status. No video is served as either of these.
    let header = |name: &str| resp.headers().get(name).and_then(|v| v.to_str().ok()).unwrap_or_default();
    let mt = header("content-type").split(';').next().unwrap_or_default().trim().to_ascii_lowercase();
    if mt == "text/html" || mt == "application/json" {
        return (LinkVerdict::Broken, format!("served {mt}"));
    }
    if want_size > 0 {
        if let Some(total) = content_range_total(header("content-range")) {
            if ((total - want_size) as f64).abs() > LINK_SIZE_TOLERANCE * want_size as f64 {
                return (LinkVerdict::Broken, format!("serves {total} bytes, the debrid listed {want_size}"));
            }
        }
    } else if release_size > 0 {
        if let Some(total) = content_range_total(header("content-range")) {
            if total * 2 < release_size {
                return (LinkVerdict::WrongFile, format!("serves {total} bytes of a {release_size}-byte release"));
            }
        }
    }
    (LinkVerdict::Playable, String::new())
}

/// Go `contentRangeTotal` reads the complete length out of `bytes 0-0/12345`. A `*` total is unknown.
pub(crate) fn content_range_total(v: &str) -> Option<i64> {
    let (_, total) = v.split_once('/')?;
    total.trim().parse::<i64>().ok().filter(|n| *n > 0)
}

/// Go `(*handler).verifyLink` runs `check_link` for /play with whatever the stores already know about the file's size.
///
/// No probe client wired means no check, the same opt-in the probe fan-out uses: a caller that has not
/// provided one (a test, an embedder) gets the old behaviour exactly.
///
/// The release size is passed for a movie only. An episode's file is one of many in a pack, so a size for
/// the whole release says nothing about it — and a pack's episode always has its size known from the list
/// it was picked from anyway.
///
/// `known_size` is Go's `pool.KnownFileSize(rt)`, which the caller asks of its `StorePool`: the pool and its
/// stores are another module's, so the answer arrives here already looked up.
pub async fn verify_link(
    probe_client: Option<&dyn Doer>,
    known_size: Option<i64>,
    rt: &ResolveTarget,
    link: &str,
) -> LinkVerdict {
    let Some(client) = probe_client else { return LinkVerdict::Unverified };
    let size = known_size.unwrap_or(0);
    let release = if rt.season.is_none() && rt.episode.is_none() { rt.release_size } else { 0 };
    let (verdict, reason) = check_link(client, link, size, release).await;
    match verdict {
        LinkVerdict::Broken => log_limited(
            "play-link-rejected",
            &format!("play {}: the minted link failed its check ({reason})", short_hash(&rt.info_hash)),
        ),
        LinkVerdict::WrongFile => log_limited(
            "play-link-wrong-file",
            &format!("play {}: the minted link names another file in the release ({reason})", short_hash(&rt.info_hash)),
        ),
        LinkVerdict::Unverified => log_limited(
            "play-link-unverified",
            &format!("play {}: could not check the minted link ({reason}), serving it anyway", short_hash(&rt.info_hash)),
        ),
        LinkVerdict::Playable => {}
    }
    verdict
}

/// Go `rdSizeKey` is the byte size RD listed for the file a remembered torrent serves. Same scope as the id.
pub(crate) fn rd_size_key(token: &str, info_hash: &str, t: &ResolveTarget) -> String {
    format!("{}:bytes", rd_torrent_key(token, info_hash, t))
}

/// Go `sizeOf`: the listed size of file `id`, when the list names one.
pub(crate) fn size_of(files: &[TorrentFile], id: i64) -> Option<i64> {
    files.iter().find(|f| f.index == id && f.size_bytes.is_some_and(|n| n > 0)).and_then(|f| f.size_bytes)
}
