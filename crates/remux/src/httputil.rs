//! Remux's HTTP plumbing: the shared builders from `den-http`, the response shapes only the session routes need, and
//! a streamed body over files already opened on disk.

use bytes::Bytes;
use futures_util::{StreamExt, TryStreamExt};
use http_body_util::{BodyExt, StreamBody};
use hyper::body::Frame;
use hyper::{Response, StatusCode};
use tokio::io::AsyncReadExt;
use tokio_util::io::ReaderStream;

pub use den_http::{error, full, not_found, Body};

/// A satisfied single byte range. Callers still compare all three values with the range they requested and the
/// identity they already hold; parsing a plausible total is not validation.
pub fn content_range(value: &str) -> Option<(u64, u64, u64)> {
    let (span, total) = value.strip_prefix("bytes ")?.split_once('/')?;
    let (start, end) = span.split_once('-')?;
    let (start, end, total) = (start.parse().ok()?, end.parse().ok()?, total.parse().ok()?);
    (start <= end && end < total).then_some((start, end, total))
}

/// JSON with an explicit Content-Length and any extra headers. Every session answer is private to its caller, so
/// unlike the shared builder a success is `no-store` too unless the caller names a `Cache-Control` of its own.
pub fn json(status: StatusCode, value: &serde_json::Value, extra: &[(&str, &str)]) -> Response<Body> {
    if extra.iter().any(|(k, _)| k.eq_ignore_ascii_case("cache-control")) {
        return den_http::json(status, value, extra);
    }
    let mut extra = extra.to_vec();
    extra.push(("cache-control", "no-store"));
    den_http::json(status, value, &extra)
}

/// A text body of a given type, with the `Cache-Control` it may be kept under.
pub fn text(content_type: &str, cache_control: &str, body: String) -> Response<Body> {
    Response::builder()
        .status(StatusCode::OK)
        .header("content-type", content_type)
        .header("content-length", body.len())
        .header("cache-control", cache_control)
        .body(full(body))
        .unwrap()
}

/// One read buffer per file being streamed. Segments are a few MB; 64 KiB keeps the syscall count low
/// without holding much per response.
const STREAM_BUF: usize = 64 * 1024;

/// `len` random bytes, made as they go out a buffer at a time: nothing on the way can compress them, so the time they
/// take to arrive is the link's.
pub fn random_body(len: u64) -> Body {
    let stream = futures_util::stream::unfold(len, |left| async move {
        (left > 0).then(|| {
            let n = left.min(STREAM_BUF as u64);
            let mut chunk = vec![0u8; n as usize];
            getrandom::fill(&mut chunk).expect("the OS random source is unavailable");
            (Ok(Frame::data(Bytes::from(chunk))), left - n)
        })
    });
    BodyExt::boxed(StreamBody::new(stream))
}

/// Stream already-opened files back to back, `len` bytes of each from its current position. The
/// files are opened before the response starts, so deleting them from the scratch window while the
/// body is still going out cannot cut it short — an open descriptor keeps its inode.
pub fn files_body(parts: Vec<(std::fs::File, u64)>) -> Body {
    let stream = futures_util::stream::iter(parts)
        .map(|(f, len)| ReaderStream::with_capacity(tokio::fs::File::from_std(f).take(len), STREAM_BUF))
        .flatten()
        .map_ok(Frame::data);
    BodyExt::boxed(StreamBody::new(stream))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Every session answer is private to its caller: a 200 with no `Cache-Control` of its own is `no-store` and
    /// carries no validator, where the shared builder would send neither.
    #[test]
    fn a_session_answer_is_no_store_unless_it_says_otherwise() {
        let plain = json(StatusCode::OK, &serde_json::json!({"ok": true}), &[]);
        assert_eq!(plain.headers()["cache-control"], "no-store");
        assert!(plain.headers().get("etag").is_none());
        assert_eq!(plain.headers()["content-type"], "application/json");

        let busy = json(StatusCode::SERVICE_UNAVAILABLE, &serde_json::json!({}), &[("retry-after", "2")]);
        assert_eq!(busy.headers()["cache-control"], "no-store");
        assert_eq!(busy.headers()["retry-after"], "2");

        let own =
            json(StatusCode::SERVICE_UNAVAILABLE, &serde_json::json!({}), &[("cache-control", "no-store")]);
        assert_eq!(own.headers().get_all("cache-control").iter().count(), 1, "one directive, not two");

        assert_eq!(error(StatusCode::BAD_REQUEST, "bad_request", "x").headers()["cache-control"], "no-store");
        assert_eq!(not_found().headers()["cache-control"], "no-store");
    }
}
