//! Go: `internal/scout/mock_test.go` (18 lines).
//!
//! Not a test: the shared helpers (`mockDoer`, `resp`) every other test module uses. A `Doer` that
//! routes requests to a closure, and a `reqwest::Response` built from a status and a body.

use async_trait::async_trait;

use crate::scrape::{DoError, Doer};

/// mockDoer routes requests to a test function.
pub(crate) struct MockDoer<F>(pub(crate) F)
where
    F: Fn(reqwest::Request) -> Result<reqwest::Response, DoError> + Send + Sync;

#[async_trait]
impl<F> Doer for MockDoer<F>
where
    F: Fn(reqwest::Request) -> Result<reqwest::Response, DoError> + Send + Sync,
{
    async fn do_request(&self, req: reqwest::Request) -> Result<reqwest::Response, DoError> {
        (self.0)(req)
    }
}

/// Go `httptest.NewServer(http.ServeFile(...))`: a loopback HTTP server answering every request with `body`,
/// honouring a single `Range: bytes=a-b` as `http.ServeFile` does. Returns its base URL. The server runs on the
/// calling test's runtime, so it makes progress while the test awaits.
pub(crate) async fn loopback_file(body: Vec<u8>) -> String {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.expect("bind a loopback port");
    let base = format!("http://{}", listener.local_addr().expect("a local address"));
    let body = std::sync::Arc::new(body);
    tokio::spawn(async move {
        loop {
            let Ok((mut sock, _)) = listener.accept().await else { return };
            let body = body.clone();
            tokio::spawn(async move {
                let mut head = Vec::new();
                let mut buf = [0u8; 1024];
                while !head.windows(4).any(|w| w == b"\r\n\r\n") {
                    match sock.read(&mut buf).await {
                        Ok(0) | Err(_) => return,
                        Ok(n) => head.extend_from_slice(&buf[..n]),
                    }
                }
                let text = String::from_utf8_lossy(&head).to_ascii_lowercase();
                let range = text.lines().find_map(|l| l.strip_prefix("range: bytes=")).and_then(|r| {
                    let (a, b) = r.trim().split_once('-')?;
                    Some((a.parse::<usize>().ok()?, b.parse::<usize>().ok()))
                });
                let total = body.len();
                let (status, slice, extra) = match range {
                    Some((a, b)) if a < total => {
                        let end = b.map_or(total, |b| (b + 1).min(total));
                        (
                            "206 Partial Content",
                            &body[a..end],
                            format!("Content-Range: bytes {a}-{}/{total}\r\n", end - 1),
                        )
                    }
                    _ => ("200 OK", &body[..], String::new()),
                };
                let head = format!(
                    "HTTP/1.1 {status}\r\n{extra}Content-Length: {}\r\nContent-Type: application/octet-stream\r\nConnection: close\r\n\r\n",
                    slice.len()
                );
                let _ = sock.write_all(head.as_bytes()).await;
                let _ = sock.write_all(slice).await;
                let _ = sock.shutdown().await;
            });
        }
    });
    base
}

pub(crate) fn resp(status: u16, body: &str) -> reqwest::Response {
    let r = http::Response::builder().status(status).body(body.to_owned()).expect("valid status");
    reqwest::Response::from(r)
}
