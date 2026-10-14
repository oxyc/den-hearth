//! A neighbour served by this same process, reached by calling it rather than over a socket.
//!
//! In den-hearth, Scout runs beside Remux and Subtitles, which used to reach it over HTTP at its LAN address.
//! `Local` is the list of base URLs at which the process answers a neighbour itself, each with the function that
//! serves a request there. A caller asks `get` with the URL it would have fetched: when the URL is under one of
//! the bases, the request is served in process and the answer comes back as the same status, headers and body;
//! otherwise `get` says so and the caller fetches over HTTP as before. Matching is exact on scheme, host and port
//! (the scheme's default port filled in) and on whole path segments, so nothing is taken to be local by accident.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use bytes::Bytes;
use http::header::HOST;
use http::{HeaderMap, HeaderValue, Request, Response, Uri};
use http_body_util::{BodyExt, Limited};

use crate::Body;

/// What a base's server returns for one request.
pub type LocalFuture = Pin<Box<dyn Future<Output = Response<Body>> + Send>>;

/// Serves one request at a base. It is given the path under the base (`/stream/movie/tt1.json` for
/// `https://scout.example/cfg/stream/movie/tt1.json` at base `https://scout.example/cfg`) with the query,
/// and the `Host` the URL named.
pub type Serve = Arc<dyn Fn(Request<Bytes>) -> LocalFuture + Send + Sync>;

#[derive(Clone)]
struct Base {
    /// As configured, for the startup line.
    raw: String,
    scheme: String,
    host: String,
    port: u16,
    /// The base's path without a trailing slash: "" for an origin.
    prefix: String,
    serve: Serve,
}

#[derive(Clone, Default)]
pub struct Local {
    bases: Vec<Base>,
}

/// Scheme, lower-cased host, port (the scheme's default filled in) and path of an http(s) URL. A URL carrying
/// credentials has none: `http://ok@evil/` is a request to `evil`.
fn parts(uri: &Uri) -> Option<(String, String, u16, &str)> {
    let scheme = uri.scheme_str()?.to_ascii_lowercase();
    let authority = uri.authority()?;
    if authority.as_str().contains('@') {
        return None;
    }
    let port = match (authority.port_u16(), scheme.as_str()) {
        (Some(port), "http" | "https") => port,
        (None, "http") => 80,
        (None, "https") => 443,
        _ => return None,
    };
    Some((scheme, authority.host().to_ascii_lowercase(), port, uri.path()))
}

impl Local {
    /// Answer every URL under `base` (an http(s) origin, optionally with a path) with `serve`. A base that is
    /// not one is refused with the reason.
    pub fn add(&mut self, base: &str, serve: Serve) -> Result<(), String> {
        let uri: Uri = base.trim().parse().map_err(|_| format!("{base:?} is not a URL"))?;
        if uri.query().is_some() {
            return Err(format!("{base:?} has a query"));
        }
        let (scheme, host, port, path) =
            parts(&uri).ok_or_else(|| format!("{base:?} is not a plain http(s) URL"))?;
        let prefix = path.trim_end_matches('/').to_string();
        self.bases.push(Base { raw: base.trim().to_string(), scheme, host, port, prefix, serve });
        Ok(())
    }

    /// The bases, as configured.
    pub fn bases(&self) -> Vec<&str> {
        self.bases.iter().map(|b| b.raw.as_str()).collect()
    }

    /// Is `url` served in process?
    pub fn serves(&self, url: &str) -> bool {
        self.find(url).is_some()
    }

    fn find(&self, url: &str) -> Option<(&Base, Uri)> {
        let uri: Uri = url.parse().ok()?;
        let (scheme, host, port, path) = parts(&uri)?;
        let base = self.bases.iter().find(|b| {
            b.scheme == scheme
                && b.host == host
                && b.port == port
                && path
                    .strip_prefix(b.prefix.as_str())
                    .is_some_and(|rest| rest.is_empty() || rest.starts_with('/'))
        })?;
        Some((base, uri))
    }

    /// A GET of `url` with `headers`, served in process, its body read up to `cap` bytes; `None` when `url` is
    /// not under any base, and the caller should fetch it over HTTP. The caller bounds the time it takes.
    pub fn get(
        &self,
        url: &str,
        headers: HeaderMap,
        cap: usize,
    ) -> Option<impl Future<Output = Result<Response<Bytes>, String>> + Send + 'static> {
        let (base, uri) = self.find(url)?;
        let rest = &uri.path()[base.prefix.len()..];
        let rest = if rest.is_empty() { "/" } else { rest };
        let path_and_query = match uri.query() {
            Some(query) => format!("{rest}?{query}"),
            None => rest.to_string(),
        };
        let authority = uri.authority().map(|a| a.as_str().to_string()).unwrap_or_default();
        let serve = base.serve.clone();
        Some(async move {
            let mut req = Request::get(path_and_query)
                .body(Bytes::new())
                .map_err(|_| "the URL's path is not a request path".to_string())?;
            *req.headers_mut() = headers;
            if let Ok(host) = HeaderValue::from_str(&authority) {
                req.headers_mut().insert(HOST, host);
            }
            let (parts, body) = serve(req).await.into_parts();
            let body = Limited::new(body, cap)
                .collect()
                .await
                .map_err(|_| format!("the in-process answer was unreadable or over {cap} bytes"))?
                .to_bytes();
            Ok(Response::from_parts(parts, body))
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A server that answers with the path and Host it was given.
    fn echo() -> Serve {
        Arc::new(|req: Request<Bytes>| {
            let text = format!(
                "{} {}",
                req.uri(),
                req.headers().get(HOST).and_then(|h| h.to_str().ok()).unwrap_or("")
            );
            Box::pin(async move { Response::new(crate::full(text)) })
        })
    }

    fn local(bases: &[&str]) -> Local {
        let mut local = Local::default();
        for base in bases {
            local.add(base, echo()).unwrap();
        }
        local
    }

    async fn answer(local: &Local, url: &str) -> Option<String> {
        let resp = local.get(url, HeaderMap::new(), 1 << 10)?.await.unwrap();
        Some(String::from_utf8(resp.into_body().to_vec()).unwrap())
    }

    #[tokio::test]
    async fn a_url_under_a_base_is_served_with_the_path_under_it() {
        let l =
            local(&["http://192.168.86.193:8080", "http://127.0.0.1:8090/scout/", "https://D-Play.example"]);
        assert_eq!(
            answer(&l, "http://192.168.86.193:8080/cfg/stream/movie/tt1.json").await.unwrap(),
            "/cfg/stream/movie/tt1.json 192.168.86.193:8080"
        );
        assert_eq!(
            answer(&l, "http://127.0.0.1:8090/scout/p/t?x=1").await.unwrap(),
            "/p/t?x=1 127.0.0.1:8090"
        );
        assert_eq!(answer(&l, "http://127.0.0.1:8090/scout").await.unwrap(), "/ 127.0.0.1:8090");
        assert_eq!(answer(&l, "https://d-play.example:443/p/t").await.unwrap(), "/p/t d-play.example:443");
        assert_eq!(answer(&l, "HTTPS://d-play.EXAMPLE/p/t").await.unwrap(), "/p/t d-play.EXAMPLE");
    }

    /// An IPv6 base matches an IPv6 URL, both written bracketed as a URL writes them.
    #[tokio::test]
    async fn an_ipv6_base_matches_its_own_address() {
        let l = local(&["http://[::1]:8080"]);
        assert_eq!(answer(&l, "http://[::1]:8080/p/t").await.unwrap(), "/p/t [::1]:8080");
        assert_eq!(
            answer(&l, "http://[::1]:8080/cfg/stream/movie/tt1.json").await.unwrap(),
            "/cfg/stream/movie/tt1.json [::1]:8080"
        );
    }

    /// Any other IPv6 address, or the same one on another port, is fetched over HTTP.
    #[test]
    fn another_ipv6_address_is_left_to_http() {
        let l = local(&["http://[::1]:8080"]);
        for url in [
            "http://[::2]:8080/p/t",
            "http://[::1]:8081/p/t",
            "http://[fd7a:115c:a1e0::1]:8080/p/t",
            "http://127.0.0.1:8080/p/t",
        ] {
            assert!(local_get_is_none(&l, url), "{url} was served in process");
        }
    }

    #[tokio::test]
    async fn anything_else_is_left_to_http() {
        let l = local(&["http://192.168.86.193:8080", "http://127.0.0.1:8090/scout"]);
        for url in [
            "http://192.168.86.193:8081/cfg",              // another port
            "https://192.168.86.193:8080/cfg",             // another scheme
            "http://192.168.86.194:8080/cfg",              // another host
            "http://192.168.86.193.evil:8080/cfg",         // a longer name
            "http://x@192.168.86.193:8080/cfg",            // credentials
            "http://192.168.86.193:8080@evil.example/cfg", // credentials disguising the host
            "http://127.0.0.1:8090/scouting/p/t",          // not a whole segment
            "http://127.0.0.1:8090/reel/play/x.mp4",       // another mount on the same origin
            "http://127.0.0.1:8090/",
            "ftp://192.168.86.193:8080/cfg",
            "not a url",
        ] {
            assert!(local_get_is_none(&l, url), "{url} was served in process");
        }
        assert!(Local::default().get("http://192.168.86.193:8080/cfg", HeaderMap::new(), 1).is_none());
    }

    fn local_get_is_none(l: &Local, url: &str) -> bool {
        !l.serves(url) && l.get(url, HeaderMap::new(), 1).is_none()
    }

    #[test]
    fn a_base_must_be_a_plain_http_url() {
        let mut l = Local::default();
        for bad in ["ftp://h", "http://u@h", "http://h/?q=1", "nope", ""] {
            assert!(l.add(bad, echo()).is_err(), "{bad:?}");
        }
        assert!(l.bases().is_empty());
    }

    #[tokio::test]
    async fn a_body_past_the_cap_is_an_error_not_a_truncation() {
        let l = local(&["http://h"]);
        let err = l.get("http://h/long/enough/path", HeaderMap::new(), 4).unwrap().await.unwrap_err();
        assert!(err.contains("over 4 bytes"), "{err}");
    }

    #[tokio::test]
    async fn the_callers_headers_reach_the_server() {
        let mut l = Local::default();
        l.add(
            "http://h",
            Arc::new(|req: Request<Bytes>| {
                let key = req.headers().get("x-den-remux-key").cloned();
                Box::pin(async move {
                    Response::new(crate::full(key.map(|k| k.as_bytes().to_vec()).unwrap_or_default()))
                })
            }),
        )
        .unwrap();
        let mut headers = HeaderMap::new();
        headers.insert("x-den-remux-key", HeaderValue::from_static("k"));
        let resp = l.get("http://h/x", headers, 16).unwrap().await.unwrap();
        assert_eq!(resp.into_body(), "k");
    }
}
