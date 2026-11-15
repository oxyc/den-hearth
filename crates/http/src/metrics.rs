//! The fleet-wide `/metrics` HTTP contract.

use http::header::{HeaderMap, HeaderValue, ALLOW, CACHE_CONTROL, CONTENT_TYPE};
use http::{Method, Response, StatusCode};

use crate::{full, metrics_authorized, not_found, Body};

const PROMETHEUS: &str = "text/plain; version=0.0.4; charset=utf-8";

/// Answer an operational metrics route without letting its method handling drift between services.
///
/// Authorization is checked first so a disabled endpoint, missing token, or wrong token remains the
/// same concealed 404 for every method. GET alone renders the exposition. HEAD deliberately does not
/// call `render`: these bodies can be several KiB and a probe only needs the status and representation
/// headers. Every other method is a bounded 405 naming the two supported methods.
pub fn metrics_response(
    method: &Method,
    headers: &HeaderMap,
    token: Option<&str>,
    render: impl FnOnce() -> String,
) -> Response<Body> {
    if !metrics_authorized(headers, token) {
        return not_found();
    }

    match *method {
        Method::GET => response(full(render())),
        Method::HEAD => response(full("")),
        _ => {
            let mut response = crate::json(
                StatusCode::METHOD_NOT_ALLOWED,
                &serde_json::json!({"error": "method_not_allowed"}),
                &[],
            );
            response.headers_mut().insert(ALLOW, HeaderValue::from_static("GET, HEAD"));
            response
        }
    }
}

fn response(body: Body) -> Response<Body> {
    Response::builder()
        .status(StatusCode::OK)
        .header(CONTENT_TYPE, PROMETHEUS)
        .header(CACHE_CONTROL, "no-store")
        .body(body)
        .expect("static metrics response")
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use http::header::AUTHORIZATION;
    use http_body_util::BodyExt;

    use super::*;

    fn authorized() -> HeaderMap {
        let mut headers = HeaderMap::new();
        headers.insert(AUTHORIZATION, HeaderValue::from_static("Bearer secret"));
        headers
    }

    #[tokio::test]
    async fn get_renders_but_head_does_not() {
        let calls = AtomicUsize::new(0);
        let render = || {
            calls.fetch_add(1, Ordering::SeqCst);
            "sample 1\n".to_string()
        };
        let get = metrics_response(&Method::GET, &authorized(), Some("secret"), render);
        assert_eq!(get.status(), StatusCode::OK);
        assert_eq!(get.headers()[CONTENT_TYPE], PROMETHEUS);
        assert_eq!(get.headers()[CACHE_CONTROL], "no-store");
        assert_eq!(get.into_body().collect().await.unwrap().to_bytes(), "sample 1\n");
        assert_eq!(calls.load(Ordering::SeqCst), 1);

        let head = metrics_response(&Method::HEAD, &authorized(), Some("secret"), render);
        assert_eq!(head.status(), StatusCode::OK);
        assert_eq!(head.headers()[CONTENT_TYPE], PROMETHEUS);
        assert_eq!(head.headers()[CACHE_CONTROL], "no-store");
        assert!(head.into_body().collect().await.unwrap().to_bytes().is_empty());
        assert_eq!(calls.load(Ordering::SeqCst), 1, "HEAD rendered the exposition");
    }

    #[tokio::test]
    async fn authorization_is_concealed_before_the_method_contract() {
        for (headers, token) in [(HeaderMap::new(), Some("secret")), (authorized(), None)] {
            let response = metrics_response(&Method::POST, &headers, token, || panic!("must not render"));
            assert_eq!(response.status(), StatusCode::NOT_FOUND);
        }

        let response = metrics_response(&Method::POST, &authorized(), Some("secret"), || {
            panic!("an unsupported method must not render")
        });
        assert_eq!(response.status(), StatusCode::METHOD_NOT_ALLOWED);
        assert_eq!(response.headers()[ALLOW], "GET, HEAD");
        assert_eq!(response.headers()[CACHE_CONTROL], "no-store");
        assert_eq!(
            response.into_body().collect().await.unwrap().to_bytes(),
            r#"{"error":"method_not_allowed"}"#
        );
    }
}
