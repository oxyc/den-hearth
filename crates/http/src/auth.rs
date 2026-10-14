//! The `/metrics` bearer check every Den addon reads the same way.

use http::header::{HeaderMap, AUTHORIZATION};
use subtle::ConstantTimeEq;

/// Whether an `Authorization` header value presents `token`: the `Bearer ` prefix required (a bare token is
/// refused), the presented value trimmed, compared in constant time so a caller cannot recover the token a byte at
/// a time from how long a refusal took. An empty token disables the route rather than matching an empty header.
pub fn bearer_matches(authorization: &str, token: &str) -> bool {
    if token.is_empty() {
        return false;
    }
    let Some(presented) = authorization.strip_prefix("Bearer ") else { return false };
    presented.trim().as_bytes().ct_eq(token.as_bytes()).into()
}

/// May this request read `/metrics`? Only when a token is configured and the request presents it (see
/// [`bearer_matches`]). The caller answers a refusal with [`crate::not_found`], the same 404 an unknown path gets,
/// so nobody is told there is something here to poke at.
pub fn metrics_authorized(headers: &HeaderMap, token: Option<&str>) -> bool {
    let Some(token) = token else { return false };
    headers.get(AUTHORIZATION).and_then(|v| v.to_str().ok()).is_some_and(|v| bearer_matches(v, token))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn with(auth: Option<&str>) -> HeaderMap {
        let mut h = HeaderMap::new();
        if let Some(a) = auth {
            h.insert(AUTHORIZATION, a.parse().unwrap());
        }
        h
    }

    #[test]
    fn only_a_bearer_token_that_matches_is_let_in() {
        assert!(metrics_authorized(&with(Some("Bearer s3cret")), Some("s3cret")));
        assert!(metrics_authorized(&with(Some("Bearer  s3cret ")), Some("s3cret")), "the token is trimmed");
        assert!(!metrics_authorized(&with(Some("s3cret")), Some("s3cret")), "a bare token is refused");
        assert!(
            !metrics_authorized(&with(Some("bearer s3cret")), Some("s3cret")),
            "the scheme is case-sensitive"
        );
        assert!(!metrics_authorized(&with(Some("Bearer nope")), Some("s3cret")));
        assert!(!metrics_authorized(&with(None), Some("s3cret")));
    }

    #[test]
    fn no_token_configured_means_no_metrics() {
        assert!(!metrics_authorized(&with(Some("Bearer ")), None));
        assert!(!metrics_authorized(&with(Some("Bearer ")), Some("")), "an empty token matches nothing");
        assert!(!bearer_matches("Bearer ", ""));
    }
}
