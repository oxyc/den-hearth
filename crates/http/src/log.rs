//! What the process says on stderr, and what it may never say.
//!
//! A request line is `<METHOD> <redacted path> <status> <ms>ms[ …][ rid=<id>]`. The `rid` is the caller's
//! `X-Request-Id`: it is what joins an addon's line to the app's own line for the same request, so its shape is
//! something ops grep for. The path is redacted before it gets here (the first segment of a per-install route is the
//! credential), the query string is never logged (`?s=` is a signature, `?resync=` a provider-token URL), and the
//! request id is cut down to what cannot forge a line.

use std::borrow::Cow;
use std::fmt::Write as _;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use http::HeaderMap;

/// The caller's `X-Request-Id` as it may be logged: only `[A-Za-z0-9_-]` survives, cut at 32 characters. It comes
/// off the network and is written verbatim, so nothing that could forge a line or smuggle a secret gets through.
/// Empty when nothing safe is left.
pub fn sanitize_request_id(raw: &str) -> String {
    raw.chars().filter(|c| c.is_ascii_alphanumeric() || *c == '-' || *c == '_').take(32).collect()
}

/// [`sanitize_request_id`] of the request's `X-Request-Id` header; `None` when there is none, or nothing safe in it.
pub fn request_id(headers: &HeaderMap) -> Option<String> {
    let id = sanitize_request_id(headers.get("x-request-id")?.to_str().ok()?);
    (!id.is_empty()).then_some(id)
}

/// The front of a request line: `<METHOD> <path> <status> <ms>ms`. `path` is already redacted.
pub fn request_head(method: &str, path: &str, status: u16, ms: u128) -> String {
    format!("{method} {path} {status} {ms}ms")
}

/// Append ` rid=<id>` when there is an id.
pub fn push_rid(line: &mut String, rid: Option<&str>) {
    if let Some(rid) = rid {
        let _ = write!(line, " rid={rid}");
    }
}

/// One request log line: `<METHOD> <redacted path> <status> <ms>ms[ rid=<id>]`.
pub fn request_line(method: &str, path: &str, status: u16, ms: u128, rid: Option<&str>) -> String {
    let mut line = request_head(method, path, status, ms);
    push_rid(&mut line, rid);
    line
}

/// The request path as a request log may show it, for an addon whose per-install routes put the credential in the
/// FIRST segment (`/<config>/manifest.json`, where `<config>` carries a BYOK key, sealed or not): every path whose
/// first segment is not one of `routes` has that segment written as `<config>`. That covers a client probing
/// `/<config>/configure` or pasting the bare segment, which would otherwise put the key in the log through a 404.
///
/// Leading slashes are skipped before the first segment is read, so `//<config>/manifest.json` is redacted at the
/// segment the router reads as the config, not at the empty one in front of it. The query is never passed in.
pub fn redact_first_segment<'a>(path: &'a str, routes: &[&str]) -> Cow<'a, str> {
    let rest = path.trim_start_matches('/');
    let (first, tail) = match rest.split_once('/') {
        Some((first, tail)) => (first, Some(tail)),
        None => (rest, None),
    };
    if first.is_empty() || routes.contains(&first) {
        return path.into();
    }
    match tail {
        Some(tail) => format!("/<config>/{tail}").into(),
        None => "/<config>".into(),
    }
}

/// How often one failure condition may write a line.
const LOG_EVERY: Duration = Duration::from_secs(60);

/// Writes a failure at most once per minute per condition, saying how many were held back since the last line.
/// The log is for state changes: in an outage every lookup and every download fails the same way, and one line a
/// minute says so as well as a thousand.
///
/// No timer. A held-back count is reported when the condition next occurs after its window, and a condition that
/// has stopped occurring has nothing left to say. Conditions are a closed set — an upstream and a status, a failure
/// reason — so the list stays a handful long; never key one on anything a caller picks.
///
/// One per service (a `static`), so two services' conditions never silence each other.
pub struct LimitedLog {
    seen: Mutex<Vec<(String, Instant, u32)>>,
}

impl LimitedLog {
    pub const fn new() -> LimitedLog {
        LimitedLog { seen: Mutex::new(Vec::new()) }
    }

    /// Write `line()` to stderr unless `condition` already wrote one inside the window. `line` is only built when
    /// it is written.
    pub fn log(&self, condition: &str, line: impl FnOnce() -> String) {
        // Written after the lock is released: stderr is a pipe someone else drains.
        match self.admit(condition, Instant::now()) {
            None => {}
            Some(0) => eprintln!("{}", line()),
            Some(n) => eprintln!("{} ({n} more like it since the last line)", line()),
        }
    }

    /// `None` when the line is held back; otherwise how many were held back since the last one written.
    fn admit(&self, condition: &str, now: Instant) -> Option<u32> {
        let mut seen = self.seen.lock().unwrap_or_else(|e| e.into_inner());
        match seen.iter_mut().find(|(c, _, _)| c == condition) {
            Some((_, at, held)) if now.duration_since(*at) < LOG_EVERY => {
                *held += 1;
                None
            }
            Some((_, at, held)) => {
                *at = now;
                Some(std::mem::take(held))
            }
            None => {
                seen.push((condition.to_string(), now, 0));
                Some(0)
            }
        }
    }
}

impl Default for LimitedLog {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rid(value: &str) -> Option<String> {
        let mut headers = HeaderMap::new();
        headers.insert("x-request-id", http::HeaderValue::from_str(value).unwrap());
        request_id(&headers)
    }

    #[test]
    fn a_hostile_request_id_is_stripped_and_truncated() {
        // Spaces and `=` would forge a field; only the safe characters survive, in order.
        assert_eq!(rid("ab cd=ef"), Some("abcdef".to_string()));
        assert_eq!(rid(&"x".repeat(100)), Some("x".repeat(32)));
        assert_eq!(rid("a1B2-c3_d4"), Some("a1B2-c3_d4".to_string()));
        assert_eq!(rid("!!!"), None, "nothing safe left means no id");
        assert_eq!(request_id(&HeaderMap::new()), None);
        assert_eq!(sanitize_request_id("x\ny rid=evil"), "xyridevil", "a newline cannot start a second line");
    }

    #[test]
    fn a_request_line_ends_with_the_callers_id() {
        assert_eq!(request_line("GET", "/health", 200, 3, None), "GET /health 200 3ms");
        assert_eq!(request_line("GET", "/health", 200, 3, Some("abc")), "GET /health 200 3ms rid=abc");
        // Extra fields a service adds sit between the head and the id.
        let mut line = request_head("POST", "/remux/session", 201, 12);
        push_rid(&mut line, Some("abc"));
        line.push_str(" session=AAAAAA");
        assert_eq!(line, "POST /remux/session 201 12ms rid=abc session=AAAAAA");
    }

    #[test]
    fn the_first_segment_of_an_unknown_route_is_the_credential() {
        let routes = ["", "health", "manifest.json", "play"];
        let r = |p| redact_first_segment(p, &routes).into_owned();
        assert_eq!(r("/"), "/");
        assert_eq!(r("/health"), "/health");
        assert_eq!(r("/play/abc.mp4"), "/play/abc.mp4");
        assert_eq!(r("/eyJrZXkiOiJzZWNyZXQifQ/manifest.json"), "/<config>/manifest.json");
        assert_eq!(r("/eyJrZXkiOiJzZWNyZXQifQ"), "/<config>", "a bare segment, probed with nothing after it");
        assert_eq!(r("/eyJrZXkiOiJzZWNyZXQifQ/configure"), "/<config>/configure");
        assert_eq!(
            r("//eyJrZXkiOiJzZWNyZXQifQ/manifest.json"),
            "/<config>/manifest.json",
            "extra leading slashes"
        );
        // No routes at all: every non-empty first segment goes.
        assert_eq!(redact_first_segment("/health/x", &[]), "/<config>/x");
    }

    #[test]
    fn a_condition_writes_once_a_minute_and_counts_what_it_held_back() {
        let log = LimitedLog::new();
        let t0 = Instant::now();
        assert_eq!(log.admit("tmdb 503", t0), Some(0));
        assert_eq!(log.admit("tmdb 503", t0 + Duration::from_secs(1)), None);
        assert_eq!(log.admit("tmdb 503", t0 + Duration::from_secs(59)), None);
        assert_eq!(log.admit("other", t0 + Duration::from_secs(2)), Some(0), "conditions are independent");
        assert_eq!(log.admit("tmdb 503", t0 + Duration::from_secs(60)), Some(2));
        assert_eq!(log.admit("tmdb 503", t0 + Duration::from_secs(61)), None);
        assert_eq!(log.admit("tmdb 503", t0 + Duration::from_secs(120)), Some(1));
    }
}
