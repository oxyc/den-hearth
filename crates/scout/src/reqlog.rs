//! Port of `internal/scout/reqlog.go`. Work package 6.
//!
//! The request log: one line per response when `LOG_REQUESTS` is on. The path is redacted (the first segment of a
//! per-install route is the credential, and a play token names the release being watched), and the request id is
//! cut down to what cannot forge a line.

use crate::ticket::TICKET_ROUTE;

/// Go `redactPath`: the path as it may be logged. Only per-install segments go; the route and the stream id stay.
/// The query string is never passed in, so it cannot leak. What is kept is escaped, so a path cannot write a newline
/// into the log.
///
/// The first segment of any per-install route IS the credential — sealed or not, whoever holds it can spend the
/// account's quota — and a /play token names exactly which release a household is watching, so both are replaced.
/// A /p/<ticket> is both at once — a credential for one release — so the ticket goes as well.
///
/// Split exactly as the handler splits (empty segments dropped), so a `//<config>/…` path is redacted at the segment
/// the handler reads as the config, not the empty one before it.
pub(crate) fn redact_path(p: &str) -> String {
    match p {
        "/" | "/configure" | "/configure/" | "/health" | "/metrics" | "/manifest.json" | "/config-key"
        | "/validate" => {
            return p.to_owned();
        }
        _ => {}
    }
    let parts: Vec<&str> = p.split('/').filter(|s| !s.is_empty()).collect();
    if parts.is_empty() {
        return "/".to_owned();
    }
    let ticketed = parts.len() == 2 && parts[0] == TICKET_ROUTE;
    let out: Vec<String> = parts
        .iter()
        .enumerate()
        .map(|(i, seg)| match i {
            0 if ticketed => TICKET_ROUTE.to_owned(),
            1 if ticketed => "<ticket>".to_owned(),
            0 => "<config>".to_owned(),
            2 if parts[1] == "play" => "<token>".to_owned(),
            _ => path_segment_escape(seg),
        })
        .collect();
    format!("/{}", out.join("/"))
}

/// Go `url.PathEscape`: escapes one path segment. Unreserved characters and `$&+:=@` stay as they are, and every
/// other byte of the UTF-8 is written as an uppercase `%XX`.
fn path_segment_escape(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for b in s.bytes() {
        match b {
            b'A'..=b'Z'
            | b'a'..=b'z'
            | b'0'..=b'9'
            | b'-'
            | b'_'
            | b'.'
            | b'~'
            | b'$'
            | b'&'
            | b'+'
            | b':'
            | b'='
            | b'@' => out.push(char::from(b)),
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
}

/// Go `requestID`: the caller's `X-Request-Id` as it may be logged: only `[A-Za-z0-9_-]` survives, cut at 32.
/// It comes off the network, so nothing it carries can forge a line or smuggle a credential into the log.
pub(crate) use den_http::log::sanitize_request_id as request_id;
