//! Port of `internal/scout/reqlog.go`. Work package 6.
//!
//! The request log: one line per response when `LOG_REQUESTS` is on. The path is redacted (the first segment of a
//! per-install route is the credential, and a play token names the release being watched), and the request id is
//! cut down to what cannot forge a line.

/// Go `redactPath`: the path as it may be logged. Only per-install segments go; the route and the stream id stay.
/// The query string is never passed in, so it cannot leak. What is kept is escaped, so a path cannot write a newline
/// into the log.
pub(crate) fn redact_path(p: &str) -> String {
    todo!("reqlog.go redactPath")
}

/// Go `requestID`: the caller's `X-Request-Id` as it may be logged: only `[A-Za-z0-9_-]` survives, cut at 32.
pub(crate) fn request_id(v: &str) -> String {
    todo!("reqlog.go requestID")
}
