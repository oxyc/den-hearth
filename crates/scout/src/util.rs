//! Port of `internal/scout/util.go`: ETags, cache-key digests and lenient base64url.

use base64::alphabet::URL_SAFE;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::engine::{DecodePaddingMode, GeneralPurpose, GeneralPurposeConfig};
use base64::Engine as _;
use sha2::{Digest, Sha256};

/// Go `etagHex`: 16 hex digits of FNV-1a-64 of `s`.
///
/// 64 bits, because the ETag is the only thing standing between a revalidating client and a 304 for a body
/// it does not hold; at 32 bits a collision between two versions of a list is within reach of an ordinary
/// cache's lifetime.
pub fn etag_hex(s: &str) -> String {
    const OFFSET_BASIS: u64 = 0xcbf2_9ce4_8422_2325;
    const PRIME: u64 = 0x0000_0100_0000_01b3;
    let h = s.bytes().fold(OFFSET_BASIS, |h, b| (h ^ u64::from(b)).wrapping_mul(PRIME));
    format!("{h:016x}")
}

/// Go `keyHash`: 128 bits of SHA-256 as hex, the collision-resistant digest for cache keys.
///
/// The key gates cross-config data, so a 32-bit FNV was unsafe (audit #7). 128 bits of SHA-256 is ample.
pub fn key_hash(s: &str) -> String {
    Sha256::digest(s.as_bytes())[..16].iter().map(|b| format!("{b:02x}")).collect()
}

/// Go `b64urlDecode`: base64url, padded or not (lenient, like the TS decoder).
///
/// Trailing `=` are trimmed, carriage returns and newlines are skipped anywhere, and non-zero trailing bits
/// are accepted, as Go's `RawURLEncoding` does.
pub fn b64url_decode(s: &str) -> Result<Vec<u8>, String> {
    const LENIENT: GeneralPurpose = GeneralPurpose::new(
        &URL_SAFE,
        GeneralPurposeConfig::new()
            .with_encode_padding(false)
            .with_decode_allow_trailing_bits(true)
            .with_decode_padding_mode(DecodePaddingMode::RequireNone),
    );
    let cleaned: String = s.trim_end_matches('=').chars().filter(|c| !matches!(c, '\r' | '\n')).collect();
    LENIENT.decode(cleaned).map_err(|e| e.to_string())
}

/// Go `b64urlEncode`: unpadded base64url.
pub fn b64url_encode(b: &[u8]) -> String {
    URL_SAFE_NO_PAD.encode(b)
}

/// Go `strings.ToLower`: maps each character through its SIMPLE lower-case form.
///
/// Rust's `str::to_lowercase` uses the full mapping (U+0130 becomes two characters) and a context rule for the
/// final sigma, so it can disagree with Go on the same input. A value lower-cased here is then matched against an
/// ASCII-only pattern (an infohash), where "İ" must become "i" as it does in Go.
pub(crate) fn go_to_lower(s: &str) -> String {
    s.chars().map(|c| c.to_lowercase().next().unwrap_or(c)).collect()
}

/// The shape of a JSON document as far as [`fold_json_keys`] needs it: which keys name fields, and what each holds.
pub(crate) enum Shape {
    /// A value with no field names inside it that matter.
    Leaf,
    /// An array whose elements all have the one shape.
    Array(&'static Shape),
    /// An object, with the exact spelling of each field it reads.
    Object(&'static [(&'static str, &'static Shape)]),
}

/// A leaf, for `Shape::Object` tables to point at.
pub(crate) static LEAF: Shape = Shape::Leaf;

/// Go's `encoding/json` matches an object key to a struct field case-insensitively, after trying the exact
/// spelling. serde matches exactly. This rewrites every key `shape` names to the field's exact spelling, so a
/// document Go would have read as `{"DEBRID": …}` decodes here as well. Keys the shape does not name are left alone
/// (serde ignores them, as Go does).
///
/// Folding is Go's simple case folding, which for the ASCII field names used here adds two characters to the
/// ASCII pairs: U+017F (long s) folds with "s" and U+212A (Kelvin sign) with "k".
///
/// One divergence: the keys of a `serde_json::Map` are ordered, not in document order, so when one object holds
/// two spellings of the same field the winner is the one that sorts last (the exact spelling when there is one),
/// where Go keeps the later one in the document. A config that does that was not written by any encoder.
pub(crate) fn fold_json_keys(v: &mut serde_json::Value, shape: &Shape) {
    match (v, shape) {
        (serde_json::Value::Object(map), Shape::Object(fields)) => {
            let old = std::mem::take(map);
            for (key, mut child) in old {
                let field = fields
                    .iter()
                    .find(|(name, _)| *name == key)
                    .or_else(|| fields.iter().find(|(name, _)| fold_eq(name, &key)));
                match field {
                    Some((name, sub)) => {
                        fold_json_keys(&mut child, sub);
                        map.insert((*name).to_owned(), child);
                    }
                    None => {
                        map.insert(key, child);
                    }
                }
            }
        }
        (serde_json::Value::Array(items), Shape::Array(inner)) => {
            for item in items {
                fold_json_keys(item, inner);
            }
        }
        _ => {}
    }
}

/// Whether two keys are equal under simple case folding. `name` is ASCII.
fn fold_eq(name: &str, key: &str) -> bool {
    fn fold(c: char) -> char {
        match c {
            '\u{017F}' => 's',
            '\u{212A}' => 'k',
            c => c.to_ascii_lowercase(),
        }
    }
    name.chars().map(fold).eq(key.chars().map(fold))
}

/// A key for work shared between callers, in a TEST build scoped to the runtime the caller is on.
///
/// Go's tests run one at a time and share the package's flights and batchers; cargo runs ours in parallel, each on
/// its own runtime, so two tests that use the same token or title would otherwise join each other's work. Scoping by
/// runtime id partitions them. A release build has one runtime and returns the key unchanged.
#[cfg(test)]
pub(crate) fn scoped(key: &str) -> String {
    format!("{}:{key}", tokio::runtime::Handle::current().id())
}

#[cfg(not(test))]
pub(crate) fn scoped(key: &str) -> String {
    key.to_owned()
}

/// Go `singleflight.Group.DoChan`: concurrent callers of one key share one run of the work and its answer, and the
/// work is NOT tied to the caller that started it. Dropping the caller that started the work (the request went
/// away, its budget ran out) leaves the work running for everyone still waiting, which is the property the
/// account listing, the shared status reads and the indexer answers were built to have: "the leader now runs
/// detached from cancellation".
///
/// `indexerconfig::Flight` is the other shape and the two do not merge: there a dropped leader hands the work to a
/// waiter, who runs it again (right for a mint, which a request that left has no use for), where here the work
/// outlives every caller because it has an effect beyond them (a listing fetched, a cache warmed).
///
/// The work runs in its own task, so a panic in it ends that task and nothing else: every waiter is answered
/// `None` rather than left waiting, where Go would have died. This is Go's `recoverBackground`: a singleflight
/// closure that panics re-raises on a bare goroutine nothing can recover, so each of these jobs recovers its own,
/// counts it, and abandons the work.
pub(crate) struct DetachedFlight<T> {
    calls:
        std::sync::Mutex<std::collections::BTreeMap<String, tokio::sync::watch::Receiver<Option<Option<T>>>>>,
    /// What the work is, for the log line a panic writes. Always a literal, so it is a fixed vocabulary for the
    /// limiter's key.
    what: &'static str,
}

/// Forgets a flight's key when its task ends, however it ends (a runtime shutting down drops the task mid-flight):
/// a key left behind would have every later caller join a question nobody is asking.
struct ForgetKey<T: 'static> {
    flight: &'static DetachedFlight<T>,
    key: String,
}

impl<T> Drop for ForgetKey<T> {
    fn drop(&mut self) {
        self.flight.calls.lock().unwrap_or_else(std::sync::PoisonError::into_inner).remove(&self.key);
    }
}

impl<T: Clone + Send + Sync + 'static> DetachedFlight<T> {
    pub(crate) const fn new(what: &'static str) -> Self {
        DetachedFlight { calls: std::sync::Mutex::new(std::collections::BTreeMap::new()), what }
    }

    /// Runs `make()` for the first caller of `key`; a caller that arrives while it is in flight waits for the same
    /// answer. The key is forgotten before the answer is published, so a caller arriving after it runs the work
    /// again: nothing is cached here. The answer is `None` for a run that panicked, and the flag says whether
    /// this caller shared a flight someone else started (Go's `result.Shared`).
    pub(crate) async fn run<F, Fut>(&'static self, key: &str, make: F) -> (Option<T>, bool)
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = T> + Send + 'static,
    {
        let key = scoped(key);
        let (mut rx, shared) = {
            let mut calls = self.calls.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
            match calls.get(&key) {
                Some(rx) => (rx.clone(), true),
                None => {
                    let (tx, rx) = tokio::sync::watch::channel(None);
                    calls.insert(key.clone(), rx.clone());
                    let work = make();
                    let forget = ForgetKey { flight: self, key };
                    tokio::spawn(async move {
                        let answer = match tokio::spawn(work).await {
                            Ok(answer) => Some(answer),
                            Err(e) => {
                                if e.is_panic() {
                                    crate::metrics::metrics()
                                        .background_panic
                                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                                    crate::loglimit::log_limited(
                                        &format!("panic:{}", self.what),
                                        &format!(
                                            "{} panicked, abandoning it: {}",
                                            self.what,
                                            panic_text(e.into_panic())
                                        ),
                                    );
                                }
                                None
                            }
                        };
                        drop(forget);
                        let _ = tx.send(Some(answer));
                    });
                    (rx, false)
                }
            }
        };
        let Ok(answered) = rx.wait_for(|v| v.is_some()).await else {
            return (None, shared);
        };
        let answer = (*answered).clone().flatten();
        drop(answered);
        (answer, shared)
    }
}

/// A panic's payload as Go's `%v` of the recovered value would print it.
fn panic_text(payload: Box<dyn std::any::Any + Send>) -> String {
    match payload.downcast::<&str>() {
        Ok(s) => (*s).to_owned(),
        Err(payload) => match payload.downcast::<String>() {
            Ok(s) => *s,
            Err(_) => "a panic with no message".to_owned(),
        },
    }
}

/// What Go's `url.Parse` makes of a string, as far as scout reads it: the scheme, the host exactly as written (not
/// lower-cased, a written port kept), the percent-decoded path, and whether the URL is opaque (`mailto:x`).
pub(crate) struct GoUrl {
    pub(crate) scheme: String,
    pub(crate) host: String,
    pub(crate) path: Vec<u8>,
    pub(crate) opaque: bool,
}

/// Go's `url.Parse(raw)` (`net/url.parse(raw, false)` plus the fragment): `None` where Go returns an error. The WHATWG
/// parser behind `reqwest::Url` accepts and rejects different strings (a relative reference, a host with a space), so
/// anything that must answer "would Go take this?" asks here, and anything that must key on Go's `URL.Host` reads it
/// from here.
pub(crate) fn go_url_parse(raw: &str) -> Option<GoUrl> {
    if raw.bytes().any(|b| b < 0x20 || b == 0x7f) {
        return None; // net/url: invalid control character in URL
    }
    let (u, fragment) = raw.split_once('#').unwrap_or((raw, ""));
    let parsed = go_url_parse_main(u)?;
    go_unescape(fragment, GoEscape::Fragment)?;
    Some(parsed)
}

fn go_url_parse_main(raw: &str) -> Option<GoUrl> {
    if raw == "*" {
        return Some(GoUrl {
            scheme: String::new(),
            host: String::new(),
            path: b"*".to_vec(),
            opaque: false,
        });
    }
    // getScheme: letters, then letters, digits, `+`, `-` and `.`, closed by `:`. A `:` first is an error; any other
    // character first means there is no scheme.
    let mut scheme = String::new();
    let mut rest = raw;
    for (i, c) in raw.char_indices() {
        match c {
            'a'..='z' | 'A'..='Z' => {}
            '0'..='9' | '+' | '-' | '.' => {
                if i == 0 {
                    break;
                }
            }
            ':' => {
                if i == 0 {
                    return None; // missing protocol scheme
                }
                scheme = raw[..i].to_ascii_lowercase();
                rest = &raw[i + 1..];
                break;
            }
            _ => break,
        }
    }
    let rest = if rest.ends_with('?') && rest.matches('?').count() == 1 {
        &rest[..rest.len() - 1]
    } else {
        rest.split_once('?').map_or(rest, |(path, _)| path)
    };
    if !rest.starts_with('/') {
        if !scheme.is_empty() {
            // Rootless paths are opaque.
            return Some(GoUrl { scheme, host: String::new(), path: Vec::new(), opaque: true });
        }
        // Avoid confusion with malformed schemes, like cache_object:foo/bar.
        if rest.split('/').next().is_some_and(|segment| segment.contains(':')) {
            return None;
        }
    }
    let (host, path) = if (!scheme.is_empty() || !rest.starts_with("///")) && rest.starts_with("//") {
        let authority_and_path = &rest[2..];
        let (authority, path) =
            authority_and_path.split_at(authority_and_path.find('/').unwrap_or(authority_and_path.len()));
        (go_parse_authority(authority)?, path)
    } else {
        (String::new(), rest)
    };
    let path = go_unescape(path, GoEscape::Path)?;
    Some(GoUrl { scheme, host, path, opaque: false })
}

/// Go `parseAuthority`: the host of `user:pass@host:port`, validated.
fn go_parse_authority(authority: &str) -> Option<String> {
    let (userinfo, host) = match authority.rfind('@') {
        Some(i) => (Some(&authority[..i]), &authority[i + 1..]),
        None => (None, authority),
    };
    let host = go_parse_host(host)?;
    if let Some(userinfo) = userinfo {
        let valid = userinfo.chars().all(|c| c.is_alphanumeric() || "-._:~!$&'()*+,;=%@".contains(c));
        if !valid {
            return None; // net/url: invalid userinfo
        }
    }
    Some(host)
}

/// Go `validOptionalPort`.
fn go_valid_port(port: &str) -> bool {
    match port.strip_prefix(':') {
        None => port.is_empty(),
        Some(digits) => digits.bytes().all(|b| b.is_ascii_digit()),
    }
}

/// Go `parseHost`.
fn go_parse_host(host: &str) -> Option<String> {
    if host.starts_with('[') {
        let close = host.rfind(']')?;
        if !go_valid_port(&host[close + 1..]) {
            return None;
        }
        if let Some(zone) = host[..close].find("%25") {
            let host1 = go_unescape(&host[..zone], GoEscape::Host)?;
            let host2 = go_unescape(&host[zone..close], GoEscape::Zone)?;
            let host3 = go_unescape(&host[close..], GoEscape::Host)?;
            let mut out = host1;
            out.extend(host2);
            out.extend(host3);
            return String::from_utf8(out).ok();
        }
    } else if let Some(i) = host.rfind(':') {
        if !go_valid_port(&host[i..]) {
            return None;
        }
    }
    String::from_utf8(go_unescape(host, GoEscape::Host)?).ok()
}

#[derive(Clone, Copy, PartialEq)]
enum GoEscape {
    Host,
    Zone,
    Path,
    Fragment,
}

/// Go `url.unescape`: `%XX` to its byte, `None` for a `%` not followed by two hex digits, and in a host a byte Go
/// refuses there (a space, a brace) or an escape of an ASCII byte other than `%25`.
fn go_unescape(s: &str, mode: GoEscape) -> Option<Vec<u8>> {
    let b = s.as_bytes();
    let mut out = Vec::with_capacity(b.len());
    let mut i = 0;
    while i < b.len() {
        match b[i] {
            b'%' => {
                let hex = b.get(i + 1..i + 3)?;
                if !hex.iter().all(u8::is_ascii_hexdigit) {
                    return None;
                }
                let value = u8::from_str_radix(std::str::from_utf8(hex).ok()?, 16).ok()?;
                // Go: a host may not hide an ASCII byte behind an escape (only the zone separator `%25` is allowed).
                if mode == GoEscape::Host && value < 0x80 && &b[i..i + 3] != b"%25" {
                    return None;
                }
                out.push(value);
                i += 3;
            }
            c => {
                if matches!(mode, GoEscape::Host | GoEscape::Zone)
                    && c < 0x80
                    && !(c.is_ascii_alphanumeric() || b"-_.~!$&'()*+,;=:[]<>\"".contains(&c))
                {
                    return None; // invalid character in host name
                }
                out.push(c);
                i += 1;
            }
        }
    }
    Some(out)
}
