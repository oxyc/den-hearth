//! Port of `internal/scout/util.go`: ETags, cache-key digests and lenient base64url.

use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine as _;
use sha2::{Digest, Sha256};

/// Go `etagHex`: 16 hex digits of FNV-1a-64 of `s`.
///
/// 64 bits, because the ETag is the only thing standing between a revalidating client and a 304 for a body
/// it does not hold; at 32 bits a collision between two versions of a list is within reach of an ordinary
/// cache's lifetime.
pub fn etag_hex(s: &str) -> String {
    format!("{:016x}", den_http::stable_hash(s.as_bytes()))
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
    den_net::b64url_decode(s)
}

/// Go `b64urlEncode`: unpadded base64url.
pub fn b64url_encode(b: &[u8]) -> String {
    URL_SAFE_NO_PAD.encode(b)
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
            // Every document an encoder wrote spells its keys exactly. Then nothing is renamed, the map stays as
            // it is, and only the children the shape looks into are visited.
            let renames = map.keys().any(|key| {
                !fields.iter().any(|(name, _)| *name == key.as_str())
                    && fields.iter().any(|(name, _)| fold_eq(name, key))
            });
            if !renames {
                for (key, child) in map.iter_mut() {
                    if let Some((_, sub)) = fields.iter().find(|(name, _)| *name == key.as_str()) {
                        fold_json_keys(child, sub);
                    }
                }
                return;
            }
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

/// A key for work shared between callers, scoped to the owning `ScoutRuntime` in every build.
///
/// Process-wide flights are shared by every hosted Scout instance. The owner namespace prevents independent
/// instances with equal provider credentials or titles from joining work whose detached task carries another
/// owner's budget and metrics, while clones of one owner continue to coalesce.
pub(crate) fn scoped(key: &str) -> String {
    format!("{}:{key}", crate::runtime::current_runtime().id())
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
    calls: std::sync::Arc<std::sync::Mutex<std::collections::BTreeMap<String, Call<T>>>>,
    /// What the work is, for the log line a panic writes. Always a literal, so it is a fixed vocabulary for the
    /// limiter's key.
    what: &'static str,
}

/// One run of a flight's work: where its answer will appear, and how many callers joined it (Go `call.dups`).
struct Call<T> {
    answer: tokio::sync::watch::Receiver<Option<Option<T>>>,
    dups: std::sync::Arc<std::sync::atomic::AtomicUsize>,
}

/// Forgets a flight's key when its task ends, however it ends (a runtime shutting down drops the task mid-flight):
/// a key left behind would have every later caller join a question nobody is asking.
struct ForgetKey<T: 'static> {
    calls: std::sync::Arc<std::sync::Mutex<std::collections::BTreeMap<String, Call<T>>>>,
    key: String,
}

impl<T> Drop for ForgetKey<T> {
    fn drop(&mut self) {
        self.calls.lock().unwrap_or_else(std::sync::PoisonError::into_inner).remove(&self.key);
    }
}

impl<T: Clone + Send + Sync + 'static> DetachedFlight<T> {
    pub(crate) fn new(what: &'static str) -> Self {
        DetachedFlight {
            calls: std::sync::Arc::new(std::sync::Mutex::new(std::collections::BTreeMap::new())),
            what,
        }
    }

    #[cfg(test)]
    pub(crate) fn joiners(&self, key: &str) -> Option<usize> {
        let key = scoped(key);
        self.calls
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(&key)
            .map(|call| call.dups.load(std::sync::atomic::Ordering::SeqCst))
    }

    #[cfg(test)]
    pub(crate) fn active(&self) -> usize {
        self.calls.lock().unwrap_or_else(std::sync::PoisonError::into_inner).len()
    }

    /// Runs `make()` for the first caller of `key`; a caller that arrives while it is in flight waits for the same
    /// answer. The key is forgotten before the answer is published, so a caller arriving after it runs the work
    /// again: nothing is cached here. The answer is `None` for a run that panicked, and the flag is Go's
    /// `result.Shared`: the run was handed to more than one caller. Every caller of such a run sees it, the one
    /// that started the work included (`c.dups > 0` is read when the answer goes out), so a run shared by two
    /// callers is reported twice.
    pub(crate) async fn run<F, Fut>(&self, key: &str, make: F) -> (Option<T>, bool)
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = T> + Send + 'static,
    {
        self.run_inner(key, None, make).await.expect("an unbounded detached flight cannot be refused")
    }

    /// Like [`Self::run`], but a new key is installed only when `admission` has a permit immediately available.
    /// An existing key is checked first and can always be joined, including while every permit is in use. `None`
    /// means this distinct key was refused; a completed or panicked admitted run is the tuple inside `Some`.
    pub(crate) async fn run_admitted<F, Fut>(
        &self,
        key: &str,
        admission: &std::sync::Arc<tokio::sync::Semaphore>,
        make: F,
    ) -> Option<(Option<T>, bool)>
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = T> + Send + 'static,
    {
        self.run_inner(key, Some(admission), make).await
    }

    async fn run_inner<F, Fut>(
        &self,
        key: &str,
        admission: Option<&std::sync::Arc<tokio::sync::Semaphore>>,
        make: F,
    ) -> Option<(Option<T>, bool)>
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = T> + Send + 'static,
    {
        let key = scoped(key);
        let (mut rx, dups, leader) = {
            let mut calls = self.calls.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
            match calls.get(&key) {
                Some(call) => {
                    call.dups.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    (call.answer.clone(), call.dups.clone(), None)
                }
                None => {
                    let permit = match admission {
                        Some(slots) => Some(std::sync::Arc::clone(slots).try_acquire_owned().ok()?),
                        None => None,
                    };
                    let (tx, rx) = tokio::sync::watch::channel(None);
                    let dups = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
                    calls.insert(key.clone(), Call { answer: rx.clone(), dups: dups.clone() });
                    (rx, dups, Some((tx, permit, key.clone())))
                }
            }
        };
        if let Some((tx, permit, key)) = leader {
            // Constructing the future is outside the map lock. If `make` itself panics, these guards still
            // remove the key and return the permit without deadlocking on the mutex being unwound.
            let forget = ForgetKey { calls: self.calls.clone(), key };
            let work = make();
            let what = self.what;
            let runtime = crate::runtime::current_runtime();
            tokio::spawn(crate::runtime::scope(runtime.clone(), async move {
                // Admission belongs to detached work, not to the request that happened to create it.
                let _permit = permit;
                let answer = match tokio::spawn(crate::runtime::scope(runtime, work)).await {
                    Ok(answer) => Some(answer),
                    Err(e) => {
                        if e.is_panic() {
                            crate::metrics::metrics()
                                .background_panic
                                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                            crate::loglimit::log_limited(
                                &format!("panic:{what}"),
                                &format!("{} panicked, abandoning it: {}", what, panic_text(e.into_panic())),
                            );
                        }
                        None
                    }
                };
                drop(forget);
                let _ = tx.send(Some(answer));
            }));
        }
        let Ok(answered) = rx.wait_for(|v| v.is_some()).await else {
            return Some((None, dups.load(std::sync::atomic::Ordering::SeqCst) > 0));
        };
        let answer = (*answered).clone().flatten();
        drop(answered);
        // A joiner is counted in `dups` before the work can finish, and the key is forgotten before the answer is
        // published, so the count is final by now.
        Some((answer, dups.load(std::sync::atomic::Ordering::SeqCst) > 0))
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
