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
