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
