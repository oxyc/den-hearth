//! Port of `internal/scout/util.go`: ETags, cache-key digests and lenient base64url.

/// Go `etagHex`: 16 hex digits of FNV-1a-64 of `s`.
pub fn etag_hex(s: &str) -> String {
    todo!("util.go etagHex")
}

/// Go `keyHash`: 128 bits of SHA-256 as hex, the collision-resistant digest for cache keys.
pub fn key_hash(s: &str) -> String {
    todo!("util.go keyHash")
}

/// Go `b64urlDecode`: base64url, padded or not.
pub fn b64url_decode(s: &str) -> Result<Vec<u8>, String> {
    todo!("util.go b64urlDecode")
}

/// Go `b64urlEncode`: unpadded base64url.
pub fn b64url_encode(b: &[u8]) -> String {
    todo!("util.go b64urlEncode")
}
