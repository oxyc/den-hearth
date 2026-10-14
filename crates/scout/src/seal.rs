//! Go `seal.go`'s names for the sealed config-in-URL, over `den_seal`, which holds the keyring Scout, Reel
//! and Subtitles share (libsodium `crypto_box_seal`, byte for byte). `ticket.rs` derives its play keys from
//! the keyring's private bytes.

use crate::loglimit::emit;

pub use den_seal::{seal, Keyring as SealKeyring, SEALED_VERSION};

/// Go `parseSealKeyring`: a current base64 private key plus comma-separated prior keys. Empty current
/// gives `Ok(None)` (sealed URLs disabled; legacy plaintext still works); a malformed current key is an
/// error; a malformed prior key is skipped and logged.
pub fn parse_seal_keyring(current: &str, prev: &str) -> Result<Option<SealKeyring>, String> {
    SealKeyring::parse(current, prev, &emit)
}
