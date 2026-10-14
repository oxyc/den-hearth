//! Port of `internal/scout/seal.go`: sealed config-in-URL (libsodium `crypto_box_seal`: X25519 +
//! XSalsa20-Poly1305, nonce = blake2b_24(eph_pub || recipient_pub)), interoperable byte for byte with
//! libsodium.js and the Rust `crypto_box` crate den-subtitles and den-reel use.

/// Go `sealedVersion`: the first byte of a DECODED config segment marking a sealed blob. A legacy
/// plaintext segment starts with `{` (0x7b), so the two never collide.
pub const SEALED_VERSION: u8 = 0x01;
pub const X25519_KEY_SIZE: usize = 32;
pub const SEAL_EPH_PUB_SIZE: usize = 32;
/// 32 (ephemeral public key) + 16 (Poly1305 tag).
pub const SEAL_OVERHEAD: usize = SEAL_EPH_PUB_SIZE + 16;

/// Go `sealKeypair`: a recipient X25519 keypair; `pub_key` is derived from `priv_key`.
#[derive(Clone)]
pub struct SealKeypair {
    pub priv_key: [u8; X25519_KEY_SIZE],
    pub pub_key: [u8; X25519_KEY_SIZE],
}

/// FROZEN. Go `sealKeyring`: the current key first, then prior keys (tried in order on open) so the
/// keypair can rotate without breaking installs sealed to an older key. `ticket.rs` derives its play
/// keys from this.
#[derive(Clone, Default)]
pub struct SealKeyring {
    pub keys: Vec<SealKeypair>,
}

/// Go `newSealKeypair`.
pub fn new_seal_keypair(priv_key: [u8; X25519_KEY_SIZE]) -> SealKeypair {
    todo!("seal.go newSealKeypair")
}

/// Go `sealNonce`: blake2b_24(ephPub || recipientPub).
pub fn seal_nonce(eph_pub: &[u8; X25519_KEY_SIZE], recipient_pub: &[u8; X25519_KEY_SIZE]) -> [u8; 24] {
    todo!("seal.go sealNonce")
}

/// Go `seal`: encrypts to `recipient_pub`; output = eph_pub (32) || box(plaintext).
pub fn seal(recipient_pub: &[u8; X25519_KEY_SIZE], plaintext: &[u8]) -> Result<Vec<u8>, String> {
    todo!("seal.go seal")
}

impl SealKeyring {
    /// Go `(*sealKeyring).open`: fails closed, no key opening it is an error and no plaintext.
    pub fn open(&self, sealed: &[u8]) -> Result<Vec<u8>, String> {
        todo!("seal.go sealKeyring.open")
    }

    /// Go `currentPubBase64` (standard base64), empty with no keys.
    pub fn current_pub_base64(&self) -> String {
        todo!("seal.go currentPubBase64")
    }
}

/// Go `parseSealKeyring`: a current base64 private key plus comma-separated prior keys. Empty current
/// gives `Ok(None)` (sealed URLs disabled); a malformed current key is an error; a malformed prior key
/// is skipped and logged.
pub fn parse_seal_keyring(current: &str, prev: &str) -> Result<Option<SealKeyring>, String> {
    todo!("seal.go parseSealKeyring")
}

/// Go `decodeKeyB64`: std or url base64, with or without padding.
pub fn decode_key_b64(s: &str) -> Result<Vec<u8>, String> {
    todo!("seal.go decodeKeyB64")
}
