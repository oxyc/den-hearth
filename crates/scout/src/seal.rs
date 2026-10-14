//! Port of `internal/scout/seal.go`: sealed config-in-URL (libsodium `crypto_box_seal`: X25519 +
//! XSalsa20-Poly1305, nonce = blake2b_24(eph_pub || recipient_pub)), interoperable byte for byte with
//! libsodium.js and the Rust `crypto_box` crate den-subtitles and den-reel use.

use base64::Engine;
use blake2::digest::consts::U24;
use blake2::Digest;
use crypto_box::aead::{Aead, OsRng};
use crypto_box::{Nonce, PublicKey, SalsaBox, SecretKey};

use crate::loglimit::emit;

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
    let pub_key = *SecretKey::from(priv_key).public_key().as_bytes();
    SealKeypair { priv_key, pub_key }
}

/// Go `sealNonce`: blake2b_24(ephPub || recipientPub) — the libsodium crypto_box_seal nonce derivation.
pub fn seal_nonce(eph_pub: &[u8; X25519_KEY_SIZE], recipient_pub: &[u8; X25519_KEY_SIZE]) -> [u8; 24] {
    let mut h = blake2::Blake2b::<U24>::new();
    h.update(eph_pub);
    h.update(recipient_pub);
    let mut n = [0u8; 24];
    n.copy_from_slice(&h.finalize());
    n
}

/// Go `seal`: encrypts to `recipient_pub` (crypto_box_seal). Output = eph_pub(32) || box(plaintext).
pub fn seal(recipient_pub: &[u8; X25519_KEY_SIZE], plaintext: &[u8]) -> Result<Vec<u8>, String> {
    let eph_sk = SecretKey::generate(&mut OsRng);
    let eph_pub = *eph_sk.public_key().as_bytes();
    let nonce: Nonce = seal_nonce(&eph_pub, recipient_pub).into();
    let recipient = PublicKey::from(*recipient_pub);
    let ct = SalsaBox::new(&recipient, &eph_sk)
        .encrypt(&nonce, plaintext)
        .map_err(|_| "sealed: encrypt failed".to_owned())?;
    let mut out = Vec::with_capacity(SEAL_EPH_PUB_SIZE + ct.len());
    out.extend_from_slice(&eph_pub);
    out.extend_from_slice(&ct);
    Ok(out)
}

impl SealKeyring {
    /// Go `(*sealKeyring).open`: decrypts a crypto_box_seal ciphertext with the keyring (current key first,
    /// then prior keys). Fails closed: returns an error and no plaintext if no key opens it (never a
    /// partial/empty config).
    pub fn open(&self, sealed: &[u8]) -> Result<Vec<u8>, String> {
        if sealed.len() < SEAL_OVERHEAD {
            return Err("sealed: too short".to_owned());
        }
        let mut eph_pub = [0u8; X25519_KEY_SIZE];
        eph_pub.copy_from_slice(&sealed[..SEAL_EPH_PUB_SIZE]);
        let ct = &sealed[SEAL_EPH_PUB_SIZE..];
        let eph = PublicKey::from(eph_pub);
        for k in &self.keys {
            let nonce: Nonce = seal_nonce(&eph_pub, &k.pub_key).into();
            if let Ok(pt) = SalsaBox::new(&eph, &SecretKey::from(k.priv_key)).decrypt(&nonce, ct) {
                return Ok(pt);
            }
        }
        Err("sealed: no key opened it".to_owned())
    }

    /// Go `currentPubBase64` (standard base64), empty with no keys. The current recipient public key is
    /// served to /configure + /config-key so the browser (or den-app) can seal to it.
    pub fn current_pub_base64(&self) -> String {
        match self.keys.first() {
            Some(k) => base64::engine::general_purpose::STANDARD.encode(k.pub_key),
            None => String::new(),
        }
    }
}

/// Go `parseSealKeyring`: a current base64 private key plus comma-separated prior keys. Empty current
/// gives `Ok(None)` (sealed URLs disabled; legacy plaintext still works); a malformed current key is an
/// error; a malformed prior key is skipped and logged.
pub fn parse_seal_keyring(current: &str, prev: &str) -> Result<Option<SealKeyring>, String> {
    let current = current.trim();
    if current.is_empty() {
        return Ok(None);
    }
    // A bad CURRENT key is a real misconfig.
    let mut keys = vec![key_from_b64(current)?];
    // A malformed PRIOR key is skipped, not fatal — a typo in one rotation entry must not disable the
    // whole ring (which would silently take sealing offline for the good current key too).
    for p in prev.split(',') {
        let p = p.trim();
        if p.is_empty() {
            continue;
        }
        match key_from_b64(p) {
            Ok(k) => keys.push(k),
            Err(e) => emit(&format!("skipping a malformed CONFIG_KEYS_PREV entry: {e}")),
        }
    }
    Ok(Some(SealKeyring { keys }))
}

/// One base64 X25519 private key, as Go's `add` closure reads it.
fn key_from_b64(s: &str) -> Result<SealKeypair, String> {
    const BAD: &str = "sealed: bad config key (want base64 32-byte X25519 private key)";
    let raw = decode_key_b64(s).map_err(|_| BAD.to_owned())?;
    let priv_key: [u8; X25519_KEY_SIZE] = raw.try_into().map_err(|_| BAD.to_owned())?;
    Ok(new_seal_keypair(priv_key))
}

/// Go `decodeKeyB64` accepts std/url base64 with or without padding.
///
/// Lenient the way Go's decoder is: carriage returns and newlines are skipped, and non-zero trailing bits are
/// accepted (the `base64` crate refuses both by default).
pub fn decode_key_b64(s: &str) -> Result<Vec<u8>, String> {
    use base64::alphabet::{STANDARD, URL_SAFE};
    use base64::engine::{DecodePaddingMode, GeneralPurpose, GeneralPurposeConfig};
    let config = |mode| GeneralPurposeConfig::new().with_decode_allow_trailing_bits(true).with_decode_padding_mode(mode);
    let engines = [
        GeneralPurpose::new(&STANDARD, config(DecodePaddingMode::RequireCanonical)),
        GeneralPurpose::new(&STANDARD, config(DecodePaddingMode::RequireNone)),
        GeneralPurpose::new(&URL_SAFE, config(DecodePaddingMode::RequireCanonical)),
        GeneralPurpose::new(&URL_SAFE, config(DecodePaddingMode::RequireNone)),
    ];
    let cleaned: String = s.chars().filter(|c| !matches!(c, '\r' | '\n')).collect();
    engines.iter().find_map(|enc| enc.decode(&cleaned).ok()).ok_or_else(|| "bad base64".to_owned())
}
