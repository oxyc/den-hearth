use base64::Engine;
use crypto_box::aead::OsRng;
use crypto_box::{PublicKey, SecretKey};

/// First byte of a DECODED config segment marking a sealed blob. Legacy plaintext decodes to JSON whose
/// first byte is `{` (0x7b), so the two never collide.
pub const SEALED_VERSION: u8 = 0x01;

/// A recipient X25519 keypair. The private bytes stay readable because Scout derives its play-ticket keys
/// (HKDF) from them.
#[derive(Clone)]
pub struct KeyPair {
    pub priv_key: [u8; 32],
    pub pub_key: [u8; 32],
}

impl KeyPair {
    /// The keypair of a private key; the public half is derived from it.
    pub fn new(priv_key: [u8; 32]) -> KeyPair {
        let pub_key = *SecretKey::from(priv_key).public_key().as_bytes();
        KeyPair { priv_key, pub_key }
    }
}

/// The addon's recipient keys: current first, then prior keys (tried in order on open) so the keypair can
/// be rotated without breaking installs whose URL was sealed to an older key.
#[derive(Clone, Default)]
pub struct Keyring {
    pub keys: Vec<KeyPair>,
}

impl Keyring {
    /// `parse`, with a skipped prior key reported on stderr.
    pub fn from_env(current: &str, prev: &str) -> Result<Option<Keyring>, String> {
        Keyring::parse(current, prev, &|m| eprintln!("{m}"))
    }

    /// Build from a base64 current private key + comma-separated prior keys. Empty current gives `Ok(None)`
    /// (sealed URLs disabled; legacy plaintext still works). A malformed CURRENT key is an `Err` (a real
    /// misconfiguration). A malformed PRIOR key is skipped and reported to `warn`: a typo in one rotation
    /// entry must not take sealing offline for the good current key too.
    pub fn parse(current: &str, prev: &str, warn: &dyn Fn(&str)) -> Result<Option<Keyring>, String> {
        let current = current.trim();
        if current.is_empty() {
            return Ok(None);
        }
        let mut keys = vec![parse_key(current)?];
        for p in prev.split(',').map(str::trim).filter(|p| !p.is_empty()) {
            match parse_key(p) {
                Ok(k) => keys.push(k),
                Err(e) => warn(&format!("skipping a malformed CONFIG_KEYS_PREV entry: {e}")),
            }
        }
        Ok(Some(Keyring { keys }))
    }

    /// Open a `crypto_box_seal` ciphertext (current key first, then prior keys). `None` on any failure: the
    /// caller then refuses, never serves a partial or empty config.
    pub fn open(&self, sealed: &[u8]) -> Option<Vec<u8>> {
        self.keys.iter().find_map(|k| SecretKey::from(k.priv_key).unseal(sealed).ok())
    }

    /// A ring of keys derived from these, one for one and in the same order (HKDF-SHA256 over each private key,
    /// with `info`). A second addon can then seal under the operator's existing secret, and rotate with it,
    /// while the two rings hold different private keys: neither opens what was sealed to the other, nor anything
    /// derived from the other's keys.
    pub fn derive(&self, info: &[u8]) -> Keyring {
        let keys = self
            .keys
            .iter()
            .map(|k| {
                let mut derived = [0u8; 32];
                hkdf::Hkdf::<sha2::Sha256>::new(None, &k.priv_key)
                    .expand(info, &mut derived)
                    .expect("32 bytes is within HKDF-SHA256's output limit");
                KeyPair::new(derived)
            })
            .collect();
        Keyring { keys }
    }

    /// The current recipient public key (std base64), served at `/config-key` so a client can seal to it.
    /// Empty with no keys.
    pub fn current_pub_base64(&self) -> String {
        match self.keys.first() {
            Some(k) => base64::engine::general_purpose::STANDARD.encode(k.pub_key),
            None => String::new(),
        }
    }
}

/// `crypto_box_seal` to `recipient_pub`: `eph_pub(32) ‖ box(plaintext)`.
pub fn seal(recipient_pub: &[u8; 32], plaintext: &[u8]) -> Result<Vec<u8>, String> {
    PublicKey::from(*recipient_pub)
        .seal(&mut OsRng, plaintext)
        .map_err(|_| "sealed: encrypt failed".to_owned())
}

fn parse_key(s: &str) -> Result<KeyPair, String> {
    const BAD: &str = "bad config key (want base64 32-byte X25519 private key)";
    let raw = decode_key(s).ok_or(BAD)?;
    let priv_key: [u8; 32] = raw.try_into().map_err(|_| BAD.to_owned())?;
    Ok(KeyPair::new(priv_key))
}

/// Key material in std or url base64, with or without padding. Lenient the way Go's decoder is: carriage
/// returns and newlines are skipped and non-zero trailing bits accepted (the `base64` crate refuses both by
/// default).
pub fn decode_key(s: &str) -> Option<Vec<u8>> {
    use base64::alphabet::{STANDARD, URL_SAFE};
    use base64::engine::{DecodePaddingMode, GeneralPurpose, GeneralPurposeConfig};
    let config = |mode| {
        GeneralPurposeConfig::new().with_decode_allow_trailing_bits(true).with_decode_padding_mode(mode)
    };
    let engines = [
        GeneralPurpose::new(&STANDARD, config(DecodePaddingMode::RequireCanonical)),
        GeneralPurpose::new(&STANDARD, config(DecodePaddingMode::RequireNone)),
        GeneralPurpose::new(&URL_SAFE, config(DecodePaddingMode::RequireCanonical)),
        GeneralPurpose::new(&URL_SAFE, config(DecodePaddingMode::RequireNone)),
    ];
    let cleaned: String = s.chars().filter(|c| !matches!(c, '\r' | '\n')).collect();
    engines.iter().find_map(|enc| enc.decode(&cleaned).ok())
}
