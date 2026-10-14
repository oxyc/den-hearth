//! The config segment of an addon URL, read the same way by Scout, Reel and Subtitles: the sealed-config
//! keyring (`Keyring`, `seal`, `open_segment`) and the install-revocation policy (`InstallId`, `Revocation`,
//! `Rejected`), and the combined Den addon's config that holds a part for each (`addon`). What a config MEANS
//! (its JSON fields) stays in each service.
//!
//! Wire format, shared with the Go addon and the browser bundle: the base64url-decoded segment is either
//! `version_byte(0x01) ‖ crypto_box_seal(config_json)` or, where the addon has no key, the plaintext JSON.

pub mod addon;
mod install;
mod keyring;

pub use addon::Addon;
pub use install::{InstallId, Rejected, Revocation, MAX_EPOCH};
pub use keyring::{decode_key, seal, KeyPair, Keyring, SEALED_VERSION};

/// A config segment read as far as its JSON.
pub struct Opened {
    pub json: Vec<u8>,
    /// It came out of a sealed blob. A plaintext segment is the credentials in the clear, so a service that
    /// grants a narrower scope only to sealed configs needs to know.
    pub sealed: bool,
}

/// Read the path segment `blob` down to its JSON: base64url, then open the seal. Fails CLOSED: a sealed
/// segment with no keyring, or one no key opens, is `Undecodable`; and with a keyring every install
/// `/configure` issues is sealed (its public key is public, so a plaintext config is one anyone could have
/// minted), so a plaintext segment is `Plaintext`. Without a keyring plaintext is the only form there is.
pub fn open_segment(keyring: Option<&Keyring>, blob: &str) -> Result<Opened, Rejected> {
    let data = den_net::b64url_decode(blob).map_err(|_| Rejected::Undecodable)?;
    match data.first() {
        None => Err(Rejected::Undecodable),
        Some(&SEALED_VERSION) => {
            let json = keyring.and_then(|k| k.open(&data[1..])).ok_or(Rejected::Undecodable)?;
            Ok(Opened { json, sealed: true })
        }
        Some(_) if keyring.is_some() => Err(Rejected::Plaintext),
        Some(_) => Ok(Opened { json: data, sealed: false }),
    }
}

#[cfg(test)]
mod tests;
