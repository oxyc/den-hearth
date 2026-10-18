//! Per-install config, base64url-encoded into the addon URL path (den-scout/den-subtitles style). It
//! carries the user's BYOK TMDB key (and an optional KinoCheck key) — the discovery credentials that
//! used to live in the server environment. It is a **bearer secret**: the Den app builds it at
//! `/den/configure`, seals it to the addon's key, stores it in the Keychain, and never logs it. We validate
//! + bound the untrusted blob before use and never echo the key back.
//!
//! The segment's seal and the revocation policy are `den_seal`'s, shared with Scout and Subtitles.

use den_seal::{open_segment, InstallId, Keyring, MAX_EPOCH};
pub use den_seal::{Rejected, Revocation};
use serde::Deserialize;

/// A validated install config. Both keys are BYOK and ride in the addon URL. The TMDB key (trailer
/// discovery) is required; the KinoCheck key (fallback source) is optional.
#[derive(Debug, Clone)]
pub struct UserConfig {
    pub tmdb_key: String,
    pub kinocheck_key: Option<String>,
    /// Install id (`iid`), minted by /den/configure for each link it builds: what `REVOKED_INSTALLS`
    /// names. `None` on a link built before ids existed, which only `CONFIG_EPOCH` can revoke.
    pub iid: Option<String>,
    /// The config epoch (`ep`) the link was stamped with; absent reads as 0.
    pub ep: u64,
}

/// Untrusted wire shape before validation.
#[derive(Deserialize)]
struct RawConfig {
    #[serde(rename = "tmdbKey", default)]
    tmdb_key: String,
    #[serde(rename = "kinocheckKey")]
    kinocheck_key: Option<String>,
    iid: Option<String>,
    ep: Option<u64>,
}

/// `decode`, then the revocation check. The config-scoped routes read their config through this
/// and nothing else, so a revocation reaches every URL that embeds the config.
pub fn decode_checked(
    keyring: Option<&Keyring>,
    revocation: &Revocation,
    blob: &str,
) -> Result<UserConfig, Rejected> {
    let cfg = decode_segment(keyring, blob)?;
    revocation.check(cfg.iid.as_deref(), cfg.ep)?;
    Ok(cfg)
}

/// Decode the config path segment into a validated config. The decoded bytes are either a SEALED blob
/// (decrypt with the keyring) or a plaintext JSON config. Sealed with no keyring, or a decrypt failure,
/// fails CLOSED — never a partial/empty config. Plaintext decodes only with no keyring: with one, every
/// install `/den/configure` issues is sealed, and `/den/config-key` is public, so a plaintext config is one anyone
/// could have minted (`den_seal::open_segment`).
fn decode_segment(keyring: Option<&Keyring>, blob: &str) -> Result<UserConfig, Rejected> {
    parse(open_segment(keyring, blob)?)
}

fn parse(opened: den_seal::Opened) -> Result<UserConfig, Rejected> {
    let raw: RawConfig = serde_json::from_slice(&opened.json).map_err(|_| Rejected::Undecodable)?;
    validate(raw).ok_or(Rejected::Undecodable)
}

/// Whether `json` is a config Reel would accept, for the combined Den addon to check its Reel member with.
pub fn is_valid(json: &[u8]) -> bool {
    parse(den_seal::Opened { json: json.to_vec(), sealed: false }).is_ok()
}

/// The terms a request's config and its install-bound links are read on: Reel's own, or the combined Den
/// addon's when hearth routed the request under `/den` (`den_seal::Addon`). Each refuses the other's configs:
/// the keys differ, and a combined config is read through its Reel member.
#[derive(Clone, Copy)]
pub(crate) struct Terms<'a> {
    pub keyring: Option<&'a Keyring>,
    pub key_invalid: bool,
    pub revocation: &'a Revocation,
    pub addon: Option<&'a den_seal::Addon>,
}

impl<'a> Terms<'a> {
    pub fn of(state: &'a crate::AppState, addon: Option<&'a den_seal::Addon>) -> Terms<'a> {
        match addon {
            Some(addon) => Terms {
                keyring: addon.keyring.as_ref(),
                key_invalid: addon.key_invalid,
                revocation: &addon.revocation,
                addon: Some(addon),
            },
            None => Terms {
                keyring: state.config_keyring.as_ref(),
                key_invalid: state.config_key_invalid,
                revocation: &state.cfg.revocation,
                addon: None,
            },
        }
    }

    /// How a link minted on these terms for the install `iid`/`ep` is bound.
    pub fn binding<'b>(&self, iid: Option<&'b str>, ep: u64) -> crate::sign::Binding<'b> {
        crate::sign::Binding::install(iid, ep, self.addon.is_some())
    }

    /// `decode_checked` on these terms.
    pub fn decode_checked(&self, blob: &str) -> Result<UserConfig, Rejected> {
        let Some(addon) = self.addon else {
            return decode_checked(self.keyring, self.revocation, blob);
        };
        let cfg = parse(addon.open(blob)?)?;
        self.revocation.check(cfg.iid.as_deref(), cfg.ep)?;
        Ok(cfg)
    }
}

fn validate(raw: RawConfig) -> Option<UserConfig> {
    // A malformed install id is a malformed config: admitting it would leave an install that no
    // `REVOKED_INSTALLS` entry can ever name.
    if raw.iid.as_deref().is_some_and(|iid| InstallId::parse(iid).is_none()) {
        return None;
    }
    let ep = raw.ep.unwrap_or(0);
    if ep > MAX_EPOCH {
        return None;
    }
    // The TMDB key is the discovery credential — required, bounded. (TMDB v3 keys are 32 hex chars;
    // v4 read tokens are longer JWTs. Accept a generous range so either works without pinning a format.)
    if raw.tmdb_key.is_empty() || raw.tmdb_key.len() > 512 {
        return None;
    }
    let kinocheck_key = raw.kinocheck_key.filter(|k| !k.is_empty() && k.len() <= 256);
    Some(UserConfig { tmdb_key: raw.tmdb_key, kinocheck_key, iid: raw.iid, ep })
}

#[cfg(test)]
mod tests {
    use base64::Engine;

    use super::*;

    fn decode(keyring: Option<&Keyring>, blob: &str) -> Option<UserConfig> {
        decode_segment(keyring, blob).ok()
    }

    fn encode(json: &str) -> String {
        base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(json)
    }

    #[test]
    fn decodes_a_plaintext_config() {
        let cfg = decode(None, &encode(r#"{"tmdbKey":"abc123"}"#)).unwrap();
        assert_eq!(cfg.tmdb_key, "abc123");
        assert!(cfg.kinocheck_key.is_none());
    }

    #[test]
    fn carries_an_optional_kinocheck_key() {
        let cfg = decode(None, &encode(r#"{"tmdbKey":"abc","kinocheckKey":"kc"}"#)).unwrap();
        assert_eq!(cfg.kinocheck_key.as_deref(), Some("kc"));
    }

    #[test]
    fn rejects_a_config_with_no_tmdb_key() {
        assert!(decode(None, &encode(r#"{"kinocheckKey":"kc"}"#)).is_none());
        assert!(decode(None, &encode(r#"{"tmdbKey":""}"#)).is_none());
        assert!(decode(None, "not base64!!").is_none());
    }

    #[test]
    fn decodes_a_sealed_config() {
        // A fixed segment sealing {tmdbKey, kinocheckKey} to the vector key with real libsodium
        // (PyNaCl SealedBox) — the sealed→UserConfig gate for den-reel, byte-compatible with the Go
        // addon, den-subtitles, and the browser bundle (same wire format, same vector key).
        const VEC_PRIV: &str = "AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8=";
        const SEG: &str = "Abo-qmntVxuOmeVa0Q5pPWju0VrZDS4aRoAP-0JHNtk7nmMcduhttWlvldwvUdXPafUGUegc4ul5J3gFVo8nEGOd8htc7he_3BihPsWtiuA5_2Du-FL5NpaNzfvqhDAHM_LAjw";
        let kr = Keyring::from_env(VEC_PRIV, "").unwrap().unwrap();

        let cfg = decode(Some(&kr), SEG).expect("sealed segment decodes");
        assert_eq!(cfg.tmdb_key, "sealed-tmdb-ok");
        assert_eq!(cfg.kinocheck_key.as_deref(), Some("kc-ok"));

        // Fail CLOSED: the same sealed segment with no keyring configured.
        assert!(decode(None, SEG).is_none());
        // With a keyring, plaintext is refused — and says why in the log.
        let plain = encode(r#"{"tmdbKey":"plain"}"#);
        assert!(decode(Some(&kr), &plain).is_none());
        let open = Revocation::default();
        assert_eq!(decode_checked(Some(&kr), &open, &plain).unwrap_err(), Rejected::Plaintext);
        assert_eq!(decode_checked(Some(&kr), &open, "not base64!!").unwrap_err(), Rejected::Undecodable);
    }

    /// A config sealed by `den_seal::seal` today opens here, exactly as one sealed by libsodium does.
    #[test]
    fn a_config_sealed_by_den_seal_opens() {
        let kr = Keyring::from_env("AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8=", "").unwrap().unwrap();
        let mut framed = vec![den_seal::SEALED_VERSION];
        framed.extend(
            den_seal::seal(&kr.keys[0].pub_key, br#"{"tmdbKey":"fresh","kinocheckKey":"kc"}"#).unwrap(),
        );
        let cfg =
            decode(Some(&kr), &base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(framed)).unwrap();
        assert_eq!((cfg.tmdb_key.as_str(), cfg.kinocheck_key.as_deref()), ("fresh", Some("kc")));
    }

    /// A segment copied from a mail or a terminal arrives padded, hand-wrapped, or with a stray trailing bit,
    /// and opens on Scout; it opens here too. (`{"tmdbKey":"abc123"}` is 20 bytes: one `=` of padding, and
    /// the last character carries two spare bits.)
    #[test]
    fn a_padded_or_wrapped_segment_opens_like_it_does_on_scout() {
        let seg = encode(r#"{"tmdbKey":"abc123"}"#);
        let (head, last) = seg.split_at(seg.len() - 1);
        let alphabet = base64::alphabet::URL_SAFE.as_str();
        let idx = alphabet.find(last).unwrap();
        let loose = format!("{head}{}", &alphabet[idx | 1..(idx | 1) + 1]);
        assert_ne!(loose, seg, "the fixture must differ in a trailing bit");
        let wrapped = format!("{}\r\n{}\n", &seg[..10], &seg[10..]);
        for (what, blob) in
            [("padded", format!("{seg}=")), ("wrapped", wrapped), ("non-zero trailing bits", loose)]
        {
            let cfg = decode(None, &blob).unwrap_or_else(|| panic!("a {what} segment did not open"));
            assert_eq!(cfg.tmdb_key, "abc123", "{what}");
        }
        // Still not base64url at all.
        assert!(decode(None, "AAECAwQFBgcICQoLDA0O+w").is_none(), "standard base64 opened");
    }

    /// Bytes 0..16 as an install id, and a second one that uses both url-safe characters.
    const IID: &str = "AAECAwQFBgcICQoLDA0ODw";
    const OTHER_IID: &str = "_-_-_-_-_-_-_-_-_-_-_w";

    #[test]
    fn a_config_carries_its_install_id_and_epoch() {
        let cfg = decode(None, &encode(&format!(r#"{{"tmdbKey":"k","iid":"{IID}","ep":3}}"#))).unwrap();
        assert_eq!(cfg.iid.as_deref(), Some(IID));
        assert_eq!(cfg.ep, 3);
        // Absent: no id, epoch 0.
        let cfg = decode(None, &encode(r#"{"tmdbKey":"k"}"#)).unwrap();
        assert!(cfg.iid.is_none());
        assert_eq!(cfg.ep, 0);
        // A malformed id or epoch makes the whole config malformed.
        for bad in [
            r#"{"tmdbKey":"k","iid":"short"}"#,
            r#"{"tmdbKey":"k","iid":"AAECAwQFBgcICQoLDA0ODw=="}"#,
            r#"{"tmdbKey":"k","iid":7}"#,
            r#"{"tmdbKey":"k","ep":-1}"#,
            r#"{"tmdbKey":"k","ep":1.5}"#,
            r#"{"tmdbKey":"k","ep":"1"}"#,
            r#"{"tmdbKey":"k","ep":2147483648}"#,
        ] {
            assert!(decode(None, &encode(bad)).is_none(), "accepted {bad}");
        }
        assert!(decode(None, &encode(r#"{"tmdbKey":"k","ep":2147483647}"#)).is_some());
    }

    #[test]
    fn a_listed_install_and_an_old_epoch_are_refused() {
        let revocation = Revocation::from_env(&format!(" {IID} ,, not-an-id"), Some("2"));
        let check = |json: &str| decode_checked(None, &revocation, &encode(json));

        let listed = format!(r#"{{"tmdbKey":"k","iid":"{IID}","ep":5}}"#);
        assert_eq!(check(&listed).unwrap_err(), Rejected::Revoked { iid_prefix: "AAECAw".into() });
        assert!(check(&format!(r#"{{"tmdbKey":"k","iid":"{OTHER_IID}","ep":5}}"#)).is_ok());

        assert_eq!(
            check(r#"{"tmdbKey":"k","ep":1}"#).unwrap_err(),
            Rejected::EpochTooOld { ep: 1, epoch: 2 }
        );
        assert_eq!(check(r#"{"tmdbKey":"k"}"#).unwrap_err(), Rejected::EpochTooOld { ep: 0, epoch: 2 });
        assert!(check(r#"{"tmdbKey":"k","ep":2}"#).is_ok());

        // Nothing configured: a link with no id and no epoch is admitted, as every link before ids was.
        assert!(decode_checked(None, &Revocation::default(), &encode(r#"{"tmdbKey":"k"}"#)).is_ok());
        assert_eq!(decode_checked(None, &revocation, "!!").unwrap_err(), Rejected::Undecodable);
    }

    #[test]
    fn requiring_ids_refuses_only_links_without_one() {
        let revocation = Revocation::from_env("", None).requiring_install_id(true);
        let check = |json: &str| decode_checked(None, &revocation, &encode(json));
        assert_eq!(check(r#"{"tmdbKey":"k"}"#).unwrap_err(), Rejected::NoInstallId);
        assert!(check(&format!(r#"{{"tmdbKey":"k","iid":"{IID}","ep":0}}"#)).is_ok());
    }
}
