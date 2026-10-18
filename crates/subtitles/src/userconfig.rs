//! Per-install config, base64url-encoded into the addon URL path (Torrentio/den-scout style). It
//! carries the user's BYOK LLM credential, so it is a **bearer secret**: the Den app builds it at
//! `/den/configure`, stores it in the Keychain, and never logs it. We validate + clamp the untrusted
//! blob before use and never echo the key back.
//!
//! The segment's seal and the revocation policy are `den_seal`'s, shared with Scout and Reel.

use den_seal::{open_segment, InstallId, Keyring, MAX_EPOCH};
pub use den_seal::{Rejected, Revocation};
use serde::Deserialize;

/// LLM providers we know how to call. The wire value is the lowercase tag in the config JSON.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Provider {
    OpenAI,
    Anthropic,
    Google,
    Xai,
    OpenRouter,
    DeepL,
}

impl Provider {
    fn parse(s: &str) -> Option<Provider> {
        Some(match s {
            "openai" => Provider::OpenAI,
            "anthropic" => Provider::Anthropic,
            "google" => Provider::Google,
            "xai" => Provider::Xai,
            "openrouter" => Provider::OpenRouter,
            "deepl" => Provider::DeepL,
            _ => return None,
        })
    }

    /// Stable short name for this provider. Goes into cache keys, so it is deliberately not the
    /// Debug spelling: renaming a variant should not silently move every key that names it.
    pub fn tag(self) -> &'static str {
        match self {
            Provider::OpenAI => "openai",
            Provider::Anthropic => "anthropic",
            Provider::Google => "google",
            Provider::Xai => "xai",
            Provider::OpenRouter => "openrouter",
            Provider::DeepL => "deepl",
        }
    }

    /// Sensible default model when the user leaves the field blank — the cheap/fast/decent tier.
    pub fn default_model(self) -> &'static str {
        match self {
            Provider::OpenAI => "gpt-4o-mini",
            Provider::Anthropic => "claude-haiku-4-5",
            Provider::Google => "gemini-2.0-flash",
            Provider::Xai => "grok-2",
            Provider::OpenRouter => "openai/gpt-4o-mini",
            Provider::DeepL => "", // DeepL has no model knob
        }
    }
}

/// The translation credential. Optional on a config — a subtitles-only install (fetch + auto-sync,
/// no AI) simply omits it.
#[derive(Debug, Clone)]
pub struct LlmConfig {
    pub provider: Provider,
    pub model: String,
    pub api_key: String,
}

/// A validated install config. Everything is BYOK and rides in the addon URL — the app stores the
/// blob in the Keychain. The OpenSubtitles key (subtitle source) is required; the LLM credential
/// (translation) is optional.
#[derive(Debug, Clone)]
pub struct UserConfig {
    /// OpenSubtitles API-consumer key — the subtitle source. Required.
    pub opensubtitles_key: String,
    /// Optional OpenSubtitles service-account bearer to lift the download quota above anonymous.
    pub opensubtitles_token: Option<String>,
    /// Translation credential. `None` → subtitles-only (fetch + sync, no AI translation).
    pub llm: Option<LlmConfig>,
    /// Auto-sync non-hash results through Tier-1 reference alignment (`?ref=`). On by default; when
    /// off, the subtitle proxy still serves and caches, just without the automatic alignment.
    pub auto_sync: bool,
    /// Install id (`iid`), minted by /den/configure for each link it builds: what `REVOKED_INSTALLS`
    /// names. `None` on a link built before ids existed, which only `CONFIG_EPOCH` can revoke.
    pub iid: Option<String>,
    /// The config epoch (`ep`) the link was stamped with; absent reads as 0.
    pub ep: u64,
}

/// Untrusted wire shape before validation.
#[derive(Deserialize)]
struct RawConfig {
    #[serde(default)]
    provider: String,
    #[serde(default)]
    model: String,
    #[serde(rename = "apiKey", default)]
    api_key: String,
    #[serde(rename = "osKey", default)]
    opensubtitles_key: String,
    #[serde(rename = "osToken")]
    opensubtitles_token: Option<String>,
    #[serde(rename = "autoSync")]
    auto_sync: Option<bool>,
    iid: Option<String>,
    ep: Option<u64>,
}

/// `decode_segment`, then the revocation check.
pub fn decode_checked(
    keyring: Option<&Keyring>,
    revocation: &Revocation,
    blob: &str,
) -> Result<UserConfig, Rejected> {
    let cfg = decode_segment(keyring, blob)?;
    revocation.check(cfg.iid.as_deref(), cfg.ep)?;
    Ok(cfg)
}

/// `decode_checked` for a combined Den addon config: its Subtitles member, opened with the addon's keys and
/// admitted by the addon's revocation.
pub fn decode_den(den: &den_seal::Addon, blob: &str) -> Result<UserConfig, Rejected> {
    let cfg = parse(den.open(blob)?)?;
    den.revocation.check(cfg.iid.as_deref(), cfg.ep)?;
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

/// Whether `json` is a config Subtitles would accept, for the combined Den addon to check its member with.
pub fn is_valid(json: &[u8]) -> bool {
    parse(den_seal::Opened { json: json.to_vec(), sealed: false }).is_ok()
}

/// `decode_segment` for the tests that only want the config.
#[cfg(test)]
pub fn decode(keyring: Option<&Keyring>, blob: &str) -> Option<UserConfig> {
    decode_segment(keyring, blob).ok()
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
    // The OpenSubtitles key is the subtitle source — required, bounded.
    if raw.opensubtitles_key.is_empty() || raw.opensubtitles_key.len() > 128 {
        return None;
    }
    let opensubtitles_token = raw.opensubtitles_token.filter(|t| !t.is_empty() && t.len() <= 512);

    // The LLM credential is optional. It's absent iff neither a provider nor a key was given
    // (subtitles-only). If either is present, both must be valid — a half-filled AI section (e.g. a
    // key with no provider) is a 400 rather than a silent no-translate surprise.
    let llm = if raw.provider.is_empty() && raw.api_key.is_empty() {
        None
    } else {
        let provider = Provider::parse(&raw.provider)?;
        if raw.api_key.is_empty() || raw.api_key.len() > 512 {
            return None;
        }
        let model = if raw.model.trim().is_empty() {
            provider.default_model().to_string()
        } else if raw.model.trim().len() > 128 {
            return None;
        } else {
            raw.model.trim().to_string()
        };
        Some(LlmConfig { provider, model, api_key: raw.api_key })
    };

    Some(UserConfig {
        opensubtitles_key: raw.opensubtitles_key,
        opensubtitles_token,
        llm,
        auto_sync: raw.auto_sync.unwrap_or(true),
        iid: raw.iid,
        ep,
    })
}

#[cfg(test)]
mod tests {
    use base64::Engine;

    use super::*;

    fn encode(json: &str) -> String {
        base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(json)
    }

    #[test]
    fn decodes_and_defaults_the_model() {
        let blob = encode(r#"{"provider":"openai","apiKey":"sk-test","osKey":"os-test"}"#);
        let cfg = decode(None, &blob).unwrap();
        let llm = cfg.llm.unwrap();
        assert_eq!(llm.provider, Provider::OpenAI);
        assert_eq!(llm.model, "gpt-4o-mini");
        assert_eq!(cfg.opensubtitles_key, "os-test");
        assert!(cfg.auto_sync);
    }

    /// The model is stored trimmed and its emptiness is judged trimmed, so its length must be too.
    /// Measuring the untrimmed string rejected the WHOLE config — every route 400s — over padding
    /// that was about to be thrown away.
    #[test]
    fn a_padded_model_name_is_judged_on_what_is_kept() {
        let padded = format!("{}gpt-4o-mini{}", " ".repeat(80), " ".repeat(80));
        let blob = encode(&format!(
            r#"{{"provider":"openai","apiKey":"sk-test","osKey":"os-test","model":"{padded}"}}"#
        ));
        let cfg = decode(None, &blob).expect("padding is not a reason to reject a config");
        assert_eq!(cfg.llm.unwrap().model, "gpt-4o-mini");

        // A genuinely over-long model is still refused.
        let long = "m".repeat(129);
        let blob = encode(&format!(
            r#"{{"provider":"openai","apiKey":"sk-test","osKey":"os-test","model":"{long}"}}"#
        ));
        assert!(decode(None, &blob).is_none(), "an over-long model name must be refused");
    }

    /// A segment copied from a mail or a terminal arrives padded, hand-wrapped, or with a stray trailing bit,
    /// and opens on Scout; it opens here too. (`{"osKey":"os-test"}` is 19 bytes: two `=` of padding, and
    /// the last character carries four spare bits.)
    #[test]
    fn a_padded_or_wrapped_segment_opens_like_it_does_on_scout() {
        let seg = encode(r#"{"osKey":"os-test"}"#);
        let (head, last) = seg.split_at(seg.len() - 1);
        let alphabet = base64::alphabet::URL_SAFE.as_str();
        let idx = alphabet.find(last).unwrap();
        let loose = format!("{head}{}", &alphabet[idx | 1..(idx | 1) + 1]);
        assert_ne!(loose, seg, "the fixture must differ in a trailing bit");
        let wrapped = format!("{}\r\n{}\n", &seg[..10], &seg[10..]);
        for (what, blob) in
            [("padded", format!("{seg}==")), ("wrapped", wrapped), ("non-zero trailing bits", loose)]
        {
            let cfg = decode(None, &blob).unwrap_or_else(|| panic!("a {what} segment did not open"));
            assert_eq!(cfg.opensubtitles_key, "os-test", "{what}");
        }
        // Still not base64url at all.
        assert!(decode(None, "AAECAwQFBgcICQoLDA0O+w").is_none(), "standard base64 opened");
    }

    #[test]
    fn llm_is_optional_when_only_opensubtitles_is_given() {
        let cfg = decode(None, &encode(r#"{"osKey":"os-test"}"#)).unwrap();
        assert!(cfg.llm.is_none());
        assert_eq!(cfg.opensubtitles_key, "os-test");
    }

    #[test]
    fn rejects_bad_input() {
        // OpenSubtitles key is always required.
        assert!(decode(None, &encode(r#"{"provider":"openai","apiKey":"x"}"#)).is_none());
        // A half-filled AI section (provider, no key) is rejected, not silently dropped.
        assert!(decode(None, &encode(r#"{"provider":"openai","osKey":"o"}"#)).is_none());
        // Unknown provider.
        assert!(decode(None, &encode(r#"{"provider":"acme","apiKey":"x","osKey":"o"}"#)).is_none());
    }

    #[test]
    fn decodes_a_sealed_segment_minted_by_the_browser() {
        // A fixed segment minted by the /den/configure browser bundle (tweetnacl + blake2b crypto_box_seal),
        // sealing {osKey, provider, apiKey} to the vector key — the JS→Rust interop gate + full decode
        // path. Regenerate with scratch/sealsrc/entry.js's denSeal() if the wire format ever changes.
        const VEC_PRIV: &str = "AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8=";
        const JS_SEG: &str = "Ac3WWHzRZKV9OjdSgIPaNFFhaE9UY0vwxgSO6F5Ghug1nyjlKUodEQmhhlPhX-j1KffJnpj58HPhlpePWcbnuX9GL9rGMsdki1hGXSzRG94ON_aYocvFkl9bSU2QZa8o3waeHHm9wmjLQg";
        let kr = Keyring::from_env(VEC_PRIV, "").unwrap().unwrap();

        let cfg = decode(Some(&kr), JS_SEG).expect("sealed segment decodes");
        assert_eq!(cfg.opensubtitles_key, "os-js-ok");
        assert_eq!(cfg.llm.unwrap().provider, Provider::OpenAI);

        // Fail CLOSED: a sealed segment with no keyring configured.
        assert!(decode(None, JS_SEG).is_none());
        // With a keyring, plaintext is refused; with none it is the only form, and decodes.
        let plain = encode(r#"{"osKey":"os-plain"}"#);
        assert!(decode(Some(&kr), &plain).is_none());
        assert!(decode(None, &plain).is_some());
        assert_eq!(
            decode_checked(Some(&kr), &Revocation::default(), &plain).unwrap_err(),
            Rejected::Plaintext
        );
        assert_eq!(decode_checked(Some(&kr), &Revocation::default(), JS_SEG).map(|c| c.ep), Ok(0));
    }

    /// A config sealed by `den_seal::seal` today opens here, exactly as one sealed by the browser does.
    #[test]
    fn a_config_sealed_by_den_seal_opens() {
        let kr = Keyring::from_env("AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8=", "").unwrap().unwrap();
        let mut framed = vec![den_seal::SEALED_VERSION];
        framed.extend(den_seal::seal(&kr.keys[0].pub_key, br#"{"osKey":"os-fresh"}"#).unwrap());
        let cfg =
            decode(Some(&kr), &base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(framed)).unwrap();
        assert_eq!(cfg.opensubtitles_key, "os-fresh");
    }

    /// Bytes 0..16 as an install id, and a second one that uses both url-safe characters.
    const IID: &str = "AAECAwQFBgcICQoLDA0ODw";
    const OTHER_IID: &str = "_-_-_-_-_-_-_-_-_-_-_w";

    #[test]
    fn a_config_carries_its_install_id_and_epoch() {
        let cfg = decode(None, &encode(&format!(r#"{{"osKey":"o","iid":"{IID}","ep":3}}"#))).unwrap();
        assert_eq!(cfg.iid.as_deref(), Some(IID));
        assert_eq!(cfg.ep, 3);
        // Absent: no id, epoch 0.
        let cfg = decode(None, &encode(r#"{"osKey":"o"}"#)).unwrap();
        assert!(cfg.iid.is_none());
        assert_eq!(cfg.ep, 0);
        // A malformed id or epoch makes the whole config malformed.
        for bad in [
            r#"{"osKey":"o","iid":"short"}"#,
            r#"{"osKey":"o","iid":"AAECAwQFBgcICQoLDA0ODw=="}"#,
            r#"{"osKey":"o","iid":7}"#,
            r#"{"osKey":"o","ep":-1}"#,
            r#"{"osKey":"o","ep":1.5}"#,
            r#"{"osKey":"o","ep":"1"}"#,
            r#"{"osKey":"o","ep":2147483648}"#,
        ] {
            assert!(decode(None, &encode(bad)).is_none(), "accepted {bad}");
        }
        assert!(decode(None, &encode(r#"{"osKey":"o","ep":2147483647}"#)).is_some());
    }

    #[test]
    fn a_listed_install_and_an_old_epoch_are_refused() {
        let revocation = Revocation::from_env(&format!(" {IID} ,, not-an-id"), Some("2"));
        let check = |json: &str| decode_checked(None, &revocation, &encode(json));

        let listed = format!(r#"{{"osKey":"o","iid":"{IID}","ep":5}}"#);
        assert_eq!(check(&listed).unwrap_err(), Rejected::Revoked { iid_prefix: "AAECAw".into() });
        assert!(check(&format!(r#"{{"osKey":"o","iid":"{OTHER_IID}","ep":5}}"#)).is_ok());

        assert_eq!(check(r#"{"osKey":"o","ep":1}"#).unwrap_err(), Rejected::EpochTooOld { ep: 1, epoch: 2 });
        assert_eq!(check(r#"{"osKey":"o"}"#).unwrap_err(), Rejected::EpochTooOld { ep: 0, epoch: 2 });
        assert!(check(r#"{"osKey":"o","ep":2}"#).is_ok());

        // Nothing configured: a link with no id and no epoch is admitted, as every link before ids was.
        assert!(decode_checked(None, &Revocation::default(), &encode(r#"{"osKey":"o"}"#)).is_ok());
        assert_eq!(decode_checked(None, &revocation, "!!").unwrap_err(), Rejected::Undecodable);
    }

    #[test]
    fn requiring_ids_refuses_only_links_without_one() {
        let revocation = Revocation::from_env("", None).requiring_install_id(true);
        let check = |json: &str| decode_checked(None, &revocation, &encode(json));
        assert_eq!(check(r#"{"osKey":"o"}"#).unwrap_err(), Rejected::NoInstallId);
        assert!(check(&format!(r#"{{"osKey":"o","iid":"{IID}","ep":0}}"#)).is_ok());
    }
}
