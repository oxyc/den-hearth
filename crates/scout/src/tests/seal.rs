//! Go: `internal/scout/seal_test.go` (143 lines). Work package 6.

use base64::Engine;

use super::core::blob;
use crate::config::decode_config;
use crate::seal::{parse_seal_keyring, seal, SEALED_VERSION};
use crate::util::b64url_encode;

// A fixed libsodium crypto_box_seal vector, generated with PyNaCl (SealedBox) — see
// docs/SEALED-CONFIG.md. This is the cross-language interop GATE: the Go `open` MUST decrypt a
// ciphertext produced by a real libsodium binding (the same one /configure uses via libsodium.js).
pub(crate) const VEC_PRIV_B64: &str = "AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8="; // recipient X25519 private key (seed 00..1f)
const VEC_PUB_B64: &str = "j0DFrbaPJWJK5bIU6nZ6bslNgp09e14a0bpvPiE4KF8="; // its public key
const VEC_CT_B64: &str = "YVIKGV1+YCwzCoPC0WNrcle9bYR0iWhBDAsy2ylVhGmDWneqpfb/Oug5izEfwx2Q9j8UDKM2XF/u+Q9Sg1jqQeMb5RNWpLZk+81tVixiI5qFpc/zNGAwfTJSMVj+B48nCbgqRk0rqQzqniVKm1d85g==";
const VEC_PLAIN: &str = r#"{"debrid":[{"service":"realdebrid","token":"SEALED-VECTOR-OK"}]}"#;

#[test]
fn seal_interop_vector() {
    let kr = parse_seal_keyring(VEC_PRIV_B64, "").expect("keyring").expect("sealing on");
    // Our derived public key must match libsodium's.
    assert_eq!(kr.current_pub_base64(), VEC_PUB_B64, "derived pub");
    let ct = base64::engine::general_purpose::STANDARD.decode(VEC_CT_B64).unwrap();
    let pt = kr.open(&ct).expect("open libsodium vector");
    assert_eq!(String::from_utf8(pt).unwrap(), VEC_PLAIN, "plaintext");
}

// A fixed segment MINTED by the /configure browser bundle (tweetnacl + blake2b crypto_box_seal) to
// vecPub — the JS→Go interop gate, complementing the PyNaCl (libsodium→Go) vector above. Regenerate with
// scratch/sealsrc/entry.js's denSeal() if the wire format ever changes.
const VEC_JS_SEG: &str = "AZx6VE32YvP8XAhp0oq6d-gAODZb0o9i7TKBCHudMDxiregenxybtd2DLQc4VKyX7wAeyiAu2LHHYs2k3Pj51dqEvZzc-Fmaa2_uvca4IMMyHMMP6hANKv6f9F8TF7vYyrLDPvdOjDK7wzl8Kg";

#[test]
fn seal_js_interop_vector() {
    let kr = parse_seal_keyring(VEC_PRIV_B64, "").unwrap().unwrap();
    let c = decode_config(Some(&kr), VEC_JS_SEG);
    let ok = matches!(&c, Some(c) if c.debrid.len() == 1 && c.debrid[0].token == "JS-SEALED-OK");
    assert!(ok, "browser-minted segment didn't open: {c:?}");
}

#[test]
fn seal_round_trip() {
    let kr = parse_seal_keyring(VEC_PRIV_B64, "").expect("keyring").expect("sealing on");
    let msg = br#"{"hello":"world"}"#;
    let ct = seal(&kr.keys[0].pub_key, msg).expect("seal");
    let pt = kr.open(&ct);
    assert!(matches!(&pt, Ok(p) if p == msg), "round-trip: pt={pt:?}");
}

#[test]
fn seal_fails_closed() {
    let kr = parse_seal_keyring(VEC_PRIV_B64, "").unwrap().unwrap();
    let ct = seal(&kr.keys[0].pub_key, b"secret").unwrap();

    // Tampered ciphertext → error, no plaintext (never fall through to a partial config).
    let mut bad = ct.clone();
    let last = bad.len() - 1;
    bad[last] ^= 0xff;
    assert!(kr.open(&bad).is_err(), "tampered blob opened");
    // Too short.
    assert!(kr.open(b"nope").is_err(), "short blob should fail");
    // Wrong key (different recipient) → cannot open.
    let zero_key = base64::engine::general_purpose::STANDARD.encode([0u8; 32]);
    let other = parse_seal_keyring(&zero_key, "").unwrap().unwrap();
    assert!(other.open(&ct).is_err(), "wrong-key open should fail");
}

#[test]
fn seal_keyring_rotation() {
    // A blob sealed to an OLD key must still open once that key is demoted to the "prev" list.
    let old_kr = parse_seal_keyring(VEC_PRIV_B64, "").unwrap().unwrap();
    let ct = seal(&old_kr.keys[0].pub_key, b"rotate-me").unwrap();

    let new_current = base64::engine::general_purpose::STANDARD.encode(bytes_seq(0x40, 32)); // a different current key
    let kr = parse_seal_keyring(&new_current, VEC_PRIV_B64).expect("keyring").unwrap(); // old key kept as prev
    let pt = kr.open(&ct);
    assert!(matches!(&pt, Ok(p) if p == b"rotate-me"), "rotation open: pt={pt:?}");
    assert_eq!(kr.keys.len(), 2, "keyring size (current + prev)");
}

#[test]
fn decode_config_sealed() {
    let kr = parse_seal_keyring(VEC_PRIV_B64, "").unwrap().unwrap();
    let cfg_json = br#"{"debrid":[{"service":"realdebrid","token":"rd-secret"}],"resultCap":20}"#;
    let sealed = seal(&kr.keys[0].pub_key, cfg_json).unwrap();
    let mut framed = vec![SEALED_VERSION];
    framed.extend_from_slice(&sealed);
    let seg = b64url_encode(&framed); // the URL path segment

    // Sealed segment decrypts + validates to the same config a plaintext one would.
    let c = decode_config(Some(&kr), &seg);
    assert!(
        matches!(&c, Some(c) if c.debrid.len() == 1 && c.debrid[0].token == "rd-secret"),
        "sealed decode: {c:?}"
    );
    // Fail CLOSED: sealed segment but no keyring configured.
    assert!(decode_config(None, &seg).is_none(), "sealed blob with no keyring must fail closed");
    // Fail CLOSED: sealed to a different recipient key.
    let zero_key = base64::engine::general_purpose::STANDARD.encode([0u8; 32]);
    let other = parse_seal_keyring(&zero_key, "").unwrap().unwrap();
    assert!(decode_config(Some(&other), &seg).is_none(), "sealed to a different key must fail closed");
    // With a keyring, plaintext is refused: every issued install is sealed, and /config-key is public, so a
    // plaintext config is one anyone could have minted. Without one it is the only form, and decodes.
    let plain = blob(r#"{"debrid":[{"service":"torbox","token":"t"}]}"#);
    assert!(
        decode_config(Some(&kr), &plain).is_none(),
        "a plaintext config must be refused while sealing is on"
    );
    assert!(decode_config(None, &plain).is_some(), "a plaintext config must decode with sealing off");
}

/// Go `bytesSeq`: `n` bytes counting up from `start`. Shared with the ticket and install-id tests.
pub(crate) fn bytes_seq(start: u8, n: usize) -> Vec<u8> {
    (0..n).map(|i| start.wrapping_add(i as u8)).collect()
}
