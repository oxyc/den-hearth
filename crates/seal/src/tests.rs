use base64::Engine;

use super::*;

const STD: base64::engine::GeneralPurpose = base64::engine::general_purpose::STANDARD;

// The fixed libsodium crypto_box_seal vector Scout, Subtitles and Reel all test (PyNaCl SealedBox). The
// interop GATE: this ciphertext, produced by a real libsodium binding, must open here, proving the seal is
// byte-identical to the Go addon and the browser bundle.
const VEC_PRIV_B64: &str = "AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8=";
const VEC_PUB_B64: &str = "j0DFrbaPJWJK5bIU6nZ6bslNgp09e14a0bpvPiE4KF8=";
const VEC_CT_B64: &str = "YVIKGV1+YCwzCoPC0WNrcle9bYR0iWhBDAsy2ylVhGmDWneqpfb/Oug5izEfwx2Q9j8UDKM2XF/u+Q9Sg1jqQeMb5RNWpLZk+81tVixiI5qFpc/zNGAwfTJSMVj+B48nCbgqRk0rqQzqniVKm1d85g==";
const VEC_PLAIN: &str = r#"{"debrid":[{"service":"realdebrid","token":"SEALED-VECTOR-OK"}]}"#;
// A segment sealing {tmdbKey, kinocheckKey} to the vector key, minted before this crate existed.
const OLD_SEG: &str = "Abo-qmntVxuOmeVa0Q5pPWju0VrZDS4aRoAP-0JHNtk7nmMcduhttWlvldwvUdXPafUGUegc4ul5J3gFVo8nEGOd8htc7he_3BihPsWtiuA5_2Du-FL5NpaNzfvqhDAHM_LAjw";

fn ring() -> Keyring {
    Keyring::from_env(VEC_PRIV_B64, "").unwrap().unwrap()
}

#[test]
fn opens_libsodium_vector() {
    let kr = ring();
    assert_eq!(kr.current_pub_base64(), VEC_PUB_B64, "derived pub must match libsodium");
    let pt = kr.open(&STD.decode(VEC_CT_B64).unwrap()).expect("open libsodium vector");
    assert_eq!(String::from_utf8(pt).unwrap(), VEC_PLAIN);
}

#[test]
fn opens_a_segment_sealed_before_the_crate() {
    let opened = open_segment(Some(&ring()), OLD_SEG).unwrap();
    assert!(opened.sealed);
    let json = String::from_utf8(opened.json).unwrap();
    assert!(json.contains("sealed-tmdb-ok") && json.contains("kc-ok"), "{json}");
}

#[test]
fn a_sealed_message_round_trips_and_is_libsodium_shaped() {
    let kr = ring();
    let ct = seal(&kr.keys[0].pub_key, b"hello").unwrap();
    assert_eq!(ct.len(), 32 + 16 + 5, "eph_pub ‖ tag ‖ message");
    assert_eq!(kr.open(&ct).as_deref(), Some(&b"hello"[..]));
    assert_ne!(ct, seal(&kr.keys[0].pub_key, b"hello").unwrap(), "a fresh ephemeral key each time");
}

#[test]
fn fails_closed() {
    let kr = ring();
    let mut ct = STD.decode(VEC_CT_B64).unwrap();
    let n = ct.len() - 1;
    ct[n] ^= 0xff;
    assert!(kr.open(&ct).is_none(), "tampered ciphertext must not open");
    assert!(kr.open(b"short").is_none());
    let other = Keyring::from_env(&STD.encode([0u8; 32]), "").unwrap().unwrap();
    assert!(other.open(&STD.decode(VEC_CT_B64).unwrap()).is_none());
}

#[test]
fn empty_current_disables_and_a_bad_current_is_fatal() {
    assert!(Keyring::from_env("", "").unwrap().is_none());
    assert!(Keyring::from_env("  ", "").unwrap().is_none());
    assert!(Keyring::from_env("not-base64-@@@", "").is_err(), "unparseable key must be fatal");
    assert!(Keyring::from_env("AAAA", "").is_err(), "a key of the wrong length must be fatal");
}

#[test]
fn a_malformed_prior_key_is_skipped_and_reported() {
    let said = std::cell::RefCell::new(Vec::new());
    let kr =
        Keyring::parse(VEC_PRIV_B64, "not-base64-@@@, ,also!!bad", &|m| said.borrow_mut().push(m.to_owned()))
            .unwrap()
            .expect("current key valid → ring built despite bad prior entries");
    assert_eq!(kr.keys.len(), 1);
    assert_eq!(said.borrow().len(), 2, "the blank entry is not reported");
    assert_eq!(String::from_utf8(kr.open(&STD.decode(VEC_CT_B64).unwrap()).unwrap()).unwrap(), VEC_PLAIN);
}

#[test]
fn a_rotated_ring_opens_what_was_sealed_to_the_old_key() {
    let old = ring();
    let ct = seal(&old.keys[0].pub_key, b"rotate-me").unwrap();
    let new_current = STD.encode([0x40u8; 32]);
    let kr = Keyring::from_env(&new_current, VEC_PRIV_B64).unwrap().unwrap();
    assert_eq!(kr.keys.len(), 2);
    assert_eq!(kr.open(&ct).as_deref(), Some(&b"rotate-me"[..]));
}

#[test]
fn key_material_is_read_leniently() {
    let raw = [0xfbu8; 32];
    let std_pad = STD.encode(raw);
    let url_nopad = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(raw);
    for s in [
        std_pad.clone(),
        std_pad.trim_end_matches('=').to_owned(),
        url_nopad,
        format!("{}\r\n{}\n", &std_pad[..10], &std_pad[10..]),
    ] {
        assert_eq!(decode_key(&s).as_deref(), Some(&raw[..]), "{s:?}");
    }
    assert!(decode_key("@@@").is_none());
}

fn encode(s: impl AsRef<[u8]>) -> String {
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(s)
}

#[test]
fn a_segment_is_read_as_json_sealed_or_plain() {
    let kr = ring();
    let plain = encode(r#"{"a":1}"#);
    let opened = open_segment(None, &plain).unwrap();
    assert!(!opened.sealed);
    assert_eq!(opened.json, br#"{"a":1}"#);
    // With a keyring a plaintext segment is refused, and says why.
    assert_eq!(open_segment(Some(&kr), &plain).err(), Some(Rejected::Plaintext));
    // Sealed with no keyring, undecodable, empty, and sealed to another key all fail closed.
    assert_eq!(open_segment(None, OLD_SEG).err(), Some(Rejected::Undecodable));
    assert_eq!(open_segment(Some(&kr), "not base64!!").err(), Some(Rejected::Undecodable));
    assert_eq!(open_segment(Some(&kr), "").err(), Some(Rejected::Undecodable));
    let other = Keyring::from_env(&STD.encode([9u8; 32]), "").unwrap().unwrap();
    assert_eq!(open_segment(Some(&other), OLD_SEG).err(), Some(Rejected::Undecodable));
    let mut framed = vec![SEALED_VERSION];
    framed.extend(seal(&kr.keys[0].pub_key, br#"{"b":2}"#).unwrap());
    let opened = open_segment(Some(&kr), &encode(&framed)).unwrap();
    assert!(opened.sealed);
    assert_eq!(opened.json, br#"{"b":2}"#);
    // A padded or hand-wrapped segment opens like it does on Scout (the shared lenient decoder).
    assert!(open_segment(None, &format!("{plain}=")).is_ok());
}

/// Bytes 0..16 as an install id, and a second one that uses both url-safe characters.
const IID: &str = "AAECAwQFBgcICQoLDA0ODw";
const OTHER_IID: &str = "_-_-_-_-_-_-_-_-_-_-_w";

#[test]
fn install_ids_are_validated_strictly() {
    for ok in [IID, OTHER_IID] {
        assert!(InstallId::parse(ok).is_some_and(|id| id.as_str() == ok), "refused {ok}");
    }
    let refused = [
        "",
        "AAECAwQFBgcICQoLDA0OD",    // 21 characters
        "AAECAwQFBgcICQoLDA0ODwA",  // 23 characters
        "AAECAwQFBgcICQoLDA0ODw==", // padded
        "AAECAwQFBgcICQoLDA0ODx",   // the same bytes with a non-zero trailing bit
        "AAECAwQFBgcICQoLDA0O+w",   // standard base64, not url-safe
        "AAECAwQFBgcICQoLDA0O/w",
        "AAECAwQFBgcICQoLDA0O w",
    ];
    for bad in refused {
        assert!(InstallId::parse(bad).is_none(), "accepted {bad:?}");
    }
}

#[test]
fn a_listed_install_and_an_old_epoch_are_refused() {
    let said = std::cell::Cell::new(0);
    let revocation =
        Revocation::parse(&format!(" {IID} ,, not-an-id"), Some("2"), &|_| said.set(said.get() + 1));
    assert_eq!(said.get(), 1, "the malformed entry is said");
    assert_eq!(revocation.revoked_count(), 1, "and skipped");
    assert_eq!(revocation.epoch(), 2);
    assert!(revocation.is_revoked(IID) && !revocation.is_revoked(OTHER_IID));

    assert_eq!(revocation.check(Some(IID), 5), Err(Rejected::Revoked { iid_prefix: "AAECAw".into() }));
    assert_eq!(revocation.check(Some(OTHER_IID), 5), Ok(()));
    assert_eq!(revocation.check(None, 1), Err(Rejected::EpochTooOld { ep: 1, epoch: 2 }));
    assert_eq!(revocation.check(None, 0), Err(Rejected::EpochTooOld { ep: 0, epoch: 2 }));
    assert_eq!(revocation.check(None, 2), Ok(()));
    // The id is the more specific answer, so it wins over a stale epoch.
    assert_eq!(revocation.check(Some(IID), 0), Err(Rejected::Revoked { iid_prefix: "AAECAw".into() }));
    // Nothing configured: a link with no id and no epoch is admitted, as every link before ids was.
    assert_eq!(Revocation::default().check(None, 0), Ok(()));
}

#[test]
fn a_refusal_names_no_more_than_the_revoked_id_s_prefix() {
    assert_eq!(
        Rejected::Revoked { iid_prefix: "AAECAw".into() }.to_string(),
        "install revoked (iid=AAECAw…)"
    );
    assert_eq!(
        Rejected::EpochTooOld { ep: 1, epoch: 2 }.to_string(),
        "install epoch too old (ep=1 < CONFIG_EPOCH=2)"
    );
    assert_eq!(Rejected::NoInstallId.to_string(), "no install id (REQUIRE_INSTALL_ID is on)");
}

#[test]
fn requiring_ids_refuses_only_links_without_one_and_never_a_scoped_config() {
    let revocation = Revocation::from_env("", None).requiring_install_id(true);
    assert!(revocation.requires_install_id());
    assert_eq!(revocation.check(None, 0), Err(Rejected::NoInstallId));
    assert_eq!(revocation.check(Some(IID), 0), Ok(()));
    // A scoped config is minted apart from the install and may carry no id.
    assert_eq!(revocation.check_scoped(None, 0), Ok(()));
    // Off by default: links from before ids keep working until the operator turns it on.
    assert!(!Revocation::from_env("", None).requires_install_id());
}

#[test]
fn a_scoped_config_is_still_subject_to_named_revocation_and_the_epoch() {
    let revocation = Revocation::from_env(IID, Some("2")).requiring_install_id(true);
    assert_eq!(revocation.check_scoped(Some(IID), 9), Err(Rejected::Revoked { iid_prefix: "AAECAw".into() }));
    assert_eq!(revocation.check_scoped(None, 1), Err(Rejected::EpochTooOld { ep: 1, epoch: 2 }));
}

#[test]
fn the_order_is_id_then_epoch_then_missing_id() {
    let revocation = Revocation::from_env(IID, Some("2")).requiring_install_id(true);
    assert_eq!(revocation.check(Some(IID), 0), Err(Rejected::Revoked { iid_prefix: "AAECAw".into() }));
    assert_eq!(revocation.check(None, 0), Err(Rejected::EpochTooOld { ep: 0, epoch: 2 }));
    assert_eq!(revocation.check(None, 2), Err(Rejected::NoInstallId));
}

fn slice(json: &str, member: &str) -> Option<serde_json::Value> {
    addon::slice_json(json.as_bytes(), member).map(|j| serde_json::from_slice(&j).unwrap())
}

#[test]
fn a_service_reads_its_member_with_the_install_identity_put_in() {
    let combined = format!(
        r#"{{"iid":"{IID}","ep":3,"scout":{{"debrid":[]}},"subs":{{"osKey":"o"}},"reel":{{"tmdbKey":"t"}}}}"#
    );
    assert_eq!(slice(&combined, "subs").unwrap(), serde_json::json!({"osKey": "o", "iid": IID, "ep": 3}));
    assert_eq!(slice(&combined, "reel").unwrap(), serde_json::json!({"tmdbKey": "t", "iid": IID, "ep": 3}));
    // No identity at the top: none in the member either.
    assert_eq!(slice(r#"{"reel":{"tmdbKey":"t"}}"#, "reel").unwrap(), serde_json::json!({"tmdbKey": "t"}));
}

#[test]
fn a_member_cannot_name_an_install_of_its_own() {
    let combined = format!(r#"{{"iid":"{IID}","scout":{{"IID":"{OTHER_IID}","iid":"{OTHER_IID}","Ep":9}}}}"#);
    assert_eq!(slice(&combined, "scout").unwrap(), serde_json::json!({"iid": IID}));
}

#[test]
fn a_missing_or_malformed_member_is_no_config() {
    for json in [r#"{"subs":{"osKey":"o"}}"#, r#"{"reel":"tmdb"}"#, r#"{"reel":null}"#, "[1]", "not json"] {
        assert!(slice(json, "reel").is_none(), "{json}");
    }
    // A service's own config is not a combined one: its fields are not a member.
    assert!(slice(r#"{"tmdbKey":"t"}"#, "reel").is_none());
}

#[test]
fn a_combined_segment_opens_on_the_addon_s_terms_and_fails_closed() {
    let kr = ring();
    let mut framed = vec![SEALED_VERSION];
    framed.extend(seal(&kr.keys[0].pub_key, br#"{"ep":1,"reel":{"tmdbKey":"t"}}"#).unwrap());
    let segment = encode(&framed);
    let addon = Addon {
        keyring: Some(kr.clone()),
        key_invalid: false,
        revocation: Revocation::default(),
        public_base: None,
        slice: "reel",
    };
    let opened = addon.open(&segment).unwrap();
    assert!(opened.sealed);
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&opened.json).unwrap(),
        serde_json::json!({"tmdbKey": "t", "ep": 1})
    );
    // A member this install does not have.
    assert_eq!(Addon { slice: "subs", ..addon.clone() }.open(&segment).err(), Some(Rejected::Undecodable));
    // Plaintext while sealing is on, and a sealed segment with no keyring.
    assert_eq!(addon.open(&encode(r#"{"reel":{"tmdbKey":"t"}}"#)).err(), Some(Rejected::Plaintext));
    assert_eq!(Addon { keyring: None, ..addon.clone() }.open(&segment).err(), Some(Rejected::Undecodable));
}

#[test]
fn the_base_is_the_public_one_or_the_request_s_origin_under_the_mount() {
    let mut addon = Addon {
        keyring: None,
        key_invalid: false,
        revocation: Revocation::default(),
        public_base: Some("https://node.example/den/".into()),
        slice: "subs",
    };
    assert_eq!(addon.base("http://ignored"), "https://node.example/den");
    addon.public_base = None;
    assert_eq!(addon.base("http://lan:8090/"), "http://lan:8090/den");
}

#[test]
fn a_derived_ring_is_another_ring_that_rotates_with_its_source() {
    let source = Keyring::from_env(&STD.encode([0x40u8; 32]), VEC_PRIV_B64).unwrap().unwrap();
    let derived = source.derive(b"den-test");
    assert_eq!(derived.keys.len(), 2);
    // Deterministic, and specific to the info string.
    assert_eq!(derived.current_pub_base64(), source.derive(b"den-test").current_pub_base64());
    assert_ne!(derived.current_pub_base64(), source.derive(b"other").current_pub_base64());
    // Neither ring opens what was sealed to the other.
    let to_derived = seal(&derived.keys[0].pub_key, b"x").unwrap();
    let to_source = seal(&source.keys[0].pub_key, b"x").unwrap();
    assert!(source.open(&to_derived).is_none());
    assert!(derived.open(&to_source).is_none());
    assert_eq!(derived.open(&to_derived).as_deref(), Some(&b"x"[..]));
    // A link sealed to the derived key of a rotated-out source key still opens.
    let old = ring().derive(b"den-test");
    assert_eq!(derived.open(&seal(&old.keys[0].pub_key, b"old").unwrap()).as_deref(), Some(&b"old"[..]));
}

#[test]
fn an_epoch_that_is_not_an_integer_in_range_enforces_zero() {
    assert_eq!(Revocation::from_env("", None).epoch(), 0);
    assert_eq!(Revocation::from_env("", Some("two")).epoch(), 0);
    assert_eq!(Revocation::from_env("", Some("-1")).epoch(), 0);
    assert_eq!(Revocation::from_env("", Some(" 7 ")).epoch(), 7);
    assert_eq!(Revocation::from_env("", Some(&MAX_EPOCH.to_string())).epoch(), MAX_EPOCH);
    assert_eq!(Revocation::from_env("", Some(&(MAX_EPOCH + 1).to_string())).epoch(), 0);
}
