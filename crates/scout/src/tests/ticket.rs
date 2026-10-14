//! Go: `internal/scout/ticket_test.go` (541 lines). Work package 6.
//!
//! Pins the play-ticket wire format: the legacy payloads built below are the byte shape older tickets carry,
//! and must keep opening. Tests that drive `handler::serve`, the settings loader or the decision log need
//! work packages 4 and 5; they are ignored, and their bodies are ported in full inside a compiled-out item
//! so they only need to parse until the owner enables them.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use base64::Engine;
use chacha20poly1305::aead::Aead;
use chacha20poly1305::{KeyInit, XChaCha20Poly1305, XNonce};

use super::core::blob;
use super::seal::{bytes_seq, VEC_PRIV_B64};
use crate::config::{
    decode_config, Config, DebridAccount, DebridService, INSTALL_ID_BYTES, MAX_CONFIG_BLOB, MAX_CONFIG_EPOCH,
    MAX_DEBRID_ACCOUNTS,
};
use crate::play::{encode_play_token, PlayTarget};
use crate::seal::{parse_seal_keyring, SealKeyring};
use crate::ticket::{TicketError, TicketKeys};
use crate::util::{b64url_decode, b64url_encode};

fn test_iid() -> String {
    b64url_encode(&bytes_seq(0x00, INSTALL_ID_BYTES))
}

fn other_iid() -> String {
    b64url_encode(&bytes_seq(0x10, INSTALL_ID_BYTES))
}

// Unterminated, so a test can append iid/ep/scope before closing it.
const TICKET_CFG: &str =
    r#"{"debrid":[{"service":"torbox","token":"tb-secret"}],"indexers":["torrentio"],"cachedOnly":true"#;

pub(crate) fn ticket_keyring() -> SealKeyring {
    parse_seal_keyring(VEC_PRIV_B64, "").expect("keyring").expect("sealing on")
}

fn other_keyring(prev: &str) -> SealKeyring {
    let current = base64::engine::general_purpose::STANDARD.encode(bytes_seq(0x40, 32));
    parse_seal_keyring(&current, prev).expect("keyring").expect("sealing on")
}

/// Go `newTicketKeys(kr)`, for a keyring that is set.
pub(crate) fn ticket_keys(kr: &SealKeyring) -> TicketKeys {
    TicketKeys::new(Some(kr)).expect("a keyring turns tickets on")
}

/// Go `validBlob` (handler_test.go:18): the config every handler test starts from.
pub(crate) fn valid_blob() -> String {
    blob(
        r#"{"debrid":[{"service":"torbox","token":"tb-secret"}],"indexers":["torrentio"],"filters":{"excludeCam":true},"cachedOnly":true,"resultCap":20}"#,
    )
}

fn unix_secs(t: SystemTime) -> i64 {
    t.duration_since(UNIX_EPOCH).expect("after the epoch").as_secs() as i64
}

/// A ticket in the wire shape older builds minted, sealed under `key` with an all-zero nonce, as Go's
/// `aeads[0].Seal(nonce, nonce, old, nil)` does. The payload is written out field by field so the test does
/// not depend on the current encoder.
fn legacy_ticket(key: &[u8; 32], payload: &[u8]) -> String {
    let nonce = [0u8; 24];
    let aead = XChaCha20Poly1305::new_from_slice(key).expect("32-byte key");
    let sealed = aead.encrypt(XNonce::from_slice(&nonce), payload).expect("seal");
    let mut out = nonce.to_vec();
    out.extend_from_slice(&sealed);
    b64url_encode(&out)
}

// A ticket opens to exactly what was minted: the accounts, the target, and the install id and epoch.
#[test]
fn play_ticket_round_trip() {
    let kr = ticket_keyring();
    let tk = ticket_keys(&kr);
    let config = Config {
        debrid: vec![
            DebridAccount { service: DebridService::TorBox, token: "tb".to_owned() },
            DebridAccount { service: DebridService::RealDebrid, token: "rd".to_owned() },
        ],
        iid: test_iid(),
        epoch: 3,
        ..Default::default()
    };
    let target = PlayTarget {
        info_hash: "a".repeat(40),
        file_idx: Some(4),
        season: Some(1),
        episode: Some(2),
        ..Default::default()
    };
    let ticket = tk.mint(&config, &target, SystemTime::now() + Duration::from_secs(3600));
    let (got_config, got_target) = tk.open(&ticket, SystemTime::now()).expect("open");
    assert_eq!(got_config, config, "config");
    assert_eq!(got_target, target, "target");
}

// A ticket carries the indexer's release size when the list had one, and opens with 0 when it did not —
// including a ticket minted before the field existed, whose payload has no `z` at all.
#[test]
fn play_ticket_carries_the_release_size() {
    let kr = ticket_keyring();
    let tk = ticket_keys(&kr);
    let config = Config {
        debrid: vec![DebridAccount { service: DebridService::TorBox, token: "tb".to_owned() }],
        ..Default::default()
    };
    let exp = SystemTime::now() + Duration::from_secs(3600);
    for want in [16_000_000_000_i64, 0] {
        let target = PlayTarget {
            info_hash: "a".repeat(40),
            file_idx: Some(0),
            release_size: want,
            ..Default::default()
        };
        let (_, got) = tk.open(&tk.mint(&config, &target, exp), SystemTime::now()).expect("open");
        assert_eq!(got, target, "size {want}");
    }

    let old = serde_json::json!({
        "h": "a".repeat(40),
        "f": 0,
        "d": [["torbox", "tb"]],
        "x": unix_secs(exp),
    });
    let ticket = legacy_ticket(&tk.keys[0], &serde_json::to_vec(&old).expect("json"));
    let (_, got) = tk.open(&ticket, SystemTime::now()).expect("a ticket from before the size");
    assert!(got.release_size == 0 && got.file_idx == Some(0), "a ticket from before the size: {got:?}");
}

// A ticket carries the release title and title id purely for the play/probe/cancel decision lines
// (decisionlog.go) — round-tripped like every other field, and absent (zero value) on a ticket minted
// before they existed, the same backward-compat shape the release size above already has.
#[test]
fn play_ticket_carries_title_and_imdb_for_logging() {
    let kr = ticket_keyring();
    let tk = ticket_keys(&kr);
    let config = Config {
        debrid: vec![DebridAccount { service: DebridService::TorBox, token: "tb".to_owned() }],
        ..Default::default()
    };
    let exp = SystemTime::now() + Duration::from_secs(3600);
    let target = PlayTarget {
        info_hash: "a".repeat(40),
        file_idx: Some(0),
        title: "Movie.2024.2160p.BluRay.REMUX".to_owned(),
        imdb: "tt1234567".to_owned(),
        ..Default::default()
    };
    let (_, got) = tk.open(&tk.mint(&config, &target, exp), SystemTime::now()).expect("open");
    assert_eq!(got, target);

    let old = serde_json::json!({
        "h": "a".repeat(40),
        "f": 0,
        "d": [["torbox", "tb"]],
        "x": unix_secs(exp),
    });
    let ticket = legacy_ticket(&tk.keys[0], &serde_json::to_vec(&old).expect("json"));
    let (_, got) = tk.open(&ticket, SystemTime::now()).expect("a ticket minted before title/imdb existed");
    assert!(
        got.title.is_empty() && got.imdb.is_empty(),
        "a ticket minted before title/imdb existed should open with both empty: {got:?}"
    );
}

// Anything that is not a ticket this addon minted, unaltered, is refused.
#[test]
fn play_ticket_refuses_what_does_not_open() {
    let kr = ticket_keyring();
    let tk = ticket_keys(&kr);
    let config = Config {
        debrid: vec![DebridAccount { service: DebridService::TorBox, token: "tb".to_owned() }],
        ..Default::default()
    };
    let target = PlayTarget { info_hash: "a".repeat(40), ..Default::default() };
    let exp = SystemTime::now() + Duration::from_secs(3600);
    let good = tk.mint(&config, &target, exp);
    let raw = b64url_decode(&good).expect("minted tickets are base64url");
    let flipped = |i: usize| {
        let mut b = raw.clone();
        b[i] ^= 1;
        b64url_encode(&b)
    };
    let other = ticket_keys(&other_keyring(""));
    let cases: [(&str, String); 9] = [
        ("a flipped nonce byte", flipped(0)),
        ("a flipped ciphertext byte", flipped(raw.len() / 2)),
        ("a flipped tag byte", flipped(raw.len() - 1)),
        ("truncated", good[..good.len() - 4].to_owned()),
        ("not base64", "!!!".to_owned()),
        ("empty", String::new()),
        ("over the size cap", "A".repeat(MAX_CONFIG_BLOB + 4)),
        ("another key's ticket", other.mint(&config, &target, exp)),
        ("a legacy play token", encode_play_token(&target)),
    ];
    for (name, ticket) in cases {
        let r = tk.open(&ticket, SystemTime::now());
        assert!(matches!(&r, Err(TicketError::Bad)), "{name}: err = {r:?}, want errTicketBad");
    }
}

// A key rotated into CONFIG_KEYS_PREV still opens the tickets it minted, and stops once it is dropped.
#[test]
fn play_ticket_opens_with_a_prior_key() {
    let ticket = ticket_keys(&ticket_keyring()).mint(
        &Config {
            debrid: vec![DebridAccount { service: DebridService::TorBox, token: "tb".to_owned() }],
            ..Default::default()
        },
        &PlayTarget { info_hash: "a".repeat(40), ..Default::default() },
        SystemTime::now() + Duration::from_secs(3600),
    );
    if let Err(e) = ticket_keys(&other_keyring(VEC_PRIV_B64)).open(&ticket, SystemTime::now()) {
        panic!("a ticket from the prior key: {e:?}");
    }
    let r = ticket_keys(&other_keyring("")).open(&ticket, SystemTime::now());
    assert!(
        matches!(&r, Err(TicketError::Bad)),
        "with the prior key dropped: err = {r:?}, want errTicketBad"
    );
}

// The largest config the field caps admit still mints a ticket under the size cap open() enforces.
#[test]
fn play_ticket_the_largest_config_fits() {
    let accounts: Vec<DebridAccount> = (0..MAX_DEBRID_ACCOUNTS)
        .map(|_| DebridAccount { service: DebridService::Premiumize, token: "t".repeat(512) })
        .collect();
    let kr = ticket_keyring();
    let tk = ticket_keys(&kr);
    let ticket = tk.mint(
        &Config { debrid: accounts, iid: test_iid(), epoch: MAX_CONFIG_EPOCH, ..Default::default() },
        &PlayTarget {
            info_hash: "a".repeat(40),
            file_idx: Some(9999),
            season: Some(99),
            episode: Some(999),
            release_size: 1 << 50,
            ..Default::default()
        },
        SystemTime::now() + Duration::from_secs(3600),
    );
    if let Err(e) = tk.open(&ticket, SystemTime::now()) {
        panic!(
            "the largest legitimate ticket ({} bytes) does not open under the {MAX_CONFIG_BLOB} cap: {e:?}",
            ticket.len()
        );
    }
    eprintln!("largest legitimate ticket: {} bytes, against a {MAX_CONFIG_BLOB} cap", ticket.len());
}

// The account cap is a ceiling, not a target — see core.rs `decode_config_keeps_every_account_up_to_the_ceiling`.
// Also pins the install-id and epoch validation of a full config, which the handler tests rely on.
#[test]
fn config_install_id_and_epoch() {
    let account = r#"{"debrid":[{"service":"torbox","token":"t"}],"#;
    let c = decode_config(None, &blob(&format!("{account}\"iid\":\"{}\",\"ep\":7}}", test_iid())));
    assert!(matches!(&c, Some(c) if c.iid == test_iid() && c.epoch == 7), "got {c:?}");
    let c = decode_config(None, &blob(&format!("{account}\"cachedOnly\":true}}")));
    assert!(matches!(&c, Some(c) if c.iid.is_empty() && c.epoch == 0), "absent: got {c:?}");

    // The install id and epoch are strictly validated; a config carrying a bad one is refused, not trimmed.
    let iid = test_iid();
    let cases = [
        ("a short id", format!(r#""iid":"{}""#, &iid[..21])),
        ("a long id", format!(r#""iid":"{iid}A""#)),
        ("a padded id", format!(r#""iid":"{iid}==""#)),
        ("standard base64", format!(r#""iid":"+/{}""#, &iid[2..])),
        ("a non-canonical id", format!(r#""iid":"{}B""#, &iid[..21])),
        ("an empty id", r#""iid":"""#.to_owned()),
        ("a numeric id", r#""iid":5"#.to_owned()),
        ("a negative epoch", r#""ep":-1"#.to_owned()),
        ("a fractional epoch", r#""ep":1.5"#.to_owned()),
        ("a string epoch", r#""ep":"1""#.to_owned()),
        ("an epoch past the ceiling", r#""ep":1e12"#.to_owned()),
    ];
    for (name, extra) in cases {
        assert!(decode_config(None, &blob(&format!("{account}{extra}}}"))).is_none(), "{name} was accepted");
    }
}

// Handler-level helpers. They need `handler::serve`, `Deps::for_test` and the `captureLog` port, which are
// packages 4 and 6, so they stay compiled out until those land. Names follow the assumptions in the
// coordinator's note; package 4 reconciles them.
#[cfg(any())]
mod pending_handler {
    use super::*;
    use crate::deps::Deps;
    use crate::handler::serve;

    /// Go `sealedConfig` (availability_test.go:42).
    pub(super) fn sealed_config(kr: &SealKeyring, cfg_json: &str) -> String {
        let sealed = seal(&kr.keys[0].pub_key, cfg_json.as_bytes()).expect("seal");
        let mut framed = vec![SEALED_VERSION];
        framed.extend_from_slice(&sealed);
        b64url_encode(&framed)
    }

    /// Go `do` / `doMethod` (handler_test.go:50-62): a request to the scout origin, through `serve`.
    pub(super) fn do_method(deps: &Deps, method: &str, path: &str, body: &str) -> http::Response<String> {
        let req = http::Request::builder()
            .method(method)
            .uri(format!("https://scout.example{path}"))
            .header("content-type", "application/json")
            .body(body.to_owned())
            .expect("request");
        serve(deps, req)
    }

    pub(super) fn get(deps: &Deps, path: &str) -> http::Response<String> {
        do_method(deps, "GET", path, "")
    }

    /// Go `streamURLs` (ticket_test.go:~60): the play URLs of a movie stream list for a config segment.
    pub(super) fn stream_urls(deps: &Deps, config: &str) -> Vec<String> {
        let rr = get(deps, &format!("/{config}/stream/movie/tt1234567.json"));
        assert_eq!(rr.status(), 200, "stream: {} {}", rr.status(), rr.body());
        let body: serde_json::Value = serde_json::from_str(rr.body()).expect("json");
        let streams = body["streams"].as_array().expect("streams");
        assert!(!streams.is_empty(), "no streams");
        streams.iter().map(|s| s["url"].as_str().unwrap_or_default().to_owned()).collect()
    }

    /// Go `pathOf`: the path of a URL, without its query.
    pub(super) fn path_of(u: &str) -> String {
        let after = u.split_once("://").map_or(u, |(_, rest)| rest);
        let path = after.find('/').map_or("/", |i| &after[i..]);
        path.split('?').next().unwrap_or("/").to_owned()
    }

    /// Go `availabilityOf` (availability_test.go:15): the verdict for one id through `/<config>/availability`.
    pub(super) fn availability_of(deps: &Deps, config: &str, id: &str) -> String {
        let rr =
            do_method(deps, "POST", &format!("/{config}/availability"), &format!(r#"{{"ids":["{id}"]}}"#));
        assert_eq!(rr.status(), 200, "availability: {}", rr.body());
        let body: serde_json::Value = serde_json::from_str(rr.body()).expect("json");
        body["availability"][id].as_str().unwrap_or_default().to_owned()
    }
}

// Needs `handler::serve`, `Deps::for_test` and the `/p/<ticket>` route (work package 4).
#[test]
#[ignore = "needs handler::serve / Deps::for_test / the /p/ route (package 4)"]
fn play_ticket_an_expired_ticket_is_gone() {
    // Enable when package 4 lands: move this body up and delete the wrapper.
    #[cfg(any())]
    fn pending() {
        use super::pending_handler::{get, sealed_config};
        use crate::deps::Deps;
        let kr = ticket_keyring();
        let tk = ticket_keys(&kr);
        let config =
            decode_config(Some(&kr), &sealed_config(&kr, &format!("{TICKET_CFG}}}"))).unwrap_or_default();
        let expired = tk.mint(
            &config,
            &PlayTarget { info_hash: "a".repeat(40), file_idx: Some(0), ..Default::default() },
            SystemTime::now() - Duration::from_secs(1),
        );
        let r = tk.open(&expired, SystemTime::now());
        assert!(matches!(&r, Err(TicketError::Expired)), "err = {r:?}, want errTicketExpired");
        let deps = Deps::for_test(|d| d.seal_keyring = Some(kr.clone()));
        for q in ["", "?probe=1", "?fresh=1"] {
            let rr = get(&deps, &format!("/p/{expired}{q}"));
            assert_eq!(rr.status(), 410, "/p/<expired>{q}: want 410");
        }
    }
    todo!("pending: ticket_test.go:~106 TestPlayTicket_anExpiredTicketIsGone")
}

// Needs `handler::serve`, `Deps::for_test` and the stream list (work package 4).
#[test]
#[ignore = "needs handler::serve / Deps::for_test / the stream list (package 4)"]
fn play_ticket_a_stream_list_names_tickets_that_play() {
    // Enable when package 4 lands: move this body up and delete the wrapper.
    #[cfg(any())]
    fn pending() {
        use super::pending_handler::{get, path_of, sealed_config, stream_urls};
        use crate::deps::Deps;
        let kr = ticket_keyring();
        let deps = Deps::for_test(|d| d.seal_keyring = Some(kr.clone()));
        let seg = sealed_config(&kr, &format!(r#"{TICKET_CFG},"iid":"{}"}}"#, test_iid()));
        let urls = stream_urls(&deps, &seg);
        for u in &urls {
            let p = path_of(u);
            assert!(
                p.starts_with("/p/")
                    && p.matches('/').count() == 2
                    && !u.contains(&seg)
                    && !u.contains("tb-secret"),
                "play url: {u}"
            );
        }
        let play = path_of(&urls[0]);
        let rr = get(&deps, &play);
        let location = rr.headers().get("location").and_then(|v| v.to_str().ok()).unwrap_or_default();
        assert!(
            rr.status() == 302 && location.starts_with("https://cdn.torbox/"),
            "GET /p/<ticket>: {} location={location:?}",
            rr.status()
        );
        let head = super::pending_handler::do_method(&deps, "HEAD", &play, "");
        assert_eq!(head.status(), 405, "HEAD /p/<ticket>: want 405");
    }
    todo!("pending: ticket_test.go:~121 TestPlayTicket_aStreamListNamesTicketsThatPlay")
}

// Needs `handler::serve` and the legacy `/<config>/play/` route (work package 4).
#[test]
#[ignore = "needs handler::serve and the legacy play route (package 4)"]
fn play_ticket_no_config_key_keeps_the_legacy_url() {
    // Enable when package 4 lands: move this body up and delete the wrapper.
    #[cfg(any())]
    fn pending() {
        use super::pending_handler::{get, path_of, stream_urls, valid_blob};
        use crate::deps::Deps;
        let deps = Deps::for_test(|_| {});
        let play = path_of(&stream_urls(&deps, &valid_blob())[0]);
        assert!(play.starts_with(&format!("/{}/play/", valid_blob())), "play url without a key: {play}");
        assert_eq!(get(&deps, &play).status(), 302, "legacy play");
        let ticket = ticket_keys(&ticket_keyring()).mint(
            &Config {
                debrid: vec![DebridAccount { service: DebridService::TorBox, token: "tb".to_owned() }],
                ..Default::default()
            },
            &PlayTarget { info_hash: "a".repeat(40), ..Default::default() },
            SystemTime::now() + Duration::from_secs(3600),
        );
        assert_eq!(get(&deps, &format!("/p/{ticket}")).status(), 404, "/p/ without a key: want 404");
    }
    todo!("pending: ticket_test.go:~144 TestPlayTicket_noConfigKeyKeepsTheLegacyURL")
}

// Needs `handler::serve` and the scope check on the stream route (work package 4).
#[test]
#[ignore = "needs handler::serve and the scope check (package 4)"]
fn play_ticket_the_scoped_config_never_mints_one() {
    // Enable when package 4 lands: move this body up and delete the wrapper.
    #[cfg(any())]
    fn pending() {
        use super::pending_handler::{get, sealed_config};
        use crate::deps::Deps;
        let kr = ticket_keyring();
        let scoped = sealed_config(&kr, &format!(r#"{TICKET_CFG},"scope":"availability"}}"#));
        let deps = Deps::for_test(|d| d.seal_keyring = Some(kr.clone()));
        for path in [
            format!("/{scoped}/stream/movie/tt1234567.json"),
            format!("/{scoped}/stream/movie/tt1234567.json?debug=1"),
        ] {
            let rr = get(&deps, &path);
            assert!(
                rr.status() == 403 && !rr.body().contains("/p/"),
                "scoped stream: {} {}",
                rr.status(),
                rr.body()
            );
        }
    }
    todo!("pending: ticket_test.go:~163 TestPlayTicket_theScopedConfigNeverMintsOne")
}

// Needs `handler::serve`, the reqlog port's `capture_log` and the revocation refusal (packages 4 and 6).
#[test]
#[ignore = "needs handler::serve, capture_log and the revocation refusal (packages 4, 6)"]
fn revoked_install_is_refused_everywhere() {
    // Enable when package 4 lands: move this body up and delete the wrapper.
    #[cfg(any())]
    fn pending() {
        use super::pending_handler::{get, path_of, sealed_config, stream_urls};
        use crate::deps::Deps;
        use crate::tests::reqlog::{capture_log, forget_logged};
        let kr = ticket_keyring();
        let revoked = sealed_config(&kr, &format!(r#"{TICKET_CFG},"iid":"{}"}}"#, test_iid()));
        let live = sealed_config(&kr, &format!(r#"{TICKET_CFG},"iid":"{}"}}"#, other_iid()));
        // Minted before the revocation, as a client would still hold them.
        let before = Deps::for_test(|d| d.seal_keyring = Some(kr.clone()));
        let revoked_ticket = path_of(&stream_urls(&before, &revoked)[0]);
        let live_ticket = path_of(&stream_urls(&before, &live)[0]);

        forget_logged("install-revoked");
        let logs = capture_log(|| {
            let deps = Deps::for_test(|d| {
                d.seal_keyring = Some(kr.clone());
                d.revoked_installs = vec![test_iid()];
            });
            let token = encode_play_token(&PlayTarget {
                info_hash: "a".repeat(40),
                file_idx: Some(0),
                ..Default::default()
            });
            let cases: Vec<(String, u16)> = vec![
                (format!("/{revoked}/manifest.json"), 400),
                (format!("/{revoked}/stream/movie/tt7654321.json"), 400),
                (format!("/{revoked}/play/{token}"), 400),
                (revoked_ticket.clone(), 400),
                (format!("/{live}/manifest.json"), 200),
                (format!("/{live}/play/{token}"), 302),
                (live_ticket.clone(), 302),
            ];
            for (path, want) in cases {
                let rr = get(&deps, &path);
                assert_eq!(rr.status(), want, "{path}");
            }
            let rr = super::pending_handler::do_method(
                &deps,
                "POST",
                &format!("/{revoked}/availability"),
                r#"{"ids":[]}"#,
            );
            assert_eq!(rr.status(), 400, "revoked availability");
        });
        assert!(
            logs.contains(&format!("config refused: install revoked (iid {}…)", &test_iid()[..6])),
            "no revocation line in:\n{logs}"
        );
        assert!(
            !logs.contains(&test_iid()) && !logs.contains(&other_iid()[..6]),
            "the log carries more of an install id than it may:\n{logs}"
        );
    }
    todo!("pending: ticket_test.go:~183 TestRevokedInstall_isRefusedEverywhere")
}

// Needs `handler::serve` and the legacy play route (work package 4).
#[test]
#[ignore = "needs handler::serve and the legacy play route (package 4)"]
fn revoked_install_a_config_without_an_id_still_works() {
    // Enable when package 4 lands: move this body up and delete the wrapper.
    #[cfg(any())]
    fn pending() {
        use super::pending_handler::{get, path_of, stream_urls, valid_blob};
        use crate::deps::Deps;
        let deps = Deps::for_test(|d| d.revoked_installs = vec![test_iid()]);
        let play = path_of(&stream_urls(&deps, &valid_blob())[0]);
        assert_eq!(get(&deps, &play).status(), 302, "legacy play");
    }
    todo!("pending: ticket_test.go:~246 TestRevokedInstall_aConfigWithoutAnIDStillWorks")
}

// Needs `handler::serve`, the reqlog port's `capture_log` and `/config-key` (packages 4 and 6).
#[test]
#[ignore = "needs handler::serve, capture_log and /config-key (packages 4, 6)"]
fn config_epoch_refuses_what_was_minted_before_it() {
    // Enable when package 4 lands: move this body up and delete the wrapper.
    #[cfg(any())]
    fn pending() {
        use super::pending_handler::{get, path_of, sealed_config, stream_urls};
        use crate::deps::Deps;
        use crate::tests::reqlog::{capture_log, forget_logged};
        let kr = ticket_keyring();
        let old = sealed_config(&kr, &format!(r#"{TICKET_CFG},"ep":1}}"#));
        let current = sealed_config(&kr, &format!(r#"{TICKET_CFG},"ep":2}}"#));
        let unstamped = sealed_config(&kr, &format!("{TICKET_CFG}}}"));
        let before = Deps::for_test(|d| d.seal_keyring = Some(kr.clone()));
        let old_ticket = path_of(&stream_urls(&before, &old)[0]);

        forget_logged("install-epoch");
        let logs = capture_log(|| {
            let deps = Deps::for_test(|d| {
                d.seal_keyring = Some(kr.clone());
                d.config_epoch = 2;
            });
            let cases: Vec<(String, u16)> = vec![
                (format!("/{old}/manifest.json"), 400),
                (format!("/{old}/stream/movie/tt7654321.json"), 400),
                (format!("/{unstamped}/manifest.json"), 400),
                (old_ticket.clone(), 400),
                (format!("/{current}/manifest.json"), 200),
                (format!("/{current}/stream/movie/tt7654321.json"), 200),
            ];
            for (path, want) in cases {
                assert_eq!(get(&deps, &path).status(), want, "{path}");
            }
            let rr = get(&deps, "/config-key");
            assert!(rr.body().contains(r#""epoch":2"#), "/config-key: {}", rr.body());
        });
        // Whichever refused config the map yields first writes the line; logLimited holds back the rest.
        // Go matches `config refused: install epoch too old \([01] < CONFIG_EPOCH 2\)`.
        let has_line = logs.lines().any(|l| {
            l.contains("config refused: install epoch too old (")
                && (l.contains("(0 < CONFIG_EPOCH 2)") || l.contains("(1 < CONFIG_EPOCH 2)"))
        });
        assert!(has_line, "no epoch line in:\n{logs}");
    }
    todo!("pending: ticket_test.go:~262 TestConfigEpoch_refusesWhatWasMintedBeforeIt")
}

// Needs `handler::serve` and `handler.admitInstall` (work package 4).
#[test]
#[ignore = "needs handler::admit_install (package 4)"]
fn require_install_id_refuses_only_links_without_one() {
    // Enable when package 4 lands: move this body up and delete the wrapper.
    #[cfg(any())]
    fn pending() {
        use super::pending_handler::{get, sealed_config, valid_blob};
        use crate::deps::Deps;
        use crate::handler::Handler;
        let kr = ticket_keyring();
        let with_id = sealed_config(&kr, &format!(r#"{TICKET_CFG},"iid":"{}"}}"#, test_iid()));
        let without = sealed_config(&kr, &format!("{TICKET_CFG}}}"));
        let deps = Deps::for_test(|d| {
            d.seal_keyring = Some(kr.clone());
            d.require_install_id = true;
        });
        let cases: Vec<(String, u16)> = vec![
            (format!("/{without}/manifest.json"), 400),
            (format!("/{}/manifest.json", valid_blob()), 400),
            (format!("/{with_id}/manifest.json"), 200),
        ];
        for (path, want) in cases {
            assert_eq!(get(&deps, &path).status(), want, "{path}");
        }
        let strict = Handler { deps: Deps::for_test(|d| d.require_install_id = true) };
        assert!(
            strict.admit_install(&Config { scope: "availability".to_owned(), ..Default::default() }),
            "a scoped config was refused for having no install id"
        );
        let lax = Handler { deps: Deps::for_test(|_| {}) };
        assert!(
            lax.admit_install(&Config::default()),
            "off by default: a config without an id must still be admitted"
        );
    }
    todo!("pending: ticket_test.go:~300 TestRequireInstallID_refusesOnlyLinksWithoutOne")
}

// Needs `verdictPrefix` and `availabilityOf` (work packages 4 and 1).
#[test]
#[ignore = "needs verdict_prefix / availability_of (packages 4, 1)"]
fn verdict_prefix_ignores_scope_install_and_epoch() {
    // Enable when packages 4 and 1 land: move this body up and delete the wrapper.
    #[cfg(any())]
    fn pending() {
        use super::pending_handler::{sealed_config, stream_urls};
        use crate::deps::Deps;
        use crate::handler::verdict_prefix;
        let kr = ticket_keyring();
        let tv_seg = sealed_config(&kr, &format!(r#"{TICKET_CFG},"iid":"{}","ep":3}}"#, test_iid()));
        let browser_seg =
            sealed_config(&kr, &format!(r#"{TICKET_CFG},"scope":"availability","iid":"{}"}}"#, other_iid()));
        let tv = decode_config(Some(&kr), &tv_seg).unwrap_or_default();
        let browser = decode_config(Some(&kr), &browser_seg).unwrap_or_default();
        assert_eq!(
            verdict_prefix(&tv),
            verdict_prefix(&browser),
            "the TV's and the browser's configs key their verdicts apart"
        );
        let another_seg =
            sealed_config(&kr, &format!("{}}}", TICKET_CFG.replacen("tb-secret", "another", 1)));
        let another = decode_config(Some(&kr), &another_seg).unwrap_or_default();
        assert_ne!(
            verdict_prefix(&another),
            verdict_prefix(&tv),
            "the prefix no longer tells accounts apart"
        );

        let deps = Deps::for_test(|d| d.seal_keyring = Some(kr.clone()));
        stream_urls(&deps, &tv_seg);
        let got = super::pending_handler::availability_of(&deps, &browser_seg, "tt1234567");
        assert_eq!(got, "available", "the browser's scoped config should read the TV's verdict");
    }
    todo!("pending: ticket_test.go:~327 TestVerdictPrefix_ignoresScopeInstallAndEpoch")
}

// Needs `handler::serve` and the LEGACY_PLAY_UNTIL gate (work package 4).
#[test]
#[ignore = "needs handler::serve and the LEGACY_PLAY_UNTIL gate (package 4)"]
fn legacy_play_closes_at_legacy_play_until() {
    // Enable when package 4 lands: move this body up and delete the wrapper.
    #[cfg(any())]
    fn pending() {
        use super::pending_handler::{do_method, sealed_config, valid_blob};
        use crate::deps::Deps;
        let kr = ticket_keyring();
        let token = format!(
            "/play/{}",
            encode_play_token(&PlayTarget {
                info_hash: "a".repeat(40),
                file_idx: Some(0),
                ..Default::default()
            })
        );
        // With a keyring only a sealed config is an install; without one the plaintext blob is.
        let sealed_path = format!("/{}{token}", sealed_config(&kr, &format!("{TICKET_CFG}}}")));
        let plain_path = format!("/{}{token}", valid_blob());
        // `None` is Go's zero time.Time: no deadline.
        let cases: [(&str, Option<SealKeyring>, Option<SystemTime>, u16); 4] = [
            ("no deadline", Some(kr.clone()), None, 302),
            (
                "before the deadline",
                Some(kr.clone()),
                Some(SystemTime::now() + Duration::from_secs(3600)),
                302,
            ),
            ("after the deadline", Some(kr.clone()), Some(SystemTime::now() - Duration::from_secs(1)), 403),
            ("a passed deadline, no ticket", None, Some(SystemTime::now() - Duration::from_secs(1)), 302),
        ];
        for (name, keyring, until, want) in cases {
            let path = if keyring.is_some() { &sealed_path } else { &plain_path };
            let deps = Deps::for_test(|d| {
                d.seal_keyring = keyring.clone();
                d.legacy_play_until = until;
            });
            let rr = do_method(&deps, "GET", path, "");
            assert_eq!(rr.status(), want, "{name}");
        }
    }
    todo!("pending: ticket_test.go:~362 TestLegacyPlay_closesAtLegacyPlayUntil")
}

// Needs `handler::serve` and the reqlog port's `capture_log` (packages 4 and 6).
#[test]
#[ignore = "needs handler::serve and capture_log (packages 4, 6)"]
fn legacy_play_is_logged_once_while_tickets_are_on() {
    // Enable when packages 4 and 6 land: move this body up and delete the wrapper.
    #[cfg(any())]
    fn pending() {
        use super::pending_handler::{do_method, sealed_config, valid_blob};
        use crate::deps::Deps;
        use crate::tests::reqlog::capture_log;
        let kr = ticket_keyring();
        let token = format!(
            "/play/{}",
            encode_play_token(&PlayTarget {
                info_hash: "a".repeat(40),
                file_idx: Some(0),
                ..Default::default()
            })
        );
        let logs = capture_log(|| {
            do_method(&Deps::for_test(|_| {}), "GET", &format!("/{}{token}", valid_blob()), "");
            let deps = Deps::for_test(|d| d.seal_keyring = Some(kr.clone()));
            let sealed_path = format!("/{}{token}", sealed_config(&kr, &format!("{TICKET_CFG}}}")));
            do_method(&deps, "GET", &sealed_path, "");
            do_method(&deps, "GET", &sealed_path, "");
        });
        let n = logs.matches("serving a legacy /<config>/play URL").count();
        assert_eq!(n, 1, "logged {n} times, want once:\n{logs}");
    }
    todo!("pending: ticket_test.go:~390 TestLegacyPlay_isLoggedOnceWhileTicketsAreOn")
}

// Needs `Settings`, `settings_from_env`, `startup_summary`, `build_deps` (deps.rs, package 4) and the
// decision log's malformed-entry line (package 6).
#[test]
#[ignore = "needs Settings / settings_from_env / build_deps (deps.rs, package 4)"]
fn settings_revocation_and_tickets() {
    // Enable when package 4 lands: move this body up and delete the wrapper.
    #[cfg(any())]
    fn pending() {
        use crate::deps::{build_deps, settings_from_env, startup_summary};
        use crate::tests::reqlog::capture_log;
        use std::collections::HashMap;
        let mut env: HashMap<&str, String> = HashMap::new();
        env.insert("CONFIG_KEY", VEC_PRIV_B64.to_owned());
        env.insert("REVOKED_INSTALLS", format!(" {}, not-an-id ,,{}", test_iid(), other_iid()));
        env.insert("CONFIG_EPOCH", "3".to_owned());
        env.insert("PLAY_TICKET_TTL_SECS", "7200".to_owned());
        env.insert("LEGACY_PLAY_UNTIL", "2026-09-17T12:00:00+02:00".to_owned());
        let logs = capture_log(|| {
            let s = settings_from_env(|k| env.get(k).cloned().unwrap_or_default());
            assert!(
                s.revoked_installs == vec![test_iid(), other_iid()]
                    && s.config_epoch == 3
                    && s.play_ticket_ttl == Duration::from_secs(7200)
                    && s.legacy_play_until == Some(UNIX_EPOCH + Duration::from_secs(1_789_639_200)),
                "settings: {s:?}"
            );
            let line = startup_summary(&s, true);
            assert!(line.contains(" revoked=2 epoch=3 "), "startup line: {line}");
            let d = build_deps(&s, None, crate::cache::MemoryCache::new(1 << 10));
            assert!(
                d.revoked_installs.len() == 2
                    && d.config_epoch == 3
                    && d.play_ticket_ttl == Duration::from_secs(7200),
                "deps: {d:?}"
            );
        });
        assert!(
            logs.contains("skipping a malformed REVOKED_INSTALLS entry") && !logs.contains("not-an-id"),
            "the malformed entry: {logs}"
        );

        let defaults = settings_from_env(|_| String::new());
        assert!(
            defaults.revoked_installs.is_empty()
                && defaults.config_epoch == 0
                && defaults.legacy_play_until.is_none(),
            "defaults: {defaults:?}"
        );

        let malformed: HashMap<&str, &str> =
            [("CONFIG_KEY", VEC_PRIV_B64), ("CONFIG_EPOCH", "two"), ("LEGACY_PLAY_UNTIL", "next tuesday")]
                .into_iter()
                .collect();
        let m = settings_from_env(|k| malformed.get(k).copied().unwrap_or_default().to_owned());
        assert_eq!(m.config_epoch, 0, "a malformed CONFIG_EPOCH");
        assert!(
            m.legacy_play_until.is_some_and(|u| u < SystemTime::now()),
            "a malformed LEGACY_PLAY_UNTIL must read as passed: {:?}",
            m.legacy_play_until
        );

        let no_key: HashMap<&str, &str> =
            [("LEGACY_PLAY_UNTIL", "2020-01-01T00:00:00Z")].into_iter().collect();
        let u =
            settings_from_env(|k| no_key.get(k).copied().unwrap_or_default().to_owned()).legacy_play_until;
        assert!(u.is_none(), "LEGACY_PLAY_UNTIL without a key: {u:?}, want unset");

        let short: HashMap<&str, &str> =
            [("CONFIG_KEY", VEC_PRIV_B64), ("PLAY_TICKET_TTL_SECS", "900"), ("LIST_TTL_SECS", "300")]
                .into_iter()
                .collect();
        let warn = capture_log(|| {
            build_deps(
                &settings_from_env(|k| short.get(k).copied().unwrap_or_default().to_owned()),
                None,
                crate::cache::MemoryCache::new(1 << 10),
            );
        });
        assert!(
            warn.contains("PLAY_TICKET_TTL_SECS (15m0s) is not above 3×LIST_TTL_SECS (15m0s)"),
            "no warning for a TTL the list outlives:\n{warn}"
        );
    }
    todo!("pending: ticket_test.go:~420 TestSettings_revocationAndTickets")
}
