//! Go: `internal/scout/ticket_test.go` (541 lines). Work package 6.
//!
//! Pins the play-ticket wire format: the legacy payloads built below are the byte shape older tickets carry,
//! and must keep opening. The route tests drive the handler; the settings test drives `settings_from_env` and
//! `build_deps`.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use base64::Engine;
use chacha20poly1305::aead::Aead;
use chacha20poly1305::{KeyInit, XChaCha20Poly1305, XNonce};
use regex::Regex;

use super::availability::availability_of;
use super::core::blob;
use super::handler::{body_str, get, hdr, method, post_json, sealed_config, test_deps};
use super::reqlog::{forget_logged, LogCapture};
use super::seal::{bytes_seq, VEC_PRIV_B64};
use crate::availability::verdict_prefix;
use crate::cache::MemoryCache;
use crate::config::{
    decode_config, Config, DebridAccount, DebridService, INSTALL_ID_BYTES, MAX_CONFIG_BLOB, MAX_CONFIG_EPOCH,
    MAX_DEBRID_ACCOUNTS, SCOPE_AVAILABILITY,
};
use crate::deps::{build_deps, settings_from_env, startup_summary};
use crate::handler::{new_handler, Handler};
use crate::play::{encode_play_token, PlayTarget};
use crate::seal::{parse_seal_keyring, SealKeyring};
use crate::ticket::{TicketError, TicketKeys, DEFAULT_PLAY_TICKET_TTL};
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

// Go reads a payload's keys case-insensitively and takes the last of a repeated one; a payload that spells every
// key exactly takes a shorter route. Whichever route a payload takes, it must open to the same ticket.
#[test]
fn play_ticket_payload_keys_open_whatever_their_spelling() {
    let kr = ticket_keyring();
    let tk = ticket_keys(&kr);
    let hash = "a".repeat(40);
    let x = unix_secs(SystemTime::now() + Duration::from_secs(3600));
    // The key `h` as a JSON unicode escape (backslash, `u`, then 0068), built here so no editor or tool can fold
    // the escape back into a plain `h`.
    let escaped_h = format!("{}u0068", '\\');
    let payloads = [
        ("exact", format!(r#"{{"h":"{hash}","f":4,"d":[["torbox","tb"]],"x":{x}}}"#)),
        ("upper case", format!(r#"{{"H":"{hash}","F":4,"D":[["torbox","tb"]],"X":{x}}}"#)),
        ("one key off", format!(r#"{{"h":"{hash}","f":4,"d":[["torbox","tb"]],"X":{x}}}"#)),
        ("an unknown key", format!(r#"{{"h":"{hash}","f":4,"d":[["torbox","tb"]],"x":{x},"extra":[1]}}"#)),
        // Only a decoder that unescapes keys finds this `h`.
        ("an escaped key", format!(r#"{{"{escaped_h}":"{hash}","f":4,"d":[["torbox","tb"]],"x":{x}}}"#)),
        ("a repeated key", format!(r#"{{"h":"nope","h":"{hash}","f":4,"d":[["torbox","tb"]],"x":{x}}}"#)),
        (
            "a repeated spelling",
            format!(r#"{{"H":"nope","h":"{hash}","f":4,"d":[["torbox","tb"]],"x":{x}}}"#),
        ),
    ];
    for (name, payload) in payloads {
        let ticket = legacy_ticket(&tk.keys[0], payload.as_bytes());
        let (config, target) =
            tk.open(&ticket, SystemTime::now()).unwrap_or_else(|e| panic!("{name}: {e:?}"));
        assert_eq!(target.info_hash, hash, "{name}");
        assert_eq!(target.file_idx, Some(4), "{name}");
        assert_eq!(config.debrid.len(), 1, "{name}");
    }
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

/// Go `streamURLs`: builds a movie stream list for a config segment and returns its play URLs.
async fn stream_urls(h: &Handler, config: &str) -> Vec<String> {
    let rr = get(h, &format!("/{config}/stream/movie/tt1234567.json"), &[]).await;
    assert_eq!(rr.status(), 200, "stream: {} {}", rr.status(), body_str(&rr));
    let body: serde_json::Value = serde_json::from_slice(rr.body()).expect("json");
    let streams = body["streams"].as_array().expect("streams");
    assert!(!streams.is_empty(), "no streams");
    streams.iter().map(|s| s["url"].as_str().unwrap_or_default().to_owned()).collect()
}

/// Go `pathOf`: the path of a URL, without its query.
fn path_of(u: &str) -> String {
    let after = u.split_once("://").map_or(u, |(_, rest)| rest);
    let path = after.find('/').map_or("/", |i| &after[i..]);
    path.split('?').next().unwrap_or("/").to_owned()
}

fn with_keyring(over: impl FnOnce(&mut crate::deps::Deps)) -> Handler {
    new_handler(test_deps(|d| {
        d.seal_keyring = Some(Arc::new(ticket_keyring()));
        over(d);
    }))
}

// An expired ticket is 410 on the route, probe or not, so the client fetches the list again.
#[tokio::test]
async fn play_ticket_an_expired_ticket_is_gone() {
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
    let h = with_keyring(|_| {});
    for q in ["", "?probe=1", "?fresh=1"] {
        let rr = get(&h, &format!("/p/{expired}{q}"), &[]).await;
        assert_eq!(rr.status(), 410, "/p/<expired>{q}: {}, want 410", rr.status());
    }
}

// With a key set, every play URL in a list is a ticket, carries neither the config nor the token, and plays.
#[tokio::test]
async fn play_ticket_a_stream_list_names_tickets_that_play() {
    let kr = ticket_keyring();
    let h = with_keyring(|_| {});
    let seg = sealed_config(&kr, &format!(r#"{TICKET_CFG},"iid":"{}"}}"#, test_iid()));
    let urls = stream_urls(&h, &seg).await;
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
    let rr = get(&h, &play, &[]).await;
    let location = hdr(&rr, "location");
    assert!(
        rr.status() == 302 && location.starts_with("https://cdn.torbox/"),
        "GET /p/<ticket>: {} location={location:?}",
        rr.status()
    );
    let head = method(&h, http::Method::HEAD, &play, &[]).await;
    assert_eq!(head.status(), 405, "HEAD /p/<ticket>: {}, want 405", head.status());
}

// Without CONFIG_KEY nothing changes: the list names the legacy route and /p/ does not exist.
#[tokio::test]
async fn play_ticket_no_config_key_keeps_the_legacy_url() {
    let h = new_handler(test_deps(|_| {}));
    let play = path_of(&stream_urls(&h, &valid_blob()).await[0]);
    assert!(play.starts_with(&format!("/{}/play/", valid_blob())), "play url without a key: {play}");
    let rr = get(&h, &play, &[]).await;
    assert_eq!(rr.status(), 302, "legacy play: {}, want 302", rr.status());
    let ticket = ticket_keys(&ticket_keyring()).mint(
        &Config {
            debrid: vec![DebridAccount { service: DebridService::TorBox, token: "tb".to_owned() }],
            ..Default::default()
        },
        &PlayTarget { info_hash: "a".repeat(40), ..Default::default() },
        SystemTime::now() + Duration::from_secs(3600),
    );
    let rr = get(&h, &format!("/p/{ticket}"), &[]).await;
    assert_eq!(rr.status(), 404, "/p/ without a key: {}, want 404", rr.status());
}

// The scoped config is refused before any list is built, so it never mints a ticket.
#[tokio::test]
async fn play_ticket_the_scoped_config_never_mints_one() {
    let kr = ticket_keyring();
    let scoped = sealed_config(&kr, &format!(r#"{TICKET_CFG},"scope":"availability"}}"#));
    let h = with_keyring(|_| {});
    for path in [
        format!("/{scoped}/stream/movie/tt1234567.json"),
        format!("/{scoped}/stream/movie/tt1234567.json?debug=1"),
    ] {
        let rr = get(&h, &path, &[]).await;
        assert!(
            rr.status() == 403 && !body_str(&rr).contains("/p/"),
            "scoped stream: {} {}",
            rr.status(),
            body_str(&rr)
        );
    }
}

// A revoked install is refused on every route that takes its config, on the tickets it already holds and on
// the legacy play route — each with the answer an undecodable config gets there. Other installs carry on.
#[tokio::test]
async fn revoked_install_is_refused_everywhere() {
    let kr = ticket_keyring();
    let revoked = sealed_config(&kr, &format!(r#"{TICKET_CFG},"iid":"{}"}}"#, test_iid()));
    let live = sealed_config(&kr, &format!(r#"{TICKET_CFG},"iid":"{}"}}"#, other_iid()));
    // Minted before the revocation, as a client would still hold them.
    let before = with_keyring(|_| {});
    let revoked_ticket = path_of(&stream_urls(&before, &revoked).await[0]);
    let live_ticket = path_of(&stream_urls(&before, &live).await[0]);

    forget_logged("install-revoked");
    let out = LogCapture::start();
    let h = with_keyring(|d| d.revoked_installs = HashSet::from([test_iid()]));
    let token =
        encode_play_token(&PlayTarget { info_hash: "a".repeat(40), file_idx: Some(0), ..Default::default() });
    let cases: Vec<(String, u16)> = vec![
        (format!("/{revoked}/manifest.json"), 400),
        (format!("/{revoked}/stream/movie/tt7654321.json"), 400),
        (format!("/{revoked}/play/{token}"), 400),
        (revoked_ticket, 400),
        (format!("/{live}/manifest.json"), 200),
        (format!("/{live}/play/{token}"), 302),
        (live_ticket, 302),
    ];
    for (path, want) in cases {
        let name = path.replace(&revoked, "<revoked>").replace(&live, "<live>");
        let rr = get(&h, &path, &[]).await;
        assert_eq!(rr.status(), want, "{name}: {}, want {want}", rr.status());
    }
    let rr = post_json(&h, &format!("/{revoked}/availability"), r#"{"ids":[]}"#).await;
    assert_eq!(rr.status(), 400, "revoked availability: {}, want 400", rr.status());
    let logs = out.text();
    assert!(
        logs.contains(&format!("config refused: install revoked (iid {}…)", &test_iid()[..6])),
        "no revocation line in:\n{logs}"
    );
    assert!(
        !logs.contains(&test_iid()) && !logs.contains(&other_iid()[..6]),
        "the log carries more of an install id than it may:\n{logs}"
    );
}

// A config without an install id cannot be revoked by id, and keeps working while CONFIG_EPOCH is 0.
#[tokio::test]
async fn revoked_install_a_config_without_an_id_still_works() {
    let h = new_handler(test_deps(|d| d.revoked_installs = HashSet::from([test_iid()])));
    let play = path_of(&stream_urls(&h, &valid_blob()).await[0]);
    let rr = get(&h, &play, &[]).await;
    assert_eq!(rr.status(), 302, "legacy play: {}, want 302", rr.status());
}

// CONFIG_EPOCH refuses every config, and every ticket, minted under an older epoch — an unstamped one
// included — and /config-key hands /configure the epoch to stamp.
#[tokio::test]
async fn config_epoch_refuses_what_was_minted_before_it() {
    let kr = ticket_keyring();
    let old = sealed_config(&kr, &format!(r#"{TICKET_CFG},"ep":1}}"#));
    let current = sealed_config(&kr, &format!(r#"{TICKET_CFG},"ep":2}}"#));
    let unstamped = sealed_config(&kr, &format!("{TICKET_CFG}}}"));
    let before = with_keyring(|_| {});
    let old_ticket = path_of(&stream_urls(&before, &old).await[0]);

    forget_logged("install-epoch");
    let out = LogCapture::start();
    let h = with_keyring(|d| d.config_epoch = 2);
    let cases: Vec<(String, u16)> = vec![
        (format!("/{old}/manifest.json"), 400),
        (format!("/{old}/stream/movie/tt7654321.json"), 400),
        (format!("/{unstamped}/manifest.json"), 400),
        (old_ticket, 400),
        (format!("/{current}/manifest.json"), 200),
        (format!("/{current}/stream/movie/tt7654321.json"), 200),
    ];
    for (path, want) in cases {
        let name = path.replace(&old, "<ep1>").replace(&current, "<ep2>").replace(&unstamped, "<unstamped>");
        let rr = get(&h, &path, &[]).await;
        assert_eq!(rr.status(), want, "{name}: {}, want {want}", rr.status());
    }
    // Whichever refused config the map yields first writes the line; logLimited holds back the rest.
    let logs = out.text();
    assert!(
        Regex::new(r"config refused: install epoch too old \([01] < CONFIG_EPOCH 2\)")
            .unwrap()
            .is_match(&logs),
        "no epoch line in:\n{logs}"
    );
    let rr = get(&h, "/config-key", &[]).await;
    assert!(body_str(&rr).contains(r#""epoch":2"#), "/config-key: {}", body_str(&rr));
}

// REQUIRE_INSTALL_ID refuses every full config minted before install ids, keeps those that carry one, and
// leaves a scoped config alone.
#[tokio::test]
async fn require_install_id_refuses_only_links_without_one() {
    let kr = ticket_keyring();
    let with_id = sealed_config(&kr, &format!(r#"{TICKET_CFG},"iid":"{}"}}"#, test_iid()));
    let without = sealed_config(&kr, &format!("{TICKET_CFG}}}"));
    let h = with_keyring(|d| d.require_install_id = true);
    let cases: Vec<(String, u16)> = vec![
        (format!("/{without}/manifest.json"), 400),
        (format!("/{}/manifest.json", valid_blob()), 400),
        (format!("/{with_id}/manifest.json"), 200),
    ];
    for (path, want) in cases {
        let name = path.replace(&without, "<no-iid>").replace(&with_id, "<iid>");
        let rr = get(&h, &path, &[]).await;
        assert_eq!(rr.status(), want, "{name}: {}, want {want}", rr.status());
    }
    let strict = new_handler(test_deps(|d| d.require_install_id = true));
    assert!(
        strict.admit_install(&Config { scope: SCOPE_AVAILABILITY.to_owned(), ..Default::default() }),
        "a scoped config was refused for having no install id"
    );
    assert!(
        new_handler(test_deps(|_| {})).admit_install(&Config::default()),
        "off by default: a config without an id must still be admitted"
    );
}

// The TV's full config and the browser's separately-minted scoped config differ in scope, install id and
// epoch, and must still share verdicts. A different account must not.
#[tokio::test]
async fn verdict_prefix_ignores_scope_install_and_epoch() {
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
    let another_seg = sealed_config(&kr, &format!("{}}}", TICKET_CFG.replacen("tb-secret", "another", 1)));
    let another = decode_config(Some(&kr), &another_seg).unwrap_or_default();
    assert_ne!(verdict_prefix(&another), verdict_prefix(&tv), "the prefix no longer tells accounts apart");

    let h = with_keyring(|_| {});
    stream_urls(&h, &tv_seg).await;
    let got = availability_of(&h, &browser_seg, "tt1234567").await;
    assert_eq!(got, "available", "the browser's scoped config should read the TV's verdict: {got:?}");
}

// The old route stays open until LEGACY_PLAY_UNTIL passes — and only while tickets are on, since without
// them it is the only play route there is.
#[tokio::test]
async fn legacy_play_closes_at_legacy_play_until() {
    let kr = ticket_keyring();
    let token = format!(
        "/play/{}",
        encode_play_token(&PlayTarget { info_hash: "a".repeat(40), file_idx: Some(0), ..Default::default() })
    );
    // With a keyring only a sealed config is an install; without one the plaintext blob is.
    let sealed_path = format!("/{}{token}", sealed_config(&kr, &format!("{TICKET_CFG}}}")));
    let plain_path = format!("/{}{token}", valid_blob());
    // `None` is Go's zero time.Time: no deadline.
    let cases: [(&str, bool, Option<SystemTime>, u16); 4] = [
        ("no deadline", true, None, 302),
        ("before the deadline", true, Some(SystemTime::now() + Duration::from_secs(3600)), 302),
        ("after the deadline", true, Some(SystemTime::now() - Duration::from_secs(1)), 403),
        ("a passed deadline, no ticket", false, Some(SystemTime::now() - Duration::from_secs(1)), 302),
    ];
    for (name, keyed, until, want) in cases {
        let path = if keyed { &sealed_path } else { &plain_path };
        let h = new_handler(test_deps(|d| {
            d.seal_keyring = keyed.then(|| Arc::new(ticket_keyring()));
            d.legacy_play_until = until;
        }));
        let rr = get(&h, path, &[]).await;
        assert_eq!(rr.status(), want, "{name}: {}, want {want}", rr.status());
    }
}

#[tokio::test]
async fn legacy_play_is_logged_once_while_tickets_are_on() {
    let kr = ticket_keyring();
    let token = format!(
        "/play/{}",
        encode_play_token(&PlayTarget { info_hash: "a".repeat(40), file_idx: Some(0), ..Default::default() })
    );
    let out = LogCapture::start();
    get(&new_handler(test_deps(|_| {})), &format!("/{}{token}", valid_blob()), &[]).await;
    let h = with_keyring(|_| {});
    let sealed_path = format!("/{}{token}", sealed_config(&kr, &format!("{TICKET_CFG}}}")));
    get(&h, &sealed_path, &[]).await;
    get(&h, &sealed_path, &[]).await;
    let logs = out.text();
    let n = logs.matches("serving a legacy /<config>/play URL").count();
    assert_eq!(n, 1, "logged {n} times, want once:\n{logs}");
}

#[test]
fn settings_revocation_and_tickets() {
    let logs = LogCapture::start();
    let env: HashMap<&str, String> = HashMap::from([
        ("CONFIG_KEY", VEC_PRIV_B64.to_owned()),
        ("REVOKED_INSTALLS", format!(" {}, not-an-id ,,{}", test_iid(), other_iid())),
        ("CONFIG_EPOCH", "3".to_owned()),
        ("PLAY_TICKET_TTL_SECS", "7200".to_owned()),
        ("LEGACY_PLAY_UNTIL", "2026-09-17T12:00:00+02:00".to_owned()),
    ]);
    let s = settings_from_env(|k| env.get(k).cloned().unwrap_or_default());
    // 2026-09-17T10:00:00Z
    let legacy = UNIX_EPOCH + Duration::from_secs(1_789_639_200);
    assert!(
        s.revoked_installs == vec![test_iid(), other_iid()]
            && s.config_epoch == 3
            && s.play_ticket_ttl == Duration::from_secs(7200)
            && s.legacy_play_until == Some(legacy),
        "settings: {s:?}"
    );
    let text = logs.text();
    assert!(
        text.contains("skipping a malformed REVOKED_INSTALLS entry") && !text.contains("not-an-id"),
        "the malformed entry: {text}"
    );
    let line = startup_summary(&s, true);
    assert!(line.contains(" revoked=2 epoch=3 "), "startup line: {line}");
    let d = build_deps(&s, reqwest::Client::new(), Arc::new(MemoryCache::new(1 << 10)));
    assert!(
        d.revoked_installs.contains(&test_iid())
            && d.revoked_installs.contains(&other_iid())
            && d.revoked_installs.len() == 2
            && d.config_epoch == 3
            && d.play_ticket_ttl == Duration::from_secs(7200)
            && d.legacy_play_until == s.legacy_play_until,
        "deps: revoked={:?} epoch={} ttl={:?} until={:?}",
        d.revoked_installs,
        d.config_epoch,
        d.play_ticket_ttl,
        d.legacy_play_until
    );

    let defaults = settings_from_env(|_| String::new());
    assert!(
        defaults.revoked_installs.is_empty()
            && defaults.config_epoch == 0
            && defaults.play_ticket_ttl == DEFAULT_PLAY_TICKET_TTL
            && defaults.legacy_play_until.is_none(),
        "defaults: {defaults:?}"
    );

    let malformed: HashMap<&str, &str> = HashMap::from([
        ("CONFIG_KEY", VEC_PRIV_B64),
        ("CONFIG_EPOCH", "two"),
        ("LEGACY_PLAY_UNTIL", "next tuesday"),
    ]);
    let m = settings_from_env(|k| malformed.get(k).copied().unwrap_or_default().to_owned());
    assert_eq!(m.config_epoch, 0, "a malformed CONFIG_EPOCH: {}, want 0", m.config_epoch);
    assert!(
        m.legacy_play_until.is_some_and(|u| u < SystemTime::now()),
        "a malformed LEGACY_PLAY_UNTIL must read as passed: {:?}",
        m.legacy_play_until
    );

    let no_key: HashMap<&str, &str> = HashMap::from([("LEGACY_PLAY_UNTIL", "2020-01-01T00:00:00Z")]);
    let u = settings_from_env(|k| no_key.get(k).copied().unwrap_or_default().to_owned()).legacy_play_until;
    assert!(u.is_none(), "LEGACY_PLAY_UNTIL without a key: {u:?}, want unset");

    let short: HashMap<&str, &str> = HashMap::from([
        ("CONFIG_KEY", VEC_PRIV_B64),
        ("PLAY_TICKET_TTL_SECS", "900"),
        ("LIST_TTL_SECS", "300"),
    ]);
    build_deps(
        &settings_from_env(|k| short.get(k).copied().unwrap_or_default().to_owned()),
        reqwest::Client::new(),
        Arc::new(MemoryCache::new(1 << 10)),
    );
    let text = logs.text();
    assert!(
        text.contains("PLAY_TICKET_TTL_SECS (15m0s) is not above 3×LIST_TTL_SECS (15m0s)"),
        "no warning for a TTL the list outlives:\n{text}"
    );
}
