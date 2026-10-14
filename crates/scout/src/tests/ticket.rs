//! Go: `internal/scout/ticket_test.go` (541 lines). Work package 6.
//!
//! Pins the play-ticket wire format: the legacy payloads built below are the byte shape older tickets carry,
//! and must keep opening. Tests that drive `NewHandler`, the settings loader or the decision log need work
//! packages 4 and 5; they are ignored with their Go bodies kept as comments.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use base64::Engine;
use chacha20poly1305::aead::Aead;
use chacha20poly1305::{KeyInit, XChaCha20Poly1305, XNonce};

use super::core::blob;
use super::seal::{bytes_seq, VEC_PRIV_B64};
use crate::config::{decode_config, Config, DebridAccount, DebridService, INSTALL_ID_BYTES, MAX_CONFIG_BLOB, MAX_CONFIG_EPOCH, MAX_DEBRID_ACCOUNTS};
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
const TICKET_CFG: &str = r#"{"debrid":[{"service":"torbox","token":"tb-secret"}],"indexers":["torrentio"],"cachedOnly":true"#;

fn ticket_keyring() -> SealKeyring {
    parse_seal_keyring(VEC_PRIV_B64, "").expect("keyring").expect("sealing on")
}

fn other_keyring(prev: &str) -> SealKeyring {
    let current = base64::engine::general_purpose::STANDARD.encode(bytes_seq(0x40, 32));
    parse_seal_keyring(&current, prev).expect("keyring").expect("sealing on")
}

/// Go `newTicketKeys(kr)`, for a keyring that is set.
fn ticket_keys(kr: &SealKeyring) -> TicketKeys {
    TicketKeys::new(Some(kr)).expect("a keyring turns tickets on")
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
    assert!(got.title.is_empty() && got.imdb.is_empty(), "a ticket minted before title/imdb existed should open with both empty: {got:?}");
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
    assert!(matches!(&r, Err(TicketError::Bad)), "with the prior key dropped: err = {r:?}, want errTicketBad");
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
        panic!("the largest legitimate ticket ({} bytes) does not open under the {MAX_CONFIG_BLOB} cap: {e:?}", ticket.len());
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

// Needs `NewHandler`, `testDeps`, `do`, `sealedConfig` and `decodeConfig` through the handler (work package 4).
#[test]
#[ignore = "needs NewHandler / testDeps / do from handler.rs (package 4)"]
fn play_ticket_an_expired_ticket_is_gone() {
    /*
    kr := ticketKeyring(t)
    tk := newTicketKeys(kr)
    config, _ := decodeConfig(kr, sealedConfig(t, kr, ticketCfg+`}`))
    expired := tk.mint(config, PlayTarget{InfoHash: repeat("a", 40), FileIdx: intp(0)}, time.Now().Add(-time.Second))
    if _, _, err := tk.open(expired, time.Now()); !errors.Is(err, errTicketExpired) {
        t.Fatalf("err = %v, want errTicketExpired", err)
    }
    h := NewHandler(testDeps(func(d *Deps) { d.SealKeyring = kr }))
    for _, q := range []string{"", "?probe=1", "?fresh=1"} {
        if rr := do(h, "/p/"+expired+q, nil); rr.Code != http.StatusGone {
            t.Errorf("/p/<expired>%s: %d, want 410", q, rr.Code)
        }
    }
    */
}

// Needs `NewHandler`, the stream list and `/p/` routing (work packages 4 and 5).
#[test]
#[ignore = "needs NewHandler / streamURLs / the /p/ route from handler.rs (package 4)"]
fn play_ticket_a_stream_list_names_tickets_that_play() {
    /*
    kr := ticketKeyring(t)
    h := NewHandler(testDeps(func(d *Deps) { d.SealKeyring = kr }))
    seg := sealedConfig(t, kr, ticketCfg+`,"iid":"`+testIID+`"}`)
    urls := streamURLs(t, h, seg)
    for _, u := range urls {
        if p := pathOf(t, u); !strings.HasPrefix(p, "/p/") || strings.Count(p, "/") != 2 ||
            strings.Contains(u, seg) || strings.Contains(u, "tb-secret") {
            t.Errorf("play url: %s", u)
        }
    }
    play := pathOf(t, urls[0])
    rr := do(h, play, nil)
    if rr.Code != http.StatusFound || !strings.HasPrefix(rr.Header().Get("location"), "https://cdn.torbox/") {
        t.Fatalf("GET /p/<ticket>: %d location=%q", rr.Code, rr.Header().Get("location"))
    }
    if rr := doMethod(h, http.MethodHead, play, nil); rr.Code != http.StatusMethodNotAllowed {
        t.Errorf("HEAD /p/<ticket>: %d, want 405", rr.Code)
    }
    */
}

// Needs `NewHandler` and the legacy `/<config>/play/` route (work package 4).
#[test]
#[ignore = "needs NewHandler and the legacy play route from handler.rs (package 4)"]
fn play_ticket_no_config_key_keeps_the_legacy_url() {
    /*
    h := NewHandler(testDeps(nil))
    play := pathOf(t, streamURLs(t, h, validBlob)[0])
    if !strings.HasPrefix(play, "/"+validBlob+"/play/") {
        t.Errorf("play url without a key: %s", play)
    }
    if rr := do(h, play, nil); rr.Code != http.StatusFound {
        t.Errorf("legacy play: %d, want 302", rr.Code)
    }
    ticket := newTicketKeys(ticketKeyring(t)).mint(&Config{Debrid: []DebridAccount{{ServiceTorBox, "tb"}}},
        PlayTarget{InfoHash: repeat("a", 40)}, time.Now().Add(time.Hour))
    if rr := do(h, "/p/"+ticket, nil); rr.Code != http.StatusNotFound {
        t.Errorf("/p/ without a key: %d, want 404", rr.Code)
    }
    */
}

// Needs `NewHandler` and the scope check on the stream route (work package 4).
#[test]
#[ignore = "needs NewHandler and the scope check from handler.rs (package 4)"]
fn play_ticket_the_scoped_config_never_mints_one() {
    /*
    kr := ticketKeyring(t)
    scoped := sealedConfig(t, kr, ticketCfg+`,"scope":"availability"}`)
    h := NewHandler(testDeps(func(d *Deps) { d.SealKeyring = kr }))
    for _, path := range []string{"/" + scoped + "/stream/movie/tt1234567.json",
        "/" + scoped + "/stream/movie/tt1234567.json?debug=1"} {
        if rr := do(h, path, nil); rr.Code != http.StatusForbidden || strings.Contains(rr.Body.String(), "/p/") {
            t.Errorf("scoped stream: %d %s", rr.Code, rr.Body.String())
        }
    }
    */
}

// Needs `NewHandler`, `captureLog` and the revocation refusal on every route (work package 4).
#[test]
#[ignore = "needs NewHandler / captureLog / the revocation refusal from handler.rs (package 4)"]
fn revoked_install_is_refused_everywhere() {
    /*
    kr := ticketKeyring(t)
    revoked := sealedConfig(t, kr, ticketCfg+`,"iid":"`+testIID+`"}`)
    live := sealedConfig(t, kr, ticketCfg+`,"iid":"`+otherIID+`"}`)
    // Minted before the revocation, as a client would still hold them.
    before := NewHandler(testDeps(func(d *Deps) { d.SealKeyring = kr }))
    revokedTicket := pathOf(t, streamURLs(t, before, revoked)[0])
    liveTicket := pathOf(t, streamURLs(t, before, live)[0])

    forgetLogged("install-revoked")
    out := captureLog(t)
    h := NewHandler(testDeps(func(d *Deps) {
        d.SealKeyring = kr
        d.RevokedInstalls = map[string]bool{testIID: true}
    }))
    token := encodePlayToken(PlayTarget{InfoHash: repeat("a", 40), FileIdx: intp(0)})
    for path, want := range map[string]int{
        "/" + revoked + "/manifest.json":               http.StatusBadRequest,
        "/" + revoked + "/stream/movie/tt7654321.json": http.StatusBadRequest,
        "/" + revoked + "/play/" + token:               http.StatusBadRequest,
        revokedTicket:                                  http.StatusBadRequest,
        "/" + live + "/manifest.json":                  http.StatusOK,
        "/" + live + "/play/" + token:                  http.StatusFound,
        liveTicket:                                     http.StatusFound,
    } {
        name := strings.NewReplacer(revoked, "<revoked>", live, "<live>").Replace(path)
        if rr := do(h, path, nil); rr.Code != want {
            t.Errorf("%s: %d, want %d", name, rr.Code, want)
        }
    }
    if rr := postJSON(h, "/"+revoked+"/availability", `{"ids":[]}`); rr.Code != http.StatusBadRequest {
        t.Errorf("revoked availability: %d, want 400", rr.Code)
    }
    logs := out.String()
    if !strings.Contains(logs, "config refused: install revoked (iid "+testIID[:6]+"…)") {
        t.Errorf("no revocation line in:\n%s", logs)
    }
    if strings.Contains(logs, testIID) || strings.Contains(logs, otherIID[:6]) {
        t.Errorf("the log carries more of an install id than it may:\n%s", logs)
    }
    */
}

// Needs `NewHandler` (work package 4).
#[test]
#[ignore = "needs NewHandler from handler.rs (package 4)"]
fn revoked_install_a_config_without_an_id_still_works() {
    /*
    h := NewHandler(testDeps(func(d *Deps) { d.RevokedInstalls = map[string]bool{testIID: true} }))
    play := pathOf(t, streamURLs(t, h, validBlob)[0])
    if rr := do(h, play, nil); rr.Code != http.StatusFound {
        t.Errorf("legacy play: %d, want 302", rr.Code)
    }
    */
}

// Needs `NewHandler`, `captureLog` and `/config-key` (work package 4).
#[test]
#[ignore = "needs NewHandler / captureLog / /config-key from handler.rs (package 4)"]
fn config_epoch_refuses_what_was_minted_before_it() {
    /*
    kr := ticketKeyring(t)
    old := sealedConfig(t, kr, ticketCfg+`,"ep":1}`)
    current := sealedConfig(t, kr, ticketCfg+`,"ep":2}`)
    unstamped := sealedConfig(t, kr, ticketCfg+`}`)
    before := NewHandler(testDeps(func(d *Deps) { d.SealKeyring = kr }))
    oldTicket := pathOf(t, streamURLs(t, before, old)[0])

    forgetLogged("install-epoch")
    out := captureLog(t)
    h := NewHandler(testDeps(func(d *Deps) { d.SealKeyring = kr; d.ConfigEpoch = 2 }))
    for path, want := range map[string]int{
        "/" + old + "/manifest.json":                   http.StatusBadRequest,
        "/" + old + "/stream/movie/tt7654321.json":     http.StatusBadRequest,
        "/" + unstamped + "/manifest.json":             http.StatusBadRequest,
        oldTicket:                                      http.StatusBadRequest,
        "/" + current + "/manifest.json":               http.StatusOK,
        "/" + current + "/stream/movie/tt7654321.json": http.StatusOK,
    } {
        name := strings.NewReplacer(old, "<ep1>", current, "<ep2>", unstamped, "<unstamped>").Replace(path)
        if rr := do(h, path, nil); rr.Code != want {
            t.Errorf("%s: %d, want %d", name, rr.Code, want)
        }
    }
    // Whichever refused config the map yields first writes the line; logLimited holds back the rest.
    if !regexp.MustCompile(`config refused: install epoch too old \([01] < CONFIG_EPOCH 2\)`).MatchString(out.String()) {
        t.Errorf("no epoch line in:\n%s", out)
    }
    if rr := do(h, "/config-key", nil); !strings.Contains(rr.Body.String(), `"epoch":2`) {
        t.Errorf("/config-key: %s", rr.Body.String())
    }
    */
}

// Needs `NewHandler` and `handler.admitInstall` (work package 4).
#[test]
#[ignore = "needs NewHandler and handler.admit_install from handler.rs (package 4)"]
fn require_install_id_refuses_only_links_without_one() {
    /*
    kr := ticketKeyring(t)
    withID := sealedConfig(t, kr, ticketCfg+`,"iid":"`+testIID+`"}`)
    without := sealedConfig(t, kr, ticketCfg+`}`)
    h := NewHandler(testDeps(func(d *Deps) { d.SealKeyring = kr; d.RequireInstallID = true }))
    for path, want := range map[string]int{
        "/" + without + "/manifest.json":   http.StatusBadRequest,
        "/" + validBlob + "/manifest.json": http.StatusBadRequest,
        "/" + withID + "/manifest.json":    http.StatusOK,
    } {
        if rr := do(h, path, nil); rr.Code != want {
            t.Errorf("%s: %d, want %d", strings.NewReplacer(without, "<no-iid>", withID, "<iid>").Replace(path), rr.Code, want)
        }
    }
    strict := &handler{deps: Deps{RequireInstallID: true}}
    if !strict.admitInstall(&Config{Scope: scopeAvailability}) {
        t.Error("a scoped config was refused for having no install id")
    }
    if (&handler{}).admitInstall(&Config{}) != true {
        t.Error("off by default: a config without an id must still be admitted")
    }
    */
}

// Needs `verdictPrefix` and `availabilityOf` (work packages 4 and 1).
#[test]
#[ignore = "needs verdict_prefix / availability_of from handler.rs and availability.rs (packages 4, 1)"]
fn verdict_prefix_ignores_scope_install_and_epoch() {
    /*
    kr := ticketKeyring(t)
    tvSeg := sealedConfig(t, kr, ticketCfg+`,"iid":"`+testIID+`","ep":3}`)
    browserSeg := sealedConfig(t, kr, ticketCfg+`,"scope":"availability","iid":"`+otherIID+`"}`)
    tv, _ := decodeConfig(kr, tvSeg)
    browser, _ := decodeConfig(kr, browserSeg)
    if verdictPrefix(tv) != verdictPrefix(browser) {
        t.Error("the TV's and the browser's configs key their verdicts apart")
    }
    another, _ := decodeConfig(kr, sealedConfig(t, kr, strings.Replace(ticketCfg, "tb-secret", "another", 1)+`}`))
    if verdictPrefix(another) == verdictPrefix(tv) {
        t.Error("the prefix no longer tells accounts apart")
    }

    h := NewHandler(testDeps(func(d *Deps) { d.SealKeyring = kr }))
    streamURLs(t, h, tvSeg)
    if got := availabilityOf(t, h, browserSeg, "tt1234567"); got != "available" {
        t.Errorf("the browser's scoped config should read the TV's verdict: %q", got)
    }
    */
}

// Needs `NewHandler` and the legacy route's deadline (work package 4).
#[test]
#[ignore = "needs NewHandler and the LEGACY_PLAY_UNTIL gate from handler.rs (package 4)"]
fn legacy_play_closes_at_legacy_play_until() {
    /*
    kr := ticketKeyring(t)
    token := "/play/" + encodePlayToken(PlayTarget{InfoHash: repeat("a", 40), FileIdx: intp(0)})
    // With a keyring only a sealed config is an install; without one the plaintext blob is.
    sealedPath := "/" + sealedConfig(t, kr, ticketCfg+`}`) + token
    plainPath := "/" + validBlob + token
    for name, c := range map[string]struct {
        kr    *sealKeyring
        until time.Time
        want  int
    }{
        "no deadline":                  {kr, time.Time{}, http.StatusFound},
        "before the deadline":          {kr, time.Now().Add(time.Hour), http.StatusFound},
        "after the deadline":           {kr, time.Now().Add(-time.Second), http.StatusForbidden},
        "a passed deadline, no ticket": {nil, time.Now().Add(-time.Second), http.StatusFound},
    } {
        path := plainPath
        if c.kr != nil {
            path = sealedPath
        }
        h := NewHandler(testDeps(func(d *Deps) { d.SealKeyring = c.kr; d.LegacyPlayUntil = c.until }))
        if rr := do(h, path, nil); rr.Code != c.want {
            t.Errorf("%s: %d, want %d", name, rr.Code, c.want)
        }
    }
    */
}

// Needs `NewHandler` and `captureLog` (work package 4).
#[test]
#[ignore = "needs NewHandler / captureLog from handler.rs (package 4)"]
fn legacy_play_is_logged_once_while_tickets_are_on() {
    /*
    kr := ticketKeyring(t)
    token := "/play/" + encodePlayToken(PlayTarget{InfoHash: repeat("a", 40), FileIdx: intp(0)})
    out := captureLog(t)
    do(NewHandler(testDeps(nil)), "/"+validBlob+token, nil)
    h := NewHandler(testDeps(func(d *Deps) { d.SealKeyring = kr }))
    sealedPath := "/" + sealedConfig(t, kr, ticketCfg+`}`) + token
    do(h, sealedPath, nil)
    do(h, sealedPath, nil)
    if n := strings.Count(out.String(), "serving a legacy /<config>/play URL"); n != 1 {
        t.Errorf("logged %d times, want once:\n%s", n, out)
    }
    */
}

// Needs `SettingsFromEnv`, `StartupSummary`, `BuildDeps` and `Settings` (deps.rs, package 4).
#[test]
#[ignore = "needs Settings / SettingsFromEnv / BuildDeps from deps.rs (package 4)"]
fn settings_revocation_and_tickets() {
    /*
    out := captureLog(t)
    env := map[string]string{
        "CONFIG_KEY":           vecPrivB64,
        "REVOKED_INSTALLS":     " " + testIID + ", not-an-id ,," + otherIID,
        "CONFIG_EPOCH":         "3",
        "PLAY_TICKET_TTL_SECS": "7200",
        "LEGACY_PLAY_UNTIL":    "2026-09-17T12:00:00+02:00",
    }
    s := SettingsFromEnv(func(k string) string { return env[k] })
    if !reflect.DeepEqual(s.RevokedInstalls, []string{testIID, otherIID}) || s.ConfigEpoch != 3 ||
        s.PlayTicketTTL != 2*time.Hour || !s.LegacyPlayUntil.Equal(time.Date(2026, 9, 17, 10, 0, 0, 0, time.UTC)) {
        t.Errorf("settings: %+v", s)
    }
    if !strings.Contains(out.String(), "skipping a malformed REVOKED_INSTALLS entry") || strings.Contains(out.String(), "not-an-id") {
        t.Errorf("the malformed entry: %s", out)
    }
    if line := StartupSummary(s, true); !strings.Contains(line, " revoked=2 epoch=3 ") {
        t.Errorf("startup line: %s", line)
    }
    d := BuildDeps(s, nil, NewMemoryCache(1<<10))
    if !d.RevokedInstalls[testIID] || !d.RevokedInstalls[otherIID] || len(d.RevokedInstalls) != 2 ||
        d.ConfigEpoch != 3 || d.PlayTicketTTL != 2*time.Hour || !d.LegacyPlayUntil.Equal(s.LegacyPlayUntil) {
        t.Errorf("deps: %+v", d)
    }

    defaults := SettingsFromEnv(func(string) string { return "" })
    if defaults.RevokedInstalls != nil || defaults.ConfigEpoch != 0 || defaults.PlayTicketTTL != defaultPlayTicketTTL ||
        !defaults.LegacyPlayUntil.IsZero() {
        t.Errorf("defaults: %+v", defaults)
    }

    malformed := map[string]string{"CONFIG_KEY": vecPrivB64, "CONFIG_EPOCH": "two", "LEGACY_PLAY_UNTIL": "next tuesday"}
    m := SettingsFromEnv(func(k string) string { return malformed[k] })
    if m.ConfigEpoch != 0 {
        t.Errorf("a malformed CONFIG_EPOCH: %d, want 0", m.ConfigEpoch)
    }
    if m.LegacyPlayUntil.IsZero() || !m.LegacyPlayUntil.Before(time.Now()) {
        t.Errorf("a malformed LEGACY_PLAY_UNTIL must read as passed: %v", m.LegacyPlayUntil)
    }

    noKey := map[string]string{"LEGACY_PLAY_UNTIL": "2020-01-01T00:00:00Z"}
    if u := SettingsFromEnv(func(k string) string { return noKey[k] }).LegacyPlayUntil; !u.IsZero() {
        t.Errorf("LEGACY_PLAY_UNTIL without a key: %v, want unset", u)
    }

    short := map[string]string{"CONFIG_KEY": vecPrivB64, "PLAY_TICKET_TTL_SECS": "900", "LIST_TTL_SECS": "300"}
    BuildDeps(SettingsFromEnv(func(k string) string { return short[k] }), nil, NewMemoryCache(1<<10))
    if !strings.Contains(out.String(), "PLAY_TICKET_TTL_SECS (15m0s) is not above 3×LIST_TTL_SECS (15m0s)") {
        t.Errorf("no warning for a TTL the list outlives:\n%s", out)
    }
    */
}
