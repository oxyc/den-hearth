//! The combined Den addon end to end, through `handle`: its manifest, its admission, each resource reaching its
//! service with that service's member of the config, and every per-service route answering as before.

use base64::Engine;

use super::*;
use crate::den::Den;

/// The libsodium vector key the services' own seal tests use.
const VEC_PRIV: &str = "AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8=";
/// Bytes 0..16 as an install id.
const IID: &str = "AAECAwQFBgcICQoLDA0ODw";

fn keyring(private: &str) -> den_seal::Keyring {
    den_seal::Keyring::from_env(private, "").unwrap().unwrap()
}

/// The Den addon sealed to `key` (or plaintext with none), refusing `revoked`, epochs below `epoch`, and with
/// `require_iid` configs without an install id.
fn den_with(key: Option<&str>, revoked: &str, epoch: &str, require_iid: bool) -> Den {
    let revocation = den_seal::Revocation::from_env(revoked, Some(epoch)).requiring_install_id(require_iid);
    Den::new(key.map(keyring), false, revocation, Some("http://node.test/den".into()), "own")
}

fn plain(json: &str) -> String {
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(json)
}

fn sealed(key: &str, json: &str) -> String {
    let mut framed = vec![den_seal::SEALED_VERSION];
    framed.extend(den_seal::seal(&keyring(key).keys[0].pub_key, json.as_bytes()).unwrap());
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(framed)
}

const SCOUT: &str = r#""scout":{"debrid":[{"service":"torbox","token":"tb"}]}"#;
const SUBS: &str = r#""subs":{"osKey":"os"}"#;
const SUBS_AI: &str = r#""subs":{"osKey":"os","provider":"openai","apiKey":"sk"}"#;
const REEL: &str = r#""reel":{"tmdbKey":"tm"}"#;

/// A combined config: the install `IID` at epoch `ep`, and `members`.
fn config(ep: u64, members: &[&str]) -> String {
    format!(r#"{{"iid":"{IID}","ep":{ep},{}}}"#, members.join(","))
}

async fn get_from(hearth: &Arc<Hearth>, path: &str) -> (StatusCode, serde_json::Value) {
    let resp = send(hearth, path).await;
    (resp.status(), json_of(resp).await)
}

fn resource_names(manifest: &serde_json::Value) -> Vec<&str> {
    manifest["resources"].as_array().unwrap().iter().map(|r| r["name"].as_str().unwrap()).collect()
}

#[tokio::test]
async fn the_manifest_declares_only_what_the_install_configured() {
    let hearth = hearth_den(Limits::default(), None, den_with(None, "", "0", false));
    for (members, want) in [
        (vec![SCOUT], vec!["stream"]),
        (vec![SUBS], vec!["subtitles"]),
        (vec![REEL], vec!["meta"]),
        (vec![SCOUT, SUBS], vec!["stream", "subtitles"]),
        (vec![SCOUT, REEL], vec!["stream", "meta"]),
        (vec![SUBS, REEL], vec!["subtitles", "meta"]),
        (vec![REEL, SUBS, SCOUT], vec!["stream", "subtitles", "meta"]),
    ] {
        let (status, manifest) =
            get_from(&hearth, &format!("/den/{}/manifest.json", plain(&config(0, &members)))).await;
        assert_eq!(status, StatusCode::OK, "{members:?}");
        assert_eq!(resource_names(&manifest), want, "{members:?}");
        assert_eq!(manifest["id"], "com.den.addon");
        assert_eq!(manifest["denInstallId"], IID);
        assert_eq!(manifest["behaviorHints"]["configurationRequired"], false);
        // Trailers take tmdb ids too, and only a Reel member brings that prefix.
        let tmdb = manifest["idPrefixes"].as_array().unwrap().contains(&serde_json::json!("tmdb:"));
        assert_eq!(tmdb, members.contains(&REEL), "{members:?}");
        // Credits travel with the resource they belong to.
        let credits = manifest["denAttribution"].to_string();
        assert_eq!(credits.contains("OpenSubtitles"), members.contains(&SUBS), "{members:?}");
        assert_eq!(credits.contains("KinoCheck"), members.contains(&REEL), "{members:?}");
    }
    let (status, manifest) = get_from(&hearth, "/den/manifest.json").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(resource_names(&manifest), ["stream", "subtitles", "meta"]);
    assert_eq!(manifest["behaviorHints"]["configurationRequired"], true);
    assert!(manifest.get("denInstallId").is_none());
}

#[tokio::test]
async fn a_config_with_no_member_or_one_its_service_refuses_is_no_config() {
    let hearth = hearth_den(Limits::default(), None, den_with(None, "", "0", false));
    for json in [
        config(0, &[]).replace(",}", "}"),
        config(0, &[r#""reel":{}"#]),
        config(0, &[SCOUT, r#""subs":{"apiKey":"sk"}"#]),
        config(0, &[r#""scout":{"debrid":[]}"#]),
        config(0, &[r#""reel":"tm""#]),
        format!(r#"{{"iid":"short",{REEL}}}"#),
        format!(r#"{{"ep":-1,{REEL}}}"#),
        format!(r#"{{"ep":1.5,{REEL}}}"#),
        // A service's own config is not a combined one.
        r#"{"tmdbKey":"tm"}"#.to_string(),
        "[]".to_string(),
    ] {
        let (status, body) = get_from(&hearth, &format!("/den/{}/manifest.json", plain(&json))).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{json}");
        assert_eq!(body, serde_json::json!({"error": "bad_config"}), "{json}");
    }
}

/// Each resource reaches its service with that service's member: an install without the member is refused
/// there, and one with it gets past the config to the service's own answer.
#[tokio::test]
async fn each_resource_reaches_its_service_with_its_member() {
    let hearth = hearth_den(Limits::default(), None, den_with(Some(VEC_PRIV), "", "0", false));
    let without = |member: &str| {
        let others: Vec<&str> =
            [SCOUT, SUBS_AI, REEL].into_iter().filter(|m| !m.starts_with(member)).collect();
        sealed(VEC_PRIV, &config(0, &others))
    };

    // Subtitles: a translation's progress is answered from the config alone, with no upstream.
    let status = "/translate/movie/tt0111161/Swedish.status";
    let seg = sealed(VEC_PRIV, &config(0, &[SUBS_AI]));
    assert_eq!(get_from(&hearth, &format!("/den/{seg}{status}")).await.0, StatusCode::OK);
    let seg = sealed(VEC_PRIV, &config(0, &[SUBS]));
    assert_eq!(get_from(&hearth, &format!("/den/{seg}{status}")).await.1["error"], "no_llm");
    let (code, body) = get_from(&hearth, &format!("/den/{}{status}", without("\"subs\""))).await;
    assert_eq!((code, body["error"].as_str()), (StatusCode::BAD_REQUEST, Some("bad_config")));

    // Reel: an id it does not take is answered with no links, and no upstream asked.
    let meta = "/meta/movie/not-an-id.json";
    let seg = sealed(VEC_PRIV, &config(0, &[REEL]));
    let (code, body) = get_from(&hearth, &format!("/den/{seg}{meta}")).await;
    assert_eq!((code, &body["meta"]["links"]), (StatusCode::OK, &serde_json::json!([])));
    let (code, body) = get_from(&hearth, &format!("/den/{}{meta}", without("\"reel\""))).await;
    assert_eq!((code, body["error"].as_str()), (StatusCode::BAD_REQUEST, Some("bad_config")));

    // Scout: a member scoped to availability opens, and is then refused a stream list for its scope, before any
    // indexer is asked.
    let stream = "/stream/movie/tt0111161.json";
    let scoped = r#""scout":{"debrid":[{"service":"torbox","token":"tb"}],"scope":"availability"}"#;
    let seg = sealed(VEC_PRIV, &config(0, &[scoped]));
    let (code, body) = get_from(&hearth, &format!("/den/{seg}{stream}")).await;
    assert_eq!((code, body["error"].as_str()), (StatusCode::FORBIDDEN, Some("out_of_scope")));
    let (code, body) = get_from(&hearth, &format!("/den/{}{stream}", without("\"scout\""))).await;
    assert_eq!((code, body["error"].as_str()), (StatusCode::BAD_REQUEST, Some("bad_config")));
    // The same segment on Scout's own mount is not a config Scout has a key for.
    assert_eq!(get_from(&hearth, &format!("/scout/{seg}{stream}")).await.0, StatusCode::BAD_REQUEST);
}

/// Sealed, plaintext, revoked, epoch and missing id, judged once by the Den addon's terms on its own routes and
/// on a service's.
#[tokio::test]
async fn the_den_addon_s_admission_holds_on_every_route() {
    let ok = StatusCode::OK;
    let bad = StatusCode::BAD_REQUEST;
    let check = |den: Den, seg: String, want: StatusCode, why: &'static str| async move {
        let hearth = hearth_den(Limits::default(), None, den);
        let manifest = send(&hearth, &format!("/den/{seg}/manifest.json")).await.status();
        let service = send(&hearth, &format!("/den/{seg}/translate/movie/tt1/Swedish.status")).await.status();
        assert_eq!((manifest, service), (want, want), "{why}");
    };
    let all = config(2, &[SUBS_AI]);
    check(den_with(Some(VEC_PRIV), "", "0", false), sealed(VEC_PRIV, &all), ok, "sealed").await;
    check(den_with(Some(VEC_PRIV), "", "0", false), plain(&all), bad, "plaintext while sealing is on").await;
    check(den_with(None, "", "0", false), plain(&all), ok, "plaintext with sealing off").await;
    check(den_with(None, "", "0", false), sealed(VEC_PRIV, &all), bad, "sealed with no key").await;
    let other = base64::engine::general_purpose::STANDARD.encode([7u8; 32]);
    check(den_with(Some(&other), "", "0", false), sealed(VEC_PRIV, &all), bad, "sealed to another key").await;
    check(den_with(Some(VEC_PRIV), IID, "0", false), sealed(VEC_PRIV, &all), bad, "revoked").await;
    check(den_with(Some(VEC_PRIV), "", "3", false), sealed(VEC_PRIV, &all), bad, "below the epoch").await;
    check(den_with(Some(VEC_PRIV), "", "2", false), sealed(VEC_PRIV, &all), ok, "at the epoch").await;
    let no_iid = format!(r#"{{"ep":2,{SUBS_AI}}}"#);
    check(den_with(Some(VEC_PRIV), "", "0", true), sealed(VEC_PRIV, &no_iid), bad, "no install id").await;
    check(den_with(Some(VEC_PRIV), "", "0", false), sealed(VEC_PRIV, &no_iid), ok, "no id, not required")
        .await;
}

/// A link `den_configure.html` built: its own script, run in node with every section filled in (debrid token
/// `tb-js`, OpenSubtitles key `os-js` with an OpenAI key `sk-js`, TMDB key `tm-js`) and the vector key served
/// at `/config-key` with epoch 2, so sealed by the page's DenSeal bundle. Regenerate it the same way if the
/// page's config shape ever changes.
const PAGE_SEGMENT: &str = "AZRnksKjNqWvDE1VILvHtNDxyZzlP06Ewl7AsOsq17FVUmvxKnl9a54ZyuXtpCdppXjq1iR_TF9qe7K0tTauxnZ6WfgJaqnBQn-DJW8KZbVJemFcEy3qMQBTbJKeMFGEQ5EBn1HjgNyqIv5Vnf6L_2k1FANkdeZ7hT84T3mH5htuJ1yKMkO2erGLQgfgPMzt53pRsG85BcCF59q8St7ycxXqY7SIOXNG5dg8k-N8BBBNdNFT_GHPO46x0V_QBj19KAXKPUdWp86kxiiXff1RBdNcIeNDn0SuXgs9k4bHH3na1OkiqzselTnB14NfSnD8OjjzI2OSrivhPdUn0HxB9xuaALkAZcmqxiEFCPcLXyGA";

#[tokio::test]
async fn a_link_the_configure_page_built_installs_and_every_resource_opens_it() {
    let hearth = hearth_den(Limits::default(), None, den_with(Some(VEC_PRIV), "", "2", true));
    let (status, manifest) = get_from(&hearth, &format!("/den/{PAGE_SEGMENT}/manifest.json")).await;
    assert_eq!(status, StatusCode::OK, "{manifest}");
    assert_eq!(resource_names(&manifest), ["stream", "subtitles", "meta"]);
    assert_eq!(manifest["denInstallId"], "s6fLT-p0FJKZgbkp3iEs2w");
    let status = format!("/den/{PAGE_SEGMENT}/translate/movie/tt0111161/Swedish.status");
    assert_eq!(get_from(&hearth, &status).await.0, StatusCode::OK, "the subtitles member has its AI key");
    let meta = format!("/den/{PAGE_SEGMENT}/meta/movie/not-an-id.json");
    assert_eq!(get_from(&hearth, &meta).await.0, StatusCode::OK);
    // Built under epoch 2: refused once the addon's epoch passes it.
    let bumped = hearth_den(Limits::default(), None, den_with(Some(VEC_PRIV), "", "3", false));
    assert_eq!(
        get_from(&bumped, &format!("/den/{PAGE_SEGMENT}/manifest.json")).await.0,
        StatusCode::BAD_REQUEST
    );
}

#[tokio::test]
async fn a_malformed_den_key_refuses_every_den_config_and_nothing_else() {
    let den = Den::new(None, true, Default::default(), None, "own, invalid");
    let hearth = hearth_den(Limits::default(), None, den);
    let seg = plain(&config(0, &[SCOUT, SUBS_AI, REEL]));
    for path in [
        "/den/config-key".to_string(),
        format!("/den/{seg}/manifest.json"),
        format!("/den/{seg}/stream/movie/tt0111161.json"),
        format!("/den/{seg}/translate/movie/tt1/Swedish.status"),
        format!("/den/{seg}/meta/movie/not-an-id.json"),
    ] {
        let (status, body) = get_from(&hearth, &path).await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE, "{path}");
        assert_eq!(body["error"], "config_key_invalid", "{path}");
    }
    // The page and the unconfigured manifest need no key; the services' own routes are untouched.
    assert_eq!(send(&hearth, "/den/configure").await.status(), StatusCode::OK);
    assert_eq!(send(&hearth, "/den/manifest.json").await.status(), StatusCode::OK);
    let own = plain(r#"{"osKey":"os"}"#);
    assert_eq!(send(&hearth, &format!("/subs/{own}/manifest.json")).await.status(), StatusCode::OK);
}

#[tokio::test]
async fn the_page_seals_to_the_key_it_is_served_with() {
    let hearth = hearth_den(Limits::default(), None, den_with(Some(VEC_PRIV), "", "4", false));
    let (status, body) = get_from(&hearth, "/den/config-key").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body, serde_json::json!({"key": keyring(VEC_PRIV).current_pub_base64(), "epoch": 4}));
    let plaintext = hearth_den(Limits::default(), None, den_with(None, "", "4", false));
    assert_eq!(
        get_from(&plaintext, "/den/config-key").await,
        (StatusCode::NOT_FOUND, serde_json::json!({"error": "no_key", "epoch": 4}))
    );
    for path in ["/den", "/den/", "/den/configure", "/den/configure/"] {
        let resp = send(&hearth, path).await;
        assert_eq!(resp.status(), StatusCode::OK, "{path}");
        assert_eq!(resp.headers()["content-type"], "text/html; charset=utf-8");
        let page = String::from_utf8(resp.into_body().collect().await.unwrap().to_bytes().to_vec()).unwrap();
        assert!(page.contains("function denSeal(") && page.contains("collectScout"), "{path}");
    }
}

/// Without a key of its own the Den addon seals under one derived from Scout's: sealing follows Scout's, and
/// neither opens what was sealed to the other.
#[test]
fn the_den_key_is_its_own_or_derived_from_scout_s() {
    let env = |pairs: &'static [(&'static str, &'static str)]| {
        move |name: &str| {
            pairs.iter().find(|(k, _)| *k == name).map(|(_, v)| v.to_string()).unwrap_or_default()
        }
    };
    let scout = keyring(VEC_PRIV);

    let derived = Den::from_env(env(&[]), VEC_PRIV, "");
    let ring = derived.scout.keyring.as_ref().expect("Scout seals, so the Den addon does");
    assert_ne!(ring.current_pub_base64(), scout.current_pub_base64());
    assert!(ring.open(&den_seal::seal(&scout.keys[0].pub_key, b"x").unwrap()).is_none());
    assert!(scout.open(&den_seal::seal(&ring.keys[0].pub_key, b"x").unwrap()).is_none());
    assert!(derived.summary().contains("sealed=on key=derived from scout"), "{}", derived.summary());

    let own = Den::from_env(
        env(&[("HEARTH_CONFIG_KEY", "QEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEA=")]),
        VEC_PRIV,
        "",
    );
    assert_eq!(own.scout.keyring.unwrap().keys[0].priv_key, [0x40u8; 32]);

    assert!(Den::from_env(env(&[]), "", "").scout.keyring.is_none(), "nothing seals: plaintext");
    for invalid in [
        Den::from_env(env(&[("HEARTH_CONFIG_KEY", "not-a-key")]), VEC_PRIV, ""),
        Den::from_env(env(&[]), "not-a-key", ""),
    ] {
        assert!(invalid.scout.key_invalid && invalid.scout.keyring.is_none(), "fails closed");
    }

    let terms = Den::from_env(
        env(&[
            ("HEARTH_REVOKED_INSTALLS", IID),
            ("HEARTH_CONFIG_EPOCH", "3"),
            ("HEARTH_REQUIRE_INSTALL_ID", "1"),
            ("HEARTH_PUBLIC_BASE_URL", "https://node.example/"),
        ]),
        "",
        "",
    );
    for member in [&terms.scout, &*terms.subs, &*terms.reel] {
        assert!(member.revocation.is_revoked(IID));
        assert_eq!(member.revocation.epoch(), 3);
        assert!(member.revocation.requires_install_id());
        assert_eq!(member.public_base.as_deref(), Some("https://node.example/den"));
    }
    assert_eq!((terms.scout.slice, terms.subs.slice, terms.reel.slice), ("scout", "subs", "reel"));
}

// --- The services on their own keys -------------------------------------------------------------------------

/// Scout's, Reel's and Subtitles' own sealing keys, each its own, and none of them the Den addon's.
const SCOUT_PRIV: &str = "ERERERERERERERERERERERERERERERERERERERERERE=";
const REEL_PRIV: &str = "IiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiI=";
const SUBS_PRIV: &str = "MzMzMzMzMzMzMzMzMzMzMzMzMzMzMzMzMzMzMzMzMzM=";
/// An install every service's own revocation names.
const REVOKED: &str = "_-_-_-_-_-_-_-_-_-_-_w";
/// Reel's `PLAY_SECRET` in these tests.
const PLAY_SECRET: &str = "s3cret";
const VID: &str = "dQw4w9WgXcQ";

/// A loopback OpenSubtitles answering every search with one English subtitle, file 4242.
async fn opensubtitles_stub() -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        while let Ok((stream, _)) = listener.accept().await {
            let service = service_fn(|_req: Request<Incoming>| async {
                let body = r#"{"data":[{"attributes":{"language":"en","download_count":1,"release":"Film.2020.1080p","files":[{"file_id":4242}]}}]}"#;
                let resp = Response::builder()
                    .header("content-type", "application/json")
                    .body(http_body_util::Full::new(Bytes::from_static(body.as_bytes())))
                    .unwrap();
                Ok::<_, Infallible>(resp)
            });
            tokio::spawn(
                hyper::server::conn::http1::Builder::new().serve_connection(TokioIo::new(stream), service),
            );
        }
    });
    format!("http://{addr}")
}

/// A hearth whose Scout, Reel and Subtitles seal with their own keys, Reel signs its links, and each revokes
/// `REVOKED`; with `den` as the Den addon.
fn configured_hearth(den: Den, opensubtitles: &str) -> Arc<Hearth> {
    let revocation = || den_seal::Revocation::from_env(REVOKED, Some("0"));
    let settings = || {
        let mut s = den_scout::deps::settings_from_env(|_| String::new());
        s.cache_dir = String::new();
        s.config_key = SCOUT_PRIV.into();
        s.revocation = revocation();
        s
    };
    let mut reel = den_reel::Config::from_env();
    reel.config_key = REEL_PRIV.into();
    reel.play_secret = Some(PLAY_SECRET.into());
    reel.revocation = revocation();
    let mut subs = den_subtitles::Config::from_env();
    subs.config_key = SUBS_PRIV.into();
    subs.revocation = revocation();
    subs.os_api_base = opensubtitles.into();
    subs.public_base_url = Some("http://node.test/subs".into());
    Hearth::new(
        den_reel::AppState::new(reel),
        den_subtitles::AppState::new(subs),
        den_remux::AppState::new(den_remux::Config::from_env()),
        scout::Scout::new(settings(), settings(), &den.scout),
        den,
        Limits::default(),
        Options::default(),
    )
}

/// Reel's link tag for `message` under `PLAY_SECRET`: keyed BLAKE2b-96 over the message, its key the first half
/// of BLAKE2b-512 of the secret — computed here apart from Reel, so the wire format is what is pinned.
fn reel_tag(message: &str) -> String {
    use blake2::digest::{consts::U12, Mac};
    use blake2::{Blake2b512, Blake2bMac, Digest};
    let key = Blake2b512::digest(PLAY_SECRET.as_bytes());
    let mut mac = <Blake2bMac<U12> as Mac>::new_from_slice(&key[..32]).unwrap();
    mac.update(message.as_bytes());
    mac.finalize().into_bytes().iter().map(|b| format!("{b:02x}")).collect()
}

/// A Reel install-bound play link for `iid`: Reel's own kind, or the Den addon's.
fn play_link(iid: &str, den: bool) -> String {
    let kind = if den { "den" } else { "v2" };
    let ep = 0;
    let tag = reel_tag(&format!("{kind}\0{VID}\0{iid}\0{ep}"));
    format!("/play/{VID}.mp4?s={tag}&i={iid}&e=0")
}

/// A Reel media capability for `iid`, in a form no handler plays: one that passes every check answers 404
/// without fetching anything.
fn media_link(iid: &str, den: bool) -> String {
    let flag = if den { r#","den":true"# } else { "" };
    let blob = plain(&format!(r#"{{"v":"{VID}","f":"none","i":"{iid}","e":0,"x":4000000000{flag}}}"#));
    format!("/m/s/{blob}?s={}", reel_tag(&format!("m\0{blob}")))
}

/// A Scout play ticket for install `iid` under `ring`, with ticket keys of `info`, good until `exp`.
fn ticket(ring: &den_seal::Keyring, info: &str, iid: &str, exp: std::time::SystemTime) -> String {
    use den_scout::config::{Config, DebridAccount, DebridService};
    let config = Config {
        debrid: vec![DebridAccount { service: DebridService::TorBox, token: "tb".into() }],
        iid: iid.into(),
        ..Default::default()
    };
    let target = den_scout::play::PlayTarget { info_hash: "a".repeat(40), ..Default::default() };
    den_scout::ticket::TicketKeys::with_info(Some(ring), info).unwrap().mint(&config, &target, exp)
}

/// A while ago: a ticket that opens answers 410 for it, one that does not 400, and neither reaches a debrid.
fn expired() -> std::time::SystemTime {
    std::time::UNIX_EPOCH + std::time::Duration::from_secs(1)
}

fn later() -> std::time::SystemTime {
    std::time::SystemTime::now() + std::time::Duration::from_secs(3600)
}

/// Every per-service route answers byte for byte as it did, whatever the Den addon is configured with —
/// including the ones a key, a signature or a revocation decides, on services sealing, signing and revoking.
#[tokio::test]
async fn the_services_own_routes_are_untouched_by_the_den_addon() {
    let opensubtitles = opensubtitles_stub().await;
    let before = configured_hearth(Den::new(None, false, Default::default(), None, "none"), &opensubtitles);
    let after = configured_hearth(den_with(Some(VEC_PRIV), IID, "7", true), &opensubtitles);
    let scout =
        sealed(SCOUT_PRIV, &format!(r#"{{"debrid":[{{"service":"torbox","token":"tb"}}],"iid":"{IID}"}}"#));
    let scoped = sealed(
        SCOUT_PRIV,
        &format!(
            r#"{{"debrid":[{{"service":"torbox","token":"tb"}}],"scope":"availability","iid":"{IID}"}}"#
        ),
    );
    let scout_revoked = sealed(
        SCOUT_PRIV,
        &format!(r#"{{"debrid":[{{"service":"torbox","token":"tb"}}],"iid":"{REVOKED}"}}"#),
    );
    let subs = sealed(SUBS_PRIV, &format!(r#"{{"osKey":"os","iid":"{IID}"}}"#));
    let reel = sealed(REEL_PRIV, &format!(r#"{{"tmdbKey":"tm","iid":"{IID}"}}"#));
    let scout_ring = keyring(SCOUT_PRIV);
    let scout_info = den_scout::ticket::PLAY_TICKET_INFO;
    let den_info = den_scout::ticket::DEN_PLAY_TICKET_INFO;
    let stream = "stream/movie/tt0111161.json";
    // Each request, and what it answers: a refusal must be the refusal it was, not a 404 both happen to give.
    let requests = [
        ("/scout/manifest.json".to_string(), 200),
        ("/scout/configure".to_string(), 200),
        ("/scout/config-key".to_string(), 200),
        (format!("/scout/{scout}/manifest.json"), 200),
        (format!("/scout/{scoped}/{stream}"), 403),
        (format!("/scout/{scout_revoked}/{stream}"), 400),
        (format!("/scout/p/{}", ticket(&scout_ring, scout_info, IID, expired())), 410),
        (format!("/scout/p/{}", ticket(&scout_ring, scout_info, REVOKED, later())), 400),
        (format!("/scout/p/{}", ticket(&scout_ring, den_info, IID, expired())), 400),
        ("/reel/manifest.json".to_string(), 200),
        ("/reel/configure".to_string(), 200),
        ("/reel/config-key".to_string(), 200),
        (format!("/reel/{reel}/manifest.json"), 200),
        (format!("/reel/{reel}/meta/movie/not-an-id.json"), 200),
        (format!("/reel{}", play_link(REVOKED, false)), 403),
        (format!("/reel{}", play_link(IID, true)), 403),
        (format!("/reel/play/{VID}.mp4?s=deadbeefdeadbeefdeadbeef"), 403),
        (format!("/reel{}", media_link(IID, false)), 404),
        (format!("/reel{}", media_link(REVOKED, false)), 403),
        (format!("/reel{}", media_link(IID, true)), 403),
        ("/subs/manifest.json".to_string(), 200),
        ("/subs/configure".to_string(), 200),
        ("/subs/config-key".to_string(), 200),
        (format!("/subs/{subs}/manifest.json"), 200),
        (format!("/subs/{subs}/subtitles/movie/tt0111161.json"), 200),
        (format!("/subs/{subs}/translate/movie/tt1/Swedish.status"), 400),
        ("/remux/health".to_string(), 200),
        ("/nope".to_string(), 404),
    ];
    let answer = |hearth: Arc<Hearth>, own: Option<Service>, path: String| async move {
        let resp = send_on(&hearth, own, &path).await;
        let status = resp.status();
        let mut headers: Vec<(String, String)> = resp
            .headers()
            .iter()
            .filter(|(name, _)| *name != "server-timing")
            .map(|(name, value)| (name.to_string(), value.to_str().unwrap_or("").to_string()))
            .collect();
        headers.sort();
        let body = resp.into_body().collect().await.unwrap().to_bytes();
        (status, headers, body)
    };
    for (path, status) in &requests {
        let was = answer(before.clone(), None, path.clone()).await;
        assert_eq!(was.0.as_u16(), *status, "{path}: {:?}", was.2);
        assert_eq!(answer(after.clone(), None, path.clone()).await, was, "{path}");
    }
    // The subtitle list's URLs are built from the service's own base, as they always were.
    let (_, list) = get_from(&after, &format!("/subs/{subs}/subtitles/movie/tt0111161.json")).await;
    let url = list["subtitles"][0]["url"].as_str().expect("a subtitle url");
    assert!(url.starts_with(&format!("http://node.test/subs/{subs}/subtitle/4242.srt")), "{url}");
    // The services' own listeners too, at the root.
    for (service, mount) in [(Service::Scout, "/scout"), (Service::Reel, "/reel"), (Service::Subs, "/subs")] {
        for path in requests.iter().filter_map(|(p, _)| p.strip_prefix(mount)) {
            let was = answer(before.clone(), Some(service), path.to_string()).await;
            assert_eq!(
                answer(after.clone(), Some(service), path.to_string()).await,
                was,
                "{service:?} {path}"
            );
        }
    }
}

/// A play ticket opens only on the routes of the addon that minted it, and the Den addon's revocation reaches
/// its own: with the Den key derived from Scout's, and with the very same key, where only the ticket keys'
/// own HKDF info keeps the two apart. (An expired ticket that opens answers 410, one that does not 400; neither
/// reaches a debrid.)
#[tokio::test]
async fn a_ticket_opens_only_where_it_was_minted() {
    let opensubtitles = opensubtitles_stub().await;
    let scout_ring = keyring(SCOUT_PRIV);
    let derived = Den::from_env(
        |name| if name == "HEARTH_REVOKED_INSTALLS" { REVOKED.into() } else { String::new() },
        SCOUT_PRIV,
        "",
    );
    let same_key = Den::new(
        Some(keyring(SCOUT_PRIV)),
        false,
        den_seal::Revocation::from_env(REVOKED, Some("0")),
        None,
        "own",
    );
    for (what, den) in [("derived", derived), ("the same key", same_key)] {
        let den_ring = den.scout.keyring.clone().unwrap();
        let hearth = configured_hearth(den, &opensubtitles);
        let status = |path: String| {
            let hearth = hearth.clone();
            async move { send(&hearth, &path).await.status().as_u16() }
        };
        let den_ticket = ticket(&den_ring, den_scout::ticket::DEN_PLAY_TICKET_INFO, IID, expired());
        let scout_ticket = ticket(&scout_ring, den_scout::ticket::PLAY_TICKET_INFO, IID, expired());
        assert_eq!(
            status(format!("/den/p/{den_ticket}")).await,
            410,
            "{what}: a Den ticket on the Den addon"
        );
        assert_eq!(status(format!("/scout/p/{scout_ticket}")).await, 410, "{what}: Scout's on Scout's");
        assert_eq!(
            status(format!("/scout/p/{den_ticket}")).await,
            400,
            "{what}: a Den ticket opened on /scout"
        );
        assert_eq!(
            status(format!("/den/p/{scout_ticket}")).await,
            400,
            "{what}: a Scout ticket opened on /den"
        );
        // Revoked by the Den addon: refused before any debrid is asked.
        let revoked = ticket(&den_ring, den_scout::ticket::DEN_PLAY_TICKET_INFO, REVOKED, later());
        assert_eq!(status(format!("/den/p/{revoked}")).await, 400, "{what}: a revoked Den ticket");
    }
}

/// den-edge's LAN relay can check a Den media capability at `/den/_internal/validate`, as it checks Reel's at
/// Reel's; each validates only its own kind.
#[tokio::test]
async fn the_den_validate_route_checks_den_capabilities_only() {
    let hearth = configured_hearth(den_with(Some(VEC_PRIV), "", "0", false), "http://127.0.0.1:1");
    let capability = |den: bool| {
        let flag = if den { r#","den":true"# } else { "" };
        let blob =
            plain(&format!(r#"{{"v":"{VID}","f":"p","h":720,"i":"{IID}","e":0,"x":4000000000{flag}}}"#));
        format!("/_internal/validate/m/s/{blob}?s={}", reel_tag(&format!("m\0{blob}")))
    };
    let status = |path: String| {
        let hearth = hearth.clone();
        async move { send(&hearth, &path).await.status().as_u16() }
    };
    assert_eq!(status(format!("/den{}", capability(true))).await, 204);
    assert_eq!(status(format!("/reel{}", capability(false))).await, 204);
    assert_eq!(status(format!("/den{}", capability(false))).await, 403);
    assert_eq!(status(format!("/reel{}", capability(true))).await, 403);
}

/// A node that moves from the key derived from Scout's to one of its own names `scout` among its prior keys,
/// and every install sealed under the derived key keeps opening; without it they stop, as any rotated-out key's.
#[test]
fn moving_to_an_own_key_keeps_the_derived_one_by_name() {
    let env = |prev: &str| {
        let prev = prev.to_string();
        move |name: &str| match name {
            "HEARTH_CONFIG_KEY" => VEC_PRIV.to_string(),
            "HEARTH_CONFIG_KEYS_PREV" => prev.clone(),
            _ => String::new(),
        }
    };
    let derived = Den::from_env(|_| String::new(), SCOUT_PRIV, "").scout.keyring.unwrap();
    let to_derived = den_seal::seal(&derived.keys[0].pub_key, b"x").unwrap();
    let kept = Den::from_env(env("scout"), SCOUT_PRIV, "").scout.keyring.unwrap();
    assert_eq!(kept.keys[0].priv_key, keyring(VEC_PRIV).keys[0].priv_key, "the own key is current");
    assert_eq!(kept.open(&to_derived).as_deref(), Some(&b"x"[..]));
    let dropped = Den::from_env(env(""), SCOUT_PRIV, "").scout.keyring.unwrap();
    assert!(dropped.open(&to_derived).is_none());
    // Alongside other prior keys, and with no Scout key to derive from it is skipped, not fatal.
    let mixed = Den::from_env(env(&format!("{SUBS_PRIV}, scout")), SCOUT_PRIV, "");
    assert_eq!(mixed.scout.keyring.unwrap().keys.len(), 3);
    let none = Den::from_env(env("scout"), "", "");
    assert_eq!(none.scout.keyring.unwrap().keys.len(), 1);
}
