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

/// Every per-service route answers byte for byte as it did, whatever the Den addon is configured with.
#[tokio::test]
async fn the_services_own_routes_are_untouched_by_the_den_addon() {
    let before = hearth();
    let after = hearth_den(Limits::default(), None, den_with(Some(VEC_PRIV), IID, "7", true));
    let scout = plain(&format!(r#"{{"debrid":[{{"service":"torbox","token":"tb"}}],"iid":"{IID}"}}"#));
    let subs = plain(&format!(r#"{{"osKey":"os","iid":"{IID}"}}"#));
    let reel = plain(&format!(r#"{{"tmdbKey":"tm","iid":"{IID}"}}"#));
    let paths = [
        "/scout/manifest.json".to_string(),
        "/scout/configure".to_string(),
        "/scout/config-key".to_string(),
        format!("/scout/{scout}/manifest.json"),
        "/reel/manifest.json".to_string(),
        "/reel/configure".to_string(),
        "/reel/config-key".to_string(),
        format!("/reel/{reel}/manifest.json"),
        format!("/reel/{reel}/meta/movie/not-an-id.json"),
        "/subs/manifest.json".to_string(),
        "/subs/configure".to_string(),
        "/subs/config-key".to_string(),
        format!("/subs/{subs}/manifest.json"),
        format!("/subs/{subs}/translate/movie/tt1/Swedish.status"),
        "/remux/health".to_string(),
        "/nope".to_string(),
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
    for path in &paths {
        let was = answer(before.clone(), None, path.clone()).await;
        assert_eq!(answer(after.clone(), None, path.clone()).await, was, "{path}");
    }
    // The services' own listeners too, at the root.
    for (service, mount) in [(Service::Scout, "/scout"), (Service::Reel, "/reel"), (Service::Subs, "/subs")] {
        for path in paths.iter().filter_map(|p| p.strip_prefix(mount)) {
            let was = answer(before.clone(), Some(service), path.to_string()).await;
            assert_eq!(
                answer(after.clone(), Some(service), path.to_string()).await,
                was,
                "{service:?} {path}"
            );
        }
    }
}
