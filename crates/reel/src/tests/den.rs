//! Reel under the combined Den addon (hearth's `/den`): its member of the combined config, the links it mints
//! for one, and the revocation those links answer to.

use super::*;

/// The Den addon's terms for Reel: plaintext configs (no key), minting under `http://node.test/den`.
fn den_terms(revoked: &str, epoch: &str) -> den_seal::Addon {
    den_seal::Addon {
        keyring: None,
        key_invalid: false,
        revocation: crate::userconfig::Revocation::from_env(revoked, Some(epoch)),
        public_base: Some("http://node.test/den".into()),
        slice: "reel",
    }
}

/// `spawn_server`, with every request carrying `den` as hearth puts it on one it routed under `/den`.
async fn spawn_den_server(state: Arc<AppState>, den: den_seal::Addon) -> String {
    let den = Arc::new(den);
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        loop {
            let (stream, _) = listener.accept().await.unwrap();
            let (state, den) = (state.clone(), den.clone());
            tokio::spawn(async move {
                let svc = service_fn(move |mut req: hyper::Request<hyper::body::Incoming>| {
                    req.extensions_mut().insert(den.clone());
                    let state = state.clone();
                    async move { Ok::<_, Infallible>(crate::handle_request(state, req).await) }
                });
                let _ = hyper::server::conn::http1::Builder::new()
                    .serve_connection(TokioIo::new(stream), svc)
                    .await;
            });
        }
    });
    format!("http://{addr}")
}

/// A combined config carrying Reel's member and the install `IID` at epoch `ep`.
fn combined(ep: u64) -> String {
    plain_segment(&format!(r#"{{"iid":"{IID}","ep":{ep},"reel":{{"tmdbKey":"k"}}}}"#))
}

/// The meta answer for `segment` from the server at `base`.
async fn meta(base: &str, segment: &str) -> reqwest::Response {
    reqwest::get(format!("{base}/{segment}/meta/movie/tt0111161.json")).await.unwrap()
}

#[tokio::test]
async fn a_den_install_s_links_are_minted_under_its_base_and_bound_as_its() {
    use crate::sign::Binding;
    let dir = temp_dir();
    let den = spawn_den_server(signed_state(dir, "", "0", false), den_terms("", "0")).await;
    let body: Value = meta(&den, &combined(0)).await.json().await.unwrap();
    let link = &body["meta"]["links"][0];
    let tag = bound_tag("cachedVid07", Binding::Den { iid: Some(IID), ep: 0 });
    assert_eq!(link["trailers"], format!("http://node.test/den/play/cachedVid07.mp4?s={tag}&i={IID}&e=0"));
    assert_eq!(link["sources"], format!("http://node.test/den/sources/cachedVid07.json?s={tag}&i={IID}&e=0"));
    // The same install's tag as Reel's own would be another one: the scope is in what is signed.
    assert_ne!(tag, bound_tag("cachedVid07", Binding::Install { iid: Some(IID), ep: 0 }));
}

#[tokio::test]
async fn a_link_plays_only_on_the_routes_of_the_addon_that_minted_it() {
    use crate::sign::Binding;
    let dir = temp_dir();
    seed_cache(&dir, "cachedVid07", 100);
    let state = signed_state(dir, "", "0", false);
    let reel = spawn_server(state.clone()).await;
    let den = spawn_den_server(state, den_terms("", "0")).await;
    let link = |binding: Binding| {
        format!("/play/cachedVid07.mp4?s={}{}", bound_tag("cachedVid07", binding), binding.query())
    };
    let den_link = link(Binding::Den { iid: Some(IID), ep: 0 });
    let reel_link = link(Binding::Install { iid: Some(IID), ep: 0 });
    let status = |base: &str, link: &str| {
        let url = format!("{base}{link}");
        async move { reqwest::get(url).await.unwrap().status().as_u16() }
    };
    assert_eq!(status(&den, &den_link).await, 200);
    assert_eq!(status(&reel, &reel_link).await, 200);
    // Carried to the other addon's routes, where the other's revocation would be asked: refused like a bad tag.
    assert_eq!(status(&reel, &den_link).await, 403, "a Den link played on Reel's own route");
    assert_eq!(status(&den, &reel_link).await, 403, "a Reel link played on the Den addon's route");
}

#[tokio::test]
async fn the_den_addon_s_revocation_reaches_its_links_and_reel_s_does_not() {
    use crate::sign::Binding;
    let dir = temp_dir();
    seed_cache(&dir, "cachedVid07", 100);
    let den_link = {
        let binding = Binding::Den { iid: Some(IID), ep: 0 };
        format!("/play/cachedVid07.mp4?s={}{}", bound_tag("cachedVid07", binding), binding.query())
    };
    // Revoked by the Den addon: its links and its config stop.
    let revoked = spawn_den_server(signed_state(dir.clone(), "", "0", false), den_terms(IID, "0")).await;
    assert_eq!(reqwest::get(format!("{revoked}{den_link}")).await.unwrap().status(), 403);
    assert_eq!(meta(&revoked, &combined(0)).await.status(), 400);
    // Below the Den addon's epoch: the same.
    let bumped = spawn_den_server(signed_state(dir.clone(), "", "0", false), den_terms("", "1")).await;
    assert_eq!(reqwest::get(format!("{bumped}{den_link}")).await.unwrap().status(), 403);
    assert_eq!(meta(&bumped, &combined(0)).await.status(), 400);
    assert_eq!(meta(&bumped, &combined(1)).await.status(), 200);
    // Revoked, and its epoch raised, by Reel alone: nothing of the Den addon's is touched.
    let reel_only = spawn_den_server(signed_state(dir, IID, "5", true), den_terms("", "0")).await;
    assert_eq!(reqwest::get(format!("{reel_only}{den_link}")).await.unwrap().status(), 200);
    assert_eq!(meta(&reel_only, &combined(0)).await.status(), 200);
}

#[tokio::test]
async fn a_reel_config_is_not_a_den_config_nor_the_reverse() {
    let state = signed_state(temp_dir(), "", "0", false);
    let reel = spawn_server(state.clone()).await;
    let den = spawn_den_server(state, den_terms("", "0")).await;
    let own = plain_segment(&format!(r#"{{"tmdbKey":"k","iid":"{IID}","ep":0}}"#));
    assert_eq!(meta(&reel, &own).await.status(), 200);
    assert_eq!(meta(&den, &combined(0)).await.status(), 200);
    assert_eq!(meta(&den, &own).await.status(), 400, "Reel's own config opened as the Den addon's");
    assert_eq!(meta(&reel, &combined(0)).await.status(), 400, "a Den config opened as Reel's own");
    // A Den config without a Reel member: no trailers for it.
    let subs_only = plain_segment(r#"{"subs":{"osKey":"o"}}"#);
    assert_eq!(meta(&den, &subs_only).await.status(), 400);
}

#[tokio::test]
async fn a_malformed_den_key_refuses_its_configs_and_leaves_reel_s_alone() {
    let state = signed_state(temp_dir(), "", "0", false);
    let reel = spawn_server(state.clone()).await;
    let den = spawn_den_server(state, den_seal::Addon { key_invalid: true, ..den_terms("", "0") }).await;
    let refused = meta(&den, &combined(0)).await;
    assert_eq!(refused.status(), 503);
    assert_eq!(refused.json::<Value>().await.unwrap()["error"], "config_key_invalid");
    let own = plain_segment(r#"{"tmdbKey":"k"}"#);
    assert_eq!(meta(&reel, &own).await.status(), 200);
}

/// A media capability says which addon minted it, under the tag, and is honoured only on that addon's routes.
#[tokio::test]
async fn a_media_capability_is_honoured_only_where_it_was_minted() {
    let state = signed_state(temp_dir(), "", "0", false);
    let den = den_terms("", "0");
    let signer = crate::sign::Signer::new("s3cret");
    // A form no handler plays, so a capability that passes every check answers 404 without fetching anything.
    let media = |den: bool| crate::sources::Media {
        v: "dQw4w9WgXcQ".into(),
        f: "none".into(),
        h: None,
        a: false,
        n: false,
        p: None,
        i: Some(IID.into()),
        e: Some(0),
        x: 4_000_000_000,
        den,
    };
    let open = |sealed: String, den: Option<&den_seal::Addon>| {
        let state = state.clone();
        let den = den.cloned();
        async move {
            let (blob, query) = sealed.strip_prefix("m/s/").unwrap().split_once('?').unwrap();
            crate::sources::handle_media(state, den.as_ref(), &hyper::HeaderMap::new(), "s", blob, query)
                .await
                .status()
        }
    };
    let den_cap = crate::sources::seal(Some(&signer), &media(true));
    let reel_cap = crate::sources::seal(Some(&signer), &media(false));
    assert_eq!(open(den_cap.clone(), Some(&den)).await, 404, "passes every check");
    assert_eq!(open(reel_cap.clone(), None).await, 404, "passes every check");
    assert_eq!(open(den_cap, None).await, 403, "a Den capability opened on Reel's own route");
    assert_eq!(open(reel_cap.clone(), Some(&den)).await, 403, "a Reel capability opened on the Den addon's");
    // Reel's own capability is the blob it always was: no `den` field.
    let blob = reel_cap.strip_prefix("m/s/").unwrap().split_once('?').unwrap().0;
    use base64::Engine;
    let json = base64::engine::general_purpose::URL_SAFE_NO_PAD.decode(blob).unwrap();
    assert!(!String::from_utf8(json).unwrap().contains("den"));
    // Revoked by the Den addon.
    let revoked = den_terms(IID, "0");
    assert_eq!(open(crate::sources::seal(Some(&signer), &media(true)), Some(&revoked)).await, 403);
    // The Den addon mints only for an install, so a Den capability naming none is not one it minted.
    let unbound = crate::sources::Media { i: None, e: None, ..media(true) };
    assert_eq!(open(crate::sources::seal(Some(&signer), &unbound), Some(&den)).await, 403);
}

/// Reel's unbound links (its config-less `/meta` and `/prepare`) verify only on Reel's routes: the Den addon
/// mints none, and its `/sources` must not mint Den capabilities from one.
#[tokio::test]
async fn an_unbound_link_plays_only_on_reel_s_routes() {
    use crate::sign::Binding;
    let dir = temp_dir();
    seed_cache(&dir, "cachedVid07", 100);
    let state = signed_state(dir, "", "0", false);
    let reel = spawn_server(state.clone()).await;
    let den = spawn_den_server(state, den_terms("", "0")).await;
    let tag = bound_tag("cachedVid07", Binding::Unbound);
    let play = format!("/play/cachedVid07.mp4?s={tag}");
    let sources = format!("/sources/cachedVid07.json?s={tag}&surface=silent&player=hls.js");
    let status = |url: String| async move { reqwest::get(url).await.unwrap().status().as_u16() };
    assert_eq!(status(format!("{reel}{play}")).await, 200);
    assert_eq!(status(format!("{den}{play}")).await, 403, "an unbound link played on the Den addon's route");
    assert_eq!(status(format!("{den}{sources}")).await, 403, "Den media minted from an unbound link");
    assert_eq!(status(format!("{den}/play/cachedVid07.mp4")).await, 403, "untagged");
}

/// The LAN relay's capability check, on the Den addon's terms: only Den capabilities validate there, and only
/// Reel's on Reel's own route.
#[test]
fn the_den_validate_route_accepts_only_den_capabilities() {
    let state = signed_state(temp_dir(), "", "0", false);
    let den = den_terms("", "0");
    let signer = crate::sign::Signer::new("s3cret");
    let media = |den: bool| crate::sources::Media {
        v: "dQw4w9WgXcQ".into(),
        f: "p".into(),
        h: Some(720),
        a: false,
        n: false,
        p: None,
        i: Some(IID.into()),
        e: Some(0),
        x: 4_000_000_000,
        den,
    };
    let validate = |media: &crate::sources::Media, terms: Option<&den_seal::Addon>| {
        let sealed = crate::sources::seal(Some(&signer), media);
        let (blob, query) = sealed.strip_prefix("m/s/").unwrap().split_once('?').unwrap();
        crate::sources::validate_direct_capability(&state, terms, blob, query).status()
    };
    assert_eq!(validate(&media(true), Some(&den)), 204);
    assert_eq!(validate(&media(false), None), 204);
    assert_eq!(validate(&media(false), Some(&den)), 403);
    assert_eq!(validate(&media(true), None), 403);
    assert_eq!(validate(&media(true), Some(&den_terms(IID, "0"))), 403, "revoked by the Den addon");
}

/// The message Reel signs for its own links is the one it always signed: installs in the field hold them.
#[test]
fn reel_s_own_link_messages_are_unchanged() {
    use crate::sign::{message, Binding};
    assert_eq!(message("dQw4w9WgXcQ", Binding::Unbound), "v2\0dQw4w9WgXcQ\0\0");
    assert_eq!(
        message("dQw4w9WgXcQ", Binding::Install { iid: Some(IID), ep: 3 }),
        format!("v2\0dQw4w9WgXcQ\0{IID}\x003")
    );
    assert_eq!(message("dQw4w9WgXcQ", Binding::Install { iid: None, ep: 0 }), "v2\0dQw4w9WgXcQ\0\x000");
}
