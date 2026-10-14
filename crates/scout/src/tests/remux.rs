//! Go: `internal/scout/remux_test.go` (89 lines). Work package 4.
//!
//! Scoped configs (availability-only) and the den-remux key, through the handler.

use std::sync::Arc;

use crate::handler::new_handler;
use crate::tests::availability::sealed_config;
use crate::tests::handler::{body_str, first_play_path, get, test_deps, test_iid, ticket_keyring, TICKET_CFG};

/// Go `TestRemuxTicket_preservesScopedAdmissionAndRevocation`: a scoped config's play tickets keep its admission
/// (the remux key) and its revocation, with and without an install id.
#[tokio::test]
async fn remux_ticket_preserves_scoped_admission_and_revocation() {
    let kr = ticket_keyring();
    let headers = [("X-Den-Remux-Key", "remux-secret")];
    for iid in ["", test_iid().as_str()] {
        let mut cfg = format!("{TICKET_CFG},\"scope\":\"availability\"");
        if !iid.is_empty() {
            cfg.push_str(&format!(",\"iid\":\"{iid}\""));
        }
        let scoped = sealed_config(&kr, &format!("{cfg}}}"));
        let mut deps = test_deps(|d| {
            d.seal_keyring = Some(Arc::new(ticket_keyring()));
            d.remux_key = "remux-secret".to_owned();
            d.require_install_id = true;
        });
        let path = format!("/{scoped}/stream/movie/tt1234567.json");
        let h = new_handler(test_deps(|d| {
            d.seal_keyring = Some(Arc::new(ticket_keyring()));
            d.remux_key = "remux-secret".to_owned();
            d.require_install_id = true;
        }));
        assert_eq!(get(&h, &path, &[]).await.status(), 403, "scoped list without remux key (iid={iid:?})");
        let rr = get(&h, &path, &headers).await;
        assert_eq!(rr.status(), 200, "authorized list (iid={iid:?}): {}", body_str(&rr));
        let play = first_play_path(&body_str(&rr), "/p/");
        assert_eq!(get(&h, &play, &headers).await.status(), 302, "fresh scoped ticket");
        deps.config_epoch = 1;
        assert_eq!(get(&new_handler(deps), &play, &headers).await.status(), 400, "old epoch ticket");
        if !iid.is_empty() {
            let mut revoked = test_deps(|d| {
                d.seal_keyring = Some(Arc::new(ticket_keyring()));
                d.remux_key = "remux-secret".to_owned();
                d.require_install_id = true;
            });
            revoked.revoked_installs.insert(iid.to_owned());
            assert_eq!(get(&new_handler(revoked), &play, &headers).await.status(), 400, "revoked scoped ticket");
        }
    }
}

/// Go `TestRemuxKey_letsAScopedConfigListAndPlay`: an availability-scoped config lists and plays only for den-remux
/// (REMUX_KEY in X-Den-Remux-Key), and a list built for den-remux is never what a request without the key gets.
#[tokio::test]
async fn remux_key_lets_a_scoped_config_list_and_play() {
    let kr = ticket_keyring();
    let scoped = sealed_config(&kr, &format!("{TICKET_CFG},\"scope\":\"availability\"}}"));
    let h = new_handler(test_deps(|d| {
        d.seal_keyring = Some(Arc::new(ticket_keyring()));
        d.remux_key = "remux-secret".to_owned();
    }));
    let path = format!("/{scoped}/stream/movie/tt7654321.json");
    let with_key = [("X-Den-Remux-Key", "remux-secret")];

    assert_eq!(get(&h, &path, &[]).await.status(), 403, "scoped, no key");
    let rr = get(&h, &path, &with_key).await;
    assert!(rr.status() == 200 && body_str(&rr).contains("/p/"), "scoped + key: {} {}, want a list of tickets", rr.status(), body_str(&rr));
    assert_eq!(get(&h, &path, &[]).await.status(), 403, "scoped, no key, after a keyed build: the keyed list is cached apart");
    assert_eq!(get(&h, &path, &[("X-Den-Remux-Key", "wrong")]).await.status(), 403, "scoped, wrong key");
}

/// Go `TestRemuxKey_offByDefault`: with REMUX_KEY unset the header means nothing, and a full config never needed it.
#[tokio::test]
async fn remux_key_off_by_default() {
    let kr = ticket_keyring();
    let scoped = sealed_config(&kr, &format!("{TICKET_CFG},\"scope\":\"availability\"}}"));
    let full = sealed_config(&kr, &format!("{TICKET_CFG}}}"));
    let h = new_handler(test_deps(|d| d.seal_keyring = Some(Arc::new(ticket_keyring()))));
    assert_eq!(get(&h, &format!("/{scoped}/stream/movie/tt7654321.json"), &[("X-Den-Remux-Key", "")]).await.status(), 403, "scoped with REMUX_KEY unset");
    assert_eq!(get(&h, &format!("/{full}/stream/movie/tt7654321.json"), &[]).await.status(), 200, "full config");
}
