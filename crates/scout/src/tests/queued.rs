//! Go: `internal/scout/queued_test.go` (280 lines). Work package 1.
//!
//! Some tests go through the handler (`TestPlay_queuedAddAnswers202`, `TestProbe_queuedReleaseAnswers202NotNotQueued`)
//! and stay red until work package 4 lands; `TestAddMagnet_queuedChargeIsNotRefunded` needs the add budget
//! (package 3). Those three are cfg-gated until their owners land.

use std::sync::Arc;

use crate::cache::{Cache, MemoryCache};
use crate::config::DebridService;
use crate::stores::torbox::{was_added_by_us, TorBoxStore, TORBOX_API};
use crate::stores::{ResolveTarget, Store, StoreError, FETCH_QUEUED};
use crate::tests::mock::{resp, MockDoer};

fn base(req: &reqwest::Request) -> String {
    req.url().path().rsplit('/').next().unwrap_or("").to_owned()
}

// Decoding `queued_id`: createtorrent answering it rather than `torrent_id` used to fall through to the
// generic "no torrent_id" DeadLinkError (stores.go, pre-den#204), which the client blacklisted a release
// over while TorBox still held the add and would start it eventually. addMagnet must read it as a
// genuine success — a torrent id of 0 is not "nothing happened".
#[tokio::test]
async fn add_magnet_queued_id_is_not_a_dead_link() {
    let cache: Arc<dyn Cache> = Arc::new(MemoryCache::new(1 << 20));
    let s = TorBoxStore {
        token: "tb-queued-addmagnet".to_owned(),
        api: TORBOX_API.to_owned(),
        cache: Some(cache.clone()),
        client: Arc::new(MockDoer(|_| Ok(resp(200, r#"{"success":true,"data":{"queued_id":7}}"#)))),
    };
    let hash = "a".repeat(40);
    let (torrent_id, queued_id) =
        s.add_magnet(&hash, false, false).await.expect("a queued_id answer must not be an error");
    assert!(torrent_id == 0 && queued_id == 7, "add_magnet returned ({torrent_id}, {queued_id}), want (0, 7)");
    assert_eq!(s.known_queued_id(&hash), Some(7), "known_queued_id");
    assert!(
        was_added_by_us(&*cache, &s.account_identity(), &hash),
        "a queued add must still be marked as ours — Cancel needs it"
    );
}

// Resolve answers errAddQueued, not a dead link, for a parked add — and does not re-add on the next poll.
#[tokio::test]
async fn resolve_queued_add_is_pending_not_dead() {
    let hash = "a".repeat(40);
    let createtorrent_calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let counter = createtorrent_calls.clone();
    let s = TorBoxStore {
        token: "tb-queued-resolve".to_owned(),
        api: TORBOX_API.to_owned(),
        cache: Some(Arc::new(MemoryCache::new(1 << 20))),
        client: Arc::new(MockDoer(move |req| {
            match base(&req).as_str() {
                "createtorrent" => {
                    counter.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    Ok(resp(200, r#"{"success":true,"data":{"queued_id":7}}"#))
                }
                // Still parked: the account listing does not carry it yet.
                "mylist" => Ok(resp(200, r#"{"success":true,"data":[]}"#)),
                _ => Ok(resp(404, "{}")),
            }
        })),
    };
    let target = ResolveTarget { info_hash: hash.clone(), file_idx: Some(0), ..Default::default() };
    let err = s.resolve(&target).await.expect_err("first resolve must be pending");
    assert_eq!(err, StoreError::AddQueued, "first resolve: want AddQueued");
    // A second poll must not send createtorrent again — the knownQueuedID gate takes over.
    let err = s.resolve(&target).await.expect_err("second resolve must be pending");
    assert_eq!(err, StoreError::AddQueued, "second resolve: want AddQueued");
    assert_eq!(
        createtorrent_calls.load(std::sync::atomic::Ordering::SeqCst),
        1,
        "createtorrent called across two polls, want 1"
    );
}

// Once TorBox promotes the queued add to a real torrent (the account listing now carries it), Resolve
// serves it like any other held torrent — no second add, no lingering "queued" answer.
#[tokio::test]
async fn resolve_queued_add_promoted_to_a_torrent() {
    let hash = "a".repeat(40);
    let promoted_hash = hash.clone();
    let s = TorBoxStore {
        token: "tb-queued-promoted".to_owned(),
        api: TORBOX_API.to_owned(),
        cache: Some(Arc::new(MemoryCache::new(1 << 20))),
        client: Arc::new(MockDoer(move |req| match base(&req).as_str() {
            "createtorrent" => Ok(resp(200, r#"{"success":true,"data":{"queued_id":7}}"#)),
            "mylist" => {
                if req.url().query_pairs().any(|(k, _)| k == "id") {
                    Ok(resp(200, r#"{"success":true,"data":{"id":42,"download_finished":false}}"#))
                } else {
                    // Promoted: the account listing now carries the torrent.
                    Ok(resp(200, &format!(r#"{{"success":true,"data":[{{"id":42,"hash":"{promoted_hash}"}}]}}"#)))
                }
            }
            "requestdl" => Ok(resp(200, r#"{"success":true,"data":"https://cdn.torbox/promoted.mkv"}"#)),
            _ => Ok(resp(404, "{}")),
        })),
    };
    let target = ResolveTarget { info_hash: hash.clone(), file_idx: Some(0), ..Default::default() };
    // First poll: parked.
    let err = s.resolve(&target).await.expect_err("first resolve must be pending");
    assert_eq!(err, StoreError::AddQueued, "first resolve: want AddQueued");
    // Second poll: promoted, and playable.
    let link = s.resolve(&target).await.expect("resolve after promotion");
    assert_eq!(link, "https://cdn.torbox/promoted.mkv", "resolve after promotion");
    assert_eq!(s.known_queued_id(&hash), None, "the queued marker must be cleared once promoted");
}

// The add charge is NOT refunded for a queued_id answer — createtorrent genuinely accepted the add; it is
// merely parked. Refunding it would let an account loop past its own hourly allowance by polling a
// release that is, correctly, always answered as "queued".
//
// Gated until package 3 lands `global_add_budget` and `budget_account` (addbudget.rs).
#[test]
#[ignore = "needs global_add_budget and budget_account from package 3"]
fn add_magnet_queued_charge_is_not_refunded() {
    // Enable when package 3 lands: move this body up and delete the wrapper.
    #[cfg(any())]
    async fn pending() {
    use crate::addbudget::{budget_account, global_add_budget};

    let account = format!("tb-queued-budget-{}", "x".repeat(8));
    let s = TorBoxStore {
        token: account,
        api: TORBOX_API.to_owned(),
        cache: Some(Arc::new(MemoryCache::new(1 << 20))),
        client: Arc::new(MockDoer(|_| Ok(resp(200, r#"{"success":true,"data":{"queued_id":7}}"#)))),
    };
    let budget_key = || budget_account(DebridService::TorBox, &s.account_identity());
    let before = global_add_budget().remaining(&budget_key());
    s.add_magnet(&"a".repeat(40), false, false).await.expect("add_magnet");
    let after = global_add_budget().remaining(&budget_key());
    assert_eq!(after, before - 1, "remaining budget after a queued add (charged, not refunded)");
    }
    todo!("pending: queued_test.go:103 TestAddMagnet_queuedChargeIsNotRefunded")
}

// createtorrent answering 400 DIFF_ISSUE "Download already queued." for a hash with no queued-id marker
// (e.g. one added before ownership tracking existed) used to fall straight through to the generic
// DeadLinkError — a release genuinely sitting in TorBox's own queue read as dead. addMagnet must discover
// the queue entry via getqueued and report it the same way a direct queued_id answer is.
#[tokio::test]
async fn add_magnet_already_queued_is_discovered_via_getqueued() {
    let hash = "a".repeat(40);
    let listed_hash = hash.clone();
    let calls = Arc::new(std::sync::Mutex::new(Vec::<String>::new()));
    let recorder = calls.clone();
    let cache: Arc<dyn Cache> = Arc::new(MemoryCache::new(1 << 20));
    let s = TorBoxStore {
        token: "tb-diffissue".to_owned(),
        api: TORBOX_API.to_owned(),
        cache: Some(cache.clone()),
        client: Arc::new(MockDoer(move |req| {
            recorder.lock().unwrap().push(base(&req));
            match base(&req).as_str() {
                "createtorrent" => Ok(resp(400, r#"{"error":"DIFF_ISSUE","detail":"Download already queued."}"#)),
                "getqueued" => Ok(resp(200, &format!(r#"{{"success":true,"data":[{{"id":55,"hash":"{listed_hash}"}}]}}"#))),
                _ => Ok(resp(404, "{}")),
            }
        })),
    };
    let (torrent_id, queued_id) =
        s.add_magnet(&hash, false, false).await.expect("already-queued must not be an error");
    assert!(torrent_id == 0 && queued_id == 55, "add_magnet returned ({torrent_id}, {queued_id}), want (0, 55)");
    assert_eq!(s.known_queued_id(&hash), Some(55), "known_queued_id");
    assert!(
        was_added_by_us(&*cache, &s.account_identity(), &hash),
        "a discovered queue entry must still be marked as ours — Cancel needs it"
    );
    assert_eq!(*calls.lock().unwrap(), vec!["createtorrent", "getqueued"], "upstream calls");
}

// A plain 400 that is NOT "already queued" — a malformed magnet, an account past its limit — must stay a
// dead link. The DIFF_ISSUE branch must not swallow every 400.
#[tokio::test]
async fn add_magnet_ordinary_400_stays_a_dead_link() {
    let hash = "a".repeat(40);
    let getqueued_calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let counter = getqueued_calls.clone();
    let s = TorBoxStore {
        token: "tb-plain400".to_owned(),
        api: TORBOX_API.to_owned(),
        cache: Some(Arc::new(MemoryCache::new(1 << 20))),
        client: Arc::new(MockDoer(move |req| {
            if base(&req) == "getqueued" {
                counter.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            }
            Ok(resp(400, r#"{"error":"BAD_MAGNET","detail":"Invalid magnet link."}"#))
        })),
    };
    let err = s.add_magnet(&hash, false, false).await.expect_err("addMagnet err must be a DeadLinkError");
    assert!(matches!(err, StoreError::DeadLink(_)), "addMagnet err = {err:?}, want a DeadLinkError");
    assert_eq!(getqueued_calls.load(std::sync::atomic::Ordering::SeqCst), 0, "getqueued called for an ordinary 400");
}

// DIFF_ISSUE is reported, but the hash is not actually in the queue listing (a stale TorBox-side report,
// or a race): addMagnet must not fabricate a queued id and must fall back to the dead-link answer.
#[tokio::test]
async fn add_magnet_already_queued_but_hash_missing_from_listing_stays_a_dead_link() {
    let hash = "a".repeat(40);
    let s = TorBoxStore {
        token: "tb-diffissue-miss".to_owned(),
        api: TORBOX_API.to_owned(),
        cache: Some(Arc::new(MemoryCache::new(1 << 20))),
        client: Arc::new(MockDoer(|req| match base(&req).as_str() {
            "createtorrent" => Ok(resp(400, r#"{"error":"DIFF_ISSUE","detail":"Download already queued."}"#)),
            "getqueued" => Ok(resp(200, r#"{"success":true,"data":[]}"#)),
            _ => Ok(resp(404, "{}")),
        })),
    };
    let err = s.add_magnet(&hash, false, false).await.expect_err("addMagnet err must be a DeadLinkError");
    assert!(matches!(err, StoreError::DeadLink(_)), "addMagnet err = {err:?}, want a DeadLinkError");
    assert_eq!(s.known_queued_id(&hash), None, "no queued marker should be written when the hash was not found in the listing");
}

// /play answers 202 for a release TorBox queued, never 404 — den#204.
//
// Gated until package 4 lands the handler (`NewHandler`, `do`, `cancelTorBoxHandler`).
#[test]
#[ignore = "needs handler from package 4"]
fn play_queued_add_answers_202() {
    // Enable when package 4 lands: move this body up and delete the wrapper.
    #[cfg(any())]
    fn pending() {
    use crate::tests::handler::{do_req, valid_blob};
    let hash = "b".repeat(40);
    let (h, _) = crate::tests::cancel::cancel_torbox_handler(
        |req| match base(req).as_str() {
            "createtorrent" => resp(200, r#"{"success":true,"data":{"queued_id":9}}"#),
            "mylist" => resp(200, r#"{"success":true,"data":[]}"#),
            _ => resp(404, "{}"),
        },
        None,
    );
    let rr = do_req(&h, &format!("/{}/play/{}", valid_blob(), crate::play::encode_play_token(&crate::play::PlayTarget {
        info_hash: hash,
        file_idx: Some(0),
        ..Default::default()
    })), None);
    assert_eq!(rr.status(), 202, "play on a queued add: {:?}", rr.body());
    }
    todo!("pending: queued_test.go:202 TestPlay_queuedAddAnswers202")
}

// StatusAnswer only ever asked mylist (the torrent listing), never TorBox's own queue — so a release
// parked in the queue with no cached marker yet (a cold hash, or one whose marker's TTL lapsed) answered
// "not fetching this" exactly like one TorBox never heard of. That is what made the probe route answer
// 404 not_queued forever for a release still genuinely downloading, once den-scout started checking only
// the active list.
#[tokio::test]
async fn status_answer_queued_release_with_no_marker_is_found_via_getqueued() {
    let hash = "d".repeat(40);
    let queued_hash = hash.clone();
    let s = TorBoxStore {
        token: "tb-status-queued".to_owned(),
        api: TORBOX_API.to_owned(),
        cache: Some(Arc::new(MemoryCache::new(1 << 20))),
        client: Arc::new(MockDoer(move |req| match base(&req).as_str() {
            // The torrent listing: TorBox has never heard of this hash as a torrent at all.
            "mylist" => Ok(resp(200, r#"{"success":true,"data":[]}"#)),
            "getqueued" => Ok(resp(200, &format!(r#"{{"success":true,"data":[{{"id":77,"hash":"{queued_hash}"}}]}}"#))),
            _ => Ok(resp(404, "{}")),
        })),
    };
    let target = ResolveTarget { info_hash: hash.clone(), ..Default::default() };
    let status = s.status(&target).await.expect("Status ok = false, want true for a release sitting in TorBox's own queue");
    assert_eq!(status.state, FETCH_QUEUED, "status.State");
    assert_eq!(status.service, Some(DebridService::TorBox), "status.Service");
    assert!(s.known_queued_id(&hash).is_some(), "a discovered queue entry should be memoised so the next poll avoids another getqueued call");
}

// The probe route (?probe=1) is what Home's progress bar polls, and must answer the same "coming" state
// as /play for a release parked in TorBox's queue — never 404 not_queued, which the client reads as the
// release being gone.
//
// Gated until package 4 lands the handler (`NewHandler`, `do`, `cancelTorBoxHandler`).
#[test]
#[ignore = "needs handler from package 4"]
fn probe_queued_release_answers_202_not_not_queued() {
    // Enable when package 4 lands: move this body up and delete the wrapper.
    #[cfg(any())]
    fn pending() {
    use crate::tests::handler::{do_req, valid_blob};
    let hash = "e".repeat(40);
    let queued_hash = hash.clone();
    let (h, _) = crate::tests::cancel::cancel_torbox_handler(
        move |req| match base(req).as_str() {
            "mylist" => resp(200, r#"{"success":true,"data":[]}"#),
            "getqueued" => resp(200, &format!(r#"{{"success":true,"data":[{{"id":88,"hash":"{queued_hash}"}}]}}"#)),
            _ => resp(404, "{}"),
        },
        None,
    );
    let rr = do_req(&h, &format!("/{}/play/{}?probe=1", valid_blob(), crate::play::encode_play_token(&crate::play::PlayTarget {
        info_hash: hash,
        file_idx: Some(0),
        ..Default::default()
    })), None);
    assert_eq!(rr.status(), 202, "probe on a queued release: {:?}", rr.body());
    let body: serde_json::Value = serde_json::from_slice(rr.body()).expect("probe body did not decode");
    assert_eq!(body["state"], "queued", "probe state");
    assert_eq!(body["service"], "torbox", "probe service");
    }
    todo!("pending: queued_test.go:255 TestProbe_queuedReleaseAnswers202NotNotQueued")
}
