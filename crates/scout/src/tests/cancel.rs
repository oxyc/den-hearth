//! Go: `internal/scout/cancel_test.go` (272 lines). Work package 1.
//!
//! Every test here goes through the handler (`NewHandler`, `testDeps`, `doMethod`, package 4) and the cache
//! keys of package 2 (`torrentIDKey`), so the module is cfg-gated until both land. The bodies are ported as
//! written; `sameCalls` is an order-sensitive equality, so it is an `assert_eq!` on the recorded call list.

#[cfg(any())]
mod gated {
    use std::sync::{Arc, Mutex};

    use crate::cache::{Cache, MemoryCache};
    use crate::handler::{new_handler, Handler};
    use crate::play::{encode_play_token, PlayTarget};
    use crate::stores::realdebrid::RealDebridStore;
    use crate::stores::refusal::torrent_id_key;
    use crate::stores::torbox::{mark_added_by_us, TorBoxStore, RESOLVE_CACHE_TTL, TORBOX_API};
    use crate::stores::Store;
    use crate::tests::handler::{do_method, test_deps, valid_blob};
    use crate::tests::mock::{resp, MockDoer};

    /// cancelTorBoxHandler is a handler whose one store is a real TorBox store, answering upstream through
    /// `reply`. `prime` runs against the cache before any request is made, so a test can mark a hash as
    /// scout's own add (or leave it unmarked, for the "not ours" case) and seed the torrent id the way Resolve
    /// or Status would have.
    fn cancel_torbox_handler(
        reply: impl Fn(&reqwest::Request) -> reqwest::Response + Send + Sync + 'static,
        prime: Option<Box<dyn FnOnce(&dyn Cache)>>,
    ) -> (Handler, Arc<Mutex<Vec<String>>>) {
        let calls: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
        let cache = Arc::new(MemoryCache::new(1 << 20));
        if let Some(prime) = prime {
            prime(&*cache);
        }
        let recorder = calls.clone();
        let store: Arc<dyn Store> = Arc::new(TorBoxStore {
            token: "tb-secret".to_owned(),
            cache: Some(cache.clone()),
            api: TORBOX_API.to_owned(),
            client: Arc::new(MockDoer(move |req| {
                recorder.lock().unwrap().push(base(&req));
                Ok(reply(&req))
            })),
        });
        let h = new_handler(test_deps(move |d| {
            d.cache = Some(cache.clone());
            let store = store.clone();
            d.make_stores = Box::new(move |_| vec![store.clone()]);
        }));
        (h, calls)
    }

    fn base(req: &reqwest::Request) -> String {
        req.url().path().rsplit('/').next().unwrap_or("").to_owned()
    }

    fn cancel_path(hash: &str) -> String {
        format!("/{}/play/{}", valid_blob(), encode_play_token(&PlayTarget {
            info_hash: hash.to_owned(),
            file_idx: Some(0),
            ..Default::default()
        }))
    }

    // 204: scout's own torrent, not finished, TorBox accepts the delete — and the memo/marker it leaves
    // behind is gone afterwards, so a second cancel of the same hash finds nothing left to cancel.
    #[test]
    fn cancel_ours_and_unfinished() {
        let hash = "a".repeat(40);
        let prime_hash = hash.clone();
        let (h, calls) = cancel_torbox_handler(
            |req| match (base(req).as_str(), req.url().query_pairs().any(|(k, _)| k == "id")) {
                ("mylist", true) => resp(200, r#"{"success":true,"data":{"id":42,"download_finished":false,"download_state":"downloading"}}"#),
                // The account listing (no `id`): empty, as it is once the delete below has taken effect.
                ("mylist", false) => resp(200, r#"{"success":true,"data":[]}"#),
                ("controltorrent", _) => resp(200, r#"{"success":true}"#),
                _ => resp(404, "{}"),
            },
            Some(Box::new(move |cache| {
                cache.put(&torrent_id_key("tb-secret", &prime_hash), "42", RESOLVE_CACHE_TTL);
                mark_added_by_us(cache, "tb-secret", &prime_hash);
            })),
        );
        let rr = do_method(&h, "DELETE", &cancel_path(&hash), None);
        assert_eq!(rr.status(), 204, "cancel: want 204");
        assert_eq!(*calls.lock().unwrap(), vec!["mylist", "controltorrent"], "upstream calls");
        // The memo and marker must be gone: a second cancel of the same hash finds no known torrent at all
        // (mylist now answers empty, as TorBox's own account would once the delete has taken effect).
        let rr2 = do_method(&h, "DELETE", &cancel_path(&hash), None);
        assert_eq!(rr2.status(), 404, "cancelling the same hash again: want 404 (memo should be cleared)");
    }

    // 409 finished: a completed download holds no slot and is kept, never deleted.
    #[test]
    fn cancel_finished() {
        let hash = "a".repeat(40);
        let prime_hash = hash.clone();
        let (h, calls) = cancel_torbox_handler(
            |req| match base(req).as_str() {
                "mylist" => resp(200, r#"{"success":true,"data":{"id":42,"download_finished":true}}"#),
                _ => resp(404, "{}"),
            },
            Some(Box::new(move |cache| {
                cache.put(&torrent_id_key("tb-secret", &prime_hash), "42", RESOLVE_CACHE_TTL);
                mark_added_by_us(cache, "tb-secret", &prime_hash);
            })),
        );
        let rr = do_method(&h, "DELETE", &cancel_path(&hash), None);
        assert_eq!(rr.status(), 409, "cancel finished: want 409");
        assert!(String::from_utf8_lossy(rr.body()).contains("finished"), "body = {:?}, want an error naming \"finished\"", rr.body());
        assert!(!calls.lock().unwrap().iter().any(|c| c == "controltorrent"), "a finished torrent must never be deleted: calls = {:?}", calls.lock().unwrap());
    }

    // 409 not_ours: the account held this hash before scout ever added it (no addedByUs marker), and it is
    // healthy — downloading fine, just not scout's to touch.
    #[test]
    fn cancel_not_ours() {
        let hash = "a".repeat(40);
        let prime_hash = hash.clone();
        let (h, _) = cancel_torbox_handler(
            |req| match base(req).as_str() {
                "mylist" => resp(200, r#"{"success":true,"data":{"id":42,"download_finished":false,"download_state":"downloading","seeds":5,"peers":3}}"#),
                _ => resp(404, "{}"),
            },
            Some(Box::new(move |cache| {
                cache.put(&torrent_id_key("tb-secret", &prime_hash), "42", RESOLVE_CACHE_TTL);
                // Deliberately no mark_added_by_us: this torrent id was discovered, not added by this process.
            })),
        );
        let rr = do_method(&h, "DELETE", &cancel_path(&hash), None);
        let body = String::from_utf8_lossy(rr.body()).into_owned();
        assert!(rr.status() == 409 && body.contains("not_ours"), "cancel not-ours: {} {body}, want 409 not_ours", rr.status());
    }

    // The narrow ownership exception: a torrent with no addedByUs marker that is stalled dead (no seeds, no
    // peers — TorBox's own "stalled" vocabulary) and not finished IS cancelled. This is den's actual incident:
    // a pack hash held a TorBox slot forever because the client's fallback had already abandoned it for
    // every episode, and `not_ours` meant no cancel ever reached TorBox since scout had never recorded adding
    // it (added before ownership tracking existed, or by some other path).
    #[test]
    fn cancel_not_ours_but_stalled_is_cancelled() {
        let hash = "a".repeat(40);
        let prime_hash = hash.clone();
        let (h, calls) = cancel_torbox_handler(
            |req| match base(req).as_str() {
                "mylist" => resp(200, r#"{"success":true,"data":{"id":42,"download_finished":false,"download_state":"stalled (no seeds)","seeds":0,"peers":0}}"#),
                "controltorrent" => resp(200, r#"{"success":true}"#),
                _ => resp(404, "{}"),
            },
            Some(Box::new(move |cache| {
                cache.put(&torrent_id_key("tb-secret", &prime_hash), "42", RESOLVE_CACHE_TTL);
                // Deliberately no mark_added_by_us.
            })),
        );
        let rr = do_method(&h, "DELETE", &cancel_path(&hash), None);
        assert_eq!(rr.status(), 204, "cancel not-ours-but-stalled: want 204");
        assert_eq!(*calls.lock().unwrap(), vec!["mylist", "controltorrent"], "upstream calls");
    }

    // The same torrent, not stalled (an active, healthy swarm) and still with no addedByUs marker: the
    // exception must not widen into "any unmarked torrent may be cancelled" — only a dead one.
    #[test]
    fn cancel_not_ours_and_not_stalled_is_refused() {
        let hash = "a".repeat(40);
        let prime_hash = hash.clone();
        let (h, calls) = cancel_torbox_handler(
            |req| match base(req).as_str() {
                "mylist" => resp(200, r#"{"success":true,"data":{"id":42,"download_finished":false,"download_state":"downloading","seeds":12,"peers":4}}"#),
                _ => resp(404, "{}"),
            },
            Some(Box::new(move |cache| {
                cache.put(&torrent_id_key("tb-secret", &prime_hash), "42", RESOLVE_CACHE_TTL);
            })),
        );
        let rr = do_method(&h, "DELETE", &cancel_path(&hash), None);
        let body = String::from_utf8_lossy(rr.body()).into_owned();
        assert!(rr.status() == 409 && body.contains("not_ours"), "cancel not-ours, healthy: {} {body}, want 409 not_ours", rr.status());
        assert!(!calls.lock().unwrap().iter().any(|c| c == "controltorrent"), "a healthy torrent with no addedByUs marker must never be deleted: calls = {:?}", calls.lock().unwrap());
    }

    // A finished torrent with no addedByUs marker is still kept — "finished" outranks the stalled exception
    // the same way it outranks "ours": torboxIsFinished is checked before either ownership question.
    #[test]
    fn cancel_not_ours_and_finished_is_refused() {
        let hash = "a".repeat(40);
        let prime_hash = hash.clone();
        let (h, calls) = cancel_torbox_handler(
            |req| match base(req).as_str() {
                "mylist" => resp(200, r#"{"success":true,"data":{"id":42,"download_finished":true,"download_state":"completed"}}"#),
                _ => resp(404, "{}"),
            },
            Some(Box::new(move |cache| {
                cache.put(&torrent_id_key("tb-secret", &prime_hash), "42", RESOLVE_CACHE_TTL);
            })),
        );
        let rr = do_method(&h, "DELETE", &cancel_path(&hash), None);
        let body = String::from_utf8_lossy(rr.body()).into_owned();
        assert!(rr.status() == 409 && body.contains("finished"), "cancel not-ours, finished: {} {body}, want 409 finished", rr.status());
        assert!(!calls.lock().unwrap().iter().any(|c| c == "controltorrent"), "a finished torrent must never be deleted: calls = {:?}", calls.lock().unwrap());
    }

    // 404 not_queued: nothing of ours to cancel — no known torrent id, and TorBox's own listing agrees.
    #[test]
    fn cancel_not_queued() {
        let hash = "a".repeat(40);
        let (h, _) = cancel_torbox_handler(|_| resp(200, r#"{"success":true,"data":[]}"#), None);
        let rr = do_method(&h, "DELETE", &cancel_path(&hash), None);
        let body = String::from_utf8_lossy(rr.body()).into_owned();
        assert!(rr.status() == 404 && body.contains("not_queued"), "cancel nothing queued: {} {body}, want 404 not_queued", rr.status());
    }

    // 503: TorBox refuses the delete itself (a throttle or a fault), not a verdict about the release.
    #[test]
    fn cancel_refused() {
        let hash = "a".repeat(40);
        let prime_hash = hash.clone();
        let (h, _) = cancel_torbox_handler(
            |req| match base(req).as_str() {
                "mylist" => resp(200, r#"{"success":true,"data":{"id":42,"download_finished":false}}"#),
                "controltorrent" => resp(500, r#"{"error":"server_error"}"#),
                _ => resp(404, "{}"),
            },
            Some(Box::new(move |cache| {
                cache.put(&torrent_id_key("tb-secret", &prime_hash), "42", RESOLVE_CACHE_TTL);
                mark_added_by_us(cache, "tb-secret", &prime_hash);
            })),
        );
        let rr = do_method(&h, "DELETE", &cancel_path(&hash), None);
        assert_eq!(rr.status(), 503, "cancel refused: want 503");
    }

    // 501 unsupported: Real-Debrid and Premiumize have no Cancel at all.
    #[test]
    fn cancel_unsupported_store() {
        let hash = "a".repeat(40);
        let store: Arc<dyn Store> = Arc::new(RealDebridStore {
            token: "rd-secret".to_owned(),
            api: String::new(),
            client: Arc::new(MockDoer(|_| Ok(resp(404, "{}")))),
        });
        let h = new_handler(test_deps(move |d| {
            let store = store.clone();
            d.make_stores = Box::new(move |_| vec![store.clone()]);
        }));
        let rr = do_method(&h, "DELETE", &cancel_path(&hash), None);
        let body = String::from_utf8_lossy(rr.body()).into_owned();
        assert!(rr.status() == 501 && body.contains("unsupported"), "cancel on an unsupported store: {} {body}, want 501 unsupported", rr.status());
    }

    // ?op=reannounce: the same safety rules, but TorBox is asked to reannounce rather than delete, and the
    // memo is NOT cleared — the torrent is still ours and still in flight.
    #[test]
    fn cancel_reannounce() {
        let hash = "a".repeat(40);
        let prime_hash = hash.clone();
        let deleted = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let deleted_in_reply = deleted.clone();
        let (h, calls) = cancel_torbox_handler(
            move |req| match base(req).as_str() {
                "mylist" => resp(200, r#"{"success":true,"data":{"id":42,"download_finished":false,"download_state":"stalled (no seeds)","seeds":0}}"#),
                "controltorrent" => {
                    deleted_in_reply.store(true, std::sync::atomic::Ordering::SeqCst);
                    resp(200, r#"{"success":true}"#)
                }
                _ => resp(404, "{}"),
            },
            Some(Box::new(move |cache| {
                cache.put(&torrent_id_key("tb-secret", &prime_hash), "42", RESOLVE_CACHE_TTL);
                mark_added_by_us(cache, "tb-secret", &prime_hash);
            })),
        );
        let rr = do_method(&h, "DELETE", &format!("{}?op=reannounce", cancel_path(&hash)), None);
        assert_eq!(rr.status(), 202, "reannounce: want 202");
        assert_eq!(*calls.lock().unwrap(), vec!["mylist", "controltorrent"], "upstream calls");
        assert!(deleted.load(std::sync::atomic::Ordering::SeqCst), "precondition failed: controltorrent was never called");
        // The memo must survive a reannounce: a later plain cancel still finds the torrent ours.
        calls.lock().unwrap().clear();
        let rr2 = do_method(&h, "DELETE", &cancel_path(&hash), None);
        assert_eq!(rr2.status(), 204, "cancel after reannounce: want 204 (the memo must have survived the reannounce)");
    }

}
