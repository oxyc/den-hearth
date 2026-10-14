//! Go: `internal/scout/playwarm_test.go` (169 lines). Work package 4.
//!
//! Every test here drives `/play` through a real TorBox store (package 1's `stores/torbox.rs`), so each keeps its
//! Go body in a compiled-out `pending` item until that store lands.

/// A play of a release the account already holds resolves straight away: no status read first, and no add.
#[tokio::test]
#[ignore = "needs package 1's torBoxStore (held-release resolve)"]
async fn play_a_held_release_skips_the_status_read() {
    // Enable when package 1 lands: move `pending` up and delete the wrapper.
    #[cfg(any())]
    async fn pending() {
        // Go TestPlay_aHeldReleaseSkipsTheStatusRead: was mylist (status) + requestdl for a movie; was mylist (status)
        // + mylist (file list) + requestdl for an episode.
        let hash = "a".repeat(40);
        for (name, target, want) in [
            ("movie", PlayTarget { info_hash: hash.clone(), file_idx: Some(0), ..Default::default() }, vec!["requestdl"]),
            ("episode", PlayTarget { info_hash: hash.clone(), season: Some(1), episode: Some(2), ..Default::default() }, vec!["mylist", "requestdl"]),
        ] {
            let (h, calls) = held_torbox_play(&hash, |r| match r.uri().path().rsplit('/').next().unwrap_or("") {
                "requestdl" => resp(200, r#"{"success":true,"data":"https://cdn.torbox/held.mkv"}"#),
                "mylist" => resp(200, r#"{"success":true,"data":{"id":42,"files":[{"id":0,"name":"Show.S01E01.mkv","size":10},{"id":1,"name":"Show.S01E02.mkv","size":20}]}}"#),
                _ => resp(404, "{}"),
            }, vec![]);
            let rr = get(&h, &format!("/{}/play/{}", valid_blob(), encode_play_token(&target)), &[]).await;
            assert!(rr.status() == 302 && hdr(&rr, "location") == "https://cdn.torbox/held.mkv", "play {name}: {} {:?}", rr.status(), hdr(&rr, "location"));
            assert_eq!(*calls.lock().expect("the lock"), want, "upstream calls for {name}");
        }
    }
    todo!("pending: playwarm_test.go:TestPlay_aHeldReleaseSkipsTheStatusRead")
}

/// When the read-only resolve cannot serve, /play runs exactly what it ran before: the status read, and then the
/// resolve that may add.
#[tokio::test]
#[ignore = "needs package 1's torBoxStore (held resolve fallback)"]
async fn play_a_held_resolve_that_fails_falls_back_to_the_old_sequence() {
    // Enable when package 1 lands: move `pending` up and delete the wrapper.
    #[cfg(any())]
    async fn pending() {
        // Go TestPlay_aHeldResolveThatFailsFallsBackToTheOldSequence: the read-only try first, then the status read,
        // then the old resolve.
        let hash = "a".repeat(40);
        let requestdls = Arc::new(AtomicI32::new(0));
        let rd = requestdls.clone();
        let (h, calls) = held_torbox_play(&hash, move |r| match r.uri().path().rsplit('/').next().unwrap_or("") {
            "requestdl" => {
                if rd.fetch_add(1, Ordering::SeqCst) == 0 {
                    resp(200, r#"{"success":false}"#) // "no link yet"
                } else {
                    resp(200, r#"{"success":true,"data":"https://cdn.torbox/second.mkv"}"#)
                }
            }
            "mylist" => resp(200, r#"{"success":true,"data":{"id":42,"progress":1,"download_finished":true}}"#),
            _ => resp(404, "{}"),
        }, vec![]);
        let rr = get(&h, &format!("/{}/play/{}", valid_blob(), encode_play_token(&PlayTarget { info_hash: hash, file_idx: Some(0), ..Default::default() })), &[]).await;
        assert!(rr.status() == 302 && hdr(&rr, "location") == "https://cdn.torbox/second.mkv", "play: {} {:?}", rr.status(), hdr(&rr, "location"));
        assert_eq!(*calls.lock().expect("the lock"), vec!["requestdl", "mylist", "requestdl"], "upstream calls");
    }
    todo!("pending: playwarm_test.go:TestPlay_aHeldResolveThatFailsFallsBackToTheOldSequence")
}

/// NoAdd is what keeps the new first step add-free: the id the cache remembers is stale, so the only way to a link is
/// to buy the torrent again. The read-only step must decline and leave that to the old sequence.
#[tokio::test]
#[ignore = "needs package 1's torBoxStore (no-add guarantee)"]
async fn play_the_held_step_never_adds_even_for_a_stale_id() {
    // Enable when package 1 lands: move `pending` up and delete the wrapper.
    #[cfg(any())]
    async fn pending() {
        // Go TestPlay_theHeldStepNeverAddsEvenForAStaleID: the first upstream call is the read-only resolve, and no
        // add happens before the first status read.
        let hash = "a".repeat(40);
        let (h, calls) = held_torbox_play(&hash, |r| match r.uri().path().rsplit('/').next().unwrap_or("") {
            "requestdl" => {
                if r.uri().query().unwrap_or("").contains("torrent_id=42") {
                    resp(404, "{}") // TorBox no longer has torrent 42
                } else {
                    resp(200, r#"{"success":true,"data":"https://cdn.torbox/rebought.mkv"}"#)
                }
            }
            "mylist" => resp(200, r#"{"success":true,"data":[]}"#),
            "createtorrent" => resp(200, r#"{"success":true,"data":{"torrent_id":43}}"#),
            _ => resp(404, "{}"),
        }, vec![]);
        get(&h, &format!("/{}/play/{}", valid_blob(), encode_play_token(&PlayTarget { info_hash: hash, file_idx: Some(0), ..Default::default() })), &[]).await;
        let calls = calls.lock().expect("the lock").clone();
        assert_eq!(calls.first().map(String::as_str), Some("requestdl"), "want the read-only resolve first: {calls:?}");
        if let Some(status) = calls.iter().position(|c| c == "mylist") {
            assert!(!calls[..status].iter().any(|c| c == "createtorrent"), "added before any status read: {calls:?}");
        }
    }
    todo!("pending: playwarm_test.go:TestPlay_theHeldStepNeverAddsEvenForAStaleID")
}

/// A download in progress reuses the bounded status snapshot: the held step would cost a failing link request on
/// every poll.
#[tokio::test]
#[ignore = "needs package 1's torBoxStore (poll snapshot)"]
async fn play_a_download_in_progress_is_polled_with_the_status_read_alone() {
    // Enable when package 1 lands: move `pending` up and delete the wrapper.
    #[cfg(any())]
    async fn pending() {
        // Go TestPlay_aDownloadInProgressIsPolledWithTheStatusReadAlone: the first poll reads requestdl then mylist and
        // answers 202; the second poll makes no upstream call at all.
        let hash = "a".repeat(40);
        let (h, calls) = held_torbox_play(&hash, |r| match r.uri().path().rsplit('/').next().unwrap_or("") {
            "requestdl" => resp(200, r#"{"success":false}"#),
            "mylist" => resp(200, r#"{"success":true,"data":{"id":42,"progress":0.5,"download_finished":false}}"#),
            _ => resp(404, "{}"),
        }, vec![]);
        let play = format!("/{}/play/{}", valid_blob(), encode_play_token(&PlayTarget { info_hash: hash, file_idx: Some(0), ..Default::default() }));
        assert_eq!(get(&h, &play, &[]).await.status(), 202, "first poll");
        assert_eq!(*calls.lock().expect("the lock"), vec!["requestdl", "mylist"], "first poll calls");
        calls.lock().expect("the lock").clear();
        assert_eq!(get(&h, &play, &[]).await.status(), 202, "second poll");
        assert!(calls.lock().expect("the lock").is_empty(), "second poll made upstream calls");
    }
    todo!("pending: playwarm_test.go:TestPlay_aDownloadInProgressIsPolledWithTheStatusReadAlone")
}

/// Go `heldTorBoxPlay`: a handler whose one store is a real TorBox store already holding `hash` as torrent 42,
/// answering upstream through `reply`. Every upstream call is recorded by its endpoint name, in order. `over`
/// adjusts the handler's deps further, after the store is wired. Lands with package 1's TorBox store.
#[cfg(any())]
fn held_torbox_play(
    hash: &str,
    reply: impl Fn(&http::Request<String>) -> http::Response<String> + Send + Sync + 'static,
    over: Vec<Box<dyn FnOnce(&mut Deps)>>,
) -> (Handler, Arc<Mutex<Vec<String>>>) {
    let calls = Arc::new(Mutex::new(Vec::new()));
    let cache = Arc::new(MemoryCache::new(1 << 20));
    cache.put(&torrent_id_key("tb-secret", hash), "42", RESOLVE_CACHE_TTL);
    let rec = calls.clone();
    let store = TorBoxStore::with_client("tb-secret", cache.clone(), MockDoer::new(move |r| {
        rec.lock().expect("the lock").push(r.uri().path().rsplit('/').next().unwrap_or("").to_owned());
        reply(r)
    }));
    let h = new_handler(test_deps(move |d| {
        d.cache = cache;
        d.make_stores = Arc::new(move |_| vec![Box::new(store.clone())]);
        for o in over {
            o(d);
        }
    }));
    (h, calls)
}
