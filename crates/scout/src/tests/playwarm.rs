//! Go: `internal/scout/playwarm_test.go` (169 lines). Work package 4.
//!
//! Every test here drives `/play` through a real TorBox store. Also home to `held_torbox_play` and `same_calls`, which
//! `linkcheck_test.go` shares.

use std::sync::atomic::{AtomicI32, Ordering};
use std::sync::{Arc, Mutex};

use crate::cache::{Cache, MemoryCache};
use crate::deps::Deps;
use crate::handler::{new_handler, Handler};
use crate::play::{encode_play_token, PlayTarget};
use crate::stores::refusal::torrent_id_key;
use crate::stores::torbox::{TorBoxStore, RESOLVE_CACHE_TTL, TORBOX_API};
use crate::stores::Store;
use crate::tests::handler::{get, hdr, shared_stores, test_deps, valid_blob};
use crate::tests::mock::{resp, MockDoer};

/// Go `heldTorBoxPlay`: a handler whose one store is a real TorBox store already holding `hash` as torrent 42,
/// answering upstream through `reply`. Every upstream call is recorded by its endpoint name, in order. `over`
/// adjusts the handler's deps further, after the store is wired.
pub(crate) fn held_torbox_play(
    hash: &str,
    reply: impl Fn(&reqwest::Request) -> reqwest::Response + Send + Sync + 'static,
    over: impl FnOnce(&mut Deps),
) -> (Handler, Arc<Mutex<Vec<String>>>) {
    let calls: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
    let cache = Arc::new(MemoryCache::new(1 << 20));
    cache.put(&torrent_id_key("tb-secret", hash), "42", RESOLVE_CACHE_TTL);
    let recorder = calls.clone();
    let store: Arc<dyn Store> = Arc::new(TorBoxStore {
        token: "tb-secret".to_owned(),
        cache: Some(cache.clone()),
        api: TORBOX_API.to_owned(),
        client: Arc::new(MockDoer(move |req: reqwest::Request| {
            recorder.lock().expect("the lock").push(req.url().path().rsplit('/').next().unwrap_or("").to_owned());
            Ok(reply(&req))
        })),
    });
    let h = new_handler(test_deps(move |d| {
        d.cache = cache;
        d.make_stores = shared_stores(store.clone());
        over(d);
    }));
    (h, calls)
}

/// Go `sameCalls`: the upstream calls, in order, equal `want`.
pub(crate) fn same_calls(got: &[String], want: &[&str]) -> bool {
    got.len() == want.len() && got.iter().zip(want).all(|(g, w)| g == w)
}

fn endpoint(req: &reqwest::Request) -> &str {
    req.url().path().rsplit('/').next().unwrap_or("")
}

/// A play of a release the account already holds resolves straight away: no status read first, and no add.
/// The status read was a mylist round trip that, for a finished torrent, only ever said "not downloading".
#[tokio::test]
async fn play_a_held_release_skips_the_status_read() {
    let hash = "a".repeat(40);
    for (name, target, want) in [
        // Was mylist (status) + requestdl.
        ("movie", PlayTarget { info_hash: hash.clone(), file_idx: Some(0), ..Default::default() }, vec!["requestdl"]),
        // Was mylist (status) + mylist (file list) + requestdl.
        (
            "episode",
            PlayTarget { info_hash: hash.clone(), season: Some(1), episode: Some(2), ..Default::default() },
            vec!["mylist", "requestdl"],
        ),
    ] {
        let (h, calls) = held_torbox_play(
            &hash,
            |r| match endpoint(r) {
                "requestdl" => resp(200, r#"{"success":true,"data":"https://cdn.torbox/held.mkv"}"#),
                "mylist" => resp(
                    200,
                    r#"{"success":true,"data":{"id":42,"files":[{"id":0,"name":"Show.S01E01.mkv","size":10},{"id":1,"name":"Show.S01E02.mkv","size":20}]}}"#,
                ),
                _ => resp(404, "{}"),
            },
            |_| {},
        );
        let rr = get(&h, &format!("/{}/play/{}", valid_blob(), encode_play_token(&target)), &[]).await;
        assert!(
            rr.status() == 302 && hdr(&rr, "location") == "https://cdn.torbox/held.mkv",
            "{name}: play: {} {:?}",
            rr.status(),
            hdr(&rr, "location")
        );
        let got = calls.lock().expect("the lock").clone();
        assert!(same_calls(&got, &want), "{name}: upstream calls = {got:?}, want {want:?}");
    }
}

/// When the read-only resolve cannot serve, /play runs exactly what it ran before: the status read, and then the
/// resolve that may add.
#[tokio::test]
async fn play_a_held_resolve_that_fails_falls_back_to_the_old_sequence() {
    let hash = "a".repeat(40);
    let requestdls = Arc::new(AtomicI32::new(0));
    let counter = requestdls.clone();
    let (h, calls) = held_torbox_play(
        &hash,
        move |r| match endpoint(r) {
            "requestdl" => {
                if counter.fetch_add(1, Ordering::SeqCst) + 1 == 1 {
                    resp(200, r#"{"success":false}"#) // "no link yet"
                } else {
                    resp(200, r#"{"success":true,"data":"https://cdn.torbox/second.mkv"}"#)
                }
            }
            "mylist" => resp(200, r#"{"success":true,"data":{"id":42,"progress":1,"download_finished":true}}"#),
            _ => resp(404, "{}"),
        },
        |_| {},
    );
    let rr = get(
        &h,
        &format!("/{}/play/{}", valid_blob(), encode_play_token(&PlayTarget { info_hash: hash, file_idx: Some(0), ..Default::default() })),
        &[],
    )
    .await;
    assert!(
        rr.status() == 302 && hdr(&rr, "location") == "https://cdn.torbox/second.mkv",
        "play: {} {:?}",
        rr.status(),
        hdr(&rr, "location")
    );
    let got = calls.lock().expect("the lock").clone();
    assert!(
        same_calls(&got, &["requestdl", "mylist", "requestdl"]),
        "upstream calls = {got:?}, want [requestdl, mylist, requestdl] (read-only try, then status, then the old resolve)"
    );
}

/// NoAdd is what keeps the new first step add-free, and this is the case where it has to: the id the cache
/// remembers is stale, so the only way to a link is to buy the torrent again. The read-only step must
/// decline and leave that to the old sequence, which reads status before it adds.
#[tokio::test]
async fn play_the_held_step_never_adds_even_for_a_stale_id() {
    let hash = "a".repeat(40);
    let (h, calls) = held_torbox_play(
        &hash,
        |r| match endpoint(r) {
            "requestdl" => {
                if r.url().query_pairs().any(|(k, v)| k == "torrent_id" && v == "42") {
                    resp(404, "{}") // TorBox no longer has torrent 42
                } else {
                    resp(200, r#"{"success":true,"data":"https://cdn.torbox/rebought.mkv"}"#)
                }
            }
            "mylist" => resp(200, r#"{"success":true,"data":[]}"#),
            "createtorrent" => resp(200, r#"{"success":true,"data":{"torrent_id":43}}"#),
            _ => resp(404, "{}"),
        },
        |_| {},
    );
    get(
        &h,
        &format!("/{}/play/{}", valid_blob(), encode_play_token(&PlayTarget { info_hash: hash, file_idx: Some(0), ..Default::default() })),
        &[],
    )
    .await;

    let calls = calls.lock().expect("the lock").clone();
    assert!(calls.first().is_some_and(|c| c == "requestdl"), "upstream calls = {calls:?}, want the read-only resolve first");
    for c in &calls {
        if c == "mylist" {
            break;
        }
        assert!(c != "createtorrent", "added before any status read: {calls:?} — the held step spent an add");
    }
}

/// A download in progress reuses the bounded status snapshot. The held step would cost a failing link
/// request on every poll, while asking mylist every two seconds used 30 calls/minute/viewer.
#[tokio::test]
async fn play_a_download_in_progress_is_polled_with_the_status_read_alone() {
    let hash = "a".repeat(40);
    let (h, calls) = held_torbox_play(
        &hash,
        |r| match endpoint(r) {
            "requestdl" => resp(200, r#"{"success":false}"#),
            "mylist" => resp(200, r#"{"success":true,"data":{"id":42,"progress":0.5,"download_finished":false}}"#),
            _ => resp(404, "{}"),
        },
        |_| {},
    );
    let play_path = format!("/{}/play/{}", valid_blob(), encode_play_token(&PlayTarget { info_hash: hash, file_idx: Some(0), ..Default::default() }));

    let rr = get(&h, &play_path, &[]).await;
    assert_eq!(rr.status(), 202, "first poll: {}, want 202", rr.status());
    let got = calls.lock().expect("the lock").clone();
    assert!(same_calls(&got, &["requestdl", "mylist"]), "first poll calls = {got:?}, want [requestdl, mylist]");
    calls.lock().expect("the lock").clear();
    let rr = get(&h, &play_path, &[]).await;
    assert_eq!(rr.status(), 202, "second poll: {}, want 202", rr.status());
    let got = calls.lock().expect("the lock").clone();
    assert!(got.is_empty(), "second poll calls = {got:?}, want the shared snapshot with no upstream call");
}
