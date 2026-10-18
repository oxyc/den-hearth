//! The per-release download state log, through the play route with a fake TorBox.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use crate::cache::{Cache, MemoryCache};
use crate::handler::new_handler;
use crate::loglimit::Capture;
use crate::play::{encode_play_token, PlayTarget};
use crate::stores::refusal::torrent_id_key;
use crate::stores::torbox::{clear_torbox_status, TorBoxStore, RESOLVE_CACHE_TTL, TORBOX_API};
use crate::stores::Store;
use crate::tests::handler::{get, shared_stores, test_deps, valid_blob};
use crate::tests::mock::{resp, MockDoer};

fn base(req: &reqwest::Request) -> String {
    req.url().path().rsplit('/').next().unwrap_or("").to_owned()
}

fn probe(hash: &str, episode: i64, title: &str) -> String {
    format!(
        "/{}/play/{}?probe=1",
        valid_blob(),
        encode_play_token(&PlayTarget {
            info_hash: hash.to_owned(),
            file_idx: Some(episode - 1),
            season: Some(2),
            episode: Some(episode),
            title: title.to_owned(),
            imdb: "tt6623682".to_owned(),
            ..Default::default()
        })
    )
}

fn changes(log: &str) -> Vec<String> {
    log.lines().filter(|l| l.starts_with("event=download outcome=changed")).map(str::to_owned).collect()
}

/// A season pack polled for two episodes in the same minute logs each episode's state once, and a later change of
/// one of them logs again naming the state it left. The condition-keyed sample line alone said "1 more like this"
/// for the second episode and never named it.
#[tokio::test]
async fn each_episode_logs_its_own_state_changes() {
    let pack = "f7c1a15ae28b".to_owned() + &"0".repeat(28);
    let stalled = Arc::new(AtomicBool::new(false));
    let swarm = stalled.clone();
    let cache = Arc::new(MemoryCache::new(1 << 20));
    cache.put(&torrent_id_key("tb-secret", &pack), "42", RESOLVE_CACHE_TTL);
    let store = Arc::new(TorBoxStore {
        token: "tb-secret".to_owned(),
        cache: Some(cache.clone()),
        api: TORBOX_API.to_owned(),
        client: Arc::new(MockDoer(move |req| {
            Ok(match base(&req).as_str() {
                "mylist" if swarm.load(Ordering::SeqCst) => resp(
                    200,
                    r#"{"success":true,"data":{"id":42,"progress":0.4,"download_finished":false,"download_state":"stalled (no seeds)","seeds":0,"peers":0}}"#,
                ),
                "mylist" => resp(
                    200,
                    r#"{"success":true,"data":{"id":42,"progress":0.4,"download_finished":false,"download_state":"downloading","seeds":3,"peers":1}}"#,
                ),
                _ => resp(404, "{}"),
            })
        })),
    });
    let shared: Arc<dyn Store> = store.clone();
    let deps_cache = cache.clone();
    let h = new_handler(test_deps(move |d| {
        d.cache = deps_cache.clone();
        d.make_stores = shared_stores(shared.clone());
    }));
    let title = "Rebecka.Martinsson.S02.1080p.AMZN.WEB-DL.DD+2.0.H.264-playWEB";
    let log = Capture::start();

    assert_eq!(get(&h, &probe(&pack, 2, title), &[]).await.status(), 202);
    assert_eq!(get(&h, &probe(&pack, 3, title), &[]).await.status(), 202);
    assert_eq!(get(&h, &probe(&pack, 2, title), &[]).await.status(), 202);
    let first = changes(&log.text());
    assert_eq!(first.len(), 2, "one line per episode, none for an unchanged answer: {first:#?}");
    for (line, episode) in first.iter().zip([2, 3]) {
        assert!(line.contains("reason=probe: none -> downloading"), "{line}");
        assert!(line.contains("upstream=torbox"), "{line}");
        assert!(line.contains(&format!("season=2 episode={episode} ")), "{line}");
        assert!(line.contains("hash=f7c1a15ae28b"), "{line}");
    }

    // The swarm empties: the episode's next answer is a change, and says what it changed from.
    stalled.store(true, Ordering::SeqCst);
    clear_torbox_status(&*cache, &store.account_identity(), &pack);
    assert_eq!(get(&h, &probe(&pack, 2, title), &[]).await.status(), 202);
    let all = changes(&log.text());
    assert_eq!(all.len(), 3, "{all:#?}");
    assert!(all[2].contains("reason=probe: downloading -> stalled"), "{all:#?}");
    assert!(all[2].contains("episode=2 "), "{}", all[2]);
}

/// The memory is bounded: past `REMEMBERED` releases the longest-remembered is forgotten, and its next answer is a
/// first sight again rather than growing the map with traffic.
#[test]
fn remembered_states_are_bounded() {
    use crate::releaselog::{States, REMEMBERED};
    let mut states = States::default();
    assert_eq!(states.change("first", "queued").as_deref(), Some("none"));
    assert_eq!(states.change("first", "queued"), None);
    for i in 0..REMEMBERED {
        states.change(&i.to_string(), "queued");
    }
    assert_eq!(states.change("first", "queued").as_deref(), Some("none"), "the oldest entry was evicted");
}
