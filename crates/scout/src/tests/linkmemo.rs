//! Go: `internal/scout/linkmemo_test.go` (246 lines). Work package 6.
//!
//! Also hosts `TallyStore`, the Go `tallyStore` that other test modules share. The pure memo tests are live;
//! the `/play` and probe tests drive the handler, so they wait for package 4.

use std::collections::HashMap;
use std::sync::atomic::{AtomicI32, Ordering};
use std::time::{Duration, UNIX_EPOCH};

use async_trait::async_trait;

use crate::config::{Config, DebridAccount, DebridService};
use crate::linkmemo::{link_memo_key, LinkMemo, LINK_MEMO_MAX_ENTRIES, LINK_MEMO_TTL};
use crate::stores::{ResolveTarget, Store, StoreError, StoreStatus};

/// Go `tallyStore`: counts every upstream-shaped call a pool can make of it, so a test can prove a memo hit
/// made none. `resolve` is handed the 1-based number of the call.
pub(crate) struct TallyStore {
    pub(crate) svc: DebridService,
    pub(crate) resolve: Box<dyn Fn(i32) -> Result<String, StoreError> + Send + Sync>,
    pub(crate) resolves: AtomicI32,
    pub(crate) statuses: AtomicI32,
    pub(crate) checks: AtomicI32,
}

impl TallyStore {
    pub(crate) fn calls(&self) -> i32 {
        self.resolves.load(Ordering::SeqCst)
            + self.statuses.load(Ordering::SeqCst)
            + self.checks.load(Ordering::SeqCst)
    }
}

#[async_trait]
impl Store for TallyStore {
    fn service(&self) -> DebridService {
        self.svc
    }

    async fn cache_check(&self, _hashes: &[String]) -> Result<HashMap<String, bool>, StoreError> {
        self.checks.fetch_add(1, Ordering::SeqCst);
        Ok(HashMap::new())
    }

    async fn resolve(&self, _t: &ResolveTarget) -> Result<String, StoreError> {
        let n = self.resolves.fetch_add(1, Ordering::SeqCst) + 1;
        (self.resolve)(n)
    }

    async fn status(&self, _t: &ResolveTarget) -> Option<StoreStatus> {
        self.statuses.fetch_add(1, Ordering::SeqCst);
        None
    }
}

/// Go `numberedLinks`: mints a different link on every resolve, so a test can tell which resolve a 302 came from.
pub(crate) fn numbered_links(n: i32) -> Result<String, StoreError> {
    Ok(format!("https://cdn.example/minted-{n}.mkv"))
}

#[test]
fn link_memo_expires_and_stays_bounded() {
    let m = LinkMemo::default();
    let now = UNIX_EPOCH + Duration::from_secs(1_000_000);
    m.put("k", "https://a", false, now);
    let got = m.get("k", now + LINK_MEMO_TTL - Duration::from_secs(1));
    assert!(matches!(&got, Some(g) if g.link == "https://a"), "a live entry was not returned: {got:?}");
    assert!(m.get("k", now + LINK_MEMO_TTL).is_none(), "an entry was served past its TTL");

    for i in 0..LINK_MEMO_MAX_ENTRIES + 10 {
        m.put(&format!("k{i}"), "https://x", false, now);
        if i == 0 {
            // Touched after insertion, so it is the most recently used when the ceiling bites.
            m.get("k0", now);
        }
    }
    assert!(
        m.lru_len() == LINK_MEMO_MAX_ENTRIES && m.map_len() == LINK_MEMO_MAX_ENTRIES,
        "memo holds {}/{} entries, want {LINK_MEMO_MAX_ENTRIES}",
        m.lru_len(),
        m.map_len()
    );
    assert!(m.get("k1", now).is_none(), "the least recently used entry survived the ceiling");
}

// markChecked must not restart the TTL: it counts from the mint, not from the last look.
#[test]
fn link_memo_mark_checked_keeps_the_mint_time() {
    let m = LinkMemo::default();
    let now = UNIX_EPOCH + Duration::from_secs(1_000_000);
    m.put("k", "https://a", false, now);
    m.mark_checked("k", "https://a");
    let got = m.get("k", now);
    assert!(matches!(&got, Some(g) if g.checked && g.expires == now + LINK_MEMO_TTL), "got {got:?}");
    // A different link under the same key is a newer mint, and a check of the old one says nothing about it.
    m.put("k", "https://b", false, now);
    m.mark_checked("k", "https://a");
    assert!(!m.get("k", now).is_some_and(|g| g.checked), "a check of one link was credited to another");
}

// The key is the account set plus the file. Two installs must never share a link, and the probe's
// read-only target must land on the same entry /play reads.
#[test]
fn link_memo_key_scopes_by_account_and_file() {
    let a = Config {
        debrid: vec![DebridAccount { service: DebridService::TorBox, token: "one".to_owned() }],
        ..Default::default()
    };
    let b = Config {
        debrid: vec![DebridAccount { service: DebridService::TorBox, token: "two".to_owned() }],
        ..Default::default()
    };
    let hash = "a".repeat(40);
    let movie = ResolveTarget { info_hash: hash.clone(), file_idx: Some(0), ..Default::default() };
    assert!(link_memo_key(&a, &movie) != link_memo_key(&b, &movie), "two accounts share a memo key");
    let read_only = ResolveTarget { no_add: true, ..movie.clone() };
    assert_eq!(
        link_memo_key(&a, &movie),
        link_memo_key(&a, &read_only),
        "the probe's NoAdd target keys differently from /play's"
    );
    let ep1 =
        ResolveTarget { info_hash: hash.clone(), season: Some(1), episode: Some(1), ..Default::default() };
    let ep2 = ResolveTarget { info_hash: hash, season: Some(1), episode: Some(2), ..Default::default() };
    assert!(link_memo_key(&a, &ep1) != link_memo_key(&a, &ep2), "two episodes of one pack share a memo key");
}

// Needs `handler::serve`, `Deps::for_test` and `TallyStore` in the handler's stores (package 4). The body is
// ported and compiled out until it lands.
#[test]
#[ignore = "needs handler::serve / Deps::for_test (package 4)"]
fn play_memo_serves_a_repeat_with_no_upstream_calls() {
    // Enable when package 4 lands: move this body up and delete the wrapper.
    #[cfg(any())]
    fn pending() {
        use crate::deps::Deps;
        use crate::handler::serve;
        let store = Arc::new(TallyStore {
            svc: DebridService::TorBox,
            resolve: Box::new(numbered_links),
            resolves: AtomicI32::new(0),
            statuses: AtomicI32::new(0),
            checks: AtomicI32::new(0),
        });
        let deps = Deps::for_test(|d| {
            d.make_stores = Box::new({
                let s = store.clone();
                move |_| vec![s.clone()]
            })
        });
        let path = format!(
            "/{}/play/{}",
            crate::tests::ticket::valid_blob(),
            encode_play_token(&PlayTarget {
                info_hash: "a".repeat(40),
                file_idx: Some(0),
                ..Default::default()
            })
        );

        let first = serve(&deps, get(&path));
        assert_eq!(first.status(), 302, "first play");
        let before = store.calls();
        let second = serve(&deps, get(&path));
        assert!(
            second.status() == 302 && second.headers()["location"] == first.headers()["location"],
            "repeat play: {} want 302 to the first link",
            second.status()
        );
        assert_eq!(store.calls() - before, 0, "a memo hit made store calls, want 0");
    }
    todo!("pending: linkmemo_test.go:~106 TestPlay_memoServesARepeatWithNoUpstreamCalls")
}

// fresh=1 skips the memo and replaces it: the client could not open the last link.
#[test]
#[ignore = "needs handler::serve / Deps::for_test (package 4)"]
fn play_fresh_bypasses_and_replaces_the_memo() {
    // Enable when package 4 lands: move this body up and delete the wrapper.
    #[cfg(any())]
    fn pending() {
        use crate::deps::Deps;
        use crate::handler::serve;
        let store = Arc::new(TallyStore {
            svc: DebridService::TorBox,
            resolve: Box::new(numbered_links),
            resolves: AtomicI32::new(0),
            statuses: AtomicI32::new(0),
            checks: AtomicI32::new(0),
        });
        let deps = Deps::for_test(|d| {
            d.make_stores = Box::new({
                let s = store.clone();
                move |_| vec![s.clone()]
            })
        });
        let path = format!(
            "/{}/play/{}",
            crate::tests::ticket::valid_blob(),
            encode_play_token(&PlayTarget {
                info_hash: "a".repeat(40),
                file_idx: Some(0),
                ..Default::default()
            })
        );

        let rr = serve(&deps, get(&path));
        assert_eq!(
            rr.headers()["location"],
            "https://cdn.example/minted-1.mkv",
            "first play: {}",
            rr.status()
        );
        let rr = serve(&deps, get(&format!("{path}?fresh=1")));
        assert_eq!(
            rr.headers()["location"],
            "https://cdn.example/minted-2.mkv",
            "fresh play did not mint a new link: {}",
            rr.status()
        );
        let rr = serve(&deps, get(&path));
        assert_eq!(
            rr.headers()["location"],
            "https://cdn.example/minted-2.mkv",
            "the fresh link did not replace the memo"
        );
        assert_eq!(store.resolves.load(Ordering::SeqCst), 2, "resolves");
    }
    todo!("pending: linkmemo_test.go:~128 TestPlay_freshBypassesAndReplacesTheMemo")
}

// fresh=1 drops the old link even when the new resolve fails, so the link the client just reported as
// unopenable is not served again by the next plain request.
#[test]
#[ignore = "needs handler::serve / Deps::for_test (package 4)"]
fn play_fresh_drops_the_old_link_even_when_the_resolve_fails() {
    // Enable when package 4 lands: move this body up and delete the wrapper.
    #[cfg(any())]
    fn pending() {
        use crate::deps::Deps;
        use crate::handler::serve;
        let store = Arc::new(TallyStore {
            svc: DebridService::TorBox,
            resolve: Box::new(|n| {
                if n == 1 {
                    numbered_links(n)
                } else {
                    Err(StoreError::DeadLink(DeadLinkError { reason: "gone".to_owned() }))
                }
            }),
            resolves: AtomicI32::new(0),
            statuses: AtomicI32::new(0),
            checks: AtomicI32::new(0),
        });
        let deps = Deps::for_test(|d| {
            d.make_stores = Box::new({
                let s = store.clone();
                move |_| vec![s.clone()]
            })
        });
        let path = format!(
            "/{}/play/{}",
            crate::tests::ticket::valid_blob(),
            encode_play_token(&PlayTarget {
                info_hash: "a".repeat(40),
                file_idx: Some(0),
                ..Default::default()
            })
        );

        serve(&deps, get(&path));
        let rr = serve(&deps, get(&format!("{path}?fresh=1")));
        assert_eq!(rr.status(), 404, "fresh play");
        let rr = serve(&deps, get(&path));
        assert_ne!(
            rr.status(),
            302,
            "the link the client reported as unopenable was served again: {:?}",
            rr.headers().get("location")
        );
    }
    todo!("pending: linkmemo_test.go:~153 TestPlay_freshDropsTheOldLinkEvenWhenTheResolveFails")
}

// A failure is never remembered: the next play asks again.
#[test]
#[ignore = "needs handler::serve / Deps::for_test (package 4)"]
fn play_a_failed_resolve_is_not_memoised() {
    // Enable when package 4 lands: move this body up and delete the wrapper.
    #[cfg(any())]
    fn pending() {
        use crate::deps::Deps;
        use crate::handler::serve;
        let store = Arc::new(TallyStore {
            svc: DebridService::TorBox,
            resolve: Box::new(|n| {
                if n == 1 {
                    Err(StoreError::DeadLink(DeadLinkError { reason: "not yet".to_owned() }))
                } else {
                    numbered_links(n)
                }
            }),
            resolves: AtomicI32::new(0),
            statuses: AtomicI32::new(0),
            checks: AtomicI32::new(0),
        });
        let deps = Deps::for_test(|d| {
            d.make_stores = Box::new({
                let s = store.clone();
                move |_| vec![s.clone()]
            })
        });
        let path = format!(
            "/{}/play/{}",
            crate::tests::ticket::valid_blob(),
            encode_play_token(&PlayTarget {
                info_hash: "a".repeat(40),
                file_idx: Some(0),
                ..Default::default()
            })
        );

        assert_eq!(serve(&deps, get(&path)).status(), 404, "first play");
        assert_eq!(serve(&deps, get(&path)).status(), 302, "second play, from a second resolve");
        assert_eq!(store.resolves.load(Ordering::SeqCst), 2, "resolves");
    }
    todo!("pending: linkmemo_test.go:~176 TestPlay_aFailedResolveIsNotMemoised")
}

// The probe fan-out mints a link per probed release; /play then serves that link instead of minting the
// same file again.
#[test]
#[ignore = "needs handler::probe_behind, Deps::for_test and a loopback CDN (package 4)"]
fn probe_behind_fills_the_link_memo_for_play() {
    // Enable when package 4 lands: move this body up and delete the wrapper.
    #[cfg(any())]
    fn pending() {
        use crate::deps::Deps;
        use crate::handler::{probe_job, Handler};
        // Go httptest.NewServer + http.ServeFile of testdata/tracks.mkv, as a loopback CDN.
        let cdn = loopback_cdn_serving("testdata/tracks.mkv");
        let store = Arc::new(TallyStore {
            svc: DebridService::TorBox,
            resolve: Box::new(move |_| Ok(format!("{cdn}/tracks.mkv"))),
            resolves: AtomicI32::new(0),
            statuses: AtomicI32::new(0),
            checks: AtomicI32::new(0),
        });
        let h = Handler {
            deps: Deps::for_test(|d| {
                d.probe_client = Some(cdn.client());
                d.make_stores = Box::new({
                    let s = store.clone();
                    move |_| vec![s.clone()]
                });
            }),
        };
        let config = crate::config::decode_config(None, &crate::tests::ticket::valid_blob())
            .expect("validBlob did not decode");
        let hash = "a".repeat(40);
        let target = ResolveTarget { info_hash: hash.clone(), file_idx: Some(0), ..Default::default() };
        let probe_target = ResolveTarget { no_add: true, ..target.clone() };
        h.probe_behind(
            &config,
            vec![probe_job(format!("probe:v1:{hash}:0"), vec![DebridService::TorBox], probe_target)],
        );

        let deadline = SystemTime::now() + Duration::from_secs(5);
        while h.links.get(&link_memo_key(&config, &target), SystemTime::now()).is_none() {
            assert!(SystemTime::now() < deadline, "the probe never filled the link memo");
            std::thread::sleep(Duration::from_millis(10));
        }
        let before = store.calls();
        let rr = h.serve(get(&format!(
            "/{}/play/{}",
            crate::tests::ticket::valid_blob(),
            encode_play_token(&PlayTarget { info_hash: hash, file_idx: Some(0), ..Default::default() })
        )));
        assert!(
            rr.status() == 302 && rr.headers()["location"] == format!("{cdn}/tracks.mkv"),
            "play: {} want 302 to the probe's link",
            rr.status()
        );
        assert_eq!(store.calls() - before, 0, "play re-asked the store for a link the probe had just minted");
    }
    todo!("pending: linkmemo_test.go:~200 TestProbeBehind_fillsTheLinkMemoForPlay")
}
