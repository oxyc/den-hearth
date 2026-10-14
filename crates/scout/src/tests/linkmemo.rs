//! Go: `internal/scout/linkmemo_test.go` (246 lines). Work package 6.
//!
//! Also hosts `TallyStore`, the Go `tallyStore` that other test modules share.

use std::collections::HashMap;
use std::sync::atomic::{AtomicI32, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;

use super::handler::{dead_link, get, hdr, shared_stores, test_deps, valid_blob};
use super::mock::loopback_file;
use crate::config::{decode_config, Config, DebridAccount, DebridService};
use crate::handler::{new_handler, Handler};
use crate::linkmemo::{link_memo_key, LinkMemo, LINK_MEMO_MAX_ENTRIES, LINK_MEMO_TTL};
use crate::play::{encode_play_token, PlayTarget};
use crate::probe_fanout::ProbeJob;
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
        self.resolves.load(Ordering::SeqCst) + self.statuses.load(Ordering::SeqCst) + self.checks.load(Ordering::SeqCst)
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
    let a = Config { debrid: vec![DebridAccount { service: DebridService::TorBox, token: "one".to_owned() }], ..Default::default() };
    let b = Config { debrid: vec![DebridAccount { service: DebridService::TorBox, token: "two".to_owned() }], ..Default::default() };
    let hash = "a".repeat(40);
    let movie = ResolveTarget { info_hash: hash.clone(), file_idx: Some(0), ..Default::default() };
    assert!(link_memo_key(&a, &movie) != link_memo_key(&b, &movie), "two accounts share a memo key");
    let read_only = ResolveTarget { no_add: true, ..movie.clone() };
    assert_eq!(link_memo_key(&a, &movie), link_memo_key(&a, &read_only), "the probe's NoAdd target keys differently from /play's");
    let ep1 = ResolveTarget { info_hash: hash.clone(), season: Some(1), episode: Some(1), ..Default::default() };
    let ep2 = ResolveTarget { info_hash: hash, season: Some(1), episode: Some(2), ..Default::default() };
    assert!(link_memo_key(&a, &ep1) != link_memo_key(&a, &ep2), "two episodes of one pack share a memo key");
}

fn tally(resolve: impl Fn(i32) -> Result<String, StoreError> + Send + Sync + 'static) -> Arc<TallyStore> {
    Arc::new(TallyStore {
        svc: DebridService::TorBox,
        resolve: Box::new(resolve),
        resolves: AtomicI32::new(0),
        statuses: AtomicI32::new(0),
        checks: AtomicI32::new(0),
    })
}

fn tally_handler(store: &Arc<TallyStore>) -> Handler {
    let shared: Arc<dyn Store> = store.clone();
    new_handler(test_deps(move |d| d.make_stores = shared_stores(shared.clone())))
}

fn play_path() -> String {
    format!(
        "/{}/play/{}",
        valid_blob(),
        encode_play_token(&PlayTarget { info_hash: "a".repeat(40), file_idx: Some(0), ..Default::default() })
    )
}

// A repeat play of the same file inside the TTL is answered from the memo with nothing asked upstream.
#[tokio::test]
async fn play_memo_serves_a_repeat_with_no_upstream_calls() {
    let store = tally(numbered_links);
    let h = tally_handler(&store);
    let path = play_path();

    let first = get(&h, &path, &[]).await;
    assert_eq!(first.status(), 302, "first play: {}", first.status());
    let before = store.calls();
    let second = get(&h, &path, &[]).await;
    assert!(
        second.status() == 302 && hdr(&second, "location") == hdr(&first, "location"),
        "repeat play: {} {:?}, want 302 {:?}",
        second.status(),
        hdr(&second, "location"),
        hdr(&first, "location")
    );
    let got = store.calls() - before;
    assert_eq!(got, 0, "a memo hit made {got} store calls, want 0");
}

// fresh=1 skips the memo and replaces it: the client could not open the last link.
#[tokio::test]
async fn play_fresh_bypasses_and_replaces_the_memo() {
    let store = tally(numbered_links);
    let h = tally_handler(&store);
    let path = play_path();

    let rr = get(&h, &path, &[]).await;
    assert_eq!(hdr(&rr, "location"), "https://cdn.example/minted-1.mkv", "first play: {} {:?}", rr.status(), hdr(&rr, "location"));
    let rr = get(&h, &format!("{path}?fresh=1"), &[]).await;
    assert_eq!(
        hdr(&rr, "location"),
        "https://cdn.example/minted-2.mkv",
        "fresh play did not mint a new link: {} {:?}",
        rr.status(),
        hdr(&rr, "location")
    );
    let rr = get(&h, &path, &[]).await;
    assert_eq!(hdr(&rr, "location"), "https://cdn.example/minted-2.mkv", "the fresh link did not replace the memo: {:?}", hdr(&rr, "location"));
    let got = store.resolves.load(Ordering::SeqCst);
    assert_eq!(got, 2, "resolves = {got}, want 2");
}

// fresh=1 drops the old link even when the new resolve fails, so the link the client just reported as
// unopenable is not served again by the next plain request.
#[tokio::test]
async fn play_fresh_drops_the_old_link_even_when_the_resolve_fails() {
    let store = tally(|n| if n == 1 { numbered_links(n) } else { Err(dead_link("gone")) });
    let h = tally_handler(&store);
    let path = play_path();

    get(&h, &path, &[]).await;
    let rr = get(&h, &format!("{path}?fresh=1"), &[]).await;
    assert_eq!(rr.status(), 404, "fresh play: {}, want 404", rr.status());
    let rr = get(&h, &path, &[]).await;
    assert_ne!(rr.status(), 302, "the link the client reported as unopenable was served again: {:?}", hdr(&rr, "location"));
}

// A failure is never remembered: the next play asks again.
#[tokio::test]
async fn play_a_failed_resolve_is_not_memoised() {
    let store = tally(|n| if n == 1 { Err(dead_link("not yet")) } else { numbered_links(n) });
    let h = tally_handler(&store);
    let path = play_path();

    let rr = get(&h, &path, &[]).await;
    assert_eq!(rr.status(), 404, "first play: {}, want 404", rr.status());
    let rr = get(&h, &path, &[]).await;
    assert_eq!(rr.status(), 302, "second play: {}, want 302 from a second resolve", rr.status());
    let got = store.resolves.load(Ordering::SeqCst);
    assert_eq!(got, 2, "resolves = {got}, want 2");
}

// The probe fan-out mints a link per probed release; /play then serves that link instead of minting the
// same file again.
#[tokio::test]
async fn probe_behind_fills_the_link_memo_for_play() {
    let cdn = loopback_file(std::fs::read(concat!(env!("CARGO_MANIFEST_DIR"), "/testdata/tracks.mkv")).expect("testdata fixture")).await;
    let link = format!("{cdn}/tracks.mkv");
    let store = tally({
        let link = link.clone();
        move |_| Ok(link.clone())
    });
    let shared: Arc<dyn Store> = store.clone();
    let h = new_handler(test_deps(move |d| {
        d.probe_client = Some(reqwest::Client::new());
        d.make_stores = shared_stores(shared.clone());
    }));
    let config = decode_config(None, &valid_blob()).expect("validBlob did not decode");
    let hash = "a".repeat(40);
    let target = ResolveTarget { info_hash: hash.clone(), file_idx: Some(0), ..Default::default() };
    let probe_target = ResolveTarget { no_add: true, ..target.clone() };
    h.probe_behind(
        &config,
        vec![ProbeJob { key: format!("probe:v1:{hash}:0"), holders: vec![DebridService::TorBox], target: probe_target }],
    );

    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while h.links().get(&link_memo_key(&config, &target), SystemTime::now()).is_none() {
        assert!(std::time::Instant::now() < deadline, "the probe never filled the link memo");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    let before = store.calls();
    let rr = get(
        &h,
        &format!("/{}/play/{}", valid_blob(), encode_play_token(&PlayTarget { info_hash: hash, file_idx: Some(0), ..Default::default() })),
        &[],
    )
    .await;
    assert!(rr.status() == 302 && hdr(&rr, "location") == link, "play: {} {:?}, want 302 to the probe's link", rr.status(), hdr(&rr, "location"));
    let got = store.calls() - before;
    assert_eq!(got, 0, "play re-asked the store {got} times for a link the probe had just minted");
}
