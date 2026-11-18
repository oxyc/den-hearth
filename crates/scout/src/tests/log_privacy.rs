//! What Scout's decision lines say about media and releases, through the real rank, scrape, probe/download-state
//! and cancel log paths with sentinel values: off by default, an explicit `LOG_IDENTITY=1` adds the title and
//! release name, and in every setting the info hash, source URLs and account tokens stay out while `rid`, the
//! outcome and the upstream stay in.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use crate::cache::{Cache, MemoryCache};
use crate::config::Indexer;
use crate::decisionlog::{log_rank_decision, set_log_identity};
use crate::handler::new_handler;
use crate::id::StreamId;
use crate::loglimit::Capture;
use crate::play::{encode_play_token, PlayTarget};
use crate::rank::{RankDebug, RawStream};
use crate::scrape::{ScrapeQuery, StremioScraper};
use crate::stores::refusal::torrent_id_key;
use crate::stores::torbox::{mark_added_by_us, TorBoxStore, RESOLVE_CACHE_TTL, TORBOX_API};
use crate::stores::Store;
use crate::tests::cancel::{cancel_path, cancel_torbox_handler};
use crate::tests::handler::{get, method, shared_stores, test_deps, valid_blob};
use crate::tests::mock::{resp, MockDoer};
use crate::tests::token_leak::FailingDoer;

const IMDB: &str = "tt7770777";
const TITLE: &str = "SENTINEL-TITLE.2160p.WEB-DL.DDP5.1-GROUP";
const HASH: &str = "5e47e1a55e47e1a55e47e1a55e47e1a55e47e1a5";
const URL_SECRET: &str = "SENTINEL-URL-SECRET";
const INDEXER_HOST: &str = "sentinel-indexer.example";
const TOKEN: &str = "tb-secret";

fn base(req: &reqwest::Request) -> String {
    req.url().path().rsplit('/').next().unwrap_or("").to_owned()
}

fn query(rid: &str) -> ScrapeQuery {
    ScrapeQuery {
        kind: "series".into(),
        imdb: IMDB.into(),
        has_ep: true,
        season: 77,
        episode: 88,
        rid: rid.into(),
        ..Default::default()
    }
}

/// Drives the rank line, a failing scrape (HTTP error and transport error), a probe that changes a release's
/// download state, and a cancel that the debrid refuses, and answers everything they logged.
async fn drive() -> String {
    let out = Capture::start();

    // rank
    let sid = StreamId { kind: "series".into(), imdb: IMDB.into(), season: 77, episode: 88, has_ep: true };
    let release = |title: &str, hash: &str| RawStream {
        title: title.into(),
        info_hash: hash.into(),
        size_bytes: Some(4 << 30),
        cached: true,
        ..Default::default()
    };
    let ranked = vec![release(TITLE, HASH), release("SENTINEL-RUNNERUP.1080p", &"ab".repeat(20))];
    let dbg = RankDebug { dropped_by: BTreeMap::from([("cam".to_owned(), 2)]), ..Default::default() };
    log_rank_decision(&sid, 5, &ranked, &dbg, Duration::from_millis(7), "rid-rank");

    // scrape: a refused request, then one that never got an answer
    let status = StremioScraper {
        indexer: Indexer("torz".into()),
        base_url: format!("https://{INDEXER_HOST}/{URL_SECRET}"),
        client: Arc::new(MockDoer(|_r: reqwest::Request| Ok(resp(502, "{}")))),
    };
    let _ = status.scrape_once("", &query("rid-scrape-status")).await;
    let unreachable = StremioScraper {
        indexer: Indexer("comet".into()),
        base_url: format!("https://{INDEXER_HOST}/{URL_SECRET}"),
        client: Arc::new(FailingDoer { last_url: std::sync::Mutex::new(String::new()) }),
    };
    let _ = unreachable.scrape_once("", &query("rid-scrape-down")).await;

    // probe: a release changing state, through the play route with a fake TorBox
    let stalled = Arc::new(AtomicBool::new(false));
    let cache = Arc::new(MemoryCache::new(1 << 20));
    cache.put(&torrent_id_key(TOKEN, HASH), "42", RESOLVE_CACHE_TTL);
    let store: Arc<dyn Store> = Arc::new(TorBoxStore {
        token: TOKEN.to_owned(),
        cache: Some(cache.clone()),
        api: TORBOX_API.to_owned(),
        client: Arc::new(MockDoer(move |req| {
            Ok(match base(&req).as_str() {
                "mylist" => resp(
                    200,
                    if stalled.load(Ordering::SeqCst) {
                        r#"{"success":true,"data":{"id":42,"progress":0.4,"download_finished":false,"download_state":"stalled (no seeds)","seeds":0,"peers":0}}"#
                    } else {
                        r#"{"success":true,"data":{"id":42,"progress":0.4,"download_finished":false,"download_state":"downloading","seeds":3,"peers":1}}"#
                    },
                ),
                _ => resp(404, "{}"),
            })
        })),
    });
    let deps_cache = cache.clone();
    let h = new_handler(test_deps(move |d| {
        d.cache = deps_cache.clone();
        d.make_stores = shared_stores(store.clone());
    }));
    let probe = format!(
        "/{}/play/{}?probe=1",
        valid_blob(),
        encode_play_token(&PlayTarget {
            info_hash: HASH.to_owned(),
            file_idx: Some(87),
            season: Some(77),
            episode: Some(88),
            title: TITLE.to_owned(),
            imdb: IMDB.to_owned(),
            ..Default::default()
        })
    );
    assert_eq!(get(&h, &probe, &[("X-Request-Id", "rid-probe")]).await.status(), 202);

    // cancel: the debrid refuses, so the route logs a decision line naming it
    let (h, _) = cancel_torbox_handler(
        |r| match r.url().path().rsplit('/').next().unwrap_or("") {
            "mylist" if r.url().query_pairs().any(|(k, v)| k == "id" && !v.is_empty()) => {
                resp(200, r#"{"success":true,"data":{"id":42,"download_finished":false}}"#)
            }
            "mylist" => resp(200, r#"{"success":true,"data":[]}"#),
            "controltorrent" => resp(503, r#"{"error":"proxy_error"}"#),
            _ => resp(404, "{}"),
        },
        Some(Box::new(|cache| {
            cache.put(&torrent_id_key(TOKEN, HASH), "42", RESOLVE_CACHE_TTL);
            mark_added_by_us(cache, TOKEN, HASH);
        })),
    );
    let rr = method(&h, http::Method::DELETE, &cancel_path(HASH), &[("X-Request-Id", "rid-cancel")]).await;
    assert_eq!(rr.status(), 503);

    out.text()
}

/// What no setting may ever write.
fn assert_never_logged(log: &str) {
    for secret in [HASH, &HASH[..12], &"ab".repeat(10), URL_SECRET, INDEXER_HOST, TOKEN, "hash=", "file_idx="]
    {
        assert!(!log.contains(secret), "{secret:?} reached the log:\n{log}");
    }
}

/// What every setting keeps: the request id, the outcome and the upstream, and a class for each line.
fn assert_operational_fields_kept(log: &str) {
    for keep in [
        "event=rank outcome=served reason=picked from 5 candidates dur_ms=7 rid=rid-rank type=series",
        "top_drops=cam:2",
        "picked_cached=yes",
        "event=scrape outcome=",
        "upstream=torz",
        "upstream=comet",
        "rid=rid-scrape-status",
        "rid=rid-scrape-down",
        "reason=http 502",
        "event=download outcome=changed reason=probe: none -> downloading upstream=torbox",
        "rid=rid-probe",
        "kind=episode",
        "event=cancel",
        "rid=rid-cancel",
    ] {
        assert!(log.contains(keep), "the log lost {keep:?}:\n{log}");
    }
}

/// What only the explicit opt-in may add.
const IDENTITY: [&str; 4] = [IMDB, "SENTINEL-TITLE", "season=77 episode=88", "SENTINEL-RUNNERUP"];

#[tokio::test]
async fn the_default_logs_no_title_release_or_coordinates() {
    // LOG_IDENTITY unset: nothing is switched on.
    let log = drive().await;
    assert_never_logged(&log);
    assert_operational_fields_kept(&log);
    for identity in
        IDENTITY.into_iter().chain(["imdb=", "season=", "episode=", "release=", "picked=", "runnerup"])
    {
        assert!(!log.contains(identity), "{identity:?} reached the default log:\n{log}");
    }
}

#[tokio::test]
async fn an_explicit_off_logs_no_title_release_or_coordinates() {
    set_log_identity(false);
    let log = drive().await;
    assert_never_logged(&log);
    assert_operational_fields_kept(&log);
    for identity in IDENTITY {
        assert!(!log.contains(identity), "{identity:?} reached the LOG_IDENTITY=0 log:\n{log}");
    }
}

#[tokio::test]
async fn the_opt_in_adds_the_title_and_release_but_never_hash_urls_or_tokens() {
    set_log_identity(true);
    let log = drive().await;
    // The precondition that makes the two tests above mean something: with the gate open the sentinels do arrive.
    for identity in IDENTITY {
        assert!(
            log.contains(identity),
            "the opt-in lost {identity:?}, so the absence checks prove nothing:\n{log}"
        );
    }
    assert_never_logged(&log);
    assert_operational_fields_kept(&log);
}
