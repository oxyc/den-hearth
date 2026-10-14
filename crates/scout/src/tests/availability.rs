//! Go: `internal/scout/availability_test.go` (300 lines). Work package 4.

use std::collections::HashMap;
use std::sync::atomic::{AtomicI32, Ordering};
use std::sync::{mpsc, Arc};
use std::time::{Duration, Instant};

use async_trait::async_trait;

use crate::availability::{verdict_prefix, MAX_AVAILABILITY_IDS, VERDICT_UNDETERMINED};
use crate::config::DebridService;
use crate::deps::Deps;
use crate::handler::new_handler;
use crate::rank::RawStream;
use crate::scrape::{DoError, ScrapeQuery};
use crate::seal::{seal, SealKeyring, SEALED_VERSION};
use crate::stores::{ResolveTarget, Store, StoreError, StoreStatus};
use crate::tests::handler::{
    blob, body_str, fake_scraper, get, post_json, test_deps, test_seeds, valid_blob, FakeFuture, VEC_PUB_B64,
};
use crate::util::b64url_encode;

/// Go `availabilityOf`: asks the availability route for one id and returns its verdict.
pub(crate) async fn availability_of(h: &crate::handler::Handler, config: &str, id: &str) -> String {
    let r = post_json(h, &format!("/{config}/availability"), &format!(r#"{{"ids":["{id}"]}}"#)).await;
    assert_eq!(r.status(), 200, "availability: {} {}", r.status(), body_str(&r));
    let body: serde_json::Value = serde_json::from_slice(r.body()).expect("availability is JSON");
    body["availability"][id].as_str().unwrap_or("").to_owned()
}

/// Go `waitForVerdict`: asks until the check behind the replies has answered.
async fn wait_for_verdict(h: &crate::handler::Handler, config: &str, id: &str, want: &str) {
    let deadline = Instant::now() + Duration::from_secs(3);
    loop {
        let got = availability_of(h, config, id).await;
        if got == want {
            return;
        }
        if Instant::now() > deadline {
            panic!("availability of {id}: still {got:?}, want {want:?}");
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

/// Go `sealedConfig`: a JSON config sealed to the test key, as the base64url path segment a client sends.
/// Takes the keyring for parity with the Go helper; the seal uses the test key's public half.
pub(crate) fn sealed_config(_kr: &SealKeyring, cfg_json: &str) -> String {
    let pub_key: [u8; 32] =
        den_seal::decode_key(VEC_PUB_B64).expect("the test public key decodes").try_into().expect("32 bytes");
    let sealed = seal(&pub_key, cfg_json.as_bytes()).expect("seals");
    let mut framed = vec![SEALED_VERSION];
    framed.extend_from_slice(&sealed);
    b64url_encode(&framed)
}

/// Go `countingScraper`: a scraper factory that counts its asks and answers with `streams` or `err`.
fn counting_scraper(
    n: Arc<AtomicI32>,
    streams: Vec<RawStream>,
    err: Option<&'static str>,
) -> crate::deps::ScraperFactory {
    Arc::new(move |_| {
        let (n, streams) = (n.clone(), streams.clone());
        vec![fake_scraper("torrentio", move || {
            let (n, streams) = (n.clone(), streams.clone());
            Box::pin(async move {
                n.fetch_add(1, Ordering::SeqCst);
                match err {
                    Some(msg) => Err(Box::new(std::io::Error::other(msg)) as DoError),
                    None => Ok(streams),
                }
            }) as FakeFuture
        })]
    })
}

/// A cache-check store that counts its checks and answers with nothing.
struct AvailabilityCountingStore {
    checks: Arc<AtomicI32>,
}

#[async_trait]
impl Store for AvailabilityCountingStore {
    fn service(&self) -> DebridService {
        DebridService::TorBox
    }

    async fn cache_check(&self, _hashes: &[String]) -> Result<HashMap<String, bool>, StoreError> {
        self.checks.fetch_add(1, Ordering::SeqCst);
        Ok(HashMap::new())
    }

    async fn resolve(&self, _t: &ResolveTarget) -> Result<String, StoreError> {
        Err(StoreError::Other("unused".to_owned()))
    }

    async fn status(&self, _t: &ResolveTarget) -> Option<StoreStatus> {
        None
    }
}

/// A cache-check store that reports one held hash, sends each batch it was asked about, and counts its checks.
struct AvailabilityTruthStore {
    checks: Arc<AtomicI32>,
    seen: mpsc::Sender<Vec<String>>,
    held: String,
}

#[async_trait]
impl Store for AvailabilityTruthStore {
    fn service(&self) -> DebridService {
        DebridService::TorBox
    }

    async fn cache_check(&self, hashes: &[String]) -> Result<HashMap<String, bool>, StoreError> {
        self.checks.fetch_add(1, Ordering::SeqCst);
        let _ = self.seen.send(hashes.to_vec());
        Ok(hashes.iter().map(|h| (h.clone(), *h == self.held)).collect())
    }

    async fn resolve(&self, _t: &ResolveTarget) -> Result<String, StoreError> {
        Err(StoreError::Other("unused".to_owned()))
    }

    async fn status(&self, _t: &ResolveTarget) -> Option<StoreStatus> {
        None
    }
}

/// The first ask answers unknown and checks behind the reply; a later ask has the verdict.
#[tokio::test]
async fn availability_checks_behind_the_reply() {
    let h = new_handler(test_deps(|_| {}));
    assert_eq!(availability_of(&h, &valid_blob(), "tt1234567").await, "unknown", "first ask");
    wait_for_verdict(&h, &valid_blob(), "tt1234567", "available").await;
}

#[tokio::test]
async fn availability_default_costs_no_debrid_cache_check() {
    let checks = Arc::new(AtomicI32::new(0));
    let config =
        blob(r#"{"debrid":[{"service":"torbox","token":"tb"}],"indexers":["torrentio"],"cachedOnly":false}"#);
    let c = checks.clone();
    let h = new_handler(test_deps(move |d| {
        d.make_scrapers = counting_scraper(Arc::new(AtomicI32::new(0)), test_seeds(), None);
        d.make_stores = Arc::new(move |_| vec![Box::new(AvailabilityCountingStore { checks: c.clone() })]);
    }));
    wait_for_verdict(&h, &config, "tt1234567", "available").await;
    assert_eq!(checks.load(Ordering::SeqCst), 0, "default availability made debrid cache checks, want zero");
}

#[tokio::test]
async fn availability_cached_only_filters_before_its_first_cache_check() {
    let checks = Arc::new(AtomicI32::new(0));
    let held = "f".repeat(40);
    let mut streams: Vec<RawStream> = (0..120)
        .map(|i| RawStream {
            info_hash: format!("{:040x}", i + 1),
            title: "CAM release".to_owned(),
            ..RawStream::default()
        })
        .collect();
    streams.push(RawStream {
        info_hash: held.clone(),
        title: "Movie.2026.1080p.WEB-DL".to_owned(),
        ..RawStream::default()
    });
    let (tx, rx) = mpsc::channel();
    let (c, held2) = (checks.clone(), held.clone());
    let h = new_handler(test_deps(move |d| {
        d.make_scrapers = counting_scraper(Arc::new(AtomicI32::new(0)), streams.clone(), None);
        d.make_stores = Arc::new(move |_| {
            vec![Box::new(AvailabilityTruthStore {
                checks: c.clone(),
                seen: tx.clone(),
                held: held2.clone(),
            })]
        });
    }));
    wait_for_verdict(&h, &valid_blob(), "tt1234567", "available").await;
    assert_eq!(checks.load(Ordering::SeqCst), 1, "cache checks, want one batch ending at the first positive");
    let hashes = rx.recv().expect("one batch was sent");
    assert!(
        hashes.len() == 1 && hashes[0] == held,
        "cache check saw {} hashes ({hashes:?}), want only the one release surviving filters",
        hashes.len()
    );
}

/// Every indexer answering with nothing is a verdict. A failed scrape is not: it stays unknown, and holds off the
/// next check rather than scraping again on every ask.
#[tokio::test]
async fn availability_empty_is_unavailable_but_a_failure_is_unknown() {
    let none = Arc::new(AtomicI32::new(0));
    let empty = new_handler(test_deps(|d| d.make_scrapers = counting_scraper(none, Vec::new(), None)));
    wait_for_verdict(&empty, &valid_blob(), "tt1234567", "unavailable").await;

    let scrapes = Arc::new(AtomicI32::new(0));
    let deps =
        test_deps(|d| d.make_scrapers = counting_scraper(scrapes.clone(), Vec::new(), Some("indexer down")));
    let cache = deps.cache.clone();
    let failing = new_handler(deps);
    let config = crate::config::decode_config(None, &valid_blob()).expect("the valid config decodes");
    let key = format!("{}tt1234567", verdict_prefix(&config));
    availability_of(&failing, &valid_blob(), "tt1234567").await;
    let deadline = Instant::now() + Duration::from_secs(3);
    while cache.get(&key).as_deref() != Some(VERDICT_UNDETERMINED) {
        assert!(Instant::now() < deadline, "the failed check left no marker");
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    assert_eq!(
        availability_of(&failing, &valid_blob(), "tt1234567").await,
        "unknown",
        "after a failed check"
    );
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert_eq!(scrapes.load(Ordering::SeqCst), 1, "a failed check must hold off the next");
}

/// A stream list someone opened answers the availability question too, with no check of its own.
#[tokio::test]
async fn availability_a_stream_list_records_the_verdict() {
    let scrapes = Arc::new(AtomicI32::new(0));
    let h =
        new_handler(test_deps(|d| d.make_scrapers = counting_scraper(scrapes.clone(), test_seeds(), None)));
    assert_eq!(
        get(&h, &format!("/{}/stream/movie/tt1234567.json", valid_blob()), &[]).await.status(),
        200,
        "stream"
    );
    assert_eq!(availability_of(&h, &valid_blob(), "tt1234567").await, "available", "after the stream list");
    assert_eq!(scrapes.load(Ordering::SeqCst), 1, "scrapes");
}

/// A nonempty partial scrape may become empty after filtering. The missing indexer could still have a playable
/// release, so neither the stream route nor the background checker may record absence.
#[tokio::test]
async fn availability_partial_filtered_list_stays_unknown() {
    for from_stream in [false, true] {
        let scrapes = Arc::new(AtomicI32::new(0));
        let s = scrapes.clone();
        let deps = test_deps(move |d| {
            let s = s.clone();
            d.make_scrapers = Arc::new(move |_| {
                let s = s.clone();
                vec![
                    fake_scraper("torrentio", move || {
                        let s = s.clone();
                        Box::pin(async move {
                            s.fetch_add(1, Ordering::SeqCst);
                            Ok(test_seeds()[2..].to_vec()) // only the CAM survives this indexer
                        })
                    }),
                    fake_scraper("comet", || {
                        Box::pin(async {
                            Err(Box::new(std::io::Error::other("temporary outage")) as DoError)
                        })
                    }),
                ]
            });
        });
        let cache = deps.cache.clone();
        let h = new_handler(deps);
        if from_stream {
            assert_eq!(
                get(&h, &format!("/{}/stream/movie/tt1234567.json", valid_blob()), &[]).await.status(),
                200,
                "stream"
            );
        } else {
            availability_of(&h, &valid_blob(), "tt1234567").await;
        }
        let config = crate::config::decode_config(None, &valid_blob()).expect("the valid config decodes");
        let key = format!("{}tt1234567", verdict_prefix(&config));
        let deadline = Instant::now() + Duration::from_secs(3);
        loop {
            if let Some(verdict) = cache.get(&key) {
                assert_eq!(&*verdict, VERDICT_UNDETERMINED, "partial filtered list verdict");
                break;
            }
            assert!(Instant::now() < deadline, "background check never recorded its result");
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        assert_eq!(availability_of(&h, &valid_blob(), "tt1234567").await, "unknown", "stream={from_stream}");
        assert_eq!(
            scrapes.load(Ordering::SeqCst),
            1,
            "unknown should retain the retry backoff (stream={from_stream})"
        );
    }
}

/// The scoped config answers availability — reading the verdicts the full config wrote — and its manifest, and
/// nothing that lists or plays streams. A scope on a plaintext config, or an unknown one, is refused.
#[tokio::test]
async fn availability_the_scoped_config() {
    let kr = crate::tests::handler::ticket_keyring();
    let cfg =
        r#"{"debrid":[{"service":"torbox","token":"tb-secret"}],"indexers":["torrentio"],"cachedOnly":true"#;
    let full = sealed_config(&kr, &format!("{cfg}}}"));
    let scoped = sealed_config(&kr, &format!(r#"{cfg},"scope":"availability"}}"#));
    let kr_arc = Arc::new(crate::tests::handler::ticket_keyring());
    let h = new_handler(test_deps(move |d| d.seal_keyring = Some(kr_arc)));

    assert_eq!(
        get(&h, &format!("/{full}/stream/movie/tt1234567.json"), &[]).await.status(),
        200,
        "full stream"
    );
    assert_eq!(
        availability_of(&h, &scoped, "tt1234567").await,
        "available",
        "the scoped config should read the full config's verdict"
    );
    assert_eq!(get(&h, &format!("/{scoped}/manifest.json"), &[]).await.status(), 200, "scoped manifest");
    let token = crate::play::encode_play_token(&crate::play::PlayTarget {
        info_hash: "a".repeat(40),
        ..crate::play::PlayTarget::default()
    });
    for path in [
        format!("/{scoped}/stream/movie/tt7654321.json"),
        format!("/{scoped}/stream/movie/tt7654321.json?debug=1"),
        format!("/{scoped}/play/{token}"),
        format!("/{scoped}/play/{token}?probe=1"),
    ] {
        assert_eq!(get(&h, &path, &[]).await.status(), 403, "{}", path.replace(&scoped, "<scoped>"));
    }
    let plaintext = blob(&format!(r#"{cfg},"scope":"availability"}}"#));
    assert_eq!(
        post_json(&h, &format!("/{plaintext}/availability"), r#"{"ids":[]}"#).await.status(),
        400,
        "a plaintext scoped config"
    );
    let unknown_scope = sealed_config(&kr, &format!(r#"{cfg},"scope":"everything"}}"#));
    assert_eq!(
        post_json(&h, &format!("/{unknown_scope}/availability"), r#"{"ids":[]}"#).await.status(),
        400,
        "an unknown scope"
    );
}

#[tokio::test]
async fn availability_refuses_what_is_not_a_page_of_movie_ids() {
    let h = new_handler(test_deps(|_| {}));
    let path = format!("/{}/availability", valid_blob());
    let r = get(&h, &path, &[]).await;
    assert!(
        r.status() == 405 && crate::tests::handler::hdr(&r, "allow") == "POST",
        "GET: {} allow",
        r.status()
    );
    let ids: Vec<String> = (0..=MAX_AVAILABILITY_IDS).map(|i| format!(r#""tt{}""#, i + 1)).collect();
    for (name, body) in [
        ("not json", "ids".to_owned()),
        ("too many", format!(r#"{{"ids":[{}]}}"#, ids.join(","))),
        ("not an imdb id", r#"{"ids":["tt1","1234"]}"#.to_owned()),
        ("an episode", r#"{"ids":["tt1:1:1"]}"#.to_owned()),
    ] {
        assert_eq!(post_json(&h, &path, &body).await.status(), 400, "{name}");
    }
    assert_eq!(post_json(&h, "/@@@/availability", r#"{"ids":[]}"#).await.status(), 400, "bad config");
}

/// `Deps` is built by `test_deps`, which every test in this module uses; this keeps the import live for readers.
#[allow(dead_code)]
fn _deps_type(_: &Deps) {}

/// The scraper trait object type, as the availability factories return it.
#[allow(dead_code)]
fn _scraper_query(_: &ScrapeQuery) {}
