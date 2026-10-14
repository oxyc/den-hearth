//! Go: `internal/scout/sources_test.go` (172 lines). Work package 5.

use std::net::IpAddr;

use crate::scrape::parse_stremio_streams;
use crate::sources::MAX_SOURCE_URL;

#[test]
fn normalize_sources() {
    let mf = "https://mediafusion.example/D-secret";
    let got = crate::sources::normalize_sources(&[
        // what an addon's configure page hands out
        format!(" {mf}/manifest.json "),
        // an install button's link
        "stremio://comet.example/cfg/manifest.json".to_owned(),
        // the first one again
        format!("{mf}/"),
        // not https
        "http://plain.example/cfg".to_owned(),
        // a non-public address, written out
        "https://127.0.0.1/cfg".to_owned(),
        "https://[::1]/cfg".to_owned(),
        // the tailnet
        "https://100.101.102.103/cfg".to_owned(),
        "https://localhost/cfg".to_owned(),
        "https://user:pw@creds.example/cfg".to_owned(),
        "https://query.example/cfg?token=x".to_owned(),
        format!("https://long.example/{}", "x".repeat(MAX_SOURCE_URL)),
        "https://third.example/cfg".to_owned(),
        // past maxSources
        "https://fourth.example/cfg".to_owned(),
    ]);
    let want = [mf, "https://comet.example/cfg", "https://third.example/cfg"];
    assert_eq!(got.join(" "), want.join(" "), "sources = {got:?}, want {want:?}");
}

// Sources ride in the install config, and an unusable one is dropped rather than refusing the config, like
// every other field there.
#[test]
fn decode_config_carries_sources() {
    use crate::config::decode_config;
    use crate::tests::core::blob;

    let c = decode_config(None, &blob(r#"{"debrid":[{"service":"torbox","token":"t"}],"sources":["https://mf.example/cfg/manifest.json","http://nope.example"]}"#))
            .expect("config decodes");
    assert!(c.sources.len() == 1 && c.sources[0] == "https://mf.example/cfg", "config = {c:?}");
}

#[test]
fn public_addr() {
    let cases = [
        ("1.1.1.1", true),
        ("2606:4700:4700::1111", true),
        ("10.0.0.1", false),
        ("172.16.5.4", false),
        // den itself
        ("192.168.86.193", false),
        ("127.0.0.1", false),
        // cloud metadata
        ("169.254.169.254", false),
        // tailnet / CGNAT
        ("100.64.0.1", false),
        ("0.0.0.0", false),
        ("255.255.255.255", false),
        ("::1", false),
        ("fe80::1", false),
        // tailnet IPv6
        ("fd7a:115c:a1e0::1", false),
        // IPv4-mapped
        ("::ffff:192.168.1.1", false),
        // NAT64 to 192.168.1.1
        ("64:ff9b::c0a8:101", false),
        // 6to4 around 192.168.1.1
        ("2002:c0a8:101::1", false),
        ("224.0.0.1", false),
        ("198.18.0.1", false),
        ("2001:4860:4860::8888", true),
        ("::ffff:8.8.8.8", true),
        // documentation ranges, Teredo, and the IPv4-compatible block: unroutable, and refused with Subtitles'
        ("2001:db8::1", false),
        ("192.0.2.1", false),
        ("198.51.100.7", false),
        ("203.0.113.9", false),
        ("2001::1", false),
        ("::127.0.0.1", false),
        // NAT64, 6to4 and SIIT are refused whatever they embed, as before
        ("64:ff9b::808:808", false),
        ("2002:808:808::1", false),
        ("::ffff:0:8.8.8.8", false),
        ("192.88.99.1", false),
        ("93.184.216.34", true),
        ("2a00:1450:4001::200e", true),
        ("240.0.0.1", false),
        ("fec0::1", false),
        ("64:ff9b:1::1", false),
        ("100.127.255.255", false),
        ("100.128.0.1", true),
        ("172.32.0.1", true),
        ("192.169.0.1", true),
        ("11.0.0.1", true),
        ("126.255.255.255", true),
        ("128.0.0.1", true),
        ("9.255.255.255", true),
        ("1.0.0.0", true),
        ("223.255.255.255", true),
        ("::", false),
        ("ff02::1", false),
        ("fc00::1", false),
        ("2001:470::1", true),
        ("::ffff:100.64.0.1", false),
        ("::ffff:169.254.169.254", false),
    ];
    for (addr, want) in cases {
        let ip: IpAddr = addr.parse().expect("test address parses");
        let got = crate::sources::public_addr(ip);
        assert_eq!(got, want, "publicAddr({addr}) = {got}, want {want}");
    }
}

// The guard sits on the connection, not on the name: a host that resolves to the box's own network is
// refused at dial time, whatever the config said.
//
// Go stands a TLS server up on the loopback and fails the test if it is reached. No TLS server is available to
// this crate's tests, so a plain loopback listener plays it: the source client must refuse the address without ever
// connecting, which the listener never accepting proves.
#[tokio::test]
async fn source_client_refuses_a_non_public_address() {
    use crate::scrape::{Doer, Scraper};
    use crate::sources::{new_source_client, ERR_NOT_PUBLIC};

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.expect("bind a loopback port");
    let url = format!("https://{}", listener.local_addr().expect("a local address"));
    let err = new_source_client()
        .do_request(get(&format!("{url}/manifest.json")))
        .await
        .expect_err("the non-public refusal");
    assert!(err.to_string().contains(ERR_NOT_PUBLIC), "err = {err}, want the non-public refusal");
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(50), listener.accept()).await.is_err(),
        "a source on a loopback address was reached"
    );

    // And a scraper asking it reports the refusal once, without spending the budget on retries.
    let calls = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let counter = calls.clone();
    let client: std::sync::Arc<dyn Doer> =
        std::sync::Arc::new(super::mock::MockDoer(move |_r: reqwest::Request| {
            counter.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Err(ERR_NOT_PUBLIC.into())
        }));
    let sc = crate::scrape::SourceScraper::new(&url, client);
    let q = crate::scrape::ScrapeQuery { kind: "movie".into(), imdb: "tt1".into(), ..Default::default() };
    let result = sc.scrape(&q).await;
    let n = calls.load(std::sync::atomic::Ordering::SeqCst);
    assert!(
        result.is_err() && n == 1,
        "err = {:?} after {n} request(s), want one refused request",
        result.as_ref().err()
    );
}

// Go reads the scraper's fields off a `*stremioScraper`; here the scraper is a trait object, so what is checked is
// what it does: its id, the host that labels it, and which client it was asked through.
#[tokio::test]
async fn make_scrapers_asks_household_sources_through_the_guarded_client() {
    use crate::config::{Config, Indexer};
    use crate::scrape::{make_scrapers, Doer, SourceScraper};
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex};

    let asked: Arc<Mutex<Vec<(&'static str, String)>>> = Arc::new(Mutex::new(Vec::new()));
    let doer = |which: &'static str| -> Arc<dyn Doer> {
        let log = Arc::clone(&asked);
        Arc::new(super::mock::MockDoer(move |r: reqwest::Request| {
            log.lock().expect("lock").push((which, r.url().to_string()));
            Ok(super::mock::resp(200, "{}"))
        }))
    };
    let (plain, guarded) = (doer("plain"), doer("guarded"));
    let cfg = Config {
        indexers: vec![Indexer("torrentio".into())],
        sources: vec!["https://mf.example/cfg".into()],
        ..Default::default()
    };
    let got = make_scrapers(&cfg, plain, Arc::clone(&guarded), &HashMap::new());
    assert_eq!(got.len(), 2, "scrapers = {}", got.len());
    let q = crate::scrape::ScrapeQuery { kind: "movie".into(), imdb: "tt1".into(), ..Default::default() };
    assert!(
        got[0].id() == Indexer("own".into()) && got[0].label() == "mf.example",
        "household source scraper = {} {:?}",
        got[0].id().0,
        got[0].label()
    );
    got[0].scrape(&q).await.expect("an answer");
    assert_eq!(
        asked.lock().expect("lock").as_slice(),
        [("guarded", "https://mf.example/cfg/stream/movie/tt1.json".to_owned())],
        "a household source must be fetched with the guarded client"
    );
    let name = SourceScraper::new("https://mf.example/cfg", guarded).stremio.name("mf.example");
    assert_eq!(name, "own source mf.example", "name = {name:?}");
}

// An addon configured with a debrid account links to its own resolver rather than naming the torrent; the
// hash in that link is what scout checks its own debrid for.
#[test]
fn parse_stremio_streams_reads_the_hash_out_of_a_playback_link() {
    let h1 = "a".repeat(40);
    let h2 = "B".repeat(40);
    let h3 = "c".repeat(40);
    let body = format!(
        r#"{{"streams":[
{{"name":"MediaFusion","description":"Movie.2024.1080p.WEB-DL","url":"https://mf.example/streaming_provider/SECRET/playback/torbox/{h1}"}},
{{"name":"Comet","description":"Movie.2024.2160p.BluRay","url":"https://comet.example/CFG/playback/{h2}/0/Movie.mkv"}},
{{"name":"Torrentio","title":"Movie.2024.720p","url":"https://torrentio.example/resolve/torbox/KEY/{h3}/Movie.mkv/1"}},
{{"name":"no hash","url":"https://x.example/playback/short"}},
{{"name":"hash only in the query","url":"https://x.example/play?h={h1}"}},
{{"name":"field wins","infoHash":"{h3}","url":"https://x.example/playback/{h1}"}}
]}}"#
    );
    let seeds = parse_stremio_streams(body.as_bytes(), "own");
    let got: Vec<String> = seeds.iter().map(|s| s.info_hash.clone()).collect();
    let want = [h1.clone(), h2.to_lowercase(), h3.clone(), h3.clone()];
    assert_eq!(got.join(" "), want.join(" "), "hashes = {got:?}, want {want:?}");
}

#[tokio::test]
async fn bounded_host_limiter_forgets_hosts_past_its_ceiling() {
    use crate::hostlimit::{new_bounded_host_limiter, wait};
    use std::time::Duration;

    let l = new_bounded_host_limiter(Duration::from_secs(3600), 1, 2);
    wait(Some(&l), "a.example").await;
    wait(Some(&l), "b.example").await;
    wait(Some(&l), "c.example").await;
    let buckets = l.buckets.lock().unwrap().len();
    assert_eq!(buckets, 1, "buckets = {buckets}, want the map started over at the ceiling");
}

// Not in the Go suite. Go refuses a non-public address in the dialer's Control hook, which sees every connection's
// address. reqwest has no such hook, so the guard is built from a resolver filter, a check on an IP-literal host
// before the first request, and the same check on each redirect hop; these pin each of the three.

fn get(url: &str) -> reqwest::Request {
    reqwest::Request::new(reqwest::Method::GET, url.parse().expect("test url parses"))
}

#[tokio::test]
async fn source_client_refuses_an_ip_literal_on_the_first_request() {
    use crate::scrape::Doer;
    use crate::sources::{new_source_client, ERR_NOT_PUBLIC};

    let client = new_source_client();
    for url in [
        "https://127.0.0.1:1/manifest.json",
        "https://[::1]:1/manifest.json",
        "https://10.0.0.5/cfg",
        "https://192.168.86.193:8080/",
        "https://169.254.169.254/latest/meta-data/",
        "https://100.101.102.103/cfg",
        "https://[::ffff:127.0.0.1]/cfg",
        "https://[fd7a:115c:a1e0::1]/cfg",
        // The url crate reads these as the IPv4 address they stand for, as getaddrinfo would.
        "https://2130706433/cfg",
        "https://0x7f.1/cfg",
    ] {
        let err = client.do_request(get(url)).await.expect_err(url);
        assert_eq!(err.to_string(), ERR_NOT_PUBLIC, "{url} was not refused as a non-public address");
    }
}

#[tokio::test]
async fn source_client_refuses_a_name_that_resolves_to_a_non_public_address() {
    use crate::scrape::Doer;
    use crate::sources::{new_source_client, ERR_NOT_PUBLIC};

    // `localhost` resolves to the loopback, and nothing in the config check is what stops it here.
    let err =
        new_source_client().do_request(get("https://localhost:1/manifest.json")).await.expect_err("refused");
    assert_eq!(err.to_string(), ERR_NOT_PUBLIC, "a name resolving to loopback was not refused");
}

#[test]
fn source_redirects_are_judged_on_every_hop() {
    use crate::sources::{
        redirect_verdict,
        RedirectVerdict::{Follow, Refuse, Stop},
    };

    let cases = [
        ("https://example.com/next", 1, Follow, "an ordinary hop"),
        ("https://8.8.8.8/next", 2, Follow, "a public literal"),
        ("https://example.com/next", 3, Stop, "past three hops"),
        ("http://example.com/next", 1, Stop, "https only"),
        // A downgrade is not followed, so the private address behind it is never dialled either.
        ("http://127.0.0.1/next", 1, Stop, "https only comes first"),
        ("https://127.0.0.1/next", 1, Refuse, "loopback literal"),
        ("https://[::1]/next", 2, Refuse, "IPv6 loopback literal"),
        ("https://10.1.2.3:8443/next", 1, Refuse, "private literal"),
        ("https://169.254.169.254/latest/meta-data/", 1, Refuse, "cloud metadata"),
        ("https://[::ffff:192.168.1.1]/next", 1, Refuse, "IPv4-mapped private literal"),
        ("https://2130706433/next", 1, Refuse, "decimal loopback"),
    ];
    for (url, hops, want, why) in cases {
        let got = redirect_verdict(&url.parse().expect("test url parses"), hops);
        assert_eq!(got, want, "{url} after {hops} hops ({why})");
    }
}

#[test]
fn normalize_sources_refuses_what_go_does_not_read_as_a_plain_https_url() {
    for link in [
        "https://@host.example/cfg",
        "https:host.example/cfg",
        "HTTP://host.example/cfg",
        "https://",
        "https:///cfg",
    ] {
        let got = crate::sources::normalize_sources(&[link.to_owned()]);
        assert!(got.is_empty(), "{link:?} was kept as {got:?}");
    }
    let got = crate::sources::normalize_sources(&["HTTPS://host.example/cfg".to_owned()]);
    assert_eq!(
        got,
        ["HTTPS://host.example/cfg"],
        "Go keeps the link as it was written when the scheme is upper-case"
    );
}
