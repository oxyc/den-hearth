//! Go: `internal/scout/decisionlog_test.go` (192 lines).
//!
//! The rank, scrape and cancel decision lines come out of the handler, the Stremio scraper and the cancel route.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use crate::config::Indexer;
use crate::decisionlog::{decision_line, format_pick, set_log_identity, top_drop_reasons, MAX_LOGGED_TITLE};
use crate::handler::new_handler;
use crate::rank::RawStream;
use crate::scrape::{ScrapeQuery, StremioScraper};
use crate::stores::refusal::torrent_id_key;
use crate::stores::torbox::{mark_added_by_us, RESOLVE_CACHE_TTL};
use crate::tests::cancel::{cancel_path, cancel_torbox_handler};
use crate::tests::handler::{get, method, test_deps, valid_blob};
use crate::tests::mock::{resp, MockDoer};
use crate::tests::reqlog::{forget_logged, LogCapture};
use crate::tests::token_leak::{FailingDoer, LEAK_TOKEN};

const GIB: i64 = 1 << 30;

// The shared shape, and LOG_IDENTITY's one job: strip the identity suffix, keep everything else.
#[test]
fn decision_line_identity_gating() {
    set_log_identity(true);
    let on = decision_line(
        "rank",
        "served",
        "picked from 3 candidates",
        "torrentio",
        Duration::from_millis(12),
        "req-1",
        "imdb=tt1 type=movie",
    );
    for want in [
        "event=rank",
        "outcome=served",
        "reason=picked from 3 candidates",
        "upstream=torrentio",
        "dur_ms=12",
        "rid=req-1",
        "imdb=tt1 type=movie",
    ] {
        assert!(on.contains(want), "LOG_IDENTITY=on: line lacks {want:?}: {on}");
    }

    set_log_identity(false);
    let off = decision_line(
        "rank",
        "served",
        "picked from 3 candidates",
        "torrentio",
        Duration::from_millis(12),
        "req-1",
        "imdb=tt1 type=movie",
    );
    for want in ["event=rank", "outcome=served", "upstream=torrentio", "dur_ms=12", "rid=req-1"] {
        assert!(off.contains(want), "LOG_IDENTITY=off: line lost a non-identity field {want:?}: {off}");
    }
    assert!(
        !off.contains("imdb=") && !off.contains("type=movie"),
        "LOG_IDENTITY=off: identity survived: {off}"
    );
    set_log_identity(true);

    // upstream and rid are each omitted on their own when absent — neither forces the other's presence.
    let bare = decision_line("probe", "skipped", "not queued", "", Duration::ZERO, "", "");
    assert!(
        !bare.contains("upstream=") && !bare.contains("rid="),
        "an absent upstream/rid should not print at all: {bare}"
    );
}

#[test]
fn top_drop_reasons_ranks_by_count() {
    assert_eq!(top_drop_reasons(&HashMap::new(), 3), "", "nil map");
    let counts: HashMap<String, i64> = [("cam", 1), ("resolution", 5), ("minSeeders", 3), ("hdrOnly", 1)]
        .into_iter()
        .map(|(k, v)| (k.to_owned(), v))
        .collect();
    let got = top_drop_reasons(&counts, 3);
    assert!(
        got == "resolution:5,minSeeders:3,cam:1" || got == "resolution:5,minSeeders:3,hdrOnly:1",
        "top 3 by count, ties by name: got {got:?}"
    );
    assert!(got.starts_with("resolution:5,minSeeders:3,"), "highest counts first: got {got:?}");
}

#[test]
fn format_pick_labels_a_release() {
    let s = RawStream {
        title: "Movie.2024.2160p.BluRay.REMUX".to_owned(),
        info_hash: "a".repeat(40),
        size_bytes: Some(42 * GIB),
        cached: true,
        ..Default::default()
    };
    let got = format_pick(&s);
    for want in
        [r#""Movie.2024.2160p.BluRay.REMUX""#, &format!("hash={}", "a".repeat(12)), "size_gb=42.0", "cached"]
    {
        assert!(got.contains(want), "formatPick missing {want:?}: {got}");
    }
    assert!(!got.contains("uncached"), "a cached release must not also read uncached: {got}");
    // A release name longer than the cap is cut, never left to grow the log line without bound.
    let long = RawStream {
        title: "x".repeat(MAX_LOGGED_TITLE + 50),
        info_hash: "b".repeat(40),
        ..Default::default()
    };
    let got = format_pick(&long);
    assert!(
        !got.contains(&"x".repeat(MAX_LOGGED_TITLE + 1)),
        "formatPick did not truncate a long title: {got}"
    );
}

/// Go `lastLineContaining`: the last line of a captured log containing `needle`, for asserting on one decision line
/// within a buffer that accumulated several.
fn last_line_containing(buf: &str, needle: &str) -> String {
    match buf.lines().rev().find(|l| l.contains(needle)) {
        Some(l) => l.to_owned(),
        None => panic!("no line containing {needle:?} in:\n{buf}"),
    }
}

// Every list build logs its own pick — not just ?debug=1 — with the title id, how many candidates were
// in contention, the chosen release and its runner-up, and the drop reason that removed the junk one.
// LOG_IDENTITY off keeps rid but drops all of that.
#[tokio::test]
async fn rank_decision_line() {
    set_log_identity(true);
    let out = LogCapture::start();
    let h = new_handler(test_deps(|_| {}));

    forget_logged("rank-decision");
    get(&h, &format!("/{}/stream/movie/tt9000001.json", valid_blob()), &[("X-Request-Id", "req-rank-1")])
        .await;
    let got = out.text();
    for want in [
        "event=rank",
        "outcome=served",
        "rid=req-rank-1",
        "imdb=tt9000001",
        "type=movie",
        "top_drops=cam:1",
        "picked=",
        "runnerup1=",
    ] {
        assert!(got.contains(want), "rank decision line missing {want:?}:\n{got}");
    }

    set_log_identity(false);
    forget_logged("rank-decision");
    get(&h, &format!("/{}/stream/movie/tt9000002.json", valid_blob()), &[("X-Request-Id", "req-rank-2")])
        .await;
    let line2 = last_line_containing(&out.text(), "req-rank-2");
    assert!(line2.contains("rid=req-rank-2"), "rid must survive LOG_IDENTITY=off: {line2}");
    for leak in ["imdb=", "picked=", "top_drops="] {
        assert!(!line2.contains(leak), "LOG_IDENTITY=off still carries {leak:?}: {line2}");
    }
    set_log_identity(true);
}

// A scrape failure names the indexer, the error CLASS, how long it took and the request/title that hit
// it — never the indexer's base URL, which for a configured source can carry an encrypted config.
#[tokio::test]
async fn scrape_decision_line() {
    let out = LogCapture::start();

    forget_logged("indexer-status:torz");
    let bad = StremioScraper {
        indexer: Indexer("torz".into()),
        base_url: "https://torz.example/SECRET-PATH".into(),
        client: Arc::new(MockDoer(|_r: reqwest::Request| Ok(resp(502, "{}")))),
    };
    let _ = bad
        .scrape_once(
            "",
            &ScrapeQuery {
                kind: "movie".into(),
                imdb: "tt1234567".into(),
                rid: "req-scrape-1".into(),
                ..Default::default()
            },
        )
        .await;
    let got = out.text();
    for want in [
        "event=scrape",
        "upstream=torz",
        "rid=req-scrape-1",
        "imdb=tt1234567",
        "type=movie",
        "reason=http 502",
    ] {
        assert!(got.contains(want), "scrape decision line missing {want:?}:\n{got}");
    }
    assert!(
        !got.contains("SECRET-PATH") && !got.contains("torz.example"),
        "the indexer base URL leaked into the log:\n{got}"
    );

    // A transport failure never logs the error itself — Go's http.Client wraps a *url.Error carrying the
    // full request URL, which for a debrid-backed indexer carries the account token in its query string.
    forget_logged("indexer-unreachable:torz");
    let unreachable = StremioScraper {
        indexer: Indexer("torz".into()),
        base_url: "https://torz.example/SECRET-PATH".into(),
        client: Arc::new(FailingDoer { last_url: std::sync::Mutex::new(String::new()) }),
    };
    let _ = unreachable
        .scrape_once(
            "",
            &ScrapeQuery {
                kind: "movie".into(),
                imdb: "tt7654321".into(),
                rid: "req-scrape-2".into(),
                ..Default::default()
            },
        )
        .await;
    let got = out.text();
    assert!(
        got.contains("rid=req-scrape-2") && got.contains("event=scrape"),
        "transport-failure decision line missing its shape:\n{got}"
    );
    assert!(
        !got.contains("SECRET-PATH") && !got.contains(LEAK_TOKEN),
        "the transport error (or its URL) leaked into the log:\n{got}"
    );
}

// A play/probe/cancel decision line never carries the debrid token, even when the store's own error
// text would have — the redaction is at the store (stores.go), and this proves the new line respects it
// end to end, through the real DELETE /play route.
#[tokio::test]
async fn cancel_decision_line_never_leaks_the_token() {
    let out = LogCapture::start();
    forget_logged("cancel-store-unavailable");
    let hash = "a".repeat(40);
    let prime_hash = hash.clone();
    let (h, _) = cancel_torbox_handler(
        |r| match r.url().path().rsplit('/').next().unwrap_or("") {
            "mylist" if r.url().query_pairs().any(|(k, v)| k == "id" && !v.is_empty()) => {
                resp(200, r#"{"success":true,"data":{"id":42,"download_finished":false}}"#)
            }
            "mylist" => resp(200, r#"{"success":true,"data":[]}"#),
            "controltorrent" => {
                resp(503, r#"{"error":"proxy_error","detail":"upstream rejected Bearer tb-secret"}"#)
            }
            _ => resp(404, "{}"),
        },
        Some(Box::new(move |cache| {
            cache.put(&torrent_id_key("tb-secret", &prime_hash), "42", RESOLVE_CACHE_TTL);
            mark_added_by_us(cache, "tb-secret", &prime_hash);
        })),
    );
    let rr = method(&h, http::Method::DELETE, &cancel_path(&hash), &[("X-Request-Id", "req-cancel-1")]).await;
    assert_eq!(rr.status(), 503, "cancel: {}, want 503: {}", rr.status(), String::from_utf8_lossy(rr.body()));
    let got = out.text();
    assert!(
        got.contains("event=cancel") && got.contains("rid=req-cancel-1") && got.contains("upstream=torbox"),
        "cancel decision line missing its shape:\n{got}"
    );
    // The precondition: the refusal detail really did reach the error, so redaction — not the fixture's
    // silence — is what the assertion below is proving.
    assert!(
        got.contains("proxy_error"),
        "precondition failed: the refusal detail never reached the line:\n{got}"
    );
    assert!(!got.contains("tb-secret"), "the debrid token leaked into the cancel decision line:\n{got}");
}
