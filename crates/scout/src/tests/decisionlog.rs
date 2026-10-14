//! Go: `internal/scout/decisionlog_test.go` (192 lines). Work package 6.
//!
//! The line shape, the drop ranking and the pick label are live. The rank, scrape and cancel decision lines come
//! out of the handler, the Stremio scraper and the cancel route (packages 4, 5 and 1), so those are ignored with
//! their bodies compiled out.

use std::collections::HashMap;
use std::time::Duration;

use crate::decisionlog::{decision_line, format_pick, set_log_identity, top_drop_reasons, MAX_LOGGED_TITLE};
use crate::rank::RawStream;

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

// Every list build logs its own pick — not just ?debug=1 — with the title id, how many candidates were
// in contention, the chosen release and its runner-up, and the drop reason that removed the junk one.
// LOG_IDENTITY off keeps rid but drops all of that.
#[test]
#[ignore = "needs handler::serve, capture_log and forget_logged (packages 4, 6)"]
fn rank_decision_line() {
    // Enable when package 4 lands: move this body up and delete the wrapper.
    #[cfg(any())]
    fn pending() {
        use crate::deps::Deps;
        use crate::handler::serve;
        use crate::tests::reqlog::{capture_log, forget_logged, last_line_containing};
        set_log_identity(true);
        let out = capture_log(|| {
            let deps = Deps::for_test(|_| {});
            forget_logged("rank-decision");
            serve(&deps, get_with_rid("/tt9000001", "req-rank-1"));
        });
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
            assert!(out.contains(want), "rank decision line missing {want:?}:\n{out}");
        }

        set_log_identity(false);
        let out = capture_log(|| {
            let deps = Deps::for_test(|_| {});
            forget_logged("rank-decision");
            serve(&deps, get_with_rid("/tt9000002", "req-rank-2"));
        });
        let line2 = last_line_containing(&out, "req-rank-2");
        assert!(line2.contains("rid=req-rank-2"), "rid must survive LOG_IDENTITY=off: {line2}");
        for leak in ["imdb=", "picked=", "top_drops="] {
            assert!(!line2.contains(leak), "LOG_IDENTITY=off still carries {leak:?}: {line2}");
        }
        set_log_identity(true);
    }
    todo!("pending: decisionlog_test.go:~101 TestRankDecisionLine")
}

// A scrape failure names the indexer, the error CLASS, how long it took and the request/title that hit
// it — never the indexer's base URL, which for a configured source can carry an encrypted config.
#[test]
#[ignore = "needs stremioScraper (scrape.rs, package 5), handler test doubles and capture_log (packages 4, 5, 6)"]
fn scrape_decision_line() {
    // Enable when packages 4, 5 and 6 land: move this body up and delete the wrapper.
    #[cfg(any())]
    fn pending() {
        use crate::scrape::{ScrapeQuery, StremioScraper};
        use crate::tests::reqlog::{capture_log, forget_logged};
        let out = capture_log(|| {
            forget_logged("indexer-status:torz");
            let bad = StremioScraper::new(
                "torz",
                "https://torz.example/SECRET-PATH",
                mock_doer(|_| resp(502, "{}")),
            );
            let _ = bad.scrape_once(&ScrapeQuery {
                kind: "movie".into(),
                imdb: "tt1234567".into(),
                rid: "req-scrape-1".into(),
                ..Default::default()
            });
        });
        for want in [
            "event=scrape",
            "upstream=torz",
            "rid=req-scrape-1",
            "imdb=tt1234567",
            "type=movie",
            "reason=http 502",
        ] {
            assert!(out.contains(want), "scrape decision line missing {want:?}:\n{out}");
        }
        assert!(
            !out.contains("SECRET-PATH") && !out.contains("torz.example"),
            "the indexer base URL leaked into the log:\n{out}"
        );

        // A transport failure never logs the error itself — Go's http.Client wraps a *url.Error carrying the
        // full request URL, which for a debrid-backed indexer carries the account token in its query string.
        let out = capture_log(|| {
            forget_logged("indexer-unreachable:torz");
            let unreachable = StremioScraper::new("torz", "https://torz.example/SECRET-PATH", failing_doer());
            let _ = unreachable.scrape_once(&ScrapeQuery {
                kind: "movie".into(),
                imdb: "tt7654321".into(),
                rid: "req-scrape-2".into(),
                ..Default::default()
            });
        });
        assert!(
            out.contains("rid=req-scrape-2") && out.contains("event=scrape"),
            "transport-failure decision line missing its shape:\n{out}"
        );
        assert!(
            !out.contains("SECRET-PATH") && !out.contains(LEAK_TOKEN),
            "the transport error (or its URL) leaked into the log:\n{out}"
        );
    }
    todo!("pending: decisionlog_test.go:~146 TestScrapeDecisionLine")
}

// A play/probe/cancel decision line never carries the debrid token, even when the store's own error
// text would have — the redaction is at the store (stores.go), and this proves the new line respects it
// end to end, through the real DELETE /play route.
#[test]
#[ignore = "needs the cancel route, cancelTorBoxHandler and the TorBox store (packages 1, 4)"]
fn cancel_decision_line_never_leaks_the_token() {
    // Enable when packages 1 and 4 land: move this body up and delete the wrapper.
    #[cfg(any())]
    fn pending() {
        use crate::tests::reqlog::{capture_log, forget_logged};
        let out = capture_log(|| {
            forget_logged("cancel-store-unavailable");
            let hash = "a".repeat(40);
            let h = cancel_torbox_handler(
                |r| match last_segment(r.path()) {
                    "mylist" if r.query("id").is_some() => {
                        resp(200, r#"{"success":true,"data":{"id":42,"download_finished":false}}"#)
                    }
                    "mylist" => resp(200, r#"{"success":true,"data":[]}"#),
                    "controltorrent" => {
                        resp(503, r#"{"error":"proxy_error","detail":"upstream rejected Bearer tb-secret"}"#)
                    }
                    _ => resp(404, "{}"),
                },
                |cache| {
                    cache.put(&torrent_id_key("tb-secret", &hash), "42", RESOLVE_CACHE_TTL);
                    mark_added_by_us(cache, "tb-secret", &hash);
                },
            );
            let rr = do_method(&h, "DELETE", &cancel_path(&hash), &[("X-Request-Id", "req-cancel-1")]);
            assert_eq!(rr.status(), 503, "cancel: want 503: {}", rr.body());
        });
        assert!(
            out.contains("event=cancel")
                && out.contains("rid=req-cancel-1")
                && out.contains("upstream=torbox"),
            "cancel decision line missing its shape:\n{out}"
        );
        // The precondition: the refusal detail really did reach the error, so redaction — not the fixture's
        // silence — is what the assertion below is proving.
        assert!(
            out.contains("proxy_error"),
            "precondition failed: the refusal detail never reached the line:\n{out}"
        );
        assert!(!out.contains("tb-secret"), "the debrid token leaked into the cancel decision line:\n{out}");
    }
    todo!("pending: decisionlog_test.go:~179 TestCancelDecisionLine_neverLeaksTheToken")
}
