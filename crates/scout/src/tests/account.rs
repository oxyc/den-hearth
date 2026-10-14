//! Go: `internal/scout/account_test.go` (157 lines). Work package 1.
//!
//! Every test here goes through the handler (`new_handler`, `test_deps`, `get`, `valid_blob`).

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use serde_json::Value;

use crate::cache::MemoryCache;
use crate::handler::{new_handler, Handler};
use crate::stores::realdebrid::RealDebridStore;
use crate::stores::torbox::{TorBoxStore, TORBOX_API};
use crate::stores::Store;
use crate::tests::handler::{get, shared_stores, test_deps, valid_blob};
use crate::tests::mock::{resp, MockDoer};

/// accountTorBoxHandler is a handler whose one store is a real TorBox store answering /<config>/account's
/// three upstream reads (user/me, mylist, getqueued) through `reply`.
fn account_torbox_handler(
    token: &str,
    reply: impl Fn(&reqwest::Request) -> reqwest::Response + Send + Sync + 'static,
) -> (Handler, Arc<AtomicUsize>) {
    let calls = Arc::new(AtomicUsize::new(0));
    let counter = calls.clone();
    let store: Arc<dyn Store> = Arc::new(TorBoxStore {
        token: token.to_owned(),
        api: TORBOX_API.to_owned(),
        cache: Some(Arc::new(MemoryCache::new(1 << 20))),
        client: Arc::new(MockDoer(move |req| {
            counter.fetch_add(1, Ordering::SeqCst);
            Ok(reply(&req))
        })),
    });
    let h = new_handler(test_deps(move |d| d.make_stores = shared_stores(store.clone())));
    (h, calls)
}

fn decode_account_body(body: &[u8]) -> Vec<serde_json::Map<String, Value>> {
    match serde_json::from_slice::<Vec<serde_json::Map<String, Value>>>(body) {
        Ok(out) => out,
        Err(e) => panic!("account body did not decode as an array: {e} ({})", String::from_utf8_lossy(body)),
    }
}

fn base(req: &reqwest::Request) -> String {
    req.url().path().rsplit('/').next().unwrap_or("").to_owned()
}

// A known plan code maps to its name, active/finished entries are told apart, and the queue length is
// the account's own queued-torrents list — not scout's add budget.
#[tokio::test]
async fn account_known_plan() {
    let (h, calls) = account_torbox_handler("tb-account-known-plan", |req| match base(req).as_str() {
        "me" => resp(
            200,
            r#"{"success":true,"data":{"plan":2,"cooldown_until":null,"premium_expires_at":"2027-01-02T00:00:00Z"}}"#,
        ),
        "mylist" => resp(
            200,
            r#"{"success":true,"data":[{"active":true,"download_finished":false},{"active":true,"download_finished":true},{"active":false,"download_finished":false}]}"#,
        ),
        "getqueued" => resp(200, r#"{"success":true,"data":[{"id":1},{"id":2}]}"#),
        _ => resp(404, "{}"),
    });
    let rr = get(&h, &format!("/{}/account", valid_blob()), &[]).await;
    assert_eq!(rr.status(), 200, "account");
    let out = decode_account_body(rr.body());
    assert!(
        out.len() == 1 && out[0]["service"] == "torbox" && out[0]["plan"] == "pro",
        "account body = {out:?}"
    );
    // "active" counts the active, unfinished entries only.
    assert_eq!(out[0]["slots"]["active"], 1, "slots = {:?}, want active=1", out[0]["slots"]);
    assert_eq!(out[0]["queued"], 2, "queued = {:?}, want 2", out[0]["queued"]);
    assert_eq!(out[0]["expiresAt"], "2027-01-02T00:00:00Z");
    assert!(!out[0].contains_key("cooldownUntil"), "cooldownUntil should be omitted when null");
    assert_eq!(calls.load(Ordering::SeqCst), 3, "upstream calls (user/me, mylist, getqueued)");
}

// A plan code this package does not know still answers — "unknown" rather than a guess, and no slot
// limit anywhere: den#204's owner decision is to show "N active" alone.
#[tokio::test]
async fn account_unknown_plan() {
    let (h, _) = account_torbox_handler("tb-account-unknown-plan", |req| match base(req).as_str() {
        "me" => resp(200, r#"{"success":true,"data":{"plan":99}}"#),
        "mylist" => resp(200, r#"{"success":true,"data":[]}"#),
        "getqueued" => resp(200, r#"{"success":true,"data":[]}"#),
        _ => resp(404, "{}"),
    });
    let rr = get(&h, &format!("/{}/account", valid_blob()), &[]).await;
    let out = decode_account_body(rr.body());
    assert!(out.len() == 1 && out[0]["plan"] == "unknown", "account body = {out:?}, want plan=unknown");
    assert!(
        !out[0]["slots"].as_object().is_some_and(|s| s.contains_key("limit")),
        "slots.limit must never be present: {:?}",
        out[0]["slots"]
    );
}

// A refused upstream degrades to the bare service name rather than failing the whole route: the Downloads
// shelf still has something to show (scout's own budget is reported from a separate, always-answerable
// source — see account_known_plan).
#[tokio::test]
async fn account_refused_upstream() {
    let (h, _) = account_torbox_handler("tb-account-refused", |_| resp(500, r#"{"error":"server_error"}"#));
    let rr = get(&h, &format!("/{}/account", valid_blob()), &[]).await;
    assert_eq!(rr.status(), 200, "account: degraded per-account, not a route failure");
    let out = decode_account_body(rr.body());
    assert!(out.len() == 1 && out[0]["service"] == "torbox", "account body = {out:?}");
    assert!(!out[0].contains_key("plan"), "a refused read must not report a plan: {:?}", out[0]);
}

// Cached for 30s per account: a second request within the window costs no further upstream reads.
#[tokio::test]
async fn account_cache_reuse_within_30s() {
    let (h, calls) = account_torbox_handler("tb-account-cache-reuse", |req| match base(req).as_str() {
        "me" => resp(200, r#"{"success":true,"data":{"plan":1}}"#),
        "mylist" => resp(200, r#"{"success":true,"data":[]}"#),
        "getqueued" => resp(200, r#"{"success":true,"data":[]}"#),
        _ => resp(404, "{}"),
    });
    get(&h, &format!("/{}/account", valid_blob()), &[]).await;
    let first = calls.load(Ordering::SeqCst);
    get(&h, &format!("/{}/account", valid_blob()), &[]).await;
    let second = calls.load(Ordering::SeqCst);
    assert_eq!(second, first, "a second read within 30s made {} more upstream calls, want 0", second - first);
}

// A non-TorBox account answers with only its service name — Real-Debrid and Premiumize publish nothing
// comparable.
#[tokio::test]
async fn account_non_torbox_service() {
    let store: Arc<dyn Store> = Arc::new(RealDebridStore {
        token: "rd-account".to_owned(),
        api: String::new(),
        cache: None,
        client: Arc::new(MockDoer(|_| Ok(resp(404, "{}")))),
    });
    let h = new_handler(test_deps(move |d| d.make_stores = shared_stores(store.clone())));
    let rr = get(&h, &format!("/{}/account", valid_blob()), &[]).await;
    let out = decode_account_body(rr.body());
    assert!(
        out.len() == 1 && out[0]["service"] == "realdebrid" && out[0].len() == 1,
        "account body = {out:?}, want only {{service: realdebrid}}"
    );
}

// ---- The store's AccountSummary, without the handler -------------------------------------------------------------
//
// /<config>/account renders what the store reports. These pin what the store reports, with the same upstream replies
// the handler tests above use.

mod store {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    use crate::cache::MemoryCache;
    use crate::stores::torbox::{TorBoxStore, TORBOX_API};
    use crate::stores::StoreError;
    use crate::tests::mock::{resp, MockDoer};

    fn base(req: &reqwest::Request) -> String {
        req.url().path().rsplit('/').next().unwrap_or("").to_owned()
    }

    fn store(
        token: &str,
        reply: impl Fn(&reqwest::Request) -> reqwest::Response + Send + Sync + 'static,
    ) -> (TorBoxStore, Arc<AtomicUsize>) {
        let calls = Arc::new(AtomicUsize::new(0));
        let counter = calls.clone();
        let s = TorBoxStore {
            token: token.to_owned(),
            api: TORBOX_API.to_owned(),
            cache: Some(Arc::new(MemoryCache::new(1 << 20))),
            client: Arc::new(MockDoer(move |req| {
                counter.fetch_add(1, Ordering::SeqCst);
                Ok(reply(&req))
            })),
        };
        (s, calls)
    }

    // Active entries are told apart from finished and idle ones, and the queue length is the account's own
    // queued-torrents list — not scout's add budget.
    #[tokio::test]
    async fn summary_counts_active_and_queued() {
        let (s, calls) = store("tb-account-known-plan", |req| match base(req).as_str() {
            "me" => resp(
                200,
                r#"{"success":true,"data":{"plan":2,"cooldown_until":null,"premium_expires_at":"2027-01-02T00:00:00Z"}}"#,
            ),
            "mylist" => resp(
                200,
                r#"{"success":true,"data":[{"active":true,"download_finished":false},{"active":true,"download_finished":true},{"active":false,"download_finished":false}]}"#,
            ),
            "getqueued" => resp(200, r#"{"success":true,"data":[{"id":1},{"id":2}]}"#),
            _ => resp(404, "{}"),
        });
        let sum = s.account_summary().await.expect("summary");
        assert_eq!(sum.plan, 2);
        assert_eq!((sum.active, sum.queued), (1, 2), "active counts the active, unfinished entries only");
        assert_eq!(sum.premium_expires_at.map(|t| t.to_rfc3339()).as_deref(), Some("2027-01-02T00:00:00Z"));
        assert!(sum.cooldown_until.is_none(), "a null cooldown is omitted");
        assert_eq!(calls.load(Ordering::SeqCst), 3, "upstream calls (user/me, mylist, getqueued)");
    }

    // Cached for 30s per account: a second request within the window costs no further upstream reads.
    #[tokio::test]
    async fn summary_is_reused_within_the_window() {
        let (s, calls) = store("tb-account-cache-reuse", |req| match base(req).as_str() {
            "me" => resp(200, r#"{"success":true,"data":{"plan":1}}"#),
            "mylist" | "getqueued" => resp(200, r#"{"success":true,"data":[]}"#),
            _ => resp(404, "{}"),
        });
        let first = s.account_summary().await.expect("summary");
        let made = calls.load(Ordering::SeqCst);
        let second = s.account_summary().await.expect("summary");
        assert_eq!(first, second);
        assert_eq!(calls.load(Ordering::SeqCst), made, "a second read within 30s made upstream calls");
    }

    // A refused upstream is an error the route degrades on, not a summary of nothing.
    #[tokio::test]
    async fn summary_of_a_refused_upstream_is_an_error() {
        let (s, _) = store("tb-account-refused", |_| resp(500, r#"{"error":"server_error"}"#));
        let err = s.account_summary().await.expect_err("a refused read is no summary");
        assert!(matches!(&err, StoreError::Unavailable(u) if u.status == 500), "{err:?}");
    }

    // The times are printed the way Go prints a parsed RFC 3339 time: offset kept, fraction trimmed.
    #[tokio::test]
    async fn summary_times_keep_their_offset_and_trim_the_fraction() {
        let (s, _) = store("tb-account-times", |req| match base(req).as_str() {
            "me" => resp(
                200,
                r#"{"success":true,"data":{"plan":3,"cooldown_until":"2027-01-02T03:04:05.500+02:00","premium_expires_at":"not a time"}}"#,
            ),
            "mylist" | "getqueued" => resp(200, r#"{"success":true,"data":[]}"#),
            _ => resp(404, "{}"),
        });
        let sum = s.account_summary().await.expect("summary");
        assert_eq!(
            sum.cooldown_until.map(|t| t.to_rfc3339()).as_deref(),
            Some("2027-01-02T03:04:05.5+02:00")
        );
        assert!(sum.premium_expires_at.is_none(), "an unparseable time is unknown, not a failure");
    }
}
