//! Go: `internal/scout/token_leak_test.go` (97 lines).

use std::sync::{Arc, Mutex};

use async_trait::async_trait;

use crate::scrape::{DoError, Doer};
use crate::stores::torbox::TorBoxStore;

pub(crate) const LEAK_TOKEN: &str = "tb-super-secret-token-do-not-log";

/// Go `failingDoer` reproduces what an http.Client returns on a transport failure: a *url.Error carrying the
/// full request URL — including, for TorBox's requestdl, the account token in its query string.
pub(crate) struct FailingDoer {
    pub(crate) last_url: Mutex<String>,
}

#[async_trait]
impl Doer for FailingDoer {
    async fn do_request(&self, req: reqwest::Request) -> Result<reqwest::Response, DoError> {
        let url = req.url().to_string();
        *self.last_url.lock().unwrap() = url.clone();
        Err(format!("error sending request for url ({url}): dial tcp: i/o timeout").into())
    }
}

// The debrid token must never reach an error string.
//
// `docs/SEALED-CONFIG.md` lists "the token is never logged" as an acceptance criterion, and the sealing
// design exists precisely because the URL *is* the credential. TorBox's requestdl takes the token as a
// query parameter, and `*url.Error.Error()` embeds the whole URL — Go redacts userinfo passwords, not
// query strings. Both the resolve log line and the /play error line print %v of this error, so one
// timeout would have written a live token into a rotated, shipped container log.
#[tokio::test]
async fn request_download_transport_error_never_carries_the_token() {
    let doer = Arc::new(FailingDoer { last_url: Mutex::new(String::new()) });
    let s = TorBoxStore {
        token: LEAK_TOKEN.to_owned(),
        client: doer.clone(),
        cache: None,
        api: "https://api.torbox.example/v1/api".to_owned(),
    };

    let err = s.request_download(42, None).await.expect_err("a transport failure must be an error");
    // The token really was on the wire — otherwise this test would pass for the wrong reason.
    let last = doer.last_url.lock().unwrap().clone();
    assert!(last.contains(LEAK_TOKEN), "precondition failed: the request did not carry the token ({last})");
    let msg = format!("{err:?}");
    assert!(!msg.contains(LEAK_TOKEN), "the token leaked into the error: {msg}");
    // And the cause survives, or the redaction has cost the diagnosis it was meant to preserve.
    assert!(msg.contains("timeout"), "the reason was lost along with the URL: {msg}");
}

// den#204's cancel route authenticates controltorrent/controlqueued/mylist by header, not query string —
// unlike requestdl above, the token is never on the URL at all. But a misbehaving proxy can still echo it
// into a response BODY, and every error path here must redact that the same way addMagnet already does.
#[tokio::test]
async fn cancel_never_leaks_the_token() {
    use std::time::Duration;

    use crate::cache::{Cache, MemoryCache};
    use crate::stores::refusal::torrent_id_key;
    use crate::stores::torbox::{mark_added_by_us, TORBOX_API};
    use crate::stores::Store;
    use crate::tests::mock::{resp, MockDoer};

    let hash = "a".repeat(40);
    let doer = MockDoer(|req| {
        let p = req.url().path().to_owned();
        if p.contains("mylist") {
            return Ok(resp(200, r#"{"success":true,"data":{"id":42,"download_finished":false}}"#));
        }
        if p.contains("controltorrent") {
            return Ok(resp(
                503,
                &format!(r#"{{"error":"proxy_error","detail":"upstream rejected Bearer {LEAK_TOKEN}"}}"#),
            ));
        }
        Ok(resp(404, "{}"))
    });
    let cache = Arc::new(MemoryCache::new(1 << 20));
    let s = TorBoxStore {
        token: LEAK_TOKEN.to_owned(),
        client: Arc::new(doer),
        cache: Some(cache.clone()),
        api: TORBOX_API.to_owned(),
    };
    let identity = s.account_identity();
    mark_added_by_us(&*cache, &identity, &hash);
    cache.put(&torrent_id_key(&identity, &hash), "42", Duration::from_secs(3600));

    let err = match s.cancel(&hash, false).await {
        Some(Err(e)) => e,
        other => panic!("a 503 from controltorrent must be an error, got {other:?}"),
    };
    // The token really was in the body — otherwise this test would pass for the wrong reason.
    let msg = format!("{err:?}");
    assert!(
        msg.contains("proxy_error"),
        "precondition failed: the refusal detail did not reach the error: {msg}"
    );
    assert!(!msg.contains(LEAK_TOKEN), "the token leaked into the cancel error: {msg}");
}

// transportKind reports the cause without the address.
//
// The Go fixtures are *url.Error values; here they are the boxed transport errors reqwest hands back, built
// from the same text.
#[test]
fn transport_kind() {
    use crate::scrape::DoError;
    use crate::stores::refusal::transport_kind;

    let timeout: DoError = format!(
        "error sending request for url (https://api.torbox.example/x?token={LEAK_TOKEN}): \
         context deadline exceeded (Client.Timeout exceeded)"
    )
    .into();
    assert!(
        !transport_kind(Some(timeout.as_ref())).contains(LEAK_TOKEN),
        "url.Error leaked its query string"
    );
    let cancelled: DoError =
        format!("error sending request for url (https://x?token={LEAK_TOKEN}): operation cancelled").into();
    let got = transport_kind(Some(cancelled.as_ref()));
    assert!(got == "cancelled" && !got.contains(LEAK_TOKEN), "cancellation: {got}");
    assert_eq!(transport_kind(None), "unknown");
}
