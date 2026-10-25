//! TorBox stopping answering, end to end through the handler and a real `TorBoxStore`: each poll used to wait out
//! the whole status budget again (the 2026-10-25 outage: 8 s a request, all night). Once TorBox is known down a poll
//! is answered at once with `debrid_down`, /health says so, and the first answer after it comes back ends it.

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use tokio::time::Instant;

use crate::handler::{new_handler, DebridSignal, Handler};
use crate::managed_hls::{ManagedHlsError, ManagedHlsRequest, ManagedHlsResolver};
use crate::play::{encode_play_token, PlayTarget};
use crate::scrape::{DoError, Doer};
use crate::stores::torbox::health::{CallOutcome, DOWN_AFTER, RECOVERY_PROBE_EVERY, SLOW_ANSWER};
use crate::stores::torbox::{TorBoxStore, TORBOX_API};
use crate::tests::handler::{blob, body_str, get, test_deps, valid_blob};

/// A TorBox that can stop answering: while `down` is set every request hangs, as TorBox's API did; once cleared,
/// each is answered with an empty success.
#[derive(Clone, Default)]
struct SwitchableTorBox {
    down: Arc<AtomicBool>,
    calls: Arc<AtomicUsize>,
}

#[async_trait]
impl Doer for SwitchableTorBox {
    async fn do_request(&self, _req: reqwest::Request) -> Result<reqwest::Response, DoError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        if self.down.load(Ordering::SeqCst) {
            std::future::pending::<()>().await;
        }
        Ok(crate::tests::mock::resp(200, r#"{"success":true,"data":[]}"#))
    }
}

fn handler_over(torbox: &SwitchableTorBox) -> Handler {
    let doer = torbox.clone();
    new_handler(test_deps(move |d| {
        d.make_stores = Arc::new(move |_| {
            vec![Box::new(TorBoxStore {
                token: "tb-secret".into(),
                client: Arc::new(doer.clone()),
                cache: None,
                api: TORBOX_API.into(),
            })]
        });
    }))
}

async fn health(h: &Handler) -> serde_json::Value {
    serde_json::from_str(&body_str(&get(h, "/health", &[]).await)).expect("health is JSON")
}

#[tokio::test(start_paused = true)]
async fn a_torbox_that_stopped_answering_is_answered_at_once_and_recovers_on_its_own() {
    let torbox = SwitchableTorBox::default();
    torbox.down.store(true, Ordering::SeqCst);
    let h = handler_over(&torbox);
    let token = encode_play_token(&PlayTarget { info_hash: "a".repeat(40), ..Default::default() });
    let probe = format!("/{}/play/{token}?probe=1", valid_blob());
    assert!(
        health(&h).await.get("debrid").is_none(),
        "nothing to report before TorBox has been asked anything"
    );

    // Each poll waits out the status budget until enough calls have failed to call TorBox down.
    let mut waited = Vec::new();
    let refused = loop {
        let start = Instant::now();
        let r = get(&h, &probe, &[]).await;
        let took = start.elapsed();
        if body_str(&r).contains("debrid_down") {
            break (r, took);
        }
        waited.push(took);
        assert!(waited.len() <= 6, "TorBox was never called down; polls waited {waited:?}");
    };
    assert!(!waited.is_empty() && waited.iter().all(|t| *t >= SLOW_ANSWER), "polls before: {waited:?}");

    let (r, took) = refused;
    assert!(
        took < Duration::from_millis(50),
        "a poll while TorBox is down took {took:?}, want an immediate answer"
    );
    assert_eq!(r.status(), 503);
    assert_eq!(r.headers().get("x-den-degraded").map(|v| v.to_str().unwrap()), Some("debrid_down"));
    let retry: u64 = r.headers()["retry-after"].to_str().unwrap().parse().unwrap();
    assert!((1..=RECOVERY_PROBE_EVERY.as_secs()).contains(&retry), "Retry-After {retry}");
    let body: serde_json::Value = serde_json::from_str(&body_str(&r)).unwrap();
    assert_eq!(body["error"], "debrid_down");
    assert_eq!(body["service"], "torbox");
    assert_eq!(body["reason"], "timeout");

    // Polling while down asks TorBox nothing.
    let calls = torbox.calls.load(Ordering::SeqCst);
    get(&h, &probe, &[]).await;
    assert_eq!(torbox.calls.load(Ordering::SeqCst), calls, "a refused poll still reached TorBox");

    // A client must never read a stale "ok" from a cache.
    let raw = get(&h, "/health", &[]).await;
    assert_eq!(raw.headers().get("cache-control").map(|v| v.to_str().unwrap()), Some("no-store"));
    let report = health(&h).await;
    assert_eq!(report["status"], "degraded");
    assert_eq!(report["reason"], "debrid");
    assert_eq!(report["debrid"]["provider"], "torbox");
    assert_eq!(report["debrid"]["state"], "down");
    assert_eq!(report["debrid"]["reason"], "timeout");
    assert!(report["debrid"]["since"].as_u64().is_some_and(|ms| ms > 1_700_000_000_000), "since: {report}");

    // TorBox comes back. The next poll after the recovery window is let through, and its answer ends the outage.
    torbox.down.store(false, Ordering::SeqCst);
    tokio::time::advance(RECOVERY_PROBE_EVERY).await;
    let r = get(&h, &probe, &[]).await;
    assert!(!body_str(&r).contains("debrid_down"), "the recovery probe was refused: {}", body_str(&r));
    let report = health(&h).await;
    assert_eq!(report["debrid"]["state"], "ok", "{report}");
    assert_eq!(report["status"], "ok", "{report}");
}

#[tokio::test(start_paused = true)]
async fn play_is_answered_at_once_while_torbox_is_down() {
    let torbox = SwitchableTorBox::default();
    torbox.down.store(true, Ordering::SeqCst);
    let h = handler_over(&torbox);
    let token = encode_play_token(&PlayTarget { info_hash: "b".repeat(40), ..Default::default() });
    let play = format!("/{}/play/{token}", valid_blob());

    let mut tries = 0;
    let took = loop {
        let start = Instant::now();
        let r = get(&h, &play, &[]).await;
        if body_str(&r).contains("debrid_down") {
            break start.elapsed();
        }
        tries += 1;
        assert!(tries <= 6, "play never answered debrid_down");
    };
    assert!(took < Duration::from_millis(50), "a play while TorBox is down took {took:?}");
}

/// Counts the managed streams asked for, and makes none.
#[derive(Default)]
struct CountingManagedHls(AtomicUsize);

#[async_trait]
impl ManagedHlsResolver for CountingManagedHls {
    async fn resolve(
        &self,
        _config: &crate::config::Config,
        _target: &crate::stores::ResolveTarget,
        _subtitle: Option<i64>,
    ) -> Result<crate::managed_hls::ManagedHlsPrepared, ManagedHlsError> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Err(ManagedHlsError::Unavailable)
    }
}

// Hearth's /playback asks for a managed TorBox stream before Remux, which waited on TorBox as a play did before the
// play route's gate. It is refused at once by the same gate, and `debrid_signal` tells Hearth what that route says.
#[tokio::test(start_paused = true)]
async fn a_managed_stream_and_the_debrid_signal_follow_the_play_route_s_gate() {
    let runtime = crate::runtime::ScoutRuntime::new();
    let resolver = Arc::new(CountingManagedHls::default());
    let (rt, r) = (runtime.clone(), resolver.clone());
    let h = new_handler(test_deps(move |d| {
        d.runtime = rt;
        d.managed_hls = Some(r);
    }));
    let health = &runtime.torbox.health;
    let install = valid_blob();
    let mixed = blob(r#"{"debrid":[{"service":"torbox","token":"t"},{"service":"realdebrid","token":"r"}]}"#);
    let elsewhere = blob(r#"{"debrid":[{"service":"realdebrid","token":"r"}]}"#);
    assert_eq!(h.debrid_signal(&install), None, "nothing to say before TorBox was asked anything");

    health.record(CallOutcome::Answered(SLOW_ANSWER), Instant::now());
    assert_eq!(h.debrid_signal(&install), Some(DebridSignal::Degraded("debrid_slow")));

    for _ in 0..DOWN_AFTER {
        health.record(CallOutcome::Failed("timeout"), Instant::now());
    }
    assert!(
        matches!(h.debrid_signal(&install), Some(DebridSignal::Refused { reason: "timeout", retry_after })
            if retry_after <= RECOVERY_PROBE_EVERY),
        "{:?}",
        h.debrid_signal(&install)
    );
    // Scout's play route refuses only an install whose every debrid is TorBox; another's 503s are only marked.
    assert_eq!(h.debrid_signal(&mixed), Some(DebridSignal::Degraded("debrid_down")));
    assert_eq!(h.debrid_signal(&elsewhere), None);

    let request = ManagedHlsRequest { imdb: "tt0111161".into(), ..Default::default() };
    let started = Instant::now();
    let refused = h.managed_hls(&install, &request).await.expect_err("TorBox is down");
    assert!(
        matches!(refused, ManagedHlsError::DebridDown { retry_after } if retry_after <= RECOVERY_PROBE_EVERY),
        "{refused:?}"
    );
    assert_eq!(refused.code(), "torbox_down");
    assert_eq!(resolver.0.load(Ordering::SeqCst), 0, "TorBox was asked for a stream while down");
    assert!(started.elapsed() < Duration::from_millis(50), "took {:?}", started.elapsed());

    // Once a recovery probe is due, the signal lets the play go and leaves the probe for it to take.
    tokio::time::advance(RECOVERY_PROBE_EVERY).await;
    assert_eq!(h.debrid_signal(&install), Some(DebridSignal::Degraded("debrid_down")));
    assert_eq!(h.debrid_signal(&install), Some(DebridSignal::Degraded("debrid_down")));
    assert!(health.admit().is_ok(), "the signal took the recovery probe");
}
