//! Go: `internal/scout/refusal_test.go` (1563 lines). Work package 2.
//!
//! About a third of it goes through the handler (`TestHandleProbe_*`, `TestHandlePlay_*`,
//! `TestProbeAndPlay_*`) and `TestTieredCache_*` needs `diskcache` (work package 6); those stay red until
//! those modules land. Tests that need TorBox, the add budget or `scrape_all` are ignored stubs too: the
//! Go line is in each `todo!`, and the body is ported when the module it needs lands.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crate::cache::{Cache, MemoryCache};
use crate::config::DebridService;
use crate::scrape::{DoError, Doer};
use crate::stores::premiumize::{PremiumizeStore, PREMIUMIZE_API};
use crate::stores::realdebrid::{RealDebridStore, REAL_DEBRID_API};
use crate::stores::refusal::{
    add_outcome_unknown, already_queued, backed_off, note_add_attempt, pm_queued_key, record_refusal,
    unknown_outcome_key, ADD_GIVE_UP, PENDING_GIVE_UP, QUEUED_TTL, UNKNOWN_OUTCOME_TTL,
};
use crate::stores::{DeadLinkError, ResolveTarget, Store, StoreError, StoreUnavailableError};

use super::mock::{resp, MockDoer};

/// Go `H` (stores_test.go:19).
const H: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";

type Answer = Result<reqwest::Response, DoError>;

/// A `Doer` that answers every request with the same status and body.
fn answer(status: u16, body: &'static str) -> Arc<dyn Doer> {
    Arc::new(MockDoer(move |_r: reqwest::Request| -> Answer { Ok(resp(status, body)) }))
}

/// Go `context.Canceled` as the transport reports it: the request failed before any answer came back.
fn transport_error() -> Answer {
    Err("context canceled".into())
}

fn mem() -> Arc<dyn Cache> {
    Arc::new(MemoryCache::new(1 << 20))
}

/// The Unix time `ago` before now, as the markers store it.
fn unix_secs_ago(ago: Duration) -> String {
    (SystemTime::now() - ago).duration_since(UNIX_EPOCH).expect("after the epoch").as_secs().to_string()
}

fn target(hash: &str) -> ResolveTarget {
    ResolveTarget { info_hash: hash.to_owned(), ..Default::default() }
}

fn real_debrid(token: &str, cache: &Arc<dyn Cache>, client: Arc<dyn Doer>) -> RealDebridStore {
    RealDebridStore { token: token.into(), client, cache: Some(cache.clone()), api: REAL_DEBRID_API.into() }
}

fn premiumize(token: &str, cache: &Arc<dyn Cache>, client: Arc<dyn Doer>) -> PremiumizeStore {
    PremiumizeStore { token: token.into(), client, cache: Some(cache.clone()), api: PREMIUMIZE_API.into() }
}

/// A Real-Debrid or Premiumize store over the given cache and client, for the tests that run the same
/// assertion over both.
fn store_for(svc: DebridService, token: &str, cache: &Arc<dyn Cache>, client: Arc<dyn Doer>) -> Box<dyn Store> {
    match svc {
        DebridService::RealDebrid => Box::new(real_debrid(token, cache, client)),
        DebridService::Premiumize => Box::new(premiumize(token, cache, client)),
        DebridService::TorBox => unreachable!("TorBox is work package 1"),
    }
}

/// A refusal the service made (HTTP 429 and the like), as the stores record it.
fn refused(svc: DebridService, reason: &str) -> StoreError {
    StoreError::Unavailable(StoreUnavailableError { service: svc, reason: reason.into(), status: 429 })
}

/// Go `errors.As(err, &unavailable)`: the error is the service refusing us.
fn is_refusal(err: &StoreError) -> bool {
    matches!(err, StoreError::Unavailable(_))
}

// A refusal names the service and its reason. This string reaches the app, which renders it to the
// viewer as "TorBox is refusing requests right now" — a sentence someone can act on, unlike the "no
// source found" it replaced.
#[test]
fn store_unavailable_error_names_the_service() {
    let err = StoreUnavailableError {
        service: DebridService::TorBox,
        reason: "createtorrent http 429 (60 per 1 hour)".into(),
        status: 429,
    };
    let msg = err.to_string();
    assert!(msg.contains("torbox") && msg.contains("429"), "a refusal must carry who refused and why: {msg}");
    assert!(msg.starts_with("store_unavailable:"), "prefix identifies the class: {msg}");
}

// The probe route cannot discover a refusal on its own — it deliberately never calls the endpoint that
// refuses. It reads the backoff the queueing path wrote, so a throttled account reads as "the service
// refused us" rather than as an absence, which the client cannot tell from a dead release.
#[test]
#[ignore = "needs stores/torbox.rs (package 1) and the pool reporter"]
fn recent_refusal_reads_what_the_queueing_path_wrote() {
    todo!("port refusal_test.go:33 once package 1 lands")
}

// The disk tier turns itself off on the first write failure rather than logging the same fact per entry.
// Once off it must stay off and must not panic — the memory tier carries on alone.
#[test]
#[ignore = "needs diskcache.rs (package 6)"]
fn tiered_cache_disables_once_and_keeps_serving() {
    todo!("port refusal_test.go:59 once package 6 lands")
}

// The disk tier round-trips a value and expires it. This is what makes a restart cheap: the container is
// redeployed on every image push, and a probe costs a debrid resolve to rebuild.
#[test]
#[ignore = "needs diskcache.rs (package 6)"]
fn tiered_cache_persists_and_expires() {
    todo!("port refusal_test.go:76 once package 6 lands")
}

// A probe answers what is already true and starts nothing. `handleProbe` is the route that exists so a
// client can poll a wait without re-queueing the torrent on every poll — the bug that spent an hour's
// worth of the debrid's add allowance inside a single wait.
#[test]
#[ignore = "needs handler.rs (package 4)"]
fn handle_probe_reports_without_queueing() {
    todo!("port refusal_test.go:99 once package 4 lands")
}

// The probe's other three answers. Each is a different fact, and the app renders each differently — a
// download in progress, a release ready to play, and an account being refused are three situations that
// spent an evening being indistinguishable from one another.
#[test]
#[ignore = "needs handler.rs (package 4)"]
fn handle_probe_distinguishes_its_answers() {
    todo!("port refusal_test.go:131 once package 4 lands")
}

// Every store draws the same line: being turned away is a fact about the account, not the release.
//
// Only TorBox did. On a Real-Debrid or Premiumize install a 429 became `DeadLinkError`, reached the app
// as 404 "this release does not exist", and the player then walked the whole candidate list collecting
// the identical non-answer — condemning healthy releases on the way. That is the bug that was fixed once
// and left unfixed twice.
#[tokio::test]
async fn every_store_reports_a_refusal_as_a_refusal() {
    let stores = [
        (
            DebridService::RealDebrid,
            Box::new(RealDebridStore {
                token: "t".into(),
                client: answer(429, "{}"),
                cache: None,
                api: "https://rd.example".into(),
            }) as Box<dyn Store>,
        ),
        (
            DebridService::Premiumize,
            Box::new(PremiumizeStore {
                token: "t".into(),
                client: answer(429, "{}"),
                cache: None,
                api: "https://pm.example".into(),
            }) as Box<dyn Store>,
        ),
    ];
    for (want, store) in stores {
        let err = store.resolve(&target(&"e".repeat(40))).await;
        match err {
            Err(StoreError::Unavailable(u)) => assert_eq!(u.service, want, "wrong service named"),
            other => panic!("a throttle must not read as a dead link: {other:?}"),
        }
    }
}

// An unconfigured indexer must not make "nobody has this" permanently unsayable.
//
// Counting an indexer that can never be asked as one that did not answer made the quorum unsatisfiable
// on the shipped default (mediafusion has no config URL), so every genuinely empty result was reported
// as an outage, negative caching never ran, and three unavailable titles in a row flipped /health to
// degraded on a perfectly healthy service. A permanent misconfiguration is not a transient failure.
#[test]
#[ignore = "needs scrape.rs scrape_all (package 5)"]
fn scrape_all_unaskable_indexer_does_not_block_an_empty_verdict() {
    todo!("port refusal_test.go:222 once package 5 lands")
}

// Season and episode are read off the stream id, and a movie has neither. A wrong answer here sends the
// probe at the wrong episode of a pack.
#[test]
#[ignore = "needs probe_fanout.rs seasonOf/episodeOf (package 5)"]
fn season_episode_of() {
    todo!("port refusal_test.go:251 once package 5 lands")
}

// The backoff belongs to every store that can be refused, not just the one whose refusal was noticed
// first.
//
// Only TorBox remembered a refusal. The other two had no cache at all, so a client polling /play for the
// length of a download re-added the magnet once per poll — hundreds of adds for one wait, each leaving a
// duplicate torrent on the account. TorBox backing off correctly made this WORSE: ResolvePreferring fell
// straight through to whichever store had no memory of being turned away.
#[tokio::test]
async fn refusal_backoff_applies_to_every_store() {
    for svc in [DebridService::RealDebrid, DebridService::Premiumize] {
        let adds = Arc::new(AtomicUsize::new(0));
        let counter = adds.clone();
        let client: Arc<dyn Doer> = Arc::new(MockDoer(move |_r: reqwest::Request| -> Answer {
            counter.fetch_add(1, Ordering::SeqCst);
            Ok(resp(503, r#"{"error":"busy"}"#))
        }));
        let cache = mem();
        let store = store_for(svc, "tok", &cache, client);
        for i in 0..6 {
            // A client polling /play through a download.
            let err = store.resolve(&target(H)).await;
            assert!(matches!(&err, Err(e) if is_refusal(e)), "{svc:?} poll {i}: want a refusal, got {err:?}");
        }
        assert_eq!(adds.load(Ordering::SeqCst), 1, "{svc:?}: added more than once across six polls");
        assert!(store.recent_refusal(H).is_some(), "{svc:?}: cannot report the refusal it just recorded");
    }
}

// A refusal is remembered per SERVICE. One store being throttled says nothing about another, and sharing
// a key would let TorBox's backoff silence a Premiumize that is answering perfectly well.
#[test]
fn refusal_backoff_is_per_service() {
    let cache = MemoryCache::new(1 << 20);
    record_refusal(&cache, DebridService::TorBox, "tok", H, &refused(DebridService::TorBox, "429"));

    assert!(backed_off(&cache, DebridService::TorBox, "tok", H).is_some(), "torbox's own refusal was not remembered");
    assert!(
        backed_off(&cache, DebridService::Premiumize, "tok", H).is_none(),
        "torbox's refusal silenced premiumize"
    );
}

// A cancelled request is not a refusal. /play runs on the client's context and the client cancels
// aggressively, so recording one served a healthy release 503 for the next minute.
#[test]
fn refusal_backoff_ignores_cancellation() {
    let cache = MemoryCache::new(1 << 20);
    record_refusal(&cache, DebridService::TorBox, "tok", H, &StoreError::Cancelled);
    assert!(
        backed_off(&cache, DebridService::TorBox, "tok", H).is_none(),
        "a cancelled request was remembered as a refusal by the store"
    );
    // The Go test's second case, an expired deadline, is the same `StoreError::Cancelled` here.
    record_refusal(&cache, DebridService::TorBox, "tok", H, &StoreError::Cancelled);
    assert!(
        backed_off(&cache, DebridService::TorBox, "tok", H).is_none(),
        "an expired deadline was remembered as a refusal by the store"
    );
}

// The probe never queues a torrent, and says so honestly when it cannot tell.
//
// Three answers that were each wrong at some point: "ready" from a cache check alone (which reports what
// TorBox has, not what this account has), 404 "not_queued" when the check was simply unreachable, and a
// resolve that queued the very torrent the probe exists to avoid queueing.
#[test]
#[ignore = "needs handler.rs (package 4)"]
fn handle_probe_never_queues_and_says_when_it_cannot_tell() {
    todo!("port refusal_test.go:351 once package 4 lands")
}

// One step further out than the case above: the cache check WORKED, and the status read is the part that
// could not find out. 404 "not_queued" there is what makes a client blacklist a release that is
// downloading right now, so an indeterminate read gets "ask again" instead.
//
// Nothing asserted it — deleting the branch left the whole suite green, because the case it exists for
// (checkcached answers while mylist times out) is reachable only through a store that answers the two
// questions differently.
#[test]
#[ignore = "needs handler.rs (package 4)"]
fn handle_probe_an_indeterminate_status_is_not_an_absence() {
    todo!("port refusal_test.go:389 once package 4 lands")
}

// Scout's own refusals reach the CLIENT as scout's, not as the debrid's.
//
// errScoutSide was wired into the refusal memory and then the route went on reading the wrapped
// StoreUnavailableError and printing its service — so the app told the viewer TorBox was refusing while
// TorBox was answering perfectly well. Both scout-side refusals (the hourly allowance, and an add
// already in flight) go through this path.
#[test]
#[ignore = "needs handler.rs (package 4), addbudget.rs (package 3)"]
fn handle_play_names_scout_not_the_debrid_for_its_own_refusals() {
    todo!("port refusal_test.go:454 once packages 3 and 4 land")
}

// A read-only resolve is answered before the ADD backoff, and is never blocked by an add already in
// flight — both describe a state a NoAdd caller cannot have caused.
#[test]
#[ignore = "needs stores/torbox.rs (package 1)"]
fn resolve_no_add_is_answered_ahead_of_the_add_guards() {
    todo!("port refusal_test.go:485 once package 1 lands")
}

// An add scout already sent is answered 202 "coming", not 503 "refused".
//
// The release IS being fetched — by us, moments ago. Answering 503 named the debrid and, on the tvOS
// client, stopped it trying other sources: the viewer was told their debrid was refusing, for a release
// scout had itself just queued.
#[test]
#[ignore = "needs handler.rs (package 4)"]
fn handle_play_an_in_flight_add_is_queued_not_refused() {
    todo!("port refusal_test.go:504 once package 4 lands")
}

// A spent clock must not be remembered as the STORE refusing.
//
// The guard that stops an add being charged on a dead context returns a StoreUnavailableError, and
// recordRefusal files anything that is not a cancellation, not errScoutSide, not errAddInFlight and not
// errTorrentGone. So the refusal memory learned that TorBox had declined a release TorBox was never
// asked about: one cancelled poll — a viewer changing focus is enough — made a healthy release answer
// 503 on BOTH routes for the next sixty seconds, on a fresh clock, with the store skipped as "backing
// off". The clock is scout's, so the error says so, and errScoutSide is what keeps it out of the memory.
//
// All three stores, because only TorBox's guard had a test and the other two were free to drift.
//
// Not portable as written: a Rust store takes no context (PORTING.md: cancellation is dropping the
// future), so "a resolve on a spent clock" needs a decision on how a spent deadline reaches the store.
#[test]
#[ignore = "needs a decision: a Rust store takes no context (PORTING.md); also stores/torbox.rs (package 1)"]
fn resolve_a_spent_clock_is_not_remembered_as_the_store_refusing() {
    todo!("port refusal_test.go:538 once the deadline seam is decided")
}

// The probe stops believing an add at the same moment /play does.
//
// addInFlight, which /play's resolve consults, checks unknownTooLong FIRST and calls the release dead
// past addGiveUp. The pool's reporter read the attempt marker alone, and the two markers have different
// lifetimes — the attempt marker 90s, the unknown-outcome stamp 30 minutes — so they overlap: the last
// add before the give-up leaves an attempt marker live for up to ninety seconds past it. In that window
// ?probe=1 said 202 downloading while /play said 404 dead_link, which is the disagreement the reporter
// was added to remove, pointing the other way.
#[test]
fn add_in_flight_stops_believing_an_add_at_the_give_up() {
    let token = "giveup";
    let hash = "8".repeat(40);
    let cache = mem();
    let s = real_debrid(token, &cache, answer(200, "{}"));

    note_add_attempt(&*cache, DebridService::RealDebrid, token, &hash);
    assert_eq!(s.add_in_flight(&hash), Some(true), "a fresh add attempt should read as in flight");

    // Age the unknown-outcome stamp past the give-up, leaving the 90s attempt marker live beneath it.
    let stamp = unix_secs_ago(ADD_GIVE_UP + Duration::from_secs(60));
    cache.put(&unknown_outcome_key(DebridService::RealDebrid, token, &hash), &stamp, UNKNOWN_OUTCOME_TTL);

    assert_ne!(
        s.add_in_flight(&hash),
        Some(true),
        "past the give-up the probe still called the add live, while /play calls the release dead — the \
         client is told to keep waiting for something /play has stopped waiting for"
    );
}

// The pool asks EVERY store, not just the first.
//
// Every fixture for this reporter built a single-store pool, so the loop was never exercised: a version
// that answered false for any pool with more than one store passed the whole suite, and would have made
// the probe blind to in-flight adds on exactly the multi-account installs it matters for.
#[test]
#[ignore = "needs stores/torbox.rs (package 1)"]
fn store_pool_add_in_flight_asks_every_store() {
    todo!("port refusal_test.go:607 once package 1 lands")
}

// A release Real-Debrid already holds is READY on the probe route, not "not queued".
//
// The probe's readiness branch was gated on the cache check alone, and RD publishes no cache API — it
// answers all-false by design, saying "RD contributes no cache truth". So the branch was unreachable on
// an RD-only install and the route fell through to 404, while /play resolved the very same release from
// a cache lookup plus an info call and returned a 302. The client polls the probe to know when its
// download has landed; it never learned that it had.
#[test]
#[ignore = "needs handler.rs (package 4)"]
fn probe_a_torrent_real_debrid_already_holds_reads_as_ready() {
    todo!("port refusal_test.go:629 once package 4 lands")
}

// NOTE ON THIS TEST'S SCOPE: it pins premiumizeStore.AddInFlight at three marker ages and nothing else.
// It does NOT observe the agreement between the two routes, despite what an earlier version of this
// comment claimed — and that overclaim is why two real disagreements passed it: a queued transfer that
// directdl had since REFUSED, and one that had since COMPLETED. Its fixture answers
// success-with-no-content for every call, which is the one case where the marker alone is right. The
// route-level agreements are pinned by the two tests below it.
//
// Premiumize has a second way to be mid-fetch, and the probe could not see it either.
//
// directdl answering success with empty content means the transfer was queued: the store stamps
// pmQueuedKey and returns errAddInFlight, so /play says 202 downloading. That is not the add marker —
// settleAddAttempt clears the add marker as soon as the body is read — so a probe asking only about
// adds answered 404 for the whole ten minutes /play spends saying "coming".
//
// Both ends of the window are asserted. Past pendingGiveUp /play reports the release dead, so a probe
// still claiming 202 there would be the same defect pointing the other way.
#[test]
fn premiumize_add_in_flight_believes_a_queued_transfer_only_until_the_give_up() {
    let cases = [
        ("just queued", Duration::ZERO, true),
        ("still believable", Duration::from_secs(9 * 60), true),
        ("past the give-up", PENDING_GIVE_UP + Duration::from_secs(60), false),
    ];
    for (name, queued_ago, want_queued) in cases {
        let token = format!("pm-{name}");
        let hash = "9".repeat(40);
        let cache = mem();
        // Stamp the marker at the age under test, the way noteQueued writes it.
        cache.put(&pm_queued_key(&token, &hash), &unix_secs_ago(queued_ago), QUEUED_TTL);

        // Still nothing to serve.
        let s = premiumize(&token, &cache, answer(200, r#"{"status":"success","content":[]}"#));
        assert_eq!(
            s.add_in_flight(&hash),
            Some(want_queued),
            "{name}: the probe route reads this to decide between 202 downloading and 404 not_queued, and it \
             must change sides at the same moment /play does"
        );
    }
}

// HoldingServices asks EVERY store, and builds Real-Debrid's key the way /play writes it.
//
// Two gaps in one: the loop had no multi-store fixture, so stopping after the first store passed the
// whole suite; and the only test used a movie target, so dropping season/episode from the RD key — which
// would make the fix silently do nothing for series, most of the traffic — also passed.
#[test]
#[ignore = "needs stores/torbox.rs (package 1)"]
fn holding_services_asks_every_store_and_keys_episodes_correctly() {
    todo!("port refusal_test.go:721 once package 1 lands")
}

// EveryAddRefusedByScout means EVERY, and an empty pool refuses nothing.
//
// The quantifier was held by nothing: flipping it to ANY passed the entire suite, and shipped it would
// answer 503 scout_busy on a two-account install where one budget is spent and the other is healthy —
// while /play serves a 302 from the healthy account. That is a fresh disagreement of exactly the kind
// the branch was added to remove.
#[test]
#[ignore = "needs stores/torbox.rs (package 1), addbudget.rs (package 3)"]
fn every_add_refused_by_scout_needs_every_account_spent() {
    todo!("port refusal_test.go:760 once packages 1 and 3 land")
}

// A refusal on record outranks the queue marker, and NOT the other way round.
//
// premiumizeStore.AddInFlight checks the add marker first, then the refusal, then the queue marker.
// Hoisting the refusal check above the add marker passed the whole suite, and it inverts the precedence
// addStillBelievable's own comment calls the point: an add genuinely in flight would read as
// not-in-flight whenever a stale per-release refusal happened to exist.
#[test]
fn premiumize_add_in_flight_an_add_in_flight_outranks_a_stale_refusal() {
    let token = "pm-order";
    let hash = "c".repeat(40);
    let cache = mem();
    note_add_attempt(&*cache, DebridService::Premiumize, token, &hash); // an add of ours IS out
    // ...alongside an older per-release refusal
    record_refusal(
        &*cache,
        DebridService::Premiumize,
        token,
        &hash,
        &StoreError::DeadLink(DeadLinkError { reason: "premiumize directdl: error something earlier".into() }),
    );

    let s = premiumize(token, &cache, answer(200, "{}"));
    assert_eq!(
        s.add_in_flight(&hash),
        Some(true),
        "an add scout has out read as not-in-flight because a stale refusal exists — /play answers 202 from \
         that marker, so the probe must too"
    );
}

// A queued Premiumize transfer that directdl has SINCE refused must stop reading as "downloading".
//
// The test above pins the marker's boolean and drives only one route, which is how this escaped: it
// answers success-with-no-content for every call, and the disagreeing case is the one where directdl
// answers something else. Premiumize reports an unsupported magnet, an account at its limit and "not
// enough space" as HTTP 200 with status:error — none of those branches clears pmQueuedKey, so the
// marker alone kept the probe at 202 for the whole ten-minute window while /play had the verdict.
//
// Both routes are driven here, and the assertion is that they agree.
#[test]
#[ignore = "needs handler.rs (package 4)"]
fn probe_and_play_agree_when_a_queued_premiumize_transfer_is_refused() {
    todo!("port refusal_test.go:815 once package 4 lands")
}

// A Premiumize transfer that has COMPLETED must read as ready, not as still downloading.
//
// Nothing on the probe path could clear pmQueuedKey: settleQueuedTransfer is reached only from
// directdl's content branch, and Premiumize refused a NoAdd target before consulting anything, so the
// probe never called directdl and the marker went on saying "coming". The probe therefore never
// transitioned to ready for Premiumize at all — 202 until the marker aged past pendingGiveUp, then 404,
// while /play answered 302 with a playable link. Measured before the fix: probe 202, /play 302.
//
// For an already-queued transfer directdl is a READ — the charge is suppressed in exactly that case —
// so letting a read-only caller through buys nothing and is the only way to discover completion.
// Every realistic outcome of Premiumize's /cache/check, because the readiness enquiry runs only when a
// store NAMES itself a holder, and PM's cache check was for a while the only channel that could. The
// first version of this test asserted the cached-true row alone — the one branch that worked — so the
// other four kept answering 202 for a completed transfer and the suite stayed green.
#[test]
#[ignore = "needs handler.rs (package 4)"]
fn probe_and_play_agree_when_a_premiumize_transfer_completes() {
    todo!("port refusal_test.go:876 once package 4 lands")
}

// A read-only probe poll must not write Premiumize's ADD-path in-flight marker.
//
// The NoAdd gate was widened so the probe could see a queued transfer finish, and everything below that
// gate is add-path bookkeeping. A probe runs on the client's context under statusBudget and clients
// cancel aggressively, so a cut-off poll left a ninety-second marker behind — and /play then
// short-circuits on that marker WITHOUT calling directdl, answering 202 "downloading" for a release
// Premiumize already holds. Unplayable for the marker's life, with no Status API to clear it.
//
// A read-only route stranding a release by writing add-path memory is exactly the contract
// recordRefusalFor states, and the commit that widened this gate fixed that contract for Real-Debrid
// while missing the store it had just widened.
#[tokio::test]
async fn probe_a_read_only_poll_writes_no_premiumize_add_memory() {
    let token = "pm-readonly";
    let hash = "1".repeat(40);
    let cache = mem();
    cache.put(&pm_queued_key(token, &hash), &unix_secs_ago(Duration::ZERO), QUEUED_TTL);

    // The poll is cut off mid-call, as a focus change does.
    let s = premiumize(
        token,
        &cache,
        Arc::new(MockDoer(move |_r: reqwest::Request| -> Answer { transport_error() })),
    );
    let _ = s.resolve(&ResolveTarget { no_add: true, ..target(&hash) }).await;

    assert!(
        !add_outcome_unknown(&*cache, DebridService::Premiumize, token, &hash),
        "a read-only poll left an add-path in-flight marker — /play then answers 202 'downloading' without \
         calling directdl, for a release Premiumize already holds"
    );

    // The queue marker must survive a read-only poll: it is the only thing naming Premiumize a holder,
    // so clearing it from here makes the very next probe answer 404 for a transfer /play still serves.
    //
    // A SECOND store, because the branch that clears it is only reached when directdl answers with
    // content. The cancelled fixture above returns long before it, so asserting the marker there could
    // not fail for the reason it names — which is exactly the flaw the previous version of this test was
    // rewritten to remove, repeated one assertion over.
    let served = premiumize(
        token,
        &cache,
        answer(
            200,
            r#"{"status":"success","content":[{"path":"Movie.mkv","link":"https://pm.example/final.mkv","size":100}]}"#,
        ),
    );
    let link = served.resolve(&ResolveTarget { no_add: true, ..target(&hash) }).await;
    assert!(matches!(&link, Ok(l) if !l.is_empty()), "the read-only resolve should serve a completed transfer: {link:?}");
    assert!(
        already_queued(&*cache, token, &hash),
        "a read-only poll cleared the queue marker — the next poll names no holder and answers 404 not_queued \
         for a release /play resolves from the same marker"
    );
}

// A refusal Premiumize ANSWERS must be recorded even by a read-only poll, because for this store that
// key is what withdraws the queue marker's "coming" claim.
//
// This half was previously asserted with a fixture whose error was context.Canceled — which recordRefusal
// excludes by itself, so the assertion could not fail for the reason it named, and reverting the whole
// change passed the entire suite. The errors below are the ones Premiumize actually returns: "not enough
// space" and an invalid magnet arrive as HTTP 200 with status:error.
#[tokio::test]
async fn premiumize_a_read_only_poll_records_an_answered_refusal() {
    let cases = [
        ("out of space", r#"{"status":"error","message":"not enough space"}"#, 200),
        ("server error", "{}", 500),
        // A non-2xx that is NOT a refusal of the account (401/403/429/5xx are handled a branch up).
        // Reverting only this site reproduced the whole defect through a different status code while the
        // suite stayed green — the two rows above cover the other two sites.
        ("bad request", "{}", 400),
    ];
    for (name, body, code) in cases {
        let token = format!("pm-answered-{name}");
        let hash = "1".repeat(40);
        let cache = mem();
        cache.put(&pm_queued_key(&token, &hash), &unix_secs_ago(Duration::ZERO), QUEUED_TTL);

        let s = premiumize(&token, &cache, answer(code, body));
        let _ = s.resolve(&ResolveTarget { no_add: true, ..target(&hash) }).await;

        assert!(
            backed_off(&*cache, DebridService::Premiumize, &token, &hash).is_some(),
            "{name}: an answered refusal was not recorded by a read-only poll — nothing then withdraws the queue \
             marker's claim, so the probe reports 202 downloading on every poll of a transfer Premiumize has \
             refused, re-asking directdl each time"
        );
        assert_ne!(
            s.add_in_flight(&hash),
            Some(true),
            "{name}: the queue marker still reads as in-flight after Premiumize refused the transfer — the \
             client sits on a spinner it cannot fall through"
        );
    }
}

// A rejected KEY outranks an add in flight, on the probe route as in every store.
//
// All three stores check accountBackedOff above addInFlight and say why — a dead key is not a wait. The
// probe consulted its in-flight reporter first, so a live marker pre-empted the rejected key and the two
// routes disagreed: /play 503 naming the debrid, ?probe=1 202 downloading, for the same release at the
// same instant, for up to ninety seconds per outstanding add.
#[test]
#[ignore = "needs handler.rs (package 4)"]
fn probe_and_play_agree_when_the_account_key_is_rejected() {
    todo!("port refusal_test.go:1043 once package 4 lands")
}

// A dead key on ONE account must not outrank a live add on ANOTHER.
//
// Two rules meet here. Per store, a rejected key beats that store's own add in flight — every Resolve
// says so. Across stores, an add in flight anywhere beats a refusal elsewhere — ResolvePreferring
// returns `coming` before `refused` precisely so store order cannot decide the verdict. Applying the
// first rule at POOL level broke the second: with TorBox's key expired and Real-Debrid fetching, the
// probe answered 503 naming torbox while /play answered 202 downloading — naming a store that was not
// the one fetching, on the URL the client polls for its progress bar.
//
// The sibling single-account case is pinned by TestProbeAndPlay_agreeWhenTheAccountKeyIsRejected, which
// configures one store and so cannot see this. Both are needed.
#[test]
#[ignore = "needs handler.rs (package 4)"]
fn probe_and_play_agree_when_one_accounts_key_is_dead_and_another_is_fetching() {
    todo!("port refusal_test.go:1092 once package 4 lands")
}

// TorBox must rank its own add in flight above a refusal recorded in the meantime, as RD and PM do.
//
// The per-release backoff was hoisted above the warm fast path so a binge could not bypass it, and that
// hoist put it above the in-flight marker for TorBox alone — reversing the order the gate further down
// states in as many words. Both markers can be live at once without planting anything: /play has no
// singleflight and Stremio races requests, so one poll's createtorrent can be answered 429 (settling its
// attempt and recording the refusal) while another's connection resets, leaving its attempt marker.
#[test]
#[ignore = "needs stores/torbox.rs (package 1)"]
fn torbox_an_add_in_flight_outranks_a_refusal_recorded_meanwhile() {
    todo!("port refusal_test.go:1139 once package 1 lands")
}

// /play must not buy a release another configured account already holds.
//
// Its resolve ordering came only from the cache check, and Real-Debrid can never appear there: its
// CacheCheck answers all-false by design, so HeldBy never names it. A release RD already held was
// therefore invisible to the ordering, configured order won, and TorBox — first in the list — was asked
// to resolve, which means createtorrent: one of the fifty per hour spent, a duplicate torrent left on
// TorBox, and the probe (which consults Status first) then reporting a download the viewer never needed
// for a file the other account could have served instantly.
//
// HoldingServices is the free cache read that answers this, added for the probe and not applied to the
// route that pays for being wrong.
#[test]
#[ignore = "needs handler.rs (package 4), stores/torbox.rs (package 1), addbudget.rs (package 3)"]
fn play_does_not_buy_a_release_another_account_already_holds() {
    todo!("port refusal_test.go:1187 once packages 1, 3 and 4 land")
}

// A season pack Premiumize has already served must not read as "not queued" for its next episode.
//
// Premiumize has no torrent id to remember, so the queue marker was the only thing naming it a holder —
// and /play settles that marker on success. The first play of a pack therefore blinded the probe to the
// whole rest of it: every later poll answered 404 while /play kept serving 302s from the same transfer.
// Measured across three polls of episode 2 after playing episode 1.
//
// The probe is driven AFTER the play here, deliberately. The sibling three-poll test polls before any
// play and so cannot see a marker that /play removes.
#[test]
#[ignore = "needs handler.rs (package 4)"]
fn probe_and_play_agree_on_a_later_episode_after_premiumize_served_the_first() {
    todo!("port refusal_test.go:1262 once package 4 lands")
}

// A Premiumize release that has been EVICTED must not let read-only polls buy it back, uncharged.
//
// The held mark is a six-hour claim about the account, and a claim can be wrong. Once Premiumize evicts
// the release the mark still reads "held", which lets a read-only poll through the NoAdd gate to
// directdl — and directdl IS the purchase. Letting that mark also suppress the charge made every poll a
// real, unbilled add with no queue marker to bound it:
//
//	before: 30 polls -> 2 directdl, allowance 50->49, marker set (10-minute give-up)
//	after:  30 polls -> 32 directdl, allowance 50->50, no marker, no give-up
//
// Two guards are asserted: the charge is taken (so the fifty an hour sees it), and the mark is dropped
// (so the next poll does not walk the same path again).
#[test]
#[ignore = "needs addbudget.rs (package 3)"]
fn premiumize_an_evicted_release_is_not_bought_back_by_read_only_polls() {
    todo!("port refusal_test.go:1325 once package 3 lands")
}

// A Real-Debrid release that is DOWNLOADING must not read as dead on either route.
//
// This was the plainest failure in the route and it survived every earlier round, because both routes
// agreed and every comparison between them looked fine. An uncached release on an RD-only install is
// added, starts downloading, and has no links until it finishes — so the resolve returned a dead link,
// which is neither errAddInFlight nor a StoreUnavailableError, and RD reported no status to rescue it.
// The add is already spent, so the viewer pays for the fetch and is told the release is dead throughout.
//
// The terminal statuses are asserted alongside, because the guard this replaces existed to stop scout
// turning a dead torrent into a promise — that must still hold.
#[test]
#[ignore = "needs handler.rs (package 4)"]
fn real_debrid_a_downloading_release_is_not_reported_dead() {
    todo!("port refusal_test.go:1372 once package 4 lands")
}

// A refusal SCOUT made must not read as "nothing is queued".
//
// recordRefusal excludes errScoutSide on purpose — scout's own ceiling is not the debrid declining, and
// filing it as one blames a healthy service — so the backoff memory the probe reads stays empty and the
// route fell through all three of its "could not ask" guards to 404. The budget is per account and
// rolling, so once the ceiling is reached every uncached release on that account answers that way for
// the rest of the hour, and 404 is the answer that makes a client blacklist a release.
#[test]
#[ignore = "needs handler.rs (package 4), stores/torbox.rs (package 1), addbudget.rs (package 3)"]
fn probe_and_play_agree_when_scouts_own_budget_is_spent() {
    todo!("port refusal_test.go:1445 once packages 1, 3 and 4 land")
}

// ?probe=1 and /play must give the SAME answer about an add scout has out.
//
// They are one vocabulary by design — the doc on handleProbe says so — and this is the one fact in the
// package that is about us rather than about a service. /play reads it through errAddInFlight, raised
// from inside the resolve. The probe route never resolves, and all three stores answer a NoAdd target
// with errWouldAdd BEFORE they consult the marker, so it had no way to learn the state at all and fell
// through to 404 "not_queued" — for the same release, at the same instant, that /play was answering 202
// "downloading".
//
// A 404 there is the single failure that route exists to prevent: it is the URL the client polls to
// draw its progress bar, and it reads a 404 as a release nobody has. The window is not brief for two of
// the three services — Real-Debrid and Premiumize have no Status API to rediscover the torrent with, so
// nothing clears the marker before addAttemptTTL expires 90 seconds later.
//
// Asserted as an agreement between the two routes rather than as a status code, because the defect was
// the DISAGREEMENT; pinning one route's number invites the other to drift again.
//
// This fixture reaches the disagreement one branch earlier than the reported 404: with every upstream
// answering an empty body the probe stops at 503 (`status_unavailable` for TorBox, which has a Status
// API, `cache_check_unavailable` for the two that do not) while /play answers 202. Removing the guard
// reproduces that, which is the same defect at a different exit — the route not knowing a fact it
// holds. Reaching the literal 404 needs a healthy cache check AND a store that answers "not queued",
// which is a fuller fixture than this invariant needs.
#[test]
#[ignore = "needs handler.rs (package 4)"]
fn probe_and_play_agree_that_an_in_flight_add_is_downloading() {
    todo!("port refusal_test.go:1513 once package 4 lands")
}
