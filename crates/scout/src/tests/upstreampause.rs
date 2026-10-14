//! Go: `internal/scout/upstreampause_test.go` (180 lines).
//!
//! The Go `http.RoundTripper` double is a `Doer` here. Every pause test is live. The add-budget test belongs to
//! package 3, so it is ignored with its body compiled out.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use reqwest::header::{HeaderMap, HeaderName, HeaderValue};

use crate::scrape::{DoError, Doer};
use crate::upstreampause::{
    exhausted_for, pause_on_refusal, retry_after, MAX_REFUSAL_PAUSE, MAX_RETRY_AFTER, REFUSAL_PAUSE,
};

type Answer = Box<dyn Fn(&reqwest::Request) -> reqwest::Response + Send + Sync>;

/// Go `countingTransport`: counts every request it is asked, and answers with `answer`.
struct CountingDoer {
    calls: AtomicUsize,
    answer: Mutex<Answer>,
}

impl CountingDoer {
    fn new(answer: Answer) -> CountingDoer {
        CountingDoer { calls: AtomicUsize::new(0), answer: Mutex::new(answer) }
    }

    fn calls(&self) -> usize {
        self.calls.load(Ordering::SeqCst)
    }

    fn set_answer(&self, answer: Answer) {
        *self.answer.lock().unwrap() = answer;
    }
}

#[async_trait]
impl Doer for CountingDoer {
    async fn do_request(&self, req: reqwest::Request) -> Result<reqwest::Response, DoError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        let answer = self.answer.lock().unwrap();
        Ok(answer(&req))
    }
}

/// Lets a test keep a handle on the counting upstream after wrapping it.
#[async_trait]
impl Doer for Arc<CountingDoer> {
    async fn do_request(&self, req: reqwest::Request) -> Result<reqwest::Response, DoError> {
        self.as_ref().do_request(req).await
    }
}

/// Go `answering`: a response of `status` carrying `headers`.
fn answering(status: u16, headers: &[(&str, &str)]) -> Answer {
    let headers: Vec<(String, String)> =
        headers.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect();
    Box::new(move |_req: &reqwest::Request| {
        let mut b = http::Response::builder().status(status);
        for (k, v) in &headers {
            b = b.header(k.as_str(), v.as_str());
        }
        reqwest::Response::from(b.body(String::new()).expect("valid response"))
    })
}

/// Go `get`: one request through a `Doer`, the way a caller would make it.
async fn get(doer: &(impl Doer + ?Sized), url: &str, authorization: Option<&str>) -> reqwest::Response {
    let mut b = reqwest::Client::new().get(url);
    if let Some(a) = authorization {
        b = b.header("authorization", a);
    }
    doer.do_request(b.build().expect("request")).await.expect("transport")
}

fn clock_fn(clock: &Arc<Mutex<SystemTime>>) -> Box<dyn Fn() -> SystemTime + Send + Sync> {
    let clock = Arc::clone(clock);
    Box::new(move || *clock.lock().unwrap())
}

fn headers(pairs: &[(&'static str, &str)]) -> HeaderMap {
    let mut h = HeaderMap::new();
    for (k, v) in pairs {
        h.append(HeaderName::from_static(k), HeaderValue::from_str(v).expect("valid header value"));
    }
    h
}

/// 2026-09-14T12:00:00Z.
fn sept_14_noon() -> SystemTime {
    UNIX_EPOCH + Duration::from_secs(1_789_387_200)
}

// A 429 with Retry-After closes that upstream to every later request for as long as it asked, answered locally
// with the time left; an answer after the pause ends it.
#[tokio::test]
async fn pause_on_refusal_waits_out_a_retry_after_for_every_request() {
    let next = Arc::new(CountingDoer::new(answering(429, &[("retry-after", "120")])));
    let clock = Arc::new(Mutex::new(SystemTime::now()));
    let mut rt = pause_on_refusal(Arc::clone(&next));
    rt.now = clock_fn(&clock);

    assert_eq!(
        get(&rt, "https://torrentio.example/stream/movie/tt1.json", None).await.status(),
        429,
        "first answer"
    );
    let paused = get(&rt, "https://torrentio.example/stream/movie/tt2.json", None).await;
    assert_eq!(next.calls(), 1, "asked the upstream {} times inside its Retry-After", next.calls());
    assert!(
        paused.status() == 429
            && paused.headers()["retry-after"] == "120"
            && paused.headers()["x-scout-paused"] == "1",
        "paused answer = {} {:?}",
        paused.status(),
        paused.headers()
    );
    get(&rt, "https://other.example/x", None).await;
    assert_eq!(next.calls(), 2, "a pause on one host held back another");

    *clock.lock().unwrap() += Duration::from_secs(121);
    next.set_answer(answering(200, &[]));
    let resp = get(&rt, "https://torrentio.example/stream/movie/tt3.json", None).await;
    assert!(
        resp.status() == 200 && next.calls() == 3,
        "after the pause: {}, {} calls",
        resp.status(),
        next.calls()
    );
    assert_eq!(rt.left("torrentio.example"), Duration::ZERO, "an answer did not end the pause");
}

// One account's rate limit pauses that account, not every account on the same debrid API.
#[tokio::test]
async fn pause_on_refusal_pauses_the_account_not_the_api() {
    let next = Arc::new(CountingDoer::new(answering(429, &[])));
    let rt = pause_on_refusal(Arc::clone(&next));
    let checkcached = "https://api.torbox.example/v1/api/torrents/checkcached";
    get(&rt, checkcached, Some("Bearer a")).await;
    get(&rt, checkcached, Some("Bearer a")).await;
    assert_eq!(next.calls(), 1, "the throttled account was asked again");
    get(&rt, checkcached, Some("Bearer b")).await;
    get(&rt, "https://premiumize.example/api/cache/check?apikey=c", None).await;
    assert_eq!(next.calls(), 3, "another account was held back");
}

// Without a Retry-After the pause starts short and doubles; a bare 503 is a shed request, not a limit, and pauses
// nothing.
#[tokio::test]
async fn pause_on_refusal_doubles_without_retry_after_and_leaves_a_bare_503_alone() {
    let clock = Arc::new(Mutex::new(SystemTime::now()));
    let mut rt = pause_on_refusal(Arc::new(CountingDoer::new(answering(200, &[]))));
    rt.now = clock_fn(&clock);
    let first = rt.refused("h", Duration::ZERO, false);
    *clock.lock().unwrap() += first + Duration::from_secs(1);
    let second = rt.refused("h", Duration::ZERO, false);
    assert!(
        first >= REFUSAL_PAUSE
            && first <= REFUSAL_PAUSE * 5 / 4
            && second >= 2 * REFUSAL_PAUSE
            && second <= 2 * REFUSAL_PAUSE * 5 / 4,
        "pauses {first:?} then {second:?}"
    );
    for _ in 0..20 {
        let pause = rt.refused("h", Duration::ZERO, false);
        *clock.lock().unwrap() += pause + Duration::from_secs(1);
    }
    let capped = rt.refused("h", Duration::ZERO, false);
    assert!(capped <= MAX_REFUSAL_PAUSE * 5 / 4, "pause grew past its ceiling: {capped:?}");

    let next = Arc::new(CountingDoer::new(answering(503, &[])));
    let shed = pause_on_refusal(Arc::clone(&next));
    get(&shed, "https://torrentio.example/a", None).await;
    get(&shed, "https://torrentio.example/b", None).await;
    assert_eq!(next.calls(), 2, "a 503 without Retry-After paused the host");
}

// Refusals that land while a pause is already running (a burst of requests sent before the first 429 came back) are
// one refusal, not N: the next window after the pause is the second, not 2^N.
#[test]
fn a_burst_of_refusals_inside_one_pause_counts_once() {
    let clock = Arc::new(Mutex::new(SystemTime::now()));
    let mut rt = pause_on_refusal(Arc::new(CountingDoer::new(answering(200, &[]))));
    rt.now = clock_fn(&clock);
    let first = rt.refused("h", Duration::ZERO, false);
    for _ in 0..7 {
        rt.refused("h", Duration::ZERO, false);
    }
    assert!(first <= REFUSAL_PAUSE * 5 / 4);
    // Past the jittered end of the pause the burst extended.
    *clock.lock().unwrap() += REFUSAL_PAUSE * 5 / 4 + Duration::from_secs(1);
    let second = rt.refused("h", Duration::ZERO, false);
    assert!(
        second >= 2 * REFUSAL_PAUSE && second <= 2 * REFUSAL_PAUSE * 5 / 4,
        "a burst of 8 escalated the next window to {second:?}"
    );
}

#[test]
fn retry_after_reads_seconds_and_dates_and_caps_them() {
    let now = sept_14_noon();
    let cases: [(&str, Option<Duration>); 4] = [
        ("30", Some(Duration::from_secs(30))),
        ("999999", Some(MAX_RETRY_AFTER)),
        ("Mon, 14 Sep 2026 12:02:00 GMT", Some(Duration::from_secs(120))),
        ("Mon, 14 Sep 2026 11:00:00 GMT", Some(Duration::ZERO)),
    ];
    for (value, want) in cases {
        let got = retry_after(&headers(&[("retry-after", value)]), now);
        assert_eq!(got, want, "Retry-After {value:?}");
    }
    for value in ["", "soon", "-5"] {
        assert!(
            retry_after(&headers(&[("retry-after", value)]), now).is_none(),
            "Retry-After {value:?} read as a wait"
        );
    }
}

// An answer that says the allowance is spent pauses the upstream until its reset, before any 429.
#[tokio::test]
async fn pause_on_refusal_waits_out_an_allowance_an_answer_says_is_spent() {
    let now = sept_14_noon();
    let legacy_reset =
        (now + Duration::from_secs(90)).duration_since(UNIX_EPOCH).expect("epoch").as_secs().to_string();
    let spent: [(&str, HeaderMap); 3] = [
        ("draft", headers(&[("ratelimit", r#""hourly";r=0;t=90"#)])),
        ("older", headers(&[("ratelimit-remaining", "0"), ("ratelimit-reset", "90")])),
        ("legacy", headers(&[("x-ratelimit-remaining", "0"), ("x-ratelimit-reset", legacy_reset.as_str())])),
    ];
    for (name, header) in spent {
        assert_eq!(exhausted_for(&header, now), (Duration::from_secs(90), true), "{name}");
    }
    let not_spent: [(&str, HeaderMap); 2] = [
        ("requests left", headers(&[("ratelimit", r#""hourly";r=4;t=90"#), ("x-ratelimit-remaining", "4")])),
        ("nothing said", HeaderMap::new()),
    ];
    for (name, header) in not_spent {
        assert!(!exhausted_for(&header, now).1, "{name} read as spent");
    }

    let next = Arc::new(CountingDoer::new(answering(
        200,
        &[("x-ratelimit-remaining", "0"), ("x-ratelimit-reset", "60")],
    )));
    let rt = pause_on_refusal(Arc::clone(&next));
    get(&rt, "https://api.torbox.example/v1/api/user/me", None).await;
    let resp = get(&rt, "https://api.torbox.example/v1/api/user/me", None).await;
    assert!(
        resp.status() == 429 && next.calls() == 1,
        "asked a spent upstream again: {} {} calls",
        resp.status(),
        next.calls()
    );
}

// The add budget frees a slot when its oldest charge drains. Package 3 (addbudget.rs) owns the budget.
#[test]
fn add_budget_frees_in_when_its_oldest_charge_drains() {
    use crate::addbudget::AddBudget;
    let mut b = AddBudget::new(Duration::from_secs(3600), 2);
    let clock = Arc::new(Mutex::new(sept_14_noon()));
    b.now = clock_fn(&clock);
    b.take("acct", false);
    assert_eq!(
        b.frees_in("acct", false),
        Duration::ZERO,
        "an account with allowance left must not be told to wait"
    );
    *clock.lock().unwrap() += Duration::from_secs(600);
    b.take("acct", false);
    assert_eq!(
        b.frees_in("acct", false),
        Duration::from_secs(50 * 60),
        "freesIn: want the first charge's 50 minutes left"
    );
}

// Go's ProbeClient is the same client as the stores' (cmd/den-scout/main.go wraps its transport), and Go has one
// process-wide pause memory. A probe, link check or token validation must honour a pause the stores learned, and
// a second handler built over the same client (hearth serves one per origin) must share it.
#[tokio::test]
async fn the_probe_doer_shares_the_pause_with_every_handler_built_over_one_client() {
    use crate::cache::MemoryCache;
    use crate::deps::{build_deps_shared, settings_from_env};

    let next = Arc::new(CountingDoer::new(answering(429, &[("retry-after", "120")])));
    let http: Arc<dyn Doer> = Arc::new(pause_on_refusal(Arc::clone(&next)));
    let settings = settings_from_env(|_| String::new());
    let first = build_deps_shared(&settings, http.clone(), Arc::new(MemoryCache::new(1 << 20)));
    let second = build_deps_shared(&settings, http, Arc::new(MemoryCache::new(1 << 20)));

    let probe = |d: &crate::deps::Deps| d.probe_doer.clone().expect("the probe client is the shared one");
    let url = "https://cdn.example/dld/abc";
    let resp = get(&*probe(&first), url, None).await;
    assert!(resp.status() == 429 && next.calls() == 1, "the upstream was not asked once: {}", next.calls());

    let resp = get(&*probe(&second), url, None).await;
    assert!(
        resp.status() == 429 && next.calls() == 1,
        "the second handler's probe asked a paused upstream ({} calls)",
        next.calls()
    );
}
