//! `/den/<config>/wanted` end to end, through `handle`, with Scout and the debrid faked behind `wanted::Finder`: what
//! a client's list asks for is searched on den-core's schedule and started once a release is listed.

use std::collections::HashMap;
use std::sync::Mutex;

use base64::Engine;
use serde_json::json;

use super::*;
use crate::wanted::{self, Finder, Wanted};

const IID: &str = "AAECAwQFBgcICQoLDA0ODw";
const SCOUT: &str = r#""scout":{"debrid":[{"service":"torbox","token":"tb"}]}"#;
const MINUTE: i64 = 60_000;
const DAY: i64 = 86_400_000;

fn plain(json: &str) -> String {
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(json)
}

fn config() -> String {
    plain(&format!(r#"{{"iid":"{IID}","ep":0,{SCOUT}}}"#))
}

/// Scout's lists by stream id, and every list asked for and add made.
#[derive(Default)]
struct Fake {
    listings: Mutex<HashMap<String, Value>>,
    asked: Mutex<Vec<String>>,
    adds: Mutex<Vec<String>>,
}

struct Shared(Arc<Fake>);

impl Finder for Shared {
    fn streams<'a>(
        &'a self,
        _: &'a Hearth,
        config: &'a str,
        id: &'a str,
        _: bool,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Option<Value>> + Send + 'a>> {
        Box::pin(async move {
            assert!(!config.is_empty(), "searched without the install's config");
            self.0.asked.lock().unwrap().push(id.to_owned());
            Some(self.0.listings.lock().unwrap().get(id).cloned().unwrap_or_else(empty))
        })
    }

    fn add<'a>(
        &'a self,
        _: &'a Hearth,
        url: &'a str,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = StatusCode> + Send + 'a>> {
        Box::pin(async move {
            self.0.adds.lock().unwrap().push(url.to_owned());
            StatusCode::ACCEPTED
        })
    }
}

impl Fake {
    fn list(&self, id: &str, answer: Value) {
        self.listings.lock().unwrap().insert(id.to_owned(), answer);
    }
    fn asked(&self) -> Vec<String> {
        self.asked.lock().unwrap().clone()
    }
    fn adds(&self) -> Vec<String> {
        self.adds.lock().unwrap().clone()
    }
}

/// Every source answered and none lists anything.
fn empty() -> Value {
    json!({"streams": [], "den": {"answerKind": "empty", "coverage": {"sources": [{"outcome": "answered"}]}}})
}

/// The season pack once it is seeded, as Scout lists it for S02E03.
fn season_pack() -> Value {
    json!({
        "streams": [{
            "name": "Den",
            "title": "Rebecka.Martinsson.S02.1080p.AMZN.WEB-DL.DD+2.0.H.264-playWEB",
            "url": "http://node.test/den/p/TICKET3",
            "behaviorHints": {"filename": "Rebecka.Martinsson.S02E03.1080p.AMZN.WEB-DL.DD+2.0.H.264-playWEB.mkv"},
            "attributes": {"resolution": "1080p", "cached": false, "seeders": 4},
        }],
        "den": {"answerKind": "live", "coverage": {"sources": [{"outcome": "answered"}]}},
    })
}

fn node(den: den::Den) -> (Arc<Hearth>, Arc<Fake>) {
    let fake = Arc::new(Fake::default());
    let mut hearth = hearth_den_with_options(Limits::default(), den, Options::default());
    Arc::get_mut(&mut hearth).unwrap().wanted = Wanted::new(Box::new(Shared(fake.clone())), None);
    (hearth, fake)
}

fn plain_node() -> (Arc<Hearth>, Arc<Fake>) {
    node(den::Den::new(None, false, Default::default(), Some("http://node.test/den".into()), "none"))
}

/// The wanted row for Rebecka Martinsson S02E`episode`, as a client describes it.
fn episode(episode: u32, since: i64) -> Value {
    json!({"key": format!("tv:70742:2:{episode}"), "imdb": "tt6189176", "season": 2, "episode": episode,
           "tried": [], "retries": 0, "since": since})
}

async fn post(hearth: &Arc<Hearth>, config: &str, body: Value) -> (StatusCode, Value) {
    let req = Request::post(format!("/den/{config}/wanted"))
        .header("content-type", "application/json")
        .body(http_body_util::Full::new(Bytes::from(body.to_string())))
        .unwrap();
    let resp = handle(hearth.clone(), None, false, req).await;
    (resp.status(), json_of(resp).await)
}

async fn read(hearth: &Arc<Hearth>, config: &str) -> (StatusCode, Value) {
    let resp = send(hearth, &format!("/den/{config}/wanted")).await;
    (resp.status(), json_of(resp).await)
}

#[tokio::test]
async fn a_wanted_episode_starts_by_itself_once_a_release_is_listed() {
    let (hearth, fake) = plain_node();
    let t0 = wanted::now_ms();
    let (status, answer) = post(&hearth, &config(), json!({"items": [episode(3, t0)]})).await;
    assert_eq!(status, StatusCode::OK, "{answer}");
    assert_eq!(answer["items"][0]["state"], "searching");
    // den-core's first wait: 15 minutes, spread to 12–18.
    let next = answer["items"][0]["nextAt"].as_i64().unwrap();
    assert!((t0 + 12 * MINUTE..=t0 + 18 * MINUTE).contains(&next), "{next} vs {t0}");

    // Not before its first search is due.
    wanted::tick(&hearth, t0 + 5 * MINUTE).await;
    assert!(fake.asked().is_empty());

    // Due: nothing listed yet, so the next search waits longer and nothing is added.
    wanted::tick(&hearth, t0 + 20 * MINUTE).await;
    assert_eq!(fake.asked(), ["tt6189176:2:3"]);
    assert!(fake.adds().is_empty());
    let (_, answer) = read(&hearth, &config()).await;
    assert_eq!(answer["items"][0]["state"], "searching");
    assert_eq!(answer["items"][0]["retries"], 1);
    wanted::tick(&hearth, t0 + 21 * MINUTE).await;
    assert_eq!(fake.asked().len(), 1, "searched again before the backoff");

    // The pack gets seeded. The next search (24–36 minutes on) starts it, with no client open.
    fake.list("tt6189176:2:3", season_pack());
    wanted::tick(&hearth, t0 + 40 * MINUTE).await;
    assert_eq!(fake.asked().len(), 1, "searched again before the backoff");
    wanted::tick(&hearth, t0 + 60 * MINUTE).await;
    assert_eq!(fake.asked().len(), 2);
    assert_eq!(fake.adds(), ["http://node.test/den/p/TICKET3"]);

    let (_, answer) = read(&hearth, &config()).await;
    let item = &answer["items"][0];
    assert_eq!(item["state"], "started", "{answer}");
    assert_eq!(item["key"], "tv:70742:2:3");
    assert_eq!(item["identity"], "rebecka.martinsson.s02e03.1080p.amzn.web-dl.dd+2.0.h.264-playweb.mkv");
    assert_eq!(item["startedAt"], t0 + 60 * MINUTE);
    assert_eq!(item["candidates"], 1);
    assert_eq!(item["stream"]["url"], "http://node.test/den/p/TICKET3");

    // Started is final: no further search or add for it.
    wanted::tick(&hearth, t0 + 10 * DAY).await;
    assert_eq!((fake.asked().len(), fake.adds().len()), (2, 1));

    // The client adopts it and sends its list without it; an empty list clears the install.
    let (status, answer) = post(&hearth, &config(), json!({"items": []})).await;
    assert_eq!((status, answer), (StatusCode::OK, json!({"items": []})));
    assert_eq!(read(&hearth, &config()).await.1, json!({"items": []}));
}

#[tokio::test]
async fn nothing_is_searched_before_the_episode_airs() {
    let (hearth, fake) = plain_node();
    let t0 = wanted::now_ms();
    let airs = t0 + 3 * DAY;
    fake.list("tt6189176:2:5", season_pack());
    let mut item = episode(5, t0);
    item["airDate"] = json!(airs);
    let (_, answer) = post(&hearth, &config(), json!({"items": [item]})).await;
    assert_eq!(answer["items"][0]["nextAt"], airs);

    for at in [t0 + 20 * MINUTE, t0 + DAY, airs - MINUTE] {
        wanted::tick(&hearth, at).await;
    }
    assert!(fake.asked().is_empty(), "searched before it aired");
    wanted::tick(&hearth, airs).await;
    assert_eq!(fake.asked(), ["tt6189176:2:5"]);
    assert_eq!(read(&hearth, &config()).await.1["items"][0]["state"], "started");
}

#[tokio::test]
async fn an_answer_that_settles_nothing_is_asked_again_shortly_and_not_counted() {
    let (hearth, fake) = plain_node();
    let t0 = wanted::now_ms();
    // One source didn't answer: an empty list here says nothing about what exists.
    fake.list(
        "tt6189176:2:4",
        json!({"streams": [], "den": {"answerKind": "unknown", "coverage": {"sources": [{"outcome": "timeout"}]}}}),
    );
    post(&hearth, &config(), json!({"items": [episode(4, t0)]})).await;
    wanted::tick(&hearth, t0 + 20 * MINUTE).await;
    assert_eq!(fake.asked().len(), 1);
    assert_eq!(read(&hearth, &config()).await.1["items"][0]["retries"], 0);
    wanted::tick(&hearth, t0 + 24 * MINUTE).await;
    assert_eq!(fake.asked().len(), 1, "asked again inside the five minutes");
    wanted::tick(&hearth, t0 + 25 * MINUTE).await;
    assert_eq!(fake.asked().len(), 2);

    // An unseeded release is no release: nothing is added for it.
    let mut unseeded = season_pack();
    unseeded["streams"][0]["attributes"]["seeders"] = json!(0);
    fake.list("tt6189176:2:4", unseeded);
    wanted::tick(&hearth, t0 + 31 * MINUTE).await;
    assert_eq!(fake.asked().len(), 3);
    assert!(fake.adds().is_empty());
    assert_eq!(read(&hearth, &config()).await.1["items"][0]["retries"], 1);
}

#[tokio::test]
async fn a_list_sent_again_keeps_what_hearth_learned_until_the_row_moves_on() {
    let (hearth, fake) = plain_node();
    let t0 = wanted::now_ms();
    post(&hearth, &config(), json!({"items": [episode(3, t0)]})).await;
    wanted::tick(&hearth, t0 + 20 * MINUTE).await;
    // The same row, sent again: Hearth's own count stands.
    let (_, answer) = post(&hearth, &config(), json!({"items": [episode(3, t0)]})).await;
    assert_eq!(answer["items"][0]["retries"], 1);

    // A start the client then tried and gave up on is searched for again.
    fake.list("tt6189176:2:3", season_pack());
    wanted::tick(&hearth, t0 + 60 * MINUTE).await;
    assert_eq!(read(&hearth, &config()).await.1["items"][0]["state"], "started");
    let mut gave_up = episode(3, t0 + 90 * MINUTE);
    gave_up["tried"] = json!(["rebecka.martinsson.s02e03.1080p.amzn.web-dl.dd+2.0.h.264-playweb.mkv"]);
    gave_up["retries"] = json!(2);
    let (_, answer) = post(&hearth, &config(), json!({"items": [gave_up]})).await;
    assert_eq!(answer["items"][0]["state"], "searching");
    assert_eq!(answer["items"][0]["retries"], 2);
}

#[tokio::test]
async fn the_lists_and_the_search_rate_are_bounded() {
    let (hearth, fake) = plain_node();
    let t0 = wanted::now_ms();
    let many = |n: u32| (1..=n).map(|e| episode(e, t0)).collect::<Vec<_>>();
    let (status, answer) = post(&hearth, &config(), json!({ "items": many(201) })).await;
    assert_eq!((status, &answer["error"]), (StatusCode::PAYLOAD_TOO_LARGE, &json!("too_many_items")));
    let (status, answer) = post(&hearth, &config(), json!({ "items": many(200) })).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(answer["items"].as_array().unwrap().len(), 200);

    // Every item due at once: one tick asks Scout a few times, not two hundred.
    wanted::tick(&hearth, t0 + 20 * MINUTE).await;
    assert_eq!(fake.asked().len(), wanted::SEARCHES_PER_TICK);

    // Installs: a bounded number held, each its own config.
    for n in 1..wanted::MAX_INSTALLS {
        let other = plain(&format!(r#"{{"scout":{{"debrid":[{{"service":"torbox","token":"tb{n}"}}]}}}}"#));
        assert_eq!(post(&hearth, &other, json!({"items": [episode(1, t0)]})).await.0, StatusCode::OK, "{n}");
    }
    let one_more = plain(r#"{"scout":{"debrid":[{"service":"torbox","token":"tb-more"}]}}"#);
    let (status, answer) = post(&hearth, &one_more, json!({"items": [episode(1, t0)]})).await;
    assert_eq!((status, &answer["error"]), (StatusCode::SERVICE_UNAVAILABLE, &json!("wanted_full")));
}

#[tokio::test]
async fn den_core_decides_and_never_adds_a_release_tried_or_in_flight_again() {
    let (hearth, fake) = plain_node();
    let t0 = wanted::now_ms();
    let identity = "rebecka.martinsson.s02e03.1080p.amzn.web-dl.dd+2.0.h.264-playweb.mkv";
    fake.list("tt6189176:2:3", season_pack());
    fake.list("tt6189176:2:4", season_pack());
    let mut fetching = episode(3, t0);
    fetching["inFlight"] = json!([identity]);
    fetching["queuedAt"] = json!(t0 - DAY);
    let mut tried = episode(4, t0);
    tried["tried"] = json!([identity]);
    let (status, _) = post(&hearth, &config(), json!({"items": [fetching, tried]})).await;
    assert_eq!(status, StatusCode::OK);
    wanted::tick(&hearth, t0 + 20 * MINUTE).await;
    assert_eq!(fake.asked().len(), 2);
    assert!(fake.adds().is_empty(), "added a release the row already has");
    let (_, answer) = read(&hearth, &config()).await;
    assert!(answer["items"]
        .as_array()
        .unwrap()
        .iter()
        .all(|i| i["state"] == "searching" && i["retries"] == 1));
}

#[tokio::test]
async fn an_install_untouched_for_thirty_days_is_forgotten() {
    let (hearth, fake) = plain_node();
    let t0 = wanted::now_ms();
    post(&hearth, &config(), json!({"items": [episode(3, t0)]})).await;
    wanted::tick(&hearth, t0 + wanted::EXPIRY_MS + MINUTE).await;
    assert!(fake.asked().is_empty());
    assert_eq!(read(&hearth, &config()).await.1, json!({"items": []}));
}

#[tokio::test]
async fn the_route_needs_an_admitted_install_that_has_scout() {
    let revoked = den::Den::new(None, false, den_seal::Revocation::from_env(IID, Some("0")), None, "none");
    let (refusing, _) = node(revoked);
    let (hearth, fake) = plain_node();
    let t0 = wanted::now_ms();
    let subs_only = plain(&format!(r#"{{"iid":"{IID}","ep":0,"subs":{{"osKey":"os"}}}}"#));
    for (node, config) in [(&hearth, "not-a-config".to_owned()), (&hearth, subs_only), (&refusing, config())]
    {
        let (status, answer) = post(node, &config, json!({"items": [episode(3, t0)]})).await;
        assert_eq!((status, &answer["error"]), (StatusCode::BAD_REQUEST, &json!("bad_config")));
        assert_eq!(read(node, &config).await.0, StatusCode::BAD_REQUEST);
    }
    // Nothing was taken in by a refused list.
    wanted::tick(&hearth, t0 + 20 * MINUTE).await;
    wanted::tick(&refusing, t0 + 20 * MINUTE).await;
    assert!(fake.asked().is_empty());

    // An item is ids and a schedule, nothing that names a search Scout cannot make.
    let mut bad = episode(3, t0);
    bad["imdb"] = json!("../../etc");
    let (status, answer) = post(&hearth, &config(), json!({"items": [bad]})).await;
    assert_eq!((status, &answer["error"]), (StatusCode::BAD_REQUEST, &json!("bad_item")));
}
