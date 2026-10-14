//! Go: `internal/scout/linkcheck_test.go` (353 lines). Work package 6.
//!
//! The Go tests run the check against `httptest` servers. The verdict table below runs it through a mocked
//! `Doer` instead, which sees the same request and answers with the same headers. The `/play` tests drive the
//! handler and the stores, so they wait for packages 1, 2 and 4 and keep their Go bodies as comments.

use async_trait::async_trait;

use super::mock::MockDoer;
use crate::linkcheck::{check_link, LinkVerdict};
use crate::scrape::{DoError, Doer};

/// Builds a response with headers, the way `httptest` would have written them.
fn response(status: u16, headers: &[(&str, &str)], body: &str) -> reqwest::Response {
    let mut b = http::Response::builder().status(status);
    for (k, v) in headers {
        b = b.header(*k, *v);
    }
    reqwest::Response::from(b.body(body.to_owned()).expect("valid response"))
}

/// A `Doer` that never answers: the check's own timeout is the only thing that ends it.
struct NeverAnswers;

#[async_trait]
impl Doer for NeverAnswers {
    async fn do_request(&self, _req: reqwest::Request) -> Result<reqwest::Response, DoError> {
        std::future::pending().await
    }
}

#[tokio::test]
async fn check_link_verdicts() {
    // (name, status, content-type, content-range, want size, want verdict)
    let cases: [(&str, u16, &str, &str, i64, LinkVerdict); 12] = [
        ("206 of the listed size", 206, "video/x-matroska", "bytes 0-0/1000", 1000, LinkVerdict::Playable),
        ("200, the range ignored", 200, "application/octet-stream", "", 1000, LinkVerdict::Playable),
        ("size unknown to the debrid", 206, "video/mp4", "bytes 0-0/5555", 0, LinkVerdict::Playable),
        ("total not stated", 206, "video/mp4", "bytes 0-0/*", 1000, LinkVerdict::Playable),
        ("within the tolerance", 206, "video/mp4", "bytes 0-0/1015", 1000, LinkVerdict::Playable),
        ("a different file", 206, "video/mp4", "bytes 0-0/1100", 1000, LinkVerdict::Broken),
        ("an error page", 200, "text/html; charset=utf-8", "", 0, LinkVerdict::Broken),
        ("a JSON error body", 200, "application/json", "", 0, LinkVerdict::Broken),
        ("not found", 404, "text/plain", "", 0, LinkVerdict::Broken),
        ("forbidden", 403, "text/plain", "", 0, LinkVerdict::Broken),
        ("throttled", 429, "text/plain", "", 0, LinkVerdict::Unverified),
        ("CDN fault", 503, "text/plain", "", 0, LinkVerdict::Unverified),
    ];
    for (name, status, ctype, rng, want_size, want) in cases {
        let doer = MockDoer(move |req: reqwest::Request| {
            assert_eq!(
                req.headers().get("range").and_then(|v| v.to_str().ok()),
                Some("bytes=0-0"),
                "range"
            );
            let mut headers = vec![("content-type", ctype)];
            if !rng.is_empty() {
                headers.push(("content-range", rng));
            }
            Ok(response(status, &headers, "\0"))
        });
        let (got, reason) = check_link(&doer, "http://cdn.test/f.mkv", want_size, 0).await;
        assert_eq!(got, want, "{name}: verdict = {got:?} ({reason}), want {want:?}");
    }
}

// The check failing to get an answer says nothing about the link, so it never condemns one.
#[tokio::test(start_paused = true)]
async fn check_link_its_own_failure_does_not_condemn_the_link() {
    let refused = MockDoer(|_req: reqwest::Request| Err(DoError::from("connection refused")));
    let (got, _) = check_link(&refused, "http://cdn.test/f.mkv", 0, 0).await;
    assert_eq!(got, LinkVerdict::Unverified, "a refused connection gave verdict {got:?}, want unverified");

    // The Go test bounds this with a 50 ms context. check_link owns its own timeout (linkCheckTimeout), and
    // the paused clock lets the test run past it without waiting.
    let (got, _) = check_link(&NeverAnswers, "http://cdn.test/f.mkv", 0, 0).await;
    assert_eq!(got, LinkVerdict::Unverified, "a timed-out check gave verdict {got:?}, want unverified");
}

// Needs `handler`, `/play` and the dead-link route (work package 4), and `tallyStore` (test helper for
// packages 1 and 2). The Go body is kept, to port once they land.
#[test]
#[ignore = "needs handler, /play and tallyStore (packages 4, 1, 2)"]
fn play_a_link_that_fails_its_check_is_a_dead_link() {
    /*
    srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
        w.Header().Set("content-type", "text/html")
        _, _ = w.Write([]byte("<html>error</html>"))
    }))
    defer srv.Close()
    store := &tallyStore{svc: ServiceTorBox, resolve: func(int32) (string, error) { return srv.URL + "/f.mkv", nil }}
    h := NewHandler(testDeps(func(d *Deps) {
        d.ProbeClient = srv.Client()
        d.MakeStores = func(*Config) []Store { return []Store{store} }
    }))
    path := "/" + validBlob + "/play/" + encodePlayToken(PlayTarget{InfoHash: repeat("a", 40), FileIdx: intp(0)})

    for i := 1; i <= 2; i++ {
        rr := do(h, path, nil)
        if rr.Code != http.StatusNotFound || !strings.Contains(rr.Body.String(), "dead_link") {
            t.Fatalf("play %d: %d %s, want 404 dead_link", i, rr.Code, rr.Body.String())
        }
    }
    if got := store.resolves.Load(); got != 2 {
        t.Errorf("resolves = %d, want 2 — a rejected link must not be served from the memo", got)
    }
    */
}

// Needs `handler`, `/play`, `linkMemo` and `tallyStore` (packages 4, 1, 2).
#[test]
#[ignore = "needs handler, /play, linkMemo and tallyStore (packages 4, 1, 2)"]
fn play_a_check_that_cannot_answer_passes_the_link_through() {
    /*
    closed := httptest.NewServer(http.NotFoundHandler())
    link := closed.URL + "/f.mkv"
    closed.Close()
    store := &tallyStore{svc: ServiceTorBox, resolve: func(int32) (string, error) { return link, nil }}
    h := &handler{deps: testDeps(func(d *Deps) {
        d.ProbeClient = http.DefaultClient
        d.MakeStores = func(*Config) []Store { return []Store{store} }
    })}
    config, _ := decodeConfig(nil, validBlob)
    target := ResolveTarget{InfoHash: repeat("a", 40), FileIdx: intp(0)}
    path := "/" + validBlob + "/play/" + encodePlayToken(PlayTarget{InfoHash: target.InfoHash, FileIdx: target.FileIdx})

    rr := do(http.HandlerFunc(h.serve), path, nil)
    if rr.Code != http.StatusFound || rr.Header().Get("location") != link {
        t.Fatalf("play: %d %q, want 302 to the unchecked link", rr.Code, rr.Header().Get("location"))
    }
    hit, ok := h.links.get(linkMemoKey(config, target), time.Now())
    if !ok || hit.checked {
        t.Fatalf("memo = %+v ok=%v, want an unchecked entry", hit, ok)
    }
    */
}

// Needs `handler`, `/play` and `linkMemo` (packages 4 and 1).
#[test]
#[ignore = "needs handler, /play and linkMemo (packages 4, 1)"]
fn play_a_checked_link_is_not_checked_again() {
    /*
    var checks atomic.Int32
    srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
        checks.Add(1)
        w.Header().Set("content-type", "video/x-matroska")
        w.Header().Set("content-range", "bytes 0-0/1000")
        w.WriteHeader(http.StatusPartialContent)
        _, _ = w.Write([]byte{0})
    }))
    defer srv.Close()
    store := &tallyStore{svc: ServiceTorBox, resolve: func(int32) (string, error) { return srv.URL + "/f.mkv", nil }}
    h := NewHandler(testDeps(func(d *Deps) {
        d.ProbeClient = srv.Client()
        d.MakeStores = func(*Config) []Store { return []Store{store} }
    }))
    path := "/" + validBlob + "/play/" + encodePlayToken(PlayTarget{InfoHash: repeat("a", 40), FileIdx: intp(0)})

    for i := 1; i <= 3; i++ {
        if rr := do(h, path, nil); rr.Code != http.StatusFound {
            t.Fatalf("play %d: %d", i, rr.Code)
        }
    }
    if got := checks.Load(); got != 1 {
        t.Errorf("link checked %d times, want once", got)
    }
    */
}

// Needs the TorBox and Real-Debrid stores' `KnownFileSize`, `rememberFileSize` and `rdTorrentKey` (packages
// 1 and 2).
#[test]
#[ignore = "needs torBoxStore / realDebridStore KnownFileSize from stores/torbox.rs and realdebrid.rs (packages 1, 2)"]
fn known_file_size_from_what_the_stores_already_hold() {
    /*
    cache := NewMemoryCache(1 << 20)
    tb := &torBoxStore{token: "t", cache: cache, api: torboxAPI}
    cache.Put(resolveKey("t", H), `{"torrentId":9,"files":[{"Index":0,"Name":"S01E01.mkv","SizeBytes":10},`+
        `{"Index":1,"Name":"S01E02.mkv","SizeBytes":20}]}`, resolveCacheTTL)
    if n, ok := tb.KnownFileSize(ResolveTarget{InfoHash: H, Season: intp(1), Episode: intp(2)}); !ok || n != 20 {
        t.Errorf("torbox size = %d ok=%v, want 20", n, ok)
    }
    if _, ok := tb.KnownFileSize(ResolveTarget{InfoHash: repeat("f", 40)}); ok {
        t.Error("torbox claimed a size for a release it has no entry for")
    }

    target := ResolveTarget{InfoHash: H}
    rdCache := NewMemoryCache(1 << 20)
    rdCache.Put(rdTorrentKey("k", H, target), "abc", resolveCacheTTL)
    rd := &realDebridStore{token: "k", api: realDebridAPI, cache: rdCache, client: routed{
        routes: map[string]func() (*http.Response, error){
            "torrents/info": ok(`{"status":"downloaded","links":["https://rd/l"],` +
                `"files":[{"id":1,"path":"Movie.mkv","bytes":900,"selected":1}]}`),
            "unrestrict": ok(`{"download":"https://rd/final.mkv"}`),
        }}}
    if _, err := rd.Resolve(t.Context(), target); err != nil {
        t.Fatal(err)
    }
    if n, ok := rd.KnownFileSize(target); !ok || n != 900 {
        t.Errorf("realdebrid size = %d ok=%v, want 900", n, ok)
    }
    */
}

// The helpers below serve the handler-level tests further down: `sizedCDN` answers the one-byte range the
// check asks for, `tbTorrent` answers as TorBox holding a torrent with a file list.
/*
type tbFile struct {
    id   int
    name string
    size int64
}

// sizedCDN serves each path in `totals` as a file of that many bytes, answering the one-byte range the link
// check asks for. Any other path answers 200 with no Content-Range.
func sizedCDN(t *testing.T, totals map[string]int64) *httptest.Server {
    srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
        w.Header().Set("content-type", "video/x-matroska")
        if n, ok := totals[r.URL.Path]; ok {
            w.Header().Set("content-range", "bytes 0-0/"+strconv.FormatInt(n, 10))
            w.WriteHeader(http.StatusPartialContent)
        }
        _, _ = w.Write([]byte{0})
    }))
    t.Cleanup(srv.Close)
    return srv
}

// tbTorrent answers as TorBox holding torrent 42 with `files`: mylist lists them, and requestdl links each
// file id to `<cdn>/<id>.mkv`.
func tbTorrent(cdn string, files []tbFile) func(*http.Request) *http.Response {
    return func(r *http.Request) *http.Response {
        switch path.Base(r.URL.Path) {
        case "requestdl":
            return resp(200, `{"success":true,"data":"`+cdn+"/"+r.URL.Query().Get("file_id")+`.mkv"}`)
        case "mylist":
            var list []string
            for _, f := range files {
                list = append(list, fmt.Sprintf(`{"id":%d,"name":%q,"size":%d}`, f.id, f.name, f.size))
            }
            return resp(200, `{"success":true,"data":{"id":42,"files":[`+strings.Join(list, ",")+`]}}`)
        }
        return resp(404, `{}`)
    }
}
*/

// Needs `handler.verifyLink` and `StorePool` (packages 4 and 1).
#[test]
#[ignore = "needs handler.verify_link, StorePool and torBoxStore (packages 4, 1)"]
fn verify_link_wrong_file_only_for_a_movie_whose_file_size_is_unknown() {
    /*
    cdn := sizedCDN(t, map[string]int64{"/sample.mkv": 522})
    h := &handler{deps: Deps{ProbeClient: cdn.Client()}}
    unknown := &StorePool{stores: []Store{&torBoxStore{token: "t", cache: NewMemoryCache(1 << 20), api: torboxAPI}}}
    cache := NewMemoryCache(1 << 20)
    cache.Put(resolveKey("t", H), `{"torrentId":9,"files":[{"Index":0,"Name":"Movie.mkv","SizeBytes":522}]}`,
        resolveCacheTTL)
    listed := &StorePool{stores: []Store{&torBoxStore{token: "t", cache: cache, api: torboxAPI}}}
    const release = 16_000_000_000
    for _, tc := range []struct {
        name string
        pool *StorePool
        rt   ResolveTarget
        path string
        want linkVerdict
    }{
        {"a movie whose file size is unknown", unknown,
            ResolveTarget{InfoHash: H, FileIdx: intp(0), ReleaseSize: release}, "/sample.mkv", linkWrongFile},
        {"no release size", unknown, ResolveTarget{InfoHash: H, FileIdx: intp(0)}, "/sample.mkv", linkPlayable},
        {"an episode", unknown,
            ResolveTarget{InfoHash: H, Season: intp(1), Episode: intp(2), ReleaseSize: release}, "/sample.mkv", linkPlayable},
        {"the file size is known", listed,
            ResolveTarget{InfoHash: H, FileIdx: intp(0), ReleaseSize: release}, "/sample.mkv", linkPlayable},
        {"at least half the release", unknown,
            ResolveTarget{InfoHash: H, FileIdx: intp(0), ReleaseSize: 1000}, "/sample.mkv", linkPlayable},
        {"no total served", unknown,
            ResolveTarget{InfoHash: H, FileIdx: intp(0), ReleaseSize: release}, "/whole.mkv", linkPlayable},
    } {
        if got := h.verifyLink(t.Context(), tc.pool, tc.rt, cdn.URL+tc.path); got != tc.want {
            t.Errorf("%s: verdict %d, want %d", tc.name, got, tc.want)
        }
    }
    */
}

// Needs `/play`, the ticket route and `heldTorBoxPlay` (packages 4 and 1).
#[test]
#[ignore = "needs /play, the ticket route and heldTorBoxPlay (packages 4, 1)"]
fn play_a_movie_link_to_another_file_is_re_resolved_with_the_file_list() {
    /*
    cdn := sizedCDN(t, map[string]int64{"/0.mkv": 522, "/1.mkv": 16_000_000_000})
    h, calls, play := sizedTicketPlay(t, cdn,
        []tbFile{{0, "Movie/Sample/sample.mkv", 522}, {1, "Movie/Movie.2160p.REMUX.mkv", 16_000_000_000}},
        PlayTarget{InfoHash: repeat("a", 40), FileIdx: intp(0), ReleaseSize: 16_000_000_000})

    rr := do(h, play, nil)
    if rr.Code != http.StatusFound || rr.Header().Get("location") != cdn.URL+"/1.mkv" {
        t.Fatalf("play: %d %q, want 302 to the feature", rr.Code, rr.Header().Get("location"))
    }
    if want := []string{"requestdl", "mylist", "requestdl"}; !sameCalls(*calls, want) {
        t.Errorf("upstream calls = %v, want %v", *calls, want)
    }

    *calls = nil
    rr = do(h, play+"?fresh=1", nil)
    if rr.Code != http.StatusFound || rr.Header().Get("location") != cdn.URL+"/1.mkv" {
        t.Fatalf("second play: %d %q, want 302 to the feature", rr.Code, rr.Header().Get("location"))
    }
    if want := []string{"requestdl"}; !sameCalls(*calls, want) {
        t.Errorf("second play calls = %v, want %v — the list should have been kept", *calls, want)
    }
    */
}

// Needs `/play`, the ticket route and `heldTorBoxPlay` (packages 4 and 1).
#[test]
#[ignore = "needs /play, the ticket route and heldTorBoxPlay (packages 4, 1)"]
fn play_a_movie_link_of_the_release_size_costs_nothing_more() {
    /*
    cdn := sizedCDN(t, map[string]int64{"/0.mkv": 16_000_000_000})
    h, calls, play := sizedTicketPlay(t, cdn, nil,
        PlayTarget{InfoHash: repeat("a", 40), FileIdx: intp(0), ReleaseSize: 16_000_000_000})
    if rr := do(h, play, nil); rr.Code != http.StatusFound || rr.Header().Get("location") != cdn.URL+"/0.mkv" {
        t.Fatalf("play: %d %q", rr.Code, rr.Header().Get("location"))
    }
    if want := []string{"requestdl"}; !sameCalls(*calls, want) {
        t.Errorf("upstream calls = %v, want %v", *calls, want)
    }
    */
}

// Needs `/play`, the ticket route and `heldTorBoxPlay` (packages 4 and 1).
#[test]
#[ignore = "needs /play, the ticket route and heldTorBoxPlay (packages 4, 1)"]
fn play_a_collection_film_smaller_than_half_the_release_still_plays() {
    /*
    cdn := sizedCDN(t, map[string]int64{"/0.mkv": 9_000_000_000, "/1.mkv": 10_000_000_000, "/2.mkv": 11_000_000_000})
    h, calls, play := sizedTicketPlay(t, cdn,
        []tbFile{{0, "Trilogy/Part.1.mkv", 9_000_000_000}, {1, "Trilogy/Part.2.mkv", 10_000_000_000},
            {2, "Trilogy/Part.3.mkv", 11_000_000_000}},
        PlayTarget{InfoHash: repeat("a", 40), FileIdx: intp(0), ReleaseSize: 30_000_000_000})
    if rr := do(h, play, nil); rr.Code != http.StatusFound || rr.Header().Get("location") != cdn.URL+"/0.mkv" {
        t.Fatalf("play: %d %q, want 302 to part 1", rr.Code, rr.Header().Get("location"))
    }
    if want := []string{"requestdl", "mylist", "requestdl"}; !sameCalls(*calls, want) {
        t.Errorf("upstream calls = %v, want %v", *calls, want)
    }
    */
}

// Needs `/play`, the ticket route and `heldTorBoxPlay` (packages 4 and 1).
#[test]
#[ignore = "needs /play, the ticket route and heldTorBoxPlay (packages 4, 1)"]
fn play_a_wrong_file_whose_re_resolve_fails_serves_the_first_link() {
    /*
    cdn := sizedCDN(t, map[string]int64{"/0.mkv": 522})
    hash := repeat("a", 40)
    kr := ticketKeyring(t)
    listing := tbTorrent(cdn.URL, nil)
    h, calls := heldTorBoxPlay(hash, func(r *http.Request) *http.Response {
        if path.Base(r.URL.Path) == "mylist" {
            return resp(500, "boom")
        }
        return listing(r)
    }, func(d *Deps) { d.ProbeClient = cdn.Client(); d.SealKeyring = kr })
    play := "/p/" + newTicketKeys(kr).mint(&Config{Debrid: []DebridAccount{{ServiceTorBox, "tb-secret"}}},
        PlayTarget{InfoHash: hash, FileIdx: intp(0), ReleaseSize: 16_000_000_000}, time.Now().Add(time.Hour))

    rr := do(h, play, nil)
    if rr.Code != http.StatusFound || rr.Header().Get("location") != cdn.URL+"/0.mkv" {
        t.Fatalf("play: %d %q, want 302 to the first link", rr.Code, rr.Header().Get("location"))
    }
    if want := []string{"requestdl", "mylist"}; !sameCalls(*calls, want) {
        t.Errorf("upstream calls = %v, want %v", *calls, want)
    }
    */
}
