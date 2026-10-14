//! Go: `internal/scout/client_test.go` (132 lines). Work package 5.

use crate::attributes::stream_attributes;
use crate::client::{ClientPlayable, AUDIO_CONVERTED_COST, NEVER_PLAYS_COST, VIDEO_CONVERTED_COST, VIDEO_UNNAMED_COST};
use crate::probe::Probe;
use crate::rank::{rank_streams, RankFilters, RawStream};

use super::rank::{rs, titles};

// A browser that decodes 4K HDR HEVC and AV1 but no Dolby Vision or E-AC-3, as Chrome on a Mac reports.
pub(crate) fn a_browser(change: impl FnOnce(&mut ClientPlayable)) -> ClientPlayable {
    let mut p = ClientPlayable {
        h264: 51,
        hevc_main: 153,
        hevc_main10: 153,
        hdr: true,
        aac_multichannel: true,
        av1: 13,
        av1_main10: 13,
        av1_hdr: true,
        ..Default::default()
    };
    change(&mut p);
    p
}

#[test]
fn rank_streams_for_the_browser_that_asked() {
    let remux = rs("Film.2024.2160p.UHD.BluRay.REMUX.HEVC.HDR.TrueHD.7.1.mkv", |s| s.info_hash = "remux".into());
    let dv5 = rs("Film.2024.2160p.WEB-DL.DV.HEVC.AAC.mkv", |s| {
        s.info_hash = "dv5".into();
        s.probe = Some(Probe {
            container: "matroska".into(),
            video_codec: "hevc".into(),
            dolby_vision: true,
            dv_profile: 5,
            bit_depth: 10,
            ..Default::default()
        });
    });
    let web = rs("Film.2024.1080p.WEB-DL.H.264.AAC.mkv", |s| s.info_hash = "web".into());
    let xvid = rs("Film.2024.DVDRip.XviD.MP3.avi", |s| s.info_hash = "xvid".into());
    let all = [xvid.clone(), web.clone(), dv5.clone(), remux.clone()];
    let rank = |client: Option<ClientPlayable>| {
        titles(&rank_streams(&all, RankFilters { result_cap: 10, client, ..Default::default() }))
    };

    let tv = rank(None);
    assert_eq!(tv[0], remux.title, "the TV's list starts {:?}, want the 4K remux", tv[0]);

    let cases: Vec<(&str, Option<ClientPlayable>, Vec<String>)> = vec![
        (
            "4K HDR HEVC: the remux only needs its audio converted",
            Some(a_browser(|_| {})),
            vec![remux.title.clone(), web.title.clone(), dv5.title.clone(), xvid.title.clone()],
        ),
        (
            "no HEVC: the remux has to be transcoded, so the 1080p H.264 plays first",
            Some(a_browser(|p| {
                p.hevc_main = 0;
                p.hevc_main10 = 0;
            })),
            vec![web.title.clone(), remux.title.clone(), dv5.title.clone(), xvid.title.clone()],
        ),
        (
            "no HDR: the same",
            Some(a_browser(|p| p.hdr = false)),
            vec![web.title.clone(), remux.title.clone(), dv5.title.clone(), xvid.title.clone()],
        ),
        (
            "Dolby Vision 5 shown: the 4K web release copies whole, ahead of the remux whose TrueHD is converted",
            Some(a_browser(|p| p.dolby_vision.p5 = true)),
            vec![dv5.title.clone(), remux.title.clone(), web.title.clone(), xvid.title.clone()],
        ),
    ];
    for (name, client, want) in cases {
        let got = rank(client);
        assert_eq!(got.join("|"), want.join("|"), "{name}: order\n got {got:?}\nwant {want:?}");
    }
}

#[test]
fn client_playable_cost() {
    let p = a_browser(|_| {});
    let bare = |title: &str| RawStream { title: title.into(), ..Default::default() };
    let cases = [
        ("Film.1080p.WEB-DL.H.264.AAC.mkv", 0),
        ("Film.1080p.WEB-DL.H.264.DDP5.1.mkv", AUDIO_CONVERTED_COST),
        ("Film.1080p.WEB-DL.AAC.mkv", VIDEO_UNNAMED_COST),
        ("Film.2160p.WEB-DL.H.264.AAC.mkv", 0),
        ("Film.1080p.Hi10P.AAC.mkv", NEVER_PLAYS_COST),
        ("Film.2160p.WEB-DL.AV1.AAC.mkv", 0),
        ("Film.1080p.WEB-DL.AV1.AAC.webm", NEVER_PLAYS_COST),
        ("Film.1080p.BluRay.H.264.AAC.3D.HSBS.mkv", NEVER_PLAYS_COST),
        ("Film.1080p.BluRay.H.264.AAC.m2ts", NEVER_PLAYS_COST),
    ];
    for (title, want) in cases {
        let got = p.cost(bare(title), stream_attributes(bare(title)));
        assert_eq!(got, want, "cost({title:?}) = {got}, want {want}");
    }

    let eac3 = a_browser(|p| p.eac3 = true);
    let got = eac3.cost(
        bare("Film.1080p.WEB-DL.H.264.DDP5.1.mkv"),
        stream_attributes(bare("Film.1080p.WEB-DL.H.264.DDP5.1.mkv")),
    );
    assert_eq!(got, 0, "E-AC-3 for a player that plays it costs {got}, want 0: it is copied");

    let mut dv5 = stream_attributes(bare("Film.2160p.WEB-DL.DV.HEVC.AAC.mkv"));
    dv5.dv_profile = 5;
    dv5.dv_profile_guess = 0;
    let got = p.cost(bare("Film.2160p.WEB-DL.DV.HEVC.AAC.mkv"), dv5);
    assert_eq!(
        got,
        VIDEO_CONVERTED_COST * 2,
        "probed Profile 5 costs {got}, want {}: keep it last but available for den-remux's definitive probe",
        VIDEO_CONVERTED_COST * 2
    );
}

// Needs `NewHandler`, `testDeps`, `Deps`, `scraper` and the `do` request helper from package 4 (handler.rs);
// the Go body is kept below as the reference for that port.
#[test]
#[ignore = "needs NewHandler, Deps, the scraper seam and the do helper from package 4 (handler)"]
fn stream_list_ranked_for_the_browser_that_asked() {
    todo!("port once handler.rs lands; Go body below")
    // seeds := func(context.Context) ([]RawStream, error) {
    //     return []RawStream{
    //         {InfoHash: repeat("a", 40), Title: "Movie 2160p WEB-DL HDR HEVC AAC", SizeBytes: intp(18 * gib), Seeders: intp(100), FileIdx: intp(0)},
    //         {InfoHash: repeat("b", 40), Title: "Movie 1080p WEB-DL H.264 AAC", SizeBytes: intp(8 * gib), Seeders: intp(50), FileIdx: intp(0)},
    //     }, nil
    // }
    // h := NewHandler(testDeps(func(d *Deps) {
    //     d.MakeScrapers = func(*Config) []scraper { return []scraper{fakeScraper{"torrentio", seeds}} }
    // }))
    // path := "/" + validBlob + "/stream/movie/tt1234567.json"
    // first := func(rr *httptest.ResponseRecorder) string {
    //     t.Helper()
    //     var list streamsResponse
    //     if rr.Code != 200 || json.Unmarshal(rr.Body.Bytes(), &list) != nil || len(list.Streams) == 0 {
    //         t.Fatalf("list: %d %s", rr.Code, rr.Body.String())
    //     }
    //     return list.Streams[0].Title
    // }
    // noHEVC, _ := json.Marshal(aBrowser(func(p *ClientPlayable) { p.HEVCMain, p.HEVCMain10 = 0, 0 }))
    //
    // tv := do(h, path, nil)
    // if got := first(tv); !strings.Contains(got, "2160p") {
    //     t.Errorf("the TV's list starts %q, want the 4K release", got)
    // }
    // if !strings.Contains(tv.Header().Get("vary"), playableHeader) {
    //     t.Errorf("vary = %q, want it to name %s", tv.Header().Get("vary"), playableHeader)
    // }
    // if got := first(do(h, path, map[string]string{playableHeader: string(noHEVC)})); !strings.Contains(got, "1080p") {
    //     t.Errorf("a browser without HEVC gets %q first, want the 1080p H.264", got)
    // }
    // if got := first(do(h, path, nil)); !strings.Contains(got, "2160p") {
    //     t.Errorf("after a browser's list the TV gets %q first: the two lists must be cached apart", got)
    // }
    // if got := first(do(h, path, map[string]string{playableHeader: "{not json"})); !strings.Contains(got, "2160p") {
    //     t.Errorf("a report that doesn't parse gets %q first, want the TV's list", got)
    // }
}
