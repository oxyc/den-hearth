//! Go: `internal/scout/client_test.go` (132 lines).

use crate::attributes::stream_attributes;
use crate::client::{
    ClientPlayable, AUDIO_CONVERTED_COST, NEVER_PLAYS_COST, VIDEO_CONVERTED_COST, VIDEO_UNNAMED_COST,
};
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
    let remux =
        rs("Film.2024.2160p.UHD.BluRay.REMUX.HEVC.HDR.TrueHD.7.1.mkv", |s| s.info_hash = "remux".into());
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
        // The XviD AVI is transcoded, which ranks it above a profile 5 this browser most likely can't show at all.
        (
            "4K HDR HEVC: the remux only needs its audio converted",
            Some(a_browser(|_| {})),
            vec![remux.title.clone(), web.title.clone(), xvid.title.clone(), dv5.title.clone()],
        ),
        (
            "no HEVC: the remux has to be transcoded, so the 1080p H.264 plays first",
            Some(a_browser(|p| {
                p.hevc_main = 0;
                p.hevc_main10 = 0;
            })),
            vec![web.title.clone(), remux.title.clone(), xvid.title.clone(), dv5.title.clone()],
        ),
        (
            "no HDR: the same",
            Some(a_browser(|p| p.hdr = false)),
            vec![web.title.clone(), remux.title.clone(), xvid.title.clone(), dv5.title.clone()],
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
        // Transcoded for their container whatever the codec, the audio with them.
        ("Film.2005.DVDRip.XviD.MP3.avi", VIDEO_CONVERTED_COST + AUDIO_CONVERTED_COST),
        ("Film.2026.1080p.HDTS.H.264.AAC.ts", VIDEO_CONVERTED_COST + AUDIO_CONVERTED_COST),
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

#[tokio::test]
async fn stream_list_ranked_for_the_browser_that_asked() {
    use std::sync::Arc;

    use crate::handler::{new_handler, PLAYABLE_HEADER};
    use crate::tests::handler::{fake_scraper, get, hdr, test_deps, valid_blob};

    const GIB: i64 = 1 << 30;
    let h = new_handler(test_deps(|d| {
        d.make_scrapers = Arc::new(|_| {
            vec![fake_scraper("torrentio", || {
                Box::pin(async {
                    Ok(vec![
                        RawStream {
                            info_hash: "a".repeat(40),
                            title: "Movie 2160p WEB-DL HDR HEVC AAC".into(),
                            size_bytes: Some(18 * GIB),
                            seeders: Some(100),
                            file_idx: Some(0),
                            ..Default::default()
                        },
                        RawStream {
                            info_hash: "b".repeat(40),
                            title: "Movie 1080p WEB-DL H.264 AAC".into(),
                            size_bytes: Some(8 * GIB),
                            seeders: Some(50),
                            file_idx: Some(0),
                            ..Default::default()
                        },
                    ])
                })
            })]
        });
    }));
    let path = format!("/{}/stream/movie/tt1234567.json", valid_blob());
    let first = |rr: &http::Response<Vec<u8>>| -> String {
        let list: serde_json::Value = serde_json::from_slice(rr.body()).unwrap_or(serde_json::Value::Null);
        let title = list["streams"][0]["title"].as_str();
        match (rr.status().as_u16(), title) {
            (200, Some(title)) => title.to_owned(),
            _ => panic!("list: {} {}", rr.status(), String::from_utf8_lossy(rr.body())),
        }
    };
    let no_hevc = serde_json::to_string(&a_browser(|p| {
        p.hevc_main = 0;
        p.hevc_main10 = 0;
    }))
    .expect("json");

    let tv = get(&h, &path, &[]).await;
    let got = first(&tv);
    assert!(got.contains("2160p"), "the TV's list starts {got:?}, want the 4K release");
    let vary = hdr(&tv, "vary");
    assert!(vary.contains(PLAYABLE_HEADER), "vary = {vary:?}, want it to name {PLAYABLE_HEADER}");
    let got = first(&get(&h, &path, &[(PLAYABLE_HEADER, no_hevc.as_str())]).await);
    assert!(got.contains("1080p"), "a browser without HEVC gets {got:?} first, want the 1080p H.264");
    let got = first(&get(&h, &path, &[]).await);
    assert!(
        got.contains("2160p"),
        "after a browser's list the TV gets {got:?} first: the two lists must be cached apart"
    );
    let got = first(&get(&h, &path, &[(PLAYABLE_HEADER, "{not json")]).await);
    assert!(got.contains("2160p"), "a report that doesn't parse gets {got:?} first, want the TV's list");
}
