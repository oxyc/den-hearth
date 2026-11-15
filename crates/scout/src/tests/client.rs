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
            "Dolby Vision 5 shown: it plays, but the 4K remux whose TrueHD is converted is still the better picture",
            Some(a_browser(|p| p.dolby_vision.p5 = true)),
            vec![remux.title.clone(), dv5.title.clone(), web.title.clone(), xvid.title.clone()],
        ),
    ];
    for (name, client, want) in cases {
        let got = rank(client);
        assert_eq!(got.join("|"), want.join("|"), "{name}: order\n got {got:?}\nwant {want:?}");
    }
}

/// A guest's Chrome on macOS, one episode's list (The Sopranos S02E05, 2026-11-14): a 3.5 GB H.264 "4K" ranked
/// first and never played, and a 0.9 GB 10-bit x265 re-encode won the pick over the Blu-ray and web releases,
/// because converting their DTS or DD+ cost more than their source and three times their size were worth.
#[test]
fn rank_a_bigger_release_of_one_resolution_above_a_small_re_encode() {
    let gb = |n: f64| Some((n * crate::rank::GIB as f64) as i64);
    let release = |title: &str, size: Option<i64>| {
        rs(title, |s| {
            s.info_hash = title.into();
            s.size_bytes = size;
        })
    };
    let remux = release("The.Sopranos.S02E05.1080p.BluRay.REMUX.AVC.DTS-HD.MA.5.1-EPSiLON.mkv", gb(10.4));
    let bluray = release("The.Sopranos.S02E05.1080p.BluRay.x264.DTS-SbR.mkv", gb(4.4));
    let web = release("The.Sopranos.S02E05.1080p.WEB-DL.DD+5.1.H.264-NTb.mkv", gb(2.9));
    let fake_4k = release("The.Sopranos.S02E05.2160p.WEB-DL.H.264.AAC-XEBEC.mp4", gb(3.5));
    let hazmatt = release("The Sopranos S02E05 1080p Bluray 10bit x265 AAC 5.1 HazMatt.mkv", gb(0.87));
    let small = release("The.Sopranos.S02E05.1080p.BluRay.x264.AAC-YTS.mp4", gb(0.41));
    let all = [fake_4k.clone(), hazmatt.clone(), small.clone(), web.clone(), bluray.clone(), remux.clone()];
    let got = titles(&rank_streams(
        &all,
        RankFilters { result_cap: 10, client: Some(a_browser(|_| {})), ..Default::default() },
    ));
    let at = |s: &RawStream| got.iter().position(|t| *t == s.title).unwrap();
    assert_eq!(got[0], remux.title, "the remux leads: {got:?}");
    assert!(at(&bluray) < at(&hazmatt), "the 4.4 GB Blu-ray above the re-encode: {got:?}");
    assert!(at(&hazmatt) < at(&small), "{got:?}");
    assert!(at(&web) < at(&small), "{got:?}");
    assert!(at(&fake_4k) > at(&bluray), "a 9 Mbit/s H.264 “4K” ranks as the 1080p it is: {got:?}");
}

/// An install that prefers 2160p sank every other resolution below the "2160p H.264" re-encode, so it led the list
/// although its score ranks it as 1080p (The Sopranos S01E01, 2026-11-15). A starved "4K" is a 1080p release to the
/// preference and the resolutions filter too, and a real 4K keeps the preference.
#[test]
fn a_starved_4k_does_not_ride_a_2160p_preference() {
    let gb = |n: f64| Some((n * crate::rank::GIB as f64) as i64);
    let release = |title: &str, size: Option<i64>| {
        rs(title, |s| {
            s.info_hash = title.into();
            s.size_bytes = size;
        })
    };
    let fake = release("The Sopranos S01E01 2160p H.264 (moviesbyrizzoconv).mp4", gb(4.0));
    let remux = release("The.Sopranos.S01E01.1080p.BluRay.REMUX.AVC.DTS-HD.MA.5.1-NOGRP.mkv", gb(14.3));
    let real = release("Show.S01E01.2160p.WEB-DL.DDP5.1.HEVC-X.mkv", gb(8.0));
    let ranked = |all: &[RawStream], prefer: &str, resolutions: Vec<String>| {
        titles(&rank_streams(
            all,
            RankFilters {
                result_cap: 10,
                prefer_resolution: prefer.into(),
                resolutions,
                client: Some(a_browser(|_| {})),
                ..Default::default()
            },
        ))
    };
    let got = ranked(&[fake.clone(), remux.clone()], "2160p", vec![]);
    assert_eq!(got[0], remux.title, "the starved 4K led under a 2160p preference: {got:?}");
    let got = ranked(&[remux.clone(), real.clone()], "2160p", vec![]);
    assert_eq!(got[0], real.title, "a real 4K keeps the preference: {got:?}");
    let got = ranked(&[fake.clone(), real.clone()], "", vec!["2160p".into()]);
    assert_eq!(got, vec![real.title.clone()], "only-2160p keeps the real 4K and drops the starved one");
}

#[test]
fn a_2160p_release_is_judged_by_its_bitrate_for_the_codec_it_names() {
    use crate::rank::starved_4k;
    let gib = |n: f64| Some((n * crate::rank::GIB as f64) as i64);
    let starved = |title: &str, size: Option<i64>, height: i32| {
        let s = rs(title, |s| {
            s.size_bytes = size;
            if height > 0 {
                s.probe = Some(Probe { height, ..Default::default() });
            }
        });
        starved_4k(s.traits(), &s)
    };
    assert!(starved("Show.S01E01.2160p.WEB-DL.H.264-X.mkv", gib(3.5), 0), "H.264 episode at ~11 Mbit/s");
    assert!(!starved("Show.S01E01.2160p.WEB-DL.H.264-X.mkv", gib(6.0), 0), "H.264 episode at ~19 Mbit/s");
    assert!(!starved("Show.S01E01.2160p.WEB-DL.HEVC-X.mkv", gib(3.5), 0), "HEVC episode at ~11 Mbit/s");
    assert!(starved("Show.S01E01.2160p.WEB-DL.HEVC-X.mkv", gib(1.5), 0), "HEVC episode at ~4.8 Mbit/s");
    assert!(starved("Film.2024.2160p.WEB-DL.H.264-X.mkv", gib(9.0), 0), "H.264 film at ~13 Mbit/s");
    assert!(!starved("Film.2024.2160p.WEB-DL.HEVC-X.mkv", gib(9.0), 0), "HEVC film at ~13 Mbit/s");
    assert!(!starved("Film.2024.2160p.WEB-DL-X.mkv", gib(3.5), 0), "no codec named: only the size floor");
    assert!(starved("Film.2024.2160p.WEB-DL-X.mkv", gib(0.5), 0), "under the size floor");
    assert!(starved("Film.2024.2160p.WEB-DL.HEVC-X.mkv", gib(20.0), 1080), "probed at 1080 lines");
    assert!(!starved("Film.2024.2160p.WEB-DL.HEVC-X.mkv", None, 0), "size unknown: no judgement");
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
