//! Go: `internal/scout/stores_pick_test.go` (1211 lines). Work package 2.
//!
//! File and episode selection per store. The episode matchers (`pickEpisodeFile`, `namesEpisode`, ...) are
//! `season.go`, work package 5, and TorBox's `selectFileID` is work package 1; those tests are ignored
//! stubs until their modules land. Three run against this package: the Real-Debrid position mapping, the
//! status and service-name checks, and the dead-link message.
//!
//! Picking a file out of a season pack is the step where a mistake is invisible until playback: the
//! stream plays, it is simply the wrong episode. Each store picks differently when the name match fails
//! (TorBox trusts the given index, RD falls back to the first file, Premiumize to the largest), so each
//! needs its own table rather than one shared assumption.

use std::sync::Arc;

use crate::scrape::{DoError, Doer};
use crate::season::TorrentFile;
use crate::stores::premiumize::{PremiumizeStore, PREMIUMIZE_API};
use crate::stores::realdebrid::{RealDebridStore, REAL_DEBRID_API};
use crate::stores::{DeadLinkError, ResolveTarget, Store, StoreError};

use super::mock::{resp, MockDoer};

/// Go `H` (stores_test.go:19).
const H: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";

type Answer = Result<reqwest::Response, DoError>;

/// Go `file`.
fn file(idx: i64, name: &str, size: i64) -> TorrentFile {
    TorrentFile { index: idx, name: name.into(), size_bytes: Some(size) }
}

/// Go `wantPick`: the picker served a file, and it is `want`.
fn want_pick(got: Result<Option<i64>, StoreError>, want: i64, what: &str) {
    match got {
        Err(e) => panic!("{what}: got error {e:?}, want file {want}"),
        Ok(None) => panic!("{what}: got nil, want {want}"),
        Ok(Some(v)) => assert_eq!(v, want, "{what}"),
    }
}

/// A Real-Debrid store that is never asked anything, for the tests that call a picker directly.
fn idle_real_debrid() -> RealDebridStore {
    let client: Arc<dyn Doer> =
        Arc::new(MockDoer(|_r: reqwest::Request| -> Answer { Ok(resp(200, "{}")) }));
    RealDebridStore { token: String::new(), client, cache: None, api: REAL_DEBRID_API.into() }
}

fn idle_premiumize() -> PremiumizeStore {
    let client: Arc<dyn Doer> =
        Arc::new(MockDoer(|_r: reqwest::Request| -> Answer { Ok(resp(200, "{}")) }));
    PremiumizeStore { token: String::new(), client, cache: None, api: PREMIUMIZE_API.into() }
}

// The pack is the common case: name-matching must beat the positional index, because the indexer's
// fileIdx and the debrid's own file order are not guaranteed to agree.
#[test]
#[ignore = "needs season.rs pickEpisodeFile (package 5)"]
fn pick_episode_file_name_match_beats_order() {
    #[cfg(any())]
    fn pending() {
        let files = [
            file(0, "Show.S01E01.1080p.mkv", 900),
            file(1, "Show.S01E02.1080p.mkv", 950),
            file(2, "Show.S01E03.1080p.mkv", 1000),
        ];
        want_pick(pick_episode_file(&files, 1, 2), 1, "S01E02");
        want_pick(pick_episode_file(&files, 1, 3), 2, "S01E03");

        // Every file here NAMES an episode, and none of them is S02E01 — so this pack demonstrably does not
        // hold what was asked for. Falling back to the largest video served S01E03 as though it were S02E01:
        // the stream plays, it is simply the wrong episode, and nothing upstream can tell.
        want_not_in_torrent(pick_episode_file(&files, 2, 1), "an episode-labelled pack that lacks the episode");
    }
    todo!("pending: stores_pick_test.go:51")
}

// The fallback survives where it is actually needed: a release whose filename names no episode at all is
// a single episode, and its one video is the only thing it could be. Killing this to fix the case above
// would break every untagged single-episode release.
#[test]
#[ignore = "needs season.rs pickEpisodeFile (package 5)"]
fn pick_episode_file_unlabelled_still_falls_back() {
    #[cfg(any())]
    fn pending() {
        let single = [file(0, "Show.1080p.WEB-DL.x264.mkv", 4000)];
        want_pick(pick_episode_file(&single, 4, 7), 0, "an untagged release falls back to its video");

        // A year and a resolution are digit runs that the bare 102/0102 matchers can brush against; neither
        // makes a name episode-LABELLED, so this must still fall back rather than refuse.
        let yearly = [file(0, "Movie.2019.1080p.BluRay.mkv", 8000)];
        want_pick(pick_episode_file(&yearly, 1, 1), 0, "a year/resolution is not an episode label");
    }
    todo!("pending: stores_pick_test.go:72")
}

// Samples and extras carry the episode number too. The real episode is the big file.
#[test]
#[ignore = "needs season.rs pickEpisodeFile (package 5)"]
fn pick_episode_file_prefers_the_largest_match() {
    #[cfg(any())]
    fn pending() {
        // Samples and extras carry the episode number too. The real episode is the big file.
        let files = [file(0, "Show.S01E02.sample.mkv", 20), file(1, "Show.S01E02.1080p.mkv", 4000)];
        want_pick(pick_episode_file(&files, 1, 2), 1, "the sample must lose to the episode");
    }
    todo!("pending: stores_pick_test.go:85")
}

// Non-video files (nfo, subs, artwork) are excluded — but a pack of ONLY non-video must still be
// searchable rather than answering nil, or a legitimate odd container becomes unplayable.
#[test]
#[ignore = "needs season.rs pickEpisodeFile (package 5)"]
fn pick_episode_file_videos_first_then_anything() {
    #[cfg(any())]
    fn pending() {
        // Non-video files (nfo, subs, artwork) are excluded — but a pack of ONLY non-video must still be
        // searchable rather than answering nil, or a legitimate odd container becomes unplayable.
        let mixed = [file(0, "Show.S01E02.nfo", 1), file(1, "Show.S01E02.mkv", 500)];
        want_pick(pick_episode_file(&mixed, 1, 2), 1, "the video wins over the nfo");

        let only_other = [file(0, "Show.S01E02.iso", 500)];
        want_pick(pick_episode_file(&only_other, 1, 2), 0, "with no video extension, fall back to the pool");

        // Nothing to pick, and nothing to be wrong about — an empty list is not a mismatch.
        let got = pick_episode_file(&[], 1, 2);
        assert!(matches!(got, Ok(None)), "an empty pack has nothing to pick: {got:?}");
    }
    todo!("pending: stores_pick_test.go:96")
}

// TorBox: name match first, then the caller's index, and an out-of-range index is handed back untouched
// (it is the debrid's own id space, not a slice offset).
#[test]
#[ignore = "needs stores/torbox.rs selectFileID (package 1)"]
fn select_file_id_torbox() {
    #[cfg(any())]
    fn pending() {
        // TorBox: name match first, then the caller's index, and an out-of-range index is handed back untouched
        // (it is the debrid's own id space, not a slice offset).
        let files = [file(0, "a.mkv", 10), file(1, "b.mkv", 20)];

        // Neither file names an episode, so the match falls back to the largest video rather than failing.
        want_pick(select_file_id(&files, &ep(1, 1)), 1, "no name match → largest video");
        assert!(matches!(select_file_id(&files, &target_none()), Ok(None)), "no episode and no index → nothing to select");

        want_pick(select_file_id(&files, &target_idx(1)), 1, "an in-range index maps through the pack");
        want_pick(select_file_id(&files, &target_idx(9)), 9, "an out-of-range index is passed through as an id");

        let named = [file(7, "Show.S02E05.mkv", 100), file(8, "Show.S02E06.mkv", 100)];
        want_pick(select_file_id(&named, &ep(2, 6)), 8, "the name match wins over any index");

        // The indexer's fileIdx is not a second opinion once the file list is in hand: the list itself proves
        // S02E09 is not here, so an in-range index must not smuggle S02E05 back in.
        let with_idx = ResolveTarget { season: Some(2), episode: Some(9), file_idx: Some(0), ..target_none() };
        want_not_in_torrent(select_file_id(&named, &with_idx), "a fileIdx cannot override a proven mismatch");
    }
    todo!("pending: stores_pick_test.go:117")
}

// Real-Debrid: same name-match precedence, but an unusable index falls back to the FIRST file rather
// than to nothing — RD always needs a concrete file id to unrestrict.
#[test]
#[ignore = "needs season.rs errEpisodeNotInTorrent (package 5)"]
fn pick_file_id_real_debrid() {
    #[cfg(any())]
    fn pending() {
        // Real-Debrid: same name-match precedence, but an unusable index falls back to the FIRST file rather
        // than to nothing — RD always needs a concrete file id to unrestrict.
        let s = idle_real_debrid();
        let files = [file(3, "a.mkv", 10), file(4, "b.mkv", 20)];

        assert!(matches!(s.pick_file_id(&[], &ep(1, 1)), Ok(None)), "no files → no pick");
        want_pick(s.pick_file_id(&files, &target_idx(1)), 4, "in-range index");
        // An out-of-range index describes a file list that is not this one, so it is no opinion at all — and
        // files[0] handed back whatever sorted first, which on a release with a Sample/ directory is the
        // sample. The largest file is the same guess Premiumize makes, and what "no index at all" already got.
        want_pick(s.pick_file_id(&files, &target_idx(99)), 4, "out of range → largest, not whatever sorted first");

        let named = [file(3, "Show.S01E04.mkv", 10), file(4, "Show.S01E05.mkv", 20)];
        want_pick(s.pick_file_id(&named, &ep(1, 5)), 4, "the name match wins");

        // Without the refusal, RD's own fallback ("first file") hands back S01E04 for a request for S01E09.
        want_not_in_torrent(s.pick_file_id(&named, &ep(1, 9)), "RD refuses rather than falling back to the first file");
    }
    todo!("pending: stores_pick_test.go:148")
}

// Premiumize: an unusable index falls back to the LARGEST file — with no id space of its own, the
// biggest file in a pack is the best guess at the feature.
#[test]
#[ignore = "needs season.rs errEpisodeNotInTorrent (package 5)"]
fn pick_index_premiumize() {
    #[cfg(any())]
    fn pending() {
        // Premiumize: an unusable index falls back to the LARGEST file — with no id space of its own, the
        // biggest file in a pack is the best guess at the feature.
        let s = idle_premiumize();
        let files = [file(0, "small.mkv", 10), file(1, "big.mkv", 9000)];

        assert!(matches!(s.pick_index(&[], &ep(1, 1)), Ok(None)), "no files → no pick");
        want_pick(s.pick_index(&files, &target_idx(0)), 0, "in-range index");
        want_pick(s.pick_index(&files, &target_idx(42)), 1, "out of range → largest");
        want_pick(s.pick_index(&files, &target_none()), 1, "no index at all → largest");

        let named = [file(0, "Show.S03E01.mkv", 10), file(1, "Show.S03E02.mkv", 20)];
        want_pick(s.pick_index(&named, &ep(3, 1)), 0, "the name match wins over the largest");

        // Premiumize's largest-file fallback is the most dangerous of the three: it reliably returns the
        // biggest episode in the pack, which looks entirely plausible right up until it plays.
        want_not_in_torrent(s.pick_index(&named, &ep(3, 9)), "Premiumize refuses rather than serving the largest episode");
    }
    todo!("pending: stores_pick_test.go:174")
}

// A negative index is untrusted input from an addon, not an offset — it must never index backwards.
#[test]
#[ignore = "needs stores/torbox.rs selectFileID (package 1)"]
fn pickers_reject_negative_index() {
    #[cfg(any())]
    fn pending() {
        // A negative index is untrusted input from an addon, not an offset — it must never index backwards.
        let files = [file(0, "a.mkv", 10), file(1, "b.mkv", 20)];
        let neg = target_idx(-1);

        want_pick(select_file_id(&files, &neg), -1, "TorBox hands a negative back as an id, never as an offset");
        want_pick(idle_real_debrid().pick_file_id(&files, &neg), 1, "RD falls back to the largest");
        want_pick(idle_premiumize().pick_index(&files, &neg), 1, "Premiumize falls back to the largest");
    }
    todo!("pending: stores_pick_test.go:199")
}

// The two stores with no status API must say so — and be honest that they are saying nothing, so the
// pool moves on to one that can answer instead of reporting a wait nobody is serving.
#[tokio::test]
async fn stores_without_status_report_nothing() {
    let stores: [Box<dyn Store>; 2] = [Box::new(idle_real_debrid()), Box::new(idle_premiumize())];
    for st in stores {
        let target = ResolveTarget { info_hash: H.into(), ..Default::default() };
        assert!(
            st.status(&target).await.is_none(),
            "{} claims a download status it cannot know",
            st.service().as_str()
        );
    }
}

// Each store names itself — the pool's ordering and the cache-truth rule both key on this.
#[test]
#[ignore = "needs stores/torbox.rs (package 1)"]
fn stores_identify_themselves() {
    #[cfg(any())]
    fn pending() {
        // Each store names itself — the pool's ordering and the cache-truth rule both key on this.
        assert_eq!(torbox_store().service(), DebridService::TorBox);
        assert_eq!(idle_real_debrid().service(), DebridService::RealDebrid);
        assert_eq!(idle_premiumize().service(), DebridService::Premiumize);
        // Only TorBox and Premiumize have a real cache API; RD contributing "cache truth" would make an
        // all-false answer look authoritative and empty every cached-only list.
        assert!(is_cache_truth_service(DebridService::TorBox) && is_cache_truth_service(DebridService::Premiumize),
            "TorBox and Premiumize are the cache-truth services");
        assert!(!is_cache_truth_service(DebridService::RealDebrid), "RD has no cache API — it must never count as cache truth");
    }
    todo!("pending: stores_pick_test.go:222")
}

// The error a dead link raises is matched by string in places; keep its shape asserted.
#[test]
fn dead_link_error_message() {
    let err = DeadLinkError { reason: "torbox createtorrent http 429".into() };
    assert_eq!(err.to_string(), "dead_link: torbox createtorrent http 429");
}

// The per-store fallbacks pick from the VIDEOS, the same pool pickEpisodeFile uses. RD's and Premiumize's
// tails picked over every file, which was unreachable while pickEpisodeFile answered for an unlabelled
// pack — and the moment it stopped, a pack whose biggest entry is an .iso resolved to that: a 302, no
// error, no video.
#[test]
#[ignore = "needs stores/torbox.rs (package 1) and season.rs (package 5)"]
fn stores_the_fallback_never_picks_a_non_video() {
    #[cfg(any())]
    fn pending() {
        // The per-store fallbacks pick from the VIDEOS, the same pool pickEpisodeFile uses. RD's and Premiumize's
        // tails picked over every file, which was unreachable while pickEpisodeFile answered for an unlabelled
        // pack — and the moment it stopped, a pack whose biggest entry is an .iso resolved to that: a 302, no
        // error, no video.
        let files = [
            file(0, "Show.S01.Extras.iso", 9000),
            file(1, "Show.1080p.mkv", 4000),
            file(2, "Sample/sample.mkv", 2),
        ];
        // TorBox is excluded from the out-of-range case on purpose: an index past the list is handed back
        // as a raw TorBox file id there, which is its own documented id space rather than a slice offset.
        let cases: [(&str, bool); 3] = [("torbox", false), ("realdebrid", true), ("premiumize", true)];
        for (name, out_of_range_falls_back) in cases {
            let mut targets = vec![ep(1, 3)];
            if out_of_range_falls_back {
                targets.push(ResolveTarget { file_idx: Some(42), ..ep(1, 3) });
            }
            for t in &targets {
                let got = match name {
                    "torbox" => select_file_id(&files, t),
                    "realdebrid" => idle_real_debrid().pick_file_id(&files, t),
                    _ => idle_premiumize().pick_index(&files, t),
                };
                want_pick(got, 1, &format!("{name}: the feature, not the .iso"));
            }
        }
        // A pool with no video at all still has to answer with something rather than nothing.
        let only_other = [file(0, "Show.iso", 10), file(1, "Show.big.iso", 9000)];
        want_pick(idle_real_debrid().pick_file_id(&only_other, &ep(1, 3)), 1, "with no video anywhere, the largest is all there is");
    }
    todo!("pending: stores_pick_test.go:258")
}

// A pack numbered the way most anime and much TV is packed: the episode is found by its own number.
//
// A miss, though, is NOT evidence the pack lacks the episode — bare numbering is often absolute, so
// season 2 episode 1 is packed as `- 13` and nothing in the name says so. Only an unambiguous SxxExx
// label, which carries its season, can condemn a pack; a bare miss defers to the indexer's file index.
#[test]
#[ignore = "needs season.rs pickEpisodeFile (package 5)"]
fn pick_episode_file_bare_numbered_packs() {
    #[cfg(any())]
    fn pending() {
        // A pack numbered the way most anime and much TV is packed: the episode is found by its own number.
        //
        // A miss, though, is NOT evidence the pack lacks the episode — bare numbering is often absolute, so
        // season 2 episode 1 is packed as `- 13` and nothing in the name says so. Only an unambiguous SxxExx
        // label, which carries its season, can condemn a pack; a bare miss defers to the indexer's file index.
        let pack = [
            file(0, "[Grp] Show - 01 [1080p].mkv", 900),
            file(1, "[Grp] Show - 02 [1080p].mkv", 4000), // the largest: what the old fallback served
            file(2, "[Grp] Show - 03 [1080p].mkv", 950),
        ];
        want_pick(pick_episode_file(&pack, 1, 3), 2, "the episode is named by its own number");
        want_pick(pick_episode_file(&pack, 1, 1), 0, "and so is the first one");
        assert!(matches!(pick_episode_file(&pack, 1, 9), Ok(None)), "a bare-number miss must defer, not condemn");
    }
    todo!("pending: stores_pick_test.go:297")
}

// The bare-number matcher's boundaries, asserted where they can actually fail.
//
// Every name below is a real release shape. A dot cannot open a standalone number and can only close one
// when no digit follows, because `DDP5.1`, `TrueHD.7.1`, `AC3.5.1`, `H.264`, `AAC.2.0` and a date all
// contain dot-delimited digit runs. Treating the dot as an ordinary separator made every file in a pack
// with 5.1 audio — most packs — match episode 1, and the largest was served: ask for E01 of a
// ten-episode pack, get E10, 302, no error.
#[test]
#[ignore = "needs season.rs bareEpisodePatterns (package 5)"]
fn episode_patterns_bare_number_boundaries() {
    #[cfg(any())]
    fn pending() {
        // The bare-number matcher's boundaries, asserted where they can actually fail.
        //
        // Every name below is a real release shape. A dot cannot open a standalone number and can only close one
        // when no digit follows, because `DDP5.1`, `TrueHD.7.1`, `AC3.5.1`, `H.264`, `AAC.2.0` and a date all
        // contain dot-delimited digit runs. Treating the dot as an ordinary separator made every file in a pack
        // with 5.1 audio — most packs — match episode 1, and the largest was served: ask for E01 of a
        // ten-episode pack, get E10, 302, no error.
        let cases: [(&str, i64, bool, &str); 17] = [
            ("[Grp] Show - 03 [1080p].mkv", 3, true, "the shape the matcher exists for"),
            ("[Grp] Show - 3 [1080p].mkv", 3, true, "unpadded"),
            ("[Grp] Show - 03.mkv", 3, true, "a dot closes the token when no digit follows"),
            ("Show_07_1080p.mkv", 7, true, "underscores are separators too"),
            ("Show.S01E03.1080p.AMZN.WEB-DL.DDP5.1.H.264-NTb.mkv", 1, false, "DDP5.1 is not episode 1"),
            ("Show.S01E03.1080p.AMZN.WEB-DL.DDP5.1.H.264-NTb.mkv", 264, false, "H.264 is not episode 264"),
            ("Show.S03E07.TrueHD.7.1.Atmos-GRP.mkv", 1, false, "TrueHD.7.1 is not episode 1"),
            ("Show.S03E07.DTS-HD.MA.5.1-GRP.mkv", 5, false, "a dot-led 5 is not episode 5"),
            ("Show.S03E07.AAC.2.0-GRP.mkv", 2, false, "AAC.2.0 is not episode 2"),
            ("Show.S03E07.AAC.2.0-GRP.mkv", 0, false, "nor episode 0"),
            // Space-separated names are where the trailing rule earns its keep: the leading rule alone cannot
            // see `5` in "DDP 5.1", because a space really does open the token there. Only "a dot may not
            // close a number when a digit follows" rejects it.
            ("Show S03E07 DDP 5.1 Atmos-GRP.mkv", 5, false, "DDP 5.1 is not episode 5"),
            ("Show S03E07 DTS-HD MA 7.1-GRP.mkv", 7, false, "nor 7.1 episode 7"),
            ("Show.2024.03.15.1080p.WEB.h264-GRP.mkv", 3, false, "a date is not an episode"),
            ("[Group8] Feature [720p].mkv", 8, false, "a group tag is not an episode"),
            ("Movie.2019.1080p.BluRay.x264.mkv", 19, false, "a year is not an episode"),
            ("Show.S01E03.1080p.mkv", 1080, false, "a resolution is not an episode"),
            ("[Grp] Show - 03 [1080p][HEVC][10bit].mkv", 10, false, "10bit is not episode 10"),
        ];
        for (name, episode, want, why) in cases {
            let got = matches_episode(name, &bare_episode_patterns(episode));
            assert_eq!(got, want, "ep {episode} vs {name:?} — {why}");
        }
    }
    todo!("pending: stores_pick_test.go:319")
}

// The two failures the boundaries above prevent, driven through the real pickers.
#[test]
#[ignore = "needs season.rs pickEpisodeFile (package 5)"]
fn pick_episode_file_modern_pack_names_are_not_all_episode_one() {
    #[cfg(any())]
    fn pending() {
        // The two failures the boundaries above prevent, driven through the real pickers.
        //
        // A plain WEB-DL season pack. Every file carries DDP5.1 and H.264.
        let pack = [
            file(0, "Show.S01E01.First.1080p.AMZN.WEB-DL.DDP5.1.H.264-NTb.mkv", 900),
            file(1, "Show.S01E02.Second.1080p.AMZN.WEB-DL.DDP5.1.H.264-NTb.mkv", 950),
            file(2, "Show.S01E10.Tenth.1080p.AMZN.WEB-DL.DDP5.1.H.264-NTb.mkv", 9000),
        ];
        want_pick(pick_episode_file(&pack, 1, 1), 0, "E01 is E01, not the biggest file in the pack");

        // An absolute-numbered anime pack: season 2 packed as episodes 13-24, which is what the catalogue's
        // S02E01 means. The names match nothing, and the pool must NOT be condemned — the indexer's file
        // index is right and the caller needs the chance to use it.
        let absolute = [
            file(0, "[Grp] Show - 13 [1080p][HEVC][10bit].mkv", 900),
            file(1, "[Grp] Show - 14 [1080p][HEVC][10bit].mkv", 950),
        ];
        assert!(matches!(pick_episode_file(&absolute, 2, 1), Ok(None)), "an absolute-numbered pack must defer to the indexer");

        // A lone episode beside its sample, neither naming an episode: still playable.
        let sampled = [
            file(0, "Show.1080p.WEB-DL.DDP5.1.H.264-NTb.mkv", 4000),
            file(1, "Show.1080p.WEB-DL.DDP5.1.H.264-NTb-sample.mkv", 2),
        ];
        want_pick(select_file_id(&sampled, &ep(1, 3)), 0, "a feature plus a sample is not a pack that lacks your episode");
    }
    todo!("pending: stores_pick_test.go:355")
}

// The precedence between the two kinds of evidence, which a union got wrong.
//
// A bare number carries no season and cannot say which show it belongs to. Where the filenames DO name
// their episodes it is noise — `[10-bit]` is not episode 10, `SG-1` is not episode 1 — and unioning the
// two, then breaking the tie by file size, let that noise outrank the file that really did name the
// episode. Where the names carry no label it is the only evidence there is, and must still work.
#[test]
#[ignore = "needs season.rs pickEpisodeFile (package 5)"]
fn pick_episode_file_a_bare_number_never_outranks_a_label() {
    #[cfg(any())]
    fn pending() {
        // The precedence between the two kinds of evidence, which a union got wrong.
        //
        // A bare number carries no season and cannot say which show it belongs to. Where the filenames DO name
        // their episodes it is noise — `[10-bit]` is not episode 10, `SG-1` is not episode 1 — and unioning the
        // two, then breaking the tie by file size, let that noise outrank the file that really did name the
        // episode. Where the names carry no label it is the only evidence there is, and must still work.
        //
        // Every file is tagged [10-bit]; only one is E10. The tag must not win, and it is on the big file.
        let ten_bit = [
            file(0, "Show.S01E10.1080p.WEB-DL.x265.[10-bit].mkv", 900),
            file(1, "Show.S01E11.1080p.WEB-DL.x265.[10-bit].mkv", 9000),
        ];
        want_pick(pick_episode_file(&ten_bit, 1, 10), 0, "E10 is the file named E10, not the biggest one carrying a 10");

        // A number in the show's own title, on a properly labelled pack.
        let titled = [
            file(0, "Stargate SG-1 - S03E01 - Into the Fire.mkv", 900),
            file(1, "Stargate SG-1 - S03E02 - Seth.mkv", 9000),
        ];
        want_pick(pick_episode_file(&titled, 3, 1), 0, "SG-1 is the show, not episode 1");

        // And a labelled pack that lacks the episode is still refused, not answered from a title number.
        want_not_in_torrent(pick_episode_file(&titled, 3, 9), "a labelled pack cannot be talked into an answer by a bare number");

        // Unlabelled, the bare number is all there is and must still decide.
        let bare = [
            file(0, "[Grp] Show - 01 [1080p].mkv", 9000),
            file(1, "[Grp] Show - 02 [1080p].mkv", 900),
        ];
        want_pick(pick_episode_file(&bare, 1, 2), 1, "with nothing else to go on, the bare number decides");
    }
    todo!("pending: stores_pick_test.go:391")
}

// Real-Debrid's in-range fileIdx branch maps a POSITION to RD's own file id. Nothing verified it: every
// fixture used ids equal to their slice positions, so the mapping and a raw pass-through were the same
// value and deleting the branch changed nothing. RD ids are 1-based in practice, and never a slice index.
#[test]
fn pick_file_id_real_debrid_maps_position_to_its_own_id() {
    let files = [file(11, "a.mkv", 10), file(22, "b.mkv", 20), file(33, "c.mkv", 15)];
    let got = idle_real_debrid().pick_file_id(
        &files,
        &ResolveTarget { info_hash: H.into(), file_idx: Some(2), ..Default::default() },
    );
    want_pick(got, 33, "position 2 is RD's file id 33, not the number 2");
}

// A movie's fileIdx that lands on something other than the feature — TorBox listing the torrent in another
// order — gives way to the largest video. A collection's films are alike in size, so there it still decides.
#[test]
#[ignore = "needs stores/torbox.rs selectFileID (package 1)"]
fn select_file_id_torbox_movie_position_on_an_extra() {
    #[cfg(any())]
    fn pending() {
        // A movie's fileIdx that lands on something other than the feature — TorBox listing the torrent in another
        // order — gives way to the largest video. A collection's films are alike in size, so there it still decides.
        let remux = [
            file(40, "Movie/Movie.2012.2160p.mkv", 16_000_000),
            file(41, "Movie/Movie.nfo", 5),
            file(42, "Movie/Sample/sample.mkv", 522),
        ];
        want_pick(select_file_id(&remux, &target_idx(2)), 40, "a position on the sample serves the feature");
        want_pick(select_file_id(&remux, &target_idx(1)), 40, "a position on the .nfo serves the feature");

        let trilogy = [
            file(50, "Trilogy/Part.1.mkv", 9_000),
            file(51, "Trilogy/Part.2.mkv", 10_000),
            file(52, "Trilogy/Part.3.mkv", 11_000),
        ];
        want_pick(select_file_id(&trilogy, &target_idx(0)), 50, "in a collection the position still picks the film");
    }
    todo!("pending: stores_pick_test.go:432")
}

// The same gap on the other two, so the three are pinned alike.
#[test]
#[ignore = "needs stores/torbox.rs selectFileID (package 1)"]
fn pickers_map_position_to_the_stores_own_id() {
    #[cfg(any())]
    fn pending() {
        // The same gap on the other two, so the three are pinned alike.
        let files = [file(11, "a.mkv", 10), file(22, "b.mkv", 20), file(33, "c.mkv", 15)];
        want_pick(select_file_id(&files, &target_idx(1)), 22, "TorBox: position 1 is its file id 22");
        // Premiumize indexes its own content array, so there the position IS the answer — asserted with ids
        // that differ from positions so the two cannot be confused.
        want_pick(idle_premiumize().pick_index(&files, &target_idx(1)), 1, "Premiumize: the position is the answer, and it is not the id");
    }
    todo!("pending: stores_pick_test.go:453")
}

// All three stores hand back PATHS, so the torrent's root directory is part of every file's name — and
// no fixture in this suite was path-shaped, which is why the directory could speak for every file.
// A directory describes the pack; only the filename describes the file.
#[test]
#[ignore = "needs season.rs pickEpisodeFile (package 5)"]
fn pick_episode_file_the_directory_is_not_evidence_about_the_file() {
    #[cfg(any())]
    fn pending() {
        // All three stores hand back PATHS, so the torrent's root directory is part of every file's name — and
        // no fixture in this suite was path-shaped, which is why the directory could speak for every file.
        // A directory describes the pack; only the filename describes the file.
        //
        // A scene pack whose root names the whole range: every file matched a request for S01E01 through the
        // directory, and the largest was served.
        let root = "Show.S01E01-E10.1080p.AMZN.WEB-DL.DDP5.1.H.264-NTb/";
        let scene = [
            file(201, &format!("{root}Show.S01E01.1080p.mkv"), 900),
            file(208, &format!("{root}Show.S01E08.1080p.mkv"), 9000),
        ];
        want_pick(pick_episode_file(&scene, 1, 1), 201, "E01 is the file named E01, not every file under a directory that says E01-E10");

        // An anime pack whose root carries a season and a bit-depth tag. `(Season 1)` and `[10-bit]` matched
        // episodes 1 and 10 in every file.
        let anime = "[Anime Time] Show (Season 1) [BD 1080p][HEVC 10-bit][AAC]/";
        let tagged = [
            file(101, &format!("{anime}[Anime Time] Show - 01.mkv"), 900),
            file(107, &format!("{anime}[Anime Time] Show - 07.mkv"), 9000),
        ];
        want_pick(pick_episode_file(&tagged, 1, 1), 101, "episode 1 is the file numbered 01, not the one under a (Season 1) directory");
        assert!(matches!(pick_episode_file(&tagged, 1, 10), Ok(None)), "[10-bit] in the directory is not episode 10");

        // Nested directories, which is what a season pack of a multi-season release looks like.
        let deep = [
            file(301, "Show Complete/Season 01/Show.S01E01.mkv", 900),
            file(308, "Show Complete/Season 01/Show.S01E08.mkv", 9000),
        ];
        want_pick(pick_episode_file(&deep, 1, 1), 301, "only the last component names the file");
    }
    todo!("pending: stores_pick_test.go:466")
}

// A number that matches EVERY candidate is part of what they share — the show's own title — not what
// tells them apart. Serving the largest of "all of them" is how a request for episode 1 of Stargate
// SG-1 returned episode 2.
#[test]
#[ignore = "needs season.rs pickEpisodeFile (package 5)"]
fn pick_episode_file_a_title_number_is_not_an_episode() {
    #[cfg(any())]
    fn pending() {
        // A number that matches EVERY candidate is part of what they share — the show's own title — not what
        // tells them apart. Serving the largest of "all of them" is how a request for episode 1 of Stargate
        // SG-1 returned episode 2.
        let sg1 = [
            file(301, "Stargate SG-1 - 01 - Children of the Gods.mkv", 900),
            file(302, "Stargate SG-1 - 02 - The Enemy Within.mkv", 9000),
        ];
        // Every file matches episode 1 — one by its number, both by the title — so the match tells them apart
        // not at all, and the honest answer is to say nothing and let the indexer's own file index decide.
        // What must NOT happen is what did: taking the largest of "all of them" and serving episode 2 for a
        // request for episode 1.
        assert!(matches!(pick_episode_file(&sg1, 1, 1), Ok(None)), "a number matching every file decides nothing");
        // Episode 2 is told apart by its own number, so it is still found.
        want_pick(pick_episode_file(&sg1, 1, 2), 302, "episode 2 is named by one file only, so it is found");

        // Episode 7 is not here. "SG-1" must not answer for it, and neither must the largest file.
        let blake = [
            file(401, "Blake's 7 - 01 - The Way Back.mkv", 900),
            file(402, "Blake's 7 - 02 - Space Fall.mkv", 9000),
        ];
        assert!(matches!(pick_episode_file(&blake, 1, 7), Ok(None)), "a number in the title is not an episode");
    }
    todo!("pending: stores_pick_test.go:501")
}

// One labelled file must not switch the weak tier off for the rest of the pack. An anime batch commonly
// carries a single labelled special beside bare-numbered episodes; treating the pool as labelled turned
// a hit into a hard refusal, and the client records a release that plays as dead.
#[test]
#[ignore = "needs season.rs pickEpisodeFile (package 5)"]
fn pick_episode_file_one_labelled_file_does_not_condemn_the_rest() {
    #[cfg(any())]
    fn pending() {
        // One labelled file must not switch the weak tier off for the rest of the pack. An anime batch commonly
        // carries a single labelled special beside bare-numbered episodes; treating the pool as labelled turned
        // a hit into a hard refusal, and the client records a release that plays as dead.
        let batch = [
            file(0, "[Grp] Show - 01 [1080p].mkv", 900),
            file(1, "[Grp] Show - 02 [1080p].mkv", 950),
            file(2, "[Grp] Show - 03 [1080p].mkv", 980),
            file(3, "[Grp] Show OVA S00E01 [1080p].mkv", 9000),
        ];
        want_pick(pick_episode_file(&batch, 1, 3), 2, "the special's label says nothing about the numbered episodes");

        // And when the bare tier finds nothing either, a mixed pack must DEFER rather than refuse — the
        // unlabelled files could be absolutely numbered, so their silence proves nothing about episode 9.
        assert!(matches!(pick_episode_file(&batch, 1, 9), Ok(None)), "a mixed pack cannot be condemned by its one labelled file");

        // And a pack where EVERY file names its episode still refuses when none of them is yours.
        let all_labelled = [file(0, "Show.S01E01.mkv", 900), file(1, "Show.S01E02.mkv", 950)];
        want_not_in_torrent(pick_episode_file(&all_labelled, 1, 9), "a fully labelled pack that lacks the episode holds nothing to play");
    }
    todo!("pending: stores_pick_test.go:530")
}

// One stray file cannot unmake a pack, and cannot rescue a number that decides nothing. Both guards used
// to be phrased as set-size comparisons over the pool, so a single extra video — a sample, a featurette,
// a bonus rip, all of them ordinary in a season pack and all of them videos — was enough to disarm each.
#[test]
#[ignore = "needs season.rs pickEpisodeFile (package 5)"]
fn pick_episode_file_one_stray_file_disarms_nothing() {
    #[cfg(any())]
    fn pending() {
        // One stray file cannot unmake a pack, and cannot rescue a number that decides nothing. Both guards used
        // to be phrased as set-size comparisons over the pool, so a single extra video — a sample, a featurette,
        // a bonus rip, all of them ordinary in a season pack and all of them videos — was enough to disarm each.
        let labelled = [
            file(0, "Show.S01E01.1080p.mkv", 900),
            file(1, "Show.S01E02.1080p.mkv", 950),
            file(2, "Show.S01E03.1080p.mkv", 980),
        ];
        // The pack names its episodes and none of them is S02E01, so there is nothing here to play — and
        // that stays true with a sample, a featurette or both alongside.
        let extras: [Vec<TorrentFile>; 4] = [
            vec![],
            vec![file(3, "Sample/show-sample.mkv", 20)],
            vec![file(3, "Extras/Behind the Scenes.mkv", 9000)],
            vec![file(3, "Sample/show-sample.mkv", 20), file(4, "Extras/Behind the Scenes.mkv", 9000)],
        ];
        for extra in extras {
            let mut pool = labelled.to_vec();
            pool.extend(extra.clone());
            want_not_in_torrent(pick_episode_file(&pool, 2, 1), &format!("a pack of other episodes plus {} extras", extra.len()));
        }

        // The mirror: a title number matches several files, so it tells them apart not at all — and adding a
        // sample must not turn that back into an answer.
        let sg1 = [
            file(301, "Stargate SG-1 - 01 - Children of the Gods.mkv", 900),
            file(302, "Stargate SG-1 - 02 - The Enemy Within.mkv", 950),
            file(303, "Stargate SG-1 - 03 - Emancipation.mkv", 9000),
        ];
        let mut with_sample = sg1.to_vec();
        with_sample.push(file(304, "sample.mkv", 20));
        for pool in [&sg1[..], &with_sample[..]] {
            assert!(matches!(pick_episode_file(pool, 1, 1), Ok(None)), "a number matching several files decides nothing ({} files)", pool.len());
        }
        // And the episode whose number really is unique to one file is still found, sample or not.
        for pool in [&sg1[..], &with_sample[..]] {
            want_pick(pick_episode_file(pool, 1, 2), 302, "episode 2 is named by exactly one file");
        }

        // A single labelled episode is not a pack: it cannot condemn anything.
        let one = [file(0, "Show.S01E01.1080p.mkv", 900)];
        assert!(matches!(pick_episode_file(&one, 2, 1), Ok(Some(_))), "a lone episode file is not a pack that lacks your episode");
    }
    todo!("pending: stores_pick_test.go:558")
}

// A backslash is legal in a POSIX filename, and these names come from the torrent. Splitting on it
// unconditionally threw away the part of the name that carries the label.
#[test]
#[ignore = "needs season.rs baseName (package 5)"]
fn base_name_only_splits_where_a_directory_really_ends() {
    #[cfg(any())]
    fn pending() {
        // A backslash is legal in a POSIX filename, and these names come from the torrent. Splitting on it
        // unconditionally threw away the part of the name that carries the label.
        let cases = [
            ("Show.S01E01.mkv", "Show.S01E01.mkv"),
            ("Pack/Show.S01E01.mkv", "Show.S01E01.mkv"),
            ("Pack/Sub/Show.S01E01.mkv", "Show.S01E01.mkv"),
            // A backslash is a legal filename character and every client joins the torrent's path components
            // with a forward slash, so a backslash is never a separator here.
            (r"Show S01E01 - Part 1 \ Part 2.mkv", r"Show S01E01 - Part 1 \ Part 2.mkv"),
            (r"Pack/Show S01E01 - Part 1 \ Part 2.mkv", r"Show S01E01 - Part 1 \ Part 2.mkv"),
        ];
        for (input, want) in cases {
            assert_eq!(base_name(input), want, "baseName({input:?})");
        }
        // The label survives, which is the point.
        let backslashed = [
            file(0, r"Show S01E01 - Part 1 \ Part 2.mkv", 900),
            file(1, r"Show S01E02 - Part 1 \ Part 2.mkv", 9000),
        ];
        want_pick(pick_episode_file(&backslashed, 1, 1), 0, "a backslash in the filename does not erase its episode label");
    }
    todo!("pending: stores_pick_test.go:605")
}

// An anime batch numbered absolutely, with a couple of labelled OVAs alongside — the shape the refusal
// must never fire on. The bare tier cannot match S02E01 against `- 13`, so control reaches the refusal,
// and because the error short-circuits every store's picker the indexer's correct fileIdx is never read:
// a release that plays, answered 404 and recorded dead by the client.
#[test]
#[ignore = "needs season.rs (package 5) and stores/torbox.rs selectFileID (package 1)"]
fn pick_episode_file_labelled_extras_do_not_condemn_a_numbered_batch() {
    #[cfg(any())]
    fn pending() {
        // An anime batch numbered absolutely, with a couple of labelled OVAs alongside — the shape the refusal
        // must never fire on. The bare tier cannot match S02E01 against `- 13`, so control reaches the refusal,
        // and because the error short-circuits every store's picker the indexer's correct fileIdx is never read:
        // a release that plays, answered 404 and recorded dead by the client.
        let batch: Vec<TorrentFile> = (13..=24)
            .map(|i| file(i, &format!("[Grp] Show S2 - {i} [1080p].mkv"), 900 + i))
            .collect();
        let specials = [
            file(90, "[Grp] Show OVA S00E01 [1080p].mkv", 500),
            file(91, "[Grp] Show OVA S00E02 [1080p].mkv", 500),
        ];
        // Two labelled specials among twelve numbered episodes: the labels describe the extras, not the pack.
        let mut mixed = batch.clone();
        mixed.extend(specials);
        assert!(matches!(pick_episode_file(&mixed, 2, 1), Ok(None)), "a numbered batch with labelled extras must defer to the indexer");
        // The store then uses the position the indexer gave, which is right.
        want_pick(select_file_id(&mixed, &ResolveTarget { season: Some(2), episode: Some(1), file_idx: Some(0), ..target_none() }), 13, "the indexer's position names episode 13, which is S02E01");

        // The majority rule in the other direction: labels that DO describe the pack still condemn it, even
        // with an unlabelled extra or two alongside.
        let pack = [
            file(0, "Show.S01E01.1080p.mkv", 900),
            file(1, "Show.S01E02.1080p.mkv", 950),
            file(2, "Show.S01E03.1080p.mkv", 980),
            file(3, "Sample/show-sample.mkv", 20),
        ];
        want_not_in_torrent(pick_episode_file(&pack, 2, 1), "three labelled episodes and one sample is still a labelled pack");
    }
    todo!("pending: stores_pick_test.go:632")
}

// A number naming several files does not tell them apart — but on a dual-quality pack those several ARE
// the episode, and the better copy is the answer. Discarding the match sent all three stores to "largest
// video anywhere in the pack", which is a different episode.
#[test]
#[ignore = "needs season.rs (package 5) and stores/torbox.rs selectFileID (package 1)"]
fn pickers_a_duplicate_number_is_still_the_episode() {
    #[cfg(any())]
    fn pending() {
        // A number naming several files does not tell them apart — but on a dual-quality pack those several ARE
        // the episode, and the better copy is the answer. Discarding the match sent all three stores to "largest
        // video anywhere in the pack", which is a different episode.
        let mut dual = Vec::new();
        for i in 1..=4 {
            dual.push(file(i * 2, &format!("[Grp] Show - 0{i} [1080p].mkv"), 4000 + i));
            dual.push(file(i * 2 + 1, &format!("[Grp] Show - 0{i} [720p].mkv"), 900 + i));
        }
        // pickEpisodeFile itself declines: two files carry the number, so it cannot say which.
        assert!(matches!(pick_episode_file(&dual, 1, 1), Ok(None)), "two matches is not a unique answer");
        // The stores then take the largest of the files that DO carry the number — episode 1 in 1080p —
        // rather than the largest of the pack, which is episode 4.
        let target = ResolveTarget { season: Some(1), episode: Some(1), ..target_none() };
        want_pick(select_file_id(&dual, &target), 2, "torbox: the 1080p copy of episode 1, not the biggest episode");
        want_pick(idle_real_debrid().pick_file_id(&dual, &target), 2, "realdebrid: likewise");
        // Premiumize answers with an index into its own content array, and builds TorrentFile.Index from
        // exactly that position — so the same value is the right answer there too.
        want_pick(idle_premiumize().pick_index(&dual, &target), 2, "premiumize: likewise, by its own content index");

        // And the indexer's own position still outranks the guess: position 3 is the fourth file, whose
        // TorBox id is 5, where the guess would have said 2.
        let with_idx = ResolveTarget { file_idx: Some(3), ..target };
        want_pick(select_file_id(&dual, &with_idx), 5, "a position in this torrent beats a number that named several files");
    }
    todo!("pending: stores_pick_test.go:665")
}

// A file that names an episode has named a DIFFERENT one by the time any guess is reached, so it can
// never be the answer, whatever its size. Counting rules decide whether a pack is condemned and there is
// always a band they miss — labelled files present but not a majority — where control used to reach
// "the largest video anywhere", pick a file whose filename says S01E02, and serve it for a request for
// S03E09 with a 302 and no error.
#[test]
#[ignore = "needs season.rs (package 5) and stores/torbox.rs selectFileID (package 1)"]
fn pickers_never_serve_a_file_that_names_a_different_episode() {
    #[cfg(any())]
    fn pending() {
        // A file that names an episode has named a DIFFERENT one by the time any guess is reached, so it can
        // never be the answer, whatever its size. Counting rules decide whether a pack is condemned and there is
        // always a band they miss — labelled files present but not a majority — where control used to reach
        // "the largest video anywhere", pick a file whose filename says S01E02, and serve it for a request for
        // S03E09 with a 302 and no error.
        //
        // Walk the whole counting band, including the equal counts the majority rule leaves open.
        for (labelled, unlabelled) in [(2, 1), (2, 2), (2, 4), (3, 3), (3, 5), (4, 4), (4, 6)] {
            let mut files = Vec::new();
            for i in 1..=labelled {
                files.push(file(i, &format!("Show.S01E{i:02}.1080p.mkv"), 1000 * i));
            }
            for i in 1..=unlabelled {
                files.push(file(100 + i, &format!("Extras/featurette-{i}.mkv"), 10));
            }
            let target = ResolveTarget { season: Some(3), episode: Some(9), ..target_none() };
            let picks: [(&str, Result<Option<i64>, StoreError>); 3] = [
                ("torbox", select_file_id(&files, &target)),
                ("realdebrid", idle_real_debrid().pick_file_id(&files, &target)),
                ("premiumize", idle_premiumize().pick_index(&files, &target)),
            ];
            for (name, got) in picks {
                // A refusal is always a safe answer here.
                let Ok(got) = got else { continue };
                let Some(index) = got else {
                    panic!("{name} {labelled}/{unlabelled}: got no answer and no error");
                };
                // What is asserted: never a file that has NAMED a different episode. What is knowingly NOT
                // asserted: that an unlabelled extra is refused. S03E09 is absent from this pool, so serving
                // `Extras/featurette-1.mkv` is wrong too — but scout cannot tell an extra from an absolutely
                // numbered episode by name, and refusing on that basis is what condemned whole anime batches
                // two rounds ago (see TestPickEpisodeFile_labelledExtrasDoNotCondemnANumberedBatch). The
                // trade is deliberate and the mis-scrape that reaches it is rare; the confident wrong answer
                // this test does forbid is not.
                let served = name_of_index(&files, index);
                assert!(served.contains("featurette"), "{name} {labelled}/{unlabelled} labelled/unlabelled: served {served:?} for a request for S03E09");
            }
        }
    }
    todo!("pending: stores_pick_test.go:700")
}

// The demoted bare tier must weigh the same candidates the tier it continues weighs. Over the whole pool
// it admitted a file naming its own, different episode — and since the guess outranks the last-resort
// largest, that file won over both copies of the episode actually asked for.
#[test]
#[ignore = "needs season.rs ambiguousEpisodeGuess (package 5) and stores/torbox.rs (package 1)"]
fn ambiguous_episode_guess_ignores_files_that_name_their_own_episode() {
    #[cfg(any())]
    fn pending() {
        // The demoted bare tier must weigh the same candidates the tier it continues weighs. Over the whole pool
        // it admitted a file naming its own, different episode — and since the guess outranks the last-resort
        // largest, that file won over both copies of the episode actually asked for.
        let files = [
            file(0, "Show.S01E12 - Part 5.mkv", 9000), // labelled, and carries a standalone 5
            file(1, "[Grp] Show - 05 [1080p].mkv", 4000),
            file(2, "[Grp] Show - 05 [720p].mkv", 900),
        ];
        assert_eq!(ambiguous_episode_guess(&files, 5), Some(1), "guess = the 1080p copy of episode 5 (index 1)");
        let target = ResolveTarget { season: Some(1), episode: Some(5), ..target_none() };
        want_pick(select_file_id(&files, &target), 1, "torbox serves episode 5, not the file that says S01E12");
        want_pick(idle_real_debrid().pick_file_id(&files, &target), 1, "realdebrid likewise");
        want_pick(idle_premiumize().pick_index(&files, &target), 1, "premiumize likewise");
    }
    todo!("pending: stores_pick_test.go:745")
}

// The bare tier's own candidate restriction, asserted where it can fail: over the whole pool the labelled
// file also matches the number, so the match is no longer unique and the tier answers nothing.
#[test]
#[ignore = "needs season.rs pickEpisodeFile (package 5)"]
fn pick_episode_file_the_bare_tier_ignores_labelled_files() {
    #[cfg(any())]
    fn pending() {
        // The bare tier's own candidate restriction, asserted where it can fail: over the whole pool the labelled
        // file also matches the number, so the match is no longer unique and the tier answers nothing.
        let files = [
            file(0, "Show.S01E12 - Part 5.mkv", 9000),
            file(1, "[Grp] Show - 05 [1080p].mkv", 4000),
        ];
        want_pick(pick_episode_file(&files, 1, 5), 1, "the one unlabelled file carrying the number is the answer");
    }
    todo!("pending: stores_pick_test.go:765")
}

// The majority rule's boundary. Equal counts must NOT condemn: the unlabelled files may be absolutely
// numbered, and a refusal short-circuits every store's picker before the indexer's position is read.
#[test]
#[ignore = "needs season.rs pickEpisodeFile (package 5)"]
fn pick_episode_file_equal_counts_defer_rather_than_condemn() {
    #[cfg(any())]
    fn pending() {
        // The majority rule's boundary. Equal counts must NOT condemn: the unlabelled files may be absolutely
        // numbered, and a refusal short-circuits every store's picker before the indexer's position is read.
        let files = [
            file(0, "Show.S01E01.1080p.mkv", 900),
            file(1, "Show.S01E02.1080p.mkv", 950),
            file(2, "[Grp] Show - 13 [1080p].mkv", 980),
            file(3, "[Grp] Show - 14 [1080p].mkv", 990),
        ];
        assert!(matches!(pick_episode_file(&files, 2, 1), Ok(None)), "two labelled and two unlabelled must defer to the indexer");
        // A strict majority of labels still condemns.
        let mut majority = files[..2].to_vec();
        majority.push(file(4, "Show.S01E03.1080p.mkv", 960));
        majority.push(file(5, "Extras/featurette.mkv", 10));
        want_not_in_torrent(pick_episode_file(&majority, 2, 1), "three labelled episodes against one extra is a labelled pack");
    }
    todo!("pending: stores_pick_test.go:776")
}

// A double episode names TWO, and only the first was ever matched. Asked for the second, nothing matched
// — and because the file DOES declare an episode it then ruled itself out of every guess below, so a
// pack of them answered 404 for half the season while the sample beside them was served for the rest.
#[test]
#[ignore = "needs season.rs (package 5) and stores/torbox.rs selectFileID (package 1)"]
fn pick_episode_file_double_episodes_name_both_of_them() {
    #[cfg(any())]
    fn pending() {
        // A double episode names TWO, and only the first was ever matched. Asked for the second, nothing matched
        // — and because the file DOES declare an episode it then ruled itself out of every guess below, so a
        // pack of them answered 404 for half the season while the sample beside them was served for the rest.
        for spelling in ["S01E05-E06", "S01E05E06", "S01E05-06", "S01E05.E06"] {
            let files = [
                file(0, &format!("Show.{spelling}.1080p.WEB-DL.mkv"), 5_000_000),
                file(1, "Sample/sample.mkv", 30_000),
            ];
            for ep_n in [5, 6] {
                want_pick(pick_episode_file(&files, 1, ep_n), 0, &format!("{spelling} holds episode {ep_n}"));
            }
            // Through the real pickers too, since that is where the sample was being served.
            for ep_n in [5, 6] {
                let target = ResolveTarget { season: Some(1), episode: Some(ep_n), ..target_none() };
                want_pick(select_file_id(&files, &target), 0, &format!("torbox serves the episode, not the sample ({spelling}, E{ep_n})"));
                want_pick(idle_real_debrid().pick_file_id(&files, &target), 0, "realdebrid likewise");
                want_pick(idle_premiumize().pick_index(&files, &target), 0, "premiumize likewise");
            }
        }

        // A whole season packed as doubles: every episode must resolve, and none may 404.
        let pack: Vec<TorrentFile> = (0..6)
            .map(|i| file(i, &format!("Show.S01E{:02}-E{:02}.1080p.mkv", i * 2 + 1, i * 2 + 2), 1000 + i))
            .collect();
        for ep_n in 1..=12 {
            want_pick(pick_episode_file(&pack, 1, ep_n), (ep_n - 1) / 2, &format!("episode {ep_n} lives in the {}th double", (ep_n - 1) / 2));
        }
        // And a genuinely absent episode is still refused rather than guessed at.
        want_not_in_torrent(pick_episode_file(&pack, 1, 21), "episode 21 is not in a pack of E01-E12");

        // The first number stays free: a single episode must not match its neighbours.
        assert!(!matches_episode("Show.S01E05.1080p.mkv", &episode_patterns(1, 6)), "a single episode must not match the episode after it");
        assert!(!matches_episode("Show.S01E10-E12.1080p.mkv", &episode_patterns(1, 1)), "E10-E12 must not match episode 1");
    }
    todo!("pending: stores_pick_test.go:796")
}

// An episode number with no season anywhere: the strong patterns cannot fire and labelledEpisodeRe does
// not see it, so every file looked identical and the largest was served for all twelve episodes.
#[test]
#[ignore = "needs season.rs pickEpisodeFile (package 5)"]
fn pick_episode_file_season_less_episode_labels() {
    #[cfg(any())]
    fn pending() {
        // An episode number with no season anywhere: the strong patterns cannot fire and labelledEpisodeRe does
        // not see it, so every file looked identical and the largest was served for all twelve episodes.
        for shape in ["Show - E%02d [1080p].mkv", "Show.E%02d.1080p.mkv", "Show_E%02d_1080p.mkv"] {
            let pack: Vec<TorrentFile> = (1..=12)
                .map(|i| file(i, &shape.replace("%02d", &format!("{i:02}")), 1000 + i))
                .collect();
            for ep_n in 1..=12 {
                want_pick(pick_episode_file(&pack, 1, ep_n), ep_n, &format!("{shape}: episode {ep_n}"));
            }
        }
        // The guards that keep the `e` form from firing on audio and codec tags, where the bare token cannot.
        let cases = [
            ("Show.S01E03.1080p.WEB-DL.DDP5.1.H.264-NTb.mkv", 1),
            ("Show.S03E07.TrueHD.7.1.Atmos-GRP.mkv", 1),
            ("Show.S03E07.AAC.2.0-GRP.mkv", 0),
            ("Show.S01E03.2160p.HEVC.x265.mkv", 265),
        ];
        for (name, episode) in cases {
            assert!(!matches_episode(name, &bare_episode_patterns(episode)), "{name:?} must not read as episode {episode}");
        }
    }
    todo!("pending: stores_pick_test.go:847")
}

// The second element of a double-episode range must look like an episode. Written as an optional `e`
// over a possibly-empty separator it accepted whatever number came next after the label, and because
// this is a STRONG match it also overrode the indexer's correct fileIdx.
#[test]
#[ignore = "needs season.rs namesEpisode (package 5)"]
fn episode_patterns_the_range_second_element_must_be_an_episode() {
    #[cfg(any())]
    fn pending() {
        // The second element of a double-episode range must look like an episode. Written as an optional `e`
        // over a possibly-empty separator it accepted whatever number came next after the label, and because
        // this is a STRONG match it also overrode the indexer's correct fileIdx.
        let cases: [(&str, i64, i64, bool, &str); 12] = [
            ("Show.S01E05-E06.1080p.mkv", 1, 6, true, "the range form it exists for"),
            ("Show.S01E05E06.1080p.mkv", 1, 6, true, "no separator"),
            ("Show.S01E05-06.1080p.mkv", 1, 6, true, "a tight dash needs no second e"),
            ("Show.S01E05.E06.1080p.mkv", 1, 6, true, "a dot separator"),
            ("Show.S1E05-E06.1080p.mkv", 1, 6, true, "an unpadded season"),
            // The false matches that made every one of these a strong match for a number in the title.
            ("Show.S01E01.7.Minutes.1080p.WEB-DL.mkv", 1, 7, false, "a title starting with a digit"),
            ("Show.S01E01.42.1080p.WEB-DL.mkv", 1, 42, false, "a title that IS a number"),
            ("Show.S01E08.4K.HDR.WEB-DL.mkv", 1, 4, false, "a 4K tag is not episode 4"),
            ("Show - S02E01 - 014 - Title.mkv", 2, 14, false, "an absolute number in its own field"),
            ("Show.S01E05-E06.1080p.mkv", 1, 50, false, "not a prefix of the second number"),
            ("Show.S01E05-E06.1080p.mkv", 2, 6, false, "the season still has to match"),
            ("Show.S01E11-E12.1080p.mkv", 1, 1, false, "no backtracking into a false split"),
        ];
        for (name, season, episode, want, why) in cases {
            let got = names_episode(name, season, episode);
            assert_eq!(got, want, "S{season:02}E{episode:02} vs {name:?} — {why}");
        }

        // End to end: a pack whose titles start with digits must still resolve each episode to its own file.
        let pack = [
            file(0, "Show.S01E01.7.Minutes.1080p.WEB-DL.mkv", 9000),
            file(1, "Show.S01E07.Real.Episode.1080p.WEB-DL.mkv", 1000),
        ];
        want_pick(pick_episode_file(&pack, 1, 7), 1, "episode 7 is the file named E07, not the one whose title starts with 7");

        // And every file in a 4K pack must not claim to be episode 4.
        let uhd: Vec<TorrentFile> = (1..=8)
            .map(|i| file(i, &format!("Show.S01E{i:02}.4K.HDR.WEB-DL.mkv"), 1000 + i))
            .collect();
        want_pick(pick_episode_file(&uhd, 1, 4), 4, "the 4K tag names no episode");
    }
    todo!("pending: stores_pick_test.go:891")
}

// A range is arithmetic, not punctuation. A tight dash was tried as the thing that made a bare second
// number safe; it is not, because every digit-initial tag joined to the label wears one. What identifies
// the form is that the number before the dash is the one just below the number after it.
#[test]
#[ignore = "needs season.rs namesEpisode (package 5)"]
fn episode_patterns_a_tight_dash_is_not_enough_to_make_a_range() {
    #[cfg(any())]
    fn pending() {
        // A range is arithmetic, not punctuation. A tight dash was tried as the thing that made a bare second
        // number safe; it is not, because every digit-initial tag joined to the label wears one. What identifies
        // the form is that the number before the dash is the one just below the number after it.
        let cases: [(&str, i64, i64, bool, &str); 11] = [
            ("Show.S01E05-06.1080p.mkv", 1, 6, true, "a real range, bare far end"),
            ("Show.S01E04-06.1080p.mkv", 1, 6, true, "a three-episode range still ends at 6"),
            ("Show.S01E05-E06.1080p.mkv", 1, 6, true, "the e-marked form is unaffected"),
            // Every one of these is a digit-initial tag or title joined tightly to the label.
            ("Show.S01E01-7.Minutes.1080p.WEB-DL.mkv", 1, 7, false, "a title starting with a digit"),
            ("Show.S01E01-42.1080p.WEB-DL.mkv", 1, 42, false, "a numeric title"),
            ("Show.S01E08-4K.HDR.WEB-DL.mkv", 1, 4, false, "a 4K tag"),
            ("Show.S01E02-60fps.mkv", 1, 60, false, "a frame-rate tag"),
            ("Show.S01E02-8bit.mkv", 1, 8, false, "a bit-depth tag"),
            ("Show.S01E02-3D.mkv", 1, 3, false, "a 3D tag"),
            ("Show - S02E01-014 - Title.mkv", 2, 14, false, "an absolute number"),
            ("Show.S01E01-2HD.mkv", 1, 2, false, "a group tag starting with a digit"),
        ];
        for (name, season, episode, want, why) in cases {
            let got = names_episode(name, season, episode);
            assert_eq!(got, want, "S{season:02}E{episode:02} vs {name:?} — {why}");
        }

        // End to end: a pack tagged -4K must not have every file claim to be episode 4, and a pack tagged
        // -60fps must still REFUSE an episode it does not hold rather than hand back a file.
        let uhd: Vec<TorrentFile> = (1..=8)
            .map(|i| file(i, &format!("Show.S01E{i:02}-4K.HDR.WEB-DL.mkv"), 1000 + i))
            .collect();
        want_pick(pick_episode_file(&uhd, 1, 4), 4, "the 4K tag names no episode");

        let fps: Vec<TorrentFile> = (1..=6)
            .map(|i| file(i, &format!("Show.S01E{i:02}-60fps.mkv"), 1000 + i))
            .collect();
        want_not_in_torrent(pick_episode_file(&fps, 1, 60), "a pack of E01-E06 does not hold episode 60");
    }
    todo!("pending: stores_pick_test.go:939")
}

// A bare range wider than a double episode is an ordinary release, and refusing it condemns a pack that
// plays: the refusal short-circuits every store's picker before the indexer's fileIdx is read, so /play
// answers 404 for a file sitting right there. What excludes the tags is not how WIDE the span is but
// that both ends are written the same way and the far end stops at a separator.
#[test]
#[ignore = "needs season.rs namesEpisode (package 5)"]
fn episode_patterns_bare_ranges_span_a_whole_season() {
    #[cfg(any())]
    fn pending() {
        // A bare range wider than a double episode is an ordinary release, and refusing it condemns a pack that
        // plays: the refusal short-circuits every store's picker before the indexer's fileIdx is read, so /play
        // answers 404 for a file sitting right there. What excludes the tags is not how WIDE the span is but
        // that both ends are written the same way and the far end stops at a separator.
        let cases: [(&str, i64, bool, &str); 16] = [
            ("Show.S01E05-06.1080p.mkv", 6, true, "a double"),
            ("Show.S01E01-06.1080p.mkv", 6, true, "six episodes in one file"),
            ("Show.S01E01-08.1080p.mkv", 8, true, "eight"),
            ("Show.S01E01-12.1080p.mkv", 12, true, "a whole season"),
            ("Show.S1E5-6.1080p.mkv", 6, true, "unpadded on both ends"),
            ("Show.S01E01-E12.1080p.mkv", 12, true, "the e-marked form spans freely"),
            // Mixed widths are how a tag or a title gives itself away.
            ("Show.S01E01-7.Minutes.1080p.WEB-DL.mkv", 7, false, "a title starting with a digit"),
            ("Show.S01E07-8.Mile.mkv", 8, false, "consecutive, but mixed widths"),
            ("Show.S01E01-2.Broke.Girls.1080p.mkv", 2, false, "likewise"),
            ("Show.S01E04-5.1.AC3.mkv", 5, false, "an audio tag"),
            ("Show - S02E01-014 - Title.mkv", 14, false, "an absolute number is three wide"),
            ("Show.S01E01-42.1080p.WEB-DL.mkv", 42, false, "no release packs 42 episodes in one file"),
            // And the terminator still does its half.
            ("Show.S01E02-3D.mkv", 3, false, "a 3D tag"),
            ("Show.S01E01-2HD.mkv", 2, false, "a group tag"),
            ("Show.S01E08-4K.HDR.WEB-DL.mkv", 4, false, "a 4K tag"),
            ("Show.S01E02-60fps.mkv", 60, false, "a frame-rate tag"),
        ];
        for (name, episode, want, why) in cases {
            // The S02 fixture is the only one in season 2.
            let season = if name.as_bytes()[7] == b'2' { 2 } else { 1 };
            let got = names_episode(name, season, episode);
            assert_eq!(got, want, "episode {episode} vs {name:?} — {why}");
        }

        // End to end: a pack of six-episode files must resolve every episode it holds, and still refuse one
        // it does not.
        let pack: Vec<TorrentFile> = (0..2)
            .map(|i| file(i, &format!("Show.S01E{:02}-{:02}.1080p.mkv", i * 6 + 1, i * 6 + 6), 1000 + i))
            .collect();
        for ep_n in 1..=12 {
            want_pick(pick_episode_file(&pack, 1, ep_n), (ep_n - 1) / 6, &format!("episode {ep_n} lives in file {}", (ep_n - 1) / 6));
        }
        want_not_in_torrent(pick_episode_file(&pack, 1, 20), "episode 20 is not in a pack of E01-E12");
    }
    todo!("pending: stores_pick_test.go:987")
}

// A range is MEMBERSHIP, not two endpoints. Anchoring only the far end matched episodes 1 and 6 of
// `S01E01-06.mkv` and none of the four between, so a pack of such files was condemned for two thirds of
// its episodes — and that refusal short-circuits every picker before the indexer's fileIdx is read.
#[test]
#[ignore = "needs season.rs namesEpisode (package 5)"]
fn names_episode_a_range_holds_every_episode_in_it() {
    #[cfg(any())]
    fn pending() {
        // A range is MEMBERSHIP, not two endpoints. Anchoring only the far end matched episodes 1 and 6 of
        // `S01E01-06.mkv` and none of the four between, so a pack of such files was condemned for two thirds of
        // its episodes — and that refusal short-circuits every picker before the indexer's fileIdx is read.
        for ep_n in 1..=6 {
            assert!(names_episode("Show.S01E01-06.1080p.mkv", 1, ep_n), "S01E01-06 holds episode {ep_n}");
        }
        for ep_n in [7, 12, 42] {
            assert!(!names_episode("Show.S01E01-06.1080p.mkv", 1, ep_n), "S01E01-06 does not hold episode {ep_n}");
        }
        // The e-marked form spans freely and is likewise membership.
        for ep_n in 1..=12 {
            assert!(names_episode("Show.S01E01-E12.1080p.mkv", 1, ep_n), "S01E01-E12 holds episode {ep_n}");
        }
        // The season still has to match, and a range cannot run backwards.
        assert!(!names_episode("Show.S01E01-06.1080p.mkv", 2, 3), "an S01 range says nothing about season 2");
        assert!(!names_episode("Show.S01E06-01.1080p.mkv", 1, 3), "a backwards range is not a range");

        // Every episode of a six-per-file pack resolves, through the real picker.
        let pack: Vec<TorrentFile> = (0..2)
            .map(|i| file(i, &format!("Show.S01E{:02}-{:02}.1080p.mkv", i * 6 + 1, i * 6 + 6), 1000 + i))
            .collect();
        for ep_n in 1..=12 {
            want_pick(pick_episode_file(&pack, 1, ep_n), (ep_n - 1) / 6, &format!("episode {ep_n} lives in file {}", (ep_n - 1) / 6));
        }
        want_not_in_torrent(pick_episode_file(&pack, 1, 20), "episode 20 is not in a pack of E01-E12");
    }
    todo!("pending: stores_pick_test.go:1040")
}

// Sonarr writes six multi-episode styles and three of them chain past two numbers. Capturing exactly one
// far end read those as ending at the SECOND number, so the third episode matched nothing — and since
// the file is labelled, that miss became a hard refusal: a third of a playing release answered 404.
#[test]
#[ignore = "needs season.rs (package 5) and stores/torbox.rs selectFileID (package 1)"]
fn names_episode_reads_every_episode_in_a_chain() {
    #[cfg(any())]
    fn pending() {
        // Sonarr writes six multi-episode styles and three of them chain past two numbers. Capturing exactly one
        // far end read those as ending at the SECOND number, so the third episode matched nothing — and since
        // the file is labelled, that miss became a hard refusal: a third of a playing release answered 404.
        let shapes = [
            ("Show - S01E01-02-03 - Title.mkv", "Extend"),
            ("Show - S01E01E02E03 - Title.mkv", "Repeat"),
            ("Show - S01E01-E02-E03 - Title.mkv", "Scene"),
            ("Show - S01E01-03 - Title.mkv", "Range"),
            ("Show - S01E01-E03 - Title.mkv", "PrefixedRange"),
        ];
        for (name, style) in shapes {
            for ep_n in 1..=3 {
                assert!(names_episode(name, 1, ep_n), "{style}: {name:?} does not name episode {ep_n}");
            }
            for ep_n in [4, 9, 42] {
                assert!(!names_episode(name, 1, ep_n), "{style}: {name:?} must not name episode {ep_n}");
            }
        }

        // A LIST names exactly what it lists; only a two-number range fills in the middle.
        assert!(!names_episode("Show - S01E01E02E09 - Title.mkv", 1, 5), "a list of 1, 2 and 9 does not hold episode 5");
        assert!(names_episode("Show - S01E01E02E09 - Title.mkv", 1, 9), "a list of 1, 2 and 9 does hold episode 9");

        // A chain whose bare element is written differently is a tag, not a chain.
        assert!(!names_episode("Show.S01E01-2-3D.mkv", 1, 3), "a mixed-width chain is not an episode list");

        // End to end: every episode of a three-per-file pack resolves, on all three stores.
        let pack: Vec<TorrentFile> = (0..4)
            .map(|i| file(i, &format!("Show.S01E{:02}E{:02}E{:02}.1080p.mkv", i * 3 + 1, i * 3 + 2, i * 3 + 3), 1000 + i))
            .collect();
        for ep_n in 1..=12 {
            let want = (ep_n - 1) / 3;
            want_pick(pick_episode_file(&pack, 1, ep_n), want, &format!("episode {ep_n} lives in file {want}"));
            let target = ResolveTarget { season: Some(1), episode: Some(ep_n), ..target_none() };
            want_pick(select_file_id(&pack, &target), want, "torbox");
            want_pick(idle_real_debrid().pick_file_id(&pack, &target), want, "realdebrid");
            want_pick(idle_premiumize().pick_index(&pack, &target), want, "premiumize");
        }
        want_not_in_torrent(pick_episode_file(&pack, 1, 20), "episode 20 is not in a pack of E01-E12");
    }
    todo!("pending: stores_pick_test.go:1081")
}

// The compiled pattern set is shared across picks, and the thing to prove about any cache is not that
// it is fast — it is that it hands back the set that was asked for. A cache keyed too loosely would
// answer episode 3 with episode 2's patterns, which is the whole pick decided wrong, silently.
#[test]
#[ignore = "needs season.rs cachedEpisodePatterns (package 5)"]
fn cached_episode_patterns_answers_for_the_episode_asked_about() {
    #[cfg(any())]
    fn pending() {
        // The compiled pattern set is shared across picks, and the thing to prove about any cache is not that
        // it is fast — it is that it hands back the set that was asked for. A cache keyed too loosely would
        // answer episode 3 with episode 2's patterns, which is the whole pick decided wrong, silently.
        //
        // The same question, twice, gets literally the same compiled set — that IS the memo.
        let first = cached_episode_patterns(4, 7);
        let second = cached_episode_patterns(4, 7);
        assert!(std::ptr::eq(&first[0], &second[0]), "the same episode compiled its patterns twice");

        // And every neighbouring key stays its own answer. Each of these differs from (4,7) in one number,
        // so a cache that drops either from the key hands back the wrong set here.
        let keys = [(4, 7), (4, 8), (5, 7), (3, 7)];
        for (season, episode) in keys {
            let patterns = cached_episode_patterns(season, episode);
            let mine = format!("Show.S{season:02}E{episode:02}.1080p.mkv");
            assert!(matches_episode(&mine, &patterns), "s{season}e{episode}'s patterns do not match {mine:?}");
            for (other_season, other_episode) in keys {
                if (other_season, other_episode) == (season, episode) {
                    continue;
                }
                let theirs = format!("Show.S{other_season:02}E{other_episode:02}.1080p.mkv");
                assert!(!matches_episode(&theirs, &patterns), "s{season}e{episode}'s patterns also claimed {theirs:?}");
            }
        }

        // Past the cap the map is dropped whole. The pick that lands on a FULL cache is the one to check,
        // and it has to be an episode not already held — re-asking a cached key is answered by the hit
        // above the cap branch, which proves nothing about it. The reset may cost a compile, never a wrong
        // set.
        for i in 0..EPISODE_PATTERN_CAP + 2 {
            cached_episode_patterns(90, i);
        }
        assert!(matches_episode("Show.S06E11.1080p.mkv", &cached_episode_patterns(6, 11)),
            "the pick that filled the cache was answered with someone else's patterns");
    }
    todo!("pending: stores_pick_test.go:1139")
}

// unresolvedSeasonPack: a title that never names this episode, with the season either generic or named
// only by a range that happens to span it, is a pack whatever its declared size claims. A title that
// does name this episode — plainly or by an explicit list — is trusted exactly as namesEpisode trusts it.
#[test]
#[ignore = "needs season.rs unresolvedSeasonPack (package 5)"]
fn unresolved_season_pack() {
    #[cfg(any())]
    fn pending() {
        // unresolvedSeasonPack: a title that never names this episode, with the season either generic or named
        // only by a range that happens to span it, is a pack whatever its declared size claims. A title that
        // does name this episode — plainly or by an explicit list — is trusted exactly as namesEpisode trusts it.
        let cases = [
            ("bare season, no episode at all", "Fauda.S01.COMPLETE.1080p.NF.WEB-DL", true),
            ("worded season, no episode", "Fauda.Season.1.1080p.BluRay", true),
            ("a range spanning this episode", "Fauda.S01E01-12.1080p.NF.WEB-DL.ITA-ENG-HEB.AAC2.0.H.265-G66", true),
            ("a range NOT spanning this episode", "Fauda.S02E01-12.1080p.NF.WEB-DL", false),
            ("names exactly this episode", "Fauda.S01E03.WEB-DL.1080p.mkv", false),
            ("names exactly this episode, HEBREW tagged", "Fauda (2015) S01E03 (1080p HEBREW NF WEB-DL x265) [UTR].mkv", false),
            ("an explicit double naming this one", "Fauda.S01E02E03.1080p.mkv", false),
            ("a movie, unrelated to any season wording", "Some.Other.Movie.2019.1080p.mkv", false),
        ];
        for (name, title, want) in cases {
            let got = unresolved_season_pack(title, 1, 3);
            assert_eq!(got, want, "{name}: unresolvedSeasonPack({title:?}, 1, 3) = {got}, want {want}");
        }
    }
    todo!("pending: stores_pick_test.go:1180")
}

// s01e03 itself must never trip the bare season pattern: there is no boundary between a season's digits
// and an episode marker glued straight onto them.
#[test]
#[ignore = "needs season.rs seasonOnlyPattern (package 5)"]
fn season_only_pattern_does_not_fire_inside_an_episode_marker() {
    #[cfg(any())]
    fn pending() {
        // s01e03 itself must never trip the bare season pattern: there is no boundary between a season's digits
        // and an episode marker glued straight onto them.
        assert!(!season_only_pattern(1).is_match("fauda.s01e03.web-dl.1080p.mkv"), r#""s01e03" matched the bare-season pattern for season 1"#);
        assert!(season_only_pattern(1).is_match("fauda.s01.complete.1080p"), r#""s01" alone did not match the bare-season pattern for season 1"#);
    }
    todo!("pending: stores_pick_test.go:1204")
}
