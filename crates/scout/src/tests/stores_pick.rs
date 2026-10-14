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
    todo!("port stores_pick_test.go:51 once package 5 lands")
}

// The fallback survives where it is actually needed: a release whose filename names no episode at all is
// a single episode, and its one video is the only thing it could be. Killing this to fix the case above
// would break every untagged single-episode release.
#[test]
#[ignore = "needs season.rs pickEpisodeFile (package 5)"]
fn pick_episode_file_unlabelled_still_falls_back() {
    todo!("port stores_pick_test.go:72 once package 5 lands")
}

// Samples and extras carry the episode number too. The real episode is the big file.
#[test]
#[ignore = "needs season.rs pickEpisodeFile (package 5)"]
fn pick_episode_file_prefers_the_largest_match() {
    todo!("port stores_pick_test.go:85 once package 5 lands")
}

// Non-video files (nfo, subs, artwork) are excluded — but a pack of ONLY non-video must still be
// searchable rather than answering nil, or a legitimate odd container becomes unplayable.
#[test]
#[ignore = "needs season.rs pickEpisodeFile (package 5)"]
fn pick_episode_file_videos_first_then_anything() {
    todo!("port stores_pick_test.go:96 once package 5 lands")
}

// TorBox: name match first, then the caller's index, and an out-of-range index is handed back untouched
// (it is the debrid's own id space, not a slice offset).
#[test]
#[ignore = "needs stores/torbox.rs selectFileID (package 1)"]
fn select_file_id_torbox() {
    todo!("port stores_pick_test.go:117 once package 1 lands")
}

// Real-Debrid: same name-match precedence, but an unusable index falls back to the FIRST file rather
// than to nothing — RD always needs a concrete file id to unrestrict.
#[test]
#[ignore = "needs season.rs errEpisodeNotInTorrent (package 5)"]
fn pick_file_id_real_debrid() {
    todo!("port stores_pick_test.go:148 once package 5 lands")
}

// Premiumize: an unusable index falls back to the LARGEST file — with no id space of its own, the
// biggest file in a pack is the best guess at the feature.
#[test]
#[ignore = "needs season.rs errEpisodeNotInTorrent (package 5)"]
fn pick_index_premiumize() {
    todo!("port stores_pick_test.go:174 once package 5 lands")
}

// A negative index is untrusted input from an addon, not an offset — it must never index backwards.
#[test]
#[ignore = "needs stores/torbox.rs selectFileID (package 1)"]
fn pickers_reject_negative_index() {
    todo!("port stores_pick_test.go:199 once package 1 lands")
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
    todo!("port stores_pick_test.go:222 once package 1 lands")
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
    todo!("port stores_pick_test.go:258 once packages 1 and 5 land")
}

// A pack numbered the way most anime and much TV is packed: the episode is found by its own number.
//
// A miss, though, is NOT evidence the pack lacks the episode — bare numbering is often absolute, so
// season 2 episode 1 is packed as `- 13` and nothing in the name says so. Only an unambiguous SxxExx
// label, which carries its season, can condemn a pack; a bare miss defers to the indexer's file index.
#[test]
#[ignore = "needs season.rs pickEpisodeFile (package 5)"]
fn pick_episode_file_bare_numbered_packs() {
    todo!("port stores_pick_test.go:297 once package 5 lands")
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
    todo!("port stores_pick_test.go:319 once package 5 lands")
}

// The two failures the boundaries above prevent, driven through the real pickers.
#[test]
#[ignore = "needs season.rs pickEpisodeFile (package 5)"]
fn pick_episode_file_modern_pack_names_are_not_all_episode_one() {
    todo!("port stores_pick_test.go:355 once package 5 lands")
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
    todo!("port stores_pick_test.go:391 once package 5 lands")
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
    todo!("port stores_pick_test.go:432 once package 1 lands")
}

// The same gap on the other two, so the three are pinned alike.
#[test]
#[ignore = "needs stores/torbox.rs selectFileID (package 1)"]
fn pickers_map_position_to_the_stores_own_id() {
    todo!("port stores_pick_test.go:453 once package 1 lands")
}

// All three stores hand back PATHS, so the torrent's root directory is part of every file's name — and
// no fixture in this suite was path-shaped, which is why the directory could speak for every file.
// A directory describes the pack; only the filename describes the file.
#[test]
#[ignore = "needs season.rs pickEpisodeFile (package 5)"]
fn pick_episode_file_the_directory_is_not_evidence_about_the_file() {
    todo!("port stores_pick_test.go:466 once package 5 lands")
}

// A number that matches EVERY candidate is part of what they share — the show's own title — not what
// tells them apart. Serving the largest of "all of them" is how a request for episode 1 of Stargate
// SG-1 returned episode 2.
#[test]
#[ignore = "needs season.rs pickEpisodeFile (package 5)"]
fn pick_episode_file_a_title_number_is_not_an_episode() {
    todo!("port stores_pick_test.go:501 once package 5 lands")
}

// One labelled file must not switch the weak tier off for the rest of the pack. An anime batch commonly
// carries a single labelled special beside bare-numbered episodes; treating the pool as labelled turned
// a hit into a hard refusal, and the client records a release that plays as dead.
#[test]
#[ignore = "needs season.rs pickEpisodeFile (package 5)"]
fn pick_episode_file_one_labelled_file_does_not_condemn_the_rest() {
    todo!("port stores_pick_test.go:530 once package 5 lands")
}

// One stray file cannot unmake a pack, and cannot rescue a number that decides nothing. Both guards used
// to be phrased as set-size comparisons over the pool, so a single extra video — a sample, a featurette,
// a bonus rip, all of them ordinary in a season pack and all of them videos — was enough to disarm each.
#[test]
#[ignore = "needs season.rs pickEpisodeFile (package 5)"]
fn pick_episode_file_one_stray_file_disarms_nothing() {
    todo!("port stores_pick_test.go:558 once package 5 lands")
}

// A backslash is legal in a POSIX filename, and these names come from the torrent. Splitting on it
// unconditionally threw away the part of the name that carries the label.
#[test]
#[ignore = "needs season.rs baseName (package 5)"]
fn base_name_only_splits_where_a_directory_really_ends() {
    todo!("port stores_pick_test.go:605 once package 5 lands")
}

// An anime batch numbered absolutely, with a couple of labelled OVAs alongside — the shape the refusal
// must never fire on. The bare tier cannot match S02E01 against `- 13`, so control reaches the refusal,
// and because the error short-circuits every store's picker the indexer's correct fileIdx is never read:
// a release that plays, answered 404 and recorded dead by the client.
#[test]
#[ignore = "needs season.rs (package 5) and stores/torbox.rs selectFileID (package 1)"]
fn pick_episode_file_labelled_extras_do_not_condemn_a_numbered_batch() {
    todo!("port stores_pick_test.go:632 once packages 1 and 5 land")
}

// A number naming several files does not tell them apart — but on a dual-quality pack those several ARE
// the episode, and the better copy is the answer. Discarding the match sent all three stores to "largest
// video anywhere in the pack", which is a different episode.
#[test]
#[ignore = "needs season.rs (package 5) and stores/torbox.rs selectFileID (package 1)"]
fn pickers_a_duplicate_number_is_still_the_episode() {
    todo!("port stores_pick_test.go:665 once packages 1 and 5 land")
}

// A file that names an episode has named a DIFFERENT one by the time any guess is reached, so it can
// never be the answer, whatever its size. Counting rules decide whether a pack is condemned and there is
// always a band they miss — labelled files present but not a majority — where control used to reach
// "the largest video anywhere", pick a file whose filename says S01E02, and serve it for a request for
// S03E09 with a 302 and no error.
#[test]
#[ignore = "needs season.rs (package 5) and stores/torbox.rs selectFileID (package 1)"]
fn pickers_never_serve_a_file_that_names_a_different_episode() {
    todo!("port stores_pick_test.go:700 once packages 1 and 5 land")
}

// The demoted bare tier must weigh the same candidates the tier it continues weighs. Over the whole pool
// it admitted a file naming its own, different episode — and since the guess outranks the last-resort
// largest, that file won over both copies of the episode actually asked for.
#[test]
#[ignore = "needs season.rs ambiguousEpisodeGuess (package 5) and stores/torbox.rs (package 1)"]
fn ambiguous_episode_guess_ignores_files_that_name_their_own_episode() {
    todo!("port stores_pick_test.go:745 once packages 1 and 5 land")
}

// The bare tier's own candidate restriction, asserted where it can fail: over the whole pool the labelled
// file also matches the number, so the match is no longer unique and the tier answers nothing.
#[test]
#[ignore = "needs season.rs pickEpisodeFile (package 5)"]
fn pick_episode_file_the_bare_tier_ignores_labelled_files() {
    todo!("port stores_pick_test.go:765 once package 5 lands")
}

// The majority rule's boundary. Equal counts must NOT condemn: the unlabelled files may be absolutely
// numbered, and a refusal short-circuits every store's picker before the indexer's position is read.
#[test]
#[ignore = "needs season.rs pickEpisodeFile (package 5)"]
fn pick_episode_file_equal_counts_defer_rather_than_condemn() {
    todo!("port stores_pick_test.go:776 once package 5 lands")
}

// A double episode names TWO, and only the first was ever matched. Asked for the second, nothing matched
// — and because the file DOES declare an episode it then ruled itself out of every guess below, so a
// pack of them answered 404 for half the season while the sample beside them was served for the rest.
#[test]
#[ignore = "needs season.rs (package 5) and stores/torbox.rs selectFileID (package 1)"]
fn pick_episode_file_double_episodes_name_both_of_them() {
    todo!("port stores_pick_test.go:796 once packages 1 and 5 land")
}

// An episode number with no season anywhere: the strong patterns cannot fire and labelledEpisodeRe does
// not see it, so every file looked identical and the largest was served for all twelve episodes.
#[test]
#[ignore = "needs season.rs pickEpisodeFile (package 5)"]
fn pick_episode_file_season_less_episode_labels() {
    todo!("port stores_pick_test.go:847 once package 5 lands")
}

// The second element of a double-episode range must look like an episode. Written as an optional `e`
// over a possibly-empty separator it accepted whatever number came next after the label, and because
// this is a STRONG match it also overrode the indexer's correct fileIdx.
#[test]
#[ignore = "needs season.rs namesEpisode (package 5)"]
fn episode_patterns_the_range_second_element_must_be_an_episode() {
    todo!("port stores_pick_test.go:891 once package 5 lands")
}

// A range is arithmetic, not punctuation. A tight dash was tried as the thing that made a bare second
// number safe; it is not, because every digit-initial tag joined to the label wears one. What identifies
// the form is that the number before the dash is the one just below the number after it.
#[test]
#[ignore = "needs season.rs namesEpisode (package 5)"]
fn episode_patterns_a_tight_dash_is_not_enough_to_make_a_range() {
    todo!("port stores_pick_test.go:939 once package 5 lands")
}

// A bare range wider than a double episode is an ordinary release, and refusing it condemns a pack that
// plays: the refusal short-circuits every store's picker before the indexer's fileIdx is read, so /play
// answers 404 for a file sitting right there. What excludes the tags is not how WIDE the span is but
// that both ends are written the same way and the far end stops at a separator.
#[test]
#[ignore = "needs season.rs namesEpisode (package 5)"]
fn episode_patterns_bare_ranges_span_a_whole_season() {
    todo!("port stores_pick_test.go:987 once package 5 lands")
}

// A range is MEMBERSHIP, not two endpoints. Anchoring only the far end matched episodes 1 and 6 of
// `S01E01-06.mkv` and none of the four between, so a pack of such files was condemned for two thirds of
// its episodes — and that refusal short-circuits every picker before the indexer's fileIdx is read.
#[test]
#[ignore = "needs season.rs namesEpisode (package 5)"]
fn names_episode_a_range_holds_every_episode_in_it() {
    todo!("port stores_pick_test.go:1040 once package 5 lands")
}

// Sonarr writes six multi-episode styles and three of them chain past two numbers. Capturing exactly one
// far end read those as ending at the SECOND number, so the third episode matched nothing — and since
// the file is labelled, that miss became a hard refusal: a third of a playing release answered 404.
#[test]
#[ignore = "needs season.rs (package 5) and stores/torbox.rs selectFileID (package 1)"]
fn names_episode_reads_every_episode_in_a_chain() {
    todo!("port stores_pick_test.go:1081 once packages 1 and 5 land")
}

// The compiled pattern set is shared across picks, and the thing to prove about any cache is not that
// it is fast — it is that it hands back the set that was asked for. A cache keyed too loosely would
// answer episode 3 with episode 2's patterns, which is the whole pick decided wrong, silently.
#[test]
#[ignore = "needs season.rs cachedEpisodePatterns (package 5)"]
fn cached_episode_patterns_answers_for_the_episode_asked_about() {
    todo!("port stores_pick_test.go:1139 once package 5 lands")
}

// unresolvedSeasonPack: a title that never names this episode, with the season either generic or named
// only by a range that happens to span it, is a pack whatever its declared size claims. A title that
// does name this episode — plainly or by an explicit list — is trusted exactly as namesEpisode trusts it.
#[test]
#[ignore = "needs season.rs unresolvedSeasonPack (package 5)"]
fn unresolved_season_pack() {
    todo!("port stores_pick_test.go:1180 once package 5 lands")
}

// s01e03 itself must never trip the bare season pattern: there is no boundary between a season's digits
// and an episode marker glued straight onto them.
#[test]
#[ignore = "needs season.rs seasonOnlyPattern (package 5)"]
fn season_only_pattern_does_not_fire_inside_an_episode_marker() {
    todo!("port stores_pick_test.go:1204 once package 5 lands")
}
