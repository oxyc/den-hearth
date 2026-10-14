//! Go: `internal/scout/rank_show_test.go` (66 lines). Work package 5.

use crate::rank::{rank_streams, title_overlap, title_tokens, RankFilters, RawStream};

use super::rank::{rs, titles};

// Pokémon S1E1 as the indexers answered it on 2026-10-11, and the Horizons release Den Web auto-played.
// Every name here is from that list; the only thing invented is which ones are cached.
#[test]
fn rank_streams_a_spin_off_or_another_episode_sinks_below_the_show() {
    let cached = |s: &mut RawStream| {
        s.cached = true;
        s.cache_known = true;
    };
    let streams = [
        rs("Pokemon.Horizons.The.Series.S01E01.The.Pendant.That.Starts.It.All.Part.One.1080p.NF.WEB-DL.AAC2.0.H.264-VARYG.mkv", cached),
        rs("Pokemon.Master.Journeys.The.Series.S01E01.to.Train.or.Not.to.Train!.1080p.NF.WEB-DL.DDP2.0.x264-NanDesuKa.mkv", cached),
        rs("www.UIndex.org - Pokemon S00E82 Distant Blue Sky 1080p AMZN WEB-DL DDP2 0 H 264-Kitsune", cached),
        rs("Pokémon Horizons - 01 (1920x1080 - Cartoon Network CA).mkv", cached),
        rs("Pokémon (1997) - S01E01 - Pokémon - I Choose You! (1080p BluRay x265 ImE).mkv", |_| {}),
        rs("Pokemon - Temporada 1 - 001.mp4", |_| {}),
    ];
    let ranked = rank_streams(
        &streams,
        RankFilters { result_cap: 10, show_tokens: title_tokens("Pokémon"), episode: Some([1, 1]), ..Default::default() },
    );
    let got = titles(&ranked);
    assert_eq!(got[0], streams[4].title, "the 1997 show's own S01E01 must lead, uncached or not; got {got:?}");
    assert_eq!(got[1], streams[5].title, "a season word and number between show and episode name neither; got {got:?}");
}

#[test]
fn other_show() {
    let expected = title_tokens("Pokémon");
    let cases = [
        ("Pokemon.Horizons.The.Series.S01E01.1080p.mkv", true),
        ("Pokémon Horizons - 01 (1920x1080).mkv", true),
        // a season name reads as a spin-off: sunk, not dropped
        ("[DragsterPS] Pokémon Indigo League E01 [1080p].mkv", true),
        ("Pokémon (1997) - S01E01 - Pokémon - I Choose You.mkv", false),
        ("Pokemon S01E01 Pokemon, I Choose You!.mkv", false),
        // the marker comes first: nothing to judge
        ("EP01 Pokémon! Minä valitsen sinut!.mp4", false),
        // no marker this reads
        ("[PM]Pocket_Monsters_Best_Wishes_001.mkv", false),
    ];
    for (title, want) in cases {
        let got = crate::rank::other_show(title, &expected);
        assert_eq!(got, want, "otherShow({title:?}) = {got}, want {want}");
    }
}

#[test]
fn other_episode() {
    let cases = [
        ("Pokemon S00E82 Distant Blue Sky 1080p", true),
        ("Pokemon.S01E01.1080p.mkv", false),
        // a range holding it names it
        ("Pokemon.S01E01-E10.1080p", false),
        // a pack labels no episode
        ("Pokemon.S01.Complete.1080p", false),
        ("Pokemon - 001.mkv", false),
    ];
    for (title, want) in cases {
        let got = crate::rank::other_episode(title, 1, 1);
        assert_eq!(got, want, "otherEpisode({title:?}) = {got}, want {want}");
    }
}

#[test]
fn title_tokens_folds_diacritics() {
    assert!(
        title_overlap("Pokemon.S04E25.From.Ghost.to.Ghost.mkv", &title_tokens("Pokémon")),
        "Pokémon and Pokemon are the same word"
    );
}
