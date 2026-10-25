//! Go: `internal/scout/rank_show_test.go` (66 lines).

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
        RankFilters {
            result_cap: 10,
            show_tokens: title_tokens("Pokémon"),
            episode: Some([1, 1]),
            ..Default::default()
        },
    );
    let got = titles(&ranked);
    assert_eq!(
        got[0], streams[4].title,
        "the 1997 show's own S01E01 must lead, uncached or not; got {got:?}"
    );
    assert_eq!(
        got[1], streams[5].title,
        "a season word and number between show and episode name neither; got {got:?}"
    );
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

// Jordskott S01E10 (tt2309405) as the indexers answered it on 2026-10-25, while TorBox's cache check was down so
// nothing was known cached. The Italian dub was served first and auto-played; the Swedish original came last, sunk
// as "another show" because "Säsong" was not read as the word for season. Names and sizes are from that night's
// rank line.
fn jordskott_s01e10() -> [RawStream; 4] {
    let gb = |tenths: i64| move |s: &mut RawStream| s.size_bytes = Some(tenths * crate::rank::GIB / 10);
    [
        rs("Jordskott.S01E10.Rivelazione.720p.HDTVMux.ITA.x264-BlackBit.mkv", gb(13)),
        rs("Jordskott.S01E10.VOSTFR.WEBRip.x264.mp4", gb(4)),
        rs("Тайны.Сильверхёйда.S01E10.mkv", gb(15)),
        rs("Säsong 1 - Avsnitt 10.mkv", gb(13)),
    ]
}

fn jordskott_filters(languages: &[&str]) -> RankFilters {
    RankFilters {
        result_cap: 10,
        show_tokens: title_tokens("Jordskott"),
        episode: Some([1, 10]),
        languages: languages.iter().map(|l| (*l).to_owned()).collect(),
        ..Default::default()
    }
}

#[test]
fn rank_streams_the_original_language_release_leads_and_a_foreign_dub_sinks() {
    let streams = jordskott_s01e10();
    // Cinemeta lists Jordskott as "Sweden, Finland, United Kingdom, Norway".
    let got = titles(&rank_streams(&streams, jordskott_filters(&["sv", "fi", "en", "no"])));
    assert_eq!(
        got,
        [
            "Säsong 1 - Avsnitt 10.mkv",
            // Swedish audio with French subtitles: below the untagged original, above any dub.
            "Jordskott.S01E10.VOSTFR.WEBRip.x264.mp4",
            "Jordskott.S01E10.Rivelazione.720p.HDTVMux.ITA.x264-BlackBit.mkv",
            "Тайны.Сильверхёйда.S01E10.mkv",
        ],
        "the Swedish original must lead a Swedish series, and the Italian and Russian dubs sink"
    );
}

#[test]
fn rank_streams_a_dub_outranks_nothing_even_when_it_alone_is_cached() {
    let mut streams = jordskott_s01e10();
    streams[0].cached = true;
    streams[0].cache_known = true;
    let got = titles(&rank_streams(&streams, jordskott_filters(&["sv", "fi", "en", "no"])));
    assert_eq!(
        got[0], "Säsong 1 - Avsnitt 10.mkv",
        "a cached Italian dub must not outrank the original: {got:?}"
    );
}

#[test]
fn rank_streams_with_no_known_original_language_judges_no_tag() {
    // Without an original language, an ITA tag may be the original (an Italian film), so nothing sinks for it.
    let ranked = rank_streams(&jordskott_s01e10(), jordskott_filters(&[]));
    assert_eq!(titles(&ranked)[0], "Jordskott.S01E10.Rivelazione.720p.HDTVMux.ITA.x264-BlackBit.mkv");
}

#[test]
fn rank_streams_an_italian_titles_own_ita_release_is_not_a_dub() {
    let streams = [
        rs("La.Grande.Bellezza.2013.ITA.1080p.BluRay.x264.mkv", |_| {}),
        rs("La.Grande.Bellezza.2013.FRENCH.1080p.BluRay.x264.mkv", |_| {}),
        rs("La.Grande.Bellezza.2013.ITA.ENG.720p.BluRay.x264.mkv", |_| {}),
        rs("La.Grande.Bellezza.2013.MULTi.720p.WEB.x264.mkv", |_| {}),
    ];
    let got = titles(&rank_streams(
        &streams,
        RankFilters {
            result_cap: 10,
            expected_title_tokens: title_tokens("La grande bellezza"),
            languages: ["it".to_owned()].into(),
            ..Default::default()
        },
    ));
    assert_eq!(
        got.last().map(String::as_str),
        Some(streams[1].title.as_str()),
        "only the French dub sinks: {got:?}"
    );
}

#[test]
fn other_show_reads_season_words_in_the_languages_releases_use() {
    let expected = title_tokens("Jordskott");
    for title in ["Säsong 1 - Avsnitt 10.mkv", "Jordskott Sesong 1 - 10.mkv", "Jordskott Kausi 1 - 10.mkv"] {
        assert!(!crate::rank::other_show(title, &expected), "{title:?} names no other show");
    }
}

#[test]
fn title_tokens_folds_diacritics() {
    assert!(
        title_overlap("Pokemon.S04E25.From.Ghost.to.Ghost.mkv", &title_tokens("Pokémon")),
        "Pokémon and Pokemon are the same word"
    );
}
