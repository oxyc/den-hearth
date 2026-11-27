//! Language tags and the audio track a session plays. The comparison lives in `den_media::lang` and the pick is
//! den-core's `audio_pick`, which Scout picks its TorBox audio track with too, as do the Apple TV and iPhone players;
//! this adds the display name, which needs Remux's own placeholder tag.

use crate::probe::AudioTrack;

pub use den_media::lang::canonical;

/// Why `pick_audio` chose the track it did — for the one log line a session writes naming its audio
/// track, never for the decision itself.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AudioReason {
    /// The player named the track by index, from an earlier session's `audioTracks`.
    Explicit,
    /// It is in this preferred language (most wanted first in `Want::audio`), the best-ranked one a
    /// track is in.
    Preferred(String),
    /// No preference matched, and this unlabelled track was taken over one tagged with a language nobody
    /// asked for — more often the original than the tagged dub is.
    Unlabelled,
    /// No preference matched; it is the container's own default-flagged track — usually the release's
    /// original language.
    Default,
    /// No preference matched and nothing is flagged default: the first track, counting audio tracks
    /// only (or track 0, where every track is a commentary).
    First,
}

impl std::fmt::Display for AudioReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AudioReason::Explicit => write!(f, "named by the player"),
            AudioReason::Preferred(lang) => write!(f, "preferred language {lang}"),
            AudioReason::Unlabelled => write!(f, "unlabelled over an unwanted language"),
            AudioReason::Default => write!(f, "original language"),
            AudioReason::First => write!(f, "first"),
        }
    }
}

/// The audio track to play, counting audio tracks only, by den-core's `audio_pick`. In the first of `prefs`
/// (most wanted first) that a track is in, never a commentary: the most channels, then the one flagged
/// default, then the first. Without a match, an untagged track before one tagged with another language, then
/// the default-flagged one, then the first.
pub fn pick_audio(tracks: &[AudioTrack], prefs: &[String]) -> (usize, AudioReason) {
    let facts: Vec<den_sync::AudioTrack> = tracks
        .iter()
        .map(|t| den_sync::AudioTrack {
            language: t.language.clone(),
            default: t.default,
            commentary: t.commentary,
            channels: t.channels,
            ..Default::default()
        })
        .collect();
    let Some(pick) = den_sync::audio_pick(&facts, None, prefs) else {
        return (0, AudioReason::First);
    };
    let reason = match pick.reason {
        den_sync::AudioReason::Kept | den_sync::AudioReason::Wanted => {
            AudioReason::Preferred(canonical(tracks[pick.index].language.as_deref().unwrap_or_default()))
        }
        den_sync::AudioReason::Unlabelled => AudioReason::Unlabelled,
        den_sync::AudioReason::Default => AudioReason::Default,
        den_sync::AudioReason::First => AudioReason::First,
    };
    (pick.index, reason)
}

/// A name to show for the tag: English where known, `subs::UNDETERMINED`'s own "Unknown", else the tag.
pub fn name(tag: &str) -> String {
    if tag == crate::subs::UNDETERMINED {
        return "Unknown".to_string();
    }
    den_media::lang::english_name(tag).map(str::to_string).unwrap_or_else(|| tag.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn track(lang: Option<&str>, default: bool, commentary: bool) -> AudioTrack {
        AudioTrack {
            codec: "A_AC3".into(),
            language: lang.map(String::from),
            channels: 6,
            name: None,
            default,
            commentary,
            bytes: None,
        }
    }

    #[test]
    fn every_spelling_of_a_language_compares_equal() {
        for tag in ["de", "DE-at", "ger", "deu", "de_CH"] {
            assert_eq!(canonical(tag), "de", "{tag}");
        }
        assert_eq!(canonical("nob"), "no");
        assert_eq!(canonical("nn-NO"), "no");
        assert_eq!(canonical("xyz"), "xyz", "an unknown tag compares as itself");
        assert_eq!(name("fin"), "Finnish");
        assert_eq!(name("xyz"), "xyz");
    }

    #[test]
    fn the_first_preference_a_track_is_in_wins() {
        let tracks = [
            track(Some("rus"), true, false),
            track(Some("eng"), false, false),
            track(Some("fin"), false, false),
        ];
        let prefs = |p: &[&str]| p.iter().map(|s| s.to_string()).collect::<Vec<_>>();
        assert_eq!(pick_audio(&tracks, &prefs(&["fi", "en"])), (2, AudioReason::Preferred("fi".into())));
        let (i, why) = pick_audio(&tracks, &prefs(&["sv", "en-US"]));
        assert_eq!((i, why), (1, AudioReason::Preferred("en".into())), "no Swedish, so English");
        assert_eq!(
            pick_audio(&tracks, &prefs(&["sv"])),
            (0, AudioReason::Default),
            "nothing matches: the default track"
        );
        assert_eq!(pick_audio(&tracks, &[]), (0, AudioReason::Default));
        assert_eq!(AudioReason::Preferred("fi".into()).to_string(), "preferred language fi");
        assert_eq!(AudioReason::Default.to_string(), "original language");
        assert_eq!(AudioReason::First.to_string(), "first");
        assert_eq!(AudioReason::Explicit.to_string(), "named by the player");
    }

    #[test]
    fn a_commentary_is_never_picked_for_its_language() {
        let tracks = [
            track(Some("eng"), false, true),
            track(Some("fre"), true, false),
            track(Some("eng"), false, false),
        ];
        assert_eq!(pick_audio(&tracks, &["en".to_string()]), (2, AudioReason::Preferred("en".into())));
        // Among matches, the default-flagged one.
        let tracks = [track(Some("eng"), false, false), track(Some("eng"), true, false)];
        assert_eq!(pick_audio(&tracks, &["en".to_string()]), (1, AudioReason::Preferred("en".into())));
        // Only commentaries: track 0 rather than nothing, and "first" rather than "original" — a
        // commentary's own default flag never stands for the release's original language.
        assert_eq!(pick_audio(&[track(None, true, true)], &[]), (0, AudioReason::First));
    }

    /// den-hearth#286: a dual-audio release tagged Italian first, its English track untagged. Nothing is tagged
    /// English, so the untagged track plays rather than the Italian one the default flag points at.
    #[test]
    fn an_untagged_track_beats_one_tagged_with_an_unwanted_language() {
        let tracks =
            [track(Some("ita"), true, false), track(None, false, false), track(Some("eng"), false, true)];
        assert_eq!(pick_audio(&tracks, &["en".to_string()]), (1, AudioReason::Unlabelled));
        assert_eq!(AudioReason::Unlabelled.to_string(), "unlabelled over an unwanted language");
        // A tagged match still wins over the untagged track, and asking for nothing makes no language unwanted.
        assert_eq!(pick_audio(&tracks, &["it".to_string()]), (0, AudioReason::Preferred("it".into())));
        assert_eq!(pick_audio(&tracks, &[]), (0, AudioReason::Default));
    }

    #[test]
    fn the_surround_mix_beats_its_default_stereo_downmix() {
        let stereo = AudioTrack { channels: 2, ..track(Some("eng"), true, false) };
        let tracks = [stereo, track(Some("eng"), false, false)];
        assert_eq!(pick_audio(&tracks, &["en".to_string()]), (1, AudioReason::Preferred("en".into())));
    }
}
