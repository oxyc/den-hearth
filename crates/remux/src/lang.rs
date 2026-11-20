//! Language tags and the audio track a session plays. The comparison and the pick live in `den_media::lang`, which
//! Scout picks its TorBox audio track with too; this adds the display name, which needs Remux's own placeholder tag.

use crate::probe::AudioTrack;

pub use den_media::lang::{canonical, pick_audio, AudioCandidate, AudioReason};

impl AudioCandidate for AudioTrack {
    fn language(&self) -> Option<&str> {
        self.language.as_deref()
    }
    fn is_default(&self) -> bool {
        self.default
    }
    fn is_commentary(&self) -> bool {
        self.commentary
    }
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
}
