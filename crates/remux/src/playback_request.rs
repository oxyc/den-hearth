//! The closed client-authored portion of a playback/session request.
//!
//! Hearth adds server-owned Scout/Subtitles URLs only after this type has rejected unknown fields, so a
//! caller cannot smuggle either credential through the facade. Remux uses the same parser and validation.

use den_net::Playable;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct PlaybackRequest {
    pub imdb: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub season: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub episode: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub filename: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub audio: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub audio_track: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub subtitle_languages: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub original_language: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub video_codecs: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub playable: Option<Playable>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub start_at: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_bitrate: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub player: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub exclude: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub transcode: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub fits_only: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub replaces: Option<String>,
}

impl PlaybackRequest {
    pub fn validate(&self) -> Result<(), &'static str> {
        if self.imdb.len() < 3
            || self.imdb.len() > 12
            || !self.imdb.starts_with("tt")
            || !self.imdb[2..].bytes().all(|byte| byte.is_ascii_digit())
        {
            return Err("imdb must be an IMDb id, tt followed by digits.");
        }
        if self.season.is_some() != self.episode.is_some() {
            return Err("An episode needs both season and episode.");
        }
        if self.start_at.is_some_and(|value| !value.is_finite() || value < 0.0) {
            return Err("startAt is seconds into the title, 0 or more.");
        }
        if self.max_bitrate == Some(0) {
            return Err("maxBitrate is bits a second, more than 0.");
        }
        if !matches!(self.transcode.as_deref(), None | Some("allow" | "never")) {
            return Err("transcode is \"allow\", \"never\", or absent.");
        }
        if self.replaces.as_deref().is_some_and(|value| {
            value.len() != 22
                || !value.bytes().all(|byte| byte.is_ascii_alphanumeric() || byte == b'-' || byte == b'_')
        }) {
            return Err("replaces is the sid of a session this browser or install started.");
        }
        let short_tags = |values: Option<&Vec<String>>| {
            values.is_none_or(|values| values.len() <= 8 && values.iter().all(|value| value.len() <= 35))
        };
        if !short_tags(self.audio.as_ref())
            || !short_tags(self.subtitle_languages.as_ref())
            || !short_tags(self.video_codecs.as_ref())
        {
            return Err("audio, subtitleLanguages and videoCodecs are at most 8 short tags each.");
        }
        if self.original_language.as_ref().is_some_and(|value| value.len() > 35) {
            return Err("originalLanguage is one short language tag.");
        }
        if self
            .exclude
            .as_ref()
            .is_some_and(|values| values.len() > 16 || values.iter().any(|value| value.len() > 512))
        {
            return Err("exclude is at most 16 filenames.");
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parsing_is_closed_and_validation_is_shared() {
        let request: PlaybackRequest = serde_json::from_value(serde_json::json!({
            "imdb": "tt1",
            "season": 1,
            "episode": 2,
            "transcode": "never"
        }))
        .unwrap();
        assert_eq!(request.validate(), Ok(()));
        for forbidden in ["scout", "subtitles", "directFailed", "directOnly", "future"] {
            let mut value = serde_json::json!({"imdb": "tt1"});
            value[forbidden] = serde_json::json!(true);
            assert!(serde_json::from_value::<PlaybackRequest>(value).is_err(), "accepted {forbidden}");
        }
    }

    #[test]
    fn validation_matches_the_session_limits() {
        let parse = |value| serde_json::from_value::<PlaybackRequest>(value).unwrap();
        assert!(parse(serde_json::json!({"imdb":"tt1","season":1})).validate().is_err());
        assert!(parse(serde_json::json!({"imdb":"tt1","maxBitrate":0})).validate().is_err());
        assert!(parse(serde_json::json!({"imdb":"tt1","exclude": vec!["x"; 17]})).validate().is_err());
        assert!(parse(serde_json::json!({"imdb":"tt1","originalLanguage":"en"})).validate().is_ok());
        let long = "x".repeat(36);
        assert!(parse(serde_json::json!({"imdb":"tt1","originalLanguage": long})).validate().is_err());
    }
}
