//! The rustypipe adapter: ask YouTube's innertube API in-process and hand `resolver` what it found.
//!
//! Compiled only with the `rustypipe` cargo feature. Without it [`Rp`] is an empty stand-in whose
//! every resolve fails, so `REEL_RESOLVER=auto` on a default build is just yt-dlp.
//!
//! **PO tokens.** rustypipe's web client needs one; its iOS and TV clients, which it tries when it has
//! no way to make one, do not. Tokens come from the separate `rustypipe-botguard` binary
//! (`REEL_BOTGUARD_BIN`), never linked in: it embeds a stripped Deno and a DOM, which is the weight
//! this whole spike is trying to be rid of. Unset, no token is ever asked for.

use crate::config::Config;
use crate::direct::Direct;
use crate::ytdlp::PlayError;

#[cfg(not(feature = "rustypipe"))]
#[derive(Default)]
pub struct Rp;

#[cfg(not(feature = "rustypipe"))]
impl Rp {
    pub async fn resolve(
        &self,
        _cfg: &Config,
        _vid: &str,
        _format: &str,
        _now: u64,
    ) -> Result<Direct, PlayError> {
        Err(crate::resolver::play_error(
            crate::resolver::Why::Failed,
            "this build has no rustypipe feature".into(),
        ))
    }
}

#[cfg(feature = "rustypipe")]
pub use enabled::Rp;

#[cfg(feature = "rustypipe")]
mod enabled {
    use std::time::Duration;

    use rustypipe::client::RustyPipe;
    use rustypipe::error::{Error, ExtractionError, UnavailabilityReason};
    use rustypipe::model::{AudioCodec, VideoCodec, VideoFormat, VideoPlayer};

    use super::{Config, Direct, PlayError};
    use crate::resolver::{self, Aud, Offered, Vid, Why};

    /// A resolve is a metadata round-trip. Under `direct`'s 30 s so that `auto` still has time to ask
    /// yt-dlp after this gives up.
    const TIMEOUT: Duration = Duration::from_secs(20);

    #[derive(Default)]
    pub struct Rp {
        client: tokio::sync::OnceCell<RustyPipe>,
    }

    impl Rp {
        pub async fn resolve(
            &self,
            cfg: &Config,
            vid: &str,
            format: &str,
            now: u64,
        ) -> Result<Direct, PlayError> {
            let client = self
                .client
                .get_or_try_init(|| async { build(cfg) })
                .await
                .map_err(|e| resolver::play_error(Why::Failed, format!("client: {e}")))?;
            let query = client.query().unauthenticated();
            let player = match tokio::time::timeout(TIMEOUT, query.player(vid)).await {
                Ok(Ok(p)) => p,
                Ok(Err(e)) => {
                    let (why, detail) = why(&e);
                    return Err(resolver::play_error(why, detail));
                }
                Err(_) => {
                    return Err(resolver::play_error(Why::TimedOut, format!("no answer in {TIMEOUT:?}")))
                }
            };
            resolver::answer(&offered(&player), &resolver::rungs(format), now)
        }
    }

    fn build(cfg: &Config) -> Result<RustyPipe, Error> {
        let mut b = RustyPipe::builder()
            // The deobfuscation JS and client versions, so a restart does not refetch them. Under the
            // cache volume, which is the one place this process may write.
            .storage_dir(cfg.cache_dir.join("rustypipe"))
            // Its error reports hold whole YouTube responses; the decision log is the record here.
            .no_reporter()
            .timeout(TIMEOUT)
            // The fallback is yt-dlp, not another try at the same thing.
            .n_http_retries(1);
        b = match &cfg.botguard_bin {
            Some(bin) => b.botguard_bin(bin.clone()).po_token_cache(),
            // Never a binary that merely happens to be on PATH: the same build must behave the same.
            None => b.no_botguard(),
        };
        b.build()
    }

    /// A rustypipe error in Reel's terms. The class is the point; the detail is for the log.
    pub(super) fn why(e: &Error) -> (Why, String) {
        let why = match e {
            Error::Extraction(ExtractionError::Unavailable { reason, .. }) => match reason {
                UnavailabilityReason::Geoblocked => Why::Geoblocked,
                UnavailabilityReason::AgeRestricted
                | UnavailabilityReason::Private
                | UnavailabilityReason::MembersOnly
                | UnavailabilityReason::Premium
                | UnavailabilityReason::Paid => Why::Restricted,
                UnavailabilityReason::Deleted => Why::Gone,
                // YouTube refusing this box, not the video.
                UnavailabilityReason::IpBan
                | UnavailabilityReason::VpnBan
                | UnavailabilityReason::Captcha
                | UnavailabilityReason::TryAgain => Why::Throttled,
                _ => Why::Failed,
            },
            Error::Extraction(ExtractionError::NotFound { .. }) => Why::Gone,
            Error::HttpStatus(429, _) => Why::Throttled,
            _ => Why::Failed,
        };
        (why, e.to_string())
    }

    /// A player response as the neutral `Offered`. Alternate-language dubs are left out: yt-dlp's `ba`
    /// takes the video's own track, and so does this.
    pub(super) fn offered(p: &VideoPlayer) -> Offered {
        let video = |s: &rustypipe::model::VideoStream, muxed: bool| Vid {
            url: s.url.clone(),
            itag: s.itag,
            width: s.width,
            height: s.height,
            fps: s.fps.into(),
            bitrate: s.bitrate,
            avc1: s.codec == VideoCodec::Avc1,
            mp4: s.format == VideoFormat::Mp4,
            muxed,
        };
        Offered {
            videos: p
                .video_only_streams
                .iter()
                .map(|s| video(s, false))
                .chain(p.video_streams.iter().map(|s| video(s, true)))
                .collect(),
            audios: p
                .audio_streams
                .iter()
                .filter(|a| a.track.as_ref().is_none_or(|t| t.is_default))
                .map(|a| Aud { url: a.url.clone(), bitrate: a.bitrate, mp4a: a.codec == AudioCodec::Mp4a })
                .collect(),
            hls: p.hls_manifest_url.clone(),
        }
    }

    #[cfg(test)]
    mod tests {
        use super::*;
        use crate::ytdlp::classify;

        fn unavailable(reason: UnavailabilityReason) -> Error {
            Error::Extraction(ExtractionError::Unavailable { reason, msg: "from YT".into() })
        }

        #[test]
        fn rustypipes_errors_land_in_reels_outcome_classes() {
            let class = |e: Error| {
                let (w, d) = why(&e);
                let p = resolver::play_error(w, d);
                (p.status, p.reason)
            };
            let c = |s: u16, r: &str| (s, r.to_string());
            assert_eq!(class(unavailable(UnavailabilityReason::Geoblocked)), c(451, "geo_blocked"));
            for r in [
                UnavailabilityReason::AgeRestricted,
                UnavailabilityReason::Private,
                UnavailabilityReason::MembersOnly,
                UnavailabilityReason::Premium,
                UnavailabilityReason::Paid,
            ] {
                assert_eq!(class(unavailable(r)), c(403, "restricted"), "{r:?}");
            }
            assert_eq!(class(unavailable(UnavailabilityReason::Deleted)), c(404, "unavailable"));
            for r in [
                UnavailabilityReason::IpBan,
                UnavailabilityReason::VpnBan,
                UnavailabilityReason::Captcha,
                UnavailabilityReason::TryAgain,
            ] {
                assert_eq!(class(unavailable(r)), c(503, "throttled"), "{r:?}");
            }
            assert_eq!(class(unavailable(UnavailabilityReason::Unplayable)), c(502, "extraction_failed"));
            assert_eq!(
                class(Error::Extraction(ExtractionError::NotFound { id: "x".into(), msg: "gone".into() })),
                c(404, "unavailable")
            );
            assert_eq!(class(Error::HttpStatus(429, "".into())), c(503, "throttled"));
            assert_eq!(class(Error::HttpStatus(500, "".into())), c(502, "extraction_failed"));
            assert_eq!(
                class(Error::Extraction(ExtractionError::Deobfuscation("sig".into()))),
                c(502, "extraction_failed")
            );
            assert_eq!(class(Error::Http("connect".into())), c(502, "extraction_failed"));
            // The same video yt-dlp calls removed is called removed here.
            assert_eq!(classify(None, "ERROR: Video unavailable").reason, "unavailable");
        }

        /// A player response with the shape YouTube answers with, parsed through rustypipe's own serde, so
        /// the adapter is held to the fields the crate really has.
        #[test]
        fn a_player_response_becomes_the_streams_the_ladder_chooses_from() {
            let stream = |itag: u32, h: u32, codec: &str| {
                format!(
                    r#"{{"url":"https://rr1.googlevideo.com/videoplayback?itag={itag}&expire=4000000000","itag":{itag},
                    "bitrate":1000000,"average_bitrate":900000,"size":null,"index_range":null,"init_range":null,
                    "duration_ms":null,"width":{w},"height":{h},"fps":24,"quality":"{h}p","hdr":false,
                    "mime":"video/mp4","format":"mp4","codec":"{codec}","drm_track_type":null,"drm_systems":[]}}"#,
                    w = h * 16 / 9
                )
            };
            let audio = |itag: u32, codec: &str, default: bool| {
                format!(
                    r#"{{"url":"https://rr1.googlevideo.com/videoplayback?itag={itag}&expire=4000000000","itag":{itag},
                    "bitrate":128000,"average_bitrate":128000,"size":1,"index_range":null,"init_range":null,
                    "duration_ms":null,"mime":"audio/mp4","format":"m4a","codec":"{codec}","channels":2,
                    "loudness_db":null,"track":{{"id":"en.0","lang":"en","lang_name":"English","is_default":{default},
                    "track_type":null}},"drm_track_type":null,"drm_systems":[]}}"#
                )
            };
            let json = format!(
                r#"{{"details":{{"id":"dQw4w9WgXcQ","name":null,"description":null,"duration":10,"thumbnail":[],
                "channel_id":"c","channel_name":null,"view_count":null,"keywords":[],"is_live":false,
                "is_live_content":false}},
                "video_streams":[{m18}],
                "video_only_streams":[{v137},{v248}],
                "audio_streams":[{a140},{a_dub}],
                "subtitles":[],"expires_in_seconds":21540,"valid_until":"2030-01-01T00:00:00Z",
                "hls_manifest_url":"https://manifest.googlevideo.com/x","dash_manifest_url":null,
                "preview_frames":[],"drm":null,"client_type":"ios","visitor_data":null}}"#,
                m18 = stream(18, 360, "avc1"),
                v137 = stream(137, 1080, "avc1"),
                v248 = stream(248, 1080, "vp9"),
                a140 = audio(140, "mp4a", true),
                a_dub = audio(141, "mp4a", false),
            );
            let player: VideoPlayer =
                serde_json::from_str(&json).expect("the fixture is rustypipe's own shape");
            let o = offered(&player);
            assert_eq!(o.videos.len(), 3);
            assert_eq!(o.audios.len(), 1, "the dub is not offered");
            assert_eq!(o.hls.as_deref(), Some("https://manifest.googlevideo.com/x"));

            let d = resolver::answer(&o, &[1080, 720, 480], 0).unwrap();
            assert!(d.video.contains("itag=137"), "{}", d.video);
            assert!(d.audio.unwrap().contains("itag=140"));
            assert_eq!((d.width, d.height), (Some(1920), Some(1080)));
        }
    }
}
