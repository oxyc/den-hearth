//! Which resolver answers a `/direct` resolve: yt-dlp (the default), rustypipe in-process, or
//! rustypipe with yt-dlp behind it.
//!
//! Everything here is independent of the `rustypipe` cargo feature, so the selection rules, the error
//! mapping and the fallback are tested without the crate and without a network. `rp.rs` is the thin
//! adapter that turns a rustypipe player response into [`Offered`] and its errors into [`Why`].
//!
//! Scope: the URL resolve behind `/direct` and its warm-up (`direct::extract`), which is where the
//! 1.2–3.6 s a resolve costs is spent. The `/play` download and the `probe` stay on yt-dlp: they need
//! its muxing and its extractability gate, and a download is bytes, not a resolve.

// Selection and error mapping are reached only through `rp.rs`, which a default build leaves out;
// their tests still run in it.
#![cfg_attr(not(feature = "rustypipe"), allow(dead_code))]

use std::future::Future;

use crate::config::Config;
use crate::direct::Direct;
use crate::ytdlp::PlayError;

/// `REEL_RESOLVER`: which resolver `/direct` asks first.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum ResolverMode {
    /// yt-dlp only. The default, so behaviour is unchanged unless someone opts in.
    #[default]
    Ytdlp,
    /// rustypipe only; its failure is the answer. For measuring it alone.
    Rustypipe,
    /// rustypipe first, yt-dlp on ANY rustypipe failure.
    Auto,
}

impl ResolverMode {
    /// An unset or unrecognised value is the default: a typo should cost the setting, never the service.
    pub fn parse(value: Option<&str>) -> ResolverMode {
        match value.map(|v| v.trim().to_ascii_lowercase()).as_deref() {
            Some("rustypipe") => ResolverMode::Rustypipe,
            Some("auto") => ResolverMode::Auto,
            _ => ResolverMode::Ytdlp,
        }
    }
}

/// Who actually produced an answer, for the decision log.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Resolver {
    Ytdlp,
    Rustypipe,
}

impl Resolver {
    pub fn tag(self) -> &'static str {
        match self {
            Resolver::Ytdlp => "ytdlp",
            Resolver::Rustypipe => "rustypipe",
        }
    }
}

/// A decision-log line with the resolver that answered appended. Appended rather than threaded through
/// `direct::resolve_cache_line`, whose field order is pinned by log consumers.
pub(crate) fn tag(line: &str, by: Resolver) -> String {
    format!("{line} resolver={}", by.tag())
}

/// The decision line for a rustypipe failure that yt-dlp was then asked about. `vid` is an identity
/// field (`LOG_IDENTITY`); the reason is not.
pub(crate) fn fallback_line(cfg: &Config, why: &PlayError, vid: &str) -> String {
    let mut line =
        format!("event=resolver_fallback from=rustypipe to=ytdlp reason={} upstream=youtube", why.reason);
    if cfg.log_identity {
        line.push_str(&format!(" vid={vid}"));
    }
    line
}

/// What one resolve produced, and who produced it.
pub struct Resolved {
    pub answer: Result<Direct, PlayError>,
    pub resolver: Resolver,
    /// Set when `Auto` asked rustypipe, it failed, and `answer` is yt-dlp's. Why it failed.
    pub fallback: Option<PlayError>,
}

/// Run the resolver(s) `mode` names. Both are closures so the one not asked is never started.
pub async fn resolve_with<P, PF, Y, YF>(mode: ResolverMode, rustypipe: P, ytdlp: Y) -> Resolved
where
    P: FnOnce() -> PF,
    PF: Future<Output = Result<Direct, PlayError>>,
    Y: FnOnce() -> YF,
    YF: Future<Output = Result<Direct, PlayError>>,
{
    match mode {
        ResolverMode::Ytdlp => Resolved { answer: ytdlp().await, resolver: Resolver::Ytdlp, fallback: None },
        ResolverMode::Rustypipe => {
            Resolved { answer: rustypipe().await, resolver: Resolver::Rustypipe, fallback: None }
        }
        ResolverMode::Auto => match rustypipe().await {
            Ok(d) => Resolved { answer: Ok(d), resolver: Resolver::Rustypipe, fallback: None },
            Err(why) => Resolved { answer: ytdlp().await, resolver: Resolver::Ytdlp, fallback: Some(why) },
        },
    }
}

// ---- format selection -------------------------------------------------------------------------

/// The height rungs of a yt-dlp format ladder, highest first: every `height<=N` in `format`, once.
/// The ladder is built only by `config::format_ladder`, so this reads back exactly what it wrote.
pub fn rungs(format: &str) -> Vec<u32> {
    let mut out: Vec<u32> = Vec::new();
    for part in format.split("height<=").skip(1) {
        let digits: String = part.chars().take_while(char::is_ascii_digit).collect();
        if let Ok(h) = digits.parse::<u32>() {
            if !out.contains(&h) {
                out.push(h);
            }
        }
    }
    out
}

/// One video stream a resolver offers.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Vid {
    pub url: String,
    pub itag: u32,
    pub width: u32,
    pub height: u32,
    pub fps: u32,
    pub bitrate: u32,
    /// H.264. The ladder is avc1-only: AVPlayer and the browsers' hardware decoders take it.
    pub avc1: bool,
    pub mp4: bool,
    /// Carries its own audio track (itag 18 and 22), as opposed to the adaptive video-only streams.
    pub muxed: bool,
}

/// One audio stream a resolver offers.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Aud {
    pub url: String,
    pub bitrate: u32,
    /// AAC, the only audio the ladder allows (`ba[acodec^=mp4a]`).
    pub mp4a: bool,
}

/// Everything a resolver found for one video.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Offered {
    pub videos: Vec<Vid>,
    pub audios: Vec<Aud>,
    pub hls: Option<String>,
}

fn rank(v: &Vid) -> (u32, u32, u32, bool) {
    // yt-dlp's default sort: resolution, then frame rate, then bitrate. Video-only wins a tie, since a
    // muxed stream carries audio we would not use.
    (v.height, v.fps, v.bitrate, !v.muxed)
}

/// `config::format_ladder`, applied to what was offered: for each rung from the cap down, the best
/// avc1 video under it with the best AAC audio, else a muxed avc1+AAC stream under it; then itag 18;
/// then any muxed mp4 avc1.
///
/// One difference from yt-dlp, on purpose: a muxed stream is never the `bv*` half of a pair. yt-dlp
/// would merge a muxed video with a separate audio track; here a muxed stream is simply served alone.
pub fn select(o: &Offered, rungs: &[u32]) -> Option<(Vid, Option<Aud>)> {
    let audio = o.audios.iter().filter(|a| a.mp4a).max_by_key(|a| a.bitrate);
    for &h in rungs {
        let under = |v: &&Vid| v.avc1 && v.height <= h;
        if let Some(a) = audio {
            if let Some(v) = o.videos.iter().filter(|v| !v.muxed).filter(under).max_by_key(|v| rank(v)) {
                return Some((v.clone(), Some(a.clone())));
            }
        }
        if let Some(m) = o.videos.iter().filter(|v| v.muxed).filter(under).max_by_key(|v| rank(v)) {
            return Some((m.clone(), None));
        }
    }
    if let Some(m) = o.videos.iter().find(|v| v.muxed && v.itag == 18) {
        return Some((m.clone(), None));
    }
    o.videos.iter().filter(|v| v.muxed && v.mp4 && v.avc1).max_by_key(|v| rank(v)).map(|m| (m.clone(), None))
}

/// The answer for `/direct`: the selection above, through the same constructor the resident worker's
/// answer goes through, so the expiry rules are one place.
pub fn answer(o: &Offered, rungs: &[u32], now: u64) -> Result<Direct, PlayError> {
    let Some((video, audio)) = select(o, rungs) else {
        return Err(play_error(Why::NoStream, "no avc1/AAC stream within the ladder".into()));
    };
    let mut urls = vec![video.url];
    urls.extend(audio.map(|a| a.url));
    let spoken = crate::worker::Answer {
        width: Some(video.width).filter(|w| *w > 0),
        height: Some(video.height).filter(|h| *h > 0),
        urls,
        hls: o.hls.clone(),
    };
    crate::direct::from_worker(spoken, now)
        .ok_or_else(|| play_error(Why::NoStream, "the selection carried no URL".into()))
}

// ---- errors -----------------------------------------------------------------------------------

/// Why a resolver could not answer, in Reel's own terms. Each maps to the outcome class
/// `ytdlp::classify` gives the same situation, so `/play`'s cooldowns, `/health`'s systemic signal
/// and the removed-video cache behave identically whichever resolver spoke.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Why {
    /// 451 `geo_blocked`.
    Geoblocked,
    /// 403 `restricted`: private, age-restricted, members-only, paid.
    Restricted,
    /// 404 `unavailable`: deleted, removed, never existed.
    Gone,
    /// 503 `throttled`: YouTube refusing this box (bot wall, 429), not the video.
    Throttled,
    /// 504 `timeout`.
    TimedOut,
    /// 502 `no_direct_url`: the resolver worked, nothing in the ladder was offered.
    NoStream,
    /// 502 `urls_capped`: the resolver answered, but googlevideo refuses the URLs past the first
    /// minute or so (`direct::refuses_its_end`). An answer that cannot play a trailer through is not
    /// an answer; `auto` hands such a video to yt-dlp instead of caching it for six hours.
    Capped,
    /// 502 `extraction_failed`: anything else, including a resolver that is itself broken.
    Failed,
}

pub fn play_error(why: Why, detail: String) -> PlayError {
    let (status, reason, message) = match why {
        Why::Geoblocked => (451, "geo_blocked", "This trailer is not available in your region."),
        Why::Restricted => (403, "restricted", "This trailer is private or age-restricted."),
        Why::Gone => (404, "unavailable", "This trailer is no longer available."),
        Why::Throttled => (503, "throttled", crate::ytdlp::THROTTLED_MESSAGE),
        Why::TimedOut => (504, "timeout", "This trailer took too long to resolve."),
        Why::NoStream => (502, "no_direct_url", "Could not fetch this trailer."),
        Why::Capped => (502, "urls_capped", "Could not fetch this trailer."),
        Why::Failed => (502, "extraction_failed", "Could not fetch this trailer."),
    };
    PlayError {
        status,
        reason: reason.into(),
        message: message.into(),
        detail: format!("rustypipe: {detail}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ytdlp::classify;

    fn vid(itag: u32, height: u32, fps: u32, bitrate: u32, avc1: bool, muxed: bool) -> Vid {
        Vid {
            url: format!("https://rr1.googlevideo.com/videoplayback?itag={itag}&expire=4000000000"),
            itag,
            width: height * 16 / 9,
            height,
            fps,
            bitrate,
            avc1,
            mp4: avc1,
            muxed,
        }
    }

    fn aud(itag: u32, bitrate: u32, mp4a: bool) -> Aud {
        Aud {
            url: format!("https://rr1.googlevideo.com/videoplayback?itag={itag}&expire=4000000000"),
            bitrate,
            mp4a,
        }
    }

    /// What a current YouTube answer looks like: adaptive avc1 + vp9 video, aac + opus audio, and the
    /// 360p muxed itag 18.
    fn typical() -> Offered {
        Offered {
            videos: vec![
                vid(137, 1080, 24, 4_000_000, true, false),
                vid(136, 720, 24, 2_000_000, true, false),
                vid(135, 480, 24, 1_000_000, true, false),
                vid(248, 1080, 24, 3_000_000, false, false), // vp9
                vid(18, 360, 24, 500_000, true, true),
            ],
            audios: vec![aud(140, 128_000, true), aud(251, 160_000, false)],
            hls: Some("https://manifest.googlevideo.com/api/manifest/hls_variant/expire/4000000000/x".into()),
        }
    }

    fn itags(d: &Direct) -> (Option<String>, Option<String>) {
        let itag = |u: &str| crate::progressive::media_param(u, "itag");
        (itag(&d.video), d.audio.as_deref().and_then(itag))
    }

    #[test]
    fn the_mode_is_read_leniently_and_defaults_to_ytdlp() {
        assert_eq!(ResolverMode::parse(None), ResolverMode::Ytdlp);
        assert_eq!(ResolverMode::parse(Some("")), ResolverMode::Ytdlp);
        assert_eq!(ResolverMode::parse(Some("yt-dlp")), ResolverMode::Ytdlp, "not a name; the default");
        assert_eq!(ResolverMode::parse(Some(" RustyPipe ")), ResolverMode::Rustypipe);
        assert_eq!(ResolverMode::parse(Some("auto")), ResolverMode::Auto);
    }

    #[test]
    fn the_rungs_read_back_what_the_ladder_wrote() {
        assert_eq!(rungs(&crate::config::format_ladder(1080)), vec![1080, 720, 480]);
        assert_eq!(rungs(&crate::config::format_ladder(720)), vec![720, 480]);
        assert_eq!(rungs(&crate::config::format_ladder(480)), vec![480]);
        assert_eq!(rungs("fmt"), Vec::<u32>::new());
    }

    #[test]
    fn the_top_rung_takes_the_best_avc1_video_and_aac_audio() {
        let d = answer(&typical(), &[1080, 720, 480], 0).unwrap();
        assert_eq!(itags(&d), (Some("137".into()), Some("140".into())), "vp9 and opus are never chosen");
        assert_eq!((d.width, d.height), (Some(1920), Some(1080)));
        assert!(d.hls.is_some());
        assert_eq!(d.expires, 4_000_000_000_000, "expiry is read from the URLs, as for yt-dlp's answer");
    }

    #[test]
    fn a_cap_steps_down_the_ladder_and_never_past_it() {
        let d = answer(&typical(), &[720, 480], 0).unwrap();
        assert_eq!(itags(&d).0.as_deref(), Some("136"));
        let d = answer(&typical(), &[480], 0).unwrap();
        assert_eq!(itags(&d).0.as_deref(), Some("135"));
    }

    #[test]
    fn a_missing_top_rung_falls_to_the_next_not_to_360p() {
        let mut o = typical();
        o.videos.retain(|v| v.itag != 137);
        let d = answer(&o, &[1080, 720, 480], 0).unwrap();
        assert_eq!(itags(&d).0.as_deref(), Some("136"), "a good 720p beat the muxed 360p");
    }

    #[test]
    fn frame_rate_then_bitrate_break_a_tie_in_height() {
        let mut o = typical();
        o.videos.push(vid(299, 1080, 60, 6_000_000, true, false));
        o.videos.push(vid(399, 1080, 30, 5_000_000, true, false));
        let d = answer(&o, &[1080], 0).unwrap();
        assert_eq!(itags(&d).0.as_deref(), Some("299"));
    }

    #[test]
    fn without_aac_audio_the_muxed_stream_is_served_alone() {
        let mut o = typical();
        o.audios.retain(|a| !a.mp4a);
        let d = answer(&o, &[1080, 720, 480], 0).unwrap();
        assert_eq!(itags(&d), (Some("18".into()), None), "no sound track to pair, so itag 18");
        assert!(d.audio.is_none());
    }

    #[test]
    fn itag_18_is_the_floor_even_under_the_lowest_rung() {
        let o = Offered { videos: vec![vid(18, 360, 24, 500_000, true, true)], ..Default::default() };
        let d = answer(&o, &[240], 0).unwrap();
        assert_eq!(itags(&d).0.as_deref(), Some("18"));
    }

    #[test]
    fn nothing_the_ladder_allows_is_no_direct_url_not_an_extraction_failure() {
        let o = Offered {
            videos: vec![vid(248, 1080, 24, 3_000_000, false, false)],
            audios: vec![aud(251, 160_000, false)],
            hls: None,
        };
        let err = answer(&o, &[1080, 720, 480], 0).unwrap_err();
        assert_eq!((err.status, err.reason.as_str()), (502, "no_direct_url"));
    }

    /// The point of the mapping: the same situation lands in the same class whichever resolver saw it.
    #[test]
    fn each_failure_lands_in_the_class_yt_dlp_gives_the_same_situation() {
        let same = |why: Why, said: &str| {
            let ours = play_error(why, "x".into());
            let theirs = classify(Some(1), said);
            assert_eq!(
                (ours.status, ours.reason.as_str(), ours.message.as_str()),
                (theirs.status, theirs.reason.as_str(), theirs.message.as_str()),
                "{why:?} vs {said:?}"
            );
        };
        same(Why::Geoblocked, "The uploader has not made this video available in your country");
        same(Why::Restricted, "ERROR: [youtube] abc: Private video. Sign in if you've been granted access");
        same(Why::Gone, "ERROR: [youtube] abc: Video unavailable");
        same(Why::Throttled, "ERROR: Sign in to confirm you're not a bot");
        same(Why::Failed, "ERROR: something nobody has seen before");
    }

    #[test]
    fn the_classes_that_have_no_yt_dlp_twin_are_the_ones_direct_already_uses() {
        let timed = play_error(Why::TimedOut, "x".into());
        assert_eq!((timed.status, timed.reason.as_str()), (504, "timeout"));
        let none = play_error(Why::NoStream, "x".into());
        assert_eq!((none.status, none.reason.as_str()), (502, "no_direct_url"));
        let capped = play_error(Why::Capped, "x".into());
        assert_eq!((capped.status, capped.reason.as_str()), (502, "urls_capped"));
    }

    fn direct(tag: &str) -> Direct {
        Direct {
            video: format!("https://rr1.googlevideo.com/{tag}"),
            audio: None,
            width: None,
            height: None,
            hls: None,
            expires: 0,
        }
    }

    fn failure(reason: Why) -> PlayError {
        play_error(reason, "nope".into())
    }

    async fn run(
        mode: ResolverMode,
        rp: Result<Direct, PlayError>,
        yt: Result<Direct, PlayError>,
    ) -> (
        Resolved,
        std::sync::Arc<std::sync::atomic::AtomicUsize>,
        std::sync::Arc<std::sync::atomic::AtomicUsize>,
    ) {
        use std::sync::atomic::{AtomicUsize, Ordering::SeqCst};
        use std::sync::Arc;
        let (rp_calls, yt_calls) = (Arc::new(AtomicUsize::new(0)), Arc::new(AtomicUsize::new(0)));
        let (a, b) = (rp_calls.clone(), yt_calls.clone());
        let resolved = resolve_with(
            mode,
            move || async move {
                a.fetch_add(1, SeqCst);
                rp
            },
            move || async move {
                b.fetch_add(1, SeqCst);
                yt
            },
        )
        .await;
        (resolved, rp_calls, yt_calls)
    }

    #[tokio::test]
    async fn ytdlp_mode_never_starts_rustypipe() {
        use std::sync::atomic::Ordering::SeqCst;
        let (r, rp, yt) = run(ResolverMode::Ytdlp, Ok(direct("rp")), Ok(direct("yt"))).await;
        assert_eq!((rp.load(SeqCst), yt.load(SeqCst)), (0, 1));
        assert_eq!(r.resolver, Resolver::Ytdlp);
        assert!(r.answer.unwrap().video.ends_with("/yt"));
    }

    #[tokio::test]
    async fn rustypipe_mode_reports_its_own_failure_without_asking_yt_dlp() {
        use std::sync::atomic::Ordering::SeqCst;
        let (r, rp, yt) = run(ResolverMode::Rustypipe, Err(failure(Why::Failed)), Ok(direct("yt"))).await;
        assert_eq!((rp.load(SeqCst), yt.load(SeqCst)), (1, 0));
        assert_eq!(r.resolver, Resolver::Rustypipe);
        assert!(r.answer.is_err() && r.fallback.is_none());
    }

    #[tokio::test]
    async fn auto_keeps_a_rustypipe_answer_and_does_not_start_yt_dlp() {
        use std::sync::atomic::Ordering::SeqCst;
        let (r, rp, yt) = run(ResolverMode::Auto, Ok(direct("rp")), Ok(direct("yt"))).await;
        assert_eq!((rp.load(SeqCst), yt.load(SeqCst)), (1, 0));
        assert_eq!(r.resolver, Resolver::Rustypipe);
        assert!(r.fallback.is_none());
    }

    #[tokio::test]
    async fn auto_falls_back_on_any_rustypipe_failure_and_keeps_its_reason() {
        use std::sync::atomic::Ordering::SeqCst;
        for why in [Why::Failed, Why::Gone, Why::Throttled, Why::TimedOut, Why::NoStream, Why::Capped] {
            let (r, rp, yt) = run(ResolverMode::Auto, Err(failure(why)), Ok(direct("yt"))).await;
            assert_eq!((rp.load(SeqCst), yt.load(SeqCst)), (1, 1), "{why:?}");
            assert_eq!(r.resolver, Resolver::Ytdlp);
            assert!(r.answer.unwrap().video.ends_with("/yt"));
            assert_eq!(r.fallback.expect("the reason survives").reason, failure(why).reason);
        }
    }

    #[tokio::test]
    async fn when_both_fail_in_auto_yt_dlps_verdict_is_the_answer() {
        let (r, _, _) = run(ResolverMode::Auto, Err(failure(Why::Failed)), Err(failure(Why::Gone))).await;
        assert_eq!(r.resolver, Resolver::Ytdlp);
        assert_eq!(r.answer.unwrap_err().reason, "unavailable");
        assert_eq!(r.fallback.unwrap().reason, "extraction_failed");
    }

    #[test]
    fn decision_lines_carry_the_resolver_and_the_fallback_gates_the_video_id() {
        assert_eq!(
            tag("event=ytdlp_resolve outcome=served", Resolver::Rustypipe),
            "event=ytdlp_resolve outcome=served resolver=rustypipe"
        );
        let mut cfg = crate::tests::test_cfg(crate::tests::temp_dir());
        let why = failure(Why::Throttled);
        cfg.log_identity = true;
        assert_eq!(
            fallback_line(&cfg, &why, "dQw4w9WgXcQ"),
            "event=resolver_fallback from=rustypipe to=ytdlp reason=throttled upstream=youtube vid=dQw4w9WgXcQ"
        );
        cfg.log_identity = false;
        assert!(!fallback_line(&cfg, &why, "dQw4w9WgXcQ").contains("dQw4w9WgXcQ"));
    }
}
