//! The stream source: den-scout.
//!
//! A session's releases come from the scout install the web app names — its library's full install URL,
//! or, for a logged-in browser, a scope=availability one that scout honours only with `X-Den-Remux-Key` —
//! or, as a fallback, this service's own `SCOUT_INSTALL_URL`. The browser never gets a ticket or a debrid link: it asks for a
//! title and gets back a signed session for one release. Scout ranks the list for the browser, from the capability
//! report passed on in `X-Den-Playable`, and the first release that is cached on the debrid, probes cleanly and
//! plays in the browser as it is wins; one it can only have converted is the last resort.

use den_upstream::{AddressGuard, GuardError};
use serde::Deserialize;

#[derive(Deserialize)]
struct StreamList {
    #[serde(default)]
    streams: Vec<Stream>,
}

#[derive(Deserialize, Clone, Debug)]
pub struct Stream {
    #[serde(default)]
    pub title: String,
    pub url: String,
    #[serde(default)]
    pub attributes: Attributes,
    #[serde(default, rename = "behaviorHints")]
    pub hints: Hints,
}

#[derive(Deserialize, Clone, Debug, Default)]
pub struct Attributes {
    pub codec: Option<String>,
    /// Three answers, as scout sends them: held by the debrid, not held, or nobody could ask (absent).
    pub cached: Option<bool>,
    /// The size of the file this stream actually plays. For an episode scout picked out of a season pack,
    /// scout corrects this to the episode's own size once it has probed the resolved file — never the
    /// pack's, which is what an indexer's title names.
    #[serde(rename = "sizeBytes")]
    pub size_bytes: Option<u64>,
    /// The season pack's own total, present only when scout knows it differs meaningfully from
    /// `size_bytes` for an episode request. Display-only context ("1.1 GB · from a 68 GB pack"), never a
    /// size to resolve, budget or open a door against — `resolved.size` in session.rs is the one that does.
    #[serde(default, rename = "packSizeBytes")]
    pub pack_size_bytes: Option<u64>,
    #[serde(default)]
    pub label: String,
    #[serde(default, rename = "threeD")]
    pub three_d: bool,
    /// `2160p`, `1080p`, … as the release name gives it.
    pub resolution: Option<String>,
    /// HDR by the name or the file; Dolby Vision counts.
    #[serde(default)]
    pub hdr: bool,
    /// 8 or 10, 0 when nobody read it. The name supplies 10 for "10bit"/"Hi10P" and for any HDR or Dolby Vision
    /// release; scout's probe replaces that with what the codec's configuration record says.
    #[serde(default, rename = "bitDepth")]
    pub bit_depth: u32,
    /// The Dolby Vision profile scout's probe read from the file (5, 7, 8); 0 when unknown, never "none".
    #[serde(default, rename = "dvProfile")]
    pub dv_profile: u32,
    /// Scout read the file itself, so the codec, depth and profile above are the file's, not the name's.
    #[serde(default)]
    pub probed: bool,
}

#[derive(Deserialize, Clone, Debug, Default)]
pub struct Hints {
    pub filename: Option<String>,
}

impl Stream {
    /// The release name: scout's `behaviorHints.filename`, else the stream title.
    pub fn filename(&self) -> &str {
        self.hints.filename.as_deref().unwrap_or(&self.title)
    }
}

pub fn is_imdb(id: &str) -> bool {
    id.len() >= 3 && id.len() <= 12 && id.starts_with("tt") && id[2..].bytes().all(|c| c.is_ascii_digit())
}

/// Scout's id for a title: the IMDb id for a movie, `<imdb>:<season>:<episode>` for an episode.
pub fn title_id(imdb: &str, episode: Option<(u32, u32)>) -> String {
    match episode {
        Some((s, e)) => format!("{imdb}:{s}:{e}"),
        None => imdb.to_string(),
    }
}

/// The Stremio type a title id names.
pub fn kind(id: &str) -> &'static str {
    if id.contains(':') {
        "series"
    } else {
        "movie"
    }
}

pub fn stream_list_url(install: &str, id: &str) -> String {
    format!("{install}/stream/{}/{id}.json", kind(id))
}

pub fn parse(body: &[u8]) -> Result<Vec<Stream>, String> {
    serde_json::from_slice::<StreamList>(body)
        .map(|l| l.streams)
        .map_err(|e| format!("not a stream list: {e}"))
}

/// The extension of a release whose name says it is in a container left out by name: one the probe can't open
/// (it reads Matroska, MP4, MPEG-TS and AVI only), or WebM. `None` for any other name, which the probe decides.
/// Blu-ray's M2TS is an MPEG-TS of 192-byte packets, which the probe doesn't read.
pub fn left_out_by_name(s: &Stream) -> Option<&'static str> {
    let name = s.filename().to_ascii_lowercase();
    [
        ".m2ts", ".mts", ".m2t", ".iso", ".img", ".wmv", ".asf", ".mpg", ".mpeg", ".vob", ".flv", ".rm",
        ".rmvb", ".divx", ".ogm", ".ogv", ".webm",
    ]
    .into_iter()
    .find(|ext| name.ends_with(ext))
}

/// Whether the release's name says MPEG-TS or AVI: transcoded whatever its codec (`MediaInfo::grid`), so the MPEG-4
/// Part 2 that scout's probe names in an AVI is tried too.
pub(crate) fn grid_by_name(s: &Stream) -> bool {
    let name = s.filename().to_ascii_lowercase();
    name.ends_with(".ts") || name.ends_with(".avi")
}

/// Could the browser take this release? Cached — an uncached one would start a debrid download and play nothing —
/// and H.264 or HEVC, AV1 or VP9 for a player that decodes it (`av1`, `vp9`), or a codec the title does not name (the
/// probe decides those), or MPEG-4 Part 2 in an MPEG-TS or AVI, which is transcoded. AV1 or VP9 for any other
/// player, and VC-1, need the video re-encoded from a container this service copies. Some containers are left out by
/// name (`left_out_by_name`).
fn remuxable(s: &Stream, av1: bool, vp9: bool) -> bool {
    let codec_ok = match s.attributes.codec.as_deref().map(str::to_ascii_lowercase) {
        None => true,
        Some(c) => {
            c == "h264"
                || c == "hevc"
                || (av1 && c == "av1")
                || (vp9 && c == "vp9")
                || (c == "mpeg4" && grid_by_name(s))
        }
    };
    s.attributes.cached == Some(true) && codec_ok && left_out_by_name(s).is_none() && !s.attributes.three_d
}

/// The releases worth trying, in the order to try them: the one the browser named first when it is
/// playable here, then scout's order — ranked for this browser when the session passed on what it plays.
/// `av1`, `vp9`: the player decodes AV1, VP9.
pub fn candidates(streams: &[Stream], filename: Option<&str>, av1: bool, vp9: bool) -> Vec<Stream> {
    let mut out: Vec<Stream> = streams.iter().filter(|s| remuxable(s, av1, vp9)).cloned().collect();
    if let Some(want) = filename {
        if let Some(i) = out.iter().position(|s| s.filename() == want) {
            let chosen = out.remove(i);
            out.insert(0, chosen);
        }
    }
    out
}

/// How much of the file to read when resolving it. Enough for the Matroska SeekHead, Info and Tracks,
/// or a faststart `moov` for a short file; anything larger is fetched by what points at it.
pub const HEAD_BYTES: u64 = 256 * 1024;

#[derive(Clone)]
pub struct Resolved {
    /// Where the play URL led — the debrid's link. ffmpeg reads this directly, so a seek does not
    /// go back through scout.
    pub url: String,
    pub head: Vec<u8>,
    pub size: Option<u64>,
}

/// The header a scope=availability scout config asks for before it lists or plays.
pub const KEY_HEADER: &str = "x-den-remux-key";

/// Where a session's releases come from: a scout base URL (config segment included) and the service key
/// to present to it.
pub struct ScoutSource {
    pub base: String,
    pub key: Option<String>,
}

impl ScoutSource {
    /// Is `url` on this scout's origin — the only place the key may be sent?
    fn is_scout(&self, url: &reqwest::Url) -> bool {
        reqwest::Url::parse(&self.base).is_ok_and(|b| b.origin() == url.origin())
    }
}

/// Which hosts a play URL may lead to once it leaves scout. Scout's own origin (`SCOUT_ORIGINS`, after
/// `ORIGIN_ALIASES`) is always allowed, LAN or loopback as it may be; every other hop of the redirect chain, the
/// debrid's link that ffmpeg then reads included, must be on the public internet. Without this a scout, or an
/// indexer behind it, that answers a play URL with `Location: http://169.254.169.254/…` has this service fetch it.
#[derive(Clone, Copy, Debug)]
pub struct SourceGuard(AddressGuard);

impl SourceGuard {
    /// Refuses every non-public host off scout's origin.
    pub const fn strict() -> SourceGuard {
        SourceGuard(AddressGuard::strict())
    }

    /// What the running service uses. Only this crate's own tests are exempt (their sources are 127.0.0.1
    /// servers); there is no switch for it in a build that ships.
    #[cfg(not(test))]
    pub const fn for_build() -> SourceGuard {
        SourceGuard::strict()
    }

    #[cfg(test)]
    pub const fn for_build() -> SourceGuard {
        SourceGuard(AddressGuard::permissive())
    }

    /// `Ok` when `url` may be fetched: an IP literal must be public, a name must resolve and every address it
    /// resolves to must be public.
    async fn check(&self, url: &reqwest::Url) -> Result<(), String> {
        let host = safe_host_fact(url);
        self.0.check(url).await.map_err(|refused| match refused {
            GuardError::Unresolved(_) => format!("the play URL's host did not resolve (host={host})"),
            GuardError::NotPublic | GuardError::Nothing => {
                format!("the play URL led to a host that is not on the public internet (host={host})")
            }
        })
    }
}

/// A bounded, non-secret host description for a guard refusal. A TorBox CDN suffix is operationally useful when
/// its domains change; every other DNS name and every address literal stays deliberately opaque. Paths, queries,
/// credentials and config segments never enter this function.
fn safe_host_fact(url: &reqwest::Url) -> &str {
    let Some(host) = url.host_str() else { return "missing" };
    if host.parse::<std::net::IpAddr>().is_ok() {
        return "ip_literal";
    }
    host.find("tb-cdn.")
        .map(|at| &host[at..])
        .filter(|suffix| {
            suffix.strip_prefix("tb-cdn.").is_some_and(|tld| {
                (2..=10).contains(&tld.len()) && tld.bytes().all(|byte| byte.is_ascii_lowercase())
            })
        })
        .unwrap_or("dns_name")
}

/// A scout base URL a browser handed over, accepted only as `<allowed origin>/<config>` for legacy Scout or
/// `<allowed origin>/den/<config>` for Hearth: an origin in `SCOUT_ORIGINS`, one base64url config segment, and no
/// credentials, query or fragment. This is the SSRF guard — without it a logged-in browser could make
/// this service fetch any URL on the LAN. Returns the normalised base.
pub fn validate_scoped(url: &str, origins: &[String]) -> Result<String, &'static str> {
    if url.len() > 4096 {
        return Err("too long");
    }
    if url.contains(['?', '#']) {
        return Err("a query or fragment");
    }
    let (scheme, rest) = url.split_once("://").ok_or("not an absolute URL")?;
    let (authority, path) = rest.split_once('/').ok_or("no config segment")?;
    if authority.contains('@') {
        return Err("credentials in the URL");
    }
    let origin = format!("{}://{}", scheme.to_ascii_lowercase(), authority.to_ascii_lowercase());
    if !origins.contains(&origin) {
        return Err("an origin not in SCOUT_ORIGINS");
    }
    let config = path.strip_prefix("den/").unwrap_or(path);
    // One config segment only after the optional fixed Hearth mount: this also refuses a trailing slash/further path.
    if config.is_empty() || !config.bytes().all(|c| c.is_ascii_alphanumeric() || c == b'-' || c == b'_') {
        return Err("not a single base64url config segment");
    }
    Ok(format!("{origin}/{path}"))
}

/// How scout is reached: in process when this service runs inside den-hearth beside the scout the URL names
/// (`local`, set by hearth), over `http` otherwise. `http` must not follow redirects: one would carry the key off
/// scout's origin.
#[derive(Clone, Copy)]
pub struct Via<'a> {
    pub http: &'a reqwest::Client,
    pub local: Option<&'a den_http::Local>,
}

/// The total time one request to scout may take, in process or over HTTP (`state::scout_profile`).
pub const SCOUT_TOTAL: std::time::Duration = std::time::Duration::from_secs(60);

/// The most of an answer from scout read in process: a stream list is read up to 8 MiB, and nothing else scout
/// answers is larger.
const LOCAL_CAP: usize = 8 << 20;

impl Via<'_> {
    /// A GET of `url`, which is on scout's origin (`ScoutSource::is_scout`), with `headers`: in process when hearth
    /// serves `url` itself, and over HTTP otherwise. The answer is the same either way, a `reqwest::Response`, so
    /// what reads it cannot tell which way it came.
    async fn get_scout(
        &self,
        url: &reqwest::Url,
        headers: reqwest::header::HeaderMap,
    ) -> Result<reqwest::Response, String> {
        if let Some(call) = self.local.and_then(|local| local.get(url.as_str(), headers.clone(), LOCAL_CAP)) {
            let resp = tokio::time::timeout(SCOUT_TOTAL, call)
                .await
                .map_err(|_| "scout did not answer in time".to_string())??;
            let (mut parts, body) = resp.into_parts();
            // The URL the answer is for, as reqwest records it; `Response::url` reads it.
            use reqwest::ResponseBuilderExt;
            let tagged = hyper::Response::builder().url(url.clone()).body(()).expect("an empty response");
            parts.extensions.extend(tagged.into_parts().0.extensions);
            return Ok(reqwest::Response::from(hyper::Response::from_parts(parts, body)));
        }
        self.http.get(url.clone()).headers(headers).send().await.map_err(|e| e.without_url().to_string())
    }
}

/// Why a stream list could not be had: scout's status when it answered one, and what happened.
pub struct ListError {
    pub status: Option<u16>,
    pub detail: String,
}

/// The header carrying the browser's capability report to scout, which ranks the list for that browser.
pub const PLAYABLE_HEADER: &str = "x-den-playable";

/// The stream list for `imdb`, with the service key when the source has one, ranked for the browser whose report
/// `playable` is.
pub async fn list(
    via: Via<'_>,
    src: &ScoutSource,
    imdb: &str,
    playable: Option<&crate::session::Playable>,
) -> Result<Vec<Stream>, ListError> {
    let fail = |status: Option<u16>, detail: String| ListError { status, detail };
    let url = reqwest::Url::parse(&stream_list_url(&src.base, imdb))
        .map_err(|_| fail(None, "the scout URL does not parse".into()))?;
    let mut headers = reqwest::header::HeaderMap::new();
    if let Some(k) = &src.key {
        headers.insert(
            KEY_HEADER,
            k.parse().map_err(|_| fail(None, "the service key is not a header value".into()))?,
        );
    }
    if let Some(p) = playable {
        // Integers and booleans only: there is nothing in a Playable that fails to serialise, or that is not ASCII.
        let report = serde_json::to_string(p).expect("a Playable serialises");
        headers.insert(PLAYABLE_HEADER, report.parse().expect("a Playable report is a header value"));
    }
    let resp = via.get_scout(&url, headers).await.map_err(|d| fail(None, d))?;
    if !resp.status().is_success() {
        let status = resp.status().as_u16();
        return Err(fail(Some(status), format!("scout answered {status}")));
    }
    let body = crate::probe::read_capped(resp, 8 << 20).await.map_err(|d| fail(None, d))?;
    parse(&body).map_err(|d| fail(None, d))
}

/// How many redirects a play URL may take to reach the file. Scout's is one 302; a debrid may add one.
const MAX_HOPS: usize = 5;

/// Follow a play URL (scout's `/p/<ticket>`, a 302 to the debrid) and read the head of the file in the
/// same request.
///
/// Redirects are followed here rather than by the client, because the service key must reach scout
/// and nothing else: an HTTP client forwards custom headers across a cross-origin redirect, which would
/// hand `X-Den-Remux-Key` to the debrid's CDN.
///
/// Only the hops on scout's origin go in process (`Via`); the debrid's are fetched over HTTP behind the guard as
/// always, even one that names an address hearth itself answers on.
pub async fn resolve(
    via: Via<'_>,
    play_url: &str,
    src: &ScoutSource,
    guard: SourceGuard,
) -> Result<Resolved, String> {
    let mut url = reqwest::Url::parse(play_url).map_err(|_| "the play URL does not parse".to_string())?;
    let mut hops = 0;
    let resp = loop {
        let mut headers = reqwest::header::HeaderMap::new();
        let range = format!("bytes=0-{}", HEAD_BYTES - 1);
        headers.insert(reqwest::header::RANGE, range.parse().expect("a range is a header value"));
        let resp = if src.is_scout(&url) {
            // Scout itself is reached wherever it lives, and is the only place the key goes.
            if let Some(k) = src.key.as_deref() {
                headers.insert(
                    KEY_HEADER,
                    k.parse().map_err(|_| "the service key is not a header value".to_string())?,
                );
            }
            via.get_scout(&url, headers).await?
        } else {
            // Any other hop, the last one (which ffmpeg then reads) included, is checked before a byte is sent to it.
            guard.check(&url).await?;
            via.http
                .get(url.clone())
                .headers(headers)
                .send()
                .await
                .map_err(|e| e.without_url().to_string())?
        };
        if !resp.status().is_redirection() {
            break resp;
        }
        hops += 1;
        let next = resp
            .headers()
            .get(reqwest::header::LOCATION)
            .and_then(|v| v.to_str().ok())
            .and_then(|l| url.join(l).ok());
        match next {
            Some(n) if hops <= MAX_HOPS => url = n,
            _ => return Err(format!("the play URL redirected badly ({})", resp.status().as_u16())),
        }
    };
    let status = resp.status().as_u16();
    // Scout answers an uncached or failed resolve with JSON, not media.
    let json = resp
        .headers()
        .get(reqwest::header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .is_some_and(|v| v.contains("json"));
    if !(status == 200 || status == 206) || json {
        return Err(format!(
            "the play URL answered {status}{}",
            if json { " (JSON, not media)" } else { "" }
        ));
    }
    let range = match status {
        206 => {
            let (start, end, total) = resp
                .headers()
                .get(reqwest::header::CONTENT_RANGE)
                .and_then(|v| v.to_str().ok())
                .and_then(crate::httputil::content_range)
                .ok_or_else(|| "the play URL answered an invalid Content-Range".to_string())?;
            let length = end - start + 1;
            if start != 0
                || length != HEAD_BYTES.min(total)
                || resp.content_length().is_some_and(|content_length| content_length != length)
            {
                return Err("the play URL answered a Content-Range other than the requested head".into());
            }
            Some((length, total))
        }
        _ => None,
    };
    let size = range.map(|(_, total)| total).or_else(|| resp.content_length());
    let url = resp.url().to_string();
    let head = crate::probe::read_capped(resp, HEAD_BYTES).await?;
    if range.is_some_and(|(length, _)| head.len() as u64 != length) {
        return Err("the play URL's body did not match its Content-Range".into());
    }
    Ok(Resolved { url, head, size })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A scout at 127.0.0.1: `/p/t` redirects to `location` (absolute, or a path on itself), `/final` is a file.
    async fn scout_redirecting_to(location: &'static str) -> String {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        tokio::spawn(async move {
            while let Ok((mut sock, _)) = listener.accept().await {
                let mut buf = [0u8; 4096];
                let n = sock.read(&mut buf).await.unwrap_or(0);
                let resp = if buf[..n].starts_with(b"GET /final") {
                    "HTTP/1.1 200 OK\r\ncontent-length: 3\r\nconnection: close\r\n\r\nabc".to_string()
                } else {
                    format!("HTTP/1.1 302 Found\r\nlocation: {location}\r\ncontent-length: 0\r\nconnection: close\r\n\r\n")
                };
                let _ = sock.write_all(resp.as_bytes()).await;
                let _ = sock.shutdown().await;
            }
        });
        base
    }

    async fn resolve_through(location: &'static str, guard: SourceGuard) -> Result<Resolved, String> {
        let origin = scout_redirecting_to(location).await;
        let src = ScoutSource { base: format!("{origin}/cfg"), key: None };
        let client = reqwest::Client::builder().redirect(reqwest::redirect::Policy::none()).build().unwrap();
        resolve(Via { http: &client, local: None }, &format!("{origin}/p/t"), &src, guard).await
    }

    #[tokio::test]
    async fn a_redirect_off_scout_to_a_non_public_host_is_refused() {
        for location in [
            "http://127.0.0.1:9/x",
            "http://169.254.169.254/latest/meta-data",
            "http://192.168.86.193:8080/x",
            "http://10.0.0.5/x",
            "http://100.64.0.1/x",
            "http://[::1]/x",
            "http://[fd7a:115c:a1e0::1]/x",
            "http://[::ffff:10.0.0.1]/x",
            "http://[64:ff9b::808:808]/x",
            "http://localhost/x",
        ] {
            let err = resolve_through(location, SourceGuard::strict()).await.err().expect(location);
            assert!(err.contains("not on the public internet"), "{location}: {err}");
        }
    }

    #[test]
    fn guard_diagnostics_expose_only_a_safe_host_class_or_torbox_suffix() {
        let signed = reqwest::Url::parse(
            "https://node.tb-cdn.future/private/config/path?token=never-log-me&sig=also-secret",
        )
        .unwrap();
        assert_eq!(safe_host_fact(&signed), "tb-cdn.future");
        assert_eq!(safe_host_fact(&reqwest::Url::parse("http://10.0.0.5/private").unwrap()), "ip_literal");
        assert_eq!(
            safe_host_fact(&reqwest::Url::parse("https://private.example/secret").unwrap()),
            "dns_name"
        );
        for secret in ["never-log-me", "also-secret", "private", "config", "path"] {
            assert!(!safe_host_fact(&signed).contains(secret));
        }
    }

    #[tokio::test]
    async fn scout_itself_stays_reachable_wherever_it_lives() {
        // The play URL, the redirect target and the file are all on the scout at 127.0.0.1.
        let r =
            resolve_through("/final", SourceGuard::strict()).await.expect("scout's own origin is allowed");
        assert_eq!(r.head, b"abc");
    }

    #[tokio::test]
    async fn a_public_host_passes_the_guard() {
        let guard = SourceGuard::strict();
        for url in ["http://93.184.216.34/x", "https://[2606:4700:4700::1111]/x"] {
            guard.check(&reqwest::Url::parse(url).unwrap()).await.expect(url);
        }
    }

    fn fixture() -> Vec<Stream> {
        parse(include_bytes!("../testdata/scout-streams.json")).expect("fixture parses")
    }

    #[test]
    fn imdb_ids_are_validated() {
        assert!(is_imdb("tt0111161"));
        assert!(!is_imdb("tt"));
        assert!(!is_imdb("tt01x"));
        assert!(!is_imdb("../etc"));
        assert!(!is_imdb("tt0111161:1:2"), "an episode comes as season and episode fields");
    }

    #[test]
    fn only_an_exact_satisfiable_content_range_parses() {
        assert_eq!(crate::httputil::content_range("bytes 0-262143/1000000"), Some((0, 262143, 1000000)));
        for invalid in [
            "bytes */100",
            "bytes 5-4/100",
            "bytes 0-100/100",
            "bytes 0-9/*",
            "items 0-9/100",
            "bytes 0-9/100/extra",
        ] {
            assert_eq!(crate::httputil::content_range(invalid), None, "{invalid}");
        }
    }

    #[test]
    fn an_episode_lists_from_scouts_series_route() {
        let base = "http://scout/cfg";
        assert_eq!(stream_list_url(base, &title_id("tt1", None)), "http://scout/cfg/stream/movie/tt1.json");
        assert_eq!(
            stream_list_url(base, &title_id("tt1", Some((2, 5)))),
            "http://scout/cfg/stream/series/tt1:2:5.json"
        );
    }

    #[test]
    fn only_cached_remuxable_releases_are_candidates() {
        let names: Vec<String> =
            candidates(&fixture(), None, false, false).iter().map(|s| s.filename().to_string()).collect();
        assert_eq!(
            names,
            [
                "Film.2019.2160p.UHD.BluRay.REMUX.HEVC.TrueHD.7.1.mkv",
                "Film.2019.1080p.WEB-DL.DDP5.1.H.264.mkv",
                "Film.2019.720p.BluRay.XviD.avi",
                "Film.2019.1080p.BluRay.mkv",
            ],
            "scout's order, minus uncached, cache-unknown, AV1, VP9 and 3D"
        );
    }

    #[test]
    fn mpegts_and_avi_are_candidates_with_mpeg4_and_m2ts_is_left_out() {
        let stream = |filename: &str, codec: Option<&str>| Stream {
            title: String::new(),
            url: String::new(),
            hints: Hints { filename: Some(filename.into()) },
            attributes: Attributes {
                cached: Some(true),
                codec: codec.map(String::from),
                ..Default::default()
            },
        };
        for s in [
            stream("Film.2026.TELESYNC.ts", None),
            stream("Film.2005.DVDRip.XviD.avi", Some("mpeg4")),
            stream("Film.1999.DVB.ts", Some("h264")),
        ] {
            assert_eq!(left_out_by_name(&s), None, "{}", s.filename());
            assert!(remuxable(&s, false, false), "{}", s.filename());
        }
        assert!(
            !remuxable(&stream("Film.2005.DVDRip.XviD.mkv", Some("mpeg4")), false, false),
            "a copy's container"
        );
        assert_eq!(left_out_by_name(&stream("Film.BluRay.m2ts", None)), Some(".m2ts"));
    }

    #[test]
    fn a_named_release_goes_first_when_it_is_playable() {
        let pick = candidates(&fixture(), Some("Film.2019.1080p.BluRay.mkv"), false, false);
        assert_eq!(pick[0].filename(), "Film.2019.1080p.BluRay.mkv");
        assert_eq!(pick.len(), 4, "the others stay as fallbacks");
        // Named but uncached: scout's first cached one instead.
        let pick = candidates(&fixture(), Some("Film.2019.1080p.WEB.x264-UNCACHED.mkv"), false, false);
        assert_eq!(pick[0].filename(), "Film.2019.2160p.UHD.BluRay.REMUX.HEVC.TrueHD.7.1.mkv");
        // Named 4K: it still goes first, ahead of the ranking.
        let pick = candidates(
            &fixture(),
            Some("Film.2019.2160p.UHD.BluRay.REMUX.HEVC.TrueHD.7.1.mkv"),
            false,
            false,
        );
        assert_eq!(pick[0].filename(), "Film.2019.2160p.UHD.BluRay.REMUX.HEVC.TrueHD.7.1.mkv");
    }

    #[test]
    fn vp9_is_a_candidate_only_for_a_player_that_decodes_it() {
        let names = |vp9| -> Vec<String> {
            candidates(&fixture(), None, false, vp9).iter().map(|s| s.filename().to_string()).collect()
        };
        let vp9 = "Film.2019.2160p.WEB-DL.VP9.Opus.mkv".to_string();
        assert!(!names(false).contains(&vp9));
        assert_eq!(names(true)[1], vp9, "in scout's order");
        assert_eq!(names(true).len(), names(false).len() + 1, "and nothing else changes");
    }

    #[test]
    fn av1_is_a_candidate_only_for_a_player_that_decodes_it() {
        let names = |av1| -> Vec<String> {
            candidates(&fixture(), None, av1, false).iter().map(|s| s.filename().to_string()).collect()
        };
        assert!(!names(false).contains(&"Film.2019.2160p.WEB-DL.AV1.mkv".to_string()));
        assert_eq!(
            names(true),
            [
                "Film.2019.2160p.UHD.BluRay.REMUX.HEVC.TrueHD.7.1.mkv",
                "Film.2019.2160p.WEB-DL.AV1.mkv",
                "Film.2019.1080p.WEB-DL.DDP5.1.H.264.mkv",
                "Film.2019.720p.BluRay.XviD.avi",
                "Film.2019.1080p.BluRay.mkv",
            ],
            "in scout's order"
        );
    }

    #[test]
    fn a_scoped_scout_url_must_be_an_allowed_origin_and_one_config_segment() {
        let allowed = ["http://192.168.86.193:8080".to_string()];
        let ok = |u: &str| validate_scoped(u, &allowed);
        assert_eq!(
            ok("http://192.168.86.193:8080/eyJ2IjoxfQ-_x"),
            Ok("http://192.168.86.193:8080/eyJ2IjoxfQ-_x".into())
        );
        assert_eq!(
            ok("HTTP://192.168.86.193:8080/abc"),
            Ok("http://192.168.86.193:8080/abc".into()),
            "normalised"
        );
        assert_eq!(
            ok("http://192.168.86.193:8080/den/eyJ2IjoxfQ-_x"),
            Ok("http://192.168.86.193:8080/den/eyJ2IjoxfQ-_x".into()),
            "Hearth's one fixed mount preserves the same origin and segment boundary"
        );
        for refused in [
            "http://192.168.86.193:8081/abc",          // another port
            "https://192.168.86.193:8080/abc",         // another scheme
            "http://169.254.169.254/abc",              // anywhere else
            "http://x@192.168.86.193:8080/abc",        // credentials
            "http://192.168.86.193:8080@evil.lan/abc", // credentials disguising the host
            "http://192.168.86.193:8080/abc?x=1",      // query
            "http://192.168.86.193:8080/abc#f",        // fragment
            "http://192.168.86.193:8080/abc/stream",   // two segments
            "http://192.168.86.193:8080/other/abc",    // only Hearth's fixed mount is admitted
            "http://192.168.86.193:8080/den/abc/more", // Hearth plus more than one config segment
            "http://192.168.86.193:8080/abc/",         // trailing slash
            "http://192.168.86.193:8080/",             // no config
            "http://192.168.86.193:8080",              // no path at all
            "http://192.168.86.193:8080/%2e%2e",       // not base64url
            "192.168.86.193:8080/abc",                 // not absolute
        ] {
            assert!(ok(refused).is_err(), "{refused} was accepted");
        }
        assert!(
            validate_scoped("http://192.168.86.193:8080/abc", &[]).is_err(),
            "no origins, no scoped URLs"
        );
    }

    #[test]
    fn the_stream_fields_parse() {
        let s = &fixture()[1];
        assert_eq!(s.attributes.size_bytes, Some(58_000_000_000));
        assert!(s.url.contains("/p/"));
        assert!(!s.attributes.label.is_empty());
    }

    // Fauda S1E3: scout corrects `sizeBytes` to the episode's own 1.1 GB and keeps the season pack's 68 GB
    // only as `packSizeBytes` — a field den-remux must carry through rather than drop, since nothing here
    // should ever fall back to re-deriving a size from the indexer's title.
    #[test]
    fn a_season_pack_episode_carries_its_pack_size_separately() {
        let s = &fixture()[9];
        assert_eq!(s.attributes.size_bytes, Some(1_136_580_921));
        assert_eq!(s.attributes.pack_size_bytes, Some(68_000_000_000));
    }
}
