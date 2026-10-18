//! Which service a request path belongs to, the path that service gets to see, and how it is admitted.

use hyper::Method;

use crate::den::Route;

#[derive(Debug, PartialEq, Eq)]
pub enum Target {
    /// Hearth's own `/health`, which reports every service.
    Health,
    /// Hearth's own `/metrics`: admission state, behind a bearer token.
    Metrics,
    /// `/reel/<rest>`, served as `/<rest>`.
    Reel(String),
    /// `/subs/<rest>`, served as `/<rest>`.
    Subs(String),
    /// `/scout/<rest>`, served as `/<rest>`.
    Scout(String),
    /// `/remux/<rest>`, served as it came: Remux's routes already live under `/remux`.
    Remux(String),
    /// `/den/<rest>`: the combined Den addon (`den.rs`), answered by hearth or by the service it routes to.
    Den(Route),
    NotFound,
}

/// A service with a listener of its own, where every request goes to it with the path as it came.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Service {
    Scout,
    Reel,
    Subs,
    Remux,
}

/// What a request to a service's own listener is: always that service, and always the path unchanged,
/// exactly as the old standalone container served it (`/health` is the service's, not hearth's).
pub fn legacy(service: Service, path: &str) -> Target {
    let path = path.to_string();
    match service {
        Service::Scout => Target::Scout(path),
        Service::Reel => Target::Reel(path),
        Service::Subs => Target::Subs(path),
        Service::Remux => Target::Remux(path),
    }
}

pub fn target(path: &str) -> Target {
    // Routes v1 probes `<entry.url>/health`. The combined addon's entries end in `/den`, so its health address
    // is `/den/health`; answer it with Hearth's same aggregate report rather than parsing `health` as a config.
    if matches!(path, "/health" | "/den/health") {
        return Target::Health;
    }
    if path == "/metrics" {
        return Target::Metrics;
    }
    if let Some(rest) = mounted(path, "/reel") {
        return Target::Reel(rest);
    }
    if let Some(rest) = mounted(path, "/subs") {
        return Target::Subs(rest);
    }
    if let Some(rest) = mounted(path, "/scout") {
        return Target::Scout(rest);
    }
    if path == "/remux" || path.starts_with("/remux/") {
        // Everything Remux serves begins `/remux/` except its Prometheus endpoint, which sits at its
        // root; reached here as `/remux/metrics` so it has an address beside the others.
        return Target::Remux(if path == "/remux/metrics" {
            "/metrics".to_string()
        } else {
            path.to_string()
        });
    }
    if let Some(rest) = mounted(path, den_seal::addon::MOUNT) {
        return Target::Den(crate::den::route(&rest));
    }
    Target::NotFound
}

/// `path` under `mount`, with the mount removed: `/reel/play/x.mp4` → `/play/x.mp4`, `/reel` → `/`.
/// `/reeling` is not under `/reel`.
fn mounted(path: &str, mount: &str) -> Option<String> {
    let rest = path.strip_prefix(mount)?;
    if rest.is_empty() {
        Some("/".to_string())
    } else if rest.starts_with('/') {
        Some(rest.to_string())
    } else {
        None
    }
}

/// Which admission lane a request takes. Control is a reserved share that bulk work cannot occupy.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Lane {
    Control = 0,
    Bulk = 1,
}

/// A streamed-body class with its own permits (`media.rs`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MediaClass {
    Reel = 0,
    Remux = 1,
}

#[derive(Debug, PartialEq, Eq)]
pub struct Class {
    pub lane: Lane,
    /// Set when a successful answer is a media body that must own a stream permit.
    pub media: Option<MediaClass>,
}

/// How a request is admitted.
///
/// Control is an explicit allowlist of small, bounded answers (probes, manifests, session keep-alives).
/// Anything not named here, including every route added later, is bulk, so it can never inherit the
/// reserved lane by being unclassified.
pub fn classify(target: &Target, method: &Method) -> Class {
    let read = matches!(*method, Method::GET | Method::HEAD);
    let control = Class { lane: Lane::Control, media: None };
    let bulk = |media: Option<MediaClass>| Class {
        lane: Lane::Bulk,
        media: media.filter(|_| *method == Method::GET),
    };
    match target {
        Target::Health | Target::Metrics => control,
        Target::NotFound => bulk(None),
        // A CORS preflight is answered before routing, from no state.
        _ if *method == Method::OPTIONS => control,
        Target::Scout(path) if read && scout_control(path) => control,
        // Scout streams nothing: `/p/<ticket>` and `/play` answer a 302 and a stream list is a buffered
        // JSON body, so it has no media class. What it does is wait on indexers and debrid, which is
        // exactly what the bulk subcap is for.
        Target::Scout(_) => bulk(None),
        Target::Reel(path) if read && reel_control(path) => control,
        Target::Reel(path) => bulk(reel_media(path).then_some(MediaClass::Reel)),
        Target::Subs(path) if read && subs_control(path) => control,
        Target::Subs(_) => bulk(None),
        Target::Remux(path) if remux_control(path, method) => control,
        Target::Remux(path) => bulk(remux_media(path).then_some(MediaClass::Remux)),
        // A resource is admitted exactly as the same path on its service's own mount; the addon's own pages,
        // key and manifests are answered from no upstream, like each service's.
        Target::Den(route) => match route {
            Route::Scout(path) => classify(&Target::Scout(path.clone()), method),
            Route::Subs(path) => classify(&Target::Subs(path.clone()), method),
            Route::Reel(path) => classify(&Target::Reel(path.clone()), method),
            Route::Playback(_) => bulk(None),
            Route::Wanted(_) => bulk(None),
            Route::Guest => bulk(None),
            Route::NotFound => bulk(None),
            // Up to three public-key decryptions: never consume the reserved control lane under abuse.
            Route::Migrate if *method == Method::OPTIONS => control,
            Route::Migrate => bulk(None),
            Route::Configure | Route::Manifest | Route::ConfigKey | Route::Installed(_) if read => control,
            _ => bulk(None),
        },
    }
}

/// Scout's static answers, none of which touch an indexer or a debrid service: the configure page, the
/// probes, the unconfigured manifest, the sealing key, and a configured install's `/<config>/manifest.json`
/// (which only opens the config). `/<config>/stream`, `/play`, `/account` and `/p/<ticket>` are bulk.
fn scout_control(path: &str) -> bool {
    if matches!(
        path,
        "/" | "/configure" | "/configure/" | "/health" | "/metrics" | "/manifest.json" | "/config-key"
    ) {
        return true;
    }
    config_manifest(path)
}

/// Reel's static answers, like Scout's and Subtitles': the probes, the manifests (an installed one included, which
/// only opens its config), the configure page, the sealing key, and the cheap capability check.
fn reel_control(path: &str) -> bool {
    matches!(
        path,
        "/health" | "/metrics" | "/manifest.json" | "/config-key" | "/" | "/configure" | "/configure/"
    ) || path.starts_with("/_internal/validate/m/s/")
        || config_manifest(path)
}

/// `/<config>/manifest.json`.
fn config_manifest(path: &str) -> bool {
    path.strip_prefix('/')
        .and_then(|rest| rest.split_once('/'))
        .is_some_and(|(_, tail)| tail == "manifest.json")
}

/// Reel's streamed bodies: a trailer file, a progressive MP4, and HLS / direct-media segments. Both the
/// `/play/<id>.mp4` form and the bare `/play?v=<id>` one are served.
fn reel_media(path: &str) -> bool {
    path == "/play"
        || path.starts_with("/play/")
        || path.starts_with("/progressive/")
        || path == "/hls/seg"
        || path.starts_with("/m/")
}

fn subs_control(path: &str) -> bool {
    if matches!(path, "/health" | "/metrics" | "/manifest.json") {
        return true;
    }
    config_manifest(path)
}

fn remux_control(path: &str, method: &Method) -> bool {
    match path {
        // `/health` bare is what den-update probes on Remux's own port (the legacy listener passes paths unchanged).
        "/remux/health" | "/health" | "/metrics" if matches!(*method, Method::GET | Method::HEAD) => true,
        _ => match session_file(path) {
            // The viewer's own keep-alive and goodbye: tiny, and refusing them would strand a session.
            Some(("heartbeat" | "end", _)) => *method == Method::POST,
            Some(("", _)) => *method == Method::DELETE,
            _ => false,
        },
    }
}

/// `/remux/s/<sid>/<sig>/<file>` as `(file, true)`; `/remux/s/<sid>/<sig>` as `("", false)`.
fn session_file(path: &str) -> Option<(&str, bool)> {
    let mut it = path.strip_prefix("/remux/s/")?.splitn(3, '/');
    let (_sid, _sig) = (it.next()?, it.next()?);
    Some(it.next().map_or(("", false), |file| (file, true)))
}

/// Remux's streamed bodies: a segment, the init section, and the bandwidth probe.
fn remux_media(path: &str) -> bool {
    if path == "/remux/speed" {
        return true;
    }
    match session_file(path) {
        Some(("init.mp4" | "speed", _)) => true,
        Some((file, _)) => file.strip_prefix("seg").is_some_and(|n| n.ends_with(".m4s")),
        None => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn class(target: Target, method: Method) -> (Lane, Option<MediaClass>) {
        let c = classify(&target, &method);
        (c.lane, c.media)
    }

    #[test]
    fn only_known_small_routes_are_control() {
        use Lane::*;
        for (target, method) in [
            (Target::Health, Method::GET),
            (Target::Metrics, Method::GET),
            (Target::Scout("/".into()), Method::GET),
            (Target::Scout("/configure".into()), Method::GET),
            (Target::Scout("/health".into()), Method::GET),
            (Target::Scout("/metrics".into()), Method::GET),
            (Target::Scout("/manifest.json".into()), Method::GET),
            (Target::Scout("/config-key".into()), Method::GET),
            (Target::Scout("/CONFIG/manifest.json".into()), Method::GET),
            (Target::Scout("/CONFIG/stream/movie/tt1.json".into()), Method::OPTIONS),
            (Target::Reel("/health".into()), Method::GET),
            (Target::Reel("/manifest.json".into()), Method::GET),
            (Target::Reel("/".into()), Method::GET),
            (Target::Reel("/configure".into()), Method::GET),
            (Target::Reel("/CONFIG/manifest.json".into()), Method::GET),
            (Target::Remux("/health".into()), Method::GET),
            (Target::Reel("/_internal/validate/m/s/blob".into()), Method::GET),
            (Target::Subs("/manifest.json".into()), Method::GET),
            (Target::Subs("/CONFIG/manifest.json".into()), Method::GET),
            (Target::Remux("/remux/health".into()), Method::GET),
            (Target::Remux("/metrics".into()), Method::GET),
            (Target::Remux("/remux/s/sid/sig/heartbeat".into()), Method::POST),
            (Target::Remux("/remux/s/sid/sig/end".into()), Method::POST),
            (Target::Remux("/remux/s/sid/sig".into()), Method::DELETE),
            (Target::Den(Route::Migrate), Method::OPTIONS),
            (Target::Reel("/play/x.mp4".into()), Method::OPTIONS),
        ] {
            assert_eq!(class(target, method.clone()).0, Control, "{method}");
        }
        for (target, method) in [
            // New or unknown routes are bulk until someone names them.
            (Target::NotFound, Method::GET),
            (Target::Scout("/CONFIG/stream/movie/tt1.json".into()), Method::GET),
            (Target::Scout("/CONFIG/play/abc".into()), Method::GET),
            (Target::Scout("/p/ticket".into()), Method::GET),
            (Target::Scout("/p/ticket".into()), Method::DELETE),
            (Target::Scout("/validate".into()), Method::POST),
            (Target::Scout("/CONFIG/availability".into()), Method::POST),
            (Target::Scout("/manifest.json".into()), Method::POST),
            (Target::Reel("/something-new".into()), Method::GET),
            (Target::Reel("/meta/movie/tt1.json".into()), Method::GET),
            (Target::Subs("/CONFIG/subtitles/movie/tt1.json".into()), Method::GET),
            (Target::Subs("/manifest.json".into()), Method::POST),
            (Target::Remux("/remux/session".into()), Method::POST),
            (Target::Remux("/remux/login".into()), Method::POST),
            (Target::Remux("/remux/releases".into()), Method::POST),
            // A keep-alive verb on the wrong method is not the keep-alive.
            (Target::Remux("/remux/s/sid/sig/heartbeat".into()), Method::GET),
            (Target::Remux("/remux/admin/kill".into()), Method::POST),
            (Target::Den(Route::Guest), Method::POST),
        ] {
            assert_eq!(class(target, method.clone()).0, Bulk, "{method}");
        }
    }

    #[test]
    fn media_is_a_get_of_a_streaming_route() {
        use MediaClass::*;
        let get = Method::GET;
        for (path, expect) in [
            ("/play/dQw4w9WgXcQ.mp4", Reel),
            // The bare form, `/play?v=<id>`, is served too.
            ("/play", Reel),
            ("/progressive/dQw4w9WgXcQ.mp4", Reel),
            ("/hls/seg", Reel),
            ("/m/s/seg", Reel),
            ("/m/n/blob", Reel),
        ] {
            assert_eq!(class(Target::Reel(path.into()), get.clone()), (Lane::Bulk, Some(expect)), "{path}");
        }
        for path in ["/hls/abc.m3u8", "/direct/x.json", "/sources/x.json", "/meta/movie/tt1.json"] {
            assert_eq!(class(Target::Reel(path.into()), get.clone()).1, None, "{path}");
        }
        for path in [
            "/remux/s/sid/sig/seg0.m4s",
            "/remux/s/sid/sig/seg1234.m4s",
            "/remux/s/sid/sig/init.mp4",
            "/remux/s/sid/sig/speed",
            "/remux/speed",
        ] {
            assert_eq!(class(Target::Remux(path.into()), get.clone()), (Lane::Bulk, Some(Remux)), "{path}");
        }
        for path in
            ["/remux/s/sid/sig/master.m3u8", "/remux/s/sid/sig/media.m3u8", "/remux/s/sid/sig/sub0.vtt"]
        {
            assert_eq!(class(Target::Remux(path.into()), get.clone()).1, None, "{path}");
        }
        // A HEAD has no body to stream, and a POST is not a read.
        assert_eq!(class(Target::Remux("/remux/s/sid/sig/seg0.m4s".into()), Method::HEAD).1, None);
        assert_eq!(class(Target::Reel("/play/x.mp4".into()), Method::POST).1, None);
    }

    /// A service's own port is classified like its prefix on the main port: the same routes are control,
    /// bulk and media, whichever way they arrive.
    #[test]
    fn a_legacy_listener_is_classified_like_its_prefix() {
        let get = Method::GET;
        for (service, path) in [
            (Service::Scout, "/CONFIG/stream/movie/tt1.json"),
            (Service::Reel, "/meta/movie/tt1.json"),
            (Service::Reel, "/play/x.mp4"),
            (Service::Reel, "/health"),
            (Service::Subs, "/CONFIG/manifest.json"),
            (Service::Remux, "/remux/s/sid/sig/seg0.m4s"),
            (Service::Remux, "/remux/health"),
            // den-update probes the bare path on Remux's own port.
            (Service::Remux, "/health"),
            (Service::Reel, "/CONFIG/manifest.json"),
            (Service::Reel, "/configure"),
            (Service::Reel, "/play"),
            (Service::Remux, "/metrics"),
        ] {
            let on_port = classify(&legacy(service, path), &get);
            let mounted = match service {
                Service::Scout => Target::Scout(path.into()),
                Service::Reel => Target::Reel(path.into()),
                Service::Subs => Target::Subs(path.into()),
                Service::Remux => Target::Remux(path.into()),
            };
            assert_eq!(on_port, classify(&mounted, &get), "{service:?} {path}");
        }
        assert_eq!(classify(&legacy(Service::Reel, "/play/x.mp4"), &get).media, Some(MediaClass::Reel));
        assert_eq!(
            classify(&legacy(Service::Remux, "/remux/s/sid/sig/init.mp4"), &get).media,
            Some(MediaClass::Remux)
        );
        // Scout never owns a media permit, on any listener.
        assert_eq!(classify(&legacy(Service::Scout, "/p/ticket"), &get).media, None);
    }

    #[test]
    fn health_is_hearths_own() {
        assert_eq!(target("/health"), Target::Health);
        assert_eq!(target("/den/health"), Target::Health);
    }

    #[test]
    fn reel_and_subs_lose_their_mount() {
        assert_eq!(target("/reel/play/dQw4w9WgXcQ.mp4"), Target::Reel("/play/dQw4w9WgXcQ.mp4".into()));
        assert_eq!(target("/reel/health"), Target::Reel("/health".into()));
        assert_eq!(target("/reel"), Target::Reel("/".into()));
        assert_eq!(
            target("/subs/CONFIG/subtitles/movie/tt1.json"),
            Target::Subs("/CONFIG/subtitles/movie/tt1.json".into())
        );
        assert_eq!(target("/subs/"), Target::Subs("/".into()));
    }

    #[test]
    fn scout_loses_its_mount() {
        assert_eq!(target("/scout/CONFIG/manifest.json"), Target::Scout("/CONFIG/manifest.json".into()));
        assert_eq!(target("/scout/p/ticket"), Target::Scout("/p/ticket".into()));
        assert_eq!(target("/scout"), Target::Scout("/".into()));
        assert_eq!(target("/scouting"), Target::NotFound);
    }

    #[test]
    fn a_legacy_listener_never_touches_the_path() {
        assert_eq!(
            legacy(Service::Scout, "/CONFIG/stream/movie/tt1.json"),
            Target::Scout("/CONFIG/stream/movie/tt1.json".into())
        );
        assert_eq!(legacy(Service::Scout, "/health"), Target::Scout("/health".into()));
        assert_eq!(legacy(Service::Scout, "/scout/health"), Target::Scout("/scout/health".into()));
        assert_eq!(legacy(Service::Reel, "/play/x.mp4"), Target::Reel("/play/x.mp4".into()));
        assert_eq!(legacy(Service::Reel, "/reel/x"), Target::Reel("/reel/x".into()));
        assert_eq!(
            legacy(Service::Subs, "/CONFIG/manifest.json"),
            Target::Subs("/CONFIG/manifest.json".into())
        );
        assert_eq!(legacy(Service::Remux, "/remux/session"), Target::Remux("/remux/session".into()));
        assert_eq!(legacy(Service::Remux, "/metrics"), Target::Remux("/metrics".into()));
    }

    #[test]
    fn remux_keeps_its_own_prefix() {
        assert_eq!(target("/remux/session"), Target::Remux("/remux/session".into()));
        assert_eq!(
            target("/remux/s/abc/def/master.m3u8"),
            Target::Remux("/remux/s/abc/def/master.m3u8".into())
        );
        assert_eq!(target("/remux/health"), Target::Remux("/remux/health".into()));
    }

    #[test]
    fn remux_metrics_is_reached_through_its_mount() {
        assert_eq!(target("/remux/metrics"), Target::Remux("/metrics".into()));
    }

    #[test]
    fn a_longer_name_is_not_a_mount() {
        for path in
            ["/reeling", "/subsonic", "/remuxed", "/dentist", "/", "/play/x.mp4", "/CONFIG/manifest.json"]
        {
            assert_eq!(target(path), Target::NotFound, "{path}");
        }
    }

    #[test]
    fn the_den_addon_loses_its_mount() {
        assert_eq!(target("/den"), Target::Den(Route::Configure));
        assert_eq!(target("/den/CFG/manifest.json"), Target::Den(Route::Installed("CFG".into())));
        assert_eq!(target("/den/CFG/playback"), Target::Den(Route::Playback("CFG".into())));
        assert_eq!(
            target("/den/CFG/stream/movie/tt1.json"),
            Target::Den(Route::Scout("/CFG/stream/movie/tt1.json".into()))
        );
    }

    /// A resource under `/den` is admitted exactly as the same path under its service's own mount.
    #[test]
    fn the_den_addon_is_classified_like_the_services_it_routes_to() {
        let get = Method::GET;
        for path in [
            "/CFG/stream/movie/tt1.json",
            "/p/ticket",
            "/CFG/subtitles/movie/tt1.json",
            "/CFG/meta/movie/tt1.json",
            "/play/x.mp4",
            "/progressive/x.mp4",
            "/m/s/blob",
            "/hls/x.m3u8",
        ] {
            let den = classify(&target(&format!("/den{path}")), &get);
            let own = match crate::den::route(path) {
                Route::Scout(p) => classify(&Target::Scout(p), &get),
                Route::Subs(p) => classify(&Target::Subs(p), &get),
                Route::Reel(p) => classify(&Target::Reel(p), &get),
                other => panic!("{path} routed to {other:?}"),
            };
            assert_eq!(den, own, "{path}");
        }
        assert_eq!(class(target("/den/play/x.mp4"), get.clone()), (Lane::Bulk, Some(MediaClass::Reel)));
        assert_eq!(class(target("/den/CFG/stream/movie/tt1.json"), get.clone()), (Lane::Bulk, None));
        for path in
            ["/den", "/den/configure", "/den/manifest.json", "/den/config-key", "/den/CFG/manifest.json"]
        {
            assert_eq!(class(target(path), get.clone()).0, Lane::Control, "{path}");
            assert_eq!(class(target(path), Method::POST).0, Lane::Bulk, "{path}");
        }
        assert_eq!(class(target("/den/nope/nope"), get).0, Lane::Bulk);
    }
}
