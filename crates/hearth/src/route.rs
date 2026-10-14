//! Which service a request path belongs to, and the path that service gets to see.

#[derive(Debug, PartialEq, Eq)]
pub enum Target {
    /// Hearth's own `/health`, which reports every service.
    Health,
    /// `/reel/<rest>`, served as `/<rest>`.
    Reel(String),
    /// `/subs/<rest>`, served as `/<rest>`.
    Subs(String),
    /// `/scout/<rest>`, served as `/<rest>`.
    Scout(String),
    /// `/remux/<rest>`, served as it came: Remux's routes already live under `/remux`.
    Remux(String),
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
    if path == "/health" {
        return Target::Health;
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn health_is_hearths_own() {
        assert_eq!(target("/health"), Target::Health);
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
            ["/reeling", "/subsonic", "/remuxed", "/", "/metrics", "/play/x.mp4", "/CONFIG/manifest.json"]
        {
            assert_eq!(target(path), Target::NotFound, "{path}");
        }
    }
}
