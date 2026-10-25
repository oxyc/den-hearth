//! Environment configuration and process assembly.

use tokio::sync::watch;

use crate::application::{Hearth, Limits, Options};
use crate::route::Service;
use crate::server::{adopt_inherited, bind, serve_until, shutdown_signal, DRAIN_GRACE};
use crate::{admit, alloc, den, env, inproc, media, playback, ports, scout, services, sock, wanted};

const DEFAULT_PORT: u16 = 8090;

struct Configs {
    port: u16,
    /// The extra listeners: a service and the port its old container used.
    own: Vec<(Service, u16)>,
    reel: Option<den_reel::Config>,
    subs: Option<den_subtitles::Config>,
    remux: Option<den_remux::Config>,
    /// Scout behind `/scout` and on its own listener: the same environment but for the public origin.
    scout_mount: Option<den_scout::deps::Settings>,
    scout_own: Option<den_scout::deps::Settings>,
    den: den::Den,
    /// Where Remux and Subtitles reach Scout in process (`inproc`).
    scout_bases: Vec<inproc::Base>,
    metrics_token: Option<String>,
    managed_playback: bool,
    wanted_file: Option<std::path::PathBuf>,
    services: services::Services,
}

/// The handed-over sockets, each as the listener on its port. One on a port this node does not serve is an error:
async fn run(c: Configs) -> std::io::Result<()> {
    let port = c.port;
    let reel = c.reel.map(den_reel::init);
    let subs = c.subs.map(den_subtitles::init);
    let remux = match c.remux {
        Some(config) => Some(den_remux::init(config).await),
        None => None,
    };
    let scout =
        c.scout_mount.zip(c.scout_own).map(|(mount, own)| scout::Scout::new(mount, own, &c.den.scout));
    let hearth = Hearth::new(
        reel,
        subs,
        remux,
        scout,
        c.den,
        Limits::default(),
        Options {
            metrics_token: c.metrics_token,
            managed_playback: c.managed_playback,
            wanted_file: c.wanted_file,
            controller_socket: std::env::var_os("HEARTH_CONTROLLER_SOCKET").map(Into::into),
        },
    );
    if let Some(scout) = &hearth.scout {
        scout.start_sweep();
    }
    let looks_for_downloads = c.services.den() && hearth.scout.is_some();
    if looks_for_downloads {
        wanted::start(hearth.clone());
    }
    let in_process = inproc::install(&hearth, &c.scout_bases);
    let (stop, shutdown) = watch::channel(false);
    let signal = shutdown_signal();
    tokio::spawn(async move {
        signal.await;
        let _ = stop.send(true);
    });
    // Every port is bound here: it is the address containers on the same network reach this one by. A socket the
    // supervisor handed over is served as well, as the listener with its port, and is the one that stays open while
    // this process is replaced.
    let mut listeners = vec![(None, bind(port).await?)];
    for (service, own_port) in &c.own {
        listeners.push((Some(*service), bind(*own_port).await?));
    }
    listeners.extend(adopt_inherited(sock::inherited_listeners()?, port, &c.own)?);
    let own_listeners = if c.own.is_empty() {
        "none".to_string()
    } else {
        c.own.iter().map(|(s, p)| format!("{} :{p}", ports::name(*s))).collect::<Vec<_>>().join(", ")
    };
    if c.services == services::Services::ALL {
        // Keep the established fleet line byte-for-byte compatible: the deploy probe reads the four versions.
        eprintln!(
            "den-hearth {} listening on :{port} — /scout (den-scout {}), /reel (den-reel {}), /subs (den-subtitles {}), /remux (den-remux {}); own listeners: {own_listeners}",
            env!("CARGO_PKG_VERSION"),
            den_scout::release_version(),
            den_reel::release_version(),
            den_subtitles::release_version(),
            den_remux::release_version(),
        );
    } else {
        eprintln!(
            "den-hearth {} listening on :{port} — services={}; own listeners: {own_listeners}",
            env!("CARGO_PKG_VERSION"),
            c.services.names().join(","),
        );
    }
    eprintln!("{}", alloc::startup_line());
    if let Some(scout) = &hearth.scout {
        eprintln!("scout: {}", scout.summary());
    }
    if c.services.den() {
        eprintln!("{}", hearth.den.summary());
    }
    if looks_for_downloads {
        eprintln!("{}", hearth.wanted.summary());
    }
    eprintln!("managed playback: {}", hearth.playback.summary());
    eprintln!("scout in process for remux and subs at: {}", in_process.join(", "));
    // Each service's own startup line: operators read `transcode=` and the like from them.
    if let Some(reel) = &hearth.reel {
        eprintln!("{}", den_reel::startup_line(reel));
    }
    if let Some(remux) = &hearth.remux {
        eprintln!("{}", den_remux::startup_line(remux));
    }
    if let Some(subs) = &hearth.subs {
        eprintln!("{}", den_subtitles::startup_line(subs));
    }
    let (adm, media) = (admit::Limits::default(), media::Limits::default());
    eprintln!(
        "bounds (all listeners): connections={} requests={} (bulk {}) media={} (reel {}, remux {}) idle={}s lifetime={}s metrics={}",
        adm.connections,
        adm.requests,
        adm.bulk_requests,
        media.total,
        media.reel,
        media.remux,
        media.idle.as_secs(),
        media.lifetime.as_secs(),
        if hearth.metrics_token.is_some() { "on" } else { "off" },
    );

    let servers: Vec<_> = listeners
        .into_iter()
        .map(|(own, listener)| {
            tokio::spawn(serve_until(listener, own, hearth.clone(), shutdown.clone(), DRAIN_GRACE))
        })
        .collect();
    let mut drained = true;
    for server in servers {
        drained &= server.await.unwrap_or(false);
    }

    if let Some(reel) = &hearth.reel {
        den_reel::finish(reel).await;
    }
    if let Some(remux) = &hearth.remux {
        den_remux::finish(remux).await;
    }
    // What the last searches learned, which is otherwise written at most every 15 minutes.
    hearth.wanted.flush(wanted::now_ms(), true);
    if drained {
        eprintln!("shut down cleanly");
    }
    Ok(())
}

/// Benchmark builds only (`--features stub-upstreams`, `bench/http/`): the services' upstream bases are fixed to
/// the public APIs, so a load test that must make no external call points them at local stubs here. The feature
/// is off in every image that ships, because a base taken from the environment would let whoever sets it
/// collect the TMDB and OpenSubtitles keys.
#[cfg(feature = "stub-upstreams")]
fn stub_upstreams(reel: &mut den_reel::Config, subs: &mut den_subtitles::Config) {
    if let Ok(base) = std::env::var("HEARTH_STUB_TMDB") {
        reel.tmdb_base = base;
    }
    if let Ok(base) = std::env::var("HEARTH_STUB_KINOCHECK") {
        reel.kinocheck_base = base;
    }
    if let Ok(base) = std::env::var("HEARTH_STUB_OPENSUBTITLES") {
        subs.os_api_base = base;
    }
    eprintln!("stub-upstreams: the TMDB, KinoCheck and OpenSubtitles bases come from HEARTH_STUB_* (benchmark build)");
}

/// `raw`, the value of variable `name`, parsed as a `T`; `default` when it is unset, and also when it
/// is set but unusable, which is said rather than silently replaced.
pub(crate) fn parsed_or<T: std::str::FromStr + std::fmt::Display>(
    name: &str,
    raw: Option<String>,
    default: T,
) -> T {
    let Some(raw) = raw else { return default };
    match raw.parse() {
        Ok(value) => value,
        Err(_) => {
            eprintln!("{name}={raw:?} is not a valid value; using {default}");
            default
        }
    }
}

pub(crate) fn main() {
    // Read before the runtime exists. Each service gets an explicit prefixed lookup; the process
    // environment is never rewritten while its configuration is assembled.
    let port = parsed_or("PORT", std::env::var("PORT").ok(), DEFAULT_PORT);
    let threads = parsed_or("HEARTH_WORKER_THREADS", std::env::var("HEARTH_WORKER_THREADS").ok(), 2);
    let services = services::Services::parse(std::env::var("HEARTH_SERVICES").ok()).unwrap_or_else(|e| {
        eprintln!("fatal: {e}");
        std::process::exit(1);
    });
    let own = ports::legacy_ports(|name| std::env::var(name).ok(), port, |service| services.has(service))
        .unwrap_or_else(|e| {
            eprintln!("fatal: {e}");
            std::process::exit(1);
        });
    // `HEARTH_PUBLIC_BASE_URL` gives a service the prefix it is mounted under. A service with a listener of
    // its own keeps the origin its own `PUBLIC_BASE_URL` names, as its old container did, so it gets none.
    let mount =
        |service: Service, name: &'static str| (!own.iter().any(|(s, _)| *s == service)).then_some(name);
    #[allow(unused_mut)]
    let mut reel = services.has(Service::Reel).then(|| {
        den_reel::Config::from_lookup(|name| env::lookup("REEL_", mount(Service::Reel, "reel"), name))
    });
    #[allow(unused_mut)]
    let mut subs = services.has(Service::Subs).then(|| {
        den_subtitles::Config::from_lookup(|name| env::lookup("SUBS_", mount(Service::Subs, "subs"), name))
    });
    let mut remux = services
        .has(Service::Remux)
        .then(|| den_remux::Config::from_lookup(|name| env::lookup("REMUX_", None, name)));
    // This reserved origin never leaves the process. Remux still applies its ordinary origin SSRF guards, then
    // `inproc` serves only the combined addon's Scout and Subtitles resource slices for it.
    if services.playback() {
        if let Some(subs) = subs.as_mut() {
            subs.add_scout_origin(playback::DEN_SCOUT_ORIGIN);
        }
        let remux = remux.as_mut().expect("playback selection has remux");
        if !remux.scout_origins.iter().any(|origin| origin == playback::DEN_SCOUT_ORIGIN) {
            remux.scout_origins.push(playback::DEN_SCOUT_ORIGIN.to_string());
        }
        if !remux.subtitle_origins.iter().any(|origin| origin == playback::DEN_SCOUT_ORIGIN) {
            remux.subtitle_origins.push(playback::DEN_SCOUT_ORIGIN.to_string());
        }
    }
    #[cfg(feature = "stub-upstreams")]
    if let (Some(reel), Some(subs)) = (&mut reel, &mut subs) {
        stub_upstreams(reel, subs);
    }
    let scout_settings = |mount| {
        den_scout::deps::settings_from_env(|name| env::lookup("SCOUT_", mount, name).unwrap_or_default())
    };
    let scout_mount = services.has(Service::Scout).then(|| scout_settings(Some("scout")));
    let mut scout_own = services.has(Service::Scout).then(|| scout_settings(None));
    // The startup line names the port Scout answers on.
    let scout_port = own.iter().find(|(s, _)| *s == Service::Scout).map(|(_, p)| *p);
    if let Some(scout_own) = scout_own.as_mut() {
        if let Some(p) = scout_port {
            scout_own.port = p.to_string();
        } else {
            scout_own.port = format!("{port}/scout");
        }
    }
    let scout_bases = if services.has(Service::Scout) {
        inproc::scout_bases(
            port,
            scout_port,
            std::env::var("HEARTH_PUBLIC_BASE_URL").ok().as_deref(),
            &scout_own.as_ref().expect("selected Scout has settings").public_url,
        )
    } else {
        Vec::new()
    };

    // Scout's key as Scout reads it: the Den addon's ring is derived from it when it has none of its own.
    let den = den::Den::from_env(
        |name| std::env::var(name).unwrap_or_default(),
        scout_own.as_ref().map_or("", |settings| settings.config_key.as_str()),
        scout_own.as_ref().map_or("", |settings| settings.config_keys_prev.as_str()),
    );
    // Beside Scout's disk cache, and only while sealing is on, when a config can only be a sealed one: a plaintext
    // config segment on disk would be a debrid token.
    let wanted_file = scout_own
        .as_ref()
        .map(|settings| settings.cache_dir.clone())
        .filter(|dir| !dir.is_empty() && den.scout.keyring.is_some())
        .map(|dir| std::path::Path::new(&dir).join("hearth-wanted.json"));

    let metrics_token = std::env::var("HEARTH_METRICS_TOKEN")
        .or_else(|_| std::env::var("METRICS_TOKEN"))
        .ok()
        .filter(|token| !token.is_empty());
    let managed_playback = services.playback()
        && std::env::var("HEARTH_MANAGED_PLAYBACK")
            .ok()
            .is_some_and(|value| !value.is_empty() && value != "0" && value != "false");
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(threads)
        // Disk-cache calls use `block_in_place`, not queued `spawn_blocking` jobs. Admission permits at most this many
        // request tasks to be inside one, and matching the blocking-thread ceiling keeps slow storage from growing an
        // unbounded set of replacement workers while the remaining Tokio core workers keep serving control traffic.
        .max_blocking_threads(admit::REQUESTS)
        .enable_all()
        .build()
        .expect("tokio runtime");
    let configs = Configs {
        port,
        own,
        reel,
        subs,
        remux,
        scout_mount,
        scout_own,
        den,
        scout_bases,
        metrics_token,
        managed_playback,
        wanted_file,
        services,
    };
    if let Err(e) = rt.block_on(run(configs)) {
        eprintln!("fatal: {e}");
        std::process::exit(1);
    }
}
