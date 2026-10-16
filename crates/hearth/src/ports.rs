//! The extra listener each service can have, on the port its old standalone container used, so that
//! everything that already points at that port (tunnels, relays, installed links) keeps working.

use crate::route::Service;

/// `HEARTH_<NAME>_PORT` and the port the old container listened on.
const LEGACY: [(Service, &str, u16); 4] = [
    (Service::Scout, "HEARTH_SCOUT_PORT", 8080),
    (Service::Reel, "HEARTH_REEL_PORT", 8092),
    (Service::Subs, "HEARTH_SUBS_PORT", 8093),
    (Service::Remux, "HEARTH_REMUX_PORT", 8095),
];

/// The legacy listeners to open. Unset takes the old port; empty or `0` disables the listener. A value that
/// is not a port, or a port two listeners (this one's `main` included) would share, is an error: a node
/// that silently skipped a listener would look healthy while its clients got connection refused.
pub fn legacy_ports(
    get: impl Fn(&str) -> Option<String>,
    main: u16,
    enabled: impl Fn(Service) -> bool,
) -> Result<Vec<(Service, u16)>, String> {
    let mut taken: Vec<(u16, String)> = vec![(main, "PORT".to_string())];
    let mut out = Vec::new();
    for (service, name, default) in LEGACY {
        if !enabled(service) {
            continue;
        }
        let port = match get(name) {
            None => default,
            Some(v) if v.trim().is_empty() => continue,
            Some(v) => v.trim().parse::<u16>().map_err(|_| format!("{name}={v:?} is not a port"))?,
        };
        if port == 0 {
            continue;
        }
        if let Some((_, other)) = taken.iter().find(|(p, _)| *p == port) {
            return Err(format!("{name}={port} is already used by {other}"));
        }
        taken.push((port, name.to_string()));
        out.push((service, port));
    }
    Ok(out)
}

pub fn name(service: Service) -> &'static str {
    match service {
        Service::Scout => "scout",
        Service::Reel => "reel",
        Service::Subs => "subs",
        Service::Remux => "remux",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn with(vars: &'static [(&str, &str)]) -> impl Fn(&str) -> Option<String> {
        move |name| vars.iter().find(|(k, _)| *k == name).map(|(_, v)| v.to_string())
    }

    #[test]
    fn unset_takes_each_services_old_port() {
        assert_eq!(
            legacy_ports(with(&[]), 8090, |_| true).unwrap(),
            vec![
                (Service::Scout, 8080),
                (Service::Reel, 8092),
                (Service::Subs, 8093),
                (Service::Remux, 8095)
            ]
        );
    }

    #[test]
    fn empty_or_zero_disables_one_listener() {
        let got = legacy_ports(with(&[("HEARTH_SCOUT_PORT", ""), ("HEARTH_REEL_PORT", "0")]), 8090, |_| true)
            .unwrap();
        assert_eq!(got, vec![(Service::Subs, 8093), (Service::Remux, 8095)]);
    }

    #[test]
    fn a_port_can_be_moved() {
        let got = legacy_ports(with(&[("HEARTH_SUBS_PORT", "9093")]), 8090, |_| true).unwrap();
        assert!(got.contains(&(Service::Subs, 9093)));
    }

    #[test]
    fn a_shared_port_is_an_error() {
        let err = legacy_ports(with(&[("HEARTH_REEL_PORT", "8080")]), 8090, |_| true).unwrap_err();
        assert!(err.contains("HEARTH_REEL_PORT=8080") && err.contains("HEARTH_SCOUT_PORT"), "{err}");
        let err = legacy_ports(with(&[("HEARTH_SCOUT_PORT", "8090")]), 8090, |_| true).unwrap_err();
        assert!(err.contains("PORT"), "{err}");
    }

    #[test]
    fn a_non_port_is_an_error() {
        assert!(legacy_ports(with(&[("HEARTH_SCOUT_PORT", "http")]), 8090, |_| true).is_err());
        assert!(legacy_ports(with(&[("HEARTH_SCOUT_PORT", "70000")]), 8090, |_| true).is_err());
    }

    #[test]
    fn disabled_services_get_no_listener_and_their_values_are_ignored() {
        let got = legacy_ports(with(&[("HEARTH_REEL_PORT", "not-a-port")]), 8090, |service| {
            service == Service::Scout
        })
        .unwrap();
        assert_eq!(got, vec![(Service::Scout, 8080)]);
    }
}
