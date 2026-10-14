//! Keeping an outbound request off the LAN, the loopback and the tailnet.
//!
//! A URL a household, a browser or an upstream hands over is fetched from inside the box. Three services guard that,
//! each where its shape allows, all judging an address with `den_net::is_public`:
//!
//!  * at dial time ([`PublicOnly`], through a profile's [`Guard::PublicDial`](crate::Guard)): every address a name
//!    resolves to is filtered before anything connects. Scout's household sources;
//!  * per hop, by hand ([`AddressGuard::check`]): the caller walks a redirect chain and judges each hop before it
//!    is requested. Remux's play URLs;
//!  * per hop, pinned ([`AddressGuard::vet`]): the hop is resolved once, judged, and the connection is made to the
//!    address that was judged. Subtitles' resync relay.

use std::error::Error;
use std::io;
use std::net::{IpAddr, SocketAddr};

use reqwest::Url;

/// The text of the refusal a dial or a redirect hop gets for a non-public address.
pub const NOT_PUBLIC: &str = "household source: refusing a non-public address";

/// The host of a URL without the brackets an IPv6 literal carries.
pub fn bare_host(url: &Url) -> Option<&str> {
    url.host_str().map(|h| h.trim_start_matches('[').trim_end_matches(']'))
}

/// Whether a URL names a non-public address directly. A name is not judged here: that is the resolver's job.
pub fn literal_refused(url: &Url) -> bool {
    bare_host(url).and_then(|h| h.parse::<IpAddr>().ok()).is_some_and(|ip| !den_net::is_public(ip))
}

/// Whether the guard is what stopped a request, anywhere along its error chain (the resolver's refusal and a
/// refused redirect both surface as the source of a reqwest error).
pub fn is_refusal(e: &reqwest::Error) -> bool {
    let mut cause: Option<&(dyn Error + 'static)> = Some(e);
    while let Some(c) = cause {
        if c.to_string() == NOT_PUBLIC {
            return true;
        }
        cause = c.source();
    }
    false
}

/// A resolver that drops every non-public answer before anything connects, and refuses a name with none left.
/// reqwest connects to exactly the addresses it returns, so a rebinding answer cannot be swapped in later. A host
/// that is only an IP literal never reaches a resolver: those are refused where they are named
/// ([`literal_refused`]) and where they are redirected to.
pub struct PublicOnly;

impl reqwest::dns::Resolve for PublicOnly {
    fn resolve(&self, name: reqwest::dns::Name) -> reqwest::dns::Resolving {
        Box::pin(async move {
            let found = tokio::net::lookup_host((name.as_str(), 0)).await?;
            let public: Vec<SocketAddr> = found.filter(|a| den_net::is_public(a.ip())).collect();
            if public.is_empty() {
                let refused: Box<dyn Error + Send + Sync> = Box::new(io::Error::other(NOT_PUBLIC));
                return Err(refused);
            }
            let addrs: reqwest::dns::Addrs = Box::new(public.into_iter());
            Ok(addrs)
        })
    }
}

/// Why a host was not let through.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GuardError {
    /// An address literal, or an address a name resolves to, is not on the public internet.
    NotPublic,
    /// The name did not resolve; the resolver's own words.
    Unresolved(String),
    /// The name resolved to nothing.
    Nothing,
}

/// A host that was let through.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Vetted {
    /// An address literal.
    Literal(IpAddr),
    /// A name, and the first address it resolved to: the one to connect to.
    Resolved(SocketAddr),
}

/// A per-hop check of where a URL leads.
#[derive(Clone, Copy, Debug)]
pub struct AddressGuard {
    allow_private: bool,
}

impl AddressGuard {
    /// Refuses every non-public host.
    pub const fn strict() -> AddressGuard {
        AddressGuard { allow_private: false }
    }

    /// Lets every host through (a name is still resolved by [`vet`](Self::vet), which pins what it finds). For a
    /// host the operator named, and for tests whose sources are loopback servers.
    pub const fn permissive() -> AddressGuard {
        AddressGuard { allow_private: true }
    }

    /// `Ok` when `url` may be fetched: an address literal must be public, a name must resolve and every address it
    /// resolves to must be public. A permissive guard resolves nothing.
    pub async fn check(&self, url: &Url) -> Result<(), GuardError> {
        if self.allow_private {
            return Ok(());
        }
        let host = bare_host(url).unwrap_or_default();
        if host.is_empty() {
            return Err(GuardError::NotPublic);
        }
        match self.vet(host, url.port_or_known_default().unwrap_or(443)).await {
            Err(GuardError::Nothing) => Err(GuardError::NotPublic),
            other => other.map(|_| ()),
        }
    }

    /// Judge `host` (no brackets) and, for a name, resolve it once so the caller can connect to the address that
    /// was judged rather than resolving again. Every address a name resolves to must be public: a name that also
    /// answers with an internal address is not one to be trusted with the rest.
    pub async fn vet(&self, host: &str, port: u16) -> Result<Vetted, GuardError> {
        if let Ok(ip) = host.parse::<IpAddr>() {
            return if self.allow_private || den_net::is_public(ip) {
                Ok(Vetted::Literal(ip))
            } else {
                Err(GuardError::NotPublic)
            };
        }
        let addrs: Vec<SocketAddr> = tokio::net::lookup_host((host, port))
            .await
            .map_err(|e| GuardError::Unresolved(e.to_string()))?
            .collect();
        if !self.allow_private && addrs.iter().any(|a| !den_net::is_public(a.ip())) {
            return Err(GuardError::NotPublic);
        }
        addrs.first().copied().map(Vetted::Resolved).ok_or(GuardError::Nothing)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn url(s: &str) -> Url {
        Url::parse(s).unwrap()
    }

    #[tokio::test]
    async fn a_strict_guard_refuses_what_is_not_public() {
        let guard = AddressGuard::strict();
        for refused in [
            "http://127.0.0.1:9/x",
            "http://169.254.169.254/latest/meta-data",
            "http://192.168.86.193:8080/x",
            "http://100.64.0.1/x",
            "http://[::1]/x",
            "http://[fd7a:115c:a1e0::1]/x",
            "http://[::ffff:10.0.0.1]/x",
            "http://localhost/x",
        ] {
            assert_eq!(guard.check(&url(refused)).await, Err(GuardError::NotPublic), "{refused}");
        }
        for public in ["http://93.184.216.34/x", "https://[2606:4700:4700::1111]/x"] {
            assert_eq!(guard.check(&url(public)).await, Ok(()), "{public}");
        }
    }

    #[tokio::test]
    async fn a_permissive_guard_checks_nothing_but_still_pins_a_name() {
        let guard = AddressGuard::permissive();
        assert_eq!(guard.check(&url("http://127.0.0.1:9/x")).await, Ok(()));
        let ip: IpAddr = "10.0.0.5".parse().unwrap();
        assert_eq!(guard.vet("10.0.0.5", 80).await, Ok(Vetted::Literal(ip)));
        let Ok(Vetted::Resolved(addr)) = guard.vet("localhost", 8080).await else {
            panic!("localhost did not resolve")
        };
        assert_eq!(addr.port(), 8080);
        assert!(addr.ip().is_loopback());
    }

    #[tokio::test]
    async fn a_name_must_resolve_to_public_addresses_only() {
        let guard = AddressGuard::strict();
        assert_eq!(guard.vet("localhost", 80).await, Err(GuardError::NotPublic));
        assert!(matches!(guard.vet("no-such-host.invalid", 80).await, Err(GuardError::Unresolved(_))));
        assert_eq!(guard.vet("10.1.2.3", 80).await, Err(GuardError::NotPublic));
        let public: IpAddr = "8.8.8.8".parse().unwrap();
        assert_eq!(guard.vet("8.8.8.8", 80).await, Ok(Vetted::Literal(public)));
    }

    #[test]
    fn a_literal_is_judged_without_a_resolver() {
        assert!(literal_refused(&url("https://[::1]/")));
        assert!(literal_refused(&url("https://10.0.0.1/")));
        assert!(!literal_refused(&url("https://8.8.8.8/")));
        assert!(!literal_refused(&url("https://localhost/")), "a name is the resolver's to judge");
        assert_eq!(bare_host(&url("https://[::1]:8080/")), Some("::1"));
    }
}
