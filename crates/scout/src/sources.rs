//! Port of `internal/scout/sources.go`. Work package 5.
//!
//! A household's own sources: Stremio stream addons it configured itself — its own MediaFusion, Comet, or
//! Torrentio with its debrid account — named in the install config and scraped beside the indexers.
//!
//! They exist because the shared indexer instances increasingly answer only with a debrid account attached,
//! and minting one server-side (MINT_INDEXER_CONFIGS) sends every household's token to a third party. A
//! household that pastes its own addon link sends its token only where it chose to, and the link stays
//! sealed inside the install config like the token does.
//!
//! The URL is the caller's, so the fetch is guarded where it can't be talked around: at dial time, every
//! address a connection is about to reach must be public. A hostname that resolves to the LAN, loopback or
//! the tailnet is refused however it got there — by DNS, a rebinding answer or a redirect.

use std::error::Error;
use std::io;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::sync::Arc;
use std::time::Duration;

/// Go `maxSources`: how many household sources one config may name.
pub(crate) const MAX_SOURCES: usize = 3;
/// Go `maxSourceURL`: the longest source URL kept.
pub(crate) const MAX_SOURCE_URL: usize = 2048;
/// Go `errNotPublic`: the refusal a source connection gets for a non-public address. A reason constant.
pub(crate) const ERR_NOT_PUBLIC: &str = "household source: refusing a non-public address";

/// Go `normalizeSources` keeps the sources a config may name, as base URLs, in order, without repeats.
pub(crate) fn normalize_sources(raw: &[String]) -> Vec<String> {
    let mut out: Vec<String> = Vec::new();
    for s in raw {
        if let Some(n) = normalize_source(s) {
            if !out.contains(&n) {
                out.push(n);
                if out.len() == MAX_SOURCES {
                    break;
                }
            }
        }
    }
    out
}

/// Go `normalizeSource` turns a pasted addon link into the base its routes hang off: the `/manifest.json` an
/// addon's configure page hands out is dropped, and a `stremio://` install link read as the https address
/// it stands for. Anything but a plain https URL is refused — no credentials, query or fragment, which a
/// stream path would not keep — and so is a host that is literally a non-public address. The dial guard
/// is the real defence; this keeps a link that can never work out of the config.
fn normalize_source(s: &str) -> Option<String> {
    let s = s.trim();
    let s = match s.strip_prefix("stremio://") {
        Some(rest) => format!("https://{rest}"),
        None => s.to_owned(),
    };
    let s = s.strip_suffix("/manifest.json").unwrap_or(&s).trim_end_matches('/').to_owned();
    if s.is_empty() || s.len() > MAX_SOURCE_URL {
        return None;
    }
    let u = url::Url::parse(&s).ok()?;
    if u.scheme() != "https"
        || host_str(&u).is_none_or(str::is_empty)
        || !u.username().is_empty()
        || u.password().is_some()
        || u.query().is_some_and(|q| !q.is_empty())
        || u.fragment().is_some_and(|f| !f.is_empty())
    {
        return None;
    }
    let host = host_str(&u)?;
    if let Ok(ip) = host.parse::<IpAddr>() {
        if !public_addr(ip) {
            return None;
        }
    }
    if host == "localhost" || host.ends_with(".localhost") {
        return None;
    }
    Some(s)
}

/// The host of a URL without the brackets an IPv6 literal carries (Go's `url.URL.Hostname`).
fn host_str(u: &url::Url) -> Option<&str> {
    u.host_str().map(|h| h.trim_start_matches('[').trim_end_matches(']'))
}

/// Address space a household source may not reach, beyond what the standard library classifies as loopback,
/// link-local, multicast or private: the prefix, and its length in bits.
const NON_PUBLIC_PREFIXES: [(IpAddr, u32); 9] = [
    (IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)), 8),
    (IpAddr::V4(Ipv4Addr::new(100, 64, 0, 0)), 10), // carrier-grade NAT — and the tailnet, which is where den lives
    (IpAddr::V4(Ipv4Addr::new(192, 0, 0, 0)), 24),
    (IpAddr::V4(Ipv4Addr::new(198, 18, 0, 0)), 15),
    (IpAddr::V4(Ipv4Addr::new(240, 0, 0, 0)), 4),
    (IpAddr::V6(Ipv6Addr::new(0x64, 0xff9b, 0, 0, 0, 0, 0, 0)), 96), // NAT64, which reaches any IPv4 address, private ones included
    (IpAddr::V6(Ipv6Addr::new(0x64, 0xff9b, 1, 0, 0, 0, 0, 0)), 48), // local-use NAT64
    (IpAddr::V6(Ipv6Addr::new(0x2002, 0, 0, 0, 0, 0, 0, 0)), 16),    // 6to4, which embeds an IPv4 address
    (IpAddr::V6(Ipv6Addr::new(0xfec0, 0, 0, 0, 0, 0, 0, 0)), 10),    // deprecated site-local
];

/// Go `publicAddr`: whether an address is one a source may be fetched from.
pub(crate) fn public_addr(ip: IpAddr) -> bool {
    // Go's `Unmap`: an IPv4-mapped IPv6 address is judged as the IPv4 address it wraps.
    let ip = match ip {
        IpAddr::V6(v6) => v6.to_ipv4_mapped().map_or(IpAddr::V6(v6), IpAddr::V4),
        v4 => v4,
    };
    if !is_global_unicast(ip) || is_private(ip) {
        return false;
    }
    !NON_PUBLIC_PREFIXES.iter().any(|&(net, bits)| prefix_contains(net, bits, ip))
}

/// Go `netip.Addr.IsGlobalUnicast`: not unspecified, loopback, multicast, link-local or the broadcast address.
fn is_global_unicast(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(a) => {
            !(a.is_unspecified() || a.is_loopback() || a.is_multicast() || a.is_link_local() || a == Ipv4Addr::BROADCAST)
        }
        IpAddr::V6(a) => {
            let link_local = a.segments()[0] & 0xffc0 == 0xfe80;
            !(a.is_unspecified() || a.is_loopback() || a.is_multicast() || link_local)
        }
    }
}

/// Go `netip.Addr.IsPrivate`: RFC 1918 and the IPv6 unique-local range.
fn is_private(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(a) => {
            let o = a.octets();
            o[0] == 10 || (o[0] == 172 && (16..=31).contains(&o[1])) || (o[0] == 192 && o[1] == 168)
        }
        IpAddr::V6(a) => a.segments()[0] & 0xfe00 == 0xfc00,
    }
}

/// Go `netip.Prefix.Contains`: whether `ip` lies in the `bits`-long prefix of `net`, and never across families.
fn prefix_contains(net: IpAddr, bits: u32, ip: IpAddr) -> bool {
    let (n, a, width) = match (net, ip) {
        (IpAddr::V4(n), IpAddr::V4(a)) => (u128::from(u32::from(n)), u128::from(u32::from(a)), 32),
        (IpAddr::V6(n), IpAddr::V6(a)) => (u128::from(n), u128::from(a), 128),
        _ => return false,
    };
    let shift = width - bits;
    n >> shift == a >> shift
}

/// Go `newSourceClient` is the client household sources are fetched with. No proxy: a proxy is dialled in the
/// source's place, which would put the guard in front of the proxy instead of the source. Redirects stay on
/// https and stop after three hops.
pub(crate) fn new_source_client() -> reqwest::Client {
    // An error here can only come from the static configuration above, never from the network.
    reqwest::Client::builder()
        .timeout(Duration::from_secs(30))
        .connect_timeout(Duration::from_secs(10))
        .pool_max_idle_per_host(4)
        .pool_idle_timeout(Duration::from_secs(90))
        .no_proxy()
        .dns_resolver(Arc::new(PublicOnly))
        .redirect(reqwest::redirect::Policy::custom(follow_source_redirect))
        .build()
        .expect("the household source client's static configuration builds")
}

/// Go `CheckRedirect` on the source client: https only, and at most three hops. An IP-literal target is
/// judged here as well, since the name resolver never sees a literal.
fn follow_source_redirect(attempt: reqwest::redirect::Attempt) -> reqwest::redirect::Action {
    let next = attempt.url();
    let literal_refused = host_str(next).and_then(|h| h.parse::<IpAddr>().ok()).is_some_and(|ip| !public_addr(ip));
    if next.scheme() != "https" || attempt.previous().len() >= 3 || literal_refused {
        attempt.stop()
    } else {
        attempt.follow()
    }
}

/// Go `refusePrivateDial`, run on the name lookup rather than the connect: the names a source resolves to
/// are filtered down to the public ones before any connection is made, so no answer to the lookup can reach
/// a private address. A host that is only an IP literal never reaches this resolver; those are refused where
/// they are named and where they are redirected to.
struct PublicOnly;

impl reqwest::dns::Resolve for PublicOnly {
    fn resolve(&self, name: reqwest::dns::Name) -> reqwest::dns::Resolving {
        Box::pin(async move {
            let found = tokio::net::lookup_host((name.as_str(), 0)).await?;
            let public: Vec<SocketAddr> = found.filter(|a| public_addr(a.ip())).collect();
            if public.is_empty() {
                let refused: Box<dyn Error + Send + Sync> = Box::new(io::Error::other(ERR_NOT_PUBLIC));
                return Err(refused);
            }
            let addrs: reqwest::dns::Addrs = Box::new(public.into_iter());
            Ok(addrs)
        })
    }
}
