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
use std::net::{IpAddr, SocketAddr};
use std::sync::{Arc, LazyLock};
use std::time::Duration;

use async_trait::async_trait;

use crate::hostlimit::{new_bounded_host_limiter, HostLimiter};
use crate::scrape::{DoError, Doer};

/// Go `ownSources`: the metrics and log name every household source shares. Its hosts are not used as names: they are
/// chosen by whoever built the config, so a map keyed by them would grow with traffic. (Go types it `Indexer`.)
pub(crate) const OWN_SOURCES: &str = "own";

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
    // Go's parser reads `https:host` as an opaque URL with no host, which is refused; the url crate would read it
    // as `https://host/`, so the authority marker has to be there to begin with.
    if !s.get(..8).is_some_and(|p| p.eq_ignore_ascii_case("https://")) {
        return None;
    }
    let u = url::Url::parse(&s).ok()?;
    // `https://@host` has an empty user, which Go's `u.User != nil` refuses and the url crate does not report.
    // The url crate also reads `https:///host` and a backslash as the `//host` and `/` they stand for in a browser;
    // Go reads the first as an empty host and refuses the second as a character no host may hold.
    let authority = s[8..].split(['/', '?', '#']).next().unwrap_or_default();
    if u.scheme() != "https"
        || host_str(&u).is_none_or(str::is_empty)
        || authority.is_empty()
        || authority.contains(['@', '\\'])
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

/// Go `sourceHost`: the host of a source's base URL, or the shared `own` name when it does not parse.
pub(crate) fn source_host(base: &str) -> String {
    match url::Url::parse(base) {
        Ok(u) => host_str(&u).unwrap_or_default().to_owned(),
        Err(_) => OWN_SOURCES.to_owned(),
    }
}

/// Go `sourceLimiter`: household sources are paced like the indexers, per host, but with a ceiling on how many hosts
/// it remembers: the hosts come from configs anyone can build, and `indexerLimiter` keeps a bucket per host forever.
pub(crate) static SOURCE_LIMITER: LazyLock<HostLimiter> =
    LazyLock::new(|| new_bounded_host_limiter(Duration::from_secs(1), 30, 4096));

/// The host of a URL without the brackets an IPv6 literal carries (Go's `url.URL.Hostname`).
fn host_str(u: &url::Url) -> Option<&str> {
    u.host_str().map(|h| h.trim_start_matches('[').trim_end_matches(']'))
}

/// Go `publicAddr`: whether an address is one a source may be fetched from. The classifier is shared with
/// Subtitles and Remux (`den-net`), so every service refuses the same ranges.
pub(crate) use den_net::is_public as public_addr;

/// Go `newSourceClient` is the client household sources are fetched with. No proxy: a proxy is dialled in the
/// source's place, which would put the guard in front of the proxy instead of the source. Redirects stay on
/// https and stop after three hops.
///
/// Go refuses a non-public address in the dialer's `Control` hook, which sees the address of every connection
/// about to be made. reqwest has no such hook, so the same guarantee is built from three pieces that together
/// cover every way a connection gets its address:
///
///  - a host NAME is resolved by [`PublicOnly`], which drops every non-public answer before anything connects
///    (and connects to exactly the addresses it returned, so a rebinding answer cannot be swapped in later);
///  - a host that is an IP LITERAL never reaches a resolver, so [`SourceClient::do_request`] judges it before the
///    first request goes out;
///  - the same literal check runs on every redirect hop in [`redirect_verdict`], and a redirect to a name goes
///    through the resolver like any other.
#[derive(Clone)]
pub(crate) struct SourceClient {
    client: reqwest::Client,
}

pub(crate) fn new_source_client() -> SourceClient {
    // An error here can only come from the static configuration above, never from the network.
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(30))
        .connect_timeout(Duration::from_secs(10))
        .pool_max_idle_per_host(4)
        .pool_idle_timeout(Duration::from_secs(90))
        .no_proxy()
        .dns_resolver(Arc::new(PublicOnly))
        .redirect(reqwest::redirect::Policy::custom(follow_source_redirect))
        .build()
        .expect("the household source client's static configuration builds");
    SourceClient { client }
}

#[async_trait]
impl Doer for SourceClient {
    async fn do_request(&self, req: reqwest::Request) -> Result<reqwest::Response, DoError> {
        if literal_refused(req.url()) {
            return Err(refused());
        }
        // Without the URL: it carries the household's addon config, which is a credential.
        self.client.execute(req).await.map_err(|e| {
            if is_refusal(&e) {
                refused()
            } else {
                Box::new(e.without_url()) as DoError
            }
        })
    }
}

/// Go `errNotPublic`, as the error a refused connection returns.
fn refused() -> DoError {
    Box::new(io::Error::other(ERR_NOT_PUBLIC))
}

/// Whether the guard is what stopped a request, anywhere along its error chain (the resolver's refusal and a
/// refused redirect both surface as the source of a reqwest error).
fn is_refusal(e: &reqwest::Error) -> bool {
    let mut cause: Option<&(dyn Error + 'static)> = Some(e);
    while let Some(c) = cause {
        if c.to_string() == ERR_NOT_PUBLIC {
            return true;
        }
        cause = c.source();
    }
    false
}

/// Whether a URL names a non-public address directly. A name is not judged here: that is the resolver's job.
fn literal_refused(u: &url::Url) -> bool {
    host_str(u).and_then(|h| h.parse::<IpAddr>().ok()).is_some_and(|ip| !public_addr(ip))
}

/// What the source client does with a redirect.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum RedirectVerdict {
    /// Follow it.
    Follow,
    /// Return the redirect response itself, as Go's `http.ErrUseLastResponse` does.
    Stop,
    /// Fail the request: the redirect points at a non-public address.
    Refuse,
}

/// Go `CheckRedirect` on the source client: https only, and at most three hops (`hops` counts the requests made so
/// far, as Go's `len(via)`). A hop that is allowed and is an IP literal is judged like the first request; a name is
/// resolved, and filtered, when it is connected to.
pub(crate) fn redirect_verdict(next: &url::Url, hops: usize) -> RedirectVerdict {
    if next.scheme() != "https" || hops >= 3 {
        RedirectVerdict::Stop
    } else if literal_refused(next) {
        RedirectVerdict::Refuse
    } else {
        RedirectVerdict::Follow
    }
}

fn follow_source_redirect(attempt: reqwest::redirect::Attempt) -> reqwest::redirect::Action {
    match redirect_verdict(attempt.url(), attempt.previous().len()) {
        RedirectVerdict::Follow => attempt.follow(),
        RedirectVerdict::Stop => attempt.stop(),
        RedirectVerdict::Refuse => attempt.error(io::Error::other(ERR_NOT_PUBLIC)),
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
