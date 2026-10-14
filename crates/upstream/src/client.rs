//! Named outbound clients.
//!
//! A client's behaviour is five settings: how long a connection may take to open, how long a request may take (as a
//! whole, or only between bytes), whether a redirect is followed, who the caller says it is, and which addresses it
//! may reach. Each was chosen on purpose somewhere (a total timeout counts the time a paused player leaves a relay
//! unread; a followed redirect carries a credential to a new host), so a service names its profiles once, in one
//! place, and builds one client per profile.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use reqwest::redirect::{Action, Attempt, Policy};
use reqwest::{Client, ClientBuilder};

use crate::guard::PublicOnly;

/// What a client does with a 3xx.
#[derive(Clone, Copy, Debug, Default)]
pub enum Redirect {
    /// reqwest's own: up to ten hops, every one followed.
    #[default]
    Default,
    /// None. The 3xx is the answer; the caller walks the chain itself, or treats a redirect as a failed read.
    Never,
    /// The caller's verdict on each hop.
    Custom(fn(Attempt<'_>) -> Action),
}

/// Which addresses a client may reach.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub enum Guard {
    /// Anything the machine can route to, through the proxy the environment names.
    #[default]
    None,
    /// No proxy: a proxy is dialled in the destination's place, so the address a caller judged would not be the one
    /// connected to.
    NoProxy,
    /// No proxy, and `host` connects to `addr`, the address the caller judged ([`AddressGuard::vet`](crate::AddressGuard)).
    /// The name is never resolved a second time, so an answer that changes between check and connection is not used.
    Pinned { host: String, addr: SocketAddr },
    /// No proxy, and every address a name resolves to is filtered to the public ones at dial time ([`PublicOnly`]).
    /// A literal is the caller's to judge, per request and per redirect hop.
    PublicDial,
}

/// The settings of one outbound client.
#[derive(Clone, Debug, Default)]
pub struct ClientProfile {
    connect: Option<Duration>,
    total: Option<Duration>,
    read_idle: Option<Duration>,
    redirect: Redirect,
    user_agent: Option<String>,
    guard: Guard,
    pool: Option<(usize, Duration)>,
}

impl ClientProfile {
    /// Every setting at reqwest's default: no timeout, redirects followed, no user agent, no guard.
    pub fn new() -> ClientProfile {
        ClientProfile::default()
    }

    /// How long a connection may take to open.
    pub fn connect(mut self, timeout: Duration) -> ClientProfile {
        self.connect = Some(timeout);
        self
    }

    /// How long a whole request may take, body included. Not for a relay: it counts the time a slow or paused
    /// player leaves the response unread.
    pub fn total(mut self, timeout: Duration) -> ClientProfile {
        self.total = Some(timeout);
        self
    }

    /// How long a read may sit with no byte arriving, timed only while it is being read.
    pub fn read_idle(mut self, timeout: Duration) -> ClientProfile {
        self.read_idle = Some(timeout);
        self
    }

    pub fn redirect(mut self, redirect: Redirect) -> ClientProfile {
        self.redirect = redirect;
        self
    }

    /// Named, so an upstream operator reading their logs can tell who is calling.
    pub fn user_agent(mut self, user_agent: impl Into<String>) -> ClientProfile {
        self.user_agent = Some(user_agent.into());
        self
    }

    pub fn guard(mut self, guard: Guard) -> ClientProfile {
        self.guard = guard;
        self
    }

    /// Idle connections kept per host, and for how long.
    pub fn pool(mut self, max_idle_per_host: usize, idle_timeout: Duration) -> ClientProfile {
        self.pool = Some((max_idle_per_host, idle_timeout));
        self
    }

    pub fn connect_timeout(&self) -> Option<Duration> {
        self.connect
    }

    pub fn total_timeout(&self) -> Option<Duration> {
        self.total
    }

    pub fn read_idle_timeout(&self) -> Option<Duration> {
        self.read_idle
    }

    pub fn redirect_policy(&self) -> Redirect {
        self.redirect
    }

    pub fn user_agent_str(&self) -> Option<&str> {
        self.user_agent.as_deref()
    }

    pub fn address_guard(&self) -> &Guard {
        &self.guard
    }

    pub fn pool_settings(&self) -> Option<(usize, Duration)> {
        self.pool
    }

    /// The builder, for a caller that adds what a profile does not carry.
    pub fn builder(&self) -> ClientBuilder {
        let mut builder = Client::builder();
        if let Some(timeout) = self.connect {
            builder = builder.connect_timeout(timeout);
        }
        if let Some(timeout) = self.total {
            builder = builder.timeout(timeout);
        }
        if let Some(timeout) = self.read_idle {
            builder = builder.read_timeout(timeout);
        }
        match self.redirect {
            Redirect::Default => {}
            Redirect::Never => builder = builder.redirect(Policy::none()),
            Redirect::Custom(verdict) => builder = builder.redirect(Policy::custom(verdict)),
        }
        if let Some(user_agent) = &self.user_agent {
            builder = builder.user_agent(user_agent);
        }
        if let Some((max_idle, idle_timeout)) = self.pool {
            builder = builder.pool_max_idle_per_host(max_idle).pool_idle_timeout(idle_timeout);
        }
        match &self.guard {
            Guard::None => {}
            Guard::NoProxy => builder = builder.no_proxy(),
            Guard::Pinned { host, addr } => builder = builder.no_proxy().resolve(host, *addr),
            Guard::PublicDial => builder = builder.no_proxy().dns_resolver(Arc::new(PublicOnly)),
        }
        builder
    }

    pub fn build(&self) -> Result<Client, reqwest::Error> {
        self.builder().build()
    }
}

#[cfg(test)]
mod tests {
    use std::time::Instant;

    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    use super::*;
    use crate::guard::is_refusal;

    /// A server that answers every connection with `respond`'s bytes, written whole, then closes.
    async fn serve(respond: fn(&str) -> String) -> String {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        tokio::spawn(async move {
            while let Ok((mut sock, _)) = listener.accept().await {
                tokio::spawn(async move {
                    let mut buf = vec![0u8; 8192];
                    let n = sock.read(&mut buf).await.unwrap_or(0);
                    let request = String::from_utf8_lossy(&buf[..n]).into_owned();
                    let _ = sock.write_all(respond(&request).as_bytes()).await;
                    let _ = sock.shutdown().await;
                });
            }
        });
        base
    }

    /// A 10-byte body that arrives one byte per 60 ms, behind its headers: 600 ms in all, never idle for long.
    async fn trickle() -> String {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        tokio::spawn(async move {
            while let Ok((mut sock, _)) = listener.accept().await {
                tokio::spawn(async move {
                    let mut buf = vec![0u8; 8192];
                    let _ = sock.read(&mut buf).await;
                    let _ = sock.write_all(b"HTTP/1.1 200 OK\r\ncontent-length: 10\r\n\r\n").await;
                    for _ in 0..10 {
                        tokio::time::sleep(Duration::from_millis(60)).await;
                        let _ = sock.write_all(b"x").await;
                    }
                });
            }
        });
        base
    }

    /// Headers, then silence.
    async fn stall_after_headers() -> String {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        tokio::spawn(async move {
            while let Ok((mut sock, _)) = listener.accept().await {
                tokio::spawn(async move {
                    let mut buf = vec![0u8; 8192];
                    let _ = sock.read(&mut buf).await;
                    let _ = sock.write_all(b"HTTP/1.1 200 OK\r\ncontent-length: 10\r\n\r\nx").await;
                    tokio::time::sleep(Duration::from_secs(30)).await;
                });
            }
        });
        base
    }

    #[test]
    fn a_new_profile_asks_for_nothing() {
        let p = ClientProfile::new();
        assert_eq!(
            (
                p.connect_timeout(),
                p.total_timeout(),
                p.read_idle_timeout(),
                p.user_agent_str(),
                p.pool_settings()
            ),
            (None, None, None, None, None)
        );
        assert!(matches!(p.redirect_policy(), Redirect::Default));
        assert_eq!(p.address_guard(), &Guard::None);
        assert!(p.build().is_ok());
    }

    #[test]
    fn every_setting_is_carried() {
        let p = ClientProfile::new()
            .connect(Duration::from_secs(1))
            .total(Duration::from_secs(2))
            .read_idle(Duration::from_secs(3))
            .redirect(Redirect::Never)
            .user_agent("den-test/1")
            .guard(Guard::PublicDial)
            .pool(4, Duration::from_secs(90));
        assert_eq!(p.connect_timeout(), Some(Duration::from_secs(1)));
        assert_eq!(p.total_timeout(), Some(Duration::from_secs(2)));
        assert_eq!(p.read_idle_timeout(), Some(Duration::from_secs(3)));
        assert!(matches!(p.redirect_policy(), Redirect::Never));
        assert_eq!(p.user_agent_str(), Some("den-test/1"));
        assert_eq!(p.address_guard(), &Guard::PublicDial);
        assert_eq!(p.pool_settings(), Some((4, Duration::from_secs(90))));
        assert!(p.build().is_ok());
    }

    #[tokio::test]
    async fn the_user_agent_is_sent() {
        let base = serve(|request| {
            let ua = request.lines().find_map(|l| l.strip_prefix("user-agent: ")).unwrap_or("none");
            format!("HTTP/1.1 200 OK\r\ncontent-length: {}\r\n\r\n{ua}", ua.len())
        })
        .await;
        let client = ClientProfile::new().user_agent("den-test/1").build().unwrap();
        assert_eq!(client.get(&base).send().await.unwrap().text().await.unwrap(), "den-test/1");
    }

    #[tokio::test]
    async fn a_redirect_is_followed_only_when_the_profile_follows_one() {
        let base = serve(|request| {
            if request.starts_with("GET /final") {
                "HTTP/1.1 200 OK\r\ncontent-length: 3\r\n\r\nend".to_string()
            } else {
                "HTTP/1.1 302 Found\r\nlocation: /final\r\ncontent-length: 0\r\n\r\n".to_string()
            }
        })
        .await;
        let follows = ClientProfile::new().build().unwrap();
        assert_eq!(follows.get(format!("{base}/start")).send().await.unwrap().text().await.unwrap(), "end");

        let never = ClientProfile::new().redirect(Redirect::Never).build().unwrap();
        let answer = never.get(format!("{base}/start")).send().await.unwrap();
        assert_eq!(answer.status(), 302, "a redirect was followed");

        let custom =
            ClientProfile::new().redirect(Redirect::Custom(|attempt| attempt.stop())).build().unwrap();
        assert_eq!(custom.get(format!("{base}/start")).send().await.unwrap().status(), 302);
    }

    /// The relay profile: no total timeout, so a response that takes longer than any total still arrives whole.
    #[tokio::test]
    async fn a_total_timeout_cuts_a_slow_body_and_its_absence_does_not() {
        let base = trickle().await;
        let unbounded = ClientProfile::new().connect(Duration::from_secs(5)).build().unwrap();
        assert_eq!(unbounded.get(&base).send().await.unwrap().text().await.unwrap().len(), 10);

        let bounded = ClientProfile::new().total(Duration::from_millis(250)).build().unwrap();
        let err = match bounded.get(&base).send().await {
            Ok(resp) => resp.text().await.expect_err("the body outlived the total timeout"),
            Err(e) => e,
        };
        assert!(err.is_timeout(), "{err}");
    }

    /// A read-idle timeout counts only the silence between bytes: the trickle's 600 ms in all passes, a stall fails.
    #[tokio::test]
    async fn a_read_idle_timeout_counts_only_the_silence() {
        let idle = ClientProfile::new().read_idle(Duration::from_millis(250)).build().unwrap();
        assert_eq!(idle.get(trickle().await).send().await.unwrap().text().await.unwrap().len(), 10);

        let started = Instant::now();
        let resp = idle.get(stall_after_headers().await).send().await.unwrap();
        let err = resp.text().await.expect_err("a stalled read never ends");
        assert!(err.is_timeout(), "{err}");
        assert!(started.elapsed() < Duration::from_secs(5));
    }

    #[tokio::test]
    async fn a_public_dial_refuses_a_name_that_resolves_to_a_private_address() {
        let client = ClientProfile::new().guard(Guard::PublicDial).build().unwrap();
        let err = client.get("http://localhost:1/").send().await.expect_err("localhost is not public");
        assert!(is_refusal(&err), "{err:?}");
    }

    #[tokio::test]
    async fn a_pinned_host_connects_to_the_address_that_was_judged() {
        let base = serve(|_| "HTTP/1.1 200 OK\r\ncontent-length: 2\r\n\r\nok".to_string()).await;
        let addr: SocketAddr = base.trim_start_matches("http://").parse().unwrap();
        let client = ClientProfile::new()
            .guard(Guard::Pinned { host: "pinned.invalid".into(), addr })
            .build()
            .unwrap();
        let url = format!("http://pinned.invalid:{}/", addr.port());
        assert_eq!(client.get(url).send().await.unwrap().text().await.unwrap(), "ok");

        let unpinned = ClientProfile::new().guard(Guard::NoProxy).build().unwrap();
        assert!(unpinned.get(format!("http://pinned.invalid:{}/", addr.port())).send().await.is_err());
    }
}
