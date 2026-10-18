//! Listener ownership, connection serving, and graceful shutdown.

use std::convert::Infallible;
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use hyper::body::Incoming;
use hyper::service::service_fn;
use hyper::Request;
use hyper_util::rt::{TokioIo, TokioTimer};
use hyper_util::server::graceful::GracefulShutdown;
use tokio::net::TcpListener;
use tokio::sync::watch;

use crate::application::{handle, Hearth};
use crate::route::Service;
use crate::{admit, sock};

/// How long in-flight requests get to finish after SIGTERM: under podman's default 10 s stop timeout.
pub(crate) const DRAIN_GRACE: Duration = Duration::from_secs(8);

/// How long a client may take to send a request head.
const HEADER_READ_TIMEOUT: Duration = Duration::from_secs(10);

pub(crate) async fn serve_until(
    listener: TcpListener,
    own: Option<Service>,
    hearth: Arc<Hearth>,
    mut shutdown: watch::Receiver<bool>,
    grace: Duration,
) -> bool {
    let graceful = GracefulShutdown::new();
    // Hyper's adaptive read buffer would otherwise grow to ~400 KiB per connection.
    let mut http = hyper::server::conn::http1::Builder::new();
    http.timer(TokioTimer::new())
        .header_read_timeout(HEADER_READ_TIMEOUT)
        .max_buf_size(admit::read_buffer(own));
    // Connections past the cap: control requests only, one response, a short head timeout.
    let mut probation_http = hyper::server::conn::http1::Builder::new();
    probation_http
        .timer(TokioTimer::new())
        .header_read_timeout(admit::PROBATION_HEADER_TIMEOUT)
        .keep_alive(false)
        .max_buf_size(admit::read_buffer(own));
    loop {
        // Capacity is taken BEFORE `accept`. Once the cap is reached the next peer waits in the kernel's
        // listen backlog and costs hearth no task, buffer or descriptor. Every listener draws on the same
        // cap, so a service's own port cannot add connections on top of the main port's.
        //
        // But a flood of persistent clients can hold every position for as long as it lasts, and a new
        // connection (the container's /health probe) would wait behind it the whole time. So past the cap a
        // few connections are accepted on probation: they may ask control routes only, get one response and
        // are closed (see `handle`). A general position that frees up while the loop waits is preferred.
        let mut probation = None;
        let mut accepted = None;
        let mut connection = hearth.admission.try_connection();
        if connection.is_none() {
            probation = hearth.admission.try_probation();
            tokio::select! {
                biased;
                _ = async { let _ = shutdown.wait_for(|stop| *stop).await; } => break,
                general = hearth.admission.connection() => {
                    connection = Some(general);
                    probation = None;
                }
                pair = listener.accept(), if probation.is_some() => accepted = Some(pair),
            }
        }
        let mut on_probation = false;
        let pair = match accepted {
            Some(pair) => pair,
            None => tokio::select! {
                pair = listener.accept() => pair,
                _ = async { let _ = shutdown.wait_for(|stop| *stop).await; } => break,
            },
        };
        let (stream, peer) = match pair {
            Ok(pair) => pair,
            Err(e) => {
                eprintln!("accept: {e}");
                // The listener stays readable while the process is out of descriptors; back off
                // rather than spin.
                tokio::time::sleep(Duration::from_millis(100)).await;
                continue;
            }
        };
        // A position that freed up while this connection was arriving makes it an ordinary one.
        let connection = match (connection, probation) {
            (Some(general), _) => general,
            (None, Some(extra)) => match hearth.admission.try_connection() {
                Some(general) => general,
                None => {
                    on_probation = true;
                    hearth.admission.probation_accepted();
                    extra
                }
            },
            (None, None) => unreachable!("a connection is accepted only with a permit"),
        };
        // A connection whose kernel queues cannot be bounded is refused rather than served unbounded.
        if let Err(e) = sock::bound_connection_buffers(&stream) {
            eprintln!("refusing {peer}: cannot bound TCP buffers: {e}");
            continue;
        }
        let hearth = hearth.clone();
        let peer = den_remux::Peer(peer.ip());
        let service = service_fn(move |mut req: Request<Incoming>| {
            // Remux decides who is a trusted proxy and who is the visitor from the socket's address.
            req.extensions_mut().insert(peer);
            let hearth = hearth.clone();
            async move { Ok::<_, Infallible>(handle(hearth, own, on_probation, req).await) }
        });
        let builder = if on_probation { &probation_http } else { &http };
        let conn = graceful.watch(builder.serve_connection(TokioIo::new(stream), service));
        tokio::spawn(async move {
            let _connection = connection;
            let _ = conn.await;
        });
    }
    drop(listener);
    tokio::select! {
        _ = graceful.shutdown() => true,
        _ = tokio::time::sleep(grace) => {
            eprintln!("drain deadline ({grace:?}) reached with requests still in flight");
            false
        }
    }
}

/// Resolves on SIGTERM (a redeploy) or SIGINT (a terminal); a second signal exits at once.
///
/// Both are registered NOW, by the caller, not when the future is first polled: until then SIGTERM keeps
/// its default disposition, and a stop in that window would kill the process outright.
pub(crate) fn shutdown_signal() -> impl Future<Output = ()> {
    use tokio::signal::unix::{signal, SignalKind};
    let term = signal(SignalKind::terminate());
    let int = signal(SignalKind::interrupt());
    async move {
        tokio::select! {
            _ = wait_for(term, "SIGTERM") => {}
            _ = wait_for(int, "SIGINT") => {}
        }
        // Tokio does not restore the default disposition when a `Signal` drops, so without this a
        // second ^C would be caught and discarded and the drain could only be left with SIGKILL.
        tokio::spawn(async move {
            tokio::select! {
                _ = quietly(signal(SignalKind::terminate())) => {}
                _ = quietly(signal(SignalKind::interrupt())) => {}
            }
            eprintln!("second signal — exiting without finishing the drain");
            den_remux::kill_live_groups();
            den_reel::kill_live_groups();
            std::process::exit(0);
        });
    }
}

/// Resolve when this signal arrives, or never if it could not be registered: returning at once would
/// stop the server the moment it started.
async fn wait_for(registered: std::io::Result<tokio::signal::unix::Signal>, name: &str) {
    match registered {
        Ok(mut sig) => {
            sig.recv().await;
            eprintln!("{name} — draining in-flight requests");
        }
        Err(e) => {
            eprintln!("{name} handler unavailable ({e}); it will be a hard kill");
            std::future::pending::<()>().await
        }
    }
}

/// Like `wait_for`, but says nothing.
async fn quietly(registered: std::io::Result<tokio::signal::unix::Signal>) {
    match registered {
        Ok(mut sig) => {
            sig.recv().await;
        }
        Err(_) => std::future::pending::<()>().await,
    }
}

pub(crate) fn adopt_inherited(
    inherited: Vec<std::net::TcpListener>,
    main: u16,
    own: &[(Service, u16)],
) -> std::io::Result<Vec<(Option<Service>, TcpListener)>> {
    inherited
        .into_iter()
        .map(|socket| {
            let at = socket.local_addr()?;
            let Some(service) = listener_for(at.port(), main, own) else {
                return Err(std::io::Error::other(format!(
                    "an inherited socket on {at} is none of this node's ports"
                )));
            };
            eprintln!("serving on an inherited socket, {at}");
            Ok((service, TcpListener::from_std(socket)?))
        })
        .collect()
}

/// Which listener a socket on `port` stands for: `Some(None)` the main one, `Some(Some(service))` that service's
/// own, `None` neither.
pub(crate) fn listener_for(port: u16, main: u16, own: &[(Service, u16)]) -> Option<Option<Service>> {
    if port == main {
        return Some(None);
    }
    own.iter().find(|(_, p)| *p == port).map(|(service, _)| Some(*service))
}

pub(crate) async fn bind(port: u16) -> std::io::Result<TcpListener> {
    TcpListener::bind(("0.0.0.0", port))
        .await
        .map_err(|e| std::io::Error::new(e.kind(), format!("cannot listen on :{port}: {e}")))
}
