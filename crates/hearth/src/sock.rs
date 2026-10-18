//! Kernel socket-buffer ceilings for accepted connections (den-edge#153).
//!
//! Rust-side bounds (one-frame channels, permits) say nothing about the kernel's TCP queues, which
//! Linux autotunes upward per socket. Hyper can write a whole response into a slow reader's send queue,
//! finish the body, and release its media permit while megabytes stay charged to the container's cgroup:
//! in den-edge's soak, RSS read 7 MiB while the cgroup sat at 63 of 64 MiB. Setting SO_SNDBUF/SO_RCVBUF
//! turns autotuning off for the socket and fixes the bound, so memory tracks the permits again.

use std::io;
use std::os::fd::AsFd;

/// Requested send queue when `HEARTH_SNDBUF` is unset: the part that holds a response the client has not read
/// yet. It is also the window a connection can have in flight, so it caps throughput at about
/// `send queue / round-trip time`: 256 KiB allows ~42 Mbit/s at 50 ms and ~21 Mbit/s at 100 ms (the kernel
/// counts twice the requested size, ~512 KiB, against the cgroup). The first choice, 64 KiB, capped a 50 ms
/// connection near 10 Mbit/s, below a 4K remux. LAN clients (RTT ~1 ms) are far from either limit. The
/// kernel also clamps a request to `net.core.wmem_max` (208 KiB by default), which a node serving distant
/// clients should raise to match.
pub const SEND_BUFFER: usize = 256 * 1024;
/// The smallest `HEARTH_SNDBUF` taken: below this a connection stalls on every segment.
const MIN_SEND_BUFFER: usize = 16 * 1024;

/// The send queue requested for each accepted socket: `HEARTH_SNDBUF` (bytes) or `SEND_BUFFER`.
pub fn send_buffer() -> usize {
    static SIZE: std::sync::OnceLock<usize> = std::sync::OnceLock::new();
    *SIZE.get_or_init(|| match std::env::var("HEARTH_SNDBUF") {
        Err(_) => SEND_BUFFER,
        Ok(raw) => match raw.trim().parse::<usize>() {
            Ok(size) if size >= MIN_SEND_BUFFER => size,
            _ => {
                eprintln!(
                    "HEARTH_SNDBUF={raw:?} is not a size of at least {MIN_SEND_BUFFER} bytes; using {SEND_BUFFER}"
                );
                SEND_BUFFER
            }
        },
    })
}
/// Requested receive queue: requests are small.
pub const RECV_BUFFER: usize = 32 * 1024;
/// Linux reports twice the requested size, counting its own bookkeeping. At most that is honoured.
const ACCOUNTING_FACTOR: usize = 2;

/// Cap `stream`'s kernel queues and read back what the platform actually granted. An error means the
/// ceiling is not in force; the caller refuses the connection rather than serve it unbounded.
pub fn bound_connection_buffers(stream: &impl AsFd) -> io::Result<()> {
    let socket = socket2::SockRef::from(stream);
    let send_ceiling = send_buffer();
    socket.set_send_buffer_size(send_ceiling)?;
    socket.set_recv_buffer_size(RECV_BUFFER)?;
    let send = socket.send_buffer_size()?;
    let recv = socket.recv_buffer_size()?;
    // The readback means "the ceiling holds" only where SO_*BUF turns autotuning off (Linux, which is what
    // ships). macOS reports the queue it has already grown for bytes that arrived before this call, so a
    // developer's machine would refuse a connection that sent its request head early.
    let enforced = cfg!(target_os = "linux");
    if enforced && (send > send_ceiling * ACCOUNTING_FACTOR || recv > RECV_BUFFER * ACCOUNTING_FACTOR) {
        return Err(io::Error::other(format!(
            "effective buffers exceed the ceilings (send={send}, recv={recv})"
        )));
    }
    Ok(())
}

/// The listening sockets the supervisor handed over (systemd socket activation: `LISTEN_PID` is this process,
/// `LISTEN_FDS` of them from fd 3). The supervisor keeps them open while this process is replaced, so a connection
/// made during a restart waits in the kernel queue for the next process instead of being refused. Empty when
/// nothing was handed over.
pub fn inherited_listeners() -> io::Result<Vec<std::net::TcpListener>> {
    use std::os::fd::FromRawFd;
    activation_fds(
        std::env::var("LISTEN_PID").ok().as_deref(),
        std::env::var("LISTEN_FDS").ok().as_deref(),
        std::process::id(),
    )
    .map(|fd| {
        // SAFETY: the activation protocol hands this process sole ownership of these fds.
        let listener = unsafe { std::net::TcpListener::from_raw_fd(fd) };
        listener.set_nonblocking(true)?;
        Ok(listener)
    })
    .collect()
}

/// The fds handed to process `me`: from 3, `LISTEN_FDS` of them, when `LISTEN_PID` names it.
fn activation_fds(listen_pid: Option<&str>, listen_fds: Option<&str>, me: u32) -> std::ops::Range<i32> {
    const FIRST_FD: i32 = 3;
    let (Some(pid), Some(count)) =
        (listen_pid.and_then(|p| p.parse::<u32>().ok()), listen_fds.and_then(|c| c.parse::<i32>().ok()))
    else {
        return FIRST_FD..FIRST_FD;
    };
    if pid != me {
        eprintln!("LISTEN_FDS is set for process {pid}, not this one ({me}); ignoring it");
        return FIRST_FD..FIRST_FD;
    }
    FIRST_FD..FIRST_FD + count.max(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::AsyncWriteExt;

    #[test]
    fn the_handed_over_fds_are_taken_only_for_this_process() {
        assert_eq!(activation_fds(Some("1"), Some("5"), 1), 3..8);
        assert!(activation_fds(Some("2"), Some("5"), 1).is_empty(), "another process's");
        assert!(activation_fds(None, None, 1).is_empty(), "nothing handed over");
        assert!(activation_fds(Some("1"), Some("junk"), 1).is_empty());
    }

    async fn pair() -> (tokio::net::TcpStream, tokio::net::TcpStream) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let client = tokio::spawn(tokio::net::TcpStream::connect(addr));
        let (server, _) = listener.accept().await.unwrap();
        (server, client.await.unwrap().unwrap())
    }

    #[tokio::test]
    async fn an_accepted_socket_is_held_to_the_ceilings() {
        let (server, _client) = pair().await;
        bound_connection_buffers(&server).unwrap();
        let socket = socket2::SockRef::from(&server);
        assert!(socket.send_buffer_size().unwrap() <= send_buffer() * ACCOUNTING_FACTOR);
        assert!(socket.recv_buffer_size().unwrap() <= RECV_BUFFER * ACCOUNTING_FACTOR);
    }

    /// The point of the ceiling, not just the number: a writer to a client that never reads is stopped
    /// by the kernel after a bounded amount, however much it tries to send.
    #[tokio::test]
    async fn a_writer_to_a_client_that_never_reads_is_stopped_by_the_ceiling() {
        let (mut server, client) = pair().await;
        // Start from the queue the kernel would have grown by itself, so the test fails if the
        // ceiling is not applied rather than passing on a platform whose default happens to be small.
        socket2::SockRef::from(&server).set_send_buffer_size(4 * 1024 * 1024).unwrap();
        bound_connection_buffers(&server).unwrap();
        // The client side bounds its receive queue too, or the loopback path swallows megabytes.
        socket2::SockRef::from(&client).set_recv_buffer_size(RECV_BUFFER).unwrap();
        let chunk = [0u8; 16 * 1024];
        let mut written = 0usize;
        // Each write that cannot finish within the timeout means the queues are full.
        while tokio::time::timeout(std::time::Duration::from_millis(200), server.write_all(&chunk))
            .await
            .is_ok()
        {
            written += chunk.len();
            assert!(written < 8 * 1024 * 1024, "wrote {written} bytes to a client that reads nothing");
        }
        // Send + receive queues, with loopback and accounting slack: far below the 4 MiB asked for above.
        assert!(written <= 2 * 1024 * 1024, "{written} bytes were absorbed by the kernel");
        drop(client);
    }
}
