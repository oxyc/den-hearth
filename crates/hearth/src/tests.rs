use super::*;

mod den_addon;
mod wanted_downloads;

fn hearth_with(limits: Limits, token: Option<&str>) -> Arc<Hearth> {
    hearth_den(limits, token, den::Den::new(None, false, Default::default(), None, "none"))
}

fn hearth_den(limits: Limits, token: Option<&str>, den: den::Den) -> Arc<Hearth> {
    hearth_den_with_options(
        limits,
        den,
        Options { metrics_token: token.map(str::to_string), ..Default::default() },
    )
}

fn hearth_den_with_options(limits: Limits, den: den::Den, options: Options) -> Arc<Hearth> {
    // No disk tier: a test must not write the temp dir.
    let settings = || {
        let mut s = den_scout::deps::settings_from_env(|_| String::new());
        s.cache_dir = String::new();
        s
    };
    Hearth::new(
        Some(den_reel::AppState::new(den_reel::Config::from_env())),
        Some(den_subtitles::AppState::new(den_subtitles::Config::from_env())),
        Some(den_remux::AppState::new(den_remux::Config::from_env())),
        Some(scout::Scout::new(settings(), settings(), &den.scout)),
        den,
        limits,
        options,
    )
}

fn hearth() -> Arc<Hearth> {
    hearth_with(Limits::default(), None)
}

fn scout_only() -> Arc<Hearth> {
    let settings = || {
        let mut settings = den_scout::deps::settings_from_env(|_| String::new());
        settings.cache_dir = String::new();
        settings
    };
    let den = den::Den::new(None, false, Default::default(), None, "none");
    Hearth::new(
        None,
        None,
        None,
        Some(scout::Scout::new(settings(), settings(), &den.scout)),
        den,
        Limits::default(),
        Options::default(),
    )
}

async fn send_on(hearth: &Arc<Hearth>, own: Option<Service>, path: &str) -> Response<Body> {
    handle(hearth.clone(), own, false, Request::get(path).body(Empty::<Bytes>::new()).unwrap()).await
}

async fn send(hearth: &Arc<Hearth>, path: &str) -> Response<Body> {
    send_on(hearth, None, path).await
}

async fn json_of(resp: Response<Body>) -> serde_json::Value {
    let bytes = resp.into_body().collect().await.unwrap().to_bytes();
    serde_json::from_slice(&bytes).unwrap_or(serde_json::Value::Null)
}

async fn get_on(own: Option<Service>, path: &str) -> (StatusCode, serde_json::Value) {
    let resp = send_on(&hearth(), own, path).await;
    (resp.status(), json_of(resp).await)
}

#[tokio::test]
async fn unified_playback_is_a_concealed_404_by_default() {
    let request = Request::post("/den/not-a-config/playback")
        .header("content-type", "application/json")
        .header("idempotency-key", "attempt-1")
        .body(Empty::<Bytes>::new())
        .unwrap();
    let response = handle(hearth(), None, false, request).await;
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

async fn get(path: &str) -> (StatusCode, serde_json::Value) {
    get_on(None, path).await
}

#[test]
fn a_bad_value_falls_back_to_the_default() {
    assert_eq!(parsed_or("PORT", None, 8090u16), 8090);
    assert_eq!(parsed_or("PORT", Some("9000".into()), 8090u16), 9000);
    assert_eq!(parsed_or("PORT", Some("http".into()), 8090u16), 8090);
    assert_eq!(parsed_or("HEARTH_WORKER_THREADS", Some("-1".into()), 2usize), 2);
}

#[tokio::test]
async fn a_scout_only_node_hides_other_routes_and_reports_only_scout() {
    let node = scout_only();
    let health = send(&node, "/health").await;
    assert_eq!(health.status(), StatusCode::OK);
    let health = json_of(health).await;
    assert_eq!(health["status"], "ok");
    assert_eq!(health["services"].as_object().unwrap().keys().collect::<Vec<_>>(), ["scout"]);

    for path in ["/reel/health", "/subs/health", "/remux/health", "/den/manifest.json"] {
        assert_eq!(send(&node, path).await.status(), StatusCode::NOT_FOUND, "{path}");
    }
    assert_eq!(send(&node, "/scout/manifest.json").await.status(), StatusCode::NOT_FOUND);
}

/// Scout's header ceiling (Go's MaxHeaderBytes) holds under `/scout` too, not only on its own port.
#[tokio::test]
async fn scout_under_the_main_port_refuses_an_oversized_head() {
    let (status, _) = get(&format!("/scout/{}/stream/movie/tt1.json", "a".repeat(40 << 10))).await;
    assert_eq!(status, StatusCode::REQUEST_HEADER_FIELDS_TOO_LARGE);
    let (status, _) = get(&format!("/scout/{}/stream/movie/tt1.json", "a".repeat(16 << 10))).await;
    assert_ne!(status, StatusCode::REQUEST_HEADER_FIELDS_TOO_LARGE);
}

/// A node with room for two requests, one of them bulk, and one media stream.
fn tiny() -> Arc<Hearth> {
    let limits = Limits {
        admission: admit::Limits {
            connections: 2,
            probation: 1,
            requests: 2,
            bulk_requests: 1,
            wait: Duration::from_millis(5),
            body_idle: Duration::from_secs(30),
        },
        media: media::Limits {
            total: 1,
            reel: 1,
            remux: 1,
            idle: Duration::from_secs(30),
            remux_idle: Duration::from_secs(30),
            lifetime: Duration::from_secs(60),
        },
    };
    hearth_with(limits, Some("secret"))
}

#[tokio::test]
async fn the_control_lane_answers_while_bulk_is_saturated() {
    let hearth = tiny();
    let _bulk = hearth.admission.admit(route::Lane::Bulk).await.unwrap();

    // Bulk is full: a bulk request is refused, promptly and in the stable shape.
    let refused = send(&hearth, "/reel/meta/movie/tt0111161.json").await;
    assert_eq!(refused.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(refused.headers()["retry-after"], "1");
    assert_eq!(json_of(refused).await["error"], "server_busy");

    // The reserved position is still there for health and the combined manifest.
    for path in ["/health", "/den/manifest.json", "/remux/health"] {
        let resp = send(&hearth, path).await;
        assert_eq!(resp.status(), StatusCode::OK, "{path} starved by bulk work");
    }
}

/// Scout and every service's own listener are in the same lanes as the main port: the bulk work of each
/// is refused when bulk is full, and each one's health probe still answers.
#[tokio::test]
async fn every_listener_shares_the_lanes() {
    let hearth = tiny();
    let _bulk = hearth.admission.admit(route::Lane::Bulk).await.unwrap();

    for (own, bulk, control) in [
        (Service::Scout, "/CONFIG/stream/movie/tt1.json", "/health"),
        (Service::Scout, "/p/ticket", "/health"),
        (Service::Reel, "/meta/movie/tt0111161.json", "/health"),
        (Service::Subs, "/CONFIG/subtitles/movie/tt1.json", "/health"),
        (Service::Remux, "/remux/session", "/remux/health"),
    ] {
        let refused = send_on(&hearth, Some(own), bulk).await;
        assert_eq!(refused.status(), StatusCode::SERVICE_UNAVAILABLE, "{own:?} {bulk}");
        assert_eq!(refused.headers()["retry-after"], "1", "{own:?} {bulk}");
        assert_eq!(json_of(refused).await["error"], "server_busy", "{own:?} {bulk}");

        let resp = send_on(&hearth, Some(own), control).await;
        assert_eq!(resp.status(), StatusCode::OK, "{own:?} {control} starved by bulk work");
    }
}

#[tokio::test]
async fn a_refusal_gives_nothing_back_to_leak() {
    let hearth = tiny();
    let held = hearth.admission.admit(route::Lane::Bulk).await.unwrap();
    for _ in 0..20 {
        assert_eq!(send(&hearth, "/reel/nope").await.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(
            send_on(&hearth, Some(Service::Scout), "/nope/nope").await.status(),
            StatusCode::SERVICE_UNAVAILABLE
        );
    }
    drop(held);
    // Every refused request returned whatever it had taken.
    assert_eq!(send(&hearth, "/reel/nope").await.status(), StatusCode::NOT_FOUND);
    assert_eq!(send_on(&hearth, Some(Service::Scout), "/nope/nope").await.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn media_is_refused_with_its_own_stable_503_when_its_permits_are_spent() {
    let hearth = tiny();
    let _stream = hearth.media.try_enter(route::MediaClass::Remux).unwrap();

    let refused = send(&hearth, "/remux/s/abc/def/seg0.m4s").await;
    assert_eq!(refused.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(refused.headers()["retry-after"], "1");
    assert_eq!(json_of(refused).await["error"], "media_busy");
    // The class limit is per class and the total is shared: Reel is refused too.
    assert_eq!(send(&hearth, "/reel/play/dQw4w9WgXcQ.mp4").await.status(), StatusCode::SERVICE_UNAVAILABLE);

    // Playlists are not media: they do not need a stream permit, so a player can still ask.
    let playlist = send(&hearth, "/remux/s/abc/def/master.m3u8").await;
    assert_ne!(playlist.status(), StatusCode::SERVICE_UNAVAILABLE);
    // And the request positions the refusals passed through are free again.
    assert_eq!(send(&hearth, "/health").await.status(), StatusCode::OK);
}

/// The streaming services' own ports draw on the same media permits as their prefixes on the main port.
#[tokio::test]
async fn media_on_a_service_port_spends_the_same_permits() {
    let hearth = tiny();
    let _stream = hearth.media.try_enter(route::MediaClass::Reel).unwrap();

    for (own, path) in [
        (Service::Reel, "/play/dQw4w9WgXcQ.mp4"),
        (Service::Reel, "/progressive/dQw4w9WgXcQ.mp4"),
        // The total is shared across classes.
        (Service::Remux, "/remux/s/abc/def/seg0.m4s"),
        (Service::Remux, "/remux/speed"),
    ] {
        let refused = send_on(&hearth, Some(own), path).await;
        assert_eq!(refused.status(), StatusCode::SERVICE_UNAVAILABLE, "{own:?} {path}");
        assert_eq!(json_of(refused).await["error"], "media_busy", "{own:?} {path}");
    }
    // Scout and Subtitles have nothing to stream, so a spent media budget does not reach them.
    for (own, path) in [(Service::Scout, "/health"), (Service::Subs, "/health")] {
        assert_eq!(send_on(&hearth, Some(own), path).await.status(), StatusCode::OK, "{own:?}");
    }
}

#[tokio::test]
async fn metrics_need_the_token_and_name_the_lanes() {
    let hearth = tiny();
    let anonymous = send(&hearth, "/metrics").await;
    assert_eq!(anonymous.status(), StatusCode::NOT_FOUND);

    let req = Request::get("/metrics")
        .header("authorization", "Bearer secret")
        .body(Empty::<Bytes>::new())
        .unwrap();
    let resp = handle(hearth.clone(), None, false, req).await;
    assert_eq!(resp.status(), StatusCode::OK);
    let text = String::from_utf8(resp.into_body().collect().await.unwrap().to_bytes().to_vec()).unwrap();
    for needle in [
        "den_hearth_request_admission_active{lane=\"control\"}",
        "den_hearth_request_admission_refused_total{lane=\"bulk\"}",
        "den_hearth_connection_admission_waited_total",
        "den_hearth_media_active{class=\"remux\"}",
        "den_hearth_media_refused_total{class=\"reel\"}",
        "den_hearth_playback_managed_prepare_total{outcome=\"timeout\"}",
        "den_hearth_playback_operation_total{outcome=\"replay\"}",
    ] {
        assert!(text.contains(needle), "{needle}\n{text}");
    }
    // No token configured, no endpoint.
    assert_eq!(send(&hearth_with(Limits::default(), None), "/metrics").await.status(), StatusCode::NOT_FOUND);
}

/// A service's own `/metrics` is the service's, not hearth's: admission counters live on the main port.
#[tokio::test]
async fn a_service_port_metrics_is_the_services_own() {
    let hearth = tiny();
    let resp = send_on(&hearth, Some(Service::Scout), "/metrics").await;
    // Scout's own endpoint is off without its token: its 404, not hearth's text.
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    let text = String::from_utf8_lossy(&resp.into_body().collect().await.unwrap().to_bytes()).to_string();
    assert!(!text.contains("den_hearth_"), "{text}");
}

async fn serve_on(hearth: Arc<Hearth>, own: Option<Service>) -> (std::net::SocketAddr, watch::Sender<bool>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (stop, stopped) = watch::channel(false);
    tokio::spawn(serve_until(listener, own, hearth, stopped, Duration::from_millis(200)));
    (addr, stop)
}

async fn serve(hearth: Arc<Hearth>) -> (std::net::SocketAddr, watch::Sender<bool>) {
    serve_on(hearth, None).await
}

/// `serve_until` with a drain grace of `grace`, keeping the server's handle so a test can watch it stop.
async fn serve_draining(
    grace: Duration,
) -> (std::net::SocketAddr, watch::Sender<bool>, tokio::task::JoinHandle<bool>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (stop, stopped) = watch::channel(false);
    let server = tokio::spawn(serve_until(listener, None, hearth(), stopped, grace));
    (addr, stop, server)
}

/// A handed-over socket is served as the listener with its port; one on a port the node does not serve is
/// refused rather than left accepting connections nothing answers.
#[test]
fn an_inherited_socket_stands_for_the_listener_on_its_port() {
    let own = [(Service::Scout, 8080), (Service::Reel, 8092), (Service::Remux, 8095)];
    assert_eq!(listener_for(8090, 8090, &own), Some(None));
    assert_eq!(listener_for(8080, 8090, &own), Some(Some(Service::Scout)));
    assert_eq!(listener_for(8095, 8090, &own), Some(Some(Service::Remux)));
    assert_eq!(listener_for(8094, 8090, &own), None);
}

/// The point of the handed-over socket: a connection made while no process is serving (a restart) waits in
/// the kernel queue, and the next process adopts the socket as that service's listener and answers it.
#[tokio::test]
async fn a_connection_queued_on_an_inherited_socket_is_answered_by_the_next_process() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let supervisor = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = supervisor.local_addr().unwrap();
    let mut early = tokio::net::TcpStream::connect(addr).await.unwrap();
    early.write_all(b"GET /health HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n").await.unwrap();
    supervisor.set_nonblocking(true).unwrap();
    let stray = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    assert!(adopt_inherited(vec![stray], 1, &[]).is_err(), "a socket on a port nobody serves");
    let mut adopted = adopt_inherited(vec![supervisor], 1, &[(Service::Scout, addr.port())]).unwrap();
    let (own, listener) = adopted.pop().unwrap();
    assert_eq!(own, Some(Service::Scout));
    let (stop, stopped) = watch::channel(false);
    let server = tokio::spawn(serve_until(listener, own, hearth(), stopped, Duration::from_secs(1)));
    let mut reply = String::new();
    tokio::time::timeout(Duration::from_secs(5), early.read_to_string(&mut reply)).await.unwrap().unwrap();
    assert!(reply.starts_with("HTTP/1.1 200"), "{reply}");
    stop.send(true).unwrap();
    assert!(tokio::time::timeout(Duration::from_secs(3), server).await.unwrap().unwrap());
}

#[tokio::test]
async fn an_idle_server_stops_at_once() {
    let (_, stop, server) = serve_draining(Duration::from_secs(5)).await;
    stop.send(true).unwrap();
    let drained = tokio::time::timeout(Duration::from_secs(1), server)
        .await
        .expect("an idle server waited out the grace instead of stopping")
        .unwrap();
    assert!(drained);
}

/// A graceful shutdown waits for every connection, so without a deadline a client that sends half a
/// request head and goes quiet would hold the stop open for as long as it liked.
#[tokio::test]
async fn a_half_sent_request_cannot_hold_the_stop_open() {
    use tokio::io::AsyncWriteExt;
    let (addr, stop, server) = serve_draining(Duration::from_millis(300)).await;
    let mut sock = tokio::net::TcpStream::connect(addr).await.unwrap();
    sock.write_all(b"GET /health HTTP/1.1\r\nHost: x\r\n").await.unwrap(); // no terminating blank line
    tokio::time::sleep(Duration::from_millis(50)).await; // let the server accept it first
    stop.send(true).unwrap();
    let drained =
        tokio::time::timeout(Duration::from_secs(3), server).await.expect("the drain is unbounded").unwrap();
    assert!(!drained, "the half-sent request should have run out the grace");
}

/// Send one request on `sock` and read the status line back.
async fn status_line(sock: &mut tokio::net::TcpStream, request: &[u8]) -> String {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    sock.write_all(request).await.unwrap();
    let mut buf = [0u8; 256];
    let n = sock.read(&mut buf).await.unwrap();
    String::from_utf8_lossy(&buf[..n]).lines().next().unwrap_or("").to_string()
}

const HEALTH: &[u8] = b"GET /health HTTP/1.1\r\nHost: x\r\n\r\n";

/// The cap is one budget for the node: a connection on a service's own port counts against the same
/// permits as one on the main port, so adding listeners does not add connections.
#[tokio::test]
async fn the_connection_cap_holds_peers_in_the_backlog_not_in_userspace() {
    let hearth = hearth_with(
        Limits {
            // No probation, so the cap itself is what is under test.
            admission: admit::Limits { connections: 1, probation: 0, ..admit::Limits::default() },
            media: media::Limits::default(),
        },
        Some("secret"),
    );
    let (main_addr, _stop_main) = serve(hearth.clone()).await;
    let (scout_addr, _stop_scout) = serve_on(hearth.clone(), Some(Service::Scout)).await;

    let mut first = tokio::net::TcpStream::connect(main_addr).await.unwrap();
    assert!(status_line(&mut first, HEALTH).await.contains("200"));

    // The second connect, to ANOTHER listener, succeeds (the kernel completes the handshake) but is not
    // served: hearth has not accepted it, so it has no task and no buffer.
    let mut second = tokio::net::TcpStream::connect(scout_addr).await.unwrap();
    let held = tokio::time::timeout(Duration::from_millis(300), status_line(&mut second, HEALTH)).await;
    assert!(held.is_err(), "a connection beyond the cap was served");

    // The first closes, capacity returns, and the waiting peer is served without reconnecting.
    drop(first);
    let served = tokio::time::timeout(Duration::from_secs(5), async {
        use tokio::io::AsyncReadExt;
        let mut buf = [0u8; 256];
        let n = second.read(&mut buf).await.unwrap();
        String::from_utf8_lossy(&buf[..n]).lines().next().unwrap_or("").to_string()
    })
    .await
    .expect("the waiting peer was never served");
    assert!(served.contains("200"), "{served}");
    let mut text = String::new();
    hearth.admission.render(&mut text);
    // The accept loops waited for capacity while the first connection held it (and again for the
    // second's, which is why this is not an exact count).
    assert!(!text.contains("den_hearth_connection_admission_waited_total 0"), "{text}");
    assert!(text.contains("den_hearth_connection_admission_high_water 1"), "never more than the cap: {text}");
}

/// A full connection pool must not keep a new control connection out: past the cap a few connections are
/// accepted on probation, answer control routes once and are closed, and refuse everything else in the
/// stable shape.
#[tokio::test]
async fn a_full_pool_still_answers_a_new_control_connection() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let hearth = hearth_with(
        Limits {
            admission: admit::Limits { connections: 1, probation: 2, ..admit::Limits::default() },
            media: media::Limits::default(),
        },
        Some("secret"),
    );
    let (addr, _stop) = serve(hearth.clone()).await;

    // A persistent client takes the only general position and keeps it.
    let mut holder = tokio::net::TcpStream::connect(addr).await.unwrap();
    assert!(status_line(&mut holder, HEALTH).await.contains("200"));

    // New control connections are served however long the holder stays, and are closed after one answer.
    for _ in 0..6 {
        let mut probe = tokio::net::TcpStream::connect(addr).await.unwrap();
        probe.write_all(HEALTH).await.unwrap();
        let mut all = Vec::new();
        tokio::time::timeout(Duration::from_secs(5), probe.read_to_end(&mut all))
            .await
            .expect("a probation connection must be closed after its answer")
            .unwrap();
        let text = String::from_utf8_lossy(&all).to_string();
        assert!(text.starts_with("HTTP/1.1 200"), "{text}");
        assert!(text.to_ascii_lowercase().contains("connection: close"), "{text}");
    }

    // A non-control request on probation is told so, in the contract's shape, and the connection closes.
    let mut bulk = tokio::net::TcpStream::connect(addr).await.unwrap();
    bulk.write_all(b"GET /reel/meta/movie/tt0111161.json HTTP/1.1\r\nHost: x\r\n\r\n").await.unwrap();
    let mut all = Vec::new();
    tokio::time::timeout(Duration::from_secs(5), bulk.read_to_end(&mut all)).await.unwrap().unwrap();
    let text = String::from_utf8_lossy(&all).to_ascii_lowercase();
    assert!(text.starts_with("http/1.1 503"), "{text}");
    assert!(text.contains("retry-after: 1") && text.contains("server_busy"), "{text}");

    // The holder was never disturbed. (`status_line` read only the head of its first answer: drain the rest.)
    let mut rest = [0u8; 8192];
    let _ = tokio::time::timeout(Duration::from_millis(100), holder.read(&mut rest)).await;
    assert!(status_line(&mut holder, HEALTH).await.contains("200"));
    let mut metrics = String::new();
    hearth.admission.render(&mut metrics);
    assert!(metrics.contains("den_hearth_connection_admission_probation_total 7"), "{metrics}");
    assert!(metrics.contains("den_hearth_connection_admission_probation_refused_total 1"), "{metrics}");
}

/// A position that frees up while the loop waits goes to the next peer as an ordinary connection, which
/// may then ask anything: probation is for overflow, not for the first connection after a busy moment.
#[tokio::test]
async fn a_connection_after_the_pool_frees_is_not_on_probation() {
    let hearth = hearth_with(
        Limits {
            admission: admit::Limits { connections: 1, probation: 1, ..admit::Limits::default() },
            media: media::Limits::default(),
        },
        Some("secret"),
    );
    let (addr, _stop) = serve(hearth.clone()).await;
    let mut holder = tokio::net::TcpStream::connect(addr).await.unwrap();
    assert!(status_line(&mut holder, HEALTH).await.contains("200"));
    // The loop is now parked in `accept` holding a probation permit; free the general position.
    drop(holder);
    tokio::time::sleep(Duration::from_millis(100)).await;
    let mut next = tokio::net::TcpStream::connect(addr).await.unwrap();
    let line = status_line(&mut next, b"GET /reel/nope HTTP/1.1\r\nHost: x\r\n\r\n").await;
    assert!(line.contains("404"), "an ordinary connection was refused as overflow: {line}");
}

async fn head_status(addr: std::net::SocketAddr, pad: usize) -> String {
    use tokio::io::AsyncWriteExt;
    let mut sock = tokio::net::TcpStream::connect(addr).await.unwrap();
    let head = format!("GET /health HTTP/1.1\r\nHost: x\r\nX-Pad: {}\r\n\r\n", "a".repeat(pad));
    let _ = sock.write_all(head.as_bytes()).await;
    let mut buf = [0u8; 256];
    let n = tokio::io::AsyncReadExt::read(&mut sock, &mut buf).await.unwrap_or(0);
    String::from_utf8_lossy(&buf[..n]).lines().next().unwrap_or("").to_string()
}

#[tokio::test]
async fn a_request_head_past_the_read_buffer_is_refused_not_buffered() {
    let hearth = hearth();
    // Reel's port holds the 16 KiB ceiling; Scout's and the main port hold 32 KiB because a sealed
    // config rides in the URL.
    let (reel, _a) = serve_on(hearth.clone(), Some(Service::Reel)).await;
    let (scout, _b) = serve_on(hearth.clone(), Some(Service::Scout)).await;
    let (main, _c) = serve(hearth.clone()).await;

    let over_reel = admit::CONNECTION_READ_BUFFER * 2;
    assert!(!head_status(reel, over_reel).await.contains("200"), "an oversized head was served");
    // A head between the two ceilings: refused where the buffer is 16 KiB, served where it is 32 KiB.
    let between = admit::CONNECTION_READ_BUFFER + 4096;
    assert!(!head_status(reel, between).await.contains("200"));
    assert!(head_status(scout, between).await.contains("200"));
    assert!(head_status(main, between).await.contains("200"));
    let over_scout = admit::SCOUT_READ_BUFFER * 2;
    assert!(!head_status(scout, over_scout).await.contains("200"));
    assert!(!head_status(main, over_scout).await.contains("200"));

    // A normal head still works on a fresh connection.
    assert!(head_status(reel, 16).await.contains("200"));
}

#[tokio::test]
async fn legacy_ports_no_longer_install_individual_addons() {
    for service in [Service::Scout, Service::Reel, Service::Subs] {
        assert_eq!(get_on(Some(service), "/manifest.json").await.0, StatusCode::NOT_FOUND);
    }
    let (status, remux) = get_on(Some(Service::Remux), "/remux/health").await;
    assert_eq!(status, StatusCode::OK);
    assert!(remux["status"].is_string(), "{remux}");
}

#[tokio::test]
async fn a_legacy_port_answers_health_as_its_service_not_as_hearth() {
    for service in [Service::Scout, Service::Reel, Service::Subs] {
        let (status, body) = get_on(Some(service), "/health").await;
        assert_eq!(status, StatusCode::OK, "{service:?}");
        assert!(body["status"].is_string(), "{service:?}: {body}");
        assert!(body.get("services").is_none(), "{service:?} answered with hearth's combined health");
    }
}

#[tokio::test]
async fn a_legacy_port_does_not_strip_or_honour_prefixes() {
    // The path reaches the service as it came, so a prefix is the service's to judge (Scout reads
    // `scout` as a config segment and refuses it; Reel and Subtitles have no such route): never the
    // manifest the prefixed path would have served on :8090.
    for (service, path) in [
        (Service::Scout, "/scout/manifest.json"),
        (Service::Reel, "/reel/manifest.json"),
        (Service::Subs, "/subs/manifest.json"),
        // Another service's prefix is not routed from a legacy port either.
        (Service::Scout, "/reel/manifest.json"),
    ] {
        let (status, body) = get_on(Some(service), path).await;
        assert!(status.is_client_error(), "{service:?} {path}: {status}");
        assert!(body["id"].is_null(), "{service:?} {path}: {body}");
    }
}

#[tokio::test]
async fn the_main_port_keeps_its_prefixes_and_scout_among_them() {
    assert_eq!(get("/scout/manifest.json").await.0, StatusCode::NOT_FOUND);
    assert_eq!(get("/scout/CONFIG/manifest.json").await.0, StatusCode::NOT_FOUND);
    assert_eq!(get("/scout/config-key").await.0, StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn each_prefix_reaches_its_own_service() {
    assert_eq!(get("/reel/health").await.0, StatusCode::OK);
    assert_eq!(get("/subs/health?x=1").await.0, StatusCode::OK);
    // Remux is not stripped: its health answer lives at /remux/health.
    let (status, remux) = get("/remux/health").await;
    assert_eq!(status, StatusCode::OK);
    assert!(remux["status"].is_string(), "{remux}");
}

#[tokio::test]
async fn the_updater_manifest_probe_proves_the_combined_hearth_release() {
    let root = send(&hearth(), "/manifest.json").await;
    assert_eq!(root.status(), StatusCode::OK);
    assert_eq!(root.headers()["content-type"], "application/json");
    let root = json_of(root).await;
    assert_eq!(root["id"], "com.den.hearth");
    assert_eq!(root["version"], env!("CARGO_PKG_VERSION"));

    let mounted = json_of(send(&hearth(), "/den/manifest.json").await).await;
    assert_eq!(root, mounted, "the updater alias must be the sole combined manifest");

    assert_eq!(send(&hearth(), "/configure").await.status(), StatusCode::NOT_FOUND);
    for path in ["/scout/manifest.json", "/reel/manifest.json", "/subs/manifest.json"] {
        assert_eq!(send(&hearth(), path).await.status(), StatusCode::NOT_FOUND, "{path}");
    }
}

#[tokio::test]
async fn an_unrouted_path_is_hearths_404_not_a_services() {
    let (status, body) = get("/not-a-route").await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(body["error"], "not_found");
}

#[tokio::test]
async fn health_reports_all_four() {
    let (status, body) = get("/health").await;
    assert_eq!(status, StatusCode::OK);
    for service in ["scout", "reel", "subs", "remux"] {
        assert!(body["services"][service]["status"].is_string(), "{service}: {body}");
    }
    let all_ok = ["scout", "reel", "subs", "remux"].iter().all(|s| body["services"][s]["status"] == "ok");
    assert_eq!(body["status"], if all_ok { "ok" } else { "degraded" });

    let (mounted_status, mounted_body) = get("/den/health").await;
    assert_eq!(mounted_status, StatusCode::OK);
    assert_eq!(mounted_body, body, "the routes-v1 probe sees Hearth's aggregate health");
}

#[tokio::test]
async fn public_torbox_status_warns_without_becoming_a_liveness_gate() {
    let node = hearth();
    let before = json_of(send(&node, "/den/health").await).await;
    node.scout.as_ref().unwrap().runtime().torbox_public_status_for_test("down");
    let body = json_of(send(&node, "/den/health").await).await;

    assert_eq!(body["debrid"]["provider"], "torbox");
    assert_eq!(body["debrid"]["state"], "down");
    assert_eq!(body["debrid"]["source"], "status_page");
    assert_eq!(body["debrid"]["statusPage"]["state"], "down");
    assert_eq!(body["services"]["scout"]["status"], "ok", "an external page is not Scout liveness");
    assert_eq!(body["status"], before["status"], "an external page cannot change Hearth rollout health");
}
