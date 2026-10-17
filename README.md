# den-hearth

Den's self-hosted media node: Scout (stream addon), Reel (trailers, yt-dlp + ffmpeg), Subtitles and Remux (HLS
remux and transcode) in one process and one runtime.

```
crates/
  scout/      den-scout      library: ported from the Go den-scout, which it replaced
  reel/       den-reel       library
  subtitles/  den-subtitles  library
  remux/      den-remux      library
  hearth/     den-hearth     the combined binary
```

Reel, Subtitles and Remux were imported with their full history (`git subtree`). Each is the same code its
repository ships, turned into a library: its old `main.rs` is now `lib.rs`. They no longer have a binary or a
serve loop of their own; hearth is every service's server.

## Scout, and the Go den-scout as the rollback

Scout replaces the Go `den-scout` container. The Go container stays installed but stopped, and rolling back is
starting it again: both listen on :8080, so only one runs at a time. Nothing installed changes either way.

- **Wire compatibility runs both ways.** `crates/scout/src/tests/go_wire_vectors.rs` holds what the live Go code
  minted (config segments, play tickets, play tokens), which Rust must open. `rust_wire_vectors.rs` is the reverse
  and is what makes the rollback safe: what Rust minted, Go's own code opened to the same fields. A ticket or an
  installed link made while Rust ran keeps working after a rollback, and the other way round, as long as
  `SCOUT_CONFIG_KEY` / `SCOUT_CONFIG_KEYS_PREV` are the keys the Go service had.
- **Scout's cache is its own**, `/cache/scout` (`SCOUT_CACHE_DIR`), never the Go service's directory: the two disk
  formats are not proven compatible, so Go keeps its own volume and a rollback starts from the Go cache it left.
- **Environment.** Scout reads the Go service's variables unchanged, with the `SCOUT_` prefix (see Configuration):
  the Go `CONFIG_KEY` becomes `SCOUT_CONFIG_KEY`, `METRICS_TOKEN` becomes `SCOUT_METRICS_TOKEN`, `REMUX_KEY`
  becomes `SCOUT_REMUX_KEY`, `PUBLIC_BASE_URL` becomes `SCOUT_PUBLIC_BASE_URL`, and so on.
- **It runs inside hearth's runtime, supervised the way the others are.** A panic in a request is a 500 for that
  request; the hourly cache sweep is a supervised task; Scout is part of the combined `/health` and the startup
  line; and SIGTERM drains its requests with the rest.

## Run

For a ready-to-use Docker Compose deployment, selective services, persistent cache defaults, LAN HTTP Chromecast
setup, HTTPS options, and Mac/Raspberry Pi notes, see **[Self-host den-hearth](selfhost/README.md)**.

```
cargo run -p den-hearth        # listens on :8090, and on the four ports below
```

You need `ffmpeg` (Reel, Subtitles, Remux), `yt-dlp` (Reel) and `alass` (Subtitles) on the path, as for the
standalone services. Scout needs nothing outside the binary.

## Container image

One image, `den-hearth`, replaces the per-service images (`den-reel`, `den-subtitles`, `den-remux`, and the Go
`den-scout` once switched over). It is
Debian trixie-slim (Reel's yt-dlp needs Python, while deno and MP4Box are glibc builds) with:

| Tool | For | Source |
|---|---|---|
| `den-hearth` | the binary, built with cargo-chef so dependencies are a cached layer | this workspace |
| `ffmpeg` | Reel (mux, cropdetect) and Subtitles (audio decode for sync): broad software input support, only the output/filter/protocol surface they use | checksummed source, `FFMPEG_VERSION` / `FFMPEG_SHA256` |
| `ffmpeg-remux` | Remux: minimal source build, VAAPI, checksummed tarball | `FFMPEG_VERSION` / `FFMPEG_SHA256` |
| `intel-media-va-driver-non-free` | Remux's GPU transcode (UHD 630, driver `iHD`), amd64 only. The free package has no bitrate-controlled encode (CQP only) and no `scale_vaapi` kernels, so the non-free component is enabled for this one install | apt |
| `yt-dlp` zipapp, `deno`, `python3` | Reel's extractor and its resident resolver | pinned versions, SHA-256 checked |
| `MP4Box` + `libgpac` | Reel's `clap` box | GPAC pinned to a commit |
| `alass` | Subtitles' aligner | `cargo install --locked`, pinned |

Both ffmpegs are built from the same pinned source and updated by changing one version/checksum pair. Do not narrow
the software build's decoders or parsers from a sample of current releases: Subtitles may meet any audio codec a
guest's release carries. Its container list covers normal video releases while deliberately excluding playlist and
network-aware demuxers; only Reel/Subtitles' required muxers, encoders, filters and protocols are enabled. The e2e
target is the compatibility gate for changes to that line, including alass's otherwise easy-to-miss raw `s16le`
subprocess call.

It runs as uid/gid 65532 (`nonroot`) with `/cache/{scout,reel,subs,remux}` owned by it and `VOLUME /cache`. It
exposes 8090 (`PORT`) and the four legacy ports 8080, 8092, 8093 and 8095 (see Listeners). `HEALTHCHECK` polls `/health` every 5 minutes. The release number is `VERSION`, passed
as `DEN_VERSION` (all three services report it); it is deliberately not in any `Cargo.toml`.

```
docker build --platform linux/amd64 --build-arg DEN_VERSION="$(cat VERSION)" -t den-hearth:local .
docker run --rm -p 8090:8090 -p 8080:8080 -p 8092:8092 -p 8093:8093 -p 8095:8095 den-hearth:local
curl localhost:8090/health
curl localhost:8090/scout/manifest.json
curl localhost:8090/reel/manifest.json
curl localhost:8090/subs/manifest.json
curl localhost:8090/remux/health
curl localhost:8080/manifest.json      # Scout on its old port, at the root
curl localhost:8092/manifest.json      # Reel
curl localhost:8093/manifest.json      # Subtitles
curl localhost:8095/remux/health       # Remux
docker run --rm --entrypoint ffmpeg-remux den-hearth:local -hwaccels
```

`docker buildx bake` builds the same image from `docker-bake.hcl`; `docker buildx bake e2e` runs Remux's `#[ignore]`d
end-to-end tests against the image's own `ffmpeg-remux`, plus Reel/Subtitles' crop, merge, HTTP relay, raw-sync and
14-codec decode matrix against the image's own software `ffmpeg`. A transcode needs the GPU: pass `--device /dev/dri` and add
the host's `render` gid with `--group-add` (the old `den-remux` quadlet did the same).

On a host with a GPU, `scripts/probe-image.sh` also runs Remux's real encode (`h264_vaapi` with `-b:v/-maxrate/-bufsize`,
after `scale_vaapi`) on a six-frame clip, and by hand it is:

```
docker run --rm -i --device /dev/dri/renderD128 --group-add "$(stat -c %g /dev/dri/renderD128)" \
  --entrypoint ffmpeg-remux den-hearth:local -hide_banner -loglevel error \
  -hwaccel vaapi -hwaccel_device /dev/dri/renderD128 -hwaccel_output_format vaapi -f matroska -i pipe:0 -map 0:V:0 \
  -vf scale_vaapi=w=256:h=144:format=nv12 -c:v h264_vaapi -profile:v high -level:v 4.1 \
  -b:v 4000000 -maxrate 6000000 -bufsize 8000000 -frames:v 6 -f mp4 -movflags frag_keyframe+empty_moov pipe:1 \
  < crates/remux/testdata/gpu-probe.mkv > /dev/null
```

Remux runs the same encode once at start-up; if it fails, transcoding is off and `/remux/health` says
`transcode_unavailable` with ffmpeg's reason in the startup log. `ffmpeg -h encoder=h264_vaapi` listing the encoder
proves nothing: the free `intel-media-va-driver` lists it and then fails with "Driver does not support any RC mode".

CI (`.github/workflows/`): `ci.yml` formats, lints and tests the workspace, runs the e2e stage, and on `main` builds,
probes (`scripts/probe-image.sh`) and scans an immutable candidate. `docker-publish.yml` promotes that digest on a
`v*` tag (cosign keyless signature, `:latest` last) and rebuilds the newest tag weekly without cache for Debian
security fixes. `ytdlp-update.yml` and `deno-update.yml` bump the checksummed downloads. `scripts/release.sh X.Y.Z`
commits `VERSION`, waits for that commit's CI and tags it.

## Routing

| Path | Goes to | The service sees |
|---|---|---|
| `/scout/...` | Scout | `/...` (prefix removed) |
| `/reel/...` | Reel | `/...` (prefix removed) |
| `/subs/...` | Subtitles | `/...` (prefix removed) |
| `/remux/...` | Remux | unchanged (Remux already serves everything under `/remux`) |
| `/remux/metrics` | Remux | `/metrics` (its one route outside `/remux`) |
| `/den/...` | the Den addon: hearth, or the service a resource belongs to | see "The Den addon" |
| `/health` | hearth | all four side by side, below |
| `/metrics` | hearth | its own admission counters (see Bounds); needs `HEARTH_METRICS_TOKEN` (or `METRICS_TOKEN`), else 404 |
| anything else | hearth | 404 |

`/health` answers 200 with `{"status": "ok"|"degraded", "services": {"scout": {...}, "reel": {...}, "subs": {...},
"remux": {...}}}`, each entry being that service's own `/health` body. It is `degraded` when any service
says anything but `ok`. Each service's own `/health` and `/metrics` stay reachable under its prefix.

## The Den addon

One Stremio addon for what were three installs ([oxyc/den#286](https://github.com/oxyc/den/issues/286)): one
manifest, one `/den/configure` and one sealed config holding a member per service — `scout` (streams), `subs`
(subtitles), `reel` (trailers), each exactly the config that service's own `/configure` builds — with one install
id (`iid`) and epoch (`ep`) at the top. The manifest at `/den/<config>/manifest.json` declares only the members the
install has. Hearth answers the page, `/den/config-key` and both manifests; each resource goes to its service in
process (`crates/hearth/src/den.rs`), which reads its member with the code that reads its own config and mints its
links under `/den`, so they come back there and answer to the addon's revocation. A config or link of either kind is
refused on the other's routes; every per-service route, key, revocation list and response is unchanged.

| Env | |
|---|---|
| `HEARTH_CONFIG_KEY`, `HEARTH_CONFIG_KEYS_PREV` | The addon's sealing key and prior keys. Unset: a ring derived from Scout's `CONFIG_KEY`/`CONFIG_KEYS_PREV` (HKDF), so a node that seals Scout seals this too and neither opens the other's configs or play tickets. Neither set: plaintext configs. A malformed key refuses every Den config (503 `config_key_invalid`). Play tickets are keyed with the addon's own HKDF info, so they stay apart from Scout's even if both keys are equal (hearth warns at startup when they are). |
| `HEARTH_REVOKED_INSTALLS`, `HEARTH_CONFIG_EPOCH`, `HEARTH_REQUIRE_INSTALL_ID` | The addon's revocation, as each service's `*_REVOKED_INSTALLS` etc. |
| `HEARTH_PUBLIC_BASE_URL` | The addon's links go under `<it>/den`; unset, under the request's origin and `/den`. |
| `HEARTH_MANAGED_PLAYBACK` | Enables the new `POST /den/<config>/playback` coordinator. It is off by default (the route is a concealed 404) so the full Edge and Web path can ship and be validated before activation. |

The startup line `den addon at /den — sealed=… key=…` shows what is in force.

**Moving from the derived key to one of its own breaks every Den install** unless the derived ring is kept: its
private half is never written anywhere, so it cannot be copied into `HEARTH_CONFIG_KEYS_PREV`. Name it instead —
`HEARTH_CONFIG_KEYS_PREV=scout` (beside any other prior keys) keeps installs sealed under the ring derived from
Scout's current and prior keys opening, for as long as Scout keeps those keys. Changing Scout's `CONFIG_KEY`
while the Den addon derives from it rotates the Den key with it: put Scout's old key in Scout's
`CONFIG_KEYS_PREV`, as for Scout's own installs.

`/den/_internal/validate/m/s/<blob>` is Reel's capability check for den-edge's LAN relay, on the Den addon's terms:
it validates only capabilities Reel minted for a Den install, as Reel's own route validates only Reel's.

When enabled, `/den/<config>/playback` accepts the current Remux play fields plus an `Idempotency-Key`. Hearth
derives Scout and Subtitles from that same combined config, chooses direct managed HLS only for a trusted Edge
request, and otherwise translates the existing Remux session into a response whose one required field is `url`.
It does not create an outer session. Responses are JSON with `Cache-Control: no-store`; completed retries are
held briefly in bounded memory so a lost response cannot create another Remux session.

## Listeners

Besides `PORT` (8090, the table above), each service can have a listener of its own, on the port its old standalone
container used, where **every request goes to that service with the path unchanged**, exactly as that container
served it. That is what lets the switch-over change nothing outside the box: the tunnels, the relays, the LAN
addresses in `ROUTES` and every installed link keep pointing at the ports they point at today.

| Env | Default | Service | Reached by |
|---|---|---|---|
| `HEARTH_SCOUT_PORT` | 8080 | Scout | `/<config>/manifest.json`, `/<config>/stream/...`, `/<config>/play/...`, `/p/<ticket>`, `/configure`, `/health`, `/validate`, `/metrics`, `/config-key` |
| `HEARTH_REEL_PORT` | 8092 | Reel | `/play/...`, `/manifest.json`, ... |
| `HEARTH_SUBS_PORT` | 8093 | Subtitles | `/<config>/manifest.json`, ... |
| `HEARTH_REMUX_PORT` | 8095 | Remux | `/remux/...` (it already serves under `/remux`, so this is a pass-through) |

Unset takes the default; empty or `0` turns that listener off. A value that is not a port, or a port already used by
another listener (`PORT` included), stops hearth at startup. On these listeners `/health` is the service's own, not
hearth's combined one.

Absolute URLs. On its own listener a service builds them exactly as its old container did: from its own
`PUBLIC_BASE_URL` (`<SERVICE>_PUBLIC_BASE_URL`) as-is, else from the request's forwarded headers or `Host`. Under
`/scout`, `/reel` and `/subs` on :8090 the prefix must survive, which is what `HEARTH_PUBLIC_BASE_URL` is for (below).
Scout is built twice for this (same client and cache, one handler per origin), so both are exact. Reel and Subtitles
are one instance each, so while one has its own listener `HEARTH_PUBLIC_BASE_URL` is not applied to it (the
prefixed paths on :8090 then follow its own `PUBLIC_BASE_URL`, or the request's `Host`, without the prefix).

## Scout in process

Remux (stream lists, play URLs) and Subtitles (a resync target's play redirect) reach Scout by calling hearth's own
request path when the scout URL is one of hearth's own Scout bases, and over HTTP otherwise (`crates/hearth/src/inproc.rs`).
A call goes through everything a request on the socket does: admission lanes, Scout's handler with its scope and
revocation checks and `X-Den-Remux-Key`, the request log and metrics; the caller's timeouts still bound it. Only the
Scout hop moves: the debrid's redirect is still fetched over HTTP behind the address guard. The bases, listed on the
startup line `scout in process for remux and subs at: ...`, are only what hearth knows to be itself: Scout's own port
on the loopback names, Scout's `PUBLIC_BASE_URL`, `http://<host>:<HEARTH_SCOUT_PORT>` when `HEARTH_PUBLIC_BASE_URL` is
`http://<host>:<PORT>`, and `/scout` on the main port (loopback and `HEARTH_PUBLIC_BASE_URL`). A scout on another
machine, or a name hearth was not told is its own, stays HTTP. `ORIGIN_ALIASES`/`SCOUT_ALIASES` still map a public
name to the LAN address first, and that address is then one of the bases.

The LAN base is inferred from `HEARTH_PUBLIC_BASE_URL`, so when that is `http://<host>:<PORT>` the host must be hearth
itself: a request to that host on Scout's port is answered in process and never sent.

## Configuration

`HEARTH_SERVICES` is a comma-separated subset of `scout`, `reel`, `subs`, and `remux` (or `all`). It defaults to
`all` for compatibility. Unselected services are not initialised or exposed and are omitted from `/health`.
The combined `/den` addon requires Scout, Reel, and Subtitles; managed playback additionally requires Remux.

The allocator is jemalloc with its purge tuning compiled in (`crates/hearth/src/alloc.rs`; the startup line
`allocator: ...` shows what is in force); `_RJEM_MALLOC_CONF` (jemalloc's `MALLOC_CONF`, prefixed) overrides it.

`PORT` (default 8090) and `HEARTH_WORKER_THREADS` (default 2) are hearth's own. Each service keeps the
environment it has today, but several share names (`CACHE_DIR`, `CACHE_MAX_BYTES`, `CONFIG_KEY`,
`METRICS_TOKEN`, `LOG_REQUESTS`, `SCOUT_ORIGINS`, `PUBLIC_BASE_URL`, ...), so a service can be given a
prefixed copy that wins over the bare name for that service alone:

```
SCOUT_CACHE_DIR=/cache/scout REEL_CACHE_DIR=/cache/reel   SUBS_CACHE_DIR=/cache/subs   REMUX_SCRATCH_DIR=/cache/remux
SCOUT_METRICS_TOKEN=...      REEL_METRICS_TOKEN=...       SUBS_METRICS_TOKEN=...
```

The prefixes are `SCOUT_*`, `REEL_*`, `SUBS_*` and `REMUX_*`; `HEARTH_*` (`HEARTH_SERVICES`,
`HEARTH_WORKER_THREADS`, `HEARTH_PUBLIC_BASE_URL`, `HEARTH_<SERVICE>_PORT`) is hearth's own, and `PORT` is hearth's alone. The image sets these,
which a deploy overrides as needed:

```
SCOUT_CACHE_DIR=/cache/scout  SCOUT_MEMORY_CACHE_BYTES=50331648  SCOUT_DISK_CACHE_BYTES=268435456
REEL_CACHE_DIR=/cache/reel      REEL_FFMPEG_PATH=/usr/local/bin/ffmpeg  REEL_YTDLP_PATH=/usr/local/bin/yt-dlp
REEL_YTDLP_WORKER=/app/resolve.py   REEL_MAX_HEIGHT=1080
REEL_YOUTUBEI_WORKER=/app/youtubei/main.mjs   REEL_DENO_PATH=/usr/local/bin/deno
SUBS_CACHE_DIR=/cache/subs      SUBS_FFMPEG_PATH=/usr/local/bin/ffmpeg  SUBS_ALASS_PATH=/usr/local/bin/alass
REMUX_SCRATCH_DIR=/cache/remux  REMUX_FFMPEG_PATH=/usr/local/bin/ffmpeg-remux
LIBVA_DRIVER_NAME=iHD           DEN_VERSION=<VERSION>
```

A setting from a service's old env file takes that service's prefix: Reel's `TMDB_KEY` becomes `REEL_TMDB_KEY`,
Subtitles' `CONFIG_KEY` becomes `SUBS_CONFIG_KEY`, Remux's `EDGE_SECRET` becomes `REMUX_EDGE_SECRET`. Remux's own
variables that already begin with `REMUX_` (`REMUX_URL_KEY`, `REMUX_SCOUT_KEY`) keep their names: the prefix is added
as a second name, not stripped from the first.

A bare name with no prefixed twin reaches every service (right for `FFMPEG_PATH`, wrong for `CACHE_DIR`). The
services' defaults differ (`den-scout-cache`, `den-reel-cache` and `den-subtitles-cache` under the temp dir,
Remux's scratch at `/cache`), so unset they do not collide.

Scout, Reel and Subtitles put absolute URLs in what they return, built from `PUBLIC_BASE_URL` or, when unset,
from the request's `Host`, which would drop the prefix. Set `HEARTH_PUBLIC_BASE_URL=https://node.example`
and hearth gives Scout `https://node.example/scout`, Reel `https://node.example/reel` and Subtitles
`https://node.example/subs` (an explicit `SCOUT_PUBLIC_BASE_URL`, `REEL_PUBLIC_BASE_URL` or `SUBS_PUBLIC_BASE_URL`
wins; see Listeners for what a service's own listener does). `SCOUT_ORIGINS`, which Subtitles reads, begins with the
Scout prefix and so also reaches Scout as `ORIGINS`, a name Scout does not read.

## Bounds

What the node may hold is fixed, so memory follows these limits rather than offered load (design and
numbers: [oxyc/den-edge#120](https://github.com/oxyc/den-edge/issues/120), [oxyc/den#282](https://github.com/oxyc/den/issues/282)).
They are constants (`admit.rs`, `media.rs`, `sock.rs`), not settings. **Every listener draws on the same
budget**: the main port and the four service ports (Scout 8080, Reel 8092, Subtitles 8093, Remux 8095) share one
connection cap, one pair of request lanes and one set of media permits, so opening a port adds no capacity.

| Bound | Limit | When it is hit |
|---|---|---|
| Connections | 256 in all, taken before `accept`; 64 more on probation | past 256 the peer is accepted on probation: control routes only, one answer, then closed (`Connection: close`, 1 s head timeout); anything else gets `503 server_busy`. With no probation slot either, the peer waits in the kernel's listen backlog |
| Hyper read buffer | 32 KiB on :8090 and Scout's port, 16 KiB on the others | an oversized request head is refused |
| Requests | 64 live, 48 of them bulk | waits 25 ms, then `503 {"error":"server_busy"}` with `Retry-After: 1` |
| Media bodies | 24 total; Remux 16, Reel 12 | no wait: `503 {"error":"media_busy"}` with `Retry-After: 1` |
| Media idle / lifetime | Remux 10 s, Reel 30 s without a byte moved to the client / 15 min | the body ends with an error and its permit returns |
| Idle non-media body | 30 s without a frame | the request position returns, and the body errors if it is polled again |
| Socket queues | send 256 KiB (`HEARTH_SNDBUF`, bytes), receive 32 KiB requested | verified after `accept`; a connection the platform cannot bound is closed |

A connection can have about its send queue in flight, so the send queue caps its throughput at roughly
`queue / round-trip time`: 256 KiB allows ~42 Mbit/s at 50 ms and ~21 Mbit/s at 100 ms; LAN clients (about 1 ms) are
nowhere near it. The default favours distant clients without letting a stalled reader hold much (the kernel counts
twice the requested size, 512 KiB, against the cgroup; at most 24 media bodies stream at once). A node that serves
4K remuxes over a long path can raise it with `HEARTH_SNDBUF` (and `net.core.wmem_max`, which clamps the request,
208 KiB by default); a value below 16 KiB is refused with a log line and the default is used.

The service's own 503s (Remux's `scratch_unavailable` and `scout_unconfigured`, Subtitles' `service_unavailable` and
`translation_unavailable`) get `Retry-After: 5` from hearth when the service sent none.

The 16 positions bulk cannot take are the control lane: `/health`, `/metrics`, the manifests and `/config-key`
(Scout's and Reel's also `/` and `/configure`, their static page; every service's installed
`/<config>/manifest.json`; Remux's bare `/health` on its own port), Remux's session keep-alive and end, and CORS preflights. It is
an allowlist (`route::classify`); any route not named is bulk, which includes everything of Scout's that asks an
indexer or a debrid service (`/<config>/stream`, `/play`, `/p/<ticket>`, `/account`, `/validate`). A media permit
belongs to the response body and returns when the body finishes, the client goes away, the upstream fails or a
deadline passes. Media is GET only: Reel's `/play` (also the bare `/play?v=<id>`), `/progressive`, `/hls/seg` and `/m/...`, and Remux's segments,
`init.mp4` and `/speed`. Scout streams nothing (a play is a 302, a stream list a buffered JSON body), so it owns no
media permit; its own deadlines are the 15 s request read and its upstream timeouts. Scout and Reel carry sealed
configs in the URL, which is why :8090 and Scout's port keep a 32 KiB head.

The idle clock runs only while the client is not reading, so a slow reader that keeps moving bytes holds its permit
until it finishes (or the 15 min lifetime). Remux's is short because a player fetches a segment whole and pauses
between segments; Reel's stays 30 s because a paused player stops reading a progressive file mid-body and asks again
with a Range when it resumes.

`/metrics` on :8090 (low cardinality: two lanes, two media classes) carries `den_hearth_request_admission_*` by
`lane`, `den_hearth_connection_admission_*`, and `den_hearth_media_*` by `class`; on a service's own port `/metrics`
is the service's. `bench/http/run.py` has an `overload` case that holds the node past these limits on every port and
reports the cgroup's memory and `/health`'s latency (see [BENCHMARKS.md](BENCHMARKS.md)).

## Per-crate tools

The image, CI, versioning and releases are the root's alone. What the crates still carry beyond their code:

- `crates/reel/worker/resolve.py`: the yt-dlp worker the root `Dockerfile` copies into the image.
- `crates/remux/testdata/`, `crates/scout/testdata/`: fixtures for the tests and the e2e target.
- `crates/subtitles/scripts/tier1-corpus.py`: the sync parity and memory gate (alass against ffsubsync); run it
  anywhere with `alass`, `ffsubsync` and `ffmpeg`. No CI job runs it yet.
- `crates/subtitles/scripts/smoke.sh`: a live OpenSubtitles smoke test against the standalone `den-subtitles` binary.

## Develop

```
cargo test --workspace
cargo clippy --workspace --all-targets -- -D warnings
cargo fmt --check        # each crate keeps its own rustfmt.toml
```

Benchmarks, the load and soak harness, and the baseline tables: [BENCHMARKS.md](BENCHMARKS.md).
