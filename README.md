# den-hearth

Den's self-hosted media node: Reel (trailers, yt-dlp + ffmpeg), Subtitles and Remux (HLS remux and
transcode) in one process, one runtime and one listener.

```
crates/
  reel/       den-reel       library + the standalone `den-reel` binary
  subtitles/  den-subtitles  library + the standalone `den-subtitles` binary
  remux/      den-remux      library + the standalone `den-remux` binary
  hearth/     den-hearth     the combined binary
  scout/      den-scout      skeleton of the Go -> Rust port, see crates/scout/PORTING.md
```

The three services were imported with their full history (`git subtree`). Each is the same code its
repository ships, turned into a library: its old `main.rs` is now `lib.rs`, and `main.rs` is three lines.
The standalone binaries still build and their tests still run from this workspace.

## Run

```
cargo run -p den-hearth        # listens on :8090
```

You need `ffmpeg` (all three), `yt-dlp` (Reel) and `alass` (Subtitles) on the path, as for the standalone
services.

## Container image

One image, `den-hearth`, replaces the three per-service images (`den-reel`, `den-subtitles`, `den-remux`). It is
Debian trixie-slim (glibc: Reel's yt-dlp, deno and MP4Box are glibc builds) with:

| Tool | For | Source |
|---|---|---|
| `den-hearth` | the binary, built with cargo-chef so dependencies are a cached layer | this workspace |
| `ffmpeg` (Debian) | Reel (mux, cropdetect) and Subtitles (audio decode for sync) | apt |
| `ffmpeg-remux` | Remux: minimal source build, VAAPI, checksummed tarball | `FFMPEG_VERSION` / `FFMPEG_SHA256` |
| `intel-media-va-driver` | Remux's GPU transcode (UHD 630, driver `iHD`), amd64 only | apt |
| `yt-dlp` (binary + zipapp), `deno`, `python3` | Reel's extractor and its resident resolver | pinned versions, SHA-256 checked |
| `MP4Box` + `libgpac` | Reel's `clap` box | GPAC pinned to a commit |
| `alass` | Subtitles' aligner | `cargo install --locked`, pinned |

It runs as uid/gid 65532 (`nonroot`) with `/cache/{reel,subs,remux}` owned by it and `VOLUME /cache`. It listens on
`PORT` (hearth's default, 8090). `HEALTHCHECK` polls `/health` every 5 minutes. The release number is `VERSION`, passed
as `DEN_VERSION` (all three services report it); it is deliberately not in any `Cargo.toml`.

```
docker build --platform linux/amd64 --build-arg DEN_VERSION="$(cat VERSION)" -t den-hearth:local .
docker run --rm -p 8090:8090 den-hearth:local
curl localhost:8090/health
curl localhost:8090/reel/manifest.json
curl localhost:8090/subs/manifest.json
curl localhost:8090/remux/health
docker run --rm --entrypoint ffmpeg-remux den-hearth:local -hwaccels
```

`docker buildx bake` builds the same image from `docker-bake.hcl`; `docker buildx bake e2e` runs Remux's `#[ignore]`d
end-to-end tests against the image's own `ffmpeg-remux`. A transcode needs the GPU: pass `--device /dev/dri` and add
the host's `render` gid with `--group-add` (the old `den-remux` quadlet did the same).

CI (`.github/workflows/`): `ci.yml` formats, lints and tests the workspace, runs the e2e stage, and on `main` builds,
probes (`scripts/probe-image.sh`) and scans an immutable candidate. `docker-publish.yml` promotes that digest on a
`v*` tag (cosign keyless signature, `:latest` last) and rebuilds the newest tag weekly without cache for Debian
security fixes. `ytdlp-update.yml` and `deno-update.yml` bump the checksummed downloads. `scripts/release.sh X.Y.Z`
commits `VERSION`, waits for that commit's CI and tags it.

## Routing

| Path | Goes to | The service sees |
|---|---|---|
| `/reel/...` | Reel | `/...` (prefix removed) |
| `/subs/...` | Subtitles | `/...` (prefix removed) |
| `/remux/...` | Remux | unchanged (Remux already serves everything under `/remux`) |
| `/remux/metrics` | Remux | `/metrics` (its one route outside `/remux`) |
| `/health` | hearth | all three side by side, below |
| anything else | hearth | 404 |

`/health` answers 200 with `{"status": "ok"|"degraded", "services": {"reel": {...}, "subs": {...},
"remux": {...}}}`, each entry being that service's own `/health` body. It is `degraded` when any service
says anything but `ok`. Each service's own `/health` and `/metrics` stay reachable under its prefix.

## Configuration

`PORT` (default 8090) and `HEARTH_WORKER_THREADS` (default 2) are hearth's own. Each service keeps the
environment it has today, but several share names (`CACHE_DIR`, `CACHE_MAX_BYTES`, `CONFIG_KEY`,
`METRICS_TOKEN`, `LOG_REQUESTS`, `SCOUT_ORIGINS`, `PUBLIC_BASE_URL`, ...), so a service can be given a
prefixed copy that wins over the bare name for that service alone:

```
REEL_CACHE_DIR=/cache/reel   SUBS_CACHE_DIR=/cache/subs   REMUX_SCRATCH_DIR=/cache/remux
REEL_METRICS_TOKEN=...       SUBS_METRICS_TOKEN=...
```

The prefixes are `REEL_*`, `SUBS_*` and `REMUX_*`; `HEARTH_*` (`HEARTH_WORKER_THREADS`, `HEARTH_PUBLIC_BASE_URL`) is
hearth's own, and `PORT` is hearth's alone. The image sets these, which a deploy overrides as needed:

```
REEL_CACHE_DIR=/cache/reel      REEL_FFMPEG_PATH=/usr/bin/ffmpeg  REEL_YTDLP_PATH=/usr/local/bin/yt-dlp
REEL_YTDLP_WORKER=/app/resolve.py   REEL_MAX_HEIGHT=1080
SUBS_CACHE_DIR=/cache/subs      SUBS_FFMPEG_PATH=/usr/bin/ffmpeg  SUBS_ALASS_PATH=/usr/local/bin/alass
REMUX_SCRATCH_DIR=/cache/remux  REMUX_FFMPEG_PATH=/usr/local/bin/ffmpeg-remux
LIBVA_DRIVER_NAME=iHD           DEN_VERSION=<VERSION>
```

A setting from a service's old env file takes that service's prefix: Reel's `TMDB_KEY` becomes `REEL_TMDB_KEY`,
Subtitles' `CONFIG_KEY` becomes `SUBS_CONFIG_KEY`, Remux's `EDGE_SECRET` becomes `REMUX_EDGE_SECRET`. Remux's own
variables that already begin with `REMUX_` (`REMUX_URL_KEY`, `REMUX_SCOUT_KEY`) keep their names: the prefix is added
as a second name, not stripped from the first.

A bare name with no prefixed twin reaches all three (right for `FFMPEG_PATH`, wrong for `CACHE_DIR`). The
services' defaults differ (`den-reel-cache` and `den-subtitles-cache` under the temp dir, Remux's scratch
at `/cache`), so unset they do not collide.

Reel and Subtitles put absolute URLs in what they return, built from `PUBLIC_BASE_URL` or, when unset,
from the request's `Host`, which would drop the prefix. Set `HEARTH_PUBLIC_BASE_URL=https://node.example`
and hearth gives Reel `https://node.example/reel` and Subtitles `https://node.example/subs` (an explicit
`REEL_PUBLIC_BASE_URL` or `SUBS_PUBLIC_BASE_URL` wins).

## Legacy per-service build files (to delete after cutover)

Each imported crate still carries the files that built and shipped its own image. They are inert here (GitHub only
reads the root `.github/`, Docker the root `Dockerfile`) and are superseded by the root files above:

```
crates/reel/Dockerfile        crates/reel/docker-bake.hcl        crates/reel/VERSION
crates/reel/.github/          crates/reel/scripts/release.sh
crates/subtitles/Dockerfile   crates/subtitles/.dockerignore     crates/subtitles/VERSION
crates/subtitles/.github/     crates/subtitles/scripts/release.sh
crates/subtitles/scripts/smoke.sh   crates/subtitles/scripts/tier1-corpus.py
crates/remux/Dockerfile       crates/remux/docker-bake.hcl       crates/remux/VERSION
crates/remux/.github/         crates/remux/scripts/release.sh    crates/remux/scripts/probe-image.sh
```

Keep `crates/reel/worker/resolve.py` (the root `Dockerfile` copies it) and `crates/remux/testdata/` (the e2e tests).
`tier1-corpus.py` is the Subtitles sync parity gate (its `corpus` Dockerfile stage), which has no equivalent at the
root yet; carry it over if that gate is still wanted.

## Develop

```
cargo test --workspace
cargo clippy --workspace --all-targets -- -D warnings
cargo fmt --check        # each crate keeps its own rustfmt.toml
```
