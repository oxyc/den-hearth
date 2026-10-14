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

A bare name with no prefixed twin reaches all three (right for `FFMPEG_PATH`, wrong for `CACHE_DIR`). The
services' defaults differ (`den-reel-cache` and `den-subtitles-cache` under the temp dir, Remux's scratch
at `/cache`), so unset they do not collide.

Reel and Subtitles put absolute URLs in what they return, built from `PUBLIC_BASE_URL` or, when unset,
from the request's `Host`, which would drop the prefix. Set `HEARTH_PUBLIC_BASE_URL=https://node.example`
and hearth gives Reel `https://node.example/reel` and Subtitles `https://node.example/subs` (an explicit
`REEL_PUBLIC_BASE_URL` or `SUBS_PUBLIC_BASE_URL` wins).

## Develop

```
cargo test --workspace
cargo clippy --workspace --all-targets -- -D warnings
cargo fmt --check        # each crate keeps its own rustfmt.toml
```
