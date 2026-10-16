# Self-host den-hearth

This is the portable deployment. It uses one container, one published port, a persistent cache volume, and all
four services by default. It works with Docker Compose or Podman Compose on an always-on Linux host, an Intel Mac,
or Docker Desktop on Apple Silicon. A Raspberry Pi can run the Scout-only image described below.

## Start the node

From the repository root:

```sh
sh selfhost/init.sh
# Review selfhost/.env and add any optional provider settings you need.
docker compose --env-file selfhost/.env -f selfhost/compose.yaml up -d
curl http://localhost:8090/health
```

Open `http://<host>:8090/den/configure` on a machine that can reach the host. The page produces the combined addon
URL to install in Den or another Stremio-compatible client. On a Scout-only node use `/scout/configure` instead.
On a trusted home LAN, plain HTTP is enough for Den. Do not expose the port to the internet without HTTPS and
access controls; installed URLs are bearer credentials even when their config payload is sealed.

The generated config keys are optional only for a disposable trusted-LAN setup. With them set, account tokens are
sealed in addon URLs. Losing or changing one breaks that addon's installed URLs. The generated Reel signing secret
also prevents editing signed media links, and the Remux URL key keeps browser sessions valid across restarts. The
debrid account belongs in the configure page, not in the server environment.

## Choose services

`HEARTH_SERVICES` accepts a comma-separated subset of `scout`, `reel`, `subs`, and `remux`, or `all` (the default).
Unselected services are not initialised, are absent from `/health`, get no listener, and their mounted routes
return 404.

```dotenv
HEARTH_SERVICES=all                # every service; the default
HEARTH_SERVICES=scout              # stream addon only
HEARTH_SERVICES=scout,reel,subs    # individual addons plus the combined /den addon
```

The combined `/den` addon is available only when Scout, Reel, and Subtitles are all selected. Managed playback
also needs Remux. Individual addons remain at `/scout`, `/reel`, and `/subs`.

When installing an individual addon, set `HEARTH_PUBLIC_BASE_URL` to the origin clients use, such as
`http://192.168.1.20:8090` or `https://hearth.example.net`. The configure page keeps the correct mount, but
services cannot infer that mount after Hearth dispatches a request; without this setting, later media URLs can
lose `/scout`, `/reel`, or `/subs`. The combined `/den` addon carries its mount internally and does not have this
limitation.

The Compose file disables the four compatibility listeners and publishes only `8090`; this avoids five firewall
rules and works well behind a reverse proxy. If you run the image directly, the legacy listeners default to 8080,
8092, 8093, and 8095. Set the corresponding `HEARTH_<SERVICE>_PORT=0` to disable them.

## Storage and sizing

The default named volume `hearth-cache` keeps all rebuildable data below `/cache` across upgrades and is the most
portable choice. Override it with a bind mount by setting `HEARTH_CACHE=/absolute/path`. The container runs as
uid/gid 65532, so a bind-mounted directory must be writable by that id. On a conventional Linux Docker host:

```sh
sudo install -d -o 65532 -g 65532 /srv/den-hearth/cache
```

That ownership command is not portable to rootless Podman; use `podman unshare chown -R 65532:65532 <path>` there.

Scout defaults to 48 MiB in memory and 256 MiB on disk. Reel defaults to 4 GiB, Subtitles to 256 MiB, and Remux
scratch to 1 GiB. Override `SCOUT_MEMORY_CACHE_BYTES`, `SCOUT_DISK_CACHE_BYTES`, `REEL_CACHE_MAX_BYTES`,
`SUBS_CACHE_MAX_BYTES`, or `REMUX_SCRATCH_MAX_BYTES` in `.env`. None is authoritative state, so deleting the volume
is safe but makes the next requests cold. Config keys are different: keep the `.env` values backed up.

For Scout only, two worker threads and 48 MiB of cache are conservative defaults. Reel can run yt-dlp, Deno, and
ffmpeg concurrently; Subtitles can run ffmpeg and alass; Remux can run several ffmpeg processes. Full-media nodes
therefore need much more memory and CPU than Scout-only nodes.

## HTTPS, Chromecast, and remote clients

For one home LAN, use `http://<LAN address>:8090`. For a public name, put a TLS reverse proxy in front, set
`HEARTH_PUBLIC_BASE_URL=https://hearth.example.net`, and forward only the proxy's HTTPS port. A minimal Caddy
example is included in `Caddyfile.example`; it assumes Caddy runs on the host. Set `HEARTH_BIND=127.0.0.1` so port
8090 cannot bypass that proxy. A Caddy container must proxy to the Hearth service rather than its own
`127.0.0.1`. The hostname must resolve to an address the client can actually reach; `localhost` in an addon URL
means the client itself, not the server.

Chromecast splits this into two paths. A Web Sender page must use HTTPS, and a published custom receiver must use
HTTPS, but Google permits the receiver to load media over HTTP from a private LAN address. Den's current Cast path
is stricter and accepts only HTTPS media, so a plain-HTTP Hearth is not sufficient today. The media URL must be
routable and resolvable **from the Chromecast**, not just from the sender; `localhost` means the Chromecast itself.
Disable Wi-Fi client isolation and keep sender and receiver on a LAN where Cast discovery works.

If media does use HTTPS, use a publicly trusted certificate with the complete intermediate chain; a private CA is
not a dependable Cast baseline. A public sender that probes a private Hearth address also encounters browser Local
Network Access permission (Chrome 142+) and may encounter mixed-content rules, even though the receiver itself may
load HTTP media. Split DNS plus the same trusted HTTPS hostname on the LAN is the least surprising arrangement.

Every HLS playlist, init segment, media segment, and subtitle needs CORS; progressive media with tracks does too.
Range requests must return correct `206`, `Content-Range`, and `Accept-Ranges` responses. This Compose file does not
provision DNS, certificates, a sender/receiver application, or the separate Den Cast/Edge services. Hearth over
plain HTTP is allowed by Google, but enabling it safely in Den needs a future Cast-only LAN path that never exposes
an insecure remote URL or asks the HTTPS browser sender to probe mixed content.

Remux emits fMP4 HLS. A Cast sender must mark both `hlsSegmentFormat` and `hlsVideoSegmentFormat` as `FMP4`; Cast
otherwise assumes MPEG-TS. H.264/AAC is the broad compatibility fallback. HEVC, AV1, surround audio, and HDR support
vary by receiver generation, so passing a source codec through is not proof that a particular Chromecast can play it.

Practical network choices:

- **LAN-only:** use a real hostname and publicly trusted certificate on a LAN-bound Caddy. Obtain the certificate
  with DNS-01 so no inbound port is required, then use split DNS to resolve that name to the Hearth host. Caddy's
  private CA (`tls internal`) is not a dependable Chromecast trust path.
- **Remote-capable:** point a DNS-only record at the home address and forward TCP/443 to a narrowly configured Caddy.
  Use dynamic DNS if necessary, and split DNS or working hairpin NAT at home. The home upload still carries video.
- **Cloudflare:** an ordinary proxied hostname or Cloudflare Tunnel is not the default media path. Cloudflare says
  Free/Pro/Business Tunnel users must use its paid video/large-file services for this traffic. A gray-cloud
  DNS-only record is fine because media bypasses Cloudflare; Tunnel/Pages can still carry small static sender and
  control traffic.
- **Tailscale:** ordinary tailnet traffic has no video prohibition, but classic Chromecast devices are not tailnet
  clients, so Tailscale Serve/MagicDNS addresses are unreachable to them. Funnel is public HTTPS, but remains beta
  with unpublished, non-configurable bandwidth limits and is best treated as an experiment rather than the default.

Primary references: [Google Cast sender and HTTPS](https://developers.google.com/cast/docs/web_sender),
[receiver/media URL rules](https://developers.google.com/cast/docs/registration),
[media and codec support](https://developers.google.com/cast/docs/media),
[Cloudflare Tunnel streaming limits](https://developers.cloudflare.com/cloudflare-one/faq/cloudflare-tunnels-faq/#large-file-and-streaming-traffic-through-tunnel),
[Tailscale Serve](https://tailscale.com/docs/features/tailscale-serve), and
[Tailscale Funnel](https://tailscale.com/docs/features/tailscale-funnel).

Remote Remux streams traverse the home server's upload connection. A file whose average bitrate looks safe can
still burst well above it and stall. Managed provider HLS can avoid that path, but it currently depends on the
larger Den Edge/Web deployment and is off by default; Hearth alone does not turn it on.

## Mac and Raspberry Pi notes

- The published full image is currently `linux/amd64`. Intel Macs run it directly; Docker Desktop on Apple
  Silicon uses the Compose file's explicit `linux/amd64` emulation, with a startup/performance cost.
- The full media image and VAAPI transcode path are built for the project's amd64 Intel host. They are not a
  supported Raspberry Pi media stack, and passing a Pi GPU through will not make the Intel VAAPI setup work.
- Scout has no external media-tool dependency. On an ARM64 Pi, build the portable Scout image natively:

  ```sh
  docker build -f Dockerfile.scout --build-arg DEN_VERSION="$(cat VERSION)" -t den-hearth:scout-local .
  sed -i.bak -e 's|^HEARTH_SERVICES=.*|HEARTH_SERVICES=scout|' \
    -e 's|^HEARTH_PLATFORM=.*|HEARTH_PLATFORM=linux/arm64|' \
    -e 's|^# HEARTH_IMAGE=.*|HEARTH_IMAGE=den-hearth:scout-local|' selfhost/.env
  docker compose --env-file selfhost/.env -f selfhost/compose.yaml up -d
  ```

### Optional Intel VAAPI

Copy-remux works without a GPU. Hardware transcoding on a Linux Intel host additionally needs the render device
and its group id passed into the container:

```sh
printf 'HEARTH_RENDER_GID=%s\n' "$(getent group render | cut -d: -f3)" >>selfhost/.env
docker compose --env-file selfhost/.env -f selfhost/compose.yaml -f selfhost/compose.vaapi.yaml up -d
```

This override is for the full amd64 image and `/dev/dri/renderD128`; change `HEARTH_RENDER_DEVICE` only if the host
uses a different render node. It is not a Raspberry Pi GPU recipe.

## Updating and troubleshooting

```sh
docker compose --env-file selfhost/.env -f selfhost/compose.yaml pull
docker compose --env-file selfhost/.env -f selfhost/compose.yaml up -d
docker compose --env-file selfhost/.env -f selfhost/compose.yaml logs -f hearth
```

For the locally built ARM64 Scout image, rebuild with the `docker build` command above instead of running
`compose pull`. For predictable rollback, pin `HEARTH_IMAGE` to a release tag or digest before upgrading; changing
it back and running `compose up -d` rolls the container back without discarding the cache volume or `.env`.

`/health` describes only selected services, but it is a liveness endpoint and returns HTTP 200 even when the JSON
status is `degraded`; readiness monitors must inspect the top-level `.status`. `degraded` means the process is
answering but a configured dependency is missing or unusable; the startup log includes the reason. Configuration
names in those reasons are service-local: for example Reel's `CONFIG_KEY` is `REEL_CONFIG_KEY` in `.env`, and
Remux's `SCRATCH_DIR` is `REMUX_SCRATCH_DIR`. Common mistakes are an unwritable bind mount, a lost config key, an
external URL that omits the service prefix, and expecting Remux transcoding without a compatible Intel VAAPI
device. The full environment reference remains in the root README under Configuration. Variables added to
`selfhost/.env` are passed through to the container, so advanced settings from that reference work without editing
the Compose file.
