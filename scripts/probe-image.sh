#!/usr/bin/env bash
# Start an image and prove it is the release it claims to be, before anything signs or promotes it.
#
#   scripts/probe-image.sh ghcr.io/oxyc/den-hearth@sha256:<digest> 0.1.0
#
# Unconfigured, den-hearth still starts and answers /health. The startup line must name the expected release for
# all four services, each service's manifest or health must answer through its prefix on :8090 AND on the port its
# old container used (Scout 8080, Reel 8092, Subtitles 8093, Remux 8095), and Remux must not report a broken
# ffmpeg, scratch directory or transcode. On a host with a GPU it also runs a real rate-controlled VAAPI encode.
set -euo pipefail

image="${1:?usage: probe-image.sh <image@digest> <version>}"
version="${2:?usage: probe-image.sh <image@digest> <version>}"

docker pull --quiet "$image" >/dev/null
id=$(docker run --rm -d -p 127.0.0.1:18090:8090 -p 127.0.0.1:18080:8080 -p 127.0.0.1:18092:8092 \
    -p 127.0.0.1:18093:8093 -p 127.0.0.1:18095:8095 "$image")
trap 'docker rm -f "$id" >/dev/null 2>&1 || true' EXIT

body=""
for _ in $(seq 1 30); do
    body=$(curl -fsS http://127.0.0.1:18090/health 2>/dev/null) && break
    sleep 1
done
[ -n "$body" ] || { echo "::error::no /health answer within 30 s"; docker logs "$id" >&2; exit 1; }

line=$(docker logs "$id" 2>&1 | grep -m1 ' listening on ' || true)
echo "startup: $line"
echo "health: $body"
for service in "den-scout $version" "den-reel $version" "den-subtitles $version" "den-remux $version"; do
    case "$line" in
        *"$service"*) ;;
        *) echo "::error::startup line does not name '$service'"; exit 1 ;;
    esac
done

# Each service answers through its own prefix and reports the release the image carries.
for path in scout reel subs; do
    got=$(curl -fsS "http://127.0.0.1:18090/$path/manifest.json" | jq -r .version)
    [ "$got" = "$version" ] || { echo "::error::/$path/manifest.json version is '$got', expected $version"; exit 1; }
    curl -fsS "http://127.0.0.1:18090/$path/health" | jq -e '.status' >/dev/null
done
curl -fsS http://127.0.0.1:18090/remux/health | jq -e '.status' >/dev/null

# The same services on their old ports, at the root, the way installed links and relays reach them.
for port in 18080 18092 18093; do
    got=$(curl -fsS "http://127.0.0.1:$port/manifest.json" | jq -r .version)
    [ "$got" = "$version" ] || { echo "::error::port $port /manifest.json version is '$got', expected $version"; exit 1; }
    curl -fsS "http://127.0.0.1:$port/health" | jq -e '.status' >/dev/null
done
curl -fsS http://127.0.0.1:18095/remux/health | jq -e '.status' >/dev/null

# Reel's one SHA-pinned yt-dlp zipapp serves both paths. Prove system Python can execute it as the
# CLI fallback and import it for the resident resolver; this catches a future artifact-format change
# before promotion. A malformed id is rejected without depending on YouTube or runner egress.
ytdlp_version=$(docker run --rm --entrypoint yt-dlp "$image" --version)
[ -n "$ytdlp_version" ] || { echo "::error::yt-dlp zipapp did not run under system Python"; exit 1; }
reply=$(printf '%s\n' '{"id":"x","format":"best","rungs":[]}' | \
    docker run --rm -i --entrypoint python3 "$image" /app/resolve.py)
jq -e '.ok == false and (.error | type == "string" and length > 0)' <<<"$reply" >/dev/null \
    || { echo "::error::Reel's resident yt-dlp worker did not report its malformed-id failure: $reply"; exit 1; }
echo "yt-dlp: zipapp $ytdlp_version executes as the CLI fallback and imports in the resident worker"
# Scout's installer page is served at its root too. Read the whole body before matching: `grep -q` stops at the
# first match and closes the pipe, which fails curl (23) under pipefail on a page that is fine.
configure="$(curl -fsS http://127.0.0.1:18080/configure)"
grep -qi '<html' <<<"$configure" \
    || { echo "::error::Scout's /configure on :8080 is not a page"; exit 1; }
configure="$(curl -fsS http://127.0.0.1:18090/scout/configure)"
grep -qi '<html' <<<"$configure" \
    || { echo "::error::Scout's /scout/configure is not a page"; exit 1; }

# Remux's ffmpeg and scratch directory are what a broken image loses first.
reason=$(jq -r '.services.remux.reason // empty' <<<"$body")
case "$reason" in
    ffmpeg_unavailable | scratch_unwritable | transcode_unavailable) echo "::error::/health reports remux $reason"; exit 1 ;;
esac

# Reel's youtubei.js worker: deno starts under the permissions Reel gives it (config.rs `deno_args`), loads the
# packages the image locked from the cache it only reads, and answers. A malformed id is refused before any request
# is made, so this needs no network. Run read-only with a tmpfs home, as the box does.
reply=$(printf '%s\n' '{"id":"x"}' | docker run --rm -i --read-only --tmpfs /home/nonroot:uid=65532 \
    --entrypoint deno -e DENO_DIR=/app/youtubei/deno "$image" run --quiet --no-prompt --cached-only --frozen \
    --config=/app/youtubei/deno.json --lock=/app/youtubei/deno.lock \
    --allow-net=www.youtube.com,youtubei.googleapis.com /app/youtubei/main.mjs)
jq -e '.ok == false and (.error | test("not a video id"))' <<<"$reply" >/dev/null \
    || { echo "::error::Reel's youtubei.js worker did not answer from the image: $reply"; exit 1; }
echo "youtubei: worker starts and answers from the image"

# The GPU transcode, for real. ffmpeg listing h264_vaapi proves nothing about the driver under it: Debian's free
# intel-media-va-driver lists it and then refuses every bitrate-controlled encode, and has no scale_vaapi kernels.
# So with a render node on this host, run Remux's own flags (job.rs) on a six-frame clip. A runner without one
# (CI) skips it; the image is not proven for transcoding there, only for everything else.
node=/dev/dri/renderD128
if [ -e "$node" ]; then
    clip="$(dirname "$0")/../crates/remux/testdata/gpu-probe.mkv"
    gid=$(stat -c %g "$node")
    docker run --rm -i --device "$node" --group-add "$gid" --entrypoint ffmpeg-remux "$image" \
        -hide_banner -loglevel error -hwaccel vaapi -hwaccel_device "$node" -hwaccel_output_format vaapi \
        -f matroska -i pipe:0 -map 0:V:0 -vf scale_vaapi=w=256:h=144:format=nv12 \
        -c:v h264_vaapi -profile:v high -level:v 4.1 -b:v 4000000 -maxrate 6000000 -bufsize 8000000 \
        -frames:v 6 -f mp4 -movflags frag_keyframe+empty_moov pipe:1 <"$clip" >/dev/null \
        || { echo "::error::the rate-controlled h264_vaapi encode failed on $node (see above)"; exit 1; }
    echo "gpu: rate-controlled h264_vaapi encode ok on $node"
    # An MPEG-TS or AVI run as a session makes one from its second grid keyframe (job.rs, `Keyframes::Grid`): times
    # rounded to frame ticks, cut back to the keyframe, the next one forced. H.264 is decoded on the GPU; MPEG-2 and
    # XviD on the CPU and uploaded (`hwupload`) — the filters and decoders this image adds for them.
    grid="setpts='if(gt(FR,0),round(PTS*TB*FR)/(FR*TB),PTS)',trim=start=5.98"
    for clip in h264.ts mpeg2.ts xvid.avi; do
        case "$clip" in
            h264.ts) decode=(-hwaccel vaapi -hwaccel_device "$node" -hwaccel_output_format vaapi); upload="" ;;
            *) decode=(-init_hw_device "vaapi=va:$node" -filter_hw_device va); upload="format=nv12,hwupload," ;;
        esac
        docker run --rm -i --device "$node" --group-add "$gid" --entrypoint ffmpeg-remux "$image" \
            -hide_banner -loglevel error "${decode[@]}" -i pipe:0 -map 0:V:0 \
            -vf "$grid,${upload}scale_vaapi=w=256:h=144:format=nv12" \
            -c:v h264_vaapi -profile:v high -level:v 4.1 -b:v 4000000 -maxrate 6000000 -bufsize 8000000 \
            -force_key_frames 11.98 -g 1000 -frames:v 24 -f mp4 -movflags frag_keyframe+empty_moov pipe:1 \
            <"$(dirname "$0")/../crates/remux/testdata/$clip" >/dev/null \
            || { echo "::error::the grid transcode of $clip failed on $node (see above)"; exit 1; }
        echo "gpu: grid transcode of $clip ok on $node"
    done
else
    echo "gpu: skipped, no $node on this host (the transcode path is not proven here)"
fi
