#!/usr/bin/env bash
# Start an image and prove it is the release it claims to be, before anything signs or promotes it.
#
#   scripts/probe-image.sh ghcr.io/oxyc/den-hearth@sha256:<digest> 0.1.0
#
# Unconfigured, den-hearth still starts and answers /health. The startup line must name the expected release for
# all three services, each service's manifest or health must answer through its prefix, and Remux must not report
# a broken ffmpeg or scratch directory. On a host with a GPU it also runs a real rate-controlled VAAPI encode.
set -euo pipefail

image="${1:?usage: probe-image.sh <image@digest> <version>}"
version="${2:?usage: probe-image.sh <image@digest> <version>}"

docker pull --quiet "$image" >/dev/null
id=$(docker run --rm -d -p 127.0.0.1:18090:8090 "$image")
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
for service in "den-reel $version" "den-subtitles $version" "den-remux $version"; do
    case "$line" in
        *"$service"*) ;;
        *) echo "::error::startup line does not name '$service'"; exit 1 ;;
    esac
done

# Each service answers through its own prefix and reports the release the image carries.
for path in reel subs; do
    got=$(curl -fsS "http://127.0.0.1:18090/$path/manifest.json" | jq -r .version)
    [ "$got" = "$version" ] || { echo "::error::/$path/manifest.json version is '$got', expected $version"; exit 1; }
done
curl -fsS http://127.0.0.1:18090/remux/health | jq -e '.status' >/dev/null

# Remux's ffmpeg and scratch directory are what a broken image loses first.
reason=$(jq -r '.services.remux.reason // empty' <<<"$body")
case "$reason" in
    ffmpeg_unavailable | scratch_unwritable | transcode_unavailable) echo "::error::/health reports remux $reason"; exit 1 ;;
esac

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
else
    echo "gpu: skipped, no $node on this host (the transcode path is not proven here)"
fi
