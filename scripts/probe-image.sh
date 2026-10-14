#!/usr/bin/env bash
# Start an image and prove it is the release it claims to be, before anything signs or promotes it.
#
#   scripts/probe-image.sh ghcr.io/oxyc/den-hearth@sha256:<digest> 0.1.0
#
# Unconfigured, den-hearth still starts and answers /health. The startup line must name the expected release for
# all three services, each service's manifest or health must answer through its prefix, and Remux must not report
# a broken ffmpeg or scratch directory.
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
    ffmpeg_unavailable | scratch_unwritable) echo "::error::/health reports remux $reason"; exit 1 ;;
esac
