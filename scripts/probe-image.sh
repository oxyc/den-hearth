#!/usr/bin/env bash
# Start an image and prove it is the release it claims to be, before anything signs or promotes it.
#
#   scripts/probe-image.sh ghcr.io/oxyc/den-hearth@sha256:<digest> 0.1.0
#
# Unconfigured, den-hearth still starts and answers /health. The startup line must name the expected release for
# all four services, each service's manifest or health must answer through its prefix on :8090 AND on the port its
# old container used (Scout 8080, Reel 8092, Subtitles 8093, Remux 8095), and Remux must not report a broken
# ffmpeg or scratch directory.
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
# Scout's installer page is served at its root too.
curl -fsS http://127.0.0.1:18080/configure | grep -qi '<html' \
    || { echo "::error::Scout's /configure on :8080 is not a page"; exit 1; }
curl -fsS http://127.0.0.1:18090/scout/configure | grep -qi '<html' \
    || { echo "::error::Scout's /scout/configure is not a page"; exit 1; }

# Remux's ffmpeg and scratch directory are what a broken image loses first.
reason=$(jq -r '.services.remux.reason // empty' <<<"$body")
case "$reason" in
    ffmpeg_unavailable | scratch_unwritable) echo "::error::/health reports remux $reason"; exit 1 ;;
esac
