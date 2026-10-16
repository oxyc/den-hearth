#!/bin/sh
set -eu

here=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
target=${1:-"$here/.env"}

if [ -e "$target" ]; then
    echo "refusing to overwrite $target" >&2
    exit 1
fi
if ! command -v openssl >/dev/null 2>&1; then
    echo "openssl is required to generate the self-hosting keys" >&2
    exit 1
fi

umask 077
tmp=$(mktemp "${target}.XXXXXX")
trap 'rm -f "$tmp"' EXIT HUP INT TERM

scout_key=$(openssl rand -base64 32 | tr -d '\n')
scout_remux=$(openssl rand -base64 32 | tr -d '\n')
reel_key=$(openssl rand -base64 32 | tr -d '\n')
subs_key=$(openssl rand -base64 32 | tr -d '\n')
reel_play=$(openssl rand -base64 32 | tr -d '\n')
remux_url=$(openssl rand -base64 32 | tr -d '\n')

sed \
    -e "s|^SCOUT_CONFIG_KEY=$|SCOUT_CONFIG_KEY=$scout_key|" \
    -e "s|^SCOUT_REMUX_KEY=$|SCOUT_REMUX_KEY=$scout_remux|" \
    -e "s|^REEL_CONFIG_KEY=$|REEL_CONFIG_KEY=$reel_key|" \
    -e "s|^SUBS_CONFIG_KEY=$|SUBS_CONFIG_KEY=$subs_key|" \
    -e "s|^REEL_PLAY_SECRET=$|REEL_PLAY_SECRET=$reel_play|" \
    -e "s|^REMUX_URL_KEY=$|REMUX_URL_KEY=$remux_url|" \
    "$here/.env.example" >"$tmp"
mv "$tmp" "$target"
trap - EXIT HUP INT TERM

echo "created $target (mode 0600)"
echo "back it up: losing its config keys invalidates installed addon URLs"
