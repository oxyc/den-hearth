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

public_base=${HEARTH_PUBLIC_BASE_URL:-}
if [ -n "$public_base" ]; then
    case "$public_base" in
        http://*|https://*) ;;
        *) echo "HEARTH_PUBLIC_BASE_URL must begin with http:// or https://" >&2; exit 1 ;;
    esac
    public_host=${public_base#*://}
    case "$public_host" in
        ''|*/*|*' '*|*'&'*|*'?'*|*'#'*|*'|'*|*@*)
            echo "HEARTH_PUBLIC_BASE_URL must be a bare origin without credentials, a path, or a trailing slash" >&2
            exit 1
            ;;
    esac
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
    -e "s|^HEARTH_PUBLIC_BASE_URL=$|HEARTH_PUBLIC_BASE_URL=$public_base|" \
    "$here/.env.example" >"$tmp"
mv "$tmp" "$target"
trap - EXIT HUP INT TERM

echo "created $target (mode 0600)"
echo "back it up: losing its config keys invalidates installed addon URLs"
if [ -z "$public_base" ]; then
    echo "set HEARTH_PUBLIC_BASE_URL to this host's stable LAN origin (for example http://192.168.1.20:8090)"
else
    echo "clients and Chromecast media will use $public_base"
fi
