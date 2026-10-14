#!/usr/bin/env bash
# Live smoke test of the OpenSubtitles path against the real API. Free (no LLM calls).
#
#   OPENSUBTITLES_KEY=your-key ./scripts/smoke.sh          # default: Shawshank (tt0111161)
#   OPENSUBTITLES_KEY=your-key ./scripts/smoke.sh tt0068646 # any imdb id (The Godfather)
#
# Builds the binary, starts it on a spare port with a temp cache, builds a config blob carrying
# your key (apiKey is a placeholder — translation is never invoked), then exercises manifest →
# subtitles search → one download → SRT sanity. The key stays in your shell env; it is never
# printed, written to disk, or committed.
set -euo pipefail

: "${OPENSUBTITLES_KEY:?set OPENSUBTITLES_KEY in your environment}"
IMDB="${1:-tt0111161}"
PORT="${PORT:-8099}"
CACHE="$(mktemp -d)"
ROOT="$(cd "$(dirname "$0")/.." && pwd)"

b64url() { python3 -c 'import base64,sys; print(base64.urlsafe_b64encode(sys.stdin.buffer.read()).decode().rstrip("="))'; }

echo "→ building…"
( cd "$ROOT" && cargo build --quiet -p den-hearth )

# Subtitles runs inside den-hearth: serve it on its own legacy port ($PORT) and switch the other services'
# ports off. Hearth's main port moves out of the way.
echo "→ starting den-hearth (subtitles) on :$PORT"
PORT="$((PORT + 1))" HEARTH_SUBS_PORT="$PORT" HEARTH_SCOUT_PORT=0 HEARTH_REEL_PORT=0 HEARTH_REMUX_PORT=0 \
  SUBS_CACHE_DIR="$CACHE" "$ROOT/../../target/debug/den-hearth" &
SRV=$!
trap 'kill "$SRV" 2>/dev/null || true; rm -rf "$CACHE"' EXIT
for _ in $(seq 1 30); do curl -sf "http://127.0.0.1:$PORT/health" >/dev/null && break; sleep 0.2; done

CFG=$(printf '{"provider":"openai","apiKey":"smoke-placeholder","osKey":"%s"}' "$OPENSUBTITLES_KEY" | b64url)
BASE="http://127.0.0.1:$PORT/$CFG"

echo "→ configured manifest:"
curl -s "$BASE/manifest.json" | python3 -m json.tool | sed 's/^/    /'

echo "→ subtitles search for $IMDB (hash-matches sort first):"
SUBS=$(curl -s "$BASE/subtitles/movie/$IMDB.json")
# NOTE: the url embeds the config blob, which is base64 (not encryption) of your keys — so we
# redact it to <config> here. Addon URLs are bearer secrets; never print or log them.
echo "$SUBS" | python3 -c '
import json,sys,re
d=json.load(sys.stdin); subs=d.get("subtitles",[])
print("    %d results" % len(subs))
for s in subs[:8]:
    safe = re.sub(r"/[A-Za-z0-9_-]{20,}/", "/<config>/", s["url"])
    print("    - %-5s %-14s %s" % (s["lang"], s["id"], safe))
'

FIRST=$(echo "$SUBS" | python3 -c 'import json,sys; s=json.load(sys.stdin)["subtitles"]; print(s[0]["url"] if s else "")')
if [ -n "$FIRST" ]; then
  echo "→ downloading first subtitle (proxied + cached), first 6 lines:"
  curl -s "$FIRST" | head -n 6 | sed 's/^/    /'
else
  echo "    (no subtitles returned — try another imdb id)"
fi
echo "✓ done"
