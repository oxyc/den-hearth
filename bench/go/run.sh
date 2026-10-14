#!/usr/bin/env bash
# The Go side of the Rust-vs-Go comparison: den-scout's own benchmarks plus bench/go/hearth_bench_test.go, on the
# same fixtures the Rust benches read. den-scout's checkout is never modified: everything runs in a throwaway
# `git worktree` of origin/main that is removed afterwards.
#
#   bench/go/run.sh                     # all benchmarks, 5 runs each
#   DEN_SCOUT=~/src/den-scout COUNT=10 bench/go/run.sh
#
# Go comes from nix (`nix shell nixpkgs#go`); a Go already on PATH is used if `nix` is missing.
set -euo pipefail

ROOT=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)
SCOUT=${DEN_SCOUT:-$HOME/Projects/Personal/den-scout}
COUNT=${COUNT:-5}
OUT=${OUT:-$ROOT/bench/results}
mkdir -p "$OUT"
STAMP=$(date -u +%Y%m%dT%H%M%SZ)
RESULT="$OUT/go-$STAMP.jsonl"
RAW=$(mktemp "${TMPDIR:-/tmp}/go-bench.XXXXXX")

python3 "$ROOT/bench/gen_fixtures.py" "$ROOT/bench/fixtures" >/dev/null

# Best effort: an offline run measures whatever origin/main last fetched, and the meta record says which commit.
git -C "$SCOUT" fetch --quiet origin || echo "warning: fetch failed, using the origin/main already fetched" >&2
WT=$(mktemp -d "${TMPDIR:-/tmp}/den-scout-go.XXXXXX")/den-scout
git -C "$SCOUT" worktree add --quiet --detach "$WT" origin/main
cleanup() {
  git -C "$SCOUT" worktree remove --force "$WT" >/dev/null 2>&1 || true
  rm -rf "$(dirname "$WT")" "$RAW"
}
trap cleanup EXIT

cp "$ROOT/bench/go/hearth_bench_test.go" "$WT/internal/scout/"
COMMIT=$(git -C "$WT" rev-parse --short HEAD)

if command -v nix >/dev/null 2>&1; then GO=(nix shell nixpkgs#go --command go); else GO=(go); fi

env BENCH_FIXTURES="$ROOT/bench/fixtures" BENCH_TESTDATA="$ROOT/crates/scout/testdata" \
  "${GO[@]}" -C "$WT" test ./internal/scout -run '^$' -bench . -benchmem -count "$COUNT" | tee "$RAW"

printf '{"kind":"meta","date":"%s","den_scout_commit":"%s","go":"%s","machine":"%s","load":"%s"}\n' \
  "$STAMP" "$COMMIT" "$("${GO[@]}" version)" \
  "$(sysctl -n machdep.cpu.brand_string 2>/dev/null || grep -m1 'model name' /proc/cpuinfo | cut -d: -f2 | xargs)" \
  "$(uptime | sed 's/.*load averages*: //')" >"$RESULT"
python3 "$ROOT/bench/go/go_to_jsonl.py" <"$RAW" >>"$RESULT"
echo "wrote $RESULT"
