#!/usr/bin/env bash
# Microbenchmarks (divan, with allocation counts) for scout, subtitles, remux and reel.
#
#   bench/micro.sh                 # run all four, write bench/results/micro-<stamp>.jsonl
#   bench/micro.sh scout rank      # one crate, only benchmarks whose name contains "rank"
#   bench/micro.sh --test          # run each benchmark once (the fast "does it still run" check)
#
# One JSON object per benchmark: median/fastest/mean/slowest time (ns), throughput where the bench declares bytes,
# and allocations per iteration (count and bytes: alloc, dealloc, max alloc, grow, shrink) from divan's AllocProfiler.
# Times are the machine's; allocation counts are not, and are the numbers to compare across machines and commits.
set -euo pipefail

ROOT=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
cd "$ROOT"
OUT=${OUT:-$ROOT/bench/results}
mkdir -p "$OUT"
STAMP=$(date -u +%Y%m%dT%H%M%SZ)

TEST_ONLY=0
if [ "${1:-}" = "--test" ]; then TEST_ONLY=1; shift; fi
CRATES=${1:-"scout subtitles remux reel"}
FILTER=${2:-}

python3 "$ROOT/bench/gen_fixtures.py" "$ROOT/bench/fixtures" >/dev/null

RESULT="$OUT/micro-$STAMP.jsonl"
if [ "$TEST_ONLY" = 0 ]; then
  printf '{"kind":"meta","date":"%s","commit":"%s","dirty":%s,"machine":"%s","cpus":%s,"profile":"bench (release: opt-level 3, fat LTO, 1 codegen unit)","rustc":"%s","load":"%s"}\n' \
    "$STAMP" "$(git rev-parse --short HEAD)" "$([ -n "$(git status --porcelain --untracked-files=no)" ] && echo true || echo false)" \
    "$(sysctl -n machdep.cpu.brand_string 2>/dev/null || grep -m1 'model name' /proc/cpuinfo | cut -d: -f2 | xargs)" \
    "$(getconf _NPROCESSORS_ONLN)" "$(rustc --version)" "$(uptime | sed 's/.*load averages*: //')" >"$RESULT"
fi

for crate in $CRATES; do
  RAW=$(mktemp "${TMPDIR:-/tmp}/divan-$crate.XXXXXX")
  if [ "$TEST_ONLY" = 1 ]; then
    cargo bench -p "den-$crate" --features bench --bench "$crate" --locked -- --test ${FILTER:+"$FILTER"}
  else
    cargo bench -p "den-$crate" --features bench --bench "$crate" --locked -- --color never \
      ${DIVAN_ARGS:-} ${FILTER:+"$FILTER"} | tee "$RAW"
    python3 "$ROOT/bench/divan_to_jsonl.py" "$crate" <"$RAW" >>"$RESULT"
  fi
  rm -f "$RAW"
done

[ "$TEST_ONLY" = 1 ] || echo "wrote $RESULT"
