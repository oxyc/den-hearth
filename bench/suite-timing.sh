#!/usr/bin/env bash
# Test-suite timing baseline: wall time of `cargo test --workspace`, clean and incremental, per-crate test time and
# the slowest tests. Writes into bench/results/ (JSONL). Run on an otherwise idle machine; the numbers are the
# machine's.
#
#   bench/suite-timing.sh [--no-clean]
#
# `cargo test` stops at the first failing test binary; --no-fail-fast runs every crate so one red crate (the
# handler tests while handler.rs is a stub) does not hide the others' time.
set -euo pipefail

ROOT=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
cd "$ROOT"
OUT=${OUT:-$ROOT/bench/results}
mkdir -p "$OUT"
STAMP=$(date -u +%Y%m%dT%H%M%SZ)
RESULT="$OUT/suite-timing-$STAMP.jsonl"
WORK=$(mktemp -d "${TMPDIR:-/tmp}/suite-timing.XXXXXX")
export CARGO_TERM_COLOR=never

now() { python3 -c 'import time; print(time.time())'; }
elapsed() { python3 -c "print(round($2 - $1, 2))"; }
emit() { printf '%s\n' "$1" | tee -a "$RESULT"; }

meta() {
  printf '{"kind":"meta","date":"%s","commit":"%s","dirty":%s,"machine":"%s","cpus":%s,"load_before":"%s"}' \
    "$STAMP" "$(git rev-parse --short HEAD)" "$([ -n "$(git status --porcelain --untracked-files=no)" ] && echo true || echo false)" \
    "$(sysctl -n machdep.cpu.brand_string 2>/dev/null || grep -m1 'model name' /proc/cpuinfo | cut -d: -f2 | xargs)" \
    "$(getconf _NPROCESSORS_ONLN)" "$(uptime | sed 's/.*load averages*: //')"
}
emit "$(meta)"

if [ "${1:-}" != "--no-clean" ]; then
  cargo clean >/dev/null 2>&1
  s=$(now)
  cargo test --workspace --locked --no-fail-fast --no-run >"$WORK/clean-build.log" 2>&1
  e=$(now)
  emit "{\"kind\":\"build\",\"case\":\"clean\",\"seconds\":$(elapsed "$s" "$e")}"
fi

# Per-test times need libtest's unstable --report-time; RUSTC_BOOTSTRAP lets the stable toolchain's harness accept it.
# It changes cargo's fingerprint, so build once with it first and keep it set for everything measured below.
export RUSTC_BOOTSTRAP=1
cargo test --workspace --locked --no-fail-fast --no-run >/dev/null 2>&1

s=$(now)
cargo test --workspace --locked --no-fail-fast -- -Zunstable-options --report-time >"$WORK/run.log" 2>&1 || true
e=$(now)
emit "{\"kind\":\"run\",\"case\":\"all tests, warm build\",\"seconds\":$(elapsed "$s" "$e")}"

touch_and_build() {
  file=$1
  printf '\n// timing touch\n' >>"$file"
  s=$(now)
  cargo test --workspace --locked --no-fail-fast --no-run >"$WORK/incr.log" 2>&1
  e=$(now)
  git checkout -- "$file"
  emit "{\"kind\":\"build\",\"case\":\"incremental: touch $file\",\"seconds\":$(elapsed "$s" "$e")}"
}
touch_and_build crates/scout/src/util.rs
touch_and_build crates/remux/src/redact.rs
touch_and_build crates/hearth/src/route.rs
cargo test --workspace --locked --no-fail-fast --no-run >/dev/null 2>&1

python3 - "$WORK/run.log" "$RESULT" <<'PY'
import json, re, sys
log, result = sys.argv[1], sys.argv[2]
tests, crates, current = [], [], None
for line in open(log, errors="replace"):
    m = re.match(r"\s+Running (?:unittests )?(\S+)(?: \((.*)\))?", line)
    if m:
        binary = re.search(r"deps/(\w+?)-[0-9a-f]{16}", m.group(2) or "")
        current = f"{binary.group(1).replace('_', '-')} ({m.group(1)})" if binary else m.group(1)
        continue
    m = re.match(r"\s+Doc-tests (\S+)", line)
    if m:
        current = "doc-tests " + m.group(1)
    m = re.match(r"test (\S+) \.\.\. (\w+)(?: <([\d.]+)s>)?", line)
    if m and m.group(3):
        tests.append((float(m.group(3)), current, m.group(1), m.group(2)))
    m = re.match(r"test result: (\w+)\. (\d+) passed; (\d+) failed; (\d+) ignored.*finished in ([\d.]+)s", line)
    if m:
        crates.append({"kind": "crate", "binary": current, "result": m.group(1), "passed": int(m.group(2)),
                       "failed": int(m.group(3)), "ignored": int(m.group(4)), "seconds": float(m.group(5))})
with open(result, "a") as out:
    for c in crates:
        print(json.dumps(c), file=out)
    for secs, binary, name, outcome in sorted(tests, reverse=True)[:20]:
        print(json.dumps({"kind": "slowest", "binary": binary, "test": name, "outcome": outcome, "seconds": secs}), file=out)
PY
rm -rf "$WORK"
echo "wrote $RESULT"
