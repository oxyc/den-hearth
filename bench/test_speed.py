#!/usr/bin/env python3
"""Before/after timing of the test suite: clean build, warm run (cargo test and nextest), incremental rebuild and the
slowest tests. Every case is repeated --runs times and reported as wall AND CPU seconds (user+sys of the whole
process tree), because on a busy machine wall time measures the neighbours and CPU time does not. Appends JSONL.

  bench/test_speed.py --label after --root . --target /path/to/target [--runs 3] [--skip clean,nextest] \
      [--cargo-arg=--config=...]

`--root` is the checkout to measure (a second `git worktree` at the base commit gives the "before").
"""
import argparse, json, os, re, resource, subprocess, sys, time
from pathlib import Path


def run(cmd, cwd, env):
    """Run to completion; return (wall seconds, cpu seconds, combined output)."""
    before = resource.getrusage(resource.RUSAGE_CHILDREN)
    t0 = time.perf_counter()
    p = subprocess.run(cmd, cwd=cwd, env=env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, errors="replace")
    wall = time.perf_counter() - t0
    after = resource.getrusage(resource.RUSAGE_CHILDREN)
    cpu = (after.ru_utime + after.ru_stime) - (before.ru_utime + before.ru_stime)
    return round(wall, 2), round(cpu, 2), p.returncode, p.stdout


def slowest(log, n=10):
    tests, current = [], None
    for line in log.splitlines():
        m = re.match(r"\s+Running (?:unittests )?(\S+)(?: \((.*)\))?", line)
        if m:
            b = re.search(r"deps/(\w+?)-[0-9a-f]{16}", m.group(2) or "")
            current = b.group(1).replace("_", "-") if b else m.group(1)
        m = re.match(r"test (\S+) \.\.\. ok <([\d.]+)s>", line)
        if m:
            tests.append((float(m.group(2)), current, m.group(1)))
    return [{"seconds": s, "binary": b, "test": t} for s, b, t in sorted(tests, reverse=True)[:n]]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--label", required=True)
    ap.add_argument("--root", default=".")
    ap.add_argument("--target", required=True)
    ap.add_argument("--runs", type=int, default=3)
    ap.add_argument("--skip", default="", help="comma list of: clean, cargo, nextest, incremental, slowest")
    ap.add_argument("--out", default=str(Path(__file__).parent / "results" / "test-speed"))
    ap.add_argument("--cargo-arg", action="append", default=[], help="extra cargo argument, e.g. --config=...")
    ap.add_argument("--nextest", default="nix run nixpkgs#cargo-nextest --")
    a = ap.parse_args()
    skip = set(filter(None, a.skip.split(",")))
    root = Path(a.root).resolve()
    env = dict(os.environ, CARGO_TARGET_DIR=a.target, CARGO_TERM_COLOR="never")
    cargo = ["cargo"] + a.cargo_arg
    build = cargo + ["test", "--workspace", "--locked", "--no-fail-fast", "--no-run"]
    out = Path(a.out)
    out.mkdir(parents=True, exist_ok=True)
    path = out / f"{a.label}-{time.strftime('%Y%m%dT%H%M%SZ', time.gmtime())}.jsonl"
    commit = subprocess.run(["git", "rev-parse", "--short", "HEAD"], cwd=root, capture_output=True, text=True).stdout.strip()

    def emit(rec):
        rec["label"] = a.label
        line = json.dumps(rec)
        print(line, flush=True)
        with open(path, "a") as f:
            f.write(line + "\n")

    emit({"kind": "meta", "commit": commit, "load": os.getloadavg(), "runs": a.runs, "cargo_args": a.cargo_arg})

    def repeat(kind, case, fn):
        for i in range(a.runs):
            wall, cpu, rc, log = fn()
            emit({"kind": kind, "case": case, "run": i, "wall": wall, "cpu": cpu, "rc": rc})
        return log

    if "clean" not in skip:
        def clean():
            run(["cargo", "clean"], root, env)
            return run(build, root, env)
        repeat("build", "clean", clean)
    run(build, root, env)

    if "cargo" not in skip:
        repeat("run", "cargo test (warm)", lambda: run(cargo + ["test", "--workspace", "--locked", "--no-fail-fast"], root, env))
    if "nextest" not in skip:
        nx = a.nextest.split() + ["nextest", "run", "--workspace", "--locked", "--no-fail-fast"] + a.cargo_arg
        repeat("run", "nextest (warm)", lambda: run(nx, root, env))

    if "incremental" not in skip:
        for rel in ("crates/scout/src/util.rs", "crates/remux/src/redact.rs"):
            f = root / rel
            orig = f.read_bytes()

            def touch():
                f.write_bytes(orig + b"\n// timing touch\n")
                try:
                    return run(build, root, env)
                finally:
                    f.write_bytes(orig)
            repeat("build", f"incremental: touch {rel}", touch)
            run(build, root, env)

    if "slowest" not in skip:
        e = dict(env, RUSTC_BOOTSTRAP="1")
        _, _, _, log = run(cargo + ["test", "--workspace", "--locked", "--no-fail-fast", "--", "-Zunstable-options", "--report-time"], root, e)
        for rec in slowest(log):
            emit(dict(rec, kind="slowest"))
    print("wrote", path)


if __name__ == "__main__":
    main()
