#!/usr/bin/env python3
"""The same cases against the release binary running NATIVELY on this machine: no container, no emulation.

    cargo build --release -p den-hearth --features stub-upstreams
    bench/http/native.py [--only case,case]

The Docker run is the enforcing one (it has a cgroup to read) but, on an Apple-silicon laptop, runs the amd64 image under
emulation, so its CPU-bound numbers are pessimistic. This run gives the CPU path without that: the stubs and the load
driver run on this machine too (stubs.py, load.py, unchanged), hearth listens on 127.0.0.1, and the numbers are req/s,
latency, CPU cores (the process's user+system time from rusage) and peak RSS (rusage maxrss). There is no memory cap, no
OOM gate and no cgroup here, and no Remux session case (it needs the image's ffmpeg-remux).
"""
import json
import os
import pathlib
import resource
import signal
import socket
import subprocess
import sys
import tempfile
import time

sys.path.insert(0, str(pathlib.Path(__file__).parent))
import run as harness  # noqa: E402

ROOT = harness.ROOT
BIN = ROOT / "target" / "release" / "den-hearth"
# Session flow: needs the image's ffmpeg-remux. Cancellation: macOS exhausts its 16k ephemeral ports in seconds at that
# rate, and the case needs the cgroup to mean anything. Soak: the Docker run's job.
SKIP = {"remux-session-flow", "cancellation-subs-slow-upstream", "cancellation-remux-speed", "mixed-soak", "remux-health"}


def free_port():
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def main():
    only = set(sys.argv[sys.argv.index("--only") + 1].split(",")) if "--only" in sys.argv else None
    if not BIN.exists():
        sys.exit(f"{BIN} is missing: cargo build --release -p den-hearth --features stub-upstreams")
    work = pathlib.Path(tempfile.mkdtemp(prefix="hearth-native."))
    stub_port, port = free_port(), free_port()
    (work / "manifests.paths").write_text(f"/reel/manifest.json\n/subs/manifest.json\n/subs/{harness.SUBS_CFG}/manifest.json\n")
    stub = subprocess.Popen([sys.executable, str(ROOT / "bench/http/stubs.py"), "--port", str(stub_port),
                             "--testdata", str(ROOT / "crates/remux/testdata")], stdout=subprocess.DEVNULL)
    env = {**os.environ, "PORT": str(port), "LOG_REQUESTS": "0", "REEL_TMDB_KEY": "bench-tmdb-key",
           "HEARTH_STUB_TMDB": f"http://127.0.0.1:{stub_port}/tmdb/3", "HEARTH_STUB_KINOCHECK": f"http://127.0.0.1:{stub_port}/kinocheck",
           "HEARTH_STUB_OPENSUBTITLES": f"http://127.0.0.1:{stub_port}/os", "REEL_CACHE_DIR": str(work / "reel"),
           "SUBS_CACHE_DIR": str(work / "subs"), "REMUX_SCRATCH_DIR": str(work / "remux"), "REMUX_URL_KEY": "bench-url-key",
           "REMUX_TRUSTED_PROXIES": "127.0.0.1", "HEARTH_WORKER_THREADS": os.environ.get("HEARTH_WORKER_THREADS", "2")}
    (work / "remux").mkdir()
    hearth = subprocess.Popen([str(BIN)], env=env, stderr=subprocess.DEVNULL)
    base = f"http://127.0.0.1:{port}"
    for _ in range(100):
        try:
            socket.create_connection(("127.0.0.1", port), timeout=0.2).close()
            break
        except OSError:
            time.sleep(0.1)

    stamp = time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())
    out = harness.RESULTS / f"native-{stamp}.jsonl"
    harness.RESULTS.mkdir(parents=True, exist_ok=True)
    rows = [{"kind": "meta", "date": stamp, "binary": "target/release/den-hearth --features stub-upstreams",
             "machine": subprocess.run(["sysctl", "-n", "machdep.cpu.brand_string"], capture_output=True, text=True).stdout.strip() or "?",
             "cpus": os.cpu_count(), "worker_threads": env["HEARTH_WORKER_THREADS"], "duration_s": harness.DURATION,
             "concurrency": harness.CONCURRENCY, "load": os.getloadavg()}]
    failed = False
    try:
        for case in harness.build_cases(base, str(work / "manifests.paths"), f"http://127.0.0.1:{stub_port}"):
            if case["name"] in SKIP or (only and case["name"] not in only):
                continue
            args = [sys.executable, str(ROOT / "bench/http" / case.get("script", "load.py")), *case["args"]]
            if case.get("script", "load.py") == "load.py" and "--requests" not in args:
                args += ["--procs", str(harness.PROCS)]
            cpu_before = _cpu(hearth.pid)
            result = subprocess.run(args, capture_output=True, text=True)
            if result.returncode:
                sys.exit(f"{case['name']}: {result.stderr[-500:]}")
            res = json.loads(result.stdout.strip().splitlines()[-1])
            res.pop("_hist", None)
            row = {"case": case["name"], **{k: res[k] for k in ("concurrency", "elapsed_s", "requests", "req_s", "gib_s", "p50_ms", "p95_ms", "p99_ms", "statuses", "errors")},
                   "cpu_cores": round((_cpu(hearth.pid) - cpu_before) / res["elapsed_s"], 2), "rss_mib": round(_rss(hearth.pid) / 1024, 1)}
            bad = [s for s in res["statuses"] if int(s) not in case["spec"].get("allowed_statuses", {200})] or res["errors"]
            row["gate"] = "failed" if bad else "passed"
            failed = failed or bool(bad)
            rows.append(row)
            print(json.dumps(row, separators=(",", ":")), flush=True)
    finally:
        hearth.send_signal(signal.SIGTERM)
        hearth.wait(timeout=20)
        stub.terminate()
        usage = resource.getrusage(resource.RUSAGE_CHILDREN)
        rows.append({"kind": "process", "max_rss_mib": round(usage.ru_maxrss / (1 << 20), 1) if sys.platform == "darwin" else round(usage.ru_maxrss / 1024, 1)})
        out.write_text("\n".join(json.dumps(r, separators=(",", ":")) for r in rows) + "\n")
        print(f"wrote {out}")
    return 1 if failed else 0


def _ps(pid, field):
    return subprocess.run(["ps", "-o", f"{field}=", "-p", str(pid)], capture_output=True, text=True).stdout.strip()


def _rss(pid):
    return int(_ps(pid, "rss") or 0)  # KiB


def _cpu(pid):
    """Cumulative CPU seconds of `pid` (ps prints [dd-][hh:]mm:ss.ss)."""
    text = _ps(pid, "time")
    days, _, rest = text.rpartition("-")
    seconds = 0.0
    for part in rest.split(":"):
        seconds = seconds * 60 + float(part)
    return seconds + (int(days) * 86400 if days else 0)


if __name__ == "__main__":
    sys.exit(main())
