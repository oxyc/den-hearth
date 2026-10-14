#!/usr/bin/env python3
"""Enforcing HTTP load and soak run for the den-hearth release binary, in a memory-capped container.

    bench/http/run.py [--only case,case] [--list] [--no-build]
    env: DURATION=10 SOAK_DURATION=60 CONCURRENCY=16 MEMORY=512m RECOVERY_TIMEOUT=20 HEARTH_WORKER_THREADS=2
         CONTAINER_ENGINE=docker IMAGE=den-hearth:bench PLATFORM=linux/amd64 RESULTS=bench/results

Topology (all on one `--internal` Docker network, which has no route out, so no case CAN reach a real service):

    driver    runs load.py / remux_flow.py against hearth:8090      (the hearth image, for its python3)
    hearth    the release binary, `--memory M --memory-swap M`      (the container under test)
    stub      TMDB, KinoCheck, OpenSubtitles + CDN, scout, debrid   (bench/http/stubs.py)
    observer  --pid=host --cgroupns=host: reads hearth's cgroup and /proc from outside, so measuring costs it nothing

Every case: probe -> load (with a sampler on the cgroup) -> probe -> wait for /health and for resources to return to
the case's start AND the run's baseline -> gate. Any gate failure keeps the run's artifacts and exits non-zero. The
report is published (renamed from a hidden .partial) only after a terminal {"gate":"passed","complete":true} row.
Exit 77: no container engine, so a misleading host-only run was not made.
"""
import argparse
import base64
import hashlib
import json
import os
import pathlib
import shutil
import subprocess
import sys
import tempfile
import threading
import time

sys.path.insert(0, str(pathlib.Path(__file__).parent))
import gate  # noqa: E402

ROOT = pathlib.Path(__file__).resolve().parents[2]
ENGINE = os.environ.get("CONTAINER_ENGINE", "docker")
IMAGE = os.environ.get("IMAGE", "den-hearth:bench")
# The stub, driver and observer run native (not under amd64 emulation): they must not be the bottleneck being measured.
TOOLS_IMAGE = os.environ.get("TOOLS_IMAGE", "python:3.13-slim")
PROCS = int(os.environ.get("PROCS", "4"))
PLATFORM = os.environ.get("PLATFORM", "linux/amd64")
MEMORY = os.environ.get("MEMORY", "512m")
DURATION = float(os.environ.get("DURATION", "10"))
SOAK_DURATION = float(os.environ.get("SOAK_DURATION", "60"))
CONCURRENCY = int(os.environ.get("CONCURRENCY", "16"))
RECOVERY_TIMEOUT = float(os.environ.get("RECOVERY_TIMEOUT", "20"))
WORKERS = os.environ.get("HEARTH_WORKER_THREADS", "2")
RESULTS = pathlib.Path(os.environ.get("RESULTS", ROOT / "bench" / "results"))
RUN_ID = f"hearth-bench-{os.getpid()}"
NET, HEARTH, STUB, DRIVER, OBSERVER = (f"{RUN_ID}-{n}" for n in ("net", "hearth", "stub", "driver", "observer"))


def memory_bytes(text):
    units = {"k": 1 << 10, "m": 1 << 20, "g": 1 << 30}
    return int(text[:-1]) * units[text[-1].lower()] if text[-1].lower() in units else int(text)


def engine(*args, check=True, timeout=None, input=None):
    result = subprocess.run([ENGINE, *args], capture_output=True, text=True, timeout=timeout, input=input)
    if check and result.returncode:
        raise RuntimeError(f"{ENGINE} {' '.join(args[:3])} failed ({result.returncode}): {result.stderr.strip()[:600]}")
    return result


def b64url(obj):
    return base64.urlsafe_b64encode(json.dumps(obj, separators=(",", ":")).encode()).decode().rstrip("=")


SUBS_CFG = b64url({"osKey": "bench-os-key", "autoSync": False})


class Run:
    def __init__(self, out_dir):
        self.out = out_dir
        self.pid = None
        self.baseline = None
        self.rows = []
        self.failures = []
        self.memory_max = memory_bytes(MEMORY)

    # ---- containers --------------------------------------------------------------------------------------------
    def up(self):
        engine("network", "create", "--internal", NET)
        mount = f"{ROOT / 'bench' / 'http'}:/bench/http:ro"
        testdata = f"{ROOT / 'crates' / 'remux' / 'testdata'}:/testdata:ro"
        common = ["run", "-d", "--platform", PLATFORM, "--network", NET]
        native = ["run", "-d", "--network", NET]
        engine(*native, "--name", STUB, "--network-alias", "stub", "--memory", "1g", "-v", mount, "-v", testdata,
               "--entrypoint", "python3", TOOLS_IMAGE, "/bench/http/stubs.py", "--testdata", "/testdata")
        engine(*native, "--name", DRIVER, "--network-alias", "driver", "--memory", "1g", "-v", mount,
               "--entrypoint", "sleep", TOOLS_IMAGE, "infinity")
        # The driver is the trusted proxy: it varies X-Forwarded-For to stand in for many independent viewers, so the
        # per-visitor rate limits measure shared capacity and not one address's quota.
        driver_ip = engine("inspect", "-f", "{{(index .NetworkSettings.Networks \"" + NET + "\").IPAddress}}", DRIVER).stdout.strip()
        env = {
            "PORT": "8090", "HEARTH_WORKER_THREADS": WORKERS, "LOG_REQUESTS": "0",
            "REEL_TMDB_KEY": "bench-tmdb-key",
            "HEARTH_STUB_TMDB": "http://stub:9090/tmdb/3", "HEARTH_STUB_KINOCHECK": "http://stub:9090/kinocheck",
            "HEARTH_STUB_OPENSUBTITLES": "http://stub:9090/os",
            "REMUX_SCOUT_ORIGINS": "http://stub:9090", "REMUX_SUBTITLE_ORIGINS": "http://stub:9090",
            "REMUX_SCOUT_KEY": "bench-scout-key", "REMUX_URL_KEY": "bench-url-key",
            "REMUX_SCOUT_INSTALL_URL": "http://stub:9090/cfg", "REMUX_MAX_SESSIONS": "8",
            "REMUX_MAX_SESSIONS_PER_INSTALL": "2", "REMUX_TRUSTED_PROXIES": driver_ip,
            "REMUX_SESSION_IDLE_SECS": "300",
        }
        # EXTRA_ENV="A=1,B=2": more variables for the process under test (the allocator comparison sets MALLOC_ARENA_MAX).
        env |= dict(pair.split("=", 1) for pair in os.environ.get("EXTRA_ENV", "").split(",") if pair)
        envargs = [x for k, v in env.items() for x in ("-e", f"{k}={v}")]
        engine(*common, "--name", HEARTH, "--network-alias", "hearth", "--memory", MEMORY, "--memory-swap", MEMORY,
               "--pids-limit", "512", "-v", "/cache", *envargs, IMAGE)
        self.pid = int(engine("inspect", "-f", "{{.State.Pid}}", HEARTH).stdout.strip())
        engine("run", "-d", "--name", OBSERVER, "--network", NET, "--pid=host",
               "--cgroupns=host", "--privileged", "-v", "/sys/fs/cgroup:/sys/fs/cgroup:ro", "-v", mount,
               "--entrypoint", "sleep", TOOLS_IMAGE, "infinity")
        self.wait_health(60)

    def down(self):
        for name in (OBSERVER, DRIVER, HEARTH, STUB):
            engine("rm", "-f", "-v", name, check=False)
        engine("network", "rm", NET, check=False)

    # ---- measurement -------------------------------------------------------------------------------------------
    def exec(self, container, *cmd, timeout=None):
        return engine("exec", container, *cmd, timeout=timeout)

    def probe(self):
        out = self.exec(OBSERVER, "python3", "/bench/http/observer.py", "--once", str(self.pid)).stdout
        return json.loads(out)

    def sampler(self):
        proc = subprocess.Popen([ENGINE, "exec", OBSERVER, "python3", "/bench/http/observer.py", "--sample",
                                 str(self.pid), "0.25"], stdout=subprocess.PIPE, text=True)
        samples = []

        def read():
            for line in proc.stdout:
                try:
                    samples.append(json.loads(line))
                except ValueError:
                    pass

        thread = threading.Thread(target=read, daemon=True)
        thread.start()
        return proc, thread, samples

    def stub_stats(self):
        out = self.exec(DRIVER, "python3", "-c",
                        "import urllib.request;print(urllib.request.urlopen('http://stub:9090/stats').read().decode())").stdout
        return json.loads(out)

    def stub_control(self, slow_cdn):
        self.exec(DRIVER, "python3", "-c",
                  "import urllib.request;urllib.request.urlopen(urllib.request.Request("
                  f"'http://stub:9090/control/slow_cdn?on={1 if slow_cdn else 0}',method='POST')).read()")

    def healthy(self):
        result = engine("exec", DRIVER, "python3", "-c",
                        "import urllib.request,sys;r=urllib.request.urlopen('http://hearth:8090/health',timeout=3);"
                        "sys.exit(0 if r.status==200 else 1)", check=False, timeout=20)
        return result.returncode == 0

    def wait_health(self, seconds=RECOVERY_TIMEOUT):
        deadline = time.time() + seconds
        while time.time() < deadline:
            if self.healthy():
                return True
            time.sleep(0.3)
        return False

    def wait_recovery(self, before, pool=0, fill=0, timeout=None):
        deadline = time.time() + (timeout or RECOVERY_TIMEOUT)
        recovered = self.probe()
        while time.time() < deadline:
            if gate.recovered_enough([(before, pool, fill), (self.baseline, gate.RUN_POOL, gate.RUN_CACHE_FILL)], recovered):
                break
            time.sleep(0.4)
            recovered = self.probe()
        return recovered

    def load(self, script, args, timeout):
        if script == "load.py" and PROCS > 1 and "--requests" not in args and "--procs" not in args:
            args = [*args, "--procs", str(PROCS)]
        result = engine("exec", DRIVER, "python3", f"/bench/http/{script}", *args, timeout=timeout, check=False)
        if result.returncode:
            raise RuntimeError(f"{script} failed: {result.stderr.strip()[-800:]}")
        return json.loads(result.stdout.strip().splitlines()[-1])

    # ---- cases -------------------------------------------------------------------------------------------------
    def run_case(self, case):
        name = case["name"]
        if case.get("before"):
            case["before"](self)
        before = self.probe()
        stub_before = self.stub_stats()
        proc, thread, samples = self.sampler()
        try:
            if "parallel" in case:
                result = self.run_parallel(case)
            else:
                result = self.load(case.get("script", "load.py"), case["args"], timeout=case.get("timeout", 180))
        finally:
            proc.terminate()
            thread.join(timeout=5)
        after = self.probe()
        healthy = self.wait_health()
        recovered = self.wait_recovery(before, case["spec"].get("pool_allowance", 0), case["spec"].get("cache_fill_bytes", 0))
        stub_after = self.stub_stats()
        if case.get("after"):
            case["after"](self)
        samples.append(after)
        row, failures = gate.evaluate_case(case["spec"] | {"name": name}, result, samples, before, after, recovered,
                                           self.baseline, healthy, stub_before, stub_after)
        if "components" in result:
            row["components"] = result["components"]
        if "kinds" in result:
            row["kinds"] = result["kinds"]
        if "sessions" in result:
            row["sessions"] = result["sessions"]
        self.rows.append(row)
        print(json.dumps(row, separators=(",", ":")), flush=True)
        (self.out / f"{name}.samples.jsonl").write_text("\n".join(json.dumps(s, separators=(",", ":")) for s in samples))
        for failure in failures:
            print(f"GATE FAILURE [{name}]: {failure}", file=sys.stderr)
        self.failures += [f"{name}: {f}" for f in failures]
        return row

    def run_parallel(self, case):
        parts, errors = [], []

        def one(part):
            if part.get("script", "load.py") == "load.py":
                part = {**part, "args": [*part["args"], "--procs", "1"]}
            try:
                parts.append(self.load(part.get("script", "load.py"), part["args"], timeout=case.get("timeout", 180)))
            except Exception as e:  # reported through the gate as a client error
                errors.append(str(e))

        threads = [threading.Thread(target=one, args=(p,)) for p in case["parallel"]]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        if errors:
            raise RuntimeError("; ".join(errors))
        statuses, refusals, outcomes = {}, {}, {}
        for p in parts:
            for k, v in p["statuses"].items():
                statuses[k] = statuses.get(k, 0) + v
            for k, v in p.get("refusal_codes", {}).items():
                refusals[k] = refusals.get(k, 0) + v
            for k, v in p.get("error_outcomes", {}).items():
                outcomes[k] = outcomes.get(k, 0) + v
        elapsed = max(p["elapsed_s"] for p in parts)
        total_bytes = sum(p["payload_bytes"] for p in parts)
        requests = sum(p["requests"] for p in parts)
        return {
            "label": "mixed", "mode": "mixed", "concurrency": sum(p["concurrency"] for p in parts),
            "elapsed_s": elapsed, "requests": requests, "req_s": round(requests / elapsed, 2),
            "payload_bytes": total_bytes, "gib_s": round(total_bytes / elapsed / (1 << 30), 6),
            "p50_ms": None, "p95_ms": None, "p99_ms": None,
            "statuses": dict(sorted(statuses.items())), "errors": sum(p["errors"] for p in parts),
            "error_outcomes": outcomes, "refusal_codes": refusals,
            "components": [{k: p.get(k) for k in ("label", "mode", "concurrency", "requests", "req_s", "gib_s", "p50_ms", "p95_ms", "p99_ms", "statuses", "errors")} for p in parts],
        }


def build_cases(base="http://hearth:8090", manifests="/tmp/manifests.paths", scout="http://stub:9090"):
    """The case table. native.py reuses it with its own addresses."""
    d, c = DURATION, CONCURRENCY

    def load(*extra, **kw):
        return {"args": ["--base", base, *extra], **kw}

    def spec(**kw):
        return kw

    refusal_ok = {"429:rate_limited", "503:server_busy", "503:busy"}
    cases = [
        {"name": "control-health", "spec": spec(allowed_statuses={200}, min_requests=100),
         **load("--path", "/health", "--duration", str(d), "--concurrency", str(c), "--expect-prefix", '{"services"')},
        {"name": "remux-health", "spec": spec(allowed_statuses={200}, min_requests=100),
         **load("--path", "/remux/health", "--duration", str(d), "--concurrency", str(c), "--expect-prefix", "{")},
        {"name": "manifests", "spec": spec(allowed_statuses={200}, min_requests=100),
         **load("--paths-file", manifests, "--duration", str(d), "--concurrency", str(c), "--expect-prefix", "{")},
        {"name": "reel-meta-warm", "spec": spec(allowed_statuses={200}, min_requests=100),
         **load("--path", "/reel/meta/movie/tt0000001.json?prewarm=0", "--duration", str(d), "--concurrency", str(c), "--expect-prefix", '{"meta"')},
        {"name": "reel-meta-cold", "spec": spec(allowed_statuses={200}, min_requests=50),
         **load("--path-template", "/reel/meta/movie/tt{n:07d}.json?prewarm=0", "--template-offset", "1000", "--duration", str(d), "--concurrency", str(c), "--expect-prefix", '{"meta"')},
        {"name": "subs-search-warm", "spec": spec(allowed_statuses={200}, min_requests=100),
         **load("--path", f"/subs/{SUBS_CFG}/subtitles/movie/tt0000001.json", "--duration", str(d), "--concurrency", str(c), "--expect-prefix", '{"subtitles"')},
        {"name": "subs-search-cold", "spec": spec(allowed_statuses={200}, min_requests=50),
         **load("--path-template", f"/subs/{SUBS_CFG}/subtitles/movie/tt{{n:07d}}.json", "--template-offset", "5000", "--duration", str(d), "--concurrency", str(c), "--expect-prefix", '{"subtitles"')},
        {"name": "subs-serve-warm", "spec": spec(allowed_statuses={200}, min_requests=50, allow_max=True),
         **load("--path", f"/subs/{SUBS_CFG}/subtitle/101.srt", "--duration", str(d), "--concurrency", str(c), "--min-200-bytes", "50000")},
        {"name": "subs-serve-cold", "spec": spec(allowed_statuses={200}, min_requests=20, allow_max=True),
         **load("--path-template", f"/subs/{SUBS_CFG}/subtitle/{{n}}.srt", "--template-offset", "20000", "--duration", str(d), "--concurrency", str(c), "--min-200-bytes", "50000")},
        {"name": "subs-serve-slow-readers", "spec": spec(allowed_statuses={200}, min_requests=20, allow_max=True),
         **load("--path", f"/subs/{SUBS_CFG}/subtitle/101.srt", "--duration", str(d), "--concurrency", "24", "--mode", "slow-reader", "--slow-read-ms", "20", "--min-200-bytes", "50000")},
        {"name": "remux-speed", "spec": spec(allowed_statuses={200}, allowed_refusals=refusal_ok, min_requests=50),
         **load("--path", "/remux/speed?bytes=2097152", "--duration", str(d), "--concurrency", str(c), "--rotate-ip", "--expect-200-bytes", "2097152")},
        {"name": "remux-session-flow", "script": "remux_flow.py", "timeout": 240,
         "spec": spec(allowed_statuses={200, 201, 204, 503}, required_statuses={200, 201, 204}, min_requests=20, allow_max=True),
         "args": ["--base", base, "--scout", scout, "--sessions", "2", "--duration", str(max(d, 15)), "--concurrency", "4"]},
        {"name": "slow-readers-remux-speed", "spec": spec(allowed_statuses={200}, allowed_refusals=refusal_ok, min_requests=10),
         **load("--path", "/remux/speed?bytes=2097152", "--duration", str(d), "--concurrency", "24", "--mode", "slow-reader", "--slow-read-ms", "20", "--rotate-ip", "--expect-200-bytes", "2097152")},
        {"name": "cancellation-remux-speed", "spec": spec(cancel=True, min_requests=100),
         **load("--path", "/remux/speed?bytes=8388608", "--duration", str(d), "--concurrency", "32", "--mode", "cancel", "--rotate-ip")},
        # The subtitle download is a request-independent worker (it finishes and caches for the next viewer), so
        # cancelling the viewer is not expected to abort the upstream: the row records what the stub saw, and the
        # gate holds the process to returning to its baseline once the workers finish.
        {"name": "cancellation-subs-slow-upstream", "spec": spec(cancel=True, min_requests=100, allow_max=True),
         "before": lambda r: r.stub_control(True), "after": lambda r: r.stub_control(False),
         **load("--path-template", f"/subs/{SUBS_CFG}/subtitle/{{n}}.srt", "--template-offset", "700000", "--duration", str(d),
                "--concurrency", "16", "--mode", "cancel", "--cancel-delay-ms", "20")},
    ]
    soak = str(SOAK_DURATION)
    cases.append({
        "name": "mixed-soak", "timeout": int(SOAK_DURATION) + 120,
        "spec": spec(allowed_statuses={200, 201, 204, 503, 0}, required_statuses={200}, allowed_refusals=refusal_ok, min_requests=200, allow_max=True),
        "parallel": [
            {"args": ["--base", base, "--path", "/health", "--duration", soak, "--concurrency", "2", "--label", "health"]},
            {"args": ["--base", base, "--paths-file", manifests, "--duration", soak, "--concurrency", "2", "--label", "manifests"]},
            {"args": ["--base", base, "--path", "/reel/meta/movie/tt0000001.json?prewarm=0", "--duration", soak, "--concurrency", "2", "--label", "reel-meta-warm"]},
            {"args": ["--base", base, "--path-template", "/reel/meta/movie/tt{n:07d}.json?prewarm=0", "--template-offset", "200000", "--duration", soak, "--concurrency", "2", "--label", "reel-meta-cold"]},
            {"args": ["--base", base, "--path", f"/subs/{SUBS_CFG}/subtitles/movie/tt0000001.json", "--duration", soak, "--concurrency", "2", "--label", "subs-search"]},
            {"args": ["--base", base, "--path-template", f"/subs/{SUBS_CFG}/subtitle/{{n}}.srt", "--template-offset", "400000", "--duration", soak, "--concurrency", "2", "--label", "subs-serve-cold", "--min-200-bytes", "50000"]},
            {"args": ["--base", base, "--path", "/remux/speed?bytes=2097152", "--duration", soak, "--concurrency", "4", "--rotate-ip", "--label", "remux-speed", "--expect-200-bytes", "2097152"]},
            {"args": ["--base", base, "--path", "/remux/speed?bytes=2097152", "--duration", soak, "--concurrency", "8", "--mode", "slow-reader", "--slow-read-ms", "20", "--rotate-ip", "--label", "slow-readers"]},
            {"args": ["--base", base, "--path", "/remux/speed?bytes=8388608", "--duration", soak, "--concurrency", "8", "--mode", "cancel", "--rotate-ip", "--label", "cancellations"]},
            {"script": "remux_flow.py", "args": ["--base", base, "--scout", scout, "--sessions", "2", "--duration", soak, "--concurrency", "2"]},
        ],
    })
    # Cases that ask an upstream leave idle keep-alive connections pooled (reqwest: 90 s). They declare how many.
    for case in cases:
        if case["name"] in ("reel-meta-cold", "subs-search-cold", "subs-serve-warm", "subs-serve-cold",
                            "subs-serve-slow-readers", "remux-session-flow", "mixed-soak", "cancellation-subs-slow-upstream"):
            case["spec"]["pool_allowance"] = 128
        if case["name"] in ("subs-search-cold", "subs-serve-cold", "mixed-soak", "cancellation-subs-slow-upstream"):
            case["spec"]["cache_fill_bytes"] = gate.RUN_CACHE_FILL - 10 * 1024 * 1024  # Subtitles' 64 MiB memory tier fills
        if case["name"] == "reel-meta-cold":
            case["spec"]["cache_fill_bytes"] = 16 * 1024 * 1024  # Reel's resolve cache (10,000 entries)
    return cases


def idle_drain(run):
    """Idle for longer than the upstream pool's idle timeout, then require the process to be back to the run's
    baseline with NO pool allowance: the pooled connections must all go, and nothing else may remain."""
    started = time.time()
    deadline = started + float(os.environ.get("DRAIN_TIMEOUT", "150"))
    sample = run.probe()
    refs = [(run.baseline, 0, gate.RUN_CACHE_FILL)]  # descriptors and sockets must be exactly back; caches keep what they hold
    while time.time() < deadline and not gate.recovered_enough(refs, sample):
        time.sleep(3)
        sample = run.probe()
    drained = gate.recovered_enough(refs, sample)
    row = {"case": "idle-drain", "gate": "passed" if drained else "failed", "failures": [], "idle_s": round(time.time() - started, 1),
           "fds": {"baseline": run.baseline["cgroup_processes"]["fds"], "after": sample["cgroup_processes"]["fds"]},
           "sockets": {"established_baseline": run.baseline["tcp"]["established"], "established_after": sample["tcp"]["established"],
                       "to_stub_after": sample["tcp"]["established_to_stub"]},
           "rss_kib": {"baseline": run.baseline["cgroup_processes"]["rss_kib"], "after": sample["cgroup_processes"]["rss_kib"]},
           "cgroup": {"current_baseline": run.baseline["cgroup"]["current"], "current_after": sample["cgroup"]["current"],
                      "anon_baseline": run.baseline["cgroup"]["anon"], "anon_after": sample["cgroup"]["anon"],
                      "events_total": sample["cgroup"]["events"], "peak_kernel": sample["cgroup"]["peak"]},
           "threads": sample["init"]["threads"]}
    if not drained:
        row["failures"].append(f"after {row['idle_s']} s idle the process still holds more than the baseline: {row['fds']} {row['sockets']}")
    for field in gate.FATAL_EVENTS:
        if sample["cgroup"]["events"].get(field, 0) != run.baseline["cgroup"]["events"].get(field, 0):
            row["failures"].append(f"memory.events {field} = {sample['cgroup']['events'][field]} by the end of the run")
    return row


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--only")
    parser.add_argument("--list", action="store_true")
    parser.add_argument("--no-build", action="store_true")
    args = parser.parse_args()
    cases = build_cases()
    if args.list:
        print("\n".join(c["name"] for c in cases))
        return 0
    if args.only:
        wanted = set(args.only.split(","))
        cases = [c for c in cases if c["name"] in wanted]

    if shutil.which(ENGINE) is None or subprocess.run([ENGINE, "info"], capture_output=True).returncode:
        print(f"SKIP: {ENGINE} is not available, so no enforced container run was made", file=sys.stderr)
        return 77
    if engine("image", "inspect", IMAGE, check=False).returncode:
        if args.no_build:
            print(f"image {IMAGE} is missing", file=sys.stderr)
            return 1
        subprocess.run([ENGINE, "build", "--platform", PLATFORM, "--build-arg", f"DEN_VERSION={(ROOT / 'VERSION').read_text().strip()}",
                        "--build-arg", "HEARTH_FEATURES=stub-upstreams", "-t", IMAGE, str(ROOT)], check=True)

    RESULTS.mkdir(parents=True, exist_ok=True)
    stamp = time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())
    final = RESULTS / f"http-{stamp}-{os.getpid()}.jsonl"
    partial = RESULTS / f".{final.name}.partial"
    out = pathlib.Path(tempfile.mkdtemp(prefix="hearth-bench."))
    run = Run(out)
    code = 1
    try:
        run.up()
        # The container has no route out. Prove it rather than assume it: a run that could reach the real services
        # is not allowed to continue.
        egress = engine("exec", HEARTH, "python3", "-c",
                        "import socket,sys\ntry:\n socket.create_connection(('1.1.1.1',443),timeout=3)\nexcept OSError:\n sys.exit(0)\nsys.exit(1)",
                        check=False)
        if egress.returncode:
            raise RuntimeError("the benchmark network can reach the internet; refusing to run")
        (out / "manifests.paths").write_text(f"/reel/manifest.json\n/subs/manifest.json\n/subs/{SUBS_CFG}/manifest.json\n")
        run.exec(DRIVER, "sh", "-c", "true")
        engine("cp", str(out / "manifests.paths"), f"{DRIVER}:/tmp/manifests.paths")
        # Warm everything lazy (stub reachability, upstream clients, first ffmpeg start) before the baseline.
        for path in ("/reel/meta/movie/tt0000001.json?prewarm=0", f"/subs/{SUBS_CFG}/subtitles/movie/tt0000001.json",
                     f"/subs/{SUBS_CFG}/subtitle/101.srt", "/remux/speed?bytes=65536", "/reel/manifest.json"):
            run.load("load.py", ["--base", "http://hearth:8090", "--path", path, "--requests", "3", "--concurrency", "1", "--duration", "30"], 60)
        run.load("remux_flow.py", ["--base", "http://hearth:8090", "--scout", "http://stub:9090", "--sessions", "1", "--duration", "3", "--concurrency", "1"], 120)
        time.sleep(2)
        first = run.probe()
        gate.check_initial(first, run.memory_max)
        run.baseline = run.probe()
        header = {"kind": "meta", "run": RUN_ID, "date": stamp, "image": IMAGE, "platform": PLATFORM, "memory": MEMORY,
                  "memory_max_bytes": run.memory_max, "worker_threads": WORKERS, "duration_s": DURATION,
                  "soak_s": SOAK_DURATION, "concurrency": CONCURRENCY, "egress": "blocked (internal network)",
                  "extra_env": os.environ.get("EXTRA_ENV", ""),
                  "docker": engine("version", "-f", "{{.Server.Os}}/{{.Server.Arch}} {{.Server.Version}}").stdout.strip(),
                  "cpus": engine("info", "-f", "{{.NCPU}}").stdout.strip(), "vm_memory": engine("info", "-f", "{{.MemTotal}}").stdout.strip(),
                  "baseline": {"rss_kib": run.baseline["cgroup_processes"]["rss_kib"], "cgroup_current": run.baseline["cgroup"]["current"],
                               "anon": run.baseline["cgroup"]["anon"], "fds": run.baseline["cgroup_processes"]["fds"]}}
        partial.write_text(json.dumps(header) + "\n")
        for case in cases:
            run.run_case(case)
            with partial.open("a") as f:
                f.write(json.dumps(run.rows[-1], separators=(",", ":")) + "\n")
            if run.failures:
                break
        if not run.failures and not args.only:
            row = idle_drain(run)
            print(json.dumps(row, separators=(",", ":")), flush=True)
            with partial.open("a") as f:
                f.write(json.dumps(row, separators=(",", ":")) + "\n")
            run.failures += [f"idle-drain: {f}" for f in row["failures"]]
        if not run.failures:
            with partial.open("a") as f:
                f.write(json.dumps({"run": RUN_ID, "gate": "passed", "complete": not args.only}) + "\n")
            partial.rename(final)
            print(f"report={final}")
            code = 0
    except Exception as error:
        print(f"ERROR: {error}", file=sys.stderr)
        run.failures.append(str(error))
    finally:
        if run.failures:
            keep = RESULTS / f"failed-{out.name}"
            for name in (HEARTH, STUB):
                logs = engine("logs", name, check=False)
                (out / f"{name}.log").write_text(logs.stdout + logs.stderr)
            if partial.exists():
                shutil.move(str(partial), out / "report.partial.jsonl")
            shutil.move(str(out), keep)
            print(f"failure artifacts={keep}", file=sys.stderr)
        else:
            shutil.rmtree(out, ignore_errors=True)
        run.down()
    return code


if __name__ == "__main__":
    sys.exit(main())
