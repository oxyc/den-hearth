"""Resolver benchmark for Reel: how fast does a trailer resolve, and does the answer PLAY?

A resolve that returns a URL is not a success: on 2026-10-11 about one extraction in four came back
with URLs that googlevideo refuses past ~60 s of media (see `direct::refuses_its_end`), and the
rustypipe trial (den-hearth#6) managed 1 of 83 past the first minute. So every resolved stream is
fetched with Range requests at several offsets and must answer 206 with data at all of them.

It drives a worker script over Reel's own protocol (one JSON line in, one out), so the baseline is the
real `resolve.py` and a candidate is whatever script and request fields it names.

    python3 -I bench/reel/bench.py --candidate A --out bench/results/reel-resolve/A.jsonl

Environment: YTDLP_ZIP (the pinned zipapp), DENO_DIR_BIN (directory holding `deno`), BENCH_SCRATCH
(throwaway cache dirs). Sequential on purpose, with `GAP_SECS` between resolves: this talks to YouTube.
"""

import argparse
import json
import os
import shutil
import statistics
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request

HERE = os.path.dirname(os.path.abspath(__file__))
GAP_SECS = 3.0
UA = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4 Safari/605.1.15"


def ladder(cap=1080, steps=(720, 480)):
    """Mirror of `config::format_ladder` in crates/reel/src/config.rs."""
    rung = lambda h: f"bv*[height<={h}][vcodec^=avc1]+ba[acodec^=mp4a]/b[height<={h}][vcodec^=avc1][acodec^=mp4a]/"
    out = rung(cap) + "".join(rung(s) for s in steps if s < cap)
    return out + "18/b[ext=mp4][vcodec^=avc1][acodec^=mp4a]"


# name -> worker script (relative to the repo's crates/reel/worker or this directory) + request fields.
CANDIDATES = {
    # A: the worker as shipped before this change, no player-client pin.
    "A": {"worker": "baseline_resolve.py", "request": {}},
    # B knobs, each on top of the baseline worker where the baseline can express it.
    "A+visionos": {"worker": "baseline_resolve.py", "request": {"extractor_args": "youtube:player_client=visionos"}},
}


def load_candidates():
    path = os.path.join(HERE, "candidates.json")
    if os.path.exists(path):
        with open(path) as f:
            # {SCRATCH} is where the throwaway installs live (ytjs/, xdg/): BENCH_SCRATCH.
            CANDIDATES.update(json.loads(f.read().replace("{SCRATCH}", os.environ.get("BENCH_SCRATCH", ""))))


class Worker:
    def __init__(self, script, env_extra, cmd=None, cwd=None):
        env = dict(os.environ)
        env.update(env_extra)
        env["PATH"] = os.environ.get("DENO_DIR_BIN", "") + os.pathsep + env.get("PATH", "")
        argv = [a.replace("{script}", script) for a in cmd] if cmd else [sys.executable, "-I", script]
        self.proc = subprocess.Popen(
            argv, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL, text=True, env=env, cwd=cwd,
        )

    def ask(self, request, timeout=60):
        self.proc.stdin.write(json.dumps(request) + "\n")
        self.proc.stdin.flush()
        # Blocking read; the worker answers or dies. A hung one is caught by the per-run wall clock.
        line = self.proc.stdout.readline()
        if not line:
            return {"ok": False, "error": "worker closed its output"}
        return json.loads(line)

    def stop(self):
        try:
            self.proc.kill()
            self.proc.wait(timeout=5)
        except Exception:
            pass


def param(url, key):
    for p in url.split("?", 1)[-1].split("&"):
        if p.startswith(key + "="):
            return p[len(key) + 1:]
    return None


def ranged(url, start, length=32768):
    req = urllib.request.Request(url, headers={"Range": f"bytes={start}-{start + length - 1}", "User-Agent": UA})
    try:
        with urllib.request.urlopen(req, timeout=15) as res:
            body = res.read(length)
            total = (res.headers.get("Content-Range") or "").rpartition("/")[2]
            return res.status, len(body), total
    except urllib.error.HTTPError as e:
        return e.code, 0, ""
    except Exception as e:  # noqa: BLE001 - recorded, never raised: a network error is a finding
        return type(e).__name__, 0, ""


def playable(url, duration):
    """Range-fetch one stream at the start (control), 25/50/90 % and just past 60 s of media."""
    status0, n0, total = ranged(url, 0)
    length = param(url, "clen") or total
    out = {"start": [status0, n0], "at": {}}
    if not length or not str(length).isdigit():
        out["ok"] = False
        out["why"] = "no content length"
        return out
    length = int(length)
    out["length"] = length
    fracs = {"25%": 0.25, "50%": 0.5, "90%": 0.9, "t70s": min(0.95, 70.0 / duration) if duration else 0.9}
    for name, frac in fracs.items():
        status, n, _ = ranged(url, min(int(length * frac), max(0, length - 32768)))
        out["at"][name] = [status, n]
    out["ok"] = status0 == 206 and n0 > 0 and all(s == 206 and n > 0 for s, n in out["at"].values())
    return out


def check(answer, duration):
    urls = answer.get("urls") or []
    streams = [playable(u, duration) for u in urls]
    hls = answer.get("hls")
    hls_ok = None
    if hls:
        try:
            with urllib.request.urlopen(urllib.request.Request(hls, headers={"User-Agent": UA}), timeout=15) as r:
                hls_ok = r.status == 200 and b"#EXTM3U" in r.read(4096)
        except Exception:  # noqa: BLE001
            hls_ok = False
    return {
        "streams": streams,
        "playable": bool(streams) and all(s["ok"] for s in streams),
        "hls_ok": hls_ok,
        "expire": [param(u, "expire") for u in urls],
    }


def resolve_one(worker, spec, cache, video, phase, name):
    request = {"id": video["id"], "format": ladder(), "cache": cache, **spec["request"]}
    started = time.monotonic()
    answer = worker.ask(request)
    ms = round((time.monotonic() - started) * 1000)
    row = {
        "candidate": name, "phase": phase, "vid": video["id"], "ok": bool(answer.get("ok")),
        "ms": ms, "height": answer.get("height"), "n_urls": len(answer.get("urls") or []),
    }
    if answer.get("ok"):
        row.update(check(answer, video.get("duration")))
    else:
        row["error"] = (answer.get("error") or "")[-300:]
    return row


def run(args):
    """Every candidate sees the same videos in the same minutes.

    The first version ran candidates one after another, and YouTube's behaviour drifted between them
    (a stretch where nearly every answer was capped past ~60 s), so a candidate's score said more about
    WHEN it ran than about what it did. Now each video goes to every candidate, in a rotating order,
    so any drift lands on all of them alike.
    """
    load_candidates()
    names = args.candidate.split(",")
    specs = {n: CANDIDATES[n] for n in names}
    with open(os.path.join(HERE, "corpus.json")) as f:
        corpus = json.load(f)[: args.videos]
    os.makedirs(os.path.dirname(os.path.abspath(args.out)), exist_ok=True)
    scratch = tempfile.mkdtemp(prefix="reel-bench-", dir=os.environ.get("BENCH_SCRATCH"))
    throttled = []

    def start(name):
        spec = specs[name]
        script = spec["worker"]
        script = script if os.path.isabs(script) else os.path.join(HERE, script)
        env = {"YTDLP_ZIP": os.environ["YTDLP_ZIP"], **spec.get("env", {})}
        return Worker(script, env, spec.get("cmd"), spec.get("cwd"))

    def emit(row):
        with open(args.out, "a") as f:
            f.write(json.dumps(row) + "\n")
        print(json.dumps({k: row.get(k) for k in ("candidate", "phase", "vid", "ok", "ms", "playable", "error")}), flush=True)
        said = row.get("error", "").lower()
        if "429" in said or "not a bot" in said or "too many requests" in said:
            throttled.append(row)
        time.sleep(GAP_SECS)

    # Phase 1 - cold: a fresh process and an empty yt-dlp cache each time.
    for i in range(args.cold):
        for name in names:
            w = start(name)
            emit(resolve_one(w, specs[name], os.path.join(scratch, f"cold-{name}-{i}"), corpus[i], "cold", name))
            w.stop()

    # Phase 2 - warm: one long-lived worker per candidate over the whole corpus. The first answer pays
    # the cold start and is labelled `first`; the rest are the steady state.
    workers = {n: start(n) for n in names}
    for i, video in enumerate(corpus):
        for name in names[i % len(names):] + names[: i % len(names)]:
            emit(resolve_one(workers[name], specs[name], os.path.join(scratch, f"warm-{name}"), video,
                             "first" if i == 0 else "warm", name))
        if throttled:
            print("YouTube is throttling; stopping.", flush=True)
            break
    for w in workers.values():
        w.stop()

    # Phase 3 - restart: a new process, but the cache directory survives (a redeploy on the /cache volume).
    for i in range(args.cold if not throttled else 0):
        for name in names:
            w = start(name)
            emit(resolve_one(w, specs[name], os.path.join(scratch, f"warm-{name}"), corpus[-1 - i], "restart", name))
            w.stop()
    shutil.rmtree(scratch, ignore_errors=True)


def pct(values, q):
    values = sorted(values)
    if not values:
        return None
    k = (len(values) - 1) * q
    lo, hi = int(k), min(int(k) + 1, len(values) - 1)
    return round(values[lo] + (values[hi] - values[lo]) * (k - lo))


def summarize(paths):
    by = {}
    for p in paths:
        with open(p) as f:
            for line in f:
                r = json.loads(line)
                by.setdefault(r["candidate"], []).append(r)
    print("| candidate | n | success | playable | cold p50 (n) | restart p50 (n) | warm p50 | warm p95 | 360p-or-lower |")
    print("|---|---|---|---|---|---|---|---|---|")
    for name, rows in by.items():
        ok = [r for r in rows if r["ok"]]
        play = [r for r in rows if r.get("playable")]
        ms = lambda ph: [r["ms"] for r in ok if r["phase"] in ph]
        low = [r for r in ok if (r.get("height") or 0) < 720]
        print(
            f"| {name} | {len(rows)} | {len(ok)}/{len(rows)} | {len(play)}/{len(rows)} "
            f"| {pct(ms(['cold','first']), .5)} ({len(ms(['cold','first']))}) "
            f"| {pct(ms(['restart']), .5)} ({len(ms(['restart']))}) "
            f"| {pct(ms(['warm']), .5)} | {pct(ms(['warm']), .95)} | {len(low)} |"
        )


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--candidate")
    ap.add_argument("--out")
    ap.add_argument("--videos", type=int, default=44)
    ap.add_argument("--cold", type=int, default=3)
    ap.add_argument("--summarize", nargs="*")
    a = ap.parse_args()
    if a.summarize is not None:
        summarize(a.summarize)
    else:
        run(a)
