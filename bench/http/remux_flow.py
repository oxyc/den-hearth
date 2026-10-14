#!/usr/bin/env python3
"""A viewer's traffic against Remux, with a local sample and no real debrid.

    remux_flow.py --base http://hearth:8090 --scout http://stub:9090 --sessions 2 --duration 15 --concurrency 4

Each session is one `POST /remux/session` against a stub scout install (`<scout>/cfgN`; the stub lists the sample and
302s its play URL to a Range-capable file), which starts a real ffmpeg job over the sample. The workers then do what
hls.js does: master.m3u8, media.m3u8, init.mp4, then segments in order, at the pace the server makes them (a 503 with
Retry-After is "not made yet" and is counted, not an error). Sessions are DELETEd at the end.

Prints one JSON object in load.py's shape, plus `kinds`: per-request-kind latency and byte counts.
"""
import argparse
import collections
import http.client
import json
import math
import threading
import time
import urllib.parse


def percentile(values, p):
    if not values:
        return 0.0
    values = sorted(values)
    return values[min(len(values) - 1, math.ceil(len(values) * p) - 1)]


def request(conn, method, path, body=None, headers=None):
    conn.request(method, path, body=body, headers=headers or {})
    response = conn.getresponse()
    data = response.read()
    return response, data


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--base", required=True)
    parser.add_argument("--scout", required=True, help="the stub scout origin; install N is <scout>/cfgN")
    parser.add_argument("--imdb", default="tt0000001")
    parser.add_argument("--sessions", type=int, default=2)
    parser.add_argument("--duration", type=float, default=15)
    parser.add_argument("--concurrency", type=int, default=4, help="workers per session")
    args = parser.parse_args()

    parsed = urllib.parse.urlsplit(args.base)
    lock = threading.Lock()
    latencies = []
    kinds = collections.defaultdict(lambda: {"requests": 0, "bytes": 0, "lat": []})
    statuses = collections.Counter()
    errors = collections.Counter()
    pending_503 = 0
    payload_bytes = 0
    sessions = []

    def connect():
        return http.client.HTTPConnection(parsed.hostname, parsed.port, timeout=40)

    # Start the sessions one after another: each one's first request is also the first ffmpeg start.
    for n in range(args.sessions):
        conn = connect()
        body = json.dumps({"imdb": args.imdb, "scout": f"{args.scout}/cfg{n + 1}", "player": "hls.js"})
        started = time.perf_counter()
        response, data = request(conn, "POST", "/remux/session", body, {"Content-Type": "application/json"})
        elapsed = time.perf_counter() - started
        with lock:
            statuses[response.status] += 1
            kinds["session-create"]["requests"] += 1
            kinds["session-create"]["lat"].append(elapsed)
        if response.status != 201:
            errors[f"session {n}: {response.status} {data[:200]!r}"] += 1
            continue
        created = json.loads(data)
        playlist = created["playlist"]
        base_path = urllib.parse.urlsplit(playlist).path.rsplit("/", 1)[0]
        sessions.append({"base": base_path, "segments": len(created.get("segments", [])) or 1})
        conn.close()

    deadline = time.monotonic() + args.duration

    def worker(session, offset):
        nonlocal pending_503, payload_bytes
        conn = connect()
        base = session["base"]
        plan = ["master.m3u8", "media.m3u8", "init.mp4"]
        i = offset
        while time.monotonic() < deadline:
            if plan:
                name = plan.pop(0)
            else:
                name = f"seg{i % session['segments']}.m4s"
                i += 1
            kind = name.rsplit(".", 1)[-1] if not name.startswith("seg") else "segment"
            started = time.perf_counter()
            try:
                response, data = request(conn, "GET", f"{base}/{name}")
            except (OSError, http.client.HTTPException) as error:
                conn.close()
                conn = connect()
                with lock:
                    errors[f"{name}: {type(error).__name__}: {error}"] += 1
                continue
            elapsed = time.perf_counter() - started
            with lock:
                statuses[response.status] += 1
                if response.status == 503 and kind == "segment":
                    pending_503 += 1
                elif response.status != 200:
                    errors[f"{name}: status {response.status}"] += 1
                else:
                    if name.endswith(".m3u8") and not data.startswith(b"#EXTM3U"):
                        errors[f"{name}: not a playlist"] += 1
                    if name == "init.mp4" and b"ftyp" not in data[:64]:
                        errors[f"{name}: not an MP4 init segment"] += 1
                    if kind == "segment" and not data:
                        errors[f"{name}: empty segment"] += 1
                    payload_bytes += len(data)
                    k = kinds[kind]
                    k["requests"] += 1
                    k["bytes"] += len(data)
                    k["lat"].append(elapsed)
                    latencies.append(elapsed)
            if response.status == 503:
                time.sleep(float(response.getheader("Retry-After", "1")) / 2)
        conn.close()

    began = time.monotonic()
    threads = [
        threading.Thread(target=worker, args=(s, w))
        for s in sessions
        for w in range(args.concurrency)
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    elapsed = time.monotonic() - began

    for s in sessions:
        conn = connect()
        response, _ = request(conn, "DELETE", s["base"])
        with lock:
            statuses[response.status] += 1
            if response.status != 204:
                errors[f"delete: {response.status}"] += 1
        conn.close()

    completed = len(latencies)
    print(json.dumps({
        "label": "remux-flow",
        "mode": "normal",
        "method": "mixed",
        "concurrency": args.concurrency * max(1, len(sessions)),
        "elapsed_s": round(elapsed, 3),
        "requests": completed,
        "attempted": completed + pending_503,
        "req_s": round(completed / elapsed, 2) if elapsed else 0,
        "payload_bytes": payload_bytes,
        "gib_s": round(payload_bytes / elapsed / (1024**3), 6) if elapsed else 0,
        "p50_ms": round(percentile(latencies, 0.5) * 1000, 3),
        "p95_ms": round(percentile(latencies, 0.95) * 1000, 3),
        "p99_ms": round(percentile(latencies, 0.99) * 1000, 3),
        "max_ms": round(max(latencies, default=0) * 1000, 3),
        "statuses": dict(sorted(statuses.items())),
        "errors": sum(errors.values()),
        "error_outcomes": dict(errors),
        "refusal_codes": {},
        "sessions": len(sessions),
        "segment_503_retries": pending_503,
        "kinds": {
            name: {
                "requests": k["requests"],
                "bytes": k["bytes"],
                "p50_ms": round(percentile(k["lat"], 0.5) * 1000, 3),
                "p95_ms": round(percentile(k["lat"], 0.95) * 1000, 3),
            }
            for name, k in kinds.items()
        },
    }, separators=(",", ":")))


if __name__ == "__main__":
    main()
