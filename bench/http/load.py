#!/usr/bin/env python3
"""Small dependency-free HTTP load driver with strict response checking, cancellation and slow readers.

Ported from den-edge's bench/load.py (same framing checks, same result shape) and extended with request methods and
bodies, templated paths for cold unique requests, and a content prefix check. It runs INSIDE the benchmark network
(`docker exec driver python3 /bench/http/load.py ...`) because that network has no route out.

    load.py --base http://hearth:8090 --path /health --duration 10 --concurrency 16 --expect-200-bytes 123

Modes: normal (keep-alive client), slow-reader (reads a few bytes per --slow-read-ms), cancel (sends the request, waits
--cancel-delay-ms, closes the socket). The result is one JSON object on stdout; a client-side error (reset, truncated
body, Content-Length mismatch, unframed 200) is counted in `errors` and fails the case in the gate.
"""
import argparse
import collections
import http.client
import json
import math
import pathlib
import socket
import threading
import time
import urllib.parse


def percentile(values, p):
    if not values:
        return 0.0
    values = sorted(values)
    return values[min(len(values) - 1, math.ceil(len(values) * p) - 1)]


def refusal_code(payload):
    try:
        value = json.loads(payload)
        return value.get("error") if isinstance(value, dict) and isinstance(value.get("error"), str) else None
    except (UnicodeDecodeError, json.JSONDecodeError):
        return None


def decode_chunked(payload):
    decoded = bytearray()
    rest = payload
    while True:
        line, separator, rest = rest.partition(b"\r\n")
        if not separator:
            raise OSError("truncated chunk header")
        try:
            size = int(line.split(b";", 1)[0], 16)
        except ValueError as error:
            raise OSError("invalid chunk header") from error
        if size == 0:
            if rest == b"\r\n":
                return bytes(decoded)
            if rest.endswith(b"\r\n\r\n"):
                trailers = rest[:-4].split(b"\r\n")
                if trailers and all(line and b":" in line for line in trailers):
                    return bytes(decoded)
            raise OSError("truncated or malformed chunk trailer")
        if len(rest) < size + 2 or rest[size : size + 2] != b"\r\n":
            raise OSError(f"truncated chunk: declared={size} available={len(rest)}")
        decoded.extend(rest[:size])
        rest = rest[size + 2 :]


def framed_payload(status, fields, payload):
    length = fields.get(b"content-length")
    transfer = fields.get(b"transfer-encoding")
    if length is not None and transfer is not None:
        raise OSError("ambiguous response framing")
    if transfer is not None:
        if transfer.lower() != b"chunked":
            raise OSError(f"unsupported transfer-encoding: {transfer!r}")
        return decode_chunked(payload)
    if length is not None:
        try:
            declared = int(length)
        except ValueError as error:
            raise OSError("invalid Content-Length") from error
        if len(payload) != declared:
            raise OSError(f"truncated response: declared={declared} received={len(payload)}")
        return payload
    if status == 200:
        raise OSError("unframed 200 response")
    return payload


def raw_request(parsed, path, headers, mode, slow_read_ms, cancel_delay_ms):
    sock = socket.create_connection((parsed.hostname, parsed.port or 80), timeout=10)
    request = [f"GET {path} HTTP/1.1", f"Host: {parsed.netloc}", "Connection: close"]
    request.extend(f"{key}: {value}" for key, value in headers)
    sock.sendall(("\r\n".join(request) + "\r\n\r\n").encode())
    if mode == "cancel":
        # A small explicit window to be accepted and dispatched, then gone long before the answer can complete.
        time.sleep(cancel_delay_ms / 1000)
        sock.close()
        return 0, 0, None, {}
    body = bytearray()
    while True:
        block = sock.recv(4096)
        if not block:
            break
        body.extend(block)
        time.sleep(slow_read_ms / 1000)
    sock.close()
    head, _, payload = bytes(body).partition(b"\r\n\r\n")
    status = int(head.split(None, 2)[1]) if head else 0
    fields = {}
    for line in head.split(b"\r\n")[1:]:
        if b":" in line:
            name, value = line.split(b":", 1)
            name = name.strip().lower()
            if name in fields:
                raise OSError(f"duplicate response header: {name!r}")
            fields[name] = value.strip()
    payload = framed_payload(status, fields, payload)
    return status, len(payload), refusal_code(payload) if status >= 400 else None, fields


def histogram_percentile(histogram, p):
    total = sum(histogram.values())
    if not total:
        return 0.0
    want, seen = math.ceil(total * p), 0
    for bucket in sorted(histogram):
        seen += histogram[bucket]
        if seen >= want:
            return (bucket + 1) / 10  # ms; buckets are 0.1 ms wide
    return 0.0


def run_procs(args):
    """Split the load over `--procs` copies of this script and merge their results (percentiles from histograms)."""
    import subprocess
    import sys

    if args.requests is not None:
        raise SystemExit("--procs and --requests do not combine")
    child = [sys.executable, __file__]
    skip = False
    argv = sys.argv[1:]
    out_args = []
    for i, a in enumerate(argv):
        if skip:
            skip = False
            continue
        if a in ("--procs", "--concurrency"):
            skip = True
            continue
        out_args.append(a)
    per = max(1, args.concurrency // args.procs)
    children = [subprocess.Popen(child + out_args + ["--procs", "1", "--concurrency", str(per), "--emit-hist", "--proc-index", str(i)],
                                 stdout=subprocess.PIPE, text=True) for i in range(args.procs)]
    results = []
    for c in children:
        out, _ = c.communicate()
        if c.returncode:
            raise SystemExit(f"a driver process failed ({c.returncode})")
        results.append(json.loads(out.strip().splitlines()[-1]))
    merged_hist = collections.Counter()
    statuses, refusals, outcomes = collections.Counter(), collections.Counter(), collections.Counter()
    for r in results:
        merged_hist.update({int(k): v for k, v in r["_hist"].items()})
        statuses.update(r["statuses"])
        refusals.update(r["refusal_codes"])
        outcomes.update(r["error_outcomes"])
    elapsed = max(r["elapsed_s"] for r in results)
    requests = sum(r["requests"] for r in results)
    payload = sum(r["payload_bytes"] for r in results)
    print(json.dumps({
        "label": args.label, "mode": args.mode, "method": args.method, "concurrency": per * args.procs, "procs": args.procs,
        "elapsed_s": elapsed, "requests": requests, "attempted": sum(r["attempted"] for r in results),
        "req_s": round(requests / elapsed, 2), "payload_bytes": payload, "gib_s": round(payload / elapsed / (1024**3), 6),
        "p50_ms": histogram_percentile(merged_hist, 0.50), "p95_ms": histogram_percentile(merged_hist, 0.95),
        "p99_ms": histogram_percentile(merged_hist, 0.99), "max_ms": max(r["max_ms"] for r in results),
        "statuses": dict(sorted((int(k), v) for k, v in statuses.items())), "errors": sum(r["errors"] for r in results),
        "error_outcomes": dict(outcomes), "refusal_codes": dict(refusals),
    }, separators=(",", ":")))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--base", required=True, help="scheme://host:port")
    parser.add_argument("--path", help="one path, with query")
    parser.add_argument("--paths-file", help="one path per line, rotated through")
    parser.add_argument("--path-template", help="a path with {n}, filled from --template-offset upward (cold, unique requests)")
    parser.add_argument("--template-offset", type=int, default=1)
    parser.add_argument("--template-count", type=int, default=100000)
    parser.add_argument("--method", default="GET")
    parser.add_argument("--data", help="request body (JSON text) for POST")
    parser.add_argument("--duration", type=float, default=10)
    parser.add_argument("--requests", type=int, help="stop after this many claimed requests, for a cold one-pass set")
    parser.add_argument("--concurrency", type=int, default=16)
    parser.add_argument("--header", action="append", default=[])
    parser.add_argument("--mode", choices=("normal", "cancel", "slow-reader"), default="normal")
    parser.add_argument("--slow-read-ms", type=float, default=25)
    parser.add_argument("--cancel-delay-ms", type=float, default=5)
    parser.add_argument("--rotate-ip", action="store_true", help="vary X-Forwarded-For per request")
    parser.add_argument("--label")
    parser.add_argument("--expect-200-bytes", type=int)
    parser.add_argument("--min-200-bytes", type=int)
    parser.add_argument("--expect-prefix", help="a 200 body must start with this text")
    parser.add_argument("--expect-304-bytes", type=int, default=0)
    parser.add_argument("--procs", type=int, default=1, help="driver processes (one Python process saturates near 10k small requests/s)")
    parser.add_argument("--emit-hist", action="store_true", help="(internal) include the latency histogram for --procs to merge")
    parser.add_argument("--proc-index", type=int, default=0, help="(internal) keeps --path-template ids unique across --procs")
    args = parser.parse_args()
    if args.procs > 1:
        return run_procs(args)

    parsed = urllib.parse.urlsplit(args.base)
    if args.paths_file:
        paths = [l.strip() for l in pathlib.Path(args.paths_file).read_text().splitlines() if l.strip()]
    elif args.path_template:
        paths = None
    else:
        paths = [args.path]
    headers = [item.split(":", 1) for item in args.header]
    body_bytes = args.data.encode() if args.data is not None else None
    prefix = args.expect_prefix.encode() if args.expect_prefix else None
    deadline = time.monotonic() + args.duration
    lock = threading.Lock()
    latencies = []
    statuses = collections.Counter()
    payload_bytes = 0
    errors = 0
    error_outcomes = collections.Counter()
    refusal_codes = collections.Counter()
    sequence = 0
    fatal = []

    def worker():
        nonlocal payload_bytes, errors, sequence
        connection = None
        while time.monotonic() < deadline:
            with lock:
                if args.requests is not None and sequence >= args.requests:
                    return
                n = sequence
                sequence += 1
            if paths is not None:
                path = paths[n % len(paths)]
            else:
                path = args.path_template.format(n=args.template_offset + args.proc_index * 1_000_000 + n % args.template_count)
            started = time.perf_counter()
            try:
                request_headers = list(headers)
                if args.rotate_ip:
                    request_headers.append(("x-forwarded-for", f"198.18.{(n // 250) % 250}.{n % 250 + 1}"))
                if args.mode == "normal":
                    if connection is None:
                        connection = http.client.HTTPConnection(parsed.hostname, parsed.port, timeout=30)
                    send_headers = dict(request_headers)
                    if body_bytes is not None:
                        send_headers.setdefault("Content-Type", "application/json")
                    connection.request(args.method, path, body=body_bytes, headers=send_headers)
                    response = connection.getresponse()
                    body = response.read()
                    status, size = response.status, len(body)
                    framing = {}
                    content_length = response.getheader("content-length")
                    transfer_encoding = response.getheader("transfer-encoding")
                    if content_length is not None and transfer_encoding is not None:
                        raise OSError("ambiguous response framing")
                    if content_length is not None:
                        framing[b"content-length"] = content_length.encode()
                    if transfer_encoding is not None:
                        if transfer_encoding.lower() != "chunked":
                            raise OSError("unsupported transfer-encoding")
                        # http.client has already decoded a chunked body; a successful read is the terminator proof.
                        framing[b"content-length"] = str(size).encode()
                    framed_payload(status, framing, body)
                    code = refusal_code(body) if status >= 400 else None
                    if response.getheader("connection", "").lower() == "close":
                        connection.close()
                        connection = None
                else:
                    status, size, code, _ = raw_request(parsed, path, request_headers, args.mode, args.slow_read_ms, args.cancel_delay_ms)
                    body = b""
                if status == 200 and args.expect_200_bytes is not None and size != args.expect_200_bytes:
                    raise OSError(f"wrong 200 body size: expected={args.expect_200_bytes} received={size}")
                if status == 200 and args.min_200_bytes is not None and size < args.min_200_bytes:
                    raise OSError(f"short 200 body: minimum={args.min_200_bytes} received={size}")
                if status == 200 and prefix is not None and args.mode == "normal" and not body.startswith(prefix):
                    raise OSError(f"200 body does not start with {prefix!r}")
                if status == 304 and size != args.expect_304_bytes:
                    raise OSError(f"wrong 304 body size: expected={args.expect_304_bytes} received={size}")
                elapsed = time.perf_counter() - started
                with lock:
                    statuses[status] += 1
                    if status >= 400:
                        refusal_codes[f"{status}:{code or 'missing'}"] += 1
                    payload_bytes += size
                    latencies.append(elapsed)
            except (OSError, http.client.HTTPException) as error:
                connection = None
                with lock:
                    errors += 1
                    error_outcomes[f"{type(error).__name__}:{str(error) or getattr(error, 'errno', None)}"] += 1
            except Exception as error:  # a bug in the driver, not in the target
                with lock:
                    fatal.append(repr(error))
                return

    began = time.monotonic()
    threads = [threading.Thread(target=worker, daemon=args.requests is not None) for _ in range(args.concurrency)]
    for thread in threads:
        thread.start()
    if args.requests is None:
        for thread in threads:
            thread.join()
    else:
        watchdog = deadline + 31
        for thread in threads:
            thread.join(max(0, watchdog - time.monotonic()))
        if any(thread.is_alive() for thread in threads):
            raise RuntimeError("request-count workload exceeded its hard watchdog")
    if fatal:
        raise RuntimeError(f"load worker failed unexpectedly: {fatal[0]}")
    elapsed = time.monotonic() - began
    if args.requests is not None and sequence != args.requests:
        raise RuntimeError(f"request-count workload missed its deadline: claimed={sequence} required={args.requests}")
    completed = len(latencies)
    histogram = collections.Counter(min(600_000, int(value * 10_000)) for value in latencies) if args.emit_hist else None
    print(json.dumps({
        "_hist": dict(histogram) if histogram is not None else None,
        "label": args.label,
        "mode": args.mode,
        "method": args.method,
        "concurrency": args.concurrency,
        "elapsed_s": round(elapsed, 3),
        "requests": completed,
        "attempted": sequence,
        "req_s": round(completed / elapsed, 2) if elapsed else 0,
        "payload_bytes": payload_bytes,
        "gib_s": round(payload_bytes / elapsed / (1024**3), 6) if elapsed else 0,
        "p50_ms": round(percentile(latencies, 0.50) * 1000, 3),
        "p95_ms": round(percentile(latencies, 0.95) * 1000, 3),
        "p99_ms": round(percentile(latencies, 0.99) * 1000, 3),
        "max_ms": round(max(latencies, default=0) * 1000, 3),
        "statuses": dict(sorted(statuses.items())),
        "errors": errors,
        "error_outcomes": dict(sorted(error_outcomes.items())),
        "refusal_codes": dict(sorted(refusal_codes.items())),
    }, separators=(",", ":")))


if __name__ == "__main__":
    main()
