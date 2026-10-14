#!/usr/bin/env python3
"""Turn `go test -bench -benchmem -count N` output (stdin) into one JSON object per benchmark (stdout).

Each benchmark's repeated runs are reduced to the median and the fastest, the same two statistics divan reports.
"""
import json
import re
import statistics
import sys
from collections import defaultdict

LINE = re.compile(
    r"^Benchmark(?P<name>\S+?)(?:-\d+)?\s+(?P<n>\d+)\s+(?P<ns>[\d.]+) ns/op"
    r"(?:\s+(?P<mbs>[\d.]+) MB/s)?(?:\s+(?P<bytes>\d+) B/op)?(?:\s+(?P<allocs>\d+) allocs/op)?"
)

runs = defaultdict(list)
for line in sys.stdin:
    m = LINE.match(line)
    if m:
        runs[m["name"]].append(m)

for name, ms in runs.items():
    ns = [float(m["ns"]) for m in ms]
    record = {
        "suite": "go-scout",
        "bench": name,
        "runs": len(ms),
        "median_ns": statistics.median(ns),
        "fastest_ns": min(ns),
        "slowest_ns": max(ns),
    }
    if ms[0]["allocs"] is not None:
        record["alloc_count_median"] = statistics.median(float(m["allocs"]) for m in ms)
        record["alloc_bytes_median"] = statistics.median(float(m["bytes"]) for m in ms)
    if ms[0]["mbs"] is not None:
        record["throughput_median_bytes_s"] = statistics.median(float(m["mbs"]) for m in ms) * 1e6
    print(json.dumps(record))
