#!/usr/bin/env python3
"""Turn divan's table (stdin, `--color never`) into one JSON object per benchmark (stdout).

    divan_to_jsonl.py <suite> < divan-output.txt

divan has no machine-readable output, so this reads the table by column position: the header line fixes where the
name column ends. Times are nanoseconds, throughput bytes per second, allocation sizes bytes (divan prints
decimal KB/MB). Allocation counts are per iteration, from the AllocProfiler the benches install.
"""
import json
import re
import sys

TIME = {"ps": 1e-3, "ns": 1.0, "µs": 1e3, "us": 1e3, "ms": 1e6, "s": 1e9}
SIZE = {"B": 1, "KB": 1e3, "MB": 1e6, "GB": 1e9, "TB": 1e12, "KiB": 1024, "MiB": 1 << 20, "GiB": 1 << 30}
STATS = {"max alloc:": "max_alloc", "alloc:": "alloc", "dealloc:": "dealloc", "grow:": "grow", "shrink:": "shrink"}
COLUMNS = ("fastest", "slowest", "median", "mean")


def number_with_unit(text, units):
    m = re.fullmatch(r"([\d.]+)\s*(\S+)", text.strip())
    if not m or m.group(2) not in units:
        return None
    return float(m.group(1)) * units[m.group(2)]


def main():
    suite = sys.argv[1]
    lines = sys.stdin.read().splitlines()
    header = next((i for i, l in enumerate(lines) if "fastest" in l and "slowest" in l), None)
    if header is None:
        sys.exit("no divan header found")
    width = lines[header].index("fastest")
    names, nodes, stat, expect_count = {}, [], None, True
    node = None
    for line in lines[header + 1 :]:
        name_col, rest = line[:width], line[width:]
        fields = [f.strip() for f in rest.split("│")]
        marker = re.search(r"[├╰]─ ", name_col)
        if marker:
            depth = marker.start() // 3
            name = name_col[marker.end() :].strip()
            names[depth] = name
            for deeper in [d for d in names if d > depth]:
                del names[deeper]
            stat = None
            node = {"suite": suite, "bench": "/".join(names[d] for d in sorted(names))}
            if fields[0]:
                for col, text in zip(COLUMNS, fields):
                    node[col + "_ns"] = number_with_unit(text, TIME)
                node["samples"] = int(fields[4]) if len(fields) > 4 and fields[4] else None
                node["iters"] = int(fields[5]) if len(fields) > 5 and fields[5] else None
                nodes.append(node)
            continue
        if node is None or not fields or not fields[0]:
            continue
        first = fields[0]
        if first in STATS:
            stat, expect_count = STATS[first], True
        elif first.endswith("/s"):
            for col, text in zip(COLUMNS, fields):
                value = number_with_unit(text.replace("/s", ""), SIZE)
                if col in ("median", "fastest") and value is not None:
                    node["throughput_" + col + "_bytes_s"] = value
        elif stat:
            key = stat + ("_count" if expect_count else "_bytes")
            for col, text in zip(COLUMNS, fields):
                if col not in ("median", "mean"):
                    continue
                value = float(text) if expect_count else number_with_unit(text, SIZE)
                node[f"{key}_{col}"] = value
            expect_count = not expect_count
    for n in nodes:
        print(json.dumps(n, ensure_ascii=False))


if __name__ == "__main__":
    main()
