#!/usr/bin/env python3
"""Table of the divan runs of bench/results/allocator/micro-raw-<variant>-<crate>-r<round>.txt.

    bench/allocator_micro_summary.py bench/results/allocator

Per benchmark and allocator: the median over the rounds of the fastest and of the median sample, and the ratio to
`system` (below 1.00x is faster).
"""
import json
import pathlib
import re
import subprocess
import sys

CONVERT = pathlib.Path(__file__).with_name("divan_to_jsonl.py")


def med(values):
    values = sorted(values)
    n = len(values)
    return values[n // 2] if n % 2 else (values[n // 2 - 1] + values[n // 2]) / 2


def fmt(ns):
    for unit, scale in (("s", 1e9), ("ms", 1e6), ("µs", 1e3)):
        if ns >= scale:
            return f"{ns / scale:.3g} {unit}"
    return f"{ns:.3g} ns"


def main(directory):
    data = {}  # (crate, bench) -> variant -> [(fastest, median)]
    for path in sorted(pathlib.Path(directory).glob("micro-raw-*.txt")):
        variant, crate, _ = re.fullmatch(r"micro-raw-(\w+)-(\w+)-r(\d+)\.txt", path.name).groups()
        out = subprocess.run([sys.executable, "-I", str(CONVERT), crate], stdin=path.open(), capture_output=True, text=True, check=True).stdout
        for line in out.splitlines():
            row = json.loads(line)
            data.setdefault((crate, row["bench"]), {}).setdefault(variant, []).append((row["fastest_ns"], row["median_ns"]))
    variants = ["system", "mimalloc", "jemalloc"]
    print("| benchmark | " + " | ".join(f"{v} fastest | {v} median" for v in variants) + " |")
    print("|---|" + "---:|" * (2 * len(variants)))
    for (crate, bench), per in data.items():
        cells = []
        base = None
        for v in variants:
            runs = per.get(v)
            if not runs:
                cells += ["", ""]
                continue
            fastest, median = med([r[0] for r in runs]), med([r[1] for r in runs])
            if v == "system":
                base = (fastest, median)
                cells += [fmt(fastest), fmt(median)]
            else:
                cells += [f"{fmt(fastest)} ({fastest / base[0]:.2f}x)", f"{fmt(median)} ({median / base[1]:.2f}x)"]
        print(f"| {crate}/{bench} | " + " | ".join(cells) + " |")


if __name__ == "__main__":
    main(sys.argv[1])
