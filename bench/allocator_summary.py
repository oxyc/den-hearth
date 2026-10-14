#!/usr/bin/env python3
"""Markdown tables comparing bench/http/run.py reports of different allocators.

    bench/allocator_summary.py glibc=a.jsonl glibc=b.jsonl mimalloc=c.jsonl ...

Each argument is label=path; a label given twice is two runs. The columns are the runs, in order.
"""
import json
import sys

MIB = 1 << 20


def load(path):
    rows = [json.loads(line) for line in open(path) if line.strip()]
    cases = {r["case"]: r for r in rows if "case" in r and r["case"] != "idle-drain"}
    drain = next((r for r in rows if r.get("case") == "idle-drain"), None)
    return cases, drain


def table(header, body):
    out = ["| " + " | ".join(header) + " |", "|" + "|".join(["---"] + ["---:"] * (len(header) - 1)) + "|"]
    out += ["| " + " | ".join(str(c) for c in row) + " |" for row in body]
    return "\n".join(out)


def main(args):
    runs, seen = [], {}
    for arg in args:
        label, path = arg.split("=", 1)
        seen[label] = seen.get(label, 0) + 1
        cases, drain = load(path)
        runs.append((f"{label} r{seen[label]}", cases, drain))
    names = list(runs[0][1])
    head = ["case"] + [r[0] for r in runs]

    def per_case(title, get, fmt=lambda v: v):
        body = []
        for name in names:
            cells = []
            for _, cases, _ in runs:
                row = cases.get(name)
                value = get(row) if row else None
                cells.append("" if value is None else fmt(value))
            body.append([name] + cells)
        print(f"\n**{title}**\n")
        print(table(head, body))

    per_case("req/s", lambda r: r["req_s"], lambda v: f"{v:,.0f}")
    per_case("p50 ms", lambda r: r["p50_ms"])
    per_case("p99 ms", lambda r: r["p99_ms"])
    per_case("anon peak MiB (sampled)", lambda r: r["cgroup"]["anon_peak_sampled"] / MIB, lambda v: f"{v:.0f}")
    per_case("RSS peak MiB", lambda r: r["rss_kib"]["peak"] / 1024, lambda v: f"{v:.0f}")
    per_case("RSS MiB after recovery (case end)", lambda r: r["rss_kib"]["after_recovery"] / 1024, lambda v: f"{v:.0f}")

    # The laptop's load moves req/s by 2x between runs, so the headline compares per label: the median over its runs,
    # and the median of the per-round ratios to the first label (round i of a label against round i of the first,
    # which ran back to back). Req per CPU-second divides out how much CPU the host gave the process.
    labels = list(dict.fromkeys(r[0].rsplit(" ", 1)[0] for r in runs))
    by_label = {lab: [r for r in runs if r[0].rsplit(" ", 1)[0] == lab] for lab in labels}
    first = labels[0]

    def med(values):
        values = sorted(v for v in values if v is not None)
        return values[len(values) // 2] if len(values) % 2 else sum(values[len(values) // 2 - 1:len(values) // 2 + 1]) / 2

    def compact(title, get, digits=0):
        body = []
        for name in names:
            row = [name]
            for lab in labels:
                vals = [get(r[1][name]) for r in by_label[lab] if name in r[1] and get(r[1][name]) is not None]
                m = med(vals) if vals else None
                cell = "" if m is None else f"{m:,.{digits}f}"
                if lab != first and vals:
                    ratios = [get(a[1][name]) / get(b[1][name]) for a, b in zip(by_label[lab], by_label[first])
                              if name in a[1] and name in b[1] and get(a[1][name]) and get(b[1][name])]
                    cell += f" ({med(ratios):.2f}x)" if ratios else ""
                row.append(cell)
            body.append(row)
        print(f"\n**{title}** (median over runs; in brackets the median of per-round ratios to {first})\n")
        print(table(["case"] + labels, body))

    compact("req/s", lambda r: r["req_s"])
    compact("req per CPU-second", lambda r: r["req_s"] / r["cpu_cores"] if r.get("cpu_cores") else None)
    compact("p50 ms", lambda r: r["p50_ms"], 1)
    compact("p99 ms", lambda r: r["p99_ms"], 1)

    body, numbers = [], {}
    for label, cases, drain in runs:
        worst_anon = max(c["cgroup"]["anon_peak_sampled"] for c in cases.values()) / MIB
        worst_rss = max(c["rss_kib"]["peak"] for c in cases.values()) / 1024
        soak = cases.get("mixed-soak")
        oom = sum(c["cgroup"]["events_delta"]["oom"] for c in cases.values())
        kill = sum(c["cgroup"]["events_delta"]["oom_kill"] for c in cases.values())
        failed = [n for n, c in cases.items() if c["gate"] != "passed"]
        vals = [worst_anon, worst_rss, soak["rss_kib"]["peak"] / 1024 if soak else None,
                drain["cgroup"]["anon_after"] / MIB if drain else None, drain["rss_kib"]["after"] / 1024 if drain else None,
                drain["cgroup"]["current_after"] / MIB if drain else None]
        numbers.setdefault(label.rsplit(" ", 1)[0], []).append(vals)
        body.append([label] + [f"{v:.0f}" if v is not None else "" for v in vals] + [f"{oom}/{kill}", ", ".join(failed) or "passed"])
    head = ["anon peak (any case)", "RSS peak (any case)", "RSS peak, mixed-soak", "anon after 121 s idle",
            "RSS after 121 s idle", "memory.current after idle"]
    print("\n**Memory headline (MiB)**\n")
    print(table(["run"] + head + ["oom/oom_kill", "gate failed in"], body))
    print("\n**Memory headline, median per label (MiB)**\n")
    print(table(["label"] + head, [[lab] + [f"{med([r[i] for r in rs]):.0f}" for i in range(len(head))]
                                   for lab, rs in numbers.items()]))


if __name__ == "__main__":
    main(sys.argv[1:])
